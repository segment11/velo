# Replication Module Bug Review (Round 4)

Date: 2026-04-20
Author: AI agent 1

## Bug 4: slave catch-up advances replication state before async apply work has actually executed

**Severity:** High
**Files:** `src/main/java/io/velo/repl/TcpClient.java:140-168`, `src/main/java/io/velo/command/XGroup.java:1609-1666`, `src/main/java/io/velo/repl/incremental/XWalV.java:163-179`, `src/main/java/io/velo/repl/incremental/XBigStrings.java:183-196`

```java
BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
        .decodeStream(new ReplDecoder())
        .mapAsync(pipeline -> {
            ...
            var reply = xGroup.handleRepl(replRequest);
```

```java
for (var content : decodedContents) {
    content.apply(slot, replPair);
}
...
replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(
        new Binlog.FileIndexAndOffset(fetchedFileIndex, fetchedOffset + readSegmentLength));
```

```java
oneSlot.asyncExecute(() -> {
    var putResult = targetWal.put(isValueShort, key, v2);
    if (putResult.needPersist()) {
        oneSlot.doPersist(walGroupIndex, key, putResult);
    }
});
```

Slave-side REPL traffic is processed in the TCP client pipeline, and `s_catch_up()` treats `content.apply(...)` as if it were synchronous completion. That assumption is false for cross-slot replay work: `XWalV.apply()` and `XBigStrings.apply()` enqueue the actual persistence onto target slot event loops via `oneSlot.asyncExecute(...)` and then return immediately.

After those asynchronous applies are merely queued, `s_catch_up()` advances the slave’s catch-up offset, updates `metaChunkSegmentIndex`, toggles readability, and schedules the next fetch. A crash, disconnect, or role switch in the window between “queued” and “executed” can leave the replica with offsets that claim a segment was applied even though the destination slot workers never ran the writes. This is a replay-consistency bug and can produce silent data loss or divergence after recovery.

## Bug 5: incremental big-string fetch can drop the file payload permanently while marking the fetch as complete

**Severity:** High
**Files:** `src/main/java/io/velo/repl/incremental/XBigStrings.java:183-196`, `src/main/java/io/velo/repl/ReplPair.java:782-835`, `src/main/java/io/velo/command/XGroup.java:946-1000`

```java
oneSlot.asyncExecute(() -> {
    oneSlot.put(key, s.bucketIndex(), cv, true);
});
replPair.addToFetchBigStringId(uuid, bucketIndex, keyHash, key);
```

```java
var bigStringBytes = bigStringFiles.getBigStringBytes(uuid, s.bucketIndex(), s.keyHash());
if (bigStringBytes == null) {
    ...;
    bigStringBytes = new byte[0];
}
```

```java
if (buffer.hasRemaining()) {
    ...
    oneSlot.asyncExecute(() -> {
        bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
    });
}

replPair.doneFetchBigStringUuid(uuid);
```

`XBigStrings.apply()` persists the logical metadata/value first and only records the backing big-string file fetch in `ReplPair`’s in-memory fetch queues. Later, when the slave asks for `incremental_big_string`, the master returns an empty payload if the underlying big-string file is already missing. On the slave side, `s_incremental_big_string()` interprets “no remaining payload bytes” as “nothing to write”, but it still calls `doneFetchBigStringUuid(uuid)` unconditionally.

That clears the only fetch-tracking entry even though no file bytes were stored. Because the to-fetch and in-progress lists are transient `LinkedList`s in memory rather than durable replay state, reconnect or restart loses the retry opportunity entirely. The slave can then retain a key whose metadata points at a big string that was never actually fetched, leading to missing or corrupted reads later.

---

## Review Notes (AI agent 2)

Date: 2026-04-20
Reviewer: AI agent 2
Scope: verify the two findings against `main` at the time of this review.

### Bug 4 — Confirmed

Verified against:

- `TcpClient.java:140-168` — slave REPL pipeline dispatches each `ReplRequest` through `xGroup.handleRepl(...)` synchronously per pipeline entry.
- `XGroup.java:1618-1656` — `s_catch_up()` iterates `decodedContents` and calls `content.apply(slot, replPair)` directly inside the handler, then unconditionally advances `setSlaveLastCatchUpBinlogFileIndexAndOffset(...)` at line 1656.
- `XWalV.apply()` (`XWalV.java:163-180`) — schedules the actual WAL put via `oneSlot.asyncExecute(...)`. The apply returns as soon as the task is enqueued. It also calls `replPair.setSlaveCatchUpLastSeq(v.seq())` synchronously — which means the "catch-up sequence" claim is made before the write lands.
- `XBigStrings.apply()` (`XBigStrings.java:183-196`) — same pattern: `oneSlot.asyncExecute(() -> oneSlot.put(...))` then synchronous `addToFetchBigStringId(...)`.

**Why this is a real replay-consistency problem:**

1. `BaseCommand.slot(key, slotNumber)` inside `XWalV.apply()` can resolve to a **different slot** than the one currently catching up, so the queued work runs on another slot worker's event loop. There is no backpressure or completion signal from the target slot back to `s_catch_up()`.
2. `s_catch_up()` persists the advanced offset via `setSlaveLastCatchUpBinlogFileIndexAndOffset(...)` and (for the latest-segment case) writes `metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(...)`. After a crash/restart, recovery reads these offsets and assumes the binlog range up to that offset is durable on disk, so the slave never re-fetches.
3. Entries still pending on the target slot's event loop at crash time are lost — they were never written to WAL or persisted.

**Severity assessment:** High is appropriate. The window is short in normal operation (target event loop drains quickly), but visible on: master failover, slave process crash, unclean shutdown, or any path that relies on offsets as a durability marker.

**Suggested fix direction:**

- Option A (preferred): make `BinlogContent.apply()` return a `Promise<Void>` (or boolean) for cross-slot work, and have `s_catch_up()` wait for all applies in the batch before advancing offsets. Requires threading promises through `XWalV` / `XBigStrings`.
- Option B: have `apply()` block on the cross-slot work synchronously via `oneSlot.submit(...).get()`-style wait. Simpler but reintroduces cross-worker blocking and risks deadlock if the target slot currently waits on the source.
- Option C: persist an "applied-up-to" watermark per target slot distinct from the fetched-offset watermark, and on restart replay from `min(applied)` across slots. More invasive but matches Redis replication semantics.

**Tests worth adding:** inject a delayed `asyncExecute` on the target slot, call `content.apply(...)` and then `setSlaveLastCatchUpBinlogFileIndexAndOffset(...)`, restart the replica mid-flight, and assert no data loss.

### Bug 5 — Confirmed, with one refinement

Verified against:

- `XBigStrings.apply()` (`XBigStrings.java:183-196`) — metadata/value written via `oneSlot.put(key, ..., cv, true)`, then `addToFetchBigStringId(...)`. The persistence of metadata happens *asynchronously* — another instance of the Bug 4 pattern — but the tracking-list add is synchronous.
- Master side `incremental_big_string` (`XGroup.java:946-969`) — when `bigStringFiles.getBigStringBytes(...)` returns `null`, substitutes `new byte[0]` and ships a reply with key header but empty file bytes. The master warns but succeeds.
- Slave side `s_incremental_big_string` (`XGroup.java:971-1001`) — skips the `writeBigStringBytes` call when `!buffer.hasRemaining()`, then unconditionally executes `replPair.doneFetchBigStringUuid(uuid)` at line 999.
- `ReplPair.doneFetchBigStringUuid` (`ReplPair.java:839-841`) — drains from `doFetchingBigStringIdList` only (no re-enqueue). And lists are plain in-memory `LinkedList`s (`ReplPair.java:783,788`), so they do not survive a restart.

**Why this is a real data-divergence problem:**

1. Slave has a `CompressedValue` whose encoding references a big-string file by uuid (persisted in step 1 of `XBigStrings.apply`).
2. That reference is now pointing at nothing on the slave filesystem, and the fetch queue entry has been cleared.
3. Subsequent reads of the key will either return empty/partial data or throw, depending on how the big-string reader handles a missing file.
4. No repair path: no durable retry queue, no periodic reconcile against master, no invalidation of the parent value.

**Severity assessment:** High is appropriate, and arguably this is the more immediately exploitable of the two findings because it does not need a crash — the failure happens on any normal master → slave replication where the big-string file has already been GC'd on master before the slave's incremental fetch reaches it.

**Refinement to the original finding:**

The report frames the issue as "the master returns an empty payload and the slave still marks done". That is accurate, but the root cause is more specific: **the protocol lacks an "unavailable" sentinel distinct from "0-byte value"**. A legitimate 0-length big string (e.g., an empty string encoded as big-string due to sizing thresholds upstream) and a missing file are both encoded as `remaining() == 0` on the wire. The slave cannot tell them apart.

**Suggested fix direction:**

- Preferred: change the master's `incremental_big_string` reply to include an explicit status byte (e.g., `0 = ok`, `1 = missing on master`). When missing, the slave should either (a) delete the parent key/value it had optimistically persisted in `XBigStrings.apply()`, or (b) surface an error to the operator so the value can be reconciled. Silently dropping the reference is the worst option.
- Additionally: make the fetch queues durable — either flush to the binlog applied-watermark or to a local sidecar file — so restart does not lose pending fetches independent of this bug.
- Minor: `replPair.doneFetchBigStringUuid(uuid)` should only be called on the success path inside the `if (buffer.hasRemaining())` block (and after the async write completes, if Bug 4 fix lands).

**Tests worth adding:**

- Master has big-string file `X`, slave applies `XBigStrings` with uuid `X`, master deletes the file, slave sends `incremental_big_string`, master responds with empty payload — assert the slave retains the uuid in `toFetchBigStringIdList` or invalidates the parent value (depending on chosen fix).
- Master has legitimately empty big-string file (if that is a supported case) — assert the slave distinguishes from "missing" via the new status byte.

### Inter-bug observation

Both bugs share a root cause: **slave-side replay treats `apply()` as synchronous completion while the implementation is fire-and-forget via `oneSlot.asyncExecute(...)`**. Fixing Bug 4 properly (apply returns a promise, catch-up waits for it) may eliminate a significant portion of Bug 5 if combined with a missing-file sentinel — because the slave would only ever mark a uuid "done" after both the metadata and the file bytes have actually landed.

Consider fixing Bug 4's async-completion plumbing first, then layering Bug 5's protocol-level status byte on top.
