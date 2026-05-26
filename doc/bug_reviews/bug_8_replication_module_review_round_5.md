# Replication Module Bug Review (Round 5)

Date: 2026-04-20
Author: AI agent 1

## Bug 6: Global "first slot exists done" flag can be stale after master failover when a non-first slot handles `hi` first

**Severity:** Medium

**Files:** `src/main/java/io/velo/persist/LocalPersist.java:479-497`, `src/main/java/io/velo/command/XGroup.java:526-528`, `src/main/java/io/velo/command/XGroup.java:1513-1524`, `src/main/java/io/velo/command/XGroup.java:1382-1384`

```java
// LocalPersist.java
private volatile boolean isAsSlaveFirstSlotFetchedExistsAllDone = false;
```

```java
// XGroup.java hi() — slave receives hi
if (slot == localPersist.firstOneSlot().slot()) {
    localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(false);
}
```

```java
// XGroup.java s_catch_up() — non-first slot gate
if (slot != localPersist.firstOneSlot().slot()) {
    if (!localPersist.isAsSlaveFirstSlotFetchedExistsAllDone()) {
        // delay and retry
        return Repl.emptyReply();
    }
}
```

```java
// XGroup.java s_exists_all_done() — slave finished exists for one slot
if (slot == localPersist.firstOneSlot().slot()) {
    localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(true);
}
```

`isAsSlaveFirstSlotFetchedExistsAllDone` is a global per-node flag used as a barrier: non-first slots must wait until the first slot has finished its exists phase (in particular, fetched the global dict) before they start applying catch-up binlog. The flag is only reset to `false` in `hi()` when `slot == firstOneSlot.slot()`. Each slot has its own `ReplPair` and its own `hi`/`hello` handshake path, and they run concurrently on different slot worker eventloops.

On master failover — `resetAsSlave` is called for every slot and each slot reconnects to the new master independently — there is no ordering guarantee that slot 0's `hi` arrives and is processed first. If a non-first slot's `hi` arrives first, it enters the "need flush local data" path and proceeds to fetch its own exists data, and eventually its binlog catch-up begins. Meanwhile the global flag still holds the stale `true` set by the previous master's session (it is set to true in `s_exists_all_done` at line 1383 and is never cleared by a non-first slot's `hi`). The non-first slot's catch-up gate at `s_catch_up` line 1515 therefore passes, and it starts applying binlog entries from the new master before slot 0 has refreshed the dict.

Consequence: entries that reference a dict seq that exists on the new master but not yet in the slave's `DictMap` will fail to decompress during apply, surfacing as an apply error or wrongly-decoded value. The window is small in practice (slot 0 usually reaches `hi` quickly too) but the correctness of the gate is not enforced by the code — it relies on timing.

Fix direction: reset the flag to `false` at the start of *any* slot's `hi()` when the remote master UUID changes, or move the flag out of `LocalPersist` and into `MetaChunkSegmentIndex` per-slot with a cross-slot coordination primitive.

---

## Bug 7: Slave persists "exists data all fetched" before target-slot writes land, losing data across reconnect

**Severity:** High

**Files:** `src/main/java/io/velo/command/XGroup.java:727-749` (`putToTargetWal`), `src/main/java/io/velo/command/XGroup.java:896-907` (`s_exists_chunk_segments`), `src/main/java/io/velo/command/XGroup.java:1123-1137` (`s_exists_big_string`), `src/main/java/io/velo/command/XGroup.java:1369-1384` (`s_exists_all_done`), `src/main/java/io/velo/persist/MetaChunkSegmentIndex.java:100-132`

```java
// s_exists_chunk_segments — cross-slot fire-and-forget write
for (var entry : groupedBySlot.entrySet()) {
    var slotInner = entry.getKey();
    var oneSlot = localPersist.oneSlot(slotInner);
    oneSlot.asyncExecute(() -> {
        for (var entry2 : entry.getValue().entrySet()) {
            var key = entry2.getKey();
            var cv = entry2.getValue();
            var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash());
            oneSlot.put(key, bucketIndex, cv, true);
        }
    });
}
```

```java
// s_exists_all_done — durably persist "all fetched" flag
var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
oneSlot.updateChunkSegmentIndexFromMeta();

metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
        lastUpdatedFileIndex, lastUpdatedOffset);

if (slot == localPersist.firstOneSlot().slot()) {
    localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(true);
}
```

```java
// MetaChunkSegmentIndex — file-backed durable state
public void setMasterBinlogFileIndexAndOffset(long masterUuid, boolean isExistsDataAllFetched,
        int masterBinlogFileIndexNextTimeToFetch, long masterBinlogOffsetNextTimeToFetch) {
    ...
    updatedBuffer.putInt(isExistsDataAllFetched ? 1 : 0);
    ...
}
```

The initial exists-phase handlers on the slave — `s_exists_wal` (via `putToTargetWal`), `s_exists_chunk_segments`, and `s_exists_big_string` — iterate the decoded content, group entries by target slot, and dispatch each group via `oneSlot.asyncExecute(...)` to the destination slot's eventloop. They then immediately advance the state machine and return the next REPL request (e.g., next WAL group, next chunk-segment batch, next bucket). There is no promise-based synchronization like the uncommitted Bug 4 fix provides for `s_catch_up`.

Eventually the slave completes the entire exists handshake and enters `s_exists_all_done`. That method **durably persists** `isExistsDataAllFetched = true` to `MetaChunkSegmentIndex` (which is file-backed). If the slave process crashes between "all the target-slot `asyncExecute` tasks are queued" and "all those tasks have actually written their data through WAL/persist", the slave's on-disk metadata says "all exists data fetched" but the actual data is partial.

On restart/reconnect, `hi()` at line 537 reads `isExistsDataAllFetched == true` from the persisted meta; if the master UUID matches, the slave **skips the entire exists fetch** and jumps directly into binlog catch-up. Missing keys from the prior exists phase are never reconciled. Reads will either miss data that exists on the master, or serve stale/empty values for keys whose big-string files were never written on disk.

This differs from Bug 4 in three ways: (a) it happens during initial sync, not incremental catch-up, so the divergence is baseline; (b) the "progress" that is persisted is on-disk `MetaChunkSegmentIndex`, not just in-memory offsets, so restart does not recover via retry; (c) the fire-and-forget spans three different handler paths, so a single-point async-completion fix (as in Bug 4) must be generalized.

Trigger scenario: a slave doing first-time sync with a large dataset, where the target-slot eventloops are slow (cross-slot key distribution means heavy concurrent writes), and the slave process is killed or crashes after receiving the final `s_exists_all_done` but before the background writes finish. This window is not microseconds — `asyncExecute` enqueues on the target slot's eventloop, which may be minutes behind under load.

Fix direction: either (a) extend the `BinlogContent.applyAsync` pattern from the Bug 4 fix to the three exists handlers so `s_exists_all_done` only persists the flag after all queued writes have completed, or (b) introduce a per-target-slot "exists writes acknowledged" counter and have `s_exists_all_done` block on it before persisting. Option (a) is consistent with the direction already taken for Bug 4.

---

## Review Notes (AI agent 2)

Date: 2026-04-20
Reviewer: AI agent 2
Scope: verify the two findings against `main` at the time of this review.

### Bug 6 — Confirmed, with one impact refinement

Verified against:

- `LocalPersist.java:479-497` — `isAsSlaveFirstSlotFetchedExistsAllDone` is a single global volatile boolean.
- `XGroup.java:526-528` — the flag is reset to `false` only in `hi()` when `slot == firstOneSlot.slot()`.
- `XGroup.java:578-601` — after `hi()`, only the first slot fetches dicts; non-first slots explicitly skip `exists_dict` and go directly to `exists_big_string`.
- `XGroup.java:1513-1524` — non-first slots gate `s_catch_up()` solely on that global flag.
- `XGroup.java:1382-1384` — the flag is set back to `true` only when the first slot handles `s_exists_all_done()`.
- `OneSlot.resetAsSlave()` (`OneSlot.java:2458-2474`) — per-slot persisted replication metadata is cleared during re-slave, but the global `LocalPersist` flag is not.

Why the bug is real:

1. The barrier is node-global, but reconnect/`hi()` is per slot.
2. On failover or any re-slave sequence, `resetAsSlave()` clears per-slot meta first, then each slot reconnects independently.
3. If a non-first slot reaches `hi()` before the first slot, the old `true` barrier value can still be present from the previous master session.
4. That non-first slot will skip dict fetch, run through its exists phase, and then pass the `s_catch_up()` gate before slot 0 has refreshed the global dict set for the new master.

Refinement to the original write-up:

The doc overstates the immediate failure mode when it says this will "surface as an apply error or wrongly-decoded value" during catch-up apply. The current catch-up apply paths do **not** decompress dictionary-backed values at apply time:

- `XWalV.apply()` stores `cvEncoded` directly into the target WAL.
- `XBigStrings.apply()` decodes `CompressedValue`, but that decode does not resolve the referenced dict.

So the directly supported consequence is narrower and more precise:

- the slave can accept and persist values whose dict seq is not yet present in `DictMap`;
- later reads/decompression on those keys can fail with a missing-dict path such as `DictMissingException` / `ERR cannot read` until slot 0 fetches and persists the dicts.

That still makes the bug valid. The barrier is meant to prevent exactly this stale-global-dict window, and the current code does not guarantee it.

Severity assessment:

- Medium is reasonable.
- The window is timing-dependent and often heals once slot 0 catches up, so I would not rate it High.
- It becomes more serious if the slave crashes or reconnects repeatedly before slot 0 refreshes dict state, because the stale barrier can be observed across sessions too.

Suggested fix direction:

- Reset the flag whenever a new slave session starts, not only when slot 0 processes `hi()`.
- Practically, clear it in the re-slave orchestration path before any slot reconnects, or clear it in `hi()` for every slot when the master UUID changed from the last persisted one.
- A per-master-session epoch would be safer than a raw boolean.

### Bug 7 — Confirmed

Verified against:

- `XGroup.java:727-749` — `putToTargetWal(...)` dispatches cross-slot WAL replay through `oneSlot.asyncExecute(...)` and returns immediately.
- `XGroup.java:896-907` — `s_exists_chunk_segments()` groups entries by target slot, enqueues `oneSlot.put(...)` via `asyncExecute(...)`, then immediately advances to the next replication step.
- `XGroup.java:1123-1137` — `s_exists_big_string()` enqueues `writeBigStringBytes(...)` to target slots asynchronously, then immediately advances.
- `XGroup.java:1369-1384` — `s_exists_all_done()` persists `isExistsDataAllFetched = true` to `MetaChunkSegmentIndex`.
- `MetaChunkSegmentIndex.java:100-132` — that metadata is file-backed and survives restart.
- `XGroup.java:537-566` — on the next `hi()`, if persisted meta says `isExistsDataAllFetched == true` for the same master UUID, the slave skips the entire exists phase and jumps straight to catch-up.

Why the bug is real:

1. The exists-phase handlers enqueue target-slot writes but do not wait for completion.
2. `s_exists_all_done()` then durably records "exists data all fetched".
3. A crash between those two moments leaves persisted progress ahead of actual persisted data.
4. On restart, the slave trusts the durable flag and skips re-fetching exists data, so the missing writes are never repaired.

This is stronger than Bug 4 because the incorrect progress marker is explicitly persisted on disk, not just kept in memory.

Severity assessment:

- High is appropriate.
- This can produce durable baseline divergence after restart, not just a transient in-memory inconsistency.

Suggested fix direction:

- The review’s proposed direction is sound: the exists handlers need the same completion accounting that Bug 4 introduced for catch-up apply.
- The durable `isExistsDataAllFetched` marker should only be written after all enqueued target-slot writes for the current exists session have completed successfully.

### Cross-finding observation

Bug 6 and Bug 7 are different correctness failures, but they share the same architectural smell:

- cross-slot replication work is modeled as "done" too early;
- the code relies on ordering/timing assumptions that are not enforced by completion tracking.

Bug 7 is the higher-priority fix because it can permanently lose baseline data across restart. Bug 6 is still valid, but its primary effect is an invalid global readiness/readability signal rather than immediate apply failure.

---

## Commit Review (AI agent 2)

Date: 2026-04-21 14:36:28 +0800
Reviewer: AI agent 2
Commit reviewed: `9d20f5a` (`fix: await exists-phase cross-slot writes; reset dict barrier on master change`)
Scope: review the bug 6/7 fix commit for remaining correctness issues and regressions.

### Finding 1 — High: bug 7 remains open for `s_exists_short_string`

Verified against:

- `XGroup.java:1278-1329` — `s_exists_short_string()` still dispatches cross-slot writes with `oneSlot.asyncExecute(...)`.
- `XGroup.java:1313-1327` — after enqueueing, it immediately advances to the next exists step or next short-string batch.
- `XGroup.java:1439-1441` — `s_exists_all_done()` still durably persists `isExistsDataAllFetched = true`.

Why this is still a bug:

The commit correctly added completion gating for:

- `s_exists_wal()`
- `s_exists_chunk_segments()`
- `s_exists_big_string()`

But `s_exists_short_string()` was left on the old fire-and-forget path. That means the original bug 7 still reproduces for short-string cross-slot replay:

1. short-string writes are queued to target slot eventloops;
2. the protocol advances immediately;
3. `s_exists_all_done()` can persist "all fetched";
4. a crash/restart in that window leaves durable exists-progress ahead of actual short-string data.

On reconnect, `hi()` will trust the persisted flag and skip exists replay for that master session, so the missing short-string keys are not repaired.

Verdict:

- The commit partially fixes bug 7, but does not close it completely.

Suggested fix direction:

- Convert `s_exists_short_string()` to the same promise-based gating pattern used elsewhere in this commit, and add a regression test that blocks the target slot eventloop and verifies `exists_all_done` is not reached early.

### Finding 2 — Medium: delayed exists-step branches can stall forever on async write failure

Verified against:

- `XGroup.java:721-732` — delayed `s_exists_wal()` branch waits on `pendingWrites`, logs on error, then returns without replying or retrying.
- `XGroup.java:971-982` — delayed `s_exists_chunk_segments()` branch has the same behavior.
- `XGroup.java:752-767` — the non-delayed helper `wrapExistsReplyAfter(...)` does convert async write failures into a `Repl.error(...)`.

Why this is a regression:

In the delayed branches, once the handler returns `Repl.emptyReply()`, the session depends on the callback to either:

- schedule the next request, or
- surface a failure

But on error, the callback only logs and exits. No error reply is sent, and no retry is scheduled. Replication can therefore hang silently instead of failing fast.

This produces inconsistent behavior:

- non-delayed path: async write failure becomes a replication error
- delayed path: async write failure becomes a stuck session

Suggested fix direction:

- Reuse the same failure semantics as `wrapExistsReplyAfter(...)` in the delayed branches:
  either send an explicit `Repl.error(...)` / disconnect, or schedule a deterministic retry policy.

### Positive note

The bug 6 change itself looks correct:

- `XGroup.java:536-543` now resets the global first-slot dict barrier when the persisted master UUID differs from the new master's UUID, which closes the stale-barrier case for non-first slots.

### Test coverage note

The commit adds useful tests for:

- catch-up async gating (`XGroupTest.groovy:2543-2727`)
- master-UUID barrier reset (`XGroupTest.groovy:2772-2814`)

I do not see a direct exists-phase regression test in this commit for:

- `s_exists_wal()`
- `s_exists_chunk_segments()`
- `s_exists_big_string()`
- `s_exists_short_string()`
