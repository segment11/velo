# Replication Module Bug Review (Round 1)

Date: 2026-06-25
Author: AI agent 1

Scope: `XGroup` existing-data replay and incremental catch-up when a slave has more local slots than the master, for
example slave `slotNumber = master slotNumber * 2`. This review follows the bug review workflow: record findings with
severity, cited files and line ranges, code excerpts, root cause, impact, and suggested fix direction. No production code
was changed in this round.

Design context:

- Replication is per-slot and uses a fetch-existing-data phase followed by incremental catch-up
  (`doc/design/09_replication_design.md`).
- The scale-up design notes that vanilla slave replication is slot-to-slot at equal `slotNumber`, while a shadow
  reshard/slave needs a special apply mode that re-hashes each binlog entry under the new `slotNumber` and fans it out
  (`doc/design/18_scale_up_design.md`).

## Bug 1: local slave slots above the master's slot count are reset as replicas but never complete replay

**Severity:** High

**Files:**

- `src/main/java/io/velo/ConfForSlot.java:160-167`
- `src/main/java/io/velo/repl/LeaderSelector.java:522-536`
- `src/main/java/io/velo/persist/OneSlot.java:2576-2599`
- `src/main/java/io/velo/repl/ReplPair.java:440-443`
- `src/main/java/io/velo/command/XGroup.java:255-257`

**Code excerpts:**

```java
// ConfForSlot.java:160-167
public boolean slaveCanMatch(SlaveCheckValues checkValuesFromMaster) {
    ...
    if (ConfForGlobal.slotNumber < checkValuesFromMaster.slotNumber) {
        return false;
    }
```

```java
// LeaderSelector.java:522-536
Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
    var oneSlot = localPersist.oneSlot((short) i);
    promises[i] = oneSlot.asyncRun(() -> {
        ...
        oneSlot.resetAsSlave(host, port);
    });
}
```

```java
// OneSlot.java:2576-2599
public void resetAsSlave(@NotNull String host, int port) throws IOException {
    ...
    createReplPairAsSlave(host, port);

    if (!isReadonly()) {
        setReadonly(true);
    }
    if (isCanRead()) {
        setCanRead(false);
    }
    ...
    dynConfig.setBinlogOn(false);
}
```

```java
// ReplPair.java:440-443
var replContent = new Hello(slaveUuid, ConfForGlobal.announcedHostPortString());

tcpClient = new TcpClient(slot, eventloop, requestHandler, this);
tcpClient.connect(host, port, connectTimeoutMillis, () -> Repl.buffer(slaveUuid, slot, ReplType.hello, replContent));
```

```java
// XGroup.java:255-257
if (slotRemote >= ConfForGlobal.slotNumber) {
    log.warn("Repl do nothing for slot remote={}, repl type={}", slotRemote, replType);
    return Repl.emptyReply();
}
```

**Root cause:**

The compatibility check intentionally permits `local slave slotNumber >= master slotNumber`; it only rejects the case
where the slave has fewer slots than the master. `LeaderSelector.resetAsSlave(...)` then resets every local slave slot,
including slots that do not exist on the master. Those extra local slots become readonly, cannot read, have binlog writes
disabled, and open a slave-side `ReplPair`.

Each extra slot sends `hello` with its local slot id. The master immediately drops any replication frame whose slot id is
greater than or equal to the master's `ConfForGlobal.slotNumber`, returning an empty reply before `hello`, `hi`, exists
fetch, or catch-up can run. Therefore slave slots in `[masterSlotNumber, slaveSlotNumber)` never receive
`s_exists_all_done`, never become readable, and never participate in catch-up progress.

This is not fully mitigated by existing-data fanout. `XGroup.s_exists_wal`, `s_exists_chunk_segments`,
`s_exists_big_string`, and `s_exists_short_string` do re-hash fetched keys under the slave's larger `slotNumber` and can
write data into the extra slots. However, the extra slots' own replica lifecycle still remains stuck in the
pre-readable slave state.

**Impact:**

A slave configured with twice the master's slots can accept the topology check and start replay, but any client request
whose key hashes to one of the extra local slots can be rejected or blocked because that target `OneSlot` remains
`canRead=false`. Replication status is also misleading: some local slots have live slave pairs that can never complete,
while data may still be written into them by fanout from lower master-slot streams.

**Suggested fix direction:**

Treat mismatched slot counts as a separate reshard-replication mode instead of normal per-slot slave replication. Minimal
safe options are:

- Reject `slave slotNumber != master slotNumber` until a full reshard mode exists, so the current slot-to-slot replay
  invariant remains explicit.
- Or create a dedicated mapping where only master slots open replication streams, existing and incremental data are
  re-hashed into target slave slots, and all target slave slots' readable/progress state is advanced by the owning master
  stream after its fanout writes complete.

The second option needs an explicit progress model for target slots that do not have a one-to-one master stream.

## Bug 2: incremental `XFlush` only flushes the master stream slot, leaving stale data in fanout target slots

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/XGroup.java:999-1012`
- `src/main/java/io/velo/command/XGroup.java:1237-1258`
- `src/main/java/io/velo/command/XGroup.java:1364-1384`
- `src/main/java/io/velo/command/XGroup.java:1765-1769`
- `src/main/java/io/velo/repl/incremental/XFlush.java:85-89`

**Code excerpts:**

```java
// XGroup.java:999-1012, existing chunk replay fanout
var keyHash = KeyHash.hash(Wal.keyBytes(key));
var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, cv);
```

```java
// XGroup.java:1237-1258, existing big-string replay fanout
var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
var bucketIndexInner = KeyHash.bucketIndex(keyHash);
entries.add(new BigStringEntry(uuid, slotInner, bucketIndexInner, keyHash, offset, bigStringBytesLength));
...
targetOneSlot.getBigStringFiles().writeBigStringBytes(entry.uuid(), entry.bucketIndexInner(),
        entry.keyHash(), contentBytes, entry.offset(), entry.length());
```

```java
// XGroup.java:1364-1384, existing short-string replay fanout
HashMap<Short, HashMap<String, CompressedValue>> groupedBySlot = new HashMap<>();
KeyLoader.decodeShortStringListFromBuf(slice, (keyHash, expireAt, seq, key, valueBytes) -> {
    var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
    var cv = CompressedValue.decode(valueBytes, Wal.keyBytes(key), keyHash);
    groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, cv);
});
...
targetOneSlot.put(key, bucketIndex, cv, true);
```

```java
// XGroup.java:1765-1769, incremental replay for non-async contents
if (!hasAsyncApply) {
    try {
        for (var content : decodedContents) {
            content.apply(slot, replPair);
        }
```

```java
// XFlush.java:85-89
public void apply(short slot, ReplPair replPair) {
    log.warn("Repl slave apply one slot flush, !!!, slot={}", slot);
    var oneSlot = localPersist.oneSlot(slot);
    oneSlot.flush();
    log.warn("Repl slave apply one slot flush done, !!!, slot={}", slot);
}
```

**Root cause:**

The existing-data replay path has a partial reshard behavior: it decodes keys from a master slot and re-hashes them using
the slave's current `ConfForGlobal.slotNumber`, then writes each key to the target slave slot. With a larger slave slot
count, one master slot's data can fan out to multiple slave slots.

Incremental catch-up does not apply the same rule to non-keyed slot-wide operations. `XGroup.s_catch_up(...)` calls
`content.apply(slot, replPair)` for decoded binlog contents that do not report asynchronous cross-slot work. `XFlush`
then flushes only `localPersist.oneSlot(slot)`, where `slot` is the master stream slot. It does not know which target
slave slots previously received keys from that stream during existing-data fanout.

**Impact:**

After bootstrap, suppose master slot `0` contains key `K`, and under the larger slave `slotNumber`, `K` belongs to slave
slot `N`. Existing-data replay writes `K` into slave slot `N`. If the master later appends an `XFlush` for slot `0`, the
slave incremental replay flushes only slave slot `0`. The copy of `K` in slave slot `N` survives even though the master
slot was flushed.

This violates Redis/Velo flush semantics on a larger-slot slave and can leave stale data visible after catch-up.

**Suggested fix direction:**

Do not allow `XFlush` through a mismatched-slot topology until there is explicit reshard replay support. In a real
reshard mode, `XFlush` needs either:

- a target-slot set derived from the same master-slot-to-slave-slot fanout mapping used by data replay, and it must flush
  all target slave slots before advancing the binlog offset; or
- a full-database flush representation when the source command semantics require clearing all slots.

The fix should include a regression that builds a `masterSlotNumber -> larger slave slotNumber` scenario where a key is
written to a different target slave slot, then an incremental flush proves the target slot is cleared before catch-up
progress is persisted.

## Non-bug observation: several existing-data paths already re-hash under the slave slot count

The following paths appear intentionally prepared for larger slave slot counts:

- `s_exists_wal`: re-hashes WAL keys with `BaseCommand.calcSlotByKeyHash(..., ConfForGlobal.slotNumber)` before
  `putToTargetWal(...)`.
- `s_exists_chunk_segments`: decodes segment entries and groups by target slave slot before `targetOneSlot.put(...)`.
- `s_exists_big_string`: maps big-string file ids by key hash to target slave slots before writing files.
- `s_exists_short_string`: decodes short strings and writes each key to its target slave slot.

These are useful building blocks, but they are not sufficient on their own because slot lifecycle/progress and
non-keyed incremental operations still assume a one-to-one master slot stream.

## AI agent 2 review (verification round)

Date: 2026-06-25
Reviewer: AI agent 2

Verdict: **Both bugs confirmed real.** Every cited file/line and code excerpt was checked against the current
source and matches. The two bugs are both gated on the same precondition — a slave configured with
`slotNumber > master slotNumber` (e.g. the `slave = master * 2` scale-up scenario). Under vanilla slot-to-slot
replication (equal `slotNumber`) neither bug triggers.

### Bug 1 — CONFIRMED (High)

Traced the full lifecycle and it holds:

- `ConfForSlot.slaveCanMatch` (`ConfForSlot.java:165-167`) rejects only `local slotNumber < master slotNumber`;
  `local >= master` is accepted. Confirmed.
- `LeaderSelector.doResetAsSlave` (`LeaderSelector.java:522-540`) loops `i` over the slave's own
  `ConfForGlobal.slotNumber` and calls `oneSlot.resetAsSlave(host, port)` on **every** local slot, including slots
  in `[masterSlotNumber, slaveSlotNumber)`. Confirmed.
- `OneSlot.resetAsSlave` (`OneSlot.java:2575-2599`) sets the slot `readonly=true`, `canRead=false`, `binlogOn=false`
  and calls `createReplPairAsSlave`, which (`ReplPair.java:442-443`) opens a `TcpClient` to the master and sends
  `hello` tagged with the **local** slot id.
- On the master, `XGroup.handleRepl` (`XGroup.java:255-258`) drops any frame whose `slotRemote >= master
  ConfForGlobal.slotNumber` with `Repl.emptyReply()` — before `hello`/`hi`/exists-fetch/catch-up can run. Confirmed.
- Additional verification the author did not cite: the **only** code path that flips a slave slot back to
  `canRead=true` is `XGroup.finishSlaveCatchUpApply` (`XGroup.java:1841-1848`), which is reachable only after
  `s_exists_all_done` (`XGroup.java:1496-1524`) → `catch_up`. For the extra slots that handshake is never completed,
  so `setCanRead(true)` is never called. The extra slots are therefore stuck `readonly + canRead=false` permanently.

Net: extra slots stay non-readable forever, while existing-data fanout from the lower master streams still writes
real data into them (see non-bug observation below). The slave-side `ReplPair` for each extra slot will also keep
re-attempting and receiving empty replies, so this is a permanent stuck state plus ongoing log noise, not a
transient race. Confirmed real.

### Bug 2 — CONFIRMED (High)

- Existing-data fanout re-hashes under the slave's `ConfForGlobal.slotNumber` and writes to the *inner* target slot,
  not the stream slot: chunk segments (`XGroup.java:999-1012` group, `1023-1033` write to `targetOneSlot`), big
  string (`1237-1259`, writes to `localPersist.oneSlot(entry.slotInner())`), short string (`1364-1384`). So master
  slot `0` legitimately fans out to multiple slave slots. Confirmed.
- Incremental catch-up applies non-async contents with the **stream** slot: `XGroup.java:1765-1769` calls
  `content.apply(slot, replPair)`.
- `XFlush` does not override `isApplyAsync` (default `false`, `BinlogContent.java:103-105`), so it always takes the
  synchronous path, and `XFlush.apply` (`XFlush.java:84-90`) flushes only `localPersist.oneSlot(slot)` — the single
  stream slot. It has no knowledge of the fanout target slots. Confirmed.

Net: after an `XFlush` for master slot `0`, the slave clears only slave slot `0`; fanned-out copies in the other
target slave slots survive. Stale data remains visible after catch-up. Confirmed real.

### Refinements / notes

- The four `s_exists_*` re-hash paths are deliberate, which shows partial intent to support larger slave slot
  counts. But lifecycle/progress (Bug 1) and non-keyed slot-wide ops like `XFlush` (Bug 2) were not carried through,
  so this is an **incomplete feature with real correctness holes**, not merely a theoretical concern. I agree with
  the author's framing.
- Severity: both are correctly rated High *within* the mismatched-slot topology. Real-world exposure is gated on an
  operator actually configuring `slave slotNumber != master slotNumber`, which today only the (incomplete) scale-up
  path would do.
- Recommended immediate action: adopt Bug 1's first suggested option — tighten `slaveCanMatch` (and/or
  `resetAsSlave`) to reject `slave slotNumber != master slotNumber` until a full reshard-replication mode exists.
  This single guard also neutralizes Bug 2, since `XFlush` can then never reach a mismatched-slot topology. The
  fuller fanout-aware fix (mapping master→slave target slots for both lifecycle progress and `XFlush`) should be
  scoped as part of dedicated reshard-replication work, not bolted onto vanilla slot-to-slot replay.

## Recommended next step for AI agent 2

Confirm whether the project intends to support `slave slotNumber > master slotNumber` today. If not, Bug 1 can be fixed
by tightening the compatibility check and adding a focused test around `ConfForSlot.slaveCanMatch(...)` plus
`LeaderSelector.resetAsSlave(...)` refusal. If yes, both bugs should be treated as part of a larger reshard-replication
implementation, with separate tests for lifecycle progress, fanout ordering, and non-keyed incremental operations.

## Fix Notes (implementation on branch `review/2n-replication`)

Date: 2026-06-25
Implemented by: AI agent (task-by-task TDD per `docs/plans/2026-06-25-master-n-slave-2n-replication.md`)

The project decided to support `slave slotNumber == master slotNumber * 2` (2N scale-up / shadow rebuild mode).
Both bugs are now fixed. Commits (in dependency order):

| Commit | Task | Summary |
|--------|------|---------|
| `0a7c40d5` | T1 | `slaveCanMatch` now accepts only `N` or `2N` (rejects any other ratio) |
| `a4770707` | T2 | `ConfForGlobal.masterSlotNumber` tracks the remote master's slot count; `LocalPersist.isAsSlaveScaleUp()` detects 2N mode; `checkMasterConfigMatch` returns `SlaveCheckValues`; lifecycle set/rollback/clear wired in `doResetAsSlave`/`doResetAsMaster` |
| `de920ce9` | T3 | `OneSlot.resetAsSlave(host, port, openReplStream)` overload; extra 2N slots skip repl stream creation (Bug 1 fix) |
| `d7f1f365` | T4 | Central, lock-serialized global read gate in `LocalPersist` (`publishStreamReadyAndRefreshGate`); all local slots become readable only when every stream slot is read-ready; teardown publishes `false` |
| `70ea1f89` | T5 | `XWalV`/`XBigStrings` `applyAsync` always return a real awaitable promise (shortcut removed); `s_catch_up` forces the async path in scale-up so the fetched-offset advances only after all writes land |
| `0bf29695` | T6 | `XFlush.apply` and `applyAsync` throw in scale-up mode (stuck-but-safe: stream stalls, gate stays closed, no data loss). Bug 2 fix — FLUSH is rejected until a cross-stream barrier exists |
| `f6466c4b` | T7 | `doResetAsMaster` promotes extra no-stream slots too (they were previously skipped as "already master") |
| `c6a412c3` | T8 | E2e regression: cross-slot XWalV routing, gate lifecycle, and flush rejection |

### Tests run

```bash
./gradlew :test --tests "io.velo.ConfForSlotTest"
./gradlew :test --tests "io.velo.repl.LeaderSelectorTest"
./gradlew :test --tests "io.velo.persist.OneSlotTest"
./gradlew :test --tests "io.velo.persist.LocalPersistTest"
./gradlew :test --tests "io.velo.command.XGroupTest"
./gradlew :test --tests "io.velo.repl.incremental.XWalVTest"
./gradlew :test --tests "io.velo.repl.incremental.XBigStringsTest"
./gradlew :test --tests "io.velo.repl.incremental.XFlushTest"
./gradlew :test --tests "io.velo.repl.ScaleUpReplicationTest"
```

All pass. JaCoCo coverage confirmed for every changed line/branch (see `scripts/jacoco_cover.py` output per task).

### Remaining out-of-scope items

- **Cross-stream flush barrier**: `FLUSHALL`/`FLUSHDB` on the master during a 2N rebuild stalls the affected stream (stuck-but-safe). Operators must rebuild during a flush-free window until a cross-stream flush barrier is implemented.
- **Downstream replication from a promoted 2N node**: extra slots `[N, 2N)` are promoted with data in key/chunk layers but no binlog history. A downstream slave cannot reconstruct those slots from the binlog tail.
- **General `N -> M` resharding** where `M != 2N` remains unsupported.
- **Cluster-enabled** (`clusterEnabled=true`) routing for 2N needs a separate design pass.
