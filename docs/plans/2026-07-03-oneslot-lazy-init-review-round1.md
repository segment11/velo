# Review: OneSlot Lazy Init Implementation Plan (Round 1)

**Plan under review:** `docs/plans/2026-07-03-oneslot-lazy-init.md`
**Reviewer:** AI agent (codebase verification)
**Date:** 2026-07-03
**Method:** Every claim in the plan was verified against the current source (`LocalPersist.java`, `MultiWorkerServer.java`, `TaskRunnable.java`, `LeaderSelector.java`, `ClusterxCommand.groovy`, `ManageCommand.groovy`, `MultiShard.java`, `Shard.java`, `OneSlot.java`, `LocalPersistTest.groovy`).

## Verdict

The core design is sound and the plan follows repo TDD/JaCoCo conventions. However it is
**not safe to execute as-is**: it misses null-safety for the failover/replication loops, which run
in exactly the cluster scenario the plan enables. One blocking task must be added before approval.

## Status summary

| # | Severity | Area | Disposition |
|---|----------|------|-------------|
| 1 | Blocking | LeaderSelector `resetAsMaster`/`resetAsSlave` + `publishStreamReadyAndRefreshGate` NPE on sparse slots | Must add a task |
| 2 | Blocking | Cross-thread visibility race on `oneSlots[]` | Must fix design |
| 3 | Medium | No explicit audit/enumeration of `oneSlot(slot)` call sites | Add a section |
| 4 | Medium | Task 2 cannot pass green on its own | Merge into Task 2 |
| 5 | Medium | `firstOneSlot()` can legitimately return null when node owns slots | Add test case |
| 6 | Low | `localAssignedInnerSlots()` iterates full range | Optional |
| 7 | Info | Task 1 test mechanics verified correct | Confirmed |

---

## Blocking findings

### Finding 1 — Missed NPEs in failover & replication loops (the dominant cluster operation)

Task 3 enumerates the `LocalPersist` aggregate loops to make null-safe (`doSthInSlots`,
`walLazyRead`, `initCheck`, `warmUp`, `cleanUp`, etc.) but omits the one loop that lives inside
`LocalPersist` itself **and** all loops in `LeaderSelector`, which bypass `doSthInSlots` and iterate
raw over the full `ConfForGlobal.slotNumber` range. These fire precisely when a clustered node
(which, per the plan, owns only a slot subset) changes role — i.e. the primary use case for clusters.

- **`LeaderSelector.java:394-397` (`resetAsMaster`)** — called on every failover:
  ```java
  Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
  for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
      var oneSlot = localPersist.oneSlot((short) i);
      promises[i] = oneSlot.asyncRun(() -> { ... });   // NPE if oneSlot == null
  ```
- **`LeaderSelector.java:560-563` (`resetAsSlave`)** — called on every `SLAVEOF`/`resetAsSlave`:
  ```java
  Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
  for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
      var oneSlot = localPersist.oneSlot((short) i);
      promises[i] = oneSlot.asyncRun(() -> { ... });   // NPE if oneSlot == null
  ```
- **`LocalPersist.java:512-522` (`publishStreamReadyAndRefreshGate`)** — fans `canRead` to every
  slot from a repl stream thread:
  ```java
  for (short s = 0; s < ConfForGlobal.slotNumber; s++) {
      var ts = oneSlot(s);
      ts.asyncRun(() -> { ... });                        // NPE if oneSlot == null
  ```

All three must switch to `initializedOneSlots()` (or an explicit `if (ts == null) continue;`) and
gain focused tests. The plan currently guarantees a crash the first time a sparse node fails over or
replicates.

**Required:** insert a new task between current Task 4 and Task 5 —
*"Make LeaderSelector reset loops and `publishStreamReadyAndRefreshGate` null-safe"* — with tests
covering the sparse-array case (e.g. a 4-slot node owning only slot 2 calling `resetAsMaster`).

### Finding 2 — Cross-thread visibility race on `oneSlots[]`

`ensureOneSlotInitialized(short slot)` is declared `synchronized` (plan Task 2 Step 2), but every
reader of a slot is a lock-free direct array access:

- `LocalPersist.oneSlot(short)` → `return oneSlots[slot];` (`LocalPersist.java:86-88`)
- `LeaderSelector` loops (Finding 1)
- `publishStreamReadyAndRefreshGate` (Finding 1)
- All ~100 command-path call sites.

A `synchronized` block on the **writer** alone establishes happens-before only with readers that
take the same lock. Lock-free readers on another thread may observe a stale `null` element even after
the writer has published the `OneSlot`.

In practice the per-slot run-to-completion model limits cross-thread reads for the *request* path
(each command touches only its owning worker's slot), but the missed loops in Finding 1 deliberately
touch foreign slots from non-owning threads via `asyncRun`, so the race is reachable, not
theoretical.

**Required:** either
- republish the whole array through a `volatile` reference (swap a new array on lazy create — but
  this conflicts with the plan's "stable length" invariant), **or**
- use `VarHandle`/`AtomicReferenceArray` for element publication, **or**
- document a stronger invariant (e.g. lazy creation happens-before any foreign-thread access because
  it is serialized through `ensureOneSlotInitialized` and foreign accessors always go through
  `asyncRun` on the owning eventloop, which has its own happens-before).

Pick one and state it in the plan; the current "just make the writer `synchronized`" is insufficient.

---

## Medium findings

### Finding 3 — No explicit audit of `oneSlot(slot)` call sites

A grep for `oneSlots|oneSlot\(` returns ~100 call sites: `BaseCommand` (7+), `XGroup` (~20),
`ZGroup`, `SGroup`, `MGroup`, `RGroup`, `CGroup`, repl/incremental (`XWalV`, `XBigStrings`,
`XFlush`), `MyRDBVisitorEventListener`, jmh benchmarks, etc.

The MOVED-redirection argument **does** make the request path safe: `MultiWorkerServer.checkClusterSlot`
(`MultiWorkerServer.java:341-391`) returns a MOVED error before dispatch if the slot is not owned by
this shard, so command handlers only ever touch owned (and therefore initialized) slots. Repl/
incremental handlers run on the owning worker of an owned slot, also safe.

But this reasoning is load-bearing for the whole design and must be an explicit **section** in the
plan, not an omission, and the non-request loops (LeaderSelector, `publishStreamReadyAndRefreshGate`,
jmh) enumerated as in/out of scope. Right now a reader has no way to know whether the omissions are
deliberate.

### Finding 4 — Task 2 cannot pass green on its own

Task 2 changes `initSlots(...)` to emit a sparse array. The very next line of `initSlots` calls
`initSlotsAgainAfterMultiShardLoadedOrChanged()` (`LocalPersist.java:198`), whose body
(`LocalPersist.java:204-207`) is:

```java
for (var oneSlot : oneSlots) {
    oneSlot.clearGlobalMetricsCollect();   // NPE on null entries
}
```

So Task 2's `initSlots` NPEs immediately. Task 2 Step 4 acknowledges this ("If it fails from startup
loops, continue to Task 3"), but that breaks the per-task red→green property the plan advertises at
the top ("REQUIRED SUB-SKILL: executing-plans").

**Fix:** fold the `initSlotsAgainAfterMultiShardLoadedOrChanged` null-safety change into Task 2, or
make Task 2 keep the array dense and defer sparseness to Task 3.

### Finding 5 — `firstOneSlot()` can legitimately return null when the node owns slots

In cluster mode `firstOneSlot()` (`LocalPersist.java:360-372`) returns the min client-slot across
**all** shards via `multiShard.firstToClientSlot()` (`MultiShard.java:249-263`), not just the local
shard. If a peer shard owns a lower client-slot range, `getInnerOneSlotByToClientSlot(lowerSlot)`
maps to an inner slot this node does not own → `oneSlots[lowerInnerSlot] == null`.

The current code NPEs there (`getInnerOneSlotByToClientSlot` does `for (oneSlot : oneSlots)
oneSlot.slot()`); the plan's Task 3 Step 3 direct-index fix correctly returns `null` instead (good),
and Task 5's ManageCommand control-slot pattern already null-checks (good). Add a test case:

> "Node owns slot 2 only; a second shard owns client slots 0–4095; assert `firstOneSlot() == null`
> and that downstream code (e.g. ManageCommand) handles it."

---

## Low / nits

### Finding 6 — `localAssignedInnerSlots()` iterates the full client-slot range

The helper (plan Task 2 Step 3) walks `range.getBegin()..range.getEnd()` filtering by
`MultiShard.isToClientSlotSkip`. It is correct, but for a shard owning half of 16384 slots that is
8192 iterations at startup. Stepping by `step = MultiShard.TO_CLIENT_SLOT_NUMBER /
ConfForGlobal.slotNumber` would be O(slots) instead. Negligible cost either way; optional.

### Finding 7 — Task 1 test mechanics verified correct

- `updateClusterVersion` → `saveMeta` (`MultiShard.java:190-195`) persists the modified shard range,
  so the `new MultiShard(persistDir)` constructed inside `initSlots` reloads it. ✓
- `8192–12287` with `slotNumber=4` → `16384/4 = 4096` per inner slot → inner slot 2. ✓
- `isToClientSlotSkip` filters non-boundary slots; only 8192 survives → inner slot 2 added. ✓
- Test assertions (`isOneSlotInitialized` true for 2, false for 0/1/3; `firstOneSlot().slot() == 2`)
  all hold given the Task 3 `getInnerOneSlotByToClientSlot` direct-index fix. ✓
- Cleanup resets `ConfForGlobal.slotNumber = 1`, deletes `Consts.persistDir` — consistent with
  existing `LocalPersistTest` house style. ✓

### Finding 8 — Misc

- `addOneSlotIfCharged` (plan Task 4 Step 3) uses `ArrayList.contains` (O(n)) — fine for small worker
  counts.
- `ensureOneSlotInitialized` binds `threadIdProtectedForSafe` inside `bindOneSlotIfInitialized` after
  `initFds()`. Eager init binds thread ids in `onStart` after `initSlots`; the plan preserves this
  and lazy slots created mid-run find `slotWorkerThreadIds` already populated. OK.
- **Open Review Question** (new config knob?): agree with the recommended v1 answer — no knob, reuse
  persisted cluster metadata; avoids a second source of topology truth.

---

## Suggested changes to the plan (concrete)

1. **Add Task 4.5 (new):** *Null-safe `LeaderSelector.resetAsMaster` / `resetAsSlave` and
   `LocalPersist.publishStreamReadyAndRefreshGate`* — switch to `initializedOneSlots()` or explicit
   null skip, with tests for a sparse node failing over and replicating. Gate Task 5 on this.
2. **Resolve Finding 2:** pick a publication strategy for `oneSlots[]` elements and document it.
3. **Add a section** "Why command-path `oneSlot(slot)` is safe" citing
   `MultiWorkerServer.checkClusterSlot` and enumerating the out-of-scope non-request loops.
4. **Merge** the `initSlotsAgainAfterMultiShardLoadedOrChanged` null-safety change into Task 2 so
   each task is independently green.
5. **Add the Finding 5 test case** to Task 6.

Once 1–2 are addressed the plan is approvable; 3–5 are strongly recommended for completeness.
