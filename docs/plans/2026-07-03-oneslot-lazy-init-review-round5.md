# Review: OneSlot Lazy Init Implementation (Round 5)

**Plan under review:** `docs/plans/2026-07-03-oneslot-lazy-init.md`
**Reviewer:** AI agent (codebase verification)
**Date:** 2026-07-03
**Method:** Post-implementation review. Read the plan, then reviewed the three implementation
commits (`d32a5c70`, `18a810e8`, `3fc7bb1a`) against it. Ran focused test suites and inspected
sparse-array code paths that `checkClusterSlot` does not guard.

Unlike rounds 1–4 (which reviewed the *plan* before execution), this round reviews the *committed
code* and revisits the plan's design/audit assumptions in light of two bugs the implementation
shipped.

## Verdict

**Implementation is correct and matches the plan.** The core lazy-init design (volatile
copy-on-write publication, `synchronized` clone-modify-publish, bind-before-publish,
`initializedOneSlots()` null-safe loops) is implemented faithfully and the focused tests pass.

Two production bugs escaped the plan's Command Path Audit and were caught in review; both are now
fixed in `3fc7bb1a` with tests. Beyond those, five design/process gaps in the plan remain worth
addressing — two of them (**R5-1**, **R5-3**) before considering the feature done.

## Commits reviewed

| Commit | Scope |
|--------|-------|
| `d32a5c70` | feat: lazy-init only locally owned OneSlot at cluster startup (LocalPersist, MultiWorkerServer, TaskRunnable, LeaderSelector, ManageCommand, ClusterxCommand + tests) |
| `18a810e8` | fix: init local slot before persisting cluster ownership in setslot (P1); clear lazy-init cached fields in `resetForTest` (P2) |
| `3fc7bb1a` | fix: null-safe dyn-config loop and countkeysinslot for lazy slots (round-5 findings B1/B2) |

## What was verified correct

- **Copy-on-write publication** — `ensureOneSlotInitialized` clones → sets → publishes to the
  `volatile OneSlot[]`, with runtime binding *before* the publish and scheduler registration
  *after*, exactly as the plan specifies. `synchronized` serializes the RMW; readers hit the
  volatile snapshot lock-free.
- **Binding array indexing** — `bindOneSlotIfInitialized` indexes eventloop/handler/threadId with
  `slot % slotWorkerThreadIds.length`; verified all four runtime arrays (eventloop, handler,
  threadId, scheduleRunnable) are sized `slotWorkers` in `MultiWorkerServer`, so the shared modulus
  is consistent. Snowflake still uses `slot % netWorkers`, matching the original eager loop.
- **`localAssignedInnerSlots` boundary math** — `ceil(begin/step)*step` correctly maps owned
  client-slot ranges to inner slots for aligned ranges (walked through 8192–12287 → inner slot 2,
  and multi-slot ranges).
- **Null-safe loops** — the audited set (`doSthInSlots`, `walLazyRead`, `initCheck`, `warmUp`,
  `debugMode`, `currentThreadFirstOneSlot`, `cleanUp`/`cleanUpAsync`, both `LeaderSelector` reset
  loops, read-gate fan-out) all iterate `initializedOneSlots()`. `getInnerOneSlotByToClientSlot`
  now direct-indexes with bounds guards.
- **Both round-4/P1-P2 fixes** — setslot inits the local slot *before* mutating metadata (failed
  init cannot advertise an unservable slot); `resetForTest` clears the new cached fields to prevent
  cross-session leakage.

## Findings

### B1 — `ManageCommand.dynConfig` NPE on sparse array — FIXED

**File:** `dyn/src/io/velo/command/ManageCommand.groovy:944`
**Severity:** High (any cluster lazy deployment with unowned slots)

`manage dyn-config set` iterated the raw `localPersist.oneSlots()` array and called
`oneSlot.asyncCall(...)` on every entry with no null check. In cluster lazy mode the array is
sparse, so the first null entry NPEs and breaks the command.

**Fix (`3fc7bb1a`):** iterate `localPersist.initializedOneSlots()` instead. Covered by
`ManageCommandTest.'test dyn-config on sparse local persist skips uninitialized slots'` (4 slots,
only inner slot 2 owned).

### B2 — `ClusterxCommand.countkeysinslot` NPE on non-owned slot — FIXED

**File:** `dyn/src/io/velo/command/ClusterxCommand.groovy:384`
**Severity:** Medium (behavior regression + NPE)

`CLUSTER COUNTKEYSINSLOT <slot>` derives the inner slot from a **numeric argument** (only
bounds-checked to `0..16383`) and dereferenced `oneSlot(innerSlot).allKeyCount` with no null/
ownership check. Pre-lazy-init this returned 0 (empty initialized slot); under lazy init a slot
this node does not own is null → NPE.

**Fix (`3fc7bb1a`):** `if (oneSlot == null) return new IntegerReply(0)` — restores Redis "no keys
here" semantics. Covered by `ClusterxCommandTest.'test countkeysinslot returns 0 for uninitialized
slot'`.

### R5-1 (design, **recommended before done**) — empty-ownership fallback defeats the feature

**Plan:** Target Behavior bullet 3; `LocalPersist.initSlots(...)`.

The compatibility fallback eager-inits **all** slots whenever the local owned set is empty:

```java
if (ConfForGlobal.clusterEnabled && !localSlots.isEmpty()) { /* lazy */ }
else { /* eager init ALL slots */ }
```

This fires in exactly the scenarios the feature targets:

- A **brand-new node joining a cluster** has an empty `multiSlotRange` until slots migrate in →
  it eager-allocates all N slots (the 5.6–14 GiB the plan is trying to avoid) at the worst moment.
- A node that legitimately owns nothing (all slots on other shards) does the same.

Root cause: an empty owned set cannot distinguish "no metadata yet" from "metadata says I own
nothing." Recommendation: gate eager fallback on **`!ConfForGlobal.clusterEnabled` OR
`multiShard.mySelfShard() == null`** (truly no metadata). When metadata exists and says zero slots,
init zero and rely on the lazy migration-in path. The plan's "Open Review Question" dismissed a new
config knob, but the real gap is the empty-vs-absent ambiguity, not the knob.

### R5-2 (design, non-blocking) — memory is never reclaimed

"Physical slot removal is out of scope" is acceptable for v1, but the plan should state the
consequence plainly: because migration-*out* never frees a `OneSlot`, memory only grows. After
enough rebalancing a long-lived node converges back toward all-slots-initialized. The **startup**
saving is real; the **steady-state** saving is not. Suggest documenting this as a known limitation
and filing the drain/quiesce/unpublish/cleanup follow-up now so the memory cap is not assumed
permanent.

### R5-3 (process, **recommended**) — the audit method has a systematic blind spot

B1 and B2 escaped because the plan's Command Path Audit trusts `checkClusterSlot` for request paths
and manually lists non-request loops, but never names the class both bugs belong to: **commands
that derive a slot from a numeric argument rather than a key** (`asInnerSlotByToClientSlot(userArg)`),
which `checkClusterSlot` does not guard.

Swept the remaining `asInnerSlotByToClientSlot` call sites in commands:

| Site | Slot source | Safe? |
|------|-------------|-------|
| `ClusterxCommand.groovy:341` | iterates `mySelfShard.multiSlotRange` (owned) | Yes — owned ⇒ initialized |
| `ClusterxCommand.groovy:384` | numeric arg | **was B2**, now fixed |
| `ClusterxCommand.groovy:541` | `exportMigratingSlot` (owned, being exported) | Yes |
| `ClusterxCommand.groovy:1159` | numeric arg (setslot) | Yes — handled by P1 fix |
| `ManageCommand.groovy:944` | all-slots loop | **was B1**, now fixed |

Recommendation: add "arg-derived slot lookups" and "`firstOneSlot()`/`lastOneSlot()` dereferenced
without null check" as explicit audit categories in the plan, so this class is covered by design
rather than rediscovered by luck. The ~10 `firstOneSlot().slot()` call sites (XGroup:593/1559/1690,
FGroup:153, etc.) are safe only under the "node owns ≥1 slot" invariant — which R5-1's zero-owned
case violates.

### R5-4 (observability, non-blocking) — no signal that lazy init actually happened

Nothing logs or meters how many slots were lazily vs eagerly initialized. For a capacity feature
with a silent "accidentally went eager" failure mode (R5-1), a one-line startup log
(`lazy-init: initialized N of M slots`) plus a gauge are near-zero cost and let operators confirm
the feature works in production. Suggest adding to Task 4 or Task 7.

### R5-5 (minor)

- **`localAssignedInnerSlots` alignment:** `firstBoundary = ceil(begin/step)*step` silently *skips*
  an inner slot if a range boundary is misaligned to the block size. Cluster assigns aligned blocks
  today; a defensive assert or log-warn on misalignment turns a silent unreachable-data bug into a
  loud one.
- **Lock scope:** `ensureOneSlotInitialized` holds the `LocalPersist` monitor across `initFds()`
  (disk I/O) and large buffer allocation. Fine for rare migration calls; worth a comment that
  topology ops serialize behind slot I/O.

## Test / build status

- `./gradlew :test --tests io.velo.persist.LocalPersistTest --tests io.velo.task.TaskRunnableTest
  --tests io.velo.repl.LeaderSelectorTest` — PASS.
- `./gradlew :test --tests io.velo.command.ClusterxCommandTest --tests
  io.velo.command.ManageCommandTest` — PASS (both new tests and the two originally-affected tests
  `test countkeysinslot and getkeysinslot` / `test dict`). A transient first-run failure of those
  two pre-existing tests was traced to stale `/tmp/velo-data` state from the cluttered working tree
  (stray `meta_chunk_segment_*.dat`, `dump.rdb`, `hs_err_pid*.log`), not this change; it does not
  reproduce after cleanup. Recommend `git clean`-ing the working dir before the next full-suite run.

## Closing

The lazy-init feature is functionally complete and correct against the plan. The two shipped bugs
are fixed and tested. The remaining items are plan-level: **R5-1** (empty-ownership fallback) and
**R5-3** (audit blind spot) are worth resolving before calling the feature done; **R5-2**, **R5-4**,
and **R5-5** are documentation/observability/robustness improvements that can follow.
