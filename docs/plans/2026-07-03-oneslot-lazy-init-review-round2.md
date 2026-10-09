# Review: OneSlot Lazy Init Implementation Plan (Round 2)

**Plan under review:** `docs/plans/2026-07-03-oneslot-lazy-init.md` (post-round-1 revision)
**Reviewer:** AI agent (codebase verification)
**Date:** 2026-07-03
**Method:** Re-verified every round-1 finding against the updated plan text and current source
(`LocalPersist.java`, `LeaderSelector.java`, `MultiShard.java`, `ClusterxCommand.groovy`,
`ManageCommand.groovy`, `LocalPersistTest.groovy`).

## Verdict

Round-1 blockers are resolved correctly. The plan introduces one **new medium-high** issue: Task 2's
`ensureOneSlotInitialized(...)` body calls helpers whose signatures/declarations only appear in
Task 4, and the two tasks disagree on the binding signature and call placement. This breaks the
"each task independently green" property and will trip a literal executor. It is a wording/spec
reconciliation, not a design rework — **approvable after N1/N2 are fixed.**

## Round-1 findings — resolution status

| # | Round-1 finding | Status | Evidence in updated plan |
|---|-----------------|--------|--------------------------|
| 1 | LeaderSelector + read-gate NPE | **Resolved** | New "Command Path Audit" § (L30-40) lists all three loops; new Task 4.5 (L442-490) covers `resetAsMaster`, `resetAsSlave`, `publishStreamReadyAndRefreshGate` with tests |
| 2 | Cross-thread visibility race | **Resolved** | Architecture (L7) + Task 2 Step 1 (L128) make `oneSlots` volatile; Task 2 Step 2 (L178-184) implements copy-on-write (clone → set → volatile publish). `synchronized` serializes clone-modify-publish; volatile write publishes all pre-write element state. Verified correct per JMM |
| 3 | No call-site audit | **Resolved** | "Command Path Audit" § (L30-40) states the MOVED argument and enumerates non-request loops |
| 4 | Task 2 not independently green | **Resolved** | Task 2 Step 4 (L237-239) folds `initSlotsAgainAfterMultiShardLoadedOrChanged` null-safety into Task 2 |
| 5 | `firstOneSlot()` null case | **Resolved** | Task 6 Step 2 (L582-584) adds the peer-owns-lower-range test |
| 6 | `localAssignedInnerSlots` iteration | **Resolved (improved)** | Task 2 Step 3 (L211-216) now steps by `step = 16384/slotNumber` with ceiling-boundary math. Math verified for slotNumber 4/8, begin=0, partial ranges |

### Verification notes

- **Volatile COW correctness:** a volatile read of `oneSlots` in `oneSlot(short)` /
  `initializedOneSlots()` synchronizes-with the volatile write in `ensureOneSlotInitialized`, and
  that write is preceded (program order) by `initFds()`, debug flag, and `bindOneSlotIfInitialized`,
  so readers observe a fully constructed slot. ✓
- **`synchronized` + COW necessity:** without the monitor, two concurrent `ensureOneSlotInitialized`
  calls could clone the same base and one slot would be lost on publish. The monitor serializes the
  RMW; the volatile publishes the result. Combination is correct. ✓
- **`localAssignedInnerSlots` math** (L211-216): for slotNumber=4, range 8192-12287 →
  `firstBoundary = ((8192+4095)/4096)*4096 = 8192`, loop yields {8192} → inner slot 2. ✓
  Edge begin=0 → `((0+4095)/4096)*4096 = 0`. ✓

---

## New findings introduced by the revision

### N1 (medium-high) — Task 2 references helpers not declared until Task 4 (signature + ordering defect)

Task 2 Step 2 (L163-185) shows `ensureOneSlotInitialized(...)` calling:

```java
bindOneSlotIfInitialized(oneSlot);              // L178 — OneSlot argument
...
registerOneSlotWithTaskRunnableIfInitialized(oneSlot);  // L183
```

But:

1. **Signature mismatch.** Task 4 Step 1 (L365-378) defines
   `private void bindOneSlotIfInitialized(short slot)` — a **`short`** argument, not `OneSlot`. The
   two tasks specify different signatures for the same helper. `initSlotRuntimeBindings` (L356)
   also calls it with a `short`.
2. **Forward reference.** `registerOneSlotWithTaskRunnableIfInitialized(oneSlot)` and the
   `TaskRunnable[]` storage it needs are only introduced in Task 4 Step 3 ("Recommended v1: store
   `TaskRunnable[]` ... through a new `initSlotTaskRunnables(...)`"). Task 2 executes first and
   will not compile against undeclared symbols, or the executor must invent stubs — defeating the
   stated red→green-per-task goal.

**Required reconciliation (pick one):**
- (a) Move the declarations of `bindOneSlotIfInitialized(OneSlot)`,
  `registerOneSlotWithTaskRunnableIfInitialized(OneSlot)`, the `TaskRunnable[]` field, and
  `initSlotTaskRunnables(...)` into **Task 2** as no-op-safe stubs (null guards), so Task 2
  compiles and Task 4 only fills in the wiring; **or**
- (b) Split `ensureOneSlotInitialized` so Task 2 does construct → initFds → debug → publish only,
  and Task 4 inserts the bind + register calls (with a note that Task 2's lazy slots are unbound
  until Task 4 lands — acceptable since `initSlotRuntimeBindings` rebinds on startup).

Option (a) is cleaner and keeps the per-task green property intact.

### N2 (medium) — Binding placement ambiguity vs. publication ordering

Task 2 Step 2 orders the bind **before** the volatile publish (L178 bind → L180-182 clone+publish).
That is the **correct** order: readers acquiring the slot via the volatile array reference are
guaranteed to see `threadIdProtectedForSafe`, eventloop, and request-handler already set.

Task 4 Step 1 (L381) says: *"Call `bindOneSlotIfInitialized(slot)` at the end of
`ensureOneSlotInitialized(...)`."* "At the end" reads as **after** the publish (and after
`registerOneSlotWithTaskRunnableIfInitialized` at L183). If implemented literally, a reader could
observe a published-but-unbound slot (`threadIdProtectedForSafe == 0`), which breaks
`currentThreadFirstOneSlot()` thread-id matching and any code that uses the slot's eventloop before
the late bind lands.

**Required:** reconcile the wording — bind must occur **before** `oneSlots = newSlots;` (the
volatile publish). State this invariant explicitly.

### N3 (low) — JaCoCo line ranges will be stale; LeaderSelector not covered

Task 7 Step 2 (L618-619) checks:
```
python3 scripts/jacoco_cover.py io.velo.persist.LocalPersist 178 430 --src
python3 scripts/jacoco_cover.py io.velo.task.TaskRunnable 42 70 --src
```
After adding `initializedOneSlots`, `ensureOneSlotInitialized`, `localAssignedInnerSlots`,
`bindOneSlotIfInitialized`, `initSlotRuntimeBindings`, `initSlotTaskRunnables`, etc., the
178-430 band will no longer map to the changed code. Also Task 4.5 edits `LeaderSelector` but no
`jacoco_cover.py io.velo.repl.LeaderSelector ...` line is included.

**Suggested:** replace fixed ranges with method/branch guidance ("confirm the lazy-init, COW
publish, null-skip, and `initializedOneSlots()` branches are hit") and add a `LeaderSelector`
coverage check for the two reset loops.

### N4 (low) — `@TestOnly` in-place mutators now violate the COW discipline

`addOneSlot` and `addOneSlotForTest2` (`LocalPersist.java:140-147`, `154-165`) do
`oneSlots[slot] = oneSlot;` — an in-place element write, not a volatile republish. With the field
now volatile, this is technically a torn publication. These are `@TestOnly` and single-threaded, so
there is no real race, but future readers may copy the pattern into production code.

**Suggested:** add a one-line comment on both helpers noting they are exempt from the COW rule
because they are single-threaded test setup, or convert them to republish for consistency.

### N5 (low) — Task 4.5 test-extraction work is real and unestimated

Task 4.5 Step 1 hedges: *"If LeaderSelector setup is too heavy for a direct unit test, extract
package-visible helpers that build the promise list from `initializedOneSlots()` and test those
helpers."* Extracting those helpers from `resetAsMaster`/`resetAsSlave` (each ~40 lines, with
ReplPair/ZK coupling) is non-trivial refactoring not reflected in the task sizing. The hedge is
acceptable, but the plan should either commit to the extraction as an explicit sub-step or confirm
`LeaderSelectorTest.groovy` can drive the path directly.

### N6 (info) — Partial inner-slot range falls back to eager init (safe)

`localAssignedInnerSlots` only yields a slot when a client-slot boundary falls inside the range.
A manually-edited `nodes.json` with a sub-boundary range (e.g. client slots 5000-6000 with
slotNumber=8) yields an empty set → fallback eager-inits all slots. This is a safe
over-initialization, not a correctness bug, and normal cluster commands (`setslot`,
`ClusterxCommand.groovy:1155-1161`) always assign full inner-slot blocks. No action needed; noted
for completeness.

---

## Summary

- **Round-1 blockers:** all resolved and verified.
- **New blocking:** none.
- **New medium-high:** N1 (Task 2/4 helper signature + forward reference) and N2 (bind-vs-publish
  ordering wording) — must be reconciled before execution; both are spec/wording fixes.
- **New low:** N3 (JaCoCo ranges + missing LeaderSelector check), N4 (test-helper comment),
  N5 (Task 4.5 extraction sizing).

**Recommendation:** approve after N1 and N2 are reconciled (prefer N1 option (a): declare the
helpers as null-guarded stubs in Task 2). N3-N5 can be addressed during execution.
