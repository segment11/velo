# Review: OneSlot Lazy Init Implementation Plan (Round 3)

**Plan under review:** `docs/plans/2026-07-03-oneslot-lazy-init.md` (post-round-2 revision)
**Reviewer:** AI agent (codebase verification)
**Date:** 2026-07-03
**Method:** Re-verified all round-2 findings; traced the lazy-registration call chain through command
routing (`ClusterxCommand.parseSlots`, `BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT`,
`MultiWorkerServer.handleRequest`) to `TaskRunnable.run()` to check thread affinity.

## Verdict

Round-2 findings are fully resolved. One **new medium-high** concurrency bug is now visible: lazy
scheduler registration mutates `TaskRunnable.oneSlots` (a plain `ArrayList`) from a thread that may
not be the slot's owning worker, racing with that worker's `run()` iteration. Fix is small
(`CopyOnWriteArrayList` or eventloop hop). **Approvable after R3-N1 is addressed.**

## Round-2 findings — resolution status

| # | Round-2 finding | Status | Evidence |
|---|-----------------|--------|----------|
| N1 | Task 2/4 helper signature + forward reference | **Resolved** | Task 2 Step 1 (L130-139) now declares all binding fields incl. `slotTaskRunnables`; Task 2 Step 2 (L191-214) defines both `bindOneSlotIfInitialized(@NotNull OneSlot)` and `registerOneSlotWithTaskRunnableIfInitialized(@NotNull OneSlot)` inline. Signature is consistently `OneSlot` everywhere. Task 2 is now self-contained and compiles |
| N2 | Bind-vs-publish ordering ambiguity | **Resolved** | Task 2 Step 1 (L143) states the invariant: "Runtime binding must happen before the volatile `oneSlots = newSlots` publication; scheduler registration may happen after publication." Code at L182 (bind) → L184-186 (publish) → L187 (register). Task 4 Step 1 (L387) restates it. Correct per JMM — readers see a bound slot |
| N3 | JaCoCo ranges stale + missing LeaderSelector | **Resolved** | Task 7 Step 2 (L627-635) now says use `nl -ba` post-edit ranges, adds a `LeaderSelector` jacoco line, and enumerates the exact branches to confirm |
| N4 | `@TestOnly` in-place mutators comment | **Resolved** | Task 2 Step 3 (L217-219) adds the exemption comment requirement |
| N5 | Task 4.5 test extraction work | **Resolved (acknowledged)** | Task 4.5 Step 1 (L470) now commits to "prefer direct; else extract package-visible helpers keeping lambda bodies unchanged." Clearer and bounded |

### Verification notes

- **Failure atomicity of `ensureOneSlotInitialized`:** `initFds()` runs before the COW publish, so an
  IOException leaves `oneSlots` without the partial slot — clean failure. ✓
- **Startup registration ordering:** `eventloopAsScheduler` → `chargeOneSlots` (null-safe) happens
  before `initSlotRuntimeBindings`/`initSlotTaskRunnables` in `onStart`; the latter's
  `registerOneSlotWithTaskRunnableIfInitialized` re-registers already-charged slots but is dedup'd
  by `addOneSlotIfCharged`'s `contains` check. No double-add. ✓
- **`localAssignedInnerSlots` step math** re-verified for slotNumber 4/8 and begin=0. ✓

---

## New findings

### R3-N1 (medium-high) — Lazy scheduler registration races with `TaskRunnable.run()` iteration

`TaskRunnable.oneSlots` is a plain `ArrayList<OneSlot>` (`TaskRunnable.java:35`, `final`, no
synchronization). Two writers mutate it:

1. `chargeOneSlots(...)` — startup only, completes before the first scheduled `run()` (the 1s
   `delay` in `eventloopAsScheduler`). Safe by ordering. **Pre-existing.**
2. `addOneSlotIfCharged(...)` — called at **runtime** from
   `ensureOneSlotInitialized` → `registerOneSlotWithTaskRunnableIfInitialized`
   (Task 2 Step 2, L205-214; Task 4 Step 3, L419-426). **Introduced by this plan.**

`run()` iterates the list with a for-each loop (`TaskRunnable.java:88`), whose try/catch wraps only
`oneSlot.doTask(loopCount)` (L89-93) — **not** the iterator's `hasNext()`/`next()`.

**The race is reachable.** `registerOneSlotWithTaskRunnableIfInitialized` targets
`slotTaskRunnables[oneSlot.slot() % len]` — the *target slot's* worker. But the command that triggers
lazy init need not run on that worker:

- `ClusterxCommand.setslot` parses to `SlotWithKeyHash.TO_FIX_FIRST_SLOT`
  (`ClusterxCommand.groovy:68`; `TO_FIX_FIRST_SLOT` = slot 0, `BaseCommand.java:579`) → routes to
  **worker 0** (`MultiWorkerServer.handleRequest`, `firstSlot % requestHandlerArray.length`).
  Task 5 Step 2 makes it call `localPersist.ensureOneSlotInitialized(innerSlot as short)`. If the
  target inner slot maps to a worker ≠ 0 (e.g. `netWorkers=4`, `innerSlot=3` → worker 3), then
  worker 0 mutates worker 3's `ArrayList` while worker 3's eventloop iterates it in `run()`.
- (`ManageCommand.migrateFrom` is safe: it parses to the target inner slot and runs on the target's
  own worker — same-thread mutation.)

**Impact:** `ArrayList.add` may resize via `System.arraycopy` (non-atomic) → torn read in the
iterator, or a `ConcurrentModificationException` on `next()` that escapes the inner try/catch and
propagates to the eventloop, breaking periodic task scheduling for that worker. Migration is exactly
when robustness matters.

**Required fix (pick one):**
- (a) Make `TaskRunnable.oneSlots` a `CopyOnWriteArrayList<OneSlot>`. Iterations (every 10ms) vastly
  outnumber mutations (only on lazy slot init), so copy cost is negligible and the for-each in
  `run()` becomes snapshot-safe. Simplest and most robust. **Recommended.**
- (b) Hop the registration onto the target eventloop:
  `slotWorkerEventloopArray[i].submit(() -> taskRunnable.addOneSlotIfCharged(oneSlot))` so the
  mutation runs on the same thread that iterates.

Either fix needs a focused concurrency test (e.g. a `setslot`-style lazy init of a slot owned by a
non-zero worker while that worker's `run()` is active) or, at minimum, a comment documenting the
chosen happens-before guarantee.

### R3-N2 (low-medium) — Task 1 test cleanup NPEs between Task 2 and Task 3

The Task 1 test's `cleanup:` block (plan L93-98) calls `localPersist.cleanUp()`. `cleanUp()` is made
null-safe only in **Task 3** (Step 1 list, L302), but Task 2 produces the sparse array. So when
Task 2 Step 6 runs the Task 1 test, the `then:` block passes while `cleanup:` throws NPE inside the
raw `for (var oneSlot : oneSlots)` loop in `cleanUp()` (`LocalPersist.java:561`). Spock fails an
iteration on cleanup exceptions, so Task 2 Step 6's "Expected: PASS" does not hold.

(Contrast: Task 2 Step 5 already folds `initSlotsAgainAfterMultiShardLoadedOrChanged` null-safety
into Task 2 for precisely this reason — because `initSlots` calls it. `cleanUp` is invoked from the
*test's* cleanup, not from production `initSlots`, so it was missed.)

**Fix (pick one):**
- Move `cleanUp()` / `cleanUpAsync()` null-safety into Task 2 (same rationale as Step 5), or
- Make the Task 1 test `cleanup:` iterate `localPersist.initializedOneSlots()` directly instead of
  calling the not-yet-null-safe `cleanUp()`.

---

## Summary

- **Round-2 blockers:** all resolved and verified.
- **New blocking:** none.
- **New medium-high:** R3-N1 — `TaskRunnable` registration race (CopyOnWriteArrayList or eventloop
  hop). Must address before execution.
- **New low-medium:** R3-N2 — transient Task 1 cleanup NPE; fold `cleanUp` null-safety into Task 2
  or adjust the test cleanup.

**Recommendation:** approve after R3-N1 is resolved (prefer `CopyOnWriteArrayList`). R3-N2 should be
folded in for cleanliness but is not blocking. The design is otherwise sound: volatile COW
publication is JMM-correct, failure atomicity holds, startup ordering is preserved, and all
previously missed loops (`LeaderSelector`, read-gate, aggregates) are now covered with tests.
