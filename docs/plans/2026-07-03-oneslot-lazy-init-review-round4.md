# Review: OneSlot Lazy Init Implementation Plan (Round 4)

**Plan under review:** `docs/plans/2026-07-03-oneslot-lazy-init.md` (post-round-3 revision)
**Reviewer:** AI agent (codebase verification)
**Date:** 2026-07-03
**Method:** Re-verified round-3 findings; inspected `TaskRunnableTest.groovy` to confirm the
`CopyOnWriteArrayList` type change does not break existing tests.

## Verdict

**APPROVED for execution.** Both round-3 findings are correctly resolved, and a final
backward-compatibility check on the `CopyOnWriteArrayList` substitution found no breakage. Only
one optional, non-blocking test-quality note remains.

## Round-3 findings — resolution status

| # | Round-3 finding | Status | Evidence |
|---|-----------------|--------|----------|
| R3-N1 | `TaskRunnable` ArrayList registration race | **Resolved** | Task 4 Step 3 (L397-399) now specifies `CopyOnWriteArrayList<OneSlot>` with the race rationale ("Lazy registration can be triggered from a different worker than the target slot worker ... a plain `ArrayList` can race with `TaskRunnable.run()` iteration"). Step 3 (L440) adds the focused concurrency test requirement. Task 7 Step 2 (L635) adds `CopyOnWriteArrayList` task registration to the JaCoCo coverage list |
| R3-N2 | Task 1 cleanup NPE between Task 2/3 | **Resolved** | Task 2 Step 5 (L270-272) now folds `cleanUp()` and `cleanUpAsync()` null-safety into Task 2 with the explicit rationale ("the new sparse startup test calls `cleanUp()` from its Spock cleanup block"). Task 2 Step 6 (L282) updated expectation to "PASS for ... and Spock cleanup". Task 3 Step 1 list (L296-301) no longer lists `cleanUp()`/`cleanUpAsync()` |

### Backward-compatibility verification (new this round)

Inspected `src/test/groovy/io/velo/task/TaskRunnableTest.groovy` to confirm the
`ArrayList<OneSlot>` → `CopyOnWriteArrayList<OneSlot>` field-type change is safe:

- Existing tests only touch `oneSlots.size()` (L68) and call `chargeOneSlots(...)`. No
  `instanceof ArrayList`, no ArrayList-specific API, no direct `remove`/iterator mutation. ✓
- All three tests pass dense non-null arrays to `chargeOneSlots`, so the new null-skip branch is a
  no-op for them. ✓
- `CopyOnWriteArrayList` implements `List`, supports `size()`, `contains`, `add`, and for-each
  iteration — all the operations the plan uses. ✓

No existing test breaks from the type change.

## Optional, non-blocking note

**R4-O1 (optional) — Prefer a deterministic assertion for the new concurrency test.** Task 4 Step 3
(L440) asks for a test where "a setslot-style lazy initialization of a slot owned by a non-zero
worker registers with that worker while `TaskRunnable.run()` is active ... demonstrate no
`ConcurrentModificationException`." With `CopyOnWriteArrayList` a CME is structurally impossible, so
a stress-style test adds flakiness risk without proving much. A stronger, deterministic assertion:
register a slot into worker X's `TaskRunnable` from a different thread, then assert
`slotTaskRunnables[X].oneSlots.contains(oneSlot)` and that a concurrent `run()` tick completes
without exception. The plan's intent is fine; the executor should favor the deterministic form.

## Closing

Across four review rounds the plan converged from missing failover/read-gate null-safety and a
visibility race to a design that is:

- **Correct:** volatile copy-on-write publication establishes the needed happens-before; `synchronized`
  serializes the clone-modify-publish RMW; binding precedes publication.
- **Concurrency-safe:** the one runtime cross-thread mutation path (`TaskRunnable` registration) is
  now backed by `CopyOnWriteArrayList`.
- **Null-safe:** every non-request loop over slots (`LeaderSelector.resetAsMaster`/`resetAsSlave`,
  `publishStreamReadyAndRefreshGate`, all `LocalPersist` aggregates) iterates `initializedOneSlots()`.
- **Failure-atomic:** `initFds()` runs before the COW publish, so a failed init leaves no partial slot.
- **Compat-preserving:** non-cluster startup is unchanged; empty cluster metadata falls back to eager
  init; `oneSlot(short)` stays a raw lookup; existing `TaskRunnableTest` cases still pass.
- **TDD-disciplined:** each task is independently green (Task 2 folds in `cleanUp`/metrics
  null-safety), JaCoCo uses `nl -ba`-derived ranges, and coverage obligations are enumerated.

No blocking or medium issues remain. The plan is ready to execute.
