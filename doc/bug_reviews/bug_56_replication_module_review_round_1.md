# Replication Module Bug Review — Scale-Up Promotion & Reset (Round 1)

Date: 2026-06-30
Author: AI agent 1

Scope: `LeaderSelector` promotion/reset logic and `XGroup` catch-up offset handling discovered during review of the
master-N-to-slave-2N scale-up replication implementation (`docs/plans/2026-06-25-master-n-slave-2n-replication.md`).
The bugs affect both equal-slot and scale-up modes. No production code was changed in this round.

Design context:

- Replication role transitions (`resetAsSlave`, `resetAsMaster`) are orchestrated by `LeaderSelector` and run per-slot
  callbacks dispatched through parallel `Promises.all` (`doc/design/09_replication_design.md`).
- Scale-up replication (slave 2N, master N) adds a global read gate and extra-slot promotion, but the core
  promotion/reset paths are shared with equal-slot mode (`doc/design/18_scale_up_design.md`).

## Bug 1: `doResetAsMaster` skips promotion of stream slots whose repl pair was torn down

**Severity:** High

**AI agent 2 status:** Confirmed. Fixed in this change set.

**Files:**

- `src/main/java/io/velo/repl/LeaderSelector.java:419-438`
- `src/main/java/io/velo/persist/OneSlot.java:431-459`

**Code excerpt (LeaderSelector.java:419-438):**

```java
var isSelfSlave = oneSlot.removeReplPairAsSlave();
if (isSelfSlave) {
    oneSlot.resetAsMaster();
    resetAsMasterCount = 0;
} else if (localPersist.isAsSlaveScaleUp()
        && oneSlot.slot() >= ConfForGlobal.masterSlotNumber) {
    // 2N scale-up extra slot: no slave ReplPair (data arrived via fan-out),
    // but it IS in slave state (readonly, canRead=false). Promote it too.
    log.warn("Repl promote scale-up extra slot to master (no binlog history), slot={}", oneSlot.slot());
    oneSlot.resetAsMaster();
    resetAsMasterCount = 0;
} else {
    resetAsMasterCount++;
    if (resetAsMasterCount % 100 == 0) {
        log.info("Repl reset as master, is already master, do nothing, slot={}", oneSlot.slot());
    }
}
```

**Root cause:**

The promotion decision is gated entirely on whether a slave `ReplPair` exists:

1. `removeReplPairAsSlave()` iterates `replPairs`, sends `bye`, and returns `true` only if it found a slave-side
   `ReplPair` (`OneSlot.java:431-446`).
2. If a stream slot's repl pair was already torn down before promotion (e.g. TCP disconnect removed it from
   `replPairs`), `removeReplPairAsSlave()` returns `false`.
3. The `else if` branch only targets scale-up **extra** slots (`slot >= masterSlotNumber`). For stream slots
   (`slot < masterSlotNumber`), this condition is false.
4. The code falls through to the `else` branch which logs "already master, do nothing" — but the slot is still in
   **slave state**: `readonly=true`, `canRead=false`, `binlogOn=false` (set during the original `resetAsSlave`).

The slot is silently left in slave state, unable to serve reads.

**Impact:**

In equal-slot mode: a single stream slot left in slave state means the entire node is partly unreadable after
promotion. In scale-up mode: stream slots stuck as slaves while extra slots are promoted, producing a split-mode
node that silently serves partial data.

Trigger scenario: network partition tears down a stream slot's TCP repl connection, then Sentinel issues a forced
promotion (`resetAsMaster(force=true)`). The check at line 410 (`!force && !canResetSelfAsMasterNow`) is bypassed
by `force=true`, but the repl-pair-existence gate at line 419 still fails.

**Suggested fix direction:**

In the `else` branch (or as a new `else if`), check `oneSlot.isReadonly()` or `!oneSlot.isCanRead()` — if the slot
is still in slave state but has no repl pair, call `oneSlot.resetAsMaster()` anyway:

```java
} else if (oneSlot.isReadonly()) {
    log.warn("Repl promote stream slot whose repl pair was already torn down, slot={}", oneSlot.slot());
    oneSlot.resetAsMaster();
    resetAsMasterCount = 0;
} else {
    // truly already master
    resetAsMasterCount++;
    ...
}
```

---

## Bug 2: Same-master `doResetAsSlave` early return skips per-slot `canRead` reset

**Severity:** Medium

**AI agent 2 status:** Confirmed, with the impact narrowed to stale read visibility; the skipped binlog/offset
reset side effects are expected to be corrected by the ongoing catch-up loop. Fixed in this change set.

**Files:**

- `src/main/java/io/velo/repl/LeaderSelector.java:558-572`

**Code excerpt (LeaderSelector.java:558-572):**

```java
var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
if (replPairAsSlave != null) {
    if (replPairAsSlave.getHost().equals(host) && replPairAsSlave.getPort() == port) {
        log.debug("Repl old repl pair as slave is same as new master, slot={}", oneSlot.slot());
        return;  // <-- skips resetAsSlave entirely
    } else {
        oneSlot.removeReplPairAsSlave();
    }
}

var masterSlotNumberForStream = ConfForGlobal.masterSlotNumber > 0
        ? ConfForGlobal.masterSlotNumber : ConfForGlobal.slotNumber;
boolean openReplStream = oneSlot.slot() < masterSlotNumberForStream;
oneSlot.resetAsSlave(host, port, openReplStream);
```

**Root cause:**

When a slave is reset to the **same** master it was already replicating from, the per-slot loop returns early
(line 563) without calling `oneSlot.resetAsSlave()`. This is an optimisation to avoid tearing down and
re-creating the repl pair for an unchanged master.

However, `oneSlot.resetAsSlave()` also performs side effects that this early return skips:

- `setCanRead(false)` (`OneSlot.java:2652-2653`)
- `metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset()` (`OneSlot.java:2637`)
- `binlog.moveToNextSegment()` (`OneSlot.java:2640`)
- `setBinlogOn(false)` (`OneSlot.java:2658`)

The upstream caller (`doResetAsSlave`) does reset the **global** state (`masterSlotNumber`, `resetScaleUpReadGate`),
but the **per-slot** `canRead` flag is preserved from the previous session.

**Impact:**

In scale-up mode: the global read gate was already reset, so any stale `canRead=true` will be overwritten when
the gate reopens. The window between gate-reset and gate-reopen is brief.

In **equal-slot mode** (no gate): the per-slot `canRead` persists until the next `finishSlaveCatchUpApply`
overwrites it. If the slot was previously caught up (`canRead=true`), the slot serves reads based on stale
state between the reset and the next catch_up completion.

The stale `binlogOff` and `metaChunkSegmentIndex` offset are corrected by the ongoing catch_up loop
(which writes the correct binlog state), so these are not correctness concerns — only `canRead` is material.

Trigger scenario: Sentinel re-issues `SLAVEOF` for the current master (redundant demotion). Rare in production.

**Suggested fix direction:**

Option A: After the early return, explicitly call `oneSlot.setCanRead(false)` to clear the stale flag
(smallest change).

Option B: Remove the early return and always call `resetAsSlave` — let the existing `removeReplPairAsSlave`
branch handle the teardown before re-creating. Slightly higher churn but no special case.

---

## Bug 3: `finishSlaveCatchUpApply` can advance `replPair` offset then throw, leaving split state

**Severity:** Low


**AI agent 2 status:** Confirmed as a low-severity split-state hazard on the exceptional invariant-failure path.

**Files:**

- `src/main/java/io/velo/command/XGroup.java:1877-1893`

**Code excerpt (XGroup.java:1877-1894):**

```java
replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(
    new Binlog.FileIndexAndOffset(fetchedFileIndex, fetchedOffset + readSegmentLength));

// ... gate / canRead logic ...

var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
if (readSegmentLength != binlogOneSegmentLength) {
    throw new IllegalStateException("Repl slave handle error: read segment length="
        + readSegmentLength + " is not equal to binlog one segment length=" + binlogOneSegmentLength);
}
```

**Root cause:**

Line 1877 mutates the in-memory `replPair` offset **before** the consistency guard at line 1891-1893.
If the guard fires (invariant violation: `readSegmentLength != binlogOneSegmentLength`), the method throws
`IllegalStateException`. The in-memory `replPair` has the advanced offset, but `metaChunkSegmentIndex`
(durable state) was **not** updated — the `setMasterBinlogFileIndexAndOffset` calls at lines 1882-1883
and 1901-1902 come after the guard.

The caller in the sync path (`s_catch_up` at line 1796-1798) catches the exception and returns an error
reply. In the async path (lines 1832-1834), the exception is caught at lines 1835-1838. In both cases
the error is surfaced, but `replPair` is left in a split state relative to `metaChunkSegmentIndex`.

**Impact:**

The invariant should never fire in practice — it checks basic encoding consistency. If it does fire,
the next catch_up iteration uses the advanced `replPair` offset, potentially skipping binlog data.
Crash recovery would use the correct, unadvanced `metaChunkSegmentIndex` offset, so the data would
eventually be replayed. No permanent data loss, but a temporary inconsistency.

**Suggested fix direction:**

Reorder: move the guard check **before** line 1877, or move `replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset`
**after** the guard. The guard only depends on `readSegmentLength` and `binlogOneSegmentLength`, neither of
which is mutated by lines 1850-1877, so reordering is safe.

---

## Bug 4: Synchronous exception in role transition drops callback notification

**Severity:** Low

**AI agent 2 status:** Confirmed for synchronous `RuntimeException` paths after transition state is changed and
before `Promises.all(...).whenComplete` is installed. Fixed in this change set.

**Files:**

- `src/main/java/io/velo/repl/LeaderSelector.java:468-472, 598-602`

**Code excerpt (LeaderSelector.java:468-472):**

```java
} catch (RuntimeException e) {
    ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
    MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
    throw e;
}
```

**Root cause:**

Both `doResetAsMaster` and `doResetAsSlave` wrap their main logic in `try { ... } catch (RuntimeException e)`.
The catch blocks restore failover state and `masterSlotNumber`, then **re-throw** the exception without
calling `callback.accept(e)`.

The callback is supposed to notify the caller (typically Sentinel or a cluster manager) of the transition
outcome. If a synchronous exception occurs (e.g. `localPersist.oneSlot()` returns null, `asyncRun` fails),
the callback is silently lost. The caller hangs or assumes success.

The async failure path (`Promises.all(...).whenComplete`) does correctly call `callback.accept(e)`.

**Impact:**

Pre-existing issue, not introduced by scale-up changes. Requires a synchronous failure in the setup phase
(before `Promises.all` is reached) to trigger. Callers that use the callback for orchestration (Sentinel
failover state machine) would not observe the failure. The failover state field
(`MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState`) is correctly restored, so polling-based
observers see the reversion.

**Suggested fix direction:**

Add `callback.accept(e)` before `throw e` in both catch blocks. This is a one-line change per catch block.

---

## Summary

| # | Severity | Area | Mode affected | Root cause |
|---|----------|------|---------------|------------|
| 1 | High | `doResetAsMaster` | Both | Torn-down repl pair bypasses promotion gate |
| 2 | Medium | `doResetAsSlave` | Both | Same-master early return skips `canRead=false` |
| 3 | Low | `finishSlaveCatchUpApply` | Both | Offset written before consistency guard |
| 4 | Low | `doResetAs{Master,Slave}` | Both | Sync exception drops callback |

All bugs are pre-existing in the role transition paths and became visible during scale-up implementation
review. Bugs 1 and 2 are the most actionable; bugs 3 and 4 are low-severity but worth addressing for
robustness.

## AI Agent 2 Verification - 2026-06-30

Reviewed against the current workspace source and the replication/scale-up design docs (`doc/design/09_replication_design.md`, `doc/design/18_scale_up_design.md`). The review findings are confirmed.

| # | AI agent 2 status | Notes |
|---|-------------------|-------|
| 1 | **Confirmed** | `LeaderSelector.doResetAsMaster()` still promotes stream slots only when `removeReplPairAsSlave()` returns true. If the slave-side pair is already absent, a stream slot (`slot < masterSlotNumber`) falls through as "already master" even though `OneSlot.resetAsSlave()` may have left it `readonly=true` and `canRead=false`. |
| 2 | **Confirmed** | `LeaderSelector.doResetAsSlave()` still returns early for an unchanged master before calling `OneSlot.resetAsSlave()`. That skips the per-slot `setCanRead(false)` reset; in equal-slot mode there is no scale-up read gate to mask the stale readable state. |
| 3 | **Confirmed** | `XGroup.finishSlaveCatchUpApply()` still advances `replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(...)` before checking `readSegmentLength != binlogOneSegmentLength`, while the durable `metaChunkSegmentIndex` update happens later. If the guard throws, in-memory and durable offsets diverge. |
| 4 | **Confirmed** | The synchronous catch blocks in `LeaderSelector.doResetAsMaster()` and `doResetAsSlave()` restore state and rethrow without invoking `callback.accept(e)`. Async failures do notify through `whenComplete`, so this is limited to synchronous setup failures. |

Confirmed bugs to fix, in priority order:

1. Bug 1: promote readonly/no-repl-pair stream slots during forced or elected master reset.
2. Bug 2: clear per-slot readability on same-master slave reset.
3. Bug 4: notify callback before rethrow on synchronous transition failure.
4. Bug 3: move the catch-up segment-length guard before advancing the in-memory repl-pair offset.


## Bug 1 Fix Implementation - 2026-06-30

Status: **Fixed**.

Changed `LeaderSelector.doResetAsMaster()` so a slot that no longer has a slave-side `ReplPair` but is still `readonly` is promoted with `oneSlot.resetAsMaster()` instead of falling through to the already-master path. This covers stream slots whose repl pair was already torn down before forced promotion.

Regression coverage added in `LeaderSelectorTest`: `test promote readonly stream slot without repl pair to master` reproduces the stale slave state (`readonly=true`, `canRead=false`, no slave repl pair), verifies the pre-fix failure, and now passes after the production fix.

Verification:

- Red: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test promote readonly stream slot without repl pair to master"` failed before the production change at `LeaderSelectorTest.groovy:256` because the slot remained readonly.
- Green: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test promote readonly stream slot without repl pair to master"` passed after the fix.
- Relevant class: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest"` passed.
- JaCoCo: `python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector 419 438 --src` shows the new readonly/no-repl-pair branch lines 433-436 covered.

---

## Review Feedback — Bug 1 Fix (commit `4e8c6b0d`)

**Reviewer:** AI agent (Step 4 feedback)
**Date:** 2026-06-30

### Summary of the fix

The fix adds a single `else if (oneSlot.isReadonly())` branch between the scale-up extra-slot branch and the already-master fallback in `LeaderSelector.doResetAsMaster()` (line 433-436). When `removeReplPairAsSlave()` returns `false`, the slot has no slave `ReplPair`, and the slot is not a scale-up extra slot, the new branch detects residual slave state via `isReadonly()` and calls `oneSlot.resetAsMaster()`. A regression test (`test promote readonly stream slot without repl pair to master`) was added in `LeaderSelectorTest`.

### Strengths

- **Minimal change.** 4 lines of production code, 20 lines of test. No structural refactoring needed.
- **Correct branch ordering.** The new check sits at the right priority level: after the explicit repl-pair and scale-up checks, before the "truly already master" fallback. This means explicit checks still have priority; the readonly fallback only catches the missed cases.
- **`isReadonly()` is a reliable signal.** `setReadonly(true)` is only called in `OneSlot.resetAsSlave()` (line 2650), so `readonly=true` uniquely identifies a slot that was reset as slave but lost its repl pair.
- **Test faithfully reproduces the bug.** The test sets `readonly=true`, `canRead=false`, no repl pair — exactly the post-teardown state. It asserts the slot becomes `readonly=false` and `canRead=true` after forced promotion.
- **JaCoCo confirmed.** Lines 433-436 are fully covered; the new branch was executed.
- **No regression.** The existing `test promote 2N slave extra slots to master` test still passes alongside the new test.

### Concerns

- **No `slot < masterSlotNumber` guard.** The new branch does not distinguish between stream slots and extra slots. In theory, if `isAsSlaveScaleUp()` were to return `false` for a scale-up extra slot (e.g. due to a `masterSlotNumber` mismatch), the readonly fallback would promote it anyway. This is actually **defense-in-depth** — the slot needs promotion regardless — but it means the warning log message ("promote readonly slot without slave repl pair") would fire instead of the scale-up-specific message. Not a correctness concern, but could confuse diagnostics if `masterSlotNumber` is temporarily zero during a race.

### Pre-commit checks (verified)

| Check | Result |
|-------|--------|
| New test fails pre-fix? | Yes — `LeaderSelectorTest.groovy:256` |
| New test passes post-fix? | Yes |
| Existing promotion test passes? | Yes (`test promote 2N slave extra slots to master`) |
| JaCoCo covers new branch? | Yes (lines 433-436, all green) |
| `git diff --check` clean? | Yes |
| Commit message follows style? | Yes: `fix: promote slave slot without repl pair` |

### Post-commit follow-ups

- **No follow-up required for Bug 1.** The fix is self-contained and verified.
- Bugs 2, 3, and 4 remain open per the review document's priority order. Bug 2 (same-master `canRead` reset) is next in priority.

---

## Bug 2 Fix Implementation - 2026-06-30

Status: **Fixed**.

Changed `LeaderSelector.doResetAsSlave()` so the same-master early-return path preserves the existing repl pair but clears stale slot readability before returning. The fix calls `oneSlot.setCanRead(false)` only when `oneSlot.isCanRead()` is true, keeping the original no-teardown optimization intact.

Regression coverage added in `LeaderSelectorTest`: `test same master reset clears canRead` creates a same-master slave repl pair, marks the slot readable, refreshes the local Redis config-check key during `resetAsSlave`, and verifies `canRead` becomes false after the same-master reset.

Verification:

- Red: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test same master reset clears canRead"` failed before the production change at `LeaderSelectorTest.groovy:716` because `oneSlot.canRead` remained true.
- Green: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test same master reset clears canRead"` passed after the fix.
- Relevant class: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest"` passed.
- JaCoCo: `python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector 563 576 --src` shows the same-master branch and new `setCanRead(false)` line covered.


---

## Bug 4 Fix Implementation - 2026-06-30

Status: **Fixed**.

Changed both synchronous `RuntimeException` catch blocks in `LeaderSelector.doResetAsMaster()` and `doResetAsSlave()` to invoke `callback.accept(e)` after restoring `masterSlotNumber` and `masterFailoverState`, then rethrow the same exception. The async `Promises.all(...).whenComplete` error path is unchanged.

Regression coverage added in `LeaderSelectorTest`:

- `test resetAsMaster notifies callback on synchronous transition exception`
- `test resetAsSlave notifies callback on synchronous transition exception`

Both tests force a synchronous slot-loop exception before the promise completion handler can be installed and assert the callback receives the same exception that is rethrown.

Verification:

- Red: the two focused Bug 4 tests failed before the production change because `callbackException` remained unset.
- Green: `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test resetAsMaster notifies callback on synchronous transition exception" --tests "io.velo.repl.LeaderSelectorTest.test resetAsSlave notifies callback on synchronous transition exception"` passed after the fix.
- Relevant class: fresh `./gradlew :cleanTest` then `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest"` passed.
- JaCoCo: `python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector 468 609 --src` shows both new `callback.accept(e)` lines covered.
