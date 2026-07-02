# Replication Module Bug Review - Scale-Up 2N Replication Round 1

Date: 2026-07-01
Author: AI agent 1

Scope: Review of `docs/plans/2026-06-25-master-n-slave-2n-replication.md` against the current codebase after
the `bug_56` and `bug_57` fixes. Focus areas: 2N promotion ordering, global read-gate correctness, and
incremental big-string replay readiness.

Design context:

- In 2N scale-up mode, only stream slots have slave `ReplPair`s; extra slots receive data by fan-out.
- `LocalPersist.publishStreamReadyAndRefreshGate` is intentionally global: all local slots become readable only
  when every master stream is ready.
- Promotion from a 2N slave back to master must leave every local slot `readonly=false` and `canRead=true`.
- Incremental `XBigStrings` replay stores a big-string marker first, then fetches the actual big-string file later
  through the replication pair's fetch queue.

## Bug 1: 2N promotion can enqueue stale gate-close tasks after `resetAsMaster()` sets slots readable

**Severity:** High
**Status:** Confirmed → Fixed (commit `218b9f45`)

**Files:**

- `src/main/java/io/velo/repl/LeaderSelector.java:419-431`
- `src/main/java/io/velo/persist/OneSlot.java:431-455`
- `src/main/java/io/velo/persist/LocalPersist.java:491-515`

**Code excerpt (`LeaderSelector.java:419-431`):**

```java
var isSelfSlave = oneSlot.removeReplPairAsSlave();
if (isSelfSlave) {
    oneSlot.resetAsMaster();
    resetAsMasterCount = 0;
} else if (localPersist.isAsSlaveScaleUp()
        && oneSlot.slot() >= ConfForGlobal.masterSlotNumber) {
    log.warn("Repl promote scale-up extra slot to master (no binlog history), slot={}", oneSlot.slot());
    oneSlot.resetAsMaster();
    resetAsMasterCount = 0;
}
```

**Code excerpt (`OneSlot.java:448-455`):**

```java
if (isSelfSlave) {
    var localPersist = LocalPersist.getInstance();
    if (localPersist.isAsSlaveScaleUp()) {
        localPersist.publishStreamReadyAndRefreshGate(slot, false);
    }
}
```

**Root cause:**

During 2N promotion, each stream slot calls `removeReplPairAsSlave()` before `resetAsMaster()`.
`removeReplPairAsSlave()` treats stream teardown as a normal slave-stream close and publishes `ready=false` to the
scale-up gate. The gate fan-out submits `setCanRead(false)` tasks to every local slot via `asyncRun`.

In production, the per-slot promotion work and the gate fan-out are asynchronous eventloop tasks. For an extra slot,
the ordering can be:

1. `doResetAsMaster` submits the extra slot's own promotion task.
2. A stream slot runs `removeReplPairAsSlave()` and the gate enqueues `setCanRead(false)` to that extra slot.
3. The extra slot runs `resetAsMaster()` and sets `canRead=true`.
4. The later gate-close task runs and sets `canRead=false`.

The current unit test fixes every slot's thread id to the test thread, making `asyncRun` inline and hiding this queue
ordering problem.

**Impact:**

`resetAsMaster` can call back successfully and clear `ConfForGlobal.masterSlotNumber`, but one or more promoted slots
can end with `canRead=false`. After Sentinel/client switch-over, the promoted master may reject reads even though the
promotion succeeded. This is especially likely for extra slots because they do not call `removeReplPairAsSlave()`;
they only receive the stale gate-close fan-out from stream-slot teardown.

**Trigger scenario:**

A 2N slave with at least one stream slot and one extra slot is promoted to master on real slot-worker eventloops while
the global scale-up gate has an active readiness array. Stream-slot repl-pair removal publishes `ready=false` before
the promotion completes.

**Suggested fix direction:**

Do not publish scale-up read-gate updates from `removeReplPairAsSlave()` when the removal is part of promotion.
For example:

- Add an overload such as `removeReplPairAsSlave(boolean publishScaleUpGateClose)`.
- Call it with `false` from `LeaderSelector.doResetAsMaster`.
- Keep the existing `true` behavior for ordinary stream disconnect/teardown paths.

Alternatively, make promotion explicitly cancel/clear the scale-up gate before any stream-slot removal can fan out,
while preserving the captured master slot count needed to identify extra slots.

---

## Bug 2: Scale-up read gate can open before incremental big-string files are fetched and written

**Severity:** Medium
**Status:** Confirmed → Fixed (commit `019df3c9`)

**Files:**

- `src/main/java/io/velo/repl/incremental/XBigStrings.java:214-223`
- `src/main/java/io/velo/command/XGroup.java:1699-1702,1833-1868`
- `src/main/java/io/velo/persist/OneSlot.java:1389-1398`
- `src/main/java/io/velo/command/XGroup.java:1127-1143`
- `src/main/java/io/velo/repl/ReplPair.java:677-697`

**Code excerpt (`XBigStrings.java:214-223`):**

```java
var s = targetSlot();
var oneSlot = localPersist.oneSlot(s.slot());
var promise = oneSlot.asyncCall(() -> oneSlot.putIfSeqBigger(key, s.bucketIndex(), cv, true));
promise.whenResult(isPut -> {
    if (isPut) {
        // use remote bucket index
        replPair.addToFetchBigStringId(uuid, bucketIndex, keyHash, key);
    }
});
return promise.map(isPut -> null);
```

**Code excerpt (`XGroup.java:1864-1868`):**

```java
if (isScaleUp) {
    localPersist.publishStreamReadyAndRefreshGate(slot, canRead);
}
```

**Code excerpt (`OneSlot.java:1389-1398`, `XGroup.java:1127-1143`):**

```java
var toFetchBigStringId = replPair.doingFetchBigStringId();
if (toFetchBigStringId != null) {
    replPair.write(ReplType.incremental_big_string, new RawBytesContent(bytes));
}
```

```java
if (buffer.hasRemaining()) {
    var oneSlot = localPersist.oneSlot(s.slot());
    oneSlot.asyncExecute(() -> {
        bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
    });
}

replPair.doneFetchBigStringUuid(uuid);
return Repl.emptyReply();
```

**Root cause:**

`XBigStrings.applyAsync` first writes the key's `CompressedValue` marker and then queues the big-string file UUID in
`ReplPair.toFetchBigStringIdList`. `finishSlaveCatchUpApply` and the 13-byte no-more-binlog path publish read-ready
based only on binlog offset/readiness. They do not check whether:

- `toFetchBigStringIdList` still contains files to fetch,
- `doFetchingBigStringIdList` still contains in-flight fetches, or
- the eventual `s_incremental_big_string` file write has completed.

The actual file fetch is performed later by the periodic replication-pair subtask. In the response path,
`s_incremental_big_string` submits the file write with `asyncExecute()` and immediately calls
`replPair.doneFetchBigStringUuid(uuid)`, so even the in-flight list can be cleared before the target slot's file write
has run.

**Impact:**

The global 2N gate can open and allow all local slots to serve reads while one or more big-string values only have
metadata markers and do not yet have their big-string files on disk. A client reading such a key can observe a missing
or incomplete big-string value. In 2N mode this is amplified because a big-string marker from any stream can land in
any local slot, including extra slots, and the gate fans readability to all slots at once.

**Trigger scenario:**

A catch-up segment contains an `XBigStrings` entry and all streams are otherwise near caught up or receive a
no-more-binlog response. The gate opens before the next slave-side periodic task fetches the queued
`incremental_big_string`, or before the async target-slot file write completes.

**Suggested fix direction:**

Make stream readiness depend on both binlog position and pending big-string file fetches. Options:

- Keep the stream not-ready while `toFetchBigStringIdList` or `doFetchingBigStringIdList` is non-empty, then publish
  readiness again after the final `s_incremental_big_string` write completes.
- Change `s_incremental_big_string` to use an awaitable `asyncRun`/`asyncCall` for the target-slot file write and only
  call `doneFetchBigStringUuid` after that promise completes.
- Add tests where an `XBigStrings` catch-up reaches near-current offset in 2N mode but the actual big-string file is
  intentionally delayed; the gate must remain closed until the file is written.

---

## Summary

| # | Severity | Status | Area | Root cause |
|---|----------|--------|------|------------|
| 1 | High | Fixed (`218b9f45`) | 2N promotion | Stream repl-pair teardown enqueues stale global gate-close tasks during promotion |
| 2 | Medium | Fixed (`019df3c9`) | Incremental big strings / read gate | Gate readiness ignores pending big-string fetches and async file writes |

Both findings are independent of the already-reviewed `bug_57` callback-thread and gate-mode-capture issues.

---

## Review Notes (AI Agent 2)

Date: 2026-07-01
Reviewer: AI agent 2

Both bugs were verified against the current `main` codebase. All cited file paths and line numbers match.
Both are **confirmed**.

### Bug 1 — Confirmed

**Verification of the race chain:**

1. `LeaderSelector.doResetAsMaster` (line 382-443) submits one `asyncRun` promotion task per slot.
   Each stream-slot task calls `oneSlot.removeReplPairAsSlave()` (line 419) **before** `oneSlot.resetAsMaster()` (line 421).

2. `OneSlot.removeReplPairAsSlave()` (line 431-459) publishes `ready=false` to the gate when
   `localPersist.isAsSlaveScaleUp()` is true (line 451-456).

3. During promotion `isAsSlaveScaleUp()` is still true: `ConfForGlobal.masterSlotNumber` is only
   cleared to 0 on overall success (LeaderSelector.java:459), which runs after `Promises.all` completes —
   i.e., after every per-slot task has already executed. Confirmed at `LocalPersist.isAsSlaveScaleUp()`
   (LocalPersist.java:455-457): `masterSlotNumber > 0 && slotNumber > masterSlotNumber`.

4. `publishStreamReadyAndRefreshGate(slot, false)` (LocalPersist.java:491-521) recomputes the AND,
   finds it false (gate was open), and fans `setCanRead(false)` to **all** local slots via `asyncRun`
   (line 512-519). Only the first stream to publish triggers fan-out; subsequent `ready=false` calls
   find `open == scaleUpGateOpen` and return early (line 504-506).

5. `resetAsMaster()` (OneSlot.java:2582-2609) sets `canRead = true` (line 2605-2607).

**The concrete ordering hazard (confirmed):**

In production each slot runs on its own eventloop thread. When stream slot S publishes `ready=false`,
the fan-out calls `ts.asyncRun(setCanRead(false))` for every slot. For S itself this runs inline
(same thread, line 622), so S gets `canRead=false` then immediately `resetAsMaster()` sets it back to
`canRead=true` — S is fine. But for any *other* slot X (stream or extra), the `setCanRead(false)` task
is enqueued on X's eventloop **after** X's own promotion task (promotion was submitted first in the
for-loop at line 394-444). So X runs `resetAsMaster()` (canRead=true) first, then the stale
`setCanRead(false)` lands and leaves X unreadable.

This affects **all** promoted slave slots except the single stream slot that triggers the fan-out —
not only extra slots as the original doc emphasizes. Extra slots are guaranteed affected because they
never publish themselves.

**Test-gap confirmed:**

`ScaleUpReplicationTest.groovy:22-24` fixes every slot's thread ID to the test thread
(`localPersist.fixSlotThreadId(s, Thread.currentThread().threadId())`), which makes `asyncRun` always
take the inline branch (OneSlot.java:622). This serializes the promotion task and the fan-out task on
the same call stack, so the stale-close always runs *before* `resetAsMaster()` — the exact opposite of
the production ordering. The race is invisible to this test.

**Suggested fix direction is sound.** The `removeReplPairAsSlave(boolean publishScaleUpGateClose)`
overload approach is the least invasive. Alternatively, promotion could clear/reset the scale-up gate
array before any stream-slot teardown can fan out.

### Bug 2 — Confirmed

**Verification of the readiness path:**

1. `XBigStrings.applyAsync` (XBigStrings.java:209-224) writes the `CompressedValue` marker via
   `oneSlot.asyncCall(putIfSeqBigger)` and, on success, queues the big-string UUID in
   `replPair.toFetchBigStringIdList` via `addToFetchBigStringId` (line 220). The marker is persisted
   immediately; the actual big-string file arrives later.

2. `finishSlaveCatchUpApply` (XGroup.java:1845-1874) computes `canRead` solely from binlog offset
   proximity: `diffOffset < catchUpOffsetMinDiff` (line 1859). It does **not** inspect
   `toFetchBigStringIdList` or `doFetchingBigStringIdList`. When `isScaleUp`, it publishes that
   `canRead` to the global gate (line 1864-1868).

3. The 13-byte no-more-binlog path (XGroup.java:1699-1702) publishes `ready=true` unconditionally
   when `isAsSlaveScaleUp()`, also without checking pending big-string fetches.

4. The big-string file fetch is fully asynchronous and lagging:
   - `OneSlot.java:1389-1400`: the periodic ping task polls `doingFetchBigStringId()` and sends the
     `incremental_big_string` request only on subsequent eventloop ticks.
   - `s_incremental_big_string` (XGroup.java:1112-1144): writes the file via `asyncExecute` — a
     fire-and-forget submit to the target slot's eventloop (line 1133) — and then **immediately** calls
     `replPair.doneFetchBigStringUuid(uuid)` (line 1142), clearing the in-flight list *before* the
     target-slot write has executed.

**Race confirmed:**

If a catch-up segment contained an `XBigStrings` entry, the marker is persisted and the UUID is queued,
but the gate can still open on the next catch-up response (or no-more-binlog response) because
readiness is based only on binlog position. The big-string file may not yet be fetched or written.
A client reading such a key sees the CompressedValue marker but the backing big-string file is absent
or incomplete.

Additionally, even if the readiness check were extended to inspect `doFetchingBigStringIdList`, the
premature `doneFetchBigStringUuid` call at line 1142 would already have cleared that list before the
`asyncExecute` file write completes. Both defects must be addressed.

**Scope refinement:** The same gate-ignores-big-strings race exists in non-scale-up (equal-slot) mode
too — `finishSlaveCatchUpApply` sets per-slot `canRead` from binlog offset alone (line 1869-1872).
The bug is broader than 2N, but this review is scoped to the 2N replication plan, so the finding is
valid as written. The fix should ideally cover both paths.

**Suggested fix direction is sound.** Making readiness depend on both binlog position and pending
big-string fetches, plus deferring `doneFetchBigStringUuid` until the file-write promise completes,
addresses both defects.

---

## Bug 1 Fix Summary

**Commit:** `218b9f45` — `fix: gate fan-out skips promoted slots in 2N promotion`

**Approach:** Rather than the overload suggested by AI agent 1 (caller must remember to pass `false`),
the fix adds an `isReadonly()` guard inside the gate fan-out itself (`LocalPersist.java:518`):

```java
if (ts.isReadonly() && ts.isCanRead() != gateOpen) {
    ts.setCanRead(gateOpen);
}
```

The gate is a slave-only mechanism. Once a slot is promoted (`resetAsMaster()` sets `readonly=false`),
the fan-out skips it — no caller-side flag needed. This is race-safe because the fan-out task runs on
the slot's own eventloop (same thread as `resetAsMaster()`), so `isReadonly()` is read single-threaded.

**TDD evidence:**
- New test `'test scale-up read gate skips promoted slots'` fails pre-fix (promoted slot's `canRead`
  overridden to `false` by stale fan-out), passes post-fix.
- Existing tests updated to set `readonly=true` on slave slots (matching production state set by
  `resetAsSlave`).
- JaCoCo: line 518, all 4 branches covered (isReadonly true/false × canRead matches/not).

---

## Review Feedback - Bug 1 Fix - AI agent 2 - 2026-07-01

Reviewed commit `a7d9ce7f` (`fix: gate fan-out skips promoted slots in 2N promotion`).

### Summary

The fix changes the central scale-up gate fan-out to update `canRead` only for slots that are still readonly:

```java
if (ts.isReadonly() && ts.isCanRead() != gateOpen) {
    ts.setCanRead(gateOpen);
}
```

This addresses the confirmed promotion race: once `resetAsMaster()` has made a slot `readonly=false`, any stale
gate-close fan-out that arrives later on that slot's eventloop is skipped instead of overriding the promoted
master's `canRead=true`.

### Strengths

- The fix is localized to the gate fan-out and does not add a caller contract to every `removeReplPairAsSlave()`
  call site.
- It preserves normal slave behavior because `resetAsSlave()` sets slots `readonly=true`, so ordinary scale-up gate
  open/close fan-outs still update slave slots.
- The guard is evaluated on each target slot's eventloop, the same place `resetAsMaster()` mutates readonly/canRead,
  so the ordering decision is made against the slot's current role state.
- Focused tests cover both normal readonly-slave gate behavior and the promoted-slot skip behavior.

### Concerns

- No blocking code concerns found.
- Non-blocking doc mismatch: earlier in this document the Bug 1 fixed commit is listed as `218b9f45`, but the
  reviewed commit in this workspace is `a7d9ce7f`.

### Verification

- Ran:
  `./gradlew :test --tests "io.velo.persist.LocalPersistTest.test scale-up read gate skips promoted slots" --tests "io.velo.persist.LocalPersistTest.test scale-up read gate" --tests "io.velo.repl.ScaleUpReplicationTest"`
- Result: Gradle build successful.
- JaCoCo inspection:
  `python3 scripts/jacoco_cover.py io.velo.persist.LocalPersist 491 520 --src`
- Result: line 518 covered, all 4 branches covered.

---

## Bug 2 Fix Summary

**Commit:** `019df3c9` — `fix: block read gate while big-string fetches pending`

**Approach:** Three coordinated changes:

1. **`ReplPair.hasPendingBigStringFetches()`** (new method): returns true when either
   `toFetchBigStringIdList` or `doFetchingBigStringIdList` is non-empty.

2. **Gate-readiness checks** in `XGroup`:
   - `finishSlaveCatchUpApply` (line 1871): `if (canRead && replPair.hasPendingBigStringFetches())`
     forces `canRead=false` before publishing to the gate.
   - 13-byte no-more-binlog path (line 1706): `publishStreamReadyAndRefreshGate(slot, !replPair.hasPendingBigStringFetches())`.

3. **Deferred `doneFetchBigStringUuid`** in `s_incremental_big_string` (line 1133-1144): changed
   `asyncExecute` (fire-and-forget) to `asyncRun` (returns promise) and moved
   `doneFetchBigStringUuid` into `whenResult`, so the in-flight list is only cleared after the
   target-slot file write completes. `whenResult` fires on the caller's thread (stream slot
   eventloop), preserving `@ForSlaveField` thread safety.

**TDD evidence:**
- New test `'test finish catch up gate stays closed when big string fetches pending'` — exercises
  both `finishSlaveCatchUpApply` and the 13-byte `s_catch_up` path. Fails pre-fix (gate opens with
  pending fetches), passes post-fix (gate stays closed until fetches complete).
- JaCoCo: line 1871 all 4 branches covered; line 1706 all branches covered; `s_incremental_big_string`
  all lines covered.

---

## Review Feedback - Bug 2 Fix - AI agent 2 - 2026-07-01

Reviewed commit `433e94fd` (`fix: block read gate while big-string fetches pending`).

### Findings

1. **High - Gate can remain closed forever after a readonly no-more-binlog response with pending big strings.**

   The fix correctly prevents `s_catch_up` from opening the scale-up gate while
   `replPair.hasPendingBigStringFetches()` is true:

   ```java
   localPersist.publishStreamReadyAndRefreshGate(slot, !replPair.hasPendingBigStringFetches());
   ```

   But `s_incremental_big_string` only clears the pending state after the file write:

   ```java
   writePromise.whenResult(v -> replPair.doneFetchBigStringUuid(uuid));
   ```

   It does not re-publish stream readiness after clearing the final pending big-string fetch.
   In the 13-byte no-more-binlog branch, if `masterReadonlyFlag == 1`, `s_catch_up` does not schedule another
   catch-up request (`if (!isMasterReadonly) { ... delayRun(... catch_up ...) }`). Therefore the sequence can be:

   - stream receives readonly 13-byte no-more-binlog response while a big-string fetch is pending;
   - gate publishes `ready=false`;
   - no next catch-up is scheduled because the master is readonly;
   - big-string fetch completes and pending state is cleared;
   - no code publishes `ready=true`, so the scale-up read gate stays closed indefinitely.

   The new test misses this because it manually invokes `s_catch_up` a second time after calling
   `doneFetchBigStringUuid`. That second no-more-binlog response is not guaranteed in the real readonly path.

   Suggested fix: after the final big-string fetch completes, re-evaluate and publish readiness for the stream.
   A minimal approach is to make `doneFetchBigStringUuid` (or the `s_incremental_big_string` completion callback)
   detect when no pending big-string fetches remain and then publish/readiness-refresh for the stream, using the
   last known caught-up state. Be careful to cover both scale-up global gate and equal-slot `canRead` paths.

### Strengths

- The fix correctly models both queued and in-flight big-string fetches via `hasPendingBigStringFetches()`.
- Moving `doneFetchBigStringUuid` after the target-slot file write addresses the original premature in-flight-clear
  problem.
- The gate checks were added in both relevant catch-up readiness paths: segment finish and 13-byte no-more-binlog.

### Verification

- Ran:
  `./gradlew :test --tests "io.velo.command.XGroupTest.test finish catch up gate stays closed when big string fetches pending"`
- Result: Gradle build successful.
- Ran:
  `./gradlew :test --tests "io.velo.command.XGroupTest.test as slave handle s incremental big string" --tests "io.velo.command.XGroupTest.test as slave handle s incremental big string when master file missing"`
- Result: Gradle build successful.

### Follow-ups

- ~~Add a regression test for the readonly 13-byte path where pending big strings complete without another `s_catch_up`
  invocation; the gate should open after the final big-string file write.~~ **Done** — commit `1f716c3d` adds
  `tryRePublishReadiness()` which re-publishes stream readiness after the last pending big-string fetch completes
  (checking `isAllCaughtUp()`). New test `'test gate opens after big string fetch completes without another catch up'`
  proves the gate opens with no further `s_catch_up`. JaCoCo confirms the re-publish path (lines 1144-1147, 1164) is covered.
- Non-blocking doc mismatch: this document's Bug 2 fixed commit is listed as `019df3c9`, but the reviewed commit in
  this workspace is `433e94fd`.

---

## Review Feedback - Bug 2 Addressed Follow-up - AI agent 2 - 2026-07-01

Reviewed commit `de0674b6` (`fix: re-publish gate readiness after last big-string fetch completes`).

### Findings

No blocking findings.

### Summary

- The addressed commit resolves the previous liveness concern by re-publishing stream readiness after
  `s_incremental_big_string` clears the final pending big-string fetch and the stream is already marked
  `allCaughtUp`.
- The new regression test covers the readonly 13-byte no-more-binlog path where no second `s_catch_up`
  is invoked; the scale-up gate opens after the big-string file write completes.

### Strengths

- The readiness re-publish is guarded by both `!replPair.hasPendingBigStringFetches()` and
  `replPair.isAllCaughtUp()`, so segment catch-up paths that are only nearly caught up still wait for the normal
  catch-up readiness calculation.
- The scale-up path uses `LocalPersist.publishStreamReadyAndRefreshGate`, preserving the global AND gate and
  promoted-slot skip behavior already added for Bug 1.

### Concerns

- Non-blocking coverage gap: JaCoCo shows the equal-slot branch of `tryRePublishReadiness()` is not covered
  (lines 1166-1168). The bug under review is the 2N scale-up gate, and the covered scale-up branch is the critical
  path, but a future equal-slot regression test would make the helper fully covered.
- Non-blocking doc mismatch: the prior follow-up note says this addressed fix was commit `1f716c3d`; the actual
  reviewed commit in this workspace is `de0674b6`.

### Verification

- Ran:
  `./gradlew :test --tests "io.velo.command.XGroupTest.test gate opens after big string fetch completes without another catch up" --tests "io.velo.command.XGroupTest.test finish catch up gate stays closed when big string fetches pending" --tests "io.velo.command.XGroupTest.test as slave handle s incremental big string" --tests "io.velo.command.XGroupTest.test as slave handle s incremental big string when master file missing"`
- Result: Gradle build successful.
- Ran:
  `python3 scripts/jacoco_cover.py io.velo.command.XGroup 1138 1169 --src`
- Result: scale-up re-publish path covered (`writePromise.whenResult`, `doneFetchBigStringUuid`,
  `tryRePublishReadiness(slot)`, and `publishStreamReadyAndRefreshGate(slot, true)` covered); equal-slot branch
  uncovered.
- Ran:
  `python3 scripts/jacoco_cover.py io.velo.command.XGroup 1728 1734 --src`
- Result: 13-byte scale-up readiness publish line covered with all branches on
  `publishStreamReadyAndRefreshGate(slot, !replPair.hasPendingBigStringFetches())` covered.
