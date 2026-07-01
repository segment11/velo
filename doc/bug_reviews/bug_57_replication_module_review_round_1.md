# Replication Module Bug Review — Scale-Up 2N Replication Round 2

Date: 2026-07-01
Author: AI agent 1

Scope: Deeper review of the master-N-to-slave-2N replication implementation (`docs/plans/2026-06-25-master-n-slave-2n-replication.md`)
after round 1 (`bug_56`) fixes were merged. Focus areas: async apply thread-safety, gate transition consistency,
and cross-slot fan-out correctness during extended runtime.

Design context:

- Scale-up uses `Promises.all` for await-before-advance: all `applyAsync` promises must complete before
  `finishSlaveCatchUpApply` advances the fetched offset (`doc/design/09_replication_design.md` § 2N Scale-Up).
- Cross-slot writes fan out via `asyncRun`/`asyncCall` which dispatch to the target slot's eventloop.
  Individual `whenResult` callbacks fire on the target thread, not the stream slot's thread.

## Bug 1: `XBigStrings.applyAsync` — `ReplPair.addToFetchBigStringId` called from foreign threads without synchronization

**Severity:** Medium

**AI agent 2 status:** Not confirmed for the production replication path. The `ReplPair` list is unsynchronized, but
current `OneSlot.asyncRun` / `asyncCall` route completion callbacks back to the caller reactor when invoked from
the normal slot-worker/network-worker eventloop path. The target-thread callback behavior exists in the no-reactor
fallback path only, so this is optional hardening unless non-reactor catch-up callers are treated as supported.

**Files:**

- `src/main/java/io/velo/repl/incremental/XBigStrings.java:217-221`
- `src/main/java/io/velo/repl/ReplPair.java:650,677-678`

**Code excerpt (XBigStrings.java:209-224):**

```java
public Promise<Void> applyAsync(short slot, ReplPair replPair) {
    var keyBytes = Wal.keyBytes(key);
    var keyHash = KeyHash.hash(keyBytes);
    var cv = CompressedValue.decode(cvEncoded, keyBytes, keyHash);

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
}
```

**Root cause:**

When a single catch-up segment contains multiple `XBigStrings` entries targeting different local slots,
each `applyAsync` returns a promise that completes on its target slot's eventloop. The `whenResult`
callback fires on that same target thread.

`ReplPair.toFetchBigStringIdList` is a plain `LinkedList`:
```java
private final LinkedList<BigStringFiles.IdWithKey> toFetchBigStringIdList = new LinkedList<>();
```

`addToFetchBigStringId` calls `toFetchBigStringIdList.add(...)`. If two target threads complete their
`asyncCall` promises concurrently, both call `LinkedList.add()` on the same list without synchronization.

**Impact:**

Concurrent `LinkedList.add()` without synchronization can cause:
- Lost entries (both threads see the same tail and overwrite each other)
- `NullPointerException` within `linkLast` if the internal pointers are torn
- `OutOfMemoryError` from infinite loops during concurrent growth

The consequence is silently lost big-string fetch IDs — after catch-up, the slave would not fetch
those big strings, leaving them as dangling references in the key-value storage. A client reading
such a key would get a `BigString` marker pointing to a file that was never fetched.

**Trigger scenario:** Two or more big-string writes in a single master binlog segment that,
after re-hashing on the 2N slave, target different local slots. Both `applyAsync` promises
resolve concurrently on different threads.

**Suggested fix direction:**

Option A: Synchronize `addToFetchBigStringId`:
```java
public synchronized void addToFetchBigStringId(long uuid, int bucketIndex, long keyHash, String key) {
    toFetchBigStringIdList.add(new BigStringFiles.IdWithKey(uuid, bucketIndex, keyHash, key));
}
```
Also synchronize the other accessors (`doingFetchBigStringId`, `getToFetchBigStringIdList`).

Option B: Collect IDs in a thread-local or `applyAsync`-local list, then batch-merge into
`toFetchBigStringIdList` inside the `Promises.all` callback (runs on a single thread after
all children complete). This avoids synchronizing the fetch loop.

---

## Bug 2: `finishSlaveCatchUpApply` gate-mode check can race with role transition

**Severity:** Low

**AI agent 2 status:** Confirmed. The async dispatch decision uses `localPersist.isAsSlaveScaleUp()` at line 1785,
but `finishSlaveCatchUpApply` re-checks the same volatile global-derived state later at line 1863. A concurrent
role transition can therefore route the readiness update through the equal-slot branch after the operation was
processed as scale-up.

**Fix status:** Fixed by AI agent 2 on 2026-07-01. `s_catch_up` now captures `isScaleUp` once before dispatch
and passes that stable value into `finishSlaveCatchUpApply`, so the completion path cannot switch from the
scale-up gate branch to the equal-slot `oneSlot.setCanRead` branch after a role flag change.

**Files:**

- `src/main/java/io/velo/command/XGroup.java:1785,1863`
- `src/main/java/io/velo/persist/LocalPersist.java:455`

**Code excerpt (XGroup.java:1782-1785, 1863-1867):**

```java
// line 1785 — gate decision at dispatch time
boolean useAsync = hasAsyncApply || localPersist.isAsSlaveScaleUp();

// ...

// line 1863 — gate decision inside the async callback, potentially stale
if (localPersist.isAsSlaveScaleUp()) {
    localPersist.publishStreamReadyAndRefreshGate(slot, canRead);
} else if (...) {
    oneSlot.setCanRead(canRead);
}
```

**Root cause:**

`isAsSlaveScaleUp()` reads two static fields:
```java
public boolean isAsSlaveScaleUp() {
    return ConfForGlobal.masterSlotNumber > 0 && ConfForGlobal.slotNumber > ConfForGlobal.masterSlotNumber;
}
```

`masterSlotNumber` is volatile and can be cleared to 0 by a concurrent `doResetAsMaster` or `doResetAsSlave`.
Between the `allApplyPromise.whenComplete` registration (line 1840) and its execution, a role transition
could clear `masterSlotNumber`, making `isAsSlaveScaleUp()` return `false`.

When this happens:
- The gate was initialized with `masterSlotNumber > 0` entries (scale-up mode)
- `finishSlaveCatchUpApply` enters the equal-slot branch instead
- `oneSlot.setCanRead(canRead)` is called on just the stream slot
- The global gate does NOT receive this stream's readiness update
- If all other streams already published ready, the gate stays open from stale data

**Impact:**

A mid-transition readiness update is lost. If `canRead=true` was being published, the gate might stay
closed when it should open. If `canRead=false`, the gate might stay open when it should close.

In practice, the role transition will tear down all repl pairs and re-initialize the gate, so the
stale gate state is short-lived and corrected by the new session. Low impact.

**Suggested fix direction:**

Capture `isAsSlaveScaleUp()` before the async dispatch (alongside the `useAsync` decision at line 1785)
and pass it into `finishSlaveCatchUpApply`, or capture it as a local final boolean before the
`whenComplete` registration:

```java
final boolean isScaleUp = hasAsyncApply || localPersist.isAsSlaveScaleUp();
boolean useAsync = isScaleUp;
// ... inside whenComplete:
if (isScaleUp) {
    localPersist.publishStreamReadyAndRefreshGate(slot, canRead);
}
```

---

## Bug 3: `readSegmentLength` not validated for 13-byte no-more-binlog path before gate publish

**Severity:** Low

**AI agent 2 status:** Not confirmed. The 13-byte payload is the master's no-more-binlog control response
(`readonly byte + current file index + current offset`), not a segment response, so `readSegmentLength` is not
part of this path. The master emits it only from no-more-binlog branches and the slave validates the reported
current position before publishing readiness. No fix is recommended for the stated issue.

**Files:**

- `src/main/java/io/velo/command/XGroup.java:1680-1702`

**Code excerpt (XGroup.java:1680-1702):**

```java
boolean resetMasterReadonlyByContentBytes = contentBytes.length == 13;
boolean isMasterReadonly = false;
if (resetMasterReadonlyByContentBytes) {
    var buffer = ByteBuffer.wrap(contentBytes);
    var masterReadonlyFlag = buffer.get();
    // ...
    replPair.setAllCaughtUp(true);
    // ...
    if (localPersist.isAsSlaveScaleUp()) {
        localPersist.publishStreamReadyAndRefreshGate(slot, true);
    }
}
```

**Root cause:**

The 13-byte no-more-binlog path publishes `ready=true` to the gate unconditionally for scale-up mode.
However, this path does not validate that the stream's `lastUpdatedOffset` is actually near the
master's current offset before declaring readiness. The `readSegmentLength` guard (line 1891-1893
in `finishSlaveCatchUpApply`) does not apply here because this is a separate code path.

If a 13-byte message arrives when the stream is far behind (e.g., due to a network hiccup), the
stream would be marked as read-ready prematurely.

**Impact:**

The gate would open before all streams have actually caught up, allowing stale reads on the slave.
This is a consistency issue but requires a specific malformed or mistimed master response.

**Suggested fix direction:**

Add a validation step in the 13-byte path: compare `lastUpdatedOffset` against the master's reported
`masterCurrentOffset` and only publish `ready=true` if the stream is near-caught-up (similar to the
`diffOffset < catchUpOffsetMinDiff` check in `finishSlaveCatchUpApply`).

---

## Bug 4: `XWalV.applyAsync` — `replPair.setSlaveCatchUpLastSeq` long assignment from foreign threads

**Severity:** Low

**AI agent 2 status:** Not confirmed for the production replication path, for the same reason as Bug 1. In the
normal reactor path, `asyncRun` completion callbacks are bounced back to the caller thread, so this is not a
foreign target-slot-thread write. Optional hardening could still be considered for no-reactor fallback/test callers.

**Files:**

- `src/main/java/io/velo/repl/incremental/XWalV.java:202`
- `src/main/java/io/velo/repl/ReplPair.java:271`

**Code excerpt (XWalV.java:188-203):**

```java
public Promise<Void> applyAsync(short slot, ReplPair replPair) {
    var key = v.key();
    var s = targetSlot(key);
    // ...
    var promise = oneSlot.asyncRun(() -> { ... });
    promise.whenResult(unused -> replPair.setSlaveCatchUpLastSeq(v.seq()));
    return promise;
}
```

**Root cause:**

Same `whenResult` dispatch pattern as Bug 1. Multiple `XWalV` entries in the same catch-up segment
targeting different slots will complete their `asyncRun` promises on different threads. Each `whenResult`
callback calls `replPair.setSlaveCatchUpLastSeq(v.seq())`.

`slaveCatchUpLastSeq` is a plain `long` field. Although `long` writes are atomic on 64-bit JVMs,
they are not guaranteed atomic. The Java Memory Model does not guarantee atomicity of `long` writes.

**Impact:**

The sequence number tracks the last applied binlog entry for diagnostics/offset tracking. A torn
write could result in an incorrect `lastSeq` value, but this field is informational and not used
for offset advancement (that's handled by `metaChunkSegmentIndex`). Low practical impact.

**Suggested fix direction:**

Same approach as Bug 1: synchronize `setSlaveCatchUpLastSeq` or capture the max seq and set it
once inside the `Promises.all` callback.

---

## Summary

| # | Severity | Area | Root cause |
|---|----------|------|------------|
| 1 | Medium | `XBigStrings.applyAsync` | Unsynchronized `LinkedList.add()` from foreign threads |
| 2 | Low | `finishSlaveCatchUpApply` | `isAsSlaveScaleUp()` re-check races with role transition |
| 3 | Low | `s_catch_up` 13-byte path | No near-caught-up validation before gate publish |
| 4 | Low | `XWalV.applyAsync` | Unsynchronized `long` assignment from foreign threads |

Bugs 1 and 4 are the same class of concurrency issue (`whenResult` callbacks on target threads
mutating `ReplPair` fields). Bug 1 is the most consequential because it can cause silent data loss
(missing big-string fetches). Bugs 2 and 3 are edge cases with low practical impact.

## AI Agent 2 Verification - 2026-07-01

Reviewed the current code against the four findings in this document.

| # | Status | Verification notes |
|---|--------|--------------------|
| 1 | Not confirmed | `XBigStrings.applyAsync` does call `replPair.addToFetchBigStringId` from a `whenResult` callback, and `toFetchBigStringIdList` is a plain `LinkedList`. However, `OneSlot.asyncCall` uses `Promise.ofFuture(...)` when `Reactor.getCurrentReactorOrNull() != null`, and the adjacent `asyncRun` comment documents that production completion callbacks are bounced back to the caller reactor/thread. The target-slot callback behavior is the no-reactor fallback path only. |
| 2 | Confirmed | `s_catch_up` computes `useAsync = hasAsyncApply || localPersist.isAsSlaveScaleUp()` before applying entries, then `finishSlaveCatchUpApply` calls `localPersist.isAsSlaveScaleUp()` again before publishing the scale-up read gate. Because `isAsSlaveScaleUp()` depends on volatile/global role state, a role transition can change the branch between dispatch and completion. |
| 3 | Not confirmed | The 13-byte branch is explicitly `readonly byte + current file index + current offset`. On the master side, this payload is generated by the no-more-binlog paths in `catch_up`, and the slave validates the reported current position with `validateCatchUpPosition` before setting caught-up/read-ready state. The segment-length validation is correctly limited to segment payloads. |
| 4 | Not confirmed | `XWalV.applyAsync` writes `slaveCatchUpLastSeq` from `whenResult`, but the production callback-thread assumption in the finding does not match `OneSlot.asyncRun`'s reactor path. The possible target-thread write is limited to no-reactor fallback/test execution. |

Only Bug 2 is confirmed as a current production bug. Bugs 1 and 4 may be considered defensive hardening for
unsupported/no-reactor callers, but they are not confirmed under the normal replication execution model. Bug 3
does not need a fix for the stated root cause.

## Bug 2 Fix - AI Agent 2 - 2026-07-01

Implemented the confirmed Bug 2 fix in `XGroup`:

- Captured `localPersist.isAsSlaveScaleUp()` once as `isScaleUp` before choosing sync/async apply.
- Passed `isScaleUp` into `finishSlaveCatchUpApply`.
- Replaced the volatile/global re-check inside `finishSlaveCatchUpApply` with the captured argument.

Regression coverage:

- Added `XGroupTest.test finish catch up keeps captured scale up gate after role flag clears`.
- Red check: the new test failed before the production change because slot 0 was set readable directly after
  `masterSlotNumber` was cleared.
- Green check: the focused test passed after the fix.
- Relevant verification: `./gradlew :test --tests "io.velo.command.XGroupTest"` passed.
- JaCoCo inspection confirmed the changed `XGroup` capture line and both `finishSlaveCatchUpApply` gate branches
  were executed.

---

## Second Verification — 2026-07-01 (independent re-read)

Re-verified all four bugs against the current codebase. Key evidence and verdicts:

### Bug 1 / Bug 4 — Thread dispatch

`OneSlot.asyncRun` line 635-636:
```java
if (Reactor.getCurrentReactorOrNull() != null) {
    return Promise.ofFuture(slotWorkerEventloop.submit(runnableEx));
}
```

`Promise.ofFuture` captures the caller's reactor and bounces completion callbacks back to the
caller's eventloop. In production (replication path), `whenResult` callbacks execute on the
stream slot's thread, not target-slot threads. No concurrent mutation of `ReplPair` fields.
The `SettablePromise` fallback (lines 642-651) only fires on target thread in no-reactor mode
(unit tests), where `fixSlotThreadId` still makes access effectively single-threaded.

**Verdict: AGREE with AI agent 2 — not a production bug.**

### Bug 2 — Gate-mode race

`isAsSlaveScaleUp()` reads `masterSlotNumber` (volatile). Between dispatch (line 1785) and
callback (line 1863), `doResetAsMaster` can clear it to 0, routing the readiness update to
the wrong branch.

**Verdict: AGREE with AI agent 2 — confirmed race.**

### Bug 3 — 13-byte gate publish

The 13-byte payload is a control message from the master's no-more-binlog branches, not a
segment. `validateCatchUpPosition` at line 1691 validates the reported position. The slave
sets its position to the master's confirmed current position at line 1697. No `readSegmentLength`
concern applies.

**Verdict: AGREE with AI agent 2 — not a bug, position already validated.**

### Summary

Only Bug 2 is a confirmed production issue. The fix is to capture `isAsSlaveScaleUp()` before
the async dispatch and use the captured value in the callback.
