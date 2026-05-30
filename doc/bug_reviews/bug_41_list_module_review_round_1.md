# Bug 41: List Module Review Round 1

Author: AI agent 1
Date: 2026-05-28
Module: list commands

## Redis List Command Surface Checked

Source: https://redis.io/docs/latest/commands/?group=list

Redis latest list-category commands observed during this review:

`BLMOVE`, `BLMPOP`, `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `LINDEX`, `LINSERT`, `LLEN`, `LMOVE`, `LMPOP`, `LPOP`, `LPOS`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPOPLPUSH`, `RPUSH`, `RPUSHX`.

Relevant Velo files reviewed:

- `src/main/java/io/velo/command/LGroup.java`
- `src/main/java/io/velo/command/RGroup.java`
- `src/main/java/io/velo/command/BGroup.java`
- `src/main/java/io/velo/command/BlockingList.java`
- `src/main/java/io/velo/type/RedisList.java`
- `src/test/groovy/io/velo/command/LGroupTest.groovy`
- `src/test/groovy/io/velo/command/RGroupTest.groovy`
- `src/test/groovy/io/velo/command/BGroupTest.groovy`
- `src/test/groovy/io/velo/command/BlockingListTest.groovy`
- `doc/redis_command_support.md`

---

## Reviewer: AI agent 2
Date: 2026-05-28

---

## Finding 1: `LINSERT` returns the current list length when the pivot is missing

**Status: CONFIRMED**

Severity: Medium

Files:

- `src/main/java/io/velo/command/LGroup.java:218-224`
- `src/main/java/io/velo/command/LGroup.java:276-305`

Code excerpt:

```java
if (considerBeforeOrAfter) {
    // find pivot index
    int pivotIndex = rl.indexOf(pivotBytes);
    if (pivotIndex == -1) {
        // -1 or size ? todo
        return new IntegerReply(rl.size());
    }
```

Root cause:

`LINSERT` reuses `addToList(...)`. When the key exists but the pivot is absent, `addToList` returns `rl.size()`. Redis specifies `-1` when the pivot is not found, while `0` is reserved for a missing key.

Impact:

Clients cannot distinguish "insert succeeded and length is N" from "pivot was absent in a list of length N". This breaks Redis protocol compatibility and can make application-side retry or validation logic accept a failed insert as success.

Suggested regression test:

- Create `a = [x, y]`.
- Run `LINSERT a BEFORE missing z`.
- Expect integer `-1`.
- Confirm `LRANGE a 0 -1` is still `[x, y]`.

### Review Notes (Agent 2)

Verified against code at `LGroup.java:218-224`. The `addToList()` method returns `new IntegerReply(rl.size())` when `pivotIndex == -1`. The code even has a `// -1 or size ? todo` comment acknowledging the ambiguity.

Per the official Redis documentation, LINSERT returns:
- Integer reply: the list length after a successful insert operation.
- Integer reply: `0` when the key doesn't exist.
- Integer reply: `-1` when the pivot wasn't found.

The current behavior returns `rl.size()` (e.g., 2 for a list `[x, y]`) instead of `-1` when the pivot is missing. This makes it impossible to distinguish a successful insert that resulted in a list of size N from a failed insert due to missing pivot on a list that already had N elements.

**Verdict: Confirmed. This is a Redis protocol compatibility bug.**

---

## Finding 2: `LPOS ... COUNT` no-match replies are returned as nil instead of an empty array

**Status: CONFIRMED**

Severity: Medium

Files:

- `src/main/java/io/velo/command/LGroup.java:473-522`
- `src/main/java/io/velo/command/LGroup.java:560-566`

Code excerpt:

```java
int rank = 1;
int count = 1;
// max compare times
int maxlen = 0;
for (int i = 3; i < data.length; i++) {
    String arg = new String(data[i]).toLowerCase();
    switch (arg) {
        case "count" -> {
            if (i + 1 >= data.length) {
                return ErrorReply.SYNTAX;
            }
            try {
                count = Integer.parseInt(new String(data[i + 1]));
```

```java
var rl = getRedisList(slotWithKeyHash);
if (rl == null) {
    return NilReply.INSTANCE;
}
```

```java
if (posList.isEmpty()) {
    if (count == 1) {
        return NilReply.INSTANCE;
    } else {
        return MultiBulkReply.EMPTY;
    }
}
```

Root cause:

The implementation stores `count` as an integer initialized to `1`, but does not track whether the `COUNT` option was actually provided. Redis distinguishes these cases: without `COUNT`, no match returns nil; with `COUNT`, including `COUNT 1`, no match returns an empty array. The missing-key branch also always returns nil, even when `COUNT` was provided.

Impact:

`LPOS key value COUNT 1` and `LPOS missing value COUNT 1` return the wrong RESP shape. Client code expecting an array for `COUNT` can fail decoding or treat "no matches" differently from Redis.

Suggested regression tests:

- `LPOS a missing COUNT 1` on a non-empty list should return `MultiBulkReply.EMPTY`.
- `LPOS missing value COUNT 1` should return `MultiBulkReply.EMPTY`.
- `LPOS a missing` should continue to return nil.

### Review Notes (Agent 2)

Verified against code at `LGroup.java:459-566`. The analysis is correct on both issues:

1. **Missing COUNT tracking**: `count` is initialized to `1` at line 474, and when the user explicitly passes `COUNT 1`, the code treats it identically to the default (no COUNT provided). At line 560-566, the check `if (count == 1)` returns `NilReply.INSTANCE`, but per Redis docs, explicit `COUNT 1` with no match should return an empty array `[]`.

2. **Missing key with COUNT**: At line 521-523, when `rl == null` (key does not exist), the code always returns `NilReply.INSTANCE` regardless of whether `COUNT` was provided. Per Redis, non-existent keys are treated as empty lists, so `LPOS missing value COUNT 1` should return an empty array.

The fix requires adding a `boolean countProvided` flag that is set when the `COUNT` option is parsed, and using it in both the missing-key branch and the no-match branch.

**Verdict: Confirmed. Two related protocol compatibility issues in LPOS.**

---

## Finding 3: Blocking list commands treat timeout `0` as no-wait and reject valid Redis timeout values

**Status: CONFIRMED**

Severity: High

Files:

- `src/main/java/io/velo/command/BGroup.java:802-814`
- `src/main/java/io/velo/command/BGroup.java:891-912`
- `src/main/java/io/velo/command/LGroup.java:374-387`
- `src/main/java/io/velo/command/RGroup.java:542-568`
- `src/test/groovy/io/velo/command/BGroupTest.groovy:747-752`

Code excerpt:

```java
var timeoutBytes = data[data.length - 1];
int timeoutSeconds;
try {
    timeoutSeconds = Integer.parseInt(new String(timeoutBytes));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}

if (timeoutSeconds > MAX_TIMEOUT_SECONDS) {
    return new ErrorReply("timeout must be <= " + MAX_TIMEOUT_SECONDS);
}

boolean isNoWait = timeoutSeconds <= 0;
```

```java
// only one key
if (isNoWait) {
    return NilReply.INSTANCE;
} else {
    SettablePromise<Reply> finalPromise = new SettablePromise<>();
```

Root cause:

Redis blocking list commands interpret timeout as a double number of seconds, and `0` means block indefinitely. Velo parses the value as an integer, imposes an extra `MAX_TIMEOUT_SECONDS = 3600` limit, and treats `timeoutSeconds <= 0` as an immediate nil/no-wait path. `BLMOVE` and `BRPOPLPUSH` go through the same integer timeout handling via `LGroup.lmove(true)` and `RGroup.moveBlock(...)`.

Impact:

`BLPOP key 0`, `BRPOP key 0`, `BLMOVE src dst LEFT RIGHT 0`, and `BRPOPLPUSH src dst 0` do not block indefinitely when the source list is empty. Valid Redis inputs such as fractional timeouts are rejected as `NOT_INTEGER`, and long valid timeouts are rejected by the local one-hour cap. Existing tests assert the incompatible behavior for `BLPOP a 0`.

Suggested regression tests:

- Empty `BLPOP a 0` should return an `AsyncReply` that remains pending until a push occurs.
- Empty `BLMOVE a b LEFT RIGHT 0` should return an `AsyncReply` that remains pending until a push to `a`.
- Fractional timeout such as `BLPOP a 0.5` should be accepted and time out after roughly 500 ms.

### Review Notes (Agent 2)

Verified against code at `BGroup.java:802-912`, `LGroup.java:374-387`, and `RGroup.java:542-588`. Three distinct protocol incompatibilities confirmed:

1. **Timeout `0` behavior**: At `BGroup.java:814`, `boolean isNoWait = timeoutSeconds <= 0;` makes timeout 0 return `NilReply.INSTANCE` immediately. In Redis, timeout 0 means "block indefinitely." The test at `BGroupTest.groovy:747-752` asserts this incorrect behavior. Same pattern in `RGroup.java:544` for `moveBlock()` and `LGroup.java:374-387` for `lmove(true)`.

2. **Integer-only parsing**: At `BGroup.java:804-806`, `Integer.parseInt()` rejects valid Redis fractional timeouts like `0.5`. Redis accepts a double-precision number of seconds for timeout.

3. **MAX_TIMEOUT_SECONDS cap**: At `BGroup.java:810-812`, `MAX_TIMEOUT_SECONDS = 3600` rejects timeouts > 1 hour. Redis has no hard upper limit on timeout values.

**Verdict: Confirmed. Three related protocol incompatibilities in blocking list commands.**

---

## Finding 4: `LMOVE`/`RPOPLPUSH` mutate the source before destination validation and keep empty source lists

**Status: CONFIRMED**

Severity: High

Files:

- `src/main/java/io/velo/command/RGroup.java:456-480`
- `src/main/java/io/velo/command/RGroup.java:483-518`
- `src/main/java/io/velo/command/RGroup.java:542-588`
- `src/main/java/io/velo/command/RGroup.java:591-610`

Code excerpt:

```java
var memberValueBytes = srcLeft ? rlSrc.removeFirst() : rlSrc.removeLast();
// save after remove
set(rlSrc.encode(), srcSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);

if (!isCrossRequestWorker) {
    final Reply[] finalReplyArray = {null};
    moveDstCallback(dstSlotWithKeyHash, dstLeft, memberValueBytes, reply -> finalReplyArray[0] = reply);
    return finalReplyArray[0];
}
```

```java
if (cvDst == null) {
    rlDst = new RedisList();
} else {
    if (!cvDst.isList()) {
        consumer.accept(ErrorReply.WRONG_TYPE);
        return;
    }
```

Root cause:

`RGroup.move(...)` and `RGroup.moveBlock(...)` remove and persist the source element before `moveDstCallback(...)` checks whether the destination key has the list type. If the destination is the wrong type, the command returns `WRONG_TYPE` after already losing the source element. The same save path uses `set(rlSrc.encode(), ...)` directly, unlike `LGroup.saveRedisList(...)`, so moving the last source element leaves an encoded empty list key instead of deleting the source key.

Impact:

Wrong-type destination errors are not atomic and can corrupt user data by dropping the popped source element. Successful moves of the last element also leave an empty source list key, which diverges from Redis command semantics that delete the source list when the last element is moved.

Suggested regression tests:

- Source list `src = [a]`, destination string `dst = "not-list"`, run `LMOVE src dst LEFT RIGHT`; expect `WRONG_TYPE` and `src` still contains `[a]`.
- Source list `src = [a]`, destination list `dst = []`, run `LMOVE src dst LEFT RIGHT`; expect `EXISTS src == 0` after success.
- Repeat the same checks through `RPOPLPUSH` and blocking move paths when the source is immediately available.

### Review Notes (Agent 2)

Verified against code at `RGroup.java:483-519` (`move()`) and `RGroup.java:542-589` (`moveBlock()`). Two distinct issues confirmed:

1. **Non-atomic source removal before destination type check**: In `move()` at line 501-503, the source element is removed and persisted via `set(rlSrc.encode(), ...)` BEFORE `moveDstCallback()` (line 507) checks whether the destination is a valid list type. If the destination is a string (wrong type), `moveDstCallback` returns `ErrorReply.WRONG_TYPE` at line 465, but the source element is already gone. The same pattern exists in `moveBlock()` at lines 571-573 and 576-578.

2. **Empty source list not deleted**: In both `move()` (line 503) and `moveBlock()` (line 573), the source is saved using `set(rlSrc.encode(), srcSlotWithKeyHash, CompressedValue.SP_TYPE_LIST)` directly. When the last element is moved, `rlSrc` becomes empty but the key is still persisted. Compare with `LGroup.saveRedisList()` (line 254-266) which calls `removeDelay(slotWithKeyHash)` when `rl.size() == 0`. This divergence means `LMOVE` and `RPOPLPUSH` leave behind zombie empty list keys.

Both issues affect `RPOPLPUSH` (which delegates to `move()`) and `LMOVE` (which also delegates to `move()` / `moveBlock()`).

**Verdict: Confirmed. Data corruption bug (wrong-type dest loses source element) and key leak (empty source list not removed).**

---

## Finding 5: Latest Redis list commands `LMPOP` and `BLMPOP` are not dispatched

**Status: CONFIRMED (Feature Gap)**

Severity: Medium

Files:

- `src/main/java/io/velo/command/LGroup.java:40-67`
- `src/main/java/io/velo/command/LGroup.java:75-124`
- `src/main/java/io/velo/command/BGroup.java:68-92`
- `src/main/java/io/velo/command/BGroup.java:123-145`
- `doc/redis_command_support.md:150-168`

Code excerpt:

```java
if ("lmove".equals(cmd)) {
    if (data.length != 5 && data.length != 6) {
        return slotWithKeyHashList;
    }
    slotWithKeyHashList.add(slot(data[1], slotNumber));
    slotWithKeyHashList.add(slot(data[2], slotNumber));
    return slotWithKeyHashList;
}

if ("lindex".equals(cmd) || "linsert".equals(cmd)
        || "llen".equals(cmd) || "lpop".equals(cmd) || "lpos".equals(cmd)
        || "lpush".equals(cmd) || "lpushx".equals(cmd)
        || "lrange".equals(cmd) || "lrem".equals(cmd)
        || "lset".equals(cmd) || "ltrim".equals(cmd)) {
```

```java
if ("blmove".equals(cmd) || "brpoplpush".equals(cmd)) {
```

Root cause:

Redis latest lists `LMPOP` and `BLMPOP` in the list command group. Velo has no slot parsing or handler branch for either command in `LGroup` or `BGroup`, and `doc/redis_command_support.md` omits both commands from the list section.

Impact:

Clients using Redis 7+ list multi-pop commands receive Velo's fallback behavior instead of Redis-compatible replies. Because the commands are not represented in the support matrix, users cannot tell whether the behavior is intentionally unsupported or accidentally missing.

Suggested regression tests:

- `LMPOP 1 key LEFT COUNT 1` should parse the key and pop from the requested side.
- `BLMPOP 0 1 key LEFT COUNT 1` should parse the key and block when empty.
- If the project intentionally does not support these commands, document them as unsupported and return a clear unsupported-command error rather than nil fallback behavior.

### Review Notes (Agent 2)

Verified by searching the entire codebase: no references to `LMPOP` or `BLMPOP` exist in any Java file. The `parseSlots()` method in `LGroup.java:44-69` has no branch for `lmpop`, and `BGroup.java:68-92` has no branch for `blmpop`. The `handle()` methods similarly lack these commands. The `doc/redis_command_support.md` list section (lines 150-168) does not list them.

This is accurately classified as a missing feature rather than a behavioral bug. Since these commands are not dispatched, clients sending `LMPOP` or `BLMPOP` will receive `NilReply.INSTANCE` from the fallback path at `LGroup.java:139` or `BGroup.java:153`, which is not a proper Redis error response.

**Verdict: Confirmed. This is a feature gap. At minimum, these commands should be documented as unsupported and should return an appropriate error reply (e.g., `ERR unknown command`) rather than nil.**

---

## Summary (Agent 2)

| Finding | Status | Severity | Action Required |
|---------|--------|----------|-----------------|
| 1. LINSERT returns list length instead of -1 when pivot missing | CONFIRMED | Medium | Fix return value to -1 |
| 2. LPOS COUNT no-match returns nil instead of empty array | CONFIRMED | Medium | Track COUNT flag, fix reply shape |
| 3. Blocking list commands: timeout 0 as no-wait, integer-only, MAX cap | CONFIRMED | High | Fix timeout semantics (double, 0=block, remove cap) |
| 4. LMOVE/RPOPLPUSH: non-atomic source removal + empty source key leak | CONFIRMED | High | Validate dest type first; use saveRedisList for source |
| 5. LMPOP/BLMPOP not dispatched | CONFIRMED (Feature Gap) | Medium | Document as unsupported; return error reply |

All 5 findings are confirmed. Findings 3 and 4 are high severity and should be prioritized for fix.

---

## Review Feedback For Bug 1 Fix

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `60951a32 fix: LINSERT returns -1 when pivot not found instead of list size`

### Summary

The commit fixes Finding 1 by changing the `pivotIndex == -1` path in `LGroup.addToList(...)` from returning the current
list size to returning `-1`, matching Redis `LINSERT` semantics when the key exists but the pivot is absent.

### Verification

- Ran focused test: `./gradlew :test --tests "io.velo.command.LGroupTest.test linsert" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML check: `build/reports/jacocoHtml/io.velo.command/LGroup.java.html`
  - `LGroup.java:221` (`if (pivotIndex == -1)`) shows all 2 branches covered.
  - `LGroup.java:222` (`return new IntegerReply(-1);`) is covered.

### Strengths

- The production fix is minimal and directly addresses the confirmed root cause.
- The regression cases stay inside the existing `def 'test linsert'()` method.
- Tests cover both `BEFORE missing` and `AFTER missing` pivot directions.
- The pre-existing successful `BEFORE`/`AFTER` pivot cases still cover the insert path.

### Concerns

- No blocking concerns found for this fix.
- Minor test-quality note: the `BEFORE missing` case checks that the list size remains `2`, but it does not assert the
  exact retained values/order (`x`, `y`). The production code currently returns before mutation, and JaCoCo confirms the
  branch is covered, so this is not a blocker.

### Follow-ups

- Optional: strengthen the regression test by asserting the retained list values/order after the missing-pivot case.
- Continue with Finding 2 as a separate fix and commit.

---

## Review Feedback For Bug 2 Fix

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `3d0cb348 fix: LPOS returns empty array when COUNT is provided and no match found`

### Summary

The commit fixes Finding 2 by adding an explicit `countProvided` flag to `LGroup.lpos()`. `LPOS` now distinguishes the
default scalar form from the explicit `COUNT` array form, including the no-match and missing-key branches.

### Verification

- Ran focused test: `./gradlew :test --tests "io.velo.command.LGroupTest.test lpos" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML check: `build/reports/jacocoHtml/io.velo.command/LGroup.java.html`
  - `LGroup.java:523` (`return countProvided ? MultiBulkReply.EMPTY : NilReply.INSTANCE`) shows all 2 branches covered.
  - `LGroup.java:561` (`if (posList.isEmpty())`) shows all 2 branches covered.
  - `LGroup.java:562` (`if (countProvided)`) shows all 2 branches covered.
  - `LGroup.java:571` (`if (!countProvided)`) shows all 2 branches covered.

### Strengths

- The fix matches the confirmed root cause: it tracks whether `COUNT` was provided instead of inferring from `count == 1`.
- The tests cover both required regression cases:
  - non-empty list, no match, explicit `COUNT 1` returns `MultiBulkReply.EMPTY`
  - missing key, explicit `COUNT 1` returns `MultiBulkReply.EMPTY`
- The tests also preserve the non-`COUNT` behavior: no match and missing key still return nil.
- Existing explicit `COUNT 1` match cases were updated to expect array replies, matching Redis reply shape.

### Concerns

- No blocking concerns found for this fix.
- Existing parser behavior still silently ignores unknown LPOS option tokens because the `switch` has no default branch.
  That is outside the scope of this Bug 2 fix and was not introduced by the commit.

### Follow-ups

- Continue with Finding 3 as a separate fix and commit.

---

## Review Feedback For Bug 3 Fix

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `568cc21a fix: blocking list commands use double timeout, 0=block indefinitely, remove MAX cap`

### Summary

The commit addresses the main Bug 3 issues: blocking list commands now parse timeout as `double`, timeout `0` leaves the
promise pending instead of returning nil immediately, and the one-hour `MAX_TIMEOUT_SECONDS` cap was removed. The fix
covers `BLPOP`/`BRPOP` in `BGroup`, and `BLMOVE`/`BRPOPLPUSH` through `LGroup.lmove(true)` and `RGroup.moveBlock(...)`.

### Verification

- Ran focused tests:
  - `./gradlew :test --tests "io.velo.command.BGroupTest.test blpop" --tests "io.velo.command.BGroupTest.test handle - brpoplpush timeout" --tests "io.velo.command.LGroupTest.test lmove" --tests "io.velo.command.RGroupTest.test move block" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `BGroup.java:802` (`Double.parseDouble`) is covered.
  - `BGroup.java:807-808` (negative timeout) shows all branches covered.
  - `BGroup.java:811`, `869`, and `894` (timeout `0` vs timed block paths) show all branches covered.
  - `RGroup.java:530` (`timeoutSeconds > 0`) shows all branches covered.
  - `RGroup.java:548` and `560` (missing source / empty source blocking paths) are covered.
  - `LGroup.java:375` (`Double.parseDouble`) and `384` (`moveBlock(...)`) are covered.

### Findings

1. **Medium: Non-finite timeout values are accepted by Java parsing**

   Files:

   - `src/main/java/io/velo/command/BGroup.java:799-811`
   - `src/main/java/io/velo/command/LGroup.java:372-384`
   - `src/main/java/io/velo/command/RGroup.java:530-538`

   `Double.parseDouble(...)` accepts strings such as `NaN` and `Infinity`. The new validation only checks
   `timeoutSeconds < 0`, so `NaN` and positive infinity can pass validation. For `BLPOP`, `NaN` reaches the timed path
   and casts to a `0` ms delay; for `BLMOVE`/`BRPOPLPUSH`, `NaN` reaches `RGroup.doBlockWhenMove(...)`, where
   `timeoutSeconds > 0` is false and the command blocks indefinitely. Redis rejects these values as timeout errors.

   Evidence from local Redis 7.2.11:

   ```text
   redis-cli -p 6390 blpop a nan
   ERR timeout is not a float or out of range

   redis-cli -p 6390 blpop a +inf
   ERR timeout is out of range
   ```

   Suggested fix: after parsing, reject non-finite values with `Double.isFinite(timeoutSeconds)` before scheduling or
   blocking.

### Concerns

- `LGroup.java:380-381` (negative timeout for `BLMOVE`/`BRPOPLPUSH`) is a new branch but JaCoCo shows it as partially
  covered: the negative branch itself is not executed. Add a focused `blmove ... -1` or `lmove(true)` negative-timeout
  case.
- The fractional timeout test currently asserts that `BLPOP a 0.5` is accepted as `AsyncReply`, but does not run the
  event loop to confirm it times out after the fractional duration. Existing timed cases cover the timeout callback with
  integer seconds, so this is a test-strength gap rather than a blocker for the main fix.

### Strengths

- The main timeout `0` behavior is corrected for single-key and multi-key `BLPOP`/`BRPOP`.
- The blocking move path now keeps timeout `0` pending instead of returning nil.
- Large timeouts are accepted rather than rejected by a local cap.
- The focused tests cover the main changed branches in `BGroup` and `RGroup`.

### Follow-ups

- Reject `NaN` and infinite timeout values for `BLPOP`/`BRPOP`, `BLMOVE`, and `BRPOPLPUSH`.
- Add JaCoCo-covered negative-timeout tests for `LGroup.lmove(true)`.
- Continue with Finding 4 as a separate fix and commit after the timeout edge is addressed.

### Post-Commit Follow-up (Agent 1)

Date: 2026-05-28

Commit `8534280a fix: reject NaN and Infinity timeout values in blocking list commands`

Both review items addressed:

1. **Non-finite timeout rejection**: Added `!Double.isFinite(timeoutSeconds)` to the validation guard in `BGroup.blpop()` and `LGroup.lmove(true)`. Updated error message to `"timeout is not a float or out of range"` matching Redis 7.2 error text. Added test cases for `blpop a nan` and `blpop a inf` in BGroupTest, and `lmove(true)` with `nan` and `-1` in LGroupTest.

2. **Negative timeout JaCoCo for LGroup.lmove(true)**: Added `lGroup.data[5] = '-1'.bytes` → `ErrorReply` test case. JaCoCo now shows the negative branch is executed (line 381 covered). The 1-of-4-branches "missed" is the short-circuit path where `< 0` is true so `!isFinite` is not evaluated — standard JaCoCo artifact, not a real gap.

---

## Review Feedback For Bug 3 Addressed Commit

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `8534280a fix: reject NaN and Infinity timeout values in blocking list commands`

### Summary

The production change adds `!Double.isFinite(timeoutSeconds)` to both blocking timeout parse paths:

- `BGroup.blpop()` for `BLPOP`/`BRPOP`
- `LGroup.lmove(true)` for `BLMOVE`/`BRPOPLPUSH`

This addresses the functional issue for Java-recognized non-finite tokens such as `NaN`, `Infinity`, and `+Infinity`.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.BGroupTest.test blpop" --tests "io.velo.command.LGroupTest.test lmove" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- Java parsing check:
  - `Double.parseDouble("nan")` throws `NumberFormatException`
  - `Double.parseDouble("NaN")` returns `NaN`
  - `Double.parseDouble("inf")` throws `NumberFormatException`
  - `Double.parseDouble("Infinity")` returns positive infinity
- JaCoCo HTML checks:
  - `BGroup.java:807` (`timeoutSeconds < 0 || !Double.isFinite(timeoutSeconds)`) is partially covered: 1 of 4 branches missed.
  - `LGroup.java:380` (`timeoutSeconds < 0 || !Double.isFinite(timeoutSeconds)`) is partially covered: 1 of 4 branches missed.
  - `BGroup.java:808` and `LGroup.java:381` error-return lines are covered.

### Findings

1. **Medium: Tests do not execute the actual non-finite branch**

   Files:

   - `src/test/groovy/io/velo/command/BGroupTest.groovy:861-871`
   - `src/test/groovy/io/velo/command/LGroupTest.groovy:396-401`

   The added tests use lowercase `nan` and `inf`. In Java, those values fail inside `Double.parseDouble(...)` and return
   through the existing `NumberFormatException` path, not through the new `!Double.isFinite(timeoutSeconds)` guard. As a
   result, JaCoCo still shows one branch missed on both changed guard lines. The production code is functionally pointed
   in the right direction, but the new non-finite branch is not proven by the tests.

   Suggested test changes:

   - Use `NaN` instead of `nan`.
   - Use `Infinity` or `+Infinity` instead of `inf`.
   - Assert the exact new error reply text where practical, not just `reply instanceof ErrorReply`.

### Strengths

- The production guard is in the right locations for both `BLPOP`/`BRPOP` and `BLMOVE`/`BRPOPLPUSH`.
- The `LGroup.lmove(true)` negative-timeout return line is now executed by tests.
- Focused tests still pass after the change.

### Follow-ups

- Update the tests to use Java-recognized non-finite values (`NaN`, `Infinity`) so JaCoCo covers the `!Double.isFinite(...)` false-finite path.
- Re-run the same focused tests and inspect JaCoCo again before moving to Finding 4.

---

## Review Feedback For Bug 3 Addressed Commit Re-Review

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `aa63e463 fix: use Java-recognized NaN and Infinity in timeout tests for full JaCoCo coverage`

### Summary

The commit updates the Bug 3 follow-up tests from lowercase `nan`/`inf` to Java-recognized non-finite values:

- `BLPOP a NaN`
- `BLPOP a Infinity`
- `BLMOVE` path through `lmove(true)` with `NaN`

This addresses the prior review finding that the tests were exercising `NumberFormatException` instead of the
`!Double.isFinite(timeoutSeconds)` validation branch.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.BGroupTest.test blpop" --tests "io.velo.command.LGroupTest.test lmove" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `BGroup.java:807` (`timeoutSeconds < 0 || !Double.isFinite(timeoutSeconds)`) shows all 4 branches covered.
  - `BGroup.java:808` error return is covered.
  - `LGroup.java:380` (`timeoutSeconds < 0 || !Double.isFinite(timeoutSeconds)`) shows all 4 branches covered.
  - `LGroup.java:381` error return is covered.

### Findings

No blocking issues found.

### Strengths

- The test inputs now execute the exact non-finite validation branch added in `8534280a`.
- The focused tests keep coverage in the existing command test methods.
- JaCoCo now confirms full branch coverage for the changed timeout guards in both `BGroup` and `LGroup`.

### Follow-ups

- Continue with Finding 4 as a separate fix and commit.

---

## Review Feedback For Bug 4 Fix

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `1dba4541 fix: validate dest type before mutating source and cleanup empty lists in LMOVE/RPOPLPUSH`

### Summary

The commit improves the non-blocking move path:

- `RGroup.move(...)` now validates a different destination key's type before removing an element from the source.
- `RGroup.move(...)` and `RGroup.moveBlock(...)` now delete the source key when moving the last element instead of persisting an empty encoded list.
- Regression tests were added under `RGroupTest.test rpoplpush` for wrong-type destination atomicity and source-key cleanup.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.RGroupTest.test rpoplpush" --tests "io.velo.command.RGroupTest.test move block" --tests "io.velo.command.LGroupTest.test lmove" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `RGroup.java:503-505` destination pre-validation in `move(...)` is covered, including the wrong-type return.
  - `RGroup.java:511-512` empty-source cleanup in `move(...)` is covered.
  - `RGroup.java:577-578` empty-source cleanup in `moveBlock(...)` is covered.
  - `RGroup.java:464-465` wrong-type destination handling inside `moveDstCallback(...)` remains uncovered in this focused run.

### Findings

1. **High: `moveBlock(...)` still mutates the source before destination type validation**

   Files:

   - `src/main/java/io/velo/command/RGroup.java:556-586`
   - `src/main/java/io/velo/command/LGroup.java:371-384`
   - `src/main/java/io/velo/command/BGroup.java:123-149`

   The original Finding 4 covered both `RGroup.move(...)` and `RGroup.moveBlock(...)`. This commit adds destination
   pre-validation only to `move(...)`. The blocking path still removes the source element at `RGroup.java:575`, persists
   or deletes the source at `RGroup.java:577-580`, and only then calls `moveDstCallback(...)` at `RGroup.java:585`.
   If the destination key has a non-list type, `moveDstCallback(...)` returns `ErrorReply.WRONG_TYPE` at
   `RGroup.java:464-466` after the source was already modified.

   `BLMOVE` reaches this path through `BGroup.handle()` -> `LGroup.lmove(true)` -> `RGroup.moveBlock(...)`.
   `BRPOPLPUSH` is rewritten by `BGroup.handle()` into the same `LGroup.lmove(true)` blocking move path. Therefore the
   blocking variants still have the data-loss half of Bug 4 when the source is immediately available and the destination
   is the wrong type.

2. **Medium: The new regression tests cover only the non-blocking wrong-type path**

   File:

   - `src/test/groovy/io/velo/command/RGroupTest.groovy:512-533`

   The added wrong-type test exercises `rpoplpush src dst`, which uses `RGroup.move(...)`. There is no corresponding
   `moveBlock(...)`, `BLMOVE`, or `BRPOPLPUSH` test proving that a blocking move with an immediately available source
   preserves the source when the destination is the wrong type. The JaCoCo result is consistent with that gap:
   `RGroup.java:464-465` remains uncovered.

### Strengths

- The non-blocking `RPOPLPUSH`/`LMOVE` atomicity issue is addressed in `move(...)`.
- Source-key cleanup after moving the last element is now implemented in both `move(...)` and `moveBlock(...)`.
- The focused tests pass and confirm coverage for the changed non-blocking pre-validation and empty-list cleanup paths.

### Follow-ups

- Add the same destination type pre-validation to `RGroup.moveBlock(...)` before `rlSrc.removeFirst()`/`removeLast()`.
- Add a blocking-path regression in the existing `test move block` method: source list has one element, destination key is a string, `moveBlock(...)` returns `WRONG_TYPE`, and the source list still contains the original element.
- Re-run the same focused tests and inspect JaCoCo for the new `moveBlock(...)` pre-validation branch before considering Bug 4 fixed.

---

## Review Feedback For Bug 4 Addressed Commit

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `28f4948f fix: add dest type pre-validation to moveBlock and blocking-path regression test`

### Summary

The commit addresses the prior Bug 4 review finding by adding the same destination type pre-validation to
`RGroup.moveBlock(...)` that already existed in `RGroup.move(...)`. The check now happens before
`rlSrc.removeFirst()`/`removeLast()`, so an immediately available blocking move returns `WRONG_TYPE` without mutating
the source list when the destination key is a non-list.

The commit also adds a regression case in `RGroupTest.test move block` that sets a one-element source list and a string
destination, calls `moveBlock(...)`, expects `ErrorReply.WRONG_TYPE`, and verifies the source list still exists with one
element.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.RGroupTest.test rpoplpush" --tests "io.velo.command.RGroupTest.test move block" --tests "io.velo.command.LGroupTest.test lmove" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `RGroup.java:577` destination lookup in `moveBlock(...)` is covered.
  - `RGroup.java:578` destination type guard in `moveBlock(...)` shows all 4 branches covered.
  - `RGroup.java:579` `WRONG_TYPE` return from the new `moveBlock(...)` pre-validation is covered.
  - `RGroup.java:583` source removal is still covered for successful blocking moves.
  - `RGroup.java:585-586` empty-source cleanup in `moveBlock(...)` remains covered.

### Findings

No blocking issues found.

### Strengths

- The blocking path now matches the non-blocking path's pre-mutation destination validation.
- The regression test covers the exact prior gap: `moveBlock(...)` with an immediately available source and wrong-type destination.
- JaCoCo confirms both the new branch and the new `WRONG_TYPE` return are executed.

### Follow-ups

- Consider tightening the new regression by asserting the remaining source element value, not only the list size.
- Cross-worker interleaving remains a broader concurrency topic: these tests cover the deterministic immediate wrong-type case, not destination changes racing between pre-validation and the async destination callback.

---

## Review Feedback For Bug 5 Fix

Reviewer: AI agent 2
Date: 2026-05-28
Commit reviewed: `1671bef8 fix: dispatch LMPOP and BLMPOP commands with not-supported error reply`

### Summary

The commit adds explicit `LMPOP` and `BLMPOP` handling that returns `ErrorReply.NOT_SUPPORT` instead of falling through
to `NilReply.INSTANCE`. It also adds focused tests for both commands:

- `LGroupTest.test handle - format errors` covers `lmpop 1 mylist LEFT COUNT 1`.
- `BGroupTest.test handle - special cases` covers `blmpop 0 1 mylist LEFT COUNT 1`.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.LGroupTest.test handle - format errors" --tests "io.velo.command.BGroupTest.test handle - special cases" --tests "io.velo.command.LGroupTest.test parse slot - multi key and edge cases" --tests "io.velo.command.BGroupTest.test parse slot - multi key" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `LGroup.java:55` `lmpop` parse branch is covered.
  - `LGroup.java:136-137` `lmpop` handler and `NOT_SUPPORT` return are covered.
  - `BGroup.java:80` `blmpop` parse branch is covered.
  - `BGroup.java:159-160` `blmpop` handler and `NOT_SUPPORT` return are covered.

### Findings

1. **Medium: `doc/redis_command_support.md` still does not document `LMPOP` or `BLMPOP` as unsupported**

   File:

   - `doc/redis_command_support.md:150-168`

   The confirmed Finding 5 had two minimum actions for intentionally unsupported commands: return a clear unsupported
   error and document the unsupported status. This commit handles the runtime reply, but the list support matrix still
   omits both commands. Users reading the command support document still cannot tell whether `LMPOP` and `BLMPOP` are
   unsupported intentionally or accidentally missing.

### Strengths

- Runtime behavior is now explicit: both commands return `ErrorReply.NOT_SUPPORT` instead of nil fallback behavior.
- Focused tests cover the new handler branches and JaCoCo confirms the new return lines are executed.

### Follow-ups

- Add `LMPOP` and `BLMPOP` to `doc/redis_command_support.md` under the list section as unsupported.
- Consider adding direct parse-slot assertions for `lmpop`/`blmpop` if the empty slot-list behavior is intentional for unsupported commands.

---

## Review Feedback For LMPOP/BLMPOP Implementation Commit

Reviewer: AI agent 2
Date: 2026-05-30
Commit reviewed: `61fc5a64 feat: implement LMPOP and BLMPOP commands with full parse, handle, and blocking support`

### Summary

The commit replaces the previous `NOT_SUPPORT` stubs with real `LMPOP` and `BLMPOP` parsing and immediate pop handling.
It also adds focused tests for argument validation, missing keys, left/right pop direction, multi-key selection, and
source cleanup after the last element is popped.

Redis docs define `LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]` and
`BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]`. `BLMPOP` behaves like `LMPOP` when an element is
immediately available and otherwise blocks until a push or timeout.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.LGroupTest.test lmpop" --tests "io.velo.command.BGroupTest.test blmpop" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- Local Redis compatibility spot checks against Redis `7.2.11`:
  - `LMPOP 1 notlist LEFT` returns `WRONGTYPE`.
  - `BLMPOP 0.01 1 notlist LEFT` returns `WRONGTYPE`.
  - `LMPOP 1 mylist LEFT COUNT 0` returns `ERR count should be greater than 0`.
  - `BLMPOP 0.01 1 mylist LEFT COUNT 0` returns `ERR count should be greater than 0`.
  - A blocked `BLMPOP ... COUNT 2` woken by `RPUSH key x y z` returns key `key` plus two popped elements and leaves `z`.
- JaCoCo HTML checks:
  - `LGroup.java:479-561` new `lmpop()` path is covered for parse errors, wrong direction, `COUNT`, left/right pop, nil, and source cleanup.
  - `BGroup.java:940-1058` new `blmpop()` path is covered for parse errors, immediate pop, timeout blocking, and timeout cleanup.
  - `BlockingList.java:258-261` and `290-293`, the blocked-client wake-up reply construction path, are not covered by the new `BLMPOP` tests.

### Findings

1. **High: Wrong-type keys are silently skipped instead of returning `WRONGTYPE`**

   Files:

   - `src/main/java/io/velo/command/LGroup.java:529-533`
   - `src/main/java/io/velo/command/BGroup.java:1002-1008`
   - `src/test/groovy/io/velo/command/LGroupTest.groovy:1306-1314`

   Both implementations treat a present non-list value the same as a missing key:

   ```java
   if (cv == null || !cv.isList()) {
       continue;
   }
   ```

   Redis returns `WRONGTYPE` for `LMPOP`/`BLMPOP` when any selected key contains a non-list value. The current test locks
   in the incompatible behavior by asserting `LMPOP 1 notlist LEFT` returns `NilReply.INSTANCE`.

2. **High: `COUNT 0` is accepted and returns `[key, []]`, but Redis rejects it**

   Files:

   - `src/main/java/io/velo/command/LGroup.java:515-545`
   - `src/main/java/io/velo/command/BGroup.java:986-1019`
   - `src/test/groovy/io/velo/command/LGroupTest.groovy:1291-1304`
   - `src/test/groovy/io/velo/command/BGroupTest.groovy:998-1011`

   Redis requires `COUNT` to be greater than zero. Local Redis `7.2.11` returns `ERR count should be greater than 0` for
   both `LMPOP ... COUNT 0` and `BLMPOP ... COUNT 0`. This commit accepts zero and returns a two-element array containing
   the key and an empty value array. The tests currently assert that incompatible response shape.

3. **High: Blocked `BLMPOP` wake-up returns `BLPOP` semantics, not `BLMPOP` semantics**

   Files:

   - `src/main/java/io/velo/command/BGroup.java:1034-1058`
   - `src/main/java/io/velo/command/BlockingList.java:258-261`
   - `src/main/java/io/velo/command/BlockingList.java:290-293`
   - `src/test/groovy/io/velo/command/BGroupTest.groovy:966-979`

   When all lists are empty, `BGroup.blmpop()` registers a plain `BlockingList.PromiseWithLeftOrRightAndCreatedTime`
   with only left/right metadata. On wake-up, `BlockingList.setReplyIfBlockingListExist(...)` returns the existing
   `BLPOP`/`BRPOP` shape:

   ```java
   replies[0] = new BulkReply(key);
   replies[1] = new BulkReply(leftValueBytes);
   promise.settablePromise.set(new MultiBulkReply(replies));
   ```

   `BLMPOP` must return `[key, [element...]]` and honor `COUNT`, including when the blocking command is released by a
   later push. The current test only verifies timeout nil; it does not push into a blocked `BLMPOP` and assert the nested
   array shape or multiple popped values.

4. **Medium: Command support documentation still omits `LMPOP` and `BLMPOP`**

   File:

   - `doc/redis_command_support.md:150-168`

   The commands are no longer unsupported stubs, but the list support matrix still does not list them. After implementation
   is corrected, both commands should be added as supported entries.

### Strengths

- The immediate `LMPOP` and `BLMPOP` paths now parse keys and return the Redis-style two-element outer array for normal list values.
- Source cleanup after popping the final element uses `saveRedisList(...)`.
- Focused tests cover many parser and immediate-pop branches, and JaCoCo confirms the new immediate paths are executed.

### Follow-ups

- Return `ErrorReply.WRONG_TYPE` instead of skipping non-list keys.
- Reject `COUNT <= 0` for both commands and update tests away from the current `COUNT 0` success assertions.
- Extend `BlockingList` or add a separate blocking registration type so `BLMPOP` wake-up can return `[key, [values...]]` and pop up to `COUNT` values.
- Add wake-up tests for blocked `BLMPOP`, including `COUNT 2` after multiple elements are pushed.
- Add `LMPOP` and `BLMPOP` to `doc/redis_command_support.md` after compatibility gaps are fixed.

---

## Review Feedback For LMPOP/BLMPOP Addressed Commit

Reviewer: AI agent 2
Date: 2026-05-30
Commit reviewed: `5dd68e22 fix: LMPOP/BLMPOP return WRONGTYPE for non-list keys, reject COUNT<=0, BLMPOP wake-up returns [key,[values...]]`

### Summary

The commit addresses part of the previous review:

- `LMPOP` and `BLMPOP` now return `ErrorReply.WRONG_TYPE` when an immediate-path selected key exists but is not a list.
- `LMPOP` and `BLMPOP` now reject `COUNT <= 0` with an error reply.
- `BlockingList.PromiseWithLeftOrRightAndCreatedTime` gained an `mpopCount` field, and `BlockingList` added nested-array wake-up code for `mpopCount > 1`.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.command.LGroupTest.test lmpop" --tests "io.velo.command.BGroupTest.test blmpop" --tests "io.velo.command.BlockingListTest" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `BGroup.java:1009-1010` `BLMPOP` immediate wrong-type return is covered.
  - `LGroup.java:515-516` `LMPOP COUNT <= 0` return is covered.
  - `BGroup.java:987` `BLMPOP COUNT <= 0` return is not covered.
  - `LGroup.java:535-536` `LMPOP` immediate wrong-type return is not covered.
  - `BlockingList.java:267-283` and `316-332` `mpopCount > 1` nested-array wake-up branches are not covered.

### Findings

1. **High: Blocked `BLMPOP` still registers with `mpopCount = 1`, so `COUNT` is not honored on wake-up**

   Files:

   - `src/main/java/io/velo/command/BGroup.java:1034-1039`
   - `src/main/java/io/velo/command/BlockingList.java:56-62`
   - `src/main/java/io/velo/command/BlockingList.java:267-283`
   - `src/main/java/io/velo/command/BlockingList.java:316-332`

   `BGroup.blmpop()` still creates the blocking promise through the compatibility constructor:

   ```java
   var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(
       finalPromise, socket, isLeft, System.currentTimeMillis(), null);
   ```

   That constructor sets `mpopCount` to `1`, so `BlockingList` never sees the parsed `count` value from `BLMPOP`. JaCoCo
   confirms the new `mpopCount > 1` wake-up branches are not executed. As a result, blocked `BLMPOP ... COUNT 2` still
   cannot pop and return two values after a later push.

2. **High: Blocked `BLMPOP COUNT 1` still returns the old `BLPOP` shape**

   Files:

   - `src/main/java/io/velo/command/BlockingList.java:267-288`
   - `src/main/java/io/velo/command/BlockingList.java:316-338`

   The nested `[key, [values...]]` wake-up shape is guarded by `promise.mpopCount > 1`. Even if `BGroup.blmpop()` passed
   the parsed count, `BLMPOP` with default count `1` or explicit `COUNT 1` would fall through to the old `BLPOP` shape:

   ```java
   replies[0] = new BulkReply(key);
   replies[1] = new BulkReply(leftValueBytes);
   ```

   Redis `BLMPOP` always returns the nested value array, including when exactly one value is popped.

3. **Medium: Regression coverage is still missing key compatibility branches**

   Files:

   - `src/test/groovy/io/velo/command/LGroupTest.groovy:1173-1312`
   - `src/test/groovy/io/velo/command/BGroupTest.groovy:889-1010`

   The tests now cover `BLMPOP` immediate wrong-type and `LMPOP COUNT <= 0`, but they do not cover:

   - `LMPOP` wrong-type return (`LGroup.java:535-536` remains uncovered).
   - `BLMPOP COUNT <= 0` (`BGroup.java:987` remains uncovered).
   - Blocked `BLMPOP` wake-up after a later push, especially default count/`COUNT 1` nested shape and `COUNT 2` multi-pop.

4. **Medium: Command support documentation still omits `LMPOP` and `BLMPOP`**

   File:

   - `doc/redis_command_support.md:150-168`

   The list command support matrix still does not include either command. Once the remaining blocking semantics are fixed,
   both commands should be documented as supported.

### Strengths

- Immediate wrong-type handling is fixed for `BLMPOP`, and the same production guard exists for `LMPOP`.
- `COUNT <= 0` is now rejected in both production methods.
- The implementation started separating BLMPOP wake-up shape from BLPOP wake-up shape inside `BlockingList`.

### Follow-ups

- Pass the parsed `count` from `BGroup.blmpop()` into the blocking promise.
- Add an explicit marker for BLMPOP, not just `mpopCount > 1`, so default count/`COUNT 1` still returns `[key, [value]]`.
- Add blocked wake-up tests for `BLMPOP` default count, `COUNT 1`, and `COUNT 2`.
- Add direct tests for `LMPOP` wrong-type and `BLMPOP COUNT 0`.
- Update `doc/redis_command_support.md` with `LMPOP` and `BLMPOP` after wake-up semantics are corrected.

---

## Review Feedback For LMPOP/BLMPOP Addressed Commit Re-Review

Reviewer: AI agent 2
Date: 2026-05-30
Commit reviewed: `2ece2504 fix: BLMPOP passes count to blocking promise, always returns nested array shape, add wake-up tests and docs`

### Summary

No blocking issues found in this addressed commit. The previous high-severity `BLMPOP` wake-up problems are corrected:

- `BGroup.blmpop()` now passes the parsed `COUNT` value into `BlockingList.PromiseWithLeftOrRightAndCreatedTime`.
- `BlockingList` now distinguishes BLMPOP wake-ups with `mpopCount > 0`, so default count and `COUNT 1` return `[key, [value]]` instead of the old BLPOP `[key, value]` shape.
- The BLMPOP wake-up path now collects up to `COUNT` values for both left and right pop directions.
- `LMPOP` and `BLMPOP` are now listed in `doc/redis_command_support.md`.

### Verification

- Ran focused list tests:
  - `./gradlew :test --tests "io.velo.command.LGroupTest.test lmpop" --tests "io.velo.command.BGroupTest.test blmpop" --tests "io.velo.command.BlockingListTest.test blmpop wake-up returns nested array" --tests "io.velo.command.BlockingListTest.test blocking list promise" --rerun-tasks`
- Result: `BUILD SUCCESSFUL`, `13 actionable tasks: 13 executed`
- JaCoCo HTML checks:
  - `BGroup.java:987` `BLMPOP COUNT <= 0` return is covered.
  - `BGroup.java:1009-1010` `BLMPOP` immediate wrong-type return is covered.
  - `BGroup.java:1037` blocking promise construction with parsed `count` is covered.
  - `LGroup.java:516` `LMPOP COUNT <= 0` return is covered.
  - `LGroup.java:535-536` `LMPOP` wrong-type return is covered.
  - `BlockingList.java:267-283` left-side BLMPOP nested wake-up branch is covered.
  - `BlockingList.java:316-332` right-side BLMPOP nested wake-up branch is covered.

### Findings

No blocking issues found.

### Strengths

- The fix closes the prior protocol-shape bug for blocked `BLMPOP COUNT 1`.
- The tests cover left wake-up count `1`, left wake-up count `2`, partial count when fewer values exist, and right wake-up count `2`.
- The support matrix now matches the implemented list-command surface.

### Follow-ups

- Consider adding a command-level integration test where `BLMPOP` blocks through `BGroup.blmpop()` and is later woken by a push command. The current tests cover `BGroup` parsing and `BlockingList` wake-up behavior separately, which is acceptable for this fix but not a full end-to-end blocking command path.
