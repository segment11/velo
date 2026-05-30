# Bug 42: Set Module Review Round 1

Author: AI agent 1
Date: 2026-05-30
Branch: main (workspace already dirty; no branch switch performed)

## Scope

Reviewed Redis set command compatibility against Redis latest set command docs:

- Source: https://redis.io/docs/latest/commands//?group=set
- Supporting command docs:
  - https://redis.io/docs/latest/commands/sintercard/
  - https://redis.io/docs/latest/commands/smismember/
  - https://redis.io/docs/latest/commands/srandmember/
  - https://redis.io/docs/latest/commands/spop/
  - https://redis.io/docs/latest/commands/srem/
- Redis set commands observed in Redis latest docs: SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERCARD, SINTERSTORE, SISMEMBER, SMEMBERS, SMISMEMBER, SMOVE, SPOP, SRANDMEMBER, SREM, SSCAN, SUNION, SUNIONSTORE.
- Main Velo implementation reviewed: `src/main/java/io/velo/command/SGroup.java`
- Main tests reviewed: `src/test/groovy/io/velo/command/SGroupTest.groovy`
- Local Redis reference checks: `redis-server v=7.2.11` on port 63879 for edge cases where the docs are concise.

## Finding 1: SINTERCARD rejects valid single-key calls and mishandles LIMIT

Status: **Confirmed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/SGroup.java:1102-1128`
- `src/main/java/io/velo/command/SGroup.java:1131-1148`
- `src/main/java/io/velo/command/SGroup.java:1160-1181`
- `src/main/java/io/velo/command/SGroup.java:1195-1221`

Code excerpt:

```java
if (numkeys < 2) {
    return ErrorReply.INVALID_INTEGER;
}

ArrayList<SlotWithKeyHash> list = new ArrayList<>(numkeys);
// begin from 2
for (int i = 2, j = 0; i < numkeys + 2; i++, j++) {
    var keyBytes = data[i];
```

```java
var limitFlagBytes = data[numkeys + 2];
if (!"limit".equals(new String(limitFlagBytes))) {
    return ErrorReply.SYNTAX;
}

var limitBytes = data[numkeys + 3];
try {
    limit = Integer.parseInt(new String(limitBytes));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}
```

```java
if (limit != 0 && set.size() >= limit) {
    break;
}
...
int min = limit != 0 ? Math.min(set.size(), limit) : set.size();
return min == 0 ? IntegerReply.REPLY_0 : new IntegerReply(min);
```

Root cause and impact:

Redis documents `SINTERCARD numkeys key [key ...] [LIMIT limit]`, with the `key` argument marked as one or more keys. Redis accepts `SINTERCARD 1 a` and returns that set's cardinality. Velo rejects `numkeys < 2`, so valid single-key usage fails.

Velo also indexes `data[i]` up to `numkeys + 2` without first proving that enough key arguments exist. A malformed request like `SINTERCARD 2 a` throws `ArrayIndexOutOfBoundsException` instead of returning a stable Redis protocol error. Redis returns `ERR Number of keys can't be greater than number of args`.

The `LIMIT` token is compared case-sensitively against lowercase `limit`, even though Redis command keywords are case-insensitive and the Redis syntax shows uppercase `LIMIT`. `SINTERCARD 2 a b LIMIT 1` is therefore rejected by Velo.

Finally, Velo accepts negative limit values and can return negative cardinalities. Redis rejects `LIMIT -1` with `ERR LIMIT can't be negative`. Velo also breaks out when an intermediate retained set has at least `limit` members; with three or more keys this can return `limit` before later sets reduce the real intersection below the limit. For example, with `a={1,2,3}`, `b={1,2,3}`, `c={1}`, `SINTERCARD 3 a b c LIMIT 2` should return `1`, but Velo can return `2`.

## Finding 2: Cross-worker SDIFFSTORE always removes the first source set from itself

Status: **Confirmed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/SGroup.java:991-1046`
- `src/main/java/io/velo/command/SGroup.java:1049-1097`
- `src/main/java/io/velo/command/SGroup.java:869-895`
- `src/test/groovy/io/velo/command/SGroupTest.groovy:1240-1310`

Code excerpt:

```java
ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size());
for (int i = 0; i < list.size(); i++) {
    var other = list.get(i);
    var oneSlot = localPersist.oneSlot(other.slot());
    var p = oneSlot.asyncCall(() -> getRedisSet(other));
    promises.add(p);
}
```

```java
var rhk = promises.getFirst().getResult();
...
var set = rhk.getSet();

ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
for (var promise : promises) {
    otherRhkList.add(promise.getResult());
}
operateSet(set, otherRhkList, isInter, isUnion);
```

```java
} else {
    // diff
    set.removeAll(otherSet);
}
```

Root cause and impact:

The synchronous `sdiffstore` path fetches the first source set separately and builds `otherRhkList` from source key index 1 onward. The cross-worker async path fetches every source key into `promises`, then adds every promise result, including the first source set, to `otherRhkList`. For `SDIFFSTORE`, `operateSet` receives the first set as both the mutable base and an "other" set, so `set.removeAll(firstSet)` empties the result before any real difference is computed.

This only affects the cross-worker path (`isCrossRequestWorker == true`), so existing same-worker tests pass. The current async test section covers `SINTERSTORE` and `SUNIONSTORE`, but it does not assert a successful non-empty `SDIFFSTORE` result in the async path.

## Finding 3: SMISMEMBER returns an empty array for missing or empty sets

Status: **Confirmed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/SGroup.java:1290-1323`

Code excerpt:

```java
var rhk = getRedisSet(slotWithKeyHash);
if (rhk == null) {
    return MultiBulkReply.EMPTY;
}
if (rhk.size() == 0) {
    return MultiBulkReply.EMPTY;
}

var replies = new Reply[memberBytesArr.length];
for (int i = 0; i < memberBytesArr.length; i++) {
    var isMember = rhk.contains(new String(memberBytesArr[i]));
    replies[i] = isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
}
return new MultiBulkReply(replies);
```

Root cause and impact:

Redis documents that `SMISMEMBER` returns one membership result for every requested member, and returns `0` for each member when the key does not exist. Velo returns `MultiBulkReply.EMPTY` when the set key is missing or empty, losing the response arity. For `SMISMEMBER missing x y`, Redis returns `[0, 0]`; Velo returns `[]`. Clients rely on positional correspondence between requested members and returned integers, so this is a protocol compatibility break.

## Finding 4: SPOP accepts negative count values

Status: **Confirmed**

Severity: Medium

Cited files:

- `src/main/java/io/velo/command/SGroup.java:1698-1774`
- `src/main/java/io/velo/command/HGroup.java:1819-1837`

Code excerpt:

```java
if (hasCount) {
    var countBytes = data[2];
    try {
        count = Integer.parseInt(new String(countBytes));
    } catch (NumberFormatException e) {
        return ErrorReply.NOT_INTEGER;
    }
}
...
int absCount = Math.abs(count);

ArrayList<Integer> indexes = HGroup.getRandIndex(count, size, absCount);
```

```java
boolean canUseSameField = count < 0;
var rand = new Random();
for (int i = 0; i < absCount; i++) {
    int index;
    do {
        index = rand.nextInt(size);
    } while (!canUseSameField && indexes.contains(index));
    indexes.add(index);
}
```

Root cause and impact:

Redis rejects `SPOP key -1` with `ERR value is out of range, must be positive`. Velo does not validate positivity for `SPOP` count. Because `SPOP` reuses the `SRANDMEMBER` random-index helper, negative counts enable duplicate index selection while also removing selected members. This can return duplicate bulk entries and mutate fewer distinct members than the reply suggests, which is neither Redis-compatible nor internally consistent for a destructive pop command.

## Finding 5: SSCAN is listed by Redis as a set command but falls through to nil

Status: **Confirmed**

Severity: Medium

Cited files:

- `src/main/java/io/velo/command/SGroup.java:53-90`
- `src/main/java/io/velo/command/SGroup.java:163-244`
- `doc/redis_command_support.md:211-228`

Code excerpt:

```java
if ("set".equals(cmd) || "setbit".equals(cmd) || "setex".equals(cmd) || "setrange".equals(cmd) ||
        "setnx".equals(cmd) || "strlen".equals(cmd) || "substr".equals(cmd) ||
        "sadd".equals(cmd) || "scard".equals(cmd) ||
        "sismember".equals(cmd) || "smembers".equals(cmd) || "smismember".equals(cmd) ||
        "sort".equals(cmd) || "sort_ro".equals(cmd) ||
        "spop".equals(cmd) || "srandmember".equals(cmd) || "srem".equals(cmd)) {
```

```java
if ("sunionstore".equals(cmd)) {
    return sdiffstore(false, true);
}

if ("slaveof".equals(cmd)) {
    return slaveof();
}

return NilReply.INSTANCE;
```

Root cause and impact:

Redis latest set docs include `SSCAN` in the set command family. Velo's support matrix lists 16 set commands and omits `SSCAN`. `SGroup.parseSlots` does not parse an `sscan` key, and `SGroup.handle()` has no `sscan` branch, so `SSCAN` falls through to `NilReply.INSTANCE` rather than returning either a cursor/array scan response or a clear unsupported/unknown-command error. Returning nil for a cursor command is especially misleading because nil is not one of `SSCAN`'s normal reply shapes.

## Suggested Fix Order

1. Fix `SINTERCARD` validation and limit handling: allow `numkeys == 1`, validate enough key args before indexing, compare `LIMIT` case-insensitively, reject negative limits, and compute limited cardinality only after checking all sets for each counted candidate.
2. Fix cross-worker `SDIFFSTORE` by excluding the first source result from `otherRhkList`, matching the synchronous path. Add an async test where the result is non-empty.
3. Fix `SMISMEMBER` missing/empty key behavior to return one `0` integer per requested member.
4. Add a `SPOP` count positivity guard while keeping `SRANDMEMBER` negative-count behavior intact.
5. Decide whether to implement `SSCAN` or return a protocol-appropriate unsupported/unknown-command error instead of nil.

---

## Review Notes — AI Agent 2

Reviewer: AI agent 2
Date: 2026-05-30

### Finding 1: SINTERCARD rejects valid single-key calls and mishandles LIMIT

Status: **Confirmed — all sub-issues verified**

I verified each sub-claim against the source code at `SGroup.java:1102-1224`:

1. **`numkeys < 2` rejection**: Confirmed. Line 1115 rejects `numkeys < 2` with `ErrorReply.INVALID_INTEGER`. Redis 7.2 accepts `SINTERCARD 1 a` and returns the set cardinality. This is a valid Redis protocol incompatibility. **Severity: High — confirmed.**

2. **ArrayIndexOutOfBoundsException on insufficient args**: Confirmed. At line 1121, the loop iterates `i` from 2 to `numkeys + 2` accessing `data[i]` without first checking `data.length >= numkeys + 2`. For `SINTERCARD 2 a`, `data.length` is 3 but the loop reads `data[3]` which is out of bounds. Note: `parseSlots` at line 75-80 uses `KeyIndexBegin2` which iterates from index 2 to `data.length - 1` and is safe, but `slotWithKeyHashListParsed` will have fewer entries than `numkeys` when args are insufficient. So the `data[i]` access at line 1122 will throw `ArrayIndexOutOfBoundsException` first, before even reaching the `slotWithKeyHashListParsed.get(j)` at line 1127. A guard like `if (numkeys + 2 > data.length)` (accounting for possible LIMIT tokens) is needed. Redis returns `ERR Number of keys can't be greater than number of args`. **Severity: High — confirmed.**

3. **Case-sensitive LIMIT comparison**: Confirmed. Line 1139 uses `"limit".equals(new String(limitFlagBytes))`. Redis command keywords are case-insensitive per the RESP protocol. `SINTERCARD 2 a b LIMIT 1` would be rejected. Should use `equalsIgnoreCase`. **Severity: Medium — confirmed (downgrading from the overall High since it's a sub-issue).**

4. **Negative limit not rejected**: Confirmed. Lines 1143-1148 parse the limit integer but never check `limit < 0`. Redis returns `ERR LIMIT can't be negative` for negative limits. **Severity: Medium — confirmed.**

5. **Premature limit break with 3+ keys**: Confirmed. In the synchronous path (line 1171), after `retainAll` with an intermediate set, the code breaks if `set.size() >= limit`. But a later set could further reduce the intersection. For `a={1,2,3}, b={1,2,3}, c={1}` with `LIMIT 2`, after processing `b` the intersection is `{1,2,3}` (size 3 >= limit 2), so Velo breaks and returns `min(3,2)=2`. The correct answer is 1 (intersection with `c` is `{1}`). The same issue exists in the async path at line 1211. The `LIMIT` optimization must only short-circuit after all sets have been processed or the intersection is already at or below `limit`. **Severity: High — confirmed.**

**Overall Finding 1 verdict: Confirmed. Multiple sub-bugs, all verified against source.**

### Finding 2: Cross-worker SDIFFSTORE always removes the first source set from itself

Status: **Confirmed**

Verified by comparing the synchronous path (lines 1037-1042) with the async path (lines 1089-1093):

- **Synchronous path** (lines 1037-1042): Correctly iterates `i = 1` to `list.size() - 1`, excluding the first source set from `otherRhkList`. This is correct for SDIFF: `set = first_set - all_other_sets`.
- **Async path** (lines 1089-1093): Iterates over ALL promises (starting from index 0), adding every result including `promises.getFirst()` to `otherRhkList`. Since `set` is already the first set's data, `operateSet` with `isInter=false, isUnion=false` calls `set.removeAll(firstSet)`, which empties the result entirely.

For SINTERSTORE and SUNIONSTORE, including the first set in `otherRhkList` is harmless (intersection of a set with itself is the same set; union of a set with itself is the same set). But for SDIFFSTORE, it's catastrophic — `set.removeAll(set)` always produces an empty set.

I also verified the test at `SGroupTest.groovy:1240-1310`. The async test cases only assert:
- `sdiffstore(true, false)` (SINTERSTORE) with result 0
- `sdiffstore(false, true)` (SUNIONSTORE) with result 3
- `sdiffstore(false, false)` (SDIFFSTORE) with result 0 — but only after sets have been emptied or removed

There is no async SDIFFSTORE test with a non-empty expected result, confirming the bug would not be caught by existing tests.

**Severity: High — confirmed.**

### Finding 3: SMISMEMBER returns an empty array for missing or empty sets

Status: **Confirmed**

Verified at `SGroup.java:1310-1316`:

```java
if (rhk == null) {
    return MultiBulkReply.EMPTY;
}
if (rhk.size() == 0) {
    return MultiBulkReply.EMPTY;
}
```

Redis behavior (verified from docs): `SMISMEMBER key member [member ...]` returns an array of integers, one per member. When the key does not exist, every member maps to `0`. Velo returns `MultiBulkReply.EMPTY` (an empty array `[]`) instead of an array of `0`s with the correct arity.

The test at `SGroupTest.groovy:1593-1595` actually asserts the buggy behavior:
```groovy
reply = sGroup.execute('smismember a 1 2')
then:
reply == MultiBulkReply.EMPTY
```

And at line 1616-1618, the empty-set case also asserts `MultiBulkReply.EMPTY`. These tests will need updating when the fix is applied.

**Severity: High — confirmed.**

### Finding 4: SPOP accepts negative count values

Status: **Confirmed**

Verified at `SGroup.java:1698-1736`. The `srandmember` method is shared by both `SPOP` (called with `doPop=true` at line 217) and `SRANDMEMBER` (called with `doPop=false` at line 221). The count parsing at lines 1710-1717 does not validate that `count > 0` for the pop case.

The `HGroup.getRandIndex` helper at `HGroup.java:1819-1837` supports negative counts by setting `canUseSameField = count < 0`, allowing duplicate random indices. This is correct for `SRANDMEMBER` (Redis supports negative count to allow duplicates), but incorrect for `SPOP` where Redis requires a positive count.

With `SPOP key -2`, Velo would select 2 random members (possibly the same one twice), remove them, and return duplicates. Redis rejects this with `ERR value is out of range, must be positive`.

Note: there is also a subtle issue — when `count` is negative and `doPop` is true, `absCount = Math.abs(count)` but `count > size` at line 1730 caps `count` (not `absCount`) to `size`. Since `count` is negative, `count > size` is false, so no capping occurs. Then `getRandIndex(count, size, absCount)` gets the negative `count` and allows duplicate indices. When popping, the `isAlreadyRemoved` flag at line 1746 only prevents double-removal of the same iterator position, but the same logical member could be at different indices if the set contains only one element and duplicate indices are generated. The actual impact: duplicate members in the response array, and potentially removing fewer distinct members than reported.

**Severity: Medium — confirmed.**

### Finding 5: SSCAN is listed by Redis as a set command but falls through to nil

Status: **Confirmed**

Verified:
- `SGroup.parseSlots()` (lines 50-92): No branch handles `"sscan"`. The method returns an empty list for unrecognized commands.
- `SGroup.handle()` (lines 101-244): No branch handles `"sscan"`. Falls through to `return NilReply.INSTANCE` at line 244.
- `sscan` is listed in ACL categories (`Category.java:470,535,681`) but has no implementation.
- `SGroupTest.groovy` has no SSCAN test.

SSCAN's normal reply is a two-element array `[cursor, [members...]]`. Returning `NilReply.INSTANCE` is misleading. Other scan commands (HSCAN, ZSCAN) may exist elsewhere; checking for those patterns could inform the implementation approach. At minimum, returning an error reply would be more appropriate than nil.

**Severity: Medium — confirmed.** This is a missing feature rather than a behavioral bug, but the nil fallback is worse than an explicit error.

---

## Summary

| Finding | Status | Severity | Notes |
|---------|--------|----------|-------|
| 1: SINTERCARD multiple issues | **Confirmed** | High | 5 sub-bugs all verified: single-key rejection, AIOOBE on insufficient args, case-sensitive LIMIT, negative limit accepted, premature limit break |
| 2: Cross-worker SDIFFSTORE bug | **Confirmed** | High | Async path includes first set in otherRhkList; diff of set with itself always empty. Existing async tests do not cover non-empty SDIFFSTORE. |
| 3: SMISMEMBER empty array | **Confirmed** | High | Returns `[]` instead of `[0, 0, ...]` for missing/empty keys. Test at line 1595 asserts the buggy behavior. |
| 4: SPOP negative count | **Confirmed** | Medium | No positivity guard for SPOP count; negative count enables duplicates in a destructive command. |
| 5: SSCAN missing | **Confirmed** | Medium | No parse/handle branch; falls through to NilReply. ACL references exist. |

All 5 findings are confirmed. Suggested fix order from agent 1 is reasonable.

---

## Review Feedback — Bug 1 Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `f9b36ecdde1bd1d54c101d0cb515c5088cd4f709` (`fix: SINTERCARD multiple validation and logic bugs`)

### Summary of the fix

The commit updates `SGroup.sintercard()` to address the five handler-level sub-issues from Finding 1:

- Allows `numkeys == 1`.
- Guards insufficient key arguments before indexing `data[i]`.
- Parses `LIMIT` case-insensitively.
- Rejects negative `LIMIT`.
- Removes the premature `LIMIT` short-circuit before all sets have been intersected.

The commit also expands `SGroupTest.test sintercard` with regressions for single-key calls, insufficient key count, uppercase `LIMIT`, negative limit, and the three-key premature-limit case.

### Strengths

- The production change is scoped to `SINTERCARD` and preserves the existing synchronous and async execution structure.
- The new tests directly cover each originally reported handler-level failure mode.
- Focused verification passed:
  - Command: `./gradlew :test --tests "io.velo.command.SGroupTest.test sintercard"`
  - Result: `BUILD SUCCESSFUL`
  - JaCoCo: `build/reports/jacocoHtml/io.velo.command/SGroup.java.html` shows `sintercard()` at 98% instruction coverage and 95% branch coverage; the new argument-count, `LIMIT`, negative-limit, single-key, sync, and async branches are executed.

### Concern

`SINTERCARD` slot parsing still treats option tokens as keys when `LIMIT` is present. `SGroup.parseSlots()` adds every argument from index 2 onward via `KeyIndexBegin2` (`SGroup.java:75-80`, `BaseCommand.java:72-79`), so a request like `SINTERCARD 2 a b LIMIT 1` records slots for `a`, `b`, `LIMIT`, and `1`.

That does not break the direct unit test path because `sintercard()` consumes only the first `numkeys` parsed slots. It can still affect real request handling before `handle()` runs: `MultiWorkerServer.handleRequest()` uses the full parsed slot list for cluster validation and cross-worker detection (`MultiWorkerServer.java:546-566`, `MultiWorkerServer.java:341-390`). In cluster mode this can incorrectly reject or redirect a valid `SINTERCARD ... LIMIT ...` based on the hash slots for `LIMIT` or the numeric limit value; outside cluster mode it can also mark a request cross-worker unnecessarily.

Recommended follow-up: parse only the declared `numkeys` keys for `SINTERCARD`, and add a parse-slot regression such as `sintercard 2 a b limit 1` returning exactly two parsed slots. If cluster validation tests exist for multi-key commands, add one that proves `LIMIT` and its value are ignored as key material.

### Verdict

The committed fix resolves Finding 1 at the command-handler behavior level and is covered by focused tests. I would not treat the broader `SINTERCARD` request path as fully closed until the slot parsing follow-up above is fixed or explicitly tracked as a separate bug.

---

## Review Feedback — Bug 1 Addressed Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `fc1c035f65839c85c736175b5661e03c2f143f3b` (`fix: SINTERCARD multiple validation and logic bugs`)

### Summary of the follow-up

The addressed commit fixes the remaining `SINTERCARD` slot-parsing concern from the prior review. `SGroup.parseSlots()` now parses `numkeys` from `data[1]` and slots only arguments `data[2]` through `data[numkeys + 1]`, bounded by the actual request length. This prevents `LIMIT` and the limit value from being treated as key material.

The test update adds a parse-slot regression for `sintercard 2 a b limit 1`, asserting exactly two parsed slots.

### Review result

No issues found in the addressed change.

The parser fix matches the command-handler contract: malformed `numkeys`, `numkeys < 1`, or too few arguments return fewer/no parsed slots and are still rejected later by `sintercard()` with the existing validation. Valid requests with `LIMIT` no longer contaminate cluster-slot validation or cross-worker detection with option tokens.

### Verification

- Command: `./gradlew :test --tests "io.velo.command.SGroupTest.test parse slot" --tests "io.velo.command.SGroupTest.test sintercard"`
- Result: `BUILD SUCCESSFUL`
- JaCoCo: `build/reports/jacocoHtml/io.velo.command/SGroup.java.html` shows the new `SINTERCARD` parse-slot lines executed, including `numkeys` parsing and the bounded loop. `build/reports/jacocoHtml/io.velo.command/SGroup.html` reports `parseSlots(String, byte[][], int)` at 100% instruction and branch coverage in the focused run, and `sintercard()` remains at 98% instruction and 95% branch coverage.

### Verdict

Finding 1 is now resolved across both slot parsing and command-handler behavior. No additional follow-up is needed for Bug 1.

---

## Review Feedback — Bug 2 Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `05125477b0d56209ca479d9373afeacf6b6b0019` (`fix: cross-worker SDIFFSTORE excludes first set from other list`)

### Summary of the fix

The commit targets Finding 2 (cross-worker `SDIFFSTORE` removing the first source set from itself). In the async (`isCrossRequestWorker == true`) branch of `SGroup.sdiffstore(isInter, isUnion)`, the loop that builds `otherRhkList` was changed to start at index 1:

```java
ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
-for (var promise : promises) {
-    otherRhkList.add(promise.getResult());
+for (int i = 1; i < promises.size(); i++) {
+    otherRhkList.add(promises.get(i).getResult());
}
operateSet(set, otherRhkList, isInter, isUnion);
```

Here `set` is the first source set (`rhk = promises.getFirst().getResult()`), and in this method `promises` is built from the full source `list` (`for i = 0 .. list.size()-1`), so index 0 is the first source. Excluding promise index 0 from `otherRhkList` stops the diff path from computing `set.removeAll(firstSet)`. This now matches the synchronous branch (`SGroup.java:1049-1053`), which already iterated `i = 1 .. list.size()-1`.

The commit also adds an async cross-worker `SDIFFSTORE` regression to `SGroupTest.test sdiffstore` that asserts a non-empty positive result (the exact scenario the bug broke — it would have returned `0`).

### Strengths

- The production change is minimal and correct: the diff base (`set`) and the "other" sets are now disjoint, so the async SDIFFSTORE path no longer self-cancels. SINTERSTORE/SUNIONSTORE are unchanged in behavior (including the first set was harmless for intersect/union, and is now simply omitted).
- It aligns the async branch with the synchronous branch's index-1 convention, removing the divergence that hid the bug.
- The new test is a genuine regression: with `a = {1, 2}` and `b = {3}`, async `SDIFFSTORE dst a b` asserts `IntegerReply == 2` via a hard `getResult()` assertion. Under the old code this returned `0`, so the test fails before the fix and passes after.

### Why this is scoped only to `sdiffstore`

I checked the other two async blocks in this file to confirm the same bug does not exist there:
- `sdiff(...)` async (`SGroup.java:961-983`): `promises` is built from `i = 1 .. list.size()-1` (it excludes the first key, which is fetched separately as `set`). Iterating all promises is therefore correct there — no index-0 contamination.
- `sintercard()` async (`SGroup.java:1207+`): cardinality-only path, no diff base mutation.

So the index-0 inclusion bug was unique to `sdiffstore`'s async block, and the fix addresses it completely.

### On the previously-suspected null-guard concern (withdrawn)

An earlier draft of this review flagged the fixed loop for not null-checking `promises.get(i).getResult()`. That concern is **incorrect and withdrawn**: `operateSet` (`SGroup.java:880-906`) already handles `null` entries explicitly — for a `null` other-set it clears on intersect and is a no-op for diff/union. The synchronous branch likewise adds possibly-`null` results (`SGroup.java:1052`). Passing `null` through is intended and safe, so no extra guard is needed.

### Verification

- Test: `./gradlew :test --tests "io.velo.command.SGroupTest.test sdiffstore" --rerun-tasks` → `BUILD SUCCESSFUL`. The async cross-worker `SDIFFSTORE`/`SINTERSTORE`/`SUNIONSTORE` cases all pass, including the new non-empty SDIFFSTORE (`== 2`). (Note: `test sdiff` is a *different* method and does not exercise the store path — the `jacoco_cover.py` tool flagged lines 1100-1108 as not-covered when only `test sdiff` was run, which is what surfaced the right test to use.)
- JaCoCo (via the documented `scripts/jacoco_cover.py`):
  ```
  python3 scripts/jacoco_cover.py io.velo.command.SGroup 1100 1108 --src
  ```
  Result: lines 1100-1108 are `100.0% (6/6)` covered, with the changed loop line `1101` and body `1102` both `fc`. The fixed async path is exercised by the new test.

### Verdict

Bug 2 (Finding 2) is fully resolved. The fix is correct and minimal, the async path now matches the synchronous semantics, the regression test fails-before/passes-after with a correct expected cardinality, and JaCoCo confirms the changed lines execute. No follow-up required.

---

## Review Feedback — Bug 3 Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `f3db7d3b` (`fix: SMISMEMBER returns per-member 0s for missing/empty keys`)

> Process note: the in-session tool backend was unreliable and at one point fabricated a non-existent commit hash (`e7d8ed8c`, rejected by `git rev-parse`) and bogus diff hunks. This review was done against canonical content at HEAD = `f3db7d3b`: the committed `git show` diff, the working-tree method `SGroup.smismember()` (`SGroup.java:1309-1336`), and the test `SGroupTest.test smismember` (`SGroupTest.groovy:1667`), plus a fresh test run and `jacoco_cover.py`.

### Summary of the fix

Finding 3 was that `SMISMEMBER` returned `MultiBulkReply.EMPTY` (an empty array) when the key was missing or the set was empty, losing reply arity. The committed diff removes the two early returns and makes the per-member loop null-tolerant:

```java
 var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
 var rhk = getRedisSet(slotWithKeyHash);
-if (rhk == null) {
-    return MultiBulkReply.EMPTY;
-}
-if (rhk.size() == 0) {
-    return MultiBulkReply.EMPTY;
-}

 var replies = new Reply[memberBytesArr.length];
 for (int i = 0; i < memberBytesArr.length; i++) {
-    var isMember = rhk.contains(new String(memberBytesArr[i]));
+    var isMember = rhk != null && rhk.contains(new String(memberBytesArr[i]));
     replies[i] = isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
 }
 return new MultiBulkReply(replies);
```

The reply array is sized to the member count (`memberBytesArr.length`), so arity is preserved, and `rhk != null && ...` short-circuits a missing key to `0` for every member.

### Correctness vs Redis

- Missing key: `rhk == null` → each member maps to `0` (e.g. `SMISMEMBER missing x y` → `[0, 0]`). Correct.
- Empty set: `rhk` non-null but `contains` always false → all `0`. Correct.
- Populated set: per-member `1`/`0`. Correct.

This matches Redis semantics and the Finding 3 recommendation. The FORMAT (`data.length < 3`), key-too-long, and member-too-long guards are unchanged and still precede the lookup.

### Strengths

- Minimal, correct change scoped to `smismember()`; no behavior change for the populated-set path.
- The updated `test smismember` replaces the old `MultiBulkReply.EMPTY` assertions for the missing-key and empty-set cases with length-preserving per-member `0` assertions, so it fails before / passes after the fix.

### Verification

- Test: `./gradlew :test --tests "io.velo.command.SGroupTest.test smismember" --rerun-tasks` → `BUILD SUCCESSFUL`.
- JaCoCo (`scripts/jacoco_cover.py io.velo.command.SGroup 1309 1336`): **16/18 lines covered; 6/7 branch lines fully covered.** The fix's own lines are fully covered — the key-too-long guard (1315), the member-too-long guard (1320/1322), the `getRedisSet` lookup (1329), and the changed per-member loop with all 4 branches of `rhk != null && rhk.contains(...)` (1333) exercised (missing-key `rhk == null`, plus present/absent member), and the `MultiBulkReply` build (1336).
- The only gaps are pre-existing and outside the fix: line 1310 is partial and line 1311 (`return ErrorReply.FORMAT` for `data.length < 3`) is not covered, because `test smismember` has no fewer-than-3-args case. This is unrelated to Finding 3, but worth a one-line follow-up: add a `smismember a` (no members) case asserting `ErrorReply.FORMAT` to close the gap, consistent with the command-test checklist in `doc/how_to_write_high_coverage_test_cases_for_commands.md`.

### Verdict

Bug 3 (Finding 3) is fully resolved. The fix is correct, minimal, Redis-compatible, and its changed lines are exercised by the updated focused test. The one optional follow-up — covering the pre-existing `data.length < 3` FORMAT branch — is unrelated to the bug and can be tracked separately.

---

## Review Feedback — Bug 5 Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `1b8d1ed2` (`feat: implement SSCAN command with cursor, MATCH, and COUNT support`)

### Summary of the fix

Finding 5 was that `SSCAN` had no parse/handle branch and fell through to `NilReply.INSTANCE`. The commit:

- Adds `"sscan"` to the key-parsing branch in `parseSlots()` (`SGroup.java:58-59`), so the set key at `data[1]` is parsed for slot routing.
- Adds an `if ("sscan".equals(cmd)) return sscan();` dispatch branch in `handle()` (`SGroup.java:240-242`).
- Adds a new `sscan()` method (`SGroup.java:1839-1927`) modeled directly on `HGroup.hscan2()`: arg/cursor validation, optional `MATCH`/`COUNT` (case-insensitive), `getRedisSet()` lookup, offset-based skip over the backing `TreeSet`, and a `[nextCursor, [members...]]` reply.
- Adds `test sscan` to `SGroupTest` covering missing key, empty set, full scan, `COUNT`, `MATCH` hit, `MATCH` miss, non-integer cursor, and key-too-long.

The command no longer returns nil, and the empty/missing cases now correctly return `MultiBulkReply.SCAN_EMPTY`. This is the right shape and resolves the original "nil for a cursor command" complaint.

### Blocking concern — cursor does not advance across pages (pagination infinite loop)

`SGroup.java:1924-1925`:

```java
var isEnd = loopCount == set.size();
var nextCursor = String.valueOf(isEnd ? 0 : skipCount + matchMembers.size());
```

`skipCount` starts at `(int) cursorLong` but is **decremented to 0** inside the loop while skipping already-returned members (`SGroup.java:1904-1906`). So whenever the incoming cursor is fully consumed, `skipCount == 0` at this point and `nextCursor` collapses to just `matchMembers.size()` — it loses the incoming offset entirely. The cumulative position is dropped.

The correct expression is the *original* cursor plus what was returned this page, i.e. `(int) cursorLong + matchMembers.size()`.

Concrete repro (the backing store is a `TreeSet`, so iteration order is stable and sorted — `RedisHashKeys.getSet()` at `RedisHashKeys.java:99`):

- Set `{a,b,c,d,e}`, `SSCAN key 0 COUNT 2`:
  - Call 1: cursor `0` → returns `[a,b]`, nextCursor `0+2 = 2`. ✓
  - Call 2: cursor `2` → skips `a,b` (skipCount `2→0`), returns `[c,d]`, `isEnd` is false (loopCount 4 ≠ 5), nextCursor `= 0 + 2 = 2`. ✗ should be `4`.
  - Client sees a non-zero cursor `2`, re-issues `SSCAN key 2`, gets `[c,d]` again — **forever**. `e` is never returned and the scan never terminates.

This breaks the core contract of a cursor command for any set whose size exceeds `COUNT` (default 10), which is precisely when SSCAN matters. Single-page scans (set size ≤ COUNT) accidentally work because `isEnd` is true and the cursor is forced to `0`, which is why every test in `test sscan` passes.

Note: this bug is inherited verbatim from `HGroup.hscan()`/`hscan2()` (`HGroup.java:1947`, `1995`), so HSCAN/ZSCAN likely share it. That is a separate pre-existing defect, but the SSCAN commit newly introduces the same broken arithmetic and should not ship it. Recommend fixing here (`(int) cursorLong + matchMembers.size()`) and tracking the HSCAN/HSCAN2 twin as its own bug.

### Test gap that hid the bug

No test in `test sscan` ever issues a follow-up call with the returned non-zero cursor. JaCoCo confirms the skip branch is dead in tests:

```
python3 scripts/jacoco_cover.py io.velo.command.SGroup 1839 1928
...
1905  ✗ not-covered
1906  ✗ not-covered      # if (skipCount > 0) { skipCount--; continue; }
```

Because the skip path is never exercised, the cursor-continuation logic — the only place the bug lives — is untested. A regression must: build a set larger than `COUNT`, call `SSCAN` once, then **loop on the returned cursor until it returns `0`**, and assert the union of all pages equals the full set with no duplicates and no missing members. That test fails against the current code (infinite loop / missing `e`) and passes after the `(int) cursorLong + matchMembers.size()` fix.

### Secondary observations (non-blocking)

- **`COUNT 0` accepted**: only `count < 0` is rejected (`SGroup.java:1877-1879`). Redis rejects `COUNT 0` with a syntax error; here `COUNT 0` returns one member per call (the `matchMembers.size() >= count` check fires after the first add). Minor, and consistent with HSCAN, so optional.
- **Coverage of error branches**: the `data.length < 3` FORMAT (1841), MATCH-missing-arg SYNTAX (1864), COUNT-missing-arg SYNTAX (1870), COUNT NOT_INTEGER (1875), unknown-option SYNTAX (1882), and negative-count INVALID_INTEGER (1878) branches are all not-covered by `test sscan`. Worth a few extra cases per the command-test checklist, but unrelated to the cursor defect.

### Verification

- Test: `./gradlew :test --tests "io.velo.command.SGroupTest.test sscan" --rerun-tasks` → `BUILD SUCCESSFUL` (but, per above, it does not cover the buggy continuation path).
- JaCoCo (`scripts/jacoco_cover.py io.velo.command.SGroup 1839 1928`): the happy-path lines and `isEnd`/`nextCursor` computation are marked covered for the cursor-0 case, but lines 1905-1906 (the skip branch) are not-covered, confirming the pagination path is untested.

### Verdict

Bug 5 is **partially resolved**: SSCAN is now wired up, validated, and returns a proper scan-shaped reply instead of nil, which closes the original "nil fallback" complaint. However, the fix introduces a **blocking cursor-advance bug** that makes multi-page SSCAN loop forever and never enumerate sets larger than `COUNT`. This should be fixed (use the original cursor value, not the decremented `skipCount`) and covered by a multi-page continuation test before Bug 5 is considered closed. The identical defect in `HGroup.hscan()`/`hscan2()` should be tracked as a separate finding.

---

## Review Feedback — Bug 5 Addressed Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `071ffdc9` (`feat: implement SSCAN command with cursor, MATCH, and COUNT support`) — amends the prior `1b8d1ed2`

### Summary of the follow-up

The blocking cursor-advance bug from the prior review is fixed. `SGroup.java:1925`:

```java
-var nextCursor = String.valueOf(isEnd ? 0 : skipCount + matchMembers.size());
+var nextCursor = String.valueOf(isEnd ? 0 : (int) cursorLong + matchMembers.size());
```

The next cursor now derives from the *original* incoming cursor (`cursorLong`) rather than the post-decrement `skipCount`, so the cumulative offset is preserved across pages and the cursor strictly advances.

Re-tracing the prior repro — set `{a,b,c,d,e}`, `SSCAN key 0 COUNT 2`:
- Call 1: cursor `0` → `[a,b]`, nextCursor `0+2=2`.
- Call 2: cursor `2` → skip `a,b`, return `[c,d]`, nextCursor `2+2=4`.
- Call 3: cursor `4` → skip `a,b,c,d`, return `[e]`, `loopCount 5 == size 5` → `isEnd`, cursor `0`. Terminates, all 5 enumerated, 3 pages.

### Test added

`test sscan` now has a true multi-page continuation case: it grows the set to 5 members, loops `SSCAN a <cursor> COUNT 2` until the cursor returns `0`, and asserts (a) the union of all pages equals the full set, (b) no duplicates (`allMembers.toSet().size() == allMembers.size()`), and (c) `pageCount >= 3`. It also has a hard `pageCount > 20` guard that trips on the infinite-loop regression. This test fails against the pre-fix code (loops past 20 pages) and passes after, so it is a genuine fail-before/pass-after regression for the exact defect.

### Verification

- Test: `./gradlew :test --tests "io.velo.command.SGroupTest.test sscan" --rerun-tasks` → `BUILD SUCCESSFUL`.
- JaCoCo (`scripts/jacoco_cover.py io.velo.command.SGroup 1893 1926`): the previously-dead skip branch is now live — line `1904` "All 2 branches covered", lines `1905-1906` (`skipCount--; continue;`) covered, and line `1925` (`nextCursor`) shows **both** `isEnd` branches covered. The continuation path is now exercised.

### Verdict

The blocking concern is fully resolved. SSCAN now paginates correctly over sets larger than `COUNT`, the fix is minimal and correct, and the new multi-page test covers the formerly-untested skip/continuation path with JaCoCo confirmation. **Bug 5 is resolved.**

The secondary, non-blocking items from the prior review remain open and unrelated to this bug:
- The identical cursor defect still exists in `HGroup.hscan()`/`hscan2()` (and likely ZSCAN) — should be tracked as a separate finding and fixed the same way.
- `COUNT 0` is still accepted where Redis returns a syntax error (consistent with HSCAN; optional).
- The `data.length < 3` FORMAT, MATCH/COUNT missing-arg SYNTAX, COUNT NOT_INTEGER, unknown-option SYNTAX, and negative-count INVALID_INTEGER branches in `sscan()` remain uncovered by `test sscan`; a few error-path cases would close the gap per the command-test checklist.

---

## Review Feedback — HSCAN Twin Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `51583515` (`fix: HSCAN cursor pagination infinite loop (same bug as SSCAN)`)

This is the follow-up for the "identical defect in `HGroup.hscan()`/`hscan2()`" item flagged above.

### Summary of the fix

Both hash member-scan paths had the same cursor-advance bug as SSCAN. The commit applies the identical correction to each:

```java
-int skipCount = (int) cursorLong;
+long skipCount = cursorLong;
...
-var nextCursor = String.valueOf(isEnd ? 0 : skipCount + matchFields.size());
+var nextCursor = String.valueOf(isEnd ? 0L : cursorLong + matchFields.size());
```

- `hscan()` (split-keys path, `hashSaveMemberTogether == false`): `HGroup.java:1915`, `1947`.
- `hscan2()` (together/`RedisHH` path): `HGroup.java:1959`, `1995`.

The production change is **correct and matches the now-accepted SSCAN fix**: the next cursor derives from the original `cursorLong` instead of the post-decrement `skipCount`, so the offset accumulates and pagination terminates. The `int → long` change is harmless (member offsets fit in `int`) and brings HSCAN in line with SSCAN's type usage — both are now consistently `long`, which removes the cross-command inconsistency.

### Blocking concern — no regression test, fixed branch not covered

Unlike the SSCAN fix, this commit touches **only `HGroup.java` — no test was added** (`git show --stat`: 1 file, 4 insertions, 4 deletions). The existing `test hscan` cases all start from cursor `0`, and none loop on the returned cursor. JaCoCo confirms the fixed code path is dead in tests:

```
python3 scripts/jacoco_cover.py io.velo.command.HGroup 1915 1947   # hscan()
1925  ~ partial       1 of 2 branches missed
1926  ✗ not-covered                                  # skipCount--;
1927  ✗ not-covered                                  # continue;
1947  ~ partial       1 of 2 branches missed         # the fixed nextCursor line

python3 scripts/jacoco_cover.py io.velo.command.HGroup 1959 1995   # hscan2()
1970  ~ partial       1 of 2 branches missed
1971  ✗ not-covered
1972  ✗ not-covered
1995  ~ partial       1 of 2 branches missed         # the fixed nextCursor line
```

The skip branch (`skipCount > 0`) is never taken, and the `isEnd == false` half of the fixed `nextCursor` lines (1947, 1995) — the exact branch that was buggy — is never executed. Per the project bug-fix workflow this fix is not complete: "A passing test is not enough; JaCoCo confirmation is required," and TDD requires a failing test first. The current `test hscan` would pass against the *old* buggy code too (page 1 with cursor 0 never triggers the bug), so there is no fail-before/pass-after evidence.

Required follow-up: add a multi-page continuation regression to `test hscan`, mirroring the SSCAN one, **for both storage modes** — `hashSaveMemberTogether == false` (drives `hscan()`/`liveFields`) and `hashSaveMemberTogether == true` (drives `hscan2()`/`RedisHH`). Each should build > `COUNT` fields, loop `HSCAN a <cursor> COUNT 2` until the cursor returns `0`, assert the union equals the full field set with no dupes, and guard against an iteration cap. That test fails on the pre-fix code (infinite loop) and passes after, and it will flip lines 1925-1927/1970-1972 and the non-end branch of 1947/1995 to covered.

### Verification

- Test: `./gradlew :test --tests "io.velo.command.HGroupTest.test hscan" --rerun-tasks` → `BUILD SUCCESSFUL`, but it does **not** exercise the changed branch (see JaCoCo above).

### Verdict

The production fix is **correct** and resolves the HSCAN/hscan2 pagination twin, consistent with the SSCAN fix. But the commit is **incomplete on verification**: no regression test, and JaCoCo shows the fixed branch and the skip-continuation path are not executed by any test. Add the two-mode multi-page regression before considering the HSCAN twin closed. ZSCAN should still be checked for the same pattern.

---

## Review Feedback — SSCAN Non-blocking Items Addressed

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `e583f8f9` (`fix: SSCAN reject COUNT 0, add error-branch tests`)

### Summary

Two of the three non-blocking items from the Bug 5 review are now addressed:

1. **`COUNT 0` rejection**: Changed `if (count < 0)` to `if (count <= 0)` and return `ErrorReply.SYNTAX` (matching Redis behavior which returns a syntax error for `COUNT 0`). Previously `COUNT 0` was silently accepted and returned one member per call.
2. **Error-branch test coverage**: Added 7 new test cases to `test sscan` covering all previously-uncovered error branches:
   - `sscan a` → `FORMAT` (too few args)
   - `sscan a 0 match` → `SYNTAX` (MATCH missing arg)
   - `sscan a 0 count` → `SYNTAX` (COUNT missing arg)
   - `sscan a 0 count notanumber` → `NOT_INTEGER`
   - `sscan a 0 count -1` → `SYNTAX` (now caught by `count <= 0`)
   - `sscan a 0 count 0` → `SYNTAX`
   - `sscan a 0 unknownoption` → `SYNTAX` (unknown option)

### JaCoCo verification

```
python3 scripts/jacoco_cover.py io.velo.command.SGroup 1839 1883 --src
Lines: 31 | Covered: 31 | Partial: 0 | Not-covered: 0
Branches: 9 lines with branches, 9 fully covered
```

All error branches in `sscan()` are now fully covered.

### Remaining open items

- HSCAN multi-page regression test not yet added (see HSCAN review section above).
- ZSCAN should be checked for the same cursor pagination pattern.
