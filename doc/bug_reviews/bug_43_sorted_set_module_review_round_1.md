# Bug 43: Sorted Set Module Review Round 1

Author: AI agent 1
Date: 2026-05-30
Branch: main (workspace already dirty; no branch switch performed)

## Scope

Reviewed Redis 7.2 sorted-set command compatibility against Velo's sorted-set implementation.

- Source: https://redis.io/docs/latest/commands/redis-7-2-commands/
- Redis sorted-set command surface used for comparison: `ZADD`, `ZCARD`, `ZCOUNT`, `ZDIFF`, `ZDIFFSTORE`, `ZINCRBY`, `ZINTER`, `ZINTERCARD`, `ZINTERSTORE`, `ZLEXCOUNT`, `ZMSCORE`, `ZPOPMAX`, `ZPOPMIN`, `ZRANDMEMBER`, `ZRANGE`, `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANGESTORE`, `ZRANK`, `ZREM`, `ZREMRANGEBYLEX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`, `ZREVRANK`, `ZSCORE`, plus Redis 7.2 additions `ZSCAN`, `ZMPOP`, `BZMPOP`, `BZPOPMAX`, and `BZPOPMIN`.
- Main Velo implementation reviewed: `src/main/java/io/velo/command/ZGroup.java`
- Data structure reviewed: `src/main/java/io/velo/type/RedisZSet.java`
- Main tests reviewed: `src/test/groovy/io/velo/command/ZGroupTest.groovy`

## Finding 1: `ZRANGE ... LIMIT 0 0` is treated as unbounded instead of empty

Status: **Fixed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/ZGroup.java:1456-1478`
- `src/main/java/io/velo/command/ZGroup.java:1605-1608`
- `src/main/java/io/velo/command/ZGroup.java:1619-1673`
- `src/test/groovy/io/velo/command/ZGroupTest.groovy:1669-1708`

Code excerpt:

```java
} else if ("limit".equals(tmp)) {
    if (i + 2 >= dd.length) {
        return ErrorReply.SYNTAX;
    }
    var offsetBytes = dd[i + 1];
    try {
        offset = Integer.parseInt(new String(offsetBytes));
    } catch (NumberFormatException e) {
        return ErrorReply.NOT_INTEGER;
    }
    if (offset < 0) {
        return ErrorReply.INVALID_INTEGER;
    }

    var countBytes = dd[i + 2];
    try {
        count = Integer.parseInt(new String(countBytes));
    } catch (NumberFormatException e) {
        return ErrorReply.NOT_INTEGER;
    }

    hasLimit = count != 0 || offset != 0;
    i += 2;
}
```

```java
int size = rz.size();
if (count <= 0) {
    count = size;
}
```

Root cause and impact:

Redis 7.2 uses `LIMIT offset count` as an explicit slice on the range result. A request such as `ZRANGE key 0 -1 LIMIT 0 0` should return no members. The follow-up commit narrows the behavior to Redis 7.2 semantics: `LIMIT` is now rejected for `BYINDEX`, `count == 0` returns empty, and negative counts behave as "no limit" for `BYSCORE` and `BYLEX`.

## Finding 2: Reverse lexicographic ranges are over-restricted

Status: **Fixed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/ZGroup.java:1493-1529`
- `src/main/java/io/velo/type/RedisZSet.java:168-188`

Code excerpt:

```java
if (byLex) {
    if (minBytes[0] == '-') {
        minLex = RedisZSet.MEMBER_MIN;
        minInclusive = false;
    } else {
        if (minBytes[0] != '(' && minBytes[0] != '[') {
            return ErrorReply.SYNTAX;
        }
        if (minBytes.length == 1) {
            return ErrorReply.SYNTAX;
        }
        minLex = minStr.substring(1);
        if (minBytes[0] == '(') {
            minInclusive = false;
        }
    }
    if (maxBytes[0] == '+') {
        maxLex = RedisZSet.MEMBER_MAX;
        maxInclusive = false;
    } else {
        if (maxBytes[0] != '(' && maxBytes[0] != '[') {
            return ErrorReply.SYNTAX;
        }
        if (maxBytes.length == 1) {
            return ErrorReply.SYNTAX;
        }
        maxLex = maxStr.substring(1);
        if (maxBytes[0] == '(') {
            maxInclusive = false;
        }
    }

    int compareMinMax = minLex.compareTo(maxLex);
    if (compareMinMax > 0 && !RedisZSet.MEMBER_MIN.equals(minLex)) {
        return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
    }
}
```

Root cause and impact:

Redis 7.2 supports reverse lexicographic ranges through `ZREVRANGEBYLEX` and the `REV` form of `ZRANGE ... BYLEX`. The follow-up commit adds sentinel-aware endpoint normalization and uses the reversed iterator for both read and store paths, which matches Redis 7.2 behavior for the validated cases.

## Finding 3: `ZRANGESTORE` likely stores the wrong members for `REV` and `LIMIT`

Status: **Fixed**

Severity: High

Cited files:

- `src/main/java/io/velo/command/ZGroup.java:1689-1711`
- `src/main/java/io/velo/command/ZGroup.java:1751-1773`
- `src/main/java/io/velo/command/ZGroup.java:1622-1649`
- `src/main/java/io/velo/command/ZGroup.java:1679-1687`
- `src/main/java/io/velo/command/ZGroup.java:1741-1748`

Code excerpt:

```java
if (doStore) {
    var dstRz = new RedisZSet();
    int storedCount = 0;
    // subMap can be empty
    var it2 = subMap.entrySet().iterator();
    while (it2.hasNext()) {
        if (storedCount >= count) {
            break;
        }
        var entry = it2.next();
        dstRz.add(entry.getValue().score(), entry.getKey());
        storedCount++;
    }

    var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
    if (!isCrossRequestWorker) {
        saveRedisZSet(dstRz, dstSlotWithKeyHash);
    } else {
        var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
        dstOneSlot.asyncExecute(() -> saveRedisZSet(dstRz, dstSlotWithKeyHash));
    }

    return dstRz.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(dstRz.size());
}
```

```java
var it = isReverse ? subMap.descendingMap().entrySet().iterator() : subMap.entrySet().iterator();
if (hasLimit) {
    int skip = offset;
    while (skip > 0 && it.hasNext()) {
        it.next();
        it.remove();
        skip--;
    }
}
```

Root cause and impact:

Redis uses a single-pass approach for `ZRANGESTORE` (verified in Redis source `src/t_zset.c` in `zrangeGenericCommand`, `genericZrangebyscoreCommand`, and `genericZrangebylexCommand`): iterate in the requested direction, skip `offset` entries, then emit up to `limit` entries. The same pass drives both the reply and the destination object.

Velo's prior BYLEX and BYSCORE store paths reconstructed the destination from a separate ascending iteration over the trimmed view. That could diverge from the requested reverse order when `REV + LIMIT` was used. The current fix mirrors Redis more closely by using the same reverse-aware iterator for both read and store paths.

## Finding 4: Several Redis 7.2 sorted-set commands are absent and fall through to `NilReply`

Status: **Confirmed**

Severity: Medium

Cited files:

- `src/main/java/io/velo/command/ZGroup.java:44-58`
- `src/main/java/io/velo/command/ZGroup.java:156-358`
- `src/test/groovy/io/velo/command/ZGroupTest.groovy:15-40`

Code excerpt:

```java
if ("zadd".equals(cmd) || "zcard".equals(cmd) || "zcount".equals(cmd)
        || "zincrby".equals(cmd)
        || "zlexcount".equals(cmd) || "zmscore".equals(cmd)
        || "zpopmax".equals(cmd) || "zpopmin".equals(cmd)
        || "zrandmember".equals(cmd)
        || "zrange".equals(cmd) || "zrangebylex".equals(cmd) || "zrangebyscore".equals(cmd)
        || "zrank".equals(cmd)
        || "zrem".equals(cmd) || "zremrangebylex".equals(cmd) || "zremrangebyrank".equals(cmd) || "zremrangebyscore".equals(cmd)
        || "zrevrange".equals(cmd) || "zrevrangebylex".equals(cmd) || "zrevrangebyscore".equals(cmd) || "zrevrank".equals(cmd)
        || "zscore".equals(cmd)) {
```

```java
if ("zunionstore".equals(cmd)) {
    return zdiffstore(false, true);
}

return NilReply.INSTANCE;
```

Root cause and impact:

Redis 7.2 includes additional sorted-set commands that are not mapped here: `ZSCAN`, `ZMPOP`, `BZMPOP`, `BZPOPMAX`, and `BZPOPMIN`. Velo currently returns `NilReply.INSTANCE` for those commands instead of either implementing Redis-compatible behavior or returning a clearer unsupported-command error. For clients using Redis 7.2 command discovery, these are observable compatibility gaps rather than internal implementation details.

## Suggested Review Order

1. Verify whether `LIMIT 0 0` should be treated as empty for all `ZRANGE` forms and whether the current `count <= 0` normalization is incorrect.
2. Verify `REV` lex semantics against Redis 7.2 using a small data set and compare both `ZRANGE ... BYLEX REV` and `ZREVRANGEBYLEX`.
3. Verify `ZRANGESTORE` on a reverse-limited query to confirm whether the stored members differ from the read reply.
4. Confirm the Redis 7.2 command surface for the missing pop/scan family and decide whether they should be implemented or explicitly rejected.

---

## Review Notes - AI Agent 2

Reviewer: AI agent 2

## Review Feedback - AI Agent 1

Reviewed commit: `507c33bd40a589aafe48b6ed3bbe4ecf02316e91`

Summary:

- The fix correctly addresses the original `ZRANGE LIMIT 0 0` bug for the zero-count case.
- The added Spock coverage exercises the changed branch and confirms the fix is executed.
- The patch is too broad for Redis 7.2 compatibility because it treats every `count <= 0` as empty once `LIMIT` is present.
- Finding 1 is fixed for the zero-count symptom, but not fully clean for Redis 7.2 semantics.

Strengths:

- The implementation changes are small and localized in `src/main/java/io/velo/command/ZGroup.java`.
- The new test method covers `BYSCORE`, `BYINDEX`, and `BYLEX` paths and verifies the empty-result behavior.
- JaCoCo shows the modified branch is covered, so this is not a test-only green result.
- The test method contains 13 existing assertions, not 8.

Concerns:

- Redis 7.2 allows negative `LIMIT` counts for `BYSCORE` and `BYLEX` ranges as a request for the remaining results. The current `count <= 0` check changes that behavior and will reject valid negative-count queries.
- The new test uses `zrange a 0 -1 limit 0 0`, but Redis 7.2 does not support `LIMIT` on the `BYINDEX` form. That makes the test a Velo-specific contract rather than a Redis-compatibility test.

Pre-commit follow-ups:

- Narrow the empty-limit guard so it only covers `count == 0` for the supported `LIMIT` forms.
- Rework the regression test to stay within Redis 7.2 syntax, and add separate assertions for negative-count behavior on `BYSCORE` and `BYLEX`.

Post-commit follow-ups:

- Re-run the focused sorted-set test cases after adjusting the `LIMIT` handling.
- Re-check JaCoCo for the updated `ZRANGE` branches and confirm the negative-count path is covered.

## Review Feedback - AI Agent 2

Reviewed commit: `507c33bd40a589aafe48b6ed3bbe4ecf02316e91`

Addressed status:

- The original zero-count symptom is fixed.
- The broader Redis 7.2 compatibility concern is not fully addressed in the current commit.

Verification:

- The current code still uses `if (count <= 0)` under the `LIMIT` branch in `src/main/java/io/velo/command/ZGroup.java`.
- Redis 7.2 still treats negative `LIMIT` counts as valid for `BYSCORE` and `BYLEX` forms.
- The new regression test still uses `BYINDEX LIMIT`, which Redis 7.2 rejects as syntax error.
- JaCoCo showed 1 partial line in the touched `ZRANGE` range, not 3.

Review notes:

- The fix is acceptable for the narrow zero-count bug that was reported.
- The implementation remains too broad if the target is strict Redis 7.2 compatibility.
- The follow-up concerns in the prior review are still valid and should stay open until the limit handling and test coverage are narrowed.

## Review Feedback - AI Agent 2

Reviewed commit: `27347433b09214d5fc5bb4252806a65569d01166`

Addressed status:

- Finding 1 is fully addressed in this commit.
- The implementation now matches Redis 7.2 semantics for the supported `LIMIT` forms.

Verification:

- `ZGroup.java` now rejects `LIMIT` on `BYINDEX` with `ErrorReply.SYNTAX`.
- `count == 0` returns empty, while `count < 0` falls back to the full range for `BYSCORE` and `BYLEX`.
- The updated test suite asserts `BYINDEX + LIMIT` is syntax error and includes a negative-count `BYSCORE` case.
- JaCoCo shows the updated `ZRANGE` branches are fully covered.
- Redis 7.2 spot checks matched the new behavior.

Review notes:

- The original zero-count bug is resolved.
- The follow-up semantic concerns from the first review are also resolved by this commit.
- Finding 1 can be treated as closed.

## Review Feedback - AI Agent 2

Reviewed commit: `fcf36f06af45198e246f155a9657ce017418d336`

Addressed status:

- Finding 2 is fully addressed in this commit.

Verification:

- `ZGroup.java` now recognizes `+` and `-` sentinels on the BYLEX boundaries and performs a reverse-aware swap when needed.
- The read path now uses `subMap.descendingMap().entrySet().iterator()` for reverse lex queries.
- The store path uses the same reverse iterator, so `ZRANGESTORE` stays aligned with the read reply.
- The new Spock test covers `ZREVRANGEBYLEX` and `ZRANGE ... REV BYLEX` cases, including sentinel boundaries and empty cases.
- Redis 7.2 spot checks matched the new behavior for `ZREVRANGEBYLEX` and `ZRANGE ... REV BYLEX`.

Review notes:

- The original reverse-lex restriction is resolved.
- The read and store paths are now consistent for the validated reverse lex cases.
- Finding 2 can be treated as closed.

## Review Feedback - AI Agent 1

Reviewed changes: current worktree diff for `src/main/java/io/velo/command/ZGroup.java` and `src/test/groovy/io/velo/command/ZGroupTest.groovy`

Summary:

- The store-path root cause is now aligned with Redis 7.2 source: a single reverse-aware pass drives both reply emission and destination storage.
- The BYSCORE `ZRANGESTORE ... REV LIMIT` case is validated against Redis output and matches.
- The BYLEX store test is directionally correct, but the current test data uses mixed scores, which Redis documents as unspecified for lex range behavior.

Strengths:

- The code now uses one iterator for both read and store paths in BYLEX and BYSCORE, matching Redis’ `zrangeGenericCommand` flow.
- The new `test zrangestore rev limit` covers the store-path case that previously regressed.
- Redis 7.2 spot checks for `ZRANGESTORE` BYSCORE reverse limit matched the stored members.

Concerns:

- The BYLEX assertion in `test zrangestore rev limit` uses members with different scores. Redis docs say lex range behavior is unspecified unless all members have the same score, so that test is not a strict compatibility proof.

Pre-commit follow-ups:

- If you want a stronger Redis-equivalence test, change the BYLEX store case to use same-score members.

Post-commit follow-ups:

- Re-run the `zrangestore rev limit` test after adjusting the BYLEX fixture if you want the test to prove Redis-compatible lex semantics rather than just exercising the code path.
Date: 2026-05-30

### Finding 1: `ZRANGE ... LIMIT 0 0` is treated as unbounded instead of empty

Status: **Confirmed**

Severity: High

Analysis:

The bug is confirmed. Two interacting issues:

1. **`hasLimit` gate** (`ZGroup.java:1477`): `hasLimit = count != 0 || offset != 0;` means `LIMIT 0 0` sets `hasLimit = false`, disabling all LIMIT logic downstream.
2. **`count` normalization** (`ZGroup.java:1606-1608`): `if (count <= 0) { count = size; }` converts the initial `count=0` to the full set size.

Result: `ZRANGE key 0 -1 LIMIT 0 0` returns all members instead of an empty result. This applies to all three range modes (BYINDEX, BYLEX, BYSCORE).

The fix should:
- Change line 1477 to always set `hasLimit = true` when the LIMIT keyword is parsed (i.e., `hasLimit = true`).
- After the normalization at 1606-1608, add a check: if the original count was 0 (before normalization), return empty immediately. Alternatively, move the `count == 0` early-return before the `count = size` normalization.

### Finding 2: Reverse lexicographic ranges are over-restricted

Status: **Confirmed**

Severity: High

Analysis:

The bug is confirmed. When `ZREVRANGEBYLEX key + -` or `ZRANGE key + - REV BYLEX` is called:

1. The dispatch code (`ZGroup.java:306-321`) passes `dd[2] = data[2]` (Redis max, e.g. `+`) and `dd[3] = data[3]` (Redis min, e.g. `-`) into `zrange()`. Inside `zrange()`, these are named `minBytes`/`maxBytes` — so `minLex` becomes `MEMBER_MAX` and `maxLex` becomes `MEMBER_MIN`.
2. The BYLEX validation at line 1525-1529 checks `compareMinMax > 0 && !MEMBER_MIN.equals(minLex)`. Since `minLex = MEMBER_MAX`, the condition is true, and it returns empty — rejecting valid reverse lex queries.
3. Unlike the BYSCORE path (lines 1569-1580) which swaps min/max for REV, the BYLEX path has no swap logic.

The fix should mirror the BYSCORE REV swap: when `isReverse && byLex` and `minLex > maxLex`, swap min/max and their inclusive flags, then proceed. The skip the `compareMinMax` early-return for the REV case.

### Finding 3: `ZRANGESTORE` likely stores the wrong members for `REV` and `LIMIT`

Status: **Fixed**

Severity: High

Analysis:

The bug is confirmed for the BYLEX and BYSCORE store paths, but NOT for the BYINDEX path.

**BYINDEX path** (lines 1610-1673): The store builds `dstRz` during the same iteration that produces the read reply (lines 1637-1638). This is correct — the same iterator (ascending or descending) feeds both the reply and the destination.

**BYLEX store path** (lines 1689-1711): The code is two-pass:
1. Pass 1 (lines 1679-1687): Skip `offset` entries using a descending iterator, removing them via `it.remove()` from `subMap`.
2. Pass 2 (lines 1689-1701): Create a **new ascending** iterator over `subMap.entrySet()` and take up to `count` entries.

If REV is used, pass 1 removes entries from the high end (descending). `subMap` then contains the lower entries. Pass 2 iterates ascending and takes the first `count` entries — which are the lowest members, not the highest as the REV query intended. The read reply (lines 1714-1733) also iterates ascending over the trimmed `subMap`, so read and store are at least self-consistent, but both produce ascending order instead of the requested descending slice.

**BYSCORE store path** (lines 1751-1773): Identical two-pass pattern with the same issue.

The fix should: iterate the store path using the same direction as the read path (descending if REV), and ensure both the read reply and the stored set reflect the same slice. Alternatively, build `dstRz` during the read iteration (like the BYINDEX path does) to avoid the two-pass inconsistency.

### Finding 4: Several Redis 7.2 sorted-set commands are absent and fall through to `NilReply`

Status: **Confirmed**

Severity: Medium (feature gap, not a correctness bug)

Analysis:

Confirmed. The following Redis 7.2 sorted-set commands are not handled in `ZGroup.handle()` and fall through to `NilReply.INSTANCE` (line 358):

- `ZSCAN` — cursor-based iteration over sorted set members
- `ZMPOP` — pop from multiple sorted sets
- `BZMPOP` — blocking version of ZMPOP
- `BZPOPMAX` — blocking pop max
- `BZPOPMIN` — blocking pop min

Additionally, `ZDIFFSTORE` is handled but was omitted from the command list in the finding; it IS implemented at line 175.

These are missing features rather than bugs. Returning `NilReply` is arguably worse than returning an unsupported-command error — clients may misinterpret nil as "empty result" rather than "command not implemented." Priority is lower since blocking commands require infrastructure changes.

### Summary

| Finding | Verdict | Action |
|---------|---------|--------|
| 1. LIMIT 0 0 unbounded | **Fixed** | Fix `hasLimit` gate + count normalization |
| 2. REV BYLEX over-restricted | **Fixed** | Add min/max swap + sentinel-aware comparison for REV BYLEX |
| 3. ZRANGESTORE REV+LIMIT wrong members | **Fixed** | Refactor BYLEX/BYSCORE to single-pass (like BYINDEX) |
| 4. Missing Redis 7.2 commands | **Confirmed** (feature gap) | Implement ZSCAN; ZMPOP/BZ* are lower priority |

---

## Review Feedback — Bug 1 Fix Commit

Reviewer: AI agent 1
Date: 2026-05-30
Reviewed commit: `507c33bd` (`fix: ZRANGE LIMIT 0 0 returns empty instead of all members`)

### Summary of the fix

Two production-code changes in `ZGroup.java`:

1. **Line 1477**: `hasLimit = count != 0 || offset != 0` → `hasLimit = true`. When the `LIMIT` keyword is parsed, `hasLimit` is now unconditionally true.

2. **Lines 1606-1614**: The `count <= 0` normalization is now gated behind `hasLimit`:
   - `hasLimit == true && count <= 0` → early return empty (the fix)
   - `hasLimit == false && count <= 0` → `count = size` (original behavior preserved)

### Correctness

The fix is correct for the reported bug. `ZRANGE key -inf +inf BYSCORE LIMIT 0 0` and `ZRANGE key - + BYLEX LIMIT 0 0` now return empty, matching Redis 7.2 semantics. `ZRANGESTORE ... LIMIT 0 0` correctly returns `IntegerReply.REPLY_0`.

Note: BYINDEX + LIMIT was also accepted by the initial fix (returning empty), but Redis 7.2 rejects it as syntax error — addressed in the follow-up commit below.

### Test changes

- 13 existing assertions updated from "returns N members" to "returns empty" or `REPLY_0` for LIMIT 0 0 cases across BYSCORE, BYINDEX, and BYLEX paths (including both read and ZRANGESTORE variants).
- 2 `LIMIT offset 0` test cases changed to `LIMIT offset count` with `count > 0` to preserve offset-only coverage (e.g., `limit 1 0` → `limit 1 1`).
- 1 ZRANGESTORE LIMIT 0 0 case changed to LIMIT 0 2 to keep the store path exercised.
- New test method `test zrange limit 0 0 returns empty` added with 7 cases: BYSCORE/BYINDEX/BYLEX LIMIT 0 0, LIMIT with count > 0, LIMIT offset+count=0, no-LIMIT BYINDEX, no-LIMIT BYSCORE.

### JaCoCo verification

```
python3 scripts/jacoco_cover.py io.velo.command.ZGroup 1605 1614 --src
Lines: 6 | Covered: 5 | Partial: 1 | Not-covered: 0
Branches: 4 lines with branches, 3 fully covered
```

The `hasLimit=true` and `hasLimit=false` branches are both fully covered. The `count=size` normalization in the else branch is covered by the two no-LIMIT test cases. Line 1611 (`count <= 0` in the else branch) is partial — the `count > 0` sub-branch is never taken because `count` is always 0 when `hasLimit=false` (no LIMIT keyword parsed, count stays at default 0). This is expected and harmless.

### Concern (non-blocking) — addressed in follow-up commit

The initial fix had three issues flagged during review:

1. **`count <= 0` too broad**: `LIMIT 0 -1` returned empty instead of all members (Redis treats negative count as "no limit"). Verified against Redis 7.2: `ZRANGE key -inf +inf BYSCORE LIMIT 0 -1` → all members.

2. **BYINDEX + LIMIT accepted**: Redis 7.2 rejects `ZRANGE key 0 -1 LIMIT 0 0` with `ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX`. The initial fix allowed it.

3. **Test used BYINDEX+LIMIT**: The new regression test `zrange a 0 -1 limit 0 0` tested a command Redis would reject.

All three were addressed in follow-up commit `27347433`:
- `count <= 0` → split into `count == 0` (return empty) and `count < 0` (set count = size, matching Redis "no limit").
- Added post-loop validation: `if (hasLimit && !byScore && !byLex) return ErrorReply.SYNTAX`.
- Updated all BYINDEX+LIMIT test cases to expect `ErrorReply.SYNTAX`.
- Added negative-count test: `BYSCORE LIMIT 0 -1` → all 10 members.
- Verified all three fixes against `redis-cli`.

### Verdict

**Bug 1 is resolved.** Both the LIMIT 0 0 bug and the three review concerns are addressed. The fix now matches Redis 7.2 behavior for `LIMIT 0 0` (empty), `LIMIT 0 -1` (no limit), and BYINDEX+LIMIT (syntax error).
