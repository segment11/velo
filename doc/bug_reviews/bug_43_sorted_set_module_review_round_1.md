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

Redis 7.2 uses `LIMIT offset count` as an explicit slice on the range result. A request such as `ZRANGE key 0 -1 LIMIT 0 0` should return no members. Velo converts `count <= 0` into `size`, which turns `LIMIT 0 0` into a full-range fetch. This looks like a pagination bug and can leak full range results when callers intentionally request an empty page.

## Finding 2: Reverse lexicographic ranges are over-restricted

Status: **Confirmed**

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

Redis 7.2 supports reverse lexicographic ranges through `ZREVRANGEBYLEX` and the `REV` form of `ZRANGE ... BYLEX`. Velo parses `REV`, but the lex-range validation still rejects reversed endpoints whenever `minLex > maxLex`. That means requests that should return descending lex results can be cut off before the set is even queried. The `RedisZSet.betweenByMember()` API already returns a sorted `NavigableMap`, so this appears to be a command-layer validation issue rather than a storage limitation.

## Finding 3: `ZRANGESTORE` likely stores the wrong members for `REV` and `LIMIT`

Status: **Confirmed**

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

The code uses one iterator for the read path, but the store path reconstructs the destination from the original ascending `subMap` / `subSet` view instead of the already-trimmed reverse view. If Redis 7.2 semantics are followed, `ZRANGESTORE ... REV LIMIT offset count` should persist exactly the slice that the read path would return. Here, the destination appears to be built from the wrong ordering context, so a reverse query can store the wrong subset even if the read reply looks correct.

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

Status: **Confirmed**

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
| 2. REV BYLEX over-restricted | **Confirmed** | Add min/max swap for REV BYLEX |
| 3. ZRANGESTORE REV+LIMIT wrong members | **Confirmed** (BYLEX, BYSCORE) | Fix store path to use same iterator direction |
| 4. Missing Redis 7.2 commands | **Confirmed** (feature gap) | Implement ZSCAN; ZMPOP/BZ* are lower priority |
