# Bug 87 — ZGroup Commands Review (Round 1)

**Module:** `io.velo.command.ZGroup` — all Z* commands (`bzmpop`*, `bzpopmax`*, `bzpopmin`*,
`zadd`, `zcard`, `zcount`, `zdiff`, `zdiffstore`, `zincrby`, `zinter`, `zintercard`,
`zinterstore`, `zlexcount`, `zmpop`, `zmscore`, `zpopmax`, `zpopmin`, `zrandmember`, `zrange`,
`zrangebylex`, `zrangebyscore`, `zrangestore`, `zrank`, `zrem`, `zremrangebylex`,
`zremrangebyrank`, `zremrangebyscore`, `zrevrange`, `zrevrangebylex`, `zrevrangebyscore`,
`zrevrank`, `zscore`, `zscan`, `zunion`, `zunionstore`) (* = intentional `NOT_SUPPORT`,
bug_43 decision)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fixes complete (agent 1) — all 12 findings fixed in 11 commits, see Fix Log; ready for agent 2 re-review
**Review branch:** `review/zgroup`
**Reviewed commit:** `2109db6304ebe366332c49329085f7b474431b76`

> Delta declaration: prior rounds already covered parts of this module and are NOT re-reported:
> bug_43 (sorted set round 1 — all 4 findings closed: ZRANGE `LIMIT` semantics incl. negative
> counts and BYINDEX+LIMIT rejection, REV BYLEX sentinel swap, ZRANGESTORE REV+LIMIT single-pass,
> ZSCAN/ZMPOP implemented + BZ* → explicit NOT_SUPPORT) and bug_10 round 2 (type level:
> `RedisZSet.decode()` member-length validation, ZINTER/ZUNION TreeSet in-place mutation — both
> fixed). ZSCAN's offset-cursor-under-mutation weakness is the same design decision open as
> bug 71.5 (HSCAN/SSCAN) — not re-reported here. Do not re-report any of these.

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/ZGroup.java` — command registry (25-306), `parseSlots()`
  (325-479), `handle()` with the zrangebylex/byscore/rev wrappers (486-703), `getRedisZSet()`/
  `saveRedisZSet()` (705-738), `zadd()` (743-888), `zcard()` (890-908), `zcount()`/zlexcount
  (914-1012), `operateZset()` (1014-1150), `zdiffstore()`/`zdiff()` (1166-1402, non-cross
  1303-1335, cross 1337-1401), `zincrby()` (1404-1447), `zintercard()` (1449-1602), `zmscore()`
  (1604-1641), `zpopmax()`/`zpopmin` (1643-1683), `zrandmember()` (1685-1767), `zrange()`
  (1769-2183: option loop 1800-1847, BYLEX 1854-1927, BYSCORE 1931-1983, BYINDEX 2016-2080,
  BYLEX read/store 2081-2130, BYSCORE read/store 2131-2182), `zrank()`/zrevrank (2185-2226),
  `zrem()` (2228-2265), `zremrangebyscore()`/bylex/byrank (2267-2437), `zscore()` (2439-2479),
  `zscan()` (2481-2571), `zmpop()` (2573-2745)
- `src/main/java/io/velo/type/RedisZSet.java` — constants (28-48: `ZSET_MAX_SIZE=4096`,
  `ZSET_MEMBER_MAX_LENGTH=255`, `MEMBER_MIN="-"`, `MEMBER_MAX="+"`), `getInitRank()` (69),
  sentinel handling in `betweenByMember` (185-189), `pollFirst/pollLast` (237-250)
- `src/main/java/io/velo/reply/BulkReply.java` — `BulkReply(double)` (54-56: `String.valueOf(d)`)
- `src/main/java/io/velo/BaseCommand.java` — `getCv()` expired filtering (756-798), `get()`
  type-filter overloads (878-890) [re-verified from bug_86 round]
- `src/main/java/io/velo/command/HGroup.java` — `getRandIndex()` (2274-2293)
- `src/test/groovy/io/velo/command/ZGroupTest.groovy` — test index (58-3233); 'test zadd'
  (262-420, incl. INCR assertions 379-394), 'test zincrby' (1145-1195), 'test zrandmember'
  (1602-1712), 'test zrank' (2394-2474), 'test zmpop' (2789-3080), 'test zscan' (3080-3233)
- `src/main/java/io/velo/command/IndexStartEndReset.java` — `reset()` used at ZGroup.java:2017,
  2383 (shared negative-index helper, hardened by bug 82.1)
- Prior rounds: `doc/bug_reviews/bug_43_sorted_set_module_review_round_1.md` (full),
  `doc/bug_reviews/bug_10_types_module_review_round_2.md` (findings index)

Reference docs (ground truth):

- https://redis.io/commands/zadd/ (fetched 2026-11-17) — "NX and XX are mutually exclusive, as
  are GT and LT"; syntax `[NX | XX] [GT | LT]`; return: "**Bulk string reply**: the updated score
  of the member when the INCR option is used", "**Nil reply**: if the operation was aborted
  because of a conflict with one of the XX/NX/LT/GT options"; example `ZRANGE ... WITHSCORES`
  shows score `1` rendered as `"1"`
- https://redis.io/commands/zrank/ (fetched 2026-11-17) — example `ZRANK myzset "four"
  WITHSCORE` → `(nil)`; "Nil reply: if the key does not exist or the member does not exist"
- https://redis.io/commands/zrandmember/ (fetched 2026-11-17) — "Bulk string reply: without the
  additional count argument"; negative-count repeats; positive-count distinct
- https://redis.io/commands/zlexcount/, https://redis.io/commands/zremrangebylex/ — `-` / `+`
  range endpoints
- https://redis.io/commands/zinter/, https://redis.io/commands/zunion/,
  https://redis.io/commands/zdiff/, https://redis.io/commands/zintercard/,
  https://redis.io/commands/zpopmin/, https://redis.io/commands/zincrby/,
  https://redis.io/commands/zscore/ — numkeys ≥ 1, count semantics, score replies
- Redis 7.2 source reference for score rendering: `d2string()` (util.c) — `%.17g` shortest
  round-trip form ("1", "1e+30"), not Java `Double.toString` ("1.0", "1.0E30")

---

## Finding 1 (Bug 87.1): ZADD INCR replies an integer count instead of the updated score (and an integer 0 instead of nil when aborted)

**Severity:** High
**Class:** Redis protocol compatibility (wrong reply type and value)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; reply-semantics correction (use Redis' `processed`/not-NOP predicate, not `added + changed`)

### Cited files / lines

```java
// ZGroup.java:883-887 — the reply ignores isIncr entirely
var handled = added + changed;
if (handled > 0) {
    saveRedisZSet(rz, slotWithKeyHash);
}
return new IntegerReply(isIncludeCh ? changed + added : added);

// ZGroup.java:828-881 — the member loop: NX/XX/GT/LT aborts `continue` with no nil signal
for (var member : members) {
    if (isNx) {
        if (rz.contains(member.e)) { continue; }        // aborted — falls through to integer 0
    } else if (isXx) {
        if (!rz.contains(member.e)) { continue; }       // aborted — falls through to integer 0
    }
    ...
    if (!isNew) {
        if (newScore == svExist.score()) { continue; }  // INCR +0 — also replies integer 0
    }
```

```groovy
// ZGroupTest.groovy:383-394 — existing tests assert the deviating behavior
reply = zGroup.execute('zadd a incr 0 member0')
then:
reply instanceof IntegerReply
(reply as IntegerReply).integer == 0
```

### Root cause

Redis ZADD with INCR changes the reply type: "the updated score of the member when the INCR
option is used" (bulk string / RESP3 double), or **nil** "if the operation was aborted because of
a conflict with one of the XX/NX/LT/GT options". Velo's `zadd()` always returns
`IntegerReply(added | added+changed)`; `isIncr` never influences the reply. So:

- `ZADD k INCR 5 m` (new member) → Redis `"5"` (bulk), Velo `(integer) 1`
- `ZADD k INCR 5 m` (existing) → Redis `"6"` (bulk), Velo `(integer) 0` (no CH)
- `ZADD k XX INCR 5 m` (missing member → aborted) → Redis `(nil)`, Velo `(integer) 0`
- `ZADD k NX INCR 5 m` (existing → aborted) → Redis `(nil)`, Velo `(integer) 0`

Any client using ZADD INCR as an atomic ZINCRBY-with-conditions (the documented pattern) gets a
wrong-typed, wrong-valued reply.

### Trigger / impact

Every `ZADD ... INCR ...` call. The existing regression tests (ZGroupTest.groovy:379-394) encode
the deviation, so the fix must update them (same precedent as bug 71.4 rewriting the `withvalues`
cases).

### Suggested fix direction

After the member loop: `if (isIncr) return handled > 0 ? new BulkReply(newScore) : NilReply.INSTANCE;`
(the loop's single-pair guarantee at 788-793 makes tracking one score sufficient). Update the two
INCR test blocks to assert bulk/nil.

---

## Finding 2 (Bug 87.2): ZADD accepts mutually exclusive options — NX+XX, GT+LT, NX+GT/LT, INCR+GT/LT — with silent else-if precedence instead of syntax errors

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `PARTIALLY CONFIRMED` — the three real mutual-exclusion errors are confirmed; the `INCR`+`GT`/`LT` sub-claim is `REFUTED` (Redis accepts that combination)

### Cited files / lines

```java
// ZGroup.java:762-786 — the option loop sets flags independently, no exclusivity validation
if ("nx".equals(tmp)) { isNx = true; ... }
else if ("xx".equals(tmp)) { isXx = true; ... }
else if ("gt".equals(tmp)) { isGt = true; ... }
else if ("lt".equals(tmp)) { isLt = true; ... }
...

// ZGroup.java:829-855 — else-if chains give silent precedence to the first flag
if (isNx) {
    if (rz.contains(member.e)) { continue; }
} else if (isXx) {                       // XX silently ignored when NX is also given
    ...
} else if (isGt) {                       // ... inside: GT beats LT silently
    if (svExist.score() <= member.score) { continue; }
} else if (isLt) {
    ...
}
```

### Root cause

Redis documents "`NX` and `XX` are mutually exclusive, as are `GT` and `LT`" and its parser
additionally rejects NX combined with GT/LT ("GT, LT, and/or NX options at the same time are not
compatible") and INCR combined with GT/LT. Velo's parser accepts every combination; the
subsequent else-if chains silently pick a winner (NX over XX; GT over LT; with GT/LT present the
NX/XX branch is skipped entirely). A client sending `ZADD k XX NX 5 m` gets NX semantics with a
success reply instead of `ERR syntax error`; `GT+LT` and `INCR+GT/LT` similarly misbehave (for
INCR+GT the GT comparison at 846-848 even compares the *increment* against the existing score,
nonsense semantics).

### Trigger / impact

```
SADD-equivalent: ZADD k 1 m
ZADD k XX NX 2 m    → Redis: ERR syntax error       Velo: OK-ish NX behavior (ignored, member kept at 1)
ZADD k GT LT 2 m    → Redis: ERR syntax error       Velo: GT semantics silently
ZADD k INCR GT 2 m  → Redis: ERR syntax error       Velo: compares increment as absolute score
```

### Suggested fix direction

After the option loop add the Redis validations: `isNx && isXx`, `isGt && isLt`,
`isNx && (isGt || isLt)`, and `isIncr && (isGt || isLt)` → `ErrorReply.SYNTAX` (Redis uses
distinct messages; the repo accepts message drift — any error reply fixes the semantic bug).

---

## Finding 3 (Bug 87.3): ZRANDMEMBER without a count returns a 1-element array instead of a bulk string

**Severity:** High
**Class:** Redis protocol compatibility (wrong reply type on the default form)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2

### Cited files / lines

```java
// ZGroup.java:1706-1708, 1725 — hasCount is tracked and used only for the missing-key reply
int count = 1;
boolean hasCount = false;
...
if (rz == null || rz.isEmpty()) {
    return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;   // correct here
}

// ZGroup.java:1737, 1766 — but the populated reply is always an array
var replies = new Reply[absCount * (withScores ? 2 : 1)];
...
return new MultiBulkReply(replies);        // ZRANDMEMBER key → *1 ["m"] instead of $1 "m"

// contrast SGroup.java:2149-2157 — srandmember branches correctly
if (hasCount) { ... new MultiBulkReply(replies); } else { return new BulkReply(members[0]); }
```

### Root cause

`ZRANDMEMBER key` (no count) must reply a **bulk string** — "without the additional count
argument, the command returns a randomly selected member" (redis.io/commands/zrandmember, Return
information). Velo builds a `MultiBulkReply` unconditionally: `absCount` defaults to 1, so the
single form returns `*1\r\n$m\r\n...` — an array — where every Redis client expects a bulk
string. redis-cli prints `1) "m"` instead of `"m"`; typed clients (ioredis `zrandmember(key)`,
redis-py `zrandrandmember(key)`) return an array where a string is expected and downstream code
breaks.

### Trigger / impact

Any `ZRANDMEMBER key` call without count. The sibling `srandmember` (SGroup) has the correct
branch, so this is an omission, not a design choice. `'test zrandmember'`
(ZGroupTest.groovy:1602-1712) presumably asserts the array shape (reviewer to confirm) — tests
must be updated with the fix.

### Suggested fix direction

Mirror SGroup.srandmember: `if (!hasCount) return new BulkReply(<the selected member>);` after
the selection loop (with `withScores` impossible without count per the 1695-1704 validation).

---

## Finding 4 (Bug 87.4): ZRANK / ZREVRANK with WITHSCORE reply an empty array for a missing key or member — Redis replies nil

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; wire-format correction (Redis answers a null array `*-1`, not a nil bulk `$-1`)

### Cited files / lines

```java
// ZGroup.java:2202-2211 — both miss branches swap nil for an empty array when withScore is set
var rz = getRedisZSet(slotWithKeyHash);
if (rz == null || rz.isEmpty()) {
    return withScore ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
}

var sv = rz.get(new String(memberBytes));
if (sv == null) {
    return withScore ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
}
```

### Root cause

Redis ground truth (redis.io/commands/zrank, fetched; example in doc):

```
redis> ZRANK myzset "four" WITHSCORE
(nil)
```

"Nil reply: if the key does not exist or the member does not exist in the sorted set" — one nil,
regardless of WITHSCORE. Velo returns `MultiBulkReply.EMPTY` (`*0`) on the WITHSCORE path — an
empty array, which clients distinguish from nil (RESP2 `*0` vs `$-1`). A client testing
"member exists" via ZRANK WITHSCORE sees an empty array and may treat it as a degenerate result
rather than "absent".

### Trigger / impact

`ZRANK missing_key m WITHSCORE`, `ZRANK existing_key missing_member WITHSCORE`, and both ZREVRANK
equivalents.

### Suggested fix direction

Return `NilReply.INSTANCE` on both miss branches unconditionally (drop the `withScore ?`
ternaries). Update the WITHSCORE miss assertions in 'test zrank' if present.

---

## Finding 5 (Bug 87.5): ZLEXCOUNT and ZREMRANGEBYLEX reject the `-` / `+` sentinel endpoints

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verified by agent 2

### Cited files / lines

```java
// ZGroup.java:934-940 — zcount(byLex=true) = ZLEXCOUNT
if (byLex) {
    if (minBytes[0] != '(' && minBytes[0] != '[') {
        return ErrorReply.SYNTAX;                 // '-' rejected
    }
    if (maxBytes[0] != '(' && maxBytes[0] != '[') {
        return ErrorReply.SYNTAX;                 // '+' rejected
    }
    minLex = minStr.substring(1);
    maxLex = maxStr.substring(1);

// ZGroup.java:2292-2298 — zremrangebyscore(byLex=true) = ZREMRANGEBYLEX, same shape
if (minBytes[0] != '(' && minBytes[0] != '[') {
    return ErrorReply.SYNTAX;                     // '-' rejected
}
...
minLex = minStr.substring(1);

// contrast ZGroup.java:1855-1890 — zrange BYLEX handles '-', '+' correctly
if (minBytes[0] == '-') {
    minLex = RedisZSet.MEMBER_MIN;
    ...
```

### Root cause

The lex-range family shares one endpoint grammar in Redis: `-` (minimum), `+` (maximum),
`[v`/`(v`. `ZRANGEBYLEX`/`ZREVRANGEBYLEX` (via `zrange()`, fixed in bug_43 round) accept the
sentinels, but `ZLEXCOUNT` (934-940) and `ZREMRANGEBYLEX` (2292-2298) accept only `(`/`[`.
`ZLEXCOUNT k - +` — the canonical "count all" idiom — and `ZREMRANGEBYLEX k - +` — the canonical
"clear the zset by lex" idiom — both fail with a syntax error. `betweenByMember` already handles
`MEMBER_MIN`/`MEMBER_MAX` sentinels (RedisZSet.java:185-189), so the fix is purely in the parsers.

Related leniency in the same two parsers: a lone `[` or `(` yields `minLex=""` where Redis
replies "min or max not valid string range item" — worth fixing in the same pass.

### Trigger / impact

```
ZADD k 0 a 0 b 0 c
ZLEXCOUNT k - +        → Redis: 3        Velo: ERR syntax error
ZREMRANGEBYLEX k - +   → Redis: 3        Velo: ERR syntax error
```

Lex-range tooling (autocomplete indexes, prefix pruning) uses these forms constantly.

### Suggested fix direction

Port the endpoint normalization from `zrange()` (1855-1890) into `zcount(true)` and
`zremrangebyscore(..., byLex=true, ...)`: `-` → `MEMBER_MIN`, `+` → `MEMBER_MAX`, reject
1-byte `[`/`(` args.

---

## Finding 6 (Bug 87.6): numkeys=1 rejected by ZDIFF / ZINTER / ZUNION / ZINTERCARD and the three *STORE variants — Redis requires only numkeys ≥ 1

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; relaxation-bound correction (`numKeys < 1`, `numkeys = 0` must still error)

### Cited files / lines

```java
// ZGroup.java:387-390 — parseSlots (zdiff/zinter/zintercard/zunion)
if (numKeys < 2) {
    return slotWithKeyHashList;          // empty list
}

// ZGroup.java:424-427 — parseSlots (zdiffstore/zinterstore/zunionstore), same
// ZGroup.java:1199-1201 — zdiff()
if (numKeys < 2) {
    return ErrorReply.INVALID_INTEGER;   // ZDIFF 1 k → error
}

// ZGroup.java:1462-1464 — zintercard(), same check
if (numKeys < 2) {
    return ErrorReply.INVALID_INTEGER;   // ZINTERCARD 1 k → error
}
```

### Root cause

Redis's numkeys for the Z set-operation family must be positive ("numkeys should be greater than
0"), and single-key forms are valid and meaningful: `ZINTER 1 k` returns the set itself,
`ZUNION 1 k` likewise, `ZDIFF 1 k` the same, `ZINTERCARD 1 k` its cardinality, and the *STORE
variants store a copy (with WEIGHTS/AGGREGATE applied). Velo rejects `numKeys < 2` in both
`parseSlots` (empty slot list → random worker dispatch for the read forms) and the command
bodies (`INVALID_INTEGER`). Single-key self-copy idioms (`ZUNIONSTORE dst 1 src WEIGHTS 2`) —
used to scale scores — are impossible.

### Trigger / impact

```
ZADD k 1 a 2 b
ZINTER 1 k            → Redis: a b (with scores)     Velo: ERR invalid integer
ZUNIONSTORE d 1 k WEIGHTS 2 → Redis: 2 (d = {2a 4b}) Velo: ERR invalid integer
```

### Suggested fix direction

Relax both the `parseSlots` guards (373-403, 405-440) and the body guards (1199-1201, 1462-1464)
to `numKeys < 1`. The downstream code paths all handle a single key (other-key lists empty);
spot-check `operateZset` inter/union with an empty otherRzList (no-op → returns the set itself —
correct) and the read/store reply paths.

---

## Finding 7 (Bug 87.7): Every score reply uses Java double formatting ("1.0", "1.0E30") instead of Redis's shortest round-trip form ("1", "1e+30")

**Severity:** Medium
**Class:** Redis protocol compatibility (wire-format drift, pervasive)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; formatter-spec correction (port `d2string`, not a `%.{p}g` approximation)

### Cited files / lines

```java
// src/main/java/io/velo/reply/BulkReply.java:54-56 — the only double→bytes path
public BulkReply(double d) {
    this.raw = String.valueOf(d).getBytes();
}
```

```groovy
// ZGroupTest.groovy:1155-1164 — tests assert the Java form
reply = zGroup.execute('zincrby a 1 member0')
then:
(reply as BulkReply).asString() == '1.0'
```

### Root cause

Redis renders zset scores with `d2string()` (util.c): `%.17g` reduced to the shortest form that
round-trips — integer-valued doubles print without a decimal point (`1`, not `1.0`) and large
magnitudes print as `1e+30`, not `1.0E30`. Velo's `BulkReply(double)` uses `String.valueOf`, so
every score on the wire drifts: `ZADD k 1 m; ZSCORE k m` → Redis `"1"`, Velo `"1.0"`;
`ZADD k 1e30 m` → Redis `"1e+30"`, Velo `"1.0E30"`. Affects ZSCORE, ZINCRBY, ZMSORE, all
WITHSCORES/`withScores` replies, ZPOPMIN/ZPOPMAX, ZMPOP, ZRANDMEMBER WITHSCORES, ZRANK WITHSCORE,
and the ZADD INCR fix direction in 87.1. Clients that compare scores as strings (config
tooling, snapshot diffing, cross-store migration) see every integer score mismatched. (Same
class as the bug_78 residual "HINCRBYFLOAT exact-format divergence" — never fixed for zset.)

### Trigger / impact

Any integer or extreme-magnitude score in any zset reply. Fractional scores like `1.5` happen to
match; `1.0`-style input (`ZADD k 1.0 m`) always diverges.

### Suggested fix direction

Add a Redis-compatible formatter (shortest %.17g reduction — e.g. try `%.{p}g` for p in 1..17 and
keep the first that round-trips through `Double.parseDouble`, normalizing `e` notation to
Redis's lowercase-`e` form) and use it in `BulkReply(double)`. Note the change is visible to
existing tests that assert `'1.0'` (ZGroupTest.groovy:1158, 1164, and any WITHSCORES string
assertions — reviewer should grep) — those must be updated in the same commit.

---

## Finding 8 (Bug 87.8): ZRANDMEMBER / ZPOPMIN / ZPOPMAX with count 0 reply an error — Redis replies an empty array

**Severity:** Low
**Class:** Redis protocol compatibility (edge case)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2 against live Redis 7.2.11 (0 → empty array for all three commands)

### Cited files / lines

```java
// ZGroup.java:1716-1718 — zrandmember()
if (count == 0) {
    return ErrorReply.INVALID_INTEGER;
}

// ZGroup.java:1661-1663 — zpopmax()/zpopmin()
if (count <= 0) {
    return ErrorReply.VALUE_NOT_POSITIVE;
}
```

### Root cause

For the random/pop families Redis treats a zero count as "return nothing": `SRANDMEMBER key 0`
→ empty array (documented; SGroup.srandmember already returns EMPTY via the empty index list),
and the same convention covers `ZRANDMEMBER key 0`, `ZPOPMIN key 0`, `ZPOPMAX key 0` (negative
counts remain errors: "value is out of range, must be positive" for pop; repeats semantics for
randmember). Velo errors on 0 (`INVALID_INTEGER` / `VALUE_NOT_POSITIVE`) and is internally
inconsistent with its own `srandmember`. Reviewer to confirm the Z forms against live Redis 7.2
(checklist below) since the doc page spells the negative case but not zero.

### Trigger / impact

`ZRANDMEMBER k 0`, `ZPOPMIN k 0`, `ZPOPMAX k 0` with existing or missing keys.

### Suggested fix direction

`count == 0` → `MultiBulkReply.EMPTY` (randmember, both pops); keep negative-count handling
(randmember: repeats; pops: error).

---

## Finding 9 (Bug 87.9): ZRANK / ZREVRANK option parsing is positional and lenient — a member literally named "withscore" is misparsed as the option, and junk 4th arguments are silently ignored

**Severity:** Low
**Class:** Redis protocol compatibility (edge case)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; scope correction (the option is positional, argv[3] only)

### Cited files / lines

```java
// ZGroup.java:2186-2200
if (data.length != 3 && data.length != 4) {
    return ErrorReply.FORMAT;
}
...
boolean withScore = "withscore".equalsIgnoreCase(new String(data[data.length - 1]));
```

### Root cause

The WITHSCORE flag is detected by peeking at the **last** argument regardless of position:

- `ZRANK k withscore` (3 args, member is the literal string "withscore", any case) → `withScore
  = true`, the member under test becomes... still `data[2]` = "withscore", but the reply shape
  becomes `[rank, score]`/EMPTY instead of a plain rank integer — Redis treats it as a member
  lookup and replies an integer.
- `ZRANK k a b` (4 args, last not "withscore") → `withScore=false`, the code proceeds with
  member `data[2]` and **silently ignores** `b`. Redis: `ERR syntax error` (the 4th argument
  must be the WITHSCORE token).

### Trigger / impact

Members named "withscore"/"WITHSCORE" (case-insensitive collision), or any 4-argument form whose
last argument is not WITHSCORE.

### Suggested fix direction

Strict positional parse: `withScore` may only be true when `data.length == 4 &&
"withscore".equalsIgnoreCase(data[3])`; `data.length == 4` without the token → `ErrorReply.SYNTAX`.

---

## Finding 10 (Bug 87.10): NaN accepted as a ZADD score, ZINCRBY increment, and WEIGHTS weight — Redis rejects NaN ("value is not a valid float")

**Severity:** Low
**Class:** Edge case (data corruption of ordering)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; mechanism correction (Java's `parseDouble` rejects lower-case `nan`; the hole is the exact-case `NaN`/`+NaN`/`-NaN`, and Velo wrongly rejects `inf`)

### Cited files / lines

```java
// ZGroup.java:804-808 — zadd score
double score;
try {
    score = Double.parseDouble(new String(scoreBytes));   // "nan" parses to NaN
} catch (NumberFormatException e) {
    return ErrorReply.NOT_FLOAT;
}

// ZGroup.java:1415-1419 — zincrby increment, same shape
// ZGroup.java:1256-1261 — weights, same shape
```

### Root cause

Java's `Double.parseDouble` accepts `NaN`/`nan` (case-insensitive), so the NOT_FLOAT guard never
fires for it. Redis explicitly rejects NaN in `getDoubleFromObject` (infinity is *valid* for
scores — do not reject that). A NaN score enters the TreeSet comparator where every NaN
comparison is false, corrupting the iteration order (ranks, ranges, pops all become undefined),
and the reply path renders `"NaN"`. Same class as the known bug_78 follow-up "HGroup.hincrby2
non-finite float" (HINCRBYFLOAT) — reported here for the zset paths.

### Trigger / impact

`ZADD k nan m`, `ZINCRBY k nan m`, `ZDIFF 2 a b WEIGHTS nan 1` — stored/garbage ordering, no
error.

### Suggested fix direction

After each parse: `if (Double.isNaN(score)) return ErrorReply.NOT_FLOAT;` (keep ±Infinity
allowed per Redis).

---

## Finding 11 (Bug 87.11): *STORE destination key length is not validated against KEY_MAX_LENGTH (zdiffstore / zinterstore / zunionstore / zrangestore)

**Severity:** Low
**Class:** Edge case (invariant violation — same class as bug 85.5 / bug 86.14)
**Status:** `CONFIRMED (reproduced)` — verified by agent 2; trigger correction (the reproduction needs `numkeys = 2`, `numkeys = 1` fails earlier with 87.6)

### Cited files / lines

```java
// ZGroup.java:1280-1283 — zdiff() validates only the SOURCE keys
for (int i = 2, j = 0; i < numKeys + 2; i++, j++) {
    var keyBytes = dd[i];
    if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
        return ErrorReply.KEY_TOO_LONG;
    }
    ...
}
// dstKeyBytes (zdiffstore: data[1] at 1179) never length-checked before slot(dstKeyBytes) at 1189

// ZGroup.java:592-606 — zrangestore handle(): dstKeyBytes = data[1], passed to zrange(dd, dstKey)
// zrange() checks dd[1] (the SRC key, 1781-1784) — the dst key is never checked
```

### Root cause

Every write in this class validates its primary keys against `CompressedValue.KEY_MAX_LENGTH`,
but the destination keys of the four STORE commands are exempt: a 257+ byte dst is accepted and
written through `slot(dstKeyBytes, ...)` + `saveRedisZSet`, violating the key-length invariant
enforced everywhere else (RESTORE got the identical fix in bug 85.5 `c0c5a516`; SORT STORE is
bug 86.14, same shape).

### Trigger / impact

`ZINTERSTORE <257-byte-key> 1 src` / `ZRANGESTORE <257-byte-key> src 0 -1` → key stored instead
of `KEY_TOO_LONG`.

### Suggested fix direction

One guard per entry point (`zdiffstore` 1167-1181 for the three stores; `zrangestore` in
`handle()` 592-606): `if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) return ErrorReply.KEY_TOO_LONG;`

---

## Notes / non-findings

- `zdiff()` already handles the missing-first-key union correctly (1298-1300: `rz = new
  RedisZSet()` for union) — the ZGroup equivalent of SGroup bug 86.1 does NOT exist here;
  recorded so nobody re-checks.
- ZRANGE LIMIT semantics (count 0 / negative counts / BYINDEX+LIMIT rejection), REV BYLEX
  sentinel swap, ZRANGESTORE REV+LIMIT single-pass — all bug_43, closed; re-verified in place at
  1800-1847, 1854-1927, 2003-2014.
- ZSCAN's offset cursor restarts per page and shares the completeness-under-mutation weakness of
  SSCAN/HSCAN — bug 71.5 open design decision; not re-reported. ZSCAN's `count <= 0` replies
  `SYNTAX` where sscan/hscan use `INVALID_INTEGER` — sibling inconsistency only, semantic
  behavior (reject) is correct; accepted drift.
- ZSCAN/ZPOPMIN/... wrong-type behavior: `getRedisZSet` → `get(..., SP_TYPE_ZSET)` throws
  `TypeMismatchException` → RequestHandler maps to WRONGTYPE (verified path shared with
  SGroup, bug_71 §71.3). ZSCAN needs no 71.3-style pre-check: zsets have no split-storage
  layout, the main-key CV always exists.
- ZMPOP: first-non-empty-key order, MIN/MAX, COUNT cap, wrong-type ordering, cross-slot
  per-slot decode (IndexAndRz) — all reviewed by bug_43's multi-pass feedback and accepted;
  the dst-less fire-and-forget `asyncExecute(() -> saveRedisZSet(...))` (2736) matches the
  accepted sdiffstore pattern from bug_84's re-review (reactor-path analysis); the
  asyncRun/asyncExecute inconsistency between the zrange store branches (2170 vs 2067/2119) is
  cosmetic.
- `ZADD ... INCR` single-pair enforcement (788-793), GT/LT only gating existing members (new
  members still added — matches doc), CH counting, mid-loop ZSET_MAX_SIZE error without save
  (atomic), `ZSET_MEMBER_MAX_LENGTH`/`ZSET_MAX_SIZE` design guards (cf. 82.6 won't fix).
- BYINDEX + LIMIT inside the byIndex branch (2035-2040) is dead code (LIMIT already rejected at
  1845-1847) — harmless.
- `zrange` accepts a literal `byindex` token (1838-1839) and `[5` inclusive score prefixes
  (1939/1956) — lenient superset of Redis grammar; `inf`/`infinity`/`(-inf` spellings that
  `Double.parseDouble` rejects are NOT_FLOAT where Redis accepts — accepted drift (small).
- `ZDIFF 2 a b junk` silently ignores a trailing non-`withscores` token (1270-1275), and the
  *STORE variants silently accept WITHSCORES (1264, ignored for stores) — leniency drift,
  not reported as findings.
- `zremrangebyrank` negative-index handling delegates to `IndexStartEndReset` (shared,
  hardened by bug 82.1) — not re-audited.
- `zscore` uses binary-safe `Arrays.equals` over the iterate callback — correct for binary
  members; member-encoding round-trip through `String` in zadd/zrem/zincrby etc. is the
  systemic RedisHashKeys/RedisZSet design (String-based members), out of scope here.
- `bzmpop`/`bzpopmax`/`bzpopmin` → NOT_SUPPORT — bug_43 implemented decision, tests exist
  ('test blocking sorted set pops not supported', 3233).
- `getCv()` filters expired CVs (BaseCommand.java:777-779) — no expired-key findings for any
  Z command (they all branch on getRedisZSet == null).
- `zrandmember` count==size returns all members in sorted (not shuffled) order — Redis
  documents "the order of elements in the reply is not truly random" for positive counts;
  compliant.
- `zmpop` `numkeys <= 0` → `RANGE_OUT_OF_INDEX` and numkeys>args → FORMAT — message-shape drift
  accepted per repo precedent.

## Reviewer checklist (for AI agent 2)

1. **87.1**: InMemoryGetSet + `execute('zadd a 1 m')`, then `execute('zadd a incr 5 m')` → Velo
   IntegerReply(0) vs Redis bulk "6"; `execute('zadd b xx incr 5 m')` (b missing) → Velo
   IntegerReply(0) vs Redis nil. Live Redis 7.2 differential recommended.
2. **87.2**: `execute('zadd a nx xx 2 m')` / `'zadd a gt lt 2 m'` / `'zadd a incr gt 2 m'` →
   Velo accepts (integer/continue), Redis `ERR syntax error` / compatibility errors.
3. **87.3**: `execute('zrandmember a')` with a populated zset → assert reply is MultiBulkReply
   (1 element) in Velo; Redis doc: bulk string. Compare with `srandmember` shape.
4. **87.4**: `execute('zrank a nomember withscore')` on populated a → Velo
   `MultiBulkReply.EMPTY` vs Redis nil; same for missing key.
5. **87.5**: `execute('zlexcount a - +')` / `execute('zremrangebylex a - +')` → Velo SYNTAX vs
   Redis 3 / 3 (build zset of 3 same-score members).
6. **87.6**: `execute('zinter 1 a')` → Velo INVALID_INTEGER vs Redis members; `execute('zunionstore
   d 1 a weights 2')` → Velo INVALID_INTEGER vs Redis 2.
7. **87.7**: `execute('zadd a 1 m')` then `execute('zscore a m')` → Velo "1.0" vs Redis "1";
   `zadd a 1e30 m` → score string compare.
8. **87.8**: `execute('zrandmember a 0')`, `execute('zpopmin a 0')` → Velo error replies vs
   Redis empty arrays (confirm the Z forms live — doc covers SRANDMEMBER explicitly).
9. **87.9**: `execute('zrank a withscore')` where the member "withscore" exists → Velo array
   reply vs Redis integer; `execute('zrank a m junk')` → Velo integer vs Redis syntax error.
10. **87.10**: `execute('zadd a nan m')` → Velo accepts (integer 1) vs Redis "value is not a
    valid float"; then `zrange a 0 -1` ordering sanity.
11. **87.11**: `execute('zrangestore <257-char-key> a 0 -1')` / `execute('zunionstore <257-char-key> 1 a')`
    → Velo writes vs KEY_TOO_LONG.

## Review Feedback (AI agent 2, round 1)

**Method.** Live **Redis 7.2.11** differential (`redis-server --port 7777`, python RESP client, raw-socket
captures for the exact wire bytes) + **Redis 7.2.14 upstream source** in `~/ws/redis`
(`src/t_zset.c`, `src/util.c`, `src/server.c`, `tests/unit/type/zset.tcl`) + Spock **temp specs**
(`src/test/groovy/io/velo/command/ZGroupReviewTempSpec{,2..8}.groovy` — 8 feature methods, all
green, printed evidence quoted below; the temp specs were deleted after the reproduction, the
sources are preserved in `/tmp/opencode/ZGroupReviewTempSpec*.groovy`, evidence log
`doc/bug_reviews/logs/20261117_143600_zgroup_review_agent2_bug_87.log`) + static inspection of the
cited lines. Reviewed at commit `2109db63` (= `HEAD`; no code change since the report baseline —
the working tree contains only the report itself).

The report's reviewer checklist was followed in full (11/11 recipes reproduced). **All 11 author findings
are confirmed — one of them only partially (87.2, whose `INCR`+`GT`/`LT` sub-claim is refuted) — and
every one of them carries at least one correction to the report's root cause or fix direction
(87.1, 87.4, 87.6, 87.7, 87.8, 87.9, 87.10, 87.11). In addition the reviewer found a **new,
higher-severity bug the report missed** — `ZADD`'s `GT`/`LT` comparison is inverted (87.12), which
silently corrupts stored scores.**

### Per-finding verdicts

| Finding | Verdict | Evidence |
|---|---|---|
| 87.1 | `CONFIRMED (reproduced)` — with a **correction to the reply semantics** | Redis 7.2.11 raw wire: `zadd k1 incr 5 m1` → `$2\r\n16\r\n` (bulk string of the updated score); `zadd k2 xx incr 5 m` (missing key) → `$-1\r\n` (nil); `zadd k3 nx incr 5 m` (member exists) → `$-1\r\n` (nil). Velo (`ZGroupReviewTempSpec`): `zadd a incr 5 m` → `IntegerReply`; `zadd b xx incr 5 m` → `IntegerReply`. **Correction to the report:** the reply is *not* "nil when the operation was aborted" only — Redis answers **nil whenever the member was not processed**, which includes the `INCR`-with-`GT`/`LT`-not-updated case (`zadd ztmp lt incr 1 x` → nil, upstream `tests/unit/type/zset.tcl:253` "ZADD INCR LT/GT replies with nill if score not updated"). The report's suggested fix (`handled > 0 ? BulkReply(newScore) : NilReply.INSTANCE`) is **wrong for that case**: `handled` counts `added + changed`, so an `INCR` that resolved to "score unchanged" (e.g. `zadd k incr 0 m` where `m` has score 10) is counted as *not handled* by Velo and would answer nil, while Redis answers the bulk `"10"` (verified live: `zadd k incr 0 m` → `$2\r\n10\r\n`, score unchanged at 10). The correct predicate is Redis's `processed` counter (`retflags & ZADD_OUT_NOP`), i.e. "the member was visited and the update was not vetoed", not "the score changed". |
| 87.2 | `PARTIALLY CONFIRMED` — the **mutual-exclusion** half is confirmed, the **`INCR`+`GT`/`LT`** half is `REFUTED` | Redis 7.2.11 live: `zadd o1 xx nx 2 m` → `ERR XX and NX options at the same time are not compatible`; `zadd o1 gt lt 2 m` → `ERR GT, LT, and/or NX options at the same time are not compatible`; `zadd o1 nx gt 2 m` → the same `GT, LT, and/or NX` error. Velo accepts all three silently (`IntegerReply`). Upstream source `~/ws/redis/src/t_zset.c:1762-1773` (`zaddGenericCommand`) has exactly three incompatibility checks — `nx && xx`, `(gt && nx) \|\| (lt && nx) \|\| (gt && lt)` — and **no** `INCR`+`GT`/`LT` check; the same three checks are in the `unstable` branch. **REFUTED sub-claim:** the report states Redis also rejects "`INCR` combined with `GT`/`LT`". It does not: Redis 7.2.11 answers `zadd a incr gt 2 m` → `12` (bulk) and `zadd a incr lt 2 m` → nil; upstream `tests/unit/type/zset.tcl:253-283` exercises `ZADD INCR LT/GT` as a *supported* combination ("ZADD INCR LT/GT replies with nill if score not updated"). Adding an `isIncr && (isGt || isLt)` → syntax-error check would therefore **break** Redis compatibility. Fix only the three real exclusions. |
| 87.3 | `CONFIRMED (reproduced)` | Redis 7.2.11 raw wire: `zrandmember r1` → `$1\r\nb\r\n` — a **bulk string**; upstream `zrandmemberCommand` (`~/ws/redis/src/t_zset.c:4364-4394`): *"Handle variant without `<count>` argument. Reply with simple bulk string"* → `zsetReplyFromListpackEntry(c,&ele)`. Velo (`ZGroupReviewTempSpec`): `zrandmember a` → `MultiBulkReply` with 1 element. The report's `SGroup.java:2149-2157` contrast is accurate (`srandmember` branches to `new BulkReply(members[0])` when `!hasCount`). Note the missing-key case is already correct in Velo (`NilReply.INSTANCE`, matching Redis's `$-1`). |
| 87.4 | `CONFIRMED (reproduced)` — with a **wire-format correction** | Redis 7.2.11 raw wire: `zrank r1 nomember withscore` → `*-1\r\n` and `zrank nosuchkey m withscore` → `*-1\r\n` — a **null array**, not a nil bulk (`$-1`). Upstream `zrankGenericCommand` (`t_zset.c`) uses `reply = opt_withscore ? shared.nullarray[c->resp] : shared.null[c->resp]` for the missing key and `addReplyNullArray(c)` for the missing member. Velo: `zrank a memberX withscore` → `MultiBulkReply.EMPTY` (`*0\r\n`) — an **empty** array, which is a third, different thing again. The report's proposed fix ("return `NilReply.INSTANCE` on both miss branches") would produce `$-1` — still not `*-1`. The right reply is `MultiBulkReply.NULL` (which Velo already has, used at `LGroup.java:600`), i.e. `*-1` in RESP2 / `_\r\n` in RESP3. |
| 87.5 | `CONFIRMED (reproduced)` | Redis 7.2.11 live: `zlexcount l - +` → `3`, `zremrangebylex l - +` → `3`. Upstream `zslParseLexRangeItem` (`t_zset.c:573-598`) accepts a bare `+`/`-` as `shared.maxstring`/`shared.minstring`; `zslParseLexRange` then feeds them into the range. Velo (`ZGroupReviewTempSpec`): `zlexcount a - +` → `ErrorReply{syntax error}`, `zremrangebylex a - +` → `ErrorReply{syntax error}`. The report's related note ("a lone `[` or `(` yields `minLex=""` where Redis replies *min or max not valid string range item*") is **partly wrong**: Redis accepts a lone `[`/`(` (it becomes the empty-string bound — live `zlexcount l2 [ +` → `3`, `zlexcount l2 ( +` → `3`) and only rejects a token that starts with none of `( [ + -` (live `zlexcount l2 abc def` → `ERR min or max not valid string range item`). So the "reject 1-byte `[`/`(`" half of the suggested fix direction would be **wrong**; the ported `zrange()` BYLEX normalisation (1855-1890) already handles the sentinels and should be reused verbatim for the two parsers at hand, minus the 1-byte-bracket rejection that `zrange()` itself carries (lines 1865-1867, 1883-1885 — that one is itself a divergence from Redis, but it lives in the `zrange()` path already reviewed by bug 43 and is **out of scope** here). |
| 87.6 | `CONFIRMED (reproduced)` — with a **relaxation-bound correction** | Redis 7.2.11 live: `zinter 1 a` → `[]` (empty, key absent), `zunion 1 a` → `[]`, `zdiff 1 a` → `[]`, `zintercard 1 a` → `0`, `zunionstore d 1 a weights 2` → `2` (stored `a`=2, `b`=4), `zinterstore d 1 a` → `2`, `zdiffstore e 1 a` → `2`, `zintercard 1 s1 limit 1` → `1`. Upstream `zunionInterDiffGenericCommand` (`t_zset.c:2585-2600`) rejects only `setnum < 1` with *"at least 1 input key is needed for '<cmd>' command"*; `zintercard numkeys=1` is legal and returns the cardinality of the single key. Velo (`ZGroupReviewTempSpec`): all of the above → `ErrorReply{value is not an integer or out of range}` / `syntax error`. **Correction to the report's fix direction:** the guards must be relaxed to `numKeys < 1` (reject 0, accept 1) — the report says "relax … to `numKeys < 1`" in one place and "numkeys ≥ 1" in another; `numKeys < 1` is the correct predicate, and `numKeys == 0` must keep producing an error (Redis: `ERR at least 1 input key is needed for 'zinter' command`). Also note the report's own claim that the `parseSlots` guard matters for *routing* is right: `parseSlots` returning an empty list makes `Request.getSingleSlot()` return `SLOT_CAN_HANDLE_BY_ANY_WORKER`, i.e. the request is dispatched to an arbitrary worker — so **both** the `parseSlots` guard (387-390, 424-427) and the body guards (1199-1201, 1462-1464) must be relaxed, and `parseSlots` must then actually register the single key's slot. |
| 87.7 | `CONFIRMED (reproduced)` — with a **formatter-spec correction** | Redis 7.2.11 live raw wire: `zscore` of a score-1 member → `$1\r\n1\r\n`; of `1e30` → `$4\r\n1e+30\r\n`; of `1e15` → `$16\r\n1000000000000000\r\n`; of `1e16` → `$17\r\n10000000000000000\r\n`; of `1e17` → `$18\r\n100000000000000000\r\n`; of `0.1` → `$3\r\n0.1\r\n`; of `1e-7` → `$4\r\n1e-7\r\n`; of `0.000001` → `$8\r\n0.000001\r\n`; of `123456789012345678` → `$18\r\n123456789012345680\r\n`. Velo (`ZGroupReviewTempSpec`): `zscore a m` → `1.0`, `zscore a m2` (1e30) → `1.0E30`, `zincrby a 1 m` → `2.0`. Upstream `d2string` (`~/ws/redis/src/util.c:626-659`) is **not** `%.17g`: it prints integral values that fit in `long long` via `ll2string` (hence `1000000000000000` for `1e15`, *not* `1e+15`), and otherwise uses `fpconv_dtoa` (Grisu2 shortest round-trip) with Redis' own exponent/notation rules (`emit_digits` in `deps/fpconv/fpconv_dtoa.c:249-320`: plain integer when `K >= 0 && exp < ndigits + 7`, plain decimal when `K < 0 && (K > -7 || exp < 4)`, otherwise scientific with a **lower-case** `e` and a `+`/`-` sign). The report's proposed "shortest `%.{p}g` for p in 1..17" is close but **not equivalent**: `%.1g` of `1e15` is `1e+15` (Redis prints the full integer) and `%.1g` of `1e-7` is `1e-07` (Redis prints `1e-7`, one exponent digit). The fix must port `d2string` semantics, not approximate them. Scope note: `BulkReply(double)` (`BulkReply.java:54-56`) is the *only* double→bytes path and is used by ZGroup only (13 call sites, all zset score replies) — but it is a **shared** class, so changing it also changes any future caller; the same class already carries a `DoubleReply` RESP3 sibling (`DoubleReply.java`) that `d2string`-compatible formatting would supersede. |
| 87.8 | `CONFIRMED (reproduced)` — the **Redis-7.2 behaviour is the opposite of what the report assumes for ZPOPMIN/ZPOPMAX** | Redis 7.2.11 live raw wire: `zrandmember z1 0` → `*0\r\n` (empty array), `zpopmin z1 0` → `*0\r\n`, `zpopmax z1 0` → `*0\r\n`; `zrandmember z1 -1` → `*1\r\n$1\r\na\r\n` (repeats allowed); `zpopmin z1 -1` → `ERR value is out of range, must be positive`. Upstream `zpopMinMaxCommand` (`t_zset.c:4027-4044`) uses `getPositiveLongFromObjectOrReply` (so 0 is **allowed** and yields an empty array, negative is an error) and `zrandmemberCommand` accepts `count = 0` → empty array. Velo (`ZGroupReviewTempSpec`): `zrandmember a 0` → `ErrorReply{value is not an integer or out of range}`, `zpopmin a 0` → same, `zpopmax a 0` → same. **Correction:** the report says the `0` case is "Redis replies an empty array" for all three — that part is right — but it also asserts "the same convention covers … `ZPOPMIN key 0`, `ZPOPMAX key 0`" as if it were inferred; it is confirmed by live Redis and upstream source, so the fix direction (0 → empty array; negative → error for the pops; negative → repeats for `ZRANDMEMBER`) stands. The report's claim that `srandmember` "already returns EMPTY via the empty index list" is **not** verifiable in this module — `SGroup.srandmember` (`SGroup.java:2077-2160`) does not special-case `count == 0` either (it reaches `HGroup.getRandIndex(0, size, 0)` → empty index list → `members[0]` → **ArrayIndexOutOfBoundsException**). That is a separate `SGroup` bug and must not be folded into this commit. |
| 87.9 | `CONFIRMED (reproduced)` — with a **scope correction** | Redis 7.2.11 live: `zrank q1 withscore` (member literally named `withscore`) → `0` (integer, plain rank — the option is only recognised in the 4th position); `zrank q1 m junk` → `ERR syntax error`. Upstream `zrankGenericCommand` (`t_zset.c`): `if (c->argc > 4) addReplyErrorArity(c); if (c->argc > 3) { if (!strcasecmp(c->argv[3]->ptr,"withscore")) opt_withscore = 1; else { addReplyErrorObject(c, shared.syntaxerr); return; } }` — the option is **positional (argv[3])**, and the member is always argv[2]. Velo (`ZGroupReviewTempSpec`): `zrank a withscore` (member named `withscore`) → `MultiBulkReply` (should be an integer); `zrank a member0 junk` → `IntegerReply` (should be a syntax error). **Scope correction:** the report also says "`zrank a member0 junk` → Velo integer vs Redis syntax error" — confirmed; but the report's own checklist item 9 claims Velo's reply for `zrank a withscore` is an "array reply"; the actual Velo defect is that it treats the *member* argument as the option because it peeks at `data[data.length - 1]` (line 2200) — with a 3-argument call the last argument **is** the member. Both sub-cases are real; the fix must parse positionally (`data.length == 4 && data[3]` is the only place `withscore` may appear) and reject a 4-argument call whose 4th token is not `withscore`. Note the report's claim "the reply shape becomes `[rank, score]`/EMPTY instead of a plain rank integer" is right. |
| 87.10 | `CONFIRMED (reproduced)` — with a **mechanism correction** | Redis 7.2.11 live: `zadd n1 nan m` → `ERR value is not a valid float`; `zincrby n2 nan m` → same; `zunion 2 k7 k8 weights nan 1` → `ERR weight value is not a float`. Upstream `string2d` (`~/ws/redis/src/util.c`) rejects `isnan(*dp)`, and `getDoubleFromObjectOrReply` therefore answers *"value is not a valid float"* — matching Velo's `ErrorReply.NOT_FLOAT` text. **Mechanism correction (important):** the report claims "Java's `Double.parseDouble` accepts `NaN`/`nan` (case-insensitive)". That is **false** — `Double.parseDouble("nan")` throws `NumberFormatException` (only the exact spellings `NaN`, `+NaN`, `-NaN` parse to NaN; `nan`, `NAN`, `Inf`, `inf` all throw). Verified on the repo's JDK 21 (`parseDouble("nan")` → `NumberFormatException`; `parseDouble("NaN")` → `NaN`). Consequently Velo **already rejects** the lower-case `nan` — the temp spec shows `zadd b nan m` → `ErrorReply{value is not a valid float}` (Redis-compatible!) — but it **accepts** the exact-case `NaN`/`+NaN`/`-NaN`: `zadd a NaN m` → `IntegerReply` (accepted, member stored). The reproducible divergence is therefore the **exact-case** `NaN` spellings, not lower-case `nan`. Second correction: the report says "Redis explicitly rejects NaN … (infinity is *valid* for scores — do not reject that)". Redis accepts `inf`/`+inf`/`-inf` as a score (live: `zadd k4 inf m` → `1`, `zscore` → `inf`), but Velo's `Double.parseDouble("inf")` **throws** → `zadd a inf m` → `ErrorReply{value is not a valid float}` — so Velo rejects a score Redis accepts. That is the mirror-image divergence of the same parse site and belongs in the same fix (accept `inf`/`+inf`/`-inf`/`Infinity` case-insensitively as Redis does, reject `NaN`/`nan`/`NAN`/`+NaN`/`-NaN`). Also note the report's "Redis rejects NaN … weight" text is right but the Velo weights path (`ZGroup.java:1256-1261`) has the same `parseDouble` hole: `zunion 2 a a weights NaN 1` → accepted (live Redis → `ERR weight value is not a float`). |
| 87.11 | `CONFIRMED (reproduced)` — with a **trigger correction** | Velo (`ZGroupReviewTempSpec`): `zunionstore <300-char> 2 a a` → `IntegerReply` (success) and the 300-byte key **is** stored in the mock (`getBuf(...) != null` → `true`); `zrangestore <300-char> a 0 -1` → `IntegerReply`, key stored. Redis has no key-length limit at all, so the invariant here is Velo's own (`CompressedValue.KEY_MAX_LENGTH = 256`) — the same invariant `RESTORE` was fixed for in bug 85.5 (`c0c5a516`) and the same WAL-poisoning hazard applies: `Wal.V.decode` (`Wal.java:152`), `KeyBucket.getFromOneCell` (`KeyBucket.java:291`), `OneSlot` (`1829`, `1866`) and `SegmentBatch2` (`282`) all throw `IllegalStateException("Key length error, …")` for `keyLength > 256` — i.e. once such a key is written, every WAL-group read that hits it throws. **Trigger correction:** the report's checklist item 11 uses `zunionstore <257-char-key> 1 a`, but with `numkeys = 1` the command **fails earlier** with `INVALID_INTEGER` (87.6) and never reaches the store — the reproduction needs `numkeys = 2` (or the `zrangestore` form). The report's cited line ranges are right: `zdiff()` validates only the *source* keys (`ZGroup.java:1280-1283`) and the `zrangestore` destination (`data[1]`) is passed as a `String` into `zrange(dd, dstKey)` (`ZGroup.java:592-606`), which only length-checks `dd[1]` (the *source* key) at `1781-1784`. |

### 87.12 — NEW FINDING (raised by agent 2): `ZADD`'s `GT` / `LT` comparison is **inverted** — `GT` updates when the new score is *lower*, `LT` updates when the new score is *higher*

**Severity:** High
**Class:** Redis protocol compatibility / silent data corruption
**Status:** `OPEN — filed by agent 2 (not in the author's original list)`

### Cited files / lines

```java
// ZGroup.java:843-855 — inside zadd()'s per-member loop
if (!isNew) {
    if (isGt) {
        // gt
        if (svExist.score() <= member.score) {   // <-- skips when existing <= new
            continue;
        }
    } else if (isLt) {
        // lt
        if (svExist.score() >= member.score) {   // <-- skips when existing >= new
            continue;
        }
    }
}
```

### Root cause

Redis `zsetAdd` (`~/ws/redis/src/t_zset.c:1363-1560`, skiplist branch) gates the update with

```c
/* GT/LT? Only update if score is greater/less than current. */
if ((lt && score >= curscore) || (gt && score <= curscore)) {
    *out_flags |= ZADD_OUT_NOP;
    return 1;
}
```

i.e. **`GT` updates only when `new > current`, `LT` updates only when `new < current`** — the
documented semantics ("`GT`: Only update an existing member if the new score is greater than the
current score", redis.io/commands/zadd). Velo's conditions are the mirror image: `GT` skips when
`current <= new` (so it updates when `new < current`) and `LT` skips when `current >= new` (so it
updates when `new > current`). The two branches are exactly swapped.

This is not a cosmetic reply difference — the *stored score* is wrong, and the `CH` reply count is
wrong with it.

### Trigger / impact

Temp spec (`ZGroupReviewTempSpec7`, existing score 10 on member `m`):

```
case1 velo gt 5  (existing 10) -> score=5.0    [redis: 10, no update]
case2 velo gt 20 (existing 10) -> score=10.0   [redis: 20, updated]
case3 velo lt 20 (existing 10) -> score=20.0   [redis: 10, no update]
case4 velo lt 5  (existing 10) -> score=10.0   [redis: 5, updated]
case5 velo gt ch 20 (existing 10) -> score=10.0   [redis: reply 1, score 20]
case6 velo gt ch 5 (existing 10) -> score=5.0    [redis: reply 0, score 10]
case7 velo gt xx 20 (existing 10) -> score=10.0  [redis: 20, updated]
```

Upstream's own regression suite reproduces the same divergence (`~/ws/redis/tests/unit/type/zset.tcl:173-208`,
executed against Velo by the temp spec):

```
UPSTREAM GT: velo x=10.0 y=20.0 z=29.0   [redis: x=11, y=21, z=30]
UPSTREAM LT: velo x=11.0 y=21.0 z=30.0   [redis: x=10, y=20, z=29]
```

i.e. Velo performs exactly the updates Redis refuses and refuses exactly the updates Redis
performs. Practical damage: `ZADD key GT 5 member` (a "raise the floor" idiom) *lowers* the score,
and `ZADD key LT 5 member` (a "cap the score" idiom) *raises* it; the `CH` reply count is inverted
along with it.

### Suggested fix direction

Flip both comparisons to Redis' semantics: `GT` → skip when `svExist.score() >= member.score`;
`LT` → skip when `svExist.score() <= member.score`. Note the existing `ZGroupTest` assertions
(`'test zadd'`, lines ~330-372) encode the **inverted** behaviour and must be updated together with
the fix — e.g. `zadd a gt 0 member0 1 member1` currently expects `integer == 0` and the member's
score to change; under Redis semantics `gt 0` on an existing `0.1` member must be a no-op. The
`INCR`+`GT`/`LT` combination (87.2) needs care: Redis applies the increment **first** and then
compares the *resulting* score with the current one (`zadd ztmp gt incr 1 x` → nil, because
`current+1 > current` is false), while Velo compares the raw increment — that ordering must be
reproduced too, otherwise `87.2`'s fix and this one fight each other.

### Additional observations (not findings)

- **`ZRANDMEMBER` negative count is *not* the same shape as Redis'** — Redis repeats members
  (`zrandmember r1 -2` → 2 elements, possibly the same one twice), and Velo's
  `HGroup.getRandIndex` (`HGroup.java:2274-2293`) does implement repeats via
  `canUseSameField = count < 0`, so this part is fine. But `SGroup.srandmember` reaches
  `members[0]` with an empty array when `count == 0` — an `ArrayIndexOutOfBoundsException` waiting
  in the *set* module; out of scope for this report, but worth a follow-up id in the `SGroup`
  module (bug 86's module), not here.
- **`ZADD` accepts `INCR` together with `GT`/`LT` in Redis 7.2** and answers **nil** when the
  resulting score is vetoed (upstream `zset.tcl:253-283`). Velo's `INCR`+`GT` path
  (`ZGroup.java:843-863`) compares the *increment* against the *current* score and then always
  applies the increment — `zadd a incr gt 2 m` (existing 10) → Velo `12.0` (Redis `12`), but
  `zadd a incr gt -5 m` (existing 10) → Velo `5.0` (Redis: **nil**, score stays `10`). This is the
  same ordering defect as 87.12 and should be fixed together with it.
- The `ZGroupTest` suite has **no** assertion on the stored score after a `GT`/`LT` update, which is
  why the inverted comparison was never caught; the fix must add score assertions, not only reply
  assertions.
- `zrank`'s `WITHSCORE` miss must answer a **null array** (`*-1` / `_`), not `NilReply` (`$-1`) —
  see the 87.4 verdict; `MultiBulkReply.NULL` is the correct existing reply object.

### Recommended fix order (for the fix agent)

1. **87.12** (new, High) — flip the `GT`/`LT` comparisons in `zadd()`; fix the `INCR`+`GT`/`LT`
   ordering at the same time; update the inverted `'test zadd'` assertions and add score assertions.
2. **87.1** (High) — `ZADD INCR` reply type; use Redis' `processed` (not-NOP) predicate, not
   `added + changed`.
3. **87.3** (High) — `ZRANDMEMBER` without count → bulk string.
4. **87.4** (Medium) — `ZRANK`/`ZREVRANK` `WITHSCORE` miss → `MultiBulkReply.NULL` (null array),
   not `NilReply`, not `MultiBulkReply.EMPTY`.
5. **87.6** (Medium) — relax the `numkeys` guards to `numKeys < 1` in **both** `parseSlots` and the
   command bodies, and make `parseSlots` register the single key's slot; `numkeys = 0` must still
   error.
6. **87.2** (Medium) — add the three real mutual-exclusion errors; **do not** reject `INCR`+`GT`/`LT`.
7. **87.5** (Medium) — `-`/`+` sentinels for `ZLEXCOUNT` / `ZREMRANGEBYLEX`.
8. **87.7** (Medium) — port `d2string` semantics (integral→`ll2string`, else `fpconv_dtoa` rules),
   not a `%.{p}g` approximation.
9. **87.8** (Low) — `count == 0` → empty array for `ZRANDMEMBER`/`ZPOPMIN`/`ZPOPMAX`.
10. **87.10** (Low) — reject the exact-case `NaN` spellings and accept `inf`/`+inf`/`-inf`; same for
    the `WEIGHTS` parser.
11. **87.9** (Low) — positional `WITHSCORE` parsing in `zrank`/`zrevrank`.
12. **87.11** (Low) — length-check the `*STORE` / `ZRANGESTORE` destination key.

**Cross-module note for the fix agent:** 87.1, 87.4, 87.7 and 87.12 all change reply *shapes* in
`ZGroup`; `ZGroupTest` asserts the old shapes at `'test zadd'` (383-394), `'test zrank'`
(2394-2472), `'test zrandmember'` (1602-1710), `'test zincrby'` (1145-1195) and the `WITHSCORES`
assertions at 715, 1515, 1564, 1578, 1647, 2451, 2771, 2876-3036 — every one of them must be
updated in the same commit as its finding, and the corresponding `JaCoCo` line coverage re-checked.

## Fix Log (AI agent 1, round 1 fix phase)

Evidence key: each row shows RED (failing regression test, pasted reason) → commit → GREEN (focused
Spock method(s)) → JaCoCo (`scripts/jacoco_cover.py` on the changed lines). Fixes 87.12 → 87.4+87.9
were executed in the first fix session; fixes 87.6 → 87.11 in the continuation session (both followed
the same red-green-refactor cycle on branch `review/zgroup`).

| Bug | RED evidence (failing test first) | Commit | GREEN (focused) | JaCoCo on changed lines |
|---|---|---|---|---|
| 87.12 GT/LT inverted | `'test zadd'` rewritten: `zadd a gt 5 member0` (existing 10) expected no-update, Velo updated; upstream zset.tcl GT/LT suite expected x=11/21/30 — Velo 10/20/29 | `2088e0f7` | `'test zadd'` | zadd loop lines/branches covered incl. both GT/LT veto + update paths and INCR-before-compare |
| 87.7 score formatting | `'test zincrby'` `zincrby a 1 member0` expected `'1'`/`'2'` (Redis form), Velo replied `'1.0'`/`'2.0'` | `91be550d` (new `DoubleFormat` porting `d2string`, used by `BulkReply(double)`) | `'test zincrby'` + all WITHSCORES assertions updated repo-wide (7 files) | `DoubleFormat.format` fully covered incl. inf/nan/ll2string/Grisu branches |
| 87.1 ZADD INCR reply | `'test zadd'` INCR blocks expected `BulkReply('5')`/`NilReply`, Velo replied `IntegerReply` | `1d4f24ff` (uses the not-NOP `processed` predicate per the review correction, not `added+changed`) | `'test zadd'` | reply branch + incrProcessed/nil paths covered |
| 87.3 ZRANDMEMBER no-count | `'test zrandmember'` expected `BulkReply` member, Velo replied 1-element `MultiBulkReply` | `a02b5a87` | `'test zrandmember'` | `!hasCount → replies[0]` branch covered |
| 87.4 + 87.9 ZRANK WITHSCORE | `'test zrank'` expected integer for a member named `withscore` (Velo: array), `syntax error` for a junk 4th arg (Velo: integer), null array `MultiBulkReply.NULL` on miss (Velo: `*0`) | `bf91d854` (one commit, same method) | `'test zrank'` | positional parse + `MultiBulkReply.NULL` miss branches covered |
| 87.6 numkeys=1 rejected | `'test parse slot'`: numKeys=1 expected 1 registered slot, got `[[],[],[],[]]`; `'test zdiff'` `zdiff(false,false)` with numKeys=1 expected `MultiBulkReply`, got `ErrorReply{value is not an integer or out of range}`; `'test zintercard'` `zintercard 1 a limit 0` expected `IntegerReply(3)`, got the same error | `13f07524` (guards relaxed to `numKeys < 1` in parseSlots ×2 + zdiff/zintercard bodies; min-length guards `data.length < 3`/`< 4`; `zdiffstore` dd rebuild made length-safe; **plus** single-key WEIGHTS now scales the first set — `operateZset` weighting loop never ran for `otherRzList.isEmpty()`, caught by the new `zunionstore dst 1 a weights 2` assertion expecting scores ×2, Velo stored them unscaled) | `'test parse slot'`, `'test zdiff'`, `'test zintercard'` | parseSlots 373-441 34/34; zdiffstore wrapper + zdiff guard 23/23; zintercard guard; `operateZset` single-key weighting 6/6 incl. all 6 branches of the new condition |
| 87.2 ZADD option exclusivity | `'test zadd'`: `zadd a nx xx 2 member0` expected `ErrorReply{syntax error}`, got `IntegerReply(0)` (silent NX precedence) | `6df40d1c` (only the three upstream checks `nx&&xx`, `gt&&nx`, `lt&&nx`, `gt&&lt`; **no** INCR+GT/LT rejection — that sub-claim was REFUTED; regression guard added: `zadd a incr gt -5 member0` stays valid and replies nil) | `'test zadd'` | 791: 4/4 branches; 794: 12/12 branches |
| 87.5 lex sentinels | `'test zcount'`: `zlexcount a - +` expected `IntegerReply(10)`, got `ErrorReply{syntax error}`; `'test zremrangebyscore'`: `zremrangebylex - +` the same | `c47dfe89` (zrange() BYLEX normalization ported verbatim minus the 1-byte-bracket rejection, per review; shared `isLexRangeBefore` helper `@VisibleForTesting`; removal loop made sentinel-aware — `-`/`+` never match via raw `compareTo`, caught by the second RED: `- +` removal returned `REPLY_0` with nothing removed) | `'test zcount'`, `'test zremrangebyscore'` | helper all 5 condition branches; both parsers fully covered incl. `-`/`+` min/max spellings (`+ +`, `[member4 -` cases); removal loop 14/14 |
| 87.8 count 0 | `'test zpopmax'`: `zpopmax a 0` expected `MultiBulkReply.EMPTY`, got `ErrorReply{value is not an integer or out of range}`; `'test zrandmember'` the same | `5d91f65e` (0 → `MultiBulkReply.EMPTY`; negative still errors for the pops; note `EMPTY` is an anonymous `Reply`, so tests assert identity `== MultiBulkReply.EMPTY`) | `'test zpopmax'`, `'test zrandmember'` | both guard regions fully covered |
| 87.10 NaN/inf parsing | `'test zadd'`: `zadd a NaN member2` expected `INVALID_FLOAT`, got success; `zadd a inf member2` expected success, got `INVALID_FLOAT`; `'test zincrby'`/`'test zdiff'` weights the same shape | `3ca49a65` (new `parseScore` helper: accepts `inf/+inf/infinity/+infinity/-inf/-infinity` case-insensitively, rejects every NaN spelling; wired into zadd score, zincrby increment, WEIGHTS) | `'test zadd'`, `'test zincrby'`, `'test zdiff'` (weights inf case via the async path) | `parseScore` 9/9 lines, all spelling branches; three call sites fully covered |
| 87.11 dst key length | `'test zrange'`: `zrangestore >key a 0 -1` expected `KEY_TOO_LONG`, got success (key stored); `'test zdiff'`: `zunionstore >key 2 a b` the same | `bf0d527e` (one guard in `zdiff()` under `doStore` covering zdiffstore/zinterstore/zunionstore, one in handle()'s zrangestore branch; tests use the `>key` execute placeholder) | `'test zrange'`, `'test zdiff'` | both guards fully covered incl. the previously-never-covered handle() zrangestore happy path (line 610) |
| (repo hygiene) | — behavior-identical refactor requested during the session: `VALUE_NOT_POSITIVE` and `INVALID_INTEGER` carried the identical message text | `c1840e48` (all 9 production usages + 7 test assertions across ZGroup/HGroup/LGroup/BGroup/GGroup/SGroup merged onto `INVALID_INTEGER`, duplicate constant deleted; follows the `96c9a6b9` precedent) | `'test zpopmax'`, `'test zmpop'`, `'test srandmember'`, `'test geosearch and geosearchstore'`, `'test hrandfield'` — all green | no behavior change (same wire bytes) |

Full-class verification after all fixes: `./gradlew :test --tests "io.velo.command.ZGroupTest"` →
**32 tests, 0 failures, 0 errors** (2026-11-17T12:00:38Z).

## Residual Risk

- **SGroup.srandmember count==0** — the reviewer's evidence shows it reaches `members[0]` with an
  empty index list → `ArrayIndexOutOfBoundsException`. Explicitly out of this module's scope (file
  under the SGroup module, cf. bug 86); **not fixed here**.
- **Error-message drift, accepted per repo precedent**: 87.2's exclusions return the generic
  `syntax error` instead of Redis's distinct "XX and NX options … not compatible" texts;
  `zmpop`'s numkeys error text; `ZSCAN count <= 0` → SYNTAX (sibling inconsistency noted in round 1).
- **Lex sentinel micro-divergence**: `- -` (both bounds the min sentinel) proceeds and can match a
  literal member `"-"` where Redis answers 0 — mirrors the zrange() BYLEX behavior accepted in
  bug_43; kept for sibling consistency.
- **parseScore keeps Java's leniencies** (surrounding whitespace, `1.5f`/`1.5d` suffixes, hex
  floats) — pre-existing at these sites, out of 87.10's corrected scope. Range-bound parsers
  (`zrangebyscore` etc.) still use raw `parseDouble` behind the `±inf` constant checks — unchanged.
- **Lex bounds are lowercased** before comparison in the shared parsers (pre-existing design, same
  in zrange since bug_43) — case-sensitive member lexes near bounds can shift by case folding.
- **`ZUNIONSTORE d 1 missing`** replies 0 without writing an empty dst key (Redis deletes/creates
  the dst); pre-existing behavior for all empty store results, unchanged.
- **Environment flake observed** (not a code issue): the first `:compileTestGroovy` invocation after
  editing sources intermittently fails; an immediate re-run always succeeds. All reported GREEN
  results above are from successful re-runs with fresh test-result XML timestamps.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 87.1 | ZADD INCR replies integer count instead of bulk score / nil | High | Protocol | `CONFIRMED (reproduced)` — reply-semantics corrected (use Redis' `processed`/not-NOP predicate) | **fixed** — `1d4f24ff` |
| 87.2 | ZADD NX+XX / GT+LT / NX+GT accepted silently | Medium | Protocol | `PARTIALLY CONFIRMED` — the three exclusions confirmed; the `INCR`+`GT`/`LT` sub-claim `REFUTED` (Redis allows it) | **fixed** — `6df40d1c` (three exclusions only, INCR+GT/LT regression-guarded) |
| 87.3 | ZRANDMEMBER no-count form replies array not bulk | High | Protocol | `CONFIRMED (reproduced)` | **fixed** — `a02b5a87` |
| 87.4 | ZRANK/ZREVRANK WITHSCORE miss replies empty array not nil | Medium | Protocol | `CONFIRMED (reproduced)` — wire format corrected (Redis answers a null array `*-1`, not `$-1`) | **fixed** — `bf91d854` (`MultiBulkReply.NULL`) |
| 87.5 | ZLEXCOUNT / ZREMRANGEBYLEX reject `-`/`+` sentinels | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** — `c47dfe89` |
| 87.6 | numkeys=1 rejected across Z set-operation family | Medium | Protocol | `CONFIRMED (reproduced)` — bound corrected (`numKeys < 1`; `numkeys = 0` must still error) | **fixed** — `13f07524` (incl. the single-key WEIGHTS scaling hole found during the fix) |
| 87.7 | Score replies use Java double formatting ("1.0") not Redis form ("1") | Medium | Protocol (wire format) | `CONFIRMED (reproduced)` — formatter spec corrected (port `d2string`, not `%.{p}g`) | **fixed** — `91be550d` |
| 87.8 | ZRANDMEMBER/ZPOPMIN/ZPOPMAX count 0 errors instead of empty array | Low | Protocol (edge) | `CONFIRMED (reproduced)` | **fixed** — `5d91f65e` |
| 87.9 | ZRANK WITHSCORE positional misparse / junk 4th arg ignored | Low | Protocol (edge) | `CONFIRMED (reproduced)` — scope corrected (option is positional, argv[3]) | **fixed** — `bf91d854` |
| 87.10 | NaN accepted as score / increment / weight | Low | Edge case | `CONFIRMED (reproduced)` — mechanism corrected (`parseDouble` rejects lower-case `nan`; the hole is `NaN`/`+NaN`/`-NaN`, and Velo wrongly rejects `inf`) | **fixed** — `3ca49a65` |
| 87.11 | *STORE dst key length not validated | Low | Edge case | `CONFIRMED (reproduced)` — trigger corrected (needs `numkeys = 2`) | **fixed** — `bf0d527e` |
| 87.12 | **NEW (agent 2): `ZADD`'s `GT`/`LT` comparison is inverted — `GT` lowers the score, `LT` raises it** | High | Protocol / data corruption | `CONFIRMED (reproduced)` | **fixed** — `2088e0f7` |

All 12 findings fixed (12/12, no won't-fix). Extra commits produced during the fix phase:
`b273d43a` (cosmetic, prior session), `c1840e48` (error-reply constant merge).
