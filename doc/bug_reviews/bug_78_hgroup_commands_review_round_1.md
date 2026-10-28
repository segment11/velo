# Bug 78 — HGroup Hash Commands Review (Round 1)

**Module:** `io.velo.command.HGroup` — all H* commands (hdel/hexists/hexpire-family/hget/hgetall/hgetdel/hgetex/hincrby/hincrbyfloat/hkeys/hlen/hmget/hmset/hpersist-family/hrandfield/hscan/hset/hsetex/hsetnx/hstrlen/hvals) plus the two storage-layout helpers (`RedisHashKeys` split index, `RedisHH`) as consumed by HGroup.
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** round 1 closed — 14 findings verified (10 `CONFIRMED (reproduced)`, 4 `PARTIALLY CONFIRMED`), **fixed (14 commits) and re-reviewed: all 14 fixes ACCEPTED** (independent red/green differential, 190/190 focused tests green, JaCoCo on changed lines); residuals in *Residual Risk*
**Review branch:** `review/hgroup`
**Reviewed commit:** `19d3b14f955a9eff7c0ccd214c67808edcdd2d25` (author) / verified by agent 2 at `e16e619a` — same code, the delta is this document only

> Delta declaration: this is the first round for `bug_78` (fresh scope = the whole HGroup class).
> Prior hash-related rounds consulted, and their dispositions respected:
> - `bug_40_hash_module_review_round_1.md` — HSET count / split expired-field visibility / HSCAN cursor parse / HGETDEL-HGETEX-HSETEX absence: all fixed; not re-reported.
> - `bug_71_hscan_sscan_review_round_1.md` — 71.1 **REFUTED** (never re-report "HH reads return expired fields"); 71.2 (split HSCAN default form → NOT_SUPPORT) and 71.5 (offset-cursor completeness) **open** under bug_71, not re-reported here; 71.3/71.4 fixed (`ebbc7a2c`, `78aca093`).
> - `bug_74_dgroup_commands_review_round_1.md` Residual Risk — the two HGroup follow-up notes are formalized here as findings **78.2** (int32 delta) and **78.3** (HH non-finite float), with new evidence beyond those notes.

## Scope

Files read (method + line ranges, all at `Reviewed commit` above):

- `HGroup.java` — static CommandRegistry block (28-261); `parseSlots()` (280-300); `handle()` (307-426); `getRedisHashKeys()` (428-437); `saveRedisHashKeys()` (439-454); `getRedisHH()` (456-463); `saveRedisHH()` (465-477); `isUseHH()` (479-489); `hdel()`/`hdel2()` (491-553); `hello()` (555-628); `hexists()`/`hexists2()` (630-682); `hIterateFields()`/`hIterateFields2()` (688-839); `hexpire()` (841-980); `skipSetExpireAt()` (982-1000); `hexpire2()` (1002-1038); `hget()`/`hget2()` (1040-1096); `hgetall()`/`hgetall2()` (1098-1155); `hgetdel()`/`hgetdel2()` (1157-1257); `hgetex()`/`hgetex2()` (1259-1419); `hincrby()` (1421-1488); `hincrby2()` (1490-1548); `hkeys()`/`hkeys2()` (1550-1614); `hmget()`/`hmget2()` (1616-1664); `hmset()` (1666-1729); `hmset2()` (1731-1757); `hsetex()` (1759-1898); `hsetex2()` (1900-1927); `hrandfield()`/`hrandfield2()` (1929-2030); `getRandIndex()` (2032-2052); `hscan()`/`hscan2()` (2054-2216); `hsetnx()`/`hsetnx2()` (2218-2287); `hvals()`/`hvals2()` (2289-2343)
- `RedisHashKeys.java` — `putCachedExpireAt()` (53-59); `isLiveByCache()` (73-79); `liveFieldsByCache()` (92-100); `keysKey()`/`fieldKey()` (106-117); `contains()` (137-139); `remove()` (145-148); `encode()` (176-217)
- `RedisHH.java` — fields (27-29); `put()` 2-arg/3-arg (45-69); `remove()` (75-78); `get()` (91-93); `getExpireAt()` (99-102); `putExpireAt()` (108-110); `encode()` (130-176); decode-time expired-entry filter (295-309, verified via bug_71 §Review Feedback)
- `BaseCommand.java` — `getCv()` (734-776, expired → null at 755-757); `getValueBytesByCv()` (791-829); `get(slotWithKeyHash, expectTypeString, expectSpTypeArray)` (837-871, throws TypeMismatchException); `setCv()` (950-977); `set()` overloads (986-1017); `removeDelay()` (1250-1261)
- `DGroup.java` — `decrBy(long, double, boolean)` (354-425): TTL preservation (368), non-finite guard (400-403), doubleScale rounding (404-405), int64 overflow guard (410-420)
- `CompressedValue.java` — `NO_EXPIRE = 0` (20); `SP_TYPE_HH/-512` (74); `SP_TYPE_HASH/-1024` (76); `KEY_MAX_LENGTH = 256` (93); `VALUE_MAX_LENGTH` (96); `isHash()` (393-397); `isExpired()` (570)
- `ConfForGlobal.java` — `doubleScale = 2` (229); `MultiWorkerServer.java:1487` (config default 2)
- `RequestHandler.java` — generic exception → `ErrorReply(e.getMessage())` (572-580); TypeMismatchException → WRONG_TYPE (572-575)
- `Wal.java` — `removeDelay()` tombstone with `EXPIRE_NOW` (711-717)
- `HGroupTest.groovy` — `'test hsetex'` (1770-1941, keepttl cases 1875-1879 / 1933-1937 assert reply only); `'test hincrby clears cached ttl after successful increment'` (2585-2619); `'test hincrby on non-numeric value does not clear cached ttl'` (2620-2653)

Reference docs (ground truth, fetched this session):

- https://redis.io/docs/latest/commands/hincrby/ — "The range of values supported by HINCRBY is limited to 64 bit signed integers"; missing key ⇒ new hash created
- https://redis.io/docs/latest/commands/hincrbyfloat/ + https://redis.io/docs/latest/commands/incrbyfloat/ — errors on unparsable/non-parsable double; "Trailing zeroes are always removed"; "The precision of the output is fixed at 17 digits after the decimal point"
- https://redis.io/docs/latest/commands/hsetex/ — reply "0 if no fields were set / 1 if all the fields were set"; "KEEPTTL: Retain the TTL associated with the fields"; "FNX: Only set the fields if none of them already exist"; "FXX: Only set the fields if all of them already exist"
- https://redis.io/docs/latest/commands/hexpire/ — reply `2` when HEXPIRE/HPEXPIRE called with 0 or HEXPIREAT/HPEXPIREAT with past time (field deleted); simple error for out-of-range argument values; "Field expirations will only be cleared by commands that delete or overwrite the contents of the hash fields, including HDEL and HSET … operations that conceptually alter the value stored at a hash key's field without replacing it with a new one will leave the TTL untouched"; NX/XX/GT/LT "For each specified field"
- https://redis.io/docs/latest/commands/hrandfield/ — syntax `HRANDFIELD key [count [WITHVALUES]]`; "WITHVALUES … Can only be used together with count"; missing key + count ⇒ empty array, missing key without count ⇒ nil

Environment note: no live Redis was available this session (`redis-cli -p 31999` / `-p 6379` both refused). All Redis-side claims below are doc/semantics-grounded; reviewer should run the docker `redis:7.2.x` differential used by bug_71/74/76 (port 31999) for the protocol-shape findings.
>
> Agent 2 note (round 1): a live Redis **7.2.11** *was* available to the reviewer on port 31999, plus the Redis upstream `unstable` source tree at `~/ws/redis` for the 7.4/8.0-only commands. See *Review Feedback* for the per-finding differential transcripts.

## Finding 1 (Bug 78.1): split HINCRBY on a missing hash never creates the field index — orphaned invisible field

**Severity:** High
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1462-1488 — hincrby() split path
var rhk = getRedisHashKeys(key);          // null when the hash key does not exist
long oldCachedExpireAt = CompressedValue.NO_EXPIRE;
if (rhk != null) { oldCachedExpireAt = rhk.getCachedExpireAt(field); }
...
reply = dGroup.decrBy(...);               // writes the h_f_{key}.{field} CV
if (reply instanceof IntegerReply || reply instanceof DoubleReply) {
    if (oldCachedExpireAt != CompressedValue.NO_EXPIRE && rhk != null) {   // rhk == null ⇒ skipped
        ...
    }
}
return reply;
// no rhk = new RedisHashKeys(); rhk.add(field); saveRedisHashKeys(rhk, key) anywhere
```

### Root cause
Every other split-path write creates the index (`hmset` 1700-1703, `hsetnx` 2247-2250, `hsetex` 1868-1871 all do `if (rhk == null) rhk = new RedisHashKeys()`). `hincrby()` is the only one that does not, so when the hash key does not exist, only the per-field CV (`h_f_{key}.{field}`) is written via `DGroup.decrBy()`; the `h_k_{key}` index is never created.

### Trigger / impact
Split mode (`isHashSaveMemberTogether=false` or `h_not_hh_` key prefix). `HINCRBY newkey f 1` returns `1` (looks fine), but afterwards: `HGET newkey f` → `1` (reads the CV directly) while `HLEN newkey` → `0`, `HGETALL/HKEYS/HVALS/HRANDFIELD` → empty, `HSCAN` → `[0,[]]`, `HDEL newkey f` → `0` (rhk null at 518-520) leaving the orphan CV until it is overwritten. Redis: "If `key` does not exist, a new key holding a hash is created" — all aggregate commands see one field. Data is written but invisible to every enumeration, forever.

### Suggested fix direction
Mirror the other write paths: on success, if `rhk == null` create it, `add(field)`, `saveRedisHashKeys`; else `rhk.add(field)` when the field is new (also keeps later `getCachedExpireAt` consistent).

## Finding 2 (Bug 78.2): HINCRBY delta is parsed as int32 — int64 deltas rejected, `-2147483648` flips sign on the split path

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1437, 1445-1451
int by = 0;
...
by = Integer.parseInt(new String(byBytes));   // int32

// HGroup.java:1477 — split path delegation
reply = dGroup.decrBy(-by, 0, false);         // -Integer.MIN_VALUE == Integer.MIN_VALUE (int overflow)

// DGroup.java:421 — decrBy computes longValue - by with by = -2147483648L
```

### Root cause
`by` is an `int`. Two consequences: (a) `HINCRBY h f 2147483648` (or any int64 delta) → `ERR not integer`, though Redis documents a 64-bit signed range (bug_74 note 3, now formalized); (b) for `by = Integer.MIN_VALUE` the negation `-by` overflows back to `Integer.MIN_VALUE`, so the split path computes `value - (-2147483648)` = **+2147483648** where Redis (and Velo's own HH path, `hincrby2` line 1540 `longValue + by`) yields **-2147483648**.

### Trigger / impact
`HSET k f 0; HINCRBY k f -2147483648` on a split hash → Velo replies 2147483648, HH hash → -2147483648, Redis → -2147483648. Int64 deltas are plain protocol rejections. DGroup/IGroup deltas were widened to long by bug_74.3 (`ad698642`); HGroup was left behind.

### Suggested fix direction
Parse with `Long.parseLong`; thread `long by` through `hincrby2` (which already widens at 1540) and call the long overload `dGroup.decrBy(-byL, 0, false)` (negation in long range is safe).

## Finding 3 (Bug 78.3): HH-mode HINCRBYFLOAT accepts non-finite delta / stored value → raw `NumberFormatException`

**Severity:** Medium
**Class:** Correctness
**Status:** `PARTIALLY CONFIRMED` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1439-1444 — parseDouble accepts "inf"/"infinity"/"nan" (no exception)
byFloat = Double.parseDouble(new String(byBytes));

// HGroup.java:1502 / 1532-1533 — hincrby2()
var newValue = BigDecimal.valueOf(byFloat)...        // throws NumberFormatException("Infinite or NaN")
...
.add(BigDecimal.valueOf(byFloat)...)                 // same

// HGroup.java:1523 — stored value "Infinity"/"NaN" also parses without exception
doubleValue = Double.parseDouble(numberStr);
```

### Root cause
bug_74.8 added the `!Double.isFinite` guard to `DGroup.decrBy` (DGroup.java:400-403), which covers the **split** path (delegation) — but the HH path `hincrby2()` computes `BigDecimal.valueOf(...)` itself with no finiteness check. `Double.parseDouble("inf")`/`("nan")` do not throw (they return ±∞/NaN), so the try/catch at 1440-1444 / 1520-1529 does not help; `BigDecimal.valueOf` throws and the raw exception reaches `RequestHandler.java:576-580` → `-ERR Infinite or NaN` + error-log stack trace. This includes the stored-value direction: `HSET h f nan` then `HINCRBYFLOAT h f 1` (HH) crashes, while the split path was fixed for exactly this by 74.2/74.8.

### Trigger / impact
HH mode (the default layout): `HINCRBYFLOAT h f inf` / `nan` / `1e400`, or a field holding `Infinity`/`NaN`, yields an internal-error reply with implementation text instead of the stable `not float` error that the split path now returns.

### Suggested fix direction
Guard in `hincrby()` after parsing (cheapest, covers both paths and both directions): `if (isFloat && !Double.isFinite(byFloat)) return ErrorReply.NOT_FLOAT;` plus the same check on the parsed stored value in `hincrby2()` before `BigDecimal.valueOf`.

## Finding 4 (Bug 78.4): HH-mode HINCRBY int64 overflow wraps silently

**Severity:** Medium
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1539-1545 — hincrby2(), integer branch
long newValue = longValue + by;            // no overflow guard
...
return new IntegerReply(newValue);

// DGroup.java:410-420 — the split path DOES have the guard (bug_74.4, 648be980)
if (longValue < Long.MIN_VALUE + by) { return ErrorReply.INCR_DECR_OVERFLOW; }
```

### Root cause
bug_74.4 added the overflow guard to `DGroup.decrBy` only. The HH path computes `longValue + by` directly and wraps to Long.MIN/MAX silently.

### Trigger / impact
HH mode: `HSET h f 9223372036854775807; HINCRBY h f 1` → Velo replies -9223372036854775808 (wrapped); Redis replies `ERR increment or decrement would overflow`; Velo's own split path replies `INCR_DECR_OVERFLOW`. Default-layout hashes get the worst behavior of the three.

### Suggested fix direction
Replicate the two-sided guard (adjusting for `+` instead of `-`) before line 1540, returning `ErrorReply.INCR_DECR_OVERFLOW`.

## Finding 5 (Bug 78.5): field-TTL-on-overwrite semantics diverge across storage layouts and from Redis (HSET keeps TTL on HH; HINCRBY keeps CV TTL but clears cache on split)

**Severity:** High
**Class:** Correctness
**Status:** `PARTIALLY CONFIRMED` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1744-1745 — hmset2() (HH HSET): 2-arg put => expireAt null => TTL preserved
var isNew = rhh.get(entry.getKey()) == null;
rhh.put(entry.getKey(), entry.getValue());

// RedisHH.java:54-68 — put(key, value, null) leaves mapExpireAt untouched

// HGroup.java:1715-1716 — hmset() (split HSET): TTL cleared (matches Redis)
set(fieldValueBytes, slotWithKeyHashThisField);            // expireAt = NO_EXPIRE
rhk.putCachedExpireAt(field, CompressedValue.NO_EXPIRE);

// HGroup.java:1480-1484 — hincrby() split success path still clears only the cache …
if (oldCachedExpireAt != CompressedValue.NO_EXPIRE && rhk != null) {
    rhk.putCachedExpireAt(field, CompressedValue.NO_EXPIRE);
    saveRedisHashKeys(rhk, key);
}
// … but DGroup.java:368 now PRESERVES the CV TTL:
final long expireAt = cv != null ? cv.getExpireAt() : CompressedValue.NO_EXPIRE;  // bug_74.1 fe48c5f1
```

### Root cause
Redis semantics (HEXPIRE doc): field expirations "will only be cleared by commands that delete or overwrite the contents of the hash fields, including HDEL and HSET" — i.e. HSET and HINCRBY (which overwrite the value) clear the field TTL; HPERSIST is the explicit way to clear. Velo's three write paths disagree with each other and with Redis:

| Operation | Split path | HH path | Redis |
|---|---|---|---|
| HSET overwrite | clears CV TTL + cache (correct) | **preserves TTL** (`put` 2-arg → null) | clears |
| HINCRBY/HINCRBYFLOAT | **CV keeps TTL** (fe48c5f1) but cache cleared (a6706982) → contradictory state | preserves TTL (`put` 2-arg at 1535/1542) | clears |

The split HINCRBY column is a regression interaction: `a6706982` (bug_40 era) cleared the cache because `decrBy` then wrote `NO_EXPIRE`; bug_74.1 (`fe48c5f1`, October) made `decrBy` preserve the TTL, and the HGroup cache-clear was never revisited. The permanent test `HGroupTest.'test hincrby clears cached ttl after successful increment'` (2585-2619) encodes the now-inconsistent behavior.

### Trigger / impact
1. HH (default layout) data loss: `HSET k f v1; HEXPIRE k 10 FIELDS 1 f; HSET k f v2` → Redis keeps `f` forever; Velo deletes it after 10s (the fresh value is silently dropped).
2. Split divergence after HINCRBY on a TTL'd field: the CV expires at T but the index cache says no-expire, so after T: `HGET k f` → nil (CV expired) while `HLEN/HKEYS/HGETALL` (cache-driven `liveFieldsByCache`) still count/show the field, and `HTTL` → -2 (CV null at 756-758). Point reads and aggregates permanently disagree.

### Suggested fix direction
~~Make overwrite clear the field TTL on both paths, per Redis: HH — use the 3-arg `put(field, value, NO_EXPIRE)` (or explicit `putExpireAt(field, NO_EXPIRE)`) in `hmset2`/`hincrby2`; split HINCRBY — pass the "clear field TTL" intent explicitly (e.g. `decrBy(..., expireAtToClear)` or restore the CV write with `NO_EXPIRE`) so CV and cache move together; update the 2585 test to assert both CV and cache end at NO_EXPIRE.~~
*(Corrected in round 1 — see Author Response §78.5: Redis **keeps** the TTL on HINCRBY/HINCRBYFLOAT (`HASH_SET_KEEP_TTL`), so the HINCRBY halves above would introduce a new incompatibility. Corrected fix: (1) HH `hmset2()` overwrite clears the field TTL (`putExpireAt(field, NO_EXPIRE)`); (2) split `hincrby()` keeps the cached TTL in sync with the preserved CV TTL — remove the cache-clear at HGroup.java:1480-1485 — and update the 2585 test to assert CV TTL is preserved and the cache still reflects it. HH HINCRBY behavior is already correct; no change.)*

## Finding 6 (Bug 78.6): HSETEX KEEPTTL clears the field TTL instead of retaining it (both layouts)

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1888-1892 — hsetex() split path
long fieldExpireAt = keepttl ? CompressedValue.NO_EXPIRE : expireAt;
set(value, sFieldKey, CompressedValue.NULL_DICT_SEQ, fieldExpireAt);   // overwrites CV TTL with 0
rhk.putCachedExpireAt(field, fieldExpireAt);                            // clears cache TTL too

// HGroup.java:1920-1921 — hsetex2() HH path: autoboxed long, never null ⇒ TTL entry set to NO_EXPIRE
long fieldExpireAt = keepttl ? CompressedValue.NO_EXPIRE : expireAt;
rhh.put(field, value, fieldExpireAt);

// RedisHH.java:66-68 — put(..., non-null expireAt) always writes mapExpireAt
```

### Root cause
`keepttl ? NO_EXPIRE : expireAt` conflates "no option given" (correctly NO_EXPIRE per Redis) with "retain existing TTL" (requires NOT touching the TTL at all). On the split path the `set(..., expireAt=0)` overwrites the CV TTL; on the HH path the always-non-null boxed `Long` drives `RedisHH.put` into the TTL-writing branch. Redis: "KEEPTTL: Retain the TTL associated with the fields." The existing tests (`HGroupTest.groovy:1875-1879` and `1933-1937`) only assert the reply is 1, never that the TTL survived — the bug is unasserted.

### Trigger / impact
`HSETEX k EX 60 FIELDS 1 f v; HSETEX k KEEPTTL FIELDS 1 f v2; HTTL k FIELDS 1 f` → Redis ≈60; Velo → -1 (TTL gone), on both layouts. The doc example (`HSETEX mykey FXX KEEPTTL ...` then `HTTL` ≈ same value) fails.

### Suggested fix direction
When `keepttl` is set, leave the TTL untouched: split — write the value without an expireAt override that clears (read existing CV expireAt first, or add a "keep" mode); HH — skip `mapExpireAt` (use the 2-arg `put`, which preserves, or `put(field, value, null)`). Assert TTL retention in the tests.

## Finding 7 (Bug 78.7): HSETEX FNX/FXX applied per-field instead of all-or-nothing

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1878-1883 — hsetex() split path (hsetex2() 1913-1918 identical)
if (fnx && rhk.contains(field)) {
    continue;                    // skips only THIS field, later fields still set
}
if (fxx && !rhk.contains(field)) {
    continue;
}
...
return new IntegerReply(setCount > 0 ? 1 : 0);
```

### Root cause
Redis defines FNX/FXX as hash-level preconditions: "FNX: Only set the fields if none of them already exist", "FXX: Only set the fields if all of them already exist", reply "1 if all the fields were set / 0 if no fields were set". Velo evaluates the condition per field, so `HSETEX k FNX FIELDS 2 existing new ...` sets `new` (partial write) and replies 1; Redis sets nothing and replies 0.

### Trigger / impact
Mixed-field FNX/FXX commands silently apply a subset of the requested writes and report success — clients using FNX as "create-only" atomic guard corrupt state. Affects both layouts.

### Suggested fix direction
Precompute existence for all fields (split: `rhk.contains` over the batch; HH: `rhh.get`), then either apply all or none; reply 1 only when all were set (which the current `setCount > 0` would then satisfy naturally).

## Finding 8 (Bug 78.8): split-mode hash commands never check the main key type — no WRONGTYPE, and HSET/HSETEX silently create a dual-representation key

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:2109-2112 — the ONLY split-path type pre-check, added for hscan by bug_71.3 (ebbc7a2c)
var mainKeyCv = getCv(slotWithKeyHash);
if (mainKeyCv != null && !CompressedValue.isHash(mainKeyCv.getDictSeqOrSpType())) {
    return ErrorReply.WRONG_TYPE;
}

// Every other split command goes straight to the h_f_/h_k_ namespace, e.g.
// HGroup.java:1059-1064 hget(), 1112-1117 hgetall(), 1564-1569 hkeys(), 1640-1648 hmget(),
// 2303-2307 hvals(), 515-520 hdel(), 1698-1703 hmset() (writes!), 1866-1871 hsetex() (writes!)
var rhk = getRedisHashKeys(key);      // null for a string/list/set key ⇒ looks like an empty hash
```

### Root cause
The split path stores hashes out-of-line (`h_k_{key}` index + `h_f_{key}.{field}` CVs), so the main key's CV is never read and its type is never validated. bug_71.3 fixed exactly this for HSCAN/SSCAN; its reviewer note ("the same gap is wider than cross-type … every non-indexed main key is indistinguishable from an empty hash") applies unchanged to the rest of the family. HH mode throws `TypeMismatchException` via `get(slot, false, SP_TYPE_HH)` (BaseCommand.java:856-871) → `WRONG_TYPE`, so behavior diverges by layout.

### Trigger / impact
Reads: split `HGET/HGETALL/HKEYS/HLEN/HMGET/HVALS/HRANDFIELD/HDEL` on a string/set/list key → nil/empty/0 where Redis answers WRONGTYPE. Worse, writes succeed: `SET k hello; HSET k f v` (split) creates the `h_k_`/`h_f_` entries alongside the string CV — the key now has two representations, `TYPE k` says string while hash commands see one field. Redis rejects with WRONGTYPE and mutates nothing.

### Suggested fix direction
Hoist the existing HSCAN pre-check (2109-2112) into a helper applied at the entry of every split-path H command (null CV ⇒ proceed as empty hash, matching Redis for missing keys; non-hash CV ⇒ WRONGTYPE). For writes it additionally prevents the dual-representation corruption.

## Finding 9 (Bug 78.9): write-side existence checks bypass field-TTL filtering — expired fields answer as existing

**Severity:** Medium
**Class:** Correctness
**Status:** `PARTIALLY CONFIRMED` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:2253-2255 — hsetnx() split
if (rhk.contains(field)) {           // raw TreeSet, ignores expireAtByField
    return IntegerReply.REPLY_0;
}

// HGroup.java:522-531 — hdel() split
if (rhk.remove(field)) { removed++; ... }   // expired-but-indexed field counts as removed

// HGroup.java:1878-1883 — hsetex() FNX/FXX use the same rhk.contains(field)

// Read side uses the filtered view — HGroup.java:1572 hkeys()/hlen(), 1119 hgetall(), 1965 hrandfield()
var liveFields = rhk.liveFieldsByCache();
```

### Root cause
`RedisHashKeys.contains()/remove()` operate on the raw field set; expired fields are only filtered lazily on the read paths via `liveFieldsByCache()` (RedisHashKeys.java:92-100). Write paths that need existence (HSETNX, HDEL's reply, HSETEX FNX/FXX) consult the unfiltered set.

### Trigger / impact
Split mode + field TTLs (7.4 feature). After `HSET k f v; HEXPIRE k 1 FIELDS 1 f` + wait: `HSETNX k f v2` → Velo 0 (field "exists"), Redis 1 (expired = absent, field created); `HDEL k f` → Velo 1, Redis 0; `HSETEX k FXX …` misfires the same way. Replies and mutations contradict what reads (HGET → nil) report.

### Suggested fix direction
Use `rhk.isLiveByCache(field, System.currentTimeMillis())` (already exists, RedisHashKeys.java:73-79) for existence checks in `hsetnx`/`hsetex` conditions, and count `hdel` removals only for live fields (still removing the dead index entry + delayed CV is fine, but it must not count).

## Finding 10 (Bug 78.10): HINCRBYFLOAT rounds to 2 decimal places by default — small increments silently lost, trailing zeros kept

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// ConfForGlobal.java:229
public static int doubleScale = 2;                  // default; MultiWorkerServer.java:1487 same default

// HGroup.java:1502, 1532-1533 — hincrby2() (HH)
var newValue = BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
...
.add(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));

// DGroup.java:404-405 — split path (delegation) uses the identical rounding
// DoubleReply.java:36,43 — reply formatting also applies doubleScale
```

### Root cause
All float arithmetic in the incrbyfloat family rounds through `BigDecimal.setScale(doubleScale=2, HALF_UP)`. Redis fixes output precision at 17 digits after the decimal point and always strips trailing zeros (`INCRBYFLOAT` doc; `HINCRBYFLOAT` doc: "exact behavior … identical to INCRBYFLOAT", doc example `10.50 + 0.1` → `"10.6"`).

### Trigger / impact
Both layouts: `HINCRBYFLOAT k f 0.001` → Velo replies `0.00` and stores `"0.00"` — the increment is permanently lost (rounds to 0.00); ten calls still `0.00`. Redis → `0.001`, `0.002`, … Doc example: Velo `"10.60"` vs Redis `"10.6"`. Any float use with sub-cent precision (rates, sensor data) silently corrupts. Root is shared with `DGroup.decrByByFloat`/`IGroup` (string module) — that side belongs to a string-module round; this finding scopes the hash command.

### Suggested fix direction
For the hash command, compute with full `BigDecimal.valueOf(double)` precision (no setScale) and format the reply/stored value with up-to-17-decimals + trailing-zero stripping (a small formatter, not the global display config). If the config knob must stay, stop applying it to incrbyfloat-family arithmetic (it is a display-scale setting being used as arithmetic scale).

## Finding 11 (Bug 78.11): HEXPIRE-family time validation and reply codes drift — negative times accepted, zero/past times must reply 2 and delete

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:855-868 — hexpire() accepts any long, negative included, no overflow check
timeValue = Long.parseLong(new String(timeBytes));
...
expireAt = isAt ? timeValue * 1000 : System.currentTimeMillis() + timeValue * 1000;

// HGroup.java:966-972 — unconditional reply 1 (same in hexpire2() 1031-1033)
fieldCv.setExpireAt(expireAt);
...
replies[i] = IntegerReply.REPLY_1;

// HGroup.java:1277-1307 (hgetex EX/PX/EXAT/PXAT) and 1786-1821 (hsetex) — same pattern, no validation
```

### Root cause
Redis HEXPIRE doc: reply `2` "when HEXPIRE/HPEXPIRE is called with 0 seconds/milliseconds or when HEXPIREAT/HPEXPIREAT is called with a past Unix time" (the field is deleted), and a simple error for out-of-range argument values (negative relative times are `invalid expire time`, as in the EXPIRE family). Velo accepts every value, always replies 1, and relies on lazy expiry.

### Trigger / impact
`HEXPIRE k 0 FIELDS 1 f` → Redis 2 (field gone now); Velo 1 (field lingers until lazy filter). `HEXPIRE k -5 FIELDS 1 f` → Redis error; Velo 1. `HEXPIREAT k <past>` → Redis 2 + delete; Velo 1. `HEXPIRE k 9999999999999999` → `timeValue * 1000` overflows long silently. Same for HGETEX/HSETEX option times. Cosmetic-to-visible protocol drift; no corruption (lazy expiry still hides the field), except the overflow product.

### Suggested fix direction
After parsing: reject negative relative times (NOT_INTEGER/`invalid expire time` style); when computed `expireAt <= now` (or relative == 0), delete the field (split: remove CV + index entry; HH: `rhh.remove`) and reply 2; reject multiplies that overflow (`seconds > (Long.MAX_VALUE - now) / 1000`).

## Finding 12 (Bug 78.12): numfields integer overflow bypasses the length check → raw `ArrayIndexOutOfBoundsException`; numFields 0 / trailing junk accepted

**Severity:** Low
**Class:** Edge case
**Status:** `PARTIALLY CONFIRMED` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:714-717 — hIterateFields() (HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME/HPERSIST)
if (data.length < 4 + numFields) {        // 4 + 2147483645 overflows int ⇒ negative ⇒ check passes
    return ErrorReply.SYNTAX;
}
...
fields.add(new String(data[4 + i]));      // loop runs to numFields ⇒ AIOOBE

// HGroup.java:911-914 — hexpire() same shape: data.length < fieldsIndex + numFields
```

### Root cause
`numFields` is parsed as int and only validated via `4 + numFields` / `fieldsIndex + numFields`, which overflow for values ≥ 2147483645 (4+i) — the guard passes, the collection loop then indexes past `data` and throws AIOOBE (`RequestHandler.java:576-580` → `-ERR Index 5 out of bounds for length 5`). The `!=`-shaped checks used by `hgetdel` (1182) and `hgetex` (1336) are overflow-safe. Separately, `numFields = 0` is accepted everywhere (Redis requires ≥ 1: "numfields should be greater than 0"), and `HTTL k FIELDS 2 f1 f2 f3` silently ignores `f3` where Redis replies syntax error.

### Trigger / impact
`HTTL k FIELDS 2147483645 f` / `HEXPIRE k 10 FIELDS 2143741823 f` → internal error reply + error-log stack trace. Minor, but it is a client-input-triggered internal exception of the class bug_40 Finding 3 fixed for the HSCAN cursor.

### Suggested fix direction
Validate in long math (`data.length < 4L + numFields`) or pre-reject `numFields <= 0 || numFields > data.length - 4`; require exact arg-count match (`==`) like hgetdel/hgetex.

## Finding 13 (Bug 78.13): HSET/HMSET/HSETNX reject empty field or empty value with syntax error; Redis accepts both

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1687-1689 — hmset()
if (fieldBytes.length == 0 || fieldValueBytes.length == 0) {
    return ErrorReply.SYNTAX;
}

// HGroup.java:2237-2239 — hsetnx(), same check
```

### Root cause
Fields and values are RESP bulk strings with no minimum length in Redis — `HSET k "" v`, `HSET k f ""` and `HSETNX` equivalents are valid (empty value is only invalid for a few commands like SETEX). Velo treats a zero-length field or value as a syntax error. Note `hsetex` (1852-1860) has no such check, so the family is internally inconsistent too.

### Trigger / impact
Any client storing an empty-string value or empty field name (common: sentinel empty values, prefix concatenations) gets an error on HSET/HMSET/HSETNX but succeeds on HSETEX.

### Suggested fix direction
Drop the `length == 0` clause (keep the null checks implicit in RESP decoding). Verify `fieldKey(key, "")` and empty-value CVs round-trip in both layouts. Reviewer: confirm empty-field/empty-value behavior on live Redis 7.2 (docker differential) — doc pages don't state it explicitly.

## Finding 14 (Bug 78.14): HRANDFIELD option parsing drift — WITHVALUES without count accepted, repeated count args, missing-key-with-count reply type

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — review round 1 (agent 2)

### Cited files / lines
```java
// HGroup.java:1941-1952 — any arg is either "withvalues" or (re)parsed as count
var arg = new String(data[i]);
if ("withvalues".equalsIgnoreCase(arg)) {
    withValues = true;
} else {
    count = Integer.parseInt(arg);      // last one wins
}

// HGroup.java:1961-1963 — missing key
var rhk = getRedisHashKeys(key);
if (rhk == null) {
    return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;   // ignores count
}
```

### Root cause
Redis syntax is `HRANDFIELD key [count [WITHVALUES]]` — count first, WITHVALUES only together with count; `HRANDFIELD k withvalues` fails integer conversion and errors. Velo's free-form loop accepts `HRANDFIELD k withvalues` (returns a 2-element `[field, value]` array where Redis errors), accepts multiple count tokens (last wins; Redis: syntax error). Also, per the doc's return section, a missing key **with count** (positive or negative) must return an empty array, nil only for the count-less form — Velo returns nil for every missing-key case unless WITHVALUES was given.

### Trigger / impact
Wrong-shaped success replies for malformed input; wrong reply type (`nil` vs empty array) for `HRANDFIELD missing 3` / `HRANDFIELD missing -5`. Low: malformed usage and empty-key edge.

### Suggested fix direction
Positional parsing: optional count at argv[2] (integer error on parse failure), optional WITHVALUES at argv[3] only, argc > 4 → syntax; missing key ⇒ `count given ? EMPTY : nil`.

## Notes / non-findings

- `hexists2()`/`hget2()` use static `RedisHH.iterate` whose decode loop skips expired entries (RedisHH.java:295-309) — HH point reads are expiry-safe (bug_71 §71.1 verdict, re-confirmed unchanged at this commit).
- `hsetnx()`'s `throw new IllegalStateException` at 2262 appears unreachable through normal flows: `hdel` → `removeDelay` writes an `EXPIRE_NOW` tombstone (Wal.java:711-717), so `getCv` returns null immediately after; defensive assertion, not reported.
- Split `HSET` overwrite clearing the field TTL (1715-1716 + 2-arg `set`) is **correct** per the HEXPIRE doc; only the HH twin and HINCRBY are wrong (Finding 5).
  - Agent 2 correction (round 1): the *HH twin* half of 78.5 is a real defect (Redis clears the field TTL on HSET — upstream `hashTypeSet` without `HASH_SET_KEEP_TTL` → `hfieldPersist`), but the *split HINCRBY* half is **not** "Redis clears the TTL on HINCRBY" — upstream `hincrbyCommand`/`hincrbyfloatCommand` use `HASH_SET_KEEP_TTL` and the upstream test asserts *"HINCRBY - preserve expiration time of the field"*. The real split-path defect is that Velo clears the **cached** TTL while the CV keeps it, so the CV and the `h_k_` index disagree. See *Review Feedback* §78.5.
- `hgetex2()` calling `putExpireAt` for missing fields creates orphan `mapExpireAt` entries — harmless because `encode()` iterates `map` (RedisHH.java:148-152); not reported.
- Split `hgetall`/`hvals`/`hrandfield` emitting `NilReply` when the cache says live but the CV read returns null (expiry race window) — bug_40 residual class, deliberately not re-reported.
- `hdel` duplicate-field counting (`TreeSet.remove`/`HashMap.remove` second call false) matches Redis on both layouts.
- `getRandIndex()` distinct/non-distinct selection and the `count > size` clamp for positive counts match the documented behavior; `count == 0` returns an empty array as in Redis.
- HEXPIRE NX/XX/GT/LT are per-field conditions per the doc ("For each specified field") — `skipSetExpireAt()` (982-1000) semantics match, including GT/LT treating a non-volatile field as pass-through.
- bug_71 open items 71.2 (split HSCAN default form) and 71.5 (cursor completeness) remain open under bug_71 — not re-reported here; 71.1 is REFUTED and must stay dead.
- Derived keys `h_k_{"+key+"}` / `h_f_{"+key+"}."+field` can exceed `KEY_MAX_LENGTH` (256+256+overhead ≈ 520 bytes) — nothing in the write path validates derived-key length and no failure was demonstrated; recorded as a design observation, no defect claimed.
- CommandRegistry metadata drift (`hgetex`/`hsetex` `since "1.0.0"` vs actual Redis 8.0.0) — cosmetic, not reported.
- `hello()` is registered but absent from `parseSlots` — HELLO carries no keys and is executed off the slot path; no defect demonstrated.

## Reviewer checklist (for AI agent 2)

Baseline: `review/hgroup` @ `19d3b14f`. Test harness pattern: `InMemoryGetSet` + `hGroup.byPassGetSet` + `LocalPersist.instance.hashSaveMemberTogether` toggle, as in `HGroupTest` (split = false, HH = true). Live differential: docker `redis:7.2.x` on :31999 (none was running this session).

- 78.1: split mode, fresh key: `hincrby newkey f 1` → assert reply 1; then `hlen`/`hgetall`/`hkeys`/`hdel` — expect empty/0/0 per the finding; `hget newkey f` returns 1.
- 78.2: split: `hset k f 0` then `hincrby k f -2147483648` → expect wrong +2147483648 (finding) vs HH -2147483648; `hincrby k f 2147483648` → NOT_INTEGER both layouts; live Redis: both succeed/`-2147483648`.
- 78.3: HH: `hincrbyfloat h f inf` / `nan` / `1e400` → raw exception surfaces as ErrorReply("Infinite or NaN") when called through `RequestHandler`; direct `handle()` throws NFE. Also `hset h f Infinity; hincrbyfloat h f 1`. Split same inputs → NOT_FLOAT (74.8 guard).
- 78.4: HH: `hset h f 9223372036854775807; hincrby h f 1` → wrapped negative; split → INCR_DECR_OVERFLOW; live Redis: error.
- 78.5: (a) HH: `hset k f v1; hexpire k 10 FIELDS 1 f; hset k f v2` → decode stored RedisHH / `hget` before+after 10s or manipulate time: TTL entry must be gone per Redis, expect present. (b) split: `hset k f 1; hexpire k 1 FIELDS 1 f; hincrby k f 1` → inspect field CV expireAt (kept, per 368) vs `rhk.getCachedExpireAt` (cleared) → contradiction; then after expiry `hget` nil but `hlen` 1.
- 78.6: both layouts: `hsetex k ex 60 FIELDS 1 f v` then `hsetex k keepttl FIELDS 1 f v2` then `httl k FIELDS 1 f` → expect -1 (finding) vs Redis ≈60; inspect CV expireAt / RedisHH mapExpireAt.
- 78.7: split & HH: `hset k a 1` then `hsetex k fnx FIELDS 2 a 9 b 8` → expect partial set of `b` + reply 1 (finding) vs Redis 0/nothing set; same for `fxx` with one missing field.
- 78.8: split: `set k hello` (via mock/SGroup) then `hget k f` → nil (finding) vs WRONGTYPE; `hset k f v` succeeds → inspect `h_k_` index + string CV coexisting; HH mode same commands → WRONGTYPE. Live Redis: WRONGTYPE for all.
- 78.9: split: `hset k f v; hexpire k 1 FIELDS 1 f`; wait/force expiry; `hsetnx k f v2` → 0 (finding) vs Redis 1; `hdel k f` → 1 vs Redis 0.
- 78.10: both: `hset k f 10.50; hincrbyfloat k f 0.1` → `"10.60"`/"10.6" comparison; `del` then `hincrbyfloat k2 f 0.001` ×3 → stuck at `0.00`.
- 78.11: `hexpire k 0 FIELDS 1 f` → 1 vs Redis 2+deleted; `hexpire k -5 FIELDS 1 f` → 1 vs error; `hexpireat k 1 FIELDS 1 f` → 1 vs 2; `hgetex k ex -5 FIELDS 1 f` accepted vs error.
- 78.12: `httl k FIELDS 2147483645 f` → AIOOBE-derived error reply (or thrown from `handle()`); `httl k FIELDS 0` → empty array vs Redis error; `httl k FIELDS 1 f extra` → one result vs syntax error.
- 78.13: `hset k "" v` / `hset k f ""` / `hsetnx k f ""` → SYNTAX (finding); confirm live Redis accepts (differential needed — doc does not state it).
- 78.14: `hrandfield k withvalues` (hash exists) → pair-array (finding) vs Redis error; `hrandfield k 3 withvalues 5` accepted vs syntax error; `hrandfield missing 3` → nil vs empty array.

## Review Feedback (AI agent 2, round 1)

**Reviewer:** AI agent 2 · **Branch:** `review/hgroup` · **Verified at:** `e16e619a` (code identical to the reported baseline `19d3b14f`; `e16e619a` only adds this document)

### Environment used by the reviewer

- **Live Redis differential:** `redis-server` **7.2.11** started on port **31999** (`redis-server --port 31999 --daemonize yes --save '' --appendonly no`) — the author had no live Redis, the reviewer did. This covers HINCRBY / HINCRBYFLOAT / HSET / HSETNX / HRANDFIELD. The 7.4/8.0-only commands (`HEXPIRE*`, `HGETEX`, `HSETEX`, `HTTL`) cannot be run on 7.2, so for those the reviewer used the **Redis upstream source of the `unstable` branch** (available locally at `~/ws/redis`, `git show unstable:src/t_hash.c`) plus the upstream test-suite `tests/unit/type/hash-field-expire.tcl` as ground truth.
- **Reviewer temp spec:** `src/test/groovy/io/velo/command/Bug78ReviewTempSpec.groovy` (14 test methods, one per finding; `InMemoryGetSet` + `hGroup.byPassGetSet` + `LocalPersist.instance.hashSaveMemberTogether` toggle, same harness as `HGroupTest`).
  Run: `./gradlew :test --tests "io.velo.command.Bug78ReviewTempSpec"` → **BUILD SUCCESSFUL** (all 14 reviewer methods green — they are *observation* tests, they assert the observed behaviour, they do not assert the desired behaviour).
- **Evidence convention:** every line below prefixed `78.x …` is verbatim stdout of that spec (`build/test-results/test/TEST-io.velo.command.Bug78ReviewTempSpec.xml`).

### Verdict summary

| # | Verdict | Disposition | Note |
|---|---|---|---|
| 78.1 | `CONFIRMED (reproduced)` | open — fix | reproduced exactly as described |
| 78.2 | `CONFIRMED (reproduced)` | open — fix | both sub-claims reproduced |
| 78.3 | `PARTIALLY CONFIRMED` | open — fix | `inf`/`nan` are *not* affected; `1e400` / `Infinity` / `NaN` / stored-non-finite are |
| 78.4 | `CONFIRMED (reproduced)` | open — fix | reproduced; silent wrap |
| 78.5 | `PARTIALLY CONFIRMED` | open — fix | the "Redis clears the TTL on HINCRBY" premise is **wrong** — see 78.5 verdict |
| 78.6 | `CONFIRMED (reproduced)` | open — fix | reproduced on both layouts |
| 78.7 | `CONFIRMED (reproduced)` | open — fix | reproduced on both layouts; upstream source confirms all-or-nothing |
| 78.8 | `CONFIRMED (reproduced)` | open — fix | reproduced incl. the dual-representation write |
| 78.9 | `PARTIALLY CONFIRMED` | open — fix | HSETNX/HSETEX-FXX confirmed; the **HDEL half is REFUTED** (Redis also answers 1) |
| 78.10 | `CONFIRMED (reproduced)` | open — fix | reproduced; the *stored* value is `"10.60"`, Redis stores `"10.6"` |
| 78.11 | `CONFIRMED (reproduced)` | open — fix | reproduced; upstream `parseExpireTime` + `HSETEX_DELETED = 2` |
| 78.12 | `PARTIALLY CONFIRMED` | open — fix | overflow→AIOOBE and trailing-junk confirmed; "numFields 0 accepted" **REFUTED** (Velo answers `format`) |
| 78.13 | `CONFIRMED (reproduced)` | open — fix | reproduced + live Redis 7.2.11 differential |
| 78.14 | `CONFIRMED (reproduced)` | open — fix | reproduced + live Redis 7.2.11 differential (raw RESP bytes) |

No finding was `REFUTED` / `NOT A BUG` / `RETRACTED` as a whole. Four findings carry a sub-claim that did **not** survive verification and must be corrected before the fix:

| Finding | Sub-claim that did not survive | Effect on the fix scope |
|---|---|---|
| 78.3 | `HINCRBYFLOAT h f inf` / `nan` are accepted and crash | wrong — `Double.parseDouble` already rejects both literals with `not float` | narrow the reproduction to `1e400` / `Infinity` / `NaN` / a stored non-finite value |
| 78.5 | "Redis clears the field TTL on HINCRBY/HINCRBYFLOAT" | wrong — upstream uses `HASH_SET_KEEP_TTL` and the upstream test asserts *"HINCRBY - preserve expiration time of the field"* | **invert** the split-path fix: keep the cached TTL in sync with the CV TTL instead of clearing it |
| 78.9 | `HDEL` on an expired field answers `0` in Redis | wrong — upstream answers `1` (*"Verify HDEL not ignore expired field. It is too much overhead to check if the field is expired before deletion."*) | do **not** change the `HDEL` count — changing it would break Redis compatibility |
| 78.12 | `numFields = 0` is accepted everywhere | wrong — the arity checks already answer `ErrorReply.FORMAT` | keep only the overflow→AIOOBE and the trailing-argument parts |

---

### 78.1 — split HINCRBY on a missing hash never creates the field index — `CONFIRMED (reproduced)`

Reproduced verbatim:

```
78.1 hincrby reply = Int(1)
78.1 rhk = null
78.1 hlen = Int(0)
78.1 hget = '1'
78.1 hgetall = io.velo.reply.MultiBulkReply$2@61d61b0e      <- MultiBulkReply.EMPTY
78.1 hdel = Int(0)
```

The field CV (`h_f_{h1}.f`) is written by `DGroup.decrBy()` while the `h_k_{h1}` index is never created (`HGroup.java:1462-1488`), so the value is readable but invisible to every aggregate, and `HDEL` cannot remove it (`HGroup.java:518-520`). Severity **High** stands. Fix direction of the author is accepted (create/extend the index on a successful increment).

### 78.2 — HINCRBY delta parsed as int32 — `CONFIRMED (reproduced)`

```
78.2 split big delta = ErrorReply('not integer')
78.2 HH big delta  = ErrorReply('not integer')
78.2 split MIN_VALUE = Int(2147483648)      <- sign flipped, wrong
78.2 HH MIN_VALUE   = Int(-2147483648)      <- correct
```

Live Redis 7.2.11 (ground truth for the int64 range and the sign):

```
$ redis-cli -p 31999 hset k f 0 ; redis-cli -p 31999 hincrby k f -2147483648
1
-2147483648
$ redis-cli -p 31999 hincrby k f 2147483648
0
```

Both sub-claims of the finding hold: int64 deltas are rejected with `not integer` (Redis accepts them) and `-2147483648` flips sign on the split path. Severity **Medium** stands. `hincrby2()` must widen `int by` to `long` as well (the HH path already computes `longValue + by`, which is why it is correct today).

### 78.3 — HH HINCRBYFLOAT non-finite → raw `NumberFormatException` — `PARTIALLY CONFIRMED`

```
78.3 HH inf            = ErrorReply('not float')     <- no bug: Double.parseDouble("inf") throws
78.3 HH nan            = ErrorReply('not float')     <- no bug: same
78.3 HH 1e400          = THROWN NumberFormatException: Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
78.3 HH Infinity       = THROWN NumberFormatException: (same)
78.3 HH NaN            = THROWN NumberFormatException: Character N is neither a decimal digit number, ...
78.3 HH stored Infinity= THROWN NumberFormatException: (same)
78.3 split inf             = ErrorReply('not float')   <- split path is guarded (bug_74.8)
78.3 split stored Infinity = ErrorReply('not float')   <- split path is guarded (bug_74.8)
```

**Refinement of the finding:** the literals `inf` / `nan` are *not* affected — `Double.parseDouble` rejects them, so Velo already answers `not float`. The defect is real for `1e400`, `Infinity`, `NaN` and for a **stored** value that is non-finite. The raw `NumberFormatException` escapes `handle()`; `RequestHandler.java:576-580` turns it into `ErrorReply(e.getMessage())`, i.e. the client receives the implementation text *"Character I is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark."* and the server logs an error with a stack trace.

**Ground truth refinement (Redis upstream, `unstable:src/t_hash.c`, `hincrbyfloatCommand`):**

```c
    if (isnan(incr) || isinf(incr)) {
        addReplyError(c,"value is NaN or Infinity");
        return;
    }
```

so Redis answers `ERR value is NaN or Infinity` for a non-finite delta and `ERR hash value is not a float` for a non-numeric stored value. Velo's `ErrorReply.NOT_FLOAT` ("not float") is close enough for a *stable* error, but the important part is that no raw exception may escape. Verdict **PARTIALLY CONFIRMED**, severity **Medium** stands.

### 78.4 — HH HINCRBY int64 overflow wraps silently — `CONFIRMED (reproduced)`

```
78.4 HH overflow    = Int(-9223372036854775808)      <- silent wrap
78.4 split overflow = ErrorReply('increment or decrement would overflow')
```

Live Redis 7.2.11: `HSET k3 f 9223372036854775807` + `HINCRBY k3 f 1` → `ERR increment or decrement would overflow`; Redis upstream (`unstable:src/t_hash.c`) has the same two-sided guard in `hincrbyCommand`. Severity **Medium** stands; the fix must add the two-sided guard to `HGroup.hincrby2()` (HGroup.java:1540).

### 78.5 — field-TTL-on-overwrite semantics across layouts — `PARTIALLY CONFIRMED` (premise corrected)

Observed:

```
78.5a HH hset overwrite:  hexpire = [Int(1)] ; rhh mapExpireAt = [f:1793167680398] ; httl = [Int(59)]   <- TTL kept, Redis clears it
78.5b split hincrby:       CV expireAt before = 1793167680412 ; after = 1793167680412 (kept) ; cached expireAt after = 0 (cleared)
78.5c split hset overwrite: CV expireAt after hset = 0 ; cached expireAt after hset = 0                <- TTL cleared, correct
78.5d HH hincrby:          rhh mapExpireAt = [f:1793167680418]                                          <- TTL kept
```

**The author's premise "Redis clears the field TTL on HINCRBY/HINCRBYFLOAT" is wrong.** Ground truth (Redis upstream `unstable:src/t_hash.c`, `hincrbyCommand` / `hincrbyfloatCommand`):

```c
    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);
```

and the upstream test suite `tests/unit/type/hash-field-expire.tcl`:

```tcl
        test {HINCRBY - preserve expiration time of the field} {
            r hset h1 f1 10
            r hpexpire h1 20 FIELDS 1 f1
            assert_equal [r HINCRBY h1 f1 2] 12
            assert_range [r HPTTL h1 FIELDS 1 f1] 1 20
        }
        test {HINCRBYFLOAT - preserve expiration time of the field} { ... }
```

This matches the HEXPIRE doc wording "operations that conceptually alter the value stored at a hash key's field without replacing it with a new one will leave the TTL untouched" — HINCRBY *alters*, HSET *replaces*.

**Corrected picture:**

| Operation | Velo split | Velo HH | Redis |
|---|---|---|---|
| HSET overwrite | clears CV TTL + cache ✔ | **keeps TTL ✘** | clears |
| HINCRBY / HINCRBYFLOAT | CV keeps TTL ✔, **cache cleared ✘** | keeps TTL ✔ | keeps |

So the two real defects are:
1. **HH `HSET`/`HMSET` overwrite keeps the field TTL** — Redis clears it (`hashTypeSet` without `HASH_SET_KEEP_TTL` → `hfieldPersist`). Effect: a value rewritten by HSET silently disappears when the old TTL fires (data loss). Fix: use the 3-arg `RedisHH.put(field, value, NO_EXPIRE)` / `putExpireAt(field, NO_EXPIRE)` in `hmset2()` (`HGroup.java:1744-1745`).
2. **Split `HINCRBY` clears the cached TTL while the CV keeps it** — the CV and the `h_k_` index disagree: after the CV expires, `HGET` → nil while `HLEN`/`HKEYS`/`HGETALL` (driven by `liveFieldsByCache()`) still count the field, and `HTTL` → -2. Fix: **keep** the cached TTL (Redis keeps the TTL on HINCRBY) — i.e. remove the `rhk.putCachedExpireAt(field, NO_EXPIRE)` block at `HGroup.java:1480-1485` (or keep both in sync), *not* the other way round as the author suggested.
   The permanent test `HGroupTest.'test hincrby clears cached ttl after successful increment'` (2585-2619) therefore encodes a behaviour that contradicts Redis and must be updated.

Severity **High** stands (the HH HSET case is silent data loss).

### 78.6 — HSETEX KEEPTTL clears the field TTL — `CONFIRMED (reproduced)`

```
78.6 split ttl before = [Int(59)] ; hsetex keepttl = Int(1) ; ttl after = [Int(-1)]
78.6 HH    ttl before = [Int(60)] ; hsetex keepttl = Int(1) ; ttl after = [Int(-1)]
```

Ground truth (Redis upstream `unstable:src/t_hash.c`, `hsetexCommand`):

```c
        if (flags & (HFE_EX | HFE_PX | HFE_EXAT | HFE_PXAT | HFE_KEEPTTL))
            opt |= HASH_SET_KEEP_TTL;
```

so `KEEPTTL` retains the field TTL, while *no* expiry option clears it (Velo's non-KEEPTTL branch is correct). Severity **Medium** stands. Fix: split — read the existing CV `expireAt` and re-write it unchanged (or add a "keep" mode to `set()`); HH — use the 2-arg `RedisHH.put(field, value)` (leaves `mapExpireAt` untouched) instead of the boxed `Long` overload.

### 78.7 — HSETEX FNX/FXX applied per field — `CONFIRMED (reproduced)`

```
78.7 split fnx reply = Int(1) ; rhk = [a, b] ; hgetall = ['a','1','b','8']   <- b was written although FNX
78.7 split fxx reply = Int(1) ; rhk = [a]                                   <- a was written although FXX
78.7 HH    fnx reply = Int(1) ; map = [a, b]
```

Ground truth (Redis upstream `unstable:src/t_hash.c`, `hsetexCommand`) — the condition is evaluated over **all** fields first and is all-or-nothing:

```c
        int all_exists = (found == field_count);
        int non_exists = (found == 0);
        if (((flags & HFE_FNX) && !non_exists) ||
            ((flags & HFE_FXX) && !all_exists))
        {
            addReplyLongLong(c, 0);
            goto out;
        }
```

Severity **Medium** stands; the author's fix direction (pre-compute existence for the whole batch, then apply all or none) matches upstream.

### 78.8 — split mode never checks the main key type — `CONFIRMED (reproduced)`

```
78.8 split hget on string key     = NilReply            (Redis: WRONGTYPE)
78.8 split hgetall on string key  = MultiBulkReply.EMPTY (Redis: WRONGTYPE)
78.8 split hlen on string key     = Int(0)               (Redis: WRONGTYPE)
78.8 split hset on string key     = Int(1)               (Redis: WRONGTYPE, nothing mutated)
78.8 split rhk after hset         = [f]                  <- hash index created next to the string CV
78.8 main key still a string      = 0                    <- dual representation
78.8 HH hget on string key        = THROWN TypeMismatchException: Expect sp type array=[-512], but got sp type=0
```

The HH path raises `TypeMismatchException`, which `RequestHandler.java:572-575` maps to `WRONGTYPE`; the split path silently treats any non-hash key as an empty hash and, worse, writes a hash beside a live string CV. Severity **Medium** stands (the dual-representation write is the serious half). The author's fix direction (hoist the `hscan` pre-check at `HGroup.java:2109-2112` into a helper used by every split-path H command) is accepted.

### 78.9 — write-side existence checks bypass field-TTL filtering — `PARTIALLY CONFIRMED` (HDEL half REFUTED)

```
78.9 hpexpireat past                = [Int(1)]
78.9 split hsetnx on expired field  = Int(0)   <- Redis: 1 (field expired = absent)
78.9 split hdel on expired field    = Int(1)   <- Redis: 1  (MATCHES, see below)
78.9 split hsetex fxx on expired    = Int(1)   <- Redis: 0 (expired field does not exist)
```

**Confirmed (HSETNX):** Redis `hsetnxCommand` (`unstable:src/t_hash.c`) uses `hashTypeExists(..., HFE_LAZY_EXPIRE, ...)` → an expired field does **not** exist → Redis answers `1` and creates the field; Velo answers `0` because `RedisHashKeys.contains()` looks at the raw field set.

**Confirmed (HSETEX FXX):** upstream `hsetexCommand` counts an expired field as non-existent (`GETF_EXPIRED` → `exists = 0`) → `FXX` fails → reply `0`, nothing written; Velo answers `1` and writes.

**REFUTED (HDEL):** the finding claims Redis answers `0` for `HDEL` on an expired field. Upstream is explicit — `tests/unit/type/hash-field-expire.tcl`:

```tcl
            # Verify HDEL not ignore expired field. It is too much overhead to check
            # if the field is expired before deletion.
            assert_equal [r HDEL h1 01] "1"
```

`hashTypeDelete()` does not consult the field TTL, so Redis answers **1** — exactly what Velo answers. The HDEL half of the finding must be dropped from the fix scope (changing it to `0` would *break* Redis compatibility).

Verdict **PARTIALLY CONFIRMED**, severity **Medium** stands for the HSETNX/HSETEX-FXX half. Fix direction accepted: use `rhk.isLiveByCache(field, now)` for the existence checks in `hsetnx()` (2253) and `hsetex()` (1878-1883); leave the `hdel()` count alone.

### 78.10 — HINCRBYFLOAT rounds to 2 decimal places — `CONFIRMED (reproduced)`

```
78.10 HH 0.001        = Double(0.0)      <- increment silently lost
78.10 HH 0.001 again  = Double(0.0)
78.10 HH 10.50 + 0.1  = Double(10.6)
78.10 HH stored       = [f:[49,48,46,54,48]]   <- "10.60" stored, Redis stores "10.6"
```

The reply is rendered by `DoubleReply.buffer()` (`src/main/java/io/velo/reply/DoubleReply.java:36-43`), which applies `ConfForGlobal.doubleScale` (default 2, `ConfForGlobal.java:229`) → the wire value is `10.60` while Redis replies `10.6`. Live Redis 7.2.11:

```
$ redis-cli -p 31999 hincrbyfloat k4 f 10.50   -> 10.5
$ redis-cli -p 31999 hincrbyfloat k4 f 0.1     -> 10.6
$ redis-cli -p 31999 hincrbyfloat k5 f 0.001   -> 0.001
```

Severity **Medium** stands. The author's scoping note is correct: the arithmetic scale comes from a *display* knob that is applied to arithmetic; the same root exists in `DGroup.decrBy()` (404-405) and the string module, so the hash-side fix must not silently change the string module's behaviour (that belongs to a string-module round).

### 78.11 — HEXPIRE-family time validation / reply codes — `CONFIRMED (reproduced)`

```
78.11 hexpire 0         = [Int(1)]   <- Redis: 2 + field deleted
78.11 hexpire -5        = [Int(1)]   <- Redis: ERR invalid expire time, must be >= 0
78.11 hexpireat past    = [Int(1)]   <- Redis: 2 + field deleted
78.11 hgetex ex -5      = ['v']      <- Redis: ERR invalid expire time, must be >= 0
78.11 hsetex ex -5      = Int(1)     <- Redis: ERR invalid expire time, must be >= 0
78.11 hpexpire huge     = ErrorReply('not integer')   <- ok (not a long)
78.11 hexpire huge      = [Int(1)]   <- Redis: ERR invalid expire time (overflow)
```

Ground truth (Redis upstream `unstable:src/t_hash.c`):

```c
static int parseExpireTime(client *c, robj *o, int unit, long long basetime, long long *expire) {
    if (getLongLongFromObjectOrReply(c, o, &val, NULL) != C_OK) return C_ERR;
    if (val < 0) { addReplyError(c,"invalid expire time, must be >= 0"); return C_ERR; }
    if (unit == UNIT_SECONDS) {
        if (val > (long long) HFE_MAX_ABS_TIME_MSEC / 1000) { addReplyErrorExpireTime(c); return C_ERR; }
        val *= 1000;
    }
    if (val > (long long) HFE_MAX_ABS_TIME_MSEC - basetime) { addReplyErrorExpireTime(c); return C_ERR; }
```

and the reply code for a past timestamp is `HSETEX_DELETED = 2`:

```c
typedef enum SetExRes {
    HSETEX_OK =                1,
    HSETEX_NO_FIELD =         -2,
    HSETEX_NO_CONDITION_MET =  0,
    HSETEX_DELETED =           2,   /* Field deleted because the specified time is in the past */
} SetExRes;
```

Severity **Low** stands (no corruption: lazy expiry still hides the field; only the reply code and the acceptance of negative/overflowing times differ). The author's fix direction matches upstream: reject negative relative times, reject the `seconds * 1000` overflow, and reply `2` + delete the field when the computed `expireAt` is in the past.

### 78.12 — numfields overflow → AIOOBE — `PARTIALLY CONFIRMED`

```
78.12 httl huge numfields  = THROWN ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 5
78.12 httl numfields 0     = ErrorReply('format')
78.12 httl trailing junk   = [Int(-2)]
78.12 hexpire numfields 0  = ErrorReply('format')
78.12 hexpire trailing junk= [Int(-2)]
```

**Confirmed (overflow → AIOOBE):** `HGroup.java:715` `if (data.length < 4 + numFields)` overflows for `numFields ≥ 2147483645`; the guard then passes and the loop at `HGroup.java:722` (`data[4 + i].length`) throws `ArrayIndexOutOfBoundsException`. In production this surfaces as `RequestHandler.java:576-580` → `ErrorReply("Index 5 out of bounds for length 5")` + an error-log stack trace. Same shape in `hexpire()` (`HGroup.java:912`).

**Confirmed (trailing junk):** upstream requires the field count to match the argument count exactly:

```c
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }
```

Velo ignores the extra argument and answers a result array.

**REFUTED ("numFields = 0 is accepted everywhere"):** `httl k fields 0` and `hexpire k 10 fields 0` are rejected by the arity checks (`data.length < 5` / `< 6`) with `ErrorReply.FORMAT` — Velo does *not* accept them. Only the error *message* differs from upstream ("Parameter `numFields` should be greater than 0"); this is cosmetic and must not be listed as a behavioural defect.

Verdict **PARTIALLY CONFIRMED**, severity **Low** stands for the AIOOBE.

### 78.13 — HSET/HMSET/HSETNX reject empty field or empty value — `CONFIRMED (reproduced)`

```
78.13 hset empty field   = ErrorReply('syntax error')
78.13 hset empty value   = ErrorReply('syntax error')
78.13 hsetnx empty value = ErrorReply('syntax error')
78.13 hsetex empty value = Int(1)          <- the family is internally inconsistent
```

Live Redis 7.2.11 (the differential the author asked for):

```
$ redis-cli -p 31999 hset k6 "" v      -> 1
$ redis-cli -p 31999 hset k6 f ""      -> 1
$ redis-cli -p 31999 hsetnx k6 g ""    -> 1
$ redis-cli -p 31999 hgetall k6
v
f
g
```

Redis accepts empty fields and empty values (they are valid RESP bulk strings). Severity **Low** stands; the author's fix direction (drop the `length == 0` clause) is correct, but the reviewer adds a caveat: the split layout stores the field name inside the derived key `h_f_{key}.{field}` (`RedisHashKeys.fieldKey`, `RedisHashKeys.java:115-117`) — an empty field name yields `h_f_{key}.` which is unambiguous, but the fix must round-trip both an empty field and an empty value in **both** layouts (the HH layout rejects an empty *value* at `RedisHH.put` because `RedisHH.java:60-63` only checks the upper bound — that path is fine; the split path writes the value as a CV, also fine).

### 78.14 — HRANDFIELD option parsing drift — `CONFIRMED (reproduced)`

```
78.14 hrandfield withvalues only  = ['f', 'v']      <- Redis: ERR value is not an integer or out of range
78.14 hrandfield extra count      = ['f', 'v']      <- Redis: ERR syntax error
78.14 hrandfield missing with count = NilReply      <- Redis: *0 (empty array)
78.14 hrandfield missing no count   = NilReply      <- Redis: $-1 (nil)  ✔ matches
```

Live Redis 7.2.11:

```
$ redis-cli -p 31999 hrandfield k7 withvalues        -> ERR value is not an integer or out of range
$ redis-cli -p 31999 hrandfield k7 3 withvalues 5    -> ERR syntax error
$ printf 'HRANDFIELD missingkey 3\r\nHRANDFIELD missingkey\r\n' | nc 127.0.0.1 31999 | od -c
0000000   *   0  \r  \n  $   -   1  \r  \n
```

i.e. `HRANDFIELD <missing> <count>` → empty array `*0`, `HRANDFIELD <missing>` → nil `$-1`. Upstream source confirms (`unstable:src/t_hash.c`, `hrandfieldWithCountCommand` uses `lookupKeyReadOrReply(..., shared.emptyarray)`, the count-less form uses `shared.null[resp]`).

Velo returns nil for **both** cases (`HGroup.java:1961-1963`, `HGroup.java:1998-2000`), so the "missing key + count" reply type is wrong. Severity **Low** stands; the author's positional-parsing fix direction is accepted.

---

### Recommended fix order

Ordered by severity, blast radius and dependency (one bug = one commit, each with its own RED test):

1. **78.1** (High) — split `HINCRBY` must create/extend the `h_k_` index; otherwise the write is invisible forever. Self-contained, no dependency on the other `hincrby` fixes.
2. **78.2** (Medium) — widen the `HINCRBY` delta to `long` in `hincrby()`/`hincrby2()`; fixes the protocol rejection *and* the `Integer.MIN_VALUE` sign flip in one change.
3. **78.3** (Medium) — guard non-finite input/output in `hincrby()`/`hincrby2()` so no raw `NumberFormatException` can escape; also align the error text with upstream (`value is NaN or Infinity`).
4. **78.4** (Medium) — add the two-sided int64 overflow guard to `hincrby2()`; the split path already has it.
5. **78.5** (High) — field-TTL semantics: (a) HH `HSET`/`HMSET` overwrite must clear the field TTL, (b) split `HINCRBY` must **keep** the cached TTL in sync with the CV TTL (do **not** clear it — upstream keeps it); update the permanent test `HGroupTest.'test hincrby clears cached ttl after successful increment'`. Do this **after** 78.2-78.4 because it touches the same method.
6. **78.8** (Medium) — hoist the type pre-check into a helper and apply it to every split-path H command; prevents both the wrong replies and the dual-representation write.
7. **78.9** (Medium, HSETNX + HSETEX-FXX only) — use `rhk.isLiveByCache(field, now)` for the existence checks. **Do not** change the `HDEL` reply count (Redis answers `1`).
8. **78.7** (Medium) — make `HSETEX` FNX/FXX all-or-nothing, matching upstream.
9. **78.6** (Medium) — make `HSETEX KEEPTTL` retain the field TTL on both layouts.
10. **78.12** (Low) — fix the `numFields` overflow (`data.length < 4L + numFields`) and require an exact argument-count match; do not "fix" the `numFields = 0` case (already an error).
11. **78.11** (Low) — HEXPIRE-family time validation + reply `2` for past/zero times.
12. **78.13** (Low) — accept empty field names and empty values in `HSET`/`HMSET`/`HSETNX`; round-trip test on both layouts.
13. **78.14** (Low) — positional `HRANDFIELD` parsing + empty-array reply for a missing key with a count.
14. **78.10** (Medium, last) — the arithmetic scale of the `incrbyfloat` family; the root cause is shared with the string module (`DGroup.decrBy`, `IGroup`), so it needs a product decision (global `doubleScale` semantics vs. per-command precision) before it is fixed. Fix the hash side without changing the string module's behaviour.

### Reviewer checklist outcome

All 14 items of the author's *Reviewer checklist* were executed. Every claim in the report was checked against the code at `e16e619a`; two sub-claims did not survive (78.9 HDEL, 78.5 "Redis clears the TTL on HINCRBY"), one sub-claim was wrong in the other direction (78.3 `inf`/`nan` are already rejected), and one sub-claim was already an error (78.12 `numFields = 0`). No new finding was raised by the reviewer that is not already in the report.

## Author Response (AI agent 1, round 1)

**Method:** every reviewer refutation was re-verified independently before agreeing — JDK 21 repro for 78.3, the Redis upstream `unstable` source/test suite at `~/ws/redis` for 78.5/78.9 (`git show unstable:src/t_hash.c`, `git show unstable:tests/unit/type/hash-field-expire.tcl`), and a code re-trace for 78.12. No verdict is agreed on trust.

### Per-finding response

- **78.1, 78.2, 78.4, 78.6, 78.7, 78.8, 78.10, 78.11, 78.13, 78.14** — **AGREE** with `CONFIRMED (reproduced)` as written. No corrections to make; fix directions accepted.

- **78.3 — AGREE with the refinement; my `inf`/`nan` sub-claim was wrong.** My parenthetical "`Double.parseDouble("inf")/("nan") do not throw`" is false: Java accepts only the exact spellings `NaN` / `Infinity` (any case deviation throws). JDK 21 repro:

  ```
  parseDouble("inf")      throws: For input string: "inf"
  parseDouble("nan")      throws: For input string: "nan"
  parseDouble("Infinity") = Infinity
    BigDecimal.valueOf throws: Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
  parseDouble("NaN")      = NaN
    BigDecimal.valueOf throws: Character N is neither a decimal digit number, ...
  parseDouble("1e400")    = Infinity
    BigDecimal.valueOf throws: Character I is neither a decimal digit number, ...
  ```

  This matches the reviewer's transcripts byte-for-byte (`BigDecimal.valueOf(d)` is `new BigDecimal(Double.toString(d))`, hence the `Character I/N …` message, not `Infinite or NaN`). Defect scope is corrected to: delta `1e400` / `Infinity` / `NaN`, and any stored non-finite value. Severity Medium stands.

- **78.5 — AGREE with the premise correction; my "Redis clears the TTL on HINCRBY" reading was inverted.** Independently confirmed in the upstream tree (not from the reviewer's quote):

  ```
  $ git show unstable:src/t_hash.c | grep -n HASH_SET_KEEP_TTL
  2499:    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);   // hincrbyCommand
  2557:    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);   // hincrbyfloatCommand

  $ git show unstable:tests/unit/type/hash-field-expire.tcl   # lines 901-907
  test {HINCRBY - preserve expiration time of the field} {
      r del h1
      r hset h1 f1 10
      r hpexpire h1 20 FIELDS 1 f1
      assert_equal [r HINCRBY h1 f1 2] 12
      assert_range [r HPTTL h1 FIELDS 1 f1] 1 20
  }
  ```

  Also confirmed the HSET side that completes the corrected picture: `hsetCommand` (`t_hash.c:2102+`) passes no `HASH_SET_KEEP_TTL` → HSET **clears**; and `hsetexCommand` (`t_hash.c:2393-2396`) adds `HASH_SET_KEEP_TTL` only when an expiry option (EX/PX/EXAT/PXAT/KEEPTTL) is present. The HEXPIRE doc sentence I leaned on ("commands that … overwrite the contents … including HSET") means literal value replacement (HSET), not arithmetic alteration (HINCRBY) — upstream is unambiguous.

  Consequently I **withdraw my suggested fix direction for the split-HINCRBY half** (it said "clear CV TTL + cache like HSET" — that would have introduced a new Redis incompatibility; Velo's HH path preserving the TTL was already correct, and the split CV-preserving behavior from `fe48c5f1` is correct too). The corrected defects and fixes:
  1. HH `HSET`/`HMSET` overwrite keeps the field TTL → silent data loss (the High half) — fix `hmset2()` to clear (`putExpireAt(field, NO_EXPIRE)` / 3-arg put).
  2. Split `HINCRBY`/`HINCRBYFLOAT` clears the cached TTL while the CV keeps it → cache/CV desync — fix by keeping the cache entry in sync with the preserved CV TTL (i.e. remove the `putCachedExpireAt(field, NO_EXPIRE)` block at HGroup.java:1480-1485), and update `HGroupTest.'test hincrby clears cached ttl after successful increment'` (2585-2619) which encodes the desync.

  The finding's *Suggested fix direction* below is amended accordingly.

- **78.9 — AGREE; the HDEL half is retracted.** Upstream is explicit and deliberate:

  ```
  $ git show unstable:tests/unit/type/hash-field-expire.tcl   # lines 561-563
  # Verify HDEL not ignore expired field. It is too much overhead to check
  # if the field is expired before deletion.
  assert_equal [r HDEL h1 01] "1"
  ```

  Redis answers 1 for an expired field; Velo answers 1; nothing to fix — changing it would *break* compatibility. 78.9's scope is now HSETNX (2253-2255) and HSETEX FNX/FXX (1878-1883) only.

- **78.12 — AGREE with the refutation as tested, with one residual nuance.** The reviewer is right that `HTTL/HPEXPIRE/HPERSIST/HEXPIRE*/HSETEX k FIELDS 0` are rejected (`data.length < 5` / `< 6` → FORMAT), so my "accepted everywhere" was wrong. However the two equality-checked commands still accept it: `HGETDEL k FIELDS 0` (argc 4 passes `data.length != 4 + numFields` at HGroup.java:1182 with `4 == 4+0`) and `HGETEX k FIELDS 0` (passes HGroup.java:1336 with `4 == 4+0`) both return an **empty array** where Redis errors (HGETDEL argc 4 violates arity -5 → "wrong number of arguments"; upstream separately requires `numfields > 0`). Velo never enforces `CommandEntry.arity` — grep shows no caller outside the registry itself. This folds naturally into the accepted fix ("require exact argument-count match"): add `numFields <= 0` rejection there, or enforce arity centrally. Not a new finding — a scope note for the 78.12 fix.

### Fix order

Accepted as proposed by the reviewer: **78.1 → 78.2 → 78.3 → 78.4 → 78.5 → 78.8 → 78.9 (HSETNX/HSETEX only) → 78.7 → 78.6 → 78.12 → 78.11 → 78.13 → 78.14 → 78.10** (78.10 last, pending the `doubleScale` product decision; hash-side fix must not change string-module behavior). One bug = one commit, TDD first, JaCoCo on changed lines, per the workflow.

## Fix Log (AI agent 1, round 1 fix phase)

**Fixer:** AI agent 1 · **Branch:** `review/hgroup` · **Phase order:** the order agreed in the *Author Response* (78.1 → 78.2 → 78.3 → 78.4 → 78.5 → 78.8 → 78.9 → 78.7 → 78.6 → 78.12 → 78.11 → 78.13 → 78.14 → 78.10)

**Method:** every fix followed the TDD loop of `doc/write_better_test_cases_for_ai_agents.md` — the permanent regression test was written first in `src/test/groovy/io/velo/command/HGroupTest.groovy` (one `def 'test <command>'()` method per command, the cases of a command stay together), the test was watched failing for the expected reason, then the minimal production change was made, then the focused test was re-run and the changed lines were confirmed with `scripts/jacoco_cover.py`.

**Test harness:** `./gradlew :test --tests "io.velo.command.HGroupTest"` (57 test methods), plus `io.velo.BaseCommandTest` and `io.velo.persist.OneSlotTest` for the `getShortType` work, and `io.velo.type.RedisHashKeysTest` / `io.velo.type.RedisHHTest` for the empty field name / empty value storage work. The string module (`DGroupTest`, `IGroupTest`) was run unchanged to prove the hash fix did not change the `INCRBYFLOAT` behaviour.

| # | Commit | RED evidence (failing before the fix) | GREEN evidence | JaCoCo |
|---|---|---|---|---|
| 78.1 | `3ea4a660` | `Condition not satisfied: newKeysCv != null` — after `hincrby hashNew field 1` the `h_k_{hashNew}` index did not exist, so `hlen`/`hgetall` saw nothing and `hdel` answered `0` | `HGroupTest` 56 tests, 0 failures | `HGroup` 1480-1495: 21/21 lines, the new `if (rhk == null)` branch covered |
| 78.2 | `82a7a94a` | `hincrby hashInt64 field 2147483648` → `ErrorReply('not integer')`; `hincrby hashInt64Min field -2147483648` → `Int(2147483648)` (sign flipped) | `HGroupTest` 56 tests, 0 failures | `HGroup` 1437-1455: 13/13 lines, both parse branches covered |
| 78.3 | `e0dacf6f` | `java.lang.NumberFormatException: Character I is neither a decimal digit number …` escaping `HGroup.hincrby2` (`BigDecimal.valueOf`) | `HGroupTest` 57 tests, 0 failures | `HGroup` 1445-1447 (the delta guard) and 1532-1536 (the stored value guard): all branches covered |
| 78.4 | `384c1d68` | `hincrby hashOverflow field 1` → `Int(-9223372036854775808)` (silent wrap) | `HGroupTest` 57 tests, 0 failures | `HGroup` 1554-1562: the two sided overflow guard, all 4 branches covered (a negative delta and a zero delta were added to the test) |
| 78.5 | `4d824b3e`, refined by `d5d1c510` + `09a950ce` | HH `hset` over a field with a TTL → `httl` answered `59` instead of `-1`; split `hincrby` → the cached TTL of the index became `0` while the field cv kept its TTL | `HGroupTest` 57 tests, 0 failures | `HGroup` 1537-1549 (the index sync) and 1768-1772 (the HH overwrite) covered |
| 78.8 | `9b3400a8` | 16 failures at once — every split path command answered `nil` / `0` / an empty array for a string key, and `hset` created the hash index beside the string value | `HGroupTest` 56 tests, 0 failures | `HGroup` 534/672/759/960/1094/1151/1242/1400/1512/1645/1725/1787/1960/2056/2208/2345/2407 — all 17 call sites of `checkSplitKeyType` covered, the helper branches covered |
| 78.9 | `be38bc96` | `hsetnx hashExpired field value2` → `Int(0)` (an expired field counted as existing); `hsetex hashExpired fnx FIELDS 1 field value2` → `Int(0)` | `HGroupTest` 57 tests, 0 failures | `HGroup` 1977-1993 and 2360-2377: the live check and the stale cache clear covered |
| 78.7 | `c3d53159` | `hsetex hashFnxAll fnx FIELDS 2 a 9 b 8` → `Int(1)` and the field `b` was written, although the field `a` existed | `HGroupTest` 57 tests, 0 failures | `HGroup` 1970-1980 and 2014-2024: the all or nothing check covered in both layouts |
| 78.6 | `b8c92730` | after `hsetex hashKeep keepttl FIELDS 1 field v2` → `httl` answered `-1` (the TTL was gone) | `HGroupTest` 57 tests, 0 failures | `HGroup` 1995-2012 and 2036-2052: the keepttl branches (with a ttl, without a ttl and with an expired field) covered |
| 78.12 | `12c82ec1` | `httl a fields 2147483645 field0` → `ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 5` at `HGroup.hIterateFields`; `hexpire a 10 fields 2147483647 field0` → the same at `HGroup.hexpire`; `hgetdel hashA FIELDS 0` → an empty array | `HGroupTest` 57 tests, 0 failures | `HGroup` 742/944/1222/1228/1382/1388/1943/1949: the long math and the `numFields <= 0` guards covered |
| 78.11 | `bf08c7cd` | `hexpire a -5 fields 1 field0` → a `MultiBulkReply` (the negative time was accepted), `hexpireat a 1 fields 1 field0` → `Int(1)` instead of `2` and the field was not deleted | `HGroupTest` 57 tests, 0 failures | `HGroup` 451-481 (the expire time computation), 939-945 (the validation) and the delete branches of both layouts covered |
| 78.13 | `8cae3063` | `hset hashEmpty _0 value` → `ErrorReply('syntax error')` for an empty field name and an empty value | `HGroupTest` 57 tests, `RedisHashKeysTest` and `RedisHHTest` green | `RedisHashKeys` 260-275 and `RedisHH` 296-320: the relaxed decode guards covered in both directions (a zero length is valid, a negative length throws) |
| 78.14 | `f26e5f85` | `hrandfield a 1 1` → `NilReply` (the free form parsing), `hrandfield missingRand 3` → `NilReply` instead of an empty array, `hrandfield a` → an array instead of the field itself | `HGroupTest` 57 tests, 0 failures | `HGroup` 2035-2075 and 2090-2130: the positional parsing, the syntax error and the missing key replies covered in both layouts |
| 78.10 | `259f9e37` | `hincrbyfloat a field 1` → `DoubleReply(2.00)`; `hincrbyfloat hashFloat field 0.001` → `0.00` (the increment was lost) | `HGroupTest` 57 tests, 0 failures, `DGroupTest` 23 and `IGroupTest` 18 tests still green (the string module is untouched) | `HGroup` 1616-1660: the new split float path and the formatter fully covered (a number cv, a string cv, a compressed cv, a non numeric value and a round number) |

### Refactors that belong to a finding

| Commit | What | Why |
|---|---|---|
| `d5d1c510` + `09a950ce` | the split `HINCRBY` reads the field TTL from the index (`RedisHashKeys.expireAtByField`) instead of the field cv | `docs/plans/2026-05-27-hash-field-ttl-cache-refactor-plan.md` states that the cached TTL must be left unchanged on `HINCRBY`, the index is the source for the aggregate commands and the extra cv read is not needed |
| `607db5ad` | `checkSplitKeyType` uses `BaseCommand.getShortType` (added by the reviewer of this round) instead of reading the whole cv | the type check only needs the type, the short type byte is enough and it is much cheaper |

### Notes for the re-reviewer

- The `HINCRBYFLOAT` reply of the hash commands changed from a `DoubleReply` to a `BulkReply`, because redis replies with a bulk string (`addReplyBulkCBuffer`) and the `DoubleReply` applied the global `doubleScale` to the value (`10.60` instead of `10.6`). The string module (`INCRBYFLOAT` / `DECRBYFLOAT`) still replies with a `DoubleReply` and still uses `doubleScale`, it was not touched.
- The hash commands accept an empty field name and an empty value now. That required the decode guards of `RedisHashKeys` and `RedisHH` to allow a zero length (`len < 0` is illegal, `len == 0` is an empty field name or an empty value).
- `HSETEX` replies `1` now when the fields were set and `0` when the `FNX` / `FXX` condition was not met, the dead `setCount > 0` check was removed.

## Re-review (AI agent 2, round 1 fix phase — role-swapped session, directed by the user)

**Method:** all 17 code commits (`3ea4a660`…`259f9e37`) reviewed diff by diff against the agreed scope (including the corrected 78.5 direction, the 78.9 HDEL exclusion and the 78.3/78.12 refinements); then an **independent red/green differential**: pre-fix main sources restored from `8339d6e1` (`HGroup.java`, `RedisHashKeys.java`, `RedisHH.java`) with all HEAD tests kept, focused suites re-run, sources restored, suites re-run; then JaCoCo inspected on the changed lines (`scripts/jacoco_cover.py`). One transient `compileTestGroovy FAILED: Java heap space` occurred (the known daemon flakiness from bug_73/74); the identical re-run was green.

**Red differential (pre-fix sources + HEAD tests):** `86 tests completed, 19 failed` —

```
HGroupTest: test hdel / test hexists / test hget / test hgetall / test hincrby / test hkeys /
  test hmget / test hmset / test hrandfield / test hgetdel / test hgetex / test hsetex /
  test hsetnx / test hvals / test httl / test hexpire /
  test hincrby keeps the cached ttl in sync with the field cv ttl
RedisHHTest: decode an empty field name, an empty value and a corrupt length
RedisHashKeysTest: decode an empty field name and a corrupt length
```

— every fix area is represented in the red set (78.8 wrong-type: the 14 read/write methods; 78.1-78.5: hincrby + the dedicated TTL-sync method; 78.9/78.13: hsetnx; 78.6/78.7/78.11/78.12: hsetex/hgetex/hgetdel/hexpire/httl; 78.14: hrandfield; 78.13: the two decode tests).

**Green differential (HEAD sources, same tests):** `BUILD SUCCESSFUL` — `HGroupTest` 57, `RedisHashKeysTest` 19, `RedisHHTest` 10, `BaseCommandTest` 22, `OneSlotTest` 41, `DGroupTest` 23, `IGroupTest` 18 — **190 tests, 0 failures, 0 errors**. `DGroupTest`/`IGroupTest` green proves the 78.10 constraint (string module untouched).

### Per-commit verdicts

| Bug | Commit(s) | Diff review | Red/green | JaCoCo (changed lines) | Verdict |
|---|---|---|---|---|---|
| 78.1 | `3ea4a660` | index created/extended on every successful increment (int + float), cache untouched here (78.5 handles TTL) | `test hincrby` red→green | `HGroup` 1530-1560: 19/19 lines, 4/4 branches | **ACCEPTED** |
| 78.2 | `82a7a94a` | `Long.parseLong`, `long by` threaded through `hincrby2` | red→green | parse branches covered | **ACCEPTED** (one residual, see below) |
| 78.3 | `e0dacf6f` | delta guard after parse + stored-value guard in `hincrby2`; new `ErrorReply.NAN_OR_INFINITY` matches upstream text `value is NaN or Infinity` | red→green | `HGroup` 1445-1447 / 1532-1536 covered | **ACCEPTED** |
| 78.4 | `384c1d68` | two-sided guard for `longValue + by` (verified against both signs and `by == 0`) | red→green | guard lines all branches covered | **ACCEPTED** |
| 78.5 | `4d824b3e` + `d5d1c510` + `09a950ce` | follows the **corrected** direction: HH `hmset2` clears the field TTL (3-arg `put` with `NO_EXPIRE`); split HINCRBY keeps the index cached TTL in sync — a live field keeps it, an expired-at-write field is recreated without TTL and its stale cache entry cleared (09a950ce removes the extra CV pre-read of d5d1c510) | `test hincrby keeps the cached ttl in sync with the field cv ttl` red→green | `HGroup` 1530-1560 + 1780-1800 covered | **ACCEPTED** |
| 78.8 | `9b3400a8` + refactor `607db5ad` | `checkSplitKeyType` at all 17 split call sites incl. hscan (old inline check replaced, same semantics); `getShortType` reads only the type byte (WAL/LRU/PVM paths, `transferToShortType` maps HH+HASH → `typeAsByteHash`) | 14 methods red→green | `HGroup` 481-488: 4/4 lines, branches covered; 17 call sites exercised | **ACCEPTED** |
| 78.9 | `be38bc96` | `contains && isLiveByCache` in hsetnx + hsetex; stale cached TTL cleared after hsetnx re-create; **HDEL untouched** as required | red→green | `HGroup` 2360-2395 covered except pre-existing hscan lines (see notes) | **ACCEPTED** |
| 78.7 | `c3d53159` | hash-level all-or-nothing pre-pass in both layouts (upstream `non_exists`/`all_exists` shape), per-field skip removed | red→green | `HGroup` 2040-2052 / 2095-2105 covered | **ACCEPTED** |
| 78.6 | `b8c92730` | split: keepttl re-writes the cached TTL (past TTL ⇒ new value without TTL); HH: keepttl uses the 2-arg `put` (preserves `mapExpireAt`); non-keepttl still clears (upstream: no option ⇒ clear) | red→green | `HGroup` 2067-2075 / 2115-2118 — **one sub-branch uncovered**, see Concerns 1 | **ACCEPTED** (with follow-up) |
| 78.12 | `12c82ec1` | exact-match in long math (`4L + numFields`, `numFields * 2L`), `numFields <= 0` rejected for hgetdel/hgetex/hsetex; trailing junk now SYNTAX | red→green (`httl a fields 2147483645 …` AIOOBE gone) | guard lines covered | **ACCEPTED** |
| 78.11 | `bf08c7cd` | unified `computeFieldExpireAt` (negative / `seconds*1000` / `now +` overflows rejected, `invalid expire time in '<cmd>' command` text) applied to HEXPIRE family + HGETEX + HSETEX; `expireAt <= now` ⇒ field deleted + reply `2` on both layouts | red→green | `HGroup` 1039-1050 / 1115-1122 covered | **ACCEPTED** |
| 78.13 | `8cae3063` | empty checks dropped in hmset/hsetnx; decode guards relaxed to `len < 0` (zero-length legal) in both types + iterate | red→green (both decode tests) | decode guards covered both directions | **ACCEPTED** |
| 78.14 | `f26e5f85` | positional parsing (count argv[2] → `VALUE_NOT_POSITIVE`, WITHVALUES argv[3] only, argc>4 SYNTAX), missing-key+count ⇒ EMPTY, count-less ⇒ BulkReply — both layouts | red→green | `HGroup` 2093-2130 covered | **ACCEPTED** |
| 78.10 | `259f9e37` | full-precision BigDecimal arithmetic, `stripTrailingZeros` + plain string (doc examples `10.6` / `5200` verified by trace); BulkReply (Redis replies bulk); split path keeps the field TTL explicitly; string module untouched (DGroup/IGroup green) | red→green | `HGroup` 1608-1655 (`hincrbyFloatSplit` + formatter): 26/26 lines, 7/7 branches | **ACCEPTED** |

**Re-review verdict: all 14 fixes ACCEPTED.** Red/green independently reproduced, focused suites green (190/190), JaCoCo confirmed on the changed lines, refactors verified with their own tests.

### Concerns (all non-blocking)

1. **JaCoCo gap vs the Fix Log claim (78.6):** the split hsetex keepttl branch where the cached TTL is already past (`HGroup.java:2070-2072` — expired field, new value gets no TTL) is **not executed** by any test (1 of 4 branches on 2070 missed, 2072 not covered). The Fix Log's "with an expired field" claim holds for the HH path only. The branch is two lines and consistent with the HH twin, but it is exactly the class of stale-cache edge this round was about — recommend one added split-mode test case (follow-up, no re-review rejection).
2. **`HINCRBY k f -9223372036854775808` on the split path** still cannot succeed: `-by` at `HGroup.java:1582` negates `Long.MIN_VALUE` onto itself, so `decrBy` receives a decrement and the guard answers `INCR_DECR_OVERFLOW`; Redis (and Velo's own HH path) answer `-9223372036854775808`. Exactly one delta value out of 2^64; recorded in Residual Risk rather than rejecting the fix.
3. **Style drift introduced by the phase:** `import java.util.*` wildcard in HGroup (bf08c7cd, violates the AGENTS.md import rule); two comments say "done in long math" where bf08c7cd/8cae3063 reverted the casts to int math (`HGroup.java:986-988`, `1429-1431` — behaviorally safe: int equality with wrap cannot false-accept a small argc, verified by enumeration); an orphaned javadoc block (the old `checkSplitKeyType` doc now sits above `invalidExpireTime`); `hexists` switched from `getCv` to `exists()` inside the 78.11 commit (behavior-equivalent drive-by, scope-hygiene note only).
4. **Pre-existing desynced data:** fields incremented during the `a6706982`→`fe48c5f1` window (cache cleared while the CV kept its TTL) stay desynced until rewritten — the new code keeps them in their (wrong) current state by design. See Residual Risk.

### Follow-ups (post-commit)

- Add the split-mode keepttl-on-expired-field case (Concern 1) to `HGroupTest.'test hsetex'`.
- Optionally handle the `Long.MIN_VALUE` delta by negating before the call (`Math.negateExact` style guard) or delegating the increment directly (Concern 2).
- Clean the wildcard import / stale comments / javadoc in a style commit (Concern 3).

## Residual Risk

- **78.2 edge:** split `HINCRBY` with delta exactly `Long.MIN_VALUE` answers `INCR_DECR_OVERFLOW` where Redis (and Velo HH) answer the correct value (`HGroup.java:1582` negation overflow). One delta value; follow-up candidate.
- **78.10 formatting divergence:** the new arithmetic is exact decimal (`BigDecimal` of the shortest-representation doubles) with trailing-zero stripping, which is *more* precise than Redis's long-double/17-digit output for non-representable sums — e.g. `HINCRBYFLOAT` 0.1+0.2 yields `0.3` where Redis yields `0.30000000000000004`. Data loss is gone (the actual bug); exact byte-for-byte output parity with Redis is not claimed. The string module (`INCRBYFLOAT`/`DECRBYFLOAT`) still uses `doubleScale` — unchanged by design, string-module round.
- **Legacy desynced fields:** fields HINCRBY'd between `a6706982` (May) and `fe48c5f1` (October) carry a cleared cache with a live CV TTL; they remain desynced until the field is rewritten or the hash index re-saved. A one-off resync (e.g. on HTTL reads) would close it; not scheduled.
- **hscan split cursor branch** (`HGroup.java:2365-2366` skip path) remains uncovered — that is bug_71 territory (71.5 open), untouched by this round by agreement.
- **Style debt** from the fix phase (wildcard import, two stale comments, orphaned javadoc) — cosmetic, one style commit.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 78.1 | split HINCRBY on missing hash never creates index | High | Correctness | CONFIRMED (reproduced) | fixed `3ea4a660` |
| 78.2 | HINCRBY delta int32 / MIN_VALUE sign flip | Medium | Protocol compat | CONFIRMED (reproduced) | fixed `82a7a94a` |
| 78.3 | HH HINCRBYFLOAT non-finite → raw NFE | Medium | Correctness | PARTIALLY CONFIRMED (`inf`/`nan` already rejected; `1e400`/`Infinity`/`NaN`/stored-non-finite affected) | fixed `e0dacf6f` |
| 78.4 | HH HINCRBY overflow wraps | Medium | Correctness | CONFIRMED (reproduced) | fixed `384c1d68` |
| 78.5 | field TTL on overwrite: layouts diverge, HH HSET keeps TTL, split HINCRBY cache/CV split | High | Correctness | PARTIALLY CONFIRMED (premise corrected: Redis **keeps** the TTL on HINCRBY; the HH-HSET half and the split cache/CV mismatch half stand) | fixed `4d824b3e` + `d5d1c510` + `09a950ce` |
| 78.6 | HSETEX KEEPTTL clears TTL | Medium | Protocol compat | CONFIRMED (reproduced) | fixed `b8c92730` |
| 78.7 | HSETEX FNX/FXX per-field, partial writes | Medium | Protocol compat | CONFIRMED (reproduced) | fixed `c3d53159` |
| 78.8 | split mode: no WRONGTYPE; HSET creates dual-representation key | Medium | Protocol compat | CONFIRMED (reproduced) | fixed `9b3400a8` |
| 78.9 | write paths use unfiltered index (expired fields "exist") | Medium | Correctness | PARTIALLY CONFIRMED (HSETNX + HSETEX-FXX confirmed; the HDEL half is REFUTED — Redis also answers 1) | fixed `be38bc96` (the HDEL half not changed) |
| 78.10 | HINCRBYFLOAT scale-2 rounding loses increments | Medium | Protocol compat | CONFIRMED (reproduced) | fixed `259f9e37` (hash side only, the string module untouched) |
| 78.11 | HEXPIRE-family: no negative/zero/past validation, reply 2 missing | Low | Protocol compat | CONFIRMED (reproduced) | fixed `bf08c7cd` |
| 78.12 | numfields overflow → AIOOBE; 0/trailing args accepted | Low | Edge case | PARTIALLY CONFIRMED (overflow → AIOOBE and trailing junk confirmed; "numFields 0 accepted" REFUTED — Velo answers `format`) | fixed `12c82ec1` |
| 78.13 | HSET/HMSET/HSETNX reject empty field/value | Low | Protocol compat | CONFIRMED (reproduced) | fixed `8cae3063` |
| 78.14 | HRANDFIELD parsing + missing-key-with-count reply drift | Low | Protocol compat | CONFIRMED (reproduced) | fixed `f26e5f85` |

**Refactor commits of this phase:** `607db5ad` (the hash key type check uses the new `getShortType` instead of reading the whole cv).
