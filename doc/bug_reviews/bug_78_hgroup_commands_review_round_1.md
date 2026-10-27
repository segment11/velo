# Bug 78 — HGroup Hash Commands Review (Round 1)

**Module:** `io.velo.command.HGroup` — all H* commands (hdel/hexists/hexpire-family/hget/hgetall/hgetdel/hgetex/hincrby/hincrbyfloat/hkeys/hlen/hmget/hmset/hpersist-family/hrandfield/hscan/hset/hsetex/hsetnx/hstrlen/hvals) plus the two storage-layout helpers (`RedisHashKeys` split index, `RedisHH`) as consumed by HGroup.
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 14 findings, none verified yet
**Review branch:** `review/hgroup`
**Reviewed commit:** `19d3b14f955a9eff7c0ccd214c67808edcdd2d25`

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

## Finding 1 (Bug 78.1): split HINCRBY on a missing hash never creates the field index — orphaned invisible field

**Severity:** High
**Class:** Correctness
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
Make overwrite clear the field TTL on both paths, per Redis: HH — use the 3-arg `put(field, value, NO_EXPIRE)` (or explicit `putExpireAt(field, NO_EXPIRE)`) in `hmset2`/`hincrby2`; split HINCRBY — pass the "clear field TTL" intent explicitly (e.g. `decrBy(..., expireAtToClear)` or restore the CV write with `NO_EXPIRE`) so CV and cache move together; update the 2585 test to assert both CV and cache end at NO_EXPIRE.

## Finding 6 (Bug 78.6): HSETEX KEEPTTL clears the field TTL instead of retaining it (both layouts)

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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

(to be appended)

## Fix Log (AI agent 1, round 1 fix phase)

(to be appended)

## Re-review (AI agent 2)

(to be appended)

## Residual Risk

(to be appended after fix phase)

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 78.1 | split HINCRBY on missing hash never creates index | High | Correctness | pending | pending |
| 78.2 | HINCRBY delta int32 / MIN_VALUE sign flip | Medium | Protocol compat | pending | pending |
| 78.3 | HH HINCRBYFLOAT non-finite → raw NFE | Medium | Correctness | pending | pending |
| 78.4 | HH HINCRBY overflow wraps | Medium | Correctness | pending | pending |
| 78.5 | field TTL on overwrite: layouts diverge, HH HSET keeps TTL, split HINCRBY cache/CV split | High | Correctness | pending | pending |
| 78.6 | HSETEX KEEPTTL clears TTL | Medium | Protocol compat | pending | pending |
| 78.7 | HSETEX FNX/FXX per-field, partial writes | Medium | Protocol compat | pending | pending |
| 78.8 | split mode: no WRONGTYPE; HSET creates dual-representation key | Medium | Protocol compat | pending | pending |
| 78.9 | write paths use unfiltered index (expired fields "exist") | Medium | Correctness | pending | pending |
| 78.10 | HINCRBYFLOAT scale-2 rounding loses increments | Medium | Protocol compat | pending | pending |
| 78.11 | HEXPIRE-family: no negative/zero/past validation, reply 2 missing | Low | Protocol compat | pending | pending |
| 78.12 | numfields overflow → AIOOBE; 0/trailing args accepted | Low | Edge case | pending | pending |
| 78.13 | HSET/HMSET/HSETNX reject empty field/value | Low | Protocol compat | pending | pending |
| 78.14 | HRANDFIELD parsing + missing-key-with-count reply drift | Low | Protocol compat | pending | pending |
