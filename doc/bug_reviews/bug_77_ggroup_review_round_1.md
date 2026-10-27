# Bug 77 — GGroup Commands Review (Round 1)

**Module:** `io.velo.command.GGroup` (getbit/getdel/getex/getrange/getset/geoadd/geodist/geohash/geopos/geosearch/geosearchstore) with support classes `io.velo.BaseCommand`, `io.velo.type.RedisGeo`, `io.velo.reply.BulkReply`
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete (AI agent 1) — 9/10 fixed in 9 commits, 77.8 won't fix (product decision); re-review pending (AI agent 2)
**Review branch:** `review/ggroup`
**Reviewed commit:** `f95ce1ac8e44d585585127b5ac37f394c8eaf549`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/GGroup.java — static registry block (26-139); parseSlots() (159-194); handle() (202-228); getbit() (230-266); getdel() (268-281); getex() (283-362); getrange() (366-397); getset() (399-417); geo() (419-479); saveRedisGeo() (493-501); geoadd() (503-589); geodist() (591-623); geohash()/geopos() (625-657); geosearch() (662-1007)`
- `src/main/java/io/velo/BaseCommand.java — getCv() (734-776); getValueBytesByCv() (791-829); get() (837-868); setCv() (950-977); set() (1027-1179); putToOneSlot() (1207, 1215); removeDelay() (1250)`
- `src/main/java/io/velo/type/RedisGeo.java — Unit (24-77); add() (117-127); distance() (146-161); isWithinBox() (179-199); geohashEncode() (221-243); hashAsStore() (283-286); hash() (292-307); encode()/decode() (316-410)`
- `src/main/java/io/velo/reply/BulkReply.java — BulkReply(double) (61-63); buffer() (160-174)`
- `src/main/java/io/velo/RequestHandler.java — GET shortcut (474-516); generic dispatch + TypeMismatchException → WRONG_TYPE (556-580)`
- `src/main/java/io/velo/command/EGroup.java — expire() (241-329, overflow guards 279-288)`
- `src/main/java/io/velo/type/RedisBitSet.java — get() (79-89)`
- `src/main/java/io/velo/CompressedValue.java — SP types (76-96); isExpired() (569-571); isCompressed() (587-589); isTypeString() (606-609)`
- `src/test/groovy/io/velo/command/GGroupTest.groovy — all test methods (19-806)`
- `doc/design/04_command_processing_design.md — Slot Parsing / Dispatch Model / Reply Encoding sections`

Reference docs (ground truth):

- Redis 7.2 source `src/geo.c` (fetched, full): `https://raw.githubusercontent.com/redis/redis/7.2/src/geo.c`
- Redis 7.2 source `src/object.c` `getDoubleFromObject()` + `src/util.c` `string2d()` (fetched): NaN rejected via `isnan(*dp)`
- `https://redis.io/docs/latest/commands/getdel/`, `.../getex/`, `.../getrange/`, `.../getset/`, `.../geoadd/`, `.../geodist/`, `.../geohash/`, `.../geopos/`, `.../geosearch/`, `.../geosearchstore/`

Prior rounds checked (not re-reported):

- `doc/bug_reviews/bug_44_geo_commands_review_round_1.md` — STOREDIST/WITH* mode split, mandatory BYRADIUS/BYBOX unit, COUNT n ANY, duplicate ASC/DESC, FROMLONLAT bounds+NaN, positive radius/box: all fixed in current code, verified present at current lines (719-829, 749-754) — not re-reported
- `doc/bug_reviews/bug_39_string_commands_review_round_1.md` — GET fast path verified correct (still is, RequestHandler.java:503 uses `expectTypeString=true`); no refuted GGroup items to avoid

## Finding 1 (Bug 77.1): GETDEL/GETRANGE/GETSET/GETEX (and GETBIT — reviewer extension accepted) skip the string-type check — WRONGTYPE missing, container internals leaked, GETEX corrupts the type marker

**Severity:** High
**Class:** Redis protocol compatibility / Correctness
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase; scope extended to `getbit` per author response)

### Cited files / lines
```java
// GGroup.java:273-274 (getdel), 380-381 (getrange), 406-410 (getset), 336-341 + 357-360 (getex)
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var valueBytes = get(slotWithKeyHash);          // BaseCommand.get(s) → get(s, false)

// BaseCommand.java:837-839
public byte[] get(SlotWithKeyHash slotWithKeyHash) {
    return get(slotWithKeyHash, false);         // expectTypeString = false → no TypeMismatchException

// BaseCommand.java:972-975 (setCv, used by getex for non-number non-compressed values)
} else {
    // check if the value needs do compression
    set(cv.getCompressedData(), slotWithKeyHash, 0, cv.getExpireAt());   // spType 0 drops the marker
```

### Root cause

All four string commands fetch via `get(slotWithKeyHash)` which is `get(s, false)` (BaseCommand.java:837-839), so the
`expectTypeString` guard at BaseCommand.java:861-862 never fires. Redis requires OBJ_STRING for all four and replies
`WRONGTYPE` otherwise. Velo instead returns the internal container encoding (`SP_TYPE_LIST/-2048`, `SP_TYPE_HASH/-1024`,
`SP_TYPE_ZSET/-8192`, `SP_TYPE_SET/-4096`, `SP_TYPE_GEO/-8193` stored bytes) to the client:

- GETDEL: replies binary container encoding, then deletes the container key.
- GETRANGE: replies a substring of the binary encoding.
- GETSET: replies the binary encoding and overwrites the container with a plain string (silent type/data loss).
- GETEX: replies the binary encoding and persists it back via `setCv()`; for non-number non-compressed values `setCv`
  calls `set(..., 0, expireAt)` which re-stores the container bytes as a plain string — the type marker is dropped
  (EGroup.java:325-327 documents this exact hazard and uses `putToOneSlot` instead). A small list key becomes a
  "string" whose value is list-encoded binary; subsequent LRANGE gets WRONGTYPE. `getex` on a big-string key has a
  second corruption path: `getCv()` inlines the big-string bytes (BaseCommand.java:759-773) and `setCv()` re-stores the
  whole value inline, orphaning the big-string file.

### Trigger / impact

`LPUSH l a b c` → `GETDEL l` / `GETSET l x` / `GETEX l persist`: Redis replies WRONGTYPE for all three; Velo leaks
internal binary, and for GETSET/GETEX permanently converts the key to a plain string. Any client mixing container
types with string commands silently corrupts data. The GET fast path (RequestHandler.java:503) and the generic
dispatch (RequestHandler.java:572-575) already map `TypeMismatchException` to WRONGTYPE, so the fix is local.

### Suggested fix direction

In getdel/getrange/getset/**getbit** use `get(slotWithKeyHash, true)`; in getex check `cv.isTypeString()` (CompressedValue.java:606)
after `getCv()` and throw `TypeMismatchException` otherwise. Write the new expireAt back with `putToOneSlot(...)`
(or at minimum only call `setCv` for string/number types), mirroring EGroup.expire.

> **Author response note (stage 3):** reviewer observation 1 accepted — `GGroup.getbit()` (GGroup.java:259) has the
> same unchecked `get(slotWithKeyHash)` call; the author had verified GETBIT's offset semantics but missed the type
> check. getbit is folded into this finding's fix scope.

## Finding 2 (Bug 77.2): GETEX expire-time arithmetic can overflow — garbage/negative expireAt persisted instead of an error

**Severity:** Medium
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:347-355
} else if (ex > -1) {
    expireAt = System.currentTimeMillis() + ex * 1000;   // ex*1000 overflows for ex > Long.MAX_VALUE/1000
} else if (px > -1) {
    expireAt = System.currentTimeMillis() + px;          // overflows for px near Long.MAX_VALUE
} else if (exAt > -1) {
    expireAt = exAt * 1000;                              // same overflow
```

### Root cause

`x` is parsed as long (GGroup.java:311) and only checked for `x < 0`. `ex * 1000`, `exAt * 1000`, and
`now + px` silently wrap for large inputs. EGroup.expire got the same bug fixed in round bug 75.3 with guards
(EGroup.java:279-288: `seconds > Long.MAX_VALUE / 1000` and `when > Long.MAX_VALUE - basetime` → error).

### Trigger / impact

`GETEX key EX 9223372036854775807` → `ex * 1000` wraps (e.g. to 8 … ≈ -9223372036854775808*1000 ≡ 0), producing a
wrong/instantly-past or far-future `expireAt` that `setCv` persists — the key may vanish immediately or never expire.
Redis 7.2 replies `ERR invalid expire time in 'getex' command`.

### Suggested fix direction

Reuse the EGroup.expire guard pattern before each multiply/add; reply an invalid-expire error on overflow.

## Finding 3 (Bug 77.3): GETSET skips KEY_MAX_LENGTH and VALUE_MAX_LENGTH validation

**Severity:** Medium
**Class:** Edge case / Internal consistency (refined by reviewer: **not** a Redis-parity issue — Redis 7.2 accepts a 257-byte key and a 1 MB value; see Review Feedback 77.3)
**Status:** `CONFIRMED (reproduced)` + `REFINED` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:399-417 (getset) — no key/value length checks anywhere in the method
var valueBytes = data[2];
...
set(valueBytes, slotWithKeyHash);   // BaseCommand.set() performs no length validation either

// RequestHandler.java:520-530 (SET shortcut) and SGroup.java:840-846 / 1038-1044 — both enforce
if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH && bigStringNoMemoryCopy == null) {
    return ErrorReply.VALUE_TOO_LONG;
}
```

### Root cause

Key/value length limits are enforced per-command in Velo (RequestHandler SET shortcut, SGroup.set, LGroup, HGroup,
ZGroup …). `GGroup.getbit` (236) and `GGroup.getex` (288-291) check `KEY_MAX_LENGTH`, but `getset` checks neither
limit, and `BaseCommand.set()` itself does not validate. GETSET can therefore create a key longer than
KEY_MAX_LENGTH and store a value larger than VALUE_MAX_LENGTH, violating invariants other modules rely on
(e.g. the 24-bit length fields noted at BaseCommand.java:968-970).

### Trigger / impact

`GETSET <8KB+ key> v` / `GETSET k <1MB+ value>` succeed in Velo; the equivalent `SET` returns
`ERR key too long` / `ERR value too long`. Oversized keys/values then flow into WAL/index code that assumes the cap.

### Suggested fix direction

Add the two standard checks at the top of getset() (`ErrorReply.KEY_TOO_LONG` / `ErrorReply.VALUE_TOO_LONG`),
mirroring RequestHandler.java:481-483/528-530.

## Finding 4 (Bug 77.4): GEOADD accepts NaN longitude/latitude — NaN points stored, downstream geo replies become NaN

**Severity:** Medium
**Class:** Edge case (refined by reviewer: scope is **NaN only** — infinite coordinates are already rejected by the bounds check; see Review Feedback 77.4)
**Status:** `CONFIRMED (reproduced)` + `REFINED` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:527-533 — bounds check only; every comparison with NaN is false
var lon = Double.parseDouble(new String(data[i]));
var lat = Double.parseDouble(new String(data[i + 1]));
if (lon > RedisGeo.GEO_LONG_MAX || lon < RedisGeo.GEO_LONG_MIN ||
    lat > RedisGeo.GEO_LAT_MAX || lat < RedisGeo.GEO_LAT_MIN) {
    return ErrorReply.NOT_FLOAT;
}

// RedisGeo.java:117-127 add() — same comparison-only validation, NaN passes

// RedisGeo.java:236-242 — NaN offsets: (int) NaN == 0 → geohash of a NaN point silently encodes as 0
```

### Root cause

`Double.parseDouble("NaN")` succeeds and NaN fails every `<`/`>` bound, so `GEOADD key NaN 37.5 m` is accepted and
stored. Redis 7.2 rejects NaN at parse time — `string2d()` in util.c returns 0 on `isnan(*dp)` →
`ERR value is not a valid float`. bug_44 round 1 fixed the identical hole for GEOSEARCH FROMLONLAT
(GGroup.java:749-754 has the `Double.isNaN`/`isInfinite` checks) but GEOADD was not covered.

### Trigger / impact

`GEOADD sicily NaN NaN palermo` succeeds → `GEOHASH sicily palermo` returns a garbage all-zero hash,
`GEOPOS sicily palermo` returns `"NaN"` bulk strings, `GEODIST` returns `"NaN"`. Persisted data is poisoned.

### Suggested fix direction

Add `Double.isNaN || Double.isInfinite` checks to the geoadd parse block (same expression as GGroup.java:749-754) and
to `RedisGeo.add()` for defense in depth.

## Finding 5 (Bug 77.5): Geo double replies formatted with Java `String.valueOf(double)` — not Redis fixed-4-decimals; scientific notation for large/small values

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` + `REFINED` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// BulkReply.java:61-63
public BulkReply(double d) {
    this.raw = String.valueOf(d).getBytes();
}

// GGroup.java:622 (geodist), 645-647 (geopos), 989 (withdist), 997-1000 (withcoord)
return new BulkReply(unit.fromMeters(distance));
```

### Root cause

Redis 7.2 formats geo doubles with dedicated encoders (geo.c):
`addReplyDoubleDistance()` = `fixedpoint_d2string(d, 4)` — fixed notation, always 4 decimals ("0.0000",
"166274.1516") — for GEODIST and GEOSEARCH WITHDIST; `addReplyHumanLongDouble()` for GEOPOS/WITHCOORD.
Velo uses Java's `Double.toString`, which (a) never pads to 4 decimals ("0.0"), (b) switches to scientific notation
at ≥1e7 and <1e-3, (c) after-unit-conversion exposes up to 7 decimals because `RedisGeo.distance()` rounds to 4
decimals **in meters before** dividing by the unit factor.

### Trigger / impact

- Same point twice: Redis `"0.0000"` vs Velo `"0.0"` (GEODIST).
- Palermo↔Catania in km: Redis `"166.2742"` vs Velo `"166.2741516"`.
- Antipodal points in meters (~2.0015e7): Velo `"2.0015086E7"` — an unparseable distance for numeric clients.
- `GEOPOS` of lon 0.0001: Velo `"1.0E-4"` vs Redis `"0.0001"`.

### Suggested fix direction

Format distances with fixed 4-decimals (e.g. `%.4f` via `String.format` or a fixedpoint d2string port) before
constructing the BulkReply for geodist/withdist; use a `%g`-style human formatter for coordinates. Consider doing the
4-decimal rounding after unit conversion to match Redis output exactly.

## Finding 6 (Bug 77.6): GEOSEARCH WITHHASH returns a bulk string; Redis returns an integer reply

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:992-994
if (isWithHash) {
    subReplies[ii] = new BulkReply(RedisGeo.hashAsStore(result.p));   // "$19\r\n3479099956230698\r\n"
    ii++;
}
```

### Root cause

Redis geo.c georadiusGeneric replies `addReplyLongLong(c, gp->score)` for WITHHASH (`:3479099956230698\r\n`).
Velo emits the 52-bit geohash as a bulk string. Strictly-typed RESP clients (and RESP3 type dispatch) mis-handle it.

### Trigger / impact

`GEOSEARCH sicily FROMMEMBER palermo BYRADIUS 200 km WITHHASH` — reply element type differs from Redis 7.2.

### Suggested fix direction

Reply with `IntegerReply` (or a dedicated reply holding the raw long).

## Finding 7 (Bug 77.7): GEOSEARCHSTORE does not delete the destination key when the result is empty or the source key is missing

**Severity:** Medium
**Class:** Redis protocol compatibility / Correctness
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:851-854
var rg = getRedisGeo(slotWithKeyHash);
if (rg == null) {
    return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;   // dst left untouched
}
// GGroup.java:889-891
if (resultList.isEmpty()) {
    return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;   // dst left untouched
}
```

### Root cause

Redis 7.2 geo.c georadiusGeneric deletes the store key in both empty paths before replying 0:
`zobj == NULL` → `dbDelete(c->db, storekey)`; `returned_items == 0` → `dbDelete(c->db, storekey)`. Velo returns 0 but
leaves a stale destination key holding the previous query's result.

### Trigger / impact

`GEOSEARCHSTORE dst src FROMMEMBER a BYRADIUS 1 m` (no matches, dst exists from a wider previous query) returns 0
while `ZRANGE dst`/`GEOPOS dst`-style reads still serve the old result — clients that trust "0 means dst now empty"
serve stale data.

### Suggested fix direction

On both empty paths in store mode call `removeDelay(dstS)` (cross-worker safe via the existing async pattern) before
replying 0.

## Finding 8 (Bug 77.8): GEOSEARCHSTORE (without STOREDIST) destination is a Velo geo-type key, not a sorted set

**Severity:** Low (known design divergence)
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: product decision (see Review Feedback)

### Cited files / lines
```java
// GGroup.java:933-937
var dstRg = new RedisGeo();
for (var result : resultList) {
    // not sorted, todo
    dstRg.add(result.member(), result.p.lon(), result.p.lat());
}
...
saveRedisGeo(dstRg, dstS);      // stored as CompressedValue.SP_TYPE_GEO (-8193)
```

### Root cause

Redis stores GEOSEARCHSTORE results in a real sorted set (geo.c: `createZsetObject()` + `zslInsert`, score = geohash
bits), and the docs explicitly support post-processing with ZRANGE/ZCARD/ZSCAN etc. Velo stores
`SP_TYPE_GEO`; `ZRANGE dst 0 -1` then throws TypeMismatchException → WRONGTYPE. The result is also unordered
("not sorted, todo" — Redis dst is ordered by geohash score). Context: bug_44 round 1 deliberately made STOREDIST a
zset (commit 2b474042) while plain store remained geo-typed — this finding covers the remaining gap, not the fixed
part.

### Trigger / impact

`GEOSEARCHSTORE dst src FROMLONLAT 13 38 BYRADIUS 200 km` then `ZRANGE dst 0 -1` works in Redis, WRONGTYPE in Velo.

### Suggested fix direction

Store the plain-store destination as a zset with `hashAsStore` scores (mirroring the STOREDIST path's
`ZGroup.saveRedisZSet` usage), or document the divergence as a product decision.

## Finding 9 (Bug 77.9): GEOSEARCH FROMMEMBER of a missing member returns an empty result; Redis returns an error

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:856-863
if (fromMember != null) {
    var p = rg.get(fromMember);
    if (p == null) {
        return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;   // silent empty
    }
```

### Root cause

Redis 7.2 geo.c, when the source key exists but the FROMMEMBER member is missing, replies
`-ERR could not decode requested zset member` (both GEOSEARCH and GEOSEARCHSTORE; the empty-array early return only
applies when the source key itself does not exist).

### Trigger / impact

`GEOSEARCH sicily FROMMEMBER nosuch BYRADIUS 100 km` → Redis error vs Velo empty array — clients cannot distinguish
"member typo" from "no matches".

### Suggested fix direction

Reply an ErrorReply when `rg != null` but the from-member is absent (keep empty/0 when the source key is missing).

## Finding 10 (Bug 77.10): GETRANGE parses start/end as 32-bit int — valid 64-bit ranges rejected as NOT_INTEGER

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)` — verdict: `CONFIRMED (reproduced)` · disposition: `fixed` (pending fix phase)

### Cited files / lines
```java
// GGroup.java:371-378
start = Integer.parseInt(new String(data[2]));
end = Integer.parseInt(new String(data[3]));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}
```

### Root cause

Redis GETRANGE takes 64-bit signed offsets (t_string.c getrangeCommand uses `long`); out-of-range offsets are
clamped, so `GETRANGE key 99999999999 -1` is a valid empty-result query. Velo's `Integer.parseInt` rejects any value
outside ±2^31-1 with an error reply.

### Trigger / impact

`GETRANGE key 9223372036854775806 9223372036854775807` → empty bulk in Redis, `ERR value is not an integer or out of
range` in Velo.

### Suggested fix direction

Parse with `Long.parseLong` and clamp before `IndexStartEndReset.reset` (which needs ints — clamp to
[-len, len] first, matching Redis normalization).

## Notes / non-findings

- **GEOHASH lat ranges are correct vs Redis**: Velo `hashAsStore` encodes with ±85.05112878 (Redis internal score
  range) and `hash` re-encodes with ±90 (Redis geohashCommand does exactly this re-encode, comment in geo.c); the
  11-char zero-padded tail matches geo.c. Checked against fetched source — do not re-investigate.
- **bug_44 fixes verified present in current code**: STOREDIST accepted/`WITH*` rejected per mode (719-727), required
  units for BYRADIUS/BYBOX (788-797, 818-828), `COUNT n ANY` (716-718), duplicate ASC/DESC rejected via `hasOrder`
  (692-702), FROMLONLAT NaN/infinite/bounds checks (749-754), positive finite radius/box (780-782, 809-812).
- **GET fast path** (RequestHandler.java:475-516) does the string-type check correctly (`get(s, true)`).
- **GETBIT semantics**: out-of-range bit → 0 via RedisBitSet.get (83-85) matches Redis; the 1MB offset cap
  (GGroup.java:251-256) is an inline-documented deliberate Velo limit — not a bug.
- **GETEX parse branches** (persist / ex / px / exat / pxat, negative → INVALID_INTEGER, junk → SYNTAX) match Redis
  syntax; `pxat` millisecond passthrough and no-arg GETEX (no TTL change) verified against tests.
- **GEOADD return values**: `CH` → changed+added, plain → added, NX/XX skip logic — matches Redis ZADD semantics.
- **GEOADD option-position laxness** (flags accepted interleaved after triples, e.g. `GEOADD k 13 39 m CH` accepted
  where Redis syntax-errors): accepted laxity, low value in fixing — recorded to avoid re-investigation.
- **GEODIST unit error text** differs (Velo SYNTAX vs Redis "unsupported unit provided. please use M, KM, FT, MI"):
  error-text parity not required, both error.
- **`geosearch` reply option order** (member, dist, hash, coord) matches Redis; empty-search read replies empty array
  like Redis emptyarray.
- **isWithinBox approximation** (degree-shift box vs Redis geohash rectangle): known simplified geo math, consistent
  with Velo's design; not re-reported.

## Reviewer checklist (for AI agent 2)

> **Executed by AI agent 2 in round 1** — every item below was reproduced; see `## Review Feedback` for the pasted evidence and per-finding verdicts.

- Finding 77.1: Spock temp spec with `InMemoryGetSet`: put a cv with `cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST`
  and known `compressedData`, then `execute('getdel a')` / `'getrange a 0 1'` / `'getset a v'` — assert current code
  returns a BulkReply with the raw list bytes (and for getset/getex the stored type marker is lost); differential:
  redis 7.2 `LPUSH l a` + `GETDEL l` → WRONGTYPE.
- Finding 77.2: `execute('getex a ex 9223372036854775807')` on an existing key, inspect `inMemoryGetSet.getBuf(...).cv().expireAt`
  — expect wrapped garbage (e.g. ≈ now); Redis: `GETEX k EX 9223372036854775807` → invalid expire time error.
- Finding 77.3: `execute('getset <KEY_MAX_LENGTH+1 byte key> v')` and a >1MB value — current code stores; compare
  `SET` same args → KEY_TOO_LONG/VALUE_TOO_LONG.
- Finding 77.4: `execute('geoadd a NaN 37.5 m')` succeeds today; follow with `geopos a m` → "NaN" bulk; redis:
  `GEOADD k NaN 37.5 m` → "value is not a valid float".
- Finding 77.5: unit-level: `new BulkReply(20015086.123d).raw` → "2.0015086123E7"; geodist between antipodal members
  in m unit; `GEODIST` same member twice → "0.0" vs Redis "0.0000".
- Finding 77.6: parse `GEOSEARCH ... WITHHASH` reply element — BulkReply vs Redis integer `:3479099956230698`.
- Finding 77.7: pre-populate dst key via geosearchstore wide query; re-run with BYRADIUS 1 m (no match) → dst still
  readable/non-empty in Velo; Redis deletes dst (geo.c dbDelete path).
- Finding 77.8: after `GEOSEARCHSTORE dst src ...` (no STOREDIST), `execute('zrange dst 0 -1')` → WRONGTYPE; Redis
  returns members.
- Finding 77.9: `geoadd a 13 38 m1` then `execute('geosearch a frommember nosuch byradius 100 km')` → empty array;
  Redis → "could not decode requested zset member".
- Finding 77.10: `execute('getrange a 99999999999 -1')` → NOT_INTEGER; Redis → "".

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (primary) + live Redis 7.2.11 differential (secondary) + static inspection of the cited lines.

**Environment**

- Velo baseline: commit `f95ce1ac8e44d585585127b5ac37f394c8eaf549` (branch `main`; the doc's `review/ggroup` branch was not created — reviewer worked on `main` at the reviewed sha).
- Reviewer temp spec (untracked, must be deleted or converted to permanent regression tests by the fix agent):
  `src/test/groovy/io/velo/command/Bug77ReviewTempSpec.groovy` — 10 spec methods, one per finding, all green against the *current* (buggy) code, i.e. they pin the buggy behaviour.
- Run with: `./gradlew :test --tests "io.velo.command.Bug77ReviewTempSpec" -i`
- Differential server: `redis-server --port 7799` → `Redis server v=7.2.11 sha=d4c381df:0 malloc=jemalloc-5.3.0 bits=64 build=e3db856fd57ff434`. Full transcript saved to `doc/bug_reviews/logs/20261027_162000_ggroup_differential_redis72_bug_77.log`.
- Redis ground truth re-fetched and quoted below: `src/geo.c`, `src/t_string.c` (redis/redis @ 7.2).

**Temp spec output (verbatim, `./gradlew :test --tests "io.velo.command.Bug77ReviewTempSpec" -i`)**

```
77.1 getdel  -> BulkReply raw=LIST-ENCODED-BYTES-XYZ
77.1 getrange-> BulkReply raw=LIST
77.1 getset  -> BulkReply raw=LIST-ENCODED-BYTES-XYZ
77.1 getset  -> stored spType after getset = 0 (SP_TYPE_LIST=-2048)
77.1 getex   -> BulkReply raw=LIST-ENCODED-BYTES-XYZ
77.1 getex   -> stored spType after getex = 0 (SP_TYPE_LIST=-2048)
77.1 getdel hash -> BulkReply raw=LIST-ENCODED-BYTES-XYZ
77.2 getex ex max -> reply=BulkReply, stored expireAt=1793089372773, now=1793089373775
77.2 getex px max -> reply=BulkReply, stored expireAt=-9223370243765402033, now=1793089373777
77.2 getex exat max -> reply=BulkReply, stored expireAt=-1000
77.3 getset long key -> reply=NilReply, stored keys=[257]
77.3 getset long value -> reply=NilReply, stored value length=1048577
77.3 set long key -> ErrorReply{message='key too long (max length is 256)'}, set long value -> ErrorReply{message='value too long (max length is 1048576)'}
77.4 geoadd NaN -> IntegerReply 1
77.4 geopos -> lon=NaN lat=37.5
77.4 geoadd Infinity -> ErrorReply not float
77.4 geoadd NaN lat -> IntegerReply 1
77.5 BulkReply(20015086.123d) -> 2.0015086123E7
77.5 BulkReply(0.0001d) -> 1.0E-4
77.5 geodist same member -> '0.0' (redis: '0.0000')
77.5 geodist km -> '166.2742578' (redis: '166.2742')
77.5 geosearch withdist -> '0.0' (redis: '0.0000')
77.5 geodist far m -> '1.8908471E7' (redis: '18908470.9226')
77.5 geopos -> lon='1.0E-4' lat='1.0E-4' (redis: '0.00009924173355103')
77.6 withhash -> BulkReply '3479099956230698' (redis: :3479099956230698)
77.7 first store -> 2
77.7 empty store -> io.velo.reply.IntegerReply$2@49005dc4, dst still present=true
77.7 missing source -> io.velo.reply.IntegerReply$2@49005dc4, dst2 present=false
77.8 dst spType = -8193 (SP_TYPE_GEO=-8193, SP_TYPE_ZSET=-8192)
77.8 zrange -> TypeMismatchException: Expect sp type array=[-8192], but got sp type=-8193
77.9 geosearch missing frommember ->  EMPTY
77.10 getrange 64bit -> ErrorReply{message='not integer'}
77.10 getrange max -> ErrorReply{message='not integer'}

10 tests completed, 0 failed     (BUILD SUCCESSFUL)
```

**Redis 7.2.11 differential transcript (verbatim)**

```
$ redis-cli -p 7799 lpush l a ; redis-cli -p 7799 getdel l
1
WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 7799 lpush l2 a ; redis-cli -p 7799 getset l2 x
1
WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 7799 lpush l3 a ; redis-cli -p 7799 getex l3
1
WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 7799 lpush l4 a ; redis-cli -p 7799 getrange l4 0 1
1
WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 7799 set k v ; redis-cli -p 7799 getex k EX 9223372036854775807
OK
ERR invalid expire time in 'getex' command
$ redis-cli -p 7799 set k v ; redis-cli -p 7799 getex k PX 9223372036854775807
OK
ERR invalid expire time in 'getex' command
$ redis-cli -p 7799 set k v ; redis-cli -p 7799 getex k EXAT 9223372036854775807
OK
ERR invalid expire time in 'getex' command
$ redis-cli -p 7799 geoadd g NaN 37.5 m
ERR value is not a valid float
$ redis-cli -p 7799 geoadd g inf 37.5 m
ERR invalid longitude,latitude pair inf,37.500000
$ redis-cli -p 7799 geodist sicily Palermo Palermo        # after GEOADD 13.361389 38.115556 Palermo
0.0000
$ redis-cli -p 7799 geodist sicily Palermo Catania km      # after GEOADD 15.087269 37.502669 Catania
166.2742
$ redis-cli -p 7799 geodist far a b m                      # after GEOADD far 0 85 a / 0 -85 b
18908470.9226
$ redis-cli -p 7799 geosearch sicily frommember Palermo byradius 200 km withhash
Palermo
3479099956230698
Catania
3479447370796909
$ redis-cli -p 7799 geosearchstore dst sicily frommember Palermo byradius 200 km
2
$ redis-cli -p 7799 type dst
zset
$ redis-cli -p 7799 zcard dst
2
$ redis-cli -p 7799 geosearchstore dst sicily fromlonlat 0 0 byradius 1 m   # empty result
0
$ redis-cli -p 7799 exists dst                            # redis deleted the stale dst
0
$ redis-cli -p 7799 geosearchstore dst2 sicily frommember Palermo byradius 200 km
2
$ redis-cli -p 7799 type dst2
zset
$ redis-cli -p 7799 zrange dst2 0 -1 withscores
Palermo
3479099956230698
Catania
3479447370796909
$ redis-cli -p 7799 geosearch sicily frommember nosuch byradius 100 km
ERR could not decode requested zset member
$ redis-cli -p 7799 set s hello ; redis-cli -p 7799 getrange s 99999999999 -1
OK
(empty string)
```

Raw RESP check for the reply *types* (python socket against 127.0.0.1:7799):

```
WITHHASH: b'*2\r\n*2\r\n$7\r\nPalermo\r\n:3479099956230698\r\n*2\r\n$7\r\nCatania\r\n:3479447370796909\r\n'
WITHDIST: b'*2\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n'
WITHCOORD: b'*2\r\n*2\r\n$7\r\nPalermo\r\n*2\r\n$20\r\n13.36138933897018433\r\n$20\r\n38.11555639549629859\r\n...\r\n'
```

### Verdict table

| Finding | Verdict | Evidence |
|---|---|---|
| 77.1 | `CONFIRMED (reproduced)` | Temp spec: `getdel`/`getrange`/`getset`/`getex` on an `SP_TYPE_LIST`/`SP_TYPE_HASH` key all return the raw container bytes instead of `WRONGTYPE` (`77.1 getdel -> BulkReply raw=LIST-ENCODED-BYTES-XYZ`, `77.1 getdel hash -> BulkReply raw=LIST-ENCODED-BYTES-XYZ`). The stored type marker is destroyed: `stored spType after getset = 0` and `stored spType after getex = 0` (was `SP_TYPE_LIST=-2048`). Redis 7.2.11 replies `WRONGTYPE` for all four commands (`getGenericCommand` → `checkType(c,o,OBJ_STRING)` in `src/t_string.c:323-335`, used by `getdelCommand`, `getsetCommand`, `getexCommand`; `getrangeCommand` uses the same `checkType`). Additional impact confirmed by inspection: `getdel` also **deletes** the container key (`removeDelay`, GGroup.java:276) — Redis deletes nothing when it errors, so Velo loses the list/set/zset data. Big-string sub-claim confirmed by inspection: `getCv()` inlines the big-string payload and rewrites `dictSeqOrSpType` (BaseCommand.java:759-773), `setCv()` then re-stores the value inline via `set(..., 0, expireAt)` (BaseCommand.java:972-975), which drops the `SP_TYPE_BIG_STRING` marker and orphans the big-string file. |
| 77.2 | `CONFIRMED (reproduced)` | Temp spec: `77.2 getex ex max -> stored expireAt=1793089372773, now=1793089373775` → the expire time wrapped to **1 second in the past** (`ex * 1000` overflows to `-1000`); `77.2 getex px max -> stored expireAt=-9223370243765402033` (negative → instantly expired); `77.2 getex exat max -> stored expireAt=-1000`. In all three cases Velo replies `BulkReply` (success) and persists the wrapped value; the key is then immediately expired (`CompressedValue.isExpired()` → `getCv()` returns null → the key has silently vanished). Redis 7.2.11 replies `ERR invalid expire time in 'getex' command` for `EX`/`PX`/`EXAT` = `Long.MAX_VALUE` (`getExpireMillisecondsOrReply`, `src/t_string.c:164-189`: `unit == UNIT_SECONDS && *milliseconds > LLONG_MAX / 1000` → error, plus an overflow check after adding `commandTimeSnapshot()`). The `EGroup.expire` guard pattern cited by the author is present at EGroup.java:279-288. |
| 77.3 | `CONFIRMED (reproduced)` + `REFINED` | Temp spec: `77.3 getset long key -> reply=NilReply, stored keys=[257]` — a 257-byte key (KEY_MAX_LENGTH=256) is accepted and stored by `GETSET`; `77.3 getset long value -> reply=NilReply, stored value length=1048577` — a 1 MB + 1 byte value (VALUE_MAX_LENGTH=1048576) is accepted and stored. The same input through `SET` is rejected: `77.3 set long key -> ErrorReply{key too long (max length is 256)}`, `77.3 set long value -> ErrorReply{value too long (max length is 1048576)}` (`SGroup.set()`, SGroup.java:839-848). **Refinement:** this is *not* a Redis-parity bug — Redis 7.2.11 accepts both (`SET <257-byte key> v` → `OK`; Redis has no 256-byte/1 MB cap, only `proto-max-bulk-len`). The real defect is a Velo-internal invariant bypass: `GETSET` is the only string write command that skips the guards enforced by `SET`/`SETBIT`/`GETBIT`/`GETEX`/`RequestHandler`'s SET shortcut. Also note the author's "24-bit length field" justification is inaccurate: `valueLengthHigh24WithShortTypeLow8 = length << 8 | shortType` (BaseCommand.java:969, 1191) holds up to 16 777 215 bytes, so a 1 MB value does not overflow it — the real invariant at risk is `VALUE_MAX_LENGTH` itself (`CompressedValue.java:93-95`: "must be less than `Wal.ONE_GROUP_BUFFER_SIZE`"). Fix = add the two standard guards to `getset()`; the error text should match `SGroup` (`ErrorReply.KEY_TOO_LONG` / `ErrorReply.VALUE_TOO_LONG`). |
| 77.4 | `CONFIRMED (reproduced)` + `REFINED` | Temp spec: `77.4 geoadd NaN -> IntegerReply 1` (accepted, no `NOT_FLOAT`); `77.4 geopos -> lon=NaN lat=37.5` (the poisoned point is served back); `77.4 geoadd NaN lat -> IntegerReply 1` (NaN latitude accepted too). Redis 7.2.11: `GEOADD g NaN 37.5 m` → `ERR value is not a valid float` (`string2d()` rejects `isnan`). **Refinement:** the scope is NaN only — `77.4 geoadd Infinity -> ErrorReply not float`, because `Infinity > GEO_LONG_MAX` is a true comparison and is caught by the existing bounds check. Redis's own message for infinity differs (`ERR invalid longitude,latitude pair inf,37.500000`) but both are errors. Fix = add `Double.isNaN()` checks to the geoadd parse block (GGroup.java:527-533) mirroring GGroup.java:749-754, and optionally to `RedisGeo.add()` (RedisGeo.java:117-127) for defence in depth. |
| 77.5 | `CONFIRMED (reproduced)` + `REFINED` | Temp spec: `77.5 BulkReply(20015086.123d) -> 2.0015086123E7`, `77.5 BulkReply(0.0001d) -> 1.0E-4` (scientific notation), `77.5 geodist same member -> '0.0'` (Redis `0.0000`), `77.5 geodist km -> '166.2742578'` (Redis `166.2742`), `77.5 geosearch withdist -> '0.0'` (Redis `0.0000`), `77.5 geodist far m -> '1.8908471E7'` (Redis `18908470.9226`), `77.5 geopos -> lon='1.0E-4'` (Redis `0.00009924173355103`). Redis formats distances with `addReplyDoubleDistance()` = `fixedpoint_d2string(dbuf, sizeof(dbuf), d, 4)` (geo.c:212-216) — always fixed notation with exactly 4 decimals — and coordinates with `addReplyHumanLongDouble()` (geo.c:799-800). **Refinement:** the author's quoted Velo km figure `166.2741516` is wrong; the actual Velo output is `166.2742578` (Velo's own Haversine distance is `166274.2578` m vs Redis's `166274.1516` m — a *separate*, pre-existing geo-math divergence, see Additional observations). The formatting defect itself is fully confirmed. Fix = format the distance with fixed 4 decimals **after** the unit conversion (`RedisGeo.distance()` already rounds to 4 decimals *in meters* — RedisGeo.java:160 — so rounding after the division is what matches Redis); use a fixed-point formatter (`%.17f`-style, as `addReplyHumanLongDouble` does) for coordinates. |
| 77.6 | `CONFIRMED (reproduced)` | Temp spec: `77.6 withhash -> BulkReply '3479099956230698'`; the Redis raw RESP shows an integer reply: `WITHHASH: b'...$7\r\nPalermo\r\n:3479099956230698\r\n...'`. Redis geo.c:795 uses `addReplyLongLong(c, gp->score)`. Fix = reply with `IntegerReply` (GGroup.java:992-994). Note `RedisGeo.hashAsStore()` already returns a `long`, so the change is one line. |
| 77.7 | `CONFIRMED (reproduced)` | Temp spec: `77.7 first store -> 2` (dst created), then `77.7 empty store -> <IntegerReply 0>, dst still present=true` — the destination key survives an empty result. Redis 7.2.11 deletes it: `GEOSEARCHSTORE dst sicily fromlonlat 0 0 byradius 1 m` → `0` and `EXISTS dst` → `0` (also after a previous wide store, `dst` existed with 2 members and was deleted by the empty store). Redis geo.c:836 (`} else if (dbDelete(c->db,storekey)) {`) and geo.c:701 (the `zobj == NULL` path) both delete the store key before replying 0. **Partial correction to the author's citation:** the "source key missing" path in Velo is already correct — `77.7 missing source -> <IntegerReply 0>, dst2 present=false` — because the destination was never created; but when the destination already exists and the *source* is missing, Velo returns 0 **without** deleting the stale destination (Redis deletes it, geo.c:701). The fix must therefore cover **three** paths: (a) empty result set, (b) source key missing, (c) source key missing while a stale destination exists. |
| 77.8 | `CONFIRMED (reproduced)` | Temp spec: `77.8 dst spType = -8193` (`SP_TYPE_GEO`, not `SP_TYPE_ZSET=-8192`) and `77.8 zrange -> TypeMismatchException: Expect sp type array=[-8192], but got sp type=-8193` → `RequestHandler` maps that to `WRONGTYPE`. Redis 7.2.11: `TYPE dst2` → `zset`, `ZRANGE dst2 0 -1 WITHSCORES` → the members with their geohash scores. Redis geo.c:806-840 creates a real sorted set (`createZsetObject()` + `zslInsert`) with `score = storedist ? gp->dist : gp->score` — i.e. for the plain (non-STOREDIST) store the score is the **geohash** score, not the distance. **Discretion required from the product owner:** the code carries a `// not sorted, todo` marker (GGroup.java:935), and bug_44 round 1 deliberately made the STOREDIST path a zset while leaving the plain path geo-typed. If the fix is taken, the plain-store destination must be a zset scored with `RedisGeo.hashAsStore(result.p)` (Redis's `gp->score`), *not* with the distance — and the STOREDIST path must keep scoring with the converted distance. |
| 77.9 | `CONFIRMED (reproduced)` | Temp spec: `77.9 geosearch missing frommember -> EMPTY` (`MultiBulkReply.EMPTY`, i.e. an empty array). Redis 7.2.11: `GEOSEARCH sicily frommember nosuch byradius 100 km` → `ERR could not decode requested zset member` (geo.c:630). The empty array is only correct when the **source key itself** is missing (geo.c:620-628, `zobj == NULL` → `shared.emptyarray`). Fix = reply `ErrorReply` when the source exists but the FROMMEMBER member does not. |
| 77.10 | `CONFIRMED (reproduced)` | Temp spec: `77.10 getrange 64bit -> ErrorReply{not integer}` and `77.10 getrange max -> ErrorReply{not integer}` for `GETRANGE s 99999999999 -1` and `GETRANGE s 9223372036854775806 9223372036854775807`. Redis 7.2.11: `GETRANGE s 99999999999 -1` → empty string (no error); `getrangeCommand` parses both offsets with `getLongLongFromObjectOrReply` (64-bit) and clamps out-of-range offsets (t_string.c:502-541). Fix = `Long.parseLong` + clamp to `[-len, len]` before `IndexStartEndReset.reset`. |

### Additional observations

1. **77.1 is wider than reported — the same hole exists for `GETBIT`.** `GGroup.getbit()` (GGroup.java:259) also calls `get(slotWithKeyHash)` without the string-type check, so `GETBIT` on a list key returns a bit of the container encoding instead of `WRONGTYPE`. The author did not list `getbit` in 77.1 (the finding is titled "GETDEL/GETRANGE/GETSET/GETEX"). The fix should include `getbit` in the same commit, otherwise the type-confusion hole stays open through a sibling command. (Redis `getbitCommand` → `getGenericCommand` → `checkType(OBJ_STRING)`.)
2. **77.5 — Velo's Haversine distance differs from Redis's.** Velo: `166274.2578` m for Palermo→Catania; Redis 7.2.11: `166274.1516` m (and `18908471.0` vs `18908470.9226` m for the 0/±85° pair). The rounding is done in `RedisGeo.distance()` (RedisGeo.java:146-161) with `EARTH_RADIUS = 6372797.560856`; Redis uses `EARTH_RADIUS = 6372797.560856` too but computes the distance on the **geohash-decoded** coordinates and with a different formula (spherical law of cosines over the *decoded* point). This is a separate accuracy divergence from the formatting defect and is **not** part of 77.5 — recorded so the fix agent does not try to "fix" the distance algorithm inside the formatting commit, and so the next geo round does not re-report it.
3. **77.7 — `GEOSEARCHSTORE` with `STOREDIST` writes the distance as the zset score**, while Redis uses `gp->dist` (distance in the *output* unit) — Velo matches here (`dstRz.add(outputUnit.fromMeters(result.distance()), member)`, GGroup.java:906). No action.
4. **`GEOSEARCH`/`GEOSEARCHSTORE` source-key-type check** — `getRedisGeo()` calls `get(slot, false, SP_TYPE_GEO)` (GGroup.java:485), so a non-geo key correctly yields `WRONGTYPE`. Redis behaves the same way (a zset that is not a geo index is simply "not a geo index" and yields an empty result, Redis geo.c:620-628 — Redis has no geo type at all). Minor divergence, recorded, not re-reported.
5. **`GEODIST` unit error text** — Velo replies `ErrorReply.SYNTAX` ("syntax error"), Redis replies `ERR unsupported unit provided. please use M, KM, FT, MI`. Both are errors; error-text parity is not required (already noted by the author, confirmed by inspection of GGroup.java:604-607).
6. **Doc bookkeeping** — `doc/bug_reviews/STATE.md` line 22 still says "verification pending" for bug 77; the fix agent should update it to reflect the verdicts and the commit shas once the fixes land.

### Recommended fix order (for the fix agent)

Fix strictly one bug per commit, TDD first, and rebuild the reviewer's temp spec as a permanent regression test before touching production code. The temp spec currently *pins the buggy behaviour* — invert every assertion while converting it.

| # | Bug | Why this order | Notes for the fix |
|---|---|---|---|
| 1 | **77.1** (+ `getbit`, see observation 1) | Highest impact: silent type confusion, container bytes leaked to the client, and `GETDEL`/`GETSET`/`GETEX` **destroy** the container's type marker (data loss). | Use `get(slotWithKeyHash, true)` for `getdel`/`getrange`/`getset`/`getbit`; for `getex` check `cv.isTypeString()` after `getCv()` and throw `TypeMismatchException`; write the new `expireAt` back with `putToOneSlot(...)` (mirroring `EGroup.expire`, EGroup.java:319-325) instead of `setCv()`, so neither the type marker nor the big-string file is lost. `RequestHandler` already maps `TypeMismatchException` → `WRONGTYPE` (RequestHandler.java:572-575). |
| 2 | **77.2** | Data loss: the key is silently expired (vanishes) instead of returning an error. | Reuse the `EGroup.expire` guard pattern (EGroup.java:279-288) before every multiply/add; reply an "invalid expire time" error on overflow. Note `ex * 1000` must be checked against `Long.MAX_VALUE / 1000` **and** `now + delta` against `Long.MAX_VALUE`. |
| 3 | **77.4** | Persisted poisoned data (`GEOPOS` → `NaN`); the point stays in the geo index until the key is rewritten. | Add `Double.isNaN()` to the geoadd parse block (GGroup.java:527-533); `Double.isInfinite()` is already covered by the bounds check — do not change the error text for infinity. |
| 4 | **77.3** | Invariant bypass: the only string write command without the length guards. | Add `ErrorReply.KEY_TOO_LONG` / `ErrorReply.VALUE_TOO_LONG` at the top of `getset()`, mirroring `SGroup.set()` (SGroup.java:839-848). |
| 5 | **77.7** | Stale data: clients that trust "0 means dst is now empty" keep serving the previous query's result. | Delete the destination on **all** empty paths, including "source key missing while a stale destination exists" (see verdict 77.7). Use the existing cross-worker safe pattern (`removeDelay` / the `asyncRun` block used at GGroup.java:942-958). |
| 6 | **77.5** | Reply-format parity; clients parsing distances as fixed decimals break on `1.8908471E7`. | Format the distance with fixed 4 decimals **after** the unit conversion; do **not** touch the distance algorithm (observation 2). |
| 7 | **77.10** | Cheap, self-contained: 64-bit offsets. | `Long.parseLong` + clamp to `[-len, len]` before `IndexStartEndReset.reset`. |
| 8 | **77.9** | Cheap, self-contained: error parity for a missing FROMMEMBER. | Reply `ErrorReply` when the source exists but the member does not; keep the empty array when the source key is missing. |
| 9 | **77.6** | Cheap, one line: reply type parity. | `IntegerReply` instead of `BulkReply` at GGroup.java:992-994. |
| 10 | **77.8** | Lowest impact / needs a product decision. | Either implement the zset destination scored with `RedisGeo.hashAsStore(result.p)` (Redis semantics) or record it as an accepted divergence and remove the `// not sorted, todo` marker. Do not change the STOREDIST path (it is already correct). |

## Author Response (AI agent 1, round 1 — post-verification)

All 10 verdicts and the 3 refinements are **accepted**; nothing is disputed. Evidence re-checked against the codebase
and the local Redis source tree (`~/ws/redis`, 7.2) before agreeing:

| Finding | Response | Notes |
|---|---|---|
| 77.1 | Agree — incl. reviewer's `getbit` extension | `GGroup.java:259` confirmed: `get(slotWithKeyHash)` in getbit, same hole. Finding title/scope updated in place. |
| 77.2 | Agree | Repro output shows all three wrap modes (`ex`→past-by-1s, `px`→negative, `exat`→-1000); Redis t_string.c `getExpireMillisecondsOrReply` guard confirmed. |
| 77.3 | Agree with refinement | Class re-framed as internal consistency: Redis has no 256B/1MB caps (only `proto-max-bulk-len`), so Velo-only limits. Reviewer's correction of my "24-bit field" justification verified: `length << 8` in a 24-bit field tolerates 16M, so 1MB does not overflow it; the binding invariant is `VALUE_MAX_LENGTH < Wal.ONE_GROUP_BUFFER_SIZE` (CompressedValue.java:93-96 comment confirmed). SGroup.java:839-848 guard pattern confirmed. |
| 77.4 | Agree with refinement | Scope narrowed to NaN only. I had noted infinity rejection in my own analysis but left the redundant `isInfinite` in the fix direction — reviewer's `geoadd Infinity -> ErrorReply not float` repro closes it; fix adds `Double.isNaN()` only. |
| 77.5 | Agree with refinement | My quoted km figure `166.2741516` was Redis's output, not Velo's — actual Velo is `166.2742578` (see observation 2 correction below). The formatting defect itself (String.valueOf vs fixed-4-decimals, sci-notation at ≥1e7/<1e-3) is the confirmed core. Fix rounds **after** unit conversion. |
| 77.6 | Agree | One-line `IntegerReply` change; `hashAsStore()` already returns long. |
| 77.7 | Agree with the three-path fix scope | My original wording ("empty result **or** source key missing") already covered the stale-dst case; reviewer's decomposition (a) empty result, (b) source missing, (c) source missing + stale dst is the precise fix contract. All three delete dst in store mode. |
| 77.8 | Agree — product decision | Accept disposition; no code change until the product owner picks Redis-semantics zset (score = `hashAsStore`) vs documented divergence. |
| 77.9 | Agree | Error only when source key exists; empty array stays when source missing. |
| 77.10 | Agree | `Long.parseLong` + clamp to `[-len, len]` before `IndexStartEndReset.reset`. |

### On observation 2 — one factual correction (does not change any verdict)

The reviewer's *disposition* is correct — the distance-value divergence is a separate issue from the 77.5 formatting
defect and must not be "fixed" inside the formatting commit — but the mechanism given ("different formula, spherical
law of cosines") is inaccurate. Verified in the local Redis source:

- `~/ws/redis/src/geohash_helper.c:229-242` — `geohashGetDistance()` **is the Haversine formula**
  (`2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u*u + cos(lat1r)*cos(lat2r)*v*v))`), the same formula family and the
  same constant (`EARTH_RADIUS_IN_METERS = 6372797.560856`) as Velo's `RedisGeo.distance()` (RedisGeo.java:137,
  146-161).
- The actual source of `166274.2578` (Velo) vs `166274.1516` (Redis) is the **input coordinates**: Redis
  `geodistCommand` computes over the geohash-decoded 52-bit quantized points (`decodeGeohash(score1, xyxy)` …,
  geo.c), while Velo computes over the exact stored doubles. Redis also has a `v == 0.0` latitude-only shortcut
  (`geohashGetLatDistance`) and rounds only at output (`fixedpoint_d2string(…, 4)`); Velo rounds to 4 decimals in
  meters *before* unit conversion.

Recorded so the fix agent and round 2 do not chase a formula difference that does not exist.

### Bookkeeping

STATE.md bug-77 rows updated to "verification complete / fix phase pending" per observation 6.

## Fix Log (AI agent 1, round 1 fix phase)

Fix order followed the reviewer's recommended order. One bug = one commit; every fix went RED → minimal production
change → focused test green → JaCoCo line/branch confirmation on the changed code. The reviewer's temp spec
(`Bug77ReviewTempSpec.groovy`) was converted case-by-case into permanent `GGroupTest` cases (one `def 'test <command>'()`
method per command) and then deleted.

| Bug | Failing test evidence (RED) | Fix commit | Focused test (GREEN) | JaCoCo |
|---|---|---|---|---|
| 77.1 | `GGroupTest` 5 methods RED: `WrongExceptionThrownError` at GGroupTest.groovy:151/182/279/327/365 — getbit/getdel/getex/getrange/getset on an `SP_TYPE_LIST` key returned BulkReply instead of throwing (5 tests completed, 5 failed) | `b720ac57` `fix: ggroup string commands check type before read/write` | 5 methods + full `GGroupTest` class green | `get(s,true)` call sites 260/275/391/418 covered; getex `isTypeString` guard 342-343 both branches; `putToOneSlot` write-back 369 + `expireAt != expireAtOld` both branches covered |
| 77.2 | `test getex` RED: `Condition not satisfied: reply == new ErrorReply("invalid expire time…")` — reply was BulkReply with wrapped expireAt | `876e7d54` `fix: getex reject expire time overflow like expire` | green | guards 355-385: 20/20 lines, 9/9 branch lines fully covered (incl. `Long.MAX_VALUE.intdiv(1000)` boundary that hits the add-only overflow branch) |
| 77.4 | `test geoadd` RED: `IntegerReply 1` returned for `geoadd b NaN 37.5 m0` (expected NOT_FLOAT) | `0d0958c9` `fix: geoadd reject nan longitude latitude` | green | parse block 556-566: 6/6 lines, 12/12 branches (NaN lon, NaN lat, ±lon bounds, ±lat bounds) |
| 77.3 | `test getset` RED: `getset >key v` / `getset k >value` returned NilReply and stored, expected KEY_TOO_LONG/VALUE_TOO_LONG | `ab03c146` `fix: getset check key and value length limits` | green | guards 431-438 both checks both branches; FORMAT guard 427 covered via `test handle - format errors` |
| 77.7 | `test geosearch and geosearchstore` RED: after empty store, `getBuf(slot,'dst77',0,0L) == null` failed — stale dst survived | `010cc912` `fix: geosearchstore delete stale dst key on empty result` | green | helper `deleteDstKeyAndReplyZero` 705-728: sync + async success paths covered; call sites 914-931: store-mode deletion + read-mode EMPTY both branches |
| 77.5 | 3 methods RED: geodist same-member `'0.0'` vs `'0.0000'`; km `'166.2742578'` vs `'166.2743'`; far `'1.8908471E7'` (sci-notation); geopos `'1.0E-4'` vs `'0.0001'` | `c279c722` `fix: geo distance and coordinate reply formatting` | green | `distanceReply` 526 / `coordinate` 534 covered; call sites 678 (geodist), 701-702 (geopos), 1089-1090 (withcoord) + 1080 (withdist) covered |
| 77.10 | `test getrange` RED: `getrange a 99999999999 -1` → NOT_INTEGER (expected empty bulk) | `12a70d7d` `fix: getrange accept 64 bit offsets with clamping` | green | 404-421: 13/13 lines, both branch lines fully covered (clamp both directions) |
| 77.9 | `test geosearch and geosearchstore` RED: missing frommember returned `MultiBulkReply.EMPTY` (expected error) | `9607153d` `fix: geosearch error when frommember member missing` | green | error site 951-954 covered |
| 77.6 | RED: withhash element was BulkReply (expected IntegerReply) | `6458e0c8` `fix: geosearch withhash reply integer type` | green | 1083-1086 both branches covered — also closed the pre-existing uncovered-withhash gap at 1084 |
| 77.8 | — | — | — | **won't fix — product decision (user, 2026-10-27)**: dst stays geo-typed; documented divergence below |

Final class-level confirmation: `./gradlew :cleanTest :test --tests "io.velo.command.GGroupTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.acl.RCmdTest" --tests "io.velo.acl.UTest"` → BUILD SUCCESSFUL (13s).

Environment note: `:compileTestGroovy` intermittently failed on the *first* build after editing `GGroup.java` (~5 occurrences, incl. the RED runs) and passed on immediate retry every time — same daemon-race symptom as the "gradle daemon heap" follow-up recorded in bug_73. No production impact; flagging for the next infra pass.

## Re-review (AI agent 2)

<per-commit verification, repeat as needed>

## Residual Risk

- **77.8 — won't fix (product decision, user 2026-10-27)**: `GEOSEARCHSTORE` (without STOREDIST) keeps storing the
  destination as a Velo geo-type key (`SP_TYPE_GEO`). Redis documents the destination as a sorted set that can be
  read with `ZRANGE`/`ZCARD`/`ZSCAN`; in Velo those commands reply WRONGTYPE on such a key (Velo `GEO*` commands can
  still read it). The `// not sorted, todo` marker at the store site remains as the acknowledged gap. Revisit if a
  client workflow needs zset post-processing of stored results.
- **Geo distance value divergence (reviewer observation 2, mechanism corrected by author)**: Velo computes Haversine
  on the exact stored coordinates; Redis computes the same Haversine on geohash-decoded (52-bit quantized)
  coordinates. Result: ~0.1 m level digit differences (e.g. 166274.2578 vs 166.2742 in km). Not fixed — matching
  Redis would require quantizing stored points, a storage-format change.
- **`getex`/`expire` on big-string keys share one pre-existing pattern**: `getCv()` inlines the big-string payload,
  so a TTL update re-stores the whole value inline and orphans the big-string file. 77.1's fix aligned getex with
  EGroup.expire (`putToOneSlot`), which preserves type markers, but the big-string inline duplication itself is a
  cross-cutting issue in `getCv()` — out of scope here, recorded for a persist-layer round.
- **Async exception sub-branches** in `geosearch`/`geosearchstore` promise wrappers (`t != null` → setException) are
  uncovered — identical pre-existing pattern in the two older async blocks; forcing an asyncRun failure is not cheap
  in the current mock setup.
- **GETRANGE offsets are clamped to the value length** like Redis, but the raw parse is still bounded by
  `Long.parseLong` — non-numeric input replies NOT_INTEGER, matching Redis.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 77.1 | String commands skip type check (GETDEL/GETRANGE/GETSET/GETEX, plus GETBIT) | High | Redis protocol compatibility / Correctness | `CONFIRMED (reproduced)` | `fixed` (`b720ac57`) |
| 77.2 | GETEX expire arithmetic overflow | Medium | Edge case | `CONFIRMED (reproduced)` | `fixed` (`876e7d54`) |
| 77.3 | GETSET missing key/value length checks | Medium | Edge case / Internal consistency (refined) | `CONFIRMED (reproduced)` + `REFINED` | `fixed` (`ab03c146`) |
| 77.4 | GEOADD accepts NaN coordinates (NaN only; infinity already rejected) | Medium | Edge case (refined) | `CONFIRMED (reproduced)` + `REFINED` | `fixed` (`0d0958c9`) |
| 77.5 | Geo double replies via String.valueOf | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` + `REFINED` | `fixed` (`c279c722`) |
| 77.6 | WITHHASH bulk vs integer reply | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | `fixed` (`6458e0c8`) |
| 77.7 | GEOSEARCHSTORE leaves stale dst on empty | Medium | Redis protocol compatibility / Correctness | `CONFIRMED (reproduced)` | `fixed` (`010cc912`) |
| 77.8 | GEOSEARCHSTORE dst not a zset | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | `won't fix` — product decision |
| 77.9 | FROMMEMBER missing member: empty vs error | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | `fixed` (`9607153d`) |
| 77.10 | GETRANGE 32-bit offsets | Low | Edge case | `CONFIRMED (reproduced)` | `fixed` (`12a70d7d`) |
