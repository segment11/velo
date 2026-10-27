# Bug 75 — EGroup Commands Review (Round 1)

**Module:** `io.velo.command.EGroup` (echo / exists / expire / expireat / expiretime / extend; `expire()`/`expiretime()` are also reused by PGroup for pexpire/pexpireat/pexpiretime)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete — 6/6 confirmed, 6 fixed (8 commits: `bad9cf26`, `ff6c55c2`, `37ab2b4d`, `ee014c93`, `9bf87c44`, `e20e5aa5`, `71c87daa`, `126e013f`); re-review pending
**Review branch:** `review/egroup`
**Reviewed commit:** `5a0c1529997ea2aeb960b78417204f14f5aeff8d`

## Scope

Files read (method + line ranges):
- `src/main/java/io/velo/command/EGroup.java — static block/CommandRegistry (23-72), parseSlots() (93-125), handle() (132-162), exists() (164-229), expire() (231-292), expiretime() (294-311), extend() (313-319)`
- `src/main/java/io/velo/BaseCommand.java — addToSlotWithKeyHashList() (75-84), slot() (620-650), getExpireAt() (713-724), setCv() (950-977), putToOneSlot() (1207-1238, readonly fence at 1220), exists() (1269-1281)`
- `src/main/java/io/velo/CompressedValue.java — NO_EXPIRE/KEY_MAX_LENGTH constants (20, 93)`
- `src/main/java/io/velo/persist/OneSlot.java — getFromWal() (1814-1834), exists() (1848-1864)`
- `src/main/java/io/velo/persist/Wal.java — V record, isExpired() (61-76)`
- `src/main/java/io/velo/command/PGroup.java — pexpire/pexpireat/pexpiretime delegation (224-240)`
- `src/test/groovy/io/velo/command/EGroupTest.groovy — full file (1-343), incl. GT-on-no-TTL block (256-270)`

Reference docs (ground truth):
- Redis 7.2.14 source, local at `~/ws/redis/src/expire.c` — `parseExtendedExpireArgumentsOrReply()` (489-529), `expireGenericCommand()` (542-666), GT branch incl. "current_expire == -1 → always fail GT" (608-616), LT branch (619-627), overflow checks (566-577), `checkAlreadyExpired()` (462-471)
- Redis 7.2.14 source, `~/ws/redis/src/networking.c` — `addReplyErrorExpireTime()` (685-687): "invalid expire time in '%s' command"
- https://redis.io/commands/expire/ — "A non-volatile key is treated as an infinite TTL for the purpose of GT/LT"; options are oneof NX|XX|GT|LT
- `doc/design/04_command_processing_design.md`, `doc/design/19_command_registry_design.md` (no expire-specific guidance found — grep for expire/exists returned nothing)
- Prior-session Redis 7.2.11 differential capture: `doc/bug_reviews/logs/20261022_183000_egroup_differential_redis72_bug_75.log` (redis-cli transcript; Velo side not captured)

Prior rounds on this module: none (STATE.md has no EGroup entry).

## Finding 1 (Bug 75.1): GT option on a key with no TTL wrongly sets the expiry (must be a no-op)

**Severity:** High
**Class:** Redis protocol compatibility / Correctness
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

**Reviewer note (agent 2):** reproduced in-process (`Bug75ReviewTempSpec`) and end-to-end against a live Velo server
(`expire k 100 gt` → Velo `1` / TTL 99 vs Redis 7.2.14 `0` / TTL -1). The *fix direction* in this finding is **refined**:
dropping the `!= NO_EXPIRE` clause from the GT guard is **not** sufficient — with `expireAtExist == 0` the remaining
condition `expireAtExist >= expireAt` evaluates to `0 >= expireAt` → false → the guard still does not fire and the expiry is
still set. The guard has to become an explicit early return:
`if (isGt && expireAtExist == CompressedValue.NO_EXPIRE) { return IntegerReply.REPLY_0; }`
The existing test `EGroupTest.groovy:265-270` asserts the buggy `REPLY_1` and must be updated in the same change.

### Cited files / lines
```java
// EGroup.java:280-282 — the NO_EXPIRE guard makes the GT condition unreachable for persistent keys
if (isGt && expireAtExist != CompressedValue.NO_EXPIRE && expireAtExist >= expireAt) {
    return IntegerReply.REPLY_0;
}
```
```c
/* ~/ws/redis/src/expire.c:608-616 — Redis 7.2.14 */
/* GT option is set, check current expiry */
if (flag & EXPIRE_GT) {
    /* When current_expire is -1, we consider it as infinite TTL,
     * so expire command with gt always fail the GT. */
    if (when <= current_expire || current_expire == -1) {
        addReply(c,shared.czero);
        return;
    }
}
```

### Root cause
Redis treats a non-volatile key as having infinite TTL, so `GT` (only-extend) can never succeed against a persistent key — it replies 0 and leaves the key persistent. Velo's condition short-circuits on `expireAtExist != NO_EXPIRE`, so for a persistent key the guard is skipped entirely and execution falls through to `cv.setExpireAt(expireAt)` + `setCv(...)` (EGroup.java:287-291), returning 1.

### Trigger / impact
`SET k v; EXPIRE k 100 GT` → Velo returns 1 and makes the key volatile (TTL 100); Redis 7.2 returns 0 and TTL stays -1 (also confirmed by the differential log lines 6-11). Blast radius: any client using GT to "only extend expiry" silently converts persistent keys into expiring ones → premature data deletion. Same defect reachable via `PEXPIRE/EXPIREAT/PEXPIREAT ... GT` (PGroup delegates to this method, PGroup.java:224-234). Note: the existing test encodes the wrong semantics — EGroupTest.groovy:265-270 asserts `REPLY_1` for GT on `NO_EXPIRE` and must be updated together with the fix.

### Suggested fix direction
For `isGt`, return 0 when `expireAtExist == CompressedValue.NO_EXPIRE` (drop the `!= NO_EXPIRE` clause from the GT guard). LT is already correct (infinite TTL → any new expiry is less → set), keep it unchanged.

## Finding 2 (Bug 75.2): Unknown expire option is silently ignored instead of a syntax error

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

**Reviewer note (agent 2):** reproduced in-process and end-to-end (Velo `expire k 10 FOO` → `1`, TTL 9; Redis 7.2.14 →
`ERR Unsupported option FOO`, TTL -1). One extra divergence found while verifying: Redis parses the **option tokens before**
the integer argument (`expire.c:557-562` calls `parseExtendedExpireArgumentsOrReply()` first, `getLongLongFromObjectOrReply()`
after), so `EXPIRE k abc FOO` answers `ERR Unsupported option FOO` in Redis, while Velo answers `not integer` (Velo parses the
integer first, `EGroup.java:236-243`). Not a separate finding — the fix should validate the option token before/independently of
the integer so that both orders match Redis.

### Cited files / lines
```java
// EGroup.java:257-264 — any unrecognized data[3] leaves all flags false
if (data.length == 4) {
    var typeBytes = data[3];
    var type = new String(typeBytes);
    isNx = "nx".equalsIgnoreCase(type);
    isXx = "xx".equalsIgnoreCase(type);
    isGt = "gt".equalsIgnoreCase(type);
    isLt = "lt".equalsIgnoreCase(type);
}
```
```c
/* ~/ws/redis/src/expire.c:516-521 — Redis 7.2.14 */
} else {
    addReplyErrorFormat(c, "Unsupported option %s", opt);
    return C_ERR;
}
```

### Root cause
`EXPIRE key 10 FOO` (or a typo like `NG`, `XX `, or a number) parses to all-false flags and executes as an unconditional EXPIRE, returning 1 and setting the TTL. Redis rejects with `ERR Unsupported option FOO` (differential log lines 12-13) and leaves the TTL untouched.

### Trigger / impact
Typos in client code (e.g. sending lowercase-then-uppercase variants, or an extra positional arg) silently succeed and mutate TTL instead of erroring. Protocol-compat break for any client that relies on error replies to catch bad arguments. Secondary (cosmetic) divergence in the same parse block: Redis parses any number of option tokens and returns specific combo errors ("NX and XX, GT or LT options at the same time are not compatible", expire.c:523-530), while Velo rejects `data.length == 5` wholesale with `ErrorReply.FORMAT` — acceptable since both error, but note Redis accepts repeated same-flag tokens (`EXPIRE k 10 NX NX` is valid in Redis) where Velo errors.

### Suggested fix direction
Validate `data[3]`: if none of nx/xx/gt/lt matches (case-insensitive), return a syntax error (Redis text: `ERR Unsupported option <arg>`). Keep the `data.length > 4` FORMAT rejection; do not bother implementing multi-token parsing.

## Finding 3 (Bug 75.3): No overflow/range check on expire timestamp — silent key deletion via long overflow

**Severity:** Medium
**Class:** Edge case / Correctness
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

**Reviewer note (agent 2):** reproduced in-process and end-to-end. Live Velo: `expire k 9223372036854775807` → `1`,
then `ttl k` → `-2` and `exists k` → `0` — the key is **silently deleted** (silent data loss). Redis 7.2.14 → 
`ERR invalid expire time in 'expire' command`, key alive. `pexpire` has the same defect
(`now + LLONG_MAX` wraps to a negative `expireAt` → key gone).
The scope stated in this finding is **refined**: the overflow does **not** affect `PEXPIREAT` (absolute milliseconds — no
multiplication and no base-time addition), where Redis 7.2.14 also accepts `LLONG_MAX`
(`PEXPIREAT k 9223372036854775807` → `1`). Affected: `EXPIRE`, `EXPIREAT` (seconds → `* 1000`), `PEXPIRE`
(ms → `now + ms`). Redis also rejects the negative overflow `EXPIRE k -9223372036854775808` (`when < LLONG_MIN/1000`),
which Velo currently accepts (and turns into an immediate expiry) — include it in the fix.
Note the exact Redis thresholds when implementing: `when > LLONG_MAX/1000 || when < LLONG_MIN/1000` (seconds only) and
`when > LLONG_MAX - basetime`; `basetime` is `now` for the relative variants and `0` for the `*AT` variants.
Live evidence: `EXPIRE k 9223372036854775` is also rejected by Redis (adding `basetime` would overflow) while Velo accepts it.

### Cited files / lines
```java
// EGroup.java:245-250 — unguarded arithmetic
long expireAt;
if (isMilliseconds) {
    expireAt = isAt ? seconds : System.currentTimeMillis() + seconds;
} else {
    expireAt = isAt ? seconds * 1000 : System.currentTimeMillis() + seconds * 1000;
}
```
```java
// Wal.java:71-73 — negative expireAt is treated as already expired
boolean isExpired() {
    return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
}
```
```c
/* ~/ws/redis/src/expire.c:566-577 — Redis 7.2.14 */
if (unit == UNIT_SECONDS) {
    if (when > LLONG_MAX / 1000 || when < LLONG_MIN / 1000) {
        addReplyErrorExpireTime(c);
        return;
    }
    when *= 1000;
}
if (when > LLONG_MAX - basetime) {
    addReplyErrorExpireTime(c);
    return;
}
```

### Root cause
`EXPIRE key 9223372036854775807` parses fine (`Long.parseLong`) but `seconds * 1000` overflows `long` and wraps negative. The negative `expireAt` is stored via `setCv`, reply is 1; on the next read `Wal.V.isExpired()` (or `KeyLoader` expiry) sees `expireAt != 0 && expireAt < now` → the key vanishes. Redis instead replies `ERR invalid expire time in 'expire' command` and leaves the key intact (differential log lines 18-22 show both `9223372036854775807` and `9223372036854775` rejected for EXPIRE).

### Trigger / impact
Any client passing a huge TTL/EXPIREAT value (unit mismatch bugs — e.g. sending milliseconds to EXPIREAT — routinely produce values ≥ 9.2e15): Velo returns success and then the key is silently gone; TTL/EXPIRETIME afterward return -2. Silent data loss instead of an error reply. Affects expire/expireat/pexpire/pexpireat (all share `expire()`).

### Suggested fix direction
Before computing, mirror Redis bounds: reject when `seconds > Long.MAX_VALUE / 1000` (or `/ 1` for ms variants) or the post-multiplication/post-addition result would overflow (`when > Long.MAX_VALUE - basetime`), with error text `ERR invalid expire time in '<cmd>' command`. Use `Math.multiplyExact`/`addExact` or explicit checks; keep the reply an error and leave TTL unchanged.

## Finding 4 (Bug 75.4): EXPIREAT/PEXPIREAT 0 makes the key persistent instead of deleting it (0-sentinel collision)

**Severity:** Medium
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

**Reviewer note (agent 2):** reproduced in-process and end-to-end. Live Velo: `expireat k 0` → `1`, then `exists k` → `1`,
`ttl k` → `-1`, `expiretime k` → `-1` — the key became **immortal**. Redis 7.2.14: `expireat k 0` → `1`, `exists k` → `0`,
`ttl k` → `-2`, `expiretime k` → `-2` — the key is deleted. `PEXPIREAT k 0` shows the same defect (Velo keeps the key,
Redis deletes it). The "minimal diff" option proposed in the fix direction (store `now - 1`) is acceptable, but note that
`now - 1` can itself be `0` only in 1970 — not a practical concern; a delete path (`remove(slotWithKeyHash)`, like
`PGroup.persist()`'s sibling path) is closer to Redis because the key disappears immediately instead of lazily.

### Cited files / lines
```java
// EGroup.java:288 + CompressedValue.java:20 — stored expireAt equals the "no expire" sentinel
cv.setExpireAt(expireAt);            // expireAt == 0 for EXPIREAT key 0 / PEXPIREAT key 0
// CompressedValue.java:20
public static final long NO_EXPIRE = 0;
```
```java
// Wal.java:71-73 — expireAt == 0 is never expired
boolean isExpired() { return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis(); }
```
```c
/* ~/ws/redis/src/expire.c:462-471 + 638-648 — Redis 7.2.14: past timestamp => real DEL */
int checkAlreadyExpired(long long when) {
    return (when <= commandTimeSnapshot() && !server.loading && !server.masterhost);
}
...
if (checkAlreadyExpired(when)) {
    int deleted = dbGenericDelete(...);
    ... addReply(c, shared.cone); return;
}
```

### Root cause
`EXPIREAT key 0` computes `expireAt = 0 * 1000 = 0`, which is bit-identical to `CompressedValue.NO_EXPIRE`. The write is performed (returns 1), but the persistence layer interprets `expireAt == 0` as "persistent": `isExpired()` can never be true, `TTL` returns -1, `EXPIRETIME` returns -1. Redis semantics: an absolute timestamp of 0 is in the past → the key is deleted (docs: "EXPIREAT/PEXPIREAT with a time in the past will result in the key being deleted rather than expired").

### Trigger / impact
`SET k v; EXPIREAT k 0` → Velo: returns 1, `TTL k` → -1 (key immortal, value kept forever); Redis 7.2: returns 1, `EXISTS k` → 0. Divergence converts an intended deletion into an un-expiring key (storage leak / resurrected data). Note only the exact value 0 is affected; other past timestamps (including negatives) work because they are `!= NO_EXPIRE` and `< now`.

### Suggested fix direction
In `expire()`, after computing `expireAt`: if it is in the past or exactly 0, either perform the delete path (`remove(slotWithKeyHash)`, matching Redis `DEL` propagation) or store a 1-ms-in-the-past timestamp (`now - 1`, which is `!= 0` and `< now` → lazily expires identically to Redis). The minimal-diff option is the latter; the delete path is closer to Redis (immediate visibility to EXISTS is equivalent either way, since expired-on-read behaves as deleted).

## Finding 5 (Bug 75.5): EXISTS key-length validation differs between fast path and cross-worker path

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

**Reviewer note (agent 2):** reproduced in-process: fast path → `ErrorReply{message='key too long (max length is 256)'}`,
cross-worker path → `AsyncReply` (no error at all, plain count). The inconsistency is **unique to `EXISTS`** in the whole
command set: every other multi-key command hoists the key-length check **before** the `isCrossRequestWorker` branch —
`DGroup.del()` (`DGroup.java:277-283`, comment: *"check the key length before any remove, in all modes"*), `RGroup`
(`RGroup.java:309-312` before `:322`), `ZGroup`, `SGroup`, `LGroup`, `GGroup`, `TGroup`, `BGroup`.
The same fix was already merged for `DEL` in bug 74.7 (commit `f6b15320` *"fix: check the key length before remove in del"*).
Therefore the recommended direction (hoist the check before the branch) is confirmed and must follow the `DGroup.del()` pattern.

### Cited files / lines
```java
// EGroup.java:169-184 — fast path: per-key length check
var keyBytes = data[i];
if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
    return ErrorReply.KEY_TOO_LONG;
}
// EGroup.java:187-228 — cross-worker path: no length check at all
var groupBySlot = slotWithKeyHashListParsed.stream().collect(Collectors.groupingBy(SlotWithKeyHash::slot));
...
var isExists = exists(slotWithKeyHash);
```

### Root cause
The `isCrossRequestWorker` branch groups by slot and checks existence without ever validating `keyBytes.length > KEY_MAX_LENGTH` (256). The same request payload gets `ErrorReply.KEY_TOO_LONG` when all keys land on one worker, but a plain count (0 for the long key, non-error) when keys span multiple workers.

### Trigger / impact
`EXISTS <257-byte key> <key-in-another-slot>` → IntegerReply; `EXISTS <257-byte key>` alone → error. Inconsistent reply shape for identical input depending on slot layout; clients cannot rely on either behavior. (Redis itself has no such key-length limit, so the cross path is arguably the more compatible one — the inconsistency is the bug.)

### Suggested fix direction
Pick one: either hoist the length check before the branch (error consistently, matches existing KEY_TOO_LONG tests), or drop it from the fast path for EXISTS specifically (long keys simply do not exist → count 0, closer to Redis). Recommend hoisting for minimal semantic change and consistency with `expire`'s key handling.

## Finding 6 (Bug 75.6): EXPIRE destroys the type marker of a structured value (hash/list/set/zset → string)

*(Added by AI agent 2 during the round-1 verification. The author's own temp spec `Bug75TempSpec` contained a probe
"75.1 expire on a hash key destroys the hash type", but the finding was never written into the report.)*

**Severity:** High
**Class:** Correctness / data corruption
**Status:** `CONFIRMED (reproduced)` — reviewer round 1

### Cited files / lines
```java
// EGroup.java:287-290 — the expire write path re-writes the value through setCv()
cv.setSeq(snowFlake.nextId());
cv.setExpireAt(expireAt);

setCv(cv, slotWithKeyHash);
```
```java
// BaseCommand.java:950-977 — setCv() drops the type marker for every structured type
public void setCv(CompressedValue cv, @NotNull SlotWithKeyHash slotWithKeyHash) {
    if (cv.isTypeNumber()) {
        setNumber(cv.numberValue(), slotWithKeyHash, cv.getExpireAt());
    } else {
        cv.setSeq(snowFlake.nextId());
        cv.setKeyHash(slotWithKeyHash.keyHash());
        if (cv.isCompressed()) {                 // dictSeqOrSpType > NULL_DICT_SEQ
            putToOneSlot(slot, slotWithKeyHash, cv);
        } else {
            // spType 0 == plain string -> the structure marker (negative spType) is lost
            set(cv.getCompressedData(), slotWithKeyHash, 0, cv.getExpireAt());
        }
    }
}
```
```java
// CompressedValue.java:587-589 — isCompressed() is false for every structure marker
public boolean isCompressed() {
    return dictSeqOrSpType > NULL_DICT_SEQ;     // SP_TYPE_HH=-512, SP_TYPE_LIST=-2048, SP_TYPE_SET=-4096, ...
}
```
```java
// PGroup.java:294-312 — PERSIST does it right: it writes the cv back unchanged
cv.setExpireAt(CompressedValue.NO_EXPIRE);
putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);
```

### Root cause
All structured types are stored with a **negative** `spType` marker (`SP_TYPE_HH`, `SP_TYPE_HASH`, `SP_TYPE_LIST`,
`SP_TYPE_SET`, `SP_TYPE_ZSET`, `SP_TYPE_GEO`, `SP_TYPE_STREAM`, `SP_TYPE_BLOOM_BITMAP`, `SP_TYPE_HLL`).
`CompressedValue.isCompressed()` is `dictSeqOrSpType > NULL_DICT_SEQ`, so for every structure the `setCv()` branch
`if (cv.isCompressed())` is **false** and `setCv()` re-writes the payload with `spType = 0` (plain string). `EXPIRE` /
`EXPIREAT` / `PEXPIRE` / `PEXPIREAT` therefore convert the key into a string. `PERSIST` is unaffected because it uses
`putToOneSlot()` directly.

### Trigger / impact
Live Velo server (`build/libs/velo-1.0.0.jar`, port 41500) vs Redis 7.2.14 (port 7799):
```
$V hset h f1 v1 f2 v2 ; $R hset h f1 v1 f2 v2
Velo : type before => hash    | expire h 100 => 1 | type after => string | hgetall h => ERR wrong type
Redis: type before => hash    | expire h 100 => 1 | type after => hash   | hgetall h => f1 v1 f2 v2
```
Same for every structure (measured end-to-end):
```
Velo : l=list  s=set  z=zset  h=hash   ->  after expire: l=string s=string z=string h=string
Redis: l=list  s=set  z=zset  h=hash   ->  after expire: l=list   s=set   z=zset   h=hash
```
Impact: silent data-type corruption. The payload bytes are still there, but every type-specific command
(`HGETALL`/`LPOP`/`SMEMBERS`/`ZRANGE`…) now answers `ERR wrong type` (Velo's `TypeMismatchException`), so the client sees
its hash/list/set as a string. This is far more damaging than a wrong reply code: it silently changes the data model of the
key. It is also replicated (`EXPIRE` is propagated as `PEXPIREAT`), so a replica corrupts the type as well.

### Suggested fix direction
Do not re-encode the value when only the TTL changes. Write the `cv` back unchanged, like `PGroup.persist()` does:
```java
cv.setSeq(snowFlake.nextId());
cv.setExpireAt(expireAt);
putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);
```
(`putToOneSlot` is `protected` in `BaseCommand` and `EGroup` extends `BaseCommand`, so it is reachable.)
Careful: `setCv()` also refreshes `cv.setKeyHash(...)` and pushes the key-analysis index; the cv read by `getCv()`
already carries the right key hash, and the index handler entry only tracks the value length, which does not change here.
The same helper should be reused by the 75.4 fix (delete path) — a deleted key must not be re-encoded either.

## Notes / non-findings

- NX/XX semantics match Redis exactly (EGroup.java:274-279 vs expire.c:592-607); covered by existing tests (EGroupTest.groovy:212-245).
- GT/LT with an existing TTL are equivalent: Velo `expireAtExist >= expireAt` (GT) / `<=` (LT) ≡ Redis `when <= current` / `when >= current` (EGroup.java:280-285 vs expire.c:611, 623).
- LT on a key with no TTL correctly sets the expiry (infinite TTL → any new expiry is less) — EGroupTest.groovy:290-295 asserts REPLY_1, matching expire.c:620-627.
- Negative relative TTL (`EXPIRE key -1`): Velo stores `now - 1000` → lazily expired on read; Redis does a real DEL returning 1 (differential log lines 24-27). Observable behavior equivalent (EXISTS → 0 both sides). Not reported as a bug.
- EXPIRETIME: -2 for missing key / -1 for no TTL / `expireAt / 1000` truncation all match Redis (EGroup.java:294-311; test 298-342). PEXPIRETIME via PGroup delegation returns ms ✓.
- ECHO: arity 2 enforced, BulkReply echo ✓ (EGroup.java:134-139).
- EXISTS counts duplicate keys per occurrence (loop over data / groupingBy keeps duplicates) — matches Redis `EXISTS k k` → 2.
- The 5-arg `EXPIRE key 10 NX XX` case: Velo FORMAT error vs Redis combo-specific error message — both error, text-only difference; not reported (see 75.2 note).
- EXPIRE write path is readonly-fenced: `setCv` → `putToOneSlot` → `oneSlot.isReadonly() || isRoleTransitionWriteFenced()` throws (BaseCommand.java:1220) — no readonly-slot bypass.
- `expire()`/`expiretime()` `slotWithKeyHashListParsed.getFirst()` cannot NPE/IndexOutOfBounds: handle() validates `data.length` 3/4 (2 for expiretime) before reaching them, and parseSlots adds one entry whenever `data.length >= 2`.
- AsyncReply error propagation in cross-worker EXISTS sets the exception on the final promise (EGroup.java:209-214) — OK.
- "CLIENT REPLY OFF bypass for PING/ECHO" is already tracked as an open item in bug_73 (STATE.md) — intentionally not re-reported here.
- Reviewer addition: `PEXPIREAT` with `LLONG_MAX` is accepted by Redis 7.2.14 (`1`) and by Velo (no multiplication, no base-time
  addition) — the only overflow-free variant; the 75.3 fix must not reject it.
- Reviewer addition: Redis parses the option tokens **before** the integer argument (`expire.c:557-562`), Velo the other way
  round — a one-line ordering divergence, folded into the 75.2 fix direction rather than reported separately.
- Reviewer addition: `EXPIRE` on a **number**-typed key goes through `setCv()` → `setNumber()` (`BaseCommand.java:951-952`) and
  keeps the number type — only the *structured* types (negative `spType`) lose their marker (see 75.6).

## Reviewer checklist (for AI agent 2)

- Finding 1 (75.1): Spock temp spec with `InMemoryGetSet` bypass: put persistent cv (`expireAt = NO_EXPIRE`), run `expire(true, true)` (or execute `'expire a 100 gt'`-style data) → expect reply 1 (bug) vs 0 (Redis); assert TTL/`getExpireAt` afterwards — bug: `> 0`; Redis: still NO_EXPIRE. Differential: `SET k v; EXPIRE k 100 GT; TTL k` → Redis 7.2: `0` then `-1`. Note EGroupTest.groovy:265-270 currently asserts the buggy `1`.
- Finding 2 (75.2): data4 = [expire, key, ttl, 'FOO'.bytes] → Velo returns REPLY_1 (bug); Redis: `ERR Unsupported option FOO`. Also try `'ng'`.
- Finding 3 (75.3): `'9223372036854775807'.bytes` as data[2] for expire(false,false) and expire(true,false) → Velo REPLY_1 + subsequent exists → 0 (bug); Redis errors `invalid expire time in 'expire' command` and TTL unchanged. JDK repro: `Long.parseLong("9223372036854775807") * 1000` wraps negative.
- Finding 4 (75.4): put cv for key 'a', run expire(true,false) with `'0'.bytes` → returns 1; then `expiretime a` → bug: -1 (persistent); Redis: key deleted (EXISTS → 0). Also `'0'.bytes` with expire(true,true) (PEXPIREAT 0).
- Finding 5 (75.5): execute `'exists <257-byte key>'` with `crossRequestWorker=false` → ErrorReply.KEY_TOO_LONG; same key with a second key mapped to another slot and `crossRequestWorker=true` → IntegerReply without error (use the eventloop scaffolding from EGroupTest.groovy:125-147).

## Review Feedback (AI agent 2, round 1)

**Method:** live Redis 7.2.14 differential (source `~/ws/redis` @ tag `7.2.14`, server on port 7799) + live Velo server
(`build/libs/velo-1.0.0.jar`, port 41500, temp dir `/tmp/opencode/velo75`) + Spock temp spec
(`src/test/groovy/io/velo/command/Bug75ReviewTempSpec.groovy`, green — it documents the *current buggy* behaviour) +
static inspection of the cited lines. All six findings reproduced.
Full transcript: `doc/bug_reviews/logs/20261027_102500_egroup_review_agent2_differential_bug_75.log`
(Velo line first, Redis line second, in every pair).

**Reviewer scope additions (AI agent 2):** `src/main/java/io/velo/command/PGroup.java` (persist 294-312, pexpire/pexpireat
delegation 224-240, parseSlots 159-174), `src/main/java/io/velo/command/DGroup.java` (del 273-295 — key-length check
precedent), `src/main/java/io/velo/command/RGroup.java` (305-325), `src/main/java/io/velo/CompressedValue.java`
(isCompressed 587-589, spType constants 55-95, isExpired 569-570), `src/main/java/io/velo/reply/IntegerReply.java`
(REPLY_0/REPLY_1 are anonymous replies, not `IntegerReply` instances), `src/main/java/io/velo/persist/OneSlot.java`
(exists 1845-1870, getFromWal 1814-1834), `src/main/java/io/velo/persist/Wal.java` (V record 61-76),
`src/main/java/io/velo/BaseCommand.java` (set 1015-1200, setCv 950-977, putToOneSlot 1207-1225, remove 1233-1243,
exists 1269-1281, get/getCv 734-868), `~/ws/redis/src/expire.c` (462-666), `~/ws/redis/src/networking.c` (685-687),
commit `f6b15320` (bug 74.7 `DEL` key-length precedent), `src/test/groovy/io/velo/command/EGroupTest.groovy` (1-343),
`src/test/groovy/io/velo/command/Bug75TempSpec.groovy` (the author's temp spec, 1-150).
Reviewed commit: `5a0c1529997ea2aeb960b78417204f14f5aeff8d` (= HEAD at review time).

### Verdicts

| Finding | Verdict | Evidence |
|---|---|---|
| 75.1 | `CONFIRMED (reproduced)` | **Redis 7.2.14:** `expire k 100 gt` → `0`, `ttl k` → `-1`. **Velo (live):** `expire k 100 gt` → `1`, `ttl k` → `99`. **Velo (spec):** `75.1 reply=1 expireAt=1793067836015 (NO_EXPIRE=0)` — the key became volatile. Redis source `expire.c:608-616` (`current_expire == -1` → always fail GT) matches the observed Redis reply. Fix direction refined (see the reviewer note on 75.1 — dropping the `!= NO_EXPIRE` clause alone does not fix it). |
| 75.2 | `CONFIRMED (reproduced)` | **Redis 7.2.14:** `expire k 10 FOO` → `ERR Unsupported option FOO`, `ttl k` → `-1`; `expire k 10 ng` → `ERR Unsupported option ng`. **Velo (live):** `expire k 10 FOO` → `1`, `ttl k` → `9`. **Velo (spec):** `75.2 reply=1 expireAt=1793067746115`. Extra divergence: Redis validates the option token *before* the integer (`expire k abc FOO` → `ERR Unsupported option FOO`), Velo answers `not integer`. |
| 75.3 | `CONFIRMED (reproduced)` | **Redis 7.2.14:** `expire k 9223372036854775807` → `ERR invalid expire time in 'expire' command`, `ttl k` → `-1`, `exists k` → `1`; also `expire k 9223372036854775` → same error (basetime addition would overflow) and `expire k -9223372036854775808` → same error. **Velo (live):** `expire k 9223372036854775807` → `1`, then `ttl k` → `-2`, `exists k` → `0` — the key is silently deleted. **Velo (spec):** `75.3 reply=1 expireAt=1793067735118 now=1793067736120 (raw seconds*1000=-1000)`, `75.3 getCv after=null`; `75.3b` (PEXPIRE) `expireAt=-9223370243787039679`, `getCv after=null`. Scope refined: `PEXPIREAT` is **not** affected (Redis accepts `LLONG_MAX` there too). |
| 75.4 | `CONFIRMED (reproduced)` | **Redis 7.2.14:** `expireat k 0` → `1`, then `exists k` → `0`, `ttl k` → `-2`, `expiretime k` → `-2`. **Velo (live):** `expireat k 0` → `1`, `exists k` → `1`, `ttl k` → `-1`, `expiretime k` → `-1` — the key became immortal. **Velo (spec):** `75.4 reply=1 expireAt=0 (NO_EXPIRE=0)`, `75.4 expiretime=-1`, `75.4 getCv after=CompressedValue{... expireAt=0 ...}` (still alive); `75.4b` (PEXPIREAT 0) shows the same. |
| 75.5 | `CONFIRMED (reproduced)` | **Velo (spec):** `75.5 fast path reply=ErrorReply{message='key too long (max length is 256)'}` vs `75.5 cross path reply=AsyncReply` — the cross-worker path performs no length check at all. Confirmed unique to `EXISTS`: every other multi-key command hoists the check before the `isCrossRequestWorker` branch (`DGroup.java:277-283`, same fix already merged for `DEL` in bug 74.7 / commit `f6b15320`). |
| 75.6 | `CONFIRMED (reproduced)` | **Velo (live):** `hset h f1 v1 f2 v2` → `type h` = `hash`; `expire h 100` → `1`; `type h` = `string`; `hgetall h` → `ERR wrong type`. **Redis 7.2.14:** `type h` stays `hash`, `hgetall h` → `f1 v1 f2 v2`. Same for `list`/`set`/`zset` (`l=string s=string z=string h=string` in Velo vs `l=list s=set z=zset h=hash` in Redis). **Velo (spec):** `75.6 reply=1 spType before=-512 after=0` — the `SP_TYPE_HH` marker is dropped by `setCv()` → `set(..., spType=0, ...)`. |

### Additional observations

- **Fix 75.6 first.** It is the only finding with silent *data corruption* (a client's hash becomes a string), and the
  current `setCv()` write path is also the path that 75.3/75.4 have to touch. Fixing the TTL write path once
  (`putToOneSlot` with the untouched cv) removes the type corruption and keeps the type marker for the delete path of 75.4.
- **75.1 fix direction is wrong as written.** "drop the `!= NO_EXPIRE` clause from the GT guard" leaves
  `expireAtExist >= expireAt` → `0 >= expireAt` → false → the expiry is still set. An explicit early return is required.
- **`EGroupTest.groovy:265-270` encodes the buggy 75.1 behaviour** (`gt` + `NO_EXPIRE` → `REPLY_1`) and must be updated in the
  same commit as the 75.1 fix, otherwise the fix looks like a regression.
- **Redis option parsing happens before the integer parsing** (`expire.c:557-562`): `EXPIRE k abc FOO` → `ERR Unsupported
  option FOO`. Velo parses the integer first → `not integer`. Worth aligning while fixing 75.2 (one extra line, same code path).
- **Redis accepts repeated identical option tokens** (`expire k 10 NX NX` → `1`), Velo rejects any 5-arg form with
  `ErrorReply.FORMAT`. Recorded as a non-finding (both error out) — but if 75.2 is implemented with a token loop instead of a
  single-token check, this divergence disappears for free.
- **75.5 error text vs Redis:** Redis has no key-length limit at all (`exists <257-byte key>` → `0`). Velo errors on the fast
  path and counts on the cross path. The DGroup precedent (bug 74.7) is "error consistently", so keep `KEY_TOO_LONG` and hoist it.
- **`PERSIST` is not affected by 75.6** — `PGroup.persist()` writes the cv back with `putToOneSlot()` (PGroup.java:309-310).
  Use it as the template for the fix.
- The `EXPIRE` write path stays readonly-fenced after the fix: `putToOneSlot()` keeps the
  `oneSlot.isReadonly() || isRoleTransitionWriteFenced()` guard (BaseCommand.java:1220) — verified.
- Live Velo evidence was collected with `build/libs/velo-1.0.0.jar` (built 2026-10-22 16:29). The files touched by these
  findings (`BaseCommand.setCv/set`, `EGroup.expire`) were last modified in commit `2669f81f` (2026-10-10), i.e. before that
  jar — the jar therefore reflects the reviewed code. `Reviewed commit:` of this round is still `5a0c152`.

### Recommended fix order (for the fix agent)

1. **75.6** (High, data corruption) — replace the `setCv()` write in `EGroup.expire()` with a `putToOneSlot()` write of the
   untouched cv; add a regression test that stores a hash/list/set/zset cv, expires it and asserts the `spType` survives
   (`BaseCommand.getCv()` → `cv.isHash()` / `isList()` / …). Fixing this first also unblocks the 75.4 delete path.
2. **75.1** (High, GT on a no-TTL key) — explicit early return `if (isGt && expireAtExist == NO_EXPIRE) return REPLY_0;`;
   update `EGroupTest.groovy:265-270`.
3. **75.3** (Medium, overflow → silent key deletion) — mirror Redis' two checks (`LLONG_MAX/1000` / `LLONG_MIN/1000` for the
   seconds variants, `LLONG_MAX - basetime` for all) and answer `ERR invalid expire time in '<cmd>' command`.
4. **75.4** (Medium, `EXPIREAT 0` → immortal key) — delete path (or `now - 1`); must not re-encode the value (see 75.6).
5. **75.2** (Medium, unknown option ignored) — validate the option token, answer `ERR Unsupported option <arg>`.
6. **75.5** (Low, key-length check inconsistency) — hoist the check before the `isCrossRequestWorker` branch, mirroring
   `DGroup.del()` (bug 74.7 / `f6b15320`).

One bug = one commit, TDD first (failing regression test before the production change), JaCoCo confirmation on the changed
lines, per `AGENTS.md`.

## Response (AI agent 1, round 1)

**Method:** static re-inspection of every reviewer-added citation (`DGroup.del()`, `CompressedValue` type constants,
`setCv()`/`putToOneSlot()`, `PGroup.persist()`, `expire.c:555-565`) + reading the reviewer's transcript
`logs/20261027_102500_egroup_review_agent2_differential_bug_75.log` and `Bug75ReviewTempSpec.groovy`. No re-testing needed —
the reviewer's evidence is pasted and the code citations check out.

| Finding | Response | Note |
|---|---|---|
| 75.1 | AGREE with verdict + **accept the fix-direction refinement** | My suggested fix ("drop the `!= NO_EXPIRE` clause") was wrong: for a persistent key the remaining `expireAtExist >= expireAt` evaluates `0 >= expireAt` → false for any positive expiry, so the guard still does not fire and the TTL is still set. The reviewer's explicit early return `if (isGt && expireAtExist == CompressedValue.NO_EXPIRE) return IntegerReply.REPLY_0;` is required. |
| 75.2 | AGREE, incl. the parsing-order addition | Verified `expire.c:555-565`: `parseExtendedExpireArgumentsOrReply()` runs before `getLongLongFromObjectOrReply()`, so `EXPIRE k abc FOO` must answer "Unsupported option", not "not integer". Will fold the ordering into the fix (validate the option token before parsing the integer). |
| 75.3 | AGREE with the scope refinement | Verified: PEXPIREAT (`isAt && isMilliseconds`) computes `expireAt = seconds` with no arithmetic (EGroup.java:247), and Redis 7.2.14 skips both the `/1000` check (ms unit) and `> LLONG_MAX - 0` for it — `PEXPIREAT LLONG_MAX` is legitimately accepted. Fix must cover EXPIRE/EXPIREAT (`*1000`) and PEXPIRE (`now +`), including the negative bound (`seconds < Long.MIN_VALUE / 1000`), and must NOT reject PEXPIREAT `LLONG_MAX`. |
| 75.4 | AGREE | Prefer the reviewer's delete-path option: after 75.6 lands, `expire()` no longer needs `setCv()` at all, so `remove(slotWithKeyHash)` (no re-encode, immediate, matches Redis DEL propagation) is both closer to Redis and the smaller diff. |
| 75.5 | AGREE | Verified the precedent: `DGroup.del()` hoists the length check before the branch with comment "check the key length before any remove, in all modes" (DGroup.java:277-283); same pattern merged for DEL in bug 74.7 (`f6b15320`). Will mirror it for EXISTS. |
| 75.6 | AGREE — accept as finding 75.6 | Independently verified the chain: `NULL_DICT_SEQ = 0` and `SP_TYPE_HH/-512 … SP_TYPE_ZSET/-8192` are negative (CompressedValue.java:30, 74-82); `isCompressed()` is `dictSeqOrSpType > NULL_DICT_SEQ` (CompressedValue.java:587-589) → false for every structured type; `setCv()` then calls `set(cv.getCompressedData(), slotWithKeyHash, 0, cv.getExpireAt())` with hardcoded spType 0 (BaseCommand.java:972-974) → marker destroyed. `PGroup.persist()` proves the correct pattern (`putToOneSlot(slot, slotWithKeyHash, cv)`, PGroup.java:309-310) and `putToOneSlot` is `protected` (BaseCommand.java:1207/1215), reachable from EGroup. Embarrassing miss on my side — the probe was in my own prior-session temp spec (`Bug75TempSpec.groovy`) but I never wrote the finding. |

No retractions — all 5 original findings were confirmed. Fix order accepted as recommended: **75.6 → 75.1 → 75.3 → 75.4 → 75.2 → 75.5**
(75.6 first because 75.4's delete path depends on the write-path rework). Ready to start the fix phase (TDD, one bug = one commit).

## Fix Log (AI agent 1, round 1 fix phase)

TDD for every bug: the reviewer's temp spec (`Bug75ReviewTempSpec`, scratch file, removed after the permanent tests were
written) was rebuilt as a **permanent failing regression test** first, watched failing, then the minimal production change
was made. One bug = one commit. JaCoCo was inspected for every changed line/branch
(`scripts/jacoco_cover.py io.velo.command.EGroup <start> <end>`).

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 75.6 | `EGroupTest.test expire` — `inMemoryGetSet.getBuf(slot, 'a', 0, 0).cv().dictSeqOrSpType == CompressedValue.SP_TYPE_HH` → **FAILED**, actual `dictSeqOrSpType=0` (the `SP_TYPE_HH` marker was dropped, the key became a plain string) | `bad9cf26` | `./gradlew :test --tests "io.velo.command.EGroupTest" --tests "io.velo.command.PGroupTest"` → BUILD SUCCESSFUL | `io.velo.command.EGroup` 287-293 → 7/7 lines covered, all branches covered |
| 75.1 | `EGroupTest.test expire` — GT on a key without TTL: `reply == IntegerReply.REPLY_0` → **FAILED**, actual `REPLY_1` (the key became volatile) | `ff6c55c2` | same focused run → BUILD SUCCESSFUL | `io.velo.command.EGroup` 280-283 → all 6 branches covered (GT with/without TTL) |
| 75.3 | `EGroupTest.test expire` — `expire a 9223372036854775807` → **FAILED**, actual `REPLY_1` (silent key deletion); `PGroupTest.test pexpire` — `pexpire a 9223372036854775807` → **FAILED**, actual `REPLY_1` | `37ab2b4d` | `./gradlew :test --tests "io.velo.command.EGroupTest" --tests "io.velo.command.PGroupTest" --tests "io.velo.reply.RESPReplyTest"` → BUILD SUCCESSFUL | `io.velo.command.EGroup` 245-262 → all branches covered (seconds/ms, over/under flow, `*at` vs relative) |
| 75.4 | `EGroupTest.test expire` — `expireat a 0` → **FAILED**, the key was still alive (`expireAt=0`, `expiretime` → -1); `PGroupTest.test pexpireat` — `pexpireat a 0` → **FAILED**, same | `ee014c93` (+ follow-up `9bf87c44`) | same focused run → BUILD SUCCESSFUL | `io.velo.command.EGroup` 294-299 → all branches covered (delete path) |
| 75.2 | `EGroupTest.test expire` — `expire a 10 FOO` → **FAILED**, actual `REPLY_1` (the TTL was set instead of an error) | `e20e5aa5` | same focused run → BUILD SUCCESSFUL | `io.velo.command.EGroup` 236-249 → all 8 branches covered (nx/xx/gt/lt/unsupported) |
| 75.5 | `EGroupTest.test exists` — cross-worker path with an over-long key: `reply == ErrorReply.KEY_TOO_LONG` → **FAILED**, actual `AsyncReply` (a plain count, no error) | `71c87daa` | same focused run → BUILD SUCCESSFUL | `io.velo.command.EGroup` 169-175 → all 4 branches covered |

**Follow-up commits (same bugs, no behaviour change):**

- `9bf87c44` *refactor: use removeDelay when the expire time is in the past* — the 75.4 delete path used `remove()`, which
  does an extra `exists()` lookup inside `OneSlot.remove()` and returns a boolean that was ignored; `removeDelay()` writes the
  delete unconditionally and keeps the readonly fence. Same behaviour, less work.
- `126e013f` *refactor: move the expire error replies into the egroup class* — `ErrorReply.INVALID_EXPIRE_TIME(cmd)` and
  `ErrorReply.UNSUPPORTED_OPTION(opt)` were moved to `EGroup.invalidExpireTime(cmd)` / `EGroup.unsupportedOption(opt)`: they
  are used by one class only, so they belong to that class (the `ErrorReply` class keeps the messages shared by several
  classes, e.g. `UNKNOWN_COMMAND(cmd)`).

**End-to-end verification (live servers, Redis 7.2.14 vs Velo):**
`doc/bug_reviews/logs/20261027_105500_egroup_fix_phase_differential_bug_75.log` — every case now answers identically:

| Case | Velo (after) | Redis 7.2.14 |
|---|---|---|
| `expire k 100 gt` (no ttl) | `0`, ttl `-1` | `0`, ttl `-1` |
| `expire k 10 FOO` | `ERR Unsupported option FOO` | `ERR Unsupported option FOO` |
| `expire k 9223372036854775807` | `ERR invalid expire time in 'expire' command`, key alive | same |
| `expireat k 9223372036854775807` | `ERR invalid expire time in 'expireat' command` | same |
| `pexpire k 9223372036854775807` | `ERR invalid expire time in 'pexpire' command` | same |
| `pexpireat k 9223372036854775807` | `1` (accepted, no overflow) | `1` |
| `expireat k 0` | `1`, key deleted, ttl `-2` | `1`, key deleted, ttl `-2` |
| `expireat k 1` / `expire k -1` | `1`, key deleted | `1`, key deleted |
| `expire` on hash/list/set/zset | type preserved (`h=hash`, `hgetall` → `f v`) | same |

**Known pre-existing failure (not caused by this round):** `LGroupTest.test lpush` fails
(`lpushx a a` → `1`, expected `0`) on the base commit `5a0c1529` as well — reproduced by restoring the base versions of
`EGroup.java` / `ErrorReply.java`; the list code path is untouched by this round → follow-up candidate for the list module.

**Not covered by the fix (recorded, out of scope):** `EXPIRE k 10 NX NX` (Redis accepts a repeated identical option, Velo
answers `ErrorReply.FORMAT`) and `EXPIRE k 10 NX XX` (Redis answers a combo-specific message, Velo answers `format`) — both
error out in Velo, only the message differs.

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 75.1 | GT on no-TTL key sets expiry | High | Redis protocol compatibility / Correctness | `CONFIRMED (reproduced)` | fixed `ff6c55c2` |
| 75.2 | Unknown expire option silently ignored | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed `e20e5aa5` |
| 75.3 | No overflow check → silent key deletion | Medium | Edge case / Correctness | `CONFIRMED (reproduced)` | fixed `37ab2b4d` |
| 75.4 | EXPIREAT 0 → persistent instead of delete | Medium | Edge case | `CONFIRMED (reproduced)` | fixed `ee014c93` (+ `9bf87c44`) |
| 75.5 | EXISTS key-length check inconsistent across paths | Low | Edge case | `CONFIRMED (reproduced)` | fixed `71c87daa` |
| 75.6 | EXPIRE destroys the type marker of a structured value (added by reviewer) | High | Correctness / data corruption | `CONFIRMED (reproduced)` | fixed `bad9cf26` (+ `126e013f`) |
