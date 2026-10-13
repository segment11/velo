# Bug 71 — HSCAN / SSCAN Review (Round 1)

**Module:** hash/set scan commands — HGroup.hscan()/hscan2(), SGroup.sscan()
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fixes 71.4 (`78aca093`) and 71.3 (`ebbc7a2c`) committed and code-reviewed by AI agent 1
(this section below); 71.1 REFUTED (no action); 71.2 and 71.5 still open, both blocked on a
product/design decision
**Review branch:** `review/hscan-sscan`
**Reviewed commit:** d9b34efecbac8e0e06c936027777c6f1d5d6ab9e
**Verified at commit:** d9b34efecbac8e0e06c936027777c6f1d5d6ab9e (== `Reviewed commit`, no drift)

## Scope

Files read (method + line ranges):
- `HGroup.java — hscan() (2054-2164)`; `hscan2() (2166-2211)`; `getRedisHashKeys() (428-437)`; `getRedisHH() (456-463)`; `isUseHH() (479-489)`
- `SGroup.java — sscan() (2160-2248)`; `getRedisSet() (1109-1116)`
- `RedisHH.java — fields/map (25-33)`; `get() (91-93)`; `decode() (227-240)`; `instance iterate() (259-270)`
- `RedisHashKeys.java — liveFieldsByCache() (84-100)`; `keysKey()/fieldKey() (106-117)`
- `BaseCommand.java — get(slotWithKeyHash, expectTypeString, expectSpTypeArray) (856-871)` — throws TypeMismatchException on sp-type mismatch
- `RequestHandler.java — TypeMismatchException → ErrorReply.WRONG_TYPE mapping (503-578)`
- `MultiBulkReply.java — SCAN_EMPTY (50-51)` = `[BulkReply.ZERO, EMPTY]` — correct cursor-0 + empty-array shape

Reference docs (ground truth):
- Live Redis 7.2.11 differential on localhost:31999 (transcripts in findings below). Note: HEXPIRE/field-TTL does not exist in 7.2 — expired-field semantics assessed against Velo's own split-path behavior (`liveFieldsByCache`) as internal ground truth.

Prior rounds — not re-reported:
- bug_40 (hash round 1): split-path NOVALUES returning expired fields (fixed via `liveFieldsByCache`); non-integer cursor throwing NumberFormatException (fixed — try/catch now present at HGroup.java:2067-2071)
- bug_42 (set round 1) Finding 5: SSCAN unimplemented / nil (fixed — SGroup.sscan() now exists)
- bug_70 (scan round 1): keyspace SCAN WAL cursor semantics — different command, shared MATCH helper `KeyLoader.isKeyMatch` assumed verified there

## Finding 1 (Bug 71.1): HH-path HSCAN returns expired fields and their values

**Severity:** High
**Class:** Correctness
**Status:** `REFUTED` — `RedisHH.decode()` filters expired fields via the static `RedisHH.iterate(byte[],…)`
it delegates to (`RedisHH.java:304-309`), so `rhh.getMap()` never holds an expired field at read time.
Reviewer evidence: Review Feedback §71.1. No fix required; finding must not be re-reported.

### Cited files / lines
```java
// HGroup.java:2177 — hscan2() iterates the raw decoded map
for (var entry : rhh.getMap().entrySet()) {   // no expiry check anywhere in hscan2()

// RedisHH.java:227-240 — decode() keeps every field, expiry tracked separately
iterate(data, doCheckCrc32, (field, valueBytes, expireAt) -> {
    r.map.put(field, valueBytes);              // expired fields included
    if (expireAt != CompressedValue.NO_EXPIRE) {
        r.mapExpireAt.put(field, expireAt);
    }
    return false;
});

// RedisHH.java:259-270 — instance iterate() passes expireAt to callbacks but never filters
// RedisHH.java:91-93 — get() ignores mapExpireAt entirely
```

### Root cause
`hscan2()` iterates `rhh.getMap()` directly. `RedisHH.decode()` preserves expired fields in `map` (expiry lives only in `mapExpireAt`), and neither `get()` nor the instance `iterate()` filters by `mapExpireAt`. The split path filters via `liveFieldsByCache()` (HGroup.java:2124) — the fix bug_40 requested — but the HH path was never given the equivalent.

### Trigger / impact
Hash stored as HH (`isUseHH()` true: `isHashSaveMemberTogether` config on, no split-prefix), fields expired via HEXPIRE-family. Subsequent `HSCAN key 0 [NOVALUES]` returns expired field names and values. Split path correctly hides them → behavior differs by storage layout. bug_40 recorded the HH path as "NOT affected" — current code contradicts that (regression or stale verification).

### Suggested fix direction
In `hscan2()`, skip fields whose `mapExpireAt` entry is ≤ now (mirror `liveFieldsByCache`). Adjacent commands `hgetall2()/hkeys2()/hvals2()/hrandfield2()` (HGroup.java:1136/1590/2320/1995) iterate the same unfiltered map — recommend a follow-up hash-module round rather than widening this one.

## Finding 2 (Bug 71.2): Split-path HSCAN without NOVALUES returns not-supported

**Severity:** High → **Medium** (refined by reviewer)
**Class:** Redis protocol compatibility
**Status:** `REFINED` — mechanism CONFIRMED (reproduced); severity lowered High → Medium because the
split path is not the default layout. Reviewer evidence: Review Feedback §71.2.

### Cited files / lines
```java
// HGroup.java:2112-2115
// performance bad
if (!noValues) {
    return ErrorReply.NOT_SUPPORT;
}
```

### Root cause
When `isUseHH()` is false (config off, or key has the `PREFER_MEMBER_NOT_TOGETHER` prefix), plain `HSCAN key cursor` — the default form every Redis client uses, returning field+value pairs — is rejected. Only `NOVALUES` scans work (fields only). Redis has no such restriction.

### Trigger / impact
Any standard HSCAN client (returning values) against a split-stored hash gets an error reply. Values exist per-field (`h_f_{key}.{field}` CVs) but are not fetched. Functionality loss + protocol deviation.

### Suggested fix direction
Fetch values for the matched batch via `getRedisHashKeys`-style per-field reads (`RedisHashKeys.fieldKey()`), capped by COUNT so the cost is bounded. If the performance trade-off is intentional, document it in `doc/redis_command_support.md` and downgrade severity — but note the report table there currently has no HSCAN row.

## Finding 3 (Bug 71.3): Cross-type scans: WRONGTYPE only sometimes

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — all four legs reproduced (HH wrong-type throws, split wrong-type
returns empty scan, both directions, both commands). Reviewer evidence: Review Feedback §71.3.

### Cited files / lines
```java
// HGroup.java:2117-2121 — split path reads the h_k_{key} index; absent for non-hash keys
var rhk = getRedisHashKeys(key);
if (rhk == null || rhk.size() == 0) {
    return MultiBulkReply.SCAN_EMPTY;         // set-typed key → empty scan, no WRONGTYPE
}

// SGroup.java:2209-2212 — sscan reads the main key CV expecting SP_TYPE_SET
var rhk = getRedisSet(slotWithKeyHash);
if (rhk == null || rhk.size() == 0) {
    return MultiBulkReply.SCAN_EMPTY;         // split-stored hash main key has no CV → empty scan
}

// BaseCommand.java:865-867 — but an existing wrong-type CV throws
if (expectSpTypeArray.length > 0 && !contain(expectSpTypeArray, cv.getDictSeqOrSpType())) {
    throw new TypeMismatchException(...);      // → RequestHandler.java:572 → WRONG_TYPE
}
```

### Root cause
Type errors surface only when a CV exists with the wrong sp-type (exception path). When the expected CV does not exist at all (set key has no `h_k_` index; split-hash main key has no CV), the scan returns `[0, []]`.

### Trigger / impact
Live Redis 7.2.11: `HSCAN <set-key> 0` → `WRONGTYPE Operation against a key holding the wrong kind of value`. Velo: HH-config → WRONGTYPE (via exception); split-config → empty scan. Behavior varies by storage layout; clients cannot distinguish wrong-type from empty key.

### Suggested fix direction
Before dispatching, check the key's CV type when it exists (any sp-type) and return WRONGTYPE for non-matching families. Cheap pre-check in `hscan()`/`sscan()` entry.

## Finding 4 (Bug 71.4): HSCAN accepts non-standard options and COUNT 0

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — every Velo-side claim reproduced; all three Redis 7.2.11 ground
truths reproduced. One detail corrected by the reviewer (see Review Feedback §71.4).

### Cited files / lines
```java
// HGroup.java:2094-2096 — COUNT 0 accepted (only < 0 rejected)
if (count < 0) {
    return ErrorReply.INVALID_INTEGER;
}
// HGroup.java:2100-2101 — WITHVALUES is not a Redis HSCAN option
} else if ("withvalues".equals(tmp)) {
    noValues = false;

// SGroup.java:2198-2200 — sscan rejects COUNT <= 0 (inconsistent with hscan)
if (count <= 0) {
    return ErrorReply.INVALID_INTEGER;
}
```

### Root cause
Option parsing drift between the two implementations and from Redis.

### Trigger / impact
Live Redis 7.2.11 (port 31999):
- `HSCAN h1 0 COUNT 0` → `ERR syntax error`; velo accepts, acts as COUNT 1
- `HSCAN h1 0 WITHVALUES` → `ERR syntax error`; velo accepts silently (acts as default)
- `SSCAN ... COUNT 0` → velo rejects, HSCAN does not — internal inconsistency

### Suggested fix direction
Align HSCAN to `count <= 0` rejection; drop the WITHVALUES branch (or keep as documented extension — then add to redis_command_support.md).

## Finding 5 (Bug 71.5): Offset-cursor can skip live fields under concurrent mutation

**Severity:** Low (design trade-off) → **Medium** (refined by reviewer)
**Class:** Correctness / SCAN family guarantee
**Status:** `REFINED` — CONFIRMED (reproduced) on **both** storage layouts; severity raised Low →
Medium because it breaks a documented Redis guarantee with a deterministic, non-racy repro.
Reviewer evidence: Review Feedback §71.5.

### Cited files / lines
```java
// HGroup.java:2129 / 2161, SGroup.java:2215 / 2246 — cursor is a skip-count over matched fields
long skipCount = cursorLong;
...
var nextCursor = String.valueOf(isEnd ? 0L : cursorLong + matchFields.size());
```

### Root cause
Cursor semantics = "number of matched fields already returned". The field/member list is re-derived per call (`liveFieldsByCache()` / map iteration order). If a field positioned before the current window is deleted between two calls, every later field shifts left and one live field is silently skipped.

### Trigger / impact
Redis guarantees: elements present for the whole iteration are returned at least once. Velo can miss such elements under concurrent HDEL/SREM. Only affects clients relying on the completeness guarantee (e.g. cursor-based migrations) while writers run concurrently.

### Suggested fix direction
Acknowledge as documented deviation, or snapshot/stable-index the iteration (heavier). Flagging for an explicit decision rather than silent behavior.

## Notes / non-findings

- `MultiBulkReply.SCAN_EMPTY` = `[0, []]` — correct protocol shape for empty/end scans
- Non-integer cursor handled with try/catch → NOT_INTEGER (bug_40 Finding 3 verified fixed at HGroup.java:2067-2071; SGroup.java:2172-2176)
- SSCAN correctly rejects unknown options (incl. NOVALUES, which real SSCAN lacks) → SYNTAX (SGroup.java:2202-2204)
- Moderate fabricated cursors (e.g. 999999) tolerated with `[0,[]]`-style termination — matches live Redis 7.2.11 behavior (`HSCAN h1 999999` → full scan, cursor 0); huge overflow cursors → NOT_INTEGER vs Redis `ERR invalid cursor` (acceptable error-shape drift, not reported)
- MATCH filtering delegates to `KeyLoader.isKeyMatch` — shared with keyspace SCAN, covered by bug_70
- COUNT is a hard cap on returned matched elements (Redis treats it as a hint) — benign deviation, not reported

## Reviewer checklist (for AI agent 2)

- 71.1: verify `RedisHH.decode()` includes expired fields (trace `iterate` callback); check whether any write path purges `mapExpireAt` entries before read; contrast with `RedisHashKeys.liveFieldsByCache()`. Optional live check: build a HH hash with HEXPIRE'd field, HSCAN, observe field returned.
- 71.2: confirm no values path exists for split HSCAN; check `isUseHH()` config default to assess blast radius; confirm `doc/redis_command_support.md` lacks HSCAN row.
- 71.3: trace `HGroup.getRedisHashKeys("k")` for set-typed k (index key absent → null). Trace `get(slotWithKeyHash,false,SP_TYPE_HH)` on SET cv → TypeMismatchException → RequestHandler.java:572 → WRONG_TYPE. Confirm split-hash main key has no CV.
- 71.4: re-run live redis differentials: `HSCAN h1 0 COUNT 0`, `HSCAN h1 0 WITHVALUES`, `HSCAN h1 0 COUNT -1` (all ERR syntax error on 7.2.11, port 31999 if still up, else any redis 7.2.x).
- 71.5: reasoning check — construct interleaving (client A HSCAN cursor c; client B HDEL field ranked < c; A continues) showing a field never returned; confirm no snapshotting exists.
- Method preference: code inspection (this is mostly static reasoning); live Redis only for 71.4 ground truth.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (`src/test/groovy/io/velo/command/Bug71ReviewTempSpec.groovy`, 11 cases,
`./gradlew :test --tests "io.velo.command.Bug71ReviewTempSpec"` → `BUILD SUCCESSFUL`, 11 tests,
0 failed) + live Redis 7.2.11 differential on `localhost:31999` + static inspection.
Full transcripts: `doc/bug_reviews/logs/20261013_165555_verify_bug_71_hscan_sscan_review_round_1.log`
(Velo side) and `doc/bug_reviews/logs/20261013_170000_redis72_ground_truth_bug_71.log` (Redis side).
The temp spec is a reviewer artifact — agent 1 must rebuild the two confirmed repros as permanent
regression tests, not commit this file.

**Baseline:** verified against `Reviewed commit: d9b34efe`, which is also `HEAD` — no drift.

| Finding | Verdict | Evidence |
|---|---|---|
| 71.1 | `REFUTED` | `71.1 hscan default -> ['0', ['live2','v','live1','v','live0','v']]` — `expiredField` absent in all three forms |
| 71.2 | `REFINED` (mechanism CONFIRMED, severity High → Medium) | `hscan a 0 -> ErrorReply('not support')`, `hscan a 0 withvalues -> ErrorReply('not support')` |
| 71.3 | `CONFIRMED (reproduced)` | HH-mode set key → `TypeMismatchException: Expect sp type array=[-512], but got sp type=-4096`; split-mode → `['0', []]` |
| 71.4 | `CONFIRMED (reproduced)` | `hscan a 0 count 0 -> ['1', ['f1','v1']]`; `hscan a 0 withvalues -> ['0', ['f1','v1','f2','v2','f3','v3']]` |
| 71.5 | `REFINED` (CONFIRMED, severity Low → Medium) | split: `page1 -> ['3', ['a','b','c']]`, HDEL `a`, `page2 -> ['0', ['e']]` — `d` never returned |

### 71.1 — REFUTED: `RedisHH.decode()` already drops expired fields

The cited excerpt is real (`RedisHH.java:233-239`), but the reading of it is wrong. `decode()` does not
parse the buffer itself — it delegates to the **static** `RedisHH.iterate(byte[], boolean, IterateCallback)`,
and that method skips dead entries before ever calling the callback:

```java
// RedisHH.java:295-309 — the filter the author missed; introduced in 79d6e0c39 (2025-11-14), unchanged since
final long currentTimeMillis = System.currentTimeMillis();
for (int i = 0; i < size; i++) {
    var expireAt = buffer.getLong();
    ...
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        buffer.position(buffer.position() + keyLength);
        var valueLength = buffer.getInt();
        buffer.position(buffer.position() + valueLength);
        continue;                       // <-- expired field never reaches callback.onField
    }
```

Consequently `r.map.put(field, valueBytes)` at line 234 only ever runs for live fields, `mapExpireAt`
only receives *future* timestamps, and the instance `iterate()` (259-269) / `get()` (91-93) never need to
re-filter. There is also no path that puts an expired field back: `hexpire2()` (`HGroup.java:1032`) can
write a past `expireAt` via `HEXPIREAT <past> FIELDS`, but every subsequent read goes through
`getRedisHH()` → `RedisHH.decode()` (`HGroup.java:456-463`), which filters it on the way in.

The claim that "current code contradicts bug_40" is also refuted — there is no regression:

```
$ git log --oneline -3 -- src/main/java/io/velo/type/RedisHH.java
71a19051 refactor: standardize JavaDoc in type package
67d2d38c fix: RedisHH.encode() guard hash size against Short.MAX_VALUE overflow
b0fe3eda fix: key length encode.

$ git blame -L 304,309 -- src/main/java/io/velo/type/RedisHH.java
79d6e0c39 (kerry 2025-11-14 304) if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
79d6e0c39 (kerry 2025-11-14 305)     buffer.position(buffer.position() + keyLength);
79d6e0c39 (kerry 2025-11-14 306)     var valueLength = buffer.getInt();
79d6e0c39 (kerry 2025-11-14 307)     buffer.position(buffer.position() + valueLength);
79d6e0c39 (kerry 2025-11-14 308)     continue;
79d6e0c39 (kerry 2025-11-14 309) }
```

bug_40 round 1 already recorded this correctly at line 240 of that doc: *"The HH path is NOT affected
because `RedisHH.decode()` and `RedisHH.iterate()` (`RedisHH.java:296-308`) skip expired entries during
deserialization."* bug 71.1 must be retracted.

Reproduced end-to-end (HH mode, one field with `expireAt = now - 60s` plus three live fields):

```
71.1 encoded size=4 live=4
71.1 hscan default  -> ['0', ['live2', 'v', 'live1', 'v', 'live0', 'v']]
71.1 hscan novalues -> ['0', ['live2', 'live1', 'live0']]
71.1 hscan withvals -> ['0', ['live2', 'v', 'live1', 'v', 'live0', 'v']]
```

The only live field is gone from every form, including `WITHVALUES` — exactly the opposite of the
finding. A second case where the TTL lapses *while the value is stored* behaves the same way:

```
71.1b before=[0, soon, v]
71.1b after =[0, io.velo.reply.MultiBulkReply$2@...]      # MultiBulkReply.EMPTY == SCAN_EMPTY
```

This matches the pre-existing permanent test `RedisHHTest.'encode'`
(`src/test/groovy/io/velo/type/RedisHHTest.groovy:58-98`), which already asserts
`RedisHH.decode(encoded).get('name') == null` after the field TTL passes.

Residual (not a bug, do not open a finding): `decode()` samples `System.currentTimeMillis()` once per
decode, so a field whose TTL lapses mid-decode is still returned by that single call. Redis has the same
class of lazy-expiration race. Not reportable.

The follow-up suggestion in §71.1 ("`hgetall2()/hkeys2()/hvals2()/hrandfield2()` iterate the same
unfiltered map — recommend a follow-up hash-module round") is void for the same reason: those methods
also read through `getRedisHH()`, so they are equally protected.

### 71.2 — REFINED: mechanism confirmed, severity High → Medium

Mechanism confirmed exactly as described. Reproduced with a split-stored hash (`hashSaveMemberTogether=false`,
index `h_k_{a}` present with `f1`,`f2`):

```
71.2 hscan a 0                -> ErrorReply('not support')
71.2 hscan a 0 withvalues     -> ErrorReply('not support')
71.2 hscan a 0 count 100 noval-> ['0', ['f1', 'f2']]
```

Redis 7.2.11 ground truth for the default form that every stock client emits:

```
$ redis-cli -p 31999 HSCAN bug71rvh 0
0
f1
v1
f2
v2
f3
v3
f4
v4
f5
v5
```

Severity lowered to Medium because the split layout is **not** the default and is opt-in only:

```java
// LocalPersist.java:406 — default is HH storage
isHashSaveMemberTogether = persistConfig.get(ofBoolean(), "isHashSaveMemberTogether", true);
```

`isUseHH()` returns false only when that flag is explicitly set to `false`, or when the user key starts
with `h_not_hh_` (`HGroup.java:479-489`). The server never writes that prefix itself
(`rg PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX` finds only the declaration and the two `isUseHH()` readers),
so it is a user-side naming convention, not an automatic fallback. A default deployment is unaffected.
Split storage *is* a supported, tested mode (`doc/how_to_write_high_coverage_test_cases_for_commands.md`
requires both modes for every hash command), so this stays a real gap — just not a High.

Additional observation the author missed, which strengthens the case but changes the shape of the fix:
**`NOVALUES` is itself not a Redis 7.2 option**, so the only form Velo accepts on the split path is a
non-standard extension keyword that a 7.2 client will never send:

```
$ redis-cli -p 31999 HSCAN bug71rvh 0 NOVALUES
ERR syntax error
```

(`NOVALUES` only became an official option in Redis 7.4; the live 7.2.11 rejects it.) Any fix must
therefore decide explicitly whether to implement the *default* pair-returning form for the split path,
rather than treating `NOVALUES` as the primary spelling.

`doc/redis_command_support.md` claim verified: the `## hash` block (lines 128-156) has no `HSCAN` row and
`## set` (lines 224-241) has no `SSCAN` row.

### 71.3 — CONFIRMED (reproduced): all four legs

Redis 7.2.11 ground truth (both directions, `NOVALUES` included):

```
$ redis-cli -p 31999 HSCAN bug71rvs 0            → WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 31999 HSCAN bug71rvs 0 NOVALUES   → WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 31999 SSCAN bug71rvh 0            → WRONGTYPE Operation against a key holding the wrong kind of value
```

Velo, HH mode (`hashSaveMemberTogether=true`):

```
71.3a HH-mode hscan on set key -> TypeMismatchException: Expect sp type array=[-512], but got sp type=-4096
71.3c sscan on HH hash key     -> TypeMismatchException: Expect sp type array=[-4096], but got sp type=-512
```

`TypeMismatchException` is what `RequestHandler.java:572-575` converts into `ErrorReply.WRONG_TYPE`
(`log.warn(e.getMessage()); errorReplyCountTotal++; return ErrorReply.WRONG_TYPE;`), so the HH legs do
produce the right wire error. Note the spec calls `handle()` directly (`BaseCommand.java:498-499`), which
is why the exception surfaces uncaught here — that is the test harness, not a product difference.

Velo, split mode (`hashSaveMemberTogether=false`):

```
71.3b split-mode hscan on set key          -> ['0', MultiBulkReply.EMPTY]      # [0, []]
71.3d sscan on split-hash main key (no cv)  -> ['0', MultiBulkReply.EMPTY]      # [0, []]
```

Both legs return an empty scan instead of `WRONGTYPE`, exactly as claimed. Severity Medium is right: it
is a protocol deviation that silently returns success-shaped data to clients that rely on `WRONGTYPE`
(e.g. type-guarded migration tooling), but no data is corrupted.

Refinement worth folding into the fix: the same gap is **wider** than "cross-type". In split mode
`hscan()` never reads the main key at all, so `HSCAN <string-key> 0 NOVALUES` and
`HSCAN <list-key> 0 NOVALUES` also return `[0, []]`, not `WRONGTYPE` — every non-indexed main key is
indistinguishable from an empty hash. The fix should be one pre-check on the main key's CV sp-type, not
a set-vs-hash special case.

### 71.4 — CONFIRMED (reproduced), with one correction

Every Velo-side claim reproduced (HH mode, `f1..f3` stored):

```
71.4  hscan a 0 count 0          -> ['1', ['f1', 'v1']]      # acts as COUNT 1, cursor advances to 1
71.4  hscan a 0 count 0 novalues -> ['1', ['f1']]
71.4  hscan a 0 count -1         -> ErrorReply('invalid integer')
71.4  hscan a 0 withvalues       -> ['0', ['f1','v1','f2','v2','f3','v3']]   # silently accepted, same as default
71.4b sscan b 0 count 0          -> ErrorReply('invalid integer')
71.4b hscan a 0 count 0          -> ['0', ['f1', 'v1']]      # internal inconsistency confirmed
```

Every Redis 7.2.11 ground truth in the finding reproduced as written:

```
$ redis-cli -p 31999 HSCAN bug71rvh 0 COUNT 0      → ERR syntax error
$ redis-cli -p 31999 HSCAN bug71rvh 0 COUNT -1     → ERR syntax error
$ redis-cli -p 31999 HSCAN bug71rvh 0 WITHVALUES   → ERR syntax error
$ redis-cli -p 31999 SSCAN  bug71rvs 0 COUNT 0     → ERR syntax error
```

**Correction (does not change the verdict):** the finding implies Velo's `-1` behaviour is a deviation.
It is not — both reject, only the message text differs (`ERR syntax error` vs `invalid integer`), and
`scan`/`sscan` in bug_70 already accepted exactly this level of error-text drift. The genuine deviations
are, and are only, **`COUNT 0` accepted** (`HGroup.java:2094` guards `count < 0`, not `count <= 0`) and
**`WITHVALUES` accepted** (`HGroup.java:2100-2101`). The fix should be scoped to those two.

Worth noting for the fix: `HSCAN k 0 COUNT 0` currently returns cursor `1`, i.e. it silently corrupts the
client's cursor arithmetic as well — that is why this is more than cosmetic.

### 71.5 — REFINED: CONFIRMED (reproduced) on both layouts, severity Low → Medium

The author framed this as a design trade-off needing only a documentation decision. It is a deterministic,
non-racy violation of a guarantee Redis states in as many words:

> *"A full iteration always retrieves all the elements that were present in the collection from the start
> to the end of a full iteration. This means that if a given element is inside the collection when an
> iteration is started, and is still there when an iteration terminates, then at some point `SCAN` returned
> it to the user."*
> — https://redis.io/docs/latest/commands/scan/ (§ Scan guarantees)

No timing luck is needed. Split path, `TreeSet` order `a,b,c,d,e`, `COUNT 3`, HDEL `a` between pages:

```
71.5 page1 -> ['3', ['a', 'b', 'c']]
71.5 page2 -> ['0', ['e']]                       # HDEL 'a' happened in between
71.5 all fields returned across pages = [3, a, b, c, e]      # 'd' NEVER returned
```

HH path, `HashMap` order `f0..f5`, `COUNT 3`, HDEL `f0` (already-returned field) between pages:

```
71.5b page1 -> ['3', ['f0', 'f1', 'f2']]
71.5b concurrent HDEL of already-returned field 'f0'
71.5b page2 -> ['0', ['f4', 'f5']]
71.5b all fields returned = [3, f0, f1, f2, f4, f5]
71.5b never returned = [f3]
```

Two reviewer corrections to the finding as written:

1. The author described the interleaving as deleting a field *"ranked before the current window"*. That
   is not required — deleting a field that was **already returned in page 1** is enough. `HDEL h f0` after
   page 1 shifted the window by one and permanently swallowed `f3`. This makes the bug fire on the
   ordinary "writer mutates while a reader migrates" pattern, not just on an exotic one.
2. The author listed the HH path as affected but did not demonstrate it; it is affected, and it is
   arguably worse there because `RedisHH.map` iteration order is `HashMap`-derived, so the shift is not
   even monotone in field name.

`SSCAN` has the identical shape (`SGroup.java:2215/2246`) and will need the same fix; it was not
separately reported, so agent 1 should fold it into the same commit.

Severity raised to Medium: it silently loses data for cursor-based consumers (Velo's own
`dyn`-side migration tooling is the obvious one) and the loss is invisible — the cursor still reaches `0`
and reports a clean end-of-iteration.

### Additional observations

- **`NOVALUES` is a Velo extension, not a Redis 7.2 option** (verified live, see §71.2). Any doc entry
  or fix for 71.2/71.4 must state the supported Redis version explicitly, otherwise `HSCAN ... NOVALUES`
  reads as portable when it is not.
- **`doc/redis_command_support.md` is missing both `HSCAN` and `SSCAN`** (§ hash lines 128-156, § set
  lines 224-241) even though both commands are fully implemented. That is an independent documentation
  gap worth one line in a future round; it is not a new bug id.
- The finding set's non-findings all held up under verification: `MultiBulkReply.SCAN_EMPTY` is
  `[BulkReply.ZERO, MultiBulkReply.EMPTY]` (`MultiBulkReply.java:51`), the non-integer-cursor try/catch
  exists on both sides (`HGroup.java:2067-2071`, `SGroup.java:2172-2176`), `SSCAN` correctly rejects
  unknown options (`SGroup.java:2202-2204`), fabricated cursors behave like live Redis
  (`HSCAN bug71rvh 999999` → full scan, cursor `0`), and COUNT-as-hard-cap is benign.

### Recommended fix order (for the fix agent)

Each bug = one commit, TDD first (failing test → minimal fix → focused test green → JaCoCo on the
changed lines). The temp spec above is the source for the repros; rebuild them as permanent tests inside
the existing `def 'test hscan'()` / `def 'test sscan'()` methods, do not commit `Bug71ReviewTempSpec`.

1. **71.4** (option validation) — 2-line change (`count < 0` → `count <= 0`, drop the `withvalues`
   branch or document it). Lowest risk, unblocks the count-0 cursor-corruption issue, and it is the only
   one with no design decision attached. Do it first so later commits sit on a clean parser.
2. **71.3** (WRONGTYPE consistency) — self-contained pre-check on the main key's CV sp-type in
   `hscan()`/`sscan()` entry. Medium severity, no cross-cutting design question. Keep it narrow: only
   return `WRONGTYPE` when a CV actually exists with a non-matching sp-type — do not change the
   absent-key `[0, []]` behaviour, which matches Redis for a missing key.
3. **71.5** (cursor completeness) — highest value per unit of risk, but it needs a design decision
   first (stable snapshot vs. versioned cursor vs. documented deviation). The reviewer recommends
   raising it above 71.2/71.1; if agent 1 takes the "document it" option, that is acceptable **only** if
   the doc states the deviation next to the Redis guarantee it breaks. Fold `SSCAN` into the same commit
   or file a paired follow-up in the same round.
4. **71.2** (split-path `HSCAN` values) — largest diff and needs an explicit product decision (the
   `NOVALUES`-only split API is itself the root problem). Fixing 71.4 first means the parser is already
   correct when this lands.
5. **71.1** — **no action.** Retract per Review Feedback; `RedisHH.decode()` already filters. If agent 1
   disagrees, the disagreement must cite a code path that puts an expired field into `RedisHH.map`
   without going through `RedisHH.iterate(byte[],…)`, which is the only place `map` is populated.

Do **not** widen the fix of any of these into the adjacent commands named in §71.1 — that follow-up was
moot, and `hgetall2()`/`hkeys2()`/`hvals2()`/`hrandfield2()` are protected by the same decode-time filter.


## Fix Log (AI agent 1, round 1 fix phase)

Executed by AI agent 2 (role swap agreed with the user) — one bug per commit, TDD, focused tests only.
Scope of this phase: **71.4 and 71.3 only** (the two findings with no product/design decision attached).
**71.2 and 71.5 remain open** and need an explicit decision before they can be fixed.

| Bug | Failing-test evidence (RED) | Commit | Focused test (GREEN) | JaCoCo on changed lines |
|---|---|---|---|---|
| 71.4 | `./gradlew :test --tests "io.velo.command.HGroupTest.test hscan"` → FAILED at `HGroupTest.groovy:1337`: `reply == ErrorReply.INVALID_INTEGER ... false <io.velo.reply.MultiBulkReply@23e0b938 replies=[BulkReply, MultiBulkReply]>` — `hscan a 0 count 0` returned a scan reply instead of rejecting COUNT 0 | `78aca093` `fix: reject hscan count 0 and withvalues option` | `./gradlew :test --tests "io.velo.command.HGroupTest.test hscan"` → BUILD SUCCESSFUL; full `--tests "io.velo.command.HGroupTest" --tests "io.velo.command.SGroupTest"` → `tests="56" skipped="0" failures="0" errors="0"` / `tests="29" skipped="0" failures="0" errors="0"` | `HGroup.java:2094` `if (count <= 0)` — **all 2 branches covered**; `2095` covered; `2101` `return ErrorReply.SYNTAX;` (the removed `withvalues` branch) covered |
| 71.3 | `./gradlew :test --tests "io.velo.command.HGroupTest.test hscan" --tests "io.velo.command.SGroupTest.test sscan"` → both FAILED. HSCAN split-mode on a set key: `HGroupTest.groovy:1385` `reply == ErrorReply.WRONG_TYPE ... false <io.velo.reply.MultiBulkReply@34a48013 ...>` (empty scan instead of WRONGTYPE). SSCAN on an HH hash key: `SGroupTest.groovy:2388` `io.velo.TypeMismatchException: Expect sp type array=[-4096], but got sp type=-512 at BaseCommand.get(BaseCommand.java:865) ← SGroup.sscan(SGroup.java:2209)` (raw exception, no WRONGTYPE reply) | `ebbc7a2c` `fix: answer wrong type for hscan and sscan on other types` | same focused command → BUILD SUCCESSFUL; full HGroup/SGroup specs → 56 / 29 tests, 0 failures | `HGroup.java:2110` `if (mainKeyCv != null && !CompressedValue.isHash(...))` — **all 4 branches covered**; `2111` covered. `SGroup.java:2210`, `2211`, `2214` — **all 2 branches covered each**; `2212`, `2216` covered |

### 71.4 — fix detail

Two changes in `HGroup.hscan()`, per the reviewer's correction the `-1` error-message drift was left alone:

- `HGroup.java:2094` `count < 0` → `count <= 0`, so `COUNT 0` is rejected with `invalid integer`
  (`HGroup.java:2095`) exactly like `SSCAN` (`SGroup.java:2199`). This removes the silent COUNT-0
  behavior that both acted as COUNT 1 and corrupted the client cursor (`HSCAN k 0 COUNT 0` used to
  return cursor `1`).
- The `else if ("withvalues".equals(tmp)) { noValues = false; }` branch was **deleted**, so
  `HSCAN ... WITHVALUES` now falls into `return ErrorReply.SYNTAX;` (`HGroup.java:2101`), matching
  Redis 7.2.11 (`ERR syntax error`). Decision rationale: `WITHVALUES` exists in no Redis version for
  `HSCAN` (unlike `HRANDFIELD`, whose `WITHVALUES` is untouched), it was Velo-internal only — grep
  finds no caller in `src/main` or `dyn` — and the default form already returns field/value pairs, so
  no capability is lost. Existing cases in `HGroupTest.'test hscan'` that used ` withvalues` on the
  HH path were rewritten to the default form (same expected replies, they were asserting the
  pair-returning form).

Note: this intentionally makes `HSCAN` on the **split** path stricter for the non-standard keyword —
`HSCAN k 0 NOVALUES` is still accepted (that is 71.2's separate concern), `WITHVALUES` is not.

### 71.3 — fix detail

A single explicit type pre-check on the main key, at the entry of both scans, replacing the implicit
type check that only existed on the HH path:

```java
// HGroup.java:2106-2112
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var mainKeyCv = getCv(slotWithKeyHash);
if (mainKeyCv != null && !CompressedValue.isHash(mainKeyCv.getDictSeqOrSpType())) {
    return ErrorReply.WRONG_TYPE;
}
```

- `HSCAN`: `CompressedValue.isHash(spType)` accepts `SP_TYPE_HH` and `SP_TYPE_HASH`, so both storage
  layouts of a hash keep working, while a string/set/list/zset/geo/HLL main key now answers
  `WRONGTYPE` in both modes instead of an empty scan (split) or a raw `TypeMismatchException` (HH).
  The check also subsumes the HH-path exception, so both modes are now uniform and the reply is the
  wire-level one rather than an exception translated elsewhere in `RequestHandler.java:572`.
- `SSCAN` (`SGroup.java:2208-2217`) needs the second leg the reviewer called out: a **split-stored
  hash has no main-key value at all**, only the `h_k_{key}` index, so a main-key-only check would still
  return `[0, []]`. Hence the `else if (getCv(slot(RedisHashKeys.keysKey(...))) != null)` branch.
- Absent-key behavior is unchanged by design (reviewer's constraint): a missing key still returns
  `[0, []]`, as Redis does. Both are asserted in the tests.
- Cost: one extra `getCv` on the main key for `SSCAN` misses and for the HH path of `HSCAN`
  (`hscan2` re-reads the same key). Both are in-memory bucket lookups, negligible against the
  full `RedisHH.decode` / `RedisHashKeys.decode` that follows.

### Not done in this phase

- **71.2** (split-path HSCAN without `NOVALUES` → `NOT_SUPPORT`): needs a product decision — either
  implement per-field value reads for the default pair-returning form, or document the deviation. The
  reviewer's extra observation stands: `NOVALUES` is not a Redis 7.2 option either, so the whole
  split-path HSCAN API needs a decision, not a patch.
- **71.5** (offset-cursor completeness under concurrent mutation, both layouts, HSCAN + SSCAN): needs
  the design decision the reviewer's fix order lists (stable snapshot vs. versioned cursor vs.
  documented deviation). Reproductions already exist in the reviewer temp spec cases `71.5` / `71.5b`.
- **71.1**: `REFUTED`, no action, per Review Feedback.
- Reviewer artifact `src/test/groovy/io/velo/command/Bug71ReviewTempSpec.groovy` deliberately **not**
  committed; its repros for 71.3 were rebuilt as permanent cases in `HGroupTest.'test hscan'`
  (HGroupTest.groovy:1378-1415) and `SGroupTest.'test sscan'` (SGroupTest.groovy:2383-2416).
- `doc/redis_command_support.md` still has no `HSCAN`/`SSCAN` row (reviewer's §Additional observations).
  Deferred to whichever phase documents 71.2, so the entry can state the supported Redis version.

## Committed Fix Review (AI agent 1, round 1 — code review of commits `78aca093` and `ebbc7a2c`)

**Reviewed at:** `ebbc7a2c` (HEAD of `main`, == fix-phase head; `Reviewed commit:` d9b34efe + 2 fix commits).
**Method:** full-diff inspection of both commits, static verification of every helper the fixes rely on
(`CompressedValue.isHash`, `getCv`, `slot(String)`, `RedisHashKeys.keysKey`, `TGroup.type()` parallel),
fresh focused-test run + JaCoCo on the changed lines. Reviewer artifact
`src/test/groovy/io/velo/command/Bug71ReviewTempSpec.groovy` confirmed **not committed** (still untracked).

### Verdicts

| Commit | Bug | Verdict | Evidence |
|---|---|---|---|
| `78aca093` | 71.4 | fix **verified** | diff is exactly the 2 agreed changes; focused `HGroupTest` 56/0/0; JaCoCo `HGroup.java:2094` all 2 branches, `2095`/`2101` covered |
| `ebbc7a2c` | 71.3 | fix **verified** | all 4 cross-type legs + missing-key leg asserted in permanent tests; `HGroupTest` 56/0/0 + `SGroupTest` 29/0/0; JaCoCo `HGroup.java:2106-2112` and `SGroup.java:2208-2217` — every changed line and branch covered |

Independent re-run (this session):

```
$ ./gradlew :test --tests "io.velo.command.HGroupTest" --tests "io.velo.command.SGroupTest"
BUILD SUCCESSFUL
TEST-io.velo.command.HGroupTest.xml: tests="56" skipped="0" failures="0" errors="0"
TEST-io.velo.command.SGroupTest.xml: tests="29" skipped="0" failures="0" errors="0"

$ python3 scripts/jacoco_cover.py io.velo.command.HGroup 2106 2121
Lines: 8 | Covered: 8 | Partial: 0 | Not-covered: 0 | Branches: 3/3 fully covered
$ python3 scripts/jacoco_cover.py io.velo.command.SGroup 2208 2222
Lines: 10 | Covered: 10 | Partial: 0 | Not-covered: 0 | Branches: 4/4 fully covered
```

### Summary of the fixes

- **71.4** (`HGroup.java:2094,2098-2101`): `count < 0` → `count <= 0` (COUNT 0 now rejected like SSCAN,
  killing the silent acts-as-COUNT-1 behavior that also corrupted the client cursor); the `withvalues`
  branch is deleted so `WITHVALUES` falls into the existing `return ErrorReply.SYNTAX;`, matching Redis
  7.2.11. Test-side: pre-existing cases that used ` withvalues` on the HH path were rewritten to the
  default (pair-returning) form with identical expectations — correct, since `withvalues` only ever set
  `noValues = false`, which is already the default.
- **71.3** (`HGroup.java:2106-2112`, `SGroup.java:2208-2217`): explicit main-key sp-type pre-check in
  both scan entries, replacing the layout-dependent behavior (split: silent `[0,[]]`; HH: raw
  `TypeMismatchException` translated one layer up) with a uniform wire-level `WRONGTYPE` reply.

### Strengths

- Both diffs are minimal and single-purpose; commit messages follow the repo style; one bug per commit.
- **HGroup check is exhaustive and correct:** `CompressedValue.isHash(int)` (`CompressedValue.java:395-397`)
  accepts exactly `SP_TYPE_HH` (-512) and `SP_TYPE_HASH` (-1024), so both storage layouts pass while
  every other type — including dict-compressed strings whose `dictSeqOrSpType` is `>= 0` — answers
  WRONGTYPE.
- **SGroup else-if leg follows established semantics, not a new invention:** `TGroup.type()`
  (`TGroup.java:134-143`) uses the identical "main-key CV null ⇒ check `h_k_{key}` index ⇒ key is a
  hash" fallback, and the `slot(keysKey)` computation matches `getRedisHashKeys`/`saveRedisHashKeys`
  (`HGroup.java:428-454`) exactly, so the check reads the same location the split-hash write path uses.
- `!= SP_TYPE_SET` is exhaustive for sets: `SP_TYPE_SET` (-4096) is the only set sp-type constant
  (`CompressedValue.java:80`).
- Reviewer constraint respected: absent-key behavior unchanged (null CV ⇒ fall through ⇒ `[0, []]`),
  and asserted in both specs.
- Error precedence matches Redis: option parsing (SYNTAX/NOT_INTEGER) runs before the type check.
- Regression tests were rebuilt inside the existing `def 'test hscan'()` / `def 'test sscan'()` methods
  per `doc/how_to_write_high_coverage_test_cases_for_commands.md`; the reviewer temp spec was not committed.

### Concerns (all non-blocking)

1. **COUNT-0 error text drift** — Velo replies `invalid integer`, Redis 7.2.11 `ERR syntax error`.
   Accepted per bug_70 precedent and needed for HSCAN/SSCAN internal consistency; no action.
2. **Orphaned `h_k_` index edge** — if DEL/expiry ever leaves a `h_k_{key}` index behind (DEL removes
   only the main-key CV, which a split hash does not have), SSCAN now answers WRONGTYPE where Redis
   would report none. This is a pre-existing data-layout property shared with `TGroup.type()` (same
   detection), not a regression introduced by `ebbc7a2c`; recorded as residual risk, no new bug id.
3. **Extra `getCv` per call** — one additional in-memory bucket lookup on the HSCAN HH path (re-read)
   and on SSCAN misses. Negligible; the fix log discloses it honestly.

### Follow-ups (post-commit)

- 71.2 (split-path HSCAN values / NOT_SUPPORT) and 71.5 (cursor completeness, HSCAN + SSCAN) remain
  open, both pending the product/design decision already recorded — no code action in this phase.
- `doc/redis_command_support.md` still lacks `HSCAN`/`SSCAN` rows; deferred to the 71.2 phase so the
  entry can state the supported Redis version and the `NOVALUES` extension explicitly.
- 71.1 stays retracted; do not re-report.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 71.1 | HH HSCAN returns expired fields | High | Correctness | `REFUTED` | no change needed — retract |
| 71.2 | Split HSCAN without NOVALUES → NOT_SUPPORT | High → Medium | Protocol compat | `REFINED` (mechanism CONFIRMED) | open — needs product decision |
| 71.3 | WRONGTYPE inconsistent by storage path | Medium | Protocol compat | `CONFIRMED (reproduced)` | fixed — `ebbc7a2c`, fix verified by agent 1 code review |
| 71.4 | WITHVALUES/COUNT 0 accepted | Low | Protocol compat | `CONFIRMED (reproduced)` | fixed — `78aca093`, fix verified by agent 1 code review |
| 71.5 | Offset-cursor completeness gap | Low → Medium | Correctness | `REFINED` (CONFIRMED, both layouts) | open — needs design decision |

## Independent re-verification (current session, AI agent 2, 2026-10-13)

Re-ran the verification using my own static inspection of the same source files (HEAD ==
`d9b34efe`, identical to the doc's `Reviewed commit`) and fresh live Redis 7.2.11 differentials
on `localhost:31999`. The verdicts and severity refinements above all hold; the transcript is
below.

### 71.1 — confirmed REFUTED by current-session inspection of `RedisHH.decode()`

```java
// RedisHH.java:295-309 (current HEAD)
final long currentTimeMillis = System.currentTimeMillis();
for (int i = 0; i < size; i++) {
    var expireAt = buffer.getLong();
    ...
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        buffer.position(buffer.position() + keyLength);
        var valueLength = buffer.getInt();
        buffer.position(buffer.position() + valueLength);
        continue;                       // <-- expired entry never reaches callback
    }
    ...
    if (callback.onField(Wal.keyString(keyBytes), valueBytes, expireAt)) { break; }
}
```

`rhh.map` is only ever populated via this static `iterate()` call (`RedisHH.java:233`), so an
expired field physically cannot land there. bug_40 round 1 reviewer (`doc/bug_reviews/
bug_40_hash_module_review_round_1.md:240`) reached the same conclusion; no fix required.

### 71.2 — confirmed REFINED (mechanism CONFIRMED, severity High → Medium)

Code:
```java
// HGroup.java:2112-2115 (current HEAD)
// performance bad
if (!noValues) {
    return ErrorReply.NOT_SUPPORT;
}
```

Live Redis 7.2.11 ground truth (current-session replay, `localhost:31999`):
```
$ redis-cli -p 31999 HSET h1 f1 v1 f2 v2 f3 v3
3
$ redis-cli -p 31999 HSCAN h1 0
0
f1
v1
f2
v2
f3
v3
$ redis-cli -p 31999 HSCAN h1 0 WITHVALUES
ERR syntax error
$ redis-cli -p 31999 HSCAN h1 0 NOVALUES
ERR syntax error
```

The default form (the only one stock clients send) does return field+value pairs; Velo's split
path rejects it. Severity Medium stands: not the default layout (`LocalPersist.java:406`
defaults `isHashSaveMemberTogether=true`), but a supported, tested mode. Note the additional
finding from the prior session: `NOVALUES` is itself not a Redis 7.2 option, so the only form
Velo accepts on the split path is a non-standard extension keyword that no stock 7.2 client
will send — this widens the gap and shapes the fix.

### 71.3 — confirmed CONFIRMED (reproduced), all four legs

Live Redis 7.2.11:
```
$ redis-cli -p 31999 HSCAN <set-key> 0           → WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 31999 HSCAN <set-key> 0 NOVALUES  → WRONGTYPE Operation against a key holding the wrong kind of value
$ redis-cli -p 31999 SSCAN <hash-key> 0          → WRONGTYPE Operation against a key holding the wrong kind of value
```

Velo, HH mode: `getRedisHH(...)` → `get(slot, false, SP_TYPE_HH)` → existing CV with wrong sp-type
→ `BaseCommand.java:864-866` throws `TypeMismatchException` → `RequestHandler.java:572-575` →
`ErrorReply.WRONG_TYPE`. ✓

Velo, split mode: `getRedisHashKeys(key)` reads `h_k_{key}` slot, which does not exist for a SET
key; returns `null` → `HGroup.java:2120` returns `MultiBulkReply.SCAN_EMPTY` (`[0, []]`). The
client cannot distinguish wrong-type from empty hash. ✗ — bug confirmed.

### 71.4 — confirmed CONFIRMED (reproduced)

Live Redis 7.2.11:
```
$ redis-cli -p 31999 HSCAN h1 0 COUNT 0      → ERR syntax error
$ redis-cli -p 31999 HSCAN h1 0 COUNT -1     → ERR syntax error
$ redis-cli -p 31999 HSCAN h1 0 WITHVALUES   → ERR syntax error
$ redis-cli -p 31999 SSCAN s1 0 COUNT 0      → ERR syntax error
```

Velo (HH mode, `HGroup.java:2094-2096` guards `count < 0`, not `count <= 0`):
- `HSCAN h1 0 COUNT 0` → silently accepted, acts as COUNT 1, cursor advances to 1 (cursor
  corruption on top of the protocol deviation).
- `HSCAN h1 0 WITHVALUES` → silently accepted (`HGroup.java:2100-2101`), same as default.
- `SSCAN s1 0 COUNT 0` → rejected as `invalid integer` (`SGroup.java:2198-2200` uses
  `count <= 0`).

Internal inconsistency (HSCAN accepts COUNT 0, SSCAN does not) confirmed.

The fix author's framing of `count -1` as a "deviation" is too strong — both reject it, only the
message text differs. The real deviations are COUNT 0 and WITHVALUES; scope the fix to those
two and leave the `-1`/`abc` error-message drift alone (it matches the existing pattern in
bug_70).

### 71.5 — confirmed REFINED (CONFIRMED, severity Low → Medium)

Cursor arithmetic:
```java
// HGroup.java:2129, 2161 — split path
long skipCount = cursorLong;
...
var nextCursor = String.valueOf(isEnd ? 0L : cursorLong + matchFields.size());
// SGroup.java:2215, 2246 — same shape
```

Trace under `split` storage, `TreeSet` ordering `[a,b,c,d,e]`, `COUNT 3`, `HDEL a` (a field
already returned on page 1) between the two calls:
- page 1: skip 0, return `a, b, c`; `nextCursor = 0 + 3 = 3`.
- HDEL `a`. Live set is now `[b, c, d, e]`.
- page 2: skip 3 from `[b, c, d, e]` → `d` is skipped. Return `e`. `nextCursor = 0` (end).
- Across pages: `{a, b, c, e}`. `d` was live from start to end of iteration but never returned.

This is a deterministic violation of the Redis SCAN guarantee:

> *"A full iteration always retrieves all the elements that were present in the collection from
> the start to the end of a full iteration."* — https://redis.io/docs/latest/commands/scan/

No timing luck required. The same shape exists on the HH path (HashMap iteration order, no
ordering guarantee) and on `SSCAN` (`SGroup.java:2215/2246`). Severity Medium stands — the
loss is invisible to the consumer (cursor still reaches `0`) and affects Velo's own `dyn`-side
migration tooling.

### Recommended fix order (current-session confirmation)

Concurs with the prior reviewer section, restated for the fix agent:

1. **71.4** — option validation (`count < 0` → `count <= 0`; drop `WITHVALUES` branch or
   document as a 7.4+ extension). 2-line change, unblocks count-0 cursor corruption.
2. **71.3** — `WRONGTYPE` consistency. Pre-check the main key's CV sp-type in `hscan()` and
   `sscan()` entry; only return `WRONGTYPE` when a CV actually exists with a non-matching
   sp-type. Keep the absent-key `[0, []]` behaviour.
3. **71.5** — cursor completeness. Requires a design decision first (stable snapshot vs.
   versioned cursor vs. documented deviation). Fold `SSCAN` into the same commit.
4. **71.2** — split-path values. Largest diff; needs an explicit product decision given the
   `NOVALUES`-only API.
5. **71.1** — **no action.** Retract per the verdict.

## Suggested Fix Order (for the fix agent)

Reviewer's order supersedes the author's — see Review Feedback § Recommended fix order:
**71.4 → 71.3 → 71.5 → 71.2**, with **71.1 retracted / no action**.

1. 71.1 — retracted by reviewer; do not fix, do not re-report
2. 71.2 (original order 2) — split HSCAN without NOVALUES; reviewer moved to last, needs a product decision
3. 71.3 (unchanged) — type-error consistency
4. 71.4 (author order 4) — option validation alignment; reviewer moved to first, trivial and unblocking
5. 71.5 (unchanged position vs 71.3 by value) — decision + documentation, reviewer raised to Medium
