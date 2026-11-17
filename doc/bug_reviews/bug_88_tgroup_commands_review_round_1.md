# Bug 88 — TGroup Commands Review (Round 1)

**Module:** `io.velo.command.TGroup` (commands: `TIME`, `TTL`, `TYPE`; `PTTL` delegates to `TGroup.ttl(true)` from `PGroup`; `TOUCH` is not implemented — unknown-command path, consistent with the 85.15 fix)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete — 88.4 `9751d1fe`, 88.1+88.5 `cda79946`, 88.2 `c0734b06` (+ `e40b22ca` refinement), 88.3 `1c5ec017` (+ direct unit tests `35523b1b`); two review-follow-up refactors `e40b22ca`/`65ec4f07` (existence-only `getCv` sites → `exists`); re-review pending
**Review branch:** `review/tgroup`
**Reviewed commit:** `569ea2c021328bd143bb4d1bcbb719c1c31b44c8`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/TGroup.java` — whole class (1-188): CommandRegistry block (20-45), `parseSlots()` (79-92), `handle()` (99-121), `type()` (123-163), `ttl(boolean)` (165-187)
- `src/main/java/io/velo/BaseCommand.java` — `getExpireAt()` (707-725), `getShortType()` (727-747), `getCv()` (749-798, incl. the big-string full read at 781-795), `exists()` (1285-1303)
- `src/main/java/io/velo/persist/OneSlot.java` — `getExpireAt()` (1607-1650)
- `src/main/java/io/velo/persist/KeyLoader.java` — `typeAsByte*` constants (379-403), `transferToShortType()` (405-425), `getExpireAt()`/`getExpireAtAndSeqByKey()` (826-853)
- `src/main/java/io/velo/persist/KeyBucket.java` — `getExpireAtAndSeqByKey()` (689-730)
- `src/main/java/io/velo/persist/Wal.java` — `V.isExpired()` (64-78)
- `src/main/java/io/velo/persist/BigStringFiles.java` — `getBigStringBytes()` (351-369)
- `src/main/java/io/velo/CompressedValue.java` — `isHash()` (394-403), `isList()`/`isSet()` (405-436), `isExpired()` (569-576), `isTypeString()` (596-614), constants `NO_EXPIRE`/`SP_TYPE_*`/`KEY_MAX_LENGTH` (20-99)
- `src/main/java/io/velo/type/RedisHashKeys.java` — `keysKey()`/`fieldKey()` (112-127)
- `src/main/java/io/velo/command/HGroup.java` — `checkSplitKeyType()` (472-487), `getRedisHashKeys()`/`saveRedisHashKeys()` (489-522), `isUseHH()` (554-564), split-mode hincrby path (1570-1617)
- `src/main/java/io/velo/command/EGroup.java` — `parseSlots()` (106-134), `handle()` (141-172), `exists()` (174-235), `expire()` tail (276-332), `expiretime()` (334-351)
- `src/main/java/io/velo/command/PGroup.java` — `pttl` delegation (278-288)
- `src/main/java/io/velo/command/SGroup.java` — `set()` keep-ttl path (955-1007); split-hash comment (2371-2372)
- `src/main/java/io/velo/command/RGroup.java` — restore split-hash existence check (537-546)
- `src/main/java/io/velo/MultiWorkerServer.java` — reply path, `ErrorReply.FORMAT` → `WRONG_NUMBER` rewrite (594-634, 655-710)
- `src/main/java/io/velo/command/CommandRegistry.java` (1-60) + registration call sites — verified arity is COMMAND-metadata only; no dispatch-time arity enforcement exists (grep over `src/main/java`: no `.arity`/`getArity` consumer)
- `src/main/java/io/velo/reply/ErrorReply.java` — `FORMAT`/`WRONG_NUMBER`/`KEY_TOO_LONG` (11-30, 36); `src/main/java/io/velo/reply/BulkReply.java` — constructors (27-54)
- `src/main/java/io/velo/decode/Request.java:236`, `src/main/java/io/velo/decode/RESP.java:210` — cmd lowercasing
- `src/test/groovy/io/velo/command/TGroupTest.groovy` — whole file (1-217)

Reference docs (ground truth — local Redis 7.2 source `~/ws/redis`):

- `~/ws/redis/src/expire.c` — `ttlGenericCommand()` (688-710): rounds to nearest `((ttl+500)/1000)`, clamps `if (ttl < 0) ttl = 0`, `-2`/`-1` semantics
- `~/ws/redis/src/db.c` — `typeCommand()` (1176-1180): `lookupKeyRead` + status reply
- `~/ws/redis/src/server.c` — `timeCommand()` (4547-4551): `[unixtime, ustime-unixtime*1e6]`
- Redis command-table arity: `TIME`/`TTL`/`TYPE` arity enforced generically by `processCommand` in Redis; Velo has no equivalent dispatcher check

Baseline: `./gradlew :test --tests "io.velo.command.TGroupTest"` → BUILD SUCCESSFUL at the reviewed commit.

Prior rounds checked for overlap: STATE.md module table — no prior TGroup round; TTL-family semantics were not in bug_75 (EGroup: expire/expireat/expiretime fixes were GT/overflow/type-marker/key-length); bug_84 (PGroup) covered the `pttl` delegation target only for reply plumbing, not TTL math.

### Reviewer verification scope (AI agent 2, added during verification)

Additional files read (method + line ranges):

- `src/main/java/io/velo/persist/OneSlot.java` — `get()` (1751-1796), `getShortType()` (1684-1732), `getExpireAt()` (1615-1652)
- `src/main/java/io/velo/persist/BigStringFiles.java` — `getBigStringBytes()` (351-381), the big-string LRU setup (263-277)
- `src/main/java/io/velo/CompressedValue.java` — `decode()` (869-927), `onlyReadSpType()` (759-770), `isBigString()` (378-382), `isHash()`/`isList()`/`isSet()` (394-436), `VALUE_MAX_LENGTH` (101)
- `src/main/java/io/velo/type/RedisHH.java` — `PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX` (21)
- `src/main/java/io/velo/command/HGroup.java` — `hmset()` split path (1866-1930), `hgetdel()` split path (1319-1360), `hgetex()` split path (1014-1080), `saveRedisHashKeys()` (500-522), `isUseHH()` (554-564)
- `src/main/java/io/velo/command/EGroup.java` — `expire()` (276-332), `expiretime()` (334-351)
- `src/main/java/io/velo/persist/SegmentBatch2.java:204`, `src/main/java/io/velo/persist/Wal.java:445`, `src/main/java/io/velo/persist/ChunkWalGroupRebuilder.java:127` — the persisted short-type stamping (constraint for the 88.3 fix)
- `src/main/java/io/velo/reply/` — the reply class list (no `StatusReply` exists)
- `src/test/groovy/io/velo/command/TGroupTest.groovy` — re-read (1-217); `src/main/java/io/velo/mock/InMemoryGetSet.java`, `src/main/java/io/velo/mock/ByPassGetSet.java` — the bypass harness

Reference docs re-read: `~/ws/redis/src/expire.c` (688-710), `~/ws/redis/src/server.c` (3798-3799 generic arity check, 4547-4551 `timeCommand()`), `~/ws/redis/src/db.c` (1176-1180 `typeCommand()`), `~/ws/redis/src/commands/time.json` (`"arity": 1`).

Verification artifacts: temp Spock spec (5 methods, one per finding) — source kept at `/tmp/opencode/Bug88ReviewTempSpec.groovy`, removed from the tree after reproduction; live Velo `:41100` (HH mode) and `:41101` (split mode) vs live Redis 7.2.11 `:7777`; full transcript in `doc/bug_reviews/logs/20261117_204500_tgroup_review_agent2_bug_88.log`.

## Finding 1 (Bug 88.1): TTL truncates instead of rounding to nearest — off-by-one vs Redis on every fresh seconds TTL

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`
**Reviewer note (agent 2):** confirmed by temp spec + live differential (Velo `TTL`=99 vs Redis `TTL`=100 for a fresh `SETEX k 100 v`). One cited detail is refined: the "2500 ms → Redis answers 3" example is only true when the remaining time is *exactly* 2500 ms; measured a few ms later Redis answers 2 as well (the divergence needs a remainder ≥ 500 ms). The existing `TGroupTest.groovy:115-121` assertion becomes flaky (2 or 3) once the rounding lands — see Review Feedback.

### Cited files / lines
```java
// TGroup.java:185-186
var ttlMilliseconds = expireAt - System.currentTimeMillis();
return new IntegerReply(isMilliseconds ? ttlMilliseconds : ttlMilliseconds / 1000);
```
```c
/* ~/ws/redis/src/expire.c:703-708 */
if (ttl < 0) ttl = 0;
...
addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
```

### Root cause
`ttl()` converts the remaining milliseconds to seconds with plain integer division (truncation). Redis rounds to nearest: `(ttl+500)/1000`. Seconds-based writes (`SETEX k 100 v`, `EXPIRE k 100`, `SET k v EX 100`) store `expireAt = now + 100_000`, so the first `TTL` observes ~99_999 ms remaining: Redis replies 100, Velo replies 99. The existing test codifies the divergence: `TGroupTest.groovy:115-121` sets `expireAt = now + 2500` and asserts 2, where Redis answers 3 (`(2500+500)/1000`).

### Trigger / impact
Deterministic, not a race: any key with a sub-second remainder ≥ 0 ms and < 500 ms... more precisely, whenever `ttlMilliseconds % 1000 != 0` after truncation differs from Redis' rounding — with a fresh `SETEX/EXPIRE` of N seconds the client always sees N-1 instead of N. Clients that schedule refreshes from `TTL` (cache stampede avoiders, lock renewers) renew one second early or mis-compare against their intended N. PTTL (`isMilliseconds=true`) is exact and unaffected.

### Suggested fix direction
`return new IntegerReply(isMilliseconds ? ttlMilliseconds : (ttlMilliseconds + 500) / 1000);` — mirror Redis' rounding. Update the TGroupTest expectation (2500ms → 3) and add a fresh-SETEX-style regression (100_000ms-ish remainder → 100, not 99). Cross-module sibling (do not fold in here, EGroup round closed): `EGroup.expiretime()` (EGroup.java:350) truncates `expireAt / 1000` where Redis rounds absolute seconds too.

## Finding 2 (Bug 88.2): TTL answers -2 (key does not exist) for a split-mode hash that TYPE reports as "hash"

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`
**Reviewer note (agent 2):** confirmed by temp spec (TYPE=hash, TTL=-2) and by live differential on a split-mode instance (`:41101`, `persist.isHashSaveMemberTogether=false`) and on a **default** deployment with the `h_not_hh_` key prefix: `TYPE h`→`hash`, `TTL h`→`-2`, `PTTL h`→`-2`, `EXISTS h`→`0` (Redis: `hash`/`-1`/`-1`/`1`). The finding understates the blast radius: `EXISTS` and `EXPIRE` are broken for the same keys.

### Cited files / lines
```java
// TGroup.java:136-144 — type() has the split-hash fallback
var cv = getCv(slotWithKeyHash);
if (cv == null) {
    // hash keys changed
    var keysKey = RedisHashKeys.keysKey(Wal.keyString(keyBytes));
    cv = getCv(slot(keysKey));
    if (cv == null) {
        return TYPE_NONE;
    }
}

// TGroup.java:176-179 — ttl() has no fallback
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var expireAt = getExpireAt(slotWithKeyHash);
if (expireAt == null) {
    return new IntegerReply(-2);
}
```
```java
// HGroup.java:473-474 — the split layout keeps no cv under the main key
// "The split layout stores the hash out of line, the main key cv is never read by the hash commands"
// RGroup.java:544-545 — restore already special-cases it:
// "in the split hash mode the hash is stored under the derived keys key, the main key does not exist"
```

### Root cause
When hashes are stored split (`isHashSaveMemberTogether=false`, `HGroup.isUseHH()` at HGroup.java:554-564), the main key has no CV at all; the field index lives under `h_k_{key}` (`RedisHashKeys.keysKey()`, RedisHashKeys.java:116-118). `type()` was taught this fallback (TGroup.java:136-144), `ttl()` was not: `getExpireAt()` sees no key and TTL answers -2 — Redis' code for "key does not exist" — for a hash that exists and that Velo itself reports as `hash`.

### Trigger / impact
On a split-hash deployment: `HSET h f v` then `TYPE h` → `hash` but `TTL h` → `-2` (Redis: `-1`), and `PTTL h` → `-2` likewise (PGroup.java:284-288 delegates into the same `ttl(true)`). Self-contradictory replies from the same server; clients keying on -2 delete their bookkeeping for a live hash. Redis has one key space — a hash always answers -1 or its real TTL.

### Suggested fix direction
Mirror `type()`'s fallback in `ttl()`: when `expireAt == null` (and not the FORMAT/KEY_TOO_LONG paths), check `getCv(slot(RedisHashKeys.keysKey(key)))`; if present, answer -1 (the split index carries no top-level expire semantics today; `saveRedisHashKeys` defaults to `NO_EXPIRE`, HGroup.java:500-522). Cross-module siblings for the reviewer to note (not this report's fix scope): `BaseCommand.exists()` (1285-1303, used by EXISTS) and `EGroup.expire()` also lack the fallback — EGroup's round is closed, listed here only so the TTL fix follows the same product decision.

## Finding 3 (Bug 88.3): TYPE decompresses the whole value — for big strings it loads the entire file — for an O(1) metadata query

**Severity:** Low (refined from Medium — see reviewer note)
**Class:** Performance
**Status:** `PARTIALLY CONFIRMED`
**Reviewer note (agent 2):** the *core* claim is confirmed — `TYPE` performs O(value-size) work and, for a big string, reads the whole big-string file (the temp spec stack trace proves `TGroup.type()` → `BaseCommand.getCv():782` enters the big-string branch; live: the first `TYPE` on a 1 MB big string costs 0.68 ms vs 0.03 ms warm). Two corrections: (a) the value is **not** zstd-decompressed — `CompressedValue.decode()` only copies the *compressed* bytes, so the cost is I/O + a copy, not decompression; (b) the impact is bounded by `CompressedValue.VALUE_MAX_LENGTH` = 1 MB, hence Low, not Medium. The "missing big-string file turns TYPE into an error" tail is confirmed (`BaseCommand.java:790` throws `BigStringFileMissingException`). The suggested fix direction needs a redesign — see Review Feedback (the short-type byte cannot tell geo from string, and `transferToShortType` is persisted on disk).

### Cited files / lines
```java
// TGroup.java:133-135
// need not decompress at all, todo: optimize
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var cv = getCv(slotWithKeyHash);

// BaseCommand.java:781-795 — getCv() on a big string loads ALL bytes
if (cv.isBigString()) {
    ...
    var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true);
    if (bigStringBytes == null) {
        throw new BigStringFileMissingException(...);
    }
    cv.setCompressedData(bigStringBytes);
}
```
```c
/* ~/ws/redis/src/db.c:1176-1180 — O(1) */
o = lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH);
addReplyStatus(c, getObjectTypeName(o));
```

### Root cause
`type()` only needs the type marker, but calls `getCv()`, which decodes the CV (zstd decompression for dict-compressed values) and, for big-string keys, reads the whole big-string file into memory (through the LRU; evicting real workload data). The in-code `todo: optimize` acknowledges it. `BaseCommand.getShortType()` (727-747) exists for exactly this and has a precedent consumer (`HGroup.checkSplitKeyType()`, HGroup.java:481). Secondary correctness tail: if the big-string file is missing, TYPE throws `BigStringFileMissingException` and the client gets a generic error instead of `string`.

### Trigger / impact
Every `TYPE` on a big-string or compressed key does O(value-size) work + allocation; with values of tens of MB (the big-string design point) one TYPE per request can dominate latency and churn the big-string LRU. Redis TYPE is O(1). Also inconsistent with Velo's own design (`getShortType` javadoc: "Gets the short type byte of a key without reading the whole value").

### Suggested fix direction
Switch `type()` to `getShortType(slotWithKeyHash)` + keep the keysKey fallback (getShortType of the derived key). Pitfall the fixer must handle: `KeyLoader.transferToShortType()` (KeyLoader.java:409-425) currently maps **geo → string** (line 410) while `type()` correctly answers `zset` for geo (TGroup.java:155, Redis semantics — geo keys are zsets); the mapping must be extended in lockstep (geo → `typeAsByteZSet`) — and that change also affects scan/KEYS `TYPE` filters (see cross-module notes). Big-string/number/dict types already map to string via `isTypeString(spType)` (CompressedValue.java:602-604), so the common path is safe.

## Finding 4 (Bug 88.4): TIME ignores extra arguments instead of answering wrong-number-of-arguments

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`
**Reviewer note (agent 2):** confirmed by temp spec (`execute('time x')` → `MultiBulkReply`, no error) and live: Velo `:41100` answers `TIME x` with the time array, Redis 7.2 answers `ERR wrong number of arguments for 'time' command`. The suggested fix direction is correct — `ErrorReply.FORMAT` is rewritten to exactly that Redis text by the net layer.

### Cited files / lines
```java
// TGroup.java:101-109 — no data.length check for time
if ("time".equals(cmd)) {
    var now = Instant.now();
    ...
    return new MultiBulkReply(...);
}
```
```java
// MultiWorkerServer.java:612-613 — the only arity-ish net-layer guard needs ErrorReply.FORMAT as sentinel
if (reply == ErrorReply.FORMAT) {
    reply = ErrorReply.WRONG_NUMBER(cmd);
}
```

### Root cause
`CommandRegistry` arity is metadata for the `COMMAND` family only (CommandRegistry.java:12-30 — no dispatch-time consumer; grep over `src/main/java` finds none), so each `handle()` owns its arity check. TTL and TYPE check `data.length != 2` and return `ErrorReply.FORMAT`, which the net layer rewrites to Redis' `wrong number of arguments for '<cmd>' command` (MultiWorkerServer.java:612-613/675-676/702-703). The `time` branch checks nothing: `TIME junk` returns the time array. Redis (generic arity check in `processCommand`, command table arity 1) answers `ERR wrong number of arguments for 'time' command`.

### Trigger / impact
`TIME x`, `TIME x y` → Velo returns the time reply; Redis returns an error. Clients that typo/pipeline wrongly get silent success. `TIME` is also the canonical client clock-sync command, so scripts varying on its arity get divergent behavior between Redis and Velo.

### Suggested fix direction
In the `time` branch: `if (data.length != 1) return ErrorReply.FORMAT;` — the existing net-layer rewrite turns it into the exact Redis text (returning `WRONG_NUMBER` directly would double-`ERR`-prefix nothing but FORMAT is the established sentinel; both work, FORMAT keeps one style with TTL/TYPE).

## Finding 5 (Bug 88.5): PTTL can return a small negative value; Redis clamps negative TTL to 0

**Severity:** Low
**Class:** Edge case
**Status:** `PARTIALLY CONFIRMED`
**Reviewer note (agent 2):** the clamp really is missing (`TGroup.java:185-186`), but the claimed impact is **not reachable** and one supporting argument is wrong. (a) The suggested reproduction does not work: injecting an expired CV through the bypass yields `-2`, not a negative number — every storage path filters expired keys before the value is returned (`Wal.V.isExpired()`, `OneSlot.getExpireAt()`, `KeyBucket.getExpireAtAndSeqByKey()`), and the residual race window is sub-millisecond, so `ttlMilliseconds` can only reach `-1`/`-2`, which the current truncation already maps to `0`. (b) The claim "after 88.1's rounding fix a -600 ms remainder would round to -1" is arithmetically wrong: Java integer division truncates *toward zero*, so `(-600+500)/1000 == 0` — the rounding fix alone maps the whole `(-1000, 0)` ms range to `0`, exactly like Redis' clamp. Verdict: the missing clamp is defense-in-depth, not a reachable bug; fold the one-line clamp into the 88.1 fix instead of fixing it standalone.

### Cited files / lines
```java
// TGroup.java:185-186 — no clamp
var ttlMilliseconds = expireAt - System.currentTimeMillis();
return new IntegerReply(isMilliseconds ? ttlMilliseconds : ttlMilliseconds / 1000);
```
```c
/* ~/ws/redis/src/expire.c:702-703 */
ttl = output_abs ? expire : expire-commandTimeSnapshot();
if (ttl < 0) ttl = 0;
```

### Root cause
All three storage paths pre-filter expired keys (`Wal.V.isExpired()` Wal.java:71-73; LRU check OneSlot.java:1643-1645; `KeyBucket.getExpireAtAndSeqByKey` KeyBucket.java:702), so `getExpireAt()` never returns an already-past timestamp. But the expiry test runs at one `System.currentTimeMillis()` read inside `getExpireAt()` and the subtraction at line 185 runs at a later one — a key expiring inside that window yields a negative result (e.g. PTTL → -3). Redis defends this exact race with an explicit clamp (expire.c:703); its TTL/PTTL output can never be negative.

### Trigger / impact
Race-only, microsecond window, but observable: PTTL returning a negative integer is impossible output in Redis; strict clients (or wrappers mapping negatives to "no TTL"/"missing") can mis-handle it. The seconds form is additionally masked by truncation finding 88.1 (small negatives truncate to 0) — after 88.1's rounding fix, a -400ms remainder would round to **0** correctly, but e.g. -600ms rounds to -1 — so the clamp becomes *required* once rounding lands, to avoid TTL answering -1 ("no expire") for a just-expired key.

### Suggested fix direction
`if (ttlMilliseconds < 0) ttlMilliseconds = 0;` before the reply (mirrors expire.c:703). Verify with a temp spec by injecting `expireAt = now - 400` through the bypass and calling `ttl(true)`/`ttl(false)` — with the clamp both answer 0/-2-consistent behavior instead of -400/-1-after-rounding. (Direct injection bypasses the storage filters, which is legitimate for proving the arithmetic; the live race is the same arithmetic.)

## Notes / non-findings

- **Arity error text for TTL/TYPE is correct on the wire.** `ErrorReply.FORMAT` is a sentinel rewritten to `WRONG_NUMBER(cmd)` at the network layer (MultiWorkerServer.java:612-613, 675-676, 702-703). Only in-process callers (tests) see "syntax error". Not a finding; recorded so later rounds don't re-investigate. (The known velo-wide `WRONG_NUMBER` stray-`*` prefix follow-up from bug_83 stays tracked there.)
- **TYPE geo → `zset` is correct** (TGroup.java:155; geo keys are zsets in Redis). BF keys answer `string` — matches core Redis (no module types in Velo's compat target).
- **TYPE as BulkReply vs Redis status reply** (`+string` vs `$6\r\nstring`): no generic StatusReply class exists in `io.velo.reply`; repo-wide simplification, all mainstream clients read both as strings. Not filed.
- **`isHash()` covers both `SP_TYPE_HH` and `SP_TYPE_HASH`** (CompressedValue.java:400-403) — HH-mode hashes TYPE correctly.
- **TTL -2/-1 IntegerReply semantics, TIME structure and epoch math are correct** (TGroup.java:102-108, 176-186 vs server.c:4547-4551 / expire.c:692-709).
- **Command names are lowercased at decode** (Request.java:236, RESP.java:210) — case-insensitive `TTL`/`Ttl` work.
- **KEY_TOO_LONG** on >256-byte keys is Velo's documented design guard (accepted in prior rounds, e.g. 75.5).
- **`expireAt == CompressedValue.NO_EXPIRE`** compares a boxed `Long` to the `long` constant 0 (CompressedValue.java:20) — unboxes safely, no Long-cache hazard.
- **Cross-module observations (other modules' reports, not re-filed here):**
  - `EGroup.expiretime()` truncates absolute seconds (EGroup.java:350) where Redis rounds — same family as 88.1, EGroup round closed.
  - `KeyLoader.transferToShortType()` maps geo → string (KeyLoader.java:410), so `SCAN ... TYPE zset` / `KEYS ... TYPE zset` miss geo keys and `TYPE string` matches them — bug_70/bug_80 module territory; also a constraint for the 88.3 fix.
  - `BaseCommand.exists()` (1285-1303) and `EGroup.expire()` lack the split-hash keysKey fallback (same family as 88.2) — EXISTS on a split hash answers 0 today.
  - `EXPIRE` on a split hash answers 0 ("key missing") where Redis would set the TTL — EGroup/base-layer, for a future round.

## Reviewer checklist (for AI agent 2)

- Finding 1 (88.1): temp Spock spec, `InMemoryGetSet` bypass: put key `a` with `cv.expireAt = System.currentTimeMillis() + 99_800` → `execute('ttl a')` asserts 99 today; Redis 7.2 differential: `SETEX k 100 v` + immediate `TTL k` → Velo 99 vs Redis 100 (fresh key guarantees ~99_9xx ms remainder). Also the existing TGroupTest.groovy:115-121 (2500ms → 2) documents the divergence: Redis answers 3.
- Finding 2 (88.2): bypass spec: `inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cv(SP_TYPE_HASH))` → `execute('type a')` == `TYPE_HASH` while `execute('ttl a')` == `IntegerReply(-2)` (and PGroup `pttl` path the same). Live: requires `isHashSaveMemberTogether=false` deployment + HSET.
- Finding 3 (88.3): static inspection — `type()` → `getCv()` → `BaseCommand.getCv():781-795` → `getBigStringBytes(uuid, ..., true)` full read; contrast `getShortType()` (BaseCommand.java:727-747) and its consumer HGroup.java:481. Perf variant: TYPE on a big-string key under load vs Redis.
- Finding 4 (88.4): `new TGroup('time', [['time'].bytes, 'x'.bytes] as byte[][], ...)` (or bypass `execute('time x')`) → returns MultiBulkReply, no error; Redis 7.2: `TIME x` → `ERR wrong number of arguments for 'time' command`.
- Finding 5 (88.5): bypass spec injecting `cv.expireAt = now - 400` proves the unclamped arithmetic (`ttl(true)` → ≤ -400); live Redis never emits negative PTTL/TTL (expire.c:703 clamp). Note the interaction with 88.1: rounding without clamping makes -600ms → -1.

**Checklist outcome (AI agent 2):** all five recipes were executed. 88.1 ✔ (reproduced; the "Redis answers 3" sub-claim is imprecise — see Review Feedback), 88.2 ✔ (reproduced, and reachable in the default deployment via the `h_not_hh_` prefix), 88.3 ✔ (static inspection + live cold-path measurement; the "zstd decompression" wording is wrong), 88.4 ✔ (reproduced live), 88.5 ✖ **the recipe does not reproduce** — the injected expired CV is filtered by the storage layer and answers `-2`, and the "-600 ms → -1 after rounding" claim is refuted by Java's truncation toward zero.

## Review Feedback (AI agent 2, round 1)

**Reviewer:** AI agent 2
**Reviewed commit:** `569ea2c021328bd143bb4d1bcbb719c1c31b44c8` (branch `review/tgroup`)
**Verification environment**

- Spock temp spec `src/test/groovy/io/velo/command/Bug88ReviewTempSpec.groovy` (5 methods, one per finding; `InMemoryGetSet` bypass + `BaseCommand.mockAGroup()`, same harness as `TGroupTest.groovy`). Source kept at `/tmp/opencode/Bug88ReviewTempSpec.groovy`, temp spec removed from the tree after reproduction.
- Live Velo `build/libs/velo-1.0.0.jar`: `:41100` (HH mode, default config) and `:41101` (`persist.isHashSaveMemberTogether=false`, split mode).
- Live Redis **7.2.11** on `:7777` for the differential.
- Upstream ground truth: `~/ws/redis/src/expire.c`, `src/db.c`, `src/server.c`, `src/commands/time.json`.
- Full transcript: `doc/bug_reviews/logs/20261117_204500_tgroup_review_agent2_bug_88.log`.

### Finding 88.1 — TTL truncates instead of rounding to nearest — `CONFIRMED (reproduced)`

**Evidence — temp spec stdout**

```
88.1 fresh SETEX 100s: velo TTL=99  redis-formula=100  velo PTTL=99959
88.1 99600ms left: velo TTL=99  redis-formula=100
88.1 2500ms left: velo TTL=2  redis-formula=2 (exact 2500 -> 3)
```

**Evidence — live differential (Velo `:41100` vs Redis `:7777`)**

```
$ redis-cli -p 41100 setex sk 100 v   -> OK
$ redis-cli -p 41100 ttl sk           -> 99        [redis: 100]
$ redis-cli -p 41100 pttl sk          -> 99994     [redis: 99998]
$ redis-cli -p 41100 expire sk 100    -> 1
$ redis-cli -p 41100 ttl sk           -> 99        [redis: 100]
$ redis-cli -p 7777   setex sk2 100 v -> OK
$ redis-cli -p 7777   ttl sk2         -> 100
```

**Ground truth** — `~/ws/redis/src/expire.c:700-708`:

```c
ttl = output_abs ? expire : expire-commandTimeSnapshot();
if (ttl < 0) ttl = 0;
...
addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
```

**Refinement of the finding's example.** The divergence is *not* "every fresh seconds TTL"; the two formulas differ only when the remaining milliseconds have a remainder ≥ 500 (`ttl/1000` vs `(ttl+500)/1000`). A fresh `SETEX k 100 v` leaves ~99 990 ms (remainder 990 ≥ 500) so the client always sees the off-by-one there, but the cited `2500 ms` case is *borderline*: Redis answers `3` only when the remaining time is exactly 2500 ms; measured a few ms later Redis answers `2` as well (temp spec line `88.1 2500ms left: velo TTL=2 redis-formula=2`). The bug is real, the example is imprecise.

**Consequence for the fix.** The existing regression `TGroupTest.groovy:115-121` (`expireAt = now + 2500` → expects `2`) becomes **flaky** after the rounding fix: the remaining time is 2499 or 2500 ms depending on the clock, so the reply is `2` or `3`. The fixer must not simply change the expectation to `3` (as the finding suggests) — use a deterministic remainder (e.g. `now + 2400` → `(2399+500)/1000 == 2` either way) and/or assert the Redis formula computed from the measured remaining time. PTTL is unaffected (`isMilliseconds` path) — the temp spec shows `PTTL=99959`, exact.

### Finding 88.2 — TTL/PTTL answer -2 for a split-mode hash — `CONFIRMED (reproduced)`

**Evidence — temp spec stdout**

```
88.2 split hash: TYPE=io.velo.reply.BulkReply@becb93a  TTL=-2
```

(`TGroup.TYPE_HASH` == `BulkReply("hash")`; the main key has no CV, the index lives under `h_k_{a}`.)

**Evidence — live differential, split-mode instance `:41101` (`persist.isHashSaveMemberTogether=false`)**

```
$ redis-cli -p 41101 hset h f v       -> 1
$ redis-cli -p 41101 type h           -> hash      [redis: hash]
$ redis-cli -p 41101 ttl h            -> -2        [redis: -1]
$ redis-cli -p 41101 pttl h           -> -2        [redis: -1]
$ redis-cli -p 41101 exists h         -> 0         [redis: 1]
$ redis-cli -p 41101 expire h 100     -> 0         [redis: 1]
$ redis-cli -p 41101 hget h f         -> v         [redis: v]
```

**Evidence — reachable in the *default* deployment** (HH mode `:41100`, key with the split prefix `RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX` = `"h_not_hh_"`, `RedisHH.java:21`; `HGroup.isUseHH()` at `HGroup.java:554-564`):

```
$ redis-cli -p 41100 hset h_not_hh_k f v -> 1
$ redis-cli -p 41100 type h_not_hh_k     -> hash     [redis: hash]
$ redis-cli -p 41100 ttl h_not_hh_k      -> -2       [redis: -1]
$ redis-cli -p 41100 pttl h_not_hh_k     -> -2       [redis: -1]
$ redis-cli -p 41100 exists h_not_hh_k   -> 0        [redis: 1]
```

So the finding is confirmed and is *not* limited to a non-default configuration.

**Two refinements.**

1. The blast radius is larger than reported: `EXISTS` (`BaseCommand.exists()`, 1285-1303) and `EXPIRE` (`EGroup.expire()`, 276-332 — `getCv(mainKey)` is null → `REPLY_0`) are broken for exactly the same keys. They are cross-module (already flagged in the report's notes), but the fix must not create an *inconsistent* state: if `TTL` is taught the split-hash fallback and `EXPIRE` is not, then `EXPIRE h 100` still answers 0 while `TTL h` answers -1 — still a divergence, but a different one. The product decision (does the split hash own a top-level TTL?) has to be taken once and applied consistently.
2. The fallback answer cannot be "the TTL of the index": `saveRedisHashKeys(rhk, key)` stores the index with `CompressedValue.NO_EXPIRE` (`HGroup.java:500-522`), so the only correct value today is `-1` ("exists, no expire"). The fix suggested in the report (answer `-1` when the derived key exists) is right for the current storage layout.

### Finding 88.3 — TYPE does O(value-size) work / loads the whole big-string file — `PARTIALLY CONFIRMED`

**Evidence that the big-string file is read by TYPE** — temp spec stack trace (the spec itself fails because the isolated test JVM has no `LocalPersist` slots, which is exactly the point: the big-string branch of `getCv` is reached *from* `type()`):

```
Caused by: java.lang.NullPointerException: Cannot load from object array because "this.oneSlots" is null
    at io.velo.persist.LocalPersist.oneSlot(LocalPersist.java:108)
    at io.velo.BaseCommand.getCv(BaseCommand.java:782)
    at io.velo.command.TGroup.type(TGroup.java:135)
    at io.velo.command.TGroup.handle(TGroup.java:116)
```

**Evidence of the cost** — live, 1 MB big string on `:41100` (raw socket, `TYPE` after `SET`):

```
TYPE #0: 0.677 ms  (cold: whole big-string file read + LRU insert)
TYPE #1: 0.110 ms  (warm)  TYPE #2: 0.046 ms  TYPE #3: 0.033 ms  TYPE #4: 0.031 ms
redis-benchmark -n 200 -c 1 type big   -> avg 0.068 ms
redis-benchmark -n 200 -c 1 type small -> avg 0.062 ms
```

**Corrections to the finding.**

1. **No decompression happens.** `CompressedValue.decode()` (`CompressedValue.java:881-927`) only *copies* the compressed bytes into `cv.compressedData`; the type predicates (`isHash()`, `isList()`, …) read `dictSeqOrSpType` only. So the cost of `TYPE` is "read the whole encoded value from WAL/LRU/key bucket + copy it into a new array" (`OneSlot.get()` at 1751-1796, `BaseCommand.getCv()` at 749-798), plus, for a big string, the **full big-string file read** (`BaseCommand.java:781-795` → `getBigStringBytes(uuid, bucketIndex, keyHash, true)`, `BigStringFiles.java:369-381`, which also inserts the whole value into the big-string LRU). The finding's wording "decodes the CV (zstd decompression for dict-compressed values)" is wrong — no zstd call is on this path.
2. **Severity is Low, not Medium.** The payload is bounded by `CompressedValue.VALUE_MAX_LENGTH = 1 MB` (`CompressedValue.java:101`) and by the ≥ 256 KB big-string threshold (`ConfForGlobal.bigStringNoMemoryCopySize`). The measured cold-path cost is ~0.7 ms for the largest possible value, and the steady-state cost is an LRU hit. The finding's *correctness* tail — a missing big-string file makes `TYPE` throw `BigStringFileMissingException` (`BaseCommand.java:790`) instead of answering `string` — is confirmed and is the more interesting half of the finding.

**The suggested fix direction must be redesigned before it is implemented.** A naive "use `getShortType()`" swap breaks `TYPE` for geo keys: `KeyLoader.transferToShortType()` (`KeyLoader.java:409-425`) maps **geo → `typeAsByteString`** while `type()` correctly answers `zset` (`TGroup.java:155`), so the short-type byte cannot distinguish a geo key from a string key. Worse, `transferToShortType()` is used by the *persist* layer to stamp the short type into the persisted value meta (`SegmentBatch2.java:204`, `Wal.java:445`, `ChunkWalGroupRebuilder.java:127`), so changing the mapping changes what is written on disk and the persisted data of existing deployments would disagree with the new mapping. Options for the fixer, in order of preference:

- **Option A (recommended):** keep `getCv()` but make the type read *header-only* — add a `getTypeMarker()`/`getCvTypeOnly()` path that decodes the CV header (seq / expireAt / spType) and **skips** the big-string body load; the big-string branch of `getCv()` is the only part that reads the payload. This keeps the geo/zset distinction, touches no persisted format, and also removes the `BigStringFileMissingException` tail for `TYPE`.
- Option B: extend the short-type space with a dedicated geo byte — requires a persist-format decision (PVM contents) and a migration story; not acceptable inside a bug-fix commit.
- Option C: use `getShortType()` as a fast path and fall back to `getCv()` only for the ambiguous `typeAsByteString` bucket — this keeps correctness but still reads the value for the *common* case (strings), so it barely helps.

Note for the scan/KEYS cross-module constraint already recorded in the report's notes: `SCAN ... TYPE zset` / `KEYS ... TYPE zset` currently miss geo keys because of the same `transferToShortType()` mapping — that is `bug_70`/`bug_80` territory and must not be changed as part of this fix.

### Finding 88.4 — TIME ignores extra arguments — `CONFIRMED (reproduced)`

**Evidence — temp spec stdout**

```
88.4 'time x' returned: io.velo.reply.MultiBulkReply@4fb99927 (MultiBulkReply)
```

**Evidence — live differential**

```
$ redis-cli -p 41100 time x  -> 1794919142 / 953831
$ redis-cli -p 7777   time x  -> ERR wrong number of arguments for 'time' command
```

**Ground truth** — `~/ws/redis/src/server.c:3798-3799` (generic arity check in `processCommand`) and `src/commands/time.json` (`"arity": 1`). Velo has no dispatch-time arity check (grep over `src/main/java` finds no consumer of `CommandEntry.arity`), so the `time` branch owns it — the finding's root-cause analysis is correct.

The suggested fix (`if (data.length != 1) return ErrorReply.FORMAT;`) produces exactly the Redis text: `MultiWorkerServer.java:612-613 / 675-676 / 702-703` rewrite the `ErrorReply.FORMAT` sentinel into `ErrorReply.WRONG_NUMBER(cmd)`.

### Finding 88.5 — no negative clamp on TTL/PTTL — `PARTIALLY CONFIRMED`

**What is confirmed.** The clamp is genuinely absent (`TGroup.java:185-186`), and Redis has one (`expire.c:702-703`).

**What is refuted — the reproduction recipe.** The finding suggests injecting `expireAt = now - 400` through the bypass and observing a negative reply. It does not work:

```
88.5 expired cv injected: TTL=-2 (the storage layer filtered it, no negative value)
```

`BaseCommand.getExpireAt()` (707-725) → `getCv()` → `cv.isExpired()` (`CompressedValue.java:569-576`) returns null for an expired CV, so `ttl()` answers `-2`. In production the same filtering happens in all three storage paths: `Wal.V.isExpired()` (`Wal.java:71-73`), `OneSlot.getExpireAt()` LRU branch (`OneSlot.java:1641-1645`), `KeyBucket.getExpireAtAndSeqByKey()` (`KeyBucket.java:702`).

**What is refuted — the "becomes -1-capable after 88.1" argument.** Java integer division truncates *toward zero*, so the rounding formula never produces `-1` for a small negative:

```
88.5 java arithmetic: (-600+500)/1000 = 0, (-400+500)/1000 = 0, (-600)/1000 = 0
```

The whole `(-1000, 0)` ms range maps to `0` — exactly what Redis' clamp produces. A negative below `-1000 ms` cannot be reached: the residual race window between the expiry check inside `getExpireAt()` and the subtraction in `ttl()` is sub-millisecond, so `ttlMilliseconds` can only be `-1`/`-2` there.

**Disposition.** The impact claimed by the finding is not reachable → this is *not* a standalone bug. Recommended handling: fold the one-line clamp (`if (ttlMilliseconds < 0) ttlMilliseconds = 0;`) into the **88.1** fix as cheap defense-in-depth (it also makes the intent explicit for future storage paths that may not filter expired keys). Do not spend a separate commit on it.

### Recommended fix order

1. **88.4** (Low, 2 lines) — `TIME` arity check. Trivial, self-contained, zero risk.
2. **88.1 + 88.5** (Medium) — TTL rounding `(ttl+500)/1000` **plus** the negative clamp; update `TGroupTest.groovy:115-121` to a deterministic remainder (do *not* simply change `2` → `3`, that is flaky) and add a fresh-`SETEX`-style regression (`now + 100_000` → `100`).
3. **88.2** (Medium) — split-hash `TTL`/`PTTL` fallback (answer `-1` when the derived `h_k_{key}` index exists), together with a decision on `EXISTS`/`EXPIRE` parity so the three commands cannot disagree. Keep it in this module (TGroup) and record the `EXISTS`/`EXPIRE` siblings as follow-ups.
4. **88.3** (Low) — `TYPE` header-only read (Option A). Deliberately last: it touches the read path, needs the geo/zset distinction to be preserved, and must not change the persisted short-type mapping.

Each fix = one commit, TDD first (failing regression test), then `./gradlew :test --tests "<focused class>"` green, then a JaCoCo check of the changed lines (`scripts/jacoco_cover.py`).

## Author Response (AI agent 1, round 1)

All five verdicts accepted. Both of my refuted sub-claims are retracted with evidence below; no verdict is disputed. Every reviewer correction was re-checked against the codebase before acceptance, not taken on trust.

**88.1 — accept `CONFIRMED (reproduced)`, accept the refinement.** The remainder arithmetic is right: the two formulas differ iff `ttlMilliseconds % 1000 >= 500` (Velo `k`, Redis `k+1`), so a fresh `SETEX` (remainder ~990-999) always diverges, while the `2500 ms` example is borderline — my "Redis answers 3" holds only at exactly 2500 ms remaining; a few ms later Redis answers 2. I also accept the flaky-test warning: after the rounding lands, `TGroupTest.groovy:115-121` (`now + 2500`) sits exactly on the rounding boundary (remaining 2499/2500 → 2/3). The fix will use a deterministic remainder (e.g. `now + 2400` → 2 both sides of the boundary, or `now + 99_800` → 100) as the reviewer suggests, not a bare `2 → 3` flip.

**88.2 — accept `CONFIRMED (reproduced)`, accept the blast-radius expansion.** Verified: `RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX = "h_not_hh_".getBytes()` (RedisHH.java:21) and `HGroup.isUseHH()` (HGroup.java:554-564) returns `false` for keys with that prefix *before* consulting `isHashSaveMemberTogether` — so the split layout is reachable on a default deployment, not only via the non-default config. My finding's trigger understated this. I accept both refinements: (1) `EXISTS`/`EXPIRE` parity is a product decision to take once (they stay cross-module follow-ups, but the 88.2 fix must be written knowing they exist); (2) the fallback answer must be `-1`, because `saveRedisHashKeys` stores the index with `NO_EXPIRE` (HGroup.java:500-522) — the index's own expire is not the hash's TTL.

**88.3 — accept `PARTIALLY CONFIRMED`, retract my "zstd decompression" wording, accept Low.** Verified: `CompressedValue.decode()` (CompressedValue.java:881-927) parses the header (seq/expireAt/keyHash/spType) and *copies* the compressed payload — no zstd call exists on the TYPE path; the type predicates read `dictSeqOrSpType` only. My "decodes (zstd decompression)" claim was wrong. Verified: `CompressedValue.VALUE_MAX_LENGTH = 1024*1024` (CompressedValue.java:101) is enforced in the write path (`SGroup.set()` SGroup.java:852-853, same check across LGroup/HGroup/GGroup), so a big string is at most 1 MB — my "tens of MB" premise was wrong, the measured 0.68 ms cold cost bounds the impact, and Low is the right severity. The confirmed half worth keeping is the correctness tail (`BigStringFileMissingException` → TYPE errors instead of answering `string`, BaseCommand.java:790). I accept the fix-direction redesign: Option A (header-only CV read that skips the big-string body load) preserves the geo→zset distinction and touches no persisted format; the constraint that `transferToShortType()` output is stamped into persisted meta (Wal.java:443-446, SegmentBatch2.java:204, ChunkWalGroupRebuilder.java:127 — re-verified) rules out changing the mapping inside this fix.

**88.4 — accept `CONFIRMED (reproduced)`.** No corrections to consider; the fix direction stands as written.

**88.5 — accept `PARTIALLY CONFIRMED`, retract both refuted arguments, accept the disposition.** (a) My injection recipe was wrong, retracted: the bypass path filters expired CVs too (`getExpireAt()` → `getCv()` → `cv.isExpired()` → null → -2, BaseCommand.java:713-725/777-779), so `expireAt = now - 400` can never reach the arithmetic. (b) My "-600 ms rounds to -1" arithmetic was wrong, retracted: Java division truncates toward zero, `(-600+500)/1000 == 0` — the entire `(-1000, 0)` ms range maps to 0 with or without the clamp, same as Redis. One precision note (not a dispute): for the **PTTL form** (`isMilliseconds=true`) the raw negative milliseconds *are* the reply — the sub-ms race between the expiry filter's clock read inside `getExpireAt()` and the subtraction at TGroup.java:185 can still emit e.g. `PTTL → -1` where Redis always clamps to 0 — so the folded clamp covers a real (if astronomically narrow) client-visible case in the ms form, not pure decoration. Disposition accepted regardless: fold `if (ttlMilliseconds < 0) ttlMilliseconds = 0;` into the 88.1 commit, no separate commit.

**Fix order accepted as recommended:** 88.4 → 88.1 (+88.5 clamp) → 88.2 → 88.3, one commit each, TDD + JaCoCo per commit.

## Fix Log (AI agent 1, round 1 fix phase)

Fix order as accepted: 88.4 → 88.1+88.5 → 88.2 → 88.3. Every bug: failing regression first (watched RED), minimal fix, focused spec green, JaCoCo on the changed lines. All regression tests live inside the command's single test method (`test handle - time` / `test ttl` / `test type`) per the command-test structure rule.

| Bug | Failing test evidence (RED) | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 88.4 | `TGroupTest.test handle - time`: `SpockComparisonFailure at TGroupTest.groovy:74` — `time x` returned the time `MultiBulkReply` instead of the error | `9751d1fe` `fix: time answers wrong number of arguments error for extra args` | TGroupTest 11/11 green (+ PGroupTest 22/22) | TGroup.java:102 arity check — both branches covered, line 104 (FORMAT return) covered |
| 88.1 + 88.5 | `TGroupTest.test ttl`: `SpockComparisonFailure at TGroupTest.groovy:133` — a ~99.8 s remainder answered 99; deterministic both-sides cases (`now+99_800` → 100, `now+99_100` → 99) replace the boundary-flaky `now+2500` case as the reviewer required | `cda79946` `fix: ttl rounds to nearest like redis and clamps negative to 0` | TGroupTest 11/11 + PGroupTest 22/22 green | TGroup.java:191-192 covered; ternary both branches (PTTL ms form + TTL rounded form); clamp via branchless `Math.max` |
| 88.2 | `TGroupTest.test ttl`: `SpockComparisonFailure at TGroupTest.groovy:162` — split-hash keysKey index present, TTL answered -2 | `c0734b06` `fix: ttl answers -1 for a split stored hash instead of -2` + refinement `e40b22ca` (fallback uses the metadata `exists()` check instead of `getCv()` — product decision during the fix phase, same semantics: `OneSlot.exists` filters deleted+expired, OneSlot.java:1906-1930; matches the RGroup.java:545 precedent) | TGroupTest 11/11 + PGroupTest 22/22 green (PTTL inherits via delegation) | TGroup.java:186 fallback check — both branches covered (index exists → -1, missing → -2) |
| 88.3 | `TGroupTest.test type`: `NullPointerException at TGroupTest.groovy:249` — `getCv()` entered the big-string branch and touched `LocalPersist` for a type-only query | `1c5ec017` `fix: type reads only the encoded header, no value payload or big string load` — new `BaseCommand.getCvSpTypeOnly()` (header-only: no payload copy, no decompression, no big-string file load, no `BigStringFileMissingException` tail) + `CompressedValue.onlyReadSpType/onlyReadExpireAt(ByteBuf)` overloads; geo→zset preserved via static predicates, persisted short-type mapping untouched (reviewer Option A) | TGroupTest 11/11 green (all type answers incl. big-string → string, expired → none, split-hash fallback) | TGroup.type() 127-165: 22/22 lines, 9/9 branch lines fully covered; `BaseCommand.getCvSpTypeOnly` direct unit tests added in `35523b1b` — `BaseCommandTest.'test get cv sp type only'` drives a **real OneSlot** (missing key / decoded cv / expired cv / production WAL-encoded buf / big-string marker without file load / `CannotReadException` guard) and `CompressedValueTest.'test only read sp type and expire at from byte buf'` covers both ByteBuf overloads (normal + short-string forms, parity with the byte[] overloads): 17/20 lines, 4/6 branch lines fully covered — only the defensive expired-encoded-buf side (836-837) stays uncovered, `OneSlot.get` pre-filters expired so it is unreachable through the real path (same defense-in-depth class as the 88.5 clamp) |

Review-follow-up refactors (behavior-preserving, user-directed during the fix phase, not bug fixes):

- `e40b22ca` `refactor: split hash ttl fallback uses the metadata exists check` — see 88.2 above.
- `65ec4f07` `refactor: existence-only getcv call sites use the exists check` — audit of all ~40 `getCv` call sites found 4 existence-only uses converted to `exists()`: SGroup.java:2371 (split-hash index in the SET-scan WRONGTYPE guard), HGroup.java:2500 (split-hash field invariant), CGroup.java:665+685 (COPY dst-exists, both worker paths; also removes a `BigStringFileMissingException` risk on an existing big-string dst). `ThrowOnDstCGroup` in CGroupTest now also overrides `exists()` so the bug-73.1 promise-settling regression still throws. CGroupTest 11/11 + HGroupTest 57/57 + SGroupTest 29/29 green; HGroup.java:2501 throw-branch stays uncovered (can't-happen invariant, structurally unchanged by the refactor).

Final regression at HEAD `35523b1b`: TGroupTest 11/11, PGroupTest 22/22, CGroupTest 11/11, HGroupTest 57/57, SGroupTest 29/29, BaseCommandTest 23/23, CompressedValueTest 8/8 — all green.

Direct unit tests for the new public methods (the new-public-method rule) were added in `35523b1b` `test: unit tests for getcvSptypeOnly and the byte buf header readers`: `BaseCommandTest.'test get cv sp type only'` (real `OneSlot` via `LocalPersistTest.prepareLocalPersist()` — the production branch and the WAL-encoded buf branch are exercised through the real storage stack, no test-only bypass harness) and `CompressedValueTest.'test only read sp type and expire at from byte buf'`. An earlier TGroupTest-local anonymous `ByPassGetSet` harness for the buf branch was dropped by user decision — the real-slot spec supersedes it.

## Re-review (AI agent 2)

*(pending)*

## Residual Risk

*(to be filled after dispositions — cross-module risks already identified during verification are listed here so the fix phase does not lose them)*

- **Split-hash command family (cross-module, not fixed by this round):** for a split-mode hash (or a key with the `h_not_hh_` prefix) `EXISTS` answers `0` and `EXPIRE` answers `0` (Redis: `1` / `1`) — `BaseCommand.exists()` (1285-1303) and `EGroup.expire()` (276-332) have no `RedisHashKeys.keysKey()` fallback. Fixing only `TTL`/`PTTL` (88.2) would leave the three commands disagreeing with each other; the product decision (does the split hash own a top-level TTL?) must be taken once.
- **`EGroup.expiretime()` truncates the absolute seconds** (`EGroup.java:350`, `expireAt / 1000`) while Redis rounds (`expire.c:709` → `((ttl+500)/1000)` for `output_abs` too) — the same family as 88.1, in a module whose round is closed.
- **The persisted short-type mapping** (`KeyLoader.transferToShortType()`, `KeyLoader.java:409-425`) maps geo → `typeAsByteString`; it is stamped into the persisted value meta (`SegmentBatch2.java:204`, `Wal.java:445`, `ChunkWalGroupRebuilder.java:127`), so it must not be changed by the 88.3 fix; the same mapping also makes `SCAN ... TYPE zset` / `KEYS ... TYPE zset` miss geo keys (`bug_70`/`bug_80` territory).
- **`TOUCH` is not implemented** — it answers the unknown-command error (consistent with the 85.15 fix); Redis 7.2 has `TOUCH` with arity -2. Not filed in this round.
- **`TYPE` answers a bulk reply, Redis a status reply** — repo-wide simplification, no `StatusReply` class exists; all mainstream clients accept both.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 88.1 | TTL truncates instead of rounds to nearest (`(ttl+500)/1000`) | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`cda79946`) |
| 88.2 | TTL/PTTL answer -2 for a split-mode hash (TYPE says "hash") | Medium | Correctness / Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`c0734b06` + `e40b22ca` refinement) |
| 88.3 | TYPE loads the whole encoded value (big-string file included) for an O(1) query; missing-file tail errors instead of "string" | Low (refined from Medium) | Performance | `PARTIALLY CONFIRMED` | **fixed** (`1c5ec017` + unit tests `35523b1b`) |
| 88.4 | TIME ignores extra arguments (no arity check; registry arity is metadata-only) | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`9751d1fe`) |
| 88.5 | No negative clamp on TTL/PTTL (Redis: `if (ttl < 0) ttl = 0`) | Low | Edge case | `PARTIALLY CONFIRMED` | **fixed** — folded into the 88.1 commit (`cda79946`) |
