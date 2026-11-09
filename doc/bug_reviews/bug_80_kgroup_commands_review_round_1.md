# Bug 80 — KGroup Commands Review (Round 1)

**Module:** `io.velo.command.KGroup` (KEYS) plus its supporting pipeline: `KeyAnalysisHandler.prefixMatch()`, `DynConfig` knob wiring, `RequestHandler` dispatch
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** all 7 findings CONFIRMED (reproduced) by agent 2; fix phase complete — `keys()` rewritten on the SCAN machinery per maintainer decision (TDD red step waived), (fix commit d8e41ddd)
**Review branch:** `review/kgroup`
**Reviewed commit:** `2dc770d709507d8ca437d6172094b4ea8e9bf4db`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/KGroup.java` — class doc + registry (16-30); `parseSlots()` (49-53); `handle()` (60-67); `keys()` (72-149)
- `src/main/java/io/velo/command/SGroup.java` — scan registration (50); `parseSlots()` scan slot-pinning branch (308-330); `scanResultToReply()` (547-558); **`scan()` MATCH/COUNT parse + WAL-then-key-bucket iteration (576-733)** — read as the reference implementation for the 80.2 fix direction
- `src/main/java/io/velo/persist/KeyLoader.java` — **`isKeyMatch()` glob dispatch + fast path (427-459)**; **`stringMatchLen()`/`stringMatchImpl()` — Redis `stringmatchlen` port (461-494)** — read as the reference matcher for the 80.1 fix direction
- `src/main/java/io/velo/persist/Wal.java` — `scan()` signature + isKeyMatch usage (392-438)
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java` — class doc (30-35); `initOptions()` (93-123); `createDB()` (125-130); ctor (141-159); `addKey()` (168-190); `addBatch()` (207-231); `removeKey()` (233-238); `clearAllKeysAfterAnalysis()` (253-265); `flushdb()` (272-274); `filterKeys()` (334-371); **`prefixMatch()` (382-404)**; `run()` (411-429)
- `src/main/java/io/velo/BaseCommand.java` — `execute()` test entry (454-489); `set()` write path incl. `addKey` call sites (975-1000, 1160-1222)
- `src/main/java/io/velo/RequestHandler.java` — shortcut + group dispatch, exception catch-all (474-580)
- `src/main/java/io/velo/decode/Request.java` — `cmd()` lowercasing (228-237)
- `src/main/java/io/velo/decode/RESP.java` — `parseRedisNumber()` (57-87); `readLine()` (93-108); `decode()` (124-221)
- `src/main/java/io/velo/decode/RequestDecoder.java` — `tryDecodeOne()` (74-209)
- `src/main/java/io/velo/reply/ErrorReply.java` — FORMAT/WRONG_NUMBER etc. (9-52); `buffer()` (192-196)
- `src/main/java/io/velo/persist/DynConfig.java` — `SUPPORTED_ITEMS` (43-156); `INTERNAL_KEYS` (158-165); ctor/file load (400-422); `get()` (213-215); `update()` (449-457); `parseAndValidate()` (424-433)
- `src/main/java/io/velo/persist/OneSlot.java` — `getDynConfig()` (953-955); `updateDynConfig()` (965-972)
- `src/main/java/io/velo/persist/LocalPersist.java` — index handler pool (714-734); `cleanUp()` (835-847); `resetForTest()` (849-869)
- `dyn/src/io/velo/command/ManageCommand.groovy` — `dynConfig()` (1018-1060)
- `src/main/java/io/velo/MultiWorkerServer.java` — FORMAT→WRONG_NUMBER rewrite (594-619); config defaults (1397-1405); `startIndexHandlerPool()` call (1155); `indexWorkers` validation (1689-1700)
- `src/main/java/io/velo/ConfForGlobal.java` — defaults (33, 43, 108)
- `src/test/groovy/io/velo/command/KGroupTest.groovy` — full (14-107)

Reference docs (ground truth):

- Redis KEYS official command doc — https://redis.io/commands/keys/ (glob patterns `h?llo`/`h*llo`/`h[ae]llo`/`h[^e]llo`/`h[a-b]llo`, `\` escaping, "Returns **all** keys matching pattern", `KEYS *` = whole keyspace, O(N) full scan)
- `doc/redis_command_support.md` line 92 — `KEYS` marked `[√]` supported
- Prior round on this pipeline (not re-reported, see Delta): `doc/bug_reviews/bug_52_activej_usage_review_round_1.md` Finding 2 (KEYS promise completion cross-eventloop — fixed by the current `Promise.ofFuture` branch) and Finding 3 (FLUSHDB/FLUSHALL stale key-analysis index)

Fix direction note: on 2026-11-09 the maintainer agreed to re-implement `keys()` on the SCAN machinery (`isKeyMatch` glob + per-slot WAL/key-bucket iteration via `oneSlot.asyncCall` fan-out); the consolidated design lives in Finding 80.2's *Suggested fix direction*, and 80.1/80.3/80.5/80.6/80.7 reference it.

Baseline: `./gradlew :test --tests "io.velo.command.KGroupTest"` — BUILD SUCCESSFUL at reviewed commit.

## Finding 1 (Bug 80.1): KEYS interprets a Redis glob pattern as a Java regex — silent wrong matches for every wildcard

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite`

### Cited files / lines
```java
// KGroup.java:79-98 — first-byte guard only rejects 5 lead chars; prefix breaks at the FIRST of * ? [ . +
var patternFirstByte = data[1][0];
if (patternFirstByte == '*' || patternFirstByte == '?' || patternFirstByte == '['
        || patternFirstByte == '.' || patternFirstByte == '+') {
    return ONLY_SUPPORT_PREFIX_MATCH_PATTERN;
}
...
// KGroup.java:107 — raw pattern string compiled as java.util.regex
// todo, check if need add . before *
var pattern = Pattern.compile(patternString);

// KeyAnalysisHandler.java:391-393 — prefix seek + FULL regex match
if (key.startsWith(prefix)) {
    if (pattern.matcher(key).matches()) {
```

### Root cause
Redis `KEYS` semantics are glob (`*` = any run, `?` = one char, `[...]` = class; redis.io/commands/keys). Velo compiles the client-supplied pattern string **unchanged** as a `java.util.regex.Pattern` and requires `matches()` (full-key match). In regex, `*`/`?` are *quantifiers binding to the preceding single character*, `.`/`+` are metachars that glob treats as literals. Every glob pattern containing `*`, `?`, `.` or `+` after the prefix is therefore evaluated with the wrong semantics. The author's own `// todo, check if need add . before *` (KGroup.java:106) flags exactly this unresolved question.

### Trigger / impact
With keys `user:1` … `user:100` present (Redis: `KEYS user:*` → 100 keys):

| Client pattern | Regex actually evaluated | Velo result vs Redis |
|---|---|---|
| `user:*` | `use` + `r*` full-match, prefix `user` | only `user`, `userr`, `userrr`… — misses all `user:N` (silent empty-ish reply) |
| `h?llo` | `h?llo` = optional `h` + `llo` full-match, prefix `h` | only `hllo`; Redis: `hello`, `hallo`, `hxllo` |
| `foo.bar*` | `foo.bar*` = ends in `r`*; prefix breaks at `.` → `foo` | misses `foo.barx`; Redis matches it |
| `app.log` (literal dot) | `.` = any char, full-match 7 chars | false positives, e.g. `appXlog`; Redis: only `app.log` |

`doc/redis_command_support.md` claims `KEYS [√]`, so any Redis client (jedis `keys("user:*")`, redis-cli `KEYS *` family) silently gets wrong data. No error is returned — worst-case failure mode.

### Suggested fix direction
**Don't write a glob→regex translator — reuse the existing one the SCAN family already uses.** `KeyLoader.isKeyMatch` (KeyLoader.java:432-459) delegates to `stringMatchLen` (KeyLoader.java:470+, a port of Redis `stringmatchlen`: `*`, `?`, `[...]`, `^` negation, `a-b` ranges, `\` escapes) with a `startsWith` fast path for simple `user:*`-style prefixes. `SGroup.scan` / `Wal.scan` / `KeyLoader.scan` already filter keys through it for `SCAN MATCH`. Replace KGroup's `Pattern.compile(patternString)` + `prefixMatch` regex path with `isKeyMatch(key, pattern)` over the real iteration (see 80.2's unified design) — exact Redis glob semantics for free, including the `\`-escape and unclosed-`[` handling (also resolves 80.3). If regex-flavored input is a product feature (the existing test uses `key:.*`), it is incompatible with the `[√]` Redis claim and needs a product decision — but `KEYS user:*` must not silently return wrong keys.

## Finding 2 (Bug 80.2): KEYS queries the 1%-sampled, periodically-wiped key-analysis RocksDB instead of the keyspace — structurally incomplete and stale results

**Severity:** High
**Class:** Correctness
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite`

### Cited files / lines
```java
// KGroup.java:109-110 — KEYS data source is the key-analysis handler
var keyAnalysisHandler = localPersist.getIndexHandlerPool().getKeyAnalysisHandler();
var f = keyAnalysisHandler.prefixMatch(keyPrefix, pattern, maxCount);

// KeyAnalysisHandler.java:150-152 — sampling budget: percent(default 1) * estimateKeyNumber * slotNumber / 100
this.doAddWhenAddLoopIncreaseCount = 100 / ConfForGlobal.keyAnalysisNumberPercent;
this.keyAnalysisNumberTotal = ConfForGlobal.keyAnalysisNumberPercent *
        ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber / 100;

// KeyAnalysisHandler.java:168-190 — addKey: 99 of 100 writes skipped by default; stops entirely when full
public void addKey(String key, int valueLengthHigh24WithShortTypeLow8) {
    if (isKeyAnalysisNumberFull) { return; }
    if (addLoopCount % doAddWhenAddLoopIncreaseCount != 0) { addLoopCount++; return; }
    ...
    if (addCount >= keyAnalysisNumberTotal) { isKeyAnalysisNumberFull = true; }

// KeyAnalysisHandler.java:233-238 — per-key removal is TEST ONLY; DEL/expire never propagate
@TestOnly
void removeKey(String key) { ... db.delete(...); }

// KeyAnalysisHandler.java:253-265 + 416-421 — whole DB wiped after each analysis pass
void clearAllKeysAfterAnalysis() { db.close(); FileUtils.deleteDirectory(keysDir); createDB(); ... }

// ConfForGlobal.java:43 / 33 — defaults
public static int keyAnalysisNumberPercent = 1;      // 1% sampling
public static long estimateKeyNumber = 1_000_000L;   // → cap = 10k × slotNumber
```

### Root cause
`KeyAnalysisHandler` is a background *statistics* store ("Samples keys … periodically groups them by prefix", class doc lines 30-34), populated on the write path (`BaseCommand.java:992, 1214`) with 1% sampling by default, a hard add-stop once `keyAnalysisNumberTotal` (10k × slots with defaults) is reached, zero propagation of deletes/expiries, and a full directory wipe each time the periodic `KeyAnalysisTask` completes a pass (`run()` line 417). `KGroup.keys()` treats this buffer as the keyspace.

### Trigger / impact
Redis: "Returns all keys matching pattern", O(N) over the live database. Velo with default config:

1. Write 10,000 keys `u:0…u:9999`; `KEYS u:*` returns ≈ 1% of them (~100 sampled), not 10,000.
2. After ~10k×slots sampled writes, no further key is ever visible to KEYS until the next analysis wipe.
3. Immediately after an analysis pass completes, the DB is deleted → `KEYS u:*` ≈ empty even though all keys exist (bug_52 round 1 already recorded the FLUSHDB flavor of this staleness; this finding is the KEYS-side structural dependency).
4. `DEL u:5` (or expiry) — `u:5` still returned by KEYS until the next full wipe; clients see deleted keys.

Any client using KEYS for enumeration/cleanup verification gets silently wrong answers. (`SCAN` in SGroup uses the real persist layer — so Velo itself already has a correct iteration path to borrow.)

### Suggested fix direction
**Re-implement `keys()` on the SCAN machinery — unified design agreed with the maintainer (2026-11-09); it resolves 80.1/80.2/80.3/80.5/80.7 as a family:**

1. **Data source — the real keyspace per slot, not the analysis RocksDB.** Inside each slot run the exact loop shape of `SGroup.scan()` (SGroup.java:676-732): iterate WAL groups first via `oneSlot.getWalByGroupIndex(i).scan(skipCount, typeAsByteIgnore, matchPattern, leftCount, beginScanSeq)` until `lastScanCursor.isWalIterateEnd()`, then key buckets via `oneSlot.getKeyLoader().scan(...)` until `ScanCursor.END`. WAL-shadow skipping and `beginScanSeq` snapshot semantics (`SnowFlake.asBeginScanSeqEpoch()`, cf. bug 70.7) come for free — no sampling, no periodic wipe, deletes/expiries respected.
2. **Pattern matching** — `KeyLoader.isKeyMatch(key, matchPattern)` (see 80.1), i.e. Redis glob, not `java.util.regex`.
3. **Cross-slot fan-out + threading** — SCAN pins one slot via its cursor (SGroup.java:312-329) but KEYS must aggregate all slots in one reply. Precedent: `FGroup.flushall` and `ManageCommand.dynConfig` (ManageCommand.groovy:1030-1057) — submit per-slot work with `oneSlot.asyncCall(() -> <slot scan loop>)`, aggregate with `Promises.all(promises)`, so WAL/key-bucket reads stay on the owning slot worker thread (thread-affinity model preserved).
4. **Reply plumbing** — keep the existing `AsyncReply` + `SettablePromise` and the `Promise.ofFuture` reactor-affinity branch (KGroup.java:112-148); concatenate per-slot lists into one `MultiBulkReply` in completion order-independent fashion (slot order), honoring `keys.matchMaxCount` as an early-exit cap (see 80.6/80.4).

Trade-off accepted: O(N) full scan per call — the same complexity class as Redis KEYS itself (doc: O(N), "may ruin performance" warning); work runs on slot workers so other slots are unaffected, and the `keys.matchMaxCount` cap is the safety valve. The RocksDB prefix-seek speed of the current path is intentionally sacrificed for correctness. If instead the analysis-buffer source were kept by product decision, the `[√]` claim in `doc/redis_command_support.md` must be downgraded to "approximate" — but silent wrong data argues for the real iteration.

## Finding 3 (Bug 80.3): glob-legal patterns crash `Pattern.compile` — `KEYS foo[` replies with a JVM internals error

**Severity:** Medium
**Class:** Edge case / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite (REFINED: bare trailing-backslash example rejected earlier by the no-wildcard guard)`

### Cited files / lines
```java
// KGroup.java:84-107 — no guard between pattern acceptance and compile
var patternString = new String(data[1]);
...
var pattern = Pattern.compile(patternString);   // throws PatternSyntaxException

// RequestHandler.java:576-579 — catch-all turns it into a raw JVM message reply
} catch (Exception e) {
    log.error("Request handle error.", e);
    errorReplyCountTotal++;
    return new ErrorReply(e.getMessage());
}
```

### Root cause
Patterns like `foo[` (unclosed class), `foo\` (trailing backslash), `foo{2,` … pass the first-byte guard (they don't *start* with a metachar) and reach `Pattern.compile` raw. `PatternSyntaxException` escapes `keys()` and is stringified into the client reply. In Redis, `stringmatchlen` treats an unterminated `[` literally and never errors — KEYS cannot fail on pattern syntax.

### Trigger / impact
`KEYS foo[` → `-ERR Unclosed character class near index 3 …` (JVM message, not a Redis error text); same request also logs a full ERROR stack server-side (log-spam vector from any connected client — KEYS is `@dangerous`/`@slow` but not admin-gated).

### Suggested fix direction
Falls out of the 80.2 unified design: with `KeyLoader.isKeyMatch`/`stringMatchLen` there is no `Pattern.compile` at all — an unterminated `[` is matched literally, exactly like Redis `stringmatchlen`, and no pattern can raise. (If KEYS keeps any regex compilation independently of 80.2, wrap it in try/catch → stable `ErrorReply`; but the agreed direction removes the regex path entirely.)

## Finding 4 (Bug 80.4): the `keys.matchMaxCount` escape knob is dead — DynConfig rejects the key it was designed for

**Severity:** Medium
**Class:** Correctness / operability
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite`

### Cited files / lines
```java
// KGroup.java:102-104 — reads the knob
var firstOneSlot = localPersist.currentThreadFirstOneSlot();
var maxCountStr = firstOneSlot.getDynConfig().get("keys.matchMaxCount");
int maxCount = maxCountStr == null ? 100 : Integer.parseInt((String) maxCountStr);

// DynConfig.java:158-165 — key is in neither allow-list
private static final Set<String> INTERNAL_KEYS = Set.of("masterUuid", "readonly", "canRead",
        "canWrite", "binlogOn", "testKey");

// DynConfig.java:424-433 — unknown keys are rejected on update AND on file load
private static Object parseAndValidate(@NotNull String key, @NotNull Object value) {
    ...
    throw new IllegalArgumentException("Unsupported dyn config key: " + key);

// OneSlot.java:965-970 + ManageCommand.groovy:1018-1035 — the only write path funnels into that rejection
```

### Root cause
`keys.matchMaxCount` is missing from `DynConfig.SUPPORTED_ITEMS` and `INTERNAL_KEYS`. `manage dyn-config keys.matchMaxCount 500` → `IllegalArgumentException("Unsupported dyn config key: keys.matchMaxCount")` → per-slot failure. Hand-editing the slot's dyn-config JSON (the file `DynConfig` loads in its ctor, lines 411-421) makes every `parseAndValidate` of that entry throw during slot init. So the value is always `null` and `maxCount` is hard-stuck at 100 — the code comment "refer ManageCommand" (KGroup.java:100-101) points at a path that cannot succeed. Secondary latent hazards in the same line: `(String)` cast CCEs if the key ever holds a JSON number, `parseInt` NFEs on non-numeric strings.

### Trigger / impact
Operator raises `keys.matchMaxCount` to make KEYS usable beyond 100 keys → command fails; editing the file directly breaks slot startup. Combined with 80.2/80.6, KEYS can never return more than 100 keys on any config.

### Suggested fix direction
Register the item in `SUPPORTED_ITEMS` with a `parsePositiveInt` validator (returns `Integer`, not `String` — then drop the `(String)` cast at KGroup.java:104) or add it to `INTERNAL_KEYS` and parse defensively. Mirror the wiring of `max_connections` as the pattern.

## Finding 5 (Bug 80.5): `KEYS *` and wildcard-free patterns are rejected — the canonical invocations error out

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite`

### Cited files / lines
```java
// KGroup.java:79-82 — leading-metachar patterns rejected outright
var patternFirstByte = data[1][0];
if (patternFirstByte == '*' || patternFirstByte == '?' || patternFirstByte == '['
        || patternFirstByte == '.' || patternFirstByte == '+') {
    return ONLY_SUPPORT_PREFIX_MATCH_PATTERN;
}

// KGroup.java:94-97 — patterns with NO special char also rejected
var index = indexes.isEmpty() ? -1 : indexes.first();
if (index == -1) {
    return ONLY_SUPPORT_PREFIX_MATCH_PATTERN;
}
```

### Root cause
Two Redis-legal pattern families are refused: (a) patterns *starting* with a metachar — `KEYS *` (whole keyspace, the single most common KEYS call, shown verbatim in the Redis doc examples), `KEYS *name*`, `KEYS ?a`; (b) wildcard-free patterns — `KEYS foo` which in Redis returns the exact key if present. The error message shows this is deliberate ("only support prefix match pattern"), but `doc/redis_command_support.md` still claims `[√]` full support, and standard tooling (redis-cli, jedis `keys("*")`, GUI clients' "browse all keys") sends exactly these.

### Trigger / impact
Any client that opens with `KEYS *` gets an error instead of the keyspace; inventory/cleanup scripts doing `KEYS prefix` (exact) break. Deliberate divergence → candidate for won't-fix, but it must be a recorded product decision with the support doc downgraded, not silent behavior.

### Suggested fix direction
Both families fall out of the 80.2 unified design: with `isKeyMatch`, `KEYS *` is the `"*"` fast path (KeyLoader.java:437-439 → always true) and a wildcard-free `KEYS foo` is a plain literal `stringMatchLen` exact match — the first-byte guard and the `indexes`/`indexOf` prefix machinery (KGroup.java:79-98) are deleted rather than extended. If the prefix-only restriction is nonetheless kept by product decision, downgrade `redis_command_support.md` `[√]` to document it.

## Finding 6 (Bug 80.6): silent truncation at 100 results — Redis returns all matches

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — knob registered + cap documented by the bug 80 rewrite (product decision: cap kept)`

### Cited files / lines
```java
// KGroup.java:103-104,110 — default cap 100, no partial-result signal
int maxCount = maxCountStr == null ? 100 : Integer.parseInt((String) maxCountStr);
var f = keyAnalysisHandler.prefixMatch(keyPrefix, pattern, maxCount);

// KeyAnalysisHandler.java:388 — loop stops at maxCount, result indistinguishable from complete
while (iterator.isValid() && result.size() < maxCount) {
```

### Root cause
Redis `KEYS` returns the complete match set (doc: "Returns all key names that match a pattern"). Velo caps at `keys.matchMaxCount` (dead per 80.4 → always 100) and returns a normal-looking array; the client cannot distinguish truncated from complete.

### Trigger / impact
300 matching keys → 100 returned, silently. Scripts computing deletions from KEYS output leave 200 keys behind.

### Suggested fix direction
With 80.2's unified design, keep the cap as the O(N)-safety valve but make it honest: register `keys.matchMaxCount` first (80.4 — otherwise the cap is stuck at 100 forever), let the per-slot loops early-exit once the aggregate reaches `maxCount`, and document the cap in `redis_command_support.md` next to the `[√]`. Alternative: drop the cap entirely and match Redis (return all); O(N) is already accepted. Product decision — but either way the knob must be alive (80.4) or removed, never dead-but-present.

## Finding 7 (Bug 80.7): `KEYS ""` throws AIOOBE — `data[1][0]` on a zero-length bulk argument

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (reproduced) — fixed by the bug 80 rewrite`

### Cited files / lines
```java
// KGroup.java:78 — data[1] may be byte[0]
var patternFirstByte = data[1][0];

// RESP.java:165-167,204 — $0 bulk strings decode fine: size 0 is legal
int size = parseRedisNumber(lineBuf);
if (size < 0) { throw ... }
...
bytes[i] = new byte[size];   // size == 0 → byte[0]
```

### Root cause
`KEYS <empty>` (`*2\r\n$4\r\nkeys\r\n$0\r\n\r\n`) decodes to `data[1] = new byte[0]` (RESP.java:204); `data[1][0]` throws `ArrayIndexOutOfBoundsException`, caught by RequestHandler:576 → `-ERR Index 0 out of bounds for length 0`. Redis 7.2 replies an empty array (no key has an empty name).

### Trigger / impact
Any client sending an empty pattern (dynamic-pattern construction bug, fuzzer) gets a JVM error-string reply.

### Suggested fix direction
Length-check first: `if (data[1].length == 0) return MultiBulkReply.EMPTY` (empty pattern can match no key since key names are non-empty). In the 80.2 unified design this is a two-line guard at the top of `keys()`; no array indexing on `data[1]` remains.

## Notes / non-findings

- **Arity error text is correct**: `data.length != 2` → `ErrorReply.FORMAT` is centrally rewritten to `ERR wrong number of arguments for 'keys' command` (MultiWorkerServer.java:612-613; same disposition as bug_73 finding round). Not re-reported.
- **Uppercase `KEYS` dispatch**: `Request.cmd()` lowercases (Request.java:236); `cmd.equals("keys")` is safe. Correct.
- **Reactor-affinity of the async reply**: fixed by the `Promise.ofFuture` branch (KGroup.java:119-131) — this *is* bug_52 round 1 Finding 2's fix; not re-reported. The off-reactor fallback cast `(Exception) e` (line 136) only sees `Exception`s in practice (`AsyncComputation` completions); no defect claimed.
- **Missing null guard on `getIndexHandlerPool()` (KGroup.java:109)** vs the guarded write path (BaseCommand.java:1210-1211): pool is always constructed at startup (MultiWorkerServer.java:1155; `indexWorkers` default 1, validated ≥ 1 at 1689-1693); `indexHandlerPool` is only nulled by `@TestOnly resetForTest()` (LocalPersist.java:860). No production path → no defect claimed.
- **`parseSlots()` returns empty list**: consistent with other no-key commands (DBSIZE et al.); registry metadata (arity 2, readonly, `@keyspace @read @slow @dangerous`, group generic) matches the Redis doc exactly. Correct.
- **KGroupTest `test keys` assertion is powerless for the count**: `(reply as AsyncReply).settablePromise.whenResult { … length == 10 }.result` — the closure is coerced to ActiveJ's `Consumer` (boolean return value discarded; if the promise is incomplete the callback runs later off-thread), so the `== 10` check cannot fail the test; `KGroupTest` passing does not certify result counts. Test-quality observation for the fix phase, not a production bug. Also note the test never sets `ConfForGlobal.keyAnalysisNumberPercent` (default 1 → ~1/100 writes sampled; cf. KeyAnalysisHandlerTest:66 which must set 100 to see keys) — how 10 keys can be returned under 1% sampling is exactly what the reviewer recipe below probes.
- **`KEYS` over HTTP**: body split by spaces reaches the same `keys()` path; no HTTP-specific divergence found.

## Reviewer checklist (for AI agent 2)

- **80.1**: `prepareLocalPersist` + `indexWorkers=1` (copy KGroupTest setup); set `ConfForGlobal.keyAnalysisNumberPercent = 100` before `startIndexHandlerPool()`; `kGroup.set('user:1','x')`, `kGroup.set('user:2','x')`, `kGroup.set('hxllo','x')`, `kGroup.set('appXlog','x')`; then `execute('keys user:*')` → expect AsyncReply completing with MultiBulkReply NOT containing `user:1`/`user:2` (assert actual `.replies.length == 0` via `settablePromise.whenComplete` blocking `get()`, not the powerless `whenResult{}); `execute('keys h?llo')` → misses `hxllo`; `execute('keys app?log')`-style literal-dot case: set `app.log` + `appXlog`, `execute('keys app.log')` → returns both (Redis: one). Differential: same keys in redis 7.2 → all match.
- **80.2**: same env, percent left at default 1: write 20 distinct `k:N` keys, force completion (`execute('keys k:.*')` promise `.get()`), assert returned count ≪ 20 (expect ~1). Then percent=100: write keys, `kGroup.execute('keys ...')` complete; `kGroup.del`/expire one sampled key → KEYS still returns it. Optional: check `KeyAnalysisHandler.run()`/`clearAllKeysAfterAnalysis` wipe path via `getTopKPrefixCounts` task completion.
- **80.3**: `execute('keys foo[')` → reply is ErrorReply whose message contains `Unclosed character class` (JVM text) instead of a Redis-standard error/empty array; also `execute('keys foo\\')`.
- **80.4**: `oneSlot.updateDynConfig('keys.matchMaxCount', '500')` → expect `IllegalArgumentException: Unsupported dyn config key`; static: confirm `SUPPORTED_ITEMS`/`INTERNAL_KEYS` lack the key (DynConfig.java:43-165). Optionally write the key into a slot dyn-config JSON and construct `DynConfig` → ctor throws.
- **80.5**: `execute('keys *')`, `execute('keys foo')` → ONLY_SUPPORT_PREFIX_MATCH_PATTERN error (Redis: array). Table-driven.
- **80.6**: static (cap at KeyAnalysisHandler.java:388) + demo: percent=100, write 150 `q:N` keys, `execute('keys q:.*')` → 100 results max, no truncation marker.
- **80.7**: `execute('keys ' + BYTES_0_LENGTH_PLACEHOLDER)` if the test harness placeholder exists (BaseCommand.execute placeholder set), or construct `data = ['keys'.bytes, new byte[0]]` via `execute(String, DataArrayReplacer)` → expect AIOOBE-derived `-ERR Index 0 out of bounds…` vs Redis empty array.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (`src/test/groovy/io/velo/command/Bug80ReviewTempSpec.groovy`, deleted after evidence capture) over the real persist layer (`prepareLocalPersist` + `indexWorkers=1`, `fixSlotThreadId`), blocking on `settablePromise.toCompletableFuture().get()`; regex ground truth via JDK repro; DynConfig behavior via `OneSlot.updateDynConfig` + hand-edited JSON ctor. All findings verified at reviewed commit `2dc770d7` before the fix.

| Finding | Verdict | Evidence |
|---|---|---|
| 80.1 | CONFIRMED (reproduced) + REFINED detail | `keys h?llo` → `[hllo]` (misses `hxllo`; Redis: both); `keys app.log` → `[app.log, appXlog]` (false positive `appXlog`; Redis: one); `keys user:*` → `[]` with all 8 keys present in the analysis DB (dump via `filterKeys` proved `user` present and `Pattern 'user:*'.matches('user')=true`, yet the reply was empty — the analysis path is even wronger than reported). Refinements: (a) the report's regex analysis had the quantifier binding wrong — `user:*` compiles to `user` + `:`* (the `*` binds to the colon), so the report's "returns user/userr/userrr" row was itself inaccurate; (b) the report's Redis expectations had two errors — glob `user:*` does NOT match `user`/`userr` (colon required), and `h?llo` does NOT match the 4-char `hllo`. None of this weakens the finding: wildcard patterns were evaluated with non-glob semantics and silently returned wrong results. |
| 80.2 | CONFIRMED (reproduced) | Default `keyAnalysisNumberPercent=1` (log: `do add when add loop increase count=100`), 20 keys `k:0..k:19` written → `keys k:.*` returned exactly 1 (`k:0`), Redis returns 20. With percent=100 (log: `count=1`): 10 `k:*` keys written, `del k:5` → 1, `keys k:.*` still returned 10 including `k:5`; `handler.flushdb()` (the same `clearAllKeysAfterAnalysis` the periodic `run()` calls, KeyAnalysisHandler.java:417) → `keys k:.*` returned 0 while all 10 keys still exist in the keyspace. Static: `removeKey` has no production call site (grep: definition only). |
| 80.3 | CONFIRMED (reproduced) | `execute('keys foo[')` → `THROWN java.util.regex.PatternSyntaxException: Unclosed character class near index 3`; in production RequestHandler.java:576-579 stringifies it into `-ERR Unclosed character class near index 3…`. Refinement: bare `keys foo\` is REJECTED earlier by the no-wildcard guard (`\` is not among the 5 indexOf chars, KGroup.java:85-97) with the only-support error — it never reaches `Pattern.compile`; a pattern that both contains a metachar and ends with a backslash (e.g. `fo.o\`) still crashes the compile. |
| 80.4 | CONFIRMED (reproduced) | `oneSlot.updateDynConfig('keys.matchMaxCount','500')` → `IllegalArgumentException: Unsupported dyn config key: keys.matchMaxCount`; `new DynConfig(slot, file-with-{"keys.matchMaxCount":500}, oneSlot)` → ctor throws the same (slot init breaks on hand-edited JSON); live `get('keys.matchMaxCount')` → null → cap hard-stuck at 100. Static: key in neither `SUPPORTED_ITEMS` (DynConfig.java:43-156) nor `INTERNAL_KEYS` (158-165). |
| 80.5 | CONFIRMED (reproduced) | `keys *` and `keys foo` → `ErrorReply('only support prefix match pattern')`; Redis 7.2 returns the whole keyspace / the exact key if present. |
| 80.6 | CONFIRMED (reproduced) | percent=100, 150 `q:*` keys written → `keys q:.*` returned exactly 100, normal-looking array, no truncation marker. |
| 80.7 | CONFIRMED (reproduced) | `execute('keys _0')` (BYTES_0_LENGTH_PLACEHOLDER → `data[1]=byte[0]`) → `THROWN java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0` at KGroup.java:78; production catch-all → `-ERR Index 0 out of bounds for length 0`; Redis 7.2 replies an empty array. |

### Additional observations

- The `execute('keys user:*')` → `[]` anomaly (direct `prefixMatch('user', user:*)` → `[user]` in the same instant) points at an additional defect inside the old analysis path (prime suspect: the RocksDB fixed-length prefix bloom, `useFixedLengthPrefixExtractor(3)`, interacting with `iterator.seek`) — moot after the rewrite removed the analysis source, recorded here so it is not lost.
- Confirmed the reviewer-checklist suspicion about KGroupTest's powerless `whenResult{}` assertion: under default 1% sampling the old test wrote 10 keys and "expected" 10 back while the analysis DB held exactly 1 — the test could not fail.
- `Promises.all` discards results (`Promise<Void>`); result-collecting fan-out must use `Promises.toList` (bit the first compile of the rewrite).

### Recommended fix order (for the fix agent)

Superseded by the maintainer decision of 2026-11-09 (second session): the whole family 80.1/80.2/80.3/80.5/80.7 is fixed by one rewrite of `keys()` on the SCAN machinery (no per-bug commits), 80.4 by registering `keys.matchMaxCount` in `DynConfig.SUPPORTED_ITEMS`, 80.6 resolved as product decision: cap kept + knob alive + documented in `doc/redis_command_support.md`.

## Fix Log (round 1 fix phase)

Maintainer waiver: since `keys` is a full rewrite off the key-analysis store, the per-bug TDD red step was waived ("as this 'keys' need rewrite, do not use key analysis, need not write TDD red step"); the rewritten `KGroupTest.'test keys'` covers every finding's before/after behavior in one command test per `doc/how_to_write_high_coverage_test_cases_for_commands.md`.

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 80.1 | waived (rewrite); old wrong behavior captured in reviewer temp spec above | d8e41ddd | `./gradlew :test --tests "io.velo.command.KGroupTest"` green — `keys user:*`→[user:1,user:2], `keys user*`→[user,user:1,user:2,userr], `keys h?llo`→[hello,hxllo], `keys app.log`→[app.log] only, `keys h[ax]llo`, `keys h[eo]llo` | KGroup.java 57/64 lines covered, 10/14 branch lines fully covered |
| 80.2 | waived (rewrite); temp spec: 1-of-20 keys, del-blind, wipe-blind | d8e41ddd | same run — `del user:1` visible, `expire user 1` + sleep visible, key-bucket phase via `persistShortValueListBatchInOneWalGroup` (incl. mid-cursor resume with `onceScanMaxLoopCount=1`) | same |
| 80.3 | waived (rewrite); temp spec: PSE on `foo[` | d8e41ddd | same run — `keys foo[` → empty array, no error | same |
| 80.4 | waived (rewrite); temp spec: IAE on set + ctor throw | d8e41ddd | same run — `updateDynConfig('keys.matchMaxCount','200')` accepted and honored; `'abc'` → `thrown(IllegalArgumentException)` | DynConfig.java:156-170 5/5 covered |
| 80.5 | waived (rewrite); temp spec: `*`/`foo` rejected | d8e41ddd | same run — `keys *` → all 9 keys, `keys user` → [user] | same as 80.1 |
| 80.6 | waived (rewrite); temp spec: silent 100 cap | d8e41ddd | same run — 150 `q:*` keys → 100 (default), knob→200 → 150; cap documented in `doc/redis_command_support.md` | same |
| 80.7 | waived (rewrite); temp spec: AIOOBE | d8e41ddd | same run — `keys _0` → `MultiBulkReply.EMPTY` | same |

## Re-review (AI agent 2)

Fix-phase verification (agent 2, same session, pre-commit):

- `./gradlew :test --tests "io.velo.command.KGroupTest"` → BUILD SUCCESSFUL (4 methods; `test keys` exercises glob family / del / expire / cap + knob / bucket phase + cursor resume over 2 slots).
- Related suites green: `DynConfigTest`, `SGroupTest`, `CategoryTest`, `CommandRegistryPopulationTest`, `XDynConfigTest`.
- JaCoCo (`scripts/jacoco_cover.py`): `io.velo.command.KGroup` 69-185 → 57/64 lines, 10/14 branch lines fully covered; `io.velo.persist.DynConfig` 155-175 → 5/5.
- Semantics spot-check against Redis 7.2 glob: `user:*` ≠ `user`/`userr` (colon required), `h?llo` ≠ `hllo` (5-char pattern) — the rewritten expectations encode Redis semantics, verified against `stringMatchLen` (the Redis `stringmatchlen` port) via `KeyLoader.isKeyMatch`.
- Accept: the rewrite + knob registration + doc note. Commit pending (maintainer has not requested commit).

### Re-review 2 (AI agent 2, post-commit — commits `d8e41ddd`, `db9564fc`, `eaf416db`)

**Method:** independent re-derivation — diff review against the agreed design + focused suites + JaCoCo re-inspection + live Redis 7.2.11 differential. No reliance on the pre-commit re-review's claims.

**Static verification of the rewrite (KGroup.java 69-196):**

- `ScanCursor` construction matches the record order `(slot, walGroupIndex, walSkipCount, keyBucketsSkipCount, splitIndex, bucketIndex)` (ScanCursor.java:6-7); the bucket resume loop feeds back exactly `walGroupIndex/splitIndex/bucketIndex/keyBucketsSkipCount` in `KeyLoader.scan`'s parameter order (KeyLoader.java:726-733), mirroring SGroup.java:727-728.
- WAL loop terminates: `Wal.scan` always advances — in-group (`nextTimeSkipCount ≥ snapshotKeys.size()` → group+1) or END sentinel (Wal.java:458-479). No `onceScanMaxLoopCount` needed (full iteration by design; progress guaranteed).
- Bucket loop terminates: `KeyLoader.scan` early-return advances split/group (KeyLoader.java:767-776), exhaustion returns `END` or a next-slot cursor via `scanNextSlotCursor` (782-797); both flavors are caught by KGroup's `next == END || next.slot() != oneSlot.slot()` break — **all 4 branches of that check JaCoCo-covered**.
- Reactor affinity does **not** regress vs bug_52 F2: the dropped `Promise.ofFuture` bounce is inherited per-slot — `asyncCall` on a reactor returns `Promise.ofFuture(slotWorkerEventloop.submit(...))` (OneSlot.java:736-738), so every constituent promise completes on the request reactor; `Promises.toList(...).whenComplete` and `finalPromise.set(...)` therefore also run there, where `transferAsyncReply`'s `promise.map` subscribed (MultiWorkerServer.java:694-710).
- WAL/bucket dedup + snapshot: per-WAL-group `inWalKeys` (KeyLoader.java:746, the bug_13 fix) skips bucket entries shadowed by WAL; both phases filter `seq > beginScanSeq` (Wal.java:416, KeyLoader.java:685) and tombstones/expired (Wal.java:433, KeyLoader.java:653). One `beginScanSeq` shared by all slots.
- Cap: per-slot budget `matchMaxCount`, aggregate trimmed in slot order; knob read via `instanceof Integer` (the old `(String)` cast hazard gone); `firstOneSlot()` null-guarded (LocalPersist.java:651, cluster-no-slots → default 100).

**Focused tests (this session, independent run):** `./gradlew :test --tests "io.velo.command.KGroupTest" --tests "io.velo.persist.DynConfigTest" --tests "io.velo.command.SGroupTest"` → **BUILD SUCCESSFUL** (KGroupTest 4 methods incl. the rewritten `test keys`; DynConfigTest incl. new knob normalize/reject cases; SGroupTest green = shared SCAN machinery untouched).

**JaCoCo (this session, `scripts/jacoco_cover.py`):** `io.velo.command.KGroup` 90-185 → **50/57 lines, 7/11 branch lines fully covered**; not-covered 117-119 + partial 94/116/161/174 are fault-injection / cluster-no-slots / loop-entry flavors only. `io.velo.persist.DynConfig` 155-175 → covered (5/5).

**Live Redis 7.2.11 differential** (port 16380, same 9 keys as the test):

```
PAT[user:*] => user:1,user:2,          PAT[*] => app.log,appXlog,hello,hllo,hxllo,user,user:1,user:2,userr,
PAT[user*]   => user,user:1,user:2,userr,   PAT[user] => user,
PAT[h?llo] => hello,hxllo,             PAT[nonexistent] =>,
PAT[hllo]  => hllo,                    PAT[foo[] =>,          (no error)
PAT[h[ax]llo] => hxllo,                PAT[] =>,              (empty array)
PAT[h[eo]llo] => hello,                PAT[app.log] => app.log,
PAT[app?log] => app.log,appXlog,
```

**13/13 patterns identical to the rewritten `KGroupTest` expectations.**

**Verdict: fix ACCEPTED.** All seven findings verified fixed at `d8e41ddd`/`db9564fc`; the TDD-red waiver is recorded with the maintainer's decision; follow-ups (non-blocking): `KeyAnalysisHandler.prefixMatch` now has zero callers (dead code, cleanup candidate); stale "commit pending" wording in the pre-commit re-review superseded by this section.


## Residual Risk

- Uncovered defensive branches in KGroup.java: `firstOneSlot == null` (94), the `whenComplete` exception translation (116-119), one cursor-flavor branch (174) — reachable only via fault injection / cluster-with-no-slots; noted, not blocking.
- Per-slot budget = `matchMaxCount` for every slot in parallel, then the aggregate is trimmed to `matchMaxCount` in slot order — a slot-order bias when the cap is hit (acceptable: KEYS result order is unspecified in Redis, and the cap is a documented safety valve).
- KEYS is O(N) per call over all slots (Redis-equivalent complexity class; runs on slot workers so other slots are unaffected; the cap bounds the reply size but not the scan work when matches are sparse — same as before for SCAN).
- The key-analysis RocksDB store itself is untouched (still sampled, still wiped by the periodic task) — it now only feeds `KeyAnalysisTask` statistics, no longer any command path. The `prefixMatch` anomaly recorded above lives there.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 80.1 | Glob pattern evaluated as Java regex (`KEYS user:*` silent wrong results) | High | Correctness / Redis compat | CONFIRMED (reproduced) | fixed (rewrite) |
| 80.2 | KEYS source is 1%-sampled, periodically-wiped, delete-blind analysis DB | High | Correctness | CONFIRMED (reproduced) | fixed (rewrite) |
| 80.3 | `Pattern.compile` crash on glob-legal patterns (`foo[`) | Medium | Edge case / Redis compat | CONFIRMED (reproduced, `foo\` example refined) | fixed (rewrite) |
| 80.4 | `keys.matchMaxCount` un-settable (DynConfig rejects the key; JSON edit breaks slot init) | Medium | Correctness / operability | CONFIRMED (reproduced) | fixed (knob registered) |
| 80.5 | `KEYS *` / wildcard-free `KEYS foo` rejected | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (rewrite) |
| 80.6 | Silent truncation at 100 matches | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (knob alive + cap documented; product decision: cap kept) |
| 80.7 | `KEYS ""` AIOOBE → JVM error string reply | Low | Edge case | CONFIRMED (reproduced) | fixed (rewrite) |
