# Bug 80 — KGroup Commands Review (Round 1)

**Module:** `io.velo.command.KGroup` (KEYS) plus its supporting pipeline: `KeyAnalysisHandler.prefixMatch()`, `DynConfig` knob wiring, `RequestHandler` dispatch
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 7 findings, none verified yet
**Review branch:** `review/kgroup`
**Reviewed commit:** `2dc770d709507d8ca437d6172094b4ea8e9bf4db`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/KGroup.java` — class doc + registry (16-30); `parseSlots()` (49-53); `handle()` (60-67); `keys()` (72-149)
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

Baseline: `./gradlew :test --tests "io.velo.command.KGroupTest"` — BUILD SUCCESSFUL at reviewed commit.

## Finding 1 (Bug 80.1): KEYS interprets a Redis glob pattern as a Java regex — silent wrong matches for every wildcard

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `OPEN — not verified`

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
Translate glob → regex before compiling: `*`→`.*`, `?`→`.`, `[...]`→`[...]` (Redis `^` negation and `a-b` ranges map directly), escape all other regex metachars (`.` `+` `(` `)` `{` `}` `\` …) with `\\`; in Redis 7.2 `\` inside the pattern is a literal unless escaping a special char — follow `stringmatchlen` semantics. Restrict the prefix-break set to real glob metachars (`*`, `?`, `[`) so `.`/`+` no longer break the prefix (they are plain literals in glob). Keep the `startsWith(prefix)` seek optimization. If regex-flavored input is a product feature (the existing test uses `key:.*`), it is incompatible with the `[√]` Redis claim and needs a product decision — but `KEYS user:*` must not silently return wrong keys.

## Finding 2 (Bug 80.2): KEYS queries the 1%-sampled, periodically-wiped key-analysis RocksDB instead of the keyspace — structurally incomplete and stale results

**Severity:** High
**Class:** Correctness
**Status:** `OPEN — not verified`

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
Short term, minimal: route `keys()` through the real key space the way `SGroup.scan()` iterates (KeyLoader + WAL shadow check), keeping the prefix seek as optimization; that fixes completeness, staleness, and deleted-keys at once and makes `maxCount` the only limit. If the analysis-buffer source is kept by product decision, the `[√]` claim in `doc/redis_command_support.md` must be downgraded to "approximate" — but silent wrong data argues for the real iteration.

## Finding 3 (Bug 80.3): glob-legal patterns crash `Pattern.compile` — `KEYS foo[` replies with a JVM internals error

**Severity:** Medium
**Class:** Edge case / Redis protocol compatibility
**Status:** `OPEN — not verified`

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
Falls out of the 80.1 glob translation (a glob translator never produces invalid regex); independently, wrap compile in try/catch → `ErrorReply` with a stable message (or treat as literal per Redis).

## Finding 4 (Bug 80.4): the `keys.matchMaxCount` escape knob is dead — DynConfig rejects the key it was designed for

**Severity:** Medium
**Class:** Correctness / operability
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
Cheapest: treat a leading `*` as "empty prefix" (prefix `""` iterates from the first key — `prefixMatch` already handles it via `iterator.seek(keyBytes(""))`), and treat a wildcard-free pattern as an exact-key existence lookup. At minimum downgrade `redis_command_support.md` and document the restriction.

## Finding 6 (Bug 80.6): silent truncation at 100 results — Redis returns all matches

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

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
If 80.2's real-iteration fix lands, either drop the cap or keep it configurable *and alive* (80.4); any retained cap should be documented. (Depends on 80.4 disposition; listed separately because the root cause — the cap itself — is independent of the knob being dead.)

## Finding 7 (Bug 80.7): `KEYS ""` throws AIOOBE — `data[1][0]` on a zero-length bulk argument

**Severity:** Low
**Class:** Edge case
**Status:** `OPEN — not verified`

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
Length-check first: `if (data[1].length == 0) return MultiBulkReply.EMPTY` (or route through the 80.1 translator which handles it naturally).

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

**Method:** <Spock temp spec | live Redis 7.2 differential | JDK repro | static inspection>

| Finding | Verdict | Evidence |
|---|---|---|
| 80.1 | | |
| 80.2 | | |
| 80.3 | | |
| 80.4 | | |
| 80.5 | | |
| 80.6 | | |
| 80.7 | | |

### Additional observations

### Recommended fix order (for the fix agent)

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 80.1 | | | | |
| 80.2 | | | | |
| 80.3 | | | | |
| 80.4 | | | | |
| 80.5 | | | | |
| 80.6 | | | | |
| 80.7 | | | | |

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 80.1 | Glob pattern evaluated as Java regex (`KEYS user:*` silent wrong results) | High | Correctness / Redis compat | | |
| 80.2 | KEYS source is 1%-sampled, periodically-wiped, delete-blind analysis DB | High | Correctness | | |
| 80.3 | `Pattern.compile` crash on glob-legal patterns (`foo[`, `foo\`) | Medium | Edge case / Redis compat | | |
| 80.4 | `keys.matchMaxCount` un-settable (DynConfig rejects the key; JSON edit breaks slot init) | Medium | Correctness / operability | | |
| 80.5 | `KEYS *` / wildcard-free `KEYS foo` rejected | Medium | Redis protocol compatibility | | |
| 80.6 | Silent truncation at 100 matches | Low | Redis protocol compatibility | | |
| 80.7 | `KEYS ""` AIOOBE → JVM error string reply | Low | Edge case | | |
