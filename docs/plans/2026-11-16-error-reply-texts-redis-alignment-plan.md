# Error Reply Texts Redis Alignment Implementation Plan
Date: 2026-11-16
Status: Approved for execution (round 3, 2026-11-16)
Related: doc/design/06_response_encoding_design.md, doc/design/10_acl_security_design.md, doc/design/16_cluster_management_design.md, doc/bug_reviews/bug_69_acl_module_review_round_1.md, ~/ws/redis/src (ground truth, Redis 7.2.14)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> **REVISIONS 1 AND 2 BELOW ARE AUTHORITATIVE** — where they conflict with the task steps, mapping tables, and Findings 4/5/6/9 in the body above, the revisions (latest wins) supersede.

**Goal:** Align Velo's client-facing error reply texts with Redis error reply texts (protocol/ACL/type/cluster families), per user request "need not exactly same"; text-only refactor, no behavior change.
**Architecture:** All changes are ErrorReply text constants in `src/main/java/io/velo/reply/ErrorReply.java` (using the existing private `rawErrorPrefix` ctor path) plus one inline `new ErrorReply(...)` text (SGroup.java:579), two constant swaps (SGroup.java:945,959 NOT_STRING→WRONG_TYPE) and one constant return (HGroup.java:646 → the new `ErrorReply.NOPROTO`). Do-not-change boundary: reply buffering logic (`buffer()`/`bufferAsHttp()`), constant names and reference identity, where/when each error is returned, all non-listed error texts, replication/sentinel/cluster wire parsing (verified: no main-code string matching on error texts — grep in Current Code Findings #7).
**Tech Stack:** Java 21, Groovy/Spock, Gradle, JaCoCo

## Current Behavior

`ErrorReply.buffer()` (ErrorReply.java:226-229) emits `-ERR <message>\r\n` normally, or `-<message>\r\n` when the private `rawErrorPrefix` ctor flag is set (already used by `WRONG_TYPE_HLL`, `READONLY`, `BUSYKEY`, `clusterMoved`). Many widely-used constants carry non-Redis texts, e.g. `WRONG_TYPE` = `"wrong type"`, `NO_AUTH` = `"no auth"`, `AUTH_FAILED` = `"auth failed !WRONGPASS!"`, and `WRONG_NUMBER(cmd)` has a stray leading `*` (`"*wrong number of arguments for 'x' command"`).

## Target Behavior

Constants/inline texts listed in the mapping below match Redis texts except the rows the mapping tables mark as Velo-authored paraphrases (cited from ~/ws/redis/src, Redis 7.2.14). Redis error-code replies (`WRONGTYPE`, `NOAUTH`, `WRONGPASS`, `NOPERM`, `CROSSSLOT`, `CLUSTERDOWN`, `NOPROTO`) use `rawErrorPrefix=true` so the wire form is `-WRONGTYPE ...` not `-ERR WRONGTYPE ...` (Redis sends `-WRONGTYPE ...`; acl.c/server.c cite "-ERR"-style entries with a leading `-` inside the string which `addReplyError` emits without adding another ERR). Everything else in ErrorReply.java and all other inline sites are explicitly out of scope.

## Command Path Audit

Every command funnels through `RequestHandler.handle` (src/main/java/io/velo/RequestHandler.java) / `MultiWorkerServer` response encoding; ErrorReply constants are the single source of error texts. Sites touched: ErrorReply.java constants; inline sites SGroup.java:578-579,945,959, HGroup.java:646 (PGroup.java:725 dropped in Revision 1). Consumers verified for text-matching: `MultiWorkerServer.java:336` and `RequestHandler.java:308-466` compare by reference (`== ErrorReply.NO_AUTH` etc.), not text; no `getMessage()`-based dispatch exists in main code (grep in Finding #7). Test consumers asserting raw wire text are enumerated per task.

## Open Review Question

**Both questions were closed in Revision 1 — see Revision 1 "Header fixes": keep `NOT_INTEGER` / `INVALID_INTEGER` / `NOT_INTEGER_OR_OUT_OF_RANGE` and `FORMAT` / `SYNTAX` as separate constants (no aliasing, no rename).**

1. `NOT_INTEGER` ("not integer", ~90 main-code uses) and `INVALID_INTEGER` ("invalid integer", ~55 uses) both map to Redis `"value is not an integer or out of range"`, which duplicates existing `NOT_INTEGER_OR_OUT_OF_RANGE`. Plan keeps all three constants (same text, distinct call sites) to avoid a 145-site rename — acceptable, or should we alias?
2. `FORMAT` ("format", ~240 uses incl. dyn) maps to `"syntax error"` which equals existing `SYNTAX`. Same aliasing question.

## Current Code Findings

1. `src/main/java/io/velo/reply/ErrorReply.java:14` — `FORMAT = new ErrorReply("format")`; used 204 times across all command groups + dyn commands (measured) as generic bad-arguments error.
2. `src/main/java/io/velo/reply/ErrorReply.java:20-22` —
```java
public static ErrorReply WRONG_NUMBER(String cmd) {
    return new ErrorReply("*wrong number of arguments for '" + cmd + "' command");
}
```
Redis: `"wrong number of arguments for '%s' command"` (networking.c:681, server.c:3803, acl.c:3038). The `*` is not in Redis text.
3. `src/main/java/io/velo/reply/ErrorReply.java:46-61` — `AUTH_FAILED="auth failed !WRONGPASS!"`, `NO_AUTH="no auth"`, `ACL_PERMIT_LIMIT="user acl permit limit !NOPERM!"`, `ACL_PERMIT_KEY_LIMIT="user acl permit limit !NOPERM! !key!"`, `ACL_SETUSER_RULE_INVALID="!Error in ACL SETUSER modifier!"`. Redis: `"-WRONGPASS invalid username-password pair or user is disabled."` (acl.c:1458), `"-NOAUTH Authentication required."` (server.c:1869), `"-NOPERM %s"` with text from getAclErrorMessage `"User %S has no permissions to run the '%S' command"` / `"...to access the '%S' key"` (acl.c:2658-2664, 3943), `"Error in ACL SETUSER modifier '%s': %s"` (acl.c:2070).
4. `src/main/java/io/velo/reply/ErrorReply.java:73-100` — `NOT_INTEGER="not integer"`, `NOT_FLOAT="not float"`, `NOT_STRING="not string"`, `INVALID_INTEGER="invalid integer"`, `WRONG_TYPE="wrong type"`. Redis: `"value is not an integer or out of range"` (object.c:901), `"value is not a valid float"` (object.c:834,868), `"-WRONGTYPE Operation against a key holding the wrong kind of value"` (server.c:1842). `NOT_STRING` only used at SGroup.java:945,959 — both are wrong-type-on-read contexts (`if (!cv.isTypeString()) return ErrorReply.NOT_STRING;` inside SET NX GET / SET XX GET paths) → should be `WRONG_TYPE`, then constant becomes unused and is deleted.
5. `src/main/java/io/velo/reply/ErrorReply.java:94,125,137,155-158` — `RANGE_OUT_OF_INDEX="range out of index"` (uses: ZGroup.java:2585, BGroup.java:1232, LGroup.java:587,644 — numeric range params where Redis replies `"value is out of range"`, object.c:917/t_hash.c:1147), `NOT_SUPPORT="not support"` → `"operation not supported"` (Velo-specific capabilities; no exact Redis text), `CLUSTER_SLOT_CROSS_SHARDS="cluster slot cross shards"` → Redis `"-CROSSSLOT Keys in request don't hash to the same slot"` (cluster.c:7570), `CLUSTER_SLOT_NOT_SET="cluster slot not set"` → Redis `"-CLUSTERDOWN Hash slot not served"` (cluster.c:7581). Used only in MultiWorkerServer.java:2,3 sites (slot pre-check).
6. Inline sites: `src/main/java/io/velo/command/SGroup.java:579` `INVALID_SCAN_CURSOR="invalid scan cursor"` → Redis `"invalid cursor"` (db.c:898); `src/main/java/io/velo/command/HGroup.java:646` `"Server protover not support: " + ver` (HELLO) → Redis `"-NOPROTO unsupported protocol version"` (networking.c:3566). `src/main/java/io/velo/command/PGroup.java:725` `"unknown subcommand '" + subCmd + "'. Try PUBSUB HELP."` is **already** Redis' unknown-subcommand wording (server.c:3776) — the Redis arity variant is networking.c:1170 — so Revision 1 drops the PGroup change.
7. No main-code text matching on error messages: `rg 'getMessage\(\)\.(equals|startsWith|contains)|"(MOVED|ERR|WRONGTYPE|auth failed|no auth|wrong type|server stopped)"' src/main/java src/main/groovy dyn/src` → only ErrorReply.java definitions hit. Reference comparisons: `MultiWorkerServer.java:336` (`reply == ErrorReply.NO_AUTH || ...`), `RequestHandler.java:308-466`.
8. Wire-text assertions in tests that must move with the change: `src/test/groovy/io/velo/reply/RESPReplyTest.groovy:58-62` (`"*wrong number..."`, `"-ERR unknown command 'test'\r\n"` — the latter unchanged), `src/test/groovy/io/velo/MultiWorkerServerTest.groovy:150,151` (`'-ERR format\r\n'`), `:228,479` (`'-ERR user acl permit limit !NOPERM!\r\n'`), `:399,400` (`'-ERR no auth\r\n'`), `src/test/groovy/io/velo/command/SGroupTest.groovy` (2 asserts vs `ErrorReply.NOT_STRING`). `src/test/groovy/io/velo/command/PGroupTest.groovy:842` is **not** affected — Revision 1 dropped the PGroup change. All other ~500 test references compare constant references (`reply == ErrorReply.X`) and are unaffected by text change.
9. Already Redis-aligned (no change needed, verified against redis src): `SYNTAX="syntax error"`, `NO_SUCH_KEY="no such key"` (server.c:1845 `"-ERR no such key\r\n"`), `INDEX_OUT_OF_RANGE` (server.c:1851), `INCR_DECR_OVERFLOW` (t_string.c:612, t_hash.c:658), `NAN_OR_INFINITY` (t_hash.c:680), `NOT_INTEGER_OR_OUT_OF_RANGE`, `VALUE_NOT_POSITIVE`, `READONLY` (server.c:1867), `BUSYKEY` (server.c:1877), `MIGRATE_KEY_NOT_EMPTY`, CGroup.java:245,568,634,644, LGroup.java:557, EGroup/SGroup/HGroup/GGroup `"invalid expire time in '%s' command"` (networking.c:686), AGroup.java:166 `"Unknown category"` (acl.c:2922 style), FGroup.java:103, RGroup.java:562, MGroup.java:649,738, BGroup bloom texts (RedisBloom module texts, e.g. "err should be between 0 and 1" — Velo's "error must be > 0 and < 1" acceptable under "need not exactly same").
10. Deliberately out of scope (Velo-internal, no Redis counterpart, or text implies behavior): `SERVER_STOPPED`, `DICT_MISSING`, `KEY_TOO_LONG`, `VALUE_TOO_LONG`, `LIST/HASH/SET/ZSET_SIZE_TO_LONG`, `SET/ZSET_MEMBER_LENGTH_TO_LONG`, `BF_*`, `CAPACITY_NOT_POSITIVE`, `BAD_DATA_FORMAT`, `DUMP_TYPE_NOT_SUPPORT`, `DEBUG_SUB_COMMAND_NOT_SUPPORT`, `NO_SUCH_FILE`, `INVALID_FILE`, `CANNOT_READ`, `CURSOR_NOT_MATCH` (SGroup.java:578, internal double-buffer state), SGroup.java:663 `"type not support"` (SCAN TYPE alias check), SGroup.java:1763-1772 SORT texts (Redis clamps negative LIMIT, sort.c:331-343 — different behavior, not a text issue), WGroup.java:85 (WAIT timeout cap), IGroup ingest texts, dyn Clusterx/Manage/Config texts.

---

### Task 1: Raw-prefix protocol/ACL/cluster error texts

**Files:**
- Modify: `src/main/java/io/velo/reply/ErrorReply.java` (WRONG_TYPE, AUTH_FAILED, NO_AUTH, ACL_PERMIT_LIMIT, ACL_PERMIT_KEY_LIMIT, CLUSTER_SLOT_CROSS_SHARDS, CLUSTER_SLOT_NOT_SET, ACL_SETUSER_RULE_INVALID)
- Test: `src/test/groovy/io/velo/reply/RESPReplyTest.groovy`
- Test: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy` (update 4 raw-text asserts)
- Reference: doc/design/10_acl_security_design.md, doc/design/16_cluster_management_design.md

Mapping (all `rawErrorPrefix=true`, cite: redis src):

| Constant | New message |
|---|---|
| WRONG_TYPE | `WRONGTYPE Operation against a key holding the wrong kind of value` |
| AUTH_FAILED | `WRONGPASS invalid username-password pair or user is disabled.` |
| NO_AUTH | `NOAUTH Authentication required.` |
| ACL_PERMIT_LIMIT | `NOPERM this user has no permissions to run the command` |
| ACL_PERMIT_KEY_LIMIT | `NOPERM this user has no permissions to access the key` |
| CLUSTER_SLOT_CROSS_SHARDS | `CROSSSLOT Keys in request don't hash to the same slot` |
| CLUSTER_SLOT_NOT_SET | `CLUSTERDOWN Hash slot not served` |

`ACL_SETUSER_RULE_INVALID` keeps ERR prefix: text → `Error in ACL SETUSER modifier` (drop `!...!`).

**Step 1: Write the failing test** — extend RESPReplyTest error-reply block: for each raw constant assert `constant.message == '<new text>'` and `constant.buffer().asArray() == "-<new text>\r\n".bytes`; update MultiWorkerServerTest:228,479 to `'-NOPERM this user has no permissions to run the command\r\n'` and :399,400 to `'-NOAUTH Authentication required.\r\n'`.

**Step 2: Run test to verify it fails**
```bash
./gradlew :test --tests "io.velo.reply.RESPReplyTest.test error reply"
```
Expected: fail because constants still have old texts (`assert ErrorReply.WRONG_TYPE.message == 'WRONGTYPE ...'` fails on `"wrong type"`).

**Step 3: Write minimal implementation** — edit the 8 constants in ErrorReply.java, using the private 2-arg ctor for raw ones (same pattern as READONLY at ErrorReply.java:131).

**Step 4: Run test to verify it passes**
```bash
./gradlew :test --tests "io.velo.reply.RESPReplyTest.test error reply"
```
plus `./gradlew :test --tests "io.velo.MultiWorkerServerTest.test server request and response"` (updated raw asserts), `--tests "io.velo.RequestHandlerTest.test malformed http basic auth returns auth failed"` (reference-equality, must stay green).
Expected: pass

**Step 5: Check JaCoCo coverage**
```bash
python3 scripts/jacoco_cover.py io.velo.reply.ErrorReply 1 235 --src
```
Expected: constants + buffer() branches covered (both rawErrorPrefix true/false paths asserted).

### Task 2: ERR-prefixed value/format texts + WRONG_NUMBER stray `*`

**Files:**
- Modify: `src/main/java/io/velo/reply/ErrorReply.java` (NOT_INTEGER, INVALID_INTEGER, NOT_FLOAT, FORMAT, RANGE_OUT_OF_INDEX, NOT_SUPPORT, WRONG_NUMBER)
- Test: `src/test/groovy/io/velo/reply/RESPReplyTest.groovy`
- Test: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy` (update 2 raw-text asserts at :150,151)

Mapping: `NOT_INTEGER`→`value is not an integer or out of range`; `INVALID_INTEGER`→same; `NOT_FLOAT`→`value is not a valid float`; `FORMAT`→`syntax error`; `RANGE_OUT_OF_INDEX`→`value is out of range`; `NOT_SUPPORT`→`operation not supported`; `WRONG_NUMBER(cmd)`→`wrong number of arguments for '<cmd>' command` (drop `*`).

**Step 1: Write the failing test** — RESPReplyTest: assert each constant's new `.message` and wire form `-ERR <new text>\r\n`; `WRONG_NUMBER('test').buffer()` == `"-ERR wrong number of arguments for 'test' command\r\n"`; MultiWorkerServerTest:150,151 → `'-ERR syntax error\r\n'`.

**Step 2: Run test to verify it fails**
```bash
./gradlew :test --tests "io.velo.reply.RESPReplyTest.test error reply"
```
Expected: fail because old texts differ (`FORMAT.message == 'format'` != `'syntax error'`, WRONG_NUMBER keeps `*`).

**Step 3: Write minimal implementation** — edit the 7 constants.

**Step 4: Run test to verify it passes**
```bash
./gradlew :test --tests "io.velo.reply.RESPReplyTest.test error reply" && ./gradlew :test --tests "io.velo.MultiWorkerServerTest.test server request and response"
```
plus spot-check dyn constant-reference tests stay green: `./gradlew :test --tests "io.velo.command.ManageCommandTest"`.
Expected: pass

**Step 5: Check JaCoCo coverage**
```bash
python3 scripts/jacoco_cover.py io.velo.reply.ErrorReply 14 180 --src
```
Expected: changed lines covered.

### Task 3: Inline command texts + retire NOT_STRING

**Files:**
- Modify: `src/main/java/io/velo/command/SGroup.java` (:579 cursor text; :943,957 NOT_STRING→WRONG_TYPE; delete NOT_STRING in ErrorReply.java)
- Modify: `src/main/java/io/velo/command/HGroup.java` (:646 HELLO protover → raw `NOPROTO unsupported protocol version`)
- Modify: `src/main/java/io/velo/command/PGroup.java` (:725 → `Unknown subcommand or wrong number of arguments for '<subCmd>'. Try PUBSUB HELP.`)
- Test: `src/test/groovy/io/velo/command/SGroupTest.groovy`, `src/test/groovy/io/velo/command/PGroupTest.groovy`
- Reference: doc/design/04_command_processing_design.md

**Step 1: Write the failing test** — SGroupTest: the 2 asserts using `ErrorReply.NOT_STRING` → `ErrorReply.WRONG_TYPE`; add/extend scan test asserting `(reply as ErrorReply).message == 'invalid cursor'`; PGroupTest:842 → `"Unknown subcommand or wrong number of arguments for 'xxx'. Try PUBSUB HELP."`; HGroupTest: add assert HELLO with bad protover returns ErrorReply with message `'NOPROTO unsupported protocol version'` and wire form starts with `-NOPROTO` (raw ctor via `new ErrorReply(msg, true)`).

**Step 2: Run test to verify it fails**
```bash
./gradlew :test --tests "io.velo.command.SGroupTest.test scan" --tests "io.velo.command.PGroupTest.test pubsub" --tests "io.velo.command.HGroupTest.test hello"
```
Expected: fail because texts still `'invalid scan cursor'` / `'unknown subcommand ...'` / `'Server protover not support: 9'`, and SGroup still returns NOT_STRING; compile error on `ErrorReply.NOT_STRING` reference is the expected failure for SGroupTest (constant deleted in Step 3 — Groovy resolves at runtime, so test asserts fail before deletion, compile fine).

**Step 3: Write minimal implementation** — change the 4 inline texts (SGroup:579; HGroup:646 use `new ErrorReply("NOPROTO unsupported protocol version", true)`; PGroup:725), replace 2 NOT_STRING uses with `ErrorReply.WRONG_TYPE`, delete `NOT_STRING` constant (ErrorReply.java:85).

**Step 4: Run test to verify it passes**
```bash
./gradlew :test --tests "io.velo.command.SGroupTest.test scan" --tests "io.velo.command.SGroupTest.test get*" --tests "io.velo.command.PGroupTest.test pubsub" --tests "io.velo.command.HGroupTest.test hello"
```
Expected: pass

**Step 5: Check JaCoCo coverage**
```bash
python3 scripts/jacoco_cover.py io.velo.command.SGroup 930 970 --src && python3 scripts/jacoco_cover.py io.velo.command.HGroup 640 670 --src
```
Expected: changed branches (isTypeString false path, protover default branch) covered.

### Task 4 (final): Commit plan + broader regression + validation status

**Step 1:** Run the wider affected suites (command groups touching changed constants): `./gradlew :test --tests "io.velo.command.*GroupTest" --tests "io.velo.reply.*" --tests "io.velo.RequestHandlerTest"`; append `## Validation status` to this plan.
**Step 2:** `git add` per-task commits already done; this task commits this plan doc + any test-remainder: `git commit -m "refactor: align error reply texts with redis"`
**Step 3:** `git status --short` → Expected: only intended files

---

## Review Checklist

- Wire form of raw constants is `-WRONGTYPE ...` (no `-ERR WRONGTYPE ...` double prefix)
- No constant renamed; constants deleted: `NOT_STRING`, `TARGET_KEY_BUSY` (unused); constant added: `NOPROTO` (Revision 1/2); no call-site moved to a different error semantically (NOT_STRING sites are wrong-type reads → WRONG_TYPE)
- No text changes outside the mapping tables; items in Finding 10 untouched
- No new `getMessage()`-based dispatch introduced anywhere
- `WRONG_NUMBER` output no longer starts with `*`
- Tests updated assert new texts; no test left asserting old texts (`'format'`, `'no auth'`, `'!NOPERM!'`, `'invalid scan cursor'`, etc.)
- Each task = one commit, each green independently; JaCoCo inspected per task

## Review Feedback (appended per consensus round)

<!-- use templates/review-round.md; rounds append here; revisions append "## Revision <k> — Response to Round <k>" sections -->

### Plan Review — Round 1
Date: 2026-11-16
Reviewer: AI agent 2
Plan under review: docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md (revision 0)
Method: independently read `src/main/java/io/velo/reply/ErrorReply.java`, `MultiWorkerServer.java`, `RequestHandler.java`, `command/SGroup.java`, `command/HGroup.java`, `command/PGroup.java`, the affected test classes, `doc/design/06_response_encoding_design.md`, `doc/design/10_acl_security_design.md`, `doc/design/16_cluster_management_design.md`, and the Redis ground truth at `~/ws/redis/src` (my own greps for call sites and derived paths)

#### Resolved verification (round 0 findings)
n/a — first round.

#### Verified as correct (so the planner does not re-derive them)

- The raw-prefix mechanism is right: Redis emits these codes without an `ERR` prefix because `addReplyError()` passes a leading `-` through verbatim. Verified `server.c:1841-1842` (`-WRONGTYPE Operation against a key holding the wrong kind of value`), `server.c:1868-1869` (`-NOAUTH Authentication required.`), `acl.c:1458` (`-WRONGPASS invalid username-password pair or user is disabled.`), `cluster.c:7570` (`-CROSSSLOT Keys in request don't hash to the same slot`), `cluster.c:7581` (`-CLUSTERDOWN Hash slot not served`), `networking.c:3566` (`-NOPROTO unsupported protocol version`). `ErrorReply.buffer()` at `ErrorReply.java:226-229` reproduces this with `rawErrorPrefix=true`.
- Texts verified byte-exact in the ground truth: `invalid cursor` (`db.c:898`), `value is out of range` (`object.c:917`, `t_hash.c:1147`), `value is not an integer or out of range` (`object.c:901`), `value is not a valid float` (`object.c:834,868`), `syntax error` (`server.c:1846-1847`), `no such key` (`server.c:1844-1845`), `index out of range` (`server.c:1850-1851`), `wrong number of arguments for '%s' command` (`networking.c:681`, `server.c:3803`, `acl.c:3038`), `READONLY You can't write against a read only replica.` (`server.c:1867-1868`), `BUSYKEY Target key name already exists.` (`server.c:1876-1877`).
- The "no text-based dispatch" claim holds. I grepped `src/main/java`, `src/main/groovy`, `dyn/src` for `getMessage()`-driven dispatch, `"-ERR"`, `MOVED`, `ASK`, `WRONGTYPE`, `CLUSTERDOWN`, `CROSSSLOT`: the only hits are the constant definitions plus comments (`GGroup.java:455`, `PGroup.java:227,518,589,609`). The only identity checks are `MultiWorkerServer.java:336` and `RequestHandler.java:308-466`, which compare constant references and are unaffected by text changes.
- The inline-site sweep holds: I extracted every literal `new ErrorReply("…")` in `src/main/java`, `src/main/groovy` and `dyn/src` (177 sites) and confirmed that outside `ErrorReply.java` no inline site duplicates a constant text, so no "stale duplicate" of `WRONG_TYPE` / `NO_AUTH` / `NOT_INTEGER` exists.
- `NOT_STRING` → `WRONG_TYPE` is semantically right: the two sites are `SET … NX GET` (`SGroup.java:945`) and `SET … XX GET` (`SGroup.java:959`), and Redis answers a non-string key there with `-WRONGTYPE`.

#### New findings

### R1-N1: Task 3's `new ErrorReply(msg, true)` cannot compile — the 2-arg ctor is private
**Severity:** Blocking
**Evidence:** `src/main/java/io/velo/reply/ErrorReply.java:185-192`
```java
public ErrorReply(String message) {
    this(message, false);
}

private ErrorReply(String message, boolean rawErrorPrefix) {
```
`src/main/java/io/velo/command/HGroup.java:646` is in package `io.velo.command`; the only raw-prefix ctor is `private` in `io.velo.reply`. The plan's Task 3 Step 3 ("HGroup:646 use `new ErrorReply("NOPROTO unsupported protocol version", true)`") will not compile.
**Required:** add a public creation path in `ErrorReply` — e.g. `public static ErrorReply rawPrefix(String message)` or a `public static final ErrorReply NOPROTO = new ErrorReply("NOPROTO unsupported protocol version", true);` — and have Task 3 use it. Name the exact symbol in the plan so Task 3 has no forward reference to a later task.

### R1-N2: The `--tests` filters name test methods that do not exist, and miss the asserts that move
**Severity:** Blocking
**Evidence:**
- `src/test/groovy/io/velo/reply/RESPReplyTest.groovy:7,30,45,123` — the only methods are `test static`, `test static as http`, `test others`, `test async reply`. The error-reply asserts live in `test others` (`:45-62`). There is no `io.velo.reply.RESPReplyTest.test error reply`; Task 1 Step 2/Step 4 and Task 2 Step 2/Step 4 all run that name, so Gradle aborts with "No tests found for given includes" instead of the intended assertion failure.
- `src/test/groovy/io/velo/MultiWorkerServerTest.groovy` — there is no `test server request and response`. The four asserts are in: `:150,151` in `test shutdown command requests server stop when acl permits` (`:42`); `:228` in `test shutdown command is blocked by acl dangerous disallow` (`:167`); `:399,400` in `test fast auth gate keeps quit available` (`:326`); `:479` in `test pipeline auth refresh acl state` (`:412`).
- `src/test/groovy/io/velo/command/SGroupTest.groovy:644` is in `test set` (`:574`) and `:790` in `test set nx get and xx get preserve flags` (`:738`). Task 3 Step 4 runs `SGroupTest.test scan` and `SGroupTest.test get*` — there is no `test get*` method in `SGroupTest`, and neither of the two methods that actually hold the `ErrorReply.NOT_STRING` asserts is executed, so the changed assertions would never run.
**Required:** replace every filter with an existing method name (or add a named method and name it explicitly), and make Task 3 Step 4 run `io.velo.command.SGroupTest.test set` and `io.velo.command.SGroupTest.test set nx get and xx get preserve flags`.

### R1-N3: Stale Velo citations in `Current Code Findings` / `Command Path Audit`
**Severity:** Blocking
**Evidence:** `src/main/java/io/velo/command/SGroup.java:945` and `:959` are the `ErrorReply.NOT_STRING` returns (the plan says `943,957` in Finding 4 and `SGroup.java:578-579,943,957` in the Command Path Audit). `src/main/java/io/velo/command/SGroup.java:578-579` are `CURSOR_NOT_MATCH` / `INVALID_SCAN_CURSOR` (correct). Finding 5 claims `RANGE_OUT_OF_INDEX` has "ZGroup.java:1" use — there is no `RANGE_OUT_OF_INDEX` in `ZGroup.java`; the only uses are `BGroup.java:1232`, `LGroup.java:587`, `LGroup.java:644`.
**Required:** re-derive every `file:line` anchor from the current tree and fix the plan before execution.

### R1-N4: Redis ground-truth citations are misattributed to the wrong file
**Severity:** Medium
**Evidence** (all verified in `~/ws/redis/src`):

| Plan citation | Actual location |
|---|---|
| `networking.c:3038/3803, server.c:681` (wrong number of arguments) | `acl.c:3038`, `server.c:3803`, `networking.c:681` |
| `db.c:901` (value is not an integer or out of range) | `object.c:901` |
| `t_string.c:834/868` (value is not a valid float) | `object.c:834`, `object.c:868` |
| `t_string.c:1147` (value is out of range) | `t_hash.c:1147` |
| `server.c:3566` (NOPROTO) | `networking.c:3566` |
| `server.c:3776` ("Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.") | `networking.c:1170` (lowercase "unknown"); `server.c:3776` is the *other* variant "unknown subcommand '%.128s'. Try %s HELP." |
| `server.c:686` (invalid expire time) | `networking.c:686` |
| `t_string.c:612/658` (overflow) | `t_string.c:612`, `t_hash.c:658` |
| `t_string.c:680` (value is NaN or Infinity) | `t_hash.c:680` (string counterpart is `t_string.c:676` "increment would produce NaN or Infinity") |

**Suggested:** the file names are systematically swapped (`networking.c` ↔ `server.c` ↔ `object.c` ↔ `t_hash.c`/`t_string.c`). Re-check each anchor; an implementation that follows the plan's anchors will not find the cited text.

### R1-N5: The declared ground truth version is wrong
**Severity:** Medium
**Evidence:** the plan's `Related:` says `~/ws/redis/src (ground truth, Redis 8.x unstable)`, but `~/ws/redis/src/version.h` reads `#define REDIS_VERSION "7.2.14"`. Several cited texts are 7.2-specific.
**Required:** state `Redis 7.2.14` as the ground truth (or point at an 8.x tree and re-verify the anchors), so that a later reader knows which tree the mappings were checked against.

### R1-N6: The two `NOPERM` rows are not Redis texts, and they contradict the plan's own Target Behavior
**Severity:** Medium
**Evidence:** Redis builds these dynamically — `acl.c:2655-2666` `getAclErrorMessage()`: `User %S has no permissions to run the '%S' command` (ACL_DENIED_CMD), `User %S has no permissions to access the '%S' key` / `No permissions to access a key` (ACL_DENIED_KEY), emitted as `-NOPERM %s` by `acl.c:3943` `rejectCommandFormat(c, "-NOPERM %s", msg)`. The plan's `NOPERM this user has no permissions to run the command` / `NOPERM this user has no permissions to access the key` match neither form, so the Target Behavior claim "Constants/inline texts listed in the mapping below match Redis texts" is false for these two rows. Note `doc/bug_reviews/bug_69_acl_module_review_round_1.md:165-170` already records the Redis expectations ("NOPERM No permissions to access a key", "NOPERM User ro has no permissions to run the 'lset' command").
**Required:** either use Redis' static-compatible wording (`NOPERM No permissions to access a key` for the key case) or state explicitly in the mapping table which rows are Redis-derived and which are Velo-authored paraphrases. The user asked for "need not exactly same", but the plan must not claim Redis fidelity for paraphrases.

### R1-N7: The PUBSUB change makes Velo *less* Redis-aligned, and the cited line does not contain the quoted text
**Severity:** Medium
**Evidence:** `src/main/java/io/velo/command/PGroup.java:725` currently returns `"unknown subcommand '" + subCmd + "'. Try PUBSUB HELP."`, which is already Redis' unknown-subcommand form (`server.c:3775-3776`, lowercase "unknown"). Redis' other variant (`networking.c:1166-1172`, `addReplySubcommandSyntaxError`) is `"unknown subcommand or wrong number of arguments for '%.128s'. Try %s HELP."` — also lowercase. The plan proposes `"Unknown subcommand or wrong number of arguments for 'xxx'. Try PUBSUB HELP."` (capital U, and the "or wrong number of arguments" wording) for the *unknown subcommand* path.
**Required:** pick one and cite it correctly — keep the current text (it is Redis' unknown-subcommand wording), or adopt `networking.c:1170` verbatim (lowercase). Also decide whether Velo's single error covers both "unknown subcommand" and "wrong arity" (in Redis they are two different messages) and say so in the plan.

### R1-N8: Tasks 1-3 have no commit step
**Severity:** Medium
**Evidence:** the plan ends Tasks 1-3 at "Step 5: Check JaCoCo coverage"; Task 4 Step 2 says "`git add` per-task commits already done", but no task contains the commit step.
**Required:** give each task its own commit step with a one-line conventional message (e.g. `refactor: align the raw error reply texts with redis`, `refactor: align the value error reply texts with redis`, `refactor: align the inline command error texts with redis`), so that "one concern one commit" holds and the final Task 4 commit only carries the plan doc.

### R1-N9: The Task 4 regression filter skips the dyn command tests and the ACL test
**Severity:** Medium
**Evidence:** `dyn/test/io/velo/command/ClusterxCommandTest.groovy:254-330`, `dyn/test/io/velo/command/ConfigCommandTest.groovy:98-99`, `dyn/test/io/velo/command/ManageCommandTest.groovy:441,479,…` assert `ErrorReply.NOT_INTEGER` / `INVALID_INTEGER` / `FORMAT`, and `src/test/groovy/io/velo/command/AGroupTest.groovy:421-448` asserts `ErrorReply.ACL_SETUSER_RULE_INVALID`. The filter `--tests "io.velo.command.*GroupTest" --tests "io.velo.reply.*" --tests "io.velo.RequestHandlerTest"` matches none of these (the dyn tests are named `*CommandTest`, `AGroupTest` is not in the `*GroupTest` glob's scope? — it is, but the ACL constant change is in `AGroup` and the file is covered; the dyn ones are not).
**Required:** extend the filter, e.g. `--tests "io.velo.command.*CommandTest" --tests "io.velo.command.AGroupTest"`.

### R1-N10: `FORMAT` → "syntax error" silently changes the meaning of arity errors
**Severity:** Medium
**Evidence:** `ErrorReply.FORMAT` is the generic bad-argument error at ~205 main-code sites (plan says ~240 incl. dyn; measured 205 main+dyn, 379 including tests), and it is used for *arity* failures, e.g. `SGroup.java:583` (`if (data.length < 2) return ErrorReply.FORMAT;` in `scan()`), `RequestHandler.java:357,409,455,477,553`, `HGroup.java`, `LGroup.java`, `ZGroup.java`. Redis answers arity failures with "wrong number of arguments for '<cmd>' command" (`networking.c:686`), not "syntax error"; Velo's own `ErrorReply.WRONG_NUMBER(cmd)` (`MultiWorkerServer.java:613,676,703`) is the arity answer.
**Required:** state in the plan that `FORMAT` covers both arity and bad-value errors, so "syntax error" is a deliberate simplification and not a Redis alignment; alternatively route arity sites to `WRONG_NUMBER(cmd)`.

### R1-N11: The two "Open Review Question" items must be closed before execution
**Severity:** Medium
**Evidence:** plan lines 24-27 leave the aliasing decision open ("Plan keeps all three constants … acceptable, or should we alias?") and the same for `FORMAT`/`SYNTAX`.
**Required:** turn both into decisions in the plan body. Reviewer ruling: **keep the constants separate** — do not alias `NOT_INTEGER` / `INVALID_INTEGER` / `NOT_INTEGER_OR_OUT_OF_RANGE` (avoids a ~130-site rename), and keep `FORMAT` separate from `SYNTAX`. Add the side effect noted in R1-N12 to the plan.

### R1-N12: Aliasing the texts makes two constants `equals()`, weakening existing assertions
**Severity:** Low
**Evidence:** `ErrorReply.equals()` compares `message` and `rawErrorPrefix` (`ErrorReply.java:208-218`), so after Task 2 `ErrorReply.NOT_INTEGER == ErrorReply.INVALID_INTEGER` becomes true (Groovy `==` calls `equals()`), and `FORMAT` will equal `SYNTAX`. Existing assertions that use `reply == ErrorReply.INVALID_INTEGER` can then no longer distinguish the branch — e.g. `dyn/test/io/velo/command/ClusterxCommandTest.groovy:254` (`NOT_INTEGER`) vs `:260` (`INVALID_INTEGER`), and `src/test/groovy/io/velo/command/WGroupTest.groovy:139` vs `:155` (`WGroup.java:77` returns `NOT_INTEGER`, `WGroup.java:81` returns `INVALID_INTEGER`).
**Suggested:** note this in the plan; for the pairs where the branch matters, assert on `.message` plus the branch condition, or keep the two constants distinguishable.

### R1-N13: `ACL_SETUSER_RULE_INVALID` loses the modifier detail
**Severity:** Low
**Evidence:** Redis: `"Error in ACL SETUSER modifier '%s': %s"` (`acl.c:2069-2070`); the plan proposes the bare `"Error in ACL SETUSER modifier"`, and the current Velo call site `src/main/java/io/velo/command/AGroup.java:470` returns the static constant.
**Suggested:** either keep the current text or make the constant a factory that carries the offending modifier; if the bare text stays, say that the detail loss is accepted.

### R1-N14: `CROSSSLOT` / `CLUSTERDOWN` are a semantic approximation — state it
**Severity:** Low
**Evidence:** Velo's `ErrorReply.CLUSTER_SLOT_CROSS_SHARDS` is raised by `MultiWorkerServer.checkClusterSlot()` (`MultiWorkerServer.java:394,404`) when the request's keys span two different *shards* (Velo shard = a node's slot range), while Redis' `-CROSSSLOT` (`cluster.c:7570`) is raised when keys hash to different *slots* of the same node. `CLUSTER_SLOT_NOT_SET` (`MultiWorkerServer.java:366,371,385`) is only reachable with `ConfForGlobal.clusterEnabled` (`MultiWorkerServer.java:568`), which is consistent with the Redis "Hash slot not served" case.
**Suggested:** add one sentence to the plan describing the shard-vs-slot difference so a future reader does not "fix" the mapping.

### R1-N15: Finding 9 mixes Redis-aligned texts with Velo-specific ones and contains unanchored claims
**Severity:** Low
**Evidence:** `MGroup.java:649` and `MGroup.java:738` ("Target instance replied with error: …") are Velo replication texts, not Redis texts, yet they are listed under "Already Redis-aligned (no change needed, verified against redis src)". "BGroup bloom texts" carries no `file:line` anchor, violating the plan's own citation rule.
**Required:** split Finding 9 into "already Redis-aligned" (with anchors) and "Velo-specific, deliberately untouched" (with anchors).

### R1-N16: The `Related:` design docs contain no error-text contract
**Severity:** Info
**Evidence:** `doc/design/06_response_encoding_design.md` only links `ErrorReply.java` (line 20) and states no text/prefix contract; `doc/design/10_acl_security_design.md` and `doc/design/16_cluster_management_design.md` contain no error-text mentions at all (grep for `NOPERM|no auth|WRONGPASS|ErrorReply|CROSSSLOT|CLUSTERDOWN` → no hits).
**Suggested:** write "read; no error-text contract defined, so the boundary comes from the code facts in Current Code Findings" next to each `Related:` entry — that is exactly the "do not change" boundary the Architecture section claims.

### R1-N17: Task 3 Step 2's expected-failure wording is self-contradictory
**Severity:** Info
**Evidence:** plan line 142 says "compile error on `ErrorReply.NOT_STRING` reference is the expected failure for SGroupTest (constant deleted in Step 3 — Groovy resolves at runtime, so test asserts fail before deletion, compile fine)".
**Required:** state it once: Groovy resolves `ErrorReply.NOT_STRING` at runtime, so before the deletion the two asserts fail on the *value* mismatch (expected `WRONG_TYPE`, got `NOT_STRING`), and after the deletion they would fail with `MissingPropertyException`; the deletion and the assert rewrite must land in the same step.

### R1-N18: Approximate usage counts are off
**Severity:** Info
**Evidence:** measured in `src/main/java` + `src/main/groovy` + `dyn/src`: `ErrorReply.NOT_INTEGER` 87 uses (plan: ~90, OK), `ErrorReply.INVALID_INTEGER` 45 uses (plan: ~55), `ErrorReply.FORMAT` 205 uses (plan: ~240). Counts are only motivational, but the plan should use measured numbers.

#### Blind-spot sweep

Greps I ran beyond the plan's own: `getMessage()`-driven dispatch, `"-ERR"`, `MOVED`/`ASK`/`WRONGTYPE`/`CLUSTERDOWN`/`CROSSSLOT` in `src/main/java`, `src/main/groovy`, `dyn/src`; every literal `new ErrorReply("…")` in those trees (177 sites, distinct-text extraction); every `ErrorReply.<CONST>` reference in `src/test/groovy`, `dyn/test`, `src/test/java`, `segment_common`, `segmentweb`; every `.message ==` assertion in the test trees; `git log -S "!NOPERM!"`; `doc/bug_reviews/*` for error-text expectations.

Findings from the sweep, none of them covered by the plan:
- `ErrorReply.TARGET_KEY_BUSY` ("target key name is busy", `ErrorReply.java:164`) is not a Redis text; Redis' RESTORE busy answer is `-BUSYKEY Target key name already exists.` (`server.c:1876-1877`), and the plan lists `BUSYKEY` as "already aligned" while the RESTORE path uses `TARGET_KEY_BUSY`. Add it to the out-of-scope list with a reason (or align it).
- Inline texts that are already Redis-shaped and would be easy to include: "expansion too large" (Redis 7: "string exceeds maximum allowed size (proto-max-bulk-len)"), "could not decode requested zset member" (Redis: "zset member decoding failed"), "given number must be between 1 and 1024" (Redis: "bit offset is not an integer or out of range"). They are covered by the plan's blanket out-of-scope statement, but the plan's Finding 9 wording ("Everything else … explicitly out of scope") reads as if the rest had been checked against Redis.
- `doc/bug_reviews/bug_69_acl_module_review_round_1.md:165-170` records the expected Redis ACL replies; it should be listed under `Related:`.

#### Verdict

approvable after R1-N1, R1-N2, R1-N3, R1-N4, R1-N5, R1-N6, R1-N7, R1-N8, R1-N9, R1-N10, R1-N11 (R1-N12 … R1-N18 are Low/Info and may be handled in the same revision).

Round-1 note: the core idea and the raw-prefix mechanism are sound and verified; the blockers are mechanical (private ctor, wrong test names, stale anchors) plus two mapping rows that currently claim Redis fidelity they do not have.

## Revision 1 — Response to Round 1 (2026-11-16)

All Round-1 findings were re-verified against the current tree by agent 1 before acceptance. One sub-claim refuted with fresh evidence (see R1-N3 response).

### Header fixes
- `Related:` ground truth is **Redis 7.2.14** (`~/ws/redis/src/version.h`: `#define REDIS_VERSION "7.2.14"`). [R1-N5]
- `Related:` add `doc/bug_reviews/bug_69_acl_module_review_round_1.md` (records Redis ACL reply expectations). [blind-spot]
- Design docs 06/10/16 were read; none defines an error-text/prefix contract (06 only links ErrorReply.java at its line 20; 10/16 have zero error-text mentions), so the "do not change" boundary comes from the Current Code Findings code facts. [R1-N16]
- Open Review Questions closed: **keep `NOT_INTEGER` / `INVALID_INTEGER` / `NOT_INTEGER_OR_OUT_OF_RANGE` and `FORMAT` / `SYNTAX` as separate constants** (no ~130-site rename); texts may coincide. [R1-N11] Consequence recorded: after Task 2, `ErrorReply.NOT_INTEGER.equals(ErrorReply.INVALID_INTEGER)` is true and `FORMAT.equals(SYNTAX)` is true (`ErrorReply.equals()` at ErrorReply.java:208-218 compares message+flag); tests that distinguish these branches by `==` still pass but no longer discriminate — accepted, branches remain distinguishable via JaCoCo. [R1-N12]

### Current Code Findings corrections
- Finding 4: `NOT_STRING` sites are `SGroup.java:945` and `:959` (SET NX GET / SET XX GET non-string replies), not 943/957. [R1-N3]
- Finding 5 `RANGE_OUT_OF_INDEX` uses: `BGroup.java:1232`, `LGroup.java:587`, `LGroup.java:644`, **and `ZGroup.java:2585`** — REFUTATION of reviewer sub-claim "no use in ZGroup.java": `rg -n 'RANGE_OUT_OF_INDEX' src/main/java/io/velo/command/*.java` shows `ZGroup.java:2585: return ErrorReply.RANGE_OUT_OF_INDEX;`. Counts: NOT_INTEGER 86 uses, INVALID_INTEGER 44, FORMAT 204 (measured, main+groovy+dyn). [R1-N3, R1-N18]
- Finding 6 Redis citations corrected (all re-verified per-file): `"wrong number of arguments for '%s' command"` → networking.c:681 (also acl.c:3038, server.c:3803 as error-string builder); `"value is not an integer or out of range"` → object.c:901; `"value is not a valid float"` → object.c:834,868; `"value is out of range"` → object.c:917 + t_hash.c:1147; `"-NOPROTO unsupported protocol version"` → networking.c:3566; `"invalid expire time in '%s' command"` → networking.c:686; `"increment or decrement would overflow"` → t_string.c:612 + t_hash.c:658; `"value is NaN or Infinity"` → t_hash.c:680. Finding 9's `"index out of range"`/`"no such key"`/`"syntax error"` → server.c:1844-1851. [R1-N4]
- Finding 6 subcommand: Velo's current `PGroup.java:725` text `"unknown subcommand '" + subCmd + "'. Try PUBSUB HELP."` is **already** Redis' unknown-subcommand wording (server.c:3776). **PGroup change dropped from Task 3**; no test change. The Redis arity variant is networking.c:1170 ("unknown subcommand or wrong number of arguments for '%.128s'. Try %s HELP.", lowercase) — Velo's single error covers only the unknown-subcommand path, arity is answered separately by WRONG_NUMBER. [R1-N7]
- Finding 9 split [R1-N15]:
  - *Already Redis-aligned (anchors):* SYNTAX (server.c:1846-1847), NO_SUCH_KEY (server.c:1844-1845), INDEX_OUT_OF_RANGE (server.c:1850-1851), INCR_DECR_OVERFLOW (t_string.c:612, t_hash.c:658), NAN_OR_INFINITY (t_hash.c:680), NOT_INTEGER_OR_OUT_OF_RANGE, VALUE_NOT_POSITIVE, READONLY (server.c:1867-1868), BUSYKEY (server.c:1876-1877, used at RGroup.java:542,546), MIGRATE_KEY_NOT_EMPTY, CGroup.java:245/568/634/644, LGroup.java:557, EGroup.java:25 + SGroup.java:33 + HGroup.java:432 + GGroup.java:289 (networking.c:686 form), AGroup.java:166 ("Unknown category", acl.c style), FGroup.java:103, RGroup.java:562, PGroup.java:725 (server.c:3776 form).
  - *Velo-specific, deliberately untouched (anchors):* MGroup.java:649,738 (replication texts), BGroup.java:758,773,923,949,997,1222 (RedisBloom-module-style texts; "need not exactly same"), WGroup.java:85, SGroup.java:663,1763,1767,1772 (SORT/LIMIT — Redis clamps instead, sort.c:331-343), IGroup ingest texts, dyn Clusterx/Manage/Config texts, plus ErrorReply constants SERVER_STOPPED/DICT_MISSING/KEY_TOO_LONG/VALUE_TOO_LONG/LIST|HASH|SET|ZSET_SIZE_TO_LONG/SET|ZSET_MEMBER_LENGTH_TO_LONG/BF_*/CAPACITY_NOT_POSITIVE/BAD_DATA_FORMAT/DUMP_TYPE_NOT_SUPPORT/DEBUG_SUB_COMMAND_NOT_SUPPORT/NO_SUCH_FILE/INVALID_FILE/CANNOT_READ/CURSOR_NOT_MATCH (SGroup.java:578).
  - This split makes no claim that untouched texts were checked against Redis beyond what is anchored here.
- New finding: `TARGET_KEY_BUSY` (ErrorReply.java:164) is **unused** (`rg 'TARGET_KEY_BUSY' src dyn` → only the definition; RESTORE uses BUSYKEY at RGroup.java:542,546). Deleted in Task 3 as dead code (simple-removal exception per AGENTS.md — no new failing test required; relevant suites rerun). [blind-spot]

### Task 1 revision (mapping-table honesty + test names + commit) [R1-N2, R1-N6, R1-N8]
Mapping table gains a provenance column:
- WRONG_TYPE / AUTH_FAILED / NO_AUTH / CROSSSLOT / CLUSTERDOWN rows — Redis verbatim (server.c:1842, acl.c:1458, server.c:1869, cluster.c:7570, cluster.c:7581).
- ACL_PERMIT_LIMIT / ACL_PERMIT_KEY_LIMIT rows — **Velo-authored paraphrase**: Redis builds these dynamically per-request (`-NOPERM User %S has no permissions to run the '%S' command`, `-NOPERM No permissions to access a key` when non-verbose, acl.c:2655-2666 + acl.c:3943); Velo's constants are static (no user/cmd context at all call sites), so the texts stay `NOPERM this user has no permissions to run the command` / `NOPERM this user has no permissions to access the key` and are NOT claimed as Redis verbatim. Semantic note: Velo's CROSSSLOT is raised for keys spanning different *shards* (MultiWorkerServer.checkClusterSlot, MultiWorkerServer.java:394,404) while Redis' is same-node different-slots (cluster.c:7570); CLUSTER_SLOT_NOT_SET maps to Redis' unserved-slot case (MultiWorkerServer.java:366,371,385). [R1-N14]
- ACL_SETUSER_RULE_INVALID → `Error in ACL SETUSER modifier` (ERR prefix). Detail loss vs acl.c:2070 (`"...modifier '%s': %s"`) accepted — AGroup.java:470 returns a static constant. [R1-N13]
- Step 1: failing asserts go into existing method `io.velo.reply.RESPReplyTest.'test others'` (error asserts live at :45-62).
- Step 2: `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others"` — Expected: fail because e.g. `ErrorReply.WRONG_TYPE.message == 'wrong type'` != `'WRONGTYPE Operation against a key holding the wrong kind of value'`.
- Step 4: `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others" --tests "io.velo.MultiWorkerServerTest.test shutdown command is blocked by acl dangerous disallow" --tests "io.velo.MultiWorkerServerTest.test fast auth gate keeps quit available" --tests "io.velo.MultiWorkerServerTest.test pipeline auth refresh acl state" --tests "io.velo.MultiWorkerServerTest.test shutdown command requests server stop when acl permits" --tests "io.velo.RequestHandlerTest.test malformed http basic auth returns auth failed"` — Expected: pass.
- Step 6: `git add src/main/java/io/velo/reply/ErrorReply.java src/test/groovy/io/velo/reply/RESPReplyTest.groovy src/test/groovy/io/velo/MultiWorkerServerTest.groovy docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md && git commit -m "refactor: align the raw error reply texts with redis"`

### Task 2 revision (test names + FORMAT note + commit) [R1-N2, R1-N8, R1-N10]
- Note added to mapping: `FORMAT` covers both arity and bad-value failures at 204 sites (e.g. SGroup.java:583 scan arity, RequestHandler.java:357,409,455,477,553); Redis answers arity with "wrong number of arguments" (networking.c:681). Retexting FORMAT → "syntax error" is a **deliberate simplification, not a Redis alignment**; routing 204 sites to WRONG_NUMBER(cmd) is out of scope for this refactor.
- Step 1: asserts into `RESPReplyTest.'test others'`; MultiWorkerServerTest:150,151 updates land in method `test shutdown command requests server stop when acl permits` (:42).
- Step 2: `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others"` — Expected: fail because `FORMAT.message == 'format'` != `'syntax error'` and WRONG_NUMBER still emits leading `*`.
- Step 4: `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others" --tests "io.velo.MultiWorkerServerTest.test shutdown command requests server stop when acl permits" --tests "io.velo.command.ManageCommandTest"` — Expected: pass.
- Step 6: `git add src/main/java/io/velo/reply/ErrorReply.java src/test/groovy/io/velo/reply/RESPReplyTest.groovy src/test/groovy/io/velo/MultiWorkerServerTest.groovy && git commit -m "refactor: align the value error reply texts with redis"`

### Task 3 revision (NOPROTO constant, PGroup dropped, dead constants, test names, commit) [R1-N1, R1-N2, R1-N7]
- Files: SGroup.java (:579 cursor; :945,:959 NOT_STRING→WRONG_TYPE), HGroup.java (:646 → `ErrorReply.NOPROTO`), ErrorReply.java (add `public static final ErrorReply NOPROTO = new ErrorReply("NOPROTO unsupported protocol version", true);` — public constant, not the private ctor, so io.velo.command compiles; delete NOT_STRING (:85) and unused TARGET_KEY_BUSY (:164)). PGroup.java unchanged now.
- Step 1 failing tests: SGroupTest `'test scan'` (:200) add assert bad-cursor reply message == 'invalid cursor'; SGroupTest `'test set'` (:574) and `'test set nx get and xx get preserve flags'` (:738) NOT_STRING asserts → WRONG_TYPE; HGroupTest `'test hello'` (:249) add bad-protover assert: reply message == 'NOPROTO unsupported protocol version' and buffer() == "-NOPROTO unsupported protocol version\r\n". Groovy resolves `ErrorReply.NOT_STRING` at runtime, so before deletion the two asserts fail on value mismatch; deletion + assert rewrite land in the same commit. [R1-N17]
- Step 2: `./gradlew :test --tests "io.velo.command.SGroupTest.test scan" --tests "io.velo.command.SGroupTest.test set" --tests "io.velo.command.SGroupTest.test set nx get and xx get preserve flags" --tests "io.velo.command.HGroupTest.test hello"` — Expected: fail on old texts ('invalid scan cursor', NOT_STRING value, 'Server protover not support: 9').
- Step 4: same command — Expected: pass.
- Step 6: `git add src/main/java/io/velo/reply/ErrorReply.java src/main/java/io/velo/command/SGroup.java src/main/java/io/velo/command/HGroup.java src/test/groovy/io/velo/command/SGroupTest.groovy src/test/groovy/io/velo/command/HGroupTest.groovy && git commit -m "refactor: align the inline command error texts with redis"`

### Task 4 revision (regression filter) [R1-N9]
Step 1: `./gradlew :test --tests "io.velo.command.*GroupTest" --tests "io.velo.command.*CommandTest" --tests "io.velo.reply.*" --tests "io.velo.RequestHandlerTest" --tests "io.velo.MultiWorkerServerTest"` (covers dyn `io.velo.command.ClusterxCommandTest`/`ConfigCommandTest`/`ManageCommandTest` — package `io.velo.command`, dyn/test — plus `AGroupTest` for ACL_SETUSER_RULE_INVALID). Then append Validation status; commit plan-only remainder: `git commit -m "refactor: align error reply texts with redis"` if anything remains.

### Review Checklist addition
- `equals()` collisions (NOT_INTEGER≡INVALID_INTEGER, FORMAT≡SYNTAX) are expected after Task 2, not a regression.

### Plan Review — Round 2
Date: 2026-11-16
Reviewer: AI agent 2
Plan under review: docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md (revision 1)
Method: re-read the revised plan, then re-verified every revision claim with fresh reads/greps of `src/main/java`, `src/main/groovy`, `dyn/src`, `src/test/groovy`, `dyn/test` and `~/ws/redis/src` (no reliance on my round-1 notes)

#### Resolved verification (round 1 findings)

| Finding | Resolved? | Evidence (revised plan location) |
|---|---|---|
| R1-N1 private raw ctor | Yes | Revision 1 "Task 3 revision": adds `public static final ErrorReply NOPROTO = new ErrorReply("NOPROTO unsupported protocol version", true);` in `ErrorReply.java` and points `HGroup.java:646` at `ErrorReply.NOPROTO` — the constant is created inside `io.velo.reply`, so the private ctor stays private and `io.velo.command` compiles. No forward reference (NOPROTO is introduced and used in the same task). |
| R1-N2 test names | Yes | Revision 1 Tasks 1-3 now name existing methods, all confirmed by me: `RESPReplyTest.'test others'` (`RESPReplyTest.groovy:45`), `MultiWorkerServerTest.'test shutdown command requests server stop when acl permits'` (`:42`), `'test shutdown command is blocked by acl dangerous disallow'` (`:167`), `'test fast auth gate keeps quit available'` (`:326`), `'test pipeline auth refresh acl state'` (`:412`), `RequestHandlerTest.'test malformed http basic auth returns auth failed'` (`:116`), `SGroupTest.'test scan'` (`:200`), `'test set'` (`:574`), `'test set nx get and xx get preserve flags'` (`:738`), `HGroupTest.'test hello'` (`:249`), `io.velo.command.ManageCommandTest` (dyn/test). The two `NOT_STRING` asserts (`:644`, `:790`) are now inside the executed set. |
| R1-N3 stale anchors | Yes (with a correction against me, see below) | Revision 1 "Current Code Findings corrections": `NOT_STRING` → `SGroup.java:945,959` (confirmed by me: `rg -n NOT_STRING src/main/java` → 945, 959). |
| R1-N4 Redis citations | Yes | Revision 1 re-anchors every citation; I re-verified each one independently: `networking.c:681`, `acl.c:3038`, `server.c:3803`, `object.c:901`, `object.c:834,868`, `object.c:917`, `t_hash.c:1147`, `networking.c:3566`, `networking.c:686`, `t_string.c:612`, `t_hash.c:658`, `t_hash.c:680`, `server.c:1844-1851`, `server.c:3776`, `networking.c:1170`, `acl.c:1458`, `server.c:1869`, `cluster.c:7570`, `cluster.c:7581` — all correct now. |
| R1-N5 ground truth version | Yes (in the revision) | Revision 1 "Header fixes" states Redis 7.2.14 and adds `doc/bug_reviews/bug_69_acl_module_review_round_1.md` to `Related:`. See R2-N1 for the still-stale body header. |
| R1-N6 NOPERM provenance | Yes | Revision 1 "Task 1 revision" adds a provenance column; the two ACL rows are declared Velo-authored paraphrases with the Redis dynamic forms quoted (`acl.c:2655-2666`, `acl.c:3943`). |
| R1-N7 PUBSUB | Yes | Revision 1 drops the PGroup change; I confirmed `PGroup.java:725` already carries Redis' unknown-subcommand wording (`server.c:3776`) and that `PGroupTest.groovy:842` therefore needs no change. |
| R1-N8 per-task commit | Yes | Revision 1 adds Step 6 to Tasks 1-3 with one-line conventional messages. See R2-N4 (plan doc staged in Task 1 only). |
| R1-N9 Task 4 filter | Yes | Revision 1 Task 4 adds `--tests "io.velo.command.*CommandTest"` and `--tests "io.velo.MultiWorkerServerTest"`; the dyn tests (`dyn/test/io/velo/command/ClusterxCommandTest.groovy`, `ConfigCommandTest.groovy`, `ManageCommandTest.groovy`) are in package `io.velo.command` in the root test source set (`build.gradle:35` adds `dyn/test`), so the glob matches them. |
| R1-N10 FORMAT arity | Yes | Revision 1 "Task 2 revision" records the deliberate simplification; `SGroup.java:583` is indeed the scan arity check and `RequestHandler.java:357,409,455,477,553` are FORMAT returns (confirmed). |
| R1-N11 open questions | Yes | Revision 1 "Header fixes" closes both with the ruling I gave (keep the constants separate). |
| R1-N12 equals() collision | Accepted (and I verified it is harmless) | Revision 1 records the collision. I additionally checked for `!=` assertions on `ErrorReply` in `src/test/groovy` and `dyn/test` — there are none, so no existing assertion can flip; `ErrorReply.equals()` (`ErrorReply.java:208-218`) compares message + flag, so the collision is confined to `NOT_INTEGER≡INVALID_INTEGER` and `FORMAT≡SYNTAX`. |
| R1-N13 ACL SETUSER detail | Accepted | Revision 1 states the detail loss is accepted; `AGroup.java:470` returns the static constant (confirmed). |
| R1-N14 CROSSSLOT/CLUSTERDOWN | Yes | Revision 1 adds the shard-vs-slot note; `MultiWorkerServer.java:394,404` (cross-shard) and `:366,371,385` (slot not set, only reachable with `ConfForGlobal.clusterEnabled` at `:568`) — confirmed. |
| R1-N15 Finding 9 split | Yes | Revision 1 splits it with anchors; I re-verified the new anchors: `CGroup.java:245/568/634/644`, `LGroup.java:557`, `EGroup.java:25`, `SGroup.java:33`, `HGroup.java:432`, `GGroup.java:289`, `AGroup.java:166` ("Unknown category '" + category + "'"), `FGroup.java:103`, `RGroup.java:562`, `RGroup.java:542,546` (BUSYKEY), `BGroup.java:758,773,923,949,997,1222`, `WGroup.java:85`, `SGroup.java:663,1763,1767,1772` — all exist with the quoted texts. `sort.c:331-343` is the negative-LIMIT clamp in Redis (verified: `sort.c:332-334` `start = min(max(limit_start, 0), vectorlen)`), so the SORT texts are a behaviour difference, correctly out of scope. |
| R1-N16 design docs | Yes | Revision 1 records that docs 06/10/16 define no error-text contract; I confirmed the same greps. |
| R1-N17 contradictory step-2 wording | Yes | Revision 1 "Task 3 revision" states it once, correctly (Groovy resolves the property at runtime → value mismatch before deletion, `MissingPropertyException` after; deletion and assert rewrite land together). |
| R1-N18 counts | Yes (my numbers were off) | Revision 1 counts `NOT_INTEGER` 86, `INVALID_INTEGER` 44, `FORMAT` 204 (main+groovy+dyn, excluding the definitions). My round-1 numbers (87/45/205) counted the constant definitions in `ErrorReply.java`; the revision's are the correct ones. |
| Blind-spot: TARGET_KEY_BUSY | Yes | Revision 1 deletes it in Task 3 as dead code. I confirmed `rg -n TARGET_KEY_BUSY src dyn` matches only the definition at `ErrorReply.java:164`; RESTORE uses `ErrorReply.BUSYKEY` at `RGroup.java:542,546`. The `doc/bug_reviews/bug_85_rgroup_commands_review_round_1.md` mentions are comments, not code. |
| Blind-spot: ACL expectations doc | Yes | Added to `Related:`. |

**Correction against my own round-1 review:** R1-N3's sub-claim "there is no `RANGE_OUT_OF_INDEX` in `ZGroup.java`" was **wrong** — the planner is right, `src/main/java/io/velo/command/ZGroup.java:2585` is `return ErrorReply.RANGE_OUT_OF_INDEX;`. My round-1 grep was truncated by `head -12`. I withdraw that sub-claim; the rest of R1-N3 (the `NOT_STRING` line numbers) stands and is now fixed in the revision.

#### New findings

### R2-N1: The plan body was not updated, and it still contains the wrong test names, the wrong anchors and the old header
**Severity:** Medium (required before execution)
**Evidence:** the body at lines 1-174 is unchanged — `Status: Draft for review` (line 3), `Related: … Redis 8.x unstable` (line 4), Target Behavior's "Constants/inline texts listed in the mapping below match Redis texts" (line 18, false for the two NOPERM rows per R1-N6), `SGroup.java:578-579,943,957` (line 22), Findings 4/5/6/9 with the stale anchors (lines 40,41,42,45), the Task 1-3 steps with the non-existent test names (lines 76, 86, 108, 116, 140, 148), and the Review Checklist "No constant renamed/deleted except `NOT_STRING`" (line 169) which the revision now contradicts (TARGET_KEY_BUSY deleted, NOPROTO added).
The append-a-Revision mechanism is the sanctioned one, but nothing in the body points at it, so an implementer reading top-down runs `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test error reply"` (Gradle: "No tests found for given includes") and applies `SGroup.java:943,957`.
**Required:**
1. Add a banner right under the header: "**Revision 1 (2026-11-16) below is authoritative and supersedes the task steps, the mapping tables and the findings numbered 4/5/6/9 in the body.**" (one line; the body text then stays traceable).
2. Fix the header: `Status: Approved for execution` (at approval time) and `Related:` → `~/ws/redis/src (ground truth, Redis 7.2.14)`, plus `doc/bug_reviews/bug_69_acl_module_review_round_1.md`.
3. Fix the three false statements in the body so a reader who starts at the top cannot copy them: Finding 4's `SGroup.java:943,957` → `:945,959`; Finding 5's "ZGroup.java:1" → `ZGroup.java:2585` (plus `BGroup.java:1232`, `LGroup.java:587`, `LGroup.java:644`); the Target Behavior sentence about "match Redis texts" → "match Redis texts except the rows the mapping table marks as Velo-authored paraphrases".
4. Amend the Review Checklist: constants added/deleted = `NOPROTO` added, `NOT_STRING` and `TARGET_KEY_BUSY` deleted.

### R2-N2: The revision's PUBSUB sentence about arity is factually wrong
**Severity:** Low
**Evidence:** Revision 1 "Finding 6 subcommand" says "Velo's single error covers only the unknown-subcommand path, arity is answered separately by WRONG_NUMBER". But `PGroup.java:685-687` answers a PUBSUB arity error with `ErrorReply.FORMAT` (`if (data.length < 2) { return ErrorReply.FORMAT; }`), not `WRONG_NUMBER`. After Task 2 that becomes "syntax error", whereas Redis answers "wrong number of arguments for 'pubsub' command" (`networking.c:681`).
**Required:** correct the sentence ("arity is answered by `ErrorReply.FORMAT`, which becomes 'syntax error' after Task 2 — the same deliberate simplification recorded for FORMAT in Task 2"). No code change needed.

### R2-N3: Task 3's JaCoCo window misses the changed line `SGroup.java:579`
**Severity:** Low
**Evidence:** Task 3 Step 5 runs `python3 scripts/jacoco_cover.py io.velo.command.SGroup 930 970 --src`; the cursor-text change is at `SGroup.java:579` (the `INVALID_SCAN_CURSOR` constant), outside that range. Only the `NOT_STRING`→`WRONG_TYPE` lines (945, 959) fall inside.
**Required:** extend the check, e.g. `python3 scripts/jacoco_cover.py io.velo.command.SGroup 570 600 --src && python3 scripts/jacoco_cover.py io.velo.command.SGroup 930 970 --src && python3 scripts/jacoco_cover.py io.velo.command.HGroup 640 670 --src`, and state that `SGroupTest.'test scan'` exercises the `SGroup.java:597` branch.

### R2-N4: The plan doc is staged only in Task 1's commit
**Severity:** Info
**Evidence:** Revision 1 Task 1 Step 6 stages `docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md`, while Task 2/3 Step 6 do not; Task 4 Step 2 then says "commit plan-only remainder … if anything remains".
**Suggested:** either drop the plan doc from Task 1's `git add` (so the plan is committed once, in Task 4, together with the `## Validation status` section), or say explicitly in Task 4 that the plan doc lands with Task 1 and Task 4 commits only the appended validation status.

### R2-N5: The two NOPERM paraphrases could use Redis' static-compatible wording
**Severity:** Info
**Evidence:** Redis' non-verbose key-denial text is "No permissions to access a key" (`acl.c:2664`), which needs no user/cmd context and would fit a static constant; the plan keeps "NOPERM this user has no permissions to access the key". The command case has no static Redis equivalent ("User %S has no permissions to run the '%S' command" needs the user and command).
**Suggested:** optional polish — use `NOPERM No permissions to access a key` for the key case, keep the paraphrase for the command case, and label it as such.

### R2-N6: Task 2's mapping table has no provenance column
**Severity:** Info
**Evidence:** Task 1's table gained a provenance column in the revision; Task 2's table (body line 102) does not, so `NOT_SUPPORT` → "operation not supported" (Finding 5 itself says "no exact Redis text") and `RANGE_OUT_OF_INDEX` → "value is out of range" read as Redis-verbatim.
**Suggested:** give Task 2's table the same provenance column.

#### Blind-spot sweep (round 2)

Re-grepped after the revision: `RANGE_OUT_OF_INDEX`, `TARGET_KEY_BUSY`, `BUSYKEY`, `invalid expire time in`, `ErrorReply.NOT_STRING`, `ErrorReply.AUTH_FAILED`, `ErrorReply.SYNTAX`, `!= ErrorReply`, `--tests` names, `new ErrorReply(` literals, `ErrorReply.NOPROTO` collisions, `PGroup.java` arity paths, `HGroupTest.'test hello'` (the existing `hGroup.execute('hello 4')` case at `HGroupTest.groovy:270-272` is exactly the bad-protover path the revision's new assert targets — the revision should extend that existing case rather than add a separate one).

New observations from this sweep, none of them covered by the revision:
- `HGroupTest.'test hello'` already contains the bad-protover case (`hGroup.execute('hello 4')` → `reply instanceof ErrorReply`); the revision's "add bad-protover assert" should extend that case (it also proves the `ver` detail is dropped by design, matching Redis' `-NOPROTO unsupported protocol version`).
- `ErrorReply.equals()` also makes `FORMAT` equal `SYNTAX` after Task 2 — no test relies on inequality (verified), so no test change is needed; the revision's note covers it.
- `ErrorReply.NOPROTO` does not collide with any existing symbol (`rg NOPROTO src dyn` → no hits outside the plan doc).

#### Verdict

approvable after R2-N1, R2-N2 (R2-N3 … R2-N6 are Low/Info and can be folded into the same revision).

Round-2 note: the revision is technically sound — I re-verified every anchor, every Redis citation, every test-method name and the two dead-code claims, and I withdraw my one wrong round-1 sub-claim about `ZGroup.java`. What is left is document hygiene: the body still says `Status: Draft for review`, still names Redis 8.x, still carries the wrong `--tests` names and the wrong `NOT_STRING` line numbers, and contradicts the revision's own checklist. A one-line supersede banner plus those corrections make the plan executable; after that I approve it for execution.

## Revision 2 — Response to Round 2 (2026-11-16)

All round-2 findings verified before acceptance (PGroup.java:683 arity `return ErrorReply.FORMAT;` confirmed; HGroupTest `hello 4` case confirmed at :270-272). Accepted: R2-N1 (all four required body edits applied in-place above: supersede banner under header, header Related fixed to Redis 7.2.14 + bug_69 doc, Target Behavior paraphrase qualifier, Finding 4 anchors 945/959, Finding 5 anchors ZGroup.java:2585/BGroup.java:1232/LGroup.java:587,644 + object.c:917, Command Path Audit anchors + PGroup drop, Review Checklist constants line), R2-N2, R2-N3, R2-N4, R2-N5, R2-N6, and the round-2 blind spots.

- **R2-N2 correction (supersedes Revision 1's PUBSUB sentence):** Velo's PUBSUB arity error is `ErrorReply.FORMAT` (PGroup.java:683, `if (data.length < 2)`), not WRONG_NUMBER; after Task 2 it becomes "syntax error" — the same deliberate simplification recorded for FORMAT in the Task 2 revision. No code change.
- **R2-N3 (Task 3 JaCoCo):** Step 5 becomes `python3 scripts/jacoco_cover.py io.velo.command.SGroup 570 600 --src && python3 scripts/jacoco_cover.py io.velo.command.SGroup 930 970 --src && python3 scripts/jacoco_cover.py io.velo.command.HGroup 640 670 --src`; `SGroupTest.'test scan'` must exercise the SGroup.java:597 `INVALID_SCAN_CURSOR` return branch.
- **R2-N4 (plan doc staging):** plan doc dropped from Task 1's `git add` (Task 1 Step 6 stages only ErrorReply.java, RESPReplyTest.groovy, MultiWorkerServerTest.groovy). The plan doc (with `## Validation status`) is committed once, in Task 4.
- **R2-N5 accepted (mapping polish, supersedes Revision 1's key row):** `ACL_PERMIT_KEY_LIMIT` → `NOPERM No permissions to access a key` (Redis' static-compatible non-verbose wording, acl.c:2664 — needs no user/key context). `ACL_PERMIT_LIMIT` stays the Velo paraphrase `NOPERM this user has no permissions to run the command` (Redis' command form needs user+cmd names, acl.c:2659-2660). Task 1's MultiWorkerServerTest:228,479 expected wire text becomes `'-NOPERM this user has no permissions to run the command\r\n'` (unchanged from Revision 1; the key-limit constant has no raw-text test).
- **R2-N6 (Task 2 provenance column), final mapping:**

| Constant | New message | Provenance |
|---|---|---|
| NOT_INTEGER | `value is not an integer or out of range` | Redis verbatim (object.c:901) |
| INVALID_INTEGER | `value is not an integer or out of range` | Redis verbatim (object.c:901) |
| NOT_FLOAT | `value is not a valid float` | Redis verbatim (object.c:834,868) |
| FORMAT | `syntax error` | Redis verbatim text (server.c:1846-1847) but deliberate simplification for mixed arity+bad-value sites — NOT a strict Redis alignment (R1-N10 note) |
| RANGE_OUT_OF_INDEX | `value is out of range` | Redis verbatim (object.c:917, t_hash.c:1147) |
| NOT_SUPPORT | `operation not supported` | Velo-authored (no Redis counterpart) |
| WRONG_NUMBER(cmd) | `wrong number of arguments for '<cmd>' command` | Redis verbatim (networking.c:681), stray `*` dropped |

- **Round-2 blind spot (Task 3 Step 1 amendment):** HGroupTest `'test hello'` already exercises the bad-protover path (`hGroup.execute('hello 4')` at :270-272 asserting `reply instanceof ErrorReply`) — strengthen that existing case with `(reply as ErrorReply).message == 'NOPROTO unsupported protocol version'` and `ErrorReply.NOPROTO.buffer().asArray() == "-NOPROTO unsupported protocol version\r\n".bytes` instead of adding a separate case.

No production-code decisions changed in this revision; all changes are documentation of the already-agreed mapping.

### Plan Review — Round 3
Date: 2026-11-16
Reviewer: AI agent 2
Plan under review: docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md (revision 2)
Method: re-verified every Revision 2 claim with fresh reads/greps of the tree and `~/ws/redis/src`, then re-read the body to confirm the R2-N1 edits landed

#### Resolved verification (round 2 findings)

| Finding | Resolved? | Evidence (revised plan location) |
|---|---|---|
| R2-N1 body consistency | Yes | Body now has the supersede banner (line 7), `Related:` fixed to Redis 7.2.14 + `doc/bug_reviews/bug_69_acl_module_review_round_1.md` (line 4), the Target Behavior qualifier (line 19), Finding 4 anchors `SGroup.java:945,959` (line 41), Finding 5 anchors `ZGroup.java:2585, BGroup.java:1232, LGroup.java:587,644` + `object.c:917` (line 42), the Command Path Audit anchors and the PGroup drop (line 23), and the Review Checklist constants line (line 170). I re-verified every one of those anchors against the tree. |
| R2-N2 PUBSUB arity | Yes | Revision 2 corrects Revision 1: PUBSUB arity is `ErrorReply.FORMAT` at `PGroup.java:683` (`if (data.length < 2)` at `:682`) — I confirmed the exact lines. No code change. |
| R2-N3 JaCoCo window | Yes | Revision 2 Task 3 Step 5 now covers `SGroup 570 600` (the `:579` cursor text and the `:597` `INVALID_SCAN_CURSOR` return) plus `SGroup 930 970` (`:945`, `:959`) and `HGroup 640 670` (`:646`). `SGroup.java:597` is indeed `return INVALID_SCAN_CURSOR;` and is reachable from `SGroupTest.'test scan'`. |
| R2-N4 plan doc staging | Yes | Revision 2 drops the plan doc from Task 1's `git add`; the doc is committed once in Task 4 together with `## Validation status`. |
| R2-N5 NOPERM key wording | Yes | Revision 2 sets `ACL_PERMIT_KEY_LIMIT` → `NOPERM No permissions to access a key` (Redis' non-verbose wording, `acl.c:2664` — confirmed in the ground truth) and keeps the command case as a labelled paraphrase. I confirmed no test asserts the key-limit wire text (`rg 'no permissions|ACL_PERMIT_KEY_LIMIT' src/test/groovy dyn/test` → no hits), so only `MultiWorkerServerTest:228,479` need the command-case text — exactly as the revision states. |
| R2-N6 Task 2 provenance | Yes | Revision 2 adds the provenance column; I re-verified each citation: `object.c:901`, `object.c:834,868`, `server.c:1846-1847`, `object.c:917` + `t_hash.c:1147`, `networking.c:681`. |
| Round-2 blind spot (HGroupTest) | Yes | Revision 2 strengthens the existing case instead of adding one: `HGroupTest.groovy:271` is `reply = hGroup.execute('hello 4')` inside `'test hello'` (`:249`) — I confirmed both the method and the line. (There is a second `hello 4` assertion at `HGroupTest.groovy:107` in `'test handle - hello and zzz'` (`:101`); it only asserts `instanceof ErrorReply` and stays green.) |

#### Body edits applied by the reviewer (post-approval corrections, each verified)

These were the only remaining stale statements in the body; they are outside the banner's scope (Findings 1/6/8, Architecture, Open Review Question), so I corrected them in place rather than opening another round:

1. Line 3 — `Status:` → `Approved for execution (round 3, 2026-11-16)`.
2. Line 10 (Architecture) — "plus 4 inline `new ErrorReply(...)` sites" → the actual edit surface: one inline text (`SGroup.java:579`), two constant swaps (`SGroup.java:945,959`), one constant return (`HGroup.java:646` → `ErrorReply.NOPROTO`). Verified by `rg -n 'new ErrorReply\(' SGroup.java HGroup.java PGroup.java`.
3. Line 27 (Open Review Question) — marked "Both questions were closed in Revision 1", with the decision restated.
4. Line 32 (Finding 1) — "~240 times" → "204 times (measured)".
5. Line 43 (Finding 6) — the PGroup clause now states that Velo's text is already Redis' unknown-subcommand wording and that Revision 1 drops the change (previously it still proposed the change).
6. Line 45 (Finding 8) — `PGroupTest.groovy:842` removed from the "must move" list, with the note that Revision 1 dropped the PGroup change.
7. Lines 39, 41, 43, 46 — the stale Redis file attributions corrected to the ground truth (`acl.c:3038`, `server.c:3803`, `networking.c:681`, `object.c:901`, `object.c:834,868`, `networking.c:3566`, `t_string.c:612`+`t_hash.c:658`, `t_hash.c:680`, `networking.c:686`).

#### New findings

None. I found no new defect in Revision 2 and no residual blocker in the body.

Re-swept for round 3: every `--tests` filter name against the test sources; `ErrorReply.NOPROTO` symbol collisions; the ACL key-limit wire text in the tests; `hello 4` cases; the PUBSUB arity path; the inline `new ErrorReply(...)` sites in the three touched command files; and the `Status:`/banner placement.

#### Verdict

**APPROVED for execution.**

The plan is executable as written, with Revisions 1 and 2 authoritative over the body (banner at line 7). The executor must follow the Revision 1/2 task steps, not the body steps — i.e. use `RESPReplyTest.'test others'`, the four named `MultiWorkerServerTest` methods, `SGroupTest.'test set'` and `'test set nx get and xx get preserve flags'`, `HGroupTest.'test hello'`, add `ErrorReply.NOPROTO` (public constant), delete `NOT_STRING` and the unused `TARGET_KEY_BUSY`, keep `PGroup.java:725` unchanged, and use the Revision 2 mapping tables (Task 1's `ACL_PERMIT_KEY_LIMIT` → `NOPERM No permissions to access a key`; Task 2's provenance column).

Reminders for the executor: one commit per task with the Revision 1 commit messages, the plan doc (with `## Validation status`) committed in Task 4, and JaCoCo confirmation on the changed lines per task — the acceptance of the `equals()` collisions (`NOT_INTEGER≡INVALID_INTEGER`, `FORMAT≡SYNTAX`) is pre-approved and is not a regression.

## Validation status (2026-11-16)

- Task 1: commit `9bf17626` — RESPReplyTest.test others + 4 MultiWorkerServerTest methods + RequestHandlerTest.test malformed http basic auth returns auth failed: 6/6 green; red step verified first (WRONG_TYPE old text); JaCoCo: all 8 changed constants covered, both rawErrorPrefix wire branches asserted.
- Task 2: commit `fc80f509` — RESPReplyTest.test others + MultiWorkerServerTest.test shutdown command requests server stop when acl permits + ManageCommandTest (11/11 dyn): green; red step verified first (WRONG_NUMBER leading `*`); JaCoCo: ErrorReply lines 14-180 48/48 covered.
- Task 3: commit `2082f95e` — SGroupTest.test scan/test set/test set nx get and xx get preserve flags + HGroupTest.test hello: 4/4 green; red step verified first (old cursor text, NOT_STRING value mismatch, NOPROTO MissingPropertyException); JaCoCo: SGroup 570-600 (:579 covered, :597 all 6 branches), SGroup 930-970 (:945/:959 covered), HGroup 640-670 21/21 covered.
- Task 4 (regression): `./gradlew :test --tests "io.velo.command.*GroupTest" --tests "io.velo.command.*CommandTest" --tests "io.velo.reply.*" --tests "io.velo.RequestHandlerTest" --tests "io.velo.MultiWorkerServerTest"` — 602 tests completed, 5 failed. All 5 failures (MultiWorkerServerTest pipeline readonly/connection-state/barrier trio, mock inject and handle, cluster slot cross shards) reproduce identically on baseline main commit `8e811e5c` — pre-existing on main, not regressions. 597/602 green.
- equals() collisions NOT_INTEGER≡INVALID_INTEGER and FORMAT≡SYNTAX observed as pre-approved (plan Review Checklist).

## Commit Review (Stage 4, 2026-11-16)

Date: 2026-11-16
Reviewer: AI agent 2 (commit reviewer)
Branch under review: `refactor/error-reply-texts`, base `main` @ 8e811e5c
Commits under review: 9bf17626, fc80f509, 2082f95e, 70ed456b
Method: read-only git plumbing on the checked-out branch (`git show <sha>`, `git diff main..refactor/error-reply-texts -- <path>`, `git log --oneline`), independent re-reads of `src/main/java/io/velo/reply/ErrorReply.java`, `src/main/java/io/velo/command/{SGroup,HGroup,PGroup}.java` and the four touched test files, a stale-text sweep over `src/` + `dyn/`, one focused green check (`./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others"`) and the JaCoCo report it produced. No production code or test was modified by this review; the only write was this section.

### 1. Per-commit verification

| # | Commit | Plan task | Revision-1 `git add` list (plan :359/:366/:373) | Actual `git show --stat` | Verdict |
|---|---|---|---|---|---|
| 1 | 9bf17626 `refactor: align the raw error reply texts with redis` | Task 1 | ErrorReply.java, RESPReplyTest.groovy, MultiWorkerServerTest.groovy | identical 3 files (43+/19-) | matches — no scope creep |
| 2 | fc80f509 `refactor: align the value error reply texts with redis` | Task 2 | ErrorReply.java, RESPReplyTest.groovy, MultiWorkerServerTest.groovy | identical 3 files (27+/15-) | matches — no scope creep |
| 3 | 2082f95e `refactor: align the inline command error texts with redis` | Task 3 | ErrorReply.java, SGroup.java, HGroup.java, SGroupTest.groovy, HGroupTest.groovy | identical 5 files (13+/13-) | matches — no scope creep |
| 4 | 70ed456b `refactor: align error reply texts with redis` | Task 4 | plan doc only | `docs/plans/2026-11-16-error-reply-texts-redis-alignment-plan.md` only (540 insertions) | matches — plan doc committed once, with the Validation status (R2-N4) |

Commit messages are byte-identical to the Revision-1 Step 6 messages (plan :359, :366, :373) and the Revision-1 Task-4 message (plan :376). Whole-branch file set (`git diff main..refactor/error-reply-texts --stat`): plan doc + `ErrorReply.java`, `SGroup.java`, `HGroup.java`, `MultiWorkerServerTest.groovy`, `RESPReplyTest.groovy`, `SGroupTest.groovy`, `HGroupTest.groovy` — nothing outside the plan's file list. `git diff main..refactor/error-reply-texts -- src/main/java/io/velo/command/PGroup.java` is **empty** (Revision 1 dropped the PUBSUB change) ✓.

#### Task 1 (9bf17626) — constants vs the Revision-1/2 mapping (branch `ErrorReply.java`)

| Constant | Plan text (Rev 1 + Rev 2 override) | Branch line | Prefix |
|---|---|---|---|
| WRONG_TYPE | `WRONGTYPE Operation against a key holding the wrong kind of value` | :100-101 | raw ✓ |
| AUTH_FAILED | `WRONGPASS invalid username-password pair or user is disabled.` | :49-50 | raw ✓ |
| NO_AUTH | `NOAUTH Authentication required.` | :53 | raw ✓ |
| ACL_PERMIT_LIMIT | `NOPERM this user has no permissions to run the command` (Velo paraphrase, plan :473) | :56-57 | raw ✓ |
| ACL_PERMIT_KEY_LIMIT | `NOPERM No permissions to access a key` (**Revision 2 override** of the body row, plan :473) | :60-61 | raw ✓ |
| CLUSTER_SLOT_CROSS_SHARDS | `CROSSSLOT Keys in request don't hash to the same slot` | :156-157 | raw ✓ |
| CLUSTER_SLOT_NOT_SET | `CLUSTERDOWN Hash slot not served` | :160-161 | raw ✓ |
| ACL_SETUSER_RULE_INVALID | `Error in ACL SETUSER modifier` (ERR prefix kept, `!...!` dropped) | :64 | ERR ✓ |

Task 1 tests: `RESPReplyTest.groovy:78-95` adds `.message` + wire-form asserts for all 8 constants (e.g. `:88-89` `-NOPERM this user has no permissions to run the command\r\n`, `:91-92` `-NOPERM No permissions to access a key\r\n`); `MultiWorkerServerTest.groovy:228` → `'-NOPERM this user has no permissions to run the command\r\n'`, `:399-400` → `'-NOAUTH Authentication required.\r\n'` (×2), `:479` → `'+OK\r\n-NOPERM this user has no permissions to run the command\r\n'` = exactly the 4 planned wire-text updates. No plan doc in this commit ✓.

#### Task 2 (fc80f509) — constants vs the Revision-2 provenance table (plan :476-484)

| Constant | Plan text | Branch line | ✓ |
|---|---|---|---|
| NOT_INTEGER | `value is not an integer or out of range` | :76 | ✓ |
| INVALID_INTEGER | `value is not an integer or out of range` | :88 | ✓ |
| NOT_FLOAT | `value is not a valid float` | :79 | ✓ |
| FORMAT | `syntax error` | :14 | ✓ |
| RANGE_OUT_OF_INDEX | `value is out of range` | :94 | ✓ |
| NOT_SUPPORT | `operation not supported` | :138 | ✓ |
| WRONG_NUMBER(cmd) | `wrong number of arguments for '<cmd>' command` — stray `*` dropped | :20-22 | ✓ |

Task 2 tests: `RESPReplyTest.groovy:58-70` (`.message` + `-ERR …\r\n` wire form for each, incl. `:59` `"-ERR wrong number of arguments for 'test' command\r\n"`), `MultiWorkerServerTest.groovy:150-151` → `'-ERR syntax error\r\n'` (×2). No plan doc in this commit ✓.

#### Task 3 (2082f95e) — inline sites, constants, tests

| Planned item (plan :369-373) | Branch evidence | ✓ |
|---|---|---|
| `ErrorReply.NOPROTO` added, public constant, raw | `ErrorReply.java:169-170` `new ErrorReply("NOPROTO unsupported protocol version", true)` — created inside `io.velo.reply`, so the private 2-arg ctor stays private and `io.velo.command` compiles (R1-N1 satisfied) | ✓ |
| `NOT_STRING` deleted | removed at `ErrorReply.java` (was :87-88) | ✓ |
| unused `TARGET_KEY_BUSY` deleted | removed (was :164) | ✓ |
| `SGroup.java:579` cursor text → `invalid cursor` | `SGroup.java:579` | ✓ |
| `SGroup.java:945,:959` NOT_STRING → WRONG_TYPE | `SGroup.java:945`, `:959` | ✓ |
| `HGroup.java:646` → `ErrorReply.NOPROTO` | `HGroup.java:646` | ✓ |
| `PGroup.java` unchanged | diff empty | ✓ |
| tests only in SGroupTest / HGroupTest | `SGroupTest.groovy:415` (`'invalid cursor'` assert inside `'test scan'` :200), `:645` (`'test set'` :575), `:791` (`'test set nx get and xx get preserve flags'` :739); `HGroupTest.groovy:271-274` (`'test hello'` :249) | ✓ |

No forward reference: `NOPROTO` is introduced and consumed in the same commit; the `NOT_STRING` deletion and both call-site swaps land together (R1-N17).

#### Task 4 (70ed456b) — plan doc structure

Body (:1) → `## Review Feedback` (:179) → `### Plan Review — Round 1` (:183) → `## Revision 1` (:330) → `### Plan Review — Round 2` (:381) → `## Revision 2` (:466) → `### Plan Review — Round 3` (:490) → `## Validation status (2026-11-16)` (:534) — order intact, `## Validation status` appended last ✓. Header still reads `Status: Approved for execution (round 3, 2026-11-16)` (:3) ✓.

### 2. Review Checklist verification (plan :169-177 + Revision-1 addition :379)

| Checklist item | Result | Evidence |
|---|---|---|
| Raw constants are `-WRONGTYPE …`, no `-ERR` double prefix | PASS | `ErrorReply.java:230` `((rawErrorPrefix ? "-" : "-ERR ") + message + "\r\n")`; `RESPReplyTest.groovy:79-92` asserts the wire form for all 7 raw constants; `buffer()` line 230 is reported "All 2 branches covered" in the current JaCoCo report |
| No constant renamed; deleted = `NOT_STRING`, `TARGET_KEY_BUSY`; added = `NOPROTO`; no semantic call-site move | PASS | `git diff main..branch -- src/main/java/io/velo/reply/ErrorReply.java` shows only: the 7 Task-1 texts, the 6 Task-2 texts + `WRONG_NUMBER`, `NOT_STRING` removed, `TARGET_KEY_BUSY` removed, `NOPROTO` added. `INVALID_INTEGER` was relocated in the file but not renamed. The two `NOT_STRING` sites are wrong-type reads on `SET NX GET` / `SET XX GET` → `WRONG_TYPE` ✓ |
| No text changes outside the mapping tables; Finding 10 items untouched | PASS | the branch touches 4 production files only (ErrorReply/SGroup/HGroup + plan doc); `PGroup.java` diff empty; `SERVER_STOPPED`, `DICT_MISSING`, `KEY_TOO_LONG`, `VALUE_TOO_LONG`, `*_SIZE_TO_LONG`, `BF_*`, `CAPACITY_NOT_POSITIVE`, `BAD_DATA_FORMAT`, `DUMP_TYPE_NOT_SUPPORT`, `DEBUG_SUB_COMMAND_NOT_SUPPORT`, `NO_SUCH_FILE`, `INVALID_FILE`, `CANNOT_READ`, `CURSOR_NOT_MATCH` (SGroup.java:578), `SGroup.java:663`, SORT texts, `MGroup.java:649,738`, dyn texts — all unchanged |
| No new `getMessage()`-based dispatch | PASS | `rg 'getMessage\(\)\s*\.\s*(equals\|startsWith\|contains)' src dyn` → no hits; every `getMessage()` in `src/main` + `dyn/src` is an exception-message or log call (e.g. `RequestHandler.java:351`, `MGroup.java:649`) and none was added by this branch (the diff contains no `getMessage()`) |
| `WRONG_NUMBER` no longer starts with `*` | PASS | `ErrorReply.java:21` `new ErrorReply("wrong number of arguments for '" + cmd + "' command")`; asserted at `RESPReplyTest.groovy:58-59` |
| Tests assert the new texts; no test left asserting the old ones | PASS with one cosmetic leftover | `rg` over `src/` + `dyn/` finds no remaining assertion on `'format'`, `'no auth'`, `'!NOPERM!'`, `'invalid scan cursor'`, `'Server protover not support'`, `'range out of index'`, `'not integer'`, `'not float'`, `'target key name is busy'`; the only hits are comments/javadoc/labels — see S4-N1 |
| One task = one commit, each green independently; JaCoCo inspected per task | PASS (with the caveat in S4-N3) | 4 commits, one per task; each commit is self-consistent (Task 1 left `'-ERR format'` untouched because `FORMAT` was still `'format'`; Task 2 changed the constant and its 2 asserts together); `## Validation status` records the per-task green runs and JaCoCo windows |
| `equals()` collisions `NOT_INTEGER≡INVALID_INTEGER`, `FORMAT≡SYNTAX` pre-approved | PASS | `ErrorReply.equals()` compares message + `rawErrorPrefix` (`ErrorReply.java:212-221`); the collisions are recorded in the Validation status as pre-approved; no `!=` assertion on `ErrorReply` exists in the test trees, so no existing assertion can flip |

### 3. Stale-text sweep (`src/` + `dyn/`, code and tests)

`rg -e 'auth failed' -e 'no auth' -e 'wrong type' -e '!NOPERM!' -e 'invalid scan cursor' -e 'Server protover not support' -e 'range out of index' -e '"format"' -e 'not integer' -e 'not float' -e 'target key name is busy' -e 'NOT_STRING'` → the only hits are (a) comments/javadoc that describe behaviour rather than assert it (`SGroup.java:331`, `GGroup.java:455`, `ErrorReply.java:93,99,103`), (b) Spock `when:` labels describing the scenario (`DGroupTest.groovy:101,121,471`, `BGroupTest.groovy:1454,1469,1479`, `PGroupTest.groovy:250,285,354,…`, `LGroupTest.groovy:1316,…`, `ZGroupTest.groovy:2806,…`, `HGroupTest.groovy:1798,2418`, `CGroupTest.groovy:874`, `IGroupTest.groovy:188`), (c) the method name `RequestHandlerTest.groovy:116` `'test malformed http basic auth returns auth failed'`, and (d) one leftover naming the deleted constant — see S4-N1. `rg 'TARGET_KEY_BUSY'` → no hits. `rg 'ErrorReply' segment_common/src segmentweb/src` → no cross-module references.

### 4. Validation status accuracy (plan :534-540)

| Claim | Result | Evidence |
|---|---|---|
| Task 1 = `9bf17626`, Task 2 = `fc80f509`, Task 3 = `2082f95e`, with the stated messages | PASS | `git log --oneline main..refactor/error-reply-texts` lists exactly these 4 hashes/messages; `git show --stat` matches the planned file lists |
| Named test methods exist | PASS | `RESPReplyTest.'test others'` (:45), `MultiWorkerServerTest.'test shutdown command requests server stop when acl permits'` (:42), `'test shutdown command is blocked by acl dangerous disallow'` (:167), `'test fast auth gate keeps quit available'` (:326), `'test pipeline auth refresh acl state'` (:412), `RequestHandlerTest.'test malformed http basic auth returns auth failed'` (:116), `SGroupTest.'test scan'` (:200) / `'test set'` (:575) / `'test set nx get and xx get preserve flags'` (:739), `HGroupTest.'test hello'` (:249), `dyn/test/io/velo/command/ManageCommandTest.groovy` (11 `def` methods → the claimed "11/11 dyn") — all present; the asserts the Validation status claims were exercised sit inside exactly these methods (SGroupTest :415/:645/:791, HGroupTest :271-274) |
| Task 1 "6/6 green" | consistent | 1 (RESPReplyTest) + 4 (MultiWorkerServerTest) + 1 (RequestHandlerTest) = 6 ✓ |
| Task 3 JaCoCo "HGroup 640-670 21/21 covered" | consistent | the changed HGroup line is :646 (`return ErrorReply.NOPROTO;`) inside the 640-670 window ✓ |
| Task 4 regression: 602 tests, 5 failed, "reproduce identically on baseline main 8e811e5c" | NOT independently re-verified | the full suite was out of scope for this review. Indirect evidence supports it: the 5 named failing methods (`MultiWorkerServerTest` pipeline readonly / connection-state / barrier trio, `'test mock inject and handle'`, `'test cluster slot cross shards'`) contain **no** assertion on any changed text (rg over `MultiWorkerServerTest.groovy:489-1000` and `:1548-1646` for `ERR|NOAUTH|NOPERM|WRONGTYPE|syntax|format|no auth` → no hits; the cross-shards test asserts a `MOVED …` message, `ErrorReply.clusterMoved` :178-180, unchanged) and the branch touches no production code those paths execute |
| Task 2 JaCoCo "ErrorReply lines 14-180 48/48 covered" | NOT independently re-verifiable | the current JaCoCo report reflects only the single-class run I performed (see below); historical per-task coverage numbers can only be reproduced by re-running each task's suite |

Fast green check on the branch working tree: `./gradlew :test --tests "io.velo.reply.RESPReplyTest.test others"` → **BUILD SUCCESSFUL**; `build/test-results/test/TEST-io.velo.reply.RESPReplyTest.xml` → `tests="1" skipped="0" failures="0" errors="0"`. (The first invocation aborted with `Java heap space` in `:compileTestGroovy` — a daemon-memory hiccup of the sandbox, not a code failure; the identical command re-run was green.) The JaCoCo report produced by that run shows `ErrorReply.java` declaration lines 14, 21, 49, 53, 56, 60, 64, 76, 79, 88, 94, 100, 138, 156, 160, 170 all covered and `buffer()` :230 "All 2 branches covered" (both the raw and the `-ERR ` paths executed) — i.e. the changed constant lines and the raw-prefix wire branch are executed, as the checklist requires.

### 5. Findings

#### S4-N1: A test label still names the deleted `NOT_STRING` constant
**Severity:** Low
**Evidence:** `src/test/groovy/io/velo/command/SGroupTest.groovy:784` — `when: 'NX GET on existing non-string key returns NOT_STRING error'`, while the assertion right below it (`:791`) now asserts `reply == ErrorReply.WRONG_TYPE`. `ErrorReply.NOT_STRING` was deleted in 2082f95e, so the label points at a symbol that no longer exists; it is the only remaining `NOT_STRING` occurrence in `src/` + `dyn/` (all other sweep hits are unrelated scenario labels).
**Impact:** no behavioural impact (a Spock `when:` description), but it contradicts the plan's stale-text rule (Review Checklist :176) and misleads a reader of the test.
**Required:** reword the label to `... returns WRONG_TYPE error` in a follow-up commit.

#### S4-N2: The `FORMAT` javadoc claims redis alignment that the plan explicitly qualified
**Severity:** Info
**Evidence:** `src/main/java/io/velo/reply/ErrorReply.java:13-14` — `/** Error reply for bad arguments structure, the text is aligned with redis. */ public static final ErrorReply FORMAT = new ErrorReply("syntax error");`, whereas the plan's Revision-2 provenance table (plan :481) records this row as "Redis verbatim text (server.c:1846-1847) but deliberate simplification for mixed arity+bad-value sites — NOT a strict Redis alignment" (R1-N10).
**Impact:** documentation only; the text is byte-identical to Redis' `syntax error`, so the wording is defensible, but the javadoc drops the "not a strict alignment" qualifier that the plan attached to this constant (the other retexted constants are unconditional Redis texts, so the qualifier only matters here).
**Required:** none for correctness; optionally append "(the usage is a deliberate simplification for mixed arity sites)" to the javadoc.

#### S4-N3: The per-task JaCoCo numbers and the baseline reproduction of the 5 regressions are historical claims that Stage 4 could not re-verify
**Severity:** Info
**Evidence:** plan :537-539 claims "ErrorReply lines 14-180 48/48 covered", "SGroup 570-600 (:597 all 6 branches)", "HGroup 640-670 21/21 covered" and "602 tests completed, 5 failed … reproduce identically on baseline main commit 8e811e5c". The current JaCoCo report was regenerated by my single-class run and therefore reflects only `RESPReplyTest` (SGroup/HGroup show as uncovered), and the full suite was out of scope for this review.
**Impact:** the claims are internally consistent and supported indirectly (the 5 failing methods assert no changed text, see §4), but they are not independently reproducible from the artefacts left in the tree.
**Required:** none; if a future round needs the numbers, re-run each task's suite and re-check the windows with `scripts/jacoco_cover.py`.

#### S4-N4: The plan body still carries the pre-Revision-2 key-limit text
**Severity:** Info
**Evidence:** the body mapping table (plan :69) lists `ACL_PERMIT_KEY_LIMIT → NOPERM this user has no permissions to access the key`, while Revision 2 (plan :473) overrides it with `NOPERM No permissions to access a key`. The implemented constant (`ErrorReply.java:60-61`) follows Revision 2 — correct — but the body row was never corrected in place, unlike the body edits listed under Round 3 (plan :508-518).
**Impact:** none for the code; a reader consulting the body table instead of the revisions gets the superseded text.
**Required:** optionally correct plan :69 to the Revision-2 text for future readers.

### 6. Verdict

All four commits match what the plan promised (Revisions 1+2 authoritative): task boundaries respected, no scope creep, no missing piece, no forward reference, and every planned wire text and constant change present verbatim. The only open items are cosmetic/documentation (S4-N1 stale test label, S4-N2 javadoc qualifier, S4-N4 body row) and two historical numbers that Stage 4 could not re-derive (S4-N3).

executed with concerns: S4-N1 (stale `NOT_STRING` label in `SGroupTest.groovy:784` — follow-up one-liner), S4-N2/S4-N4 (doc wording only), S4-N3 (per-task JaCoCo/baseline numbers not re-verifiable in Stage 4)
