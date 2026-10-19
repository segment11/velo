# Bug 73 — CGroup Commands Review (Round 1)

**Module:** `io.velo.command.CGroup` (client / cluster / clusterx / command / config / copy)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2 (round 1 verification complete)
**Status:** 7 findings fixed and accepted; A1 closed (`94db1fa4`); R1 follow-up `9f06178c` REJECTED in re-review round 2 (closes on a slot-worker thread)
**Review branch:** `review/cgroup`
**Reviewed commit:** `0accc625dd94fb52917d08664ff8bafb39f2a203`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/CGroup.java — static block/ctor/parseSlots/handle (22-136), client() (145-279), clientList() (286-304), ClientKillFilter/parseClientKillFilter (306-394), isSupportedClientKillType (405-409), clientType (421-431), matchesClientKillFilter (436-492), killClientsMatching (498-532), clusterx/command/config/copy (536-625)` — full file
- `src/main/java/io/velo/VeloUserDataInSocket.java — ReplyMode.from() (142-158), clientId (16-30), clientName (223-241)` — full file
- `src/main/java/io/velo/BaseCommand.java — getExpireAt/getCv (713-776), getValueBytesByCv (791-819), setCv (950-977)` — read ranges
- `src/main/java/io/velo/SocketInspector.java — createUserDataIfNotSet (51), setAuthUser/getAuthUser (157-169), getClientInfo (184), isSubscribed (479)` — symbol-level read
- `src/main/java/io/velo/MultiWorkerServer.java — async reply transfer (560-683)` incl. `transferAsyncReply (667-683)`, FORMAT→WRONG_NUMBER mapping (587-589, 648-650)
- `src/main/java/io/velo/RequestHandler.java — exception→error-reply handling (494-580)` — read range
- `src/main/java/io/velo/persist/OneSlot.java — asyncExecute (704-716)` — read range
- `src/main/java/io/velo/reply/AsyncReply.java` — full file (29 lines)
- `src/main/java/io/velo/reply/EmptyReply.java` — full file
- `dyn/src/io/velo/script/CommandCommandHandle.groovy` — full file (10 lines; actual delegation target of `CGroup.command()`, instantiates `io.velo.command.CommandCommand`) *(citation amended in Stage 3; discovery originally listed the one-hop-further target below)*
- `dyn/src/io/velo/command/CommandCommand.groovy — handle (39-70), info (72-94)` — read range (script class invoked by the handle script; quoted in finding 73.2)
- `src/test/groovy/io/velo/command/CGroupTest.groovy` — full file (746 lines)

Prior rounds checked for overlap (per STATE.md):

- `doc/bug_reviews/bug_53_client_sub_commands_review_round_1.md` — CLIENT sub-commands review. Its finding 1 (addr formatting) and finding 2 (replica kill overclaim) are already reflected in current code (`isSupportedClientKillType` rejects slave/replica at parse; ADDR normalized to `ip:port`) — NOT re-reported here.

Reference docs (ground truth):

- Redis 7.2 `db.c` `copyCommand` — https://raw.githubusercontent.com/redis/redis/7.2/src/db.c (fetched, exact source pasted into findings below)
- Redis 7.2 `networking.c` `clientCommand` / CLIENT KILL block (lines 3012-3340 of the fetched file) — https://raw.githubusercontent.com/redis/redis/7.2/src/networking.c
- https://redis.io/commands/copy/ (arity -3, `DB`/`REPLACE` args, `:0` when destination exists)
- https://redis.io/commands/client-kill/ (old form → `+OK`, filter form → integer)
- https://redis.io/commands/command/ (bare COMMAND returns all command details)
- `doc/design/04_command_processing_design.md`, `doc/design/19_command_registry_design.md` (module context)

## Finding 1 (Bug 73.1): Cross-slot COPY async lambda has no exception handling — client connection hangs forever on error

**Severity:** High
**Class:** Correctness / Robustness (Concurrency)
**Status:** `REFINED` — hang reproduced; "only site that omits try/catch" refuted (see Review Feedback)

### Cited files / lines
```java
// CGroup.java:595-615 — copy(), cross-request-worker branch
if (isCrossRequestWorker) {
    // current slot worker is src key slot's slot worker
    var dstSlot = dstSlotWithKeyHash.slot();
    var dstOneSlot = localPersist.oneSlot(dstSlot);

    SettablePromise<Reply> finalPromise = new SettablePromise<>();
    var asyncReply = new AsyncReply(finalPromise);

    boolean finalReplace = replace;
    dstOneSlot.asyncExecute(() -> {
        var dstCv = getCv(dstSlotWithKeyHash);          // can throw CannotReadException / DictMissingException / BigStringFileMissingException
        if (dstCv != null && !finalReplace) {
            finalPromise.set(IntegerReply.REPLY_0);
            return;
        }

        setCv(srcCv, dstSlotWithKeyHash);               // can throw on write errors
        finalPromise.set(IntegerReply.REPLY_1);
    });

    return asyncReply;
}
```
```java
// MultiWorkerServer.java:667-683 — the only consumer of the promise; no timeout
private Promise<ByteBuf> transferAsyncReply(Request request, AsyncReply reply, boolean isResp3) {
    var promise = reply.getSettablePromise();
    return promise.map((r, e) -> { ... });
}
```

### Root cause

*Amended in Stage 3 (response):* the sibling implementations cited during discovery (`SGroup.java:1310`, `MGroup.java:232`, `KGroup.java:130`, `BGroup.java:1019`) are `Promises.all(...).whenComplete(...)` / future-callback completions that do route failures into the promise — but they are not `asyncExecute` lambdas, and they are **not** the only relevant sibling shape. The reviewer identified the actual unguarded `asyncExecute`-lambda sites: `SGroup.java:2031` (SMOVE cross-slot, identical hang), `ZGroup.java:1372` (promise pre-completed on the calling thread → wrong success reply instead of hang), `BGroup.java:1154` (blocking-client promise never completed → blocked client never woken). So the defect class is codebase-wide, not CGroup-specific; this round fixes the CGroup instance (`CGroup.java:604-613`), and the SGroup/ZGroup/BGroup sites are cross-referenced for their owning modules' next rounds.

For the CGroup site itself: when the lambda runs cross-thread, `OneSlot.asyncExecute` (`OneSlot.java:704-716`) hands it to `slotWorkerEventloop.execute(runnable)`; an exception thrown inside the lambda is swallowed by the eventloop and nothing ever completes `finalPromise`.

### Trigger / impact

`COPY src dst` where src and dst hash to different slot-worker threads (the normal cross-slot case) plus any exception on the dst-slot worker while running the lambda:

- `getCv(dstSlotWithKeyHash)` throws `CannotReadException` when the dst slot is not readable (migrating / readonly during failover), `DictMissingException` (dst cv uses a dict not loaded on... decompress path), `BigStringFileMissingException` (dst is a big-string whose file is missing);
- `setCv` throws on write failures (e.g. big-string write RuntimeException at `BaseCommand.java:1054`).

Impact: the reply promise never resolves; `transferAsyncReply`'s `.map` never runs; the client socket never gets a reply or an error and no timeout exists on this path — the connection hangs until the client gives up. Redis semantics: any server-side error must surface as an error reply, never a silent hang.

### Suggested fix direction

Mirror the promise-completing siblings: wrap the lambda body in try/catch, `finalPromise.set(IntegerReply...)` on success paths and `finalPromise.setException(e)` in the catch, which `transferAsyncReply` maps to an error reply. Keep the diff minimal (that one lambda). Note (per review): `CannotReadException` carries a null message, so the catch must produce a non-null one (e.g. `new ErrorReply(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())`) or the client gets an empty `-ERR `.

## Finding 2 (Bug 73.2): Bare `COMMAND` is rejected with a wrong-number-of-args error; the intended handler branch is dead code

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines
```java
// CGroup.java:548-551
private Reply command() {
    if (data.length < 2) {
        return ErrorReply.FORMAT;
    }
    ...
}
```
```groovy
// dyn/src/io/velo/command/CommandCommand.groovy:43-50 — delegation target, unreachable branch
    // bare COMMAND (no subcommand) returns info for all commands
    if (data.length == 1) {
        return info()
    }

    if (data.length < 2) {
        return ErrorReply.FORMAT
    }
```
```java
// CGroup.java:48-55 — registry entry declares arity -1, i.e. zero extra args is legal
CommandRegistry.register(new CommandEntry(
        "command", -1, ...
```

### Root cause

`CGroup.command()` short-circuits `data.length < 2`, so a bare `COMMAND` (data.length == 1) returns `ErrorReply.FORMAT`, which `MultiWorkerServer` rewrites to `ERR wrong number of arguments for 'command' command` (MultiWorkerServer.java:587-589 / 648-650). The dyn script explicitly implements the Redis behavior (`data.length == 1 → info()`, and its help text documents "(no subcommand) — Return details about all Redis commands") but the guard in CGroup makes that branch unreachable.

### Trigger / impact

Any client sending `COMMAND` with no arguments (redis-cli `COMMAND | grep get`, Jedis `Protocol.Command.COMMAND` discovery, client capability probing) gets an arity error instead of the full command-details array that Redis 7.2 returns (https://redis.io/commands/command/). This also contradicts the command's own registered arity (-1).

### Suggested fix direction

In `CGroup.command()`, allow `data.length == 1` to delegate to the script (change guard to reject only the impossible `data.length < 1`, or drop it — the decoder guarantees at least one token). The script's own `data.length == 1 → info()` then handles bare COMMAND. Verify `info()`'s `isAll = data.length <= 2` covers both `COMMAND` and `COMMAND INFO`.

## Finding 3 (Bug 73.3): COPY does not reject source == destination

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — fix direction corrected (no `ERR ` prefix inside the Velo message)

### Cited files / lines
```java
// CGroup.java:572-594 — no src==dst check anywhere in copy()
private Reply copy() {
    ...
    var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
    var srcCv = getCv(slotWithKeyHash);
    if (srcCv == null) {
        return IntegerReply.REPLY_0;
    }

    var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
```
```c
/* Redis 7.2 src/db.c copyCommand — ground truth */
    robj *key = c->argv[1];
    robj *newkey = c->argv[2];
    if (src == dst && (sdscmp(key->ptr, newkey->ptr) == 0)) {
        addReplyErrorObject(c,shared.sameobjecterr);   /* "ERR source and destination objects are the same" */
        return;
    }
```

### Root cause

Redis 7.2 explicitly errors when source and destination are the same key (`shared.sameobjecterr`). Velo accepts it: `copy a a` returns `:0` (no REPLACE, dst exists) or `:1` with REPLACE, where `setCv` regenerates the seq and rewrites the record in place — a silent self-overwrite with a fresh seq.

### Trigger / impact

`COPY a a` / `COPY a a REPLACE` — clients get `:0`/`:1` instead of `ERR source and destination objects are the same`. With REPLACE the key's seq is regenerated and the record rewritten for nothing, which also perturbs replication/monitor ordering expectations for a no-op command.

### Suggested fix direction

After the strict option scan (see 73.4 — Redis parses options *before* the same-object check, so `COPY a a garbage` must be a syntax error), compare `data[1]` and `data[2]` (byte-level, e.g. `Arrays.equals`) and return `new ErrorReply("source and destination objects are the same")` when equal. Note: byte equality is the right comparison (Redis compares sds bytes), not slot equality. Do **not** include an `ERR ` prefix in the message — `ErrorReply.buffer()` (`ErrorReply.java:182-186`) prepends `-ERR ` itself.

## Finding 4 (Bug 73.4): COPY silently ignores unknown options and the `DB` option value

**Severity:** Medium
**Class:** Redis protocol compatibility / Edge case
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines
```java
// CGroup.java:580-586 — only scans for "replace", ignores everything else
boolean replace = false;
for (int i = 3; i < data.length; i++) {
    if ("replace".equalsIgnoreCase(new String(data[i]))) {
        replace = true;
        break;
    }
}
```
```c
/* Redis 7.2 src/db.c copyCommand — ground truth */
    for (j = 3; j < c->argc; j++) {
        int additional = c->argc - j - 1;
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "db") && additional >= 1) {
            if (getIntFromObjectOrReply(c, c->argv[j+1], &dbid, NULL) != C_OK)
                return;
            if (selectDb(c, dbid) == C_ERR) {
                addReplyError(c,"DB index is out of range");
                return;
            }
            ...
            j++; /* Consume additional arg. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }
```

### Root cause

The option loop never rejects unrecognized tokens. `COPY a b FOO`, `COPY a b db`, `COPY a b db 0`, `COPY a b db nonsense` are all accepted and the copy proceeds. Redis 7.2 returns `ERR syntax error` for unknown tokens / dangling `DB` without a value, an integer-parse error for a non-numeric db, and `ERR DB index is out of range` for out-of-range db ids.

### Trigger / impact

Typos in option names (`REPLCE`, `DESTINATION`) silently perform the copy as if no option were given — the caller believes REPLACE semantics were applied when they were not. `DB` is accepted although Velo has a single logical DB: cross-db copies silently land in the same db instead of erroring.

### Suggested fix direction

Strict scan, replacing the current loop entirely (the `break` on first `replace` also hides trailing garbage): recognize `REPLACE` (token) and `DB <int>` (value required). Unknown token or dangling `DB` → `ErrorReply.SYNTAX`; `DB <non-numeric>` → `ErrorReply.NOT_INTEGER` (`ErrorReply.java:67`, matching Redis's `getIntFromObjectOrReply` error); `DB 0` keeps working. For a nonzero db id pick one Redis analogue and note the choice in the commit: either `new ErrorReply("DB index is out of range")` (standandalone out-of-range) or `new ErrorReply("Copying to another database is not allowed in cluster mode")` (cluster-mode any-nonzero) — both defensible for a single-db server; this round adopts the standalone message. The scan must run before the 73.3 equality check to preserve Redis's check ordering.

## Finding 5 (Bug 73.5): Legacy `CLIENT KILL ip:port` form diverges from Redis on two counts: reply type and skipme default

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — existing test CGroupTest.groovy:334-335 asserts the buggy reply type

### Cited files / lines
```java
// CGroup.java:325-334 — legacy form parsed with skipMe=true
private ClientKillFilter parseClientKillFilter() {
    // Legacy form: CLIENT KILL ip:port (exactly three args, second is the addr).
    if (data.length == 3) {
        var legacyAddr = new String(data[2]);
        if (legacyAddr.contains(":")) {
            return new ClientKillFilter(null, null, null, legacyAddr, null, true, null);
        }
```
```java
// CGroup.java:530-531 — always replies with the kill count
        log.info("Client kill complete, killed={}", killed);
        return new IntegerReply(killed);
```
```c
/* Redis 7.2 src/networking.c clientCommand CLIENT KILL block — ground truth */
        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        }
        ...
        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }
```

### Root cause

Two divergences on the legacy path:

1. Redis sets `skipme = 0` for the old form — the caller can kill itself by address. Velo stamps `skipMe=true`, so `CLIENT KILL <own-ip:port>` kills nothing (`:0`) instead of closing the calling connection.
2. Redis replies `+OK` on a successful old-form kill and `-ERR No such client` when nothing matched (also documented at https://redis.io/commands/client-kill/). Velo replies `:0` / `:1` integers on this path too.

### Trigger / impact

Scripts or older Sentinel-style clients that use the deprecated 3-arg form and check for `+OK` see an integer reply instead; `CLIENT KILL own-address` fails to disconnect the caller.

### Suggested fix direction

Thread an `oldForm` flag (or return type carrying it) from `parseClientKillFilter` to `killClientsMatching`: reply `OKReply.INSTANCE` when killed > 0, `new ErrorReply("No such client")` when 0, and default `skipMe=false` for the legacy form. Filter-form behavior stays unchanged.

## Finding 6 (Bug 73.6): `CLIENT REPLY <unknown-mode>` is accepted as ON and acknowledged with +OK

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines
```java
// CGroup.java:185-199 — no validation of the mode token
if ("reply".equals(subCmd)) {
    if (data.length != 3) {
        return ErrorReply.FORMAT;
    }

    var replyModeStr = new String(data[2]).toLowerCase();
    var replyMode = VeloUserDataInSocket.ReplyMode.from(replyModeStr);
    ...
    veloUserData.setReplyMode(replyMode);
    return replyMode == VeloUserDataInSocket.ReplyMode.on ? OKReply.INSTANCE : EmptyReply.INSTANCE;
}
```
```java
// VeloUserDataInSocket.java:151-157 — default maps garbage to ON
public static ReplyMode from(String str) {
    return switch (str) {
        case "off" -> off;
        case "skip" -> skip;
        default -> on;
    };
}
```
```c
/* Redis 7.2 src/networking.c — ground truth */
    } else if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            ...
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            ...
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            ...
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
```

### Root cause

`ReplyMode.from()` folds every unknown token into `on`, and `client()` does not validate the token before calling it. `CLIENT REPLY maybe` stores mode `on` and replies `+OK`, silently confirming a mode that does not exist.

### Trigger / impact

`CLIENT REPLY ogn` (typo), `CLIENT REPLY maybe` — Redis replies `ERR syntax error`; Velo replies `+OK` and leaves the connection in ON mode, hiding client bugs. Notably `CLIENT REPLY skip` is a one-shot mode, so a typo silently turns a skip into a confirmed ON.

### Suggested fix direction

Validate in `client()` (keep `ReplyMode.from` untouched — it is also used on decode paths where a lenient default may be intended): `on`/`off`/`skip` → proceed; anything else → `ErrorReply.SYNTAX`. Do not mutate `veloUserData` on the error path.

## Finding 7 (Bug 73.7): `CLIENT SETNAME` accepts spaces/newlines/binary junk and does not treat the empty name as a clear

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — fix direction corrected (no `ERR ` prefix inside the Velo message)

### Cited files / lines
```java
// CGroup.java:224-233 — stores any bytes verbatim
if ("setname".equals(subCmd)) {
    if (data.length != 3) {
        return ErrorReply.FORMAT;
    }

    var clientName = new String(data[2]);
    var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
    veloUserData.setClientName(clientName);
    return OKReply.INSTANCE;
}
```
```c
/* Redis 7.2 src/networking.c — ground truth */
static int validateClientAttr(char *val) {
    ...
        if (*val < '!' || *val > '~') { /* ASCII is assumed. */
            return C_ERR;
        }
...
int validateClientName(robj *name, const char **err) {
    const char *err_msg = "Client names cannot contain spaces, newlines or special characters.";
    int len = (name != NULL) ? sdslen(name->ptr) : 0;
    /* We allow setting the client name to an empty string. */
    if (len == 0)
        return C_OK;
    ...
/* clientSetName(): "Setting the client name to an empty string actually removes the current name." */
```

### Root cause

No character validation and no empty-string clearing. Redis restricts names to printable ASCII `!`..`~` (0x21-0x7E) — no spaces, newlines, or special characters — and defines `SETNAME ""` as removing the name so a subsequent `CLIENT GETNAME` returns nil.

### Trigger / impact

`CLIENT SETNAME "foo bar"` / `"foo\nbar"` → Velo `+OK`, then `CLIENT LIST name=foo bar` emits a line whose field layout is corrupted for any parser splitting on spaces (Redis guarantees that never happens). `CLIENT SETNAME ""` then `CLIENT GETNAME` returns an empty bulk `""` instead of nil, breaking clients that test for null.

### Suggested fix direction

Validate every byte of `data[2]` is in `[0x21, 0x7E]`, else `new ErrorReply("Client names cannot contain spaces, newlines or special characters.")` — no `ERR ` prefix, `ErrorReply.buffer()` adds `-ERR ` itself (`ErrorReply.java:182-186`). On an empty name, set the client name to null (`setClientName(null)`) so GETNAME returns `NilReply` (the existing `clientName == null ? NilReply : bulk` path at CGroup.java:158 already handles null).

## Notes / non-findings

- **COPY returning `:0` when destination exists without REPLACE is CORRECT.** Verified against Redis 7.2 `db.c` (`addReply(c,shared.czero)`) and https://redis.io/commands/copy/ ("The command returns zero when the destination key already exists"). Recorded here so no later round "fixes" it into an error reply.
- COPY preserves TTL: `setCv` carries `cv.getExpireAt()` through (`BaseCommand.java:950-975`), matching Redis `setExpire(dst, newkey, expire)`.
- bug_53 findings already resolved in current code, not re-reported: ADDR normalization to `ip:port` (CGroup.java:464-474), TYPE slave/replica rejected at parse time (`isSupportedClientKillType`, CGroup.java:405-409).
- `CLIENT LIST` `TYPE`/`ID` filters unsupported → FORMAT error: documented feature gap in `clientList()` javadoc, not a correctness bug.
- `CLIENT INFO` / `CLIENT LIST` return bulk strings, Redis returns RESP3 verbatim (`=` txt): codebase-wide encoding design, consistent with INFO handling — not a CGroup bug.
- `CLIENT KILL USER <unknown-user>` returns `:0` where Redis errors `No such user 'x'` (Redis resolves the ACL user at parse time); `CLIENT KILL ID 0` / negative id accepted (returns `:0`) where Redis requires id ≥ 1. Both are minor validation-only divergences on the filter form; noted for a possible hardening pass, not elevated to findings.
- Cross-slot COPY has a TOCTOU window (src cv read on src worker, dst write later on dst worker) — inherent to the multi-threaded architecture, same class of window as other cross-slot commands; not actionable in CGroup alone.
- `client reply off` returning `EmptyReply` (zero-byte buffer) is equivalent to Redis's no-reply for wire purposes; `skip` semantics are handled by the response encoder reading `replyMode`, outside CGroup.
- `AsyncReply` returned by cross-slot COPY is correctly transferred by `MultiWorkerServer.transferAsyncReply` (both net-worker paths at 584-585 / 645-646).
- `client()` unknown subcommand → SYNTAX: Redis replies with a longer "Unknown subcommand" message; both are errors, text-only difference.
- Delegated scripts `ClusterxCommandHandle` / `ConfigCommandHandle` internals are covered by the cluster (bug_59-65) and configuration (bug_24-26) rounds; CGroup-side delegation (arity guards only) was checked and is consistent with their arities.

## Reviewer checklist (for AI agent 2)

- Finding 1 (73.1): build a two-slot fixture (src slot worker ≠ dst slot worker) with `byPassGetSet = null` and a dst slot whose `isCanRead()` is false (or stub `getCv` to throw via a missing big-string file). Send `copy src dst` cross-worker. Assert: with the current code the `SettablePromise` never completes within N seconds (inspect `AsyncReply.getSettablePromise().isComplete()` polling); contrast with any sibling command (e.g. SGroup async path) that completes with an exception. Static cross-check: grep for try/catch around `dstOneSlot.asyncExecute` in CGroup vs SGroup/MGroup/KGroup/BGroup.
- Finding 2 (73.2): `cGroup.execute('command')` (single-token data) currently returns `ErrorReply.FORMAT`; after fix it must reach `CommandCommand.handle()`'s `data.length == 1 → info()` and return a MultiBulkReply. Also assert `CommandRegistry.get('command').arity() == -1` consistency. Differential: `redis-cli COMMAND` on Redis 7.2 returns the full array.
- Finding 3 (73.3): with key `a` existing, `execute('copy a a')` → expect `ErrorReply` with message "ERR source and destination objects are the same" after fix (currently `:0`); `copy a a replace` → same error (currently `:1`).
- Finding 4 (73.4): `execute('copy a b replce')` / `'copy a b db'` / `'copy a b db 5'` → expect `ErrorReply.SYNTAX` / `SYNTAX` / "DB index is out of range" after fix (all currently perform the copy); `'copy a b db 0'` → still performs the copy.
- Finding 5 (73.5): register a second socket, then (a) `client kill <second-addr>` → currently IntegerReply(1), expect OKReply after fix; (b) `client kill 127.0.0.1:1` (no match) → currently IntegerReply(0), expect ErrorReply "No such client"; (c) `client kill <own-addr>` → currently IntegerReply(0) with self preserved, expect self scheduled for close after fix.
- Finding 6 (73.6): `execute('client reply maybe')` → currently OKReply, expect `ErrorReply.SYNTAX`; follow-up `client reply on` must still return OK; assert `veloUserData.replyMode` unchanged after the rejected call.
- Finding 7 (73.7): `execute('client setname "a b"')` → currently OKReply, expect ErrorReply("Client names cannot contain spaces, newlines or special characters."); `setname ""` then `getname` → currently empty bulk, expect NilReply after fix; valid name `abc!~` still OK.

## Review Feedback (AI agent 2, round 1)

**Reviewed commit:** `0accc625dd94fb52917d08664ff8bafb39f2a203` (unchanged during verification)
**Method:** Spock temp spec (`src/test/groovy/io/velo/command/CGroupReview73Spec.groovy`, written by reviewer, deleted after evidence capture) + Redis 7.2 source differential (`src/db.c`, `src/networking.c`, `src/server.c` re-fetched and re-quoted) + static inspection of the sibling cross-slot call sites and of the reply-transfer path.
**Command:** `./gradlew :cleanTest :test --tests "io.velo.command.CGroupReview73Spec"` → `BUILD SUCCESSFUL` (all 7 features green; each feature asserts the *current, buggy* behaviour, so green == the bug is reproducible).

Every finding below was checked against Redis 7.2 source re-fetched by the reviewer; all quoted Redis lines match the report verbatim.

| Finding | Verdict | Evidence |
|---|---|---|
| 73.1 | `REFINED` — hang reproduced; the "only cross-slot command without try/catch" claim is REFUTED | Spock + static, below |
| 73.2 | `CONFIRMED (reproduced)` | Spock + `dyn/test/.../CommandCommandTest.groovy:24-38` |
| 73.3 | `CONFIRMED (reproduced)` | Spock + `server.c:1848` |
| 73.4 | `CONFIRMED (reproduced)` | Spock + `db.c copyCommand` |
| 73.5 | `CONFIRMED (reproduced)` (all 3 sub-claims) | Spock + `networking.c:3156-3160, 3233-3240` |
| 73.6 | `CONFIRMED (reproduced)` | Spock + `networking.c:3117-3133` |
| 73.7 | `CONFIRMED (reproduced)` (both sub-claims) | Spock + `networking.c:2896-2939` |

### 73.1 — `REFINED`

**Confirmed (reproduced): the client hang is real.** The reviewer drove `copy a b replace` down the cross-request-worker branch with a dst-side `getCv` that throws `CannotReadException`, exactly one of the exceptions the report lists:

```
20:16:40.407 [Thread-4] ERROR [io.activej.eventloop.Eventloop] - Fatal error in io.velo.command.CGroup$$Lambda/...@3de2ab9e
io.velo.persist.CannotReadException
	at io.velo.command.CGroupReview73Spec$ThrowOnDstCGroup.getCv(CGroupReview73Spec.groovy:40)
	at io.velo.command.CGroup.lambda$copy$1(CGroup.java:605)
	at io.activej.eventloop.Eventloop.executeTask(Eventloop.java:578)
73.1 promise complete after 1s = false
73.1 same-worker exception = io.velo.persist.CannotReadException: null
```

The eventloop swallows the throw ("Fatal error in ... CGroup$$Lambda", `Eventloop.java:578` — `slotWorkerEventloop.execute(runnable)` path of `OneSlot.asyncExecute`, `OneSlot.java:714`), `finalPromise` is never completed, `transferAsyncReply` (`MultiWorkerServer.java:667-683`) never fires, and no timeout exists on that path. The control line (`same-worker exception = ...`) shows the identical exception propagates normally when the command runs on the dst worker itself, i.e. the loss of the error reply is caused by the missing try/catch and nothing else.

Reproducer sketch for the fix agent (boilerplate: reuse `CGroupTest.clientFixture()` / `registerSocketOnInspector`):

```groovy
static class ThrowOnDstCGroup extends CGroup {
    ThrowOnDstCGroup(String cmd, byte[][] data, ITcpSocket socket) { super(cmd, data, socket) }
    @Override CompressedValue getCv(BaseCommand.SlotWithKeyHash s) {
        if ('b' == new String(s.rawKey())) { throw new CannotReadException() }
        return super.getCv(s)
    }
}
// byPassGetSet = InMemoryGetSet holding cv under 'a'; dst slot owned by a second Eventloop;
// cGroup.crossRequestWorker = true; execute('copy a b replace') on a third eventloop,
// then poll (reply as AsyncReply).settablePromise.isComplete()
```

**Refuted part — the uniqueness claim.** The report states "Every other cross-slot command in this codebase wraps its `asyncExecute` lambda in try/catch ... `CGroup.copy()` is the only one that omits the try/catch", citing `SGroup.java:1310`, `MGroup.java:232`, `KGroup.java:130`, `BGroup.java:1019`. Those four line numbers are `Promises.all(...).whenComplete(...)` / future callbacks — they do set the exception into a promise, but they are not `asyncExecute` lambdas. The actual unguarded `asyncExecute` lambdas that complete a `SettablePromise` are the same shape as CGroup's:

- `src/main/java/io/velo/command/SGroup.java:2031-2039` (SMOVE cross-slot: `dstOneSlot.asyncExecute(() -> { ... finalPromise.set(IntegerReply.REPLY_1); });` — no try/catch)
- `src/main/java/io/velo/command/ZGroup.java:1372` (`dstOneSlot.asyncExecute(() -> saveRedisZSet(finalRz, dstSlotWithKeyHash));` — no try/catch)
- `src/main/java/io/velo/command/BGroup.java:1154` (`asyncExecute(() -> completeIfListNotEmpty(...))` — no try/catch)

So the defect is real but **not CGroup-specific**, and it should not be framed as a CGroup-exclusive mistake. Recommended handling: fix the CGroup lambda in this round (it is the smallest, most reachable case — `getCv` on the dst slot is the one call with a live `CannotReadException` path), and file the SGroup/ZGroup/BGroup sites as a separate, sibling-module finding rather than expanding 73.1 beyond its cited file. Severity stays `High` (a silent client hang), trigger likelihood stays low.

**Fix-direction correction:** nothing to correct — `finalPromise.setException(e)` is the right completion, matching what `transferAsyncReply` already maps to `new ErrorReply(e.getMessage())`. Note for the fix agent: `CannotReadException`'s message is `null`, so the resulting error reply will be `ErrorReply{null}` unless the catch supplies a fallback message (e.g. `new ErrorReply("ERR " + e)` or `e.getClass().getSimpleName()`). Redis's `transferAsyncReply` equivalent always carries a message, so please make the catch produce a non-null message.

### 73.2 — `CONFIRMED (reproduced)`

```
73.2 bare COMMAND -> ErrorReply                      # via CGroup.execute('command')
73.2 CommandCommand.handle() data.length==1 -> MultiBulkReply
```

`CGroup.command()`'s guard (`CGroup.java:548-551`) rejects the single-token form before the dyn script is consulted, so `CommandCommand.groovy:43-45` (`if (data.length == 1) return info()`) is unreachable in production.

Independent corroboration that this is a defect and not an intentional guard: `dyn/test/io/velo/command/CommandCommandTest.groovy:24-38` already asserts the opposite of production —

```groovy
when:
def reply = commandCommand.execute('command')
then:
reply instanceof MultiBulkReply
// bare COMMAND returns info for all commands
(reply as MultiBulkReply).replies.length == CommandRegistry.size()
```

Redis 7.2 `server.c commandCommand` confirms bare `COMMAND` returns the full array (no arity check), and `CGroup.java:48-55` registers arity `-1`, which is consistent with "zero extra args is legal". Nothing in `CGroupTest` pins the current `ErrorReply.FORMAT` behaviour (its `test handle` table covers `client`/`clusterx`/`config`/`copy`/`zzz` only), so the fix is unblocked test-wise.

### 73.3 — `CONFIRMED (reproduced)`

```
73.3 'copy a a' -> io.velo.reply.IntegerReply$2@6f926d01 ; 'copy a a replace' -> io.velo.reply.IntegerReply$1@c67a89
```

(`IntegerReply$1` / `$2` are the anonymous `REPLY_1` / `REPLY_0` singletons — the assertions also compare against `IntegerReply.REPLY_1` / `REPLY_0` directly.)

Redis 7.2 ground truth re-quoted from `src/server.c:1846-1850`:

```c
shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
    "-ERR source and destination objects are the same\r\n"));
```

and `src/db.c copyCommand` checks it *after* the option loop and *before* the source lookup.

**Fix-direction correction (must be followed):** the report suggests `new ErrorReply("ERR source and destination objects are the same")`. `ErrorReply.buffer()` (`ErrorReply.java:182-186`) prepends `-ERR ` itself, so that literal would go on the wire as `-ERR ERR source and destination objects are the same`. Use `new ErrorReply("source and destination objects are the same")`. Same correction applies to 73.7 below.

### 73.4 — `CONFIRMED (reproduced)`

```
73.4 replce -> REPLY_1; db -> REPLY_1; db 5 -> REPLY_1; db nonsense -> REPLY_1; replace whatever -> REPLY_1
```

All five malformed invocations perform the copy and reply `:1`. Redis 7.2 `copyCommand` (re-quoted by the reviewer, identical to the report) replies `ERR syntax error` for `replce`, for a dangling `db`, and for `db whatever`; the `break` on the first `replace` in `CGroup.java:581-586` is confirmed to make trailing garbage invisible.

One clarification on the suggested fix: Redis accepts `DB <id>` for any id `< databases` in standalone mode (default 16) — only *out-of-range* ids get `ERR DB index is out of range`, and in **cluster mode** any nonzero db id gets `ERR Copying to another database is not allowed in cluster mode`. Since Velo has exactly one logical db, both Redis analogues are defensible; the important, unambiguous part is: unknown tokens and a dangling `DB` must be `ErrorReply.SYNTAX`, `DB <non-numeric>` must be `ErrorReply.NOT_INTEGER` (matching Redis `getIntFromObjectOrReply` → `ERR value is not an integer or out of range`), and `DB 0` must keep working. Pick one of the two nonzero-db messages and note the choice in the commit.

### 73.5 — `CONFIRMED (reproduced)` — three divergences, all observed

```
73.5 legacy kill of another client -> IntegerReply(1)              # Redis: +OK
73.5 legacy kill, no match        -> IntegerReply(0)              # Redis: -ERR No such client
73.5 legacy self kill             -> IntegerReply(0) ; caller still registered = true   # Redis: skipme=0, self is killed
```

Redis 7.2 `networking.c` re-quoted at 3156-3160 (`skipme = 0; /* With the old form, you can kill yourself. */`) and 3233-3240 (`if (c->argc == 3) { if (killed == 0) addReplyError(c,"No such client"); else addReply(c,shared.ok); }`). The third line is the most user-visible: `CLIENT KILL <own ip:port>` is the documented way to force-close a connection from inside a script, and in Velo it silently returns `:0` and leaves the connection open (`CGroup.java:330` stamps `skipMe=true` on the legacy path).

**Blocking note for the fix agent:** `src/test/groovy/io/velo/command/CGroupTest.groovy:334-335` currently asserts the buggy reply type for this exact command —

```groovy
reply = cGroup.execute("client kill ${addrToKillStr}")
then:
reply instanceof IntegerReply
(reply as IntegerReply).integer == 1L
```

That assertion must be inverted as part of the 73.5 fix (the `ADDR`-filter case at :346-347 and the other filter cases must keep replying an integer). The legacy-form test also kills a socket, so keep the polling cleanup.

### 73.6 — `CONFIRMED (reproduced)`

```
73.6 'client reply maybe' -> io.velo.reply.OKReply@... ; stored mode = on
```

`CGroup.java:190-198` stores the mode and acknowledges `+OK`; `VeloUserDataInSocket.ReplyMode.from` (`VeloUserDataInSocket.java:151-157`) maps anything that is not `off`/`skip` to `on`. Redis 7.2 `networking.c:3117-3133` ends the same if-chain with `addReplyErrorObject(c,shared.syntaxerr)`. Confirmed as reported; the fix must validate before `setReplyMode`, leaving `ReplyMode.from` alone (the report's reasoning there is sound — it is a lenient default used on decode paths).

### 73.7 — `CONFIRMED (reproduced)` — both sub-claims observed

```
73.7 'client setname "a b"' -> io.velo.reply.OKReply@...
73.7 'client setname ""'    -> io.velo.reply.OKReply@... ; getname -> BulkReply ''
```

Redis 7.2 `networking.c:2896-2939` re-quoted: `validateClientAttr` rejects any byte `< '!' || > '~'`, `validateClientName` allows `len == 0`, and `clientSetName` clears `c->name` for an empty name — so a Redis server returns `+OK` for `SETNAME "a b"`? No: it returns the error `"Client names cannot contain spaces, newlines or special characters."`, and `SETNAME ""` followed by `GETNAME` returns nil. Velo returns `+OK` for the space-containing name and an empty bulk string (not nil) after clearing, exactly as reported. `CLIENT LIST` line corruption is a real consequence of the missing validation (`SocketInspector.getClientInfo` emits `name=<raw>`).

**Fix-direction correction:** use `new ErrorReply("Client names cannot contain spaces, newlines or special characters.")` — without the `ERR ` prefix, since `ErrorReply.buffer()` adds `-ERR ` (see 73.3). The empty-name clearing is already supported downstream: `CGroup.java:158` already returns `NilReply.INSTANCE` when `clientName == null`, so `setClientName(null)` is sufficient.

### Additional observations

- **Doc citation drift in the Scope section (not a finding).** The report cites `dyn/src/io/velo/command/CommandCommand.groovy` as the delegation target; the production target is `dyn/src/io/velo/script/CommandCommandHandle.groovy` (10 lines) which instantiates `io.velo.command.CommandCommand`. The quoted Groovy is real, just one hop further away. All CGroup line numbers cited in the findings still match commit `0accc625`.
- **73.3 / 73.4 precedence.** Redis performs option parsing *before* the same-object check. If both are fixed, `COPY a a garbage` must be a syntax error, not "same object" — put the strict option scan first in `copy()`.
- **73.2 side effect.** Allowing `data.length == 1` through `CGroup.command()` also means bare `COMMAND` now returns an array sized `CommandRegistry.size()`, i.e. tens of thousands of elements per call. That is Redis behaviour, but consider whether an ACL/cost guard is wanted (out of scope for this round).
- **Non-findings re-checked, all still negative:** COPY returning `:0` for an existing dst without REPLACE is correct (`db.c`: `addReply(c,shared.czero)`); TTL is carried by `setCv`; `isSupportedClientKillType` still rejects slave/replica at parse; unknown CLIENT subcommand already returns `ErrorReply.SYNTAX`. No new findings were raised by the verification pass.
- **Coverage discipline for the fix agent.** `scripts/jacoco_cover.py io.velo.command.CGroup 548 625` should show the new `command()` guard, the `copy()` option-scan branches (all three: `replace` / `db`-with-value / unknown-token, plus the `db`-without-value fallthrough), the legacy-kill `oldForm` branch, and the `setname` validation loop as covered — the current suite never enters several of these.

### Recommended fix order (for the fix agent)

1. **73.2** — smallest diff (one guard), zero risk, and it revives already-tested dyn code (`CommandCommandTest.groovy:24-38` becomes the regression test).
2. **73.6** — one validation block, no shared state touched, no existing test asserts the current behaviour.
3. **73.3 + 73.4** — same method (`copy()`); do them as **two commits** (73.4's strict option scan first, since it must run before 73.3's equality check), so each has its own failing test and its own red/green evidence. Update `CGroupTest.'test copy'` expectations.
4. **73.7** — validation loop + empty-name clearing; small, self-contained.
5. **73.5** — needs the `oldForm` flag threaded from `parseClientKillFilter` to `killClientsMatching`, plus the inverted assertion in `CGroupTest.groovy:334-335`. Do it last among the protocol fixes so the test churn lands in one commit.
6. **73.1** — highest value, lowest reproduction cost in production. Fix after the protocol fixes so its diff is isolated; add the regression test using the `ThrowOnDstCGroup` sketch above. Note the new requirement to give the error reply a non-null message.
7. **Out of this round:** the equivalent unguarded `asyncExecute` lambdas in `SGroup.java:2031`, `ZGroup.java:1372`, `BGroup.java:1154` — file as a new finding in the owning module's next round, cross-referenced to 73.1.

## Fix Log (AI agent 1, round 1 fix phase)

Fix order per Review Feedback: 73.2 → 73.6 → 73.4 → 73.3 → 73.7 → 73.5 → 73.1. Line ranges in the JaCoCo column were measured at each commit's code state (the sha anchors the state). Focused-test evidence per commit is the `build/test-results/test/TEST-io.velo.command.CGroupTest.xml` of a clean rerun; final class-level sweep: `CGroupTest` 11/11 + `CommandCommandTest` 7/7 green.

| Bug | Failing test evidence (red) | Fix commit | Focused test (green) | JaCoCo confirmation |
|---|---|---|---|---|
| 73.1 | `test copy` RED: `(reply as AsyncReply).settablePromise.isComplete()` = **false** after 3s poll — hang reproduced exactly as in the review | `df89638e` `fix: complete copy async promise on exception` | `tests="1" failures="0"` | async lambda 650-672: 15/15 lines, 2/2 branch lines fully covered (incl. both message-fallback ternary arms via `CannotReadException` and `RuntimeException('boom')`) |
| 73.2 | `test command` RED: `ConditionNotSatisfiedError at CGroupTest.groovy:686` — bare COMMAND returned `ErrorReply.FORMAT`, not `MultiBulkReply` | `2f91f43b` `fix: allow bare command to delegate to dyn script` | `tests="1" failures="0"` | `command()` 548-556 all covered (guard removed, delegation executed) |
| 73.3 | `test copy` RED: `copy a a` returned `IntegerReply$2` (`:0`), not an ErrorReply | `30f31f95` `fix: reject copy when source equals destination` | `tests="1" failures="0"` | equality check 612-622: both branches covered (incl. options-first ordering case `copy a a garbage` → SYNTAX) |
| 73.4 | `test copy` RED: `copy a b replce` returned `IntegerReply$2` (`:0`), not SYNTAX | `28fced74` `fix: strict copy option parsing for replace and db` | `tests="1" failures="0"` | option scan 587-615: every branch covered (replace / db+value / dangling db / NOT_INTEGER / nonzero db / unknown token); pinned `replace_` cases inverted to SYNTAX |
| 73.5 | `test client` RED: legacy kill returned `IntegerReply integer=1`, not `OKReply` (pinned case at former lines 334-335 inverted per review) | `1189e147` `fix: legacy client kill reply type and skipme default` | `tests="1" failures="0"` | legacy parse 348-358 both branches + `oldForm` reply 557-558 both branches covered; self-kill case uses a dedicated socket so the fixture lifecycle stays intact |
| 73.6 | `test client` RED: `reply == ErrorReply.SYNTAX` got `OKReply` — `client reply maybe` folded to ON and was acknowledged | `686c5914` `fix: reject unknown client reply mode with syntax error` | `tests="1" failures="0"` | reply branch 185-216: 17/17 lines, 6/6 branch lines fully covered; stored-mode-unchanged proven via reflection on `replyMode` |
| 73.7 | `test client` RED: `client setname "a b"` returned `OKReply`, not the validation error | `3dc99870` `fix: validate client setname charset and clear name on empty` | `tests="1" failures="0"` | charset loop 236-258: 12/12 lines, 5/5 branch lines fully covered (0x21/0x7E arms + empty-name ternary; `setname ""` → GETNAME nil asserted) |
| A1 (re-review action) | coverage regression visible only via JaCoCo (no failing behavior) | `94db1fa4` `test: cover same-worker copy dst-exists branch again` | `tests="1" failures="0"` | `CGroup.java:676-677` restored: `All 4 branches covered` |
| R1 (re-review follow-up) | `test client` RED on `1189e147`: self-killed socket already removed within 200ms (close-before-reply) | `9f06178c` `fix: close self-killed client after reply written` | `tests="1" failures="0"`; class sweep CGroupTest 11/11 + CommandCommandTest 7/7 + SocketInspectorTest 10/10 | `CGroup.java:539` both arms; `SocketInspector.java:827-832` both arms (true from self-kill cases, false from SocketInspectorTest) |

Notes from the fix phase:

- Test-only adjustment during 73.6 green phase: the "stored mode unchanged" assertion initially expected `on`, but the preceding valid `client reply off` case had already stored `off`; corrected to assert `off` — which is the actual unchanged proof.
- Test-only adjustment during 73.5 green phase: the self-kill case first killed + re-registered the fixture socket, which degraded the later pre-existing `SKIPME no` case's close lifecycle (consistent failure at `CGroupTest.groovy:477`); rewritten to use a dedicated socket with its own `CGroup` instance — the failure disappeared.
- Transient first-run gradle failures (`:compileTestGroovy` / `:test` failing once after edits, passing on immediate rerun with unchanged sources) occurred repeatedly during the phase; every green claim above is therefore backed by the test-results XML of a clean rerun, not the gradle exit code alone.

## Response (AI agent 1, round 1)

All seven verdicts accepted. Every reviewer correction was re-verified against the codebase before agreeing; no disagreement is raised. Retractions: none (no finding was refuted as a whole — only a sub-claim inside 73.1, retracted and amended in place as noted below).

- **73.1 `REFINED` — accepted, sub-claim retracted.** I re-read all seven cited sites. Confirmed: `SGroup.java:1310`, `MGroup.java:232`, `KGroup.java:130`, `BGroup.java:1019` are `Promises.all(...).whenComplete(...)` / `Promise.ofFuture(...).whenException(...)` completions, not `asyncExecute` lambdas — my "only site that omits try/catch" claim was wrong and is struck from the root cause (amended in place). Confirmed the reviewer's three real sibling sites: `SGroup.java:2031` (SMOVE: lambda completes `finalPromise` with no try/catch — identical hang), `ZGroup.java:1372` (no try/catch, but `finalPromise.set(new IntegerReply(finalRz.size()))` runs on the calling thread before the dst save — failure mode is a *wrong success reply*, i.e. the client is told `:N` while the dst write was lost, not a hang), `BGroup.java:1154` (`recheckBlockingList` → `completeIfListNotEmpty` completes a *blocking client's* promise — an exception leaves the blocked client never woken, a hang variant on the blocking path). One precision added for the cross-referenced sibling findings: the three sites are the same defect class but with two distinct failure modes (hang vs wrong-success); the ZGroup instance may deserve the higher severity of the two when its owning round picks it up. The CGroup finding itself (client hang at `CGroup.java:604-613`) stands as reproduced, severity High unchanged.
- **73.2 — accepted.** Independently re-verified `dyn/test/io/velo/command/CommandCommandTest.groovy:24-38`: the dyn test already pins `execute('command')` → `MultiBulkReply` of size `CommandRegistry.size()`, i.e. production contradicts an existing green test through the CGroup guard. Fix order 1st accepted.
- **73.3 — accepted, fix direction corrected in place.** Verified `ErrorReply.buffer()` (`ErrorReply.java:182-186`) prepends `-ERR ` (unless `rawErrorPrefix`), so the message must be `"source and destination objects are the same"` without the prefix; the finding text now says so. Redis `server.c` quote for `sameobjecterr` accepted.
- **73.4 — accepted, fix direction corrected in place.** Verified `ErrorReply.NOT_INTEGER` exists (`ErrorReply.java:67`, `"not integer"` — the codebase's existing convention for this Redis error). Adopted the reviewer's clarification: dangling/unknown → `SYNTAX`, non-numeric → `NOT_INTEGER`, `DB 0` works, nonzero → the standalone `"DB index is out of range"` message (choice recorded in the finding). Precedence note accepted: option scan before the 73.3 equality check, matching `db.c` ordering (`COPY a a garbage` → syntax error).
- **73.5 — accepted.** Re-verified `CGroupTest.groovy:333-336`: the legacy-form case asserts `reply instanceof IntegerReply` / `integer == 1L` — this pins the buggy reply type and must be inverted by the fix while the `ADDR`-filter cases (line ~352) keep the integer. The reviewer's third sub-claim (self-kill via legacy form impossible with `skipMe=true` at `CGroup.java:330`) is the most user-visible divergence; agreed.
- **73.6 — accepted.** No test pins the current lenient behavior; fix is validation before `setReplyMode`, `ReplyMode.from` stays untouched. Agreed with order 2nd.
- **73.7 — accepted, fix direction corrected in place.** Same `ErrorReply.buffer()` prefix reasoning as 73.3 — message without `ERR `. Empty-name → `setClientName(null)` relies on the already-existing null→`NilReply` path at `CGroup.java:158`; verified.
- **Additional observations — all accepted.** Scope citation amended (`CommandCommandHandle.groovy` hop added); 73.2 array-size cost and the JaCoCo coverage discipline note are taken as constraints for the fix phase.

**Dispositions:** 73.1–73.7 all proceed to fix, in the reviewer's order: 73.2 → 73.6 → 73.4 → 73.3 → 73.7 → 73.5 → 73.1, one commit each. The SGroup/ZGroup/BGroup unguarded-lambda sites are deferred to their owning modules' next rounds (cross-reference to 73.1 recorded above), per the reviewer's recommendation 7.

## Re-review (AI agent 2)

**Reviewed state:** `71e7b230` (`docs: bug 73 all fixed with 7 commits, update fix log and state`), tree clean for `src/main/java/io/velo/command/CGroup.java` and `src/test/groovy/io/velo/command/CGroupTest.groovy`.
**Method:** (a) independent Spock re-review spec `src/test/groovy/io/velo/command/CGroupReReview73Spec.groovy` asserting the *fixed* behaviour, written from the findings rather than from the fix agent's tests, then deleted; (b) per-commit diff review of all 7 shas; (c) focused test run at HEAD; (d) JaCoCo inspection via `scripts/jacoco_cover.py`; (e) wire-format assertions (`ErrorReply.buffer()` bytes) against Redis 7.2 strings.

Spec output at HEAD (`./gradlew :cleanTest :test --tests "io.velo.command.CGroupReReview73Spec"` → `BUILD SUCCESSFUL`, all 7 features green):

```
73.2 bare COMMAND -> MultiBulkReply of 6 (registry=6)
73.2 command count -> 6 ; command info get -> MultiBulkReply ; command docs -> MultiBulkReply ; command zzz -> ErrorReply{message='syntax error'}
73.4 replce -> ErrorReply{message='syntax error'}; replace whatever -> ErrorReply{message='syntax error'}; db -> ErrorReply{message='syntax error'}; db nonsense -> ErrorReply{message='not integer'}; db 5 -> DB index is out of range; db -1 -> DB index is out of range
73.4 db 0 -> REPLY_1; REPLACE DB 0 -> REPLY_1; no option -> REPLY_1; replace -> REPLY_1
73.3 'copy a a' -> source and destination objects are the same ; 'copy a a replace' -> source and destination objects are the same ; 'copy a a garbage' -> ErrorReply{message='syntax error'}
73.1 null-message exception -> RuntimeException: copy failed: CannotReadException
73.1 own-message exception -> boom
73.1 happy path -> REPLY_0
73.6 off -> EmptyReply; maybe -> ErrorReply{message='syntax error'}; '' -> ErrorReply{message='syntax error'}; SKIP -> EmptyReply; ON -> OKReply
73.7 space / newline / 0x7F / 0x80 -> Client names cannot contain spaces, newlines or special characters.
73.7 'abc!~' -> OKReply; getname -> abc!~; setname '' -> OKReply; getname -> NilReply
73.5 legacy kill of another -> OKReply ; legacy kill, no match -> ErrorReply 'No such client' ; legacy self kill -> OKReply
73.5 filter kill -> IntegerReply(1) ; filter self -> IntegerReply(0)
```

### Per-commit verdict

| Bug | Commit | Failing-test evidence (red) | Focused test (green) | JaCoCo | Re-review verdict |
|---|---|---|---|---|---|
| 73.2 | `2f91f43b` | verified: reverting only `CGroup.java` to `2f91f43b^` and running `CGroupTest.test command` → `BUILD FAILED` | pass | `command()` delegation executed | **ACCEPTED** |
| 73.6 | `686c5914` | red state reproduced in the round-1 spec on `0accc625` (`client reply maybe -> OKReply`) | pass | lines 241-257: 11/11, 5/5 branch lines | **ACCEPTED** |
| 73.4 | `28fced74` | red state reproduced in the round-1 spec on `0accc625` (5 malformed forms all returned `REPLY_1`) | pass, **but coverage regression** (below) | option scan branches all covered | **ACCEPTED WITH ACTION** |
| 73.3 | `30f31f95` | red state reproduced in the round-1 spec on `0accc625` (`copy a a -> REPLY_0`) | pass | line 623 `All 2 branches covered` | **ACCEPTED** |
| 73.7 | `3dc99870` | red state reproduced in the round-1 spec on `0accc625` (`setname "a b" -> OKReply`) | pass | lines 240-258: 11/11, 5/5 branch lines | **ACCEPTED** |
| 73.5 | `1189e147` | red state reproduced in the round-1 spec on `0accc625` (integer replies + self preserved) | pass | lines 557-558 `All 2 branches covered` | **ACCEPTED**, one residual risk |
| 73.1 | `df89638e` | red state reproduced in the round-1 spec on `0accc625` (`promise complete after 1s = false`, eventloop "Fatal error in CGroup$$Lambda") | pass | lines 655-669 covered incl. both ternary arms | **ACCEPTED** |

Focused test run at HEAD:

```
./gradlew :cleanTest :test --tests "io.velo.command.CGroupTest" --tests "io.velo.command.CommandCommandTest" --tests "io.velo.script.ScriptHandleTest"
BUILD SUCCESSFUL in 15s
TEST-io.velo.command.CGroupTest.xml:         tests="11" skipped="0" failures="0" errors="0"
TEST-io.velo.command.CommandCommandTest.xml: tests="7"  skipped="0" failures="0" errors="0"
TEST-io.velo.script.ScriptHandleTest.xml:    tests="1"  skipped="0" failures="0" errors="0"
```

Matches the Fix Log's final sweep claim (11/11 + 7/7). No other test source in the repo references `copy` / `client setname` / `client reply` / `client kill` (grep over `src/test/groovy` + `dyn/test`), so the blast radius of these 7 commits is contained to `CGroupTest` and the dyn command tests — all green.

### Per-bug correctness review

- **73.1** — `try/catch` wraps the whole lambda body; both success completions are unchanged; the catch completes the promise exceptionally, which `transferAsyncReply` (`MultiWorkerServer.java:667-683`) maps to an error reply. The message fallback required by my review note is present and both arms verified (`copy failed: CannotReadException` and the original `boom`). The happy cross-worker path still completes with `:0`/`:1`. One thing the fix correctly did *not* do: leave `ReplyMode`-style "swallow and continue" semantics in place. Accepted.
- **73.2** — the guard is removed rather than relaxed, which is safe: `handle()` only reaches `command()` when `"command".equals(cmd)`, so `data[0]` always exists and the script's own `data.length == 1 → info()` is the sole bare path. Verified that `COMMAND COUNT / INFO / DOCS / <unknown>` still behave, and that `COMMAND INFO get` returns a 1-element array. Accepted.
- **73.4** — option scan is case-insensitive, consumes the `DB` value via `data[++i]` (loop counter and body increment agree, so the value is not re-scanned), rejects dangling `DB` and unknown tokens with `SYNTAX`, non-numeric with `NOT_INTEGER`, and nonzero db with `DB index is out of range` — the message choice recorded in the Response. `DB 0`, `REPLACE DB 0`, `replace`, and no-option forms all still copy. Accepted. See the coverage action below.
- **73.3** — placement is exactly right: after the option loop (so `COPY a a garbage` is `SYNTAX`, matching `db.c` ordering) and before the source lookup (so a missing source still yields the same-object error, like Redis). `Arrays.equals(data[1], data[2])` is byte comparison, not slot comparison — verified that `copy a aX` (distinct key, same slot) still copies. Wire format asserted as `-ERR source and destination objects are the same\r\n`, i.e. no duplicated `ERR` prefix — the correction from my review was applied correctly. Accepted.
- **73.7** — byte loop uses signed comparison, so both `< 0x21` and `>= 0x80` (negative byte) are rejected, matching Redis' `*val < '!' || *val > '~'`. Empty name maps to `null`, and the pre-existing `clientName == null → NilReply` path (`CGroup.java:158`) yields nil, verified. Rejected names leave the previous name untouched. Wire format asserted against the Redis string. Accepted.
- **73.6** — validation happens before `setReplyMode`, so a rejected token cannot mutate the stored mode; verified by asserting the mode is still `on` after `maybe` and `""` were rejected. `ReplyMode.from` is untouched as agreed. Accepted.
- **73.5** — `oldForm` is threaded through the record and the two reply branches are correct (`killed == 0 → ErrorReply("No such client")`, else `OKReply`), with the filter form untouched (verified `addr` filter still answers `IntegerReply(1)` and still skips the caller with `IntegerReply(0)`). `skipMe=false` on the legacy path makes self-kill work, verified by the caller socket actually being removed from the inspector map. Accepted — but see residual risk R1.

### Action item (must be closed before this round is signed off)

**A1 — 73.4 silently dropped JaCoCo coverage of a production branch.** The pre-73.4 test used `copy a b replace_` (expecting `REPLY_0`) to reach the same-worker "destination exists and no REPLACE" path; `28fced74` inverted that case to `SYNTAX`, which is correct, but nothing replaced it. Current state at HEAD:

```
python3 scripts/jacoco_cover.py io.velo.command.CGroup 660 685
  675  ✓ covered
  676  ~ partial       1 of 4 branches missed.
  677  ✗ not-covered        # return IntegerReply.REPLY_0;
```

`CGroup.java:676-677` is the same-worker `if (existCv != null && !replace) return IntegerReply.REPLY_0;` — i.e. `COPY src dst` with an existing destination and no REPLACE, both keys on one slot worker. For contrast, the same branch at the pre-fix commit was fully covered:

```
# at 28fced74^ (CGroup.java + CGroupTest.groovy checked out), :test --tests io.velo.command.CGroupTest → tests="11" failures="0"
python3 scripts/jacoco_cover.py io.velo.command.CGroup 618 632
  625  ✓ covered       All 4 branches covered.
  626  ✓ covered
```

So the branch went from covered to uncovered as a side effect of the fix, not by design. Fix: add one same-worker case to `CGroupTest.'test copy'` (`copy a b` twice — the second one must return `REPLY_0`), then re-check with `scripts/jacoco_cover.py`. This is a test-only change; it does not reopen 73.4.

Coverage claims otherwise verified as accurate: `command()` delegation (73.2), lines 241-257 (73.6), 240-258 (73.7), 557-558 (73.5), 623 (73.3), option-scan branches (73.4), 655-669 including both ternary arms (73.1).

### Residual Risk

- **R1 (new, from the 73.5 fix) — self-kill may close the caller before its `+OK` is written.** With `skipMe=false` on the legacy path, `CLIENT KILL <own ip:port>` now closes the calling socket. `killClientsMatching` does `s.getReactor().submit(() -> s.close())` (`CGroup.java:539`) *during* `handle()`, while the reply is written afterwards by the net worker; there is no `CLIENT CLOSE_AFTER_REPLY` equivalent anywhere in the codebase (grep: no such flag; the only unconditional close loop is the server-stop path at `MultiWorkerServer.java:1283-1287`). Redis explicitly defers the close until after the reply is queued (`networking.c:3242-3243`). Consequence: the client may observe a reset connection instead of `+OK`. Not a fix failure — the fix moved Velo toward Redis — but a follow-up is warranted: track `closeThisClient` in `killClientsMatching` and close after the reply is enqueued. **Owner disposition (after re-review): the delayed close is approved and is to be implemented** — 73.5 itself stays accepted/closed, R1 is tracked as its own follow-up item in this doc and re-reviewed the same way. Required shape: thread a `closeThisClient` flag out of `killClientsMatching` (Redis sets it in the kill loop, `networking.c:3228-3231`) and close the socket only after the reply has been queued, rather than inside the reactor task submitted from `handle()`; the flag cannot be applied to sockets other than the caller, because for those the reply is irrelevant.
  - **Closure (AI agent 1):** fixed in `9f06178c` (`fix: close self-killed client after reply written`), TDD — RED: the self-kill case rewritten to the CLOSE_AFTER_REPLY contract failed on `1189e147` (socket already removed within 200ms of the kill); GREEN: `killClientsMatching` sets `closeAfterReply` on the caller instead of submitting a reactor close (`CGroup.java:539`, both arms covered), and `SocketInspector.onWrite` closes the socket after the reply bytes were handed to the channel (`SocketInspector.java:827-832`, both arms covered; ActiveJ `TcpSocket` bytecode inspected to confirm `Inspector.onWrite` runs after `SocketChannel.write` returns). Applied to both kill forms (legacy and filter), matching Redis's `close_this_client`. The SKIPME-no case was updated to the same deterministic contract (registered until the write hook fires). Edge: with `CLIENT REPLY OFF` the suppressed reply may never invoke `onWrite`, leaving the flagged socket open until the client disconnects — Redis has the analogous flag-with-suppressed-reply corner; accepted as residual.
- **Action item A1 (coverage regression on the same-worker `COPY` branch)** — **closed** in `94db1fa4` (`test: cover same-worker copy dst-exists branch again`): same-worker `copy a b` twice case added (second returns `:0`); `CGroup.java:676-677` back to `All 4 branches covered` / covered. Test-only, does not reopen 73.4.
- **"Transient gradle failures" are actually a heap problem, not flakiness.** The fix log records repeated first-run failures "passing on immediate rerun with unchanged sources". The reviewer reproduced it and captured the cause: `:compileTestGroovy FAILED ... > Java heap space` (observed 3 times in this re-review). `gradle.properties` sets only `org.gradle.java.home`; no `org.gradle.jvmargs`, so Groovy compilation runs in the Gradle daemon with the default heap while the repo compiles `src/test/groovy` + `dyn/test` together (`build.gradle:34-35`). Suggested follow-up: set `org.gradle.jvmargs=-Xmx4g` (or raise the daemon heap) so green claims do not depend on a lucky rerun. Not part of bug 73. *(Agent 1 note: reproduced again twice during the A1/R1 work — same `:compileTestGroovy` first-fail-then-pass pattern; the heap fix is recommended to the repo owner.)*
- **Sibling unguarded `asyncExecute` lambdas** (carried over from round 1): `SGroup.java:2031` (SMOVE cross-slot, identical hang), `ZGroup.java:1372` (promise pre-completed on the calling thread → wrong success reply, lost dst write), `BGroup.java:1154` (`recheckBlockingList`, blocked client never woken). Belong to the owning modules' next rounds; cross-referenced from 73.1. Note the 73.1 fix pattern (try/catch + `finalPromise.setException` + non-null message) is the template for all three.
- **Unchanged from round 1:** minor CLIENT KILL filter-form validation divergences (`USER <unknown-user>` → `:0`, `ID 0`/negative accepted); CLIENT LIST `TYPE`/`ID` filters unsupported; bare `COMMAND` now returns the full array with no ACL/cost guard (Redis-equivalent, still unbounded).


### Re-review round 2 (AI agent 2) — follow-ups A1 and R1

Reviewed: `94db1fa4` (`test: cover same-worker copy dst-exists branch again`) and `9f06178c` (`fix: close self-killed client after reply written`), docs commit `66df3744`.

**A1 — ACCEPTED.** The same-worker `copy a b` case (second call returns `REPLY_0`) is added to `CGroupTest.'test copy'`. Re-measured at HEAD:

```
python3 scripts/jacoco_cover.py io.velo.command.CGroup 660 685
  675  ✓ covered
  676  ✓ covered       All 4 branches covered.
  677  ✓ covered
```

`CGroup.java:676-677` is restored to fully covered. A1 is closed.

**R1 (`9f06178c`) — REJECTED, new defect in the follow-up fix.** The intent is right and the mechanism is a faithful port of Redis `CLIENT CLOSE_AFTER_REPLY`, but the close is executed on the wrong thread, and in any deployment with distinct slot and net workers (`slotWorkers > 1 || netWorkers > 1`, i.e. exactly the sharded setup Velo exists for) it throws and leaks a counter.

Mechanism — `CGroup.java:539-546` sets the flag instead of closing:

```java
if (s == socket) {
    SocketInspector.createUserDataIfNotSet(s).setCloseAfterReply(true);
    killed++;
    continue;
}
```

and `SocketInspector.java:826-832` performs the close from the `onWrite` hook:

```java
if (veloUserData.isCloseAfterReply()) {
    veloUserData.setCloseAfterReply(false);
    socket.close();
}
```

Why that is the wrong thread:

1. `CLIENT KILL` parses to **no keys** (`CGroup.parseSlots` returns an empty list for non-`copy` commands), so `Request.getSingleSlot()` returns `SLOT_CAN_HANDLE_BY_ANY_WORKER` (`Request.java:213-214`) and `handleRequest` dispatches through `getByteBufPromiseByOtherEventloop(..., slotWorkerEventloopArray[i])` (`MultiWorkerServer.java:599-608`). The reply promise is therefore completed **on a slot worker thread**, and the reactive pipeline continues on that thread: `.streamTo(ChannelConsumers.ofSocket(socket))` (`MultiWorkerServer.java:845-848`) → `ChannelConsumers.ofSocket` = `ofAsyncConsumer(socket::write, socket)` → `TcpSocket.write` → `doWrite` → `inspector.onWrite(...)` (`activej TcpSocket.java:444-531`).
2. Velo's own `SocketInspector.onWrite` already documents and handles this: `// when async reply, slot eventloop trigger write` — it falls back to `getSlotThreadLocalIndexByCurrentThread()` when `getNetThreadLocalIndexByCurrentThread()` is `-1` (`SocketInspector.java:814-820`). So slot-thread `onWrite` is a first-class, expected case, not an edge case.
3. `socket.close()` → `AsyncCloseable.close()` → `TcpSocket.closeEx` → `doClose()` → `reactor.closeChannel(...)` **and `inspector.onDisconnect(this)`** — on that same slot worker thread.
4. `SocketInspector.onDisconnect` ends with an unguarded net-worker index:

```java
var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
connectedClientCountArray[threadIndex]--;
```

`getNetThreadLocalIndexByCurrentThread()` returns `-1` for a thread that is not a net worker (`MultiWorkerServer.java:1875-1882`), so this is `connectedClientCountArray[-1]--`.

Reproduced (temporary probe `CGroupR1CloseThreadProbeSpec`, run against HEAD, then deleted — the same call `onWrite` makes on the slot-worker path, isolated from the rest of `onDisconnect`):

```
R1 thread probe: onDisconnect(non-net-worker) -> java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 1
R1 thread probe: onDisconnect(net-worker)     -> thrown=null connectedClientCountArray=[0]
```

Why the added tests miss it: both new cases invoke the hook **inside `fixtureEventloop.execute { ... }`**, i.e. on a thread that the fixture has registered in `netWorkerThreadIds` (`clientFixture()` sets `STATIC_GLOBAL_V.netWorkerThreadIds = [eventloop.getEventloopThread().threadId()]`). The tests therefore prove the mechanism works on a net worker — the one thread where it is never used in a multi-worker deployment. They also hand-call `inspector.onWrite(socket, ByteBuf.empty(), 1)` with a synthetic buffer, so they do not exercise the real write path or assert that the reply bytes precede the close.

Severity/impact: `doClose()` closes the channel *before* calling the inspector, so the `+OK` still reaches the client (that part of the intent is achieved) — but the exception then propagates out of `onWrite` → `doWrite` → `write()` → the reply pipeline, `connectedClientCountArray` is never decremented (per-worker connected-client drift, `INFO`/monitoring wrong), and the stack trace is logged per self-kill. In the single-worker configuration (`slotWorkers == 1 && netWorkers == 1`, where `isReuseNetWorkerEventloop` makes the slot worker *be* the net worker — `MultiWorkerServer.java:1041-1049`) the bug is invisible, which is why it survives a local run.

Secondary issues in the same change, to fold into the fix:

- **Leak when no reply is ever written.** The flag is only consumed by `onWrite`. `CLIENT REPLY OFF` + `CLIENT KILL <own addr>` produces no write (`MultiWorkerServer.java:576-578` returns `ByteBuf.empty()` for `replyMode != on`), so `onWrite` never fires and the connection is never closed. Redis closes regardless of reply mode.
- **Pipelined commands.** `onWrite` closes on the *first* write that happens after the flag is set, which for a pipelined `CLIENT KILL <self>; PING` may be the kill's `+OK` — the pending `+PONG` is then discarded (`closeEx` recycles `writeBuf`). Redis flushes the whole output buffer before closing (`networking.c` freeClient path), so all pipelined replies are delivered.
- **Field visibility.** `closeAfterReply` is a plain (non-`volatile`) field written on a slot worker and read on the net worker in the cases where the write crosses threads; no happens-before edge is established for it. Make it `volatile` (or `AtomicBoolean`).
- **Partial writes.** `onWrite` fires even when `channel.write` wrote fewer bytes than the buffer, and the close then discards the remainder. Harmless for the 5-byte `+OK`, but the close belongs after `onWriteReady()` drained `writeBuf`, not on first attempt.

Suggested fix direction (minimal, keeps the approved design): in `SocketInspector.onWrite`, do not close inline on a foreign thread — instead `socket.getReactor().submit(() -> socket.close())` (net workers are already eventloops, so `submit` runs inline when already on the reactor), or gate the close on `getNetThreadLocalIndexByCurrentThread() != -1`. Independently, make `SocketInspector.onDisconnect` tolerate non-net threads the same way `onRead`/`onWrite` already do (slot-thread fallback, skip the counter otherwise), so no future caller can trip it. A regression test must invoke the hook from a thread that is **not** in `netWorkerThreadIds` — that is the case that currently throws.

Fix Log rows for A1/R1 should be marked rejected-pending-rework, not closed: `66df3744` records R1 as closed on a test that only exercises the net-worker thread.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 73.1 | Cross-slot COPY async lambda lacks exception handling → client hang | High | Correctness/Robustness | `REFINED` | fixed (`df89638e`); sibling sites SGroup:2031/ZGroup:1372/BGroup:1154 deferred to owning rounds |
| 73.2 | Bare `COMMAND` rejected; dyn script branch dead | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`2f91f43b`) |
| 73.3 | COPY source == destination not rejected | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`30f31f95`) |
| 73.4 | COPY ignores unknown options / `DB` value | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`28fced74`); re-review action A1 (coverage regression) closed (`94db1fa4`) |
| 73.5 | Legacy CLIENT KILL: reply type + skipme default | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`1189e147`); R1 close-after-reply follow-up fixed (`9f06178c`) |
| 73.6 | CLIENT REPLY unknown mode accepted as ON | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`686c5914`) |
| 73.7 | CLIENT SETNAME no charset validation / empty-name clear | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`3dc99870`) |

*(Fix order per Review Feedback "Recommended fix order": 73.2 → 73.6 → 73.4 → 73.3 → 73.7 → 73.5 → 73.1, one commit each. 73.1 fixed in `df89638e` — see row above the fold.)*
