# Bug 83 — MGroup commands review (Round 1)

**Module:** `io.velo.command.MGroup` (all `M*` commands: `manage`, `mget`, `migrate`, `move`, `mset`, `msetnx`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 8 findings, none verified yet
**Review branch:** `review/mgroup`
**Reviewed commit:** `4f58f9c5f374f15872281c78ea1a9779c17f0059` (main)

> Delta declaration: this is the **first** round whose target is the class `MGroup` itself. Earlier
> rounds that touched `M*` commands are **not** re-reported here:
> - `doc/bug_reviews/bug_39_string_commands_review_round_1.md` / `round_2` (string module) — `SET`
>   family. Its "lossy `SET` number normalisation" follow-up (`BaseCommand.set()` stores a
>   numeric-looking value as a number type, so `MSET k 007` stores `7`) is a **velo-wide**
>   `SET`/`MSET` behaviour, already tracked as a follow-up in bug 74, and is **not** re-reported
>   here (see *Notes / non-findings*).
> - `doc/bug_reviews/bug_74_dgroup_commands_review_round_1.md` (DGroup) — `DUMP`/`DEL`/`DECR*`;
>   `MGroup.migrate()` reuses `DGroup.dumpBytes()`, whose `null` / `TYPE_NOT_SUPPORT` returns are
>   analysed in 83.6/83.7 below.
> - `doc/bug_reviews/bug_82_lgroup_commands_review_round_1.md` (LGroup) — no `M*` overlap.
>
> Not re-reported (checked, correct, or a product decision): `MOVE` → `ERR not supported`
> (Velo has a single logical DB — product decision), `MGET` answering nil for a non-string key
> (matches Redis), `MGET`/`MSET` argument-count checks (Redis-compatible), the `MANAGE` command
> (implemented in the hot-reloadable Groovy module `dyn/`, out of scope for this round).

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/MGroup.java` — registry block (28-77); `parseSlots()` (98-130);
  `handle()` dispatch (137-164); `mget()` (175-255); `mset()` (257-308); `msetnx()` (310-399);
  `migrate()` (406-590) — option parsing (407-454), Jedis connect/auth/select (456-477),
  local (non-cross-worker) path (482-512), cross-worker path (513-589); `manage()` (592-598)
- `src/main/java/io/velo/BaseCommand.java` — `FromToKeyIndex` / `addToSlotWithKeyHashList()`
  (40-84); `execute()` test entry point (454-500); `slot(byte[],String,int)` (620-650);
  `getCv()` (756-798); `getValueBytesByCv()` (813-851); `set(...)` (1016-1049, 1104-1127);
  `removeDelay()` (1272-1283); `exists()` (1291-1303)
- `src/main/java/io/velo/persist/OneSlot.java` — `asyncRun()` (661-690), `asyncExecute()` (702-717),
  `asyncCall()` (723-745), `checkCurrentThreadId()` (1600-1604), `get()` (1751-1752),
  `removeDelay()` (1958-1959)
- `src/main/java/io/velo/persist/LocalPersist.java` — `bindOneSlotIfInitialized()` (212-222)
  (slot → owning slot-worker thread binding)
- `src/main/java/io/velo/MultiWorkerServer.java` — `handleRequest()` cross-worker detection
  (567-590) and worker routing (620-634)
- `src/main/java/io/velo/decode/Request.java` — `getSingleSlot()` (208-219)
- `src/main/java/io/velo/RequestHandler.java` — `parseSlots()` (190-236); generic
  `catch (Exception)` → `ErrorReply` mapping (566-577)
- `src/main/java/io/velo/command/DGroup.java` — `dumpBytes()` (492-550), `TYPE_NOT_SUPPORT` (552)
- `src/main/java/io/velo/command/SGroup.java` — `set()` length validation (834-848)
- `src/main/java/io/velo/CompressedValue.java` — `KEY_MAX_LENGTH` (99), `VALUE_MAX_LENGTH` (101),
  `NO_EXPIRE` (20)
- `src/main/java/io/velo/repl/incremental/XWalV.java` — `decodeFrom()` key-length check (120-136)
- `src/main/java/io/velo/command/XGroup.java` — replica catch-up decode/apply error path (1884-1930)
- `src/test/groovy/io/velo/command/MGroupTest.groovy` — `test mget` (101-159), `test mset` (161-230),
  `test msetnx` (232-321), `test migrate` (323-393)

Reference docs (ground truth):

- Redis 7.2 `src/cluster.c` (upstream source of truth for `MIGRATE` / `RESTORE` semantics) —
  `migrateCommand()` (6958-7260): option loop (6976-7013), `KEYS` empty-key check (7001-7010),
  `timeout`/`dbid` sanity check (7017-7023), source-key deletion `if (!copy) dbDelete(...)`
  (7197-7200), target-error reply (7190-7196); `restoreCommand()` (6734-6808) — fetched from
  `https://raw.githubusercontent.com/redis/redis/7.2/src/cluster.c`
- Redis 7.2 command reference: `https://redis.io/docs/latest/commands/migrate/`,
  `https://redis.io/docs/latest/commands/msetnx/`, `https://redis.io/docs/latest/commands/mget/`
- `doc/design/04_command_processing_design.md` — command dispatch / slot routing model
- `doc/design/11_multithreading_design.md` — slot-worker thread ownership model
- `AGENTS.md` — build/test/JaCoCo commands
- Live oracle: `redis-server v=7.2.11 sha=d4c381df`; differential transcript saved to
  `doc/bug_reviews/logs/20261110_140500_migrate_differential_redis72_bug_83.log`
- Agent-1 probe transcript (temporary Spock spec, removed after the run):
  `doc/bug_reviews/logs/20261110_140000_mgroup_probe_agent1_bug_83.log`

## Finding 1 (Bug 83.1): `MIGRATE` registers no slot, so it runs on a random slot worker and throws "Thread id not match"

**Severity:** High
**Class:** Concurrency / Correctness
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:98-130 — parseSlots() has no "migrate" branch
    @Override
    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ...
        if ("mset".equals(cmd) || "msetnx".equals(cmd)) { ... }
        return slotWithKeyHashList;   // <- "migrate" falls through with an EMPTY list
    }
```

```java
// src/main/java/io/velo/MultiWorkerServer.java:576-590 — the cross-worker flag needs > 1 slot
        if (!request.isCrossRequestWorker()) {
            if (slotWithKeyHashList != null && slotWithKeyHashList.size() > 1 && !request.isRepl()) {
                int expectRequestWorkerId = -1;
                for (var slotWithKeyHash : slotWithKeyHashList) {
                    int slot = slotWithKeyHash.slot();
                    var expectRequestWorkerIdInner = slot % requestHandlerArray.length;
                    ...
                        request.setCrossRequestWorker(true);
```

```java
// src/main/java/io/velo/MultiWorkerServer.java:620-634 — empty slot list => RANDOM worker
        var firstSlot = request.getSingleSlot();
        if (firstSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER) {
            i = new Random().nextInt(requestHandlerArray.length);
            targetHandler = requestHandlerArray[i];
        }
```

```java
// src/main/java/io/velo/persist/OneSlot.java:1751-1752 + 1600-1604 — the slot's owner thread is enforced
    public BufOrCompressedValue get(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();
...
    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id=" + threadId + ...
```

### Root cause

`MIGRATE` is the only command in `MGroup` whose keys are not known to `parseSlots()`: the key
names live inside the `KEYS` option of the command line, so `parseSlots()` returns an **empty**
slot list for it.

Consequences, in order:

1. `Request.getSingleSlot()` returns `SLOT_CAN_HANDLE_BY_ANY_WORKER` for an empty list, so
   `MultiWorkerServer.handleRequest()` picks a **random** slot worker to run the command on.
2. `request.isCrossRequestWorker()` is only set when the parsed slot list has **more than one**
   entry, so it stays `false` for `MIGRATE`. The cross-worker branch of `migrate()`
   (`MGroup.java:513-589`), which correctly dispatches every slot access through
   `oneSlot.asyncCall(...)` / `oneSlot.asyncExecute(...)`, is therefore **unreachable in
   production** (only a unit test that flips the flag by hand can reach it).
3. The local branch (`MGroup.java:482-512`) touches the migrated keys **directly**:
   `getCv(slotWithKeyHash)` → `BaseCommand.getCv()` (`BaseCommand.java:756-798`) →
   `oneSlot.get(key, bucketIndex, keyHash)` → `checkCurrentThreadId()`, and
   `removeDelay(slotWithKeyHash)` → `oneSlot.removeDelay(...)` → `checkCurrentThreadId()`.
   Both throw `IllegalStateException("Thread id not match, ...")` unless the random worker
   happens to own the key's slot (`slot % slotWorkers`, see `LocalPersist.bindOneSlotIfInitialized()`,
   `LocalPersist.java:212-222`).

Every other command in the class registers its keys, so the request is routed to the worker that
owns the first slot and the cross-worker flag is raised when the keys span workers.

### Trigger / impact

`slotWorkers > 1` (the default `velo.properties` uses `slotWorkers=1`, so a single-worker
deployment hides the bug) and any key whose slot is not owned by the randomly chosen worker:

- `MIGRATE 127.0.0.1 6379 "" 0 1000 KEYS k1` → `ERR Thread id not match, thread id=42,
  thread id protected for safe=43` (the exception is caught by the generic handler in
  `RequestHandler.java:576-580` and turned into an error reply).
- With `slotNumber` slots and `slotWorkers` workers, only ~`1/slotWorkers` of the keys can be
  migrated by a given request; the rest fail. The behaviour is **random per request** because
  the worker is picked with `new Random()`.
- Even in the lucky case where the worker owns the slot, the *source key deletion*
  (`removeDelay`, `MGroup.java:503-505`) happens on a foreign thread for every other key.

### Suggested fix direction

Register the migrated keys in `parseSlots()` so the request is routed to the owning worker and
the cross-worker flag is raised when the keys span workers:

- parse the `MIGRATE` option line in `parseSlots()` the same way `migrate()` does, and add
  `slot(key, slotNumber)` for every key of the `KEYS` option (and for the single-key form),
- the command must then use the **cross-worker** path whenever the keys do not all belong to one
  worker, i.e. keep the `isCrossRequestWorker` branch and make it the only path that touches
  keys (the local path may only be used when the current thread owns every slot involved),
- the cross-worker path must not delete the source key from the promise-completion thread
  (see 83.6).

## Finding 2 (Bug 83.2): `MIGRATE` decides whether to delete the source key from `REPLACE` instead of `COPY`

**Severity:** High
**Class:** Correctness (silent data loss / no data loss)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:417-436 — COPY is parsed and then never used
        boolean isCopy = false;
        boolean isReplace = false;
        ...
            } else if (arg.equalsIgnoreCase("copy")) {
                isCopy = true;
            } else if (arg.equalsIgnoreCase("replace")) {
                isReplace = true;
```

```java
// src/main/java/io/velo/command/MGroup.java:496-506 — the source key is deleted only when REPLACE
                var result = jedisTo.restore(Wal.keyBytes(key), expireAt, dumpBytes, restoreParams);
                if (!result.equals("OK")) { ... continue; }

                count++;
                if (isReplace) {
                    removeDelay(slotWithKeyHash);
                }
```

```java
// src/main/java/io/velo/command/MGroup.java:569-576 — same mistake in the cross-worker path
                            count++;
                            if (isReplaceFinal) {
                                var slotWithKeyHash = one.slotWithKeyHash;
                                var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());
                                oneSlot.asyncExecute(() -> {
                                    removeDelay(slotWithKeyHash);
                                });
                            }
```

Redis 7.2 (`src/cluster.c:7197-7200`) does the opposite of what Velo does:

```c
            if (!copy) {
                /* No COPY option: remove the local key, signal the change. */
                dbDelete(c->db,kv[j]);
```

### Root cause

`COPY` and `REPLACE` are two independent options in Redis:

- `COPY` = *keep* the key in the source after the transfer,
- `REPLACE` = *overwrite* the key on the destination if it already exists.

The source key must be deleted when `COPY` is **absent**, and must be kept when `COPY` is
present — independently of `REPLACE`. Velo gates the deletion on `isReplace` instead, and the
`isCopy` flag it parses is never read anywhere in the class (verified: `isCopy` occurs only at
`MGroup.java:417` and `MGroup.java:434`).

### Trigger / impact

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `MIGRATE host port "" 0 1000 KEYS k` (no `COPY`) | source key **stays** | source key is **deleted** |
| `MIGRATE host port "" 0 1000 COPY KEYS k` | source key stays | source key stays |
| `MIGRATE host port "" 0 1000 COPY REPLACE KEYS k` | source key is **deleted** | source key stays |
| `MIGRATE host port "" 0 1000 REPLACE KEYS k` | source key is deleted | source key is deleted |

- The default (no `COPY`) leaves the data behind on the source: the migration does not free the
  memory on the source node, and the caller cannot tell that the key was not removed. This is
  the case a migration operator cares about most.
- `COPY REPLACE` **deletes the source key although the caller explicitly asked to keep it** —
  irreversible data loss on the source node. `COPY` is the option used by "copy then verify then
  delete" workflows; this silently breaks them.

Reproduced by the agent-1 probe (temporary Spock spec, `MGroup` + `InMemoryGetSet` +
`Mock.prepareCompressedValueList`, live `redis-server 7.2.11` as the destination):

```
PROBE migrate no-copy reply = io.velo.reply.OKReply@7cf66cf9
PROBE migrate no-copy source key still exists = true      <- Redis: false
PROBE migrate no-copy target = value0
PROBE migrate copy reply = io.velo.reply.OKReply@7cf66cf9
PROBE migrate copy source key still exists = true
PROBE migrate copy+replace reply = io.velo.reply.OKReply@7cf66cf9
PROBE migrate copy+replace source key still exists = false <- Redis: true
```

### Suggested fix direction

Gate the source-key deletion on `!isCopy` (and only on a successful restore), in **both** the
local and the cross-worker path. `REPLACE` must only control the `RESTORE ... REPLACE` option
that is already passed to the destination (`MGroup.java:473-477`).

## Finding 3 (Bug 83.3): `MIGRATE` with the `AUTH` option always fails with "syntax error"

**Severity:** High
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:422-450 — the option loop never skips the AUTH arguments
        ArrayList<String> keys = new ArrayList<>();
        for (int i = 6; i < data.length; i++) {
            var arg = new String(data[i]);
            if (arg.equalsIgnoreCase("keys")) {
                ...
            } else if (arg.equalsIgnoreCase("copy")) {
                isCopy = true;
            } else if (arg.equalsIgnoreCase("replace")) {
                isReplace = true;
            } else if (arg.equalsIgnoreCase("auth")) {
                if (i + 2 >= data.length) {
                    return ErrorReply.SYNTAX;
                }
                if (new String(data[i + 2]).equalsIgnoreCase("keys")) {
                    authPassword = new String(data[i + 1]);
                } else {
                    authUsername = new String(data[i + 1]);
                    authPassword = new String(data[i + 2]);
                }
            } else {
                return ErrorReply.SYNTAX;      // <- the password token lands here
            }
        }
```

```java
// src/main/java/io/velo/command/MGroup.java:456-471 — SELECT is issued before AUTH
            jedisTo = new Jedis(host, port, jedisClientConfig);
            jedisTo.select(destinationDb);

            if (authUsername != null) {
                jedisTo.auth(authUsername, authPassword);
            } else if (authPassword != null) {
                jedisTo.auth(authPassword);
            }
```

Redis 7.2 (`src/cluster.c:6976-7013`) consumes the option arguments properly and supports
`AUTH2`:

```c
        } else if (!strcasecmp(c->argv[j]->ptr,"auth")) {
            if (!moreargs) { addReplyErrorObject(c,shared.syntaxerr); return; }
            j++;
            password = c->argv[j]->ptr;
        } else if (!strcasecmp(c->argv[j]->ptr,"auth2")) {
            if (moreargs < 2) { addReplyErrorObject(c,shared.syntaxerr); return; }
            username = c->argv[++j]->ptr;
            password = c->argv[++j]->ptr;
```

and Redis sends `AUTH` **before** `SELECT` to the destination (`src/cluster.c:7085-7110`).

### Root cause

Three independent defects in the option handling:

1. **The loop does not advance past the consumed `AUTH` arguments.** After the `auth` branch
   stores the password, the `for` loop continues with `i + 1`, which is the *password token*
   itself. That token matches none of `keys` / `copy` / `replace` / `auth`, so the loop returns
   `ErrorReply.SYNTAX`. Every `MIGRATE ... AUTH <password> ...` therefore fails, whatever the
   password is.
2. **`AUTH2 <username> <password>` is not supported at all** — `auth2` matches no branch and
   falls into the `else` → `ErrorReply.SYNTAX`.
3. **`SELECT` is issued before `AUTH`** on the destination connection. Redis sends `AUTH`
   first. Against a password-protected destination the `SELECT` is rejected with
   `-NOAUTH Authentication required.`, so the migration cannot work even if 1. and 2. are fixed.
   (The `auth` branch is also unreachable today, so 3. is latent — but it must be fixed together
   with 1./2.)

The `"keys"` heuristic in the `auth` branch (`new String(data[i + 2]).equalsIgnoreCase("keys")`)
is a further symptom: the author tried to distinguish `AUTH <password>` from
`AUTH2 <username> <password>` by peeking at the *next* token, which only works if the option
parser consumed its arguments in the first place.

### Trigger / impact

- `MIGRATE 127.0.0.1 6379 "" 0 1000 AUTH secret KEYS k` → `ERR syntax error`
  (Redis: `OK`, the key is transferred and the source key is deleted).
- `MIGRATE 127.0.0.1 6379 "" 0 1000 AUTH2 default secret KEYS k` → `ERR syntax error`
  (Redis: `OK`).
- With 1./3. fixed, a destination that requires a password still fails, because `SELECT` runs
  before `AUTH`.

Verified by the agent-1 probe against a `redis-server 7.2.11 --requirepass secret` destination:

```
PROBE migrate auth reply = ErrorReply{message='syntax error'}
PROBE migrate auth2 reply = ErrorReply{message='syntax error'}
```

and against live Redis 7.2.11 (same destination, `doc/bug_reviews/logs/20261110_140500_migrate_differential_redis72_bug_83.log`):

```
== 2) AUTH option accepted ==
OK
   -> source after: ''  (empty = source key deleted)
== 3) AUTH2 option accepted ==
OK
   -> source after: ''  (empty = source key deleted)
```

### Suggested fix direction

Rewrite the option parser to consume its arguments the way Redis does (`i` must advance past
the consumed tokens), support `AUTH <password>` and `AUTH2 <username> <password>`, and issue
`AUTH` **before** `SELECT` on the destination connection. The "peek at the next token" heuristic
must go away.

## Finding 4 (Bug 83.4): `MIGRATE` rejects Redis's single-key form

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:406-415 — the key argument is parsed and then dropped
    private Reply migrate() {
        if (data.length < 7) {
            return ErrorReply.FORMAT;
        }

        var host = new String(data[1]);
        var port = Integer.parseInt(new String(data[2]));
        var keyOrBlank = new String(data[3]);      // <- never read again
        var destinationDb = Integer.parseInt(new String(data[4]));
        var timeoutMs = Integer.parseInt(new String(data[5]));
```

```java
// src/main/java/io/velo/command/MGroup.java:452-454 — the KEYS option is mandatory
        if (keys.isEmpty()) {
            return ErrorReply.SYNTAX;
        }
```

Redis 7.2 (`src/cluster.c:6972-6974, 7001-7010`) makes the `KEYS` option optional and
defaults to the `key` argument:

```c
    int first_key = 3; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */
```

### Root cause

`MIGRATE` has two syntaxes in Redis:

- `MIGRATE host port key destination-db timeout [COPY] [REPLACE] [AUTH ...] [KEYS key ...]`
- the `KEYS` option is **optional**; without it the single `key` argument is migrated.

Velo parses the single key into `keyOrBlank` and never uses it, requires `data.length >= 7`
(the shortest Redis form has 6 arguments), and requires a non-empty `KEYS` list. The result is
that the single-key form is rejected with `ERR wrong number of arguments for 'migrate' command`
(the server maps `ErrorReply.FORMAT` → `ErrorReply.WRONG_NUMBER(cmd)` in
`MultiWorkerServer.java:613-615` and `:676-678`).

Velo also does not implement Redis's check that the `key` argument must be empty when the
`KEYS` option is used (`src/cluster.c:7002-7006`).

### Trigger / impact

- `MIGRATE 127.0.0.1 6379 mykey 0 1000` → `ERR wrong number of arguments for 'migrate' command`
  (Redis: `OK`, the key is transferred). The agent-1 probe printed
  `PROBE migrate single key form reply = ErrorReply{message='format'}`.
- Live Redis 7.2.11 accepted the same command (`OK`, source key deleted) — see the differential
  log, section 1.
- `MIGRATE 127.0.0.1 6379 notempty 0 1000 KEYS k` → Velo migrates the `KEYS` list and ignores
  the non-empty key argument; Redis answers
  `ERR When using MIGRATE KEYS option, the key argument must be set to the empty string`.

### Suggested fix direction

Support both syntaxes: when no `KEYS` option is present, migrate the single `key` argument
(`data[3]`), and require it to be empty when the `KEYS` option is present. The `data.length < 7`
guard must become `data.length < 6`, and `keyOrBlank` must actually be used.

## Finding 5 (Bug 83.5): `MSET` / `MSETNX` bypass the key and value length limits that `SET` enforces

**Severity:** Medium
**Class:** Correctness / robustness (replication)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:257-308 — mset() has no length validation
    Reply mset() {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            for (int i = 1, j = 0; i < data.length; i += 2, j++) {
                var valueBytes = data[i + 1];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                set(valueBytes, slotWithKeyHash);      // BaseCommand.set() does not check lengths
            }
            return OKReply.INSTANCE;
        }
```

```java
// src/main/java/io/velo/command/SGroup.java:834-848 — SET does check them
    Reply set(byte[][] dd) {
        ...
        var keyBytes = dd[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        var valueBytes = dd[2];
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }
```

```java
// src/main/java/io/velo/repl/incremental/XWalV.java:132-135 — the replica rejects such a key
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }
```

### Root cause

Velo limits a key to `CompressedValue.KEY_MAX_LENGTH` (256 bytes) and a value to
`CompressedValue.VALUE_MAX_LENGTH` (1 MiB). `SET` enforces both limits
(`SGroup.java:840-848`), and the replication stream enforces the key limit again on the
replica side (`XWalV.decodeFrom()`).

`MGroup.mset()` / `MGroup.msetnx()` call `BaseCommand.set()` directly, which performs **no**
length validation, so `MSET` can create keys and values that `SET` refuses. The over-long key
is then written to the WAL and to the incremental-replication binlog, where the key length is
encoded as a `short` and re-validated by the replica.

### Trigger / impact

- `MSET <257-byte key> v` → `OK` and the key is created (Velo's own limit is 256 bytes);
  the same key through `SET` is rejected with `ERR key too long (max length is 256)`.
- `MSET k <1 MiB + 1 value>` → `OK`; `SET k <same value>` → `ERR value too long (max length is
  1048576)`.
- With a replica attached, the binlog entry for the over-long key is decoded by
  `XWalV.decodeFrom()` → `IllegalStateException("Key length error, key length=300")`. The
  replica's catch-up path catches it and reports
  `Repl slave handle error: decode and apply binlog error, slot=<n>`
  (`XGroup.java:1884-1892`), i.e. the replica **cannot advance its offset** for that slot: one
  `MSET` permanently breaks replication for that slot.

Agent-1 probe (temporary Spock spec):

```
PROBE mset long key reply = io.velo.reply.OKReply@7cf66cf9
PROBE mset long key exists = true
PROBE set long key reply = ErrorReply{message='key too long (max length is 256)'}
PROBE mset big value reply = io.velo.reply.OKReply@7cf66cf9
PROBE set big value reply = ErrorReply{message='value too long (max length is 1048576)'}
```

### Suggested fix direction

Validate every key and value of an `MSET` / `MSETNX` before writing anything (the same
`KEY_MAX_LENGTH` / `VALUE_MAX_LENGTH` checks as `SET`, with the same error replies), and reject
the whole command — not just the offending pair — so that `MSET` stays atomic from the client's
point of view.

## Finding 6 (Bug 83.6): `MSETNX` is not atomic in the cross-worker path — a concurrently created key can be overwritten

**Severity:** Medium
**Class:** Concurrency / Correctness (data loss)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:339-371 — phase 1: the existence check
        ArrayList<Promise<Boolean>> anyKeyExistsPromises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            ...
            var p = oneSlot.asyncCall(() -> {
                for (var one : subList) {
                    if (exists(one.slotWithKeyHash)) {
                        return true;
                    }
                }
                return false;
            });
            anyKeyExistsPromises.add(p);
        }
        ...
            var anyKeyExists = anyKeyExistsPromises.stream().anyMatch(Promise::getResult);
            if (anyKeyExists) {
                finalPromise.set(IntegerReply.REPLY_0);
                return;
            }
```

```java
// src/main/java/io/velo/command/MGroup.java:373-386 — phase 2: the write, a *separate* slot task
            List<Promise<Void>> writePromises = new ArrayList<>();
            for (var entry : groupBySlot.entrySet()) {
                ...
                Promise<Void> p = oneSlot.asyncCall(() -> {
                    for (var one : subList) {
                        set(one.valueBytes, one.slotWithKeyHash);
                    }
                    return null;
                });
                writePromises.add(p);
            }
```

```java
// src/main/java/io/velo/command/MGroup.java:316-329 — the local path is atomic (check + write in one task)
        if (!isCrossRequestWorker) {
            for (int i = 1, j = 0; i < data.length; i += 2, j++) { if (exists(...)) return REPLY_0; }
            for (int i = 1, j = 0; i < data.length; i += 2, j++) { set(...); }
            return IntegerReply.REPLY_1;
        }
```

### Root cause

Redis's `MSETNX` is atomic: "Set multiple keys to multiple values, only if none of the keys
exist" — the existence check and the write are one indivisible step.

The local path of `MGroup.msetnx()` reproduces that (both loops run inside the same slot task,
so no other command can interleave). The cross-worker path splits the command into **two
independent slot tasks**: phase 1 (`asyncCall`, the check) and phase 2 (`asyncCall`, the write).
The two are separated by the completion of phase 1 and the scheduling of phase 2, so any other
command that runs on the same slot in between (a `SET` from another client, a `MSETNX`, an
expiring key, ...) is not observed by phase 2.

The same split exists **within one slot**: two keys of the same `MSETNX` that belong to the same
slot are checked in phase 1 and written in phase 2, i.e. the "none of the keys exist" guarantee
does not hold across the two phases.

### Trigger / impact

With `slotWorkers > 1` (so that the request is flagged as cross-worker):

1. client A: `MSETNX k v` — the check phase finds `k` missing.
2. client B: `SET k other` — runs on `k`'s slot between phase 1 and phase 2.
3. client A's phase 2 overwrites `k` with `v`, and answers `1` (success).

The value written by client B is lost, even though `MSETNX` promised not to touch existing
keys. The same interleaving breaks the "all or nothing" property between the keys of one
`MSETNX` that live in different slots.

Note that this is a *behavioural difference between the two paths of the same command*: the same
`MSETNX` is atomic in a single-worker deployment and is not atomic in a multi-worker one.

### Suggested fix direction

Perform the check and the write of a slot group in a **single** slot task (one `asyncCall` that
first checks all of its keys and then writes them), so that the per-slot decision is atomic.
For the cross-slot case, the whole command must still be serialised per slot group; a full
cross-slot atomicity guarantee requires a coordination mechanism that Velo does not have for
multi-slot commands — the fix should at least remove the intra-slot window and document the
remaining cross-slot limitation.

## Finding 7 (Bug 83.7): `MIGRATE` does not handle a destination-side error — the client can hang and the connection leaks

**Severity:** Medium
**Class:** Concurrency / error handling
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:548-586 — the restore loop runs INSIDE the whenComplete callback
            Promises.all(promises).whenComplete((r, e) -> {
                if (e != null) { ... finalPromise.setException(e); ... return; }

                try {
                    var count = 0;
                    for (var p : promises) {
                        for (var d : p.getResult()) {
                            var one = list.get(d.index);
                            var key = one.slotWithKeyHash.rawKey();
                            var result = jedisTo.restore(Wal.keyBytes(key), d.expireAt, d.dumpBytes, restoreParams);
                            ...
                        }
                    }
                    var rr = count != 0 ? OKReply.INSTANCE : NOKEY_REPLY;
                    finalPromise.set(rr);
                } finally {
                    jedisTo.close();
                    log.info("Close jedis {}:{}", host, port);
                }
            });
```

```java
// src/main/java/io/velo/command/MGroup.java:496-509 — the local path: an exception skips the close()
                var result = jedisTo.restore(Wal.keyBytes(key), expireAt, dumpBytes, restoreParams);
                ...
            }
            log.info("Migrate {} keys to {}:{}", count, host, port);

            jedisTo.close();
```

### Root cause

Two error-handling holes around the destination round trip:

1. **Cross-worker path**: `jedisTo.restore()` throws when the destination answers with an error
   (Jedis raises `JedisDataException` for an error reply, e.g. `BUSYKEY Target key name already
   exists.`). The call sits inside the `Promises.all(...).whenComplete(...)` callback, so the
   exception leaves the callback through the `finally` (the connection is closed) and propagates
   into the promise machinery — but `finalPromise` is **never** completed. The `AsyncReply`
   therefore never resolves and the client waits for a reply that never comes.
2. **Local path**: the same exception escapes `migrate()` before `jedisTo.close()` is reached
   (the `close()` is not in a `finally`), so the Jedis connection/socket is leaked for every
   failed migration.

The rest of the class (`mget`, `mset`, `msetnx`) only computes results inside the callback and
therefore does not have this problem.

### Trigger / impact

- Destination already has the key and `REPLACE` was not given:
  `MIGRATE host port "" 0 1000 KEYS k` → the destination answers `BUSYKEY`. Redis answers the
  client `ERR Target instance replied with error: BUSYKEY Target key name already exists.`
  (and keeps the source key). Velo (local path) leaks the connection and answers with the raw
  Jedis message; Velo (cross-worker path) never answers at all → the client hangs until it
  times out.
- Any other destination-side error (connection reset, `READONLY`, `OOM`, ...) has the same
  effect in the cross-worker path.

Redis 7.2 reference (`src/cluster.c:7186-7196`):

```c
                error_from_target = 1;
                addReplyErrorFormat(c,"Target instance replied with error: %s", errbuf+1);
```

### Suggested fix direction

Wrap the restore loop in its own `try/catch` **inside** the callback and complete
`finalPromise` with an error reply (`finalPromise.setException(e)`), mirroring the `e != null`
branch; put the `jedisTo.close()` in a `finally` in the local path as well; and translate the
destination error into Redis's `Target instance replied with error: <msg>` wording.

## Finding 8 (Bug 83.8): `MIGRATE` argument validation diverges from Redis and leaks exceptions

**Severity:** Low
**Class:** Redis protocol compatibility / error handling
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/MGroup.java:406-415 — raw Integer.parseInt, no range check
        if (data.length < 7) {
            return ErrorReply.FORMAT;
        }

        var host = new String(data[1]);
        var port = Integer.parseInt(new String(data[2]));
        var keyOrBlank = new String(data[3]);
        var destinationDb = Integer.parseInt(new String(data[4]));
        var timeoutMs = Integer.parseInt(new String(data[5]));
```

```java
// src/main/java/io/velo/command/MGroup.java:456-457 — the timeout is used verbatim
        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(timeoutMs).build();
```

### Root cause

- The port, the destination DB and the timeout are parsed with `Integer.parseInt`, which throws
  `NumberFormatException` for a non-numeric argument. The exception escapes the command; the
  generic handler in `RequestHandler.java:576-580` turns it into
  `ERR For input string: "abc"`. Redis answers `ERR value is not an integer or out of range`
  (`getLongFromObjectOrReply` in `src/cluster.c:7017-7021`). The same applies to a value that
  does not fit into an `int` (Redis parses the timeout as a `long`).
- Redis clamps a non-positive timeout to 1000 ms (`if (timeout <= 0) timeout = 1000;`,
  `src/cluster.c:7023`). Velo passes the value through to Jedis, where `0` means "no socket
  timeout" — a hung destination then blocks the Velo worker forever instead of failing after
  one second.
- The destination DB index is not range-checked; Redis lets the destination reject it
  (`ERR DB index is out of range`) — Velo does the same through `jedisTo.select()`, so this part
  is consistent.

### Trigger / impact

- `MIGRATE 127.0.0.1 abc "" 0 1000 KEYS k` → the agent-1 probe aborted with
  `java.lang.NumberFormatException: For input string: "abc"` (in the server this becomes
  `ERR For input string: "abc"` plus an `ERROR` log line). Redis: `ERR value is not an integer
  or out of range`.
- `MIGRATE 127.0.0.1 6379 "" 0 xyz KEYS k` → same divergence.
- `MIGRATE 127.0.0.1 <unreachable> "" 0 0 KEYS k` → Velo waits forever on the socket instead of
  timing out after 1 s.

### Suggested fix direction

Parse the port / DB / timeout with an explicit `try/catch` and answer
`ErrorReply.NOT_INTEGER` (or Redis's exact wording), parse the timeout as a `long`, and clamp
`timeout <= 0` to 1000 ms like Redis does.

## Notes / non-findings

- `MOVE` → `ErrorReply.NOT_SUPPORT` (`MGroup.java:151-153`): Velo has a single logical DB, so
  there is nothing to move the key to. **Product decision**, not a bug — recorded so later
  rounds do not re-investigate it.
- `MGET` answering nil for a key that holds a non-string type (`MGroup.java:186-188`): matches
  Redis ("For every key that does not hold a string value or does not exist, the special value
  nil is returned").
- `MGET` / `MSET` / `MSETNX` arity and pair-count checks (`data.length < 2`, `data.length < 3`,
  `data.length % 2 == 0`): Redis-compatible. `ErrorReply.FORMAT` is mapped to
  `ERR wrong number of arguments for '<cmd>' command` by the server
  (`MultiWorkerServer.java:613-615` and `:676-678`).
- `MSET` storing a numeric-looking value as a number type (`BaseCommand.set()`): `MSET k 007`
  stores `7` and `MGET k` answers `7`. This is the **velo-wide** `SET` behaviour already tracked
  as a follow-up in `doc/bug_reviews/bug_74_dgroup_commands_review_round_1.md` ("lossy `SET`
  number normalisation"), not an `MGroup` defect — **not** re-reported.
- `MSET` / `MSETNX` are not atomic **across** slots (each slot group is written separately):
  this is the general Velo multi-slot limitation (the same is true for multi-key `DEL`,
  `EXISTS`, …) and is only listed as a note; 83.6 covers the *intra*-slot window, which is a
  real difference between the two paths of the same command.
- `MIGRATE` transfers the value with `RESTORE ... ABSTTL` and an absolute expiry
  (`MGroup.java:473-477`, `MGroup.java:496`), while Redis sends a **relative** TTL
  (`src/cluster.c:7100-7113`). The absolute form is semantically correct (the key expires at the
  same instant on the destination) — **not** a bug.
- `MIGRATE` uses `DGroup.dumpBytes()` (`DGroup.java:492-550`), which returns `null` for an empty
  collection and `TYPE_NOT_SUPPORT` (`"-1-1"`) for an unsupported type. The local path skips a
  `null` payload (`MGroup.java:491-494`) but the cross-worker path does not
  (`MGroup.java:536-537`) and it never checks `TYPE_NOT_SUPPORT` — both are real defects, but
  they are only reachable on the cross-worker path, which is unreachable in production today
  (see 83.1). They are therefore folded into 83.1's fix direction and **not** raised as
  separate findings in this round; a later round must re-check them once 83.1 makes the
  cross-worker path reachable.
- `MANAGE` (`MGroup.java:592-598`) delegates to the hot-reloadable Groovy scripts
  (`dyn/src/io/velo/script/ManageCommandHandle.groovy`) — out of scope for this round (the
  `dyn` module has its own review).
- Binary (non-UTF-8) key names: Velo's key model is `String`-based end to end
  (`Wal.keyBytes()` / `Wal.keyString()`), so `MIGRATE`'s `new String(data[i])` is not an
  `MGroup`-specific defect. **Not a bug** for this class.

## Reviewer checklist (for AI agent 2)

- Finding 83.1: **live multi-worker reproduction** — start Velo with `slotWorkers=2` (or more)
  and `slotNumber>1` (see `src/main/resources/velo.properties`), then
  `MIGRATE 127.0.0.1 <port> "" 0 1000 KEYS <key>` for keys in several slots. Expect
  `ERR Thread id not match, thread id=..., thread id protected for safe=...`
  (`OneSlot.checkCurrentThreadId()`), randomly, depending on the worker that
  `MultiWorkerServer.handleRequest()` picks. Confirm statically that
  `MGroup.parseSlots("migrate", ...)` returns an empty list, that
  `Request.getSingleSlot()` therefore returns `SLOT_CAN_HANDLE_BY_ANY_WORKER`, and that
  `request.setCrossRequestWorker(true)` (the only place that sets the flag) is guarded by
  `slotWithKeyHashList.size() > 1`. Also confirm that `MGroup.migrate()`'s cross-worker branch
  is therefore unreachable without a hand-set flag.
- Finding 83.2: **live Redis 7.2 differential** — source `redis-server` on port A, destination
  `redis-server` on port B. (a) `SET k v` on A; `MIGRATE 127.0.0.1 B "" 0 1000 KEYS k` on A;
  `GET k` on A → Redis: empty (deleted); Velo: still `v`. (b) `SET k2 v` on A;
  `MIGRATE 127.0.0.1 B "" 0 1000 COPY REPLACE KEYS k2` on A → Redis: `k2` still `v` on A;
  Velo: `k2` gone. Velo side: a Spock temp spec with `MGroup` + `InMemoryGetSet` +
  `Mock.prepareCompressedValueList` and a real `redis-server` destination (harness:
  `MGroupTest.groovy:323-393`), asserting `inMemoryGetSet.getBuf((short) 0, 'k', 0, 0L)`.
- Finding 83.3: `MIGRATE 127.0.0.1 B "" 0 1000 AUTH <pw> KEYS k` and
  `MIGRATE 127.0.0.1 B "" 0 1000 AUTH2 default <pw> KEYS k` against a
  `redis-server --requirepass <pw>` destination. Redis: `OK`; Velo: `ERR syntax error`. Also
  confirm statically that the option loop does not advance `i` past the consumed `AUTH`
  arguments, and that `jedisTo.select()` is called before `jedisTo.auth()`.
- Finding 83.4: `MIGRATE 127.0.0.1 B mykey 0 1000` (single-key form, no `KEYS`). Redis: `OK`;
  Velo: `ERR wrong number of arguments for 'migrate' command`. Plus
  `MIGRATE 127.0.0.1 B notempty 0 1000 KEYS k` → Redis:
  `ERR When using MIGRATE KEYS option, the key argument must be set to the empty string`.
- Finding 83.5: `MSET <257-byte key> v` and `MSET k <1 MiB + 1 value>` → Velo: `OK`;
  `SET` with the same arguments → `ERR key too long (max length is 256)` /
  `ERR value too long (max length is 1048576)`. Replication impact: attach a replica and check
  that the binlog entry for the over-long key makes the replica report
  `Repl slave handle error: decode and apply binlog error` (`XWalV.decodeFrom()`,
  `XWalV.java:132-135`).
- Finding 83.6: **concurrency** — in a `slotWorkers>1` deployment, interleave
  `MSETNX k v` (client A) with `SET k other` (client B) on the same key; assert that A never
  overwrites a key that existed at the time of its write (Redis's `MSETNX` is atomic). A
  Spock temp spec can force the window by hooking the slot task: run the check phase, then
  `SET` the key from the test thread, then let the write phase run.
- Finding 83.7: destination already has the key and `REPLACE` was not given →
  `MIGRATE 127.0.0.1 B "" 0 1000 KEYS k` → the destination answers `BUSYKEY`. Redis:
  `ERR Target instance replied with error: BUSYKEY Target key name already exists.`;
  Velo cross-worker path: **no reply at all** (assert the `AsyncReply`'s promise is never
  completed — e.g. `settablePromise.getResult()` times out); Velo local path: the Jedis
  connection is leaked (assert `jedisTo.close()` is not reached).
- Finding 83.8: `MIGRATE 127.0.0.1 abc "" 0 1000 KEYS k` → Velo: a `NumberFormatException`
  escapes the command (the server turns it into `ERR For input string: "abc"`); Redis:
  `ERR value is not an integer or out of range`. Same for a non-numeric timeout and DB.
  Also check the `timeout <= 0` clamp (Redis: 1000 ms).
- Cross-checks (must stay green): `MGroupTest` as a whole; `DGroupTest` (DUMP is reused by
  `MIGRATE`); and the live differential transcripts in
  `doc/bug_reviews/logs/20261110_140000_mgroup_probe_agent1_bug_83.log` and
  `doc/bug_reviews/logs/20261110_140500_migrate_differential_redis72_bug_83.log`.

## Review Feedback (AI agent 2, round 1)

**Method:**

| Finding | Verdict | Evidence |
|---|---|---|
| 83.1 | | |
| 83.2 | | |
| 83.3 | | |
| 83.4 | | |
| 83.5 | | |
| 83.6 | | |
| 83.7 | | |
| 83.8 | | |

### Additional observations

### Recommended fix order (for the fix agent)

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 83.1 | | | | |
| 83.2 | | | | |
| 83.3 | | | | |
| 83.4 | | | | |
| 83.5 | | | | |
| 83.6 | | | | |
| 83.7 | | | | |
| 83.8 | | | | |

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 83.1 | `MIGRATE` registers no slot → wrong slot worker → "Thread id not match" | High | Concurrency / Correctness | | |
| 83.2 | `MIGRATE` uses `REPLACE` instead of `COPY` to decide the source-key deletion | High | Correctness | | |
| 83.3 | `MIGRATE ... AUTH ...` always fails with "syntax error"; no `AUTH2`; `SELECT` before `AUTH` | High | Redis protocol compatibility | | |
| 83.4 | `MIGRATE` rejects Redis's single-key form | Medium | Redis protocol compatibility | | |
| 83.5 | `MSET`/`MSETNX` bypass the key/value length limits → breaks replication | Medium | Correctness / robustness | | |
| 83.6 | `MSETNX` is not atomic in the cross-worker path (check and write are two tasks) | Medium | Concurrency / Correctness | | |
| 83.7 | `MIGRATE` does not handle a destination error → client hangs / connection leaks | Medium | Concurrency / error handling | | |
| 83.8 | `MIGRATE` numeric-argument validation diverges from Redis | Low | Redis protocol compatibility | | |
