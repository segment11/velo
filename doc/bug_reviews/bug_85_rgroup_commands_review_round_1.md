# Bug 85 — RGroup commands review (Round 1)

**Module:** `io.velo.command.RGroup` (all `R*` commands: `randomkey`, `readonly`, `readwrite`, `rename`,
`renamenx`, `replicaof`, `restore`, `role`, `rpop`, `rpoplpush`, `rpush`, `rpushx`) plus the shared
`move()` / `moveBlock()` helpers that `LGroup.lmove()` delegates to
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** round 1 closed — re-review (agent 2) **accepted all 13 fix commits**; 85.15 (the other command groups answer a nil for an unimplemented command) was filed during the fix phase and fixed in `16b96e12`, the re-review follow ups are fixed in `4943c4c0`, `618f7acc`, `c37b1bde` and `71145b1a`; 85.2 won't fix (latent, production safe); 85.13 partly fixed (the `RESTORE` data loss half; the `RENAME` half needs a product decision); see Residual Risk for the open follow-ups
**Review branch:** `review/rgroup`
**Reviewed commit:** `bb7fb2fa8a3991162bf75830eb86ba99fffd4ff9` (main, merge of `review/pgroup`)

> Delta declaration: round 1 — no prior round covered the class `RGroup`. The following adjacent reports were
> read and are **not** re-reported here: `bug_82_lgroup_commands_review_round_1.md` (`LGroup` list commands),
> `bug_81_ggroup_lgroup_commands_review_round_1.md`, `bug_83_mgroup_commands_review_round_1.md`,
> `bug_84_pgroup_commands_review_round_1.md`. `RGroup.move()`/`moveBlock()` were read during the bug 82 round
> but were **not** filed as findings there (bug 82 finding list covers `LGroup` only); the cross-worker
> destination read in `move()` is therefore first reported here as **85.1**.

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/RGroup.java` — command registry block (26-123); constructor (130-132);
  `parseSlots()` (142-180); `handle()` dispatch (187-248); `randomkey()` (250-298); `rename()` (300-360);
  `restore()` (367-498) incl. the `RDBCallback` (446-492); `role()` (500-556); `moveDstCallback()` (558-583);
  `move()` (585-633); `doBlockWhenMove()` (635-657); `moveBlock()` (659-708); `rpoplpush()` (710-730)
- `src/main/java/io/velo/BaseCommand.java` — `execute()` test entry point (454-500); `slot()` (620-650);
  `getCv()` (756-798); `getValueBytesByCv()` (813-851); `setCv()` (972-999); `set()` (1037-1222);
  `putToOneSlot()` (1237-1247); `remove()` (1255-1265); `removeDelay()` (1272-1283); `exists()` (1291-1303);
  `from()` (362-384)
- `src/main/java/io/velo/persist/OneSlot.java` — `asyncExecute()` (704-717); `asyncCall()` (726-748);
  `checkCurrentThreadId()` (1600-1604); `get()` (1752); `exists()` (1906-1918); `removeDelay()` (1958-1990);
  `put()` (2080-2092)
- `src/main/java/io/velo/persist/LocalPersist.java` — `initSlots()` (387-425); `addOneSlot()` (310-330);
  `fixSlotThreadId()` (630-632); `currentThreadFirstOneSlot()` (639-646)
- `src/main/java/io/velo/persist/Wal.java` — `removeDelay()` (711-717); `V.encode()/V.decode()` (112-165);
  `readBytesToList()` (522-550); `put()` (869-940)
- `src/main/java/io/velo/persist/KeyBucket.java` — `iterate()` (267-286); `getFromOneCell()` (288-300);
  `putTo()` (577-615)
- `src/main/java/io/velo/RequestHandler.java` — `parseSlots()` (190-236); `handle()` (281-582) incl. the
  exception → error-reply mapping (564-580)
- `src/main/java/io/velo/MultiWorkerServer.java` — cross-worker detection (576-590); worker dispatch (597-608)
- `src/main/java/io/velo/command/LGroup.java` — `lmove()` (493-556); `lpop()` (557-627); `lpush()` (845-908);
  `saveRedisList()` (418-430)
- `src/main/java/io/velo/command/SGroup.java` — `saveRedisSet()` (1132-1153)
- `src/main/java/io/velo/command/ZGroup.java` — `saveRedisZSet()` (713-725)
- `src/main/java/io/velo/command/HGroup.java` — `checkSplitKeyType()` (479-486); `saveRedisHashKeys()` (499-513);
  `saveRedisHH()` (525-537); `isUseHH()` (539-547)
- `src/main/java/io/velo/command/VeloRDBImporter.java` — `restore()` (15-37)
- `src/main/java/io/velo/SocketInspector.java` — `isConnectionReadonly()` (129-136);
  `setConnectionReadonly()` (145-150)
- `src/main/java/io/velo/acl/Category.java` — write/read/connection category lists (the `READONLY` gate at
  `MultiWorkerServer.java:563`)
- `src/main/java/io/velo/reply/ErrorReply.java` — error constants (34, 68, 95, 162)
- `src/main/java/io/velo/persist/LocalPersist.java` — `isHashSaveMemberTogether` default (406)
- `src/test/groovy/io/velo/command/RGroupTest.groovy` — whole file (1-759): `test parse slot - single key`
  (22-42), `test parse slot - edge cases` (44-64), `test handle - format errors` (66-86),
  `test handle - readonly readwrite role` (88-106), `test randomkey` (108-155), `test rename` (157-239),
  `test restore` (241-380), `test role` (382-431), `test rpop` (433-446), `test rpoplpush` (448-559),
  `test rpush and rpushx` (561-587), `test move block` (589-758)

Reference docs (ground truth):

- Redis 7.2 upstream source, `restoreCommand()`, `readonlyCommand()`, `readwriteCommand()` —
  `https://raw.githubusercontent.com/redis/redis/7.2/src/cluster.c` (lines 6734-6820 and 7300-7335)
- Redis 7.2 upstream source, `renameGenericCommand()` —
  `https://raw.githubusercontent.com/redis/redis/7.2/src/db.c` (lines 1243-1283)
- Redis command reference: `https://redis.io/docs/latest/commands/restore/`,
  `https://redis.io/docs/latest/commands/randomkey/`, `https://redis.io/docs/latest/commands/rename/`
- `doc/design/04_command_processing_design.md` — "Cross-Slot Behavior" (group keys by slot, submit work to the
  owning slot worker via `OneSlot.asyncCall/asyncExecute`, aggregate the replies)
- `doc/design/11_multithreading_design.md` — thread affinity: "mutable slot state is owned by one slot worker",
  "avoid … direct cross-thread access" (lines 15, 141, 212)
- `doc/redis_command_support.md` — support matrix (`RESTORE` is still marked `[x]` = unsupported, lines 104)
- `AGENTS.md` — build/test/JaCoCo commands, TDD rules
- `doc/write_better_test_cases_for_ai_agents.md`, `doc/how_to_write_high_coverage_test_cases_for_commands.md`

Local scratch reproduction specs used while writing this report (temporary, **deleted** after the reproduction —
their content is inlined in the reviewer checklist below):
`RGroupScratchTest.groovy` (cross-worker rename), `RGroupRestoreScratchTest.groovy` (restore ttl/option
validation), `RGroupMoveScratchTest.groovy` (cross-worker rpoplpush).

Baseline coverage of the module under review (`./gradlew :test --tests "io.velo.command.RGroup"` →
`python3 scripts/jacoco_cover.py io.velo.command.RGroup 1 731`): 365 lines, 339 covered (93%), 8 not covered.
Important: the cross-worker branches of `rename()` (341-357) and `move()`/`moveBlock()` (619-632, 694-707) are
**already covered** by `RGroupTest` — but only through the `InMemoryGetSet` mock (`byPassGetSet`), which
bypasses `OneSlot.checkCurrentThreadId()` entirely. That is why findings 85.1-85.3 are invisible to the current
test suite: the mock cannot reproduce a thread-affinity violation.

---

## Finding 1 (Bug 85.1): `RPOPLPUSH` / `LMOVE` / `BLMOVE` always fail when the two keys live in different slot workers

**Severity:** High
**Class:** Concurrency / Correctness (Redis protocol compatibility — the command is unusable across workers)
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **fixed** (`ffde6e8e`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:596-617 (move())
        var valueBytesSrc = getValueBytesByCv(cvSrc, srcSlotWithKeyHash);
        var rlSrc = RedisList.decode(valueBytesSrc);

        var size = rlSrc.size();
        if (size == 0) {
            return NilReply.INSTANCE;
        }

        // validate destination type before mutating source
        if (!srcSlotWithKeyHash.rawKey().equals(dstSlotWithKeyHash.rawKey())) {
            var cvDst = getCv(dstSlotWithKeyHash);          // <-- line 605: reads the DST slot
            if (cvDst != null && !cvDst.isList()) {          //     from the SOURCE worker thread
                return ErrorReply.WRONG_TYPE;
            }
        }
```
```java
// src/main/java/io/velo/command/RGroup.java:670-684 (moveBlock()) — the same check, same defect
        if (!srcSlotWithKeyHash.rawKey().equals(dstSlotWithKeyHash.rawKey())) {
            var cvDst = getCv(dstSlotWithKeyHash);          // line 680
            if (cvDst != null && !cvDst.isList()) {
                return ErrorReply.WRONG_TYPE;
            }
        }
```
```java
// src/main/java/io/velo/persist/OneSlot.java:1752 + 1600-1604
    public OneSlot.BufOrCompressedValue get(...) { checkCurrentThreadId(); ... }
    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id=" + threadId + ", thread id protected for safe=" + threadIdProtectedForSafe);
        }
    }
```

### Root cause

`move()`/`moveBlock()` run on the worker that owns the **source** slot (`MultiWorkerServer.java:597-608` picks
the handler from the first slot of the request). The destination-slot type pre-check, however, calls
`getCv(dstSlotWithKeyHash)` **before** the `if (!isCrossRequestWorker)` branch, i.e. on the source worker's
thread. `BaseCommand.getCv()` → `OneSlot.get()` → `checkCurrentThreadId()` throws `IllegalStateException` when
the destination slot belongs to another slot worker. The exception is caught by
`RequestHandler.handle()` (line 576-580) and turned into `ERR Thread id not match, thread id=…, thread id
protected for safe=…`.

The destination is only ever touched correctly later, inside `moveDstCallback()`, which is dispatched to the
destination worker (`dstOneSlot.asyncExecute(...)`, lines 630 / 705). The pre-check at line 605/680 is the
only place where the destination slot is read from the wrong thread.

`LGroup.lmove()` (line 553 / 551) delegates to `RGroup.move()`/`moveBlock()`, so `LMOVE` and `BLMOVE` inherit
the same defect.

### Trigger / impact

Trigger: the source and destination keys resolve to slots owned by **different** slot workers — i.e. exactly
the case `request.isCrossRequestWorker == true` (`MultiWorkerServer.java:576-590`). With the default
`slotNumber=1, slotWorkers=1` (`src/main/resources/velo.properties:9-10`) the bug is invisible; with
`slotWorkers >= 2` roughly half of all key pairs land in different workers.

Impact: `RPOPLPUSH` (and `LMOVE`/`BLMOVE`) never succeed for such key pairs — the client always gets an
internal error whose text leaks the thread ids, instead of the moved element. Redis moves the element
atomically and answers the value.

### Suggested fix direction

Do not read the destination slot from the source worker. Two options:
(a) drop the destination type pre-check from the current thread and perform it inside the destination task
(`moveDstCallback()` already decodes the destination there and answers `ErrorReply.WRONG_TYPE`), or
(b) read the destination asynchronously first (`dstOneSlot.asyncCall(() -> getCv(dst))`) and only mutate the
source after the destination type is known.
Option (a) is the smaller diff but changes the failure ordering (the source element would already be removed
when the destination turns out to be a wrong type) — so option (b) is preferred, or move the whole
"remove from source + append to destination" into a single destination-worker task after the source worker has
handed over the element.

---

## Finding 2 (Bug 85.2): cross-worker `RENAME` / `RENAMENX` delete the source key only by accident (the ActiveJ `Promise.ofFuture` bounce) — the fallback path loses it

**Severity:** Low (latent) — downgraded from High after the agent 2 review (the production impact does not occur, see the Response section)
**Class:** Correctness (latent thread-affinity defect + non-atomic rename)
**Status:** `REFINED — agent 2: reproduced only on the non-reactor fallback path; the production reactor path is safe via the Promise.ofFuture bounce, see Review Feedback. Agent 1 accepts the refinement (see Response section); severity High → Low` — **won't fix** (latent, production safe, see the Fix Log)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:335-359 (rename())
        var dstSlot = dstS.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncCall(() -> {                       // runs on the DST worker thread
            if (isNx) {
                var isExist = exists(dstS);
                if (isExist) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return false;
                }
            }

            setCv(srcCv, dstS);
            finalPromise.set(isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE);
            return true;
        }).whenComplete((r, e) -> {                       // <-- also runs on the DST worker thread
            if (r) {
                removeDelay(srcSlotWithKeyHash);            // <-- line 355: mutates the SRC slot from the DST thread
            }
        });

        return asyncReply;
```
```java
// src/main/java/io/velo/BaseCommand.java:1272-1283 (removeDelay)
        var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot);
        if (oneSlot.isReadonly() || localPersist.isRoleTransitionWriteFenced()) {
            throw new ReadonlyException();
        }
        oneSlot.removeDelay(slotWithKeyHash.rawKey, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
```
```java
// src/main/java/io/velo/persist/OneSlot.java:1958-1962
    public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();       // throws when called from a foreign worker thread
```

### Root cause

The source key must be deleted on the worker that owns the source slot. In the cross-worker branch the
deletion is scheduled from the `whenComplete` callback of the **destination** slot's promise, so it executes on
the destination worker's thread. `OneSlot.removeDelay()` → `checkCurrentThreadId()` throws
`IllegalStateException("Thread id not match, …")`. The exception is raised inside a `whenComplete` callback of
a promise whose result is discarded, so it is swallowed: the source key is never deleted, while the client has
already received `+OK` (the reply is set before the deletion).

### Trigger / impact

Trigger: `RENAME`/`RENAMENX` whose source and destination keys are in different slot workers.
Impact: the source key survives, so after `RENAME a b` **both** `a` and `b` hold the value. Any client that
assumes `RENAME` moves the key (e.g. a lock hand-over, a session hand-over) silently duplicates the data, and
the duplicate is not garbage collected because the source TTL is still ticking on the old key.

> **Corrected after the agent 2 review (round 1 Response).** The impact above only materialises on the
> **non-reactor fallback path**. In production the destination promise is created through
> `Promise.ofFuture(...)` (`OneSlot.java:736-737`), and ActiveJ's `Promise.ofCompletionStage` captures the
> **calling** reactor and bounces the completion back to it, so the `whenComplete` callback — and therefore
> `removeDelay(src)` — runs on the **source** worker thread and passes `checkCurrentThreadId()`. The
> production severity is therefore **Low (latent)**, not High. See the Response section for the citations.

### Suggested fix direction

Delete the source key on the source worker explicitly instead of relying on the ActiveJ bounce: capture the
source `OneSlot` and use `srcOneSlot.asyncExecute(() -> removeDelay(src))` (or `asyncCall`) instead of calling
`removeDelay` directly inside the destination's `whenComplete`. The `whenComplete` callback must also not
swallow errors — see 85.3. Making the dispatch explicit also removes the non-atomicity window (today the
`+OK` is sent from the destination task *before* the source removal runs, so a client can still read the
source key right after the reply).

---

## Finding 3 (Bug 85.3): cross-worker `RENAME` / `RENAMENX` never answer the client when the destination write fails (the connection hangs)

**Severity:** High
**Class:** Correctness / Availability (the request promise is never settled)
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **fixed** (`608cb077`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:341-357 (rename())
        dstOneSlot.asyncCall(() -> {
            if (isNx) { ... finalPromise.set(IntegerReply.REPLY_0); return false; }

            setCv(srcCv, dstS);
            finalPromise.set(isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE);
            return true;
        }).whenComplete((r, e) -> {
            if (r) {                       // line 354: unboxes a null Boolean when the task failed
                removeDelay(srcSlotWithKeyHash);
            }
        });
```

### Root cause

Two defects in the same callback:

1. `finalPromise` is only settled on the **success** path of the destination task. If `exists(dstS)` or
   `setCv(srcCv, dstS)` throws (e.g. the destination slot is readonly / write-fenced → `ReadonlyException`, or
   a disk error), the task promise completes exceptionally and `finalPromise` is **never** set — the client
   never gets a reply.
2. `whenComplete((r, e) -> { if (r) … })` ignores `e` and unboxes `r`. When the task failed, `r` is `null`
   → `NullPointerException: Cannot invoke "java.lang.Boolean.booleanValue()" because "r" is null`, thrown
   inside the callback, so the real cause (`ReadonlyException`) is swallowed and no error is logged by the
   command.

### Trigger / impact

Trigger: a cross-worker `RENAME`/`RENAMENX` whose destination write fails — the destination slot is readonly
or write-fenced (failover, role transition, `isRoleTransitionWriteFenced()`), or the WAL write fails.
Impact: the request never completes → the client connection hangs forever (no reply, no timeout), the source
key is not deleted, and the underlying error is invisible. A failover window is exactly when this happens.

### Suggested fix direction

Settle the promise on every path: catch the failure inside the destination task and call
`finalPromise.setException(e)` — `MultiWorkerServer.transferAsyncReply()` (694-710) already maps a rejected
promise to `new ErrorReply(e.getMessage())`, so the client gets an error reply instead of hanging. The
`whenComplete` callback must also be null-safe (`if (Boolean.TRUE.equals(r))`) and must not swallow `e`.
The same pattern must be applied to the `move()`/`moveBlock()` destination dispatch (see the note in 85.1 and
85.12).

---

## Finding 4 (Bug 85.4): `RESTORE` silently drops the TTL for every non-string payload

**Severity:** Medium
**Class:** Correctness (the restored key never expires)
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec + live Redis 7.2.11 differential, see Review Feedback` — **fixed** (`a4f31233`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:446-492 (restore(), the RDBCallback)
            rdbImporter.restore(nettyBuf, new RDBCallback() {
                @Override
                public void onString(byte[] valueBytes) {
                    set(valueBytes, slotWithKeyHash, CompressedValue.NULL_DICT_SEQ, finalExpireAt);  // TTL applied
                }

                @Override
                public void onList(RedisList rl) {
                    LGroup.saveRedisList(rl, slotWithKeyHash, _rGroup, dictMap);      // TTL lost
                }

                @Override
                public void onSet(RedisHashKeys rhk) {
                    SGroup.saveRedisSet(rhk, slotWithKeyHash, _rGroup, dictMap);       // TTL lost
                }

                @Override
                public void onZSet(RedisZSet rz) {
                    ZGroup.saveRedisZSet(rz, slotWithKeyHash, _rGroup, dictMap);      // TTL lost
                }

                @Override
                public void onHash(RedisHH rhh) { ... hGroup.saveRedisHH(rhh, slotWithKeyHash); ... }  // TTL lost
            });
```
```java
// src/main/java/io/velo/command/LGroup.java:429   baseCommand.set(rl.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_LIST);
// src/main/java/io/velo/command/SGroup.java:1152  baseCommand.set(rhk.encode(preferDict), slotWithKeyHash, SP_TYPE_SET, CompressedValue.NO_EXPIRE, ignoreReadonly);
// src/main/java/io/velo/command/ZGroup.java:724   baseCommand.set(rz.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_ZSET);
// src/main/java/io/velo/command/HGroup.java:497/535  set(rhk.encode(preferDict), slotWithKeyHash, SP_TYPE_HASH) / set(rhh.encode(preferDict), slotWithKeyHash, SP_TYPE_HH);
```
(`set(valueBytes, slotWithKeyHash, spType)` defaults to `CompressedValue.NO_EXPIRE`, see
`BaseCommand.java:1025-1027`.)

### Root cause

`restore()` computes `expireAt` from the `ttl` argument (line 418-428) but only forwards it to the
`onString` callback. The collection callbacks delegate to the type-specific `saveRedis*()` helpers, which
have no `expireAt` parameter and therefore always write the value with `NO_EXPIRE`.

### Trigger / impact

Trigger: `RESTORE key <non-zero ttl> <payload>` where the payload is a list, set, sorted set or hash.
Impact: the key is created with **no** expiry. `PTTL`/`TTL` answer `-1` instead of the requested value and
the key lives forever — a memory leak that is invisible to the client, and a divergence from Redis, which
applies the TTL to every restored type (`RESTORE` docs: "If `ttl` is 0 the key is created without any
expire, otherwise the specified expire time (in milliseconds) is set").

### Suggested fix direction

Thread the expiry through the restore path: extend the `saveRedisList/saveRedisSet/saveRedisZSet/saveRedisHH`
helpers with an `expireAt` parameter (they are `static` helpers shared with `LGroup`/`SGroup`/`ZGroup`/
`HGroup`, so the change is cross-module but mechanical), or restore into an in-memory `CompressedValue` and
write it with `setCv()` (which preserves `cv.getExpireAt()`).

---

## Finding 5 (Bug 85.5): `RESTORE` does not enforce the key length limit — an oversized key poisons the WAL group / key bucket

**Severity:** Medium
**Class:** Correctness / Availability (the persisted entry can never be decoded again)
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **fixed** (`c0c5a516`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:367-377 (restore()) — no key length check at all
    private Reply restore() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        // refer kvrocks: cmd_server.cc redis::CommandRestore
        var keyBytes = data[1];
        ...
```
(compare `rename()` line 308-313 and `rpoplpush()` line 719-724, which both check
`keyBytes.length > CompressedValue.KEY_MAX_LENGTH` → `ErrorReply.KEY_TOO_LONG`)

```java
// src/main/java/io/velo/persist/Wal.java:152 (V.decode) and src/main/java/io/velo/persist/KeyBucket.java:291 (getFromOneCell)
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }
```
(`CompressedValue.KEY_MAX_LENGTH = 256`, `src/main/java/io/velo/CompressedValue.java:99`)

### Root cause

`restore()` writes the key through `set()` → `putToOneSlot()` → `OneSlot.put()` → `Wal.put()`. Neither the WAL
write path nor the key-bucket write path validates the key length; only the **read** paths do
(`Wal.V.decode()`, `KeyBucket.getFromOneCell()`), where an over-long key raises
`IllegalStateException("Key length error")`. A key longer than 256 bytes is therefore accepted by `RESTORE`
and persisted, and the resulting WAL group / key bucket can never be decoded again.

### Trigger / impact

Trigger: `RESTORE <key longer than 256 bytes> 0 <payload>`.
Impact: the reply is `+OK` (Redis answers `ERR string exceeds maximum length`), the entry is written to the
WAL, and afterwards every read that has to decode that WAL group (`Wal.readBytesToList()` → `V.decode()`) or
that key bucket (`KeyBucket.getFromOneCell()`) throws `IllegalStateException` — i.e. the failure is not
confined to the offending key but can take the whole WAL group / bucket with it.

### Suggested fix direction

Reject an over-long key up front, like every other key-taking command does:
`if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) return ErrorReply.KEY_TOO_LONG;`
Optionally also harden the persist layer so an over-long key can never be written (a defensive check in
`Wal.put()`/`KeyBucket.putTo()`).

---

## Finding 6 (Bug 85.6): `RESTORE` with a non-numeric TTL raises an uncaught `NumberFormatException`

**Severity:** Medium
**Class:** Redis protocol compatibility / Robustness
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec + live Redis 7.2.11 differential, see Review Feedback` — **fixed** (`05c08552`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:418-428
        long expireAt;
        if (ttlBytes.length == 0 || ttl.equals("0")) {
            // -1 means no expire, different from redis, important!!!
            expireAt = CompressedValue.NO_EXPIRE;
        } else {
            if (absttl) {
                expireAt = Long.parseLong(ttl);            // line 424: may throw NumberFormatException
            } else {
                expireAt = System.currentTimeMillis() + Long.parseLong(ttl);   // line 426: same
            }
        }
```
```java
// src/main/java/io/velo/command/RGroup.java:402 and 410 (idletime / freq) — same problem
                    idleTime = Integer.parseInt(new String(data[i + 1]));
                    freq = Integer.parseInt(new String(data[i + 1]));
```
```java
// src/main/java/io/velo/RequestHandler.java:576-580 — the exception becomes the reply text
            } catch (Exception e) {
                log.error("Request handle error.", e);
                errorReplyCountTotal++;
                return new ErrorReply(e.getMessage());
            }
```

### Root cause

The TTL and the option values are parsed with `Long.parseInt/parseLong` without a `NumberFormatException`
guard. The parse happens **before** the "target key is busy" check (line 433-438), while Redis 7.2 parses the
TTL *after* the busy-key check (`restoreCommand()` in `src/cluster.c`: the busy-key test comes first, the TTL
parse afterwards).

### Trigger / impact

Trigger: `RESTORE key abc <payload>` or `RESTORE key 0 <payload> idletime abc`.
Impact: the client receives `ERR For input string: "abc"` (the raw Java message) instead of Redis's
`ERR value is not an integer or out of range`. The internal exception is also logged at ERROR level for every
such request (log noise / log-flooding vector for an untrusted client). For an existing key the reply is also
the wrong one: Redis answers `BUSYKEY` first, Velo answers the parse error.

### Suggested fix direction

Parse the TTL and the option values with a `try/catch (NumberFormatException)` and answer
`ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE` ("value is not an integer or out of range", already defined in
`ErrorReply.java:77`), and move the TTL parse after the busy-key check to match the Redis ordering.

---

## Finding 7 (Bug 85.7): `RESTORE` accepts a negative TTL and silently ignores unknown options

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec + live Redis 7.2.11 differential, see Review Feedback` — **fixed** (`aebb27dd`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:391-416 (option parsing — the else branch is missing)
        if (data.length > 4) {
            for (int i = 4; i < data.length; i++) {
                var arg = new String(data[i]);
                if (REPLACE.equalsIgnoreCase(arg)) {
                    replace = true;
                } else if (ABSTTL.equalsIgnoreCase(arg)) {
                    absttl = true;
                } else if (IDLETIME.equalsIgnoreCase(arg)) {
                    ...
                } else if (FREQ.equalsIgnoreCase(arg)) {
                    ...
                }
                // no else -> unknown options are silently ignored
            }
        }
```

### Root cause

Redis 7.2 `restoreCommand()` ends the option loop with
`} else { addReplyErrorObject(c, shared.syntaxerr); return; }` — an unknown option is a syntax error. Velo has
no `else` branch, so `RESTORE key 0 payload badoption` is accepted. Redis also rejects a negative TTL with
`ERR Invalid TTL value, must be >= 0`, while Velo computes `expireAt = now + ttl` (a timestamp in the past) and
stores the key as already expired.

Redis additionally refuses `IDLETIME` and `FREQ` in the same command (each requires the other to be unset);
Velo accepts both.

### Trigger / impact

Trigger: `RESTORE key 0 payload badoption` (answer `+OK` instead of `ERR syntax error`), `RESTORE key -1
payload` (answer `+OK` and create an already-expired key instead of `ERR Invalid TTL value, must be >= 0`),
`RESTORE key 0 payload idletime 5 freq 3` (answer `+OK` instead of `ERR syntax error`).
Impact: protocol divergence; a client that relies on the error to detect a malformed command proceeds with a
silently wrong result (e.g. a key that will never expire).

### Suggested fix direction

Add the missing `else` branch answering `ErrorReply.SYNTAX`, reject a negative TTL with
`new ErrorReply("Invalid TTL value, must be >= 0")`, and reject `IDLETIME`+`FREQ` together. The option loop
should also consume the option argument (`i++`) like Redis does (`j++` in the upstream code).

---

## Finding 8 (Bug 85.8): `RANDOMKEY` only inspects one slot and can answer nil while the keyspace is not empty

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **fixed** (`52b95928`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:250-298 (randomkey())
    private Reply randomkey() {
        if (data.length != 1) {
            return ErrorReply.FORMAT;
        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();   // <-- ONE slot only
        ...
        final int maxTryTimes = 10;
        for (int i = 0; i < maxTryTimes; i++) {
            var bucketIndex = random.nextInt(ConfForSlot.global.confBucket.bucketsPerSlot);
            ...
        }

        return NilReply.INSTANCE;
    }
```
```java
// src/main/java/io/velo/persist/LocalPersist.java:639-646
    public OneSlot currentThreadFirstOneSlot() {
        for (var oneSlot : initializedOneSlots()) {
            if (oneSlot.threadIdProtectedForSafe == Thread.currentThread().threadId()) {
                return oneSlot;                 // the FIRST slot owned by this worker
            }
        }
        throw new IllegalStateException("No one slot for current thread");
    }
```

### Root cause

`RANDOMKEY` must return "a random key from the currently selected database"
(`https://redis.io/docs/latest/commands/randomkey/`). Velo restricts the search to the **first** slot owned by
the calling worker, so in a multi-slot deployment (`slotNumber > 1`, the sharded configuration) it can only
ever return a key that lives in that single slot — `1/slotNumber` of the keyspace — and it never looks at the
other workers' slots at all. In addition, the search gives up after 10 random bucket probes, so a keyspace
whose keys are spread over `bucketsPerSlot` buckets (default 262144, `velo.properties:48`) is very likely to
be reported as empty.

### Trigger / impact

Trigger: `slotNumber > 1` (any sharded deployment) or a sparse keyspace.
Impact: `RANDOMKEY` answers `(nil)` although the database is not empty, or returns a key from a single slot
only. Clients that use `RANDOMKEY` to pick a victim key for eviction/sampling (or Redis Sentinel-style
"find any key") get a wrong or missing answer.

### Suggested fix direction

Search across the slots of the current worker (and, if the design allows, across workers via
`LocalPersist.doSthInSlots`), and increase/retry the number of probes (e.g. probe until a key is found or the
total key count of the searched slots is exhausted) instead of giving up after 10 attempts.

---

## Finding 9 (Bug 85.9): `RANDOMKEY` may return an expired key and the random index inside the selected bucket is not honoured

**Severity:** Low
**Class:** Correctness
**Status:** `CONFIRMED (reproduced) — bucket bias reproduced; the expired-key and stale-count NPE sub-claims are inspection, see Review Feedback` — **fixed** (`1ad59c79`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:268-293
            var keyCount = firstOneSlot.getKeyLoader().getKeyCountInBucketIndex(bucketIndex);
            if (keyCount > 0) {
                var skipN = random.nextInt(keyCount);
                final int[] countArray = {0};
                final String[] targetKeyArray = new String[1];

                var keyBuckets = firstOneSlot.getKeyLoader().readKeyBuckets(bucketIndex);
                for (var keyBucket : keyBuckets) {
                    if (keyBucket == null) {
                        continue;
                    }

                    keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
                        if (countArray[0] == skipN) {
                            targetKeyArray[0] = key;     // overwritten again and again, see below
                            return;                     // does NOT stop the iteration
                        }
                        countArray[0]++;
                    });

                    if (targetKeyArray[0] != null) {
                        break;
                    }
                }

                return new BulkReply(targetKeyArray[0]);
            }
```
```java
// src/main/java/io/velo/persist/KeyBucket.java:267-286 — the callback has a void return type, there is no way to stop
    public void iterate(@NotNull IterateCallBack callBack) {
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            ...
            callBack.call(cellHashValue, expireAt, seq, kvBytes.key, kvBytes.valueBytes);
        }
    }
```

### Root cause

Two issues in the same loop:

1. `KeyBucket.iterate()` takes a `void` callback, so the `return;` in the lambda does not stop the iteration.
   `countArray[0]` is not incremented once the target was found, so **every** following key of the bucket
   matches `countArray[0] == skipN` and overwrites `targetKeyArray[0]`. The result is always the *last* key of
   the selected bucket (in cell order), not the `skipN`-th one — the "random" index inside the bucket has no
   effect.
2. The `expireAt` handed to the callback is ignored, so an expired key that is still present in the bucket can
   be returned. `GET` on it then answers `(nil)`, i.e. `RANDOMKEY` returned a key that does not exist.

If the bucket turns out to contain no live key while `keyCount > 0` (a stale count), `targetKeyArray[0]`
stays `null` and `new BulkReply(targetKeyArray[0])` throws a `NullPointerException`
(`BulkReply(@NotNull String content)` → `content.getBytes()`), which the request handler turns into
`ERR null`.

### Trigger / impact

Trigger: any `RANDOMKEY` that resolves through the key-bucket path (i.e. the key is not in the LRU).
Impact: the returned key is biased (always the last key of the bucket) and may be an expired/non-existent key;
in the stale-count case the client receives `ERR null` instead of a key.

### Suggested fix direction

Count the live keys (`expireAt` in the future) and pick the `skipN`-th one with a counter that stops the
selection (`if (found) return;` inside a counting lambda that only increments while the target has not been
chosen); skip expired entries; and guard against a `null` target (`return NilReply.INSTANCE`).

---

## Finding 10 (Bug 85.10): `READONLY` / `READWRITE` answer `+OK` even when the cluster support is disabled

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection + live Redis 7.2.11 differential) — agent 2, see Review Feedback` — **fixed** (`a6306ef6`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:193-201
        if ("readonly".equals(cmd)) {
            SocketInspector.setConnectionReadonly(socket, true);
            return OKReply.INSTANCE;
        }

        if ("readwrite".equals(cmd)) {
            SocketInspector.setConnectionReadonly(socket, false);
            return OKReply.INSTANCE;
        }
```

### Root cause

Redis 7.2 (`src/cluster.c`, `readonlyCommand()` / `readwriteCommand()`):

```c
void readonlyCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_READONLY;
    addReply(c,shared.ok);
}
```

Velo always answers `+OK` and only flips its own connection flag. The flag itself is honoured
(`MultiWorkerServer.java:563` rejects write commands on a readonly connection, and `Category.isWriteCmd()`
lists `rpop`, `rpush`, `rpushx`, `rename`, `renamenx`, `restore`, `rpoplpush` as write commands — verified
correct), so the divergence is limited to the reply.

### Trigger / impact

Trigger: any `READONLY` / `READWRITE` on a non-cluster Velo instance (the default configuration,
`velo.properties` has no cluster settings).
Impact: a Redis client that checks the reply of `READONLY` (e.g. cluster-aware clients, `redis-py` with
`cluster_mode`) sees a different answer than Redis gives; the "cluster support disabled" signal is lost.

### Suggested fix direction

Decide with the product owner whether Velo wants to keep the lenient behaviour (the connection flag is useful
outside cluster mode). If Redis compatibility is wanted, answer
`new ErrorReply("This instance has cluster support disabled")` when `ConfForGlobal.clusterEnabled` is false.

---

## Finding 11 (Bug 85.11): the `RENAME` / `RESTORE` error texts diverge from Redis 7.2

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection + live Redis 7.2.11 differential) — redis text refined: the reply is BUSYKEY-prefixed, see Review Feedback` — **fixed** (`a944f57b`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:318   return ErrorReply.NO_SUCH_KEY;      // RENAME, source missing
// src/main/java/io/velo/command/RGroup.java:436   return ErrorReply.TARGET_KEY_BUSY;  // RESTORE without REPLACE
```
```java
// src/main/java/io/velo/reply/ErrorReply.java:95    public static final ErrorReply NO_SUCH_KEY = new ErrorReply("not such key");
// src/main/java/io/velo/reply/ErrorReply.java:162   public static final ErrorReply TARGET_KEY_BUSY = new ErrorReply("target key name is busy");
```

### Root cause

Redis 7.2 answers `ERR no such key` (`shared.nokeyerr`, `src/server.c:1844`) for `RENAME` of a missing key and
**`BUSYKEY Target key name already exists.`** (`shared.busykeyerr`, `src/server.c:1876` — the reply is
BUSYKEY-prefixed, not `ERR …`) for `RESTORE` on an existing key. Velo answers `ERR not such key` and
`ERR target key name is busy`.

### Trigger / impact

Trigger: `RENAME missing other`, `RESTORE existing 0 <payload>`.
Impact: clients that match on the error text (Redis tooling, sentinel scripts, application code that checks for
`BUSYKEY` / `no such key`) do not recognise the Velo answers — in particular the `BUSYKEY` prefix is a
protocol-level marker that Redis tooling keys on, so the fix must emit `-BUSYKEY Target key name already
exists.` and not `-ERR …`. The constants are shared with other command groups, so the fix is velo-wide (see
bug 84.10, which aligned the `PEXPIRE`/`PSETEX` texts with Redis).

### Suggested fix direction

Align the two texts with Redis (`"no such key"`, `"Target key name already exists."`) — either by changing the
shared constants (and fixing the other users) or by introducing Redis-specific replies used by `RGroup`.

---

## Finding 12 (Bug 85.12): an unknown `r*` command answers a nil bulk reply instead of `ERR unknown command`

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **fixed** (`f645ca48`)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:247
        return NilReply.INSTANCE;      // fall-through for an unknown command
```

### Root cause

Every `R*` command registered in the command registry (26-123) has a dispatch branch, so the fall-through is
only reached for commands that are not registered at all (e.g. `RFOO`). Other groups answer
`ErrorReply.UNKNOWN_COMMAND(cmd)` for the same situation (`BGroup.java:314`, `NGroup.java:45`,
`OGroup.java:45`, `QGroup.java:45`, `VGroup.java:47`, `YGroup.java:45`, `JGroup.java:45`), and Redis answers
`ERR unknown command 'rfoo'`.

### Trigger / impact

Trigger: any unregistered command starting with `r`.
Impact: the client gets `(nil)` instead of an error and cannot distinguish "unknown command" from a valid
nil-replying command. (The same inconsistency exists in the other groups that return `NilReply.INSTANCE` —
this finding is filed here only because `RGroup` is the module under review.)

### Suggested fix direction

Return `ErrorReply.UNKNOWN_COMMAND(cmd)` from the fall-through, consistent with the groups that already do so.

---

## Finding 13 (Bug 85.13): with the split hash storage mode, `RENAME` of a hash answers "no such key" and `RESTORE` does not detect an existing hash

**Severity:** Low
**Class:** Correctness (configuration dependent)
**Status:** `CONFIRMED (reproduced) — agent 2, temp Spock spec, see Review Feedback` — **partly fixed** (`059c0674`, the `RESTORE` data loss half)

### Cited files / lines

```java
// src/main/java/io/velo/command/RGroup.java:315-319 (rename) — the source is looked up by its own key only
        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var srcCv = getCv(srcSlotWithKeyHash);
        if (srcCv == null) {
            return ErrorReply.NO_SUCH_KEY;
        }
```
```java
// src/main/java/io/velo/command/RGroup.java:433-438 (restore) — the busy-key check also uses the main key only
        if (!replace) {
            var cv = getCv(slotWithKeyHash);
            if (cv != null) {
                return ErrorReply.TARGET_KEY_BUSY;
            }
        }
```
```java
// src/main/java/io/velo/type/RedisHashKeys.java:116 and 125 — in split mode the hash lives under derived keys
    public static String keysKey(String key) {
        return "h_k_{" + key + "}";
    }

    public static String fieldKey(String key, String field) {
        return "h_f_" + "{" + key + "}." + field;
    }
```

### Root cause

When `persist.isHashSaveMemberTogether=false` (`LocalPersist.java:406`, default `true`), a hash is not stored
under its own key: the field names live in `h_k_{key}` and the field values in `h_f_{key}.field`; the main key
`key` itself does not exist. `RENAME`/`RESTORE` only look at the main key, so:
* `RENAME h h2` → `getCv(h)` is null → `ERR not such key`, although the hash exists;
* `RESTORE h 0 <hash payload>` without `REPLACE` → the busy-key check finds nothing → the existing hash is
  overwritten silently, and the stale fields that are not part of the new payload are left behind.

### Trigger / impact

Trigger: `persist.isHashSaveMemberTogether=false` (a supported, non-default configuration).
Impact: `RENAME` fails for hashes and `RESTORE` overwrites an existing hash without `REPLACE`, leaving stale
fields — Redis answers `BUSYKEY` and renames the whole hash.

### Suggested fix direction

When the key is a hash in split mode, resolve the existence through the hash keys key
(`RedisHashKeys.keysKey(key)`) instead of the main key, and make `RENAME` move the derived keys as well
(`h_k_{key}` → `h_k_{newkey}` and every `h_f_{key}.field` → `h_f_{newkey}.field`). If the split mode is
planned to be dropped, this finding can be closed as "not applicable" — a product decision.

---

## Finding 15 (Bug 85.15): the other command groups answer a nil for an unimplemented command

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified` — **fixed** (`16b96e12`)

### Cited files / lines

```java
// src/main/java/io/velo/command/AGroup.java:126-129 (the same fall through in CGroup, DGroup, EGroup,
// FGroup, GGroup, HGroup, IGroup, KGroup, LGroup, MGroup, PGroup, SGroup, TGroup, UGroup, WGroup, ZGroup)
        if ("asking".equals(cmd)) {
            // velo slot migration node A to node B, not by keys like redis, end when B all received.
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }
```

### Root cause

Only `BGroup`, `JGroup`, `NGroup`, `OGroup`, `QGroup`, `VGroup`, `YGroup` (and `RGroup` since 85.12) answered
`ErrorReply.UNKNOWN_COMMAND(cmd)` for a command they do not implement, the other groups answered a nil reply.
A client cannot tell "the command is not supported" from "the command ran and the answer is nil" — Redis answers
`ERR unknown command 'foo'` for a command it does not know.

The audit found no registered command without a dispatch branch in any group, so the fall through is only
reachable for an unknown command. The `XGroup` is the internal `x_repl` dispatch: the stream commands (`XADD`,
`XACK`, …) are not implemented at all, so the user commands now answer the unknown command error while the
internal dispatch (`cmd == X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH`) keeps its behaviour.

### Trigger / impact

Trigger: any command that is not implemented, e.g. `XADD key field value` or `FOO`.
Impact: the client gets a nil reply instead of an error, and cannot detect the missing command.

### Suggested fix direction

Answer `ErrorReply.UNKNOWN_COMMAND(cmd)` in the fall through of every command group (done), and add a value based
`equals()` / `hashCode()` to `ErrorReply` so that the tests can compare an error reply by its message
(`BulkReply` and `IntegerReply` already compare by value).

## Notes / non-findings

Checked and **correct** (kept so later rounds do not re-investigate):

- `parseSlots()` (142-180) matches the arities of all registered `R*` commands; `restore` needs 4 arguments,
  `rename`/`renamenx`/`rpoplpush` exactly 3, `rpop` 2 or 3, `rpush`/`rpushx` at least 3.
- `RENAME` of a key onto itself: Velo removes and re-writes the key (`removeDelay` then `setCv`, lines
  330-331), which leaves the key present with its value — the same observable result as Redis
  (`renameGenericCommand()` in `src/db.c`, the `samekey` branch). `RENAMENX a a` correctly answers `0`.
- `RENAME` preserves the TTL of the source key (`setCv()` keeps `cv.getExpireAt()`, `BaseCommand.java:972-999`).
- `RENAME`/`RENAMENX` enforce `KEY_MAX_LENGTH` for both keys (308-313) and answer `ErrorReply.KEY_TOO_LONG`.
- `restore()` validates the DUMP payload: the RDB version and the CRC64 are checked in
  `VeloRDBImporter.restore()` (15-37) — Redis also rejects a bad version/checksum ("DUMP payload version or
  checksum are wrong").
- `role()` (500-556) returns the Redis shape: `[master, offset, [[host, port, offset], …]]` for a master and
  `[slave, host, port, state, offset]` for a replica; the link state is derived from the real TCP state
  (`replPair.isLinkUpAnyOk()`), not hard-coded.
- `readonly`/`readwrite` really do gate writes: `MultiWorkerServer.java:563` +
  `Category.isWriteCmd()` (the `write` category contains `rename`, `renamenx`, `restore`, `rpop`, `rpoplpush`,
  `rpush`, `rpushx`).
- The `replicaof` delegation to `SGroup.slaveof()` (211-215) is out of scope for this round (the `SGroup`
  module has no review report yet); the arity check happens in `SGroup`.
- The `rpop` / `rpush` / `rpushx` delegations to `LGroup.lpop()` / `LGroup.lpush()` (225-245) are correct:
  `LGroup.lpop()` implements the `RPOP key [count]` form generically (557-627) and `LGroup.lpush()` implements
  the "push only if the key exists" rule for `rpushx` (845-880).
- `RPOPLPUSH`/`LMOVE` remove the source element and delete the source key when the list becomes empty
  (612-617) — the "bug 4" regression that the existing `RGroupTest` covers.
- The existing `RGroupTest` cross-worker cases (`test rename` 193-225, `test rpoplpush` 487-506,
  `test move block` 727-753) all run through the `InMemoryGetSet` mock, which bypasses
  `OneSlot.checkCurrentThreadId()` — the thread-affinity violations in 85.1-85.3 are therefore not detected
  (the mock cannot express them). A regression test must use a real `LocalPersist` with two slots owned by two
  different threads.
- `doc/redis_command_support.md:104` still marks `RESTORE` as unsupported (`[x]`) although it is implemented
  and registered — the support matrix is stale (documentation only, not a code bug).
- **The ActiveJ `Promise.ofFuture` bounce hides cross-worker thread bugs** (found in round 1, see 85.2):
  `OneSlot.asyncCall()` (`OneSlot.java:736-737`) builds the destination promise with `Promise.ofFuture(...)`,
  and ActiveJ 6.0-beta2's `Promise.ofCompletionStage` captures the **calling** reactor
  (`Reactor.getCurrentReactor()`) and completes the promise back on that reactor
  (`reactor.execute(() -> cb.set(r, e))`). Consequence: a `whenComplete` callback attached to that promise runs
  on the **calling** (source) worker thread, not on the thread that executed the destination task. Any
  cross-worker code that mutates the *source* slot from such a callback happens to be correct in production —
  but it is correct by accident, it breaks on the non-reactor fallback path (`OneSlot.java:739-748`), and it
  makes the operation non-atomic (the reply is sent before the source mutation runs). Future rounds should not
  assume "the callback runs where the task ran".

## Reviewer checklist (for AI agent 2)

Common setup for the cross-worker reproductions (the scratch specs used below are inlined):

```groovy
// two slots, slot 0 owned by the test thread, slot 1 owned by another thread
LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
def localPersist = LocalPersist.instance
localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())
def eventloop = Eventloop.builder().withIdleInterval(Duration.ofMillis(50)).build()
eventloop.keepAlive(true)
Thread.start { eventloop.run() }
localPersist.oneSlot((short) 1).setSlotWorkerEventloop(eventloop)
localPersist.fixSlotThreadId((short) 1, eventloop.eventloopThread.threadId())
// find a key in slot 0 and a key in slot 1 (slotNumber=2 → the slot depends on the sign of the key hash)
def srcKey = null; def dstKey = null
for (int i = 0; i < 1000 && (srcKey == null || dstKey == null); i++) {
    def k = 'k' + i
    def s = BaseCommand.slot(k, 2)
    if (s.slot() == 0 && srcKey == null) srcKey = k
    if (s.slot() == 1 && dstKey == null) dstKey = k
}
```

- **Finding 1**: build the setup above, put a list with one element in `srcKey` (slot 0), call
  `rGroup.rpoplpush()` with `rGroup.crossRequestWorker = true` and
  `rGroup.slotWithKeyHashListParsed = [srcSWKH, dstSWKH]`. Expectation: the call throws
  `IllegalStateException: Thread id not match, thread id=1, thread id protected for safe=53` at
  `RGroup.move` line 605 (`getCv` → `OneSlot.get` → `checkCurrentThreadId`) — i.e. `RPOPLPUSH`/`LMOVE`/
  `BLMOVE` can never work across workers. Confirm the same for `LGroup.lmove()` (`LMOVE`).
  A live alternative: start Velo with `slotWorkers=2` and issue `RPOPLPUSH` for two keys that hash to
  different workers.
- **Finding 2**: same setup; write `srcKey` on the test thread, then `rGroup.rename(false)` with
  `crossRequestWorker = true`. Expectation: the reply is `OKReply`, the destination key exists **and** the
  source key still exists (`rGroup.getCv(srcSWKH) != null` → `true`). The `removeDelay` inside the
  `whenComplete` throws `IllegalStateException` on the destination thread and is swallowed.
- **Finding 3**: same setup as finding 2 plus `localPersist.oneSlot((short) 1).setReadonly(true)`.
  Expectation: `rGroup.rename(false)` returns an `AsyncReply` whose `settablePromise.isComplete()` is
  `false` after a delay — the client would hang forever; the underlying `ReadonlyException` is swallowed and
  `if (r)` throws `NullPointerException` (the promise value is `null`).
- **Finding 4**: with the mock `InMemoryGetSet` (`rGroup.byPassGetSet = new InMemoryGetSet()`), run
  `rGroup.execute('restore list0 100000 bbb') { data -> data[3] = RDBParser.dumpList(rl) }` and then read
  `rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst())`. Expectation: `expireAt == 0`
  (`CompressedValue.NO_EXPIRE`) although the TTL was 100000 ms. Repeat for a set / zset / hash payload.
  Redis 7.2 keeps the TTL (`PTTL` ≈ 100000).
- **Finding 5**: `rGroup.execute('restore >key 0 bbb') { data -> data[3] = RDBParser.dumpString('v'.bytes) }`
  (the `>key` placeholder is `KEY_MAX_LENGTH + 1` bytes). Expectation: the reply is `OKReply`, i.e. a
  257-byte key is accepted, while every other key command answers `ErrorReply.KEY_TOO_LONG`. Then check the
  consequence on the persist layer: `Wal.V.decode()` (`Wal.java:152`) and `KeyBucket.getFromOneCell()`
  (`KeyBucket.java:291`) throw `IllegalStateException("Key length error, …")` for such a key — verify with a
  real (non-mock) `LocalPersist` whether the WAL group / key bucket becomes unreadable.
- **Finding 6**: `rGroup.execute('restore k abc bbb')` → the call throws
  `NumberFormatException: For input string: "abc"` out of `handle()` (the server turns it into
  `ERR For input string: "abc"`); Redis answers `ERR value is not an integer or out of range`.
- **Finding 7**: `rGroup.execute('restore k 0 bbb badoption') { data -> data[3] = RDBParser.dumpString('v'.bytes) }`
  → the reply is `OKReply`; Redis answers `ERR syntax error`. `rGroup.execute('restore k -1 bbb')` → `OKReply`;
  Redis answers `ERR Invalid TTL value, must be >= 0`.
- **Finding 8**: with a real `LocalPersist` of 2 slots, put a key in slot 1 only and call `RANDOMKEY` from a
  thread that owns slot 0 → the reply is `(nil)` although the keyspace is not empty. Also check the
  single-slot limit: `RANDOMKEY` can only ever return a key of `localPersist.currentThreadFirstOneSlot()`.
- **Finding 9**: fill a bucket with several keys, call `RANDOMKEY` repeatedly and observe that the same
  (last) key of the bucket is always returned; and check that a key whose TTL has passed can be returned.
- **Finding 10**: `READONLY` on a non-cluster instance → `+OK`; Redis 7.2 (`src/cluster.c:7318`) answers
  `ERR This instance has cluster support disabled`.
- **Finding 11**: `RENAME missing other` → `ERR not such key`; Redis: `ERR no such key`.
  `RESTORE existing 0 <payload>` → `ERR target key name is busy`; Redis: `ERR Target key name already exists.`
- **Finding 12**: send an unregistered command such as `RFOO` → the reply is `(nil)` instead of
  `ERR unknown command 'RFOO'`.
- **Finding 13**: with `LocalPersist.instance.setHashSaveMemberTogether(false)`, `HSET h f v` then
  `RENAME h h2` → `ERR not such key`; and `RESTORE h 0 <hash payload>` over the existing hash → `+OK` instead
  of `ERR Target key name already exists.`

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (real `LocalPersist`, slotNumber=2, two slot workers on two real eventloop threads, real
`OneSlot.get/put/removeDelay` — the `InMemoryGetSet` mock is only used where the finding is mock-expressible) + live
Redis 7.2.11 differential (`redis-server --port 47999`, python RESP client) + static inspection.
Temp spec: `RGroupBug85VerifyTempSpec.groovy` (8 feature methods, 8 passed / 0 failed — deleted after the
reproduction, the full source is preserved in `doc/bug_reviews/logs/` … inlined below per finding).
Reviewed at commit `a5064790` (= `bb7fb2fa` + comment-only commits, no behavior change vs the report baseline).

| Finding | Verdict | Evidence |
|---|---|---|
| 85.1 | `CONFIRMED (reproduced)` | Real persist, slot 0 owned by worker-1 eventloop, slot 1 by worker-2. Src list seeded into slot 0 (LRU roundtrip verified first). `rpoplpush <src-slot0> <dst-slot1>` executed ON worker-1 (production routing: first key): `caught = IllegalStateException: Thread id not match, thread id=55, thread id protected for safe=<worker-2 tid>` at `OneSlot.get ← BaseCommand.getCv ← RGroup.move` — the dst type pre-check (RGroup.java:609) runs inline on the src worker. `LGroup.lmove` delegates to `move()`/`moveBlock()` (LGroup.java:551/553) → LMOVE/BLMOVE equally affected. |
| 85.2 | `REFINED` — production-safe, fallback-path-only | **Production mode** (rename executed on the src worker's eventloop, mirroring `MultiWorkerServer.getByteBufPromiseByOtherEventloop`): `dstOneSlot.asyncCall` takes the `Promise.ofFuture` branch (OneSlot.java:736-737); ActiveJ `ofCompletionStage` captures the **calling** reactor and bounces the completion back to it, so the `whenComplete` runs on the **src** worker → `removeDelay(src)` passes the guard. Reproduced: promise result `OKReply`, src key gone (`getCv(src) == null`), dst written. **Fallback mode** (agent 1's checklist: plain thread owning slot 0, not a reactor): promise result `OKReply` but `getCv(src) != null` — the src SURVIVES, the `IllegalStateException` from `removeDelay` on the dst worker is swallowed. Production callers always run on a reactor (`isReuseNetWorkerEventloop` implies a single worker, so never cross-worker), so the High "silent data duplication" impact does not occur in production; severity High → Low (latent). Same bounce nuance as bug 84.1 (see bug_80 round 1 line 469). |
| 85.3 | `CONFIRMED (reproduced)` | Production mode + `oneSlot(1).setReadonly(true)`: the dst task on worker-2 throws `ReadonlyException` in `setCv` → after 2 s of pumping both eventloops `finalPromise.isComplete() == false` — the client would hang with no reply; worker-2 stays alive. Mechanism of the swallowed cause is code-evident: the task only settles `finalPromise` on success (RGroup.java:345-356), and `whenComplete((r, e) -> { if (r) … })` ignores `e` and NPE-unboxes the null `r`. |
| 85.4 | `CONFIRMED (reproduced)` | Mock restore with ttl=100000: string payload → `cv.getExpireAt() > 0`; list/set/zset/hash payloads → `cv.getExpireAt() == CompressedValue.NO_EXPIRE` (all four). Redis 7.2.11 differential: `RESTORE lst85dst 60000 <list dump>` → `OK`, `PTTL` → `60000` — Redis applies the TTL to a restored list. |
| 85.5 | `CONFIRMED (reproduced)` | `restore >key 0 <string dump>` (`>key` → 257 zero bytes) → `OKReply`, the key is stored in the mock — no `KEY_TOO_LONG`. Persist-layer hazard proven at the codec level: `new Wal.V(seq, bucket, hash, NO_EXPIRE, type, <257-byte key>, cvEncoded, false).encode(true)` succeeds, `Wal.V.decode(DataInputStream)` → `IllegalStateException: Key length error, key length=257` — i.e. once such a record is written, every WAL-group read that hits it throws. |
| 85.6 | `CONFIRMED (reproduced)` | `rGroup.execute('restore k85 abc bbb')` → `NumberFormatException: For input string: "abc"` escapes `handle()` (RequestHandler.java:576-580 turns it into `ERR For input string: "abc"`). Redis 7.2.11: `ERR value is not an integer or out of range`. Differential also confirms the ordering claim: `RESTORE existing abc <dump>` → `BUSYKEY …` (busy-key check precedes the TTL parse). |
| 85.7 | `CONFIRMED (reproduced)` | Velo: `restore k85 0 <dump> badoption` → `OKReply` (key created); `restore k85neg -1 <dump>` → `OKReply` with `cv.getExpireAt() < now` (born expired). Redis 7.2.11: badoption → `ERR syntax error`; `-1` → `ERR Invalid TTL value, must be >= 0`; `idletime 5 freq 3` → `ERR syntax error`. All three divergences confirmed. |
| 85.8 | `CONFIRMED (reproduced)` | Real persist, slot 0 owned by the calling thread, slot 1 by a background eventloop; 10 keys persisted into slot 1 (bucket 3, `persistShortValueListBatchInOneWalGroup`). `randomkey` on the slot-0 thread → `NilReply` although the keyspace is not empty. The 10-probe sparsity (`maxTryTimes = 10`, RGroup.java:258) is inspection-confirmed. |
| 85.9 | `CONFIRMED (reproduced)` | Real persist: 10 keys seeded into ONE bucket (bucketIndex pinned via `Mock.prepareShortValueList(10, 3)`), `randomkey` called up to 2000× collecting distinct replies — exactly **1** distinct key ever returned from the 10-key bucket (the bucket's last key in cell order): the `skipN` random index has no effect because `countArray[0]` stops incrementing after the first match and every later cell overwrites `targetKeyArray[0]`. The expired-key return (lambda ignores `expireAt`, RGroup.java:280-286) and the stale-count `BulkReply(null)` NPE are inspection-confirmed. |
| 85.10 | `CONFIRMED (inspection + differential)` | Velo: `readonly`/`readwrite` return `OKReply.INSTANCE` unconditionally (RGroup.java:193-201; also covered by RGroupTest 88-106), `ConfForGlobal.clusterEnabled` defaults `false` (ConfForGlobal.java:215). Redis 7.2.11 (non-cluster): both `READONLY` and `READWRITE` → `ERR This instance has cluster support disabled`. |
| 85.11 | `CONFIRMED (inspection + differential)` | Velo: `ErrorReply.NO_SUCH_KEY = "not such key"` (ErrorReply.java:95), `TARGET_KEY_BUSY = "target key name is busy"` (ErrorReply.java:162). Redis 7.2.11: `RENAME missing other` → `ERR no such key`; `RESTORE existing 0 <dump>` → `BUSYKEY Target key name already exists.` — refinement of the finding: the Redis reply is **BUSYKEY-prefixed**, not `ERR Target key name already exists.` as the finding wrote; the fix must use the BUSYKEY prefix (clients match on it). |
| 85.12 | `CONFIRMED (reproduced)` | `rGroup.execute('rfoo')` → `NilReply.INSTANCE` (also visible in the existing suite: RGroupTest 'test handle - format errors' where-clause `'zzz' | NilReply.INSTANCE`, line 85). Redis: `ERR unknown command`. |
| 85.13 | `CONFIRMED (reproduced)` | `LocalPersist.instance.hashSaveMemberTogether = false`; `hset h85 f1 v1` → mock holds `h_k_{h85}` and `h_f_{h85}.f1`, main key `h85` absent; `rename h85 h85x` → `ErrorReply.NO_SUCH_KEY` although the hash exists; `restore h85 0 <hash dump>` over the existing hash (no REPLACE) → `OKReply` — the busy check missed it. |

### Additional observations

- **85.2 refinement detail** — the reviewer checklist setup (plain thread + `fixSlotThreadId`) does reproduce the src-survival,
  but it exercises `OneSlot.asyncCall`'s no-reactor fallback branch (OneSlot.java:740-748), which production never takes for
  a cross-worker command (handle() is always submitted to a slot-worker eventloop; the single-worker reuse mode can never be
  cross-worker). The report's root-cause sentence "also runs on the DST worker thread" is wrong for the reactor path. The
  defensive fix (dispatch `removeDelay` via `srcOneSlot.asyncExecute`) is still worthwhile — it removes the reliance on the
  undocumented `Promise.ofFuture` bounce, exactly like the bug 84.1 pfmerge fix did.
- **READWRITE also errors in Redis** when cluster support is disabled — the differential shows both commands rejected; the
  Velo side diverges for both (finding 85.10 text mentions `READONLY`/`READWRITE` already, so no scope change).
- The restore-related findings (85.4-85.7) cluster on the same method; `restore` is still marked unsupported in
  `doc/redis_command_support.md` (noted as a non-finding in the report — agree, documentation only).
- `doc/bug_reviews/bug_85_rgroup_commands_review_round_1.md` Reviewer checklist recipes were followed where possible and
  are accurate except for the 85.2 production-mode caveat above.

### Recommended fix order (for the fix agent)

1. **85.3** — settle `finalPromise` on every path + null-safe `whenComplete` (`Boolean.TRUE.equals(r)`, surface `e`); smallest diff, worst client impact (hang).
2. **85.1** — move the dst type pre-check into the dst-worker task (or async read first); cross-worker `RPOPLPUSH`/`LMOVE`/`BLMOVE` is currently always broken.
3. **85.2** — while touching the same lines as 85.3: `srcOneSlot.asyncExecute(() -> removeDelay(src))` instead of relying on the bounce (separate commit from 85.3).
4. **85.4** — thread `expireAt` through the collection restore callbacks.
5. **85.5** — up-front `KEY_MAX_LENGTH` check (+ optional defensive `Wal.put`/`KeyBucket.putTo` guard).
6. **85.6**, then **85.7** — restore parse robustness; one commit per bug.
7. **85.8 + 85.9** — randomkey: search all slots of the worker (or fan out), honour `skipN`/skip expired keys, nil-guard the target.
8. **85.10 / 85.11 / 85.12** — protocol texts and unknown-command reply (note the BUSYKEY prefix from 85.11).
9. **85.13** — needs a product decision on the split hash mode before investing in derived-key renaming.

## Response (AI agent 1, round 1)

**Overall:** I accept the agent 2 verdicts. 12 findings `CONFIRMED`, 1 finding (`85.2`) `REFINED` — the
reviewer is right and my round 1 root-cause sentence for 85.2 was wrong for the production path; I retract the
"High / silent data duplication in production" claim and keep the finding as **Low (latent)**. One new finding
(`85.14`) is filed from this round.

### Per-finding response

| Finding | Agent 2 verdict | Agent 1 response | New citations / corrections |
|---|---|---|---|
| 85.1 | `CONFIRMED (reproduced)` | **Agree.** | The `getCv(dst)` call is at `RGroup.java:605` (609 is the closing brace of the `if`). The failure happens *before* any async dispatch, so the `Promise.ofFuture` bounce does not help — the production impact stands. |
| 85.2 | `REFINED` (fallback path only) | **Agree — I was wrong.** The bounce returns the completion to the calling reactor, so in production the `removeDelay(src)` runs on the source worker. Severity High → **Low (latent)**; the fix is still worth doing (it removes the reliance on an undocumented ActiveJ behaviour, repairs the fallback path and closes the non-atomicity window). | ActiveJ 6.0-beta2 bytecode: `Promise.ofFuture(CompletableFuture)` → `Promise.ofCompletionStage(stage)` → `Promise.ofCallback(...)` → `lambda$ofCompletionStage$2(stage, cb)`: `Reactor reactor = Reactor.getCurrentReactor(); reactor.startExternalTask(); stage.whenCompleteAsync((r,e) -> { reactor.execute(() -> cb.set(r,e)); reactor.completeExternalTask(); });` — the reactor is captured **on the calling (source) thread** and the completion is bounced back to it. `Reactor.getCurrentReactorOrNull()` is non-null on every ActiveJ eventloop thread because `Eventloop`'s thread body calls `Reactor.setCurrentReactor(this)`. `OneSlot.asyncCall` (`OneSlot.java:726-749`) therefore takes the `Promise.ofFuture` branch at 736-737 in production; the fallback at 739-748 is only reachable off-reactor (unit tests / direct calls). The single-worker reuse mode can never be cross-worker (`MultiWorkerServer.java:1068-1069`: `isReuseNetWorkerEventloop = true` only when `slotWorkers == 1 && netWorkers == 1`), so the fallback branch is unreachable in production. |
| 85.3 | `CONFIRMED (reproduced)` | **Agree.** The bounce does not help here: the destination task fails, the promise completes exceptionally with `r == null`, `whenComplete((r,e) -> { if (r) … })` NPE-unboxes the null and `finalPromise` is never settled → the client hangs. | `MultiWorkerServer.transferAsyncReply()` (694-710) would turn a rejected `finalPromise` into an error reply — the fix only has to settle the promise on every path. |
| 85.4 | `CONFIRMED (reproduced)` | **Agree.** | `LGroup.saveRedisList` (418-430), `SGroup.saveRedisSet` (1132-1153, explicitly `NO_EXPIRE`), `ZGroup.saveRedisZSet` (713-725), `HGroup.saveRedisHH` (525-537) / `saveRedisHashKeys` (499-513) all call the 3-arg `set(...)` → `CompressedValue.NO_EXPIRE`; only `onString` (448-450) passes `finalExpireAt`. |
| 85.5 | `CONFIRMED (reproduced)` | **Agree.** | `Wal.V.decode()` (`Wal.java:152`) and `KeyBucket.getFromOneCell()` (`KeyBucket.java:291`) throw `IllegalStateException("Key length error, …")` for `keyLength > CompressedValue.KEY_MAX_LENGTH` (256) — the write path (`Wal.put()`, `KeyBucket.putTo()`) has no guard. |
| 85.6 | `CONFIRMED (reproduced)` | **Agree.** | Redis 7.2.14 (local `~/ws/redis`): `getLongLongFromObjectOrReply` → `ERR value is not an integer or out of range`; the busy-key check precedes the TTL parse (`src/cluster.c:6750-6763`), so `RESTORE existing abc <dump>` → `BUSYKEY …` — the reviewer's ordering claim is right. |
| 85.7 | `CONFIRMED (reproduced)` | **Agree.** | Redis 7.2.14 `src/cluster.c:6711-6747`: the option loop ends with `addReplyErrorObject(c,shared.syntaxerr)` for an unknown option; `idletime` requires `lfu_freq == -1` and `freq` requires `lru_idle == -1`, so `IDLETIME`+`FREQ` together is a syntax error; `ttl < 0` → `Invalid TTL value, must be >= 0` (6761). |
| 85.8 | `CONFIRMED (reproduced)` | **Agree.** | `LocalPersist.currentThreadFirstOneSlot()` (639-646) returns the **first** slot owned by the calling worker, so `RANDOMKEY` can only ever see one of the worker's slots; `maxTryTimes = 10` (`RGroup.java:258`) over `bucketsPerSlot` buckets (default 262144) makes the nil answer likely. |
| 85.9 | `CONFIRMED (reproduced)` | **Agree.** | `KeyBucket.iterate()` takes a `void` callback (`KeyBucket.java:253-286`) so the lambda's `return;` cannot stop the scan, and `countArray[0]` stops incrementing after the first match → every later cell overwrites `targetKeyArray[0]` (the bucket's last key). |
| 85.10 | `CONFIRMED (inspection + differential)` | **Agree.** | Redis 7.2.14 `src/cluster.c:7291-7310`: `readonlyCommand()` and `readwriteCommand()` both answer `This instance has cluster support disabled` when `server.cluster_enabled == 0`. The Velo side is a deliberate-looking product difference (the connection flag is enforced by `MultiWorkerServer.java:563`), so the disposition may be "won't fix" — the fix direction in the finding is unchanged. |
| 85.11 | `CONFIRMED (inspection + differential)` | **Agree — with the reviewer's refinement.** My round 1 text wrote `ERR Target key name already exists.`; the real Redis reply is **BUSYKEY-prefixed**. | Redis 7.2.14 `src/server.c:1876`: `shared.busykeyerr = createObject(OBJ_STRING, sdsnew("-BUSYKEY Target key name already exists.\r\n"));` and `src/server.c:1844`: `shared.nokeyerr = … "-ERR no such key\r\n"`. The fix must emit the `BUSYKEY` prefix, not `ERR`. |
| 85.12 | `CONFIRMED (reproduced)` | **Agree.** | `RGroup.handle()` falls through to `NilReply.INSTANCE` (247) for an unregistered `r*` command, while `BGroup.java:314`, `NGroup.java:45`, `OGroup.java:45`, `QGroup.java:45`, `VGroup.java:47`, `YGroup.java:45` answer `ErrorReply.UNKNOWN_COMMAND(cmd)`. |
| 85.13 | `CONFIRMED (reproduced)` | **Agree.** | In split mode the hash lives under `RedisHashKeys.keysKey(key)` = `h_k_{key}` and `RedisHashKeys.fieldKey(key, field)` = `h_f_{key}.field` (`RedisHashKeys.java:116,125`), the main key is absent → `getCv(src)` is null (`RGroup.java:316-319`) and the busy-key check (433-438) misses the hash. |
| **85.14** | *(new, filed by agent 1 in this response)* | **New finding — cross-worker `RPOPLPUSH` / `LMOVE` / `BLMOVE` hang the client when the destination write fails.** | see below |

### New finding 85.14 — the destination task of `move()` / `moveBlock()` swallows its exception and never settles the reply

**Severity:** Medium
**Class:** Correctness / Availability
**Status:** `OPEN — not verified` — **fixed** (`dee8ffd0`)

```java
// src/main/java/io/velo/command/RGroup.java:630-632 (move()) and 705-707 (moveBlock())
        var dstSlot = dstSlotWithKeyHash.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);
        dstOneSlot.asyncExecute(() -> moveDstCallback(dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise::set));
```
```java
// src/main/java/io/velo/persist/OneSlot.java:704-717 (asyncExecute)
    public void asyncExecute(@NotNull Runnable runnable) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                runnable.run();
                return;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        slotWorkerEventloop.execute(runnable);      // the exception is thrown on the DST eventloop thread
    }
```

**Root cause.** In the cross-worker case the destination push is dispatched with `asyncExecute`, i.e. as a bare
`Runnable` on the destination eventloop. `moveDstCallback()` → `set(...)` → `putToOneSlot()` throws
`ReadonlyException` when the destination slot is readonly / write-fenced (failover window) — the exception is
thrown inside the destination eventloop task, is handled by ActiveJ's `FatalErrorHandler` and never reaches
`finalPromise`, so `finalPromise::set` is never called: **the client hangs forever**. The element has already
been removed from the source list at that point (`RGroup.java:611-617`), so the element is lost as well.

**Trigger / impact.** `RPOPLPUSH` / `LMOVE` / `BLMOVE` whose destination slot is readonly or write-fenced.
Today this path is masked by 85.1 (the destination type pre-check fails first for every cross-worker pair), so
it becomes reachable as soon as 85.1 is fixed. The single-worker path is fine: `asyncExecute` runs the task
inline and rethrows, so `RequestHandler.handle()` turns it into an error reply.

**Suggested fix direction.** Wrap the destination task so that a failure settles the promise:
`dstOneSlot.asyncExecute(() -> { try { moveDstCallback(...); } catch (Exception e) { finalPromise.setException(e); } })`
(`MultiWorkerServer.transferAsyncReply()` already maps a rejected promise to an error reply), and consider
restoring the source element when the destination push fails.

**Fix-order note (agent 1).** 85.14 must be fixed **together with or right after 85.1**: today 85.1 aborts the
cross-worker `RPOPLPUSH` before the destination task is ever dispatched, so 85.14 is masked. Once 85.1 moves the
destination type pre-check into the destination task, the destination write path becomes reachable and 85.14
becomes a live client hang. The two fixes belong in the same commit or in two consecutive commits
(85.1 → 85.14), never 85.14 alone.

## Fix Log (AI agent 1, round 1 fix phase)

Every fix follows RED → GREEN → JaCoCo, one commit per bug. The focused test command for all of them is
`./gradlew :test --tests "io.velo.command.RGroupTest"` (27 features, green); the whole command module
(`./gradlew :test --tests "io.velo.command.*Test"`) is green after the last commit.

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 85.1 | `RGroupTest.test rpoplpush` RED: `Condition not satisfied: asyncMove[0] != null ... [] null` — the cross worker `rpoplpush` threw `IllegalStateException: Thread id not match` inside `RGroup.move` (`RGroup.java:605`) and produced no reply at all | `ffde6e8e` `fix: rpoplpush and lmove check the destination type on the destination slot worker` | green (the move answers the moved element, the destination list on worker 2 holds it, the source keeps the other element, a wrong type destination answers `WRONGTYPE` without touching the source, the cross worker `blmove` moves too) | `RGroup` 601-760: the new `moveCrossWorker`/`moveBlockCrossWorker`/`popFromSource` paths covered, only the defensive branches (the catch of the destination type check, the "the source became empty" race) stay uncovered |
| 85.2 | **no RED test is possible** — see the note below | *won't fix* (latent, production safe) | n/a | n/a |
| 85.3 | `RGroupTest.test rename` RED: `Condition not satisfied: renamePromise85.isComplete() ... SettablePromise false` — the cross worker rename never answered the client | `608cb077` `fix: settle the cross worker rename reply when the destination write fails` | green (a readonly destination answers `ERR READONLY`, an unexpected destination failure answers the exception, the source key is kept) | `RGroup` 336-376: all the branches of the destination task covered (the success, the readonly and the unexpected failure) |
| 85.4 | `RGroupTest.test restore` RED: `Condition not satisfied: listExpireAt > System.currentTimeMillis() + 50000 ... 0 ...` — the restored list had no expire time | `a4f31233` `fix: restore keeps the ttl for list, set, zset and hash payloads` | green (the list, set, zset and hash payloads keep the ttl) | the new `expireAt` overloads are covered: `LGroup.saveRedisList` 418-436, `SGroup.saveRedisSet` 1132-1166, `ZGroup.saveRedisZSet` 713-736, `HGroup.saveRedisHashKeys`/`saveRedisHH` 499-552 (only the pre-existing `preferDict == null` branches stay partial) |
| 85.5 | `RGroupTest.test restore` RED: `Condition not satisfied: reply == ErrorReply.KEY_TOO_LONG ... OKReply` — a 257 byte key was accepted | `c0c5a516` `fix: restore rejects a key that is too long` | green | `RGroup` 383-395: the new key length guard covered |
| 85.6 | `RGroupTest.test restore` RED: `java.lang.NumberFormatException: For input string: "abc"` escaped `handle()` | `05c08552` `fix: restore rejects a non numeric ttl like redis does` | green (a non numeric ttl, idletime and freq answer `ERR value is not an integer or out of range`, the busy key check comes first, an absolute ttl is used as it is) | `RGroup` 383-480: the ttl and the option parse branches covered |
| 85.7 | `RGroupTest.test restore` RED: `Condition not satisfied: reply == ErrorReply.SYNTAX ... OKReply` — an unknown option was accepted | `aebb27dd` `fix: restore rejects unknown options and a negative ttl like redis does` | green (an unknown option, a negative ttl and `idletime`+`freq` together answer an error) | `RGroup` 383-480: all the option branches covered |
| 85.8 | `RGroupTest.test randomkey` RED: `Condition not satisfied: rGroup.execute('randomkey') instanceof BulkReply ... NilReply` — the key of another slot was not found | `52b95928` `fix: randomkey searches all the slots of the keyspace` | green (the key of another slot of the worker and the key of another slot worker are found) | `RGroup` 250-290 and `LocalPersist.currentThreadSlots` 653-661 covered; the "no key in any slot" branch of the fan out stays uncovered |
| 85.9 | `RGroupTest.test randomkey` RED: `Condition not satisfied: distinctKeys.size() > 1 ... 1` (the same key every time) and `Condition not satisfied: randomKeyInBucket(...) == null ... expired:key` (an expired key was returned) | `1ad59c79` `fix: randomkey honours the random index and skips the expired keys` | green (more than one key of the bucket is returned, the expired key is not returned) | `RGroup` 250-345: the new `randomKeyInBucket` covered, only the null key bucket branch stays uncovered |
| 85.10 | `RGroupTest.test handle - readonly readwrite role` RED: `Condition not satisfied: readonlyReply instanceof ErrorReply ... OKReply` | `a6306ef6` `fix: readonly and readwrite answer an error when the cluster support is disabled` | green (the cluster enabled case still answers `+OK`) | `RGroup` 190-215: both branches covered |
| 85.11 | `RGroupTest.test rename` RED: `not such key` vs the redis `no such key`; `RGroupTest.test restore` RED: `target key name is busy` vs the redis `BUSYKEY Target key name already exists.` | `a944f57b` `fix: the rename and restore error texts are the redis ones` | green (the raw bytes are `-BUSYKEY Target key name already exists.\r\n`) | `ErrorReply` 95 and 162-165: the new constant covered |
| 85.12 | `RGroupTest.test handle - format errors` RED: `Condition not satisfied: rGroup.execute(input) == expected ... NilReply` — an unknown `r*` command answered a nil | `f645ca48` `fix: an unknown r command answers an unknown command error` | green (`ERR unknown command 'rfoo'`) | `RGroup` 240-255: the new fall through covered |
| 85.13 | `RGroupTest.test restore` RED: `Condition not satisfied: splitHashReply == ErrorReply.BUSYKEY ... OKReply` — the existing hash of the split mode was overwritten | `059c0674` `fix: restore detects an existing hash of the split mode` | green | `RGroup` 520-545: the new split mode check covered |
| 85.14 | `RGroupTest.test rpoplpush` RED: `Condition not satisfied: failMovePromise.isComplete() ... SettablePromise false` — the cross worker move hung the client | `dee8ffd0` `fix: report a failing cross worker move to the client instead of hanging` | green (a readonly destination answers `ERR READONLY`) | `RGroup` 640-720 and `pushToDestination` 890-907: the failure path covered |
| 85.15 | the group tests RED: `Condition not satisfied: reply == NilReply.INSTANCE ... ErrorReply{message='unknown command 'zzz''}` — every group answered a nil for an unimplemented command | `16b96e12` `fix: an unimplemented command answers an unknown command error` | green (every group test class run one by one: A, C, D, E, F, G, H, I, K, L, M, P, R, S, T, U, W, X, Z + `RequestHandlerTest`, `BGroupTest` and `RESPReplyTest`) | the new fall through branch of every group is covered by its own group test (`ErrorReply.UNKNOWN_COMMAND('zzz')`), `ErrorReply.equals()` / `hashCode()` added |
| 85.15 follow up | `RGroupTest.test rename` RED: `Condition not satisfied: unreadablePromise.getException().getMessage() != null ... null` — a destination failure without a message answered `ERR null` | `618f7acc` `fix: the destination failure without a message answers a readable error` | green | the wrapped exception path of the rename and of the move is covered |
| 85.7 follow up | the re-review found that the legacy `idletime 0 freq -1` case was shadowed by the new `idletime`+`freq` guard, so the freq range branch was not executed any more | `4943c4c0` `test: cover the restore freq range branch that the idletime guard shadowed` | green | `RGroup` 515-530: the freq range branch is covered again |
| 85.15 follow up 2 | the other command groups answer `ERR null` when the failure exception has no message (e.g. `ReadonlyException`, `CannotReadException`) | `c37b1bde` `fix: the readonly and cannot read exceptions carry a message` + `71145b1a` `fix: the command error replies carry a readable message` | green (every affected group test class run one by one: B, D, E, F, G, K, M, P, R, S, Z + `ExceptionTest`, `BaseCommandTest`, `LocalPersistTest`) | `Utils.wrapException()` keeps the exception type when the exception has a message (the `BGroupTest` wrong type assertion still sees the `TypeMismatchException`) and wraps it with the command context otherwise |

### Why 85.2 is not fixed

The production path of the cross worker `RENAME` is correct: the destination promise is built with
`Promise.ofFuture(...)` (`OneSlot.java:736-737`) and ActiveJ's `Promise.ofCompletionStage` captures the
**calling** reactor and completes the promise back on it, so the `whenComplete` callback — and therefore
`removeDelay(src)` — runs on the **source** worker thread and passes `checkCurrentThreadId()` (the reviewer
reproduced this in production mode, the bytecode is cited in the round 1 Response section).

The remaining defect is only reachable on the non-reactor fallback path of `OneSlot.asyncCall`
(`OneSlot.java:739-748`), which needs a caller that owns the source slot **without** owning its eventloop —
such a binding cannot be repaired by any dispatch (the queued task would run on a thread that is not the owner
either), and it is only reachable from a unit test. A defensive `srcOneSlot.asyncExecute(...)` dispatch would
therefore be a change without a failing test (the TDD rule of `doc/write_better_test_cases_for_ai_agents.md`
forbids it) and it cannot be proven by the current test infrastructure. The reliance on the ActiveJ bounce is
documented in the code (`RGroup.java`, comment added in `a5064790`), so the disposition is **won't fix**
(latent, production safe) — the same reasoning the reviewer applied.

The `RENAME` half of 85.13 (renaming a hash of the split mode, which needs the derived keys to be moved as
well) is also left open: it needs a product decision on the split hash storage mode, the data loss half
(`RESTORE` overwriting an existing hash) is fixed in `059c0674`.

## Re-review (AI agent 2)

**Method:** per-commit diff review at HEAD `d3b8d9cb`; focused tests re-run at HEAD
(`RGroupTest` 27/27, `LGroupTest` 41/41, `SGroupTest` 29/29, `ZGroupTest` 32/32, `HGroupTest` 57/57,
`DGroupTest` 23/23 — all green); JaCoCo re-inspected with `scripts/jacoco_cover.py`; **RED independently
reproduced** for the two High fixes and one restore sample in a throwaway worktree at each fix commit's parent
with only the fix commit's test file applied.

### RED re-verification (worktree at parent + fix-commit test file)

| Bug | Parent | Re-produced RED failure |
|---|---|---|
| 85.3 | `608cb077^` = `a5064790` | `test rename` FAILED: `Condition not satisfied: renamePromise85.isComplete() … SettablePromise false` — identical to the Fix Log quote |
| 85.1 | `ffde6e8e^` = `608cb077` | `test rpoplpush` FAILED: `Condition not satisfied: asyncMove[0] != null … [] null` — identical to the Fix Log quote |
| 85.5 | `c0c5a516^` = `a4f31233` | `test restore` FAILED: `reply == ErrorReply.KEY_TOO_LONG … <OKReply@…>` — identical to the Fix Log quote |

The remaining RED quotes (85.4/85.6/85.7/85.8/85.9/85.10-85.13/85.14) match the failure shapes agent 2
independently reproduced in the round 1 verification (list ttl 0, `NumberFormatException`, badoption `+OK`,
`not such key`, nil vs unknown command, randomkey nil / 1-distinct-key bias, split-hash busy miss) and the
tests are mechanical asserts on public APIs — accepted without a per-commit worktree re-run.

### Per-commit verdicts

| Bug | Commit | Diff review | Test at HEAD | JaCoCo | Verdict |
|---|---|---|---|---|---|
| 85.1 | `ffde6e8e` | 3-step handoff (dst type check on dst worker → pop on src worker → push on dst worker); source untouched on `WRONGTYPE` (Redis semantics kept); sync path behavior-preserving (`checkDstTypeOnCurrentWorker` + `popFromList` extract) | green | `moveCrossWorker` happy path + `WRONGTYPE` covered; only the defensive catches and the source-emptied race uncovered (as declared) | **ACCEPTED** |
| 85.3 | `608cb077` | dst task wrapped: `ReadonlyException` → `ErrorReply.READONLY`, generic → `setException` (mapped to an error reply by `transferAsyncReply`); `whenComplete` null-safe `Boolean.TRUE.equals(r)` | green | all three dst-task branches covered | **ACCEPTED** |
| 85.14 | `dee8ffd0` | step-2 (`popFromSource`) and step-3 (`pushToDestination`) failures settle the promise; `ReadonlyException` → `ERR READONLY` | green | `pushToDestination` failure path covered (906 partial, as declared) | **ACCEPTED** |
| 85.4 | `a4f31233` | `expireAt` threaded through new `saveRedisList/Set/ZSet/HashKeys/HH` overloads; old callers default `NO_EXPIRE` (behavior-preserving); split-mode field values also get the ttl | green (5 classes) | overloads executed in L/S/Z/HGroup (partials only on the pre-existing `preferDict == null` branches) | **ACCEPTED** |
| 85.5 | `c0c5a516` | up-front `KEY_MAX_LENGTH` guard, same reply as sibling commands | green | guard covered | **ACCEPTED** |
| 85.6 | `05c08552` | `try/catch` on ttl/idletime/freq parses → `NOT_INTEGER_OR_OUT_OF_RANGE`; **busy-key check moved before the ttl parse** — matches the Redis ordering confirmed by the round 1 differential | green | parse branches covered | **ACCEPTED** |
| 85.7 | `aebb27dd` | unknown option → `SYNTAX` (with `else`), negative ttl → `"Invalid TTL value, must be >= 0"` (exact Redis text), `idletime`+`freq` → `SYNTAX`, option args consumed (`i++`), busy check via `exists()` | green | option branches covered — **one coverage regression, see Follow-ups** | **ACCEPTED** (with follow-up) |
| 85.8 | `52b95928` | searches all slots of the current worker inline, then `doSthInSlots` fan-out to the other workers (each slot searched on its owning thread); empty-keyspace early exit when the worker owns all slots | green | `currentThreadSlots` 653-661 covered; the "no key anywhere" fan-out branch uncovered (as declared) | **ACCEPTED** |
| 85.9 | `1ad59c79` | new `randomKeyInBucket`: found-flag stops the overwrite, `skipN` honoured, expired keys skipped, null returns instead of `BulkReply(null)` NPE | green | covered except the null-`keyBucket` continue | **ACCEPTED** |
| 85.10 | `a6306ef6` | both `READONLY`/`READWRITE` error when `!clusterEnabled` — matches the differential (both commands rejected by Redis); cluster mode still `+OK` | green | both branches covered | **ACCEPTED** |
| 85.11 | `a944f57b` | `NO_SUCH_KEY` → `"no such key"`; new `BUSYKEY` constant using the existing `rawErrorPrefix` mechanism (like `MOVED`) → raw bytes `-BUSYKEY Target key name already exists.\r\n` — exactly the Redis wire reply | green (`DGroupTest` re-run for the shared constant) | constant covered | **ACCEPTED** |
| 85.12 | `f645ca48` | fall-through → `ErrorReply.UNKNOWN_COMMAND(cmd)` | green | covered | **ACCEPTED** |
| 85.13 | `059c0674` | busy check extended with the split-mode derived key (`h_k_{key}`) — fixes the silent-overwrite half; the `RENAME` half stays open for a product decision (declared) | green | covered | **ACCEPTED** (half, as declared) |
| 85.2 | — | **won't fix accepted** — agent 1's reasoning matches agent 2's round 1 refinement: production always calls `handle()` on a reactor, so the bounce guarantees the src removal runs on the src worker; the fallback path cannot be repaired by dispatch and is unit-test-only; the reliance is documented in the code comment from `a5064790` | n/a | n/a | **ACCEPTED (won't fix)** |

### Follow-ups (non-blocking)

1. **Coverage regression from `aebb27dd` (85.7):** the legacy test case `restore a 0 bbb replace absttl idletime 0
   freq -1` now returns `SYNTAX` at the earlier `hasIdleTime`+`FREQ` guard, so the freq range branch
   (`RGroup.java:521-522`) is no longer executed and the assertion `reply instanceof ErrorReply` passes vacuously.
   Replace the case with `freq 300` alone (no `idletime`) — and keep one `idletime+freq` case for the new guard.
2. Fix Log says "28 features" — `RGroupTest` currently has 27 (doc nit only).
3. `608cb077` maps a generic destination failure via `finalPromise.setException(e)`;
   `transferAsyncReply` builds the error text from `getMessage()`, which is null for some exception types
   (e.g. `CannotReadException`) — consider the null-message wrapping `CGroup.copy` already does (bug 84 pattern).

### Verdict

**All 13 fix commits ACCEPTED** (RED → GREEN → JaCoCo verified per the sampled worktree re-runs and the at-HEAD
evidence); 85.2 won't-fix accepted; 85.13 half fixed as declared. The fix round is complete.

## Residual Risk

- **Element loss on a failing cross-worker push (pre-existing, unchanged):** in `moveCrossWorker` the source
  element is popped and saved (step 2) before the destination push (step 3). If step 3 fails (e.g. the destination
  turns readonly in between), the client now gets `ERR READONLY` (85.14 fixed the hang) but the element is already
  gone from the source and never delivered — `RPOPLPUSH` is not atomic across workers. The sync path always had the
  same shape; a full fix needs a rollback/undo write on the source worker.
- **Cross-worker type races:** the destination type is validated at step 1 and re-validated inside
  `moveDstCallback` at push time, but another client can still interleave between the two — a dst that became a
  non-list between step 1 and step 3 answers `WRONGTYPE` after the element was popped (same window as above).
- **`RANDOMKEY` randomness is first-slot-wins across workers** (the fan-out returns the first non-null key in slot
  order) and the fan-out re-searches the current worker's slots — a bias/waste, not a correctness issue.
- **85.2 latent path** (non-reactor caller of `OneSlot.asyncCall`) remains by design; safe as long as every
  production caller stays on a reactor — the `a5064790` comment documents the reliance.
- **85.13 `RENAME` half** open: renaming a split-mode hash still answers `no such key` — awaiting the product
  decision on the split hash storage mode.
- The follow-ups listed above (freq-range test case, `getMessage()` null wrapping) are small, non-blocking items
  for the next round.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 85.1 | `RPOPLPUSH`/`LMOVE`/`BLMOVE` read the destination slot from the source worker thread → always fail across workers | High | Concurrency | `CONFIRMED (reproduced)` | **fixed** (`ffde6e8e`) |
| 85.2 | cross-worker `RENAME`/`RENAMENX` delete the source key only by accident (the ActiveJ bounce); the fallback path loses it | High → **Low (latent)** | Correctness | `REFINED` — agent 1 accepts the refinement (the production impact does not occur) | **won't fix** (latent, production safe — see the Fix Log) |
| 85.3 | cross-worker `RENAME`/`RENAMENX` never answer the client when the destination write fails | High | Correctness | `CONFIRMED (reproduced)` | **fixed** (`608cb077`) |
| 85.4 | `RESTORE` drops the TTL for list/set/zset/hash payloads | Medium | Correctness | `CONFIRMED (reproduced)` | **fixed** (`a4f31233`) |
| 85.5 | `RESTORE` does not enforce the key length limit | Medium | Correctness | `CONFIRMED (reproduced)` | **fixed** (`c0c5a516`) |
| 85.6 | `RESTORE` with a non-numeric TTL raises an uncaught `NumberFormatException` | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`05c08552`) |
| 85.7 | `RESTORE` accepts a negative TTL and ignores unknown options | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`aebb27dd`) |
| 85.8 | `RANDOMKEY` only inspects one slot and can answer nil for a non-empty keyspace | Medium | Correctness | `CONFIRMED (reproduced)` | **fixed** (`52b95928`) |
| 85.9 | `RANDOMKEY` may return an expired key and ignores the random index | Low | Correctness | `CONFIRMED (reproduced)` | **fixed** (`1ad59c79`) |
| 85.10 | `READONLY`/`READWRITE` answer `+OK` when the cluster support is disabled | Low | Redis protocol compatibility | `CONFIRMED (inspection + differential)` | **fixed** (`a6306ef6`) |
| 85.11 | the `RENAME`/`RESTORE` error texts diverge from Redis 7.2 | Low | Redis protocol compatibility | `CONFIRMED (inspection + differential)` | **fixed** (`a944f57b`) |
| 85.12 | an unknown `r*` command answers a nil instead of `ERR unknown command` | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | **fixed** (`f645ca48`) |
| 85.13 | split hash storage mode: `RENAME` of a hash fails, `RESTORE` misses the busy key | Low | Correctness | `CONFIRMED (reproduced)` | **partly fixed** (`059c0674`, the `RESTORE` data loss half); the `RENAME` half needs a product decision on the split hash mode |
| 85.14 | cross-worker `RPOPLPUSH`/`LMOVE`/`BLMOVE` hang the client when the destination write fails (masked by 85.1) | Medium | Correctness / Availability | `OPEN` — filed by agent 1 in the round 1 response | **fixed** (`dee8ffd0`) |
| 85.15 | the other command groups answer a nil for an unimplemented command | Low | Redis protocol compatibility | `OPEN` — filed by agent 1 during the fix phase | **fixed** (`16b96e12`, follow ups `618f7acc` and `4943c4c0`) |
