# Bug 90 — Blocking Sorted-Set Commands (BZPOPMIN / BZPOPMAX / BZMPOP) Review (Round 1)

**Module:** `io.velo.command.BGroup` (new blocking sorted-set pops) + `io.velo.command.BlockingZSet` (new) + the `io.velo.command.ZGroup` save path
**Round:** 1
**Author:** AI agent 1 (feature commit `33458c51`, no separate stage-1 discovery report — this document was written directly by the reviewer)
**Reviewer:** AI agent 2
**Status:** **agent-2 C1-C5 re-review (2026-11-19, HEAD `fa086da7`): C1/C2/C3/C5 fixed; C4 partially fixed.** All 76 focused tests/probes passed and cleanup coverage was inspected. Remaining changes requested concern the command-gap document's CLIENT REPLY claims and inconsistent totals; see the latest appended re-review. Earlier feedback is retained as history.
**Review branch:** `main` (feature commit is on `main`)
**Reviewed commit:** `33458c51ca6f55a8e4c4aded5becabe3f3396f34`
**Follow-up commit reviewed for context:** `cf50f306547621f486d39dd37516cbe614537384` (fixes 90.3)

> Delta declaration: n/a (round 1). The reviewed commit reverses the "BZMPOP/BZPOPMAX/BZPOPMIN → `NOT_SUPPORT` by design" decision recorded in `bug_43_sorted_set_module_review_round_1.md` (lines 671-673) and `bug_87_zgroup_commands_review_round_1.md` (line 666); those records are stale after this commit (see 90.8).

## Scope

Files read (method + line ranges), all at the reviewed commit `33458c51` unless stated:

- `src/main/java/io/velo/command/BGroup.java` — `static` command registration (96-119), `parseSlots()` (275-303, bzmpop 275-289 / bzpopmax-min 294-303), `handle()` (371-377), `bzpop()` (1522-1630), `bzmpop()` (1638-1773), `popOtherKeyZAsync()` (1775-1800), `popOtherKeyZMpopAsync()` (1802-1828), `popZSetOneReply()` (1830-1854), `popZSetValuesReply()` (1856-1876), `addZSetBlockingWaiter()` (1878-1890), `removeZSetBlockingWaiter()` (1892-1903), `recheckBlockingZSet()` (1905-1917), `completeIfZSetNotEmpty()` (1930-1968)
- `src/main/java/io/velo/command/BlockingZSet.java` (new, 364 lines) — `PromiseWithMinOrMaxAndCreatedTime` (29-113), `Inner` (196-227), `addOne/removeOne` (233-248), `blockingClientCount/blockingKeyCount` (250-268), `onZSetSaved()` (284-311), `completeIfZSetNotEmpty()` delegate (320-328), `removeBySocket()` (330-352), `clearBlockingZSetPromisesForAllKeys()` (356-362)
- `src/main/java/io/velo/command/ZGroup.java` — `parseSlots()` (302-437; the store variants exclude the destination key, 353-390), `getRedisZSet()` (655-673), `saveRedisZSet()` (675-698; empty-zset early return 684-687, notification 697), `zpopmax()` (1695-1738), `zunionstore` non-cross-worker destination write (1354-1377) vs. the dispatched cross-worker write (1429-1441)
- `src/main/java/io/velo/BaseCommand.java` — `slot()` (600-692), `getCv()` (756-805), `get()` (920-937), `set()` (1079-1140)
- `src/main/java/io/velo/RequestHandler.java` — `parseSlots()` (190-236)
- `src/main/java/io/velo/MultiWorkerServer.java` — `handleRequest()` worker routing (560-635), `isCrossRequestWorker` detection (577-593), `transferAsyncReply()` (688-712), `initBySlotWorkerEventloopArray` call site (1118-1122), PING fast path (430)
- `src/main/java/io/velo/SocketInspector.java` — close path → `BlockingZSet.removeBySocket()` (1033-1036)
- `src/main/java/io/velo/persist/OneSlot.java` — `global_blocking_client_count` gauge (2972-2977)
- `src/main/java/io/velo/reply/ErrorReply.java` — `WRONG_TYPE` (98-99), `INVALID_INTEGER` (76), `RANGE_OUT_OF_INDEX` (92)
- `src/main/java/io/velo/reply/NilReply.java`, `reply/AsyncReply.java`, `reply/BulkReply.java` (double formatting via `DoubleFormat`)
- `src/main/java/io/velo/type/RedisZSet.java` — `ScoreValue` (53-113), `pollFirst()/pollLast()` (237-252)
- `dyn/src/io/velo/command/InfoCommand.groovy` — clients section (180-187)
- `src/test/groovy/io/velo/command/BGroupTest.groovy` — `test bzpopmin` (1826-2063), `test bzpopmax` (2065-2121), `test bzmpop` (2123-2376)
- `src/test/groovy/io/velo/command/BlockingZSetTest.groovy` (new, 365 lines) — all three specs
- `src/test/groovy/io/velo/command/ZGroupTest.groovy` / `ZGroupPart2Test.groovy` — split of the former single spec (diffed the `def '…'` method lists: all preserved, only `test blocking sorted set pops not supported` removed)
- `REDIS_COMMANDS_NOT_IMPLEMENTED.md` (new, 368 lines) — the whole file
- `doc/bug_reviews/bug_43_sorted_set_module_review_round_1.md` (671-679), `doc/bug_reviews/bug_87_zgroup_commands_review_round_1.md` (3-20, 666), `doc/bug_reviews/STATE.md`

Reference docs (ground truth):
- Redis 7.2 `BZPOPMIN` — https://redis.io/docs/latest/commands/bzpopmin/
- Redis 7.2 `BZPOPMAX` — https://redis.io/docs/latest/commands/bzpopmax/
- Redis 7.2 `BZMPOP` — https://redis.io/docs/latest/commands/bzmpop/
- Redis 7.2 source `src/timeout.c` (`getTimeoutFromObjectOrReply`, lines 165-201 — the timeout texts live **here**, not in `t_zset.c`) — https://github.com/redis/redis/blob/7.2/src/timeout.c
- Redis 7.2 source `src/t_zset.c` (`bzmpopCommand`/`zmpopGenericCommand`, `genericZpopCommand`, the `numkeys`/`count` texts) — https://github.com/redis/redis/blob/7.2/src/t_zset.c
- Redis 7.2 source `src/blocked.c` (null-array reply on timeout) — https://github.com/redis/redis/blob/7.2/src/blocked.c

Method: **static inspection only**. The workspace was held by another running job, so no Gradle build, no Spock run and no live Redis 7.2 differential was executed for this round; every verdict below is therefore `CONFIRMED (inspection)` and each finding carries a reproduction recipe in the reviewer checklist for the fix agent to run RED-first.

## Finding 1 (Bug 90.1): a delivered multi-key waiter is not removed from the other keys it was registered on

**Severity:** Medium
**Class:** Correctness (resource leak) / Observability
**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`99c842f1` plus terminal-error cleanup `45a91226`); the previously failing successful/error-path cleanup probes now pass.

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1878-1890 — addZSetBlockingWaiter(): registered on EVERY key
private void addZSetBlockingWaiter(ArrayList<String> keys, BlockingZSet.PromiseWithMinOrMaxAndCreatedTime one) {
    for (int i = 0; i < keys.size(); i++) {
        var key = keys.get(i);
        var slot = slotWithKeyHashListParsed.get(i).slot();
        localPersist.oneSlot(slot).asyncExecute(() -> BlockingZSet.addOne(key, one));
    }
}

// src/main/java/io/velo/command/BGroup.java:1960-1965 — completeIfZSetNotEmpty(): removed from the DELIVERING key only
BlockingZSet.removeOne(Wal.keyString(keyBytes), one);
if (one.mpopCount() > 0) {
    one.settablePromise().set(popZSetValuesReply(baseCommand, dictMap, slotWithKeyHash, keyBytes, rz, one.isMin(), one.mpopCount()));
} else {
    one.settablePromise().set(popZSetOneReply(baseCommand, dictMap, slotWithKeyHash, keyBytes, rz, one.isMin()));
}
```
The same one-key-only removal happens in the two early exits of the same method (`BGroup.java:1945` type-mismatch branch, `BGroup.java:1956` lost-race branch). `removeZSetBlockingWaiter(keys, one)` (`BGroup.java:1892-1903`), which removes from all keys, is only reached from the timeout callback (`BGroup.java:1599`, `BGroup.java:1739`).

### Root cause
The delivery path removes the waiter from the map of the key that served it, but a multi-key block registers the *same* waiter object in the map of every key (`addZSetBlockingWaiter`). The waiter therefore survives in the other keys' maps after its promise has been completed. The stale entry is only reclaimed when that other key is written again (`BlockingZSet.onZSetSaved()` drops waiters whose promise is already complete, `BlockingZSet.java:298-306`) or when the client socket closes (`BlockingZSet.removeBySocket`).

### Trigger / impact
`BZPOPMIN A B 0` (or `BZMPOP 0 2 A B MIN`) blocked on two keys; a member is added to `A`; the client is served from `A`. The waiter stays registered on `B` until `B` is written or the socket closes. Impact:
- `global_blocking_client_count` (`OneSlot.java:2975-2976`) over-reports blocked clients;
- `BlockingZSet.blockingKeyCount()` over-reports blocked keys (also surfaced through `INFO`, see 90.5);
- the completed promise and the `ITcpSocket` reference are retained (memory retention per blocked client);
- Redis has no such state: a served `BZPOPMIN` removes the client from every key it was blocked on.

Note: the identical shape exists in the accepted `BlockingList` code (`BlockingList.setReplyIfBlockingListExist` removes the waiter from the serving key only) — the fix should be applied to both, or the shape should be consciously accepted for the list side.

### Suggested fix direction
Carry the registered key set in the waiter (or pass the key list to the completion) and remove the waiter from **every** key it was registered on when it is delivered, not only from the serving key. The removal must be dispatched to each key's owning worker (`localPersist.oneSlot(slot).asyncExecute(...)`) exactly like the registration, and must be idempotent (the timeout path may already have removed it).

## Finding 2 (Bug 90.2): a zset written by a worker that does not own the key never wakes the blocked client

**Severity:** Medium
**Class:** Concurrency / Correctness (client hang)
**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`ccba406e`); independently verified with real two-worker persistence, wake-up and failed-write propagation. The committed test helper needs follow-up C5.

### Cited files / lines
```java
// src/main/java/io/velo/command/ZGroup.java:353-390 — parseSlots(): the store destination is EXCLUDED
if ("zdiffstore".equals(cmd) || "zinterstore".equals(cmd) || "zunionstore".equals(cmd)) {
    ...
// reuse zdiff method, need to keep the order, exclude dst key
//            var dstKeyBytes = data[1];
//            var dstSlotWithKeyHash = slot(dstKeyBytes, slotNumber);
//            slotWithKeyHashList.add(dstSlotWithKeyHash);
```
```java
// src/main/java/io/velo/command/ZGroup.java:1371-1374 — non-cross-worker path: the DESTINATION is saved on the SOURCE worker
if (doStore) {
    saveRedisZSet(rz, dstSlotWithKeyHash);
    return new IntegerReply(rz.size());
}
// (the cross-worker path at 1435-1438 does dispatch: dstOneSlot.asyncExecute(() -> saveRedisZSet(finalRz, dstSlotWithKeyHash));)
```
```java
// src/main/java/io/velo/command/BlockingZSet.java:284-297 — onZSetSaved(): the inner is resolved from the CURRENT thread
public static void onZSetSaved(BaseCommand.SlotWithKeyHash slotWithKeyHash, BaseCommand baseCommand, DictMap dictMap) {
    var key = slotWithKeyHash.rawKey();
    var inner = getInnerOrNull();
    if (inner == null) { return; }
    var list = inner.blockingZSetPromisesByKey.get(key);
```

### Root cause
`isCrossRequestWorker` is computed from the parsed slot list only (`MultiWorkerServer.java:577-593`), and the parsed list of `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE` excludes the destination key. When all *source* keys happen to live on one worker but the *destination* key lives on another worker, the non-cross-worker branch saves the destination zset **from the source worker**.

*Refined by the independent verification (see the section at the end of this document):* the save does not silently corrupt a foreign slot — the write path is `saveRedisZSet` → `BaseCommand.set` → `putToOneSlot` → `OneSlot.put` → `checkCurrentThreadId()` (`OneSlot.java:1601-1606`), which **throws `IllegalStateException("Thread id not match …")` before the value is written and before `onZSetSaved()` runs**. So the defect is "the destination save is attempted on the wrong worker and fails", not "an unsynchronised write happens". The mock `InMemoryGetSet` bypasses that guard, so a mock-only experiment cannot establish the production behaviour.

### Trigger / impact
`slotWorkers >= 2`; `ZUNIONSTORE dst 2 k1 k2` with `k1,k2` on worker A and `dst` on worker B:
- the store command **fails** with an `IllegalStateException` instead of writing the destination;
- the destination key is never written, so a client blocked with `BZPOPMIN dst 0` is never woken → it hangs until its timeout, and **forever** when the timeout is 0.

The blocked-client hang is a consequence of the failed store, not of a lost notification after a successful store — the write never reaches `onZSetSaved()`.

### Suggested fix direction
Always dispatch the destination save to the destination slot's owning worker (`localPersist.oneSlot(dstSlotWithKeyHash.slot()).asyncExecute(() -> saveRedisZSet(rz, dstSlotWithKeyHash))`), like the cross-worker branch already does — the notification then runs on the owner worker and finds the waiters. The write failure and the store reply must also reach the caller instead of escaping as an exception. The same audit applies to `ZGroup.java:2126`, `2178`, `2229` (already dispatched) and to `GGroup.java:1017/1028` / `RGroup.java:595`.

## Finding 3 (Bug 90.3): the numeric-argument error replies do not match Redis 7.2

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection)` — **already fixed by the follow-up commit `cf50f306`**, kept here as the audit trail

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1539-1546 — bzpop(): timeout parsing
double timeoutSeconds;
try {
    timeoutSeconds = Double.parseDouble(new String(timeoutBytes));
} catch (NumberFormatException e) {
    return ErrorReply.INVALID_INTEGER;          // redis: "timeout is not a float or out of range"
}
if (timeoutSeconds < 0 || !Double.isFinite(timeoutSeconds)) {
    return new ErrorReply("timeout is not a float or out of range");   // redis: "timeout is negative" for < 0
}
```
The same two patterns repeat in `bzmpop()` (`BGroup.java:1645-1651`), plus `numkeys` (`1655-1659`: `INVALID_INTEGER` / `RANGE_OUT_OF_INDEX`) and `count` (`1680-1687`: `INVALID_INTEGER`).

### Root cause
The new code reuses Velo's generic `ErrorReply.INVALID_INTEGER` / `RANGE_OUT_OF_INDEX` sentinels instead of the Redis-specific texts that `getTimeoutFromObjectOrReply()` / `bzmpopCommand()` produce.

### Trigger / impact
Any client that parses the error text (or a byte-level differential test) sees a different reply than Redis 7.2:
- `BZPOPMIN k x` → `ERR value is not an integer or out of range` (Redis: `ERR timeout is not a float or out of range`);
- `BZPOPMIN k -1` → `ERR timeout is not a float or out of range` (Redis: `ERR timeout is negative`);
- `BZMPOP 1 abc k MIN` → `ERR value is not an integer or out of range` (Redis: `ERR numkeys should be greater than 0`);
- `BZMPOP 1 0 k MIN` → `ERR value is out of range` (Redis: `ERR numkeys should be greater than 0`);
- `BZMPOP 1 1 k MIN COUNT abc` / `COUNT 0` → `ERR value is not an integer or out of range` (Redis: `ERR count should be greater than 0`).

### Suggested fix direction
Already done in `cf50f306` — keep the Redis texts per argument class, and assert them in the spec. Nothing further to do beyond making sure the regression test names the Redis text (the current spec asserts the texts, which is what made the fix possible).

## Finding 4 (Bug 90.4): the multi-key wrong-type error leaks the internal exception message

**Severity:** Low
**Class:** Redis protocol compatibility / Error handling
**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`1f380ffd`); real asynchronous two-worker checks passed for all three blocking zset commands.

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1577-1579 — bzpop() multi-key continuation
log.error("{} error={}", isMin ? "bzpopmin" : "bzpopmax", e.getMessage());
finalPromise.setException(Utils.wrapException(isMin ? "bzpopmin" : "bzpopmax", e));
```
```java
// src/main/java/io/velo/MultiWorkerServer.java:694-698 — transferAsyncReply(): the exception message becomes the error text
return promise.map((r, e) -> {
    if (e != null) {
        var errorReply = new ErrorReply(e.getMessage());
```
The single-key path answers `ErrorReply.WRONG_TYPE` (`BGroup.java:1546-1549`, `ErrorReply.java:98-99` = `WRONGTYPE Operation against a key holding the wrong kind of value`).

### Root cause
`ZGroup.getRedisZSet()` raises `TypeMismatchException("Expect sp type array=[-8192], but got sp type=…")`; `Utils.wrapException` keeps the original message when it is non-null (`Utils.java:124-129`), and `transferAsyncReply` turns that message into the client-visible error text.

### Trigger / impact
`BZPOPMIN k1 k2 0` where `k2` holds a hash → the blocked client receives
`ERR Expect sp type array=[-8192], but got sp type=-4` instead of the Redis
`WRONGTYPE Operation against a key holding the wrong kind of value`. The single-key form is already Redis-aligned → the two forms of the same command disagree.

*Refined by the independent verification and by its D3 clarification:* the **synchronous** completion check maps a preserved `TypeMismatchException` to `ErrorReply.WRONG_TYPE` (`BGroup.java:1767-1771`), so BZMPOP answers the Redis text when the check completes inline (all keys on the same worker). The **asynchronous** failure path is still in scope: it goes through `BGroup.java:1736-1739`, returns an `AsyncReply`, and its exception message is published by `MultiWorkerServer.java:694-698` — so the leak remains for BZPOPMIN/BZPOPMAX (always) and for BZMPOP when the check completes asynchronously (a second key on another worker). That BZMPOP case was inspected, not reproduced live (open item O1).

### Suggested fix direction
Map `TypeMismatchException` to `ErrorReply.WRONG_TYPE` before completing the promise (the same mapping already exists in `bzmpop()`'s inline branch, `BGroup.java:1745-1750` and `1768-1773`), and log the internal detail server-side only.

## Finding 5 (Bug 90.5): `INFO` does not count the blocked sorted-set clients

**Severity:** Low
**Class:** Observability
**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`dda64496`) for INFO inclusion and unique-waiter counting; stale registrations from C2/C3 remain separate cleanup defects.

### Cited files / lines
```groovy
// dyn/src/io/velo/command/InfoCommand.groovy:180-187
connected_clients:${socketInspector.connectedClientCount()}
cluster_connections:0
maxclients:${socketInspector.maxConnections()}
blocked_clients:${BlockingList.blockingClientCount()}
total_blocking_keys:${BlockingList.blockingKeyCount()}
```
```java
// src/main/java/io/velo/persist/OneSlot.java:2975-2976 — the prometheus gauge DOES count them
map.put("global_blocking_client_count", new SimpleGauge.ValueWithLabelValues(
        (double) (BlockingList.blockingClientCount() + BlockingZSet.blockingClientCount()), labelValues));
```

### Root cause
The commit wired `BlockingZSet.blockingClientCount()` into the Prometheus gauge but not into the `INFO` clients section, so the two observability surfaces disagree.

### Trigger / impact
A client blocked with `BZPOPMIN`/`BZPOPMAX`/`BZMPOP` is invisible in `INFO clients` (`blocked_clients`, `total_blocking_keys`) although it is counted in `global_blocking_client_count`. Operators debugging a blocked client see `blocked_clients:0`.

### Suggested fix direction
Add `BlockingZSet.blockingClientCount()` / `BlockingZSet.blockingKeyCount()` to the `INFO` clients section (sum with the list counters), and add the same keys to the `INFO` spec.

## Finding 6 (Bug 90.6): `REDIS_COMMANDS_NOT_IMPLEMENTED.md` lists commands that are implemented

**Severity:** Low
**Class:** Documentation
**Status:** `PARTIALLY CONFIRMED` — `f54c0703` removes some false positives, but supported subcommands remain listed as missing (C4); no final fixed disposition.

### Cited files / lines
```markdown
<!-- REDIS_COMMANDS_NOT_IMPLEMENTED.md:37-62 — "Connection (21 commands)" -->
- **AUTH** - Authenticates a connection
- **CLIENT** - A container for client connection commands
- **GETNAME** - Returns name of a connection
- **HELLO** - Handshakes with a Redis server
- **PING** - Returns server's liveliness response
- **QUIT** - Closes a connection
```
Implementation evidence: `PING` → `MultiWorkerServer.java:430` (`PongReply.INSTANCE`); `AUTH` → `RequestHandler.java:453-460` + `HGroup.java:653`; `QUIT` → `MultiWorkerServer.java:531`; `HELLO` → `HGroup.java:312`; `CLIENT` → `CGroup.java:26,116`; `CLIENT GETNAME` → `CGroup.java:153`.

*Refined by the independent verification — two of my original examples were wrong and are withdrawn:* `SELECT` is listed correctly (Velo answers `ErrorReply.NOT_SUPPORT`, `SGroup.java:424-425`), and the document's `Object (5 commands)` section lists the subcommands `ENCODING`/`FREQ`/`IDLETIME`/`OBJECT-HELP`/`REFCOUNT` — not an `OBJECT` container entry (Velo implements `DEBUG OBJECT` only, `DGroup.java:115-116`; the bare `OBJECT` command answers unknown command, `OGroup.java:49-50`). `GETNAME` is a `CLIENT` subcommand, not a standalone Redis command, so listing it as a separate missing command is a classification issue rather than a missing implementation.

### Root cause
The new document was produced by a `grep` for `if ("cmd".equals(cmd))` patterns only, so the commands handled through other shapes (connection commands handled in `RequestHandler`/`MultiWorkerServer`, and the `xxx subcmd` dispatch form) were reported as missing. The document itself claims "comprehensive verification … based on actual command handlers".

### Trigger / impact
The next agent (or a reviewer) uses the document as the work list and re-implements or re-reports commands that already work — the same risk the `bug_87`/`bug_43` rows of `STATE.md` try to avoid. The `Sorted Set` section of the same document is correct (it records the new `BGroup` implementation), which makes the wrong sections harder to spot.

### Suggested fix direction
Regenerate the document from the real dispatch table (`CommandRegistry` entries + the `RequestHandler` connection-command list + the `cmd subcmd` forms), or drop the "missing" claim for every command that has a real handler. A registry entry proves routing, not implementation — the document should distinguish **supported**, **partially supported containers** (CLIENT) and **explicitly unsupported** (`NOT_SUPPORT`, e.g. SELECT) instead of a flat missing list.

## Finding 7 (Bug 90.7): the timeout reply is a null bulk, Redis answers a null array

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection)`
**Disposition:** `fixed` (`7d4bd1b7`) for the scoped blocking-pop timeout paths; direct encoders and focused timeout tests passed. A live TCP capture remains additional evidence.

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1598-1601 and 1738-1741 — the timeout answer
if (one.tryMarkDelivered()) {
    finalPromise.set(NilReply.INSTANCE);
```
```java
// src/main/java/io/velo/reply/NilReply.java:19-24 — RESP2 encoding
private static final byte[] NIL = new BulkReply().buffer().asArray();   // "$-1\r\n"
```

### Root cause
Redis answers a blocked pop that times out with a **null array** (`*-1\r\n`, see `src/blocked.c` in Redis 7.2 — `addReplyNullArray`), while `NilReply` encodes the RESP2 null **bulk** (`$-1\r\n`).

### Trigger / impact
A byte-level differential against Redis 7.2 shows `$-1` instead of `*-1` for a timed-out `BZPOPMIN`/`BZPOPMAX`/`BZMPOP`. Clients that distinguish the two (or test harnesses that compare the raw frame) see a difference. The same divergence already exists for `BLPOP`/`BRPOP`/`BLMPOP` — it is systemic, not introduced here, but the new commands inherit it.

### Suggested fix direction
Use a dedicated null-array reply for the blocking-pop timeout path (and, in the same pass, for the list blocking pops), or record the divergence as an accepted, documented difference.

## Finding 8 (Bug 90.8): the "NOT_SUPPORT by design" decision records were not updated

**Severity:** Low
**Class:** Process / documentation
**Status:** `REFINED` — STATE.md was already current during verification; the older bug_43/bug_87 notes needed supersession.
**Disposition:** `fixed` (`94ed9044`), verified by the explicit supersession notes in both reports.

### Cited files / lines
```markdown
<!-- doc/bug_reviews/bug_43_sorted_set_module_review_round_1.md:671-673 -->
- `BZMPOP` returns `ErrorReply.NOT_SUPPORT`.
- `BZPOPMAX` returns `ErrorReply.NOT_SUPPORT`.
- `BZPOPMIN` returns `ErrorReply.NOT_SUPPORT`.
```
```markdown
<!-- doc/bug_reviews/bug_87_zgroup_commands_review_round_1.md:666 -->
- `bzmpop`/`bzpopmax`/`bzpopmin` → NOT_SUPPORT — bug_43 implemented decision, tests exist
```
```markdown
<!-- doc/bug_reviews/STATE.md — the ZGroup row -->
| 87 | ZGroup commands (whole class: all Z* commands; bzmpop/bzpopmax/bzpopmin are bug_43 NOT_SUPPORT by design) | 1 | ... |
```
*Partially done:* `STATE.md` was updated when this report was filed (the bug_87 row now records that the decision was reversed by `33458c51`, and the bug_90 rows were added). The **bug_43 and bug_87 notes themselves** still describe the intentional `NOT_SUPPORT` and still need a supersession note.

### Root cause
The commit implements the three commands and drops the `test blocking sorted set pops not supported` spec, but the design decision recorded in bug_43 / bug_87 still says the commands are intentionally `NOT_SUPPORT`.

### Trigger / impact
A later round (bug_87 round 2, or the ZGroup sweep) reads the stale record and either re-reports the commands as missing, or treats the new implementation as an undocumented deviation from the agreed design.

### Suggested fix direction
Append a short supersession note to `bug_43` and `bug_87` stating that the `NOT_SUPPORT` decision was reversed by commit `33458c51`, with the new implementation reviewed in this document (bug_90). `STATE.md` is already updated — keep it in sync with the final verdicts.

## Finding 9 (Bug 90.9): the BZMPOP reply is flat, Redis emits one array per member

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — raised by the independent verification (probe + Redis 7.2.14 source); it **rejects** the "reply shape matches Redis" non-finding of the first version of this document
**Disposition:** `fixed` (`42110363`); nested immediate/wake-up expectations and focused coverage verified during committed-fix review.

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1874-1884 — popZSetValuesReply(): a FLAT array of members and scores
var valueReplies = new Reply[min * 2];
for (int j = 0; j < min; j++) {
    var sv = isMin ? rz.pollFirst() : rz.pollLast();
    valueReplies[j * 2] = new BulkReply(sv.member());
    valueReplies[j * 2 + 1] = new BulkReply(sv.score());
}
var arr = new Reply[]{new BulkReply(keyBytes), new MultiBulkReply(valueReplies)};
return new MultiBulkReply(arr);
```
Redis ground truth (`/home/kerry/ws/redis`, `src/version.h` = **7.2.14**, `src/t_zset.c`):
```c
/* genericZpopCommand(), use_nested_array && emitkey -> ZMPOP/BZMPOP in RESP2 and RESP3. */
} else if (use_nested_array && emitkey) {
    /* ZMPOP/BZMPOP in RESP2 and RESP3. */
    addReplyArrayLen(c, 2);
    addReplyBulk(c, key);
    addReplyArrayLen(c, rangelen);
}
...
if (use_nested_array) {
    addReplyArrayLen(c,2);
}
addReplyBulkCBuffer(c,ele,sdslen(ele));
addReplyDouble(c,score);
```

### Root cause
`popZSetValuesReply()` puts the member and the score as two sibling entries of one flat array instead of wrapping each member/score **pair** in its own two-element array.

### Trigger / impact
`BZMPOP 0 1 k MIN COUNT 2` with the members `a:1`, `b:2` — Velo answers
`["k", ["a", "1", "b", "2"]]`, Redis answers
`["k", [["a", "1"], ["b", "2"]]]`. The pair array is required even for `COUNT 1` (Redis: `["k", [["a", "1"]]]`). Any client that reads the reply positionally (the documented BZMPOP shape) mis-parses it. Both the immediate pop and the waiter wake-up share `popZSetValuesReply()`, so both inherit the defect.

The existing spec asserts the flat shape (`BGroupTest.groovy:2212-2237`), so its green result does **not** prove compatibility — the expectation itself must change.

### Sibling half — see Finding 90.10 (the non-blocking `ZMPOP`)

Redis uses the **same** nested shape for `ZMPOP` and `BZMPOP`: `zmpopGenericCommand()` calls
`genericZpopCommand(..., emitkey=1, count, use_nested_array=1, reply_nil_when_empty=1, NULL)` for
both (`src/t_zset.c:4444-4449`), and the reply comment states "In ZMPOP/BZMPOP we reply with a
structured nested array containing key name" (`src/t_zset.c:3885`). Velo's non-blocking `ZMPOP`
builds the **same flat** `Reply[min * 2]` array → it is wrong in the same way, in RESP2 *and* RESP3.
This half is outside the reviewed commit (it predates it) and was missed by both review passes; it is
filed as **Finding 90.10**.

### Suggested fix direction
Emit one `MultiBulkReply` per member/score pair (`[key, [[member, score], [member, score]]]`), update `popZSetValuesReply()` and its two callers (the immediate pop and `completeIfZSetNotEmpty()`), and rewrite the BZMPOP expectations in `BGroupTest` to the Redis shape **before** touching the production helper (the reproduction must be RED first). Add a live Redis 7.2.11 wire comparison for BZMPOP — and for the `ZMPOP` sibling half above.

## Finding 10 (Bug 90.10): the non-blocking `ZMPOP` emits flat member/score entries

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection)` — raised by the first review as the sibling half of 90.9 and confirmed independently by the second verification; it predates the reviewed commit (`33458c51`) and was missed by both review passes
**Disposition:** `fixed` (`2490462d`); both branches now share the tested nested-pair helper.

### Cited files / lines
```java
// src/main/java/io/velo/command/ZGroup.java:2760-2772 — zmpop(), same-worker path
var valueReplies = new Reply[min * 2];
for (int j = 0; j < min; j++) {
    var sv = isMin ? rz.pollFirst() : rz.pollLast();
    valueReplies[j * 2] = new BulkReply(sv.member());
    valueReplies[j * 2 + 1] = new BulkReply(sv.score());
}
...
var arr = new Reply[]{keyReply, new MultiBulkReply(valueReplies)};
return new MultiBulkReply(arr);
```
The same flat construction is repeated in the cross-slot path (`ZGroup.java:2838-2849`).

Redis ground truth (`/home/kerry/ws/redis`, 7.2.14, `src/t_zset.c:4444-4449`):
```c
if (is_block) {
    /* BLOCK. We will handle CLIENT_DENY_BLOCKING flag in blockingGenericZpopCommand. */
    blockingGenericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1);
} else {
    /* NON-BLOCK */
    genericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1, NULL);
}
```
`use_nested_array=1` for **both** `ZMPOP` and `BZMPOP`; `genericZpopCommand` then emits one
two-element array per member (`src/t_zset.c:3946-3950`, `4001-4005`).

### Root cause
`ZGroup.zmpop()` puts the member and the score as two sibling entries of one flat array instead of wrapping each member/score **pair** in its own two-element array — the same defect as 90.9, in the pre-existing non-blocking command.

### Trigger / impact
`ZMPOP 1 k MIN COUNT 2` with the members `a:1`, `b:2` — Velo answers `["k", ["a", "1", "b", "2"]]`, Redis answers `["k", [["a", "1"], ["b", "2"]]]`. The pair array is required even for `COUNT 1`. Both Velo paths (same-worker and cross-slot) are affected, in RESP2 and RESP3. Any client that reads the reply positionally mis-parses it.

### Suggested fix direction
Emit one `MultiBulkReply` per member/score pair in both `ZGroup.zmpop()` paths, and add RED expectations that assert the **pair nesting** (not a copy of the existing flat expectations) for both paths. Fix it as its own commit (one bug = one commit), adjacent to but separate from 90.9. The finding belongs to the ZGroup module as well — it can be referenced from the open `bug_87` report.

## Notes / non-findings

Verified correct — kept so later rounds do not re-investigate:

- **The one-shot delivery arbiter works.** `PromiseWithMinOrMaxAndCreatedTime.tryMarkDelivered()` (`BlockingZSet.java:104-111`) is a compare-and-set that runs *before* any value is touched, so exactly one completer may pop and complete the promise even when several workers race (`BGroup.java:1952-1958`). Covered by `BGroupTest` 2021-2043 and `BlockingZSetTest` 320-334.
- **The check → register window is closed.** The single-key form is safe because the check and `BlockingZSet.addOne()` run on the same owner worker (`BGroup.java:1612-1614`); the multi-key form closes the window with `recheckBlockingZSet()` (`BGroup.java:1590`, `1737`).
- **The empty-zset early return in `saveRedisZSet()` cannot lose a wake-up** (`ZGroup.java:684-687`): a zset that became empty has nothing to serve, and the client that emptied it is already awake.
- **The notification covers every zset writer.** All zset writes funnel through `ZGroup.saveRedisZSet()` (`ZGroup.java:675-698`): ZADD/ZINCRBY/ZREM/Z*STORE/ZRANGESTORE, `RGroup` restore (`RGroup.java:595`), the RDB loader (`MyRDBVisitorEventListener.java:131`) — all of them notify.
- **The socket-close cleanup is wired**: `SocketInspector.onClosed()` → `BlockingZSet.removeBySocket()` (`SocketInspector.java:1033-1036`), which dispatches to every slot worker and removes the waiters by socket identity.
- **The waiter maps are only mutated by their owner worker** — registration/removal always go through `localPersist.oneSlot(slot).asyncExecute(...)`, so there is no shared-`HashMap` race.
- **Command routing is sound**: the request is routed to the worker of the *first* key (`Request.getSingleSlot()` → `MultiWorkerServer.java:626-631`), so the inline first-key check and `BlockingZSet.addOne()` in the single-key form run on the owning worker.
- **The BZMPOP reply shape does NOT match Redis 7.2** — this bullet was **wrong** in the first version of this document and is replaced by **Finding 90.9** (the reply is flat, Redis emits one two-element array per member). The member/score ordering (MIN ascending, MAX descending) and the `COUNT` cap are correct; only the nesting is missing.
- **The score formatting matches Redis**: `BulkReply(double)` → `DoubleFormat.format` yields `1`, `1.5`, `26` like Redis' `addReplyDouble`.
- **No test coverage was lost in the spec split**: `ZGroupTest.groovy` was split into `ZGroupTest.groovy` + `ZGroupPart2Test.groovy` (the compiler-OOM workaround); diffing the `def '…'` lists shows every former test method survives, only `test blocking sorted set pops not supported` is dropped — correctly, since the commands are now implemented.
- **`ErrorReply.FORMAT` is the right arity-error sentinel** for the format failures (`BGroup.java:1524`, `1640`): `MultiWorkerServer.java:613-614` rewrites it into the Redis `wrong number of arguments` reply.
- **`getInnerOrNull()` returning null is the right defensive choice** in `onZSetSaved()` (`BlockingZSet.java:287-290`): a notification from a thread that is not a slot worker has no waiter to serve.

## Reviewer checklist (for the fix agent)

Each item is a RED-first recipe (write the test, watch it fail for the expected reason, then fix).

- **90.1 (stale waiter leak)**
  1. `localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())`, mock `InMemoryGetSet`.
  2. Register a waiter on two keys: `BlockingZSet.addOne('k1', one)` and `BlockingZSet.addOne('k2', one)` (or drive it through a multi-key `BZPOPMIN k1 k2 0`).
  3. Put a member in `k1` and call `BGroup.completeIfZSetNotEmpty(BaseCommand.slot('k1', 1), 'k1'.bytes, cmd, cmd.dictMap, one)`.
  4. Assert: the promise is completed **and** `BlockingZSet.blockingClientCount() == 0` (today it stays 1 → RED), and that `k2` no longer holds the waiter.
- **90.2 (wake-up lost on a foreign worker)**
  1. Start a server with `slotWorkers=2` (live e2e) or emulate with two `Eventloop`s and `LocalPersist.fixSlotThreadId`.
  2. Block a client with `BZPOPMIN <dst> 0` where `dst` is owned by worker B.
  3. From worker A run `ZUNIONSTORE <dst> 2 <k1> <k2>` with `k1,k2` owned by worker A.
  4. Assert: the blocked client is answered within a few seconds (today it hangs → RED) and the destination zset is readable from worker B.
- **90.3 (error texts)** — already fixed by `cf50f306`; keep the spec assertions (`BZPOPMIN a x` → `timeout is not a float or out of range`, `BZPOPMIN a -1` → `timeout is negative`, `BZMPOP 1 abc k MIN` → `numkeys should be greater than 0`, `BZMPOP 1 1 k MIN COUNT 0` → `count should be greater than 0`) and re-run them RED against `33458c51` to prove the audit trail.
- **90.4 (wrong-type text)**
  1. Put a hash payload under `k2`, block with `BZPOPMIN k1 k2 0`.
  2. Assert the client's error text is `WRONGTYPE Operation against a key holding the wrong kind of value` (today it is `ERR Expect sp type array=[-8192], but got sp type=…` → RED).
- **90.5 (INFO)** — assert that `INFO clients` reports a non-zero `blocked_clients` while a `BZPOPMIN` client is blocked (today 0 → RED).
- **90.6 (doc)** — grep the document for every command that has a `CommandRegistry` entry or a `RequestHandler` fast path and assert none of them is listed as missing (a small script assertion is enough).
- **90.7 (null array)** — capture the raw frame of a timed-out `BZPOPMIN` and assert it is `*-1\r\n` (today `$-1\r\n` → RED).
- **90.8 (records)** — update `STATE.md` + the bug_43/bug_87 notes; no code test.

## Review Feedback (AI agent 2, round 1)

**Method:** static inspection of the reviewed commit (no build executed — the workspace was held by another job). Every verdict is `CONFIRMED (inspection)`; no finding was reproduced on a live server in this round, so the fix phase must re-derive each RED locally.

> **Reconciliation (after the independent verification appended at the end of this document):** the table below is the first review pass and is kept as history. The independent verification **refined 90.1 (reproduced, single worker), 90.2 (the ownership guard throws — the store fails, it is not a lost notification after a successful write), 90.4 (BZMPOP already maps the type mismatch to `WRONG_TYPE`), 90.6 (my SELECT/OBJECT examples were wrong) and 90.8 (STATE.md was already updated when the report was filed)**, confirmed 90.3/90.5/90.7, and **rejected my "BZMPOP reply shape matches Redis" non-finding** → new finding **90.9**. The per-finding sections above now carry the refined wording, and the Summary table reflects the refined verdicts.

| Finding | Verdict | Evidence |
|---|---|---|
| 90.1 | CONFIRMED (inspection) | `BGroup.java:1878-1890` registers the waiter on every key, `BGroup.java:1960` (and 1945/1956) removes it from the serving key only; `removeZSetBlockingWaiter(keys, one)` (`1892`) is reachable only from the timeout callback (`1599`, `1739`). |
| 90.2 | CONFIRMED (inspection) | `ZGroup.java:361` (the store destination is excluded from the parsed slots) + `MultiWorkerServer.java:577-593` (`isCrossRequestWorker` ignores it) + `ZGroup.java:1373` (destination saved on the source worker) + `BlockingZSet.java:287` (`getInnerOrNull()` resolves the inner of the current, foreign thread). |
| 90.3 | CONFIRMED (inspection) | `BGroup.java:1539-1546`, `1645-1651`, `1655-1659`, `1680-1687` use `ErrorReply.INVALID_INTEGER` / `RANGE_OUT_OF_INDEX` / the wrong timeout text; Redis 7.2 returns `timeout is not a float or out of range` / `timeout is negative` (`src/timeout.c:172,188`) and `numkeys should be greater than 0` / `count should be greater than 0` (`src/t_zset.c:4408,4434`). Fixed by `cf50f306`. |
| 90.4 | CONFIRMED (inspection) | `BGroup.java:1578`/`1723` forward the raw `TypeMismatchException`; `Utils.java:124-129` keeps its message; `MultiWorkerServer.java:696-697` publishes that message as the error text, while the single-key branch (`BGroup.java:1546-1549`) answers `ErrorReply.WRONG_TYPE`. |
| 90.5 | CONFIRMED (inspection) | `dyn/.../InfoCommand.groovy:185-186` uses only `BlockingList`; `OneSlot.java:2975-2976` uses `BlockingList + BlockingZSet`. |
| 90.6 | CONFIRMED (inspection) | The document lists PING/AUTH/CLIENT/GETNAME/SELECT/OBJECT/QUIT as missing; they are implemented at `MultiWorkerServer.java:430`, `RequestHandler.java:40`, `CGroup.java:26,116,153`, `SGroup.java:86,424`, `DGroup.java:116`. |
| 90.7 | CONFIRMED (inspection) | `NilReply.java:19-24` encodes `$-1`; Redis 7.2 `src/blocked.c` answers a null array for a timed-out blocking pop. |
| 90.8 | CONFIRMED (inspection) | `bug_43…:671-673`, `bug_87…:666` and the `STATE.md` bug_87 row still record the `NOT_SUPPORT` decision that this commit reverses. |

### Additional observations

- The implementation deliberately mirrors the accepted `BlockingList`/`BLMPOP` machinery (`BGroup.java` blocking-list half, `BlockingList.java`), including the one-shot arbiter and the per-owner-worker waiter map — the shape is consistent with the code already accepted in bug_72, which is a plus.
- The wake-up design differs from the list side on purpose: the list side hands the pushed values to the waiter, the zset side re-reads the set from storage on the owning worker. The re-read is the right call for a sorted set (the waiter must pop the *lowest/highest* member, not the pushed one), and the re-entrancy of `save → onZSetSaved → pop → save` is correctly guarded by the arbiter plus the `isComplete()` check.
- `BlockingZSet` duplicates ~90% of `BlockingList` (the `Inner` map, the counters, `removeBySocket`); a shared generic `BlockingRegistry<T>` would remove the duplication — optional, not a bug.
- The new `REDIS_COMMANDS_NOT_IMPLEMENTED.md` is the only new documentation artefact and is inaccurate (90.6); it should not be trusted as the gap list until regenerated.

### Recommended fix order (for the fix agent)

1. **90.9** — a wrong wire format for every BZMPOP reply (the client mis-parses the pairs); also needs the spec expectations rewritten to the Redis shape first.
1b. **90.10** — the same flat reply shape in the non-blocking `ZMPOP` (both paths); own commit, same fix.
2. **90.2** — the store fails on a foreign worker and a client blocked on the destination hangs forever (timeout 0).
3. **90.1** — a per-client waiter/metric leak on every multi-key block that is served (reproduced on a single worker).
4. **90.4** — a wrong error text on the BZPOPMIN/BZPOPMAX multi-key wrong-type path (one-line mapping).
5. **90.3** — already fixed in `cf50f306`; verify only.
6. **90.5** — `INFO` observability gap (one-line change + spec row); note that the counters count registrations, not unique clients.
7. **90.6** — regenerate or correct the gap document.
8. **90.8** — append the supersession note to bug_43 / bug_87 (`STATE.md` is already updated).
9. **90.7** — systemic null-array divergence; decide fix-vs-accept (it also affects the list blocking pops).

One bug = one commit, TDD (RED → GREEN) + JaCoCo on the changed lines, per `AGENTS.md` and the skill's Stage 4.

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 90.1 | RED `BGroupTest.test bzpopmin` — `Condition not satisfied: BlockingZSet.blockingClientCount() == 0 | 1 false` (the waiter stayed on the second key) | `99c842f1` `fix: a served blocking zset waiter is released from every key it was registered on` | green (BGroupTest, BlockingZSetTest, ZGroupTest, ZGroupPart2Test) | `BlockingZSet` 236-275 (the release + the empty key entry), `BGroup` 1960-1970 — all covered, the defensive branches partial |
| 90.2 | RED `ZGroupTest.test zunionstore` — `java.lang.IllegalStateException: Thread id not match, thread id=1, thread id protected for safe=70` at `OneSlot.put` → `ZGroup.zdiff(ZGroup.java:1409)` | `ccba406e` `fix: zstore writes the destination on its owner worker and settles the reply after the write` | green (ZGroupTest, ZGroupPart2Test, BGroupTest, BlockingZSetTest, OneSlotTest — 125 tests) | `ZGroup` 1260-1282 (the failed write branch ✓), 1435-1450, 1500-1516 (the cross worker error branch ✓), `OneSlot` 666, `BaseCommand` 713 |
| 90.3 | n/a — fixed by `cf50f306` | `cf50f306` `fix: align numeric-arg error replies with redis texts (timeout/count/cursor/bit/min-max)` | verified (the existing `test bzpopmin` / `test bzmpop` cases assert the redis texts) | n/a |
| 90.4 | RED `BGroupTest.test bzpopmin` — the promise held `SettablePromise{exception=TypeMismatchException}` instead of `ErrorReply.WRONG_TYPE` | `1f380ffd` `fix: the blocking zset wrong type answers the redis wrongtype error` | green (BGroupTest, BlockingZSetTest, ZGroupTest, ZGroupPart2Test, LGroupTest) | `BGroup` 1588-1600 (both the wrong type and the generic error branch), 1744-1756 |
| 90.5 | RED `BlockingZSetTest` — `blockingClientCount() == 1 | 2 false` (one client on two keys counted twice) and `InfoCommandTest.test clients` — `blocked_clients:0` vs `1` | `dda64496` `fix: info counts the blocking zset clients and the counters count the clients, not the key registrations` | green (BlockingZSetTest, InfoCommandTest, BGroupTest, ZGroupTest, ZGroupPart2Test) | `BlockingZSet` 230-275 — the add/remove/counter paths covered |
| 90.6 | n/a — documentation | `f54c0703` `docs: correct the redis commands not implemented list, it listed implemented commands as missing` | verified by a script over the `CommandRegistry` + the connection fast paths: no listed command is implemented any more | n/a |
| 90.7 | RED `BGroupTest` — the timed out blocking pop answered `$-1` instead of `*-1` (4 test methods failed) | `7d4bd1b7` `fix: a timed out blocking pop answers a null array like redis` | green (BGroupTest, BlockingListTest, NullArrayReplyTest) | `NullArrayReply` 7/7 lines covered (the new direct spec) |
| 90.8 | n/a — documentation | `94ed9044` `docs: note that the blocking sorted set pops are implemented since 33458c51` | n/a | n/a |
| 90.9 | RED `BGroupTest.test bzmpop` — `GroovyCastException at BGroupTest.groovy:2134` (the reply was flat, the pair was a `BulkReply`) | `42110363` `fix: bzmpop replies with one array per member like redis` | green (BGroupTest, BlockingZSetTest, ZGroupTest, ZGroupPart2Test) | `BGroup` 1871-1887 — 9/9 lines, both branches of the min/max ternary |
| 90.10 | RED `ZGroupPart2Test.test zmpop` — `GroovyCastException at ZGroupPart2Test.groovy:1293` | `2490462d` `fix: zmpop replies with one array per member like redis` | green (ZGroupPart2Test, ZGroupTest, BGroupTest, BlockingZSetTest) | `ZGroup` 2702-2709 (the helper), 2777-2785 (the same worker path), 2848-2856 (the cross slot path) |
| follow-up (the list side of 90.1 / 90.5, O6) | RED `BlockingListTest` — the waiter stayed on the second key after the first one was served | `a17223f4` `fix: the blocking list releases the waiter from every key and counts the clients` | green (BlockingListTest, BGroupTest, BlockingZSetTest, InfoCommandTest, ZGroupTest) | `BlockingList` 200-250 — the add/remove/release/counter paths covered |
| C1 (the duplicate key regression of `a17223f4`) | RED `BlockingListTest` — `Condition not satisfied: BlockingList.blockingKeyCount() == 0 | -1 false` (the same waiter registered twice on one key) and the delivery could raise a `ConcurrentModificationException` | `de9fe620` `fix: a blocking client is registered once per distinct key and the global cleanup runs after the delivery iteration` | green (BlockingListTest, BGroupTest) | `BGroup` 1192-1201 (the distinct key registration, both branches), `BlockingList` 410-425 (the delivery iteration) |
| C2 (the list recheck delivery) | RED `BGroupTest.test blpop` — `BlockingList.blockingClientCount() == 0 | 1 false` after `completeIfListNotEmpty` | `c05fa127` `fix: the blocking list recheck delivery releases the waiter from every key` | green (BGroupTest, BlockingListTest) | `BGroup` 1269-1295 (the recheck helper, both the success and the arbiter branch) |
| C3 (the zset wrong type completion) | RED `BGroupTest.test bzpopmin` — `BlockingZSet.blockingClientCount() == 0 | 1 false` after the wrong type completion | `45a91226` `fix: the blocking zset wrong type completion releases the waiter from every key` | green (BGroupTest, BlockingZSetTest) | `BGroup` 1952-1970 (the wrong type branch, both the arbiter and the completion) |
| C4 (the gap document subcommands) | n/a — documentation, verified by the `CommandRegistry` + the client subcommand handlers | `3b37482a` `docs: the gap list drops the implemented client and acl subcommands` | the document lists no implemented command any more | n/a |
| C4 (the second re-review corrections: the stale `CLIENT REPLY` claims and the aggregate counts) | n/a — documentation, verified by a script over the document sections | `930b12b1` `docs: the gap list drops the implemented client reply and reconciles the aggregate counts` | every section header matches its entry list and every table row reconciles (`total - missing = implemented`), checked by a script | n/a |
| C5 (the destination read assertion) | RED `ZGroupTest.test zunionstore` — `Condition not satisfied: stored | false` (the read was not awaited, `Promise.getResult()` does not block) | `4ed30995` `test: the zstore destination read is awaited, the promise result alone is read before the worker completed it` | green (ZGroupTest, ZGroupPart2Test, BGroupTest, BlockingListTest, BlockingZSetTest, InfoCommandTest) | the read helper is a test helper, the production lines of `ccba406e` are unchanged and stay covered |

**Regression after the fix phase:** `io.velo.command.*` + `io.velo.reply.*` + `io.velo.persist.*` → **801 tests, 0 failures, 0 errors** (`./gradlew -I /tmp/velo-groovy-compiler-heap.init.gradle :test --tests "io.velo.command.*" --tests "io.velo.reply.*" --tests "io.velo.persist.*"`).

**Regression after the re-review fixes (C1-C5):** the same selection → **802 tests, 0 failures, 0 errors**.

**Environment note:** `compileTestGroovy` runs out of heap with the default in-daemon Groovy compiler, the
focused runs above used `-I /tmp/velo-groovy-compiler-heap.init.gradle` (it only sets the Groovy
compiler fork heap to 8g, it changes no repository file).

## Re-review (AI agent 2)

Completed at `b85565eb`; see the appended **Review Feedback — committed fixes**. Overall verdict:
**changes requested**, with per-commit acceptance and five follow-up items.

## Residual Risk

- **The cross-worker destination write in the `*STORE` commands is pre-existing** (`ZGroup.java:1373`): the destination zset is written from a foreign worker. 90.2 fixes the wake-up by dispatching the save; the data race itself is the same fix's side effect and should be regression-tested (read the destination from its owner worker after the store).
- **The list blocking pops carried the same waiter leak and the same registration counter** — closed by the follow-up commit `a17223f4` (`BlockingList` now releases the waiter from every key, drops the empty map entries and counts the clients).
- **No live differential against Redis 7.2.11 exists for these three commands.** Every new test is a unit test over the mocked `InMemoryGetSet`; a live wire-level comparison (reply shapes, error texts, the timeout frame, the multi-key ordering) is still missing and should be produced together with the 90.2 fix, which needs a two-worker server anyway.
- **The multi-worker paths have no coverage**: all the new specs use a single slot and a single worker. Note that this only applies to the **multi-worker** aspects — 90.1 (the waiter leak) and the 90.9 reply shape reproduce on a single worker, while 90.2 needs a `slotWorkers=2` server.
- **The zset blocking commands are not covered by `bug_87`** (ZGroup round 1, 11 findings still open) — the two reports now overlap; the round-2 sweep must not re-report 90.1/90.2/90.4/90.5.
- **`BlockingZSet` duplicates `BlockingList`**: a future change to one registry can silently diverge from the other.
- **The non-blocking `ZMPOP` has the same flat reply shape as `BZMPOP`** (Redis nests for both, `src/t_zset.c:4444-4449`): a sibling half of 90.9 that predates the reviewed commit and was missed by both review passes — it needs its own finding and the same fix.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 90.1 | A delivered multi-key waiter is not removed from the other keys it was registered on | Medium | Correctness / Observability | CONFIRMED (reproduced) — scope refined: it happens on a **single** worker too; the counters count registrations, not clients, and empty map entries are never dropped | fixed (`99c842f1`) |
| 90.2 | The `*STORE` destination is saved from the wrong worker: the ownership guard throws, the store fails and the client blocked on the destination hangs | Medium | Concurrency | CONFIRMED (reproduced) — **refined**: the write is rejected by `OneSlot.checkCurrentThreadId()` before the value is written, so this is a failed store, not a lost notification after a successful one | fixed (`ccba406e`) |
| 90.3 | The numeric-argument error replies do not match Redis 7.2 | Low | Redis protocol compatibility | CONFIRMED (inspection) | fixed (`cf50f306`) |
| 90.4 | The multi-key wrong-type error leaks the internal exception message | Low | Redis protocol compatibility | CONFIRMED (reproduced) — **scope corrected**: the synchronous BZMPOP completion maps the exception to `WRONG_TYPE`, but the **asynchronous** BZMPOP path (`BGroup.java:1736-1739`) still leaks it (see O1) | fixed (`1f380ffd`) |
| 90.5 | `INFO` does not count the blocked sorted-set clients | Low | Observability | CONFIRMED (inspection) | fixed (`dda64496`) |
| 90.6 | `REDIS_COMMANDS_NOT_IMPLEMENTED.md` lists commands that are implemented | Low | Documentation | PARTIALLY CONFIRMED — **refined**: AUTH/PING/QUIT/CLIENT/HELLO are wrongly listed; my SELECT/OBJECT examples were wrong (Velo answers `NOT_SUPPORT` for SELECT, and the document's Object section lists subcommands, not an OBJECT container) | fixed (`f54c0703`) |
| 90.7 | The timeout reply is a null bulk, Redis answers a null array | Low | Redis protocol compatibility | CONFIRMED (inspection) | fixed (`7d4bd1b7`) — the list blocking pops too |
| 90.8 | The "NOT_SUPPORT by design" decision records were not updated | Low | Process | PARTIALLY CONFIRMED — **refined**: `STATE.md` was updated when the report was filed; the bug_43 / bug_87 notes still need the supersession | fixed (`94ed9044`) |
| 90.9 | The BZMPOP reply is flat, Redis emits one array per member | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`42110363`) |
| 90.10 | The non-blocking `ZMPOP` emits flat member/score entries (the sibling half of 90.9) | Medium | Redis protocol compatibility | CONFIRMED (inspection) — predates the reviewed commit, missed by both review passes | fixed (`2490462d`) |
| follow-up | The list blocking pops carry the same waiter leak and the same registration counter (O6) | Low | Correctness / Observability | CONFIRMED (reproduced) | fixed (`a17223f4`) |
| C1 | A blocking client registered twice on the same key corrupted the delivery iteration and the key counts | High | Concurrency / Correctness | CONFIRMED (reproduced) — introduced by `a17223f4`, accepted by agent 1 | fixed (`de9fe620`) |
| C2 | The list recheck delivery released only the serving key | Medium | Correctness | CONFIRMED (reproduced), accepted by agent 1 | fixed (`c05fa127`) |
| C3 | The zset wrong-type completion released only the serving key | Medium | Correctness | CONFIRMED (reproduced), accepted by agent 1 | fixed (`45a91226`) |
| C4 | The gap document still listed supported CLIENT/ACL subcommands as missing | Low | Documentation | CONFIRMED (inspection) — accepted by agent 1; the second re-review added the stale `CLIENT REPLY` claims and the unreconciled aggregate counts, both fixed | fixed (`3b37482a` + `930b12b1`) |
| C5 | The committed destination-read assertion could not establish existence | Medium | Test evidence | CONFIRMED (inspection), accepted by agent 1 — the defect was in agent 1's own test | fixed (`4ed30995`) |

## Independent verification — AI agent 2, 2026-11-19

Reviewed the current workspace at `769a9eab`, including the follow-up numeric-error fixes, against
the local Redis source in `/home/kerry/ws/redis` (`src/version.h`: **7.2.14**). Earlier sections are
preserved as review history. This section supersedes their blanket “8 confirmed” conclusion.
Only this document is changed in the repository; no production fix or commit is part of this review.

### Verdicts

| Finding | Independent verdict | Reason / current-code evidence |
|---|---|---|
| 90.1 | **Agree — reproduced**, refine scope | `BGroup.java:1893-1899` registers on every key; `1975-1979` removes only from the serving key. A temporary probe executed `BZPOPMIN review-a review-b 0`, then saved one member to `review-a`: the promise completed, but client/key counts remained `1/1`. This occurs with **one slot and one worker**, contrary to the residual-risk claim that two workers are required. Redis `blocked.c:423-436` explicitly unblocks from every registered key. |
| 90.2 | **Agree with the routing defect; reject the stated successful-write/lost-notification explanation in production** | The store destination is omitted in `ZGroup.java:353-390`; `MultiWorkerServer.java:577-593` checks only parsed slots; the non-cross branch saves directly at `ZGroup.java:1408-1410`. However, the real path is `saveRedisZSet` → `BaseCommand.set` → `putToOneSlot` (`BaseCommand.java:1279-1287`) → `OneSlot.put` (`OneSlot.java:2081-2082`) → `checkCurrentThreadId` (`1601-1604`). A foreign worker **throws before writing the value and before `onZSetSaved()`**. The temporary real-persistence save probe reproduced `IllegalStateException: Thread id not match`. The same guard exists at reviewed commit `33458c51`. Thus the STORE command fails and the blocked client remains waiting because no destination value was written; this is not a demonstrated lost wake-up after a successful store. Mock `InMemoryGetSet` bypasses that guard, so a mock-only experiment cannot establish the report's production explanation. Dispatching the destination write is still the right direction, and write failures/completion need to reach the STORE caller. |
| 90.3 | **Agree historically; cited cases already fixed** | Current `BGroup.java:1545-1557`, `1657-1675`, `1695-1700` use the Redis messages named in the finding. The existing `test bzpopmin` and `test bzmpop` passed. Redis source evidence is `timeout.c:165-201` plus `t_zset.c:4400-4448`; the timeout helper is in **timeout.c**, not t_zset.c. This confirms the listed examples, not every numeric parsing/range/precedence edge case. |
| 90.4 | **Agree with a narrower BZMPOP trigger** | BZPOPMIN/MAX always return the multi-key `AsyncReply` and retain the type exception (`BGroup.java:1587-1592`); `MultiWorkerServer.java:694-698` exposes its message. For BZMPOP, `BGroup.java:1767-1771` maps an already-completed `TypeMismatchException` to `WRONG_TYPE`. `Utils.wrapException` (`124-129`) preserves the exception type. The probe confirmed that a wrong-type second key on the **same worker** leaks an exception for BZPOPMIN but returns `WRONG_TYPE` for BZMPOP. BZMPOP's leak requires the check to complete asynchronously, e.g. a second key on another worker. That cross-worker case was inspected, not reproduced live. |
| 90.5 | **Agree — inspection** | `InfoCommand.groovy:185-186` counts only `BlockingList`; `OneSlot.java:2975-2976` also includes `BlockingZSet`. Both INFO fields omit zset waiters. Simply summing counters still leaves the registration-vs-client overcount described below. |
| 90.6 | **Partly agree; reject SELECT/OBJECT examples and the registry-only fix criterion** | AUTH/PING/QUIT have real handlers (`RequestHandler.java:453`, `MultiWorkerServer.java:428-430,531-533`); `CGroup.java:116-117,153-163` implements CLIENT/CLIENT GETNAME. Their “missing” labels are inaccurate. But `SGroup.java:424-425` explicitly returns `NOT_SUPPORT` for SELECT. `DGroup.java:115-116` handles **DEBUG OBJECT**, not **OBJECT**; `OGroup.java:49-50` returns unknown command. Also, the current gap document's Object section lists subcommands, not an OBJECT container entry as quoted in the report. GETNAME is a CLIENT subcommand, not a standalone Redis command. A registry/dispatch entry proves routing, not implementation or complete subcommand support; the document should distinguish supported commands, partial containers, and explicit unsupported handlers. |
| 90.7 | **Agree — encoder and timeout paths verified** | The timeout paths return `NilReply.INSTANCE`; the existing timeout specs passed and the probe verified its RESP2 bytes are `$-1\r\n`. Redis `blocked.c:229-234` calls `addReplyNullArray` for BLOCKED_ZSET. This is a RESP2 null-bulk/null-array mismatch; RESP3 uses a common null representation. No live wire capture was run here. |
| 90.8 | **Partly agree; STATE.md claim is stale** | The old bug_43 implementation note and bug_87 scope/non-finding still describe intentional `NOT_SUPPORT`. Appending a supersession note there is reasonable. However, current `STATE.md:32` already says the decision was reversed by `33458c51`, and `STATE.md:35,63` tracks bug_90. Its bug_87 row is therefore **not** currently stale in the way asserted. The bug_90 row still repeats the inaccurate unconditional confirmations and should ultimately reflect this verification. Existing workspace edits to STATE.md were left untouched. |

### Rejected non-finding: BZMPOP reply shape is not Redis-compatible (additional Bug 90.9)

**Severity: Medium — Redis protocol compatibility. Confirmed by executed probe and local Redis source.**

The earlier “BZMPOP reply shape matches Redis 7.2” assertion is false.
`BGroup.java:1874-1884` creates a flat `Reply[min * 2]` and places bulk member/score entries into it:

```java
var valueReplies = new Reply[min * 2];
valueReplies[j * 2] = new BulkReply(sv.member());
valueReplies[j * 2 + 1] = new BulkReply(sv.score());
var arr = new Reply[]{new BulkReply(keyBytes), new MultiBulkReply(valueReplies)};
```

For `BZMPOP 0 1 review-z MIN COUNT 2` with members `a:1`, `b:2`, the actual reply is
`["review-z", ["a", "1", "b", "2"]]`. Redis requires
`["review-z", [["a", "1"], ["b", "2"]]]`, including a pair array even for COUNT 1.
Redis `t_zset.c:4445-4446` passes `use_nested_array=1`; `genericZpopCommand`
(`3946-3950`, `4001-4005`) emits both the members array and one two-element array per pair.
The probe confirmed four `BulkReply` children instead of two `MultiBulkReply` pairs.
The existing BGroup `test bzmpop` asserts the flat shape (`BGroupTest.groovy:2212-2237`), so
its green result does not prove compatibility. Both immediate pops and waiter wake-ups share
`popZSetValuesReply` and inherit the same defect. A future fix needs Redis-shaped expectations
for both paths before changing the production helper.

### Further corrections to the original analysis

- **Counters count registrations, not unique clients.** In the 90.1 probe, one still-pending
  client registered on two keys already yielded `blockingClientCount() == 2` before delivery
  (`BlockingZSet.java:202-208`). Cleaning stale registrations alone will not make this a correct
  client count. INFO/Prometheus fixes should account for waiter identity across keys/workers.
- **Empty key-list objects remain in the map.** `BlockingZSet.Inner.removeOne`
  (`211-223`) decrements counts but never removes an empty `blockingZSetPromisesByKey` entry.
  This is separate from retaining a completed waiter. Even single-key delivery/timeout can leave
  empty map entries until socket cleanup or another explicit map clear. The 90.1 fix should consider
  both kinds of retention.
- **Do not describe the current foreign-worker STORE as an actual unsynchronized write.** The
  ownership guard rejects it. The defect is attempting the save on the wrong worker; see 90.2 above.
- The tested 90.1 and reply-shape scenarios do not require a multi-worker server. The original
  residual-risk statement conflates multi-key coverage with multi-worker coverage.

### Executed verification and limits

The first focused Gradle run failed during `compileTestGroovy` with `Java heap space`, before tests
ran. A temporary init script increased only the Groovy compiler fork heap to 8G and added review
probes from `/tmp`; no repository build configuration or source tests were edited. The first probe
run had a probe-only Groovy accessor mistake (`exception` resolved to `isException()`); it was
corrected to `getException()` and the complete focused selection was rerun successfully:

```bash
./gradlew -I /tmp/bug90-probes.init.gradle :test \
  --tests 'io.velo.command.BGroupTest.test bzpop*' \
  --tests 'io.velo.command.BGroupTest.test bzmpop' \
  --tests 'io.velo.command.BlockingZSetTest' \
  --tests 'io.velo.command.Bug90ReviewProbe'
```

**Result: BUILD SUCCESSFUL; 9 tests, 0 failures** (3 existing BGroup command specs,
3 existing BlockingZSet specs, 3 temporary observation probes). These probes assert observed
defective behavior to establish the review evidence; they are not regression tests for a fix.
Log: `/tmp/bug90-review-probes.log`; probes:
`/tmp/bug90-probes/io/velo/command/Bug90ReviewProbe.groovy`.

After the successful run, inspected the generated JaCoCo HTML source pages and checked the
specific paths using `scripts/jacoco_cover.py`:

- `BGroup.java:1874-1884`: **9/9 executable lines covered**, confirming the flat reply helper ran.
- `BGroup.java:1975-1981`: **5/5 executable lines covered**, confirming serving-key-only cleanup ran.
- `OneSlot.java:1601-1605`: **3/3 executable lines and both guard branches covered**, confirming the
  real persistence rejection path ran (`OneSlot.java.html#L1604` marked `fc`).

No production class was changed, so no changed-class coverage threshold applies. No live Velo/Redis
differential, full-suite run, or two-worker STORE end-to-end reproduction is claimed. The production
STORE trigger is supported by the inspected routing/save chain plus the executed real-save guard
probe; a future fix still needs an end-to-end two-worker test that checks both STORE completion and
the blocked client's reply.

## Disputed and unsettled items — reconciliation table (AI agent 1 + AI agent 2, 2026-11-19)

This section records every point where the first review pass and the independent verification
disagreed, and every point that **neither** pass settled. It is the hand-off list for the fix agent
and for the next reviewer: the `D` rows are disputes (already closed or explicitly re-opened), the
`O` rows are the open questions. Nothing here changes the per-finding verdicts above except where a
dispute row says so.

### A. Points where the two reviews disagreed

| # | Point | Review 1 (first pass) | Independent verification | Why they differ (one line) | Status |
|---|---|---|---|---|---|
| D1 | 90.2 mechanism | cross-thread write + lost wake-up after a successful store | the ownership guard throws, so the store fails and nothing is written | review 1 stopped at `onZSetSaved`'s thread lookup and never traced `OneSlot.put` → `checkCurrentThreadId` | **closed for the verification** (`OneSlot.java:1601-1606` throws `IllegalStateException: Thread id not match`); review 1 accepted |
| D2 | 90.2 impact wording | the client hangs *because* the wake-up was lost | the client hangs *because* no value was ever written | the missing save→write link leads to a different causal chain | closed with D1 |
| D3 | 90.4 scope | BZMPOP leaks the type message as well | BZMPOP already maps it to `WRONG_TYPE` (`BGroup.java:1768-1773`); only the cross-worker path can leak | review 1 read the inline branch, not the async-completion branch | closed for the verification; 90.4 narrowed |
| D4 | 90.6 examples | `SELECT` and `OBJECT` are wrongly listed as missing | `SELECT` is explicitly `NOT_SUPPORT` (`SGroup.java:424-425`); the document's Object section lists subcommands, not an `OBJECT` container entry | the quoted `OBJECT` line does not exist in the document | closed for the verification; review 1 withdrew the examples |
| D5 | 90.8 staleness | `STATE.md` still carries the stale `NOT_SUPPORT` claim | the inspected workspace already records the reversal (`STATE.md:32,35,63`) | the current-state correction is verified; the timing and alleged read/write race are not established | closed as a current-state correction; 90.8 partly confirmed (bug_43/bug_87 notes still stale) |
| D6 | BZMPOP reply shape | "the reply shape matches Redis 7.2" | flat, Redis nests one array per member → **new finding 90.9** | review 1 compared the outer shape only and never checked Redis's per-pair `use_nested_array` nesting | closed for the verification; the non-finding was **rejected** and became 90.9 |
| D7 | 90.1 trigger | needs a two-worker server to reproduce | reproduces on a **single** slot and a single worker | review 1's residual-risk claim ignored the registration/removal asymmetry between the keys | closed for the verification; review 1 accepted |
| D8 | 90.3 Redis citation | the timeout texts come from `src/t_zset.c` | the timeout helper lives in `src/timeout.c` (`timeout.c:172,188`), the numkeys/count texts in `src/t_zset.c:4408,4434` | review 1 cited the wrong file | closed; the citation in the report was corrected |
| D9 | 90.5 fix design | sum the two counters in `INFO` | summing is not enough — the counters count **registrations**, so one client blocked on two keys counts twice | review 1 treated the counters as client counts | **CONFIRMED correctness requirement**: count each blocked client once; implementation design remains to be chosen (see O5) |

### B. Points neither pass settled

| # | Open item | Why it is open | What would settle it |
|---|---|---|---|
| O1 | The cross-worker BZMPOP wrong-type path | the verification inspected it but did not reproduce it live | a two-worker probe with a wrong-type second key |
| O2 | The two-worker `*STORE` end-to-end | the production claim rests on the inspected routing/save chain plus a real-persistence guard probe, not on a full end-to-end run | a live `slotWorkers=2` run checking both the store's reply and the blocked client's reply |
| O3 | 90.7 live-wire verification | the null-bulk/null-array defect is established by the executed encoder probe and Redis source; only the live capture remains missing | one live capture of a timed-out `BZPOPMIN`; this is additional verification, not an unresolved defect verdict |
| O4 | 90.3 completeness | the listed examples are verified; other numeric/range/precedence cases were not comprehensively compared | a targeted Redis differential covering parsing, finite/overflow boundaries, fractional timeouts, and competing invalid arguments; do not claim exhaustive compatibility from the listed examples |
| O5 | 90.5 / D9 counter implementation | the current counters count registrations; Redis `blocked_clients` counts clients once (`blocked.c:96,217`, `server.c:5581-5590`) | choose a thread-safe implementation and test one pending client across two keys/workers reports `blocked_clients == 1`, then zero after delivery/timeout/disconnect; no product decision is needed to establish the expected meaning |
| O6 | 90.1 fix scope | the same waiter leak exists in `BlockingList` (bug_72 accepted code) — fix both or consciously accept the divergence | a product decision |
| O7 | 90.7 fix vs accept | the null-array divergence is systemic (BLPOP/BRPOP/BLMPOP too) | a product decision |
| O8 | 90.2 async error propagation | the existing contract already converts command failures to error replies (`RequestHandler.java:576-579`, `MultiWorkerServer.java:694-698`); dispatching a destination save must preserve that contract | complete the STORE promise only after the destination write succeeds or fails, and test an injected destination failure reaches the caller as an error; not a product-decision blocker |
| O9 | mock vs real | `InMemoryGetSet` bypasses the ownership guard, so it cannot prove real persistence/thread-ownership behavior | use real persistence for 90.2's save/ownership test and two-worker integration for worker-sensitive paths; focused mocks remain valid for reply structure, counter logic and other independent behavior; documentation fixes need no storage test |
| O10 | 90.9 sibling half — the non-blocking `ZMPOP` | Redis nests the reply for `ZMPOP` too (`src/t_zset.c:4445-4448`), and Velo's `ZGroup.zmpop()` is flat in both the same-worker and the cross-slot paths | **CONFIRMED (inspection), assigned 90.10 below**; add RED expectations for both paths and fix separately under the one-bug-per-commit workflow |

### Hand-off notes for the fix agent

- The recommended fix order is **90.9 → 90.10 → 90.2 → 90.1 → 90.4 → 90.3 (cited cases done) → 90.5 → 90.6 → 90.8 → 90.7**. Keep each identified bug's test intent and commit separate per `AGENTS.md`; batching has not been authorized.
- Apply O9 according to the behavior being verified. Real persistence is essential for the ownership claim, not a blanket prerequisite for every fix.
- D9/O5 and O8 are correctness and implementation requirements, not unanswered product questions. O6/O7 concern the scope of repairing inherited list defects or accepting compatibility deviations; they do not block unrelated confirmed fixes. An acceptance/deferment decision has not been made here.

### AI agent 2 confirmation of the reconciliation table

Rechecked on 2026-11-19 at unchanged HEAD `769a9eab`. **Confirmed with the corrections now recorded
in D5/D9 and O3/O4/O5/O8/O9/O10 above.** D1/D2/D4/D6/D7 are consistent with the earlier
executed probes and current code. D8's file-location correction is correct, although the original
reference list near the top of this document still attributes the timeout helper to `t_zset.c`;
the authoritative location is `timeout.c:165-201`.

**D3 needs to retain the asynchronous BZMPOP case.** The synchronous completion check at
`BGroup.java:1767-1771` maps a preserved `TypeMismatchException` to `WRONG_TYPE`. An asynchronous
failure goes through `1736-1739` and returns an `AsyncReply`, whose exception is converted using its
message by `MultiWorkerServer.java:694-698`. Thus O1 remains an integration-test gap, not a reason
to exclude BZMPOP from 90.4. The earlier Summary wording that narrowed 90.4 to BZPOPMIN/BZPOPMAX
alone is incomplete and is superseded by this clarification.

O1/O2 remain unexecuted two-worker integration scenarios. O3 is a live-wire verification gap for an
already established defect. O4 remains a compatibility-coverage gap. O6/O7 remain scope/disposition
questions about inherited list behavior. None of these entries establishes that a fix has landed.

**90.10 — ZMPOP emits flat member/score entries (Medium, Redis protocol compatibility).**
Independently confirmed by inspection of both `ZGroup.zmpop()` paths:

```java
// ZGroup.java:2760-2772 (same-worker); repeated at 2838-2849 (cross-worker)
var valueReplies = new Reply[min * 2];
// inside the pop loop:
valueReplies[j * 2] = new BulkReply(sv.member());
valueReplies[j * 2 + 1] = new BulkReply(sv.score());
// the key and the flat array are returned without per-pair nesting
var arr = new Reply[]{keyReply, new MultiBulkReply(valueReplies)};
```

For `ZMPOP 1 k MIN COUNT 2` with `a:1,b:2`, the code builds
`["k", ["a", "1", "b", "2"]]` rather than
`["k", [["a", "1"], ["b", "2"]]]`. Redis `t_zset.c:4447-4448` invokes
`genericZpopCommand` with `use_nested_array=1`; `3946-3950,4001-4005` emits the nested shape.
COUNT 1 also requires a pair array. This finding is confirmed independently of 90.9 but has not
been reproduced in a new live run. Future verification must cover both Velo paths and assert the
pair nesting rather than copy the existing flat expectations.

This reconciliation pass changed documentation only. No new tests, live server runs, production
fixes or commits were performed; the executed-probe evidence remains the earlier verification log.

## Review Feedback — committed fixes (AI agent 2, 2026-11-19)

**Reviewed range:** `769a9eab..b85565eb`. **Overall verdict: CHANGES REQUESTED.**
The successful-pair reply formatting and STORE dispatch changes address their original defects,
and the error mapping works on real foreign-worker reads. The cleanup work is incomplete and the
list follow-up introduces a new regression with duplicate key arguments. No production code was
changed by this review; the probes described below live under `/tmp`.

### Per-commit review outcomes

The acceptance labels in this table describe commit review outcomes. Formal finding verdicts and
dispositions using the skill's vocabulary are recorded in the compliance table appended below.

| Finding / commit | Verdict | Independent evidence / remaining work |
|---|---|---|
| 90.9 — `42110363` | ACCEPTED | BZMPOP immediate and wake-up specs assert nested pairs; helper `BGroup.java:1888-1903` has 8/8 executable lines covered. |
| 90.10 — `2490462d` | ACCEPTED | Both ZMPOP branches call the nested-pair helper; `test zmpop` passed; helper `ZGroup.java:2730-2753` has 8/8 executable lines covered. |
| 90.2 — `ccba406e` | Core fix ACCEPTED; test follow-up C5 required | A real two-worker probe, without `InMemoryGetSet`, verified destination save, BZPOPMIN wake-up, two remaining members read on the owner, and propagation of a readonly destination error. `saveRedisZSetToDst` success/error branches executed. |
| 90.1 — `99c842f1` | PARTIALLY ACCEPTED | Successful delivery now releases all registrations, and empty entries are removed. Wrong-type completion still retains the waiter on other keys: C3. |
| 90.4 — `1f380ffd` | ACCEPTED | Real two-worker checks of BZPOPMIN, BZPOPMAX and BZMPOP with a wrong-type second key each returned `ErrorReply.WRONG_TYPE`, including asynchronous BZMPOP completion. |
| 90.5 — `dda64496` | ACCEPTED for INFO wiring and unique-waiter semantics | Existing INFO/registry specs passed; one waiter counts once across keys. Retained completed waiters from C2/C3 still affect the resulting metrics until cleanup is completed. |
| 90.6 — `f54c0703` | PARTIALLY ACCEPTED | Several top-level false positives removed, but supported subcommands remain in the missing list: C4. |
| 90.8 — `94ed9044` | ACCEPTED | bug_43/bug_87 now explicitly supersede the old NOT_SUPPORT decision. |
| 90.7 — `7d4bd1b7` | ACCEPTED for scoped timeout paths | Focused blocking-pop timeout specs and `NullArrayReplyTest` passed. New reply's seven executable lines covered; RESP2 `*-1\r\n` and RESP3 `_\r\n` tested. No TCP frame capture was performed. |
| List follow-up — `a17223f4` | CHANGES REQUESTED | New duplicate-key regression C1 and unrepaired recheck delivery C2. |
| 90.3 — `cf50f306` (earlier commit) | Listed cases verified only | Existing BZPOP/BZMPOP error-text assertions passed; numeric boundary/precedence matrix remains outside this acceptance. |
| `b85565eb` | ACCEPTED | Explicit `getException()` removes the known boolean-property ambiguity; no production behavior changed. |

### C1 — High: duplicate list keys invalidate delivery iteration and corrupt key counts

**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`de9fe620`), verified by the formerly failing duplicate-count and second-client iteration probes, now green.

**Introduced by `a17223f4`.** `BlockingList.java:479-484` (and `533-538`) calls
`removeEverywhere(promise)` while `setReplyIfBlockingListExist` is iterating the serving key's
`LinkedList`. `removeEverywhere` can run inline on this worker (`629-645`) and call
`Inner.removeOne` on that **same list**. A valid command such as `BLPOP k k 0` registers the
same waiter twice on `k`; the second registration is therefore removed outside the active iterator.

Reproduced with the actual registry/delivery helper in two temporary cases:

- One waiter registered twice on `k`, then one pushed value: client completes, but
  `blockingKeyCount()` becomes **-1**. `Inner.removeOne` decrements/removes the key on the second
  registration, then the enclosing delivery loop decrements it again at `BlockingList.java:544`.
- The same duplicate-key waiter followed by another client on `k`, then two pushed values:
  **`ConcurrentModificationException`** at `BlockingList.java:417` (`LinkedList.ListItr.next`).
  The second client is not served by this delivery; a valid push can fail after the first client
  has already received a value.

Redis accepts repeated keys and deduplicates registrations (`redis/src/blocked.c:379-383`).
Deduplicate waiter registrations or ensure global cleanup cannot mutate the serving list during
iteration; account for key removal exactly once. Cover both left/right delivery and BLMPOP as
appropriate. This is a required regression fix, not an optional metrics cleanup.

### C2 — Medium: list recheck delivery still cleans only the serving key

**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`c05fa127`), verified by the formerly failing recheck probe and coverage of both cleanup helpers.

**Incomplete list half of 90.1 (`a17223f4`).** Normal push notification now performs global
cleanup, but the separate check/register-window completion helpers still use only
`BlockingList.removeOne`: `BGroup.java:1263-1278` (`completeIfListNotEmpty`) and `1521-1523`
(`completeMpopIfListNotEmpty`). They never call `removeEverywhere`.

The executed probe registered one waiter on two keys, put a list value into the first key, and
called `completeIfListNotEmpty`. The promise completed, but `blockingClientCount()` remained **1**
instead of 0. This models a value already present when the post-registration recheck runs. With
timeout 0 and no subsequent write/disconnect, the completed waiter remains retained. The BLMPOP
helper has the same missing cleanup by inspection; it was not a separate executed probe.

All terminal list-delivery paths must release all registrations. Keep a regression for the recheck
path rather than relying only on the new push-notification test.

### C3 — Medium: zset wrong-type completion retains registrations on other keys

**Status:** `CONFIRMED (reproduced)`
**Disposition:** `fixed` (`45a91226`), verified by the formerly failing wrong-type cleanup probe and executed terminal cleanup lines.

**Incomplete 90.1 fix (`99c842f1`).** The successful completion now calls
`BlockingZSet.removeEverywhere` at `BGroup.java:1979`, but the terminal wrong-type branch at
`1957-1964` removes only the current key and completes the promise with `WRONG_TYPE`.

The probe registered the same waiter on two keys and rechecked a first key containing a hash.
`getResult()` was `ErrorReply.WRONG_TYPE`, but `blockingClientCount()` and the remaining key
registration stayed **1**. A type change during the cross-worker check/register/recheck interval
can reach this branch. Completed promises are skipped by the other rechecks, and the timeout
cannot clean it after the delivery arbiter is claimed. Release every registration on this terminal
error path as well as on successful delivery.

### C4 — Low: the corrected gap document still labels supported subcommands missing

**Status:** `PARTIALLY CONFIRMED` — `3b37482a` corrects the detailed lists, but the remaining CLIENT REPLY claims and inconsistent totals prevent closing this item; see the latest re-review.

**90.6 remains partially open.** `REDIS_COMMANDS_NOT_IMPLEMENTED.md:49-58` still lists ID, KILL,
LIST, REPLY, SETINFO and SETNAME as missing, even though `CGroup.client()` implements them
(`CGroup.java:163,183,187,212,235,256`). The document's own note already acknowledges several of
these as supported. The Major Gaps section also explicitly labels REPLY missing (`311`).
ACL DELUSER/DRYRUN are further false positives (`AGroup.java:176,199`).

The new definition “registered means implemented” (`REDIS_COMMANDS_NOT_IMPLEMENTED.md:5-10`)
does not establish subcommand support, as discussed during reconciliation. Audit container plus
subcommand identities, remove these remaining false positives, and update the derived counts
instead of treating a registry-only script as complete verification.

### C5 — Medium: the committed destination-read assertion cannot establish existence

**Status:** `CONFIRMED (inspection)`
**Disposition:** `fixed` (`4ed30995`), verified by the bounded future read and passing present/missing-key assertions in `test zunionstore`.

**Test follow-up for `ccba406e`.** `ZGroupTest.groovy:2057-2069` schedules an owner-worker read,
then immediately returns `promise.getResult()` without awaiting completion. The helper returns
primitive `boolean`, while its caller asserts `stored != null` (`2014-2016`). Whether the helper
returns true or false, that assertion succeeds; delayed completion/failure is not reliably observed.

Use a bounded thread-safe completion mechanism, assert the actual boolean or decoded contents,
and verify that a missing destination makes the test fail. The new helper also uses `var` and a
fully qualified `SettablePromise` expression contrary to `GroovyStyleRefer.groovy`; use `def` and
an import when repairing it. The independent real-persistence probe used `CompletableFuture.get`
with a five-second bound and verified **two remaining members** after the blocked pop.

### Verification evidence and limits

- First focused regression run passed (`/tmp/bug90-fix-review-regression.log`). The expanded
  focused selection executed **168 existing tests with zero failures** across BGroup, BlockingList,
  BlockingZSet, InfoCommand, LGroup, RGroup, ZGroup, ZGroupPart2 and NullArrayReply. Its log is
  `/tmp/bug90-fix-review-final.log`; that combined invocation was red because it also selected the
  new review probes.
- The final isolated probe rerun (`/tmp/bug90-fix-review-probes-final.log`) executed **five tests**:
  **one passed**, covering real two-worker STORE/wake-up, owner read, write-error propagation and
  async WRONGTYPE; **four failed for the expected production defects** (C1 twice, C2 and C3).
  These failures are intentional review evidence, not fixed regressions.
- Temporary sources: `/tmp/bug90-fix-review-probes/io/velo/command/Bug90FixReviewProbe.groovy`
  and `Bug90StoreReviewProbe.groovy`; Gradle init: `/tmp/bug90-fix-review.init.gradle`. It adds that
  source directory and raises only the Groovy compiler fork heap to 8G. Initial probe compilation
  and accessor mistakes were corrected before drawing conclusions; a shared persisted readonly
  flag in the first expanded probe run was addressed by explicit writable setup and fresh keys.
- Inspected JaCoCo HTML source pages and `scripts/jacoco_cover.py` results for the touched paths:
  `ZGroup.java:1263-1274` (STORE helper, success and error), BGroup nested-pair helper (8/8),
  ZGroup nested-pair helper (8/8), BlockingList registry/counter methods (25/25 in 287-358),
  BlockingZSet registry/counter methods (23/23 executed in 248-314, one partial branch), and
  NullArrayReply (7/7). The failing probes also executed the wrong-type cleanup at
  `BGroup.java:1962`, list recheck cleanup at `1263`, and duplicate cleanup/count decrement at
  `BlockingList.java:484,544`; the HTML marks these lines executed. This coverage is evidence of
  execution, not proof that these defective paths are correct.
- No full-suite run, live TCP Redis differential or independent per-commit historical RED replay
  was performed. No production/test source fixes or commits were made during this review.

**Pre-acceptance follow-ups:** repair C1-C5 with focused regression coverage and the required
one-bug-per-commit workflow. **Post-acceptance follow-ups:** a live RESP2 timeout capture and the
targeted numeric argument matrix remain additional compatibility evidence; they must not be
silently described as completed by the green unit tests.

## Re-review — velo-bug-review compliance (AI agent 2)

**Skill read:** `doc/skills/velo-bug-review/SKILL.md`, Stage 5 (re-review), with the Stage 2
feedback/status requirements applied to the follow-up findings. **Reviewed commit:**
`b85565eb29d02d9ca7c7a5f60369633509f988c0`. This supplements the detailed Review Feedback above;
it does not start a new review round or replace its evidence.

### Formal per-finding verdicts

An em dash in Disposition means no closing disposition has been assigned. The original finding
Status fields above have been updated in place. Verdicts describe the findings; `fixed` describes
the reviewed disposition, rather than introducing an unsupported ACCEPTED status.

| Finding | Verdict | Disposition | Evidence |
|---|---|---|---|
| 90.1 | CONFIRMED (reproduced) | — | C3's final probe still reports `BlockingZSet.blockingClientCount() == 0` with actual value `1`; `99c842f1` repairs successful delivery only. |
| 90.2 | CONFIRMED (reproduced) | fixed | `Bug90StoreReviewProbe: tests=1, failures=0, errors=0`; real owner-worker save, blocked-client delivery and readonly failure propagation verified. C5 is a separate test defect. |
| 90.3 | CONFIRMED (inspection) | fixed | Existing BZPOP/BZMPOP numeric-error assertions are among the 168 passing existing tests; disposition covers the cited examples only. |
| 90.4 | CONFIRMED (reproduced) | fixed | The passing real two-worker probe also asserts `WRONG_TYPE` for asynchronous BZPOPMIN, BZPOPMAX and BZMPOP. |
| 90.5 | CONFIRMED (reproduced) | fixed | INFO/registry specs passed; unique-waiter methods covered. C2/C3 still retain completed waiters and must be repaired separately. |
| 90.6 | PARTIALLY CONFIRMED | — | `REDIS_COMMANDS_NOT_IMPLEMENTED.md:49-58` still lists supported CLIENT subcommands; `CGroup.java:163,183,187,212,235,256` contains the handlers. |
| 90.7 | CONFIRMED (inspection) | fixed | Focused blocking timeout specs passed; `NullArrayReply` JaCoCo shows 7/7 lines executed for RESP2/RESP3/HTTP encoders. |
| 90.8 | REFINED | fixed | `94ed9044` adds explicit SUPERSEDED notes to bug_43/bug_87; STATE.md's earlier correction is preserved. |
| 90.9 | CONFIRMED (reproduced) | fixed | Existing nested-pair assertions passed; BGroup reply helper has 8/8 executable lines covered. |
| 90.10 | CONFIRMED (inspection) | fixed | Both ZMPOP paths call the nested helper, whose eight executable lines were covered; existing `test zmpop` passed. |
| C1 | CONFIRMED (reproduced) | — | Two failing probes: key count `-1`, and `ConcurrentModificationException`; introduced by `a17223f4`. |
| C2 | CONFIRMED (reproduced) | — | List recheck completes the promise but the blocked-client count remains `1`. |
| C3 | CONFIRMED (reproduced) | — | Wrong-type zset completion returns WRONGTYPE but the blocked-client count remains `1`. |
| C4 | CONFIRMED (inspection) | — | Missing-command entries conflict with the cited CLIENT/ACL implementations. |
| C5 | CONFIRMED (inspection) | — | `ZGroupTest.groovy:2057-2069` returns an unawaited promise result as boolean; `2014-2016` checks that boolean with `!= null`. |

### Pasted verification evidence

From `/tmp/bug90-fix-review-regression.log`:

```text
BUILD SUCCESSFUL in 32s
```

From the final probe XML, extracted without substituting Groovy property access for getters:

```text
io.velo.command.Bug90FixReviewProbe: tests=4, failures=4, errors=0
wrongtype zset recheck releases every registration
Condition not satisfied:

BlockingZSet.blockingClientCount() == 0
|            |                     |
|            1                     false
class io.velo.command.BlockingZSet
list recheck delivery releases every registration
Condition not satisfied:

BlockingList.blockingClientCount() == 0
|            |                     |
|            1                     false
class io.velo.command.BlockingList
duplicate list keys do not make key count negative
Condition not satisfied:

BlockingList.blockingKeyCount() == 0
|            |                  |
|            -1                 false
class io.velo.command.BlockingList
duplicate list keys with another waiting client do not invalidate iterator
Expected no exception to be thrown, but got 'java.util.ConcurrentModificationException'
io.velo.command.Bug90StoreReviewProbe: tests=1, failures=0, errors=0
```

From `/tmp/bug90-fix-review-probes-final.log`:

```text
5 tests completed, 4 failed
> Task :test FAILED
BUILD FAILED in 9s
```

These red results are the remaining defects, not an assertion that the committed regression suite
failed: the expanded run's XML showed 168 existing tests with zero failures. C4/C5 are inspection
verdicts with the code excerpts and cited implementations above, not claimed runtime reproductions.

### Four-evidence audit per fix commit

The author Fix Log's RED evidence, commit SHA, focused GREEN selection and JaCoCo ranges were
read and checked against each commit's diff, tests and the independent focused/probe results above.
`42110363`, `2490462d`, `ccba406e`, `1f380ffd`, `dda64496` and `7d4bd1b7` have all four evidence
fields recorded; independent current-code checks support the scoped dispositions in the formal
table. `99c842f1` and `a17223f4` also have those fields, but the new RED probes prevent closing
their full cleanup scope. Documentation commits `f54c0703`/`94ed9044` legitimately have no
production RED/GREEN/JaCoCo requirement; inspection prevents closing `f54c0703`.

The earlier numeric-error commit `cf50f306` has the author's original RED audit marked not
applicable in this report; this reviewer verified the current listed-case tests, not an independent
historical RED replay. Do not infer stronger historical evidence from the current green result.
No independent historical checkout/replay was performed for any commit in this pass.

### Recommended fix order

1. **C1** — repair the introduced duplicate-key iterator/count regression.
2. **C2** — release every registration in BLPOP/BRPOP and BLMPOP recheck completion paths.
3. **C3** — release every registration in terminal zset WRONGTYPE completion.
4. **C5** — make the committed destination-read test await and assert real contents/existence.
5. **C4** — finish the subcommand-aware missing-command document correction.

Use the existing RED probes to construct permanent regressions first. Keep one bug per commit,
rerun the focused tests, and verify the changed lines/branches in JaCoCo before asking agent 2
to re-review. Review feedback remains in this same document.

## Residual Risk

- C1-C5 remain unresolved; no final fixed disposition is assigned to these follow-ups.
- The real two-worker integration evidence covers the original destination-owner STORE case and
  asynchronous wrong-type replies. It is not a live TCP comparison with Redis.
- Timeout wire capture and broader numeric parsing/range/precedence comparisons remain additional
  verification. The accepted dispositions above cover their stated scope only.
- No full-suite run or independent per-commit historical RED replay was performed. Current focused
  execution and JaCoCo evidence must not be described as either.

## Fix response to the re-review findings (AI agent 1, 2026-11-19)

**Reviewed range of the re-review:** `769a9eab..b85565eb` — overall verdict **CHANGES REQUESTED**,
five follow-up findings (C1-C5).

**Response:** all five findings are **accepted without exception**. C1 was a regression introduced by
agent 1's own follow-up commit `a17223f4`, and C5 weakened agent 1's own 90.2 evidence; both were
fixed first and the evidence was re-derived. The independent code citations of the review were
re-verified in the current tree before every fix (see the per-finding table).

| Finding | Agent 1 position | Independent check before the fix | Fix commit |
|---|---|---|---|
| C1 | accepted — a real regression of `a17223f4`: `addBlockingWaiter` registered the same waiter once per key argument, so `BLPOP k k` registered it twice, and the global cleanup mutated the serving list while it was iterated | `BlockingList.java:479-484` / `533-538` call `removeEverywhere(promise)` inside the `while (it.hasNext())` loop (416) — confirmed | `de9fe620` |
| C2 | accepted — the list recheck helpers only released the serving key | `BGroup.completeIfListNotEmpty` used `BlockingList.removeOne(...)` only — confirmed | `c05fa127` |
| C3 | accepted — the terminal zset wrong-type branch only released the serving key | `BGroup.completeIfZSetNotEmpty` used `BlockingZSet.removeOne(...)` only — confirmed | `45a91226` |
| C4 | accepted — the document listed supported CLIENT/ACL subcommands as missing | `CGroup.java:163,183,187,212,235,256` and `AGroup.java:176,199` implement them — confirmed; the audit went further, `no-evict`/`no-touch` are accepted no-op flags and were removed as well, the remaining `CLIENT` subcommands answer `NOT_SUPPORT` and stay listed | `3b37482a` |
| C4 (second re-review) | accepted — the two remaining documentation items | `CLIENT REPLY` was still called missing in the summary paragraphs (`CGroup.java:187-210` implements it) and the coverage table did not reconcile with the section lists (`Connection 23-16=7` vs 8 listed, `Server 58-26=32` vs 16 listed) — confirmed by a script over the document | `930b12b1` |
| C5 | accepted — agent 1's own assertion was a no-op: the helper returned a primitive `boolean` and the caller asserted `stored != null`, which is always true | the deeper defect: `AbstractPromise.getResult()` **does not block** (it returns `null` until the promise is completed), so the helper had to await the owner-worker read — confirmed with a probe spec whose deliberately false inner comparison still passed | `4ed30995` |

### Systemic note discovered while fixing C5

`io.activej.promise.Promise` declares **both** `T getResult()` and `boolean isResult()`:

- `getResult()` is a **non blocking peek** — it returns `null` while the promise is not completed, so
  `promise.getResult()` in a test can read `null` instead of waiting (this is what made C5's helper
  return `false`);
- Groovy property access `promise.result` resolves to **`isResult()`** (a boolean), not to the value —
  the same trap as `promise.exception` → `isException()`.

The repo's specs use the idiom `(reply as AsyncReply).settablePromise.whenResult { result -> result == X }.result`
**79 times** (76 with the trailing `.result`). Two problems: the comparison inside the closure is
**never asserted** (Spock only asserts the expressions of a `then:`/`expect:` block) and the outer
`.result` is a non blocking peek, so the idiom only proves "a non-null value arrived". A probe spec
with a deliberately false inner comparison passed. It was removed from the seven timeout assertions of
`BGroupTest` while fixing 90.7, but the remaining occurrences are a **separate cleanup** — they are
not part of bug 90 and were not changed here.

### Notes on the fix shape

- C1 is fixed in two parts: the waiter is registered **once per distinct key** (redis deduplicates
  the keys of a blocking command, `redis/src/blocked.c:379-383`), and the global cleanup is
  **deferred until after the delivery iteration**, so it can never mutate the serving list while it is
  iterated. The distinct keys are also what the waiter carries, so the release counter stays correct.
- C2 and C3 reuse the same mechanism as the successful delivery (`removeEverywhere`), so every
  terminal path (success, wrong type, timeout) releases every registration.
- C4 changed the document only, the counts of the affected sections were recomputed.
- C5 changed the test only, the production behaviour of `ccba406e` is unchanged and stays covered.

**Regression after the C1-C5 fixes:** `io.velo.command.*` + `io.velo.reply.*` + `io.velo.persist.*`
→ **802 tests, 0 failures, 0 errors**.

### The C4 follow-up (second re-review)

The second re-review accepted C1, C2, C3 and C5 and kept **C4** open with two concrete items, both
accepted and fixed in `930b12b1`:

1. **`CLIENT REPLY` was still called missing** in the two summary paragraphs (the detailed Connection
   note already listed it as implemented) — `REPLY` is implemented in `CGroup.java:187-210` and is now
   removed from both statements.
2. **The aggregate counts did not reconcile** — the sections list the missing commands but the
   coverage table carried independent numbers (`Connection 23 - 16 = 7` against 8 listed, `Server
   58 - 26 = 32` against 16 listed). The table now derives the implemented and coverage columns from
   the **exact** section counts, the commands column stays an estimate and is labelled as such, and
   the sections that listed the *implemented* commands (`List`, `Set`, `Sorted Set`) became
   no-missing sections, so every section lists missing commands only. A script verifies that every
   section header matches its entry list and that every table row reconciles
   (`total - missing = implemented`).

**Still open (unchanged):** the two-worker `*STORE` end-to-end and the asynchronous BZMPOP wrong-type
path are covered by the reviewer's own real-persistence probes but not by a permanent two-worker
spec in the repository; there is still no live TCP capture of the timeout frame and no exhaustive
numeric-argument matrix against Redis 7.2. These are recorded in the Residual Risk section and were
not claimed as fixed by the green unit tests.

## Re-review — C1-C5 fixed commits (AI agent 2, 2026-11-19)

**Skill:** `doc/skills/velo-bug-review/SKILL.md`, Stage 5.
**Reviewed range:** `b85565eb..fa086da70431ab706ef96f4f2a528fba9bacbd3a`.
**Conclusion:** C1/C2/C3/C5 have disposition `fixed`; C4 remains `PARTIALLY CONFIRMED` with no
closing disposition. Only the remaining documentation corrections are requested in this pass.
The original C1-C5 Status fields and the C4 summary row above have been updated in place.

### Review Feedback

| Item | Commit | Verdict | Disposition | Independent verification |
|---|---|---|---|---|
| C1 | `de9fe620` | CONFIRMED (reproduced) | fixed | Registration now deduplicates keys and waiter slot lists. Global list cleanup runs after the serving-list iteration. Both original reviewer probes (negative count and duplicate waiter followed by another client) passed unchanged. Both left/right deferred-cleanup lines were executed. |
| C2 | `c05fa127` | CONFIRMED (reproduced) | fixed | Both `completeIfListNotEmpty` and `completeMpopIfListNotEmpty` call `removeEverywhere`; the original failing list recheck probe now passes. JaCoCo marks `BGroup.java:1288,1548` executed. |
| C3 | `45a91226` | CONFIRMED (reproduced) | fixed | Terminal WRONGTYPE completion now releases every registration. The original failing wrong-type cleanup probe passed; `BGroup.java:1996` executed. This also closes the remaining terminal-error half of 90.1. |
| C4 | `3b37482a` | PARTIALLY CONFIRMED | — | Supported CLIENT/ACL entries are removed from the detailed missing lists, but CLIENT REPLY is still called missing at `REDIS_COMMANDS_NOT_IMPLEMENTED.md:304,340`. Coverage totals also remain inconsistent with the lists. |
| C5 | `4ed30995` | CONFIRMED (inspection) | fixed | `keyExistsOnWorker` uses `CompletableFuture.get(5, TimeUnit.SECONDS)`; callers assert `stored` and `!missing`, with `isComplete()` checked before reading the STORE result. The complete `ZGroupTest`, including positive/negative owner-worker reads, passed. `def` and imports now follow GroovyStyleRefer. |

The implementation changes are small and directly address the reported terminal paths. C1's two
parts cover both the Redis duplicate-key semantics and the unsafe iteration mechanism. C5 now
observes worker completion and can distinguish an existing key from a missing one, so its result
is meaningful rather than a non-null boolean check.

### C4 remaining corrections

The current source still says:

```text
304:     state commands (`REPLY`, `CACHING`, `PAUSE`, `UNPAUSE`, `UNBLOCK`) are missing
340:   - Implement the missing connection management commands (the client tracking family, `REPLY`,
```

`CLIENT REPLY` is implemented in `CGroup.java:187-210`, and the detailed Connection note in the
same document correctly includes it. Remove REPLY from both remaining statements.

The section-heading counts now match their bullet lists, but the summary totals do not reconcile:

```text
Connection header=8 listed=8
Server header=16 listed=16
| Connection | 23 | 16 | 70% |
| Server | 58 | 26 | 45% |
```

For Connection, 23 total minus 16 implemented implies 7 missing, not 8. For Server, 58 minus 26
implies 32 missing, not 16. The commit changes Server implemented coverage from 28 to 26 while
removing two falsely missing entries. Recompute using consistent command/subcommand identities,
or explicitly remove/qualify unsupported aggregate estimates; this review does not invent corrected
totals from inconsistent inputs. C4 is still the same documentation finding, not a new bug id.

### Executed evidence

Focused command (temporary compiler/probe init only; no repository test code changed):

```bash
./gradlew -I /tmp/bug90-fix-review.init.gradle :test \
  --tests io.velo.command.BGroupTest \
  --tests io.velo.command.BlockingListTest \
  --tests io.velo.command.BlockingZSetTest \
  --tests io.velo.command.ZGroupTest \
  --tests io.velo.command.Bug90FixReviewProbe \
  --tests io.velo.command.Bug90StoreReviewProbe
```

Pasted final XML summaries and log evidence (`/tmp/bug90-c1-c5-review-final.log`):

```text
io.velo.command.BGroupTest tests=50 failures=0 errors=0
io.velo.command.BlockingListTest tests=5 failures=0 errors=0
io.velo.command.BlockingZSetTest tests=3 failures=0 errors=0
io.velo.command.Bug90FixReviewProbe tests=4 failures=0 errors=0
io.velo.command.Bug90StoreReviewProbe tests=1 failures=0 errors=0
io.velo.command.ZGroupTest tests=13 failures=0 errors=0
BUILD SUCCESSFUL in 37s
```

**Total: 76 tests, zero failures/errors.** All four prior RED reviewer probes are now GREEN.
The real two-worker probe also passed STORE/wake-up, owner read, failed-write propagation and
asynchronous zset WRONGTYPE checks. In the first combined attempt, that temporary probe left a
readonly flag in the shared test persistence directory and caused the following ZGroupTest to
fail. Its temporary cleanup was corrected to restore writable state on the owner worker, and the
entire selection was rerun successfully; that fixture interference is not a production finding.

Pasted `scripts/jacoco_cover.py` summaries after the successful rerun:

```text
io.velo.command.BGroup 1193-1203 Lines: 7 | Covered: 7 | Partial: 0 | Not-covered: 0
io.velo.command.BGroup 1286-1289 Lines: 3 | Covered: 3 | Partial: 0 | Not-covered: 0
io.velo.command.BGroup 1546-1549 Lines: 3 | Covered: 3 | Partial: 0 | Not-covered: 0
io.velo.command.BGroup 1994-1998 Lines: 4 | Covered: 4 | Partial: 0 | Not-covered: 0
io.velo.command.BlockingList 482-555 Lines: 55 | Covered: 52 | Partial: 3 | Not-covered: 0
```

Inspected the corresponding JaCoCo HTML source pages: `BlockingList.java:487,541,545,546`
(defer and perform global cleanup), and `BGroup.java:1197,1288,1548,1996` (distinct keys and
terminal cleanup) are marked fully covered. Coverage confirms the changed paths ran.

### Four-evidence gate

Read each C1-C5 Fix Log row and checked the cited RED result, actual commit diff, current focused
GREEN execution, and JaCoCo evidence. C1-C3 also have independent prior RED probes in this same
document and unchanged GREEN probes in this pass. C5's author-recorded RED evidence is followed
by meaningful present/missing-key assertions and a passing real-worker test; its implementation
changes test code only. C4 changes documentation only and has no test/JaCoCo obligation, but
inspection prevents assigning it a fixed disposition. Historical checkouts were not replayed.

### Recommended fix order

1. Finish **C4**: remove both stale CLIENT REPLY claims and reconcile or qualify the aggregate
   counts using a consistent command/subcommand inventory.
2. Re-review that documentation commit; C1/C2/C3/C5 require no additional changes for these findings.

## Residual Risk

- C4 remains partly corrected; the report's original 90.6 documentation finding is not closed.
- The permanent tests do not include every two-worker scenario covered by the temporary probes.
  Keeping those scenarios as permanent regressions remains useful follow-up coverage.
- The broader `.result` / `whenResult` assertion idiom noted by agent 1 is outside C1-C5; this pass
  does not validate all such assertions or claim that cleanup was performed.
- No full-suite run, live Redis TCP differential, independent historical per-commit replay, or
  broader numeric-argument matrix was performed. Only this review document changed in the repo.

