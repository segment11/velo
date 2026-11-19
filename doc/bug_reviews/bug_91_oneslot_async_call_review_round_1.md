# Bug 91 — `OneSlot.asyncCall` Usage & Thread-Affinity Review (Round 1)

**Module:** `io.velo.persist.OneSlot.asyncCall(...)` and all 30 production call sites across `io.velo.command.*`, `io.velo.persist.LocalPersist`, `io.velo.repl.incremental.XBigStrings`, `dyn/src/io/velo/command/ManageCommand.groovy`, and `dyn/src/io/velo/command/ClusterxCommand.groovy`
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** Stage 4 complete — 91.1 fixed (`e56ed7a8`), 91.3 fixed (`3fca457a`), 91.4 fixed (`01b5b74a`); 91.2/91.5 no change needed. Test helper follow-up `7f0cf850`. Formal Stage 5 re-review pending.
**Review branch:** `main`
**Reviewed commit:** `a17223f4807b26ff12ad7e3e240191db65d17dab`

> Delta declaration: n/a (round 1). Cross-references prior rounds without re-reporting closed/accepted items:
> - `bug_52_activej_usage_review_round_1.md` (ActiveJ reactor bounce & socket/promise affinity — closed)
> - `bug_84_pgroup_commands_review_round_1.md` (Finding 84.1 added `dstOneSlot.asyncCall` to `PGroup.pfmerge` — re-examined here in Finding 91.2 strictly from the production reactor-bounce perspective)
> - `bug_85_rgroup_commands_review_round_1.md` (Finding 85.2 marked won't fix because production `Promise.ofFuture` bounces back to the source worker — consistent with the baseline used here)
> - `bug_90_blocking_zset_commands_review_round_1.md` (BZPOPMIN/BZPOPMAX/BZMPOP functional fixes — not re-reported; Finding 91.3 strictly addresses the missing `!isCrossRequestWorker` guard in `BGroup`)

## Scope

Files read (method + line ranges) at commit `a17223f4807b26ff12ad7e3e240191db65d17dab`:

- `src/main/java/io/velo/persist/OneSlot.java` — `isOwnedByCurrentThread()` (666-668), `asyncRun()` (676-708), `asyncExecute()` (715-727), `asyncCall()` (737-760)
- `src/main/java/io/velo/MultiWorkerServer.java` — `handleReplRequest()` (510-518), `handleRequest()` (525-635, cross-worker detection 576-593, worker routing 597-635), `getByteBufPromiseByOtherEventloop()` (650-687), `transferAsyncReply()` (695-711), `handlePipeline()` (726-754), `onStart()` (1060-1150), `onStop()` (`cleanUpAsync()` call at 1326)
- `src/main/java/io/velo/persist/LocalPersist.java` — `bindOneSlotIfInitialized()` (213-225), `doSthInSlots()` (277-304), `addOneSlot()` (311-330), `addOneSlotForTest2()` (338-351), `firstOneSlot()` (668-680), `cleanUpAsync()` (910-934)
- `src/main/java/io/velo/BaseCommand.java` — `execute()` (454-500)
- `src/main/java/io/velo/command/AGroup.java` — `parseSlots()` (74-107), `acl()` (`deluser` 176-198, `load` 277-330, `save` 361-390, `setuser` 391-471), `appendAclUpdateBinlog()` (499-519)
- `src/main/java/io/velo/command/BGroup.java` — `parseSlots()` (220-307), `blpop()` (1046-1152), `popOtherKeyAsync()` (1164-1184), `blmpop()` (1295-1411), `popOtherKeyMpopAsync()` (1423-1438), `bzpop()` (1545-1651), `bzmpop()` (1660-1794), `popOtherKeyZAsync()` (1806-1821), `popOtherKeyZMpopAsync()` (1833-1848)
- `src/main/java/io/velo/command/CGroup.java` — `parseSlots()` (93-107)
- `src/main/java/io/velo/command/DGroup.java` — `parseSlots()` (107-155), `del()` (273-336)
- `src/main/java/io/velo/command/EGroup.java` — `parseSlots()` (103-134), `exists()` (174-241)
- `src/main/java/io/velo/command/FGroup.java` — `parseSlots()` (71-74), `failover()` (99-200)
- `src/main/java/io/velo/command/IGroup.java` — `doSthInSlots` call sites (352, 560)
- `src/main/java/io/velo/command/KGroup.java` — `parseSlots()` (60-63), `keys()` (80-144)
- `src/main/java/io/velo/command/MGroup.java` — `parseSlots()` (101-145), `mget()` (270-346), `msetnx()` (430-530), `migrate()` (553-747)
- `src/main/java/io/velo/command/PGroup.java` — `parseSlots()` (163-215), `pfcount()` (472-530), `pfmerge()` (532-638)
- `src/main/java/io/velo/command/RGroup.java` — `parseSlots()` (146-183), `rename()` (369-447)
- `src/main/java/io/velo/command/SGroup.java` — `parseSlots()` (313-390), `sdiff()` (1330-1403), `sdiffstore()` (1406-1515), `sintercard()` (1600-1663), `smove()` (2135-2192)
- `src/main/java/io/velo/command/ZGroup.java` — `parseSlots()` (303-431), `zdiff()` (1430-1510), `zintercard()` (1630-1710), `zmpop()` (2800-2911)
- `src/main/java/io/velo/command/XGroup.java` — `applyAsync()` loop (1900-1965)
- `src/main/java/io/velo/repl/incremental/XBigStrings.java` — `apply()` (193-206), `applyAsync()` (209-224)
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java` — `apply()` (126-134)
- `dyn/src/io/velo/command/ManageCommand.groovy` — `parseSlots()` (66-103), `dynConfig()` (1018-1060)
- `dyn/src/io/velo/command/ClusterxCommand.groovy` — `info()` (515-598), `reset()` (955-978)
- Unit tests inspected for `eventloopCurrent.run()` and `asyncCall` patterns:
  - `src/test/groovy/io/velo/persist/OneSlotTest.groovy` (33-60, 700-774)
  - `src/test/groovy/io/velo/command/AGroupTest.groovy` (504-638)
  - `src/test/groovy/io/velo/command/BGroupTest.groovy` (1040-1106, 1564-1595)
  - `src/test/groovy/io/velo/command/DGroupTest.groovy` (275-333)
  - `src/test/groovy/io/velo/command/EGroupTest.groovy` (126-158)
  - `src/test/groovy/io/velo/command/FGroupTest.groovy` (77-170)
  - `src/test/groovy/io/velo/command/KGroupTest.groovy` (48-120)
  - `src/test/groovy/io/velo/command/MGroupTest.groovy` (146-230, 305-395)
  - `src/test/groovy/io/velo/command/PGroupTest.groovy` (476-591)
  - `src/test/groovy/io/velo/command/RGroupTest.groovy` (383-425)
  - `src/test/groovy/io/velo/command/SGroupTest.groovy` (1585-1680, 2501-2561)
  - `src/test/groovy/io/velo/command/ZGroupTest.groovy` (1259-1345, 1608-1685)
  - `src/test/groovy/io/velo/command/ZGroupPart2Test.groovy` (1450-1553)

Reference docs (ground truth):
- `doc/design/11_multithreading_design.md` — worker domains, slot affinity (`slot % slotWorkers`), `@ThreadNeedLocal`, cross-thread coordination (lines 19-25, 67-90, 128-149)
- `doc/design/04_command_processing_design.md` — net-worker slot parsing, thread handoff, `isCrossRequestWorker` (lines 42-102, 218-235)
- `doc/design/12_server_bootstrap_design.md` — worker eventloop initialization and shutdown
- ActiveJ 6.0-beta2 source (`/home/kerry/ws/src_learn/activej`):
  - `core-promise/src/main/java/io/activej/promise/Promise.java:197-222` (`Promise.ofFuture` → `Promise.ofCompletionStage` captures caller `Reactor`, calls `reactor.startExternalTask()`, posts completion back via `reactor.execute(...)`)
  - `core-promise/src/main/java/io/activej/promise/AbstractPromise.java:83-90` (`getResult()` returns `null` when `result == PROMISE_NOT_SET`; `getException()` returns `null` while incomplete)

## Production Execution Baseline vs. Unit Test `eventloopCurrent.run()`

1. **Production data flow**:
   - Every slot `s` is owned by slot worker `w(s) = s % slotWorkers`, bound via `oneSlot.threadIdProtectedForSafe = slotWorkerThreadIds[w(s)]` (`LocalPersist.java:217-218`).
   - Every client command with a non-empty `parseSlots(...)` result is dispatched by `MultiWorkerServer.handleRequest` (`MultiWorkerServer.java:597-635`) to `slotWorkerEventloopArray[firstSlot % slotWorkers]`, where `firstSlot = slotWithKeyHashList.getFirst().slot()`. Commands with an empty `parseSlots(...)` list (`SLOT_CAN_HANDLE_BY_ANY_WORKER == -1`) go to a random slot worker.
   - Inside every slot worker thread, `Reactor.getCurrentReactorOrNull()` is **always non-null** (`slotWorkerEventloopArray[i]`).
   - Therefore, in production, `OneSlot.asyncCall(...)` (`OneSlot.java:737-760`) takes only **two** paths:
     - **Branch 1 (inline)**: `Thread.currentThread().threadId() == threadIdProtectedForSafe` → runs immediately on the current thread and returns `Promise.of(...)`.
     - **Branch 2 (reactor bounce)**: `Reactor.getCurrentReactorOrNull() != null` → `Promise.ofFuture(slotWorkerEventloop.submit(AsyncComputation.of(supplierEx)))`. ActiveJ runs the supplier on the target slot worker and **bounces every `.whenComplete` / `.then` callback back onto the caller's slot worker reactor**.
   - **Branch 3 (non-reactor fallback, `SettablePromise` + `slotWorkerEventloop.execute(...)` with no bounce-back)** is **never reached** by command or replication handlers in production; it only runs for non-reactor threads (such as `MultiWorkerServer.onStop()` calling `LocalPersist.cleanUpAsync()` on the primary shutdown thread, or unit tests that omit `Eventloop.builder().withCurrentThread().build()`).
2. **Unit test `eventloopCurrent.run()` pattern**:
   - Most command tests place `slot 0` on a background thread (`LocalPersist.instance.addOneSlot(slot, eventloop)`), bind an idle reactor to the Spock thread via `Eventloop.builder().withCurrentThread().build()`, force `crossRequestWorker = true`, invoke `execute(...)` on the Spock thread, and call `eventloopCurrent.run()` to drain the `Promise.ofFuture` completion bounced back to the Spock thread.
   - Because the Spock thread is **not** `slot 0`'s owner thread (unlike production, where `firstSlot` is always owned by the calling worker), tests mask same-thread vs. cross-thread routing. In `PGroupTest.groovy:534-586`, the test even puts `dstKey` on `eventloopDst` and runs `pfmerge` from `eventloopSrc` — the reverse of production routing. Conversely, `AGroupTest.groovy` pins `fixSlotThreadId(0, currentThread)` without `eventloopCurrent`, masking the cross-thread bug in `AGroup.appendAclUpdateBinlog()`.

---

## Finding 1 (Bug 91.1): `AGroup.appendAclUpdateBinlog()` calls `firstOneSlot.asyncCall()` then reads `p.getResult()` synchronously — redundant on worker 0 (`ACL SETUSER`/`DELUSER` standalone) and broken on cross-thread paths (`ACL SAVE`, `ACL LOAD`, cluster mode)

**Severity:** High
**Class:** Concurrency / Correctness
**Status:** `CONFIRMED (reproduced)` — agent 2; evidence and qualifications in Review Feedback

**Disposition:** fixed (`e56ed7a8`); see Stage 4 Fix Log.

### Cited files / lines
```java
// src/main/java/io/velo/command/AGroup.java:77-96 — parseSlots() skips 2-arg ACL subcommands and hardcodes slot 0
        if ("acl".equals(cmd)) {
            if (data.length > 2) {
                var isDryrun = "dryrun".equalsIgnoreCase(new String(data[1]));
                if (isDryrun) {
                    ...
                } else {
                    slotWithKeyHashList.add(SlotWithKeyHash.TO_FIX_FIRST_SLOT);
                }
            }
        }
```
```java
// src/main/java/io/velo/command/AGroup.java:317-324 — ACL LOAD (data.length == 2 -> random worker), ignores returned Promise
                    firstOneSlot.asyncCall(() -> {
                        try {
                            firstOneSlot.getBinlog().append(new XAclUpdate(lines));
                        } catch (IOException e) {
                            throw new RuntimeException("Append binlog error, acl lines=" + lines, e);
                        }
                        return null;
                    });
```
```java
// src/main/java/io/velo/command/AGroup.java:499-519 — appendAclUpdateBinlog() called by deluser (197), save (386), setuser (466)
    private void appendAclUpdateBinlog() {
        var line = dataToLine();
        var firstOneSlot = localPersist.firstOneSlot();
        if (firstOneSlot != null && firstOneSlot.getDynConfig().isBinlogOn()) {
            var p = firstOneSlot.asyncCall(() -> {
                try {
                    firstOneSlot.getBinlog().append(new XAclUpdate(line));
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException("Append binlog error, acl line=" + line, e);
                }
            });
            // sync
            var r = p.getResult();
            var e = p.getException();
            if (e != null) {
                throw (RuntimeException) e;
            }
            log.warn("Append binlog success, acl line={}, result={}", line, r);
        }
    }
```
```java
// src/main/java/io/velo/persist/LocalPersist.java:668-680 — in cluster mode firstOneSlot() need not be inner slot 0
    public @Nullable OneSlot firstOneSlot() {
        if (oneSlots == null) {
            return null;
        }
        if (!ConfForGlobal.clusterEnabled) {
            return oneSlots[0];
        }
        var toClientSlot = multiShard == null ? null : multiShard.firstToClientSlot();
        return getInnerOneSlotByToClientSlot(toClientSlot);
    }
```

### Root cause
Two interacting defects around `firstOneSlot.asyncCall(...)` in `AGroup`:

1. **Redundant `asyncCall` on the intended happy path (`ACL SETUSER` / `ACL DELUSER`, non-cluster mode)**:
   `parseSlots` pins `TO_FIX_FIRST_SLOT` (`slot = 0`), so `MultiWorkerServer` always dispatches `ACL SETUSER` / `ACL DELUSER` to slot worker 0. When `!ConfForGlobal.clusterEnabled`, `localPersist.firstOneSlot()` is `oneSlots[0]` (owned by worker 0). Thus `firstOneSlot.asyncCall(...)` at line 503 is **always on the same thread** in standalone mode and executes inline — which is the *only* reason the `// sync` comment and immediate `p.getResult()` / `p.getException()` check at lines 511-516 work.
2. **Broken synchronous `p.getResult()` whenever execution actually crosses threads**:
   - `ACL SAVE` (`data = ["acl", "save"]`, `data.length == 2`) fails `if (data.length > 2)` in `parseSlots`, so `slotWithKeyHashList` is empty (`SLOT_CAN_HANDLE_BY_ANY_WORKER`). With `slotWorkers >= 2`, `MultiWorkerServer` dispatches `ACL SAVE` to a **random** slot worker. Whenever it lands on worker `!= 0`, `firstOneSlot.asyncCall(...)` submits asynchronously via `Promise.ofFuture(...)` and returns an **incomplete** `Promise`.
   - In cluster mode (`ConfForGlobal.clusterEnabled == true`), `firstOneSlot()` resolves to the shard's first owned inner slot (`LocalPersist.java:678-679`). If `firstOneSlot().slot() % slotWorkers != 0`, even `ACL SETUSER` and `ACL DELUSER` (pinned to `TO_FIX_FIRST_SLOT = 0`) run on a different worker than `firstOneSlot`.
   - ActiveJ `AbstractPromise.getResult()` returns `null` when `result == PROMISE_NOT_SET` and `getException()` returns `null` while pending (`AbstractPromise.java:83-90`). Calling them immediately without awaiting neither waits for the binlog append nor observes any `IOException` — `AGroup` returns `OKReply.INSTANCE` and logs `"Append binlog success, ..., result=null"` before the append runs or even if it fails.
   - Similarly, `ACL LOAD` (`AGroup.java:317`, `data.length == 2`) lands on a random worker and discards the returned `Promise` completely (fire-and-forget), returning `OK` before replication binlog append completes and silently dropping failures.
   - `AGroupTest.groovy` never caught this because every test calls `localPersist.fixSlotThreadId(0, Thread.currentThread().threadId())` (`AGroupTest.groovy:518, 563, 614`), forcing the inline branch.

### Trigger / impact
- Standalone with `slotWorkers >= 2`, binlog on: `ACL SAVE` or `ACL LOAD` landing on `slot-worker-1` returns `OK` before `XAclUpdate` is written to slot 0's binlog and silently swallows binlog `IOException`s; `ACL SAVE` logs `result=null`.
- Cluster mode with `slotWorkers >= 2` on a shard whose `firstOneSlot().slot() % slotWorkers != 0`: **every** `ACL SETUSER`, `ACL DELUSER`, `ACL SAVE`, and `ACL LOAD` suffers the same silent-failure / premature-`OK` race.
- Standalone `ACL SETUSER` / `ACL DELUSER` on worker 0: `asyncCall` is dead machinery because execution is already on `firstOneSlot`'s thread.

### Suggested fix direction
Choose one consistent threading contract (prefer option (a) to eliminate `asyncCall` altogether):
- **(a) Pin all binlog-writing ACL subcommands (`setuser`, `deluser`, `save`, `load`) to `firstOneSlot`'s actual slot in `AGroup.parseSlots`**: return `firstOneSlot().slot()` (when non-null, falling back to `TO_FIX_FIRST_SLOT` in tests) for `data.length >= 2` on mutating/binlog subcommands (`setuser`, `deluser`, `save`, `load`). Then `handle()` is **guaranteed** to run on `firstOneSlot`'s own worker thread in both standalone and cluster modes, so both `AGroup.java:317` and `AGroup.java:503` can call `firstOneSlot.getBinlog().append(...)` directly with no `asyncCall`. (Verify `parseSlots` purity: reading `localPersist.firstOneSlot()` uses the volatile `oneSlots` / `multiShard` reference and touches no mutable slot state.)
- **(b) Or return `AsyncReply`**: if `ACL` subcommands remain on arbitrary workers, return an `AsyncReply` settled from `firstOneSlot.asyncCall(...).whenComplete(...)` instead of calling `p.getResult()` synchronously or discarding the `Promise`.

---

## Finding 2 (Bug 91.2): `PGroup.pfmerge()` wraps the destination merge/write in `dstOneSlot.asyncCall()` even though production always executes and bounces back onto `dstOneSlot`'s own worker thread

**Severity:** Low
**Class:** Concurrency / Redundant dispatch
**Status:** `NOT A BUG` — agent 2; evidence and qualifications in Review Feedback

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:198-204 — dst key is index 0
        if ("pfmerge".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }
```
```java
// src/main/java/io/velo/command/PGroup.java:596-630 — dstOneSlot.asyncCall inside whenComplete
                // The destination key may only be touched on its owning slot worker thread.
                // On the reactor path (production: handle() runs on the dst slot worker and
                // Promise.ofFuture bounces every source promise completion back to that reactor),
                // this callback already runs on the dst worker thread and asyncCall executes inline.
                // On the non-reactor fallback path (no bounce, e.g. tests calling execute() directly),
                // this callback runs on a source slot worker thread, so asyncCall submits the merge
                // to the dst slot eventloop instead — see bug 84.1.
                var dstOneSlot = localPersist.oneSlot(dstS.slot());
                dstOneSlot.asyncCall(() -> {
                    ...
                    saveHll(dstS, dstHll, getHllExpireAt(dstS));
                    return Boolean.TRUE;
                }).whenComplete((r1, e1) -> {
                    ...
                });
```
```java
// src/main/java/io/velo/command/SGroup.java:1480-1502 — sibling command with identical routing (no asyncCall on dst)
        // The dst key is the first key, so this request worker also owns the dst slot. On the
        // reactor path (production), Promise.ofFuture bounces every source promise completion
        // back to this reactor, so this callback and the raw removeDelay/saveRedisSet on the
        // dst key run on the dst slot worker thread.
        Promises.all(promises).whenComplete((r, e) -> {
            ...
            removeDelay(dstSlotWithKeyHash);
```

### Root cause
`PGroup.parseSlots("pfmerge", ...)` places `dstS` at index 0 (`slotWithKeyHashList.getFirst()`). In real production:
1. `MultiWorkerServer.handleRequest` (`MultiWorkerServer.java:597, 629-634`) always dispatches `PFMERGE` to `slot-worker-(dstS.slot() % slotWorkers)`, which is `dstOneSlot`'s owning thread and an active `Eventloop`.
2. Every source read at `PGroup.java:570` (`oneSlot.asyncCall(...)`) runs with `Reactor.getCurrentReactorOrNull() != null`, so `OneSlot.asyncCall` uses `Promise.ofFuture(...)` and bounces every completion back onto `dstOneSlot`'s worker eventloop.
3. Consequently, `Promises.all(promises).whenComplete(...)` (`PGroup.java:577`) is **100% guaranteed to run on `dstOneSlot`'s owning thread** in production, and `dstOneSlot.asyncCall(...)` at line 604 always executes inline (`threadId == threadIdProtectedForSafe`).

This wrapper was introduced in bug 84.1 (`8870d553`) because:
- the reviewer's bug 84.1 repro ran outside a current-thread reactor (triggering `OneSlot.asyncCall`'s non-reactor fallback Branch 3, which cannot happen on a production slot worker), and
- the permanent test in `PGroupTest.groovy:534-586` constructs `eventloopSrc` with `.withCurrentThread()`, places `dstKey` on a separate background `eventloopDst`, and executes `pfmerge $dstKey $srcKey` from `eventloopSrc` (violating production routing, where `pfmerge` always runs on `eventloopDst`).

### Trigger / impact
- No correctness bug in production (inline fast path executes on `dstOneSlot`'s thread), but adds unnecessary nested `asyncCall` + `.whenComplete` + `Promise` allocation on every cross-worker `PFMERGE` and creates an inconsistent pattern compared with `SGroup.sdiffstore` (`SGroup.java:1486-1535`), `RGroup.rename` (`RGroup.java:439-444`), and `SGroup.smove` (`SGroup.java:2186-2188`).
- Leaves the codebase carrying production code solely to satisfy an artificially routed unit test (`PGroupTest.groovy:534-586`).

### Suggested fix direction
- If the project intends to keep direct-call non-reactor tests working, document this explicitly as a test-compatibility tradeoff (`won't fix`).
- Otherwise, align `PGroup.pfmerge()` with `SGroup.sdiffstore()` / `RGroup.rename()` / `SGroup.smove()`: perform `getHll(dstS)` + `mergeHllInto` + `saveHll(...)` directly inside the `Promises.all(promises).whenComplete(...)` `try/catch` block (preserving `TypeMismatchException` → `ErrorReply.WRONG_TYPE_HLL` and `Exception` → `finalPromise.setException(...)`), and update `PGroupTest.groovy:534-586` to match production routing (bind `eventloopDst` to `.withCurrentThread()` and `eventloopSrc` to the background thread, then drain completions with `eventloopDst.run()`).

---

## Finding 3 (Bug 91.3): `BGroup`'s four multi-key blocking pops (`BLPOP`/`BRPOP`, `BLMPOP`, `BZPOPMIN`/`BZPOPMAX`, `BZMPOP`) never check `isCrossRequestWorker`, unnecessarily calling `OneSlot.asyncCall()` on same-worker multi-key commands

**Severity:** Low (revised scope: wrong RESP error prefix and inaccurate comments)
**Class:** Redis protocol compatibility / Documentation
**Status:** `CONFIRMED (reproduced)` — agent 2 follow-up accepts revised protocol scope; see Review Feedback follow-up

**Disposition:** fixed (`3fca457a`); see Stage 4 Fix Log.

### Cited files / lines
```java
// src/main/java/io/velo/command/BGroup.java:1075-1118 — blpop()/brpop() checks keys[0], then unconditionally enters asyncCall chain
        var rl = LGroup.getRedisList(firstSlotWithKeyHash, this);
        if (rl == null || rl.isEmpty()) {
            // performance bad, get all other values
            if (keys.size() > 1) {
                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);
                ...
                // the continuation below runs on the worker of the last checked key, which is also the
                // reactor used to schedule the timeout, the reply itself is sent back through the promise
                popOtherKeyAsync(1, keys, isLeft).whenComplete((r, e) -> { ... });
                return asyncReply;
            }
```
```java
// src/main/java/io/velo/command/BGroup.java:1164-1184 — popOtherKeyAsync() (line 1171)
// src/main/java/io/velo/command/BGroup.java:1423-1438 — popOtherKeyMpopAsync() (line 1430)
// src/main/java/io/velo/command/BGroup.java:1806-1821 — popOtherKeyZAsync() (line 1813)
// src/main/java/io/velo/command/BGroup.java:1833-1848 — popOtherKeyZMpopAsync() (line 1840)
        var slotWithKeyHash = slotWithKeyHashListParsed.get(fromIndex);
        return localPersist.oneSlot(slotWithKeyHash.slot()).asyncCall(() -> {
            ...
        }).then(reply -> reply != null ? Promise.of(reply) : popOtherKey...Async(fromIndex + 1, ...));
```

### Root cause
`BGroup.parseSlots(...)` (`BGroup.java:242-304`) registers every key for `blpop`/`brpop`, `blmpop`, `bzpopmin`/`bzpopmax`, and `bzmpop`, so `MultiWorkerServer.handleRequest` (`MultiWorkerServer.java:577-593`) already computes `isCrossRequestWorker` accurately for all four command families and routes execution to `keys[0]`'s slot worker.

Unlike every other multi-key command group (`DGroup.del`, `EGroup.exists`, `MGroup.mget`/`msetnx`/`migrate`, `PGroup.pfcount`/`pfmerge`, `RGroup.rename`, `SGroup.sdiff`/`sdiffstore`/`sintercard`/`smove`, `ZGroup.zdiff`/`zintercard`/`zmpop`), none of the four methods in `BGroup` has an `if (!isCrossRequestWorker)` synchronous fast path:
1. When `!isCrossRequestWorker` (100% of calls when `slotWorkers == 1`, and all same-worker multi-key calls when `slotWorkers > 1`), **every key `keys[1..n-1]` is owned by the current slot worker thread**.
2. Yet all four methods allocate `SettablePromise`, `AsyncReply`, recursive `.then(...)` lambdas, and invoke `oneSlot.asyncCall(...)` per key (plus `addBlockingWaiter`/`recheckBlockingList` using `asyncExecute` loops when blocking).
3. In `blpop`/`brpop` (`BGroup.java:1118`) and `bzpopmin`/`bzpopmax` (`BGroup.java:1626`), unlike `blmpop` (`1400-1408`) and `bzmpop` (`1783-1791`), the method unconditionally returns `asyncReply` even when a secondary key on the same worker popped immediately and `finalPromise` is already complete — forcing `MultiWorkerServer.transferAsyncReply(...)` extra promise wiring for a synchronous hit.
4. Additionally, the comments at `BGroup.java:1086-1087` and `BGroup.java:1589-1590` (*"the continuation below runs on the worker of the last checked key"*) are inaccurate for production: `OneSlot.asyncCall` uses `Promise.ofFuture(...)` on a reactor, so every `.then` / `.whenComplete` continuation bounces back to **`keys[0]`'s worker**, not the last checked key's worker.

### Trigger / impact
- Any multi-key `BLPOP`/`BRPOP`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP` where `keys[0]` is empty and all keys map to the same slot worker (always true when `slotWorkers == 1`, common when hash-tagged keys `{tag}:a {tag}:b` are used in cluster mode).
- Unnecessary per-key `asyncCall` + promise-chain allocations and (for `blpop`/`brpop`/`bzpop*`) unnecessary `AsyncReply` wrapping on immediate pops; also causes `blpop`/`bzpop*` to invoke `localPersist.oneSlot(slot)` even when `byPassGetSet` is active in tests unless `LocalPersist` has a slot populated (contrast `BGroup.blpop` on line 1171).

### Suggested fix direction
Add an `if (!isCrossRequestWorker)` fast path in `blpop()`, `blmpop()`, `bzpop()`, and `bzmpop()`:
- Iterate `i = 1 .. keys.size() - 1` synchronously on the current thread; if any key is non-empty (or wrong-type), pop and return the synchronous `Reply` directly without `asyncCall` or `AsyncReply`.
- If all keys are empty, register the waiter directly via `BlockingList.addOne` / `BlockingZSet.addOne` for each key on the current worker (no `asyncExecute` fan-out or race-window recheck needed because all keys belong to this single-threaded slot worker!) and schedule the timeout on `Reactor.getCurrentReactor()`.
- Fix the misleading continuation-thread comments at `BGroup.java:1086-1087` and `BGroup.java:1589-1590`.

---

## Finding 4 (Bug 91.4): `ZGroup.zmpop()` submits key 0 (`firstSlot`, always owned by the current thread) through `OneSlot.asyncCall()` in its cross-worker fan-out

**Severity:** Low
**Class:** Performance / Redundant dispatch
**Status:** `CONFIRMED (inspection)` — agent 2; evidence and qualifications in Review Feedback

**Disposition:** fixed (`01b5b74a`); see Stage 4 Fix Log.

### Cited files / lines
```java
// src/main/java/io/velo/command/ZGroup.java:394-411 — key 0 is slotWithKeyHashList.getFirst()
        if ("zmpop".equals(cmd)) {
            ...
            for (int i = 0; i < numKeys; i++) {
                slotWithKeyHashList.add(slot(data[2 + i], slotNumber));
            }
            return slotWithKeyHashList;
        }
```
```java
// src/main/java/io/velo/command/ZGroup.java:2830-2853 — cross-slot path starts at i = 0 instead of checking key 0 inline
        // cross-slot path
        record IndexAndRz(int index, RedisZSet rz, boolean isWrongType) {
        }
        ArrayList<Promise<IndexAndRz>> rzPromises = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
            var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());
            int finalI = i;
            var p = oneSlot.asyncCall(() -> {
                ...
            });
            rzPromises.add(p);
        }
```

### Root cause
`MultiWorkerServer` always dispatches `ZMPOP` to `slotWithKeyHashListParsed.getFirst()`'s worker (`i = 0`). Because `ZMPOP` pops from the **first** non-empty key in argument order (or stops immediately with `WRONGTYPE` if key 0 has the wrong type), key 0 is always owned by the current thread in production.

Unlike `BGroup.bzmpop()` (`BGroup.java:1725-1736`), `SGroup.sdiff()` (`SGroup.java:1323-1370`), `SGroup.sintercard()` (`SGroup.java:1595-1632`), `ZGroup.zdiff()` (`ZGroup.java:1375, 1458-1464`), and `ZGroup.zintercard()` (`ZGroup.java:1631-1676`), `ZGroup.zmpop()` does not inspect key 0 inline before entering the cross-worker fan-out. Instead it reads and decodes **all** `numKeys` zsets across all workers via `oneSlot.asyncCall(...)` — including key 0 on the current thread — even when key 0 is non-empty or wrong-type and none of the remaining keys will ever be used.

### Trigger / impact
- Every cross-worker `ZMPOP` always pays a redundant `asyncCall` wrapper for `i = 0`.
- When key 0 is non-empty (or wrong-type), `ZMPOP` needlessly fans out cross-worker `asyncCall` reads and decodes every other key's zset, and returns an `AsyncReply` instead of an immediate synchronous reply.

### Suggested fix direction
Check `i = 0` (`slotWithKeyHashListParsed.getFirst()`) inline before the `if (!isCrossRequestWorker)` / cross-worker split (matching `BGroup.bzmpop()` at `BGroup.java:1725-1736`): if key 0 is wrong-type, return `ErrorReply.WRONG_TYPE`; if non-empty, pop, save, and return the synchronous `MultiBulkReply`. Only if key 0 is missing/empty should the cross-worker path fan out over `i = 1 .. numKeys - 1`.

---

## Finding 5 (Bug 91.5): Cross-worker `groupBySlot` loops (`DEL`, `EXISTS`, `MGET`, `MSETNX`, `MIGRATE`, `PFCOUNT`) submit `firstSlot` (and other same-worker slots) via `OneSlot.asyncCall()`

**Severity:** Low
**Class:** Performance / Redundant dispatch
**Status:** `NOT A BUG` — agent 2; evidence and qualifications in Review Feedback

### Cited files / lines
- `src/main/java/io/velo/command/DGroup.java:298-315` — `del()` (`oneSlot.asyncCall` at line 306)
- `src/main/java/io/velo/command/EGroup.java:199-216` — `exists()` (`oneSlot.asyncCall` at line 207)
- `src/main/java/io/velo/command/MGroup.java:296-318` — `mget()` (`oneSlot.asyncCall` at line 303)
- `src/main/java/io/velo/command/MGroup.java:461-477` & `495-516` — `msetnx()` phase 1 & phase 2 (`oneSlot.asyncCall` at lines 468, 501)
- `src/main/java/io/velo/command/MGroup.java:662-690` — `migrate()` (`oneSlot.asyncCall` at line 669)
- `src/main/java/io/velo/command/PGroup.java:493-502` — `pfcount()` (`oneSlot.asyncCall` at line 500)

```java
// Representative pattern (DGroup.java:300-315; identical shape in EGroup, MGroup, PGroup)
        var groupBySlot = slotWithKeyHashListParsed.stream().collect(Collectors.groupingBy(SlotWithKeyHash::slot));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ...
            });
            promises.add(p);
        }
```

### Root cause
All six commands guard their async path with `if (!isCrossRequestWorker)` and are dispatched by `MultiWorkerServer` to `firstSlot`'s slot worker. Inside the cross-worker branch, they group all keys by `slot` (including `firstSlot` and any other slot with `slot % slotWorkers == firstSlot % slotWorkers`) and wrap every group in `oneSlot.asyncCall(...)` + `promises.add(p)`.

In real production, `firstSlot` (and every same-worker slot in `groupBySlot`) is always owned by the current thread, so `oneSlot.asyncCall(...)` always takes its inline branch (`OneSlot.java:739-744`) and wraps the result in `Promise.of(...)`, which is then passed to `Promises.all(promises)`. (In `MGroup.msetnx()` phase 2 at line 501, the `whenComplete` callback has bounced back to `firstSlot`'s worker via `Promise.ofFuture`, so same-worker slots again run inline.)

Unit tests using `eventloopCurrent.run()` (`DGroupTest.groovy:281-290`, `EGroupTest.groovy:134-143`, `MGroupTest.groovy:154-160, 312-340`, `PGroupTest.groovy:485-491`) hide this because they assign `slot 0` to a separate background thread (`addOneSlot(slot, eventloop)`), causing even `firstSlot` to go cross-thread in tests.

### Trigger / impact
Low impact: `OneSlot.asyncCall` already avoids thread switching when `threadId == threadIdProtectedForSafe`, so the only overhead is allocating one lambda + `Promise.of(...)` + `Promises.all` slot per same-worker slot group. Keeping the uniform `groupBySlot` loop may be an acceptable simplicity tradeoff (`won't fix` candidate) unless benchmarks show measurable allocation overhead.

### Suggested fix direction
Optional micro-optimization (evaluate simplicity vs. benefit): check `oneSlot.isOwnedByCurrentThread()` (or partition local vs. remote slots) to execute local slots directly into the result accumulator without wrapping them in `Promise` objects, only awaiting `Promises.all` for remote-worker slots. Note that unit tests relying on `slot 0` running on a background thread with `byPassGetSet` continue to work automatically if partitioned via `oneSlot.isOwnedByCurrentThread()`.

---

## Notes / non-findings

### Complete 30-site Production Audit of `OneSlot.asyncCall`

All other production call sites were audited and confirmed **genuinely needed** for cross-thread safety in production (none are bugs):

| # | Location | Caller / Command | Calling Thread (Prod) | Target `OneSlot` Thread | Production Verdict |
|---|---|---|---|---|---|
| 1 | `PGroup.java:604` | `PFMERGE` dst merge/write | `w(dst)` (bounced back) | `w(dst)` | **Unnecessary (always same thread)** — see **91.2** |
| 2 | `AGroup.java:503` | `ACL SETUSER/DELUSER/SAVE` binlog | `w(0)` (SETUSER/DELUSER); random (SAVE) | `w(firstOneSlot)` | **Redundant (standalone SETUSER/DELUSER) & broken sync `getResult()` (SAVE / cluster)** — see **91.1** |
| 3 | `AGroup.java:317` | `ACL LOAD` binlog | random worker (`data.length == 2`) | `w(firstOneSlot)` | Cross-thread today (`Promise` dropped) — see **91.1** |
| 4 | `BGroup.java:1171` | `BLPOP`/`BRPOP` (`keys[1..]`) | `w(k0)` | `w(k_i)` | **Unnecessary when `!isCrossRequestWorker`** — see **91.3** |
| 5 | `BGroup.java:1430` | `BLMPOP` (`keys[1..]`) | `w(k0)` | `w(k_i)` | **Unnecessary when `!isCrossRequestWorker`** — see **91.3** |
| 6 | `BGroup.java:1813` | `BZPOPMIN`/`BZPOPMAX` (`keys[1..]`) | `w(k0)` | `w(k_i)` | **Unnecessary when `!isCrossRequestWorker`** — see **91.3** |
| 7 | `BGroup.java:1840` | `BZMPOP` (`keys[1..]`) | `w(k0)` | `w(k_i)` | **Unnecessary when `!isCrossRequestWorker`** — see **91.3** |
| 8 | `ZGroup.java:2838` | `ZMPOP` (`i = 0..numKeys-1`) | `w(k0)` | `w(k_i)` | Needed for foreign `k_i`; **unnecessary for `i = 0`** — see **91.4** |
| 9 | `DGroup.java:306` | `DEL` (cross-worker) | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 10 | `EGroup.java:207` | `EXISTS` (cross-worker) | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 11 | `MGroup.java:303` | `MGET` (cross-worker) | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 12 | `MGroup.java:468` | `MSETNX` phase 1 | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 13 | `MGroup.java:501` | `MSETNX` phase 2 | `w(k0)` (bounced back) | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 14 | `MGroup.java:669` | `MIGRATE` (cross-worker) | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 15 | `PGroup.java:500` | `PFCOUNT` (cross-worker) | `w(k0)` | `w(slot)` | Needed for foreign slots; `firstSlot`/same-worker inline — see **91.5** |
| 16 | `PGroup.java:570` | `PFMERGE` source reads | `w(dst)` | `w(srcSlot)` | **Needed** (`dst` already excluded; inline for same-worker sources) |
| 17 | `SGroup.java:1368` | `SDIFF`/`SINTER`/`SUNION` | `w(k0)` | `w(k_i)` | **Needed** (`k0` already read inline; guarded by `isCrossRequestWorker`) |
| 18 | `SGroup.java:1472` | `SDIFFSTORE`/`SINTERSTORE`/`SUNIONSTORE` | `w(dst)` | `w(src_i)` | **Needed** (`dst` excluded; guarded by `isCrossRequestWorker`) |
| 19 | `SGroup.java:1630` | `SINTERCARD` | `w(k0)` | `w(k_i)` | **Needed** (`k0` already read inline; guarded by `isCrossRequestWorker`) |
| 20 | `ZGroup.java:1462` | `ZDIFF`/`ZINTER`/`ZUNION`/`*STORE` | `w(src0)` | `w(src_i)` | **Needed** (`src0` already read inline; guarded by `isCrossRequestWorker`) |
| 21 | `ZGroup.java:1674` | `ZINTERCARD` | `w(k0)` | `w(k_i)` | **Needed** (`k0` already read inline; guarded by `isCrossRequestWorker`) |
| 22 | `RGroup.java:414` | `RENAME`/`RENAMENX` | `w(src)` | `w(dst)` | **Strictly needed** (2 keys + `isCrossRequestWorker` ⇒ `dst` always foreign) |
| 23 | `SGroup.java:2170` | `SMOVE` | `w(src)` | `w(dst)` | **Strictly needed** (2 keys + `isCrossRequestWorker` ⇒ `dst` always foreign) |
| 24 | `FGroup.java:122` | `FAILOVER` (all slots) | random worker | all `w(s)` | **Needed** when `slotWorkers > 1` |
| 25 | `KGroup.java:103` | `KEYS` (all slots) | random worker | all `w(s)` | **Needed** when `slotWorkers > 1` |
| 26 | `LocalPersist.java:282` | `doSthInSlots` (`DBSIZE`, `FLUSHDB`, `RANDOMKEY`, `INFO`, `INGEST`, `CLUSTER RESET`, `MANAGE DEBUG`) | random worker | all `w(s)` | **Needed** when `slotWorkers > 1` |
| 27 | `ManageCommand.groovy:1033` | `MANAGE DYN-CONFIG` (all slots) | random worker | all `w(s)` | **Needed** when `slotWorkers > 1` |
| 28 | `ClusterxCommand.groovy:561` | `CLUSTER[X] INFO` | random worker | `w(innerSlot)` | **Needed** when `slotWorkers > 1` |
| 29 | `XBigStrings.java:216` | `XBigStrings.applyAsync` (repl) | `w(streamSlot)` | `w(targetSlot)` | **Needed** in 2N scale-up when `w(targetSlot) != w(streamSlot)` (and `asyncCall` bounce keeps `replPair.addToFetchBigStringId` on `w(streamSlot)`) |
| 30 | `LocalPersist.java:915` | `cleanUpAsync` (`onStop`) | primary shutdown thread | all `w(s)` | **Strictly needed** (caller is never a slot worker, even with `slotWorkers == 1`) |

- Note on `OneSlotTest.groovy` (`39`, `55`, `723`, `764`) and `LocalPersistTest.groovy` (`199`): direct unit tests of `OneSlot.asyncCall` itself; `OneSlotTest.groovy:41, 749, 759, 768` call `eventloopCurrent.run()` after inline calls (`threadId == threadIdProtectedForSafe`) where the loop exits immediately (harmless).

---

## Reviewer checklist (for AI agent 2)

- **Finding 91.1 (`AGroup` binlog `asyncCall`)**:
  1. Verify `AGroup.parseSlots("acl", ["acl", "save"], 4)` and `AGroup.parseSlots("acl", ["acl", "load"], 4)` both return an empty list (`[]`), while `setuser`/`deluser` return `[TO_FIX_FIRST_SLOT]` (`slot = 0`).
  2. Construct a 2-thread Spock spec (background `eventloop0` owning slot 0 with `binlogOn = true`, current test thread bound to `eventloopCurrent` acting as `slot-worker-1` where `threadId != oneSlot0.threadIdProtectedForSafe`). Execute `aGroup.execute("acl save")` (or inject an `IOException` on `binlog.append` / slow down `eventloop0` with a latch): observe `appendAclUpdateBinlog()` returns immediately with `p.getResult() == null` and `p.getException() == null`, returning `OKReply.INSTANCE` even when `append` throws `IOException` or has not yet executed.
  3. Verify `ConfForGlobal.clusterEnabled = true` with a shard whose `firstOneSlot()` has `slot() == 1` (`slotWorkers == 2`): `ACL SETUSER` is routed to slot 0 (worker 0) while `firstOneSlot()` is owned by worker 1 → triggers the same broken synchronous `p.getResult()`.
- **Finding 91.2 (`PGroup.pfmerge` `dstOneSlot.asyncCall`)**:
  1. Trace `MultiWorkerServer.handleRequest` for `PFMERGE dst src` with `slotWorkers = 2`, `dst` on slot 0 (worker 0), `src` on slot 1 (worker 1).
  2. Run a Spock test with `eventloopDst` on `.withCurrentThread()` (matching production routing) and `eventloopSrc` on a background thread; record `Thread.currentThread().threadId()` inside `Promises.all(promises).whenComplete(...)` (line 577) after `eventloopDst.run()` and assert it equals `dstOneSlot.threadIdProtectedForSafe`.
  3. Confirm `PGroupTest.groovy:534-586` invokes `crossGroup.execute("pfmerge $dstKey $srcKey")` from `eventloopSrc` (not `eventloopDst`).
- **Finding 91.3 (`BGroup` missing `isCrossRequestWorker` guard)**:
  1. Static check: confirm `isCrossRequestWorker` is never referenced in `blpop`, `blmpop`, `bzpop`, or `bzmpop`.
  2. Spock spec with `slotWorkers = 1` (so `isCrossRequestWorker == false`), `k1` empty, `k2` containing `"v"`: execute `blpop k1 k2 1` and `bzpopmin k1 k2 1` and verify they return `AsyncReply` (not a synchronous `MultiBulkReply`) and invoke `localPersist.oneSlot(...).asyncCall(...)`.
  3. Verify in a 2-worker reactor test that `popOtherKeyAsync(...).whenComplete(...)` runs on `keys[0]`'s reactor (contradicting comments at `BGroup.java:1086-1087`, `1589-1590`).
- **Finding 91.4 (`ZGroup.zmpop` `i = 0` in cross-worker fan-out)**:
  1. Spock spec with `crossRequestWorker = true`, `k0` non-empty (or wrong-type), `k1` on another slot: execute `zmpop 2 k0 k1 MIN` and observe `k1`'s `OneSlot.asyncCall` is still invoked and `zmpop` returns `AsyncReply` instead of an immediate synchronous reply.
- **Finding 91.5 (`groupBySlot` includes `firstSlot`)**:
  1. Static inspection + optional 2-worker Spock trace confirming `firstSlot` enters `oneSlot.asyncCall(...)` (and hits the inline branch `OneSlot.java:739`) in `DGroup.del`, `EGroup.exists`, `MGroup.mget`, `MGroup.msetnx` (both phases), `MGroup.migrate`, and `PGroup.pfcount`.

---

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 91.1 | `AGroup.appendAclUpdateBinlog()` calls `firstOneSlot.asyncCall()` + sync `p.getResult()` — redundant on worker 0 (`SETUSER`/`DELUSER` standalone), broken on cross-thread paths (`ACL SAVE`, `ACL LOAD`, cluster mode) | High | Concurrency / Correctness | `CONFIRMED (reproduced)` | fixed (`e56ed7a8`) |
| 91.2 | `PGroup.pfmerge()` wraps dst merge/write in `dstOneSlot.asyncCall()` even though production always executes and bounces back onto `dstOneSlot`'s worker thread | Low | Concurrency / Redundant dispatch | `NOT A BUG` | no change needed |
| 91.3 | `BGroup` multi-key blocking pops (`BLPOP`/`BRPOP`, `BLMPOP`, `BZPOPMIN`/`BZPOPMAX`, `BZMPOP`) never check `isCrossRequestWorker`, unnecessarily using `asyncCall()` on same-worker commands | Low | Redis protocol compatibility / Documentation | `CONFIRMED (reproduced)` | fixed (`3fca457a`) |
| 91.4 | `ZGroup.zmpop()` includes `i = 0` (`firstSlot`, always owned by current thread) in its cross-worker `asyncCall()` fan-out | Low | Performance / Redundant dispatch | `CONFIRMED (inspection)` | fixed (`01b5b74a`) |
| 91.5 | Cross-worker `groupBySlot` loops (`DEL`, `EXISTS`, `MGET`, `MSETNX`, `MIGRATE`, `PFCOUNT`) submit `firstSlot` and same-worker slots through `asyncCall()` | Low | Performance / Redundant dispatch | `NOT A BUG` | no change needed |

## Review Feedback

**Reviewer:** AI agent 2  
**Verification date:** 2026-11-19  
**Verification baseline:** `a80f744fcffd606efdcce79ce1f5b2af852c08cb` (working tree; unrelated existing edits retained).  
**Scope:** Stage 2 verification only. No production fixes or commits in this pass. The original discovery text above is retained; the qualifications below supersede its broader claims and suggested removals.

### Evidence and baseline checks

Read the report, the cited command implementations and tests, `OneSlot.asyncCall/asyncExecute`,
`LocalPersist.bindOneSlotIfInitialized/firstOneSlot/getInnerOneSlotByToClientSlot`,
`Request.getSingleSlot`, `MultiWorkerServer.checkClusterSlot/handleRequest`, the threading and command
processing design documents, and related reviews 52, 84, 85, and 90 (including prior dispositions).
The current BGroup has changed since the discovery baseline: use the current references below.

Local ground truth:

- `/home/kerry/ws/redis/src/version.h:1`: `REDIS_VERSION "7.2.14"`.
- Redis `src/acl.c:2897-2914`: LOAD/SAVE answer after their local file operations succeed/fail.
  This does **not** establish a Redis requirement to replicate ACL changes; ACL binlog propagation here
  is Velo's own implementation contract.
- Redis `src/t_zset.c:3892-3910`: `genericZpopCommand` inspects keys in order and stops at the first
  existing candidate/type error. `zmpopGenericCommand` at 4400-4450 delegates to that path.
- Local ActiveJ `core-promise/.../Promise.java:197-222` captures the caller reactor and executes
  completion on it; `AbstractPromise.java:83-90` reads current result/exception without waiting.

Pasted implementation evidence shared by the verdicts:

```java
// OneSlot.java:737-748 (current checkout)
if (threadId == threadIdProtectedForSafe) {
    try {
        return Promise.of(supplierEx.get());
    } catch (Exception e) {
        return Promise.ofException(e);
    }
}
if (Reactor.getCurrentReactorOrNull() != null) {
    return Promise.ofFuture(slotWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
}
// ActiveJ Promise.java:208-215 (local source, elided)
Reactor reactor = getCurrentReactor();
reactor.startExternalTask();
completionStage.whenCompleteAsync((result, throwable) -> {
    reactor.execute(() -> { /* cb.set(result, null) or cb.set(null, e) */ });
});
```

The inline branch runs the supplier synchronously; a call to `asyncCall` is not evidence of a thread
handoff. The remote reactor branch returns an initially incomplete promise even if the supplier finishes
quickly, because completion still needs to run on the calling reactor.

### 91.1 — CONFIRMED (reproduced), High

A temporary Spock spec called the **actual** `AGroup.execute("acl save")` with slot 0 owned by another
thread, binlog enabled, and a caller eventloop bound to the test thread. The target eventloop was not
started until after the command returned. A real `Binlog` subclass threw an injected `IOException` in
`append`; the test then started the target and drained the calling reactor. Assertions checked that
SAVE returned OK with zero append attempts and still remained OK after the failing append executed.

Pasted reproduction output:

```text
BUG91.1 before target runs: saveSlots=[], loadSlots=[], reply=OK, appendAttempts=0
java.lang.RuntimeException: Append binlog error, acl line=acl save
Caused by: java.io.IOException: bug91 injected append failure
BUG91.1 after target runs: injected IOException, appendAttempts=1, original reply remains OK
```

The target exception is logged by ActiveJ, so “silently swallows” should mean **not reported to the
command/client**, not necessarily absent from server logs. The test directly reproduced SAVE and both
empty parseSlots lists. LOAD's discarded promise and the cluster worker mismatch were verified by
inspection, not by a live cluster reproduction.

Additional qualifications:

- `AGroup.java:317-324` discards LOAD's promise even when execution is inline; `asyncCall` converts
  the thrown append exception into `Promise.ofException`, so LOAD ignores it on that path too.
- “Every ACL SAVE/LOAD in cluster mode” is too broad: their random worker can equal the binlog
  slot's worker. SETUSER/DELUSER differ whenever pinned worker 0 does not own `firstOneSlot`.
- The successful standalone SETUSER/DELUSER inline wrapper is not itself a defect.
- Awaiting the append cannot roll back an ACL snapshot or file change already applied. The fix must
  define its error behavior and test it; it should not claim transactionality.
- Routing to the actual first owned slot can work, but parsing runs on a net worker and shard ownership
  can change. Reading a published reference alone does not prove that routing remains valid until
  execution. An AsyncReply chained to the owner task preserves the existing routing contract.

### 91.2 — NOT A BUG; no change needed

The production destination wrapper is inline, as discovery states. Its defensive role is already
explicit in the implementation and supported by the bug 84.1 regression. No incorrect result, thread
handoff, or measured performance regression is demonstrated by the extra wrapper.

Pasted current evidence (`PGroup.java:596-604`):

```java
// On the reactor path (production: handle() runs on the dst slot worker and
// Promise.ofFuture bounces every source promise completion back to that reactor),
// this callback already runs on the dst worker thread and asyncCall executes inline.
// On the non-reactor fallback path (no bounce, e.g. tests calling execute() directly),
// this callback runs on a source slot worker thread, so asyncCall submits the merge
// to the dst slot eventloop instead — see bug 84.1.
var dstOneSlot = localPersist.oneSlot(dstS.slot());
dstOneSlot.asyncCall(() -> {
```

`PGroupTest.groovy:534-586` intentionally invokes the merge from the source reactor with destination
on another eventloop. That tests a defensive calling pattern rather than the production dispatcher.
It does not invalidate the test or make its protective wrapper a bug. Removing the wrapper would
regress that supported test arrangement unless its contract were deliberately changed. This is
consistent with 85.2's accepted production-bounce reasoning, without reopening that closed finding.

### 91.3 — REFINED; severity Medium → Low

Confirmed by inspection: all four methods omit an `isCrossRequestWorker` branch, and BLPOP/BZPOP
can return an already-settled AsyncReply on an immediate secondary-key hit. The comments saying the
continuation runs on the last key's worker are incorrect on the production reactor path.

Pasted current source evidence:

```text
$ rg -n 'isCrossRequestWorker|worker of the last checked' src/main/java/io/velo/command/BGroup.java
1087:                // the continuation below runs on the worker of the last checked key, which is also the
1616:            // the continuation below runs on the worker of the last checked key, which is also the
```

```java
// BGroup.java:1425-1432 (BLMPOP); equivalent at 1810-1817 (BZMPOP)
if (finalPromise.isComplete()) {
    var exception = finalPromise.getException();
    if (exception != null) {
        return exception instanceof TypeMismatchException ? ErrorReply.WRONG_TYPE :
                new ErrorReply(exception.getMessage());
    }
    // all the keys are owned by this worker, the reply is already known
    return finalPromise.getResult();
}
```

Refinement: same-owner `asyncCall` **and** `asyncExecute` execute inline. Consequently the scan,
registration, and recheck do not yield to another eventloop task on the same worker; there is no
proven same-worker race or network-protocol difference from this structure. BLMPOP/BZMPOP already
unwrap settled results. Additional local branches would reduce some wrappers but duplicate substantial
blocking logic. Any larger rewrite needs allocation/latency evidence. Correct the two comments first;
consider BLPOP/BZPOP unwrapping as a small optional improvement. No concurrency fix is established.

### 91.4 — CONFIRMED (inspection), Low

Unlike the local wrappers in 91.2/91.5, this path performs actual **foreign-worker reads and waits**
that can be avoided when key 0 already determines the answer.

Pasted current code (`ZGroup.java:2834-2866`, elided):

```java
for (int i = 0; i < numKeys; i++) {
    var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
    var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());
    var p = oneSlot.asyncCall(() -> { /* getCv, getValueBytesByCv, RedisZSet.decode */ });
    rzPromises.add(p);
}
Promises.all(rzPromises).whenComplete((r, e) -> {
    if (e != null) { /* settle exception before choosing a key */ }
    // only after all reads: choose first candidate in argument order
});
```

Redis 7.2.14 source (`src/t_zset.c:3903-3910`):

```c
while (idx < keyc) {
    key = keyv[idx++];
    zobj = lookupKeyWrite(c->db,key);
    if (!zobj) continue;
    if (checkType(c,zobj,OBJ_ZSET)) return;
    break;
}
```

A busy foreign worker delays Velo even when its key cannot be selected. The claim about key 0 is
confirmed; its `asyncCall` itself is inline and inexpensive. The actionable part is avoiding the
foreign fan-out/barrier, not merely removing a local wrapper. Keep Low; no benchmark or live latency
measurement was performed. Checking key 0 first is a bounded improvement; it does not resolve the
broader cross-worker atomicity limitations or the remaining speculative reads after an empty key 0.

### 91.5 — NOT A BUG; no change needed

Static inspection confirms the uniform group loops include the first slot and any other local slots:
`DGroup.java:298-315`, `EGroup.java:199-216`, `MGroup.java:296-318,461-477,495-516,662-690`,
`PGroup.java:493-502`. The owner check in `OneSlot` already implements the requested dispatch fast path.

Pasted representative source (`DGroup.java:300-315`, elided):

```java
var groupBySlot = slotWithKeyHashListParsed.stream().collect(Collectors.groupingBy(SlotWithKeyHash::slot));
for (var entry : groupBySlot.entrySet()) {
    var oneSlot = localPersist.oneSlot(entry.getKey());
    var p = oneSlot.asyncCall(() -> { /* remove each key in this slot group */ });
    promises.add(p);
}
```

Combined with the pasted `OneSlot` inline branch, this proves local groups execute directly and preserve
exception aggregation. There is no redundant eventloop submission. Lambda/promise overhead is a
possible optimization subject, not a demonstrated bug; allocations may also be optimized by the JVM.
Separating local accumulation from remote completion adds paths and exception-handling obligations
without measured benefit. Preserve the uniform idiom.

### Recommended fix order

1. **91.1:** add a permanent regression from the temporary reproduction, including SAVE/LOAD append
   failure, then chain completion/error propagation correctly. Verify standalone and cluster routing
   separately. Follow one-bug TDD/JaCoCo/commit workflow when fixes are requested.
2. **91.4:** check key 0 before remote reads. Add a regression asserting that a decisive first key
   does not access a foreign slot, then verify reply format and wrong-type ordering.
3. **91.3:** correct the misleading comments; evaluate optional immediate-reply unwrapping or broader
   fast paths only against measured cost. No Medium concurrency finding remains.
4. **91.2 / 91.5:** no change needed.

### Verification results and limits

Evidence log: [temporary spec source and full final test output](logs/20261119_183500_oneslot_async_call_review_agent2_bug_91.log).
The temporary source was removed from the test tree after the run; it remains in the log for reproduction.
The first harness attempt used `Mock(Binlog)` and failed because this project has no class-mocking
provider. It was replaced by a concrete subclass without adding dependencies; the final run below passed.

```bash
./gradlew :test \
  --tests 'io.velo.persist.Bug91ReviewTempTest' \
  --tests 'io.velo.command.AGroupTest' \
  --tests 'io.velo.command.PGroupTest.test pfcount and pfmerge' \
  --tests 'io.velo.command.BGroupTest.test blpop' \
  --tests 'io.velo.command.BGroupTest.test blmpop' \
  --tests 'io.velo.command.BGroupTest.test bzpopmin' \
  --tests 'io.velo.command.BGroupTest.test bzpopmax' \
  --tests 'io.velo.command.BGroupTest.test bzmpop' \
  --tests 'io.velo.command.ZGroupPart2Test.test zmpop'
```

```text
io.velo.command.AGroupTest: tests=13 failures=0 errors=0
io.velo.command.BGroupTest: tests=5 failures=0 errors=0
io.velo.command.PGroupTest: tests=1 failures=0 errors=0
io.velo.command.ZGroupPart2Test: tests=1 failures=0 errors=0
io.velo.persist.Bug91ReviewTempTest: tests=1 failures=0 errors=0
> Task :jacocoTestReport
BUILD SUCCESSFUL in 31s
```

Inspected the JaCoCo source HTML through `scripts/jacoco_cover.py` (not the aggregate Total row):

| Class / range | Fully covered lines | Partially covered lines | Uncovered lines | Interpretation |
|---|---:|---:|---:|---|
| AGroup 499-519 | 11 | 2 | 1 | append, injected IOException catch, immediate result/exception reads, and success log executed; synchronous exception rethrow at 515 not exercised |
| OneSlot 737-760 | 6 | 1 | 8 | both owner outcomes and reactor path exercised; non-reactor fallback not covered by this selection |
| PGroup 596-632 | 19 | 1 | 4 | destination wrapper/merge exercised; verdict also relies on source routing and prior review |
| BGroup 1075-1118 | 26 | 0 | 0 | BLPOP secondary-key/registration path exercised |
| ZGroup 2834-2866 | 18 | 2 | 4 | cross-worker fan-out and barrier exercised; later-key decode/worker exceptions not injected |

The tests assert existing behavior, including the reproduced defect; green does not mean 91.1 is fixed.
No live Redis differential, cluster reproduction, or performance benchmark was run. No production or
permanent test source changed, so this pass supplies verification evidence rather than fix coverage.

---

## Response (AI agent 1, round 1)

**Responded at:** commit `a80f744fcffd606efdcce79ce1f5b2af852c08cb` (same baseline as agent 2's review).
**Method:** re-read every reviewer claim and qualification against the current codebase (`AGroup.java`, `BGroup.java`, `PGroup.java`, `ZGroup.java`, `MultiWorkerServer.java`, `RequestHandler.java`, `BaseCommand.java`, `ErrorReply.java`, `XAclUpdate.java`) and local ActiveJ source (`Promise.java:84-86`).

| Finding | Response | Note |
|---|---|---|
| **91.1** | **AGREE (with all 5 qualifications; fix direction updated to option (b))** | Verified every qualification in the codebase: (1) `OneSlot.asyncCall` wraps inline exceptions in `Promise.ofException(e)` (`OneSlot.java:742-743`), which constructs `new CompleteExceptionallyPromise<>(e)` with no logging (`Promise.java:84-86`), so `ACL LOAD` (`AGroup.java:317-324`) drops `IOException` even on the inline path (never reaching `catch (Exception e)` at `AGroup.java:328`); (2) random-worker `ACL SAVE`/`LOAD` in cluster mode can coincidentally land on `firstOneSlot`'s worker, so "every" was overbroad — only pinned `SETUSER`/`DELUSER` deterministically miss whenever `firstOneSlot().slot() % slotWorkers != 0`; (3) agreed that on the cross-thread path the exception is dropped from the client reply rather than necessarily absent from server logs; (4) agreed that awaiting append does not make ACL file/snapshot mutations transactional; (5) **agreed that `AsyncReply` (option (b)) is safer than `parseSlots` pinning (option (a))**, because in cluster mode `MultiShard.firstToClientSlot()` can change across migrations between net-worker `parseSlots` and slot-worker `handle()`. Implementation constraint for the fix phase: `XAclUpdate.apply()` (`XAclUpdate.java:126-134`) calls `aGroup.execute(line)` synchronously and checks `reply instanceof ErrorReply`, so when `firstOneSlot.asyncCall(...)` completes inline on the owning worker, unwrapping the completed promise into a synchronous `Reply` (returning `AsyncReply` only when pending) preserves `XAclUpdate.apply()` compatibility. |
| **91.2** | **AGREE (`NOT A BUG`, no change needed)** | The technical fact stands — in real production data flow `PGroup.pfmerge()` (`PGroup.java:604`) is always on `dstOneSlot`'s owning thread and executes inline — and keeping it as an explicit, documented defensive inline guard that supports `PGroupTest.groovy:534-586` (from accepted bug 84.1) is not a bug. No code change needed. |
| **91.3** | **PARTIALLY DISAGREE (agree on downgrading concurrency/allocation claims to Low; DISAGREE with *"no network-protocol difference"*)** | Agreed that same-owner `asyncCall` and `asyncExecute` execute inline with no same-worker race, that `BGroup.java:1087-1088` and `1616-1617` comments are wrong on the reactor path, and that duplicating all blocking registration logic solely for micro-allocations is unwarranted without benchmarks. **However, re-checking `BGroup.java` + `MultiWorkerServer.java` reveals a real wire-level `WRONGTYPE` protocol bug directly caused by routing `BLPOP`/`BRPOP` (even on a single worker, `!isCrossRequestWorker`) and cross-worker `BLMPOP` through `asyncCall` + `AsyncReply`** — see code citations below. |
| **91.4** | **AGREE (`CONFIRMED (inspection)`, Low)** | Agent 2's sharpening is even stronger than discovery: besides `i = 0` being local, `Promises.all(rzPromises).whenComplete(...)` (`ZGroup.java:2861-2866`) checks `if (e != null)` before inspecting `rzResults[0]` (`ZGroup.java:2874`), so a foreign worker error or slow foreign worker delays or fails `ZMPOP` even when key 0 already holds a valid non-empty zset or wrong-type key (unlike Redis `src/t_zset.c:3903-3910`). Checking key 0 inline first before cross-worker fan-out fixes both. |
| **91.5** | **AGREE (`NOT A BUG`, no change needed)** | Confirmed: `OneSlot.asyncCall` already fast-paths `threadId == threadIdProtectedForSafe` (`OneSlot.java:739-745`) with zero eventloop submission. Splitting local vs. remote accumulation across 6 commands adds branching and exception-handling complexity without measured latency/allocation benefit. Preserve the uniform `groupBySlot` idiom. |

### Disputed point on 91.3 — Code citations for the wire-level `WRONGTYPE` protocol difference

Agent 2's refinement states (line 601-602): *"there is no proven same-worker race or network-protocol difference from this structure."* Re-checking the current tree (`a80f744f`) shows a concrete RESP protocol bug on `BLPOP`/`BRPOP` (even when `slotWorkers == 1` / `!isCrossRequestWorker`) and on cross-worker `BLMPOP`, directly caused by going through `OneSlot.asyncCall` without mapping `TypeMismatchException` inside `whenComplete`:

1. **Key 0 path (synchronous — returns Redis `WRONGTYPE`)**:
   - `BGroup.blpop()` (`BGroup.java:1076`) calls `LGroup.getRedisList(firstSlotWithKeyHash, this)` directly.
   - On a non-list key (e.g. a string), `BaseCommand.get(...)` (`BaseCommand.java:938-940`) throws `new TypeMismatchException("Expect sp type array=..., but got sp type=...")`.
   - `RequestHandler.handle(...)` (`RequestHandler.java:572-575`) catches `TypeMismatchException` and returns `ErrorReply.WRONG_TYPE` (`ErrorReply.java:98-99`: `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`).
2. **Key `1..n-1` path in `BLPOP`/`BRPOP` (ALWAYS goes through `asyncCall` + returns `AsyncReply`, even on 1 worker!)**:
   ```java
   // src/main/java/io/velo/command/BGroup.java:1089-1094, 1119 (current HEAD a80f744f)
   popOtherKeyAsync(1, keys, isLeft).whenComplete((r, e) -> {
       if (e != null) {
           log.error("{} error={}", isLeft ? "blpop" : "brpop", e.getMessage());
           finalPromise.setException(Utils.wrapException(isLeft ? "blpop" : "brpop", e));
           return;
       }
       ...
   });
   return asyncReply;
   ```
   - `OneSlot.asyncCall` (`OneSlot.java:742-743`) catches `TypeMismatchException` and returns `Promise.ofException(e)`.
   - `BGroup.java:1092` places `TypeMismatchException` into `finalPromise.setException(...)` (whereas `bzpop` at `BGroup.java:1621-1623` and `bzmpop` at `BGroup.java:1775-1777` were fixed in bug 90.4 `1f380ffd` to call `finalPromise.set(ErrorReply.WRONG_TYPE)`).
   - Because `blpop()` never unwraps `finalPromise` (`BGroup.java:1119`), `MultiWorkerServer.transferAsyncReply(...)` (`MultiWorkerServer.java:698-700`) handles the failed promise:
     ```java
     // src/main/java/io/velo/MultiWorkerServer.java:697-701
     return promise.map((r, e) -> {
         if (e != null) {
             var errorReply = new ErrorReply(e.getMessage());
             return request.isHttp() ? wrapHttpResponse(errorReply) : errorReply.buffer();
         }
     ```
   - `transferAsyncReply` does **not** map `TypeMismatchException` to `ErrorReply.WRONG_TYPE`; it emits `-ERR Expect sp type array=..., but got sp type=...` on the wire — even on a **single worker** (`slotWorkers == 1`)!
3. **Same bug on cross-worker `BLMPOP`**:
   - `BGroup.blmpop()` (`BGroup.java:1394-1396`) also calls `finalPromise.setException(Utils.wrapException("blmpop", e))` without mapping `TypeMismatchException`. On a single worker, the synchronous unwrap at `BGroup.java:1425-1430` hides this by converting `TypeMismatchException` → `ErrorReply.WRONG_TYPE`; on a cross-worker `BLMPOP` (`finalPromise.isComplete() == false`), `transferAsyncReply` emits `-ERR Expect sp type array=...` instead of `-WRONGTYPE`.

**Updated fix scope for 91.3 (minimal, no bloated blocking-logic duplication):**
1. Map `TypeMismatchException` → `finalPromise.set(ErrorReply.WRONG_TYPE)` inside `whenComplete` for both `blpop`/`brpop` (`BGroup.java:1090-1093`) and `blmpop` (`BGroup.java:1394-1396`), matching `bzpop` (`1621-1623`) and `bzmpop` (`1775-1777`).
2. Unwrap already-completed `finalPromise` in `blpop`/`brpop` (`1119`) and `bzpop` (`1653`), matching `blmpop` (`1425-1433`) and `bzmpop` (`1810-1818`), so same-worker secondary-key hits and errors return synchronous replies.
3. Fix the two inaccurate continuation-thread comments (`BGroup.java:1087-1088` and `1616-1617`).

### Consensus Fix Plan (ready for Stage 4 when requested)

1. **91.1 (High)**: Fix `AGroup` ACL binlog append (`appendAclUpdateBinlog` + `ACL LOAD`) by awaiting `firstOneSlot.asyncCall(...)` via `AsyncReply` when pending (unwrapping inline completion so `XAclUpdate.apply` stays synchronous on `firstOneSlot`'s worker), ensuring `ACL SAVE`/`ACL LOAD`/cluster binlog failures propagate to the client reply instead of returning premature `OK`.
2. **91.3 (Low — Redis protocol + docs)**: Map `TypeMismatchException` → `ErrorReply.WRONG_TYPE` in `blpop`/`brpop`/`blmpop` `whenComplete`, unwrap completed `finalPromise` in `blpop`/`bzpop`, and correct the two inaccurate continuation-thread comments.
3. **91.4 (Low)**: Check key 0 inline in `ZGroup.zmpop()` before the cross-worker fan-out so a decisive key 0 neither waits on nor fails from foreign workers.
4. **91.2 / 91.5 (`NOT A BUG`)**: No code change.


## Review Feedback follow-up — 91.3 dispute resolved (AI agent 2)

**Date:** 2026-11-19  
**Baseline:** `a80f744fcffd606efdcce79ce1f5b2af852c08cb`  
**Verdict:** `CONFIRMED (reproduced)` for agent 1's revised protocol scope; **Low**.

Agent 1 is correct about the missing WRONGTYPE mapping. I retract the earlier statement that this
structure has no network-protocol difference. The earlier 91.3 refinement and fix-order paragraph
remain as review history; this follow-up supersedes their protocol conclusion and fix priority.

A temporary Spock spec used a real `BGroup`, `InMemoryGetSet`, and slot 0 owned by the calling thread,
with `crossRequestWorker=false`. The first key was absent and the second held a string. It executed
both commands and passed each resulting AsyncReply through the actual private
`MultiWorkerServer.transferAsyncReply` method via reflection. It asserted the retained exception type
and the serialized RESP bytes. This exercises command execution and the actual reply encoder;
it is not a network socket test.

```text
$ ./gradlew :test --tests 'io.velo.command.Bug913ReviewTempTest'
blpop crossRequestWorker=false wire=-ERR Expect sp type array=[-2048], but got sp type=0
brpop crossRequestWorker=false wire=-ERR Expect sp type array=[-2048], but got sp type=0
io.velo.command.Bug913ReviewTempTest tests=1 failures=0 errors=0
BUILD SUCCESSFUL in 9s
```

Expected RESP error: `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`.
Redis 7.2.14 `src/t_list.c:1212-1229` checks each existing candidate with
`checkType(c, o, OBJ_LIST)`; Velo's corresponding constant is `ErrorReply.WRONG_TYPE`
(`ErrorReply.java:98-99`). No live Redis differential was needed for this source-backed comparison.

The verified error path is:

```java
// OneSlot.java:742-743: an inline supplier failure becomes a failed promise
} catch (Exception e) {
    return Promise.ofException(e);
}
// BGroup.java:1089-1093: BLPOP/BRPOP preserve that failure as an exception
if (e != null) {
    log.error("{} error={}", isLeft ? "blpop" : "brpop", e.getMessage());
    finalPromise.setException(Utils.wrapException(isLeft ? "blpop" : "brpop", e));
    return;
}
// MultiWorkerServer.java:698-700: async failures become generic ERR replies
if (e != null) {
    var errorReply = new ErrorReply(e.getMessage());
    return request.isHttp() ? wrapHttpResponse(errorReply) : errorReply.buffer();
}
```

`Utils.wrapException` (`Utils.java:124-129`) preserves an exception with a non-null message,
so the TypeMismatchException is retained. The synchronous RequestHandler catch
(`RequestHandler.java:572-575`) never sees it. This explains why even an inline call has the wrong
wire result: avoiding a thread handoff does not preserve synchronous exception handling.

The cross-worker BLMPOP claim is also accepted **by inspection**: its callback at
`BGroup.java:1393-1396` sets the same exceptional promise. The settled-result conversion at
1425-1432 only helps when completion happens before the method returns; a genuinely pending remote
read returns AsyncReply and reaches the same generic error encoder. This follow-up did not execute
a separate two-worker BLMPOP reproduction.

Inspected the generated JaCoCo source HTML with `scripts/jacoco_cover.py`:

```text
BGroup 1089-1094: 4 covered / 1 partial / 0 uncovered lines
  1092 finalPromise.setException(...): both BLPOP/BRPOP branches covered
MultiWorkerServer 695-702: 3 covered / 2 partial / 0 uncovered lines
  699 new ErrorReply(e.getMessage()): covered; RESP error buffer branch executed
OneSlot 737-744: 3 covered / 1 partial / 1 uncovered lines
  742-743 inline exception capture and Promise.ofException: covered
```

[Evidence log, including the temporary spec source](logs/20261119_184900_bug_91_3_wrongtype_agent2.log).
The temporary source was removed after verification. Two initial harness runs failed on Groovy property
resolution (`exception`/`result` selected boolean predicates); the final source uses explicit
`getException()`/`getResult()`. No production changes or dependency changes were made.

### Revised recommended fix order

1. **91.1:** ACL binlog completion/error handling, as already agreed.
2. **91.3:** map TypeMismatchException to `finalPromise.set(ErrorReply.WRONG_TYPE)` in BLPOP/BRPOP
   and BLMPOP callbacks, matching the sorted-set blocking commands. Add permanent regressions for
   the same-worker list cases and genuinely pending cross-worker BLMPOP, checking encoded error prefixes.
   Correct the misleading continuation-thread comments. Unwrapping settled BLPOP/BZPOP replies is an
   optional local optimization; it cannot replace the error mapping needed for remote reads.
3. **91.4:** decisive first-key short circuit, as already agreed.
4. **91.2 / 91.5:** no change needed.

The root cause of the revised 91.3 bug is **missing typed error conversion across the promise boundary**,
not the absence of an `isCrossRequestWorker` guard. A separate synchronous scan/registration implementation
is unnecessary to fix it. Agent 1's proposed error mapping is accepted; the allocation-only claims remain Low
and do not justify a broader blocking-logic rewrite.


## Fix Log — Stage 4

Implementation follows the revised agreed scope. Each production fix is committed separately; no
changes to 91.2/91.5. Existing unrelated working-tree edits are retained.

| Bug | Failing-test evidence | Commit | Focused green verification | JaCoCo confirmation |
|---|---|---|---|---|
| 91.1 | `AGroupTest.test acl`: expected pending `AsyncReply`, got `OKReply` before the target eventloop started | `e56ed7a8` — `fix: await acl binlog appends` | `AGroupTest` + `XAclUpdateTest`, BUILD SUCCESSFUL | AGroup 491-513: append, error mapping, success settlement and both inline/pending return branches executed; only the no-slot outcome of the existing binlog guard remains partial |
| 91.3 | `BGroupTest.test blpop`: exceptional TypeMismatchException instead of normal WRONGTYPE reply; `test blmpop`: actual encoded remote reply was `-ERR Expect sp type array=...` | `3fca457a` — `fix: preserve blocking list wrong type replies` | Entire `BGroupTest`: 50 tests, 0 failures; BUILD SUCCESSFUL | BGroup 1089-1101 and 1397-1409: all executable lines and branches covered, including typed mapping and generic errors |
| 91.4 | `ZGroupPart2Test.test zmpop`: a decisive local key returned AsyncReply and read the deliberately failing foreign slot, instead of a synchronous MultiBulkReply with zero foreign reads | `01b5b74a` — `fix: check first zmpop key before remote reads` | `ZGroupPart2Test` + `ZGroupTest`: 31 tests, 0 failures; BUILD SUCCESSFUL | ZGroup 2802-2829: all new loop/routing decisions covered; remote fan-out and result selection now start at index 1 and were executed, including secondary WRONGTYPE/empty results |

### 91.1 implementation notes

The append helper now returns a Reply and settles its result after the owner task completes. SAVE,
LOAD, SETUSER, and DELUSER return that result; DELUSER preserves its integer count. Inline completion
returns a synchronous Reply, retaining compatibility with `XAclUpdate.apply`. The regression in the
existing `test acl` method checks all four commands against queued remote appends, remote failures,
inline success/failure, and disabled binlog. ACL snapshot/file changes still are not transactional.

Initial test authoring hit a duplicate local variable name; corrected before observing the intended RED.
An initial GREEN compilation hit the Groovy compiler heap limit. The successful verification used
`./gradlew -Dorg.gradle.jvmargs=-Xmx3g :test --tests 'io.velo.command.AGroupTest' --tests 'io.velo.repl.incremental.XAclUpdateTest'`.
No build configuration changes were made. Whole-class AGroup line coverage exceeded 80% (298/307 lines
executed in the inspected range 1-550).

### 91.3 implementation notes

BLPOP/BRPOP and BLMPOP now settle their promise with `ErrorReply.WRONG_TYPE` when the secondary-key
read throws TypeMismatchException, matching BZPOP/BZMPOP. Generic exceptions remain exceptions. The
tests assert actual RESP bytes through `MultiWorkerServer.transferAsyncReply`; the remote BLMPOP case
runs on two worker eventloops and verifies its promise is initially pending. Corrupt-list tests exercise
the generic-error fallback. Corrected both continuation-thread comments; optional synchronous-reply
unwrapping was omitted to keep the protocol fix minimal. BGroup coverage exceeded 80% in the inspected
1-2100 range (996/1007 lines executed in the first green run).

Per user direction, the ACL regression's direct owner-field assignments were replaced with
`LocalPersist.fixSlotThreadId` in follow-up commit `7f0cf850` (`test: use slot thread binding helper`).
Re-ran `AGroupTest` + `XAclUpdateTest` successfully and rechecked AGroup 487-514 coverage afterward.

### 91.4 implementation notes

Reused the existing local scan/pop logic: the local scan covers all keys for same-worker requests and
only key 0 for cross-worker requests. A decisive key 0 returns immediately. Otherwise remote reads and
result selection begin at index 1. No duplicated pop implementation or new production API.

The permanent regression uses a local owner bound through `fixSlotThreadId` and a foreign slot on a
background eventloop. Its store throws if the foreign slot is read. MIN COUNT, MAX, and first-key
WRONGTYPE return synchronously with zero foreign reads; assertions check member order, remaining
cardinality, and deletion after the final pop. Missing/empty key 0 still reaches secondary keys,
including secondary WRONGTYPE and empty-zset results. Updated old tests that expected AsyncReply for
a decisive first key and added cleanup for their background eventloop.

Initial test compilation exposed Groovy closure-parameter shadowing after adding cleanup; renamed
the closure parameter before the expected RED. Whole-class ZGroup line coverage is 1648/1686
(97.7% executed; 1605 fully covered, 43 partially covered lines). ZMPOP 2799-2912 has 64 fully covered,
3 partial, and 4 uncovered lines; uncovered paths are existing null-encoded-value and remote-error
fallbacks, not the new routing/short-circuit decisions.

### Stage 4 evidence and remaining verification

[RED/GREEN outputs and JaCoCo source-line evidence](logs/20261119_bug_91_stage4.log).

Commands used:

```bash
./gradlew -Dorg.gradle.jvmargs=-Xmx3g :test --tests 'io.velo.command.AGroupTest' --tests 'io.velo.repl.incremental.XAclUpdateTest'
./gradlew -Dorg.gradle.jvmargs=-Xmx3g -I /tmp/velo-bug91-gradle.init.gradle :test --tests 'io.velo.command.BGroupTest'
./gradlew -Dorg.gradle.jvmargs=-Xmx3g -I /tmp/velo-bug91-gradle.init.gradle :test --tests 'io.velo.command.ZGroupPart2Test' --tests 'io.velo.command.ZGroupTest'
```

The temporary init script increases the Groovy compilation worker heap to 2g; repository build
configuration is unchanged. Inspection confirms all changed classes exceed 80% line coverage in their
respective relevant test runs. Coverage artifacts from each run are preserved in the evidence log
because later Gradle runs replace the current report.

These are Stage 4 fixes, not a claim that Stage 5 has completed. A live multi-process cluster/Redis
differential was not run; ACL remote-owner paths are covered with deterministic two-thread tests.
Cross-worker multi-key atomicity and rollback of previously applied ACL state are outside these fixes.

### Independent implementation review

A separate reviewer examined `a80f744f..01b5b74a` read-only and found **no blocking issues**:

- ACL dispatch captures the update, target slot, and success reply before asynchronous execution;
  pending callbacks do not read later reused command data.
- Typed list errors become `ErrorReply.WRONG_TYPE`; generic error handling is preserved.
- ZMPOP consistently starts remote reads and result selection at index 1 after the local first-key check;
  tests verify skipped foreign reads and persistence mutation results.

Nonblocking follow-ups: the ACL test uses remote ownership of slot 0 and does not configure a cluster
shard whose first owned inner slot is nonzero. This remains a cluster integration-test limitation.
The reviewer also spotted blank lines breaking the Fix Log Markdown table; corrected them before the
documentation commit. The reviewer did not run Gradle or reconstruct RED per commit; formal Stage 5
verification remains pending.
