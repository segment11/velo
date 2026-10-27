# Bug 76 — FGroup Commands Review (Round 1)

**Module:** `io.velo.command.FGroup` (commands: `failover`, `flushdb`, `flushall`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete (agent 1) — 6 fixes in 6 commits + 1 style commit; all findings fixed incl. 76.5 riding with 76.4; re-review (agent 2) pending
**Reviewer evidence log:** `doc/bug_reviews/logs/20261027_123000_fgroup_review_agent2_differential_bug_76.log` (live Velo 41500/41501 vs Redis 7.2.11 7799/7800 differential)
**Review branch:** `review/fgroup`
**Reviewed commit:** `925a9611f2d829405411218f0eccd61883325351`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/FGroup.java` — full file (1-162): static CommandRegistry blocks (21-46), ctor (53-55), `parseSlots()` (65-69), `handle()` (76-87), `failover()` (89-142), `flushdb()` (144-161)
- `src/main/java/io/velo/persist/LocalPersist.java` — `oneSlot()` (106-108), `isOneSlotInitialized()` (114-116), `initializedOneSlots()` (121-132), `bindOneSlotIfInitialized` (220-224), `initSlotRuntimeBindings()` (246-255), `doSthInSlots()` (276-303), `addOneSlot()` @TestOnly (309-331), `fixSlotThreadId()` (630-633), `currentThreadFirstOneSlot()` (639-646), `firstOneSlot()` (651-663)
- `src/main/java/io/velo/persist/OneSlot.java` — `getFirstReplPairAsMaster()` (545-548), `getReplPairAsMasterList()` (555-569), `asyncExecute()` (704-716), `asyncCall()` (726-749), `putIfSeqBigger()` (1990-2011), `put()` (1977-1978, 2022-2034), `flush()` (2238-2309), `initFds()` (2311-2322), `hasData()` (2338-2353), `prepareAsSlave()` (2723-2754), `addUpstream()` (2772-2789)
- `src/main/java/io/velo/BaseCommand.java` — `putToOneSlot()` (1207-1225), `remove()` (1233-1243), `removeDelay()` (1250-1261), `exists()` (1269+)
- `src/main/java/io/velo/MultiWorkerServer.java` — `handleRequest()` (503-613) incl. readonly-connection gate (542-544), worker selection (601-612)
- `src/main/java/io/velo/acl/Category.java` — `isWriteCmd()` (59-61), `writeCmdList` containing `flushall`/`flushdb` (891-892) and `writeCmdSet.addAll` (989)
- `src/main/java/io/velo/repl/LeaderSelector.java` — singleton `getInstance()` (48-50), `connect()` (65-86), `stopLeaderLatch()` (309-321), `cleanUp()` (323-327)
- `src/main/java/io/velo/persist/index/IndexHandlerPool.java` — (1-60), shared `KeyAnalysisHandler`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java` — `flushdb()` (272)
- `src/test/groovy/io/velo/command/FGroupTest.groovy` — full file (1-155)
- `src/main/java/io/velo/acl/U.java` — `isWriteCmd` usage (629) via grep

Reference docs (ground truth):

- Redis 7.2 / latest FLUSHDB: https://redis.io/commands/flushdb/ — syntax `FLUSHDB [ASYNC | SYNC]`, "This command never fails", writes only
- Redis 7.2 / latest FLUSHALL: https://redis.io/commands/flushall/ — syntax `FLUSHALL [ASYNC | SYNC]`
- Redis 7.2 / latest FAILOVER: https://redis.io/commands/failover/ — syntax `FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]`; documented flow: (1) `CLIENT PAUSE WRITE` **first**, (2) wait for **first** replica to catch up, (3) demote
- `doc/design/04_command_processing_design.md` — full (net worker checks, dispatch, readonly mapping)
- `doc/design/README.md` + `doc/design/` index — design doc list consulted

Prior rounds checked (no overlap): `doc/bug_reviews/` grep for flushdb/flushall/failover/FGroup — prior failover findings (bug_2, bug_54) are about the Groovy `FailoverManager`, not the `FAILOVER` command; no prior report covers FGroup.

## Finding 1 (Bug 76.1): `data.length == 2` "skip for test" early return makes legal Redis syntax a silent no-op

**Severity:** Medium (round 1 author said High — refined by reviewer, see Review Feedback 76.1)
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — disposition: fixed `33b3f472`

### Cited files / lines
```java
// FGroup.java:90-94 — failover()
Reply failover() {
    // skip for test
    if (data.length == 2) {
        return OKReply.INSTANCE;
    }

// FGroup.java:144-148 — flushdb()
private Reply flushdb() {
    // skip for test
    if (data.length == 2) {
        return OKReply.INSTANCE;
    }
```

### Root cause
Both command paths embed a test hook in the production path: any invocation carrying exactly one
argument returns `OK` immediately without performing the command. `FLUSHDB ASYNC` and `FLUSHDB SYNC`
are first-class documented Redis syntax (`FLUSHDB [ASYNC | SYNC]`), and `FAILOVER ABORT` is documented
as `FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]` — all have `data.length == 2`.
`FGroupTest.'test handle'` (FGroupTest.groovy:28-44) constructs exactly `data2` so every assertion
rides this early return.

### Trigger / impact
- `FLUSHDB ASYNC` / `FLUSHALL SYNC` etc. → `+OK`, database untouched. Redis deletes all keys and
  returns OK. A client using flush for cleanup (test teardown, data purge) silently retains all data.
- `FAILOVER ABORT` → `+OK`, nothing aborted (no abort mechanism invoked) — misleading for an
  operator trying to roll back a stuck failover.
- Any third token (`FLUSHDB ASYNC FOO`) escapes the hook and flushes for real — see 76.2.

### Suggested fix direction
Remove the early returns; gate tests on a real seam instead (e.g. `BaseCommand.mockAGroup()` /
`isMockHandle` style flag or `@TestOnly` injection). Then implement `ASYNC|SYNC` acceptance (both may
map to the same synchronous-per-slot implementation) and `FAILOVER ABORT`/options handling per 76.2.

## Finding 2 (Bug 76.2): no argument validation — garbage args either no-op (len 2) or trigger a destructive flush (len ≥ 3)

**Severity:** Medium
**Class:** Redis protocol compatibility / Edge case
**Status:** `CONFIRMED (reproduced)` — disposition: fixed `df62b0c5`

### Cited files / lines
```java
// FGroup.java:82-84 — no validation of args at all before flushing
if ("flushdb".equals(cmd) || "flushall".equals(cmd)) {
    return flushdb();
}
// FGroup.java:150 — flush executed for ANY data.length != 2, e.g. FLUSHDB FOO BAR
return localPersist.doSthInSlots(oneSlot -> {
    oneSlot.flush();
// FGroup.java:121 — failover executed for ANY data.length != 2, TO/TIMEOUT/ABORT/FORCE silently ignored
return localPersist.doSthInSlots(oneSlot -> {
```

### Root cause
Neither `flushdb()` nor `failover()` validates argument tokens. There is no generic arity/token
validation layer either (`CommandRegistry` arity is registered but never checked — grep for
`\.arity\(\)|checkArity` in `src/main/java` returns nothing).

### Trigger / impact
- `FLUSHDB FOO BAR` (len 3) → wipes the database. Redis: `ERR syntax error` — only `ASYNC|SYNC`
  are legal (https://redis.io/commands/flushdb/).
- `FAILOVER TIMEOUT notanumber` or `FAILOVER NONSENSE x` (len 3) → a real failover starts with
  options silently ignored. Redis validates: `ERR TIMEOUT is not a float or out of range`,
  `ERR syntax error`, and rejects `ABORT` combined with `TO`/`TIMEOUT`.
- `FAILOVER TO host port` (len 4) targets are ignored — failover goes to any caught-up replica.

### Suggested fix direction
In `flushdb()`: if `data.length == 2`, accept only case-insensitive `ASYNC`/`SYNC`, else return
`ERR syntax error`; `data.length > 2` always syntax error. In `failover()`: parse the documented
grammar; unsupported-but-valid options may return a clear error (`ERR ... not supported`) rather
than silently ignoring tokens; unknown tokens → `ERR syntax error`.

## Finding 3 (Bug 76.3): FLUSHDB/FLUSHALL bypass the slot readonly / role-transition write fence — wipes a replica

**Severity:** High
**Class:** Correctness / Security
**Status:** `CONFIRMED (reproduced)` — disposition: fixed `cebb1fe5`

### Cited files / lines
```java
// FGroup.java:150-160 — calls OneSlot.flush() directly, no readonly check
return localPersist.doSthInSlots(oneSlot -> {
    oneSlot.flush();
    ...

// OneSlot.java:2238-2241 — flush() has no isReadonly()/write-fence check, only thread check
@SlaveNeedReplay
@SlaveReplay
public void flush() {
    checkCurrentThreadId();

// Contrast — the write gate every other write path goes through:
// BaseCommand.java:1220-1222
if ((oneSlot.isReadonly() || localPersist.isRoleTransitionWriteFenced()) && !ignoreReadonly) {
    throw new ReadonlyException();
}
// OneSlot.java:2026-2028 (put) and 1993-1995 (putIfSeqBigger) — same gate
```

### Root cause
Readonly enforcement for replicas lives inside the individual write paths (`put`, `remove`,
`removeDelay`, ...). `OneSlot.flush()` is a raw internal method with no such gate, and
`FGroup.flushdb()` calls it directly. The net-worker gate at `MultiWorkerServer.java:542` only
applies when the *client connection* itself is in readonly mode (`SocketInspector.isConnectionReadonly`),
not when the *slot* is a replica. Replicas are readonly in practice: `OneSlot.prepareAsSlave()` sets
`setReadonly(true)` (OneSlot.java:2723-2726), as does `addUpstream()` (2781-2783).

### Trigger / impact
On any replica (or a slot mid role-transition / migration-import, where writes are fenced), a plain
client sends `FLUSHALL` → all of the replica's slots are wiped and `+OK` returned. Redis 7.2:
`READONLY You can't write against a read only replica.` Worse, `flush()` ends with
`appendBinlog(xFlush)` (OneSlot.java:2307-2308) — on a replica whose binlog is on (chained
downstream replicas, cf. `addUpstream` keeping binlog ON), the wipe propagates downstream.

### Suggested fix direction
In `FGroup.flushdb()`'s per-slot lambda (or at the top of `OneSlot.flush()` for the non-replay
path): `if (oneSlot.isReadonly() || localPersist.isRoleTransitionWriteFenced()) throw new
ReadonlyException();` — the existing exception mapping in `RequestHandler.handle` (design doc
04, step 11) already converts it to a `READONLY` error reply. Take care not to break the
replication replay path (`@SlaveReplay` XFlush) — the guard belongs on the client-command path,
not the replay path.

## Finding 4 (Bug 76.4): FAILOVER validates catch-up for one slot only, but demotes/fences ALL slots

**Severity:** High
**Class:** Correctness / Concurrency
**Status:** `CONFIRMED (inspection)` — disposition: fixed `15cbd60d`

### Cited files / lines
```java
// FGroup.java:101-119 — validation only inspects currentThreadFirstOneSlot
var firstOneSlot = localPersist.currentThreadFirstOneSlot();
var asMasterList = firstOneSlot.getReplPairAsMasterList();
if (asMasterList.isEmpty()) {
    return new ErrorReply("no slave");
}
var currentFo = firstOneSlot.getBinlog().currentFileIndexAndOffset();
for (var replPairAsMaster : asMasterList) { ... exact equality check ... }

// FGroup.java:121-129 — action applies to ALL initialized slots, each with its OWN repl pairs
return localPersist.doSthInSlots(oneSlot -> {
    ...
    oneSlot.setReadonly(true);
    oneSlot.getDynConfig().setBinlogOn(false);
    var replPairAsMasterList = oneSlot.getReplPairAsMasterList();
    for (var replPairAsMaster : replPairAsMasterList) {
        replPairAsMaster.closeSlaveConnectSocket();
    }

// OneSlot.java:555-569 — repl pairs and binlog are per-slot state
public @NotNull ArrayList<ReplPair> getReplPairAsMasterList() {
    ArrayList<ReplPair> list = new ArrayList<>();
    for (var replPair : replPairs) { // per-slot field
```

### Root cause
Replication is per-slot (each `OneSlot` owns its `replPairs` and its binlog). The catch-up
precondition (`slaveFo == currentFo` exactly) is only evaluated for the first slot bound to the
calling thread. The subsequent `doSthInSlots` turns every slot readonly, disables binlog, and
closes every slot's slave sockets — including slots whose slaves were never checked.

### Trigger / impact
- Slot 0's slaves fully caught up, slot 7's slave 500 KB behind → `FAILOVER` succeeds, slot 7's
  un-shipped binlog tail is lost → permanent data loss on that shard after promotion.
- False negative: if the calling worker's first slot happens to have no master-side repl pairs
  while other slots do → `ERR no slave` although replicas exist.
- Edge: `currentThreadFirstOneSlot()` (LocalPersist.java:639-646) throws
  `IllegalStateException("No one slot for current thread")` if the chosen worker (empty-slot-list
  command → random worker, MultiWorkerServer.java:603-605) owns no initialized slot → generic
  error reply instead of a proper failover attempt.

### Suggested fix direction
Validate inside the per-slot lambda (on each slot's own eventloop, where the binlog is stable):
recompute `currentFileIndexAndOffset()` there and require every slot's slaves to be caught up
before `setReadonly(true)` / socket close; abort the whole failover if any slot fails. The
aggregation callback can then decide OK vs error from the per-slot results.

## Finding 5 (Bug 76.5): TOCTOU — writes accepted after the catch-up check but before `setReadonly` take effect

**Severity:** Low (round 1 author said Medium — refined by reviewer, see Review Feedback 76.5)
**Class:** Concurrency
**Status:** `PARTIALLY CONFIRMED` — real for non-calling workers' slots, closed for the calling worker's own slots; merge into the 76.4 fix — disposition: fixed `15cbd60d`

### Cited files / lines
```java
// FGroup.java:108-119 — check runs on the caller thread, once
var currentFo = firstOneSlot.getBinlog().currentFileIndexAndOffset();
for (var replPairAsMaster : asMasterList) { ... equality check ... }

// FGroup.java:121-124 — readonly is applied later, async, per-slot eventloop
return localPersist.doSthInSlots(oneSlot -> {
    try {
        oneSlot.setReadonly(true);

// OneSlot.java:736-748 — asyncCall submits to the slot's eventloop; already-queued writes run first
```

### Root cause
The catch-up precondition (exact `fileIndex`/`offset` equality) is checked once, before the
per-slot lambdas are queued. Any client write accepted in the window between the check and
`setReadonly(true)` taking effect on each slot's eventloop advances that slot's binlog past the
slave's position. The sockets are then closed anyway, dropping the un-shipped tail.

### Trigger / impact
Sustained write load + `FAILOVER` → last writes before the fence are lost on slaves. Redis's
documented flow (https://redis.io/commands/failover/) pauses writes FIRST (`CLIENT PAUSE WRITE`),
then waits for the replica to consume the stream — no such window exists there. Folding the check
into the same eventloop task as the fence (see 76.4 fix direction) closes most of this window for
slots on other workers; writes already in flight on the current slot still need ordering care.

### Suggested fix direction
Combine with 76.4: per-slot, on the slot's eventloop, (1) fence first (`setReadonly(true)`),
(2) then compare binlog tail vs slave catch-up, (3) close sockets only if caught up, else report
failure so the aggregation aborts (and un-fence).

## Finding 6 (Bug 76.6): blocking, `synchronized` ZooKeeper close runs on a slot worker eventloop

**Severity:** Low
**Class:** Concurrency
**Status:** `CONFIRMED (inspection)` — disposition: fixed `82470d2c`

### Cited files / lines
```java
// FGroup.java:134-141 — aggregation callback runs where the last promise completed (a slot worker eventloop)
}, resultList -> {
    var leaderSelector = LeaderSelector.getInstance();
    leaderSelector.stopLeaderLatch();
    log.warn("Repl leader latch stopped");

// LeaderSelector.java:309-321 — synchronized + Curator LeaderLatch.close() (network I/O)
public synchronized void stopLeaderLatch() {
    if (leaderLatch != null) {
        try {
            leaderLatch.close();
```

### Root cause
`LocalPersist.doSthInSlots` (276-303) completes the final promise from the last slot's eventloop
thread, so `stopLeaderLatch()` — a `synchronized` method performing a Curator `LeaderLatch.close()`
that can block on ZooKeeper I/O — executes on a slot worker eventloop. FGroup is the only
production caller (grep). Also note the aggregation callback runs once per... it runs on whichever
thread completes `Promises.all` — fine for a single AsyncReply, but the blocking call stalls every
slot served by that worker for the duration of the ZK close.

### Trigger / impact
ZK slow/unreachable during a failover (a realistic moment for ZK trouble) → slot worker eventloop
stalls for the close timeout → latency spike / unavailability for all keys on that worker, exactly
during failover.

### Suggested fix direction
Offload `stopLeaderLatch()` to a background/daemon thread or an executor, keeping the reply
decoupled from ZK latency.

## Finding 7 (Bug 76.7): key-analysis flush trigger depends on `firstOneSlot()` reference — can silently never fire

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (inspection)` — root cause refined by the reviewer (cluster mode, not lazy slot 0) — disposition: fixed `3a118894`

### Cited files / lines
```java
// FGroup.java:153-158 — run-once guard is reference equality with firstOneSlot()
if (oneSlot == localPersist.firstOneSlot()) {
    var indexHandlerPool = localPersist.getIndexHandlerPool();
    if (indexHandlerPool != null) {
        indexHandlerPool.getKeyAnalysisHandler().flushdb();
    }
}

// LocalPersist.java:651-663 — firstOneSlot() is @Nullable; cluster mode resolves via multiShard
public @Nullable OneSlot firstOneSlot() {
    if (oneSlots == null) { return null; }
    if (!ConfForGlobal.clusterEnabled) { return oneSlots[0]; }
    var toClientSlot = multiShard == null ? null : multiShard.firstToClientSlot();
    return getInnerOneSlotByToClientSlot(toClientSlot);
```

### Root cause
`KeyAnalysisHandler` is process-wide shared (IndexHandlerPool.java:31-40), so flushing once is the
right intent; the "once" is keyed on iterating the slot returned by `firstOneSlot()`. That method
may return `null` (cluster enabled, `multiShard == null` / unresolved) or — non-cluster with lazy
slot init — `oneSlots[0]` may be uninitialized while other slots are (lazy slot creation is
documented in LocalPersist: "Lazy slots created later bind themselves"), in which case the guard
never matches and the RocksDB key-analysis store is never cleared.

### Trigger / impact
`FLUSHALL` on a node whose `firstOneSlot()` is not among `initializedOneSlots()` → in-memory data
is wiped but the RocksDB key-analysis data survives → stale analysis output until the next full
analysis.

### Suggested fix direction
Decouple the run-once from slot identity: e.g. flush the key-analysis handler in the aggregation
callback (`resultList -> ...`) of `doSthInSlots`, or guard with a simple boolean captured by the
lambda, or check `localPersist.isOneSlotInitialized((short) 0)` semantics directly.

## Notes / non-findings

- `flushall` and `flushdb` sharing one implementation is correct for Velo: single database (Redis
  Cluster behaves identically per FLUSHDB doc note).
- Returning `AsyncReply` that resolves to `OK` after all slots complete matches Redis's default
  synchronous flush semantics; per-slot FIFO on each eventloop gives per-key linearization —
  acceptable given Velo's documented per-slot threading model.
- `parseSlots` returning an empty list (→ `SLOT_CAN_HANDLE_BY_ANY_WORKER`) is appropriate for
  server-level commands.
- `flushall`/`flushdb` are in `Category.writeCmdSet` (Category.java:891-892, 989), so the
  connection-readonly gate at MultiWorkerServer.java:542 works; `failover` is in the admin list
  (Category.java:72). ACL flags/categories registered in FGroup's static block match the Redis docs.
- `OneSlot.flush()` appending `XFlush` to the binlog with `@SlaveReplay` is the correct mechanism
  for replicating a flush (the danger is only that the client path reaches it on replicas — 76.3).
- Exact-equality catch-up (`currentFo == slaveFo`) rather than `slaveFo >= currentFo` is an
  explicit commented design decision (FGroup.java:115-116) — brittle (a permanently-behind
  reporting slave blocks failover) but intentional; not reported as a bug, but note 76.4/76.5
  change where the check runs.
- Requiring ALL replicas of the checked slot to be caught up (vs Redis's "first replica to catch
  up") is stricter than Redis — availability divergence, not data loss; not reported.
- `LeaderSelector.getInstance()` is a non-null eager singleton (LeaderSelector.java:41) — no NPE
  risk at FGroup.java:135.
- `doSthInSlots` with zero initialized slots → `Promises.all` of empty array → OK; acceptable.
- Prior-round failover findings (bug_2, bug_54-56) concern the Groovy `FailoverManager`; no
  overlap with these client-command findings.

**Reviewer confirmation of the non-findings (AI agent 2, round 1):** all of the above are confirmed as non-findings. Two were re-verified
directly: (a) `flushall`/`flushdb` **are** in `writeCmdSet` (`Category.java:891-892` → `writeCmdSet.addAll(writeCmdList)`, `Category.java:989`),
so the connection-level readonly gate does apply to them — the missing fence is the *slot*-level one (76.3); (b) the exact-equality catch-up
rule is brittle — observed live: the master refused `FAILOVER` while the replica was 17 bytes behind
(`ERR slave not catch up=..., current=...offset=642, slave=...offset=625`), where Redis 7.2.11 accepted the same command. Kept as a
non-finding per the author's decision, but it raises the practical frequency of the 76.4 scenario.
One correction to the list: `OneSlot.flush()` appending `XFlush` does **not** propagate a client-triggered wipe on a plain replica, because
`Binlog.append` is a no-op when the slot's binlog is off (`Binlog.java:484-486`, replica binlog off via `OneSlot.java:2736,2752`); propagation
only happens for a slot that keeps its binlog on (migration import, `OneSlot.java:2788-2790`).

## Reviewer checklist (for AI agent 2)

- Finding 76.1: start server (or use `LocalPersistTest.prepareLocalPersist` with real slots), `SET a
  1`, send `FLUSHDB ASYNC` (byte[2] data) → expect reply OK **and** `DBSIZE`/`EXISTS a` still 1 in
  Velo (bug) vs 0 in Redis 7.2. Same for `FAILOVER ABORT` returning OK with no abort side effects.
  Static inspection alternative: FGroup.java:92/146 reachable for any socket client — confirm no
  earlier arg filtering exists (RequestDecoder/RequestHandler have no arity validation).
- Finding 76.2: send `FLUSHDB FOO BAR` (len 3) against a populated server → Velo wipes + OK vs
  Redis `ERR syntax error`. Send `FAILOVER TIMEOUT abc` → Velo attempts real failover vs Redis
  float-validation error.
- Finding 76.3: prepare a slot, `oneSlot.prepareAsSlave("h", 1, false)` (or set
  `oneSlot.setReadonly(true)` directly), populate keys first, then `fGroup.flushdb()` with len-1
  data → assert reply is OK (bug: no ReadonlyException thrown; `OneSlot.flush()` has no gate) vs
  BaseCommand.remove()/put() throwing ReadonlyException under the same state. Differential: Redis
  7.2 replica returns READONLY.
- Finding 76.4: two slots on different workers, slot A slaves caught up, slot B slave behind
  (`slaveLastCatchUpBinlogFileIndexAndOffset` left null/stale) → `failover()` proceeds (no error
  about B) — cite that the check loop only reads `currentThreadFirstOneSlot()`.
- Finding 76.5: reasoning on interleaving — check runs before `doSthInSlots` submission; a `SET`
  submitted to a slot's eventloop before the fence lambda is queued executes before the fence;
  binlog advances; sockets then closed. Contrast with Redis doc flow step 1 (pause writes first).
- Finding 76.6: confirm `Promises.all(...).whenComplete` thread = last completing slot eventloop
  (Activej semantics), `stopLeaderLatch` is `synchronized` + network close; assess stall duration.
- Finding 76.7: cluster-mode `firstOneSlot()` nullability path (`multiShard == null` with
  `clusterEnabled`) or sparse-init non-cluster; verify guard never fires for
  `initializedOneSlots()` not containing `firstOneSlot()`.

**Checklist outcomes (AI agent 2, round 1):**

- 76.1 — **executed live** (Velo 41500 vs Redis 7.2.11 7799): `FLUSHDB ASYNC` / `FLUSHDB SYNC` / `FLUSHALL ASYNC` → Velo `+OK` with the key
  still present, Redis `+OK` with the key deleted. `FAILOVER ABORT` → Velo `+OK`, Redis `ERR No failover in progress.` Confirmed; also verified
  that no earlier arg filtering exists (`data[0]` is the command name, no arity layer anywhere).
- 76.2 — **executed live**: `FLUSHDB FOO BAR` → Velo `+OK` and the key deleted, Redis `ERR syntax error` and the key intact;
  `FLUSHDB ASYNC FOO` → same; `FAILOVER NONSENSE x` → Velo starts a real failover (`ERR no slave`), Redis `ERR syntax error`.
- 76.3 — **executed live on a real Velo replica** (41501 replicating from 41500): `SET` on the replica → `ERR readonly`, but `FLUSHALL` on the
  replica → `+OK` and the replica's `DBSIZE` dropped to 0 while the master kept its data. Redis replica: `FLUSHALL` → `READONLY You can't write
  against a read only replica.` and the data intact. Confirmed.
- 76.4 — **not reproduced** (needs ≥2 slots with per-slot replica lag); verified by inspection: the precondition loop reads only
  `currentThreadFirstOneSlot()` while `doSthInSlots` fences every initialized slot of every worker. Confirmed (inspection).
- 76.5 — **reasoned against the source**; partially confirmed: the window exists for other workers' slots only, because `OneSlot.asyncCall`
  runs the fence **inline** for the calling worker's own slots. Refined and merged into 76.4.
- 76.6 — **confirmed by inspection**: `Promises.all(...).whenComplete` without an executor runs on the completing (slot eventloop) thread;
  `stopLeaderLatch()` is `synchronized` and closes a Curator latch (network I/O). Only reachable on ZK-enabled deployments.
- 76.7 — **confirmed by inspection**, but the author's trigger was corrected: non-cluster mode initializes all slots eagerly
  (`LocalPersist.java:408-421`), so the guard does fire there; the failure is cluster mode, where `firstOneSlot()` resolves to a slot owned by
  another node and returns `null`.

## Review Feedback (AI agent 2, round 1)

**Reviewer:** AI agent 2
**Reviewed commit:** `925a9611f2d829405411218f0eccd61883325351` (= current `HEAD` at review time, `git log --oneline -1`)
**MUST-READ performed:** this report; `src/main/java/io/velo/command/FGroup.java` (1-162); `src/main/java/io/velo/persist/LocalPersist.java` (`initSlots` 395-422, `ensureOneSlotInitialized` 144-171, `initializedOneSlots` 121-132, `doSthInSlots` 276-303, `currentThreadFirstOneSlot` 639-646, `firstOneSlot` 651-663, `getInnerOneSlotByToClientSlot` 701-711, `isRoleTransitionWriteFenced` 368-370); `src/main/java/io/velo/persist/OneSlot.java` (`flush` 2238-2309, `put` 2022-2034, `putIfSeqBigger` 1990-2011, `asyncCall` 726-749, `appendBinlog` 1204-1212, `prepareAsSlave` 2723-2754, `addUpstream` 2772-2789, `getReplPairAsMasterList` 555-569); `src/main/java/io/velo/BaseCommand.java` (`putToOneSlot` 1207-1225, `remove` 1233-1243, `removeDelay` 1250-1261); `src/main/java/io/velo/MultiWorkerServer.java` (`handleRequest` 503-613, `getByteBufPromiseByOtherEventloop` 628-665, `transferAsyncReply` 673-690); `src/main/java/io/velo/RequestHandler.java` (`handle` 495-580); `src/main/java/io/velo/decode/Request.java` (`cmd()` 236); `src/main/java/io/velo/repl/Binlog.java` (`append` 483-486); `src/main/java/io/velo/repl/LeaderSelector.java` (`connect` 65-86, `startLeaderLatch` 264-290, `stopLeaderLatch` 309-321, `cleanUp` 323-327); `src/main/java/io/velo/repl/cluster/MultiShard.java` (`firstToClientSlot` 266-277); `src/main/java/io/velo/acl/Category.java` (`isWriteCmd` 59-61, `writeCmdList` 867+, `flushall`/`flushdb` 891-892, `writeCmdSet.addAll` 989); `src/test/groovy/io/velo/command/FGroupTest.groovy` (1-155); `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java` (`flushdb` 272-274); `src/main/java/io/velo/persist/index/IndexHandlerPool.java` (1-60).

**Ground truth:** Redis 7.2.11 (`redis-server --version` → `v=7.2.11`) started locally on port 7799 (master) / 7800 (replica);
Velo `build/libs/velo-1.0.0.jar` (built 2026-10-27 11:04) started on port 41500 (master) / 41501 (replica), dir `/tmp/opencode/velo76{,b}`.
Full transcript: `doc/bug_reviews/logs/20261027_123000_fgroup_review_agent2_differential_bug_76.log`.

**Method note on `data.length`:** `data[0]` is the command name (`Request.cmd()` → `new String(data[0])`, `Request.java:236`), so
`data.length == 2` means "command + exactly ONE argument" (confirmed by `FGroupTest.groovy:21-22,30-31` where `data2[0]` is left null and
`data2[1] = 'a'`). All length arithmetic below uses that convention.

**Summary:** 6 CONFIRMED, 1 PARTIALLY CONFIRMED, 0 REFUTED, 0 NOT A BUG. Two severities refined (76.1 High→Medium, 76.5 Medium→Low),
one root cause refined (76.7), one fix direction corrected (76.3).

### 76.1 — `data.length == 2` "skip for test" early return → `CONFIRMED (reproduced)`

Severity refined **High → Medium**: the impact is *stale data + false success*, not data loss — the flush is skipped, not over-applied.
Plain `FLUSHDB` (no argument) still works, so the divergence is limited to the documented `ASYNC|SYNC` variants.

Live differential (Velo first, Redis second), each block starts from `SET k v`:

```
--- 76.1 FLUSHDB ASYNC (data must be deleted)
FLUSHDB ASYNC => OK
EXISTS k => 1 ; DBSIZE => 1            <-- VELO: OK but data retained
...
FLUSHDB ASYNC => OK
EXISTS k => 0 ; DBSIZE => 0            <-- REDIS: OK and data deleted
--- 76.1b FLUSHDB SYNC        VELO: OK / EXISTS k => 1        REDIS: OK / EXISTS k => 0
--- 76.1c FLUSHALL ASYNC     VELO: OK / EXISTS k => 1        REDIS: OK / EXISTS k => 0
--- 76.1d control: plain FLUSHDB   VELO: OK / EXISTS k => 0   REDIS: OK / EXISTS k => 0
```

Static citation (unchanged from the report, verified): `FGroup.java:91-94` and `FGroup.java:145-148` return `OKReply.INSTANCE` before any work.

Refinements to the round-1 text:
- The hook does **not** cover `FAILOVER TIMEOUT 1000` (`data.length == 3` → real path) nor `FAILOVER TO host port` (`data.length == 4` → real path).
  Those belong to 76.2. Only `FAILOVER ABORT` is swallowed by the hook.
- `FAILOVER ABORT` is doubly wrong: Redis answers `ERR No failover in progress.` (7.2.11, verified), Velo answers `OK` although it never entered a
  failover state that could be aborted.
- The fix must also update `FGroupTest.'test handle'` (`FGroupTest.groovy:28-44`) — that test is the only one riding the hook; the author's
  "gate tests on a real seam" direction is accepted.

### 76.2 — no argument validation → `CONFIRMED (reproduced)`

Severity **Medium** confirmed.

```
--- 76.2 FLUSHDB FOO BAR
FLUSHDB FOO BAR => OK
EXISTS k => 0 ; DBSIZE => 0            <-- VELO: destructive flush on garbage args
...
FLUSHDB FOO BAR => ERR syntax error
EXISTS k => 1 ; DBSIZE => 1            <-- REDIS: rejected, data intact
--- 76.2b FLUSHDB ASYNC FOO   VELO: OK / EXISTS k => 0        REDIS: ERR syntax error / EXISTS k => 1
--- 76.2c FAILOVER ABORT      VELO: OK                       REDIS: ERR No failover in progress.
--- 76.2d FAILOVER TIMEOUT 100  VELO: ERR no slave          REDIS: ERR FAILOVER requires connected replicas.
--- 76.2e FAILOVER TO 127.0.0.1 6399  VELO: ERR no slave    REDIS: ERR FAILOVER requires connected replicas.
--- 76.2f FAILOVER NONSENSE x  VELO: ERR no slave           REDIS: ERR syntax error
--- 76.2g FAILOVER (no args)   VELO: ERR no slave           REDIS: ERR FAILOVER requires connected replicas.
```

Notes:
- The Velo `FAILOVER ...` replies above are all produced by the *real* failover path (`FGroup.java:101-105` → `ErrorReply("no slave")`),
  i.e. Velo **starts a real failover attempt for arguments it does not understand** (`FAILOVER NONSENSE x` behaves exactly like `FAILOVER`).
  Redis rejects the unknown token outright. This is the sharpest form of the finding: an unknown option is silently reinterpreted as "fail over now".
- `FAILOVER TO host port` and `FAILOVER TIMEOUT <ms>` are silently ignored (the `TO` target and the timeout never reach the code) — the report's
  claim is confirmed; the live run only shows the resulting error text because this test node has no replica.
- No arity layer exists to catch this: `grep -rn "\.arity\b|arity()" src/main dyn/src` → only `dyn/src/io/velo/command/CommandCommand.groovy:103`
  (the `COMMAND DOCS` output). The registered arity `-1` in `FGroup.java:23,31,39` is never enforced anywhere.
- Suggested fix direction accepted; the author may reuse `ErrorReply.FORMAT` (→ `ERR wrong number of arguments`) only if the arity itself is
  wrong — for `FLUSHDB FOO BAR` Redis answers `ERR syntax error`, so a dedicated syntax-error reply is the compatible choice.

### 76.3 — FLUSHDB/FLUSHALL bypass the slot readonly / role-transition write fence → `CONFIRMED (reproduced)`

Severity **High** confirmed. This is the most damaging finding of the round: it was reproduced on a **live replica**.

Velo master 41500 → Velo replica 41501 (`REPLICAOF 127.0.0.1 41500`):

```
--- 76.3 step1: normal write on replica
ERR readonly                                        <-- the fence works for SET
--- 76.3 step2: FLUSHALL on replica
OK
replica DBSIZE => 0                                 <-- replica data WIPED
replica EXISTS k => 0
master DBSIZE => 1 ; master EXISTS k => 1           <-- master unaffected
```

Redis 7.2.11 master 7799 → replica 7800:

```
redis replica DBSIZE before => 2
redis replica FLUSHALL => READONLY You can't write against a read only replica.
redis replica DBSIZE after => 2                     <-- replica data intact
```

Static citation confirmed: `OneSlot.flush()` (`OneSlot.java:2238-2241`) has no `isReadonly()` check, unlike `OneSlot.put` (2026),
`OneSlot.putIfSeqBigger` (1993) and `BaseCommand.remove/removeDelay` (1239, 1257). The only fence on the client path is the
*connection*-level gate `MultiWorkerServer.java:542` (`SocketInspector.isConnectionReadonly(socket) && Category.isWriteCmd(cmd)`), which does
not know anything about the *slot's* role. `flushall`/`flushdb` are in `writeCmdList` (`Category.java:891-892`), so that gate does fire for a
read-only *connection* — the slot-level fence is simply missing.

Refinements:
- **Downstream propagation is conditional.** `flush()` ends with `appendBinlog(xFlush)` (`OneSlot.java:2307-2308`), but `Binlog.append`
  returns immediately when the slot's binlog is off (`Binlog.java:484-486`). A plain replica has the binlog off (`prepareAsSlave` →
  `dynConfig.setBinlogOn(false)`, `OneSlot.java:2736,2752`), so the wipe does **not** propagate downstream there. It *does* propagate for a
  slot that keeps its binlog on, i.e. the migration-import slot (`addUpstream` keeps the binlog on, `OneSlot.java:2788-2790`). The round-1 text
  stated this conditionally; the reviewer confirms it conditionally and rejects the unconditional reading.
- **The suggested fix direction needs a correction.** Throwing `ReadonlyException` *inside* the `doSthInSlots` lambda does **not** produce a
  `READONLY` reply: the exception is captured by `OneSlot.asyncCall` → `LocalPersist.doSthInSlots` (`LocalPersist.java:296-301`,
  `finalPromise.setException(e)`) → `MultiWorkerServer.transferAsyncReply` (`675-679`) → `new ErrorReply(e.getMessage())`, i.e. a generic
  `-ERR <message>`. The gate must therefore be evaluated **before** the async dispatch (on the caller side, per slot, or by mapping the
  exception explicitly to `ErrorReply.READONLY`). Note also that `ErrorReply.READONLY` is the bare string `"readonly"`
  (`ErrorReply.java:115`), not Redis' `READONLY You can't write against a read only replica.` — worth aligning while the area is open.
- The role-transition fence (`localPersist.isRoleTransitionWriteFenced()`, `LocalPersist.java:368-370`) is bypassed by the same path.

### 76.4 — FAILOVER validates catch-up for one slot only, but demotes/fences ALL slots → `CONFIRMED (inspection)`

Severity **High** confirmed (not reproduced: a faithful repro needs ≥2 slots with per-slot replica lag, i.e. a 2-slot master plus 2 replicas
with one deliberately lagging).

Static evidence, unchanged and re-verified:
- the precondition is evaluated only on `localPersist.currentThreadFirstOneSlot()` (`FGroup.java:101-119`),
- while the demotion loop runs over **every** initialized slot of **every** worker: `LocalPersist.doSthInSlots` → `initializedOneSlots()`
  (`LocalPersist.java:121-132, 276-303`) → per slot `setReadonly(true)`, `setBinlogOn(false)`, `closeSlaveConnectSocket()`
  (`FGroup.java:121-129`),
- and replication state is per slot: `OneSlot.getReplPairAsMasterList()` iterates the slot's own `replPairs` (`OneSlot.java:555-569`),
  each slot owns its own `binlog`.

The FAILOVER command body is dispatched to a *random* worker (`MultiWorkerServer.java:602-605` → `getByteBufPromiseByOtherEventloop`,
`628-634` → the slot worker eventloop of that worker), so the checked slot is an arbitrary worker's first slot while the fence covers every
worker's slots.

Refinements:
- `currentThreadFirstOneSlot()` throwing `IllegalStateException("No one slot for current thread")` (`LocalPersist.java:639-646`) does **not**
  crash the server: it is caught by the generic `catch (Exception e)` in `RequestHandler.handle` (`RequestHandler.java:573-576`) and turned into
  `ErrorReply("No one slot for current thread")`. The impact is a confusing error, not a crash — the round-1 text is accurate on the
  mechanism but the wording ("generic error reply instead of a proper failover attempt") is confirmed and should be phrased as "confusing
  error reply".
- Live observation of the exact-equality catch-up rule (design decision, recorded as a non-finding by the author, confirmed as brittle):
  with one live replica the master refused to fail over while the replica was only **17 bytes** behind:
  `ERR slave not catch up=0.0.0.0:41501, current=FileIndexAndOffset{fileIndex=0, offset=642}, slave=FileIndexAndOffset{fileIndex=0, offset=625}`,
  whereas Redis 7.2.11 accepted the same `FAILOVER` (`OK`, master became `role:slave`). Availability divergence, not data loss — kept as a
  non-finding per the author's decision, but it makes the "un-checked slot" case of 76.4 even more likely to matter.

### 76.5 — TOCTOU between the catch-up check and `setReadonly` → `PARTIALLY CONFIRMED` (refined)

Severity refined **Medium → Low**, and the finding is **partially** confirmed: the window exists only for the slots of the **other** workers.

Why the calling worker's own slots are safe: `doSthInSlots` → `OneSlot.asyncCall` (`OneSlot.java:726-749`) compares the current thread id with
the slot's `threadIdProtectedForSafe` and, **when they match, runs the lambda inline**. The FAILOVER command body itself runs on a slot worker
eventloop (`MultiWorkerServer.java:628-634`), so for that worker's slots the fence is applied synchronously inside the command; any client
write that arrives afterwards is queued behind the fence and hits the readonly gate. For the other workers' slots the fence is queued
asynchronously (`slotWorkerEventloop.execute`), so writes accepted in that window advance the binlog past the slave position and are then
dropped when the sockets are closed.

Consequently the exposure of 76.5 is a strict subset of 76.4 (the slots that are not checked are also the slots that are not fenced
atomically). The reviewer recommends **merging 76.5 into the 76.4 fix** rather than fixing it separately: fence first, then compare
binlog tail vs slave catch-up, per slot, on the slot's own eventloop.

### 76.6 — blocking, `synchronized` ZooKeeper close on a slot worker eventloop → `CONFIRMED (inspection)`

Severity **Low** confirmed.

- The aggregation callback of `doSthInSlots` (`LocalPersist.java:288-301`) is invoked by `Promises.all(promises).whenComplete(...)` without an
  executor → ActiveJ runs it on the thread that completes the last promise, i.e. a **slot worker eventloop** (or, when the calling worker's slots
  complete inline, on the calling slot worker thread). `FGroup.java:134-141` then calls `LeaderSelector.getInstance().stopLeaderLatch()`.
- `stopLeaderLatch()` (`LeaderSelector.java:309-321`) is `synchronized` and calls Curator's `LeaderLatch.close()`, which closes the ZooKeeper
  client and can block for the session/connection timeout. It also holds the `LeaderSelector` monitor for that duration, so any other thread
  calling a `synchronized` method of `LeaderSelector` (e.g. `startLeaderLatch`, `LeaderSelector.java:264`) blocks as well.

Refinements:
- "FGroup is the only production caller (grep)" is **inaccurate**: `LeaderSelector.cleanUp()` (`LeaderSelector.java:325`) also calls
  `stopLeaderLatch()`. FGroup is however the only caller that runs it on a **slot worker eventloop**.
- The stall only exists on ZooKeeper-enabled deployments: with no `zookeeperConnectString`, `LeaderSelector.connect()` returns `false`
  (`LeaderSelector.java:65-71`), `leaderLatch` stays `null` and `stopLeaderLatch()` is a no-op. Severity Low is therefore appropriate.
- Fix direction (offload to a background thread / executor) is accepted.

### 76.7 — key-analysis flush trigger depends on the `firstOneSlot()` reference → `CONFIRMED (inspection)` (root cause refined)

Severity **Low** confirmed. The author's *mechanism* is right, but the stated trigger ("lazy slot 0 uninitialized in non-cluster mode") is
**wrong** and is corrected below.

- Non-cluster mode eagerly initializes **all** slots at startup: `LocalPersist.initSlots` → `for (short slot = 0; slot < slotNumber; slot++)
  ensureOneSlotInitialized(slot)` (`LocalPersist.java:408-421`), so `oneSlots[0]` is non-null and `firstOneSlot()` returns it
  (`LocalPersist.java:651-663`) → the guard `oneSlot == localPersist.firstOneSlot()` **does** fire for slot 0. The "sparse-init" scenario
  described in the round-1 text cannot occur in a non-cluster deployment.
- The real trigger is **cluster mode**: `firstOneSlot()` resolves through `multiShard.firstToClientSlot()` (`MultiShard.java:266-277`), which
  returns the smallest client slot across **all** shards of the cluster — usually owned by a different node — and then
  `getInnerOneSlotByToClientSlot()` (`LocalPersist.java:701-711`) returns `oneSlots[slot]`, i.e. `null` for a slot this node does not own.
  With `firstOneSlot() == null` the guard `oneSlot == null` never matches any real slot → the RocksDB key-analysis store is **never** cleared
  by `FLUSHALL`/`FLUSHDB` in cluster mode, while the in-memory data *is* wiped.

Fix direction accepted (decouple the "run once" from slot identity — e.g. do it in the aggregation callback of `doSthInSlots`).

### Additional observations (recorded, **not** new findings — for agent 1's awareness)

- `KeyAnalysisHandler.flushdb()` returns a `CompletableFuture` that `FGroup.java:156` does not await → `FLUSHALL` answers `+OK` before the
  analysis store has actually been cleared. Not a bug per se, but it must be considered when 76.7 is fixed (the fix should not make the reply
  depend on the analysis flush either).
- `FGroup.failover()` never promotes a replica; it only demotes the local node and stops the leader latch (`FGroup.java:121-141`, comment at
  `138`: *"later self will start leader latch again and make self as slave"*). The command is therefore a half-implemented Redis-style
  coordinated failover. Kept out of the findings table (design gap, not a defect of the current code), but agent 1 should not treat 76.4/76.5 as
  "the failover is now correct" after fixing them.
- `FAILOVER` is not the failover entry point used in production — the cluster path is `CLUSTER FAILOVER`
  (`dyn/src/io/velo/command/ClusterxCommand.groovy:103,275-297`) and the Groovy `FailoverManager`. The plain `FAILOVER` command is currently
  a separate, partly-implemented implementation, which lowers the operational urgency of 76.4/76.5 but not their correctness impact.

### Recommended fix order

1. **76.3** (data loss on replicas / security) — highest severity, reproduced, cheapest to test.
2. **76.4 + 76.5 merged** (data loss on failover) — one fix, one commit each per the one-bug-one-commit rule; 76.5 may ride along with 76.4 only
   if it is covered by the same red test.
3. **76.1** (Redis compat, silent no-op) — must be done together with 76.2 since the two share the argument-parsing code.
4. **76.2** (argument validation) — pair with 76.1 in the same area but a separate commit.
5. **76.7** (cluster-mode key-analysis flush) — small, isolated.
6. **76.6** (blocking ZK close on the eventloop) — lowest urgency; may be scheduled any time.

Per the fix-phase rules, every one of these must start with a failing regression test that reproduces the reviewer evidence above
(`FLUSHDB ASYNC` keeps the key; `FLUSHDB FOO BAR` wipes the key; `FLUSHALL` on a readonly slot wipes it), followed by the minimal fix, a
focused `./gradlew :test --tests ...` run, and a JaCoCo check of the changed lines.


## Response (AI agent 1, round 1)

**Method:** re-verified every reviewer correction against the source before agreeing (citations below). No disagreement
filed; two of my own sub-claims are retracted. No retractions of findings.

| Finding | Reviewer verdict | My response |
|---|---|---|
| 76.1 | CONFIRMED (reproduced), High→Medium | **Agree**, including severity refinement |
| 76.2 | CONFIRMED (reproduced), Medium | **Agree** |
| 76.3 | CONFIRMED (reproduced), High + 2 refinements | **Agree**; fix-direction correction verified and extended (see below) |
| 76.4 | CONFIRMED (inspection), High | **Agree**, incl. the "confusing error, not crash" wording fix |
| 76.5 | PARTIALLY CONFIRMED, Medium→Low, merge into 76.4 | **Agree** with refinement and merge |
| 76.6 | CONFIRMED (inspection), Low + caller correction | **Agree**; my "only production caller" claim retracted |
| 76.7 | CONFIRMED (inspection), root cause refined (cluster mode) | **Agree**; my non-cluster "sparse-init" sub-claim retracted |

Per-finding notes (only where I add or retract something):

- **76.1 severity High→Medium — accepted.** The reviewer is right that the failure mode is *false success + data
  retention*, not data loss, and plain `FLUSHDB`/`FLUSHALL` (no args) still works. My original severity over-weighted
  operator surprise.
- **76.3 fix-direction correction — verified and strengthened.** I re-checked the async exception path:
  `ReadonlyException` carries **no message** (`ReadonlyException.java:6` — empty class body), so `transferAsyncReply`'s
  `new ErrorReply(e.getMessage())` (`MultiWorkerServer.java:675-679`) builds an `ErrorReply` whose `buffer()` yields
  the literal `-ERR null\r\n` (`ErrorReply.java:190-192`, string concatenation with null). So throwing inside the
  `doSthInSlots` lambda is not just "generic error" — it is a broken reply. The `ReadonlyException` → `ErrorReply.READONLY`
  mapping exists only in `RequestHandler.handle`'s synchronous catch (`RequestHandler.java:568-571`). Fix must evaluate
  the slot-readonly / role-transition fence **before** dispatching (caller side, over `initializedOneSlots()`), or
  explicitly resolve to `ErrorReply.READONLY`. Aligning `ErrorReply.READONLY`'s text (`ErrorReply.java:115`, currently
  `"readonly"`) with Redis' `READONLY You can't write against a read only replica.` is accepted as part of the fix —
  noting it will also change the SET-on-replica reply, so the focused tests must be updated in the same commit.
- **76.4 refinement — verified:** the generic catch at `RequestHandler.java:576-579` turns the
  `IllegalStateException` into `ErrorReply("No one slot for current thread")`; "confusing error reply" is the right phrasing.
- **76.5 refinement — verified:** `OneSlot.asyncCall` runs the lambda inline when the caller thread owns the slot
  (`OneSlot.java:726-734`), and the FAILOVER body executes on a slot worker eventloop (empty slot list → random worker,
  `MultiWorkerServer.java:601-612` → `getByteBufPromiseByOtherEventloop`), so the calling worker's own slots are fenced
  synchronously run-to-completion and the TOCTOU window exists only for other workers' slots — a strict subset of 76.4's
  unchecked slots. Merging into the 76.4 fix (fence first, then check, per slot, on the slot's eventloop) is the right call.
- **76.6 caller correction — accepted, my claim retracted:** `LeaderSelector.cleanUp()` also calls `stopLeaderLatch()`
  (`LeaderSelector.java:323-327`). Correct statement: FGroup is the only caller that runs it on a **slot worker
  eventloop**; `startLeaderLatch()` being `synchronized` (`LeaderSelector.java:264`) confirms the monitor-stall concern.
- **76.7 root-cause refinement — accepted, my sub-claim retracted:** non-cluster mode eagerly initializes ALL slots
  (`LocalPersist.initSlots`, `LocalPersist.java:418-421`), so my "non-cluster lazy/sparse slot 0" trigger cannot occur;
  I struck that scenario. The real trigger is cluster mode: only `localAssignedInnerSlots()` are initialized
  (`LocalPersist.java:413-417`), while `firstOneSlot()` resolves via `multiShard.firstToClientSlot()` — the smallest
  client slot across **all** shards (`MultiShard.java:266-279`) — and `getInnerOneSlotByToClientSlot` returns
  `oneSlots[slot]` = null for a slot owned by another node (`LocalPersist.java:701-712`) → the run-once guard never
  fires in cluster mode. My original cluster-mode scenario was correct; the non-cluster sub-claim was not.

Additional observations (reviewer): acknowledged, all three. In particular I commit to NOT treating 76.4/76.5 fixes as
making `FAILOVER` a complete coordinated failover (it still only demotes locally, `FGroup.java:121-141`; production
failover goes through `CLUSTER FAILOVER` / `FailoverManager`), and the 76.7 fix must not make the reply depend on the
RocksDB analysis flush future.

**Recommended fix order: accepted as given** (76.3 → 76.4+76.5 → 76.1 → 76.2 → 76.7 → 76.6; 76.1/76.2 share the
argument-parsing area but remain separate commits; 76.5 rides with 76.4 only under a red test covering the merged behavior).

## Fix Log (AI agent 1, round 1 fix phase)

Reviewed commit `925a9611`; all fixes on branch `review/fgroup`. Focused test = `./gradlew :test --tests "io.velo.command.FGroupTest"` (4/4 green after every
fix; JaCoCo via `scripts/jacoco_cover.py io.velo.command.FGroup <range>`). Note: the environment hit the known intermittent
`compileTestGroovy` "Java heap space" daemon flake (already recorded as an open item on bug_73); a daemon restart
(`./gradlew --stop`, optionally `-Dorg.gradle.jvmargs="-Xmx4g ..."`) before each re-run cleared it every time — several
"failed" runs were stale XML from a heap-crashed compile, re-run after restart before accepting any result.

| Bug | Failing test evidence (RED) | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 76.3 | `test flushdb`: readonly slot → `resolveReply: reply == ErrorReply.READONLY` failed with `AsyncReply{OKReply}` (flush ran on a readonly slot); fence case red likewise | `cebb1fe5` `fix: reject flushdb and flushall on readonly slot or role fence` | green 4/4 (+ `MultiWorkerServerTest` assertion updated for aligned READONLY text; its 2 full-class failures reproduced identically on clean HEAD — pre-existing, unrelated) | FGroup.java:146-155 fence + readonly guards, all 4 branches covered |
| 76.4+76.5 | `test failover` (2 slots, slot1 lagging): `resolveReply(reply) instanceof ErrorReply` failed — reply resolved `OKReply` (failover succeeded), slot0 left readonly, no rollback | `15cbd60d` `fix: validate slave catch up per slot before failover demotion` | green 4/4 | per-slot fence/check/demote loop FGroup.java:112-136 all branches (incl. 4/4 on offset/fileIndex compare); abort+rollback 151-183 both guard arms; only fault-injection lines (137-138, 145-148, 176-177) uncovered |
| 76.1 | `test flushdb`: `flushdb async` → `reply instanceof AsyncReply` failed (sync `OKReply`, key survived); `test failover`: `failover abort` → OKReply not ErrorReply; `test handle`: unknown token → OKReply not SYNTAX | `33b3f472` `fix: flushdb async sync actually flush and answer failover abort` | green 4/4 | abort check FGroup.java:100 all 4 branches; ASYNC/SYNC acceptance + syntax rejection 212-215 all branches |
| 76.2 | `test flushdb`: `flushdb async extra` wiped the data (OKReply, key gone) instead of SYNTAX; `test failover`: `failover nonsense x` started a real failover (`no slave` AsyncReply) instead of SYNTAX | `df62b0c5` `fix: validate flushdb and failover arguments` | green 4/4 | option-check chain FGroup.java:107 all 8 branches (to/timeout/abort/force/unknown); len>2 syntax guard 221-222 all branches |
| 76.7 | `test flushdb` (cluster mode, own shard range empty → `firstOneSlot()==null`): `countingHandler.flushdbCalls == 1` failed with `0` — analysis store never cleared while in-memory data was wiped | `3a118894` `fix: flush key analysis store once in flushdb aggregation` | green 4/4 | aggregation flush FGroup.java:225-227 incl. pool-null branch all covered |
| 76.6 | `test failover` (reflectively injected `RecordingLeaderLatch`): `RecordingLeaderLatch.closedByThread != Thread.currentThread()` failed — close ran on the Test worker (caller) thread | `82470d2c` `fix: offload stop leader latch off the slot eventloop` | green 4/4 | offload lambda FGroup.java:196-210 fully covered by the success case |

Style-only companion commit (user request, no behavior change): `25e71691` `refactor: use explicit error reply cast in
test assertions` — `(reply as ErrorReply).message` strict casts across O/J/Y/N/V/Q GroupTest, BGroupTest, RequestHandlerTest;
touched methods re-run green.

Test-infrastructure follow-up (user request): `ec2676d1` `refactor: use testonly leader latch accessors in the failover
test` — the 76.6 test's reflective `leaderLatch` field access was replaced with `@TestOnly`-annotated
`LeaderSelector.getLeaderLatch()/setLeaderLatch(...)` (same pattern as the existing `getMasterAddressLocalMocked` pair);
`test failover` and `LeaderSelectorTest` (25/25) re-run green.

Per-bug RED runs were executed with the production change absent/stashed; each row's RED output was read from the Spock
XML before the fix was written (76.3/76.4 evidence above is pasted from those runs).

## Residual Risk

- **Fault-injection lines uncovered** (76.4 fix): per-slot `IOException` → `RuntimeException` mapping (FGroup.java:137-138),
  the `Promises.all` exception path (145-148) and the rollback `IOException` handler (176-177) need forced I/O failures to
  reach; accepted as defensive.
- **76.5 fully closed only for the fence-vs-write race** — with check+fence+close now one run-to-completion task per slot's
  eventloop no write can interleave; but a slave whose catch-up report races the check still causes an abort (safe direction).
- **Exact-equality catch-up rule kept** (commented design decision) — a replica permanently reporting even 1 byte behind
  blocks failover (reviewer observed a 17-byte refusal live); availability-only, not data loss.
- **FAILOVER remains a partial implementation** (reviewer observation): it demotes locally + stops the leader latch but does
  not promote a replica or support TO/TIMEOUT/FORCE (now explicitly `not support` errors). Production failover path is
  `CLUSTER FAILOVER` / FailoverManager.
- **Rollback is best effort**: on abort, demoted slots are un-fenced and binlog restored via `asyncExecute`; slave sockets
  already closed rely on slaves reconnecting. A slot crash between demote and rollback could stay fenced.
- **`MultiWorkerServerTest` full-class failures** (`test mock inject and handle`, `test cluster slot cross shards` in some
  orders) reproduce identically on clean HEAD in this working tree — pre-existing environmental (modified local
  `velo.properties` / untracked files), not from bug 76 work.
- **Known `compileTestGroovy` "Java heap space" daemon flake** hit repeatedly during this fix phase; daemon restart clears
  it (already an open item from bug_73).

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 76.1 | `data.length == 2` test hook makes `FLUSHDB ASYNC/SYNC`, `FAILOVER ABORT` silent no-ops | Medium (was High) | Correctness / Redis compat | CONFIRMED (reproduced) | fixed `33b3f472` |
| 76.2 | No arg validation: garbage args no-op (len 2) or destructive-flush (len ≥ 3) | Medium | Redis compat / Edge case | CONFIRMED (reproduced) | fixed `df62b0c5` |
| 76.3 | flushdb bypasses slot readonly / role-transition fence — replica data wipe | High | Correctness / Security | CONFIRMED (reproduced) | fixed `cebb1fe5` |
| 76.4 | FAILOVER checks catch-up for first slot only but fences/closes ALL slots | High | Correctness / Concurrency | CONFIRMED (inspection) | fixed `15cbd60d` |
| 76.5 | Writes accepted between catch-up check and setReadonly (TOCTOU) | Low (was Medium) | Concurrency | PARTIALLY CONFIRMED — merged into 76.4 | fixed with `15cbd60d` |
| 76.6 | Blocking synchronized ZK close on slot worker eventloop | Low | Concurrency | CONFIRMED (inspection) | fixed `82470d2c` |
| 76.7 | Key-analysis flushdb guard depends on firstOneSlot() reference | Low | Edge case | CONFIRMED (inspection) — root cause refined (cluster mode) | fixed `3a118894` |
