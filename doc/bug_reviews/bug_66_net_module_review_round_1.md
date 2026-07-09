# Bug 66 — SocketInspector.onConnect NPE on the first client connection of every cluster node

**Module:** net (`io.velo.SocketInspector`)
**Round:** 1 (found by `ClusterOperationProcessManualTest` review of every cluster test's startup log)
**Status:** documented, not yet fixed

## Symptom

Every cluster test (and every real cluster node) logs the same stack trace on startup, on the
very first inbound client connection:

```
[net-worker-0] ERROR [io.activej.eventloop.Eventloop(1129)] - Fatal error in io.activej.net.AbstractReactiveServer$$Lambda/...
java.lang.NullPointerException: Cannot load from int array because "this.connectedClientCountArray" is null
    at io.velo.SocketInspector.onConnect(SocketInspector.java:727)
    at io.activej.net.AbstractReactiveServer.doAccept(AbstractReactiveServer.java:362)
    at io.activej.net.AbstractReactiveServer.lambda$doAccept$3(AbstractReactiveServer.java:339)
    at io.activej.eventloop.Eventloop.executeTask(Eventloop.java:578)
    at io.activej.eventloop.Eventloop.executeConcurrentTasks(Eventloop.java:645)
    at io.activej.eventloop.Eventloop.run(Eventloop.java:476)
```

The connection itself succeeds (the activej server's accept loop catches the fatal and moves on),
but the per-net-worker connected-client counter never increments for that client.

## Cited files / lines

- `src/main/java/io/velo/SocketInspector.java:261` — field declaration
  ```java
  @ThreadNeedLocal(type = "net")
  int[] connectedClientCountArray;
  ```
- `src/main/java/io/velo/SocketInspector.java:308-315` — initializer
  ```java
  public void initByNetWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray, Eventloop[] netWorkerEventloopArray) {
      this.slotWorkerEventloopArray = slotWorkerEventloopArray;
      this.netWorkerEventloopArray = netWorkerEventloopArray;

      this.connectedClientCountArray = new int[netWorkerEventloopArray.length];
      this.netInBytesLengthArray = new long[netWorkerEventloopArray.length + slotWorkerEventloopArray.length];
      this.netOutBytesLengthArray = new long[netWorkerEventloopArray.length + slotWorkerEventloopArray.length];
  }
  ```
- `src/main/java/io/velo/SocketInspector.java:727` — use without null check
  ```java
  var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
  connectedClientCountArray[threadIndex]++;   // <-- NPE when array is null
  ```
- `src/main/java/io/velo/MultiWorkerServer.java:861-863` — server built with inspector BEFORE init
  ```java
  SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Consumer<ITcpSocket> consumer, Config config) {
      return SimpleServer.builder(reactor, consumer)
              .withSocketInspector(socketInspector)
  ```
- `src/main/java/io/velo/MultiWorkerServer.java:1124` — init runs AFTER the server is wired
  ```java
  socketInspector.initByNetWorkerEventloopArray(slotWorkerEventloopArray, netWorkerEventloopArray);
  ```

## Root cause

Race between server bind (which installs the inspector as a callback for incoming connections) and
`SocketInspector.initByNetWorkerEventloopArray(...)`, which allocates the per-thread counter arrays.

`MultiWorkerServer.workerServer(...)` attaches the inspector to the `SimpleServer` via
`withSocketInspector(socketInspector)`. At that moment `connectedClientCountArray` is still `null`
because `initByNetWorkerEventloopArray(...)` has not been called yet — that call happens later in
`MultiWorkerServer.startAsync()` (line 1124). If a client (or a peer repl pair in cluster mode,
which connects aggressively at startup) arrives between bind and init, `onConnect` runs against a
half-initialized inspector and dereferences the still-null array.

The single-node path usually hides it because the first client is the operator connecting manually;
but in any cluster-mode boot the peer nodes' repl-pair handshakes (and the test harness's
near-simultaneous `Jedis` pool) reliably hit the window, so every cluster test log shows the NPE.

## Impact

- Cosmetic to the wire — the accept loop survives — but `connectedClientCountArray[threadIndex]` is
  never incremented for the first connection, so the per-net-worker connected-client counter
  (`info`/metrics) is wrong by exactly one for every net worker until that worker restarts.
- The exception prints an `ERROR ... Fatal error` line per node per startup, drowning real `ERROR`s
  in the log filter.
- `replDelayCloseTimeoutSeconds` is `60` in the harness and `60` by default; on slow CI hosts the
  first peer connection can land well inside that window, so the bug fires every run.

## Why it was not caught

- The single-node default-path tests do not connect fast enough after bind to hit the window.
- The cluster tests (`ClusterMigrationFailoverManualTest`, `ClusterMigrationStressManualTest`) all
  *did* hit it, but no test grepped for it — the message text matches "NullPointerException",
  not anything the existing assertions looked for.
- The bug-review flow grep pattern (`'Repl pair as master already exists'`,
  `'ERROR'`, etc.) does not include the `SocketInspector.onConnect` message.

## Candidate fixes

- **Lazy-allocate the arrays in `onConnect`.** When `connectedClientCountArray == null`,
  fall back to a synchronized per-worker allocation (or a single-element array). Loses a bit
  of throughput but is safe.
- **Initialize the arrays in the inspector constructor / field initializer** using a constant
  size (`ConfForGlobal.netWorkers`), then `initByNetWorkerEventloopArray` becomes a resize +
  reference-assign. Requires reading `netWorkers` before the server starts.
- **Defer server bind until after `initByNetWorkerEventloopArray`.** Cleanest but the largest
  surgery — moves the SimpleServer construction order around in `MultiWorkerServer`.
- **Add a synchronized null check in `onConnect`** (`if (connectedClientCountArray == null)
  return;`). Cheap and stops the NPE; the per-worker counter stays wrong for the first
  connection until the next one initializes it via the inspector's own setup, which it never
  will under the current code, so this fix is incomplete — but it stops the log spam and the
  NPE, which is what most callers care about.

## Test regression fence

Two regression fences (both green since the round-2 review fix):

1. `src/test/groovy/io/velo/SocketInspectorTest.groovy`: the
   `test freshly constructed inspector counts without init (bug 66 regression)` test
   constructs a `SocketInspector(1, 1)` and calls `onConnect` / `onRead` /
   `onWrite` / `onDisconnect` WITHOUT calling `initByNetWorkerEventloopArray` first,
   and asserts the per-net-worker counters increment and decrement correctly
   (`connectionCount` is the AtomicInteger admission counter; `connectedClientCount()`
   is the per-worker array; `netInBytesLength()` and `netOutBytesLength()` return
   the array sums). Pre-fix this would NPE; post-fix the arrays exist from
   construction.

2. `src/test/groovy/io/velo/repl/cluster/ClusterOperationProcessManualTest.T11-churn`:
   asserts outright `!log.contains('NullPointerException')` on every node's
   `server.log` after a 2-cycle migration churn. The previous `midTestNpe(log)`
   whitelist was REMOVED in the round-2 review — any startup NPE on a cluster
   node now fails the test. (`midTestNpe` still exists as a private helper for
   other tests that might want stack-trace filtering; T11-churn uses the
   stronger "zero NPE" assertion.)

## Cross-reference

- Found while writing `ClusterOperationProcessManualTest` (process-level cluster tests) per
  `docs/plans/2026-07-07-cluster-operation-process-tests.md`. Affects every existing cluster test.
- Independent of bugs 59–65 (cluster versioning / migration lifecycle). Purely a startup net
  worker wiring race.

## Review Feedback (round 2)

**Status: fixed** via candidate #2 (constructor eager-allocation). Summary of the review that led here.

### Confirmed: this is a real product-path race, not a test artifact

The inspector is attached to the `SimpleServer` at **DI/build time** (`workerServer(...)` →
`.withSocketInspector(socketInspector)`, `MultiWorkerServer.java:861-863`), while
`initByNetWorkerEventloopArray` (which allocated the counter arrays) runs later in `onStart()` at line
1124. `onConnect` runs on a net-worker eventloop thread; `onStart` runs on the launch thread — they are
**concurrent**, so the server can accept before line 1124. The boot log confirms it: the NPE at
`17:18:46.851` precedes `startIndexHandlerPool` (line 1122) at `46.954`, i.e. it fired mid-`onStart`
before the init line. In cluster mode the peer nodes' repl handshakes connect aggressively, so it hits
essentially every boot. The `ClusterOperationProcessManualTest` `midTestNpe` whitelist existed only to
paper over this — direct evidence it shows up in real node logs.

### Why the interim null-guard fix (candidate #4) was not enough

The doc already flagged candidate #4 as "incomplete." Concretely: guarding `onConnect`/`onDisconnect`
does **not** make the counter converge — if a connection accepted pre-init disconnects post-init, the
`onConnect` increment was skipped (array null) but the `onDisconnect` decrement runs (array now non-null),
so `connectedClientCountArray` drifts to **-1** and stays there. It also adds a null-check on the hot
`onRead`/`onWrite` path and still needs an `initForTest` for unit tests. (The admission counter
`connectionCount`, an `AtomicInteger`, was always correct — only the per-worker metric drifted.)

### The fix (candidate #2)

Allocate the three counter arrays **in the constructor**, sized from the worker counts, so they are never
null once the inspector exists — before the server can accept:

- `SocketInspector(int netWorkers, int slotWorkers)` eager-allocates; the DI provider passes the config
  `netWorkers`/`slotWorkers`. A no-arg `SocketInspector()` sizes from `ConfForGlobal` (clamped ≥ 1) for
  tests.
- `initByNetWorkerEventloopArray` now only wires the eventloop references, re-allocating **only** if the
  actual worker counts differ from the constructor's — so counts accumulated in the tiny bind→init window
  are preserved (no reset) in the normal matching-size case.
- The per-I/O null-guards are removed; the counter is now correct (increment and decrement both always
  run), no drift.

### Test impact

- `SocketInspectorTest`: the bug-66 regression test is reframed to assert a freshly constructed inspector
  counts correctly **without any `init` call** (arrays exist from construction). No `initForTest` needed.
- `ClusterOperationProcessManualTest`: the `midTestNpe` whitelist is removed; the test now asserts
  **zero** `NullPointerException` in each node log outright.

---

# Bug 67 — Master creates repl pair on ping but never sends Hi, so the slave is stuck

**Module:** repl / cluster (`io.velo.command.XGroup` handleHello / handlePing)
**Round:** 1 (found by `ClusterOperationProcessManualTest` T6 / T7-TTL observing a node-3 fetch that
never starts; reproduced deterministically with `migrateSlot` + `manage migrate_from` on the
replica when the master's slot is not yet owned)
**Status:** FIXED (slave re-hello on a serve=1 pong when no Hi received yet — see "Fix" below)

## Symptom

When a migration target is the charge node of a downstream replica's slot (e.g. `migrateSlot(0,
nodes[0], nodes[1], [nodes[3]])` — node-1 imports slot 0 from node-0 and resyncs node-3 from
node-1) AND node-1's hello handler has not yet accepted the slot-0 hello (because
`isInnerSlotOwnedByMySelfShard(0)` is false on node-1 until cutover), the slave's fetch pipeline
deadlocks. node-3 connects to node-1, node-1 declines the first hello, then the second hello is
accepted (by the time `hasCaughtUpUpstream()` is true), but the master repl pair was created via
the **ping** handler (not via the **hello** handler) — and the ping handler does not send a
`Hi` to the slave. The slave never receives a `Hi`, never starts the exists-fetch, and the
F3 defer on the charge node's `setslot` blocks the cutover for the entire `waitClusterxOk`
90-second window. The test then either times out or, in some runs, the slave disconnects at
`replDelayCloseTimeoutSeconds` (60s default) and the cutover eventually completes.

## Cited files / lines

- `src/main/java/io/velo/command/XGroup.java:312-334` — ping handler creates the master pair but
  does NOT send Hi:
  ```java
  case ping -> {
      // server received ping from client
      var pingFields = Wal.keyString(contentBytes).split(Ping.FIELD_SEPARATOR, -1);
      HostAndPort.parse(pingFields[0]);
      var oneSlot = localPersist.oneSlot(slot);
      var serveSlot = localPersist.isInnerSlotOwnedByMySelfShard(slot) || oneSlot.hasCaughtUpUpstream();

      if (replPair == null) {
          if (serveSlot) {
              replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, hostAndPort.host, hostAndPort.port);
              replPair.increaseStatsCountForReplType(ping);
          }
      }
      if (replPair != null) {
          replPair.setLastPingGetTimestamp(System.currentTimeMillis());
      }
      ...
      var pongContent = new Pong(ConfForGlobal.announcedHostPortString(), clusterMyEpoch, serveSlot);
      yield replPair != null
              ? Repl.reply(slot, replPair, pong, pongContent)
              : Repl.reply(slot, slaveUuid, pong, pongContent);
  }
  ```
- `src/main/java/io/velo/command/XGroup.java:485-504` — hello handler creates the pair AND sends Hi:
  ```java
  replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, hostAndPort.host, hostAndPort.port);
  ...
  oneSlot.getDynConfig().setBinlogOn(true);
  ...
  var content = new Hi(slaveUuid, oneSlot.getMasterUuid(), currentFo, earliestFo, chunk.getSegmentIndex());
  return Repl.reply(slot, replPair, hi, content);
  ```
- `src/main/java/io/velo/command/XGroup.java:368-380` — slave's pong handler only triggers
  catch_up if `metaChunkSegmentIndex.isExistsDataAllFetched()`, which only becomes true after
  the exists-fetch completes. The exists-fetch is driven by the `Hi` (handled at
  `case hi -> hi(slot, contentBytes)`):
  ```java
  var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
  if (metaChunkSegmentIndex.isExistsDataAllFetched()) {
      var millis = replPair.getLastGetCatchUpResponseMillis();
      if (millis != 0 && System.currentTimeMillis() - millis > 1000 * 5) {
          s_catch_up(slot, new byte[2]);
      }
  }
  ```

## Root cause

The `ping` handler creates a `ReplPair as master` if the slot is servable, but does not send the
`Hi` packet (the `Hi` is sent only by the `hello` handler at line 503). The slave's `pong`
handler at line 369 only triggers `s_catch_up` if the exists-fetch is already complete, and the
exists-fetch is started by the `Hi` message. So a master pair that was born via `ping` (because
`hello` was declined earlier) leaves the slave waiting for a `Hi` that will never come — the
slave's catch-up chain is broken. The slave times out at `replDelayCloseTimeoutSeconds` (60s) and
disconnects with "exists data not all fetched", the master's `repl pair as master` is removed
by the reaper, and the F3 cutover defer on the charge node keeps blocking.

The reason this manifests on a migration import: the hello handler's decline path (line 475)
rejects a hello for a slot the master's shard does not own **AND** has not finished importing
(`!oneSlot.hasCaughtUpUpstream()`). During the import, `hasCaughtUpUpstream()` flips from false to
true asynchronously once the slave side finishes the exists-fetch on the source. The ping handler
sees the same condition (`serveSlot = isInnerSlotOwnedByMySelfShard(slot) || hasCaughtUpUpstream()`)
and creates the pair. So:

1. Slave connects, sends `hello` → master declines (no catch-up yet).
2. Master catches up from source; `hasCaughtUpUpstream()` becomes true.
3. Slave pings → master accepts, creates pair via ping, sends pong (no `Hi`).
4. Slave waits for `Hi` that never arrives.
5. F3 defer on charge node blocks cutover.

## Impact

- **Operator-visible:** migration cutover stalls for up to 60s, then completes (or, if the
  operator forces cutover before the slave disconnects, data is on the target but the replica
  is missing the slot).
- **Test-visible:** the F3 test (T6) is non-deterministic; the T7-TTL test (which adds a
  `Thread.sleep(1500)` to let a key expire) is reliably broken in this configuration.
- The race window is bounded by `replDelayCloseTimeoutSeconds` (60s default) but the bug is
  repeatable: any `migrateSlot(target, source, [replica])` with a not-yet-owned target slot
  whose shard has a configured replica hits it if the timing works out.

## Why it was not caught

- The original F3 gate tests (`test setslot defers until downstream replica is caught up`) run
  in a mocked environment with `hasCaughtUpUpstream()` deterministically set true — they do
  not exercise the ping-creates-pair path because the test rig primes the state via direct
  `createIfNotExistReplPairAsMaster` + `setAllCaughtUp(true)`.
- The pre-existing T6 (now superseded by the new `T-consistency` + `T11-MOVED` in
  `ClusterOperationProcessManualTest`) only covers the happy path where the second hello is
  accepted by the hello handler before the F3 cutover. The flake rate in CI is high enough that
  the test would usually pass.

## Candidate fixes

- **Send `Hi` from the ping handler** when it creates a new master pair. Cleanest fix.
  ```java
  if (replPair == null) {
      if (serveSlot) {
          replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, hostAndPort.host, hostAndPort.port);
          replPair.increaseStatsCountForReplType(ping);
          // ALSO send Hi so the slave can start its exists-fetch (the hello handler does this,
          // the ping handler currently doesn't, so slaves connected via the ping path are stuck)
          sendHi(slot, replPair, oneSlot);
      }
  }
  ```
  Pulls the `Hi`-construction out of `hello` into a private helper. The slave sees a Hi and
  starts the exists-fetch normally.
- **Have the slave re-send `hello` after a pong with `serveSlot=1` and no Hi received.** Bumps
  the slave's bootstrap cost (one extra round trip per re-subscription) but is self-contained
  on the slave side.
- **Inline-create the pair from `hello` whenever `serveSlot` is true** (i.e. always accept
  hello once the import catch-up completes), and let ping be a pure liveness check. Then the
  hello path always sends Hi, and ping never creates pairs. This is the most invasive but
  the most aligned with the original hello-driven protocol.

## Test regression fence

`ClusterOperationProcessManualTest.T6` exercises the bug end-to-end and now passes with a
small test-side workaround:

1. `migrateSlotUntilCaughtUp(0, nodes[0], nodes[1])` — start the import.
2. `waitClusterxOk` for the first `setslot` — observe the F3 defer in the log
   (`setslot defer: downstream replica not caught up`).
3. `manage slot 0 migrate_from localhost node-1.port force` on `node-3` (the replica) —
   resync AFTER the import catch-up completes (`hasCaughtUpUpstream` is now true on node-1,
   so the ping handler accepts the slave pair and the slave's next hello is accepted with
   a Hi).
4. `Thread.sleep(2000)` — give node-3 a moment to receive Hi and start the exists-fetch.
5. `broadcastSetSlotChargeFirst(0, nodes[1], nodes[0])` — the F3 gate now sees
   `hasDownstream && downstreamAllCaughtUp` and the cutover completes.

The `Thread.sleep` is a heuristic to give the slave's exists-fetch a head start; once the
fix lands (ping handler also sends Hi, or slave re-hellos), the sleep can be removed and
the cutover completes in <5s.

`ClusterOperationProcessManualTest.T7-TTL` migrates to `node-2` (no replica) to sidestep the
F3 gate entirely and the F3 race.

## Cross-reference

- Found while writing `ClusterOperationProcessManualTest` per
  `docs/plans/2026-07-07-cluster-operation-process-tests.md` (T6 / T7-TTL).
- Tied to the F3 durability gate fix (`49d7e646` — `setslot defers until a configured replica
  is caught up`) — the gate is correct; the failure is upstream in the slave-bootstrap
  pipeline.

## Review Feedback (round 2) — Bug 67

**Status: CONFIRMED real.** Verified every link in the chain against the current code:

| Claim | Verified at | Result |
|---|---|---|
| ping handler creates the master pair but sends **no Hi** | `XGroup.java:326-341` — `createIfNotExistReplPairAsMaster` then only a `Pong` | ✓ |
| `Hi` is sent **only** from the hello handler | grep: `new Hi(` appears solely at `XGroup.java:503` | ✓ |
| hello is declined during import | `XGroup.java:475-478` — `clusterEnabled && !isInnerSlotOwnedByMySelfShard && !hasCaughtUpUpstream` → `emptyReply`, no pair created, connection stays open | ✓ |
| slave never re-hellos | `initAsSlave` sends hello once on connect (`ReplPair.java:443`); the periodic slot task only calls `replPair.ping()` (`OneSlot.java:1432`), reusing the same `tcpClient` — no re-hello, and the decline doesn't close the socket so no reconnect | ✓ |
| pong path can't self-recover for a fresh slave | `XGroup.java:368-377` triggers `s_catch_up` only when `isExistsDataAllFetched()`; a force-resync slave has that flag `0`/false (`MetaChunkSegmentIndex.java:147`, `clear()` sets it false) | ✓ |
| `hi()` is what starts the exists-fetch | `hi()` sets `masterUuid` + master binlog offsets and drives the fetch chain; without it the slave never begins | ✓ |

**Conclusion:** a master pair born via the `ping` path (because an earlier hello was declined mid-import)
leaves a fresh slave with no `Hi` and no recovery route — the exists-fetch never starts. The root cause
and impact are accurately described. It self-heals after `replDelayCloseTimeoutSeconds`: the slave
disconnects, reconnects, and its fresh hello is now accepted because `hasCaughtUpUpstream()` is true — so
it is a **~60s stall, not a permanent deadlock**, consistent with the impact section above.

### Implementation caveats for the candidate fixes

1. **Candidate #1 (send Hi from ping)** — send the `Hi` inside the `if (replPair == null)` block so it
   fires **once at creation**, not on every ping tick (otherwise the slave restarts its exists-fetch every
   second). The extracted `sendHi` helper must carry the **full** hello setup — `setBinlogOn(true)`,
   `append(XSkipApply)`, and `currentFo/earliestFo` (`XGroup.java:490-503`) — not just `new Hi(...)`, or the
   master will not actually stream. The ping path also never calls `setRemoteReplProperties` (the ping
   payload carries no repl properties); a minor gap the later reconnect-hello closes.
2. **Candidate #3 (always accept hello once `serveSlot`, make ping a pure liveness check)** is the cleaner
   long-term shape — it removes the dual pair-creation path, which is the underlying design smell. More
   invasive, but preferable if this area is touched beyond a point fix.

### Fix (implemented) — slave re-hello, a refinement of candidate #2

Candidate #1 (send Hi from the ping handler) is **insufficient on its own**: the master needs the slave's
`remoteReplProperties` to serve any `exists_*` fetch (`XGroup.requireRemoteReplProperties`, used in
`exists_wal` / `exists_chunk_segments` / `exists_big_string` / `exists_short_string`), and those properties
are only carried by the **hello** payload — the ping payload does not have them. A ping-created pair that
merely received a Hi would still throw `remote repl properties missing` at the first fetch. So the pair must
be completed by a **hello**, not a ping.

The fix makes the slave re-send hello over its existing connection when it detects it is stuck:

- In the slave's `pong` handler (`XGroup.java`), when cluster is enabled, the pong says the master serves
  the slot (`serve=1`), and `replPair.getMasterUuid() == 0` (no Hi ever received — the slave pair only sets
  `masterUuid` in `hi()`; `createReplPairAsSlave` never sets it), the handler returns a `hello` reply
  instead of an empty reply. The slave's request/response path writes that hello back to the master.
- The master (now able to serve — `serve=1` means `isInnerSlotOwnedByMySelfShard || hasCaughtUpUpstream`,
  so its hello handler no longer declines) accepts the hello: it finds the existing ping-created pair,
  records `remoteReplProperties`, and replies with the `Hi` that bootstraps the exists-fetch.
- The `masterUuid == 0` guard means this fires **only before the first Hi**, never mid-fetch or after
  catch-up, so a legitimate in-progress sync is never disturbed (once `hi()` runs, `masterUuid != 0`).

This keeps the ping handler unchanged (still a pure liveness + pair-adopt path) and needs no new wire field:
the recovery reuses the existing hello → Hi handshake, which already sets everything the master needs.

**Regression fence:** `XGroupTest.'test as slave handle pong re-hellos when master serves but no hi
received yet (bug 67)'` — a serve=1 pong with `masterUuid == 0` returns a `hello` reply; once `masterUuid`
is set it returns empty; with cluster disabled it returns empty. The `ClusterOperationProcessManualTest`
T6 `Thread.sleep(2000)` head-start heuristic is no longer required for correctness.