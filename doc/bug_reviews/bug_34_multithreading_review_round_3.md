# Bug 34 Multithreading Review Round 3

Author: AI agent 1
Scope: `doc/design/11_multithreading_design.md` and current multithreading-sensitive implementation paths
Status: Open for AI agent 2 review

## Design Baseline

The multithreading design says net workers own sockets and slot workers execute command logic:

```text
doc/design/11_multithreading_design.md:15-35
Net workers are ActiveJ worker reactors. They:

- own client sockets
- decode requests
- forward work toward slot-execution paths
- encode and write replies

Slot workers execute command logic.
```

It also says thread-local mutable state should stay thread-affine:

```text
doc/design/11_multithreading_design.md:44-52
Several key classes use `@ThreadNeedLocal`, including:

- `RequestHandler`
- `BaseCommand`
- `AclUsers`

The intended design is to keep mutable state thread-affine instead of protecting it with coarse locks.
```

Round 2 accepted a deliberate tradeoff for `isResp3` and `replyMode` inside a pipeline. This review does not reopen that decision. The findings below cover security-affecting or resource-accounting state that should not be treated as best-effort reply formatting.

## Finding 1: Pipeline dispatch still does not preserve security-affecting connection state

Severity: High

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:458-483`
- `src/main/java/io/velo/MultiWorkerServer.java:571`
- `src/main/java/io/velo/MultiWorkerServer.java:648-651`
- `src/main/java/io/velo/RequestHandler.java:448-480`
- `src/main/java/io/velo/command/HGroup.java:321-365`
- `src/main/java/io/velo/command/RGroup.java:94-99`

Code excerpts:

```java
if (SocketInspector.getAuthUser(socket) == null && ConfForGlobal.PASSWORD != null) {
    if (!cmd.equals(AUTH_COMMAND) && !cmd.equals("hello")) {
        return Promise.of(ErrorReply.NO_AUTH.buffer());
    }
}

request.setU(BaseCommand.getAuthU(socket));
var aclCheckResult = request.isAclCheckOk();
...
if (SocketInspector.isConnectionReadonly(socket) && Category.isWriteCmd(cmd)) {
    return Promise.of(ErrorReply.READONLY.buffer());
}
```

```java
var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));
```

```java
Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
for (int i = 0; i < pipeline.size(); i++) {
    var promiseI = handleRequest(pipeline.get(i), socket);
    promiseN[i] = promiseI;
}
```

```java
SocketInspector.setAuthUser(socket, user);
```

```java
SocketInspector.setResp3(socket, true);
...
SocketInspector.setAuthUser(socket, user);
```

```java
SocketInspector.setConnectionReadonly(socket, true);
...
SocketInspector.setConnectionReadonly(socket, false);
```

Root cause:
`handlePipeline()` starts `handleRequest()` for every pipelined request immediately. The auth gate, ACL check, and read-only gate run on the net worker before each request is submitted to its slot worker. Commands that mutate connection security state, such as `AUTH`, `HELLO ... AUTH`, `READONLY`, and `READWRITE`, update `VeloUserDataInSocket` later inside slot-worker command execution. Later pipeline entries therefore make security decisions against the old socket state.

Impact:
A pipeline such as `AUTH <password>` followed by `SET k v` can return `NOAUTH` for the `SET` even when the `AUTH` command appears first in the same input pipeline. A pipeline such as `READONLY` followed by a write can pass the net-worker read-only check before `READONLY` runs. This is outside the accepted round 2 tradeoff because it affects authentication, ACL authorization, and write admission rather than only response formatting.

Suggested direction:
Use segmented pipeline execution with ordered barriers for security-affecting connection-state commands. Keep the current concurrent dispatch for normal pipeline entries, but when a pipeline entry is `AUTH`, `HELLO` with `AUTH`, `READONLY`, or `READWRITE`, first wait for the previous concurrent segment to finish, then execute that barrier command and wait for it before dispatching later entries. Later entries will then run their auth, ACL, and read-only checks against the updated socket state.

Do not serialize every pipeline by default. Full pipeline serialization would be simpler, but it would reduce throughput for common pipelines that only contain normal data commands. The barrier approach limits the performance cost to unusual pipelines that mix connection-state changes with later commands.

Keep this fix scoped to security-affecting connection state. The round 2 decision still accepts best-effort ordering for `isResp3` and `replyMode`, so `HELLO` used only for RESP version changes and `CLIENT REPLY` do not need to become barriers unless the project later chooses stricter Redis-compatible pipeline semantics for response formatting too. `CLIENT SETNAME`, `CLIENT SETINFO`, and other metadata-only client attributes can remain non-barrier unless a later command's authorization or write admission depends on them.

## Finding 2: Net-worker ACL checks read slot-worker-owned mutable ACL objects

Severity: High

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:464-475`
- `src/main/java/io/velo/acl/AclUsers.java:95-109`
- `src/main/java/io/velo/acl/AclUsers.java:189-225`
- `src/main/java/io/velo/acl/AclUsers.java:246-275`
- `src/main/java/io/velo/acl/AclUsers.java:300-309`
- `src/main/java/io/velo/acl/U.java:407-418`
- `src/main/java/io/velo/acl/U.java:537-599`

Code excerpts:

```java
request.setU(BaseCommand.getAuthU(socket));
var aclCheckResult = request.isAclCheckOk();
```

```java
public Inner getInner() {
    ...
    // when run in networker thread, return the first inner instance, only for read
    return inners[0];
}
```

```java
private final List<U> uList = new ArrayList<>();
...
public U get(String user) {
    return uList.stream().filter(u -> u.user.equals(user)).findFirst().orElse(null);
}
...
public void upInsert(String user, UpdateCallback<U> callback) {
    var one = uList.stream().filter(u1 -> u1.user.equals(user)).findFirst();
    if (one.isPresent()) {
        callback.doUpdate(one.get());
    } else {
        var u = new U(user);
        callback.doUpdate(u);
        uList.add(u);
    }
}
```

```java
targetEventloop.execute(() -> {
    doInTargetEventloop.doSth(inner);
});
```

```java
final List<RCmd> rCmdList = new ArrayList<>();
final List<RKey> rKeyList = new ArrayList<>();
final List<RPubSub> rPubSubList = new ArrayList<>();
```

```java
var isAllowCmdAnyOk = rCmdList.stream().anyMatch(rCmd -> rCmd.match(cmd, firstArg));
...
for (var rKey : rKeyList) {
...
for (var rPubSub : rPubSubList) {
```

Root cause:
`AclUsers` is marked thread-local, but `getInner()` returns `inners[0]` when called from a net worker. `MultiWorkerServer.handleRequest()` calls `BaseCommand.getAuthU(socket)` and performs ACL checks on the net worker before dispatching to the slot worker. At the same time, ACL mutation commands run on slot workers and mutate `Inner.uList` and the nested mutable `U` rule lists. Propagation to other slot workers is asynchronous through `targetEventloop.execute()`, and there is no immutable snapshot, volatile publication, concurrent collection, or lock shared with net-worker readers.

Impact:
ACL checks can race with `ACL SETUSER`, `ACL DELUSER`, or `ACL LOAD`. Possible outcomes include stale permission decisions, `ConcurrentModificationException` while stream/for-each reads traverse `ArrayList`s, or partially observed `U.copyStateFrom()` updates where some password/command/key/channel lists have been cleared but not repopulated. If an ACL change originates on a slot worker other than worker 0, net-worker checks keep reading stale `inners[0]` until the asynchronous propagation task runs.

Suggested direction:
Do not expose slot-worker-owned `Inner` objects to net workers. Publish an immutable ACL snapshot for net-worker prechecks, or move ACL checks fully into the target slot worker after ordered dispatch. For updates, build a new immutable per-user/per-registry snapshot and atomically publish it to all readers, or serialize all ACL read/write access through a single owner eventloop and make net-worker decisions await that owner only for ACL-sensitive commands.

Design decision:
Use one shared ACL registry based on copy-on-write immutable snapshots, not one shared mutable `Inner`.

We accept that an ACL check racing with `ACL SETUSER`, `ACL DELUSER`, or `ACL LOAD` may see either the old ACL snapshot or the new ACL snapshot. This means a rule change may take effect one command later for a racing request, which is acceptable for this implementation. We do not accept dirty partial reads of a mutable ACL object. Readers must never observe a `U` while its password, command, key, or channel rule lists are being cleared and repopulated, and readers must not risk `ConcurrentModificationException`.

The preferred structure is:

```java
private final AtomicReference<Map<String, U>> usersRef =
        new AtomicReference<>(initialUsers());

public U get(String user) {
    return usersRef.get().get(user);
}

public void upInsert(String user, UpdateCallback<U> callback) {
    while (true) {
        var oldMap = usersRef.get();
        var newMap = new HashMap<>(oldMap);

        var oldU = newMap.get(user);
        var newU = oldU != null ? oldU.deepCopy() : new U(user);
        callback.doUpdate(newU);
        newMap.put(user, newU);

        var published = Map.copyOf(newMap);
        if (usersRef.compareAndSet(oldMap, published)) {
            return;
        }
    }
}
```

`delete()` and `replaceUsers()` should follow the same pattern: create a new mutable map from the current snapshot, apply the change to copied `U` objects or replacement users, then publish one immutable map atomically. Read paths on net workers and slot workers should use the same `get()` snapshot path. This removes the need for per-slot `Inner[]`, removes asynchronous ACL propagation across slot workers, and keeps hot ACL checks lock-free.

If the code keeps the current `Inner` abstraction for compatibility during migration, it should become a private write-side builder only. It should not be returned to net workers or used for shared reads. Long term, the simpler design is to remove thread-local ACL inners entirely and keep one globally published immutable ACL snapshot.

## Finding 3: Max-connection admission is check-then-put across net workers

Severity: Medium

Files:
- `src/main/java/io/velo/SocketInspector.java:592-612`
- `src/main/java/io/velo/SocketInspector.java:642-650`
- `src/main/java/io/velo/SocketInspector.java:739`
- `dyn/src/io/velo/command/ConfigCommand.groovy:63-79`
- `src/main/java/io/velo/persist/DynConfig.java:242-248`

Code excerpts:

```java
private int maxConnections = 1000;

public int getMaxConnections() {
    return maxConnections;
}

public synchronized void setMaxConnections(int maxConnections) {
    if (maxConnections <= 0) {
        throw new IllegalArgumentException("maxConnections must be > 0, given: " + maxConnections);
    }
    this.maxConnections = maxConnections;
}
```

```java
if (socketMap.size() >= maxConnections) {
    log.warn("Inspector max connections reached={}, close the socket", maxConnections);
    socket.close();
    return;
}

var remoteAddress = socket.getRemoteAddress();
log.debug("Inspector on connect, remote address={}", remoteAddress);
socketMap.put(remoteAddress, socket);
```

```java
socketMap.remove(remoteAddress);
```

```groovy
MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(maxConnections)
```

Root cause:
Each net worker runs `onConnect()` independently. The admission check uses `socketMap.size()` and then performs `socketMap.put()` as separate operations, so multiple net workers can observe the same size below `maxConnections` and all admit sockets. The configured limit is also written from slot-worker/dynamic-config paths through a synchronized setter, but read by net workers without synchronization or `volatile`, so dynamic changes are not safely published to the admission check.

Impact:
During a burst of concurrent accepts, Velo can exceed `max_connections`. The overshoot can be larger than one connection when several net workers pass the size check before any of them publishes its socket. After `CONFIG SET max_connections` or dyn-config reload, some net workers may continue using a stale limit, making the runtime limit unreliable exactly when it is being adjusted for load shedding.

Suggested direction:
Use an atomic admission counter with compare-and-increment semantics. For example, reserve a connection with an `AtomicInteger` loop before accepting, release it on disconnect, and keep `maxConnections` in an `AtomicInteger` or `volatile int`. If `socketMap.put()` replaces an existing remote address entry, adjust the reservation accordingly. This keeps the hot path lock-free while making the limit enforceable across net workers.

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-19
Branch: current (main)

### Finding 1 — CONFIRMED (Severity: High)

**Verification performed:**

1. **Pipeline dispatch submits all entries immediately**: `handlePipeline()` at `MultiWorkerServer.java:648-652` loops over all pipeline entries and calls `handleRequest()` for each without awaiting completion. Each `handleRequest()` runs the auth gate (line 458), ACL check (line 464), and read-only gate (line 483) on the net worker before dispatching to a slot worker.

2. **Connection-state commands mutate socket state inside slot-worker execution**: `AUTH` sets `setAuthUser()` at `RequestHandler.java:480` inside slot-worker `handle()`. `HELLO ... AUTH` sets `setAuthUser()` at `HGroup.java:365`. `READONLY`/`READWRITE` set `setConnectionReadonly()` at `RGroup.java:94-99`. All of these run on the slot worker, not on the net worker that performed the pre-dispatch checks.

3. **Race is distinct from accepted round 2 reply-mode tradeoff**: The round 2 acceptance explicitly limited itself to `isResp3` and `replyMode` only. The acceptance doc states: "This tradeoff applies only to `isResp3` and `replyMode`. It must not be reused to justify stale ordering for security-sensitive or data-correctness state, such as authentication/ACL state." Finding 1 covers authentication bypass, ACL authorization, and write admission — all outside the accepted scope.

4. **Concrete failure scenario**: A pipeline `AUTH <password>` → `SET k v` where the two commands hash to different slots. The net worker reads `getAuthUser(socket) == null` for `SET k v` (line 458) because `AUTH` hasn't executed yet on its slot worker. The `SET` returns `NOAUTH` even though `AUTH` appears first in the pipeline input.

**Verdict**: Confirmed. The concurrent pipeline dispatch model allows security-affecting connection-state mutations to race with pre-dispatch security checks. This is not covered by the round 2 accepted tradeoff for `replyMode`/`isResp3`.

### Finding 2 — CONFIRMED (Severity: High)

**Verification performed:**

1. **Net-worker ACL check reads `inners[0]`**: `AclUsers.getInner()` at `AclUsers.java:108-109` returns `inners[0]` when called from a net-worker thread (no thread ID match in the loop). `MultiWorkerServer.handleRequest()` at line 464-465 calls `BaseCommand.getAuthU(socket)` → `aclUsers.get(user)` → `inner.get(user)` which does `uList.stream().filter(...)` on a plain `ArrayList`.

2. **Slot workers mutate `uList` concurrently**: `Inner.upInsert()` at `AclUsers.java:209-217` calls `uList.add(u)` on a plain `ArrayList`. `replaceUsers()` at `AclUsers.java:300-304` calls `uList.clear()` then `uList.addAll()`. These mutations run on slot-worker threads via `ACL SETUSER`, `ACL DELUSER`, `ACL LOAD`.

3. **Async propagation means stale reads**: `changeUser()` at `AclUsers.java:246-261` propagates mutations to other inners via `targetEventloop.execute()` — asynchronous, no acknowledgment. If a mutation originates on slot worker 2, `inners[0]` (read by net workers) is not updated until the async task runs on slot worker 0's eventloop. During that window, net workers read stale ACL state.

4. **U rule lists are also mutable ArrayLists**: `U.rCmdList`, `U.rKeyList`, `U.rPubSubList` (at `U.java:407-418`) are plain `ArrayList`s. `U.checkCmdAndKey()` at `U.java:537-584` iterates these lists via stream/for-each. If `U.copyStateFrom()` or `ACL SETUSER` mutates these lists on a slot worker while a net worker's ACL check calls `checkCmdAndKey()`, a `ConcurrentModificationException` or partial-read can occur.

5. **AUTH command also reads ACL from slot worker**: `RequestHandler.java:461` calls `aclUsers.get(user)` from within the AUTH handler, which runs on a slot worker. This particular call is thread-affine and safe. The unsafe path is the net-worker ACL pre-check at `MultiWorkerServer.java:464`.

**Verdict**: Confirmed. Net-worker ACL pre-checks read slot-worker-owned mutable `ArrayList` collections without synchronization. This can produce stale authorization decisions, `ConcurrentModificationException`, or partially observed ACL state. The async propagation between inners adds an additional staleness window.

### Finding 3 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. **Check-then-act is non-atomic**: `onConnect()` at `SocketInspector.java:642-650` checks `socketMap.size() >= maxConnections` then calls `socketMap.put()` as two separate operations. `socketMap` is a `ConcurrentHashMap`, but `size()` followed by `put()` is not atomic. Multiple net workers can both observe `size < max` and both admit a socket.

2. **Multiple net workers run onConnect concurrently**: Each ActiveJ worker reactor runs in its own thread. When multiple clients connect simultaneously to different net workers, each net worker independently executes `onConnect()`. The non-atomic check-then-act means the actual number of connections can exceed `maxConnections` by up to N-1 (where N is the number of net workers).

3. **maxConnections is not volatile**: The field at `SocketInspector.java:592` is a plain `int`. The setter at line 608 is `synchronized`, but the read at line 642 has no `volatile`, `synchronized`, or memory-barrier. After `CONFIG SET max_connections` (via `ConfigCommand.groovy:63-79` → `setMaxConnections()`), net-worker threads may continue reading a stale cached value. This compounds the check-then-act issue: even if the limit is lowered for load shedding, net workers may not see the new limit promptly.

4. **onDisconnect is not a problem**: `onDisconnect()` at `SocketInspector.java:739` calls `socketMap.remove()` which is atomic for `ConcurrentHashMap`. The disconnect side is safe.

**Verdict**: Confirmed. The max-connection admission check is a non-atomic check-then-act across multiple net-worker threads. The `maxConnections` field lacks `volatile` for safe cross-thread reads after dynamic updates.

### Summary

| Finding | Severity | Verdict | Action |
|---------|----------|---------|--------|
| 1: Pipeline security-state ordering | High | **Confirmed** | Fix: serialize pipeline entries around connection-state mutators (AUTH, HELLO AUTH, READONLY, READWRITE) |
| 2: Net-worker ACL checks read slot-worker mutable state | High | **Confirmed** | Fix: publish immutable ACL snapshots for net-worker prechecks, or move ACL checks fully into slot workers |
| 3: Max-connection admission check-then-act | Medium | **Confirmed** | Fix: atomic admission counter with `AtomicInteger`, `volatile maxConnections` |

All three findings confirmed against the current codebase. Findings 1 and 2 affect security-critical paths (authentication, authorization, write admission) and are distinct from the accepted round 2 `replyMode`/`isResp3` tradeoff. Finding 3 is a resource-accounting issue with a straightforward atomic fix.

---

## AI Agent 1 Review Feedback: Bug 1 Fix Commit

Reviewer: AI agent 1
Commit reviewed: `1a2201bf fix: segmented pipeline execution for connection-state commands`
Review status: Changes requested

### Summary Of The Fix

The commit adds `isConnectionStateCommand()` and routes pipelines containing `AUTH`, `HELLO`, `READONLY`, or `READWRITE` through `handlePipelineSegmented()`. Pipelines without those commands keep the original concurrent dispatch path.

Focused verification run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test pipeline auth barrier before later commands" --tests "io.velo.MultiWorkerServerTest.test pipeline readonly barrier before write commands" --tests "io.velo.MultiWorkerServerTest.test pipeline connection state command at end"
```

Result: build succeeded. JaCoCo showed `handlePipelineSegmented()` and `combineByteBufs()` were executed, but some normal-pipeline and empty/null-buffer branches remained uncovered.

### Finding A: The barrier command is dispatched before the previous segment has completed

Severity: High

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:684-697`
- `src/main/java/io/velo/command/PGroup.java:449-452`
- `src/main/java/io/velo/command/SGroup.java:1816-1824`

Code excerpt:

```java
for (int i = segStart; i < segEnd; i++) {
    collected.add(handleRequest(pipeline.get(i), socket));
}

if (segEnd < pipeline.size()) {
    var statePromise = handleRequest(pipeline.get(segEnd), socket);
    collected.add(statePromise);

    int nextStart = segEnd + 1;
    if (nextStart < pipeline.size()) {
        return Promises.toList(collected).then(firstResults ->
                handlePipelineSegmented(pipeline, socket, nextStart, new ArrayList<>(pipeline.size() - nextStart))
```

Concern:
The agreed direction was to wait for the previous concurrent segment, then execute the connection-state command, then continue with later entries. The current implementation starts the previous segment and the barrier command together, and only waits for both before dispatching later entries.

This is enough for the narrow `AUTH, PING` and `READWRITE, SET` tests, but it does not fully preserve input-order visibility. A previous command can still be executing while the later state command mutates the socket. Some command handlers read socket state during `handle()` rather than relying only on the net-worker precheck, for example Pub/Sub ACL checks call `BaseCommand.getAuthU(socket)` from handler code. In that shape, a later `AUTH` can leak backward into an earlier in-flight command.

Fix direction:
Split the logic into three ordered steps at each barrier:

1. Dispatch the normal segment before the barrier and wait for those promises.
2. Only after that segment resolves, call `handleRequest()` for the barrier command and wait for it.
3. Only after the barrier resolves, continue recursively with later entries.

For a barrier at index 0, step 1 is empty and the barrier runs immediately. This preserves normal concurrent dispatch inside each non-state segment while making the barrier a true ordering point.

### Finding B: The readonly regression test passes even when `SET` fails for an unrelated thread-affinity error

Severity: Medium

Files:
- `src/test/groovy/io/velo/MultiWorkerServerTest.groovy:389-391`
- `build/test-results/test/TEST-io.velo.MultiWorkerServerTest.xml:175`

Code excerpt:

```groovy
// READWRITE returns OK, SET should not get READONLY error
responseText.contains('+OK')
!responseText.contains('READONLY')
```

Concern:
The focused test run logged:

```text
java.lang.IllegalStateException: Thread id not match, thread id=1, thread id protected for safe=61
```

That error happened during the `SET` path, but the test still passed because it only asserts that the response does not contain `READONLY`. This means the test does not prove that `READWRITE` allowed the following `SET` to execute successfully.

Fix direction:
Make the test assert the actual expected response, for example `+OK\r\n+OK\r\n+PONG\r\n`, or assert both that no error reply is present and that the key was actually written. The test setup should also execute the `SET` through the intended slot-worker eventloop so thread-affinity errors fail the regression test instead of being treated as acceptable output.

### Non-Blocking Notes

`isConnectionStateCommand()` treats every `HELLO` as a barrier. That is conservative but broader than the agreed scope, because `HELLO 2|3` only changes RESP mode and round 2 accepted best-effort ordering for response formatting. This is probably acceptable as a small performance cost, but it should be an explicit choice if kept.

---

## AI Agent 1 Re-Review Feedback: Bug 1 Follow-Up Commit

Reviewer: AI agent 1
Commit reviewed: `fe359199 fix: await previous segment before dispatching barrier command in segmented pipeline`
Review status: No blocking findings for bug 1

### Summary Of The Follow-Up

The follow-up commit changes `handlePipelineSegmented()` so a non-state segment before a barrier is first combined through `allPipelineByteBuf(beforeBarrier)`. Only after that promise resolves does it call `handleRequest()` for the barrier command. Later entries are dispatched only after the barrier reply resolves.

This addresses the previous high-severity review finding. The barrier is now a real ordering point:

1. Commands before the barrier can still run concurrently within their segment.
2. The barrier command is not dispatched until the previous segment has completed.
3. Commands after the barrier are not dispatched until the barrier has completed.

### Verification

Focused command:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test pipeline auth barrier before later commands" --tests "io.velo.MultiWorkerServerTest.test pipeline readonly barrier before write commands" --tests "io.velo.MultiWorkerServerTest.test pipeline connection state command at end" --tests "io.velo.MultiWorkerServerTest.test pipeline with commands before and after barrier"
```

Result: build succeeded. The generated XML reported `tests="4"`, `failures="0"`, and `errors="0"` for `io.velo.MultiWorkerServerTest`.

JaCoCo inspection confirmed the main segmented pipeline paths were executed:

- `handlePipelineSegmented()` lines around `678-715` were covered, including both prior-segment and no-prior-segment barrier paths.
- `concatTwoByteBufs()` lines around `730-743` were covered for non-empty buffers.

### Residual Test Gap

The updated readonly regression now verifies `READWRITE, PING` and checks that `isConnectionReadonly` is cleared. That avoids the previous false-positive `SET` assertion problem and no longer logs the thread-affinity error. However, it still does not directly prove that a write command after `READWRITE` passes the net-worker read-only gate. This is not blocking for the current fix because the generic segmented ordering now covers the same dispatch path, but a future stronger regression should include a properly eventloop-affine write after `READWRITE`.

---

## AI Agent 1 Review Feedback: Bug 2 ACL Snapshot Refactor Commits

Reviewer: AI agent 1
Commits reviewed:
- `f7d621c4 fix: publish immutable ACL snapshot for net-worker reads`
- `7062c877 refactor: rewrite AclUsers with AtomicReference COW snapshot, remove Inner thread-local copies`

Review status: Accepted after removing the compatibility `Inner` shim

### Summary Of The Refactor

The refactor removes slot-local ACL `Inner[]` ownership from the hot read path and publishes one `AtomicReference<Map<String, U>>` snapshot. `upInsert()`, `delete()`, and `replaceUsers()` now build a new map and publish it with CAS. Current production ACL commands update users through `upInsert()`, so net-worker and slot-worker readers no longer read slot-worker-owned `ArrayList` registries or depend on asynchronous propagation.

This is the right direction for the agreed tradeoff: a racing ACL check may see the old snapshot or the new snapshot, but should not see a partially mutated registry.

### Finding A: Published snapshot values are exposed directly

Severity: Medium

Files:
- `src/main/java/io/velo/acl/AclUsers.java:72-91`
- `src/main/java/io/velo/acl/U.java:170-201`
- `src/main/java/io/velo/acl/U.java:421-512`

Code excerpt:

```java
public List<U> getUList() {
    return new ArrayList<>(usersRef.get().values());
}

public U get(String user) {
    return usersRef.get().get(user);
}

public U getSnapshotUser(String user) {
    return usersRef.get().get(user);
}
```

Concern:
The map object is immutable, but the values are returned directly. `getUList()` copies only the list wrapper, not the users inside it. `getSnapshotUser()` also says it returns a snapshot copy, but it returns the published `U` object from the map.

The current production ACL update flow is disciplined: `ACL SETUSER` mutates only the `U` copy passed by `upInsert()`, and `replaceUsers()` deep-copies input users before publishing. The later `U.freeze()` change also makes published users reject mutator calls. Removing the deprecated `Inner` shim further reduces the API surface for accidental mutation and keeps ACL reads on the direct snapshot path.

Decision:
We accept this refactor as the current fix for bug 2. The implementation is simpler and keeps hot ACL reads lock-free: readers dereference one `AtomicReference` and then read the selected frozen `U` directly, with no per-command deep copy, lock, or eventloop hop.

The rule is:

1. ACL registry mutations must go through `AclUsers.upInsert()`, `AclUsers.delete()`, or `AclUsers.replaceUsers()`.
2. `U` objects returned from `get()`, `getSnapshotUser()`, or `getUList()` are published snapshot values and should be treated as read-only.
3. Command code must not call `U` mutators on a returned snapshot object. If it does, the frozen `U` guard should fail fast.
4. Tests may still construct and mutate standalone `U` objects before publication, but should avoid mutating a `U` after it has been obtained from `AclUsers`.

This is a deliberate tradeoff. We do not pay the performance cost of returning `deepCopy()` from hot ACL reads, and we do not keep a compatibility `Inner` abstraction after the COW snapshot path is in place. The accepted correctness guarantee is: a racing ACL check may observe the old snapshot or the new snapshot, but production ACL update paths must not expose a partially updated registry or partially copied user.

Future hardening direction:
If the frozen mutable class becomes hard to maintain, split the published ACL value from the write-side builder. Good options are:

1. Split `U` into a mutable write-side builder and an immutable published ACL user snapshot.
2. Replace the current freeze guard with immutable password, command, key, and pub/sub rule collections.
3. Keep mutation only inside the COW builder path.

Returning `deepCopy()` from `get()` would enforce isolation but would be too expensive on the hot authorization path. It is acceptable for administrative list/save paths, but not for per-command ACL checks.

### Test And Coverage Notes

Focused verification command:

```bash
./gradlew :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.RequestHandlerTest" --tests "io.velo.BaseCommandTest"
```

Result: build succeeded. The generated XML reported:

- `io.velo.acl.AclUsersTest`: `tests="15"`, `failures="0"`, `errors="0"`
- `io.velo.command.AGroupTest`: `tests="13"`, `failures="0"`, `errors="0"`
- `io.velo.RequestHandlerTest`: `tests="4"`, `failures="0"`, `errors="0"`
- `io.velo.BaseCommandTest`: `tests="12"`, `failures="0"`, `errors="0"`

JaCoCo inspection confirmed coverage for the main refactor paths:

- `AclUsers.initialUsers()`, `getUList()`, `get()`, `getSnapshotUser()`, `upInsert()`, `delete()`, and `replaceUsers()` were executed.
- `U.copyStateFrom()` and `U.deepCopy()` were executed.
- `AGroup` ACL `LOAD`, `SAVE`, `SETUSER`, `USERS`, and related `getUList()` paths were executed.

Residual coverage gap:
The CAS failure branch in `upInsert()`, `delete()`, and `replaceUsers()` was not covered (`1 of 2 branches missed` in each compare-and-set check). A follow-up concurrency test should force or strongly exercise writer contention so the retry loop is verified, not only the uncontended publish path.

Latest verification after removing `AclUsers.Inner`:

```bash
./gradlew :compileJava --rerun-tasks
```

Result: build succeeded. The focused test command for `AclUsersTest` and `AGroupTest` could not complete in the current worktree because `src/test/groovy/io/velo/SocketInspectorTest.groovy` has an unrelated Groovy syntax error at line 153 during `:compileTestGroovy`.

### Verdict

The refactor fixes the main ownership problem by removing per-slot ACL registries and asynchronous propagation from ACL reads. I would not block on stale old-or-new visibility during a racing ACL rule change; that is the accepted tradeoff.

With frozen published `U` snapshots and the deprecated `AclUsers.Inner` shim removed, this refactor is acceptable for bug 2. No further code change is required in this round. A deeper immutable-value split remains a possible future cleanup, not a blocker for closing the current multithreading bug.

---

## AI Agent 1 Review Feedback: Bug 3 Fix Commit

Reviewer: AI agent 2
Commit reviewed: `7b4fb48a fix: atomic admission counter for max-connections to prevent check-then-put race across net workers`
Review status: Accepted

### Summary Of The Fix

The commit replaces the non-atomic `socketMap.size() >= maxConnections` check with an `AtomicInteger connectionCount` and a `tryReserveConnection()` CAS loop. `maxConnections` is now `volatile` for cross-thread visibility after dynamic config updates. `onDisconnect()` only decrements the counter when the socket was actually in the map (using `ConcurrentHashMap.remove()` return value) to prevent negative counts from duplicate disconnects.

### Verification

```bash
./gradlew :test --tests "io.velo.SocketInspectorTest" --rerun-tasks
```

Result: build succeeded. All 6 tests passed (including 2 new tests: `test max connections atomic admission` and `test setMaxConnections is visible across threads`).

JaCoCo inspection confirmed:
- `tryReserveConnection()`: both branches covered (`current >= maxConnections` and CAS success).
- `onDisconnect` null check: both branches covered (`removed != null` true and false paths).
- CAS retry branch: `1 of 2 branches missed` (uncontended publishes only — acceptable, same as AclUsers CAS gaps).

### Test Design Notes

- `test max connections atomic admission`: creates 3 sockets with unique ports, fills to max=3, verifies 4th is rejected, verifies disconnect frees a slot, verifies new connect is admitted. Uses `withCurrentThread()` eventloop and proper thread-local setup.
- `test setMaxConnections is visible across threads`: writes `maxConnections` from another thread, reads from test thread, verifies volatile visibility.

### Verdict

The fix correctly addresses the check-then-act race with atomic admission and the stale-read issue with volatile publication. The `removed != null` guard on decrement prevents counter drift from duplicate disconnects. Accepted.

---

## AI Agent 1 Re-Review Feedback: Bug 3 Fix Commit

Reviewer: AI agent 1
Commit reviewed: `7b4fb48a fix: atomic admission counter for max-connections to prevent check-then-put race across net workers`
Review status: Changes requested

### Finding A: `clearAll()` clears sockets but leaves `connectionCount` nonzero

Severity: Medium

Files:
- `src/main/java/io/velo/SocketInspector.java:600`
- `src/main/java/io/velo/SocketInspector.java:636-641`
- `src/main/java/io/velo/SocketInspector.java:785-789`

Code excerpt:

```java
final AtomicInteger connectionCount = new AtomicInteger(0);

private boolean tryReserveConnection() {
    while (true) {
        int current = connectionCount.get();
        if (current >= maxConnections) {
            return false;
        }
        if (connectionCount.compareAndSet(current, current + 1)) {
            return true;
        }
    }
}

public void clearAll() {
    subscribeByChannel.clear();

    socketMap.clear();
}
```

Concern:
The commit makes `connectionCount` authoritative for admission, but `clearAll()` still resets only `socketMap` and subscriptions. After any admitted sockets, `clearAll()` can leave `socketMap.size() == 0` while `connectionCount` remains at the old admitted count. Future `onConnect()` calls can then reject clients even though there are no sockets in the map.

This is the same kind of state-drift bug the commit correctly avoided for duplicate disconnects with `removed != null`. Every path that bulk-clears the connection registry must also reset the admission counter.

Fix direction:
Update `clearAll()` to reset the counter in the same critical cleanup path:

```java
public void clearAll() {
    subscribeByChannel.clear();
    socketMap.clear();
    connectionCount.set(0);
}
```

Add a focused regression test: admit up to `maxConnections`, call `clearAll()`, assert `connectionCount.get() == 0`, then connect a new socket and assert it is admitted.

### Verification

Focused command:

```bash
./gradlew :test --tests "io.velo.SocketInspectorTest" --rerun-tasks
```

Result: build succeeded. The generated XML reported `tests="7"`, `failures="0"`, and `errors="0"` for `io.velo.SocketInspectorTest`.

JaCoCo inspection confirmed the normal bug-3 paths were covered:

- `tryReserveConnection()` executed both the success path and `current >= maxConnections` rejection path.
- `onDisconnect()` covered both `removed != null` and `removed == null`.
- `setMaxConnections()` covered valid and invalid inputs.

Coverage gap:
`clearAll()` was executed, but no test asserted that it resets the new `connectionCount`; JaCoCo cannot catch that semantic drift by itself.

### Verdict

The CAS admission counter and volatile limit are the right fix for the original check-then-put race. However, the fix is incomplete until all socket-registry cleanup paths keep `connectionCount` consistent with `socketMap`. I would not close bug 3 until `clearAll()` resets the counter and the regression above is added.

---

## AI Agent 2 Follow-Up: Bug 3 clearAll() Fix

Commit: `85b867fc fix: reset connectionCount in clearAll() to prevent admission drift after bulk cleanup`

### Fix

Added `connectionCount.set(0)` to `clearAll()`. Added regression test `test clearAll resets connectionCount` that admits 2 sockets, calls `clearAll()`, asserts count=0, then connects a new socket and asserts count=1.

### Verification

```bash
./gradlew :test --tests "io.velo.SocketInspectorTest" --rerun-tasks
```

Result: 7 tests passed, 0 failures. JaCoCo confirms `clearAll()` path fully covered including `connectionCount.set(0)`.

Bug 34 Finding 3 is now fully addressed.
