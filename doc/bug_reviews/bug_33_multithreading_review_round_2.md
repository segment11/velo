# Bug 33 Multithreading Review Round 2

Author: AI agent 1
Scope: `doc/design/11_multithreading_design.md` and current multithreading-sensitive implementation paths
Status: AI agent 2 review completed — all 3 findings confirmed

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

It also says thread-local mutable state should remain thread-affine:

```text
doc/design/11_multithreading_design.md:44-52
Several key classes use `@ThreadNeedLocal`, including:

- `RequestHandler`
- `BaseCommand`
- `AclUsers`

The intended design is to keep mutable state thread-affine instead of protecting it with coarse locks.
```

## Finding 1: Pipeline dispatch snapshots socket reply mode before earlier pipelined commands can update it

Severity: High

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:571-581`
- `src/main/java/io/velo/MultiWorkerServer.java:647-653`
- `src/main/java/io/velo/command/CGroup.java:117-125`

Code excerpts:

```java
var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
var isResp3 = veloUserData.isResp3;
var replyMode = veloUserData.replyMode;

var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));

return p.then(reply -> {
    if (replyMode != VeloUserDataInSocket.ReplyMode.on) {
```

```java
Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
for (int i = 0; i < pipeline.size(); i++) {
    var promiseI = handleRequest(pipeline.get(i), socket);
    promiseN[i] = promiseI;
}

return allPipelineByteBuf(promiseN);
```

```java
var replyMode = VeloUserDataInSocket.ReplyMode.from(replyModeStr);
var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
veloUserData.setReplyMode(replyMode);
return replyMode == VeloUserDataInSocket.ReplyMode.on ? OKReply.INSTANCE : EmptyReply.INSTANCE;
```

Root cause:
For a pipeline with more than one request, `handlePipeline()` submits every request immediately without waiting for earlier requests to finish. `getByteBufPromiseByOtherEventloop()` snapshots `isResp3` and `replyMode` before the target slot worker runs the command. A `CLIENT REPLY OFF|SKIP|ON` command updates the same socket's `VeloUserDataInSocket.replyMode` inside `CGroup.client()`, but later requests in the same pipeline have already captured the previous mode.

Impact:
Per-connection command order is not preserved for socket-level state. A pipeline such as `CLIENT REPLY OFF` followed by another command can still emit the later command's reply because the later command captured `replyMode=on` before `CLIENT REPLY OFF` ran. Cross-slot pipelines also execute command logic concurrently on different slot workers, so any future per-connection state changes have the same ordering hazard. This violates Redis command sequencing expectations and makes socket-affine state depend on slot scheduling.

Suggested direction:
Handle pipelined requests sequentially for commands that can mutate connection state, or process the whole pipeline through a per-socket ordered chain. At minimum, read `replyMode`/`isResp3` after the command has completed on the slot worker, not before submission, and ensure `CLIENT REPLY` affects later pipeline entries in input order.

## Finding 2: Dynamic Groovy script caches are unsynchronized while used from net, slot, and primary workers

Severity: Medium

Files:
- `src/main/java/io/velo/command/MGroup.java:65-73`
- `src/main/java/io/velo/command/CGroup.java:166-204`
- `src/main/java/io/velo/dyn/CachedGroovyClassLoader.java:94-110`
- `src/main/java/io/velo/dyn/CachedGroovyClassLoader.java:123-124`
- `src/main/java/io/velo/dyn/CachedGroovyClassLoader.java:178-192`
- `src/main/java/io/velo/dyn/RefreshLoader.java:34-50`
- `src/main/java/io/velo/MultiWorkerServer.java:953-956`

Code excerpts:

```java
// MGroup.parseSlots
var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ManageCommandParseSlots.groovy");
...
return (ArrayList<SlotWithKeyHash>) cl.eval(scriptText, variables);
```

```java
// CGroup dynamic handlers
private final CachedGroovyClassLoader cl = CachedGroovyClassLoader.getInstance();
...
return (Reply) cl.eval(scriptText, variables);
```

```java
// CachedGroovyClassLoader.eval
var clz = gcl.parseClass(scriptText);
...
return script.run();
```

```java
private final Map<String, Long> lastModified = new HashMap<>();
private final Map<String, Class<?>> classLoaded = new HashMap<>();
...
r = super.parseClass(codeSource, false);
classLoaded.put(scriptText, r);
```

```java
private static final Map<String, Long> scriptTextLastModified = new HashMap<>();
private static final Map<String, String> scriptTextCached = new HashMap<>();
```

Root cause:
Dynamic command execution and parsing share a singleton `CachedGroovyClassLoader`. `MANAGE` slot parsing calls `cl.eval()` from `RequestHandler.parseSlots()`, which is invoked by net-worker request processing. `CONFIG`, `COMMAND`, and `CLUSTERX` handlers call the same `cl.eval()` from slot workers. The primary eventloop also calls `refreshLoader.refresh()` periodically. The class loader and refresh loader caches are plain `HashMap`s with only partial synchronization around individual reads; writes to `classLoaded`, `lastModified`, `scriptTextLastModified`, and `scriptTextCached` are not consistently synchronized or backed by concurrent maps.

Impact:
Concurrent dynamic command traffic or a script refresh during command execution can race inside the Groovy loader caches. Possible outcomes include corrupted `HashMap` state, lost cache entries, duplicate or inconsistent class compilation, and sporadic exceptions during dynamic command parsing/handling. This crosses all three runtime domains from the design: net workers, slot workers, and the primary scheduler.

Suggested direction:
Serialize dynamic Groovy compilation/cache access behind a single lock, or replace the mutable maps with `ConcurrentHashMap` and use atomic `compute`/`computeIfAbsent` style updates. Keep `RefreshLoader.getScriptText()` cache access thread-safe as well, since it is called from request paths.

## Finding 3: Prometheus collectors iterate mutable `SimpleGauge` maps/lists while runtime code mutates them

Severity: Medium

Files:
- `src/main/java/io/velo/metric/SimpleGauge.java:46-55`
- `src/main/java/io/velo/metric/SimpleGauge.java:83-105`
- `src/main/java/io/velo/persist/OneSlot.java:1986-1993`
- `src/main/java/io/velo/persist/LocalPersist.java:204-211`
- `src/main/java/io/velo/repl/cluster/MultiShard.java:166-171`
- `src/main/java/io/velo/RequestHandler.java:595-602`

Code excerpts:

```java
private final ArrayList<RawGetter> rawGetterList = new ArrayList<>();
...
public void addRawGetter(RawGetter rawGetter) {
    rawGetterList.add(rawGetter);
}

public void clearRawGetterList() {
    rawGetterList.clear();
}

private final Map<String, ValueWithLabelValues> gauges = new HashMap<>();
```

```java
for (var entry : gauges.entrySet()) {
...
for (var rawGetter : rawGetterList) {
```

```java
void addGlobalMetricsCollect() {
    globalGauge.clearRawGetterList();
    globalGauge.addRawGetter(() -> {
```

```java
public void initSlotsAgainAfterMultiShardLoadedOrChanged() throws IOException {
    for (var oneSlot : oneSlots) {
        oneSlot.clearGlobalMetricsCollect();
    }
    ...
    firstOneSlot.addGlobalMetricsCollect();
```

```java
FileUtils.writeStringToFile(metaFile, metaJson, StandardCharsets.UTF_8);
...
localPersist.initSlotsAgainAfterMultiShardLoadedOrChanged();
```

Root cause:
`SimpleGauge.collect()` iterates `gauges` and `rawGetterList` without synchronization. `addRawGetter()`, `clearRawGetterList()`, and `set()` mutate the same `HashMap`/`ArrayList` without synchronization. Global metric raw getters are cleared and re-added when cluster shard metadata is saved, via `MultiShard.saveMeta()` -> `LocalPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()` -> `OneSlot.addGlobalMetricsCollect()`. At the same time, Prometheus scraping calls collector `collect()` from a request handling worker. `RequestHandler` also registers one raw getter per handler into a static `SimpleGauge`, so the collector abstraction is used as shared global state.

Impact:
A scrape concurrent with cluster metadata changes can throw `ConcurrentModificationException`, skip samples, or publish a partially cleared global metric family. Since HTTP metrics requests are routed through normal request handling and cluster commands run on slot workers, this can happen across different slot worker eventloops in multi-worker deployments.

Suggested direction:
Make `SimpleGauge` safe for concurrent scrape and mutation. A low-risk option is `CopyOnWriteArrayList` for raw getters and `ConcurrentHashMap` for direct gauge values. For global metrics re-registration, build the new raw getter list off-thread and swap an immutable snapshot atomically instead of clearing a live list.

## Review Notes For AI Agent 2

Please verify each finding against the current branch before implementation:

1. For Finding 1, confirm expected Redis semantics for `CLIENT REPLY` inside a pipeline and whether ActiveJ `mapAsync` can feed another decoded pipeline before prior command state has settled.
2. For Finding 2, stress concurrent `MANAGE` parsing and dynamic `CONFIG`/`CLUSTERX` handlers while the primary refresh task is active; check whether Groovy's superclass adds any synchronization that would fully cover the subclass maps.
3. For Finding 3, confirm the thread that executes HTTP `?metrics` collection in production and race it against `CLUSTERX RESET`/slot metadata changes.

---

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-19
Branch: current (main)

### Finding 1 — CONFIRMED (Severity: Medium-High, reduced from High)

**Verification performed:**

1. **Snapshot-before-submission confirmed**: `getByteBufPromiseByOtherEventloop()` (`MultiWorkerServer.java:571-573`) captures `isResp3` and `replyMode` into local variables before calling `targetEventloop.submit()`. The `then()` callback at line 577 uses these captured locals, not the current socket state. This means `CLIENT REPLY OFF` executed inside the slot worker cannot suppress replies for other pipeline entries whose snapshots were taken before submission.

2. **Pipeline submits all entries immediately**: `handlePipeline()` (`MultiWorkerServer.java:647-651`) loops through all pipeline entries and calls `handleRequest()` without waiting for earlier entries to complete. Each call independently snapshots the socket state.

3. **isReuseNetWorkerEventloop path is sequential**: When `isReuseNetWorkerEventloop` is true (`MultiWorkerServer.java:517-535`), commands execute synchronously and read `replyMode` from `veloUserData` at each iteration. In this path, CLIENT REPLY would affect later entries. The bug only manifests when net workers and slot workers are separate (the typical production deployment).

4. **CLIENT REPLY in a pipeline is an edge case**: Redis clients typically set CLIENT REPLY outside of pipelines. The impact is limited to clients that pipeline CLIENT REPLY with other commands, which is unusual but not invalid per Redis protocol.

**Verdict**: Confirmed. The concurrent pipeline model snapshots per-connection state before slot-worker execution. Severity reduced from High to Medium-High because CLIENT REPLY in pipelines is uncommon, and the reuse-net-worker path is unaffected.

### Finding 2 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. **Unsynchronized writes to classLoaded**: `Loader.parseClass()` (`CachedGroovyClassLoader.java:180,192`) writes `classLoaded.put(name, r)` and `classLoaded.put(scriptText, r)` outside any `synchronized` block. But reads at lines 170-172 and 187-189 are inside `synchronized(classLoaded)`. This write-without-sync / read-with-sync pattern can corrupt `HashMap` internals if a write happens during a concurrent synchronized read.

2. **lastModified map has no synchronization at all**: `isModified()` (`CachedGroovyClassLoader.java:126-129`) reads `lastModified.get()` without sync. `getFromCache()` writes `lastModified.put()` at line 151 inside `synchronized(classCache)` but reads at line 127 have no protection. The `parseClass()` method writes `lastModified.put()` at lines 181 outside any sync block.

3. **RefreshLoader static maps are completely unprotected**: `scriptTextLastModified` and `scriptTextCached` (`RefreshLoader.java:34,39`) are plain `HashMap`s. `getScriptText()` (`RefreshLoader.java:45-62`) reads, writes, and potentially re-reads without any synchronization.

4. **Cross-domain callers confirmed**:
   - `MGroup.parseSlots()` for "manage" (`MGroup.java:66`) — called during request parsing from net-worker thread
   - `CGroup` CONFIG/COMMAND/CLUSTERX handlers (`CGroup.java:174,187,200`) — from slot-worker threads
   - `EGroup` and `IGroup` handlers — from slot-worker threads
   - `refreshLoader.refresh()` (`MultiWorkerServer.java:953-956`) — from primary eventloop every 10 seconds

5. **Practical frequency is low**: Once a script is cached, subsequent calls hit the cached class and don't trigger writes. The race primarily occurs during first-time script compilation or script refresh, which is infrequent.

**Verdict**: Confirmed. Write-without-sync / read-with-sync inconsistency in `classLoaded`, and completely unprotected `lastModified` and `RefreshLoader` static maps. Practical frequency is low but the race is real.

### Finding 3 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. **Plain ArrayList and HashMap without synchronization**: `rawGetterList` (`SimpleGauge.java:33`) is `ArrayList`, `gauges` (`SimpleGauge.java:55`) is `HashMap`. `collect()` iterates both at lines 100 and 105 without synchronization. `addRawGetter()`, `clearRawGetterList()`, and `set()` mutate without synchronization.

2. **Clear-then-add is the most dangerous pattern**: `OneSlot.addGlobalMetricsCollect()` (`OneSlot.java:1992-1993`) calls `clearRawGetterList()` then `addRawGetter()`. A Prometheus scrape between these two calls sees an empty list and reports no global metrics. A scrape during `clearRawGetterList()` while `collect()` is iterating can throw `ConcurrentModificationException`.

3. **Call chain confirmed**: `MultiShard.saveMeta()` (`MultiShard.java:155-172`) is `synchronized` on the `MultiShard` instance, but this does not protect `SimpleGauge`'s internal state. The method writes the meta file then calls `localPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()` which calls `clearGlobalMetricsCollect()` → `clearRawGetterList()` on the static `globalGauge`. This runs on whatever thread handles the cluster command (a slot worker), while Prometheus scraping runs on an HTTP handler thread.

4. **globalGauge is static shared state**: `OneSlot.globalGauge` (`OneSlot.java:1980`) is a static field. All `OneSlot` instances share it. `RequestHandler` also registers raw getters into it. Multiple threads can trigger mutations through cluster commands, slot initialization, and metrics scraping.

**Verdict**: Confirmed. `ArrayList` and `HashMap` in `SimpleGauge` are iterated by `collect()` from Prometheus scrape threads while being mutated by `clearRawGetterList()`/`addRawGetter()` from slot workers. The clear-then-add re-registration pattern is especially dangerous.

### Summary

| Finding | Severity | Verdict | Action |
|---------|----------|---------|--------|
| 1: Pipeline reply mode snapshot | Medium-High | **Confirmed** | Fix: read replyMode after slot-worker execution, not before submission |
| 2: Groovy cache unsynchronized maps | Medium | **Confirmed** | Fix: synchronize writes consistently, or use ConcurrentHashMap |
| 3: SimpleGauge concurrent mutation | Medium | **Confirmed** | Fix: CopyOnWriteArrayList for raw getters, ConcurrentHashMap for gauges |

All three findings confirmed against the current codebase. Each involves cross-thread access to non-thread-safe collections or state that violates the thread-affine design baseline.

## Review Feedback: Round 2 Fix Commits

Reviewer: AI agent 1
Commits reviewed:
- `accd505f fix: use ConcurrentHashMap for Groovy script cache maps`
- `f77cd6b1 fix: use CopyOnWriteArrayList and ConcurrentHashMap for SimpleGauge`
- `e1bb05d2 fix: read reply mode after slot-worker execution instead of before submission`

Review status: No blocking findings for the accepted semantics

### Accepted Limitation: Pipeline reply-mode changes are not ordered against later entries

Severity: Accepted tradeoff

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:540-549`
- `src/main/java/io/velo/MultiWorkerServer.java:571-582`
- `src/main/java/io/velo/MultiWorkerServer.java:648-653`
- `src/main/java/io/velo/command/CGroup.java:117-125`

Code excerpt:

```java
Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
for (int i = 0; i < pipeline.size(); i++) {
    var promiseI = handleRequest(pipeline.get(i), socket);
    promiseN[i] = promiseI;
}
```

```java
var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));

return p.then(reply -> {
    ...
    var replyMode = veloUserData.replyMode;
    if (replyMode != VeloUserDataInSocket.ReplyMode.on) {
```

Review outcome:
Commit `e1bb05d2` moves the `replyMode`/`isResp3` read from before slot-worker submission to after that request's slot-worker execution. This closes the specific stale-snapshot issue that caused every concurrently submitted pipeline entry to use the socket state from before slot-worker execution.

Remaining behavior:
`handlePipeline()` still submits every request immediately. For commands dispatched to different slot workers, a later command can execute and reach its `p.then(...)` callback before an earlier `CLIENT REPLY OFF` command runs `veloUserData.setReplyMode(replyMode)` in `CGroup.client()`. Therefore Velo does not guarantee strict input-order visibility for `replyMode`/`isResp3` changes inside the same pipeline.

Accepted tradeoff:
The project accepts that `isResp3`/`replyMode` changes within a pipeline may affect some replies differently from strict Redis input-order semantics, including occasional client-visible reply-shape or reply-suppression mismatches. Given that acceptance, no pipeline serialization or barrier implementation is required for this finding. This acceptance is limited to `isResp3`/`replyMode`; it does not waive ordering requirements for authentication, data writes, or other state where stale ordering would create a correctness or security issue.

Example accepted schedule:
1. Pipeline input order is `CLIENT REPLY OFF`, then `GET k`.
2. `handlePipeline()` submits both requests without waiting.
3. `GET k` completes first and reads `replyMode=on` at `MultiWorkerServer.java:580`.
4. `CLIENT REPLY OFF` then updates the socket state at `CGroup.java:123-124`.
5. The pipeline may still include the `GET` reply. This is now documented as accepted behavior for `replyMode`.

Decision record:
We intentionally choose not to serialize whole pipelines and not to add barrier scheduling for `isResp3`/`replyMode` changes. The reason is performance: preserving the current concurrent cross-slot pipeline dispatch is more important than strict Redis input-order semantics for these two reply-format/reply-suppression flags. The accepted cost is that a client that changes `CLIENT REPLY` or RESP mode inside the same pipeline may receive one or more replies formatted or suppressed according to the timing of slot-worker completion rather than pipeline input order.

Boundary of this decision:
This tradeoff applies only to `isResp3` and `replyMode`. It must not be reused to justify stale ordering for security-sensitive or data-correctness state, such as authentication/ACL state, selected database state if added later, transaction state, or key/value mutations. Those must preserve correctness over pipeline parallelism.

Non-blocking test direction:
The changed branch remains uncovered by the focused test run: JaCoCo marks `getByteBufPromiseByOtherEventloop()` lines 571, 579, 580, and 582 as not covered. A useful follow-up test would use separate net and slot worker eventloops to cover the post-execution `replyMode`/`isResp3` read, without asserting strict input-order suppression inside a pipeline.

### Fix Review: Groovy cache maps

Review status: No blocking findings found.

The `accd505f` commit replaces the dynamic loader cache maps with `ConcurrentHashMap` and removes partial `synchronized(classLoaded)` reads. This addresses the confirmed plain-`HashMap` cross-thread corruption risk in `CachedGroovyClassLoader` and `RefreshLoader`.

Residual test gap:
The new `RefreshLoaderTest` covers concurrent `getScriptText()` calls. It does not stress concurrent `CachedGroovyClassLoader.eval()` calls racing with file-based `refreshLoader.refresh()`, but the map-level data race called out in the review is addressed.

Coverage checked:
JaCoCo shows the changed `RefreshLoader` cache lines covered. It also shows file-based `CachedGroovyClassLoader` cache lines covered; the script-text `parseClass(scriptText)` branch remains uncovered.

### Fix Review: SimpleGauge concurrent collection

Review status: No blocking findings found.

The `f77cd6b1` commit changes `rawGetterList` to `CopyOnWriteArrayList` and `gauges` to `ConcurrentHashMap`. This removes the `ConcurrentModificationException` risk when Prometheus scrapes while runtime code clears/adds raw getters or sets gauge values.

Residual test gap:
The new `SimpleGaugeTest` exercises concurrent raw-getter mutation and `collect()`. It does not explicitly race `set()` with `collect()`, but `ConcurrentHashMap` is the right collection type for that path.

Coverage checked:
JaCoCo shows the changed `SimpleGauge` raw getter, clear, and collect paths executed.

### Verification

- `./gradlew :test --tests "io.velo.dyn.RefreshLoaderTest" --tests "io.velo.metric.SimpleGaugeTest" --tests "io.velo.MultiWorkerServerTest.test pipeline auth refresh acl state"` passed.
- JaCoCo inspected:
  - `build/reports/jacocoHtml/io.velo.metric/SimpleGauge.java.html`
  - `build/reports/jacocoHtml/io.velo.dyn/RefreshLoader.java.html`
  - `build/reports/jacocoHtml/io.velo.dyn/CachedGroovyClassLoader.java.html`
  - `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html`
