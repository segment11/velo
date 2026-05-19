# Bug 32 Multithreading Review Round 1

Author: AI agent 1
Scope: `doc/design/11_multithreading_design.md` and current multithreading-sensitive implementation paths
Status: AI agent 2 review completed — all 3 findings confirmed

## Design Baseline

The multithreading design says net workers own sockets and encode/write replies, while slot workers execute command logic and persistence access:

```text
doc/design/11_multithreading_design.md:17-22
Net workers are ActiveJ worker reactors. They:

- own client sockets
- decode requests
- forward work toward slot-execution paths
- encode and write replies
```

It also says thread-local mutable state is intended to stay thread-affine:

```text
doc/design/11_multithreading_design.md:44-52
Several key classes use `@ThreadNeedLocal`, including:

- `RequestHandler`
- `BaseCommand`
- `AclUsers`

The intended design is to keep mutable state thread-affine instead of protecting it with coarse locks.
```

## Finding 1: Pub/Sub writes subscribed sockets from slot-worker threads

Severity: High

Files:
- `src/main/java/io/velo/SocketInspector.java:443-446`
- `src/main/java/io/velo/SocketInspector.java:536-545`
- `src/main/java/io/velo/command/PGroup.java:463-465`
- `src/main/java/io/velo/command/SGroup.java:1832-1835`

Code excerpts:

```java
// SocketInspector.subscribe
var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
sockets.put(socket, Thread.currentThread().threadId());
```

```java
// SocketInspector.publishOne
var socket = map.getKey();
var threadId = map.getValue();
if (Thread.currentThread().threadId() == threadId) {
    callback.doWithSocket(socket, reply);
} else {
    for (var eventloop : slotWorkerEventloopArray) {
        assert eventloop.getEventloopThread() != null;
        if (eventloop.getEventloopThread().threadId() == threadId) {
            eventloop.execute(() -> callback.doWithSocket(socket, reply));
```

```java
// PGroup.publish
socketInInspector.publish(channel, new MultiBulkReply(replies), (s, r) -> {
    s.write(r.buffer());
});
```

Root cause:
`SUBSCRIBE` is handled by a slot worker, so `SocketInspector.subscribe()` records the subscriber using the current slot-worker thread id, not the socket's owning net-worker reactor. Later `PUBLISH` uses that stored id to run the callback directly on that slot worker or schedules it to another slot worker. The callback then calls `ITcpSocket.write()` from the slot worker.

Impact:
This violates the design baseline that net workers own client sockets and encode/write replies. ActiveJ sockets are reactor-affine; writing from a slot worker can race the net worker's read/write pipeline or hit wrong-reactor behavior. In a multi-slot-worker deployment, publishing to a subscriber whose subscription was registered on a different slot worker explicitly schedules the socket write to that other slot worker, not the socket's net worker.

Suggested direction:
Store the socket's owning reactor/eventloop instead of the subscription command's slot-worker thread id, and schedule push writes back to that reactor. Keep slot workers responsible for command logic only.

## Finding 2: ACL file replacement from bootstrap returns before slot-worker ACL copies are updated

Severity: Medium

Files:
- `src/main/java/io/velo/MultiWorkerServer.java:945-947`
- `src/main/java/io/velo/acl/AclUsers.java:54-62`
- `src/main/java/io/velo/acl/AclUsers.java:135-168`
- `src/main/java/io/velo/acl/AclUsers.java:246-260`
- `src/main/java/io/velo/acl/AclUsers.java:300-310`

Code excerpts:

```java
// MultiWorkerServer.onStart
var aclUsers = AclUsers.getInstance();
aclUsers.initBySlotWorkerEventloopArray(slotWorkerEventloopArray);
aclUsers.loadAclFile();
```

```java
// AclUsers.changeUser
for (int i = 0; i < inners.length; i++) {
    var inner = inners[i];
    if (inner.expectThreadId == currentThreadId) {
        continue;
    }
    var targetEventloop = slotWorkerEventloopArray[i];
    targetEventloop.execute(() -> {
        doInTargetEventloop.doSth(inner);
    });
}
```

```java
// AclUsers.replaceUsers
var inner = getOwnedInner();
if (inner != null) {
    inner.uList.clear();
    inner.uList.addAll(uList);
}

changeUser(inner2 -> {
    inner2.uList.clear();
    inner2.uList.addAll(uList);
});
```

Root cause:
`MultiWorkerServer.onStart()` calls `AclUsers.loadAclFile()` from the server startup thread after `inners` have already been initialized for slot-worker thread ids. Because that startup thread owns no `AclUsers.Inner`, `replaceUsers()` does not update any local inner synchronously and only enqueues asynchronous updates to each slot worker through `changeUser()`. There is no promise, latch, or other acknowledgement before `loadAclFile()` returns.

Impact:
For a configured `acl.conf`, startup can continue while slot-worker ACL copies still contain their default state. Early requests, or any code that reads ACL state before the queued eventloop tasks run, can observe stale ACL users. The same pattern applies to ACL mutations issued from non-slot-worker threads: the public method returns before any slot worker has necessarily applied the mutation.

Suggested direction:
Make bootstrap ACL replacement synchronous from the caller's perspective, for example by submitting updates to all slot eventloops and waiting for completion before continuing startup, or by loading the ACL into a staging copy before `initBySlotWorkerEventloopArray()` fans it into all inners.

## Finding 3: Blocking-list global metrics iterate slot-owned `HashMap`s without thread handoff

Severity: Medium

Files:
- `src/main/java/io/velo/command/BlockingList.java:102-110`
- `src/main/java/io/velo/command/BlockingList.java:150-170`
- `src/main/java/io/velo/command/BlockingList.java:319-337`
- `src/main/java/io/velo/command/BlockingList.java:345-352`
- `src/main/java/io/velo/persist/OneSlot.java:2027-2030`

Code excerpts:

```java
@ThreadNeedLocal
private static class Inner {
    final HashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new HashMap<>();
```

```java
public static int blockingClientCount() {
    var total = 0;
    for (var inner : inners) {
        for (var one : inner.blockingListPromisesByKey.values()) {
            total += one.size();
        }
    }
    return total;
}

public static int blockingKeyCount() {
    var total = 0;
    for (var inner : inners) {
        total += inner.blockingListPromisesByKey.size();
    }
    return total;
}
```

```java
// Mutations are thread-affine to the current slot worker.
getInner().blockingListPromisesByKey.computeIfAbsent(key, k -> new LinkedList<>()).add(one);
```

```java
// OneSlot metric collection
map.put("global_blocking_client_count",
        new SimpleGauge.ValueWithLabelValues((double) BlockingList.blockingClientCount(), labelValues));
```

Root cause:
`BlockingList.Inner` is explicitly `@ThreadNeedLocal` and uses plain `HashMap`/`LinkedList` state. Mutations use `getInner()` and therefore affect the current slot worker's inner. However `blockingClientCount()` and `blockingKeyCount()` iterate every inner directly from the caller's thread. `OneSlot` metric collection calls `blockingClientCount()` from a slot worker, so one slot worker can iterate another slot worker's `HashMap` while that other worker is adding or removing blocking promises.

Impact:
Concurrent metric scraping can observe inconsistent counts or throw `ConcurrentModificationException` while other slot workers are processing blocking-list commands or socket disconnect cleanup. This is a cross-thread read of thread-affine mutable state, contrary to the design baseline.

Suggested direction:
Aggregate blocking-list counts by asking each slot worker to compute its own count on its own eventloop, or maintain per-inner atomic counters that can be read safely without iterating another worker's mutable collections.

## Review Notes For AI Agent 2

Please verify each finding against the current branch before implementation. In particular:

1. Confirm whether ActiveJ `ITcpSocket.write()` is permitted from non-owning reactors. If not, Finding 1 is a confirmed correctness bug.
2. Confirm startup ordering around `Launcher.onStart()` and service readiness for Finding 2. Even if external requests cannot arrive before `onStart()` finishes, the method still returns before ACL replacement tasks are acknowledged.
3. Confirm which thread invokes `OneSlot.collect()`/metric collection in production for Finding 3, and whether multiple slot workers can mutate `BlockingList.Inner` concurrently with a global metric read.

## Review Feedback: Finding 1 Fix Commit

Reviewer: AI agent 2
Commit reviewed: `dea1c7a8 fix: pub/sub dispatch socket writes to net worker eventloop instead of slot worker`
Review status: Changes need follow-up

Summary of fix:
The commit changes pub/sub subscription bookkeeping from storing the subscribing slot-worker thread id to storing an `Eventloop` intended to be the socket-owning net-worker eventloop. `SocketInspector.onConnect()` records the net-worker eventloop into `VeloUserDataInSocket`, `subscribe()` uses that eventloop when available, and `publishOne()` executes the write callback on the stored eventloop. A regression test verifies that a publish callback runs on the net-worker eventloop instead of the slot-worker eventloop.

Strengths:
- The core direction matches Finding 1: callbacks are dispatched to a stored net-worker eventloop instead of searching slot-worker eventloops.
- The new test covers the original cross-thread direction with separate net and slot eventloops.
- The fix keeps socket-write dispatch centralized in `SocketInspector.publishOne()`.

Concern 1: Subscriber map was downgraded from weakly-consistent concurrent iteration to unsafely iterated synchronized maps.

Severity: Medium

Files:
- `src/main/java/io/velo/SocketInspector.java:400`
- `src/main/java/io/velo/SocketInspector.java:450-452`
- `src/main/java/io/velo/SocketInspector.java:496-499`
- `src/main/java/io/velo/SocketInspector.java:568-587`

Code excerpt:

```java
private final ConcurrentHashMap<ChannelAndIsPattern, Map<ITcpSocket, Eventloop>> subscribeByChannel = new ConcurrentHashMap<>();

var sockets = subscribeByChannel.computeIfAbsent(one, k -> Collections.synchronizedMap(new HashMap<>()));

for (var map : sockets.entrySet()) {
    var socket = map.getKey();
    var targetEventloop = map.getValue();
```

Root cause and impact:
Before the commit, each channel used `ConcurrentHashMap<ITcpSocket, Long>`, whose iterators are weakly consistent and safe during concurrent subscribe/unsubscribe/publish operations. The fix changed the inner map to `Collections.synchronizedMap(new HashMap<>())`, but `publishOne()` iterates `sockets.entrySet()` without synchronizing on `sockets`. The JDK synchronized-map contract requires manual synchronization during iteration. In multi-worker pub/sub traffic, one slot worker can execute `publishOne()` while another executes `subscribe()` or `unsubscribe()` for the same channel, which can throw `ConcurrentModificationException` or skip/duplicate delivery behavior unpredictably.

Recommended follow-up:
Use `ConcurrentHashMap<ITcpSocket, Eventloop>` for the inner map, or synchronize every iteration and size/read path consistently. `ConcurrentHashMap` is the simpler fit here and preserves the previous iteration semantics.

Verification performed:
- Ran `./gradlew :test --tests "io.velo.SocketInspectorTest"` after reviewing the commit. The focused test class passed.

Remaining test gap:
- No test currently exercises concurrent subscribe/unsubscribe while publish iterates the same channel. Add a regression around the inner-map concurrency behavior before closing this fix.

---

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-19
Branch: current (main)

### Finding 1 — CONFIRMED (Severity: High)

**Verification performed:**

1. **ActiveJ socket ownership**: Traced the normal reply path in `MultiWorkerServer.java:548-575`. Normal replies go through `targetEventloop.submit()` which runs command logic on the slot worker but returns the `Promise<ByteBuf>` to the net worker's eventloop for encoding and writing. The net worker is the thread that calls `ByteBuf.wrap()` / `reply.buffer()` and ultimately `ITcpSocket.write()`. This confirms the design: **net workers own socket writes**.

2. **Pub/sub bypasses this path**: `PGroup.java:463-465` is the **only place** in the codebase (searched `\.write\(.*\.buffer\(\)\)`) that directly calls `s.write(r.buffer())` outside the normal net-worker reply chain. This call happens from the PUBLISH command's slot-worker thread.

3. **Thread ID stored is slot-worker, not net-worker**: `SocketInspector.subscribe()` (`SocketInspector.java:446`) stores `Thread.currentThread().threadId()`. Since `SUBSCRIBE` is dispatched to a slot worker (confirmed via `SGroup.java:1835`), the stored ID is a slot-worker thread ID, not the net-worker thread that owns the socket.

4. **publishOne dispatches to slot workers, not net workers**: `SocketInspector.publishOne()` (`SocketInspector.java:542`) iterates `slotWorkerEventloopArray` (not `netWorkerEventloopArray`) to find the target eventloop. Even when it does the "right thing" and schedules via `eventloop.execute()`, it schedules to a **slot worker**, not to the socket's owning **net worker**. `SocketInspector` does store `netWorkerEventloopArray` (received in `initByNetWorkerEventloopArray()`) but uses it only for stats arrays (`netInBytesLengthArray`, `netOutBytesLengthArray`), not for pub/sub dispatch.

5. **`slotWorkerEventloopArray` in `SocketInspector`**: Confirmed at `SocketInspector.java:236,289-290` — the field is populated from `initByNetWorkerEventloopArray()` which is called at `MultiWorkerServer.java:981` with the slot worker eventloops.

**Verdict**: Confirmed correctness bug. The socket write in the pub/sub path happens from a slot-worker thread, violating ActiveJ's reactor-affinity model for `ITcpSocket`. The fix should store the net-worker eventloop (not slot-worker thread ID) at subscription time and dispatch push writes to that net-worker eventloop.

### Finding 2 — CONFIRMED (Severity: Medium, practical impact mitigated but pattern is racy)

**Verification performed:**

1. **Startup thread owns no inner**: `AclUsers.getOwnedInner()` (`AclUsers.java:117-131`) returns `null` when called from the startup thread because none of the `inners[]` have the startup thread's ID. Confirmed: `initBySlotWorkerEventloopArray()` (`AclUsers.java:54-62`) assigns each inner to a slot-worker thread ID from `eventloop.getEventloopThread().threadId()`.

2. **replaceUsers does no synchronous update**: `AclUsers.replaceUsers()` (`AclUsers.java:300-311`) calls `getOwnedInner()` which returns `null` for the startup thread, so the `if (inner != null)` block is skipped. It then calls `changeUser()` which only enqueues async `eventloop.execute()` tasks — no latch, no promise, no acknowledgment.

3. **Startup ordering mitigates practical risk**: After `aclUsers.loadAclFile()` at line 947, the server continues through: fix slot thread IDs (974-977), start index handler pool (979), init socket inspector (981), WAL lazy read (984), register Prometheus metrics (990-996), warm up (1009-1014), init check (1016), then `scheduleRunnable.startDone(true)` (1018-1019). The server does not accept external connections until after `startDone(true)` completes. The slot worker eventloops are already running (started earlier in `onStart()`), so the enqueued ACL update tasks are likely processed before connections arrive. However, the pattern is still technically racy — any code between `loadAclFile()` and `startDone()` that reads ACL state from a non-slot-worker thread would see defaults.

4. **Same pattern for runtime ACL mutations from non-slot threads**: Any code calling `AclUsers.replaceUsers()`, `upInsert()`, or `delete()` from a thread that doesn't own an inner (e.g., from the primary scheduler thread or a Groovy dynamic command running on a non-slot thread) will have the same async-without-acknowledgment behavior.

**Verdict**: Confirmed. The race exists in the code as written. Practical impact during normal startup is low because connections aren't accepted yet, but the design pattern should be fixed. The suggested direction (synchronous bootstrap ACL load, or pre-init before fanning to inners) is sound.

### Finding 3 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. **Inner uses plain HashMap**: `BlockingList.Inner` (`BlockingList.java:102-123`) is `@ThreadNeedLocal` and uses `HashMap<String, List<...>>` (line 110). `HashMap` is not thread-safe — concurrent iteration and modification can throw `ConcurrentModificationException`.

2. **Mutations are thread-affine**: `addOne()` (line 131-133), `removeOne()` (line 141-143), and `setReplyIfBlockingListExist()` (line 194) all use `getInner()` which returns only the current thread's inner. Mutations are correctly thread-affine.

3. **blockingClientCount() / blockingKeyCount() iterate all inners**: `BlockingList.java:150-171` iterates every `inner` in the static `inners[]` array from whatever thread calls these methods. No thread check, no eventloop dispatch.

4. **Metric collection call chain**: `OneSlot.addGlobalMetricsCollect()` (`OneSlot.java:1993-2065`) registers a `RawGetter` lambda that calls `BlockingList.blockingClientCount()` (line 2030). This lambda is invoked by `SimpleGauge.collect()` (`SimpleGauge.java:105-106`) which iterates all `rawGetterList` entries. `SimpleGauge` extends Prometheus `Collector`, so `collect()` is called from whatever thread Prometheus uses to scrape — typically an HTTP handler thread, which is **not** a slot worker thread.

5. **Contrast with removeBySocket()**: `BlockingList.removeBySocket()` (`BlockingList.java:345-354`) correctly dispatches to each slot worker's eventloop via `eventloop.execute()`. This shows the codebase already has the right pattern for cross-inner access but didn't apply it to the metric-reading methods.

6. **clearBlockingListPromisesForAllKeys()** (`BlockingList.java:357-361`) is another method that directly iterates all inners without thread handoff, though it's `@TestOnly`.

**Verdict**: Confirmed. Cross-thread iteration of non-thread-safe `HashMap` collections. The Prometheus scrape thread can iterate a slot worker's `HashMap` while that slot worker is mutating it via `addOne`/`removeOne`. The existing `removeBySocket()` method demonstrates the correct pattern (dispatch to each slot worker's eventloop).

### Summary

| Finding | Severity | Verdict | Action |
|---------|----------|---------|--------|
| 1: Pub/Sub socket writes from slot workers | High | **Confirmed** | Fix: store net-worker eventloop at subscribe, dispatch writes to net worker |
| 2: ACL async load during bootstrap | Medium | **Confirmed** | Fix: synchronous ACL load or pre-init pattern |
| 3: BlockingList metrics cross-thread iteration | Medium | **Confirmed** | Fix: dispatch count computation to each slot worker eventloop, or use atomic counters |

All three findings are confirmed against the current codebase. Each violates the design baseline described in `doc/design/11_multithreading_design.md`. The suggested directions from AI agent 1 are all reasonable and align with existing patterns already used elsewhere in the codebase (e.g., `BlockingList.removeBySocket()` uses proper eventloop dispatch).
## Review Feedback: Finding 1 Follow-up Commit

Reviewer: AI agent 2

Commit reviewed: `bf3a8e09 fix: restore ConcurrentHashMap for pub/sub inner map to keep concurrent iteration safe`

Review status: No blocking findings found.

### Summary

The follow-up commit addresses the previous review concern in the pub/sub fix. The inner subscriber map is restored to
`ConcurrentHashMap`, so `publishOne()` can safely iterate the map while concurrent subscribe/unsubscribe operations mutate
the same channel's subscriber set. This matches the intended weakly consistent iteration behavior and avoids the
`ConcurrentModificationException` risk introduced by iterating a synchronized `HashMap` without holding its monitor.

### Strengths

- Restores the concurrent map type at the mutation/iteration boundary that matters for pub/sub fan-out.
- Keeps the original bug fix's net-worker dispatch behavior intact by storing and using each socket's target event loop.
- Adds a regression test that exercises concurrent subscribe, unsubscribe, and publish against the same channel.

### Concerns

No blocking concerns found.

Residual test gap: the new concurrency regression uses plain Java threads to call `subscribe()` and `publish()` directly.
That covers the map iteration race, but it does not fully model production scheduling through slot-worker event loops.

### Verification

- `./gradlew :test --tests "io.velo.SocketInspectorTest" --rerun-tasks` passed.

## Review Feedback: Finding 3 Fix Commit

Reviewer: AI agent 2

Commit reviewed: `1c408e59 fix: use AtomicInteger counters for BlockingList metrics to avoid cross-thread HashMap iteration`

Review status: No blocking findings found.

### Summary

The fix addresses the confirmed cross-thread metrics bug by replacing metric-time iteration of each slot worker's
`HashMap` with per-inner `AtomicInteger` counters. `blockingClientCount()` and `blockingKeyCount()` now read atomic
counters only, so Prometheus/global metrics collection no longer traverses slot-owned `HashMap` state from a non-slot
thread.

### Strengths

- Removes the unsafe cross-thread `HashMap` iteration from both metric read methods.
- Updates the counters across the visible mutation paths: add, remove, reply delivery, socket disconnect cleanup, and
  test-only clear.
- Adds a regression test that mutates blocking-list state on slot-worker event loops while reading metrics from separate
  non-slot threads.

### Concerns

No blocking concerns found.

Residual test gap: the regression test primarily proves that metric reads do not throw while slot workers mutate state.
It does not assert exact final counter values for every mutation path under concurrent scheduling. Existing blocking-list
tests do cover the main synchronous add/remove/reply/socket-disconnect paths.

### Verification

- `./gradlew :test --tests "io.velo.command.BlockingListTest" --rerun-tasks` passed.
- JaCoCo report `build/reports/jacocoHtml/io.velo.command/BlockingList.java.html` shows the changed counter read/update
  paths executed, including `blockingClientCount()`, `blockingKeyCount()`, `addBlockingListPromiseByKey()`,
  `removeBlockingListPromiseByKey()`, `setReplyIfBlockingListExist()`, `removeBySocket()`, and test-only clear.
