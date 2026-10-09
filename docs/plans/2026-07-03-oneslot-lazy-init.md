# OneSlot Lazy Init Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Initialize only locally owned `OneSlot` instances at startup in cluster mode, and lazily initialize other slots when migration or ownership changes require them.

**Architecture:** Keep `LocalPersist.oneSlots` sized to `ConfForGlobal.slotNumber` so slot indexes, routing, and `slot % slotWorkers` topology remain stable. Allow unowned entries to stay `null` until `LocalPersist.ensureOneSlotInitialized(short slot)` constructs the `OneSlot`, initializes FDs, applies runtime flags, and binds the slot to its worker event loop and request handler. Publish lazy slot creation with a volatile copy-on-write `OneSlot[]` reference so lock-free readers see a fully initialized slot without taking the `LocalPersist` monitor. Startup continues to eagerly initialize every slot outside cluster mode, and cluster mode falls back to eager initialization if there is no persisted local slot assignment.

**Tech Stack:** Java 21, Groovy dynamic commands, ActiveJ event loops/promises, Spock tests, Gradle, JaCoCo.

---

## Current Behavior

- `LocalPersist.initSlots(...)` currently creates all `OneSlot` instances and calls `initFds()` for every slot.
- `MultiWorkerServer.onStart()` later binds thread IDs for every slot and runs WAL lazy read, optional warmup, and init check across all slots.
- `TaskRunnable.chargeOneSlots(...)` iterates all `OneSlot` entries and assumes every entry is non-null.
- Cluster migration paths already resolve the target inner slot:
  - `dyn/src/io/velo/command/ManageCommand.groovy` in `migrateFrom()`
  - `dyn/src/io/velo/command/ClusterxCommand.groovy` in `setslot()`

## Why Lazy Init Is Necessary

In cluster mode, current eager startup initializes every configured physical slot even when this node owns only a subset. For example, with `slotNumber=1024` and local ownership of 64 slots, eager startup initializes 960 unowned `OneSlot`s.

With the default `estimateKeyNumber=1_000_000` (`c1m`) configuration, an empty initialized slot still allocates several fixed structures: `MetaChunkSegmentFlagSeq` metadata, key-count stats, chunk/key-bucket direct buffers, thousands of WAL/LRU objects (`65536 / 32 = 2048` WAL groups), `KeyLoader`, `Chunk`, `Binlog`, `BigStringFiles`, `DynConfig`, file handles, and object overhead. A conservative estimate is roughly 6-15 MiB per empty initialized slot. Avoiding 960 unowned slots therefore saves about 5.6-14 GiB of memory under c1m defaults.

The exact number depends on `estimateKeyNumber`, WAL contents, LRU settings, and configured bucket/chunk sizes. Smaller debug-sized slots may still save around 1-2 GiB in this scenario; c10m-sized slots may save tens of GiB. Lazy init is therefore a capacity feature, not a small cleanup: it keeps memory proportional to locally owned slots plus slots actually imported later.

## Target Behavior

- In non-cluster mode, preserve existing eager initialization.
- In cluster mode with persisted local ownership in `MultiShard.mySelfShard().multiSlotRange`, initialize only the owned inner slots during startup.
- In cluster mode with no persisted ownership, preserve eager initialization as a compatibility fallback so startup behavior does not accidentally produce a node with no usable slot.
- Any later migration/import/ownership command must call `ensureOneSlotInitialized(innerSlot)` before touching the target slot.
- Loops over slots must skip uninitialized entries unless the operation explicitly requires initializing them.

## Command Path Audit

The request path can continue using direct `localPersist.oneSlot(slot)` lookups after slot parsing because `MultiWorkerServer.checkClusterSlot(...)` rejects commands for slots owned by other shards with `MOVED` or cross-shard errors before dispatch to command handlers. That makes command handlers operate only on locally owned slots, which startup or migration paths must have initialized.

Non-request loops are not protected by `checkClusterSlot(...)` and must be audited explicitly:

- `LocalPersist` aggregate operations must iterate initialized slots only.
- `LocalPersist.publishStreamReadyAndRefreshGate(...)` must skip uninitialized slots when fanning out `canRead`.
- `LeaderSelector.resetAsMaster(...)` and `LeaderSelector.resetAsSlave(...)` must iterate initialized slots only.
- `TaskRunnable.chargeOneSlots(...)` must skip null entries and lazily created slots must be registered with the scheduler.
- Benchmarks and test-only helpers may keep direct setup assumptions, but production code must not dereference a sparse slot entry without either ownership proof or an explicit null check.

## Synchronization Scope

Lazy slot initialization is serialized by `ensureOneSlotInitialized(...)` using the `LocalPersist` monitor or a dedicated `slotTopologyLock`. This lock protects only topology mutation: create, initialize, bind, clone, publish, and register the slot.

Normal command paths must not take this lock. They read the volatile `oneSlots` snapshot and use already initialized local slots.

Physical slot removal is out of scope for this feature. Migration may move ownership in cluster metadata, but this plan does not clean up or remove an initialized `OneSlot`. If removal is needed later, it requires a separate drain, quiesce, unpublish, and cleanup design.

## Open Review Question

Should there be a new config knob for predefined local slots before `nodes.json` exists?

Recommended v1 answer: no. Use persisted cluster metadata when available; otherwise retain current eager behavior. This avoids adding another source of cluster topology truth.

---

### Task 1: Add Failing Tests For Cluster Startup Lazy Initialization

**Files:**
- Modify: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy`
- Reference: `src/main/java/io/velo/persist/LocalPersist.java`
- Reference: `src/main/java/io/velo/repl/cluster/MultiShard.java`

**Step 1: Write the failing test**

Add a test that persists cluster metadata assigning only one client-slot range to the local shard, then calls `LocalPersist.initSlots(...)` with multiple slots.

Test intent:

```groovy
def 'test cluster init slots only initializes local assigned slots'() {
    given:
    ConfForGlobal.clusterEnabled = true
    ConfForGlobal.slotNumber = (short) 4
    ConfForGlobal.netListenAddress = 'localhost:7379'
    def localPersist = LocalPersist.instance
    localPersist.resetForTest()
    Consts.persistDir.mkdirs()

    and:
    def multiShard = new MultiShard(Consts.persistDir)
    def mySelfShard = multiShard.mySelfShard()
    mySelfShard.multiSlotRange.addSingle(8192, 12287) // inner slot 2 for slotNumber=4
    multiShard.updateClusterVersion(1)

    and:
    def snowFlakes = [new SnowFlake(1, 1)] as SnowFlake[]

    when:
    localPersist.initSlots((byte) 1, (short) 4, snowFlakes, Consts.persistDir, Config.create())

    then:
    localPersist.oneSlots().length == 4
    !localPersist.isOneSlotInitialized((short) 0)
    !localPersist.isOneSlotInitialized((short) 1)
    localPersist.isOneSlotInitialized((short) 2)
    !localPersist.isOneSlotInitialized((short) 3)
    localPersist.firstOneSlot().slot() == 2

    cleanup:
    ConfForGlobal.clusterEnabled = false
    ConfForGlobal.slotNumber = (short) 1
    localPersist.cleanUp()
    localPersist.resetForTest()
    Consts.persistDir.deleteDir()
}
```

Adjust setup to match existing `LocalPersistTest` helper style if needed.

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest.test cluster init slots only initializes local assigned slots"
```

Expected: FAIL because `isOneSlotInitialized` does not exist and current startup initializes all slots.

**Step 3: Commit**

Do not commit yet. This task intentionally leaves a failing test for the implementation task.

---

### Task 2: Add The Lazy Initialization API

**Files:**
- Modify: `src/main/java/io/velo/persist/LocalPersist.java`
- Test: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy`

**Step 1: Add constructor state needed for lazy initialization**

Change the existing `oneSlots` field to volatile and store the values that `initSlots(...)` needs later:

```java
private volatile OneSlot[] oneSlots;
private byte netWorkers;
private short slotNumber;
private SnowFlake[] snowFlakes;
private Eventloop[] slotWorkerEventloopArray;
private RequestHandler[] requestHandlerArray;
private long[] slotWorkerThreadIds;
private TaskRunnable[] slotTaskRunnables;
```

Keep existing `persistDir` and `persistConfig`. Add imports for `Eventloop`, `RequestHandler`, and `TaskRunnable` if they are not already present.

Use copy-on-write publication for lazy slot creation: `ensureOneSlotInitialized(...)` must clone the current array, set the new slot in the clone, and assign the clone back to the volatile `oneSlots` field. Do not publish by mutating `oneSlots[slot]` in place on the production lazy path. Runtime binding must happen before the volatile `oneSlots = newSlots` publication; scheduler registration may happen after publication.

**Step 2: Add slot initialization helpers**

Add:

```java
public boolean isOneSlotInitialized(short slot) {
    return oneSlots != null && slot >= 0 && slot < oneSlots.length && oneSlots[slot] != null;
}

public ArrayList<OneSlot> initializedOneSlots() {
    var list = new ArrayList<OneSlot>();
    if (oneSlots == null) {
        return list;
    }
    for (var oneSlot : oneSlots) {
        if (oneSlot != null) {
            list.add(oneSlot);
        }
    }
    return list;
}

public synchronized OneSlot ensureOneSlotInitialized(short slot) throws IOException {
    if (slot < 0 || oneSlots == null || slot >= oneSlots.length) {
        throw new IllegalArgumentException("Invalid slot=" + slot);
    }
    var existing = oneSlots[slot];
    if (existing != null) {
        return existing;
    }

    var i = slot % netWorkers;
    var oneSlot = new OneSlot(slot, slotNumber, snowFlakes[i], persistDir, persistConfig);
    oneSlot.initFds();
    if (isDebugMode) {
        oneSlot.debugMode();
    }
    bindOneSlotIfInitialized(oneSlot);

    var newSlots = oneSlots.clone();
    newSlots[slot] = oneSlot;
    oneSlots = newSlots;
    registerOneSlotWithTaskRunnableIfInitialized(oneSlot);
    return oneSlot;
}

private void bindOneSlotIfInitialized(@NotNull OneSlot oneSlot) {
    if (slotWorkerThreadIds == null) {
        return;
    }
    int i = oneSlot.slot() % slotWorkerThreadIds.length;
    oneSlot.threadIdProtectedForSafe = slotWorkerThreadIds[i];
    if (slotWorkerEventloopArray != null) {
        oneSlot.setSlotWorkerEventloop(slotWorkerEventloopArray[i]);
    }
    if (requestHandlerArray != null) {
        oneSlot.setRequestHandler(requestHandlerArray[i]);
    }
}

private void registerOneSlotWithTaskRunnableIfInitialized(@NotNull OneSlot oneSlot) {
    if (slotTaskRunnables == null) {
        return;
    }
    int i = oneSlot.slot() % slotTaskRunnables.length;
    var taskRunnable = slotTaskRunnables[i];
    if (taskRunnable != null) {
        taskRunnable.addOneSlotIfCharged(oneSlot);
    }
}
```

**Step 3: Mark test-only in-place slot mutators**

`addOneSlot(...)` and `addOneSlotForTest2(...)` may keep their single-threaded test setup behavior, but add a short comment that they are exempt from production copy-on-write publication. Do not copy their direct `oneSlots[slot] = oneSlot` pattern into production paths.

**Step 4: Update `initSlots(...)` to use the helper**

In `initSlots(...)`:

- Save `netWorkers`, `slotNumber`, `snowFlakes`, `persistDir`, and `persistConfig`.
- Initialize `ConfVolumeDirsForSlot` and `isHashSaveMemberTogether` as today.
- Allocate `oneSlots = new OneSlot[slotNumber]`.
- Load `multiShard = new MultiShard(persistDir)` before deciding which slots to initialize.
- If cluster lazy startup applies, call `ensureOneSlotInitialized(slot)` for only local owned inner slots.
- Otherwise call it for every slot.

Add helper:

```java
private TreeSet<Short> localAssignedInnerSlots() {
    var slots = new TreeSet<Short>();
    if (!ConfForGlobal.clusterEnabled || multiShard == null) {
        return slots;
    }
    var mySelfShard = multiShard.mySelfShard();
    if (mySelfShard == null) {
        return slots;
    }
    var step = MultiShard.TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber;
    for (var range : mySelfShard.getMultiSlotRange().getList()) {
        var firstBoundary = ((range.getBegin() + step - 1) / step) * step;
        for (int toClientSlot = firstBoundary; toClientSlot <= range.getEnd(); toClientSlot += step) {
            slots.add(MultiShard.asInnerSlotByToClientSlot(toClientSlot));
        }
    }
    return slots;
}
```

Use the fallback:

```java
var localSlots = localAssignedInnerSlots();
if (ConfForGlobal.clusterEnabled && !localSlots.isEmpty()) {
    for (var slot : localSlots) {
        ensureOneSlotInitialized(slot);
    }
} else {
    for (short slot = 0; slot < slotNumber; slot++) {
        ensureOneSlotInitialized(slot);
    }
}
```

**Step 5: Make startup metrics reinitialization null-safe**

Before running the test, update `initSlotsAgainAfterMultiShardLoadedOrChanged()`, `cleanUp()`, and `cleanUpAsync()` to iterate `initializedOneSlots()` instead of raw `oneSlots`. This is part of Task 2 because `initSlots(...)` calls metrics reinitialization immediately, and the new sparse startup test calls `cleanUp()` from its Spock cleanup block.

**Step 6: Run test**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest.test cluster init slots only initializes local assigned slots"
```

Expected: PASS for `initSlots(...)`, startup metrics reinitialization, and Spock cleanup. Later aggregate-loop failures belong to Task 3.

---

### Task 3: Make LocalPersist Loops Null-Safe

**Files:**
- Modify: `src/main/java/io/velo/persist/LocalPersist.java`
- Test: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy`

**Step 1: Update all-slot operations to iterate initialized slots only**

Change these methods to use `initializedOneSlots()`:

- `doSthInSlots(...)`
- `walLazyRead()`
- `initCheck()`
- `warmUp()`
- `debugMode()`
- `currentThreadFirstOneSlot()`

For promise arrays, size them by the initialized list:

```java
var slots = initializedOneSlots();
Promise<R>[] promises = new Promise[slots.size()];
for (int i = 0; i < slots.size(); i++) {
    var oneSlot = slots.get(i);
    promises[i] = oneSlot.asyncCall(() -> fnApplyOneSlot.apply(oneSlot));
}
```

**Step 2: Make direct accessors clear**

Keep `oneSlot(short slot)` as a raw lookup for compatibility:

```java
public OneSlot oneSlot(short slot) {
    return oneSlots[slot];
}
```

Do not make it initialize implicitly. Production paths that can require lazy creation should call `ensureOneSlotInitialized(...)` explicitly.

**Step 3: Update slot navigation helpers**

Update:

- `firstOneSlot()`
- `lastOneSlot()`
- `nextOneSlot(short slot)`
- `getInnerOneSlotByToClientSlot(Integer toClientSlot)`

`getInnerOneSlotByToClientSlot(...)` can directly index after bounds checks:

```java
var slot = MultiShard.asInnerSlotByToClientSlot(toClientSlot);
if (slot < 0 || oneSlots == null || slot >= oneSlots.length) {
    return null;
}
return oneSlots[slot];
```

In non-cluster mode, preserve eager semantics and existing return values.

**Step 4: Run focused test**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest"
```

Expected: PASS.

---

### Task 4: Bind Lazily Created Slots To Worker Runtime State

**Files:**
- Modify: `src/main/java/io/velo/persist/LocalPersist.java`
- Modify: `src/main/java/io/velo/MultiWorkerServer.java`
- Modify: `src/main/java/io/velo/task/TaskRunnable.java`
- Test: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy`
- Test: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy` if existing coverage exposes scheduler binding

**Step 1: Wire runtime binding state in LocalPersist**

Task 2 already introduced the runtime binding fields and null-guarded helpers. In this task, implement `initSlotRuntimeBindings(...)` so startup stores the arrays and binds every currently initialized slot by calling `bindOneSlotIfInitialized(oneSlot)`.

```java
public void initSlotRuntimeBindings(@NotNull Eventloop[] slotWorkerEventloopArray,
                                    @NotNull RequestHandler[] requestHandlerArray,
                                    @NotNull long[] slotWorkerThreadIds) {
    this.slotWorkerEventloopArray = slotWorkerEventloopArray;
    this.requestHandlerArray = requestHandlerArray;
    this.slotWorkerThreadIds = slotWorkerThreadIds;
    for (var oneSlot : initializedOneSlots()) {
        bindOneSlotIfInitialized(oneSlot);
    }
}
```

The invariant remains: lazy `ensureOneSlotInitialized(...)` must call `bindOneSlotIfInitialized(oneSlot)` before `oneSlots = newSlots`. Do not move binding after the volatile publish.

**Step 2: Update MultiWorkerServer startup**

Replace the explicit `fixSlotThreadId(...)` loop with:

```java
localPersist.initSlotRuntimeBindings(slotWorkerEventloopArray, requestHandlerArray, slotWorkerThreadIds);
```

Also call `localPersist.initSlotTaskRunnables(scheduleRunnableArray)` after the scheduler array is populated. This preserves eager slot binding and gives future lazy slots enough state to bind themselves and join their worker task loop.

**Step 3: Update TaskRunnable**

Change `TaskRunnable.oneSlots` from `ArrayList<OneSlot>` to `CopyOnWriteArrayList<OneSlot>` and import `java.util.concurrent.CopyOnWriteArrayList`. Lazy registration can be triggered from a different worker than the target slot worker, so a plain `ArrayList` can race with `TaskRunnable.run()` iteration. Mutations are rare and iteration is frequent, so snapshot iteration is the simplest safe model.

Make `chargeOneSlots(...)` skip null entries:

```java
for (var oneSlot : oneSlots) {
    if (oneSlot == null) {
        continue;
    }
    if (oneSlot.slot() % slotWorkers == slotWorkerId) {
        this.oneSlots.add(oneSlot);
        oneSlot.setSlotWorkerEventloop(slotWorkerEventloop);
        oneSlot.setRequestHandler(requestHandler);
    }
}
```

Note: lazily initialized slots after scheduler startup will not be in the existing `TaskRunnable.oneSlots` list unless they are explicitly registered. Add a method:

```java
public void addOneSlotIfCharged(OneSlot oneSlot) {
    if (oneSlot.slot() % slotWorkers != slotWorkerId || oneSlots.contains(oneSlot)) {
        return;
    }
    oneSlots.add(oneSlot);
    oneSlot.setSlotWorkerEventloop(slotWorkerEventloop);
    oneSlot.setRequestHandler(requestHandler);
}
```

Add `initSlotTaskRunnables(...)` to populate the `slotTaskRunnables` field introduced in Task 2 and register any already initialized slots:

```java
public void initSlotTaskRunnables(@NotNull TaskRunnable[] slotTaskRunnables) {
    this.slotTaskRunnables = slotTaskRunnables;
    for (var oneSlot : initializedOneSlots()) {
        registerOneSlotWithTaskRunnableIfInitialized(oneSlot);
    }
}
```

`ensureOneSlotInitialized(...)` already calls `registerOneSlotWithTaskRunnableIfInitialized(oneSlot)` after volatile publication, so slots created after scheduler startup join their background task loop immediately. Add a deterministic focused test where a setslot-style lazy initialization of a slot owned by a non-zero worker registers with that worker from a different thread; assert the target `TaskRunnable.oneSlots.contains(oneSlot)` and that a `run()` tick completes without exception.

**Step 4: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest" --tests "io.velo.task.TaskRunnableTest"
```

Expected: PASS.

---

### Task 4.5: Make Failover And Read-Gate Loops Null-Safe

**Files:**
- Modify: `src/main/java/io/velo/repl/LeaderSelector.java`
- Modify: `src/main/java/io/velo/persist/LocalPersist.java`
- Test: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy` if existing setup is suitable
- Test: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy` for read-gate fan-out behavior

**Step 1: Add sparse-slot regression tests**

Add focused tests for a 4-slot sparse `LocalPersist` where only slot 2 is initialized:

- Calling the reset-as-master path must not dereference slots 0, 1, or 3.
- Calling the reset-as-slave path must not dereference slots 0, 1, or 3.
- Calling `publishStreamReadyAndRefreshGate(...)` must fan out only to initialized slots.

Prefer direct `LeaderSelector` path tests if existing setup supports them. If setup is too heavy, explicitly extract package-visible helpers for the promise-list construction in `resetAsMaster` and `resetAsSlave`, keeping the existing per-slot lambda bodies unchanged, then test those helpers with a sparse `LocalPersist`. Do not leave the full-slot raw loops untested.

**Step 2: Update `LeaderSelector.resetAsMaster(...)`**

Replace the `new Promise[ConfForGlobal.slotNumber]` plus `for (int i = 0; i < ConfForGlobal.slotNumber; i++)` loop with iteration over `localPersist.initializedOneSlots()`. Size the promise array from the initialized list.

**Step 3: Update `LeaderSelector.resetAsSlave(...)`**

Apply the same initialized-slot iteration pattern. The operation resets this local node state, so it should affect initialized local slots only.

**Step 4: Update `LocalPersist.publishStreamReadyAndRefreshGate(...)`**

Replace the full `ConfForGlobal.slotNumber` fan-out with `initializedOneSlots()` or an explicit null skip:

```java
for (var ts : initializedOneSlots()) {
    ts.asyncRun(() -> {
        if (ts.isReadonly() && ts.isCanRead() != gateOpen) {
            ts.setCanRead(gateOpen);
        }
    });
}
```

**Step 5: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest" --tests "io.velo.repl.LeaderSelectorTest"
```

Expected: PASS. If `LeaderSelectorTest` does not exist or cannot cover this path directly, document the exact alternate focused test class used.

---

### Task 5: Initialize Slots On Migration And Ownership Paths

**Files:**
- Modify: `dyn/src/io/velo/command/ManageCommand.groovy`
- Modify: `dyn/src/io/velo/command/ClusterxCommand.groovy`
- Test: relevant command tests if existing test setup can load dynamic Groovy commands

**Step 1: Update `ManageCommand.migrateFrom()`**

Before:

```groovy
def oneSlot = localPersist.oneSlot(innerSlot)
```

After:

```groovy
def oneSlot = localPersist.ensureOneSlotInitialized(innerSlot as short)
```

Also replace `localPersist.oneSlot((short) 0).asyncRun(...)` with a safe first initialized slot:

```groovy
def controlSlot = localPersist.firstOneSlot()
if (controlSlot == null) {
    controlSlot = localPersist.ensureOneSlotInitialized(innerSlot as short)
}
controlSlot.asyncRun(() -> {
    def mySelfShard = LocalPersist.instance.multiShard.mySelfShard()
    mySelfShard.importMigratingSlot = toClientSlot
})
```

This avoids assuming slot 0 exists when this node owns a higher slot range.

**Step 2: Update `ClusterxCommand.setslot()`**

Only initialize when the target node is this node:

```groovy
def oneSlot = isMySelfNodeChargeThisInnerSlot
        ? localPersist.ensureOneSlotInitialized(innerSlot as short)
        : localPersist.oneSlot(innerSlot)
```

For the non-local case, if `oneSlot == null`, skip `flush()` because there is no local persistence for that slot:

```groovy
if (oneSlot == null) {
    finalPromise.set(OK)
    return
}
```

If review expects exported slots to exist, keep `flush()` only for initialized slots and log when skipped.

**Step 3: Run command-focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.command.*Command*Test" --tests "io.velo.repl.cluster.*Test"
```

If this is too broad or slow, run the specific existing tests covering `ManageCommand` and `ClusterxCommand`.

Expected: PASS.

---

### Task 6: Cover Startup Operations On Sparse Slot Arrays

**Files:**
- Modify: `src/test/groovy/io/velo/persist/LocalPersistTest.groovy`

**Step 1: Add tests for null-safe aggregate methods**

Add coverage that sparse `oneSlots` still supports:

- `walLazyRead()`
- `initCheck()`
- `warmUp()`
- `doSthInSlots(...)`
- `cleanUp()`

Expected behavior: only initialized slots are touched, and aggregate operations succeed when at least one slot is initialized.

**Step 2: Add cluster navigation edge-case coverage**

Add a test where this node owns only inner slot 2, while another shard owns client slots `0-4095`. Assert `firstOneSlot() == null` because the globally first client slot maps to an uninitialized non-local inner slot, and assert migration control-slot logic uses `ensureOneSlotInitialized(innerSlot)` when `firstOneSlot()` is null. If the dynamic command path is hard to invoke directly, cover the `LocalPersist` behavior here and cover `ManageCommand` fallback in Task 5.

**Step 3: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest"
```

Expected: PASS.

---

### Task 7: Verify Coverage And Build

**Files:**
- No source modifications expected.

**Step 1: Run focused test suite**

Run:

```bash
./gradlew :test --tests "io.velo.persist.LocalPersistTest" --tests "io.velo.task.TaskRunnableTest" --tests "io.velo.repl.LeaderSelectorTest" --tests "io.velo.repl.cluster.*Test"
```

Expected: PASS.

**Step 2: Check JaCoCo coverage for changed production code**

Run after tests. Use the actual post-edit line ranges from `nl -ba` rather than the stale ranges below; the changed methods must be covered:

```bash
python3 scripts/jacoco_cover.py io.velo.persist.LocalPersist <localPersistStart> <localPersistEnd> --src
python3 scripts/jacoco_cover.py io.velo.task.TaskRunnable <taskRunnableStart> <taskRunnableEnd> --src
python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector <leaderSelectorStart> <leaderSelectorEnd> --src
```

Expected: changed branches for eager startup, cluster lazy startup, copy-on-write publication, explicit lazy initialization, null-safe iteration, `CopyOnWriteArrayList` task registration, read-gate fan-out, and the two `LeaderSelector` reset loops are covered. If any changed branch is uncovered, add a focused Spock case before proceeding.

**Step 3: Compile dynamic Groovy and Java paths**

Run:

```bash
./gradlew :compileJava
./gradlew :test --tests "io.velo.persist.LocalPersistTest"
```

Expected: PASS.

---

## Review Checklist

- Lazy initialization preserves `oneSlots.length == ConfForGlobal.slotNumber`.
- `oneSlot(short)` remains a raw lookup and does not hide unexpected slot creation.
- Only explicit migration/ownership paths initialize previously unowned slots.
- Startup behavior is unchanged in non-cluster mode.
- Cluster mode with empty metadata keeps compatibility by eager-initializing all slots.
- Background tasks are registered for lazily initialized slots.
- `firstOneSlot()` no longer assumes slot 0 exists in cluster mode.
- JaCoCo confirms changed lazy/eager branches are executed.

