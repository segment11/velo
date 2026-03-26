# Cluster Thread-Affinity Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix cluster slot validation so it no longer reads slot-worker-local shard state from the net worker thread before request dispatch.

**Architecture:** Keep the fix narrow. Preserve the current early cluster validation in `MultiWorkerServer.handleRequest(...)`, but stop using `RequestHandler.getMultiShardShadow()` there. Instead, resolve shard metadata through a thread-safe, non-slot-thread-local access path that is valid on the net worker, then keep the existing slot-worker-local shadow for worker-owned execution paths if it is still needed elsewhere.

**Tech Stack:** Java, Groovy/Spock, Gradle, ActiveJ, Velo cluster routing

---

### Task 1: Lock in the failing net-worker bug with a focused test

**Files:**
- Modify: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`
- Reference: `src/main/java/io/velo/MultiWorkerServer.java`
- Reference: `src/main/java/io/velo/RequestHandler.java`

**Step 1: Add a test that executes cluster validation from a non-slot thread**

Create a focused case that:
- sets `ConfForGlobal.clusterEnabled = true`
- initializes `RequestHandler.multiShardShadows`
- sets `STATIC_GLOBAL_V.slotWorkerThreadIds` to values that do not match the current test thread
- constructs a request whose key maps to a remote shard
- calls `m.checkClusterSlot(...)` or `m.handleRequest(...)` from the current thread

Expected current red behavior:
- either `ArrayIndexOutOfBoundsException` from `multiShardShadows[-1]`
- or another failure proving the lookup path is invalid off the slot worker

**Step 2: Run the targeted test to verify red**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest"
```

Expected:
- the new test fails for the wrong-thread lookup path

### Task 2: Replace the wrong-thread shard lookup with a safe source of truth

**Files:**
- Modify: `src/main/java/io/velo/MultiWorkerServer.java`
- Modify: `src/main/java/io/velo/RequestHandler.java`
- Reference: `src/main/java/io/velo/repl/cluster/MultiShardShadow.java`
- Reference: `src/main/java/io/velo/persist/LocalPersist.java`

**Step 1: Choose the narrow lookup boundary**

Recommended implementation:
- add a non-thread-local accessor for cluster shard lookup that can be called safely from `MultiWorkerServer.checkClusterSlot(...)`
- keep `RequestHandler.getMultiShardShadow()` for true slot-worker-local usages only

Possible shapes:
- expose a synchronized/static snapshot accessor from `RequestHandler`
- or read directly from `LocalPersist.getInstance().getMultiShard()`

Prefer the smallest change that avoids new shared mutable state hazards.

**Step 2: Keep `checkClusterSlot(...)` behavior unchanged except for data source**

Do not refactor the routing algorithm in this task:
- keep cross-shard rejection logic
- keep `MOVED` generation logic
- keep `IGNORE_TO_CLIENT_SLOT` handling

Only swap the shard metadata lookup path so it is valid on the net worker.

### Task 3: Verify the JVM regression tests green

**Files:**
- Test: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`

**Step 1: Run the focused server test**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test cluster slot cross shards"
```

Expected:
- pass
- exact `MOVED <toClientSlot> host:port` assertion still passes

**Step 2: Run the new wrong-thread regression**

Run the specific new test added in Task 1.

Expected:
- pass
- no `ArrayIndexOutOfBoundsException`

### Task 4: Re-run the VeloServer e2e cluster redirect test

**Files:**
- Test: `src/test/groovy/io/velo/e2e/ClusterMovedSlotTest.groovy`

**Step 1: Rebuild the jar**

Run:

```bash
./gradlew jar
```

Expected:
- build succeeds so `VeloServer` uses the patched server jar

**Step 2: Re-run the focused e2e**

Run:

```bash
./gradlew :test --tests "io.velo.e2e.ClusterMovedSlotTest"
```

Expected:
- pass
- the redirect still reports the Redis client slot correctly

### Task 5: Final verification checkpoint

**Files:**
- Review: `src/main/java/io/velo/MultiWorkerServer.java`
- Review: `src/main/java/io/velo/RequestHandler.java`
- Review: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`

**Step 1: Inspect JaCoCo immediately after the JVM test run**

Confirm the touched `MultiWorkerServer` cluster-routing lines are covered in:

```text
build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html
```

**Step 2: Summarize residual risk**

Residual risk after this fix:
- any other code path that still assumes slot-worker-local state while executing on the net worker
- the separate malformed HTTP Basic auth bug in `RequestHandler`
- the separate `-ERR MOVED` wire-format behavior if strict Redis compatibility is required later
