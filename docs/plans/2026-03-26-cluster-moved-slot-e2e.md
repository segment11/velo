# Cluster MOVED Slot E2E Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a focused e2e test that proves Velo returns the Redis client slot in `MOVED` replies, not the inner Velo slot.

**Architecture:** Use one `VeloServer` process in cluster mode and preseed its `persist/nodes.json` with a local shard and a remote shard. Pick a key in the remote shard’s Redis slot range, send a raw RESP `GET`, and assert the wire reply contains the expected Redis client slot.

**Tech Stack:** Groovy/Spock, VeloServer, Jackson, Jedis CRC16 helper, Gradle

---

### Task 1: Add the failing e2e test

**Files:**
- Create: `src/test/groovy/io/velo/e2e/ClusterMovedSlotTest.groovy`
- Reference: `src/main/groovy/io/velo/test/tools/VeloServer.groovy`
- Reference: `src/main/java/io/velo/repl/cluster/MultiShard.java`

**Step 1: Write the new test**

Create a single test that:
- writes a valid `persist/nodes.json`
- starts one `VeloServer` with `clusterEnabled=true`
- finds a key whose Redis client slot is in `8192-16383`
- sends raw RESP `GET`
- expects `-MOVED <slot> 127.0.0.1:<remotePort>\r\n`

**Step 2: Run the targeted test to verify red**

Run:

```bash
./gradlew :test --tests "io.velo.e2e.ClusterMovedSlotTest"
```

Expected:
- the test fails
- the actual reply contains `MOVED 0 ...` while the expected reply contains the non-zero Redis client slot

### Task 2: Keep cleanup deterministic

**Files:**
- Modify: `src/test/groovy/io/velo/e2e/ClusterMovedSlotTest.groovy`

**Step 1: Clean up runtime artifacts**

Ensure the test removes:
- the temporary data directory
- the generated `velo-port<port>.properties` file
- the spawned server process

**Step 2: Re-run the targeted test if cleanup changes**

Run:

```bash
./gradlew :test --tests "io.velo.e2e.ClusterMovedSlotTest"
```

Expected:
- still red for the slot-value bug
- no leftover process or temp-file noise from the test itself
