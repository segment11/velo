# Pipeline AUTH ACL Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make pipelined requests observe the current socket auth state so `AUTH` followed by another command in the same pipeline succeeds normally.

**Architecture:** The bug is in `MultiWorkerServer.handlePipeline(...)`, which snapshots `BaseCommand.getAuthU(socket)` once and copies that stale `U` into every request before execution. The minimal fix is to treat auth as per-request socket state and refresh `request.u` immediately before the ACL check in `handleRequest(...)`, leaving slot parsing and pipeline assembly unchanged.

**Tech Stack:** Java 21, Groovy/Spock, ActiveJ, Gradle, JaCoCo

---

### Task 1: Add a focused pipeline-auth regression

**Files:**
- Modify: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`
- Reference: `src/main/java/io/velo/MultiWorkerServer.java:621-646`
- Reference: `src/main/java/io/velo/decode/Request.java:125-138`

**Step 1: Write the failing test**

Add a new Spock method in `src/test/groovy/io/velo/MultiWorkerServerTest.groovy` that:

- creates a `MultiWorkerServer` test fixture with the existing local setup used by the file
- enables auth by setting `ConfForGlobal.PASSWORD`
- ensures the default ACL user is enabled with the matching password
- builds a two-request pipeline:
  - `AUTH password`
  - `PING`
- runs `handlePipeline(...)`
- asserts the combined RESP output is `+OK\r\n+PONG\r\n`

The important failure signal on current code is that the second command is rejected with the ACL error reply instead of `+PONG`.

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test pipeline auth refresh acl state"
```

Expected: FAIL, with the response containing the ACL permit error for the second pipelined command.

### Task 2: Refresh auth state per request at dispatch time

**Files:**
- Modify: `src/main/java/io/velo/MultiWorkerServer.java`
- Reference: `src/main/java/io/velo/MultiWorkerServer.java:621-646`
- Reference: `src/main/java/io/velo/MultiWorkerServer.java:447-468`

**Step 1: Remove the stale pipeline snapshot**

In `handlePipeline(...)`:

- delete the single `var u = BaseCommand.getAuthU(socket);`
- stop assigning the same `u` to every request during the preprocessing loop
- keep the existing slot parsing and slot-number setup intact

**Step 2: Refresh `request.u` inside `handleRequest(...)`**

Before the ACL check in `handleRequest(...)`, set:

```java
request.setU(BaseCommand.getAuthU(socket));
```

This makes each request observe the socket auth state after any earlier request in the same pipeline has updated it.

**Step 3: Keep the change minimal**

- do not change `RequestHandler` auth semantics
- do not move slot parsing
- do not add new auth caches or request fields

### Task 3: Verify the regression and touched lines

**Files:**
- Test: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`
- Inspect: `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html`

**Step 1: Run the focused test**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test pipeline auth refresh acl state"
```

Expected: PASS

**Step 2: Run nearby server tests**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest"
```

Expected: PASS

**Step 3: Check JaCoCo**

Inspect:

- `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html`

Confirm the changed lines in `handlePipeline(...)` and `handleRequest(...)` are covered, especially the new per-request `request.setU(BaseCommand.getAuthU(socket));` path.

### Task 4: Commit the fix

**Files:**
- Commit: `src/main/java/io/velo/MultiWorkerServer.java`
- Commit: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`

**Step 1: Commit**

```bash
git add src/main/java/io/velo/MultiWorkerServer.java src/test/groovy/io/velo/MultiWorkerServerTest.groovy
git commit -m "fix: refresh auth state per pipeline request"
```
