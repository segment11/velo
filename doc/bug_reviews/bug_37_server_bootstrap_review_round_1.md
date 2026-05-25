# Bug 37: Server Bootstrap Review, Round 1

Scope: Server bootstrap module, centered on `doc/design/12_server_bootstrap_design.md` and `src/main/java/io/velo/MultiWorkerServer.java`.

## Design Document Match Check

`doc/design/12_server_bootstrap_design.md` is broadly aligned with the current codebase at the architectural level:

- `MultiWorkerServer` is still the ActiveJ `Launcher`.
- Startup still validates core configuration, creates/locks the data directory, initializes dictionaries and persistence, creates handlers/tasks, starts slot worker scheduling, loads ACL users, registers JVM collectors, and wires replication leadership helpers.
- Dynamic Groovy wiring still happens through `CachedGroovyClassLoader` and `RefreshLoader`.
- Shutdown is still centralized in `onStop()` and covers request handlers, task runnables, leader selector, Jedis pool holder, local persistence, dictionary map, runtime collector, sockets, event loops, and pid-file resources.

One wording gap: the document says cleanup is centralized in "`run()`/lifecycle handling", but `run()` only calls `awaitShutdown()`; the concrete cleanup path is `onStop()`.

## Finding 1: CLI config file cannot control low-level ActiveJ socket settings

**Severity:** High

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:815-834`
- `src/main/java/io/velo/MultiWorkerServer.java:836-853`
- `src/main/java/io/velo/MultiWorkerServer.java:1645-1648`

**Code excerpt:**

```java
private static void prepareConfig() {
    var configFilePath = configFilePath();
    var config = Config.create()
            .overrideWith(ofProperties(configFilePath, true));
    ...
}

static {
    prepareConfig();
}

private static String configFilePath() {
    if (MAIN_ARGS != null && MAIN_ARGS.length > 0) {
        filePath = MAIN_ARGS[0];
    } else {
        ...
    }
}

public static void main(String[] args) throws Exception {
    MAIN_ARGS = args;
    Launcher launcher = new MultiWorkerServer();
    launcher.launch(args);
}
```

**Root cause and impact:**

Java initializes `MultiWorkerServer` before entering `main()`, so the static initializer calls `prepareConfig()` while `MAIN_ARGS` is still `null`. As a result, `ApplicationSettings.SocketSettings.*` and `ApplicationSettings.ServerSocketSettings.*` are loaded from the default discovery path, not from a config file passed as `java ... io.velo.MultiWorkerServer /path/to/custom.properties`.

The later `config()` provider can load the CLI file after `MAIN_ARGS = args`, but the socket settings were already copied into `System` properties by the static initializer. Operators using a per-instance config file can get the requested listen address and Velo settings while silently retaining default socket buffer/backlog/tcp settings. This can cause production instances started from alternate config files to run with unexpected network tuning.

**Suggested fix direction:**

Move the socket-settings system-property setup out of the static initializer and run it after CLI args are known. A minimal option is to make `main()` call a variant of `prepareConfig(args)` before constructing the launcher, while tests can call the same helper explicitly.

## Finding 2: Non-positive worker counts are not rejected before bootstrap uses them

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1507-1563`
- `src/main/java/io/velo/MultiWorkerServer.java:266-277`
- `src/main/java/io/velo/persist/index/IndexHandlerPool.java:43-67`

**Code excerpt:**

```java
int netWorkers = config.get(ofInteger(), "netWorkers", 1);
if (netWorkers > MAX_NET_WORKERS) {
    throw new IllegalArgumentException(...);
}
...
if (slotNumber % netWorkers != 0) {
    throw new IllegalArgumentException(...);
}

int slotWorkers = config.get(ofInteger(), "slotWorkers", 1);
...
if (slotNumber % slotWorkers != 0) {
    throw new IllegalArgumentException(...);
}

int indexWorkers = config.get(ofInteger(), "indexWorkers", 1);
if (indexWorkers > MAX_INDEX_WORKERS) {
    throw new IllegalArgumentException(...);
}
ConfForGlobal.indexWorkers = (byte) indexWorkers;
```

```java
public IndexHandlerPool(byte indexWorkers, File persistDir, Config persistConfig) throws IOException {
    this.indexHandlers = new IndexHandler[indexWorkers];
    this.workerEventloopArray = new Eventloop[indexWorkers];
    ...
    this.keyAnalysisHandler = new KeyAnalysisHandler(keysDir, workerEventloopArray[0], persistConfig);
}
```

**Root cause and impact:**

`beforeCreateHandler()` validates upper bounds, CPU bounds, and slot divisibility, but it never explicitly requires `netWorkers`, `slotWorkers`, or `indexWorkers` to be greater than zero.

This produces inconsistent and late failures:

- `netWorkers=0` or `slotWorkers=0` reaches `slotNumber % workers` and throws `ArithmeticException: / by zero`, not a controlled configuration error.
- Negative `netWorkers` and `slotWorkers` can pass the upper-bound checks and fail later when arrays or worker pools are created.
- `indexWorkers=0` is accepted into `ConfForGlobal.indexWorkers`; `IndexHandlerPool` then creates zero-length arrays and immediately reads `workerEventloopArray[0]`, causing `ArrayIndexOutOfBoundsException`.

The design document states bootstrap validates worker counts. The current validation rejects some invalid counts but lets zero and negative counts escape into lower-level bootstrap code, making startup failures harder to diagnose and potentially leaving partially initialized persistence/directory state.

**Suggested fix direction:**

Add explicit `<= 0` checks for all three worker counts in `beforeCreateHandler()` before any modulo, cast, or initialization side effects. Add focused Spock coverage for `netWorkers=0`, `slotWorkers=0`, `indexWorkers=0`, and negative values.

## Finding 3: Reusing an existing pid file can leave stale digits from the previous process id

**Severity:** Low

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:173-204`
- `src/main/java/io/velo/MultiWorkerServer.java:1271-1282`

**Code excerpt:**

```java
pidFileChannel = FileChannel.open(pidFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
pidFileLock = pidFileChannel.tryLock();
...
long processId = ProcessHandle.current().pid();
var bytes = String.valueOf(processId).getBytes();
var buffer = ByteBuffer.wrap(bytes);
int n = pidFileChannel.write(buffer);
```

```java
if (pidFileLock != null) {
    pidFileLock.release();
    pidFileChannel.close();
    if (!pidFile.delete()) {
        System.err.println("Delete pid file failed, file=" + pidFile.getAbsolutePath());
    }
}
```

**Root cause and impact:**

`dirFile()` opens an existing pid file with `CREATE` and `WRITE`, but without `TRUNCATE_EXISTING`. If a prior process crashed or shutdown could not delete the pid file, the next process writes its PID at the start of the existing file and leaves any remaining bytes intact when the new PID string is shorter than the old one.

For example, an old file containing `123456` followed by a new process id `789` becomes `789456`. The file lock still protects concurrent Velo instances, but operational tooling that reads `velo.pid` can report or act on a nonexistent or wrong process id.

**Suggested fix direction:**

Open the pid file with `StandardOpenOption.TRUNCATE_EXISTING`, or explicitly call `pidFileChannel.truncate(0)` before writing the new PID. A focused test can pre-create a pid file with a longer value, call `dirFile(config, true)`, and assert the resulting file content is exactly the current PID string.

---

## Reviewer Notes (Agent 2, Round 1)

### Finding 1: CLI config file cannot control low-level ActiveJ socket settings — **Confirmed**

Verified against `MultiWorkerServer.java:813-834` and `1645-1648`.

The bug is real. The JVM executes the `static { prepareConfig(); }` block (line 832-834) during class initialization, which occurs before `main()` runs. At that point `MAIN_ARGS` is still `null`, so `configFilePath()` (line 836) always falls through to the default discovery path. The `System.setProperty(...)` calls at lines 824 and 829 set socket tuning from the wrong file, and these properties persist even after `config()` (line 862) later loads the correct CLI file.

The analysis is accurate. The suggested fix direction is sound — move the property-setting logic to after `MAIN_ARGS = args` in `main()`.

**Severity: High — confirmed.** This is a silent misconfiguration in production deployments that use per-instance config files.

### Finding 2: Non-positive worker counts are not rejected before bootstrap uses them — **Confirmed**

Verified against `MultiWorkerServer.java:1507-1563`, `266-277`, and `IndexHandlerPool.java:43-67`.

The code at lines 1523-1563 checks upper bounds and CPU bounds but has no `<= 0` guard for any of the three worker counts:

- `netWorkers=0` (line 1523): passes the `> MAX_NET_WORKERS` check (0 is not > max), passes `> cpuNumber` (0 is not > cpu), then hits `slotNumber % 0` at line 1533 — `ArithmeticException`.
- `slotWorkers=0` (line 1539): same pattern, `ArithmeticException` at line 1549.
- `indexWorkers=0` (line 1555): passes all checks, gets cast to `(byte) 0` at line 1562. `IndexHandlerPool` at line 44-45 creates zero-length arrays, then line 66 reads `workerEventloopArray[0]` — `ArrayIndexOutOfBoundsException`.
- Negative values: `netWorkers=-1` passes the upper-bound and CPU-bound checks (`-1 > cpuNumber` is false for any positive CPU count). The modulo `slotNumber % -1` yields 0 (valid in Java), so it would be stored as `(byte) -1` and fail later when creating arrays/worker pools.

All three sub-scenarios are confirmed. The suggested fix of adding `<= 0` checks is correct.

**Severity: Medium — confirmed.** These are edge-case misconfigurations that produce confusing errors instead of clear validation messages.

### Finding 3: Reusing an existing pid file can leave stale digits from the previous process id — **Confirmed**

Verified against `MultiWorkerServer.java:189-198` and `1271-1282`.

Line 189 opens the file with `StandardOpenOption.CREATE, StandardOpenOption.WRITE` — no `TRUNCATE_EXISTING`. The `FileChannel.write(ByteBuffer)` at line 198 writes from position 0 but does not truncate trailing bytes. If the old file contained `123456` (6 bytes) and the new PID is `789` (3 bytes), the file will contain `789456`.

The file lock mechanism (line 190-192) prevents concurrent Velo instances, so this only manifests when a prior instance crashed or was killed without clean shutdown. The scenario is realistic in production — unclean exits happen, and monitoring/operational tooling reading `velo.pid` would see a wrong PID.

**Severity: Low — confirmed.** The fix is trivial: add `StandardOpenOption.TRUNCATE_EXISTING` to the `FileChannel.open()` call.

### Summary

| Finding | Severity | Verdict |
|---------|----------|---------|
| 1 — Static initializer runs before CLI args | High | **Confirmed** |
| 2 — Non-positive worker counts not rejected | Medium | **Confirmed** |
| 3 — Pid file stale digits without truncate | Low | **Confirmed** |

All three bugs are confirmed. Ready for fix phase.

---

## Review Feedback for Finding 1 Fix Commit

Reviewed commit: `f8415318878ca72aa02ca0d2f98002b58175dc04` (`fix: move prepareConfig() from static initializer to main() after MAIN_ARGS is set`)

### Summary of the fix

The commit removes the `static { prepareConfig(); }` initializer from `MultiWorkerServer` and calls `prepareConfig()` from `main()` immediately after `MAIN_ARGS = args`. This addresses the confirmed production root cause for the normal server entry point: the CLI config path is now available before low-level ActiveJ socket settings are copied into `System` properties.

### Strengths

- The production change is minimal and targets the actual ordering bug without changing unrelated bootstrap behavior.
- `prepareConfig()` remains close to the existing config discovery path and still reuses `configFilePath()`, keeping the fix easy to audit.
- The new regression test demonstrates that `prepareConfig()` uses `MAIN_ARGS` when set and applies both socket and server-socket settings from a custom properties file.

### Concerns

**Minor: test cleanup leaves two modified global system properties behind.**

`MultiWorkerServerTest.groovy:1666-1698` saves and restores only:

- `io.activej.reactor.net.SocketSettings.sendBufferSize`
- `io.activej.reactor.net.ServerSocketSettings.backlog`

But the test also sets and asserts:

- `io.activej.reactor.net.SocketSettings.receiveBufferSize`
- `io.activej.reactor.net.ServerSocketSettings.receiveBufferSize`

Those two properties remain set to the test values (`222222` and `333333`) after cleanup. This is not a production bug, but it is avoidable test pollution and can make later tests in the same JVM depend on execution order.

**Status: Addressed.** The cleanup block was updated to save and restore all four properties (`sendBufferSize`, `receiveBufferSize`, `backlog`, `ServerSocketSettings.receiveBufferSize`).

### Verification

Ran focused verification:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test prepareConfig respects MAIN_ARGS for socket settings"
```

Result: passed, with `tests="1"`, `failures="0"`, `errors="0"` in `build/test-results/test/TEST-io.velo.MultiWorkerServerTest.xml`.

JaCoCo inspection: `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html` shows `prepareConfig()` lines `816-830` covered (`fc`) by the focused test.

### Follow-ups

- Pre-merge: restore the two missing system properties in the new test cleanup.
- Optional: add a small helper in the test to snapshot/restore all touched `io.activej.reactor.net.*` properties to avoid repeating cleanup logic.

---

## Review Feedback for Finding 1 Fix Commit, Follow-up

Reviewed commit: `0b5de74893011ad5e1e84f08de6004e8f12528d2` (`fix: move prepareConfig() from static initializer to main() after MAIN_ARGS is set`)

### Summary of the fix

The current commit keeps the production fix from the earlier review: `prepareConfig()` no longer runs from a static initializer and is called from `main()` after `MAIN_ARGS = args`. The regression test now snapshots and restores all four system properties it mutates, so the previous test-pollution concern is addressed in code.

### Strengths

- Production bootstrap ordering now matches the root-cause fix for Finding 1.
- The regression test verifies both the default-path behavior when `MAIN_ARGS` is null and the custom file path when `MAIN_ARGS` is set.
- Test cleanup now restores all touched socket/server-socket system properties.

### Concerns

**Minor: stale follow-up text remains in the earlier review section.**

The earlier review notes now say the cleanup issue is addressed, but the old "Pre-merge: restore the two missing system properties" follow-up remains immediately below it. This is documentation noise only; it does not affect the production fix or tests.

### Verification

Ran fresh focused verification:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest.test prepareConfig respects MAIN_ARGS for socket settings" --rerun-tasks
```

Result: passed, with `tests="1"`, `failures="0"`, `errors="0"` in `build/test-results/test/TEST-io.velo.MultiWorkerServerTest.xml`.

JaCoCo inspection: `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html` shows `prepareConfig()` lines `816-830` covered (`fc`) and `configFilePath()` line `834` partially covered by the null/non-null `MAIN_ARGS` cases.

### Follow-ups

- Optional: remove or revise the stale earlier follow-up line so the document does not simultaneously say the cleanup concern is addressed and still required.

---

## Final Review Feedback (Agent 2, post-amend)

Reviewed commit: `0b5de748` (amended `fix: move prepareConfig() from static initializer to main() after MAIN_ARGS is set`)

### Summary

Production fix: `static { prepareConfig(); }` removed; `prepareConfig()` now called in `main()` after `MAIN_ARGS = args`. Visibility changed from `private` to package-private for testability. Test added that verifies both `MAIN_ARGS=null` (falls back to defaults) and `MAIN_ARGS=[customFile]` (reads from custom file), with full cleanup of all 4 mutated System properties.

### Code correctness

- **`main()` ordering is correct**: `MAIN_ARGS = args` → `prepareConfig()` → `new MultiWorkerServer()`. By the time the class is constructed, `configFilePath()` will use the CLI path.
- **No other entry point bypasses `main()` in production**: the ActiveJ `Launcher.launch()` path always starts from `main()`.
- **Tests that construct `MultiWorkerServer()` directly don't depend on pre-populated socket System properties**: verified by grepping the entire test tree — no other test references `io.activej.reactor.net.Socket*` or `ServerSocket*` properties.
- **The `prepareConfig()` visibility change (`private` → package-default) is the minimum needed**: tests in the same package can call it; it is still inaccessible outside `io.velo`.

### Test quality

- Tests both directions: `MAIN_ARGS=null` does NOT read custom file, `MAIN_ARGS=[path]` DOES read custom file.
- Cleanup restores all 4 System properties that the test mutates. No test pollution.
- JaCoCo confirms: `prepareConfig()` at 100% instruction coverage, `configFilePath()` at 85% (the `/etc/velo.properties` fallback branch is not exercised, which is expected in a unit test environment).

### Regressions

All 16 existing `MultiWorkerServerTest` tests pass. The 1 failure (`test mock inject and handle` — `TimeoutException`) is pre-existing and reproduces on the parent commit without this fix.

### Verdict

**Fix is correct and complete.** No further changes required for Finding 1.
