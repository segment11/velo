# Bug 37: Server Bootstrap Module Review Round 2

## Agent 1 Round 2 Review: Server Bootstrap and Shutdown

Scope: second-pass audit of `doc/design/12_server_bootstrap_design.md`, `MultiWorkerServer`, scheduler runnables, and shutdown-related bootstrap wiring.

### Design document match check

The design document still broadly matches the code at the module level: `MultiWorkerServer` is the ActiveJ `Launcher`, it owns config validation, pid/data directory setup, persistence/dictionary initialization, worker setup, ACL setup, metrics registration, network server wiring, replication helper scheduling, and centralized shutdown.

Shutdown coverage in the document is high-level. The concrete code path is `MultiWorkerServer.onStop()`, while `run()` only calls `awaitShutdown()`. Round 2 focused on shutdown behavior below that high-level description.

## Round 2 Finding 1: Withdrawn - `isStopping` is not reset after shutdown

**Severity:** Withdrawn

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1187-1196`
- `src/main/java/io/velo/MultiWorkerServer.java:976-1110`
- `src/main/java/io/velo/persist/OneSlot.java:838-843`

**Code excerpt:**

```java
public static volatile boolean isStopping = false;

@Override
protected void onStop() throws Exception {
    isStopping = true;
    try {
        ...
    }
}
```

```java
public void doTask(int loopCount) {
    if (MultiWorkerServer.isStopping) {
        return;
    }

    taskChain.doTask(loopCount);
    ...
}
```

**Original concern:**

`MultiWorkerServer.onStop()` sets the static `isStopping` flag to `true`, and `OneSlot.doTask()` uses that static flag to skip all scheduled slot work. The current startup path does not reset the flag to `false` in `main()`, `beforeCreateHandler()`, or `onStart()`.

**Correction after lifecycle review:**

This is not a confirmed production bug. The production entry point uses `launcher.launch(args)` from `MultiWorkerServer.main()`, and normal `onStop()` execution happens during process shutdown. A normal restart creates a fresh JVM, so the static `isStopping` value does not survive into the next server process.

The same-JVM case exists in unit tests that call `onStart()`/`onStop()` directly, but that is test harness behavior rather than the documented production lifecycle. A stale static flag may still be worth resetting in test setup if it causes test pollution, but it should not be tracked as an active server bootstrap bug from this review.

**Verdict:** Withdrawn.

## Round 2 Finding 2: `PrimaryTaskRunnable.stop()` still allows one more refresh/replication task execution

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/task/PrimaryTaskRunnable.java:29-40`
- `src/main/java/io/velo/task/PrimaryTaskRunnable.java:42-48`
- `src/main/java/io/velo/MultiWorkerServer.java:1040-1062`
- `src/main/java/io/velo/MultiWorkerServer.java:1202-1221`

**Code excerpt:**

```java
@Override
public void run() {
    task.accept(loopCount);
    loopCount++;

    if (isStopped) {
        return;
    }

    primaryEventloop.delay(1000L, this);
}

public void stop() {
    isStopped = true;
    System.out.println("Task delay stopped for primary eventloop");
}
```

```java
primaryScheduleRunnable = new PrimaryTaskRunnable(loopCount -> {
    if (loopCount % 10 == 0) {
        refreshLoader.refresh();
    }

    if (loopCount % 5 == 0) {
        try {
            doReplAfterLeaderSelect((short) 0);
        } catch (Exception e) {
            log.error("Repl leader select error", e);
        }
    }
});
```

**Root cause and impact:**

`PrimaryTaskRunnable.run()` checks `isStopped` only after it executes `task.accept(loopCount)`. During shutdown, `MultiWorkerServer.onStop()` calls `primaryScheduleRunnable.stop()` before cleaning up replication helpers and other resources. If a previously scheduled primary runnable fires after `stop()` is set, it still runs `refreshLoader.refresh()` and `doReplAfterLeaderSelect()` once before returning.

That means shutdown can still trigger dynamic Groovy refresh and replication leader-selection/reset logic while `onStop()` is closing the leader selector, Jedis pools, sockets, and persistence. The slot-worker `TaskRunnable` checks `isStopped` before doing work; the primary scheduler should follow the same ordering to avoid post-stop side effects.

**Suggested fix direction:**

Add an `isStopped` guard before invoking the callback, and keep a guard after the callback before rescheduling. The first guard prevents an already-stopped runnable from executing primary shutdown-sensitive work; the second guard prevents rescheduling if `stop()` is called while the callback is running.

```java
if (isStopped) {
    return;
}
task.accept(loopCount);
loopCount++;

if (isStopped) {
    return;
}

primaryEventloop.delay(1000L, this);
```

Add a small unit test for `PrimaryTaskRunnable` that calls `stop()` before `run()` and verifies the task callback is not invoked.

---

## Agent 2 Review Notes (Round 2)

### Finding 1: `isStopping` is never reset — **REFUTED (Not a practical bug)**

**Verification against current code:**

- `MultiWorkerServer.isStopping` is declared as `public static volatile boolean` at line 1187, initialized to `false`.
- It is set to `true` only in `onStop()` at line 1196.
- A codebase-wide grep for `isStopping = false` returns only the declaration at line 1187 — no assignment anywhere in `onStart()`, `beforeCreateHandler()`, `main()`, or any other startup/lifecycle method.
- `OneSlot.doTask()` at line 838-841 checks `MultiWorkerServer.isStopping` as a guard and returns immediately if true.

**Why this is not a practical bug:**

The original finding assumed same-JVM restart scenarios exist in the codebase. On review:

1. **Production:** When `onStop()` completes, the JVM process exits. The next startup runs in a fresh JVM with the field at its default `false`. No bug.
2. **Tests:** All `MultiWorkerServer` tests (checked `MultiWorkerServerTest.groovy` — 16 test methods that instantiate `new MultiWorkerServer()`) call `onStart()` but **never** call `onStop()`. Gradle is configured with `forkEvery = 1`, so each test runs in its own JVM. No test creates a stop-then-restart cycle.
3. **No same-JVM restart path exists** anywhere in the current codebase — neither in test helpers nor in embedded usage patterns.

The static flag design is defensive but not buggy given the actual lifecycle: `onStop()` → JVM exit → new JVM → `isStopping` defaults to `false`.

**Verdict:** No fix needed. If a same-JVM restart capability is added in the future, this would need to be addressed at that time.

---

### Finding 2: `PrimaryTaskRunnable.stop()` still allows one more task execution — **CONFIRMED**

**Verification against current code:**

- `PrimaryTaskRunnable.run()` at lines 31-40 executes `task.accept(loopCount)` **before** checking `isStopped`.
- `stop()` at line 46 sets `isStopped = true` but does not remove already-scheduled runnables from the eventloop.
- In `onStop()` (lines 1194-1221), `primaryScheduleRunnable.stop()` is called at line 1206 **before** cleanup of leader selector (line 1219), Jedis pools (line 1221), and local persist (line 1224).
- The lambda at lines 1041-1056 invokes `refreshLoader.refresh()` (Groovy class reloading) every 10 loops and `doReplAfterLeaderSelect()` every 5 loops — both are non-trivial operations that interact with resources being torn down in `onStop()`.

**Comparison with `TaskRunnable`:** Confirmed that `TaskRunnable.run()` (slot worker, line 73-76) checks `isStopped` at the **top** of `run()`, before doing any work. `PrimaryTaskRunnable` should follow the same pattern.

**Race scenario confirmed:** If the eventloop fires a previously-scheduled `PrimaryTaskRunnable` after `stop()` is called (the runnable was already submitted via `primaryEventloop.delay(1000L, this)` from the previous `run()`), it will execute `refresh()` or `doReplAfterLeaderSelect()` against resources that are mid-teardown.

**Severity agreed:** Medium — the window is narrow (one iteration at most) but the consequences include operating on closed resources during shutdown.

**Fix suggestion endorsed:** Move the `isStopped` check to the top of `PrimaryTaskRunnable.run()`, matching the `TaskRunnable` pattern.

---

### Summary

| Finding | Verdict | Severity | Fix status |
|---------|---------|----------|------------|
| 1. `isStopping` never reset on startup | **Refuted** — JVM exits on stop, no same-JVM restart path exists | N/A | No fix needed |
| 2. `PrimaryTaskRunnable` checks stop flag after work | **Confirmed** | Medium | **Fixed** (`acd47f88`) — `isStopped` guard moved before `task.accept()`; unit test added |
