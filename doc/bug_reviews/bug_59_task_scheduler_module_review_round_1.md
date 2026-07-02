# Task Scheduler Module Bug Review - Round 1

Date: 2026-07-02
Author: AI agent 1

Scope: Review of the classes in package `io.velo.task` (`TaskRunnable`, `PrimaryTaskRunnable`,
`TaskChain`, `ITask`) against the persistence-layer design, focusing on the periodic-scheduler
loop that drives all per-slot maintenance (`OneSlot.doTask`). These runnables are the only thing
that keeps the slot-worker eventloops re-arming; if one of them ever stops rescheduling itself,
every persistence maintenance task (WAL flush/merge, expiration, chunk truncation, big-string
cleanup, replication catch-up subtasks) for that worker's slots dies.

Design context (from `doc/design/02_persist_layer_design.md` and the code):

- `TaskRunnable` is scheduled on each slot-worker eventloop with a 10 ms self-rescheduling delay
  (`MultiWorkerServer.eventloopAsScheduler`, `TaskRunnable.java:89`).
- Each tick calls `oneSlot.doTask(loopCount)` for every owned slot
  (`TaskRunnable.java:84-86`), which in turn runs the slot's `TaskChain` plus direct maintenance
  (`OneSlot.doTask`, `OneSlot.java:1219-1256`).
- `PrimaryTaskRunnable` runs once per second on the primary eventloop
  (`MultiWorkerServer.java:1096`, `PrimaryTaskRunnable.java:43`).
- `TaskChain.doTask` decides per-task cadence by `loopCount % executeOnceAfterLoopCount() == 0`
  (`TaskChain.java:42`).

---

## Bug 1: Self-rescheduling scheduler loops have no exception guard — one thrown exception permanently kills a slot worker's periodic loop

**Severity:** High
**Status:** Open → Fixed

**Files:**

- `src/main/java/io/velo/task/TaskRunnable.java:72-90`
- `src/main/java/io/velo/task/PrimaryTaskRunnable.java:30-44`
- `src/main/java/io/velo/persist/OneSlot.java:1219-1256` (consumer that can throw)

**Code excerpt (`TaskRunnable.java:73-90`):**

```java
@Override
public void run() {
    if (isStopped) {
        return;
    }

    final long INTERVAL_MS = 10L;
    if (!isStartDone) {
        slotWorkerEventloop.delay(INTERVAL_MS * 100, this);
        return;
    }

    for (var oneSlot : oneSlots) {
        oneSlot.doTask(loopCount);          // line 85 — NO try-catch
    }
    loopCount++;

    slotWorkerEventloop.delay(INTERVAL_MS, this);   // line 89 — skipped if line 85 throws
}
```

**Code excerpt (`PrimaryTaskRunnable.java:31-44`):**

```java
@Override
public void run() {
    if (isStopped) {
        return;
    }

    task.accept(loopCount);                 // line 36 — NO try-catch
    loopCount++;

    if (isStopped) {
        return;
    }

    primaryEventloop.delay(1000L, this);    // line 43 — skipped if line 36 throws
}
```

**Code excerpt (`OneSlot.doTask`, `OneSlot.java:1231-1248`) — two unguarded throw sites inside the
consumer that `TaskRunnable.run()` calls with no guard:**

```java
// line 1227 — truncateChunkFile is NOT wrapped in try-catch
if (canTruncateFdIndex != -1) {
    truncateChunkFile(canTruncateFdIndex);
}

// execute once every 100ms
if (!isAsSlave() && loopCount % 10 == 0) {
    var wal = walArray[loopCount % walArray.length];   // line 1234 — can throw AIOOBE (see Bug 2)
    if (wal != null) {
        try {
            var count = wal.intervalDeleteExpiredBigStringFiles();
            ...
        } catch (Exception e) {
            log.error(...);                            // only the inner work is guarded
        }
    }
}
```

**Root cause:**

Both scheduler runnables reschedule themselves *inside* `run()` by calling
`eventloop.delay(INTERVAL, this)` as their **last** statement. There is no `try/finally` around the
task body. If the body throws any uncaught exception, control never reaches the `delay(...)` call, so
the runnable is never re-armed on the eventloop.

ActiveJ's `Eventloop.delay(Runnable)` runs the runnable inline; when it throws, the eventloop's
fatal-error handler logs it and the loop keeps ticking — but it does **not** re-enqueue a runnable
that only reschedules itself from within its own body. So a single thrown exception is fatal to that
scheduler loop for the lifetime of the process.

This is not hypothetical. `TaskRunnable.run()` calls `oneSlot.doTask(loopCount)`, and prior review
`bug_50_persist_big_string_files_review_round_1.md` already established that parts of
`OneSlot.doTask` can throw (e.g. the `walArray[...]` access on line 1234 and
`truncateChunkFile` on line 1228 are outside any try-catch). Any such throw propagates straight
through `TaskRunnable.run()` and permanently stops rescheduling.

**Impact:**

If a slot-worker's `TaskRunnable` dies, **every** periodic task for every slot owned by that worker
stops: WAL delayed-persistence flush, chunk segment merge, key/expire cleanup, chunk-file
truncation, big-string expired/overwrite file cleanup, and all replication catch-up subtasks that are
queued through `OneSlot`'s task chain. Writes may still arrive (they are driven by network workers),
but persistence maintenance silently freezes — eventually causing unbounded WAL growth, missed
expirations, and unreclaimed chunk segments. The symptom is a gradually-degrading server with no
restart, and no error after the first log line.

Because the failure is one-way (no re-arm), a single transient `RuntimeException` from any slot is
enough to brick that worker's maintenance loop.

**Trigger scenario:**

Any uncaught `RuntimeException` from `oneSlot.doTask(loopCount)` for any owned slot — for example the
`ArrayIndexOutOfBoundsException` analyzed in Bug 2, or an `IOException`-derived unchecked exception
from `truncateChunkFile`. The smallest realistic trigger is the integer overflow in Bug 2, which is
guaranteed after ~248 days of uptime.

**Suggested fix direction:**

Wrap the task body in `try { ... } catch (Exception e) { log.error(...) }` and perform the
`eventloop.delay(...)` reschedule in a `finally` block (or unconditionally after the try-catch), so a
thrown exception can never prevent the next tick. For example in `TaskRunnable.run()`:

```java
try {
    for (var oneSlot : oneSlots) {
        oneSlot.doTask(loopCount);
    }
} catch (Exception e) {
    log.error("Slot worker task loop error, slot worker id={}", slotWorkerId, e);
} finally {
    loopCount++;
    if (!isStopped) {
        slotWorkerEventloop.delay(INTERVAL_MS, this);
    }
}
```

The same `try/finally` reschedule guarantee should be added to `PrimaryTaskRunnable.run()`. This is a
defensive correctness fix independent of Bug 2.

---

## Bug 2: `TaskRunnable.loopCount` is an `int` that overflows to negative after ~248 days, breaking modulo-based array indexing and task cadence

**Severity:** Medium
**Status:** Confirmed → Fixed

**Files:**

- `src/main/java/io/velo/task/TaskRunnable.java:66` (`private int loopCount = 0;`)
- `src/main/java/io/velo/task/PrimaryTaskRunnable.java:27` (`private int loopCount = 0;`)
- `src/main/java/io/velo/persist/OneSlot.java:1219` (`doTask(int loopCount)`) and `OneSlot.java:1233-1234`
- `src/main/java/io/velo/task/TaskChain.java:40-42`

**Code excerpt (`TaskRunnable.java:66` + `84-89`):**

```java
private int loopCount = 0;
...
for (var oneSlot : oneSlots) {
    oneSlot.doTask(loopCount);
}
loopCount++;
```

**Code excerpt (`OneSlot.doTask`, `OneSlot.java:1233-1234`):**

```java
if (!isAsSlave() && loopCount % 10 == 0) {
    var wal = walArray[loopCount % walArray.length];   // negative index when loopCount < 0
```

**Code excerpt (`TaskChain.java:42`):**

```java
if (loopCount % t.executeOnceAfterLoopCount() == 0) {
```

**Root cause:**

`TaskRunnable.loopCount` is a 32-bit `int`, incremented once per 10 ms tick
(`TaskRunnable.java:89`, `INTERVAL_MS = 10L`). `Integer.MAX_VALUE` is `2_147_483_647`; at 100
ticks/second an `int` overflows after:

```
2_147_483_648 ticks / 100 ticks-per-second = 21_474_836 s ≈ 248.5 days
```

On overflow the value wraps to `Integer.MIN_VALUE` (-2_147_483_648), i.e. **negative**. This value is
passed verbatim into `OneSlot.doTask(int loopCount)` and `TaskChain.doTask(int loopCount)`, both of
which use it as a modulo operand.

Java's `%` operator preserves the sign of the dividend, so a negative `loopCount` yields a negative
remainder. That is harmless for the "every N ticks" equality test at `TaskChain.java:42` and
`OneSlot.java:1233` (negative multiples of N still give 0), but it is **fatal** for any use of the
remainder as an array index:

- `OneSlot.java:1234`: `walArray[loopCount % walArray.length]`. With `loopCount = -2147483640` (the
  first negative multiple of 10 after overflow) and `walArray.length` = e.g. 16, Java computes
  `-2147483640 % 16 = -8`, and `walArray[-8]` throws `ArrayIndexOutOfBoundsException`.

That exception is thrown *before* the inner `try { wal.intervalDeleteExpiredBigStringFiles(); }`
guard (`OneSlot.java:1237-1246`) — the array access itself is unguarded — so it propagates out of
`OneSlot.doTask`, out of `TaskRunnable.run()` (which has no guard — see Bug 1), and permanently kills
the slot-worker loop.

**Impact:**

Two independent consequences, both starting at ~248 days of continuous uptime (entirely normal for a
storage server):

1. **Worker-loop death.** The guaranteed `ArrayIndexOutOfBoundsException` from line 1234 — combined
   with Bug 1's missing reschedule guard — stops all periodic slot maintenance for the affected
   worker (see Bug 1 impact). This happens with certainty once `loopCount` first crosses into a
   negative multiple of 10, i.e. within one tick of the 248-day overflow.
2. **Wrong WAL-group selection even if guarded.** Independent of Bug 1, `loopCount %
   walArray.length` selects a *wrong* (negative, or after a fix, a wrapped) WAL group for the
   100 ms big-string cleanup sweep. The intended round-robin over `0..walArray.length-1` is
   permanently broken in the negative region, so some WAL groups stop getting their expired
   big-string files reaped.

`PrimaryTaskRunnable.loopCount` (`PrimaryTaskRunnable.java:27`) is also an `int`, but at 1 tick /
second it overflows only after ~68 years, so it is not a practical concern and is noted only for
consistency — it should still be widened to `long` for uniformity.

**Trigger scenario:**

A Velo process with ≥ 248 days of uptime. No special workload required; the overflow is purely a
function of elapsed scheduler ticks.

**Suggested fix direction:**

1. Widen the scheduling counters to `long`: `TaskRunnable.loopCount`, `PrimaryTaskRunnable.loopCount`,
   and `OneSlot.doTask(long loopCount)` / `TaskChain.doTask(long loopCount)` / `ITask` already takes
   `long` via `setLoopCount(long)`, so the int/long mismatch can be removed cleanly.
2. Use `Math.floorMod(loopCount, walArray.length)` instead of `loopCount % walArray.length` at
   `OneSlot.java:1234` (and any other place a loopCount remainder indexes an array) so the result is
   always non-negative even if a negative value ever reaches it.
3. Bug 1's try/finally fix ensures that even a residual negative index cannot kill the loop.

---

## Bug 3: `TaskChain.doTask` does not guard against `executeOnceAfterLoopCount() == 0` (division by zero)

**Severity:** Low
**Status:** Confirmed (latent) → Fixed

**Files:**

- `src/main/java/io/velo/task/TaskChain.java:40-52`
- `src/main/java/io/velo/task/ITask.java:20-24`

**Code excerpt (`TaskChain.java:40-52`):**

```java
public void doTask(int loopCount) {
    for (var t : list) {
        if (loopCount % t.executeOnceAfterLoopCount() == 0) {   // line 42 — modulo by zero if returns 0
            t.setLoopCount(loopCount);
            try {
                t.run();
            } catch (Exception e) {
                log.error("Task error, name={}", t.name(), e);
            }
        }
    }
}
```

**Root cause:**

The cadence check `loopCount % t.executeOnceAfterLoopCount()` on line 42 is evaluated **before** the
per-task `try { t.run(); } catch` guard (which only protects `t.run()`, line 45-49). If any task's
`executeOnceAfterLoopCount()` returns `0`, the `%` operator throws `ArithmeticException: / by zero`,
which is not caught here. It propagates up to `OneSlot.doTask` → `TaskRunnable.run()` (no guard —
Bug 1) and kills the worker loop.

`ITask.executeOnceAfterLoopCount()` defaults to `1` (`ITask.java:23`) and all current implementations
return `1`, `10`, `100`, or `1000` (`KeyLoader.java:1106`, `MetaChunkSegmentFlagSeq.java:320`,
`OneSlot.java:1456`, `OneSlot.java:1528`). So no task currently returns 0, and the bug is latent.
However the interface contract does not forbid 0, and "0 meaning run as often as possible" is a
plausible (if incorrect) value a future contributor might supply; the resulting failure would be
silent loop death rather than a clear error at task-registration time.

**Impact:**

Latent. If a future `ITask` implementation returns `0` from `executeOnceAfterLoopCount()`, every tick
throws `ArithmeticException` and — via Bug 1's missing guard — permanently stops the slot-worker
loop from the moment that task is added to the chain.

**Suggested fix direction:**

Defensively normalize a non-positive divisor to `1` at the check site:

```java
var every = t.executeOnceAfterLoopCount();
if (every <= 0) every = 1;
if (loopCount % every == 0) { ... }
```

Or document/validate `executeOnceAfterLoopCount()` >= 1 in `ITask` and assert it in
`TaskChain.add(...)`, so an invalid value is rejected at registration rather than crashing the
scheduler at runtime.

---

## Summary

| # | Severity | Status | Area | Root cause |
|---|----------|--------|------|------------|
| 1 | High | Confirmed | Scheduler reschedule guarantee | `TaskRunnable.run()` / `PrimaryTaskRunnable.run()` reschedule as the last statement with no try/finally; any thrown exception skips `eventloop.delay(this)` and permanently kills the worker's periodic loop |
| 2 | Medium | Confirmed | loopCount type | `loopCount` is `int`; overflows to negative after ~248 days of 10 ms ticks; `walArray[loopCount % len]` then indexes negatively → AIOOBE (guaranteed trigger for Bug 1) |
| 3 | Low | Confirmed (latent) | TaskChain cadence check | `loopCount % executeOnceAfterLoopCount()` is unguarded; a task returning 0 throws `ArithmeticException`, killing the loop via Bug 1 |

Bugs 1 and 2 are coupled in practice: Bug 2 is the guaranteed real-world trigger, Bug 1 is what turns
that single exception into a permanent worker-loop death. Bug 1 should be fixed first because it also
converts *any* transient exception from a slot's maintenance path into a fatal one-way failure.

---

## AI Agent 2 Review Notes

Date: 2026-07-02
Reviewer: AI agent 2

### Bug 1 Review

**Status:** Confirmed

Verified against the current code:

- `TaskRunnable.run()` still calls `oneSlot.doTask(loopCount)` in the slot loop and only calls `slotWorkerEventloop.delay(INTERVAL_MS, this)` after the loop (`src/main/java/io/velo/task/TaskRunnable.java:73-89`). There is no `try`/`catch`/`finally` around the slot task body, so any unchecked exception prevents re-arming.
- `PrimaryTaskRunnable.run()` still calls `task.accept(loopCount)`, increments, and only then calls `primaryEventloop.delay(1000L, this)` (`src/main/java/io/velo/task/PrimaryTaskRunnable.java:31-43`). There is no outer guard. The current primary callback catches the `doReplAfterLeaderSelect(...)` branch, but `refreshLoader.refresh()` and future callback code remain able to abort the reschedule.
- `OneSlot.doTask(...)` has unguarded work before the guarded big-string cleanup block: `taskChain.doTask(loopCount)`, `truncateChunkFile(canTruncateFdIndex)`, and the `walArray[...]` lookup all occur before or outside the local cleanup catch blocks (`src/main/java/io/velo/persist/OneSlot.java:1219-1254`).

The impact is accurate for the slot-worker scheduler: once an exception escapes `run()`, this self-rescheduling runnable has no later code path that re-enqueues it.

### Bug 2 Review

**Status:** Confirmed

Verified against the current code:

- `TaskRunnable.loopCount` is still an `int` (`src/main/java/io/velo/task/TaskRunnable.java:66`) and is incremented every scheduled 10 ms tick (`src/main/java/io/velo/task/TaskRunnable.java:87-89`).
- `OneSlot.doTask(int loopCount)` receives that `int` and uses it directly in `walArray[loopCount % walArray.length]` when `loopCount % 10 == 0` (`src/main/java/io/velo/persist/OneSlot.java:1219-1234`).
- Java `%` can produce a negative remainder for a negative dividend, so after `int` overflow the array index can become negative. With the example length of 16, the first fatal negative multiple of 10 after overflow is `-2147483640`, yielding index `-8`.

Refinement: the first fatal cleanup tick is not necessarily "within one tick" of overflow. Starting from `Integer.MIN_VALUE` (`-2147483648`), it occurs 8 scheduler ticks later when the count reaches `-2147483640`; at 10 ms per tick that is about 80 ms. This does not change the finding or severity.

`PrimaryTaskRunnable.loopCount` is also an `int` (`src/main/java/io/velo/task/PrimaryTaskRunnable.java:27`), but the document correctly treats that as a consistency issue rather than a practical near-term trigger because it advances once per second.

### Bug 3 Review

**Status:** Confirmed (latent)

Verified against the current code:

- `TaskChain.doTask(int loopCount)` evaluates `loopCount % t.executeOnceAfterLoopCount()` before entering the `try` block around `t.run()` (`src/main/java/io/velo/task/TaskChain.java:40-49`). If a task returns `0`, the resulting `ArithmeticException` is not caught by `TaskChain`.
- `ITask.executeOnceAfterLoopCount()` has a default of `1`, but the interface contract does not document or enforce a positive value (`src/main/java/io/velo/task/ITask.java:20-24`).
- Current in-tree implementations found during review return positive values (`1`, `10`, `100`, or `1000`), so this is latent rather than currently triggered by existing production tasks.

The suggested fix direction is valid: either normalize/reject non-positive cadences before the modulo operation, or make the interface contract explicit and validate it during task registration.

---

## Bug 1 Fix Summary

**Approach:** Add a per-tick exception guard around the work body in each self-rescheduling
scheduler loop, so a thrown exception can never skip the trailing `eventloop.delay(this)`. The fix
follows the original doc's suggested direction (try-catch around the body + unconditional reschedule).

`TaskRunnable.run()` (`src/main/java/io/velo/task/TaskRunnable.java:88-97`): the per-slot
`oneSlot.doTask(loopCount)` call is wrapped in `try { ... } catch (Exception e) { log.error(...) }`.
On exception the slot is skipped but the loop keeps going; `loopCount++` and
`slotWorkerEventloop.delay(INTERVAL_MS, this)` always run.

`PrimaryTaskRunnable.run()` (`src/main/java/io/velo/task/PrimaryTaskRunnable.java:40-51`): the
`task.accept(loopCount)` callback is wrapped in the same `try/catch`; the existing `isStopped` re-check
and `primaryEventloop.delay(1000L, this)` always run.

Both classes gained an SLF4J logger for the caught errors.

**TDD evidence:**

- New test `TaskRunnableTest.'test scheduler keeps running after a slot doTask throws'`: a slot whose
  `doTask` throws on the first tick. Pre-fix this failed with `RuntimeException` escaping `run()`
  (so the loop never rescheduled). Post-fix the loop runs ~150 eventloop iterations before stop.
- New test `PrimaryTaskRunnableTest.'test scheduler keeps running after the task callback throws'`:
  a callback that throws on the first tick. Same pre-fix failure / post-fix pass; the 1 s interval
  reschedules and the callback runs again.
- Both pre-fix runs failed for the expected reason (`RuntimeException` at the direct `run()` call).
- `./gradlew :test --tests "io.velo.task.*"` — all task tests pass.

**JaCoCo confirmation:**

- `io.velo.task.TaskRunnable` lines 88-97: the `for` loop (both branches), the `try`,
  `oneSlot.doTask`, the `catch (Exception e)` branch, the `log.error`, `loopCount++`, and the
  `slotWorkerEventloop.delay(...)` reschedule are all covered. The catch branch executing proves the
  thrown exception was caught rather than escaping.
- `io.velo.task.PrimaryTaskRunnable` lines 40-51: `task.accept`, the `catch (Exception e)` branch,
  `log.error`, `loopCount++`, and `primaryEventloop.delay(...)` reschedule are all covered.
- Remaining uncovered lines (`TaskRunnable` 84-85 `!isStartDone` startup-delay path;
  `PrimaryTaskRunnable` 48 mid-task `isStopped` re-check return) are pre-existing branches not
  touched by this fix.


---

## Review Feedback - Bug 1 Fix

Date: 2026-07-02
Reviewer: AI agent 2
Commit: `82c6e3ce` (`fix: guard scheduler loops so exceptions cannot stop rescheduling`)

### Summary

Reviewed the Bug 1 fix in `TaskRunnable` and `PrimaryTaskRunnable`. The commit adds scheduler-level exception guards around the slot/callback work and preserves the self-rescheduling path after caught exceptions.

### Findings

No blocking issues found.

### Strengths

- `TaskRunnable.run()` now catches exceptions per slot (`src/main/java/io/velo/task/TaskRunnable.java:88-93`), so one failing slot no longer prevents later slots in the same worker tick from running.
- The slot-worker loop still increments `loopCount` and re-arms with `slotWorkerEventloop.delay(INTERVAL_MS, this)` after a caught exception (`src/main/java/io/velo/task/TaskRunnable.java:95-97`). This directly addresses the one-way loop-death failure.
- `PrimaryTaskRunnable.run()` now catches callback exceptions before incrementing and rescheduling (`src/main/java/io/velo/task/PrimaryTaskRunnable.java:40-51`), covering the primary scheduler variant of the same pattern.
- Regression tests exercise the actual pre-fix failure mode: a task throws on the first tick, then the event loop must run a later tick.

### Verification

- Ran `./gradlew :test --tests "io.velo.task.*" --rerun-tasks`: `BUILD SUCCESSFUL` in 32s, 13 tasks executed.
- JaCoCo check for `io.velo.task.TaskRunnable` lines 86-97: 8/8 relevant executable lines covered; catch branch and reschedule line covered.
- JaCoCo check for `io.velo.task.PrimaryTaskRunnable` lines 37-51: catch branch, increment, and reschedule line covered. The only uncovered changed-region branch is the pre-existing mid-run `isStopped` return at line 48, which is unrelated to the Bug 1 exception-reschedule path.

### Concerns

None for Bug 1. Bug 2 and Bug 3 remain separate confirmed findings and are not fixed by this commit.

---

## Bug 2 Fix Summary

**Approach:** Two coordinated changes following the original doc's suggested direction (widen the
counter to `long` + make the array index non-negative):

1. **Widen the slot-worker counter to `long`.** `TaskRunnable.loopCount` (`int` → `long`), and the
   types that carry it downstream: `OneSlot.doTask(int)` → `OneSlot.doTask(long)` and
   `TaskChain.doTask(int)` → `TaskChain.doTask(long)`. `TaskChain.doTask` had to become `long`
   because `OneSlot.doTask` now passes a `long` to it (an `int` param would narrow and reintroduce
   the overflow). All existing call sites pass `int` literals which widen to `long`, so nothing else
   changes. `PrimaryTaskRunnable.loopCount` is intentionally left as `int`: its callback is
   `Consumer<Integer>` (widening would require a cast that truncates, or an API break to
   `Consumer<Long>`), and at 1 tick/second an `int` overflows only after ~68 years — not a practical
   trigger.

2. **Non-negative array index.** `OneSlot.java:1234` changed from `walArray[loopCount %
   walArray.length]` to `walArray[Math.floorMod(loopCount, walArray.length)]`. Java's `%` keeps the
   dividend's sign, so a negative `loopCount` produced a negative index; `Math.floorMod` always
   returns `[0, length)`. This is defense-in-depth so that even if a negative value ever reaches the
   line, it indexes validly and the round-robin WAL-group sweep stays correct.

**TDD evidence:**

- New test `OneSlotTest.'test do task wal array index stays valid for negative loopCount'`: builds a
  real slot (default c1m → `walGroupNumber=2048`, i.e. `walArray.length > 1`) and calls
  `oneSlot.doTask(-10)`, `oneSlot.doTask(-2147483640)`, `oneSlot.doTask(-20000)` (negative
  multiples of 10, the shape `loopCount` takes after int overflow).
- Pre-fix this failed with `ArrayIndexOutOfBoundsException` at the `doTask(-10)` line — exactly the
  confirmed Bug 2 symptom. Post-fix it passes (`noExceptionThrown()`).
- Updated the Bug 1 test's anonymous `OneSlot` override from `doTask(int)` to `doTask(long)` to
  match the new signature.
- `./gradlew :test --tests "io.velo.task.*" --tests "io.velo.persist.OneSlotTest" --tests
  "io.velo.command.XGroupTest"` — all pass (no regressions from the `int` → `long` widening).

**JaCoCo confirmation:**

- `io.velo.persist.OneSlot` line 1233 `if (!isAsSlave() && loopCount % 10 == 0)` — all 4 branches
  covered (the test exercises master + negative-multiple-of-10).
- `io.velo.persist.OneSlot` line 1234 `walArray[Math.floorMod(loopCount, walArray.length)]` — covered.
- `io.velo.task.TaskChain` `doTask(long)` line 42 `loopCount % executeOnceAfterLoopCount() == 0` —
  all branches covered.
- The `long` widening of `TaskRunnable.loopCount` is verified by compilation and the passing
  end-to-end task tests (it is private and only consumed by `OneSlot.doTask(long)`).


---

## Review Feedback - Bug 2 Fix

Date: 2026-07-02
Reviewer: AI agent 2
Commit: `4eab15cc` (`fix: widen loopCount to long and use floorMod for wal index`)

### Summary

Reviewed the Bug 2 fix in `TaskRunnable`, `OneSlot`, and `TaskChain`. The commit addresses the practical 248-day slot-worker overflow by widening the slot-worker loop counter path to `long` and making the WAL-group array index non-negative with `Math.floorMod`.

### Findings

No blocking issues found.

### Strengths

- `TaskRunnable.loopCount` is now `long` (`src/main/java/io/velo/task/TaskRunnable.java:70`), so the 10 ms slot-worker counter no longer overflows after about 248 days.
- `OneSlot.doTask` and `TaskChain.doTask` now accept `long` loop counts (`src/main/java/io/velo/persist/OneSlot.java:1219`, `src/main/java/io/velo/task/TaskChain.java:40`), avoiding a narrowing conversion that would reintroduce the overflow downstream.
- `OneSlot` now uses `Math.floorMod(loopCount, walArray.length)` for the WAL-group index (`src/main/java/io/velo/persist/OneSlot.java:1234`), so even explicitly negative loop counts stay within `[0, walArray.length)`.
- The regression test drives real negative multiples of 10, including `-2147483640`, through `OneSlot.doTask(...)`, which is the shape of the original failure.

### Verification

- Ran `./gradlew :test --tests "io.velo.task.*" --tests "io.velo.persist.OneSlotTest" --tests "io.velo.command.XGroupTest" --rerun-tasks`: `BUILD SUCCESSFUL` in 1m 20s, 13 tasks executed.
- JaCoCo check for `io.velo.persist.OneSlot` lines 1219-1234: `taskChain.doTask(loopCount)`, the `loopCount % 10 == 0` branch, and `walArray[Math.floorMod(loopCount, walArray.length)]` are covered. The unchanged stopping/truncate branches remain partially or not covered in this line range.
- JaCoCo check for `io.velo.task.TaskChain` lines 40-43: 3/3 executable lines covered, with both loop and cadence branches fully covered.
- JaCoCo check for `io.velo.task.TaskRunnable` lines 69-97: the widened `long loopCount` declaration, normal execution, exception catch, increment, and reschedule lines are covered. The pre-existing startup-delay branch at lines 83-85 remains uncovered in this focused run.

### Concerns

None for Bug 2. `PrimaryTaskRunnable.loopCount` remains `int`, but this does not leave the confirmed 248-day slot-worker failure open because the primary scheduler ticks once per second and does not feed `OneSlot.doTask`. Bug 3 (`executeOnceAfterLoopCount() == 0`) remains a separate confirmed finding and is not fixed by this commit.

---

## Bug 3 Fix Summary

**Approach:** Follow the original doc's first suggested direction — defensively normalize a
non-positive cadence to `1` at the modulo check site, so a task returning `0` (or negative) from
`executeOnceAfterLoopCount()` can no longer throw `ArithmeticException`.

`TaskChain.doTask(long loopCount)` (`src/main/java/io/velo/task/TaskChain.java:40-56`):

```java
var every = t.executeOnceAfterLoopCount();
if (every <= 0) {
    every = 1;
}
if (loopCount % every == 0) {
    ...
}
```

A cadence of `0`/negative is treated as "run every tick" (divisor normalized to `1`), which is the
safest interpretation and prevents the unguarded modulo from ever dividing by zero. The per-task
`try/catch` around `t.run()` is unchanged.

**TDD evidence:**

- New test `TaskChainTest.'test do task tolerates executeOnceAfterLoopCount returning zero'`: a
  `TaskZeroCadence` task whose `executeOnceAfterLoopCount()` returns `0`. It calls `doTask(0)`,
  `doTask(1)`, `doTask(2)` and asserts `noExceptionThrown()` plus `runCount == 3`.
- Pre-fix this failed with `ArithmeticException: / by zero` at the first `doTask(0)` (the modulo is
  evaluated before the per-task try-catch). Post-fix it passes.
- `./gradlew :test --tests "io.velo.task.*"` — all task tests pass; existing tasks (cadence 1/2)
  are unaffected by the normalization.

**JaCoCo confirmation:**

- `io.velo.task.TaskChain` line 43 `if (every <= 0)` — all 2 branches covered (the `<= 0` true
  branch by `TaskZeroCadence`, the `> 0` false branch by existing cadence 1/2 tasks).
- Line 44 `every = 1` — covered (the normalization executes).
- Line 46 `if (loopCount % every == 0)` — all branches covered.

---

## Round 1 Status

| # | Severity | Status | Root cause |
|---|----------|--------|------------|
| 1 | High | Fixed (`82c6e3ce`) | Scheduler loops reschedule as the last statement with no try/catch; an exception kills the worker loop |
| 2 | Medium | Fixed (`4eab15cc`) | `loopCount` was `int` (overflows after ~248 days); `walArray[loopCount % len]` then indexed negatively → AIOOBE |
| 3 | Low | Fixed (this commit) | `TaskChain.doTask` modulo used `executeOnceAfterLoopCount()` directly; a `0` return threw `ArithmeticException` |

All three confirmed findings are now fixed and covered by tests + JaCoCo.


---

## Review Feedback - Bug 3 Fix

Date: 2026-07-02
Reviewer: AI agent 2
Commit: `897206fb` (`fix: guard TaskChain cadence against zero to avoid division by zero`)

### Summary

Reviewed the Bug 3 fix in `TaskChain`. The commit normalizes non-positive task cadences to `1` before the modulo check, so `executeOnceAfterLoopCount()` returning `0` no longer throws `ArithmeticException` before the task-level `try/catch`.

### Findings

No blocking issues found.

### Strengths

- `TaskChain.doTask(long loopCount)` now stores `executeOnceAfterLoopCount()` in a local `every` value and normalizes `every <= 0` to `1` before performing `loopCount % every` (`src/main/java/io/velo/task/TaskChain.java:42-46`). This directly removes the division-by-zero failure path.
- The fix also handles negative cadences defensively, matching the same non-positive-input class rather than only special-casing zero.
- The regression test adds a concrete `ITask` implementation returning `0` and verifies multiple ticks run without exception (`src/test/groovy/io/velo/task/TaskChainTest.groovy:60-95`).
- Existing positive-cadence behavior remains covered by the existing `TaskChainTest` cases.

### Verification

- Ran `./gradlew :test --tests "io.velo.task.*" --rerun-tasks`: `BUILD SUCCESSFUL` in 31s, 13 tasks executed.
- JaCoCo check for `io.velo.task.TaskChain` lines 40-56: 12/12 executable lines covered; `every <= 0` has both branches covered, `every = 1` is covered, and the cadence modulo branch is fully covered.

### Concerns

None for Bug 3. This commit fixes the latent division-by-zero path. All three findings in this review doc now have corresponding fix commits and post-commit review feedback.
