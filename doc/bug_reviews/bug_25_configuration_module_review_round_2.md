# Bug 25 - Configuration Module Review (Round 2)

Reviewer: AI agent 1
Review date: 2026-05-08
Branch: `main`
Scope: Configuration module round 2 - runtime `CONFIG` handling, `SocketInspector` max-connections wiring, slot bucket config parsing, and replication config parsing.

## Files Reviewed

- `doc/design/08_configuration_design.md`
- `src/main/resources/velo.properties`
- `src/main/java/io/velo/MultiWorkerServer.java`
- `src/main/java/io/velo/ConfForSlot.java`
- `src/main/java/io/velo/SocketInspector.java`
- `src/main/java/io/velo/repl/Binlog.java`
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`
- `src/main/java/io/velo/persist/MetaKeyBucketSplitNumber.java`
- `dyn/src/io/velo/command/ConfigCommand.groovy`
- `dyn/test/io/velo/command/ConfigCommandTest.groovy`
- `src/test/groovy/io/velo/ConfForSlotTest.groovy`
- `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`

## Finding 1: `CONFIG SET max_connections` and `net.maxConnections` accept zero or negative values, locking out clients

**Severity:** High

**Files:**

- `dyn/src/io/velo/command/ConfigCommand.groovy:62-75`
- `src/main/java/io/velo/MultiWorkerServer.java:1531-1537`
- `src/main/java/io/velo/SocketInspector.java:576-607`
- `src/main/java/io/velo/persist/DynConfig.java:82-84`

**Code excerpt:**

```groovy
if ("max_connections" == configKey) {
    int maxConnections
    try {
        maxConnections = Integer.parseInt(configValue)
    } catch (NumberFormatException ignored) {
        return ErrorReply.INVALID_INTEGER
    }

    MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(maxConnections)
    ...
    firstOneSlot.dynConfig.update(configKey, maxConnections)
    return OKReply.INSTANCE
}
```

```java
public synchronized void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
}

if (socketMap.size() >= maxConnections) {
    log.warn("Inspector max connections reached={}, close the socket", maxConnections);
    socket.close();
    return;
}
```

**Root cause:**

Both startup config (`net.maxConnections`) and runtime config (`CONFIG SET max_connections`) assign the parsed integer directly to `SocketInspector` without validating that it is positive. The connect path uses `socketMap.size() >= maxConnections`; therefore, with `maxConnections <= 0`, the condition is true before any ordinary client is admitted.

The runtime path also persists the invalid value through `DynConfig`. On restart, `DynConfig.AfterUpdateCallbackInner` replays `max_connections` and calls `setMaxConnections((int) value)` again, preserving the lockout.

**Impact:**

- `CONFIG SET max_connections 0` or `CONFIG SET max_connections -1` returns `OK` and prevents new ordinary client connections.
- `net.maxConnections=0` or a negative value in the properties file starts the server in the same unusable state.
- Because runtime changes are persisted, a bad value survives restart unless the dyn-config file is manually edited.

**Suggested fix:**

Validate the value before applying or persisting it:

```groovy
if (maxConnections <= 0) {
    return ErrorReply.INVALID_INTEGER
}
```

Also validate `net.maxConnections` in `MultiWorkerServer.InnerModule.socketInspector()` and reject invalid persisted dyn-config values before replaying them into `SocketInspector`.

---

## Finding 2: Unsupported `CONFIG SET` keys return `OK` without changing anything

**Severity:** Medium

**Files:**

- `dyn/src/io/velo/command/ConfigCommand.groovy:81-85`
- `dyn/test/io/velo/command/ConfigCommandTest.groovy:116-122`

**Code excerpt:**

```groovy
if ("max_connections" == configKey) {
    ...
    return OKReply.INSTANCE
} else {
    // todo
}

OKReply.INSTANCE
```

The current test suite codifies this behavior:

```groovy
data4[2] = 'key'.bytes
data4[3] = 'value'.bytes
...
reply == OKReply.INSTANCE
```

**Root cause:**

The unsupported-key branch falls through to `OKReply.INSTANCE`. There is no error response and no persistence/update path for the requested key.

**Impact:**

- A client or automation can receive `OK` for `CONFIG SET timeout 10`, `CONFIG SET appendonly yes`, or any typo, even though Velo ignored it.
- Operators may believe a safety or performance setting has been applied when the server state is unchanged.
- This is a Redis-compatibility hazard for the partially implemented `CONFIG` command.

**Suggested fix:**

Return an error for unknown or unsupported keys, and update `ConfigCommandTest` so unsupported keys are expected to fail:

```groovy
return ErrorReply.SYNTAX
```

or a specific `ERR Unsupported CONFIG parameter` reply if the project prefers more explicit command errors.

---

## Finding 3: Replication short-sized config values truncate before validation

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1380-1381`
- `src/main/java/io/velo/ConfForSlot.java:542-547`
- `src/main/java/io/velo/ConfForSlot.java:570-575`
- `src/main/java/io/velo/repl/Binlog.java:159`
- `src/main/java/io/velo/repl/Binlog.java:578-580`

**Code excerpt:**

```java
c.confRepl.binlogForReadCacheSegmentMaxCount =
        config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount", 10).shortValue();
c.confRepl.binlogFileKeepMaxCount =
        config.get(ofInteger(), "repl.binlogFileKeepMaxCount", 10).shortValue();
c.confRepl.checkIfValid();
```

```java
if (binlogForReadCacheSegmentMaxCount <= 0) {
    throw new IllegalArgumentException("Repl binlog for read cache segment max count must be > 0, given: " + binlogForReadCacheSegmentMaxCount);
}
if (binlogFileKeepMaxCount <= 0) {
    throw new IllegalArgumentException("Repl binlog file keep max count must be > 0, given: " + binlogFileKeepMaxCount);
}
```

**Root cause:**

The parser converts an `Integer` to `short` before validation. That conversion wraps modulo 65536. Some invalid values are caught after wrapping to zero or negative, but values such as `65537` wrap to `1` and pass the existing `> 0` checks.

Examples:

- `repl.binlogForReadCacheSegmentMaxCount=65537` -> `(short) 1` -> accepted
- `repl.binlogFileKeepMaxCount=65538` -> `(short) 2` -> accepted

**Impact:**

- A typo or oversized config can silently reduce the binlog read cache to one segment.
- Oversized `repl.binlogFileKeepMaxCount` values can silently reduce retention to one or two files, causing old binlog files to be deleted far earlier than configured.
- The log and runtime config show the wrapped value, not the invalid original value, making the startup mistake hard to diagnose.

**Suggested fix:**

Validate in `int` form before narrowing:

```java
int cacheMaxCount = config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount", 10);
if (cacheMaxCount <= 0 || cacheMaxCount > Short.MAX_VALUE) {
    throw new IllegalArgumentException("repl.binlogForReadCacheSegmentMaxCount must be between 1 and " + Short.MAX_VALUE);
}
c.confRepl.binlogForReadCacheSegmentMaxCount = (short) cacheMaxCount;
```

Apply the same pattern to `repl.binlogFileKeepMaxCount`.

---

## Finding 4: `bucket.initialSplitNumber` truncates before validation, accepting invalid values as valid split counts

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1330-1331`
- `src/main/java/io/velo/ConfForSlot.java:273`
- `src/main/java/io/velo/ConfForSlot.java:296-298`
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:54-57`
- `src/main/java/io/velo/persist/MetaKeyBucketSplitNumber.java:42-49`

**Code excerpt:**

```java
if (config.getChild("bucket.initialSplitNumber").hasValue()) {
    c.confBucket.initialSplitNumber = config.get(ofInteger(), "bucket.initialSplitNumber").byteValue();
}
...
if (initialSplitNumber != 1 && initialSplitNumber != 3) {
    throw new IllegalArgumentException("Initial split number too large, initial split number should be 1 or 3");
}
```

**Root cause:**

`Integer.byteValue()` wraps before `ConfBucket.checkIfValid()` sees the value. The validator only allows `1` or `3`, but invalid inputs congruent to those values modulo 256 pass:

- `bucket.initialSplitNumber=257` -> `(byte) 1` -> accepted
- `bucket.initialSplitNumber=259` -> `(byte) 3` -> accepted

**Impact:**

- Invalid startup config is silently accepted as a different bucket layout.
- The accepted wrapped value controls new key bucket split metadata through `KeyBucketsInOneWalGroup` and `MetaKeyBucketSplitNumber`.
- This can hide configuration mistakes in deployments where bucket layout is intended to be explicit and stable.

**Suggested fix:**

Validate as an `int` before narrowing:

```java
int initialSplitNumber = config.get(ofInteger(), "bucket.initialSplitNumber");
if (initialSplitNumber != 1 && initialSplitNumber != 3 && initialSplitNumber != 9) {
    throw new IllegalArgumentException("bucket.initialSplitNumber must be 1, 3, or 9, given: " + initialSplitNumber);
}
c.confBucket.initialSplitNumber = (byte) initialSplitNumber;
```

**Decision:** The valid values for `bucket.initialSplitNumber` are `1`, `3`, or `9`. Value `9` is added to support a wider bucket split layout. Both the startup config path (`MultiWorkerServer.confForSlot()`) and the runtime validation (`ConfForSlot.ConfBucket.checkIfValid()`) enforce this exact set.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - max connections accepts non-positive values and can persist client lockout | High | New | High |
| 2 - Unsupported `CONFIG SET` keys return `OK` without effect | Medium | New | High |
| 3 - Replication short configs truncate before validation | Medium | New | High |
| 4 - `bucket.initialSplitNumber` byte truncation accepts invalid values | Medium | New | High |

## Suggested Fix Order

1. Fix Finding 1 first because it can make the server unreachable and persists through dyn-config replay.
2. Fix Finding 2 next because it is localized to `ConfigCommand` and prevents silent operator misconfiguration.
3. Fix Finding 3 with focused `MultiWorkerServer.InnerModule.confForSlot()` regression tests for overflow values.
4. Fix Finding 4 with a focused `bucket.initialSplitNumber=257` startup-config regression test.

---

## Reviewer Notes (AI agent 2)

Reviewer: AI agent 2
Review date: 2026-05-08
Branch: `main`

### Finding 1 — CONFIRMED

**Verdict: Confirmed, High severity.**

Verified against source:

- `ConfigCommand.groovy:62-80` — `Integer.parseInt(configValue)` result is passed directly to `setMaxConnections()` with no positivity check. A value of `0` or `-1` is accepted and persisted via `dynConfig.update()`.
- `SocketInspector.java:576-578` — `setMaxConnections()` assigns the raw int with no validation.
- `SocketInspector.java:607` — `socketMap.size() >= maxConnections` evaluates to `true` immediately when `maxConnections <= 0`, rejecting all new connections.
- `MultiWorkerServer.java:1533` — Startup path reads `net.maxConnections` from config with no validation; `0` or negative values are applied directly.
- `DynConfig.java:82-84` — On restart, `AfterUpdateCallbackInner` replays the persisted `max_connections` value by calling `setMaxConnections((int) value)` without validation, so a bad value survives restart.

**Additional observations:**

- The test at `ConfigCommandTest.groovy:126-131` only tests `max_connections=100` (positive), never tests `0` or negative.
- There is also no startup test for `net.maxConnections=0`.

### Finding 2 — CONFIRMED

**Verdict: Confirmed, Medium severity.**

Verified against source:

- `ConfigCommand.groovy:81-85` — The `else { // todo }` branch for unsupported keys falls through to line 85 `OKReply.INSTANCE`, returning OK for any unrecognized config key.
- `ConfigCommandTest.groovy:108-123` — The test explicitly asserts `reply == OKReply.INSTANCE` for `configKey='key'` (unsupported), codifying the wrong behavior.
- This is a Redis-compatibility issue: real Redis returns `ERR Unknown option or number of arguments for CONFIG SET -- 'key'` for unknown keys.

### Finding 3 — CONFIRMED

**Verdict: Confirmed, Medium severity.**

Verified against source:

- `MultiWorkerServer.java:1380` — `config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount", 10).shortValue()` truncates from `int` to `short` before any validation.
- `MultiWorkerServer.java:1381` — Same pattern for `repl.binlogFileKeepMaxCount`.
- `ConfForSlot.java:542,547` — Fields are `short` type.
- `ConfForSlot.java:570-575` — `checkIfValid()` only checks `<= 0`, so truncated values that happen to be positive pass validation.
- The truncation is real: `65537` (int) -> `1` (short), `65538` (int) -> `2` (short), both accepted as valid.
- `Binlog.java:159` — `forReadCacheSegmentMaxCount` is read from the config and used directly for read cache sizing.
- `Binlog.java:580` — `binlogFileKeepMaxCount` controls how many binlog files to keep; a wrapped value of `1` or `2` would aggressively delete old binlog files.

### Finding 4 — CONFIRMED

**Verdict: Confirmed, Medium severity.**

Verified against source:

- `MultiWorkerServer.java:1330-1331` — `config.get(ofInteger(), "bucket.initialSplitNumber").byteValue()` narrows from `Integer` to `byte` before validation.
- `ConfForSlot.java:273` — Field is `byte` type.
- `ConfForSlot.java:296-298` — `checkIfValid()` only checks `!= 1 && != 3`. Since `257 % 256 == 1` and `259 % 256 == 3`, wrapped values pass.
- `KeyBucketsInOneWalGroup.java` and `MetaKeyBucketSplitNumber.java` use this value to control key bucket split metadata layout.

### Summary

All four findings are confirmed. No refutations or refinements needed. The root causes and impacts described by AI agent 1 are accurate.

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - max connections accepts non-positive values and can persist client lockout | High | **Confirmed** | High |
| 2 - Unsupported `CONFIG SET` keys return `OK` without effect | Medium | **Confirmed** | High |
| 3 - Replication short configs truncate before validation | Medium | **Confirmed** | High |
| 4 - `bucket.initialSplitNumber` byte truncation accepts invalid values | Medium | **Confirmed** | High |
