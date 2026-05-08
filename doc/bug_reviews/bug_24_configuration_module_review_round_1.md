# Bug 24 — Configuration Module Review (Round 1)

Reviewer: AI agent 1
Review date: 2026-05-08
Branch: `review/persist-read-data-flow` (post-merge to `main`)
Scope: Configuration module — `ConfForGlobal`, `ConfForSlot`, `DynConfig`, `ConfVolumeDirsForSlot`, config loading in `MultiWorkerServer.confForSlot()`

## Files Reviewed

- `src/main/java/io/velo/ConfForGlobal.java`
- `src/main/java/io/velo/ConfForSlot.java`
- `src/main/java/io/velo/ConfVolumeDirsForSlot.java`
- `src/main/java/io/velo/persist/DynConfig.java`
- `src/main/java/io/velo/MultiWorkerServer.java:1217-1463` (config loading + validation)
- `src/main/java/io/velo/persist/FdReadWrite.java:400-425` (LRU init from config)
- `src/main/java/io/velo/persist/LocalPersist.java:228-243` (initDynConfigItems application)

## Finding 1: Integer division truncates `bucketLruPerFdPercent` to zero for any value < 100

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1340`

**Code excerpt:**

```java
var bucketLruPerFdPercent = config.get(ofInteger(), "bucket.lruPerFd.percent", 100);
if (bucketLruPerFdPercent < 0 || bucketLruPerFdPercent > 100) {
    throw new IllegalArgumentException("Key bucket fd read lru percent be between 0 and 100");
}
c.confBucket.lruPerFd.maxSize = bucketLruPerFdPercent / 100 * c.confBucket.bucketsPerSlot;
```

**Root cause:**

The expression `bucketLruPerFdPercent / 100` performs integer division. For any value of `bucketLruPerFdPercent` less than 100, the result is `0`:

- `50 / 100 = 0` → `maxSize = 0`
- `80 / 100 = 0` → `maxSize = 0`
- `99 / 100 = 0` → `maxSize = 0`
- `100 / 100 = 1` → `maxSize = bucketsPerSlot` (correct)

In `FdReadWrite.initLRU()` (line 423), `maxSize > 0` gates LRU cache creation. So any `bucket.lruPerFd.percent` value from 1–99 silently disables the key-bucket LRU cache entirely.

**Impact:**

- The key-bucket read LRU cache is silently disabled for any non-100 percent value
- Users expecting 50% LRU capacity get zero cache, degrading read performance
- The default (100) works correctly, so production with defaults is unaffected
- The `chunk.lruPerFd.maxSize` config (line 1357) is set directly as an integer and does NOT have this bug

**Suggested fix:**

```java
c.confBucket.lruPerFd.maxSize = (int)((long) bucketLruPerFdPercent * c.confBucket.bucketsPerSlot / 100);
```

Or simpler:
```java
c.confBucket.lruPerFd.maxSize = bucketLruPerFdPercent * c.confBucket.bucketsPerSlot / 100;
```

This works because `bucketLruPerFdPercent <= 100` and `bucketsPerSlot` is at most 131072, so the product fits in an `int`.

---

## Finding 2: `byteValue()` truncates `onceScanMaxLoopCount` config overrides — valid values silently corrupted

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/MultiWorkerServer.java:1334` (bucket)
- `src/main/java/io/velo/MultiWorkerServer.java:1368` (WAL)

**Code excerpt:**

```java
// Line 1333-1335
if (config.getChild("bucket.onceScanMaxLoopCount").hasValue()) {
    c.confBucket.onceScanMaxLoopCount = config.get(ofInteger(), "bucket.onceScanMaxLoopCount").byteValue();
}

// Line 1367-1369
if (config.getChild("wal.onceScanMaxLoopCount").hasValue()) {
    c.confWal.onceScanMaxLoopCount = config.get(ofInteger(), "wal.onceScanMaxLoopCount").byteValue();
}
```

**Root cause:**

`onceScanMaxLoopCount` is declared as `int` (line 278, line 498) and validated to be in `[1, 1024]`. But the config loading uses `.byteValue()` which truncates to 8-bit signed (`-128..127`):

- Input `128` → byte `-128` → int `-128` → fails validation ("should be between 1 and 1024")
- Input `256` → byte `0` → int `0` → fails validation
- Input `1024` → byte `0` → int `0` → fails validation

Since the default is `1024` (stored directly, not parsed through `byteValue()`), the default works. But any override via config with a value > 127 either:
1. Fails with a confusing validation error (values 128–255 map to negative, 256+ map to 0 or negative)
2. Or silently uses a wrong value in the unlikely case the truncated byte happens to land in 1..127

**Impact:**

- Users cannot set `onceScanMaxLoopCount` to any value > 127 via config
- The error message is confusing because the value the user typed IS in the valid range
- The default (1024) is unaffected because it bypasses the `byteValue()` path

**Suggested fix:**

Change `.byteValue()` to `.intValue()`:

```java
c.confBucket.onceScanMaxLoopCount = config.get(ofInteger(), "bucket.onceScanMaxLoopCount");
c.confWal.onceScanMaxLoopCount = config.get(ofInteger(), "wal.onceScanMaxLoopCount");
```

`config.get(ofInteger(), ...)` already returns `Integer`, so `.intValue()` is implicit, and no `.byteValue()` call is needed.

---

## Non-Finding: `ConfRepl.checkIfValid()` is empty — missing validation

**Severity:** Low / Informational

`ConfRepl.checkIfValid()` at `src/main/java/io/velo/ConfForSlot.java:562-564` is an empty method. Some validation exists in `MultiWorkerServer.confForSlot()` (e.g., `binlogOneFileMaxLength > 0` at line 1376), but many other fields lack bounds checking:

- `binlogOneSegmentLength` should be > 0 and power-of-2 aligned
- `binlogForReadCacheSegmentMaxCount` should be > 0
- `binlogFileKeepMaxCount` should be > 0
- `catchUpIntervalMillis` should be > 0

This is a validation gap rather than a runtime bug — invalid values could cause confusing downstream errors (e.g., ArithmeticException, NegativeArraySizeException) instead of clear startup errors.

---

## Non-Finding: `initDynConfigItems` only applied to `firstOneSlot()` — intentional but noteworthy

`ConfForGlobal.initDynConfigItems` is only applied to slot 0's `DynConfig` via `firstOneSlot()` (`src/main/java/io/velo/persist/LocalPersist.java:237-241`). Since the `AfterUpdateCallbackInner` sets **global statics** (e.g., `RedisZSet.ZSET_MAX_SIZE`), applying once to slot 0 is sufficient. Other slots' DynConfig files are not updated with init items.

On restart, slot 0's DynConfig constructor replays persisted items from the JSON file, so the init items persist correctly. This is intentional design, not a bug.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Integer division truncates bucket LRU percent to zero | Medium | **Fixed** — multiplication before division | High |
| 2 - `byteValue()` truncates `onceScanMaxLoopCount` for values > 127 | Medium | **Fixed** — removed `.byteValue()`, use Integer directly | High |
| Non-Finding: Empty `ConfRepl.checkIfValid()` | Low | Informational | High |
| Non-Finding: `initDynConfigItems` slot 0 only | Informational | By design | High |

---

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Review date: 2026-05-08
Branch verified: `main`

### Finding 1 Review: Confirmed

The finding is valid against the current code.

- `MultiWorkerServer.confForSlot()` still computes `c.confBucket.lruPerFd.maxSize` as
  `bucketLruPerFdPercent / 100 * c.confBucket.bucketsPerSlot` at
  `src/main/java/io/velo/MultiWorkerServer.java:1340`.
- Because both operands are integers, any configured percent in `1..99` produces `0` before multiplication.
- `FdReadWrite.initLRU()` only creates `oneInnerBytesByIndexLRU` when `maxSize > 0`
  at `src/main/java/io/velo/persist/FdReadWrite.java:423-425`, so this does disable key-bucket FD LRU caching.

Small correction: the suggested fix is still safe, but the documented maximum bucket count is understated.
`KeyBucket.MAX_BUCKETS_PER_SLOT` is `16384 * 16`, i.e. `262144`, not `131072`. The product
`100 * 262144` still fits comfortably in an `int`, so the simple multiplication-first fix remains safe.

Status: **Confirmed - should fix.**

### Finding 2 Review: Confirmed

The finding is valid against the current code.

- `bucket.onceScanMaxLoopCount` is loaded with `.byteValue()` at
  `src/main/java/io/velo/MultiWorkerServer.java:1333-1335`.
- `wal.onceScanMaxLoopCount` is loaded with `.byteValue()` at
  `src/main/java/io/velo/MultiWorkerServer.java:1367-1369`.
- Both target fields are declared as `int`, default to `1024`, and validate the range `1..1024`
  in `src/main/java/io/velo/ConfForSlot.java:278-300` and
  `src/main/java/io/velo/ConfForSlot.java:498-508`.

The impact is real: configured values above `127` are truncated before validation. Some valid user inputs
will be rejected with a misleading validation error, while other valid inputs can wrap into an unrelated
accepted value, for example `257` becoming `1`.

Status: **Confirmed - should fix.**

### Non-Finding Review: Empty `ConfRepl.checkIfValid()`

Confirmed as a validation gap, not promoted to a standalone runtime bug in this round.

The current code does call `c.confRepl.checkIfValid()` from `MultiWorkerServer.confForSlot()`, but the method
body is empty. One explicit validation exists for `repl.binlogOneFileMaxLength <= 0`; the other replication
fields listed in this review are not validated there.

There are plausible downstream failure modes:

- `binlogOneSegmentLength <= 0` can break `new byte[ConfForSlot.global.confRepl.binlogOneSegmentLength]`
  in `src/main/java/io/velo/repl/Binlog.java:160`.
- `binlogOneSegmentLength == 0` can break `Binlog.oneFileMaxSegmentCount()` via division by zero at
  `src/main/java/io/velo/repl/Binlog.java:410-411`.
- `catchUpIntervalMillis <= 0` reaches delayed scheduling paths in `XGroup`, where a clear startup-time
  configuration error would be preferable.

Status: **Informational - real validation debt, but acceptable as a non-finding unless this review's scope is
expanded to harden replication config validation.**

### Non-Finding Review: `initDynConfigItems` Applies To `firstOneSlot()`

Confirmed as intentional/global behavior for the keys reviewed here.

`LocalPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()` applies `ConfForGlobal.initDynConfigItems` to
`firstOneSlot().getDynConfig()` only. `DynConfig.AfterUpdateCallbackInner` updates global static settings for
the type limit keys, and the constructor replays persisted dyn config entries on restart. For slot-local keys
such as `monitor_big_key_top_k`, applying only to the first slot is observable behavior, but the current finding
correctly treats the reviewed global init path as by design.

Status: **Confirmed non-finding.**

---

## Review Feedback: Finding 1 Fix

Reviewer: AI agent 2
Review date: 2026-05-08
Reviewed commit: `4b1a293476b7a8e51416ad345f54c2054f01bc8c`
Commit message: `fix: integer division truncation in bucket LRU percent config`

### Summary Of Fix

The fix changes bucket LRU percent sizing from division-before-multiplication to
multiplication-before-division:

```java
c.confBucket.lruPerFd.maxSize = bucketLruPerFdPercent * c.confBucket.bucketsPerSlot / 100;
```

This directly addresses the confirmed root cause: integer division no longer truncates configured values in
`1..99` to zero before multiplying by `bucketsPerSlot`.

### Strengths

- The production change is minimal and localized to the configuration calculation in
  `MultiWorkerServer.confForSlot()`.
- The calculation remains integer-based and safe for current bounds. `bucketLruPerFdPercent <= 100`, and
  `KeyBucket.MAX_BUCKETS_PER_SLOT` is `262144`, so the maximum product remains far below `Integer.MAX_VALUE`.
- The new Spock regression test covers a non-100 percent value (`50`) and verifies the configured LRU max size
  is half of the bucket count instead of zero.
- The focused test executes the changed line according to JaCoCo.

### Concerns

No blocking concerns found for this fix.

One non-blocking improvement for future coverage would be to add boundary examples for `0`, `99`, and `100`.
The current `50` case is enough to prove the original truncation bug is fixed, while the existing validation
still guards values outside `0..100`.

### Verification

- Ran:
  `./gradlew :test --rerun-tasks --tests "io.velo.MultiWorkerServerTest.test bucket LRU percent computes correct maxSize for non-100 percent"`
- Result: `BUILD SUCCESSFUL`; the selected Spock test passed.
- JaCoCo confirmation:
  `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html` marks line 1340 as covered (`fc`).

### Post-Commit Follow-Ups

- Finding 1 is fixed by commit `4b1a293476b7a8e51416ad345f54c2054f01bc8c`.
- Finding 2 remains open and should be fixed separately, with its own TDD cycle and commit.

---

## Review Feedback: Finding 2 Fix

Reviewer: AI agent 2
Review date: 2026-05-08
Reviewed commit: `1dffa10b4d987cd22f2b586016e6f27e7d924f0e`
Commit message: `fix: remove byteValue() truncation for onceScanMaxLoopCount config`

### Summary Of Fix

The fix removes the narrowing `.byteValue()` conversion from both `onceScanMaxLoopCount` config paths:

```java
c.confBucket.onceScanMaxLoopCount = config.get(ofInteger(), "bucket.onceScanMaxLoopCount");
c.confWal.onceScanMaxLoopCount = config.get(ofInteger(), "wal.onceScanMaxLoopCount");
```

This preserves the parsed integer value before the existing `1..1024` validation checks run.

### Strengths

- The production change is minimal and directly targets the confirmed truncation bug.
- Both affected config paths are fixed: bucket and WAL.
- The existing validation remains responsible for rejecting values outside `1..1024`, so the fix does not
  broaden accepted values beyond the intended range.
- The new regression test configures both values as `512`, which would have been truncated to `0` by the old
  `.byteValue()` code and rejected by validation.
- The focused test executes both changed lines according to JaCoCo.

### Concerns

No blocking concerns found for this fix.

One non-blocking improvement for future regression coverage would be to include a wrapped-but-valid case such
as `257`, which used to become `1` and could pass validation with the wrong value. The current `512` case is
still sufficient to prove values above `127` are preserved rather than narrowed.

### Verification

- Ran:
  `./gradlew :test --rerun-tasks --tests "io.velo.MultiWorkerServerTest.test onceScanMaxLoopCount accepts values above 127 via config"`
- Result: `BUILD SUCCESSFUL`; the selected Spock test passed.
- JaCoCo confirmation:
  `build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html` marks lines 1334 and 1368 as covered (`fc`).

### Post-Commit Follow-Ups

- Finding 2 is fixed by commit `1dffa10b4d987cd22f2b586016e6f27e7d924f0e`.
- Both confirmed findings in this bug review now have committed fixes and post-commit review feedback.
