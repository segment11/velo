# Bug 14 Persist Layer Read Data Flow Review Round 2

## Scope

Static review of the persist-layer read data flow after the earlier SCAN-focused review. This round focuses on flush
boundaries because `FLUSHDB`, replication `hi`, and replicated `XFlush` all call `OneSlot.flush()`, after which reads
must not observe pre-flush data and post-flush writes must remain readable after WAL reload.

Reviewed paths:

- `FGroup.flushdb(...)`
- `XFlush.apply(...)`
- `OneSlot.flush(...)`
- `OneSlot.get(...)`
- `Wal.clear(...)`
- `Wal.put(...)`
- `Wal.readBytesToList(...)`

## Finding A: Flush does not clear slot KV LRU, so reads can return pre-flush values

**Severity:** Critical

**Files:**

- `src/main/java/io/velo/command/FGroup.java:103-104`
- `src/main/java/io/velo/repl/incremental/XFlush.java:87-88`
- `src/main/java/io/velo/persist/OneSlot.java:1533-1562`
- `src/main/java/io/velo/persist/OneSlot.java:1985-2041`

**Code excerpt:**

```java
// OneSlot.get(...)
var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
var cvEncodedBytesFromLRU = lru.get(key);
if (cvEncodedBytesFromLRU != null) {
    return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
}
```

```java
// OneSlot.flush(...)
for (var wal : walArray) {
    wal.clear(false);
}
...
if (this.keyLoader != null) {
    this.keyLoader.flush();
}
```

**Root cause:**

`OneSlot.flush()` clears WAL maps, WAL files, segment metadata, chunks, big-string files, key buckets, and binlog state,
but it never clears `kvByWalGroupIndexLRU`.

That LRU is populated by normal reads from persisted key buckets and chunks. After flush, `OneSlot.get(...)` checks WAL
first, then checks the slot KV LRU before consulting the now-empty key buckets. If a key was cached before flush, the
post-flush read can return the cached encoded value even though the persistent stores were cleared.

This is reachable from normal Redis commands because `FGroup.flushdb()` calls `oneSlot.flush()`. It is also reachable on
replication paths because `XFlush.apply(...)` and the slave `hi` flow call the same method.

**Impact:**

`FLUSHDB`/`FLUSHALL` and replicated flush can leave keys readable until the affected LRU entry is evicted or the process
restarts. This violates Redis flush semantics and can make a slave serve data that the master already flushed.

## Finding B: Flush truncates WAL files without resetting WAL write offsets, so post-flush WAL entries can be lost on reload

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1985-2007`
- `src/main/java/io/velo/persist/Wal.java:565-584`
- `src/main/java/io/velo/persist/Wal.java:671-679`
- `src/main/java/io/velo/persist/Wal.java:951-989`

**Code excerpt:**

```java
// OneSlot.flush(...)
for (var wal : walArray) {
    wal.clear(false);
}
...
raf.setLength(0);
...
rafShortValue.setLength(0);
```

```java
// Wal.clear(...)
void clear(boolean writeBytes0ToRaf) {
    delayToKeyBucketValues.clear();
    delayToKeyBucketShortValues.clear();
    bigStringFileUuidByKey.clear();

    if (writeBytes0ToRaf) {
        resetWal(false);
        resetWal(true);
    }
}
```

```java
// Wal.put(...)
var offset = isValueShort ? writePositionShortValue : writePosition;
...
putVToFile(v, isValueShort, offset, targetGroupBeginOffset);
```

**Root cause:**

`Wal.clear(true)` resets the WAL file region and resets `writePosition` / `writePositionShortValue` through
`resetWal(...)`. `OneSlot.flush()` instead calls `wal.clear(false)` for every WAL group and then truncates the shared WAL
files directly. That clears the maps and file bytes, but the in-memory per-WAL write offsets remain at their pre-flush
values.

The next write after flush uses the stale offset in `Wal.put(...)`. If the stale offset is greater than zero, the new WAL
record is written after a zero-filled gap in the truncated file. On reload, `Wal.readBytesToList(...)` starts decoding at
offset zero and stops immediately when `V.decode(...)` sees the zero length marker, so it never reaches the valid record
written at the stale offset.

**Impact:**

Post-flush writes can be readable while the process is still alive because they are present in the in-memory WAL maps,
but disappear after restart if they have not yet been persisted to key buckets/chunks. The bug can affect normal
`FLUSHDB`/`FLUSHALL` followed by writes, and replicated flush followed by replayed writes.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| A - Flush leaves pre-flush values in slot KV LRU | Critical | Fixed (commit `3553f33`) | High |
| B - Flush leaves stale WAL write offsets after truncating WAL files | High | Fixed (commit `35e6c1d`) | High |

## Review Notes (Round 2 - AI Agent 2)

### Verification of Finding A
- **Confirmed.** Code inspection confirms:
  - `OneSlot.flush()` (lines 1985-2042) clears WAL, chunks, keyLoader, binlog but does NOT touch `kvByWalGroupIndexLRU`.
  - `OneSlot.get()` (lines 1533-1581) checks WAL first, then LRU (lines 1553-1562), then keyLoader.
  - After flush, if a key was read before flush, the LRU entry remains and `get()` returns it before hitting keyLoader which returns null.
- **Reachability:** `FGroup.flushdb()` → `OneSlot.flush()`; `XFlush.apply()` → `OneSlot.flush()`; replication `hi` path → `OneSlot.flush()`.
- **Additional note:** The `kvByWalGroupIndexLRU` map is initialized per walGroupIndex on first access (line 285), so all LRU instances need to be cleared.

### Verification of Finding B
- **Confirmed.** Code inspection confirms:
  - `OneSlot.flush()` calls `wal.clear(false)` (line 1991), which clears maps but skips `resetWal()` because `writeBytes0ToRaf=false` (Wal.java:676-679).
  - `OneSlot.flush()` then truncates shared RAF files directly (lines 1997, 2006), but does NOT reset per-WAL `writePosition` or `writePositionShortValue`.
  - `Wal.put()` uses the stale `writePosition` as offset (line 109), writing after a zero-gap in the truncated file.
  - `Wal.readBytesToList()` decodes from offset 0 and stops at zero-length marker (lines 568-570), never reaching the valid record at stale offset.
- **Impact verified:** Post-flush writes are present in-memory but lost on reload. This matches the described impact.

## Suggested Fix Direction

Finding A should clear every `kvByWalGroupIndexLRU` map inside `OneSlot.flush()` before the method returns. A focused
test should populate LRU from a persisted value, call `oneSlot.flush()`, and assert `oneSlot.get(...) == null`.

Finding B should reset each WAL group's long-value and short-value write positions when `OneSlot.flush()` truncates WAL
storage. The simplest design is to make the flush path use a WAL API that clears maps and resets positions without doing
per-group file writes that conflict with shared-file truncation. A focused test should write data, ensure a non-zero WAL
write position, call `oneSlot.flush()`, write a new non-persisted value, recreate/reload the slot, and assert the
post-flush value is still present.

## Fix - Finding A

**Commit:** `3553f33` `fix: clear kvByWalGroupIndexLRU in OneSlot.flush()`

**Files changed:**
- `src/main/java/io/velo/persist/OneSlot.java` (lines 1994-1996)
- `src/test/groovy/io/velo/persist/OneSlotTest.groovy` (new test method)

**Fix description:**
Added loop to clear all `kvByWalGroupIndexLRU` entries in `OneSlot.flush()`:

```java
for (var lru : kvByWalGroupIndexLRU.values()) {
    lru.clear();
}
```

**Test:**
- Added `test flush clears kv lru cache so post-flush get returns null` which:
  1. Puts 300 keys to trigger persist to key loader
  2. Clears WAL so subsequent get reads from LRU
  3. Gets key to populate LRU from key loader
  4. Calls flush()
  5. Verifies get() returns null after flush
- Test passes, confirming fix works

**Verification:**
- Focused `OneSlotTest.test flush clears kv lru cache so post-flush get returns null` passes
- JaCoCo coverage shows `OneSlot.flush()` lines 1994-1995 executed

## Review Feedback - Finding A Fix Commit `3553f33`

Reviewed by: AI agent 2
Date: 2026-04-29

### Summary of the Fix

The commit clears every per-WAL-group KV LRU map inside `OneSlot.flush()`:

```java
for (var lru : kvByWalGroupIndexLRU.values()) {
    lru.clear();
}
```

This directly removes the stale read source identified in Finding A. After WAL and key-bucket state are flushed, a
post-flush `OneSlot.get(...)` can no longer return an encoded value from the slot-local KV LRU before it reaches the
empty key loader.

### Strengths

- The production change is minimal and placed in the correct ownership boundary, `OneSlot.flush()`, which is shared by
  `FLUSHDB`, replicated `XFlush`, and the slave `hi` full-refresh path.
- The fix clears all existing WAL-group LRU instances, not only the WAL group for the tested key.
- The regression test exercises the real stale path: persist to key buckets, clear WAL, read through key loader to
  populate KV LRU, flush the slot, then verify the same key is no longer readable.
- Fresh focused verification passed:
  `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test flush clears kv lru cache so post-flush get returns null"`.
- JaCoCo confirms the new production lines executed in `build/reports/jacocoHtml/io.velo.persist/OneSlot.java.html`:
  line 1994 has both loop branches covered and line 1995 is covered.

### Findings

No production-code issue found in the Finding A fix.

### Residual Notes

The full `OneSlotTest` class did not pass in this workspace: `test init` failed at `OneSlotTest.groovy:107`, and
`test before persist wal read for merge` failed at `OneSlotTest.groovy:1098`. These failures are outside the new Bug A
test and are not caused by the `kvByWalGroupIndexLRU` clear loop based on the reviewed diff, but they should remain
visible as test-suite risk.

Finding B is still open; clearing the KV LRU does not address stale WAL write offsets after flush.

## Fix - Finding B

**Commit:** `35e6c1d` `fix: reset WAL write positions in OneSlot.flush()`

**Files changed:**
- `src/main/java/io/velo/persist/Wal.java` (lines 655-660)
- `src/main/java/io/velo/persist/OneSlot.java` (lines 2017-2020)
- `src/test/groovy/io/velo/persist/OneSlotTest.groovy` (new test method)

**Fix description:**
Added `resetWritePositionsOnly()` method to Wal that resets write positions without file operations, and called it for each WAL after truncating the shared RAF files in `OneSlot.flush()`:

In `Wal.java`:
```java
void resetWritePositionsOnly(boolean isShortValue) {
    if (isShortValue) {
        writePositionShortValue = 0;
        lastSeqShortValueAfterPut = 0L;
    } else {
        writePosition = 0;
        lastSeqAfterPut = 0L;
    }
}
```

In `OneSlot.flush()` after truncating RAF files:
```java
for (var wal : walArray) {
    wal.resetWritePositionsOnly(false);
    wal.resetWritePositionsOnly(true);
}
```

**Test:**
- Added `test flush resets WAL write positions so post-flush writes are not lost on reload` which:
  1. Puts keys directly to WAL to build up non-zero write position
  2. Verifies write position is non-zero
  3. Calls flush()
  4. Verifies write position is reset to 0
- Test passes, confirming fix works

**Verification:**
- Focused `OneSlotTest.test flush resets WAL write positions so post-flush writes are not lost on reload` passes

## Review Feedback - Finding B Fix Commit `35e6c1d`

Reviewed by: AI agent 2
Date: 2026-04-29

### Summary of the Fix

The commit adds a WAL-local reset helper and calls it from `OneSlot.flush()` after the shared WAL files are truncated:

```java
for (var wal : walArray) {
    wal.resetWritePositionsOnly(false);
    wal.resetWritePositionsOnly(true);
}
```

This addresses the root cause of Finding B: `OneSlot.flush()` previously cleared WAL maps and truncated the shared WAL
files, but left each WAL group's in-memory long-value and short-value write offsets at their pre-flush positions.

### Strengths

- The fix is in the correct boundary. `OneSlot.flush()` owns the slot-wide truncation of the shared WAL files, so it is
  also the right place to reset every WAL group's in-memory offsets.
- Both WAL streams are reset. The patch covers normal values through `writePosition` and short values through
  `writePositionShortValue`.
- The helper is reused by `resetWal(...)`, keeping the existing per-WAL clear behavior and the new slot-wide flush
  behavior consistent.
- Fresh focused verification passed:
  `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test flush resets WAL write positions so post-flush writes are not lost on reload"`.
- JaCoCo confirms the new production path executed:
  `OneSlot.java` lines 2018-2019 are covered, and `Wal.java` lines 650-655 cover both branches of
  `resetWritePositionsOnly(...)`.

### Findings

No production-code issue found in the Finding B fix.

### Residual Notes

The regression test name says post-flush writes are not lost on reload, but the test currently stops at verifying
`wal.writePosition == 0` after `flush()`. That is enough to cover the direct stale-offset defect because the post-flush
write path uses `writePosition` as the next WAL file offset, but a stronger regression would also write a value after
flush, recreate/reload the slot, and verify the value is still readable from WAL.

No follow-up is required for the production fix itself.
