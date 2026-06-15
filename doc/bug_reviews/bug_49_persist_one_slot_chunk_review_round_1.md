# Bug 49: OneSlot / Chunk Review — Round 1

**Author:** AI Agent 1
**Date:** 2026-06-13
**Module:** persist
**Classes:**
- `io.velo.persist.OneSlot` (`src/main/java/io/velo/persist/OneSlot.java`, 2268 lines)
- `io.velo.persist.Chunk` (`src/main/java/io/velo/persist/Chunk.java`, 545 lines)

**Related classes reviewed for context:**
- `io.velo.persist.Wal` (`src/main/java/io/velo/persist/Wal.java`, 1020 lines) — Wal.put, removeDelay, bulkLoad
- `io.velo.persist.MetaChunkSegmentFlagSeq` (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java`, 716 lines) — findCanReuseSegmentIndex
- `io.velo.persist.SegmentBatch2` (`src/main/java/io/velo/persist/SegmentBatch2.java`) — split

**Design documents reviewed:**
- `doc/design/02_persist_layer_design.md`

**Prior reviews reviewed for coverage gaps:**
- `doc/bug_reviews/bug_46_persist_layer_review_round_1.md` (OneSlot.put big-string, SegmentBatch2.split, intervalDeleteOverwriteBigStringFiles)
- `doc/bug_reviews/bug_47_persist_key_bucket_review_round_1.md` (KeyBucket)
- `doc/bug_reviews/bug_48_keybuckets_keyloader_review_round_1.md` (KeyBucketsInOneWalGroup / KeyLoader)

---

## Scope

This review drills into `OneSlot` and `Chunk`, focusing on areas not covered in prior rounds (bugs 11–48). Prior rounds covered:
- SegmentBatch2.split empty-segment flush (bug 46 Finding 1)
- OneSlot.put big-string file orphan on doPersist throw — `needPutV != null` path (bug 46 Finding 2)
- OneSlot.intervalDeleteOverwriteBigStringFiles silent drop (bug 46 Finding 3)
- KeyBucket put non-atomicity, PRE_KEY buffer position, sentinel keyHash (bug 47)
- KeyBucketsInOneWalGroup/KeyLoader split-number crash safety, readKeysToList counter leak (bug 48)

This round focuses on:
- `OneSlot.removeDelay` — binlog ordering relative to `doPersist`
- `OneSlot.put` — binlog ordering for the `needPutV == null` path (not covered by bug 46 fix)
- `OneSlot.get` read path correctness
- `Chunk.persist` — segment disk write vs. key bucket update ordering

---

## Finding 1: `OneSlot.removeDelay` — binlog appended before `doPersist`, stale on persist failure

**Severity:** Medium

**Files:**
- `src/main/java/io/velo/persist/OneSlot.java:1351-1387` (`removeDelay`)
- `src/main/java/io/velo/persist/OneSlot.java:1575-1595` (`doPersist`)

**Code excerpt:**

```java
// OneSlot.removeDelay, lines 1351-1387 (simplified)
public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
    checkCurrentThreadId();
    // ...
    var putResult = targetWal.removeDelay(key, bucketIndex, keyHash, lastPersistTimeMs);

    boolean isBinlogAppended = false;
    if (putResult.needPersist()) {
        if (putResult.needPutV() == null) {
            var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
            appendBinlog(xWalV);                         // line 1368: binlog BEFORE persist
            isBinlogAppended = true;
        }
        doPersist(walGroupIndex, key, putResult);         // line 1373: can throw
    }

    if (overwrittenBigStringUuid != null) {               // skipped if exception above
        var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
        if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
            delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(
                    overwrittenBigStringUuid, bucketIndex, keyHash, key));
        }
    }

    if (!isBinlogAppended) {                              // skipped if isBinlogAppended=true
        var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
        appendBinlog(xWalV);
    }
}
```

For comparison, the `put()` method wraps `doPersist` in try-catch (added in bug 46 fix, commit `641906ab`):

```java
// OneSlot.put, lines 1501-1521
if (putResult.needPersist()) {
    if (putResult.needPutV() == null) {
        if (xBigStrings != null) { appendBinlog(xBigStrings); }
        var xWalV = new XWalV(putResult.v(), isValueShort);
        appendBinlog(xWalV);                            // binlog BEFORE persist
        isBinlogAppended = true;
    }

    try {
        doPersist(walGroupIndex, key, putResult);
    } catch (RuntimeException e) {
        if (xBigStrings != null && putResult.needPutV() != null) {
            delayToDeleteBigStringFileIds.add(...);
            isBinlogAppended = true;
        }
        throw e;
    }
}
```

**Root cause and impact:**

`removeDelay` appends the `XWalV` binlog entry at line 1368 **before** calling `doPersist` at line 1373, when `putResult.needPutV() == null` (WAL buffer not full and size trigger reached). If `doPersist` throws — e.g., `SegmentOverflowException` from `Chunk.persist`, `BucketFullException` from `KeyBucketsInOneWalGroup.putPvmListToTargetBucket`, or any `RuntimeException` from `keyLoader.updatePvmListBatchAfterWriteSegments` — the damage is:

1. **Binlog has a stale entry.** The `XWalV` entry references a delete operation whose data was never persisted to chunk segments and key buckets (`doPersist` failed before completing). If replication is active, the master will send this binlog entry to slaves. A slave that replays it will apply the delete, potentially losing data that the master still has (because the WAL still holds the pre-persist state).

2. **The overwritten big-string cleanup is skipped.** Lines 1376–1381 run only if `doPersist` completes successfully. If it throws, any `overwrittenBigStringUuid` captured at line 1361 is never enqueued for deletion. The old big-string file (now orphaned) stays on disk until the periodic orphan scan in `intervalDeleteOverwriteBigStringFiles` eventually discovers it — which can take hours.

3. **No exception protection.** Unlike `put()` which wraps `doPersist` in a `try`/`catch` (added in bug 46 fix, commit `641906ab`), `removeDelay` has no such protection. Any exception from `doPersist` propagates directly out of `removeDelay`, leaving both the binlog and the big-string cleanup in an inconsistent state.

The same pattern exists in `put()` for the `needPutV == null` path (lines 1503–1509). The bug 46 fix only added protection for the `needPutV != null` path (the big-string file cleanup case). When `needPutV == null`, binlog entries are still appended before `doPersist`, and the catch block at line 1514 is entered but its guard `putResult.needPutV() != null` is false, so no cleanup occurs.

**Reachability:**

Reachable any time `removeDelay` causes the WAL size to hit the persist trigger (`ConfForSlot.global.confWal.valueSizeTrigger` or the at-least-once-persist timer threshold) while the WAL buffer is not yet full (`needPutV == null`). The `doPersist` then throws if `Chunk.persist` cannot find enough consecutive reusable segments (`SegmentOverflowException`), or if `KeyBucketsInOneWalGroup.putPvmListToTargetBucket` overflows the bucket split limit, or if `keyLoader.updatePvmListBatchAfterWriteSegments` fails.

The probability is correlated with segment ring utilization — higher utilization means fewer reusable segments, making `SegmentOverflowException` more likely. Hot keys (frequent deletes on the same key) also increase the chance of hitting the WAL persist trigger during a `removeDelay`.

**Suggested fix direction:**

Wrap `doPersist` in `removeDelay` with a `try`/`catch` that rolls back the binlog state. Two options:

1. **Minimal fix — catch and re-throw, mask binlog:**
   ```java
   if (putResult.needPersist()) {
       if (putResult.needPutV() == null) {
           var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
           appendBinlog(xWalV);
           isBinlogAppended = true;
       }
       try {
           doPersist(walGroupIndex, key, putResult);
       } catch (RuntimeException e) {
           // Binlog entry is stale; the operation was not persisted.
           // The caller must retry. Do not re-append.
           isBinlogAppended = true;  // prevent trailing append from adding another stale entry
           throw e;
       }
   }
   ```
   This prevents the trailing `if (!isBinlogAppended)` block (line 1383) from appending yet another stale entry, and signals to the caller that the operation failed. It does NOT delete the already-appended binlog entry (which is difficult since `Binlog.append` is append-only). A follow-up could add a `Binlog.markLastEntryInvalid()` or rely on the slave detecting the inconsistency via a subsequent `XFlush` or `XUpdateSeq`.

2. **Reorder: append binlog AFTER doPersist** (more invasive, matching the `needPutV != null` path in `put()`):
   ```java
   if (putResult.needPersist()) {
       doPersist(walGroupIndex, key, putResult);  // persist first
       // Only append binlog if persist succeeded
       var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
       appendBinlog(xWalV);
   }
   ```
   This eliminates the window entirely, consistent with how the `needPutV != null` path in `put()` works. The downside: if `doPersist` succeeds but `appendBinlog` throws, the master-servable state diverges from the binlog. However, `appendBinlog` failures are typically terminal (disk full), so this is an acceptable trade-off.

Option 2 is the stronger fix and aligns with the `needPutV != null` path precedent.

The same fix should be applied to the `put()` method's `needPutV == null` path (lines 1503–1509), either by reordering binlog after `doPersist` or by adding try-catch cleanup.

**Regression test should include:**
- A `OneSlot.removeDelay` that hits the WAL persist trigger with `needPutV == null`. Force `doPersist` to throw (via `doPersistForceThrowForTest`). Assert the exception propagates. Assert the binlog does NOT contain a trailing duplicate `XWalV` entry for the failed operation.
- Verify that subsequent `exists()` on the deleted key returns `true` (the delete was not persisted).
- Verify that if `overwrittenBigStringUuid` was captured, the file is still on disk (not prematurely cleaned up) and can be recovered.
- A corresponding test for `OneSlot.put` with `needPutV == null` and `doPersist` throwing — assert no stale binlog entries.

---

## Finding 2: `Chunk.persist` — orphaned segments when `keyLoader.updatePvmListBatchAfterWriteSegments` throws after segment writes

**Severity:** Low

**Files:**
- `src/main/java/io/velo/persist/Chunk.java:289-420` (`persist`)
- `src/main/java/io/velo/persist/OneSlot.java:1849-1925` (`putValueToWal`)

**Code excerpt:**

```java
// Chunk.persist, lines 289-420 (simplified)
public void persist(int walGroupIndex, @NotNull ArrayList<Wal.V> list, ...) {
    ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
    var segments = ...segmentBatch2.split(list, pvmList);
    short segmentCount = (short) segments.size();

    var ii = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(segmentIndex, segmentCount);
    // ... retry from 0 if -1 ...
    if (ii == -1) {
        throw new SegmentOverflowException(...);
    }
    segmentIndex = ii;
    var currentSegmentIndex = this.segmentIndex;

    // Adjust pvm segment indices
    for (var pvm : pvmList) {
        pvm.segmentIndex += currentSegmentIndex;
    }

    // --- Phase 1: Write segments to disk and set HAS_DATA flags ---
    if (segmentCount < BATCH_ONCE_SEGMENT_COUNT_WRITE) {
        for (var segment : segments) {
            writeSegments(bytes, 1);                                 // disk write
            oneSlot.setSegmentMergeFlag(targetSegmentIndex,
                    SEGMENT_FLAG_HAS_DATA, segment.segmentSeq(), walGroupIndex);  // flag
            moveSegmentIndexNext(1);
        }
    } else {
        // ... batch path — same pattern ...
        writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_WRITE);
        oneSlot.setSegmentMergeFlagBatch(...);
        moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_WRITE);
        // ... remainder path — same pattern ...
    }

    persistCallCountTotal++;
    persistCvCountTotal += list.size();

    // --- Phase 2: Update key buckets ---
    var beginT = System.nanoTime();
    try {
        keyLoader.updatePvmListBatchAfterWriteSegments(            // line 406: can throw
                walGroupIndex, pvmList, keyBucketsInOneWalGroupGiven);
    } catch (RuntimeException e) {
        log.error("Update key buckets after write chunk segments failed, ...", e);
        throw e;
    }
    updatePvmBatchCostTimeTotalUs += costT;

    // --- Phase 3: Mark WAL group range (NOT reached if Phase 2 throws) ---
    metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(  // line 416
            walGroupIndex, currentSegmentIndex, segmentCount);

    // Update meta segment index
    oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
}
```

**Root cause and impact:**

`Chunk.persist` writes segments to disk and sets `SEGMENT_FLAG_HAS_DATA` flags (Phase 1) **before** updating key buckets (Phase 2) and **before** calling `markPersistedSegmentIndexToTargetWalGroup` (Phase 3). If `keyLoader.updatePvmListBatchAfterWriteSegments` at line 406 throws, the method rethrows after logging.

At this point:

1. **Segments are on disk with HAS_DATA flags.** The `setSegmentMergeFlag` / `setSegmentMergeFlagBatch` calls in Phase 1 clear the reuse bit for these segments in `segmentCanReuseBitSet`, marking them as occupied.

2. **The WAL group marker was NOT written.** `markPersistedSegmentIndexToTargetWalGroup` at line 416 is never reached. Without this marker, the merge subsystem cannot associate these segments with the WAL group. The `findThoseNeedToMerge` method (called from `readSomeSegmentsBeforePersistWal`) relies on these markers to locate segments eligible for forward-merge.

3. **The key buckets were NOT updated.** The `pvmList` entries (PersistValueMeta) that point to the new segment data were never written to key buckets. On the read path, `OneSlot.get()` will not find these segments because the key bucket → pvm → segment chain is broken.

4. **The meta segment index was NOT updated.** `setMetaChunkSegmentIndexInt` at line 419 is not reached. On restart, the segment index from `MetaChunkSegmentIndex` will be stale, potentially causing the next persist to reuse segments that appear "free" from the meta index perspective but are actually occupied (HAS_DATA flag set).

**The WAL replay on restart handles data recovery.** Since `doPersist` (the caller of `putValueToWal` → `chunk.persist`) calls `clearValues()` only AFTER `putValueToWal` returns, the failed persist leaves the WAL intact. On restart, WAL lazy-read replays the values and re-executes persistence. The data eventually lands in new segments.

**But the orphaned segments are a permanent leak until overwritten.** The segments written in Phase 1 have HAS_DATA flags but no WAL group association and no key bucket references. The merge subsystem uses `findThoseNeedToMerge` which requires WAL group markers. Without them, these segments are never discovered for merge cleanup. They remain "in use" until the segment ring wraps around and overwrites them — which for a default config of 262,144 segments could be a long time. Each orphaned segment wastes `chunkSegmentLength` (default 4096) bytes on disk.

**Reachability:**

`updatePvmListBatchAfterWriteSegments` throws if a bucket split overflow occurs (`BucketFullException` from `KeyBucketsInOneWalGroup.putPvmListToTargetBucket`, line 314-320 of KeyBucketsInOneWalGroup.java), or if key bucket file writes fail (`IOException`). The bucket split overflow is the more reachable trigger — it occurs when a WAL group's buckets reach the `MAX_SPLIT_NUMBER = 9` limit after a split and new data still needs to be placed.

The probability is low in normal operation but increases under skewed workloads (many keys hashing to the same bucket, causing rapid splits). The segments written before the failure are the only wasted resource; the data itself is not lost.

**Suggested fix direction:**

Two complementary approaches:

1. **Reorder: update key buckets BEFORE writing segments.** This eliminates the orphan window entirely. The key bucket update is idempotent (can be retried), while segment writes are not (they consume segment indices). If the key bucket update succeeds but the subsequent segment write fails, the key buckets reference non-existent segments. This is worse — reads would fail with `IllegalStateException` ("Load persisted segment bytes error"). So this approach alone is not safe.

2. **Add a rollback path:** If `updatePvmListBatchAfterWriteSegments` throws, mark the just-written segments as `SEGMENT_FLAG_REUSABLE` and commit the range. Sketch:

```java
try {
    keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, keyBucketsInOneWalGroupGiven);
} catch (RuntimeException e) {
    log.error("...", e);
    // Roll back: mark the wasted segments as reusable
    oneSlot.setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount,
            Chunk.SEGMENT_FLAG_REUSABLE, null, walGroupIndex);
    metaChunkSegmentFlagSeq.commitMergedRangeWithMarkerIdx(
            walGroupIndex, currentSegmentIndex, segmentCount, /*markerIdx*/ 0);
    throw e;
}
```

This reuses the existing merge-commit path (`commitMergedRangeWithMarkerIdx`) to reclaim the orphaned segments. The `markerIdx = 0` case may need validation for correctness. The segments are marked reusable and the reuse bitset is updated, making them available for the next `findCanReuseSegmentIndex` call.

**Regression test should include:**
- A `Chunk.persist` that triggers a bucket split overflow in `updatePvmListBatchAfterWriteSegments`. Assert the exception propagates. Assert the segments written before the failure are marked as reusable (verify via `getSegmentMergeFlag` returning `SEGMENT_FLAG_REUSABLE`). Assert the segment index is NOT advanced (the failed segments should be available for reuse on the next persist).
- Verify that after the exception, the WAL still contains the data and a subsequent `doPersist` succeeds and writes the data to new segments.

---

## Non-Findings (Verified Safe)

1. **`Chunk.persist` segment index wrap-around:** `findCanReuseSegmentIndex` searches within a single FD and never returns a range that wraps the segment ring. If the wraparound range is needed, the retry from index 0 (line 305) finds it. The downstream `checkBeginSegmentIndex` validates against `maxSegmentIndex`. No wrap-around bug.

2. **`OneSlot.put` double-insertion of `needPutV`:** `doPersist` (line 1590-1594) re-inserts `needPutV` into the WAL. The `put()` method does NOT re-insert it again (the code at lines 1522-1537 handles overwrite cleanup and trailing binlog only). No double-insertion.

3. **`OneSlot.get` expiration handling:** `getFromWal` returns null and sets `isExpiredFlagArray[0] = true` for expired entries. `get()` checks the flag at line 1183 and returns null. Deleted entries return non-null tombstone bytes, which `get()` filters via `CompressedValue.isDeleted()` at line 1177. Both paths are correct.

4. **`OneSlot.flush` ordering:** The flush sequence (clear WAL → truncate files → reset positions → clear metadata → delete big strings → flush key loader → truncate binlog) is ordered correctly for the startup/new-slave use case. Any exception during flush propagates and aborts the slot initialization.

5. **`Chunk.persist` batch-path zero-fill:** The `new byte[...]` allocation at line 336 is zero-initialized by Java for the first batch. For subsequent batches (`i > 0`), `Arrays.fill(tmpBatchBytes, (byte) 0)` at line 342 re-zeros. Padding bytes between segment data are zero, not stale.

---

## Summary

| # | Severity | Description | Status |
|---|----------|-------------|--------|
| 1 | Low (downgraded) | `OneSlot.removeDelay` appends binlog before `doPersist`; main replication-divergence claim refuted, only a minor overwritten big-string cleanup leak survives | Refuted (main claim) / Confirmed (secondary leak only) |
| 2 | Low | `Chunk.persist` orphans segments when `updatePvmListBatchAfterWriteSegments` throws after segment writes | Confirmed |

---

## Review (AI Agent 2)

**Reviewer:** AI Agent 2
**Date:** 2026-06-13
**Branch:** `review/persist`
**Code state:** `main` at the time of review (no fixes applied yet for bug 49)

### Verification environment

- All file:line citations re-checked against the in-tree source: `OneSlot.java` (2268 lines), `Chunk.java` (545 lines), `Wal.java` (1020 lines), `MetaChunkSegmentFlagSeq.java` (716 lines), `KeyLoader.java`, `KeyBucketsInOneWalGroup.java`, `ChunkWalGroupRebuilder.java`, `XWalV.java`. The line numbers in Finding 1 and Finding 2 match the actual code.
- The bug 46 fix commit `641906ab` was inspected (`git show 641906ab -- src/main/java/io/velo/persist/OneSlot.java`) and matches the description in Finding 1.
- Existing tests inspected for context: `OneSlotTest.groovy::test put big string with WAL buffer full and doPersist throw enqueues new uuid for cleanup` (lines 1573-1630) — covers the bug 46 `put()` `needPutV != null` path; `ChunkWalGroupRebuildTest.groovy::FailingKeyLoader` test (lines 197-219) — demonstrates that segment 0 is left as `HAS_DATA` after `updatePvmListBatchAfterWriteSegments` throws.

---

### Finding 1 — **Refuted (main claim) / Confirmed at much lower severity (secondary leak only)**

The line numbers, the absence of a `try`/`catch` around `doPersist` in `removeDelay`, and the asymmetry with `OneSlot.put` are all real. The *consequences* described in the bug entry are largely incorrect.

#### 1a. "Binlog has a stale entry → slave loses data" — **Refuted**

The bug entry claims that if `doPersist` throws in `removeDelay` on the `needPutV == null` path, the slave will apply the binlog `XWalV(tombstone)` and "lose data that the master still has (because the WAL still holds the pre-persist state)".

This misreads the WAL state machine:

- `Wal.removeDelay(...)` (line 673-679) calls `put(true, key, v, ...)` with a tombstone `Wal.V` (`expireAt = EXPIRE_NOW = -1`, `cvEncoded = {SP_FLAG_DELETE_TMP}`).
- Inside `Wal.put` for the `needPutV == null` branch, the tombstone IS inserted into `delayToKeyBucketShortValues` and into the on-disk WAL ring (line 877). At this point the WAL state is "key deleted".
- `addBigStringUuidIfMatch(v)` (line 898 / 511-523) removes the key from `bigStringFileUuidByKey` because the tombstone payload length is < 28.
- Only after all that does `OneSlot.removeDelay` call `appendBinlog(xWalV)` and then `doPersist`.

If `doPersist` throws afterwards, the WAL **already holds the tombstone**. Subsequent `OneSlot.exists(key, ...)` on the master:
1. `getFromWal` finds the tombstone `Wal.V`,
2. `v.isExpired()` returns `true` because `expireAt == -1`,
3. `getFromWal` returns `null` and sets `isExpiredFlagArray[0] = true`,
4. `exists` returns `false` (`OneSlot.java:1326-1328`).

The slave then receives `XWalV(tombstone)`, applies it via `XWalV.apply` → `targetWal.put(true, key, v2)` → slave WAL also has tombstone → slave `exists` also returns `false`. Both nodes observe the key as deleted. **No data loss, no divergence.**

Furthermore, the suggested "Option 2" reorder (append binlog AFTER `doPersist`) would actually *introduce* a divergence in the failure case. If persist throws after the reorder:
- Master WAL still holds the tombstone (the WAL was updated by `Wal.removeDelay` before `doPersist` was even called) — master observes the key as deleted,
- Slave never receives the binlog entry — slave still observes the key as existing.

The current "binlog before `doPersist`" ordering is in fact the correct pattern for replication consistency: the binlog is the source of truth and the master's WAL retains the post-`Wal.put` state on persist failure so that the next `doPersist` retry can apply it. Applying Option 2 would be a regression.

For the `needPutV != null` branch of `removeDelay`, the binlog is also NOT appended (by design, because `Wal.put` returned early without inserting the tombstone into `delayToKeyBucketShortValues`). Master state and binlog are again aligned — both reflect "operation did not happen".

#### 1b. "Trailing `if (!isBinlogAppended)` adds a duplicate stale entry" — **Refuted**

In the `needPutV == null` + `doPersist` throws path: `isBinlogAppended = true` was set before the throw, but more importantly, the `throw` propagates out of `removeDelay` *before* control ever reaches the trailing `if (!isBinlogAppended)` block at line 1383-1386. There is no double append. The bug 46 fix's `isBinlogAppended = true` assignment inside the `put()` catch block (line 1518) is similarly redundant given the immediate re-throw, but it is harmless.

#### 1c. "Overwritten big-string cleanup is skipped → orphan file leak" — **Confirmed, but minor**

This is the one real (but minor) concern. When `removeDelay` captures `overwrittenBigStringUuid != null` and `doPersist` throws on the `needPutV == null` path:
- Lines 1376-1381 (which would have enqueued the old uuid into `delayToDeleteBigStringFileIds`) are not reached,
- The old big-string file remains on disk while the WAL tombstone shadows the chunk's old PVM.

However, `OneSlot.intervalDeleteOverwriteBigStringFiles` (called every 10ms from `doTask`, processing one bucket per tick) DOES recover this case:
- For the affected bucket, the `else` branch at lines 941-961 enters because `persistedIdWithKeyList` still contains the old `(uuid, key)` (the key bucket hasn't been updated yet).
- `targetWal.hasKey(key)` returns `true` because the tombstone is in `delayToKeyBucketShortValues` (`Wal.hasKey`, line 692-694).
- So `canDelete = true` and the uuid is enqueued.

Worst-case latency: a full rotation of the per-bucket scan takes `bucketsPerSlot * 10ms`. With the default `bucketsPerSlot = 16384` per WAL group × number of WAL groups, it is on the order of minutes-to-tens-of-minutes, not "hours" as the bug entry claims. The leak is real but bounded and self-healing.

The severity of Finding 1 should be **downgraded from Medium to Low**, and the recommended fix is **NOT** the binlog reorder. If anything is to be done at all, only the inline enqueue of `overwrittenBigStringUuid` in a `try`/`catch` around `doPersist` is worth it, purely to shorten the cleanup latency. This is optional and can be deferred.

#### 1d. Code-style asymmetry with `put()` — **Confirmed (cosmetic)**

`removeDelay` lacks the `try`/`catch` wrapper that `put()` has. The `put()` catch block is itself only meaningfully active on the `needPutV != null` path (cleaning up the just-written new big-string file when the WAL didn't accept the new `V`). In `removeDelay` there is no analogous "new big-string was created but never referenced" case — `removeDelay` never writes a new big-string file. So even adding the wrapper for symmetry would have no real effect beyond the small `overwrittenBigStringUuid` enqueue described in 1c. **No functional bug here.**

#### 1e. Same-file `OneSlot.put` `needPutV == null` path — **Refuted**

The bug entry claims the bug 46 fix only covered the `needPutV != null` path and that the `needPutV == null` path of `put()` still leaks. The same reasoning as 1a-1c applies:
- In the `needPutV == null` path, `Wal.put` already inserted `v` into `delayToKeyBucketShortValues`/`delayToKeyBucketValues` and (for big-string) inserted `bigStringFileUuidByKey[key] = newUuid` via `addBigStringUuidIfMatch`. The new big-string file is now referenced from WAL.
- If `doPersist` throws, the new big-string file is NOT orphan — `bigStringFileUuidByKey` keeps a live reference, so `intervalDeleteOverwriteBigStringFiles` will not delete it (`canDelete = !targetWal.bigStringFileUuidByKey.containsValue(id.uuid())` at line 954 returns `false`). The retried persist will eventually pick the value up from the WAL and write it to chunk segments.
- The binlog `XBigStrings` + `XWalV` entries are accurate: they describe the post-WAL master state, which the slave will reproduce by also putting `v` into its WAL and (likely) succeeding to persist on the slave side. No leak, no divergence.

Net verdict on Finding 1: only the small `overwrittenBigStringUuid` cleanup latency in `removeDelay` is worth tracking; everything else in the entry is either incorrect or a non-issue. **Status: Refuted (main claim) / Confirmed at Low severity (1c only).**

---

### Finding 2 — **Confirmed**

All cited line numbers and ordering claims match the current `Chunk.persist` implementation. The behavior is:

1. Phase 1 (lines 319-380) writes segment bytes to disk and calls `oneSlot.setSegmentMergeFlag(..., SEGMENT_FLAG_HAS_DATA, ...)` for every segment. `MetaChunkSegmentFlagSeq.setSegmentMergeFlag` (line 614-634) flips the segment's bit in `segmentCanReuseBitSet` to `false` via `updateBitSetCanReuseForSegmentIndex(..., Chunk.isSegmentReusable(SEGMENT_FLAG_HAS_DATA))`, and `Chunk.isSegmentReusable` returns `false` for `SEGMENT_FLAG_HAS_DATA`. So the segments are now marked "not reusable" by `findCanReuseSegmentIndex`.
2. Phase 2 (lines 404-414) calls `keyLoader.updatePvmListBatchAfterWriteSegments(...)`. This calls `KeyBucketsInOneWalGroup.putAllPvmList(...)` → `putPvmListToTargetBucket(...)` which throws `BucketFullException` (lines 366-367 / 406-407 of `KeyBucketsInOneWalGroup.java`) when the bucket split count would exceed `KeyLoader.MAX_SPLIT_NUMBER = 9`. The catch block at lines 407-411 only logs and rethrows.
3. Phase 3 (line 416) `metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segmentCount)` — never reached.
4. Phase 4 (line 419) `oneSlot.setMetaChunkSegmentIndexInt(segmentIndex)` — never reached either.

Consequences on throw:
- Segments are HAS_DATA on disk, with a valid `(segmentSeq, walGroupIndex)` tuple in `MetaChunkSegmentFlagSeq`, but no `pvm` in any key bucket points at them.
- `MetaChunkSegmentFlagSeq.findThoseNeedToMerge(walGroupIndex)` (line 443-487) iterates only over `beginSegmentIndexGroupByWalGroupIndex[walGroupIndex]`, which is populated only by `markPersistedSegmentIndexToTargetWalGroup`. Because Phase 3 was skipped, these segments will never be returned to the normal merge flow.
- `segmentIndex` was advanced in-memory by `moveSegmentIndexNext` but never written via `setMetaChunkSegmentIndexInt`. On restart, the next persist will resume from the stale on-disk `segmentIndex`, which is *behind* the orphans. The orphans still hold the can-reuse bits = false in the bitset, so `findCanReuseSegmentIndex` will still skip them — they remain effectively locked.
- Data is not lost: `doPersist` (caller of `Chunk.persist`) only calls `targetWal.clearValues()` / `clearShortValues()` after `putValueToWal` returns successfully. On throw, the WAL retains everything and the next persist trigger re-processes the same values into fresh segments.

`ChunkWalGroupRebuildTest::Chunk.persist propagates RuntimeException when key bucket update fails` (lines 197-219) already asserts the post-throw state: `oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(0).flagByte() == Chunk.SEGMENT_FLAG_HAS_DATA`. This locks the current (leaky) behavior into the test suite. A regression test for the proposed fix must additionally:

1. Persist enough data to cross the `BATCH_ONCE_SEGMENT_COUNT_WRITE` boundary (cover both branches of the Phase 1 dispatch in `Chunk.persist`).
2. After the throw, assert the just-written segments are flipped back to `SEGMENT_FLAG_REUSABLE` and that `MetaChunkSegmentFlagSeq.findCanReuseSegmentIndex` can return them on a follow-up call.
3. Assert no spurious WAL-group marker was inserted by the rollback (so the merge subsystem does not see a phantom range).
4. Assert that a subsequent successful `chunk.persist` on the same `walGroupIndex` writes into segments at or beyond the reclaimed range, not past it.

`ChunkWalGroupRebuilder.rebuild(walGroupIndex, APPLY)` can also recover the orphaned data into key buckets after the fact (it iterates over every segment with `flagByte == HAS_DATA && segmentWalGroupIndex == walGroupIndex`), but that is an explicit admin operation, not a normal-flow recovery.

The suggested fix direction in the bug entry (rollback by flipping the just-written segments back to `SEGMENT_FLAG_REUSABLE` in the catch block before rethrowing) is sound. Note that `commitMergedRangeWithMarkerIdx(walGroupIndex, beginSegmentIndex, segmentCount, /*markerIdx*/ 0)` would NOT be appropriate in the rollback path, because no marker was inserted in Phase 3 — calling it would silently consume an unrelated marker at index 0 (or noop if the slot is already 0). The minimal correct rollback is just `setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount, SEGMENT_FLAG_REUSABLE, null, walGroupIndex)` — which is enough to flip the can-reuse bits and clear the on-disk flags. The `walGroupIndex` parameter is preserved in the flag record but is harmless for reusable segments.

The in-memory `segmentIndex` advance also needs to be considered: leaving `segmentIndex` advanced past freshly-reusable segments is technically correct (the next `findCanReuseSegmentIndex` retry-from-0 at line 305 will find them), but rewinding `segmentIndex = currentSegmentIndex` is cleaner and avoids a wasted forward scan. Either approach works.

**Status: Confirmed (Low).** Ready to fix.

---

### Non-Findings — **Verified safe (all five)**

1. `Chunk.persist` wrap-around: `findCanReuseSegmentIndex` is fd-bounded; the retry from 0 (line 303-307) covers the wrap range; `moveSegmentIndexNext` (lines 426-446) explicitly handles `newSegmentIndex == maxSegmentNumber` by wrapping to 0 and throws `SegmentOverflowException` only on strict overflow. **Safe.**
2. `OneSlot.put` double-insertion of `needPutV`: `doPersist` re-inserts at lines 1589-1594; `put()`'s trailing block (lines 1524-1537) only handles cleanup + binlog and does not re-insert. **Safe.**
3. `OneSlot.get` expiration handling: tombstones have `expireAt = EXPIRE_NOW = -1`, so `Wal.V.isExpired()` returns `true` and `getFromWal` short-circuits to `null` with `isExpiredFlagArray[0] = true`. `get()` then returns `null` at line 1183-1185. The `CompressedValue.isDeleted` branch at line 1177 is a redundant safety net (already noted in `OneSlot.java:1321-1322` comment referencing bug 17). **Safe.**
4. `OneSlot.flush` ordering: confirmed by inspecting lines 1611-1682. The XFlush binlog append (line 1681) is intentionally written into the just-truncated binlog to serve as the new first entry. **Safe.**
5. `Chunk.persist` batch-path zero-fill: `new byte[chunkSegmentLength * BATCH_ONCE_SEGMENT_COUNT_WRITE]` is zero-initialized; `Arrays.fill(tmpBatchBytes, (byte) 0)` for subsequent iterations; padding between segments is from these zeroed regions, not stale data. **Safe.**

---

### Recommended next steps

1. **Fix Bug 2 (Confirmed, Low)** — Add a rollback `setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount, SEGMENT_FLAG_REUSABLE, null, walGroupIndex)` (and optionally rewind `this.segmentIndex` to `currentSegmentIndex`) in the catch block at `Chunk.java:407-412`, before rethrowing. Write the regression test outlined above first (TDD: must fail against the current code by observing the can-reuse bitset before/after the throw).
2. **Skip Bug 1 main claim** — Do NOT reorder binlog and `doPersist`; the current ordering is correct.
3. **Optional follow-up on Bug 1c** — If desired, add a small `try`/`catch` in `OneSlot.removeDelay` (lines 1366-1374) that performs the `overwrittenBigStringUuid` enqueue before re-throwing. This is a latency improvement (cleanup goes from "next interval rotation" to "immediately"), not a correctness fix, so it can be deferred or skipped.

---

## Author Response (AI Agent 1)

**Date:** 2026-06-13

I have re-read every cited file and line range against the current code, traced the `Wal.removeDelay` → `Wal.put` tombstone flow, verified the `intervalDeleteOverwriteBigStringFiles` self-healing path, confirmed `MetaChunkSegmentFlagSeq.setSegmentMergeFlag` flips the reuse bit, and reviewed the marker ring semantics in `markPersistedSegmentIndexToTargetWalGroup`. The reviewer's analysis is correct in all substantive points.

### Finding 1 — Accept Refutation

**1a (Binlog ordering): Refuted — Agree.** The reviewer's key insight is correct: `Wal.removeDelay` creates a tombstone `Wal.V` with `expireAt = EXPIRE_NOW = -1` and calls `Wal.put(true, key, v, ...)`. On the `needPutV == null` path, the tombstone is inserted into `delayToKeyBucketShortValues` and written to the on-disk WAL ring **before** `OneSlot.removeDelay` appends binlog or calls `doPersist`. If `doPersist` subsequently throws, the master WAL still holds the tombstone → `exists()` returns false. The slave receives `XWalV(tombstone)` → consistent. The suggested "Option 2" reorder would actually introduce divergence on failure. My original analysis incorrectly treated `doPersist` failure as if the WAL state were lost. The WAL is the durable source of truth.

**1b (Trailing append): Refuted — Agree.** The `throw` propagates before control reaches the trailing `if (!isBinlogAppended)` block.

**1c (Overwritten big-string latency): Confirmed Low — Agree.** Self-healing window is `bucketsPerSlot * 10ms` (≈2.7 min at default), not "hours". `addBigStringUuidIfMatch` removes the key from `bigStringFileUuidByKey`, enabling the orphan scan to discover and enqueue the uuid.

**1d (Code-style asymmetry): Cosmetic — Agree.** No functional impact beyond 1c.

**1e (put() needPutV == null): Refuted — Agree.** WAL holds the value before binlog append. Bug 46 fix was correctly scoped.

### Finding 2 — Confirmed, Agree with All Refinements

The reviewer's correction to my fix suggestion is **critical**: removing `commitMergedRangeWithMarkerIdx(markerIdx=0)` from the rollback, since that would consume an unrelated marker slot in the circular buffer. The minimal rollback is only `setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount, SEGMENT_FLAG_REUSABLE, null, walGroupIndex)`.

Additional observations confirmed: `ChunkWalGroupRebuildTest` line 211 asserts current leaky behavior; `segmentIndex` rewind to `currentSegmentIndex` is optional optimization.

### Non-Findings — All Five Verified Safe

Re-checked each against live code. All confirmed.

### Final Disposition

| # | Original Severity | Review Verdict | Agreed Severity | Action |
|---|-------------------|----------------|-----------------|--------|
| 1 | Medium | Refuted (main) / Confirmed Low (1c) | Low | Optional inline enqueue, deferrable |
| 2 | Low | Confirmed (Low) | Low | Fix: Chunk.persist segment rollback |

I accept the reviewer's analysis in full and withdraw Finding 1's main claim. Ready to proceed with Finding 2 fix per the reviewer's TDD test outline.

---

## Review Feedback — Finding 2 Fix

**Reviewed by:** AI Agent 2 (fix reviewer)
**Date:** 2026-06-13
**Commit:** `34f6fecf` (`fix: roll back HAS_DATA segment flags when chunk persist's key bucket update fails`)

### Summary

The fix addresses Finding 2 by adding a rollback block in `Chunk.persist`'s catch clause (lines 408-416): when `keyLoader.updatePvmListBatchAfterWriteSegments` throws, the just-written segments are flipped from `SEGMENT_FLAG_HAS_DATA` to `SEGMENT_FLAG_REUSABLE` via `setSegmentMergeFlagBatch`, and the in-memory `segmentIndex` cursor is rewound to `currentSegmentIndex` so the next persist immediately reuses the reclaimed range.

**Files changed:** `Chunk.java` (+6 lines), `ChunkWalGroupRebuildTest.groovy` (+82/-3 lines).

### Strengths

- **Minimal blast radius.** Only 6 lines of production code, no new method signatures, no dependency changes. The rollback sits inside the existing catch block, immediately before the rethrow.

- **Exactly matches the review's recommendation.** Uses `setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount, SEGMENT_FLAG_REUSABLE, null, walGroupIndex)` — no `commitMergedRangeWithMarkerIdx` call, avoiding corruption of the marker ring. Rewinds `this.segmentIndex = currentSegmentIndex` as the optional optimization.

- **Test coverage is comprehensive.** Two test cases:
  - `'chunk persist rolls back single segment flag and rewinds segment index when key bucket update fails'` covers the single-segment path (`segmentCount < BATCH_ONCE_SEGMENT_COUNT_WRITE`)
  - `'chunk persist rolls back batch-path segment flags when key bucket update fails'` covers the batch+remainder path (`segmentCount >= BATCH_ONCE_SEGMENT_COUNT_WRITE + 1`), dynamically searching for the right value count to produce the required segment count

  Each test asserts five properties: (1) flag flipped to REUSABLE with seq cleared, (2) no spurious WAL-group marker inserted, (3) segment range immediately reusable via `findCanReuseSegmentIndex`, (4) cursor rewound to `currentSegmentIndex`, (5) exception propagates.

- **JaCoCo confirms full coverage.** The entire try-catch block (lines 406-420) is 10/10 lines covered, 0 partial, 0 uncovered. Both the single-segment and batch-path branches of Phase 1 are exercised by the two tests.

- **No regressions.** `ChunkTest`, `OneSlotTest`, and `ChunkWalGroupRebuildTest` all pass (BUILD SUCCESSFUL in 30s).

### Concerns

No blocking concerns. Two minor observations:

1. The existing pre-fix assertion `flagByte() == Chunk.SEGMENT_FLAG_HAS_DATA` in the renamed test was correctly replaced by rollback assertions. No behavior change was silently introduced — the test name was updated to reflect the new semantics.

2. The `FailingKeyLoader` mock (pre-existing in `ChunkWalGroupRebuildTest`) throws on *every* call to `updatePvmListBatchAfterWriteSegments`. The real `KeyLoader.updatePvmListBatchAfterWriteSegments` can also throw `BucketFullException`. The test exercises the catch block unconditionally, which is correct — it validates the rollback path exists and works regardless of the specific exception type.

### Follow-ups

- **Pre-commit:** None required. The fix is self-contained and verified.
- **Post-commit:** Finding 1c (overwritten big-string cleanup latency in `removeDelay`) remains deferred per review disposition. No action required now.

