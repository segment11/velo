# Bug 22 Persist Layer Data Write Flow Review Round 4

Author: AI agent 1

## Scope

Static review of the persist-layer data **write** flow after Bug 18 (round 1), Bug 19, Bug 20 (round 2), and Bug 21 (round 3). This round digs deeper into:

- Error handling and data safety in `doPersist()` when `putValueToWal()` throws
- Segment space management during partial failures in `Chunk.persist()`
- Merge marker consumption before merge completion
- Big string file lifecycle during write failures
- Short value persist path's impact on segment reclamation

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`
- `doc/bug_reviews/bug_18_persist_layer_data_flow_review_round_1.md`
- `doc/bug_reviews/bug_20_persist_layer_data_flow_review_round_2.md`
- `doc/bug_reviews/bug_21_persist_layer_data_flow_review_round_3.md`

---

## Finding 1: Silent data loss of `needPutV` when `putValueToWal()` throws in `doPersist()`

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1953-1970` (`doPersist`)
- `src/main/java/io/velo/persist/Wal.java:954-1052` (`Wal.put`)

**Code flow:**

```java
// OneSlot.put() - line 1898-1910
var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);
// ... binlog appended at line 1907 ...
if (putResult.needPersist()) {
    doPersist(walGroupIndex, key, putResult);   // line 1910
}
```

```java
// Wal.put() - when buffer is full (lines 960-981)
if (offset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
    // ... rewrite attempt may reduce size ...
    if (needPersist) {
        return new PutResult(true, isValueShort, v, 0);  // line 970/981
    }
}
// NOTE: the early return above happens BEFORE the V is added to delayToKeyBucketValues (line 1025)
// or delayToKeyBucketShortValues (line 999). The V is also NOT written to the WAL file (putVToFile is at line 987).
```

```java
// doPersist() - line 1953-1970
public void doPersist(int walGroupIndex, @NotNull String key, @NotNull Wal.PutResult putResult) {
    var targetWal = walArray[walGroupIndex];
    putValueToWal(putResult.isValueShort(), targetWal);  // line 1955 - CAN THROW
    lastPersistTimeMs = System.currentTimeMillis();

    if (putResult.isValueShort()) {
        targetWal.clearShortValues();
    } else {
        targetWal.clearValues();
    }

    var needPutV = putResult.needPutV();   // line 1964
    if (needPutV != null) {
        var putResultAfterPersist = targetWal.put(putResult.isValueShort(), key, needPutV, lastPersistTimeMs);
        assert !putResultAfterPersist.needPersist();   // line 1968
    }
}
```

**Root cause:**

When `Wal.put()` returns `needPersist=true`, the triggering `V` is returned as `needPutV` but:

1. It is **never added** to `delayToKeyBucketValues` or `delayToKeyBucketShortValues` (the early return at Wal.java:970/981 happens before map insertion at lines 999/1025)
2. It is **never written** to the WAL file (`putVToFile()` is at line 987, after the early return)
3. The binlog **was already appended** at OneSlot.java:1907 with the original `v` (same object as `needPutV`)

If `putValueToWal()` (line 1955) throws — e.g., `SegmentOverflowException` from `Chunk.persist()` (Chunk.java:387) or `BucketFullException` from `KeyBucketsInOneWalGroup.putPvmListToTargetBucket()` (KeyBucketsInOneWalGroup.java:300) — the exception propagates upward and `needPutV` is permanently lost from both in-memory state and WAL file.

**Impact:**

- **Silent data loss** for the key that triggered the persist cycle
- The value exists in the binlog (if replication is enabled) but not in the local persist layer
- On restart, the key will not be found — it has no WAL entry, no key bucket entry, no segment data
- The client received a successful write response before `doPersist()` was called

**Suggested fix direction:**

Wrap `putValueToWal()` in a try-catch. On failure, re-insert `needPutV` into the WAL delay maps so the data is not lost. The simplest approach:

```java
public void doPersist(int walGroupIndex, @NotNull String key, @NotNull Wal.PutResult putResult) {
    var targetWal = walArray[walGroupIndex];
    try {
        putValueToWal(putResult.isValueShort(), targetWal);
    } catch (Exception e) {
        // Re-insert the deferred entry back into WAL delay maps to prevent data loss
        var needPutV = putResult.needPutV();
        if (needPutV != null) {
            if (putResult.isValueShort()) {
                targetWal.delayToKeyBucketShortValues.put(key, needPutV);
            } else {
                targetWal.delayToKeyBucketValues.put(key, needPutV);
            }
        }
        throw e;
    }
    // ... rest of method
}
```

**Regression tests should include:**

- Trigger a persist cycle where `Chunk.persist()` throws `SegmentOverflowException`
- Verify that `needPutV` is recovered and the key remains accessible
- Verify the WAL delay maps contain the deferred entry after the exception

---

## Finding 2: Key-bucket index inconsistency after partial failure in `Chunk.persist()`

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/Chunk.java:397-491`

**Code excerpt:**

```java
// Chunk.persist() - segments written and flags set (lines 397-458)
for (var segment : segments) {
    writeSegments(bytes, 1);
    oneSlot.setSegmentMergeFlag(targetSegmentIndex, SEGMENT_FLAG_HAS_DATA, segment.segmentSeq(), walGroupIndex);
    moveSegmentIndexNext(1);
}

// These lines may never be reached if keyLoader.updatePvmListBatchAfterWriteSegments() throws:
keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, keyBucketsInOneWalGroupGiven); // line 483
metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segmentCount); // line 487
oneSlot.setMetaChunkSegmentIndexInt(segmentIndex); // line 490
```

**Root cause:**

`Chunk.persist()` writes segment bytes to disk and sets `SEGMENT_FLAG_HAS_DATA` flags (lines 397-458) **before** calling `keyLoader.updatePvmListBatchAfterWriteSegments()` (line 483). If the key-bucket update throws (e.g., `BucketFullException`), the following occurs:

1. Segments have `SEGMENT_FLAG_HAS_DATA` in the flag bitset and can be found by chunk-file recovery
2. `markPersistedSegmentIndexToTargetWalGroup()` (line 487) is never called, so the merge tracker never finds these segments for future merge-read
3. `setMetaChunkSegmentIndexInt()` (line 490) is never called, so the segment index is not advanced in metadata
4. These segments are not referenced by the current key buckets, because key-bucket metadata update failed

**Impact:**

- Current key buckets can be inconsistent with chunk data after a partial failure
- Normal reads through key buckets may miss data that was written to chunk files
- Segments remain `HAS_DATA`, which is useful for recovery but can delay space reclamation until a rebuild/merge path runs
- Repeated failures can still reduce available segment space, but the written bytes are recoverable if chunk scan is used

**Suggested fix direction:**

Do **not** blindly reset the just-written `HAS_DATA` flags to `SEGMENT_FLAG_REUSABLE` if segment bytes were written
successfully. In the intended data-broken recovery flow, chunk files are the recoverable persisted data and key buckets
are a rebuildable index. Resetting the flags would hide recoverable data from a full chunk scan.

The preferred recovery direction is:

1. Treat chunk files / `HAS_DATA` segment flags as the source for persisted normal-value recovery.
2. Re-read chunk files and group decoded records by WAL group, so memory usage is bounded by one WAL group's keys.
3. Within each WAL group, resolve duplicate records by key using the latest sequence, and apply delete/expire filtering.
4. Rebuild key buckets from the resolved live records.
5. Merge/compact the WAL group's live records and mark stale/duplicate/invalid segment ranges reusable only after rebuild
   succeeds.
6. Replay WAL files after chunk rebuild, because WAL contains newer unpersisted writes.

Recovery must validate segment headers/records; segments with incomplete or invalid bytes should be skipped or marked
reusable according to the corruption policy. Valid `HAS_DATA` segments should remain scannable until rebuild/merge has
made a new consistent index.

**Regression tests should include:**

- Force `BucketFullException` during `updatePvmListBatchAfterWriteSegments()`
- Verify that segments written before the exception remain `HAS_DATA` and are visible to the rebuild scanner
- Verify that the rebuild path can reconstruct key buckets by WAL group from chunk data
- Verify that stale/duplicate records are removed only after successful rebuild/merge

---

## Finding 3: Merge marker consumed before merge completes — lost merge opportunity

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:477-527` (`findThoseNeedToMerge`)
- `src/main/java/io/velo/persist/OneSlot.java:2216-2248` (`readSomeSegmentsBeforePersistWal`)

**Code excerpt:**

```java
// MetaChunkSegmentFlagSeq.findThoseNeedToMerge() - line 494
markedLongs[i] = 0L;   // consumed immediately
return new int[]{segmentIndex, segmentCount};
```

```java
// OneSlot.putValueToWal() - lines 2280-2323
var ext = readSomeSegmentsBeforePersistWal(walGroupIndex);
// ... merge processing ...
// If all merged CVs are expired/updated, list only contains delayToKeyBucketValues
// chunk.persist() is still called with those values
// But the old segments are marked SEGMENT_FLAG_REUSABLE (line 2337-2338)
```

**Root cause:**

`findThoseNeedToMerge()` clears the marker slot (`markedLongs[i] = 0L`) at line 494/521 **before** the caller has successfully completed the merge. The marker is consumed regardless of whether the merge succeeds.

If `readSomeSegmentsBeforePersistWal()` returns segments and the merge reads them, but then `chunk.persist()` throws (e.g., `SegmentOverflowException` at Chunk.java:387), the old segments may not be marked as `SEGMENT_FLAG_REUSABLE` (because line 2337 is never reached). The merge marker is already gone, so these segments:

1. Still have `SEGMENT_FLAG_HAS_DATA` flag
2. Have no merge marker to trigger a future merge attempt
3. Will only be reclaimed when the segment index wraps around and `findCanReuseSegmentIndex()` encounters them

**Impact:**

- Lost merge opportunity for segments that were targeted for merge but whose merge failed
- These segments remain as `HAS_DATA` until the segment index wraps, delaying space reclamation
- With high write throughput, this can accumulate segments that should have been merged

**Suggested fix direction:**

Defer marker consumption until after the merge completes successfully. Options:

1. Add a `commitMerge(walGroupIndex, segmentIndex)` method that clears the marker, called only after successful merge
2. Or re-mark the segments for merge if `chunk.persist()` fails

Option 1 is cleaner but requires changing the `findThoseNeedToMerge` / `readSomeSegmentsBeforePersistWal` contract.

---

## Finding 4: Big string file orphaned if `doPersist()` fails after file write

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1857-1878` (big string write)
- `src/main/java/io/velo/persist/OneSlot.java:1898-1910` (WAL put + doPersist)
- `src/main/java/io/velo/persist/BigStringFiles.java` (cleanup)

**Code flow:**

```java
// OneSlot.put() - lines 1857-1878
var bigStringUuid = snowFlake.nextId();
var bytes = cv.getCompressedData();
var isWriteOk = bigStringFiles.writeBigStringBytes(bigStringUuid, bucketIndex, cv.getKeyHash(), bytes); // line 1860
if (!isWriteOk) {
    throw new RuntimeException("Write big string file error...");
}

// ... creates cvAsBigString, appends to binlog ...
var v = new Wal.V(..., cvBigStringEncoded, isFromMerge);  // line 1876
isValueShort = true;  // line 1879

// Later:
var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);  // line 1898
if (putResult.needPersist()) {
    doPersist(walGroupIndex, key, putResult);  // line 1910 - CAN THROW
}
```

**Root cause:**

The big string file is written to disk (line 1860) **before** the WAL entry is created and before persist. The write order is:

1. Big string file written to disk (irreversible)
2. Binlog entry appended (line 1874)
3. `Wal.put()` called (line 1898) — may return `needPutV` if buffer full
4. `doPersist()` called (line 1910) — may throw

If step 4 throws (combining with Finding 1), the `needPutV` containing the big string UUID reference is lost. The big string file on disk has no WAL or key bucket reference.

**Impact:**

- Orphan big string file that is never referenced and never cleaned up
- `intervalDeleteOverwriteBigStringFiles()` only deletes files registered in `delayToDeleteBigStringFileIds`, which requires the overwritten UUID to be tracked — but this orphan was never "overwritten," just never persisted
- The file occupies disk space indefinitely
- Combined with Finding 1, this is a disk-space leak in addition to data loss

**Suggested fix direction:**

Either:
1. Defer the big string file write until after `Wal.put()` succeeds (requires restructuring the flow)
2. Or register the new big string UUID in a cleanup set before writing, so orphans can be detected and cleaned up

Option 2 is safer as a defensive measure. The existing `intervalDeleteOverwriteBigStringFiles()` could be extended to scan for unreferenced big string files.

---

## Finding 5: `encodeAfterPutBatch()` uses `listList.size()` instead of `maxSplitNumberTmp` for iteration

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:162-210`

**Code excerpt:**

```java
byte[][] encodeAfterPutBatch() {
    byte maxSplitNumberTmp = 1;
    for (int i = 0; i < oneChargeBucketNumber; i++) {
        if (splitNumberTmp[i] > maxSplitNumberTmp) {
            maxSplitNumberTmp = splitNumberTmp[i];
        }
    }

    var sharedBytesList = new byte[maxSplitNumberTmp][];

    for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {   // line 172 - uses listList.size()
        var list = listList.get(splitIndex);
        // ... encode key buckets ...
    }
    return sharedBytesList;
}
```

**Observation:**

The method computes `maxSplitNumberTmp` from `splitNumberTmp[]` but then iterates `listList.size()` instead of `maxSplitNumberTmp`. If `listList` was expanded (e.g., by `putPvmListToTargetBucket()` adding new split levels), `listList.size()` could be larger than `maxSplitNumberTmp`, causing `sharedBytesList` to be undersized and an `ArrayIndexOutOfBoundsException` at line 207.

However, `putPvmListToTargetBucket()` updates both `splitNumberTmp[]` AND `listList` together (lines 306-316), so after `putPvmListToTargetBucket()` runs, `maxSplitNumberTmp` should match the max of `splitNumberTmp[]`, which should equal `listList.size()`.

**Impact:**

This is currently safe because `listList.size()` always equals the max of `splitNumberTmp[]` after all buckets are processed. But it is fragile — if a future change adds a split level to `listList` without updating `splitNumberTmp[]`, the `sharedBytesList` allocation would be too small.

**Suggested fix direction:**

Use `maxSplitNumberTmp` consistently for both allocation and iteration, or use `listList.size()` for both:

```java
var sharedBytesList = new byte[listList.size()][];
for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
    // ...
}
```

---

## Finding 6: Short value persist path skips segment merge — old segments never reclaimed

**Severity:** Low / Informational

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2266-2271`

**Code excerpt:**

```java
void putValueToWal(boolean isShortValue, @NotNull Wal targetWal) {
    var walGroupIndex = targetWal.groupIndex;
    if (isShortValue) {
        keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, targetWal.delayToKeyBucketShortValues.values());
        return;   // returns immediately, no merge step
    }

    var ext = readSomeSegmentsBeforePersistWal(walGroupIndex);  // only for long values
    // ...
}
```

**Root cause:**

When `isShortValue` is true, `putValueToWal()` returns immediately after persisting short values to key buckets. The merge step (`readSomeSegmentsBeforePersistWal`) is never invoked. If a workload transitions from long values to exclusively short values, old chunk segments for this WAL group will never be merged or reclaimed.

**Impact:**

- Old segments with valid data remain as `HAS_DATA` indefinitely if only short values are written to this WAL group
- These segments consume space and are only reclaimed when the segment index wraps
- This is partly by design (short values don't go to chunks), but the merge-reclamation mechanism is bypassed

**Suggested fix direction:**

Consider triggering segment merge-check even during short-value persist cycles, at least periodically or when the merge marker buffer has pending entries for the WAL group.

---

## Non-Findings (Already Addressed or By Design)

1. **Expired entries reinserted during merge (Bug 18, Finding 1)** — Fixed in commit `d9747b7`. The `return` after `cvExpiredOrDeleted()` in the iterate callback at line 239 prevents expired existing entries from being reinserted.

2. **Split decision ignores multi-cell cost (Bug 18, Finding 2)** — Fixed in commits `d65467e` and `a04e528`. Cell cost is now correctly computed and split is rechecked after increasing split number.

3. **Expired WAL entries encoded to segments (Bug 21, Finding 1)** — Confirmed as low-severity optimization. Expired entries are filtered from key buckets but segment bytes are still written.

4. **WAL entry seq ordering** — `Wal.put()` uses `HashMap` for `delayToKeyBucketValues`, which does not preserve insertion order. However, since `putValueToWal()` collects all values and sorts by bucket index before persist, the final segment layout is deterministic regardless of map iteration order.

5. **Thread safety** — `checkCurrentThreadId()` at `OneSlot.put()` line 1813 ensures single-threaded access per slot worker. The WAL in-memory HashMaps are not synchronized but are protected by this invariant.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Silent data loss of `needPutV` when `putValueToWal()` throws | **High** | **Fixed** (commits `dded848`, `8da172f`, `0ef0214`, `7027130`, `6ff9728`, `6c36c98`) | High |
| 2 - Key-bucket index inconsistency after partial `Chunk.persist()` failure | Medium | **By design — use chunk recovery/rebuild data flow** | High |
| 3 - Merge marker consumed before merge completes | Medium | **Fixed** (commits `6b35f90`, `59ce705`, `313978d`, `9a90e09`) | Medium |
| 4 - Big string file orphaned if persist fails after file write | Medium | **Fixed** — addressed by Bug 22 #1 fix (`dded848`): `needPutV` is now recovered into WAL delay maps when `doPersist()` throws, so the new big string file retains its WAL reference after recovery | High |
| 5 - `encodeAfterPutBatch()` inconsistent iteration bound | Low | **Fragile but safe** | Medium |
| 6 - Short value persist skips segment merge | Low / Informational | **By design** | High |

## Reviewer Notes

### AI Agent 2 Review

Review date: 2026-05-06

#### Finding 1 - Silent data loss of `needPutV` when `putValueToWal()` throws

**Status:** Confirmed as a high-severity failure-atomicity / binlog-consistency bug, with one correction.

Verified current code:

- `Wal.put(...)` can return `new PutResult(true, isValueShort, v, 0)` before writing `v` to the WAL file or adding it
  to `delayToKeyBucketValues` / `delayToKeyBucketShortValues`
  (`src/main/java/io/velo/persist/Wal.java:954-987`, `src/main/java/io/velo/persist/Wal.java:999-1025`).
- `OneSlot.put(...)` appends `XWalV` to binlog before calling `doPersist(...)`
  (`src/main/java/io/velo/persist/OneSlot.java:1898-1910`).
- `doPersist(...)` calls `putValueToWal(...)` before reinserting `putResult.needPutV()`
  (`src/main/java/io/velo/persist/OneSlot.java:1953-1969`).

Correction:

The document says "The client received a successful write response before `doPersist()` was called." For the normal
request path, this is not accurate: `RequestHandler` catches the exception and returns an error for the SET shortcut and
generic command path (`src/main/java/io/velo/RequestHandler.java:492-502`,
`src/main/java/io/velo/RequestHandler.java:516-529`).

The bug is still serious because the binlog append has already happened, while the local WAL/map insertion of
`needPutV` has not. A failed local write can therefore leave binlog state ahead of local persistent state, and the
deferred value is dropped unless the client retries successfully.

Reviewer recommendation:

Fix is needed. The fix should preserve failure atomicity for `needPutV` and should also revisit binlog append ordering or
rollback behavior so failed local writes are not replicated as if they succeeded.

#### Finding 2 - Key-bucket index inconsistency after partial failure in `Chunk.persist()`

**Status:** Confirmed, with revised fix direction.

Verified current code:

- `Chunk.persist(...)` writes segment bytes, sets `SEGMENT_FLAG_HAS_DATA`, and advances in-memory `segmentIndex` before
  key-bucket metadata is updated (`src/main/java/io/velo/persist/Chunk.java:397-458`).
- `keyLoader.updatePvmListBatchAfterWriteSegments(...)` can throw after those segment writes
  (`src/main/java/io/velo/persist/Chunk.java:482-483`).
- `markPersistedSegmentIndexToTargetWalGroup(...)` and `setMetaChunkSegmentIndexInt(...)` run only after the key-bucket
  update succeeds (`src/main/java/io/velo/persist/Chunk.java:487-490`).

Reviewer conclusion:

If key-bucket update fails after segment writes, the written segments remain marked as data but are not referenced by key
buckets and are not added to the merge marker queue. This is a real index-consistency/recovery issue, but the segment
bytes are still recoverable data if the recovery path scans chunk files.

Do not treat immediate rollback to `SEGMENT_FLAG_REUSABLE` as the default fix. The intended recovery model is that key
buckets are a rebuildable index, while valid chunk records can be re-read, grouped by WAL group, resolved by latest
sequence per key, merged/compacted, and used to rebuild key buckets. Keeping valid `HAS_DATA` flags lets this recovery
flow discover the written data. Fix is needed in the chunk-scan rebuild / merge-reconstruction path rather than by
blindly clearing flags after key-bucket update failure.

#### Finding 3 - Merge marker consumed before merge completes

**Status:** Confirmed.

Verified current code:

- `MetaChunkSegmentFlagSeq.findThoseNeedToMerge(...)` clears marker slots before returning the segment range to the
  caller (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:477-522`).
- `OneSlot.putValueToWal(...)` marks the old merge range reusable only after `chunk.persist(...)` returns
  (`src/main/java/io/velo/persist/OneSlot.java:2280-2338`).

Reviewer conclusion:

If a merge range is consumed and a later persist step throws, the old segments can keep `HAS_DATA` flags while losing the
marker that would schedule a future merge attempt. The suggested fix direction is valid: either defer marker consumption
until successful completion or re-mark the range on failure.

#### Finding 4 - Big string file orphaned if `doPersist()` fails after file write

**Status:** Partially confirmed; impact is lower than stated because existing periodic cleanup can discover unreferenced
files.

Verified current code:

- Big string bytes are written before `Wal.put(...)` and before `doPersist(...)`
  (`src/main/java/io/velo/persist/OneSlot.java:1857-1879`).
- The big-string metadata binlog (`XBigStrings`) and WAL binlog (`XWalV`) are appended before `doPersist(...)` completes
  (`src/main/java/io/velo/persist/OneSlot.java:1872-1874`, `src/main/java/io/velo/persist/OneSlot.java:1906-1910`).
- If `Wal.put(...)` returns early with `needPutV`, `Wal.addBigStringUuidIfMatch(...)` is not called, so
  `bigStringFileUuidByKey` does not track the new file (`src/main/java/io/velo/persist/Wal.java:970-981`,
  `src/main/java/io/velo/persist/Wal.java:1020-1048`).
- `intervalDeleteOverwriteBigStringFiles(...)` does scan bucket files and can enqueue files that are absent from both
  key-bucket metadata and WAL big-string UUID state (`src/main/java/io/velo/persist/OneSlot.java:1248-1289`).

Reviewer conclusion:

The write-failure window is real: a big-string file can be left without a live local WAL/key-bucket reference if
`doPersist()` fails. The claim that it is "never cleaned up" is too strong; the periodic cleanup path can eventually
find unreferenced big-string files. Immediate cleanup/rollback is still desirable, especially because the binlog has
already recorded the big-string data before local persistence succeeds.

#### Finding 5 - `encodeAfterPutBatch()` inconsistent iteration bound

**Status:** Refuted as a current bug; accepted as minor maintainability cleanup.

Verified current code:

- `readBeforePutBatch()` sizes `listList` from the maximum value in `splitNumberTmp`
  (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:66-84`).
- Split expansion updates `listList` and `splitNumberTmp` together
  (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:306-312`).
- `encodeAfterPutBatch()` computes `maxSplitNumberTmp` from the same `splitNumberTmp` state before allocating
  `sharedBytesList` (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:162-172`).

Reviewer conclusion:

Under current invariants, `listList.size()` and the maximum split number stay aligned. This is not a bug today. Changing
allocation to `new byte[listList.size()][]` would be reasonable cleanup, but it is not required.

#### Finding 6 - Short value persist skips segment merge

**Status:** Confirmed by design / informational, not a correctness bug.

Verified current code:

- The short-value path persists key buckets and returns before `readSomeSegmentsBeforePersistWal(...)`
  (`src/main/java/io/velo/persist/OneSlot.java:2266-2270`).
- The only production call to `readSomeSegmentsBeforePersistWal(...)` is in the normal-value path
  (`src/main/java/io/velo/persist/OneSlot.java:2280`).

Reviewer conclusion:

Short-only write workloads can delay chunk segment reclamation for old normal values in that WAL group. This does not
break reads or writes for short values, and if normal writes resume the normal merge path is available again. Treat this
as an operational/space-reclamation tradeoff, not a required bug fix.

#### AI Agent 2 Summary

| Finding | AI Agent 2 Status | Fix Needed |
|---------|-------------------|------------|
| 1 - `needPutV` dropped on persist failure | Confirmed with corrected impact | Yes |
| 2 - Key-bucket index inconsistency after partial `Chunk.persist()` failure | Confirmed with revised recovery direction | Yes |
| 3 - Merge marker consumed before successful merge | Confirmed | Yes / design change |
| 4 - Big-string orphan on failed persist | Partially confirmed | Defensive cleanup recommended |
| 5 - `encodeAfterPutBatch()` bound mismatch | Refuted as current bug | No |
| 6 - Short-value path skips merge | Confirmed by design | No |

The required fixes from this round are Findings 1 and 2. Finding 3 should be fixed as part of the same failure-atomicity
work if merge ranges are involved. Finding 4 is best handled by the same rollback/cleanup design rather than as a
standalone correctness bug.

---

## Review Feedback - Bug 1 Committed Fix `f25513c`

Reviewed by: AI agent 1
Date: 2026-05-06

### Summary of the fix

Commit `f25513c` wraps `putValueToWal()` in a try-catch in `doPersist()`. On failure, the deferred `needPutV` (the WAL entry that triggered the persist cycle but was never added to the delay maps) is re-inserted into the appropriate WAL delay map (`delayToKeyBucketValues` or `delayToKeyBucketShortValues`) before re-throwing the exception.

The commit also adds `OneSlotTest.test needPutV recovered when doPersist throws`, which:
1. Sets `segmentNumberPerFd = 8` and `fdPerChunk = 1` to create a tiny segment pool
2. Does 50 successful writes to fill segments
3. Marks all segments as non-reusable (HAS_DATA) and clears the reuse bitset
4. Continues writing until WAL buffer overflow triggers `needPutV != null`
5. Verifies `SegmentOverflowException` is thrown and `needPutV` key is recovered in the delay map

### Strengths

- The production change targets the confirmed root cause directly with minimal code
- The test exercises the exact failure path: WAL buffer overflow → `needPutV` → persist fails → recovery
- All existing `OneSlotTest` tests pass with the change
- JaCoCo confirms the catch block and the normal-value recovery branch are covered

### Findings

No blocking issues found in the committed Bug 1 fix.

### Residual Concern

**Low: short-value `needPutV` recovery branch not tested.**

The test only triggers the normal-value path (`!isValueShort`). The short-value recovery branch (`targetWal.delayToKeyBucketShortValues.put(key, needPutV)`) at line 1961 is not covered by JaCoCo. This is acceptable for now because the short-value path follows the same pattern and the fix is symmetric. A follow-up could add a short-value-specific test if desired.

### Follow-ups

1. Treat Bug 1 as fixed.
2. Bug 2 (segment space leak on partial `Chunk.persist()` failure) remains open.
3. Bug 3 (merge marker consumed before merge completes) remains open.
4. Bug 4 (big string file orphan) remains open.

---

## Follow-up Review Feedback - Bug 1 Commit `f25513c`

Reviewed by: AI agent 1
Date: 2026-05-06

### Findings

#### Blocking: recovery is memory-only and does not restore WAL-file durability

Commit `f25513c` puts `needPutV` back into the in-memory delay map when `putValueToWal()` throws, but it does not write
that `Wal.V` to the WAL file or advance the WAL write position. The original failure-atomicity concern included
restart/binlog consistency: the triggering value was not in the WAL file when `Wal.put(...)` returned early, while
`XWalV` had already been appended before `doPersist(...)`.

Verified code:

- Early `Wal.put(...)` returns at `src/main/java/io/velo/persist/Wal.java:970` / `981` before `putVToFile(...)`
  (`src/main/java/io/velo/persist/Wal.java:987`).
- The fix inserts only into `delayToKeyBucketValues` or `delayToKeyBucketShortValues`
  (`src/main/java/io/velo/persist/OneSlot.java:1957-1964`).
- No `putVToFile(...)`, write-position update, or equivalent durable WAL rewrite happens in the catch block.

This improves retry behavior while the process remains alive, but if the persist failure is severe enough that the server
is restarted, the recovered `needPutV` is still absent from the WAL file. The binlog/local-durability divergence remains.

#### Blocking: catch-path insertion does not mirror `Wal.put(...)` side effects

The catch block writes directly to one delay map, but normal `Wal.put(...)` also removes the key from the opposite delay
map and updates big-string UUID tracking:

- short value success path: `delayToKeyBucketShortValues.put(...)`, `delayToKeyBucketValues.remove(...)`,
  `addBigStringUuidIfMatch(...)` (`src/main/java/io/velo/persist/Wal.java:999-1020`);
- normal value success path: `delayToKeyBucketValues.put(...)`, `delayToKeyBucketShortValues.remove(...)`,
  `bigStringFileUuidByKey.remove(...)` (`src/main/java/io/velo/persist/Wal.java:1025-1048`).

The fix only does:

```java
targetWal.delayToKeyBucketShortValues.put(key, needPutV);
// or
targetWal.delayToKeyBucketValues.put(key, needPutV);
```

This can leave an older value for the same key in the opposite WAL map after a failed type/size transition. For recovered
big-string short values, `bigStringFileUuidByKey` is also not updated, so the periodic big-string cleanup can see the file
as unreferenced by WAL state.

### Verification

Command run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test needPutV recovered when doPersist throws"
```

Result: passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo inspection:

- `OneSlot.java:1957-1958`, `1963`, and `1966` are covered.
- `OneSlot.java:1961` short-value recovery branch is not covered.
- The test verifies in-memory map recovery for normal values only; it does not verify WAL-file durability, binlog
  consistency, opposite-map cleanup, or big-string UUID tracking.

### Review Conclusion

`f25513c` is a useful partial fix, but Bug 1 should not be considered fully fixed yet. A complete fix should recover
`needPutV` through the same bookkeeping path as `Wal.put(...)` and should address durability/binlog atomicity for the
failure case.

---

## Revised Fix - Commit `ccbfcb1`

Reviewed by: AI agent 1
Date: 2026-05-06

### Summary

Commit `ccbfcb1` (amended from `f25513c`) addresses both blocking findings from the review:

1. **WAL-file durability**: `Wal.recoverNeedPutV()` attempts `rewriteOneGroup()` to compact the WAL buffer, then writes
   `needPutV` to the WAL file via `putVToFile()` if space is available after compaction. If the buffer remains full after
   compaction, the entry is still recovered in-memory (best-effort durability).

2. **Full bookkeeping**: `recoverNeedPutV()` mirrors all `Wal.put()` side effects:
   - Inserts into the correct delay map and removes from the opposite map
   - Updates the sequence counter (`lastSeqAfterPut` / `lastSeqShortValueAfterPut`)
   - Calls `addBigStringUuidIfMatch()` for short values or `bigStringFileUuidByKey.remove()` for normal values

The `doPersist()` catch block now delegates to `targetWal.recoverNeedPutV()` instead of doing a raw map insertion.

### Verification

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest"
./gradlew :cleanTest :test --tests "io.velo.persist.WalTest"
```

All tests pass. JaCoCo confirms:
- `OneSlot.java:1957-1963` catch block and delegation to `recoverNeedPutV` fully covered
- `Wal.java:1089-1092` normal-value bookkeeping path fully covered (map put, opposite remove, UUID remove, seq update)
- `Wal.java:1069-1070` rewrite attempt path covered
- Short-value branch and file-write path not covered by this test (buffer remained full after rewrite)

---

## Follow-up Review Feedback - Bug 1 Commits `dded848` / `27e856e`

Reviewed by: AI agent 1
Date: 2026-05-07

### Findings

#### Blocking: original overflow/failure path can still recover `needPutV` only in memory

The revised fix moved the recovery logic into `Wal.recoverNeedPutV(...)`, and that correctly mirrors the normal
`Wal.put(...)` map bookkeeping. However, the original failure path can still skip the WAL-file write and then insert
the deferred value into the delay map anyway.

Verified code:

- `OneSlot.doPersist(...)` catches `putValueToWal(...)` failures and delegates to
  `targetWal.recoverNeedPutV(...)` (`src/main/java/io/velo/persist/OneSlot.java:1957-1962`).
- `Wal.recoverNeedPutV(...)` attempts rewrite only if the buffer is full, then writes the recovered value to the WAL
  file only if the encoded value fits (`src/main/java/io/velo/persist/Wal.java:1058-1069`).
- If it still does not fit, the code logs `Recover needPutV skipped WAL file write...` and still inserts the value into
  the in-memory delay map (`src/main/java/io/velo/persist/Wal.java:1070-1085`).

The focused regression test for the real `doPersist()` failure path hit this exact branch:

```text
WARN [io.velo.persist.Wal] - Recover needPutV skipped WAL file write, buffer still full after rewrite, slot=0, group=0, key=xh!0_key:000000000076
```

So the current fix prevents live-process map loss, but it does not fully close the restart/binlog/local-durability part
of Bug 1. If the persist failure makes the server unhealthy and it restarts before the recovered delay map can be
persisted, the `needPutV` still has no WAL-file record even though the binlog entry was appended before `doPersist()`.

This is a correctness issue if Bug 1's intended fix includes crash/restart durability. If the intended contract is only
"preserve the value in memory while the process remains alive", then the bug review should explicitly narrow the impact,
and the test should assert that skipped WAL-file recovery is acceptable.

#### Non-blocking: full-buffer test does not assert the durable recovery property

`WalTest.test recoverNeedPutV when buffer full uses rewrite` sets `wal.writePosition = Wal.ONE_GROUP_BUFFER_SIZE`,
calls `recoverNeedPutV(...)`, and asserts only:

- the recovered value is in `delayToKeyBucketValues`;
- `lastSeqAfterPut` is updated.

It does not assert that `writePosition` advances after rewrite, that no skip warning occurs, or that the value can be
loaded back from the WAL file. This allowed the same memory-only recovery behavior to pass under the test name
"uses rewrite".

### Fixed From Prior Review

- The opposite-map cleanup gap is fixed: normal recovery removes the short-value map entry, and short recovery removes
  the normal-value map entry.
- Big-string UUID bookkeeping is fixed for both directions: short recovered big strings call `addBigStringUuidIfMatch`,
  and normal recovered values remove the old UUID mapping.
- Sequence counters are updated for recovered normal and short values.
- New `WalTest` coverage exercises normal recovery, short recovery, and big-string UUID tracking.

### Verification

Command run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.WalTest.test recoverNeedPutV*" --tests "io.velo.persist.OneSlotTest.test needPutV recovered when doPersist throws"
```

Result: passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo inspection:

- `OneSlot.java:1960` recovery delegation is covered.
- `Wal.java:1058-1072` covers both the rewrite attempt and the skipped WAL-file write branch.
- `Wal.java:1064-1068` WAL-file write path is covered by direct `WalTest` buffer-space tests.
- `Wal.java:1075-1085` covers both normal and short map-bookkeeping branches.

### Review Conclusion

`dded848` plus `27e856e` fixes the bookkeeping problems found in the previous review, but Bug 1 should not be considered
fully fixed if the required behavior is restart-durable recovery of the deferred `needPutV`. The current regression test
demonstrates that the real overflow/persist-failure path can still take a memory-only recovery branch.

### Design Decision - Fail the Write Instead of Recovering `needPutV`

Decision date: 2026-05-07

Agreed direction: keep the failure handling simple. If `putValueToWal(...)` throws, the write is not accepted because the
slot cannot currently make write progress. In that case, `needPutV` should not be recovered into WAL delay maps and
should not be written to binlog.

The intended data flow for a `Wal.put(...)` result with `needPersist=true` and `needPutV != null` is:

1. Defer binlog append until the local write is accepted.
2. Call `doPersist(...)`.
3. If `putValueToWal(...)` succeeds, clear the persisted WAL map, put `needPutV` into WAL normally, then append binlog
   for the accepted write.
4. If `putValueToWal(...)` throws, propagate the exception so the client receives an error; do not recover `needPutV`
   and do not append binlog for that failed write.

This avoids the inconsistent middle state where the client receives an error but the value exists only in local memory
or has already been replicated through binlog. The accepted-write invariant should be:

> A write is accepted only after it is either durable in the WAL file or persisted to key buckets/chunk. If neither is
> possible, the write fails and must not be represented in binlog as accepted.

This decision supersedes the recovery-based fix direction for Bug 1. A future fix should focus on binlog ordering and
write acceptance semantics rather than best-effort recovery of `needPutV` after `putValueToWal(...)` fails.

---

## Review Feedback - Bug 1 Commit `8da172f`

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary of the fix

Commit `8da172f` implements the agreed simpler direction for the main `OneSlot.put(...)` path:

- `appendBinlog(new XWalV(...))` is moved after `doPersist(...)`, so the `XWalV` for the failed `needPutV` path is not
  appended when `putValueToWal(...)` throws.
- `Wal.recoverNeedPutV(...)` and the `doPersist(...)` catch/recover block are removed.
- The regression test is rewritten to assert that the failed key is not recovered into WAL delay maps.

### Findings

#### Blocking: `removeDelay(...)` still appends `XWalV` before `doPersist(...)`

`OneSlot.removeDelay(...)` still has the old ordering:

```java
var putResult = targetWal.removeDelay(key, bucketIndex, keyHash, lastPersistTimeMs);
var xWalV = new XWalV(putResult.needPutV(), putResult.isValueShort());
appendBinlog(xWalV);
if (putResult.needPersist()) {
    doPersist(walGroupIndex, key, putResult);
}
```

`Wal.removeDelay(...)` delegates to `Wal.put(true, ...)`, so it can return `needPersist=true` with a deferred
`needPutV` when the short-value WAL buffer is full. If `doPersist(...)` then throws, the delete marker has already been
written to binlog while the local failed write is not accepted. This keeps the original Bug 1 binlog/local ordering bug
alive for delayed removes.

Relevant code:

- `src/main/java/io/velo/persist/OneSlot.java:1750-1756`
- `src/main/java/io/velo/persist/Wal.java:762-768`
- `src/main/java/io/velo/persist/Wal.java:949-971`

#### Blocking: big-string writes still append `XBigStrings` before local write acceptance

The main `XWalV` append moved after `doPersist(...)`, but the big-string side binlog remains before `Wal.put(...)` and
before `doPersist(...)`:

```java
var xBigStrings = new XBigStrings(bigStringUuid, bucketIndex, cv.getKeyHash(), key, cvBigStringEncoded);
appendBinlog(xBigStrings);
```

If a big-string write later reaches `putResult.needPersist()` and `putValueToWal(...)` throws, the write is rejected by
the master but an `XBigStrings` binlog entry has already been emitted. This is not harmless metadata: `XBigStrings.apply`
decodes the compressed value and calls `oneSlot.put(...)` on the slave, then queues the big-string fetch. That can make a
slave observe a write that the master rejected.

Relevant code:

- `src/main/java/io/velo/persist/OneSlot.java:1872-1876`
- `src/main/java/io/velo/repl/incremental/XBigStrings.java:187-200`

#### Blocking: deferring every `XWalV` until after `doPersist(...)` can drop binlog for already-WAL-durable writes

The new ordering is correct for the `needPutV != null` overflow case, because that value has not been written to the WAL
file yet. But it is too broad for `needPersist=true, needPutV == null` cases. In those cases `Wal.put(...)` has already:

- written the current value to the WAL file via `putVToFile(...)`;
- advanced the WAL write position;
- inserted the value into the delay map;
- then returned `needPersist=true` because the WAL map reached a persist threshold.

If `doPersist(...)` throws after that, commit `8da172f` skips `XWalV` append even though the value is already durable in
the local WAL and may be visible/recovered locally. That creates local/binlog divergence in the opposite direction:
master WAL has the value, but replication binlog does not.

Relevant code:

- `src/main/java/io/velo/persist/Wal.java:975-1041`
- `src/main/java/io/velo/persist/OneSlot.java:1898-1911`

A narrower ordering is needed: defer binlog only for the `putResult.needPutV() != null` path, or otherwise roll back the
already-written WAL entry when treating the write as failed. Rolling back WAL is much more complex.

#### Test gap: the new binlog assertion is currently vacuous

The new regression test records `oneSlot.binlog.currentReplOffset()` before and after the failed write, but the test
slot is initialized with `binlogOn=false`:

```text
Update dyn config, key=binlogOn, value=false
Binlog on=false
```

`Binlog.append(...)` returns immediately when binlog is off, so the offset equality does not prove that the new ordering
prevents a real append.

Relevant code:

- `src/main/java/io/velo/persist/OneSlot.java:184`
- `src/main/java/io/velo/repl/Binlog.java:431-434`
- `src/test/groovy/io/velo/persist/OneSlotTest.groovy:1268-1317`

### Verification

Command run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test binlog not appended and needPutV not recovered when doPersist throws"
```

Result: passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo / test-output inspection:

- The test output confirms `Binlog on=false`, and `Binlog.java:433` is covered, meaning `Binlog.append(...)` returned
  before writing.
- `OneSlot.java:1910-1911` is covered by successful setup writes, but the failed `doPersist(...)` path does not prove a
  real binlog append was suppressed.
- `OneSlot.removeDelay(...)` lines `1750-1756` are not covered by this regression test.
- The big-string binlog path at `OneSlot.java:1872-1876` is not covered by this regression test.

### Review Conclusion

`8da172f` moves the main normal-put `XWalV` append in the right direction for the `needPutV` overflow case and removes
the recovery code as agreed, but Bug 1 should not be considered fully fixed yet. The same pre-persist binlog ordering
still exists for remove-delay and big-string flows, and the blanket `XWalV` deferral can suppress binlog for writes that
were already durably written to the local WAL before `doPersist(...)` failed.

---

## Follow-up Review Feedback - Bug 1 Commits `0ef0214` / `7027130` / `6ff9728`

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary of the fix

The follow-up commits address the concrete ordering issues from the previous review:

- `removeDelay(...)` now calls `doPersist(...)` before appending `XWalV`.
- `Wal.PutResult` now carries the actual `v`, so `removeDelay(...)` and `put(...)` can append the correct value after
  local acceptance instead of relying on `needPutV`.
- Big-string `XBigStrings` is created before `Wal.put(...)` but appended only after `doPersist(...)` succeeds.
- The regression test now explicitly enables binlog and compares the binlog offset immediately before and after the
  failing write, so the previous vacuous `binlogOn=false` issue is fixed.

### Findings

#### High: `needPersist=true, needPutV=null` failure path can still leave local WAL ahead of binlog

The current flow appends `XWalV` only after `doPersist(...)` succeeds:

```java
var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);
if (putResult.needPersist()) {
    doPersist(walGroupIndex, key, putResult);
}
appendBinlog(new XWalV(putResult.v(), isValueShort));
```

This is correct for the original Bug 1 overflow path where `putResult.needPutV() != null`: that value has not been
written to the WAL file or delay maps, so if `doPersist(...)` throws, the write is rejected and no binlog is appended.

However, `Wal.put(...)` can also return `needPersist=true` with `needPutV == null` after it has already written the
current value to the WAL file, advanced the write position, and inserted it into the delay map. If `doPersist(...)`
throws in that path, the method returns an error and skips binlog append, but the master can still retain the value in
local WAL state and recover it after restart.

Relevant code:

- `src/main/java/io/velo/persist/Wal.java:975-1041` writes the value before returning `PutResult(..., needPutV=null, ...)`
- `src/main/java/io/velo/persist/OneSlot.java:1906-1914` appends binlog only after `doPersist(...)`

This is not the original `needPutV` data-loss bug, but it is still a failure-atomicity/binlog-consistency risk: a client
can receive an error, the master can retain the write locally, and replicas do not receive the corresponding binlog.

### Fixed From Prior Review

- `removeDelay(...)` no longer appends binlog before `doPersist(...)`.
- Big-string metadata binlog is no longer appended before local write acceptance.
- The regression test now enables binlog and exercises a real append path.
- JaCoCo confirms the changed `removeDelay(...)`, big-string, `put(...)`, and `doPersist(...)` lines are covered by
  `OneSlotTest`.

### Verification

Commands run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test binlog not appended and needPutV not recovered when doPersist throws"
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest"
```

Results: both passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

Test output confirms the focused regression is non-vacuous:

```text
Binlog on=true, offset before failure writes=9850
SegmentOverflowException caught for key=xh!0_key:000000000076, binlog offset before this write=64372
```

JaCoCo inspection after `OneSlotTest`:

- `OneSlot.java:1752-1757` (`removeDelay` persist-before-binlog path) covered.
- `OneSlot.java:1874` and `1910-1914` (big-string metadata delayed append and WAL append) covered.
- `OneSlot.java:1958-1969` (`doPersist`, including `needPutV` reinsertion after successful persist) covered.
- `Wal.java:960` and `971` (`needPutV != null` return paths) covered.
- `Wal.java:1012` (short-value `needPutV == null` return path) covered.
- `Wal.java:1041` normal-value `needPutV == null` return is covered only for one branch; the failure semantics for this
  path are not directly tested.

### Review Conclusion

The original Bug 1 `needPutV` overflow failure path now matches the agreed design: if `putValueToWal(...)` throws, the
deferred value is not recovered and no binlog is appended. The previous remove-delay, big-string, and vacuous-test issues
are fixed.

The remaining concern is broader than the original `needPutV` bug: writes already recorded in the local WAL before a
later `doPersist(...)` failure can still be omitted from binlog. If the chosen contract is "any `doPersist(...)` failure
rejects the write", this path still needs either rollback or a narrower binlog rule. If the contract is specifically
limited to the `needPutV != null` overflow case, Bug 1 can be treated as fixed with this residual risk documented.

---

## Final Review Feedback - Bug 1 Binlog Ordering Decision

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary

The current `OneSlot.put(...)` and `OneSlot.removeDelay(...)` code now follows the agreed write-acceptance decision:

- If `Wal.PutResult.needPutV() != null`, the current value has not been written to WAL file/map. `doPersist(...)` must
  succeed first. If `doPersist(...)` throws, the write is rejected, `needPutV` is not recovered, and binlog is not
  appended.
- If `Wal.PutResult.needPutV() == null`, the current value has already been written to WAL file/map. Binlog can be
  appended before `doPersist(...)`, so a later persist failure does not leave local WAL ahead of replication.
- Big-string metadata (`XBigStrings`) follows the same acceptance boundary as the associated `XWalV`.

### Current Data Flow

For `put(...)`:

1. Build `Wal.V`, and for big strings build `XBigStrings` but do not append it yet.
2. Call `targetWal.put(...)`.
3. If `needPersist && needPutV == null`, append `XBigStrings` if present and append `XWalV`, then call
   `doPersist(...)`.
4. If `needPersist && needPutV != null`, call `doPersist(...)` first; append `XBigStrings` and `XWalV` only after it
   succeeds.
5. If `!needPersist`, append binlog after the WAL write returns.

For `removeDelay(...)`:

1. Call `targetWal.removeDelay(...)`.
2. If `needPersist && needPutV == null`, append `XWalV`, then call `doPersist(...)`.
3. If `needPersist && needPutV != null`, call `doPersist(...)` first; append `XWalV` only after it succeeds.
4. If `!needPersist`, append binlog after the WAL write returns.

### Verification

Commands run after the final `put(...)` / `removeDelay(...)` alignment:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test binlog not appended and needPutV not recovered when doPersist throws"
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest"
```

Results: both passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo inspection after `OneSlotTest`:

- `OneSlot.java:1753-1766` remove-delay binlog split is covered, except the `needPutV == null` pre-`doPersist(...)`
  append branch is not directly hit by current tests.
- `OneSlot.java:1917-1936` put binlog split is covered, including both `needPutV == null` and `needPutV != null` paths.
- `OneSlot.java:1920-1933` big-string metadata delayed append branches are covered.
- `OneSlot.java:1968-1979` `doPersist(...)` and successful `needPutV` reinsertion are covered.

### Conclusion

Bug 1 is aligned with the agreed decision. The only remaining test gap is the specific `removeDelay(...)` branch where
`needPersist == true && needPutV == null`; existing `OneSlotTest` covers the surrounding remove-delay flow, but not that
exact pre-`doPersist(...)` binlog append branch.

---

## Review Feedback - Bug 3 Commit `6b35f90`

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary

Commit `6b35f90` changes merge marker handling in the intended direction:

- `MetaChunkSegmentFlagSeq.findThoseNeedToMerge(...)` now returns a mergeable marker without clearing or splitting the
  marker immediately.
- `MetaChunkSegmentFlagSeq.commitMergedRange(...)` clears an exactly merged marker, or advances a larger marker to the
  unmerged tail after a split merge.
- `OneSlot.putValueToWal(...)` calls `commitMergedRange(...)` only after `chunk.persist(...)` succeeds and the old
  merged segment range has been marked `SEGMENT_FLAG_REUSABLE`.

This fixes the original Bug 3 failure window for the normal path: if `chunk.persist(...)` throws after
`readSomeSegmentsBeforePersistWal(...)`, the merge marker remains in memory and the same range can be retried later.

### Findings

No blocking code issues found in the committed Bug 3 fix.

### Concerns

- The committed tests cover the marker-level behavior in `MetaChunkSegmentFlagSeqTest`, including repeated reads before
  commit and split-marker commit behavior. They do not directly cover the production integration path in
  `OneSlot.putValueToWal(...)` where `readSomeSegmentsBeforePersistWal(...)` returns an `ext`, `chunk.persist(...)`
  succeeds, and `commitMergedRange(...)` is invoked.
- JaCoCo after `./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` shows
  `MetaChunkSegmentFlagSeq.commitMergedRange(...)` lines covered, but `OneSlot.java:2366`
  (`metaChunkSegmentFlagSeq.commitMergedRange(...)`) is not covered by that run.

### Verification

Command run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"
```

Result: passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo inspection:

- `MetaChunkSegmentFlagSeq.java:541`, `550`, and `552` covered for marker decode and split-marker update.
- `MetaChunkSegmentFlagSeq.java:544` and `549` are partially covered because negative/no-match branches are not
  exercised.
- `OneSlot.java:2366` is not covered by this targeted run.

### Conclusion

Bug 3 can be treated as fixed for the reviewed marker-consumption bug. Recommended follow-up: add one focused
`OneSlot` regression test that drives a real merge `ext` through `putValueToWal(...)` and verifies the marker is not
consumed when `chunk.persist(...)` fails, then is consumed after a successful persist.

---

## Follow-up Review Feedback - Bug 3 Commit `80a3243`

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary

Commit `80a3243` tries to close the previous coverage gap by adding a `OneSlot` integration test for merge marker
survival when persist fails. The intent is right, but the committed test does not currently exercise the intended path.

### Findings

1. **Blocking: the new `OneSlot` regression test can pass without testing the bug path.**

   In the focused run, the test printed:

   ```text
   No segments with data, skipping marker test
   ```

   The test then returned early from `src/test/groovy/io/velo/persist/OneSlotTest.groovy:1381-1383`, so it did not call
   `readSomeSegmentsBeforePersistWal(...)`, did not call `putValueToWal(...)`, and did not verify marker survival after
   a real `chunk.persist(...)` failure.

   JaCoCo confirms the path was not executed:

   - `OneSlot.java:2306` (`readSomeSegmentsBeforePersistWal(...)` inside `putValueToWal`) is not covered.
   - `OneSlot.java:2366` (`metaChunkSegmentFlagSeq.commitMergedRange(...)`) is still not covered.

   This means commit `80a3243` does not actually resolve the test gap identified in the previous review.

### Additional Notes

- The test name says "survives failed persist and consumed after success", but the body only attempts the failed-persist
  side. There is no successful second persist and no assertion that the marker is consumed after success.
- The production fix from `6b35f90` still looks directionally correct; this follow-up issue is with the regression test,
  not with the marker-consumption implementation itself.

### Verification

Command run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test merge marker survives failed persist and consumed after success"
```

Result: passed with `BUILD SUCCESSFUL`, but the test skipped its assertions by returning early after finding no
`HAS_DATA` segments.

### Conclusion

Bug 3's production code can still be treated as plausibly fixed by `6b35f90`, but commit `80a3243` should not be treated
as a valid regression-test fix. The test should create deterministic `HAS_DATA` merge segments, fail `chunk.persist(...)`
after `putValueToWal(...)` has selected an `ext`, assert the marker remains, then run a successful persist and assert the
marker is consumed.

---

## Final Follow-up Review Feedback - Bug 3 Commit `59ce705`

Reviewed by: AI agent 1
Date: 2026-05-07

### Summary

Commit `59ce705` replaces the previous vacuous integration test with a deterministic `OneSlot` regression test:

- lowers `segmentNumberPerFd` and `valueSizeTrigger` so normal-value writes create `HAS_DATA` segments;
- clears auto-created markers, then adds a single manual marker for a real `HAS_DATA` segment;
- forces `putValueToWal(...)` to fail with no reusable segments and checks the marker survives;
- restores reusable capacity, retries `putValueToWal(...)`, and checks the marker count drops back to the expected
  post-success state.

### Findings

No blocking issues found in the updated Bug 3 test commit.

### Concerns

- The final success assertion is count-based (`finalMarkerCount <= 1`) rather than explicitly checking the exact marker
  identity. This is acceptable for the current test because the setup clears all previous markers and starts from one
  manual marker, but an exact helper that exposes marker ranges would make the assertion more direct.

### Verification

Commands run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test merge marker survives failed persist and consumed after success"
./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"
./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test merge marker survives failed persist and consumed after success" --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"
```

Results: all passed with `BUILD SUCCESSFUL` and generated `:jacocoTestReport`.

JaCoCo inspection after the combined run:

- `OneSlot.java:2306` (`readSomeSegmentsBeforePersistWal(...)` inside `putValueToWal`) covered.
- `OneSlot.java:2356` (`chunk.persist(...)`) covered.
- `OneSlot.java:2363` (`setSegmentMergeFlagBatch(...)` for old merged segments) covered.
- `OneSlot.java:2366` (`metaChunkSegmentFlagSeq.commitMergedRange(...)`) covered.
- `MetaChunkSegmentFlagSeq.java:544`, `549`, `550`, and `552` covered or partially covered for exact and split-marker
  commit branches.

### Conclusion

Bug 3 can now be treated as fixed and covered for the reviewed failure mode: merge markers are no longer consumed by
`findThoseNeedToMerge(...)`, they survive a failed `chunk.persist(...)`, and the production success path calls
`commitMergedRange(...)` after the old merged segments are marked reusable.
