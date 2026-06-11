# Bug 46 Persist Layer Review Round 1

Author: AI agent 1

Review date: 2026-06-02

## Scope

Fresh review of the persist layer focusing on issues that were not covered in prior rounds
(bugs 11-32 and 39-44). Prior rounds covered:

- Persisted big-string scan decode of PVM (bug 11)
- WAL recovery after `recoverNeedPutV` removal (bug 22, bug 31)
- Marker buffer exhaustion and soft-drop live discoverability (bug 31, bug 31 fix direction)
- `Wal.put` stale offset after `rewriteOneGroup` (bug 32)
- Tombstoning unpersisted big-string cleanup (bug 31, commit `3834694a`)
- Hardcoded `kerry-test-big-string-` test prefix gate (bug 31, commit `0d26ddce`)
- PVM bounds validation (bug 29, bug 30)
- Binlog ordering with `xBigStrings` + `XWalV` (bug 22, bug 31)

This round drills into:

- `SegmentBatch2.split` segment-split boundary — the strict `< bytes.length` check
- `OneSlot.put` exception path when `doPersist` throws after the new big-string file is written
  (a follow-up to bug 31 Finding 1, where the reviewer noted the new uuid should be tracked)
- `OneSlot.intervalDeleteOverwriteBigStringFiles` — silent drop on delete failure

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`

Code reviewed:

- `src/main/java/io/velo/persist/SegmentBatch2.java` (split / compressAsSegment / encodeToBuffer)
- `src/main/java/io/velo/persist/OneSlot.java` (put / removeDelay / doPersist / intervalDeleteOverwriteBigStringFiles)
- `src/main/java/io/velo/persist/BigStringFiles.java` (deleteBigStringFileIfExist / writeBigStringBytes)

---

## Finding 1: `SegmentBatch2.split` flushes an empty segment before the first value that exactly fills a segment

**Severity:** Low

**AI agent 2 status:** Confirmed

**Files:**

- `src/main/java/io/velo/persist/SegmentBatch2.java:92-119` (`split`)
- `src/main/java/io/velo/persist/SegmentBatch2.java:127-142` (`compressAsSegment`)

**Code excerpt:**

```java
// SegmentBatch2.java:92-119
public ArrayList<SegmentBytesWithIndex> split(@NotNull ArrayList<Wal.V> list, @NotNull ArrayList<PersistValueMeta> returnPvmList) {
    ArrayList<SegmentBytesWithIndex> result = new ArrayList<>(100);
    ArrayList<Wal.V> onceList = new ArrayList<>(100);

    int i = 0;

    var persistLength = SEGMENT_HEADER_LENGTH;
    for (Wal.V v : list) {
        persistLength += v.persistLength();

        if (persistLength < bytes.length) {
            onceList.add(v);
        } else {
            result.add(compressAsSegment(onceList, i, returnPvmList));
            i++;

            onceList.clear();
            persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
            onceList.add(v);
        }
    }

    if (!onceList.isEmpty()) {
        result.add(compressAsSegment(onceList, i, returnPvmList));
    }

    return result;
}
```

**Root cause and impact:**

The loop in `split` checks `if (persistLength < bytes.length)` (strict less than). When
`persistLength == bytes.length`, the else branch fires unconditionally and flushes the
current `onceList` before starting a new segment with the current `v`.

When `onceList` is empty at the moment of the flush, `compressAsSegment` is called with an
empty list. That emits a real segment to the result: a 17-byte segment header followed by
the 2-byte zero short terminator (or 4 bytes if the remaining buffer is large enough), so
the segment fills with mostly zeros but is still recorded in the in-memory segment index
and persisted to disk.

**Reachability:**

For `onceList` to be empty at the moment of the flush, the very first `Wal.V` in `list`
must trigger the else branch. That requires:

```
SEGMENT_HEADER_LENGTH + first_v.persistLength() == bytes.length
```

i.e. the first value's `persistLength` is exactly `bytes.length - SEGMENT_HEADER_LENGTH`,
which for the default `bytes.length = 4096` and `SEGMENT_HEADER_LENGTH = 17` means a value
of persist length **4079**. This is the maximum allowed length for a chunk value (the
big-string admission check in `OneSlot.put` line 1449 rejects anything with
`persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength`, i.e. `persistLength > 4079`).

So the trigger is real but narrow: a single WAL list whose first value has persist length
exactly 4079. Once it fires, the wasted segment is permanent: the segment index advances
by one, an empty `HAS_DATA` segment is written to disk, `metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup`
records the empty segment range, and the next real value lands in a later segment.

The wasted segment:

- occupies a segment index slot that a real value would have used,
- writes ~17-4096 bytes of zeros to disk (depending on how the loop body later fills the
  buffer in `compressAsSegment`),
- is recorded in the marker ring and the reuse bitset with `SEGMENT_FLAG_HAS_DATA`, so it
  is not reclaimable until a merge happens to forward-copy the empty segment's data.

After the first trigger, subsequent values in the same `list` iteration behave normally
(because `onceList` is now non-empty when the else branch fires for the second time).

**Trigger probability:**

The condition is `v.persistLength() == 4079`. Persist length is `2 (key length short) + keyLength + cvEncoded.length`. For a key length of, say, 10, `cvEncoded.length = 4067`. The probability of any given value having exactly this size is low for naturally-generated
data, but a workload that ingests fixed-size values (e.g., 4 KB binary blobs from a
specific producer) can hit this repeatedly. The bug is silent — no exception, no log —
and the only observable symptom is slightly higher disk usage and earlier-than-expected
chunk wrap-around.

**Suggested fix direction:**

Change the loop condition from strict less-than to less-than-or-equal-to and add a
"fits-exactly" branch that does not flush an empty `onceList`:

```java
if (persistLength <= bytes.length) {
    onceList.add(v);
} else {
    // onceList is non-empty here because we already added vs that fit
    result.add(compressAsSegment(onceList, i, returnPvmList));
    i++;

    onceList.clear();
    persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
    onceList.add(v);
}
```

Wait — `persistLength <= bytes.length` would also incorrectly add an over-sized value to
`onceList`. The correct fix is to handle the exact-fit case at the boundary:

```java
if (persistLength < bytes.length) {
    onceList.add(v);
} else if (persistLength == bytes.length && !onceList.isEmpty()) {
    onceList.add(v);
    result.add(compressAsSegment(onceList, i, returnPvmList));
    i++;
    onceList.clear();
    persistLength = SEGMENT_HEADER_LENGTH;
} else {
    // persistLength > bytes.length, or persistLength == bytes.length with empty onceList
    if (!onceList.isEmpty()) {
        result.add(compressAsSegment(onceList, i, returnPvmList));
        i++;
    }
    onceList.clear();
    persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
    onceList.add(v);
}
```

This ensures that a value which exactly fills a segment is written into that segment
together with the prior accumulated values, without first flushing an empty segment.

**Regression test should include:**

- A `Wal.V` list containing a single value with `persistLength() == bytes.length - SEGMENT_HEADER_LENGTH` (e.g., 4079 for default config). Call `split(...)` and assert the result has **one** segment, not two. Assert the segment's `valueBytesLength` equals the input's `persistLength()` and that the segment bytes, when re-decoded via `iterateFromSegmentBytes`, yield exactly the input value.
- A follow-up assertion that the wasted-segment counter (e.g., a new test-only field) is 0.

---

## Finding 2: `OneSlot.put` orphans the newly written big-string file when `doPersist` throws after the file write but before the WAL accepts the value

**Severity:** Medium

**AI agent 2 status:** Confirmed, with reachability refinement

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1399-1524` (`put`)
- `src/main/java/io/velo/persist/OneSlot.java:1558-1575` (`doPersist`)
- `src/main/java/io/velo/persist/BigStringFiles.java:218-253` (`writeBigStringBytes`)

**Code excerpt:**

```java
// OneSlot.put — long-value big-string branch
// (line 1453-1480, the test-prefix gate is included as in the source)
if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
    overwrittenBigStringUuid = getCurrentBigStringUuid(targetWal, key, bucketIndex, cv.getKeyHash());
    var bigStringUuid = snowFlake.nextId();
    var bytes = cv.getCompressedData();
    var isWriteOk = bigStringFiles.writeBigStringBytes(bigStringUuid, bucketIndex, cv.getKeyHash(), bytes);
    if (!isWriteOk) {
        throw new RuntimeException("Write big string file error, uuid=" + bigStringUuid + ", key=" + key);
    }
    ...
    xBigStrings = new XBigStrings(bigStringUuid, bucketIndex, cv.getKeyHash(), key, cvBigStringEncoded);
    v = new Wal.V(..., CompressedValue.SP_TYPE_BIG_STRING, key, cvBigStringEncoded, isFromMerge);
    isValueShort = true;
    ...
}

var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);

boolean isBinlogAppended = false;
if (putResult.needPersist()) {
    if (putResult.needPutV() == null) {
        // binlog-before-doPersist safe path
        if (xBigStrings != null) { appendBinlog(xBigStrings); }
        appendBinlog(new XWalV(putResult.v(), isValueShort));
        isBinlogAppended = true;
    }
    doPersist(walGroupIndex, key, putResult);  // can throw
}

if (overwrittenBigStringUuid != null) {
    var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
    if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
        delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(overwrittenBigStringUuid, bucketIndex, cv.getKeyHash(), key));
    }
}

if (!isBinlogAppended) {
    if (xBigStrings != null) { appendBinlog(xBigStrings); }
    appendBinlog(new XWalV(putResult.v(), isValueShort));
}
```

**Root cause:**

When the input value triggers the big-string admission branch (line 1453), the new
big-string file is written to disk at line 1457 **before** the value is even offered to
the WAL. If `targetWal.put(...)` returns `PutResult{needPersist=true, needPutV=v}` (WAL
buffer full) and the subsequent `doPersist(...)` then throws (e.g., `SegmentOverflowException`
from `Chunk.persist`, or `BucketFullException` from `KeyBucketsInOneWalGroup.putPvmListToTargetBucket`),
the new file at `bigStringUuid` is referenced by **nothing**:

- `delayToKeyBucketShortValues` was overwritten by the tombstone-like `v`? No — `needPutV`
  was NOT inserted into the map (per `Wal.put` line 859, the `PutResult{needPutV=v}` is
  returned **before** the delay-map write at line 877).
- The binlog trailing block at line 1517-1523 IS skipped because `isBinlogAppended=false`
  AND `doPersist` threw first — the exception propagates out of `put` before reaching that
  trailing block.
- `bigStringFileUuidByKey` for this key still points at whatever was there before
  (`overwrittenBigStringUuid` if any, otherwise the map is unchanged).

The new `bigStringUuid` file is orphaned. The `overwrittenBigStringUuid` cleanup at line
1510-1514 is also skipped because the exception propagates past it.

The file is then reaped only by the periodic `intervalDeleteOverwriteBigStringFiles`
orphan scan (`OneSlot.java:913`), which walks all big-string files in each bucket per tick
and enqueues any uuid that is absent from both the persisted key-bucket state and
`bigStringFileUuidByKey`. This is **eventually correct** (the bug 31 review concluded the
same), but:

- The scan processes one bucket per tick, so on a slot with many buckets the orphan can
  stay on disk for the duration of one full sweep (potentially hours at `confChunk.onceScanMaxLoopCount` cadence).
- The scan also does not enqueue the `bigStringUuid` for the **specific rejected write**;
  it must first observe the file via `getBigStringFileIdList`, then deduce that it is
  orphaned. If the user then writes another value at the same key before the scan reaches
  that bucket, the new value's file is also orphaned, and the scan must observe both.
- There is no metric or log for this case, so the leak is silent and unbounded under
  repeated rejection.

**Reachability:**

The `Wal.put` buffer-full early-return path is reached whenever
`writePosition + encodeLength > ONE_GROUP_BUFFER_SIZE` (`Wal.java:836`). The
`doPersist` then throws if `Chunk.persist` cannot find enough consecutive reusable
segments (`Chunk.persist` line 308-310) or if `KeyBucketsInOneWalGroup.putPvmListToTargetBucket`
overflows the bucket split limit (`KeyBucketsInOneWalGroup.java:314-320`). Both
conditions are reachable in production under skewed workloads (hot WAL group, bucket
split saturation).

The bug 31 review's `findThoseNeedToMerge` empty-segment discovery caveat (raised by AI
agent 2 in the bug 31 review) showed that live merge discovery is marker-based, so
`HAS_DATA` ranges can be stranded for a long time. The same logic applies here: a
write-stall that prevents new segments from being allocated can keep `doPersist` throwing
on the same WAL group until another group's merge frees reusable capacity, multiplying
the orphan count.

**Suggested fix direction:**

Track the newly written big-string `uuid` and enqueue it for deletion if the put
ultimately throws. Two minimal options:

1. **Inline try/catch in `OneSlot.put`** that catches `RuntimeException` from
   `doPersist`, enqueues the new uuid (if `xBigStrings != null` AND the WAL did not
   accept the new v), and re-throws. The condition "WAL did not accept" is exactly
   `putResult.needPutV() != null`.

2. **Move the big-string file write to AFTER `targetWal.put` succeeds** in the case where
   the put returns `needPutV`. That is, only write the file when the WAL has accepted the
   v. The downside is that the `getCurrentBigStringUuid` snapshot for overwrite cleanup
   becomes wrong (the WAL might still hold an old big-string value), so this option is
   more invasive.

Option 1 is the smaller-blast change. Sketch:

```java
// After doPersist block, before overwrite-cleanup block
if (xBigStrings != null && putResult.needPutV() != null) {
    // WAL buffer was full and the new v was NOT inserted; doPersist may or may not
    // have succeeded. We track the new uuid and the put result so that any later
    // failure (or the trailing path) can clean it up.
    newBigStringUuidForExceptionCleanup = xBigStrings.getUuid();
}
try {
    doPersist(walGroupIndex, key, putResult);
} catch (RuntimeException e) {
    if (newBigStringUuidForExceptionCleanup != null) {
        delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(
                newBigStringUuidForExceptionCleanup, bucketIndex, cv.getKeyHash(), key));
    }
    throw e;
}
```

The trailing `if (!isBinlogAppended)` block at line 1517 should also re-check that the
new uuid's binlog entry was actually appended. If the new uuid was enqueued for deletion,
the `xBigStrings` binlog entry should NOT be appended (the binlog must not advertise a
file that the master has chosen to discard).

**Regression test should include:**

- A `OneSlot.put` of a big-string value when the target WAL group is at the buffer-full
  state. Force `doPersist` to throw (e.g., set `isOnRewrite = false` in the target WAL,
  or set the chunk segment index to a value where no reusable segments can be found).
  Assert the exception propagates. Assert the new big-string file is either deleted
  immediately or enqueued in `delayToDeleteBigStringFileIds` (size delta == +1).
- Verify the binlog does **not** contain an `XBigStrings` entry for the rejected uuid
  (otherwise a slave that replays the binlog would try to fetch a file the master no
  longer advertises).
- Verify the next `intervalDeleteOverwriteBigStringFiles` tick actually deletes the file
  (the existing periodic sweep path), and that subsequent `get` on the key returns the
  pre-write value (or null) without throwing.

---

## Finding 3: `OneSlot.intervalDeleteOverwriteBigStringFiles` silently drops big-string file entries whose physical delete returns false

**Severity:** Low

**AI agent 2 status:** Confirmed

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:913-917` (`intervalDeleteOverwriteBigStringFiles`)
- `src/main/java/io/velo/persist/BigStringFiles.java:261-287` (`deleteBigStringFileIfExist`)

**Code excerpt:**

```java
// OneSlot.intervalDeleteOverwriteBigStringFiles
int intervalDeleteOverwriteBigStringFiles(int targetBucketIndex) {
    if (!delayToDeleteBigStringFileIds.isEmpty()) {
        var oneId = delayToDeleteBigStringFileIds.removeFirst();
        bigStringFiles.deleteBigStringFileIfExist(oneId.uuid(), oneId.bucketIndex(), oneId.keyHash());
    }
    ...
}
```

```java
// BigStringFiles.deleteBigStringFileIfExist
public boolean deleteBigStringFileIfExist(long uuid, int bucketIndex, long keyHash) {
    if (bigStringBytesByUuidLRU != null) {
        bigStringBytesByUuidLRU.remove(uuid);
    }
    var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
    if (!file.exists()) {
        return true;
    }
    var len = file.length();
    var beginT = System.nanoTime();
    var r = file.delete();
    var costT = (System.nanoTime() - beginT) / 1000;
    if (r) {
        bigStringFilesCount--;
        diskUsage -= len;
        // stats
        deleteByteLengthTotal += len;
        deleteFileCountTotal++;
        deleteFileCostTotalUs += costT;
    }
    return r;
}
```

**Root cause:**

`deleteBigStringFileIfExist` returns `false` if `file.delete()` returns `false` (e.g., a
transient I/O error, the file is held open by another process, or permissions changed
mid-run). `OneSlot.intervalDeleteOverwriteBigStringFiles` discards the return value and
unconditionally advances the queue (`removeFirst()` already happened before the call).
The entry is lost.

If the failure was transient, the file is still on disk. The next orphan scan in
`intervalDeleteOverwriteBigStringFiles` (line 921-963) walks all big-string files in
the bucket via `getBigStringFileIdList` and re-enqueues any uuid that is absent from
both the persisted key-bucket state and `bigStringFileUuidByKey`. So the file will be
re-enqueued on the next scan, **but only if** the failure was transient and the file is
still orphaned (not in WAL or persisted). For a long-running transient failure (e.g., a
stale file handle), the file leaks indefinitely.

The other caller of `deleteBigStringFileIfExist`, `BigStringFiles.handleWhenCvExpiredOrDeleted`
(line 322), DOES check the return value and throws `RuntimeException` on failure. The
asymmetry is a code smell — the queue-drain path should also react to a failure.

**Reachability:**

Low. `file.delete()` failures are rare in normal operation. The main trigger is the file
being held open (e.g., a stale `RandomAccessFile` from a crashed handler thread, or the
LRU bytes buffer being mapped somewhere). On Linux, `file.delete()` on an open file
returns `false` even if the process could have unlinked it; on Windows, it returns
`false` more aggressively.

**Suggested fix direction:**

On `false` return, re-add the entry to the tail of the queue (so the next tick tries
again) and log a warning. Sketch:

```java
if (!delayToDeleteBigStringFileIds.isEmpty()) {
    var oneId = delayToDeleteBigStringFileIds.removeFirst();
    var ok = bigStringFiles.deleteBigStringFileIfExist(oneId.uuid(), oneId.bucketIndex(), oneId.keyHash());
    if (!ok) {
        // re-enqueue at the tail for retry; log a warning
        delayToDeleteBigStringFileIds.addLast(oneId);
        log.warn("Big string file delete returned false, will retry next tick, uuid={}, key={}, slot={}",
                oneId.uuid(), oneId.key(), slot);
    }
}
```

Optionally cap the retry count (e.g., drop after 100 retries) to bound the queue size
in the pathological case where the failure is permanent.

A symmetric metric (`big_string_files_delete_failed_total`) would let operators observe
this case in production.

**Regression test should include:**

- A scenario where `file.delete()` returns false (mock via a subclass or a test-only
  flag). Verify the entry is re-enqueued, the warning is logged, and the queue size
  remains non-zero until the eventual success.
- A scenario where the failure is permanent (e.g., 5 retries, all fail). Verify the
  entry is dropped and `big_string_files_delete_failed_total` is incremented.

---

## Non-Findings (Verified Safe Or Already Tracked)

1. **`Wal.put` stale `writePosition*` after `rewriteOneGroup`**: Fixed in commit `222048ee`
   (bug 32). Verified current code: `Wal.java:851` updates local `offset` after successful
   rewrite; `Wal.java:865` uses the updated offset for `putVToFile`. Trailer decision at
   `Wal.java:736` is also based on the correct offset.

2. **`OneSlot.put` orphan big-string file from `doPersist` exception path for normal
   overwrites (non-`needPutV`)**: When `putResult.needPutV() == null`, the new v is
   inserted into the delay map BEFORE `doPersist` is called. The binlog is appended
   before `doPersist` in the safe path. If `doPersist` throws, the v is still in the
   delay map; the trailing `if (!isBinlogAppended)` block is skipped on exception
   propagation, but the in-memory state is consistent (the value is in the WAL delay
   map and will be re-flushed on the next persist cycle). No file leak in this branch.

3. **`OneSlot.removeDelay` tombstoning unpersisted big-string key**: Fixed in commit
   `3834694a`. Verified current code: `OneSlot.java:1356-1376` snapshots the current
   big-string uuid before `targetWal.removeDelay(...)`, then enqueues the old uuid after
   the tombstone's persist (or `doPersist` exception path).

4. **`KeyLoader.getPersistedBigStringIdList` PVM misdecode**: Fixed by `PersistValueMeta.isPvm`
   guard at `KeyLoader.java:913`. Verified current code returns early on `isPvm(valueBytes)`
   before any `CompressedValue.decode(...)` call.

5. **`MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup` ring buffer
   exhaustion**: Hard exception removed by commit `fe153304` (soft drop). Live
   discoverability concern raised by AI agent 2 in the bug 31 review is acknowledged
   and tracked but not addressed in this round (it would require either enlarging the
   ring or changing the live merge discovery path, both of which are out of scope for
   a single review round).

6. **Chunk fd LRU staleness after merge**: Verified that reads for chunk segments are
   gated by `OneSlot.hasData` (which checks the segment flag) for both `readForRepl` and
   `readForMerge`. After a merge, the old segments are flagged REUSABLE, so `hasData`
   returns false and no read is attempted. The LRU is never consulted for
   REUSABLE-flagged segments. Single-segment reads via `Chunk.readOneSegment` always
   follow a PVM lookup in key buckets, which is updated BEFORE the segment flag changes,
   so the LRU is not consulted for a stale PVM. No staleness defect in practice.

7. **`getPersistedBigStringIdList` decoding byte[] for each cell**: Re-verified. The
   per-cell `CompressedValue.decode(valueBytes, keyBytes, keyHash)` call at
   `KeyLoader.java:917` is necessary because the PVM-guard already filtered out PVM
   cells, and the remaining cells are short-form CVs. The decode is lightweight
   (header + dict seq + compressed data slice) and is the only way to identify
   `SP_TYPE_BIG_STRING` cells for orphan cleanup. Not a perf concern for the bucket-scan
   cadence (`onceScanMaxLoopCount` controls frequency).

---

## Summary

| Finding | Description | Severity | Status |
|---------|-------------|----------|--------|
| 1 - `SegmentBatch2.split` flushes empty segment for first v that exactly fills segment | Wasted segment slot + disk + marker-ring slot when first value's persist length is exactly `bytes.length - SEGMENT_HEADER_LENGTH` (4079 for default 4K segment) | **Low** | Fixed in commit `bd56d055`; reviewed by AI agent 2 |
| 2 - `OneSlot.put` orphans new big-string file when `doPersist` throws after the file is written but before the WAL accepts the v | New `bigStringUuid` file on disk has no in-memory or binlog reference; cleaned only by eventual periodic scan. Identified by bug 31 AI agent 2 but classified "no correctness fix required" — this round reclassifies as a real defect with a one-line fix. | **Medium** | Fixed in commit `641906ab`; reviewed by AI agent 2 after follow-up `ac3d0e88` |
| 3 - `intervalDeleteOverwriteBigStringFiles` drops `deleteBigStringFileIfExist` failures silently | Return value not checked; failed `file.delete()` leaves the file on disk and the queue entry is lost. Other caller (`handleWhenCvExpiredOrDeleted`) does check. Asymmetric error handling. | **Low** | Fixed in commit `84ff5fbd`; reviewed by AI agent 2 |

Finding 1 is fixed in commit `bd56d055`. Finding 2 is fixed in commit `641906ab` with style follow-up `ac3d0e88`. Finding 3 is fixed in commit `84ff5fbd`.

---

## Reviewer Notes

AI agent 2 reviewed the current code against the findings on 2026-06-11.

### Finding 1 Review

Initial confirmation before commit `bd56d055`. `SegmentBatch2.split` initialized `persistLength` to
`SEGMENT_HEADER_LENGTH`, adds each `Wal.V.persistLength()`, and uses a strict
`persistLength < bytes.length` check. If the first value makes
`persistLength == bytes.length`, the `else` branch calls `compressAsSegment(onceList, ...)`
while `onceList` is empty, then starts a second segment containing the actual value.

Before commit `bd56d055`, the code emitted an extra empty segment with `cv number = 0`, advances
the temporary segment index, increments segment-batch metrics, and returns two segment
records for a single exact-fit value. The impact is wasted segment space and metadata,
not data corruption, because the following segment still contains the value.

### Finding 2 Review

Confirmed, with one reachability refinement. In the big-string branch,
`OneSlot.put` writes the physical big-string file before calling `targetWal.put`.
When `targetWal.put` returns a `PutResult` with `needPersist=true` and
`needPutV != null`, `Wal.put` has not inserted the new value into
`delayToKeyBucketShortValues` and has not updated `bigStringFileUuidByKey`. If
`doPersist` throws before the later `targetWal.put(..., needPutV, ...)` retry, the new
file has no WAL, key-bucket, or binlog reference and is left for the eventual orphan
scan.

The review text cites `Chunk.persist` and `SegmentOverflowException`, but that is not
the main throw path for this exact big-string case, because the branch sets
`isValueShort = true` and `doPersist` calls
`KeyLoader.persistShortValueListBatchInOneWalGroup`. The reachable failure mode for the
confirmed bug is bucket persistence, including `BucketFullException` from
`KeyBucketsInOneWalGroup.putPvmListToTargetBucket`, or other runtime failures before
the `needPutV` retry succeeds. The cleanup/binlog concern is still valid.

### Finding 3 Review

Confirmed. `OneSlot.intervalDeleteOverwriteBigStringFiles(int)` removes one queued
`BigStringFiles.IdWithKey` with `removeFirst()` and calls
`bigStringFiles.deleteBigStringFileIfExist(...)`, but ignores the boolean return value.
`BigStringFiles.deleteBigStringFileIfExist` returns `false` when `File.delete()` fails,
and this queue-drain caller neither re-enqueues the id nor logs the failed delete. The
entry can be rediscovered by the later orphan scan only after that scan reaches the
bucket again, so transient or repeated delete failures are silent and can keep files on
disk longer than intended.



---

## Review Feedback - Finding 1 Fix

Reviewed by: AI agent 2

Reviewed commit: `bd56d055` (`fix: SegmentBatch2.split must not emit empty segment when first value exactly fills segment`)

### Summary

The fix addresses Finding 1 by changing `SegmentBatch2.split` so an exact-fill first value no longer flushes an empty `onceList`. The committed logic now only flushes before starting a new segment when `onceList` already contains values, and it writes exact-fit accumulated values into the current segment before clearing state.

### Strengths

- The production change is narrowly scoped to `SegmentBatch2.split` and preserves the existing segment-building structure.
- The new tests cover the original failing case: a single first value with `persistLength == segmentLength - SEGMENT_HEADER_LENGTH`.
- The new tests also cover the adjacent exact-fit case where a later value completes the segment, including decoded value order and offsets.
- Focused verification passed with `./gradlew :test --tests "io.velo.persist.SegmentBatch2Test"`.
- JaCoCo confirms coverage for `SegmentBatch2` lines 98-118: 17/17 lines covered, 0 partial, 0 uncovered, and all branches on the changed lines covered.

### Concerns

No blocking concerns found. I did not run a full test suite because this fix is isolated to segment splitting and the focused test class exercises the touched branch paths.

### Follow-ups

- No pre-commit follow-up required for Finding 1.
- Finding 2 was fixed later in commit `641906ab`; Finding 3 was fixed later in commit `84ff5fbd`.

---

## Review Feedback - Finding 2 Fix

Reviewed by: AI agent 2

Reviewed commits:

- `641906ab` (`fix: OneSlot.put enqueues new big-string uuid for cleanup when doPersist throws on needPutV path`)
- `ac3d0e88` (`style: use @TestOnly for OneSlot.doPersistForceThrowForTest`)

### Summary

The fix addresses Finding 2 by wrapping `doPersist(...)` in `OneSlot.put` with a `try`/`catch` and, only for the vulnerable path (`xBigStrings != null && putResult.needPutV() != null`), enqueueing the newly written big-string uuid in `delayToDeleteBigStringFileIds` before rethrowing. This matches the failure mode in the finding: the file has been written, but the WAL has not accepted the new `needPutV` value.

### Strengths

- The cleanup is narrowly scoped to the `needPutV` big-string path and does not change normal overwrite cleanup.
- The exception still propagates, preserving the existing write-failure semantics.
- The regression test forces the WAL buffer-full early-return path, forces `doPersist` to throw, and verifies the new uuid is enqueued while the WAL map still points at the previous uuid.
- The test also verifies that the orphan file exists on disk at the expected path, so it is exercising the real leaked-file condition rather than only checking queue state.
- Focused verification passed with `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test put big string with WAL buffer full and doPersist throw enqueues new uuid for cleanup"`.
- Broader focused verification passed with `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest"`.
- JaCoCo confirms the new catch path in `OneSlot` lines 1509-1515 is covered, and the `@TestOnly` throw hook at lines 1567-1574 is fully covered.

### Concerns

No blocking concerns found. The only residual coverage gap in the reviewed range is the pre-existing `xBigStrings != null` binlog branch for the non-`needPutV` path (`OneSlot.java:1499-1500`), which is outside the new failure-path cleanup.

### Follow-ups

- No pre-commit follow-up required for Finding 2.
- Finding 3 was fixed later in commit `84ff5fbd`.

---

## Review Feedback - Finding 3 Fix

Reviewed by: AI agent 2

Reviewed commit: `84ff5fbd` (`fix: re-enqueue big string id when deleteBigStringFileIfExist returns false`)

### Summary

The fix addresses Finding 3 by checking the boolean result from `bigStringFiles.deleteBigStringFileIfExist(...)` in `OneSlot.intervalDeleteOverwriteBigStringFiles(int)`. When the physical delete returns `false`, the code logs a warning and re-enqueues the same `BigStringFiles.IdWithKey` at the tail of `delayToDeleteBigStringFileIds`, so the failed delete is retried instead of being silently dropped.

### Strengths

- The production change is narrowly scoped to the queue-drain path identified in the finding.
- The fix preserves normal successful-delete behavior and only adds retry behavior on `false`.
- The warning includes uuid, key, slot, and bucket index, which makes the failure observable.
- The regression test forces `deleteBigStringFileIfExist` to return `false`, verifies the original id and key are preserved in the queue, and verifies the physical file still exists after the failed delete.
- Focused verification passed with `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest.test interval delete re-enqueues big string id when delete returns false"`.
- Broader focused verification passed with `./gradlew :cleanTest :test --tests "io.velo.persist.OneSlotTest"`.
- JaCoCo confirms `OneSlot` lines 914-920 are fully covered, including both branches of the queue-present and delete-failed checks.
- JaCoCo confirms the `BigStringFiles` forced-false hook is covered on both true and false outcomes.

### Concerns

No blocking concerns found. Minor: the Spock `then:` label says the id is re-enqueued at the head, but production uses `addLast`. The test only enqueues one id, so the assertion still proves the id is preserved; the label should be corrected during normal cleanup to avoid future confusion.

### Follow-ups

- No pre-commit follow-up required for Finding 3.
- All three findings in this review doc now have reviewed fix commits.
