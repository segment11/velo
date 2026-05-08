# Bug 23 Persist Layer Data Write Flow Review Round 5

Author: AI agent 1

## Scope

Static review of the persist-layer data **write** flow after Bug 18 (round 1), Bug 19, Bug 20 (round 2), Bug 21 (round 3), and Bug 22 (round 4). This round focuses on:

- Compressed segment (SegmentBatch) tight-packing overflow for incompressible data
- Big string overwrite detection bypassed when WAL buffer is full (`needPersist=true`)
- CompressedValue encode/decode roundtrip safety in the merge path
- Wal.exists() correctness for all value types

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`
- `doc/bug_reviews/bug_22_persist_layer_data_write_flow_review_round_4.md`

---

## Finding 1: SegmentBatch tight segment overflow on incompressible data — persist crash

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/SegmentBatch.java:237-267` (`tight` method)
- `src/main/java/io/velo/persist/SegmentBatch.java:183-228` (`tightSegments` method)
- `src/main/java/io/velo/persist/SegmentBatch.java:294-321` (`splitAndTight` — compressed split)
- `src/main/java/io/velo/persist/FdReadWrite.java:832-835` (`writeOneInner` — length guard)
- `src/main/java/io/velo/persist/Chunk.java:397-408` (single-segment write path)

**Code excerpt:**

```java
// SegmentBatch.tight() — line 246-261
int afterTightSegmentIndex = segments.getFirst().tmpSegmentIndex;
for (var segment : segments) {
    var compressedBytes = segment.compressedBytes;

    if (onceList.size() == MAX_BLOCK_NUMBER || onceListBytesLength + compressedBytes.length > chunkSegmentLength - HEADER_LENGTH) {
        // BUG: when onceList is empty (first iteration), this creates an empty tight segment
        var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
        r.add(tightOne);
        afterTightSegmentIndex++;

        onceList.clear();
        onceListBytesLength = 0;
    }

    onceList.add(segment);
    onceListBytesLength += compressedBytes.length;
}

var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
r.add(tightOne);
```

```java
// FdReadWrite.writeOneInner() — line 832-834
public int writeOneInner(int oneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
    if (bytes.length > oneInnerLength) {
        throw new IllegalArgumentException("Write bytes length must be less than one inner length");
    }
    // ...
}
```

**Root cause:**

The compressed segment path (`SegmentBatch`) uses Zstd compression on raw segment data of up to `chunkSegmentLength` (typically 4096) bytes. For incompressible data (random bytes, already-compressed payloads, encrypted data), Zstd can produce output **larger** than the raw input — approximately `rawSize + 15` bytes in the worst case.

The tight-packing loop in `tight()` checks whether a compressed sub-block fits within `chunkSegmentLength - HEADER_LENGTH` (4096 - 29 = 4067 bytes). When a single compressed sub-block exceeds this limit:

1. **Empty segment created**: The flush triggers with an empty `onceList`, producing a tight segment with `blockNumber=0` and only `HEADER_LENGTH` (29) bytes. This wastes one segment slot but is otherwise harmless.

2. **Oversized tight segment**: The oversized sub-block is then packed alone in the next tight segment. Its total byte size is `HEADER_LENGTH + compressedBytes.length`. Since `compressedBytes.length > 4067`, the total exceeds `chunkSegmentLength` (4096). For example, a 4111-byte compressed sub-block produces a 4140-byte tight segment.

3. **Write crash**: When `Chunk.persist()` writes this tight segment via `FdReadWrite.writeOneInner()`, the guard at line 833-834 throws `IllegalArgumentException("Write bytes length must be less than one inner length")`.

**Concrete reproduction scenario:**

1. `isSegmentUseCompression = true` (compressed segment mode enabled)
2. A WAL group accumulates long values with high-entropy content (e.g., compressed binary data, random UUIDs)
3. The raw segment data fills close to `chunkSegmentLength` (4096 bytes)
4. Zstd compression produces output of ~4111 bytes (larger than 4067-byte limit)
5. `tight()` creates an empty segment + an oversized segment
6. `writeOneInner()` throws `IllegalArgumentException`
7. Exception propagates: `Chunk.persist()` → `putValueToWal()` → `doPersist()` → `OneSlot.put()` → command handler
8. The persist fails; data remains in WAL but subsequent writes to the same WAL group also fail on retry
9. The WAL group becomes permanently stuck — it cannot persist its data

**Impact:**

- **Persist failure for incompressible data**: Any WAL group that accumulates high-entropy long values becomes unable to persist, blocking all future writes to keys in that WAL group
- **WAL buffer exhaustion**: Failed persists cause the WAL to accumulate entries without clearing; once the 64KB buffer fills, new writes are rejected
- **Empty segment waste**: The empty tight segment consumes a segment slot that is never referenced by any PVM; it is only reclaimed when the merge worker processes it (finding 0 entries)
- **Combined with Bug 22 Finding 1**: If `needPutV` exists when the persist crashes, the triggering value is silently lost from local state (per Bug 22 Finding 1)

**Suggested fix direction:**

1. **Fallback to raw (uncompressed) storage when compression expands the data**:

In `compressAsSegment()` (`SegmentBatch.java:332-354`), after Zstd compression, compare the compressed size with the raw size. If compression expanded the data, use the raw bytes as a single sub-block in a `NORMAL`-type segment instead of a `TIGHT`-type compressed segment:

```java
var compressedBytes = Zstd.compress(bytes);
if (compressedBytes.length >= bytes.length) {
    // Compression did not help — use raw bytes as a NORMAL segment
    return new SegmentCompressedBytesWithIndex(Arrays.copyOf(bytes, bytes.length), tmpSegmentIndex, segmentSeq, valueBytesLength);
}
```

The tight-packing code and reading code need to handle mixed NORMAL and TIGHT sub-blocks, or the oversized segment should bypass tight packing entirely and be written as a single uncompressed `NORMAL` segment.

2. **Guard in `tight()` against oversized single sub-blocks**:

Skip compression for segments where `Zstd.compressBound(rawLength)` exceeds `chunkSegmentLength - HEADER_LENGTH`, or detect the oversized case and delegate to the `SegmentBatch2` (uncompressed) path for that particular segment.

**Regression tests should include:**

- Write long values with random/incompressible content to a slot with `isSegmentUseCompression = true`
- Verify that `Chunk.persist()` does not throw `IllegalArgumentException`
- Verify that the data is correctly readable after persist
- Verify that the segment file does not contain truncated or corrupted data
- Test with `chunkSegmentLength = 4096` and values that fill the segment to ~98%+ capacity

---

## Finding 2: Big string overwrite old file not deleted when `needPersist=true` — delayed cleanup

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1866-1913` (big string overwrite + WAL put)
- `src/main/java/io/velo/persist/Wal.java:944-972` (`Wal.put` — early return when buffer full)

**Code excerpt:**

```java
// OneSlot.put() — line 1866-1913
overwrittenBigStringUuid = getCurrentBigStringUuid(targetWal, key, bucketIndex, cv.getKeyHash());
// ... big string file written with NEW UUID ...
var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs); // line 1908

// Overwrite detection — line 1909-1913
if (overwrittenBigStringUuid != null) {
    var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
    if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
        delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(overwrittenBigStringUuid, ...));
    }
}
```

```java
// Wal.put() — when buffer is full (lines 960-971)
if (offset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
    // ... rewrite attempt ...
    if (needPersist) {
        return new PutResult(true, isValueShort, v, v, 0); // early return BEFORE map insertion
    }
}
// Map insertion happens AFTER this point (lines 989-990 / 1015-1016)
```

**Root cause:**

When `Wal.put()` returns `needPersist=true`, the new value is **not** added to the WAL's in-memory maps. Specifically, `addBigStringUuidIfMatch(v)` (line 1010) is never called, so `bigStringFileUuidByKey` still maps `key → OLD_UUID`.

The overwrite detection compares:
- `overwrittenBigStringUuid` = OLD UUID (read before `Wal.put()`)
- `currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key)` = OLD UUID (not updated)

Since they are **equal**, the old big string file is **not** scheduled for deletion via `delayToDeleteBigStringFileIds`.

After `doPersist()` succeeds:
1. The WAL is cleared (`clearShortValues()` clears `bigStringFileUuidByKey`)
2. `needPutV` is re-put to the WAL, adding the NEW UUID to `bigStringFileUuidByKey`
3. On the **next** persist cycle, the new value is persisted to key buckets
4. The old key bucket entry (with OLD UUID) is overwritten, triggering `cvExpiredOrDeleted()`
5. `BigStringFiles.handleWhenCvExpiredOrDeleted()` deletes the old file

**Impact:**

- The old big string file persists on disk for one extra persist cycle (typically milliseconds to seconds)
- During this window, the old file is still referenced by the key bucket, so reads return the correct (new) value via the WAL
- On crash recovery, the WAL replay correctly overwrites the old key bucket entry and deletes the old file
- **Not a permanent leak** — the file is always cleaned up on the next successful persist or on recovery
- The practical impact is minimal; this finding is documented for completeness and to distinguish it from Bug 22 Finding 4 (which covers the case where `doPersist()` **fails**)

**Suggested fix direction:**

Move the overwrite detection to **after** `doPersist()` completes and `needPutV` has been re-inserted:

```java
if (putResult.needPersist()) {
    doPersist(walGroupIndex, key, putResult);
    // Now check overwrite after the new UUID is in bigStringFileUuidByKey
    if (overwrittenBigStringUuid != null) {
        var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
        if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
            delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(overwrittenBigStringUuid, ...));
        }
    }
}
```

Or perform the overwrite detection unconditionally in `doPersist()` after the re-put succeeds.

---

## Non-Findings (Verified Safe)

1. **`Wal.exists()` does not check `isRemove()` for long values** — The method only checks `isRemove()` for short values (in `delayToKeyBucketShortValues`), not for long values (in `delayToKeyBucketValues`). This is safe because deletion markers are always created as short values via `removeDelay()` (line 762-768), and `Wal.put()` atomically removes the key from the other map (lines 989-990 / 1015-1016). A key can only be in one map at a time, and deletion markers are always in the short-value map where `isRemove()` is checked.

2. **CompressedValue encode/decode roundtrip in merge path** — In `putValueToWal()` (line 2343-2344), merged segment values are decoded with `CompressedValue.decode()` and re-encoded with `cv.encode()`. This is safe because:
   - Chunk segments only contain **normal (long) values** — short values go to key buckets, big strings to separate files
   - Normal values are always encoded with `encode()`, which writes: `seq(8) + expireAt(8) + keyHash(8) + dictSeqOrSpType(4) + compressedLength(4) + compressedData`
   - `decode()` for normal values (first byte ≥ 0) preserves all these fields, making the roundtrip lossless

3. **`SegmentBatch2.split()` empty segment** — The uncompressed split path can theoretically create an empty segment when the first entry exactly fills the segment capacity. However, the big-string check in `OneSlot.put()` (line 1864) prevents any single entry from exceeding `chunkSegmentLength - SEGMENT_HEADER_LENGTH`, so the empty-segment case cannot occur in the uncompressed path.

4. **`KeyBucket.getFromOneCell()` byte value length** — Value length in key bucket cells is stored as a signed byte (max 127). All current value types (PVM = 12 bytes, short string ≤ 33 bytes, number ≤ 25 bytes, big string meta = 44 bytes) are well within this limit. The `cellCostInKeyBucket()` method already guards against oversized values with a `Byte.MAX_VALUE` check.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - SegmentBatch tight segment overflow on incompressible data | **High** | **Fixed** (commit `3bf4137`) | High |
| 2 - Big string overwrite delayed cleanup when `needPersist=true` | Low | **Fixed** — overwrite detection moved to after `doPersist()` so the new UUID is visible in `bigStringFileUuidByKey` | High |

---

## AI Agent 2 Review Notes

Reviewer: AI agent 2

Review date: 2026-05-08

### Finding 1 Review: Confirmed

**Status:** Confirmed

The current code still has the overflow path described by the author:

- `Chunk.persist()` selects `segmentBatch.split()` when `ConfForSlot.global.confChunk.isSegmentUseCompression` is true (`src/main/java/io/velo/persist/Chunk.java:371-372`).
- `SegmentBatch.compressAsSegment()` always calls `Zstd.compress(bytes)` on the full `chunkSegmentLength` buffer and returns the compressed bytes without checking whether compression expanded the payload (`src/main/java/io/velo/persist/SegmentBatch.java:338-354`).
- `SegmentBatch.tight()` flushes when `onceListBytesLength + compressedBytes.length > chunkSegmentLength - HEADER_LENGTH`, but it does not check whether `onceList` is empty before creating a tight segment (`src/main/java/io/velo/persist/SegmentBatch.java:246-257`).
- `SegmentBatch.tightSegments()` allocates `HEADER_LENGTH + sum(compressedBytes.length)` bytes and records that total length in the tight segment header (`src/main/java/io/velo/persist/SegmentBatch.java:200-227`).
- `Chunk.writeSegments()` writes each produced segment through `FdReadWrite.writeOneInner()` (`src/main/java/io/velo/persist/Chunk.java:397-402`, `src/main/java/io/velo/persist/Chunk.java:569-580`), and `writeOneInner()` throws when `bytes.length > oneInnerLength` (`src/main/java/io/velo/persist/FdReadWrite.java:832-835`).

I also verified the size premise with the repo's Zstd JNI dependency in JShell: compressing 4096 random bytes produced 4106 bytes, and compressing a 4096-byte buffer with 4060 random bytes plus zero tail also produced 4106 bytes. With `SegmentBatch.HEADER_LENGTH == 29`, the resulting tight segment would be 4135 bytes, larger than a 4096-byte segment. The first oversized sub-block also causes an empty tight segment to be emitted before the oversized one.

One refinement: the suggested fallback cannot simply return raw NORMAL segment bytes from the existing `SegmentBatch.split()` return type without changing the reader/write path assumptions. In compressed mode, the returned `SegmentBatch2.SegmentBytesWithIndex` values are interpreted as already formed chunk segments, and `SegmentBatch2.iterateFromSegmentBytes()` distinguishes TIGHT versus NORMAL by the segment type byte. A fix can use mixed NORMAL/TIGHT segments, but it must ensure the PVM `subBlockIndex` and segment type semantics stay valid for `SegmentBatch2.iterateFromSegmentBytes()`.

### Finding 2 Review: Confirmed With Narrowed Trigger

**Status:** Confirmed as a low-severity delayed-cleanup issue

The finding is valid for the WAL-buffer-full early-return path:

- `OneSlot.put()` reads the old big-string UUID before calling `targetWal.put()` (`src/main/java/io/velo/persist/OneSlot.java:1848`, `src/main/java/io/velo/persist/OneSlot.java:1868`, `src/main/java/io/velo/persist/OneSlot.java:1896`).
- `Wal.put()` returns `new PutResult(true, ..., v, v, 0)` before writing the WAL file, before inserting into `delayToKeyBucketShortValues`, and before calling `addBigStringUuidIfMatch(v)` when `offset + encodeLength > ONE_GROUP_BUFFER_SIZE` and persistence is required (`src/main/java/io/velo/persist/Wal.java:944-972`).
- `OneSlot.put()` performs overwrite cleanup detection immediately after `targetWal.put()` (`src/main/java/io/velo/persist/OneSlot.java:1908-1913`), so in this early-return case the map can still contain the old UUID and the old file is not queued for immediate deletion.
- `OneSlot.doPersist()` later clears the short-value WAL state and re-puts `needPutV` (`src/main/java/io/velo/persist/OneSlot.java:1979-1995`), which makes the new UUID visible in `bigStringFileUuidByKey` only after the immediate overwrite check has already run.

The trigger should be described more narrowly than "`needPersist=true`" in general. When `needPersist` is triggered after a successful map insertion because the short-value count reaches the threshold, `Wal.put()` has already called `addBigStringUuidIfMatch(v)` (`src/main/java/io/velo/persist/Wal.java:988-1012`), so the immediate overwrite check can queue the old UUID correctly. The bug is specifically the early `needPersist` return caused by WAL buffer capacity.

Impact remains low. If the old big string is already persisted and not present in the WAL UUID map, the immediate comparison sees `currentBigStringUuid == null` and queues deletion. If the old UUID is still in the WAL map, cleanup is delayed until a later key-bucket update callback (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:287-293`) or interval cleanup path (`src/main/java/io/velo/persist/OneSlot.java:1248-1292`) can delete it.

### Non-Findings Review

**Wal.exists(): confirmed safe.** `removeDelay()` always creates deletion markers as short values (`src/main/java/io/velo/persist/Wal.java:762-768`), `exists()` checks `isRemove()` for short values (`src/main/java/io/velo/persist/Wal.java:776-784`), and `Wal.put()` removes the key from the opposite map after normal insertion (`src/main/java/io/velo/persist/Wal.java:988-990`, `src/main/java/io/velo/persist/Wal.java:1015-1017`).

**CompressedValue merge roundtrip: confirmed safe for normal long values.** `putValueToWal()` re-encodes merged chunk values with `cv.encode()` (`src/main/java/io/velo/persist/OneSlot.java:2343-2344`), and the normal decode/encode path preserves seq, expireAt, keyHash, dict/type, compressed length, and compressed data (`src/main/java/io/velo/CompressedValue.java:770-781`, `src/main/java/io/velo/CompressedValue.java:867-897`).

**SegmentBatch2 empty segment: safe but the stated reason is slightly too strong.** The long-value big-string guard uses `>` rather than `>=` (`src/main/java/io/velo/persist/OneSlot.java:1862-1867`), while `SegmentBatch2.split()` flushes when `persistLength < bytes.length` is false (`src/main/java/io/velo/persist/SegmentBatch2.java:109-127`). An exactly-full first entry can therefore still emit an empty NORMAL segment before the real segment. That is a segment-slot waste rather than a data-loss bug because the empty segment has no PVM entries and the real value receives the next temporary segment index.

**KeyBucket byte value length: not re-reviewed in depth in this pass.** The size statement is plausible from the current value encodings, but I did not trace every key-bucket cell writer in this review because it is outside the two confirmed findings.

---

## Review Feedback After Bug 1 Fix Commit

Reviewer: AI agent 2

Reviewed commit: `cf75be1 fix: handle incompressible data in SegmentBatch to prevent tight segment overflow`

Review date: 2026-05-08

### Summary Of Fix

The commit fixes Finding 1 by letting `SegmentBatch` fall back to a raw `NORMAL` segment when Zstd expands a full segment buffer. Raw segments are written directly instead of being wrapped in a TIGHT header, and the tight-packing loop now avoids emitting empty TIGHT segments when a flush happens with an empty `onceList`.

### Strengths

- The root overflow condition is addressed at the right boundary: `compressAsSegment()` detects `compressedBytes.length >= bytes.length` before tight packing can add another header and exceed `chunkSegmentLength`.
- The raw fallback preserves the existing NORMAL segment format produced by `SegmentBatch2.encodeToBuffer()`, so existing NORMAL readers such as `SegmentBatch2.iterateFromSegmentBytes()` can still parse the bytes.
- The `tight()` changes preserve PVM segment-index remapping for raw fallback segments and prevent the previously described empty TIGHT segment in the oversized-first-block path.
- The regression test covers the incompressible-data write-size failure mode and verifies all produced segment byte arrays fit within `chunkSegmentLength`.

### Concerns

- No blocking correctness issue found in the committed production code for Finding 1.
- Coverage still misses the new raw NORMAL read branch in `SegmentBatch.decompressSegmentBytesFromOneSubBlock()` (`build/reports/jacocoHtml/io.velo.persist/SegmentBatch.java.html` shows line 412 as not covered). The current regression test proves the write-side overflow is avoided, but it does not decode/read back the raw fallback segment through the `decompressSegmentBytesFromOneSubBlock()` path used by `OneSlot.get()` in compression mode.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.SegmentBatchTest"`: passed.
- Inspected JaCoCo HTML for `SegmentBatch`: `compressAsSegment()` has 100% method coverage and both branches of `compressedBytes.length >= bytes.length` are covered; `tight()` covers the `segment.isRaw` branch; `decompressSegmentBytesFromOneSubBlock()` has partial branch coverage and the raw NORMAL return branch is not covered.

### Follow-Ups

- ~~Add a focused test that takes the raw fallback segment, calls `SegmentBatch.decompressSegmentBytesFromOneSubBlock()` with its PVM, and verifies `SegmentBatch2.iterateFromSegmentBytes()` or direct offset decode can read the original key/value. This would close the only meaningful coverage gap for the committed fix.~~ **Resolved:** the incompressible-data test now reads back through `decompressSegmentBytesFromOneSubBlock()` and verifies entry decode via `iterateFromSegmentBytes`. JaCoCo confirms 17% instruction coverage and 30% branch coverage for `decompressSegmentBytesFromOneSubBlock` (raw NORMAL return path covered).

---

## Second Review Feedback After Bug 1 Fix Commit

Reviewer: AI agent 2

Reviewed commit: `3bf4137 fix: handle incompressible data in SegmentBatch to prevent tight segment overflow`

Review date: 2026-05-08

### Findings

No blocking issues found in the current Bug 1 fix.

### Review Notes

The current `SegmentBatchTest` now covers the readback gap called out in the first review. The incompressible-data test
calls `SegmentBatch.decompressSegmentBytesFromOneSubBlock(...)` on the raw fallback segment and then decodes it through
`SegmentBatch2.iterateFromSegmentBytes(...)`, verifying the original sequence and key hash.

The latest HEAD commit, `abea63a`, is documentation-only and does not change the Bug 1 production or test code.

### Verification

Command run fresh:

```bash
./gradlew :test --tests "io.velo.persist.SegmentBatchTest" --rerun-tasks
```

Result: passed with `BUILD SUCCESSFUL`; `SegmentBatchTest` ran 2 tests, 0 failures, 0 ignored.

JaCoCo inspection after the fresh run:

- `SegmentBatch.java:254` (`segment.isRaw`) covered with all 2 branches covered.
- `SegmentBatch.java:380` (`compressedBytes.length >= bytes.length`) covered with all 2 branches covered.
- `SegmentBatch.java:412` (raw NORMAL return from `decompressSegmentBytesFromOneSubBlock`) covered.
- `SegmentBatch.decompressSegmentBytesFromOneSubBlock(...)` now reports 78% instruction coverage and 60% branch coverage.

### Conclusion

Bug 1 can be treated as fixed and covered for the reviewed failure mode: incompressible segment data no longer creates an
oversized TIGHT segment, empty TIGHT output is avoided for the oversized-first-block path, and the raw fallback segment is
readable through the compression-mode read helper.

---

## Finding 2 Fix

**Fix approach:** Move the big-string overwrite detection (`overwrittenBigStringUuid` check) from before `doPersist()` to after it in `OneSlot.put()` (`src/main/java/io/velo/persist/OneSlot.java:1908-1930`).

**Why it works:** When `Wal.put()` returns `needPersist=true` via the buffer-full early return, the new UUID is not yet in `bigStringFileUuidByKey`. After `doPersist()` completes, the new value has been re-put to the WAL and `addBigStringUuidIfMatch()` has updated `bigStringFileUuidByKey` with the new UUID. The overwrite check now correctly sees `oldUuid != newUuid` and schedules the old file for deletion.

**When `needPersist=false`:** The overwrite check runs immediately after `Wal.put()`, which is correct because the new UUID was added to `bigStringFileUuidByKey` during `put()`. Moving the check after the `if (putResult.needPersist())` block is a no-op in this case since the block is skipped.

**Test:** `OneSlotTest.'test overwrite big string when WAL buffer full schedules old file for deletion'` — manipulates `writePositionShortValue` to near `ONE_GROUP_BUFFER_SIZE` to trigger the buffer-full early return, then verifies the old big-string file is scheduled for deletion and the new UUID is in the WAL map.

---

## Review Feedback After Finding 2 Fix Commit

Reviewer: AI agent 2

Reviewed commit: `eef81fd fix: move big-string overwrite detection after doPersist()`

Review date: 2026-05-08

### Findings

No blocking issues found in the committed Finding 2 fix.

### Summary Of Fix

The commit moves `overwrittenBigStringUuid` cleanup detection in `OneSlot.put(...)` from immediately after
`targetWal.put(...)` to after the optional `doPersist(...)` call. This addresses the buffer-full early-return path where
`Wal.put(...)` returns `needPersist=true` before inserting the new big-string UUID into `bigStringFileUuidByKey`.

### Strengths

- The moved check now observes the post-`doPersist(...)` WAL state, so the `needPutV` re-put path has populated
  `bigStringFileUuidByKey` with the new UUID before the old-vs-current UUID comparison.
- The new regression test directly drives the buffer-full early return by setting `writePositionShortValue` near
  `Wal.ONE_GROUP_BUFFER_SIZE` with `isOnRewrite=false`.
- Existing overwrite-focused tests still pass, covering non-buffer-full overwrite behavior.

### Concerns

- JaCoCo still reports one missed branch at `OneSlot.java:1927`, the case where `overwrittenBigStringUuid` equals the
  current UUID and no deletion is enqueued. This is not part of the fixed failure mode, but it remains a small branch
  coverage gap around the moved condition.

### Verification

Commands run fresh:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest.test overwrite big string when WAL buffer full schedules old file for deletion" --rerun-tasks
./gradlew :test --tests "io.velo.persist.OneSlotTest.test overwrite*" --rerun-tasks
```

Results:

- The focused buffer-full regression test passed with `BUILD SUCCESSFUL`.
- The overwrite-focused test set ran 4 tests, 0 failures, 0 ignored.

JaCoCo inspection after the overwrite-focused run:

- `OneSlot.java:1911` (`putResult.needPersist()`) covered with both branches covered.
- `OneSlot.java:1922` (`doPersist(...)`) covered.
- `OneSlot.java:1925` (`overwrittenBigStringUuid != null`) covered with both branches covered.
- `OneSlot.java:1928` (`delayToDeleteBigStringFileIds.add(...)`) covered.
- `OneSlot.java:1982`, `1986`, and `1993` covered for the short-value `doPersist(...)` and `needPutV` re-put path.

### Conclusion

Finding 2 can be treated as fixed and covered for the reviewed buffer-full delayed-cleanup bug. The old big-string file is
now scheduled for deletion after `doPersist(...)` has reinserted the new big-string UUID into the WAL map.
