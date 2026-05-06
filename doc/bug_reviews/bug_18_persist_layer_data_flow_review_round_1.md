# Bug 18 Persist Layer Data Flow Review Round 1

Author: AI agent 1

## Scope

Static review of the persist-layer data flow after reading the current design documents and earlier persist bug-review
rounds. This round focuses on the key-bucket merge/write path, where WAL values and already-persisted key-bucket entries
are reconciled before the updated key-bucket pages are written back.

Design documents reviewed:

- `doc/design/01_overall_architecture.md`
- `doc/design/02_persist_layer_design.md`
- `doc/design/07_compression_design.md`
- `doc/review_persist_layer_bugs_summary.md`

Prior persist reviews checked to avoid repeating already reviewed findings:

- `doc/bug_reviews/bug_11_persist_layer_review_round_1.md`
- `doc/bug_reviews/bug_11_persist_layer_review_round_2.md`
- `doc/bug_reviews/bug_12_persist_merge_data_flow_review.md`
- `doc/bug_reviews/bug_13_persist_read_data_flow_review_round_1.md`
- `doc/bug_reviews/bug_14_persist_read_data_flow_review_round_2.md`
- `doc/bug_reviews/bug_15_persist_truncate_chunk_fd_review_round_1.md`
- `doc/bug_reviews/bug_16_persist_data_flow_review_round_3.md`
- `doc/bug_reviews/bug_17_persist_read_data_flow_review_round_4.md`

## Persist Data Flow

### Write / Persist Path

1. Commands write through `OneSlot.put(...)`.
2. `OneSlot.put(...)` chooses a WAL group from the bucket index and appends a `Wal.V` into either the normal-value WAL
   map/file or the short-value WAL map/file.
3. If the WAL group crosses persistence thresholds, `OneSlot.doPersist(...)` calls `putValueToWal(...)`.
4. Short values go directly to `KeyLoader.persistShortValueListBatchInOneWalGroup(...)`.
5. Normal values go through `Chunk.persist(...)`; after segment bytes are written, `KeyLoader.updatePvmListBatchAfterWriteSegments(...)`
   stores `PersistValueMeta` pointers in key buckets.
6. `KeyBucketsInOneWalGroup.putPvmListToTargetBucket(...)` is the reconciliation point. It reads existing split key
   buckets into a map, overlays WAL/PVM updates, decides whether the bucket must split, clears target key buckets, and
   writes every surviving map entry back.
7. `KeyLoader.doAfterPutAll(...)` writes updated key-bucket pages, updates split metadata and key counts, then clears the
   affected slot KV LRU.

### Read Path

1. `OneSlot.get(...)` checks WAL first.
2. If WAL has no live value, it checks the per-WAL-group slot KV LRU.
3. If LRU misses, it queries `KeyLoader.getValueXByKey(...)`.
4. Inline short values are decoded from key-bucket bytes; `PersistValueMeta` values are resolved through chunk segments;
   big-string metadata is resolved through `BigStringFiles`.
5. The command layer performs the final expiry/type handling after decoding the returned compressed value.

### Cleanup Path

1. Expired/deleted short values are reported through `KeyBucket.CvExpiredOrDeletedCallBack`.
2. The callback reaches `OneSlot.handleWhenCvExpiredOrDeleted(...)`, then `BigStringFiles.handleWhenCvExpiredOrDeleted(...)`
   for big-string cleanup.
3. Merge and batch-persist paths must remove expired/deleted entries from key-bucket state as well as triggering cleanup,
   otherwise stale metadata keeps flowing through future persistence rounds.

## Finding 1: Expired persisted entries are cleaned up, then inserted back into the key-bucket map

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:236-248`
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:308-336`
- `src/main/java/io/velo/persist/KeyBucket.java:478-490`

**Code excerpt:**

```java
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        cvExpiredOrDeleted(key, valueBytes);
    }

    var pvm = new PersistValueMeta();
    pvm.keyHash = keyHash;
    pvm.expireAt = expireAt;
    pvm.seq = seq;
    pvm.key = key;
    pvm.extendBytes = valueBytes;
    map.put(key, pvm);
});
```

```java
for (var entry : map.entrySet()) {
    var pvm = entry.getValue();
    var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
    ...
    keyBucket.put(pvm.key, pvm.keyHash, pvm.expireAt, pvm.seq,
            pvm.extendBytes != null ? pvm.extendBytes : pvm.encode(), false);
}

keyCountForStatsTmp[relativeBucketIndex] = (short) map.size();
```

**Root cause:**

`putPvmListToTargetBucket(...)` identifies expired entries while loading existing key buckets and calls
`cvExpiredOrDeleted(...)`, but it does not skip those entries. The same expired entry is put into `map` and later written
back to the cleared key bucket.

This contradicts the lower-level cleanup behavior in `KeyBucket.clearAllExpired()`, which actually clears expired cells
instead of carrying them forward.

**Impact:**

Expired key-bucket metadata can survive every merge/persist round. Reads generally filter expired values, so this is not
an immediate stale-read bug, but it is still a persist-layer correctness problem:

- key-count stats can include expired entries because `keyCountForStatsTmp` is set from `map.size()`;
- expired entries continue consuming bucket cells and can force unnecessary splits;
- a TTL-heavy bucket can eventually hit `BucketFullException` even when the live-key count is below capacity;
- expired big-string metadata can repeatedly trigger cleanup while remaining in key-bucket state.

**Suggested fix direction:**

After `cvExpiredOrDeleted(key, valueBytes)`, return from the iterator callback before building and inserting the `PersistValueMeta`.
A focused test should create an expired persisted entry, run `putPvmListToTargetBucket(...)` with no replacement for that
key, and assert that the rewritten key bucket no longer contains the expired key and the stats count excludes it.

## Finding 2: Split decision counts entries, not key-bucket cells, so multi-cell values can fail instead of splitting

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:271-280`
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:320-333`
- `src/main/java/io/velo/persist/KeyBucket.java:571-580`
- `src/main/java/io/velo/persist/PersistValueMeta.java:114-120`

**Code excerpt:**

```java
var cellCostBySplitIndex = new int[currentSplitNumber];
for (var entry : map.entrySet()) {
    var pvm = entry.getValue();

    var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
    cellCostBySplitIndex[splitIndex]++;
    if (cellCostBySplitIndex[splitIndex] > KeyBucket.INIT_CAPACITY) {
        needSplit = true;
        break;
    }
}
```

```java
int cellCount = KVMeta.calcCellCount((short) keyBytes.length, (byte) valueBytes.length);
if (cellCount >= INIT_CAPACITY) {
    throw new IllegalArgumentException("Key with value bytes too large, key length=" + keyBytes.length
            + ", value length=" + valueBytes.length);
}
```

```java
int cellCostInKeyBucket() {
    var valueLength = extendBytes != null ? extendBytes.length : ENCODED_LENGTH;
    if (valueLength > Byte.MAX_VALUE) {
        throw new IllegalArgumentException("Persist value meta extend bytes too long=" + valueLength);
    }
    return KeyBucket.KVMeta.calcCellCount((short) Wal.keyBytes(key).length, (byte) valueLength);
}
```

**Root cause:**

The split pre-check increments `cellCostBySplitIndex` by `1` per key. But the key-bucket storage model is cell-based, and
a single key can consume multiple cells depending on key length and value metadata length. `KeyBucket.put(...)` enforces
the real cell count through `KVMeta.calcCellCount(...)`, and `PersistValueMeta` already exposes `cellCostInKeyBucket()`.

Because the pre-check undercounts multi-cell entries, `needSplit` can remain false even when the target split has more
than 48 cells of data. The later `keyBucket.put(...)` calls then fail with `BucketFullException` after the method has
already decided not to split.

**Impact:**

A bucket with enough multi-cell entries can fail persistence even though increasing the split number could make the same
data fit. This is reachable for long keys and larger inline metadata such as big-string compressed-value metadata; the
class comment in `KeyBucket` explicitly notes that one key may cost two cells. The failure interrupts the WAL persist
path, which can stall writes for the affected WAL group.

**Suggested fix direction:**

Use the actual cell cost when evaluating split pressure:

```java
cellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
```

The split decision should also be rechecked after increasing `currentSplitNumber`, because one split step may still be
insufficient for a skewed bucket. A focused regression test should build a bucket with multi-cell entries whose key count
is below `INIT_CAPACITY` but whose total cell cost exceeds it; the old code should throw `BucketFullException`, while the
fixed code should split and persist successfully.

## Reviewer Notes (AI agent 2)

### Finding 1 Verification — Confirmed

Reviewed `KeyBucketsInOneWalGroup.java:236-248`. The iterate callback at line 236 does:

```java
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        cvExpiredOrDeleted(key, valueBytes);
    }
    // no return/skip here — expired entry flows through
    var pvm = new PersistValueMeta();
    ...
    map.put(key, pvm);  // expired entry inserted into map
});
```

The expired entry is not skipped after `cvExpiredOrDeleted(...)`. It is unconditionally wrapped in a `PersistValueMeta` and placed into `map`. At line 320-334, every entry in `map` is written back into the cleared key buckets. At line 336, `keyCountForStatsTmp[relativeBucketIndex] = (short) map.size()` includes the expired entries, inflating the stat that `KeyLoader.doAfterPutAll(...)` propagates to `updateKeyCountBatch(...)`.

Contrast with `KeyBucket.clearAllExpired()` (line 478-492), which actually removes expired cells from the bucket — the merge path does not follow the same discipline.

**Verdict:** Confirmed. The fix direction (return after `cvExpiredOrDeleted`) is correct.

Additional observation: expired entries in `map` also inflate the split pre-check in Finding 2 (they count toward the entry-based threshold), making an unnecessary split slightly more likely while still wasting bucket cells.

### Finding 2 Verification — Confirmed

Reviewed `KeyBucketsInOneWalGroup.java:273-278`:

```java
cellCostBySplitIndex[splitIndex]++;  // counts entries, not cells
if (cellCostBySplitIndex[splitIndex] > KeyBucket.INIT_CAPACITY) {  // INIT_CAPACITY = 48 cells
```

The check increments by `1` per key but compares against `KeyBucket.INIT_CAPACITY` (48), which is the **cell** capacity. A single key can cost multiple cells: `KVMeta.calcCellCount(...)` in `KeyBucket.java:355-362` shows that keys with longer key bytes or value bytes (e.g. big-string compressed-value metadata at 48 bytes) cost 2 cells. `PersistValueMeta.cellCostInKeyBucket()` already computes the correct cell cost.

For example, 30 keys each costing 2 cells = 60 cells. The pre-check sees only 30 (< 48) and decides no split is needed. Then `keyBucket.put(...)` at line 327-328 fails with `BucketFullException` because 60 cells exceeds the 48-cell capacity.

The suggested fix (`cellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket()`) is correct. The note about rechecking after increasing `currentSplitNumber` is also valid — a single split step (multiplying by `SPLIT_MULTI_STEP = 3`) may still not be enough for highly skewed multi-cell buckets.

**Verdict:** Confirmed. The fix direction is correct.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Expired persisted entries are reinserted during merge | High | **Confirmed** | High |
| 2 - Split decision ignores multi-cell cost | High | **Confirmed** | High |

## Review Feedback - Bug 1 Committed Fix `d9747b7`

Reviewed by: AI agent 1
Date: 2026-05-06

### Summary of the fix

Commit `d9747b7` adds an early `return` after `cvExpiredOrDeleted(key, valueBytes)` inside
`KeyBucketsInOneWalGroup.putPvmListToTargetBucket(...)`. This prevents expired entries loaded from existing key buckets
from being wrapped into `PersistValueMeta`, inserted into `map`, and written back during merge.

The commit also adds `KeyBucketsInOneWalGroupTest.test expired entries are not reinserted during merge`, which creates
an expired key-bucket entry, reloads it through the merge path, and verifies that the expired key is absent afterward and
the temporary key count drops from 5 to 4.

### Strengths

- The production change targets the confirmed root cause directly.
- The new test exercises the important two-round flow: first persist an expired existing entry, then reload and merge the
  bucket so the new `return` path is reached.
- The test checks both lookup behavior and `keyCountForStatsTmp`, covering the stale metadata and inflated-count effects.
- Fresh verification passed with `./gradlew :cleanTest :test --tests "io.velo.persist.KeyBucketsInOneWalGroupTest"`.
- JaCoCo confirms the new branch executed: `build/reports/jacocoHtml/io.velo.persist/KeyBucketsInOneWalGroup.java.html`
  marks lines 238-239, including the new `return`, as covered.

### Findings

No blocking issue found in the committed Bug 1 fix. The patch closes the confirmed defect where expired entries already
present in persisted key buckets were cleaned up and then immediately reinserted during merge.

### Residual Concern

**Medium: expired incoming WAL/PVM entries can still be written into key buckets.**

The committed fix only skips expired entries while loading existing key-bucket state:

```java
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        cvExpiredOrDeleted(key, valueBytes);
        return;
    }
    ...
});
```

The subsequent `pvmListThisBucket` overlay still inserts any non-delete `PersistValueMeta` regardless of whether its
`expireAt` is already in the past:

```java
if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
    ...
} else {
    var existPvm = map.put(pvm.key, pvm);
    ...
}
```

The new regression test demonstrates this adjacent gap: the first round persists an already-expired incoming
`Wal.V` and asserts `inner.keyCountForStatsTmp[0] == 5`. The second round then proves the committed fix removes it once
it is encountered as an existing persisted entry.

This is not a regression in `d9747b7`, but it means expired values can still enter key-bucket storage from the incoming
WAL/PVM side and remain there until a later merge of the same bucket. A follow-up should decide whether
`putPvmListToTargetBucket(...)` should also skip `pvmListThisBucket` entries whose `expireAt` is in the past and not just
`EXPIRE_NOW` tombstones.

### Follow-ups

1. Treat Bug 1 as fixed for the originally confirmed persisted-entry reinsertion path.
2. Add a separate follow-up bug or patch for already-expired incoming WAL/PVM entries if the intended invariant is that
   expired values should never be written into key buckets.
3. Bug 2 remains open; this commit does not address the split pre-check counting entries instead of cell cost.

## Review Feedback - Bug 2 Committed Fix `d65467e`

Reviewed by: AI agent 1
Date: 2026-05-06

### Summary of the fix

Commit `d65467e` changes the split pre-check in `KeyBucketsInOneWalGroup.putPvmListToTargetBucket(...)` from counting
one unit per entry to adding each entry's actual key-bucket cell cost:

```java
cellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
```

It also adds `KeyBucketsInOneWalGroupTest.test split decision uses cell cost not entry count`, which creates 25
two-cell entries. Under the old entry-count pre-check, that dataset had 25 entries but 50 cells, so it could fail without
splitting. With the fix, the method splits and all entries remain readable.

### Finding

**High: split pressure is still not rechecked after increasing the split number.**

The committed fix correctly detects that the current split number is over capacity:

```java
var cellCostBySplitIndex = new int[currentSplitNumber];
for (var entry : map.entrySet()) {
    var pvm = entry.getValue();

    var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
    cellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
    if (cellCostBySplitIndex[splitIndex] > KeyBucket.INIT_CAPACITY) {
        needSplit = true;
        break;
    }
}
```

But once `needSplit` is true, the code multiplies `currentSplitNumber` by `SPLIT_MULTI_STEP`, sets the new split number,
and immediately starts writing entries:

```java
var newMaxSplitNumber = currentSplitNumber * KeyLoader.SPLIT_MULTI_STEP;
...
currentSplitNumber = (byte) newMaxSplitNumber;
splitNumberTmp[relativeBucketIndex] = (byte) newMaxSplitNumber;
isSplit = true;
...
var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
...
var doPutResult = keyBucket.put(...);
if (!doPutResult.isPut()) {
    throw new BucketFullException(...);
}
```

There is no second cell-cost pass for the new split number. A bucket that is over capacity at split `1`, still over
capacity in one of the split `3` buckets, but would fit at split `9`, can still throw `BucketFullException` after only
one split step. This leaves part of Bug 2 open: multi-cell data can still fail persistence even when a larger configured
split number could make it fit.

The new regression test does not cover this case because its 25 two-cell entries fit after one split step. It verifies
the entry-count-to-cell-cost correction, but not the "recheck after increasing `currentSplitNumber`" requirement from
the suggested fix direction.

### Verification

- Ran `./gradlew :cleanTest :test --tests "io.velo.persist.KeyBucketsInOneWalGroupTest"` successfully.
- JaCoCo confirms the changed cell-cost line is covered:
  `build/reports/jacocoHtml/io.velo.persist/KeyBucketsInOneWalGroup.java.html` marks line 279 as covered.
- The added test is present and passed in `build/reports/tests/test/classes/io.velo.persist.KeyBucketsInOneWalGroupTest.html`.

### Follow-ups

1. Rework the split decision so it evaluates capacity for the candidate split number and repeats until all target split
   buckets fit or `MAX_SPLIT_NUMBER` is reached.
2. Add a regression test where one split step is insufficient but a later split step fits; the old code should throw
   `BucketFullException`, and the corrected code should split again and persist successfully.

## Review Feedback - Bug 2 Follow-up Fix `a04e528`

Reviewed by: AI agent 1
Date: 2026-05-06

### Summary of the fix

Commit `a04e528` replaces the one-step split decision with a loop that evaluates cell cost for the current candidate
split number, advances by `SPLIT_MULTI_STEP`, and repeats until either every target split bucket fits or the next split
would exceed `MAX_SPLIT_NUMBER`.

The commit also adds `KeyBucketsInOneWalGroupTest.test split rechecked after increasing split number`, which builds a
controlled case where split `1` and split `3` are still over capacity, while split `9` fits.

### Findings

No blocking issues found in the follow-up fix. The new loop closes the prior review finding: split pressure is now
rechecked after increasing the split number, and the code can advance from `1` to `3` to `9` before writing entries.

### Verification

- Ran `./gradlew :cleanTest :test --tests "io.velo.persist.KeyBucketsInOneWalGroupTest"` successfully.
- JaCoCo confirms the new loop is covered in
  `build/reports/jacocoHtml/io.velo.persist/KeyBucketsInOneWalGroup.java.html`:
  - `if (!overCapacity)` line 286 has both branches covered.
  - `if (newMaxSplitNumber > KeyLoader.MAX_SPLIT_NUMBER)` line 291 has both branches covered.
  - `if (targetSplitNumber > currentSplitNumber)` line 301 has both branches covered.
  - `splitNumberTmp[relativeBucketIndex] = targetSplitNumber` line 309 is covered.
- The new regression test is present and passed in
  `build/reports/tests/test/classes/io.velo.persist.KeyBucketsInOneWalGroupTest.html`.

### Notes

The new regression test intentionally reuses a small set of controlled `keyHash` values across different keys, so the
test emits `Key hash conflict` messages on stderr while probing cells. That noise is expected from the artificial setup
and did not indicate a production-code issue in this review.
