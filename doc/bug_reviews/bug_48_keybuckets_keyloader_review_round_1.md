# Bug 48: KeyBucketsInOneWalGroup / KeyLoader Review — Round 1

**Author:** AI Agent 1
**Date:** 2026-06-12
**Module:** persist
**Classes:**
- `io.velo.persist.KeyBucketsInOneWalGroup` (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`, 434 lines)
- `io.velo.persist.KeyLoader` (`src/main/java/io/velo/persist/KeyLoader.java`, 1123 lines)

**Related classes reviewed for context:**
- `io.velo.persist.KeyBucket` (`src/main/java/io/velo/persist/KeyBucket.java`, 751 lines)
- `io.velo.persist.PersistValueMeta` (`src/main/java/io/velo/persist/PersistValueMeta.java`, 160 lines)
- `io.velo.persist.FdReadWrite` (`src/main/java/io/velo/persist/FdReadWrite.java`, 861 lines)
- `io.velo.persist.MetaKeyBucketSplitNumber` (`src/main/java/io/velo/persist/MetaKeyBucketSplitNumber.java`, 162 lines)
- `io.velo.KeyHash` (`src/main/java/io/velo/KeyHash.java`, 139 lines)
- `io.velo.ConfForSlot` (`src/main/java/io/velo/ConfForSlot.java`)

**Design documents reviewed:**
- `doc/design/02_persist_layer_design.md`

---

## Reviewer Notes (AI Agent 2)

**Reviewer:** AI Agent 2
**Date:** 2026-06-12
**Verification method:** Re-read every cited file and line range, cross-checked call sites and invariants, inspected related persistence code (FdReadWrite, MetaKeyBucketSplitNumber, KeyBucket constructor, ConfForSlot), and reviewed the existing test coverage in `KeyLoaderTest`, `KeyBucketsInOneWalGroupTest`, and `WalTest`.

| # | Status | Verdict |
|---|--------|---------|
| 1 | **CONFIRMED (HIGH)** | Real, reproducible crash-safety bug. The order in `doAfterPutAll` is exactly as cited. The `KeyBucket` constructor at `KeyBucket.java:169-172` will throw `IllegalStateException` on restart if the split-number metadata lags. `FdReadWrite.writeInnerByBuffer` (`FdReadWrite.java:679-708`) uses `raf.getChannel().write(buffer)` with no `fsync`, so cross-file ordering is not guaranteed. **Fix is self-healing per-bucket recovery (bidirectional metadata repair to match on-disk data layout), not a write reorder** — a reorder just shifts which mismatch is possible. The original "always upgrade" intuition is wrong: Window B (metadata=9, data=3) must DOWNGRADE the metadata, because per-key lookups are hash-routed via `KeyHash.splitIndex(keyHash, splitNumber, bucketIndex)` and the same key maps to a different split when `splitNumber` changes. Repair reuses the existing `splitNumber = -1` decode path at `KeyBucket.java:161-167` and picks the ground truth from non-empty buckets. |
| 2 | **CONFIRMED (MEDIUM)** | `readKeysToList` does not reset `countArray[1]` at entry. Early returns at `KeyLoader.java:436` and `:442` leave the value intact, so the caller at `:570` can overcount. The `WalTest` and `KeyLoaderTest` scan tests do not exercise the null shared-bytes / zero key-count early-return path, which is why this is not caught today. |
| 3 | **REFUTED — NOT A PRODUCTION BUG (was: LOW)** | `splitNumber` is invariantly 1, 3, or 9. `ConfForSlot.ConfBucket` (`ConfForSlot.java:279-280`) and config loader (`MultiWorkerServer.java:1472-1473`) reject anything else. `KeyBucketsInOneWalGroup.putPvmListToTargetBucket` only writes `targetSplitNumber` derived from `currentSplitNumber * 3` capped at `MAX_SPLIT_NUMBER=9` (`KeyBucketsInOneWalGroup.java:313-321, 332`). The only way to see `splitNumber == 0` is on-disk corruption outside the write path — out of scope. **No fix.** |
| 4 | **REFUTED — NOT A PRODUCTION BUG (was: LOW)** | Traced all three production callers of `encodeAfterPutBatch`. In every path, `readBeforePutBatch` overwrites entries `0..maxSplitNumber-1` with non-null lists (`:100`), and `putPvmListToTargetBucket` only appends non-null lists (`:325-329`). The NPE at `:184` is only triggerable by direct test manipulation or a future regression. **No fix.** |
| 5 | **REFUTED — NOT A BUG UNDER CURRENT THREADING MODEL (was: LOW)** | Same `keyBucketsInOneWalGroup` instance is read at `OneSlot.java:1887` and then passed to `chunk.persist` at `:1913`, all on the slot worker thread. Under slot-per-thread run-to-completion, the window in `putPvmListToTargetBucket` is unobservable. **No fix.** |

### Additional reviewer observations

- **F1 also has a test-coverage gap.** No test in `KeyLoaderTest` deliberately forces a split (e.g., inserting >48 cells into one bucket) and then asserts that both writes happened. Without JaCoCo confirmation of the split-detection branch in `putPvmListToTargetBucket` and the metadata-write branch in `doAfterPutAll`, the fix cannot claim it is exercised end-to-end.
- **F2's fix is small but needs a regression test.** The existing scan tests pass even with the bug because they never hit the `sharedBytes == null` or `keyCountThisWalGroup == 0` early-return paths from a populated-but-non-zero bucket. A focused test should construct a state where one split index has data and the next does not, and assert `scanLoopCount` increments exactly once.
- **F4 is the lowest-priority fix.** A defensive null guard is cheap and makes the invariant explicit, but it does not change observable behavior in the current production flow. Defer until the related persist layer work in Findings 1 and 2 is complete.
- **F5 should be fixed only if the threading model changes.** Marking it as "won't fix" is reasonable today, but the analysis should be revisited if any background reader or async writer is added in the future.

### Suggested order of fixes

1. F1 (HIGH) — self-healing per-bucket recovery, bidirectional metadata repair. (Reorder is unsafe — both orderings have a crash window, and the upgrade-only rule is wrong for per-key hash-routed lookups.)
2. ~~F2~~ — **already fixed in commit 11a35667** (`fix: reset countArray[1] in readKeysToList and only count non-empty buckets`).
3. F3 — **REFUTED**, skip.
4. F4 — **REFUTED**, skip.
5. F5 — **REFUTED**, skip.

---

## Overview

`KeyLoader` manages key-value metadata for a single slot, owning the file-backed key bucket split files (`fdReadWriteArray`), the split-number metadata (`MetaKeyBucketSplitNumber`), and the key-count statistics (`StatKeyCountInBuckets`). It coordinates batch writes, reads, scans, and cleanup of key bucket data.

`KeyBucketsInOneWalGroup` is a transient in-memory structure that represents the key buckets for a single WAL group within a slot. It is constructed before a batch write, loads existing key bucket data from `KeyLoader`, merges incoming entries, handles splitting when buckets overflow, and produces the encoded bytes that `KeyLoader` then persists to disk.

Both classes operate under a run-to-completion threading model — per-slot operations are single-threaded.

---

## Finding 1 — Crash-safety gap: split-number metadata persisted after key bucket data

**Status:** ✅ CONFIRMED (HIGH)
**Reviewer note:** Code matches the excerpt verbatim. `KeyBucket` constructor at `KeyBucket.java:169-172` will throw `IllegalStateException` on restart if metadata is stale. No `fsync` between (A) and (B) (`FdReadWrite.java:705` uses `raf.getChannel().write(buffer)`).

**Fix direction (revised after follow-up):** A simple swap of the write order is **not** safe. Both orderings have a crash window:
- data first, metadata old (current code, Window A): bytes say new split, metadata says old → `lastUpdateSplitNumber > splitNumber` → `IllegalStateException`
- metadata first, data old (proposed reorder, Window B): metadata says new split, bytes say old → `lastUpdateSplitNumber < splitNumber` → `IllegalStateException`

Pragmatic fix: add a self-healing recovery path. On first read after a crash window, detect the mismatch, decode the bucket via the existing `splitNumber = -1` path (which uses the split number embedded in `lastUpdateSeq`), and **repair the metadata to match the on-disk data layout**. Repair can be **upward or downward** — the invariant is "metadata must match what is encoded in the data," not "metadata should only go up." The original "don't downgrade" intuition only holds for full-iteration reads; per-key lookups are hash-routed via `KeyHash.splitIndex(keyHash, splitNumber, bucketIndex)` and the same key maps to a different split when `splitNumber` changes. So Window B (metadata=9, data=3) is unsafe if we keep metadata=9: a key whose `splitIndex=1` under splitNumber=3 would be routed to `splitIndex ∈ [3..8]` under splitNumber=9, and the lookup would find nothing even though the data is in split 1.

Per-bucket recovery algorithm (in `KeyBucketsInOneWalGroup.readBeforePutBatch`):
1. Read the current `splitNumberTmp` from metadata and load all readable split bytes for the WAL group.
2. **Slow-path / lazy recovery, per-bucket, on first mismatch.** For each split, iterate over each bucket. Try the strict constructor with the metadata-supplied splitNumber. If it throws `IllegalStateException` (the bucket's embedded `lastUpdateSplitNumber` disagrees), the `KeyBucket.readWithFallback` helper retries with `splitNumber=-1` to decode the embedded value from `lastUpdateSeq` (12 bytes from the header - cheap, no cell iteration), and we patch `splitNumberTmp[i]` so subsequent iterations in this load (and per-key reads after the load completes) use the correct routing. In the common consistent case this is a single try with no extra work.
3. Persist the repair via `updateMetaKeyBucketSplitNumberBatchIfChanged` so subsequent per-key reads see the corrected metadata. This is a no-op when the values are already equal to what's on disk.

Perf shape (deliberate slow path):
- `readBeforePutBatch` is a load/recovery path. Lazy per-bucket recovery keeps the common case at one try per bucket (no pre-scan, no overcount).
- **Per-key read paths are strict and fast.** `getValueXByKey` / `getExpireAtAndSeqByKey` / `readKeyBucketForSingleKey` go back to the direct constructor; a mismatch surfaces as `IllegalStateException` rather than silently retrying on the hot path. The repair runs on the next write batch via `readBeforePutBatch`. The SCAN path (`readKeysToList`) is also strict.
- `readKeyBuckets` (the diagnostic / debug API) keeps the tolerant helper, since it's a one-off inspection, not a hot path.

The strict `KeyBucket` constructor validation at `KeyBucket.java:169-172` stays in place and is what triggers the recovery. The fallback helper is the only place that catches the `IllegalStateException` and retries.

**Severity:** HIGH

**Cited files:**
- `src/main/java/io/velo/persist/KeyLoader.java`, lines 794–804 (`doAfterPutAll`)
- `src/main/java/io/velo/persist/KeyLoader.java`, lines 850–871 (`writeSharedBytesList`)
- `src/main/java/io/velo/persist/KeyLoader.java`, lines 164–178 (`updateMetaKeyBucketSplitNumberBatchIfChanged`)

**Code excerpt:**

```java
// KeyLoader.java:794-804
private void doAfterPutAll(int walGroupIndex, @NotNull KeyBucketsInOneWalGroup inner) {
    updateKeyCountBatch(walGroupIndex, inner.beginBucketIndex, inner.keyCountForStatsTmp);

    var sharedBytesList = inner.encodeAfterPutBatch();
    writeSharedBytesList(sharedBytesList, inner.beginBucketIndex);       // (A)

    updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex,
            inner.splitNumberTmp);                                        // (B)

    if (oneSlot != null) {
        oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex);
    }
}
```

**Root cause and impact:**

When `putPvmListToTargetBucket` triggers a split (a bucket's split number grows from, e.g., 3 → 9), `splitNumberTmp` is updated in memory. `encodeAfterPutBatch` encodes key buckets using the *new* split layout. Then `doAfterPutAll` persists (A) the shared bytes (key bucket data) first, and (B) the updated split-number metadata second.

If the process crashes — or the JVM terminates — **between** calls (A) and (B):

1. The key bucket data files on disk use the **new** split layout (more split indices, redistributed keys).
2. The `meta_key_bucket_split_number.dat` file still holds the **old** split numbers (e.g., 3 instead of 9).

On restart, `KeyBucketsInOneWalGroup.readBeforePutBatch()` reads the stale split numbers from metadata. It then iterates over at most 3 split indices (the old split number). But keys that were distributed into split indices 3–8 in the new layout are invisible — they reside in files that are never opened for the affected buckets. The `KeyBucket` constructor validates `lastUpdateSplitNumber` against the given `splitNumber` and will throw an `IllegalStateException` ("last update split number not match"), crashing the slot on startup.

Even if the constructor check were relaxed, the data would be silently lost: keys in higher split indices are never read.

**Reachability:**

Reachable any time `putPvmListToTargetBucket` triggers a split for any bucket in the WAL group. Splits occur when a bucket's total cell cost exceeds `INIT_CAPACITY` (48). With `SPLIT_MULTI_STEP = 3`, a bucket splits first at 48 cells, then at 144 cells (3 splits), then up to 432 cells (9 splits, the maximum). A one-time WAL flush that inserts many keys into the same bucket is the trigger.

The reverse ordering — (A) the split is detected and `splitNumberTmp` is updated in memory, (B) key bucket data is written to new split files — creates a window for the split-number metadata to be stale. The only mitigation is that both writes are `RandomAccessFile` writes that are small (up to ~4–36 KB per split file plus ~bucket-count bytes for metadata) and usually complete within the same OS-level write burst. But there is no explicit `fsync` / `sync` between them, and Java `RandomAccessFile` does not guarantee ordering across different files.

**Suggested fix direction:**

Swap the order: persist the split-number metadata **before** writing the key bucket data. This way:

- If a crash occurs after metadata write but before key bucket write: on restart, the split numbers are already at the new (larger) value. `readBeforePutBatch` will iterate over the new number of splits, and the key bucket data files for new splits that were never written will return `null` from `readBatchInOneWalGroup`. The `KeyBucket` constructor handles `null` shared bytes by creating an empty bucket.

This is safe because `KeyBucketsInOneWalGroup.readBeforePutBatch()` already handles the case where a split data file returns `null` (line 103–105: `if (sharedBytes == null) continue`).

```java
// Suggested reorder:
private void doAfterPutAll(int walGroupIndex, @NotNull KeyBucketsInOneWalGroup inner) {
    updateKeyCountBatch(walGroupIndex, inner.beginBucketIndex, inner.keyCountForStatsTmp);

    // (B) first: persist split numbers
    updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp);

    // (A) second: persist key bucket data
    var sharedBytesList = inner.encodeAfterPutBatch();
    writeSharedBytesList(sharedBytesList, inner.beginBucketIndex);

    if (oneSlot != null) {
        oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex);
    }
}
```

**Regression test should include:**

- Set up a small WAL group, insert enough keys into one bucket to trigger a split (e.g., 50 keys → split 1→3). Verify via JaCoCo that both the split detection and the `updateMetaKeyBucketSplitNumberBatchIfChanged` path are executed.
- Simulate a crash after `writeSharedBytesList` but before `updateMetaKeyBucketSplitNumberBatchIfChanged` (e.g., in a test with a mock/override that throws after the first write). On "restart" (re-open KeyLoader), verify that either the slot comes up cleanly (empty bucket in new splits) or fails gracefully with a logged warning rather than an `IllegalStateException`.
- Verify that with the reordered writes, a crash after metadata write but before data write results in an empty bucket on restart (no data loss, no crash).

---

## Finding 2 — `countArray[1]` not reset across scan calls, causing `scanLoopCount` overcount

**Status:** ✅ CONFIRMED (MEDIUM)
**Reviewer note:** `readKeysToList` early-returns at `KeyLoader.java:436` and `:442` leave `countArray[1]` unchanged, so the caller's `if (countArray[1] > 0)` check at `:570` overcounts. Existing `WalTest` / `KeyLoaderTest` scan tests do not exercise the `sharedBytes == null` / `keyCountThisWalGroup == 0` early-return path, so the bug stays hidden. Suggested reset is correct.

**Severity:** MEDIUM

**Cited files:**
- `src/main/java/io/velo/persist/KeyLoader.java`, lines 425–523 (`readKeysToList`)
- `src/main/java/io/velo/persist/KeyLoader.java`, lines 542–592 (`scan`)

**Code excerpt:**

```java
// KeyLoader.java:559
final int[] countArray = new int[]{count, 0};  // [remaining, realScanCount]

// KeyLoader.java:560-572 (inside scan loop)
for (int j = walGroupIndex; j < walGroupNumber; j++) {
    final var inWalKeys = oneSlot.getWalByGroupIndex(j).inWalKeysFormScan(beginScanSeq);
    for (int i = 0; i < maxSplitNumber; i++) {
        if (j == walGroupIndex && i < splitIndex) continue;

        final var skipCountInThisWalGroupThisSplitIndex = ...;
        var scanCursor = readKeysToList(keys, j, (byte) i, ...,
                countArray, beginScanSeq, inWalKeys);
        if (countArray[1] > 0) {        // <--- uses accumulated value
            scanLoopCount++;
        }
        ...
    }
}
```

```java
// KeyLoader.java:434-458 (inside readKeysToList)
private ScanCursor readKeysToList(...) {
    var keyCountThisWalGroup = statKeyCountInBuckets.getKeyCountForOneWalGroup(walGroupIndex);
    if (keyCountThisWalGroup == 0) {
        return null;          // early return: countArray[1] NOT modified
    }
    ...
    var sharedBytes = readBatchInOneWalGroup(splitIndex, beginBucketIndex);
    if (sharedBytes == null) {
        return null;          // early return: countArray[1] NOT modified
    }
    ...
    for (int i = 0; i < oneChargeBucketNumber; i++) {
        ...
        countArray[1]++;      // ONLY incremented when a non-empty keyBucket is found
        ...
    }
    return null;
}
```

**Root cause and impact:**

`countArray[1]` serves as a flag tracking whether `readKeysToList` found and scanned at least one non-empty key bucket. The problem: `countArray[1]` is **not reset to 0** at the start of each `readKeysToList` call. It accumulates across all calls within the scan loop.

Scenario:
1. Call `readKeysToList(splitIndex=0, walGroup=0)` finds 5 non-empty key buckets → `countArray[1] = 5`. `scanLoopCount` increments to 1.
2. Call `readKeysToList(splitIndex=1, walGroup=0)` returns `null` early (e.g., `sharedBytes == null`) → `countArray[1]` stays at 5. The caller checks `countArray[1] > 0` → **true**. `scanLoopCount` increments to 2, even though this call did no real scanning.

This causes `scanLoopCount` to overcount. When `scanLoopCount >= onceScanMaxLoopCount`, the scan returns early with whatever keys it has collected — possibly fewer than the requested `count`. The client receives a shorter key list and a cursor to resume scanning later. Redis SCAN tolerates this (keys can appear multiple times, partial results are acceptable), but the early return means the client must issue more SCAN commands to cover the keyspace.

The practical impact depends on `onceScanMaxLoopCount` (configurable, default likely small). With the overcount, a scan that should traverse N buckets may stop after N/2 buckets, returning half the expected keys. The remaining keys are discoverable via the cursor, but at the cost of extra round trips.

**Suggested fix direction:**

Reset `countArray[1] = 0` at the beginning of `readKeysToList`.

```java
private ScanCursor readKeysToList(final @NotNull ArrayList<String> keys,
                                   final int walGroupIndex,
                                   final byte splitIndex,
                                   ...) {
    countArray[1] = 0;  // <--- reset
    var keyCountThisWalGroup = statKeyCountInBuckets.getKeyCountForOneWalGroup(walGroupIndex);
    ...
}
```

Alternatively, track the real scan count with a local variable in `readKeysToList` and return it via the `ScanCursor` or a separate output parameter, so the caller isn't relying on a mutable accumulator.

**Regression test should include:**

- A scan that traverses a WAL group with: split 0 has key buckets but split 1 does not (all null). Verify `scanLoopCount` increments exactly once, not twice.
- Verify the same scan completes within the expected `onceScanMaxLoopCount` and returns the full requested `count` when enough keys exist.

---

## Finding 3 — `KeyHash.splitIndex` divides by zero when `splitNumber == 0`

**Status:** ❌ REFUTED — NOT A PRODUCTION BUG (was: LOW)
**Reviewer note (revised after follow-up):** `splitNumber` is invariantly 1, 3, or 9 in all reachable code paths. `ConfForSlot.ConfBucket` constructor (`ConfForSlot.java:279-280`) and config loader (`MultiWorkerServer.java:1472-1473`) both reject any value that is not 1, 3, or 9. `KeyBucketsInOneWalGroup.putPvmListToTargetBucket` only sets `splitNumberTmp[relativeBucketIndex] = targetSplitNumber` where `targetSplitNumber` is grown via `currentSplitNumber * KeyLoader.SPLIT_MULTI_STEP` (3) with a cap at `KeyLoader.MAX_SPLIT_NUMBER` (9) (`KeyBucketsInOneWalGroup.java:313-321, 332`). The only way to reach `KeyHash.splitIndex` with `splitNumber == 0` is on-disk corruption of `meta_key_bucket_split_number.dat` outside the write path, which is out of scope for this code. **No fix needed.** If a defensive guard is ever wanted, it should be added at the metadata loader (which already validates other invariants) rather than at the hot path inside `KeyHash.splitIndex`.

**Severity:** LOW

**Cited files:**
- `src/main/java/io/velo/KeyHash.java`, lines 119–131 (`splitIndex`)
- `src/main/java/io/velo/persist/MetaKeyBucketSplitNumber.java`, lines 39–44 (initialization)
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`, lines 82–88 (`readBeforePutBatch`)

**Code excerpt:**

```java
// KeyHash.java:119-131
public static byte splitIndex(long keyHash, byte splitNumber, int bucketIndex) {
    if (splitNumber == 1) {
        return 0;
    }
    // ...
    return (byte) Math.abs(((keyHash >> 32) % splitNumber));
    //                                   ^ division by zero if splitNumber == 0
}
```

**Root cause and impact:**

If `splitNumber == 0`, the early return for `splitNumber == 1` is not taken, and the expression `(keyHash >> 32) % 0` throws `ArithmeticException` (division by zero). In Java, integer modulo by zero is a hard exception, not `NaN` or `Infinity`.

**Reachability:**

In practice, this is unlikely because:

1. `MetaKeyBucketSplitNumber` initializes its in-memory cache with `ConfForSlot.global.confBucket.initialSplitNumber`, which is always 1, 3, or 9 (validated in `ConfForSlot.ConfBucket.checkIfValid()`).
2. The `readBeforePutBatch` method in `KeyBucketsInOneWalGroup` uses `maxSplitNumber = 1` as a floor (line 84), so even if all split numbers in metadata were 0, at least one split would be iterated.
3. `KeyBucketsInOneWalGroup` constructor's `readExisting=false` path fills `splitNumberTmp` with `initialSplitNumber`.

However, if `meta_key_bucket_split_number.dat` is corrupted on disk (e.g., a partial write, bit flip, or external modification) and a byte reads as 0, the code would crash with a confusing `ArithmeticException` rather than a descriptive error. The call stack would point to `KeyHash.splitIndex` → `KeyBucketsInOneWalGroup.putPvmListToTargetBucket` → `KeyLoader`, making diagnosis harder.

**Suggested fix direction:**

Add a guard in `splitIndex`:

```java
public static byte splitIndex(long keyHash, byte splitNumber, int bucketIndex) {
    if (splitNumber <= 0) {
        throw new IllegalArgumentException("splitNumber must be positive, got: " + splitNumber);
    }
    if (splitNumber == 1) {
        return 0;
    }
    return (byte) Math.abs(((keyHash >> 32) % splitNumber));
}
```

Or, since callers typically read `splitNumber` from metadata, add the validation at the point of use in `KeyBucketsInOneWalGroup` where `splitNumberTmp[i]` is read, ensuring 0 is never propagated.

**Regression test should include:**

- A unit test calling `KeyHash.splitIndex(hash, (byte) 0, bucketIndex)` and asserting `IllegalArgumentException` is thrown with a message containing "splitNumber".

---

## Finding 4 — Missing null guard for `listList.get(splitIndex)` in `encodeAfterPutBatch`

**Status:** ❌ REFUTED — NOT A PRODUCTION BUG (was: LOW)
**Reviewer note (revised after data-flow re-verification):** Traced all three production callers of `encodeAfterPutBatch` via `KeyLoader.doAfterPutAll` — `updatePvmListBatchAfterWriteSegments` (`KeyLoader.java:812`), `replacePvmListBatchInOneWalGroupForRebuild` (`KeyLoader.java:826`), `persistShortValueListBatchInOneWalGroup` (`KeyLoader.java:838`). In every path:
1. A fresh `KeyBucketsInOneWalGroup` is constructed, so `listList` starts as `new ArrayList<>()`.
2. `readBeforePutBatch` (line 82) overwrites entries `0..maxSplitNumber-1` with non-null `prepareListInitWithNull()` results via `listList.set(splitIndex, list)` at line 100.
3. `putPvmListToTargetBucket` (line 325-329) only appends to `listList` with non-null `prepareListInitWithNull()` results.
The invariant `listList.get(i) != null` for `i ∈ [0, listList.size())` is structurally maintained by every reachable code path. The NPE at line 184 is **only** triggerable by direct test manipulation (see `TestSplitInner.groovy:22`) or a future regression. Not a production bug. **No fix needed.**

**Severity:** LOW

**Cited files:**
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`, lines 172–219 (`encodeAfterPutBatch`)

**Code excerpt:**

```java
// KeyBucketsInOneWalGroup.java:172-198
byte[][] encodeAfterPutBatch() {
    // ...
    for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
        var list = listList.get(splitIndex);         // could be null
        for (int i = 0; i < list.size(); i++) {      // NPE if list is null
            var keyBucket = list.get(i);
            // ...
        }
        // ...
    }
    return sharedBytesList;
}
```

**Root cause and impact:**

After `readBeforePutBatch`, `listList` may contain `null` entries at indices beyond `maxSplitNumber` if the list was grown by a prior operation and then a smaller `maxSplitNumber` was used. In the current code flow, `listList` is only ever grown (never shrunk) and `readBeforePutBatch` is called once from the constructor, so this path is not reachable in the current production flow.

However, `encodeAfterPutBatch` iterates over `listList.size()` entries. If any entry is `null`, `list.size()` at line 184 throws `NullPointerException`. This is a latent bug — the invariant that `listList` has no null entries from 0 to `size()-1` is maintained by the current usage, but the code does not guard against a future caller violating it.

**Suggested fix direction:**

Add a null check:

```java
for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
    var list = listList.get(splitIndex);
    if (list == null) continue;    // <--- guard
    for (int i = 0; i < list.size(); i++) {
        // ...
    }
    // ...
}
```

**Regression test:**

A unit test that constructs `KeyBucketsInOneWalGroup`, manually sets a null entry in `listList`, and calls `encodeAfterPutBatch` — expect no NPE and the null slot skipped.

---

## Finding 5 — Stale split-number in `splitNumberTmp` during `putPvmListToTargetBucket` repopulation window

**Status:** ❌ REFUTED — NOT A BUG UNDER CURRENT THREADING MODEL (was: LOW)
**Reviewer note (revised after threading-model re-verification):** Verified all access to `KeyBucketsInOneWalGroup` from `OneSlot.putValueToWal` (`OneSlot.java:1849-1925`): the same `keyBucketsInOneWalGroup` instance is created at `:1877`, read at `:1887` via `getExpireAtAndSeq`, and then passed to `chunk.persist` at `:1913` — all on the slot worker thread. Under the slot-per-thread run-to-completion model in `AGENTS.md` and `doc/design/11_multithreading_design.md`, no other thread can observe the in-progress `putPvmListToTargetBucket` (which runs to completion before returning to the caller). The window between `splitNumberTmp[relativeBucketIndex] = targetSplitNumber` at `KeyBucketsInOneWalGroup.java:332` and the repopulation at `:348-362` is not observable. **No fix needed** unless the threading model changes (would need re-analysis if a background reader/async writer is added).

**Severity:** LOW (race window is on a single thread; safe under run-to-completion)

**Cited files:**
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`, lines 234–365 (`putPvmListToTargetBucket`)
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java`, lines 126–142 (`getExpireAtAndSeq`)

**Code excerpt:**

```java
// KeyBucketsInOneWalGroup.java:324-362
if (targetSplitNumber > currentSplitNumber) {
    if (listList.size() < targetSplitNumber) {
        for (int i = listList.size(); i < targetSplitNumber; i++) {
            listList.add(prepareListInitWithNull());
        }
    }
    currentSplitNumber = targetSplitNumber;
    splitNumberTmp[relativeBucketIndex] = targetSplitNumber;   // (A) split number updated
    isSplit = true;
}

// clear key bucket cells first, then put all
for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
    var list = listList.get(splitIndex);
    var keyBucket = list.get(relativeBucketIndex);
    if (keyBucket != null) {
        keyBucket.clearAll();                                   // (B) key buckets cleared
    } else {
        keyBucket = new KeyBucket(...);
        list.set(relativeBucketIndex, keyBucket);
    }
}

for (var entry : map.entrySet()) {
    var pvm = entry.getValue();
    var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
    // ...
    keyBucket.put(...);                                         // (C) repopulated
}
```

**Root cause:**

At point (A), `splitNumberTmp[relativeBucketIndex]` is updated to the new (larger) split number. But the new split indices' key buckets are still empty — they were just created with `prepareListInitWithNull()` and have all-null entries. They are not populated until point (C).

Between (A) and (C), if any code reads `splitNumberTmp[relativeBucketIndex]` and uses it to look up data (via `getExpireAtAndSeq`, `getValueX`, or a concurrent access), it would compute `splitIndex` from the new split number, find a null list or null key bucket for that split index, and return `null` — as if the data doesn't exist.

**Reachability:**

Under the documented run-to-completion threading model, the slot worker thread that calls `putPvmListToTargetBucket` does not yield between (A) and (C), so no other caller on the same slot can observe the window. If the threading model were ever relaxed (e.g., a background read during a batch write), this window would become observable.

**Suggested fix direction:**

Move the `splitNumberTmp` update to after the repopulation loop (just before the `keyCountForStatsTmp` update at line 364). The key bucket population should use the new split number (for `KeyHash.splitIndex`), but the externally visible `splitNumberTmp` should not change until the data is consistent.

A clearer approach: store the new split number in a local variable for the repopulation phase, and only update `splitNumberTmp[relativeBucketIndex]` after all key buckets are repopulated.

---

## Summary

| # | Severity | Status | File | Lines | Description |
|---|----------|--------|------|-------|-------------|
| 1 | HIGH | ✅ CONFIRMED (fix: bidirectional self-healing recovery, not reorder) | KeyLoader.java | 794–804 | Crash-safety: split-number metadata persisted after key bucket data; crash window causes inconsistency |
| 2 | MEDIUM | ✅ CONFIRMED | KeyLoader.java | 425–523, 570 | `countArray[1]` not reset across `readKeysToList` calls; `scanLoopCount` overcount causing early scan termination |
| 3 | LOW | ❌ REFUTED | KeyHash.java | 119–131 | Division by zero when `splitNumber == 0`; splitNumber is invariantly 1/3/9, unreachable in normal flow |
| 4 | LOW | ❌ REFUTED | KeyBucketsInOneWalGroup.java | 182–184 | Missing null check on `listList.get(splitIndex)` in `encodeAfterPutBatch`; invariant structurally maintained by all production paths |
| 5 | LOW | ❌ REFUTED | KeyBucketsInOneWalGroup.java | 324–362 | `splitNumberTmp` updated before key bucket repopulation is complete; safe under current threading model, not observable |

---

## Review Feedback — Bug 1 Fix

**Reviewer:** AI Agent
**Date:** 2026-06-12
**Reviewed commit:** `2f86c098` (`fix: self-healing recovery for key bucket split number mismatch on crash window`)

### Summary of the fix

The commit replaces the unsafe write-order fix with a recovery-oriented read path. `KeyBucket.readWithFallback` first tries the metadata split number, then falls back to the embedded split number from `lastUpdateSeq` if the constructor detects a mismatch. `KeyBucketsInOneWalGroup.readBeforePutBatch` uses that helper to repair `splitNumberTmp` during reload, and `KeyLoader.readKeyBuckets` also uses the tolerant helper for diagnostic reads.

### Strengths

The fix matches the real failure mode better than a simple write reorder. It handles both crash windows:

- Window A: data is newer than metadata.
- Window B: metadata is newer than data.

The important part is that the repair rule is now bidirectional and keyed off the split number actually encoded in the bucket bytes, which is the only value that keeps hash-routed lookups and on-disk layout aligned.

The code also keeps the hot path explicit. `getValueXByKey`, `getExpireAtAndSeqByKey`, and `readKeysToList` stay strict instead of hiding inconsistencies behind extra retries.

### Concerns

The commit still leaves a read-only gap after a crash window. `readBeforePutBatch` is the self-healing site, but the direct key lookup path in `KeyLoader.readKeyBucketForSingleKey` remains strict, so a restart followed only by reads can still surface the split mismatch before any write batch has a chance to repair metadata.

That is acceptable only if the intended contract is "repair happens on the next batch-load/write path." If the intended contract is truly "self-healing on first access," then the current implementation does not meet it yet and needs a tolerant read-side path for `getValueXByKey` / `getExpireAtAndSeqByKey` as well.

### Follow-ups

1. Decide whether crash-window recovery is write-path-only or also read-path-first.
2. If read-path-first recovery is desired, add a tolerant helper for per-key lookup rather than relying on the next batch write.
3. Keep the regression test focused on both crash windows, but add a read-only-after-restart case if that is part of the contract.

---

## Review Feedback — Bug 1 Fix (DeepSeek-v4-pro)

**Reviewer:** DeepSeek-v4-pro
**Date:** 2026-06-12
**Reviewed commit:** `2f86c098` (`fix: self-healing recovery for key bucket split number mismatch on crash window`)

### Summary of the fix

The commit replaces the unsafe write-order approach with recovery-oriented reads. `KeyBucket.readWithFallback` tries the metadata-supplied `splitNumber` first; on `IllegalStateException` (crash-window mismatch), retries with `splitNumber=-1` so the constructor uses the split number embedded in `lastUpdateSeq`. `KeyBucketsInOneWalGroup.readBeforePutBatch` uses this helper for per-bucket lazy repair, patching `splitNumberTmp` during WAL-group load and persisting the repair immediately. Hot per-key lookup paths remain strict. Tests cover both crash windows.

### Verified behavior

- `readWithFallback` correctly handles both Window A (embedded > metadata → repair up) and Window B (embedded < metadata → repair down).
- Recovery is per-bucket and lazy: only the first mismatch for each bucket triggers fallback. Subsequent split indices in the same load see the already-patched `splitNumberTmp[i]`.
- Immediate metadata persist at `KeyBucketsInOneWalGroup.java:154` ensures the repair survives before any write batch commits. The subsequent call in `doAfterPutAll` is a no-op when nothing changed.
- SCAN path (`readKeysToList`) now only increments `countArray[1]` for `keyBucket.size > 0`, eliminating the overcount from Finding 2 as a side effect.
- Both test cases (`crash_window_1`, `crash_window_2`) verify no `IllegalStateException`, keys remain readable via `getValueX`, and metadata converges to the embedded value.

### Issue found: `encodeAfterPutBatch` loop bound stale after downgrade repair

**Severity:** HIGH (crashes on the first batch write after a Window-B recovery)

**Location:** `KeyBucketsInOneWalGroup.java:219`

After a downgrade repair (Window B: metadata was e.g. 9, embedded data says 3), `splitNumberTmp` is patched down in `readBeforePutBatch`, but `listList` retains its pre-repair size. The `readBeforePutBatch` growth loop (lines 107-112) uses `maxSplitNumber` computed from the metadata *before* any repair occurred, so `listList.size()` = 9 even after `splitNumberTmp` is repaired to 3.

When the write batch completes and `encodeAfterPutBatch` runs:

```java
// line 210-217 — maxSplitNumberTmp computed from repaired splitNumberTmp = 3
byte maxSplitNumberTmp = 1;
for (int i = 0; i < oneChargeBucketNumber; i++) {
    if (splitNumberTmp[i] > maxSplitNumberTmp) {
        maxSplitNumberTmp = splitNumberTmp[i];
    }
}
var sharedBytesList = new byte[maxSplitNumberTmp][];  // new byte[3][]

// line 219 — iterates listList.size() = 9, not maxSplitNumberTmp
for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {  // 0..8
    // ...
    sharedBytesList[splitIndex] = sharedBytes;  // line 254 → AIOOBE at splitIndex=3
}
```

`sharedBytesList` has length 3 but the loop dereferences index 3, throwing `ArrayIndexOutOfBoundsException`.

**Reachability:** Triggered on the first `putPvmListToTargetBucket` → `encodeAfterPutBatch` call after a Window-B crash recovery. Neither existing test exercises a write-after-recovery path (both stop after `getValueX` verification), so this passes CI but would crash in production.

**Required fix:**

```java
// line 219: use maxSplitNumberTmp as the loop bound
for (int splitIndex = 0; splitIndex < maxSplitNumberTmp; splitIndex++) {
```

This is safe because `putPvmListToTargetBucket` guarantees `listList.size() >= currentSplitNumber` for every bucket in the WAL group, and `maxSplitNumberTmp` is the maximum of all `splitNumberTmp[i]` values (post-repair). Entries in `listList` at indices ≥ `maxSplitNumberTmp` contain only null-initialized lists from the stale pre-repair load — no data to encode, no loss.

**Required test:** A test case that performs a Window-B recovery (metadata downgrade) and then triggers a write batch through `putPvmListToTargetBucket` / `encodeAfterPutBatch`. Assert no AIOOBE and that `sharedBytesList.length` equals the repaired max split number.

### Additional strengths

1. **`readWithFallback` is minimal and fast.** Single try/catch boundary. The common consistent case pays one `new KeyBucket(...)` call with no exception overhead. The `ReadResult` record carries `effectiveSplitNumber` and `wasRecovered` so the caller drives metadata repair without re-parsing the bucket header.

2. **All `KeyBucket` call sites documented.** Every `new KeyBucket(...)` in `KeyLoader` and `KeyBucketsInOneWalGroup` has a comment explaining whether it's strict or tolerant and why.

### Minor observations (non-blocking)

1. **`readKeyBuckets` discards repair info.** At `KeyLoader.java:756`, `readWithFallback(...).keyBucket()` ignores `effectiveSplitNumber` and `wasRecovered`. A mismatch discovered here won't trigger metadata repair — the next write batch will catch it via `readBeforePutBatch`. Acceptable for a diagnostic/debug API.

2. **`readWithFallback` catches broadly.** The catch block could mask other `IllegalStateException` sources in the `KeyBucket` constructor beyond split-number mismatch (e.g., `sharedBytes.length % KEY_BUCKET_ONE_COST_SIZE != 0`, key/value length errors in `getFromOneCell`). All would produce the same fallback behavior (retry with `splitNumber=-1`) which is harmless since the retry uses the same `sharedBytes` and would hit the same exception. The logged warning distinguishes recovery from corruption by including slot/bucket/split identifiers.

3. **`listList` retains stale entries after downgrade.** After a Window-B repair, entries at indices ≥ the repaired `maxSplitNumberTmp` are all-null lists. Bounded (max ~9 entries), transient (per instance, GC'd after the write batch). The `encodeAfterPutBatch` loop bound fix (above) makes these unreachable in the encode path — no separate fix needed.

---

## Review Feedback — Bug 1 Fix Follow-up (DeepSeek-v4-pro)

**Reviewer:** DeepSeek-v4-pro
**Date:** 2026-06-12
**Reviewed commit:** `81ad60e1` (`fix: encodeAfterPutBatch loop bound after split-number recovery`)

### Verification

The fix uses `Math.min(listList.size(), maxSplitNumberTmp)` as the loop bound. The original suggestion of `maxSplitNumberTmp` alone would NPE for Window A (where `listList.size()` < `maxSplitNumberTmp`). The `min()` handles both directions correctly.

**Window B test updated.** The `crash_window_2` test now exercises a write batch after recovery: `putAll` → `encodeAfterPutBatch`, asserting no exception, `sharedBytesListAfterWrite.length == 1`, and the new key readable via `getValueX`.

**Confirmed — no remaining concerns with the loop bound.** The fix is correct for both crash windows.

### Follow-up observation (separate issue, not introduced by this commit)

Window A (data-new, metadata-old) has a latent data-loss bug during the first write batch after recovery. `readBeforePutBatch` loads only the pre-repair max split indices, but `putPvmListToTargetBucket` then grows `listList` to the post-repair max and creates empty keyBuckets for the unloaded higher splits. `encodeAfterPutBatch` writes these as zero-filled blocks, **permanently overwriting** the on-disk data that existed pre-crash. Subsequent per-key lookups that hash to those splits find empty buckets, and the data is not recoverable on the next `readBeforePutBatch` reload — that reload reads from disk, which now has zeros, and `isBytesValidAsKeyBucket` returns false for the zero-filled bytes, so the buckets are silently skipped. The "mitigated on the next reload" wording in the original draft is incorrect; the data is lost. Not regressed by this commit. Required follow-up: in `readBeforePutBatch`, after the recovery loop completes, recompute the max from the patched `splitNumberTmp` and load any additional splits up to that post-repair max so `encodeAfterPutBatch` has the real data and the writer preserves on-disk state. Regression test for Window A write-after-recovery should be added when this is fixed.
