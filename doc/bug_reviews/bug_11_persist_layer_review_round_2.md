# Bug 11 Persist Layer Review Round 2

## Scope

Static review of the persistence layer under `src/main/java/io/velo/persist`, following the round 1 findings and
fix-review notes in `doc/bug_reviews/bug_11_persist_layer_review_round_1.md`. This round focuses on new correctness
or robustness issues in big-string lifecycle handling, WAL scan conflict resolution, and chunk segment metadata
maintenance.

## Finding 1: Big-string overwrite or delete can leave the old file protected by a stale WAL UUID map entry

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1896-1901`
- `src/main/java/io/velo/persist/Wal.java:589-597`
- `src/main/java/io/velo/persist/Wal.java:951-986`

**Code excerpt:**

```java
var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);
if (overwrittenBigStringUuid != null) {
    var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
    if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
        delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(overwrittenBigStringUuid, bucketIndex, cv.getKeyHash(), key));
    }
}
```

```java
private void addBigStringUuidIfMatch(V v) {
    // deleted flag or test value
    if (v.cvEncoded.length < 28) {
        return;
    }

    if (CompressedValue.onlyReadSpType(v.cvEncoded) == CompressedValue.SP_TYPE_BIG_STRING) {
        bigStringFileUuidByKey.put(v.key, CompressedValue.getBigStringMetaUuid(v.cvEncoded));
    }
}
```

**Root cause:**

`bigStringFileUuidByKey` is updated only when the new WAL value is a big-string metadata value. It is not cleared when
the same key is overwritten by a normal short value, normal long value, or delete tombstone. `OneSlot.put(...)` then
compares the old big-string UUID against `targetWal.bigStringFileUuidByKey.get(key)`. Because the map still contains the
old UUID, the code treats the old big-string file as current and does not enqueue it for deletion.

The same stale map also protects the orphan file in `intervalDeleteOverwriteBigStringFiles(...)` because that cleanup
path checks `targetWal.bigStringFileUuidByKey.containsValue(id.uuid())` before enqueueing file deletion.

**Impact:**

A key that is changed from big-string storage to a non-big-string value, or removed while its big-string metadata is
still WAL-resident, can leave the old big-string file on disk. In low-traffic cases where the short WAL does not quickly
persist and clear, the stale UUID map can keep protecting that orphan file indefinitely. After restart, WAL replay can
rebuild the stale UUID map from older records as well.

## Finding 2: WAL scan can return a stale normal value instead of a newer short value after reload

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/Wal.java:452-455`
- `src/main/java/io/velo/persist/Wal.java:703-723`
- `src/main/java/io/velo/persist/Wal.java:256-258`

**Code excerpt:**

```java
var allValues = new HashMap<String, V>();
allValues.putAll(delayToKeyBucketShortValues);
allValues.putAll(delayToKeyBucketValues);
```

```java
V getV(@NotNull String key) {
    var vShort = delayToKeyBucketShortValues.get(key);
    var v = delayToKeyBucketValues.get(key);
    ...
    if (vShort.seq > v.seq) {
        return vShort;
    } else {
        return v;
    }
}
```

**Root cause:**

Runtime `put(...)` removes a key from the opposite WAL map, but the old encoded record can still remain in the other WAL
file until rewrite or persistence. `lazyReadFromFile()` reloads normal and short WAL files independently, so after a
restart the same key can exist in both `delayToKeyBucketValues` and `delayToKeyBucketShortValues`.

Point lookups use `getV(...)`, which resolves this conflict by sequence. `scan(...)` does not. It copies short values
first and normal values second, so a stale normal value overwrites a newer short value or delete tombstone in
`allValues`.

**Impact:**

After restart or WAL replay, `SCAN` can expose a key using an older normal-value record even when a newer short-value
record or delete tombstone exists for the same key. This can make scan results disagree with `GET` and `EXISTS`, and can
return keys that should be hidden by the latest WAL state.

## Finding 3: Chunk truncate checks use absolute segment indexes against per-FD bitsets

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:214-225`
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:286-304`

**Code excerpt:**

```java
private void updateBitSetCanReuseForSegmentIndex(int fdIndex, int targetSegmentIndex, boolean canReuse) {
    var bitSet = segmentCanReuseBitSet[fdIndex];
    bitSet.set(targetSegmentIndex, canReuse);
```

```java
var bitSet = segmentCanReuseBitSet[checkFdIndex];
var beginSegmentIndex = checkFdIndex * segmentNumberPerFd + check1KSegmentsGroupIndex * 1024;

for (int i = 0; i < 1024; i++) {
    if (!bitSet.get(beginSegmentIndex + i)) {
        isAllCanClear1KSegmentsGroup = false;
        break;
    }
}
```

**Root cause:**

`segmentCanReuseBitSet[fdIndex]` is indexed by the segment's position inside that FD. The setter receives
`targetSegmentIndex = segmentIndex % segmentNumberPerFd`, and the bitset size is `segmentNumberPerFd`. The periodic
truncate task, however, computes an absolute `beginSegmentIndex` and calls `bitSet.get(beginSegmentIndex + i)`.

For `checkFdIndex > 0`, those indexes are outside the per-FD coordinate space, so the bitset reads false even when all
segments in that FD are reusable.

**Impact:**

Chunk files beyond FD 0 can fail the "all reusable" check forever and never set `canTruncateFdIndex`. This prevents
disk-space reclamation for fully merged/reusable chunk files after sustained writes move into later chunk FDs.

## Finding 4: Restart metadata reload drops a persisted segment run that reaches the end of the chunk ring

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:347-382`
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:422-433`

**Code excerpt:**

```java
for (int segmentIndex = 0; segmentIndex < maxSegmentNumber; segmentIndex++) {
    ...
    if (Chunk.isSegmentReusable(flagByte) || currentWalGroupIndex != walGroupIndex) {
        if (tmpBeginSegmentIndex != -1) {
            markPersistedSegmentIndexToTargetWalGroup(currentWalGroupIndex, tmpBeginSegmentIndex, (short) continueUsedSegmentCount);
            markedCount++;
        }
        ...
    } else {
        if (tmpBeginSegmentIndex == -1) {
            tmpBeginSegmentIndex = segmentIndex;
        }
        continueUsedSegmentCount++;
    }
}

return markedCount;
```

**Root cause:**

`reloadMarkPersistedSegmentIndex()` only flushes the current run when it later sees a reusable segment or a WAL-group
change. If the metadata file ends while `tmpBeginSegmentIndex` is still active, the final run is discarded because there
is no post-loop flush.

Round 1 fixed the write pointer so the last valid segment can be used. That makes a live run ending at
`maxSegmentNumber - 1` reachable.

**Impact:**

After restart, persisted segments at the tail of the chunk ring may not be registered in
`beginSegmentIndexGroupByWalGroupIndex`. Those segments are then invisible to `findThoseNeedToMerge(...)`, delaying or
preventing merge/reuse scheduling for that tail run until another path happens to rewrite the metadata.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Stale big-string UUID map protects orphan files | High | Needs reviewer verification | High - lifecycle path verified by code inspection |
| 2 - WAL scan ignores seq conflict between normal and short maps | High | Needs reviewer verification | High - scan merge contradicts `getV(...)` |
| 3 - Truncate task indexes per-FD bitset with absolute segment indexes | Medium | Needs reviewer verification | High - coordinate-space mismatch is direct |
| 4 - Metadata reload drops final persisted segment run | Medium | Needs reviewer verification | Medium - boundary path depends on tail segment usage |

## Reviewer Notes (Round 2 - AI agent 2)

### Finding 1 - Stale big-string UUID map protects orphan files

**Status:** CONFIRMED

The current code still only adds entries to `Wal.bigStringFileUuidByKey` when a short WAL value decodes as
`SP_TYPE_BIG_STRING`; non-big-string short values and delete tombstones return without clearing an existing mapping
(`src/main/java/io/velo/persist/Wal.java:589-597`). `Wal.put(...)` removes the key from the opposite WAL value map, but
does not remove `bigStringFileUuidByKey` when `isValueShort` is false or when the short value is not a big-string
metadata value (`src/main/java/io/velo/persist/Wal.java:951-990`). `OneSlot.put(...)` then compares the overwritten UUID
against `targetWal.bigStringFileUuidByKey.get(key)` and skips enqueueing deletion when the stale map entry still matches
(`src/main/java/io/velo/persist/OneSlot.java:1896-1901`). The interval cleanup path also treats any UUID still present
in that map as protected (`src/main/java/io/velo/persist/OneSlot.java:1261-1285`). This is a real lifecycle leak for
big-string-to-normal and big-string-to-delete transitions while the stale WAL map entry remains resident or is rebuilt
from WAL reload.

### Finding 2 - WAL scan ignores seq conflict between normal and short maps

**Status:** CONFIRMED

`lazyReadFromFile()` reloads the normal and short WAL files into separate maps (`src/main/java/io/velo/persist/Wal.java:256-258`),
and `readBytesToList(...)` does not reconcile records across those maps (`src/main/java/io/velo/persist/Wal.java:558-586`).
Point lookup handles the possible duplicate key by comparing `vShort.seq` and `v.seq` in `getV(...)`
(`src/main/java/io/velo/persist/Wal.java:703-723`). `scan(...)` instead copies short values first and normal values
second into a new `HashMap`, so the normal-map entry wins regardless of sequence (`src/main/java/io/velo/persist/Wal.java:452-456`).
Because runtime writes remove only the in-memory opposite map entry and do not erase the old encoded record from the
other WAL file immediately (`src/main/java/io/velo/persist/Wal.java:961-990`), reload can recreate exactly this
cross-map conflict. A stale normal record can therefore mask a newer short value or tombstone during scan.

### Finding 3 - Truncate task indexes per-FD bitset with absolute segment indexes

**Status:** CONFIRMED

The bitsets are allocated per FD with `segmentNumberPerFd` capacity (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:143-151`),
and writes update them using `segmentIndex % segmentNumberPerFd` (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:570-583`
and `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:601-623`). The truncate-check task selects the correct
per-FD bitset, but computes `beginSegmentIndex` as an absolute segment index and then calls `bitSet.get(beginSegmentIndex + i)`
(`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:286-304`). For `checkFdIndex > 0`, those reads are outside
the per-FD coordinate space; `BitSet.get(...)` returns false for unset out-of-range bits, so later FDs cannot satisfy the
all-reusable check even when their local bits are all true.

### Finding 4 - Metadata reload drops final persisted segment run

**Status:** CONFIRMED

`reloadMarkPersistedSegmentIndex()` starts a run when it sees non-reusable segment metadata and flushes that run only
inside the loop when it later encounters a reusable segment or WAL-group change (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:347-382`).
There is no post-loop flush before `markedCount` is returned. Since segment flags can be written all the way to the tail
of the configured segment range through `setSegmentMergeFlag(...)` and `setSegmentMergeFlagBatch(...)`
(`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:570-623`), a persisted run that reaches
`maxSegmentNumber - 1` is reachable and will be omitted from `beginSegmentIndexGroupByWalGroupIndex`
(`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:422-433`) after restart.

## Review Feedback - Finding 1 Fix

Reviewed by: AI agent 1
Date: 2026-04-27
Commit reviewed:
- `4499833` `fix: remove stale big-string UUID from map on overwrite or delete`

### Summary of the Fix

The fix updates `Wal` so `bigStringFileUuidByKey` is no longer append-only for a key:

- `addBigStringUuidIfMatch(...)` now removes the key when the WAL value is a delete marker or any non-big-string value.
- normal-value `put(...)` removes the key from `bigStringFileUuidByKey` when it moves a key out of the short-value WAL map.
- `lazyReadFromFile()` calls `refreshBigStringUuidMap()` after loading both WAL maps, so stale short-WAL records are reconciled against the latest `getV(...)` result.

### Strengths

- Addresses the original stale-map root cause directly in the WAL ownership boundary.
- Covers big-string to normal short, big-string to normal long, delete tombstone, and reload paths in one focused regression test.
- Keeps the cleanup behavior local to WAL state rather than adding special cases to `OneSlot.intervalDeleteOverwriteBigStringFiles(...)`.

### Findings

No blocking issues found in the Finding 1 fix commit.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest.clear big string uuid when key changes to non big string wal value" --rerun-tasks` successfully.
- JaCoCo HTML confirms the touched `Wal` paths were exercised: `Wal.java` lines 259, 598, 605, 613, 614, 1001, 1006, 1007, 1008, and 1029 are covered; line 613 shows both branches covered.

### Notes

- Review was scoped to commit `4499833`. A separate Finding 2 commit, `033dec7`, is currently on top of the branch and still needs its own review.

## Review Feedback - Finding 2 Fix

Reviewed by: AI agent 1
Date: 2026-04-27
Commit reviewed:
- `033dec7` `fix: WAL scan resolves seq conflict between short and normal value maps`

### Summary of the Fix

`Wal.scan(...)` now merges the normal-value WAL map into the temporary scan map by sequence instead of blindly overwriting
the short-value map. If both maps contain the same key after reload, scan now keeps the entry with the higher sequence,
matching the intent of `getV(...)` for normal short/long conflicts.

### Strengths

- Minimal change in the scan conflict-resolution path.
- Regression test covers the important conflict cases: short newer than normal, normal newer than short, and a newer
  short delete tombstone hiding an older normal value.
- Keeps point lookup and scan behavior aligned for the confirmed stale-normal-vs-newer-short bug.

### Findings

No blocking issues found in the Finding 2 fix commit.

### Residual Risk

- Equal sequence values in both WAL maps would still differ from `getV(...)`: `getV(...)` chooses the normal value when
  sequences are equal, while the scan merge keeps the existing short value. This is low risk because WAL sequences are
  expected to be unique for real updates, but using `>=` would make the tie behavior identical.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest.test scan resolves seq conflict between short and normal values" --rerun-tasks` successfully.
- Ran `./gradlew :test --tests "io.velo.persist.WalTest" --rerun-tasks` successfully.
- JaCoCo HTML from the full `WalTest` run confirms the changed scan merge lines were executed: `Wal.java` lines 453,
  454, 455, 456, 457, and 458 are covered; line 457 is partially branch-covered (`1 of 4 branches missed`).

## Review Feedback - Finding 3 Fix

Reviewed by: AI agent 1
Date: 2026-04-27
Commit reviewed:
- `3c99cec` `fix: use per-fd segment index for truncate check`

### Summary of the Fix

The truncate task now checks `segmentCanReuseBitSet[checkFdIndex]` using a per-FD segment offset:

```java
var beginSegmentIndex = check1KSegmentsGroupIndex * 1024;
```

This removes the absolute `checkFdIndex * segmentNumberPerFd` component that made FD indexes after 0 read outside their
per-FD bitset coordinate space.

### Strengths

- Minimal one-line production change matching how the bitset is populated.
- Regression test specifically drives `checkFdIndex > 0`, which is the previously broken path.
- Existing class-level tests continue to cover the original FD 0 truncate behavior.

### Findings

No issues found in the Finding 3 fix commit.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest.test truncate check uses per-FD bitset index for fd index > 0" --rerun-tasks` successfully.
- Ran `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest" --rerun-tasks` successfully.
- JaCoCo HTML confirms the changed path was exercised: `MetaChunkSegmentFlagSeq.java` line 289 is covered, and lines
  291, 292, 298, and 303 show full branch coverage.

## Review Feedback - Finding 4 Fix

Reviewed by: AI agent 1
Date: 2026-04-27
Commit reviewed:
- `2a6c6e6` `fix: mark tail segment run on reload`

### Summary of the Fix

`reloadMarkPersistedSegmentIndex()` now flushes an active run after the segment scan loop completes:

```java
if (tmpBeginSegmentIndex != -1) {
    markPersistedSegmentIndexToTargetWalGroup(currentWalGroupIndex, tmpBeginSegmentIndex, (short) continueUsedSegmentCount);
    markedCount++;
}
```

This preserves a final non-reusable segment run that reaches `maxSegmentNumber - 1`, where there is no following reusable
segment or WAL-group transition to trigger the existing in-loop flush.

### Strengths

- Minimal change at the precise boundary that dropped the tail run.
- Regression test writes a two-segment run ending at the configured chunk tail and verifies both `markedCount` and
  `findThoseNeedToMerge(...)`.
- Existing reload/merge tests still pass.

### Findings

No issues found in the Finding 4 fix commit.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest.test reload marks persisted segment run ending at chunk tail" --rerun-tasks` successfully.
- Ran `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest" --rerun-tasks` successfully.
- JaCoCo HTML confirms the new post-loop flush branch was exercised: `MetaChunkSegmentFlagSeq.java` lines 382, 383, and
  384 are covered, and line 382 shows full branch coverage.

## Review Feedback - Commit `a70c045` ("fix: merge overlapping keys after WAL reload instead of in scan")

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed: `a70c045`

### Summary of the Fix

The fix moves WAL conflict resolution from scan time to reload time: `mergeAfterReload()` is called in `lazyReadFromFile()` after reading both WAL files but before `refreshBigStringUuidMap()`. The previous scan-time approach (`033dec7`) was reverted in favor of merging the maps upfront so subsequent operations work on a single authoritative map.

### Strengths

- Eliminates per-scan conflict resolution overhead by resolving overlaps once at reload time.
- Simplifies scan logic: `allValues.putAll(delayToKeyBucketShortValues)` followed by `allValues.putAll(delayToKeyBucketValues)` is now safe because the maps are already deduplicated.

### Findings

**Stale big-string UUID retained after merge when normal map entry loses conflict**

- **Severity:** High
- **Confidence:** High

`mergeAfterReload()` removes a key from `delayToKeyBucketValues` when the short WAL entry has higher sequence. However, `refreshBigStringUuidMap()` (line 607-615) only iterates `delayToKeyBucketShortValues`:

```java
private void refreshBigStringUuidMap() {
    bigStringFileUuidByKey.clear();
    for (var entry : delayToKeyBucketShortValues.entrySet()) {  // only short map
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
}
```

If a big-string key in `delayToKeyBucketValues` loses the merge (because the short WAL entry has higher seq), the key is removed from `delayToKeyBucketValues` but never appears in `delayToKeyBucketShortValues`. Therefore `refreshBigStringUuidMap()` never sees it, and `bigStringFileUuidByKey` retains the stale UUID entry.

**Impact:** Same root cause as the original Finding 1: stale `bigStringFileUuidByKey` entries can protect orphan big-string files from deletion. After restart, WAL replay can rebuild the stale UUID map from older records as well.

**Root cause:** `refreshBigStringUuidMap()` only reconciles from `delayToKeyBucketShortValues`, so keys removed from `delayToKeyBucketValues` during merge are invisible to the big-string cleanup path.

### Recommendation

`refreshBigStringUuidMap()` should also iterate `delayToKeyBucketValues` after merge, or `mergeAfterReload()` should clear affected keys from `bigStringFileUuidByKey` when removing them from the normal map. The former is simpler:

```java
private void refreshBigStringUuidMap() {
    bigStringFileUuidByKey.clear();
    for (var entry : delayToKeyBucketShortValues.entrySet()) {
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
    for (var entry : delayToKeyBucketValues.entrySet()) {  // add back entries removed from short map by merge
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
}
```

Or alternatively, `mergeAfterReload()` should call `addBigStringUuidIfMatch(v)` for each `delayToKeyBucketValues` entry that survives the merge, before the removal.

### Verification

The existing test `test merge after reload resolves overlapping keys between short and normal maps` does not cover the big-string case. A test case similar to Finding 1's test but exercised through `lazyReadFromFile()` would be needed to catch this regression.

### Response to "Stale big-string UUID retained after merge" Finding

**Status:** PARTIALLY CORRECT on merge, but the response misses the actual bug.

**Analysis:**

The response correctly states that big-string metadata is always in `delayToKeyBucketShortValues` and that `mergeAfterReload()` + `refreshBigStringUuidMap()` ordering is safe for the merge scenario described.

However, the response does not address the underlying overwrite path bug that my review was pointing to. When a big-string is overwritten by a normal (non-big-string) value at runtime:

1. `cv.isShortString()` = false → `isValueShort = false`
2. Code enters `OneSlot.java:1849` (the `else` branch for non-short values)
3. Line 1852: `isPersistLengthOverSegmentLength = false` for a normal value
4. Line 1855: condition is false → code enters inner `else` at line 1884
5. `overwrittenBigStringUuid` remains **null** — `getCurrentBigStringUuid()` is **never called** in this path
6. The old big-string UUID is never enqueued for deletion at lines 1897-1902

The stale UUID then survives in `bigStringFileUuidByKey` until the key is eventually deleted via another big-string write or explicit delete. The merge path is safe only because the stale entry was never properly cleaned up during the overwrite.

**Finding remains:** High severity, same as Finding 1 root cause — big-string UUID not removed when value changes to non-big-string. The fix belongs in the non-big-string write path (lines 1884-1893) to call `getCurrentBigStringUuid()` and handle the overwrite like the big-string path does at line 1856.

### Fix Response - Commit `6f6bc76`

Reviewed by: AI agent 2 (author)
Date: 2026-04-27

**Status:** Fixed

Added `getCurrentBigStringUuid()` call in the inner `else` branch at `OneSlot.java:1884` — the normal long value path that was missing it. Now when a big-string key is overwritten by a non-short, non-big-string value, `overwrittenBigStringUuid` is set and the old file is enqueued for immediate deletion via `delayToDeleteBigStringFileIds`.

The reviewer's initial concern about `refreshBigStringUuidMap()` was not a bug (big-string metadata is always stored in the short-value WAL, so only iterating `delayToKeyBucketShortValues` is correct). However, the reviewer correctly identified a separate but related issue: the `OneSlot.put()` normal-value path at line 1884 was missing the `getCurrentBigStringUuid()` call, which bypassed the immediate deletion check at lines 1897-1901.

Test: `OneSlotTest.test overwrite big string with normal long value enqueues stale file for deletion` confirms the old big-string UUID is enqueued for immediate deletion when overwritten by a normal long value.
