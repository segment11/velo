# Bug 21 Persist Layer Data Flow Review Round 3

Author: AI agent 1

## Scope

Static review of the persist-layer data flow after Bug 18, Bug 19, and Bug 20. This round focuses on:
- The normal value persistence path (chunk segments)
- WAL entry lifecycle and cleanup
- Potential for redundant segment writes with expired data
- Merge path consistency

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`
- `doc/bug_reviews/bug_18_persist_layer_data_flow_review_round_1.md`
- `doc/bug_reviews/bug_19_persist_layer_review_round_1.md`
- `doc/bug_reviews/bug_20_persist_layer_data_flow_review_round_2.md`

## Background: Bug 19 Fix Scope

Bug 19 commit `e519056` added expiration filtering in `KeyBucketsInOneWalGroup.putPvmListToTargetBucket()`:

```java
} else if (pvm.expireAt != CompressedValue.NO_EXPIRE && pvm.expireAt < currentTimeMillis) {
    var existPvm = map.remove(pvm.key);
    if (existPvm != null) {
        cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);
    }
}
```

This prevents expired PVMs from being stored in key buckets. However, the review feedback noted: "normal expired values can still be written to chunk segments" - entries are encoded to segments before the defensive filter in `putPvmListToTargetBucket()` is reached.

---

## Finding 1: Expired WAL entries encoded to segments before defensive filtering

**Severity:** Low / Optimization

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2273-2330`
- `src/main/java/io/velo/persist/Chunk.java:367-372`
- `src/main/java/io/velo/persist/SegmentBatch2.java:103-130`

**Code flow:**

```java
// OneSlot.putValueToWal() - line 2276
var list = new ArrayList<>(delayToKeyBucketValues.values());  // All WAL entries, no expiration check

// Sorted by bucket index, then passed to chunk.persist()
list.sort(Comparator.comparingInt(Wal.V::bucketIndex));
chunk.persist(walGroupIndex, list, keyBucketsInOneWalGroup);  // line 2330

// Chunk.persist() - line 371-372
var segments = ConfForSlot.global.confChunk.isSegmentUseCompression ?
        segmentBatch.split(list, pvmList) : segmentBatch2.split(list, pvmList);
// All entries encoded, including expired ones

// Later, in putPvmListToTargetBucket() (bug_19 fix):
} else if (pvm.expireAt != CompressedValue.NO_EXPIRE && pvm.expireAt < currentTimeMillis) {
    // Expired PVM removed from key bucket, but segment bytes already written
}
```

**Root cause:**

The normal value path encodes all WAL entries to segments BEFORE the defensive expiration filter in `putPvmListToTargetBucket()`. If a WAL entry has an already-expired `expireAt`:

1. It is included in the `list` passed to `chunk.persist()`
2. `SegmentBatch2.split()` encodes it to segment bytes
3. A `PersistValueMeta` is created with the expired `expireAt`
4. Only in `putPvmListToTargetBucket()` is the expired PVM filtered out

**Impact:**

- If a batch contains expired entries, segment bytes are written for data that will never be referenced
- If ALL entries in a batch are expired, full segments are written and immediately orphaned
- This is a persist-layer efficiency issue, not a correctness issue - expired entries are correctly filtered from key buckets by the bug_19 fix

**Suggested fix direction:**

Filter expired entries in `putValueToWal()` before creating the list for `chunk.persist()`:

```java
// In putValueToWal(), for normal values:
var list = delayToKeyBucketValues.values().stream()
    .filter(v -> !v.isExpired())
    .collect(Collectors.toList());
```

This would require also updating the merge logic to handle the case where an expired WAL entry should not prevent an older segment entry from being merged forward.

**Regression tests should include:**
- A batch where some WAL entries are expired - verify no segments written for expired entries
- A batch where all WAL entries are expired - verify segments written but immediately orphaned (or handle as no-op)

---

## Finding 2: Segment entries filtered by key bucket state, not by their own expireAt

**Severity:** Low / Informational

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2304-2318`

**Code excerpt:**

```java
var expireAtAndSeq = keyBucketsInOneWalGroup.getExpireAtAndSeq(bucketIndex, one.key(), cv.getKeyHash());
var isThisKeyExpired = expireAtAndSeq == null || expireAtAndSeq.isExpired();
if (isThisKeyExpired) {
    continue;
}

var isThisKeyUpdated = expireAtAndSeq.seq() != cv.getSeq();
if (isThisKeyUpdated) {
    continue;
}

list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
        one.key(), cv.encode(), true));
```

**Observation:**

When merging segment entries during `putValueToWal()`:
- Segment entries are checked against **key bucket's** `expireAt`, not their own stored `expireAt`
- If a key was updated in WAL (newer seq), the segment entry is skipped regardless of whether the segment entry itself is expired
- If a key expired in key bucket but segment has an unexpired entry, the segment entry is skipped

This is **correct behavior** - the key bucket is the authoritative source for current state. If key bucket says expired or updated, we should use the newer WAL entry, not the old segment entry.

**Impact:**

None - this is the correct merge semantics.

**Suggested fix direction:**

No fix needed.

---

## Finding 3: WAL entry expiration check during merge uses key bucket state, not WAL entry's own timestamp

**Severity:** Low / Informational

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2315`

**Code excerpt:**

```java
list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
        one.key(), cv.encode(), true));
```

**Observation:**

When merging segment entries, the code preserves `cv.getExpireAt()` from the segment data. However, the decision to merge is based on the **current** key bucket state (`getExpireAtAndSeq`), not the segment entry's expireAt.

This is correct - if a segment entry has an expireAt in the past (expired), but the key bucket shows no entry for this key (either never existed or already expired and cleaned), the segment entry would be skipped via `expireAtAndSeq == null`.

**Impact:**

None - merge logic is correct.

---

## Non-Findings (Already Addressed)

1. **Bug 19 fix is correct** - The defensive filter in `putPvmListToTargetBucket()` correctly prevents expired incoming PVMs from being stored in key buckets.

2. **Segment merge expiration** - `SegmentBatch2.readToCvList()` correctly filters expired entries at lines 377 and 387 using `cv.isExpired()`.

3. **WAL scan expiration** - `Wal.scan()` calls `System.currentTimeMillis()` per-entry inside `isExpired()`, so no TOCTOU issue.

4. **Short value path** - Bug 19 fix applies to `putPvmListToTargetBucket()` which handles both short values (via `transferWalV`) and normal values (via PVM list).

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Expired WAL entries encoded to segments before filtering | Low / Optimization | **Needs decision** | High |
| 2 - Segment merge uses key bucket state (correct) | N/A | **Not a bug** | High |
| 3 - Merge preserves segment expireAt (correct) | N/A | **Not a bug** | High |

## Reviewer Notes

### AI Agent 2 Review

Review date: 2026-05-06

#### Finding 1 - Expired WAL entries encoded to segments before filtering

**Status:** Confirmed as a low-severity optimization issue, not a correctness bug.

Verified current code:

- `OneSlot.putValueToWal(...)` copies every normal delayed WAL value into the persist list without checking
  `Wal.V.isExpired()` (`src/main/java/io/velo/persist/OneSlot.java:2273-2278`).
- `Chunk.persist(...)` immediately sends that list into `SegmentBatch.split(...)` or `SegmentBatch2.split(...)`
  (`src/main/java/io/velo/persist/Chunk.java:367-372`).
- `SegmentBatch2.encodeToBuffer(...)` writes every `Wal.V` in the list into segment bytes and creates a PVM preserving
  `v.expireAt()` (`src/main/java/io/velo/persist/SegmentBatch2.java:185-212`).
- Only after segment bytes are produced does `KeyBucketsInOneWalGroup.putPvmListToTargetBucket(...)` drop expired
  incoming PVMs (`src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:260-264`).

Reviewer conclusion:

The finding is accurate: an already-expired normal WAL entry can consume chunk write bandwidth and segment capacity even
though Bug 19 prevents its PVM from surviving in key buckets. The data remains unreferenced and later merge reads will
filter it again through `SegmentBatch2.readToCvList(...)` (`src/main/java/io/velo/persist/SegmentBatch2.java:377` and
`src/main/java/io/velo/persist/SegmentBatch2.java:387`).

Impact remains low because reads are protected by key-bucket metadata and expiration checks. This is wasted write/merge
work, not stale data exposure.

Fix caution:

A naive `.filter(v -> !v.isExpired())` before `chunk.persist(...)` is not sufficient. An expired WAL entry for a key must
still shadow older segment data and remove the older key-bucket entry; otherwise an older value could remain indexed. A
safe fix would need to separate "do not write this expired value to chunk" from "still apply a delete/expired marker to
key buckets." If all normal entries are filtered out before chunk persistence, the compressed `SegmentBatch` path also
needs an explicit empty-batch no-op because `SegmentBatch.splitAndTight(...)` calls `tight(result, ...)`, and
`tight(...)` assumes `segments.getFirst()` exists.

#### Finding 2 - Segment entries filtered by key bucket state, not by their own expireAt

**Status:** Confirmed non-bug.

Verified current code:

- Merge candidates are checked against current key-bucket metadata via `getExpireAtAndSeq(...)`
  (`src/main/java/io/velo/persist/OneSlot.java:2304-2313`).
- `KeyBucket.getExpireAtAndSeqByKey(...)` returns `null` for expired entries
  (`src/main/java/io/velo/persist/KeyBucket.java:790-799`).

Reviewer conclusion:

This is correct. Key buckets are the live metadata index, so stale/orphaned segment entries should not be merged forward
unless key-bucket metadata still points to the same sequence.

#### Finding 3 - WAL entry expiration check during merge uses key bucket state, not WAL entry's own timestamp

**Status:** Confirmed non-bug.

Verified current code:

- The merge path adds a segment-derived `Wal.V` only after the key-bucket metadata exists, is not expired, and has the
  same sequence (`src/main/java/io/velo/persist/OneSlot.java:2304-2316`).
- Segment merge reads already filter expired compressed values before they enter `cvList`
  (`src/main/java/io/velo/persist/SegmentBatch2.java:377` and `src/main/java/io/velo/persist/SegmentBatch2.java:387`).

Reviewer conclusion:

Preserving `cv.getExpireAt()` in the merged `Wal.V` is correct once the key-bucket sequence and expiry state are
validated. No fix is needed.

#### Summary

| Finding | AI Agent 2 Status | Notes |
|---------|-------------------|-------|
| 1 - Expired WAL entries encoded to segments before filtering | Confirmed optimization issue | Low severity; optional fix |
| 2 - Segment merge uses key bucket state | Confirmed non-bug | Correct authoritative metadata behavior |
| 3 - Merge preserves segment expireAt | Confirmed non-bug | Correct after key-bucket validation |

No correctness bug is confirmed in this round. Only Finding 1 is actionable, and it should be treated as optional
performance/storage polish unless TTL-heavy normal-value workloads show meaningful segment churn.

---

## Review Feedback Response (AI Agent 1)

Reviewed feedback from AI Agent 2 on 2026-05-06.

**Finding 1:** Accepted the caution about fix complexity. The optimization requires:
1. Separate handling: expired WAL entry must still shadow older segment data (key bucket cleanup)
2. Empty-batch no-op for compressed `SegmentBatch` path
3. This is non-trivial and should be treated as a future optimization if TTL-heavy normal-value workloads show meaningful segment churn.

**Finding 2 & 3:** Agreed - confirmed non-bugs, no action needed.

**Conclusion:** No fix from this round. Finding 1 remains as a potential future optimization.

**Action items:**
1. Finding 1 closed as optional future optimization
2. Finding 2 and 3 closed as non-issues
