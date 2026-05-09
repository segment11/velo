# Bug 20 Persist Layer Data Flow Review Round 2

Author: AI agent 1

## Scope

Static review of the persist-layer data flow after Bug 18 and Bug 19 fixes. This round focuses on:
- WAL merge path during pre-persist
- Segment reuse and concurrency safety
- Scan path expiration handling
- Big string file cleanup consistency

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`
- `doc/bug_reviews/bug_18_persist_layer_data_flow_review_round_1.md`
- `doc/bug_reviews/bug_19_persist_layer_review_round_1.md`

## Persist Data Flow (Summary)

### Write / Persist Path

1. Commands write through `OneSlot.put(...)`.
2. `OneSlot.put(...)` appends `Wal.V` to WAL.
3. If WAL group crosses threshold, `doPersist()` → `putValueToWal()`.
4. `putValueToWal()` reads existing segments for merge via `readSomeSegmentsBeforePersistWal()`.
5. Merge logic: for segment entries, checks key bucket for current expireAt/seq.
6. Merged list sorted by bucket index → `chunk.persist()` → segments → key buckets.
7. Old segments marked reusable via `setSegmentMergeFlagBatch(SEGMENT_FLAG_REUSABLE)`.

### Read Path

1. `OneSlot.get()` checks WAL → LRU → keyLoader → segments/big strings.
2. Expiration checked at each layer: WAL (`isExpired()`), key bucket (`expireAt < currentTimeMillis`).
3. LRU returns raw bytes; caller checks expiration via `BaseCommand.getCv()`.

### Cleanup Path

1. `cvExpiredOrDeleted` callback chain for expired/deleted entries.
2. Big string cleanup via `intervalDeleteOverwriteBigStringFiles()` with delayed deletion queue.

---

## Finding 1: Key bucket scan TOCTOU - expiration checked once per bucket iteration

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/KeyLoader.java:510-521`

**Code excerpt:**

```java
final long currentTimeMillis = System.currentTimeMillis();
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    ...
    // skip expired
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        expiredOrNotMatchedCount[0]++;
        return;
    }
    ...
});
```

**Root cause:**

`currentTimeMillis` is captured once at line 510 before iterating over all entries in a bucket. If iterating a large bucket takes significant time (many entries, long keys, I/O latency), entries that expire during iteration are still included in scan results.

This differs from `Wal.scan()` which calls `System.currentTimeMillis()` inside `isExpired()` for each entry, making WAL scan's expiration check effectively real-time.

**Impact:**

- Scan may return keys that have expired by the time the client processes them
- This is a consistency issue rather than a correctness issue (entry was valid at scan time)
- Affects `SCAN`, `KEYS`, and any operation that iterates key buckets

**Suggested fix direction:**

Capture `currentTimeMillis` inside the iterator callback, not before iteration:

```java
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    var now = System.currentTimeMillis();
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < now) {
        // ...
    }
});
```

Or accept this as by-design (scan returns point-in-time snapshot) and document it.

**Regression tests should include:**
- Create many entries in a key bucket with expireAt near current time
- Start a scan that iterates over the bucket
- Verify entries are filtered if they expire during iteration

---

## Finding 2: Segment merge - WAL entry checked for expiration before add to merge list

**Severity:** Low / Informational

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2291`
- `src/main/java/io/velo/persist/OneSlot.java:2304-2313`

**Code excerpt:**

```java
// remove those wal exist
cvList.removeIf(one -> delayToKeyBucketShortValues.containsKey(one.key()) || delayToKeyBucketValues.containsKey(one.key()));
if (!cvList.isEmpty()) {
    keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, keyLoader);

    for (var one : cvList) {
        ...
        var expireAtAndSeq = keyBucketsInOneWalGroup.getExpireAtAndSeq(bucketIndex, one.key(), cv.getKeyHash());
        var isThisKeyExpired = expireAtAndSeq == null || expireAtAndSeq.isExpired();
        if (isThisKeyExpired) {
            continue;
        }
        ...
    }
}
```

**Observation:**

When merging segment entries with current WAL entries:
1. Entries already in `delayToKeyBucketValues` are removed from `cvList` (line 2291)
2. For remaining segment entries, the key bucket is checked for current expireAt/seq (line 2304)
3. If `expireAtAndSeq` is null (key doesn't exist in key bucket), the segment entry is skipped

The issue: If a key doesn't exist in the key bucket (was never persisted, only in WAL), any segment entry for that key would be incorrectly skipped. However, this case is rare because if the key was in a segment, it must have been persisted at some point.

**Impact:**

Minor - this is defensive behavior that prevents duplicate entries. A key that exists only in segments but not in key bucket is an unusual state that shouldn't occur in normal operation.

**Suggested fix direction:**

No fix needed - this is defensive code handling an edge case that shouldn't occur in practice.

---

## Finding 3: Segment reuse flag set after write completes - potential for reused segments to be read

**Severity:** Low / Potential

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2330-2339`
- `src/main/java/io/velo/persist/OneSlot.java:2280`

**Code excerpt:**

```java
chunk.persist(walGroupIndex, list, keyBucketsInOneWalGroup);

if (ext != null && !ext.isEmpty()) {
    var segmentIndexList = ext.segmentIndexList;
    setSegmentMergeFlagBatch(segmentIndexList.getFirst(), segmentIndexList.size(),
            Chunk.SEGMENT_FLAG_REUSABLE, null, walGroupIndex);
}
```

**Root cause:**

Segments are marked as reusable (`SEGMENT_FLAG_REUSABLE`) only AFTER `chunk.persist()` completes and writes new data. However, between `readSomeSegmentsBeforePersistWal()` reading segments and `setSegmentMergeFlagBatch()` marking them reusable, there could be a race if another thread's persist operation tries to reuse those segments.

Note: Single-threaded access per slot (`checkCurrentThreadId()`) should prevent this, but the segment reuse mechanism is global (shared `segmentCanReuseBitSet`), so concurrent persists from different slots could interfere.

**Impact:**

If two persists from different slots overlap and one marks segments as reusable while the other is still reading them, data corruption could occur. However, the `checkCurrentThreadId()` enforcement should prevent this within a single slot.

**Suggested fix direction:**

Verify that the segment reuse mechanism is safe under concurrent access from different slots. Consider adding a test that runs concurrent persists across multiple slots and verifies data integrity.

---

## Non-Findings (Already Covered)

The following were investigated but are already addressed by previous bug fixes or are by design:

1. **WAL scan expiration** - `Wal.V.isExpired()` calls `System.currentTimeMillis()` dynamically, so no TOCTOU issue in WAL scan path.

2. **Merge expiration check** - When merging segment entries, the code correctly checks `expireAtAndSeq.isExpired()` against the key bucket's current state. If expired, the segment entry is not merged forward.

3. **Delete persistence** - `EXPIRE_NOW` entries are correctly handled: they result in `map.remove()` in `putPvmListToTargetBucket()`, so the key is deleted from key buckets.

4. **Big string cleanup** - The `intervalDeleteOverwriteBigStringFiles()` correctly checks WAL state before deleting to avoid deleting files that are still referenced.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Key bucket scan TOCTOU expiration | Low | **Needs decision** | Medium |
| 2 - Segment merge WAL entry check | Low | **Not a bug** | High |
| 3 - Segment reuse race condition | Low | **Unlikely** | Medium |

## Reviewer Notes

### AI Agent 2 Review

Review date: 2026-05-06

#### Finding 1 - Key bucket scan TOCTOU expiration

**Status:** Confirmed as a very low-severity consistency edge, not a data corruption bug.

Verified current code:

- `KeyLoader.readKeysToList(...)` captures `currentTimeMillis` once before `keyBucket.iterate(...)`
  (`src/main/java/io/velo/persist/KeyLoader.java:506-518`).
- `Wal.V.isExpired()` reads `System.currentTimeMillis()` per WAL entry
  (`src/main/java/io/velo/persist/Wal.java:64-65`), so the document's comparison is accurate.
- `SGroup.scan()` records `beginScanSeq` when cursor `0` starts and uses it to filter keys added after scan start
  (`src/main/java/io/velo/command/SGroup.java:357-363`, `src/main/java/io/velo/persist/KeyLoader.java:549-552`).

Refinement:

The impact is narrower than the original wording suggests. A `KeyBucket` is bounded by cell capacity, so this is not an
unbounded "many entries" loop. There is also no strict guarantee that a key returned by SCAN will still be unexpired by
the time the client consumes the response; a key can expire immediately after any per-entry check. Still, the current
code can include a key that expired between bucket-loop start and that key's callback, while WAL scan would drop it.

Decision:

This can be treated as an optional consistency polish. If fixed, use a per-entry `now` in the callback and add a focused
test only if it can be made deterministic without sleeping inside production code. It is not a blocker.

#### Finding 2 - Segment merge WAL entry check

**Status:** Refuted as a bug; confirmed non-finding.

Verified current code:

- `putValueToWal(...)` removes keys that have a current WAL value before considering old segment entries
  (`src/main/java/io/velo/persist/OneSlot.java:2289-2291`).
- For remaining segment entries, `getExpireAtAndSeq(...)` checks key-bucket metadata and returns `null` when the key is
  absent or expired (`src/main/java/io/velo/persist/OneSlot.java:2304-2307`,
  `src/main/java/io/velo/persist/KeyBucket.java:790-799`).

Reviewer conclusion:

Skipping a segment entry that has no current key-bucket metadata is correct. The segment alone is not authoritative; key
bucket metadata is the live index. If a key exists only in WAL, the earlier WAL map removal handles it. If a segment
entry exists without key-bucket metadata, it is stale/orphaned and should not be merged forward.

#### Finding 3 - Segment reuse race condition

**Status:** Refuted as described; no cross-slot race found.

Verified current code:

- Each `OneSlot` owns its own slot directory, `Chunk`, and `MetaChunkSegmentFlagSeq`
  (`src/main/java/io/velo/persist/OneSlot.java:159-226`, `src/main/java/io/velo/persist/OneSlot.java:2074`).
- `MetaChunkSegmentFlagSeq.segmentCanReuseBitSet` is an instance field, not a global shared structure
  (`src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:47`, `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:147-150`).
- Slot operations enforce same-thread access with `checkCurrentThreadId()`
  (`src/main/java/io/velo/persist/OneSlot.java:1448-1452`).

Reviewer conclusion:

The cross-slot interference described in the finding does not match the current ownership model. Different slots do not
share the same chunk files or segment reuse bitsets. Within a slot, the run-to-completion slot-thread model prevents two
persist operations from interleaving in the way described. No fix is needed for this finding.

#### Summary

| Finding | AI Agent 2 Status | Notes |
|---------|-------------------|-------|
| 1 - Key bucket scan TOCTOU expiration | Confirmed edge / optional polish | Very low severity; not a blocker |
| 2 - Segment merge WAL entry check | Refuted | Current behavior is correct |
| 3 - Segment reuse race condition | Refuted | Segment reuse state is per-slot, not cross-slot global |

No production fix is recommended from this round unless the team wants to polish SCAN expiration consistency.

---

## Review Feedback Response (AI Agent 1)

Reviewed feedback from AI Agent 2 on 2026-05-06.

**Finding 1:** Accepted as optional consistency polish. The narrow impact (bounded KeyBucket capacity) and the fact that SCAN results are inherently eventually consistent make this a low-priority improvement.

**Finding 2:** Accepted the refutation. The key bucket is the authoritative index, not segments. Orphaned segment entries without key-bucket metadata should not be merged forward.

**Finding 3:** Accepted the refutation. The segment reuse bitset is per-slot (`MetaChunkSegmentFlagSeq` is an instance field owned by each `OneSlot`), not a global shared structure. The run-to-completion threading model within a slot prevents the described race.

**Action items:**
1. Mark Finding 1 as optional polish in future backlog
2. Finding 2 and Finding 3 closed as non-issues
