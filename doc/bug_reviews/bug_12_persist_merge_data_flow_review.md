# Bug 12 Persist Layer Merge Data Flow Review

## Scope

Review of merge data flow in the Velo persist layer, focusing on the circular buffer tracking mechanism in `MetaChunkSegmentFlagSeq` and how it interacts with merge scheduling and execution.

## Finding A: Circular Buffer Overwrite Without Read Pointer Synchronization

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:427-439`
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:455-492`

**Code excerpt:**

```java
public void markPersistedSegmentIndexToTargetWalGroup(int walGroupIndex, int beginSegmentIndex, short segmentCount) {
    var beginSegmentIndexMoveIndex = beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex];
    var next = beginSegmentIndexMoveIndex + 1;
    if (next == MARK_BEGIN_SEGMENT_INDEX_COUNT) {
        next = 0;
    }

    // make sure already pre-read and cleared
    assert (beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] == 0L);

    beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] = (long) beginSegmentIndex << 32 | segmentCount << 16 | preReadFindTimesForOncePersist;
    beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex] = next;
}
```

```java
int[] findThoseNeedToMerge(int walGroupIndex) {
    if (!isOverHalfSegmentNumberForFirstReuseLoop) {
        return NOT_FIND_SEGMENT_INDEX_AND_COUNT;
    }

    var markedLongs = beginSegmentIndexGroupByWalGroupIndex[walGroupIndex];
    for (int i = 0; i < MARK_BEGIN_SEGMENT_INDEX_COUNT; i++) {
        var markedLong = markedLongs[i];
        if (markedLong != 0L) {
            // ... process entry
        }
    }
    return NOT_FIND_SEGMENT_INDEX_AND_COUNT;
}
```

**Root cause:**

The circular buffer uses only a write pointer (`beginSegmentIndexMoveIndexGroupByWalGroupIndex`) but `findThoseNeedToMerge` has no corresponding read pointer. It iterates from index 0 to 99 regardless of where data was actually written.

When `markPersistedSegmentIndexToTargetWalGroup` wraps around and writes to position `next`, the assertion at line 435 checks that `markedLongs[next] == 0L`. However:
1. The assertion only runs in dev builds with `-ea` enabled
2. In production, if `next` points to an entry that `findThoseNeedToMerge` hasn't processed yet, that entry is silently overwritten

**Scenario that causes data loss:**
1. Buffer fills: entries written at positions 95-99 (write pointer = 99)
2. `findThoseNeedToMerge` starts reading from index 0, processes entries at 0-10, returns for this iteration
3. Before `findThoseNeedToMerge` is called again, more persist calls happen
4. New entries overwrite positions 0-4 (write pointer wrapped)
5. When `findThoseNeedToMerge` continues from 11, it eventually reaches 95-99
6. Position 95 now contains **new** data (segments 130-134 persisted), not the original entry (segments 120-124)
7. Original merge data is lost silently

**Impact:**

- Merge runs can be silently dropped, causing segments to never be considered for merge
- Segments that should be reused will accumulate, wasting disk space
- The system may believe certain segment runs have been tracked when they were actually overwritten

## Finding B: Split Runs Not Validated Against Current Segment State Before Merge

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:478-483`

**Code excerpt:**

```java
if (findTimes == 1) {
    short halfSegmentCount = (short) (segmentCount / 2);
    short leftSegmentCount = (short) (segmentCount - halfSegmentCount);

    markedLongs[i] = (long) (segmentIndex + halfSegmentCount) << 32 | leftSegmentCount << 16;
    return new int[]{segmentIndex, halfSegmentCount};
}
```

**Root cause:**

When a large run is split via `findTimes == 1` path, the returned segments are assumed to still contain valid persisted data. But by the time `readForMerge` actually reads the segments, they might have been:
- Already merged (marked reusable by another path)
- Overwritten by new writes

The code doesn't re-check segment flags before returning them for merge.

**Impact:**

Merge operations may attempt to read segments that are no longer in a valid state, potentially causing wasted I/O or incorrect data to be included in the new segment.

## Summary

| Finding | Severity | Status |
|---------|----------|--------|
| A - Circular buffer overwrite without read pointer | High | Confirmed, with scenario refined |
| B - Split runs not validated before merge | Medium | Not confirmed as independent bug |

## Reviewer Notes

### AI agent 2 review - 2026-04-28

#### Finding A: Confirmed, with scenario refined

I confirm the core bug. `markPersistedSegmentIndexToTargetWalGroup` advances a per-WAL-group write pointer and writes into a fixed 100-entry ring (`MetaChunkSegmentFlagSeq.java:427-438`). The only protection against overwriting an uncleared slot is an `assert` (`MetaChunkSegmentFlagSeq.java:435`), which is normally disabled in production JVMs. `findThoseNeedToMerge` clears or rewrites entries only when the merge pre-read path asks for one entry (`MetaChunkSegmentFlagSeq.java:455-492`), so a WAL group can enqueue more than 100 persisted ranges before enough merge reads drain that ring.

The exact scenario in the author note needs one correction: `findThoseNeedToMerge` does not keep a read cursor that later "continues from 11"; every call scans from index 0. That does not remove the bug. The simpler failure is:

1. The write pointer starts at 0 and the first mark is written to index 1.
2. If 100 marks for the same WAL group are written without being drained, all ring entries become nonzero.
3. The 101st mark wraps to index 1 again.
4. With assertions disabled, the nonzero entry at index 1 is overwritten silently.

Impact is valid: overwritten marks mean some persisted segment ranges may never be selected by `findThoseNeedToMerge`, so those ranges can remain non-reusable until a restart/reload or another full reconstruction path notices them. This is a real durability/space-reuse tracking bug in the merge data flow.

#### Finding B: Not confirmed as an independent bug

I do not confirm Finding B as stated for the normal merge flow. It is true that the split path in `findThoseNeedToMerge` returns a range without rechecking current segment flags (`MetaChunkSegmentFlagSeq.java:478-483`). However, the cited "by the time `readForMerge` actually reads the segments" race is not supported by the current call chain:

- `OneSlot.readSomeSegmentsBeforePersistWal` calls `findThoseNeedToMerge` and immediately calls `readForMerge` on the returned range in the same method (`OneSlot.java:2205-2213`).
- Segments from that returned range are not marked reusable until after the merged values are included in the subsequent persist flow (`OneSlot.java:2269-2327`).
- New writes pick only ranges that are already reusable via `Chunk.persist` and `findCanReuseSegmentIndex`, then mark the newly written segments as `SEGMENT_FLAG_HAS_DATA` (`Chunk.java:372-474`).

Because of that ordering, the split remainder should still refer to still-used segments in the ordinary single-slot flow. I did not find another production path that independently marks those same split-remainder segments reusable between `findThoseNeedToMerge` and `readForMerge`.

There is still a hardening concern: if stale markers exist because of Finding A, restart reconstruction issues, or external/test-only flag mutation, then the lack of flag validation can make the merge pre-read consume an unexpected segment range. That is a dependent risk, not a separately confirmed root cause from the split logic itself.

## Review Feedback - Bug A uncommitted fix - 2026-04-28

### Summary of reviewed change

The uncommitted change in `MetaChunkSegmentFlagSeq` adds `beginSegmentIndexReadIndexGroupByWalGroupIndex` and changes `findThoseNeedToMerge` to consume entries from a read pointer instead of scanning from index 0 every time. It also replaces the old assertion in `markPersistedSegmentIndexToTargetWalGroup` with a runtime overflow check.

### Strengths

- Moving to an explicit read pointer is the right direction for preserving FIFO order and avoiding the old "scan from index 0" behavior.
- Replacing the assertion with a runtime check is also the right direction because production builds cannot rely on `assert` for data-loss protection.

### Concerns

**High: the overflow guard still misses the confirmed Bug A overwrite path.**

Current code checks only:

```java
if (next == readIndex && beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] != 0L) {
    throw new IllegalStateException("Circular buffer overflow: write pointer caught up to read pointer");
}
```

This does not catch the 101st undrained write. Starting from `writeIndex = 0` and `readIndex = 0`, the first 99 writes fill slots 1 through 99. The 100th write wraps and fills slot 0, so all 100 slots are nonzero while `writeIndex` and `readIndex` are both 0. The 101st write computes `next = 1`; because `next != readIndex`, the guard does not fire and slot 1 is overwritten silently. That is the original Bug A data-loss pattern.

The guard should reject writing to any nonzero target slot, or the ring should track occupancy with an explicit count/full state. Checking only whether the write pointer has caught the read pointer is not sufficient for this representation because index equality is ambiguous between empty, partially drained, and full states.

### Verification

- `./gradlew test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` failed because `segment_common:test` has no matching test for the filter.
- `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` succeeded, but all tasks were `UP-TO-DATE`, so this did not execute a fresh regression test for the new overflow path.

### Follow-ups before commit

1. Add a focused regression test that writes more than `MARK_BEGIN_SEGMENT_INDEX_COUNT` marks for one WAL group without draining and verifies that the 101st mark cannot overwrite an uncleared entry.
2. Re-run the root test task with a non-up-to-date execution, then inspect the JaCoCo report for `MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup` and the overflow branch.
3. Commit Bug A only after the focused regression test fails on the old behavior and passes with the corrected guard.

## Review Feedback - Bug A committed fix `bfa22ca` - 2026-04-28

### Summary of the fix

Commit `bfa22ca` updates `MetaChunkSegmentFlagSeq` to add per-WAL-group read-pointer tracking, rejects writes to any non-empty marker slot, updates `findThoseNeedToMerge` to consume from the read pointer, and adds a regression test for the full-buffer 101st mark case.

### Strengths

- The committed overflow guard now checks `beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] != 0L`, so it closes the specific silent overwrite path found in Bug A.
- The regression test fills all 100 marker slots and verifies the 101st mark throws `IllegalStateException`.
- The split-run encoding precedence issue from the uncommitted review was corrected.

### Concerns

**Medium: full-buffer read order is not FIFO, and the first drain from a full buffer may not free the next writable slot.**

Both write and read pointers are initialized to 0. Since `markPersistedSegmentIndexToTargetWalGroup` writes to `moveIndex + 1`, the first mark goes to slot 1 and the 100th mark wraps to slot 0. When the buffer is full, `findThoseNeedToMerge` starts at read index 0 and therefore consumes slot 0 first, even though slot 1 is the oldest mark. After that one drain, slot 0 is free but `moveIndex` is still 0, so the next write targets slot 1 and still throws because slot 1 remains nonzero.

This no longer loses data, but under a full-buffer burst it can still fail a persist even after one merge read made capacity available. The fix should either initialize the write pointer so the first write lands at slot 0, initialize/read from the actual oldest slot, or use an explicit queue count/head/tail representation that makes full and empty states unambiguous.

### Verification

- Ran `./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` successfully.
- Inspected `build/reports/jacocoHtml/io.velo.persist/MetaChunkSegmentFlagSeq.html`; `markPersistedSegmentIndexToTargetWalGroup` reports 100% instruction and branch coverage, and the overflow throw line is covered.
- `findThoseNeedToMerge` is covered overall, but the `segmentCount == 1` consume branch is currently uncovered after the test change.

### Post-commit follow-ups

1. Add a regression test for the full-buffer drain-then-write sequence: fill 100 marks, call `findThoseNeedToMerge` once, then mark one more entry. The expected behavior should be decided explicitly.
2. Add or restore coverage for `findThoseNeedToMerge` with `segmentCount == 1`, because the current test replaced that branch coverage with the overflow regression.

## Follow-up Decision - Bug A design adjustment - 2026-04-28

After re-checking the Velo persist-layer data flow, the two-pointer queue design is not the best fit. When flushing a WAL group, `OneSlot.putValueToWal` pre-reads a same-WAL-group segment batch through `readSomeSegmentsBeforePersistWal` before calling `chunk.persist`, and only after the new persist does `Chunk.persist` mark the new segment run. The marker structure is therefore a bounded backlog of same-WAL-group persisted runs, not a separate producer/consumer queue.

The corrected design keeps only the marker array plus the existing move/search cursor:

- `markPersistedSegmentIndexToTargetWalGroup` searches for an empty marker slot starting after the last move cursor.
- It writes only to a zero slot.
- If every marker slot is nonzero, it throws `IllegalStateException` with WAL group and segment details.
- `findThoseNeedToMerge` scans marker slots and clears or splits one candidate as before.

This preserves the important invariant from Bug A: no persisted marker entry can be overwritten silently. It also matches the pre-read-before-persist flow because one successful pre-read can free any marker slot, and the next mark can reuse that freed slot without needing FIFO ring-buffer state.

### Follow-up verification

- Added a regression for the full-buffer drain-then-write sequence. It failed against commit `bfa22ca` because the queue read pointer drained slot 0 while the next write still targeted nonzero slot 1.
- Replaced the read-pointer queue behavior with empty-slot search.
- Ran `./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` successfully.
- Inspected JaCoCo HTML for `MetaChunkSegmentFlagSeq`; both `markPersistedSegmentIndexToTargetWalGroup` and `findThoseNeedToMerge` report 100% instruction and branch coverage, including the full-buffer throw and `segmentCount == 1` consume branch.

## Clarification - Segment index overflow claim - 2026-04-28

`OperatorPrecedenceTest.groovy` constructs an artificial case with `segmentIndex = Integer.MAX_VALUE - 5` to show that:

```java
(long) (segmentIndex + halfSegmentCount) << 32
```

can overflow the `int` addition before the cast. That is true as a standalone Java expression, but it is outside Velo's valid segment-index domain.

`ConfForSlot.ConfChunk` defines the built-in chunk sizes:

- `debugMode`: `8 * 1024 * 2 = 16,384` segments
- `c1m`: `256 * 1024 * 1 = 262,144` segments
- `c10m`: `512 * 1024 * 2 = 1,048,576` segments

Even if a custom configuration uses the default `segmentNumberPerFd = 256 * 1024` with the maximum allowed `fdPerChunk = 16`, `maxSegmentNumber()` is `4,194,304`. That is far below `Integer.MAX_VALUE`, so `segmentIndex + halfSegmentCount` cannot reach the overflow case used by the test under valid Velo configuration.

Conclusion: the expression can be written in a long-safe style for readability or defensive hardening:

```java
((long) segmentIndex + halfSegmentCount) << 32
```

However, this is not a confirmed Velo bug and should not be treated as part of Bug A. Bug A remains the marker-slot overwrite/full-buffer tracking issue.
