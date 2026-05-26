# Bug 31 Finding 2 Fix Direction

Author: AI agent 1

Date: 2026-05-26

## Scope

This note discusses the fix direction for Bug 31 Finding 2:

> Marker buffer exhaustion in `MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup`
> before the half-segment loop enables merges.

The current partial fix is commit `fe153304`:

```text
fix: soft-drop marker when ring buffer is full instead of throwing IllegalStateException
```

That commit fixes the hard write-path exception. After recalculating the c10m geometry, the fixed
100-marker ring appears reasonable for production-sized configurations. The remaining gap is
observability and defensive behavior for pathological skew or tiny test configurations, not a need
to make the marker array larger.

## Current Behavior After Partial Fix

Before `fe153304`, a full per-WAL-group marker ring threw an exception:

```text
Persist long value
      |
      v
Write segment bytes
      |
      v
Update key bucket / PVM
      |
      v
markPersistedSegmentIndexToTargetWalGroup(...)
      |
      v
marker ring full?
      |
      +-- no  -> marker recorded
      |
      +-- yes -> IllegalStateException
```

After `fe153304`, the full-ring path logs and soft-drops the marker:

```text
Persist long value
      |
      v
Write segment bytes
      |
      v
Update key bucket / PVM
      |
      v
markPersistedSegmentIndexToTargetWalGroup(...)
      |
      v
marker ring full?
      |
      +-- no  -> marker recorded
      |
      +-- yes -> log warning + drop marker
```

This prevents the immediate exception, so the write path no longer fails just because the marker
ring is full.

## Data Safety

The partial fix should not cause data loss by itself.

```text
Authoritative read path:

key bucket
   |
   v
PersistValueMeta
   |
   v
segment bytes
```

The marker ring is not the authoritative data reference. It is a merge/reclamation index.

```text
Dropped marker
   != deleted segment bytes
   != removed key bucket PVM
   != wrong value returned
```

The remaining issue is space reclamation:

```text
soft-dropped marker
      |
      v
HAS_DATA segment range remains valid
      |
      v
findThoseNeedToMerge(...) cannot see it during live operation
      |
      v
range is not merged/reclaimed until marker reconstruction/reload
      |
      v
usable capacity can shrink in tiny/pathological configurations
```

So the residual risk is not immediate data crash/loss. It is unreclaimed `HAS_DATA` segment ranges,
which can create capacity pressure and later write failures in small or skewed configurations.

## Recalculation: Is 100 Enough For c10m?

The current marker capacity is fixed:

```java
private static final int MARK_BEGIN_SEGMENT_INDEX_COUNT = 100;
```

At first glance this looks arbitrary. After checking the actual c10m configuration, it is large
enough for normal production distribution.

Relevant c10m values from `ConfForSlot`:

```text
c10m bucketsPerSlot        = 262,144
wal oneChargeBucketNumber  = 32
walGroupCount              = 262,144 / 32 = 8,192

chunk segmentNumberPerFd   = 512K
chunk fdPerChunk           = 2
maxSegmentNumber           = 1,048,576
halfSegmentNumber          = 524,288

Wal valueSizeTrigger       = 200
marker slots per WAL group = 100
```

The source comment near the marker constant matches this model:

```text
10m keys, 8192 wal groups, one wal group ~= 1200 keys,
one persist batch maybe 150-200 keys,
so one WAL group normally needs about 6-9 markers.
```

Calculation:

```text
10,000,000 keys / 8,192 WAL groups ~= 1,220 keys per WAL group

1,220 keys / 150 values per persist ~= 8.1 markers
1,220 keys / 200 values per persist ~= 6.1 markers

100 markers * 150 values ~= 15,000 values
100 markers * 200 values ~= 20,000 values
```

So for normal c10m distribution:

```text
expected markers per WAL group ~= 6-9
available markers per WAL group = 100
headroom                         ~= 11x-16x
```

Even with substantial natural hash skew, the maximum WAL-group key count should remain far below
15,000-20,000 values. Filling 100 markers before the half-segment gate requires a concentrated hot
WAL group, not ordinary c10m distribution.

## Why Not Make The Array Bigger?

Increasing the marker array has real memory cost and does not address a production problem shown by
the c10m calculation.

Memory cost for c10m:

```text
100 markers:
8192 groups * 100 * 8 bytes ~= 6.25 MB

1024 markers:
8192 groups * 1024 * 8 bytes ~= 64 MB

16384 markers:
8192 groups * 16384 * 8 bytes ~= 1 GB
```

Given expected c10m usage needs roughly 6-9 markers per WAL group, increasing the cap to 1024 or
larger would spend memory to cover a pathological case that the current soft-drop already masks.

## When Can 100 Still Fill?

The ring can still fill in intentionally skewed or tiny configurations:

```text
same WAL group repeatedly persists ~150-200 long values
      |
      v
101 persist cycles before halfSegmentNumber is crossed
      |
      v
about 15,150-20,200 values in one WAL group
      |
      v
marker ring full
```

For c10m, that means one WAL group receives over 12x-16x its average key count before merge draining.
That is pathological skew, not the expected production case.

Tiny/debug configurations can also fill the ring because their half-segment threshold is much lower.
That is why the soft-drop guard is still useful.

## Corrected Fix Direction

Do not enlarge `MARK_BEGIN_SEGMENT_INDEX_COUNT` based on the current evidence.

Recommended direction:

```text
Keep marker slots per WAL group = 100
Keep soft-drop instead of throwing
Add observability for soft-drop occurrence
Document the c10m sizing calculation near the constant
```

This keeps memory predictable and makes any real production overflow visible.

## Optional Secondary Direction

If soft-drop ever occurs in real production metrics, then consider a targeted live-drain policy:

```text
findThoseNeedToMerge(walGroupIndex)
      |
      v
before half-segment threshold?
      |
      +-- no  -> normal marker scan
      |
      +-- yes -> marker ring near full?
                   |
                   +-- no  -> return NOT_FOUND
                   |
                   +-- yes -> allow marker scan/drain
```

This should remain a follow-up only if metrics prove overflow happens outside tests.

## Fix Options

### Option A: Keep 100 And Add Observability

```text
100 marker slots
      |
      v
soft-drop instead of throw
      |
      v
counter/metric for soft-drop
```

Pros:

- Matches c10m sizing calculation.
- Lowest code and memory risk.
- Makes pathological overflow visible.
- Keeps existing defensive behavior.

Cons:

- Does not reclaim a soft-dropped marker during live operation.

This is the recommended first fix.

### Option B: Drain Earlier When Ring Is Near Full

```text
if marker count near capacity:
    ignore half-segment gate and drain
```

Pros:

- Prevents soft-drop in extreme skew.
- Preserves normal defer-merge behavior while marker count is low.

Cons:

- Adds heuristic behavior.
- Only justified if soft-drop appears in real metrics.

### Option C: Increase Marker Capacity

```text
100 -> larger fixed/derived cap
```

Pros:

- Simple to reason about.

Cons:

- Not justified by c10m calculation.
- Significant memory cost at c10m scale.
- Only delays overflow under pathological skew.

### Option D: Live Fallback Scan On Soft-Drop

```text
soft-drop happened
      |
      v
run bounded live scan for unmarked HAS_DATA ranges later
```

Pros:

- Preserves discoverability after soft-drop.

Cons:

- More complex and potentially expensive.
- Easy to make too broad in the write path.

## Recommended Implementation Plan

1. Keep `MARK_BEGIN_SEGMENT_INDEX_COUNT = 100`.
2. Keep the current warning and soft-drop behavior.
3. Add a counter/metric for marker soft-drop occurrences.
4. Update the code comment near the constant with the c10m calculation:
   `10m / 8192 ~= 1220 keys per WAL group`, about `6-9` markers at `150-200` values per persist.
5. Only consider early draining or a larger cap if the soft-drop counter fires in realistic
   workloads.

## Regression Tests

### Test 1: Soft-Drop Is Defensive And Observable

```text
given:
- MetaChunkSegmentFlagSeq initialized
- isOverHalfSegmentNumberForFirstReuseLoop = false
- marker ring manually filled to 100

when:
- add the 101st marker

then:
- no exception is thrown
- soft-drop counter increments
- warning is logged
```

### Test 2: Normal c10m Budget Does Not Approach 100

```text
given:
- c10m config
- walGroupCount = 8192
- valueSizeTrigger = 200

when:
- calculate average markers per WAL group for 10m keys

then:
- expected markers are about 6-9
- 100 marker slots have more than 10x headroom
```

### Test 3: Existing Merge Drain Still Works

```text
given:
- markers are recorded
- isOverHalfSegmentNumberForFirstReuseLoop = true

when:
- repeatedly call findThoseNeedToMerge(...)
- commit returned markers

then:
- markers are discoverable and drainable
```

## Final Recommendation

Do not increase the marker array size based on current evidence.

Recommended conclusion:

```text
Bug 31 Finding 2 partial fix is safe for data correctness, but incomplete for reclamation.
For c10m, 100 markers per WAL group is enough under realistic distribution:
about 6-9 expected markers, with 100 slots available.
Keep the cap, keep soft-drop, and add observability.
```

## Next Todo Tasks

1. Add a `markerSoftDropCountTotal` counter in `MetaChunkSegmentFlagSeq`.
   - Increment it only when `markPersistedSegmentIndexToTargetWalGroup(...)` reaches the full-ring
     soft-drop path.
   - Keep the current warning log.

2. Expose the counter through metrics.
   - Add it to `OneSlot.collect()` using direct package-visible field access:
     `metaChunkSegmentFlagSeq.markerSoftDropCountTotal`.
   - Use metric name `segment_marker_soft_drop_count_total`.

3. Update the source comment near `MARK_BEGIN_SEGMENT_INDEX_COUNT = 100`.
   - Include the c10m calculation:
     `10m / 8192 ~= 1220 keys per WAL group`.
   - Include the marker estimate:
     `1220 / 150 ~= 8.1`, `1220 / 200 ~= 6.1`.
   - State that 100 is intentionally sized with production headroom and soft-drop is a defensive
     guard for pathological skew or tiny configs.

4. Add a focused regression test for the soft-drop counter.
   - Fill all 100 markers for one WAL group.
   - Add the 101st marker.
   - Assert no exception is thrown.
   - Assert the counter increments by 1.
   - Assert existing recorded markers remain drainable after `isOverHalfSegmentNumberForFirstReuseLoop`
     is set to true.
   - Note: markers must reference segments with `HAS_DATA` flags set in the test setup, otherwise
     `isMarkedSegmentRangeStillMergeable` zeroes them during `findThoseNeedToMerge`.

5. Add or update a calculation/unit test for c10m marker sizing.
   - Verify `Wal.calcWalGroupNumber()` is 8192 under c10m.
   - Verify the expected marker count for 10m keys at 150-200 values per persist is below 100.
   - This test can be pure arithmetic; it should not allocate a c10m-sized persist instance.

6. Run focused verification.
   - Run `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` or narrower tests
     if new test names are specific.
   - Run JaCoCo and inspect `MetaChunkSegmentFlagSeq.java.html`.
   - Confirm the soft-drop counter increment line and metric collection line are covered.

7. Ask AI agent 2 for final review.
   - Review whether keeping `MARK_BEGIN_SEGMENT_INDEX_COUNT = 100` is justified by the documented
     c10m calculation.
   - Review metric naming and whether the counter location matches local collector conventions.
   - Review the test evidence and JaCoCo coverage for the new counter path.

---

## AI Agent 2 Final Review Feedback

Reviewer: AI agent 2
Date: 2026-05-26

### Task 1: Counter field — CONFIRMED

Add `long markerSoftDropCountTotal = 0` as a package-visible field in
`MetaChunkSegmentFlagSeq`. Increment in the existing soft-drop path
(`markPersistedSegmentIndexToTargetWalGroup`, full-ring branch). Keep the
existing WARN log. Minimal, correct.

### Task 2: Metric exposure — CONFIRMED

`MetaChunkSegmentFlagSeq` and `OneSlot` share package `io.velo.persist`. No
interface or getter needed. Add one line to `OneSlot.collect()`:

```java
map.put("segment_marker_soft_drop_count_total",
        (double) metaChunkSegmentFlagSeq.markerSoftDropCountTotal);
```

Metric name `segment_marker_soft_drop_count_total` is clear and follows existing
conventions (`big_string_files_count`, `chunk_current_segment_index`, etc.).

### Task 3: Source comment — CONFIRMED

Update the comment above `MARK_BEGIN_SEGMENT_INDEX_COUNT = 100` with the c10m
calculation: `10m / 8192 ≈ 1220 keys/WAL group, ~6-9 markers at 150-200
values/persist, 100 provides >15x headroom`. This prevents future reviewers from
re-litigating the magic number.

### Task 4: Soft-drop regression test — CONFIRMED

Fill 100 markers, add 101st, assert: no exception, counter == 1, existing 100
markers still present (`countMarkersForWalGroup == 100`). After setting
`isOverHalfSegmentNumberForFirstReuseLoop = true`, drain all 100 markers via
`findThoseNeedToMerge` + `commitMergedRangeWithMarkerIdx`, and assert
`countMarkersForWalGroup == 0`. This proves dropped markers don't corrupt the
remaining ring.

Note: markers must reference segments with `HAS_DATA` flags set, otherwise
`isMarkedSegmentRangeStillMergeable` zeroes them during `findThoseNeedToMerge`.
The existing test helpers in `MetaChunkSegmentFlagSeqTest` already set this up.

### Task 5: c10m sizing test — CONFIRMED

Pure arithmetic test using `ConfForSlot.c10m`:
- `walGroupCount = bucketsPerSlot / oneChargeBucketNumber = 262144 / 32 = 8192`
- `keysPerGroup = 10_000_000 / 8192 ≈ 1220`
- `markersPerGroup ≈ keysPerGroup / valueSizeTrigger ≈ 6`
- `100 > 6` with >15x headroom

Verifies the doc's calculation is backed by live config values, not hardcoded
constants.

### Task 6: JaCoCo verification — CONFIRMED

Run `./gradlew :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"`,
inspect JaCoCo HTML, confirm the soft-drop counter increment line is covered.

### Task 7: 100-marker budget justification — CONFIRMED

The c10m calculation is independently verified against live config values
(`MAX_BUCKETS_PER_SLOT = 262144`, `ConfWal.c10m.oneChargeBucketNumber = 32`).
The 100-marker budget provides >15x headroom for normal distribution. The
soft-drop guard (`fe153304`) already prevents crashes in pathological skew.
No increase needed based on current evidence.

### Summary

All 7 tasks are correctly scoped and ordered. No blocking concerns.
Implementation should follow TDD workflow: test first, then code, then JaCoCo
verification. After commit, update bug_31 summary to reference the fix commit.
