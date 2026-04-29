# Bug 15 Persist Layer Truncate Chunk FD File Review Round 1

## Scope

Static review of the "truncate one chunk fd file" data flow in the persist layer. This flow detects when all segments in a
chunk fd file have been merged (reusable) and truncates the file to reclaim SSD space.

Reviewed paths:

- `MetaChunkSegmentFlagSeq.run()` — background check that sets `canTruncateFdIndex`
- `OneSlot.doTask()` — reads `canTruncateFdIndex` and calls `truncateChunkFile()`
- `OneSlot.truncateChunkFile()` — truncates the fd file and resets `fdLengths[fdIndex] = 0`
- `FdReadWrite.truncate()` — sets raf length to 0, resets `writeIndex`, clears LRU
- `MetaChunkSegmentFlagSeq.clear()` — called during `OneSlot.flush()`
- `OneSlot.flush()` — full slot flush
- `Chunk.resetAsFlush()` — resets `segmentIndex` but not `fdLengths`

## Finding A: `MetaChunkSegmentFlagSeq.clear()` does not reset `canTruncateFdIndex`, so a stale truncate can fire after flush

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:707-719`
- `src/main/java/io/velo/persist/OneSlot.java:1177-1180`
- `src/main/java/io/velo/persist/OneSlot.java:2022-2023`

**Code excerpt:**

```java
// MetaChunkSegmentFlagSeq.clear()
void clear() {
    try {
        var tmpBytes = new byte[allCapacity];
        fillSegmentFlagInit(tmpBytes);
        raf.seek(0);
        raf.write(tmpBytes);
        inMemoryCachedByteBuffer.position(0).put(tmpBytes);
        initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear();
        System.out.println("Meta chunk segment flag seq clear done, set init flags.");
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}
```

```java
// OneSlot.flush() — calls clear() but does NOT reset canTruncateFdIndex
if (this.metaChunkSegmentFlagSeq != null) {
    this.metaChunkSegmentFlagSeq.clear();
}
```

```java
// OneSlot.doTask() — checks canTruncateFdIndex every 10ms
var canTruncateFdIndex = metaChunkSegmentFlagSeq.canTruncateFdIndex;
if (canTruncateFdIndex != -1) {
    truncateChunkFile(canTruncateFdIndex);
}
```

**Root cause:**

`canTruncateFdIndex` is a field on `MetaChunkSegmentFlagSeq` that gets set by `run()` when all 1K-segment groups in an fd
are merged. It is only reset in two places:
1. Inside `truncateChunkFile()` at `OneSlot.java:1318`
2. Inside `updateBitSetCanReuseForSegmentIndex()` when a segment becomes non-reusable (`canReuse == false`)

`MetaChunkSegmentFlagSeq.clear()` resets the in-memory cached bytes, the RAF file, the bit sets, the marked segment indices —
but it does **not** reset `canTruncateFdIndex` to `-1`. Similarly, `OneSlot.flush()` does not reset it either.

**Scenario:**

1. Background check `run()` detects that all segments in fd 2 are merged, sets `canTruncateFdIndex = 2`.
2. Before the next `doTask()` call, a `FLUSHDB` arrives and `OneSlot.flush()` runs (same thread, so truncate hasn't fired yet).
3. `flush()` calls `metaChunkSegmentFlagSeq.clear()`, which resets all segment flags to `SEGMENT_FLAG_REUSABLE` and all
   `segmentCanReuseBitSet` entries to `true`. It also calls `chunk.resetAsFlush()` which sets `segmentIndex = 0`.
4. After flush, new writes begin. These writes go into segment index 0 (fd 0). Over time, writes may reach fd 2.
5. When `doTask()` runs next, it reads the stale `canTruncateFdIndex = 2` and calls `truncateChunkFile(2)`.
6. `truncateChunkFile(2)` calls `fd.truncate()` which truncates the file to 0 length and clears the LRU. But fd 2 may now
   contain new post-flush data that was just persisted.
7. All data in chunk fd 2 is lost.

Note: This requires the flush to happen between the `run()` check and the `truncateChunkFile()` call. Since both happen in
`doTask()` within the same method (run is called first, then truncate is checked), a `flush()` cannot interleave between them
within a single `doTask()` invocation. However, `canTruncateFdIndex` can persist across multiple `doTask()` invocations:
- In one `doTask()` call, `run()` checks fd X but only some 1K groups are merged, so `canTruncateFdIndex` stays `-1`.
- Later, a separate `run()` call for the last group sets `canTruncateFdIndex = X`.
- Between that `run()` call and the next `doTask()`, a `flush()` can occur (flush is triggered by a command handler on the
  same thread, which could run between two `doTask()` invocations since `doTask()` is event-loop scheduled every 10ms).

**Impact:**

After a `FLUSHDB` / replicated flush followed by new writes, a stale `canTruncateFdIndex` from pre-flush state can cause the
truncation of a chunk fd file that now contains new post-flush persisted data. This results in silent data loss for keys whose
values were stored in segments of that fd file. Recovery requires replaying the WAL from the beginning, but since the WAL was
also flushed, those values are permanently lost.

## Finding B: `OneSlot.flush()` does not truncate chunk fd files or reset `fdLengths[]`, so post-flush reads may return stale bytes from pre-flush segments

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2029-2031`
- `src/main/java/io/velo/persist/Chunk.java:274-276`
- `src/main/java/io/velo/persist/Chunk.java:104`
- `src/main/java/io/velo/persist/Chunk.java:550-569`
- `src/main/java/io/velo/persist/FdReadWrite.java:937-952`
- `src/main/java/io/velo/persist/FdReadWrite.java:690-694`

**Code excerpt:**

```java
// OneSlot.flush() — only resets segmentIndex, not fdLengths or chunk fd files
if (this.chunk != null) {
    this.chunk.resetAsFlush();
}
```

```java
// Chunk.resetAsFlush() — only resets segmentIndex
void resetAsFlush() {
    segmentIndex = 0;
}
```

```java
// FdReadWrite.readInnerByBuffer() — uses writeIndex as the boundary for reads
var offset = oneInnerIndex * oneInnerLength;
if (offset >= writeIndex) {
    return null;
}
```

**Root cause:**

When `OneSlot.flush()` is called:
1. It calls `metaChunkSegmentFlagSeq.clear()` which resets all segment flags to `SEGMENT_FLAG_REUSABLE`.
2. It calls `chunk.resetAsFlush()` which sets `segmentIndex = 0`.
3. However, it does **not** call `FdReadWrite.truncate()` on any chunk fd, and it does **not** reset `chunk.fdLengths[]`.
4. The chunk fd files (e.g. `chunk-data-0`, `chunk-data-1`, ...) remain at their pre-flush sizes on disk with their old bytes.

After flush, new persist operations will write new segments starting from segment index 0, which corresponds to fd 0. The
`metaChunkSegmentFlagSeq` bit sets are all set to "reusable" so new segments can be allocated. The write path uses
`FdReadWrite.writeOneInner()` / `writeSegmentsBatch()` which overwrite the old bytes in the file.

The reads are protected because `MetaChunkSegmentFlagSeq` flags are all reset to `SEGMENT_FLAG_REUSABLE`, so the merge and
read flows know the old data is gone. The `readInnerByBuffer()` method also checks `writeIndex` which is NOT reset (it
retains the pre-flush value), so reads won't return null for old offsets. However, since segment flags are cleared, the
system should not attempt to read old segment data through the normal flow.

The actual risk is more subtle: `FdReadWrite.writeIndex` retains the pre-flush value. If a read targets a segment index
within the old file range but beyond newly written data, `readInnerByBuffer()` won't return null (since `offset < writeIndex`)
and will return stale pre-flush bytes. This can happen if there's any code path that reads segments by index without first
checking the segment flag.

**Mitigating factor:** The normal read path (`OneSlot.get()`) reads through the key bucket / PVM, not directly from chunk
segments. Direct chunk segment reads happen during merge operations and repl, which both check segment flags first. So under
normal operation, stale bytes are unlikely to be served. But the FdReadWrite LRU cache is also not cleared during flush (the
chunk fd files' LRU caches are only cleared when `truncate()` is called), so if an LRU entry from a pre-flush read survives,
it could theoretically be returned.

**Impact:**

Low to medium risk in practice. The stale bytes in chunk fd files are mostly harmless because segment flags gate access.
However, the LRU cache in `FdReadWrite` for chunk fds is not cleared during flush, and stale entries may remain. If any
edge-case code path reads from the chunk fd LRU without checking segment flags, stale data could be returned. The wasted
disk space from non-truncated files is cosmetic and reclamation happens naturally as new data overwrites old segments.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| A - `clear()` does not reset `canTruncateFdIndex`, stale truncate after flush | High | Pending verification | Medium |
| B - Flush does not truncate chunk fd files or reset fdLengths/writeIndex | Medium | Pending verification | Medium |

## Suggested Fix Direction

**Finding A:** Add `canTruncateFdIndex = -1;` inside `MetaChunkSegmentFlagSeq.clear()`. Additionally, reset `checkFdIndex`
and `check1KSegmentsGroupIndex` to 0 in `clear()` so the background scan restarts from the beginning. A focused test should:
1. Populate segments so that the background check sets `canTruncateFdIndex` to a specific fd index.
2. Call `OneSlot.flush()`.
3. Assert `metaChunkSegmentFlagSeq.canTruncateFdIndex == -1`.

**Finding B:** Either truncate all chunk fd files during `OneSlot.flush()` (by calling `fd.truncate()` for each fd in
`fdReadWriteArray`), or at minimum reset `chunk.fdLengths[]` and each `FdReadWrite.writeIndex` to 0. A focused test should:
1. Write data so chunk fd files are non-empty.
2. Call `OneSlot.flush()`.
3. Assert all `chunk.fdLengths[i] == 0` and all `chunk.fdReadWriteArray[i].writeIndex == 0`.

## Review Notes - AI Agent 2

Reviewed by: AI agent 2
Date: 2026-04-29

### Finding A Confirmation

**Status:** Confirmed as a state-reset bug, with reduced reachability from the original scenario.

`MetaChunkSegmentFlagSeq.clear()` currently resets segment flags, bit sets, and marked persisted segment indexes, but it
does not reset `canTruncateFdIndex`, `checkFdIndex`, or `check1KSegmentsGroupIndex`:

```java
void clear() {
    ...
    inMemoryCachedByteBuffer.position(0).put(tmpBytes);
    initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear();
}
```

The recent initializer fix (`canTruncateFdIndex = -1`) only fixes the fresh-object default. It does not fix an already
set candidate that survives a later `clear()` / `flush()`.

One important correction: in the normal production scheduler, `OneSlot.doTask()` calls `taskChain.doTask(loopCount)` and
then immediately consumes `metaChunkSegmentFlagSeq.canTruncateFdIndex` in the same method call. There is no event-loop
interleave between `MetaChunkSegmentFlagSeq.run()` setting the candidate and `truncateChunkFile(...)` consuming it in
that same `OneSlot.doTask()` invocation. Therefore the exact "run sets candidate, flush interleaves before truncate in
the next doTask" scenario is not the normal production path.

The underlying invariant is still wrong. `clear()` represents a full reset of segment metadata, so the truncate checker's
pending candidate and scan cursor should also be reset. The fix direction is still valid:

```java
canTruncateFdIndex = -1;
checkFdIndex = 0;
check1KSegmentsGroupIndex = 0;
```

inside `MetaChunkSegmentFlagSeq.clear()` or the shared reset helper.

### Finding B Confirmation

**Status:** Partially confirmed. The stale file state is real; the claimed stale user-visible read path is not confirmed.

`OneSlot.flush()` calls `chunk.resetAsFlush()`, and `Chunk.resetAsFlush()` only resets `segmentIndex`:

```java
void resetAsFlush() {
    segmentIndex = 0;
}
```

It does not truncate chunk fd files, reset `chunk.fdLengths[]`, reset `FdReadWrite.writeIndex`, or clear chunk fd LRU
entries. So old chunk bytes can remain on disk and in fd-local LRU after flush.

However, normal segment read paths are guarded by metadata:

- `OneSlot.readForMerge(...)` calls `hasData(...)` before reading chunk bytes.
- `OneSlot.readForRepl(...)` also calls `hasData(...)` before reading chunk bytes.
- `OneSlot.flush()` clears segment metadata to reusable, so those guarded paths return `null` instead of reading old
  chunk bytes.
- Normal key reads go through WAL / key loader / PVM metadata rather than directly through arbitrary chunk segment
  indexes, and key-loader state is flushed separately.

So the doc's stale read impact is not confirmed for the normal data flow. The remaining confirmed issue is stale physical
state: chunk fd file length, `fdLengths[]`, `FdReadWrite.writeIndex`, and chunk fd LRU are not reset by flush. This can
delay disk-space reclamation and leaves misleading internal state after a full slot flush, but it is lower risk than a
confirmed stale user-read bug.

### Additional Finding From This Review

**Status:** Confirmed static bug.

`Chunk.fdLengths` is an `int[]`, but the built-in `c10m` chunk fd size can be exactly 2 GiB:

```java
c10m(512 * 1024, (byte) 2, PAGE_SIZE)
```

`512 * 1024 * 4096 == 2147483648`, which overflows `int`. `Chunk.writeSegments(...)` computes and stores fd length using
`int` math:

```java
int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
if (fdLengths[fdIndex] < afterThisBatchOffset) {
    fdLengths[fdIndex] = afterThisBatchOffset;
}
```

If writes reach the tail of a default `c10m` fd, `afterThisBatchOffset` can overflow negative. This can leave
`fdLengths[fdIndex]` stale or zero even though the fd file has data. Then `OneSlot.truncateChunkFile(...)` may skip
physical truncation because it checks `fdLength != 0` before calling `fd.truncate()`.

Suggested fix: change `Chunk.fdLengths` to `long[]`, read `file.length()` without casting, compute
`afterThisBatchOffset` as `long`, and compare against `0L` in `truncateChunkFile(...)`.

## Review Feedback - Finding A Fix Commit `f412048`

Reviewed by: AI agent 2
Date: 2026-04-29

### Summary of the Fix

Commit `f412048` resets the truncate checker state inside `MetaChunkSegmentFlagSeq.clear()`:

```java
checkFdIndex = 0;
check1KSegmentsGroupIndex = 0;
canTruncateFdIndex = -1;
```

This directly addresses Finding A's confirmed invariant issue: a full metadata clear must also clear any pending fd
truncate candidate and restart the checker cursor.

### Strengths

- The reset is placed in `MetaChunkSegmentFlagSeq.clear()`, the same method used by `OneSlot.flush()` to reset segment
  metadata.
- It resets both the pending action (`canTruncateFdIndex`) and the scan cursor (`checkFdIndex`,
  `check1KSegmentsGroupIndex`), matching the suggested fix direction.
- The existing constructor/default fix (`canTruncateFdIndex = -1`) and this clear-time reset now cover both fresh object
  state and post-clear state.

### Findings

No production-code issue found in the Finding A fix commit.

### Residual Notes

The commit does not add a focused regression test that first sets `canTruncateFdIndex`, calls `clear()` or
`OneSlot.flush()`, and asserts the marker is reset to `-1`. Existing `MetaChunkSegmentFlagSeqTest` coverage does execute
the new lines through existing `clear()` calls, but it does not assert the stale-candidate behavior directly.

Fresh verification run:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"
```

Result: passed. JaCoCo confirms the new lines are covered in
`build/reports/jacocoHtml/io.velo.persist/MetaChunkSegmentFlagSeq.java.html`: lines 717-719 are covered.

## Review Feedback - Finding B Fix Commits `501144f`, `4627175`, `cf442b2`

Reviewed by: AI agent 2
Date: 2026-04-29

### Summary of the Fix

The Bug B fix was implemented across three commits:

- `501144f` adds chunk fd truncation during `OneSlot.flush()`.
- `4627175` refactors the loop into `Chunk.truncateAll()`.
- `cf442b2` adds Javadoc and a focused `ChunkTest.test truncateAll`.

The final production path is:

```java
if (this.chunk != null) {
    this.chunk.resetAsFlush();
    this.chunk.truncateAll();
}
```

and `Chunk.truncateAll()` calls `FdReadWrite.truncate()` for tracked non-empty fds, then resets `fdLengths[i] = 0`.
`FdReadWrite.truncate()` also resets `writeIndex = 0` and clears the fd-local LRU when enabled.

### Strengths

- The fix is in the correct flush boundary. `OneSlot.flush()` is the common path for normal and replicated flush.
- The refactor keeps fd truncation owned by `Chunk`, avoiding an fd-loop detail leak in `OneSlot`.
- The fix handles the state called out in Bug B for normal tracked fds: physical file length, `FdReadWrite.writeIndex`,
  and chunk fd LRU are reset through `FdReadWrite.truncate()`, while `chunk.fdLengths[]` is reset by `Chunk.truncateAll()`.
- Fresh focused verification passed:

```bash
./gradlew :cleanTest :test --tests "io.velo.persist.ChunkTest.test truncateAll" --tests "io.velo.persist.OneSlotTest.test flush clears kv lru cache so post-flush get returns null"
```

- JaCoCo confirms the final production path executed:
  - `OneSlot.java` line 2031 (`this.chunk.truncateAll()`) is covered.
  - `Chunk.java` lines 285-286 (`fdReadWriteArray[i].truncate()` and `fdLengths[i] = 0`) are covered.

### Findings

No production-code issue found in the Finding B fix commits for the normal tracked-fd flush path.

### Residual Notes

The focused `ChunkTest.test truncateAll` proves `fdLengths[]` resets, but it does not directly assert that
`FdReadWrite.writeIndex` is reset or that a real non-empty chunk fd file becomes length 0. That behavior is provided by
`FdReadWrite.truncate()` and is covered indirectly through the call path.

The separate additional finding from this review remains open: `Chunk.fdLengths` is still `int[]`, and write-length math
can still overflow near the default `c10m` 2 GiB fd size. That is distinct from Bug B's flush truncation fix and should be
handled in its own fix commit.
