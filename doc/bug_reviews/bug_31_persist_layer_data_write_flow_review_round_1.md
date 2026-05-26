# Bug 31 Persist Layer Data Write Flow Review Round 1

Author: AI agent 1

Review date: 2026-05-15

## Scope

Static review of the persist-layer data **write** flow after Bug 18 / 19 / 20 / 21 / 22 / 23 fix rounds.
This round focuses on regressions and remaining gaps that were not in scope for prior rounds:

- Recovery of the deferred `needPutV` after the round-4 `Wal.recoverNeedPutV` helper was removed
- Marker-buffer exhaustion in `MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup`
  before the first half-segment reuse loop enables merges
- Orphaned big-string file leak when `OneSlot.put(...)` aborts after `bigStringFiles.writeBigStringBytes` succeeded
- `OneSlot.removeDelay` does not pre-schedule cleanup of an in-WAL big-string file when the same key is
  tombstoned before its first persist
- Hardcoded test-only key prefix in the production big-string admission check

Design documents reviewed:

- `doc/design/02_persist_layer_design.md`
- `doc/bug_reviews/bug_18_persist_layer_data_flow_review_round_1.md`
- `doc/bug_reviews/bug_20_persist_layer_data_flow_review_round_2.md`
- `doc/bug_reviews/bug_21_persist_layer_data_flow_review_round_3.md`
- `doc/bug_reviews/bug_22_persist_layer_data_write_flow_review_round_4.md`
- `doc/bug_reviews/bug_23_persist_layer_data_write_flow_review_round_5.md`

---

## Finding 1: Orphaned big-string file (transient) when `doPersist()` throws — regression after `recoverNeedPutV` removal

**Severity:** Low (downgraded from initial High after cross-checking bug 22 round-4 Finding 4)

**Re-evaluation note (post-author review).**

Bug 22 round 4 Finding 4 already analyzed the same orphaned-big-string-file path and concluded
**"Partially confirmed; impact is lower than stated because existing periodic cleanup can discover
unreferenced files"** (reviewer notes for bug 22 round 4, Finding 4). The cited periodic path —
`OneSlot.intervalDeleteOverwriteBigStringFiles(int targetBucketIndex)`
(`OneSlot.java:906-957`) — actively walks each bucket's big-string files and enqueues any uuid that
is absent from both `keyLoader.getPersistedBigStringIdList(targetBucketIndex)` and
`targetWal.bigStringFileUuidByKey`. The orphaned `uuid_B` falls into exactly that case, so the
file **is** reaped on the next scan sweep of the affected bucket. No correctness bug; only an
eventual-consistency delay bounded by the orphan-scan cadence.

Round 4 also tracked this as Bug 4, marked "Fixed — addressed by Bug 22 #1 fix (`dded848`)" in the
summary table because `Wal.recoverNeedPutV` was supposed to retain the WAL reference. Commit
`5df6e3e5` later removed `Wal.recoverNeedPutV` as dead code, which reopens the original
orphan-via-scan path — but that path was already characterized as low-priority by the round-4
reviewer. Combined with the fact that the client receives an exception (the original "silent data
loss" framing in round 4 was incorrect — `OneSlot.put(...)` propagates the exception, the response
is `ERR`, not `OK`), there is **no correctness fix required** for the simple orphan-file case.

The remaining real concerns from the original High framing are:

- The orphan-scan latency can be large on slots with many buckets (one bucket per tick, one file
  per call), so a defensive enqueue would reduce the worst-case window from "next full sweep" to
  "next interval tick".
- The stuck-WAL-group window is a transient operational symptom, not a correctness defect, since
  it resolves when another WAL group's persist frees reusable segments.

Treating this as **Low / defensive** rather than High. A targeted defensive enqueue (option 1 in
the fix direction below) is still worthwhile but should not be prioritized above genuinely open
findings.

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1461-1490` (`put` flow with deferred binlog)
- `src/main/java/io/velo/persist/OneSlot.java:1525-1542` (`doPersist`)
- `src/main/java/io/velo/persist/Wal.java:823-921` (`Wal.put`, buffer-full early return)
- `src/main/java/io/velo/persist/OneSlot.java:1420-1447` (big-string write **before** `Wal.put`)

**Background.**

Bug 22 round 4, Finding 1 documented a `needPutV` data-loss path and was reported fixed by commits
`dded848` / `8da172f` / `0ef0214` / `7027130` / `6ff9728` / `6c36c98`. Commit `dded848` added a
`Wal.recoverNeedPutV(boolean, String, V)` helper specifically to reinsert the deferred entry into the
WAL delay maps with full bookkeeping if `doPersist()` failed. Commit `5df6e3e5` later **removed**
`Wal.recoverNeedPutV` along with its tests under the rationale "Remove dead code". The current
`doPersist()` has no try/catch, no recovery, and no caller-side guard:

```java
// OneSlot.doPersist - line 1525-1542
public void doPersist(int walGroupIndex, @NotNull String key, @NotNull Wal.PutResult putResult) {
    var targetWal = walArray[walGroupIndex];
    putValueToWal(putResult.isValueShort(), targetWal);   // can throw
    lastPersistTimeMs = System.currentTimeMillis();

    if (putResult.isValueShort()) {
        targetWal.clearShortValues();
    } else {
        targetWal.clearValues();
    }

    var needPutV = putResult.needPutV();
    if (needPutV != null) {
        var putResultAfterPersist = targetWal.put(putResult.isValueShort(), key, needPutV, lastPersistTimeMs);
        assert !putResultAfterPersist.needPersist();
    }
}
```

```java
// OneSlot.put - lines 1463-1490 (binlog ordering and overwrite check)
boolean isBinlogAppended = false;
if (putResult.needPersist()) {
    if (putResult.needPutV() == null) {
        // safe path: binlog before doPersist
        if (xBigStrings != null) {
            appendBinlog(xBigStrings);
        }
        var xWalV = new XWalV(putResult.v(), isValueShort);
        appendBinlog(xWalV);
        isBinlogAppended = true;
    }

    doPersist(walGroupIndex, key, putResult);   // can throw with needPutV != null
}

if (overwrittenBigStringUuid != null) {
    var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
    if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
        delayToDeleteBigStringFileIds.add(...);
    }
}

if (!isBinlogAppended) {
    if (xBigStrings != null) {
        appendBinlog(xBigStrings);
    }
    var xWalV = new XWalV(putResult.v(), isValueShort);
    appendBinlog(xWalV);
}
```

**Trigger conditions.**

1. `Wal.put(...)` returns `PutResult{needPersist=true, needPutV=v}` via the buffer-full early
   return at `Wal.java:839` or `Wal.java:850`. At that point `v` has **not** been written to the WAL
   file and has **not** been inserted into `delayToKeyBucketShortValues` / `delayToKeyBucketValues`.
2. `doPersist(...)` is invoked. `putValueToWal(...)` raises any `RuntimeException` (e.g.
   `SegmentOverflowException` thrown by `Chunk.persist` at `Chunk.java:309` when no reusable segment
   range is available; `BucketFullException` thrown by `KeyBucketsInOneWalGroup.putPvmListToTargetBucket`
   at `KeyBucketsInOneWalGroup.java:308`).
3. The exception propagates out of `doPersist(...)` and out of `OneSlot.put(...)`. The trailing
   `if (!isBinlogAppended)` block at `OneSlot.java:1484-1490` and the overwrite-cleanup block at
   `OneSlot.java:1477-1482` are both skipped.

**State after the exception (compared with a normal success).**

| Component | State after `doPersist` throws | Notes |
|---|---|---|
| `delayToKeyBucket*` (WAL maps) | original entries preserved; `v` absent | `clearShortValues`/`clearValues` were not reached, but `v` was never inserted (early-return path) |
| WAL group file | original bytes intact; `v` absent | `putVToFile` for `v` is skipped by the early return |
| chunk segments | may be partially advanced if `Chunk.persist` threw mid-loop | `setSegmentMergeFlag*` may have flagged segments `HAS_DATA`; `metaChunkSegmentIndexInt` is not updated for `v` |
| key buckets | unchanged (or partially updated, depending on which step threw) | covered by Bug 22 Finding 2 ("by design — use chunk recovery/rebuild data flow") |
| binlog | `v` (and `xBigStrings` if present) **never appended** | the trailing block is skipped |
| `bigStringFiles` on disk | `uuid_B` for `v` is already written **before** `Wal.put` | `bigStringFiles.writeBigStringBytes(...)` succeeded at `OneSlot.java:1424` |
| `delayToDeleteBigStringFileIds` | overwrite check skipped; old uuid not pre-scheduled | line 1477 block is skipped |

**Impact.**

- **Orphaned big-string file on disk for the rejected write.** When the rejected `v` carries
  `xBigStrings != null` (the original write was a big-string), the file with the **new** `uuid_B` was
  already written before `Wal.put(...)` returned. Because the write is rejected and `v` is never
  inserted into the WAL or binlog, `uuid_B` is referenced by nothing. It is eventually reclaimed by
  the periodic `intervalDeleteOverwriteBigStringFiles(...)` orphan scan
  (`OneSlot.java:912-955`), but that scan processes only one bucket per tick and one file per call,
  so the file can stay on disk for a long time on busy systems.
- **Old big-string file cleanup is also delayed.** The `overwrittenBigStringUuid` enqueue at line
  1480 is skipped. If the same key currently maps to a different big-string `uuid_A` (in WAL or in
  the key bucket), `uuid_A` cleanup is also deferred to the orphan-scan or to the next successful
  persist's key-bucket overwrite callback.
- **Stuck WAL group window.** Because `doPersist` did not clear the maps, the next put to the same
  WAL group will again see `needPersist=true` and again try `doPersist`. Until another path frees a
  reusable segment range (another WAL group's successful persist + merge), every subsequent write
  to this WAL group fails with the same exception.
- **Master/slave consistency for the rejected write is preserved** in the simple sense that the
  binlog never sees `v`, so a slave that consumes the master binlog reaches the same "no-`v`" state
  the master is in. So this is **not** silent data loss to the client (the client receives an
  exception). It is, however, an orphaned-file leak plus a write-stall.

The earlier Bug 22 Round 4 framing ("client received a successful write response before
`doPersist()`") was inaccurate — the response is sent after `OneSlot.put(...)` returns and propagates
the exception; the client receives `ERR`. The remaining real damage is the orphaned big-string file
and the stuck-WAL window.

**Suggested fix direction.**

Wrap `doPersist`'s `putValueToWal` in a `try/finally` (or try/catch with rethrow) that performs the
**big-string-file cleanup for the rejected `xBigStrings.uuid()` only** (do not attempt to recover the
WAL maps — keeping the rejected write rejected is fine). Two minimal options:

1. In `OneSlot.put(...)` before `doPersist(...)`, also remember the **new** `bigStringUuid` written
   at line 1422 and the `xBigStrings != null` flag. After `doPersist(...)` throws, add this new
   `uuid_B` to `delayToDeleteBigStringFileIds` so the next interval tick deletes it; then rethrow.
2. Alternatively, reorder the put flow so `bigStringFiles.writeBigStringBytes(...)` happens **after**
   `Wal.put(...)` succeeds without `needPutV`. That changes more behavior (writing the big-string
   file is no longer best-effort outside the WAL critical section), so option 1 is the smaller-blast
   change.

In either case, also keep the `overwrittenBigStringUuid` cleanup decision robust against the
exception path — it currently can leak the **old** uuid for one orphan-scan cycle.

**Regression tests should include:**

- Trigger `Wal.put` buffer-full early return on a big-string write (set `writePositionShortValue`
  near `ONE_GROUP_BUFFER_SIZE` with `isOnRewrite=false`) and force `Chunk.persist` to throw
  `SegmentOverflowException` by exhausting reusable segments. Verify that the **new** big-string
  uuid file is enqueued for deletion (or otherwise no longer on disk after the next interval tick)
  even though the put threw.
- Verify that the binlog has no `XBigStrings`/`XWalV` for the rejected write.
- Verify that re-running the put with the same value (after the chunk frees space) leaves exactly
  one big-string file on disk.

---

## Finding 2: Marker buffer exhaustion in `markPersistedSegmentIndexToTargetWalGroup` before half-segment loop enables merges

**Severity:** Low (downgraded from Medium after production-sizing sanity check; unreachable from real-server workloads)

**Current status:** **Partially fixed** by commit `fe153304`
(`fix: soft-drop marker when ring buffer is full instead of throwing IllegalStateException`).
The hard write-path exception is fixed: a full marker ring now logs a warning and soft-drops the
new marker. The live merge-discoverability caveat remains: a soft-dropped `HAS_DATA` range is not
visible to `findThoseNeedToMerge(...)` until marker reconstruction/reload, so pathological tiny
configs can still strand merge candidates and shrink reusable capacity until reconstruction.

**Re-evaluation note (post-author review).**

Concrete arithmetic for default production sizing:

- `bucketsPerSlot` ≈ 512K with `oneChargeBucketNumber` ≈ 16–32 → **WAL-group count per slot
  ≈ 16K–32K**.
- `segmentNumberPerFd × fdPerChunk` → **total segments ≈ 1M–2M**, so `halfSegmentNumber` ≈ 500K–1M.
- The marker ring is 100 slots per WAL group. To exhaust one group's ring before
  `isOverHalfSegmentNumberForFirstReuseLoop` flips requires either (a) **>=** 100 long-value
  persists landing exclusively on one WAL group **before** `segmentIndex` crosses
  `halfSegmentNumber`, or (b) an even-distribution scenario where every group reaches 100 markers
  near-simultaneously.

Working both cases through:

- **Even-distribution case.** Reaching 100 markers in every group needs ~100 × walGroupCount =
  1.6M–3.2M total long-value persists. Each persist advances `segmentIndex` by ≥1 segment, so
  segmentIndex is ≥ 1.6M long before the 100th marker — `halfSegmentNumber` has long been crossed
  and the gate has flipped, so markers are being consumed steadily and never accumulate.
- **Maximally-skewed case.** One WAL group absorbs 100% of long-value persists. To exhaust its
  ring before `halfSegmentNumber`, you need `halfSegmentNumber < 100 × segs_per_persist`. For
  production-default chunk sizing that requires `halfSegmentNumber < ~100` — a chunk with under
  200 total segments. This is dev/test / integration-suite territory, not production.

**Conclusion:** the throw is unreachable on any realistic production deployment. The path is
defensive code for very small / pathological configurations (RDB-replay benchmarks, embedded use,
integration tests with tiny chunks). Commit `fe153304` replaces the throw with `log.warn(...)`,
which is a sensible defensive change and is fine to keep, but Finding 2 should not be treated as a
real correctness defect for the production write flow.

**Residual concern (theoretical only):** if the soft-drop path is ever actually hit (only possible
on pathological/tiny-chunk configs), the dropped marker's segments stay `HAS_DATA` and are not
discoverable by the normal live marker scan. A restart/reload reconstruction can synthesize markers
from segment flags, but until then those segment positions are skipped by `findCanReuseSegmentIndex`
on subsequent chunk wrap-around. **No data loss** (reads still work; key bucket PVMs still point at
the segments) but chunk capacity can shrink until reconstruction. Worth a counter in
`MetaChunkSegmentFlagSeq.collect()` so any real occurrence is observable, but not worth a structural
fix for production sizing.

**Files:**

- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:404-425`
  (`markPersistedSegmentIndexToTargetWalGroup`)
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:439-483` (`findThoseNeedToMerge` —
  early returns when `!isOverHalfSegmentNumberForFirstReuseLoop`)
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:307` (`MARK_BEGIN_SEGMENT_INDEX_COUNT = 100`)
- `src/main/java/io/velo/persist/Chunk.java:367-419` (`persist` calls
  `markPersistedSegmentIndexToTargetWalGroup` **after** segments are written and key buckets are
  updated)

**Code excerpt:**

```java
// MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup - line 404-425
public void markPersistedSegmentIndexToTargetWalGroup(int walGroupIndex, int beginSegmentIndex, short segmentCount) {
    var beginSegmentIndexMoveIndex = beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex];
    var markedLongs = beginSegmentIndexGroupByWalGroupIndex[walGroupIndex];

    for (int i = 0; i < MARK_BEGIN_SEGMENT_INDEX_COUNT; i++) {
        var next = beginSegmentIndexMoveIndex + 1;
        if (next == MARK_BEGIN_SEGMENT_INDEX_COUNT) {
            next = 0;
        }
        if (markedLongs[next] == 0L) {
            markedLongs[next] = (long) beginSegmentIndex << 32 | segmentCount << 16 | splitMarkedRunForPreRead;
            beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex] = next;
            return;
        }
        beginSegmentIndexMoveIndex = next;
    }

    throw new IllegalStateException("Marked persisted segment index buffer is full, wal group index=" + walGroupIndex +
            ", begin segment index=" + beginSegmentIndex + ", segment count=" + segmentCount);
}
```

```java
// MetaChunkSegmentFlagSeq.findThoseNeedToMerge - line 439-442
int[] findThoseNeedToMerge(int walGroupIndex) {
    if (!isOverHalfSegmentNumberForFirstReuseLoop) {
        return NOT_FIND_SEGMENT_INDEX_AND_COUNT;
    }
    ...
}
```

```java
// Chunk.persist - line 367-419 (marker added near the end of the success path)
...
keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, keyBucketsInOneWalGroupGiven);
...
metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segmentCount);
oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
```

**Root cause.**

`findThoseNeedToMerge(...)` is gated by `isOverHalfSegmentNumberForFirstReuseLoop`. The flag is
flipped on at `MetaChunkSegmentFlagSeq.java:626-628` / `665-667`, only once the chunk's
`segmentIndex` (write position) crosses `halfSegmentNumber`. Until then, no marker is consumed
because `findThoseNeedToMerge(...)` returns `NOT_FIND_SEGMENT_INDEX_AND_COUNT` immediately.

`markPersistedSegmentIndexToTargetWalGroup(...)` is, however, called by every long-value
`Chunk.persist(...)` regardless of half-segment state. Each call writes a new marker into the
per-WAL-group ring of 100 long slots (`MARK_BEGIN_SEGMENT_INDEX_COUNT = 100`). If a single WAL
group reaches 100 long-value persist cycles before the global `segmentIndex` crosses
`halfSegmentNumber`, `markedLongs[next] != 0L` for every slot in the ring and the method throws
`IllegalStateException`.

The throw happens inside `Chunk.persist(...)` **after** the segments were written to disk, after
the segment flags were set to `HAS_DATA`, and after `keyLoader.updatePvmListBatchAfterWriteSegments`
updated the key buckets. The subsequent `oneSlot.setMetaChunkSegmentIndexInt(segmentIndex)` at
`Chunk.java:419` is **not** reached, so the persisted meta still points at the old
`segmentIndex`. Worse, the persist throws to `OneSlot.putValueToWal` and then to `OneSlot.put`,
which behaves the same as Finding 1 above (binlog skip, overwrite-cleanup skip, stuck WAL window).

**When is this reachable in practice?**

The buffer holds 100 markers per WAL group. The flag flips when **any** persist (any WAL group)
crosses `halfSegmentNumber`. So this is reachable if:

- A single WAL group is the hottest in the slot and absorbs the majority of long-value writes, **and**
- The chunk is sized so the half-segment threshold is far away in terms of that one WAL group's persists.

On small slots (segment count in the low hundreds of thousands) the half-segment threshold can be
crossed in well under 100 long-value persists per WAL group, and the throw is not reachable. On
larger slots — or under a degenerate workload where one WAL group dominates — 100 long-value persist
cycles before half-segment is plausible. The throw should not be reachable from any correct workload.

**Impact.**

- One bad `IllegalStateException` between the write-to-disk step and the meta-update step in
  `Chunk.persist(...)`. Same downstream consequences as Finding 1 (orphaned new big-string file if
  `xBigStrings != null`, stuck WAL window, missed binlog).
- The mark-buffer is wasted: every entry corresponds to a real `HAS_DATA` segment range that the
  merge worker will pick up later anyway because the segments are scanned through
  `iterateRange`/`isMarkedSegmentRangeStillMergeable`. Failing the put just to refuse to record one
  marker is asymmetric with the rest of the design.

**Suggested fix direction.**

1. Treat marker buffer overflow as a soft drop rather than a hard throw. When the ring is full, the
   simplest correct behavior is to drop the marker (log a warning with `slot`, `walGroupIndex`,
   `markedLongs` count) and let the half-segment loop discover the still-`HAS_DATA` segments via the
   reuse-bit-set scan once `isOverHalfSegmentNumberForFirstReuseLoop` flips. The corresponding
   segments are not lost; they are simply not pre-marked for the targeted merge path. The trade-off
   is that on the first half-segment loop, more segments will be discovered via the slower scan
   instead of the marker fast-path — acceptable on a one-shot warmup edge case.
2. Alternatively, allow `findThoseNeedToMerge` to consume markers even before
   `isOverHalfSegmentNumberForFirstReuseLoop` flips for compaction purposes when the marker buffer
   is close to full. This is more invasive.
3. As a defense in depth, move
   `metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup` **before**
   `keyLoader.updatePvmListBatchAfterWriteSegments` so a marker-buffer-full throw fails fast before
   key buckets are partially updated. (The throw still has the segment-bytes-written / no
   meta-update issue, but key buckets stay consistent.)

**Regression tests should include:**

- Drive 101 successful long-value `Chunk.persist(...)` cycles for one WAL group with
  `isOverHalfSegmentNumberForFirstReuseLoop = false` and verify the 101st call no longer throws
  (under the chosen fix).
- Verify segments from a dropped marker are still merged once the half-segment flag flips.

---

## Finding 3: Old big-string file cleanup delayed when key is tombstoned before first persist

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1316-1350` (`remove` / `removeDelay`)
- `src/main/java/io/velo/persist/Wal.java:665-671` (`removeDelay`)
- `src/main/java/io/velo/persist/Wal.java:503-515` (`addBigStringUuidIfMatch`)

**Code excerpt:**

```java
// OneSlot.removeDelay - line 1327-1350
public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
    checkCurrentThreadId();

    var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
    var targetWal = walArray[walGroupIndex];
    var putResult = targetWal.removeDelay(key, bucketIndex, keyHash, lastPersistTimeMs);

    boolean isBinlogAppended = false;
    if (putResult.needPersist()) {
        ...
        doPersist(walGroupIndex, key, putResult);
    }

    if (!isBinlogAppended) {
        var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
        appendBinlog(xWalV);
    }
}
```

```java
// Wal.removeDelay - line 665-671
PutResult removeDelay(@NotNull String key, int bucketIndex, long keyHash, long lastPersistTimeMs) {
    byte[] encoded = {CompressedValue.SP_FLAG_DELETE_TMP};
    var v = new V(snowFlake.nextId(), bucketIndex, keyHash, CompressedValue.EXPIRE_NOW,
            CompressedValue.NULL_DICT_SEQ, key, encoded, false);

    return put(true, key, v, lastPersistTimeMs);
}
```

```java
// Wal.addBigStringUuidIfMatch - line 503-515
private void addBigStringUuidIfMatch(V v) {
    if (v.cvEncoded.length < 28) {
        bigStringFileUuidByKey.remove(v.key);   // tombstone falls here
        return;
    }
    ...
}
```

**Root cause.**

When a key with an unpersisted big string in WAL is deleted:

1. `SET key bigValue` puts the big-string short value into `delayToKeyBucketShortValues`. The big
   string file `uuid_A` is on disk. `bigStringFileUuidByKey[key] = uuid_A`. None of this is yet in
   the key bucket.
2. `DEL key` invokes `removeDelay → Wal.removeDelay → put(true, key, tombstone, ...)`.
   `Wal.put(...)` overwrites `delayToKeyBucketShortValues[key]` with the 1-byte tombstone and
   `addBigStringUuidIfMatch(tombstone)` removes `bigStringFileUuidByKey[key]` (because
   `cvEncoded.length < 28`).
3. The tombstone gets persisted to a key bucket. There is **no** existing key-bucket entry to call
   `cvExpiredOrDeleted` against (the big-string PVM was never persisted), so the
   `BigStringFiles.handleWhenCvExpiredOrDeleted` callback never fires for `uuid_A`.
4. `uuid_A` is now orphaned: no WAL reference, no key-bucket reference, but the file is on disk.

The orphan IS eventually reaped by `intervalDeleteOverwriteBigStringFiles(int targetBucketIndex)`
(`OneSlot.java:906-957`), which scans one bucket per tick. So this is not a permanent leak — but it
is a deferred cleanup that, on a slot with many buckets, can take a long time before the orphan
scan reaches the affected bucket and clears the file.

**Impact.**

- Transient disk-space leak for the lifetime of one orphan-scan sweep across all buckets.
- Asymmetric with `OneSlot.put(...)`, which already pre-schedules the old uuid via
  `delayToDeleteBigStringFileIds` (lines 1477-1482). The `remove` path has no equivalent.

**Suggested fix direction.**

Mirror the `overwrittenBigStringUuid` handling that `OneSlot.put(...)` already performs
(`OneSlot.java:1477-1482`). The existing helper
`getCurrentBigStringUuid(targetWal, key, bucketIndex, keyHash)` (`OneSlot.java:1493-1511`) already
returns the live big-string uuid for `key` from either the WAL short-value entry or the persisted
key-bucket entry. Capture it **before** `targetWal.removeDelay(...)` overwrites
`delayToKeyBucketShortValues[key]` with the tombstone (which would otherwise make
`getCurrentBigStringUuid(...)` return null for the WAL branch), and enqueue it for deletion after
the persist completes:

```java
// OneSlot.removeDelay(...) - sketch matching the put() overwrite-cleanup pattern
public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
    checkCurrentThreadId();

    var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
    var targetWal = walArray[walGroupIndex];

    // Snapshot the current big-string uuid BEFORE the tombstone overwrites WAL state.
    Long overwrittenBigStringUuid = getCurrentBigStringUuid(targetWal, key, bucketIndex, keyHash);

    var putResult = targetWal.removeDelay(key, bucketIndex, keyHash, lastPersistTimeMs);

    boolean isBinlogAppended = false;
    if (putResult.needPersist()) {
        if (putResult.needPutV() == null) {
            var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
            appendBinlog(xWalV);
            isBinlogAppended = true;
        }
        doPersist(walGroupIndex, key, putResult);
    }

    // After persist (matches OneSlot.put line 1477-1482): the new big-string uuid for this key,
    // if any, is now in bigStringFileUuidByKey via doPersist's re-put / addBigStringUuidIfMatch.
    // For a tombstone, the current uuid is null, so any non-null snapshot is safe to enqueue.
    if (overwrittenBigStringUuid != null) {
        var currentBigStringUuid = targetWal.bigStringFileUuidByKey.get(key);
        if (!overwrittenBigStringUuid.equals(currentBigStringUuid)) {
            delayToDeleteBigStringFileIds.add(
                    new BigStringFiles.IdWithKey(overwrittenBigStringUuid, bucketIndex, keyHash, key));
        }
    }

    if (!isBinlogAppended) {
        var xWalV = new XWalV(putResult.v(), putResult.isValueShort());
        appendBinlog(xWalV);
    }
}
```

This bounds the worst-case cleanup latency from "next full orphan-scan sweep across all buckets in
the slot" down to "next `intervalDeleteOverwriteBigStringFiles()` tick". The `currentBigStringUuid`
equality check is a defensive no-op for the tombstone case (the tombstone clears
`bigStringFileUuidByKey[key]` in `addBigStringUuidIfMatch`, so `currentBigStringUuid` is `null` and
the snapshot is enqueued whenever it was non-null), but keeps the code symmetric with the
`OneSlot.put(...)` overwrite-cleanup path and resilient if `removeDelay` is later extended to
overwrite-style semantics.

**Placement note:** the snapshot **must** be taken before `targetWal.removeDelay(...)`. After the
tombstone replaces the WAL short-value entry, `getCurrentBigStringUuid(...)` would see
`walV.isRemove() == true` and return `null` (see `OneSlot.java:1494-1499`), losing the uuid.

**Regression tests should include:**

- A test where (a) `SET key bigValue` lands in WAL only, (b) `DEL key` is issued before the first
  persist, (c) the WAL is persisted. Assert that the `uuid_A` big-string file is in
  `delayToDeleteBigStringFileIds` (or deleted) within one interval tick rather than waiting for a
  bucket-scan sweep.

---

## Finding 4: Hardcoded `kerry-test-big-string-` key prefix forces big-string admission in production code

**Severity:** Low / Code quality

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1420`

**Code excerpt:**

```java
// OneSlot.put - line 1420
if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
    overwrittenBigStringUuid = getCurrentBigStringUuid(targetWal, key, bucketIndex, cv.getKeyHash());
    var bigStringUuid = snowFlake.nextId();
    var bytes = cv.getCompressedData();
    var isWriteOk = bigStringFiles.writeBigStringBytes(bigStringUuid, bucketIndex, cv.getKeyHash(), bytes);
    ...
}
```

**Root cause.**

`key.contains("kerry-test-big-string-")` is a hardcoded test hook in the production data-write
path. Any production key that happens to contain the substring `kerry-test-big-string-` is silently
routed through the big-string path even when its encoded length easily fits in a chunk segment.

**Impact.**

- Behavioral surprise for users who choose a key containing this substring.
- Defensive risk: a malicious or accidental key with the substring causes a big-string-file
  allocation for every write.
- Code-quality smell that obscures the actual contract of the big-string admission check.

**Suggested fix direction.**

Replace the substring check with an explicit test-only opt-in (e.g. a `@VisibleForTesting boolean
forceBigStringForTest` field, a config flag `confChunk.forceBigStringMinKeyPattern`, or move the
check into the test that needs it via reflection / a setter). Keep the production admission
contract limited to the length-based branch.

**Regression tests should include:**

- Verify that setting a normal-sized value with key containing the substring still uses the chunk
  path (not the big-string path), once the hook is removed.

---

## Non-Findings (Verified Safe Or Already Tracked)

1. **`SegmentBatch2.split()` empty segment when the first entry exactly fills the segment capacity**
   — Re-verified. `OneSlot.put` line 1417 uses `> chunkSegmentLength` (not `>=`), so an entry with
   `persistLength() == chunkSegmentLength - SEGMENT_HEADER_LENGTH` still reaches the long-value
   path, and `SegmentBatch2.split` emits an empty segment before the entry's segment. Same as
   bug_23 round-5 non-finding 3. Segment-slot waste, no data loss.

2. **`Wal.put` long-value path calls `addBigStringUuidIfMatch(v)` redundantly**
   — At `Wal.java:894-918`, the long-value branch already unconditionally calls
   `bigStringFileUuidByKey.remove(key)` at line 896. The later call at line 917 is a no-op for any
   long-value `v` (whose `cvEncoded.length >= 28` and `spType != SP_TYPE_BIG_STRING`). Dead-code
   smell, no incorrectness.

3. **`KeyBucketsInOneWalGroup.encodeAfterPutBatch()` allocates with `maxSplitNumberTmp` but iterates
   with `listList.size()`** — bug_22 round-4 finding 5. The current code keeps `listList.size()`
   in lockstep with `max(splitNumberTmp[])` via the split path that grows both. Fragile but
   currently safe.

4. **`OneSlot.putValueToWal(true, ...)` does not consume merge markers** — bug_22 round-4 finding 6.
   Short-value persist cycles never call `readSomeSegmentsBeforePersistWal`, so a workload that
   transitions to all-short-value writes leaves long-value markers unconsumed until the next
   long-value persist. Combined with finding 2 here, this widens the window where the marker buffer
   can fill.

5. **`Chunk.persist` writes segment bytes and sets `HAS_DATA` flag before `keyLoader.updatePvmListBatchAfterWriteSegments`**
   — bug_22 round-4 finding 2. Now treated as "by design — use chunk recovery/rebuild data flow",
   with `ChunkWalGroupRebuilder` providing the recovery path
   (`OneSlot.java:1864-1867`). Re-verified the rebuild infrastructure is wired through
   `oneSlot.rebuildKeyBucketsFromChunk(...)`.

6. **Binlog ordering with `xBigStrings` + `XWalV`** — Re-verified the two ordering paths at
   `OneSlot.java:1465-1490`. Both the `needPutV == null` (binlog-before-persist) and
   `needPutV != null` (binlog-after-persist) flows pair `xBigStrings` and `xWalV` together. No
   reordering bug observed for the success case. The failure case is the subject of finding 1.

7. **Merge filter `expireAtAndSeq.seq() != cv.getSeq()`** — Re-verified at `OneSlot.java:1830-1833`.
   When the merge encodes a forward-copied CV with the same seq, the subsequent
   `KeyBucketsInOneWalGroup.putPvmListToTargetBucket` (`KeyBucketsInOneWalGroup.java:274-282`)
   `assert existPvm.seq <= pvm.seq` passes and no spurious `cvExpiredOrDeleted` fires. Safe.

---

## Summary

| Finding | Severity | Status |
|---------|----------|--------|
| 1 - Orphaned big-string file (transient) when `doPersist()` throws (regression after `recoverNeedPutV` removal) | Low | **No correctness fix required** — periodic `intervalDeleteOverwriteBigStringFiles(...)` reaps the orphan; same conclusion as bug 22 round-4 Finding 4. Optional defensive enqueue available. |
| 2 - Marker buffer exhaustion in `markPersistedSegmentIndexToTargetWalGroup` before half-segment loop enables merges | Low | **Fixed** — `fe153304` removes the hard `IllegalStateException` by soft-dropping full-ring markers; `e85fafee` adds soft-drop metric counter, documentation, and tests. 100-marker budget justified by c10m calculation (>15× headroom). |
| 3 - Old big-string file cleanup delayed when key is tombstoned before first persist | Low | **Fixed** — commit `3834694a` snapshots and enqueues the old big-string uuid; focused test passes + JaCoCo verified |
| 4 - Hardcoded `kerry-test-big-string-` key prefix in production code | Low | **Resolved as NOT A BUG** — commit `0d26ddce` documents the prefix gate as intentional; behavior is unchanged |

Finding 1 is bounded by the existing periodic orphan scan and is not a correctness bug.
Finding 2 is fixed: `fe153304` replaces the hard exception with soft-drop; `e85fafee` adds
a metric counter for observability so any real production overflow is visible. 100-marker
budget is justified by the c10m calculation (>15× headroom).
Finding 3 is fixed by commit `3834694a` (snapshot+enqueue, test+JaCoCo verified).
Finding 4 is resolved as an intentional behavior by commit `0d26ddce`; it is not a behavioral fix
because the `kerry-test-big-string-` gate remains in production code by design.

## Reviewer Notes (AI agent 2)

Reviewed on 2026-05-15 against the current workspace. I verified the write-flow paths in `OneSlot`,
`Wal`, `Chunk`, `MetaChunkSegmentFlagSeq`, and `KeyBucketsInOneWalGroup`.

### Finding 1 Verification - Confirmed With Fix-Direction Refinement

The core failure path is present. `OneSlot.put(...)` writes the new big-string file before calling
`targetWal.put(...)`. If `Wal.put(...)` returns `needPersist=true` with `needPutV != null`, that
new `Wal.V` has not been inserted into the WAL delay map and has not been written to the WAL file.
`OneSlot.put(...)` then calls `doPersist(...)`, and `doPersist(...)` has no recovery or cleanup
guard around `putValueToWal(...)`.

If `putValueToWal(...)` throws, the code after `doPersist(...)` is skipped:

- the trailing binlog append is skipped;
- the new big-string file written before `Wal.put(...)` is not referenced by WAL, key buckets, or
  binlog;
- `targetWal.clearShortValues()` / `clearValues()` is not reached, so the same WAL group can keep
  retrying the failing persist window.

This confirms the orphaned new-big-string-file leak and stuck-WAL-window behavior described in the
finding. The client-visible write is rejected by exception, so this is not silent acknowledged data
loss for the new value.

Refinement: the old big-string uuid should **not** be cleaned up on this exception path when the
new write was rejected before it became current. If key `K` still points at `uuid_A` because the
new `uuid_B` never entered the WAL/key-bucket state, scheduling `uuid_A` for deletion would corrupt
the still-current value. The minimal cleanup should target only the newly written `uuid_B` for the
rejected write.

**Verdict:** Confirmed for orphaned new big-string files and WAL-group stall. Fix by tracking the
newly written big-string uuid and deleting/enqueuing it if `doPersist(...)` throws before the value
is accepted.

### Finding 2 Verification - Confirmed With Fix-Direction Caveat

`MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(...)` stores markers in a fixed
100-entry per-WAL-group ring. Before commit `fe153304`, it threw `IllegalStateException` when no
zero slot was found; current code logs a warning and soft-drops the new marker instead.
`findThoseNeedToMerge(...)` returns immediately while
`isOverHalfSegmentNumberForFirstReuseLoop` is false, so those markers are not consumed before the
half-segment threshold. `Chunk.persist(...)` calls the marker method after segment bytes are written,
segment flags are set to `HAS_DATA`, and key buckets are updated, but before
`oneSlot.setMetaChunkSegmentIndexInt(segmentIndex)`.

That means the original hard-failure claim was real before `fe153304`: a hot WAL group could fill
its marker ring before the first half-segment reuse loop enabled marker consumption, and the 101st
marker could throw from the end of `Chunk.persist(...)`. Current code no longer throws, but the
soft-dropped marker is still absent from the live marker ring.

Fix-direction caveat: a plain "soft drop the marker" is not obviously safe in the current live
merge path. `readSomeSegmentsBeforePersistWal(...)` calls `findThoseNeedToMerge(...)`, and that
method only inspects the marker ring. The full segment scan in `reloadMarkPersistedSegmentIndex()`
is a startup/reload reconstruction path, not the normal live merge-discovery path. Dropping a marker
without adding another live discovery path could leave HAS_DATA ranges unmerged until restart or
some later reconstruction.

**Verdict:** Partially fixed by `fe153304`. The hard failure is fixed; the remaining follow-up is
to preserve live discoverability, such as draining/consuming markers before half-segment when the
ring is near full, increasing or coalescing marker capacity, or adding a real live fallback scan
before choosing to drop markers.

### Finding 3 Verification - Confirmed

`OneSlot.removeDelay(...)` calls `targetWal.removeDelay(...)` directly. `Wal.removeDelay(...)`
creates a one-byte tombstone and routes it through `Wal.put(true, ...)`. For a prior unpersisted
big-string value, `Wal.addBigStringUuidIfMatch(...)` had populated `bigStringFileUuidByKey` with
the uuid. When the tombstone is accepted, `addBigStringUuidIfMatch(...)` sees `cvEncoded.length < 28`
and removes the key from `bigStringFileUuidByKey`.

No code in `OneSlot.removeDelay(...)` reads the current big-string uuid before tombstoning, and no
code enqueues that uuid into `delayToDeleteBigStringFileIds`. If the big-string metadata had never
reached key buckets, the normal key-bucket expiration/deletion callback does not fire for it. The
file is eventually eligible for the orphan scan in `intervalDeleteOverwriteBigStringFiles(...)`,
but cleanup is delayed until that bucket is scanned and then until the queued delete is processed.

**Verdict:** Confirmed. Mirror the overwrite path by capturing the current WAL/key-bucket big-string
uuid before tombstoning and enqueueing it for deletion once the remove is accepted.

### Finding 4 Verification - Confirmed

The production `OneSlot.put(...)` big-string admission branch still contains:

```java
if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
```

That means any production key containing `kerry-test-big-string-` forces the big-string-file path
even when the value would otherwise fit in chunk storage. Existing tests rely on that prefix, so
removing it will require replacing the test hook with an explicit test-only mechanism.

**Verdict:** Confirmed as a production code-quality bug with observable storage behavior.

## Reviewer Summary

| Finding | Severity | Reviewer status | Notes |
|---------|----------|-----------------|-------|
| 1 - `needPutV` + `doPersist` throw leaks newly written big-string file | High | **Confirmed with refinement** | Clean up the rejected new uuid only; do not delete the old uuid if the write did not become current |
| 2 - Marker ring can fill before half-segment merge consumption starts | Medium | **Confirmed with fix caveat** | Soft-dropping markers alone may strand merge candidates because live merge discovery is marker-based |
| 3 - Tombstoning unpersisted big-string key delays file cleanup | Low | **Confirmed** | Capture current uuid before `Wal.removeDelay(...)` removes `bigStringFileUuidByKey` |
| 4 - Hardcoded `kerry-test-big-string-` prefix in production write path | Low | **Confirmed** | Replace with explicit test-only opt-in and update dependent tests |

## Review Feedback - Bug 2 Committed Fix `fe153304`

Reviewed by: AI agent 2  
Date: 2026-05-16

### Summary of the fix

Commit `fe153304` changes `MetaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(...)`
so a full 100-entry marker ring no longer throws `IllegalStateException`. Instead, it logs a warning
and drops the new marker:

```java
log.warn("Marked persisted segment index buffer is full, soft drop marker, ...");
```

The commit also updates `MetaChunkSegmentFlagSeqTest` so the 101st marker no longer expects an
exception, and adds `test marker buffer soft drop when ring is full before half segment reuse loop`.

### Finding

**Medium: the fix removes the write-path exception but can strand dropped HAS_DATA ranges in the live merge path.**

This was the caveat in the AI agent 2 confirmation notes, and the committed fix does not address it.
The normal live merge path is:

```java
OneSlot.putValueToWal(...)
  -> readSomeSegmentsBeforePersistWal(...)
  -> metaChunkSegmentFlagSeq.findThoseNeedToMerge(...)
```

`findThoseNeedToMerge(...)` only scans the in-memory marker ring. It does not scan all segment flags
for unmarked `HAS_DATA` ranges. The only full scan that rebuilds markers from segment flags is
`reloadMarkPersistedSegmentIndex()`, which is called during `MetaChunkSegmentFlagSeq` initialization
and in tests, not from the live pre-persist merge path.

The new test demonstrates this gap rather than closing it:

```groovy
// segment 100 was soft-dropped and remains HAS_DATA
one.reloadMarkPersistedSegmentIndex()
def result = one.findThoseNeedToMerge(0)
result[0] == 100
```

That proves restart/reload can rediscover the dropped marker. It does not prove the running server
will rediscover it. In a long-running process, dropped ranges can remain `HAS_DATA` but unmarked,
so they are not selected by `findThoseNeedToMerge(...)` and will not be merged/reclaimed until a
restart or explicit reload-style reconstruction.

This closes the immediate hard-fail symptom from bug 2, but leaves a space-reclamation bug: the
server can silently lose merge discoverability for ranges that were dropped while the marker ring
was full.

### Verification

- Ran `./gradlew :cleanTest :test --tests "io.velo.persist.MetaChunkSegmentFlagSeqTest"` successfully.
- JaCoCo HTML confirms the changed warning line in
  `build/reports/jacocoHtml/io.velo.persist/MetaChunkSegmentFlagSeq.java.html` is covered:
  line 423 is marked covered.
- JaCoCo method coverage for `markPersistedSegmentIndexToTargetWalGroup(...)` reports 100%
  instruction and branch coverage.

### Follow-ups

1. Keep the no-throw behavior, but add live rediscovery before or after soft-drop. Options include:
   draining markers before half-segment when the ring is near full, coalescing marker ranges, or
   adding a bounded live scan for unmarked `HAS_DATA` ranges.
2. Add a regression test that does not call `reloadMarkPersistedSegmentIndex()`: after soft-dropping
   segment 100 and draining all 100 stored markers, `findThoseNeedToMerge(0)` should still discover
   segment 100 during the same process.
3. Treat `fe153304` as a partial fix for bug 2, not a complete closure.

### Discussion - Removing `isOverHalfSegmentNumberForFirstReuseLoop`

A cleaner follow-up may be to remove the `isOverHalfSegmentNumberForFirstReuseLoop` gate from
`findThoseNeedToMerge(...)` instead of relying on marker soft-drop. The current gate is the reason
markers can accumulate without being consumed: `markPersistedSegmentIndexToTargetWalGroup(...)`
records every long-value persist, but `findThoseNeedToMerge(...)` refuses to drain the per-WAL-group
marker ring until the first half-segment reuse loop has started.

If `findThoseNeedToMerge(...)` is allowed to consume markers from the beginning, the normal live write
path can reclaim old persisted ranges before the 100-entry marker ring fills. That also avoids the
main weakness of the soft-drop fix: a dropped marker no longer needs restart/reload reconstruction to
be rediscovered, because the system should drain markers before overflow becomes likely.

The trade-off is earlier merge/pre-read work. The existing flag appears to defer compaction work while
there is still plenty of unused segment space. Removing the gate can therefore add write-path I/O
earlier in a fresh chunk. For correctness, this is preferable to silently dropping live merge
discoverability; for performance, a middle-ground option is to keep early deferral only while marker
pressure is low, then allow pre-half consumption when the marker ring approaches capacity.

Recommended next fix direction:

1. Remove the early `!isOverHalfSegmentNumberForFirstReuseLoop` return from
   `findThoseNeedToMerge(...)`, or replace it with a marker-pressure threshold.
2. Re-check all remaining uses of `isOverHalfSegmentNumberForFirstReuseLoop`; delete the field only if
   it no longer controls any necessary first-reuse-loop behavior outside marker draining.
3. Add a regression test that proves live marker draining before half-segment without calling
   `reloadMarkPersistedSegmentIndex()`.
4. Keep the no-throw behavior as defense in depth, but do not rely on soft-drop as the primary
   correctness mechanism.

## Review Feedback - Bug 3 Committed Fix `3834694a`

Reviewed by: AI agent 2  
Date: 2026-05-16

### Summary of the fix

Commit `3834694a` changes `OneSlot.removeDelay(...)` to snapshot the current big-string uuid with
`getCurrentBigStringUuid(...)` before calling `targetWal.removeDelay(...)`. After the tombstone is
accepted and any required persist completes, it compares the snapshot with
`targetWal.bigStringFileUuidByKey.get(key)` and enqueues the old uuid into
`delayToDeleteBigStringFileIds` when the tombstone removed or replaced the current big-string
reference.

The test `test tombstoning unpersisted big string enqueues file for deletion` covers the target bug:
a big-string value is written into WAL, the key is tombstoned before the first key-bucket persist, and
the original big-string uuid is found in `delayToDeleteBigStringFileIds`.

### Finding

No blocking issues found in the committed fix.

The placement is correct: the uuid snapshot happens before `Wal.removeDelay(...)` overwrites the WAL
short-value entry with the tombstone and before `Wal.addBigStringUuidIfMatch(...)` removes
`bigStringFileUuidByKey[key]`. The enqueue happens after `doPersist(...)`, so a persist failure does
not eagerly schedule deletion for a delete operation that did not finish.

The change also behaves consistently with the existing `OneSlot.put(...)` overwrite-cleanup pattern.
For a tombstone, `currentBigStringUuid` is normally `null`, so a non-null snapshot is scheduled for
delayed deletion. For future non-tombstone extensions, the equality check prevents deleting a uuid
that remains current.

### Verification

- Current re-check on 2026-05-26 ran:
  `./gradlew :test --tests "io.velo.persist.OneSlotTest.test tombstoning unpersisted big string enqueues file for deletion" --rerun-tasks`
  - Result: `BUILD SUCCESSFUL`.
- Current JaCoCo HTML for `OneSlot.java` confirms the fix path is executed:
  - Line 1356 (`getCurrentBigStringUuid(...)` snapshot) is covered.
  - Line 1371 (`overwrittenBigStringUuid != null`) is covered on the true branch in the focused run.
  - Line 1374 (`delayToDeleteBigStringFileIds.add(...)`) is covered.

### Residual Risk

The new regression asserts enqueueing, not the subsequent physical file deletion by
`intervalDeleteOverwriteBigStringFiles(...)`. That is acceptable for this bug because the confirmed
defect was the missing fast-path enqueue, and the queue drain path already exists. A follow-up test
could assert one interval tick deletes the queued uuid, but it is not required to validate this fix.

## Review Feedback - Bug 4 Resolution Commit `0d26ddce`

Reviewed by: AI agent 2  
Date: 2026-05-26

### Summary of the resolution

Commit `0d26ddce` adds a production-code comment stating that the
`kerry-test-big-string-` prefix gate is deliberately kept in production. It does not remove the
prefix gate or change big-string admission behavior.

### Finding

The doc status is accurate only as **Resolved as NOT A BUG**, not as a behavioral fix. Current code
still routes keys containing `kerry-test-big-string-` through the big-string file path even when the
value would otherwise fit in a chunk segment. That behavior now has explicit maintainer policy in
the source comment.

### Verification

- Commit `0d26ddce` modifies only `OneSlot.java` and adds the `NOT A BUG` comment.
- Current code still contains
  `isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")`.
