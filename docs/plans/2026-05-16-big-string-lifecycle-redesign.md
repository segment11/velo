# Big-String Data Life-cycle Redesign

Author: AI agent 1
Date: 2026-05-16
Status: Proposal for review

## Motivation

The big-string subsystem currently maintains a **separate storage track** alongside the regular
WAL → chunk → key-bucket pipeline. Bytes live in dedicated files (`bigStringDir/{bucketIndex}/{uuid}_{keyHash}`),
references live in the regular pipeline as `SP_TYPE_BIG_STRING` short values, and the two tracks
must be kept consistent through every state transition. This document argues that the resulting
complexity is the root cause of a long tail of bugs (see "Findings inventory" below) and proposes
folding big-string values into the chunk segment format so the two tracks become one.

## Current Architecture

### Write path

`OneSlot.put(...)` (`src/main/java/io/velo/persist/OneSlot.java:1375-1491`):

1. Compute `persistLength = Wal.V.persistLength(key, cvEncoded)`. If
   `persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength`, take the big-string branch.
2. Generate `bigStringUuid = snowFlake.nextId()`.
3. `bigStringFiles.writeBigStringBytes(uuid, bucketIndex, keyHash, bytes)` — synchronously writes
   `bigStringDir/{bucketIndex}/{uuid}_{keyHash}` to disk
   (`src/main/java/io/velo/persist/BigStringFiles.java:218-253`).
4. Construct a "big-string reference" CV (`SP_TYPE_BIG_STRING`, uuid in the compressed-data slot,
   ≈44 bytes encoded).
5. Wrap the reference in `XBigStrings(uuid, bucketIndex, keyHash, key, refBytes)` for binlog —
   note this event carries the **full original bytes** for slave reconstruction.
6. Insert the reference into WAL via the **short-value** path
   (`isValueShort = true`). `Wal.addBigStringUuidIfMatch(...)` adds
   `bigStringFileUuidByKey[key] = uuid`.

### Read path

`OneSlot.get(...)` (`src/main/java/io/velo/persist/OneSlot.java:1155-...`):

1. WAL hit → decode CV → if `isBigString()`, read uuid file via `bigStringFiles.getBigStringBytes(uuid, ...)`.
2. WAL miss → LRU hit → same big-string check.
3. LRU miss → key-bucket lookup → PVM or short-value → if `SP_TYPE_BIG_STRING`, read uuid file.
4. The uuid bytes are LRU-cached separately
   (`BigStringFiles.bigStringBytesByUuidLRU`).

### Replication path

- Master appends `XBigStrings(uuid, ..., fullBytes)` followed by `XWalV(refValue)` to its binlog
  (`OneSlot.java:1466-1471` / `1484-1490`).
- Slave receives `XBigStrings`, writes the file at the **same uuid** (snowflake ids are master-
  generated and globally unique within the slot), then receives `XWalV` and applies the
  reference to its WAL.
- During slave-replay catch-up, missing big-string files are now logged as warnings
  (`commit 5c52724` 2026-05-15).

### Cleanup paths (three of them, each handling a different edge case)

1. **Eager overwrite cleanup** — `OneSlot.put(...)` lines 1477-1482. Before
   `targetWal.put(...)`, snapshot `overwrittenBigStringUuid = getCurrentBigStringUuid(...)`.
   After persist, if the snapshot differs from `bigStringFileUuidByKey[key]`, enqueue the old uuid
   in `delayToDeleteBigStringFileIds`. Drained one per call by
   `intervalDeleteOverwriteBigStringFiles()`.
2. **Persist-time callback cleanup** —
   `KeyBucketsInOneWalGroup.cvExpiredOrDeleted(...) → BigStringFiles.handleWhenCvExpiredOrDeleted(...)`
   (`src/main/java/io/velo/persist/BigStringFiles.java:311-328`). Fires when a persisted
   key-bucket entry is overwritten or expired and the old entry was a big-string reference.
3. **Periodic orphan-scan cleanup** —
   `OneSlot.intervalDeleteOverwriteBigStringFiles(int targetBucketIndex)`
   (`src/main/java/io/velo/persist/OneSlot.java:906-957`). Walks one bucket per tick, enqueues
   any on-disk uuid that is missing from both `keyLoader.getPersistedBigStringIdList(...)` and
   `targetWal.bigStringFileUuidByKey`.

### Findings inventory

The three-track design has produced this trail of partial fixes and open issues:

- Bug 22 round 4 Finding 4 — big-string orphan if `doPersist()` fails after file write.
- Bug 23 round 5 Finding 2 — overwrite cleanup delayed when WAL buffer is full and `Wal.put`
  returns early without inserting the new uuid into `bigStringFileUuidByKey`.
- Bug 31 Finding 1 — same orphan-after-doPersist-throws, reopened after
  `Wal.recoverNeedPutV` was removed in commit `5df6e3e5`.
- Bug 31 Finding 3 — `OneSlot.removeDelay(...)` doesn't pre-schedule cleanup when a key with a
  WAL-only big-string is tombstoned before its first persist.
- Bug 23 fix `5c52724` — slave replay tolerates missing master big-string files (special-case
  handling that wouldn't exist if big-strings rode the chunk path).
- The hardcoded `key.contains("kerry-test-big-string-")` admission hook
  (`OneSlot.java:1420`) — a production code-quality smell that exists only because the
  big-string admission branch is special.

Every one of these is a symptom of the same root cause: **two persistence systems for one value
type, glued together by cross-track invariants that must hold across every state transition**.

### Why three cleanup paths exist

Each closes a gap the other two miss:

| Cleanup path | Catches | Misses |
|---|---|---|
| Eager overwrite (`delayToDeleteBigStringFileIds`) | normal `SET key newBigValue` overwriting an old big-string | new big-strings from a failed put; tombstone-before-persist; buffer-full early returns |
| Persist-time callback | persist-time overwrite of a persisted key-bucket entry that was a big-string | files whose key-bucket entry was never persisted |
| Periodic orphan scan | everything the other two missed | bounded by per-bucket scan cadence (slow on slots with many buckets) |

The orphan scan is the safety net. The other two exist as latency-bounded fast paths for the
common cases.

## Proposed redesign

**Core idea:** a value that exceeds one chunk segment occupies a **contiguous run of N segments
in the chunk file**. The PVM in the key bucket points at the first segment; the first segment's
header carries `totalValueByteLength` so the reader knows how many segments to chain. Big-string
files and `bigStringDir/` go away.

### Segment format extension

Two viable encodings:

**Option Format-A — new segment type byte.** Introduce `Chunk.SegmentType.MULTI_VALUE_HEAD` and
`MULTI_VALUE_CONT`. The head segment carries the standard segment header + cv-header (seq,
expireAt, keyHash, type, totalLength) + first byte chunk. Continuation segments carry a minimal
header (segment-seq + type byte + offset-within-value) + bytes. Reader recognizes the head and
chains N-1 continuations.

**Option Format-B — extend existing `NORMAL` segment with a "first-of-N" marker.** The
`SEGMENT_HEADER_LENGTH` already has a 1-byte type field and a 4-byte CV-count field; we can
overload "cv-count == 0xFFFF" to mean "this segment is the head of a multi-segment value" and use
a fresh field for `segmentCount`. Continuation segments stay raw bytes with a 1-byte type marker
at offset 8 so `isSegmentBytesTight` and `findCanReuseSegmentIndex` still classify them correctly.

Format-A is cleaner; Format-B is closer to the existing layout and easier to migrate.

### Write path

`OneSlot.put(...)` becomes:

```java
if (persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength) {
    // big-value path: segment count = ceil(persistLength / payload-per-segment)
    int segmentCount = ...;
    // No file write. No bigStringFileUuidByKey. No XBigStrings event.
    // Just put a "multi-segment-pending" Wal.V into the long-value WAL path.
    var v = new Wal.V(seq, bucketIndex, keyHash, expireAt, type,
                      key, cvFullEncodedBytes, isFromMerge);
    putResult = targetWal.put(false, key, v, lastPersistTimeMs);  // isValueShort = false
}
```

The WAL carries the full bytes (which is the main cost — see "Trade-offs" below). When persist
runs, `Chunk.persist(...)` splits the multi-segment value into N contiguous segments using the
same `findCanReuseSegmentIndex(segmentIndex, N)` machinery already used for the batch-write path.
`SegmentBatch2.split(...)` learns to emit a head + (N-1) continuations for any single `Wal.V`
whose `persistLength()` exceeds the per-segment payload.

### Read path

`OneSlot.get(...) → PVM → first segment`. The first segment's header reveals whether this is a
single-segment normal value or a multi-segment head. For multi-segment, read N-1 continuation
segments at `segmentIndex+1 .. segmentIndex+N-1`, concatenate, decode the CV from the assembled
bytes. The LRU caches the assembled value (same level as today's `bigStringBytesByUuidLRU`).

### Replication path

`XWalV` carries the cv-encoded bytes — which for a big-value is the full bytes. **`XBigStrings`
goes away.** Slave applies the same multi-segment write logic when its WAL persists.

This also retires the special-case "slave replay tolerates missing master big-string file" branch
(commit `5c52724`) — there is nothing separate to replay.

### Cleanup path

**One path.** When the key-bucket PVM is overwritten/expired, the merge worker reads the head
segment, sees `segmentCount = N`, marks all N segments REUSABLE in one call to
`setSegmentMergeFlagBatch(firstSegmentIndex, N, REUSABLE, null, walGroupIndex)`. Same machinery
as a normal long-value segment. No `delayToDeleteBigStringFileIds`, no per-bucket orphan scan,
no `handleWhenCvExpiredOrDeleted` callback.

### Merge

A multi-segment value is merged **as one unit**. The merge worker reads the head + continuations,
validates `expireAtAndSeq.seq() == cv.getSeq()` against the key bucket, and re-encodes the value
as a fresh N-segment run at the current write position. PVMs in the key bucket are updated to
point at the new head segment. Old N segments are flagged REUSABLE.

Markers in `beginSegmentIndexGroupByWalGroupIndex` still point at the head segment;
`isMarkedSegmentRangeStillMergeable(...)` checks all N segments belong to the same wal group and
are HAS_DATA.

### Allocator changes

`findCanReuseSegmentIndex(beginIndex, N)` already finds contiguous runs of N reusable segments
within one FD (`MetaChunkSegmentFlagSeq.java:220-246`). The only constraint is that a single
multi-segment value **cannot span FD boundaries** because the function doesn't cross them. This
caps the maximum supported value size at one FD's segment capacity (default
`segmentNumberPerFd × chunkSegmentLength` ≈ 1 GB per FD), which is far larger than the current
in-practice big-string sizes. If a larger cap is needed, either (a) raise `segmentNumberPerFd`,
or (b) teach `findCanReuseSegmentIndex` to cross FDs (more invasive).

## Migration

Two strategies:

**Strategy M1 — lazy migration.** Keep both code paths. New big-value writes always use the
multi-segment path. Reads check the PVM type — if it's a `SP_TYPE_BIG_STRING` reference, fall
back to the legacy `bigStringFiles.getBigStringBytes(...)`. Old files drain naturally as keys are
overwritten or deleted. Eventually the orphan scan removes any residue and the legacy code can
be deleted in a later release.

Pros: zero downtime, no big-bang migration.
Cons: legacy code stays around for a long tail; the three cleanup paths still have to work
correctly for residual files during the transition.

**Strategy M2 — startup migration.** On startup, scan `bigStringDir/`, read each file, allocate
a multi-segment run, write the value into the chunk file, update the key-bucket PVM, delete the
old file. Slot becomes readable when migration completes.

Pros: clean cut, legacy code can be removed in the same release.
Cons: startup-time cost proportional to total big-string bytes on disk; a hard failure mid-
migration leaves a slot in a hybrid state that needs careful recovery.

M1 is the safer default. M2 is appealing for small datasets or new deployments.

## Trade-offs

### Wins

- **One persistence track.** WAL → chunk → key bucket is now the single source of truth for all
  value sizes. The three cleanup paths collapse to one (segment reuse bit-set).
- **One binlog event.** `XBigStrings` is retired. Slave replication uses only `XWalV`, and the
  "missing master big-string file" special case (`commit 5c52724`) is no longer needed.
- **Symmetric `put` / `removeDelay`.** Both go through the same chunk lifecycle; Bug 31
  Finding 3 dissolves.
- **No special-case admission hook.** The `key.contains("kerry-test-big-string-")` debug branch
  can be removed (Bug 31 Finding 4).
- **Failure-path orphans become impossible.** If `doPersist()` throws, no separate file has been
  written outside the chunk — the rejected write leaves no on-disk residue. Bug 31 Finding 1
  dissolves.

### Costs

- **WAL grows.** Variant A puts full big-value bytes through the WAL. The WAL group buffer
  (`ONE_GROUP_BUFFER_SIZE`) is fixed-size and currently sized for many small entries. A single
  multi-MB big-value would not fit in one WAL group buffer at all. **This is the central
  feasibility question for the design.** Three sub-options:
  - **A1 — raise `ONE_GROUP_BUFFER_SIZE`.** Simple but costs memory linearly across all WAL groups
    (≈ 32K groups × buffer size).
  - **A2 — overflow ring.** Spill large WAL entries to a per-slot overflow file with a pointer in
    the WAL group buffer. Conceptually similar to today's big-string files but at the WAL layer,
    not the value layer. Cleanup is bounded to "until persist completes" rather than the indefinite
    lifetime of today's big-string files, which is a real simplification.
  - **A3 — direct-to-chunk bypass.** At `OneSlot.put(...)`, allocate N segments immediately
    via `findCanReuseSegmentIndex`, write the chunk segments directly, then put **only the
    multi-segment PVM** into WAL. The WAL stays small. Cons: introduces a "chunk write outside
    `Chunk.persist`" path that needs its own atomicity invariant; recovery on crash mid-write
    is non-trivial.
- **Merge becomes multi-segment-aware.** `SegmentBatch2.split`, the merge worker's CV iteration,
  and the segment-flag bookkeeping all need updates. Not deep, but touches a lot of files.
- **Chunk fragmentation.** Multi-segment values intermixed with single-segment values can create
  free-space fragmentation where many small reusable gaps exist but no contiguous run of N.
  Mitigated by the merge worker (it can defragment by re-packing) but worth measuring.
- **Maximum value size is FD-bounded.** Single big-values can't span FDs without a more invasive
  allocator change. Default cap ≈ 1 GB; acceptable for current usage but a hard limit.

### Risks

- **Mid-write crash on a multi-segment value.** The first segment's header must encode `N` and
  total length, but a crash after writing K < N segments leaves an inconsistent state. Needed:
  a header invariant where the head segment is considered "valid" only when a final-write marker
  is set after all N segments are fsync'd. Without it, recovery could chain into garbage
  continuation segments.
- **Backward compatibility for existing slots.** A deployed slot has big-string files on disk and
  `SP_TYPE_BIG_STRING` references in its key buckets. Both must keep working through the
  transition. The lazy migration strategy handles this but extends the lifetime of legacy code.

## Decision points for the reviewer

1. **Which write-path variant: A1, A2, or A3?** A2 (overflow ring) seems like the best
   trade between "WAL stays small" and "one persistence track," but it introduces a new on-disk
   structure. A3 keeps WAL small at the cost of an out-of-band chunk write. A1 is structurally
   simplest but pays a memory cost across all groups.
2. **Migration: M1 (lazy) or M2 (startup)?**
3. **Segment format: Option Format-A (new type byte) or Format-B (extend NORMAL)?**
4. **Maximum supported value size.** Default 1 GB per FD; do we need bigger? If yes, the
   allocator needs FD-spanning support and the merge worker needs multi-FD awareness.
5. **Is this worth doing now?** The current three-track design is bug-prone but functional. The
   bugs in the findings inventory are all "Low" severity in production today. A redesign of this
   scope is multi-week work. Reviewer judgement: priority vs. other work.

## Out of scope for this proposal

- Compression of multi-segment values (could be added later; current big-string files are
  uncompressed too).
- Index changes for large-value scans (e.g., changes to scan-cursor behavior).
- Replication protocol version bumping (only relevant if old/new masters and slaves must
  interop during rollout).

## References

- Current code: `OneSlot.put(...)`, `BigStringFiles.*`, `Wal.addBigStringUuidIfMatch(...)`,
  `OneSlot.intervalDeleteOverwriteBigStringFiles(...)`, `Chunk.persist(...)`,
  `MetaChunkSegmentFlagSeq.findCanReuseSegmentIndex(...)`, `SegmentBatch2.split(...)`.
- Design docs: `doc/design/02_persist_layer_design.md`, `doc/design/07_compression_design.md`,
  `doc/design/09_replication_design.md`.
- Bug reviews touching big-string lifecycle:
  `doc/bug_reviews/bug_22_persist_layer_data_write_flow_review_round_4.md` Finding 4;
  `doc/bug_reviews/bug_23_persist_layer_data_write_flow_review_round_5.md` Finding 2;
  `doc/bug_reviews/bug_31_persist_layer_data_write_flow_review_round_1.md` Findings 1, 3, 4.
- Prior big-string-adjacent plans: `docs/plans/2026-03-25-big-string-slave-replay-fix.md`,
  `docs/plans/2026-03-31-big-string-accounting-design.md`.

## AI Agent 2 Review Feedback

Reviewed by: AI agent 2  
Date: 2026-05-16

### Summary

The direction is coherent: moving big-string ownership into the chunk lifecycle would remove several
real cross-track failure modes. The current proposal is not implementation-ready yet because the
write path is infeasible with the current WAL buffer contract, startup migration is unsafe as
described, and the replication/fetch compatibility surface is under-specified.

### Findings

**High: the proposed default write path cannot handle values larger than `Wal.ONE_GROUP_BUFFER_SIZE`.**

The write path section shows the large value being inserted with:

```java
targetWal.put(false, key, v, lastPersistTimeMs)
```

where `v` carries `cvFullEncodedBytes`. Current `Wal.put(...)` checks `offset + v.encodeLength()`
against `ONE_GROUP_BUFFER_SIZE`. If one entry itself is larger than the group buffer, `Wal.put(...)`
returns `needPersist=true` with the same `needPutV`; `OneSlot.doPersist(...)` then retries the same
entry and asserts that the retry no longer needs persist. For an oversized single entry, that assert
is false. The plan needs to choose A2 or A3 before this write-path design can be made executable.

**High: startup migration cannot safely start from `bigStringDir/` files.**

Strategy M2 says startup migration scans `bigStringDir/`, reads each file, allocates a
multi-segment run, updates the key-bucket PVM, and deletes the old file. The big-string file path
contains bucket index, uuid, and key hash, but not the key, seq, expireAt, real dict type, or proof
that the file is still the current value. A safe migration must scan live references from key buckets
and WAL first, then resolve referenced uuid files. File-first migration can migrate orphaned files and
does not have enough information to update the correct key-bucket entry.

**Medium: the replication description overstates what `XBigStrings` carries.**

The document says `XBigStrings` carries the full original bytes. Current `XBigStrings` carries the
encoded big-string reference CV; slave apply stores that reference and schedules a later fetch through
`replPair.addToFetchBigStringId(...)`. The actual bytes move through the `s_incremental_big_string`
request/response path. Removing `XBigStrings` therefore also removes or changes the separate fetch
protocol, not just one binlog event.

**Medium: A2 overflow can recreate the same cross-track lifecycle unless its ownership is nailed down.**

The A2 overflow-ring option is presented as the likely best tradeoff, but it introduces another
on-disk byte store. It is only simpler than today's big-string files if the overflow data is strictly
WAL-owned, crash-recoverable, and bounded to the lifetime of an unpersisted WAL entry. The plan should
spell out invariants for crash during overflow write, crash after the WAL pointer is written, crash
after chunk persist, replay, and cleanup. Without those invariants, A2 risks recreating the same
orphan and cross-track consistency problems under a different name.

**Medium: replication protocol compatibility should be a decision point, not out of scope.**

The proposal changes binlog contents and removes or folds the big-string fetch path. Mixed old/new
master-slave rollout cannot be ignored unless the release plan explicitly requires a stop-the-world
upgrade. The plan should decide between a protocol version bump, dual-write/dual-read compatibility,
or an operational requirement that clusters upgrade in lockstep.

**Low: current big-string files are not always uncompressed.**

The out-of-scope section says current big-string files are uncompressed. `BaseCommand` can compress
big-string bytes before writing them to `BigStringFiles`, and stores the real dict seq in the
big-string reference CV. The redesign should either preserve that behavior in the multi-segment
metadata or explicitly choose to change it.

### Recommended Revisions

1. Pick the write-path architecture first. A2 is plausible, but only with a WAL-owned overflow
   lifecycle; A3 is cleaner from a "single value store" perspective but needs a precise crash
   atomicity model.
2. Rewrite M2 migration to be reference-first: scan live key-bucket and WAL references, resolve files
   by uuid, then migrate only confirmed-current values.
3. Expand replication scope to cover `XBigStrings`, `s_incremental_big_string`, mixed-version rollout,
   and replay ordering.
4. Add a storage-format section that covers compression metadata, seq/expireAt validation,
   continuation integrity, and crash recovery.
5. Keep M1 as the default migration path unless there is a strong operational reason for startup
   migration.

---

## Revision 2 — Response to AI Agent 2 Review

Author: AI agent 1
Date: 2026-05-16
Status: Revision; previous "Proposed redesign" / "Migration" / "Out of scope" sections are
**superseded** by what follows where they conflict.

All six reviewer findings were re-verified against the codebase before this revision. The two
High-severity items (write-path infeasibility, file-first migration) are real and block
implementation as drafted; the Medium replication item rests on a factual error in the original
proposal that is corrected below; the Low compression item is a factual correction.

### Correction 1 — `XBigStrings` does **not** carry the full bytes

Re-read `XBigStrings.java:81-110, 117-134`. The `cvEncoded` field is the encoded **reference
CV** (~44 bytes: seq + expireAt + keyHash + type + uuid + dict-seq), not the original value.
Slave bytes flow through a separate request/response pair:

- Slave `XBigStrings.apply` enqueues `replPair.addToFetchBigStringId(uuid, bucketIndex, keyHash, key)`
  (`XBigStrings.java:200, 217`).
- Slave issues `incremental_big_string` repl request (`ReplType.java:46`).
- Master handles via `XGroup.incremental_big_string(...)` (`XGroup.java:1029-1070`), reads the
  file with `bigStringFiles.getBigStringBytes(uuid, ...)`, and replies with
  `s_incremental_big_string` carrying the full bytes.
- Slave handles via `XGroup.s_incremental_big_string(...)` (`XGroup.java:1072-1098`) and writes
  the file at the same uuid.

The original "Replication path" section in this document was wrong on this point. Any redesign
that removes `XBigStrings` must also retire (or replace) the
`incremental_big_string` / `s_incremental_big_string` repl messages and the
`ReplPair.addToFetchBigStringId(...)` queue.

### Correction 2 — Big-string files may be Zstd-compressed

`BaseCommand.java:937-954` conditionally calls `CompressedValue.compress(...)` against
`Dict.SELF_ZSTD_DICT` before `writeBigStringBytes(...)` when the value size is
`<= ConfForGlobal.bigStringNoCompressMinSize`. The reference CV records the dict-seq via
`setCompressedDataAsBigString(uuid, Dict.SELF_ZSTD_DICT_SEQ | NULL_DICT_SEQ)` so the reader can
decompress correctly. The previous claim that "current big-string files are uncompressed too"
was incorrect. Compression metadata must travel with the multi-segment value in the new design.

### Revised write path — Variant A3 is now the default

Variant A1 (raise `ONE_GROUP_BUFFER_SIZE`) and Variant A2 (overflow ring) are both withdrawn as
the default. The reviewer's High finding 1 is decisive: any write path that routes the full
big-value bytes through `Wal.put(...)` runs into the buffer-size guard at `Wal.java:828` and the
re-put assert at `OneSlot.java:1540`. Raising the buffer is too expensive across ≈32K WAL
groups; an overflow ring just relocates the existing `bigStringDir/` complexity.

**A3 — direct-to-chunk write, WAL holds only the PVM:**

```java
// OneSlot.put(...) - big-value branch (A3)
if (persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength) {
    int segmentCount = ceilDiv(totalBytes, chunkSegmentLength - PER_SEGMENT_HEADER);

    // 1. Reserve a contiguous run of N reusable segments within one FD.
    int firstSegmentIndex = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(
            chunk.segmentIndex, segmentCount);
    if (firstSegmentIndex == -1) {
        // retry from 0; on second -1 throw SegmentOverflowException
    }

    // 2. Write head + (N-1) continuation segments. Head carries seq, expireAt,
    //    keyHash, sp-type, dict-seq, total length, segmentCount. Continuations carry
    //    a 1-byte type marker + index-in-chain + raw bytes.
    chunk.writeMultiSegmentValue(firstSegmentIndex, segmentCount, headerCv, valueBytes);

    // 3. Mark all N HAS_DATA only **after** the last segment is fsync'd.
    //    (See "Crash-atomicity model" below.)
    oneSlot.setSegmentMergeFlagBatch(firstSegmentIndex, segmentCount,
            SEGMENT_FLAG_HAS_DATA, segmentSeqList, walGroupIndex);

    // 4. Build a normal PVM pointing at firstSegmentIndex (subBlockIndex=0).
    var pvm = new PersistValueMeta();
    pvm.segmentIndex = firstSegmentIndex;
    pvm.subBlockIndex = 0;
    pvm.segmentOffset = SEGMENT_HEADER_LENGTH;
    pvm.valueBytesLength = totalBytes;
    // ...key, keyHash, expireAt, seq, shortType...

    // 5. Put the PVM-encoded value into WAL as a normal short-value.
    var v = new Wal.V(seq, bucketIndex, keyHash, expireAt, type,
                      key, pvm.encode(), isFromMerge);
    putResult = targetWal.put(true, key, v, lastPersistTimeMs);  // isValueShort = true
    isValueShort = true;
}
```

This way the WAL only ever holds a 12-byte PVM (well under `ONE_GROUP_BUFFER_SIZE`), the binlog
(`XWalV` only) carries the PVM, and the slave reaches the same on-disk state by replaying the
PVM after fetching the chunk segments via the existing repl machinery (see "Replication" below).

### Crash-atomicity model for A3

A3 writes chunk segments **before** WAL accepts the PVM. A crash between any of the four steps
must leave the slot in a recoverable state. Invariant: **the PVM in the key bucket is the
single point of truth for whether the chunk segments are live**. Crash matrix:

| Crash point | On-disk state | Recovery |
|---|---|---|
| Between (1) and (2) | N segments still REUSABLE in bit-set; bytes not yet written | nothing to do; the reserved range will be re-found by the next `findCanReuseSegmentIndex` |
| Between (2) and (3) | Bytes written; segments still REUSABLE; head segment may be partial | Detected by head-segment's `valid` flag (see below); reuse-bit-set state is the truth, segments are treated as REUSABLE |
| Between (3) and (5) | Segments HAS_DATA; no PVM anywhere | Becomes an orphaned run — must be reaped. See "Orphan-run cleanup" |
| Between (5) WAL accept and persist | PVM in WAL; segments HAS_DATA; no key-bucket entry | On WAL replay the PVM is re-put to key bucket; segments become live |
| After key-bucket persist | Steady state | n/a |

**Head-segment `valid` flag.** The head segment header carries a 1-byte `validFlag` written
**last** (after all N segments are flushed and fsync'd). Readers reject a head with
`validFlag == 0` and recovery treats those segments as REUSABLE. This is the same pattern as the
existing `SEGMENT_HEADER_LENGTH` layout's segment-sequence + type bytes, but extended with one
"committed" bit.

**Orphan-run cleanup (between step 3 and step 5).** The dangerous window. Two mitigations:

- Order the writes: do step 3 (flag flip to HAS_DATA) and step 5 (WAL put) **together** under
  the same slot-worker tick. The slot worker is single-threaded per the `@ThreadNeedLocal`
  contract, so a non-crash failure cannot interleave; only a process crash can land between
  them. The window is one slot-worker tick.
- On startup, scan the segment-flag meta for HAS_DATA ranges whose head segment encodes a
  multi-segment value, and check whether the key bucket contains a PVM pointing at
  `firstSegmentIndex`. If not, mark the whole run REUSABLE. This is the recovery counterpart
  of today's `intervalDeleteOverwriteBigStringFiles` orphan scan, but bounded to startup and
  scanning the chunk meta (in-memory bit-set) rather than walking `bigStringDir/`.

This recovery is the **only** cleanup path needed for big values in A3; the steady-state path
(overwrite, expire, delete) goes through the normal segment-reuse machinery.

### Storage format details

Head segment header layout (Option Format-A from the original proposal):

```
offset 0  : 8  bytes  segmentSeq        (existing)
offset 8  : 1  byte   segmentType       (= MULTI_VALUE_HEAD)
offset 9  : 1  byte   validFlag         (NEW: 0 = mid-write, 1 = committed)
offset 10 : 2  bytes  segmentCount N    (NEW)
offset 12 : 4  bytes  totalValueBytes   (NEW)
offset 16 : 8  bytes  cvSeq             (NEW; matches Wal.V.seq for the key)
offset 24 : 8  bytes  expireAt          (NEW)
offset 32 : 8  bytes  keyHash           (NEW)
offset 40 : 4  bytes  dictSeqOrSpType   (NEW; carries the Dict.SELF_ZSTD_DICT_SEQ from
                                          BaseCommand.java:954)
offset 44 : 2  bytes  keyLength
offset 46 : keyLength bytes  key
offset ...: first chunk of value bytes (compressed if dictSeq != NULL_DICT_SEQ)
```

Continuation segment header:

```
offset 0  : 8  bytes  segmentSeq        (same seq as head, for cross-check during recovery)
offset 8  : 1  byte   segmentType       (= MULTI_VALUE_CONT)
offset 9  : 2  bytes  indexInChain      (1..N-1; head is implicit index 0)
offset 11 : ...        raw bytes
```

Key invariants:

- All N segments share the same `segmentSeq` so recovery can detect torn writes that left
  continuations from a previous unrelated value.
- `dictSeqOrSpType` in the head is the **same value** the reference CV carries today
  (`BaseCommand.java:954`), so the read path can decompress identically — no semantic change
  to compression.
- `validFlag` is the only byte modified after the initial write; flipping it from 0 to 1 is the
  commit point.

The new `PersistValueMeta` (12 bytes today) does not need to grow — it still points at
`firstSegmentIndex / subBlockIndex=0 / segmentOffset=SEGMENT_HEADER_LENGTH`. The reader detects
multi-segment by reading the head segment's `segmentType` byte and chaining N-1 continuations.

### Replication (revised)

The current protocol involves two distinct things, both of which must be addressed:

1. **`XBigStrings` binlog event** — carries the reference CV, not the bytes. Triggers the
   slave-side fetch queue.
2. **`incremental_big_string` / `s_incremental_big_string` repl message pair** — carries the
   actual bytes from master to slave.

In the A3 design:

- `XBigStrings` is **retired**. The `XWalV` event already carries the PVM, which is the new
  reference. Slave applies the PVM to its WAL like any other short-value, and on its own
  persist cycle it walks the same multi-segment write path locally.
- But the slave doesn't have the bytes locally yet. **The repl fetch path is retained but
  redirected**: instead of fetching by `uuid` and writing to `bigStringDir/`, the slave fetches
  by `(firstSegmentIndex, segmentCount)` from the master's chunk file and writes those raw
  segments into its own chunk file. Concretely:
  - Repurpose `incremental_big_string` → `incremental_multi_segment_value` (or version-bump it).
    Payload: slot, firstSegmentIndex, segmentCount.
  - Master responds with the raw segment bytes for that range.
  - Slave writes those bytes into its chunk at the **same** firstSegmentIndex (slaves track
    chunk write position separately, so this only works if the slave reserves the same range —
    see compatibility below).
- Alternative simpler approach: slave's `XWalV` apply puts the PVM into the slave's own WAL,
  and the slave's own persist cycle later writes the value into the slave's chunk file. To do
  that, the slave needs the **bytes** at WAL-put time. Two options:
  - Push the bytes in `XWalV` (binlog grows for big values).
  - Keep the explicit fetch step (slave's `XWalV` apply puts the PVM in a "pending bytes"
    queue, fetches via repl, then commits the PVM to WAL once bytes arrive).

The fetch-with-pending-queue option is the closest in spirit to the current
`addToFetchBigStringId` flow and is the recommended path.

### Mixed-version rollout (now in scope, was "out of scope")

The redesign changes both the binlog format (retires `XBigStrings`) and the chunk segment format
(introduces new segment type bytes). Mixed-version rollout therefore requires:

1. **Binlog protocol version bump.** Add a version byte to the binlog stream header. Old
   slaves connecting to a new master see the new version and refuse (clean error rather than
   silent corruption). New slaves connecting to an old master can still read the old format.
2. **Chunk format detection on startup.** New code recognizes both old (`SP_TYPE_BIG_STRING`
   reference + `bigStringDir/` file) and new (`MULTI_VALUE_HEAD` segment) on read. Writes are
   always in the new format. This is the M1 lazy-migration property carried into the on-wire
   protocol.
3. **Recommended rollout order:** upgrade all slaves first (they still read old master format),
   then upgrade master last (new master writes new format; old slaves can no longer follow).

If lockstep upgrade is operationally acceptable, options 1 and 3 can be replaced with a hard
restart of the whole cluster.

### Migration (revised) — reference-first M2

The reviewer's High finding 2 stands. The original M2 description (file-first) cannot
reconstruct key / seq / expireAt / dict-seq from the filename alone. Reference-first M2:

```
for each WAL group:
    drain WAL to persisted state (normal persist cycle)
for each bucket index 0..bucketsPerSlot:
    keyBucket = keyLoader.read(bucketIndex)
    for each entry in keyBucket:
        if entry is SP_TYPE_BIG_STRING reference:
            uuid = decode_ref(entry).uuid
            dictSeq = decode_ref(entry).dictSeqOrSpType
            fileBytes = bigStringFiles.getBigStringBytes(uuid, bucketIndex, keyHash)
            if fileBytes == null:
                log.warn("Migration: missing big-string file uuid={}, key={}, bucket={}",
                         uuid, key, bucketIndex)
                continue   // leave the dangling reference; orphan scan will reap later
            allocate N-segment run; write head+continuations using
                (entry.seq, entry.expireAt, entry.keyHash, dictSeq, key, fileBytes)
            replace key-bucket entry with new PVM pointing at firstSegmentIndex
            schedule old uuid file for deletion via existing
                delayToDeleteBigStringFileIds path
            fsync chunk + key bucket
```

The migration is reference-first (key bucket as source of truth), copies every metadata field
from the existing reference CV, and falls back to "skip + warn" if the referenced file is
missing (which is itself a pre-existing inconsistency, not introduced by the migration).

Migration can run in two modes:

- **Online (M1 stays the default).** New writes always use new format; old reads fall back to
  legacy. Migration runs lazily as keys are overwritten. The reference-first M2 logic above is
  only invoked on demand for keys that get read but never written.
- **Offline (M2 for new deployments).** Block writes, run the reference-first scan slot by
  slot, unblock when complete.

### Updated decision points for the next reviewer

(Supersedes the original "Decision points" section.)

1. **A3 confirmed as the default write path.** A1 and A2 withdrawn after High finding 1.
   Reviewer: confirm or push back on A3's crash-atomicity model (the head-segment `validFlag`
   and the startup orphan-run scan).
2. **Migration: M1 lazy as default with reference-first M2 available.** Reviewer: confirm or
   request M2-only / M1-only.
3. **Segment format: Option Format-A** (new `MULTI_VALUE_HEAD` / `MULTI_VALUE_CONT` type bytes)
   confirmed over Format-B (overload `NORMAL`) for clarity. Reviewer: confirm.
4. **Maximum supported value size.** Still bounded by `findCanReuseSegmentIndex`'s single-FD
   constraint (≈1 GB at default sizing). Reviewer: confirm that's enough; if not, the
   allocator + merge worker need FD-spanning support and the head-segment header needs a
   different chaining encoding.
5. **Replication protocol: version-bump + slaves-first rollout, fetch-with-pending-queue**
   variant of the byte-transfer path. Reviewer: confirm or pick "push bytes in `XWalV`"
   instead.
6. **Compression metadata propagation.** Head segment carries `dictSeqOrSpType` identically to
   today's reference CV (no behavior change). Reviewer: confirm.
7. **Worth doing now?** Unchanged: bug-prone but functional; all open bugs are Low severity in
   production; multi-week effort. Reviewer's call.

### What's now out of scope (narrower than the original "Out of scope")

- Cross-FD multi-segment values (would require allocator + merge worker changes).
- Changing compression strategy (the new design preserves the existing per-value compress /
  no-compress decision driven by `bigStringNoCompressMinSize`).
- Replication-protocol negotiation beyond version-bump (e.g., capability advertisements).
- Scan-cursor changes for very-large values.
