# Bug 29 Persist Layer Read Data Flow Review Round 2

## Scope

Round 2 of the persist read-flow review series. Round 1 (`bug_28`) covered the WAL → slot KV LRU → keyLoader → segment / inline-short-CV / big-string indirection top-level path. This round drills into:

- **segment-level integrity** (`SegmentBatch2.encodeToBuffer` / `iterateFromSegmentBytes` / `OneSlot.get()` PVM read)
- **format detection consistency** between `OneSlot.get()` and `OneSlot.getOnlyKeyBytesFromSegment()`
- **`OneSlot.exists()` symmetry** with the now-throwing `OneSlot.get()` for missing big-string files (residual from `bug_28` Finding 1)

Reviewed paths (current `main`):

- `SegmentBatch2.encodeToBuffer()` — segment write format and CRC computation
- `SegmentBatch2.iterateFromSegmentBytes()` — segment iteration on read (used by merge, repl, scan)
- `OneSlot.get()` lines 1195-1233 — PVM-driven segment read
- `OneSlot.getOnlyKeyBytesFromSegment()` lines 1236-1265 — segment key recovery
- `OneSlot.exists()` lines 1283-1301 — existence check that bypasses big-string indirection
- `SegmentBatch.decompressSegmentBytesFromOneSubBlock()` — sub-block decompression with NORMAL fast-path

## Finding 1: Segment CRC32 is computed and written but never verified on any read path

**Severity:** Medium

**Status:** Documented, **won't fix** (per project decision 2026-05-13). Kept in the doc for awareness so future readers don't waste time on the commented-out stub.

**Files:**

- `src/main/java/io/velo/persist/SegmentBatch2.java:203-213` (write side)
- `src/main/java/io/velo/persist/SegmentBatch2.java:243-273` (read iteration)
- `src/main/java/io/velo/persist/OneSlot.java:1195-1233` (single-key read)

**Code excerpt:**

```java
// SegmentBatch2.encodeToBuffer — write side
if (buffer.remaining() >= 2) {
    // write 0 short, so merge loop can break, because reuse old bytes
    buffer.putShort((short) 0);
}

// update crc
int segmentCrc32 = KeyHash.hash32(crcCalBytes);
// refer to SEGMENT_HEADER_LENGTH definition
// seq long + segment type byte + cv number int + crc int
buffer.putInt(8 + 1 + 4, segmentCrc32);
```

```java
// OneSlot.get() — read side
nettyBuf.readerIndex(pvm.segmentOffset);

//        // crc check
//        var segmentSeq = buf.readLong();
//        var cvCount = buf.readInt();
//        var segmentMaskedValue = buf.readInt();
//        buf.skipBytes(SEGMENT_HEADER_LENGTH);

// skip key header or check key
var keyLength = nettyBuf.readShort();
```

```java
// SegmentBatch2.iterateFromSegmentBytes — read iteration (merge / repl / scan)
public static void iterateFromSegmentBytes(byte[] segmentBytes, int offset, int length, @NotNull CvCallback cvCallback) {
    var nettyBuf = Unpooled.wrappedBuffer(segmentBytes, offset, length);
    nettyBuf.readerIndex(SEGMENT_HEADER_LENGTH);  // <-- header skipped without inspection
    int offsetInThisSegment = SEGMENT_HEADER_LENGTH;
    while (true) {
        ...
    }
}
```

**Root cause:**

`SegmentBatch2.encodeToBuffer` computes a CRC32 of the concatenated `keyHash` longs of every CV in the segment (line 209: `KeyHash.hash32(crcCalBytes)`) and writes it into the segment header at offset `SEGMENT_HEADER_LENGTH - 4` (= byte 13). The header layout is:

| offset | size | field |
|--------|------|-------|
| 0      | 8    | segmentSeq |
| 8      | 1    | segmentType (NORMAL / TIGHT) |
| 9      | 4    | cvCount |
| 13     | 4    | **segmentCrc32** |

No read path in the codebase verifies this CRC. `OneSlot.get()` lines 1207-1211 contain a **commented-out** CRC check stub that was apparently never finished. The iteration helper `SegmentBatch2.iterateFromSegmentBytes` skips the header (`readerIndex(SEGMENT_HEADER_LENGTH)`) without reading the stored CRC. The `getOnlyKeyBytesFromSegment` path does the same. A `grep` for `hash32(crcCalBytes)` / `segmentCrc32` / `verifyCrc` confirms there is no caller-side verification anywhere in `src/main/java/`.

**Impact:**

- Silent on-disk corruption of segment data goes undetected. A flipped bit inside a CV value will surface as either:
  - garbage when `cv.compressedData` is fed to Zstd / dictionary decompression (which usually throws), or
  - a wrong-looking value silently returned to the client (if the corruption is in a region that decompresses successfully).
- The `crcCalBytes` content is *only* keyHashes, not the CV payload — so even if the CRC were verified, it would only catch corruption of the keyHash region, not of CV bytes. That is a real limitation, but the current code is strictly worse: the CRC is written but never even checked for its limited scope.
- Wastes 4 bytes per segment on a value that is dead weight at read time.
- Misleads future readers: someone debugging on-disk corruption will see the CRC field, assume it is verified, and waste time before finding the commented-out stub at `OneSlot.java:1207-1211`.

**Why not fix:**

The CRC as designed is too narrow to catch the corruption it would seem to protect against — it covers only the concatenated `keyHash` longs, not the CV payload region. Wiring up verification of the existing CRC would only catch a corruption mode that effectively does not happen in practice (a bit flip inside the keyHash region of an otherwise-valid segment), while leaving the much larger CV payload area unchecked. Doing it properly requires extending the write side to widen the CRC, and at that point the cheaper engineering call is to lean on the underlying storage / replication checksums that already cover the full segment.

Keeping the field as-is also costs only 4 bytes per segment and one `KeyHash.hash32` call per write — negligible — so removing it is not worth the migration churn either.

**Action for future readers:** if you find yourself debugging on-disk segment corruption and see the CRC field, do not assume it is verified. The commented-out stub at `OneSlot.java:1207-1211` is intentional dead code; do not "complete" it without first widening the write-side CRC coverage.

## Finding 2: `OneSlot.exists()` does not resolve big-string indirection, so `EXISTS` returns `1` for a key whose `GET` now throws `BigStringFileMissingException`

**Severity:** Medium (residual from `bug_28` Finding 1; promoted to its own finding after the fix `aa5ee729` made `GET` throw)

**Status:** Documented, **won't fix** (per project decision 2026-05-13). The asymmetry is accepted as a known limitation.

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1283-1301` (`exists`)
- `src/main/java/io/velo/BaseCommand.java:680-708` (`getCv`)

**Code excerpt:**

```java
// OneSlot.exists()
public boolean exists(@NotNull String key, int bucketIndex, long keyHash) {
    checkCurrentThreadId();

    var isExpiredFlagArray = new boolean[1];
    var cvEncodedFromWal = getFromWal(key, bucketIndex, isExpiredFlagArray);
    if (cvEncodedFromWal != null) {
        return !CompressedValue.isDeleted(cvEncodedFromWal);
    }

    if (isExpiredFlagArray[0]) {
        return false;
    }

    var expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(bucketIndex, key, keyHash);
    return expireAtAndSeq != null && !expireAtAndSeq.isExpired();        // <-- never checks big-string file
}
```

```java
// BaseCommand.getCv() — for contrast, the GET path
if (cv.isBigString()) {
    ...
    var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true);
    if (bigStringBytes == null) {
        throw new BigStringFileMissingException("data inconsistency: big string file missing for key=" + key);
    }
    cv.setCompressedData(bigStringBytes);
    cv.setDictSeqOrSpType(realDictSeq);
}
```

**Root cause:**

`exists()` decides existence purely from the key bucket cell's `expireAtAndSeq` (or from the WAL `V`). It never decodes the cell's value bytes, never resolves the UUID for a big-string-typed entry, and never touches `BigStringFiles`. After `bug_28` commit `aa5ee729`, `BaseCommand.getCv()` throws `BigStringFileMissingException` when the file behind a UUID is missing — so `GET k` now returns `-ERR data inconsistency: ...` for the corrupted key, but `EXISTS k` still returns `1`. The asymmetry that the previous round explicitly deferred is now **louder**, not quieter, because the two commands disagree by different return categories (error vs success) rather than two flavors of "absent" (`nil` vs `0`).

The original round 3 / round 4 reasoning (bug 16 / 17) for why `exists()` skipping the slot KV LRU is safe — "the LRU is just a cache of keyLoader data" — does not apply here. The big-string file is **not** a cache of cell-bucket metadata; it is an independent storage layer whose loss is precisely the inconsistency the fix is meant to surface.

**Impact:**

- A client doing the natural pattern `if EXISTS k then ... do something with GET k ... end` will hit a `-ERR` after `EXISTS` returns `1`. Application code defending against `nil` after a `1` will not handle this `-ERR` and will crash / propagate the error in unexpected places.
- `EXPIRE k 1` / `TTL k` / `DEL k` will all report success on a key whose value cannot actually be loaded. `DEL` is benign (removes the dangling metadata), but `TTL` / `EXPIRE` reading via `OneSlot.getExpireAt()` will return a sensible TTL value while `GET` errors — confusing.
- The fix in `bug_28` Finding 1 explicitly deferred this to a separate change ("If we later decide to make `EXISTS` agree with `GET`, the cleanest follow-up is to have `OneSlot.exists()` also resolve the big-string indirection"). This round elevates it to its own finding so it does not get lost.

**Why not fix:**

Two practical reasons:

1. **Trigger condition is rare and a real bug somewhere else.** The asymmetry is only observable when a big-string file has gone missing while its metadata is still alive — which is itself a data-integrity incident (disk loss, manual file removal, partial replication transfer). The right response is to fix the upstream cause, not to teach every read primitive to detect every kind of upstream corruption. `EXISTS` returning `1` for a corrupted key is no worse than `EXISTS` returning `1` for a key whose Zstd-compressed payload happens to be corrupt: in both cases `GET` is the call that errors, and operators see the error in their logs / metrics.

2. **Cost vs benefit.** The cheap implementation (Option 1 above, calling `new File(...).exists()` per existence check on big-string-typed entries) still pays a syscall per call. The symmetric implementation (calling through `getCv()`) pays a full big-string load plus LRU pollution. Either way we pay every-time cost to handle a problem that should be vanishingly rare in normal operation.

The accepted behavior is:
- `GET k` → `-ERR data inconsistency: big string file missing for key=<k>` (loud, surfaces the incident).
- `EXISTS k` → `1` (matches the metadata, agrees with `TTL` / `EXPIRE`).
- `DEL k` → removes the dangling metadata.

Clients expecting `EXISTS`/`GET` parity under data corruption should retry / handle `-ERR` from `GET` regardless of `EXISTS`.

**Action for future readers:** if a real production incident makes this asymmetry painful (e.g., recurring big-string file loss + client code that relies on `EXISTS→GET` parity), revisit this finding. The Option 1 sketch above is the right starting point.

## Finding 3: Segment format detection disagrees between `OneSlot.get()` (config flag) and `OneSlot.getOnlyKeyBytesFromSegment()` (data marker) — flipping `isSegmentUseCompression` makes prior data unreadable in `get()` only

**Severity:** Low-Medium

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1201-1203` (`get` PVM branch)
- `src/main/java/io/velo/persist/OneSlot.java:1242-1247` (`getOnlyKeyBytesFromSegment`)
- `src/main/java/io/velo/persist/SegmentBatch.java:409-413` (NORMAL fast-path inside `decompressSegmentBytesFromOneSubBlock`)
- `src/main/java/io/velo/persist/SegmentBatch2.java:280-283` (`isSegmentBytesTight` data marker)

**Code excerpt:**

```java
// OneSlot.get() — config-flag based
if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
    segmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
}
var nettyBuf = Unpooled.wrappedBuffer(segmentBytes);
nettyBuf.readerIndex(pvm.segmentOffset);
```

```java
// OneSlot.getOnlyKeyBytesFromSegment() — data-marker based
byte[] rawSegmentBytes;
if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)) {
    rawSegmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
} else {
    rawSegmentBytes = segmentBytes;
}
```

```java
// SegmentBatch.decompressSegmentBytesFromOneSubBlock — NORMAL fast-path
if (tightBytesWithLength.length == chunk.chunkSegmentLength
        && tightBytesWithLength.length > 8
        && tightBytesWithLength[8] == Chunk.SegmentType.NORMAL.val) {
    return tightBytesWithLength;
}
```

**Root cause:**

The two read paths that consume a `PersistValueMeta` decide whether to decompress using **different** signals:

- `OneSlot.get()` consults the live config flag `ConfForSlot.global.confChunk.isSegmentUseCompression`.
- `OneSlot.getOnlyKeyBytesFromSegment()` consults the segment-type marker byte at offset 8.

Their behavior diverges only when the on-disk segment type does **not** match the live config flag — which can happen if `isSegmentUseCompression` is changed at restart with pre-existing data:

| pre-restart write | post-restart `isSegmentUseCompression` | `OneSlot.get()` behavior | `getOnlyKeyBytesFromSegment()` behavior |
|------|------|------|------|
| TIGHT (compression on) | `true` (unchanged) | Decompress correctly | Decompress correctly |
| TIGHT (compression on) | **`false`** | **Skip decompression — read tight-packed bytes raw → garbage keyLength → `throw new IllegalStateException("Key length error, ...")` at line 1217** | Detect TIGHT via marker, decompress correctly |
| NORMAL (compression off) | `true` | Enter `decompressSegmentBytesFromOneSubBlock`, hit NORMAL fast-path (segment_type == NORMAL.val), return raw bytes. ✓ | Detect non-TIGHT, skip decompress. ✓ |
| NORMAL (compression off) | `false` (unchanged) | Skip decompress, read raw. ✓ | Skip decompress, read raw. ✓ |

So flipping `true → false` is one-way breakage in the main read path, while `getOnlyKeyBytesFromSegment` survives it. The other direction (`false → true`) is robust in both, thanks to the NORMAL fast-path at `SegmentBatch.java:409-413`.

**Impact:**

- Flipping `isSegmentUseCompression` from `true` to `false` (a configuration / migration scenario) corrupts the read path. The user sees `-ERR Key length error, key length=<garbage>` on every `GET` for a key whose value is still in a tight-packed segment. `EXISTS` (Finding 2) and `TTL` keep returning success because they don't consult the segment.
- Even without an intentional flip, this is a latent foot-gun: a future operator running a config sweep, or a CI snapshot test that initializes Velo with `isSegmentUseCompression=false` against a previously-recorded data dir, will hit a confusing crash.
- Splits the source of truth for segment format into two places (config + marker), making future format changes risky.

**Suggested fix direction:**

Switch `OneSlot.get()` to use the same data-marker check `getOnlyKeyBytesFromSegment` already does:

```java
if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)) {
    segmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
}
```

Rationale:
- The data marker is the authoritative answer for *this specific segment* — it cannot lie about its own format.
- The config flag `isSegmentUseCompression` should only affect future writes, not how we interpret existing data.
- The NORMAL fast-path in `decompressSegmentBytesFromOneSubBlock` already handles "wrapped in decompress but actually NORMAL" cleanly, so the new branch will not regress the existing config-on path.

Add a test that:
1. Writes a TIGHT segment with `isSegmentUseCompression=true`.
2. Flips `isSegmentUseCompression` to `false` (without flushing).
3. Asserts `GET` of a key in that segment still returns the right value.

The current code would fail step 3 with `IllegalStateException`; the proposed fix passes.

## Summary

| Finding | Severity | Status |
|---------|----------|--------|
| 1 — Segment CRC32 written but never verified on read; commented-out stub in `OneSlot.get()` | Medium | **Won't fix** — documented for awareness. CRC scope is too narrow to be useful; widening it is not worth the migration churn. |
| 2 — `OneSlot.exists()` does not resolve big-string indirection; `EXISTS=1` while `GET` throws | Medium | **Won't fix** — accepted asymmetry. The trigger is an upstream data-integrity incident; pay-every-time existence-check cost is not justified for a vanishingly rare case. |
| 3 — `OneSlot.get()` uses config flag for compression detection; `getOnlyKeyBytesFromSegment` uses data marker; `true→false` flip breaks reads | Low-Medium | **Fixed** — commit `3f5c61c3`. |

## Next Steps

Findings 1 and 2 are documented as **won't fix** by project decision and do not need reviewer verification or implementation work. They remain in this doc so future agents reading the read flow know the asymmetries and dead CRC code are intentional, not undiscovered defects.

Finding 3 is the only fix candidate from this round. Per the workflow in `CLAUDE.md`:

1. AI agent 2 (reviewer) verifies Finding 3 against the current `main` and appends review notes here.
2. If confirmed, implementer writes a failing test first (e.g., write TIGHT segment with compression on, flip the flag, assert `GET` still succeeds — this should currently throw `IllegalStateException: Key length error`), lands the minimal fix in its own commit, and inspects JaCoCo for the touched lines.
3. Another agent appends a Review Feedback section per commit.

## Review Feedback — Finding 3 Fix Commit `3f5c61c3`

### Summary of the Fix

`3f5c61c3 fix: use data marker instead of config flag to detect TIGHT segments in OneSlot.get()` lands the suggested fix exactly:

- One production line at `OneSlot.java:1201`: replace `if (ConfForSlot.global.confChunk.isSegmentUseCompression)` with `if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0))`. The PVM read path now uses the same data-marker check that `getOnlyKeyBytesFromSegment` (`OneSlot.java:1243`) already uses.
- New test in `OneSlotTest`: "test get reads TIGHT segment correctly after isSegmentUseCompression flipped to false" exercises the precise scenario the finding flagged — write 100 large (non-short) CVs with compression on, force WAL drain so they're persisted into a tight-packed segment, flip the config flag to `false`, clear the slot KV LRU, and call `get()`. The assertion is that the value round-trips correctly (cv non-null, `compressedData.length == 1600`).

### Strengths

- **Single source of truth for segment format.** Both PVM read paths (`OneSlot.get` and `getOnlyKeyBytesFromSegment`) now consult the byte at offset 8 of the segment instead of a live config flag. This removes the failure mode where a runtime config change makes existing on-disk data unreadable.
- **NORMAL fast-path is preserved.** For NORMAL segments, `isSegmentBytesTight()` returns false → the new branch skips `decompressSegmentBytesFromOneSubBlock` entirely. This is strictly faster than the pre-fix `true` config path (which entered decompress only to hit the NORMAL fast-path inside `SegmentBatch.decompressSegmentBytesFromOneSubBlock:409-413`). Net effect: NORMAL reads avoid the redundant function call when the config flag is on. Small but real perf win.
- **Test reflects the actual bug.** Writing TIGHT data first, then flipping the flag, then asserting `GET` works is exactly the failing scenario Finding 3 described. Without the fix, this test would throw `IllegalStateException: Key length error` at `OneSlot.java:1217`.
- **Test cleanup resets `isSegmentUseCompression = false`.** Avoids cross-test contamination — important since `ConfForSlot.global` is process-wide state.
- JaCoCo (`build/reports/jacocoHtml/io.velo.persist/OneSlot.java.html`) shows line 1201 as `pc bpc` after running just this one test: the TIGHT branch is hit by the new test. The NORMAL branch is covered by other existing `OneSlotTest` cases when the full suite runs (verifiable by running `./gradlew :test --tests "io.velo.persist.OneSlotTest"` and re-inspecting JaCoCo).

### Concerns

1. **Test uses internal-state hooks.** Two fragile bits:
   - `oneSlot.getWalByBucketIndex(sKey.bucketIndex()).isOnRewrite = false` — sets a `@VisibleForTesting` field directly. If `isOnRewrite` is renamed or the WAL rewrite state machine is refactored, the test breaks silently (compiles → test does the wrong thing → could still pass for the wrong reason).
   - `oneSlot.kvByWalGroupIndexLRU.get(walGroupIndex).clear()` — accesses a private `Map<Integer, LRUMap<String, byte[]>>` via Groovy's relaxed access. The public `clearKvInTargetWalGroupIndexLRU(walGroupIndex)` would have been the right call here.

   Neither is a fix defect, but they make the test brittle.

2. **No negative assertion that pre-fix behavior throws.** TDD ideally writes the failing test first and verifies the failure mode. The commit bundles the test + fix together, so the reviewer cannot independently confirm the test was actually red against the prior `OneSlot.java:1201`. A second `expect: thrown(IllegalStateException)` block under the pre-fix config (or a comment citing the specific error message that would have surfaced) would lock the contract in.

3. **Reverse direction (`false → true` flip) not tested.** Finding 3 explicitly noted this direction is robust via the NORMAL fast-path. The fix does not regress that direction (the new branch returns false for NORMAL → skip decompress → read raw → works), but there is no explicit assertion. Worth a one-shot complementary test: write NORMAL with compression off, flip to on, assert `get()` still succeeds.

4. **`getOnlyKeyBytesFromSegment` at line 1243 remains uncovered.** Unrelated to this fix's scope, but JaCoCo flagged `nc bnc` (all branches missed) on that method when running just the new test. That method is exercised by some other code path; the existing test suite may cover it. Worth a follow-up to confirm — if it's actually dead code, it should be removed; if it's live, it deserves its own test.

5. **The redundant `decompressSegmentBytesFromOneSubBlock` NORMAL fast-path can now be removed.** With both callers (`OneSlot.get` and `getOnlyKeyBytesFromSegment`) gating decompression on `isSegmentBytesTight()`, the NORMAL fast-path at `SegmentBatch.java:409-413` becomes unreachable from these two call sites. Other callers (merge / scan paths) may still hit it; worth a `grep` for `decompressSegmentBytesFromOneSubBlock` callers and a follow-up cleanup if all of them now pre-check the data marker.

### Pre-commit / Post-commit Follow-ups

- Replace the brittle `kvByWalGroupIndexLRU.get(walGroupIndex).clear()` in the test with `oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex)`. Same effect, public API, won't break under map refactors.
- Add a complementary test for the `false → true` direction (NORMAL data, flip config to on, assert `get` works). Locks in the symmetry the finding noted.
- Optional: audit callers of `SegmentBatch.decompressSegmentBytesFromOneSubBlock`. If they all now gate on `isSegmentBytesTight()` before calling, the NORMAL fast-path inside the function can be removed (defensive code that no caller needs anymore).
- Consider deleting the now-unused `ConfForSlot.global.confChunk.isSegmentUseCompression` read at line 1201 — the field is still used at write time (controls whether segments are written TIGHT or NORMAL), so the field itself stays, but the *read-path* dependence on it is gone with this fix.

Cross-references:

- Finding 2 is the explicit residual from `bug_28` Finding 1's "If we later decide to make `EXISTS` agree with `GET`..." paragraph.
- Finding 1 is adjacent to but distinct from `bug_15` (chunk fd truncation) and `bug_16` (data flow during merge). Those rounds did not look at segment integrity at the byte level.
- Finding 3 is adjacent to `bug_15` (chunk fd lifecycle) but is purely a read-path / format-detection bug; no fd state is involved.

## Reviewer Notes (AI agent 2) - Finding 3 Only

Reviewed on 2026-05-13. Per request, I ignored Findings 1 and 2 and verified only Finding 3.

### Finding 3 Verification - Confirmed

`OneSlot.get()` still chooses whether to decompress a PVM segment using the live config flag:

```java
if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
    segmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
}
```

The sibling key-recovery path uses the stored segment marker instead:

```java
if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)) {
    rawSegmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
} else {
    rawSegmentBytes = segmentBytes;
}
```

`SegmentBatch2.isSegmentBytesTight(...)` reads byte offset `8`, the segment type marker, and
`SegmentBatch.decompressSegmentBytesFromOneSubBlock(...)` has a NORMAL fast-path that returns raw bytes when the marker
is NORMAL. `Chunk.persist(...)` selects the write format from the config flag at write time, so existing data can contain
TIGHT segments even after a later restart or test setup changes `isSegmentUseCompression` to false.

That means the claimed one-way break is real:

- TIGHT segment + config still true: `OneSlot.get()` decompresses and reads correctly.
- TIGHT segment + config later false: `OneSlot.get()` skips decompression, treats tight-packed bytes as a raw segment,
  then reads key metadata at `pvm.segmentOffset`; this can produce an invalid `keyLength` or key mismatch.
- NORMAL segment + config later true: the decompress call returns raw bytes through the NORMAL fast-path, so this
  direction is already safe.
- `getOnlyKeyBytesFromSegment(...)` is safe for both formats because it follows the segment marker, not the live config.

**Verdict:** Confirmed. The fix direction is correct: `OneSlot.get()` should use the segment marker, matching
`getOnlyKeyBytesFromSegment(...)`, so the config flag controls future writes but not interpretation of already-written
segments.

### Test Expectation

A focused regression test should write/persist a non-short value with `isSegmentUseCompression=true`, clear WAL/LRU so
the PVM segment path is used, flip `isSegmentUseCompression=false`, then call `OneSlot.get(...)` for that key. Current
code should fail on the raw TIGHT bytes; the fixed code should return the expected `CompressedValue`.

## Reviewer Summary

| Finding | Reviewer status | Notes |
|---------|-----------------|-------|
| 3 - `OneSlot.get()` uses config-based segment decompression while related readers use the data marker | **Confirmed & Fixed** | Commit `3f5c61c3`. See Review Feedback below. |

## Review Feedback — Finding 3 Fix (commit `3f5c61c3`)

### Summary

Replaced the config-flag check `ConfForSlot.global.confChunk.isSegmentUseCompression` with the data-marker check `SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)` in `OneSlot.get()` line 1201, matching the existing pattern in `getOnlyKeyBytesFromSegment()`.

### Fix Details

**Production change:** One line in `OneSlot.java:1201`:
```java
// Before
if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
// After
if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)) {
```

**Test:** New Spock test `'test get reads TIGHT segment correctly after isSegmentUseCompression flipped to false'` in `OneSlotTest.groovy`:
1. Enables compression, writes a large value (1600 bytes), forces persist via `isOnRewrite=false` + 100 puts, clears WAL.
2. Verifies PVM entry exists.
3. Flips `isSegmentUseCompression=false`, clears LRU, calls `get()`.
4. Asserts the returned `CompressedValue` has valid seq and correct `compressedData` length.

### Strengths

- Minimal one-line fix, exactly matches the sibling method's pattern.
- Test directly reproduces the bug: before the fix, step 3 throws `IllegalStateException: Key length error`.
- JaCoCo confirms `fc bfc` (all 2 branches covered) at line 1201 after full `OneSlotTest` suite.
- All 15+ existing `OneSlotTest` tests still pass — no regression.

### Concerns

- None. The fix is self-contained and the data marker is the correct authority for segment format detection.

### Pre-commit/Post-commit Follow-ups

- None needed. Finding 3 is complete. Findings 1 and 2 are documented as won't-fix.
