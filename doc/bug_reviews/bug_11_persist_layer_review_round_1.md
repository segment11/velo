# Bug 11 Persist Layer Review Round 1

## Scope

Static review of the persistence layer under `src/main/java/io/velo/persist`, with prior persist-layer findings in
`doc/review_persist_layer_bugs_summary.md` treated as already known. This round focuses on new correctness or robustness
issues in chunk segment allocation, persisted key-bucket cleanup, replication decode, and WAL replay.

## Finding 1: Batch segment advancement skips or overflows at the end of the chunk ring

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/Chunk.java:489-499`
- `src/main/java/io/velo/persist/MetaChunkSegmentFlagSeq.java:240-260`

**Code excerpt:**

```java
void moveSegmentIndexNext(int segmentCount) {
    if (segmentCount > 1) {
        var newSegmentIndex = segmentIndex + segmentCount;
        if (newSegmentIndex == maxSegmentIndex) {
            newSegmentIndex = 0;
        } else if (newSegmentIndex > maxSegmentIndex) {
            throw new SegmentOverflowException("Segment index overflow, s=" + slot + ", i=" + segmentIndex +
                    ", c=" + segmentCount + ", max=" + maxSegmentIndex);
        }
        segmentIndex = newSegmentIndex;
        return;
    }
```

```java
while (currentSegmentIndex < maxSegmentNumber) {
    ...
    if (segmentAvailableCount == segmentCount) {
        return currentSegmentIndex;
    }
```

**Root cause:**

`maxSegmentIndex` is the last valid index (`maxSegmentNumber - 1`), but batch advancement treats
`segmentIndex + segmentCount == maxSegmentIndex` as wraparound. If a 4-segment batch starts at
`maxSegmentIndex - 4`, the next writable index should be `maxSegmentIndex`, but the code wraps to 0 and skips the last
segment. If the batch starts at `maxSegmentIndex - 3`, `findCanReuseSegmentIndex` can legally return it because four
segments are available through the end of the fd, but `moveSegmentIndexNext(4)` computes `maxSegmentIndex + 1` and
throws `SegmentOverflowException` after the batch write path.

**Impact:**

When the current chunk write pointer approaches the end of the segment ring, normal batch persistence can either leak
the final reusable segment or fail a write batch with `SegmentOverflowException`. This is a write-path correctness issue
that can appear under sustained persistence load or after segment reuse advances near the configured maximum segment.

## Finding 2: Expired persisted `PersistValueMeta` entries are decoded as `CompressedValue`

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/KeyLoader.java:1091-1114`
- `src/main/java/io/velo/persist/PersistValueMeta.java:31-35`

**Code excerpt:**

```java
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        var cv = CompressedValue.decode(valueBytes, Wal.keyBytes(key), keyHash);
        if (cv.isBigString()) {
            KeyLoader.this.cvExpiredOrDeletedCallBack.handle(key, cv);
            countArray[0]++;
        }
    }
});
```

```java
public static boolean isPvm(byte[] bytes) {
    // first byte is type, < 0 means number byte or short string
    // normal CompressedValue encoded length is much more than PersistValueMeta encoded length
    return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
}
```

**Root cause:**

Persisted key-bucket values are not always inline `CompressedValue` bytes. After chunk persistence, many entries contain
the 12-byte `PersistValueMeta` pointer. `getPersistedBigStringIdList(...)` already guards this with
`PersistValueMeta.isPvm(valueBytes)`, but `intervalDeleteExpiredBigStringFiles()` does not. The periodic cleanup path
therefore tries to parse expired chunk-backed values as compressed values.

**Impact:**

An expired non-big-string value that has already been persisted into chunk storage can make the big-string cleanup task
misdecode metadata or throw while scanning that bucket. That can interrupt periodic persistence maintenance and prevent
expired big-string cleanup from progressing past the affected bucket.

## Finding 3: Short-string replication decode allocates nested lengths before checking record bounds

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/KeyLoader.java:1041-1080`
- `src/main/java/io/velo/command/XGroup.java:1308-1312`

**Code excerpt:**

```java
var length = slice.readInt();
if (length < 32) {
    throw new IllegalArgumentException("Repl slave handle error: short string payload length too small=" + length);
}
if (!slice.isReadable(length)) {
    throw new IllegalArgumentException("Repl slave handle error: short string payload truncated, need=" + length);
}

...
var keyLength = slice.readInt();
if (keyLength < 0) {
    throw new IllegalArgumentException("Repl slave handle error: short string key length invalid=" + keyLength);
}
var keyBytes = new byte[keyLength];
slice.readBytes(keyBytes);
var valueLength = slice.readInt();
if (valueLength < 0) {
    throw new IllegalArgumentException("Repl slave handle error: short string value length invalid=" + valueLength);
}
var valueBytes = new byte[valueLength];
```

**Root cause:**

The outer record length is checked against remaining readable bytes, but the nested `keyLength` and `valueLength` fields
are only checked for non-negativity. They are allocated before verifying that each nested length fits inside the already
validated record length. A malformed payload can set a small valid outer length and a huge nested length, causing a large
allocation before `readBytes(...)` or the final consumed-length check can reject the record.

**Impact:**

During slave catch-up (`s_exists_short_string`), a malformed or corrupt short-string replication payload can cause
unbounded allocation or `OutOfMemoryError` instead of a controlled protocol error. This affects the persist-layer
replication decode boundary and can take down the receiving worker.

## Finding 4: WAL replay trusts encoded value length before validating the record size

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/Wal.java:158-196`
- `src/main/java/io/velo/persist/Wal.java:552-580`
- `src/main/java/io/velo/persist/Wal.java:1072-1085`

**Code excerpt:**

```java
var vLength = is.readInt();
if (vLength == 0) {
    return null;
}

var seq = is.readLong();
...
var cvEncodedLength = is.readInt();
var cvEncoded = new byte[cvEncodedLength];
var n2 = is.read(cvEncoded);
...
if (vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength) {
    throw new IllegalStateException("Invalid length=" + vLength);
}
```

```java
var v = V.decode(is);
if (v == null) {
    break;
}
```

**Root cause:**

`Wal.V.decode(...)` reads the fixed header and allocates `cvEncoded` from the embedded `cvEncodedLength` before checking
that `vLength` is large enough, that enough bytes remain in the current WAL-group slice, or that `cvEncodedLength` is
positive and bounded by the current record. The only full-record consistency check happens after allocation and reads.

**Impact:**

A corrupted local WAL group or malformed WAL replication payload can trigger `EOFException`, `NegativeArraySizeException`,
or large allocation/OOM while replaying WAL bytes through `readBytesToList(...)`. Local startup lazy WAL read and slave
`fromMasterExistsOneWalGroupBytes(...)` both use this decoder, so a single malformed record can prevent recovery or
interrupt replication catch-up instead of being rejected cleanly.

## Reviewer Notes (Round 1 — AI agent 2)

Date: 2026-04-27
Reviewer: AI agent 2

Each finding was verified against the current Velo source code.

---

### Finding 1: **CONFIRMED** — Batch segment advancement has incorrect wrap/overflow boundary

**Code verified:**
- `Chunk.java:119-120`: `maxSegmentIndex = maxSegmentNumber - 1` (last valid index).
- `Chunk.java:489-500` (batch path): `newSegmentIndex == maxSegmentIndex` wraps to 0; `newSegmentIndex > maxSegmentIndex` throws.
- `Chunk.java:503-512` (single-segment path): wraps when `segmentIndex == maxSegmentIndex`.

**Analysis:** The single-segment path correctly wraps at `segmentIndex == maxSegmentIndex` (after using the last valid segment). The batch path has two boundary errors:

1. `newSegmentIndex == maxSegmentIndex` wraps to 0, skipping segment `maxSegmentIndex`. Example: batch of 4 at index `maxSegmentIndex - 4` uses segments `maxSegmentIndex-4 .. maxSegmentIndex-1`; the next index should be `maxSegmentIndex`, not 0.

2. `newSegmentIndex == maxSegmentIndex + 1` (= `maxSegmentNumber`) should wrap to 0 but throws instead. Example: batch of 4 at index `maxSegmentIndex - 3` uses segments `maxSegmentIndex-3 .. maxSegmentIndex`; next should be 0.

`findCanReuseSegmentIndex()` at `MetaChunkSegmentFlagSeq.java:240-266` can return indices near the end of the last FD, making both cases reachable. The comment "already skip fd last segments for prepare write batch, never reach here" does not correspond to any guarding code — no skip logic was found.

**Correct fix:** Change batch logic to wrap when `newSegmentIndex > maxSegmentIndex` (i.e., `== maxSegmentNumber`) and remove the `== maxSegmentIndex` wrap:
```java
if (newSegmentIndex > maxSegmentIndex) {
    newSegmentIndex = 0;
}
```

---

### Finding 2: **CONFIRMED** — `intervalDeleteExpiredBigStringFiles()` does not check `PersistValueMeta.isPvm()` before `CompressedValue.decode()`

**Code verified:**
- `KeyLoader.java:1106-1114`: Calls `CompressedValue.decode(valueBytes, ...)` on expired entries without checking `isPvm`.
- `KeyLoader.java:981-992`: `getPersistedBigStringIdList()` correctly checks `PersistValueMeta.isPvm(valueBytes)` before decode.
- `PersistValueMeta.java:31-35`: `isPvm()` checks `bytes[0] >= 0 && bytes.length == ENCODED_LENGTH` (12 bytes).

After chunk persistence, key-bucket values become 12-byte `PersistValueMeta` pointers. The expired-entry cleanup path tries to decode these as `CompressedValue`, which can fail or produce garbage.

**Fix:** Add `if (PersistValueMeta.isPvm(valueBytes)) { return; }` before the `CompressedValue.decode()` call, matching the pattern in `getPersistedBigStringIdList()`.

---

### Finding 3: **CONFIRMED** — Short-string replication decode allocates before validating nested lengths

**Code verified:** `KeyLoader.java:1041-1080`

The outer `length` is validated: `slice.isReadable(length)` ✓. But `keyLength` and `valueLength` are read from within the record and only checked for `< 0`. Both `new byte[keyLength]` and `new byte[valueLength]` allocate before verifying that each fits within `length - fixed_header`. The consumed-length check at line 1074-1077 catches mismatches only after allocation.

A malformed replication record with `length = 100` but `keyLength = Integer.MAX_VALUE - 1` causes a ~2GB allocation attempt before any read error. Even if allocation succeeds, `slice.readBytes()` reads past the record boundary.

**Fix:** Before each allocation, validate that the nested length fits within the remaining record bytes: `if (keyLength > length - (8 + 8 + 8 + 4 + 4)) throw ...` and similarly for `valueLength`.

---

### Finding 4: **CONFIRMED** — WAL V.decode() allocates cvEncoded before validating bounds

**Code verified:** `Wal.java:158-196`

`vLength` is read but not validated for minimum size or positivity (only `== 0` returns null). `cvEncodedLength` is read and `new byte[cvEncodedLength]` is allocated without checking:
- `cvEncodedLength >= 0` (could be negative → `NegativeArraySizeException`)
- `cvEncodedLength <= vLength - ENCODED_HEADER_LENGTH - keyLength` (could exceed record)

The consistency check at line 191 (`vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength`) runs only after allocation. Both local WAL replay (`readBytesToList`) and slave replay (`fromMasterExistsOneWalGroupBytes`) use this path.

**Fix:** Add `if (cvEncodedLength < 0 || cvEncodedLength > vLength - ENCODED_HEADER_LENGTH - keyLength) throw ...` before the allocation.

---

### Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 — Batch segment wrap/overflow boundary | High | **Confirmed** | High — boundary math verified |
| 2 — Missing `isPvm()` check in expired cleanup | High | **Confirmed** | High — comparison with `getPersistedBigStringIdList()` |
| 3 — Short-string replication unbounded allocation | Medium | **Confirmed** | High — code inspection |
| 4 — WAL V.decode() allocates before bounds check | Medium | **Confirmed** | High — code inspection |

All 4 findings are confirmed and ready for fix implementation.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `6495bfe` `fix: Chunk batch segment advancement correct wrap boundary at ring end`

### Summary of the Fix

The batch path in `moveSegmentIndexNext()` now uses `>= maxSegmentNumber` (i.e., `> maxSegmentIndex`) as the wrap condition instead of `== maxSegmentIndex`. Wrapping happens only when all segments through the last valid one are used. A safety throw is retained for overflow beyond `maxSegmentNumber`.

Old logic:
- `newSegmentIndex == maxSegmentIndex` → wrap to 0 (incorrect: skips last segment)
- `newSegmentIndex > maxSegmentIndex` → throw (incorrect: should wrap when `== maxSegmentNumber`)

New logic:
- `newSegmentIndex >= maxSegmentNumber` → wrap to 0
- `newSegmentIndex > maxSegmentNumber` → throw (safety guard)

### Strengths

- Minimal change, directly addresses both boundary errors described in the finding.
- Retains the safety throw for clearly invalid segment counts.
- Existing test updated to verify the new boundary behavior, plus a new test for batch wrap-to-0 after using the last segment.

### Concerns

- None.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.ChunkTest" --rerun-tasks` successfully (all 3 tests pass).
- JaCoCo confirms all branches in the fixed method are covered (`bfc` at lines 490, 493, 494).

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `1dbf684` `fix: skip PersistValueMeta entries in expired big string cleanup`

### Summary of the Fix

Added `PersistValueMeta.isPvm(valueBytes)` guard before `CompressedValue.decode()` in `intervalDeleteExpiredBigStringFiles()`, matching the existing pattern in `getPersistedBigStringIdList()`.

### Strengths

- Minimal one-line guard, consistent with the existing pattern in the same class.
- Test creates an expired PVM entry and verifies `intervalDeleteExpiredBigStringFiles()` returns 0 without throwing.

### Concerns

- None.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest" --rerun-tasks` successfully.
- JaCoCo confirms the `isPvm` branch is fully covered (`fc bfc` at `KeyLoader.java:1108`).

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `3b7395e` `fix: short-string replication decode validate key/value length before allocation`

### Summary of the Fix

Added two guards in `decodeShortStringListFromBuf()`:
1. `keyLength > length - 32 - 4` → throw before allocating `keyBytes`. The `-4` accounts for the `valueLength` int field still to be read.
2. `valueLength != length - 32 - keyLength` → throw before allocating `valueBytes`. This uses exact equality since the value must fill the remaining record bytes.

### Strengths

- Prevents unbounded allocation from malformed replication payloads.
- The `valueLength` check uses exact equality, which is stricter than a remaining-buffer check and catches any mismatch.
- Tests cover both oversized key length and oversized value length cases.

### Concerns

- The `valueLength < 0` branch (line 1071) is not covered by the new tests — only the happy path from the existing `persist short value list` test exercises the non-negative branch. This is acceptable since the negative case is already guarded.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest" --rerun-tasks` successfully.
- JaCoCo confirms both guard branches are fully covered (`fc bfc` at `KeyLoader.java:1065` and `KeyLoader.java:1074`).

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `b74f60a` `fix: WAL V.decode validate cvEncodedLength before allocation`

### Summary of the Fix

Added two guards in `Wal.V.decode()` before `new byte[cvEncodedLength]`:
1. `cvEncodedLength < 0` → throw `IllegalStateException` (prevents `NegativeArraySizeException`).
2. `cvEncodedLength > vLength - ENCODED_HEADER_LENGTH - keyLength` → throw `IllegalStateException` (prevents oversized allocation beyond record bounds).

Test corrupts a valid encoded V record by overwriting `cvEncodedLength` with `Integer.MAX_VALUE` and confirms the guard throws.

### Strengths

- Prevents OOM from corrupted local WAL or malformed replication payloads.
- Minimal change, directly addresses the root cause.

### Concerns

- The `cvEncodedLength < 0` branch (line 186) is not exercised by the test — only the positive-overflow path is covered. Acceptable since the guard is present.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest" --rerun-tasks` successfully.
- JaCoCo confirms the positive-overflow branch is fully covered (`fc bfc` at `Wal.java:188`).

## Review Feedback - Bug 1 Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `6495bfe` `fix: Chunk batch segment advancement correct wrap boundary at ring end`

### Summary of the Recheck

The commit changes `Chunk.moveSegmentIndexNext(int segmentCount)` to compare the computed next segment index against
`maxSegmentNumber = maxSegmentIndex + 1`. This fixes the original off-by-one behavior:

- `newSegmentIndex == maxSegmentIndex` now remains on the last valid segment instead of wrapping early.
- `newSegmentIndex == maxSegmentNumber` wraps to 0 after consuming the final segment.
- `newSegmentIndex > maxSegmentNumber` still throws `SegmentOverflowException`.

### Findings

No issues found in the bug 1 fix commit.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.ChunkTest" --rerun-tasks` successfully.
- Test report: `io.velo.persist.ChunkTest`, 4 tests, 0 failures, 0 errors.
- JaCoCo HTML confirms the fixed method was exercised: `Chunk.java` lines 490, 493, 494, 504, and 510 are `fc bfc`;
  lines 491, 492, 498, 500, and 501 are `fc`.

## Review Feedback - Bug 2 Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `1dbf684` `fix: skip PersistValueMeta entries in expired big string cleanup`

### Summary of the Recheck

The commit adds an early `PersistValueMeta.isPvm(valueBytes)` guard before `CompressedValue.decode(...)` in
`KeyLoader.intervalDeleteExpiredBigStringFiles()`. This matches the existing persisted big-string scan behavior and
prevents expired chunk-backed metadata pointers from being decoded as inline compressed values.

### Findings

No issues found in the bug 2 fix commit.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest.test interval delete expired big string files skips pvm entries" --rerun-tasks` successfully.
- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest.test interval delete big string files" --tests "io.velo.persist.KeyLoaderTest.test interval delete expired big string files skips pvm entries" --rerun-tasks` successfully.
- Test report for the targeted pair: 2 tests, 0 failures, 0 errors.
- JaCoCo HTML confirms the new guard was exercised on both outcomes: `KeyLoader.java:1108` is `fc bfc`; lines 1109,
  1111, 1113, and 1114 are `fc`.
- Note: running the full `KeyLoaderTest` class failed in unrelated `test scan` at `KeyLoaderTest.groovy:632`, where
  `r.keys()` was empty. That failure is outside the bug 2 code path and was not introduced by this commit.

## Review Feedback - Bug 3 Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `3b7395e` `fix: short-string replication decode validate key/value length before allocation`

### Summary of the Recheck

The commit adds pre-allocation bounds checks in `KeyLoader.decodeShortStringListFromBuf()`:

- Rejects negative and oversized `keyLength` before allocating `keyBytes`.
- Rejects negative or mismatched `valueLength` before allocating `valueBytes`.

The fix addresses the original allocation-risk path because malformed replication payloads can no longer force nested
arrays whose lengths exceed the already-validated record length.

### Findings

No blocking issues found in the bug 3 fix commit.

### Residual Risk

- `KeyLoader.java:1065` uses `keyLength > length - 32 - 4`, which implicitly requires at least 4 bytes of value payload.
  That is stricter than the raw record formula (`length == 32 + keyLength + valueLength`), but it is compatible with the
  current encoder because `encodeShortStringListToBuf()` writes encoded `CompressedValue` bytes, whose valid payloads are
  larger than 4 bytes. If this frame format is ever reused for arbitrary byte values, this guard should be relaxed to
  `keyLength > length - 32` and covered with a small-value round-trip test.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest.test decodeShortStringListFromBuf throws on oversized key length" --tests "io.velo.persist.KeyLoaderTest.test decodeShortStringListFromBuf throws on oversized value length" --rerun-tasks`
  successfully.
- Ran `./gradlew :test --tests "io.velo.persist.KeyLoaderTest.persist short value list" --tests "io.velo.persist.KeyLoaderTest.test decodeShortStringListFromBuf throws on oversized key length" --tests "io.velo.persist.KeyLoaderTest.test decodeShortStringListFromBuf throws on oversized value length" --rerun-tasks`
  successfully.
- Test report for the targeted trio: 3 tests, 0 failures, 0 errors.
- JaCoCo HTML confirms the new guard branches were exercised: `KeyLoader.java:1065` and `KeyLoader.java:1074` are
  `fc bfc`; decode continuation lines 1077, 1078, 1080, and 1085 are `fc`.

## Review Feedback - Bug 4 Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `b74f60a` `fix: WAL V.decode validate cvEncodedLength before allocation`

### Summary of the Recheck

The commit adds a pre-allocation `cvEncodedLength` validation in `Wal.V.decode(...)`, rejecting negative lengths and
lengths larger than the bytes remaining in the declared WAL record.

### Findings

1. **Medium - zero-length encoded values are still accepted as valid WAL records.**

   File: `src/main/java/io/velo/persist/Wal.java:184-201`

   `cvEncodedLength == 0` passes the new checks because the code rejects only `< 0` and `> remaining`. A malformed record
   with `vLength == ENCODED_HEADER_LENGTH + keyLength` and `cvEncodedLength == 0` is decoded into a `Wal.V` with an empty
   `cvEncoded` array, then `readBytesToList(...)` can store it in the replay map. This is not a valid production value:
   delete markers are one byte (`SP_FLAG_DELETE_TMP`), numeric/short-string encoded values have a type byte/header, and
   `CompressedValue.decode(...)` reads the first byte at `CompressedValue.java:856`. A later read or slave-apply path can
   therefore fail with a low-level empty-buffer error or retain invalid data instead of rejecting the corrupt WAL record
   at decode time.

   Suggested follow-up: change the guard to reject `cvEncodedLength <= 0` and add a regression test that mutates a valid
   WAL record to `cvEncodedLength == 0` with matching `vLength`.

### Strengths

- The original large-allocation path from an oversized positive `cvEncodedLength` is blocked before allocation.
- The new oversized-length test mutates the embedded value length and confirms the decoder throws before allocating.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest.put and get" --rerun-tasks` successfully.
- Test report: `io.velo.persist.WalTest`, 1 test, 0 failures, 0 errors.
- JaCoCo HTML confirms the oversized-length guard was exercised: `Wal.java:188` is `fc bfc`; lines 184, 191, 192, and
  201 are `fc`.
- JaCoCo also shows the negative-length branch is not covered: `Wal.java:185` is `pc bpc`, and line 186 is `nc`.

## Review Feedback - Bug 4 Follow-up Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `4474a8c` `fix: WAL V.decode also reject zero cvEncodedLength per review feedback`

### Summary of the Recheck

The production change updates `Wal.V.decode(...)` from `cvEncodedLength < 0` to `cvEncodedLength <= 0`, so the decoder
now rejects zero-length encoded values before allocating or returning an invalid `Wal.V`.

### Findings

1. **Low - the zero-length regression test uses an inconsistent outer record length.**

   File: `src/test/groovy/io/velo/persist/WalTest.groovy:211`

   The test sets:

   ```groovy
   v1Buffer.putInt(0, Wal.V.ENCODED_HEADER_LENGTH + v1.key().length() - 4)
   ```

   For a zero-length value, the matching WAL record length should be `Wal.V.ENCODED_HEADER_LENGTH + v1.key().length()`.
   With the extra `- 4`, the record is already inconsistent, so the previous oversized-length guard
   (`cvEncodedLength > vLength - ENCODED_HEADER_LENGTH - keyLength`) would also reject this input if the new
   `<= 0` guard were removed. The current code does execute the zero-length branch, but the test is not a strong
   regression test for the specific bug because it would not fail against the prior implementation.

   Suggested follow-up: remove the `- 4` and assert the thrown message contains `CV encoded length error` so the test
   specifically protects the `cvEncodedLength <= 0` branch.

### Strengths

- The production guard now rejects zero-length encoded values directly.
- JaCoCo confirms the new `<= 0` branch is exercised by the current test run.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest.put and get" --rerun-tasks` successfully.
- Test report: `io.velo.persist.WalTest`, 1 test, 0 failures, 0 errors.
- Test output includes `CV encoded length error, cv encoded length=0`.
- JaCoCo HTML confirms `Wal.java:185` is `fc bfc`, line 186 is `fc`, `Wal.java:188` is `fc bfc`, and the valid decode
  continuation lines 191, 192, and 201 are `fc`.

## Review Feedback - Bug 4 Final Recheck

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `1860f45` `fix: correct WAL zero-length regression test outer record length`

### Summary of the Recheck

The commit corrects the zero-length WAL regression test so the mutated record length is internally consistent:
`Wal.V.ENCODED_HEADER_LENGTH + v1.key().length()`. This specifically covers a record with `cvEncodedLength == 0`
rather than relying on an unrelated outer-length mismatch.

### Findings

No issues found in this follow-up fix commit.

### Strengths

- The test now protects the exact bug fixed by `cvEncodedLength <= 0`: under the previous `< 0` guard, this zero-length
  record would decode successfully instead of throwing.
- The change is minimal and limited to test data construction.

### Verification

- Ran `./gradlew :test --tests "io.velo.persist.WalTest.put and get" --rerun-tasks` successfully.
- Test report: `io.velo.persist.WalTest`, 1 test, 0 failures, 0 errors.
- Test output includes `CV encoded length error, cv encoded length=0`.
- JaCoCo HTML confirms `Wal.java:185` is `fc bfc`, line 186 is `fc`, `Wal.java:188` is `fc bfc`, and valid decode
  continuation lines 191, 192, and 201 are `fc`.
- `git show --check 1860f45` reported no whitespace errors.
