# Bug Review: Compression Module — Round 1

**Module**: Compression (`CompressedValue`, `Dict`, `DictMap`, `CompressStats`, `TrainSampleJob`, type-level compression)
**Review Date**: 2026-05-26
**Branch**: `review/compression`
**Status**: Initial findings (author)

---

## Bug 1 — CompressStats metrics inconsistency: `compressedTotalLength` polluted with uncompressed data

**Severity**: Medium — corrupts monitoring metrics and compression-ratio calculation

**Files**:
- `src/main/java/io/velo/BaseCommand.java` lines 1065–1081
- `src/main/java/io/velo/CompressStats.java` lines 46–48

**Code excerpt** (BaseCommand.java, `set()` method, the else branch around line 1078):

```java
// stats
compressStats.rawCount++;
compressStats.compressedTotalLength += valueBytes.length;
```

**Root cause**: In the non-compression else branch (when `dict == null` or compression is not beneficial), `valueBytes.length` — the *uncompressed* data length — is added to `compressStats.compressedTotalLength`. The `CompressStats` gauge then computes `compression_ratio = compressedTotalLength / rawTotalLength` (CompressStats.java line 68), producing a spuriously inflated ratio.

**Impact**: The Prometheus metrics for compression ratio are incorrect whenever values bypass compression (e.g., values below `TO_COMPRESS_MIN_DATA_LENGTH`, compression disabled globally, or keys matching no dictionary prefix). Operators relying on these metrics would see misleading data.

**Fix direction**: In the non-compression branch, `compressedTotalLength` should not be incremented with raw data length. The `compressedTotalLength` field should only track actually-compressed payload sizes.

---

## Bug 2 — CompressStats `rawCount` is only incremented in the non-compression branch (INTENDED BEHAVIOR — NOT A BUG)

**Severity**: ~~Medium~~ N/A — `rawCount` tracks non-compressed values only; disjoint from `compressedCount` by design

**Reassessment (2026-05-26)**: `rawCount` means "count of values handled but NOT compressed." The compression branch correctly increments `compressedCount` without `rawCount`, and the non-compression branch correctly increments `rawCount` without `compressedCount`. The two counters are disjoint by design — they count different populations. This was initially misdiagnosed as a bug. The actual issue was only Bug 3 (the `rawCount++` was gated behind the training flag). No fix was needed for the compression branch.

**Severity**: Medium — makes raw-count vs compressed-count metrics unusable for comparison

**Files**:
- `src/main/java/io/velo/BaseCommand.java` lines 1037–1081

**Code excerpt** (compression branch, lines 1037–1040):

```java
// stats
compressStats.compressedCount++;
compressStats.compressedTotalLength += cv.getCompressedLength();
compressStats.compressedCostTimeTotalUs += costT;
```

**Root cause**: The compression branch (lines 1037–1040) increments `compressedCount` but never increments `rawCount`. The non-compression branch (line 1079) increments `rawCount` but never `compressedCount`. This means the two counters are disjoint — each counts a different population, making it impossible to derive a meaningful compression-hit ratio or throughput breakdown from the metrics.

**Impact**: Monitoring dashboards that display ratios like `compressed_count / raw_count` are meaningless. The intended semantics appear to be: `rawCount` = total values processed, `compressedCount` = subset that was compressed. But currently each branch only updates one of the two counters.

**Fix direction**: Move `rawCount++` to a common location (e.g., after `rawTotalLength += valueBytes.length` at line 979) so it counts every value processed, and ensure `compressedCount++` only fires when compression was actually applied.

---

## Bug 3 — CompressStats metrics gated behind training flag in the non-compression branch

**Severity**: Low–Medium — stats silently skipped when training is disabled

**Files**:
- `src/main/java/io/velo/BaseCommand.java` lines 1065–1081

**Code excerpt**:

```java
if (ConfForGlobal.isValueSetUseCompression && ConfForGlobal.isOnDynTrainDictForCompression) {
    // add to the train sample list
    ...
    // stats
    compressStats.rawCount++;
    compressStats.compressedTotalLength += valueBytes.length;
}
```

**Root cause**: In the non-compression else branch, the `rawCount++` and `compressedTotalLength` stats updates are nested inside the `isValueSetUseCompression && isOnDynTrainDictForCompression` guard. When either flag is `false`, these stats are silently skipped, while `compressStats.rawTotalLength` (line 979) is always incremented regardless. This leaves `rawTotalLength` accumulating but `rawCount` and `compressedTotalLength` frozen, producing zero or NaN values in derived metrics.

**Impact**: When training is disabled (which is the simpler, default production config), the `CompressStats` gauge may report stale zeros for `raw_count` and `compressed_count` while `raw_total_length` grows unbounded — a clear inconsistency visible on dashboards.

**Fix direction**: Decouple the metrics updates from the training-sample guard. Stats tracking should be unconditional once `compressStats` is initialized.

---

## Bug 4 — `Dict.SELF_ZSTD_DICT` has a dummy 1-byte dictionary; direct `compressByteArray` calls would use dictionary mode with dummy data

**Severity**: Low — all current callers special-case `SELF_ZSTD_DICT`, but the public API is fragile

**Files**:
- `src/main/java/io/velo/Dict.java` lines 33, 40–42, 424–426
- `src/main/java/io/velo/CompressedValue.java` lines 643–644
- `src/main/java/io/velo/type/RedisHH.java` lines 196–197

**Code excerpt** (Dict.java constructor):

```java
public Dict() {
    // one dict byte means not set yet
    this.dictBytes = new byte[1];
    this.seq = SELF_ZSTD_DICT_SEQ;
    this.createdTime = System.currentTimeMillis();
}
```

**Root cause**: `Dict.SELF_ZSTD_DICT` is initialized with a 1-byte dummy dictionary array (`new byte[1]`). When `initCtx()` is called, it loads this 1-byte array into Zstd compression and decompression contexts via `ctxCompressArray[i].loadDict(dictBytes)`. If any future caller invokes `Dict.SELF_ZSTD_DICT.compressByteArray()` directly (instead of special-casing with `dict == Dict.SELF_ZSTD_DICT` as the current code does in `CompressedValue.compress()` line 643 and `RedisHH.compressIfBytesLengthIsLong()` line 196), it would compress using Zstd dictionary mode with a meaningless 1-byte dictionary, producing a corrupt (non-standard) frame that the non-dictionary decompression path cannot read.

**Impact**: Currently protected by call-site guards, so no production impact. However, the `compressByteArray`, `compressByteBuffer`, `decompressByteArray`, and `decompressByteBuffer` methods are all `public` on `Dict`, and the class is annotated `Serializable`. Any future code path that calls through these methods on `SELF_ZSTD_DICT` without the identity check would produce hard-to-diagnose data corruption.

**Fix direction**: Either (a) override the compress/decompress methods in `SELF_ZSTD_DICT` to delegate to plain Zstd, or (b) make `initCtx()` skip loading the dummy dict for `SELF_ZSTD_DICT`, or (c) document the invariant clearly and add a runtime assertion.

---

## Bug 5 — `CompressStats.rawTotalLength` double-counted when string value is parsed as number

**Severity**: Medium — inflates `rawTotalLength` for all numeric SET operations

**Files**:
- `src/main/java/io/velo/BaseCommand.java` lines 979, 983–991, 998–1000

**Code excerpt**:

```java
// line 979: ALWAYS increments rawTotalLength
compressStats.rawTotalLength += valueBytes.length;

// lines 983-991: number short-circuit
if (valueBytes.length <= MAX_LONG_VALUE_IN_BYTES_LENGTH && !isTypeNumber) {
    var value = new String(valueBytes);
    long longValue;
    try {
        longValue = Long.parseLong(value);
        setNumber(longValue, slotWithKeyHash, expireAt);  // calls set() AGAIN
        return;
    } catch (NumberFormatException ignore) {}
    ...
}
```

**Root cause**: When a string value like `"123"` is parsed as a number, `set()` at line 979 increments `rawTotalLength` by the original string length (3 bytes). Then `setNumber()` calls `set()` recursively with the numeric encoding (1 byte for `SP_TYPE_NUM_BYTE`). The recursive `set()` at line 979 increments `rawTotalLength` a *second* time by 1 byte. The total `rawTotalLength` becomes 4 instead of the expected 1, inflating the metric for every numeric SET.

The same double-count occurs for `short`, `int`, `long`, and `double` numeric representations.

**Impact**: `rawTotalLength` is systematically inflated for all numeric values. The derived `compression_ratio = compressedTotalLength / rawTotalLength` metric is biased low (appearing worse than reality) because the denominator is inflated. Every `SET key 123`-style operation double-counts.

**Fix direction**: Move `rawTotalLength += valueBytes.length` to *after* the number-parsing `return`, so it only increments once per stored value. Alternatively, subtract the original string length before calling `setNumber()`.

---

## Summary

| # | Severity | Title | Confirmed |
|---|----------|-------|-----------|
| 1 | Medium | `compressedTotalLength` polluted with uncompressed data | — |
| 2 | Medium | `rawCount` never incremented in compression branch | — |
| 3 | Low–Medium | Stats gated behind training flag in non-compression branch | — |
| 4 | Low | SELF_ZSTD_DICT dummy dict is a latent API hazard | — |
| 5 | Medium | `rawTotalLength` double-counted for numeric SET values | — |

---

## AI Agent 2 Review Notes — 2026-05-26

Reviewed against the current code on `review/compression`.

### Bug 1 — Confirmed, with narrowed impact

`BaseCommand.set()` still adds `valueBytes.length` to `compressStats.compressedTotalLength` in the non-compression branch
at `src/main/java/io/velo/BaseCommand.java:1078-1080`. `CompressStats` reports `compression_ratio` as
`compressedTotalLength / rawTotalLength` at `src/main/java/io/velo/CompressStats.java:68-70`, so the numerator is
polluted when this branch runs.

The impact is real, but the trigger is narrower than the initial wording: this bad increment is inside the
`isValueSetUseCompression && isOnDynTrainDictForCompression` block at `BaseCommand.java:1065`, so values bypassing
compression only pollute `compressedTotalLength` when dynamic training is enabled. If compression is globally disabled,
this exact increment does not run; that case is covered by Bug 3's skipped/stale counter problem.

**Verdict:** Confirmed metric correctness bug. `compressedTotalLength` should represent only bytes actually emitted by a
successful compression attempt.

### Bug 2 — Reassessed: NOT A BUG (2026-05-26)

`rawCount` means "count of values handled but NOT compressed" — i.e., it tracks the non-compressed population only.
The compression branch correctly increments `compressedCount` without touching `rawCount`. The initial diagnosis that
the two counters should overlap was incorrect; they are disjoint by design. No fix is needed in the compression or
big-string compression branches. The only real issue was Bug 3 (gating of `rawCount++` behind training), which has been
fixed in commit `08280a08`.

### Bug 3 — Confirmed

The only `rawCount++` in normal `set()` is still nested under the training guard at `BaseCommand.java:1065-1080`.
`rawTotalLength` increments unconditionally earlier at `BaseCommand.java:979`. Therefore, with training disabled,
normal non-compressed writes still add bytes to `rawTotalLength` but do not update `rawCount`.

There is one additional metric presentation detail: `CompressStats` only emits the compression metric family when
`compressedCount > 0` (`CompressStats.java:61-71`). That can hide the inconsistency until at least one compressed value
has been recorded, after which `rawTotalLength` and `rawCount` can describe different populations.

**Verdict:** Confirmed metric accounting bug. Training sample collection and compression stats should be decoupled.

### Bug 4 — Confirmed as a latent API hazard, not an active production corruption path

`Dict.SELF_ZSTD_DICT` is constructed with the default constructor, which sets a one-byte `dictBytes` array and sequence
`SELF_ZSTD_DICT_SEQ` at `src/main/java/io/velo/Dict.java:423-426`. `Dict.initCtx()` unconditionally loads `dictBytes`
into Zstd compression and decompression contexts at `Dict.java:160-176`, and the public `compressByteArray`,
`compressByteBuffer`, `decompressByteArray`, and `decompressByteBuffer` methods use those contexts directly at
`Dict.java:207-283`.

Current production call sites do special-case the singleton before using Zstd:
`CompressedValue.compress()` uses plain `Zstd.compressByteArray` for `dict == null || dict == Dict.SELF_ZSTD_DICT` at
`src/main/java/io/velo/CompressedValue.java:639-647`, and `RedisHH.compressIfBytesLengthIsLong()` does the equivalent at
`src/main/java/io/velo/type/RedisHH.java:190-203`. The decode paths also special-case dictionary sequence `1` for plain
Zstd at `CompressedValue.java:595-603` and `RedisHH.java:335-353`.

**Verdict:** Confirmed low-severity latent hazard. I did not find an active current path that calls
`Dict.SELF_ZSTD_DICT.compressByteArray()` directly, but the public API allows a future caller to bypass the required
identity check.

#### Follow-up Review Feedback on Bug 4 Fix Direction

Reviewed commit `96681be0` (`fix: make Dict compress/decompress methods safe when ctx arrays are null`) against the real
server lifecycle. In the normal Velo startup path, dictionary thread-local resources are expected to be prepared before
use: newly added dictionaries call `DictMap.putDict(...).initCtx()`, and loaded dictionaries call `initCtx()` during
`DictMap.initDictMap(...)`. For real trained dictionaries, `ctxCompressArray == null` / `decompressCtxArray == null`
should therefore be treated as a lifecycle error, not as a signal to fall back to plain Zstd.

The null-context fallback is not necessary for the production flow and is the wrong semantic condition. It also does not
fully address the Bug 4 hazard: if `Dict.SELF_ZSTD_DICT.initCtx()` is ever called, the public methods still use contexts
loaded with the singleton's one-byte dummy dictionary. The added test only proves direct `SELF_ZSTD_DICT.compressByteArray`
works before contexts are initialized; it does not cover the initialized-context case or a decompression round trip.

Recommended direction: do not keep a generic plain-Zstd fallback for all `Dict` instances. If Bug 4 is fixed, make the
behavior explicit for `Dict.SELF_ZSTD_DICT` only, or document/assert that direct public `Dict` compression methods must not
be called on that singleton. Missing contexts for normal trained dictionaries should fail loudly because startup/resource
preparation is broken.

### Bug 5 — Confirmed, with expected-value clarification

`BaseCommand.set()` increments `rawTotalLength` before number parsing at `BaseCommand.java:979`. For a string value such
as `"123"`, the same method then calls `setNumber(longValue, ...)` and returns at `BaseCommand.java:988-991`.
`setNumber()` encodes the number into a compact byte array and calls back into `set()` at
`BaseCommand.java:798-853`, where `rawTotalLength` is incremented a second time at `BaseCommand.java:979`.

The concrete bug is the double count. The intended single count should be clarified before fixing: it could be the
client-provided byte length (`"123"` = 3) or the compact stored numeric payload length (`SP_TYPE_NUM_BYTE` = 1). The
initial fix direction chooses the stored-payload interpretation by letting only the recursive numeric `set()` account
for the value.

**Verdict:** Confirmed metric correctness bug. The regression test should assert that numeric SET accounting increments
`rawTotalLength` exactly once.

### Reviewer Summary

| # | Reviewer verdict | Notes |
|---|------------------|-------|
| 1 | Confirmed | Pollutes `compressedTotalLength` when non-compressed values pass through the training-enabled branch. |
| 2 | Reassessed: NOT A BUG | `rawCount` tracks non-compressed values only; disjoint from `compressedCount` by design. |
| 3 | Confirmed | Stats are coupled to dynamic training, while `rawTotalLength` is not. |
| 4 | Confirmed latent hazard | Current production callers guard the singleton, but the public `Dict` methods remain unsafe for `SELF_ZSTD_DICT`. |
| 5 | Confirmed | Numeric string SET values double-count `rawTotalLength`; expected single-count semantics need to be chosen in the fix. |

---

## AI Agent 3 Review Feedback — 2026-05-26

Reviewed all 5 committed fixes against the current code on `review/compression`.

### Bug 5 fix (`a0c3330e`)

**Summary**: Moved `compressStats.rawTotalLength += valueBytes.length` from line 979 (before number-parsing
short-circuit) to line 1003 (after number-parsing `return`). Numeric values like `SET key 123` now count only the
stored byte-encoding length (e.g. 1 byte for `SP_TYPE_NUM_BYTE`) rather than the client-provided string + encoding
combined.

**Verification**: `BaseCommandTest.test compressStats rawTotalLength not double counted for numeric set` and
`test compressStats rawTotalLength not double counted for non-numeric set` both pass. JaCoCo confirms the moved line
(L1003, `fc`) and the `setNumber(longValue,...)` call site (L988, `fc`) are covered.

**Strengths**: Minimal, correct change. The `rawTotalLength` interpretation (stored form) is internally consistent
with how `compressedTotalLength` counts stored bytes.

**Concerns**: Big-string path has its own `rawTotalLength` increment at line 939 — that path was not affected and
remains correct.

### Bug 3 fix (`08280a08`)

**Summary**: Moved `compressStats.rawCount++` (and the later-removed `compressedTotalLength` increment) out of the
`isValueSetUseCompression && isOnDynTrainDictForCompression` guard in the non-compression else branch. Training
sample collection remains inside the guard; stats are now unconditional.

**Verification**: `test compressStats rawCount incremented when training is disabled` passes with
`isOnDynTrainDictForCompression = false`. JaCoCo confirms L1080 (`fc`).

**Strengths**: Clean separation of concerns — training logic and metric tracking are decoupled.

### Bug 1 fix (`4fbfb3b1`)

**Summary**: Removed `compressStats.compressedTotalLength += valueBytes.length` from the non-compression else
branch. The `compressedTotalLength` field now only tracks actual compressed payload bytes, not uncompressed data.

**Verification**: `test compressStats compressedTotalLength not polluted by uncompressed values` passes — after a
short SET (5 bytes, below `TO_COMPRESS_MIN_DATA_LENGTH`), `compressedTotalLength` is unchanged.

**Strengths**: One-line removal, clear semantics.

### Bug 2 fix — Reverted (NOT A BUG)

The initial fix (`72b0f742`) added `compressStats.rawCount++` to both compression branches under the incorrect
assumption that `rawCount` should count all processed values. That was reverted. `rawCount` correctly tracks only
non-compressed values; `compressedCount` tracks compressed values. The two counters are disjoint by design.

**Verification**: `test compressStats rawCount not incremented for compressed values` passes — after compressing a
300-byte value, `rawCount == 0` and `compressedCount == 1`.

### Bug 4 fix (`87a401c6`, squashed)

**Summary**: Added `if (this == SELF_ZSTD_DICT)` guard to all 5 public `Dict` compress/decompress methods (byte
array and direct buffer variants). `SELF_ZSTD_DICT` delegates to plain `Zstd` methods; all other `Dict` instances
access their `ctxCompressArray`/`decompressCtxArray` directly and NPE if `initCtx()` was never called. An earlier
revision used a null-ctx check (`if (ctxCompressArray != null)`) which was too broad — it would silently fall through
to plain Zstd for any trained dictionary whose contexts failed to initialize, masking a startup error. The final
version scopes the guard to `SELF_ZSTD_DICT` only.

**Verification**: `test SELF_ZSTD_DICT compressByteArray safe to call directly` passes (plain Zstd fallback works).
`test regular Dict without initCtx fails with NPE` passes (loud failure for normal dicts without initialized
contexts).

**Strengths**: Fixed the latent hazard without weakening the failure mode for real dictionaries. The guard is
explicit about which singleton gets special treatment.

**Concerns**: None.

### Overall

All 5 bugs are fixed with TDD, JaCoCo verification, and separate commits. The `CompressStats` metric semantics are
now internally consistent. No regression in the existing `test set` test suite. The `Dict` fix is properly scoped
to `SELF_ZSTD_DICT` only.

### Follow-up rename (`5b0208ce`)

Renamed `rawTotalLength` → `totalInputLength` because the `raw` prefix collided with `rawCount`'s semantics
(`rawCount` = non-compressed only, but `rawTotalLength` = all input). The new name makes the "all input" meaning
explicit. Also updated:
- `CompressStats` class JavaDoc to document all four field semantics inline
- `rawCount` JavaDoc: "Total count of raw data items processed" → "Count of items handled but NOT compressed"
- Prometheus metric `raw_total_length` → `total_input_length` (breaking change for dashboards)
- All 5 call sites and 2 test methods updated
