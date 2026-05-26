# Bug Review: Compression Module — Round 2

**Module**: Compression (`CompressedValue`, `Dict`, `DictMap`, `CompressStats`, `TrainSampleJob`, type-level compression)
**Review Date**: 2026-05-26
**Branch**: `review/compression`
**Status**: Initial findings (author)

---

## Bug 6 — Big-string non-compression path does not track `totalInputLength`

**Severity**: Low–Medium — undercounts `totalInputLength` for large values that bypass compression

**Files**:
- `src/main/java/io/velo/BaseCommand.java` lines 955–964

**Code excerpt**:

```java
} else {
    // do not compress
    var isWriteOk = oneSlot.getBigStringFiles().writeBigStringBytes(bigStringUuid,
            slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash,
            valueBytes, bigStringNoMemoryCopy.offset, bigStringNoMemoryCopy.length);
    if (!isWriteOk) {
        throw new RuntimeException("Write big string file error, uuid=" + bigStringUuid + ", key=" + key);
    }

    cvAsBigString.setCompressedDataAsBigString(bigStringUuid, CompressedValue.NULL_DICT_SEQ);
}
```

**Root cause**: The big-string compression path (line 937–954) increments `compressStats.totalInputLength` at line 939 before compressing. The big-string non-compression path (line 955–964) does not increment `totalInputLength` at all. This means large values that exceed `bigStringNoCompressMinSize` and bypass compression are never counted in `totalInputLength`, skewing the `compression_ratio` and throughput metrics.

**Impact**: Operators monitoring `total_input_length` on the dashboard will see lower-than-expected totals if large values are common in their workload. The `compression_ratio` metric is also affected because the denominator excludes non-compressed big strings.

**Fix direction**: Add `compressStats.totalInputLength += bigStringNoMemoryCopy.length` to the non-compression big-string path, mirroring the compression path.

---

## Bug 7 — `Dict.decode()` uses `DataInputStream.available()` as EOF indicator

**Severity**: Low — theoretically unreliable with some `InputStream` implementations

**Files**:
- `src/main/java/io/velo/Dict.java` lines 401–404

**Code excerpt**:

```java
public static DictWithKeyPrefixOrSuffix decode(DataInputStream is) throws IOException {
    if (is.available() < 4) {
        return null;
    }
    var vLength = is.readInt();
    if (vLength == 0) {
        return null;
    }
    ...
}
```

**Root cause**: `DataInputStream.available()` returns the number of bytes that can be read *without blocking*. The Java spec states this can return 0 even when data is still coming from some stream types. Using `available()` as an EOF check is fragile — if the underlying stream has data buffered internally but not yet surfaced through `available()`, the method returns `null` prematurely, silently dropping dictionary entries at the end of `dict-map.dat`.

**Impact**: If `dict-map.dat` is read over a non-file stream (e.g., a buffered network stream or a wrapped stream), the final dictionary entry could be silently skipped. With `FileInputStream` on a local file, this works reliably in practice, but the pattern is fragile.

**Fix direction**: Read `vLength` directly and catch `EOFException`, or restructure the loop to detect EOF through `readInt()` throwing `EOFException`.

---

## Bug 8 — `Dict.generateRandomSeq()` creates a new `Random` instance on every call

**Severity**: Low — wastes allocations and may produce identical sequences under rapid calls

**Files**:
- `src/main/java/io/velo/Dict.java` lines 451–457

**Code excerpt**:

```java
static int generateRandomSeq() {
    var random = new Random();
    return random.nextInt(1000) * 1000 * 1000 +
            random.nextInt(1000) * 1000 +
            random.nextInt(1000) +
            SELF_ZSTD_DICT_SEQ;
}
```

**Root cause**: Each call to `generateRandomSeq()` instantiates a new `java.util.Random` with a nanoTime-based seed. Under rapid successive calls (e.g., training multiple dictionaries in a tight loop), the seed may not advance, producing identical first `nextInt()` values and thus the same sequence number. The `DictMap.putDict()` method only retries once on collision before throwing.

**Impact**: In normal operation this is unlikely — dictionary training involves Zstd operations that take milliseconds, ensuring the seed advances. But it's a latent hazard on fast hardware or in tests.

**Fix direction**: Replace with a shared `ThreadLocalRandom` or a static `Random` instance with `synchronized` access, or use `UUID.randomUUID()`-derived values.

---

## Summary

| # | Severity | Title |
|---|----------|-------|
| 6 | Low–Medium | Big-string non-compression path does not track `totalInputLength` |
| 7 | Low | `Dict.decode()` uses `is.available()` as EOF indicator |
| 8 | Low | `Dict.generateRandomSeq()` creates new `Random` on every call |

---

## AI Agent 2 Review Notes — 2026-05-26

Reviewed against the current code on `review/compression` / `main` at HEAD `12d11431`.

### Bug 6 — Confirmed

The current big-string path still updates `compressStats.totalInputLength` only in the compression-capable branch:
`src/main/java/io/velo/BaseCommand.java:937-947`. The non-compression branch at
`BaseCommand.java:955-963` writes the big-string bytes and stores `CompressedValue.NULL_DICT_SEQ`, but does not update
`totalInputLength`.

This is a real metrics accounting gap. If a value exceeds `ConfForGlobal.bigStringNoCompressMinSize`, it bypasses
compression and is excluded from the denominator used by `CompressStats.compression_ratio`
(`src/main/java/io/velo/CompressStats.java:82`). With mixed workloads, that makes the ratio look better than reality
because non-compressed big-string input bytes are missing from `totalInputLength`.

There is a related metric semantics question: since `rawCount` now means "items handled but NOT compressed"
(`CompressStats.java:15` and `BaseCommandTest.test compressStats rawCount not incremented for compressed values`), the
big-string non-compression branch likely should increment `rawCount` as well. The round 2 finding only calls out
`totalInputLength`, but a complete fix should decide whether this branch is included in request-level non-compressed
item counts.

**Verdict:** Confirmed metric correctness bug.

### Bug 7 — Confirmed as a defensive/API issue, not a current production file-read bug

`Dict.decode(DataInputStream)` still uses `is.available() < 4` as the EOF guard at
`src/main/java/io/velo/Dict.java:401-404`. That pattern is fragile for arbitrary `InputStream` implementations because
`available()` is not a general EOF contract.

The production dictionary-map load path, however, currently wraps a local file stream:
`new DataInputStream(new FileInputStream(file))` in `src/main/java/io/velo/DictMap.java:256`, then loops on
`Dict.decode(is)` at `DictMap.java:257-260`. For this local file path, the reported "final dictionary silently skipped
from dict-map.dat" impact is not a confirmed current production failure. Other current code paths found by search use
`ByteArrayInputStream` for encoded dictionary bytes, where `available()` is also deterministic.

The cleaner implementation is still to read the length with `readInt()` and return `null` on `EOFException`, because it
documents EOF through the read operation itself and makes `Dict.decode` robust for any future stream source.

**Verdict:** Confirmed low-severity defensive issue; impact should be narrowed from current `dict-map.dat` corruption to
future/non-file stream fragility.

### Bug 8 — Not confirmed as stated

`Dict.generateRandomSeq()` still creates a new `Random` on every call at `src/main/java/io/velo/Dict.java:451-456`.
That is a small avoidable allocation, but the stated root cause that rapid calls may reuse the same nanoTime seed is not
accurate for the current JDK. The local JDK is OpenJDK 21.0.9, and `javap -c -p java.util.Random` shows the no-arg
constructor seeds from both `seedUniquifier()` and `System.nanoTime()`. `seedUniquifier()` is backed by an
`AtomicLong`, so rapid successive `new Random()` calls do not rely only on nanoTime advancing.

Sequence collisions are still possible because the generated sequence space is finite and random, and `DictMap.putDict()`
only retries once on collision (`src/main/java/io/velo/DictMap.java:103-110`). The existing test already acknowledges
that collisions are possible (`src/test/groovy/io/velo/DictTest.groovy:32-39`). But that is a probabilistic collision
property of the sequence generator, not a "new Random uses identical seed under rapid calls" bug.

Using `ThreadLocalRandom` or a monotonic/allocated sequence source may be a reasonable cleanup to reduce allocation or
make collision behavior easier to reason about, but I would not classify the current implementation as a confirmed bug
on the evidence in this finding.

**Verdict:** Not confirmed as stated. At most this is a low-priority cleanup / collision-policy improvement.

### Reviewer Summary

| # | Reviewer verdict | Notes |
|---|------------------|-------|
| 6 | Confirmed | Big-string non-compression skips `totalInputLength`; consider `rawCount` semantics too. |
| 7 | Confirmed latent/API issue | `available()` is fragile generally, but current file/byte-array paths are not confirmed broken. |
| 8 | Not confirmed as stated | JDK 21 `Random()` uses `seedUniquifier()` plus `nanoTime`; allocation is real, identical rapid-call seed claim is not. |
