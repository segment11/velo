# Types Module Bug Review (Round 3)

Date: 2026-04-27
Author: AI agent 1

This round documents new findings in and around the `io.velo.type` module that were not already covered by `bug_10_types_module_review_round_1.md` or `bug_10_types_module_review_round_2.md`.

## Bug 1: `ListPack.decode()` decodes 12-bit string lengths from the wrong bytes

**Severity:** High

**Files:** `src/main/java/io/velo/type/encode/ListPack.java:117-123`, `src/main/java/io/velo/rdb/RDBParser.java:256-262`, `src/main/java/io/velo/rdb/RDBParser.java:298-302`, `src/main/java/io/velo/rdb/RDBParser.java:342-352`, `src/main/java/io/velo/rdb/RDBParser.java:389-397`

```java
} else if ((c & ListPack12BitStringMask) == ListPack12BitString) {  // 12bit string
    nettyBuf.skipBytes(1);
    valueLen = nettyBuf.readUnsignedShortLE() & 0xFFF;
    valueBytes = new byte[valueLen];
    nettyBuf.readBytes(valueBytes);
    nettyBuf.skipBytes(encodeBackLen(valueLen + 2));
}
```

**Description:** Redis listpack 12-bit string length uses the low 4 bits of the first encoding byte plus the following byte. The current decoder skips the first byte and then reads two bytes little-endian, so it consumes the first payload byte as part of the length and starts reading the value from the wrong position.

**Impact:**
- RDB imports containing listpack strings with lengths `64..4095` can fail with buffer errors.
- If the misdecoded length still fits the buffer, imported lists, sets, zsets, or hashes can receive corrupted values.
- This affects all RDB paths that call `ListPack.decode()`, including quicklist2, set-listpack, zset-listpack, and hash-listpack decoding.

## Bug 2: `ZipList.decode()` reads 32-bit string lengths with the wrong byte order

**Severity:** High

**Files:** `src/main/java/io/velo/type/encode/ZipList.java:45-52`, `src/main/java/io/velo/rdb/RDBParser.java:263-268`

```java
} else if ((encoding & ZIP_STR_MASK) == ZIP_STR_32B) {
    len = nettyBuf.readIntLE();
}

var bytes = new byte[len];
nettyBuf.readBytes(bytes);
```

**Description:** Redis ziplist `ZIP_STR_32B` stores the 32-bit string length in big-endian byte order after the encoding byte. Velo reads it with `readIntLE()`. A valid ziplist entry with a large string therefore decodes to the wrong length.

**Impact:**
- Valid Redis RDB quicklist/ziplist payloads containing strings longer than `0x3fff` bytes can fail import.
- Lengths such as `65535` can become negative when read little-endian, causing `NegativeArraySizeException`.
- Other valid lengths can become much larger than the remaining payload, causing large allocation attempts or reader-index failures.

## Bug 3: `RedisList.decode()` and `iterate()` still trust oversized positive element lengths

**Severity:** Medium

**Files:** `src/main/java/io/velo/type/RedisList.java:253-260`, `src/main/java/io/velo/type/RedisList.java:307-313`

```java
int len = buffer.getShort();
if (len <= 0) {
    throw new IllegalStateException("Invalid list entry length: " + len + ", expected > 0");
}
var bytes = new byte[len];
buffer.get(bytes);
```

**Description:** Round 1 fixed the negative-length path in `RedisList.iterate()`, but both `decode()` and `iterate()` still allocate and read using any positive `short` length without checking that enough bytes remain in the buffer. This is the same remaining-buffer hardening gap that was later fixed for `RedisZSet` in round 2.

**Impact:**
- Malformed persisted list bytes can throw `BufferUnderflowException` instead of a controlled decode error.
- A forged positive length can allocate more memory than the payload can satisfy.
- Command paths such as `LRANGE`/`LINDEX` that use `RedisList.iterate()` can fail when they encounter a corrupted encoded list.

## Bug 4: `RedisHashKeys.decode()` and `iterate()` trust oversized positive field lengths

**Severity:** Medium

**Files:** `src/main/java/io/velo/type/RedisHashKeys.java:232-240`, `src/main/java/io/velo/type/RedisHashKeys.java:287-294`

```java
int len = buffer.getShort();
if (len <= 0) {
    throw new IllegalStateException("Length error, length=" + len);
}

var bytes = new byte[len];
buffer.get(bytes);
```

**Description:** `RedisHashKeys` validates only `len <= 0`. It does not check that `len` is less than or equal to the remaining encoded body before allocation and `buffer.get(bytes)`.

**Impact:**
- Corrupted hash-key/set metadata can trigger `BufferUnderflowException`.
- Forged lengths can cause unnecessary allocation before the decoder discovers the payload is too short.
- The issue is reachable from command paths that decode stored hash-key or set metadata, including hash deletion and set iteration helpers.

## Bug 5: `RedisGeo.decode()` bypasses coordinate validation and lacks a remaining-buffer guard

**Severity:** Medium

**Files:** `src/main/java/io/velo/type/RedisGeo.java:152-159`, `src/main/java/io/velo/type/RedisGeo.java:457-466`

```java
public void add(String member, double longitude, double latitude) {
    if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN) {
        throw new IllegalArgumentException("Longitude must be between " + GEO_LONG_MIN + " and " + GEO_LONG_MAX);
    }
    if (latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) {
        throw new IllegalArgumentException("Latitude must be between " + GEO_LAT_MIN + " and " + GEO_LAT_MAX);
    }
    map.put(member, new P(longitude, latitude));
}
```

```java
int memberLength = buffer.getShort();
if (memberLength <= 0) {
    throw new IllegalStateException("Invalid member length: " + memberLength + ", expected > 0");
}
var memberBytes = new byte[memberLength];
buffer.get(memberBytes);
var lon = buffer.getDouble();
var lat = buffer.getDouble();
r.map.put(Wal.keyString(memberBytes), new P(lon, lat));
```

**Description:** Round 1 added coordinate validation to `RedisGeo.add()`, but `decode()` writes directly to `r.map` instead of calling the validated path or performing equivalent checks. It also validates only non-positive member length, not whether `memberLength + 16` bytes remain for the member and coordinates.

**Impact:**
- Malformed persisted geo bytes can hydrate coordinates that normal `add()` now rejects.
- Later `GEODIST`, `GEOHASH`, or radius/box checks can operate on invalid longitude/latitude values.
- Oversized positive member lengths can still cause allocation followed by `BufferUnderflowException`.

## Bug 6: `RedisBF.decode()` accepts an empty sub-filter list, and `BF.LOADCHUNK` can persist it

**Severity:** High

**Files:** `src/main/java/io/velo/type/RedisBF.java:316-333`, `src/main/java/io/velo/type/RedisBF.java:243-250`, `src/main/java/io/velo/command/BGroup.java:651-657`

```java
var listSize = buffer.getInt();
for (int i = 0; i < listSize; i++) {
    var filterBytesLength = buffer.getInt();
    ...
    r.list.add(one);
}
```

```java
var lastOne = list.getLast();
if (lastOne.capacity > Integer.MAX_VALUE / expansion) {
    throw new RuntimeException("BF capacity overflow");
}
```

```java
var encodedLength = buffer.getInt();
var encoded = new byte[encodedLength];

Zstd.decompressByteArray(encoded, 0, encodedLength, dumpBytes, 4, dumpBytes.length - 4);
set(encoded, slotWithKeyHash, CompressedValue.SP_TYPE_BLOOM_BITMAP);
```

**Description:** `RedisBF.decode()` does not validate that `listSize` is within the type invariant `1..MAX_LIST_SIZE`. If `listSize` is `0` or negative, decode returns a Bloom filter object with no sub-filters. A later `put()` reaches `list.getLast()` and throws because the list is empty. `BF.LOADCHUNK` compounds this by storing decompressed bytes as a Bloom value without decoding or validating them first.

**Impact:**
- A malformed Bloom payload can be loaded and persisted through `BF.LOADCHUNK`.
- Later `BF.ADD`, `BF.MADD`, or `BF.INSERT` against that key can crash with an unchecked empty-list failure.
- `BF.INFO` can also report nonsensical zero-capacity metadata for an invalid Bloom filter.

## Reviewer Notes (Round 3 — AI agent 2)

Date: 2026-04-27
Reviewer: AI agent 2

Each bug was verified against the current Velo source code and the canonical Redis implementation in `~/ws/redis`.

---

### Bug 1: **CONFIRMED** — ListPack 12-bit string length decoding is incorrect

**Redis reference** (`src/listpack.c:100`):
```c
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
```
The 12-bit length uses the **low 4 bits of the encoding byte** as the high 4 bits of the length, plus **one** following byte as the low 8 bits. Total header = 2 bytes (encoding byte + 1 length byte).

**Velo code** (`ListPack.java:117-123`) skips the encoding byte (`skipBytes(1)`) then reads **two** bytes with `readUnsignedShortLE() & 0xFFF`. This consumes the first data byte as part of the length field, shifts the value read position by one byte, and corrupts all subsequent decoding.

The correct fix would be:
```java
valueLen = ((c & 0xF) << 8) | nettyBuf.readUnsignedByte();
```
Only one additional byte needs to be read after the encoding byte (which is already in `c`).

---

### Bug 2: **CONFIRMED** — ZipList `ZIP_STR_32B` uses wrong byte order (LE instead of BE)

**Redis reference** (`src/ziplist.c:417-422`, `ZIP_DECODE_LENGTH` macro):
```c
(len) = ((uint32_t)(ptr)[1] << 24) |
        ((uint32_t)(ptr)[2] << 16) |
        ((uint32_t)(ptr)[3] <<  8) |
        ((uint32_t)(ptr)[4]);
```
Redis stores and reads the 32-bit string length in **big-endian** byte order.

**Velo code** (`ZipList.java:46`) uses `readIntLE()`, which reads the 4 bytes in **little-endian** order. For any string length > 0x3FFF (the threshold for `ZIP_STR_32B`), the decoded length will be wrong. For example, length `65536` (0x00010000 big-endian) would be read as `0x00000100` = 256 in little-endian, or could even produce negative values.

The correct fix is to use `readInt()` (big-endian) or manually assemble the 4 bytes in big-endian order.

---

### Bug 3: **CONFIRMED** — `RedisList.decode()`/`iterate()` lack remaining-buffer guard

Code verified at `RedisList.java:255-260` (decode) and `RedisList.java:307-313` (iterate). Both paths check only `len <= 0` but do not verify `len <= buffer.remaining()` before `new byte[len]` + `buffer.get(bytes)`. A corrupted positive length exceeding remaining bytes will throw `BufferUnderflowException`.

This is a hardening gap consistent with the pattern already fixed for `RedisZSet` in round 2.

---

### Bug 4: **CONFIRMED** — `RedisHashKeys.decode()`/`iterate()` lack remaining-buffer guard

Code verified at `RedisHashKeys.java:234-240` (decode) and `RedisHashKeys.java:288-294` (iterate). Same pattern as Bug 3 — only checks `len <= 0`, no remaining-buffer validation.

---

### Bug 5: **CONFIRMED** — `RedisGeo.decode()` bypasses coordinate validation and lacks remaining-buffer guard

Code verified:
- `RedisGeo.java:152-159`: `add()` validates longitude ∈ [−180, 180] and latitude ∈ [−90, 90].
- `RedisGeo.java:457-466`: `decode()` reads `lon`/`lat` with `getDouble()` and writes directly to `r.map` without validation.

A malformed persisted payload can inject invalid coordinates that `add()` would reject. Additionally, there is no check that `memberLength + 16` bytes remain before reading the member and two doubles.

---

### Bug 6: **CONFIRMED** — `RedisBF.decode()` allows empty sub-filter list; `BF.LOADCHUNK` stores unvalidated bytes

Code verified:
- `RedisBF.java:316`: `listSize = buffer.getInt()` with no lower-bound check. A value of 0 or negative produces an empty list.
- `RedisBF.java:243`: `list.getLast()` is called during `put()` expansion — throws `NoSuchElementException` on an empty list.
- `BGroup.java:651-657`: `BF.LOADCHUNK` decompresses and stores bytes directly via `set()` without decoding or validating the Bloom filter structure.

Both paths allow an invalid Bloom filter to be persisted and later crash on `put()`/`madd()`/`insert()`.

---

### Summary

| Bug | Severity | Status | Confidence |
|-----|----------|--------|------------|
| 1 — ListPack 12-bit string length | High | **Confirmed** | High — verified against Redis `listpack.c:100` |
| 2 — ZipList ZIP_STR_32B byte order | High | **Confirmed** | High — verified against Redis `ziplist.c:417-422` |
| 3 — RedisList missing remaining-buffer guard | Medium | **Confirmed** | High — code inspection |
| 4 — RedisHashKeys missing remaining-buffer guard | Medium | **Confirmed** | High — code inspection |
| 5 — RedisGeo decode bypasses coordinate validation | Medium | **Confirmed** | High — code inspection |
| 6 — RedisBF empty sub-filter list / LOADCHUNK | High | **Confirmed** | High — code inspection |

All 6 bugs are confirmed and ready for fix implementation.

## Notes

- This is an authoring pass only. I did not run the second-agent verification workflow in this turn.
- I did not implement fixes in this round; this document is for bug discovery only.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `3a3d7de` `fix: ListPack 12-bit string length decode reads correct bytes`

### Summary of the Fix

The patch changes the 12-bit string decoder from reading two bytes after the encoding byte to using the low 4 bits of the encoding byte plus the following byte:

```java
valueLen = ((c & 0xF) << 8) | nettyBuf.readUnsignedByte();
```

That matches Redis listpack encoding semantics and directly addresses bug 1's root cause. The new tests construct valid 12-bit listpack entries for boundary and multi-entry cases, so they exercise the failing format rather than only normal small strings.

### Strengths

- The production change is minimal and exactly targets the documented length-decoding bug.
- The tests cover the minimum 12-bit length (`64`), a mid-range length (`300`), the maximum 12-bit length (`4095`), and multiple 12-bit entries.
- The tests also keep a 6-bit string case to guard against a nearby decoder regression.

### Concerns

- No correctness findings from this review pass.
- Residual coverage gap: this commit verifies `ListPack.decode()` directly, but it does not add an RDBParser integration test proving listpack-backed list/set/zset/hash import paths decode a 12-bit string end to end.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.encode.ListPackTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the changed 12-bit branch in `build/reports/jacocoHtml/io.velo.type.encode/ListPack.java.html`, including covered lines `src/main/java/io/velo/type/encode/ListPack.java:118-123` and the new length calculation at line 119.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `769cd3f` `fix: ZipList ZIP_STR_32B string length uses big-endian byte order`

### Summary of the Fix

The patch changes `ZIP_STR_32B` string-length decoding from little-endian to big-endian:

```java
len = nettyBuf.readInt();
```

That matches Redis ziplist encoding, where the four length bytes after the `ZIP_STR_32B` marker are stored most-significant byte first. This directly addresses bug 2's root cause.

### Strengths

- The production change is minimal and correctly matches Redis' `ZIP_DECODE_LENGTH` behavior for `ZIP_STR_32B`.
- The tests cover 32-bit string lengths that would decode incorrectly with the old little-endian read.
- The tests also cover nearby `ZIP_STR_06B` and `ZIP_STR_14B` paths to guard against regressions in the adjacent string decoder branches.

### Concerns

- No correctness findings in the production fix.
- The new test fixtures over-allocate the ziplist byte array by four bytes and leave trailing zero bytes after the EOF marker. The decoder ignores those bytes, so the tests still exercise the fixed branch, but future test cleanup should make the fixture byte-perfect.
- Residual integration coverage gap: this commit verifies `ZipList.decode()` directly, but it does not add an RDBParser quicklist/ziplist import test for a `ZIP_STR_32B` entry.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.encode.ZipListTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the changed branch in `build/reports/jacocoHtml/io.velo.type.encode/ZipList.java.html`, including covered lines `src/main/java/io/velo/type/encode/ZipList.java:45-46` and the new `readInt()` call at line 46.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `d8a925a` `fix: RedisList decode/iterate guard oversized positive entry length`

### Summary of the Fix

The patch adds remaining-buffer validation before allocating and reading list entry bytes in both decode paths:

```java
if (len > buffer.remaining()) {
    throw new IllegalStateException("Invalid list entry length: " + len + ", exceeds remaining buffer");
}
```

That directly addresses bug 3 by turning malformed positive lengths into controlled decode errors instead of allowing `BufferUnderflowException` after allocation.

### Strengths

- The production change is minimal and applied consistently to both `RedisList.decode()` and `RedisList.iterate()`.
- The new tests cover oversized positive lengths in both paths.
- The fix preserves the existing `len <= 0` validation and normal decode/iterate behavior.

### Concerns

- No correctness findings in the production fix.
- Residual coverage gap: in the fresh JaCoCo run, the new remaining-buffer branches are covered, but the `RedisList.decode()` `len <= 0` exceptional branch is still not covered. That is adjacent hardening coverage, not a gap in this bug's positive-oversize fix.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisListTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of both new remaining-buffer guards in `build/reports/jacocoHtml/io.velo.type/RedisList.java.html`, including covered checks and throws at `src/main/java/io/velo/type/RedisList.java:259-260` and `src/main/java/io/velo/type/RedisList.java:315-316`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `d8a925a` `fix: RedisList decode/iterate guard oversized positive entry length`

### Summary of the Fix

Adds a `len > buffer.remaining()` guard in both `decode()` and `iterate()`, matching the pattern already applied to `RedisZSet` in round 2. Tests overwrite the first entry length with a large positive value and confirm the `IllegalStateException` with message "exceeds remaining buffer".

### Strengths

- Consistent with the existing `RedisZSet` hardening pattern.
- Tests cover both `decode()` and `iterate()` paths.

### Concerns

- None.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisListTest" --rerun-tasks` successfully.
- JaCoCo confirms `fc` for both guard throw statements at `RedisList.java:260` and `RedisList.java:316`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `0bcb215` `fix: RedisHashKeys decode/iterate guard oversized positive field length`

### Summary of the Fix

The patch adds remaining-buffer validation before allocating and reading hash-key field bytes in both encoded-byte paths:

```java
if (len > buffer.remaining()) {
    throw new IllegalStateException("Length error, length=" + len + ", exceeds remaining buffer");
}
```

That directly addresses bug 4 by rejecting malformed positive field lengths before allocation and before `buffer.get(bytes)` can throw `BufferUnderflowException`.

### Strengths

- The production change is minimal and applied consistently to both `RedisHashKeys.decode()` and `RedisHashKeys.iterate()`.
- The new tests cover oversized positive lengths in both paths.
- The fix follows the same hardening pattern already used for `RedisZSet` and `RedisList`.

### Concerns

- No correctness findings in the production fix.
- Residual coverage gap: in the fresh JaCoCo run, the `RedisHashKeys.iterate()` `len <= 0` exceptional branch remains uncovered. That is adjacent hardening coverage, not a gap in this positive-oversize fix.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisHashKeysTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of both new remaining-buffer guards in `build/reports/jacocoHtml/io.velo.type/RedisHashKeys.java.html`, including covered checks and throws at `src/main/java/io/velo/type/RedisHashKeys.java:238-239` and `src/main/java/io/velo/type/RedisHashKeys.java:295-296`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `459c6c9` `fix: RedisGeo decode validate coordinates and guard oversized member length`

### Summary of the Fix

The patch adds two validations to `RedisGeo.decode()`:

```java
if (memberLength + 16 > buffer.remaining()) {
    throw new IllegalStateException("Invalid member length: " + memberLength + ", exceeds remaining buffer");
}
```

It also validates decoded longitude and latitude against the same bounds enforced by `RedisGeo.add()`. That closes both parts of bug 5: malformed positive member lengths no longer reach `buffer.get(...)`/`getDouble()`, and persisted bytes can no longer hydrate coordinates that normal inserts reject.

### Strengths

- The production change is minimal and localized to `RedisGeo.decode()`.
- The new tests cover oversized positive member length, invalid decoded longitude, and invalid decoded latitude.
- The decode path now preserves the same coordinate invariant as `add()`.

### Concerns

- No correctness findings in the production fix.
- Residual coverage gap: the decode tests cover upper-bound invalid longitude/latitude, but not lower-bound invalid longitude/latitude. The branch is symmetric, so this is a test completeness note rather than a correctness concern.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisGeoTest" --tests "io.velo.type.RedisBFTest" --tests "io.velo.command.BGroupTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the new remaining-buffer guard and coordinate validation in `build/reports/jacocoHtml/io.velo.type/RedisGeo.java.html`, including covered lines `src/main/java/io/velo/type/RedisGeo.java:462-463`, longitude rejection at line 470, and latitude rejection at line 473.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-27
Commit reviewed:
- `f44189a` `fix: RedisBF decode validate list size and BF.LOADCHUNK validate before store`

### Summary of the Fix

The patch adds a `RedisBF.decode()` list-size invariant:

```java
if (listSize <= 0 || listSize > MAX_LIST_SIZE) {
    throw new IllegalStateException("BF list size must be between 1 and " + MAX_LIST_SIZE + ", got: " + listSize);
}
```

It also makes `BGroup.bfLoadchunk()` call `RedisBF.decode(encoded)` before `set(...)`, so malformed Bloom filter bytes are validated before they can be persisted by `BF.LOADCHUNK`.

### Strengths

- `RedisBF.decode()` now rejects empty, negative, and oversized sub-filter lists before producing an invalid in-memory Bloom filter.
- `BF.LOADCHUNK` now validates the decoded Bloom structure before storing the bytes, closing the persistence hole described in bug 6.
- The type-level tests cover all three list-size invalid classes: `0`, `-1`, and a value above `MAX_LIST_SIZE`.

### Concerns

- No correctness findings for the original decode/list-size invariant fix.
- `BF.LOADCHUNK` malformed-input behavior is still not directly regression-tested. The valid command path covers the new `RedisBF.decode(encoded)` call, but there is no command-level test proving an invalid chunk is rejected without persisting.
- `BF.LOADCHUNK` currently lets `RedisBF.decode(encoded)` exceptions propagate. That prevents invalid bytes from being stored, but if the command layer expects Redis error replies instead of unchecked exceptions for malformed client input, this should be followed up with explicit error handling.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisGeoTest" --tests "io.velo.type.RedisBFTest" --tests "io.velo.command.BGroupTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the list-size validation in `build/reports/jacocoHtml/io.velo.type/RedisBF.java.html`, including all branches at `src/main/java/io/velo/type/RedisBF.java:317` and the throw at line 318.
- JaCoCo confirms the `BF.LOADCHUNK` validation call is executed on the valid loadchunk path in `build/reports/jacocoHtml/io.velo.command/BGroup.java.html`, including `src/main/java/io/velo/command/BGroup.java:657`.
