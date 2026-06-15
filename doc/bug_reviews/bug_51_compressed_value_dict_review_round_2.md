# Bug 51 CompressedValue/Dict/DictMap Review Round 2

Author: AI agent 1

Review date: 2026-06-15

## Scope

Round 2 review of `CompressedValue`, `Dict`, and `DictMap`. Round 1 found 6
findings (2 fixed: `isTypeNumber()` gap values, `Dict.decode()` negative
`dictBytesLength`; 4 won't-fix). This round focuses on bugs NOT covered by
round 1 — logic-level correctness, charset handling, sequence-collision with
the reserved self-dict, decompression robustness, and operation ordering.

Design documents reviewed:

- `doc/design/07_compression_design.md`
- `doc/design/02_persist_layer_design.md`

Code reviewed:

- `src/main/java/io/velo/CompressedValue.java` (879 lines)
- `src/main/java/io/velo/Dict.java` (476 lines)
- `src/main/java/io/velo/DictMap.java` (320 lines)

Call sites traced:

- `src/main/java/io/velo/BaseCommand.java` (`getValueBytesByCv`, `setCv`, `set`)
- `src/main/java/io/velo/TrainSampleJob.java` (`keyPrefixOrSuffixGroup`)
- `src/main/java/io/velo/decode/Request.java`, `RequestDecoder.java`

---

## Finding 1: `Dict.generateRandomSeq()` can return `SELF_ZSTD_DICT_SEQ` (1), causing silent data corruption

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/Dict.java:456-462` (`generateRandomSeq()`)
- `src/main/java/io/velo/DictMap.java:101-112` (`putDict()` collision check)
- `src/main/java/io/velo/BaseCommand.java:714-726` (decompression dict lookup)
- `src/main/java/io/velo/BaseCommand.java:1037` (compression dict seq storage)

**Code excerpt:**

```java
// Dict.java:456-462
static int generateRandomSeq() {
    var random = new Random();
    return random.nextInt(1000) * 1000 * 1000 +   // 0..999,000,000
            random.nextInt(1000) * 1000 +           // 0..999,000
            random.nextInt(1000) +                   // 0..999
            SELF_ZSTD_DICT_SEQ;                      // +1
}
// Minimum return value: 0 + 0 + 0 + 1 = 1 = SELF_ZSTD_DICT_SEQ
```

```java
// DictMap.java:101-112 — collision check does NOT check against SELF_ZSTD_DICT
public synchronized Dict putDict(String keyPrefixOrSuffix, Dict dict) {
    var existDict = cacheDictBySeq.get(dict.getSeq());  // SELF_ZSTD_DICT is NOT in this map
    if (existDict != null) {
        dict.setSeq(Dict.generateRandomSeq());
        ...
    }
    // if dict.getSeq() == 1, it passes the check — SELF_ZSTD_DICT is a separate singleton
    ...
}
```

```java
// BaseCommand.java:714-726 — decompression dict lookup
if (cv.isCompressed()) {
    Dict dict = null;
    if (cv.isUseDict()) {
        var dictSeqOrSpType = cv.getDictSeqOrSpType();
        if (dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ) {   // == 1
            dict = Dict.SELF_ZSTD_DICT;                      // WRONG dict used!
        } else {
            dict = dictMap.getDictBySeq(dictSeqOrSpType);
        }
    }
    var decompressed = cv.decompress(dict);  // decompresses with wrong dict
```

```java
// BaseCommand.java:1037 — compression stores the dict's seq into the value
cv.setDictSeqOrSpType(cr.isCompressed() ? dict.getSeq() : CompressedValue.NULL_DICT_SEQ);
// if dict.getSeq() == 1, the value's dictSeqOrSpType is set to 1 = SELF_ZSTD_DICT_SEQ
```

**Root cause and impact:**

`generateRandomSeq()` adds `SELF_ZSTD_DICT_SEQ` (1) as a base offset, but all
three `random.nextInt(1000)` calls can independently return 0, making the
minimum generated seq exactly 1. This collides with the reserved self-dict
sequence number.

The collision is NOT detected by `putDict()` because the collision check only
looks in `cacheDictBySeq`, which never contains `SELF_ZSTD_DICT` (it is a
separate static singleton, not registered in the map). So a trained dict with
seq=1 is silently accepted and stored.

When a value is later compressed with this trained dict, `dictSeqOrSpType` is
set to `dict.getSeq()` = 1 (BaseCommand.java:1037). On decompression,
`getValueBytesByCv()` checks `dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ` (1)
and unconditionally selects `Dict.SELF_ZSTD_DICT` (BaseCommand.java:718-719) —
bypassing the correct `dictMap.getDictBySeq(1)` lookup. The value is then
decompressed with the wrong dictionary, producing garbage bytes or a Zstd
decompression error.

This is **silent data corruption** for every value compressed with the
seq=1 dict. The `Dict.equals()` / `hashCode()` methods (which compare only
`seq`) would also consider the trained dict equal to `SELF_ZSTD_DICT`.

**Reachability:**

- Probability of generating seq=1 is 1/10^9 per trained dict.
- With automatic training (`ConfForGlobal.isOnDynTrainDictForCompression`),
  new dicts are created regularly, so over a long-running server lifetime the
  collision is plausible.
- `Dict(byte[] dictBytes)` constructor calls `generateRandomSeq()`, and every
  `putDict()` caller (`TrainSampleJob.train()`, `ManageCommand`, `XGroup`,
  `XDict` replay) uses this constructor.

**Suggested fix direction (chosen):**

Two-layer fix — correct the source (`generateRandomSeq`) **and** add a
defense-in-depth guard in `putDict()`.

**Layer 1: Offset the range so the minimum is > `SELF_ZSTD_DICT_SEQ`** — one-line
change, O(1), no loop:

```java
// Dict.java:456-462
static int generateRandomSeq() {
    var random = ThreadLocalRandom.current();
    return (random.nextInt(999) + 1) * 1000 * 1000 +   // min term = 1, not 0
            random.nextInt(1000) * 1000 +
            random.nextInt(1000) +
            SELF_ZSTD_DICT_SEQ;
    // minimum return = 1,000,000 + 0 + 0 + 1 = 1,000,001 — never 1
}
```

The range shrinks from `[1, 999_999_001]` to `[1_000_001, 999_999_001]` — losing
only 0.1 % of the sequence space, well within the ~10^9 budget. Also replaces
`new Random()` with `ThreadLocalRandom` (minor cleanup, avoids per-call
allocation).

**Layer 2: Defense-in-depth guard in `putDict()`** — the existing collision check
already inspects `cacheDictBySeq`; add the reserved-seq check to the same
condition so it catches dicts constructed with seq=1 via any path:

```java
// DictMap.java:103-104
var existDict = cacheDictBySeq.get(dict.getSeq());
if (dict.getSeq() == Dict.SELF_ZSTD_DICT_SEQ || existDict != null) {
    dict.setSeq(Dict.generateRandomSeq());
    ...
}
```

**Why both layers:** Layer 1 prevents the bad value at the source. Layer 2
protects against future regressions in `generateRandomSeq()` or direct
`dict.setSeq(1)` calls from other code paths (e.g., `XDict` replay decoding a
corrupted seq from the binlog).

**Regression test should include:**

- Call `generateRandomSeq()` in a loop (e.g., 100 000 iterations) and assert
  the result is never `SELF_ZSTD_DICT_SEQ` (1) and is always >
  `SELF_ZSTD_DICT_SEQ`.
- Construct a `Dict` with `setSeq(1)` and call `putDict()`; verify the dict's
  seq is changed to something other than 1 before being stored.
- Set a CompressedValue's `dictSeqOrSpType` to 1 and verify
  `getValueBytesByCv()` does NOT confuse a trained seq=1 dict with
  `SELF_ZSTD_DICT`.

---

## Finding 2: `CompressedValue.decompress()` throws `NegativeArraySizeException` on corrupted or unknown-size frames

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/CompressedValue.java:599-612` (`decompress()`)
- `src/main/java/io/velo/CompressedValue.java:193-199` (`getUncompressedLength()`)

**Code excerpt:**

```java
// CompressedValue.java:599-612
public byte[] decompress(@Nullable Dict dict) {
    assert compressedData != null;
    var dst = new byte[(int) Zstd.getFrameContentSize(compressedData)];  // line 601
    int r;
    if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
        r = (int) Zstd.decompress(dst, compressedData);
    } else {
        r = dict.decompressByteArray(dst, 0, compressedData, 0, compressedData.length);
    }
    if (r <= 0) {
        throw new IllegalStateException("Decompress error");
    }
    return dst;
}
```

```java
// CompressedValue.java:193-199
public int getUncompressedLength() {
    if (!isCompressed()) {
        return getCompressedLength();
    }
    return (int) Zstd.getFrameContentSize(compressedData);  // line 198
}
```

**Root cause and impact:**

`Zstd.getFrameContentSize()` (zstd-jni 1.5.5-10) follows the C library
contract and returns:

- The original content size as a `long` (≥ 0) on success.
- `-1` (`ZSTD_CONTENTSIZE_UNKNOWN`) when the frame header omits the size.
- `-2` (`ZSTD_CONTENTSIZE_ERROR`) when the frame is corrupted or not a valid
  Zstd frame.

When the return is -1 or -2, `(int)` cast produces -1 or -2, and
`new byte[-1]` throws `NegativeArraySizeException` — an unchecked exception
with no descriptive message.

`getUncompressedLength()` has the same root cause: it returns -1 or -2 as a
valid-looking length. The return value is used in two places:

- `BaseCommand.java:887`: `cv.getUncompressedLength() << 8 | shortType` — a
  negative length corrupts the stored key-analysis metadata (the high 24 bits
  of `valueLengthHigh24WithShortTypeLow8` would contain sign-extended bits).
- `SGroup.java:790`: `return new IntegerReply(cv.getUncompressedLength())` —
  returns a negative value to the Redis client.

**Reachability:**

- Velo's own `Zstd.compress()` always writes the content size into the frame
  header, so normal data is safe.
- A bit-flip in storage (disk corruption, partial write during crash) can
  corrupt the frame magic or the content-size field, causing
  `getFrameContentSize` to return -2.
- `decompress()` is called from `getValueBytesByCv()` (BaseCommand.java:729)
  on every GET of a compressed value, with no try/catch for
  `NegativeArraySizeException`.
- `getUncompressedLength()` is called during SET processing
  (BaseCommand.java:887), which runs on the write path.

**Suggested fix direction:**

Validate the return before using it:

```java
public byte[] decompress(@Nullable Dict dict) {
    assert compressedData != null;
    long rawSize = Zstd.getFrameContentSize(compressedData);
    if (rawSize < 0) {
        throw new IllegalStateException(
            "Decompress error, invalid frame content size=" + rawSize);
    }
    var dst = new byte[(int) rawSize];
    ...
}
```

Apply the same guard to `getUncompressedLength()`.

**Regression test should include:**

- Pass a corrupted byte array (e.g., all zeros, or truncated frame) to
  `decompress()` and verify `IllegalStateException` is thrown (not
  `NegativeArraySizeException`).
- Verify `getUncompressedLength()` returns a sensible value or throws for
  corrupted data.

---

## Finding 3: `Dict.encode()` buffer sized by `String.length()` (char count) but written with `getBytes()` (byte count) — overflow on non-ASCII prefixes

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/Dict.java:363-378` (`encode()`)
- `src/main/java/io/velo/Dict.java:402-438` (`decode()` — corresponding read)

**Code excerpt:**

```java
// Dict.java:363-378
public byte[] encode(String keyPrefixOrSuffix) {
    int vLength = ENCODED_HEADER_LENGTH + keyPrefixOrSuffix.length() + dictBytes.length;
    //                                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                                    Java char count, NOT byte count
    var bytes = new byte[4 + vLength];
    var buffer = ByteBuffer.wrap(bytes);

    buffer.putInt(vLength);
    buffer.putInt(seq);
    buffer.putLong(createdTime);
    buffer.putShort((short) keyPrefixOrSuffix.length());  // stored as char count
    buffer.put(keyPrefixOrSuffix.getBytes());              // writes UTF-8 byte count
    //     ^^^ --- if getBytes().length > length(), this overflows the buffer
    buffer.putShort((short) dictBytes.length);
    buffer.put(dictBytes);

    return bytes;
}
```

**Root cause and impact:**

On JDK 21, the default charset is UTF-8. `String.length()` returns the number
of Java `char` values (UTF-16 code units), while `String.getBytes()` (no
charset argument) encodes using the default charset (UTF-8).

For any prefix containing characters outside the ASCII range (U+0080..U+FFFF),
the UTF-8 byte count exceeds the char count:

| Character range | `length()` (chars) | `getBytes()` UTF-8 bytes |
|-----------------|--------------------:|--------------------------:|
| U+0000..U+007F  | 1                   | 1                        |
| U+0080..U+07FF  | 1                   | 2                        |
| U+0800..U+FFFF  | 1                   | 3                        |

The buffer is allocated based on `keyPrefixOrSuffix.length()` (char count), but
`buffer.put(keyPrefixOrSuffix.getBytes())` writes the UTF-8 byte count. When
the byte count exceeds the char count, this causes a
`BufferOverflowException` (unchecked).

The corresponding `decode()` reads `keyPrefixOrSuffixLength` (stored as char
count), allocates `new byte[keyPrefixOrSuffixLength]`, and calls
`is.readFully(...)`. If the encode somehow succeeded (e.g., the buffer was
larger for another reason), the decode would read the wrong number of bytes,
corrupting all subsequent fields (`dictBytesLength`, `dictBytes`).

**Reachability:**

- Key prefixes are derived in `TrainSampleJob.keyPrefixOrSuffixGroup(key)`
  (TrainSampleJob.java:203-227) via `key.substring(...)`, `key.lastIndexOf('.')`,
  etc. The key string originates from `new String(data[i])` (Request.java:229)
  using the default charset (UTF-8 on JDK 21).
- Redis keys are arbitrary byte sequences. If a key contains non-ASCII UTF-8
  bytes (e.g., Chinese, accented characters, emoji), the derived prefix can
  contain multi-byte characters.
- The prefix is also user-configurable via the dynamic config key
  `dict_key_prefix_or_suffix_groups` (DynConfig.java:69-70) and via the
  `ManageCommand` `xdict` subcommand (ManageCommand.groovy:571), so an
  operator can directly set a non-ASCII prefix.
- `putDict()` is called from `TrainSampleJob.train()` (automatic dict training)
  and from `ManageCommand`/`XGroup` (manual dict management).

**Suggested fix direction:**

Use the UTF-8 byte count consistently for both sizing and writing:

```java
public byte[] encode(String keyPrefixOrSuffix) {
    var prefixBytes = keyPrefixOrSuffix.getBytes(StandardCharsets.UTF_8);
    int vLength = ENCODED_HEADER_LENGTH + prefixBytes.length + dictBytes.length;
    var bytes = new byte[4 + vLength];
    var buffer = ByteBuffer.wrap(bytes);

    buffer.putInt(vLength);
    buffer.putInt(seq);
    buffer.putLong(createdTime);
    buffer.putShort((short) prefixBytes.length);
    buffer.put(prefixBytes);
    buffer.putShort((short) dictBytes.length);
    buffer.put(dictBytes);
    return bytes;
}
```

Also update `decode()` to decode with an explicit charset:
`new String(keyPrefixOrSuffixBytes, StandardCharsets.UTF_8)`.

**Regression test should include:**

- Call `Dict.encode()` with a non-ASCII prefix (e.g., `"用户"` or `"café"`)
  and verify no exception is thrown and the round-trip `decode()` produces
  the same string.
- Verify `vLength` matches `ENCODED_HEADER_LENGTH + prefixBytes.length +
  dictBytes.length`.

---

## Finding 4: `DictMap.putDict()` writes to file before binlog and cache update — partial failure leaves the system inconsistent

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/DictMap.java:101-145` (`putDict()`)

**Code excerpt:**

```java
// DictMap.java:101-145
public synchronized Dict putDict(String keyPrefixOrSuffix, Dict dict) {
    // 1. seq collision check (lines 102-112)
    ...

    // 2. Write to file FIRST (line 115)
    try {
        fos.write(dict.encode(keyPrefixOrSuffix));   // persisted immediately
    } catch (IOException e) {
        throw new RuntimeException("Write dict to file error", e);
    }

    // 3. Append to binlog (lines 120-137) — may throw RuntimeException
    var firstOneSlot = localPersist.firstOneSlot();
    if (firstOneSlot != null && firstOneSlot.getDynConfig().isBinlogOn()) {
        var p = firstOneSlot.asyncCall(() -> {
            ...
            throw new RuntimeException("Append binlog error, ...", e);
            ...
        });
        var e = p.getException();
        if (e != null) {
            throw (RuntimeException) e;  // THROWS HERE on binlog failure
        }
    }

    // 4-6. These are SKIPPED if step 3 throws:
    TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);  // line 139
    dict.initCtx();                                                   // line 141
    cacheDictBySeq.put(dict.getSeq(), dict);                         // line 143
    return cacheDict.put(keyPrefixOrSuffix, dict);                   // line 144
}
```

**Root cause and impact:**

`putDict()` performs three side effects in order: file write → binlog append →
cache update. If the binlog append fails (step 3 throws), steps 4-6 are
skipped. This creates two inconsistencies:

1. **File-vs-cache inconsistency**: The dict is persisted to `dict-map.dat`
   (append mode, line 249) but is NOT in `cacheDictBySeq` / `cacheDict`. The
   dict cannot be used for compression/decompression during the current
   session. The caller receives a `RuntimeException`, believing the operation
   failed, but the dict is actually on disk. On the next restart,
   `initDictMap()` loads it from the file, making it appear unexpectedly.

2. **Retry duplication**: If the caller retries `putDict()` after the failure,
   the dict is appended to `dict-map.dat` again (the file is opened in append
   mode). On restart, `initDictMap()` loads both entries. If the seq was
   regenerated between retries (collision check at line 106), the file ends up
   with multiple dicts for the same key prefix but different seqs — only the
   last-loaded one survives in the cache, but all remain in the file.

3. **Master-slave divergence**: If the binlog append fails on the master but
   the dict was already written to the master's `dict-map.dat`, the slave
   never receives the dict (no binlog entry). After a master restart, the dict
   is loaded from the master's file but the slave has no corresponding dict.
   Values compressed with this dict on the master would fail to decompress on
   the slave with `DictMissingException`.

**Reachability:**

- Binlog failures can occur from disk-full conditions, I/O errors, or
  binlog-size-limit enforcement.
- The method is `@SlaveNeedReplay` / `@SlaveReplay` annotated, indicating it
  runs on both master and slave, widening the failure surface.

**Suggested fix direction:**

Reorder so the cache update happens before the binlog, or make the operation
idempotent on retry:

**Option A (preferred): move cache update before binlog, and make binlog
failure non-fatal to the cache state:**

```java
// write file
fos.write(dict.encode(keyPrefixOrSuffix));
// update cache first (the dict is persisted, so it should be usable)
dict.initCtx();
cacheDictBySeq.put(dict.getSeq(), dict);
cacheDict.put(keyPrefixOrSuffix, dict);
TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);
// then append binlog (failure logged but doesn't undo the cache)
try {
    ... binlog append ...
} catch (RuntimeException e) {
    log.error("Append binlog error (dict is in cache but not replicated), dict key prefix={}", keyPrefixOrSuffix, e);
    // don't rethrow — the dict is already persisted and in cache
}
```

**Option B: clean up the file write on binlog failure** (harder, since the
file is append-mode and there's no easy rollback).

**Regression test should include:**

- Mock `firstOneSlot.getBinlog().append(...)` to throw IOException, call
  `putDict()`, and verify the dict IS in `cacheDictBySeq` after the call
  (Option A) or is NOT in the file (Option B).
- Verify a second `putDict()` with the same dict does not produce a duplicate
  entry in the file.

---

## Finding 5: `DictMap.cleanUp()` does not set `fos = null` when `fos.close()` throws, leaving the map in an inconsistent state

**Severity:** Low

**Files:**

- `src/main/java/io/velo/DictMap.java:193-207` (`cleanUp()`)

**Code excerpt:**

```java
// DictMap.java:193-207
@Override
public synchronized void cleanUp() {
    if (fos != null) {
        try {
            fos.close();
            System.out.println("Close dict fos");
            fos = null;                    // ONLY reached on success
        } catch (IOException e) {
            System.err.println("Close dict fos error, message=" + e.getMessage());
            // fos is NOT set to null here!
        }
    }

    for (var entry : cacheDictBySeq.entrySet()) {
        entry.getValue().closeCtx();
    }
}
```

**Root cause and impact:**

If `fos.close()` throws `IOException`, execution jumps to the catch block and
`fos = null` is skipped. `fos` remains non-null but points to a stream in an
indeterminate state (the underlying file descriptor may or may not be closed
depending on where the error occurred).

Consequences:

1. **Double-close on next cleanUp()**: A subsequent `cleanUp()` call re-enters
   the `if (fos != null)` block and calls `fos.close()` again on the already
   (or partially) closed stream. While `FileOutputStream.close()` on an
   already-closed stream is documented as a no-op in modern JDKs, the first
   error is silently swallowed.

2. **Write to closed stream**: If `putDict()` is called after the failed
   `cleanUp()`, `fos.write(...)` on line 115 attempts to write to the
   broken/closed stream, throwing `IOException` (wrapped as
   `RuntimeException`).

3. **Stale error**: The error message goes to `System.err` (not the SLF4J
   logger used elsewhere in the class), and is easily missed in production
   monitoring.

**Reachability:**

- `fos.close()` can throw on I/O errors (e.g., NFS flush failure, disk error
  during final flush, file descriptor exhaustion).
- `cleanUp()` is called from `MultiWorkerServer` shutdown and from test
  teardown. A transient I/O error during shutdown leaves `fos` dangling.

**Suggested fix direction:**

Use a `finally` block to ensure `fos` is always nulled, and use the SLF4J
logger:

```java
public synchronized void cleanUp() {
    if (fos != null) {
        try {
            fos.close();
            log.info("Close dict fos");
        } catch (IOException e) {
            log.error("Close dict fos error, message={}", e.getMessage());
        } finally {
            fos = null;   // always null, regardless of close success/failure
        }
    }

    for (var entry : cacheDictBySeq.entrySet()) {
        entry.getValue().closeCtx();
    }
}
```

**Regression test should include:**

- Use a mock `FileOutputStream` whose `close()` throws `IOException`, call
  `cleanUp()`, and verify `fos` is null afterward (via reflection or a
  follow-up `putDict()` that checks the state).
- Verify a second `cleanUp()` call does not attempt to close again.

---

## Summary

| # | Finding | Severity | Class |
|---|---------|----------|-------|
| 1 | `generateRandomSeq()` can return `SELF_ZSTD_DICT_SEQ` (1) → silent data corruption | Medium | Dict |
| 2 | `decompress()` / `getUncompressedLength()` crash on corrupted Zstd frames | Medium | CompressedValue |
| 3 | `Dict.encode()` char-length vs byte-length mismatch → buffer overflow on non-ASCII | Medium | Dict |
| 4 | `putDict()` partial failure: file write before binlog/cache leaves system inconsistent | Medium | DictMap |
| 5 | `cleanUp()` doesn't null `fos` on `IOException` → inconsistent state | Low | DictMap |

All five findings are genuinely new — none overlap with round 1's scope.
Findings 1 and 3 are the most impactful: Finding 1 causes silent data
corruption (wrong-dictionary decompression), and Finding 3 causes a hard crash
on realistic non-ASCII input.

Recommended fix priority (for the reviewer and implementing agent):

1. **Finding 1** — silent data corruption; highest impact; self-contained fix.
2. **Finding 3** — hard crash on realistic input; self-contained fix.
3. **Finding 2** — robustness on corrupted data; straightforward guard.
4. **Finding 4** — ordering fix; touches the hot `putDict` path; needs care.
5. **Finding 5** — minor hardening; low risk.

---

## Review Notes (AI agent 2)

Reviewer: AI agent 2
Review date: 2026-06-15

I read the current source (`Dict.java`, `CompressedValue.java`, `DictMap.java`,
`BaseCommand.java`, `TrainSampleJob.java`, `XDict.java`, `ToSlaveExistsDict.java`),
the existing tests (`DictTest.groovy`, `CompressedValueTest.groovy`,
`DictMapTest.groovy`), the zstd-jni 1.5.5-10 bytecode, and traced every
production caller of the affected methods. Below is the status for each
finding.

### Finding 1 — CONFIRMED

Verified exactly as described:

- `Dict.SELF_ZSTD_DICT_SEQ = 1` (`Dict.java:30`), and `SELF_ZSTD_DICT.seq = 1`
  via the static initializer (`Dict.java:42-44`).
- `generateRandomSeq()` (`Dict.java:456-462`) returns
  `nextInt(1000)*1e6 + nextInt(1000)*1e3 + nextInt(1000) + 1`. All three
  `nextInt(1000)` calls can independently return 0, producing minimum value 1.
  Probability of seq=1 per call is `(1/1000)^3 = 1e-9`; not vanishingly small
  over a long-running server with dynamic dict training
  (`ConfForGlobal.isOnDynTrainDictForCompression`).
- `DictMap.putDict()` collision check (`DictMap.java:101-112`) only consults
  `cacheDictBySeq`, which is the trained-dict map. `SELF_ZSTD_DICT` is a
  separate static singleton and is never registered there, so a generated
  seq=1 is accepted.
- `BaseCommand.getValueBytesByCv()` (`BaseCommand.java:716-725`) branches on
  `dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ` *before* calling
  `dictMap.getDictBySeq(dictSeqOrSpType)`. With a value compressed against a
  trained dict whose seq happens to be 1, this check selects
  `Dict.SELF_ZSTD_DICT` and the value is decompressed with the wrong
  dictionary, producing garbage.
- The same `dictSeq == Dict.SELF_ZSTD_DICT_SEQ` early-branch exists at
  `RedisHH.java:336`, so the impact also covers hash storage decompression
  (`decompressIfUseDict`), not just string-value decompression.
- `Dict.equals()` / `hashCode()` (`Dict.java:322-339`) compare only `seq`. A
  trained dict with seq=1 would be considered equal to `SELF_ZSTD_DICT`. No
  production code currently uses `equals()` for this comparison (call sites
  use `==` reference identity, see `CompressedValue.java:603,647`,
  `BaseCommand.java:1048`, `RedisHH.java:196`), so the equals side effect is
  not currently reachable — but it's a latent foot-gun if anyone refactors
  to use `equals()`.
- The compression path stores the dict seq into the value
  (`BaseCommand.java:1037`): `cv.setDictSeqOrSpType(cr.isCompressed() ? dict.getSeq() : NULL_DICT_SEQ)`. With `dict.getSeq() == 1` the value is stamped with 1
  and the decompression branch is taken on read.

Confirmed; severity Medium is appropriate (silent corruption, but rare). The
suggested fix direction (`(random.nextInt(1000) + 1) * 1e6 + ...` so the
minimum is `1_001_001` instead of 1) plus a defensive `putDict()` guard
against `dict.getSeq() == SELF_ZSTD_DICT_SEQ` are both worth doing. The first
is the cleanest.

### Finding 2 — CONFIRMED

Verified exactly as described:

- `CompressedValue.decompress()` (`CompressedValue.java:599-612`):
  `new byte[(int) Zstd.getFrameContentSize(compressedData)]` (line 601)
  executes before any error handling. The contract for `getFrameContentSize`
  is documented (zstd-jni 1.5.5-10 native bridge to libzstd): returns
  `>= 0` on success, `ZSTD_CONTENTSIZE_UNKNOWN = -1` when the frame header
  omits the size, and `ZSTD_CONTENTSIZE_ERROR = -2` on a corrupt frame.
  Casting -1 or -2 to `int` and passing to `new byte[...]` raises
  `NegativeArraySizeException` from the JVM, which has no descriptive
  message and propagates up.
- Same root cause in `getUncompressedLength()` (`CompressedValue.java:193-199`):
  on a corrupt frame it returns -1 or -2 cast to int.
- Reachability for both:
  - `decompress()` is called on every compressed-value GET via
    `getValueBytesByCv()` (`BaseCommand.java:729`), with no try/catch.
  - `getUncompressedLength()` is called in two hot paths:
    - `BaseCommand.java:887`:
      `valueLengthHigh24WithShortTypeLow8 = cv.getUncompressedLength() << 8 | shortType`.
      With `getUncompressedLength() == -1` (binary `0xFFFFFFFF`), the shift
      sign-extends and the resulting int has corrupted high bits, feeding
      garbage into the key-analysis handler.
    - `SGroup.java:790`: `return new IntegerReply(cv.getUncompressedLength())`,
      i.e. a negative length is returned directly to a Redis client (e.g.
      the `STRLEN` command path).
- Trigger conditions are realistic: bit-flips on disk, partial writes from
  crashes, and torn WAL frames all corrupt the content-size field of a
  Zstd frame. The current behavior turns a recoverable data-corruption
  situation into an unhandled runtime exception, which is worse than the
  original corruption.

Confirmed; severity Medium is appropriate. The suggested guard
(`if (rawSize < 0) throw new IllegalStateException(...)`) is the right fix;
the `getUncompressedLength()` site should get the same guard (or be
redefined to throw on bad frames instead of returning a sentinel).

### Finding 3 — CONFIRMED (with one extra site)

Verified exactly as described, with one additional concern:

- `Dict.encode()` (`Dict.java:363-378`):
  - `vLength = ENCODED_HEADER_LENGTH + keyPrefixOrSuffix.length() + dictBytes.length`
    uses Java `String.length()` (char count).
  - `new byte[4 + vLength]` sizes the buffer by char count.
  - `buffer.putShort((short) keyPrefixOrSuffix.length())` writes the char count
    as the on-wire prefix length.
  - `buffer.put(keyPrefixOrSuffix.getBytes())` writes UTF-8 bytes (default
    charset on JDK 21). For any character with code point >= U+0080, the
    UTF-8 byte count exceeds the char count (1 char = 2, 3, or 4 bytes),
    so the `put` overflows the buffer and throws `BufferOverflowException`.
- I confirmed the table: for `U+0080..U+07FF` it's 2 bytes/char; for
  `U+0800..U+FFFF` it's 3 bytes/char. So a prefix like `"用户"` (2 chars,
  6 UTF-8 bytes) or `"café"` (4 chars, 5 UTF-8 bytes) immediately crashes.
- The matching `decode()` (`Dict.java:402-438`) reads back the *char count*
  as a short and allocates `new byte[keyPrefixOrSuffixLength]`, so even if
  encoding somehow succeeded (e.g., a future allocation change), the
  decode would read fewer bytes than were written, corrupting all
  subsequent fields (`dictBytesLength`, `dictBytes`).
- Reachability:
  - `TrainSampleJob.keyPrefixOrSuffixGroup(key)` (`TrainSampleJob.java:203-227`)
    derives a prefix from any key. Keys are constructed via
    `new String(data[i])` (`Request.java:229`) on the default charset
    (UTF-8 on JDK 21). Non-ASCII keys produce non-ASCII prefixes.
  - Operators can also set non-ASCII prefixes via the dynamic config
    `dict_key_prefix_or_suffix_groups` (`TrainSampleJob.java:120`) and via
    the `ManageCommand` `xdict` subcommand. Both feed `putDict()`.
  - `putDict()` is called from automatic training
    (`TrainSampleJob.train()`), manual `ManageCommand`/`XGroup`, and binlog
    replay (`XDict.apply()` → `dictMap.putDict()`), so a non-ASCII prefix
    on any path triggers the bug.
- **Additional site of the same bug**: `Dict.encodeLength(keyPrefix)`
  (`Dict.java:353-355`) returns
  `4 + ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length` —
  char count again. This is used by `ToSlaveExistsDict.encodeToBuf()`
  (`ToSlaveExistsDict.java:68,96,97`) to write the on-wire length prefix
  to the slave, and by DictTest's `encodeLength` test assertion
  (`DictTest.groovy:54`). With a non-ASCII prefix, the slave would read
  fewer bytes than the master wrote, corrupting the dict transfer.
- For symmetry: `XDict.encodeWithType()` (`XDict.java:83-98`) already
  uses `Wal.keyBytes(keyPrefixOrSuffix)` (UTF-8 bytes, `Wal.java:28-30`)
  and is correct; `Dict.encode()` was the outlier.

Confirmed; severity Medium is correct. The suggested fix direction
(use `getBytes(StandardCharsets.UTF_8)` and store the byte count as the
short) is the right approach; the same fix must be applied to
`encodeLength()` and the decode side needs explicit
`new String(prefixBytes, StandardCharsets.UTF_8)` to stay symmetric.
A round-trip test with a non-ASCII prefix is a clean TDD entry point.

### Finding 4 — REFINED (real bug, but severity and reachability slightly narrower)

The mechanism is real and verified:

- `putDict()` (`DictMap.java:101-145`) does three side effects in this
  order: `fos.write(...)` (line 115) → binlog append (lines 120-137) →
  cache update (lines 139-144). If the binlog append throws (it does —
  `firstOneSlot.getBinlog().append(...)` throws `RuntimeException` on
  `IOException`), the file is already written and the cache is not updated.
- The file is opened in append mode (`new FileOutputStream(file, true)`
  in `initDictMap()` at `DictMap.java:249`), so retries append duplicate
  entries.
- On restart, `initDictMap()` (`DictMap.java:242-283`) re-reads every entry
  in the file. `cacheDictBySeq.put(seq, dict)` is keyed by seq, so a
  same-dict retry (same seq, same bytes) is just an overwrite — no
  harm. But a retry that re-trains and gets a new seq produces *two*
  entries with the same key prefix but different seqs. `cacheDict.put(...)`
  is keyed by prefix and the second one wins; the orphaned seq-keyed
  entry stays in `cacheDictBySeq` forever, leaking memory and producing
  duplicate metrics.
- Master-slave divergence: confirmed — the `@SlaveNeedReplay` /
  `@SlaveReplay` annotations on `putDict` (`DictMap.java:99-100`) mean the
  method is replayed on slaves. If the binlog append fails on the master
  *after* the file write, the binlog entry is never created, so the slave
  never calls `XDict.apply()`. The master has the dict in its file, the
  slave has nothing. The master's `getValueBytesByCv()` works; the slave's
  same call would either succeed (if a different dict with a different
  seq is in the slave's `cacheDictBySeq` and happens to be the right one,
  which is unlikely) or throw `DictMissingException` (`BaseCommand.java:723`).
  The user's data is then visible on the master and unreadable on the
  slave. This is a real replication bug.

Two refinements vs. the original finding:

1. **Reachability nuance**: the file-vs-cache inconsistency is real but
   the immediate-session impact is small because the binlog failure throws
   to the caller, who knows the operation failed. The cross-session and
   cross-replica impacts are the bigger concern (silent divergence).
2. **The "retry duplication" scenario only matters when the caller
   actually retries** with a different dict object (e.g., `ManageCommand`
   re-issuing the `xdict` command, or `TrainSampleJob` running another
   training cycle). A same-dict retry produces identical bytes and is
   effectively idempotent at the cache layer. Still, the orphaned
   seq-keyed entry on the second-train path is a real (small) leak.

Refined conclusion: keep the finding, severity Medium stands (the
replication divergence is the most concerning aspect). The author's
Option A (move cache update before binlog, log+swallow binlog failure
so the dict is still usable in the current session) is the right shape,
but Option A needs an additional rule: the binlog failure should be
treated as a *replication* failure (alert on it) because the slave will
be out of sync. A cleaner shape might be: do the cache update first,
then attempt binlog, and if binlog fails, mark the dict as
"not-replicated" with a follow-up repair job, or revert the cache
update and rethrow (current behavior, but with the file write
uncommitted). Either way, the current ordering is the worst of the
three options and should be fixed.

### Finding 5 — CONFIRMED (Low)

Verified exactly as described:

- `cleanUp()` (`DictMap.java:193-207`):
  ```java
  if (fos != null) {
      try {
          fos.close();
          System.out.println("Close dict fos");
          fos = null;            // ONLY reached on success
      } catch (IOException e) {
          System.err.println("Close dict fos error, message=" + e.getMessage());
          // fos is NOT set to null here
      }
  }
  ```
- The asymmetric `try`/`catch` (no `finally`) is the entire bug. On
  `IOException`, `fos` stays non-null with a closed-or-broken underlying
  file descriptor.
- Consequence 1: a second `cleanUp()` call re-enters the `if (fos != null)`
  branch and tries `fos.close()` again. `FileOutputStream.close()` on a
  closed stream is a documented no-op on modern JDKs (it just sets an
  internal flag), so this is mostly harmless, but it also re-prints
  `System.out.println("Close dict fos")` and could re-throw if the
  descriptor state is unusual (NFS/9P). Silent.
- Consequence 2 (the more serious one): a subsequent `putDict()` call
  (e.g., dynamic training happens to fire right after a failed shutdown)
  would hit `fos.write(dict.encode(...))` on the broken stream. On
  modern JDKs, `FileOutputStream.write` after `close()` throws
  `IOException` ("Stream closed") with no useful context, which
  `putDict` rewraps as `RuntimeException("Write dict to file error", e)`.
  The dict never reaches disk, the binlog append is never attempted, and
  the cache is never updated. The user gets an opaque error.
- Consequence 3: the error goes to `System.err` instead of the SLF4J
  logger that the rest of `DictMap` uses (`DictMap.java:70`). This is a
  small consistency bug; production log aggregators will miss the
  message.

Confirmed; severity Low is appropriate. The suggested fix (use a
`finally` block to always null `fos`, and switch `System.err` to the
SLF4J `log` for consistency) is correct and very small.

---

## Reviewer Summary

| # | Finding | Severity | Reviewer Status | Final Status | Notes |
|---|---------|----------|-----------------|--------------|-------|
| 1 | `generateRandomSeq()` can return 1 → silent data corruption | Medium | **Confirmed** | **Confirmed, fix** | Self-contained, real bug, broad impact (BaseCommand + RedisHH) |
| 2 | `decompress()` / `getUncompressedLength()` crash on corrupt frame | Medium | **Confirmed** | **Confirmed, fix** | Affects read path; high reachability via disk corruption |
| 3 | `Dict.encode()` char/byte length mismatch on non-ASCII | Medium | **Confirmed** (with `encodeLength` also affected) | **Confirmed, fix** | Hard crash on realistic input; also breaks `ToSlaveExistsDict` wire length |
| 4 | `putDict()` file-before-binlog leaves system inconsistent | Medium | **Refined** | **Confirmed, fix** | Real bug; replication divergence is the worst aspect |
| 5 | `cleanUp()` does not null `fos` on `IOException` | Low | **Confirmed** | **Confirmed, fix** | Small hardening; `finally` + SLF4J |

Recommended fix priority (for the implementing agent):

1. **Finding 1** — silent data corruption; self-contained; one-line fix
   in `generateRandomSeq()` plus an optional `putDict()` guard.
2. **Finding 3** — hard crash on realistic non-ASCII input; self-contained;
   symmetric encode/decode fix.
3. **Finding 2** — straightforward defensive guard on the `getFrameContentSize`
   return value; affects both `decompress` and `getUncompressedLength`.
4. **Finding 4** — ordering fix; needs care (cache vs. binlog vs.
   replication consequences); recommend Option A from the original
   finding with an extra note that binlog failure is now a
   replication-divergence event that should be alerted.
5. **Finding 5** — minor hardening; trivial fix in a `finally` block.

---

## Author Response to Review (AI agent 1)

### Finding 1 — Agree with CONFIRMED

No dispute. The collision probability is rare (1/10^9 per dict) but
real. The bug breaks on every value compressed by a dict with seq=1
because both decompression entry points (`BaseCommand.java:718` and
`RedisHH.java:336`) short-circuit to `SELF_ZSTD_DICT` on seq=1. The
single-line fix to `generateRandomSeq()` plus a defensive `putDict()`
guard is the right shape.

### Finding 2 — Agree with CONFIRMED

Verified. `Zstd.getFrameContentSize` returns long, the C library
contract is `>= 0` / `-1` (unknown) / `-2` (error), and the `(int)` cast
+ `new byte[...]` produces `NegativeArraySizeException` on `-1`/`-2`.
The suggested guard in `decompress()` and `getUncompressedLength()` is
the right fix.

### Finding 3 — Agree with CONFIRMED (and the additional `encodeLength` site)

Confirmed and thank you for catching the `encodeLength` site — I missed
that on my first pass. The `ToSlaveExistsDict` call site is also
vulnerable; the slave would read the wrong number of bytes. The full
fix must update `encode()`, `encodeLength()`, and `decode()` together
to use UTF-8 byte counts throughout.

### Finding 4 — Agree with REFINED

The reviewer's refinement on replication divergence is the most
important framing — that aspect of the bug is silent and persistent
across master restart, not just a transient in-session inconsistency.
I agree Option A (cache first, then binlog) is the right shape, but
the implementation should mark binlog failures as replication events
(logged at WARN, exposed in a metric) so operators can detect and
repair divergence.

### Finding 5 — Agree with CONFIRMED

Trivial `finally` fix; no dispute. The `System.err` → SLF4J cleanup is
a small consistency improvement that should be done in the same commit.

### Final status

I agree with all five reviewer assessments. The priority order
(1 → 3 → 2 → 4 → 5) is correct. All five are real bugs that should be
fixed.

---

## Resolution

Date: 2026-06-15

All five findings confirmed by both author and reviewer. To be fixed in
priority order: 1, 3, 2, 4, 5. Each fix will be TDD-driven with JaCoCo
coverage confirmation per the project workflow.

---

## Review Feedback: Bug 1 Fix (commit `60d15f6c`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary of the fix

Commit `60d15f6c` implements **Layer 1 only** of the two-layer fix recommended
in the review doc:

- **`Dict.java:461`** — changed the high term from
  `random.nextInt(1000) * 1000 * 1000` to
  `(random.nextInt(1000) + 1) * 1000 * 1000`, making the minimum return value
  `1_000_001` instead of `1`. This guarantees `generateRandomSeq()` never
  returns `SELF_ZSTD_DICT_SEQ` (1).
- **`Dict.java:473-474`** — added `@TestOnly static Random testOnlyRandom`
  field. When set, `generateRandomSeq()` uses it instead of `new Random()`,
  enabling deterministic tests.
- **`DictTest.groovy:42-61`** — new test
  `'test generate random seq never collides with self zstd dict seq'` injects a
  `Random` that always returns 0 (the worst case for the old formula) and
  asserts the result is `!= SELF_ZSTD_DICT_SEQ`.

### Verification

- `./gradlew :test --tests "io.velo.DictTest"` — **PASS**.
- JaCoCo: lines 458–463 fully covered; line 458 ternary both branches covered
  (`testOnlyRandom != null` and `== null`). No uncovered lines or branches.

### Strengths

1. **Correct one-line fix at the source.** `(random.nextInt(1000) + 1)` is the
   minimal change that eliminates the collision. The math is sound: minimum =
   `1 * 1_000_000 + 0 + 0 + 1 = 1_000_001`.
2. **TestOnly mechanism is well-designed.** A swappable `Random` source is
   cleaner than reflection or power-mock. The field is package-private, annotated
   `@TestOnly`, and the test resets it to `null` in its `cleanup` block.
3. **Deterministic test.** The all-zeros `Random` makes the test reproducible —
   it exercises the exact worst case without relying on probabilistic behavior.
4. **Full JaCoCo branch coverage** confirmed for the changed code.

### Concerns

1. **Layer 2 (putDict defense-in-depth guard) is missing.** The review doc
   originally recommended silently regenerating the seq in `putDict()` when
   `dict.getSeq() == SELF_ZSTD_DICT_SEQ`. On reflection, an **assert** is
   better than silent regeneration:

   ```java
   // DictMap.java:101, at the top of putDict()
   assert dict.getSeq() != Dict.SELF_ZSTD_DICT_SEQ
           : "dict seq must not be SELF_ZSTD_DICT_SEQ";
   ```

   Rationale:
   - Layer 1 already prevents `generateRandomSeq()` from producing 1, so the
     only way seq=1 reaches `putDict()` is a **programming error** (direct
     `setSeq(1)`) or **corrupted binlog replay**. Both should be loud, not
     silently masked.
   - Silent regeneration changes the dict's seq without the caller knowing,
     which is surprising and hides the real bug.
   - `assert` fails fast in dev/test (where `-ea` is on), matching the
     codebase style (`CompressedValue.java:600`, `:784`, `:809`).
   - `DictMapTest.groovy:35` still does `dict.seq = 1` and stores it
     successfully — this test would need to switch to a non-reserved seq
     (e.g., `100`) when the assert is added.

2. **Test assertion could be tighter.** The test asserts
   `Dict.generateRandomSeq() != Dict.SELF_ZSTD_DICT_SEQ`. Since the fix
   guarantees the minimum is `1_000_001`, a stronger assertion
   `> Dict.SELF_ZSTD_DICT_SEQ` would be more explicit and catch regressions
   that produce negative seqs (e.g., integer overflow).

3. **`new Random()` still allocated per call** (when `testOnlyRandom` is null).
   The review doc suggested `ThreadLocalRandom`, but the TestOnly swappable-source
   design requires an instance `Random`. This is acceptable —
   `generateRandomSeq()` is called rarely (only on new Dict creation) — but a
   comment explaining why `ThreadLocalRandom` was not used would prevent
   well-meaning future "cleanups."

4. **`DictMapTest.groovy:35` (`dict.seq = 1`) is now a latent foot-gun.** The
   test stores a dict at the reserved seq=1 and asserts
   `dictMap.getDictBySeq(1)` returns it. If the Layer 2 assert is added, this
   test will break. The test should be updated to use a non-reserved seq
   (e.g., `dict.seq = 100`) when the assert lands.

### Pre-commit / post-commit follow-ups

| Priority | Item | Status |
|----------|------|--------|
| Medium | Add Layer 2 assert in `putDict()`: `assert dict.getSeq() != Dict.SELF_ZSTD_DICT_SEQ` | **Post-commit** |
| Low | Tighten test assertion from `!=` to `>` | **Post-commit** |
| Low | Update `DictMapTest.groovy:35` to use a non-reserved seq when assert lands | **Post-commit** |
| Optional | Add a comment explaining why `new Random()` is kept (TestOnly compatibility) | **Post-commit** |

---

## Review Feedback: Bug 2 Fix (commit `3c2c8e3e`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary of the fix

Commit `3c2c8e3e` adds a `rawSize < 0` guard before the `new byte[...]`
allocation in both affected methods:

- **`CompressedValue.java:205-209` (`getUncompressedLength`)** — validates
  `Zstd.getFrameContentSize()` return before casting to int. Throws
  `IllegalStateException` with the raw size on error.
- **`CompressedValue.java:621-625` (`decompress`)** — same guard before
  `new byte[(int) rawSize]`. Prevents `NegativeArraySizeException`.
- **`CompressedValueTest.groovy:448-472`** — new test
  `'test decompress and uncompressed length on corrupted frame'` uses
  `new byte[100]` (all zeros, no valid zstd magic) and asserts both methods
  throw `IllegalStateException` with a message containing "frame content size".
- **`BaseCommandTest.groovy:1063-1071`** — updated the key-analysis test to
  use a real compressed frame (`CompressedValue.compress(rawBytes, null)`)
  instead of mock data, because the new validation would reject the old
  mock's invalid compressed bytes.

### Verification

- `./gradlew :test --tests "io.velo.CompressedValueTest" --tests "io.velo.BaseCommandTest"` — **PASS**.
- JaCoCo `getUncompressedLength` (lines 193–210): all 6 lines covered, both
  branch lines (`isCompressed()`, `rawSize < 0`) fully covered.
- JaCoCo `decompress` (lines 607–630): 6/7 lines covered; `rawSize < 0` both
  branches covered; throw line covered. The only partial is line 614
  (`assert compressedData != null`) — the null branch is expectedly not
  exercised.

### Strengths

1. **Consistent fix in both methods** — same guard pattern, same error message
   format. The reviewer confirmed both call sites are covered.
2. **Descriptive error message** — includes the raw `long` value (`-1` or
   `-2`) so operators can distinguish unknown-size from corrupted frames in
   logs.
3. **Good inline comments** — explains the C library contract (-1 unknown,
   -2 error) and why `NegativeArraySizeException` would otherwise occur.
4. **JavaDoc `@throws`** added to both methods.
5. **Deterministic test** — all-zeros byte array is not a valid zstd frame,
   so `getFrameContentSize` reliably returns -2. No probabilistic behavior.
6. **BaseCommandTest correctly updated** — the old test used mock compressed
   data that would now fail validation. The fix creates a real compressed
   frame so the key-analysis path exercises `getUncompressedLength()` on
   valid data.

### Concerns

1. **Error message says "Decompress error" in `getUncompressedLength()`.**
   No decompression happens there — it only reads the frame header. A more
   specific message like `"Invalid frame content size=" + rawSize` would be
   clearer in that context. Very minor; does not affect correctness.

2. **BaseCommandTest depends on compression succeeding.** The test data
   (`'key-analysis-value-' * 10` = 180 bytes of repetitive ASCII) always
   compresses with self-dict, so `cr.isCompressed()` is true. If the data
   were changed to something incompressible, the test would break because
   `getFrameContentSize` on raw ASCII returns -2. Adding an
   `assert cr.isCompressed()` after the compress call would make the
   dependency explicit. Low priority.

3. **`ZSTD_CONTENTSIZE_UNKNOWN` (-1) case not directly tested.** The
   all-zeros array triggers `-2` (error). The `-1` (unknown) case requires
   a valid zstd frame compressed without the content-size flag, which is
   harder to construct. The `-2` case is the more common corruption
   scenario and the guard handles both identically, so this is acceptable.

### Pre-commit / post-commit follow-ups

| Priority | Item | Status |
|----------|------|--------|
| Low | Use a more specific error message in `getUncompressedLength()` (e.g., "Invalid frame content size" instead of "Decompress error") | **Done** (`cc0e33dc`) |
| Low | Add `assert cr.isCompressed()` in BaseCommandTest to make the compression dependency explicit | **Done** (`cc0e33dc`) |

---

## Review Feedback: Bug 2 Review Follow-up (commit `cc0e33dc`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary

Commit `cc0e33dc` addresses both concerns raised in the Bug 2 review feedback:

1. **Error message clarified** — `getUncompressedLength()` now throws
   `"Invalid frame content size=X"` (dropped the misleading "Decompress error"
   prefix). `decompress()` retains `"Decompress error, invalid frame content
   size=X"` since decompression is actually about to happen there. Each message
   is now contextually accurate.

2. **BaseCommandTest made explicit** — added `assert cr.isCompressed()` with a
   clear comment, and simplified the ternary
   (`cr.isCompressed() ? cr.data() : rawBytes` → `cr.data()`) since the assert
   guarantees the compressed path.

### Verification

- `./gradlew :test --tests "io.velo.CompressedValueTest" --tests "io.velo.BaseCommandTest"` — **PASS**.
- Test assertions check `contains('frame content size')` — matches both the
  updated `getUncompressedLength()` message and the unchanged `decompress()`
  message.
- No new branches; JaCoCo coverage unchanged from the original Bug 2 fix.

### Assessment

Both concerns fully resolved. No new issues. Bug 2 is complete.

---

## Review Feedback: Bug 3 Fix (commit `698dfe35`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary of the fix

Commit `698dfe35` replaces `String.length()` (char count) with
`Wal.keyBytes()` (UTF-8 byte count) consistently across all three affected
methods in `Dict.java`, and updates `decode()` to use `Wal.keyString()` for
symmetric decoding:

- **`Dict.java:356` (`encodeLength`)** — `keyPrefix.length()` →
  `Wal.keyBytes(keyPrefix).length`. This also fixes the wire-format length
  prefix in `ToSlaveExistsDict.encodeToBuf()` (lines 68, 96–97), which calls
  `encodeLength()` to size the dict transfer to slaves.
- **`Dict.java:371-381` (`encode`)** — pre-computes
  `keyPrefixOrSuffixBytes = Wal.keyBytes(keyPrefixOrSuffix)` and uses it for
  both buffer sizing (`vLength`) and writing (`buffer.put`). The stored short
  is now the byte count, matching the actual bytes written.
- **`Dict.java:446` (`decode`)** — `new String(keyPrefixOrSuffixBytes)` →
  `Wal.keyString(keyPrefixOrSuffixBytes)`, ensuring explicit UTF-8 decoding
  symmetric with the encode side.
- **`DictTest.groovy:161-181`** — new test
  `'test encode and decode round trip with non ascii prefix'` uses `'用户'`
  (2 chars, 6 UTF-8 bytes — the worst-case 3:1 ratio) and verifies:
  `encodeLength` matches actual encoded length, no exception, and the
  round-trip `decode()` recovers the original string.

### Verification

- `./gradlew :test --tests "io.velo.DictTest" --tests "io.velo.DictMapTest"` —
  **PASS**.
- JaCoCo `encode()` (lines 353–385): all 13 lines covered.
- JaCoCo `decode()` return (line 446): covered.
- `ToSlaveExistsDict` callers (lines 68, 96–97) automatically benefit from the
  `encodeLength()` fix — no separate change needed.

### Strengths

1. **All three sites fixed consistently** — `encode()`, `encodeLength()`, and
   `decode()` all use `Wal.keyBytes()` / `Wal.keyString()`. The reviewer's
   additional finding (`encodeLength` also broken, breaking `ToSlaveExistsDict`
   wire format) is included.
2. **Delegates to existing helpers** — `Wal.keyBytes()` and `Wal.keyString()`
   (both `StandardCharsets.UTF_8`) are already used by `XDict.encodeWithType()`,
   so the fix aligns `Dict.encode()` with the existing convention rather than
   introducing a new charset pattern.
3. **Good comment** — explains the char/byte mismatch and why CJK characters
   are the worst case (1 char = 3 bytes).
4. **Test uses CJK characters** — `'用户'` gives the maximum 3:1 byte-to-char
   ratio, which is the hardest case for the old buffer-sizing bug.
5. **Round-trip verification** — the test checks `encodeLength ==
   encoded.length` (no overflow) and `decoded.keyPrefixOrSuffix() == keyPrefix`
   (correct recovery), which implicitly proves the byte count is consistent
   end-to-end.

### Concerns

1. **Import dependency** — `Dict.java` now imports `io.velo.persist.Wal`. This
   creates a dependency from the core `io.velo` package to `io.velo.persist`.
   The imported methods are trivial static utilities (getBytes/String with
   UTF_8), so there's no circular dependency risk, but moving `keyBytes` /
   `keyString` to a more neutral location (e.g., a `Charsets` or `Utils` class
   in `io.velo`) would be cleaner architecturally. Very low priority.

2. **Test doesn't assert the explicit byte count.** The round-trip success
   implicitly proves correctness, but an explicit
   `assert keyPrefix.getBytes(StandardCharsets.UTF_8).length == 6` would
   document the expected byte-to-char ratio inline. Minor.

### Pre-commit / post-commit follow-ups

| Priority | Item | Status |
|----------|------|--------|
| Low | Consider moving `Wal.keyBytes` / `Wal.keyString` to a neutral utility class | **Optional** |
| Low | Add explicit byte-count assertion in the non-ASCII test | **Optional** |

---

## Review Feedback: Bug 4 Fix (commit `524b8d7d`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary of the fix

Commit `524b8d7d` reorders the three side effects in `putDict()` from
file-write → binlog → cache to **file-write → cache → binlog**, and changes
binlog failure from rethrow to log-and-swallow:

1. **`DictMap.java:122-127`** — file write stays first; if it fails the dict
   is rejected outright (unchanged).
2. **`DictMap.java:129-135`** — cache update (`TrainSampleJob`,
   `dict.initCtx()`, `cacheDictBySeq.put`, `cacheDict.put`) moved before
   binlog. The dict is now usable in-session regardless of binlog outcome.
3. **`DictMap.java:137-165`** — binlog append moved last. Failure is logged
   at `ERROR` with a descriptive message about master-slave divergence and
   repair options, then **swallowed** (no rethrow).
4. **`DictMap.java:167`** — return value simplified from
   `cacheDict.put(keyPrefixOrSuffix, dict)` (previous value) to `return dict`
   (the stored dict). No caller uses the return value.
5. **`DictMapTest.groovy:149-203`** — new test
   `'test putDict keeps dict in cache when binlog append fails'` installs a
   stub `Binlog` whose `append()` always throws IOException, calls `putDict`,
   and asserts: no exception escapes, dict IS in cache by prefix and by seq,
   `dictSize() == 1`.

### Verification

- `./gradlew :test --tests "io.velo.DictMapTest"` — **PASS**.
- JaCoCo `putDict` (lines 101–168):
  - Cache-update block (lines 132–135): **covered**.
  - Binlog failure path (lines 155, 160–161): **covered**.
  - Binlog success path (line 163): **covered**.
  - `assert dict.getSeq() != SELF_ZSTD_DICT_SEQ` (line 108): both branches
    covered (from bug 1 Layer 2).
  - Uncovered: `throw new RuntimeException("Dict seq conflict")` (line 118,
    double-collision ~10^-12) and `catch (IOException)` from file write
    (lines 125–126, requires mocking fos). Both expected.

### Strengths

1. **Correct reorder eliminates the core inconsistency.** The dict is always
   in the cache after `putDict()` returns, regardless of binlog outcome.
   This directly fixes the "file written, cache not updated" scenario from
   Finding 4.

2. **Binlog failure treated as a replication event, not a put failure.** The
   `log.error(...)` message explicitly states "dict is in master cache but
   not replicated to slave" and mentions repair options (re-train, manual
   xdict sync). This directly addresses the reviewer's refinement that
   binlog failure should be surfaced as a replication-divergence event.

3. **Indirectly mitigates retry duplication.** Since binlog failure is now
   swallowed (not rethrown), the caller sees success and has no reason to
   retry. The original retry-duplication scenario (append-mode file getting
   duplicate entries) is no longer reachable via this path.

4. **Removed dangerous cast.** The old `throw (RuntimeException) e` could
   throw `ClassCastException` if `Promise.getException()` returned a non-
   `RuntimeException`. The new code passes `e` directly to `log.error(...)`
   as the last varargs argument, which SLF4J handles as the throwable —
   type-safe.

5. **Excellent comments.** Step-by-step numbered comments explain the
   rationale for each phase: why file write is first, why cache is before
   binlog, why binlog failure is logged not rethrown.

6. **Clean test design.** The stub `Binlog` subclass with `append()` always
   throwing is a focused way to exercise the failure path without needing
   disk corruption or file-system tricks.

### Concerns

1. **No dedicated metric for binlog append failures.** The `log.error` is
   good for log-based alerting, but a Prometheus counter (e.g.,
   `dict_binlog_append_failures_total`) would be more discoverable in a
   Grafana dashboard. Operators monitoring replication health typically
   watch metrics, not log streams. Low priority — the log message is
   sufficient for now.

2. **Test doesn't verify the dict is on disk.** The test verifies the cache
   state but doesn't reload via `initDictMap()` to confirm the file write
   succeeded. A stronger test would call `cleanUp()` + `initDictMap()` and
   assert the dict is loaded from disk. However, the file write path is
   unchanged from the original code and is covered by other tests, so this
   is a nice-to-have.

3. **`return dict` loses the old-value semantics.** The original code
   returned `cacheDict.put(keyPrefixOrSuffix, dict)` (the previous dict for
   that prefix, or null). No caller uses the return value, so this is safe.
   But the method's JavaDoc should be updated to clarify that it returns the
   stored dict, not the previous one. Very minor.

### Pre-commit / post-commit follow-ups

| Priority | Item | Status |
|----------|------|--------|
| Low | Add a Prometheus counter for dict binlog append failures | **Optional** |
| Low | Test could reload via `initDictMap()` to verify on-disk persistence | **Done** (`0705f166`) |
| Low | Update `putDict` JavaDoc to document the return value semantics | **Done** (`0705f166`) |

---

## Review Feedback: Bug 4 Review Follow-up (commit `0705f166`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary

Commit `0705f166` addresses two of the three optional concerns from the Bug 4
review:

1. **`putDict` JavaDoc clarified** (`DictMap.java:97-100`) — the `@return`
   now explicitly states it returns the stored dict (possibly seq-regenerated),
   NOT the previous dict for that prefix, and explains the "store, don't
   replace" semantics.

2. **Test enhanced with on-disk verification** (`DictMapTest.groovy:201-206`)
   — after verifying the in-memory cache state, the test now calls
   `cleanUp()` + `initDictMap()` to reload from `dict-map.dat` and asserts the
   dict survives the round-trip: `dictSize() == 1`, dict exists by prefix,
   `dictBytes` match. This proves the file write at step 1 of `putDict`
   actually persisted, even though the binlog append failed.

### Verification

- `./gradlew :test --tests "io.velo.DictMapTest"` — **PASS**.
- JaCoCo `putDict`: coverage unchanged — all key paths still covered.

### Assessment

Both concerns fully resolved. The Prometheus counter concern (#1) remains
optional — the `log.error` is sufficient for log-based alerting. Bug 4 is
complete.

---

## Review Feedback: Bug 5 Fix (commit `bd3e096b`)

Reviewer: AI agent (post-commit review)
Review date: 2026-06-15

### Summary of the fix

Commit `bd3e096b` moves `fos = null` from inside the `try` block (only reached
on success) into a `finally` block (always executed):

- **`DictMap.java:220-234`** — `fos = null` is now in `finally`, so a
  `fos.close()` `IOException` no longer leaves `fos` pointing at a broken
  stream. Added a comment explaining (a) why the `finally` is needed, and
  (b) why `System.out`/`System.err` are kept instead of the SLF4J logger
  ("the SLF4J logger may already be shut down at this point in the
  server-stop sequence").
- **`DictMapTest.groovy:206-243`** — new test
  `'test cleanUp nulls fos even when close throws'` uses reflection to
  replace `fos` with a `FileOutputStream` subclass whose `close()` always
  throws `IOException`, calls `cleanUp()`, and asserts `fosField.get(dictMap)
  == null`.

### Verification

- `./gradlew :test --tests "io.velo.DictMapTest" --rerun-tasks` — **PASS**.
- JaCoCo `cleanUp` (lines 220–237): all 7 lines covered, both branch lines
  fully covered:
  - `if (fos != null)`: both true/false branches covered.
  - `fos.close()` success path (line 229): covered.
  - `catch (IOException)` failure path (lines 231): covered.
  - `fos = null` in `finally` (line 233): covered via both paths.

### Strengths

1. **Correct minimal fix.** Moving one line into `finally` is the smallest
   possible change that fixes the bug. The `try/catch/finally` structure is
   idiomatic Java for resource cleanup.

2. **Justifies keeping `System.out`/`System.err`.** The original review doc
   suggested switching to SLF4J. The comment explains why that's wrong here:
   during server shutdown, the logger may already be stopped. This is a valid
   design decision that the reviewer missed.

3. **Deterministic test via reflection.** Injecting a throwing
   `FileOutputStream` subclass through reflection is the cleanest way to test
   an `IOException` on `close()` without file-system tricks or mocking
   frameworks. The assertion `fosField.get(dictMap) == null` directly verifies
   the core invariant.

4. **Full JaCoCo coverage** — both the success and failure paths of
   `cleanUp()` are exercised, including the `finally` block via both paths.

### Concerns

None. This is a clean, self-contained fix with full test coverage.

### Assessment

Bug 5 is complete. All five findings in round 2 are now fixed.

---

## Round 2 Final Status

| # | Finding | Severity | Fix Commit | Review Status |
|---|---------|----------|------------|---------------|
| 1 | `generateRandomSeq()` can return `SELF_ZSTD_DICT_SEQ` | Medium | `60d15f6c` + `c7f3d8ca` | Complete |
| 2 | `decompress()` / `getUncompressedLength()` crash on corrupt frames | Medium | `3c2c8e3e` + `cc0e33dc` | Complete |
| 3 | `Dict.encode()` char/byte length mismatch on non-ASCII | Medium | `698dfe35` | Complete |
| 4 | `putDict()` partial failure ordering | Medium | `524b8d7d` + `0705f166` | Complete |
| 5 | `cleanUp()` doesn't null `fos` on `IOException` | Low | `bd3e096b` | Complete |

All five findings confirmed by both author and reviewer, fixed with TDD,
verified with JaCoCo coverage, and reviewed post-commit. Round 2 is closed.
