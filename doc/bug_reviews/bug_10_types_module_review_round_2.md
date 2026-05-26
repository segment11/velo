# Types Module Bug Review (Round 2)

Date: 2026-04-22
Author: AI agent 1

This round documents new findings in and around the `io.velo.type` module that were not already covered in `bug_10_types_module_review_round_1.md`.

## Bug 1: `ZINTER` / `ZUNION` mutate `TreeSet` keys in place and corrupt sorted order

**Severity:** High

**Files:** `src/main/java/io/velo/type/RedisZSet.java:48-103`, `src/main/java/io/velo/command/ZGroup.java:655-687`, `src/main/java/io/velo/command/ZGroup.java:699-730`, `src/main/java/io/velo/command/ZGroup.java:928-935`

```java
public static class ScoreValue implements Comparable<ScoreValue> {
    private double score;
    ...
    public void score(double score) {
        this.score = score;
    }
}
```

```java
var sv = it.next();
...
sv.score(memberScore + otherMemberScore);
```

```java
for (var sv : rz.getSet()) {
    replies[i++] = new BulkReply(sv.member());
    if (withScores) {
        replies[i++] = new BulkReply(sv.score());
    }
}
```

**Description:** `RedisZSet` stores `ScoreValue` objects inside a `TreeSet`, and `ScoreValue.score(double)` mutates the field used by `compareTo(...)`. `ZGroup.operateZset(...)` updates existing members during `ZINTER` / `ZUNION` by calling `sv.score(...)` in place instead of removing and reinserting the entry. That breaks the `TreeSet` ordering invariant.

**Impact:**
- `ZUNION` / `ZINTER` replies can come back in the wrong score order.
- `ZUNIONSTORE` / `ZINTERSTORE` can persist a wrongly ordered intermediate set.
- Later score-range operations that rely on `rz.getSet()` or `between(...)` can observe inconsistent ordering.

## Bug 2: `RedisHH.encode()` still truncates hash size to `short`

**Severity:** Medium

**Files:** `src/main/java/io/velo/type/RedisHH.java:169-181`

```java
short size = (short) map.size();

var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
buffer.putShort(size);
```

**Description:** Unlike `RedisList`, `RedisHashKeys`, and `RedisZSet`, `RedisHH.encode()` still casts `map.size()` directly to `short` without enforcing `Short.MAX_VALUE`. A large hash therefore serializes a negative or wrapped entry count.

**Impact:**
- Large hashes can be encoded into unreadable bytes.
- A later decode can return the wrong number of fields or fail unexpectedly.
- This is silent data corruption at the type-encoding boundary.

## Bug 3: `RedisGeo.encode()` silently truncates both entry count and member length

**Severity:** High

**Files:** `src/main/java/io/velo/type/RedisGeo.java:381-400`, `src/main/java/io/velo/type/RedisGeo.java:451-456`

```java
short size = (short) map.size();
...
buffer.putShort((short) memberBytes.length);
```

```java
int memberLength = buffer.getShort();
if (memberLength <= 0) {
    throw new IllegalStateException("Invalid member length: " + memberLength + ", expected > 0");
}
```

**Description:** `RedisGeo.encode()` writes both the total entry count and each member name length into signed `short`s, but it never checks either bound first. A geo value with more than `32767` members or with any single member name longer than `32767` bytes will be encoded with wrapped metadata.

**Impact:**
- A single oversized member can be accepted into the in-memory map and then serialized into bytes that `RedisGeo.decode()` rejects later.
- A large enough geo index can wrap its element count and become partially unreadable after persistence.
- This is directly client-reachable because `GEOADD` does not enforce a matching member-length limit before delegating to `RedisGeo`.

## Bug 4: `RedisZSet.decode()` and `iterate()` do not validate member length before allocation

**Severity:** Medium

**Files:** `src/main/java/io/velo/type/RedisZSet.java:511-515`, `src/main/java/io/velo/type/RedisZSet.java:570-574`

```java
int len = buffer.getShort();
double score = buffer.getDouble();
var bytes = new byte[len];
buffer.get(bytes);
```

**Description:** Both decode paths trust the encoded member length completely. Unlike `RedisList.decode()` and `RedisHashKeys.decode()`, there is no `len <= 0` or maximum-length guard before allocating `new byte[len]`.

**Impact:**
- Corrupted zset payloads can throw `NegativeArraySizeException`.
- Oversized lengths can trigger large allocations or a later `BufferUnderflowException`.
- The iterate path is especially risky because it is often used as a lightweight fast path over encoded bytes.

## Bug 5: `RedisBF` accepts non-positive expansion and can fail during growth

**Severity:** High

**Files:** `src/main/java/io/velo/type/RedisBF.java:137-145`, `src/main/java/io/velo/type/RedisBF.java:240-247`, `src/main/java/io/velo/type/RedisBF.java:303-327`, `src/main/java/io/velo/command/BGroup.java:568-580`, `src/main/java/io/velo/command/BGroup.java:726-737`

```java
public RedisBF(int initCapacity, double initFpp, byte initExpansion, boolean nonScaling) {
    this.fpp = initFpp;
    this.expansion = initExpansion;
    ...
}
```

```java
if (lastOne.capacity > Integer.MAX_VALUE / expansion) {
    throw new RuntimeException("BF capacity overflow");
}
var newOneCapacity = lastOne.capacity * expansion;
```

```java
if (initExpansion > RedisBF.MAX_EXPANSION) {
    return new ErrorReply("expansion too large");
}
```

**Description:** `RedisBF` assumes `expansion` is positive, but neither the type constructor nor `decode()` validates that invariant. The command layer only rejects values above `MAX_EXPANSION`; it still accepts `0` and negative values. When a full filter tries to expand, `Integer.MAX_VALUE / expansion` can divide by zero or produce an invalid negative capacity.

**Impact:**
- `BF.RESERVE key 0.01 100 EXPANSION 0` is accepted and can later fail once the filter fills up.
- Negative expansion can propagate invalid metadata into new sub-filters.
- `BF.LOADCHUNK` can also hydrate malformed Bloom metadata because `RedisBF.decode()` trusts the stored expansion byte.

## Notes

- This file is an authoring pass only. I did not run the second-agent verification workflow in this turn.
- I did not implement fixes in this round; this document is for bug discovery only.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `a9a7655` `fix: ZINTER/ZUNION mutate TreeSet keys in place corrupting sorted order`

### Summary of the Fix

The patch moves `ZINTER` and `ZUNION` away from mutating `RedisZSet.ScoreValue` instances in place while they are still stored inside a `TreeSet`. The current version also avoids the earlier `ZUNION` double-aggregation regression by only recording score updates for members that already existed before the current `otherRz` pass, and the new tests now assert both order and scores.

### Strengths

- The fix directly addresses the root cause in the original bug report: `TreeSet` keys were being mutated after insertion.
- The new tests exercise both `zunionstore` and `zinterstore`, which is the correct command surface for this bug.
- The updated `zunionstore` test now asserts exact scores as well as member order, which closes the earlier regression-test gap.
- The code keeps the `memberMap` and `set` structures synchronized during reinsertion, which is the critical invariant to preserve.

### Concerns

- No correctness findings from the current review pass.
- Residual coverage gap: JaCoCo still shows partial branch coverage for `if (memberMap.isEmpty())` in `src/main/java/io/velo/command/ZGroup.java:665` and for the `!sv.isAlreadyWeighted` false branch in `src/main/java/io/velo/command/ZGroup.java:673` / `727`. I do not see a bug there from static review, but those paths are not covered by the current regression tests.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.ZGroupTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the changed reinsertion logic in `build/reports/jacocoHtml/io.velo.command/ZGroup.java.html`, including the updated blocks around `src/main/java/io/velo/command/ZGroup.java:657-699` and `712-755`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `67d2d38` `fix: RedisHH.encode() guard hash size against Short.MAX_VALUE overflow`

### Summary of the Fix

The patch adds an explicit `map.size() > Short.MAX_VALUE` guard in `RedisHH.encode()` before the size is cast to `short` and before the encoded bytes are passed into the compression path. That is the correct place to enforce the limit because it blocks the original silent-wrap corruption path at the serialization boundary.

### Strengths

- The production change is minimal and directly targets the reported bug.
- The guard is placed before both `buffer.putShort(...)` and `compressIfBytesLengthIsLong(...)`, so both raw and compressed encodings now share the same protection.
- The new regression test reaches the real overflow branch and asserts the expected exception text.

### Concerns

- No correctness findings from the current review pass.
- The test uses reflection to force the internal map size past `Short.MAX_VALUE`. That is acceptable here, but it does mean the coverage comes from forced internal state rather than a public entry path.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisHHTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of the new size guard in `build/reports/jacocoHtml/io.velo.type/RedisHH.java.html`, including the covered branch at `src/main/java/io/velo/type/RedisHH.java:179` and throw at line 180.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `130a989` `fix: RedisGeo.encode() guard entry count and member length against Short.MAX_VALUE`

### Summary of the Fix

The patch adds explicit guards in `RedisGeo.encode()` for both overflow cases identified in the review: total entry count above `Short.MAX_VALUE` and encoded member length above `Short.MAX_VALUE`. Both checks happen before the values are cast to `short`, which is the correct place to block the original silent-wrap bug.

### Strengths

- The production change is small and directly addresses both confirmed overflow paths.
- The count guard is applied before `buffer.putShort((short) size)`, and the member-length guard is applied before `buffer.putShort((short) memberBytes.length)`, so the broken encoding path is cut off cleanly.
- The new tests cover both overflow cases separately, which keeps intent clear and avoids one oversized case masking the other.

### Concerns

- No correctness findings from the current review pass.
- The entry-count test uses reflection to force the internal map past `Short.MAX_VALUE`. That is acceptable here for a serialization-boundary guard, but it is still forced internal state rather than a pure public-entry-path test.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisGeoTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of both new guards in `build/reports/jacocoHtml/io.velo.type/RedisGeo.java.html`, including the covered size guard at `src/main/java/io/velo/type/RedisGeo.java:390-391` and the covered member-length guard at `src/main/java/io/velo/type/RedisGeo.java:402-403`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `c8e35cd` `fix: RedisZSet decode/iterate validate member length before allocation`

### Summary of the Fix

The patch adds `len <= 0` validation to both `RedisZSet.decode()` and `RedisZSet.iterate()` before `new byte[len]`. That closes the negative-length and zero-length crash path, but it does not fully address the malformed-length bug described in the review.

### Strengths

- The production change is small and applied consistently to both decode paths.
- The new tests correctly prove that negative member lengths are now rejected in both `decode()` and `iterate()`.

### Concerns

- **The fix is only partial.** In `src/main/java/io/velo/type/RedisZSet.java:512-518` and `573-579`, the new validation only rejects `len <= 0`. A corrupted payload with a large positive length still reaches `new byte[len]` and `buffer.get(bytes)`, which can trigger oversized allocation or `BufferUnderflowException`. The original bug report for bug 4 covered both malformed negative lengths and oversized positive lengths, so this commit does not fully close the issue.
- **The new tests only cover `-1`, not oversized positive lengths.** The tests added at `src/test/groovy/io/velo/type/RedisZSetTest.groovy:238-256` mutate the encoded member length to `-1`, which is useful, but they do not prove behavior for large positive corrupted lengths.

### Verification

- Static review only for this commit.
- I did not run tests for this review pass.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `2414d71` `fix: RedisZSet decode/iterate validate member length before allocation`

### Summary of the Fix

This updated patch completes the earlier partial fix by adding a remaining-buffer guard in both `RedisZSet.decode()` and `RedisZSet.iterate()`. With the new `len + 8 > buffer.remaining()` validation in place, both malformed negative lengths and oversized positive lengths are rejected before allocation and before reading the score/member payload.

### Strengths

- The production fix now covers both malformed-length cases that mattered for bug 4.
- The new oversized-length tests complement the earlier negative-length tests, so both decode paths are now exercised for both classes of bad input.
- I ran the targeted test class and JaCoCo confirms execution of the new remaining-buffer guards.

### Concerns

- **The commit is not scoped to bug 4 only.** Although the commit message refers to `RedisZSet`, the commit also includes unrelated Bloom-filter files: `src/main/java/io/velo/command/BGroup.java` and `src/test/groovy/io/velo/command/BGroupTest.groovy`. That conflicts with the repo workflow requiring one bug fix per commit and makes review/rollback less clean than it should be.
- No correctness findings in the `RedisZSet` fix itself from the current review pass.

### Verification

- Ran `./gradlew :test --tests "io.velo.type.RedisZSetTest" --rerun-tasks` successfully.
- JaCoCo confirms execution of both new remaining-buffer guards in `build/reports/jacocoHtml/io.velo.type/RedisZSet.java.html`, including the covered checks at `src/main/java/io/velo/type/RedisZSet.java:516-517` and `581-582`.

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-04-22
Commit reviewed:
- `d8ac6e6` `fix: RedisBF reject non-positive expansion in bf.reserve and bf.insert`

### Summary of the Fix

The patch adds `initExpansion <= 0` validation in the `BF.RESERVE` and `BF.INSERT` command parsers. That correctly closes the direct command-input path for non-positive expansion values, but it does not fully close the bug described in the review because the type-level invariant is still unenforced.

### Strengths

- The command-layer change is small and directly fixes the obvious client-input path.
- The new tests cover both `BF.RESERVE` and `BF.INSERT` with `0` and `-1`, which is the right surface for the parser-level part of the bug.

### Concerns

- **The fix is only partial.** `RedisBF` itself still accepts non-positive expansion in both the constructor and `decode()`: see `src/main/java/io/velo/type/RedisBF.java:137-145` and `303-327`. That means malformed Bloom filter bytes can still be loaded via `BF.LOADCHUNK` and later reach the expansion path at `src/main/java/io/velo/type/RedisBF.java:240-244`, where `Integer.MAX_VALUE / expansion` still assumes a positive value.
- **The new tests do not cover the decoded-metadata path.** The tests added in `src/test/groovy/io/velo/command/BGroupTest.groovy:831-867` prove parser rejection only. They do not verify behavior for `RedisBF.decode()` or `BF.LOADCHUNK`, both of which were part of the original bug report.

### Verification

- Static review only for this commit.
- I did not run tests for this review pass.
