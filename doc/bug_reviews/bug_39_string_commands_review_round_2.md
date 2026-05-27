# Bug Review: String Commands Module - Round 2

**Review date**: 2026-05-26
**Role**: AI agent 1 (author)
**Status**: Initial round 2 findings, pending AI agent 2 verification

## Redis String Command Coverage Source

Fetched Redis string command documentation from the official Redis docs page requested by the user (`https://redis.io/docs/latest/commands//?group=string`) and the linked command pages. The string command group currently includes:

`APPEND`, `DECR`, `DECRBY`, `DECRBYFLOAT`, `DELEX`, `DIGEST`, `GET`, `GETBIT`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `INCREX`, `LCS`, `MGET`, `MSET`, `MSETEX`, `MSETNX`, `PSETEX`, `SET`, `SETBIT`, `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`, `SUBSTR`.

Newer Redis commands such as `DELEX`, `DIGEST`, `INCREX`, and `MSETEX` are not treated as bugs in this round because Velo may intentionally target an older Redis compatibility surface. This pass focuses on implemented commands whose behavior diverges from Redis semantics.

---

## Bug 5 - SET option parser loses earlier options when later options are present

**Severity**: High - Conditional writes can be bypassed and overwrite existing keys

**Files**:
- `src/main/java/io/velo/command/SGroup.java` lines 521-620

**Code excerpt**:

```java
boolean isNx = false;
boolean isXx = false;
boolean isKeepTtl = false;
boolean isReturnExist = false;
for (int i = 3; i < dd.length; i++) {
    var arg = new String(dd[i]);
    isNx = "nx".equalsIgnoreCase(arg);
    isXx = "xx".equalsIgnoreCase(arg);
    if (isNx || isXx) {
        continue;
    }

    isKeepTtl = "keepttl".equalsIgnoreCase(arg);
    if (isKeepTtl) {
        continue;
    }

    isReturnExist = "get".equalsIgnoreCase(arg);
    if (isReturnExist) {
        continue;
    }
    // ...
}
```

**Root cause**: The parser assigns option booleans from the current token instead of accumulating them. Any later option resets earlier booleans to `false`. For example, parsing `SET k v NX GET` sets `isNx = true` on `NX`, then resets `isNx = false` when it reaches `GET`.

**Impact**: Redis 7.0+ explicitly allows `NX`/`XX` with `GET`. In Velo, `SET k new NX GET` against an existing key will not honor `NX`; it will overwrite the key and return the old value. The same reset pattern can also break combinations such as `XX GET` and `KEEPTTL GET`.

**Fix direction**: Change option parsing to accumulate flags, e.g. `isNx = isNx || ...`, or preferably use a `switch` that sets each flag once and rejects invalid duplicate/conflicting options.

---

## Bug 6 - SET silently accepts unknown option tokens

**Severity**: Medium - Redis protocol compatibility violation and typo-prone writes

**Files**:
- `src/main/java/io/velo/command/SGroup.java` lines 548-556
- `src/test/groovy/io/velo/command/SGroupTest.groovy` lines 524-528

**Code excerpt**:

```java
boolean isEx = "ex".equalsIgnoreCase(arg);
boolean isPx = "px".equalsIgnoreCase(arg);
boolean isExAt = "exat".equalsIgnoreCase(arg);
boolean isPxAt = "pxat".equalsIgnoreCase(arg);

isExpireAtSet = isEx || isPx || isExAt || isPxAt;
if (!isExpireAtSet) {
    continue;
}
```

Existing test currently codifies this behavior:

```groovy
// skip syntax check
reply = sGroup.execute('set a value zz')
then:
reply == OKReply.INSTANCE
```

**Root cause**: Unknown SET option tokens fall through to `continue` instead of returning `ErrorReply.SYNTAX`. This means `SET a value zz` is accepted as a normal SET.

**Impact**: Clients that pass misspelled or unsupported options get a successful write instead of a syntax error. This is especially risky for condition/expiration options because a typo can turn an intended guarded or expiring write into an unconditional persistent write.

**Fix direction**: Return `ErrorReply.SYNTAX` for any unrecognized option token, and update tests to expect a syntax error for unknown options.

---

## Bug 7 - SET accepts invalid non-positive expiration values

**Severity**: Medium - Expiration semantics diverge from Redis and can produce persistent writes from invalid input

**Files**:
- `src/main/java/io/velo/command/SGroup.java` lines 558-591
- `src/test/groovy/io/velo/command/SGroupTest.groovy` lines 512-517

**Code excerpt**:

```java
long value;
try {
    value = Long.parseLong(new String(dd[i + 1]));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}
if (isEx) {
    ex = value;
} else if (isPx) {
    px = value;
} else if (isExAt) {
    exAt = value;
} else {
    pxAt = value;
}
```

Existing test currently expects `PXAT -1` to become no-expire:

```groovy
reply = sGroup.execute('set a value pxat -1')
then:
reply == OKReply.INSTANCE
inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
        .cv().expireAt == CompressedValue.NO_EXPIRE
```

**Root cause**: `SET` parses expiration integers but never validates the Redis requirement that relative expiration values must be positive, and it also accepts invalid absolute timestamps such as `PXAT -1`. Because Velo uses `CompressedValue.NO_EXPIRE == -1`, `PXAT -1` is converted into a persistent value.

**Impact**: Invalid expiration requests that Redis would reject can succeed in Velo. The worst case is `PXAT -1`, which turns an invalid expiration into a persistent key.

**Fix direction**: Mirror the validation already present in `GETEX`: reject invalid expiration values with `ErrorReply.INVALID_INTEGER` before writing. Relative `EX`/`PX` should reject values <= 0; absolute `EXAT`/`PXAT` should reject negative values, and preferably follow Redis behavior for zero/past timestamps.

---

## Bug 8 - GETRANGE returns nil for a missing key instead of an empty bulk string

**Severity**: Low - Redis protocol compatibility mismatch

**Files**:
- `src/main/java/io/velo/command/GGroup.java` lines 263-267
- `src/test/groovy/io/velo/command/GGroupTest.groovy` lines 260-264

**Code excerpt**:

```java
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var valueBytes = get(slotWithKeyHash);
if (valueBytes == null) {
    return NilReply.INSTANCE;
}
```

Existing test currently expects nil:

```groovy
def reply = gGroup.execute('getrange a 0 1')
then:
reply == NilReply.INSTANCE
```

**Root cause**: Velo treats a missing key as a nil bulk reply for `GETRANGE`. Redis string range operations treat a missing key as an empty string and return an empty bulk string.

**Impact**: Clients see a null bulk reply where Redis-compatible clients expect an empty string. This can break client code that distinguishes nil from empty byte arrays.

**Fix direction**: Return the existing `BLANK_REPLY` for missing keys in `GGroup.getrange()`.

---

## AI Agent 2 Review Notes

**Review date**: 2026-05-26
**Role**: AI agent 2 (reviewer)
**Status**: All bugs verified against source code

### Bug 5 - SET option parser loses earlier options when later options are present

**Verdict**: CONFIRMED

**Evidence**: Traced through `SET k v NX GET` on `SGroup.java` lines 530-546:
1. Iteration `i=3` ("NX"): `isNx = true`, `isXx = false` → `isNx || isXx` true → continue
2. Iteration `i=4` ("GET"): `isNx = "nx".equalsIgnoreCase("get")` → **reset to `false`**, `isXx = false` → falls through; `isReturnExist = true` → continue

After the loop: `isNx = false`, `isReturnExist = true`. At line 600, `if (isNx && isOldExist)` is false, so the NX guard is bypassed. The key is overwritten and old value returned.

The same reset pattern affects all option combinations: `XX GET` (isXx lost), `KEEPTTL GET` (isKeepTtl lost at line 538), and even `NX XX` (NX lost when XX is parsed last).

The severity assessment of High is appropriate: conditional write guarantees are broken.

### Bug 6 - SET silently accepts unknown option tokens

**Verdict**: CONFIRMED

**Evidence**: At `SGroup.java` lines 548-556, if a token doesn't match any known option (`nx`, `xx`, `keepttl`, `get`, `ex`, `px`, `exat`, `pxat`), `isExpireAtSet` is `false` and the `continue` at line 555 silently skips it. The test at `SGroupTest.groovy` line 526 (`set a value zz` → `OKReply.INSTANCE`) confirms this behavior exists and is currently expected.

Redis would return `ERR syntax error` for any unrecognized option. The fix direction is correct.

### Bug 7 - SET accepts invalid non-positive expiration values

**Verdict**: CONFIRMED, with a correction to the root cause explanation

**Correction**: The original description states "Because Velo uses `CompressedValue.NO_EXPIRE == -1`". This is factually incorrect. `CompressedValue.NO_EXPIRE` is `0` (verified at `CompressedValue.java` line 20). The actual mechanism is a **sentinel value clash**: `ex/px/exAt/pxAt` are initialized to `-1` as "not set" sentinels (lines 523-526). The expireAt computation (lines 582-591) checks `!= -1` to determine which was set. When the user passes `-1` as a value (e.g., `EX -1` or `PXAT -1`), the parsed value equals the sentinel, so the condition fails and `expireAt` stays at `NO_EXPIRE` (0), making the key persistent.

Additional cases not mentioned in original:
- `SET a value EX -1`: ex = -1, `ex != -1` false → persistent key (Redis rejects)
- `SET a value EX 0`: ex = 0, `ex != -1` true → `expireAt = now + 0` → immediate expire (Redis rejects)
- `SET a value PXAT 0`: pxAt = 0, `pxAt != -1` true → `expireAt = 0` = NO_EXPIRE → persistent (Redis rejects)

The existing GETEX implementation (`GGroup.java` lines 198-199) already validates `x < 0` with `ErrorReply.INVALID_INTEGER`, which is the correct pattern to follow.

### Bug 8 - GETRANGE returns nil for a missing key instead of an empty bulk string

**Verdict**: CONFIRMED

**Evidence**: At `GGroup.java` lines 265-267, when `get(slotWithKeyHash)` returns null, the code returns `NilReply.INSTANCE`. Redis `GETRANGE` on a non-existent key returns an empty bulk string `""`, not nil. The constant `BLANK_REPLY` already exists at line 247 (`new BulkReply(new byte[0])`) and is used for the invalid range case at line 271. The fix is straightforward: return `BLANK_REPLY` instead of `NilReply.INSTANCE` at line 266.

The severity of Low is appropriate; this only affects clients that distinguish nil from empty string.

---

## Round 2 Summary

| Bug | Command | File | Severity | Status |
|-----|---------|------|----------|--------|
| 5 | SET | SGroup.java | **High** | Confirmed |
| 6 | SET | SGroup.java / SGroupTest.groovy | **Medium** | Confirmed |
| 7 | SET | SGroup.java / SGroupTest.groovy | **Medium** | Confirmed (root cause corrected) |
| 8 | GETRANGE | GGroup.java / GGroupTest.groovy | **Low** | Confirmed |

---

## Review Feedback (AI agent 2 Post-Commit Review - Bug 5)

**Reviewer**: AI agent 2
**Date**: 2026-05-26
**Reviewed commit**: `3f89056d` - "fix: preserve SET option flags when combined with GET"

### Summary

The commit fixes the specific parser-state bug from Bug 5: `NX`, `XX`, `KEEPTTL`, and `GET` are now accumulated independently instead of being reset on each option token. This prevents `NX GET`, `XX GET`, and `KEEPTTL GET` from losing earlier flags during parsing.

### Finding

**Severity**: Medium

`SET key value NX GET` on an existing key now preserves the `NX` flag and avoids overwriting, but it returns `NilReply.INSTANCE`. Redis `SET` with `GET` returns the previous value of the key whether the key was set or not. The official Redis docs state that when `GET` is specified, a bulk string reply is the previous key value "whether the key was set or not". Therefore, the fixed code still diverges from Redis semantics for the exact Bug 5 scenario: it should not write, but it should return the old value.

**Files**:
- `src/main/java/io/velo/command/SGroup.java` lines 598-603
- `src/test/groovy/io/velo/command/SGroupTest.groovy` lines 561-566

**Current code path**:

```java
boolean isOldExist = cv != null && !cv.isExpired();
if (isNx && isOldExist) {
    return NilReply.INSTANCE;
}
```

**Current test expectation**:

```groovy
reply = sGroup.execute('set a newvalue nx get')
then:
reply == NilReply.INSTANCE
```

The test should assert a `BulkReply` with `oldvalue` while also asserting the stored value remains unchanged.

### Verification

Ran focused verification:

```bash
./gradlew :cleanTest :test --tests "io.velo.command.SGroupTest.test set nx get and xx get preserve flags"
```

Result: build successful, focused test passed.

JaCoCo HTML confirms the changed parser lines are covered: `SGroup.java` lines 533-547 are fully covered. The early `NX && isOldExist` branch at line 602 is also covered, but it currently covers the incorrect nil-return behavior.

### Follow-up

Adjust `SGroup.set(...)` so conditional failure with `GET` returns the previous string value when the key existed, while still skipping the write. Keep nil for cases where the previous key did not exist. Then update the regression test for `NX GET` on an existing key to expect the old value.

---

## Review Feedback Resolution (AI agent 1 - Bug 5 follow-up)

**Date**: 2026-05-27
**Commit**: follow-up to `3f89056d`

### Changes

1. **SGroup.java** lines 602-610: When `NX` prevents a write and `GET` is specified, the code now falls through to return the old string value (per Redis semantics: "Bulk string reply: The previous value of the key, whether the key was set or not"). The `XX && !isOldExist` case correctly returns nil since the key didn't exist before.

2. **SGroupTest.groovy**: Updated `NX GET` test to expect `BulkReply` with `oldvalue` instead of `NilReply.INSTANCE`. Added coverage for `NX GET` on a non-string key type.

### JaCoCo verification

All changed lines (600-631) are fully covered (fc).
