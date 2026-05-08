# Bug 13 Persist Layer Read Data Flow Review

## Scope

Static review of the persist-layer read data flow, focused on key lookup and key iteration paths:

- `BaseCommand.getCv(...)`
- `OneSlot.get(...)`
- `KeyLoader.getValueXByKey(...)`
- `Wal.scan(...)`
- `KeyLoader.scan(...)`

Point lookup follows the expected newest-first order: WAL, slot-local KV LRU, persisted key bucket metadata, then chunk or
big-string storage. The confirmed issues below are in the SCAN/read-iteration path over persisted key buckets.

## Finding A: Persisted key-bucket SCAN cursor does not count post-scan-start entries

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/KeyLoader.java:506-564`
- `src/main/java/io/velo/persist/Wal.java:488-500`

**Code excerpt:**

```java
// KeyLoader.readKeysToList(...)
if (seq > beginScanSeq) {
    return;
}

if (countArray[0] <= 0) {
    return;
}

addedKeyCount[0]++;
keys.add(key);
countArray[0]--;
...
var nextTimeSkipCount = skipCount + expiredOrNotMatchedCount[0] + addedKeyCount[0];
return new ScanCursor(slot, walGroupIndex, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END,
        (short) nextTimeSkipCount, splitIndex);
```

**Root cause:**

`SGroup.scan()` stores `beginScanSeq` when cursor `0` starts. Persisted key-bucket entries with `seq > beginScanSeq`
are correctly excluded from this scan snapshot, but `KeyLoader.readKeysToList(...)` does not count those skipped entries
when computing the next cursor's `keyBucketsSkipCount`.

The next SCAN call applies `skipCount` before all filters:

```java
if (tmpSkipCount[0] > 0) {
    tmpSkipCount[0]--;
    return;
}
```

Because post-scan-start entries were not counted, the next cursor can skip a different physical entry than intended.
Example:

1. Key-bucket iteration order is `[newer-than-beginScanSeq, old-key-1, old-key-2]`.
2. First SCAN with `COUNT 1` ignores the new entry, returns `old-key-1`, and stores `skipCount = 1`.
3. Next SCAN skips the new entry as the one counted item, then returns `old-key-1` again.

`Wal.scan(...)` already handles the same case correctly by incrementing `otherSkippedCount` for `v.seq > beginScanSeq`
and including it in `nextTimeSkipCount`. `KeyLoader.scan(...)` should use the same cursor accounting rule.

**Impact:**

Persisted-key SCAN can duplicate keys or skip keys across cursor calls when writes happen after a scan begins but before
or during persisted key-bucket iteration. This affects the read data flow exposed to Redis clients through `SCAN`.

## Finding B: Persisted key-bucket SCAN only skips WAL-shadowed keys for the first WAL group

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/SGroup.java:373-408`
- `src/main/java/io/velo/persist/KeyLoader.java:589-617`

**Code excerpt:**

```java
// KeyLoader.scan(...)
final var inWalKeys = oneSlot.getWalByGroupIndex(walGroupIndex).inWalKeysFormScan(beginScanSeq);
...
for (int j = walGroupIndex; j < walGroupNumber; j++) {
    for (int i = 0; i < maxSplitNumber; i++) {
        ...
        var scanCursor = readKeysToList(keys, j, (byte) i, skipCountInThisWalGroupThisSplitIndex,
                typeAsByte, matchPattern, countArray, beginScanSeq, inWalKeys);
```

**Root cause:**

`SGroup.scan()` scans WAL groups first, then scans persisted key buckets. Persisted key-bucket scanning must skip keys
that are already represented in WAL, because WAL contains the newest value or delete tombstone for that key.

`KeyLoader.scan(...)` builds `inWalKeys` once from the starting `walGroupIndex`, then reuses that same set while the loop
advances through all later WAL groups. That is only correct for the first WAL group being scanned. For `j > walGroupIndex`,
`readKeysToList(...)` still checks the original group's WAL key set, not `oneSlot.getWalByGroupIndex(j)`.

**Impact:**

When SCAN reaches persisted buckets for WAL group `j > walGroupIndex`, a key with a newer WAL value or delete tombstone
in group `j` is not skipped. The client can receive both the WAL version and the stale persisted key, or receive a key
that should be hidden by a WAL delete. This makes SCAN disagree with point reads (`GET`/`EXISTS`), which check WAL first.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| A - Persisted SCAN cursor ignores post-scan-start entries | High | Fixed and reviewed | High |
| B - Persisted SCAN skips WAL-shadowed keys only for first WAL group | High | Fixed, reviewed with test follow-up | High |

## Fix - Bug A

**Commit:** `ecce5b0` `fix: count post-scan-start entries in key bucket scan cursor`

**Files changed:**
- `src/main/java/io/velo/persist/KeyLoader.java` (lines 506-566)
- `src/test/groovy/io/velo/persist/KeyLoaderTest.groovy`

**Fix description:**

Added `postScanStartSkippedCount` counter in `KeyLoader.readKeysToList()` to track entries skipped due to `seq > beginScanSeq`:

```java
final short[] postScanStartSkippedCount = {0};
...
// skip data that is new added after time do scan
if (seq > beginScanSeq) {
    postScanStartSkippedCount[0]++;  // NEW: count the skipped entry
    return;
}
...
var nextTimeSkipCount = skipCount + expiredOrNotMatchedCount[0] + postScanStartSkippedCount[0] + addedKeyCount[0];
```

This mirrors the pattern used in `Wal.scan()` where `otherSkippedCount` tracks entries skipped due to `seq > beginScanSeq`.

**Verification:**
- All `KeyLoaderTest` tests pass
- JaCoCo coverage shows `postScanStartSkippedCount[0]++` and `nextTimeSkipCount` calculation are executed
- Coverage report: `build/reports/jacocoHtml/io.velo.persist/KeyLoader.java.html`

## Review Feedback - Bug A Fix Commit `ecce5b0`

Reviewed by: AI agent 2
Date: 2026-04-28

### Summary of the Fix

The commit updates `KeyLoader.readKeysToList(...)` to track persisted key-bucket entries skipped because
`seq > beginScanSeq` and include that count in the returned `keyBucketsSkipCount`. This makes the persisted key-bucket
cursor accounting match the existing `Wal.scan(...)` behavior for entries outside the scan snapshot.

### Strengths

- The production change targets the confirmed root cause directly.
- The counter is local to one key bucket, matching the existing `addedKeyCount` and `expiredOrNotMatchedCount` cursor
  accounting in the same method.
- The focused regression test now creates entries with `seq > beginScanSeq` and asserts the returned
  `keyBucketsSkipCount` includes both post-scan-start skipped entries and returned entries.
- The focused `KeyLoaderTest` method passed, and JaCoCo confirms the changed production lines were executed:
  `postScanStartSkippedCount[0]++` and the updated `nextTimeSkipCount` calculation are covered in
  `build/reports/jacocoHtml/io.velo.persist/KeyLoader.java.html`.

### Findings

No blocking issues found in the updated Bug A fix commit.

### Residual Test Note

The focused test proves the cursor count contract directly. A future hardening improvement could also run the second
scan from the returned cursor and assert that previously returned keys are not duplicated, but this is not required to
accept the current fix.

## Suggested Fix Direction

Finding A should mirror `Wal.scan(...)`: track entries skipped because `seq > beginScanSeq` and include that count in the
next `keyBucketsSkipCount`.

Finding B should compute WAL-shadowed keys per WAL group, either by moving `inWalKeysFormScan(beginScanSeq)` inside the
outer `for (int j = ...)` loop or by passing the WAL group index into `readKeysToList(...)` and resolving the set there.

Both fixes should be covered by focused Spock tests in `KeyLoaderTest.groovy` or an integration-style `SGroup`/`OneSlot`
test that exercises multi-call SCAN cursor behavior.

## Fix - Bug B

**Commit:** `ffd960e` `fix: compute inWalKeys per WAL group in KeyLoader.scan()`

**Files changed:**
- `src/main/java/io/velo/persist/KeyLoader.java` (line 610)

**Fix description:**

Moved `inWalKeys` computation inside the outer `for (int j = ...)` loop so each WAL group uses its own WAL key set:

```java
for (int j = walGroupIndex; j < walGroupNumber; j++) {
    final var inWalKeys = oneSlot.getWalByGroupIndex(j).inWalKeysFormScan(beginScanSeq);
    for (int i = 0; i < maxSplitNumber; i++) {
        ...
        var scanCursor = readKeysToList(keys, j, (byte) i, skipCountInThisWalGroupThisSplitIndex,
                typeAsByte, matchPattern, countArray, beginScanSeq, inWalKeys);
```

**Verification:**
- All `KeyLoaderTest` tests pass
- JaCoCo coverage shows `inWalKeysFormScan` is called inside the loop

## Review Feedback - Bug B Fix Commit `ffd960e`

Reviewed by: AI agent 2
Date: 2026-04-28

### Summary of the Fix

The commit moves `inWalKeysFormScan(beginScanSeq)` from the start of `KeyLoader.scan(...)` into the outer WAL-group
loop. Each persisted WAL-group scan now receives the shadow-key set for the same WAL group `j` that
`readKeysToList(...)` is scanning.

### Strengths

- The production change is minimal and targets the confirmed root cause directly.
- The new placement preserves one `inWalKeys` set per WAL group and reuses it across split indexes in that group, which
  is the right granularity for the current key-bucket scan loop.
- `./gradlew :cleanTest :test --tests "io.velo.persist.KeyLoaderTest"` passed.
- JaCoCo confirms the moved line is executed in `KeyLoader.scan(...)`: line 610 in
  `build/reports/jacocoHtml/io.velo.persist/KeyLoader.java.html` is covered.

### Findings

No production-code issue found in the Bug B fix.

### Test Gap

The commit does not add a focused regression test for Bug B. Existing `KeyLoaderTest` coverage executes the moved line,
but it does not prove the actual stale-key scenario:

- a persisted key-bucket entry in WAL group `j > 0`;
- a WAL value or delete tombstone for the same key in that same later group;
- `KeyLoader.scan(0, ...)` should skip the persisted stale key because it now computes `inWalKeys` for group `j`.

Recommended follow-up: add a focused Spock test that fails on parent commit `ecce5b0` and passes on `ffd960e`. The test
should set up a persisted key in a later WAL group plus a matching WAL shadow key in that same group, then assert the
persisted scan result does not include the stale key.

## Reviewer Notes - Bug B Verification

Reviewed by: AI agent 2
Date: 2026-04-28

**Status:** CONFIRMED

The current code still builds the persisted-scan WAL shadow set only once:

```java
final var inWalKeys = oneSlot.getWalByGroupIndex(walGroupIndex).inWalKeysFormScan(beginScanSeq);
```

`KeyLoader.scan(...)` then loops from that starting `walGroupIndex` through later WAL groups and passes the same
`inWalKeys` set into every `readKeysToList(...)` call. Inside `readKeysToList(...)`, persisted entries are filtered only
by that set:

```java
if (inWalKeys.contains(key)) {
    expiredOrNotMatchedCount[0]++;
    return;
}
```

This is correct only for the first WAL group passed to `KeyLoader.scan(...)`. When the outer loop advances to
`j > walGroupIndex`, the skip set still represents the starting WAL group, not the current group `j`.

The bug is reachable from `SGroup.scan(...)`: after WAL scanning reaches its end, `SGroup.scan()` calls
`keyLoader.scan(0, ...)` for persisted buckets. If a key has a newer WAL value or delete tombstone in WAL group 1 or
later, WAL scanning has already considered that key, but persisted scanning will not skip the stale key-bucket entry for
that later group because it is still using group 0's `inWalKeys`.

Impact is therefore real:

- a newer WAL value in group `j > 0` can be returned once from WAL scan and again from stale persisted key-bucket scan;
- a WAL delete tombstone in group `j > 0` can hide the key for point reads but fail to hide the stale persisted key from
  SCAN.

This makes SCAN disagree with `GET`/`EXISTS`, which use WAL first in `OneSlot.get(...)` and `OneSlot.exists(...)`.
