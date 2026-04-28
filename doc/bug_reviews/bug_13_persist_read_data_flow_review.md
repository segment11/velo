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
| A - Persisted SCAN cursor ignores post-scan-start entries | High | Needs reviewer verification | High |
| B - Persisted SCAN skips WAL-shadowed keys only for first WAL group | High | Needs reviewer verification | High |

## Suggested Fix Direction

Finding A should mirror `Wal.scan(...)`: track entries skipped because `seq > beginScanSeq` and include that count in the
next `keyBucketsSkipCount`.

Finding B should compute WAL-shadowed keys per WAL group, either by moving `inWalKeysFormScan(beginScanSeq)` inside the
outer `for (int j = ...)` loop or by passing the WAL group index into `readKeysToList(...)` and resolving the set there.

Both fixes should be covered by focused Spock tests in `KeyLoaderTest.groovy` or an integration-style `SGroup`/`OneSlot`
test that exercises multi-call SCAN cursor behavior.
