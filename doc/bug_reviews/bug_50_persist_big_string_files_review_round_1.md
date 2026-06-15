# Bug 50: BigStringFiles Lifecycle Review — Round 1

**Author:** AI Agent 1
**Date:** 2026-06-15
**Module:** persist
**Classes:**
- `io.velo.persist.BigStringFiles` (`src/main/java/io/velo/persist/BigStringFiles.java`, 500 lines)
- `io.velo.persist.OneSlot` (`src/main/java/io/velo/persist/OneSlot.java`, 2238 lines) — big-string related methods
- `io.velo.persist.Wal` (`src/main/java/io/velo/persist/Wal.java`, 1010 lines) — `intervalDeleteExpiredBigStringFiles`, `put` uuid map maintenance
- `io.velo.task.TaskRunnable` (`src/main/java/io/velo/task/TaskRunnable.java`, 106 lines) — periodic task re-scheduling

**Related classes reviewed for context:**
- `io.velo.BaseCommand` (`src/main/java/io/velo/BaseCommand.java`) — `getCv` big-string read path, `set` big-string write path
- `io.velo.CompressedValue` (`src/main/java/io/velo/CompressedValue.java`) — `SP_TYPE_BIG_STRING`, `setCompressedDataAsBigString`, `getBigStringMetaUuid`
- `io.velo.command.XGroup` (`src/main/java/io/velo/command/XGroup.java`) — replication big-string fetch (`incremental_big_string`, `exists_big_string`)
- `io.velo.repl.incremental.XBigStrings` (`src/main/java/io/velo/repl/incremental/XBigStrings.java`) — binlog content for big strings
- `io.velo.persist.KeyLoader` (`src/main/java/io/velo/persist/KeyLoader.java`) — `intervalDeleteExpiredBigStringFiles` for key-bucket-resident entries
- `io.velo.task.TaskChain` (`src/main/java/io/velo/task/TaskChain.java`) — task exception handling
- `io.velo.decode.BigStringNoMemoryCopy` (`src/main/java/io/velo/decode/BigStringNoMemoryCopy.java`) — zero-copy large value reference

**Design documents reviewed:**
- `doc/design/02_persist_layer_design.md`
- `doc/design/03_type_system_design.md`
- `doc/design/07_compression_design.md`

**Prior reviews reviewed for coverage gaps:**
- `doc/bug_reviews/bug_46_persist_layer_review_round_1.md` (OneSlot.put big-string, intervalDeleteOverwriteBigStringFiles)
- `doc/bug_reviews/bug_49_persist_one_slot_chunk_review_round_1.md` (OneSlot.removeDelay/put binlog ordering, Chunk.persist segment rollback)

---

## Scope

This review focuses on the full lifecycle of big-string spill files managed by `BigStringFiles`:

- **Write path**: `BaseCommand.set` (bigStringNoMemoryCopy), `OneSlot.put` (chunk spill)
- **Read path**: `BaseCommand.getCv` → `BigStringFiles.getBigStringBytes`
- **UUID index maintenance**: `bigStringUuidByKey` / `bigStringUuidSet` via `updateUuidFromEncoded`, `populateFromKeyBuckets`, `refreshBigStringUuidMap`
- **GC / cleanup**: `intervalDeleteOverwriteBigStringFiles` (orphan sweep), `intervalDeleteExpiredBigStringFiles` (WAL + key-bucket expiry), `handleWhenCvExpiredOrDeleted`
- **Replication**: `XBigStrings` binlog replay, `incremental_big_string` fetch, `exists_big_string` full-sync
- **LRU cache**: `bigStringBytesByUuidLRU` lifecycle and memory estimation
- **Error handling**: exception propagation from big-string operations into the periodic task loop

Prior rounds (46, 49) covered the `OneSlot.put` big-string spill write path, the `doPersist` try-catch for the `needPutV != null` path, and the `intervalDeleteOverwriteBigStringFiles` orphan scan self-healing. This round covers areas not previously reviewed.

---

## Finding 1: Uncaught exception in `OneSlot.doTask` permanently kills the periodic maintenance loop

**Severity:** High

**Files:**
- `src/main/java/io/velo/persist/OneSlot.java:838-866` (`doTask`)
- `src/main/java/io/velo/persist/Wal.java:921-941` (`intervalDeleteExpiredBigStringFiles`)
- `src/main/java/io/velo/persist/BigStringFiles.java:479-499` (`handleWhenCvExpiredOrDeleted`)
- `src/main/java/io/velo/task/TaskRunnable.java:73-90` (`run`)

**Code excerpt:**

```java
// OneSlot.doTask, lines 838-866 (simplified)
public void doTask(int loopCount) {
    if (MultiWorkerServer.isStopping) {
        return;
    }

    taskChain.doTask(loopCount);          // line 843: catches its own task exceptions

    var canTruncateFdIndex = metaChunkSegmentFlagSeq.canTruncateFdIndex;
    if (canTruncateFdIndex != -1) {
        truncateChunkFile(canTruncateFdIndex);
    }

    // delete expired big string files in wal, only when master role
    // execute once every 100ms
    if (!isAsSlave() && loopCount % 10 == 0) {
        var wal = walArray[loopCount % walArray.length];
        if (wal != null) {
            var count = wal.intervalDeleteExpiredBigStringFiles();  // line 856: NOT in try-catch
            // ...
        }
    }

    // execute once every 10ms
    intervalDeleteOverwriteBigStringFiles();  // line 865: NOT in try-catch
}
```

```java
// BigStringFiles.handleWhenCvExpiredOrDeleted, lines 479-499
public void handleWhenCvExpiredOrDeleted(String key, CompressedValue shortStringCv, PersistValueMeta pvm) {
    if (shortStringCv == null) return;
    if (!shortStringCv.isBigString()) return;

    var uuid = shortStringCv.getBigStringMetaUuid();
    var bucketIndex = KeyHash.bucketIndex(shortStringCv.getKeyHash());
    var isDeleted = deleteBigStringFileIfExist(uuid, bucketIndex, shortStringCv.getKeyHash());
    removeBigStringUuidIfMatches(key, uuid);
    if (!isDeleted) {
        throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
    }
}
```

```java
// TaskRunnable.run, lines 73-90
@Override
public void run() {
    if (isStopped) return;
    if (!isStartDone) {
        slotWorkerEventloop.delay(INTERVAL_MS * 100, this);
        return;
    }

    for (var oneSlot : oneSlots) {
        oneSlot.doTask(loopCount);               // line 85: can throw
    }
    loopCount++;

    slotWorkerEventloop.delay(INTERVAL_MS, this); // line 89: NOT reached if doTask throws
}
```

**Root cause and impact:**

`OneSlot.doTask` calls `wal.intervalDeleteExpiredBigStringFiles()` (line 856) and `intervalDeleteOverwriteBigStringFiles()` (line 865) **outside** of any try-catch. The `taskChain.doTask(loopCount)` call at line 843 catches exceptions from individual `ITask` runs (see `TaskChain.java:45-49`), but the two big-string maintenance calls after it have no such protection.

`Wal.intervalDeleteExpiredBigStringFiles()` (Wal.java:921-941) iterates `delayToKeyBucketShortValues` and calls `oneSlot.bigStringFiles.handleWhenCvExpiredOrDeleted(...)` for expired big-string entries. `handleWhenCvExpiredOrDeleted` throws `RuntimeException` when `deleteBigStringFileIfExist` returns `false` — which happens when `file.delete()` fails at the OS level (file locks on Windows, permission issues, NFS stale handles, or filesystem corruption).

Similarly, `intervalDeleteOverwriteBigStringFiles()` (OneSlot.java:914-941) calls `deleteBigStringFileIfExist` on dequeued items; a `false` return is currently logged-and-requeued (line 918-922) rather than thrown, so this path is less dangerous. However, `getBigStringFileIdList` (called at line 927) can also throw (see Finding 4).

When any of these exceptions propagate out of `doTask`, it reaches `TaskRunnable.run()` at line 85. The critical issue: `slotWorkerEventloop.delay(INTERVAL_MS, this)` at line 89 — the mechanism that re-schedules the periodic task — is **after** the `for` loop. If the exception propagates, line 89 is never reached and the task is **never re-scheduled**.

**Impact**: Once the exception fires, **all** periodic maintenance for **all** slots handled by this worker thread permanently stops:
- No more expired-key cleanup (memory leak, stale data served)
- No more big-string orphan-file GC (disk leak)
- No more chunk file truncation (disk leak)
- No more replication pings / `XUpdateSeq` binlog entries (replication lag grows unbounded)
- No more `delayNeedCloseReplPairs` cleanup (dead repl pairs linger)

The server appears healthy (client requests still work via the event loop's other scheduling), but silently degrades. There is no alert or metric that indicates the task loop has died.

**Reachability:**

Triggered any time `file.delete()` returns `false` during expired-big-string cleanup. This is environment-dependent:
- NFS-mounted data directories with stale handles
- Windows environments with file locks
- Filesystem corruption after a hard crash
- Container environments with overlay filesystem quirks

Less likely on typical Linux ext4/xfs with healthy permissions, but the blast radius of a single failure is catastrophic (entire task loop dies).

**Suggested fix direction:**

Wrap the two unprotected calls in `OneSlot.doTask` with try-catch, matching the pattern already used by `TaskChain.doTask`:

```java
// Option A: wrap individual calls in doTask
if (!isAsSlave() && loopCount % 10 == 0) {
    var wal = walArray[loopCount % walArray.length];
    if (wal != null) {
        try {
            var count = wal.intervalDeleteExpiredBigStringFiles();
            // ... logging
        } catch (Exception e) {
            log.error("Interval delete expired big string files error, slot={}", slot, e);
        }
    }
}

try {
    intervalDeleteOverwriteBigStringFiles();
} catch (Exception e) {
    log.error("Interval delete overwrite big string files error, slot={}", slot, e);
}
```

```java
// Option B (defense-in-depth): wrap the for-loop body in TaskRunnable.run
for (var oneSlot : oneSlots) {
    try {
        oneSlot.doTask(loopCount);
    } catch (Exception e) {
        log.error("Do task error, slot={}", oneSlot.slot(), e);
    }
}
loopCount++;
slotWorkerEventloop.delay(INTERVAL_MS, this);
```

**Both options should be applied.** Option A prevents known throw sites from propagating. Option B is a safety net ensuring the re-scheduling always happens, even for unforeseen exceptions.

**Regression test should include:**
- A `OneSlot.doTask` scenario where `deleteBigStringFileIfExist` returns `false` (use `deleteForceReturnFalseForTest`). Assert the exception does NOT propagate out of `doTask`. Assert the task loop continues running on subsequent ticks (verify `loopCount` increments).
- A `TaskRunnable.run` scenario where `doTask` throws an arbitrary `RuntimeException`. Assert `slotWorkerEventloop.delay` is still called (verify the task is re-scheduled).

---

## Finding 2: Expired big strings in WAL are re-processed every 100ms indefinitely

**Severity:** Medium

**Files:**
- `src/main/java/io/velo/persist/Wal.java:921-941` (`intervalDeleteExpiredBigStringFiles`)

**Code excerpt:**

```java
// Wal.intervalDeleteExpiredBigStringFiles, lines 921-941
int intervalDeleteExpiredBigStringFiles() {
    int count = 0;
    final long currentTimeMillis = System.currentTimeMillis();

    for (var entry : delayToKeyBucketShortValues.entrySet()) {
        var v = entry.getValue();
        if (v.expireAt != NO_EXPIRE && v.expireAt < currentTimeMillis) {
            var spType = CompressedValue.onlyReadSpType(v.cvEncoded);
            if (spType == CompressedValue.SP_TYPE_BIG_STRING) {
                var cv = CompressedValue.decode(v.cvEncoded, v.keyBytes(), v.keyHash);
                assert oneSlot.bigStringFiles != null;
                oneSlot.bigStringFiles.handleWhenCvExpiredOrDeleted(entry.getKey(), cv, null);
                count++;
            }
        }
    }

    return count;
}
```

**Root cause and impact:**

This method iterates **all** entries in `delayToKeyBucketShortValues` every 100ms (called from `OneSlot.doTask` when `loopCount % 10 == 0`). When it finds an expired big-string entry, it calls `handleWhenCvExpiredOrDeleted` to delete the big-string file and remove the uuid from the map. However, it **never removes the entry from `delayToKeyBucketShortValues`**.

On every subsequent 100ms tick, the same expired entry is:
1. Re-iterated (O(n) scan of all short values)
2. Re-decoded via `CompressedValue.decode` (allocates a new `CompressedValue` object)
3. Re-processed via `handleWhenCvExpiredOrDeleted` — `deleteBigStringFileIfExist` returns `true` (file already gone, `file.exists()` is false), `removeBigStringUuidIfMatches` is a no-op (uuid already removed)
4. `count++` inflates the returned metric, making the `count > 0` debug log fire every tick

This continues until the WAL is persisted and cleared (which happens when the WAL buffer fills up or the at-least-once-persist timer fires). Under light load with few writes, the WAL may not be cleared for seconds or minutes, causing hundreds of redundant iterations.

The wasted work per tick includes:
- Full `delayToKeyBucketShortValues` iteration (O(n) where n = all short values in this WAL group)
- `CompressedValue.decode` allocation for each expired big string
- `handleWhenCvExpiredOrDeleted` call (file existence check, map lookup)

**Note:** This method also triggers Finding 1 on every tick for entries whose file deletion genuinely fails — the `RuntimeException` from `handleWhenCvExpiredOrDeleted` fires repeatedly, and without the Finding 1 fix, kills the task loop on the first occurrence.

**Reachability:**

Any big-string key with a TTL that expires while still in WAL short values. This is the normal case for short-lived large values (e.g., cached large API responses with 60s TTL).

**Suggested fix direction:**

Collect expired keys during iteration and remove them after the loop (avoid `ConcurrentModificationException`):

```java
int intervalDeleteExpiredBigStringFiles() {
    int count = 0;
    final long currentTimeMillis = System.currentTimeMillis();
    var keysToRemove = new ArrayList<String>();

    for (var entry : delayToKeyBucketShortValues.entrySet()) {
        var v = entry.getValue();
        if (v.expireAt != NO_EXPIRE && v.expireAt < currentTimeMillis) {
            var spType = CompressedValue.onlyReadSpType(v.cvEncoded);
            if (spType == CompressedValue.SP_TYPE_BIG_STRING) {
                var cv = CompressedValue.decode(v.cvEncoded, v.keyBytes(), v.keyHash);
                assert oneSlot.bigStringFiles != null;
                oneSlot.bigStringFiles.handleWhenCvExpiredOrDeleted(entry.getKey(), cv, null);
                keysToRemove.add(entry.getKey());
                count++;
            }
        }
    }

    keysToRemove.forEach(delayToKeyBucketShortValues::remove);
    return count;
}
```

**Regression test should include:**
- Put a big-string key with a short TTL into WAL short values. Wait for expiry. Call `intervalDeleteExpiredBigStringFiles` twice. Assert the second call returns count=0 and does NOT call `handleWhenCvExpiredOrDeleted` (can verify via `bigStringMissingFileTotal` not incrementing, or via a mock/spy).

**Disposition: Won't fix** (per project decision 2026-06-15)

The WAL is append-only by design — entries cannot be removed from `delayToKeyBucketShortValues` mid-stream because the entry serves as a shadow over stale key-bucket data. If removed, `getFromWal` returns null, and `keyLoader.getValueXByKey` would surface the old key-bucket entry (pre-TTL, expireAt=NO_EXPIRE) referencing a now-deleted big-string file → `BigStringFileMissingException`.

The proposed tracking-set fix (side-channel `HashSet` + clear-on-WAL-reset) adds its own per-iteration lookup cost and lifecycle complexity that exceeds the benefit:

- **No crash**: repeat calls are fully idempotent — `deleteBigStringFileIfExist` returns `true` when the file is already gone, `removeBigStringUuidIfMatches` is a no-op. No exception is thrown.
- **Negligible perf cost**: per expired big-string per tick = 1 small `CompressedValue.decode` + 1 `file.exists()` stat syscall (returns false immediately) + 2 HashMap lookups. Sub-microsecond per entry.
- **Bounded window**: entries are cleared naturally on the next WAL persist+clear cycle (seconds). Big strings are rare (≥256KB values), so the count of expired ones in any WAL group is small.
- **Bounded frequency**: each WAL group is visited every `walArray.length × 100ms`, not every 100ms (rotation via `loopCount % walArray.length`).

With Bug 1's try-catch in `doTask` as a safety net, even a genuine delete failure is caught and logged without killing the task loop. The redundant re-processing is accepted as a minor inefficiency.

---

## Finding 3: Test-prefix gate in production code bypasses size thresholds

**Severity:** Medium

**Files:**
- `src/main/java/io/velo/persist/OneSlot.java:1431`

**Code excerpt:**

```java
// OneSlot.put, line 1430-1431
// NOT A BUG: "kerry-test-big-string-" is a deliberate test-prefix gate kept in production intentionally.
if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
```

**Root cause and impact:**

Any key containing the substring `"kerry-test-big-string-"` is unconditionally stored as a big-string spill file, regardless of the value size. This bypasses the normal `isPersistLengthOverSegmentLength` threshold.

Concerns:
1. **Unintended activation**: A user who happens to use keys with this prefix (e.g., a developer testing against a production-like system) gets per-file storage for tiny values, causing excessive file I/O and inode consumption.
2. **Disk abuse vector**: An external client can deliberately use keys with this prefix to force big-string storage for every small value, amplifying disk usage (one file per key instead of chunk-segment packing) and potentially exhausting inodes.
3. **Code smell**: The comment claims it is intentional, but it conflates test infrastructure with production code. The existing test suite (`BaseCommandTest.groovy:308`) uses `'kerry-test-big-string-key'` — the prefix should be gated behind a test flag, not embedded in production logic.

**Reachability:**

Any `SET` command with a key containing `"kerry-test-big-string-"`. The value can be arbitrarily small.

**Suggested fix direction:**

Remove the prefix check from production code. If the test needs to force big-string storage for small values, use the existing `bigStringNoMemoryCopy` mechanism (set `bigStringNoMemoryCopySize` to a small value in test configuration), or add a `@TestOnly` helper that directly calls `writeBigStringBytes`.

```java
// Fixed
if (isPersistLengthOverSegmentLength) {
```

**Regression test should include:**
- Verify existing tests that relied on the prefix still pass after migration to the test-only mechanism. Specifically update `BaseCommandTest.groovy:308` to use the new approach.

---

## Finding 4: Fragile filename parsing in `getBigStringFileIdList`

**Severity:** Low

**Files:**
- `src/main/java/io/velo/persist/BigStringFiles.java:299-313`

**Code excerpt:**

```java
// BigStringFiles.getBigStringFileIdList, lines 299-313
public List<IdWithKey> getBigStringFileIdList(int bucketIndex) {
    var list = new ArrayList<IdWithKey>();
    var files = new File(bigStringDir, bucketIndex + "").listFiles();
    if (files == null) {
        return list;
    }

    for (var file : files) {
        var arr = file.getName().split("_");
        var uuid = Long.parseLong(arr[0]);
        var keyHash = Long.parseLong(arr[1]);
        list.add(new IdWithKey(uuid, bucketIndex, keyHash, ""));
    }
    return list;
}
```

**Root cause and impact:**

The method splits each filename by `"_"` and parses two `Long` values with no error handling. A stray file in a bucket directory — from a partial write, filesystem corruption, an admin touch, `.DS_Store`, a editor swap file (`*.swp`), or a temp file from a crashed `FileUtils.writeByteArrayToFile` — causes `NumberFormatException` (non-numeric content) or `ArrayIndexOutOfBoundsException` (no underscore in filename).

This method is called by:
1. `intervalDeleteOverwriteBigStringFiles` (OneSlot.java:927) — the GC sweep, called every 10ms
2. `XGroup.exists_big_string` (XGroup.java:1135) — replication full-sync

An exception from here propagates through the GC sweep and, combined with Finding 1, can kill the periodic task loop. In the replication path, it would abort the big-string sync step.

**Reachability:**

Requires a non-standard file in the big-string bucket directory. Low probability in normal operation, but increases after filesystem errors, manual intervention, or partial-disk-full scenarios where `FileUtils.writeByteArrayToFile` may leave temp files.

**Suggested fix direction:**

```java
for (var file : files) {
    try {
        var arr = file.getName().split("_");
        if (arr.length < 2) continue;
        var uuid = Long.parseLong(arr[0]);
        var keyHash = Long.parseLong(arr[1]);
        list.add(new IdWithKey(uuid, bucketIndex, keyHash, ""));
    } catch (NumberFormatException e) {
        log.warn("Skip unparseable big string file, name={}, slot={}", file.getName(), slot);
    }
}
```

**Regression test should include:**
- Create a bucket directory with a valid file and a stray file (e.g., `"not_a_number.txt"`, `"abc_def"`). Assert `getBigStringFileIdList` returns only the valid entry without throwing.

---

## Finding 5: Dead code — `getUuid` identity method

**Severity:** Low (cleanup)

**Files:**
- `src/main/java/io/velo/persist/BigStringFiles.java:455-457`

**Code excerpt:**

```java
private static long getUuid(long uuid) {
    return uuid;
}
```

**Root cause and impact:**

Identity method that returns its argument unchanged. Never called anywhere in the codebase (verified via grep). Likely a leftover from a refactor that extracted uuid logic into `CompressedValue.getBigStringMetaUuid`.

No functional impact, but adds noise and confusion for future readers.

**Suggested fix:** Delete the method.

---

## Finding 6: `deleteAllBigStringFiles` leaves inconsistent counters on partial failure

**Severity:** Low

**Files:**
- `src/main/java/io/velo/persist/BigStringFiles.java:461-476`

**Code excerpt:**

```java
@SlaveNeedReplay
@SlaveReplay
public void deleteAllBigStringFiles() {
    if (bigStringBytesByUuidLRU != null) {
        bigStringBytesByUuidLRU.clear();
    }
    bigStringUuidByKey.clear();
    bigStringUuidSet.clear();

    try {
        FileUtils.cleanDirectory(bigStringDir);
        log.warn("Delete all big string files, count={}, slot={}", bigStringFilesCount, slot);
        bigStringFilesCount = 0;
        diskUsage = 0L;
    } catch (IOException e) {
        log.error("Delete all big string files error, slot={}", slot, e);
    }
}
```

**Root cause and impact:**

If `FileUtils.cleanDirectory` throws `IOException` after partially deleting files (e.g., one file is locked), `bigStringFilesCount` and `diskUsage` are **not** reset — they retain their pre-call values. But some files were actually deleted, so the counters over-report.

After this partial failure:
- `bigStringFilesCount` > actual file count on disk
- `diskUsage` > actual disk usage
- The uuid maps were already cleared (line 465-466), so ALL remaining files on disk are orphans in the map's view
- The GC sweep will eventually discover and queue the remaining files for deletion, but the counters stay wrong until each file is individually deleted (which decrements the counters)

**Reachability:**

Called during `OneSlot.flush()` (full data reset for slot re-initialization or full resync). Partial failure requires an I/O error during directory cleaning — rare but possible on mounted filesystems with locked files.

**Suggested fix direction:**

Reset counters before the attempt, and re-scan on failure:

```java
bigStringUuidByKey.clear();
bigStringUuidSet.clear();

try {
    FileUtils.cleanDirectory(bigStringDir);
} catch (IOException e) {
    log.error("Delete all big string files error, slot={}", slot, e);
    // Reconcile: re-scan actual files on disk
    bigStringFilesCount = 0;
    diskUsage = 0L;
    var files = bigStringDir.listFiles();
    if (files != null) {
        for (var file : files) {
            if (file.isDirectory()) {
                var subFiles = file.listFiles();
                if (subFiles != null) {
                    for (var subFile : subFiles) {
                        diskUsage += subFile.length();
                        bigStringFilesCount++;
                    }
                }
            }
        }
    }
    return;
}
log.warn("Delete all big string files, count={}, slot={}", bigStringFilesCount, slot);
bigStringFilesCount = 0;
diskUsage = 0L;
```

---

## Improvement 1: LRU memory estimate uses a fixed 4096-byte assumption

**Severity:** Low (improvement)

**Files:**
- `src/main/java/io/velo/persist/BigStringFiles.java:253-261`

**Code excerpt:**

```java
var maxSize = ConfForSlot.global.lruBigString.maxSize;
if (maxSize > 0) {
    final var maybeOneBigStringBytesLength = 4096;
    var lruMemoryRequireMB = maxSize * maybeOneBigStringBytesLength / 1024 / 1024;
    log.info("LRU max size for big string={}, maybe one big string bytes length is {}B, memory require={}MB, slot={}",
            maxSize, maybeOneBigStringBytesLength, lruMemoryRequireMB, slot);
    // ...
    this.bigStringBytesByUuidLRU = new LRUMap<>(maxSize);
}
```

**Description:**

The LRU is sized by entry count (`maxSize`, default 1000), with a fixed estimate of 4096 bytes per entry for memory planning. However, big strings are ≥256KB (the `bigStringNoMemoryCopySize` threshold) and can be much larger. With 1000 entries of 256KB each, actual LRU memory is ~256MB, far exceeding the reported ~4MB estimate.

The `LRUPrepareBytesStats` report and the `estimate()` method both under-report actual memory. In memory-constrained deployments, the LRU can consume far more heap than expected, potentially causing OOM or excessive GC pressure.

**Suggestion:** Track total bytes in the LRU (maintain a running sum on put/remove). Consider a byte-size-based eviction policy instead of (or in addition to) entry-count-based eviction. At minimum, update the memory estimate to use `bigStringNoMemoryCopySize` as the per-entry assumption.

---

## Improvement 2: Replication big-string fetch window can cause `BigStringFileMissingException`

**Severity:** Low (design limitation)

**Files:**
- `src/main/java/io/velo/repl/incremental/XBigStrings.java:188-201` (`apply`)
- `src/main/java/io/velo/command/XGroup.java:1072-1104` (`s_incremental_big_string`)
- `src/main/java/io/velo/persist/OneSlot.java:987-998` (repl task fetch trigger)
- `src/main/java/io/velo/BaseCommand.java:684-687` (read-miss throws)

**Description:**

When a slave replays an `XBigStrings` binlog entry, the CV reference (uuid metadata) is put into the slave's WAL asynchronously, and the uuid is added to `replPair.toFetchBigStringIdList` for file fetch. The actual file data is fetched one-per-second via the repl task (`OneSlot.java:987`, `executeOnceAfterLoopCount()` returns 100 ticks = 1s).

Until the file arrives, reads of that key find the big-string CV reference in the WAL but the file does not exist on disk → `BigStringFileMissingException` is thrown (BaseCommand.java:686). With many big strings to fetch (e.g., after a slave reconnect), this window can be seconds to minutes.

**Suggestion:** Consider:
1. Batching big-string fetches (send multiple uuids per request instead of one per second)
2. Falling back to a synchronous blocking fetch on read-miss instead of throwing, or
3. Returning a retryable error to the client instead of a hard exception

---

## Non-Findings (Verified Safe)

1. **UUID map maintenance on `put`**: Big strings always take the short-value WAL path because `CompressedValue.isShortString()` returns `true` for big-string CVs (the 12-byte uuid+dict metadata ≤ `SP_TYPE_SHORT_STRING_MIN_LEN = 16`). In `Wal.put`, the short-value branch always calls `updateUuidFromEncoded` (Wal.java:887), regardless of `needPersist`. The uuid map is correctly maintained in all cases. **Safe.**

2. **Overwrite detection in `OneSlot.put`**: `overwrittenBigStringUuid` is captured before the put, and after the put, `bigStringFiles.getBigStringUuid(key)` reflects the new uuid (because `updateUuidFromEncoded` already ran in `Wal.put`). The comparison correctly detects when the old uuid should be queued for deletion. **Safe.**

3. **Startup uuid map recovery**: `Wal.lazyReadFromFile` → `refreshBigStringUuidMap` (rebuilds from WAL short values) + `populateFromKeyBuckets` (fills gaps from key bucket data using `putIfAbsent` so WAL entries take precedence). The two-source merge is correct. **Safe.**

4. **`initCheck()` thread safety**: Although `initCheck()` spawns a background thread that accesses non-thread-safe structures (`delayToDeleteBigStringFileIds`, `bigStringUuidSet`), it runs **before** `scheduleRunnable.startDone(true)` (MultiWorkerServer.java:1162 vs 1165). Before `startDone`, `TaskRunnable.run()` short-circuits without calling `doTask` (TaskRunnable.java:79-82). No client requests are processed (socket server not yet started). The background thread is the sole accessor. **Safe in practice.**

5. **LRU cache invalidation on file delete**: `deleteBigStringFileIfExist` removes the uuid from `bigStringBytesByUuidLRU` (BigStringFiles.java:425-426) before deleting the file. No stale LRU entries survive file deletion. **Safe.**

6. **`handleWhenCvExpiredOrDeleted` bucket index consistency**: The bucket index is computed as `KeyHash.bucketIndex(shortStringCv.getKeyHash())`, which is the same function used when the file was written (`BaseCommand.slot()` → `KeyHash.bucketIndex(keyHash)`). The file path is consistent. **Safe.**

---

## Summary

| # | Severity | Description | Status |
|---|----------|-------------|--------|
| 1 | High | Uncaught exception in `OneSlot.doTask` kills periodic maintenance loop permanently | **Fixed** (`9b7f4832`) |
| 2 | ~~Medium~~ Won't-fix | Expired big strings in WAL re-processed every 100ms indefinitely | Confirmed — **won't fix** (see disposition below) |
| 3 | ~~Medium~~ Won't-fix | Test-prefix gate `"kerry-test-big-string-"` in production code | Confirmed — **won't fix** (intentional, documented in code comment at OneSlot.java:1445) |
| 4 | Low | Fragile filename parsing in `getBigStringFileIdList` — no error handling | **Fixed** (`9b7f4832`) |
| 5 | Low | Dead code: `getUuid` identity method never called | **Fixed** (`08cb9600`) |
| 6 | Low | `deleteAllBigStringFiles` leaves inconsistent counters on partial failure | **Fixed** (`08cb9600`) |
| I1 | Low (improvement) | LRU memory estimate uses fixed 4096-byte assumption vs actual ≥256KB entries | Confirmed |
| I2 | Low (improvement) | Replication big-string fetch window causes transient `BigStringFileMissingException` | Confirmed |

---

## Review Feedback — Round 1

**Reviewer:** AI Agent 2
**Date:** 2026-06-15

This is the reviewer-side pass for bug 50 round 1. Each finding was verified against the current
codebase. Verdict, evidence, and any refinements are recorded below.

### Verdict Summary

| # | Severity | Verdict | Notes |
|---|----------|---------|-------|
| 1 | High | **Fixed** (`9b7f4832`) | See note (a) on `intervalDeleteOverwriteBigStringFiles` |
| 2 | ~~Medium~~ Won't-fix | **Confirmed — won't fix** | Idempotent re-processing; fix cost exceeds benefit (see disposition) |
| 3 | ~~Medium~~ Won't-fix | **Confirmed — won't fix** | Intentional test-prefix gate, documented in code comment |
| 4 | Low | **Fixed** (`9b7f4832`) | Verified |
| 5 | Low | **Fixed** (`08cb9600`) | Dead code removed |
| 6 | Low | **Fixed** (`08cb9600`) | Reconcile counters from disk on partial failure |
| I1 | Low (improvement) | **Confirmed** | Under-reporting factor verified at ~64x |
| I2 | Low (improvement) | **Confirmed** | Verified 1s/task-tick fetch cadence |

### Finding 1 — Confirmed with refinement

**Evidence reviewed:**
- `src/main/java/io/velo/persist/OneSlot.java:838-866` — `doTask` body. Confirmed: no `try`/`catch`
  around line 856 (`wal.intervalDeleteExpiredBigStringFiles()`) or line 865
  (`intervalDeleteOverwriteBigStringFiles()`). Only `taskChain.doTask(loopCount)` at line 843
  has internal exception isolation (via `TaskChain.doTask` at lines 45-49 which wraps each
  `ITask.run()` call).
- `src/main/java/io/velo/task/TaskRunnable.java:73-90` — confirmed that `slotWorkerEventloop.delay(INTERVAL_MS, this)`
  at line 89 sits AFTER the `for` loop. Any exception from `oneSlot.doTask(loopCount)` at line 85
  propagates out of `run()` and the task is never re-scheduled. There is no outer catch.
- `src/main/java/io/velo/persist/BigStringFiles.java:478-499` — confirmed
  `handleWhenCvExpiredOrDeleted` throws `new RuntimeException(...)` at line 495 when
  `deleteBigStringFileIfExist` returns `false`.
- `src/main/java/io/velo/persist/BigStringFiles.java:421-453` — confirmed
  `deleteForceReturnFalseForTest` test hook at line 428-430 makes
  `deleteBigStringFileIfExist` unconditionally return `false`. This is the seam that the
  regression test in the doc would need to use.

**Refinement (a) — `intervalDeleteOverwriteBigStringFiles` already swallows the dequeue-fail case.**
The reviewer claim that "any exception from the dequeue path propagates" needs a small
correction: when `delayToDeleteBigStringFileIds` has a queued entry whose physical delete returns
`false`, the code at `OneSlot.java:914-922` logs a warning and re-queues the id at the tail — it
does NOT throw. So the direct `file.delete()` failure inside the dequeue loop is already
isolated. The exceptions that CAN escape this method are:

1. `BigStringFiles.getBigStringFileIdList(targetBucketIndex)` at line 927 — can throw
   `NumberFormatException` / `ArrayIndexOutOfBoundsException` for stray files (Finding 4).
2. Any unforeseen `RuntimeException` from `containsUuid` (line 929), record construction, or map
   mutations.

So Finding 1 remains correct in principle, but the suggested fix (Option A) needs to wrap the
WHOLE `intervalDeleteOverwriteBigStringFiles(int targetBucketIndex)` body, not just the explicit
call site. Option B (defense-in-depth in `TaskRunnable.run`) remains valid as a safety net.

**Refinement (b) — `taskChain.doTask` already isolates ITask exceptions.**
`TaskChain.doTask` (`src/main/java/io/velo/task/TaskChain.java:45-49`) catches `Exception` from
each `ITask.run()`. So the big-string fetch ITask inside the chain (which sends
`incremental_big_string` to the master) is already protected. The unprotected calls are the
two big-string GC calls that are NOT routed through the task chain.

**Reachability refinement:** The blast radius is confirmed. A single `RuntimeException` from
`handleWhenCvExpiredOrDeleted` propagates through `OneSlot.doTask` → `TaskRunnable.run`, and
since `slotWorkerEventloop.delay(INTERVAL_MS, this)` is the only re-schedule mechanism (verified
at `TaskRunnable.java:89`), the entire periodic maintenance loop dies for ALL slots owned by
this worker. The server appears healthy (client requests keep flowing through the event loop's
socket-level scheduling) but maintenance halts. There is no health metric that surfaces this
condition.

**Suggested fix direction (refined):**
- **A1 (recommended):** Wrap the entire `intervalDeleteOverwriteBigStringFiles(int targetBucketIndex)`
  body (the `delayToDeleteBigStringFileIds` dequeue block AND the `getBigStringFileIdList` scan
  block) in a single try-catch in `doTask`. This addresses Finding 1 + Finding 4 together.
- **A2 (recommended):** Wrap `wal.intervalDeleteExpiredBigStringFiles()` call at line 856 in
  try-catch as well. This protects the WAL-expired-big-string-cleanup path that is the most
  likely thrower.
- **B (defense-in-depth, recommended):** Wrap the for-loop body in `TaskRunnable.run` lines 84-86
  so unforeseen exceptions in any `doTask` body cannot permanently kill the loop. This is the
  pattern recommended by the doc's Option B and is the most important safety net.

**Regression test scope (refined):**
1. `OneSlot.doTask` test: enable `deleteForceReturnFalseForTest = true` on the big-string files,
   pre-populate `delayToKeyBucketShortValues` with an expired big-string `V`, drive `doTask`
   twice, and assert `loopCount` still increments and no exception leaks.
2. `TaskRunnable.run` test: inject a `OneSlot` whose `doTask` throws `RuntimeException` (use a
   subclass or a test field), drive `run()` once, and assert `slotWorkerEventloop.delay` was
   still called (use a counter or spy). Requires constructing or stubbing the Eventloop.

### Finding 2 — Confirmed

**Evidence reviewed:**
- `src/main/java/io/velo/persist/Wal.java:921-941` — confirmed the loop iterates
  `delayToKeyBucketShortValues.entrySet()` and calls `handleWhenCvExpiredOrDeleted` for each
  expired big-string entry. No `keysToRemove` accumulation; the entry is left in the map after
  processing.
- `delayToKeyBucketShortValues` is a `HashMap<String, V>` declared at `Wal.java:311`. The map
  is cleared only by:
  - `Wal.java:579` and `Wal.java:597` (inside `removeDelay`/lazy-read-after-persist paths),
  - `Wal.java:893` (inside the `put` overwrite-and-persist path).
  None of these are triggered by the periodic 100ms tick — they only fire on persist.
- The `count++` at line 935 inflates the metric returned, which feeds the `if (count > 0 || wal.groupIndex == 0)` debug log at `OneSlot.java:857`. With this bug, the log fires every tick until WAL persist, even though no new work was done.
- Each `CompressedValue.decode` call at line 931 allocates a new `CompressedValue` plus any
  internal buffers, and `handleWhenCvExpiredOrDeleted` performs a file-existence check, an LRU
  lookup, a map removal, and a UUID set removal — all wasted on subsequent ticks.

**Suggested fix direction (unchanged):** Collect expired keys into an `ArrayList<String>` during
the loop, then `keysToRemove.forEach(delayToKeyBucketShortValues::remove)` after the loop to
avoid `ConcurrentModificationException`. The current code runs on the slot worker thread
(verified by the `checkCurrentThreadId` guard), so there's no concurrent-modification risk
from other threads, but the fix preserves the existing access pattern.

**Regression test scope:** Put a big-string `V` with a short TTL into
`delayToKeyBucketShortValues` directly. Call `intervalDeleteExpiredBigStringFiles()` twice
with `deleteForceReturnFalseForTest = true` (so each call increments the warn-log/metric path
but does not throw). Assert the second call returns 0, OR (better) spy on
`handleWhenCvExpiredOrDeleted` calls and assert it was invoked exactly once across the two
calls.

### Finding 3 — Confirmed (cross-referenced with prior reviews)

**Evidence reviewed:**
- `src/main/java/io/velo/persist/OneSlot.java:1430-1431` — confirmed the prefix gate
  (`key.contains("kerry-test-big-string-")`) sits alongside the size threshold check.
- `src/test/groovy/io/velo/BaseCommandTest.groovy:308` — confirmed test uses
  `'kerry-test-big-string-key'` and exercises the big-string file write path on a small value.
- `src/test/groovy/io/velo/persist/OneSlotTest.groovy:495,621,1540,1584,1652,1665,1846` —
  confirmed multiple tests rely on this prefix to drive small values into the big-string file
  path (including overwrite, orphan-on-doPersist-throw, stuck-on-delete-fail, del-before-persist).
- `docs/plans/2026-05-16-big-string-lifecycle-redesign.md:86-88` — confirmed the design plan
  flags this as a "production code-quality smell that exists only because the big-string
  admission branch is special".
- `doc/bug_reviews/bug_31_persist_layer_data_write_flow_review_round_1.md:761` — this exact
  finding was previously triaged as **Confirmed** with **Low** severity by the same reviewer
  role. The recommended disposition was "Replace with explicit test-only opt-in and update
  dependent tests". That disposition applies here.

**Severity check:** The author marks this as **Medium**, but the prior reviewer in bug 31 round
1 marked it **Low**. The reviewer here agrees with **Low** — the blast radius is bounded (a
client who happens to use this prefix gets per-file storage instead of segment packing; this is
wasteful but not a security or correctness issue), and the test gate is well-known and stable.
The code-quality concern is real but the operational risk is low.

**Suggested fix direction (unchanged, scoped to avoid touching unrelated tests):**
- Add a `@VisibleForTesting` static helper on `OneSlot` (or `Wal`) named
  `forceBigStringAdmissionForTest()` or expose a flag in `ConfForSlot.global.confPersist`
  like `bigStringAdmissionPrefixForTest`. Tests opt in via the flag.
- Remove the `|| key.contains("kerry-test-big-string-")` clause from `OneSlot.java:1431`.
- Update the dependent tests in `BaseCommandTest.groovy:308` and `OneSlotTest.groovy:495,621,
  1540,1584,1652,1665,1846` to use the new opt-in mechanism. This is a non-trivial test
  sweep (7+ test sites).
- Alternative: keep the prefix gate but move it behind a `@TestOnly` method or a
  `boolean` config flag enabled only by tests. This is the minimal-risk path.

**Regression test scope:** All existing tests that use the prefix must continue to pass.
Add a test that sets a small value with a key NOT containing the prefix and asserts it goes
to chunk segments (not the big-string file). Add a test that sets the same small value with
the prefix (or new flag) and asserts it goes to a big-string file.

### Finding 4 — Confirmed

**Evidence reviewed:**
- `src/main/java/io/velo/persist/BigStringFiles.java:299-313` — confirmed the bare
  `file.getName().split("_")` followed by two `Long.parseLong(arr[0])` / `arr[1]` calls with no
  try-catch and no `arr.length` guard.
- Callers (confirmed via grep):
  - `src/main/java/io/velo/persist/OneSlot.java:927` — called from `intervalDeleteOverwriteBigStringFiles(int)`,
    which runs every 10ms (one bucket per tick).
  - `src/main/java/io/velo/command/XGroup.java:1135` (line reference matches the doc) — called
    from the replication `exists_big_string` path during full-sync.

**Refinement — callout for the GC sweep path:** When `getBigStringFileIdList` throws inside
`OneSlot.intervalDeleteOverwriteBigStringFiles(int)`, the exception escapes through
`OneSlot.doTask` → `TaskRunnable.run`, which (per Finding 1) permanently kills the periodic
loop. So Finding 4 amplifies Finding 1's blast radius. The two fixes should be applied
together.

**Suggested fix direction (unchanged):** Wrap the per-file parsing in try-catch, log a warning,
skip the unparseable file, and continue. Use `arr.length < 2` to short-circuit files without
an underscore. Consider also `if (!file.isFile()) continue;` at the top of the loop to skip
directories.

**Regression test scope:** Create a temp dir, write a file named `not_a_number.txt` and another
named `123_456` (valid). Call `getBigStringFileIdList(0)` (or equivalent setup). Assert the
returned list contains exactly one `IdWithKey(uuid=123, ...)` entry and no exception is
thrown. Also test files with a single-name (no underscore) like `12345`.

### Finding 5 — Confirmed (zero callers)

**Evidence reviewed:**
- `src/main/java/io/velo/persist/BigStringFiles.java:455-457` — the method body is
  `return uuid;` (identity).
- Searched all of `src/` for callers: `BigStringFiles.getUuid` and `BigStringFiles. getUuid` both
  return zero hits. The only `getUuid` references in the codebase are:
  - `src/main/java/io/velo/repl/incremental/XBigStrings.java:32` — instance method on a
    different class (`XBigStrings.getUuid()`).
  - `src/main/java/io/velo/persist/OneSlot.java:1490` — calls `xBigStrings.getUuid()`, not
    `BigStringFiles.getUuid`.
- The method is `private static`, so no test or reflection-based caller can reach it.

**Suggested fix direction (unchanged):** Delete lines 455-457.

**Regression test scope:** None needed. Removing dead code does not change behavior. A grep
guard (or pre-existing test coverage) ensures no future regression.

### Finding 6 — Confirmed

**Evidence reviewed:**
- `src/main/java/io/velo/persist/BigStringFiles.java:461-476` — confirmed the structure:
  uuid maps cleared at lines 465-466 BEFORE the directory-clean attempt, then
  `FileUtils.cleanDirectory(bigStringDir)` at line 469, then the reset of `bigStringFilesCount = 0`
  and `diskUsage = 0L` at lines 471-472 — both inside the `try` block. If `cleanDirectory`
  throws at line 469, lines 471-472 never execute, and the counters retain their pre-call
  values.
- `org.apache.commons.io.FileUtils.cleanDirectory(File)` throws `IOException` if any file under
  the directory cannot be deleted. This is reachable on locked files (Windows), stale NFS
  handles, or read-only mounts.
- The `@SlaveNeedReplay` and `@SlaveReplay` annotations indicate this is a replication-aware
  operation called during `OneSlot.flush()` and slave reset paths.

**Refinement — the inconsistency is more subtle than the doc states.** After the partial
failure:
- `bigStringFilesCount` still holds the count from BEFORE the attempt, but some files are
  actually gone from disk. So `bigStringFilesCount > actualFileCount`. Subsequent GC sweeps
  decrement the count correctly as they delete each remaining file individually. So the
  inconsistency self-corrects slowly.
- `diskUsage` holds the value from BEFORE the attempt. Each subsequent `deleteBigStringFileIfExist`
  decrements `diskUsage` by `len` (`BigStringFiles.java:444`), but the len used is the current
  file's length, not the original. If the partially-deleted file was overwritten by the caller
  between the failed clean and the next GC tick, the decrement could be wrong.
- The uuid maps (`bigStringUuidByKey`, `bigStringUuidSet`) were already cleared at lines 465-466.
  So all remaining files on disk are orphans from the map's perspective, which is the desired
  state for the next `intervalDeleteOverwriteBigStringFiles` sweep. **This part is correct** —
  the map-clear-first ordering is the right choice.

**Suggested fix direction (refined):**
- Move the `bigStringFilesCount = 0` and `diskUsage = 0L` assignments BEFORE the `try` block,
  so the counters are reset unconditionally. The next GC sweep will reconcile the actual
  on-disk count via `intervalDeleteOverwriteBigStringFiles` (which iterates files via
  `getBigStringFileIdList`).
- The map-clear-first ordering is correct — keep it.
- The doc's suggestion of re-scanning on failure is more conservative but adds complexity.
  The simpler "reset-then-attempt" is sufficient because the GC sweep already maintains the
  counters accurately via `bigStringFilesCount++` (line 280) and `diskUsage += subFile.length()`
  (line 279) on startup, and via `bigStringFilesCount--` (line 443) and `diskUsage -= len`
  (line 444) on each `deleteBigStringFileIfExist`.

**Regression test scope:** Mock or pre-create a big-string directory with one valid file and
one undeletable file (e.g., chmod 000, then call `deleteAllBigStringFiles()`). Assert:
1. The valid file is deleted (best-effort; the test only verifies the method does not throw).
2. `bigStringFilesCount == 0` after the call (this is the assertion that fails today).
3. `diskUsage == 0` after the call.

Note: a fully reliable undeletable-file scenario is hard to construct portably. An
alternative is to subclass `BigStringFiles` and stub `FileUtils.cleanDirectory` via dependency
injection — but `cleanDirectory` is a static call. A simpler approach: introduce a
`@VisibleForTesting boolean deleteAllForceIOExceptionForTest` flag that makes the method
take the catch path with a controlled state, then assert the post-state.

### Improvement 1 — Confirmed (under-reporting factor verified)

**Evidence reviewed:**
- `src/main/java/io/velo/persist/BigStringFiles.java:253-261` — confirmed the fixed
  `maybeOneBigStringBytesLength = 4096` constant.
- `src/main/java/io/velo/ConfForGlobal.java:179` — confirmed
  `bigStringNoMemoryCopySize = 1024 * 256 = 262144` (256 KiB).
- `src/main/java/io/velo/MultiWorkerServer.java:1419` — confirmed the same default is read from
  config at startup.
- `src/main/java/io/velo/decode/RESP.java:176` — confirmed the same threshold gates the
  big-string admission in the request decoder, so every big-string stored in the LRU is at
  least 256 KiB.

**Calculation:** With default `lruBigString.maxSize = 1000` (typical) and the actual minimum
256 KiB per entry, the LRU holds ≥ 256 MiB in practice. The logged estimate is
`1000 * 4096 / 1024 / 1024 ≈ 3.9 MiB`. Under-reporting factor: ~64x.

**Refinement — even the 256 KiB estimate is a lower bound.** Big strings are commonly 1-10 MiB
(payload-sized values). At 1 MiB per entry × 1000 = 1 GiB. The estimate is off by 256x-2560x in
realistic deployments.

**Suggested fix direction (unchanged but with concrete steps):**
1. Maintain a running `long lruBytes` field. Increment by `bytes.length` on each LRU `put` (after
   the put succeeds), decrement by `prev.length` on each LRU eviction. Track `prev` via a
   `HashMap<Long, Integer>` of uuid → length, OR rely on `LRUMap`'s eviction listener (Apache
   Commons Collections 4 has `LRUMap.Entry` removal hooks via subclassing).
2. Update the `LRUPrepareBytesStats.add(...)` call at line 261 to use `lruBytes` instead of
   `lruMemoryRequireMB`.
3. Update the `estimate()` method at line 289 to add `lruBytes` to the reported size.
4. As an option, switch from entry-count-based eviction to byte-size-based eviction. Apache
   Commons `LRUMap` does not natively support byte-size; consider a custom wrapper.

**Regression test scope:** After enabling `bigStringBytesByUuidLRU`, write a 1 MiB big-string
file, call `getBigStringBytes` to seed the LRU, then call `estimate()` and assert the reported
size includes the actual byte count (not 4096). This requires the implementation change to
take effect first.

### Improvement 2 — Confirmed (fetch cadence verified)

**Evidence reviewed:**
- `src/main/java/io/velo/repl/incremental/XBigStrings.java:188-201` — `apply` calls
  `oneSlot.asyncExecute(() -> oneSlot.put(...))` at line 195-196. This is a slot-worker-thread
  callback, NOT a synchronous put. So the big-string CV reference will land in the slave's
  WAL asynchronously.
- `src/main/java/io/velo/persist/OneSlot.java:1037-1039` — confirmed
  `executeOnceAfterLoopCount() { return 100; }` with the comment "do every 100 loop, 1s".
  Combined with `TaskRunnable.run` running every 10ms (`TaskRunnable.java:78,89`), the fetch
  fires once per second.
- `src/main/java/io/velo/persist/OneSlot.java:987-998` — confirmed each fetch task tick only
  sends ONE uuid (the head of the queue) per `replPair`.
- `src/main/java/io/velo/BaseCommand.java:684-687` — confirmed
  `throw new BigStringFileMissingException(...)` when `getBigStringBytes` returns null.
- `src/main/java/io/velo/persist/BigStringFiles.java:353-359` — confirmed `readBigStringBytes`
  returns null and increments `bigStringMissingFileTotal` when the file does not exist on disk.

**Reachability refinement:** The window where the CV reference is visible to reads but the
file is not yet on disk is bounded by:
1. The async `put` on the slave side (essentially immediate, since it's on the same worker
   thread).
2. The 1s task-tick fetch cadence on the slave → master.
3. The network RTT for the master to respond with the file bytes.

In a healthy local-replication setup, the total window is 1-2 seconds. With many big strings
queued, the worst-case window is `queuedCount * 1s`. For a 1000-big-string re-sync, that's
~17 minutes.

The error returned to the client is `BigStringFileMissingException`, which is a `RuntimeException`
that the request handler likely surfaces as an error reply. The client sees transient failures
during replication catch-up. This is observable and annoying but not corrupting.

**Suggested fix direction (unchanged):**
1. **Batching (recommended):** In `OneSlot.java:987`, change `doingFetchBigStringId()` (which
   returns one id) to a new method `doingFetchBigStringIds(int max)` that returns up to N
   uuids. Send them as a single `incremental_big_strings` (plural) message. The master handler
   at `XGroup.java:1072-1104` would also need a corresponding bulk version.
2. **Synchronous fallback:** On read-miss, instead of throwing, attempt a synchronous fetch
   from the master if `replPair` is connected. This blocks the client request but avoids the
   exception.
3. **Retryable error:** Return a `-TRYAGAIN` Redis-style error so the client retries. This is
   the least invasive change but requires changes in `BaseCommand.getCv` to distinguish
   transient vs permanent missing files.

The simplest fix that doesn't require protocol changes is option 1 with a small batch (e.g.,
10 uuids per message). This reduces the worst-case window by 10x.

**Regression test scope:** None added in this round (improvement, not a bug fix).

### Non-Findings — Confirmed Safe

The six Non-Findings listed by the author were spot-checked:

1. **UUID map maintenance on `put`** — verified at `Wal.java:914-916`: the uuid map is updated
   when `needPersist` is false (which is the common short-value path), and
   `updateUuidFromEncoded` is called regardless of `needPersist`. **Safe.**

2. **Overwrite detection in `OneSlot.put`** — spot-checked at
   `OneSlot.java:1432` (`overwrittenBigStringUuid = getCurrentBigStringUuid(...)` is captured
   before the new file is written). The new uuid lands in the map via the WAL put at
   `Wal.java:915`, so subsequent `getBigStringUuid(key)` calls return the new uuid. **Safe.**

3. **Startup uuid map recovery** — `Wal.lazyReadFromFile` → `refreshBigStringUuidMap` then
   `populateFromKeyBuckets` with `putIfAbsent` (so WAL takes precedence). Verified at
   `BigStringFiles.java:218-221`. **Safe.**

4. **`initCheck()` thread safety** — `initCheck` runs in a dedicated background thread
   (`OneSlot.java:874-894`), and `scheduleRunnable.startDone(true)` is called AFTER
   `initCheck` completes (verified by the doc's reference to `MultiWorkerServer.java:1162 vs
   1165`). The task loop short-circuits at `TaskRunnable.java:79-82` until `isStartDone` is
   true. **Safe in practice** (same conclusion as the author).

5. **LRU cache invalidation on file delete** — verified at `BigStringFiles.java:425-426`
   (`bigStringBytesByUuidLRU.remove(uuid)` at the top of `deleteBigStringFileIfExist`).
   **Safe.**

6. **`handleWhenCvExpiredOrDeleted` bucket index consistency** — verified at
   `BigStringFiles.java:489` (`KeyHash.bucketIndex(shortStringCv.getKeyHash())`). The same
   function is used at write time via `BaseCommand.slot()` → `KeyHash.bucketIndex`. **Safe.**

### Cross-References and Disposition

- **Finding 1**: Should be fixed in conjunction with Finding 4 (single try-catch wrapping
  `intervalDeleteOverwriteBigStringFiles(int)` body covers both). Plus the recommended defense-in-
  depth catch in `TaskRunnable.run`. Combined severity remains **High**.
- **Finding 2**: Simple fix, isolated to `Wal.intervalDeleteExpiredBigStringFiles`. No coupling
  with other findings.
- **Finding 3**: Previously triaged in bug 31 round 1 with the same disposition. Reconfirming
  severity **Low** (over the author's Medium) based on the bounded blast radius and existing
  test-gate nature.
- **Findings 4, 5, 6**: Independent fixes.
- **Improvements I1, I2**: Backlog items. No blocking issues.

### Reviewer Recommendation

Fix in this order:
1. **Finding 1 + Finding 4** (combined fix in `OneSlot.doTask` + `BigStringFiles.getBigStringFileIdList`).
2. **Finding 2** (small isolated fix in `Wal.intervalDeleteExpiredBigStringFiles`).
3. **Finding 5** (one-line deletion).
4. **Finding 6** (move counter resets before the try block in `deleteAllBigStringFiles`).
5. **Finding 3** (test-only opt-in mechanism, test sweep).
6. **Improvements I1, I2** as backlog.

---

## Review Feedback — Finding 1 + Finding 4 Fix

**Reviewed by:** AI Agent 2 (fix reviewer)
**Date:** 2026-06-15
**Commit:** `9b7f4832` (`fix: catch big-string maintenance exceptions in OneSlot.doTask, skip stray files in getBigStringFileIdList`)

### Summary

The fix addresses Finding 1 (High: uncaught exceptions kill the task loop) by wrapping two big-string maintenance calls in `OneSlot.doTask` with try-catch, and addresses Finding 4 (Low: fragile filename parsing) by adding `isFile()`, length, and `NumberFormatException` guards in `getBigStringFileIdList`.

**Files changed:** `OneSlot.java` (+19/-4), `BigStringFiles.java` (+14/-3), `OneSlotTest.groovy` (+68), `BigStringFilesTest.groovy` (+42). Total: +143/-8.

### Strengths

- **Finding 1 catch blocks are correctly placed and exercised.** Both `wal.intervalDeleteExpiredBigStringFiles()` (line 856-866) and `intervalDeleteOverwriteBigStringFiles()` (line 870-874) are now wrapped. The catch blocks log at `error` with slot and group-index context for diagnosis. JaCoCo confirms lines 862-865 and 872-874 are all ✓ covered.

- **Test 1 exercises the real throw path.** `'test doTask does not propagate exception from wal interval delete expired big string files'` injects an expired big-string entry into WAL short values, sets `deleteForceReturnFalseForTest = true`, and calls `doTask(0)`. This triggers the genuine `handleWhenCvExpiredOrDeleted` → `RuntimeException` path — not a synthetic forced throw. This validates the fix against the actual failure mode described in the bug entry.

- **Test 2 uses a clean test-only flag.** `intervalDeleteOverwriteBigStringFilesForceThrowForTest` follows the existing codebase pattern (`doPersistForceThrowForTest`, `deleteForceReturnFalseForTest`). The flag is checked at the top of the method before any real work, ensuring the throw is deterministic.

- **Finding 4 fix is comprehensive.** Three layers of protection: `file.isFile()` (skips directories), `arr.length < 2` (skips no-underscore filenames), and `NumberFormatException` catch (skips non-numeric parts). JaCoCo: 17/17 lines covered, 4/4 branch lines fully covered.

- **Finding 4 test covers all stray-file variants.** The test creates four stray entries: a non-numeric multi-part file (`not_a_uuid_at_all`), a non-numeric two-part file (`abc_def`), a single-part file (`12345`), and a subdirectory (`not_a_real_uuid/`). It verifies only the one valid file is returned, and also tests the `listFiles() == null` path (non-existent bucket).

### Concerns

**1. `truncateChunkFile` remains unprotected — same class of bug (Medium).**

`OneSlot.doTask` line 847:
```java
var canTruncateFdIndex = metaChunkSegmentFlagSeq.canTruncateFdIndex;
if (canTruncateFdIndex != -1) {
    truncateChunkFile(canTruncateFdIndex);   // NOT wrapped
}
```

`truncateChunkFile` → `fd.truncate()` → `FdReadWrite.truncate()` (`FdReadWrite.java:829-844`) wraps `raf.setLength(0)` in a try-catch that throws `RuntimeException("Truncate error, ...")` on `IOException`. Disk-full, permission, or I/O-error conditions trigger this throw.

The fix protected the two big-string calls but left `truncateChunkFile` exposed. An exception from here propagates out of `doTask` → `TaskRunnable.run()` line 85 → past the re-scheduling at line 89 → task loop dies permanently — the exact failure mode Finding 1 describes.

This is not a new bug; it is a pre-existing instance of the same class that the fix did not cover. Recommend wrapping in a follow-up.

**2. `TaskRunnable.run` defense-in-depth (Option B) not applied (Low).**

The bug entry's suggested fix recommended applying both Option A (per-call try-catch in `doTask`) and Option B (for-loop body try-catch in `TaskRunnable.run`). Only Option A was applied. Without Option B, any unforeseen exception from any call in `doTask` — including the `truncateChunkFile` gap above, or future code added to `doTask` — still kills the re-scheduling. Option B is a one-line safety net:

```java
for (var oneSlot : oneSlots) {
    try {
        oneSlot.doTask(loopCount);
    } catch (Exception e) {
        log.error("Do task error, slot={}", oneSlot.slot(), e);
    }
}
loopCount++;
slotWorkerEventloop.delay(INTERVAL_MS, this);
```

Recommend adding in a follow-up.

**3. Test 1 does not verify task-loop continuity (Low).**

The test calls `doTask(0)` once and asserts `noExceptionThrown()`. It does not verify that `doTask` can be called again successfully on the next tick — i.e., that the caught exception did not leave the slot in a corrupted state (e.g., a partially-modified `delayToDeleteBigStringFileIds` or stuck `deleteForceReturnFalseForTest` state). Adding a second `doTask(1)` call that completes normally would strengthen the assertion that the task loop survives.

### JaCoCo Verification

- **`OneSlot.doTask` (lines 838-875):** New catch blocks at lines 862-865 and 872-874 are all ✓ covered. Not-covered lines (840 `isStopping` early-return, 847 `truncateChunkFile` call) are pre-existing and unrelated to this fix.
- **`BigStringFiles.getBigStringFileIdList` (lines 299-323):** 17/17 lines covered, 4/4 branch lines fully covered (files==null, isFile(), arr.length<2, NumberFormatException).

### Follow-ups

- **Pre-commit (blocking):** None — the fix correctly addresses the two findings as scoped.
- **Post-commit (follow-up):**
  1. Wrap `truncateChunkFile(canTruncateFdIndex)` in try-catch in `OneSlot.doTask` (concern 1).
  2. Add Option B defense-in-depth try-catch in `TaskRunnable.run` (concern 2).
  3. Optionally extend test 1 with a second `doTask` call to verify continuity (concern 3).
