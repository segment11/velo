# Bug 28 Persist Layer Read Data Flow Review Round 1

## Scope

Static review of the persist layer **read** data flow, written as round 1 of a new module review series. This focuses on areas not already covered (or explicitly refuted) by the prior persist read-flow rounds:

- bug_13 (round 1) — persisted SCAN cursor accounting
- bug_14 (round 2) — flush clearing slot KV LRU / WAL write offsets
- bug_15 — chunk fd truncation
- bug_16 (round 3) — big-string overwrite during merge, `exists()` LRU symmetry, `refreshBigStringUuidMap`
- bug_17 (round 4) — `get()` LRU expiry contract, `isDeleted` dead-branch, `getExpireAtAndSeqByKey` early return

Reviewed paths (current `main` at `e925cde7`):

- `OneSlot.get()` — full read: WAL → slot KV LRU → keyLoader → segment / inline short-CV
- `OneSlot.getExpireAt()` — TTL read: WAL → slot KV LRU → keyLoader
- `OneSlot.getFromWal()` — WAL helper used by `get()` and `exists()`
- `BaseCommand.getCv()` — sole non-test caller of `OneSlot.get()`; also resolves big-string indirection
- `BigStringFiles.getBigStringBytes()` — big-string file read with LRU cache
- `CompressedValue.decode(...)` — long-form vs short-form decode
- `FdReadWrite.readInnerByBuffer()` — segment / key bucket read with LRU + Zstd

## Finding 1: `BaseCommand.getCv()` silently returns `null` for a missing big-string file (data loss masked as miss)

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/BaseCommand.java:690-704`
- `src/main/java/io/velo/persist/BigStringFiles.java:204-226`

**Code excerpt:**

```java
// BaseCommand.getCv()
if (cv.isBigString()) {
    var oneSlot = localPersist.oneSlot(slot);

    var buffer = ByteBuffer.wrap(cv.getCompressedData());
    var uuid = buffer.getLong();
    var realDictSeq = buffer.getInt();

    var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true);
    if (bigStringBytes == null) {
        return null;          // <-- silent null
    }

    cv.setCompressedData(bigStringBytes);
    cv.setDictSeqOrSpType(realDictSeq);
}
return cv;
```

```java
// BigStringFiles.readBigStringBytes()
var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
if (!file.exists()) {
    log.warn("Big string file not exists, uuid={}, slot={}", uuid, slot);
    return null;                   // <-- only logs at WARN, no further signal
}
```

**Root cause:**

When the read path encounters a `SP_TYPE_BIG_STRING` short-CV (either inline in the key bucket as a `extendBytes` entry or decoded from a chunk segment via PVM), `BaseCommand.getCv()` follows the UUID to the big-string file on disk. If that file is missing, `BigStringFiles.readBigStringBytes()` returns `null` with only a WARN log, and `BaseCommand.getCv()` then returns `null` to the caller — which is the same return value used to signal "key does not exist or is expired".

The metadata referencing the UUID (either WAL `bigStringFileUuidByKey`, inline short-CV in a key bucket cell, or a PVM that points at a segment which itself stores the short-CV) is **not cleaned up** when the underlying big-string file is found missing. Subsequent reads of the same key will keep returning `null`, and `EXISTS` (via `OneSlot.exists()` → `keyLoader.getExpireAtAndSeqByKey()`) will still report the key as **present and not expired**, because `exists()` never resolves the big-string indirection.

**Impact:**

- `GET k` returns `nil` while `EXISTS k` returns `1`. This violates Redis semantics for the contract between `GET`/`EXISTS`.
- Silent data loss: from the user's perspective the value is gone, but there is no error, no metric, and no remediation; only a WARN log on the first read.
- The orphan metadata also blocks any future `intervalDeleteOverwriteBigStringFiles` reasoning about that UUID — the WAL/persisted bucket still references a UUID that no file exists for.

**How can this actually be reached?**

The "happy" big-string lifecycle is supposed to guarantee the file exists while any reference does, but several plausible paths can desync it:

1. Manual / external removal under `persist/slot-N/big-string/<bucketIndex>/`.
2. A failed `writeBigStringBytes` after the WAL `V` was already accepted into `delayToKeyBucketShortValues` — `OneSlot.put` throws, but a slave replaying the binlog or a master that wrote the WAL successfully before the IO failure can still leave a dangling reference.
3. Disk full / partial replication transfer of big-string content (bug 8 round 5 already discussed similar issues on the repl side).

In all these cases, the read path has no way to escalate the inconsistency.

**Suggested fix direction:**

A missing big-string file is server-side data corruption (the key's metadata still claims it exists), not a normal "key not found". Returning `null` here is the same code path the protocol uses for "absent", so the client receives `nil` and cannot distinguish "you deleted it" from "we lost your data". The fix should therefore **raise a Redis error reply** rather than mask the issue.

Concretely:

1. Introduce a dedicated runtime exception (e.g. `BigStringFileMissingException extends RuntimeException`) thrown from `BaseCommand.getCv()` when the big-string file is missing — same pattern as `DictMissingException` / `CannotReadException`, which the top-level command dispatcher already translates into `Reply.error("ERR ...")`. The client then sees something like:

   ```
   -ERR data inconsistency: big string file missing for key=<k>
   ```

2. Emit `log.error` plus a Prometheus counter (e.g. `big_string_missing_total`) so operators are alerted without grepping logs.

3. Verify that the dispatcher / command framework converts the new exception into a RESP `-ERR` reply (and not into a connection close); add a test that exercises the path by deleting a big-string file under `persist/slot-N/big-string/...` after a successful write.

4. **Do not** auto-delete the dangling metadata in the same call. Surfacing the inconsistency lets ops decide remediation (restore from backup, force `DEL`, etc.); silently calling `removeDelay(key, ...)` would replace one form of silent data loss with another.

Pseudocode:

```java
var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true);
if (bigStringBytes == null) {
    log.error("Big string file missing, key={}, uuid={}, slot={}", key, uuid, slot);
    bigStringMissingCounter.inc();
    throw new BigStringFileMissingException("big string file missing for key=" + key);
}
```

If we later decide to make `EXISTS` agree with `GET`, the cleanest follow-up is to have `OneSlot.exists()` also resolve the big-string indirection (or check `bigStringFiles` file presence) for big-string-typed entries — but that is a separate change and should land in its own commit.

## Finding 2: `kvLRUHitTotal` / `kvLRUCvEncodedLengthTotal` are incremented for WAL hits, masking real slot-KV LRU effectiveness

**Severity:** Low (metric correctness)

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1463-1497` (`getExpireAt`)
- `src/main/java/io/velo/persist/OneSlot.java:1533-1565` (`get`)
- `src/main/java/io/velo/persist/OneSlot.java:845-847` (counter declarations)

**Code excerpt:**

```java
// OneSlot.get() lines 1537-1547
var cvEncodedFromWal = getFromWal(key, bucketIndex, isExpiredFlagArray);
if (cvEncodedFromWal != null) {
    kvLRUHitTotal++;                                   // <-- WAL hit, not LRU
    kvLRUCvEncodedLengthTotal += cvEncodedFromWal.length;
    ...
    return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedFromWal), null);
}
```

```java
// OneSlot.getExpireAt() lines 1468-1493
var v = targetWal.getV(key);
if (v != null) {
    kvLRUHitTotal++;                                   // <-- also WAL hit
    kvLRUCvEncodedLengthTotal += v.cvEncoded().length;
    ...
}
// from lru cache
...
if (cvEncodedBytesFromLRU != null) {
    kvLRUHitTotal++;                                   // <-- the real LRU hit
    kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;
    ...
}

kvLRUMissTotal++;                                       // <-- only the keyLoader fall-through is a "miss"
```

**Root cause:**

`kvLRUHitTotal` / `kvLRUCvEncodedLengthTotal` / `kvLRUMissTotal` are named after, and exported in metrics as, slot-KV-LRU effectiveness counters (see `collect()` at line ~2714: `slot_kv_lru_current_count_total`, `slot_kv_lru_hit_total`, ...). However both `get()` and `getExpireAt()` increment the `Hit` counter on every WAL hit too. Since WAL hits dominate hot keys, the published hit rate is inflated and tells you almost nothing about whether the slot KV LRU is actually working.

`kvLRUMissTotal` is only incremented in the `keyLoader` fall-through, so the ratio `kvLRUHitTotal / (kvLRUHitTotal + kvLRUMissTotal)` is effectively `(WAL hits + LRU hits) / (WAL hits + LRU hits + keyLoader reads)`, not "fraction of LRU lookups that hit". The slot KV LRU could be cold and useless and the metric would still look healthy.

**Impact:**

- Operators tuning `kvByWalGroupIndexLRU` sizing (which directly drives heap usage) cannot use the published hit rate to decide.
- Regressions where the LRU is silently never populated (e.g., a future change to the persist path) would not show up — WAL hits would carry the metric.
- Not a correctness bug for stored data.

**Suggested fix direction:**

Either (a) rename the counter or split it (`walHitTotal` + `lruHitTotal`) so the WAL hit is reported separately, or (b) increment only inside the LRU branch:

```java
// inside the lru branch only:
if (cvEncodedBytesFromLRU != null) {
    kvLRUHitTotal++;
    kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;
    ...
}
```

Option (a) preserves the WAL-hit observability; option (b) is the minimal change that makes the counter match its name.

## Finding 3: `BigStringFiles.getBigStringBytes(uuid, bucketIndex, keyHash)` 3-arg overload always reads from disk but never refreshes the LRU

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/BigStringFiles.java:170-194`
- `src/main/java/io/velo/command/XGroup.java:1026`

**Code excerpt:**

```java
public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash) {
    return getBigStringBytes(uuid, bucketIndex, keyHash, false);   // <-- doLRUCache=false
}

public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash, boolean doLRUCache) {
    var bytesCached = bigStringBytesByUuidLRU != null ? bigStringBytesByUuidLRU.get(uuid) : null;
    if (bytesCached != null) {
        return bytesCached;
    }

    var bytes = readBigStringBytes(uuid, bucketIndex, keyHash);
    if (bytes != null && doLRUCache && bigStringBytesByUuidLRU != null) {
        bigStringBytesByUuidLRU.put(uuid, bytes);                   // <-- only the 4-arg true caller refreshes
    }
    return bytes;
}
```

```java
// XGroup.java:1026 — the only non-BaseCommand reader path
var bigStringBytes = bigStringFiles.getBigStringBytes(uuid, s.bucketIndex(), s.keyHash());
```

**Root cause:**

The 3-arg overload (used by `XGroup` and any future caller that forgets the cache flag) does a full file read but never seeds the LRU. The LRU is therefore only populated by `BaseCommand.getCv` (which calls with `doLRUCache=true`). All other read sites — including replication-side big-string lookups in `XGroup` — pay the disk cost on every call, even for hot keys.

This isn't strictly incorrect, but it is surprising: the API name does not hint that the default overload is "no-cache". A caller reading "get big string bytes" reasonably expects it to behave like a normal cached read.

**Impact:**

- Replication and other internal flows pay full disk reads on every big-string fetch. For hot big-string keys under heavy replication / merge load, this is wasted I/O.
- The LRU appears underused under workloads dominated by `XGroup`-style readers, which makes Finding 2's metric problem worse: operators may conclude the LRU is too small when in fact it is simply not being seeded by the right callers.

**Suggested fix direction:**

Either flip the default to `doLRUCache=true` (the 3-arg overload then matches its name), or split into two clearly named methods (`getBigStringBytesCached` / `getBigStringBytesNoCache`). If we keep the existing semantics intentionally for the replication path (to avoid polluting the LRU with replay-only data), document that explicitly at the overload site.

## Finding 4: `CompressedValue.decode(...)` keyHash mismatch defense is asymmetric between long-form and short-form

**Severity:** Low (defensive code observation, not a known data corruption)

**Files:**

- `src/main/java/io/velo/CompressedValue.java:854-898`

**Code excerpt:**

```java
public static CompressedValue decode(io.netty.buffer.ByteBuf nettyBuf, byte[] keyBytes, long keyHash) {
    var cv = new CompressedValue();
    var firstByte = nettyBuf.getByte(0);
    if (firstByte < 0) {
        // SHORT-FORM (number / short string / SP_TYPE_*): no keyHash on the wire, no check.
        cv.dictSeqOrSpType = firstByte;
        nettyBuf.skipBytes(1);
        cv.seq = nettyBuf.readLong();
        cv.expireAt = nettyBuf.readLong();
        cv.compressedData = new byte[nettyBuf.readableBytes()];
        nettyBuf.readBytes(cv.compressedData);
        return cv;                                            // <-- returns without any key consistency check
    }

    cv.seq = nettyBuf.readLong();
    cv.expireAt = nettyBuf.readLong();
    cv.keyHash = nettyBuf.readLong();
    cv.dictSeqOrSpType = nettyBuf.readInt();
    ...
    if (keyHash != 0 && cv.keyHash != keyHash) {              // <-- only long-form path validates
        ...
        throw new IllegalStateException("Key hash not match, ...");
    }
    ...
}
```

**Root cause:**

The long-form CV encoding has `keyHash` on the wire and `decode()` verifies it matches the caller-provided `keyHash`. The short-form encodings (`encodeAsShortString`, `encodeAsNumber`) deliberately omit `keyHash` to save bytes — there is no field to check against, so `decode()` returns the short-form CV without validation.

This is **by design** for short-form bytes living in a key bucket cell (where the cell metadata already keyed by hash provides the binding). But the same short-form bytes can also flow through:

- the slot KV LRU (`kvByWalGroupIndexLRU`), keyed by raw `key` string;
- WAL `V.cvEncoded`, keyed by `key` in the WAL maps;
- segment storage (after a sub-block decompression) — the key bytes are written immediately before the CV in the segment, but the segment-read path in `OneSlot.get()` already does a `Arrays.equals(keyBytesRead, keyBytes)` check (line 1613) before decoding, so the segment path is safe.

For the LRU and WAL paths, if a future bug ever puts a wrong `cvEncoded` under a given key (e.g., a regression in `Wal.put`, `Wal.putFromX`, or `lru.put`), short-form values would be silently misattributed; long-form values would throw `IllegalStateException` at decode time.

**Impact:**

- No known production data corruption today. This is purely a defense-in-depth observation.
- If we ever introduce a `Wal.V` mis-association regression similar to bug 11/12, the long-form CVs will fail loudly and the short-form CVs will fail silently.

**Suggested fix direction:**

Optional. The cheapest improvement is to keep `cv.keyHash = keyHash` for the short-form decode (assigning the caller-provided hash) so that any downstream consumer can at least see a hash on the CV. A stricter approach is to add a per-encoding sanity guard, but that would change on-disk format and is not worth it for a low-impact defensive concern.

## Summary

| Finding | Severity | Status |
|---------|----------|--------|
| 1 — `BaseCommand.getCv()` silently returns `null` for a missing big-string file; `GET` / `EXISTS` disagree | Medium | Needs reviewer verification |
| 2 — `kvLRUHitTotal` inflated by WAL hits; LRU hit-rate metric meaningless | Low (metric) | Needs reviewer verification |
| 3 — `BigStringFiles.getBigStringBytes` 3-arg default is no-cache, surprising | Low | Needs reviewer verification |
| 4 — `CompressedValue.decode` short-form path skips keyHash validation | Low (defensive) | Needs reviewer verification |

## Next Steps

This is round 1. Per the bug-review workflow in `CLAUDE.md`:

1. AI agent 2 (reviewer) verifies each finding against the current `main` and appends review notes here.
2. For confirmed findings, the chosen implementer writes a failing test first, lands the minimal fix in its own commit, and inspects JaCoCo for the touched lines.
3. Another agent appends a Review Feedback section per commit.

## Review Feedback — Finding 1 Fix Commit `aa5ee729`

### Summary of the Fix

`aa5ee729 fix: throw BigStringFileMissingException when big-string file is missing instead of silent null` introduces:

- New `io.velo.persist.BigStringFileMissingException extends RuntimeException` — mirrors the `DictMissingException` / `CannotReadException` pattern.
- `BaseCommand.getCv()` (line 699) now throws `BigStringFileMissingException("data inconsistency: big string file missing for key=" + key)` instead of returning `null` when the big-string file is missing.
- `BigStringFiles.readBigStringBytes()` upgraded the `log.warn` to `log.error` and increments a new `bigStringMissingFileTotal` counter.
- The counter is exported via `collect()` as `big_string_missing_file_total` (Prometheus).
- `@TestOnly clearLRUCache()` helper added to make the new test deterministic across both metadata-only and file-deletion scenarios.
- `BaseCommandTest` updated: old "expected `null`" assertion replaced with `thrown(BigStringFileMissingException)`, plus a new scenario that writes a big-string successfully, deletes the file externally, clears the LRU, and verifies the exception is thrown.

### Strengths

- Exception type matches the suggested name and pattern. It is unchecked, so it propagates naturally up to `RequestHandler` line 526 / 583 (`catch (Exception e) { return new ErrorReply(e.getMessage()); }`) → the client sees `-ERR data inconsistency: big string file missing for key=<k>`. Confirmed by reading the dispatcher path; matches Finding 1's intent.
- The fix correctly does **not** auto-delete the dangling metadata in the same call, which preserves operator visibility and avoids replacing one silent-data-loss path with another.
- Two test scenarios cover both common reach paths: metadata-without-file (interim WAL state / replication race) and file-deleted-after-write (external corruption).
- Counter and `log.error` give ops the runtime signal that was missing in the bug report.
- JaCoCo (`build/reports/jacocoHtml/`) shows the new lines as fully covered:
  - `BaseCommand.java:704` (throw): `fc`.
  - `BigStringFiles.java:218-220` (counter increment + error log + `return null`): all `fc`.

### Concerns

1. **Metric export not exercised.** `BigStringFiles.java:79` (`map.put("big_string_missing_file_total", ...)` inside `collect()`) is marked `nc` by JaCoCo — no test calls `collect()` on this slot's `BigStringFiles`. The counter logic itself IS covered, but if the metric name or type were broken in `collect()`, no test would catch it. Low risk (it's a single map insert), but worth a one-line follow-up test or piggy-backing on an existing `BigStringFiles.collect()` test.

2. **`exists()` still disagrees with `get()`.** After the fix:
   - `GET k` → `-ERR data inconsistency: big string file missing for key=<k>`
   - `EXISTS k` → `1`

   The disagreement is now *loud* instead of *silent*, which is a real improvement, but the underlying contract violation between `GET` and `EXISTS` remains. The doc explicitly defers this to a separate change ("If we later decide to make `EXISTS` agree with `GET`, the cleanest follow-up is to have `OneSlot.exists()` also resolve the big-string indirection") — that follow-up is not in this commit. Tracking it as a known residual.

3. **No coverage of the binary-safe key in the error message.** `key=" + key` will produce mojibake when the key contains non-UTF-8 bytes. The dispatcher then ships that mojibake to the client. Cosmetic, but `Wal.keyString(...)` is used elsewhere for the same purpose; consider using it here for consistency.

4. **No test on the `RequestHandler` translation step.** The Finding 1 suggested fix called out "Verify that the dispatcher / command framework converts the new exception into a RESP `-ERR` reply (and not into a connection close)". The fix relies on the generic `catch (Exception e)` block in `RequestHandler`, which does convert it. There is no test that exercises `GET` → request handler → big-string-missing → `-ERR` end-to-end. Low risk because the catch is generic, but a one-shot integration test would lock the contract in.

### Pre-commit / Post-commit Follow-ups

- Add a `BigStringFiles.collect()` test that asserts the `big_string_missing_file_total` key appears in the returned map after a miss has been recorded.
- Optionally use `Wal.keyString(Wal.keyBytes(key))` (or the helper that already exists in this file) inside the exception message to handle non-UTF-8 keys consistently.
- Track the `GET`/`EXISTS` symmetry as residual — either pick it up as a follow-up finding in the next round or open it as Finding 5 here for AI agent 2 to triage.

## Review Feedback — Finding 2 Fix Commit `457d1355`

### Summary of the Fix

`457d1355 fix: split WAL and LRU hit counters so slot_kv_lru metrics reflect real LRU effectiveness` takes the **option (a)** path from the suggested fix direction (split the counters, preserve WAL-hit observability):

- Introduces two new counters in `OneSlot`: `kvWalHitTotal`, `kvWalCvEncodedLengthTotal` (both `@VisibleForTesting`).
- `OneSlot.get()` WAL hit branch (line 1541-1542) now increments `kvWalHitTotal` / `kvWalCvEncodedLengthTotal` instead of `kvLRUHitTotal` / `kvLRUCvEncodedLengthTotal`. Same change at `OneSlot.getExpireAt()` WAL hit branch (line 1472-1473).
- `collect()` (line 2724-2729) exports `slot_kv_wal_hit_total` and `slot_kv_wal_cv_encoded_length_avg`, gated by `kvWalHitTotal > 0` (symmetric with the existing LRU gate).
- New test in `OneSlotTest`: "test kvLRUHitTotal only counts true LRU hits not WAL hits" walks both `get()` and `getExpireAt()` paths through a WAL hit phase and an LRU hit phase, then verifies `collect()` exports both metric families.

### Strengths

- The split correctly restores the semantic of `kvLRUHitTotal`. After the fix, the ratio `kvLRUHitTotal / (kvLRUHitTotal + kvLRUMissTotal)` represents what the metric name claims: the fraction of post-WAL lookups served by the slot KV LRU. `kvLRUMissTotal` keeps its meaning ("WAL miss → LRU miss → fell through to keyLoader"); no semantic drift on the existing counter.
- WAL-hit observability is preserved (option a), which is the better trade-off — operators tuning the slot KV LRU now have an independent signal for how hot WAL is.
- Test covers all four cells of the matrix (WAL hit / LRU hit × `get` / `getExpireAt`) and the metric export, in one self-contained Spock spec. Uses `putKvInTargetWalGroupIndexLRU(...)` (already `@TestOnly`) to seed the LRU after wiping WAL — same pattern used by the bug 14 flush test.
- JaCoCo (`build/reports/jacocoHtml/io.velo.persist/OneSlot.java.html`) shows full coverage of all touched lines:
  - Counter increments at 1095-1096 and 1151-1152: `fc`.
  - `collect()` body at 2127-2129: `fc`.
  - The `if (kvWalHitTotal > 0)` check at 2126 shows `pc bpc` (1 of 2 branches missed — the `false` branch is unreached because the test always seeds WAL hits before calling `collect()`). This is symmetric with the existing LRU export gate; not a fix regression.

### Concerns

1. **No WAL miss counter.** The fix introduces `kvWalHitTotal` but not a `kvWalMissTotal`. Operators can compute `total_gets - kvWalHitTotal - kvLRUHitTotal - kvLRUMissTotal` to derive WAL miss, but that requires correlating multiple counters. Finding 2 didn't explicitly ask for a miss counter, so this is a small gap rather than a defect. Cheap to add later if needed.

2. **Test-only setter pattern leaks via direct field access.** The test mutates `oneSlot.kvWalHitTotal = 5` and `oneSlot.kvWalCvEncodedLengthTotal = 100` directly to drive `collect()`. That works because the fields are package-private `@VisibleForTesting`, but it bypasses the normal increment path. A minor risk: if the increment site changes shape (e.g., wraps in a method), the test stays green even if the metric export breaks. Low priority — the dominant scenarios (phases 1-4) exercise real increments end-to-end.

3. **No assertion on `kvLRUCvEncodedLengthTotal` in the LRU-hit phase.** The test asserts `kvLRUHitTotal == 1` / `2` but does not assert the encoded-length accumulator. Symmetric oversight to the existing pre-fix test patterns; not introduced by this fix. Could be added in a follow-up.

4. **`getExpireAt()` LRU-hit path still has the asymmetric counter accounting it had before** for the LRU case: it increments `kvLRUHitTotal` / `kvLRUCvEncodedLengthTotal` inside the LRU branch (line 1487-1489). This part was already correct pre-fix and remains correct post-fix. Confirmed by reading lines 1483-1497.

### Pre-commit / Post-commit Follow-ups

- Optional: add a `kvWalMissTotal` counter for completeness — exported under `slot_kv_wal_miss_total`. Useful for operators reasoning about read-pattern distribution.
- Optional: rename `kvLRUMissTotal` → `kvLRUMissAfterWalMissTotal` (or document it inline) to make the semantics explicit ("counts only post-WAL-miss lookups that also missed the LRU"). Cosmetic but useful since the meaning is non-obvious to readers.
- Add a one-line assertion in the existing test that `kvLRUCvEncodedLengthTotal` accumulates correctly across the two LRU hits (`== 2 * cv.encode().length` or similar), so any future regression on the encoded-length accumulator is caught.

## Review Feedback — Finding 3 Fix Commit `a5a515c8`

### Summary of the Fix

`a5a515c8 fix: make 3-arg getBigStringBytes seed LRU by default so XGroup reads benefit from cache` takes the **option 1** path from the suggested fix direction (flip the default to `doLRUCache=true`):

- Single production line change: `BigStringFiles.getBigStringBytes(uuid, bucketIndex, keyHash)` now forwards to `getBigStringBytes(uuid, bucketIndex, keyHash, true)`.
- New test in `BigStringFilesTest`: "test 3-arg getBigStringBytes seeds LRU after disk read" reads a UUID via the 3-arg overload, then verifies a subsequent read does not increment `readFileCountTotal` — clean observable proxy for "served from cache, not disk".
- Existing assertion order in `BigStringFilesTest` swapped (the `getBigStringBytes(1L, 1, 1L) == null` line was moved before the valid read) so that the null branch isn't accidentally short-circuited by a cached entry from the prior call.

### Strengths

- Minimal, focused change. One production line; everything else is test.
- Matches Finding 3's primary recommendation (flip the default). The alternative (split into two named methods) was correctly judged not worth the surface-area churn for a single non-test caller.
- The new test exercises the precise behavior change: cache miss → cache populated → subsequent call hits cache without disk I/O. `readFileCountTotal` is a fair observable.
- Reuses `clearLRUCache()` — the `@TestOnly` helper introduced in Finding 1's commit (`aa5ee729`) is now load-bearing for two tests, which validates the helper.
- JaCoCo (`build/reports/jacocoHtml/io.velo.persist/BigStringFiles.java.html`) shows line 156 (the new `true` literal) as `fc`.

### Concerns

1. **Replication cache pollution accepted without mitigation.** The only non-test caller is `XGroup.incremental_big_string` (now at `XGroup.java:1053`), the master-side handler for a slave's "fetch big-string by UUID" replication request. This is not a hot user read path: each UUID is normally fetched once per slave during catch-up. The fix unconditionally seeds the LRU on this call, which can evict hot user-facing big-strings under heavy replication load (e.g., a new slave catching up a slot with many big-strings). The original doc explicitly flagged this tradeoff ("...intentionally for the replication path (to avoid polluting the LRU with replay-only data)..."). The fix accepts it without (a) a doc-comment justifying the choice, or (b) splitting the API so replication can opt out. Worth tracking as a known residual that may need a follow-up if replication-driven eviction shows up in metrics.

2. **No `XGroup` end-to-end test for the new caching behavior.** The fix's motivation was to make `XGroup` reads benefit from the cache, but no `XGroupTest` change verifies the cache behavior on that path. The new `BigStringFilesTest` test only exercises the direct API. If a future refactor changes the call site to skip the cache (e.g., adds a 4-arg call with `false`), no test will catch the regression on the XGroup path specifically.

3. **Pre-existing test had to be reordered.** Line 41-42 swap in `BigStringFilesTest` ("the `null` assertion moved before the valid read") indicates that pre-existing tests written against the previous "no-cache" default are now order-sensitive. There are no other obvious order swaps in this commit, but other tests in this project that read the same UUID twice via the 3-arg overload now silently change their fault domain — the second read no longer exercises the disk path. Worth a one-shot grep / audit before considering this fix complete.

4. **No JavaDoc on the 3-arg overload.** The method (`BigStringFiles.java:155-157`) has no `/** ... */` block at all, so there is no doc-comment to update. This is pre-existing tech debt, not introduced by the fix, but the fix is a natural opportunity to add a one-line JavaDoc stating "reads from LRU if present, otherwise reads from disk and seeds the LRU; equivalent to the 4-arg overload with `doLRUCache=true`". Without it, the next reader has to infer the behavior from the test or from the delegation target.

### Pre-commit / Post-commit Follow-ups

- Add a one-line JavaDoc on the 3-arg overload describing the cache behavior, so the API contract is documented at the call site.
- Optional audit: grep for existing tests that read the same UUID twice via the 3-arg overload (`getBigStringBytes(.+, .+, .+)`). If any of them were relying on the prior no-cache semantics to validate disk-read paths, they now silently degrade — consider switching those tests to a `clearLRUCache()` between reads.
- Consider opening a follow-up issue tracking the replication-cache-pollution tradeoff. If `XGroup.incremental_big_string` ends up evicting user-facing big-strings under real replication load, the right fix is to give `XGroup` an explicit no-cache overload (or a `doLRUCache=false` call) — not to revert this fix.

## Review Feedback — Finding 4 Fix Commit `ce363e24`

### Summary of the Fix

`ce363e24 fix: assign caller-provided keyHash on short-form CV decode so downstream consumers see correct hash` lands the minimal defense-in-depth improvement from the suggested fix direction:

- Single production line at `CompressedValue.java:840`: after the short-form branch reads `dictSeqOrSpType`, `seq`, `expireAt`, and `compressedData`, it now assigns `cv.keyHash = keyHash` (the caller-provided hash) before returning.
- New test "test short-form decode preserves caller-provided keyHash" covers three scenarios:
  1. Short string encoding (`SP_TYPE_SHORT_STRING`) → `cv.keyHash` matches caller.
  2. Number encoding (`SP_TYPE_NUM_INT`) → `cv.keyHash` matches caller.
  3. Edge case: `keyHash=0`, `keyBytes=null` → `cv.keyHash` stays `0`.

### Strengths

- One production line; tight blast radius. Matches Finding 4's primary recommendation exactly.
- Covers both flavors of short-form (`encodeAsShortString` and `encodeAsNumber`) plus the degenerate null-input case.
- **Real downstream win for re-encode.** `CompressedValue.encode()` always emits long-form (writes `keyHash` to the wire at line 775). Before this fix, a short-form-decoded CV that flowed through `encode()` would emit `keyHash=0` on the wire, which the long-form `decode()` cannot validate later. After this fix, re-encoded bytes carry the caller-asserted hash. This is more than a "see correct hash for downstream consumers" — it tightens the LRU-store path in `OneSlot.get()` (line 1619: `lru.put(key, cv.encode())`), so future LRU hits decode into a long-form CV with the correct hash.
- JaCoCo (`build/reports/jacocoHtml/io.velo/CompressedValue.java.html`) shows line 840 as `fc`.

### Concerns

1. **Asymmetry with the long-form recompute logic.** The long-form path (line 849-851) recomputes `keyHash` from `keyBytes` when the caller passes `keyHash=0 && keyBytes != null`:
   ```java
   if (keyHash == 0 && keyBytes != null) {
       keyHash = KeyHash.hash(keyBytes);
   }
   ```
   The short-form path does **not** do this recompute. So `decode(shortFormBytes, "my-key".getBytes(), 0L)` now assigns `cv.keyHash = 0`, while the same call on long-form bytes would assign a non-zero hash. The fix tightens the gap from "no keyHash at all" to "keyHash from caller verbatim", but it does not achieve full parity with long-form. The new test confirms the gap by asserting the keyHash-stays-zero behavior in scenario 3 — but the more interesting case (`keyHash=0`, `keyBytes != null`) is not exercised, so any future contributor reading the test could reasonably conclude the parity is intentional.

2. **No validation, only assignment.** Finding 4 explicitly noted that the long-form path *validates* `keyHash != 0 && cv.keyHash != keyHash` (throws `IllegalStateException` on mismatch), and the short-form path has nothing to validate against because it omits the hash on the wire. The fix correctly does not invent a validation; assignment is the right minimal step. Worth being explicit in this review that this remains a defense gap, not a check.

3. **`cv.keyHash` is now caller-trusted, not wire-trusted, for short-form.** Long-form: `cv.keyHash = nettyBuf.readLong()` (wire), then cross-checked against caller. Short-form: `cv.keyHash = keyHash` (caller), no cross-check possible. The trust model is different by design — but the per-branch comment in the file does not call this out, so a future reader might mistakenly think both branches use the same source. A one-line comment alongside the new assignment ("short form omits keyHash on the wire; trust the caller-provided hash") would prevent that confusion.

4. **No coverage of the cross-LRU side effect.** The doc-level "real downstream win" above is asserted by reasoning, not by a test. A regression test that round-trips through the slot KV LRU (`OneSlot.get()` → `lru.put(key, cv.encode())` → next-call LRU hit → `CompressedValue.decode(buf, keyBytes, keyHash)` succeeds the long-form `cv.keyHash != keyHash` check at line 853) would lock in the benefit. Today, if a future change reintroduces `cv.keyHash = 0`, the long-form mismatch would only fire as an `IllegalStateException` at runtime, not at test time.

5. **Pre-existing tech debt left untouched.** The `// why ? todo: check` comment at line 859 and the `log.warn` followed by `throw` at lines 860-866 (the warn log is dead because we throw immediately after) are still there. Not the scope of this fix, but worth flagging since this is the area the review touched.

### Pre-commit / Post-commit Follow-ups

- Add a one-line comment at line 840 explaining the trust-model difference between short-form and long-form (caller-provided vs wire-derived `cv.keyHash`).
- Add a test case for `decode(shortFormBytes, keyBytes!=null, keyHash=0)` so the asymmetry with the long-form recompute is locked in (either as "intentionally not recomputed" or as a follow-up bug to also recompute on the short-form path for parity).
- Optional: add an end-to-end test that exercises the LRU round-trip (`OneSlot.get` → LRU put → LRU hit → re-decode) to verify the re-encoded form carries the correct `keyHash` and survives the long-form mismatch check. This lifts Finding 4 from defensive observation to verified behavior.
- Out of scope for this fix but worth filing: the `// why ? todo: check` + `log.warn` / `throw` dead-log at line 859-866 should be cleaned up in a follow-up.

## Reviewer Notes (AI agent 2)

Reviewed against the current workspace on 2026-05-13. I verified the cited read-flow code paths in
`BaseCommand`, `OneSlot`, `BigStringFiles`, `KeyLoader`, `XGroup`, and `CompressedValue`.

### Finding 1 Verification - Confirmed

`BaseCommand.getCv()` resolves big-string metadata by calling
`oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true)` and returns `null` if the file
bytes are missing. `BigStringFiles.readBigStringBytes()` logs only a WARN for a missing file and also returns `null`.
That `null` is indistinguishable from an absent or expired key to command callers.

`BaseCommand.exists()` delegates to `OneSlot.exists()`, and `OneSlot.exists()` checks WAL metadata or
`keyLoader.getExpireAtAndSeqByKey(...)`; it does not resolve the big-string file. Therefore a persisted or WAL-resident
big-string metadata entry whose file has disappeared can make `GET` return nil while `EXISTS` still returns true.

One nuance: in the direct `OneSlot.put()` big-string path, the file write happens before `targetWal.put(...)`, so a
plain `writeBigStringBytes(...)` failure should throw before the local WAL entry is accepted. The manual deletion and
replication/transfer desynchronization paths remain sufficient to reach the confirmed read-path inconsistency.

**Verdict:** Confirmed. This should be fixed as a real read-consistency bug, with a focused regression test that removes
the big-string file after metadata exists and demonstrates the current `GET`/`EXISTS` disagreement.

### Finding 2 Verification - Confirmed

`OneSlot.getExpireAt()` increments `kvLRUHitTotal` and `kvLRUCvEncodedLengthTotal` on WAL hits before it ever checks
`kvByWalGroupIndexLRU`. `OneSlot.get()` does the same through `getFromWal(...)`. The same counters are also incremented
for actual slot-KV-LRU hits, and `kvLRUMissTotal` is incremented only when the read falls through to `keyLoader`.

The exported metrics are named and grouped as slot-KV-LRU metrics:
`slot_kv_lru_hit_total`, `slot_kv_lru_miss_total`, `slot_kv_lru_hit_ratio`, and
`slot_kv_lru_cv_encoded_length_avg`. As implemented, the hit ratio is WAL+LRU hits over WAL+LRU+keyLoader reads, not
actual LRU lookup effectiveness.

**Verdict:** Confirmed as a metric correctness bug. The least disruptive fix is to count only true LRU hits in the
existing LRU counters and add separate WAL-hit counters if WAL observability is needed.

### Finding 3 Verification - Confirmed With Wording Correction

The 3-arg overload calls `getBigStringBytes(uuid, bucketIndex, keyHash, false)`. The 4-arg method always checks
`bigStringBytesByUuidLRU` first, so the 3-arg overload does not "always reads from disk" in the literal sense: it can
return an entry already cached by another caller.

The issue is still real. On an LRU miss, the 3-arg overload reads the file and does not populate the LRU because
`doLRUCache` is false. `XGroup.incremental_big_string(...)` is the only non-`BaseCommand` production caller and uses
this 3-arg overload, so repeated replication-side reads that are not already warmed by command reads will continue to
pay disk I/O and will not seed the cache.

**Verdict:** Confirmed as a low-severity API/cache-behavior issue, with the wording adjusted to "does not populate the
LRU after a disk read." If no-cache behavior is intentional for replication, the overload should be renamed or documented
explicitly.

### Finding 4 Verification - Confirmed, Impact Is Stronger Than Stated

`CompressedValue.decode(...)` leaves `cv.keyHash` unset on the short-form path because the short-form bytes contain only
type, sequence, expire time, and payload. The long-form path reads `keyHash` from the encoded bytes and validates it
against the caller-provided hash.

The asymmetry is therefore real. The suggested lightweight improvement, assigning the caller-provided `keyHash` to the
decoded `CompressedValue` on the short-form path, is compatible with the current format because it does not add bytes.

This is not only theoretical in the current code. `XGroup.s_short_string(...)` decodes replicated short-string
`valueBytes` with `CompressedValue.decode(valueBytes, Wal.keyBytes(key), keyHash)`, stores the resulting `CompressedValue`,
and later computes `bucketIndex = KeyHash.bucketIndex(cv.getKeyHash())` before calling `targetOneSlot.put(...)`. For
short-form values, `cv.getKeyHash()` remains `0`, so this downstream path appears to lose the record-level key hash that
`KeyLoader.decodeShortStringListFromBuf(...)` had already supplied.

**Verdict:** Confirmed. The finding should be upgraded from a purely defensive observation to a concrete correctness risk
for short-form decoded values that are later re-used as `CompressedValue` objects. A focused test should assert that
short-form decode preserves the caller-provided key hash, and a replication short-string test should cover the
`XGroup.s_short_string(...)` path.

## Reviewer Summary

| Finding | Severity | Reviewer status | Notes |
|---------|----------|-----------------|-------|
| 1 - Missing big-string file makes `GET` return nil while `EXISTS` can remain true | Medium | **Confirmed** | Direct local write-failure path is less likely than stated, but missing-file inconsistency is real |
| 2 - WAL hits inflate slot-KV-LRU metrics | Low | **Confirmed** | Existing metric names do not match counted events |
| 3 - 3-arg big-string read does not seed LRU | Low | **Confirmed with wording correction** | It checks existing cache first, but does not populate cache after disk read |
| 4 - Short-form decode omits caller keyHash | Low, likely higher for replication path | **Confirmed and refined** | Current `XGroup.s_short_string(...)` appears to rely on the missing keyHash |
