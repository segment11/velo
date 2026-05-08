# Bug 16 Persist Layer Data Flow Review Round 3

## Scope

Static review of the persist layer data flow following previous rounds (bug 11-15). This round focuses on the merge write path, the big-string lifecycle during merge, and the read path consistency between `get()` and `exists()`.

Reviewed paths:

- `KeyBucketsInOneWalGroup.putPvmListToTargetBucket()` — merge write path for persisted key buckets
- `OneSlot.get()` and `OneSlot.exists()` — read path consistency
- `Wal.refreshBigStringUuidMap()` — big-string UUID map rebuild on reload

## Finding 1: Big-string file not cleaned up when entry is overwritten during merge

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:236-248`
- `src/main/java/io/velo/persist/KeyBucketsInOneWalGroup.java:252-269`

**Code excerpt:**

```java
// KeyBucketsInOneWalGroup.putPvmListToTargetBucket() — lines 236-248
keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
    if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
        cvExpiredOrDeleted(key, valueBytes);  // Only called for EXPIRED entries
    }
    // ... add to map
});

// Lines 252-269 — overwrite handling
for (var pvm : pvmListThisBucket) {
    if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
        var existPvm = map.remove(pvm.key);
        if (existPvm != null) {
            cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);  // Called for deletes
        }
    } else {
        var existPvm = map.put(pvm.key, pvm);
        if (existPvm != null) {
            // OVERWRITE case — no cvExpiredOrDeleted call!
            assert existPvm.seq <= pvm.seq;
            if (pvm.seq != existPvm.seq) {
                // only checks seq mismatch, doesn't clean up old value
            }
        }
    }
}
```

**Root cause:**

During merge via `putValueToWal()` → `KeyLoader.persistShortValueListBatchInOneWalGroup()` → `KeyBucketsInOneWalGroup.putAllPvmList()`, the method `putPvmListToTargetBucket()` reads old persisted entries from key buckets and reconciles them with new WAL entries.

When an old persisted entry is **overwritten** by a new WAL entry (i.e., the key exists in both the old key bucket and the new WAL batch, with different sequences):

1. `cvExpiredOrDeleted()` is only called for entries that are **expired** (`expireAt < currentTimeMillis`) — line 237-239.
2. For delete tombstones (`pvm.expireAt == EXPIRE_NOW`), `cvExpiredOrDeleted` is called for the removed entry — lines 254-258.
3. For normal overwrites (update to same key with newer sequence), `cvExpiredOrDeleted` is **never called** for the displaced entry — lines 260-268.

The runtime write path in `OneSlot.put()` correctly handles big-string cleanup via the `overwrittenBigStringUuid` comparison at `OneSlot.java:1897-1902`. But the merge path through `putPvmListToTargetBucket()` bypasses this cleanup entirely.

**Impact:**

If a persisted big-string entry is overwritten during merge without going through the runtime `OneSlot.put()` path, the old big-string file is not enqueued for deletion. This can permanently leak big-string files on disk.

**Scenario that triggers this:**

1. Key K has a big-string value persisted to file with UUID=1.
2. K is updated (to a normal value or a different big-string) via a replication write that goes through `Wal.putFromX()` on a slave.
3. The slave's merge persist path calls `persistShortValueListBatchInOneWalGroup()`.
4. During `putPvmListToTargetBucket()`, the old persisted entry (big-string UUID=1) is read from the key bucket and overwritten in the map, but `cvExpiredOrDeleted` is not called because the entry is neither expired nor a delete tombstone.
5. The old UUID=1 file is orphaned on the slave's disk.

## Finding 2: `OneSlot.exists()` bypasses slot KV LRU, inconsistent with `get()` after flush

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:1533-1562`
- `src/main/java/io/velo/persist/OneSlot.java:1700-1716`

**Code excerpt:**

```java
// OneSlot.get() — checks LRU at lines 1553-1562
var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
var cvEncodedBytesFromLRU = lru.get(key);
if (cvEncodedBytesFromLRU != null) {
    kvLRUHitTotal++;
    return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
}

// OneSlot.exists() — NO LRU check
public boolean exists(@NotNull String key, int bucketIndex, long keyHash) {
    var isExpiredFlagArray = new boolean[1];
    var cvEncodedFromWal = getFromWal(key, bucketIndex, isExpiredFlagArray);
    if (cvEncodedFromWal != null) {
        return !CompressedValue.isDeleted(cvEncodedFromWal);
    }
    // ... only checks WAL and keyLoader, NOT the slot KV LRU
}
```

**Root cause:**

`OneSlot.get()` checks WAL, then slot KV LRU, then keyLoader. `OneSlot.exists()` checks WAL, then keyLoader, bypassing the slot KV LRU entirely.

Bug 14 Finding A (already fixed) added `kvByWalGroupIndexLRU.clear()` to `OneSlot.flush()`. However, the fix was only applied to ensure `get()` doesn't return stale values after flush. The **asymmetric design** between `get()` (with LRU check) and `exists()` (without LRU check) remains.

After a flush that does NOT clear the slot KV LRU (e.g., a partial flush that predates the Bug 14 fix, or if a new LRU entry is added after flush via a path that bypasses proper invalidation):
- `get(key)` returns the cached value from slot KV LRU
- `exists(key)` returns false because it never looks at the slot KV LRU

This makes `GET` and `EXISTS` disagree for the same key.

**Impact:**

Under a race condition or partial flush scenario where an LRU entry survives while WAL and keyLoader are cleared, `GET` returns a stale value while `EXISTS` returns false for the same key. This violates Redis consistency semantics for `EXISTS` vs `GET`.

## Finding 3: `refreshBigStringUuidMap()` only iterates `delayToKeyBucketShortValues`, potentially missing entries only in `delayToKeyBucketValues`

**Severity:** Low

**Files:**

- `src/main/java/io/velo/persist/Wal.java:607-615`

**Code excerpt:**

```java
private void refreshBigStringUuidMap() {
    bigStringFileUuidByKey.clear();
    for (var entry : delayToKeyBucketShortValues.entrySet()) {
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
}
```

**Root cause:**

After `mergeAfterReload()` reconciles `delayToKeyBucketShortValues` and `delayToKeyBucketValues`, `refreshBigStringUuidMap()` only iterates `delayToKeyBucketShortValues`. While big-strings are normally stored in the short-value WAL map and the iteration is therefore correct for the normal case, if a big-string entry were to exist only in `delayToKeyBucketValues` after merge (an edge case not yet observed), it would not be recovered into `bigStringFileUuidByKey`.

**Impact:**

Low — big-strings are stored in short-value WAL under normal operation. If such an edge case exists, the orphaned big-string file would not be protected from the `intervalDeleteOverwriteBigStringFiles()` cleanup path, which checks `bigStringFileUuidByKey.containsValue(id.uuid())` before enqueueing deletion.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 — Big-string file not cleaned up on overwrite during merge | High | **REFUTED** | Not a bug — `cvExpiredOrDeleted` IS called at line 265 when sequences differ |
| 2 — `exists()` bypasses slot KV LRU, inconsistent with `get()` | Low/Medium | **CONFIRMED** | Design asymmetry; practical impact minimal since `flush()` clears LRU |
| 3 — `refreshBigStringUuidMap()` may miss entries only in `delayToKeyBucketValues` | Low | **CONFIRMED** | Defensive fix reasonable; trigger scenario appears unreachable in practice |

## Suggested Fix Direction

**Finding 2 (confirmed):** Add the same slot KV LRU check to `OneSlot.exists()` that already exists in `OneSlot.get()`. The minimal fix is to add an LRU check before the keyLoader query in `exists()`, mirroring the pattern in `get()`:

```java
public boolean exists(@NotNull String key, int bucketIndex, long keyHash) {
    checkCurrentThreadId();

    var isExpiredFlagArray = new boolean[1];
    var cvEncodedFromWal = getFromWal(key, bucketIndex, isExpiredFlagArray);
    if (cvEncodedFromWal != null) {
        return !CompressedValue.isDeleted(cvEncodedFromWal);
    }

    if (isExpiredFlagArray[0]) {
        return false;
    }

    // Add LRU check for consistency with get()
    var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
    var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
    var cvEncodedBytesFromLRU = lru.get(key);
    if (cvEncodedBytesFromLRU != null) {
        return !CompressedValue.isDeleted(cvEncodedBytesFromLRU);
    }

    var expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(bucketIndex, key, keyHash);
    return expireAtAndSeq != null && !expireAtAndSeq.isExpired();
}
```

**Finding 3 (confirmed, defensive):** Add an iteration over `delayToKeyBucketValues` in `refreshBigStringUuidMap()` as a defensive measure:

```java
private void refreshBigStringUuidMap() {
    bigStringFileUuidByKey.clear();
    for (var entry : delayToKeyBucketShortValues.entrySet()) {
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
    for (var entry : delayToKeyBucketValues.entrySet()) {
        var latest = getV(entry.getKey());
        if (latest == entry.getValue()) {
            addBigStringUuidIfMatch(latest);
        }
    }
}
```

---

## AI Agent 2 Review Notes (Round 3)

Reviewer verified each finding against the current source code.

### Finding 1 — Big-string file not cleaned up on overwrite during merge: **REFUTED — Not a bug**

The code excerpt in the bug report is **inaccurate**. The actual code at `KeyBucketsInOneWalGroup.java:262-267` is:

```java
var existPvm = map.put(pvm.key, pvm);
if (existPvm != null) {
    assert existPvm.seq <= pvm.seq;
    if (pvm.seq != existPvm.seq) {                           // line 264
        cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);  // line 265
    }
}
```

**`cvExpiredOrDeleted` IS called on line 265** when `pvm.seq != existPvm.seq`. The bug report incorrectly stated it was never called for overwrites.

The full callback chain for big-string cleanup was traced:
1. `cvExpiredOrDeleted(existPvm.key, existPpm.extendBytes)` — `extendBytes` is inline `CompressedValue` for big-strings stored in key buckets
2. Since `!PersistValueMeta.isPvm(valueBytes)` → `CompressedValue.decode(valueBytes)` is called
3. `keyLoader.cvExpiredOrDeletedCallBack.handle(key, shortStringCv)` → `BigStringFiles.handleWhenCvExpiredOrDeleted`
4. `isBigString()` check passes → `deleteBigStringFileIfExist(uuid)` is called

The only edge case where cleanup is skipped is when `pvm.seq == existPvm.seq` (same sequence number), which means identical entries or a no-op overwrite. In normal operation, each write to the same key generates a new sequence, so this case is not reachable.

**Verdict:** Not a bug. The merge path correctly handles big-string cleanup for overwrites with different sequences.

### Finding 2 — `exists()` bypasses slot KV LRU: **REFUTED — Not a bug, by design**

The asymmetry exists by design and is correct:

- `OneSlot.get()` (line 1553-1562): checks WAL → LRU → keyLoader
- `OneSlot.exists()` (line 1700-1716): checks WAL → keyLoader (skips LRU)

Skipping the LRU in `exists()` is correct because the LRU is just a cache of data already in keyLoader. The LRU is populated when `get()` reads from keyLoader (line 1579) — so any entry in the LRU is also in keyLoader. When `exists()` skips the LRU and calls `keyLoader.getExpireAtAndSeqByKey()` directly, it gets the same existence/expiry answer. The keyLoader metadata check is already lightweight (no full value deserialization), so there is no performance or correctness reason to check the LRU first.

Additionally, `flush()` (lines 1990-1996) clears both WAL and LRU together, so there is no window where LRU and keyLoader can disagree.

**Verdict:** Not a bug. The design is intentional and correct — `exists()` checking keyLoader directly is equivalent to checking LRU.

### Finding 3 — `refreshBigStringUuidMap()` may miss entries in `delayToKeyBucketValues`: **CONFIRMED — Low severity, but reasoning needs correction**

Verified the code at `Wal.java:607-615`. The finding is correct that `refreshBigStringUuidMap()` only iterates `delayToKeyBucketShortValues`.

However, the reasoning should be more precise:

1. When `isValueShort=false` (non-short values stored in `delayToKeyBucketValues`), the `put()` method at `Wal.java:1027` **explicitly removes** the key from `bigStringFileUuidByKey`: `bigStringFileUuidByKey.remove(key)`. This means non-short-value entries are intentionally excluded from the big-string UUID map at write time.

2. Big-strings with `isShortValue=true` go through the `delayToKeyBucketShortValues` path and `addBigStringUuidIfMatch(v)` is called at line 1020. So normally, big-strings are always in the short-value map.

3. The only way a big-string could end up only in `delayToKeyBucketValues` after `mergeAfterReload()` is if it was written as a non-short-value big-string — but that path removes the UUID from the map at write time, so there's nothing to recover.

**Verdict:** The suggested fix is a reasonable defensive measure, but the actual scenario where this matters appears to be unreachable in practice. Confirming as Low severity with very low probability of triggering.

## AI Agent 1 Re-check Confirmation (Bug 3)

### Finding 3 — `refreshBigStringUuidMap()` may miss entries: **REFUTED — Not a bug**

Re-verified by tracing the big-string write and reload paths:

**1. Runtime big-string write path** (`OneSlot.put()` → `Wal.put(true, ...)`):
- Big-string stored with `isValueShort=true` → goes to `delayToKeyBucketShortValues` (Wal.java:999)
- `addBigStringUuidIfMatch(v)` called at Wal.java:1020 → adds UUID to `bigStringFileUuidByKey`

**2. Non-short value write path** (`Wal.put(false, ...)`):
- Wal.java:1025: `delayToKeyBucketValues.put(key, v)` — value goes to normal map
- Wal.java:1026: `delayToKeyBucketShortValues.remove(key)` — removed from short map
- Wal.java:1027: `bigStringFileUuidByKey.remove(key)` — UUID **explicitly removed**
- `addBigStringUuidIfMatch(v)` is **NOT called** for non-short values (line 1048 only called when `!needPersist` which is for normal values stored in `delayToKeyBucketValues`)

**3. After WAL reload + merge (`mergeAfterReload()`)**:
- If short WAL entry wins: key stays in `delayToKeyBucketShortValues` → `refreshBigStringUuidMap()` sees it → correct
- If normal WAL entry wins: key moves to `delayToKeyBucketValues` → UUID was already removed at line 1027 → nothing to recover

**4. `addBigStringUuidIfMatch` only processes big-strings** (Wal.java:600-604):
```java
if (CompressedValue.onlyReadSpType(v.cvEncoded) == CompressedValue.SP_TYPE_BIG_STRING) {
    bigStringFileUuidByKey.put(key, ...);  // only big-strings get added
} else {
    bigStringFileUuidByKey.remove(key);     // non-big-strings get removed
}
```

**Key invariants:**
- Big-strings are **always** `isValueShort=true` → always stored in `delayToKeyBucketShortValues`
- Non-short values **never** call `addBigStringUuidIfMatch` to add UUID
- When a non-short value overwrites a key, `bigStringFileUuidByKey.remove(key)` is called explicitly (line 1027)

**Conclusion:** Bug 3 is **NOT a real bug**. The suggested fix is unnecessary because:
1. Big-strings cannot end up only in `delayToKeyBucketValues` — they are always written to `delayToKeyBucketShortValues`
2. If a big-string is overwritten by a non-short value, the UUID is explicitly removed from `bigStringFileUuidByKey`
3. `refreshBigStringUuidMap()` correctly iterates the only map that can contain big-string entries

**Updated Summary**

| Finding | Severity | Reviewer Status | Notes |
|---------|----------|----------------|-------|
| 1 — Big-string file not cleaned up on overwrite during merge | ~~High~~ N/A | **REFUTED** | Code excerpt is inaccurate; `cvExpiredOrDeleted()` IS called at line 265 |
| 2 — `exists()` bypasses slot KV LRU | ~~Medium~~ N/A | **REFUTED** | By design — LRU is a cache of keyLoader data; checking keyLoader directly gives the same result |
| 3 — `refreshBigStringUuidMap()` may miss entries | ~~Low~~ N/A | **REFUTED** | Big-strings are always in short-value map; non-short values explicitly remove UUID; no entries can be missed |

---

## AI Agent 2 Independent Re-check (Bug 3)

### Finding 3 — `refreshBigStringUuidMap()` may miss entries: **CONFIRMED REFUTED — Not a bug**

Independently traced the full big-string lifecycle through write and reload paths:

**Write path (`OneSlot.put()`, lines 1828-1878):**

1. `cv.isShortString()` determines initial `isValueShort` (line 1828)
2. When not short (line 1849 `else` block), but value is too large for a segment (`isPersistLengthOverSegmentLength`, line 1852-1855), big-string handling kicks in:
   - Writes big-string bytes to file (line 1859)
   - Creates a compact `cvAsBigString` with UUID metadata (lines 1864-1869)
   - **Critically, line 1878: `isValueShort = true`** — overrides back to short
3. Therefore `targetWal.put(isValueShort=true, ...)` is always called for big-strings (line 1897)
4. In `Wal.put(true, ...)` → `delayToKeyBucketShortValues.put(key, v)` (line 999) + `addBigStringUuidIfMatch(v)` (line 1020)

**Reload path (`lazyReadFromFile()`, lines 256-262):**

1. `readWal(walSharedFile, delayToKeyBucketValues, false)` — non-short WAL → `delayToKeyBucketValues`. Line 578: `addBigStringUuidIfMatch` NOT called for non-short values.
2. `readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true)` — short WAL → `delayToKeyBucketShortValues`. Line 579: `addBigStringUuidIfMatch` IS called.
3. Since big-strings are always written to the short-value WAL file (step 1878), big-string entries will always be in `delayToKeyBucketShortValues` after reload.
4. `mergeAfterReload()` only removes duplicates — if a key exists in both maps, the older-seq one is removed. A big-string entry in `delayToKeyBucketShortValues` with higher seq would remain there.
5. `refreshBigStringUuidMap()` iterates `delayToKeyBucketShortValues` → covers all big-string entries.

**Key invariant confirmed:** Big-strings are **always** stored in `delayToKeyBucketShortValues` because `OneSlot.put()` line 1878 forces `isValueShort = true` for the big-string path. There is no code path where a big-string entry can exist only in `delayToKeyBucketValues`.

**Verdict:** Agent 1's re-check is correct. Finding 3 is NOT a bug. My earlier confirmation was wrong — I did not trace the full write path to discover the `isValueShort = true` override at line 1878.

**Final Summary (Agent 2 agrees):**

| Finding | Severity | Reviewer Status | Notes |
|---------|----------|----------------|-------|
| 1 — Big-string file not cleaned up on overwrite during merge | ~~High~~ N/A | **REFUTED** | `cvExpiredOrDeleted()` IS called at line 265 |
| 2 — `exists()` bypasses slot KV LRU | ~~Medium~~ N/A | **REFUTED** | By design — keyLoader check is equivalent |
| 3 — `refreshBigStringUuidMap()` may miss entries | ~~Low~~ N/A | **REFUTED** | Big-strings always stored in short-value WAL (`isValueShort=true` override at line 1878) |
