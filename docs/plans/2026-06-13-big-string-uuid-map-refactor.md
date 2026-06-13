# Big String UUID Map Refactor

**Date**: 2026-06-13
**Status**: Completed
**Scope**: `persist` layer — `BigStringFiles`, `Wal`, `OneSlot`

## Motivation

### Problem 1: `getCurrentBigStringUuid()` hot-path disk I/O

`getCurrentBigStringUuid()` was called on every non-short-value `put()`. When the key was not found in WAL, it fell through to `keyLoader.getValueXByKey()` — a disk read through `FdReadWrite.readOneInner()`. Under load (e.g., `redis-benchmark -d 200 -t set`), this showed up heavily in the Arthas profiler.

### Problem 2: Scattered responsibility for UUID tracking

The `bigStringFileUuidByKey` map lived in `Wal.java` (per Wal group), even though big string files are managed per-slot by `BigStringFiles`. The map was cleared on `clearShortValues()`, losing the mapping for persisted keys, forcing the expensive keyLoader fallback.

### Problem 3: Complex orphan file scanning

`intervalDeleteOverwriteBigStringFiles` had dual-branch logic that checked both `keyLoader.getPersistedBigStringIdList()` and `targetWal.hasKey()` — unnecessary once the UUID map is authoritative.

## Changes

### 1. Move UUID map from `Wal` to `BigStringFiles`

| Before | After |
|--------|-------|
| `Wal.bigStringFileUuidByKey` (per Wal group, N fragments) | `BigStringFiles.bigStringUuidByKey` (per slot, single map) |
| `Wal.addBigStringUuidIfMatch(v)` — private method | `BigStringFiles.updateUuidFromEncoded(key, cvEncoded)` — public method |
| `Wal.refreshBigStringUuidMap()` — private, operates on Wal field | `Wal.refreshBigStringUuidMap()` — delegates to `BigStringFiles` |
| Map cleared on `clearShortValues()` | Map survives `clearShortValues()` — UUIDs remain valid in KeyBucket |

New public API on `BigStringFiles`:

```java
void updateUuidFromEncoded(String key, byte[] cvEncoded)
void removeBigStringUuid(String key)
Long getBigStringUuid(String key)
boolean containsUuid(long uuid)
int uuidMapSize()
void clearUuidMap()
void populateFromKeyBuckets(KeyLoader keyLoader, int walGroupIndex)
```

### 2. Simplify `getCurrentBigStringUuid()`

**Before** (3-stage: WAL → keyLoader null check → disk read):
```java
private Long getCurrentBigStringUuid(Wal targetWal, String key, int bucketIndex, long keyHash) {
    var walV = targetWal.getV(key);           // HashMap lookup
    if (walV != null) {
        if (walV.isRemove() || walV.isExpired()) return null;
        return getBigStringUuidIfStored(walV.cvEncoded());
    }

    if (keyLoader == null) return null;

    var valueBytesWithExpireAtAndSeq = keyLoader.getValueXByKey(bucketIndex, key, keyHash);  // DISK READ
    if (valueBytesWithExpireAtAndSeq == null || valueBytesWithExpireAtAndSeq.isExpired()) return null;
    return getBigStringUuidIfStored(valueBytesWithExpireAtAndSeq.valueBytes());
}
```

**After** (2-stage: WAL → in-memory map):
```java
private Long getCurrentBigStringUuid(Wal targetWal, String key, int bucketIndex, long keyHash) {
    var walV = targetWal.getV(key);           // HashMap lookup
    if (walV != null) {
        if (walV.isRemove() || walV.isExpired()) return null;
        return getBigStringUuidIfStored(walV.cvEncoded());
    }
    return bigStringFiles.getBigStringUuid(key);  // HashMap lookup, O(1)
}
```

### 3. Populate UUID map from KeyBuckets at startup

`Wal.lazyReadFromFile()` now calls `BigStringFiles.populateFromKeyBuckets()` after WAL reload:

```java
void lazyReadFromFile() throws IOException {
    readWal(walSharedFile, delayToKeyBucketValues, false);
    readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true);
    mergeAfterReload();
    refreshBigStringUuidMap();                                    // from WAL short values
    var bsf = oneSlot.getBigStringFiles();
    if (bsf != null && oneSlot.keyLoader != null) {
        bsf.populateFromKeyBuckets(oneSlot.keyLoader, groupIndex);  // from persisted KeyBuckets
    }
}
```

`populateFromKeyBuckets` reads key buckets using batch reads per split index
(via `KeyLoader.readBatchInOneWalGroup` / `getMetaKeyBucketSplitNumberBatch`),
parsing each `KeyBucket` from shared bytes and iterating entries. This avoids
the N-individual-disk-reads pattern of `getPersistedBigStringIdList`.

`putIfAbsent` ensures WAL entries (newer) take precedence over KeyBucket entries.

### 4. Simplify `intervalDeleteOverwriteBigStringFiles`

**Before** — dual-branch logic with `keyLoader.getPersistedBigStringIdList()` + `targetWal.hasKey()`:

```java
var persistedIdWithKeyList = keyLoader.getPersistedBigStringIdList(targetBucketIndex);
if (persistedIdWithKeyList.isEmpty()) {
    for (var id : idList) {
        if (targetWal.bigStringFileUuidByKey.containsValue(id.uuid())) continue;
        delayToDeleteBigStringFileIds.add(...);
    }
} else {
    // complex map lookup + hasKey check ...
}
```

**After** — single check against authoritative map:

```java
for (var id : idList) {
    if (bigStringFiles.containsUuid(id.uuid())) continue;
    delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(id.uuid(), targetBucketIndex, id.keyHash(), ""));
    count++;
}
```

No more `keyLoader.getPersistedBigStringIdList()` or `targetWal.hasKey(key)` in this method.

### 5. Fix `handleWhenCvExpiredOrDeleted` UUID compare

When a big string key expires, the old `CompressedValue` (with old UUID) is passed in. If the key was overwritten with a new big string UUID, removing by key would delete the NEW UUID from the map.

**Fix**: Only remove if the current map entry still points to this UUID:

```java
var uuid = shortStringCv.getBigStringMetaUuid();
var bucketIndex = KeyHash.bucketIndex(shortStringCv.getKeyHash());
var isDeleted = deleteBigStringFileIfExist(uuid, bucketIndex, shortStringCv.getKeyHash());
var currentUuid = bigStringUuidByKey.get(key);
if (currentUuid != null && currentUuid == uuid) {
    bigStringUuidByKey.remove(key);
}
```

### 6. Remove `clearUuidMap()` from `Wal.clear()`

`OneSlot.flush()` already calls `bigStringFiles.deleteAllBigStringFiles()` which clears the map. Having `Wal.clear()` also clear it was a responsibility leak. Removed.

### 7. Remove `clearUuidMap()` from `Wal.clearShortValues()`

Short values persist flushes data to KeyBuckets — the UUID references remain valid. Clearing the map was the original cause of the expensive `keyLoader.getValueXByKey()` fallback.

## Architecture Diagram

```
Before                                    After
┌──────────┐  ┌──────────┐               ┌──────────────────────┐
│ Wal[0]   │  │ Wal[1]   │               │ BigStringFiles       │
│  uuidMap │  │  uuidMap │  ...          │  bigStringUuidByKey  │  ← single map
└──────────┘  └──────────┘               │  write/read/delete   │
                                            │  handleExpired       │
       Fragmented per Wal group             │  populateFromKB      │
                                            └──────────────────────┘
       ┌──────────┐
       │ KeyLoader│                         ┌──────┐  ┌──────┐
       │  getVX() │  ← disk fallback        │Wal[0]│  │Wal[1]│  ...
                                            └──────┘  └──────┘
                                                  No uuid map
```

## Performance Impact

| Operation | Before | After |
|-----------|--------|-------|
| `getCurrentBigStringUuid` WAL miss | HashMap + disk read | HashMap only |
| `getCurrentBigStringUuid` WAL hit | HashMap | HashMap |
| `intervalDeleteOverwriteBigStringFiles` | HashMap + KeyLoader scan + `hasKey` | HashMap only |
| UUID map access | Compute `walGroupIndex` first | Direct |

Primary win: eliminates `keyLoader.getValueXByKey()` disk I/O from the `put()` hot path.

## Files Changed

| File | Changes |
|------|---------|
| `BigStringFiles.java` | Added `bigStringUuidByKey` field + 7 public methods + `populateFromKeyBuckets` |
| `Wal.java` | Removed `bigStringFileUuidByKey` field + `addBigStringUuidIfMatch` method; rewrote `refreshBigStringUuidMap`; added lazyload populate; removed `clearUuidMap` from `clear()` and `clearShortValues()` |
| `OneSlot.java` | Simplified `getCurrentBigStringUuid`; simplified `intervalDeleteOverwriteBigStringFiles`; updated `put()`/`removeDelay()` overwrite checks |
| `WalTest.groovy` | Updated test references to use `oneSlot.bigStringFiles.bigStringUuidByKey`; changed `new OneSlot(slot)` → `new OneSlot(slot, Consts.slotDir, null, null)` |
| `OneSlotTest.groovy` | Updated test references to use `oneSlot.bigStringFiles.bigStringUuidByKey` |
| `CheckBigStringFiles.groovy` | Updated test references |
