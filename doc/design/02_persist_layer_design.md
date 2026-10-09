# Velo Persistence Layer Design

## Overview

Persistence is managed by [`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java),
which owns an array of per-slot [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) instances.

This is the real storage boundary in the current codebase.

## Main Components

Each `OneSlot` composes several storage structures:

- `Wal[] walArray` — one [`Wal`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Wal.java) per WAL group, for recent writes
  and delayed persistence
- [`KeyLoader`](/home/kerry/ws/velo/src/main/java/io/velo/persist/KeyLoader.java) for key buckets and metadata lookup
- `Chunk chunk` — [`Chunk`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Chunk.java) for persisted value segments,
  created lazily by `OneSlot.initChunk()`
- [`BigStringFiles`](/home/kerry/ws/velo/src/main/java/io/velo/persist/BigStringFiles.java) for oversized values
- [`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java) for slot-local mutable settings
- [`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java) when replication/binlog is enabled

Note that `Wal` is not a single object. `OneSlot.walArray` is sized by `Wal.calcWalGroupNumber()`, and all WAL groups
share two `RandomAccessFile`s owned by `OneSlot`:

- `raf` → `wal.dat`, for non-short values
- `rafShortValue` → `wal-short-value.dat`, for short values

Each group `g` owns the fixed byte range `[ONE_GROUP_BUFFER_SIZE * g, ONE_GROUP_BUFFER_SIZE * (g + 1))` in **both**
files. `Wal.ONE_GROUP_BUFFER_SIZE` is `LocalPersist.PAGE_SIZE * 16` (64K with 4K pages).

## Slot Initialization

`LocalPersist.initSlots(...)` creates one `OneSlot` per configured slot and calls `initFds()` on each one.
Slot data is laid out under a `persist/slot-N` directory tree beneath the configured data directory.

`LocalPersist` also initializes:

- volume mapping through `ConfVolumeDirsForSlot`
- `MultiShard` cluster metadata
- index-worker infrastructure through `IndexHandlerPool`

`LocalPersist.bindOneSlotIfInitialized(...)` records the owning thread id in each `OneSlot`'s
`threadIdProtectedForSafe` field, using index `slot % slotWorkerThreadIds.length`.

## WAL Groups

The WAL group index is derived from the **bucket index**, not directly from the key hash:

```java
// Wal.java:328-330
public static int calcWalGroupIndex(int bucketIndex) {
    return bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
}
```

`ConfWal.oneChargeBucketNumber` is `32` for all three bundled presets (`debugMode`, `c1m`, `c10m`), and
validation requires it to be in `Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST` (`ConfForSlot.java:489`).

The group index threads through the whole write path:

- selects the target `Wal` instance for the append
- selects the destination key buckets when short values are flushed
- selects the chunk segments and merge bookkeeping when long values are flushed
- selects the KV LRU partition used for reads
- bounds the file offset tracked by `walWriteIndex` / `walShortValueWriteIndex`
  (compared against `targetWal.fileToWriteIndex = ONE_GROUP_BUFFER_SIZE * (groupIndex + 1)`)

Merge enforces the invariant that a segment's keys all belong to the flushing group
(`OneSlot.java:2517-2522` throws `IllegalStateException` on mismatch).

## Write Path

The single implementation is:

```java
// OneSlot.java:2022
public void put(@NotNull String key, int bucketIndex, @NotNull CompressedValue cv,
                boolean isFromMerge, boolean ignoreReadonly)
```

The 2-, 3-, and 4-argument overloads are thin wrappers around it. `putIfSeqBigger(...)` wraps it with a
sequence-number comparison first.

Order of operations:

1. **Thread + readonly guard.** `checkCurrentThreadId()` throws `IllegalStateException` if the current thread is not
   the recorded slot owner. `isReadonly() && !ignoreReadonly` throws `ReadonlyException`.
2. **Stats.** `putCountTotal++`, and `ttlTotalInSecond` when the value has an expiration.
3. **WAL group resolution.** `walGroupIndex = Wal.calcWalGroupIndex(bucketIndex)`, then `targetWal = walArray[walGroupIndex]`.
4. **Storage-class decision** (see [Storage Classes](#storage-classes) below), producing a `Wal.V`.
5. **WAL append.** `targetWal.put(isValueShort, key, v, lastPersistTimeMs)`.
6. **Binlog append + persist.** If `putResult.needPersist()`, append `XBigStrings` / `XWalV` to the binlog, then
   `doPersist(walGroupIndex, key, putResult)`. A trailing binlog append covers the case where the WAL buffer had no
   room immediately after the put.
7. **Delayed big-string cleanup.** Any `overwrittenBigStringUuid` is enqueued into
   `delayToDeleteBigStringFileIds` for later physical removal.

### Storage Classes

Every value — short or long — is appended to the WAL first. The storage class only determines what happens when the
WAL group is later flushed to disk.

The decision is made in `OneSlot.put` at `OneSlot.java:2039-2107`:

| Class | Condition | Destination | Key bucket stores |
|---|---|---|---|
| **short value** | `cv.isShortString()`, i.e. type is string and `compressedData.length <= 16` | key buckets via `KeyLoader.persistShortValueListBatchInOneWalGroup` | the encoded `CompressedValue` itself |
| **big string** | not short, **and** `persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength` (or the key starts with the `kerry-test-big-string-` test prefix) | `bigStringFiles.writeBigStringBytes(uuid, bucketIndex, keyHash, bytes)` → `big-string/<bucketIndex>/<uuid>_<keyHash>` | a 40-byte `CompressedValue` with `SP_TYPE_BIG_STRING` carrying the uuid (8B) + real dict seq (4B) |
| **chunk** | everything else | `chunk.persist(walGroupIndex, list, keyBucketsInOneWalGroup)` | a 12-byte `PersistValueMeta(segmentIndex, segmentOffset)` |

Relevant constants:

| Constant | Value | Location |
|---|---|---|
| `CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN` | `16` | `CompressedValue.java:54` |
| `CompressedValue.isShortString()` | `compressedData.length <= 16` and type is string | `CompressedValue.java:722-727` |
| `Wal.V.persistLength(keyLength, cvEncodedLength)` | `CompressedValue.KEY_HEADER_LENGTH (2) + keyLength + cvEncodedLength` | `Wal.java:85-93` |
| `SegmentBatch2.SEGMENT_HEADER_LENGTH` | `17` (`8 + 1 + 4 + 4`) | `SegmentBatch2.java:31` |
| `ConfChunk.segmentLength` (default) | `4096` | `ConfForSlot.java:341` |
| `PersistValueMeta.ENCODED_LENGTH` | `12` (`2 + 1 + 1 + 4 + 4`) | `PersistValueMeta.java` |
| `CompressedValue.KEY_MAX_LENGTH` | `256` | `CompressedValue.java:93` |
| `Wal.ONE_GROUP_BUFFER_SIZE` | `PAGE_SIZE * 16` | `Wal.java:271` |
| `ConfWal.valueSizeTrigger` / `shortValueSizeTrigger` | `200` / `200` | `ConfForSlot.java:462,467` |
| `ConfWal.atLeastDoPersistOnceIntervalMs` | `2` | `ConfForSlot.java:472` |
| `ConfWal.checkAtLeastDoPersistOnceSizeRate` | `0.8` | `ConfForSlot.java:478` |

Note the big-string branch flips its own local `isValueShort` flag to `true` before appending, so the WAL write and the
subsequent flush both use the short-value file and the short-value key-bucket path. Only the 12-byte uuid+dictSeq
metadata goes through the key bucket; the payload lives in its own file.

Short values are encoded differently by subtype: `encodeAsNumber()` for numeric values, `encode()` for big-string
metadata, `encodeAsShortString()` otherwise.

### Flush To Disk

`doPersist(...)` (`OneSlot.java:2190`) calls `putValueToWal(isShortValue, targetWal)`
(`OneSlot.java:2486`), which is where the destination actually diverges:

```java
// OneSlot.java:2487-2491
var walGroupIndex = targetWal.groupIndex;
if (isShortValue) {
    keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, targetWal.delayToKeyBucketShortValues.values());
    return;
}
```

For long values the pending list is sorted by bucket index ("for future merge better"),
`readSomeSegmentsBeforePersistWal(walGroupIndex)` pre-reads segments needing merge, and then
`chunk.persist(...)` writes the segments and calls
`keyLoader.updatePvmListBatchAfterWriteSegments(...)` to install the 12-byte `PersistValueMeta` pointers.

Each `Wal` maintains two mutually exclusive delay maps, `delayToKeyBucketShortValues` and `delayToKeyBucketValues`;
a put removes the key from the other map so the two views cannot disagree.

### Deletes

`remove(key, bucketIndex, keyHash)` calls `removeDelay(...)`, which builds a `Wal.V` with
`CompressedValue.SP_FLAG_DELETE_TMP (-128)` and `CompressedValue.EXPIRE_NOW (-1)` and calls
`Wal.put(true, ...)` (`Wal.java:704-709`). **Deletes always take the short-value WAL and key-bucket path**, regardless of the previous
value's size.

## Read Path

The main read is:

```java
// OneSlot.java:1693
public BufOrCompressedValue get(String key, int bucketIndex, long keyHash)
// where BufOrCompressedValue is record BufOrCompressedValue(@Nullable ByteBuf buf, @Nullable CompressedValue cv)
```

Lookup order as coded:

1. **WAL.** `getFromWal(key, bucketIndex, isExpiredFlagArray)` (`OneSlot.java:1817`) looks up the key in both delay
   maps and keeps the one with the higher sequence. On a hit it increments `kvWalHitTotal`; a deleted value returns
   `null`; an expired value sets the flag and returns `null`. WAL hits do **not** populate the LRU.
2. **KV LRU.** `kvByWalGroupIndexLRU.get(walGroupIndex).get(key)`. On a hit it increments `kvLRUHitTotal` and returns
   the stored encoded bytes.
3. **Key bucket.** `keyLoader.getValueXByKey(bucketIndex, key, keyHash)` returns
   `valueBytes + expireAt + seq`, or `null`. An expired value returns `null`.
4. **Discriminate short value vs `PersistValueMeta`.**
   ```java
   // OneSlot.java:1739-1744
   if (!PersistValueMeta.isPvm(valueBytes)) {
       lru.put(key, valueBytes);
       return new BufOrCompressedValue(Unpooled.wrappedBuffer(valueBytes), null);
   }
   ```
   `PersistValueMeta.isPvm` is `bytes[0] == 0 && bytes[1] == 0 && bytes.length == 12`.
5. **Chunk segment.** Decode the `PersistValueMeta`, bounds-check `segmentIndex` against
   `chunk.getMaxSegmentIndex()` (throws `PersistValueMetaCorruptedException` if it overruns), read via
   `chunk.readOneSegment(...)`, and decompress via `SegmentBatch.decompressSegmentBytesFromOneSubBlock(...)` when
   `SegmentBatch2.isSegmentBytesTight(...)` says the segment is compressed.
6. **Key verification.** Read `keyLength` (must be in `1..CompressedValue.KEY_MAX_LENGTH`) and the key bytes from
   `pvm.segmentOffset`, and throw `IllegalStateException` if they do not match the requested key.
7. **Decode + seed LRU.** `CompressedValue.decode(...)`, then `lru.put(key, cv.encode())`. For this path the LRU
   stores the re-encoded `CompressedValue`, not the segment bytes.

### Big Strings Are Resolved By The Caller

`OneSlot.get` never reads big-string payload bytes. It only returns the 40-byte `CompressedValue` carrying the uuid
and the real dict seq. The payload is resolved one layer up in
[`BaseCommand.getCv`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java):

```java
// BaseCommand.java:754-770
if (cv.isBigString()) {
    var buffer = ByteBuffer.wrap(cv.getCompressedData());
    var uuid = buffer.getLong();
    var realDictSeq = buffer.getInt();
    var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, s.bucketIndex, s.keyHash, true);
    if (bigStringBytes == null) {
        throw new BigStringFileMissingException("data inconsistency: big string file missing for key=" + key);
    }
    cv.setCompressedData(bigStringBytes);
    cv.setDictSeqOrSpType(realDictSeq);
}
```

`BigStringFiles.getBigStringBytes` checks its own `LRUMap<Long, byte[]> bigStringBytesByUuidLRU` first, then reads
`big-string/<bucketIndex>/<uuid>_<keyHash>` from disk and seeds the LRU.

`getCv` is also where the expiry check that `OneSlot.get` omits on the LRU path happens (`cv.isExpired()` →
return `null`). So the "or null if absent or expired" contract of `OneSlot.get` is honoured by its caller rather than
by `OneSlot.get` itself on that one path.

### Companion Read Methods

- `getExpireAt(key, bucketIndex, keyHash)` (`OneSlot.java:1615`) follows the same WAL → LRU → `KeyLoader` order but
  returns only the expiration. Unlike `get`, it **does** check expiry on the LRU hit path, via
  `CompressedValue.onlyReadExpireAt(bytes)`.
- `exists(key, bucketIndex, keyHash)` (`OneSlot.java:1848`) checks the WAL, then goes straight to
  `keyLoader.getExpireAtAndSeqByKey(...)`, **skipping the LRU entirely**.

## Additional Slot-Storage Details

- key-bucket layout is sized from slot configuration rather than resized dynamically at runtime in a generic
  hash-table style
- bucket entries track key bytes, key hash, expiration, and value location metadata
- bucket splitting exists (`KeyLoader.MAX_SPLIT_NUMBER`, `KeyHash.splitIndex(...)`), but it is a bounded mechanism
  and is treated as something to minimize rather than a primary growth strategy
- reading a key bucket itself goes through `FdReadWrite.readOneInner(...)`, which has its own per-fd LRU

## Merge And Cleanup Work

`OneSlot` also owns background-style persistence maintenance:

- WAL lazy load on startup (`Wal.lazyReadFromFile()`)
- merge of persisted segments
- expired/deleted value cleanup
- overwrite cleanup for big-string files (`delayToDeleteBigStringFileIds`)
- binlog reset/reopen helpers for replication role changes

Before flushing new data, `putValueToWal` calls `readSomeSegmentsBeforePersistWal(walGroupIndex)`
(`OneSlot.java:2444`), which uses `metaChunkSegmentFlagSeq.findThoseNeedToMerge(walGroupIndex)` to pre-read related
persisted segments in the same WAL-group region and merge valid values forward. This is the segment-merge behavior.

Other wal-group-scoped entry points: `flush(...)` (`OneSlot.java:2238`),
`rebuildKeyBucketsFromChunk(...)` (`OneSlot.java:2571`), and the background `doTask` sweep, which walks one group per
iteration via `walArray[Math.floorMod(loopCount, walArray.length)]` (`OneSlot.java:1279`).

## LRU Layers

There are three distinct cache surfaces, and only one of them is owned by `OneSlot`:

| Cache | Owner | Contents |
|---|---|---|
| `kvByWalGroupIndexLRU` — `Map<Integer, LRUMap<String, byte[]>>` (`OneSlot.java:835`) | `OneSlot` | encoded `CompressedValue` bytes, partitioned by WAL group |
| per-fd read LRUs | `FdReadWrite` | key-bucket bytes (`bucket.lruPerFd.percent`) and chunk-segment bytes (`chunk.lruPerFd.maxSize`) |
| `bigStringBytesByUuidLRU` — `LRUMap<Long, byte[]>` | `BigStringFiles` | big-string payload bytes by uuid |

`OneSlot.initLRU(boolean)` (`OneSlot.java:279`) sizes the KV LRU:

```java
int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;  // kv.lru.maxSize = 100000
var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;                       // floored at 10
for (int walGroupIndex = 0; walGroupIndex < walGroupNumber; walGroupIndex++) {
    kvByWalGroupIndexLRU.put(walGroupIndex, new LRUMap<>(maxSizeForEachWalGroup));
}
```

Invalidation is coarse and per-group: any key-bucket batch write for group `g` clears group `g`'s entire KV LRU, via
`KeyLoader.doAfterPutAll(...)` → `oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex)`
(`KeyLoader.java:864-866`, `OneSlot.java:898-913`).

## Key Loader Details

`KeyLoader.getValueXByKey(bucketIndex, key, keyHash)` resolves
`metaKeyBucketSplitNumber.get(bucketIndex)` → `KeyHash.splitIndex(...)` →
`readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, true)`, then linearly scans the bucket's cells
comparing `keyHash` first and the full key bytes second (`KeyBucket.java:746-784`).

Batch entry points used by the write path:

- `persistShortValueListBatchInOneWalGroup(walGroupIndex, values)` — short values, straight into the buckets
- `updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, keyBucketsInOneWalGroup)` — installs `PersistValueMeta`
  pointers after `Chunk` has written the segments

## Thread Affinity

`OneSlot` guards all of the above with `threadIdProtectedForSafe`, checked by `checkCurrentThreadId()`
(`OneSlot.java:1600`). Direct calls must happen on the owning slot worker. Cross-slot command paths must schedule
through `OneSlot.asyncCall(...)` / `asyncRun(...)` / `asyncExecute(...)`.

## Metrics

`OneSlot` implements [`InSlotMetricCollector`](/home/kerry/ws/velo/src/main/java/io/velo/metric/InSlotMetricCollector.java).
Its `collect()` method exports persistence metrics covering WAL, key loader, chunks, big strings, LRU, and replication.

Two health constants are worth knowing when reading metrics:

- `OneSlot.HEALTH_WARN_CHUNK_SEGMENT_FILL_RATE` = `0.80` (`OneSlot.java:318`)
- `OneSlot.HEALTH_WARN_KEY_BUCKET_KEY_COUNT_MAX` = `KeyBucket.INIT_CAPACITY * KeyLoader.MAX_SPLIT_NUMBER * 9 / 10`
  (`OneSlot.java:321`)

## Related Documents

- [Overall Architecture](/home/kerry/ws/velo/doc/design/01_overall_architecture.md)
- [Type System](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [Compression](/home/kerry/ws/velo/doc/design/07_compression_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Metrics](/home/kerry/ws/velo/doc/design/13_metrics_monitoring_design.md)
