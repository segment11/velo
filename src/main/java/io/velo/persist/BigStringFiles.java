package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.NullableOnlyTest;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Manages big string files stored in the file system or in-memory.
 */
public class BigStringFiles implements InMemoryEstimate, InSlotMetricCollector, HandlerWhenCvExpiredOrDeleted {
    record Id(long uuid, int bucketIndex) {
    }

    public record IdWithKey(long uuid, int bucketIndex, long keyHash, String key) {
    }


    private final short slot;
    final File bigStringDir;

    long diskUsage = 0L;

    long readByteLengthTotal = 0L;
    long readFileCountTotal = 0L;
    long readFileCostTotalUs = 0L;

    long writeByteLengthTotal = 0L;
    long writeFileCountTotal = 0L;
    long writeFileCostTotalUs = 0L;

    long deleteByteLengthTotal = 0L;
    long deleteFileCountTotal = 0L;
    long deleteFileCostTotalUs = 0L;

    long bigStringMissingFileTotal = 0L;

    private static final String BIG_STRING_DIR_NAME = "big-string";

    private LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    @TestOnly
    public void clearLRUCache() {
        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.clear();
        }
    }

    int bigStringFilesCount = 0;

    HashSet<Integer> bucketIndexesWhenFirstServerStart = new HashSet<>();

    /**
     * Single source of truth for key-to-big-string-UUID mapping.
     * Populated from WAL short values and KeyBucket data, survives WAL clears.
     */
    // Owned by the slot worker thread — not safe for concurrent access from multiple threads.
    final HashMap<String, Long> bigStringUuidByKey = new HashMap<>();
    /**
     * Reverse index for O(1) UUID membership checks.
     * Kept in sync with {@link #bigStringUuidByKey} on every put/remove.
     */
    // Owned by the slot worker thread — not safe for concurrent access from multiple threads.
    final HashSet<Long> bigStringUuidSet = new HashSet<>();

    /**
     * Updates the UUID map from encoded value bytes.
     * If the encoded value is a big string reference, records its UUID;
     * otherwise removes any stale entry for the key.
     *
     * @param key       the key to update
     * @param cvEncoded the encoded bytes of the CompressedValue
     */
    public void updateUuidFromEncoded(String key, byte[] cvEncoded) {
        if (cvEncoded.length < 28) {
            var old = bigStringUuidByKey.remove(key);
            if (old != null) bigStringUuidSet.remove(old);
            return;
        }
        if (CompressedValue.onlyReadSpType(cvEncoded) == CompressedValue.SP_TYPE_BIG_STRING) {
            var newUuid = CompressedValue.getBigStringMetaUuid(cvEncoded);
            var old = bigStringUuidByKey.put(key, newUuid);
            bigStringUuidSet.add(newUuid);
            if (old != null && old != newUuid) bigStringUuidSet.remove(old);
        } else {
            var old = bigStringUuidByKey.remove(key);
            if (old != null) bigStringUuidSet.remove(old);
        }
    }

    /**
     * Removes the UUID mapping for the given key.
     *
     * @param key the key to remove
     */
    public void removeBigStringUuid(String key) {
        var old = bigStringUuidByKey.remove(key);
        if (old != null) bigStringUuidSet.remove(old);
    }

    /**
     * Removes the key from the UUID map only if its current UUID matches the given uuid.
     * Safe to call when the key may have been overwritten with a newer big string.
     *
     * @param key  the key to conditionally remove
     * @param uuid the expected UUID
     */
    public void removeBigStringUuidIfMatches(String key, long uuid) {
        var current = bigStringUuidByKey.get(key);
        if (current != null && current == uuid) {
            bigStringUuidByKey.remove(key);
            bigStringUuidSet.remove(uuid);
        }
    }

    /**
     * Returns the big string UUID for the given key, or null if not present.
     *
     * @param key the key to look up
     * @return the UUID, or null
     */
    @Nullable
    public Long getBigStringUuid(String key) {
        return bigStringUuidByKey.get(key);
    }

    /**
     * Checks whether the given UUID is present in the map.
     *
     * @param uuid the UUID to check
     * @return true if the UUID is mapped to any key
     */
    public boolean containsUuid(long uuid) {
        return bigStringUuidSet.contains(uuid);
    }

    /**
     * @return the number of entries in the UUID map
     */
    public int uuidMapSize() {
        return bigStringUuidByKey.size();
    }

    /**
     * Clears all entries from the UUID map and reverse index.
     */
    public void clearUuidMap() {
        bigStringUuidByKey.clear();
        bigStringUuidSet.clear();
    }

    /**
     * Populates the UUID map from persisted KeyBucket data for a WAL group's bucket range.
     * Called during startup after WAL reload to fill in entries that were persisted
     * and cleared from WAL. Only fills gaps (WAL entries take precedence).
     *
     * @param keyLoader     the KeyLoader to read persisted data from
     * @param walGroupIndex the WAL group index to populate for
     */
    public void populateFromKeyBuckets(@NotNull KeyLoader keyLoader, int walGroupIndex) {
        int oneCharge = ConfForSlot.global.confWal.oneChargeBucketNumber;
        int beginBucketIndex = oneCharge * walGroupIndex;

        var splitNumbers = keyLoader.getMetaKeyBucketSplitNumberBatch(beginBucketIndex, oneCharge);
        byte maxSplitNumber = 1;
        for (int i = 0; i < oneCharge; i++) {
            if (splitNumbers[i] > maxSplitNumber) {
                maxSplitNumber = splitNumbers[i];
            }
        }

        for (byte splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            var sharedBytes = keyLoader.readBatchInOneWalGroup(splitIndex, beginBucketIndex);
            if (sharedBytes == null) {
                continue;
            }

            for (int i = 0; i < oneCharge; i++) {
                int bucketIndex = beginBucketIndex + i;
                int position = KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i;
                if (position >= sharedBytes.length) {
                    continue;
                }
                if (!keyLoader.isBytesValidAsKeyBucket(sharedBytes, position)) {
                    continue;
                }

                var currentSplitNumber = splitNumbers[i];
                var result = KeyBucket.readWithFallback(slot, bucketIndex, splitIndex,
                        currentSplitNumber, sharedBytes, position, keyLoader.snowFlake);

                var keyBucket = result.keyBucket();
                keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
                    if (PersistValueMeta.isPvm(valueBytes)) {
                        return;
                    }
                    // cheap pre-check before expensive decode
                    if (valueBytes.length < 28
                            || CompressedValue.onlyReadSpType(valueBytes) != CompressedValue.SP_TYPE_BIG_STRING) {
                        return;
                    }
                    // WAL may have loaded a more recent value already; only fill gaps
                    var uuid = CompressedValue.getBigStringMetaUuid(valueBytes);
                    if (bigStringUuidByKey.putIfAbsent(key, uuid) == null) {
                        bigStringUuidSet.add(uuid);
                    }
                });
            }
        }
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();
        map.put("big_string_files_count", (double) bigStringFilesCount);
        map.put("big_string_missing_file_total", (double) bigStringMissingFileTotal);
        map.put("big_string_uuid_by_key_size", (double) bigStringUuidByKey.size());
        return map;
    }

    private static final Logger log = LoggerFactory.getLogger(BigStringFiles.class);

    /**
     * @param slot    the slot index
     * @param slotDir the directory of the slot
     */
    public BigStringFiles(short slot, @NullableOnlyTest File slotDir) throws IOException {
        this.slot = slot;
        this.bigStringDir = new File(slotDir, BIG_STRING_DIR_NAME);
        if (!bigStringDir.exists()) {
            if (!bigStringDir.mkdirs()) {
                throw new IOException("Create big string dir error, slot=" + slot);
            }
        }

        var maxSize = ConfForSlot.global.lruBigString.maxSize;
        if (maxSize > 0) {
            final var maybeOneBigStringBytesLength = 4096;
            var lruMemoryRequireMB = maxSize * maybeOneBigStringBytesLength / 1024 / 1024;
            log.info("LRU max size for big string={}, maybe one big string bytes length is {}B, memory require={}MB, slot={}",
                    maxSize,
                    maybeOneBigStringBytesLength,
                    lruMemoryRequireMB,
                    slot);
            log.info("LRU prepare, type={}, MB={}, slot={}", LRUPrepareBytesStats.Type.big_string, lruMemoryRequireMB, slot);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.big_string, String.valueOf(slot), lruMemoryRequireMB, false);

            this.bigStringBytesByUuidLRU = new LRUMap<>(maxSize);
        }

        var files = bigStringDir.listFiles();
        if (files != null) {
            for (var file : files) {
                if (file.isDirectory()) {
                    var bucketIndex = Integer.parseInt(file.getName());
                    bucketIndexesWhenFirstServerStart.add(bucketIndex);

                    var subFiles = file.listFiles();
                    if (subFiles == null) {
                        continue;
                    }

                    for (var subFile : subFiles) {
                        diskUsage += subFile.length();
                        bigStringFilesCount++;
                    }
                }

            }
        }
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = RamUsageEstimator.sizeOfMap(bigStringBytesByUuidLRU);
        sb.append("Big string files: ").append(size).append("\n");
        return size;
    }

    /**
     * @param bucketIndex the bucket index
     * @return the list of UUIDs
     */
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

    /**
     * Retrieves bytes of big string file by UUID, using the LRU cache.
     * Reads from LRU if present, otherwise reads from disk and seeds the LRU.
     * Equivalent to {@link #getBigStringBytes(long, int, long, boolean)} with {@code doLRUCache=true}.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @return the bytes of the big string file, or null if not found
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash) {
        return getBigStringBytes(uuid, bucketIndex, keyHash, true);
    }

    /**
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param doLRUCache  whether to cache the bytes in the LRU cache
     * @return the bytes of the big string file, or null if not found
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash, boolean doLRUCache) {
        if (!doLRUCache) {
            return readBigStringBytes(uuid, bucketIndex, keyHash);
        }

        var bytesCached = bigStringBytesByUuidLRU != null ? bigStringBytesByUuidLRU.get(uuid) : null;
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid, bucketIndex, keyHash);
        if (bytes != null && bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
        }
        return bytes;
    }

    private byte[] readBigStringBytes(long uuid, int bucketIndex, long keyHash) {
        var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
        if (!file.exists()) {
            bigStringMissingFileTotal++;
            log.error("Big string file not exists, uuid={}, slot={}", uuid, slot);
            return null;
        }

        try {
            var beginT = System.nanoTime();
            var bytes = FileUtils.readFileToByteArray(file);
            var costT = (System.nanoTime() - beginT) / 1000;

            // stats
            readByteLengthTotal += bytes.length;
            readFileCountTotal++;
            readFileCostTotalUs += costT;

            return bytes;
        } catch (IOException e) {
            log.error("Read big string file error, uuid={}, slot={}", uuid, slot, e);
            return null;
        }
    }

    public boolean writeBigStringBytes(long uuid, int bucketIndex, long keyHash, byte[] bytes) {
        return writeBigStringBytes(uuid, bucketIndex, keyHash, bytes, 0, bytes.length);
    }

    /**
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param bytes       the bytes to write
     * @param offset      the offset within the bytes array
     * @param length      the length of bytes to write
     * @return true if successful
     */
    public boolean writeBigStringBytes(long uuid, int bucketIndex, long keyHash, byte[] bytes, int offset, int length) {
        var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
        var len = file.exists() ? file.length() : 0;
        try {
            var beginT = System.nanoTime();
            FileUtils.writeByteArrayToFile(file, bytes, offset, length, false);
            var costT = (System.nanoTime() - beginT) / 1000;

            if (len == 0) {
                bigStringFilesCount++;
            }
            diskUsage += length - len;

            // stats
            writeByteLengthTotal += length;
            writeFileCountTotal++;
            writeFileCostTotalUs += costT;

            return true;
        } catch (IOException e) {
            log.error("Write big string file error, uuid={}, bucket index={}, slot={}", uuid, bucketIndex, slot, e);
            return false;
        }
    }

    /**
     * @param uuid        the UUID of the big string file to delete
     * @param bucketIndex the bucket index
     * @param keyHash     the hash of the key
     * @return true if the file was successfully deleted or doesn't exist
     */
    @TestOnly
    boolean deleteForceReturnFalseForTest = false;

    public boolean deleteBigStringFileIfExist(long uuid, int bucketIndex, long keyHash) {
        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.remove(uuid);
        }
        if (deleteForceReturnFalseForTest) {
            return false;
        }

        var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
        if (!file.exists()) {
            return true;
        }

        var len = file.length();
        var beginT = System.nanoTime();
        var r = file.delete();
        var costT = (System.nanoTime() - beginT) / 1000;

        if (r) {
            bigStringFilesCount--;
            diskUsage -= len;

            // stats
            deleteByteLengthTotal += len;
            deleteFileCountTotal++;
            deleteFileCostTotalUs += costT;
        }

        return r;
    }

    private static long getUuid(long uuid) {
        return uuid;
    }

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

    @Override
    public void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm) {
        if (shortStringCv == null) {
            return;
        }

        if (!shortStringCv.isBigString()) {
            return;
        }

        var uuid = shortStringCv.getBigStringMetaUuid();
        var bucketIndex = KeyHash.bucketIndex(shortStringCv.getKeyHash());
        var isDeleted = deleteBigStringFileIfExist(uuid, bucketIndex, shortStringCv.getKeyHash());
        // only remove if the current map entry still points to this uuid
        // (key may have been overwritten with a newer big string uuid since expiry)
        removeBigStringUuidIfMatches(key, uuid);
        if (!isDeleted) {
            throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
        } else {
            log.debug("Delete big string file, s={}, key={}, uuid={}", slot, key, uuid);
        }
    }
}
