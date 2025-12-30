package io.velo.persist;

import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Manages big string files stored in the file system or in-memory.
 * This class provides methods to read, write, delete, and manage big string files
 * using an LRU cache in a specified slot.
 */
public class BigStringFiles implements InMemoryEstimate, InSlotMetricCollector, CanSaveAndLoad, HandlerWhenCvExpiredOrDeleted {
    record Id(long uuid, int bucketIndex) {
    }

    record IdWithKey(long uuid, String key) {
    }


    private final short slot;
    final File bigStringDir;

    long diskUsage = 0L;

    long readByteLengthTotal = 0L;
    long readFileCountTotal = 0L;
    long readFileCostMsTotal = 0L;

    long writeByteLengthTotal = 0L;
    long writeFileCountTotal = 0L;
    long writeFileCostMsTotal = 0L;

    private static final String BIG_STRING_DIR_NAME = "big-string";

    /**
     * Special UUID value to indicate no UUID should be skipped.
     */
    public static final int SKIP_UUID = -1;

    private LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    private final HashMap<Integer, HashMap<Long, byte[]>> allBytesByBucketIndexAndUuid = new HashMap<>();

    int bigStringFilesCount = 0;

    HashSet<Integer> bucketIndexesWhenFirstServerStart = new HashSet<>();

    /**
     * Collects metrics related to this instance.
     *
     * @return A map of metric names to their corresponding values.
     */
    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();
        map.put("big_string_files_count", (double) bigStringFilesCount);
        return map;
    }

    private static final Logger log = LoggerFactory.getLogger(BigStringFiles.class);

    /**
     * Constructs a new BigStringFiles instance for a given slot and directory.
     *
     * @param slot    The slot number.
     * @param slotDir The directory where big string files are stored.
     * @throws IOException if an I/O error occurs.
     */
    public BigStringFiles(short slot, @NullableOnlyTest File slotDir) throws IOException {
        this.slot = slot;
        if (ConfForGlobal.pureMemory) {
            log.warn("Pure memory mode, big string files will not be used, slot={}", slot);
            this.bigStringDir = null;
            this.bigStringBytesByUuidLRU = null;
            return;
        }

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
        bigStringFilesCount = files != null ? files.length : 0;

        if (files != null) {
            for (var file : files) {
                diskUsage += file.length();
                var arr = file.getName().split("_");
                if (arr.length == 2) {
                    bucketIndexesWhenFirstServerStart.add(Integer.parseInt(arr[0]));
                }
            }
        }
    }

    /**
     * Estimates the memory usage of this instance.
     *
     * @param sb StringBuilder to append the memory usage estimate.
     * @return The estimated memory usage in bytes.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = RamUsageEstimator.sizeOfMap(bigStringBytesByUuidLRU);
        size += RamUsageEstimator.sizeOfMap(allBytesByBucketIndexAndUuid);
        sb.append("Big string files: ").append(size).append("\n");
        return size;
    }

    /**
     * Loads big string data from a saved file when in pure memory mode.
     *
     * @param is DataInputStream to read from.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        // count int
        bigStringFilesCount = is.readInt();
        for (int i = 0; i < bigStringFilesCount; i++) {
            // uuid long, bytes length int
            var uuid = is.readLong();
            var bucketIndex = is.readInt();
            var bytesLength = is.readInt();
            var bytes = new byte[bytesLength];
            is.readFully(bytes);
            allBytesByBucketIndexAndUuid.computeIfAbsent(bucketIndex, k -> new HashMap<>()).put(uuid, bytes);
        }
        log.warn("Load big string files from last saved file, count={}", bigStringFilesCount);
    }

    /**
     * Writes big string data to a saved file when in pure memory mode.
     *
     * @param os DataOutputStream to write to.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.writeInt(allBytesByBucketIndexAndUuid.size());
        for (var entry : allBytesByBucketIndexAndUuid.entrySet()) {
            var bucketIndex = entry.getKey();
            for (var entry2 : entry.getValue().entrySet()) {
                var uuid = entry2.getKey();
                var bytes = entry2.getValue();
                os.writeLong(uuid);
                os.writeInt(bucketIndex);
                os.writeInt(bytes.length);
                os.write(bytes);
            }
        }
    }

    /**
     * Retrieves a list of UUIDs of big string files.
     *
     * @param bucketIndex The bucket index.
     * @return A list of UUIDs.
     */
    public List<Long> getBigStringFileUuidList(int bucketIndex) {
        var list = new ArrayList<Long>();
        if (ConfForGlobal.pureMemory) {
            var map = allBytesByBucketIndexAndUuid.get(bucketIndex);
            if (map != null) {
                list.addAll(map.keySet());
            }
            return list;
        }

        var files = bigStringDir.listFiles();
        if (files == null) {
            return list;
        }

        for (File file : files) {
            var fileName = file.getName();
            var arr = fileName.split("_");
            if (arr.length != 2) {
                continue;
            }
            if (bucketIndex != Integer.parseInt(arr[0])) {
                continue;
            }
            var uuid = Long.parseLong(arr[1]);
            list.add(uuid);
        }
        return list;
    }

    /**
     * Retrieves bytes of big string file corresponding to the given UUID from the cache or file system.
     *
     * @param uuid        UUID of the big string file.
     * @param bucketIndex The bucket index.
     * @return Bytes of the big string file or null if not found.
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex) {
        return getBigStringBytes(uuid, bucketIndex, false);
    }

    /**
     * Retrieves bytes of big string file corresponding to the given UUID from the cache or file system.
     *
     * @param uuid        UUID of the big string file.
     * @param bucketIndex The bucket index.
     * @param doLRUCache  Whether to cache the bytes in the LRU cache.
     * @return Bytes of the big string file or null if not found.
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex, boolean doLRUCache) {
        if (ConfForGlobal.pureMemory) {
            var map = allBytesByBucketIndexAndUuid.get(bucketIndex);
            if (map == null) {
                return null;
            }
            return map.get(uuid);
        }

        var bytesCached = bigStringBytesByUuidLRU != null ? bigStringBytesByUuidLRU.get(uuid) : null;
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid, bucketIndex);
        if (bytes != null && doLRUCache && bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
        }
        return bytes;
    }

    /**
     * Reads bytes of a big string file from the file system.
     *
     * @param uuid        UUID of the big string file.
     * @param bucketIndex The bucket index.
     * @return Bytes of the big string file or null if the file doesn't exist or an error occurs.
     */
    private byte[] readBigStringBytes(long uuid, int bucketIndex) {
        var file = new File(bigStringDir, bucketIndex + "_" + uuid);
        if (!file.exists()) {
            log.warn("Big string file not exists, uuid={}, slot={}", uuid, slot);
            return null;
        }

        try {
            var beginT = System.currentTimeMillis();
            var bytes = FileUtils.readFileToByteArray(file);
            var costT = System.currentTimeMillis() - beginT;

            // stats
            readByteLengthTotal += bytes.length;
            readFileCountTotal++;
            readFileCostMsTotal += costT;

            return bytes;
        } catch (IOException e) {
            log.error("Read big string file error, uuid={}, slot={}", uuid, slot, e);
            return null;
        }
    }

    /**
     * Writes bytes to a big string file.
     *
     * @param uuid        UUID of the big string file.
     * @param key         Key associated with the big string.
     * @param bucketIndex The bucket index.
     * @param bytes       Bytes to write.
     * @return true if the operation was successful; false otherwise.
     */
    public boolean writeBigStringBytes(long uuid, @NotNull String key, int bucketIndex, byte[] bytes) {
        return writeBigStringBytes(uuid, key, bucketIndex, bytes, 0, bytes.length);
    }

    /**
     * Writes bytes to a big string file.
     *
     * @param uuid        UUID of the big string file.
     * @param key         Key associated with the big string.
     * @param bucketIndex The bucket index.
     * @param bytes       Bytes to write.
     * @param offset      Offset within the bytes array.
     * @param length      Length of bytes to write.
     * @return true if the operation was successful; false otherwise.
     */
    public boolean writeBigStringBytes(long uuid, @NotNull String key, int bucketIndex, byte[] bytes, int offset, int length) {
        if (ConfForGlobal.pureMemory) {
            var r = allBytesByBucketIndexAndUuid.computeIfAbsent(bucketIndex, k -> new HashMap<>()).put(uuid, bytes);
            if (r == null) {
                bigStringFilesCount++;
            }
            return true;
        }

        var file = new File(bigStringDir, bucketIndex + "_" + uuid);
        try {
            var beginT = System.currentTimeMillis();
            FileUtils.writeByteArrayToFile(file, bytes, offset, length, false);
            var costT = System.currentTimeMillis() - beginT;

            bigStringFilesCount++;
            diskUsage += bytes.length;

            // stats
            writeByteLengthTotal += bytes.length;
            writeFileCountTotal++;
            writeFileCostMsTotal += costT;

            return true;
        } catch (IOException e) {
            log.error("Write big string file error, uuid={}, key={}, slot={}", uuid, key, slot, e);
            return false;
        }
    }

    /**
     * Deletes a big string file if it exists.
     *
     * @param uuid        UUID of the big string file to delete.
     * @param bucketIndex The bucket index.
     * @return true if the file was successfully deleted or doesn't exist; false otherwise.
     */
    public boolean deleteBigStringFileIfExist(long uuid, int bucketIndex) {
        if (ConfForGlobal.pureMemory) {
            var r = allBytesByBucketIndexAndUuid.computeIfAbsent(bucketIndex, k -> new HashMap<>()).remove(uuid);
            if (r != null) {
                bigStringFilesCount--;
            }
            return true;
        }

        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.remove(uuid);
        }

        var file = new File(bigStringDir, bucketIndex + "_" + uuid);
        if (file.exists()) {
            bigStringFilesCount--;
            diskUsage -= file.length();
            return file.delete();
        } else {
            return true;
        }
    }

    /**
     * Deletes all big string files.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public void deleteAllBigStringFiles() {
        if (ConfForGlobal.pureMemory) {
            allBytesByBucketIndexAndUuid.clear();
            return;
        }

        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.clear();
        }

        try {
            FileUtils.cleanDirectory(bigStringDir);
            log.warn("Delete all big string files, slot={}", slot);
            bigStringFilesCount = 0;
            diskUsage = 0L;
        } catch (IOException e) {
            log.error("Delete all big string files error, slot={}", slot, e);
        }
    }

    /**
     * Handles expiration or deletion of a compressed value.
     *
     * @param key           The key associated with the value.
     * @param shortStringCv The compressed value (maybe null).
     * @param pvm           The persist value metadata (maybe null).
     */
    @Override
    public void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm) {
        if (shortStringCv == null) {
            return;
        }

        if (!shortStringCv.isBigString()) {
            return;
        }

        var uuid = shortStringCv.getBigStringMetaUuid();
        var bucketIndex = KeyHash.bucketIndex(shortStringCv.getKeyHash(), ConfForSlot.global.confBucket.bucketsPerSlot);
        var isDeleted = deleteBigStringFileIfExist(uuid, bucketIndex);
        if (!isDeleted) {
            throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
        } else {
            log.debug("Delete big string file, s={}, key={}, uuid={}", slot, key, uuid);
        }
    }
}