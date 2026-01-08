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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Manages big string files stored in the file system or in-memory.
 * This class provides methods to read, write, delete, and manage big string files
 * using an LRU cache in a specified slot.
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

    private static final String BIG_STRING_DIR_NAME = "big-string";

    private LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    int bigStringFilesCount = 0;

    HashSet<Integer> bucketIndexesWhenFirstServerStart = new HashSet<>();

    /**
     * Collects metrics related to this instance.
     *
     * @return the map of metric names to their corresponding values
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
     * @param slot    the slot index
     * @param slotDir the directory of the slot
     * @throws IOException if an I/O error occurs
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

    /**
     * Estimates the memory usage of this instance.
     *
     * @param sb the StringBuilder to append the memory usage estimate
     * @return the estimated memory usage in bytes
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = RamUsageEstimator.sizeOfMap(bigStringBytesByUuidLRU);
        sb.append("Big string files: ").append(size).append("\n");
        return size;
    }

    /**
     * Retrieves a list of UUIDs of big string files.
     *
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
     * Retrieves bytes of big string file corresponding to the given UUID from the cache or file system.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @return the bytes of the big string file, or null if not found
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash) {
        return getBigStringBytes(uuid, bucketIndex, keyHash, false);
    }

    /**
     * Retrieves bytes of big string file corresponding to the given UUID from the cache or file system.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param doLRUCache  whether to cache the bytes in the LRU cache
     * @return the bytes of the big string file, or null if not found
     */
    public byte[] getBigStringBytes(long uuid, int bucketIndex, long keyHash, boolean doLRUCache) {
        var bytesCached = bigStringBytesByUuidLRU != null ? bigStringBytesByUuidLRU.get(uuid) : null;
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid, bucketIndex, keyHash);
        if (bytes != null && doLRUCache && bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
        }
        return bytes;
    }

    /**
     * Reads bytes of a big string file from the file system.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @return the bytes of the big string file, or null if the file doesn't exist or an error occurs
     */
    private byte[] readBigStringBytes(long uuid, int bucketIndex, long keyHash) {
        var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
        if (!file.exists()) {
            log.warn("Big string file not exists, uuid={}, slot={}", uuid, slot);
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

    /**
     * Writes bytes to a big string file.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param keyHash     the hash of the key
     * @param bytes       the bytes to write
     * @return true if the operation was successful, false otherwise
     */
    public boolean writeBigStringBytes(long uuid, int bucketIndex, long keyHash, byte[] bytes) {
        return writeBigStringBytes(uuid, bucketIndex, keyHash, bytes, 0, bytes.length);
    }

    /**
     * Writes bytes to a big string file.
     *
     * @param uuid        the UUID of the big string file
     * @param bucketIndex the bucket index
     * @param bytes       the bytes to write
     * @param offset      the offset within the bytes array
     * @param length      the length of bytes to write
     * @return true if the operation was successful, false otherwise
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
            diskUsage += bytes.length - len;

            // stats
            writeByteLengthTotal += bytes.length;
            writeFileCountTotal++;
            writeFileCostTotalUs += costT;

            return true;
        } catch (IOException e) {
            log.error("Write big string file error, uuid={}, bucket index={}, slot={}", uuid, bucketIndex, slot, e);
            return false;
        }
    }

    /**
     * Deletes a big string file if it exists.
     *
     * @param uuid        the UUID of the big string file to delete
     * @param bucketIndex the bucket index
     * @param keyHash     the hash of the key
     * @return true if the file was successfully deleted or doesn't exist, false otherwise
     */
    public boolean deleteBigStringFileIfExist(long uuid, int bucketIndex, long keyHash) {
        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.remove(uuid);
        }

        var file = new File(bigStringDir, bucketIndex + "/" + uuid + "_" + keyHash);
        if (!file.exists()) {
            return true;
        }

        bigStringFilesCount--;
        var len = file.length();
        diskUsage -= len;

        var beginT = System.nanoTime();
        var r = file.delete();
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        deleteByteLengthTotal += len;
        deleteFileCountTotal++;
        deleteFileCostTotalUs += costT;

        return r;
    }

    private static long getUuid(long uuid) {
        return uuid;
    }

    /**
     * Deletes all big string files.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public void deleteAllBigStringFiles() {
        if (bigStringBytesByUuidLRU != null) {
            bigStringBytesByUuidLRU.clear();
        }

        try {
            FileUtils.cleanDirectory(bigStringDir);
            log.warn("Delete all big string files, count={}, slot={}", bigStringFilesCount, slot);
            bigStringFilesCount = 0;
            diskUsage = 0L;
        } catch (IOException e) {
            log.error("Delete all big string files error, slot={}", slot, e);
        }
    }

    /**
     * Handles expiration or deletion of a compressed value.
     *
     * @param key           the key associated with the value
     * @param shortStringCv the compressed value (maybe null)
     * @param pvm           the persist value metadata (maybe null)
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
        var bucketIndex = KeyHash.bucketIndex(shortStringCv.getKeyHash());
        var isDeleted = deleteBigStringFileIfExist(uuid, bucketIndex, shortStringCv.getKeyHash());
        if (!isDeleted) {
            throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
        } else {
            log.debug("Delete big string file, s={}, key={}, uuid={}", slot, key, uuid);
        }
    }
}