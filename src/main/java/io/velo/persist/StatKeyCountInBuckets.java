package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.repl.SlaveReplay;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Manages the count of keys in different buckets within a slot. This class provides in-memory caching and file storage
 * for the key counts, depending on the configuration. It also supports operations to update and retrieve key counts
 * for specific buckets and WAL groups.
 */
public class StatKeyCountInBuckets implements InMemoryEstimate, NeedCleanUp {
    private static final String STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE = "stat_key_count_in_buckets.dat";
    // short is enough for one key bucket index total value count
    public static final int ONE_LENGTH = 2;

    private final int bucketsPerSlot;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    /**
     * Retrieves the in-memory cached bytes representing key counts.
     *
     * @return the copy of the in-memory cached bytes
     */
    @SlaveReplay
    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    /**
     * Overwrites the in-memory cached bytes with the provided bytes.
     * If the configuration is set for pure memory operations, it directly updates the in-memory cache.
     * Otherwise, it writes the bytes to the file and updates the in-memory cache.
     *
     * @param bytes the bytes to overwrite the in-memory cache with
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     */
    @SlaveReplay
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl stat key count in buckets, bytes length not match");
        }

        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(0).put(bytes);
            calcKeyCount();
            return;
        }

        try {
            raf.seek(0);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(0).put(bytes);
            calcKeyCount();
        } catch (IOException e) {
            throw new RuntimeException("Repl stat key count in buckets, write file error", e);
        }
    }

    private final int[] keyCountInOneWalGroup;

    /**
     * Retrieves the total key count for a specific WAL group.
     *
     * @param walGroupIndex the index of the WAL group
     * @return the total key count for the specified WAL group
     */
    public int getKeyCountForOneWalGroup(int walGroupIndex) {
        return keyCountInOneWalGroup[walGroupIndex];
    }

    private long totalKeyCountCached;

    private static final Logger log = LoggerFactory.getLogger(StatKeyCountInBuckets.class);

    /**
     * Constructs a new instance of StatKeyCountInBuckets.
     * Initializes the in-memory cache and file storage for key counts based on the provided slot and slot directory.
     *
     * @param slot    the slot index
     * @param slotDir the directory of the slot
     * @throws IOException If an I/O error occurs during file operations.
     */
    public StatKeyCountInBuckets(short slot, @NotNull File slotDir) throws IOException {
        this.bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;
        this.allCapacity = bucketsPerSlot * ONE_LENGTH;

        var walGroupNumber = Wal.calcWalGroupNumber();
        this.keyCountInOneWalGroup = new int[walGroupNumber];

        // max 512KB * 2 = 1MB
        this.inMemoryCachedBytes = new byte[allCapacity];

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);
            FileUtils.writeByteArrayToFile(file, this.inMemoryCachedBytes, true);
        } else {
            needRead = true;
        }
        this.raf = new RandomAccessFile(file, "rw");

        if (needRead) {
            raf.seek(0);
            raf.read(inMemoryCachedBytes);
            log.warn("Read stat key count in buckets file success, file={}, slot={}, all capacity={}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
        log.info("Key count in buckets={}, slot={}", calcKeyCount(), slot);
    }

    /**
     * Estimates the capacity used by this instance.
     *
     * @param sb the StringBuilder to append the estimate details to
     * @return the estimated capacity
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Stat key count in buckets: ").append(allCapacity).append("\n");
        return allCapacity;
    }

    /**
     * Updates the key count for a specific WAL group.
     * Adjusts the total key count cached accordingly.
     *
     * @param walGroupIndex the index of the WAL group
     * @param keyCount      the new key count for the WAL group
     */
    private void updateKeyCountForTargetWalGroup(int walGroupIndex, int keyCount) {
        var oldKeyCountInOneWalGroup = keyCountInOneWalGroup[walGroupIndex];
        keyCountInOneWalGroup[walGroupIndex] = keyCount;
        totalKeyCountCached += keyCount - oldKeyCountInOneWalGroup;
    }

    /**
     * Sets the key count for a batch of buckets within a specific WAL group.
     * Updates the in-memory cache and file storage, and adjusts the key count for the WAL group.
     *
     * @param walGroupIndex    the index of the WAL group
     * @param beginBucketIndex the index of the first bucket in the batch
     * @param keyCountArray    the array of key counts for the batch of buckets
     */
    void setKeyCountBatch(int walGroupIndex, int beginBucketIndex, short[] keyCountArray) {
        var offset = beginBucketIndex * ONE_LENGTH;

        var tmpBytes = new byte[keyCountArray.length * ONE_LENGTH];
        var tmpByteBuffer = ByteBuffer.wrap(tmpBytes);

        int totalKeyCountInTargetWalGroup = 0;
        for (short keyCount : keyCountArray) {
            totalKeyCountInTargetWalGroup += keyCount;
            tmpByteBuffer.putShort(keyCount);
        }

        writeToRaf(offset, tmpBytes, inMemoryCachedByteBuffer, raf);

        updateKeyCountForTargetWalGroup(walGroupIndex, totalKeyCountInTargetWalGroup);
    }

    /**
     * Writes bytes to the RandomAccessFile and updates the in-memory cache.
     * If the configuration is set for pure memory operations, it only updates the in-memory cache.
     *
     * @param offset                   the offset in the file to write the bytes
     * @param tmpBytes                 the bytes to write
     * @param inMemoryCachedByteBuffer the in-memory cache to update
     * @param raf                      the RandomAccessFile to write to
     */
    static void writeToRaf(int offset,
                           byte[] tmpBytes,
                           @NotNull ByteBuffer inMemoryCachedByteBuffer,
                           @NotNull RandomAccessFile raf) {
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.put(offset, tmpBytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.put(offset, tmpBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets the key count for a specific bucket index in the in-memory cache.
     * This method is intended for testing purposes.
     *
     * @param bucketIndex the index of the bucket
     * @param keyCount    the key count to set
     */
    @TestOnly
    void setKeyCountForBucketIndex(int bucketIndex, short keyCount) {
        var offset = bucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.putShort(offset, keyCount);
    }

    /**
     * Retrieves the key count for a specific bucket index from the in-memory cache.
     *
     * @param bucketIndex the index of the bucket
     * @return the key count for the specified bucket
     */
    short getKeyCountForBucketIndex(int bucketIndex) {
        var offset = bucketIndex * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getShort(offset);
    }

    /**
     * Calculates the total key count by summing the key counts in all buckets.
     * Updates the key count for each WAL group based on the configuration.
     *
     * @return the total key count
     */
    private long calcKeyCount() {
        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;

        long totalKeyCount = 0;
        int tmpKeyCountInOneWalGroup = 0;
        for (int i = 0; i < bucketsPerSlot; i++) {
            var offset = i * ONE_LENGTH;
            var keyCountInOneKeyBucketIndex = inMemoryCachedByteBuffer.getShort(offset);
            totalKeyCount += keyCountInOneKeyBucketIndex;
            tmpKeyCountInOneWalGroup += keyCountInOneKeyBucketIndex;

            if (i % oneChargeBucketNumber == oneChargeBucketNumber - 1) {
                keyCountInOneWalGroup[i / oneChargeBucketNumber] = tmpKeyCountInOneWalGroup;
                tmpKeyCountInOneWalGroup = 0;
            }
        }
        totalKeyCountCached = totalKeyCount;
        return totalKeyCount;
    }

    /**
     * Retrieves the total key count cached.
     *
     * @return the total key count
     */
    long getKeyCount() {
        return totalKeyCountCached;
    }

    /**
     * Clears the key counts in memory and on file if not operating in pure memory mode.
     * Resets the total key count cached to zero.
     */
    void clear() {
        if (ConfForGlobal.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            Arrays.fill(keyCountInOneWalGroup, 0);
            totalKeyCountCached = 0;
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            Arrays.fill(tmpBytes, (byte) 0);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
            Arrays.fill(keyCountInOneWalGroup, 0);
            totalKeyCountCached = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Cleans up the resources used by this instance.
     * If operating in pure memory mode, no action is taken.
     * Otherwise, it flushes and closes the RandomAccessFile.
     */
    @Override
    public void cleanUp() {
        if (ConfForGlobal.pureMemory) {
            return;
        }

        // sync all
        try {
//            raf.getFD().sync();
            raf.close();
            System.out.println("Stat key count in buckets file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}