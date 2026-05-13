package io.velo.persist;

import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
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
 * Manages the count of keys in different buckets within a slot.
 */
public class StatKeyCountInBuckets implements InMemoryEstimate, NeedCleanUp {
    private static final String STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE = "stat_key_count_in_buckets.dat";
    // short is enough for one key bucket index total value count
    public static final int ONE_LENGTH = 2;

    private final int bucketsPerSlot;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private final RandomAccessFile raf;

    private final int[] keyCountInOneWalGroup;

    /**
     * @param walGroupIndex the index of the WAL group
     * @return the total key count for the specified WAL group
     */
    public int getKeyCountForOneWalGroup(int walGroupIndex) {
        return keyCountInOneWalGroup[walGroupIndex];
    }

    private long totalKeyCountCached;

    private static final Logger log = LoggerFactory.getLogger(StatKeyCountInBuckets.class);

    /**
     * @param slot slot index
     * @param slotDir directory of the slot
     * @throws IOException If an I/O error occurs during file operations
     */
    public StatKeyCountInBuckets(short slot, @NotNull File slotDir) throws IOException {
        this.bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;
        this.allCapacity = bucketsPerSlot * ONE_LENGTH;

        var walGroupNumber = Wal.calcWalGroupNumber();
        this.keyCountInOneWalGroup = new int[walGroupNumber];

        // max 512KB * 2 = 1MB
        this.inMemoryCachedBytes = new byte[allCapacity];

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
     * @param sb the StringBuilder to append the estimate details to
     * @return the estimated capacity
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Stat key count in buckets: ").append(allCapacity).append("\n");
        return allCapacity;
    }

    /**
     * @param walGroupIndex the index of the WAL group
     * @param keyCount the new key count for the WAL group
     */
    private void updateKeyCountForTargetWalGroup(int walGroupIndex, int keyCount) {
        var oldKeyCountInOneWalGroup = keyCountInOneWalGroup[walGroupIndex];
        keyCountInOneWalGroup[walGroupIndex] = keyCount;
        totalKeyCountCached += keyCount - oldKeyCountInOneWalGroup;
    }

    /**
     * @param walGroupIndex the index of the WAL group
     * @param beginBucketIndex the index of the first bucket in the batch
     * @param keyCountArray the array of key counts for the batch of buckets
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
     * @param offset the offset in the file to write the bytes
     * @param tmpBytes the bytes to write
     * @param inMemoryCachedByteBuffer the in-memory cache to update
     * @param raf the RandomAccessFile to write to
     */
    static void writeToRaf(int offset,
                           byte[] tmpBytes,
                           @NotNull ByteBuffer inMemoryCachedByteBuffer,
                           @NotNull RandomAccessFile raf) {
        try {
            raf.seek(offset);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.put(offset, tmpBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param bucketIndex the index of the bucket
     * @param keyCount the key count to set
     */
    @TestOnly
    void setKeyCountForBucketIndex(int bucketIndex, short keyCount) {
        var offset = bucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.putShort(offset, keyCount);
    }

    /**
     * @param bucketIndex the index of the bucket
     * @return the key count for the specified bucket
     */
    short getKeyCountForBucketIndex(int bucketIndex) {
        var offset = bucketIndex * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getShort(offset);
    }

    /**
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
     * @return the total key count
     */
    long getKeyCount() {
        return totalKeyCountCached;
    }

    void clear() {
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

    @Override
    public void cleanUp() {
        try {
            raf.close();
            System.out.println("Stat key count in buckets file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}