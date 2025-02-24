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
 * Manages the split numbers for key buckets within a specific slot.
 * This class provides in-memory caching and file storage for the split numbers, depending on the configuration.
 * It allows retrieval and update of split numbers for individual buckets or batches of buckets.
 */
public class MetaKeyBucketSplitNumber implements InMemoryEstimate, NeedCleanUp {
    private static final String META_KEY_BUCKET_SPLIT_NUMBER_FILE = "meta_key_bucket_split_number.dat";

    private final byte initialSplitNumber;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    /**
     * Retrieves the in-memory cached bytes representing the split numbers.
     *
     * @return A copy of the in-memory cached bytes.
     */
    @SlaveReplay
    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    /**
     * Overwrites the in-memory cached bytes with the provided bytes.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the bytes to the file and updates the in-memory cache.
     *
     * @param bytes The bytes to overwrite the in-memory cache with.
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     * @throws RuntimeException         If an I/O error occurs during file operations.
     */
    @SlaveReplay
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl meta key bucket split number, bytes length not match");
        }

        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(0).put(bytes);
            return;
        }

        try {
            raf.seek(0);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(0).put(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Repl meta key bucket split number, write file error", e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MetaKeyBucketSplitNumber.class);

    /**
     * Constructs a new instance of MetaKeyBucketSplitNumber.
     * Initializes the in-memory cache and file storage for the split numbers based on the provided slot and slot directory.
     *
     * @param slot    The slot index.
     * @param slotDir The directory where the slot data is stored.
     * @throws IOException If an I/O error occurs during file operations.
     */
    public MetaKeyBucketSplitNumber(short slot, @NotNull File slotDir) throws IOException {
        this.allCapacity = ConfForSlot.global.confBucket.bucketsPerSlot;
        this.initialSplitNumber = ConfForSlot.global.confBucket.initialSplitNumber;
        log.info("Meta key bucket initial split number={}", initialSplitNumber);

        // max 512KB
        this.inMemoryCachedBytes = new byte[allCapacity];
        Arrays.fill(inMemoryCachedBytes, initialSplitNumber);

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_KEY_BUCKET_SPLIT_NUMBER_FILE);
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
            log.warn("Read meta key bucket split number file success, file={}, slot={}, all capacity={}KB",
                    file, slot, allCapacity / 1024);

            var sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append(inMemoryCachedBytes[i]).append(", ");
            }
            log.info("For debug: first 10 key bucket split number: [{}]", sb);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    /**
     * Estimates the size of the in-memory cached bytes and appends it to the provided StringBuilder.
     *
     * @param sb The StringBuilder to append the estimate to.
     * @return The estimated size in bytes.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Meta key bucket split number: ").append(allCapacity).append("\n");
        return allCapacity;
    }

    /**
     * Sets the split number for a specific bucket index.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the split number to the file and updates the in-memory cache.
     *
     * @param bucketIndex The index of the bucket.
     * @param splitNumber The split number to set.
     */
    @TestOnly
    void set(int bucketIndex, byte splitNumber) {
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.put(bucketIndex, splitNumber);
            return;
        }

        try {
            raf.seek(bucketIndex);
            raf.writeByte(splitNumber);
            inMemoryCachedByteBuffer.put(bucketIndex, splitNumber);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets the split numbers for a batch of buckets starting from a specific index.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the split numbers to the file and updates the in-memory cache.
     *
     * @param beginBucketIndex The starting index of the batch.
     * @param splitNumberArray The array of split numbers to set.
     */
    void setBatch(int beginBucketIndex, byte[] splitNumberArray) {
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(beginBucketIndex).put(splitNumberArray);
            return;
        }

        try {
            raf.seek(beginBucketIndex);
            raf.write(splitNumberArray);
            inMemoryCachedByteBuffer.position(beginBucketIndex).put(splitNumberArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the split numbers for a batch of buckets starting from a specific index.
     *
     * @param beginBucketIndex The starting index of the batch.
     * @param bucketCount      The number of buckets in the batch.
     * @return An array of split numbers for the batch.
     */
    byte[] getBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount];
        inMemoryCachedByteBuffer.position(beginBucketIndex).get(dst);
        return dst;
    }

    /**
     * Retrieves the split number for a specific bucket index.
     *
     * @param bucketIndex The index of the bucket.
     * @return The split number for the specified bucket.
     */
    byte get(int bucketIndex) {
        return inMemoryCachedByteBuffer.get(bucketIndex);
    }

    /**
     * Finds the maximum split number among all buckets.
     *
     * @return The maximum split number.
     */
    byte maxSplitNumber() {
        byte max = 1;
        for (int j = 0; j < allCapacity; j++) {
            var splitNumber = inMemoryCachedByteBuffer.get(j);
            if (splitNumber > max) {
                max = splitNumber;
            }
        }
        return max;
    }

    /**
     * Clears all split numbers, setting them to the initial split number.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the initial split numbers to the file and updates the in-memory cache.
     */
    void clear() {
        if (ConfForGlobal.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, initialSplitNumber);
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            Arrays.fill(tmpBytes, initialSplitNumber);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Cleans up the resources used by this instance.
     * If operating in pure memory mode, no action is taken.
     * Otherwise, it closes the RandomAccessFile.
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
            System.out.println("Meta key bucket split number file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}