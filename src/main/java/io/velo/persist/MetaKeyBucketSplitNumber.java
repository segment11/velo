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
 * Manages split numbers for key buckets within a specific slot.
 */
public class MetaKeyBucketSplitNumber implements InMemoryEstimate, NeedCleanUp {
    private static final String META_KEY_BUCKET_SPLIT_NUMBER_FILE = "meta_key_bucket_split_number.dat";

    private final byte initialSplitNumber;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private final RandomAccessFile raf;

    private static final Logger log = LoggerFactory.getLogger(MetaKeyBucketSplitNumber.class);

    /**
     * @param slot slot index
     * @param slotDir directory of the slot
     * @throws IOException If an I/O error occurs during file operations
     */
    public MetaKeyBucketSplitNumber(short slot, @NotNull File slotDir) throws IOException {
        this.allCapacity = ConfForSlot.global.confBucket.bucketsPerSlot;
        this.initialSplitNumber = ConfForSlot.global.confBucket.initialSplitNumber;
        log.info("Meta key bucket initial split number={}", initialSplitNumber);

        // max 512KB
        this.inMemoryCachedBytes = new byte[allCapacity];
        Arrays.fill(inMemoryCachedBytes, initialSplitNumber);

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
     * @param sb the StringBuilder to append the estimate to
     * @return the estimated size in bytes
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Meta key bucket split number: ").append(allCapacity).append("\n");
        return allCapacity;
    }

    /**
     * @param bucketIndex the index of the bucket
     * @param splitNumber the split number to set
     */
    @TestOnly
    void set(int bucketIndex, byte splitNumber) {
        try {
            raf.seek(bucketIndex);
            raf.writeByte(splitNumber);
            inMemoryCachedByteBuffer.put(bucketIndex, splitNumber);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param beginBucketIndex the starting index of the batch
     * @param splitNumberArray the array of split numbers to set
     */
    void setBatch(int beginBucketIndex, byte[] splitNumberArray) {
        try {
            raf.seek(beginBucketIndex);
            raf.write(splitNumberArray);
            inMemoryCachedByteBuffer.position(beginBucketIndex).put(splitNumberArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param beginBucketIndex the starting index of the batch
     * @param bucketCount the number of buckets in the batch
     * @return the array of split numbers for the batch
     */
    byte[] getBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount];
        inMemoryCachedByteBuffer.position(beginBucketIndex).get(dst);
        return dst;
    }

    /**
     * @param bucketIndex the index of the bucket
     * @return the split number for the specified bucket
     */
    byte get(int bucketIndex) {
        return inMemoryCachedByteBuffer.get(bucketIndex);
    }

    /**
     * @return the maximum split number
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

    void clear() {
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

    @Override
    public void cleanUp() {
        try {
            raf.close();
            System.out.println("Meta key bucket split number file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}