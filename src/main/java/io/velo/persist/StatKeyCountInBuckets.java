package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.repl.SlaveReplay;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class StatKeyCountInBuckets implements InMemoryEstimate, NeedCleanUp {
    private static final String STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE = "stat_key_count_in_buckets.dat";
    // short is enough for one key bucket index total value count
    public static final int ONE_LENGTH = 2;

    private final int bucketsPerSlot;
    @VisibleForTesting
    final int allCapacity;
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    private final int[] keyCountInOneWalGroup;

    private long totalKeyCountCached;

    @SlaveReplay
    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

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

    private final Logger log = LoggerFactory.getLogger(getClass());

    public StatKeyCountInBuckets(short slot, File slotDir) throws IOException {
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
            log.warn("Read stat key count in buckets file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
        log.info("Key count in buckets: {}, slot: {}", calcKeyCount(), slot);
    }

    @Override
    public long estimate() {
        return allCapacity;
    }

    private void updateKeyCountForTargetWalGroup(int walGroupIndex, int keyCount) {
        var oldKeyCountInOneWalGroup = keyCountInOneWalGroup[walGroupIndex];
        keyCountInOneWalGroup[walGroupIndex] = keyCount;
        totalKeyCountCached += keyCount - oldKeyCountInOneWalGroup;
    }

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

    static void writeToRaf(int offset, byte[] tmpBytes, ByteBuffer inMemoryCachedByteBuffer, RandomAccessFile raf) {
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

    short getKeyCountForBucketIndex(int bucketIndex) {
        var offset = bucketIndex * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getShort(offset);
    }

    // perf bad if bucketsPerSlot = 512K when one slot will hold 100M keys
    // so cached by wal group
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

    long getKeyCount() {
        return totalKeyCountCached;
    }

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
