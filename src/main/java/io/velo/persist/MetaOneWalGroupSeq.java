package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import io.velo.repl.SlaveNeedReplay;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Manages the sequence numbers for different WAL (Write-Ahead Log) groups within a slot.
 * This class provides in-memory caching and file storage for sequence numbers, depending on the configuration.
 * It allows retrieval and update of sequence numbers for specific WAL groups.
 * This is used for replication, check if one WAL group sequence match, then can skip the WAL replication step.
 */
public class MetaOneWalGroupSeq implements InMemoryEstimate, NeedCleanUp {
    private static final String META_ONE_WAL_GROUP_SEQ_FILE = "meta_one_wal_group_seq.dat";

    private final int walGroupNumber;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    /**
     * Retrieves the in-memory cached bytes representing sequence numbers.
     * For save and reload.
     *
     * @return A copy of the in-memory cached bytes.
     */
    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    /**
     * Overwrites the in-memory cached bytes with the provided bytes.
     * For save and reload.
     *
     * @param bytes The bytes to overwrite the in-memory cache with.
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     */
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Meta one wal group seq, bytes length not match");
        }

        inMemoryCachedByteBuffer.position(0).put(bytes);
    }

    private static final Logger log = LoggerFactory.getLogger(MetaOneWalGroupSeq.class);

    /**
     * Constructs a new instance of MetaOneWalGroupSeq.
     * Initializes the in-memory cache and file storage for sequence numbers based on the provided slot and slot directory.
     *
     * @param slot    The slot index.
     * @param slotDir The directory where the slot data is stored.
     * @throws IOException If an I/O error occurs during file operations.
     */
    public MetaOneWalGroupSeq(short slot, @NotNull File slotDir) throws IOException {
        this.walGroupNumber = Wal.calcWalGroupNumber();
        // 8 bytes long seq for each one wal group, each split index
        this.allCapacity = 8 * walGroupNumber * KeyLoader.MAX_SPLIT_NUMBER;

        // max 8 * 512K / 16 * 9 = 2304KB
        this.inMemoryCachedBytes = new byte[allCapacity];

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_ONE_WAL_GROUP_SEQ_FILE);
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
            log.warn("Read one wal group seq file success, file={}, slot={}, all capacity={}KB",
                    file, slot, allCapacity / 1024);

            var sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append(inMemoryCachedBytes[i]).append(", ");
            }
            log.info("For debug: first 10 one wal group seq: [{}]", sb);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    /**
     * Estimates the capacity used by this instance.
     *
     * @param sb The StringBuilder to append the estimate details.
     * @return The estimated capacity.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Meta one wal group seq file: ").append(allCapacity).append("\n");
        return allCapacity;
    }

    /**
     * Retrieves the sequence number for a specific WAL group and split index.
     *
     * @param oneWalGroupIndex The index of the WAL group.
     * @param splitIndex       The split index.
     * @return The sequence number for the specified WAL group and split.
     */
    long get(int oneWalGroupIndex, byte splitIndex) {
        var offset = 8 * oneWalGroupIndex + 8 * walGroupNumber * splitIndex;
        return inMemoryCachedByteBuffer.getLong(offset);
    }

    /**
     * Sets the sequence number for a specific WAL group and split index.
     * If the configuration is set for pure memory operations, it directly updates the in-memory cache.
     * Otherwise, it writes the sequence number to the file and updates the in-memory cache.
     *
     * @param oneWalGroupIndex The index of the WAL group.
     * @param splitIndex       The split index.
     * @param seq              The sequence number to set.
     */
    @SlaveNeedReplay
    void set(int oneWalGroupIndex, byte splitIndex, long seq) {
        var offset = 8 * oneWalGroupIndex + 8 * walGroupNumber * splitIndex;
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.putLong(offset, seq);
            return;
        }

        try {
            raf.seek(offset);
            raf.writeLong(seq);
            inMemoryCachedByteBuffer.putLong(offset, seq);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Clears the sequence numbers in memory and on file if not operating in pure memory mode.
     * Resets the in-memory cache to zero.
     */
    void clear() {
        if (ConfForGlobal.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
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
            System.out.println("Meta one wal group seq file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}