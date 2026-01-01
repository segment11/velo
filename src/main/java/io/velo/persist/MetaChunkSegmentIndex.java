package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import io.velo.repl.Binlog;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Manages the chunk segment index and related metadata for a specific slot.
 * This class provides in-memory caching and file storage for metadata, depending on the configuration.
 * It allows retrieval and update of metadata such as the chunk segment index, master UUID, binlog file index, and offset.
 */
public class MetaChunkSegmentIndex implements NeedCleanUp {
    private static final String META_CHUNK_SEGMENT_INDEX_FILE = "meta_chunk_segment_index.dat";

    private final short slot;

    // 4 bytes for chunk segment index int
    // when slave connect master, master start binlog
    // 8 bytes for master uuid long
    // 4 bytes for target master exists data all fetched done flag int
    // 4 bytes for master binlog file index int
    // 8 bytes for master binlog offset long
    final int allCapacity = 4 + 8 + 4 + 4 + 8;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    /**
     * Retrieves the in-memory cached bytes representing the metadata.
     * For save and reload
     *
     * @return the copy of the in-memory cached bytes
     */
    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    /**
     * Overwrites the in-memory cached bytes with the provided bytes.
     * For save and reload
     *
     * @param bytes the bytes to overwrite the in-memory cache with
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     */
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Meta chunk segment index, bytes length not match");
        }

        inMemoryCachedByteBuffer.position(0).put(bytes);
    }

    private static final Logger log = LoggerFactory.getLogger(MetaChunkSegmentIndex.class);

    /**
     * Constructs a new instance of MetaChunkSegmentIndex.
     * Initializes the in-memory cache and file storage for metadata based on the provided slot and slot directory.
     *
     * @param slot    the slot index
     * @param slotDir the directory where the slot data is stored
     * @throws IOException If an I/O error occurs during file operations.
     */
    public MetaChunkSegmentIndex(short slot, @NotNull File slotDir) throws IOException {
        this.slot = slot;
        this.inMemoryCachedBytes = new byte[allCapacity];

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_INDEX_FILE);
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
            ByteBuffer tmpBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            log.warn("Read meta chunk segment index file success, file={}, slot={}, segment index={}, " +
                            "master binlog file index={}, master binlog offset={}",
                    file, slot, tmpBuffer.getInt(0), tmpBuffer.getInt(16), tmpBuffer.getLong(20));
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    /**
     * Sets the chunk segment index.
     * If the configuration is set for pure memory operations, it directly updates the in-memory cache.
     * Otherwise, it writes the segment index to the file and updates the in-memory cache.
     *
     * @param segmentIndex the chunk segment index to set
     */
    @SlaveNeedReplay
    void set(int segmentIndex) {
        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer.putInt(0, segmentIndex);
            return;
        }

        try {
            raf.seek(0);
            raf.writeInt(segmentIndex);
            inMemoryCachedByteBuffer.putInt(0, segmentIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets the master UUID, data fetch flag, binlog file index, and binlog offset.
     *
     * @param masterUuid                           the master UUID
     * @param isExistsDataAllFetched               the flag indicating if all data has been fetched from the master
     * @param masterBinlogFileIndexNextTimeToFetch the binlog file index to fetch next time
     * @param masterBinlogOffsetNextTimeToFetch    the binlog offset to fetch next time
     */
    public void setMasterBinlogFileIndexAndOffset(long masterUuid, boolean isExistsDataAllFetched,
                                                  int masterBinlogFileIndexNextTimeToFetch, long masterBinlogOffsetNextTimeToFetch) {
        setAll(get(), masterUuid, isExistsDataAllFetched, masterBinlogFileIndexNextTimeToFetch, masterBinlogOffsetNextTimeToFetch);
    }

    /**
     * Clears the master binlog file index and offset, setting them to their default values.
     */
    public void clearMasterBinlogFileIndexAndOffset() {
        setMasterBinlogFileIndexAndOffset(0L, false, 0, 0L);
        log.warn("Repl meta chunk segment index clear master binlog file index and offset done, set 0 from the beginning, slot={}", slot);
    }

    /**
     * Sets all the metadata fields: segment index, master UUID, data fetch flag, binlog file index, and binlog offset.
     * If the configuration is set for pure memory operations, it directly updates the in-memory cache.
     * Otherwise, it writes the metadata to the file and updates the in-memory cache.
     *
     * @param segmentIndex           the chunk segment index
     * @param masterUuid             the master UUID
     * @param isExistsDataAllFetched the flag indicating if all data has been fetched from the master
     * @param masterBinlogFileIndex  the binlog file index
     * @param masterBinlogOffset     the binlog offset
     */
    @VisibleForTesting
    void setAll(int segmentIndex, long masterUuid, boolean isExistsDataAllFetched,
                int masterBinlogFileIndex, long masterBinlogOffset) {
        var updatedBytes = new byte[allCapacity];
        ByteBuffer updatedBuffer = ByteBuffer.wrap(updatedBytes);
        updatedBuffer.putInt(segmentIndex);
        updatedBuffer.putLong(masterUuid);
        updatedBuffer.putInt(isExistsDataAllFetched ? 1 : 0);
        updatedBuffer.putInt(masterBinlogFileIndex);
        updatedBuffer.putLong(masterBinlogOffset);

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer.position(0).put(updatedBytes);
            return;
        }

        try {
            raf.seek(0);
            raf.write(updatedBytes);
            inMemoryCachedByteBuffer.position(0).put(updatedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the chunk segment index.
     *
     * @return the chunk segment index
     */
    int get() {
        return inMemoryCachedByteBuffer.getInt(0);
    }

    /**
     * Retrieves the master UUID.
     *
     * @return the master UUID
     */
    public long getMasterUuid() {
        return inMemoryCachedByteBuffer.getLong(4);
    }

    /**
     * Checks if all data has been fetched from the master.
     *
     * @return the flag indicating if all data has been fetched from the master
     */
    public boolean isExistsDataAllFetched() {
        return inMemoryCachedByteBuffer.getInt(12) == 1;
    }

    /**
     * Retrieves the master binlog file index and offset.
     *
     * @return the {@link Binlog.FileIndexAndOffset} object containing the file index and offset
     */
    public Binlog.FileIndexAndOffset getMasterBinlogFileIndexAndOffset() {
        return new Binlog.FileIndexAndOffset(inMemoryCachedByteBuffer.getInt(16), inMemoryCachedByteBuffer.getLong(20));
    }

    /**
     * Clears all the metadata fields, setting them to their default values.
     * If the configuration is set for pure memory operations, it directly updates the in-memory cache.
     * Otherwise, it writes the default values to the file and updates the in-memory cache.
     */
    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        setAll(0, 0L, false, 0, 0L);
        System.out.println("Meta chunk segment index clear done, set 0 from the beginning. Clear master binlog file index and offset.");
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
            System.out.println("Meta chunk segment index file closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}