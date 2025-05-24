package io.velo.persist;

import com.github.luben.zstd.Zstd;
import com.google.common.io.Files;
import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.LastError;
import jnr.ffi.Runtime;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.velo.ConfForGlobal.isPureMemoryModeKeyBucketsUseCompression;

/**
 * Manages file read and write operations with support for direct I/O and in-memory storage.
 * This class provides methods to read and write data to/from files directly, optionally using LRU cache for performance optimization.
 * It also supports metrics collection to monitor read/write operations and memory usage.
 * Need refactor to FdChunkSegments + FdKeyBuckets. todo
 */
// need thread safe
// need refactor to FdChunkSegments + FdKeyBuckets, todo
public class FdReadWrite implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp {

    /**
     * File permission for read/write operations.
     */
    private static final int PERM = 00644;

    /**
     * Direct I/O flag. Loaded from configuration file if available, otherwise defaults to 0x4000.
     */
    public static final int O_DIRECT;

    static {
        var oDirectIntStr = System.getenv("O_DIRECT");
        if (oDirectIntStr != null) {
            O_DIRECT = Integer.parseInt(oDirectIntStr);
        } else {
            var extendConfigFile = Paths.get("velo_extend.properties").toFile();
            if (extendConfigFile.exists()) {
                final String key = "o_direct";
                try {
                    var lines = Files.readLines(extendConfigFile, Charset.defaultCharset());
                    var line = lines.stream().filter(l -> l.startsWith(key)).findFirst().orElseThrow();
                    var value = line.split("=")[1].trim();
                    O_DIRECT = Integer.parseInt(value);
                    System.out.println("Load O_DIRECT=" + O_DIRECT);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                O_DIRECT = 0x4000;
            }
        }
    }

    /**
     * Logger for logging various actions and errors within this class.
     */
    private static final Logger log = LoggerFactory.getLogger(FdReadWrite.class);

    /**
     * Constructs a new instance of {@code FdReadWrite}.
     *
     * @param slot the slot number associated with this file descriptor
     * @param name the name of the file descriptor
     * @param libC the LibC instance used for direct I/O operations (can be null in pure memory mode)
     * @param file the file to be read from and written to
     * @throws IOException if there is an error during file operations
     */
    public FdReadWrite(short slot, String name, @NullableOnlyTest LibC libC, @NotNull File file) throws IOException {
        this.slot = slot;
        this.name = name;
        if (!ConfForGlobal.pureMemory) {
            if (!file.exists()) {
                FileUtils.touch(file);
            }
            this.writeIndex = file.length();

            this.libC = libC;
            if (ConfForGlobal.isUseDirectIO) {
                this.fd = libC.open(file.getAbsolutePath(), O_DIRECT | OpenFlags.O_RDWR.value() | OpenFlags.O_CREAT.value(), PERM);
                if (fd < 0) {
                    throw new IOException("Open fd error=" + strerror());
                }
                this.raf = null;
            } else {
                this.fd = 0;
                this.raf = new RandomAccessFile(file, "rw");
            }
            log.info("Opened fd={}, name={}, file length={}MB", fd, name, this.writeIndex / 1024 / 1024);
        } else {
            this.libC = null;
            this.fd = 0;
            this.raf = null;
            log.warn("Pure memory mode, not use fd, name={}", name);
        }
    }

    /**
     * Collects and returns metrics related to file operations and memory usage.
     *
     * @return a map of metric names and their corresponding values
     */
    @Override
    public Map<String, Double> collect() {
        final String prefix = "fd_" + (isChunkFd ? "c_" : "k_") + fdIndex + "_";

        var map = new HashMap<String, Double>();
        map.put(prefix + "write_index", (double) writeIndex);

        if (afterPreadCompressCountTotal > 0) {
            map.put(prefix + "after_pread_compress_time_total_us", (double) afterPreadCompressTimeTotalUs);
            map.put(prefix + "after_pread_compress_count_total", (double) afterPreadCompressCountTotal);
            double avgUs = (double) afterPreadCompressTimeTotalUs / afterPreadCompressCountTotal;
            map.put(prefix + "after_pread_compress_time_avg_us", avgUs);

            // compress ratio
            double compressRatio = (double) afterPreadCompressedBytesTotalLength / afterPreadCompressBytesTotalLength;
            map.put(prefix + "after_pread_compress_ratio", compressRatio);
        }

        if (keyBucketSharedBytesCompressCountTotal > 0) {
            map.put(prefix + "key_bucket_shared_bytes_compress_time_total_us", (double) keyBucketSharedBytesCompressTimeTotalUs);
            map.put(prefix + "key_bucket_shared_bytes_compress_count_total", (double) keyBucketSharedBytesCompressCountTotal);
            double avgUs = (double) keyBucketSharedBytesCompressTimeTotalUs / keyBucketSharedBytesCompressCountTotal;
            map.put(prefix + "key_bucket_shared_bytes_compress_time_avg_us", avgUs);

            // compress ratio
            double compressRatio = (double) keyBucketSharedBytesAfterCompressedBytesTotal / keyBucketSharedBytesBeforeCompressedBytesTotal;
            map.put(prefix + "key_bucket_shared_bytes_compress_ratio", compressRatio);
        }

        if (keyBucketSharedBytesDecompressCountTotal > 0) {
            map.put(prefix + "key_bucket_shared_bytes_decompress_time_total_us", (double) keyBucketSharedBytesDecompressTimeTotalUs);
            map.put(prefix + "key_bucket_shared_bytes_decompress_count_total", (double) keyBucketSharedBytesDecompressCountTotal);
            double avgUs = (double) keyBucketSharedBytesDecompressTimeTotalUs / keyBucketSharedBytesDecompressCountTotal;
            map.put(prefix + "key_bucket_shared_bytes_decompress_time_avg_us", avgUs);
        }

        if (readCountTotal > 0) {
            map.put(prefix + "read_bytes_total", (double) readBytesTotal);
            map.put(prefix + "read_time_total_us", (double) readTimeTotalUs);
            map.put(prefix + "read_count_total", (double) readCountTotal);
            map.put(prefix + "read_time_avg_us", (double) readTimeTotalUs / readCountTotal);
        }

        if (writeCountTotal > 0) {
            map.put(prefix + "write_bytes_total", (double) writeBytesTotal);
            map.put(prefix + "write_time_total_us", (double) writeTimeTotalUs);
            map.put(prefix + "write_count_total", (double) writeCountTotal);
            map.put(prefix + "write_time_avg_us", (double) writeTimeTotalUs / writeCountTotal);
        }

        if (lruHitCounter > 0) {
            map.put(prefix + "lru_hit_counter", (double) lruHitCounter);

            map.put(prefix + "after_lru_read_decompress_time_total_us", (double) afterLRUReadDecompressTimeTotalUs);
            map.put(prefix + "after_lru_read_decompress_time_avg_us", (double) afterLRUReadDecompressTimeTotalUs / lruHitCounter);

            var hitMissTotal = lruHitCounter + lruMissCounter;
            map.put(prefix + "lru_hit_ratio", (double) lruHitCounter / hitMissTotal);
        }
        if (lruMissCounter > 0) {
            map.put(prefix + "lru_miss_counter", (double) lruMissCounter);
        }

        return map;
    }

    /**
     * The slot number associated with this file descriptor.
     */
    private final short slot;

    /**
     * The name of the file descriptor.
     */
    @VisibleForTesting
    final String name;

    /**
     * The LibC instance used for direct I/O operations.
     */
    private final LibC libC;

    /**
     * The file descriptor for direct I/O operations.
     */
    private final int fd;

    /**
     * The RandomAccessFile instance used for file operations (used if direct I/O is not enabled).
     */
    private final RandomAccessFile raf;

    /**
     * The current write index in the file.
     */
    @VisibleForTesting
    long writeIndex;

    /**
     * Indicates whether the LRU cache is enabled.
     */
    private boolean isLRUOn = false;

    /**
     * Returns a string representation of this file descriptor.
     *
     * @return a string representation of this file descriptor
     */
    @Override
    public String toString() {
        return "FdReadWrite{" +
                "name='" + name + '\'' +
                ", fd=" + fd +
                ", writeIndex=" + writeIndex +
                ", isLRUOn=" + isLRUOn +
                ", isChunkFd=" + isChunkFd +
                ", oneInnerLength=" + oneInnerLength +
                '}';
    }

    // Metric stats
    /**
     * The total time spent decompressing data after an LRU cache hit (in microseconds).
     */
    private long afterLRUReadDecompressTimeTotalUs;

    /**
     * The total time spent compressing data after a pread operation (in microseconds).
     */
    private long afterPreadCompressTimeTotalUs;

    /**
     * The total count of pread operations that triggered compression.
     */
    @VisibleForTesting
    long afterPreadCompressCountTotal;

    /**
     * The total length of data before compression after a pread operation.
     */
    @VisibleForTesting
    long afterPreadCompressBytesTotalLength;

    /**
     * The total length of data after compression after a pread operation.
     */
    @VisibleForTesting
    long afterPreadCompressedBytesTotalLength;

    /**
     * The total number of bytes read.
     */
    private long readBytesTotal;

    /**
     * The total time spent reading (in microseconds).
     */
    private long readTimeTotalUs;

    /**
     * The total count of read operations.
     */
    @VisibleForTesting
    long readCountTotal;

    /**
     * The total number of bytes written.
     */
    private long writeBytesTotal;

    /**
     * The total time spent writing (in microseconds).
     */
    private long writeTimeTotalUs;

    /**
     * The total count of write operations.
     */
    @VisibleForTesting
    long writeCountTotal;

    /**
     * The count of LRU cache hits.
     */
    @VisibleForTesting
    long lruHitCounter;

    /**
     * The count of LRU cache misses.
     */
    @VisibleForTesting
    long lruMissCounter;

    // Metrics for pure memory mode

    /**
     * The total time spent compressing shared bytes (in microseconds).
     * Only for pure memory mode stats.
     */
    private long keyBucketSharedBytesCompressTimeTotalUs;

    /**
     * The total count of key bucket shared bytes compression operations.
     */
    @VisibleForTesting
    long keyBucketSharedBytesCompressCountTotal;

    /**
     * The total length of key bucket shared bytes before compression.
     */
    @VisibleForTesting
    long keyBucketSharedBytesBeforeCompressedBytesTotal;

    /**
     * The total length of key bucket shared bytes after compression.
     */
    @VisibleForTesting
    long keyBucketSharedBytesAfterCompressedBytesTotal;

    /**
     * The total time spent decompressing shared bytes (in microseconds).
     */
    private long keyBucketSharedBytesDecompressTimeTotalUs;

    /**
     * The total count of key bucket shared bytes decompression operations.
     */
    @VisibleForTesting
    long keyBucketSharedBytesDecompressCountTotal;

    /**
     * The number of segments to batch write at once.
     */
    public static final int BATCH_ONCE_SEGMENT_COUNT_PWRITE = 4;

    /**
     * The number of segments to batch read at once during repl operations.
     */
    public static final int REPL_ONCE_SEGMENT_COUNT_PREAD = 1024;

    /**
     * A buffer for holding a single inner segment.
     */
    private ByteBuffer oneInnerBuffer;

    /**
     * The memory address of the {@link #oneInnerBuffer}.
     */
    private long oneInnerAddress;

    /**
     * A buffer for batching writes to segments.
     * For chunk batch write segments.
     */
    private ByteBuffer writeSegmentBatchBuffer;

    /**
     * The memory address of the {@link #writeSegmentBatchBuffer}.
     */
    private long writeSegmentBatchAddress;

    /**
     * A buffer for reading during repl operations.
     */
    private ByteBuffer forReplBuffer;

    /**
     * The memory address of the {@link #forReplBuffer}.
     */
    private long forReplAddress;

    /**
     * A buffer for reading segments during the merge process.
     * For chunk segments merge.
     */
    private ByteBuffer readForMergeBatchBuffer;

    /**
     * The memory address of the {@link #readForMergeBatchBuffer}.
     */
    private long readForMergeBatchAddress;

    /**
     * A buffer for batch reading/writing key buckets.
     */
    private ByteBuffer forOneWalGroupBatchBuffer;

    /**
     * The memory address of the {@link #forOneWalGroupBatchBuffer}.
     */
    private long forOneWalGroupBatchAddress;

    /**
     * The length of one inner segment.
     */
    private int oneInnerLength;

    /**
     * A flag indicating whether this is a chunk fd (true) or a key bucket fd (false).
     */
    private boolean isChunkFd;

    /**
     * The index of a group fd array.
     */
    private int fdIndex;

    /**
     * The number of segments to batch read during the merge process.
     */
    static final int BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE = 32;

    /**
     * An array of byte arrays holding segments in memory for chunk fd files.
     * The first index is the relative segment index in the target chunk fd.
     * Only when pure memory mode.
     */
    byte[][] allBytesBySegmentIndexForOneChunkFd;

    /**
     * An array of byte arrays holding compressed shared bytes for key bucket files.
     * For key bucket, compressed, need compress before set here and decompress after read from here.
     * Only when pure memory mode.
     */
    byte[][] allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex;

    /**
     * Sets the segment bytes from the last saved file to memory.
     *
     * @param segmentBytes the bytes of the segment to set
     * @param segmentIndex the index of the segment
     */
    void setSegmentBytesFromLastSavedFileToMemory(byte[] segmentBytes, int segmentIndex) {
        allBytesBySegmentIndexForOneChunkFd[segmentIndex] = segmentBytes;
        updateWriteIndex(segmentIndex);
    }

    /**
     * Resets the shared bytes array for key buckets in memory to null.
     *
     * @param walGroupNumber the number of WAL groups
     */
    @TestOnly
    void resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndex(int walGroupNumber) {
        this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[walGroupNumber][];
    }

    /**
     * Sets compressed shared bytes to memory.
     *
     * @param sharedBytes   the shared bytes to compress and store
     * @param walGroupIndex the index of the WAL group
     */
    @VisibleForTesting
    void setSharedBytesCompressToMemory(byte[] sharedBytes, int walGroupIndex) {
        if (sharedBytes == null) {
            allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = null;
            return;
        }

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        var beginBucketIndex = walGroupIndex * oneChargeBucketNumber;

        if (!isPureMemoryModeKeyBucketsUseCompression) {
            allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = sharedBytes;
            updateWriteIndex(beginBucketIndex + oneChargeBucketNumber);
            return;
        }

        // tips: one wal group may charge 32 key buckets, = 32 * 4K = 128K, compress cost may take 500us
        // one wal group charge 16 key buckets will perform better
        var beginT = System.nanoTime();
        var sharedBytesCompressed = Zstd.compress(sharedBytes);
        var costT = (System.nanoTime() - beginT) / 1000;

        allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = sharedBytesCompressed;
        updateWriteIndex(beginBucketIndex + oneChargeBucketNumber);

        // stats
        keyBucketSharedBytesCompressTimeTotalUs += costT;
        keyBucketSharedBytesCompressCountTotal++;
        keyBucketSharedBytesBeforeCompressedBytesTotal += sharedBytes.length;
        keyBucketSharedBytesAfterCompressedBytesTotal += sharedBytesCompressed.length;
    }

    /**
     * Sets shared bytes from the last saved file to memory.
     *
     * @param sharedBytes   the shared bytes to store
     * @param walGroupIndex the index of the WAL group
     */
    void setSharedBytesFromLastSavedFileToMemory(byte[] sharedBytes, int walGroupIndex) {
        allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = sharedBytes;

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        var beginBucketIndex = walGroupIndex * oneChargeBucketNumber;
        updateWriteIndex(beginBucketIndex + oneChargeBucketNumber);
    }

    /**
     * Gets decompressed shared bytes from memory.
     *
     * @param walGroupIndex the index of the WAL group
     * @return the decompressed shared bytes
     */
    @VisibleForTesting
    byte[] getSharedBytesDecompressFromMemory(int walGroupIndex) {
        var compressedSharedBytes = allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex];
        if (compressedSharedBytes == null) {
            return null;
        }

        if (!isPureMemoryModeKeyBucketsUseCompression) {
            return compressedSharedBytes;
        }

        // tips: one wal group may charge 32 key buckets, = 32 * 4K = 128K, decompress cost may take 200-300us
        // one wal group charge 16 key buckets will perform better
        var sharedBytes = new byte[ConfForSlot.global.confWal.oneChargeBucketNumber * oneInnerLength];
        var beginT = System.nanoTime();
        Zstd.decompress(sharedBytes, compressedSharedBytes);
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        keyBucketSharedBytesDecompressTimeTotalUs += costT;
        keyBucketSharedBytesDecompressCountTotal++;
        return sharedBytes;
    }

    /**
     * Checks if the target segment index is null in memory.
     *
     * @param segmentIndex the segment index to check.
     * @return true if the segment index is null, false otherwise.
     */
    boolean isTargetSegmentIndexNullInMemory(int segmentIndex) {
        return allBytesBySegmentIndexForOneChunkFd[segmentIndex] == null;
    }

    /**
     * Clears the target segment index in memory.
     *
     * @param segmentIndex the segment index to clear.
     */
    void clearTargetSegmentIndexInMemory(int segmentIndex) {
        allBytesBySegmentIndexForOneChunkFd[segmentIndex] = null;
    }

    /**
     * LRUMap for caching inner bytes by index.
     * Chunk fd is by relative segment index, key bucket fd is by bucket index.
     */
    @VisibleForTesting
    LRUMap<Integer, byte[]> oneInnerBytesByIndexLRU;

    /**
     * Initializes the pure memory byte array.
     */
    @VisibleForTesting
    public void initPureMemoryByteArray() {
        if (isChunkFd) {
            if (this.allBytesBySegmentIndexForOneChunkFd == null) {
                var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
                this.allBytesBySegmentIndexForOneChunkFd = new byte[segmentNumberPerFd][];
            }
        } else {
            if (this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex == null) {
                var walGroupNumber = Wal.calcWalGroupNumber();
                this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[walGroupNumber][];
            }
        }
    }

    /**
     * Initializes the byte buffers for read and write operations.
     *
     * @param isChunkFd true if this is a chunk fd, false if it is a key bucket fd.
     * @param fdIndex   the index of array.
     */
    public void initByteBuffers(boolean isChunkFd, int fdIndex) {
        var oneInnerLength = isChunkFd ? ConfForSlot.global.confChunk.segmentLength : KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
        this.oneInnerLength = oneInnerLength;
        this.isChunkFd = isChunkFd;
        this.fdIndex = fdIndex;

        initLRU(isChunkFd, oneInnerLength);

        if (ConfForGlobal.pureMemory) {
            initPureMemoryByteArray();
        } else {
            long initMemoryN = 0;

            var pageManager = PageManager.getInstance();
            var m = MemoryIO.getInstance();

            var npagesOneInner = oneInnerLength / LocalPersist.PAGE_SIZE;

            this.oneInnerAddress = pageManager.allocatePages(npagesOneInner, LocalPersist.PROTECTION);
            this.oneInnerBuffer = m.newDirectByteBuffer(oneInnerAddress, oneInnerLength);

            initMemoryN += oneInnerBuffer.capacity();

            if (isChunkFd) {
                // chunk write segment batch
                var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                this.writeSegmentBatchAddress = pageManager.allocatePages(npagesWriteSegmentBatch, LocalPersist.PROTECTION);
                this.writeSegmentBatchBuffer = m.newDirectByteBuffer(writeSegmentBatchAddress, npagesWriteSegmentBatch * LocalPersist.PAGE_SIZE);

                // chunk repl
                var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_PREAD;
                this.forReplAddress = pageManager.allocatePages(npagesRepl, LocalPersist.PROTECTION);
                this.forReplBuffer = m.newDirectByteBuffer(forReplAddress, npagesRepl * LocalPersist.PAGE_SIZE);

                // only merge worker need read for merging batch
                int npagesMerge = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
                this.readForMergeBatchAddress = pageManager.allocatePages(npagesMerge, LocalPersist.PROTECTION);
                this.readForMergeBatchBuffer = m.newDirectByteBuffer(readForMergeBatchAddress, npagesMerge * LocalPersist.PAGE_SIZE);

                initMemoryN += writeSegmentBatchBuffer.capacity() + forReplBuffer.capacity() + readForMergeBatchBuffer.capacity();
            } else {
                int npagesOneWalGroup = ConfForSlot.global.confWal.oneChargeBucketNumber;
                this.forOneWalGroupBatchAddress = pageManager.allocatePages(npagesOneWalGroup, LocalPersist.PROTECTION);
                this.forOneWalGroupBatchBuffer = m.newDirectByteBuffer(forOneWalGroupBatchAddress, npagesOneWalGroup * LocalPersist.PAGE_SIZE);

                initMemoryN += forOneWalGroupBatchBuffer.capacity();
            }

            int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
            log.info("Static memory init, type={}, MB={}, name={}", StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer, initMemoryMB, name);
            StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer, initMemoryMB, false);
        }
    }

    /**
     * Estimates the memory usage of this instance.
     *
     * @param sb the StringBuilder to append the estimate details.
     * @return the estimated memory usage in bytes.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        if (ConfForGlobal.pureMemory) {
            var size1 = 0L;
            if (isChunkFd) {
                size1 += allBytesBySegmentIndexForOneChunkFd.length * 16L;
                for (var bytes : allBytesBySegmentIndexForOneChunkFd) {
                    if (bytes != null) {
                        size1 += bytes.length;
                    }
                }
                sb.append("Pure memory, Fd chunk segments, name: ").append(name).append(", size: ").append(size1).append("\n");
            } else {
                size1 += allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex.length * 16L;
                for (var bytes : allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex) {
                    if (bytes != null) {
                        size1 += bytes.length;
                    }
                }
                sb.append("Pure memory, Fd key buckets, name: ").append(name).append(", size: ").append(size1).append("\n");
            }
            size += size1;
        } else {
            var size1 = 0L;
            size1 += oneInnerBuffer.capacity();
            if (isChunkFd) {
                size1 += writeSegmentBatchBuffer.capacity();
                size1 += forReplBuffer.capacity();
                size1 += readForMergeBatchBuffer.capacity();
            } else {
                size1 += forOneWalGroupBatchBuffer.capacity();
            }
            sb.append("Fd read write buffer, name: ").append(name).append(", size: ").append(size1).append("\n");
            size += size1;

            if (isLRUOn) {
                var size2 = RamUsageEstimator.sizeOfMap(oneInnerBytesByIndexLRU);
                sb.append("LRU cache: ").append(size2).append("\n");
                size += size2;
            }
        }
        return size;
    }

    /**
     * Initializes the LRU cache.
     *
     * @param isChunkFd      true if this is a chunk fd, false if it is a key bucket fd.
     * @param oneInnerLength the length of one inner segment.
     */
    private void initLRU(boolean isChunkFd, int oneInnerLength) {
        if (ConfForGlobal.pureMemory) {
            log.warn("Pure memory mode, not use lru cache, name={}", name);
            return;
        }

        int maxSize;
        if (isChunkFd) {
            // tips: per fd, if one chunk has too many fd files, may OOM
            maxSize = ConfForSlot.global.confChunk.lruPerFd.maxSize;
            var lruMemoryRequireMB = ((long) maxSize * oneInnerLength) / 1024 / 1024;
            log.info("Chunk lru max size for one chunk fd={}, one inner length={}, memory require={}MB, name={}",
                    maxSize, oneInnerLength, lruMemoryRequireMB, name);
            log.info("LRU prepare, type={}, MB={}, fd={}", LRUPrepareBytesStats.Type.fd_chunk_data, lruMemoryRequireMB, name);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_chunk_data, name, (int) lruMemoryRequireMB, true);
        } else {
            // key bucket
            // tips: per fd, if one key loader has too many fd files as split number is big, may OOM
            maxSize = ConfForSlot.global.confBucket.lruPerFd.maxSize;
            // need to compare with metrics
            final var compressRatio = 0.25;
            var lruMemoryRequireMB = (double) ((long) maxSize * oneInnerLength) / 1024 / 1024 * compressRatio;
            log.info("Key bucket lru max size for one key bucket fd={}, one inner length={}， compress ratio maybe={}, memory require={}MB, name={}",
                    maxSize, oneInnerLength, compressRatio, lruMemoryRequireMB, name);
            log.info("LRU prepare, type={}, MB={}, fd={}", LRUPrepareBytesStats.Type.fd_key_bucket, lruMemoryRequireMB, name);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_key_bucket, name, (int) lruMemoryRequireMB, false);

        }
        if (maxSize > 0) {
            this.oneInnerBytesByIndexLRU = new LRUMap<>(maxSize);
            this.isLRUOn = true;
        }
    }

    /**
     * Cleans up resources and releases memory.
     */
    @Override
    public void cleanUp() {
        var npagesOneInner = oneInnerLength / LocalPersist.PAGE_SIZE;

        var pageManager = PageManager.getInstance();
        if (oneInnerAddress != 0) {
            pageManager.freePages(oneInnerAddress, npagesOneInner);
            System.out.println("Clean up fd read, name=" + name + ", one inner address=" + oneInnerAddress);

            oneInnerAddress = 0;
            oneInnerBuffer = null;
        }

        if (writeSegmentBatchAddress != 0) {
            var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE;
            pageManager.freePages(writeSegmentBatchAddress, npagesWriteSegmentBatch);
            System.out.println("Clean up fd write, name=" + name + ", write segment batch address=" + writeSegmentBatchAddress);

            writeSegmentBatchAddress = 0;
            writeSegmentBatchBuffer = null;
        }

        if (forReplAddress != 0) {
            var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_PREAD;
            pageManager.freePages(forReplAddress, npagesRepl);
            System.out.println("Clean up fd read, name=" + name + ", for repl address=" + forReplAddress);

            forReplAddress = 0;
            forReplBuffer = null;
        }

        if (readForMergeBatchAddress != 0) {
            var npagesMerge = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
            pageManager.freePages(readForMergeBatchAddress, npagesMerge);
            System.out.println("Clean up fd read, name=" + name + ", read for merge batch address=" + readForMergeBatchAddress);

            readForMergeBatchAddress = 0;
            readForMergeBatchBuffer = null;
        }

        if (forOneWalGroupBatchAddress != 0) {
            var npagesOneWalGroup = ConfForSlot.global.confWal.oneChargeBucketNumber;
            pageManager.freePages(forOneWalGroupBatchAddress, npagesOneWalGroup);
            System.out.println("Clean up fd read, name=" + name + ", for one wal group address=" + forOneWalGroupBatchAddress);

            forOneWalGroupBatchAddress = 0;
            forOneWalGroupBatchBuffer = null;
        }

        if (fd != 0) {
            int r = libC.close(fd);
            if (r < 0) {
                System.err.println("Close fd error=" + strerror() + ", name=" + name);
            }
            System.out.println("Closed fd=" + fd + ", name=" + name);
        }

        if (raf != null) {
            try {
                raf.close();
                System.out.println("Closed raf, name=" + name);
            } catch (IOException e) {
                System.err.println("Close raf error=" + e.getMessage() + ", name=" + name);
            }
        }
    }

    /**
     * Interface for preparing write buffers.
     */
    private interface WriteBufferPrepare {
        void prepare(ByteBuffer buffer);
    }

    /**
     * Checks the one inner index for validity.
     *
     * @param oneInnerIndex the index to check.
     */
    private void checkOneInnerIndex(int oneInnerIndex) {
        if (isChunkFd) {
            // one inner index -> chunk segment index
            if (oneInnerIndex < 0 || oneInnerIndex >= ConfForSlot.global.confChunk.segmentNumberPerFd) {
                throw new IllegalArgumentException("Segment index=" + oneInnerIndex + " must be in [0, " + ConfForSlot.global.confChunk.segmentNumberPerFd + ")");
            }
        } else {
            // one inner index -> bucket index
            if (oneInnerIndex < 0 || oneInnerIndex >= ConfForSlot.global.confBucket.bucketsPerSlot) {
                throw new IllegalArgumentException("Bucket index=" + oneInnerIndex + " must be in [0, " + ConfForSlot.global.confBucket.bucketsPerSlot + ")");
            }
        }
    }

    /**
     * Reads inner bytes from a buffer.
     *
     * @param oneInnerIndex     the index of the inner segment.
     * @param buffer            the buffer to read from.
     * @param isRefreshLRUCache true if the LRU cache should be refreshed, false otherwise.
     * @return the read bytes.
     */
    private byte[] readInnerByBuffer(int oneInnerIndex, @NotNull ByteBuffer buffer, boolean isRefreshLRUCache) {
        return readInnerByBuffer(oneInnerIndex, buffer, isRefreshLRUCache, buffer.capacity());
    }

    /**
     * Warms up the key bucket fd by reading and caching data.
     *
     * @return the number of buckets warmed up.
     */
    @TestOnly
    public int warmUp() {
        // only for key bucket fd
        if (isChunkFd) {
            throw new IllegalArgumentException("Chunk fd not support warm up");
        }

        if (ConfForGlobal.pureMemory) {
            return 0;
        }

        if (oneInnerBytesByIndexLRU == null) {
            oneInnerBytesByIndexLRU = new LRUMap<>(ConfForSlot.global.confBucket.bucketsPerSlot);
        }

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        var walGroupNumber = Wal.calcWalGroupNumber();

        long readN = 0;
        long compressedN = 0;
        int n = 0;
        for (int i = 0; i < walGroupNumber; i++) {
            var beginBucketIndex = i * oneChargeBucketNumber;
            var sharedBytes = readKeyBucketsSharedBytesInOneWalGroup(beginBucketIndex);

            if (sharedBytes == null) {
                n += oneChargeBucketNumber;
                continue;
            }

            for (int j = 0; j < oneChargeBucketNumber; j++) {
                var bucketIndex = beginBucketIndex + j;
                var start = oneInnerLength * j;
                var end = start + oneInnerLength;

                if (start >= sharedBytes.length) {
                    n++;
                    continue;
                }

                var bytes = Arrays.copyOfRange(sharedBytes, start, end);
                var compressedBytes = Zstd.compress(bytes);
                readN += oneInnerLength;
                compressedN += compressedBytes.length;

                oneInnerBytesByIndexLRU.put(bucketIndex, compressedBytes);
                n++;

                if (n % 10240 == 0) {
                    log.info("Warm up key bucket fd, name={}, bucket index={}, n={}", name, bucketIndex, n);
                }
            }
        }
        log.warn("Warm up key bucket fd, name={}, read bytes={}, compressed bytes={}, compressed ratio={}", name,
                readN, compressedN, (double) compressedN / readN);

        // n should be ConfForSlot.global.confBucket.bucketsPerSlot
        return n;
    }

    /**
     * Reads inner bytes from a buffer with a specified length.
     *
     * @param oneInnerIndex     the index of the inner segment.
     * @param buffer            the buffer to read from.
     * @param isRefreshLRUCache true if the LRU cache should be refreshed, false otherwise.
     * @param length            the length of bytes to read.
     * @return the read bytes.
     */
    private byte[] readInnerByBuffer(int oneInnerIndex, @NotNull ByteBuffer buffer, boolean isRefreshLRUCache, int length) {
        checkOneInnerIndex(oneInnerIndex);
        if (length > buffer.capacity()) {
            throw new IllegalArgumentException("Read length must be less than buffer capacity=" + buffer.capacity() + ", read length=" + length + ", name=" + name);
        }

        // for from lru cache if only read one segment
        var isOnlyOneOneInner = length == oneInnerLength;
        int oneInnerCount;
        if (isOnlyOneOneInner) {
            oneInnerCount = 1;

            if (isLRUOn) {
                var bytesCached = oneInnerBytesByIndexLRU.get(oneInnerIndex);
                if (bytesCached != null) {
                    lruHitCounter++;

                    if (isChunkFd) {
                        return bytesCached;
                    } else {
                        // only key bucket data need decompress
                        var beginT = System.nanoTime();
                        var bytesDecompressedCached = Zstd.decompress(bytesCached, oneInnerLength);
                        var costT = (System.nanoTime() - beginT) / 1000;

                        // stats
                        afterLRUReadDecompressTimeTotalUs += costT;
                        return bytesDecompressedCached;
                    }
                } else {
                    lruMissCounter++;
                }
            }
        } else {
            oneInnerCount = length / oneInnerLength;
        }

        var offset = oneInnerIndex * oneInnerLength;
        if (offset >= writeIndex) {
            log.warn("Read one inner bytes from fd, name={}, one inner index={}, offset={}, write index={}, read length={}",
                    name, oneInnerIndex, offset, writeIndex, length);
            return null;
        }

        int readLength = length;
        var lastSegmentOffset = offset + (oneInnerCount * oneInnerLength);
        if (writeIndex <= lastSegmentOffset) {
            readLength = (int) (writeIndex - offset);
        }
        if (readLength < 0) {
            throw new IllegalArgumentException("Read length must be greater than 0, read length=" + readLength);
        }
        if (readLength > length) {
            throw new IllegalArgumentException("Read length must be less than given length=" + length + ", read length=" + readLength);
        }

        // clear buffer before read
        buffer.clear();

        var beginT = System.nanoTime();
        int n;
        if (ConfForGlobal.isUseDirectIO) {
            n = libC.pread(fd, buffer, readLength, offset);
        } else {
            try {
                raf.seek(offset);
                n = raf.getChannel().read(buffer);
            } catch (IOException e) {
                throw new RuntimeException("Read error, name=" + name, e);
            }
        }
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        readTimeTotalUs += costT;
        readCountTotal++;
        readBytesTotal += n;

        if (n != readLength) {
            if (ConfForGlobal.isUseDirectIO) {
                var strerror = strerror();
                log.error("Read error, n={}, read length={}, name={}, error={}", n, readLength, name, strerror);
                throw new RuntimeException("Read error, n=" + n + ", read length=" + readLength + ", name=" + name + ", error=" + strerror);
            } else {
                if (n < readLength) {
                    log.error("Read error, n={}, read length={}, name={}", n, readLength, name);
                    throw new RuntimeException("Read error, n=" + n + ", read length=" + readLength + ", name=" + name);
                }
            }
        }

        buffer.rewind();
        var bytesRead = new byte[readLength];
        buffer.get(bytesRead);

        if (isOnlyOneOneInner && isRefreshLRUCache) {
            if (isLRUOn) {
                if (isChunkFd) {
                    oneInnerBytesByIndexLRU.put(oneInnerIndex, bytesRead);
                } else {
                    // only key bucket data need compress
                    var beginT2 = System.nanoTime();
                    var bytesCompressed = Zstd.compress(bytesRead);
                    var costT2 = (System.nanoTime() - beginT2) / 1000;

                    // stats
                    afterPreadCompressTimeTotalUs += costT2;
                    afterPreadCompressCountTotal++;

                    afterPreadCompressBytesTotalLength += bytesRead.length;
                    afterPreadCompressedBytesTotalLength += bytesCompressed.length;

                    oneInnerBytesByIndexLRU.put(oneInnerIndex, bytesCompressed);
                }
            }
        }
        return bytesRead;
    }

    /**
     * Writes inner bytes to a buffer.
     *
     * @param oneInnerIndex     the index of the inner segment.
     * @param buffer            the buffer to write to.
     * @param prepare           the preparation logic for the buffer.
     * @param isRefreshLRUCache true if the LRU cache should be refreshed, false otherwise.
     * @return the number of bytes written.
     */
    private int writeInnerByBuffer(int oneInnerIndex, @NotNull ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        checkOneInnerIndex(oneInnerIndex);

        int capacity = buffer.capacity();
        var oneInnerCount = capacity / oneInnerLength;
        var isOnlyOneSegment = oneInnerCount == 1;
        var isPwriteBatch = oneInnerCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE;

        int offset = oneInnerIndex * oneInnerLength;

        // clear buffer before write
        buffer.clear();

        prepare.prepare(buffer);
        // when write bytes length < capacity, pending 0
        if (buffer.position() < capacity) {
            // pending 0
            var bytes0 = new byte[capacity - buffer.position()];
            buffer.put(bytes0);
        }
        buffer.rewind();

        var beginT = System.nanoTime();
        int n;
        if (ConfForGlobal.isUseDirectIO) {
            n = libC.pwrite(fd, buffer, capacity, offset);
        } else {
            try {
                raf.seek(offset);
                n = raf.getChannel().write(buffer);
            } catch (IOException e) {
                throw new RuntimeException("Write error, name=" + name, e);
            }
        }
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        writeTimeTotalUs += costT;
        writeCountTotal++;
        writeBytesTotal += n;

        if (n != capacity) {
            if (ConfForGlobal.isUseDirectIO) {
                var strerror = strerror();
                log.error("Write error, n={}, buffer capacity={}, name={}, error={}", n, capacity, name, strerror);
                throw new RuntimeException("Write error, n=" + n + ", buffer capacity=" + capacity + ", name=" + name + ", error=" + strerror);
            } else {
                log.error("Write error, n={}, buffer capacity={}, name={}", n, capacity, name);
                throw new RuntimeException("Write error, n=" + n + ", buffer capacity=" + capacity + ", name=" + name);
            }
        }

        if (offset + capacity > writeIndex) {
            writeIndex = offset + capacity;
        }

        // set to lru cache
        if (isLRUOn) {
            if (isRefreshLRUCache && isChunkFd && (isOnlyOneSegment || isPwriteBatch)) {
                for (int i = 0; i < oneInnerCount; i++) {
                    var bytes = new byte[oneInnerLength];
                    buffer.position(i * oneInnerLength);
                    buffer.get(bytes);

                    // chunk data is already compressed
                    oneInnerBytesByIndexLRU.put(oneInnerIndex + i, bytes);
                }
            } else {
                for (int i = 0; i < oneInnerCount; i++) {
                    oneInnerBytesByIndexLRU.remove(oneInnerIndex + i);
                }
            }
        }

        return n;
    }

    /**
     * Reads a batch of inner segments from memory.
     *
     * @param oneInnerIndex the starting index of the inner segment.
     * @param oneInnerCount the number of inner segments to read.
     * @return the read bytes.
     */
    @VisibleForTesting
    byte[] readOneInnerBatchFromMemory(int oneInnerIndex, int oneInnerCount) {
        if (!isChunkFd) {
            // read shared bytes for one wal group
            // oneInnerIndex -> beginBucketIndex
            var walGroupIndex = Wal.calcWalGroupIndex(oneInnerIndex);
            if (oneInnerCount == 1 || oneInnerCount == ConfForSlot.global.confWal.oneChargeBucketNumber) {
                // readonly, use a copy will be safe, but not necessary
                return getSharedBytesDecompressFromMemory(walGroupIndex);
            } else {
                throw new IllegalArgumentException("Read error, key loader fd once read key buckets count invalid=" + oneInnerCount);
            }
        }

        // bellow for chunk fd
        if (oneInnerCount == 1) {
            return allBytesBySegmentIndexForOneChunkFd[oneInnerIndex];
        }

        var bytesRead = new byte[oneInnerLength * oneInnerCount];
        for (int i = 0; i < oneInnerCount; i++) {
            var oneInnerBytes = allBytesBySegmentIndexForOneChunkFd[oneInnerIndex + i];
            if (oneInnerBytes != null) {
                System.arraycopy(oneInnerBytes, 0, bytesRead, i * oneInnerLength, oneInnerBytes.length);
            }
        }
        return bytesRead;
    }

    /**
     * Reads an inner segment.
     *
     * @param oneInnerIndex     the index of the inner segment.
     * @param isRefreshLRUCache true if the LRU cache should be refreshed, false otherwise.
     * @return the read bytes.
     */
    public byte[] readOneInner(int oneInnerIndex, boolean isRefreshLRUCache) {
        if (ConfForGlobal.pureMemory) {
            return readOneInnerBatchFromMemory(oneInnerIndex, 1);
        }

        return readInnerByBuffer(oneInnerIndex, oneInnerBuffer, isRefreshLRUCache);
    }

    /**
     * Reads segments for merge.
     *
     * @param beginSegmentIndex the starting index of the segment.
     * @param segmentCount      the number of segments to read.
     * @return the read bytes.
     */
    public byte[] readSegmentsForMerge(int beginSegmentIndex, int segmentCount) {
        if (ConfForGlobal.pureMemory) {
            return readOneInnerBatchFromMemory(beginSegmentIndex, segmentCount);
        }

        return readInnerByBuffer(beginSegmentIndex, readForMergeBatchBuffer, false, segmentCount * oneInnerLength);
    }

    /**
     * Reads a batch of segments for replication.
     *
     * @param oneInnerIndex the starting index of the inner segment.
     * @return the read bytes.
     */
    public byte[] readBatchForRepl(int oneInnerIndex) {
        if (ConfForGlobal.pureMemory) {
            return readOneInnerBatchFromMemory(oneInnerIndex, REPL_ONCE_SEGMENT_COUNT_PREAD);
        }

        return readInnerByBuffer(oneInnerIndex, forReplBuffer, false);
    }

    /**
     * Reads shared bytes for key buckets in one WAL group.
     *
     * @param beginBucketIndex the starting bucket index.
     * @return the read bytes.
     */
    public byte[] readKeyBucketsSharedBytesInOneWalGroup(int beginBucketIndex) {
        if (ConfForGlobal.pureMemory) {
            return readOneInnerBatchFromMemory(beginBucketIndex, ConfForSlot.global.confWal.oneChargeBucketNumber);
        }

        return readInnerByBuffer(beginBucketIndex, forOneWalGroupBatchBuffer, false);
    }

    /**
     * Clears one key bucket in memory.
     *
     * @param bucketIndex the index of the key bucket.
     */
    @TestOnly
    public void clearOneKeyBucketToMemory(int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var sharedBytes = getSharedBytesDecompressFromMemory(walGroupIndex);
        if (sharedBytes == null) {
            return;
        }

        var position = KeyLoader.getPositionInSharedBytes(bucketIndex);
        // set 0
        Arrays.fill(sharedBytes, position, position + oneInnerLength, (byte) 0);
        // compress and set back
        setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
    }

    /**
     * Clears key buckets in memory.
     *
     * @param bucketIndex the index of the key bucket.
     */
    @TestOnly
    public void clearKeyBucketsToMemory(int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        clearAllKeyBucketsInOneWalGroupToMemory(walGroupIndex);
    }

    /**
     * Clears all key buckets in one WAL group in memory.
     *
     * @param walGroupIndex the index of the WAL group.
     */
    @TestOnly
    public void clearAllKeyBucketsInOneWalGroupToMemory(int walGroupIndex) {
        setSharedBytesCompressToMemory(null, walGroupIndex);
    }

    /**
     * Updates the write index.
     *
     * @param updatedOneIndex the updated index.
     */
    private void updateWriteIndex(int updatedOneIndex) {
        writeIndex = Math.max(writeIndex, (long) updatedOneIndex * oneInnerLength);
    }

    /**
     * Writes a batch of inner segments to memory.
     *
     * @param beginOneInnerIndex the starting index of the inner segment.
     * @param bytes              the bytes to write.
     * @param position           the position in the bytes array.
     * @return the number of bytes written.
     */
    @VisibleForTesting
    int writeOneInnerBatchToMemory(int beginOneInnerIndex, byte[] bytes, int position) {
        var isSmallerThanOneInner = bytes.length < oneInnerLength;
        if (!isSmallerThanOneInner && (bytes.length - position) % oneInnerLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of one inner length");
        }

        if (isSmallerThanOneInner) {
            // always is chunk fd
            if (!isChunkFd) {
                throw new IllegalArgumentException("Write bytes smaller than one segment length to memory must be chunk fd");
            }
            // position is 0
            allBytesBySegmentIndexForOneChunkFd[beginOneInnerIndex] = bytes;
            updateWriteIndex(beginOneInnerIndex);
            return bytes.length;
        }

        var oneInnerCount = (bytes.length - position) / oneInnerLength;
        // for key bucket, memory copy one wal group by one wal group
        if (!isChunkFd) {
            if (oneInnerCount != 1) {
                // oneInnerCount == oenChargeBucketNumber, is in another method
                throw new IllegalArgumentException("Write key bucket once by one wal group or only one key bucket");
            }
        }

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        // memory copy one segment/bucket by one segment/bucket
        var offset = position;
        for (int i = 0; i < oneInnerCount; i++) {
            var targetOneInnerIndex = beginOneInnerIndex + i;
            if (!isChunkFd) {
                var walGroupIndex = Wal.calcWalGroupIndex(targetOneInnerIndex);
                var oneWalGroupSharedBytesLength = oneChargeBucketNumber * oneInnerLength;
                var sharedBytes = getSharedBytesDecompressFromMemory(walGroupIndex);
                if (sharedBytes == null) {
                    sharedBytes = new byte[oneWalGroupSharedBytesLength];
                }
                var targetBucketIndexPosition = KeyLoader.getPositionInSharedBytes(targetOneInnerIndex);
                System.arraycopy(bytes, offset, sharedBytes, targetBucketIndexPosition, oneInnerLength);
                setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
            } else {
                var bytesOneSegment = new byte[oneInnerLength];
                System.arraycopy(bytes, offset, bytesOneSegment, 0, oneInnerLength);
                allBytesBySegmentIndexForOneChunkFd[targetOneInnerIndex] = bytesOneSegment;
            }
            offset += oneInnerLength;
        }

        updateWriteIndex(beginOneInnerIndex + oneInnerCount);
        return oneInnerCount * oneInnerLength;
    }

    /**
     * Writes an inner segment.
     *
     * @param oneInnerIndex     the index of the inner segment.
     * @param bytes             the bytes to write.
     * @param isRefreshLRUCache true if the LRU cache should be refreshed, false otherwise.
     * @return the number of bytes written.
     */
    @SlaveNeedReplay
    public int writeOneInner(int oneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > oneInnerLength) {
            throw new IllegalArgumentException("Write bytes length must be less than one inner length");
        }

        if (ConfForGlobal.pureMemory) {
            return writeOneInnerBatchToMemory(oneInnerIndex, bytes, 0);
        }

        return writeInnerByBuffer(oneInnerIndex, oneInnerBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    /**
     * Writes a batch of segments.
     *
     * @param beginOneInnerIndex the starting index of the inner segment.
     * @param bytes              the bytes to write.
     * @param isRefreshLRUCache  true if the LRU cache should be refreshed, false otherwise.
     * @return the number of bytes written.
     */
    @SlaveNeedReplay
    public int writeSegmentsBatch(int beginOneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / oneInnerLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForGlobal.pureMemory) {
            return writeOneInnerBatchToMemory(beginOneInnerIndex, bytes, 0);
        }

        return writeInnerByBuffer(beginOneInnerIndex, writeSegmentBatchBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    /**
     * Writes a batch of segments for replication.
     *
     * @param beginSegmentIndex the starting index of the segment.
     * @param bytes             the bytes to write.
     * @return the number of bytes written.
     */
    @SlaveReplay
    public int writeSegmentsBatchForRepl(int beginSegmentIndex, byte[] bytes) {
        // pure memory will not reach here, refer Chunk.writeSegmentsFromMasterExists
        return writeInnerByBuffer(beginSegmentIndex, forReplBuffer, (buffer) -> {
            buffer.put(bytes);
        }, false);
    }

    /**
     * Writes shared bytes for key buckets in one WAL group.
     *
     * @param bucketIndex the index of the key bucket.
     * @param sharedBytes the shared bytes to write.
     * @return the number of bytes written.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public int writeSharedBytesForKeyBucketsInOneWalGroup(int bucketIndex, byte[] sharedBytes) {
        var keyBucketCount = sharedBytes.length / oneInnerLength;
        if (keyBucketCount != ConfForSlot.global.confWal.oneChargeBucketNumber) {
            throw new IllegalArgumentException("Batch write bytes length not match one charge bucket number in one wal group");
        }

        if (ConfForGlobal.pureMemory) {
            var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
            setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
            return sharedBytes.length;
        }

        return writeInnerByBuffer(bucketIndex, forOneWalGroupBatchBuffer, (buffer) -> buffer.put(sharedBytes), false);
    }

    /**
     * Truncates the file to zero length.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public void truncate() {
        if (ConfForGlobal.pureMemory) {
            log.warn("Pure memory mode, not use fd, name={}", name);
            if (isChunkFd) {
                this.allBytesBySegmentIndexForOneChunkFd = new byte[allBytesBySegmentIndexForOneChunkFd.length][];
            } else {
                this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex.length][];
            }
            log.info("Clear all bytes in memory, name={}", name);
            writeIndex = 0;
            return;
        }

        if (ConfForGlobal.isUseDirectIO) {
            var r = libC.ftruncate(fd, 0);
            if (r < 0) {
                throw new RuntimeException("Truncate error=" + strerror());
            }
            log.info("Truncate fd={}, name={}", fd, name);
        } else {
            if (raf != null) {
                try {
                    raf.setLength(0);
                    log.info("Truncate raf, name={}", name);
                } catch (IOException e) {
                    throw new RuntimeException("Truncate error, name=" + name, e);
                }
            }
        }
        writeIndex = 0;

        if (isLRUOn) {
            oneInnerBytesByIndexLRU.clear();
            log.info("LRU cache clear, name={}", name);
        }
    }

    /**
     * Truncates the file after a target segment index.
     *
     * @param targetSegmentIndex the target segment index.
     */
    public void truncateAfterTargetSegmentIndex(int targetSegmentIndex) {
        var length = (long) targetSegmentIndex * oneInnerLength;

        if (ConfForGlobal.pureMemory) {
            for (int i = targetSegmentIndex; i < allBytesBySegmentIndexForOneChunkFd.length; i++) {
                allBytesBySegmentIndexForOneChunkFd[i] = null;
            }
            writeIndex = length;
            return;
        }

        if (ConfForGlobal.isUseDirectIO) {
            var r = libC.ftruncate(fd, length);
            if (r < 0) {
                throw new RuntimeException("Truncate length error=" + strerror());
            }
            log.info("Truncate length fd={}, name={}", fd, name);
        } else {
            if (raf != null) {
                try {
                    raf.setLength(length);
                    log.info("Truncate length raf, name={}", name);
                } catch (IOException e) {
                    throw new RuntimeException("Truncate length error, name=" + name, e);
                }
            }
        }
        writeIndex = length;
    }

    /**
     * Retrieves the error string from the last system error.
     *
     * @return the error string.
     */
    private String strerror() {
        var systemRuntime = Runtime.getSystemRuntime();
        var errno = LastError.getLastError(systemRuntime);
        return libC.strerror(errno);
    }
}
