package io.velo.persist;

import com.github.luben.zstd.Zstd;
import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.StaticMemoryPrepareBytesStats;
import io.velo.metric.InSlotMetricCollector;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
     * Logger for logging various actions and errors within this class.
     */
    private static final Logger log = LoggerFactory.getLogger(FdReadWrite.class);

    /**
     * Constructs a new instance of {@code FdReadWrite}.
     *
     * @param name the name of the file descriptor
     * @param file the file to be read from and written to
     * @throws IOException if there is an error during file operations
     */
    public FdReadWrite(String name, @NotNull File file) throws IOException {
        this.name = name;
        if (!file.exists()) {
            FileUtils.touch(file);
        }
        this.writeIndex = file.length();

        this.raf = new RandomAccessFile(file, "rw");
        log.info("Opened name={}, file length={}MB", name, this.writeIndex / 1024 / 1024);
    }

    /**
     * Collects and returns metrics related to file operations and memory usage.
     *
     * @return the map of metric names and their corresponding values
     */
    @Override
    public Map<String, Double> collect() {
        final String prefix = "fd_" + (isChunkFd ? "c_" : "k_") + fdIndex + "_";

        var map = new HashMap<String, Double>();
        map.put(prefix + "write_index", (double) writeIndex);

        if (afterReadCompressCountTotal > 0) {
            map.put(prefix + "after_read_compress_time_total_us", (double) afterReadCompressTimeTotalUs);
            map.put(prefix + "after_read_compress_count_total", (double) afterReadCompressCountTotal);
            double avgUs = (double) afterReadCompressTimeTotalUs / afterReadCompressCountTotal;
            map.put(prefix + "after_read_compress_time_avg_us", avgUs);

            // compress ratio
            double compressRatio = (double) afterReadCompressedBytesTotalLength / afterReadCompressBytesTotalLength;
            map.put(prefix + "after_read_compress_ratio", compressRatio);
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
     * The name of the file descriptor.
     */
    @VisibleForTesting
    final String name;

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
     * @return the string representation of this file descriptor
     */
    @Override
    public String toString() {
        return "FdReadWrite{" +
                "name='" + name + '\'' +
                ", writeIndex=" + writeIndex +
                ", isLRUOn=" + isLRUOn +
                ", isChunkFd=" + isChunkFd +
                ", oneInnerLength=" + oneInnerLength +
                '}';
    }

    // Metric stats begin
    /**
     * The total time spent decompressing data after an LRU cache hit (in microseconds).
     */
    private long afterLRUReadDecompressTimeTotalUs;

    /**
     * The total time spent compressing data after a read operation (in microseconds).
     */
    private long afterReadCompressTimeTotalUs;

    /**
     * The total count of read operations that triggered compression.
     */
    @VisibleForTesting
    long afterReadCompressCountTotal;

    /**
     * The total length of data before compression after a read operation.
     */
    @VisibleForTesting
    long afterReadCompressBytesTotalLength;

    /**
     * The total length of data after compression after a read operation.
     */
    @VisibleForTesting
    long afterReadCompressedBytesTotalLength;

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
    // Metric stats end

    /**
     * The number of segments to batch write at once.
     */
    public static final int BATCH_ONCE_SEGMENT_COUNT_WRITE = 4;

    /**
     * The number of segments to batch read at once during repl operations.
     */
    public static final int REPL_ONCE_SEGMENT_COUNT_READ = 1024;

    // read / write reuse memory buffers
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

    // ***** ***** ***** ***** ***** ***** ***** ***** ***** *****
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
     * LRUMap for caching inner bytes by index.
     * Chunk fd is by relative segment index, key bucket fd is by bucket index.
     */
    @VisibleForTesting
    LRUMap<Integer, byte[]> oneInnerBytesByIndexLRU;

    /**
     * Initializes the byte buffers for read and write operations.
     *
     * @param isChunkFd whether this is a chunk fd
     * @param fdIndex   the index of array
     */
    public void initByteBuffers(boolean isChunkFd, int fdIndex) {
        var oneInnerLength = isChunkFd ? ConfForSlot.global.confChunk.segmentLength : KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
        this.oneInnerLength = oneInnerLength;
        this.isChunkFd = isChunkFd;
        this.fdIndex = fdIndex;

        initLRU(isChunkFd, oneInnerLength);


        long initMemoryN = 0;

        var pageManager = PageManager.getInstance();
        var m = MemoryIO.getInstance();

        var npagesOneInner = oneInnerLength / LocalPersist.PAGE_SIZE;

        this.oneInnerAddress = pageManager.allocatePages(npagesOneInner, LocalPersist.PROTECTION);
        this.oneInnerBuffer = m.newDirectByteBuffer(oneInnerAddress, oneInnerLength);

        initMemoryN += oneInnerBuffer.capacity();

        if (isChunkFd) {
            // chunk write segment batch
            var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_WRITE;
            this.writeSegmentBatchAddress = pageManager.allocatePages(npagesWriteSegmentBatch, LocalPersist.PROTECTION);
            this.writeSegmentBatchBuffer = m.newDirectByteBuffer(writeSegmentBatchAddress, npagesWriteSegmentBatch * LocalPersist.PAGE_SIZE);

            // chunk repl
            var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_READ;
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

    /**
     * Estimates the memory usage of this instance.
     *
     * @param sb the StringBuilder to append the memory usage estimate
     * @return the estimated memory usage in bytes
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        size += oneInnerBuffer.capacity();
        if (isChunkFd) {
            size += writeSegmentBatchBuffer.capacity();
            size += forReplBuffer.capacity();
            size += readForMergeBatchBuffer.capacity();
        } else {
            size += forOneWalGroupBatchBuffer.capacity();
        }
        sb.append("Fd read write buffer, name: ").append(name).append(", size: ").append(size).append("\n");

        if (isLRUOn) {
            var size2 = RamUsageEstimator.sizeOfMap(oneInnerBytesByIndexLRU);
            sb.append("LRU cache: ").append(size2).append("\n");
            size += size2;
        }
        return size;
    }

    /**
     * Initializes the LRU cache.
     *
     * @param isChunkFd      whether this is a chunk fd
     * @param oneInnerLength the length of one inner segment
     */
    private void initLRU(boolean isChunkFd, int oneInnerLength) {
        int maxSize;
        if (isChunkFd) {
            // tips: per fd, if one chunk has too many fd files, may OOM
            maxSize = ConfForSlot.global.confChunk.lruPerFd.maxSize;
            var lruMemoryRequireMB = ((long) maxSize * oneInnerLength) / 1024 / 1024;
            log.info("Chunk lru max size for one chunk fd={}, one inner length={}, memory require={}MB, name={}",
                    maxSize, oneInnerLength, lruMemoryRequireMB, name);
            log.info("LRU prepare, type={}, MB={}, fd={}, chunk", LRUPrepareBytesStats.Type.fd_chunk_data, lruMemoryRequireMB, name);
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
            log.info("LRU prepare, type={}, MB={}, fd={}, key buckets", LRUPrepareBytesStats.Type.fd_key_bucket, lruMemoryRequireMB, name);
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
            var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_WRITE;
            pageManager.freePages(writeSegmentBatchAddress, npagesWriteSegmentBatch);
            System.out.println("Clean up fd write, name=" + name + ", write segment batch address=" + writeSegmentBatchAddress);

            writeSegmentBatchAddress = 0;
            writeSegmentBatchBuffer = null;
        }

        if (forReplAddress != 0) {
            var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_READ;
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
     * @param oneInnerIndex     the index of the inner segment
     * @param buffer            the buffer to read from
     * @param isRefreshLRUCache whether the LRU cache should be refreshed
     * @return the read bytes
     */
    private byte[] readInnerByBuffer(int oneInnerIndex, @NotNull ByteBuffer buffer, boolean isRefreshLRUCache) {
        return readInnerByBuffer(oneInnerIndex, buffer, isRefreshLRUCache, buffer.capacity());
    }

    /**
     * Warms up the key bucket fd by reading and caching data.
     *
     * @return the number of buckets warmed up
     */
    @TestOnly
    public int warmUp() {
        // only for key bucket fd
        if (isChunkFd) {
            throw new IllegalArgumentException("Chunk fd not support warm up");
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

                if (n % (1024 * 10 * 4) == 0) {
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
     * @param oneInnerIndex     the index of the inner segment
     * @param buffer            the buffer to read from
     * @param isRefreshLRUCache whether the LRU cache should be refreshed
     * @param length            the length of bytes to read
     * @return the read bytes
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
        try {
            raf.seek(offset);
            n = raf.getChannel().read(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Read error, name=" + name, e);
        }
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        readTimeTotalUs += costT;
        readCountTotal++;
        readBytesTotal += n;

        if (n != readLength) {
            if (n < readLength) {
                log.error("Read error, n={}, read length={}, name={}", n, readLength, name);
                throw new RuntimeException("Read error, n=" + n + ", read length=" + readLength + ", name=" + name);
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
                    afterReadCompressTimeTotalUs += costT2;
                    afterReadCompressCountTotal++;

                    afterReadCompressBytesTotalLength += bytesRead.length;
                    afterReadCompressedBytesTotalLength += bytesCompressed.length;

                    oneInnerBytesByIndexLRU.put(oneInnerIndex, bytesCompressed);
                }
            }
        }
        return bytesRead;
    }

    /**
     * Writes inner bytes to a buffer.
     *
     * @param oneInnerIndex     the index of the inner segment
     * @param buffer            the buffer to write to
     * @param prepare           the preparation logic for the buffer
     * @param isRefreshLRUCache whether the LRU cache should be refreshed
     * @return the number of bytes written
     */
    private int writeInnerByBuffer(int oneInnerIndex, @NotNull ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        checkOneInnerIndex(oneInnerIndex);

        int capacity = buffer.capacity();
        var oneInnerCount = capacity / oneInnerLength;
        var isOnlyOneSegment = oneInnerCount == 1;
        var isPwriteBatch = oneInnerCount == BATCH_ONCE_SEGMENT_COUNT_WRITE;

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
        try {
            raf.seek(offset);
            n = raf.getChannel().write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Write error, name=" + name, e);
        }
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        writeTimeTotalUs += costT;
        writeCountTotal++;
        writeBytesTotal += n;

        if (n != capacity) {
            log.error("Write error, n={}, buffer capacity={}, name={}", n, capacity, name);
            throw new RuntimeException("Write error, n=" + n + ", buffer capacity=" + capacity + ", name=" + name);
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
     * Reads an inner segment.
     *
     * @param oneInnerIndex     the index of the inner segment
     * @param isRefreshLRUCache whether the LRU cache should be refreshed
     * @return the read bytes
     */
    public byte[] readOneInner(int oneInnerIndex, boolean isRefreshLRUCache) {
        return readInnerByBuffer(oneInnerIndex, oneInnerBuffer, isRefreshLRUCache);
    }

    /**
     * Reads segments for merge.
     *
     * @param beginSegmentIndex the starting index of the segment
     * @param segmentCount      the number of segments to read
     * @return the read bytes
     */
    public byte[] readSegmentsForMerge(int beginSegmentIndex, int segmentCount) {
        return readInnerByBuffer(beginSegmentIndex, readForMergeBatchBuffer, false, segmentCount * oneInnerLength);
    }

    /**
     * Reads a batch of segments for replication.
     *
     * @param oneInnerIndex the starting index of the inner segment
     * @return the read bytes
     */
    public byte[] readBatchForRepl(int oneInnerIndex) {
        return readInnerByBuffer(oneInnerIndex, forReplBuffer, false);
    }

    /**
     * Reads shared bytes for key buckets in one WAL group.
     *
     * @param beginBucketIndex the starting bucket index
     * @return the read bytes
     */
    public byte[] readKeyBucketsSharedBytesInOneWalGroup(int beginBucketIndex) {
        return readInnerByBuffer(beginBucketIndex, forOneWalGroupBatchBuffer, false);
    }

    /**
     * Writes an inner segment.
     *
     * @param oneInnerIndex     the index of the inner segment
     * @param bytes             the bytes to write
     * @param isRefreshLRUCache whether the LRU cache should be refreshed
     * @return the number of bytes written
     */
    public int writeOneInner(int oneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > oneInnerLength) {
            throw new IllegalArgumentException("Write bytes length must be less than one inner length");
        }

        return writeInnerByBuffer(oneInnerIndex, oneInnerBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    /**
     * Writes a batch of segments.
     *
     * @param beginOneInnerIndex the starting index of the inner segment
     * @param bytes              the bytes to write
     * @param isRefreshLRUCache  whether the LRU cache should be refreshed
     * @return the number of bytes written
     */
    public int writeSegmentsBatch(int beginOneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / oneInnerLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_WRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        return writeInnerByBuffer(beginOneInnerIndex, writeSegmentBatchBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    /**
     * Writes shared bytes for key buckets in one WAL group.
     *
     * @param bucketIndex the index of the key bucket
     * @param sharedBytes the shared bytes to write
     * @return the number of bytes written
     */
    public int writeSharedBytesForKeyBucketsInOneWalGroup(int bucketIndex, byte[] sharedBytes) {
        var keyBucketCount = sharedBytes.length / oneInnerLength;
        if (keyBucketCount != ConfForSlot.global.confWal.oneChargeBucketNumber) {
            throw new IllegalArgumentException("Batch write bytes length not match one charge bucket number in one wal group");
        }

        return writeInnerByBuffer(bucketIndex, forOneWalGroupBatchBuffer, (buffer) -> buffer.put(sharedBytes), false);
    }

    /**
     * Truncates the file to zero length.
     */
    public void truncate() {
        if (raf != null) {
            try {
                raf.setLength(0);
                log.info("Truncate raf, name={}", name);
            } catch (IOException e) {
                throw new RuntimeException("Truncate error, name=" + name, e);
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
     * @param targetSegmentIndex the target segment index
     */
    public void truncateAfterTargetSegmentIndex(int targetSegmentIndex) {
        var length = (long) targetSegmentIndex * oneInnerLength;
        if (raf != null) {
            try {
                raf.setLength(length);
                log.info("Truncate length raf, name={}", name);
            } catch (IOException e) {
                throw new RuntimeException("Truncate length error, name=" + name, e);
            }
        }
        writeIndex = length;
    }
}
