package io.velo;

import io.velo.persist.FdReadWrite;
import io.velo.persist.KeyBucket;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static io.velo.persist.LocalPersist.PAGE_SIZE;

/**
 * Configuration settings for slots in the Velo application.
 * This class provides different configurations based on the estimated number of keys.
 *
 * <p>Key features:
 * <ul>
 *   <li>Configuration for different slot sizes (debugMode, c1m, c10m)</li>
 *   <li>Configuration for buckets, chunks, and write-ahead logs (WAL)</li>
 *   <li>Configuration for least recently used (LRU) caches</li>
 *   <li>Replication configuration</li>
 * </ul>
 */
public enum ConfForSlot {
    debugMode(100_000),
    c1m(1_000_000L),
    c10m(10_000_000L);

    /**
     * Logger for logging messages.
     */
    public static final Logger log = LoggerFactory.getLogger(ConfForSlot.class);

    /**
     * Configuration for buckets.
     */
    public final ConfBucket confBucket;

    /**
     * Configuration for chunks.
     */
    public final ConfChunk confChunk;

    /**
     * Configuration for write-ahead logs (WAL).
     */
    public final ConfWal confWal;

    /**
     * Configuration for replication.
     */
    public final ConfRepl confRepl = new ConfRepl();

    /**
     * Configuration for LRU cache for big strings.
     */
    public final ConfLru lruBigString = new ConfLru(1000);

    /**
     * Configuration for LRU cache for key and compressed value encoded data.
     */
    public final ConfLru lruKeyAndCompressedValueEncoded = new ConfLru(100_000);

    /**
     * Replication properties for slave.
     *
     * @param bucketsPerSlot        the number of buckets per slot
     * @param segmentNumberPerFd    the number of segments per file
     * @param fdPerChunk            the number of file descriptors per chunk
     * @param segmentLength         the length of each segment
     * @param oneChargeBucketNumber the number of buckets charged in one wal group
     */
    public record ReplProperties(int bucketsPerSlot, int segmentNumberPerFd, byte fdPerChunk, int segmentLength,
                                 int oneChargeBucketNumber) {
        /**
         * Check if the slave is redo set, or can copy key buckets / chunk / wal bytes directly.
         *
         * @param local configuration for the slave
         * @return true if the slave is redo set, false otherwise
         */
        public boolean isReplRedoSet(ReplProperties local) {
            if (bucketsPerSlot != local.bucketsPerSlot) {
                return false;
            }

            if (segmentNumberPerFd != local.segmentNumberPerFd ||
                    fdPerChunk != local.fdPerChunk ||
                    segmentLength != local.segmentLength) {
                return false;
            }

            return oneChargeBucketNumber == local.oneChargeBucketNumber;
        }
    }

    @TestOnly
    public boolean isReplRedoSet(ConfForSlot local) {
        return generateReplProperties().isReplRedoSet(local.generateReplProperties());
    }

    /**
     * Generate replication properties.
     *
     * @return the replication properties
     */
    public ReplProperties generateReplProperties() {
        return new ReplProperties(confBucket.bucketsPerSlot,
                confChunk.segmentNumberPerFd, confChunk.fdPerChunk, confChunk.segmentLength,
                confWal.oneChargeBucketNumber);
    }

    /**
     * Retrieves the appropriate configuration based on the estimated number of keys.
     *
     * @param estimateKeyNumber the estimated number of keys
     * @return the appropriate configuration for the given number of keys
     */
    public static ConfForSlot from(long estimateKeyNumber) {
        if (estimateKeyNumber <= 100_000L) {
            return debugMode;
        } else if (estimateKeyNumber <= 1_000_000L) {
            return c1m;
        } else {
            return c10m;
        }
    }

    /**
     * Returns a map of values that need to be matched for a slave to be considered compatible with the master.
     *
     * @return the map of configuration values
     */
    public HashMap<String, Object> slaveCanMatchCheckValues() {
        var map = new HashMap<String, Object>();
        map.put("datacenterId", ConfForGlobal.datacenterId);
        map.put("machineId", ConfForGlobal.machineId);
        map.put("estimateKeyNumber", ConfForGlobal.estimateKeyNumber);
        map.put("slotNumber", ConfForGlobal.slotNumber);
        map.put("bucket.bucketsPerSlot", confBucket.bucketsPerSlot);
        map.put("chunk.segmentNumberPerFd", confChunk.segmentNumberPerFd);
        map.put("chunk.fdPerChunk", confChunk.fdPerChunk);
        map.put("chunk.segmentLength", confChunk.segmentLength);
        map.put("chunk.isSegmentUseCompression", confChunk.isSegmentUseCompression);
        map.put("wal.oneChargeBucketNumber", confWal.oneChargeBucketNumber);
        map.put("repl.binlogOneSegmentLength", confRepl.binlogOneSegmentLength);
        map.put("repl.binlogOneFileMaxLength", confRepl.binlogOneFileMaxLength);

        // hash save mode needs to be the same as master
        var localPersist = LocalPersist.getInstance();
        map.put("persist.isHashSaveMemberTogether", localPersist.getIsHashSaveMemberTogether());
        return map;
    }

    /**
     * Global configuration instance.
     */
    public static ConfForSlot global = c1m;

    /**
     * Initializes the configuration based on the estimated number of keys.
     *
     * @param estimateKeyNumber the estimated number of keys
     */
    ConfForSlot(long estimateKeyNumber) {
        if (estimateKeyNumber == 100_000L) {
            this.confChunk = ConfChunk.debugMode;
            this.confBucket = ConfBucket.debugMode;
            this.confWal = ConfWal.debugMode;
        } else if (estimateKeyNumber == 1_000_000L) {
            this.confChunk = ConfChunk.c1m;
            this.confBucket = ConfBucket.c1m;
            this.confWal = ConfWal.c1m;
        } else {
            this.confChunk = ConfChunk.c10m;
            this.confBucket = ConfBucket.c10m;
            this.confWal = ConfWal.c10m;
        }
    }

    @Override
    public String toString() {
        return "ConfForSlot{" +
                "estimateKeyNumber=" + ConfForGlobal.estimateKeyNumber +
                ", isValueSetUseCompression=" + ConfForGlobal.isValueSetUseCompression +
                ", confBucket=" + confBucket +
                ", confChunk=" + confChunk +
                ", confWal=" + confWal +
                ", confRepl=" + confRepl +
                ", lruBigString=" + lruBigString +
                ", lruKeyAndCompressedValueEncoded=" + lruKeyAndCompressedValueEncoded +
                '}';
    }

    /**
     * Configuration for least recently used (LRU) caches.
     */
    public static class ConfLru {
        /**
         * Initializes the LRU cache with the specified maximum size.
         *
         * @param maxSize the maximum size of the LRU cache
         */
        public ConfLru(int maxSize) {
            this.maxSize = maxSize;
        }

        /**
         * Maximum size of the LRU cache.
         */
        public int maxSize;

        public String toString() {
            return "ConfLru{" +
                    "maxSize=" + maxSize +
                    '}';
        }
    }

    /**
     * Configuration for buckets.
     */
    public enum ConfBucket {
        debugMode(4096, (byte) 1),
        c1m(KeyBucket.DEFAULT_BUCKETS_PER_SLOT, (byte) 1),
        c10m(KeyBucket.MAX_BUCKETS_PER_SLOT, (byte) 1);

        /**
         * Initializes the bucket configuration with the specified parameters.
         *
         * @param bucketsPerSlot     the number of buckets per slot
         * @param initialSplitNumber the initialized split number
         */
        ConfBucket(int bucketsPerSlot, byte initialSplitNumber) {
            this.bucketsPerSlot = bucketsPerSlot;
            this.initialSplitNumber = initialSplitNumber;
        }

        /**
         * Number of buckets per slot.
         */
        public int bucketsPerSlot;

        /**
         * Initialized split number.
         */
        public byte initialSplitNumber;

        /**
         * For performance, when do scan, once max loop count.
         */
        public int onceScanMaxLoopCount = 1024;

        /**
         * Configuration for LRU cache per file descriptor.
         */
        public final ConfLru lruPerFd = new ConfLru(0);


        /**
         * Checks if the bucket configuration is valid.
         */
        public void checkIfValid() {
            if (bucketsPerSlot > KeyBucket.MAX_BUCKETS_PER_SLOT) {
                throw new IllegalArgumentException("Bucket count per slot too large, bucket count per slot should be less than " + KeyBucket.MAX_BUCKETS_PER_SLOT);
            }
            if (bucketsPerSlot % 1024 != 0) {
                throw new IllegalArgumentException("Bucket count per slot should be multiple of 1024");
            }
            if (initialSplitNumber != 1 && initialSplitNumber != 3) {
                throw new IllegalArgumentException("Initial split number too large, initial split number should be 1 or 3");
            }
            if (onceScanMaxLoopCount <= 0 || onceScanMaxLoopCount > 1024) {
                throw new IllegalArgumentException("Once scan max loop count should be between 1 and 1024, given: " + onceScanMaxLoopCount);
            }
        }

        @Override
        public String toString() {
            return "ConfBucket{" +
                    "bucketsPerSlot=" + bucketsPerSlot +
                    ", initialSplitNumber=" + initialSplitNumber +
                    ", onceScanMaxLoopCount=" + onceScanMaxLoopCount +
                    ", lruPerFd=" + lruPerFd +
                    '}';
        }
    }

    /**
     * Configuration for chunks.
     */
    public enum ConfChunk {
        debugMode(8 * 1024, (byte) 2, PAGE_SIZE),
        c1m(256 * 1024, (byte) 1, PAGE_SIZE),
        c10m(512 * 1024, (byte) 2, PAGE_SIZE);

        /**
         * Initializes the chunk configuration with the specified parameters.
         *
         * @param segmentNumberPerFd the number of segments per file descriptor
         * @param fdPerChunk         the number of file descriptors per chunk
         * @param segmentLength      the length of each segment
         */
        ConfChunk(int segmentNumberPerFd, byte fdPerChunk, int segmentLength) {
            this.segmentNumberPerFd = segmentNumberPerFd;
            this.fdPerChunk = fdPerChunk;
            this.segmentLength = segmentLength;
        }

        /**
         * Maximum number of file descriptors per chunk.
         */
        public static final int MAX_FD_PER_CHUNK = 16;

        /**
         * Number of segments per file descriptor.
         */
        public int segmentNumberPerFd = 256 * 1024;

        /**
         * Number of file descriptors per chunk.
         */
        public byte fdPerChunk = 1;

        /**
         * Length of each segment.
         */
        public int segmentLength = 4096;

        /**
         * List of valid segment lengths.
         */
        public static final List<Integer> VALID_SEGMENT_LENGTH_LIST = Arrays.asList(
                4096,
                8192,
                16384,
                32768,
                65536
        );

        /**
         * Flag to indicate if segments should use compression.
         */
        public boolean isSegmentUseCompression;

        /**
         * Configuration for LRU cache per file descriptor.
         */
        public final ConfLru lruPerFd = new ConfLru(0);

        /**
         * Calculates the maximum number of segments.
         *
         * @return the maximum number of segments
         */
        public int maxSegmentNumber() {
            return segmentNumberPerFd * fdPerChunk;
        }

        /**
         * Checks if the chunk configuration is valid.
         */
        public void checkIfValid() {
            if (fdPerChunk > ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK) {
                throw new IllegalArgumentException("Chunk fd per chunk too large, fd per chunk should be less than " + ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK);
            }

            if (!ConfForSlot.ConfChunk.VALID_SEGMENT_LENGTH_LIST.contains(segmentLength)) {
                throw new IllegalArgumentException("Chunk segment length invalid, chunk segment length should be one of " + ConfForSlot.ConfChunk.VALID_SEGMENT_LENGTH_LIST);
            }
            ConfForSlot.ConfChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE = new byte[FdReadWrite.REPL_ONCE_SEGMENT_COUNT_READ * segmentLength];

            if (ConfForGlobal.estimateOneValueLength > segmentLength / 10) {
                throw new IllegalArgumentException("Chunk segment length too small, chunk segment length should be larger than " + ConfForGlobal.estimateOneValueLength * 10);
            }

            // check if chunk file number is enough for all key values encoded, considering invalid need merge values
            int estimateOneValueEncodedLength = (int) (ConfForGlobal.estimateOneValueLength * 1.5);
            long estimateChunkCanStoreValueNumber = (long) maxSegmentNumber() * segmentLength / estimateOneValueEncodedLength;
            // keep 2 times space for the chunk file, so pre-read merged segments invalid number is high, for performance
            if (estimateChunkCanStoreValueNumber < ConfForGlobal.estimateKeyNumber) {
                throw new IllegalArgumentException("Chunk segment number too small, chunk segments can store key value number should be larger than " + ConfForGlobal.estimateKeyNumber +
                        ", configured chunk segment number is " + maxSegmentNumber());
            }
        }

        /**
         * Empty bytes for once write.
         */
        public static byte[] REPL_EMPTY_BYTES_FOR_ONCE_WRITE = new byte[FdReadWrite.REPL_ONCE_SEGMENT_COUNT_READ * 4096];

        @Override
        public String toString() {
            return "ConfChunk{" +
                    "segmentNumberPerFd=" + segmentNumberPerFd +
                    ", fdPerChunk=" + fdPerChunk +
                    ", segmentLength=" + segmentLength +
                    ", isSegmentUseCompression=" + isSegmentUseCompression +
                    ", lruPerFd=" + lruPerFd +
                    ", maxSegmentNumber=" + maxSegmentNumber() +
                    ", REPL_EMPTY_BYTES_FOR_ONCE_WRITE.length=" + REPL_EMPTY_BYTES_FOR_ONCE_WRITE.length +
                    '}';
        }
    }

    /**
     * Configuration for write-ahead logs (WAL).
     */
    public enum ConfWal {
        debugMode(32, 200, 200),
        c1m(32, 200, 200),
        c10m(32, 200, 200);

        /**
         * Initializes the WAL configuration with the specified parameters.
         *
         * @param oneChargeBucketNumber the number of buckets to charge at once
         * @param valueSizeTrigger      the trigger size for values
         * @param shortValueSizeTrigger the trigger size for short values
         */
        ConfWal(int oneChargeBucketNumber, int valueSizeTrigger, int shortValueSizeTrigger) {
            this.oneChargeBucketNumber = oneChargeBucketNumber;
            this.valueSizeTrigger = valueSizeTrigger;
            this.shortValueSizeTrigger = shortValueSizeTrigger;
        }

        /**
         * Number of buckets to charge in one wal group.
         */
        public int oneChargeBucketNumber;

        /**
         * Trigger to persist when >= size for values.
         */
        public int valueSizeTrigger;

        /**
         * Trigger to persist when >= size for short values.
         */
        public int shortValueSizeTrigger;

        /**
         * Trigger to persist at least once, interval ms.
         */
        public int atLeastDoPersistOnceIntervalMs = 2;

        /**
         * When value size / short value size >= size trigger * 0.8, check if last persist time is <= 2 ms, if true, do persist.
         * So can avoid io skew.
         */
        public double checkAtLeastDoPersistOnceSizeRate = 0.8;

        /**
         * For performance, when do scan, once max loop count. wal group count.
         */
        public int onceScanMaxLoopCount = 1024;

        /**
         * Checks if the WAL configuration is valid.
         */
        public void checkIfValid() {
            if (!Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST.contains(oneChargeBucketNumber)) {
                throw new IllegalArgumentException("Wal one charge bucket number invalid, wal one charge bucket number should be in " + Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST);
            }
            if (onceScanMaxLoopCount <= 0 || onceScanMaxLoopCount > 1024) {
                throw new IllegalArgumentException("Once scan max loop count should be between 1 and 1024, given: " + onceScanMaxLoopCount);
            }
        }

        @Override
        public String toString() {
            return "ConfWal{" +
                    "oneChargeBucketNumber=" + oneChargeBucketNumber +
                    ", valueSizeTrigger=" + valueSizeTrigger +
                    ", shortValueSizeTrigger=" + shortValueSizeTrigger +
                    ", atLeastDoPersistOnceIntervalMs=" + atLeastDoPersistOnceIntervalMs +
                    ", checkAtLeastDoPersistOnceSizeRate=" + checkAtLeastDoPersistOnceSizeRate +
                    ", onceScanMaxLoopCount=" + onceScanMaxLoopCount +
                    '}';
        }
    }

    /**
     * Configuration for replication.
     */
    public static class ConfRepl {
        /**
         * Length of one segment in the binlog.
         */
        public int binlogOneSegmentLength = 1024 * 1024;

        /**
         * Maximum length of one binlog file.
         */
        public int binlogOneFileMaxLength = 32 * 1024 * 1024;

        /**
         * Maximum count of segments in the read cache.
         */
        public short binlogForReadCacheSegmentMaxCount = 32;

        /**
         * Maximum count of binlog files to keep.
         */
        public short binlogFileKeepMaxCount = 10;

        /**
         * Minimum difference in catch-up offset for a slave to service reads.
         */
        public int catchUpOffsetMinDiff = 1024 * 1024;

        /**
         * Interval in milliseconds for catch-up checks.
         */
        public int catchUpIntervalMillis = 100;

        /**
         * Checks if the replication configuration is valid.
         */
        public void checkIfValid() {

        }

        @Override
        public String toString() {
            return "ConfRepl{" +
                    "binlogOneSegmentLength=" + binlogOneSegmentLength +
                    ", binlogOneFileMaxLength=" + binlogOneFileMaxLength +
                    ", binlogForReadCacheSegmentMaxCount=" + binlogForReadCacheSegmentMaxCount +
                    ", binlogFileKeepMaxCount=" + binlogFileKeepMaxCount +
                    ", catchUpOffsetMinDiff=" + catchUpOffsetMinDiff +
                    ", catchUpIntervalMillis=" + catchUpIntervalMillis +
                    '}';
        }
    }
}
