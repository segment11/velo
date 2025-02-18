package io.velo;

import io.velo.persist.KeyBucket;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static io.velo.persist.LocalPersist.PAGE_SIZE;

/**
 * Configuration settings for slots in the Velo application.
 * This class provides different configurations based on the estimated number of keys.
 *
 * <p>Key features:
 * <ul>
 *   <li>Configuration for different slot sizes (debugMode, c1m, c10m, c100m)</li>
 *   <li>Configuration for buckets, chunks, and write-ahead logs (WAL)</li>
 *   <li>Configuration for least recently used (LRU) caches</li>
 *   <li>Replication configuration</li>
 * </ul>
 */
public enum ConfForSlot {
    debugMode(100_000), c1m(1_000_000L),
    c10m(10_000_000L), c100m(100_000_000L);

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
     * Configuration for LRU cache for big strings.
     */
    public final ConfLru lruBigString = new ConfLru(1000);

    /**
     * Configuration for LRU cache for key and compressed value encoded data.
     */
    public final ConfLru lruKeyAndCompressedValueEncoded = new ConfLru(100_000);

    /**
     * Configuration for replication.
     */
    public final ConfRepl confRepl = new ConfRepl();

    /**
     * Retrieves the appropriate configuration based on the estimated number of keys.
     *
     * @param estimateKeyNumber The estimated number of keys.
     * @return The appropriate configuration for the given number of keys.
     */
    public static ConfForSlot from(long estimateKeyNumber) {
        if (estimateKeyNumber <= 100_000L) {
            return debugMode;
        } else if (estimateKeyNumber <= 1_000_000L) {
            return c1m;
        } else if (estimateKeyNumber <= 10_000_000L) {
            return c10m;
        } else {
            return c100m;
        }
    }

    /**
     * Returns a map of values that need to be matched for a slave to be considered compatible with the master.
     *
     * @return A map of configuration values.
     */
    public HashMap<String, Object> slaveCanMatchCheckValues() {
        var map = new HashMap<String, Object>();
        map.put("datacenterId", ConfForGlobal.datacenterId);
        map.put("machineId", ConfForGlobal.machineId);
        map.put("estimateKeyNumber", ConfForGlobal.estimateKeyNumber);
        map.put("pureMemory", ConfForGlobal.pureMemory);
        map.put("pureMemoryV2", ConfForGlobal.pureMemoryV2);
        map.put("slotNumber", ConfForGlobal.slotNumber);
        map.put("bucket.bucketsPerSlot", confBucket.bucketsPerSlot);
        map.put("chunk.segmentNumberPerFd", confChunk.segmentNumberPerFd);
        map.put("chunk.fdPerChunk", confChunk.fdPerChunk);
        map.put("chunk.segmentLength", confChunk.segmentLength);
        map.put("chunk.isSegmentUseCompression", confChunk.isSegmentUseCompression);
        map.put("wal.oneChargeBucketNumber", confWal.oneChargeBucketNumber);
        map.put("repl.binlogOneSegmentLength", confRepl.binlogOneSegmentLength);
        map.put("repl.binlogOneFileMaxLength", confRepl.binlogOneFileMaxLength);
        // hash save mode need be same as master
        map.put("persist.isHashSaveMemberTogether", LocalPersist.getInstance().getIsHashSaveMemberTogether());
        return map;
    }

    /**
     * Global configuration instance.
     */
    public static ConfForSlot global = c1m;

    /**
     * Initializes the configuration based on the estimated number of keys.
     *
     * @param estimateKeyNumber The estimated number of keys.
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
        } else if (estimateKeyNumber == 10_000_000L) {
            this.confChunk = ConfChunk.c10m;
            this.confBucket = ConfBucket.c10m;
            this.confWal = ConfWal.c10m;
        } else {
            this.confChunk = ConfChunk.c100m;
            this.confBucket = ConfBucket.c100m;
            this.confWal = ConfWal.c100m;
        }
    }

    @Override
    public String toString() {
        return "ConfForSlot{" +
                "estimateKeyNumber=" + ConfForGlobal.estimateKeyNumber +
                ", isValueSetUseCompression=" + ConfForGlobal.isValueSetUseCompression +
                ", confChunk=" + confChunk +
                ", confBucket=" + confBucket +
                ", confWal=" + confWal +
                '}';
    }

    /**
     * Configuration for least recently used (LRU) caches.
     */
    public static class ConfLru {
        /**
         * Initializes the LRU cache with the specified maximum size.
         *
         * @param maxSize The maximum size of the LRU cache.
         */
        public ConfLru(int maxSize) {
            this.maxSize = maxSize;
        }

        /**
         * Maximum size of the LRU cache.
         */
        public int maxSize;
    }

    /**
     * Configuration for buckets.
     */
    public enum ConfBucket {
        debugMode(4096, (byte) 1),
        c1m(KeyBucket.DEFAULT_BUCKETS_PER_SLOT, (byte) 1),
        c10m(KeyBucket.MAX_BUCKETS_PER_SLOT / 2, (byte) 1),
        c100m(KeyBucket.MAX_BUCKETS_PER_SLOT, (byte) 3);

        /**
         * Initializes the bucket configuration with the specified parameters.
         *
         * @param bucketsPerSlot     The number of buckets per slot.
         * @param initialSplitNumber The initial split number.
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
         * Initial split number.
         */
        public byte initialSplitNumber;

        /**
         * Configuration for LRU cache per file descriptor.
         */
        public final ConfLru lruPerFd = new ConfLru(0);

        @Override
        public String toString() {
            return "ConfBucket{" +
                    "bucketsPerSlot=" + bucketsPerSlot +
                    ", initialSplitNumber=" + initialSplitNumber +
                    '}';
        }
    }

    /**
     * Configuration for chunks.
     */
    public enum ConfChunk {
        debugMode(8 * 1024, (byte) 2, PAGE_SIZE),
        c1m(256 * 1024, (byte) 1, PAGE_SIZE),
        c10m(512 * 1024, (byte) 2, PAGE_SIZE),
        c100m(512 * 1024, (byte) 8, PAGE_SIZE);

        /**
         * Initializes the chunk configuration with the specified parameters.
         *
         * @param segmentNumberPerFd The number of segments per file descriptor.
         * @param fdPerChunk         The number of file descriptors per chunk.
         * @param segmentLength      The length of each segment.
         */
        ConfChunk(int segmentNumberPerFd, byte fdPerChunk, int segmentLength) {
            this.segmentNumberPerFd = segmentNumberPerFd;
            this.fdPerChunk = fdPerChunk;
            this.segmentLength = segmentLength;
        }

        /**
         * Maximum number of file descriptors per chunk.
         */
        public static final int MAX_FD_PER_CHUNK = 64;

        /**
         * Number of segments per file descriptor.
         */
        public int segmentNumberPerFd;

        /**
         * Number of file descriptors per chunk.
         */
        public byte fdPerChunk;

        /**
         * Length of each segment.
         */
        public int segmentLength;

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
         * @return The maximum number of segments.
         */
        public int maxSegmentNumber() {
            return segmentNumberPerFd * fdPerChunk;
        }

        /**
         * Empty bytes for once write.
         */
        public byte[] REPL_EMPTY_BYTES_FOR_ONCE_WRITE;

        @Override
        public String toString() {
            return "ConfChunk{" +
                    "segmentNumberPerFd=" + segmentNumberPerFd +
                    ", fdPerChunk=" + fdPerChunk +
                    ", segmentLength=" + segmentLength +
                    '}';
        }
    }

    /**
     * Configuration for write-ahead logs (WAL).
     */
    public enum ConfWal {
        debugMode(32, 200, 200),
        c1m(32, 200, 200),
        c10m(32, 200, 200),
        c100m(32, 200, 200);

        /**
         * Initializes the WAL configuration with the specified parameters.
         *
         * @param oneChargeBucketNumber The number of buckets to charge at once.
         * @param valueSizeTrigger      The trigger size for values.
         * @param shortValueSizeTrigger The trigger size for short values.
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
         * Resets WAL static values based on the one group buffer size.
         *
         * @param oneGroupBufferSize The size of one group buffer.
         */
        public void resetWalStaticValues(int oneGroupBufferSize) {
            if (Wal.ONE_GROUP_BUFFER_SIZE != oneGroupBufferSize) {
                Wal.ONE_GROUP_BUFFER_SIZE = oneGroupBufferSize;
                Wal.EMPTY_BYTES_FOR_ONE_GROUP = new byte[Wal.ONE_GROUP_BUFFER_SIZE];

                int groupNumber = Wal.calcWalGroupNumber();
                var sum = oneGroupBufferSize * groupNumber / 1024 / 1024;
                // wal init m4
                sum += 4;

                log.info("Static memory init, type={}, MB={}, all slots", StaticMemoryPrepareBytesStats.Type.wal_cache, sum);
                StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache, sum, false);
            }
            Wal.doLogAfterInit();
        }

        @Override
        public String toString() {
            return "ConfWal{" +
                    "oneChargeBucketNumber=" + oneChargeBucketNumber +
                    ", valueSizeTrigger=" + valueSizeTrigger +
                    ", shortValueSizeTrigger=" + shortValueSizeTrigger +
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
        public final int binlogOneSegmentLength = 1024 * 1024;

        /**
         * Maximum length of one binlog file.
         */
        public final int binlogOneFileMaxLength = 512 * 1024 * 1024;

        /**
         * Maximum count of segments in the read cache.
         */
        public short binlogForReadCacheSegmentMaxCount = 100;

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
         * Batch size for iterating keys.
         */
        public int iterateKeysOneBatchSize = 10000;

        @Override
        public String toString() {
            return "ConfRepl{" +
                    "binlogOneSegmentLength=" + binlogOneSegmentLength +
                    ", binlogOneFileMaxLength=" + binlogOneFileMaxLength +
                    ", binlogForReadCacheSegmentMaxCount=" + binlogForReadCacheSegmentMaxCount +
                    ", catchUpOffsetMinDiff=" + catchUpOffsetMinDiff +
                    ", catchUpIntervalMillis=" + catchUpIntervalMillis +
                    '}';
        }
    }
}
