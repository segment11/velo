package io.velo;

import io.velo.persist.KeyBucket;
import io.velo.persist.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
     * @param bucketsPerSlot          the number of buckets per slot
     * @param oneChargeBucketNumber   the number of buckets charged in one wal group
     * @param segmentNumberPerFd      the number of segments per file
     * @param fdPerChunk              the number of file descriptors per chunk
     * @param segmentLength           the length of each segment
     * @param isSegmentUseCompression whether to use compression for each segment
     */
    public record ReplProperties(int bucketsPerSlot, int oneChargeBucketNumber,
                                 int segmentNumberPerFd, byte fdPerChunk, int segmentLength,
                                 boolean isSegmentUseCompression) {
    }

    /**
     * Generate replication properties.
     *
     * @return the replication properties
     */
    public ReplProperties generateReplProperties() {
        return new ReplProperties(confBucket.bucketsPerSlot, confWal.oneChargeBucketNumber,
                confChunk.segmentNumberPerFd, confChunk.fdPerChunk, confChunk.segmentLength,
                confChunk.isSegmentUseCompression);
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

    public static class SlaveCheckValues {
        long datacenterId;
        long machineId;
        long currentTimeMillis;
        int slotNumber;

        // getter setter for json
        public long getDatacenterId() {
            return datacenterId;
        }

        public void setDatacenterId(long datacenterId) {
            this.datacenterId = datacenterId;
        }

        public long getMachineId() {
            return machineId;
        }

        public void setMachineId(long machineId) {
            this.machineId = machineId;
        }

        public long getCurrentTimeMillis() {
            return currentTimeMillis;
        }

        public void setCurrentTimeMillis(long currentTimeMillis) {
            this.currentTimeMillis = currentTimeMillis;
        }

        public int getSlotNumber() {
            return slotNumber;
        }

        public void setSlotNumber(int slotNumber) {
            this.slotNumber = slotNumber;
        }
    }

    /**
     * Returns a check values that need to be checked for a slave to be considered compatible with the master.
     *
     * @return the check values
     */
    public SlaveCheckValues getSlaveCheckValues() {
        var r = new SlaveCheckValues();
        r.setDatacenterId(ConfForGlobal.datacenterId);
        r.setMachineId(ConfForGlobal.machineId);
        r.setCurrentTimeMillis(System.currentTimeMillis());
        r.setSlotNumber(ConfForGlobal.slotNumber);
        return r;
    }

    /**
     * Check if the local slave is compatible with the remote master.
     *
     * @param checkValuesFromMaster the check values from the remote master
     * @return true if the local slave is compatible with the remote master, false otherwise
     */
    public boolean slaveCanMatch(SlaveCheckValues checkValuesFromMaster) {
        if (ConfForGlobal.datacenterId != checkValuesFromMaster.datacenterId || ConfForGlobal.machineId != checkValuesFromMaster.machineId) {
            return false;
        }

        if (ConfForGlobal.slotNumber < checkValuesFromMaster.slotNumber) {
            return false;
        }

        var currentTimeMillis = System.currentTimeMillis();
        var remoteCurrentTimeMillis = checkValuesFromMaster.currentTimeMillis;
        return currentTimeMillis >= remoteCurrentTimeMillis && currentTimeMillis <= remoteCurrentTimeMillis + 100;
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
         * Number of segments to read when do repl.
         */
        public int onceReadSegmentCountWhenRepl = 64;

        /**
         * 4K * 256 = 1M, read file will cost too much time
         */
        public static final int MAX_ONCE_READ_SEGMENT_COUNT_WHEN_REPL = 256;

        /**
         * List of valid segment lengths.
         */
        private static final List<Integer> VALID_SEGMENT_LENGTH_LIST = Arrays.asList(
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
                throw new IllegalArgumentException("Chunk segment length invalid, chunk segment length should be one of "
                        + ConfForSlot.ConfChunk.VALID_SEGMENT_LENGTH_LIST);
            }

            if (ConfForGlobal.estimateOneValueLength > segmentLength / 10) {
                throw new IllegalArgumentException("Chunk segment length too small, chunk segment length should be larger than "
                        + ConfForGlobal.estimateOneValueLength * 10);
            }

            // check if chunk file number is enough for all key values encoded, considering invalid need merge values
            int estimateOneValueEncodedLength = (int) (ConfForGlobal.estimateOneValueLength * 1.5);
            long estimateChunkCanStoreValueNumber = (long) maxSegmentNumber() * segmentLength / estimateOneValueEncodedLength;
            // keep 2 times space for the chunk file, so pre-read merged segments invalid number is high, for performance
            if (estimateChunkCanStoreValueNumber < ConfForGlobal.estimateKeyNumber) {
                throw new IllegalArgumentException("Chunk segment number too small, chunk segments can store key value number should be larger than "
                        + ConfForGlobal.estimateKeyNumber +
                        ", configured chunk segment number is " + maxSegmentNumber());
            }

            if (onceReadSegmentCountWhenRepl > ConfForSlot.ConfChunk.MAX_ONCE_READ_SEGMENT_COUNT_WHEN_REPL) {
                throw new IllegalArgumentException("Chunk once read segment count when repl too large, chunk once read segment count when repl should be less than "
                        + ConfForSlot.ConfChunk.MAX_ONCE_READ_SEGMENT_COUNT_WHEN_REPL);
            }
        }

        @Override
        public String toString() {
            return "ConfChunk{" +
                    "segmentNumberPerFd=" + segmentNumberPerFd +
                    ", fdPerChunk=" + fdPerChunk +
                    ", segmentLength=" + segmentLength +
                    ", onceReadSegmentCountWhenRepl=" + onceReadSegmentCountWhenRepl +
                    ", isSegmentUseCompression=" + isSegmentUseCompression +
                    ", lruPerFd=" + lruPerFd +
                    ", maxSegmentNumber=" + maxSegmentNumber() +
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
        public int binlogOneSegmentLength = 4096 * 64;

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
        public short binlogFileKeepMaxCount = 100;

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
