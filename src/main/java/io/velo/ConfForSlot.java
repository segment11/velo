package io.velo;

import io.velo.persist.KeyBucket;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static io.velo.persist.LocalPersist.PAGE_SIZE;

public enum ConfForSlot {
    debugMode(100_000), c1m(1_000_000L),
    c10m(10_000_000L), c100m(100_000_000L);

    public static final Logger log = LoggerFactory.getLogger(ConfForSlot.class);

    public final ConfBucket confBucket;
    public final ConfChunk confChunk;
    public final ConfWal confWal;
    public final ConfLru lruBigString = new ConfLru(1000);
    public final ConfLru lruKeyAndCompressedValueEncoded = new ConfLru(100_000);
    public final ConfRepl confRepl = new ConfRepl();

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

    public static ConfForSlot global = c1m;

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

    public static class ConfLru {
        public ConfLru(int maxSize) {
            this.maxSize = maxSize;
        }

        public int maxSize;
    }

    public enum ConfBucket {
        debugMode(4096, (byte) 1),
        c1m(KeyBucket.DEFAULT_BUCKETS_PER_SLOT, (byte) 1),
        c10m(KeyBucket.MAX_BUCKETS_PER_SLOT / 2, (byte) 1),
        c100m(KeyBucket.MAX_BUCKETS_PER_SLOT, (byte) 3);

        ConfBucket(int bucketsPerSlot, byte initialSplitNumber) {
            this.bucketsPerSlot = bucketsPerSlot;
            this.initialSplitNumber = initialSplitNumber;
        }

        /*
        suppose memory / ssd capacity ~= 1:8
        when 1000w key, c10m, wal will use 256MB, ssd may use 2-3GB
         */
        public int bucketsPerSlot;
        public byte initialSplitNumber;

        // 4KB one segment, 25 * 1000 * 4KB = 100MB
        public final ConfLru lruPerFd = new ConfLru(0);

        @Override
        public String toString() {
            return "ConfBucket{" +
                    "bucketsPerSlot=" + bucketsPerSlot +
                    ", initialSplitNumber=" + initialSplitNumber +
                    '}';
        }
    }

    public enum ConfChunk {
        debugMode(8 * 1024, (byte) 2, PAGE_SIZE),
        c1m(256 * 1024, (byte) 1, PAGE_SIZE),
        c10m(512 * 1024, (byte) 2, PAGE_SIZE),
        c100m(512 * 1024, (byte) 8, PAGE_SIZE);

        ConfChunk(int segmentNumberPerFd, byte fdPerChunk, int segmentLength) {
            this.segmentNumberPerFd = segmentNumberPerFd;
            this.fdPerChunk = fdPerChunk;
            this.segmentLength = segmentLength;
        }

        public static final int MAX_FD_PER_CHUNK = 64;

        // each slot each worker persist to a file, one file one chunk, each file max 2GB, 4KB page size, each file max 512K pages
        public int segmentNumberPerFd;
        // 16 * 2GB = 32GB per slot (per worker)
        // suppose one key value encoded length (value is already compressed) ~= 100 byte, one page size 4096 contains 40 key value pairs
        // one fd contains 512K pages, so one fd contains 20M key value pairs
        // one chunk contains 20M * 16 = 320M key value pairs
        // merge worker is another chunk, so one slot may contain 640M key value pairs

        // if one key value encoded length (value is already compressed) ~= 500 byte, one page size 4096 contains 8 key value pairs
        // fd per chunk need to be 32 or 64
        public byte fdPerChunk;
        // for better latency, PAGE_SIZE 4K is ok
        public int segmentLength;
        public boolean isSegmentUseCompression;

        // 4KB one segment, 25 * 1000 * 4KB = 100MB
        public final ConfLru lruPerFd = new ConfLru(0);

        public int maxSegmentNumber() {
            return segmentNumberPerFd * fdPerChunk;
        }

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

    public enum ConfWal {
        debugMode(32, 200, 200),
        c1m(32, 200, 200),
        c10m(32, 200, 200),
        c100m(32, 200, 200);

        ConfWal(int oneChargeBucketNumber, int valueSizeTrigger, int shortValueSizeTrigger) {
            this.oneChargeBucketNumber = oneChargeBucketNumber;
            this.valueSizeTrigger = valueSizeTrigger;
            this.shortValueSizeTrigger = shortValueSizeTrigger;
        }

        public int oneChargeBucketNumber;

        // refer to Chunk BATCH_SEGMENT_COUNT_FOR_PWRITE
        // 4 pages ~= 16KB, one V persist length is about 100B, so 4 pages can store about 160 V
        // for better latency, do not configure too large
        // 200 make sure there is at least one batch 16KB
        // must < 512, scan cursor use 10 bits for wal skip key count, value and short value key count total need < 1024
        public int valueSizeTrigger;
        // must < 512
        public int shortValueSizeTrigger;

        private int oneChargeBucketNumberOld;
        private int valueSizeTriggerOld;
        private int shortValueSizeTriggerOld;

        void mark() {
            this.oneChargeBucketNumberOld = this.oneChargeBucketNumber;
            this.valueSizeTriggerOld = this.valueSizeTrigger;
            this.shortValueSizeTriggerOld = this.shortValueSizeTrigger;
        }

        void reset() {
            this.oneChargeBucketNumber = this.oneChargeBucketNumberOld;
            this.valueSizeTrigger = this.valueSizeTriggerOld;
            this.shortValueSizeTrigger = this.shortValueSizeTriggerOld;
        }

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

    public static class ConfRepl {
        // because padding, can not change after binlog file created
        // for better latency, do not configure too large
        // 256KB ~= io depths = 64 (4KB * 64 = 256KB)
        // 1M ~= io depths = 256 (4KB * 256 = 1M)
        public final int binlogOneSegmentLength = 1024 * 1024;
        public final int binlogOneFileMaxLength = 512 * 1024 * 1024;
        public short binlogForReadCacheSegmentMaxCount = 100;
        public short binlogFileKeepMaxCount = 10;
        // if slave catch up binlog offset is less than min diff, slave can service read
        public int catchUpOffsetMinDiff = 1024 * 1024;
        public int catchUpIntervalMillis = 100;
        // for exists_keys fetch
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
