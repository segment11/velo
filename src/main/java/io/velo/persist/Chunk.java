package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.NullableOnlyTest;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE;

/**
 * A collection of segments that store data, with multiple file descriptors.
 */
public class Chunk implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp {
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    /**
     * @return the maximum segment index
     */
    public int getMaxSegmentIndex() {
        return maxSegmentIndex;
    }

    enum SegmentType {
        NORMAL((byte) 0),
        TIGHT((byte) 1);

        final byte val;

        SegmentType(byte val) {
            this.val = val;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Chunk.class);

    private final short slot;
    private final File slotDir;

    // For better latency, segment length = 4096 decompress performance is better
    final int chunkSegmentLength;

    @Override
    public String toString() {
        return "Chunk{" +
                "slot=" + slot +
                ", segmentNumberPerFd=" + segmentNumberPerFd +
                ", fdPerChunk=" + fdPerChunk +
                ", maxSegmentIndex=" + maxSegmentIndex +
                ", halfSegmentNumber=" + halfSegmentNumber +
                ", chunkSegmentLength=" + chunkSegmentLength +
                ", segmentIndex=" + segmentIndex +
                '}';
    }

    @VisibleForTesting
    long persistCallCountTotal;
    @VisibleForTesting
    long persistCvCountTotal;

    int calcSegmentNumberWhenOverHalfEstimateKeyNumber = 0;
    long calcCvCountWhenOverHalfSegmentCount = 0;

    @VisibleForTesting
    long updatePvmBatchCostTimeTotalUs;
    @VisibleForTesting
    long segmentDecompressTimeTotalUs = 0;
    @VisibleForTesting
    long segmentDecompressCountTotal = 0;

    @VisibleForTesting
    final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    private final MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;
    final SegmentBatch segmentBatch;
    private final SegmentBatch2 segmentBatch2;

    long[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    /**
     * @param slot    the slot index this chunk belongs to
     * @param slotDir the directory of the slot
     * @param oneSlot the context for the slot
     */
    public Chunk(short slot, @NotNull File slotDir, @NullableOnlyTest OneSlot oneSlot) {
        var confChunk = ConfForSlot.global.confChunk;
        this.segmentNumberPerFd = confChunk.segmentNumberPerFd;
        this.fdPerChunk = confChunk.fdPerChunk;

        int maxSegmentNumber = confChunk.maxSegmentNumber();
        this.maxSegmentIndex = maxSegmentNumber - 1;
        this.halfSegmentNumber = maxSegmentNumber / 2;

        log.info("Chunk init slot={}, segment number per fd={}, fd per chunk={}, max segment index={}, half segment number={}",
                slot, segmentNumberPerFd, fdPerChunk, maxSegmentIndex, halfSegmentNumber);

        this.slot = slot;
        this.slotDir = slotDir;

        this.chunkSegmentLength = confChunk.segmentLength;

        this.oneSlot = oneSlot;
        this.keyLoader = oneSlot.keyLoader;
        this.metaChunkSegmentFlagSeq = oneSlot.metaChunkSegmentFlagSeq;
        this.segmentBatch = new SegmentBatch(oneSlot.snowFlake);
        this.segmentBatch2 = new SegmentBatch2(oneSlot.snowFlake);
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        for (var fdReadWrite : fdReadWriteArray) {
            if (fdReadWrite != null) {
                size += fdReadWrite.estimate(sb);
            }
        }
        return size;
    }

    @VisibleForTesting
    void initFds() throws IOException {
        this.fdLengths = new long[fdPerChunk];
        this.fdReadWriteArray = new FdReadWrite[fdPerChunk];
        for (int i = 0; i < fdPerChunk; i++) {
            // Prometheus metric labels use _ instead of -
            var name = "chunk_data_index_" + i + "_slot_" + slot;
            var file = new File(slotDir, "chunk-data-" + i);
            fdLengths[i] = file.length();

            var fdReadWrite = new FdReadWrite(name, file);
            fdReadWrite.initByteBuffers(true, i);

            this.fdReadWriteArray[i] = fdReadWrite;
        }
    }

    @Override
    public void cleanUp() {
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                fdReadWrite.cleanUp();
            }
        }
    }

    /**
     * @param beginSegmentIndex the starting segment index for the read
     * @param segmentCount      the number of segments to read
     * @return the byte array containing the read segment data
     */
    byte[] readForMerge(int beginSegmentIndex, int segmentCount) {
        if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
            throw new IllegalArgumentException("Merge read segment count too large=" + segmentCount);
        }

        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentsForMerge(segmentIndexTargetFd, segmentCount);
    }

    /**
     * @param beginSegmentIndex the starting segment index for the read
     * @return the byte array containing the read segment data
     */
    byte[] readForRepl(int beginSegmentIndex) {
        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readBatchForRepl(segmentIndexTargetFd);
    }

    /**
     * @param targetSegmentIndex the segment index to read
     * @return the byte array containing the read segment data
     */
    byte[] readOneSegment(int targetSegmentIndex) {
        return readOneSegment(targetSegmentIndex, true);
    }

    /**
     * @param targetSegmentIndex the segment index to read
     * @param isRefreshLRUCache  whether to refresh the LRU cache
     * @return the byte array containing the read segment data
     */
    byte[] readOneSegment(int targetSegmentIndex, boolean isRefreshLRUCache) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInner(segmentIndexTargetFd, isRefreshLRUCache);
    }

    private int segmentIndex = 0;

    /**
     * Returns the current segment index used as the write cursor.
     *
     * @return the current segment index
     */
    public int getSegmentIndex() {
        return segmentIndex;
    }

    /**
     * Sets the current segment index used as the write cursor.
     *
     * @param segmentIndex the new segment index
     */
    public void setSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    @SlaveNeedReplay
    @SlaveReplay
    void resetAsFlush() {
        segmentIndex = 0;
    }

    void truncateAll() {
        for (int i = 0; i < fdLengths.length; i++) {
            if (fdLengths[i] != 0L) {
                fdReadWriteArray[i].truncate();
                fdLengths[i] = 0L;
            }
        }
    }

    @VisibleForTesting
    int targetFdIndex() {
        return segmentIndex / segmentNumberPerFd;
    }

    /**
     * @param targetSegmentIndex the target segment index
     * @return the file descriptor index for the target segment index
     */
    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / segmentNumberPerFd;
    }

    @VisibleForTesting
    int targetSegmentIndexTargetFd() {
        return segmentIndex % segmentNumberPerFd;
    }

    /**
     * @param targetSegmentIndex the target segment index
     * @return the segment index within the current file descriptor
     */
    @VisibleForTesting
    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % segmentNumberPerFd;
    }

    /**
     * Flag byte marking a segment as reusable (empty / overwritten).
     */
    public static final byte SEGMENT_FLAG_REUSABLE = 0;
    /**
     * Flag byte marking a segment as holding live data.
     */
    public static final byte SEGMENT_FLAG_HAS_DATA = 1;

    /**
     * Returns whether the given flag byte marks a segment as reusable.
     *
     * @param flagByte the segment flag byte
     * @return true if the segment is reusable
     */
    public static boolean isSegmentReusable(byte flagByte) {
        return flagByte == SEGMENT_FLAG_REUSABLE;
    }

    /**
     * Metadata flag stored per segment describing its state and ownership.
     *
     * @param flagByte      the segment flag, either {@link #SEGMENT_FLAG_REUSABLE} or {@link #SEGMENT_FLAG_HAS_DATA}
     * @param segmentSeq    the sequence number of the segment
     * @param walGroupIndex the WAL group index that owns the segment
     */
    public record SegmentFlag(byte flagByte, long segmentSeq, int walGroupIndex) {
        @Override
        public @NotNull String toString() {
            return "SegmentFlag{" +
                    "flagByte=" + flagByte +
                    ", segmentSeq=" + segmentSeq +
                    ", walGroupIndex=" + walGroupIndex +
                    '}';
        }
    }

    int maxOncePersistSegmentSize;

    /**
     * @param walGroupIndex                the index of the WAL group being persisted
     * @param list                         the data to persist, in the form of a list of WAL values
     * @param keyBucketsInOneWalGroupGiven optional parameter for details about key buckets in the WAL group
     */
    public void persist(int walGroupIndex,
                        @NotNull ArrayList<Wal.V> list,
                        @Nullable KeyBucketsInOneWalGroup keyBucketsInOneWalGroupGiven) {
        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = ConfForSlot.global.confChunk.isSegmentUseCompression ?
                segmentBatch.split(list, pvmList) : segmentBatch2.split(list, pvmList);
        assert (segments.size() <= Short.MAX_VALUE);
        short segmentCount = (short) segments.size();

        // metric, for debug
        maxOncePersistSegmentSize = Math.max(maxOncePersistSegmentSize, segmentCount);

        var ii = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(segmentIndex, segmentCount);
        if (ii == -1) {
            if (segmentIndex != 0) {
                // from beginning find again
                ii = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(0, segmentCount);
            }
        }
        if (ii == -1) {
            throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex + ", size=" + segmentCount);
        }
        segmentIndex = ii;

        // reset segment index after find those segments can reuse
        var currentSegmentIndex = this.segmentIndex;
        for (var pvm : pvmList) {
            pvm.segmentIndex += currentSegmentIndex;
        }

        if (segmentCount < BATCH_ONCE_SEGMENT_COUNT_WRITE) {
            for (var segment : segments) {
                int targetSegmentIndex = segment.tmpSegmentIndex() + currentSegmentIndex;
                var bytes = segment.segmentBytes();
                writeSegments(bytes, 1);

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlag(targetSegmentIndex,
                        SEGMENT_FLAG_HAS_DATA, segment.segmentSeq(), walGroupIndex);

                moveSegmentIndexNext(1);
            }
        } else {
            // batch write, perf better ? need test
            var batchCount = segmentCount / BATCH_ONCE_SEGMENT_COUNT_WRITE;
            var remainCount = segmentCount % BATCH_ONCE_SEGMENT_COUNT_WRITE;

            var tmpBatchBytes = new byte[chunkSegmentLength * BATCH_ONCE_SEGMENT_COUNT_WRITE];
            var buffer = ByteBuffer.wrap(tmpBatchBytes);

            for (int i = 0; i < batchCount; i++) {
                buffer.clear();
                if (i > 0) {
                    Arrays.fill(tmpBatchBytes, (byte) 0);
                }

                List<Long> segmentSeqListSubBatch = new ArrayList<>();
                for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_WRITE; j++) {
                    var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_WRITE + j);
                    var bytes = segment.segmentBytes();
                    buffer.put(bytes);

                    // padding to segment length
                    if (bytes.length < chunkSegmentLength) {
                        buffer.position(buffer.position() + chunkSegmentLength - bytes.length);
                    }

                    segmentSeqListSubBatch.add(segment.segmentSeq());
                }

                writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_WRITE);

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_WRITE,
                        SEGMENT_FLAG_HAS_DATA, segmentSeqListSubBatch, walGroupIndex);

                moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_WRITE);
            }

            for (int i = 0; i < remainCount; i++) {
                var leftSegment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_WRITE + i);
                var bytes = leftSegment.segmentBytes();
                int targetSegmentIndex = leftSegment.tmpSegmentIndex() + currentSegmentIndex;
                writeSegments(bytes, 1);

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlag(targetSegmentIndex,
                        SEGMENT_FLAG_HAS_DATA, leftSegment.segmentSeq(), walGroupIndex);

                moveSegmentIndexNext(1);
            }
        }

        // stats
        persistCallCountTotal++;
        persistCvCountTotal += list.size();

        if (calcSegmentNumberWhenOverHalfEstimateKeyNumber == 0) {
            if ((double) persistCvCountTotal / ConfForGlobal.estimateKeyNumber > 0.5) {
                calcSegmentNumberWhenOverHalfEstimateKeyNumber = segmentIndex;
                log.warn("!!!Over half estimate key number, calc segment count when over half estimate key number, slot={}, " +
                                "segment count={}, estimate key number={}",
                        slot, calcSegmentNumberWhenOverHalfEstimateKeyNumber, ConfForGlobal.estimateKeyNumber);
            }
        }

        if (calcCvCountWhenOverHalfSegmentCount == 0) {
            if (segmentIndex >= halfSegmentNumber) {
                calcCvCountWhenOverHalfSegmentCount = persistCvCountTotal;
                log.warn("!!!Over half segment count, calc cv count when over half segment count, slot={}, " +
                                "persist cv count={}",
                        slot, persistCvCountTotal);
            }
        }

        var beginT = System.nanoTime();
        try {
            keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, keyBucketsInOneWalGroupGiven);
        } catch (RuntimeException e) {
            log.error("Update key buckets after write chunk segments failed, s={}, wal group index={}, begin segment index={}, " +
                            "segment count={}, next segment index={}, value count={}, pvm count={}",
                    slot, walGroupIndex, currentSegmentIndex, segmentCount, segmentIndex, list.size(), pvmList.size(), e);
            // roll back: flip the just-written segments back to REUSABLE so the orphan range is not leaked.
            // Phase 3 (markPersistedSegmentIndexToTargetWalGroup) was never reached, so no marker needs to be cleared.
            oneSlot.setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount,
                    SEGMENT_FLAG_REUSABLE, null, walGroupIndex);
            // rewind the in-memory cursor so the next persist immediately reuses the rolled-back range.
            this.segmentIndex = currentSegmentIndex;
            throw e;
        }
        var costT = (System.nanoTime() - beginT) / 1000;
        updatePvmBatchCostTimeTotalUs += costT;

        metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segmentCount);

        // update meta, segment index for next time
        oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
    }

    /**
     * @param segmentCount the number of segments to move forward
     */
    @VisibleForTesting
    void moveSegmentIndexNext(int segmentCount) {
        if (segmentCount > 1) {
            var maxSegmentNumber = maxSegmentIndex + 1;
            var newSegmentIndex = segmentIndex + segmentCount;
            if (newSegmentIndex >= maxSegmentNumber) {
                if (newSegmentIndex > maxSegmentNumber) {
                    throw new SegmentOverflowException("Segment index overflow, s=" + slot + ", i=" + segmentIndex +
                            ", c=" + segmentCount + ", max=" + maxSegmentIndex);
                }
                newSegmentIndex = 0;
            }
            segmentIndex = newSegmentIndex;
            return;
        }

        if (segmentIndex == maxSegmentIndex) {
            log.warn("Chunk segment index reach max reuse, s={}, i={}", slot, segmentIndex);
            segmentIndex = 0;
        } else {
            segmentIndex++;

            if (segmentIndex == halfSegmentNumber) {
                log.warn("Chunk segment index reach half of max, start reuse from beginning, s={}, i={}", slot, segmentIndex);
            }
        }
    }

    /**
     * @param isNewAppend    whether this is a new append operation
     * @param bySegmentIndex the current segment index
     * @return the adjusted segment index for merging
     */
    int prevFindSegmentIndexSkipHalf(boolean isNewAppend, int bySegmentIndex) {
        int segmentIndexToMerge = -1;
        if (bySegmentIndex >= halfSegmentNumber) {
            // begins with 0
            // ends with 2^18 - 1
            segmentIndexToMerge = bySegmentIndex - halfSegmentNumber;
        } else {
            if (!isNewAppend) {
                segmentIndexToMerge = bySegmentIndex + halfSegmentNumber;
            }
        }
        return segmentIndexToMerge;
    }

    /**
     * @param bytes        the byte array containing the data to write
     * @param segmentCount the number of segments to write
     */
    @VisibleForTesting
    public void writeSegments(byte[] bytes, int segmentCount) {
        if (segmentCount != 1 && segmentCount != BATCH_ONCE_SEGMENT_COUNT_WRITE) {
            throw new IllegalArgumentException("Write segment count not support=" + segmentCount);
        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
        } else {
            fdReadWrite.writeSegmentsBatch(segmentIndexTargetFd, bytes, false);
        }

        long afterThisBatchOffset = (long) (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
        }
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        map.put("chunk_current_segment_index", (double) segmentIndex);
        map.put("chunk_max_segment_index", (double) maxSegmentIndex);
        map.put("chunk_max_once_segment_size", (double) maxOncePersistSegmentSize);
        map.put("chunk_segment_number_when_over_half_estimate_key_number", (double) calcSegmentNumberWhenOverHalfEstimateKeyNumber);
        map.put("chunk_cv_count_when_over_half_segment_count", (double) calcCvCountWhenOverHalfSegmentCount);

        if (persistCallCountTotal > 0) {
            map.put("chunk_persist_call_count_total", (double) persistCallCountTotal);
            map.put("chunk_persist_cv_count_total", (double) persistCvCountTotal);
            map.put("chunk_persist_cv_count_avg", (double) persistCvCountTotal / persistCallCountTotal);

            map.put("chunk_update_pvm_batch_cost_time_total_us", (double) updatePvmBatchCostTimeTotalUs);
            map.put("chunk_update_pvm_batch_cost_time_avg_us", (double) updatePvmBatchCostTimeTotalUs / persistCallCountTotal);
        }

        if (segmentDecompressCountTotal > 0) {
            map.put("chunk_segment_decompress_time_total_us", (double) segmentDecompressTimeTotalUs);
            map.put("chunk_segment_decompress_count_total", (double) segmentDecompressCountTotal);
            double segmentDecompressedCostTAvg = (double) segmentDecompressTimeTotalUs / segmentDecompressCountTotal;
            map.put("chunk_segment_decompress_cost_time_avg_us", segmentDecompressedCostTAvg);
        }

        if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
            map.putAll(segmentBatch.collect());
        } else {
            map.putAll(segmentBatch2.collect());
        }

        var diskUsage = 0L;
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    diskUsage += fdReadWrite.writeIndex;
                    map.putAll(fdReadWrite.collect());
                }
            }
        }

        diskUsage += metaChunkSegmentFlagSeq.allCapacity;
        map.put("chunk_disk_usage", (double) diskUsage);

        var maxSegmentNumber = maxSegmentIndex + 1;
        var reusableSegmentCount = metaChunkSegmentFlagSeq.countReusableSegments();
        var usedSegmentCount = maxSegmentNumber - reusableSegmentCount;
        map.put("chunk_segment_reusable_count", (double) reusableSegmentCount);
        map.put("chunk_segment_used_count", (double) usedSegmentCount);
        map.put("chunk_segment_fill_rate", (double) usedSegmentCount / maxSegmentNumber);

        return map;
    }
}
