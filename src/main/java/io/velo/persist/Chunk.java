package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.NullableOnlyTest;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import jnr.posix.LibC;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE;
import static io.velo.persist.FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;

public class Chunk implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, CanSaveAndLoad {
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    public int getMaxSegmentIndex() {
        return maxSegmentIndex;
    }

    enum SegmentType {
        NORMAL((byte) 0), TIGHT((byte) 1), SLIM((byte) 2);

        final byte val;

        SegmentType(byte val) {
            this.val = val;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Chunk.class);

    private final short slot;
    private final File slotDir;

    // for better latency, segment length = 4096 decompress performance is better
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

    int calcSegmentCountStepWhenOverHalfEstimateKeyNumber = 0;

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

    int[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    public FdReadWrite[] getFdReadWriteArray() {
        return fdReadWriteArray;
    }

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
        this.segmentBatch = new SegmentBatch(slot, oneSlot.snowFlake);
        this.segmentBatch2 = new SegmentBatch2(slot, oneSlot.snowFlake);
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
    void initFds(LibC libC) throws IOException {
        this.fdLengths = new int[fdPerChunk];
        this.fdReadWriteArray = new FdReadWrite[fdPerChunk];
        for (int i = 0; i < fdPerChunk; i++) {
            // prometheus metric labels use _ instead of -
            var name = "chunk_data_index_" + i;
            var file = new File(slotDir, "chunk-data-" + i);
            fdLengths[i] = (int) file.length();

            var fdReadWrite = new FdReadWrite(slot, name, libC, file);
            fdReadWrite.initByteBuffers(true);

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

    public void clearSegmentBytesWhenPureMemory(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
    }

    public void truncateChunkFdFromSegmentIndex(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.truncateAfterTargetSegmentIndex(segmentIndexTargetFd);
        fdLengths[fdIndex] = chunkSegmentLength * targetSegmentIndex;

        log.warn("Truncate chunk fd from segment index={}, fd index={}", targetSegmentIndex, fdIndex);

        for (int i = fdIndex + 1; i < fdReadWriteArray.length; i++) {
            fdReadWriteArray[i].truncate();
            log.warn("Truncate chunk fd={}, fd index={}", fdReadWriteArray[i].name, i);
            fdLengths[i] = 0;
        }
    }

    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        segmentIndex = is.readInt();

        // fd read write
        var fdCount = is.readInt();
        for (int i = 0; i < fdCount; i++) {
            var fdIndex = is.readInt();
            var segmentCount = is.readInt();
            var fd = fdReadWriteArray[fdIndex];
            for (int j = 0; j < segmentCount; j++) {
                var inMemorySegmentIndex = is.readInt();
                var readBytesLength = is.readInt();
                var readBytes = new byte[readBytesLength];
                is.readFully(readBytes);
                fd.setSegmentBytesFromLastSavedFileToMemory(readBytes, inMemorySegmentIndex);
                fdLengths[fdIndex] = chunkSegmentLength * inMemorySegmentIndex;
            }
        }
    }

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.writeInt(segmentIndex);

        int fdCount = 0;
        for (var fdReadWrite : fdReadWriteArray) {
            if (fdReadWrite != null) {
                fdCount++;
            }
        }

        os.writeInt(fdCount);
        for (int fdIndex = 0; fdIndex < fdReadWriteArray.length; fdIndex++) {
            var fdReadWrite = fdReadWriteArray[fdIndex];
            if (fdReadWrite == null) {
                continue;
            }

            os.writeInt(fdIndex);
            int segmentCount = 0;
            for (int inMemorySegmentIndex = 0; inMemorySegmentIndex < fdReadWrite.allBytesBySegmentIndexForOneChunkFd.length; inMemorySegmentIndex++) {
                var segmentBytes = fdReadWrite.allBytesBySegmentIndexForOneChunkFd[inMemorySegmentIndex];
                if (segmentBytes != null) {
                    segmentCount++;
                }
            }
            os.writeInt(segmentCount);

            for (int inMemorySegmentIndex = 0; inMemorySegmentIndex < fdReadWrite.allBytesBySegmentIndexForOneChunkFd.length; inMemorySegmentIndex++) {
                var segmentBytes = fdReadWrite.allBytesBySegmentIndexForOneChunkFd[inMemorySegmentIndex];
                if (segmentBytes != null) {
                    os.writeInt(inMemorySegmentIndex);
                    os.writeInt(segmentBytes.length);
                    os.write(segmentBytes);
                }
            }
        }
    }

    byte[] preadForMerge(int beginSegmentIndex, int segmentCount) {
        if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
            throw new IllegalArgumentException("Merge read segment count too large=" + segmentCount);
        }

        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentsForMerge(segmentIndexTargetFd, segmentCount);
    }

    byte[] preadForRepl(int beginSegmentIndex) {
        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readBatchForRepl(segmentIndexTargetFd);
    }

    byte[] preadOneSegment(int targetSegmentIndex) {
        return preadOneSegment(targetSegmentIndex, true);
    }

    byte[] preadOneSegment(int targetSegmentIndex, boolean isRefreshLRUCache) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInner(segmentIndexTargetFd, isRefreshLRUCache);
    }

    void clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
    }

    // begin with 0
    private int segmentIndex = 0;

    public int getSegmentIndex() {
        return segmentIndex;
    }

    public void setSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    @SlaveNeedReplay
    @SlaveReplay
    void resetAsFlush() {
        segmentIndex = 0;
    }

    @VisibleForTesting
    int targetFdIndex() {
        return segmentIndex / segmentNumberPerFd;
    }

    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / segmentNumberPerFd;
    }

    @VisibleForTesting
    int targetSegmentIndexTargetFd() {
        return segmentIndex % segmentNumberPerFd;
    }

    @VisibleForTesting
    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % segmentNumberPerFd;
    }

    public enum Flag {
        init((byte) 100),
        new_write((byte) 0),
        reuse_new((byte) 10),
        merged_and_persisted((byte) -10);

        final byte flagByte;

        public byte flagByte() {
            return flagByte;
        }

        Flag(byte flagByte) {
            this.flagByte = flagByte;
        }

        static boolean canReuse(byte flagByteTarget) {
            return flagByteTarget == init.flagByte || flagByteTarget == merged_and_persisted.flagByte;
        }

        @Override
        public String toString() {
            return name() + "(" + flagByte + ")";
        }
    }

    public record SegmentFlag(byte flagByte, long segmentSeq, int walGroupIndex) {
        @Override
        public String toString() {
            return "SegmentFlag{" +
                    "flagByte=" + flagByte +
                    ", segmentSeq=" + segmentSeq +
                    ", walGroupIndex=" + walGroupIndex +
                    '}';
        }
    }

    int maxOncePersistSegmentSize;

    @SlaveNeedReplay
    // return need merge segment index array
    public void persist(int walGroupIndex,
                        @NotNull ArrayList<Wal.V> list,
                        @NotNull XOneWalGroupPersist xForBinlog,
                        @Nullable KeyBucketsInOneWalGroup keyBucketsInOneWalGroupGiven) {
        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = ConfForSlot.global.confChunk.isSegmentUseCompression ?
                segmentBatch.split(list, pvmList) : segmentBatch2.split(list, pvmList);
        maxOncePersistSegmentSize = Math.max(maxOncePersistSegmentSize, segments.size());

        var ii = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(segmentIndex, segments.size());
        if (ii == -1) {
            if (segmentIndex != 0) {
                // from beginning find again
                ii = metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(0, segments.size());
            }
        }
        if (ii == -1) {
            throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
        }
        segmentIndex = ii;

        // reset segment index after find those segments can reuse
        var currentSegmentIndex = this.segmentIndex;
        for (var pvm : pvmList) {
            pvm.segmentIndex += currentSegmentIndex;
        }

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }

        boolean isNewAppendAfterBatch;

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        // never cross fd files because prepare batch segments to write
        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForGlobal.pureMemory) {
            isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                int targetSegmentIndex = segment.tmpSegmentIndex() + currentSegmentIndex;
                var bytes = segment.segmentBytes();
                fdReadWrite.writeOneInner(targetSegmentIndexTargetFd(targetSegmentIndex), bytes, false);

                xForBinlog.putUpdatedChunkSegmentBytes(targetSegmentIndex, bytes);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(targetSegmentIndex,
                        isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
            }

            oneSlot.setSegmentMergeFlagBatch(currentSegmentIndex, segments.size(),
                    isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segmentSeqListAll, walGroupIndex);

            moveSegmentIndexNext(segments.size());
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    int targetSegmentIndex = segment.tmpSegmentIndex() + currentSegmentIndex;
                    var bytes = segment.segmentBytes();
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);

                    xForBinlog.putUpdatedChunkSegmentBytes(targetSegmentIndex, bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(targetSegmentIndex,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
                }
            } else {
                // batch write, perf better ? need test
                var batchCount = segments.size() / BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                var remainCount = segments.size() % BATCH_ONCE_SEGMENT_COUNT_PWRITE;

                var tmpBatchBytes = new byte[chunkSegmentLength * BATCH_ONCE_SEGMENT_COUNT_PWRITE];
                var buffer = ByteBuffer.wrap(tmpBatchBytes);

                for (int i = 0; i < batchCount; i++) {
                    buffer.clear();
                    if (i > 0) {
                        Arrays.fill(tmpBatchBytes, (byte) 0);
                    }

                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_PWRITE; j++) {
                        var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_PWRITE + j);
                        var bytes = segment.segmentBytes();
                        buffer.put(bytes);

                        // padding to segment length
                        if (bytes.length < chunkSegmentLength) {
                            buffer.position(buffer.position() + chunkSegmentLength - bytes.length);
                        }

                        segmentSeqListSubBatch.add(segment.segmentSeq());

                        xForBinlog.putUpdatedChunkSegmentBytes(segment.tmpSegmentIndex() + currentSegmentIndex, bytes);
                    }

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segmentSeqListSubBatch, walGroupIndex);

                    for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_PWRITE; j++) {
                        var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_PWRITE + j);

                        xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.tmpSegmentIndex() + currentSegmentIndex,
                                isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
                    }

                    moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                }

                for (int i = 0; i < remainCount; i++) {
                    var leftSegment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_PWRITE + i);
                    var bytes = leftSegment.segmentBytes();
                    int targetSegmentIndex = leftSegment.tmpSegmentIndex() + currentSegmentIndex;
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    xForBinlog.putUpdatedChunkSegmentBytes(targetSegmentIndex, bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(targetSegmentIndex,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, leftSegment.segmentSeq());

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, leftSegment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
                }
            }
        }

        // stats
        persistCallCountTotal++;
        persistCvCountTotal += list.size();

        if (calcSegmentCountStepWhenOverHalfEstimateKeyNumber == 0) {
            if ((double) persistCvCountTotal / ConfForGlobal.estimateKeyNumber > 0.5) {
                calcSegmentCountStepWhenOverHalfEstimateKeyNumber = segmentIndex;
                log.warn("!!!Over half estimate key number, calc segment count step when over half estimate key number, slot={}, " +
                                "segment count step={}, estimate key number={}",
                        slot, calcSegmentCountStepWhenOverHalfEstimateKeyNumber, ConfForGlobal.estimateKeyNumber);
            }
        }

        var beginT = System.nanoTime();
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, xForBinlog, keyBucketsInOneWalGroupGiven);
        var costT = (System.nanoTime() - beginT) / 1000;
        updatePvmBatchCostTimeTotalUs += costT;

        metaChunkSegmentFlagSeq.addSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segments.size());

        // update meta, segment index for next time
        oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
        xForBinlog.setChunkSegmentIndexAfterPersist(segmentIndex);
    }

    @VisibleForTesting
    void moveSegmentIndexNext(int segmentCount) {
        if (segmentCount > 1) {
            var newSegmentIndex = segmentIndex + segmentCount;
            if (newSegmentIndex == maxSegmentIndex) {
                newSegmentIndex = 0;
            } else if (newSegmentIndex > maxSegmentIndex) {
                // already skip fd last segments for prepare pwrite batch, never reach here
                throw new SegmentOverflowException("Segment index overflow, s=" + slot + ", i=" + segmentIndex +
                        ", c=" + segmentCount + ", max=" + maxSegmentIndex);
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

    private boolean isAllZero(byte[] bytes) {
        boolean isAllZero = true;
        for (byte aByte : bytes) {
            if (aByte != 0) {
                isAllZero = false;
                break;
            }
        }
        return isAllZero;
    }

    @SlaveReplay
    public void writeSegmentsFromMasterExistsOrAfterSegmentSlim(byte[] bytes, int segmentIndex, int segmentCount) {
        if (ConfForGlobal.pureMemory) {
            var fdIndex = targetFdIndex(segmentIndex);
            var segmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

            var fdReadWrite = fdReadWriteArray[fdIndex];
            if (segmentCount == 1) {
                if (isAllZero(bytes)) {
                    log.warn("Repl chunk segment bytes from master all 0, segment index={}, slot={}", segmentIndex, slot);
                    fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
                } else {
                    fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
                }
            } else {
                var allZeroSegmentCount = 0;
                for (int i = 0; i < segmentCount; i++) {
                    var oneSegmentBytes = new byte[chunkSegmentLength];
                    System.arraycopy(bytes, i * chunkSegmentLength, oneSegmentBytes, 0, chunkSegmentLength);

                    if (isAllZero(oneSegmentBytes)) {
                        allZeroSegmentCount++;
                        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd + i);
                    } else {
                        fdReadWrite.writeOneInner(segmentIndexTargetFd + i, oneSegmentBytes, false);
                    }
                }
                log.warn("Repl chunk segment bytes from master all 0, segment count={}, slot={}", allZeroSegmentCount, slot);
            }
        } else {
            this.segmentIndex = segmentIndex;
            writeSegmentsForRepl(bytes, segmentCount);
        }
    }

    @SlaveReplay
    public boolean writeSegmentToTargetSegmentIndex(byte[] bytes, int targetSegmentIndex) {
        this.segmentIndex = targetSegmentIndex;
        return writeSegments(bytes, 1);
    }

    @SlaveNeedReplay
    private boolean writeSegments(byte[] bytes, int segmentCount) {
//        if (segmentCount != 1 && segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
//            throw new IllegalArgumentException("Write segment count not support=" + segmentCount);
//        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
        } else {
            fdReadWrite.writeSegmentsBatch(segmentIndexTargetFd, bytes, false);
        }

        boolean isNewAppend = false;
        int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
            isNewAppend = true;
        }

        return isNewAppend;
    }

    @SlaveReplay
    private void writeSegmentsForRepl(byte[] bytes, int segmentCount) {
        if (segmentCount > REPL_ONCE_SEGMENT_COUNT_PREAD) {
            throw new IllegalArgumentException("Write segment count not support=" + segmentCount);
        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.writeSegmentsBatchForRepl(segmentIndexTargetFd, bytes);

        int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
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

        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    map.putAll(fdReadWrite.collect());
                }
            }
        }

        return map;
    }
}
