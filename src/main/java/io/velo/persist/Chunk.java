package io.velo.persist;

import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import jnr.posix.LibC;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE;
import static io.velo.persist.FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;

public class Chunk implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp {
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    // seq long + cv number int + crc int
    public static final int SEGMENT_HEADER_LENGTH = 8 + 4 + 4;

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
    long persistCountTotal;
    @VisibleForTesting
    long persistCvCountTotal;
    @VisibleForTesting
    long updatePvmBatchCostTimeTotalUs;
    @VisibleForTesting
    long segmentDecompressTimeTotalUs = 0;
    @VisibleForTesting
    long segmentDecompressCountTotal = 0;

    @VisibleForTesting
    final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    final SegmentBatch segmentBatch;
    private final SegmentBatch2 segmentBatch2;

    @VisibleForTesting
    int[] fdLengths;
    @VisibleForTesting
    FdReadWrite[] fdReadWriteArray;

    public Chunk(short slot, File slotDir, OneSlot oneSlot,
                 SnowFlake snowFlake, KeyLoader keyLoader) {
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
        this.keyLoader = keyLoader;
        this.segmentBatch = new SegmentBatch(slot, snowFlake);
        this.segmentBatch2 = new SegmentBatch2(slot, snowFlake);
    }

    @Override
    public long estimate(StringBuilder sb) {
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

    byte[] preadForMerge(int targetSegmentIndex, int segmentCount) {
        if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
            throw new IllegalArgumentException("Merge read segment count too large=" + segmentCount);
        }

        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentsForMerge(segmentIndexTargetFd, segmentCount);
    }

    byte[] preadForRepl(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readBatchForRepl(segmentIndexTargetFd);
    }

    byte[] preadOneSegment(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInner(segmentIndexTargetFd, true);
    }

    void clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
    }

    // begin with 0
    // -1 means not init
    private int segmentIndex = -1;

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
        mergedSegmentIndexEndLastTime = NO_NEED_MERGE_SEGMENT_INDEX;
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

    public boolean initSegmentIndexWhenFirstStart(int segmentIndex) {
        log.info("Chunk init s={}, i={}", slot, segmentIndex);
        if (segmentIndex > maxSegmentIndex) {
            segmentIndex = 0;
        }
        this.segmentIndex = segmentIndex;
        return reuseSegments(true, 1, true);
    }

    @VisibleForTesting
    boolean reuseSegments(boolean isFirstStart, int segmentCount, boolean updateAsReuseFlag) {
        // skip can not reuse segments
        if (!isFirstStart && segmentIndex == 0) {
            var segmentFlagList = oneSlot.getSegmentMergeFlagBatch(segmentIndex, segmentCount);
            int skipN = 0;
            for (int i = 0; i < segmentFlagList.size(); i++) {
                var segmentFlag = segmentFlagList.get(i);
                var flagByte = segmentFlag.flagByte();
                if (!Chunk.Flag.canReuse(flagByte)) {
                    skipN = (i + 1);
                }
            }

            // begin with new segment index
            if (skipN != 0) {
                segmentIndex += skipN;
                return reuseSegments(false, segmentCount, updateAsReuseFlag);
            }
        }

        for (int i = 0; i < segmentCount; i++) {
            var targetSegmentIndex = segmentIndex + i;

            var segmentFlag = oneSlot.getSegmentMergeFlag(targetSegmentIndex);
            var flagByte = segmentFlag.flagByte();

            // already set flag to reuse, can reuse
            if (flagByte == Flag.reuse.flagByte) {
                continue;
            }

            // init can reuse
            if (flagByte == Flag.init.flagByte) {
                if (updateAsReuseFlag) {
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex, Flag.reuse.flagByte, 0L, segmentFlag.walGroupIndex);
                }
                continue;
            }

            // merged and persisted, can reuse
            if (flagByte == Flag.merged_and_persisted.flagByte) {
                if (updateAsReuseFlag) {
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex, Flag.reuse.flagByte, 0L, segmentFlag.walGroupIndex);
                }
                continue;
            }

            // left can not reuse: new_write, reuse_new, merging, merged
            log.warn("Chunk segment index is not init/merged and persisted/reuse, can not write, s={}, i={}, flag={}",
                    slot, targetSegmentIndex, flagByte);
            return false;
        }
        return true;
    }

    public enum Flag {
        init((byte) 100),
        new_write((byte) 0),
        reuse_new((byte) 10),
        merging((byte) 1),
        merged((byte) -1),
        merged_and_persisted((byte) -10),
        reuse((byte) -100);

        final byte flagByte;

        public byte flagByte() {
            return flagByte;
        }

        Flag(byte flagByte) {
            this.flagByte = flagByte;
        }

        static boolean canReuse(byte flagByteTarget) {
            return flagByteTarget == init.flagByte || flagByteTarget == reuse.flagByte || flagByteTarget == merged_and_persisted.flagByte;
        }

        static boolean isMergingOrMerged(byte flagByteTarget) {
            return flagByteTarget == merging.flagByte || flagByteTarget == merged.flagByte;
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

    // need refer Wal valueSizeTrigger
    // 32 not tight segments is enough for 400 Wal.V persist ?, need check, todo
    // one wal group charges 32 key buckets, 32 * 4KB = 128KB
    // with merged valid cv list together, once most pre-read 2 segments, valid cv list size ~= 400
    public static int ONCE_PREPARE_SEGMENT_COUNT = 32;

    private int mergedSegmentIndexEndLastTime = NO_NEED_MERGE_SEGMENT_INDEX;

    public int getMergedSegmentIndexEndLastTime() {
        return mergedSegmentIndexEndLastTime;
    }

    public void setMergedSegmentIndexEndLastTime(int mergedSegmentIndexEndLastTime) {
        this.mergedSegmentIndexEndLastTime = mergedSegmentIndexEndLastTime;
    }

    @SlaveNeedReplay
    public void setMergedSegmentIndexEndLastTimeAfterSlaveCatchUp(int mergedSegmentIndexEndLastTime) {
        this.mergedSegmentIndexEndLastTime = mergedSegmentIndexEndLastTime;
    }

    // for find bug
    void checkMergedSegmentIndexEndLastTimeValidAfterServerStart() {
        if (mergedSegmentIndexEndLastTime != NO_NEED_MERGE_SEGMENT_INDEX) {
            if (mergedSegmentIndexEndLastTime < 0 || mergedSegmentIndexEndLastTime >= maxSegmentIndex) {
                throw new IllegalStateException("Merged segment index end last time out of bound, s=" + slot + ", i=" + mergedSegmentIndexEndLastTime);
            }
        }
    }

    // merge, valid cv list is smaller, only need one segment, or need 4 or 8 ? todo
    // but even list size is smaller, one v maybe is not compressed, one v encoded length is large
    public static int ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE = 16;

    @SlaveNeedReplay
    // return need merge segment index array
    public ArrayList<Integer> persist(int walGroupIndex, ArrayList<Wal.V> list, boolean isMerge, XOneWalGroupPersist xForBinlog,
                                      KeyBucketsInOneWalGroup keyBucketsInOneWalGroupGiven) {
        logMergeCount++;
        var doLog = (Debug.getInstance().logMerge && logMergeCount % 1000 == 0);

        moveSegmentIndexForPrepare();
        boolean canWrite;
        if (isMerge) {
            canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE, false);
        } else {
            canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
        }
        if (!canWrite) {
            throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
        }

        var oncePrepareSegmentCount = isMerge ? ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE : ONCE_PREPARE_SEGMENT_COUNT;
        int[] nextNSegmentIndex = new int[oncePrepareSegmentCount];
        for (int i = 0; i < oncePrepareSegmentCount; i++) {
            nextNSegmentIndex[i] = segmentIndex + i;
        }

        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = ConfForSlot.global.confChunk.isSegmentUseCompression ?
                segmentBatch.split(list, nextNSegmentIndex, pvmList) : segmentBatch2.split(list, nextNSegmentIndex, pvmList);

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }
        oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                Flag.reuse.flagByte, segmentSeqListAll, walGroupIndex);

        boolean isNewAppendAfterBatch = true;

        var firstSegmentIndex = segments.getFirst().segmentIndex();
        var fdIndex = targetFdIndex(firstSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(firstSegmentIndex);

        // never cross fd files because prepare batch segments to write
        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForGlobal.pureMemory) {
            isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                var bytes = segment.segmentBytes();
                fdReadWrite.writeOneInner(targetSegmentIndexTargetFd(segment.segmentIndex()), bytes, false);

                xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                        isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
            }

            oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                    isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segmentSeqListAll, walGroupIndex);

            moveSegmentIndexNext(segments.size());
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    var bytes = segment.segmentBytes();
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);

                    xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
                }
            } else {
                // batch write
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

                        xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    }

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segmentSeqListSubBatch, walGroupIndex);

                    for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_PWRITE; j++) {
                        var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_PWRITE + j);

                        xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                                isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
                    }

                    moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                }

                for (int i = 0; i < remainCount; i++) {
                    var segment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_PWRITE + i);
                    var bytes = segment.segmentBytes();
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
                }
            }
        }

        // stats
        persistCountTotal++;
        persistCvCountTotal += list.size();

        var beginT = System.nanoTime();
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, xForBinlog, keyBucketsInOneWalGroupGiven);
        var costT = (System.nanoTime() - beginT) / 1000;
        updatePvmBatchCostTimeTotalUs += costT;

        // update meta, segment index for next time
        oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
        xForBinlog.setChunkSegmentIndexAfterPersist(segmentIndex);

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        if (isMerge) {
            // return empty list
            return needMergeSegmentIndexList;
        }

        for (var segment : segments) {
            var toMergeSegmentIndex = needMergeSegmentIndex(isNewAppendAfterBatch, segment.segmentIndex());
            if (toMergeSegmentIndex != NO_NEED_MERGE_SEGMENT_INDEX) {
                needMergeSegmentIndexList.add(toMergeSegmentIndex);
            }
        }

        if (needMergeSegmentIndexList.isEmpty()) {
            return needMergeSegmentIndexList;
        }

        needMergeSegmentIndexList.sort(Integer::compareTo);
        updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList);
        xForBinlog.setChunkMergedSegmentIndexEndLastTime(mergedSegmentIndexEndLastTime);

        xForBinlog.setLastSegmentSeq(segmentSeqListAll.getLast());

        if (doLog) {
            log.info("Chunk persist need merge segment index list, s={}, i={}, list={}", slot, segmentIndex, needMergeSegmentIndexList);
        }
        return needMergeSegmentIndexList;
    }

    @VisibleForTesting
    void updateLastMergedSegmentIndexEnd(ArrayList<Integer> needMergeSegmentIndexList) {
        TreeSet<Integer> sorted = new TreeSet<>(needMergeSegmentIndexList);

        // recycle, need spit to two part
        if (sorted.getLast() - sorted.getFirst() > halfSegmentNumber) {
            if (!sorted.contains(0)) {
                throw new IllegalStateException("Need merge segment index list not contains 0 while reuse from the beginning, s="
                        + slot + ", list=" + sorted);
            }
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                throw new IllegalStateException("Merged segment index end last time not set, s=" + slot);
            }

            TreeSet<Integer> onePart = new TreeSet<>();
            TreeSet<Integer> anotherPart = new TreeSet<>();
            for (var segmentIndex : sorted) {
                if (segmentIndex < oneSlot.chunk.halfSegmentNumber) {
                    onePart.add(segmentIndex);
                } else {
                    anotherPart.add(segmentIndex);
                }
            }
            log.warn("Recycle merge chunk, s={}, one part need merge segment index list ={}, another part need merge segment index list={}",
                    slot, onePart, anotherPart);

            // prepend from merged segment index end last time
            var firstNeedMergeSegmentIndex = anotherPart.getFirst();

            // mergedSegmentIndexEndLastTime maybe > firstNeedMergeSegmentIndex when server restart, because pre-read merge before persist wal
//            assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
            for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                anotherPart.add(i);
            }

            checkNeedMergeSegmentIndexListContinuous(onePart);
            checkNeedMergeSegmentIndexListContinuous(anotherPart);

            mergedSegmentIndexEndLastTime = onePart.getLast();
        } else {
            var firstNeedMergeSegmentIndex = sorted.getFirst();
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                if (firstNeedMergeSegmentIndex != 0) {
                    throw new IllegalStateException("First need merge segment index not 0, s=" + slot + ", i=" + firstNeedMergeSegmentIndex);
                }
            } else {
                // prepend from merged segment index end last time
//                assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
                for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                    sorted.add(i);
                }

                // last ONCE_PREPARE_SEGMENT_COUNT segments, need merge
                var lastNeedMergeSegmentIndex = sorted.getLast();
                if (lastNeedMergeSegmentIndex >= maxSegmentIndex - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i <= maxSegmentIndex; i++) {
                        sorted.add(i);
                    }
                    log.warn("Add extra need merge segment index to the end, s={}, i={}, list={}", slot, segmentIndex, sorted);
                } else if (lastNeedMergeSegmentIndex < halfSegmentNumber &&
                        lastNeedMergeSegmentIndex >= halfSegmentNumber - 1 - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i < halfSegmentNumber; i++) {
                        sorted.add(i);
                    }
                    log.warn("Add extra need merge segment index to the half end, s={}, i={}, list={}", slot, segmentIndex, sorted);
                }
            }

            checkNeedMergeSegmentIndexListContinuous(sorted);
            mergedSegmentIndexEndLastTime = sorted.getLast();
        }
    }

    @VisibleForTesting
    void checkNeedMergeSegmentIndexListContinuous(TreeSet<Integer> sortedSet) {
        if (sortedSet.size() == 1) {
            return;
        }

        final int maxSize = ONCE_PREPARE_SEGMENT_COUNT * 4;

        if (sortedSet.getLast() - sortedSet.getFirst() != sortedSet.size() - 1) {
            throw new IllegalStateException("Need merge segment index not continuous, s=" + slot +
                    ", first need merge segment index=" + sortedSet.getFirst() +
                    ", last need merge segment index=" + sortedSet.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + sortedSet.size());
        }

        if (sortedSet.size() > maxSize) {
            throw new IllegalStateException("Need merge segment index list size too large, s=" + slot +
                    ", first need merge segment index=" + sortedSet.getFirst() +
                    ", last need merge segment index=" + sortedSet.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + sortedSet.size() +
                    ", max size=" + maxSize);
        }

        if (sortedSet.size() >= ONCE_PREPARE_SEGMENT_COUNT) {
            log.debug("Chunk persist need merge segment index list too large, performance bad, maybe many is skipped, s={}, i={}, list={}",
                    slot, segmentIndex, sortedSet);
        }
    }

    private long logMergeCount = 0;

    @VisibleForTesting
    void moveSegmentIndexForPrepare() {
        int leftSegmentCountThisFd = segmentNumberPerFd - segmentIndex % segmentNumberPerFd;
        if (leftSegmentCountThisFd < ONCE_PREPARE_SEGMENT_COUNT) {
            // begin with next fd
            segmentIndex += leftSegmentCountThisFd;
        }
        if (segmentIndex >= maxSegmentIndex) {
            segmentIndex = 0;
        }
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

    public static final int NO_NEED_MERGE_SEGMENT_INDEX = -1;

    int needMergeSegmentIndex(boolean isNewAppend, int targetIndex) {
        int segmentIndexToMerge = NO_NEED_MERGE_SEGMENT_INDEX;
        if (targetIndex >= halfSegmentNumber) {
            // begins with 0
            // ends with 2^18 - 1
            segmentIndexToMerge = targetIndex - halfSegmentNumber;
        } else {
            if (!isNewAppend) {
                segmentIndexToMerge = targetIndex + halfSegmentNumber;
            }
        }
        return segmentIndexToMerge;
    }

    @SlaveReplay
    public void writeSegmentsFromMasterExists(byte[] bytes, int segmentIndex, int segmentCount) {
        if (ConfForGlobal.pureMemory) {
            var fdIndex = targetFdIndex(segmentIndex);
            var segmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

            var fdReadWrite = fdReadWriteArray[fdIndex];
            if (segmentCount == 1) {
                fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    var oneSegmentBytes = new byte[chunkSegmentLength];
                    System.arraycopy(bytes, i * chunkSegmentLength, oneSegmentBytes, 0, chunkSegmentLength);

                    fdReadWrite.writeOneInner(segmentIndexTargetFd + i, oneSegmentBytes, false);
                }
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
        map.put("chunk_merged_segment_index_end_last_time", (double) mergedSegmentIndexEndLastTime);
        map.put("chunk_max_segment_index", (double) maxSegmentIndex);

        if (persistCountTotal > 0) {
            map.put("chunk_persist_count_total", (double) persistCountTotal);
            map.put("chunk_persist_cv_count_total", (double) persistCvCountTotal);
            map.put("chunk_persist_cv_count_avg", (double) persistCvCountTotal / persistCountTotal);

            map.put("chunk_update_pvm_batch_cost_time_total_us", (double) updatePvmBatchCostTimeTotalUs);
            map.put("chunk_update_pvm_batch_cost_time_avg_us", (double) updatePvmBatchCostTimeTotalUs / persistCountTotal);
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
