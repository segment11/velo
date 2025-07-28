package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.NullableOnlyTest;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
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

/**
 * Represents a chunk, which is a collection of segments that store data.
 * Each chunk can have multiple file descriptors (FDs) to distribute data across.
 */
public class Chunk implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, CanSaveAndLoad {
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    /**
     * Get the maximum segment index within this chunk.
     *
     * @return the maximum segment index
     */
    public int getMaxSegmentIndex() {
        return maxSegmentIndex;
    }

    /**
     * Enumerates types of segments within a chunk.
     */
    enum SegmentType {
        NORMAL((byte) 0),
        TIGHT((byte) 1),
        SLIM((byte) 2),
        SLIM_AND_COMPRESSED((byte) 3);

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

    /**
     * Get a string representation of this chunk, including its slot, segment count, FD count, etc.
     *
     * @return a string representation of this chunk
     */
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

    int[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    /**
     * Get the array of file descriptor read-write objects.
     *
     * @return the array of FdReadWrite objects
     */
    public FdReadWrite[] getFdReadWriteArray() {
        return fdReadWriteArray;
    }

    /**
     * Constructs a new chunk with the given slot, directory, and slot context.
     *
     * @param slot    the slot number this chunk belongs to
     * @param slotDir the directory where the chunk data is stored
     * @param oneSlot the context for the slot, used for various data operations.
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

    /**
     * Estimate the memory usage of this chunk and append the details to the provided StringBuilder.
     *
     * @param sb the StringBuilder to append the memory usage details to
     * @return the estimated memory usage in bytes
     */
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

    /**
     * Initialize file descriptors for this chunk.
     *
     * @throws IOException if an I/O error occurs
     */
    @VisibleForTesting
    void initFds() throws IOException {
        this.fdLengths = new int[fdPerChunk];
        this.fdReadWriteArray = new FdReadWrite[fdPerChunk];
        for (int i = 0; i < fdPerChunk; i++) {
            // Prometheus metric labels use _ instead of -
            var name = "chunk_data_index_" + i + "_slot_" + slot;
            var file = new File(slotDir, "chunk-data-" + i);
            fdLengths[i] = (int) file.length();

            var fdReadWrite = new FdReadWrite(name, file);
            fdReadWrite.initByteBuffers(true, i);

            this.fdReadWriteArray[i] = fdReadWrite;
        }
    }

    /**
     * Clean up resources used by this chunk.
     */
    @Override
    public void cleanUp() {
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                fdReadWrite.cleanUp();
            }
        }
    }

    /**
     * Clear the segment bytes in memory for the given target segment index.
     *
     * @param targetSegmentIndex the segment index whose bytes should be cleared
     */
    public void clearSegmentBytesWhenPureMemory(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
    }

    /**
     * Truncate the chunks' file descriptors from the given segment index.
     *
     * @param targetSegmentIndex the segment index from which to truncate
     */
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

    /**
     * Load the chunk's data from the last saved file into memory, assuming the data is in pure memory mode.
     *
     * @param is the DataInputStream from which to read the data
     * @throws IOException if an I/O error occurs
     */
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

    /**
     * Write the chunk's data to the saved file from memory, assuming the data is in pure memory mode.
     *
     * @param os the DataOutputStream to which to write the data
     * @throws IOException if an I/O error occurs
     */
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

    /**
     * Read bytes from the chunk for a merge operation.
     *
     * @param beginSegmentIndex the starting segment index for the read
     * @param segmentCount      the number of segments to read
     * @return a byte array containing the read segment data
     * @throws IllegalArgumentException if the requested segment count is too large
     */
    byte[] preadForMerge(int beginSegmentIndex, int segmentCount) {
        if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
            throw new IllegalArgumentException("Merge read segment count too large=" + segmentCount);
        }

        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentsForMerge(segmentIndexTargetFd, segmentCount);
    }

    /**
     * Read bytes from the chunk for a replication operation.
     *
     * @param beginSegmentIndex the starting segment index for the read
     * @return a byte array containing the read segment data
     */
    byte[] preadForRepl(int beginSegmentIndex) {
        var fdIndex = targetFdIndex(beginSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(beginSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readBatchForRepl(segmentIndexTargetFd);
    }

    /**
     * Get the real length of a segment in the chunk.
     * When pure memory mode, one segment bytes may change during gc.
     *
     * @param targetSegmentIndex the segment index
     * @return the real length of the segment
     */
    public int getSegmentRealLength(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        var segmentBytes = fdReadWrite.allBytesBySegmentIndexForOneChunkFd[segmentIndexTargetFd];
        return segmentBytes == null ? 0 : segmentBytes.length;
    }

    /**
     * Read a single segment from the chunk.
     *
     * @param targetSegmentIndex the segment index to read
     * @return a byte array containing the read segment data
     */
    byte[] preadOneSegment(int targetSegmentIndex) {
        return preadOneSegment(targetSegmentIndex, true);
    }

    /**
     * Read a single segment from the chunk, with an option to refresh the LRU cache.
     *
     * @param targetSegmentIndex the segment index to read
     * @param isRefreshLRUCache  whether to refresh the LRU cache
     * @return a byte array containing the read segment data
     */
    byte[] preadOneSegment(int targetSegmentIndex, boolean isRefreshLRUCache) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInner(segmentIndexTargetFd, isRefreshLRUCache);
    }

    /**
     * Clear data for a segment in memory mode after it has been merged and persisted.
     *
     * @param targetSegmentIndex the segment index to clear
     */
    void clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd);
    }

    // Begin with 0
    private int segmentIndex = 0;

    /**
     * Get the current segment index.
     *
     * @return the current segment index
     */
    public int getSegmentIndex() {
        return segmentIndex;
    }

    /**
     * Set the current segment index.
     *
     * @param segmentIndex the new segment index
     */
    public void setSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    /**
     * Reset the chunk as if it was just flushed.
     */
    @SlaveNeedReplay
    @SlaveReplay
    void resetAsFlush() {
        segmentIndex = 0;
    }

    /**
     * Calculate the file descriptor index for the current segment index.
     *
     * @return the file descriptor index for the current segment index
     */
    @VisibleForTesting
    int targetFdIndex() {
        return segmentIndex / segmentNumberPerFd;
    }

    /**
     * Calculate the file descriptor index for a given target segment index.
     *
     * @param targetSegmentIndex the target segment index
     * @return the file descriptor index for the target segment index
     */
    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / segmentNumberPerFd;
    }

    /**
     * Calculate the segment index within the current file descriptor for the current segment index.
     *
     * @return the segment index within the current file descriptor
     */
    @VisibleForTesting
    int targetSegmentIndexTargetFd() {
        return segmentIndex % segmentNumberPerFd;
    }

    /**
     * Calculate the segment index within the current file descriptor for a given target segment index.
     *
     * @param targetSegmentIndex the target segment index
     * @return the segment index within the current file descriptor
     */
    @VisibleForTesting
    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % segmentNumberPerFd;
    }

    /**
     * Enumerates flags used to mark segment statuses.
     */
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

        /**
         * Determine if a flag byte indicates that a segment can be reused.
         *
         * @param flagByteTarget the flag byte to check
         * @return true if the flag indicates the segment can be reused, false otherwise
         */
        static boolean canReuse(byte flagByteTarget) {
            return flagByteTarget == init.flagByte || flagByteTarget == merged_and_persisted.flagByte;
        }

        /**
         * Get a string representation of this flag, including its byte value.
         *
         * @return a string representation of this flag
         */
        @Override
        public String toString() {
            return name() + "(" + flagByte + ")";
        }
    }

    /**
     * Records a segment's flag, sequence, and wal group index.
     */
    public record SegmentFlag(byte flagByte, long segmentSeq, int walGroupIndex) {
        /**
         * Get a string representation of this segment flag.
         *
         * @return a string representation of this segment flag
         */
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
     * Persist data from a WAL group into this chunk.
     *
     * @param walGroupIndex                the index of the WAL group being persisted
     * @param list                         the data to persist, in the form of a list of WAL values
     * @param xForBinlog                   used for logging purposes during persistence
     * @param keyBucketsInOneWalGroupGiven an optional parameter for details about key buckets in the WAL group
     */
    @SlaveNeedReplay
    // return need merge segment index array
    public void persist(int walGroupIndex,
                        @NotNull ArrayList<Wal.V> list,
                        @NotNull XOneWalGroupPersist xForBinlog,
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

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        // never cross fd files because prepare batch segments to write
        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForGlobal.pureMemory) {
            var isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                int targetSegmentIndex = segment.tmpSegmentIndex() + currentSegmentIndex;
                var bytes = segment.segmentBytes();
                fdReadWrite.writeOneInner(targetSegmentIndexTargetFd(targetSegmentIndex), bytes, false);

                if (ConfForGlobal.pureMemoryV2) {
                    keyLoader.metaChunkSegmentFillRatio.set(targetSegmentIndex, segment.valueBytesLength());
                }

                xForBinlog.putUpdatedChunkSegmentBytes(targetSegmentIndex, bytes);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(targetSegmentIndex,
                        isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segment.segmentSeq());
            }

            oneSlot.setSegmentMergeFlagBatch(currentSegmentIndex, segmentCount,
                    isNewAppendAfterBatch ? Flag.new_write.flagByte : Flag.reuse_new.flagByte, segmentSeqListAll, walGroupIndex);

            moveSegmentIndexNext(segmentCount);
        } else {
            if (segmentCount < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    int targetSegmentIndex = segment.tmpSegmentIndex() + currentSegmentIndex;
                    var bytes = segment.segmentBytes();
                    boolean isNewAppend = writeSegments(bytes, 1);

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
                var batchCount = segmentCount / BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                var remainCount = segmentCount % BATCH_ONCE_SEGMENT_COUNT_PWRITE;

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
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, xForBinlog, keyBucketsInOneWalGroupGiven);
        var costT = (System.nanoTime() - beginT) / 1000;
        updatePvmBatchCostTimeTotalUs += costT;

        metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, currentSegmentIndex, segmentCount);

        // update meta, segment index for next time
        oneSlot.setMetaChunkSegmentIndexInt(segmentIndex);
        xForBinlog.setChunkSegmentIndexAfterPersist(segmentIndex);
        xForBinlog.setToFindForMergeGroupByWalGroup(new XOneWalGroupPersist.ToFindForMergeGroupByWalGroup(walGroupIndex, currentSegmentIndex, segmentCount));
    }

    /**
     * Moves the segment index to the next position based on the given segment count.
     * <p>
     * This method updates the current segment index by adding the provided segment count. If the new index exceeds the maximum allowed index,
     * it wraps around to 0 or throws an exception if it exceeds the maximum segment index. It handles both single and multiple segment increments.
     *
     * @param segmentCount The number of segments to move forward.
     */
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

    /**
     * Finds the previous segment index while skipping half of the segment range.
     * <p>
     * This method calculates a new segment index to merge, considering whether it's a new append operation or not. It adjusts the index
     * based on the half-segment boundary.
     *
     * @param isNewAppend    Whether this is a new append operation.
     * @param bySegmentIndex The current segment index.
     * @return The adjusted segment index for merging.
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
     * Checks if all bytes in the provided byte array are zero.
     *
     * @param bytes The byte array to check.
     * @return {@code true} if all bytes are zero, {@code false} otherwise.
     */
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

    /**
     * Writes segments from the master node, handling both existing and new segments.
     * <p>
     * This method writes the provided bytes to the specified segment index. It supports writing multiple segments at once and handles
     * cases where the bytes are all zeros by clearing the target segment index in memory.
     *
     * @param bytes              The byte array containing the data to write.
     * @param segmentIndex       The starting segment index to write to.
     * @param segmentCount       The number of segments to write.
     * @param segmentRealLengths The real length of each segment.
     */
    @SlaveReplay
    public void writeSegmentsFromMasterExistsOrAfterSegmentSlim(byte[] bytes, int segmentIndex, int segmentCount, int[] segmentRealLengths) {
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
                    var segmentRealLength = segmentRealLengths[i];

                    var oneSegmentBytes = new byte[chunkSegmentLength];
                    System.arraycopy(bytes, i * chunkSegmentLength, oneSegmentBytes, 0, chunkSegmentLength);

                    if (isAllZero(oneSegmentBytes) || segmentRealLength == 0) {
                        allZeroSegmentCount++;
                        fdReadWrite.clearTargetSegmentIndexInMemory(segmentIndexTargetFd + i);
                    } else {
                        assert segmentRealLength <= chunkSegmentLength;
                        if (segmentRealLength != chunkSegmentLength) {
                            oneSegmentBytes = Arrays.copyOf(oneSegmentBytes, segmentRealLength);
                        }
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

    /**
     * Writes a single segment to a specific target segment index.
     * <p>
     * This method sets the current segment index to the target index and writes the provided bytes to that segment.
     *
     * @param bytes              The byte array containing the data to write.
     * @param targetSegmentIndex The target segment index to write to.
     * @return {@code true} if the write was successful, {@code false} otherwise.
     */
    @SlaveReplay
    public boolean writeSegmentToTargetSegmentIndex(byte[] bytes, int targetSegmentIndex) {
        this.segmentIndex = targetSegmentIndex;
        return writeSegments(bytes, 1);
    }

    /**
     * Writes segments to the chunk, handling both single and batch writes.
     * <p>
     * This method writes the provided bytes to the current segment index. It supports writing multiple segments at once and updates the
     * file descriptor lengths accordingly.
     *
     * @param bytes        The byte array containing the data to write.
     * @param segmentCount The number of segments to write.
     * @return {@code true} if this is a new append operation, {@code false} otherwise.
     */
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

    /**
     * Writes segments for replication, ensuring the segment count does not exceed the allowed limit.
     * <p>
     * This method writes the provided bytes to the current segment index for replication purposes. It ensures that the segment count does
     * not exceed the predefined limit for replication operations.
     *
     * @param bytes        The byte array containing the data to write.
     * @param segmentCount The number of segments to write.
     */
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

    /**
     * Collects metrics related to the chunk and returns them as a map.
     * <p>
     * This method gathers various metrics about the chunk, such as the current segment index, maximum segment index, and performance statistics.
     * It also includes metrics from associated components like segment batches and file descriptor read-write objects.
     *
     * @return A map containing the collected metrics.
     */
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

        return map;
    }
}
