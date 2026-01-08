package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.StaticMemoryPrepareBytesStats;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.task.ITask;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Manages the segment flags and sequence numbers for chunks within a specific slot.
 * This class provides in-memory caching and file storage for segment flags and sequence numbers,
 * depending on the configuration. It allows retrieval and update of these flags and sequences,
 * and also manages bit sets for segment reuse and file truncation optimization.
 */
public class MetaChunkSegmentFlagSeq implements InMemoryEstimate, NeedCleanUp, ITask {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    /**
     * flag byte + seq long + wal group index int
     */
    public static final int ONE_LENGTH = 1 + 8 + 4;

    public static final int INIT_WAL_GROUP_INDEX = -1;

    private final short slot;
    private final int fdPerChunk;
    private final int segmentNumberPerFd;
    private final int halfSegmentNumber;
    private final int maxSegmentNumber;

    // for find those segments can reuse
    private final BitSet[] segmentCanReuseBitSet;
    // for truncate file to save ssd space
    // at least chunk segment number is 8K, each 1K segment use one bit to indicate whether this group of segments is all merged
    // when the bit set is all 0, it means all segments in this file are all merged, so we can truncate file to save ssd space
    private final BitSet[] stepBy1KSegmentsGroupAllMergedFlagBitSet;
    private final int max1KSegmentsGroupNumber;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    /**
     * Retrieves the in-memory cached bytes representing the segment flags and sequence numbers.
     *
     * @return the in-memory cached bytes
     */
    byte[] getInMemoryCachedBytes() {
        // usually more than 10M, do not copy
        return inMemoryCachedBytes;
    }

    /**
     * Overwrites the in-memory cached bytes with the provided bytes.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the bytes to the file and updates the in-memory cache.
     *
     * @param bytes the bytes to overwrite the in-memory cache with
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     * @throws RuntimeException         If an I/O error occurs during file operations.
     */
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Meta chunk segment flag seq, bytes length not match");
        }

        inMemoryCachedByteBuffer.position(0).put(bytes);
        updateCanReuseBitSetWhenOverwrite();
    }

    /**
     * Retrieves a batch of segment flags and sequence numbers starting from a specific index.
     *
     * @param beginBucketIndex the starting index of the batch
     * @param bucketCount      the number of segments in the batch
     * @return the array of bytes representing the segment flags and sequence numbers for the batch
     */
    public byte[] getOneBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount * ONE_LENGTH];
        var offset = beginBucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.position(offset).get(dst);
        return dst;
    }

    /**
     * Overwrites a batch of segment flags and sequence numbers starting from a specific index.
     * If operating in pure memory mode, it directly updates the in-memory cache.
     * Otherwise, it writes the bytes to the file and updates the in-memory cache.
     *
     * @param bytes            the bytes to overwrite the batch with
     * @param beginBucketIndex the starting index of the batch
     * @param bucketCount      the number of segments in the batch
     * @throws IllegalArgumentException If the provided bytes array length does not match the expected length.
     * @throws RuntimeException         If an I/O error occurs during file operations.
     */
    public void overwriteOneBatch(byte[] bytes, int beginBucketIndex, int bucketCount) {
        if (bytes.length != bucketCount * ONE_LENGTH) {
            throw new IllegalArgumentException("Repl chunk segments from master one batch meta bytes length not match, length=" +
                    bytes.length + ", bucket count=" + bucketCount + ", one length=" + ONE_LENGTH + ", slot=" + slot);
        }

        var offset = beginBucketIndex * ONE_LENGTH;
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(offset).put(bytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(offset).put(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Repl chunk segments from master one batch meta bytes, write file error, slot=" + slot, e);
        }
        log.warn("Repl chunk segments from master one batch meta bytes, write file success, begin bucket index={}, bucket count={}, slot={}",
                beginBucketIndex, bucketCount, slot);
    }

    private static final Logger log = LoggerFactory.getLogger(MetaChunkSegmentFlagSeq.class);

    /**
     * Initializes the segment flags to the initial state.
     *
     * @param innerBytes the byte array to initialize
     */
    private void fillSegmentFlagInit(byte[] innerBytes) {
        var initBytes = new byte[ONE_LENGTH];
        var initBuffer = ByteBuffer.wrap(initBytes);
        initBuffer.put(Chunk.Flag.init.flagByte);
        initBuffer.putLong(0L);
        initBuffer.putInt(INIT_WAL_GROUP_INDEX);

        var innerBuffer = ByteBuffer.wrap(innerBytes);
        for (int i = 0; i < maxSegmentNumber; i++) {
            innerBuffer.put(initBytes);
        }
    }

    /**
     * Initializes the bit sets and marks segment indexes when starting for the first time or clearing.
     * Sets all segments as reusable and marks all 1K segments groups as not merged.
     */
    private void initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear() {
        for (int i = 0; i < fdPerChunk; i++) {
            // set all true
            var bitSet = segmentCanReuseBitSet[i];
            bitSet.set(0, segmentNumberPerFd, true);

            var stepBy1KBitSet = stepBy1KSegmentsGroupAllMergedFlagBitSet[i];
            stepBy1KBitSet.set(0, max1KSegmentsGroupNumber, false);
        }

        isOverHalfSegmentNumberForFirstReuseLoop = false;

        // clear marked persisted segment index
        for (int i = 0; i < beginSegmentIndexGroupByWalGroupIndex.length; i++) {
            var markedLongs = beginSegmentIndexGroupByWalGroupIndex[i];
            for (int j = 0; j < MARK_BEGIN_SEGMENT_INDEX_COUNT; j++) {
                markedLongs[j] = 0L;
            }
            beginSegmentIndexMoveIndexGroupByWalGroupIndex[i] = 0;
        }
    }

    /**
     * Updates the bit set for segment reuse when overwriting the in-memory cache.
     */
    private void updateCanReuseBitSetWhenOverwrite() {
        // set segment can reuse bit set
        for (int i = 0; i < fdPerChunk; i++) {
            var bitSet = segmentCanReuseBitSet[i];
            var offset = i * segmentNumberPerFd * ONE_LENGTH;
            for (int j = 0; j < segmentNumberPerFd; j++) {
                var flagByte = inMemoryCachedBytes[offset];
                bitSet.set(j, Chunk.Flag.canReuse(flagByte));
                offset += ONE_LENGTH;
            }
        }

        // update is over half segment number flag
        int offset = halfSegmentNumber * ONE_LENGTH;
        for (int i = halfSegmentNumber; i < maxSegmentNumber; i++) {
            var flagByte = inMemoryCachedBytes[offset];
            if (flagByte != Chunk.Flag.init.flagByte()) {
                isOverHalfSegmentNumberForFirstReuseLoop = true;
                break;
            }
            offset += ONE_LENGTH;
        }
    }

    /**
     * Constructs a new instance of MetaChunkSegmentFlagSeq.
     * Initializes the in-memory cache and file storage for segment flags and sequence numbers based on the provided slot and slot directory.
     *
     * @param slot    the slot index
     * @param slotDir the directory of the slot
     * @throws IOException If an I/O error occurs during file operations.
     */
    public MetaChunkSegmentFlagSeq(short slot, @NotNull File slotDir) throws IOException {
        this.slot = slot;

        this.fdPerChunk = ConfForSlot.global.confChunk.fdPerChunk;
        this.segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;

        this.stepBy1KSegmentsGroupAllMergedFlagBitSet = new BitSet[fdPerChunk];
        this.segmentCanReuseBitSet = new BitSet[fdPerChunk];
        this.max1KSegmentsGroupNumber = segmentNumberPerFd / 1024;
        for (int i = 0; i < fdPerChunk; i++) {
            this.segmentCanReuseBitSet[i] = new BitSet(segmentNumberPerFd);
            this.stepBy1KSegmentsGroupAllMergedFlagBitSet[i] = new BitSet(max1KSegmentsGroupNumber);
        }

        this.beginSegmentIndexGroupByWalGroupIndex = new long[Wal.calcWalGroupNumber()][MARK_BEGIN_SEGMENT_INDEX_COUNT];
        this.beginSegmentIndexMoveIndexGroupByWalGroupIndex = new int[Wal.calcWalGroupNumber()];

        initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear();

        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        this.halfSegmentNumber = maxSegmentNumber / 2;
        this.allCapacity = maxSegmentNumber * ONE_LENGTH;

        // max max segment number <= 512KB * 8, 512KB * 8 * 13 = 56MB
        this.inMemoryCachedBytes = new byte[allCapacity];
        fillSegmentFlagInit(inMemoryCachedBytes);

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_SEQ_FLAG_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);
            FileUtils.writeByteArrayToFile(file, this.inMemoryCachedBytes, true);
        } else {
            needRead = true;
        }
        this.raf = new RandomAccessFile(file, "rw");

        if (needRead) {
            raf.seek(0);
            raf.read(inMemoryCachedBytes);
            log.warn("Read meta chunk segment flag seq file success, file={}, slot={}, all capacity={}KB",
                    file, slot, allCapacity / 1024);

            this.updateCanReuseBitSetWhenOverwrite();
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
        this.reloadMarkPersistedSegmentIndex();

        var initMemoryMB = allCapacity / 1024 / 1024;
        log.info("Static memory init, type={}, MB={}, slot={}", StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, true);
    }

    /**
     * Estimates the size of the in-memory cached bytes and appends it to the provided StringBuilder.
     *
     * @param sb the StringBuilder to append the memory usage estimate
     * @return the estimated size in bytes
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        // object header 16B, about 20 fields most are int or long
        long size = allCapacity + 16 * 10;
        // bit set size
        // need not accurate
        size += fdPerChunk * ((long) segmentNumberPerFd / 8 + 16 * 10);
        sb.append("Meta chunk segment flag seq: ").append(size).append("\n");
        return size;
    }

    /**
     * Updates the bit set for segment reuse for a specific segment index.
     *
     * @param fdIndex            the file descriptor index
     * @param targetSegmentIndex the target segment index
     * @param canReuse           Whether the segment can be reused.
     */
    void updateBitSetCanReuseForSegmentIndex(int fdIndex, int targetSegmentIndex, boolean canReuse) {
        var bitSet = segmentCanReuseBitSet[fdIndex];
        bitSet.set(targetSegmentIndex, canReuse);

        if (!canReuse) {
            var stepBy1KBitSet = stepBy1KSegmentsGroupAllMergedFlagBitSet[fdIndex];
            var groupNumber = targetSegmentIndex / 1024;
            stepBy1KBitSet.set(groupNumber, false);

            if (canTruncateFdIndex == fdIndex) {
                canTruncateFdIndex = -1;
            }
        }
    }

    /**
     * Finds a segment index that can be reused starting from a specific index.
     *
     * @param beginSegmentIndex the starting index to search for a reusable segment
     * @param segmentCount      the number of segments to find
     * @return the index of the first segment that can be reused, or -1 if no such segment is found
     */
    int findCanReuseSegmentIndex(int beginSegmentIndex, int segmentCount) {
        int currentSegmentIndex = beginSegmentIndex;

        while (currentSegmentIndex < maxSegmentNumber) {
            var fdIndex = currentSegmentIndex / segmentNumberPerFd;
            var targetSegmentIndexTargetFd = currentSegmentIndex % segmentNumberPerFd;

            var bitSet = segmentCanReuseBitSet[fdIndex];

            int segmentAvailableCount = 0;
            for (int i = targetSegmentIndexTargetFd; i < segmentNumberPerFd && segmentAvailableCount < segmentCount; i++) {
                if (bitSet.get(i)) {
                    segmentAvailableCount++;
                } else {
                    break;
                }
            }

            if (segmentAvailableCount == segmentCount) {
                return currentSegmentIndex;
            }

            currentSegmentIndex++;
        }

        return -1;
    }

    private int checkFdIndex = 0;
    private int check1KSegmentsGroupIndex = 0;
    int canTruncateFdIndex;

    /**
     * Returns the name of the task.
     *
     * @return the name of the task, "segments all merged check for truncate file"
     */
    @Override
    public String name() {
        return "segments all merged check for truncate file";
    }

    /**
     * Runs the task to check for segments that can be truncated.
     */
    @Override
    public void run() {
        boolean isAllCanClear1KSegmentsGroup = true;
        var bitSet = segmentCanReuseBitSet[checkFdIndex];
        var beginSegmentIndex = checkFdIndex * segmentNumberPerFd + check1KSegmentsGroupIndex * 1024;

        for (int i = 0; i < 1024; i++) {
            if (!bitSet.get(beginSegmentIndex + i)) {
                isAllCanClear1KSegmentsGroup = false;
                break;
            }
        }

        if (isAllCanClear1KSegmentsGroup) {
            var stepBy1KBitSet = stepBy1KSegmentsGroupAllMergedFlagBitSet[checkFdIndex];
            stepBy1KBitSet.set(check1KSegmentsGroupIndex);

            // if all true, means can truncate
            if (stepBy1KBitSet.cardinality() == max1KSegmentsGroupNumber) {
                canTruncateFdIndex = checkFdIndex;
            } else {
                canTruncateFdIndex = -1;
            }
        } else {
            canTruncateFdIndex = -1;
        }

        check1KSegmentsGroupIndex++;
        if (check1KSegmentsGroupIndex == max1KSegmentsGroupNumber) {
            // reach one file end
            check1KSegmentsGroupIndex = 0;

            checkFdIndex++;
            if (checkFdIndex == fdPerChunk) {
                // reach all files end, from the first file
                checkFdIndex = 0;
            }
        }
    }

    /**
     * Returns the number of times the task should execute after the loop count.
     *
     * @return the number of times the task should execute, every 100 milliseconds
     */
    @Override
    public int executeOnceAfterLoopCount() {
        // execute once every 100ms
        return 10;
    }

    // 10m keys, 16384 wal groups, one wal group ~= 600 keys, 1 persist batch maybe 150 keys, 4 batch may persist all keys in a loop
    private static final int MARK_BEGIN_SEGMENT_INDEX_COUNT = 100;
    private final long[][] beginSegmentIndexGroupByWalGroupIndex;
    private final int[] beginSegmentIndexMoveIndexGroupByWalGroupIndex;

    /**
     * Reloads the marked persisted segment index.
     *
     * @return the number of marked segments
     */
    @VisibleForTesting
    int reloadMarkPersistedSegmentIndex() {
        int currentWalGroupIndex = INIT_WAL_GROUP_INDEX;
        int tmpBeginSegmentIndex = -1;
        int continueUsedSegmentCount = 0;

        int markedCount = 0;
        for (int segmentIndex = 0; segmentIndex < maxSegmentNumber; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            if (currentWalGroupIndex == INIT_WAL_GROUP_INDEX && walGroupIndex != INIT_WAL_GROUP_INDEX) {
                currentWalGroupIndex = walGroupIndex;
            }

            if (Chunk.Flag.canReuse(flagByte) || currentWalGroupIndex != walGroupIndex) {
                if (tmpBeginSegmentIndex != -1) {
                    markPersistedSegmentIndexToTargetWalGroup(currentWalGroupIndex, tmpBeginSegmentIndex, (short) continueUsedSegmentCount);
                    markedCount++;
                }

                // break
                currentWalGroupIndex = INIT_WAL_GROUP_INDEX;
                tmpBeginSegmentIndex = -1;
                continueUsedSegmentCount = 0;
            } else {
                if (tmpBeginSegmentIndex == -1) {
                    tmpBeginSegmentIndex = segmentIndex;
                }
                continueUsedSegmentCount++;
            }
        }

        return markedCount;
    }

    /**
     * Prints the marked persisted segment index for a specific wal group index. Only used for testing.
     *
     * @param walGroupIndex the wal group index for which to print the marked persisted segment index
     */
    @TestOnly
    void printMarkedPersistedSegmentIndex(int walGroupIndex) {
        var markedLongs = beginSegmentIndexGroupByWalGroupIndex[walGroupIndex];
        var next = beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex];

        System.out.println("walGroupIndex: " + walGroupIndex);
        for (int i = 0; i < MARK_BEGIN_SEGMENT_INDEX_COUNT; i++) {
            var longValue = markedLongs[i];
            if (longValue == 0L) {
                continue;
            }

            var beginSegmentIndex = (int) (longValue >> 32);
            var segmentCount = (short) (longValue >> 16);
            var find2Times = (short) (longValue);

            System.out.println("walGroupIndex: " + walGroupIndex + ", beginSegmentIndex: " + beginSegmentIndex + ", segmentCount: " + segmentCount + ", find2Times: " + find2Times);
        }
        System.out.println("next: " + next);
    }

    // once find those need merge, half of full, or invalid cv count is not much, for perf
    @VisibleForTesting
    byte preReadFindTimesForOncePersist = 0;

    /**
     * Marks the persisted segment index for a specific wal group index. After Chunk persist some segments of one WAL group.
     *
     * @param walGroupIndex     the wal group index
     * @param beginSegmentIndex the begin segment index
     * @param segmentCount      Persisted segment count.
     */
    public void markPersistedSegmentIndexToTargetWalGroup(int walGroupIndex, int beginSegmentIndex, short segmentCount) {
        var beginSegmentIndexMoveIndex = beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex];
        var next = beginSegmentIndexMoveIndex + 1;
        if (next == MARK_BEGIN_SEGMENT_INDEX_COUNT) {
            next = 0;
        }

        // make sure already pre-read and cleared
        assert (beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] == 0L);

        beginSegmentIndexGroupByWalGroupIndex[walGroupIndex][next] = (long) beginSegmentIndex << 32 | segmentCount << 16 | preReadFindTimesForOncePersist;
        beginSegmentIndexMoveIndexGroupByWalGroupIndex[walGroupIndex] = next;
    }

    /**
     * Represents the not found segment index and count.
     */
    static final int[] NOT_FIND_SEGMENT_INDEX_AND_COUNT = new int[]{-1, 0};

    @VisibleForTesting
    boolean isOverHalfSegmentNumberForFirstReuseLoop = false;

    /**
     * Finds the segments that need to be merged for a specific wal group index. Calculating by marked segment index.
     *
     * @param walGroupIndex the wal group index
     * @return the segment index and count that need to be merged
     */
    int[] findThoseNeedToMerge(int walGroupIndex) {
        if (!isOverHalfSegmentNumberForFirstReuseLoop) {
            return NOT_FIND_SEGMENT_INDEX_AND_COUNT;
        }

        var markedLongs = beginSegmentIndexGroupByWalGroupIndex[walGroupIndex];
        for (int i = 0; i < MARK_BEGIN_SEGMENT_INDEX_COUNT; i++) {
            var markedLong = markedLongs[i];
            if (markedLong != 0L) {
                var segmentIndex = (int) (markedLong >> 32);
                var segmentCount = (short) (markedLong >> 16 & 0xFFFF);

                if (segmentCount == 1) {
                    // clear
                    markedLongs[i] = 0L;
                    return new int[]{segmentIndex, segmentCount};
                }

                var findTimes = (byte) (markedLong & 0xFF);
                if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
                    findTimes = 1;
                }

                if (findTimes == 1) {
                    short halfSegmentCount = (short) (segmentCount / 2);
                    short leftSegmentCount = (short) (segmentCount - halfSegmentCount);

                    markedLongs[i] = (long) (segmentIndex + halfSegmentCount) << 32 | leftSegmentCount << 16;
                    return new int[]{segmentIndex, halfSegmentCount};
                } else {
                    // clear
                    markedLongs[i] = 0L;
                    return new int[]{segmentIndex, segmentCount};
                }
            }
        }
        return NOT_FIND_SEGMENT_INDEX_AND_COUNT;
    }

    /**
     * Callback interface for iterating over segments.
     */
    public interface IterateCallBack {
        /**
         * Callback method invoked for each segment.
         *
         * @param segmentIndex  the segment index
         * @param flagByte      the flag byte
         * @param segmentSeq    the segment sequence
         * @param walGroupIndex the wal group index
         * @return true to continue iterating, false to stop
         */
        boolean call(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex);
    }

    /**
     * Iterates over a range of segments and calls the provided callback for each segment.
     *
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments to iterate over
     * @param callBack          the callback to be invoked for each segment
     */
    public void iterateRange(int beginSegmentIndex, int segmentCount, @NotNull IterateCallBack callBack) {
        var end = Math.min(beginSegmentIndex + segmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            var r = callBack.call(segmentIndex, flagByte, segmentSeq, walGroupIndex);
            if (!r) {
                break;
            }
        }
    }

    /**
     * Iterates over all segments and calls the provided callback for each segment.
     *
     * @param callBack the callback to be invoked for each segment
     */
    public void iterateAll(@NotNull IterateCallBack callBack) {
        for (int segmentIndex = 0; segmentIndex < maxSegmentNumber; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, flagByte, segmentSeq, walGroupIndex);
        }
    }

    /**
     * Gets a list of segment sequence numbers for a batch of segments. For replication.
     *
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments to retrieve
     * @return the list of segment sequence numbers
     */
    List<Long> getSegmentSeqListBatchForRepl(int beginSegmentIndex, int segmentCount) {
        var offset = beginSegmentIndex * ONE_LENGTH;
        var list = new ArrayList<Long>();
        for (int i = beginSegmentIndex; i < beginSegmentIndex + segmentCount; i++) {
            list.add(inMemoryCachedByteBuffer.getLong(offset + 1));
            offset += ONE_LENGTH;
        }
        return list;
    }

    /**
     * Sets the merge flag for a segment.
     *
     * @param segmentIndex  the segment index
     * @param flagByte      the flag byte
     * @param segmentSeq    the segment sequence number
     * @param walGroupIndex the wal group index
     */
    @SlaveNeedReplay
    public void setSegmentMergeFlag(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex) {
        var offset = segmentIndex * ONE_LENGTH;

        var bytes = new byte[ONE_LENGTH];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.put(flagByte);
        wrap.putLong(segmentSeq);
        wrap.putInt(walGroupIndex);

        StatKeyCountInBuckets.writeToRaf(offset, bytes, inMemoryCachedByteBuffer, raf);

        updateBitSetCanReuseForSegmentIndex(segmentIndex / segmentNumberPerFd,
                segmentIndex % segmentNumberPerFd,
                Chunk.Flag.canReuse(flagByte));

        if (!isOverHalfSegmentNumberForFirstReuseLoop) {
            if (segmentIndex >= halfSegmentNumber) {
                isOverHalfSegmentNumberForFirstReuseLoop = true;
            }
        }
    }

    /**
     * Sets the merge flag for a batch of segments.
     *
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments to set
     * @param flagByte          the flag byte
     * @param segmentSeqList    the segment sequence numbers
     * @param walGroupIndex     the wal group index
     */
    void setSegmentMergeFlagBatch(int beginSegmentIndex,
                                  int segmentCount,
                                  byte flagByte,
                                  @Nullable List<Long> segmentSeqList,
                                  int walGroupIndex) {
        var offset = beginSegmentIndex * ONE_LENGTH;

        var bytes = new byte[segmentCount * ONE_LENGTH];
        var wrap = ByteBuffer.wrap(bytes);
        for (int i = 0; i < segmentCount; i++) {
            wrap.put(i * ONE_LENGTH, flagByte);
            wrap.putLong(i * ONE_LENGTH + 1, segmentSeqList == null ? 0L : segmentSeqList.get(i));
            wrap.putInt(i * ONE_LENGTH + 1 + 8, walGroupIndex);
        }

        StatKeyCountInBuckets.writeToRaf(offset, bytes, inMemoryCachedByteBuffer, raf);

        var canReuse = Chunk.Flag.canReuse(flagByte);
        for (int i = 0; i < segmentCount; i++) {
            var segmentIndex = beginSegmentIndex + i;
            updateBitSetCanReuseForSegmentIndex(segmentIndex / segmentNumberPerFd,
                    segmentIndex % segmentNumberPerFd,
                    canReuse);
        }

        if (!isOverHalfSegmentNumberForFirstReuseLoop) {
            if (beginSegmentIndex >= halfSegmentNumber) {
                isOverHalfSegmentNumberForFirstReuseLoop = true;
            }
        }
    }

    /**
     * Gets the merge flag for a segment.
     *
     * @param segmentIndex the segment index
     * @return the segment flag with segment sequence number and wal group index
     */
    Chunk.SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        var offset = segmentIndex * ONE_LENGTH;
        return new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset),
                inMemoryCachedByteBuffer.getLong(offset + 1),
                inMemoryCachedByteBuffer.getInt(offset + 1 + 8));
    }

    /**
     * Gets the merge flag for a batch of segments.
     *
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments to retrieve
     * @return the list of segment flags
     */
    ArrayList<Chunk.SegmentFlag> getSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount) {
        var list = new ArrayList<Chunk.SegmentFlag>(segmentCount);
        var offset = beginSegmentIndex * ONE_LENGTH;
        for (int i = 0; i < segmentCount; i++) {
            list.add(new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset),
                    inMemoryCachedByteBuffer.getLong(offset + 1),
                    inMemoryCachedByteBuffer.getInt(offset + 1 + 8)));
            offset += ONE_LENGTH;
        }
        return list;
    }

    /**
     * Check if target segments are not write or merged.
     *
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments to check
     * @return true if all segments are not write or merged, false otherwise
     */
    boolean isAllFlagsNotWrite(int beginSegmentIndex, int segmentCount) {
        var offset = beginSegmentIndex * ONE_LENGTH;
        for (int i = 0; i < segmentCount; i++) {
            var flagByte = inMemoryCachedByteBuffer.get(offset);
            if (flagByte == Chunk.Flag.new_write.flagByte || flagByte == Chunk.Flag.reuse_new.flagByte) {
                return false;
            }
            offset += ONE_LENGTH;
        }

        return true;
    }

    /**
     * Clear the meta chunk segment flag sequence. When slot do flush.
     */
    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        if (ConfForGlobal.pureMemory) {
            fillSegmentFlagInit(inMemoryCachedBytes);
            initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear();
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            fillSegmentFlagInit(tmpBytes);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
            initBitSetValueAndMarkedSegmentIndexWhenFirstStartOrClear();
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Clean up the meta chunk segment flag sequence. When server stops.
     */
    @Override
    public void cleanUp() {
        if (ConfForGlobal.pureMemory) {
            return;
        }

        // sync all
        try {
//            raf.getFD().sync();
            raf.close();
            System.out.println("Meta chunk segment flag seq file closed.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
