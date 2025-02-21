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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class MetaChunkSegmentFlagSeq implements InMemoryEstimate, NeedCleanUp, ITask {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + seq long + wal group index int
    public static final int ONE_LENGTH = 1 + 8 + 4;

    public static final int INIT_WAL_GROUP_INDEX = -1;

    private final short slot;
    private final int fdPerChunk;
    private final int segmentNumberPerFd;
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

    private static void fillSegmentFlagInit(byte[] innerBytes) {
        var initBytes = new byte[ONE_LENGTH];
        var initBuffer = ByteBuffer.wrap(initBytes);
        initBuffer.put(Chunk.Flag.init.flagByte);
        initBuffer.putLong(0L);
        initBuffer.putInt(INIT_WAL_GROUP_INDEX);

        var times = innerBytes.length / ONE_LENGTH;
        var innerBuffer = ByteBuffer.wrap(innerBytes);
        for (int i = 0; i < times; i++) {
            innerBuffer.put(initBytes);
        }
    }

    // for save and load
    // readonly
    byte[] getInMemoryCachedBytes() {
        // usually more than 10M, do not copy
        return inMemoryCachedBytes;
    }

    // for save and load
    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Meta chunk segment flag seq, bytes length not match");
        }

        inMemoryCachedByteBuffer.position(0).put(bytes);
        updateCanReuseBitSetWhenOverwrite();
    }

    public byte[] getOneBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount * ONE_LENGTH];
        var offset = beginBucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.position(offset).get(dst);
        return dst;
    }

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

    private void initBitSetValueWhenFirstStartOrClear() {
        for (int i = 0; i < fdPerChunk; i++) {
            // set all true
            var bitSet = segmentCanReuseBitSet[i];
            bitSet.set(0, segmentNumberPerFd, true);

            var stepBy1KBitSet = stepBy1KSegmentsGroupAllMergedFlagBitSet[i];
            stepBy1KBitSet.set(0, max1KSegmentsGroupNumber, false);
        }
    }

    private void updateCanReuseBitSetWhenOverwrite() {
        // set segment can reuse bit set
        for (int i = 0; i < fdPerChunk; i++) {
            var bitSet = segmentCanReuseBitSet[i];
            for (int j = 0; j < segmentNumberPerFd; j++) {
                var offset = j * ONE_LENGTH;
                var flagByte = inMemoryCachedBytes[offset];
                bitSet.set(j, Chunk.Flag.canReuse(flagByte));
            }
        }
    }

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
        initBitSetValueWhenFirstStartOrClear();

        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
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

            updateCanReuseBitSetWhenOverwrite();
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);

        var initMemoryMB = allCapacity / 1024 / 1024;
        log.info("Static memory init, type={}, MB={}, slot={}", StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, true);
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        sb.append("Meta chunk segment flag seq: ").append(allCapacity).append("\n");
        return allCapacity;
    }

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

    @Override
    public String name() {
        return "segments all merged check for truncate file";
    }

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

    @Override
    public int executeOnceAfterLoopCount() {
        // execute once every 100ms
        return 10;
    }

    public interface IterateCallBack {
        void call(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex);
    }

    // performance critical
    int[] iterateAndFindThoseNeedToMerge(int beginSegmentIndex, int nextSegmentCount, int targetWalGroupIndex) {
        var findSegmentIndexWithSegmentCount = new int[]{-1, 0};

        // only find 4 segments at most, or once write too many segments for this batch
        final var maxFindSegmentCount = 4;
        int maxSegmentIndex = ConfForSlot.global.confChunk.maxSegmentNumber() - 1;
        var segmentCount = 0;
        var end = Math.min(beginSegmentIndex + nextSegmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
//            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);
            if (walGroupIndex != targetWalGroupIndex) {
                // already find at least one segment with the same wal group index
                if (findSegmentIndexWithSegmentCount[0] != -1) {
                    break;
                }
                continue;
            }

            // merged but not persisted, also do compare again
            if (flagByte == Chunk.Flag.new_write.flagByte || flagByte == Chunk.Flag.reuse_new.flagByte) {
                // only set first segment index
                if (findSegmentIndexWithSegmentCount[0] == -1) {
                    findSegmentIndexWithSegmentCount[0] = segmentIndex;
                }
                segmentCount++;

                var targetFdIndexFirstFind = findSegmentIndexWithSegmentCount[0] / segmentNumberPerFd;
                var targetFdIndexThisFind = segmentIndex / segmentNumberPerFd;
                // cross two files, exclude this find segment
                if (targetFdIndexThisFind != targetFdIndexFirstFind) {
                    segmentCount--;
                    break;
                }

                int segmentCountMax = Math.min(maxFindSegmentCount, maxSegmentIndex - segmentIndex + 1);
                if (segmentCount >= segmentCountMax) {
                    break;
                }
            }
        }

        findSegmentIndexWithSegmentCount[1] = segmentCount;
        return findSegmentIndexWithSegmentCount;
    }

    public void iterateRange(int beginSegmentIndex, int segmentCount, @NotNull IterateCallBack callBack) {
        var end = Math.min(beginSegmentIndex + segmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, flagByte, segmentSeq, walGroupIndex);
        }
    }

    public void iterateAll(@NotNull IterateCallBack callBack) {
        for (int segmentIndex = 0; segmentIndex < ConfForSlot.global.confChunk.maxSegmentNumber(); segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, flagByte, segmentSeq, walGroupIndex);
        }
    }

    List<Long> getSegmentSeqListBatchForRepl(int beginSegmentIndex, int segmentCount) {
        var offset = beginSegmentIndex * ONE_LENGTH;
        var list = new ArrayList<Long>();
        for (int i = beginSegmentIndex; i < beginSegmentIndex + segmentCount; i++) {
            list.add(inMemoryCachedByteBuffer.getLong(offset + 1));
            offset += ONE_LENGTH;
        }
        return list;
    }

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
    }

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
    }

    Chunk.SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        var offset = segmentIndex * ONE_LENGTH;
        return new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset),
                inMemoryCachedByteBuffer.getLong(offset + 1),
                inMemoryCachedByteBuffer.getInt(offset + 1 + 8));
    }

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

    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        if (ConfForGlobal.pureMemory) {
            fillSegmentFlagInit(inMemoryCachedBytes);
            initBitSetValueWhenFirstStartOrClear();
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            fillSegmentFlagInit(tmpBytes);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
            initBitSetValueWhenFirstStartOrClear();
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
