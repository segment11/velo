package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.StaticMemoryPrepareBytesStats;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
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
import java.util.List;

public class MetaChunkSegmentFlagSeq implements InMemoryEstimate, NeedCleanUp {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + seq long + wal group index int
    public static final int ONE_LENGTH = 1 + 8 + 4;

    public static final int INIT_WAL_GROUP_INDEX = -1;

    private final short slot;
    private final int maxSegmentNumber;

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

    public MetaChunkSegmentFlagSeq(short slot, @NotNull File slotDir) throws IOException {
        this.slot = slot;
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

    public interface IterateCallBack {
        void call(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex);
    }

    // performance critical
    int[] iterateAndFindThoseNeedToMerge(int beginSegmentIndex, int nextSegmentCount, int targetWalGroupIndex, @NotNull Chunk chunk) {
        var findSegmentIndexWithSegmentCount = new int[]{Chunk.NO_NEED_MERGE_SEGMENT_INDEX, 0};

        // only find 4 segments at most, or once write too many segments for this batch
        final var maxFindSegmentCount = 4;
        var segmentCount = 0;
        var end = Math.min(beginSegmentIndex + nextSegmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
//            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);
            if (walGroupIndex != targetWalGroupIndex) {
                // already find at least one segment with the same wal group index
                if (findSegmentIndexWithSegmentCount[0] != Chunk.NO_NEED_MERGE_SEGMENT_INDEX) {
                    break;
                }
                continue;
            }

            // merged but not persisted, also do compare again
            if (flagByte == Chunk.Flag.new_write.flagByte || flagByte == Chunk.Flag.reuse_new.flagByte || flagByte == Chunk.Flag.merged.flagByte) {
                // only set first segment index
                if (findSegmentIndexWithSegmentCount[0] == Chunk.NO_NEED_MERGE_SEGMENT_INDEX) {
                    findSegmentIndexWithSegmentCount[0] = segmentIndex;
                }
                segmentCount++;

                var targetFdIndexFirstFind = chunk.targetFdIndex(findSegmentIndexWithSegmentCount[0]);
                var targetFdIndexThisFind = chunk.targetFdIndex(segmentIndex);
                // cross two files, exclude this find segment
                if (targetFdIndexThisFind != targetFdIndexFirstFind) {
                    segmentCount--;
                    break;
                }

                int segmentCountMax = Math.min(maxFindSegmentCount, chunk.maxSegmentIndex - segmentIndex + 1);
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

    int getMergedSegmentIndexEndLastTime(int currentSegmentIndex, int halfSegmentNumber) {
        // only execute once when server start, do not mind performance
        boolean isAllFlagInit = true;
        for (int i = 0; i < allCapacity; i += ONE_LENGTH) {
            if (inMemoryCachedBytes[i] != Chunk.Flag.init.flagByte) {
                isAllFlagInit = false;
                break;
            }
        }
        if (isAllFlagInit) {
            return Chunk.NO_NEED_MERGE_SEGMENT_INDEX;
        }

        var max = Chunk.NO_NEED_MERGE_SEGMENT_INDEX;

        int begin = 0;
        int end = halfSegmentNumber;
        if (currentSegmentIndex < halfSegmentNumber) {
            begin = halfSegmentNumber;
            end = ConfForSlot.global.confChunk.maxSegmentNumber();
        }
        log.info("Get merged segment index end last time, current segment index={}, half segment number={}, begin={}, end={}, slot={}",
                currentSegmentIndex, halfSegmentNumber, begin, end, slot);

        boolean isAllFlagInitHalf = true;
        for (int segmentIndex = begin; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            if (flagByte != Chunk.Flag.init.flagByte) {
                isAllFlagInitHalf = false;
            }
            if (flagByte == Chunk.Flag.merged.flagByte || flagByte == Chunk.Flag.merged_and_persisted.flagByte || flagByte == Chunk.Flag.init.flagByte) {
                max = segmentIndex;
            } else {
                break;
            }
        }

        if (isAllFlagInitHalf) {
            max = Chunk.NO_NEED_MERGE_SEGMENT_INDEX;
        }
        return max;
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
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            fillSegmentFlagInit(tmpBytes);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
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
