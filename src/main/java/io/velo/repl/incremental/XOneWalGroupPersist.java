package io.velo.repl.incremental;

import io.velo.persist.Chunk;
import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.util.TreeMap;

public class XOneWalGroupPersist implements BinlogContent {
    private boolean isShortValue;
    private boolean clearWalAfterApply;
    private final int walGroupIndex;

    @TestOnly
    public void setShortValue(boolean isShortValue) {
        this.isShortValue = isShortValue;
    }

    @TestOnly
    public void setClearWalAfterApply(boolean clearWalAfterApply) {
        this.clearWalAfterApply = clearWalAfterApply;
    }

    public XOneWalGroupPersist(boolean isShortValue, boolean clearWalAfterApply, int walGroupIndex) {
        this.isShortValue = isShortValue;
        this.clearWalAfterApply = clearWalAfterApply;
        this.walGroupIndex = walGroupIndex;
    }

    private int beginBucketIndex;

    public void setBeginBucketIndex(int beginBucketIndex) {
        this.beginBucketIndex = beginBucketIndex;
    }

    private short[] keyCountForStatsTmp;

    public void setKeyCountForStatsTmp(short[] keyCountForStatsTmp) {
        this.keyCountForStatsTmp = keyCountForStatsTmp;
    }

    private byte[][] recordXBytesArray;

    public void setRecordXBytesArray(byte[][] recordXBytesArray) {
        this.recordXBytesArray = recordXBytesArray;
    }

    private byte[][] sharedBytesListBySplitIndex;

    public void setSharedBytesListBySplitIndex(byte[][] sharedBytesListBySplitIndex) {
        this.sharedBytesListBySplitIndex = sharedBytesListBySplitIndex;
    }

    private long[] oneWalGroupSeqArrayBySplitIndex;

    public void setOneWalGroupSeqArrayBySplitIndex(long[] oneWalGroupSeqArrayBySplitIndex) {
        this.oneWalGroupSeqArrayBySplitIndex = oneWalGroupSeqArrayBySplitIndex;
    }

    private byte[] splitNumberAfterPut;

    public void setSplitNumberAfterPut(byte[] splitNumberAfterPut) {
        this.splitNumberAfterPut = splitNumberAfterPut;
    }

    record SegmentFlagWithSeq(byte flagByte, long seq) {
    }

    private final TreeMap<Integer, SegmentFlagWithSeq> updatedChunkSegmentFlagWithSeqMap = new TreeMap<>();

    public void putUpdatedChunkSegmentFlagWithSeq(int segmentIndex, byte flagByte, long seq) {
        updatedChunkSegmentFlagWithSeqMap.put(segmentIndex, new SegmentFlagWithSeq(flagByte, seq));
    }

    private final TreeMap<Integer, byte[]> updatedChunkSegmentBytesMap = new TreeMap<>();

    public void putUpdatedChunkSegmentBytes(int segmentIndex, byte[] bytes) {
        updatedChunkSegmentBytesMap.put(segmentIndex, bytes);
    }

    private int chunkSegmentIndexAfterPersist;

    public void setChunkSegmentIndexAfterPersist(int chunkSegmentIndexAfterPersist) {
        this.chunkSegmentIndexAfterPersist = chunkSegmentIndexAfterPersist;
    }

    private int chunkMergedSegmentIndexEndLastTime = Chunk.NO_NEED_MERGE_SEGMENT_INDEX;

    public void setChunkMergedSegmentIndexEndLastTime(int chunkMergedSegmentIndexEndLastTime) {
        this.chunkMergedSegmentIndexEndLastTime = chunkMergedSegmentIndexEndLastTime;
    }

    private long lastSegmentSeq;

    public void setLastSegmentSeq(long lastSegmentSeq) {
        this.lastSegmentSeq = lastSegmentSeq;
    }

    @Override
    public Type type() {
        return Type.one_wal_group_persist;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        var n = 1 + 4;
        // 1 byte for is short value, 1 byte for clear wal after apply, 4 bytes for wal group index
        n += 1 + 1 + 4;
        // 4 bytes for begin bucket index
        n += 4;

        // 4 bytes for key count for stats tmp, 2 bytes for each key count
        n += 4 + keyCountForStatsTmp.length * 2;

        n += 4;
        if (recordXBytesArray != null) {
            for (var bytes : recordXBytesArray) {
                // 4 bytes for each record x bytes length, record x bytes
                n += 4;
                if (bytes != null) {
                    n += bytes.length;
                }
            }
        }

        // 4 bytes for shared bytes list by split index size
        n += 4;
        if (sharedBytesListBySplitIndex != null) {
            for (var bytes : sharedBytesListBySplitIndex) {
                // 4 bytes for each shared bytes length, shared bytes
                n += 4;
                if (bytes != null) {
                    n += bytes.length;
                }
            }
        }

        // 4 bytes for one wal group seq array
        n += 4;
        if (oneWalGroupSeqArrayBySplitIndex != null) {
            n += oneWalGroupSeqArrayBySplitIndex.length * 8;
        }

        // 4 bytes for split number after put length, split number after put
        n += 4;
        if (splitNumberAfterPut != null) {
            n += splitNumberAfterPut.length;
        }

        // 4 bytes for updated chunk segment flag with seq map size
        n += 4;
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            // 4 bytes for segment index, 1 byte for flag, 8 bytes for seq
            n += 4 + 1 + 8;
        }

        // 4 bytes for updated chunk segment bytes map size
        n += 4;
        for (var entry : updatedChunkSegmentBytesMap.entrySet()) {
            // 4 bytes for segment index, 4 bytes for bytes length, bytes
            n += 4 + 4;
            n += entry.getValue().length;
        }

        // 4 bytes for chunk segment index after persist
        // 4 bytes for chunk merged segment index end last time
        // 4 bytes for last segment seq
        n += 4 + 4 + 8;
        return n;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.put(isShortValue ? (byte) 1 : (byte) 0);
        buffer.put(clearWalAfterApply ? (byte) 1 : (byte) 0);
        buffer.putInt(walGroupIndex);
        buffer.putInt(beginBucketIndex);

        buffer.putInt(keyCountForStatsTmp.length);
        for (var keyCount : keyCountForStatsTmp) {
            buffer.putShort(keyCount);
        }

        if (recordXBytesArray != null) {
            buffer.putInt(recordXBytesArray.length);
            for (var recordXBytes : recordXBytesArray) {
                if (recordXBytes == null) {
                    buffer.putInt(0);
                } else {
                    buffer.putInt(recordXBytes.length);
                    buffer.put(recordXBytes);
                }
            }
        } else {
            buffer.putInt(0);
        }

        if (sharedBytesListBySplitIndex != null) {
            buffer.putInt(sharedBytesListBySplitIndex.length);
            for (var sharedBytes : sharedBytesListBySplitIndex) {
                if (sharedBytes == null) {
                    buffer.putInt(0);
                } else {
                    buffer.putInt(sharedBytes.length);
                    buffer.put(sharedBytes);
                }
            }
        } else {
            buffer.putInt(0);
        }

        if (oneWalGroupSeqArrayBySplitIndex != null) {
            buffer.putInt(oneWalGroupSeqArrayBySplitIndex.length);
            for (var seq : oneWalGroupSeqArrayBySplitIndex) {
                buffer.putLong(seq);
            }
        } else {
            buffer.putInt(0);
        }

        if (splitNumberAfterPut != null) {
            buffer.putInt(splitNumberAfterPut.length);
            buffer.put(splitNumberAfterPut);
        } else {
            buffer.putInt(0);
        }

        buffer.putInt(updatedChunkSegmentFlagWithSeqMap.size());
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.put(entry.getValue().flagByte());
            buffer.putLong(entry.getValue().seq);
        }

        buffer.putInt(updatedChunkSegmentBytesMap.size());
        for (var entry : updatedChunkSegmentBytesMap.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.putInt(entry.getValue().length);
            buffer.put(entry.getValue());
        }

        buffer.putInt(chunkSegmentIndexAfterPersist);
        buffer.putInt(chunkMergedSegmentIndexEndLastTime);
        buffer.putLong(lastSegmentSeq);

        return bytes;
    }

    public static XOneWalGroupPersist decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();
        var isShortValue = buffer.get() == 1;
        var clearWalAfterApply = buffer.get() == 1;
        var walGroupIndex = buffer.getInt();
        var x = new XOneWalGroupPersist(isShortValue, clearWalAfterApply, walGroupIndex);
        x.setBeginBucketIndex(buffer.getInt());

        var keyCountForStatsTmpSize = buffer.getInt();
        var keyCountForStatsTmp = new short[keyCountForStatsTmpSize];
        for (var i = 0; i < keyCountForStatsTmpSize; i++) {
            keyCountForStatsTmp[i] = buffer.getShort();
        }
        x.setKeyCountForStatsTmp(keyCountForStatsTmp);

        var recordXBytesArraySize = buffer.getInt();
        if (recordXBytesArraySize != 0) {
            var recordXBytesArray = new byte[recordXBytesArraySize][];
            for (var i = 0; i < recordXBytesArraySize; i++) {
                var recordXBytesLength = buffer.getInt();
                if (recordXBytesLength > 0) {
                    var recordXBytes = new byte[recordXBytesLength];
                    buffer.get(recordXBytes);
                    recordXBytesArray[i] = recordXBytes;
                }
            }
            x.setRecordXBytesArray(recordXBytesArray);
        }

        var sharedBytesListBySplitIndexSize = buffer.getInt();
        if (sharedBytesListBySplitIndexSize != 0) {
            var sharedBytesListBySplitIndex = new byte[sharedBytesListBySplitIndexSize][];
            for (var i = 0; i < sharedBytesListBySplitIndexSize; i++) {
                var sharedBytesLength = buffer.getInt();
                if (sharedBytesLength > 0) {
                    var sharedBytes = new byte[sharedBytesLength];
                    buffer.get(sharedBytes);
                    sharedBytesListBySplitIndex[i] = sharedBytes;
                }
            }
            x.setSharedBytesListBySplitIndex(sharedBytesListBySplitIndex);
        }

        var oneWalGroupSeqArrayBySplitIndexSize = buffer.getInt();
        var oneWalGroupSeqArrayBySplitIndex = new long[oneWalGroupSeqArrayBySplitIndexSize];
        for (var i = 0; i < oneWalGroupSeqArrayBySplitIndexSize; i++) {
            oneWalGroupSeqArrayBySplitIndex[i] = buffer.getLong();
        }
        x.setOneWalGroupSeqArrayBySplitIndex(oneWalGroupSeqArrayBySplitIndex);

        var splitNumberAfterPutLength = buffer.getInt();
        var splitNumberAfterPut = new byte[splitNumberAfterPutLength];
        buffer.get(splitNumberAfterPut);
        x.setSplitNumberAfterPut(splitNumberAfterPut);

        var updatedChunkSegmentFlagWithSeqMapSize = buffer.getInt();
        for (var i = 0; i < updatedChunkSegmentFlagWithSeqMapSize; i++) {
            var segmentIndex = buffer.getInt();
            var flagByte = buffer.get();
            var seq = buffer.getLong();
            x.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, flagByte, seq);
        }

        var updatedChunkSegmentBytesMapSize = buffer.getInt();
        for (var i = 0; i < updatedChunkSegmentBytesMapSize; i++) {
            var segmentIndex = buffer.getInt();
            var bytesLength = buffer.getInt();
            var bytes = new byte[bytesLength];
            buffer.get(bytes);
            x.putUpdatedChunkSegmentBytes(segmentIndex, bytes);
        }

        x.setChunkSegmentIndexAfterPersist(buffer.getInt());
        x.setChunkMergedSegmentIndexEndLastTime(buffer.getInt());
        x.setLastSegmentSeq(buffer.getLong());

        if (encodedLength != x.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return x;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);

        var keyLoader = oneSlot.getKeyLoader();
        keyLoader.updateKeyCountBatch(walGroupIndex, beginBucketIndex, keyCountForStatsTmp);

        if (recordXBytesArray != null) {
            keyLoader.updateRecordXBytesArray(recordXBytesArray);
        }

        if (sharedBytesListBySplitIndex != null) {
            keyLoader.writeSharedBytesList(sharedBytesListBySplitIndex, beginBucketIndex);
        }

        for (int splitIndex = 0; splitIndex < oneWalGroupSeqArrayBySplitIndex.length; splitIndex++) {
            var seq = oneWalGroupSeqArrayBySplitIndex[splitIndex];
            if (seq != 0L) {
                keyLoader.setMetaOneWalGroupSeq((byte) splitIndex, beginBucketIndex, seq);
            }
        }

        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(beginBucketIndex, splitNumberAfterPut);

        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            var segmentIndex = entry.getKey();
            var flagByte = entry.getValue().flagByte;
            var seq = entry.getValue().seq;
            // RandAccessFile use os page cache, perf ok
            oneSlot.setSegmentMergeFlag(segmentIndex, flagByte, seq, walGroupIndex);
        }

        var chunk = oneSlot.getChunk();
        for (var entry : updatedChunkSegmentBytesMap.entrySet()) {
            var segmentIndex = entry.getKey();
            var bytes = entry.getValue();
            chunk.writeSegmentToTargetSegmentIndex(bytes, segmentIndex);
        }

        oneSlot.setMetaChunkSegmentIndexInt(chunkSegmentIndexAfterPersist, true);

        if (chunkMergedSegmentIndexEndLastTime != Chunk.NO_NEED_MERGE_SEGMENT_INDEX) {
            chunk.setMergedSegmentIndexEndLastTimeAfterSlaveCatchUp(chunkMergedSegmentIndexEndLastTime);
        }

        if (lastSegmentSeq != 0L) {
            replPair.setSlaveCatchUpLastSeq(lastSegmentSeq);
        }

        if (clearWalAfterApply) {
            var targetWal = oneSlot.getWalByBucketIndex(beginBucketIndex);
            if (isShortValue) {
                targetWal.clearShortValues();
            } else {
                targetWal.clearValues();
            }
        }
    }
}
