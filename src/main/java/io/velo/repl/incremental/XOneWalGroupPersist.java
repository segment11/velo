package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * Represents the binary log content for persisting a single WAL (Write-Ahead Log) group in the Velo replication system.
 * This class encapsulates all the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XOneWalGroupPersist implements BinlogContent {
    private boolean isShortValue;
    private boolean clearWalAfterApply;
    private final int walGroupIndex;

    /**
     * Sets whether the value to be persisted is a short value.
     * This method is intended for testing purposes only.
     *
     * @param isShortValue whether the value is a short value
     */
    @TestOnly
    public void setShortValue(boolean isShortValue) {
        this.isShortValue = isShortValue;
    }

    /**
     * Sets whether the WAL should be cleared after applying the operation.
     * This method is intended for testing purposes only.
     *
     * @param clearWalAfterApply whether the WAL should be cleared
     */
    @TestOnly
    public void setClearWalAfterApply(boolean clearWalAfterApply) {
        this.clearWalAfterApply = clearWalAfterApply;
    }

    /**
     * Constructs a new XOneWalGroupPersist object with the specified parameters.
     *
     * @param isShortValue       whether the value to be persisted is a short value
     * @param clearWalAfterApply whether the WAL should be cleared after applying the operation
     * @param walGroupIndex      the index of the WAL group to be persisted
     */
    public XOneWalGroupPersist(boolean isShortValue, boolean clearWalAfterApply, int walGroupIndex) {
        this.isShortValue = isShortValue;
        this.clearWalAfterApply = clearWalAfterApply;
        this.walGroupIndex = walGroupIndex;
    }

    private int beginBucketIndex;

    /**
     * Sets the beginning bucket index for the WAL group.
     *
     * @param beginBucketIndex the beginning bucket index
     */
    public void setBeginBucketIndex(int beginBucketIndex) {
        this.beginBucketIndex = beginBucketIndex;
    }

    private short[] keyCountForStatsTmp;

    /**
     * Sets the array of key counts for statistical purposes.
     *
     * @param keyCountForStatsTmp the array of key counts
     */
    public void setKeyCountForStatsTmp(short[] keyCountForStatsTmp) {
        this.keyCountForStatsTmp = keyCountForStatsTmp;
    }

    private byte[][] recordXBytesArray;

    /**
     * Sets the array of record bytes.
     *
     * @param recordXBytesArray the array of record bytes
     */
    public void setRecordXBytesArray(byte[][] recordXBytesArray) {
        this.recordXBytesArray = recordXBytesArray;
    }

    private byte[][] sharedBytesListBySplitIndex;

    /**
     * Sets the array of shared bytes by split index.
     *
     * @param sharedBytesListBySplitIndex the array of shared bytes
     */
    public void setSharedBytesListBySplitIndex(byte[][] sharedBytesListBySplitIndex) {
        this.sharedBytesListBySplitIndex = sharedBytesListBySplitIndex;
    }

    private long[] oneWalGroupSeqArrayBySplitIndex;

    /**
     * Sets the array of WAL group sequences by split index.
     *
     * @param oneWalGroupSeqArrayBySplitIndex the array of WAL group sequences
     */
    public void setOneWalGroupSeqArrayBySplitIndex(long[] oneWalGroupSeqArrayBySplitIndex) {
        this.oneWalGroupSeqArrayBySplitIndex = oneWalGroupSeqArrayBySplitIndex;
    }

    private byte[] splitNumberAfterPut;

    /**
     * Sets the split number after the put operation.
     *
     * @param splitNumberAfterPut the split number
     */
    public void setSplitNumberAfterPut(byte[] splitNumberAfterPut) {
        this.splitNumberAfterPut = splitNumberAfterPut;
    }

    /**
     * Record to hold segment flag and sequence number.
     *
     * @param flagByte the flag byte
     * @param seq      the sequence number
     */
    record SegmentFlagWithSeq(byte flagByte, long seq) {
    }

    private final TreeMap<Integer, SegmentFlagWithSeq> updatedChunkSegmentFlagWithSeqMap = new TreeMap<>();

    /**
     * Adds a segment flag with sequence number to the map.
     *
     * @param segmentIndex the segment index
     * @param flagByte     the flag byte
     * @param seq          the sequence number
     */
    public void putUpdatedChunkSegmentFlagWithSeq(int segmentIndex, byte flagByte, long seq) {
        updatedChunkSegmentFlagWithSeqMap.put(segmentIndex, new SegmentFlagWithSeq(flagByte, seq));
    }

    private final TreeMap<Integer, byte[]> updatedChunkSegmentBytesMap = new TreeMap<>();

    /**
     * Adds segment bytes to the map.
     *
     * @param segmentIndex the segment index
     * @param bytes        the segment bytes
     */
    public void putUpdatedChunkSegmentBytes(int segmentIndex, byte[] bytes) {
        updatedChunkSegmentBytesMap.put(segmentIndex, bytes);
    }

    private int chunkSegmentIndexAfterPersist;

    /**
     * Sets the chunk segment index after persisting.
     *
     * @param chunkSegmentIndexAfterPersist the chunk segment index
     */
    public void setChunkSegmentIndexAfterPersist(int chunkSegmentIndexAfterPersist) {
        this.chunkSegmentIndexAfterPersist = chunkSegmentIndexAfterPersist;
    }

    private long lastSegmentSeq;

    /**
     * Sets the last segment sequence number.
     *
     * @param lastSegmentSeq the last segment sequence number
     */
    public void setLastSegmentSeq(long lastSegmentSeq) {
        this.lastSegmentSeq = lastSegmentSeq;
    }

    /**
     * Record to hold information for merging a group by WAL group.
     *
     * @param walGroupIndex     the index of the WAL group
     * @param beginSegmentIndex the beginning segment index
     * @param segmentCount      the number of segments
     */
    public record ToFindForMergeGroupByWalGroup(int walGroupIndex, int beginSegmentIndex, short segmentCount) {
    }

    private ToFindForMergeGroupByWalGroup toFindForMergeGroupByWalGroup;

    /**
     * Sets the information for merging a group by WAL group.
     *
     * @param toFindForMergeGroupByWalGroup the merge group information
     */
    public void setToFindForMergeGroupByWalGroup(ToFindForMergeGroupByWalGroup toFindForMergeGroupByWalGroup) {
        this.toFindForMergeGroupByWalGroup = toFindForMergeGroupByWalGroup;
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
        // 4 bytes for last segment seq
        n += 4 + 8;

        // 4 bytes for wal group index, 4 bytes for begin segment index, 2 bytes for segment count
        n += 4 + 4 + 2;
        return n;
    }

    /**
     * Encodes this binlog content into a byte array, including the type byte and length check.
     *
     * @return the byte array representation of this binlog content
     */
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
        buffer.putLong(lastSegmentSeq);

        buffer.putInt(toFindForMergeGroupByWalGroup.walGroupIndex);
        buffer.putInt(toFindForMergeGroupByWalGroup.beginSegmentIndex);
        buffer.putShort(toFindForMergeGroupByWalGroup.segmentCount);

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XOneWalGroupPersist object
     * @throws IllegalStateException If the encoded length does not match the expected length.
     */
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
        x.setLastSegmentSeq(buffer.getLong());

        x.setToFindForMergeGroupByWalGroup(new ToFindForMergeGroupByWalGroup(
                buffer.getInt(),
                buffer.getInt(),
                buffer.getShort()
        ));

        if (encodedLength != x.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return x;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
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

        if (lastSegmentSeq != 0L) {
            replPair.setSlaveCatchUpLastSeq(lastSegmentSeq);
        }

        oneSlot.getMetaChunkSegmentFlagSeq().markPersistedSegmentIndexToTargetWalGroup(
                toFindForMergeGroupByWalGroup.beginSegmentIndex,
                toFindForMergeGroupByWalGroup.segmentCount,
                toFindForMergeGroupByWalGroup.segmentCount);

        if (clearWalAfterApply) {
            var targetWal = oneSlot.getWalByBucketIndex(beginBucketIndex);
            if (isShortValue) {
                targetWal.clearShortValues();
            } else {
                targetWal.clearValues();
            }
        }
    }

    @Override
    public boolean isSkipWhenAllSlavesInCatchUpState() {
        return true;
    }
}
