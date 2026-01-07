package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * Represents the binary log content for updating chunk segment flags in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding flag updates during replication.
 */
public class XChunkSegmentFlagUpdate implements BinlogContent {
    private final TreeMap<Integer, XOneWalGroupPersist.SegmentFlagWithSeq> updatedChunkSegmentFlagWithSeqMap = new TreeMap<>();

    /**
     * Adds an updated segment flag and sequence to the map.
     *
     * @param segmentIndex the index of the segment to update
     * @param flagByte     the new flag byte for the segment
     * @param seq          the sequence number associated with the update
     */
    public void putUpdatedChunkSegmentFlagWithSeq(int segmentIndex, Byte flagByte, long seq) {
        updatedChunkSegmentFlagWithSeqMap.put(segmentIndex, new XOneWalGroupPersist.SegmentFlagWithSeq(flagByte, seq));
    }

    /**
     * Checks if the map of updated chunk segment flags is empty.
     *
     * @return true if the map is empty, false otherwise
     */
    public boolean isEmpty() {
        return updatedChunkSegmentFlagWithSeqMap.isEmpty();
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.chunk_segment_flag_update;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        var n = 1 + 4;
        // 4 bytes for updated chunk segment flag with seq map size
        n += 4;
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            // 4 bytes for segment index, 1 byte for flag, 8 bytes for seq
            n += 4 + 1 + 8;
        }
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

        buffer.putInt(updatedChunkSegmentFlagWithSeqMap.size());
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.put(entry.getValue().flagByte());
            buffer.putLong(entry.getValue().seq());
        }
        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XChunkSegmentFlagUpdate object
     * @throws IllegalStateException If the encoded length does not match the expected length.
     */
    public static XChunkSegmentFlagUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();
        var x = new XChunkSegmentFlagUpdate();

        var updatedChunkSegmentFlagWithSeqMapSize = buffer.getInt();
        for (var i = 0; i < updatedChunkSegmentFlagWithSeqMapSize; i++) {
            var segmentIndex = buffer.getInt();
            var flagByte = buffer.get();
            var seq = buffer.getLong();
            x.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, flagByte, seq);
        }

        if (encodedLength != x.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return x;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the segment flags in the local storage with the new flags and sequence numbers.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        if (replPair.isRedoSet()) {
            return;
        }

        var oneSlot = localPersist.oneSlot(slot);

        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            var segmentIndex = entry.getKey();
            var flagByte = entry.getValue().flagByte();
            var seq = entry.getValue().seq();
            // RandAccessFile use os page cache, perf ok
            oneSlot.updateSegmentMergeFlag(segmentIndex, flagByte, seq);
        }
    }
}