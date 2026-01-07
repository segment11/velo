package io.velo.repl.incremental;

import io.velo.ConfForGlobal;
import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for updating a chunk segment in a slim format in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding segment update during replication.
 */
public class XChunkSegmentSlimUpdate implements BinlogContent {
    private final int segmentIndex;
    private final int valueBytesLength;
    private final byte[] segmentBytesSlim;

    /**
     * Constructs a new XChunkSegmentSlimUpdate object with the specified segment index and slim segment bytes.
     *
     * @param segmentIndex     the index of the segment to update
     * @param valueBytesLength the value bytes length for fill ratio calc, for pure memory v2
     * @param segmentBytesSlim the byte array representing the slim segment
     */
    public XChunkSegmentSlimUpdate(int segmentIndex, int valueBytesLength, byte[] segmentBytesSlim) {
        this.segmentIndex = segmentIndex;
        this.segmentBytesSlim = segmentBytesSlim;
        this.valueBytesLength = valueBytesLength;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.chunk_segment_slim_update;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for segment index int, 4 bytes for value bytes length, 4 bytes for encoded length for check
        return 1 + 4 + 4 + 4 + segmentBytesSlim.length;
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
        buffer.putInt(segmentIndex);
        buffer.putInt(valueBytesLength);
        buffer.putInt(segmentBytesSlim.length);
        buffer.put(segmentBytesSlim);
        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XChunkSegmentSlimUpdate object
     */
    public static XChunkSegmentSlimUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var segmentIndex = buffer.getInt();
        var valueBytesLength = buffer.getInt();
        var encodedLength = buffer.getInt();
        var bb = new byte[encodedLength];
        buffer.get(bb);
        return new XChunkSegmentSlimUpdate(segmentIndex, valueBytesLength, bb);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the segment in the local storage with the new slim segment bytes.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        if (replPair.isRedoSet()) {
            return;
        }

        var segmentRealLengths = new int[1];
        segmentRealLengths[0] = segmentBytesSlim.length;

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getChunk().writeSegmentsFromMasterExistsOrAfterSegmentSlim(segmentBytesSlim, segmentIndex, 1, segmentRealLengths);

        if (ConfForGlobal.pureMemoryV2) {
            oneSlot.getKeyLoader().getMetaChunkSegmentFillRatio().set(segmentIndex, valueBytesLength);
        }
    }
}