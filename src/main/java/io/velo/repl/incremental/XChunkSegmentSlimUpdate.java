package io.velo.repl.incremental;

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
    private final byte[] segmentBytesSlim;

    /**
     * Constructs a new XChunkSegmentSlimUpdate object with the specified segment index and slim segment bytes.
     *
     * @param segmentIndex     The index of the segment to update.
     * @param segmentBytesSlim The byte array representing the slim segment.
     */
    public XChunkSegmentSlimUpdate(int segmentIndex, byte[] segmentBytesSlim) {
        this.segmentIndex = segmentIndex;
        this.segmentBytesSlim = segmentBytesSlim;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return The type of this binlog content.
     */
    @Override
    public Type type() {
        return Type.chunk_segment_slim_update;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return The total number of bytes required for encoding.
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for segment index int, 4 bytes for encoded length for check
        return 1 + 4 + 4 + segmentBytesSlim.length;
    }

    /**
     * Encodes this binlog content into a byte array, including the type byte and length check.
     *
     * @return The byte array representation of this binlog content.
     */
    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(segmentIndex);
        buffer.putInt(segmentBytesSlim.length);
        buffer.put(segmentBytesSlim);
        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the encoded binlog content.
     * @return The decoded XChunkSegmentSlimUpdate object.
     */
    public static XChunkSegmentSlimUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var segmentIndex = buffer.getInt();
        var encodedLength = buffer.getInt();
        var bb = new byte[encodedLength];
        buffer.get(bb);
        return new XChunkSegmentSlimUpdate(segmentIndex, bb);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the segment in the local storage with the new slim segment bytes.
     *
     * @param slot     The replication slot to which this content is applied.
     * @param replPair The repl pair associated with this replication session.
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getChunk().writeSegmentsFromMasterExistsOrAfterSegmentSlim(segmentBytesSlim, segmentIndex, 1);
    }
}