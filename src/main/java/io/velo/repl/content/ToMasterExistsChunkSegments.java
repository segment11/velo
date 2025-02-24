package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

import java.nio.ByteBuffer;

/**
 * Represents the content of a REPL message of type {@link io.velo.repl.ReplType#exists_chunk_segments}
 * sent from a slave to a master. This message contains information about the existence of chunk segments,
 * including the starting segment index, the count of segments, and any associated meta data.
 */
public class ToMasterExistsChunkSegments implements ReplContent {

    private final int beginSegmentIndex;
    private final int segmentCount;
    private final byte[] metaBytes;

    /**
     * Constructs a new {@link ToMasterExistsChunkSegments} message.
     *
     * @param beginSegmentIndex The index of the first segment in the range being checked.
     * @param segmentCount      The number of segments being checked.
     * @param metaBytes         Additional meta data associated with the segments.
     */
    public ToMasterExistsChunkSegments(int beginSegmentIndex, int segmentCount, byte[] metaBytes) {
        this.beginSegmentIndex = beginSegmentIndex;
        this.segmentCount = segmentCount;
        this.metaBytes = metaBytes;
    }

    /**
     * Encodes the content of this message to the given {@link ByteBuf}.
     * <p>
     * The encoding format is as follows:
     * - First 4 bytes: the index of the first segment (beginSegmentIndex).
     * - Next 4 bytes: the count of segments (segmentCount).
     * - Following bytes: the meta data (metaBytes).
     *
     * @param toBuf The buffer to which the encoded message content will be written.
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeInt(beginSegmentIndex);
        toBuf.writeInt(segmentCount);
        toBuf.write(metaBytes);
    }

    /**
     * Returns the length of the encoded message content in bytes.
     * <p>
     * The length is the sum of:
     * - 4 bytes for the beginSegmentIndex.
     * - 4 bytes for the segmentCount.
     * - The length of the metaBytes array.
     *
     * @return The length of the encoded message content in bytes.
     */
    @Override
    public int encodeLength() {
        return 4 + 4 + metaBytes.length;
    }

    /**
     * Checks if the meta data from a slave matches the meta data expected by a master for the same batch
     * of chunk segments.
     *
     * @param metaBytesMaster       The meta data expected by the master.
     * @param contentBytesFromSlave The content bytes received from the slave, which includes the
     *                              beginSegmentIndex, segmentCount, and metaBytes.
     * @return true if the meta data matches; false otherwise.
     * @throws IllegalArgumentException if the length of the content bytes from the slave is not as expected.
     */
    public static boolean isSlaveSameForThisBatch(byte[] metaBytesMaster, byte[] contentBytesFromSlave) {
        // content bytes include begin segment index int, segment count int
        if (contentBytesFromSlave.length != metaBytesMaster.length + 8) {
            throw new IllegalArgumentException("Repl exists chunk segments meta from slave meta length is not equal to master meta length");
        }

        var bufferFromSlave = ByteBuffer.wrap(contentBytesFromSlave, 8, contentBytesFromSlave.length - 8).slice();
        var bufferMaster = ByteBuffer.wrap(metaBytesMaster);

        return bufferFromSlave.equals(bufferMaster);
    }
}