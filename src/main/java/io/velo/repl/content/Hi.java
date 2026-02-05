package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.Binlog;
import io.velo.repl.ReplContent;

/**
 * Initialization handshake message from master to slave, includes master/slave UUIDs and binlog position info.
 */
public class Hi implements ReplContent {
    private final long masterUuid;
    private final long slaveUuid;
    private final Binlog.FileIndexAndOffset currentFo;
    private final Binlog.FileIndexAndOffset earliestFo;
    private final int currentSegmentIndex;

    public Hi(long masterUuid, long slaveUuid, Binlog.FileIndexAndOffset currentFo,
              Binlog.FileIndexAndOffset earliestFo, int currentSegmentIndex) {
        this.masterUuid = masterUuid;
        this.slaveUuid = slaveUuid;
        this.currentFo = currentFo;
        this.earliestFo = earliestFo;
        this.currentSegmentIndex = currentSegmentIndex;
    }

    /**
     * Encodes the content of this message into the provided {@link ByteBuf}.
     * Encoding format: 8 (master UUID) + 8 (slave UUID) + 4/8 (current file index/offset) + 4/8 (earliest file index/offset) + 4 (current segment index) + 2 + 18 (ReplProperties)
     *
     * @param toBuf the buffer to which the message content will be written
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(masterUuid);
        toBuf.writeLong(slaveUuid);
        toBuf.writeInt(currentFo.fileIndex());
        toBuf.writeLong(currentFo.offset());
        toBuf.writeInt(earliestFo.fileIndex());
        toBuf.writeLong(earliestFo.offset());
        toBuf.writeInt(currentSegmentIndex);

        Hello.writeReplProperties(toBuf);
    }

    /**
     * Calculates and returns the total length in bytes required to encode this message.
     * The length is computed as the sum of:
     * - 8 bytes for the master UUID
     * - 8 bytes for the slave UUID
     * - 4 bytes for the current file index
     * - 8 bytes for the current offset
     * - 4 bytes for the earliest file index
     * - 8 bytes for the earliest offset
     * - 4 bytes for the current segment index
     * - 2 bytes for slot number
     * - 4 + 4 + 1 + 4 + 4 for ReplProperties
     *
     * @return the length in bytes required to encode this message
     */
    @Override
    public int encodeLength() {
        return 8 + 8 + 4 + 8 + 4 + 8 + 4 + 2 + 18;
    }
}