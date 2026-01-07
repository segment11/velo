package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.Binlog;
import io.velo.repl.ReplContent;

/**
 * Represents a content type for sending an initialization or handshake message from a master to a slave.
 * This message includes the master's and slave's unique identifiers, as well as information about the current
 * and earliest file offsets and segment indices in the write-ahead log (WAL).
 * It corresponds to the {@link io.velo.repl.ReplType#hi} message type, which is sent by a master to a slave.
 */
public class Hi implements ReplContent {
    /**
     * The unique identifier for the master.
     */
    private final long masterUuid;

    /**
     * The unique identifier for the slave.
     */
    private final long slaveUuid;

    /**
     * The current file index and offset in the binlog file for the master.
     */
    private final Binlog.FileIndexAndOffset currentFo;

    /**
     * The earliest file index and offset in the binlog file for the master.
     */
    private final Binlog.FileIndexAndOffset earliestFo;

    /**
     * The current segment index in the binlog file for the master.
     */
    private final int currentSegmentIndex;

    /**
     * Constructs a new {@code Hi} message with the specified master UUID, slave UUID,
     * current file index and offset, earliest file index and offset, and current segment index.
     *
     * @param masterUuid          the unique identifier for the master
     * @param slaveUuid           the unique identifier for the slave
     * @param currentFo           the current file index and offset in the binlog file for the master
     * @param earliestFo          the earliest file index and offset in the binlog file for the master
     * @param currentSegmentIndex the current segment index in the binlog file for the master
     */
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
        return 8 + 8 + 4 + 8 + 4 + 8 + 4 + 2 + 17;
    }
}