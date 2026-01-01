package io.velo.repl.incremental;

import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents a binary log content object that slave update catchup seq and master timestamp.
 * This is used in the replication process to handle time diff between master and slave.
 */
public class XUpdateSeq implements BinlogContent {
    /**
     * The sequence number that master last updated.
     */
    private final long seq;

    /**
     * The time in milliseconds that master last updated.
     */
    private final long timeMillis;

    /**
     * Constructs an XUpdateSeq object.
     *
     * @param seq        the sequence number that master last updated
     * @param timeMillis the time in milliseconds that master last updated
     */
    public XUpdateSeq(long seq, long timeMillis) {
        this.seq = seq;
        this.timeMillis = timeMillis;
    }

    /**
     * Returns the sequence number that master last updated.
     *
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Returns the time in milliseconds that master last updated.
     *
     * @return the time in milliseconds
     */
    public long getTimeMillis() {
        return timeMillis;
    }

    /**
     * Returns the type of this binlog content, which is {@link Type#update_seq}.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.update_seq;
    }

    /**
     * Returns the length of this object when encoded, including the type byte.
     *
     * @return the encoded length
     */
    @Override
    public int encodedLength() {
        // 1 byte for type
        // 8 bytes for seq
        // 4 bytes for time millis
        return 1 + 8 + 8;
    }

    /**
     * Encodes this object into a byte array, including the type byte.
     *
     * @return the encoded byte array
     */
    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putLong(seq);
        buffer.putLong(timeMillis);

        return bytes;
    }

    /**
     * Decodes an XUpdateSeq object from a ByteBuffer that has already read the type byte.
     *
     * @param buffer the ByteBuffer containing the encoded XUpdateSeq data
     * @return the new XUpdateSeq instance decoded from the buffer
     */
    public static XUpdateSeq decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var seq = buffer.getLong();
        var timeMillis = buffer.getLong();
        return new XUpdateSeq(seq, timeMillis);
    }

    /**
     * Logger for logging messages related to this class.
     */
    private static final Logger log = LoggerFactory.getLogger(XUpdateSeq.class);

    /**
     * Applies this skip apply instruction to the specified slot and replication pair.
     * This logs a warning, updates the slot's chunk segment index, and sets the last sequence caught up by the slave.
     *
     * @param slot     the slot number to apply the skip instruction to
     * @param replPair the replication pair involved in the replication process
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        replPair.setSlaveCatchUpLastSeq(seq);
        replPair.setSlaveCatchUpLastTimeMillis(timeMillis);
    }
}