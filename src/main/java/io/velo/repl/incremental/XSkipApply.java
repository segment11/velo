package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents a binary log content object that skips the application of a certain sequence.
 * This is used in the replication process to handle scenarios where certain changes can be skipped.
 */
public class XSkipApply implements BinlogContent {

    /**
     * The sequence number that this skip apply instruction refers to.
     */
    private final long seq;

    /**
     * The current segment index of the chunk that this skip apply instruction refers to.
     */
    private final int chunkCurrentSegmentIndex;

    /**
     * Constructs a new XSkipApply instance with the given sequence number and chunk segment index.
     *
     * @param seq                      the sequence number to skip
     * @param chunkCurrentSegmentIndex the current segment index of the chunk to skip
     */
    public XSkipApply(long seq, int chunkCurrentSegmentIndex) {
        this.seq = seq;
        this.chunkCurrentSegmentIndex = chunkCurrentSegmentIndex;
    }

    /**
     * Returns the sequence number associated with this skip apply instruction.
     *
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Returns the current segment index of the chunk associated with this skip apply instruction.
     *
     * @return the chunk segment index
     */
    public int getChunkCurrentSegmentIndex() {
        return chunkCurrentSegmentIndex;
    }

    /**
     * Returns the type of this binlog content, which is {@link Type#skip_apply}.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.skip_apply;
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
        // 4 bytes for chunk segment index
        return 1 + 8 + 4;
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
        buffer.putInt(chunkCurrentSegmentIndex);

        return bytes;
    }

    /**
     * Decodes an XSkipApply object from a ByteBuffer that has already read the type byte.
     *
     * @param buffer the ByteBuffer containing the encoded XSkipApply data
     * @return the new XSkipApply instance decoded from the buffer
     */
    public static XSkipApply decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var seq = buffer.getLong();
        var chunkCurrentSegmentIndex = buffer.getInt();
        return new XSkipApply(seq, chunkCurrentSegmentIndex);
    }

    /**
     * Logger for logging messages related to this class.
     */
    private static final Logger log = LoggerFactory.getLogger(XSkipApply.class);

    /**
     * The LocalPersist instance used to interact with the local data store.
     */
    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this skip apply instruction to the specified slot and replication pair.
     * This logs a warning, updates the slot's chunk segment index, and sets the last sequence caught up by the slave.
     *
     * @param slot     the slot index to apply the skip instruction to
     * @param replPair the replication pair involved in the replication process
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl skip apply, seq={}, chunk segment index={}",
                seq, chunkCurrentSegmentIndex);

        var oneSlot = localPersist.oneSlot(slot);
        if (!replPair.isRedoSet()) {
            oneSlot.setMetaChunkSegmentIndexInt(chunkCurrentSegmentIndex);
        }

        replPair.setSlaveCatchUpLastSeq(seq);
    }
}