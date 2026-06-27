package io.velo.repl.incremental;

import io.activej.promise.Promise;
import io.velo.ConfForGlobal;
import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a flush operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XFlush implements BinlogContent {
    private final LocalPersist localPersist = LocalPersist.getInstance();

    private static final Logger log = LoggerFactory.getLogger(XFlush.class);

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.flush;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        return 1 + 4;
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

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XFlush object
     * @throws IllegalStateException If the encoded length does not match the expected length.
     */
    public static XFlush decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var r = new XFlush();
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }

        return r;
    }

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method flushes the specified slot in the local storage, ensuring that all pending changes are written.
     * <p>
     * In 2N scale-up mode, FLUSH is rejected because a master {@code FLUSHALL} produces N independent
     * un-coordinated XFlush entries; a lagging stream's flush would wipe a leading stream's post-flush
     * writes. See {@code docs/plans/2026-06-25-master-n-slave-2n-replication.md} Task 6.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        if (localPersist.isAsSlaveScaleUp()) {
            throw new IllegalStateException("Repl 2N flush unsupported until cross-stream barrier exists");
        }
        log.warn("Repl slave apply one slot flush, !!!, slot={}", slot);
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.flush();
        log.warn("Repl slave apply one slot flush done, !!!, slot={}", slot);
    }

    /**
     * In 2N scale-up mode, throws to reject the flush (stuck-but-safe: the stream stalls at this offset,
     * the read gate stays closed, no data loss). In equal-slot mode, delegates to {@link #apply}.
     *
     * @param slot     the replication slot
     * @param replPair the repl pair
     * @return a completed promise in equal-slot mode; never returns normally in scale-up mode
     */
    @Override
    public Promise<Void> applyAsync(short slot, ReplPair replPair) {
        if (localPersist.isAsSlaveScaleUp()) {
            throw new IllegalStateException("Repl 2N flush unsupported until cross-stream barrier exists");
        }
        apply(slot, replPair);
        return Promise.complete();
    }
}
