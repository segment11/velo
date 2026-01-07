package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a Write-Ahead Log (WAL) rewrite operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XWalRewrite implements BinlogContent {
    private final boolean isValueShort;
    private final int groupIndex;
    private final byte[] writeBytes;

    /**
     * Constructs a new XWalRewrite instance.
     *
     * @param isValueShort whether the value is short
     * @param groupIndex   the WAL group index
     * @param writeBytes   the bytes representing the WAL rewrite operation
     */
    public XWalRewrite(boolean isValueShort, int groupIndex, byte[] writeBytes) {
        this.isValueShort = isValueShort;
        this.groupIndex = groupIndex;
        this.writeBytes = writeBytes;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.wal_rewrite;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 1 byte for is value short, 4 bytes as int for group index, 4 bytes as int for bytes length
        return 1 + 4 + 1 + 4 + 4 + writeBytes.length;
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
        buffer.put(isValueShort ? (byte) 1 : (byte) 0);
        buffer.putInt(groupIndex);
        buffer.putInt(writeBytes.length);
        buffer.put(writeBytes);

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XWalRewrite object
     * @throws IllegalStateException If the key length is invalid or the encoded length does not match the expected length.
     */
    public static XWalRewrite decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var isValueShort = buffer.get() == 1;
        var groupIndex = buffer.getInt();
        var writeBytesLength = buffer.getInt();
        var writeBytes = new byte[writeBytesLength];
        buffer.get(writeBytes);

        var r = new XWalRewrite(isValueShort, groupIndex, writeBytes);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the local WAL.
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
        var targetWal = oneSlot.getWalByGroupIndex(groupIndex);
        targetWal.rewriteOneGroupFromMaster(isValueShort, writeBytes);
    }

    @Override
    public boolean isSkipWhenAllSlavesInCatchUpState() {
        return true;
    }
}