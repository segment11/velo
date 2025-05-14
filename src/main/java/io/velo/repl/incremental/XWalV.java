package io.velo.repl.incremental;

import io.velo.CompressedValue;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a Write-Ahead Log (WAL) value operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XWalV implements BinlogContent {
    private final Wal.V v;
    private final boolean isValueShort;
    private final int offset;
    private final boolean isOnlyPut;

    /**
     * Retrieves the WAL value associated with this operation.
     *
     * @return The WAL value.
     */
    public Wal.V getV() {
        return v;
    }

    /**
     * Checks if the value is short.
     *
     * @return True if the value is short, false otherwise.
     */
    public boolean isValueShort() {
        return isValueShort;
    }

    /**
     * Retrieves the offset of the WAL value.
     *
     * @return The offset.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Checks if this is a put operation, not store to the target offset.
     *
     * @return True if this is a put operation, false otherwise.
     */
    public boolean isOnlyPut() {
        return isOnlyPut;
    }

    /**
     * Constructs a new XWalV object with the specified WAL value, value short flag, and offset.
     *
     * @param v            The WAL value associated with the operation.
     * @param isValueShort A flag indicating if the value is short.
     * @param offset       The offset of the WAL value.
     * @param isOnlyPut    A flag indicating if this is a put operation, not store to the target offset.
     */
    public XWalV(Wal.V v, boolean isValueShort, int offset, boolean isOnlyPut) {
        this.v = v;
        this.isValueShort = isValueShort;
        this.offset = offset;
        this.isOnlyPut = isOnlyPut;
    }

    /**
     * Constructs a new XWalV object with the specified WAL value, using default values for isValueShort and offset.
     * This constructor is intended for testing purposes.
     *
     * @param v The WAL value associated with the operation.
     */
    @TestOnly
    public XWalV(Wal.V v) {
        this(v, true, 0, false);
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return The type of this binlog content.
     */
    @Override
    public BinlogContent.Type type() {
        return BinlogContent.Type.wal;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return The total number of bytes required for encoding.
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 1 byte for is value short, 4 bytes as int for offset, 1 byte for is only put
        // 8 bytes for seq, 4 bytes for bucket index, 8 bytes for key hash, 8 bytes for expire at, 4 bytes for sp type
        // 2 bytes for key length, key bytes, 4 bytes for cv encoded length, cv encoded bytes
        return 1 + 4 + 1 + 4 + 1 + 8 + 4 + 8 + 8 + 4 + 2 + v.key().length() + 4 + v.cvEncoded().length;
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
        buffer.putInt(bytes.length);
        buffer.put(isValueShort ? (byte) 1 : (byte) 0);
        buffer.putInt(offset);
        buffer.put((byte) (isOnlyPut ? 1 : 0));
        buffer.putLong(v.seq());
        buffer.putInt(v.bucketIndex());
        buffer.putLong(v.keyHash());
        buffer.putLong(v.expireAt());
        buffer.putInt(v.spType());
        buffer.putShort((short) v.key().length());
        buffer.put(v.key().getBytes());
        buffer.putInt(v.cvEncoded().length);
        buffer.put(v.cvEncoded());

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the encoded binlog content.
     * @return The decoded XWalV object.
     * @throws IllegalStateException If the key length is invalid or the encoded length does not match the expected length.
     */
    public static XWalV decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var isValueShort = buffer.get() == 1;
        var offset = buffer.getInt();
        var isOnlyPut = buffer.get() == 1;
        var seq = buffer.getLong();
        var bucketIndex = buffer.getInt();
        var keyHash = buffer.getLong();
        var expireAt = buffer.getLong();
        var spType = buffer.getInt();
        var keyLength = buffer.getShort();

        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        var cvEncodedLength = buffer.getInt();
        var cvEncoded = new byte[cvEncodedLength];
        buffer.get(cvEncoded);

        var v = new Wal.V(seq, bucketIndex, keyHash, expireAt, spType, new String(keyBytes), cvEncoded, false);
        var r = new XWalV(v, isValueShort, offset, isOnlyPut);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the local WAL and sets the last sequence number of the slave.
     *
     * @param slot     The replication slot to which this content is applied.
     * @param replPair The repl pair associated with this replication session.
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByBucketIndex(v.bucketIndex());

        if (isOnlyPut) {
            targetWal.put(isValueShort, v.key(), v);
        } else {
            targetWal.putFromX(v, isValueShort, offset);
        }

        replPair.setSlaveCatchUpLastSeq(v.seq());
    }
}