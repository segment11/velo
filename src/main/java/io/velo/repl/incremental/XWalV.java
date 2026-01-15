package io.velo.repl.incremental;

import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
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

    /**
     * Retrieves the WAL value associated with this operation.
     *
     * @return the WAL value
     */
    public Wal.V getV() {
        return v;
    }

    /**
     * Checks if the value is short.
     *
     * @return true if the value is short, false otherwise
     */
    public boolean isValueShort() {
        return isValueShort;
    }

    /**
     * Constructs a new XWalV object with the specified WAL value, value short flag, and offset.
     *
     * @param v            the WAL value associated with the operation
     * @param isValueShort whether the value is short
     */
    public XWalV(Wal.V v, boolean isValueShort) {
        this.v = v;
        this.isValueShort = isValueShort;
    }

    /**
     * Constructs a new XWalV object with the specified WAL value, using default values for isValueShort and offset.
     * This constructor is intended for testing purposes.
     *
     * @param v the WAL value associated with the operation
     */
    @TestOnly
    public XWalV(Wal.V v) {
        this(v, true);
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public BinlogContent.Type type() {
        return BinlogContent.Type.wal;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 1 byte for is value short
        // 8 bytes for seq, 4 bytes for bucket index, 8 bytes for key hash, 8 bytes for expire at, 4 bytes for sp type
        // 2 bytes for key length, key bytes, 4 bytes for cv encoded length, cv encoded bytes
        return 1 + 4 + 1 + 8 + 4 + 8 + 8 + 4 + 2 + v.key().length() + 4 + v.cvEncoded().length;
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
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XWalV object
     * @throws IllegalStateException If the key length is invalid or the encoded length does not match the expected length.
     */
    public static XWalV decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var isValueShort = buffer.get() == 1;
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
        var r = new XWalV(v, isValueShort);
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
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var key = v.key();
        var s = BaseCommand.slot(key, ConfForGlobal.slotNumber);
        // bucket index may be changed
        var v2 = new Wal.V(v.seq(), s.bucketIndex(), v.keyHash(), v.expireAt(), v.spType(), key, v.cvEncoded(), false);

        var oneSlot = localPersist.oneSlot(s.slot());
        var walGroupIndex = Wal.calcWalGroupIndex(s.bucketIndex());
        var targetWal = oneSlot.getWalByGroupIndex(walGroupIndex);
        oneSlot.asyncExecute(() -> {
            var putResult = targetWal.put(isValueShort, key, v2);
            if (putResult.needPersist()) {
                oneSlot.doPersist(walGroupIndex, key, putResult);
            }
        });

        replPair.setSlaveCatchUpLastSeq(v.seq());
    }
}