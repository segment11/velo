package io.velo.persist;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.VisibleForTesting;

import static io.velo.CompressedValue.NO_EXPIRE;

/**
 * Metadata for a persistent value including location, type, and expiration.
 */
public class PersistValueMeta {
    /** Fixed encoded length in bytes. */
    @VisibleForTesting
    static final int ENCODED_LENGTH = 2 + 1 + 1 + 4 + 4;

    /**
     * @param bytes the byte array to check
     * @return true if the byte array represents a PersistValueMeta
     */
    public static boolean isPvm(byte[] bytes) {
        return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
    }

    /** Redis type for scan filter. */
    byte shortType = KeyLoader.typeAsByteString;
    /** Sub-block index within a segment. */
    byte subBlockIndex;
    /** Segment index where the value is stored. */
    int segmentIndex;
    /** Offset within the segment. */
    int segmentOffset;
    /** Expiration time in milliseconds since epoch, or NO_EXPIRE. */
    long expireAt = NO_EXPIRE;
    /** Unique sequence number. */
    long seq;
    /** Length of value bytes. */
    int valueBytesLength;
    /** Associated key. */
    String key;
    /** Key hash for quick comparison. */
    long keyHash;
    /** Bucket index. */
    int bucketIndex;
    /** Extended metadata bytes. */
    byte[] extendBytes;
    /** Whether created from a merge operation. */
    boolean isFromMerge;

    /**
     * @return the cost of storing this metadata entry in a key bucket
     */
    int cellCostInKeyBucket() {
        var valueLength = extendBytes != null ? extendBytes.length : ENCODED_LENGTH;
        if (valueLength > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Persist value meta extend bytes too long=" + valueLength);
        }
        return KeyBucket.KVMeta.calcCellCount((short) Wal.keyBytes(key).length, (byte) valueLength);
    }

    /**
     * @param segmentIndex  the segment index to check
     * @param subBlockIndex the sub-block index to check
     * @param segmentOffset the segment offset to check
     * @return true if this metadata corresponds to the specified location
     */
    boolean isTargetSegment(int segmentIndex, byte subBlockIndex, int segmentOffset) {
        return this.segmentIndex == segmentIndex && this.subBlockIndex == subBlockIndex && this.segmentOffset == segmentOffset;
    }

    /**
     * @return short string representation focusing on segment fields
     */
    public String shortString() {
        return "si=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
    }

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "shortType=" + shortType +
                ", segmentIndex=" + segmentIndex +
                ", subBlockIndex=" + subBlockIndex +
                ", segmentOffset=" + segmentOffset +
                ", expireAt=" + expireAt +
                '}';
    }

    /**
     * @return the byte array representation of this metadata
     */
    public byte[] encode() {
        var bytes = new byte[ENCODED_LENGTH];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeShort((short) 0);
        buf.writeByte(shortType);
        buf.writeByte(subBlockIndex);
        buf.writeInt(segmentIndex);
        buf.writeInt(segmentOffset);
        return bytes;
    }

    /**
     * @param bytes the byte array to decode
     * @return the PersistValueMeta instance represented by the byte array
     * @throws PersistValueMetaCorruptedException if any decoded field is out of its valid range
     */
    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        buf.readShort();
        pvm.shortType = buf.readByte();
        pvm.subBlockIndex = buf.readByte();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();

        if (pvm.subBlockIndex < 0 || pvm.subBlockIndex >= SegmentBatch.MAX_BLOCK_NUMBER) {
            throw new PersistValueMetaCorruptedException("subBlockIndex out of range: " + pvm.subBlockIndex);
        }
        if (pvm.segmentIndex < 0) {
            throw new PersistValueMetaCorruptedException("segmentIndex negative: " + pvm.segmentIndex);
        }
        if (pvm.segmentOffset < 0) {
            throw new PersistValueMetaCorruptedException("segmentOffset negative: " + pvm.segmentOffset);
        }
        return pvm;
    }
}
