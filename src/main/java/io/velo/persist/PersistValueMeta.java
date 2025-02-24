package io.velo.persist;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.VisibleForTesting;

import static io.velo.CompressedValue.NO_EXPIRE;

/**
 * Represents metadata for a persistent value stored in the system.
 * This class encapsulates information about the value's location, type,
 * expiration, and other relevant details needed for efficient storage and retrieval.
 */
public class PersistValueMeta {
    /**
     * The fixed encoded length of a PersistValueMeta instance in bytes.
     * This includes 2 bytes for 0 means not a compress value (number or short string)
     * 1 byte for the short type, 1 byte for the sub-block index,
     * 4 bytes for the segment index, and 4 bytes for the segment offset.
     */
    @VisibleForTesting
    static final int ENCODED_LENGTH = 2 + 1 + 1 + 4 + 4;

    /**
     * Checks if the given byte array represents a PersistValueMeta instance.
     * A byte array is considered a PersistValueMeta if its first byte is non-negative
     * and its length matches the ENCODED_LENGTH.
     *
     * @param bytes the byte array to check
     * @return true if the byte array represents a PersistValueMeta, false otherwise
     */
    public static boolean isPvm(byte[] bytes) {
        // first byte is type, < 0 means number byte or short string
        // normal CompressedValue encoded length is much more than PersistValueMeta encoded length
        return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
    }

    /**
     * The short type of the value being stored, encoded as a byte.
     * This could represent the redis type of the value (e.g., string, hash, list, set, zset, etc.).
     * For scan filter
     */
    byte shortType = KeyLoader.typeAsByteString;

    /**
     * The index of the sub-block within a segment where the value is stored.
     * A segment can include one to four compressed blocks.
     */
    byte subBlockIndex;

    /**
     * The index of the segment where the value is stored.
     * Segments are used to group related values for more efficient storage and retrieval.
     */
    int segmentIndex;

    /**
     * The offset within the segment where the value is stored.
     * This helps pinpoint the exact location of the value within its segment.
     */
    int segmentOffset;

    /**
     * The expiration time of the value in milliseconds since the epoch.
     * If the value never expires, this is set to NO_EXPIRE.
     */
    long expireAt = NO_EXPIRE;

    /**
     * A unique sequence number for the value.
     * This can be used to track changes and ensure consistency.
     */
    long seq;

    /**
     * The byte representation of the key associated with this value.
     * Used for efficient lookups and comparisons.
     */
    byte[] keyBytes;

    /**
     * A hash of the key, used for quick comparison in hash-based data structures.
     */
    long keyHash;

    /**
     * A 32-bit hash of the key, used for additional indexing and collision resolution.
     */
    int keyHash32;

    /**
     * The index of the bucket within the key's associated data structure.
     * Buckets are used to group keys that hash to similar values.
     */
    int bucketIndex;

    /**
     * Any additional bytes that may be required to store metadata not covered by other fields.
     */
    byte[] extendBytes;

    /**
     * Indicates whether this PersistValueMeta instance was created as a result of a merge operation.
     * Merging is used to combine multiple values into a single, more efficient representation.
     */
    boolean isFromMerge;

    /**
     * Calculates the cost of storing this metadata entry in a key bucket.
     * The cost is based on the length of the key and the length of the extended bytes (if present).
     *
     * @return the cost of storing this metadata entry in a key bucket
     * @throws IllegalArgumentException if the extended bytes exceed the maximum allowable length
     */
    int cellCostInKeyBucket() {
        var valueLength = extendBytes != null ? extendBytes.length : ENCODED_LENGTH;
        if (valueLength > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Persist value meta extend bytes too long=" + valueLength);
        }
        return KeyBucket.KVMeta.calcCellCount((short) keyBytes.length, (byte) valueLength);
    }

    /**
     * Checks if this PersistValueMeta instance corresponds to the specified segment, sub-block index, and segment offset.
     * This is useful for verifying the location of a value within the storage system.
     *
     * @param segmentIndex  the segment index to check
     * @param subBlockIndex the sub-block index to check
     * @param segmentOffset the segment offset to check
     * @return true if this metadata corresponds to the specified location, false otherwise
     */
    boolean isTargetSegment(int segmentIndex, byte subBlockIndex, int segmentOffset) {
        return this.segmentIndex == segmentIndex && this.subBlockIndex == subBlockIndex && this.segmentOffset == segmentOffset;
    }

    /**
     * Returns a short string representation of this PersistValueMeta instance, focusing on the segment-related fields.
     *
     * @return a short string representation of this metadata
     */
    public String shortString() {
        return "si=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
    }

    /**
     * Returns a full string representation of this PersistValueMeta instance, including all fields.
     *
     * @return a string representation of this metadata
     */
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
     * Encodes this PersistValueMeta instance into a byte array.
     * The encoding follows a specific format that includes the short type, sub-block index,
     * segment index, and segment offset.
     *
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
     * Decodes a byte array into a PersistValueMeta instance.
     * The byte array should be in the format produced by the encode() method.
     *
     * @param bytes the byte array to decode
     * @return a PersistValueMeta instance represented by the byte array
     */
    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        buf.readShort(); // skip 0
        pvm.shortType = buf.readByte();
        pvm.subBlockIndex = buf.readByte();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();
        return pvm;
    }
}