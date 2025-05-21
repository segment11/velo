package io.velo.type;

import org.jetbrains.annotations.NotNull;

/**
 * A class representing a Redis BitSet that can dynamically expand, and do bite operations.
 * Redis BitSet is Big-Endian. Java BitSet is Little-Endian.
 */
public class RedisBitSet {
    private byte[] valueBytes;

    private boolean isExpandedWhenInit = false;

    /**
     * Construct a RedisBitSet with given value bytes.
     *
     * @param valueBytes the value bytes of the RedisBitSet.
     */
    public RedisBitSet(byte[] valueBytes) {
        this.valueBytes = valueBytes == null ? new byte[8] : valueBytes;
        this.isExpandedWhenInit = valueBytes == null;
    }

    /**
     * Get the value bytes of the RedisBitSet.
     *
     * @return the value bytes of the RedisBitSet.
     */
    public byte[] getValueBytes() {
        return valueBytes;
    }

    /**
     * Expand the value bytes of the RedisBitSet if needed.
     *
     * @param offset the offset of the bit to set.
     * @return true if the value bytes is expanded, false otherwise.
     */
    private boolean expand(int offset) {
        var newByteLength = (offset + 7) / 8 + 1;
        if (newByteLength > valueBytes.length) {
            var newValueBytes = new byte[newByteLength];
            System.arraycopy(valueBytes, 0, newValueBytes, 0, valueBytes.length);
            valueBytes = newValueBytes;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Result of set operation.
     *
     * @param isExpanded is the value bytes is expanded.
     * @param isChanged  is the bit is changed.
     * @param isOldBit1  is the old bit is 1.
     */
    public record SetResult(boolean isExpanded, boolean isChanged, boolean isOldBit1) {
        @Override
        public @NotNull String toString() {
            return "SetResult{" +
                    "isExpanded=" + isExpanded +
                    ", isChanged=" + isChanged +
                    ", isOldBit1=" + isOldBit1 +
                    '}';
        }
    }

    /**
     * Set the bit at the given offset to the given value.
     *
     * @param offset the offset of the bit to set.
     * @param isBit1 the value to set, true means 1.
     * @return the result of set operation.
     */
    public SetResult set(int offset, boolean isBit1) {
        var isExpanded = expand(offset) || isExpandedWhenInit;

        var byteIndex = offset / 8;
        // from left to right
        var bitIndex = offset % 8;
        // is target bit is 1
        var oldBit = valueBytes[byteIndex] >> (7 - bitIndex) & 1;
        var isChanged = oldBit != (isBit1 ? 1 : 0);

        if (!isChanged) {
            return new SetResult(isExpanded, false, oldBit == 1);
        }

        valueBytes[byteIndex] = (byte) (valueBytes[byteIndex] ^ (1 << (7 - bitIndex)));
        return new SetResult(isExpanded, true, oldBit == 1);
    }

    /**
     * Get the bit at the given offset.
     *
     * @param offset the offset of the bit to get.
     * @return true if the bit is 1, false otherwise.
     */
    public boolean get(int offset) {
        var byteIndex = offset / 8;
        // from left to right
        var bitIndex = offset % 8;

        if (byteIndex >= valueBytes.length) {
            return false;
        }

        // is target bit is 1
        var oldBit = valueBytes[byteIndex] >> (7 - bitIndex) & 1;
        return oldBit == 1;
    }
}
