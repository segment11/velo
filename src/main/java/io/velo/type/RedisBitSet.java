package io.velo.type;

import org.jetbrains.annotations.NotNull;

/**
 * Redis BitSet implementation with dynamic expansion (Big-Endian).
 */
public class RedisBitSet {
    private byte[] valueBytes;

    private boolean isExpandedWhenInit = false;

    /**
     * @param valueBytes the value bytes of the RedisBitSet
     */
    public RedisBitSet(byte[] valueBytes) {
        this.valueBytes = valueBytes == null ? new byte[8] : valueBytes;
        this.isExpandedWhenInit = valueBytes == null;
    }

    /** @return the value bytes */
    public byte[] getValueBytes() {
        return valueBytes;
    }

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
     * @param isExpanded whether the value bytes is expanded
     * @param isChanged  whether the bit is changed
     * @param isOldBit1  whether the old bit is 1
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
     * @param offset the offset of the bit to set
     * @param isBit1 true to set bit to 1, false for 0
     * @return the result of set operation
     */
    public SetResult set(int offset, boolean isBit1) {
        var isExpanded = expand(offset) || isExpandedWhenInit;

        var byteIndex = offset / 8;
        var bitIndex = offset % 8;
        var oldBit = valueBytes[byteIndex] >> (7 - bitIndex) & 1;
        var isChanged = oldBit != (isBit1 ? 1 : 0);

        if (!isChanged) {
            return new SetResult(isExpanded, false, oldBit == 1);
        }

        valueBytes[byteIndex] = (byte) (valueBytes[byteIndex] ^ (1 << (7 - bitIndex)));
        return new SetResult(isExpanded, true, oldBit == 1);
    }

    /**
     * @param offset the offset of the bit to get
     * @return true if the bit is 1, false otherwise
     */
    public boolean get(int offset) {
        var byteIndex = offset / 8;
        var bitIndex = offset % 8;

        if (byteIndex >= valueBytes.length) {
            return false;
        }

        var oldBit = valueBytes[byteIndex] >> (7 - bitIndex) & 1;
        return oldBit == 1;
    }
}
