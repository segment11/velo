package io.velo.decode;

/**
 * Represents a big string without memory copy. Just use client read buffer.
 */
public class BigStringNoMemoryCopy {
    /**
     * The index of the resp data array.
     */
    public int readBufferInDataIndex;

    /**
     * The offset of the client read buffer.
     */
    public int offset = 0;

    /**
     * The length of the string.
     */
    public int length = 0;

    /**
     * Get a copy of this.
     *
     * @return a copy of this.
     */
    public BigStringNoMemoryCopy copy() {
        var copy = new BigStringNoMemoryCopy();
        copy.readBufferInDataIndex = readBufferInDataIndex;
        copy.offset = offset;
        copy.length = length;
        return copy;
    }

    /**
     * Reset this.
     */
    public void reset() {
        readBufferInDataIndex = 0;
        offset = 0;
        length = 0;
    }
}
