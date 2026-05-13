package io.velo.decode;

/**
 * A big string value that references the client's read buffer without copying.
 * Used to avoid memory copies for large string values.
 */
public class BigStringNoMemoryCopy {
    /** The index in the RESP data array. */
    public int readBufferInDataIndex;

    /** The offset in the client read buffer. */
    public int offset = 0;

    /** The length of the string. */
    public int length = 0;

    /**
     * @return a copy of this object
     */
    public BigStringNoMemoryCopy copy() {
        var copy = new BigStringNoMemoryCopy();
        copy.readBufferInDataIndex = readBufferInDataIndex;
        copy.offset = offset;
        copy.length = length;
        return copy;
    }

    /** Resets all fields to initial values. */
    public void reset() {
        readBufferInDataIndex = 0;
        offset = 0;
        length = 0;
    }
}
