package io.velo;

import java.util.Arrays;

/**
 * A simple auto-growing byte buffer for reading and writing binary data.
 * This class provides a lightweight alternative to Netty's ByteBuf for basic read/write operations.
 *
 * <p>Key features:
 * <ul>
 *   <li>Auto-growing capacity with 2x exponential growth strategy</li>
 *   <li>Big-endian byte order for write operations</li>
 *   <li>Little-endian byte order support for LE variants</li>
 *   <li>Separate read and write indices for sequential access</li>
 * </ul>
 *
 * <p>Usage example:
 * <pre>
 * // Writing data
 * var slice = new Slice();
 * slice.writeInt(12345);
 * slice.writeBytes("hello".getBytes());
 * byte[] result = Arrays.copyOf(slice.getArray(), slice.getWriteIndex());
 *
 * // Reading data
 * var reader = new Slice(result);
 * int value = reader.readInt();
 * byte[] data = new byte[5];
 * reader.readBytes(data);
 * </pre>
 */
public class Slice {
    /**
     * Default initial capacity for new slices.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 64;

    /**
     * The underlying byte array.
     */
    private byte[] array;

    /**
     * The current write position in the array.
     */
    private int writeIndex;

    /**
     * The current read position in the array.
     */
    private int readIndex;

    /**
     * Constructs a new Slice with default initial capacity (64 bytes).
     */
    public Slice() {
        this.array = new byte[DEFAULT_INITIAL_CAPACITY];
        this.writeIndex = 0;
        this.readIndex = 0;
    }

    /**
     * Constructs a new Slice with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity in bytes
     * @throws IllegalArgumentException if initialCapacity is less than or equal to 0
     */
    public Slice(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Initial capacity must be positive");
        }
        this.array = new byte[initialCapacity];
        this.writeIndex = 0;
        this.readIndex = 0;
    }

    /**
     * Constructs a new Slice wrapping the given byte array for reading.
     * The write index is set to the array length.
     *
     * @param data the byte array to wrap
     * @throws IllegalArgumentException if data is null
     */
    public Slice(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        this.array = data;
        this.writeIndex = data.length;
        this.readIndex = 0;
    }

    /**
     * Constructs a new Slice with a copy of the specified portion of the given byte array.
     *
     * @param data   the source byte array
     * @param offset the starting offset in the source array
     * @param length the number of bytes to copy
     * @throws IllegalArgumentException if data is null or offset/length are invalid
     */
    public Slice(byte[] data, int offset, int length) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("Invalid offset or length");
        }
        this.array = Arrays.copyOfRange(data, offset, offset + length);
        this.writeIndex = length;
        this.readIndex = 0;
    }

    /**
     * Ensures the internal array has enough capacity for the required number of bytes.
     * If needed, the array is grown using 2x exponential strategy.
     *
     * @param required the number of additional bytes needed
     */
    private void ensureCapacity(int required) {
        if (writeIndex + required <= array.length) {
            return;
        }
        int newCapacity = array.length;
        while (newCapacity < writeIndex + required) {
            newCapacity *= 2;
        }
        array = Arrays.copyOf(array, newCapacity);
    }

    /**
     * Writes a single byte to this slice.
     *
     * @param value the byte value to write (only the low 8 bits are used)
     */
    public void writeByte(int value) {
        ensureCapacity(1);
        array[writeIndex++] = (byte) value;
    }

    /**
     * Writes all bytes from the source array to this slice.
     *
     * @param src the source byte array
     * @throws IllegalArgumentException if src is null
     */
    public void writeBytes(byte[] src) {
        if (src == null) {
            throw new IllegalArgumentException("Source array cannot be null");
        }
        writeBytes(src, 0, src.length);
    }

    /**
     * Writes bytes from the source array to this slice.
     *
     * @param src     the source byte array
     * @param srcIndex the starting index in the source array
     * @param len     the number of bytes to write
     * @throws IllegalArgumentException if src is null or indices are invalid
     */
    public void writeBytes(byte[] src, int srcIndex, int len) {
        if (src == null) {
            throw new IllegalArgumentException("Source array cannot be null");
        }
        if (srcIndex < 0 || len < 0 || srcIndex + len > src.length) {
            throw new IllegalArgumentException("Invalid source index or length");
        }
        ensureCapacity(len);
        System.arraycopy(src, srcIndex, array, writeIndex, len);
        writeIndex += len;
    }

    /**
     * Writes a 32-bit integer in big-endian byte order.
     *
     * @param value the integer value to write
     */
    public void writeInt(int value) {
        ensureCapacity(4);
        array[writeIndex++] = (byte) (value >>> 24);
        array[writeIndex++] = (byte) (value >>> 16);
        array[writeIndex++] = (byte) (value >>> 8);
        array[writeIndex++] = (byte) value;
    }

    /**
     * Writes a 64-bit double in big-endian byte order.
     *
     * @param value the double value to write
     */
    public void writeDouble(double value) {
        writeLong(Double.doubleToLongBits(value));
    }

    /**
     * Writes a 64-bit long in big-endian byte order.
     *
     * @param value the long value to write
     */
    public void writeLong(long value) {
        ensureCapacity(8);
        array[writeIndex++] = (byte) (value >>> 56);
        array[writeIndex++] = (byte) (value >>> 48);
        array[writeIndex++] = (byte) (value >>> 40);
        array[writeIndex++] = (byte) (value >>> 32);
        array[writeIndex++] = (byte) (value >>> 24);
        array[writeIndex++] = (byte) (value >>> 16);
        array[writeIndex++] = (byte) (value >>> 8);
        array[writeIndex++] = (byte) value;
    }

    /**
     * Writes a 16-bit short in little-endian byte order.
     *
     * @param value the short value to write (only the low 16 bits are used)
     */
    public void writeShortLE(int value) {
        ensureCapacity(2);
        array[writeIndex++] = (byte) value;
        array[writeIndex++] = (byte) (value >>> 8);
    }

    /**
     * Writes a 32-bit integer in little-endian byte order.
     *
     * @param value the integer value to write
     */
    public void writeIntLE(int value) {
        ensureCapacity(4);
        array[writeIndex++] = (byte) value;
        array[writeIndex++] = (byte) (value >>> 8);
        array[writeIndex++] = (byte) (value >>> 16);
        array[writeIndex++] = (byte) (value >>> 24);
    }

    /**
     * Writes a 64-bit long in little-endian byte order.
     *
     * @param value the long value to write
     */
    public void writeLongLE(long value) {
        ensureCapacity(8);
        array[writeIndex++] = (byte) value;
        array[writeIndex++] = (byte) (value >>> 8);
        array[writeIndex++] = (byte) (value >>> 16);
        array[writeIndex++] = (byte) (value >>> 24);
        array[writeIndex++] = (byte) (value >>> 32);
        array[writeIndex++] = (byte) (value >>> 40);
        array[writeIndex++] = (byte) (value >>> 48);
        array[writeIndex++] = (byte) (value >>> 56);
    }

    /**
     * Returns the number of readable bytes remaining.
     *
     * @return the number of bytes that can be read
     */
    public int readableBytes() {
        return writeIndex - readIndex;
    }

    /**
     * Checks if the specified number of bytes can be read.
     *
     * @param size the number of bytes to check
     * @return true if at least 'size' bytes are readable
     */
    public boolean isReadable(int size) {
        return readIndex + size <= writeIndex;
    }

    /**
     * Checks if any bytes are readable.
     *
     * @return true if at least one byte is readable
     */
    public boolean isReadable() {
        return readIndex < writeIndex;
    }

    /**
     * Reads a single byte.
     *
     * @return the byte value (0-255)
     * @throws ArrayIndexOutOfBoundsException if no bytes are readable
     */
    public int readByte() {
        return array[readIndex++] & 0xFF;
    }

    /**
     * Reads a 32-bit integer in big-endian byte order.
     *
     * @return the integer value
     * @throws ArrayIndexOutOfBoundsException if not enough bytes are readable
     */
    public int readInt() {
        int value = ((array[readIndex] & 0xFF) << 24)
                | ((array[readIndex + 1] & 0xFF) << 16)
                | ((array[readIndex + 2] & 0xFF) << 8)
                | (array[readIndex + 3] & 0xFF);
        readIndex += 4;
        return value;
    }

    /**
     * Reads a 64-bit long in big-endian byte order.
     *
     * @return the long value
     * @throws ArrayIndexOutOfBoundsException if not enough bytes are readable
     */
    public long readLong() {
        long value = ((long) (array[readIndex] & 0xFF) << 56)
                | ((long) (array[readIndex + 1] & 0xFF) << 48)
                | ((long) (array[readIndex + 2] & 0xFF) << 40)
                | ((long) (array[readIndex + 3] & 0xFF) << 32)
                | ((long) (array[readIndex + 4] & 0xFF) << 24)
                | ((long) (array[readIndex + 5] & 0xFF) << 16)
                | ((long) (array[readIndex + 6] & 0xFF) << 8)
                | ((long) (array[readIndex + 7] & 0xFF));
        readIndex += 8;
        return value;
    }

    /**
     * Reads bytes into the destination array.
     *
     * @param dst the destination byte array
     * @throws ArrayIndexOutOfBoundsException if not enough bytes are readable
     * @throws IllegalArgumentException       if dst is null
     */
    public void readBytes(byte[] dst) {
        if (dst == null) {
            throw new IllegalArgumentException("Destination array cannot be null");
        }
        System.arraycopy(array, readIndex, dst, 0, dst.length);
        readIndex += dst.length;
    }

    /**
     * Returns the underlying byte array.
     * Note: the array may be larger than the actual written data.
     *
     * @return the underlying byte array
     */
    public byte[] getArray() {
        return array;
    }

    /**
     * Returns the current write index.
     *
     * @return the write index
     */
    public int getWriteIndex() {
        return writeIndex;
    }

    /**
     * Returns the current read index.
     *
     * @return the read index
     */
    public int getReadIndex() {
        return readIndex;
    }
}
