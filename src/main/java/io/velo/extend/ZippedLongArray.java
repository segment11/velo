package io.velo.extend;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A class that provides a compressed representation of an array of longs. This class
 * uses delta encoding and bit manipulation to compress the long values. Each shard
 * or bucket initially stores 64 numbers and if more space is required, additional shards
 * are dynamically added.
 * <p>
 * The compression algorithm is optimized for sequential numbers, where the difference
 * between consecutive numbers is stored rather than the full number, thus saving space.
 * Each long number is stored in a bit-efficient manner, where the most significant
 * bits are used for length and sign information.
 * <p>
 * This class provides methods to add and retrieve numbers as well as to get an iterator
 * that can be used to traverse a subset of the stored numbers.
 * Refer https://developer.aliyun.com/article/785184
 */
public class ZippedLongArray {
    /**
     * Each shard initially stores this many numbers.
     */
    private static final int SHARD_SIZE = 64;

    /**
     * This is the number of shards by which the internal storage is expanded when more space is needed.
     */
    private static final int SHARD_EXPAND_COUNT = 128;

    /**
     * The current number of shards (buckets) allocated.
     */
    int shardCount = SHARD_EXPAND_COUNT;

    /**
     * The total number of long values stored in the array.
     */
    private int size = 0;

    /**
     * The current shard index to which new numbers are added.
     */
    int shardCursor = 0;

    /**
     * The position within the current shard where the next number will be added.
     */
    private short lastNumPos = 0;

    /**
     * The two-dimensional array that stores the compressed long values.
     */
    private byte[][] shards;

    /**
     * Constructs a ZippedLongArray with an initial capacity based on the count of numbers expected.
     *
     * @param count The number of long values that are expected to be stored.
     */
    public ZippedLongArray(int count) {
        shardCount = count / SHARD_SIZE + 1;
        shards = new byte[shardCount][];
    }

    /**
     * Constructs a ZippedLongArray with a default initial capacity.
     */
    public ZippedLongArray() {
        shards = new byte[shardCount][];
    }

    /**
     * Adds a long value to the array. This method will throw an IllegalArgumentException
     * if the value exceeds the maximum supported value.
     *
     * @param value The long value to add to the array.
     * @throws IllegalArgumentException if the provided number is too large to be compressed.
     */
    public void add(long value) {
        if (Math.abs(value) >= 0xf0L << 55) {
            throw new IllegalArgumentException("Number too large, do not support bigger(equal) than 0xf0L << 55");
        }

        if (shards[shardCursor] == null) {
            shards[shardCursor] = new byte[SHARD_SIZE * 8];
        }

        var buf = ByteBuffer.wrap(shards[shardCursor]);
        buf.position(lastNumPos);

        var zipped = deflate(value);

        if (lastNumPos > 0) {
            zipped = deflate(value - get(shardCursor * SHARD_SIZE));
        }

        if (size - shardCursor * SHARD_SIZE + 1 > SHARD_SIZE) {
            var zippedArray = new byte[buf.position()];
            System.arraycopy(buf.array(), 0, zippedArray, 0, zippedArray.length);
            shards[shardCursor++] = zippedArray;

            if (shardCursor >= shardCount) {
                expandShards();
            }

            buf = ByteBuffer.allocate(SHARD_SIZE * 8);
            shards[shardCursor] = buf.array();

            lastNumPos = 0;
            zipped = deflate(value);
        }

        buf.put(zipped);
        lastNumPos = (short) buf.position();

        size++;
    }

    /**
     * Retrieves the long value at the specified index.
     *
     * @param ix The index of the value to retrieve.
     * @return The long value at the specified index.
     * @throws ArrayIndexOutOfBoundsException if the index is out of bounds.
     */
    public long get(int ix) {
        int i = ix / SHARD_SIZE;
        ix %= SHARD_SIZE;

        var shard = shards[i];
        long offset = 0;
        if (ix > 0) {
            int len = (Byte.toUnsignedInt(shard[0]) >>> 5);
            offset = inflate(shards[i], 0, len);
        }

        int numPos = 0;
        while (ix > 0) {
            int len = (Byte.toUnsignedInt(shard[numPos]) >>> 5);

            numPos += len;
            ix -= 1;
        }

        int len = (Byte.toUnsignedInt(shard[numPos]) >>> 5);
        return offset + inflate(shards[i], numPos, len);
    }

    /**
     * Expands the internal storage to accommodate more numbers.
     */
    private void expandShards() {
        shardCount += SHARD_EXPAND_COUNT;
        var newShards = new byte[shardCount][];
        System.arraycopy(shards, 0, newShards, 0, shards.length);
        shards = newShards;
    }

    /**
     * Compresses a long value into a byte array for storage.
     *
     * @param num The long value to compress.
     * @return A byte array containing the compressed representation of the long.
     */
    private static byte[] deflate(long num) {
        int negative = num < 0 ? 0x01 : 0x00;
        num = (num < 0) ? -num : num;

        var tp = new byte[8];
        int n = 0;
        do {
            tp[n++] = (byte) num;
        } while (((num & ~0x0f) | (num >>= 8)) > 0);

        tp[n - 1] = (byte) (tp[n - 1] | (n) << 5);
        tp[n - 1] = (byte) (tp[n - 1] | negative << 4);

        var zipped = new byte[n];
        for (int i = n; i > 0; i--) {
            zipped[n - i] = tp[i - 1];
        }
        return zipped;
    }

    /**
     * Decompresses a byte array back into a long value.
     *
     * @param bag The byte array containing the compressed data.
     * @return The decompressed long value.
     */
    private static long inflate(byte[] bag) {
        return inflate(bag, 0, bag.length);
    }

    /**
     * Decompresses a byte array back into a long value, starting at a specific position and using specified length.
     *
     * @param shard  The byte array containing the compressed data.
     * @param numPos The starting position in the byte array.
     * @param len    The number of bytes to decompress.
     * @return The decompressed long value.
     */
    private static long inflate(byte[] shard, int numPos, int len) {
        long data = 0;

        for (int i = 0; i < len; i++) {
            data |= (long) (0xff & shard[numPos + i]) << (len - i - 1) * 8;
        }

        long negative = data & (0x10L << (len - 1) * 8);
        data &= ~(0xf0L << (len - 1) * 8);
        return negative > 0 ? -data : data;
    }

    /**
     * Gets the total number of bytes used by this ZippedLongArray.
     *
     * @return The total number of bytes used, including array references.
     */
    public long getTotalBytesUsed() {
        long bytes = Arrays.stream(shards).mapToLong(t -> t == null ? 0 : t.length).sum();
        return shards.length * 16L + bytes;
    }

    /**
     * Returns the total number of long values stored in the array.
     *
     * @return The total number of long values.
     */
    public int size() {
        return size;
    }

    /**
     * Gets an iterator that can be used to traverse a subset of the stored numbers.
     *
     * @param startIndex The starting index of the subset.
     * @param count      The number of elements in the subset.
     * @return A ZippedIterator that can be used to traverse the subset.
     */
    public ZippedIterator getIterator(int startIndex, int count) {
        return new ZippedIterator(startIndex, count);
    }

    /**
     * An iterator for traversing a subset of numbers in the ZippedLongArray.
     */
    public class ZippedIterator {
        private int numPos;
        private int shardIndex;
        private long curOffset;
        private int count;

        /**
         * Constructs a new ZippedIterator for the specified subset.
         *
         * @param ix    The starting index of the subset.
         * @param count The number of elements in the subset.
         * @throws IndexOutOfBoundsException if the requested subset is out of bounds.
         */
        private ZippedIterator(int ix, int count) {
            if (ix + count > size) {
                throw new IndexOutOfBoundsException("end index overflow:" + (ix + count) + " of " + size);
            }

            shardIndex = ix / SHARD_SIZE;
            ix %= SHARD_SIZE;

            var shard = shards[shardIndex];
            curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);

            while (ix > 0) {
                int len = (0xff & shard[numPos]) >>> 5;
                numPos += len;
                ix -= 1;
            }
            this.count = count;
        }

        /**
         * Retrieves the next long value in the subset.
         *
         * @return The next long value in the subset.
         */
        public long nextLong() {
            var shard = shards[shardIndex];
            int len = (0xff & shard[numPos]) >>> 5;

            long data = numPos > 0 ? curOffset + inflate(shard, numPos, len) : inflate(shard, numPos, len);

            numPos += len;

            if (numPos >= shard.length) {
                shardIndex++;
                numPos = 0;

                shard = shards[shardIndex];
                curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);
            }
            count--;

            return data;
        }

        /**
         * Checks if there are more elements in the subset.
         *
         * @return true if there are more elements, false otherwise.
         */
        public boolean hasNext() {
            return count > 0;
        }
    }
}