package io.velo.extend;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Compressed array of longs using delta encoding.
 * Each shard stores 64 numbers initially, expanding dynamically.
 */
public class ZippedLongArray {
    /** Each shard initially stores this many numbers. */
    private static final int SHARD_SIZE = 64;

    /** Number of shards added when expansion is needed. */
    private static final int SHARD_EXPAND_COUNT = 128;

    /** Current number of shards allocated. */
    int shardCount = SHARD_EXPAND_COUNT;

    /** Total number of long values stored. */
    private int size = 0;

    /** Current shard index for new additions. */
    int shardCursor = 0;

    /** Position within current shard for next number. */
    private short lastNumPos = 0;

    /** Two-dimensional array storing compressed long values. */
    private byte[][] shards;

    /**
     * @param count expected number of long values to store
     */
    public ZippedLongArray(int count) {
        shardCount = count / SHARD_SIZE + 1;
        shards = new byte[shardCount][];
    }

    /** Creates a ZippedLongArray with default initial capacity. */
    public ZippedLongArray() {
        shards = new byte[shardCount][];
    }

    /**
     * @param value the long value to add
     * @throws IllegalArgumentException if value exceeds maximum supported
     */
    public void add(long value) {
        if (Math.abs(value) >= 0xf0L << 55) {
            throw new IllegalArgumentException(
                "Number too large, do not support bigger(equal) than 0xf0L << 55");
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
     * @param ix the index of the value to retrieve
     * @return the long value at the specified index
     * @throws ArrayIndexOutOfBoundsException if index is out of bounds
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

    private void expandShards() {
        shardCount += SHARD_EXPAND_COUNT;
        var newShards = new byte[shardCount][];
        System.arraycopy(shards, 0, newShards, 0, shards.length);
        shards = newShards;
    }

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

    private static long inflate(byte[] bag) {
        return inflate(bag, 0, bag.length);
    }

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
     * @return total bytes used including array references
     */
    public long getTotalBytesUsed() {
        long bytes = Arrays.stream(shards).mapToLong(t -> t == null ? 0 : t.length).sum();
        return shards.length * 16L + bytes;
    }

    /** @return total number of long values stored */
    public int size() {
        return size;
    }

    /**
     * @param startIndex the starting index of the subset
     * @param count      the number of elements in the subset
     * @return iterator for traversing the subset
     */
    public ZippedIterator getIterator(int startIndex, int count) {
        return new ZippedIterator(startIndex, count);
    }

    /** Iterator for traversing a subset of numbers in the ZippedLongArray. */
    public class ZippedIterator {
        private int numPos;
        private int shardIndex;
        private long curOffset;
        private int count;

        /**
         * @param ix    the starting index of the subset
         * @param count the number of elements in the subset
         * @throws IndexOutOfBoundsException if subset is out of bounds
         */
        private ZippedIterator(int ix, int count) {
            if (ix + count > size) {
                throw new IndexOutOfBoundsException(
                    "end index overflow:" + (ix + count) + " of " + size);
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

        /** @return the next long value in the subset */
        public long nextLong() {
            var shard = shards[shardIndex];
            int len = (0xff & shard[numPos]) >>> 5;

            long data = numPos > 0 ? curOffset + inflate(shard, numPos, len)
                : inflate(shard, numPos, len);

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

        /** @return true if there are more elements */
        public boolean hasNext() {
            return count > 0;
        }
    }
}