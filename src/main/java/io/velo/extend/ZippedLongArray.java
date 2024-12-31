package io.velo.extend;

import java.nio.ByteBuffer;
import java.util.Arrays;

// refer https://developer.aliyun.com/article/785184
public class ZippedLongArray {
    // each bucket initially stores 64 numbers
    private static final int SHARD_SIZE = 64;

    // each time expand the number of buckets, can be adjusted according to the size of the data, improve initialization efficiency
    private static final int SHARD_EXPAND_COUNT = 128;

    private int shardCount = SHARD_EXPAND_COUNT;

    private int size = 0;

    // current bucket pointer
    private int shardCursor = 0;

    // current in bucket pointer, must be less than SHARD_SIZE * 8, so it can be short
    private short lastNumPos = 0;

    // all numbers are stored in a two-dimensional array
    private byte[][] shards;

    public ZippedLongArray(int count) {
        shardCount = count / SHARD_SIZE + 1;
        shards = new byte[shardCount][];
    }

    public ZippedLongArray() {
        shards = new byte[shardCount][];
    }

    /**
     * add by sequence, the compression effect is the best, because it occupies the high 4 bits
     * (the first 3 bits represent the data length, and the last 1 bit represents the positive and negative),
     * so the value cannot exceed 0xf0L << 55, that is, except
     * the sign bit, the high 4 bits cannot be used.
     */
    public void add(long value) {
        if (Math.abs(value) >= 0xf0L << 55) {
            throw new IllegalArgumentException("Number too large, do not support bigger(equal) than 0xf0L << 55");
        }

        if (shards[shardCursor] == null) {
            // initialize the array size to store the corresponding number, and then reduce the entire array after compression
            shards[shardCursor] = new byte[SHARD_SIZE * 8];
        }

        var buf = ByteBuffer.wrap(shards[shardCursor]);
        buf.position(lastNumPos);

        var zipped = deflate(value);

        // for the bucket except the first number, only the offset is stored, and the data can be further compressed
        if (lastNumPos > 0) {
            zipped = deflate(value - get(shardCursor * SHARD_SIZE));
        }

        // if the current bucket is full, create a new bucket
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

            // offset no longer works after opening the bucket
            zipped = deflate(value);
        }

        buf.put(zipped);
        lastNumPos = (short) buf.position();

        size++;
    }

    public long get(int ix) {
        // first find the shard, because each bucket stores a fixed number of numbers, it can be directly mapped
        int i = ix / SHARD_SIZE;

        // the rest is the offset that needs to be linearly searched
        ix %= SHARD_SIZE;

        var shard = shards[i];

        // find the offset of the corresponding data
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
        // expand the bucket
        shardCount += SHARD_EXPAND_COUNT;

        var newShards = new byte[shardCount][];
        System.arraycopy(shards, 0, newShards, 0, shards.length);
        shards = newShards;
    }

    private static byte[] deflate(long num) {
        int negative = num < 0 ? 0x01 : 0x00;

        // only store positive numbers
        num = (num < 0) ? -num : num;

        // extract the valid bytes of the number
        var tp = new byte[8];
        int n = 0;
        do {
            tp[n++] = (byte) num;
        } while (((num & ~0x0f) | (num >>= 8)) > 0);

        // first three bits represent the number of bits occupied
        tp[n - 1] = (byte) (tp[n - 1] | (n) << 5);
        // the fourth bit represents positive or negative
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

        // store the data in a stack array represented by long
        for (int i = 0; i < len; i++) {
            // & 0xff means converting to an unsigned number
            data |= (long) (0xff & shard[numPos + i]) << (len - i - 1) * 8;
        }

        // check the sign bit
        long negative = data & (0x10L << (len - 1) * 8);
        // clear the occupied bit data
        data &= ~(0xf0L << (len - 1) * 8);
        return negative > 0 ? -data : data;
    }

    public int getShardCursor() {
        return shardCursor;
    }

    public int getShardSize() {
        return SHARD_SIZE;
    }

    public long getTotalBytesUsed() {
        long bytes = Arrays.stream(shards).mapToLong(t -> t == null ? 0 : t.length).sum();

        // a reference occupies 16 bytes
        return shards.length * 16L + bytes;
    }

    public int size() {
        return size;
    }

    public ZippedIterator getIterator(int startIndex, int count) {
        return new ZippedIterator(startIndex, count);
    }

    public class ZippedIterator {
        private int numPos;

        private int shardIndex;

        private long curOffset;

        private int count;

        private ZippedIterator(int ix, int count) {
            if (ix + count > size) {
                throw new IndexOutOfBoundsException("end index overflow:" + (ix + count) + " of " + size);
            }

            // first find the shard, because each bucket stores a fixed number of numbers, it can be directly mapped
            shardIndex = ix / SHARD_SIZE;

            // the rest is the offset that needs to be linearly searched
            ix %= SHARD_SIZE;

            // find the offset of the corresponding data
            var shard = shards[shardIndex];
            curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);

            while (ix > 0) {
                int len = (0xff & shard[numPos]) >>> 5;
                numPos += len;
                ix -= 1;
            }
            this.count = count;
        }

        public long nextLong() {
            var shard = shards[shardIndex];
            int len = (0xff & shard[numPos]) >>> 5;

            // the first data of each bucket does not need to increase the initial value
            // and the subsequent data needs to be increased, because only the offset is stored
            long data = numPos > 0 ? curOffset + inflate(shard, numPos, len) : inflate(shard, numPos, len);

            numPos += len;

            if (numPos >= shard.length) {
                shardIndex++;
                numPos = 0;

                // find the offset of the corresponding data
                shard = shards[shardIndex];
                curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);
            }
            count--;

            return data;
        }

        public boolean hasNext() {
            return count > 0;
        }
    }
}
