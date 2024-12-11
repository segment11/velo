package io.velo;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class KeyHash {
    private KeyHash() {
    }

    private static final XXHash64 xxHash64Java = XXHashFactory.fastestJavaInstance().hash64();
    private static final XXHash32 xxHash32Java = XXHashFactory.fastestJavaInstance().hash32();

    private static final long seed = 0x9747b28cL;
    private static final int seed32 = 0x9747b28c;

    private static final byte[] fixedPrefixKeyBytesForTest = "xh!".getBytes();

    public static long hash(byte[] keyBytes) {
        // for unit test, mock some keys always in the same bucket index
        if (keyBytes.length > fixedPrefixKeyBytesForTest.length) {
            if (keyBytes[0] == fixedPrefixKeyBytesForTest[0]
                    && keyBytes[1] == fixedPrefixKeyBytesForTest[1]
                    && keyBytes[2] == fixedPrefixKeyBytesForTest[2]) {
                var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

                // eg. xh!123_key1, xh!123_key2, xh!123_key3 means different key hash but in bucket index 123
                var key = new String(keyBytes);
                var index_ = key.indexOf("_");
                var rawKey = key.substring(index_ + 1);
                var rawKeyHash = xxHash64Java.hash(rawKey.getBytes(), 0, rawKey.length(), seed);

                var expectedBucketIndex = Integer.parseInt(key.substring(fixedPrefixKeyBytesForTest.length, index_));
                var mod = rawKeyHash & (bucketsPerSlot - 1);
                if (mod == expectedBucketIndex) {
                    return rawKeyHash;
                }
                return rawKeyHash + (expectedBucketIndex - mod);
            }
        }

        return xxHash64Java.hash(keyBytes, 0, keyBytes.length, seed);
    }

    public static long hashOffset(byte[] keyBytes, int offset, int length) {
        return xxHash64Java.hash(keyBytes, offset, length, seed);
    }

    public static int hash32(byte[] keyBytes) {
        return xxHash32Java.hash(keyBytes, 0, keyBytes.length, seed32);
    }

    public static int hash32Offset(byte[] keyBytes, int offset, int length) {
        return xxHash32Java.hash(keyBytes, offset, length, seed32);
    }

    // for split index re-calculating, avoid data skew
    public static byte splitIndex(long keyHash, byte splitNumber, int bucketIndex) {
        if (splitNumber == 1) {
            return 0;
        }

        // for unit test
        if (keyHash >= 10 && keyHash < 10 + splitNumber) {
            return (byte) (keyHash - 10);
        }

        return (byte) Math.abs(((keyHash >> 32) % splitNumber));
    }

    public static int bucketIndex(long keyHash, int bucketsPerSlot) {
        return Math.abs((int) (keyHash & (bucketsPerSlot - 1)));
    }
}
