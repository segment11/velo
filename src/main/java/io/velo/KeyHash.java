package io.velo;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.TestOnly;

/**
 * Provides utility methods for hashing keys using XXHash algorithms.
 * This class includes methods for generating 64-bit and 32-bit hashes, as well as methods for calculating split indices and bucket indices.
 */
public class KeyHash {
    /**
     * Private constructor to prevent instantiation.
     */
    private KeyHash() {
    }

    /**
     * XXHash64 instance for hashing.
     */
    private static final XXHash64 xxHash64Java = XXHashFactory.fastestJavaInstance().hash64();

    /**
     * XXHash32 instance for hashing.
     */
    private static final XXHash32 xxHash32Java = XXHashFactory.fastestJavaInstance().hash32();

    /**
     * Seed value for 64-bit hashing.
     */
    private static final long seed = 0x9747b28cL;

    /**
     * Seed value for 32-bit hashing.
     */
    private static final int seed32 = 0x9747b28c;

    /**
     * Fixed prefix key bytes used for unit testing.
     */
    @TestOnly
    private static final byte[] fixedPrefixKeyBytesForTest = "xh!".getBytes();

    /**
     * Computes the 64-bit hash of a given key byte array.
     * For unit testing, keys with a specific prefix are mocked to always hash to the same bucket index.
     *
     * @param keyBytes the key byte array to hash
     * @return the 64-bit hash of the key
     */
    public static long hash(byte[] keyBytes) {
        // for unit test
        // mock some keys always in the same bucket index
        if (keyBytes.length > fixedPrefixKeyBytesForTest.length) {
            if (keyBytes[0] == fixedPrefixKeyBytesForTest[0]
                    && keyBytes[1] == fixedPrefixKeyBytesForTest[1]
                    && keyBytes[2] == fixedPrefixKeyBytesForTest[2]) {
                var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

                // xh!123_key1, xh!123_key2, xh!123_key3 means different key hash but in bucket index 123
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

    /**
     * Computes the 64-bit hash of a portion of a given key byte array.
     *
     * @param keyBytes the key byte array to hash
     * @param offset   the starting offset in the key byte array
     * @param length   the number of bytes to hash
     * @return the 64-bit hash of the specified portion of the key
     */
    public static long hashOffset(byte[] keyBytes, int offset, int length) {
        return xxHash64Java.hash(keyBytes, offset, length, seed);
    }

    /**
     * Computes the 32-bit hash of a given key byte array.
     *
     * @param keyBytes the key byte array to hash
     * @return the 32-bit hash of the key
     */
    public static int hash32(byte[] keyBytes) {
        return xxHash32Java.hash(keyBytes, 0, keyBytes.length, seed32);
    }

    /**
     * Computes the 32-bit hash of a portion of a given key byte array.
     *
     * @param keyBytes the key byte array to hash
     * @param offset   the starting offset in the key byte array
     * @param length   the number of bytes to hash
     * @return the 32-bit hash of the specified portion of the key
     */
    public static int hash32Offset(byte[] keyBytes, int offset, int length) {
        return xxHash32Java.hash(keyBytes, offset, length, seed32);
    }

    /**
     * Computes the split index for a given key hash.
     * This method helps to avoid data skew when splitting indices.
     *
     * @param keyHash     the key hash
     * @param splitNumber the number of splits
     * @param bucketIndex the bucket index
     * @return the split index
     */
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

    /**
     * Computes the bucket index for a given key hash.
     *
     * @param keyHash the key hash
     * @return the bucket index
     */
    public static int bucketIndex(long keyHash) {
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;
        return Math.abs((int) (keyHash & (bucketsPerSlot - 1)));
    }
}
