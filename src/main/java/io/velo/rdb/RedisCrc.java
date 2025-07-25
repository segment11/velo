package io.velo.rdb;

/**
 * Provides utility functions for computing CRC64 checksums using a native library.
 * This class leverages the 'redis_crc64' library to perform CRC64 operations.
 */
public class RedisCrc {
    static {
        // Load the native library containing the CRC64 functions.
        System.loadLibrary("redis_crc64");
    }

    /**
     * Initializes the CRC64 computation.
     * This method must be called before performing any CRC64 computation.
     */
    public static native void crc64Init();

    /**
     * Computes the CRC64 checksum of a given byte array.
     *
     * @param crc    the initial CRC64 value
     * @param data   the byte array to compute the CRC64 checksum for
     * @param offset the starting index of the data array
     * @param length the length of bytes in the array to consider for the CRC64 computation
     * @return the computed CRC64 checksum
     */
    public static native long crc64(long crc, final byte[] data, long offset, long length);
}