package io.velo.rdb;

import com.sun.jna.Library;
import com.sun.jna.Native;

import java.nio.Buffer;

/**
 * Interface for interacting with the Redis LZF compression and decompression library.
 * This interface is loaded dynamically using JNA to call native C functions from the libredis_lzf.so library.
 */
public interface RedisLzf extends Library {

    /**
     * An instance of the RedisLzf interface which is loaded using JNA from the libredis_lzf.so shared library.
     */
    RedisLzf instance = Native.load("./libredis_lzf.so", RedisLzf.class);

    /**
     * Compresses the data buffer using LZF compression algorithm.
     *
     * @param input        The input buffer that contains the data to be compressed.
     * @param inputLength  The length of the input buffer to be compressed.
     * @param output       The buffer to hold the compressed data. The buffer must be large enough to hold the compressed data.
     * @param outputLength The size of the output buffer.
     * @return The actual size of the compressed data written to the output buffer.
     * If the return value is 0, no compression occurred (e.g., the data cannot be compressed).
     */
    int lzf_compress(final Buffer input, int inputLength, Buffer output, int outputLength);

    /**
     * Decompresses the data buffer that was compressed using LZF.
     *
     * @param input        The input buffer that contains the compressed data to be decompressed.
     * @param inputLength  The length of the input buffer with compressed data.
     * @param output       The buffer to hold the decompressed data. The buffer must be large enough to hold the original uncompressed data.
     * @param outputLength The size of the output buffer.
     * @return The actual size of the decompressed data written to the output buffer.
     * If the return value is less than or equal to 0, decompression failed.
     */
    int lzf_decompress(final Buffer input, int inputLength, Buffer output, int outputLength);
}