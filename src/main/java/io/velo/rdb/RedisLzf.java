package io.velo.rdb;

import com.sun.jna.Library;
import com.sun.jna.Native;

import java.nio.Buffer;

public interface RedisLzf extends Library {
    RedisLzf instance = Native.load("./libredis_lzf.so", RedisLzf.class);

    int lzf_compress(final Buffer input, int inputLength, Buffer output, int outputLength);

    int lzf_decompress(final Buffer input, int inputLength, Buffer output, int outputLength);
}
