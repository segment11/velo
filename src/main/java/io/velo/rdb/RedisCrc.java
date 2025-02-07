package io.velo.rdb;

public class RedisCrc {
    static {
        System.loadLibrary("redis_crc64");
    }

    public static native void crc64Init();

    public static native long crc64(long crc, final byte[] data, long l);
}
