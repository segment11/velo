package io.velo.type;

import com.github.luben.zstd.Zstd;
import io.velo.*;
import io.velo.persist.Wal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * Hash map with Zstd compression support.
 */
public class RedisHH {
    /** Prefix for members that should not be stored together. */
    public static final byte[] PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX = "h_not_hh_".getBytes();

    /** Header length: size short + dict seq int + body length int + crc int */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    private final HashMap<String, byte[]> map = new HashMap<>();

    private final HashMap<String, Long> mapExpireAt = new HashMap<>();

    /** @return internal map of key-value pairs */
    public HashMap<String, byte[]> getMap() {
        return map;
    }

    /** @return number of key-value pairs */
    public int size() {
        return map.size();
    }

    /**
     * @param key   the key
     * @param value the value
     */
    public void put(String key, byte[] value) {
        put(key, value, null);
    }

    /**
     * @param key      the key
     * @param value    the value
     * @param expireAt expiration time in milliseconds or null
     */
    public void put(@NotNull String key, byte[] value, @Nullable Long expireAt) {
        var keyBytes = Wal.keyBytes(key);
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "Key length too long, key length=" + keyBytes.length);
        }
        if (value.length > CompressedValue.VALUE_MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "Value length too long, value length=" + value.length);
        }
        map.put(key, value);

        if (expireAt != null) {
            mapExpireAt.put(key, expireAt);
        }
    }

    /**
     * @param key the key to remove
     * @return true if removed
     */
    public boolean remove(String key) {
        mapExpireAt.remove(key);
        return map.remove(key) != null;
    }

    /**
     * @param map the map to add
     */
    public void putAll(HashMap<String, byte[]> map) {
        this.map.putAll(map);
    }

    /**
     * @param key the key to retrieve
     * @return value or null if not found
     */
    public byte[] get(String key) {
        return map.get(key);
    }

    /**
     * @param key the key to retrieve expiration for
     * @return expiration time in milliseconds or 0 if not found
     */
    public long getExpireAt(String key) {
        var l = mapExpireAt.get(key);
        return l == null ? 0 : l;
    }

    /**
     * @param key      the key
     * @param expireAt expiration time in milliseconds
     */
    public void putExpireAt(String key, long expireAt) {
        mapExpireAt.put(key, expireAt);
    }

    /**
     * @return encoded byte array without compression
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * @return encoded and compressed byte array
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * @param dict compression dictionary or null
     * @return encoded byte array, possibly compressed
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var entry : map.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            bodyBytesLength += 8 + 2 + Wal.keyBytes(key).length + 4 + value.length;
        }

        int size = map.size();
        if (size > Short.MAX_VALUE) {
            throw new IllegalStateException("Hash size " + size + " exceeds Short.MAX_VALUE");
        }

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort((short) size);
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        buffer.putInt(0);
        for (var entry : map.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            var expireAt = mapExpireAt.get(key);
            buffer.putLong(expireAt == null ? CompressedValue.NO_EXPIRE : expireAt);
            var keyBytes = Wal.keyBytes(key);
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);
            buffer.putInt(value.length);
            buffer.put(value);
        }

        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (bodyBytesLength > TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = compressIfBytesLengthIsLong(
                    dict, bodyBytesLength, rawBytesWithHeader, (short) size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /** Preferred compression ratio. */
    @TestOnly
    static double PREFER_COMPRESS_RATIO = 0.9;

    /**
     * @param dict               compression dictionary
     * @param bodyBytesLength    body length
     * @param rawBytesWithHeader raw bytes with header
     * @param size               map size
     * @param crc                CRC value
     * @return compressed bytes if within ratio, otherwise null
     */
    static byte[] compressIfBytesLengthIsLong(Dict dict, int bodyBytesLength,
                                            byte[] rawBytesWithHeader, short size, int crc) {
        var dictSeq = dict.getSeq();

        var dst = new byte[((int) Zstd.compressBound(bodyBytesLength))];
        int compressedSize;
        if (dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compressByteArray(
                    dst, 0, dst.length, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength,
                    Zstd.defaultCompressionLevel());
        } else {
            compressedSize = dict.compressByteArray(
                    dst, 0, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength);
        }

        if (compressedSize < bodyBytesLength * PREFER_COMPRESS_RATIO) {
            var compressedBytes = new byte[compressedSize + HEADER_LENGTH];
            System.arraycopy(dst, 0, compressedBytes, HEADER_LENGTH, compressedSize);
            ByteBuffer buffer1 = ByteBuffer.wrap(compressedBytes);
            buffer1.putShort(size);
            buffer1.putInt(dictSeq);
            buffer1.putInt(bodyBytesLength);
            buffer1.putInt(crc);
            return compressedBytes;
        }
        return null;
    }

    /**
     * @param data the byte array to decode
     * @return decoded RedisHH
     */
    public static RedisHH decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to check CRC32
     * @return decoded RedisHH
     */
    public static RedisHH decode(byte[] data, boolean doCheckCrc32) {
        var r = new RedisHH();
        iterate(data, doCheckCrc32, (field, valueBytes, expireAt) -> {
            r.map.put(field, valueBytes);
            if (expireAt != CompressedValue.NO_EXPIRE) {
                r.mapExpireAt.put(field, expireAt);
            }
            return false;
        });
        return r;
    }

    /**
     * Callback for iterating over fields and values.
     */
    public interface IterateCallback {
        /**
         * @param field      the field name
         * @param valueBytes the value bytes
         * @param expireAt   expiration time in milliseconds
         * @return true to break iteration
         */
        boolean onField(String field, byte[] valueBytes, long expireAt);
    }

    /**
     * @param callback callback for each field-value pair
     */
    public void iterate(IterateCallback callback) {
        for (var entry : map.entrySet()) {
            var field = entry.getKey();
            var valueBytes = entry.getValue();
            var expireAt = mapExpireAt.get(field);
            if (callback.onField(field, valueBytes,
                    expireAt == null ? CompressedValue.NO_EXPIRE : expireAt)) {
                break;
            }
        }
    }

    /**
     * @param data         the encoded byte array
     * @param doCheckCrc32 whether to check CRC32
     * @param callback     callback for each field-value pair
     */
    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            buffer = decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        final long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            var expireAt = buffer.getLong();

            int keyLength = buffer.getShort();
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }

            if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
                buffer.position(buffer.position() + keyLength);
                var valueLength = buffer.getInt();
                buffer.position(buffer.position() + valueLength);
                continue;
            }

            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.getInt();
            if (valueLength > CompressedValue.VALUE_MAX_LENGTH || valueLength <= 0) {
                throw new IllegalStateException("Value length error, value length=" + valueLength);
            }

            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            if (callback.onField(Wal.keyString(keyBytes), valueBytes, expireAt)) {
                break;
            }
        }
    }

    private static final DictMap dictMap = DictMap.getInstance();

    /**
     * @param dictSeq         dictionary sequence
     * @param bodyBytesLength body length
     * @param data            compressed data
     * @return decompressed ByteBuffer
     * @throws DictMissingException if dictionary not found
     */
    static ByteBuffer decompressIfUseDict(int dictSeq, int bodyBytesLength, byte[] data) {
        if (dictSeq == Dict.SELF_ZSTD_DICT_SEQ) {
            var bodyBytes = new byte[bodyBytesLength];
            int decompressedSize = (int) Zstd.decompressByteArray(
                    bodyBytes, 0, bodyBytes.length, data, HEADER_LENGTH,
                    data.length - HEADER_LENGTH);
            if (decompressedSize <= 0) {
                throw new IllegalStateException("Decompress error");
            }
            return ByteBuffer.wrap(bodyBytes);
        } else {
            var dict = dictMap.getDictBySeq(dictSeq);
            if (dict == null) {
                throw new DictMissingException("Dict not found, dict seq=" + dictSeq);
            }

            var bodyBytes = new byte[bodyBytesLength];
            var decompressedSize = dict.decompressByteArray(
                    bodyBytes, 0, data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (decompressedSize <= 0) {
                throw new IllegalStateException("Decompress error");
            }
            return ByteBuffer.wrap(bodyBytes);
        }
    }
}
