package io.velo.type;

import com.github.luben.zstd.Zstd;
import io.velo.*;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * A class representing a hash map that can be encoded and decoded with optional compression using Zstd.
 * This class is designed to handle key-value pairs and provides methods for encoding and decoding the data.
 */
public class RedisHH {
    /**
     * A prefix used to indicate that a member should not be stored together.
     */
    public static final byte[] PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX = "h_not_hh_".getBytes();

    /**
     * The length of the header in bytes
     * size short + dict seq int + body length int + crc int
     */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /**
     * The internal map to store key-value pairs.
     */
    private final HashMap<String, byte[]> map = new HashMap<>();

    /**
     * The internal map to store expire at.
     */
    private final HashMap<String, Long> mapExpireAt = new HashMap<>();

    /**
     * Returns the internal map containing key-value pairs.
     *
     * @return The internal map.
     */
    public HashMap<String, byte[]> getMap() {
        return map;
    }

    /**
     * Returns the number of key-value pairs in the map.
     *
     * @return The size of the map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Adds a key-value pair to the map.
     *
     * @param key   The key of the pair.
     * @param value The value of the pair.
     */
    public void put(String key, byte[] value) {
        put(key, value, null);
    }

    /**
     * Adds a key-value pair to the map.
     *
     * @param key      The key of the pair.
     * @param value    The value of the pair.
     * @param expireAt The expire at milliseconds of the pair.
     * @throws IllegalArgumentException if the key or value length exceeds the maximum allowed length.
     */
    public void put(String key, byte[] value, Long expireAt) {
        if (key.length() > CompressedValue.KEY_MAX_LENGTH) {
            throw new IllegalArgumentException("Key length too long, key length=" + key.length());
        }
        if (value.length > CompressedValue.VALUE_MAX_LENGTH) {
            throw new IllegalArgumentException("Value length too long, value length=" + value.length);
        }
        map.put(key, value);

        if (expireAt != null) {
            mapExpireAt.put(key, expireAt);
        }
    }

    /**
     * Removes a key-value pair from the map by key.
     *
     * @param key The key of the pair to remove.
     * @return True if the pair was removed, false otherwise.
     */
    public boolean remove(String key) {
        mapExpireAt.remove(key);
        return map.remove(key) != null;
    }

    /**
     * Adds all key-value pairs from the provided map to the internal map.
     *
     * @param map The map containing key-value pairs to add.
     */
    public void putAll(HashMap<String, byte[]> map) {
        this.map.putAll(map);
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key The key of the pair to retrieve.
     * @return The value associated with the key, or null if the key is not found.
     */
    public byte[] get(String key) {
        return map.get(key);
    }

    /**
     * Retrieves the expire at milliseconds associated with the specified key.
     *
     * @param key The key of the pair to retrieve.
     * @return The expire at milliseconds associated with the key, or 0 if the key is not found.
     */
    public long getExpireAt(String key) {
        var l = mapExpireAt.get(key);
        return l == null ? 0 : l;
    }

    /**
     * Puts the expire at milliseconds associated with the specified key.
     *
     * @param key      The key of the pair to put.
     * @param expireAt The expire at milliseconds to put.
     */
    public void putExpireAt(String key, long expireAt) {
        mapExpireAt.put(key, expireAt);
    }

    /**
     * Encodes the map to a byte array without compression.
     *
     * @return The encoded byte array.
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * Encodes the map to a byte array with compression using the default dictionary.
     *
     * @return The encoded and compressed byte array.
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * Encodes the map to a byte array with optional compression using the specified dictionary.
     *
     * @param dict The dictionary to use for compression, or null if no compression is desired.
     * @return The encoded byte array, possibly compressed.
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var entry : map.entrySet()) {
            // key / value length use 2 bytes, expire at milliseconds use 8 bytes
            var key = entry.getKey();
            var value = entry.getValue();
            bodyBytesLength += 8 + 2 + key.getBytes().length + 4 + value.length;
        }

        short size = (short) map.size();

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort(size);
        // tmp no dict seq
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        // tmp crc
        buffer.putInt(0);
        for (var entry : map.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            var expireAt = mapExpireAt.get(key);
            buffer.putLong(expireAt == null ? CompressedValue.NO_EXPIRE : expireAt);
            buffer.putShort((short) key.getBytes().length);
            buffer.put(key.getBytes());
            buffer.putInt(value.length);
            buffer.put(value);
        }

        // crc
        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (bodyBytesLength > TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = compressIfBytesLengthIsLong(dict, bodyBytesLength, rawBytesWithHeader, size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /**
     * The preferred compression ratio for compressing data.
     */
    @TestOnly
    static double PREFER_COMPRESS_RATIO = 0.9;

    /**
     * Compresses the byte array if the compressed size is within the preferred compression ratio.
     *
     * @param dict               The dictionary to use for compression.
     * @param bodyBytesLength    The length of the body bytes.
     * @param rawBytesWithHeader The raw byte array with header.
     * @param size               The size of the map.
     * @param crc                The CRC value.
     * @return The compressed byte array if compression is successful, otherwise null.
     */
    static byte[] compressIfBytesLengthIsLong(Dict dict, int bodyBytesLength, byte[] rawBytesWithHeader, short size, int crc) {
        var dictSeq = dict.getSeq();

        var dst = new byte[((int) Zstd.compressBound(bodyBytesLength))];
        int compressedSize;
        if (dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compressByteArray(dst, 0, dst.length, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength, Zstd.defaultCompressionLevel());
        } else {
            compressedSize = dict.compressByteArray(dst, 0, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength);
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
     * Decodes a byte array to a RedisHH object. Checks the CRC32 by default.
     *
     * @param data The byte array to decode.
     * @return The RedisHH object.
     */
    public static RedisHH decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * Decodes a byte array to a RedisHH object with optional CRC32 check.
     *
     * @param data         The byte array to decode.
     * @param doCheckCrc32 Whether to check the CRC32.
     * @return The RedisHH object.
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
     * An interface for iterating over fields and values in a RedisHH object.
     */
    public interface IterateCallback {
        /**
         * Called for each field and value pair.
         *
         * @param field      The field name.
         * @param valueBytes The value bytes.
         * @param expireAt   The expire at milliseconds.
         * @return True to break the iteration, false to continue.
         */
        boolean onField(String field, byte[] valueBytes, long expireAt);
    }

    /**
     * Iterates over the fields and values in a RedisHH object.
     *
     * @param callback The callback to be called for each field and value pair.
     */
    public void iterate(IterateCallback callback) {
        for (var entry : map.entrySet()) {
            var field = entry.getKey();
            var valueBytes = entry.getValue();
            var expireAt = mapExpireAt.get(field);
            if (callback.onField(field, valueBytes, expireAt == null ? 0 : expireAt)) {
                break;
            }
        }
    }

    /**
     * Iterates over the fields and values in a byte array representing a RedisHH object.
     *
     * @param data         The byte array containing the encoded map.
     * @param doCheckCrc32 Whether to check the CRC32.
     * @param callback     The callback to be called for each field and value pair.
     */
    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            // decompress first
            buffer = decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        var currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            var expireAt = buffer.getLong();

            int keyLength = buffer.getShort();
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }

            // skip expired
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
            if (callback.onField(new String(keyBytes), valueBytes, expireAt)) {
                break;
            }
        }
    }

    private static final DictMap dictMap = DictMap.getInstance();

    /**
     * Decompresses a byte array using the specified dictionary sequence.
     *
     * @param dictSeq         The dictionary sequence.
     * @param bodyBytesLength The length of the body bytes.
     * @param data            The byte array to decompress.
     * @return The decompressed ByteBuffer.
     * @throws IllegalStateException if decompression fails.
     * @throws DictMissingException  if the dictionary is not found.
     */
    static ByteBuffer decompressIfUseDict(int dictSeq, int bodyBytesLength, byte[] data) {
        if (dictSeq == Dict.SELF_ZSTD_DICT_SEQ) {
            var bodyBytes = new byte[bodyBytesLength];
            int decompressedSize = (int) Zstd.decompressByteArray(bodyBytes, 0, bodyBytes.length, data, HEADER_LENGTH, data.length - HEADER_LENGTH);
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
            var decompressedSize = dict.decompressByteArray(bodyBytes, 0, data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (decompressedSize <= 0) {
                throw new IllegalStateException("Decompress error");
            }
            return ByteBuffer.wrap(bodyBytes);
        }
    }
}