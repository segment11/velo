package io.velo.type;

import io.velo.Dict;
import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * A class representing a set of hash keys that can be encoded and decoded with optional compression using Zstandard.
 * This class is designed to handle a sorted set of field names and provides methods for encoding and decoding the data.
 */
public class RedisHashKeys {
    /**
     * The maximum size of the hash. This is set to 4096.
     * This value can be changed by configuration.
     * The keys encoded and compressed length should be less than or equal to 4KB, assuming a compression ratio of 0.25, then 16KB.
     * Assuming a key length of 32, then 16KB / 32 = 512.
     */
    public static short HASH_MAX_SIZE = 4096;

    /**
     * The maximum length of a set member. This is set to 255.
     */
    public static short SET_MEMBER_MAX_LENGTH = 255;

    /**
     * The length of the header in bytes
     * size short + dict seq int + body length int + crc int
     */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /**
     * Generates a key for storing the keys of a hash, ensuring all keys are in the same slot.
     *
     * @param key the base key of the hash
     * @return the generated key
     */
    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String keysKey(String key) {
        // add hashtag to make sure all keys in one slot
        return "h_k_{" + key + "}";
    }

    /**
     * Generates a key for storing a field of a hash, ensuring all fields of the same key are in the same slot.
     *
     * @param key   the base key of the hash
     * @param field the field name
     * @return the generated key
     */
    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String fieldKey(String key, String field) {
        // add hashtag to make sure all keys in one slot
        // add . to make sure same key use the same dict when compress
        return "h_f_" + "{" + key + "}." + field;
    }

    /**
     * The internal set to store sorted field names.
     */
    private final TreeSet<String> set = new TreeSet<>();

    /**
     * Returns the internal sorted set containing field names.
     *
     * @return the internal set
     */
    public TreeSet<String> getSet() {
        return set;
    }

    /**
     * Returns the number of field names in the set.
     *
     * @return The size of the set.
     */
    public int size() {
        return set.size();
    }

    /**
     * Checks if the set contains the specified field name.
     *
     * @param field the field name to check
     * @return true if the field name is contained in the set, false otherwise
     */
    public boolean contains(String field) {
        return set.contains(field);
    }

    /**
     * Removes the specified field name from the set.
     *
     * @param field the field name to remove
     * @return true if the field name was removed, false otherwise
     */
    public boolean remove(String field) {
        return set.remove(field);
    }

    /**
     * Adds a field name to the set.
     *
     * @param field the field name to add
     * @return true if the field name was added, false if it was already present
     */
    public boolean add(String field) {
        return set.add(field);
    }

    /**
     * Encodes the set of field names to a byte array without compression.
     *
     * @return the encoded byte array
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * Encodes the set of field names to a byte array with compression using the default dictionary.
     *
     * @return the encoded and compressed byte array
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * Encodes the set of field names to a byte array with optional compression using the specified dictionary.
     *
     * @param dict the dictionary to use for compression, or null if no compression is desired
     * @return the encoded byte array, possibly compressed
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var e : set) {
            // key length use 2 bytes short
            bodyBytesLength += 2 + e.length();
        }

        short size = (short) set.size();

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort(size);
        // tmp no dict seq
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        // tmp crc
        buffer.putInt(0);
        for (var e : set) {
            buffer.putShort((short) e.length());
            buffer.put(e.getBytes());
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
            var compressedBytes = RedisHH.compressIfBytesLengthIsLong(dict, bodyBytesLength, rawBytesWithHeader, size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /**
     * Retrieves the size of the set without decoding the entire byte array.
     *
     * @param data the byte array containing the encoded set
     * @return the size of the set
     */
    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    /**
     * Decodes a byte array to a RedisHashKeys object. Checks the CRC32 by default.
     *
     * @param data the byte array to decode
     * @return the RedisHashKeys object
     */
    public static RedisHashKeys decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * Decodes a byte array to a RedisHashKeys object with optional CRC32 check.
     *
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to check the CRC32
     * @return the RedisHashKeys object
     */
    public static RedisHashKeys decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            // decompress first
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        var r = new RedisHashKeys();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException("Length error, length=" + len);
            }

            var bytes = new byte[len];
            buffer.get(bytes);
            r.set.add(new String(bytes));
        }
        return r;
    }

    /**
     * Iterates over the byte array and calls the callback for each element.
     */
    public interface IterateCallback {
        /**
         * Called for each element.
         *
         * @param bytes the element bytes
         * @param index the index of the element
         * @return true to break, false to continue
         */
        boolean on(byte[] bytes, int index);
    }

    /**
     * Iterates over the byte array and calls the callback for each element.
     *
     * @param data         the byte array to iterate
     * @param doCheckCrc32 whether to check the CRC32
     * @param callback     the callback to call for each element
     */
    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            // decompress first
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException("Length error, length=" + len);
            }

            var bytes = new byte[len];
            buffer.get(bytes);
            var isBreak = callback.on(bytes, i);
            if (isBreak) {
                break;
            }
        }
    }
}