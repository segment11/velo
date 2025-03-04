package io.velo.type;

import io.velo.Dict;
import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * A class representing a list that can be encoded and decoded with optional compression using Zstandard.
 * This class is designed to handle a list of byte arrays and provides methods for encoding and decoding the data.
 */
public class RedisList {
    /**
     * The maximum size of the list. This is set to Short.MAX_VALUE.
     * This value can be changed by configuration.
     * The values encoded and compressed length should be less than or equal to 4KB, assuming a compression ratio of 0.25, then 16KB.
     * Assuming a value length of 32, then 16KB / 32 = 512.
     */
    public static short LIST_MAX_SIZE = Short.MAX_VALUE;

    /**
     * The length of the header in bytes
     * size short + dict seq int + body length int + crc int
     */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /**
     * The internal list to store byte arrays.
     */
    private final LinkedList<byte[]> list = new LinkedList<>();

    /**
     * Returns the internal list containing byte arrays.
     *
     * @return The internal list.
     */
    public LinkedList<byte[]> getList() {
        return list;
    }

    /**
     * Returns the number of elements in the list.
     *
     * @return The size of the list.
     */
    public int size() {
        return list.size();
    }

    /**
     * Adds an element to the beginning of the list.
     *
     * @param e The element to add.
     */
    public void addFirst(byte[] e) {
        list.addFirst(e);
    }

    /**
     * Adds an element to the end of the list.
     *
     * @param e The element to add.
     */
    public void addLast(byte[] e) {
        list.add(e);
    }

    /**
     * Adds an element at the specified index in the list.
     *
     * @param index The index at which to add the element.
     * @param e     The element to add.
     */
    public void addAt(int index, byte[] e) {
        list.add(index, e);
    }

    /**
     * Sets the element at the specified index in the list.
     *
     * @param index The index at which to set the element.
     * @param e     The element to set.
     */
    public void setAt(int index, byte[] e) {
        list.set(index, e);
    }

    /**
     * Returns the index of the specified element in the list.
     *
     * @param b The element to search for.
     * @return The index of the element, or -1 if the element is not found.
     */
    public int indexOf(byte[] b) {
        int i = 0;
        for (var e : list) {
            if (Arrays.equals(e, b)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    /**
     * Retrieves the element at the specified index.
     *
     * @param index The index of the element to retrieve.
     * @return The element at the specified index.
     */
    public byte[] get(int index) {
        return list.get(index);
    }

    /**
     * Removes and returns the first element from the list.
     *
     * @return The first element in the list.
     */
    public byte[] removeFirst() {
        return list.removeFirst();
    }

    /**
     * Removes and returns the last element from the list.
     *
     * @return The last element in the list.
     */
    public byte[] removeLast() {
        return list.removeLast();
    }

    /**
     * Encodes the list to a byte array without compression.
     *
     * @return The encoded byte array.
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * Encodes the list to a byte array with compression using the default dictionary.
     *
     * @return The encoded and compressed byte array.
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * Encodes the list to a byte array with optional compression using the specified dictionary.
     *
     * @param dict The dictionary to use for compression, or null if no compression is desired.
     * @return The encoded byte array, possibly compressed.
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var e : list) {
            // list value length use 2 bytes
            bodyBytesLength += 2 + e.length;
        }

        short size = (short) list.size();

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort(size);
        // tmp no dict seq
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        // tmp crc
        buffer.putInt(0);
        for (var e : list) {
            buffer.putShort((short) e.length);
            buffer.put(e);
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
     * Retrieves the size of the list without decoding the entire byte array.
     *
     * @param data The byte array containing the encoded list.
     * @return The size of the list.
     */
    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    /**
     * Decodes a byte array to a RedisList object. Checks the CRC32 by default.
     *
     * @param data The byte array to decode.
     * @return The RedisList object.
     */
    public static RedisList decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * Decodes a byte array to a RedisList object with optional CRC32 check.
     *
     * @param data         The byte array to decode.
     * @param doCheckCrc32 Whether to check the CRC32.
     * @return The RedisList object.
     */
    public static RedisList decode(byte[] data, boolean doCheckCrc32) {
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

        var r = new RedisList();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            var bytes = new byte[len];
            buffer.get(bytes);
            r.list.add(bytes);
        }
        return r;
    }
}