package io.velo.type;

import io.velo.Dict;
import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * List with Zstd compression support.
 */
public class RedisList {
    /**
     * Maximum list size (Short.MAX_VALUE).
     * Encoded and compressed length should be under 4KB.
     */
    public static short LIST_MAX_SIZE = Short.MAX_VALUE;

    /** Header length: size short + dict seq int + body length int + crc int */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    private final LinkedList<byte[]> list = new LinkedList<>();

    /** @return internal list of byte arrays */
    public LinkedList<byte[]> getList() {
        return list;
    }

    /** @return number of elements */
    public int size() {
        return list.size();
    }

    /**
     * @param e the element to add at beginning
     */
    public void addFirst(byte[] e) {
        list.addFirst(e);
    }

    /**
     * @param e the element to add at end
     */
    public void addLast(byte[] e) {
        list.add(e);
    }

    /**
     * @param index the index at which to add
     * @param e     the element to add
     */
    public void addAt(int index, byte[] e) {
        list.add(index, e);
    }

    /**
     * @param index the index at which to set
     * @param e     the element to set
     */
    public void setAt(int index, byte[] e) {
        list.set(index, e);
    }

    /**
     * @param b the element to search for
     * @return index or -1 if not found
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
     * @param index the index to retrieve
     * @return element at index
     */
    public byte[] get(int index) {
        return list.get(index);
    }

    /**
     * @return first element
     */
    public byte[] removeFirst() {
        return list.removeFirst();
    }

    /**
     * @return last element
     */
    public byte[] removeLast() {
        return list.removeLast();
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
        for (var e : list) {
            bodyBytesLength += 2 + e.length;
        }

        int size = list.size();
        if (size > Short.MAX_VALUE) {
            throw new IllegalStateException("List size " + size + " exceeds Short.MAX_VALUE");
        }

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort((short) size);
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        buffer.putInt(0);
        for (var e : list) {
            buffer.putShort((short) e.length);
            buffer.put(e);
        }

        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (bodyBytesLength > TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = RedisHH.compressIfBytesLengthIsLong(
                    dict, bodyBytesLength, rawBytesWithHeader, (short) size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /**
     * @param data the encoded byte array
     * @return size without decoding entire array
     */
    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    /**
     * @param data the byte array to decode
     * @return decoded RedisList
     */
    public static RedisList decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to check CRC32
     * @return decoded RedisList
     */
    public static RedisList decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        var r = new RedisList();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException(
                        "Invalid list entry length: " + len + ", expected > 0");
            }
            if (len > buffer.remaining()) {
                throw new IllegalStateException(
                        "Invalid list entry length: " + len + ", exceeds remaining buffer");
            }
            var bytes = new byte[len];
            buffer.get(bytes);
            r.list.add(bytes);
        }
        return r;
    }

    /**
     * Callback for iterating over encoded entries.
     */
    public interface IterateCallback {
        /**
         * @param bytes the element bytes
         * @param index the element index
         * @return true to break iteration
         */
        boolean on(byte[] bytes, int index);
    }

    /**
     * @param data         the byte array to iterate
     * @param doCheckCrc32 whether to check CRC32
     * @param callback     callback for each element
     */
    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException(
                        "Invalid list entry length: " + len + ", expected > 0");
            }
            if (len > buffer.remaining()) {
                throw new IllegalStateException(
                        "Invalid list entry length: " + len + ", exceeds remaining buffer");
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
