package io.velo.type;

import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.KeyHash;
import io.velo.persist.Wal;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

/**
 * Hash keys set with Zstd compression support.
 */
public class RedisHashKeys {
    /**
     * Maximum size of the hash (4096).
     * Encoded and compressed length should be under 4KB.
     */
    public static short HASH_MAX_SIZE = 4096;

    /**
     * Maximum length of a set member.
     */
    public static short SET_MEMBER_MAX_LENGTH = 255;

    /**
     * Header length: size short + dict seq int + body length int + crc int
     */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /**
     * TTL metadata marker: ASCII "RHKT"
     */
    @VisibleForTesting
    static final int TTL_META_MARKER = 0x52484B54;

    private final TreeSet<String> set = new TreeSet<>();
    private final HashMap<String, Long> expireAtByField = new HashMap<>();
    private boolean ttlMetaEncoded = false;

    public boolean hasTtlMetaEncoded() {
        return ttlMetaEncoded;
    }

    public long getCachedExpireAt(String field) {
        var expireAt = expireAtByField.get(field);
        return expireAt == null ? CompressedValue.NO_EXPIRE : expireAt;
    }

    public void putCachedExpireAt(String field, long expireAt) {
        if (expireAt == CompressedValue.NO_EXPIRE) {
            expireAtByField.remove(field);
        } else {
            expireAtByField.put(field, expireAt);
            ttlMetaEncoded = true;
        }
    }

    public void clearCachedExpireAt(String field) {
        expireAtByField.remove(field);
    }

    public boolean isLiveByCache(String field, long now) {
        var expireAt = expireAtByField.get(field);
        if (expireAt == null || expireAt == CompressedValue.NO_EXPIRE) {
            return true;
        }
        return expireAt >= now;
    }

    public ArrayList<String> liveFieldsByCache() {
        return liveFieldsByCache(System.currentTimeMillis());
    }

    public ArrayList<String> liveFieldsByCache(long now) {
        var liveFields = new ArrayList<String>(set.size());
        for (var field : set) {
            if (isLiveByCache(field, now)) {
                liveFields.add(field);
            }
        }
        return liveFields;
    }

    /**
     * @param key the base key of the hash
     * @return generated key for storing hash keys
     */
    public static String keysKey(String key) {
        return "h_k_{" + key + "}";
    }

    /**
     * @param key   the base key of the hash
     * @param field the field name
     * @return generated key for storing a hash field
     */
    public static String fieldKey(String key, String field) {
        return "h_f_" + "{" + key + "}." + field;
    }

    /**
     * @return internal sorted set of field names
     */
    public TreeSet<String> getSet() {
        return set;
    }

    /**
     * @return number of field names
     */
    public int size() {
        return set.size();
    }

    /**
     * @param field the field name to check
     * @return true if contained
     */
    public boolean contains(String field) {
        return set.contains(field);
    }

    /**
     * @param field the field name to remove
     * @return true if removed
     */
    public boolean remove(String field) {
        expireAtByField.remove(field);
        return set.remove(field);
    }

    /**
     * @param field the field name to add
     * @return true if added, false if already present
     */
    public boolean add(String field) {
        return set.add(field);
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
        for (var e : set) {
            bodyBytesLength += 2 + Wal.keyBytes(e).length;
        }

        int size = set.size();
        if (size > Short.MAX_VALUE) {
            throw new IllegalStateException("HashKeys size " + size + " exceeds Short.MAX_VALUE");
        }

        // TTL metadata section: marker(4) + expireCount(2) + sum of (fieldLen(2) + fieldBytes + expireAt(8))
        int ttlEntries = 0;
        int ttlSectionLength = 0;
        if (size > 0) {
            for (var field : set) {
                var expireAt = expireAtByField.get(field);
                if (expireAt != null && expireAt != CompressedValue.NO_EXPIRE) {
                    ttlSectionLength += 2 + Wal.keyBytes(field).length + 8;
                    ttlEntries++;
                }
            }
        }
        int ttlMetaSectionLength = 4 + 2 + ttlSectionLength; // marker + count + entries

        int totalBodyLength = bodyBytesLength + ttlMetaSectionLength;

        var buffer = ByteBuffer.allocate(totalBodyLength + HEADER_LENGTH);
        buffer.putShort((short) size);
        buffer.putInt(0);
        buffer.putInt(totalBodyLength);
        buffer.putInt(0);
        for (var e : set) {
            var fieldBytes = Wal.keyBytes(e);
            buffer.putShort((short) fieldBytes.length);
            buffer.put(fieldBytes);
        }

        // Write TTL metadata section
        buffer.putInt(TTL_META_MARKER);
        buffer.putShort((short) ttlEntries);
        for (var field : set) {
            var expireAt = expireAtByField.get(field);
            if (expireAt != null && expireAt != CompressedValue.NO_EXPIRE) {
                var fieldBytes = Wal.keyBytes(field);
                buffer.putShort((short) fieldBytes.length);
                buffer.put(fieldBytes);
                buffer.putLong(expireAt);
            }
        }

        int crc = 0;
        if (totalBodyLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (totalBodyLength > TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = RedisHH.compressIfBytesLengthIsLong(
                    dict, totalBodyLength, rawBytesWithHeader, (short) size, crc);
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
     * @return decoded RedisHashKeys
     */
    public static RedisHashKeys decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to check CRC32
     * @return decoded RedisHashKeys
     */
    public static RedisHashKeys decode(byte[] data, boolean doCheckCrc32) {
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

        var r = new RedisHashKeys();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException("Length error, length=" + len);
            }
            if (len > buffer.remaining()) {
                throw new IllegalStateException(
                        "Length error, length=" + len + ", exceeds remaining buffer");
            }

            var bytes = new byte[len];
            buffer.get(bytes);
            r.set.add(Wal.keyString(bytes));
        }

        // Parse TTL metadata section if present
        if (buffer.hasRemaining()) {
            int marker = buffer.getInt();
            if (marker == TTL_META_MARKER) {
                int expireCount = buffer.getShort();
                for (int i = 0; i < expireCount; i++) {
                    int fieldLen = buffer.getShort();
                    if (fieldLen <= 0 || fieldLen > buffer.remaining() - 8) {
                        throw new IllegalStateException("TTL field length error: " + fieldLen);
                    }
                    var fieldBytes = new byte[fieldLen];
                    buffer.get(fieldBytes);
                    long expireAt = buffer.getLong();
                    String field = Wal.keyString(fieldBytes);
                    if (r.set.contains(field)) {
                        r.expireAtByField.put(field, expireAt);
                    }
                }
                r.ttlMetaEncoded = true;
            }
        } else {
            r.ttlMetaEncoded = false;
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
                throw new IllegalStateException("Length error, length=" + len);
            }
            if (len > buffer.remaining()) {
                throw new IllegalStateException(
                        "Length error, length=" + len + ", exceeds remaining buffer");
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
