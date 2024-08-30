package io.velo.type;

import io.velo.Dict;
import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

// key save together, one field value save as single key
public class RedisHashKeys {
    // change here to limit hash size
    // keys encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose key length is 32, then 16KB / 32 = 512
    public static final short HASH_MAX_SIZE = 4096;

    public static final int SET_MEMBER_MAX_LENGTH = 255;

    @VisibleForTesting
    // size short + dict seq int + body bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String keysKey(String key) {
        // add hashtag to make sure all keys in one slot
        return "h_k_{" + key + "}";
    }

    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String fieldKey(String key, String field) {
        // add hashtag to make sure all keys in one slot
        // add . to make sure same key use the same dict when compress
        return "h_f_" + "{" + key + "}." + field;
    }

    // sorted fields
    private final TreeSet<String> set = new TreeSet<>();

    public TreeSet<String> getSet() {
        return set;
    }

    public int size() {
        return set.size();
    }

    public boolean contains(String field) {
        return set.contains(field);
    }

    public boolean remove(String field) {
        return set.remove(field);
    }

    public boolean add(String field) {
        return set.add(field);
    }

    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

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

    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public static RedisHashKeys decode(byte[] data) {
        return decode(data, true);
    }

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
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisHashKeys();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException("Length error, length: " + len);
            }

            var bytes = new byte[len];
            buffer.get(bytes);
            r.set.add(new String(bytes));
        }
        return r;
    }
}
