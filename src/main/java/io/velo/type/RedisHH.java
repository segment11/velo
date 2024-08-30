package io.velo.type;

import com.github.luben.zstd.Zstd;
import io.velo.*;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

// key / value save together
public class RedisHH {
    public static final byte[] PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX = "h_not_hh_".getBytes();

    @VisibleForTesting
    // size short + dict seq int + body bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    private final HashMap<String, byte[]> map = new HashMap<>();

    public HashMap<String, byte[]> getMap() {
        return map;
    }

    public int size() {
        return map.size();
    }

    public void put(String key, byte[] value) {
        if (key.length() > CompressedValue.KEY_MAX_LENGTH) {
            throw new IllegalArgumentException("Key length too long, key length: " + key.length());
        }
        if (value.length > CompressedValue.VALUE_MAX_LENGTH) {
            throw new IllegalArgumentException("Value length too long, value length: " + value.length);
        }
        map.put(key, value);
    }

    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    public void putAll(HashMap<String, byte[]> map) {
        this.map.putAll(map);
    }

    public byte[] get(String key) {
        return map.get(key);
    }

    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var entry : map.entrySet()) {
            // key / value length use 2 bytes
            var key = entry.getKey();
            var value = entry.getValue();
            bodyBytesLength += 2 + key.length() + 2 + value.length;
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
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.putShort((short) value.length);
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

    @TestOnly
    static double PREFER_COMPRESS_RATIO = 0.9;

    static byte[] compressIfBytesLengthIsLong(Dict dict, int bodyBytesLength, byte[] rawBytesWithHeader, short size, int crc) {
        var dictSeq = dict.getSeq();

        var dst = new byte[((int) Zstd.compressBound(bodyBytesLength))];
        int compressedSize;
        if (dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compressByteArray(dst, 0, dst.length, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength, Zstd.defaultCompressionLevel());
        } else {
            compressedSize = (int) Zstd.compressUsingDict(dst, 0, rawBytesWithHeader, HEADER_LENGTH, bodyBytesLength, dict.getDictBytes(), Zstd.defaultCompressionLevel());
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

    public static RedisHH decode(byte[] data) {
        return decode(data, true);
    }

    public static RedisHH decode(byte[] data, boolean doCheckCrc32) {
        var r = new RedisHH();
        iterate(data, doCheckCrc32, (field, valueBytes) -> {
            r.map.put(field, valueBytes);
            return false;
        });
        return r;
    }

    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public interface IterateCallback {
        boolean onField(String field, byte[] valueBytes);
    }

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
                throw new IllegalStateException("Crc check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int keyLength = buffer.getShort();
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length: " + keyLength);
            }

            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.getShort();
            if (valueLength <= 0) {
                throw new IllegalStateException("Value length error, value length: " + valueLength);
            }

            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            var isBreak = callback.onField(new String(keyBytes), valueBytes);
            if (isBreak) {
                break;
            }
        }
    }

    static ByteBuffer decompressIfUseDict(int dictSeq, int bodyBytesLength, byte[] data) {
        if (dictSeq == Dict.SELF_ZSTD_DICT_SEQ) {
            var bodyBytes = new byte[bodyBytesLength];
            int decompressedSize = (int) Zstd.decompressByteArray(bodyBytes, 0, bodyBytes.length, data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (decompressedSize <= 0) {
                throw new IllegalStateException("Decompress error");
            }
            return ByteBuffer.wrap(bodyBytes);
        } else {
            var dict = DictMap.getInstance().getDictBySeq(dictSeq);
            if (dict == null) {
                throw new DictMissingException("Dict not found, dict seq: " + dictSeq);
            }

            var bodyBytes = new byte[bodyBytesLength];
            int decompressedSize = (int) Zstd.decompressUsingDict(bodyBytes, 0, data, HEADER_LENGTH, data.length - HEADER_LENGTH, dict.getDictBytes());
            if (decompressedSize <= 0) {
                throw new IllegalStateException("Decompress error");
            }
            return ByteBuffer.wrap(bodyBytes);
        }
    }
}
