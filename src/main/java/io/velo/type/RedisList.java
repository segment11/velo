package io.velo.type;

import io.velo.Dict;
import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static io.velo.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public class RedisList {
    // change here to limit list size
    // values encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose value length is 32, then 16KB / 32 = 512
    public static final short LIST_MAX_SIZE = 1024;

    @VisibleForTesting
    // size short + dict seq int + body bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    private final LinkedList<byte[]> list = new LinkedList<>();

    public LinkedList<byte[]> getList() {
        return list;
    }

    public int size() {
        return list.size();
    }

    public void addFirst(byte[] e) {
        list.addFirst(e);
    }

    public void addLast(byte[] e) {
        list.add(e);
    }

    public void addAt(int index, byte[] e) {
        list.add(index, e);
    }

    public void setAt(int index, byte[] e) {
        list.set(index, e);
    }

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

    public byte[] get(int index) {
        return list.get(index);
    }

    public byte[] removeFirst() {
        return list.removeFirst();
    }

    public byte[] removeLast() {
        return list.removeLast();
    }

    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

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

    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public static RedisList decode(byte[] data) {
        return decode(data, true);
    }

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
                throw new IllegalStateException("Crc check failed");
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
