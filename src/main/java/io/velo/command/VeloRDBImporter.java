package io.velo.command;

import io.netty.buffer.ByteBuf;
import io.velo.rdb.RedisLzf;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

import java.nio.ByteBuffer;

public class VeloRDBImporter implements RDBImporter {
    public static boolean DEBUG = false;

    @Override
    public void restore(ByteBuf buf, RDBCallback callback) {
        if (DEBUG) {
            callback.onInteger(123);
            callback.onString("test_value".getBytes());

            var rhh = new RedisHH();
            rhh.put("test_key", "test_value".getBytes());
            callback.onHash(rhh);

            var rl = new RedisList();
            rl.addLast("abc".getBytes());
            rl.addLast("xyz".getBytes());
            callback.onList(rl);

            var rhk = new RedisHashKeys();
            rhk.add("member0");
            rhk.add("member1");
            callback.onSet(rhk);

            var rz = new RedisZSet();
            rz.add(1.0, "member0");
            rz.add(2.0, "member1");
            callback.onZSet(rz);

            return;
        }

        // Redis DUMP/RESTORE format: <type><value-bytes><rdb-version><crc64>
        int len = buf.readableBytes();
        if (len < 11) { // at least 1 type + 2 version + 8 crc64
            throw new IllegalArgumentException("Serialized value too short");
        }

        // RDB version: 2 bytes before last 8 bytes (little-endian)
        int rdbVersion = buf.getUnsignedShortLE(len - 10);
        // CRC64: last 8 bytes (little-endian)
        long expectedCrc = buf.getLongLE(len - 8);

        // Only support type 0 (string)
        int type = buf.getUnsignedByte(0);
        if (type != 0) {
            throw new IllegalArgumentException("Only string type (0) supported, got type: " + type);
        }

        // Value bytes: from after type (offset 1) up to footer (len - 11)
        int valueLen = len - 11;
        var valueBytes = new byte[valueLen];
        buf.getBytes(1, valueBytes);

        // CRC64 check: over all bytes except the last 8 (footer)
//        var crcInput = new byte[len - 8];
//        buf.getBytes(0, crcInput);
//        long actualCrc = RedisCrc.crc64(0, crcInput, crcInput.length);
//        if (actualCrc != expectedCrc) {
//            throw new IllegalArgumentException("CRC64 mismatch: expected " + expectedCrc + ", got " + actualCrc);
//        }

        // Now decode the string (RDB string encoding)
        buf.readerIndex(1).writerIndex(len - 10);
        var decoded = decodeRdbString(buf);
        callback.onString(decoded);
    }

    // Utility: decode RDB string (length-prefixed, integer, or LZF-compressed)
    private byte[] decodeRdbString(ByteBuf buf) {
        int[] lenType = new int[1];
        long len = decodeLength(buf, lenType);
        if (lenType[0] == 0) { // plain string
            var out = new byte[(int) len];
            buf.readBytes(out);
            return out;
        } else if (lenType[0] == 1) { // integer
            // Integer encoding: 8, 16, 32 bit
            int encType = (int) len;
            if (encType == 0) { // 8 bit
                return Integer.toString(buf.readByte()).getBytes();
            } else if (encType == 1) { // 16 bit
                return Integer.toString(buf.readShortLE()).getBytes();
            } else if (encType == 2) { // 32 bit
                return Integer.toString(buf.readIntLE()).getBytes();
            } else {
                throw new IllegalArgumentException("Unknown integer encoding: " + encType);
            }
        } else if (lenType[0] == 2) { // LZF compressed
            long clen = decodeLength(buf, new int[1]);
            long ulen = decodeLength(buf, new int[1]);
            var cdata = new byte[(int) clen];
            buf.readBytes(cdata);
            var out = new byte[(int) ulen];
            int res = RedisLzf.instance.lzf_decompress(
                    ByteBuffer.wrap(cdata), (int) clen,
                    ByteBuffer.wrap(out), (int) ulen);
            if (res <= 0) {
                throw new IllegalArgumentException("LZF decompress failed");
            }
            return out;
        } else {
            throw new IllegalArgumentException("Unknown RDB string encoding");
        }
    }

    // Utility: decode RDB length (returns value, sets lenType[0]: 0=plain, 1=integer, 2=LZF)
    private long decodeLength(ByteBuf buf, int[] lenType) {
        int first = buf.readUnsignedByte();
        int type = (first & 0xC0) >> 6;
        if (type == 0) { // 6 bit
            lenType[0] = 0;
            return first & 0x3F;
        } else if (type == 1) { // 14 bit
            lenType[0] = 0;
            int second = buf.readUnsignedByte();
            return ((first & 0x3F) << 8) | second;
        } else if (type == 2) { // 32 bit
            lenType[0] = 0;
            return buf.readInt();
        } else { // special encoding
            int enc = first & 0x3F;
            if (enc == 0) { // 8 bit int
                lenType[0] = 1;
                return 0;
            } else if (enc == 1) { // 16 bit int
                lenType[0] = 1;
                return 1;
            } else if (enc == 2) { // 32 bit int
                lenType[0] = 1;
                return 2;
            } else if (enc == 3) { // LZF compressed
                lenType[0] = 2;
                return 0;
            } else {
                throw new IllegalArgumentException("Unknown special encoding: " + enc);
            }
        }
    }
}
