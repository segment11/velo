package io.velo.command;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;

public class RDBZipMap {
    final static byte ZipMapBigLen = (byte) 254;
    final static byte ZipMapEOF = (byte) 0xFF;

    static HashMap<String, byte[]> parse(ByteBuf bb) {
        HashMap<String, byte[]> map = new HashMap<>();

        var zmLen = bb.readUnsignedByte();

        while (bb.readableBytes() > 0) {
            int b = bb.readUnsignedByte();
            if (b == ZipMapEOF) {
                break;
            }

            int fieldLen;
            if (b == ZipMapBigLen) {
                fieldLen = bb.readIntLE();
            } else {
                fieldLen = b;
            }

            var fieldBytes = new byte[fieldLen];
            bb.readBytes(fieldBytes);

            int b1 = bb.readUnsignedByte();
            int fieldValueLen;
            if (b1 == ZipMapBigLen) {
                fieldValueLen = bb.readIntLE();
            } else {
                fieldValueLen = b1;
            }

            // why + 1?
            bb.skipBytes(1);

            var fieldValueBytes = new byte[fieldValueLen];
            bb.readBytes(fieldValueBytes);

            map.put(new String(fieldBytes), fieldValueBytes);
        }

        if (zmLen < ZipMapBigLen && zmLen != map.size()) {
            throw new IllegalArgumentException("Invalid zipmap length");
        }

        return map;
    }
}
