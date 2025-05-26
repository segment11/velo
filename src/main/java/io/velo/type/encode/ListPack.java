package io.velo.type.encode;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

// copy from kvrocks rdb_listpack.cc
public class ListPack {
    private static final int ListPack7BitUIntMask = 0x80;
    private static final int ListPack7BitUInt = 0;
    private static final int ListPack7BitIntEntrySize = 2;

    private static final int ListPack6BitStringMask = 0xC0;
    private static final int ListPack6BitString = 0x80;

    private static final int ListPack13BitIntMask = 0xE0;
    private static final int ListPack13BitInt = 0xC0;
    private static final int ListPack13BitIntEntrySize = 3;

    private static final int ListPack12BitStringMask = 0xF0;
    private static final int ListPack12BitString = 0xE0;

    private static final int ListPack16BitIntMask = 0xFF;
    private static final int ListPack16BitInt = 0xF1;
    private static final int ListPack16BitIntEntrySize = 4;

    private static final int ListPack24BitIntMask = 0xFF;
    private static final int ListPack24BitInt = 0xF2;
    private static final int ListPack24BitIntEntrySize = 5;

    private static final int ListPack32BitIntMask = 0xFF;
    private static final int ListPack32BitInt = 0xF3;
    private static final int ListPack32BitIntEntrySize = 6;

    private static final int ListPack64BitIntMask = 0xFF;
    private static final int ListPack64BitInt = 0xF4;
    private static final int ListPack64BitIntEntrySize = 10;

    private static final int ListPack32BitStringMask = 0xFF;
    private static final int ListPack32BitString = 0xF0;

    private static final int ListPackEOF = 0xFF;

    private static final int listPackHeaderSize = 6;

    private static int encodeBackLen(int len) {
        if (len <= 127) {
            return 1;
        } else if (len < 16383) {
            return 2;
        } else if (len < 2097151) {
            return 3;
        } else if (len < 268435455) {
            return 4;
        } else {
            return 5;
        }
    }

    public static List<byte[]> decode(ByteBuf buf) {
        int size = buf.readableBytes();
        if (size < listPackHeaderSize) {
            throw new IllegalArgumentException("Invalid listpack length");
        }

        int totalBytes = buf.readIntLE();
        if (totalBytes != size) {
            throw new IllegalArgumentException("Invalid listpack length");
        }

        // member count
        int len = buf.readShortLE();

        List<byte[]> members = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            int valueLen;
            long intValue;
            byte[] valueBytes;

            int c = buf.getUnsignedByte(buf.readerIndex());

            if ((c & ListPack7BitUIntMask) == ListPack7BitUInt) {  // 7bit unsigned int
                intValue = c & 0x7F;
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(ListPack7BitIntEntrySize);
            } else if ((c & ListPack6BitStringMask) == ListPack6BitString) {  // 6bit string
                valueLen = c & 0x3F;
                // skip the encoding type byte
                buf.skipBytes(1);
                valueBytes = new byte[valueLen];
                buf.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                buf.skipBytes(encodeBackLen(valueLen + 1));
            } else if ((c & ListPack13BitIntMask) == ListPack13BitInt) {  // 13bit int
                buf.skipBytes(1);
                intValue = ((c & 0x1F) << 8) | buf.readUnsignedByte();
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(1);
            } else if ((c & ListPack16BitIntMask) == ListPack16BitInt) {  // 16bit int
                buf.skipBytes(1);
                intValue = buf.readShortLE();
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(1);
            } else if ((c & ListPack24BitIntMask) == ListPack24BitInt) {  // 24bit int
                buf.skipBytes(1);
                intValue = buf.readUnsignedMediumLE();
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(1);
            } else if ((c & ListPack32BitIntMask) == ListPack32BitInt) {  // 32bit int
                buf.skipBytes(1);
                intValue = buf.readUnsignedIntLE();
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(1);
            } else if ((c & ListPack64BitIntMask) == ListPack64BitInt) {  // 64bit int
                buf.skipBytes(1);
                intValue = buf.readLongLE();
                valueBytes = String.valueOf(intValue).getBytes();
                buf.skipBytes(1);
            } else if ((c & ListPack12BitStringMask) == ListPack12BitString) {  // 12bit string
                buf.skipBytes(1);
                valueLen = buf.readUnsignedShortLE() & 0xFFF;
                valueBytes = new byte[valueLen];
                buf.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                buf.skipBytes(encodeBackLen(valueLen + 2));
            } else if ((c & ListPack32BitStringMask) == ListPack32BitString) {  // 32bit string
                buf.skipBytes(1);
                valueLen = buf.readIntLE();
                valueBytes = new byte[valueLen];
                buf.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                buf.skipBytes(encodeBackLen(valueLen + 5));
            } else if (c == ListPackEOF) {
                break;
            } else {
                throw new IllegalArgumentException("Invalid listpack entry");
            }

            members.add(valueBytes);
        }
        return members;
    }
}
