package io.velo.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

// refer kvrocks rdb_listpack.cc
public class RDBListPack {

    final static int ListPack7BitUIntMask = 0x80;
    final static int ListPack7BitUInt = 0;
    // skip 1byte for encoding type and 1byte for element length
    final static int ListPack7BitIntEntrySize = 2;

    final static int ListPack6BitStringMask = 0xC0;
    final static int ListPack6BitString = 0x80;

    final static int ListPack13BitIntMask = 0xE0;
    final static int ListPack13BitInt = 0xC0;
    // skip 2byte for encoding type and 1byte for element length
    final static int ListPack13BitIntEntrySize = 3;

    final static int ListPack12BitStringMask = 0xF0;
    final static int ListPack12BitString = 0xE0;

    final static int ListPack16BitIntMask = 0xFF;
    final static int ListPack16BitInt = 0xF1;
    // skip 3byte for encoding type and 1byte for element length
    final static int ListPack16BitIntEntrySize = 4;

    final static int ListPack24BitIntMask = 0xFF;
    final static int ListPack24BitInt = 0xF2;
    // skip 4byte for encoding type and 1byte for element length
    final static int ListPack24BitIntEntrySize = 5;

    final static int ListPack32BitIntMask = 0xFF;
    final static int ListPack32BitInt = 0xF3;
    // skip 5byte for encoding type and 1byte for element length
    final static int ListPack32BitIntEntrySize = 6;

    final static int ListPack64BitIntMask = 0xFF;
    final static int ListPack64BitInt = 0xF4;
    // skip 9byte for encoding type and 1byte for element length
    final static int ListPack64BitIntEntrySize = 10;

    final static int ListPack32BitStringMask = 0xFF;
    final static int ListPack32BitString = 0xF0;

    final static int ListPackEOF = 0xFF;

    final static int listPackHeaderSize = 6;

    static int encodeBackLen(int len) {
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

    static ArrayList<String> parse(ByteBuf bb) {
        int size = bb.readableBytes();
        if (size < listPackHeaderSize) {
            throw new IllegalArgumentException("Invalid listpack length");
        }

        int totalBytes = bb.readIntLE();
        if (totalBytes != size) {
            throw new IllegalArgumentException("Invalid listpack length");
        }

        // member count
        int len = bb.readShortLE();

        ArrayList<String> members = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            int valueLen = 0;
            long intValue = 0;
            byte[] valueBytes = null;

            int c = bb.getUnsignedByte(bb.readerIndex());

            if ((c & ListPack7BitUIntMask) == ListPack7BitUInt) {  // 7bit unsigned int
                intValue = c & 0x7F;
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack6BitStringMask) == ListPack6BitString) {  // 6bit string
                valueLen = c & 0x3F;
                // skip the encoding type byte
                bb.skipBytes(1);
                valueBytes = new byte[valueLen];
                bb.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                bb.skipBytes(encodeBackLen(valueLen + 1));
            } else if ((c & ListPack13BitIntMask) == ListPack13BitInt) {  // 13bit int
                intValue = ((c & 0x1F) << 8) | bb.readUnsignedByte();
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack16BitIntMask) == ListPack16BitInt) {  // 16bit int
                intValue = bb.readUnsignedShortLE();
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack24BitIntMask) == ListPack24BitInt) {  // 24bit int
                intValue = bb.readUnsignedMediumLE();
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack32BitIntMask) == ListPack32BitInt) {  // 32bit int
                intValue = bb.readUnsignedIntLE();
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack64BitIntMask) == ListPack64BitInt) {  // 64bit int
                intValue = bb.readLongLE();
                valueBytes = String.valueOf(intValue).getBytes();
                bb.skipBytes(2);
            } else if ((c & ListPack12BitStringMask) == ListPack12BitString) {  // 12bit string
                valueLen = bb.readUnsignedShortLE();
                valueBytes = new byte[valueLen];
                bb.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                bb.skipBytes(encodeBackLen(valueLen + 2));
            } else if ((c & ListPack32BitStringMask) == ListPack32BitString) {  // 32bit string
                valueLen = bb.readIntLE();
                // skip 5byte encoding type
                bb.skipBytes(1);
                valueBytes = new byte[valueLen];
                bb.readBytes(valueBytes);
                // skip the value bytes and the length of the element
                bb.skipBytes(encodeBackLen(valueLen + 5));
            } else if (c == ListPackEOF) {
                break;
            } else {
                throw new IllegalArgumentException("Invalid listpack entry");
            }

            members.add(new String(valueBytes));
        }
        return members;
    }
}
