package io.velo.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

public class RDBZipList {

    final static int zlHeaderSize = 10;
    final static int ZipListBigLen = 0xFE;
    final static int zlEnd = 0xFF;

    final static int ZIP_STR_MASK = 0xC0;
    final static int ZIP_STR_06B = (0 << 6);
    final static int ZIP_STR_14B = (1 << 6);
    final static int ZIP_STR_32B = (2 << 6);
    final static int ZIP_INT_16B = (0xC0 | 0 << 4);
    final static int ZIP_INT_32B = (0xC0 | 1 << 4);
    final static int ZIP_INT_64B = (0xC0 | 2 << 4);
    final static int ZIP_INT_24B = (0xC0 | 3 << 4);
    final static int ZIP_INT_8B = 0xFE;

    final static int ZIP_INT_IMM_MIN = 0xF1; /* 11110001 */
    final static int ZIP_INT_IMM_MAX = 0xFD; /* 11111101 */

    static ArrayList<String> parse(ByteBuf bb) {
        ArrayList<String> members = new ArrayList<>();

        bb.skipBytes(zlHeaderSize - 2);
        int zlLen = bb.readUnsignedShortLE();

        int preMemberLen = 0;
        for (int i = 0; i < zlLen; i++) {
            var b = bb.getUnsignedByte(bb.readerIndex());
            if (b == zlEnd) {
                break;
            }

            int preMemberEncodedLen = preMemberLen < ZipListBigLen ? 1 : 5;
            bb.skipBytes(preMemberEncodedLen);

            var b1 = bb.getUnsignedByte(bb.readerIndex());
            if (b1 < ZIP_STR_MASK) {
                b1 &= ZIP_STR_MASK;
            }

            int len = 0;
            if (b1 < ZIP_STR_MASK) {
                int readerIndex = bb.readerIndex();
                if (b1 == ZIP_STR_06B) {
                    len = bb.readUnsignedByte() & 0x3F;
                } else if (b1 == ZIP_STR_14B) {
                    len = (bb.readUnsignedByte() & 0x3F) << 8 | bb.readUnsignedByte();
                } else if (b1 == ZIP_STR_32B) {
                    len = bb.readUnsignedByte() << 24 | bb.readUnsignedByte() << 16 | bb.readUnsignedByte() << 8 | bb.readUnsignedByte();
                    // why 5?
                    bb.skipBytes(1);
                } else {
                    throw new IllegalArgumentException("Invalid string encoding");
                }

                var valueBytes = new byte[len];
                bb.readBytes(valueBytes);

                preMemberLen = bb.readerIndex() - readerIndex + preMemberEncodedLen;

                members.add(new String(valueBytes));
            } else {
                bb.skipBytes(1);
                String value;

                if (b1 == ZIP_INT_8B) {
                    preMemberLen = 2;
                    value = String.valueOf(bb.readUnsignedByte());
                } else if (b1 == ZIP_INT_16B) {
                    preMemberLen = 3;
                    value = String.valueOf(bb.readShortLE());
                } else if (b1 == ZIP_INT_24B) {
                    preMemberLen = 4;
                    value = String.valueOf(bb.readMediumLE());
                } else if (b1 == ZIP_INT_32B) {
                    preMemberLen = 5;
                    value = String.valueOf(bb.readIntLE());
                } else if (b1 == ZIP_INT_64B) {
                    preMemberLen = 9;
                    value = String.valueOf(bb.readLongLE());
                } else if (b1 >= ZIP_INT_IMM_MIN && b1 <= ZIP_INT_IMM_MAX) {
                    preMemberLen = 1;
                    value = String.valueOf((b1 & 0x0F) - 1);
                } else {
                    throw new IllegalArgumentException("Invalid ziplist encoding");
                }

                members.add(value);
            }
        }

        if (zlLen != members.size()) {
            throw new IllegalArgumentException("Invalid ziplist length");
        }

        return members;
    }
}
