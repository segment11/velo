package io.velo.type.encode;

import io.netty.buffer.ByteBuf;

import java.util.function.BiConsumer;

// copy from kvrocks rdb_ziplist.cc
public class ZipList {
    private static final int ZIP_STR_MASK = 0xC0;
    private static final int ZIP_STR_06B = 0;
    private static final int ZIP_STR_14B = (1 << 6);
    private static final int ZIP_STR_32B = (2 << 6);
    private static final int ZIP_INT_16B = 0xC0;
    private static final int ZIP_INT_32B = (0xC0 | 1 << 4);
    private static final int ZIP_INT_64B = (0xC0 | 2 << 4);
    private static final int ZIP_INT_24B = (0xC0 | 3 << 4);
    private static final int ZIP_INT_8B = 0xFE;
    private static final int ZIP_INT_IMM_MIN = 0xF1;
    private static final int ZIP_INT_IMM_MAX = 0xFD;
    private static final int ZIP_LIST_HEADER_SIZE = 10; // 4bytes total + 4bytes tail + 2bytes length

    public static void decode(ByteBuf nettyBuf, BiConsumer<byte[], Integer> consumer) {
        // Skip total bytes (4) and tail offset (4)
        nettyBuf.skipBytes(8);

        // Read number of entries (2 bytes, little endian)
        int numEntries = nettyBuf.readUnsignedShortLE();
        // Read entries
        for (int i = 0; i < numEntries; i++) {
            // Skip previous entry length
            int prevLen = nettyBuf.readUnsignedByte();
            if (prevLen >= 254) {
                nettyBuf.skipBytes(4); // Skip the 4-byte length
            }

            // Read encoding
            int encoding = nettyBuf.readUnsignedByte();
            if ((encoding & ZIP_STR_MASK) < ZIP_STR_MASK) {
                // String encoding
                int len;
                if ((encoding & ZIP_STR_MASK) == ZIP_STR_06B) {
                    len = encoding & 0x3F;
                } else if ((encoding & ZIP_STR_MASK) == ZIP_STR_14B) {
                    len = ((encoding & 0x3F) << 8) | nettyBuf.readUnsignedByte();
                } else if ((encoding & ZIP_STR_MASK) == ZIP_STR_32B) {
                    len = nettyBuf.readIntLE();
                } else {
                    throw new IllegalStateException("Invalid string encoding");
                }

                var bytes = new byte[len];
                nettyBuf.readBytes(bytes);
                consumer.accept(bytes, i);
            } else {
                // Integer encoding
                String value;
                if (encoding == ZIP_INT_8B) {
                    value = String.valueOf(nettyBuf.readByte());
                } else if (encoding == ZIP_INT_16B) {
                    value = String.valueOf(nettyBuf.readShortLE());
                } else if (encoding == ZIP_INT_32B) {
                    value = String.valueOf(nettyBuf.readIntLE());
                } else if (encoding == ZIP_INT_64B) {
                    value = String.valueOf(nettyBuf.readLongLE());
                } else if (encoding == ZIP_INT_24B) {
                    int val = nettyBuf.readUnsignedMediumLE();
                    // Sign extend if negative
                    if ((val & 0x800000) != 0) {
                        val |= 0xFF000000;
                    }
                    value = String.valueOf(val);
                } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
                    value = String.valueOf((encoding & 0x0F) - 1);
                } else {
                    throw new IllegalStateException("Invalid integer encoding");
                }
                consumer.accept(value.getBytes(), i);
            }
        }
    }
}
