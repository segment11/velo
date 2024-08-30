package io.velo.command;

import io.netty.buffer.ByteBuf;
import io.velo.type.RedisHashKeys;

public class RDBIntSet {
    final static int IntSetHeaderSize = 8;

    public static RedisHashKeys parse(ByteBuf bb) {
        int size = bb.readableBytes();
        int memberSize = bb.readIntLE();
        int len = bb.readIntLE();

        if (memberSize == 0) {
            throw new IllegalArgumentException("Invalid intset encoding");
        }

        if (IntSetHeaderSize + memberSize * len != size) {
            throw new IllegalArgumentException("Invalid intset length");
        }

        var rhk = new RedisHashKeys();
        for (int i = 0; i < len; i++) {
            switch (memberSize) {
                case 2 -> rhk.add(String.valueOf(bb.readUnsignedShortLE()));
                case 4 -> rhk.add(String.valueOf(bb.readUnsignedIntLE()));
                case 8 -> rhk.add(String.valueOf(bb.readLongLE()));
                default -> throw new IllegalArgumentException("Invalid intset encoding");
            }
        }
        return rhk;
    }

}
