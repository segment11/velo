package io.velo.command;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestByteOrder {
    public static void main(String[] args) {
        byte[] data = new byte[4];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(-56);

//        int r = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
//        System.out.println(r);

        int r = (64 & 0xff) & 0x3F;
        System.out.println(r);
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            int r2 = (r << 8) | (i & 0xff);
            System.out.println(r2);
        }
    }
}
