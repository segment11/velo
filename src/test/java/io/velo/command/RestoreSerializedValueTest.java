package io.velo.command;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.DataFormatException;

public class RestoreSerializedValueTest {
    public static void main(String[] args) throws DataFormatException {
//        byte[] bytes = {0, -62, -46, 2, -106, 73, 6, 0, -108, 73, 17, 118, 55, -128, 91, -103};
//        byte[] bytes = {0, 9, 97, 97, 97, 98, 98, 98, 99, 99, 99, 6, 0, 52, -111, -1, -14, 38, 66, -36, 91};
        byte[] bytes = {0, -61, 19, 65, 14, 1, 97, 97, -32, 79, 0, 0, 98, -32, 80, 0, 0, 99, -32, 78, 0, 1, 99, 99, 9, 0, -84, -101, 54, 109, 24, -123, -51, -2};

//        var base64Bytes = Base64.getDecoder().decode(bytes);
//        var inflater = new Inflater();
//        inflater.setInput(bytes);
//
//        var bos = new ByteArrayOutputStream(bytes.length);
//        var readBuffer = new byte[1024];
//        while (!inflater.finished()) {
//            int count = inflater.inflate(readBuffer);
//            bos.write(readBuffer, 0, count);
//        }
//        inflater.end();
//        var bytesDecompressed = bos.toByteArray();
//
//        var sb = new StringBuilder();
//        for (int i = 0; i < bytesDecompressed.length; i++) {
//            sb.append(bytesDecompressed[i]);
//            if (i < bytesDecompressed.length - 1) {
//                sb.append(", ");
//            }
//        }
//        System.out.println(sb.toString());

        var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        System.out.println(buffer.get());
        System.out.println(buffer.get());
        System.out.println(buffer.getInt());

//        byte[] dst = new byte[9];
//        buffer.get(dst);
//        System.out.println(dst);
//        System.out.println(buffer.getShort());
//
//        byte[] x = new byte[4];
//        ByteBuffer.wrap(x).order(ByteOrder.LITTLE_ENDIAN).putInt(1234567890);
//        System.out.println(x[0] + " " + x[1] + " " + x[2] + " " + x[3]);
    }
}
