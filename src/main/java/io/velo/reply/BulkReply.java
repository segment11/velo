package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public class BulkReply implements Reply {
    public byte[] getRaw() {
        return raw;
    }

    private byte[] raw;

    public BulkReply() {
    }

    public BulkReply(byte[] raw) {
        this.raw = raw;
    }

    // copy from camellia-redis-proxy-core/src/main/java/com/netease/nim/camellia/redis/proxy/util/Utils.java
    private static final char CR = '\r';
    private static final char LF = '\n';

    private static final byte MARKER = '$';

    public static final byte[] CRLF = new byte[]{CR, LF};

    public static final byte[] NEG_ONE = convert(-1, false);
    public static final byte[] NEG_ONE_WITH_CRLF = convert(-1, true);

    private static final int NUM_MAP_LENGTH = 256;
    private static final byte[][] numMap = new byte[NUM_MAP_LENGTH][];
    private static final byte[][] numMapWithCRLF = new byte[NUM_MAP_LENGTH][];

    static {
        for (int i = 0; i < NUM_MAP_LENGTH; i++) {
            numMapWithCRLF[i] = convert(i, true);
            numMap[i] = convert(i, false);
        }
    }

    private static byte[] convert(long value, boolean withCRLF) {
        boolean negative = value < 0;
        // Checked javadoc: If the argument is equal to 10^n for integer n, then the result is n.
        // Also, if negative, leave another slot for the sign.
        long abs = Math.abs(value);
        int index = (value == 0 ? 0 : (int) Math.log10(abs)) + (negative ? 2 : 1);
        // Append the CRLF if necessary
        var bytes = new byte[withCRLF ? index + 2 : index];
        if (withCRLF) {
            bytes[index] = CR;
            bytes[index + 1] = LF;
        }
        // Put the sign in the slot we saved
        if (negative) bytes[0] = '-';
        long next = abs;
        while ((next /= 10) > 0) {
            bytes[--index] = (byte) ('0' + (abs % 10));
            abs = next;
        }
        bytes[--index] = (byte) ('0' + abs);
        return bytes;
    }

    static byte[] numToBytes(long value, boolean withCRLF) {
        if (value >= 0 && value < NUM_MAP_LENGTH) {
            int index = (int) value;
            return withCRLF ? numMapWithCRLF[index] : numMap[index];
        } else if (value == -1) {
            return withCRLF ? NEG_ONE_WITH_CRLF : NEG_ONE;
        }
        return convert(value, withCRLF);
    }

    @Override
    public ByteBuf buffer() {
        int size = raw != null ? raw.length : -1;
        var sizeBytes = numToBytes(size, true);

        // $+size+raw+\r\n
        int len = 1 + sizeBytes.length + (size >= 0 ? size + 2 : 0);

        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(MARKER);
        // size
        bb.write(sizeBytes);
        if (size >= 0) {
            bb.write(raw);
            bb.write(CRLF);
        }
        return bb;
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(raw);
    }
}
