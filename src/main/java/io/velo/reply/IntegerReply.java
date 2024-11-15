package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

public class IntegerReply implements Reply {
    private static final byte[] b0 = "0".getBytes();
    private static final byte[] b1 = "1".getBytes();

    public static final Reply REPLY_1 = new Reply() {
        @Override
        public ByteBuf buffer() {
            return integer1ByteBuf.slice();
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(b1);
        }

        @TestOnly
        @Override
        public boolean dumpForTest(StringBuilder sb, int nestCount) {
            sb.append("1");
            return true;
        }
    };

    public static final Reply REPLY_0 = new Reply() {
        @Override
        public ByteBuf buffer() {
            return integer0ByteBuf.slice();
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(b0);
        }

        @TestOnly
        @Override
        public boolean dumpForTest(StringBuilder sb, int nestCount) {
            sb.append("0");
            return true;
        }
    };

    private static final ByteBuf integer1ByteBuf = bufferPreload(1L);
    private static final ByteBuf integer0ByteBuf = bufferPreload(0L);

    private static final byte MARKER = ':';

    public long getInteger() {
        return integer;
    }

    private final long integer;

    public IntegerReply(long integer) {
        this.integer = integer;
    }

    static ByteBuf bufferPreload(Long x) {
        if (x == null) {
            return NilReply.INSTANCE.buffer();
        }

        var sizeBytes = BulkReply.numToBytes(x, true);
        int len = 1 + sizeBytes.length;

        var buf = ByteBuf.wrapForWriting(new byte[len]);
        buf.writeByte(MARKER);
        buf.write(sizeBytes);
        return buf;
    }

    @Override
    public ByteBuf buffer() {
        return bufferPreload(integer);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(Long.toString(integer).getBytes());
    }

    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append(integer);
        return true;
    }
}
