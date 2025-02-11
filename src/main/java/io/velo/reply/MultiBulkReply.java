package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.TestOnly;

public class MultiBulkReply implements Reply {
    private static final byte[] EMPTY_HTTP_BODY_BYTES = "[]".getBytes();

    public static final Reply NULL = new Reply() {
        private static final byte[] RESP3_NULL_ARRAY_BYTES = "*-1\r\n".getBytes();

        @Override
        public ByteBuf buffer() {
            return ByteBuf.wrapForReading(RESP3_NULL_ARRAY_BYTES);
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(EMPTY_HTTP_BODY_BYTES);
        }
    };

    public static final Reply EMPTY = new Reply() {
        private static final byte[] RESP2_EMPTY_ARRAY_BYTES = "*0\r\n".getBytes();

        @Override
        public ByteBuf buffer() {
            return ByteBuf.wrapForReading(RESP2_EMPTY_ARRAY_BYTES);
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(EMPTY_HTTP_BODY_BYTES);
        }

        @TestOnly
        @Override
        public boolean dumpForTest(StringBuilder sb, int nestCount) {
            sb.append("(empty array)");
            return true;
        }
    };

    public static final MultiBulkReply SCAN_EMPTY = new MultiBulkReply(new Reply[]{BulkReply.ZERO, EMPTY});

    private static final byte MARKER = '*';

    private final Reply[] replies;

    public Reply[] getReplies() {
        return replies;
    }

    public MultiBulkReply(Reply[] replies) {
        this.replies = replies;
    }

    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        // pretty print same as redis client multi bulk reply
        var prepend = " ".repeat(nestCount * 2 + 1);
        for (int i = 0; i < replies.length; i++) {
            if (i != 0 && nestCount > 0) {
                sb.append(prepend);
            }
            sb.append(i + 1).append(") ");
            var inner = replies[i];
            inner.dumpForTest(sb, nestCount + 1);

            if (!(inner instanceof MultiBulkReply)) {
                sb.append("\n");
            }
        }
        return true;
    }

    @Override
    public ByteBuf buffer() {
        // 256 bytes
        var buf = Unpooled.buffer();
        buf.writeByte(MARKER);
        if (replies == null) {
            buf.writeBytes(BulkReply.NEG_ONE_WITH_CRLF);
        } else {
            buf.writeBytes(BulkReply.numToBytes(replies.length, true));
            for (var reply : replies) {
                var subBuffer = reply.buffer();
                buf.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
            }
        }
        return ByteBuf.wrap(buf.array(), 0, buf.writerIndex());
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return buffer();
    }
}
