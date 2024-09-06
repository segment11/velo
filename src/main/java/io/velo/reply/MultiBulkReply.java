package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.netty.buffer.Unpooled;

public class MultiBulkReply implements Reply {
    private static final byte[] EMPTY_BYTES = "[]".getBytes();

    public static final Reply EMPTY = new Reply() {
        @Override
        public ByteBuf buffer() {
            return emptyByteBuf.slice();
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(EMPTY_BYTES);
        }
    };

    public static final Reply SCAN_EMPTY = new MultiBulkReply(new Reply[]{BulkReply.ZERO, EMPTY});

    private static final ByteBuf emptyByteBuf = new MultiBulkReply(new Reply[0]).buffer();

    private static final byte MARKER = '*';

    private final Reply[] replies;

    public Reply[] getReplies() {
        return replies;
    }

    public MultiBulkReply(Reply[] replies) {
        this.replies = replies;
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
