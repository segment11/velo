package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a multi-bulk reply in the RESP (Redis Serialization Protocol) format.
 * A multi-bulk reply is a collection of replies and is used to represent an array of values in RESP.
 */
public class MultiBulkReply implements Reply {
    private static final byte[] EMPTY_HTTP_BODY_BYTES = "[]".getBytes();

    /**
     * Represents a null reply for multi-bulk replies, encoded in RESP3 format.
     */
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

    /**
     * Represents an empty multi-bulk reply, encoded in RESP2 format.
     */
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

    /**
     * A special multi-bulk reply used for SCAN command, consisting of a BulkReply.ZERO and an EMPTY reply.
     */
    public static final MultiBulkReply SCAN_EMPTY = new MultiBulkReply(new Reply[]{BulkReply.ZERO, EMPTY});

    /**
     * The marker byte for a multi-bulk reply in RESP format.
     */
    private static final byte MARKER = '*';
    private static final byte MARKER_MAP = '%';
    private static final byte MARKER_SET = '~';

    /**
     * The replies contained in this multi-bulk reply.
     */
    private final Reply[] replies;

    private final boolean isMap;

    private final boolean isSet;

    /**
     * Retrieves the replies contained in this multi-bulk reply.
     *
     * @return the array of {@link Reply} objects
     */
    public Reply[] getReplies() {
        return replies;
    }

    public MultiBulkReply(Reply[] replies, boolean isMap, boolean isSet) {
        this.replies = replies;
        this.isMap = isMap;
        this.isSet = isSet;

        assert !isMap || (replies != null && replies.length % 2 == 0);
    }

    /**
     * Constructs a new MultiBulkReply with the given array of replies.
     *
     * @param replies the replies to be contained in this multi-bulk reply
     */
    public MultiBulkReply(Reply[] replies) {
        this(replies, false, false);
    }

    /**
     * Dumps the multi-bulk reply to a StringBuilder for testing purposes.
     *
     * @param sb        the StringBuilder to append the dump to
     * @param nestCount the level of nesting for indentation
     * @return always returns true
     */
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

    /**
     * Returns a {@link ByteBuf} containing the multi-bulk reply encoded in RESP format.
     *
     * @return the {@link ByteBuf} with the encoded multi-bulk reply
     */
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
    public ByteBuf bufferAsResp3() {
        // 256 bytes
        var buf = Unpooled.buffer();
        buf.writeByte(isMap ? MARKER_MAP : (isSet ? MARKER_SET : MARKER));
        if (replies == null) {
            buf.writeBytes(BulkReply.NEG_ONE_WITH_CRLF);
        } else {
            buf.writeBytes(BulkReply.numToBytes(isMap ? replies.length / 2 : replies.length, true));
            for (var reply : replies) {
                var subBuffer = reply.bufferAsResp3();
                buf.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
            }
        }
        return ByteBuf.wrap(buf.array(), 0, buf.writerIndex());
    }

    /**
     * Returns a {@link ByteBuf} containing the multi-bulk reply encoded in HTTP format.
     * Current implementation delegates to {@link #buffer()}.
     *
     * @return the {@link ByteBuf} with the encoded multi-bulk reply
     */
    @Override
    public ByteBuf bufferAsHttp() {
        var buf = buffer();
        // replace \r\n
        var string = new String(buf.array(), buf.head(), buf.tail() - buf.head());
        return ByteBuf.wrapForReading(string.replaceAll("\r\n", "\n").getBytes());
    }
}