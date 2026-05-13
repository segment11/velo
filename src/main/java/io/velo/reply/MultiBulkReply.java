package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.Slice;
import org.jetbrains.annotations.TestOnly;

/**
 * Multi-bulk reply in RESP format for arrays of values.
 */
public class MultiBulkReply implements Reply {
    private static final byte[] EMPTY_HTTP_BODY_BYTES = "[]".getBytes();

    /** Null reply for multi-bulk in RESP3 format. */
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

    /** Empty multi-bulk reply in RESP2 format. */
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

    /** SCAN_EMPTY reply for scan with no results. */
    public static final MultiBulkReply SCAN_EMPTY = new MultiBulkReply(new Reply[]{BulkReply.ZERO, EMPTY});

    /** Marker byte for multi-bulk reply in RESP format. */
    private static final byte MARKER = '*';
    private static final byte MARKER_MAP = '%';
    private static final byte MARKER_SET = '~';

    /** The replies contained in this multi-bulk reply. */
    private final Reply[] replies;

    private final boolean isMap;

    private final boolean isSet;

    public Reply[] getReplies() {
        return replies;
    }

    /**
     * @param replies the replies to contain
     * @param isMap   true if this is a map reply
     * @param isSet   true if this is a set reply
     */
    public MultiBulkReply(Reply[] replies, boolean isMap, boolean isSet) {
        this.replies = replies;
        this.isMap = isMap;
        this.isSet = isSet;

        assert !isMap || (replies != null && replies.length % 2 == 0);
    }

    /**
     * @param replies the replies to contain
     */
    public MultiBulkReply(Reply[] replies) {
        this(replies, false, false);
    }

    /**
     * @param sb        the StringBuilder to append to
     * @param nestCount the nesting level
     * @return true
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

    @Override
    public ByteBuf buffer() {
        var slice = new Slice();
        slice.writeByte(MARKER);
        if (replies == null) {
            slice.writeBytes(BulkReply.NEG_ONE_WITH_CRLF);
        } else {
            slice.writeBytes(BulkReply.numToBytes(replies.length, true));
            for (var reply : replies) {
                var subBuffer = reply.buffer();
                slice.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
            }
        }
        return ByteBuf.wrap(slice.getArray(), 0, slice.getWriteIndex());
    }

    @Override
    public ByteBuf bufferAsResp3() {
        var slice = new Slice();
        slice.writeByte(isMap ? MARKER_MAP : (isSet ? MARKER_SET : MARKER));
        if (replies == null) {
            slice.writeBytes(BulkReply.NEG_ONE_WITH_CRLF);
        } else {
            slice.writeBytes(BulkReply.numToBytes(isMap ? replies.length / 2 : replies.length, true));
            for (var reply : replies) {
                var subBuffer = reply.bufferAsResp3();
                slice.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
            }
        }
        return ByteBuf.wrap(slice.getArray(), 0, slice.getWriteIndex());
    }

    @Override
    public ByteBuf bufferAsHttp() {
        var buf = buffer();
        // replace \r\n
        var string = new String(buf.array(), buf.head(), buf.tail() - buf.head());
        return ByteBuf.wrapForReading(string.replaceAll("\r\n", "\n").getBytes());
    }
}