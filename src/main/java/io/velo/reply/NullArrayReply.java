package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

/**
 * Null array reply in RESP format.
 * <p>
 * Redis answers a blocking command that timed out with a null array ({@code *-1}) in RESP2 and with
 * the null type ({@code _}) in RESP3, see {@code addReplyNullArray()} in the redis source
 * {@code src/blocked.c}. It is not the null bulk of {@link NilReply}.
 */
public class NullArrayReply implements Reply {
    private NullArrayReply() {
    }

    /** Singleton instance. */
    public static final NullArrayReply INSTANCE = new NullArrayReply();

    private static final byte[] NULL_ARRAY_BYTES = "*-1\r\n".getBytes();
    private static final byte[] HTTP_BODY_BYTES = "[]".getBytes();
    private static final byte[] NULL_BYTES = "_\r\n".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NULL_ARRAY_BYTES);
    }

    @Override
    public ByteBuf bufferAsResp3() {
        return ByteBuf.wrapForReading(NULL_BYTES);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(HTTP_BODY_BYTES);
    }
}
