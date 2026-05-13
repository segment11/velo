package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * NIL (null) reply in RESP2, RESP3, and HTTP formats.
 */
public class NilReply implements Reply {

    private NilReply() {
    }

    /** Singleton instance. */
    public static final NilReply INSTANCE = new NilReply();

    private static final byte[] NIL = new BulkReply().buffer().asArray();
    private static final byte[] HTTP_BODY_BYTES = "".getBytes();
    private static final byte[] NULL_BYTES = "_\r\n".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NIL);
    }

    @Override
    public ByteBuf bufferAsResp3() {
        return ByteBuf.wrapForReading(NULL_BYTES);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(HTTP_BODY_BYTES);
    }

    /**
     * @param sb        the StringBuilder to append to
     * @param nestCount the nesting level (unused)
     * @return true
     */
    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append("nil");
        return true;
    }
}