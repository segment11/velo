package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

public class NilReply implements Reply {
    private NilReply() {
    }

    public static final NilReply INSTANCE = new NilReply();

    private static final byte[] NIL = new BulkReply().buffer().asArray();
    // EOF ?
    private static final byte[] NIL_HTTP_BODY_BYTES = "".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NIL);
    }

    // resp3 null
    private static final byte[] NULL_BYTES = "_\r\n".getBytes();

    @Override
    public ByteBuf bufferAsResp3() {
        return ByteBuf.wrapForReading(NULL_BYTES);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(NIL_HTTP_BODY_BYTES);
    }

    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append("nil");
        return true;
    }
}
