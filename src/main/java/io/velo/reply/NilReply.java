package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public class NilReply implements Reply {
    private NilReply() {
    }

    public static final NilReply INSTANCE = new NilReply();

    private static final byte[] NIL = new BulkReply().buffer().asArray();
    // EOF ?
    private static final byte[] NIL_BYTES = "".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NIL);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(NIL_BYTES);
    }
}
