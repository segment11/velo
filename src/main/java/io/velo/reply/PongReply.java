package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public class PongReply implements Reply {
    private PongReply() {
    }

    public static final PongReply INSTANCE = new PongReply();

    private static final byte[] PONG = new byte[]{'+', 'P', 'O', 'N', 'G', '\r', '\n'};
    private static final byte[] PONG_BYTES = "PONG".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(PONG);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(PONG_BYTES);
    }
}
