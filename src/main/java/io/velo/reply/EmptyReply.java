package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public class EmptyReply implements Reply {
    private EmptyReply() {
    }

    public static final EmptyReply INSTANCE = new EmptyReply();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.empty();
    }
}
