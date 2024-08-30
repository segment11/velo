package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.SettablePromise;

public class AsyncReply implements Reply {
    private final SettablePromise<Reply> settablePromise;

    public AsyncReply(SettablePromise<Reply> settablePromise) {
        this.settablePromise = settablePromise;
    }

    public SettablePromise<Reply> getSettablePromise() {
        return settablePromise;
    }

    @Override
    public ByteBuf buffer() {
        return null;
    }
}
