package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.SettablePromise;

/**
 * Asynchronous reply backed by a {@link SettablePromise}.
 */
public class AsyncReply implements Reply {
    private final SettablePromise<Reply> settablePromise;

    /**
     * @param settablePromise the promise for the asynchronous reply
     */
    public AsyncReply(SettablePromise<Reply> settablePromise) {
        this.settablePromise = settablePromise;
    }

    /**
     * @return the promise backing this asynchronous reply
     */
    public SettablePromise<Reply> getSettablePromise() {
        return settablePromise;
    }

    @Override
    public ByteBuf buffer() {
        return null;
    }
}