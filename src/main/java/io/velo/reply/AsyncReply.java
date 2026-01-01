package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.SettablePromise;

/**
 * Represents an asynchronous reply in a messaging or communication system.
 * This class uses {@link SettablePromise} to handle the asynchronous nature of the reply.
 */
public class AsyncReply implements Reply {
    private final SettablePromise<Reply> settablePromise;

    /**
     * Constructs an instance of {@link AsyncReply} with a given {@link SettablePromise}.
     *
     * @param settablePromise the promise that will be used to set the reply asynchronously
     */
    public AsyncReply(SettablePromise<Reply> settablePromise) {
        this.settablePromise = settablePromise;
    }

    /**
     * Returns the {@link SettablePromise} associated with this reply.
     *
     * @return the settable promise for this reply
     */
    public SettablePromise<Reply> getSettablePromise() {
        return settablePromise;
    }

    /**
     * Returns the buffer associated with this reply. In the case of {@link AsyncReply},
     * the buffer is not used and returns null.
     *
     * @return the buffer associated with this reply, or null if there is none
     */
    @Override
    public ByteBuf buffer() {
        return null;
    }
}