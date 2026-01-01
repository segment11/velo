package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

/**
 * Represents an empty reply in the context of communication or data transmission.
 * This class is a singleton and provides an instance that can be used to indicate
 * the absence of data or a no-op response.
 */
public class EmptyReply implements Reply {

    /**
     * Private constructor to ensure that the class cannot be instantiated
     * directly, thus enforcing the singleton pattern.
     */
    private EmptyReply() {
    }

    /**
     * The singleton instance of {@code EmptyReply}. Use this instance to
     * represent an empty reply in your application.
     */
    public static final EmptyReply INSTANCE = new EmptyReply();

    /**
     * Returns an empty {@code ByteBuf} object representing the absence of data.
     *
     * @return the {@code ByteBuf} object that is empty
     */
    @Override
    public ByteBuf buffer() {
        return ByteBuf.empty();
    }
}