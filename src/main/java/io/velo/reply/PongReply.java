package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

/**
 * Represents a Pong reply in the Velo application.
 * This class implements the {@link Reply} interface and provides
 * two different byte representations for the Pong reply:
 * one suitable for standard communication and another for HTTP responses.
 */
public class PongReply implements Reply {
    /**
     * Private constructor to enforce the use of the singleton instance.
     */
    private PongReply() {
    }

    /**
     * Singleton instance of the PongReply.
     */
    public static final PongReply INSTANCE = new PongReply();

    /**
     * Byte array representing the Pong reply for standard communication.
     * It contains the byte sequence '+PONG\r\n'.
     */
    private static final byte[] PONG = new byte[]{'+', 'P', 'O', 'N', 'G', '\r', '\n'};

    /**
     * Byte array representing the body of the Pong reply for HTTP responses.
     * It contains the byte sequence 'PONG'.
     */
    private static final byte[] HTTP_BODY_BYTES = "PONG".getBytes();

    /**
     * Returns a {@link ByteBuf} wrapping the byte array representing
     * the Pong reply suitable for standard communication.
     *
     * @return A ByteBuf containing the standard Pong reply.
     */
    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(PONG);
    }

    /**
     * Returns a {@link ByteBuf} wrapping the byte array representing
     * the Pong reply suitable for HTTP responses.
     *
     * @return A ByteBuf containing the HTTP body of the Pong reply.
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(HTTP_BODY_BYTES);
    }
}