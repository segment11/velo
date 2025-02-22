package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

/**
 * Represents an OK reply as part of a communication protocol, typically used
 * in responses where an operation was successful and no additional data
 * is needed.
 */
public class OKReply implements Reply {
    /**
     * Private constructor to prevent instantiation of this class.
     * Use the {@link #INSTANCE} to get the singleton instance.
     */
    private OKReply() {
    }

    /**
     * Singleton instance of the OKReply class.
     */
    public static final OKReply INSTANCE = new OKReply();

    /**
     * Byte array representation of the "+OK\r\n" response.
     * This format is commonly used in protocols like IMAP and POP3.
     */
    private static final byte[] OK = new byte[]{'+', 'O', 'K', '\r', '\n'};

    /**
     * Byte array representation of the "OK" response body.
     * This format is commonly used in HTTP responses when only the OK status
     * is needed.
     */
    private static final byte[] HTTP_BODY_BYTES = "OK".getBytes();

    /**
     * Returns a {@link ByteBuf} containing the "+OK\r\n" response.
     * This method is typically used in protocols that require the full
     * "+OK\r\n" response format.
     *
     * @return ByteBuf containing the "+OK\r\n" response.
     */
    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(OK);
    }

    /**
     * Returns a {@link ByteBuf} containing the "OK" response body.
     * This method is typically used in HTTP responses where only the
     * OK status is needed.
     *
     * @return ByteBuf containing the "OK" response body.
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(HTTP_BODY_BYTES);
    }
}