package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a NIL (null) reply in the context of a protocol, such as RESP (REdis Serialization Protocol).
 * This class provides methods to get the NIL reply in different formats, including RESP2, RESP3, and HTTP.
 */
public class NilReply implements Reply {

    /**
     * Private constructor to enforce the singleton pattern.
     */
    private NilReply() {
    }

    /**
     * Singleton instance of NilReply.
     */
    public static final NilReply INSTANCE = new NilReply();

    /**
     * Byte array representing a NIL reply in RESP2 format.
     */
    private static final byte[] NIL = new BulkReply().buffer().asArray();

    /**
     * Byte array representing an empty HTTP body.
     */
    private static final byte[] HTTP_BODY_BYTES = "".getBytes();

    /**
     * Byte array representing a nil value in RESP3 format.
     */
    private static final byte[] NULL_BYTES = "_\r\n".getBytes();

    /**
     * Returns a {@link ByteBuf} containing the NIL reply in RESP2 format.
     *
     * @return A ByteBuf with the NIL reply.
     */
    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NIL);
    }

    /**
     * Returns a {@link ByteBuf} containing the NIL reply in RESP3 format.
     *
     * @return A ByteBuf with the NIL reply in RESP3 format.
     */
    @Override
    public ByteBuf bufferAsResp3() {
        return ByteBuf.wrapForReading(NULL_BYTES);
    }

    /**
     * Returns a {@link ByteBuf} containing the NIL reply in HTTP format, which is an empty body.
     *
     * @return A ByteBuf with an empty HTTP body.
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(HTTP_BODY_BYTES);
    }

    /**
     * Dumps the string representation of the NIL reply to the provided StringBuilder for testing purposes.
     * This method is annotated with @TestOnly to indicate it is intended for testing only.
     *
     * @param sb        The StringBuilder to append the string representation to.
     * @param nestCount The nesting level, not used in this implementation.
     * @return Always returns true.
     */
    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append("nil");
        return true;
    }
}