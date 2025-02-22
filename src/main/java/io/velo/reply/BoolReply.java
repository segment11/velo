package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

/**
 * Represents a boolean response in different protocols.
 * This enum provides two states: TRUE and FALSE.
 */
public enum BoolReply implements Reply {
    /**
     * Represents a true boolean value.
     */
    T(true),

    /**
     * Represents a false boolean value.
     */
    F(false);

    private static final byte BOOL_MARKER = '#';
    private static final byte TRUE_BYTE = 't';
    private static final byte FALSE_BYTE = 'f';

    private static final byte[] TRUE_BYTES = "true".getBytes();
    private static final byte[] FALSE_BYTES = "false".getBytes();

    private final boolean value;

    /**
     * Constructs a BoolReply with the given boolean value.
     *
     * @param value the boolean value to be represented
     */
    BoolReply(boolean value) {
        this.value = value;
    }

    /**
     * Returns a ByteBuf representing this boolean value in RESP2 format.
     *
     * @return a ByteBuf containing "true" or "false"
     */
    @Override
    public ByteBuf buffer() {
        return new BulkReply(value ? TRUE_BYTES : FALSE_BYTES).buffer();
    }

    /**
     * Returns a ByteBuf representing this boolean value in RESP3 format.
     * The format is either "#t\r\n" for true or "#f\r\n" for false.
     *
     * @return a ByteBuf containing the boolean value in RESP3 format
     */
    @Override
    public ByteBuf bufferAsResp3() {
        // #t\r\n or #f\r\n
        int len = 1 + 1 + 2;

        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(BOOL_MARKER);
        bb.put(value ? TRUE_BYTE : FALSE_BYTE);
        bb.put(BulkReply.CRLF);
        return bb;
    }
}