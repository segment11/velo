package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public enum BoolReply implements Reply {
    T(true), F(false);

    private static final byte BOOL_MARKER = '#';
    private static final byte TRUE_BYTE = 't';
    private static final byte FALSE_BYTE = 'f';

    private static final byte[] TRUE_BYTES = "true".getBytes();
    private static final byte[] FALSE_BYTES = "false".getBytes();

    private final boolean value;

    BoolReply(boolean value) {
        this.value = value;
    }

    @Override
    public ByteBuf buffer() {
        return new BulkReply(value ? TRUE_BYTES : FALSE_BYTES).buffer();
    }

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
