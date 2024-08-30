package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

public class RawBytesContent implements ReplContent {
    private final byte[] bytes;

    public RawBytesContent(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Repl raw bytes cannot be null");
        }
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Repl raw bytes cannot be empty");
        }

        this.bytes = bytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        return bytes.length;
    }
}
