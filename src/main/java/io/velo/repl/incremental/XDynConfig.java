package io.velo.repl.incremental;

import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

public class XDynConfig implements BinlogContent {
    @Override
    public Type type() {
        return null;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public byte[] encodeWithType() {
        return new byte[0];
    }

    public static XDynConfig decodeFrom(ByteBuffer buffer) {
        return null;
    }

    @Override
    public void apply(short slot, ReplPair replPair) {

    }
}
