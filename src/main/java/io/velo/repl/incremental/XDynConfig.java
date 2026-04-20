package io.velo.repl.incremental;

import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

public class XDynConfig implements BinlogContent {

    @Override
    public Type type() {
        return Type.dyn_config;
    }

    @Override
    public int encodedLength() {
        throw new UnsupportedOperationException("XDynConfig is not implemented yet");
    }

    @Override
    public byte[] encodeWithType() {
        throw new UnsupportedOperationException("XDynConfig is not implemented yet");
    }

    public static XDynConfig decodeFrom(ByteBuffer buffer) {
        throw new UnsupportedOperationException("XDynConfig is not implemented yet");
    }

    @Override
    public void apply(short slot, ReplPair replPair) {
        throw new UnsupportedOperationException("XDynConfig is not implemented yet");
    }
}
