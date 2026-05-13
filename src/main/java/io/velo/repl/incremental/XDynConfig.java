package io.velo.repl.incremental;

import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

/**
 * Dynamic configuration binlog content (not yet implemented).
 */
public class XDynConfig implements BinlogContent {

    @Override
    public Type type() {
        return Type.dyn_config;
    }

    @Override
    public int encodedLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] encodeWithType() {
        throw new UnsupportedOperationException();
    }

    public static XDynConfig decodeFrom(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(short slot, ReplPair replPair) {
        throw new UnsupportedOperationException();
    }
}
