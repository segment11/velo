package io.velo.repl.incremental;

import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class XSkipApply implements BinlogContent {
    private final long seq;

    public long getSeq() {
        return seq;
    }

    public XSkipApply(long seq) {
        this.seq = seq;
    }

    @Override
    public Type type() {
        return Type.skip_apply;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 8 bytes for seq
        return 1 + 8;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putLong(seq);

        return bytes;
    }

    public static XSkipApply decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var seq = buffer.getLong();
        return new XSkipApply(seq);
    }

    private static final Logger log = LoggerFactory.getLogger(XSkipApply.class);

    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl skip apply, seq={}", seq);
    }
}
