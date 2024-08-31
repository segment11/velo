package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class XFlush implements BinlogContent {
    @Override
    public Type type() {
        return Type.flush;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        return 1 + 4;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);

        return bytes;
    }

    public static XFlush decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var r = new XFlush();
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length: " + encodedLength);
        }

        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    private static final Logger log = LoggerFactory.getLogger(XFlush.class);

    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl slave apply one slot flush, !!!, slot: {}", slot);
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.flush();
        log.warn("Repl slave apply one slot flush done, !!!, slot: {}", slot);
    }
}
