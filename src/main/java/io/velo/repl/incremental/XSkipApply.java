package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class XSkipApply implements BinlogContent {
    public long getSeq() {
        return seq;
    }

    public int getChunkCurrentSegmentIndex() {
        return chunkCurrentSegmentIndex;
    }

    private final long seq;
    private final int chunkCurrentSegmentIndex;

    public XSkipApply(long seq, int chunkCurrentSegmentIndex) {
        this.seq = seq;
        this.chunkCurrentSegmentIndex = chunkCurrentSegmentIndex;
    }

    @Override
    public Type type() {
        return Type.skip_apply;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type
        // 8 bytes for seq
        // 4 bytes for chunk segment index
        return 1 + 8 + 4;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putLong(seq);
        buffer.putInt(chunkCurrentSegmentIndex);

        return bytes;
    }

    public static XSkipApply decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var seq = buffer.getLong();
        var chunkCurrentSegmentIndex = buffer.getInt();
        return new XSkipApply(seq, chunkCurrentSegmentIndex);
    }

    private static final Logger log = LoggerFactory.getLogger(XSkipApply.class);

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl skip apply, seq={}, chunk segment index={}",
                seq, chunkCurrentSegmentIndex);

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.setMetaChunkSegmentIndexInt(chunkCurrentSegmentIndex);

        replPair.setSlaveCatchUpLastSeq(seq);
    }
}
