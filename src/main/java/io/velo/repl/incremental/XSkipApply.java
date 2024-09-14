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

    public int getChunkMergedSegmentIndexEndLastTime() {
        return chunkMergedSegmentIndexEndLastTime;
    }

    private final long seq;
    private final int chunkCurrentSegmentIndex;
    private final int chunkMergedSegmentIndexEndLastTime;

    public XSkipApply(long seq, int chunkCurrentSegmentIndex, int chunkMergedSegmentIndexEndLastTime) {
        this.seq = seq;
        this.chunkCurrentSegmentIndex = chunkCurrentSegmentIndex;
        this.chunkMergedSegmentIndexEndLastTime = chunkMergedSegmentIndexEndLastTime;
    }

    @Override
    public Type type() {
        return Type.skip_apply;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 8 bytes for seq
        // 4 bytes for chunk segment index, 4 bytes for chunk merged segment index end last time
        return 1 + 8 + 4 + 4;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putLong(seq);
        buffer.putInt(chunkCurrentSegmentIndex);
        buffer.putInt(chunkMergedSegmentIndexEndLastTime);

        return bytes;
    }

    public static XSkipApply decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var seq = buffer.getLong();
        var chunkCurrentSegmentIndex = buffer.getInt();
        var chunkMergedSegmentIndexEndLastTime = buffer.getInt();
        return new XSkipApply(seq, chunkCurrentSegmentIndex, chunkMergedSegmentIndexEndLastTime);
    }

    private static final Logger log = LoggerFactory.getLogger(XSkipApply.class);

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl skip apply, seq={}, chunk segment index={}, chunk merged segment index end last time={}",
                seq, chunkCurrentSegmentIndex, chunkMergedSegmentIndexEndLastTime);

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.setMetaChunkSegmentIndexInt(chunkCurrentSegmentIndex);
        oneSlot.getChunk().setMergedSegmentIndexEndLastTime(chunkMergedSegmentIndexEndLastTime);

        replPair.setSlaveCatchUpLastSeq(seq);
    }
}
