package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

public class XChunkSegmentSlimUpdate implements BinlogContent {
    private final int segmentIndex;
    private final byte[] segmentBytesSlim;

    public XChunkSegmentSlimUpdate(int segmentIndex, byte[] segmentBytesSlim) {
        this.segmentIndex = segmentIndex;
        this.segmentBytesSlim = segmentBytesSlim;
    }

    @Override
    public Type type() {
        return Type.chunk_segment_slim_update;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for segment index int, 4 bytes for encoded length for check
        return 1 + 4 + 4 + segmentBytesSlim.length;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(segmentIndex);
        buffer.putInt(segmentBytesSlim.length);
        buffer.put(segmentBytesSlim);
        return bytes;
    }

    public static XChunkSegmentSlimUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var segmentIndex = buffer.getInt();
        var encodedLength = buffer.getInt();
        var bb = new byte[encodedLength];
        buffer.get(bb);
        return new XChunkSegmentSlimUpdate(segmentIndex, bb);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getChunk().writeSegmentsFromMasterExistsOrAfterSegmentSlim(segmentBytesSlim, segmentIndex, 1);
    }
}
