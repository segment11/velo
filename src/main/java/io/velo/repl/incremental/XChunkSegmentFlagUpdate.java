package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;
import java.util.TreeMap;

public class XChunkSegmentFlagUpdate implements BinlogContent {
    private final TreeMap<Integer, XOneWalGroupPersist.SegmentFlagWithSeq> updatedChunkSegmentFlagWithSeqMap = new TreeMap<>();

    public void putUpdatedChunkSegmentFlagWithSeq(int segmentIndex, Byte flagByte, long seq) {
        updatedChunkSegmentFlagWithSeqMap.put(segmentIndex, new XOneWalGroupPersist.SegmentFlagWithSeq(flagByte, seq));
    }

    public boolean isEmpty() {
        return updatedChunkSegmentFlagWithSeqMap.isEmpty();
    }

    @Override
    public Type type() {
        return Type.chunk_segment_flag_update;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        var n = 1 + 4;
        // 4 bytes for updated chunk segment flag with seq map size
        n += 4;
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            // 4 bytes for segment index, 1 byte for flag, 8 bytes for seq
            n += 4 + 1 + 8;
        }
        return n;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);

        buffer.putInt(updatedChunkSegmentFlagWithSeqMap.size());
        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.put(entry.getValue().flagByte());
            buffer.putLong(entry.getValue().seq());
        }
        return bytes;
    }

    public static XChunkSegmentFlagUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();
        var x = new XChunkSegmentFlagUpdate();

        var updatedChunkSegmentFlagWithSeqMapSize = buffer.getInt();
        for (var i = 0; i < updatedChunkSegmentFlagWithSeqMapSize; i++) {
            var segmentIndex = buffer.getInt();
            var flagByte = buffer.get();
            var seq = buffer.getLong();
            x.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, flagByte, seq);
        }

        if (encodedLength != x.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return x;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();


    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);

        for (var entry : updatedChunkSegmentFlagWithSeqMap.entrySet()) {
            var segmentIndex = entry.getKey();
            var flagByte = entry.getValue().flagByte();
            var seq = entry.getValue().seq();
            // RandAccessFile use os page cache, perf ok
            oneSlot.updateSegmentMergeFlag(segmentIndex, flagByte, seq);
        }
    }
}
