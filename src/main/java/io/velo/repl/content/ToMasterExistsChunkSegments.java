package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

import java.nio.ByteBuffer;

public class ToMasterExistsChunkSegments implements ReplContent {
    private final int beginSegmentIndex;
    private final int segmentCount;
    private final byte[] metaBytes;

    public ToMasterExistsChunkSegments(int beginSegmentIndex, int segmentCount, byte[] metaBytes) {
        this.beginSegmentIndex = beginSegmentIndex;
        this.segmentCount = segmentCount;
        this.metaBytes = metaBytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeInt(beginSegmentIndex);
        toBuf.writeInt(segmentCount);
        toBuf.write(metaBytes);
    }

    @Override
    public int encodeLength() {
        // begin segment index int, segment count int, meta bytes
        return 4 + 4 + metaBytes.length;
    }

    public static boolean isSlaveSameForThisBatch(byte[] metaBytesMaster, byte[] contentBytesFromSlave) {
        // content bytes include begin segment index int, segment count int
        if (contentBytesFromSlave.length != metaBytesMaster.length + 8) {
            throw new IllegalArgumentException("Repl exists chunk segments meta from slave meta length is not equal to master meta length");
        }

        var bufferFromSlave = ByteBuffer.wrap(contentBytesFromSlave, 8, contentBytesFromSlave.length - 8).slice();
        var bufferMaster = ByteBuffer.wrap(metaBytesMaster);

        return bufferFromSlave.equals(bufferMaster);
    }
}
