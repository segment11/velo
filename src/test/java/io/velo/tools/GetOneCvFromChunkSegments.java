package io.velo.tools;

import com.github.luben.zstd.Zstd;
import io.velo.ConfForSlot;
import io.velo.persist.ChunkMergeJob;
import io.velo.persist.SegmentBatch;
import io.velo.persist.SegmentBatch2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class GetOneCvFromChunkSegments {
    public static void main(String[] args) throws IOException {
        ConfForSlot.global = ConfForSlot.c1m;

        short slot = 0;

        final int segmentIndex = 9333;
        final int segmentOffset = 716;
        final String toFindKey = "key:000000369711";

        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
        var chunkFileIndex = segmentIndex / segmentNumberPerFd;
        var segmentIndexInChunkFile = segmentIndex % segmentNumberPerFd;

        var slotDir = new File(persistDir + "/slot-" + slot);
        var bytes = new byte[segmentLength];

        var chunkFile = new File(slotDir, "chunk-data-" + chunkFileIndex);
        if (!chunkFile.exists()) {
            throw new IllegalStateException("Chunk file not exists: " + chunkFile);
        }

        var raf = new RandomAccessFile(chunkFile, "r");
        raf.seek(segmentIndexInChunkFile * segmentLength);
        var n = raf.read(bytes);
        if (n != segmentLength) {
            throw new IllegalStateException("Read error, n=" + n + ", segment length=" + segmentLength);
        }
        raf.close();

        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(1000);

        var buffer = ByteBuffer.wrap(bytes);
        for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
            buffer.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
            var subBlockOffset = buffer.getShort();
            if (subBlockOffset == 0) {
                break;
            }

            var subBlockLength = buffer.getShort();

            var decompressedBytes = new byte[segmentLength];
            var d = Zstd.decompressByteArray(decompressedBytes, 0, segmentLength, bytes, subBlockOffset, subBlockLength);
            if (d != segmentLength) {
                throw new IllegalStateException("Decompress error, s=" + slot
                        + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", segmentLength=" + segmentLength);
            }

            int finalSubBlockIndex = subBlockIndex;
            SegmentBatch2.iterateFromSegmentBytes(decompressedBytes, 0, decompressedBytes.length, (key, cv, offsetInThisSegment) -> {
                cvList.add(new ChunkMergeJob.CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));

                if (segmentOffset == offsetInThisSegment) {
                    System.out.println("key: " + key + ", cv: " + cv);
                }

                if (toFindKey.equals(key)) {
                    System.out.println("key: " + key + ", cv: " + cv);
                }
            });
        }

        for (var one : cvList) {
            System.out.println(one.shortString() + ", encoded length=" + one.cv.encodedLength());
        }
    }

    //    private static String persistDir = "/tmp/velo-data-test-data";
    private static String persistDir = "/tmp/velo-data/persist";
}
