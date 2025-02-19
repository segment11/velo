package io.velo.tools;

import com.github.luben.zstd.Zstd;
import io.velo.ConfForSlot;
import io.velo.persist.ChunkMergeJob;
import io.velo.persist.SegmentBatch;
import io.velo.persist.SegmentBatch2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class IterateChunkSegments {
    //    private static String persistDir = "/tmp/velo-data-test-data";
    private static String persistDir = "/tmp/velo-data/persist";

    private static ConfForSlot.ConfChunk confChunk;

    // check -d 200 compressed value length is 20, filter those not match
    // check -d 1000 compressed value length is 127, filter those not match
    // todo
//    private static final int cvNormalCompressedLength = 20;
    private static final int cvNormalCompressedLength = 127;

    public static void main(String[] args) throws IOException {
        confChunk = ConfForSlot.c10m.confChunk;

        short slot = 0;
        int[] sumArray = new int[confChunk.maxSegmentNumber()];

        var slotDir = new File(persistDir + "/slot-" + slot);
        var chunkFiles = slotDir.listFiles((dir, name) -> name.startsWith("chunk-data-"));

        for (int i = 0; i < chunkFiles.length; i++) {
            iterateOneChunkFile(slot, (byte) i, chunkFiles[i], sumArray);
        }

        int sumTotal = 0;
        for (int i = 0; i < sumArray.length; i++) {
            sumTotal += sumArray[i];
        }
        System.out.println("sum total=" + sumTotal);
    }

    public static void iterateOneChunkFile(short slot, byte index, File chunkFile, int[] sumArray) throws IOException {
        if (chunkFile.length() == 0) {
            return;
        }

        var segmentLength = confChunk.segmentLength;
        var segmentNumberPerFd = confChunk.segmentNumberPerFd;

        var beginSegmentIndex = index * segmentNumberPerFd;

        final var bytes = new byte[4096];
        var fis = new FileInputStream(chunkFile);
        int segmentIndexThisFd = 0;
        while (fis.read(bytes) != -1) {
            var segmentIndex = segmentIndexThisFd + beginSegmentIndex;

            ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(1000);

            var buffer = ByteBuffer.wrap(bytes);
            for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
                // position to target sub block
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
                SegmentBatch2.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                    cvList.add(new ChunkMergeJob.CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));
                });
            }

            for (var one : cvList) {
                if (cvNormalCompressedLength != 0 && one.cv.getCompressedLength() != cvNormalCompressedLength) {
                    System.out.println("key=" + one.key + ", cv=" + one.cv);
                }
            }

            sumArray[segmentIndex] = cvList.size();

            segmentIndexThisFd++;
        }
        fis.close();
    }
}
