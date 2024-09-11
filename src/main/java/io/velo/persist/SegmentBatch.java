package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.velo.ConfForSlot;
import io.velo.SnowFlake;
import io.velo.metric.InSlotMetricCollector;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SegmentBatch implements InSlotMetricCollector {
    private final byte[] bytes;
    private final ByteBuffer buffer;
    private final short slot;

    private final int chunkSegmentLength;
    private final SnowFlake snowFlake;

    @VisibleForTesting
    long compressCountTotal;
    private long compressTimeTotalUs;

    @VisibleForTesting
    long compressBytesTotal;
    private long compressedBytesTotal;

    @VisibleForTesting
    long batchCountTotal;
    private long batchKvCountTotal;

    private long beforeTightSegmentCountTotal;
    @VisibleForTesting
    long afterTightSegmentCountTotal;

    private static final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    public SegmentBatch(short slot, SnowFlake snowFlake) {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.slot = slot;

        this.bytes = new byte[chunkSegmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        if (compressCountTotal > 0) {
            map.put("segment_compress_time_total_us", (double) compressTimeTotalUs);
            map.put("segment_compress_count_total", (double) compressCountTotal);
            map.put("segment_compress_time_avg_us", (double) compressTimeTotalUs / compressCountTotal);
        }

        if (compressBytesTotal > 0) {
            map.put("segment_compress_bytes_total", (double) compressBytesTotal);
            map.put("segment_compressed_bytes_total", (double) compressedBytesTotal);
            map.put("segment_compress_ratio", (double) compressedBytesTotal / compressBytesTotal);
        }

        if (batchCountTotal > 0) {
            map.put("segment_batch_count_total", (double) batchCountTotal);
            map.put("segment_batch_kv_count_total", (double) batchKvCountTotal);
            map.put("segment_batch_kv_count_avg", (double) batchKvCountTotal / batchCountTotal);
        }

        if (afterTightSegmentCountTotal > 0) {
            map.put("segment_before_tight_segment_count_total", (double) beforeTightSegmentCountTotal);
            map.put("segment_after_tight_segment_count_total", (double) afterTightSegmentCountTotal);
            map.put("segment_tight_segment_ratio", (double) afterTightSegmentCountTotal / beforeTightSegmentCountTotal);
        }

        return map;
    }

    @VisibleForTesting
    record SegmentCompressedBytesWithIndex(byte[] compressedBytes, int segmentIndex, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", compressedBytes.length=" + compressedBytes.length +
                    '}';
        }
    }

    public record SegmentTightBytesWithLengthAndSegmentIndex(byte[] tightBytesWithLength, int segmentIndex,
                                                             byte blockNumber, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentTightBytesWithLengthAndSegmentIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", blockNumber=" + blockNumber +
                    ", segmentSeq=" + segmentSeq +
                    ", tightBytesWithLength.length=" + tightBytesWithLength.length +
                    '}';
        }
    }

    // zstd compress ratio usually < 0.25, max 4 blocks tight to one segment
    public static final int MAX_BLOCK_NUMBER = 4;
    // seq long + total bytes length int + each sub block * (offset short + length short)
    private static final int HEADER_LENGTH = 8 + 4 + MAX_BLOCK_NUMBER * (2 + 2);

    public static int subBlockMetaPosition(int subBlockIndex) {
        if (subBlockIndex >= MAX_BLOCK_NUMBER) {
            throw new IllegalArgumentException("Segment batch sub block index must be less than=" + MAX_BLOCK_NUMBER);
        }

        return 8 + 4 + subBlockIndex * (2 + 2);
    }

    private SegmentTightBytesWithLengthAndSegmentIndex tightSegments(int afterTightSegmentIndex, ArrayList<SegmentCompressedBytesWithIndex> onceList, ArrayList<PersistValueMeta> returnPvmList) {
        for (int j = 0; j < onceList.size(); j++) {
            var subBlockIndex = (byte) j;
            var s = onceList.get(j);

            for (var pvm : returnPvmList) {
                if (pvm.segmentIndex == s.segmentIndex) {
                    pvm.subBlockIndex = subBlockIndex;
                    pvm.segmentIndex = afterTightSegmentIndex;
                }
            }
        }

        var totalBytesN = HEADER_LENGTH;
        for (var s : onceList) {
            totalBytesN += s.compressedBytes.length;
        }

        var tightBytesWithLength = new byte[totalBytesN];
        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        var segmentSeq = snowFlake.nextId();
        buffer.putLong(segmentSeq);
        buffer.putInt(totalBytesN);

        int offset = HEADER_LENGTH;
        for (var s : onceList) {
            var compressedBytes = s.compressedBytes;
            var length = compressedBytes.length;

            buffer.putShort((short) offset);
            buffer.putShort((short) length);

            buffer.mark();
            buffer.position(offset).put(compressedBytes);
            buffer.reset();

            offset += length;
        }

        return new SegmentTightBytesWithLengthAndSegmentIndex(tightBytesWithLength, afterTightSegmentIndex, (byte) onceList.size(), segmentSeq);
    }

    private ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> tight(ArrayList<SegmentCompressedBytesWithIndex> segments, ArrayList<PersistValueMeta> returnPvmList) {
        beforeTightSegmentCountTotal += segments.size();

        ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> r = new ArrayList<>(segments.size());

        ArrayList<SegmentCompressedBytesWithIndex> onceList = new ArrayList<>(MAX_BLOCK_NUMBER);
        int onceListBytesLength = 0;

        int afterTightSegmentIndex = segments.get(0).segmentIndex;
        for (var segment : segments) {
            var compressedBytes = segment.compressedBytes;

            if (onceList.size() == MAX_BLOCK_NUMBER || onceListBytesLength + compressedBytes.length > chunkSegmentLength - HEADER_LENGTH) {
                var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
                r.add(tightOne);
                afterTightSegmentIndex++;

                onceList.clear();
                onceListBytesLength = 0;
            }

            onceList.add(segment);
            onceListBytesLength += compressedBytes.length;
        }

        var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
        r.add(tightOne);

        afterTightSegmentCountTotal += r.size();
        return r;
    }

    ArrayList<SegmentBatch2.SegmentBytesWithIndex> split(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        var r = splitAndTight(list, nextNSegmentIndex, returnPvmList);
        ArrayList<SegmentBatch2.SegmentBytesWithIndex> returnList = new ArrayList<>(r.size());
        for (var one : r) {
            returnList.add(new SegmentBatch2.SegmentBytesWithIndex(one.tightBytesWithLength, one.segmentIndex, one.segmentSeq));
        }
        return returnList;
    }

    @VisibleForTesting
    ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> splitAndTight(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentCompressedBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int i = 0;

        var persistLength = Chunk.SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < chunkSegmentLength) {
                onceList.add(v);
            } else {
                if (i >= nextNSegmentIndex.length) {
                    log.warn("Batch next {} segment prepare is not enough, list size={}", nextNSegmentIndex.length, list.size());
                    throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size=" + list.size());
                }

                result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
                i++;

                onceList.clear();
                persistLength = Chunk.SEGMENT_HEADER_LENGTH + v.persistLength();
                onceList.add(v);
            }
        }

        if (!onceList.isEmpty()) {
            if (i >= nextNSegmentIndex.length) {
                log.warn("Batch next {} segment prepare is not enough, list size={}", nextNSegmentIndex.length, list.size());
                throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size=" + list.size());
            }

            result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
        }

        return tight(result, returnPvmList);
    }

    private SegmentCompressedBytesWithIndex compressAsSegment(ArrayList<Wal.V> list, int segmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();
        SegmentBatch2.encodeToBuffer(list, buffer, returnPvmList, slot, segmentIndex, segmentSeq);

        // important: 4KB decompress cost ~200us, so use 4KB segment length for better read latency
        // double compress

        compressCountTotal++;
        var beginT = System.nanoTime();
        var compressedBytes = Zstd.compress(bytes);
        var costT = (System.nanoTime() - beginT) / 1000;
        compressTimeTotalUs += costT;
        compressBytesTotal += bytes.length;
        compressedBytesTotal += compressedBytes.length;

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentCompressedBytesWithIndex(compressedBytes, segmentIndex, segmentSeq);
    }

    static byte[] decompressSegmentBytesFromOneSubBlock(short slot, byte[] tightBytesWithLength, PersistValueMeta pvm, Chunk chunk) {
        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        buffer.position(subBlockMetaPosition(pvm.subBlockIndex));
        var subBlockOffset = buffer.getShort();
        var subBlockLength = buffer.getShort();

        if (subBlockOffset == 0) {
            throw new IllegalStateException("Sub block offset is 0, pvm=" + pvm);
        }

        var decompressedBytes = new byte[chunk.chunkSegmentLength];

        var beginT = System.nanoTime();
        var d = Zstd.decompressByteArray(decompressedBytes, 0, chunk.chunkSegmentLength,
                tightBytesWithLength, subBlockOffset, subBlockLength);
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        chunk.segmentDecompressTimeTotalUs += costT;
        chunk.segmentDecompressCountTotal++;

        if (d != chunk.chunkSegmentLength) {
            throw new IllegalStateException("Decompress segment sub block error, s=" + slot +
                    ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunk.chunkSegmentLength);
        }

        return decompressedBytes;
    }
}
