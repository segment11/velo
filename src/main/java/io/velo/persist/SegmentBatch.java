package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.velo.ConfForSlot;
import io.velo.SnowFlake;
import io.velo.metric.InSlotMetricCollector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Batch of data segments for persistence with compression (segment compression mode).
 */
public class SegmentBatch implements InSlotMetricCollector {
    /**
     * Byte array to hold the segment data.
     */
    private final byte[] bytes;

    /**
     * ByteBuffer for reading and writing data into the byte array.
     */
    private final ByteBuffer buffer;

    /**
     * Length of each chunk segment.
     */
    private final int chunkSegmentLength;

    /**
     * Snowflake ID generator for segment sequences.
     */
    private final SnowFlake snowFlake;

    // Compression metrics
    @VisibleForTesting
    long compressCountTotal;
    private long compressTimeTotalUs;

    @VisibleForTesting
    long compressBytesTotal;
    private long compressedBytesTotal;

    // Batch and key-value metrics
    @VisibleForTesting
    long batchCountTotal;
    private long batchKvCountTotal;

    // Tight packing metrics
    private long beforeTightSegmentCountTotal;
    @VisibleForTesting
    long afterTightSegmentCountTotal;

    private static final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    /**
     * @param snowFlake the SnowFlake ID generator for segment sequences
     */
    public SegmentBatch(@NotNull SnowFlake snowFlake) {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;

        // Initialize the byte array and ByteBuffer for segment data
        this.bytes = new byte[chunkSegmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;
    }

    /**
     * @return the map containing various metrics about the segment batches
     */
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

    /**
     * @param compressedBytes compressed segment bytes
     * @param tmpSegmentIndex temporary segment index
     * @param segmentSeq segment sequence number
     * @param valueBytesLength length of value bytes
     * @param isRaw true if raw (uncompressed) segment bytes
     */
    @VisibleForTesting
    record SegmentCompressedBytesWithIndex(byte[] compressedBytes, int tmpSegmentIndex, long segmentSeq,
                                           int valueBytesLength, boolean isRaw) {
        @Override
        public @NotNull String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "tmpSegmentIndex=" + tmpSegmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", valueBytesLength=" + valueBytesLength +
                    ", compressedBytes.length=" + compressedBytes.length +
                    ", isRaw=" + isRaw +
                    '}';
        }
    }

    public record SegmentTightBytesWithLengthAndSegmentIndex(byte[] tightBytesWithLength, int tmpSegmentIndex,
                                                             byte blockNumber, long segmentSeq, int valueBytesLength) {
        @Override
        public @NotNull String toString() {
            return "SegmentTightBytesWithLengthAndSegmentIndex{" +
                    "tmpSegmentIndex=" + tmpSegmentIndex +
                    ", blockNumber=" + blockNumber +
                    ", segmentSeq=" + segmentSeq +
                    ", valueBytesLength=" + valueBytesLength +
                    ", tightBytesWithLength.length=" + tightBytesWithLength.length +
                    '}';
        }
    }

    /**
     * Maximum number of blocks that can be tight-packed into one segment.
     */
    public static final int MAX_BLOCK_NUMBER = 4;
    /**
     * Length of the segment header.
     * seq long + segment type bit + total bytes length int + each sub block * (offset short + length short).
     */
    private static final int HEADER_LENGTH = 8 + 1 + 4 + MAX_BLOCK_NUMBER * (2 + 2);

    /**
     * @param subBlockIndex the index of the sub-block
     * @return the position of the sub-block metadata in the segment header
     * @throws IllegalArgumentException if the sub-block index is out of bounds
     */
    public static int subBlockMetaPosition(int subBlockIndex) {
        if (subBlockIndex >= MAX_BLOCK_NUMBER) {
            throw new IllegalArgumentException("Segment batch sub block index must be less than=" + MAX_BLOCK_NUMBER);
        }

        return 8 + 1 + 4 + subBlockIndex * (2 + 2);
    }

    /**
     * @param afterTightSegmentIndex the index for the tight segment
     * @param onceList the list of compressed segments to be tight-packed
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the record representing the tight-packed segment
     */
    private SegmentTightBytesWithLengthAndSegmentIndex tightSegments(int afterTightSegmentIndex,
                                                                     @NotNull ArrayList<SegmentCompressedBytesWithIndex> onceList,
                                                                     @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        var valueBytesLength = 0;
        for (int j = 0; j < onceList.size(); j++) {
            var subBlockIndex = (byte) j;
            var s = onceList.get(j);
            valueBytesLength += s.valueBytesLength();

            for (var pvm : returnPvmList) {
                if (pvm.segmentIndex == s.tmpSegmentIndex) {
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
        buffer.put(Chunk.SegmentType.TIGHT.val);
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

        return new SegmentTightBytesWithLengthAndSegmentIndex(tightBytesWithLength, afterTightSegmentIndex, (byte) onceList.size(), segmentSeq, valueBytesLength);
    }

    /**
     * @param segments the list of compressed segments to be tight-packed
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the list of tight-packed segments
     */
    private ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> tight(@NotNull ArrayList<SegmentCompressedBytesWithIndex> segments,
                                                                        @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        beforeTightSegmentCountTotal += segments.size();

        ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> r = new ArrayList<>(segments.size());

        ArrayList<SegmentCompressedBytesWithIndex> onceList = new ArrayList<>(MAX_BLOCK_NUMBER);
        int onceListBytesLength = 0;

        int afterTightSegmentIndex = segments.getFirst().tmpSegmentIndex;
        for (var segment : segments) {
            // Raw (uncompressed) segments are exactly chunkSegmentLength bytes and
            // must be written directly, not tight-packed with an additional header.
            if (segment.isRaw) {
                if (!onceList.isEmpty()) {
                    var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
                    r.add(tightOne);
                    afterTightSegmentIndex++;
                    onceList.clear();
                    onceListBytesLength = 0;
                }

                for (var pvm : returnPvmList) {
                    if (pvm.segmentIndex == segment.tmpSegmentIndex) {
                        pvm.segmentIndex = afterTightSegmentIndex;
                    }
                }
                r.add(new SegmentTightBytesWithLengthAndSegmentIndex(
                        segment.compressedBytes, afterTightSegmentIndex, (byte) 1,
                        segment.segmentSeq, segment.valueBytesLength));
                afterTightSegmentIndex++;
                continue;
            }

            var compressedBytes = segment.compressedBytes;

            if (onceList.size() == MAX_BLOCK_NUMBER || onceListBytesLength + compressedBytes.length > chunkSegmentLength - HEADER_LENGTH) {
                if (!onceList.isEmpty()) {
                    var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
                    r.add(tightOne);
                    afterTightSegmentIndex++;
                }

                onceList.clear();
                onceListBytesLength = 0;
            }

            onceList.add(segment);
            onceListBytesLength += compressedBytes.length;
        }

        if (!onceList.isEmpty()) {
            var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
            r.add(tightOne);
        }

        afterTightSegmentCountTotal += r.size();
        return r;
    }

    /**
     * @param list the list of write-ahead log entries to be split into segments
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the list of tight-packed segments with their byte data, indices, and sequence numbers
     */
    public ArrayList<SegmentBatch2.SegmentBytesWithIndex> split(@NotNull ArrayList<Wal.V> list,
                                                                @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        var r = splitAndTight(list, returnPvmList);
        ArrayList<SegmentBatch2.SegmentBytesWithIndex> returnList = new ArrayList<>(r.size());
        for (var one : r) {
            returnList.add(new SegmentBatch2.SegmentBytesWithIndex(one.tightBytesWithLength, one.tmpSegmentIndex, one.segmentSeq, one.valueBytesLength));
        }
        return returnList;
    }

    /**
     * @param list the list of write-ahead log entries to be split into segments
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the list of tight-packed segments
     */
    public ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> splitAndTight(@NotNull ArrayList<Wal.V> list,
                                                                               @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentCompressedBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int tmpSegmentIndex = 0;

        var persistLength = SegmentBatch2.SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < chunkSegmentLength) {
                onceList.add(v);
            } else {
                result.add(compressAsSegment(onceList, tmpSegmentIndex, returnPvmList));
                tmpSegmentIndex++;

                onceList.clear();
                persistLength = SegmentBatch2.SEGMENT_HEADER_LENGTH + v.persistLength();
                onceList.add(v);
            }
        }

        if (!onceList.isEmpty()) {
            result.add(compressAsSegment(onceList, tmpSegmentIndex, returnPvmList));
        }

        return tight(result, returnPvmList);
    }

    /**
     * @param list the list of write-ahead log entries to be compressed
     * @param tmpSegmentIndex the temporary segment index
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the record representing the compressed segment
     */
    private SegmentCompressedBytesWithIndex compressAsSegment(@NotNull ArrayList<Wal.V> list,
                                                              int tmpSegmentIndex,
                                                              @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();
        var valueBytesLength = SegmentBatch2.encodeToBuffer(list, buffer, returnPvmList, tmpSegmentIndex, segmentSeq);

        compressCountTotal++;
        var beginT = System.nanoTime();
        var compressedBytes = Zstd.compress(bytes);
        var costT = (System.nanoTime() - beginT) / 1000;
        compressTimeTotalUs += costT;
        compressBytesTotal += bytes.length;
        compressedBytesTotal += compressedBytes.length;

        if (compressedBytes.length >= bytes.length) {
            var rawCopy = Arrays.copyOf(bytes, bytes.length);
            buffer.clear();
            Arrays.fill(bytes, (byte) 0);
            return new SegmentCompressedBytesWithIndex(rawCopy, tmpSegmentIndex, segmentSeq, valueBytesLength, true);
        }

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentCompressedBytesWithIndex(compressedBytes, tmpSegmentIndex, segmentSeq, valueBytesLength, false);
    }

    /**
     * @param slot the slot index associated with the segment
     * @param tightBytesWithLength the byte array containing the tight-packed segment data
     * @param pvm the metadata about the persisted value
     * @param chunk the chunk containing the segment
     * @return the decompressed segment bytes
     * @throws IllegalStateException if there is an error during decompression
     */
    static byte[] decompressSegmentBytesFromOneSubBlock(short slot,
                                                        byte[] tightBytesWithLength,
                                                        @NotNull PersistValueMeta pvm,
                                                        @NotNull Chunk chunk) {
        // If segment type is NORMAL (byte at offset 8 == 0), the data was not
        // compressed — return a copy directly.
        if (tightBytesWithLength.length == chunk.chunkSegmentLength
                && tightBytesWithLength.length > 8
                && tightBytesWithLength[8] == Chunk.SegmentType.NORMAL.val) {
            return tightBytesWithLength;
        }

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

        // Stats
        chunk.segmentDecompressTimeTotalUs += costT;
        chunk.segmentDecompressCountTotal++;

        if (d != chunk.chunkSegmentLength) {
            throw new IllegalStateException("Decompress segment sub block error, s=" + slot +
                    ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunk.chunkSegmentLength);
        }

        return decompressedBytes;
    }
}