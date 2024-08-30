package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.SnowFlake;
import io.velo.metric.InSlotMetricCollector;
import org.jetbrains.annotations.TestOnly;
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
    private final String slotStr;

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

    private final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    public SegmentBatch(short slot, SnowFlake snowFlake) {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.slot = slot;
        this.slotStr = String.valueOf(slot);

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

        if (!onceList.isEmpty()) {
            var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
            r.add(tightOne);
        }

        afterTightSegmentCountTotal += r.size();
        return r;
    }

    public ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> splitAndTight(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
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
                    log.warn("Batch next {} segment prepare is not enough, list size: {}", nextNSegmentIndex.length, list.size());
                    throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size: " + list.size());
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
                log.warn("Batch next {} segment prepare is not enough, list size: {}", nextNSegmentIndex.length, list.size());
                throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size: " + list.size());
            }

            result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
        }

        return tight(result, returnPvmList);
    }

    private SegmentCompressedBytesWithIndex compressAsSegment(ArrayList<Wal.V> list, int segmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();

        // only use key bytes hash to calculate crc
        var crcCalBytes = new byte[8 * list.size()];
        var crcCalBuffer = ByteBuffer.wrap(crcCalBytes);

        // write segment header
        buffer.clear();
        buffer.putLong(segmentSeq);
        buffer.putInt(list.size());
        // temp write crc, then update
        buffer.putInt(0);

        int offsetInThisSegment = Chunk.SEGMENT_HEADER_LENGTH;

        for (var v : list) {
            crcCalBuffer.putLong(v.keyHash());

            var keyBytes = v.key().getBytes();
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);
            buffer.put(v.cvEncoded());

            int length = v.persistLength();

            var pvm = new PersistValueMeta();
            pvm.keyBytes = keyBytes;
            pvm.keyHash = v.keyHash();
            pvm.bucketIndex = v.bucketIndex();
            pvm.isFromMerge = v.isFromMerge();

            pvm.slot = slot;
            // tmp 0, then update
            pvm.subBlockIndex = 0;
            pvm.length = length;
            // tmp current segment index, then update
            pvm.segmentIndex = segmentIndex;
            pvm.segmentOffset = offsetInThisSegment;
            pvm.expireAt = v.expireAt();
            pvm.seq = v.seq();
            returnPvmList.add(pvm);

            offsetInThisSegment += length;
        }

        if (buffer.remaining() >= 2) {
            // write 0 short, so merge loop can break, because reuse old bytes
            buffer.putShort((short) 0);
        }

        // update crc
        int segmentCrc32 = KeyHash.hash32(crcCalBytes);
        // refer to SEGMENT_HEADER_LENGTH definition
        // seq long + cv number int + crc int
        buffer.putInt(8 + 4, segmentCrc32);

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

    public interface CvCallback {
        void callback(String key, CompressedValue cv, int offsetInThisSegment);
    }

    @TestOnly
    static class ForDebugCvCallback implements CvCallback {
        @Override
        public void callback(String key, CompressedValue cv, int offsetInThisSegment) {
            System.out.println("key: " + key + ", cv: " + cv + ", offsetInThisSegment: " + offsetInThisSegment);
        }
    }

    public static void iterateFromSegmentBytes(byte[] decompressedBytes, CvCallback cvCallback) {
        var buf = Unpooled.wrappedBuffer(decompressedBytes);
        // for crc check
        var segmentSeq = buf.readLong();
        var cvCount = buf.readInt();
        var segmentCrc32 = buf.readInt();

        int offsetInThisSegment = Chunk.SEGMENT_HEADER_LENGTH;
        while (true) {
            // refer to comment: write 0 short, so merge loop can break, because reuse old bytes
            if (buf.readableBytes() < 2) {
                break;
            }

            var keyLength = buf.readShort();
            if (keyLength == 0) {
                break;
            }

            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length: " + keyLength);
            }

            var keyBytes = new byte[keyLength];
            buf.readBytes(keyBytes);
            var key = new String(keyBytes);

            var cv = CompressedValue.decode(buf, keyBytes, 0);
            int length = Wal.V.persistLength(keyLength, cv.encodedLength());

            cvCallback.callback(key, cv, offsetInThisSegment);

            offsetInThisSegment += length;
        }
    }
}
