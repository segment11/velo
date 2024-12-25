package io.velo.persist;

import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.SnowFlake;
import io.velo.metric.InSlotMetricCollector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

// do not compress
public class SegmentBatch2 implements InSlotMetricCollector {
    private final byte[] bytes;
    private final ByteBuffer buffer;
    private final short slot;

    private final SnowFlake snowFlake;

    @VisibleForTesting
    long batchCountTotal;
    private long batchKvCountTotal;

    private static final Logger log = LoggerFactory.getLogger(SegmentBatch2.class);

    public SegmentBatch2(short slot, SnowFlake snowFlake) {
        this.slot = slot;

        this.bytes = new byte[ConfForSlot.global.confChunk.segmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        if (batchCountTotal > 0) {
            map.put("segment_batch_count_total", (double) batchCountTotal);
            map.put("segment_batch_kv_count_total", (double) batchKvCountTotal);
            map.put("segment_batch_kv_count_avg", (double) batchKvCountTotal / batchCountTotal);
        }

        return map;
    }

    @VisibleForTesting
    record SegmentBytesWithIndex(byte[] segmentBytes, int segmentIndex, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", segmentBytes.length=" + segmentBytes.length +
                    '}';
        }
    }

    ArrayList<SegmentBytesWithIndex> split(@NotNull ArrayList<Wal.V> list,
                                           int[] nextNSegmentIndex,
                                           @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int i = 0;

        var persistLength = Chunk.SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < bytes.length) {
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

        return result;
    }

    private SegmentBytesWithIndex compressAsSegment(@NotNull ArrayList<Wal.V> list,
                                                    int segmentIndex,
                                                    @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();
        encodeToBuffer(list, buffer, returnPvmList, slot, segmentIndex, segmentSeq);

        var copyBytes = Arrays.copyOf(this.bytes, this.bytes.length);

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentBytesWithIndex(copyBytes, segmentIndex, segmentSeq);
    }

    static void encodeToBuffer(@NotNull ArrayList<Wal.V> list,
                               @NotNull ByteBuffer buffer,
                               @NotNull ArrayList<PersistValueMeta> returnPvmList,
                               short slot,
                               int segmentIndex,
                               long segmentSeq) {
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
            // calc again, for perf, can reuse from slot with key hash, todo
            pvm.keyHash32 = KeyHash.hash32(keyBytes);
            pvm.bucketIndex = v.bucketIndex();
            pvm.isFromMerge = v.isFromMerge();

            pvm.shortType = KeyLoader.transferToShortType(v.spType());
            // tmp 0, then update
            pvm.subBlockIndex = 0;
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
    }

    public interface CvCallback {
        void callback(@NotNull String key, @NotNull CompressedValue cv, int offsetInThisSegment);
    }

    @TestOnly
    static class ForDebugCvCallback implements CvCallback {
        @Override
        public void callback(@NotNull String key, @NotNull CompressedValue cv, int offsetInThisSegment) {
            System.out.println("key=" + key + ", cv=" + cv + ", offsetInThisSegment=" + offsetInThisSegment);
        }
    }

    public static void iterateFromSegmentBytes(byte[] segmentBytes, @NotNull CvCallback cvCallback) {
        iterateFromSegmentBytes(segmentBytes, 0, segmentBytes.length, cvCallback);
    }

    public static void iterateFromSegmentBytes(byte[] segmentBytes, int offset, int length, @NotNull CvCallback cvCallback) {
        var buf = Unpooled.wrappedBuffer(segmentBytes, offset, length).slice();
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
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }

            var keyBytes = new byte[keyLength];
            buf.readBytes(keyBytes);
            var key = new String(keyBytes);

            var cv = CompressedValue.decode(buf, keyBytes, 0);
            var persistLength = Wal.V.persistLength(keyLength, cv.encodedLength());

            cvCallback.callback(key, cv, offsetInThisSegment);

            offsetInThisSegment += persistLength;
        }
    }
}
