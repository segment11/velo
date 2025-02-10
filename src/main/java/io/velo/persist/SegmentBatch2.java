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

import java.nio.ByteBuffer;
import java.util.*;

// do not compress
public class SegmentBatch2 implements InSlotMetricCollector {
    // seq long + segment type byte + cv number int + crc int
    public static final int SEGMENT_HEADER_LENGTH = 8 + 1 + 4 + 4;

    private final byte[] bytes;
    private final ByteBuffer buffer;
    private final short slot;

    private final SnowFlake snowFlake;

    @VisibleForTesting
    long batchCountTotal;
    private long batchKvCountTotal;

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
    record SegmentBytesWithIndex(byte[] segmentBytes, int tmpSegmentIndex, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "tmpSegmentIndex=" + tmpSegmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", segmentBytes.length=" + segmentBytes.length +
                    '}';
        }
    }

    ArrayList<SegmentBytesWithIndex> split(@NotNull ArrayList<Wal.V> list,
                                           @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int i = 0;

        var persistLength = SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < bytes.length) {
                onceList.add(v);
            } else {
                result.add(compressAsSegment(onceList, i, returnPvmList));
                i++;

                onceList.clear();
                persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
                onceList.add(v);
            }
        }

        if (!onceList.isEmpty()) {
            result.add(compressAsSegment(onceList, i, returnPvmList));
        }

        return result;
    }

    private SegmentBytesWithIndex compressAsSegment(@NotNull ArrayList<Wal.V> list,
                                                    int tmpSegmentIndex,
                                                    @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();
        encodeToBuffer(list, buffer, returnPvmList, tmpSegmentIndex, segmentSeq);

        var copyBytes = Arrays.copyOf(this.bytes, this.bytes.length);

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentBytesWithIndex(copyBytes, tmpSegmentIndex, segmentSeq);
    }

    static void encodeToBuffer(@NotNull ArrayList<Wal.V> list,
                               @NotNull ByteBuffer buffer,
                               @NotNull ArrayList<PersistValueMeta> returnPvmList,
                               int tmpSegmentIndex,
                               long segmentSeq) {
        // only use key bytes hash to calculate crc
        var crcCalBytes = new byte[8 * list.size()];
        var crcCalBuffer = ByteBuffer.wrap(crcCalBytes);

        // write segment header
        buffer.clear();
        buffer.putLong(segmentSeq);
        buffer.put(Chunk.SegmentType.NORMAL.val);
        buffer.putInt(list.size());
        // temp write crc, then update
        buffer.putInt(0);

        int offsetInThisSegment = SEGMENT_HEADER_LENGTH;

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
            pvm.segmentIndex = tmpSegmentIndex;
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
        // seq long + segment type byte + cv number int + crc int
        buffer.putInt(8 + 1 + 4, segmentCrc32);
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
        var segmentType = buf.readByte();
        var cvCount = buf.readInt();
        var segmentCrc32 = buf.readInt();

        if (segmentType == Chunk.SegmentType.SLIM.val) {
            for (int i = 0; i < cvCount; i++) {
                var subBlockIndex = buf.readByte();
                var segmentOffset = buf.readInt();
                var keyLength = buf.readShort();

                if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                    throw new IllegalStateException("Key length error, key length=" + keyLength);
                }

                var keyBytes = new byte[keyLength];
                buf.readBytes(keyBytes);
                var key = new String(keyBytes);

                var cvEncodedLength = buf.readInt();
                var cv = CompressedValue.decode(buf, keyBytes, 0);

                cvCallback.callback(key, cv, segmentOffset);
            }
            return;
        }

        int offsetInThisSegment = SEGMENT_HEADER_LENGTH;
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

    static boolean isSegmentBytesTight(byte[] segmentBytes, int offset) {
        // seq long + segment type byte
        return segmentBytes[offset + 8] == Chunk.SegmentType.TIGHT.val;
    }

    static boolean isSegmentBytesSlim(byte[] segmentBytes, int offset) {
        // seq long + segment type byte
        return segmentBytes[offset + 8] == Chunk.SegmentType.SLIM.val;
    }

    record KeyBytesAndValueBytesInSegment(byte[] keyBytes, byte[] valueBytes) {
    }

    static KeyBytesAndValueBytesInSegment getKeyBytesAndValueBytesInSegmentBytesSlim(byte[] segmentBytesSlim, byte subBlockIndex, int segmentOffset) {
        var buffer = ByteBuffer.wrap(segmentBytesSlim);

        // seq long + segment type byte + cv number int + crc int
        var cvNumber = buffer.getInt(8 + 1);
        buffer.position(SEGMENT_HEADER_LENGTH);

        for (int i = 0; i < cvNumber; i++) {
            var subBlockIndexInner = buffer.get();
            var segmentOffsetInner = buffer.getInt();
            if (subBlockIndexInner == subBlockIndex && segmentOffsetInner == segmentOffset) {
                var keyLength = buffer.getShort();
                var keyBytes = new byte[keyLength];
                buffer.get(keyBytes);
                var cvEncodedLength = buffer.getInt();
                var cvEncoded = new byte[cvEncodedLength];
                buffer.get(cvEncoded);

                return new KeyBytesAndValueBytesInSegment(keyBytes, cvEncoded);
            } else {
                var keyLength = buffer.getShort();
                buffer.position(buffer.position() + keyLength);
                var cvEncodedLength = buffer.getInt();
                buffer.position(buffer.position() + cvEncodedLength);
            }
        }
        return null;
    }

    // probe liner encode and search
    static byte[] encodeValidCvListSlim(List<ChunkMergeJob.CvWithKeyAndSegmentOffset> invalidCvList) {
        var lastOneSeqAsSegmentSeq = invalidCvList.getLast().cv.getSeq();
        int bytesLengthN = SEGMENT_HEADER_LENGTH;

        for (var one : invalidCvList) {
            var keyLength = one.key.length();
            var cvEncodedLength = one.cv.encodedLength();

            // 1 byte for sub block index, 4 bytes for segment offset
            // 2 bytes for key length, key bytes, 4 bytes for cv encoded length, cv encoded bytes
            bytesLengthN += 1 + 4 + 2 + keyLength + 4 + cvEncodedLength;
        }

        // when after encoded is bigger, need not merged at all
        if (bytesLengthN >= ConfForSlot.global.confChunk.segmentLength) {
            return null;
        }

        var bytes = new byte[bytesLengthN];
        var buffer = ByteBuffer.wrap(bytes);

        // only use key bytes hash to calculate crc
        var crcCalBytes = new byte[8 * invalidCvList.size()];
        var crcCalBuffer = ByteBuffer.wrap(crcCalBytes);

        // write segment header
        buffer.putLong(lastOneSeqAsSegmentSeq);
        buffer.put(Chunk.SegmentType.SLIM.val);
        buffer.putInt(invalidCvList.size());
        // temp write crc, then update
        buffer.putInt(0);

        for (var one : invalidCvList) {
            var subBlockIndex = one.subBlockIndex;
            var segmentOffset = one.segmentOffset;
            buffer.put(subBlockIndex);
            buffer.putInt(segmentOffset);

            var keyLength = one.key.length();
            var keyBytes = one.key.getBytes();
            buffer.putShort((short) keyLength);
            buffer.put(keyBytes);

            var cvEncoded = one.cv.encode();
            buffer.putInt(cvEncoded.length);
            buffer.put(cvEncoded);

            crcCalBuffer.putLong(one.cv.getKeyHash());
        }

        // update crc
        int segmentCrc32 = KeyHash.hash32(crcCalBytes);
        // refer to SEGMENT_HEADER_LENGTH definition
        // seq long + segment type byte + cv number int + crc int
        buffer.putInt(8 + 1 + 4, segmentCrc32);

        return bytes;
    }
}
