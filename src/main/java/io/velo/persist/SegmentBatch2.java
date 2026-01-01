package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
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

/**
 * Represents a batch of data segments for persistence. Each segment contains a header and a list of key-value pairs.
 * This class is responsible for managing the encoding, compression, and splitting of these segments.
 * This is for no segment compression mode.
 */
public class SegmentBatch2 implements InSlotMetricCollector {

    /**
     * Constants for the segment header structure.
     * seq long + segment type byte + cv number int + crc int
     * when segment type is slim and compressed
     * dst size int + src size int + segment type byte + compressed bytes
     */
    public static final int SEGMENT_HEADER_LENGTH = 8 + 1 + 4 + 4;

    // byte array to hold the segment data
    private final byte[] bytes;
    // ByteBuffer for reading and writing data into the byte array
    private final ByteBuffer buffer;

    // Snowflake ID generator for segment sequences
    private final SnowFlake snowFlake;

    // Total batch count for testing and metrics
    @VisibleForTesting
    long batchCountTotal;
    // Total key-value count in all batches for testing and metrics
    private long batchKvCountTotal;

    /**
     * Constructs a new SegmentBatch2 instance for a specific slot and SnowFlake ID generator.
     *
     * @param snowFlake the SnowFlake ID generator for segment sequences
     */
    public SegmentBatch2(SnowFlake snowFlake) {
        // Initialize the byte array and ByteBuffer for segment data
        this.bytes = new byte[ConfForSlot.global.confChunk.segmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;
    }

    /**
     * Collects and returns metrics related to the segment batches.
     *
     * @return the map containing various metrics about the segment batches
     */
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

    /**
     * Represents a segment with its byte data, an index, and a sequence number.
     */
    @VisibleForTesting
    record SegmentBytesWithIndex(byte[] segmentBytes, int tmpSegmentIndex, long segmentSeq, int valueBytesLength) {
        @Override
        public @NotNull String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "tmpSegmentIndex=" + tmpSegmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", segmentBytes.length=" + segmentBytes.length +
                    ", valueBytesLength=" + valueBytesLength +
                    '}';
        }
    }

    /**
     * Splits a list of write-ahead log entries into segments according to their combined length.
     *
     * @param list          the list of write-ahead log entries to be split into segments
     * @param returnPvmList the list to store metadata about the persisted values
     * @return the list of segments with their byte data, indices, and sequence numbers
     */
    ArrayList<SegmentBytesWithIndex> split(@NotNull ArrayList<Wal.V> list, @NotNull ArrayList<PersistValueMeta> returnPvmList) {
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

    /**
     * Compresses a list of write-ahead log entries into a single segment.
     *
     * @param list            the list of write-ahead log entries to be compressed
     * @param tmpSegmentIndex the temporary segment index
     * @param returnPvmList   the list to store metadata about the persisted values
     * @return the segment with its byte data, index, and sequence number
     */
    private SegmentBytesWithIndex compressAsSegment(@NotNull ArrayList<Wal.V> list,
                                                    int tmpSegmentIndex,
                                                    @NotNull ArrayList<PersistValueMeta> returnPvmList) {
        batchCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();
        var valueBytesLength = encodeToBuffer(list, buffer, returnPvmList, tmpSegmentIndex, segmentSeq);

        var copyBytes = Arrays.copyOf(this.bytes, this.bytes.length);

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentBytesWithIndex(copyBytes, tmpSegmentIndex, segmentSeq, valueBytesLength);
    }

    /**
     * Encodes a list of write-ahead log entries into the provided ByteBuffer.
     *
     * @param list            the list of write-ahead log entries to be encoded
     * @param buffer          the ByteBuffer to write the encoded data into
     * @param returnPvmList   the list to store metadata about the persisted values
     * @param tmpSegmentIndex the temporary segment index
     * @param segmentSeq      the segment sequence number
     * @return the total length of value bytes
     */
    static int encodeToBuffer(@NotNull ArrayList<Wal.V> list,
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
        var valueBytesLengthTotal = 0;
        for (var v : list) {
            crcCalBuffer.putLong(v.keyHash());

            var key = v.key();
            assert key.length() <= Short.MAX_VALUE;
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.put(v.cvEncoded());

            int length = v.persistLength();

            var pvm = new PersistValueMeta();
            pvm.key = key;
            pvm.keyHash = v.keyHash();
            // calc again, for perf, can reuse from slot with key hash, todo
            pvm.keyHash32 = KeyHash.hash32(key.getBytes());
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
            pvm.valueBytesLength = length;
            returnPvmList.add(pvm);

            offsetInThisSegment += length;
            valueBytesLengthTotal += length;
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

        return valueBytesLengthTotal;
    }

    /**
     * Callback interface for processing key-value pairs in a segment.
     */
    public interface CvCallback {
        void callback(@NotNull String key, @NotNull CompressedValue cv, int offsetInThisSegment);
    }

    /**
     * Debug implementation of CvCallback for printing key-value pairs.
     */
    @TestOnly
    static class ForDebugCvCallback implements CvCallback {
        @Override
        public void callback(@NotNull String key, @NotNull CompressedValue cv, int offsetInThisSegment) {
            System.out.println("key=" + key + ", cv=" + cv + ", offsetInThisSegment=" + offsetInThisSegment);
        }
    }

    /**
     * Iterates over key-value pairs in a segment and applies a callback to each pair.
     *
     * @param segmentBytes the byte array containing the segment data
     * @param cvCallback   the callback to be applied to each key-value pair
     */
    public static void iterateFromSegmentBytes(byte[] segmentBytes, @NotNull CvCallback cvCallback) {
        iterateFromSegmentBytes(segmentBytes, 0, segmentBytes.length, cvCallback);
    }

    /**
     * Iterates over key-value pairs in a segment and applies a callback to each pair.
     *
     * @param segmentBytes the byte array containing the segment data
     * @param offset       the starting offset within the segment data
     * @param length       the length of the segment data to process
     * @param cvCallback   the callback to be applied to each key-value pair
     */
    public static void iterateFromSegmentBytes(byte[] segmentBytes, int offset, int length, @NotNull CvCallback cvCallback) {
        var segmentType = segmentBytes[8];
        if (segmentType == Chunk.SegmentType.SLIM.val || segmentType == Chunk.SegmentType.SLIM_AND_COMPRESSED.val) {
            ByteBuf buf;
            if (segmentType == Chunk.SegmentType.SLIM_AND_COMPRESSED.val) {
                var dstSize = ByteBuffer.wrap(segmentBytes).getInt();
                var uncompressedSegmentBytes = new byte[dstSize];
                Zstd.decompressByteArray(uncompressedSegmentBytes, 0, dstSize, segmentBytes, SEGMENT_HEADER_LENGTH, segmentBytes.length - SEGMENT_HEADER_LENGTH);
                buf = Unpooled.wrappedBuffer(uncompressedSegmentBytes);
            } else {
                buf = Unpooled.wrappedBuffer(segmentBytes);
            }

            var cvCount = buf.readerIndex(8 + 1).readInt();
            buf.readerIndex(SEGMENT_HEADER_LENGTH);
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

        var buf = Unpooled.wrappedBuffer(segmentBytes);
        buf.readerIndex(SEGMENT_HEADER_LENGTH);
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

    /**
     * Checks if the segment bytes represent a TIGHT segment type.
     *
     * @param segmentBytes the byte array containing the segment data
     * @param offset       the starting offset within the segment data
     * @return true if the segment type is TIGHT, false otherwise
     */
    static boolean isSegmentBytesTight(byte[] segmentBytes, int offset) {
        // seq long + segment type byte
        return segmentBytes[offset + 8] == Chunk.SegmentType.TIGHT.val;
    }

    /**
     * Checks if the segment bytes represent a SLIM segment type.
     *
     * @param segmentBytes the byte array containing the segment data
     * @param offset       the starting offset within the segment data
     * @return true if the segment type is SLIM, false otherwise
     */
    static boolean isSegmentBytesSlim(byte[] segmentBytes, int offset) {
        // seq long + segment type byte
        return segmentBytes[offset + 8] == Chunk.SegmentType.SLIM.val;
    }

    /**
     * Checks if the segment bytes represent a SLIM_AND_COMPRESSED segment type.
     *
     * @param segmentBytes the byte array containing the segment data
     * @param offset       the starting offset within the segment data
     * @return true if the segment type is SLIM_AND_COMPRESSED, false otherwise
     */
    static boolean isSegmentBytesSlimAndCompressed(byte[] segmentBytes, int offset) {
        // seq long + segment type byte
        return segmentBytes[offset + 8] == Chunk.SegmentType.SLIM_AND_COMPRESSED.val;
    }

    /**
     * A record to hold key and value bytes extracted from a segment.
     */
    record KeyBytesAndValueBytesInSegment(byte[] keyBytes, byte[] valueBytes) {
    }

    /**
     * Extracts key and value bytes from a SLIM segment based on sub-block index and segment offset.
     *
     * @param segmentBytesSlim the byte array containing the SLIM segment data
     * @param subBlockIndex    the sub-block index to search for
     * @param segmentOffset    the segment offset to search for
     * @return the KeyBytesAndValueBytesInSegment object containing the key and value bytes, or null if not found
     */
    static KeyBytesAndValueBytesInSegment getKeyBytesAndValueBytesInSegmentBytesSlim(byte[] segmentBytesSlim, byte subBlockIndex, int segmentOffset) {
        byte[] uncompressedSegmentBytes;
        if (isSegmentBytesSlimAndCompressed(segmentBytesSlim, 0)) {
            var buffer = ByteBuffer.wrap(segmentBytesSlim);
            var dstSize = buffer.getInt();
            var srcSize = buffer.getInt();
            uncompressedSegmentBytes = new byte[dstSize];
            Zstd.decompressByteArray(uncompressedSegmentBytes, 0, dstSize, segmentBytesSlim, SEGMENT_HEADER_LENGTH, srcSize);
        } else {
            uncompressedSegmentBytes = segmentBytesSlim;
        }

        var buffer = ByteBuffer.wrap(uncompressedSegmentBytes);

        // seq long + segment type byte + cv number int + crc int
        var cvNumber = buffer.getInt(8 + 1);
        // crc check
//        var crc32 = buffer.getInt(8 + 1 + 4);
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

    record EncodedSlimBytesAndValueBytesLength(byte[] bytes, int valueBytesLength) {
        @Override
        public @NotNull String toString() {
            return "EncodedSlimBytesAndValueBytesLength{" +
                    "bytes.length=" + bytes.length +
                    ", valueBytesLength=" + valueBytesLength +
                    '}';
        }
    }

    /**
     * Encodes a list of valid CV entries into a SLIM segment format.
     * Probe liner encode and search. Do zstd compressed if needed.
     *
     * @param invalidCvList the list of CvWithKeyAndSegmentOffset objects to encode
     * @return the byte array representing the encoded SLIM segment + value bytes length total, or null if the encoded size exceeds the segment length limit
     */
    static EncodedSlimBytesAndValueBytesLength encodeValidCvListSlim(List<CvWithKeyAndSegmentOffset> invalidCvList) {
        var lastOneSeqAsSegmentSeq = invalidCvList.getLast().cv.getSeq();
        int bytesLengthN = SEGMENT_HEADER_LENGTH;

        var valueBytesLength = 0;
        for (var one : invalidCvList) {
            var keyLength = one.key.length();
            var cvEncodedLength = one.cv.encodedLength();

            // 1 byte for sub block index, 4 bytes for segment offset
            // 2 bytes for key length, key bytes, 4 bytes for cv encoded length, cv encoded bytes
            bytesLengthN += 1 + 4 + 2 + keyLength + 4 + cvEncodedLength;
            valueBytesLength += cvEncodedLength;
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

        var compressedBytes = Zstd.compress(bytes);
        // 0.9 or 0.8
        if (compressedBytes.length < bytes.length * 0.8) {
            // should never happen here
            if (compressedBytes.length >= ConfForSlot.global.confChunk.segmentLength) {
                return null;
            }

            var bytes2 = new byte[SEGMENT_HEADER_LENGTH + compressedBytes.length];
            var buffer2 = ByteBuffer.wrap(bytes2);
            // for decompress dst / src bytes length
            buffer2.putInt(bytes.length);
            buffer2.putInt(compressedBytes.length);
            buffer2.put(Chunk.SegmentType.SLIM_AND_COMPRESSED.val);

            buffer2.put(SEGMENT_HEADER_LENGTH, compressedBytes);
            return new EncodedSlimBytesAndValueBytesLength(bytes2, valueBytesLength);
        } else {
            // should never happen here
            if (bytesLengthN >= ConfForSlot.global.confChunk.segmentLength) {
                return null;
            }

            return new EncodedSlimBytesAndValueBytesLength(bytes, valueBytesLength);
        }
    }

    /**
     * A record representing a compressed value with its associated key, segment offset, segment index, and sub-block index.
     *
     * @param cv            the compressed value
     * @param key           the key associated with the compressed value
     * @param segmentOffset the segment offset of the key value pair
     * @param segmentIndex  the segment index of the key value pair
     * @param subBlockIndex the sub-block index of the key value pair
     */
    public record CvWithKeyAndSegmentOffset(CompressedValue cv, String key, int segmentOffset, int segmentIndex,
                                            byte subBlockIndex) {
        public String shortString() {
            return "k=" + key + ", s=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
        }

        @Override
        public @NotNull String toString() {
            return "CvWithKeyAndSegmentOffset{" +
                    "key='" + key + '\'' +
                    ", segmentOffset=" + segmentOffset +
                    ", segmentIndex=" + segmentIndex +
                    ", subBlockIndex=" + subBlockIndex +
                    '}';
        }
    }

    /**
     * Reads segment bytes and populates a list of CvWithKeyAndSegmentOffset objects, excluding expired entries.
     *
     * @param cvList                     the list to populate with CvWithKeyAndSegmentOffset objects
     * @param segmentBytesBatchRead      the byte array containing the segment data
     * @param relativeOffsetInBatchBytes the starting offset within the segment data
     * @param chunkSegmentLength         the length of each chunk segment
     * @param segmentIndex               the segment index
     * @param slot                       the slot index associated with this segment batch
     * @return the count of expired entries encountered during processing
     */
    static int readToCvList(ArrayList<CvWithKeyAndSegmentOffset> cvList, byte[] segmentBytesBatchRead, int relativeOffsetInBatchBytes,
                            int chunkSegmentLength, int segmentIndex, short slot) {
        final int[] expiredCountArray = {0};
        var length = Math.min(segmentBytesBatchRead.length, chunkSegmentLength);

        if (SegmentBatch2.isSegmentBytesTight(segmentBytesBatchRead, relativeOffsetInBatchBytes)) {
            var buffer = ByteBuffer.wrap(segmentBytesBatchRead, relativeOffsetInBatchBytes, length).slice();
            // iterate sub blocks, refer to SegmentBatch.tight
            for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
                buffer.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
                var subBlockOffset = buffer.getShort();
                if (subBlockOffset == 0) {
                    // skip
                    break;
                }
                var subBlockLength = buffer.getShort();

                var decompressedBytes = new byte[chunkSegmentLength];
                var d = Zstd.decompressByteArray(decompressedBytes, 0, chunkSegmentLength,
                        segmentBytesBatchRead, relativeOffsetInBatchBytes + subBlockOffset, subBlockLength);
                if (d != chunkSegmentLength) {
                    throw new IllegalStateException("Decompress error, s=" + slot
                            + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunkSegmentLength);
                }

                int finalSubBlockIndex = subBlockIndex;
                SegmentBatch2.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                    // exclude expired
                    if (cv.isExpired()) {
                        expiredCountArray[0]++;
                        return;
                    }
                    cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));
                });
            }
        } else {
            SegmentBatch2.iterateFromSegmentBytes(segmentBytesBatchRead, relativeOffsetInBatchBytes, length, (key, cv, offsetInThisSegment) -> {
                // exclude expired
                if (cv.isExpired()) {
                    expiredCountArray[0]++;
                    return;
                }
                cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) 0));
            });
        }
        return expiredCountArray[0];
    }
}
