package io.velo.persist;

import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * Manages the segment value bytes length after set or remove for chunks within a specific slot.
 * For pure memory v2 mode. For memory gc.
 */
public class MetaChunkSegmentFillRatio implements InMemoryEstimate, CanSaveAndLoad {
    private final int maxSegmentNumber;

    // init value length int + current value length int + bucket index 1 byte
    // one segment cost bytes: 4 + 4 + 1 = 9
    private final byte[] bytes;
    private final ByteBuffer byteBuffer;

    /**
     * Flush all, set all to 0.
     */
    void flush() {
        Arrays.fill(bytes, (byte) 0);
        Arrays.fill(fillRatioBucketSegmentCount, 0);
    }

    /**
     * By value length fill ratio, every 5% is a bucket. index is 0 - 20
     */
    static final int FILL_RATIO_BUCKETS = 100 / 5 + 1;

    /**
     * For metrics.
     */
    final int[] fillRatioBucketSegmentCount = new int[FILL_RATIO_BUCKETS];

    /**
     * Constructs a new MetaChunkSegmentFillRatio instance for a given slot.
     */
    public MetaChunkSegmentFillRatio() {
        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();

        this.bytes = new byte[maxSegmentNumber * 9];
        this.byteBuffer = ByteBuffer.wrap(bytes);
    }

    /**
     * Set the fill ratio of a segment.
     *
     * @param segmentIndex         The segment index
     * @param initValueBytesLength The initial value length
     */
    public void set(int segmentIndex, int initValueBytesLength) {
        var offset = segmentIndex * 9;

        var oldInitValueBytesLength = byteBuffer.getInt(offset);
        if (oldInitValueBytesLength != 0) {
            var oldFillRatioBucket = byteBuffer.get(offset + 8);
            fillRatioBucketSegmentCount[oldFillRatioBucket]--;
        }

        byteBuffer.putInt(offset, initValueBytesLength);
        byteBuffer.putInt(offset + 4, initValueBytesLength);
        byteBuffer.put(offset + 8, (byte) (FILL_RATIO_BUCKETS - 1));
        fillRatioBucketSegmentCount[FILL_RATIO_BUCKETS - 1]++;
    }

    /**
     * Update the segment fill ratio.
     *
     * @param segmentIndex     The segment index
     * @param valueBytesLength Removed value bytes length (Wal.V persist length)
     */
    public void remove(int segmentIndex, int valueBytesLength) {
        var offset = segmentIndex * 9;
        var initValueBytesLength = byteBuffer.getInt(offset);
        // for unit test
        if (initValueBytesLength == 0) {
            return;
        }
//        assert initValueBytesLength != 0;

        var oldValueBytesLength = byteBuffer.getInt(offset + 4);
        var newValueBytesLength = oldValueBytesLength - valueBytesLength;
        assert newValueBytesLength >= 0;
        byteBuffer.putInt(offset + 4, newValueBytesLength);

        var oldFillRatio = (oldValueBytesLength * 100) / initValueBytesLength;
        var fillRatio = (newValueBytesLength * 100) / initValueBytesLength;

        var oldFillRatioBucket = oldFillRatio / 5;
        var fillRatioBucket = fillRatio / 5;
        byteBuffer.put(offset + 8, (byte) fillRatioBucket);

        if (oldFillRatioBucket != fillRatioBucket) {
            fillRatioBucketSegmentCount[oldFillRatioBucket]--;
            fillRatioBucketSegmentCount[fillRatioBucket]++;
        }
    }

    /**
     * Find segments index those fill ratio less than given fill ratio.
     *
     * @param beginSegmentIndex      Begin segment index
     * @param segmentCount           Segment count
     * @param fillRatioBucketCompare Fill ratio bucket compare
     * @return segments index list
     */
    public LinkedList<Integer> findSegmentsFillRatioLessThan(int beginSegmentIndex, int segmentCount, int fillRatioBucketCompare) {
        LinkedList<Integer> list = new LinkedList<>();
        for (int i = beginSegmentIndex; i < beginSegmentIndex + segmentCount; i++) {
            var offset = i * 9;
            var initValueBytesLength = byteBuffer.getInt(offset);
            if (initValueBytesLength == 0) {
                continue;
            }

            var fillRatioBucket = byteBuffer.get(offset + 8);
            if (fillRatioBucket < fillRatioBucketCompare) {
                list.add(i);
            }
        }
        return list;
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        // linked list node object header 16B
        long size = 9L * maxSegmentNumber;
        sb.append("Meta chunk segment fill ratio: ").append(size).append("\n");
        return size;
    }

    /**
     * Loads chunk segment fill ratio data from a saved file when in pure memory mode.
     *
     * @param is DataInputStream to read from.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        assert ConfForGlobal.pureMemoryV2;
        var bytesLength = is.readInt();
        assert bytesLength == bytes.length;
        is.readFully(bytes);

        // re-calc each segment fill ratio for metrics
        Arrays.fill(fillRatioBucketSegmentCount, 0);
        for (int i = 0; i < maxSegmentNumber; i++) {
            var offset = i * 9;
            var initValueBytesLength = byteBuffer.getInt(offset);
            var valueBytesLength = byteBuffer.getInt(offset + 4);
            if (initValueBytesLength == 0) {
                continue;
            }

            var fillRatio = (valueBytesLength * 100) / initValueBytesLength;
            var fillRatioBucket = fillRatio / 5;
            var savedFillRatioBucket = byteBuffer.get(offset + 8);
            assert fillRatioBucket == savedFillRatioBucket;
            fillRatioBucketSegmentCount[fillRatioBucket]++;
        }
    }

    /**
     * Writes chunk segment fill ratio data to a saved file when in pure memory mode.
     *
     * @param os DataOutputStream to write to.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        assert ConfForGlobal.pureMemoryV2;
        os.writeInt(bytes.length);
        os.write(bytes);
    }
}
