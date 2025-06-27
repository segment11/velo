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
    private final short slot;
    private final int maxSegmentNumber;

    // init value length int + current value length int
    private final byte[] bytes;
    private final ByteBuffer byteBuffer;

    /**
     * Flush all, set all to 0.
     */
    void flush() {
        Arrays.fill(bytes, (byte) 0);
        for (var fillRatioBucket : fillRatioBucketArray) {
            fillRatioBucket.clear();
        }
    }

    /**
     * By value length fill ratio, every 5% is a bucket. index is 0 - 20
     */
    static final int FILL_RATIO_BUCKETS = 100 / 5 + 1;
    final LinkedList<Integer>[] fillRatioBucketArray = new LinkedList[FILL_RATIO_BUCKETS];

    /**
     * Constructs a new MetaChunkSegmentFillRatio instance for a given slot.
     *
     * @param slot The slot number.
     */
    public MetaChunkSegmentFillRatio(short slot) {
        this.slot = slot;
        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();

        this.bytes = new byte[maxSegmentNumber * 8];
        this.byteBuffer = ByteBuffer.wrap(bytes);

        for (int i = 0; i < FILL_RATIO_BUCKETS; i++) {
            this.fillRatioBucketArray[i] = new LinkedList<>();
        }
    }

    /**
     * Set the fill ratio of a segment.
     *
     * @param segmentIndex         The segment index
     * @param initValueBytesLength The initial value length
     */
    public void set(int segmentIndex, int initValueBytesLength) {
        var offset = segmentIndex * 8;
        byteBuffer.putInt(offset, initValueBytesLength);
        byteBuffer.putInt(offset + 4, initValueBytesLength);

        // init fill ratio is 100%
        fillRatioBucketArray[FILL_RATIO_BUCKETS - 1].add(segmentIndex);
    }

    /**
     * Update the segment fill ratio.
     *
     * @param segmentIndex     The segment index
     * @param valueBytesLength Removed value bytes length (Wal.V persist length)
     */
    public void remove(int segmentIndex, int valueBytesLength) {
        var offset = segmentIndex * 8;
        var initValueBytesLength = byteBuffer.getInt(offset);
        var oldValueBytesLength = byteBuffer.getInt(offset + 4);
        var newValueBytesLength = initValueBytesLength - valueBytesLength;
        byteBuffer.putInt(offset + 4, newValueBytesLength);

        var oldFillRatio = (oldValueBytesLength * 100) / initValueBytesLength;
        var newFillRatio = (newValueBytesLength * 100) / initValueBytesLength;

        var oldIndex = oldFillRatio / 5;
        var newIndex = newFillRatio / 5;

        fillRatioBucketArray[oldIndex].remove(segmentIndex);
        fillRatioBucketArray[newIndex].add(segmentIndex);
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        // linked list node object header 16B
        long size = (8L + 16) * maxSegmentNumber;
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

        // re-calc each segment fill ratio
        for (var fillRatioBucket : fillRatioBucketArray) {
            fillRatioBucket.clear();
        }
        for (int i = 0; i < maxSegmentNumber; i++) {
            var offset = i * 8;
            var initValueBytesLength = byteBuffer.getInt(offset);
            var valueBytesLength = byteBuffer.getInt(offset + 4);
            if (initValueBytesLength != 0) {
                var fillRatio = (valueBytesLength * 100) / initValueBytesLength;
                var index = fillRatio / 5;
                fillRatioBucketArray[index].add(i);
            }
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
