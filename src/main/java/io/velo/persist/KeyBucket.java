package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.SnowFlake;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static io.velo.CompressedValue.NO_EXPIRE;
import static io.velo.persist.KeyLoader.KEY_BUCKET_ONE_COST_SIZE;

/**
 * A data structure that holds key-value pairs optimized for storage and retrieval.
 */
public class KeyBucket {
    public static final short INIT_CAPACITY = 48;
    public static final int DEFAULT_BUCKETS_PER_SLOT = 16384 * 4;
    public static final int MAX_BUCKETS_PER_SLOT = 16384 * 16;

    static final byte[] EMPTY_BYTES = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE];

    private static final int ONE_CELL_LENGTH = 60;
    private static final int HASH_VALUE_LENGTH = 8;
    private static final int EXPIRE_AT_VALUE_LENGTH = 8;
    private static final int SEQ_VALUE_LENGTH = 8;
    private static final int ONE_CELL_META_LENGTH = HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH + SEQ_VALUE_LENGTH;
    private static final int HEADER_LENGTH = 8 + 2 + 2;

    // just make sure one page size 4096 can contain all cells when refactoring
//    private static final int INIT_BYTES_LENGTH = HEADER_LENGTH + INIT_CAPACITY * (ONE_CELL_META_LENGTH + ONE_CELL_LENGTH);
//    static {
//        if (INIT_BYTES_LENGTH > KEY_BUCKET_ONE_COST_SIZE) {
//            throw new IllegalStateException("INIT_BYTES_LENGTH > KEY_BUCKET_ONE_COST_SIZE");
//        }
//    }

    private final int capacity;
    short size;
    short cellCost;

    @VisibleForTesting
    long lastUpdateSeq;
    private byte lastUpdateSplitNumber;

    private final SnowFlake snowFlake;

    private int oneCellOffset(int cellIndex) {
        return HEADER_LENGTH + capacity * ONE_CELL_META_LENGTH + cellIndex * ONE_CELL_LENGTH;
    }

    private int metaIndex(int cellIndex) {
        return HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
    }

    private static final long NO_KEY = 0;
    private static final long PRE_KEY = -1;

    private final short slot;
    private final int bucketIndex;
    final byte splitIndex;
    byte splitNumber;

    public byte getSplitNumber() {
        return splitNumber;
    }

    public byte getSplitIndex() {
        return splitIndex;
    }

    @Override
    public String toString() {
        return "KeyBucket{" +
                "slot=" + slot +
                ", bucketIndex=" + bucketIndex +
                ", splitIndex=" + splitIndex +
                ", splitNumber=" + splitNumber +
                ", capacity=" + capacity +
                ", size=" + size +
                ", cellCost=" + cellCost +
                ", lastUpdateSeq=" + lastUpdateSeq +
                '}';
    }

    final byte[] bytes;
    final int position;

    boolean isSharedBytes() {
        return bytes.length != KEY_BUCKET_ONE_COST_SIZE;
    }

    public void clearAll() {
        this.buffer.position(0).put(EMPTY_BYTES);
        this.size = 0;
        this.cellCost = 0;
        this.lastUpdateSeq = 0L;
        this.lastUpdateSplitNumber = 0;
    }

    /**
     * @param slot        slot index of this key bucket
     * @param bucketIndex index of this key bucket in the slot
     * @param splitIndex  the split index
     * @param splitNumber the split number
     * @param bytes       the byte array containing the key-value pairs
     * @param snowFlake   SnowFlake instance for generating unique sequence numbers
     */
    public KeyBucket(short slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] bytes, @NotNull SnowFlake snowFlake) {
        this(slot, bucketIndex, splitIndex, splitNumber, bytes, 0, snowFlake);
    }

    /**
     * @param slot        slot index of this key bucket
     * @param bucketIndex index of this key bucket in the slot
     * @param splitIndex  the split index
     * @param splitNumber the split number
     * @param sharedBytes the shared byte array containing the key-value pairs
     * @param position    position in the shared byte array
     * @param snowFlake   SnowFlake instance for generating unique sequence numbers
     */
    public KeyBucket(short slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] sharedBytes, int position, @NotNull SnowFlake snowFlake) {
        this.slot = slot;
        this.bucketIndex = bucketIndex;
        this.splitIndex = splitIndex;
        this.splitNumber = splitNumber;

        this.capacity = INIT_CAPACITY;
        this.size = 0;
        this.cellCost = 0;
        this.snowFlake = snowFlake;

        if (sharedBytes == null) {
            this.bytes = new byte[KEY_BUCKET_ONE_COST_SIZE];
            this.position = 0;
            this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE);
        } else {
            if (sharedBytes.length % KEY_BUCKET_ONE_COST_SIZE != 0) {
                throw new IllegalStateException("Key bucket shared bytes length must be multiple of " + KEY_BUCKET_ONE_COST_SIZE);
            }

            if (sharedBytes.length <= position) {
                this.bytes = new byte[KEY_BUCKET_ONE_COST_SIZE];
                this.position = 0;
                this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE);
            } else {
                this.bytes = sharedBytes;
                this.position = position;
                this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE).slice();
            }
        }

        this.lastUpdateSeq = buffer.getLong();
        this.lastUpdateSplitNumber = (byte) (lastUpdateSeq & 0b1111);
        this.size = buffer.getShort();
        this.cellCost = buffer.getShort();

        if (splitNumber == -1) {
            // use seq split number
            if (this.lastUpdateSeq != 0) {
                this.splitNumber = lastUpdateSplitNumber;
            } else {
                this.splitNumber = 1;
            }
        } else {
            if (this.lastUpdateSeq != 0 && lastUpdateSplitNumber != splitNumber) {
                throw new IllegalStateException("Key bucket last update split number not match, last=" + lastUpdateSplitNumber + ", current=" + splitNumber
                        + ", slot=" + slot + ", bucket index=" + bucketIndex + ", split index=" + splitIndex);
            }
        }
    }

    /**
     * Result of a tolerant key-bucket read that may have recovered from a crash-window mismatch.
     *
     * @param keyBucket            the constructed bucket (never null)
     * @param effectiveSplitNumber the split number actually encoded in the bucket's
     *                             lastUpdateSeq. Equals the metadata split number on the happy path, and equals the
     *                             embedded split number on the recovery path.
     * @param wasRecovered         true if the constructor threw on the first attempt and the caller
     *                             was forced to fall back to the embedded split number.
     */
    public record ReadResult(@NotNull KeyBucket keyBucket, byte effectiveSplitNumber, boolean wasRecovered) {
    }

    private static final Logger log = LoggerFactory.getLogger(KeyBucket.class);

    /**
     * Tolerant constructor for on-disk reads. Tries the given {@code metadataSplitNumber}
     * first; if the {@code lastUpdateSplitNumber} embedded in the bucket header disagrees
     * (the crash-window mismatch documented in bug 48 finding 1), retries with
     * {@code splitNumber = -1} so the constructor uses the embedded value. The caller is
     * expected to use {@link ReadResult#effectiveSplitNumber()} to drive metadata repair.
     *
     * <p>The recovery path covers both crash windows:
     * <ul>
     *   <li>Window A (data-new, metadata-old): retry gives the new split number.</li>
     *   <li>Window B (data-old, metadata-new): retry gives the old split number, which
     *       must be persisted back to metadata because per-key lookups are hash-routed
     *       by {@code KeyHash.splitIndex(keyHash, splitNumber, bucketIndex)} and a stale
     *       higher metadata would route keys to non-existent splits.</li>
     * </ul>
     */
    @VisibleForTesting
    public static ReadResult readWithFallback(short slot, int bucketIndex, byte splitIndex, byte metadataSplitNumber,
                                              @Nullable byte[] sharedBytes, int position, @NotNull SnowFlake snowFlake) {
        try {
            var bucket = new KeyBucket(slot, bucketIndex, splitIndex, metadataSplitNumber, sharedBytes, position, snowFlake);
            return new ReadResult(bucket, metadataSplitNumber, false);
        } catch (IllegalStateException e) {
            // Crash-window mismatch: decode with the embedded split number from lastUpdateSeq.
            var bucket = new KeyBucket(slot, bucketIndex, splitIndex, (byte) -1, sharedBytes, position, snowFlake);
            log.warn("Key bucket split number mismatch, recovered from crash window: slot={}, bucket={}, split={}, metadata={}, embedded={}",
                    slot, bucketIndex, splitIndex, metadataSplitNumber, bucket.splitNumber);
            return new ReadResult(bucket, bucket.splitNumber, true);
        }
    }

    public interface IterateCallBack {
        void call(long keyHash, long expireAt, long seq, String key, byte[] valueBytes);
    }

    /**
     * @param callBack callback invoked for each key-value pair
     */
    public void iterate(@NotNull IterateCallBack callBack) {
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
            var seq = buffer.getLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH);
            var kvBytes = getFromOneCell(cellIndex);

            callBack.call(cellHashValue, expireAt, seq, kvBytes.key, kvBytes.valueBytes);
        }
    }

    private record KeyAndValueBytes(byte[] keyBytes, String key, byte[] valueBytes) {
    }

    private KeyAndValueBytes getFromOneCell(int cellIndex) {
        buffer.position(oneCellOffset(cellIndex));

        var keyLength = buffer.getShort();
        // data error, need recover
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }
        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        var valueLength = buffer.get();
        // data error, need recover
        if (valueLength < 0) {
            throw new IllegalStateException("Value length error, value length=" + valueLength);
        }
        var valueBytes = new byte[valueLength];
        buffer.get(valueBytes);

        return new KeyAndValueBytes(keyBytes, Wal.keyString(keyBytes), valueBytes);
    }

    record KVMeta(int offset, short keyLength, byte valueLength) {
        int valueOffset() {
            return offset + Short.BYTES + keyLength + Byte.BYTES;
        }

        int cellCount() {
            int keyWithValueBytesLength = Short.BYTES + keyLength + Byte.BYTES + valueLength;
            int cellCount = keyWithValueBytesLength / ONE_CELL_LENGTH;
            if (keyWithValueBytesLength % ONE_CELL_LENGTH != 0) {
                cellCount++;
            }
            return cellCount;
        }

        static int calcCellCount(short keyLength, byte valueLength) {
            int keyWithValueBytesLength = Short.BYTES + keyLength + Byte.BYTES + valueLength;
            int cellCount = keyWithValueBytesLength / ONE_CELL_LENGTH;
            if (keyWithValueBytesLength % ONE_CELL_LENGTH != 0) {
                cellCount++;
            }
            return cellCount;
        }

        @Override
        public String toString() {
            return "KVMeta{" +
                    "offset=" + offset +
                    ", keyLength=" + keyLength +
                    ", valueLength=" + valueLength +
                    '}';
        }
    }

    private final ByteBuffer buffer;

    public void putMeta() {
        updateSeq();
        buffer.position(0).putLong(lastUpdateSeq).putShort(size).putShort(cellCost);
    }

    /**
     * @param doUpdateSeq whether to update the sequence number before encoding
     * @return byte array representation of this key bucket
     */
    public byte[] encode(boolean doUpdateSeq) {
        if (doUpdateSeq) {
            updateSeq();
        }

        buffer.position(0).putLong(lastUpdateSeq).putShort(size).putShort(cellCost);
        if (isSharedBytes()) {
            var dst = new byte[KEY_BUCKET_ONE_COST_SIZE];
            System.arraycopy(bytes, position, dst, 0, KEY_BUCKET_ONE_COST_SIZE);
            return dst;
        } else {
            return bytes;
        }
    }

    public interface CvExpiredOrDeletedCallBack {
        void handle(@NotNull String key, @NotNull CompressedValue shortStringCv);

        void handle(@NotNull String key, @NotNull PersistValueMeta cv);
    }

    CvExpiredOrDeletedCallBack cvExpiredOrDeletedCallBack;

    @VisibleForTesting
    void clearOneExpiredOrDeleted(int i) {
        clearOneExpiredOrDeleted(i, null);
    }

    private void clearOneExpiredOrDeleted(int i, @Nullable KeyAndValueBytes kvBytesAlreadyGet) {
        if (i >= capacity) {
            throw new IllegalArgumentException("i >= capacity");
        }

        if (cvExpiredOrDeletedCallBack != null) {
            var kvBytes = kvBytesAlreadyGet == null ? getFromOneCell(i) : kvBytesAlreadyGet;
            if (!PersistValueMeta.isPvm(kvBytes.valueBytes)) {
                var shortStringCv = CompressedValue.decode(kvBytes.valueBytes, kvBytes.keyBytes, 0L);
                cvExpiredOrDeletedCallBack.handle(kvBytes.key, shortStringCv);
            } else {
                var pvm = PersistValueMeta.decode(kvBytes.valueBytes);
                cvExpiredOrDeletedCallBack.handle(kvBytes.key, pvm);
            }
        }

        short cellCount = 1;
        for (int cellIndex = i + 1; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
            var nextCellHashValue = buffer.getLong(metaIndex);
            if (nextCellHashValue == PRE_KEY) {
                cellCount++;
            } else {
                break;
            }
        }
        clearCell(i, cellCount);
        size--;
        cellCost -= cellCount;
    }

    public void clearAllExpired() {
        final long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < capacity; i++) {
            int metaIndex = metaIndex(i);
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
            if (expireAt != NO_EXPIRE && expireAt < currentTimeMillis) {
                clearOneExpiredOrDeleted(i);
            }
        }
    }

    @TestOnly
    String allPrint() {
        var sb = new StringBuilder();
        iterate((keyHash, expireAt, seq, key, valueBytes) -> sb.append("key=").append(key)
                .append(", value=").append(PersistValueMeta.isPvm(valueBytes) ? PersistValueMeta.decode(valueBytes) : new String(valueBytes))
                .append(", expireAt=").append(expireAt)
                .append(", seq=").append(seq)
                .append("\n"));
        return sb.toString();
    }

    @VisibleForTesting
    void updateSeq() {
        long seq = snowFlake.nextId();
        // last 4 bits for split number for data check, max split number is 9
        // can not compare bigger or smaller, just compare equal or not, !important
        lastUpdateSeq = seq << 4 | splitNumber;
    }

    private record CanPutResult(boolean flag, boolean isUpdate) {
    }

    public record DoPutResult(boolean isPut, boolean isUpdate) {
    }

    /**
     * @param key        key
     * @param keyHash    hash value of the key
     * @param expireAt   expiration time
     * @param seq        sequence number
     * @param valueBytes byte array of the value
     * @return result of the put operation
     */
    public DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes) {
        return put(key, keyHash, expireAt, seq, valueBytes, true);
    }

    /**
     * @param key         key
     * @param keyHash     hash value of the key
     * @param expireAt    expiration time
     * @param seq         sequence number
     * @param valueBytes  byte array of the value
     * @param doUpdateSeq if true, update the sequence number after putting
     * @return result of the put operation
     */
    public DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes, boolean doUpdateSeq) {
        return put(key, keyHash, expireAt, seq, valueBytes, doUpdateSeq, true);
    }

    private DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes, boolean doUpdateSeq, boolean doDeleteTargetKeyFirst) {
        if (valueBytes.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Value bytes too large, value length=" + valueBytes.length);
        }

        // 0 and -1 are reserved in the key-bucket meta hash field for NO_KEY and PRE_KEY markers.
        if (keyHash == NO_KEY || keyHash == PRE_KEY) {
            throw new IllegalArgumentException("Reserved key hash for key bucket, key hash=" + keyHash + ", key=" + key);
        }

        var keyBytes = Wal.keyBytes(key);
        int cellCount = KVMeta.calcCellCount((short) keyBytes.length, (byte) valueBytes.length);
        if (cellCount >= INIT_CAPACITY) {
            throw new IllegalArgumentException("Key with value bytes too large, key length=" + keyBytes.length
                    + ", value length=" + valueBytes.length);
        }

        // all in memory, performance is not a problem
        var isExists = doDeleteTargetKeyFirst && del(key, keyHash, false);

        boolean isUpdate = false;
        int putToCellIndex = -1;
        for (int i = 0; i < capacity; i++) {
            var canPutResult = canPut(keyBytes, keyHash, i, cellCount);
            if (canPutResult.flag) {
                putToCellIndex = i;
                isUpdate = isExists;
                break;
            }
        }

        if (putToCellIndex == -1) {
            // maybe cost cell = 2, but can put cells are always is 1 because expired or deleted the same key cell cost is 1
            if (INIT_CAPACITY - cellCost >= cellCount) {
                // need re-put all in this key bucket
                rePutAll();
                // put again, but need not delete target key again
                return put(key, keyHash, expireAt, seq, valueBytes, doUpdateSeq, false);
            }
            return new DoPutResult(false, false);
        }

        putTo(putToCellIndex, cellCount, keyHash, expireAt, seq, key, keyBytes, valueBytes);
        size++;
        cellCost += (short) cellCount;

        if (doUpdateSeq) {
            updateSeq();
        }
        return new DoPutResult(true, isUpdate);
    }

    @VisibleForTesting
    void rePutAll() {
        ArrayList<PersistValueMeta> tmpList = new ArrayList<>(INIT_CAPACITY);
        iterate((keyHash, expireAt, seq, key, valueBytes) -> {
            var pvm = new PersistValueMeta();
            pvm.expireAt = expireAt;
            pvm.seq = seq;
            pvm.key = key;
            pvm.keyHash = keyHash;
            pvm.bucketIndex = bucketIndex;
            pvm.extendBytes = valueBytes;
            tmpList.add(pvm);
        });

        buffer.position(0).put(EMPTY_BYTES);

        int putToCellIndex = 0;
        for (var pvm : tmpList) {
            var keyBytes = Wal.keyBytes(pvm.key);
            var cellCount = KVMeta.calcCellCount((short) keyBytes.length, (byte) pvm.extendBytes.length);
            putTo(putToCellIndex, cellCount, pvm.keyHash, pvm.expireAt, pvm.seq, pvm.key, keyBytes, pvm.extendBytes);
            putToCellIndex += cellCount;
        }
    }

    private void putTo(int putToCellIndex, int cellCount, long keyHash, long expireAt, long seq, String key, byte[] keyBytes, byte[] valueBytes) {
        int metaIndex = metaIndex(putToCellIndex);
        buffer.putLong(metaIndex, keyHash);
        buffer.putLong(metaIndex + HASH_VALUE_LENGTH, expireAt);
        buffer.putLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, seq);

        for (int i = 1; i < cellCount; i++) {
            int nextIndex = metaIndex(putToCellIndex + i);
            buffer.putLong(nextIndex, PRE_KEY);
            buffer.putLong(nextIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(nextIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, 0L);
        }

        // reset old PRE_KEY to NO_KEY
        int beginResetOldCellIndex = putToCellIndex + cellCount;
        buffer.position(metaIndex(beginResetOldCellIndex));
        while (beginResetOldCellIndex < capacity) {
            var targetCellHashValue = buffer.getLong();
            buffer.position(buffer.position() + EXPIRE_AT_VALUE_LENGTH);

            if (targetCellHashValue != PRE_KEY) {
                break;
            }

            // never happen here, because before put, always delete target key first
            buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH - HASH_VALUE_LENGTH, NO_KEY);
            beginResetOldCellIndex++;
        }

        var cellOffset = oneCellOffset(putToCellIndex);
        buffer.position(cellOffset);
        buffer.putShort((short) keyBytes.length);
        buffer.put(keyBytes);
        // number or short value or pvm, 1 byte is enough
        buffer.put((byte) valueBytes.length);
        buffer.put(valueBytes);
    }

    private CanPutResult canPut(byte[] keyBytes, long keyHash, int cellIndex, int cellCount) {
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);
        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);

        if (cellHashValue == NO_KEY) {
            var flag = isCellAvailableN(cellIndex, cellCount, false);
            return new CanPutResult(flag, false);
        } else if (cellHashValue == PRE_KEY) {
            return new CanPutResult(false, false);
        } else {
            if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                clearOneExpiredOrDeleted(cellIndex);
                // check again
                return canPut(keyBytes, keyHash, cellIndex, cellCount);
            }

            if (cellHashValue != keyHash) {
                return new CanPutResult(false, false);
            }

            var matchMeta = keyMatch(keyBytes, oneCellOffset(cellIndex));
            if (matchMeta != null) {
                // update
                // never happen here, because before put, always delete target key first
                var flag = isCellAvailableN(cellIndex + 1, cellCount - 1, true);
                return new CanPutResult(flag, true);
            } else {
                // hash conflict
                return new CanPutResult(false, false);
            }
        }
    }

    /**
     * @param cellIndex   starting index of the cell
     * @param cellCount   number of cells to check
     * @param isForUpdate if true, checks for updating an existing key-value pair
     * @return true if the cells are available
     */
    @VisibleForTesting
    boolean isCellAvailableN(int cellIndex, int cellCount, boolean isForUpdate) {
        for (int i = 0; i < cellCount; i++) {
            int nextCellIndex = cellIndex + i;
            if (nextCellIndex >= capacity) {
                return false;
            }

            int metaIndex = metaIndex(nextCellIndex);
            var cellHashValue = buffer.getLong(metaIndex);
            if (isForUpdate) {
                if (cellHashValue != PRE_KEY && cellHashValue != NO_KEY) {
                    return false;
                }
            } else if (cellHashValue != NO_KEY) {
                return false;
            }
        }
        return true;
    }

    public record ExpireAtAndSeq(long expireAt, long seq) {
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    /**
     * @param key     key
     * @param keyHash hash value of the key
     * @return ExpireAtAndSeq, or null if key does not exist
     */
    public ExpireAtAndSeq getExpireAtAndSeqByKey(String key, long keyHash) {
        if (size == 0) {
            return null;
        }

        for (int i = 0; i < capacity; i++) {
            var r = getExpireAtAndSeqByKeyWithCellIndex(key, keyHash, i);
            if (r != null) {
                return r.isExpired() ? null : r;
            }
        }

        return null;
    }

    private ExpireAtAndSeq getExpireAtAndSeqByKeyWithCellIndex(String key, long keyHash, int cellIndex) {
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);
        // NO_KEY or PRE_KEY
        if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
            return null;
        }
        if (cellHashValue != keyHash) {
            return null;
        }

        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
        var seq = buffer.getLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH);

        var matchMeta = keyMatch(Wal.keyBytes(key), oneCellOffset(cellIndex));
        if (matchMeta == null) {
            // hash conflict
            return null;
        }

        return new ExpireAtAndSeq(expireAt, seq);
    }

    public record ValueBytesWithExpireAtAndSeq(byte[] valueBytes, long expireAt, long seq) {
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    /**
     * @param key     key
     * @param keyHash hash value of the key
     * @return ValueBytesWithExpireAtAndSeq, or null if key does not exist
     */
    public ValueBytesWithExpireAtAndSeq getValueXByKey(String key, long keyHash) {
        if (size == 0) {
            return null;
        }

        for (int i = 0; i < capacity; i++) {
            var r = getValueXByKeyWithCellIndex(key, keyHash, i);
            if (r != null) {
                return r;
            }
        }

        return null;
    }

    private ValueBytesWithExpireAtAndSeq getValueXByKeyWithCellIndex(String key, long keyHash, int cellIndex) {
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);
        // NO_KEY or PRE_KEY
        if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
            return null;
        }
        if (cellHashValue != keyHash) {
            return null;
        }

        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
        var seq = buffer.getLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH);

        var matchMeta = keyMatch(Wal.keyBytes(key), oneCellOffset(cellIndex));
        if (matchMeta == null) {
            // hash conflict
            return null;
        }

        var valueBytes = new byte[matchMeta.valueLength];
        buffer.position(matchMeta.valueOffset()).get(valueBytes);
        return new ValueBytesWithExpireAtAndSeq(valueBytes, expireAt, seq);
    }

    private void clearCell(int beginCellIndex, int cellCount) {
        for (int i = 0; i < cellCount; i++) {
            var nextCellIndex = beginCellIndex + i;
            int metaIndex = metaIndex(nextCellIndex);
            buffer.putLong(metaIndex, NO_KEY);
            buffer.putLong(metaIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, 0L);
        }

        // set 0 for better compress ratio
        var beginCellOffset = oneCellOffset(beginCellIndex);
        var bytes0 = new byte[ONE_CELL_LENGTH * cellCount];

        buffer.put(beginCellOffset, bytes0);
    }

    /**
     * @param key         key
     * @param keyHash     hash value of the key
     * @param doUpdateSeq if true, update the sequence number after deleting
     * @return true if the key-value pair was successfully deleted
     */
    public boolean del(String key, long keyHash, boolean doUpdateSeq) {
        if (size == 0) {
            return false;
        }

        boolean isDeleted = false;
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }
            if (cellHashValue != keyHash) {
                continue;
            }

            var cellOffset = oneCellOffset(cellIndex);
            var keyBytes = Wal.keyBytes(key);
            var matchMeta = keyMatch(keyBytes, cellOffset);
            if (matchMeta != null) {
                var valueBytes = new byte[matchMeta.valueLength];
                buffer.position(matchMeta.valueOffset()).get(valueBytes);

                clearOneExpiredOrDeleted(cellIndex, new KeyAndValueBytes(keyBytes, key, valueBytes));

                if (doUpdateSeq) {
                    updateSeq();
                }

                isDeleted = true;
                // need break?
                break;
            } else {
                // hash conflict, just continue
                System.err.println("Key hash conflict, key hash=" + keyHash + ", target cell index=" + cellIndex
                        + ", key=" + key + ", slot=" + slot + ", bucket index=" + bucketIndex);
            }
        }

        return isDeleted;
    }

    private KVMeta keyMatch(byte[] keyBytes, int offset) {
        // compare key length first
        if (keyBytes.length != buffer.getShort(offset)) {
            return null;
        }

        // compare key bytes
        int afterKeyLengthOffset = offset + Short.BYTES;

        var buffer0 = ByteBuffer.wrap(keyBytes);
        var buffer1 = buffer.slice(afterKeyLengthOffset, keyBytes.length);
        if (!buffer0.equals(buffer1)) {
            return null;
        }

        return new KVMeta(offset, (short) keyBytes.length, buffer.get(offset + Short.BYTES + keyBytes.length));
    }
}
