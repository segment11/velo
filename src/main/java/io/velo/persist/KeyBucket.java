package io.velo.persist;

import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.SnowFlake;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static io.velo.CompressedValue.NO_EXPIRE;
import static io.velo.persist.KeyLoader.KEY_BUCKET_ONE_COST_SIZE;

/**
 * A data structure that holds key-value pairs optimized for storage and retrieval.
 * Each key-value pair is stored in a cell, and a key bucket can hold multiple cells.
 * The `KeyBucket` class provides methods for adding, updating, deleting, and retrieving key-value pairs,
 * as well as handling expired or deleted entries.
 */
public class KeyBucket {
    /**
     * Initial capacity of the key bucket, representing the number of cells.
     */
    public static final short INIT_CAPACITY = 48;
    /**
     * Default number of buckets per slot, used in data partitioning.
     */
    public static final int DEFAULT_BUCKETS_PER_SLOT = 16384 * 4;
    /**
     * Max number of buckets per slot, used in data partitioning.
     */
    public static final int MAX_BUCKETS_PER_SLOT = 16384 * 16;

    /**
     * An empty byte array with a size of `KEY_BUCKET_ONE_COST_SIZE` used for initializing key buckets.
     * Read only.
     */
    static final byte[] EMPTY_BYTES = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE];


    /**
     * The maximum length of a single cell, considering metadata and value length.
     * key length short 2 + key length <= 24 + value length byte 1 + (pvm length 12 or number 25 or short string 33) <= 60
     * if key length > 32, refer CompressedValue.KEY_MAX_LENGTH, one key may cost 2 cells
     * for example, big string cv is short value, encoded length = 48, if key length = 16, will cost 2 cells
     */
    private static final int ONE_CELL_LENGTH = 60;
    /**
     * The length of the hash value stored in each cell.
     */
    private static final int HASH_VALUE_LENGTH = 8;
    /**
     * The length of the expiration time stored in each cell.
     */
    private static final int EXPIRE_AT_VALUE_LENGTH = 8;
    /**
     * The length of the sequence number stored in each cell.
     */
    private static final int SEQ_VALUE_LENGTH = 8;
    /**
     * The total length of metadata stored in each cell (hash value, expiration time, and sequence number).
     */
    private static final int ONE_CELL_META_LENGTH = HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH + SEQ_VALUE_LENGTH;
    /**
     * The length of the header in bytes
     * seq long + size short + cell count short
     */
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

    /**
     * The last update sequence number for this key bucket.
     */
    @VisibleForTesting
    long lastUpdateSeq;
    private byte lastUpdateSplitNumber;

    private final SnowFlake snowFlake;

    /**
     * Calculate the offset for a given cell index.
     *
     * @param cellIndex the index of the cell
     * @return the offset in bytes for the given cell index
     */
    private int oneCellOffset(int cellIndex) {
        return HEADER_LENGTH + capacity * ONE_CELL_META_LENGTH + cellIndex * ONE_CELL_LENGTH;
    }

    /**
     * Calculate the meta index for a given cell index.
     *
     * @param cellIndex the index of the cell
     * @return the meta index in bytes for the given cell index
     */
    private int metaIndex(int cellIndex) {
        return HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
    }

    private static final long NO_KEY = 0;
    private static final long PRE_KEY = -1;

    private final short slot;
    private final int bucketIndex;
    final byte splitIndex;
    byte splitNumber;

    /**
     * Get the split number of this key bucket.
     *
     * @return the split number
     */
    public byte getSplitNumber() {
        return splitNumber;
    }

    /**
     * Get the split index of this key bucket.
     *
     * @return the split index
     */
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

    // compressed
    final byte[] bytes;
    final int position;

    /**
     * Check if this key bucket shares bytes with another key bucket.
     *
     * @return true if the bytes are shared, false otherwise
     */
    boolean isSharedBytes() {
        return bytes.length != KEY_BUCKET_ONE_COST_SIZE;
    }

    /**
     * Clear all key-value pairs in this key bucket.
     */
    public void clearAll() {
        this.buffer.position(0).put(EMPTY_BYTES);
        this.size = 0;
        this.cellCost = 0;
        this.lastUpdateSeq = 0L;
        this.lastUpdateSplitNumber = 0;
    }

    /**
     * Create a new key bucket with the specified parameters.
     *
     * @param slot        the slot index of this key bucket
     * @param bucketIndex the index of this key bucket in the slot
     * @param splitIndex  the split index
     * @param splitNumber the split number
     * @param bytes       the byte array containing the key-value pairs
     * @param snowFlake   the SnowFlake instance for generating unique sequence numbers
     */
    public KeyBucket(short slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] bytes, @NotNull SnowFlake snowFlake) {
        this(slot, bucketIndex, splitIndex, splitNumber, bytes, 0, snowFlake);
    }

    /**
     * Create a new key bucket with the specified parameters.
     *
     * @param slot        the slot index of this key bucket
     * @param bucketIndex the index of this key bucket in the slot
     * @param splitIndex  the split index
     * @param splitNumber the split number
     * @param sharedBytes the shared byte array containing the key-value pairs
     * @param position    the position in the shared byte array
     * @param snowFlake   the SnowFlake instance for generating unique sequence numbers
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
     * Interface for callbacks during key-value pair iteration.
     */
    public interface IterateCallBack {
        /**
         * Callback method for each key-value pair.
         *
         * @param keyHash    the hash value of the key
         * @param expireAt   the expiration time
         * @param seq        the sequence number
         * @param key        the key
         * @param valueBytes the byte array of the value
         */
        void call(long keyHash, long expireAt, long seq, String key, byte[] valueBytes);
    }

    /**
     * Iterate over all key-value pairs in this key bucket and call the callback for each pair.
     *
     * @param callBack the callback to be called for each key-value pair
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

    private record KeyAndValueBytes(String key, byte[] valueBytes) {
    }

    /**
     * Retrieve the key and value bytes from a specific cell.
     *
     * @param cellIndex the index of the cell
     * @return the record containing the key and value bytes
     */
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

        return new KeyAndValueBytes(new String(keyBytes), valueBytes);
    }

    /**
     * Record containing metadata about a key-value pair.
     */
    record KVMeta(int offset, short keyLength, byte valueLength) {
        /**
         * Calculate the offset of the value bytes.
         *
         * @return the offset of the value bytes
         */
        int valueOffset() {
            return offset + Short.BYTES + keyLength + Byte.BYTES;
        }

        /**
         * Calculate the number of cells required to store the key-value pair.
         *
         * @return the number of cells.
         */
        int cellCount() {
            int keyWithValueBytesLength = Short.BYTES + keyLength + Byte.BYTES + valueLength;
            int cellCount = keyWithValueBytesLength / ONE_CELL_LENGTH;
            if (keyWithValueBytesLength % ONE_CELL_LENGTH != 0) {
                cellCount++;
            }
            return cellCount;
        }

        /**
         * Calculate the number of cells required to store a key-value pair with the given key and value lengths.
         *
         * @param keyLength   the length of the key
         * @param valueLength the length of the value
         * @return the number of cells
         */
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

    /**
     * Update the metadata of this key bucket (sequence number, size, and cell cost).
     */
    public void putMeta() {
        updateSeq();
        buffer.position(0).putLong(lastUpdateSeq).putShort(size).putShort(cellCost);
    }

    /**
     * Encode this key bucket into a byte array.
     *
     * @param doUpdateSeq whether to update the sequence number before encoding
     * @return the byte array representation of this key bucket
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

    /**
     * Interface for callbacks when a key-value pair is expired or deleted.
     */
    public interface CvExpiredOrDeletedCallBack {
        /**
         * Callback method for expired key-value pairs.
         *
         * @param key           the key
         * @param shortStringCv the CompressedValue of the expired key-value pair
         */
        void handle(@NotNull String key, @NotNull CompressedValue shortStringCv);

        /**
         * Callback method for deleted key-value pairs.
         *
         * @param key the key
         * @param cv  the PersistValueMeta of the deleted key-value pair
         */
        void handle(@NotNull String key, @NotNull PersistValueMeta cv);
    }

    CvExpiredOrDeletedCallBack cvExpiredOrDeletedCallBack;

    /**
     * Clear an expired or deleted key-value pair at the specified cell index.
     *
     * @param i the cell index
     */
    @VisibleForTesting
    void clearOneExpiredOrDeleted(int i) {
        clearOneExpiredOrDeleted(i, null);
    }

    /**
     * Clear an expired or deleted key-value pair at the specified cell index.
     *
     * @param i                 the cell index
     * @param kvBytesAlreadyGet the key-value bytes if already retrieved
     */
    private void clearOneExpiredOrDeleted(int i, @Nullable KeyAndValueBytes kvBytesAlreadyGet) {
        if (i >= capacity) {
            throw new IllegalArgumentException("i >= capacity");
        }

        if (cvExpiredOrDeletedCallBack != null) {
            var kvBytes = kvBytesAlreadyGet == null ? getFromOneCell(i) : kvBytesAlreadyGet;
            if (!PersistValueMeta.isPvm(kvBytes.valueBytes)) {
                var shortStringCv = CompressedValue.decode(Unpooled.wrappedBuffer(kvBytes.valueBytes), kvBytes.key.getBytes(), 0L);
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

    /**
     * Clear all expired key-value pairs in this key bucket.
     */
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

    /**
     * Print all key-value pairs in this key bucket.
     *
     * @return the string representation of all key-value pairs
     */
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

    /**
     * Update the sequence number of this key bucket.
     */
    @VisibleForTesting
    void updateSeq() {
        long seq = snowFlake.nextId();
        // last 4 bits for split number for data check, max split number is 9
        // can not compare bigger or smaller, just compare equal or not, !important
        lastUpdateSeq = seq << 4 | splitNumber;
    }

    private record CanPutResult(boolean flag, boolean isUpdate) {
    }

    /**
     * Record containing the result of a put operation.
     */
    public record DoPutResult(boolean isPut, boolean isUpdate) {
    }

    /**
     * Add or update a key-value pair in this key bucket.
     *
     * @param key        the key
     * @param keyHash    the hash value of the key
     * @param expireAt   the expiration time
     * @param seq        the sequence number
     * @param valueBytes the byte array of the value
     * @return the result of the put operation
     */
    public DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes) {
        return put(key, keyHash, expireAt, seq, valueBytes, true);
    }

    /**
     * Adds or updates a key-value pair in this key bucket.
     *
     * @param key         the key.
     * @param keyHash     the hash value of the key.
     * @param expireAt    the expiration time.
     * @param seq         the sequence number.
     * @param valueBytes  the byte array of the value.
     * @param doUpdateSeq if true, update the sequence number after putting the key-value pair.
     * @return the result of the put operation, indicating whether the operation was successful and if it was an update.
     */
    public DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes, boolean doUpdateSeq) {
        return put(key, keyHash, expireAt, seq, valueBytes, doUpdateSeq, true);
    }

    /**
     * Private method to add or update a key-value pair in this key bucket.
     *
     * @param key                    the key.
     * @param keyHash                the hash value of the key.
     * @param expireAt               the expiration time.
     * @param seq                    the sequence number.
     * @param valueBytes             the byte array of the value.
     * @param doUpdateSeq            if true, update the sequence number after putting the key-value pair.
     * @param doDeleteTargetKeyFirst if true, delete the target key before putting the new key-value pair.
     * @return the result of the put operation, indicating whether the operation was successful and if it was an update.
     */
    private DoPutResult put(String key, long keyHash, long expireAt, long seq, byte[] valueBytes, boolean doUpdateSeq, boolean doDeleteTargetKeyFirst) {
        if (valueBytes.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Value bytes too large, value length=" + valueBytes.length);
        }

        int cellCount = KVMeta.calcCellCount((short) key.length(), (byte) valueBytes.length);
        if (cellCount >= INIT_CAPACITY) {
            throw new IllegalArgumentException("Key with value bytes too large, key length=" + key.length()
                    + ", value length=" + valueBytes.length);
        }

        // all in memory, performance is not a problem
        var isExists = doDeleteTargetKeyFirst && del(key, keyHash, false);

        boolean isUpdate = false;
        int putToCellIndex = -1;
        for (int i = 0; i < capacity; i++) {
            var canPutResult = canPut(key, keyHash, i, cellCount);
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

        putTo(putToCellIndex, cellCount, keyHash, expireAt, seq, key, valueBytes);
        size++;
        cellCost += (short) cellCount;

        if (doUpdateSeq) {
            updateSeq();
        }
        return new DoPutResult(true, isUpdate);
    }

    /**
     * Re-puts all key-value pairs in this key bucket. This method is used when there is no available space for a new entry,
     * so it clears the current entries and re-inserts them in order to make space.
     */
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
            var cellCount = KVMeta.calcCellCount((short) pvm.key.length(), (byte) pvm.extendBytes.length);
            putTo(putToCellIndex, cellCount, pvm.keyHash, pvm.expireAt, pvm.seq, pvm.key, pvm.extendBytes);
            putToCellIndex += cellCount;
        }
    }

    /**
     * Puts a key-value pair into the specified cell index.
     *
     * @param putToCellIndex the index of the cell where the key-value pair should be placed.
     * @param cellCount      the number of cells required for storing the key-value pair.
     * @param keyHash        the hash value of the key.
     * @param expireAt       the expiration time.
     * @param seq            the sequence number.
     * @param key            the key.
     * @param valueBytes     the byte array of the value.
     */
    private void putTo(int putToCellIndex, int cellCount, long keyHash, long expireAt, long seq, String key, byte[] valueBytes) {
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
        buffer.putShort((short) key.length());
        buffer.put(key.getBytes());
        // number or short value or pvm, 1 byte is enough
        buffer.put((byte) valueBytes.length);
        buffer.put(valueBytes);
    }

    /**
     * Checks if a key-value pair can be put into the specified cell index.
     *
     * @param key       the key.
     * @param keyHash   the hash value of the key.
     * @param cellIndex the index of the cell to check.
     * @param cellCount the number of cells required for storing the key-value pair.
     * @return a CanPutResult object indicating whether the key-value pair can be put and if it's an update.
     */
    private CanPutResult canPut(String key, long keyHash, int cellIndex, int cellCount) {
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
                return canPut(key, keyHash, cellIndex, cellCount);
            }

            if (cellHashValue != keyHash) {
                return new CanPutResult(false, false);
            }

            var matchMeta = keyMatch(key.getBytes(), oneCellOffset(cellIndex));
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
     * Checks if a range of cells starting from the specified index is available for putting a key-value pair.
     *
     * @param cellIndex   the starting index of the cell.
     * @param cellCount   the number of cells to check.
     * @param isForUpdate if true, checks for updating an existing key-value pair.
     * @return true if the cells are available, false otherwise.
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

    /**
     * Record containing the expiration time and sequence number of a key-value pair.
     */
    public record ExpireAtAndSeq(long expireAt, long seq) {
        /**
         * Checks if the key-value pair has expired.
         *
         * @return true if the key-value pair has expired, false otherwise.
         */
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    /**
     * Retrieves the expiration time and sequence number of a key-value pair by its key.
     *
     * @param key     the key.
     * @param keyHash the hash value of the key.
     * @return an ExpireAtAndSeq object containing the expiration time and sequence number, or null if the key does not exist.
     */
    public ExpireAtAndSeq getExpireAtAndSeqByKey(String key, long keyHash) {
        if (size == 0) {
            return null;
        }

        for (int i = 0; i < capacity; i++) {
            var r = getExpireAtAndSeqByKeyWithCellIndex(key, keyHash, i);
            if (r != null) {
                return r;
            }
        }

        return null;
    }

    /**
     * Retrieves the expiration time and sequence number of a key-value pair by its key and cell index.
     *
     * @param key       the key.
     * @param keyHash   the hash value of the key.
     * @param cellIndex the index of the cell.
     * @return an ExpireAtAndSeq object containing the expiration time and sequence number, or null if the key does not exist.
     */
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

        var matchMeta = keyMatch(key.getBytes(), oneCellOffset(cellIndex));
        if (matchMeta == null) {
            // hash conflict
            return null;
        }

        return new ExpireAtAndSeq(expireAt, seq);
    }

    /**
     * Record containing the value bytes, expiration time, and sequence number of a key-value pair.
     */
    public record ValueBytesWithExpireAtAndSeq(byte[] valueBytes, long expireAt, long seq) {
        /**
         * Checks if the key-value pair has expired.
         *
         * @return true if the key-value pair has expired, false otherwise.
         */
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    /**
     * Retrieves the value bytes, expiration time, and sequence number of a key-value pair by its key.
     *
     * @param key     the key.
     * @param keyHash the hash value of the key.
     * @return a ValueBytesWithExpireAtAndSeq object containing the value bytes, expiration time, and sequence number, or null if the key does not exist.
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

    /**
     * Retrieves the value bytes, expiration time, and sequence number of a key-value pair by its key and cell index.
     *
     * @param key       the key.
     * @param keyHash   the hash value of the key.
     * @param cellIndex the index of the cell.
     * @return a ValueBytesWithExpireAtAndSeq object containing the value bytes, expiration time, and sequence number, or null if the key does not exist.
     */
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

        var matchMeta = keyMatch(key.getBytes(), oneCellOffset(cellIndex));
        if (matchMeta == null) {
            // hash conflict
            return null;
        }

        var valueBytes = new byte[matchMeta.valueLength];
        buffer.position(matchMeta.valueOffset()).get(valueBytes);
        return new ValueBytesWithExpireAtAndSeq(valueBytes, expireAt, seq);
    }

    /**
     * Clears a range of cells starting from the specified index.
     *
     * @param beginCellIndex the starting index of the cell.
     * @param cellCount      the number of cells to clear.
     */
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
     * Deletes a key-value pair by its key.
     *
     * @param key         the key.
     * @param keyHash     the hash value of the key.
     * @param doUpdateSeq if true, update the sequence number after deleting the key-value pair.
     * @return true if the key-value pair was successfully deleted, false otherwise.
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
            var matchMeta = keyMatch(key.getBytes(), cellOffset);
            if (matchMeta != null) {
                var valueBytes = new byte[matchMeta.valueLength];
                buffer.position(matchMeta.valueOffset()).get(valueBytes);

                clearOneExpiredOrDeleted(cellIndex, new KeyAndValueBytes(key, valueBytes));

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

    /**
     * Check if the key is match in the cell.
     *
     * @param keyBytes the byte array of the key.
     * @param offset   the offset of the cell.
     * @return the KVMeta object if the key is match, null otherwise.
     */
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
