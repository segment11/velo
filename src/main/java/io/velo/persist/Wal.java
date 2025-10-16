package io.velo.persist;

import io.velo.*;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XWalRewrite;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static io.velo.CompressedValue.NO_EXPIRE;

/**
 * Write-Ahead Log (WAL) implementation for persistent storage of operation logs.
 * WAL provides in-memory estimation, delayed writes, batch reads, and clearing functionalities.
 * <p>
 * WAL supports separate storage for short and long values and can operate in pure memory mode.
 */
public class Wal implements InMemoryEstimate {
    /**
     * Represents a single log entry in the WAL.
     * Each entry contains metadata about the key-value pair being logged, including sequence number,
     * bucket index, key hash, expiration time, type, key, encoded value, and whether it's from a merge operation.
     */
    public record V(long seq, int bucketIndex, long keyHash, long expireAt, int spType,
                    String key, byte[] cvEncoded, boolean isFromMerge) implements Comparable<V> {
        /**
         * Checks if this log entry represents a deletion.
         *
         * @return true if this entry is a deletion, false otherwise.
         */
        boolean isRemove() {
            return CompressedValue.isDeleted(cvEncoded);
        }

        /**
         * Checks if this log entry has expired.
         *
         * @return true if this entry has expired, false otherwise.
         */
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }

        /**
         * Returns a string representation of this log entry.
         *
         * @return a string representation of this log entry.
         */
        @Override
        public @NotNull String toString() {
            return "V{" +
                    ", seq=" + seq +
                    ", bucketIndex=" + bucketIndex +
                    ", key='" + key + '\'' +
                    ", expireAt=" + expireAt +
                    ", cvEncoded.length=" + cvEncoded.length +
                    '}';
        }

        /**
         * Calculates the length required to persist this log entry.
         *
         * @return the length required to persist this log entry.
         */
        public int persistLength() {
            // include key
            return CompressedValue.KEY_HEADER_LENGTH + key.length() + cvEncoded.length;
        }

        /**
         * Calculates the length required to persist a log entry with given key and value lengths.
         *
         * @param keyLength       the length of the key.
         * @param cvEncodedLength the length of the encoded value.
         * @return the length required to persist the log entry.
         */
        public static int persistLength(int keyLength, int cvEncodedLength) {
            // include key
            return CompressedValue.KEY_HEADER_LENGTH + keyLength + cvEncodedLength;
        }

        /**
         * The length of the encoded header for this log entry.
         * seq long + bucketIndex int + keyHash long + expireAt long + spType int + keyLength short + key bytes + cvEncodedLength int + cvEncoded bytes
         */
        private static final int ENCODED_HEADER_LENGTH = 8 + 4 + 8 + 8 + 4 + 2 + 4;

        /**
         * Calculates the total encoded length of this log entry.
         *
         * @return the total encoded length of this log entry.
         */
        public int encodeLength() {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            return 4 + vLength;
        }

        /**
         * Encodes this log entry into a byte array.
         *
         * @param isEndAppend0ForBreak whether to add 0 int to the end, for read break.
         * @return the byte array representation of this log entry.
         */
        public byte[] encode(boolean isEndAppend0ForBreak) {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            // 4 -> vLength
            var bytes = new byte[4 + vLength + (isEndAppend0ForBreak ? 4 : 0)];
            var buffer = ByteBuffer.wrap(bytes);

            buffer.putInt(vLength);
            buffer.putLong(seq);
            buffer.putInt(bucketIndex);
            buffer.putLong(keyHash);
            buffer.putLong(expireAt);
            buffer.putInt(spType);
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.putInt(cvEncoded.length);
            buffer.put(cvEncoded);
            if (isEndAppend0ForBreak) {
                buffer.putInt(0);
            }
            return bytes;
        }

        /**
         * Decodes a log entry from a DataInputStream.
         *
         * @param is the DataInputStream to read from.
         * @return the decoded log entry.
         * @throws IOException if an I/O error occurs.
         */
        public static V decode(DataInputStream is) throws IOException {
            if (is.available() < 4) {
                return null;
            }

            var vLength = is.readInt();
            if (vLength == 0) {
                return null;
            }

            var seq = is.readLong();
            var bucketIndex = is.readInt();
            var keyHash = is.readLong();
            var expireAt = is.readLong();
            var spType = is.readInt();
            var keyLength = is.readShort();

            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }

            var keyBytes = new byte[keyLength];
            var n = is.read(keyBytes);
            if (n != keyLength) {
                throw new IllegalStateException("Read key bytes error, key length=" + keyLength + ", read length=" + n);
            }
            var cvEncodedLength = is.readInt();
            var cvEncoded = new byte[cvEncodedLength];
            var n2 = is.read(cvEncoded);
            if (n2 != cvEncodedLength) {
                throw new IllegalStateException("Read cv encoded bytes error, cv encoded length=" + cvEncodedLength + ", read length=" + n2);
            }

            if (vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength) {
                throw new IllegalStateException("Invalid length=" + vLength);
            }

            return new V(seq, bucketIndex, keyHash, expireAt, spType, new String(keyBytes), cvEncoded, false);
        }

        /**
         * Compares this log entry with another based on their sequence numbers.
         *
         * @param o the other log entry to compare with.
         * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
         */
        @Override
        public int compareTo(@NotNull Wal.V o) {
            return Long.compare(this.seq, o.seq);
        }
    }

    /**
     * Represents the result of a put operation in the WAL.
     */
    public record PutResult(boolean needPersist, boolean isValueShort, @Nullable V needPutV, int offset) {
    }

    long initMemoryN = 0;

    /**
     * Constructs a new WAL instance.
     *
     * @param slot                    the slot identifier.
     * @param groupIndex              the group index.
     * @param walSharedFile           the shared file for long values.
     * @param walSharedFileShortValue the shared file for short values.
     * @param snowFlake               the SnowFlake instance for generating unique IDs.
     * @throws IOException if an I/O error occurs.
     */
    public Wal(short slot, OneSlot oneSlot, int groupIndex,
               @NullableOnlyTest RandomAccessFile walSharedFile,
               @NullableOnlyTest RandomAccessFile walSharedFileShortValue,
               @NotNull SnowFlake snowFlake) throws IOException {
        this.slot = slot;
        this.oneSlot = oneSlot;
        this.groupIndex = groupIndex;
        this.walSharedFile = walSharedFile;
        this.walSharedFileShortValue = walSharedFileShortValue;
        this.snowFlake = snowFlake;

        this.delayToKeyBucketValues = new HashMap<>();
        this.delayToKeyBucketShortValues = new HashMap<>();

        this.fileToWriteIndex = (long) ONE_GROUP_BUFFER_SIZE * (groupIndex + 1);
    }

    /**
     * Lazily reads data from the WAL file into memory.
     *
     * @throws IOException if an I/O error occurs.
     */
    void lazyReadFromFile() throws IOException {
        if (ConfForGlobal.pureMemory) {
            if (slot == 0 && groupIndex == 0) {
                log.info("Pure memory mode, skip read wal file, slot={}, group index={}", slot, groupIndex);
            }
            return;
        }

        var n1 = readWal(walSharedFile, delayToKeyBucketValues, false);
        var n2 = readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true);
        initMemoryN += RamUsageEstimator.sizeOfMap(delayToKeyBucketValues);
        initMemoryN += RamUsageEstimator.sizeOfMap(delayToKeyBucketShortValues);

        // reduce log
        if (slot == 0 && groupIndex == 0) {
            log.info("Read wal file success, slot={}, group index={}, value size={}, short value size={}, init memory n={}KB",
                    slot, groupIndex, n1, n2, initMemoryN / 1024);
        }
    }

    /**
     * Estimates the in-memory usage of this WAL instance.
     *
     * @param sb the StringBuilder to append the estimate details.
     * @return the estimated memory usage in bytes.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        // skip primitive fields
        long size1 = RamUsageEstimator.sizeOfMap(delayToKeyBucketValues);
        long size2 = RamUsageEstimator.sizeOfMap(delayToKeyBucketShortValues);
        if (groupIndex % 4096 == 0) {
            sb.append("Wal group index: ").append(groupIndex).append(", delay to save values: ").append(size1).append("\n");
            sb.append("Wal group index: ").append(groupIndex).append(", delay to save short values: ").append(size2).append("\n");
        }
        return size1 + size2;
    }

    /**
     * Returns a string representation of this WAL instance.
     *
     * @return a string representation of this WAL instance.
     */
    @Override
    public String toString() {
        return "Wal{" +
                "slot=" + slot +
                ", groupIndex=" + groupIndex +
                ", value.size=" + delayToKeyBucketValues.size() +
                ", shortValue.size=" + delayToKeyBucketShortValues.size() +
                '}';
    }

    /**
     * The buffer size for each WAL group, set to 64K in the shared file.
     * Chunk batch write 4 segments, each segment has 3 or 4 sub blocks, each blocks may contain 10-40 keys, 64K is enough for 1 batch about 100-200 keys.
     * The smaller, latency will be better, MAX_WAL_GROUP_NUMBER will be larger, wal memory will be larger
     */
    public static int ONE_GROUP_BUFFER_SIZE = LocalPersist.PAGE_SIZE * 16;
    /**
     * A readonly byte array representing empty bytes for one WAL group.
     */
    public static byte[] EMPTY_BYTES_FOR_ONE_GROUP = new byte[ONE_GROUP_BUFFER_SIZE];

    /**
     * Valid configurations for the number of buckets per charge.
     * For latency, do not configure too large.
     */
    public static final List<Integer> VALID_ONE_CHARGE_BUCKET_NUMBER_LIST = List.of(16, 32, 64);
    // 1 file max 2GB, 2GB / 64K = 32K wal groups
//    public static final int MAX_WAL_GROUP_NUMBER = (int) (2L * 1024 * 1024 * 1024 / (LocalPersist.PAGE_SIZE * VALID_ONE_CHARGE_BUCKET_NUMBER_LIST.getFirst()));

    private static final Logger log = LoggerFactory.getLogger(Wal.class);

    // current wal group write position in target group of wal file
    @VisibleForTesting
    int writePosition;
    @VisibleForTesting
    int writePositionShortValue;

    @TestOnly
    public int getWritePosition() {
        return writePosition;
    }

    @TestOnly
    public int getWritePositionShortValue() {
        return writePositionShortValue;
    }

    private final short slot;
    private final OneSlot oneSlot;
    final int groupIndex;

    /**
     * Calculates the WAL group index for a given bucket index.
     *
     * @param bucketIndex the bucket index.
     * @return the WAL group index.
     */
    public static int calcWalGroupIndex(int bucketIndex) {
        return bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }


    /**
     * Calculates the total number of WAL groups.
     *
     * @return the total number of WAL groups.
     */
    public static int calcWalGroupNumber() {
        return ConfForSlot.global.confBucket.bucketsPerSlot / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }

    private final RandomAccessFile walSharedFile;
    private final RandomAccessFile walSharedFileShortValue;
    private final SnowFlake snowFlake;

    @SlaveNeedReplay
    HashMap<String, V> delayToKeyBucketValues;
    @SlaveNeedReplay
    HashMap<String, V> delayToKeyBucketShortValues;

    final long fileToWriteIndex;

    /**
     * Gets the last sequence number after a put operation.
     * For replication, means slave already fetch done of this WAL group.
     *
     * @return the last sequence number after a put operation.
     */
    public long getLastSeqAfterPut() {
        return lastSeqAfterPut;
    }

    /**
     * Gets the last sequence number after a put operation for short values.
     * For replication, means slave already fetch done of this WAL group.
     *
     * @return the last sequence number after a put operation for short values.
     */
    public long getLastSeqShortValueAfterPut() {
        return lastSeqShortValueAfterPut;
    }

    @SlaveNeedReplay
    private long lastSeqAfterPut;
    @SlaveNeedReplay
    private long lastSeqShortValueAfterPut;

    /**
     * Gets the number of keys in the WAL.
     *
     * @return the number of keys in the WAL.
     */
    public int getKeyCount() {
        return delayToKeyBucketValues.size() + delayToKeyBucketShortValues.size();
    }


    /**
     * Gets the keys in the WAL. For scan skip when scan key buckets.
     *
     * @return the keys in the WAL.
     */
    HashSet<String> inWalKeys() {
        HashSet<String> set = new HashSet<>();
        set.addAll(delayToKeyBucketValues.keySet());
        set.addAll(delayToKeyBucketShortValues.keySet());
        return set;
    }

    /**
     * Scans the WAL for keys matching the specified criteria.
     *
     * @param skipCount    the number of keys to skip.
     * @param typeAsByte   value type, @see KeyLoader.typeAsByteIgnore
     * @param matchPattern the pattern to match against keys.
     * @param count        the maximum number of keys to return.
     * @param beginScanSeq the sequence number set when begin scanning from, return key value need >= this sequence number.
     * @return a KeyLoader.ScanCursorWithReturnKeys instance containing the scan results.
     */
    public KeyLoader.ScanCursorWithReturnKeys scan(final short skipCount,
                                                   final byte typeAsByte,
                                                   final @Nullable String matchPattern,
                                                   final int count,
                                                   final long beginScanSeq) {
        if (delayToKeyBucketValues.isEmpty() && delayToKeyBucketShortValues.isEmpty()) {
            return null;
        }

        ArrayList<String> keys = new ArrayList<>(count);

        short tmpSkipCount = skipCount;
        short removedOrExpiredOrNotMatchedCount = 0;

        var allValues = new HashMap<String, V>();
        allValues.putAll(delayToKeyBucketShortValues);
        allValues.putAll(delayToKeyBucketValues);
        for (var entry : allValues.entrySet()) {
            if (tmpSkipCount > 0) {
                tmpSkipCount--;
                continue;
            }

            var key = entry.getKey();
            var v = entry.getValue();

            if (v.isRemove() || v.isExpired()) {
                removedOrExpiredOrNotMatchedCount++;
                continue;
            }

            if (!KeyLoader.isKeyMatch(key, matchPattern)) {
                removedOrExpiredOrNotMatchedCount++;
                continue;
            }

            if (typeAsByte != KeyLoader.typeAsByteIgnore) {
                var spType = CompressedValue.onlyReadSpType(v.cvEncoded);
                var shortType = KeyLoader.transferToShortType(spType);
                if (shortType != typeAsByte) {
                    removedOrExpiredOrNotMatchedCount++;
                    continue;
                }
            }

            // skip data that is new added after time do scan
            if (v.seq > beginScanSeq) {
                continue;
            }

            keys.add(key);
            if (keys.size() >= count) {
                break;
            }
        }

        if (keys.isEmpty()) {
            return null;
        }

        var nextTimeSkipCount = skipCount + removedOrExpiredOrNotMatchedCount + keys.size();
        return new KeyLoader.ScanCursorWithReturnKeys(new ScanCursor(slot, groupIndex, (short) nextTimeSkipCount, (short) 0, (byte) 0), keys);
    }

    @VisibleForTesting
    int readWal(@NullableOnlyTest RandomAccessFile fromWalFile, @NotNull HashMap<String, V> toMap, boolean isShortValue) throws IOException {
        // for unit test
        if (fromWalFile == null) {
            return 0;
        }

        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        var readBytes = new byte[ONE_GROUP_BUFFER_SIZE];
        fromWalFile.seek(targetGroupBeginOffset);
        int readN = fromWalFile.read(readBytes);
        if (readN == -1) {
            return 0;
        }

        return readBytesToList(toMap, isShortValue, readBytes, 0, ONE_GROUP_BUFFER_SIZE);
    }

    int readFromSavedBytes(byte[] readBytes, boolean isShortValue) throws IOException {
        return readBytesToList(isShortValue ? delayToKeyBucketShortValues : delayToKeyBucketValues, isShortValue, readBytes, 0, readBytes.length);
    }

    byte[] writeToSavedBytes(boolean isShortValue, boolean isEndAppend0ForBreak) throws IOException {
        var map = isShortValue ? delayToKeyBucketShortValues : delayToKeyBucketValues;

        var bos = new ByteArrayOutputStream();
        for (var entry : map.entrySet()) {
            var encoded = entry.getValue().encode(false);
            bos.write(encoded);
        }

        // for read break
        if (isEndAppend0ForBreak) {
            bos.write(new byte[4]);
        }

        return bos.toByteArray();
    }

    private int readBytesToList(@NotNull HashMap<String, V> toMap, boolean isShortValue, byte[] readBytes, int offset, int length) throws IOException {
        int n = 0;
        int position = 0;
        long lastSeq = 0;
        var is = new DataInputStream(new ByteArrayInputStream(readBytes, offset, length));
        while (true) {
            var v = V.decode(is);
            if (v == null) {
                break;
            }

            toMap.put(v.key, v);
            lastSeq = v.seq;
            position += v.encodeLength();
            n++;
        }

        if (isShortValue) {
            writePositionShortValue = position;
            lastSeqShortValueAfterPut = lastSeq;
        } else {
            writePosition = position;
            lastSeqAfterPut = lastSeq;
        }
        return n;
    }

    private void resetWal(boolean isShortValue) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        if (!ConfForGlobal.pureMemory) {
            var raf = isShortValue ? walSharedFileShortValue : walSharedFile;
            try {
                raf.seek(targetGroupBeginOffset);
                raf.write(EMPTY_BYTES_FOR_ONE_GROUP);
            } catch (IOException e) {
                log.error("Truncate wal group error", e);
            }
        }

        // reset write position
        if (isShortValue) {
            writePositionShortValue = 0;
            lastSeqShortValueAfterPut = 0L;
        } else {
            writePosition = 0;
            lastSeqAfterPut = 0L;
        }
    }

    /**
     * Clears all values include short values of this WAL group. Only used for test.
     */
    @TestOnly
    public void clear() {
        clear(true);
    }

    /**
     * Clears all values include short values of this WAL group.
     *
     * @param writeBytes0ToRaf whether to write empty bytes to file. Skip this when pure memory mode.
     */
    @SlaveNeedReplay
    @SlaveReplay
    void clear(boolean writeBytes0ToRaf) {
        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();

        if (writeBytes0ToRaf) {
            resetWal(false);
            resetWal(true);
        }

        if (groupIndex % 100 == 0) {
            log.info("Clear wal, slot={}, group index={}", slot, groupIndex);
        }
    }

    @VisibleForTesting
    long clearShortValuesCount = 0;
    @VisibleForTesting
    long clearValuesCount = 0;

    /**
     * Clears all short values of this WAL group.
     */
    @SlaveNeedReplay
    public void clearShortValues() {
        delayToKeyBucketShortValues.clear();
        resetWal(true);

        clearShortValuesCount++;
        if (clearShortValuesCount % 1000 == 0) {
            log.info("Clear short values, slot={}, group index={}, count={}", slot, groupIndex, clearShortValuesCount);
        }
    }

    /**
     * Clears all values of this WAL group.
     */
    @SlaveNeedReplay
    public void clearValues() {
        delayToKeyBucketValues.clear();
        resetWal(false);

        clearValuesCount++;
        if (clearValuesCount % 1000 == 0) {
            log.info("Clear values, slot={}, group index={}, count={}", slot, groupIndex, clearValuesCount);
        }
    }

    /**
     * Retrieves the encoded value for a given key.
     *
     * @param key the key to retrieve.
     * @return the encoded value, or null if not found.
     */
    byte[] get(@NotNull String key) {
        var vShort = delayToKeyBucketShortValues.get(key);
        var v = delayToKeyBucketValues.get(key);
        if (vShort == null && v == null) {
            return null;
        }

        if (vShort != null) {
            if (v == null) {
                return vShort.cvEncoded;
            } else {
                if (vShort.seq > v.seq) {
                    return vShort.cvEncoded;
                } else {
                    return v.cvEncoded;
                }
            }
        }

        return v.cvEncoded;
    }

    /**
     * Removes a key-value pair from the WAL with a delay.
     *
     * @param key               the key to remove.
     * @param bucketIndex       the bucket index.
     * @param keyHash           the hash of the key.
     * @param lastPersistTimeMs the last persist time in milliseconds.
     * @return the result of the removal operation.
     */
    PutResult removeDelay(@NotNull String key, int bucketIndex, long keyHash, long lastPersistTimeMs) {
        byte[] encoded = {CompressedValue.SP_FLAG_DELETE_TMP};
        var v = new V(snowFlake.nextId(), bucketIndex, keyHash, CompressedValue.EXPIRE_NOW,
                CompressedValue.NULL_DICT_SEQ, key, encoded, false);

        return put(true, key, v, lastPersistTimeMs);
    }

    /**
     * Checks if a key exists in the WAL.
     *
     * @param key the key to check.
     * @return true if the key exists and is not removed, false otherwise.
     */
    @VisibleForTesting
    boolean exists(@NotNull String key) {
        var vShort = delayToKeyBucketShortValues.get(key);
        if (vShort != null) {
            // already removed
            return !vShort.isRemove();
        } else {
            return delayToKeyBucketValues.get(key) != null;
        }
    }

    // stats
    long needPersistCountTotal = 0;
    long needPersistKvCountTotal = 0;
    long needPersistOffsetTotal = 0;

    /**
     * Puts a log entry into the WAL from a replication source.
     * Slave catch up master binlog, replay wal, need update write position, be careful.
     *
     * @param v            the log entry to put.
     * @param isValueShort true if the value is short, false otherwise.
     * @param offset       the offset to write at.
     */
    @SlaveReplay
    public void putFromX(@NotNull V v, boolean isValueShort, int offset) {
        if (!ConfForGlobal.pureMemory) {
            var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
            putVToFile(v, isValueShort, offset, targetGroupBeginOffset);

            // update write position
            var encodeLength = v.encodeLength();
            if (isValueShort) {
                writePositionShortValue = offset + encodeLength;
            } else {
                writePosition = offset + encodeLength;
            }
        }

        if (isValueShort) {
            delayToKeyBucketShortValues.put(v.key, v);
            delayToKeyBucketValues.remove(v.key);

            lastSeqShortValueAfterPut = v.seq;
        } else {
            delayToKeyBucketValues.put(v.key, v);
            delayToKeyBucketShortValues.remove(v.key);

            lastSeqAfterPut = v.seq;
        }
    }

    private void putVToFile(@NotNull V v, boolean isValueShort, int offset, int targetGroupBeginOffset) {
        var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
        try {
            raf.seek(targetGroupBeginOffset + offset);
            if (offset + v.encodeLength() < ONE_GROUP_BUFFER_SIZE - 4) {
                raf.write(v.encode(true));
            } else {
                raf.write(v.encode(false));
            }
        } catch (IOException e) {
            log.error("Write to file error", e);
            throw new RuntimeException("Write to file error=" + e.getMessage());
        }
    }

    @TestOnly
    public PutResult put(boolean isValueShort, @NotNull String key, @NotNull V v) {
        return put(isValueShort, key, v, 0L);
    }

    /**
     * Rewrite to file for one group
     *
     * @param isValueShort true if the value is short, false otherwise.
     * @return new offset
     */
    @SlaveNeedReplay
    private int rewriteOneGroup(boolean isValueShort, byte[] writeBytesFromMasterIfIsReplay) {
        try {
            var writeBytes = writeBytesFromMasterIfIsReplay != null ? writeBytesFromMasterIfIsReplay : writeToSavedBytes(isValueShort, true);
            assert writeBytes.length > 4 && writeBytes.length <= ONE_GROUP_BUFFER_SIZE;

            // 4 bytes for int 0
            int newOffset = writeBytes.length - 4;
            var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
            if (isValueShort) {
                walSharedFileShortValue.seek(targetGroupBeginOffset);
                walSharedFileShortValue.write(writeBytes);
                writePositionShortValue = newOffset;
            } else {
                walSharedFile.seek(targetGroupBeginOffset);
                walSharedFile.write(writeBytes);
                writePosition = newOffset;
            }

            if (writeBytesFromMasterIfIsReplay == null) {
                var xWalRewrite = new XWalRewrite(isValueShort, groupIndex, writeBytes);
                oneSlot.appendBinlog(xWalRewrite);
            }

            return newOffset;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SlaveReplay
    public void rewriteOneGroupFromMaster(boolean isValueShort, byte[] bytes) {
        rewriteOneGroup(isValueShort, bytes);
    }

    @VisibleForTesting
    boolean isOnRewrite = true;

    private final Debug debug = Debug.getInstance();

    /**
     * Resets the write position after bulk loading.
     */
    void resetWritePositionAfterBulkLoad() {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        resetWal(false);
        if (!delayToKeyBucketValues.isEmpty()) {
            int offset = 0;
            for (var entry : delayToKeyBucketValues.entrySet()) {
                var v = entry.getValue();
                putVToFile(v, false, offset, targetGroupBeginOffset);
                var encodeLength = v.encodeLength();
                offset += encodeLength;
                assert offset < ONE_GROUP_BUFFER_SIZE;

                lastSeqAfterPut = v.seq;
            }
            writePosition = offset;
        }

        resetWal(true);
        if (!delayToKeyBucketShortValues.isEmpty()) {
            int offset = 0;
            for (var entry : delayToKeyBucketShortValues.entrySet()) {
                var v = entry.getValue();
                putVToFile(v, true, offset, targetGroupBeginOffset);
                var encodeLength = v.encodeLength();
                offset += encodeLength;
                assert offset < ONE_GROUP_BUFFER_SIZE;

                lastSeqShortValueAfterPut = v.seq;
            }
            writePositionShortValue = offset;
        }
    }

    /**
     * Puts a log entry into the WAL.
     *
     * @param isValueShort      true if the value is short, false otherwise.
     * @param key               the key to put.
     * @param v                 the log entry to put.
     * @param lastPersistTimeMs the last persist time in milliseconds.
     * @return the result of the put operation.
     */
    public PutResult put(boolean isValueShort, @NotNull String key, @NotNull V v, long lastPersistTimeMs) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
        var offset = isValueShort ? writePositionShortValue : writePosition;

        var encodeLength = v.encodeLength();
        if (offset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
            boolean needPersist = true;
            var keyCount = isValueShort ? delayToKeyBucketShortValues.size() : delayToKeyBucketValues.size();
            // hot data, keep in wal
            if (keyCount < (isValueShort ? ConfForSlot.global.confWal.shortValueSizeTrigger / 10 : ConfForSlot.global.confWal.valueSizeTrigger / 10) && isOnRewrite) {
                var newOffset = rewriteOneGroup(isValueShort, null);
                if (newOffset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
                    needPersistCountTotal++;
                    needPersistKvCountTotal += keyCount;
                    needPersistOffsetTotal += offset;

                    return new PutResult(true, isValueShort, v, 0);
                }

                needPersist = false;
            }

            if (needPersist) {
                needPersistCountTotal++;
                needPersistKvCountTotal += keyCount;
                needPersistOffsetTotal += offset;

                return new PutResult(true, isValueShort, v, 0);
            }
        }

        // bulk load need not wal write
        if (!ConfForGlobal.pureMemory && !debug.bulkLoad) {
            putVToFile(v, isValueShort, offset, targetGroupBeginOffset);
        }
        if (isValueShort) {
            writePositionShortValue += encodeLength;
        } else {
            writePosition += encodeLength;
        }

        // 2 ms at least do persist once, suppose once batch 150 key values, 1 thread 1s may persist 1000 / 2 * 150 = 75000 key values
        final int atLeastDoPersistOnceIntervalMs = ConfForSlot.global.confWal.atLeastDoPersistOnceIntervalMs;
        final double checkAtLeastDoPersistOnceSizeRate = ConfForSlot.global.confWal.checkAtLeastDoPersistOnceSizeRate;
        if (isValueShort) {
            delayToKeyBucketShortValues.put(key, v);
            delayToKeyBucketValues.remove(key);

            lastSeqShortValueAfterPut = v.seq;

            boolean needPersist = delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger;
            if (needPersist) {
                needPersistCountTotal++;
                needPersistKvCountTotal += delayToKeyBucketShortValues.size();
                needPersistOffsetTotal += writePositionShortValue;
            } else {
                if (delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger * checkAtLeastDoPersistOnceSizeRate) {
                    if (System.currentTimeMillis() - lastPersistTimeMs > atLeastDoPersistOnceIntervalMs) {
                        needPersist = true;
                        needPersistCountTotal++;
                        needPersistKvCountTotal += delayToKeyBucketShortValues.size();
                        needPersistOffsetTotal += writePositionShortValue;
                    }
                }
            }
            return new PutResult(needPersist, true, null, needPersist ? 0 : offset);
        }

        delayToKeyBucketValues.put(key, v);
        delayToKeyBucketShortValues.remove(key);

        lastSeqAfterPut = v.seq;

        boolean needPersist = delayToKeyBucketValues.size() >= ConfForSlot.global.confWal.valueSizeTrigger;
        if (needPersist) {
            needPersistCountTotal++;
            needPersistKvCountTotal += delayToKeyBucketValues.size();
            needPersistOffsetTotal += writePosition;
        } else {
            if (delayToKeyBucketValues.size() >= ConfForSlot.global.confWal.valueSizeTrigger * checkAtLeastDoPersistOnceSizeRate) {
                if (System.currentTimeMillis() - lastPersistTimeMs > atLeastDoPersistOnceIntervalMs) {
                    needPersist = true;
                    needPersistCountTotal++;
                    needPersistKvCountTotal += delayToKeyBucketValues.size();
                    needPersistOffsetTotal += writePosition;
                }
            }
        }
        return new PutResult(needPersist, false, null, needPersist ? 0 : offset);
    }

    /**
     * Encode to bytes for slave replay.
     *
     * @return the byte array representation of this WAL group.
     * @throws IOException if an I/O error occurs.
     */
    @SlaveReplay
    public byte[] toSlaveExistsOneWalGroupBytes() throws IOException {
        // encoded length
        // 4 bytes for group index
        // 4 bytes for one group buffer size
        // 4 bytes for value write position and short value write position
        // 8 bytes for last seq and 8 bytes short value last seq
        // value encoded + short value encoded
        int realDataOffset = 4 + 4 + (4 + 4) + (8 + 8);
        int n = realDataOffset + ONE_GROUP_BUFFER_SIZE * 2;

        var bytes = new byte[n];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(groupIndex);
        buffer.putInt(ONE_GROUP_BUFFER_SIZE);
        buffer.putInt(writePosition);
        buffer.putInt(writePositionShortValue);
        buffer.putLong(lastSeqAfterPut);
        buffer.putLong(lastSeqShortValueAfterPut);

        if (ConfForGlobal.pureMemory) {
            for (var entry : delayToKeyBucketValues.entrySet()) {
                var v = entry.getValue();
                var encodedBytes = v.encode(false);
                buffer.put(encodedBytes);
            }

            buffer.position(realDataOffset + ONE_GROUP_BUFFER_SIZE);
            for (var entry : delayToKeyBucketShortValues.entrySet()) {
                var v = entry.getValue();
                var encodedBytes = v.encode(false);
                buffer.put(encodedBytes);
            }

            return bytes;
        }

        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        walSharedFile.seek(targetGroupBeginOffset);
        walSharedFile.read(bytes, realDataOffset, ONE_GROUP_BUFFER_SIZE);

        walSharedFileShortValue.seek(targetGroupBeginOffset);
        walSharedFileShortValue.read(bytes, realDataOffset + ONE_GROUP_BUFFER_SIZE, ONE_GROUP_BUFFER_SIZE);

        return bytes;
    }

    /**
     * Decodes a WAL group from a byte array for slave replay. From master to slave.
     *
     * @param bytes the byte array to decode.
     * @throws IOException if an I/O error occurs.
     */
    @SlaveReplay
    public void fromMasterExistsOneWalGroupBytes(byte[] bytes) throws IOException {
        var buffer = ByteBuffer.wrap(bytes);
        var groupIndex1 = buffer.getInt();
        var oneGroupBufferSize = buffer.getInt();

        if (groupIndex1 != groupIndex) {
            throw new IllegalStateException("Repl slave fetch wal group error, group index=" +
                    groupIndex1 + ", expect group index=" + groupIndex + ", slot=" + slot);
        }
        if (oneGroupBufferSize != ONE_GROUP_BUFFER_SIZE) {
            throw new IllegalStateException("Repl slave fetch wal group error, group index=" +
                    groupIndex1 + ", one group buffer size=" + oneGroupBufferSize + ", expect size=" + ONE_GROUP_BUFFER_SIZE + ", slot=" + slot);
        }

        writePosition = buffer.getInt();
        writePositionShortValue = buffer.getInt();

        lastSeqAfterPut = buffer.getLong();
        lastSeqShortValueAfterPut = buffer.getLong();

        int realDataOffset = 4 + 4 + (4 + 4) + (8 + 8);

        if (!ConfForGlobal.pureMemory) {
            var targetGroupBeginOffset = oneGroupBufferSize * groupIndex1;

            walSharedFile.seek(targetGroupBeginOffset);
            walSharedFile.write(bytes, realDataOffset, oneGroupBufferSize);

            walSharedFileShortValue.seek(targetGroupBeginOffset);
            walSharedFileShortValue.write(bytes, realDataOffset + oneGroupBufferSize, oneGroupBufferSize);
        }

        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();
        var n1 = readBytesToList(delayToKeyBucketValues, false, bytes, realDataOffset, oneGroupBufferSize);
        var n2 = readBytesToList(delayToKeyBucketShortValues, true, bytes, realDataOffset + oneGroupBufferSize, oneGroupBufferSize);
        if (groupIndex1 % 100 == 0) {
            log.warn("Repl slave fetch wal group success, group index={}, value size={}, short value size={}, slot={}",
                    groupIndex1, n1, n2, slot);
        }
    }
}
