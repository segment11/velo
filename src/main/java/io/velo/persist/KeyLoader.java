package io.velo.persist;

import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import io.velo.task.ITask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * KeyLoader is responsible for managing the key-value storage in a specific slot.
 * It handles reading, writing, and managing the lifecycle of key buckets, which are the basic units of storage.
 * KeyLoader uses a split file system to handle large amounts of data efficiently, with each split file corresponding to a specific split index.
 * It also supports replaying operations from a slave node and maintaining consistent state across nodes in a distributed environment.
 */
public class KeyLoader implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, CanSaveAndLoad, ITask {
    /**
     * The number of pages per bucket. Each bucket is composed of this number of pages.
     */
    private static final int PAGE_NUMBER_PER_BUCKET = 1;

    /**
     * The cost size of a single key bucket, calculated as the number of pages per bucket times the page size.
     */
    public static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * LocalPersist.PAGE_SIZE;

    /**
     * Constructor for KeyLoader used in unit testing.
     *
     * @param slot           The slot number.
     * @param bucketsPerSlot The number of buckets in the slot.
     * @param slotDir        The directory of the slot.
     * @param snowFlake      The Snowflake instance used for generating unique identifiers.
     */
    @TestOnly
    KeyLoader(short slot, int bucketsPerSlot, @NotNull File slotDir, @NotNull SnowFlake snowFlake) {
        this(slot, bucketsPerSlot, slotDir, snowFlake, null);
    }

    /**
     * Constructor for KeyLoader.
     *
     * @param slot           The slot number.
     * @param bucketsPerSlot The number of buckets in the slot.
     * @param slotDir        The directory of the slot.
     * @param snowFlake      The Snowflake instance used for generating unique identifiers.
     * @param oneSlot        The OneSlot instance representing this slot.
     */
    public KeyLoader(short slot, int bucketsPerSlot, @NotNull File slotDir, @NotNull SnowFlake snowFlake, @Nullable OneSlot oneSlot) {
        this.slot = slot;
        this.bucketsPerSlot = bucketsPerSlot;
        this.slotDir = slotDir;
        this.snowFlake = snowFlake;

        this.oneSlot = oneSlot;
        this.cvExpiredOrDeletedCallBack = new KeyBucket.CvExpiredOrDeletedCallBack() {
            @Override
            public void handle(@NotNull String key, @NotNull CompressedValue shortStringCv) {
                // for unit test
                if (oneSlot == null) {
                    log.warn("Short value cv expired, type={}, slot={}", shortStringCv.getDictSeqOrSpType(), slot);
                    return;
                }
                oneSlot.handleWhenCvExpiredOrDeleted(key, shortStringCv, null);
            }

            @Override
            public void handle(@NotNull String key, @NotNull PersistValueMeta pvm) {
                // for unit test
                if (oneSlot == null) {
                    log.warn("Cv expired, pvm={}, slot={}", pvm, slot);
                    return;
                }
                oneSlot.handleWhenCvExpiredOrDeleted(key, null, pvm);
            }
        };

        if (ConfForGlobal.pureMemoryV2) {
            this.allKeyHashBuckets = new AllKeyHashBuckets(bucketsPerSlot, oneSlot);
            this.metaChunkSegmentFillRatio = new MetaChunkSegmentFillRatio();
        }
    }

    @TestOnly
    public void resetForPureMemoryV2() {
        if (this.allKeyHashBuckets != null) {
            return;
        }
        this.allKeyHashBuckets = new AllKeyHashBuckets(bucketsPerSlot, oneSlot);
        this.metaChunkSegmentFillRatio = new MetaChunkSegmentFillRatio();
    }

    /**
     * Returns a string representation of the KeyLoader.
     *
     * @return a string representation of the KeyLoader.
     */
    @Override
    public String toString() {
        return "KeyLoader{" +
                "slot=" + slot +
                ", bucketsPerSlot=" + bucketsPerSlot +
                '}';
    }

    /**
     * Estimates the memory usage of the KeyLoader and appends the details to the provided string builder.
     *
     * @param sb the string builder to append the memory usage details.
     * @return the total estimated memory usage.
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        size += metaKeyBucketSplitNumber.estimate(sb);
        size += metaOneWalGroupSeq.estimate(sb);
        size += statKeyCountInBuckets.estimate(sb);

        if (ConfForGlobal.pureMemoryV2) {
            size += allKeyHashBuckets.estimate(sb);
            size += metaChunkSegmentFillRatio.estimate(sb);
        } else {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    size += fdReadWrite.estimate(sb);
                }
            }
        }
        return size;
    }

    /**
     * The slot number.
     */
    private final short slot;

    /**
     * The number of buckets in the slot.
     */
    final int bucketsPerSlot;

    /**
     * The directory of the slot.
     */
    private final File slotDir;

    /**
     * The Snowflake instance used for generating unique identifiers.
     */
    final SnowFlake snowFlake;

    /**
     * The OneSlot instance representing this slot.
     */
    private final OneSlot oneSlot;

    /**
     * The callback for handling expired or deleted compressed values.
     */
    final KeyBucket.CvExpiredOrDeletedCallBack cvExpiredOrDeletedCallBack;

    /**
     * Loads the state from the last saved file when in pure memory mode.
     *
     * @param is the data input stream to read from.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        var metaKeyBucketSplitNumberBytes = new byte[this.metaKeyBucketSplitNumber.allCapacity];
        is.readFully(metaKeyBucketSplitNumberBytes);
        this.metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(metaKeyBucketSplitNumberBytes);

        var metaOneWalGroupSeqBytes = new byte[this.metaOneWalGroupSeq.allCapacity];
        is.readFully(metaOneWalGroupSeqBytes);
        this.metaOneWalGroupSeq.overwriteInMemoryCachedBytes(metaOneWalGroupSeqBytes);

        var statKeyCountInBucketsBytes = new byte[this.statKeyCountInBuckets.allCapacity];
        is.readFully(statKeyCountInBucketsBytes);
        this.statKeyCountInBuckets.overwriteInMemoryCachedBytes(statKeyCountInBucketsBytes);

        if (ConfForGlobal.pureMemoryV2) {
            allKeyHashBuckets.loadFromLastSavedFileWhenPureMemory(is);
            metaChunkSegmentFillRatio.loadFromLastSavedFileWhenPureMemory(is);
        } else {
            // fd read write
            var fdCount = is.readInt();
            for (int i = 0; i < fdCount; i++) {
                var fdIndex = is.readInt();
                var walGroupCount = is.readInt();
                var fd = fdReadWriteArray[fdIndex];
                for (int j = 0; j < walGroupCount; j++) {
                    var walGroupIndex = is.readInt();
                    var readBytesLength = is.readInt();
                    var readBytes = new byte[readBytesLength];
                    is.readFully(readBytes);
                    fd.setSharedBytesFromLastSavedFileToMemory(readBytes, walGroupIndex);
                }
            }
        }
    }

    /**
     * Writes the state to the saved file when in pure memory mode.
     *
     * @param os the data output stream to write to.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.write(metaKeyBucketSplitNumber.getInMemoryCachedBytes());
        os.write(metaOneWalGroupSeq.getInMemoryCachedBytes());
        os.write(statKeyCountInBuckets.getInMemoryCachedBytes());

        if (ConfForGlobal.pureMemoryV2) {
            allKeyHashBuckets.writeToSavedFileWhenPureMemory(os);
            metaChunkSegmentFillRatio.writeToSavedFileWhenPureMemory(os);
        } else {
            int fdCount = 0;
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    fdCount++;
                }
            }

            os.writeInt(fdCount);
            for (int fdIndex = 0; fdIndex < fdReadWriteArray.length; fdIndex++) {
                var fdReadWrite = fdReadWriteArray[fdIndex];
                if (fdReadWrite == null) {
                    continue;
                }

                os.writeInt(fdIndex);
                int walGroupCount = 0;
                for (int walGroupIndex = 0; walGroupIndex < fdReadWrite.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex.length; walGroupIndex++) {
                    var sharedBytes = fdReadWrite.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex];
                    if (sharedBytes != null) {
                        walGroupCount++;
                    }
                }
                os.writeInt(walGroupCount);

                for (int walGroupIndex = 0; walGroupIndex < fdReadWrite.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex.length; walGroupIndex++) {
                    var sharedBytes = fdReadWrite.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex];
                    if (sharedBytes != null) {
                        os.writeInt(walGroupIndex);
                        os.writeInt(sharedBytes.length);
                        os.write(sharedBytes);
                    }
                }
            }
        }
    }

    /**
     * The meta key bucket split number manager.
     */
    @VisibleForTesting
    MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    /**
     * Retrieves a batch of meta key bucket split numbers.
     *
     * @param beginBucketIndex the starting bucket index.
     * @param bucketCount      the number of buckets in the batch.
     * @return the batch of meta key bucket split numbers.
     * @throws IllegalArgumentException if the bucket index is out of range.
     */
    byte[] getMetaKeyBucketSplitNumberBatch(int beginBucketIndex, int bucketCount) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot=" + slot + ", begin bucket index=" + beginBucketIndex);
        }
        return metaKeyBucketSplitNumber.getBatch(beginBucketIndex, bucketCount);
    }

    /**
     * Updates a batch of meta key bucket split numbers if they have changed.
     *
     * @param beginBucketIndex the starting bucket index.
     * @param splitNumberArray the new split numbers.
     * @return true if the split numbers were updated, false otherwise.
     * @throws IllegalArgumentException if the bucket index is out of range.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public boolean updateMetaKeyBucketSplitNumberBatchIfChanged(int beginBucketIndex, byte[] splitNumberArray) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot=" + slot + ", begin bucket index=" + beginBucketIndex);
        }

        // if not change, need not an extra ssd io
        // even though random access file use os page cache
        var currentBytes = metaKeyBucketSplitNumber.getBatch(beginBucketIndex, splitNumberArray.length);
        if (Arrays.equals(currentBytes, splitNumberArray)) {
            return false;
        }

        metaKeyBucketSplitNumber.setBatch(beginBucketIndex, splitNumberArray);
        return true;
    }

    /**
     * Returns the maximum split number used for replay.
     *
     * @return the maximum split number.
     */
    public byte maxSplitNumberForRepl() {
        return metaKeyBucketSplitNumber.maxSplitNumber();
    }

    /**
     * Retrieves the bytes representing the meta key bucket split numbers for slave nodes.
     * Read only.
     *
     * @return the bytes representing the meta key bucket split numbers.
     */
    @SlaveReplay
    public byte[] getMetaKeyBucketSplitNumberBytesToSlaveExists() {
        return metaKeyBucketSplitNumber.getInMemoryCachedBytes();
    }

    /**
     * Overwrites the meta key bucket split number bytes from the master node for slave nodes.
     *
     * @param bytes the meta key bucket split number bytes from the master node.
     */
    @SlaveReplay
    public void overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(byte[] bytes) {
        metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(bytes);
        log.warn("Repl overwrite meta key bucket split number bytes from master exists, slot={}", slot);
    }

    /**
     * Sets the meta key bucket split number for a specific bucket index.
     *
     * @param bucketIndex the bucket index.
     * @param splitNumber the split number.
     * @throws IllegalArgumentException if the bucket index is out of range.
     */
    @TestOnly
    void setMetaKeyBucketSplitNumber(int bucketIndex, byte splitNumber) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot=" + slot + ", begin bucket index=" + bucketIndex);
        }
        metaKeyBucketSplitNumber.set(bucketIndex, splitNumber);
    }

    /**
     * The meta one WAL group sequence manager.
     */
    private MetaOneWalGroupSeq metaOneWalGroupSeq;

    /**
     * Retrieves the WAL group sequence for a specific split index and bucket index.
     *
     * @param splitIndex  the split index.
     * @param bucketIndex the bucket index.
     * @return the WAL group sequence.
     */
    public long getMetaOneWalGroupSeq(byte splitIndex, int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        return metaOneWalGroupSeq.get(walGroupIndex, splitIndex);
    }

    /**
     * Sets the WAL group sequence for a specific split index and bucket index.
     *
     * @param splitIndex  the split index.
     * @param bucketIndex the bucket index.
     * @param seq         the WAL group sequence.
     */
    @SlaveReplay
    public void setMetaOneWalGroupSeq(byte splitIndex, int bucketIndex, long seq) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        metaOneWalGroupSeq.set(walGroupIndex, splitIndex, seq);
    }

    // split 2 times, 1 * 3 * 3 = 9
    // when get bigger, batch persist pvm, will slot stall and read all 9 files, read and write perf will be bad
    // end to end read perf ok, because only read one key bucket and lru cache
    // increase buckets per slot value, then will split fewer times, but will cost more wal memory
    // or decrease wal delay persist value size, then will once put less key values, may be better for latency
    /**
     * The maximum number of splits.
     */
    public static final byte MAX_SPLIT_NUMBER = 9;

    /**
     * The step size for splits.
     */
    static final int SPLIT_MULTI_STEP = 3;

    // you can change here, the bigger, key buckets will split more times, like load factor
    // compare to KeyBucket.INIT_CAPACITY
    /**
     * The key or cell cost tolerance count when checking splits.
     */
    static final int KEY_OR_CELL_COST_TOLERANCE_COUNT_WHEN_CHECK_SPLIT = 0;

    /**
     * The array of file descriptor read/write managers, indexed by split index.
     */
    @VisibleForTesting
    FdReadWrite[] fdReadWriteArray;

    /**
     * The all key hash buckets manager for pure memory mode v2.
     */
    @VisibleForTesting
    AllKeyHashBuckets allKeyHashBuckets;

    /**
     * The meta chunk segment fill ratio manager, for memory gc.
     */
    MetaChunkSegmentFillRatio metaChunkSegmentFillRatio;

    /**
     * Get the meta chunk segment fill ratio manager, for memory gc.
     *
     * @return the meta chunk segment fill ratio manager.
     */
    public MetaChunkSegmentFillRatio getMetaChunkSegmentFillRatio() {
        return metaChunkSegmentFillRatio;
    }

    /**
     * The logger for this class.
     */
    private static final Logger log = LoggerFactory.getLogger(KeyLoader.class);

    /**
     * The stat key count in buckets manager.
     */
    private StatKeyCountInBuckets statKeyCountInBuckets;

    /**
     * Retrieves the key count for a specific bucket index.
     *
     * @param bucketIndex the bucket index.
     * @return the key count.
     * @throws IllegalArgumentException if the bucket index is out of range.
     */
    public short getKeyCountInBucketIndex(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot=" + slot + ", bucket index=" + bucketIndex);
        }
        return statKeyCountInBuckets.getKeyCountForBucketIndex(bucketIndex);
    }

    /**
     * Retrieves the total key count.
     *
     * @return the total key count.
     */
    public long getKeyCount() {
        // for unit test
        if (statKeyCountInBuckets == null) {
            return 0L;
        }
        return statKeyCountInBuckets.getKeyCount();
    }

    /**
     * Retrieves the bytes representing the stat key count in buckets for slave nodes.
     *
     * @return the bytes representing the stat key count in buckets.
     */
    @SlaveReplay
    public byte[] getStatKeyCountInBucketsBytesToSlaveExists() {
        return statKeyCountInBuckets.getInMemoryCachedBytes();
    }

    /**
     * Overwrites the stat key count in buckets bytes from the master node for slave nodes.
     *
     * @param bytes the stat key count in buckets bytes from the master node.
     */
    @SlaveReplay
    public void overwriteStatKeyCountInBucketsBytesFromMasterExists(byte[] bytes) {
        statKeyCountInBuckets.overwriteInMemoryCachedBytes(bytes);
        log.warn("Repl overwrite stat key count in buckets bytes from master exists, slot={}", slot);
    }

    /**
     * Updates a batch of key counts for a specific WAL group index and starting bucket index.
     *
     * @param walGroupIndex    the WAL group index.
     * @param beginBucketIndex the starting bucket index.
     * @param keyCountArray    the new key counts.
     * @throws IllegalArgumentException if the bucket index is out of range.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public void updateKeyCountBatch(int walGroupIndex, int beginBucketIndex, short[] keyCountArray) {
        if (beginBucketIndex < 0 || beginBucketIndex + keyCountArray.length > bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot=" + slot + ", begin bucket index=" + beginBucketIndex);
        }
        statKeyCountInBuckets.setKeyCountBatch(walGroupIndex, beginBucketIndex, keyCountArray);
    }

    /**
     * Initializes the file descriptors.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void initFds() throws IOException {
        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, slotDir);
        this.metaOneWalGroupSeq = new MetaOneWalGroupSeq(slot, slotDir);
        this.statKeyCountInBuckets = new StatKeyCountInBuckets(slot, slotDir);

        if (!ConfForGlobal.pureMemoryV2) {
            this.fdReadWriteArray = new FdReadWrite[MAX_SPLIT_NUMBER];

            var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
            this.initFds(maxSplitNumber);
        }
    }

    /**
     * Initializes file descriptors (FDs) for reading and writing key bucket data.
     *
     * @param splitNumber The number of splits to initialize FDs for.
     */
    @VisibleForTesting
    void initFds(byte splitNumber) {
        if (ConfForGlobal.pureMemoryV2) {
            return;
        }

        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            if (fdReadWriteArray[splitIndex] != null) {
                continue;
            }

            var file = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");

            // prometheus metric labels use _ instead of -
            var name = "key_bucket_split_" + splitIndex + "_slot_" + slot;
            FdReadWrite fdReadWrite;
            try {
                fdReadWrite = new FdReadWrite(name, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fdReadWrite.initByteBuffers(false, splitIndex);

            fdReadWriteArray[splitIndex] = fdReadWrite;
        }
        log.info("Persist key bucket files fd opened, split number={}, slot={}", splitNumber, slot);
    }

    /**
     * Cleans up resources used by KeyLoader, including closing file descriptors and cleaning up associated managers.
     */
    @Override
    public void cleanUp() {
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    fdReadWrite.cleanUp();
                }
            }
        }

        if (allKeyHashBuckets != null) {
            allKeyHashBuckets.cleanUp();
        }

        if (metaKeyBucketSplitNumber != null) {
            metaKeyBucketSplitNumber.cleanUp();
        }

        if (metaOneWalGroupSeq != null) {
            metaOneWalGroupSeq.cleanUp();
        }

        if (statKeyCountInBuckets != null) {
            statKeyCountInBuckets.cleanUp();
        }
    }

    /**
     * Checks if the provided byte array at a specific position contains valid key bucket data.
     *
     * @param bytes    The byte array containing the key bucket data.
     * @param position The starting position within the byte array.
     * @return true if the data is valid, false otherwise.
     */
    @VisibleForTesting
    boolean isBytesValidAsKeyBucket(byte[] bytes, int position) {
        if (bytes == null) {
            return false;
        }

        // init is 0, not write yet
        var firstLong = ByteBuffer.wrap(bytes, position, 8).getLong();
        return firstLong != 0;
    }

    /**
     * Calculates the position in shared bytes for a given bucket index.
     *
     * @param bucketIndex The bucket index.
     * @return The calculated position.
     */
    static int getPositionInSharedBytes(int bucketIndex) {
        int firstBucketIndexInTargetWalGroup;
        var mod = bucketIndex % ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (mod != 0) {
            firstBucketIndexInTargetWalGroup = bucketIndex - mod;
        } else {
            firstBucketIndexInTargetWalGroup = bucketIndex;
        }

        return (bucketIndex - firstBucketIndexInTargetWalGroup) * KEY_BUCKET_ONE_COST_SIZE;
    }

    public static final byte typeAsByteIgnore = 0;
    public static final byte typeAsByteString = 1;
    public static final byte typeAsByteList = 2;
    public static final byte typeAsByteSet = 3;
    public static final byte typeAsByteZSet = 4;
    public static final byte typeAsByteHash = 5;
    public static final byte typeAsByteStream = 6;

    /**
     * Converts a CompressedValue type to its corresponding short type byte.
     *
     * @param spType The CompressedValue type.
     * @return The corresponding short type byte.
     */
    public static byte transferToShortType(int spType) {
        if (CompressedValue.isTypeString(spType) || CompressedValue.isGeo(spType) || CompressedValue.isBloomFilter(spType)) {
            return typeAsByteString;
        } else if (CompressedValue.isList(spType)) {
            return typeAsByteList;
        } else if (CompressedValue.isSet(spType)) {
            return typeAsByteSet;
        } else if (CompressedValue.isZSet(spType)) {
            return typeAsByteZSet;
        } else if (CompressedValue.isHash(spType)) {
            return typeAsByteHash;
        } else if (CompressedValue.isStream(spType)) {
            return typeAsByteStream;
        } else {
            return typeAsByteIgnore;
        }
    }

    /**
     * Checks if a key matches a given pattern.
     * todo, need change to glob match
     *
     * @param key          The key to check.
     * @param matchPattern The pattern to match against.
     * @return true if the key matches the pattern, false otherwise.
     */
    public static boolean isKeyMatch(@NotNull String key, @Nullable String matchPattern) {
        if (matchPattern == null) {
            return true;
        }

        if ("*".equals(matchPattern)) {
            return true;
        }

        if (matchPattern.endsWith("*")) {
            if (matchPattern.startsWith("*")) {
                return key.contains(matchPattern.substring(1, matchPattern.length() - 1));
            }

            return key.startsWith(matchPattern.substring(0, matchPattern.length() - 1));
        } else if (matchPattern.startsWith("*")) {
            return key.endsWith(matchPattern.substring(1));
        } else {
            return key.equals(matchPattern);
        }
    }

    /**
     * Reads keys from a specific WAL group and split index into a list, applying filters such as skip count, type, and match pattern.
     *
     * @param keys          The list to populate with keys.
     * @param walGroupIndex The WAL group index.
     * @param splitIndex    The split index.
     * @param skipCount     The number of entries to skip.
     * @param typeAsByte    The type filter as a byte.
     * @param matchPattern  The key match pattern.
     * @param countArray    The remaining count array.
     * @param beginScanSeq  The sequence number to start scanning from.
     * @param inWalKeys     The set of keys already in the WAL.
     * @return A ScanCursor indicating the next scan position or null if no more keys are found.
     */
    private ScanCursor readKeysToList(final @NotNull ArrayList<String> keys,
                                      final int walGroupIndex,
                                      final byte splitIndex,
                                      final short skipCount,
                                      final byte typeAsByte,
                                      final @Nullable String matchPattern,
                                      final int[] countArray,
                                      final long beginScanSeq,
                                      HashSet<String> inWalKeys) {
        var keyCountThisWalGroup = statKeyCountInBuckets.getKeyCountForOneWalGroup(walGroupIndex);
        if (keyCountThisWalGroup == 0) {
            return null;
        }

        if (ConfForGlobal.pureMemoryV2) {
            return allKeyHashBuckets.readKeysToList(keys, walGroupIndex, skipCount, typeAsByte, matchPattern, countArray, inWalKeys);
        }

        var beginBucketIndex = walGroupIndex * ConfForSlot.global.confWal.oneChargeBucketNumber;

        var sharedBytes = readBatchInOneWalGroup(splitIndex, beginBucketIndex);
        if (sharedBytes == null) {
            return null;
        }

        for (int i = 0; i < ConfForSlot.global.confWal.oneChargeBucketNumber; i++) {
            var bucketIndex = beginBucketIndex + i;
            var position = getPositionInSharedBytes(bucketIndex);
            if (position >= sharedBytes.length) {
                continue;
            }

            if (!isBytesValidAsKeyBucket(sharedBytes, position)) {
                continue;
            }

            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            var keyBucket = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, sharedBytes, position, snowFlake);

            final short[] addedKeyCount = {0};
            final short[] tmpSkipCount = {skipCount};
            final short[] expiredOrNotMatchedCount = {0};
            final long currentTimeMillis = System.currentTimeMillis();
            keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                if (tmpSkipCount[0] > 0) {
                    tmpSkipCount[0]--;
                    return;
                }

                // skip expired
                if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
                    expiredOrNotMatchedCount[0]++;
                    return;
                }

                var key = new String(keyBytes);
                if (!isKeyMatch(key, matchPattern)) {
                    expiredOrNotMatchedCount[0]++;
                    return;
                }

                if (inWalKeys.contains(key)) {
                    expiredOrNotMatchedCount[0]++;
                    return;
                }

                if (typeAsByte != typeAsByteIgnore) {
                    if (PersistValueMeta.isPvm(valueBytes)) {
                        var pvm = PersistValueMeta.decode(valueBytes);
                        if (typeAsByte != pvm.shortType) {
                            expiredOrNotMatchedCount[0]++;
                            return;
                        }
                    } else {
                        // number or short string, is string
                        if (typeAsByte != typeAsByteString) {
                            expiredOrNotMatchedCount[0]++;
                            return;
                        }
                    }
                }

                // skip data that is new added after time do scan
                if (seq > beginScanSeq) {
                    return;
                }

                if (countArray[0] <= 0) {
                    return;
                }

                addedKeyCount[0]++;
                keys.add(key);
                countArray[0]--;
            });

            if (countArray[0] <= 0) {
                var nextTimeSkipCount = skipCount + expiredOrNotMatchedCount[0] + addedKeyCount[0];
                return new ScanCursor(slot, walGroupIndex, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) nextTimeSkipCount, splitIndex);
            }
        }
        return null;
    }

    /**
     * A record that holds a scan cursor and the list of keys returned by a scan operation.
     */
    public record ScanCursorWithReturnKeys(@NotNull ScanCursor scanCursor, @NotNull ArrayList<String> keys) {
    }

    /**
     * Scans through key buckets starting from a given WAL group index and split index, applying filters such as skip count, type, and match pattern.
     * need skip already wal keys
     *
     * @param walGroupIndex The WAL group index to start scanning from.
     * @param splitIndex    The split index to start scanning from.
     * @param skipCount     The number of entries to skip at the beginning.
     * @param typeAsByte    The type filter as a byte.
     * @param matchPattern  The key match pattern.
     * @param count         The maximum number of keys to return.
     * @param beginScanSeq  The sequence number to start scanning from.
     * @return A ScanCursorWithReturnKeys object containing the scan cursor and the list of keys found.
     */
    public @NotNull ScanCursorWithReturnKeys scan(final int walGroupIndex,
                                                  final byte splitIndex,
                                                  final short skipCount,
                                                  final byte typeAsByte,
                                                  final @Nullable String matchPattern,
                                                  final int count,
                                                  final long beginScanSeq) {
        final ArrayList<String> keys = new ArrayList<>(count);
        final var inWalKeys = oneSlot.getWalByGroupIndex(walGroupIndex).inWalKeys();

        var walGroupNumber = Wal.calcWalGroupNumber();
        var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();

        // for perf, do not read too many, just return empty key list, redis client will do scan again and use last cursor
        final int onceScanMaxReadCount = ConfForSlot.global.confBucket.onceScanMaxReadCount;
        int readCount = 0;

        final int[] countArray = new int[]{count};
        for (int j = walGroupIndex; j < walGroupNumber; j++) {
            for (int i = 0; i < maxSplitNumber; i++) {
                if (j == walGroupIndex && i < splitIndex) {
                    continue;
                }

                final var skipCountInThisWalGroupThisSplitIndex = i == splitIndex && j == walGroupIndex ? skipCount : 0;
                var scanCursor = readKeysToList(keys, j, (byte) i, skipCountInThisWalGroupThisSplitIndex,
                        typeAsByte, matchPattern, countArray, beginScanSeq, inWalKeys);
                readCount++;
                if (scanCursor != null) {
                    return new ScanCursorWithReturnKeys(scanCursor, keys);
                }

                if (i == maxSplitNumber - 1 && j == walGroupNumber - 1) {
                    return scanNextSlotResult(keys);
                }

                if (readCount >= onceScanMaxReadCount) {
                    var isLastSkipIndex = i == maxSplitNumber - 1;

                    var scanCursorTmp = isLastSkipIndex ?
                            new ScanCursor(slot, j + 1, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) 0) :
                            new ScanCursor(slot, j, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) (i + 1));
                    return new ScanCursorWithReturnKeys(scanCursorTmp, keys);
                }
            }
        }
        return scanNextSlotResult(keys);
    }

    private @NotNull ScanCursorWithReturnKeys scanNextSlotResult(ArrayList<String> keys) {
        var localPersist = LocalPersist.getInstance();
        var lastOneSlot = localPersist.lastOneSlot();
        var isLastSlot = lastOneSlot == null || slot == lastOneSlot.slot();
        if (isLastSlot) {
            return new ScanCursorWithReturnKeys(ScanCursor.END, keys);
        } else {
            var nextOneSlot = localPersist.nextOneSlot(slot);
            if (nextOneSlot == null) {
                return new ScanCursorWithReturnKeys(ScanCursor.END, keys);
            } else {
                var scanCursorTmp = new ScanCursor((short) (slot + 1), 0, (short) 0, (short) 0, (byte) 0);
                return new ScanCursorWithReturnKeys(scanCursorTmp, keys);
            }
        }
    }

    /**
     * Reads a single key bucket for a specific bucket index, split index, and split number.
     *
     * @param bucketIndex       The bucket index.
     * @param splitIndex        The split index.
     * @param splitNumber       The split number.
     * @param isRefreshLRUCache Whether to refresh the LRU cache.
     * @return A KeyBucket object or null if not found.
     */
    @VisibleForTesting
    KeyBucket readKeyBucketForSingleKey(int bucketIndex, byte splitIndex, byte splitNumber, boolean isRefreshLRUCache) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }

        var bytes = fdReadWrite.readOneInner(bucketIndex, isRefreshLRUCache);
        if (ConfForGlobal.pureMemory) {
            // shared bytes
            var position = getPositionInSharedBytes(bucketIndex);
            if (!isBytesValidAsKeyBucket(bytes, position)) {
                return null;
            }
            var r = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, bytes, position, snowFlake);
            r.cvExpiredOrDeletedCallBack = cvExpiredOrDeletedCallBack;
            return r;
        }

        if (!isBytesValidAsKeyBucket(bytes, 0)) {
            return null;
        }
        var r = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, bytes, snowFlake);
        r.cvExpiredOrDeletedCallBack = cvExpiredOrDeletedCallBack;
        return r;
    }

    /**
     * Retrieves the expiration time for a key in a specific bucket.
     *
     * @param bucketIndex The bucket index.
     * @param keyBytes    The key bytes.
     * @param keyHash     The hash of the key.
     * @param keyHash32   The 32-bit hash of the key.
     * @return The expiration time or null if not found.
     */
    Long getExpireAt(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32) {
        if (ConfForGlobal.pureMemoryV2) {
            var recordX = allKeyHashBuckets.get(keyHash32, bucketIndex);
            if (recordX == null) {
                return null;
            }
            return recordX.expireAt();
        }

        var r = getExpireAtAndSeqByKey(bucketIndex, keyBytes, keyHash, keyHash32);
        return r == null ? null : r.expireAt();
    }

    /**
     * Retrieves the expiration time and sequence number for a key in a specific bucket.
     *
     * @param bucketIndex The bucket index.
     * @param keyBytes    The key bytes.
     * @param keyHash     The hash of the key.
     * @param keyHash32   The 32-bit hash of the key.
     * @return An ExpireAtAndSeq object containing the expiration time and sequence number or null if not found.
     */
    KeyBucket.ExpireAtAndSeq getExpireAtAndSeqByKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32) {
        if (ConfForGlobal.pureMemoryV2) {
            var recordX = allKeyHashBuckets.get(keyHash32, bucketIndex);
            if (recordX == null) {
                return null;
            }

            // record id can be used as seq
            return new KeyBucket.ExpireAtAndSeq(recordX.expireAt(), recordX.seq());
        }

        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, true);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getExpireAtAndSeqByKey(keyBytes, keyHash);
    }

    /**
     * Retrieves the value, expiration time, and sequence number for a key in a specific bucket.
     *
     * @param bucketIndex The bucket index.
     * @param keyBytes    The key bytes.
     * @param keyHash     The hash of the key.
     * @param keyHash32   The 32-bit hash of the key.
     * @return A ValueBytesWithExpireAtAndSeq object containing the value, expiration time, and sequence number or null if not found.
     */
    KeyBucket.ValueBytesWithExpireAtAndSeq getValueXByKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32) {
        if (ConfForGlobal.pureMemoryV2) {
            var recordX = allKeyHashBuckets.get(keyHash32, bucketIndex);
            if (recordX == null) {
                return null;
            }

            // record id can be used as seq
            return new KeyBucket.ValueBytesWithExpireAtAndSeq(recordX.toPvm().encode(), recordX.expireAt(), recordX.seq());
        }

        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, true);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getValueXByKey(keyBytes, keyHash);
    }

    /**
     * Warms up the file descriptor read/write managers by performing operations that prepare them for use.
     *
     * @return The number of warmed-up file descriptors.
     */
    int warmUp() {
        int n = 0;
        for (var fdReadWrite : fdReadWriteArray) {
            if (fdReadWrite == null) {
                continue;
            }
            n += fdReadWrite.warmUp();
        }
        return n;
    }

    /**
     * Puts a value into a specific bucket. This method is intended for testing or debugging purposes only.
     * not exact correct when split, just for test or debug, not public
     *
     * @param bucketIndex The bucket index.
     * @param keyBytes    The key bytes.
     * @param keyHash     The hash of the key.
     * @param keyHash32   The 32-bit hash of the key.
     * @param expireAt    The expiration time.
     * @param seq         The sequence number.
     * @param valueBytes  The value bytes.
     */
    @TestOnly
    void putValueByKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32, long expireAt, long seq, byte[] valueBytes) {
        if (ConfForGlobal.pureMemoryV2) {
            // seq as record id
            var putR = allKeyHashBuckets.put(keyHash32, bucketIndex, expireAt, seq, (short) valueBytes.length, typeAsByteIgnore, seq);
            if (putR.isExists()) {
                metaChunkSegmentFillRatio.remove(putR.segmentIndex(), putR.oldValueBytesLength());
            }
            allKeyHashBuckets.putLocalValue(seq, valueBytes);
            return;
        }

        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, false);
        if (keyBucket == null) {
            keyBucket = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, null, snowFlake);
        }

        keyBucket.put(keyBytes, keyHash, expireAt, seq, valueBytes);
        updateKeyBucketInner(bucketIndex, keyBucket, true);
    }

    /**
     * Reads all key buckets for a specific bucket index.
     * not exact correct when split, just for test or debug, not public
     *
     * @param bucketIndex The bucket index.
     * @return A list of KeyBucket objects.
     */
    public ArrayList<KeyBucket> readKeyBuckets(int bucketIndex) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        ArrayList<KeyBucket> keyBuckets = new ArrayList<>(splitNumber);

        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                keyBuckets.add(null);
                continue;
            }

            var bytes = fdReadWrite.readOneInner(bucketIndex, false);
            if (ConfForGlobal.pureMemory) {
                // shared bytes
                var position = getPositionInSharedBytes(bucketIndex);
                if (!isBytesValidAsKeyBucket(bytes, position)) {
                    keyBuckets.add(null);
                } else {
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, position, snowFlake);
                    keyBuckets.add(keyBucket);
                }
            } else {
                if (!isBytesValidAsKeyBucket(bytes, 0)) {
                    keyBuckets.add(null);
                } else {
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                    keyBuckets.add(keyBucket);
                }
            }
        }
        return keyBuckets;
    }

    /**
     * Reads all key buckets for a specific bucket index and returns their string representation for debugging purposes.
     *
     * @param bucketIndex The bucket index.
     * @return A string representation of the key buckets.
     */
    @TestOnly
    public String readKeyBucketsToStringForDebug(int bucketIndex) {
        var keyBuckets = readKeyBuckets(bucketIndex);

        var sb = new StringBuilder();
        for (var one : keyBuckets) {
            sb.append(one).append("\n");
        }
        return sb.toString();
    }

    /**
     * Updates the internal state of a key bucket after modifications. This method is intended for testing or debugging purposes only.
     *
     * @param bucketIndex       The bucket index.
     * @param keyBucket         The KeyBucket object to update.
     * @param isRefreshLRUCache Whether to refresh the LRU cache.
     */
    @TestOnly
    private void updateKeyBucketInner(int bucketIndex, @NotNull KeyBucket keyBucket, boolean isRefreshLRUCache) {
        var bytes = keyBucket.encode(true);
        var splitIndex = keyBucket.splitIndex;

        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            initFds(keyBucket.splitNumber);
            fdReadWrite = fdReadWriteArray[splitIndex];
        }

        fdReadWrite.writeOneInner(bucketIndex, bytes, isRefreshLRUCache);
    }

    /**
     * Reads a batch of key buckets from a specific WAL group and split index.
     *
     * @param splitIndex       The split index.
     * @param beginBucketIndex The starting bucket index.
     * @return The shared bytes representing the batch of key buckets.
     */
    public byte[] readBatchInOneWalGroup(byte splitIndex, int beginBucketIndex) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }
        return fdReadWrite.readKeyBucketsSharedBytesInOneWalGroup(beginBucketIndex);
    }

    /**
     * Performs actions after putting all key buckets in a WAL group.
     *
     * @param walGroupIndex The WAL group index.
     * @param xForBinlog    The XOneWalGroupPersist object for binlog operations.
     * @param inner         The KeyBucketsInOneWalGroup object containing the key buckets.
     */
    private void doAfterPutAll(int walGroupIndex, @NotNull XOneWalGroupPersist xForBinlog, @NotNull KeyBucketsInOneWalGroup inner) {
        if (ConfForGlobal.pureMemoryV2) {
            var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
            for (int i = 0; i < oneChargeBucketNumber; i++) {
                var bucketIndex = inner.beginBucketIndex + i;
                var keyCount = allKeyHashBuckets.getKeyCountInBucketIndex(bucketIndex);
                inner.keyCountForStatsTmp[i] = keyCount;
            }

            updateKeyCountBatch(walGroupIndex, inner.beginBucketIndex, inner.keyCountForStatsTmp);
            xForBinlog.setKeyCountForStatsTmp(inner.keyCountForStatsTmp);

            var seqArray = new long[1];
            // just use first split index
            seqArray[0] = snowFlake.nextId();
            metaOneWalGroupSeq.set(walGroupIndex, (byte) 0, seqArray[0]);
            xForBinlog.setOneWalGroupSeqArrayBySplitIndex(seqArray);
        } else {
            updateKeyCountBatch(walGroupIndex, inner.beginBucketIndex, inner.keyCountForStatsTmp);
            xForBinlog.setKeyCountForStatsTmp(inner.keyCountForStatsTmp);

            var sharedBytesList = inner.writeAfterPutBatch();
            var seqArray = writeSharedBytesList(sharedBytesList, inner.beginBucketIndex);
            xForBinlog.setSharedBytesListBySplitIndex(sharedBytesList);

            for (int splitIndex = 0; splitIndex < seqArray.length; splitIndex++) {
                var seq = seqArray[splitIndex];
                if (seq != 0L) {
                    metaOneWalGroupSeq.set(walGroupIndex, (byte) splitIndex, seq);
                }
            }
            xForBinlog.setOneWalGroupSeqArrayBySplitIndex(seqArray);

            updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp);
            xForBinlog.setSplitNumberAfterPut(inner.splitNumberTmp);
        }

        if (oneSlot != null) {
            oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex);
        }
    }

    /**
     * Updates a batch of PersistValueMeta objects after writing segments.
     *
     * @param walGroupIndex                The WAL group index.
     * @param pvmList                      The list of PersistValueMeta objects.
     * @param xForBinlog                   The XOneWalGroupPersist object for binlog operations.
     * @param keyBucketsInOneWalGroupGiven The KeyBucketsInOneWalGroup object containing the key buckets or null.
     */
    public void updatePvmListBatchAfterWriteSegments(int walGroupIndex,
                                                     @NotNull ArrayList<PersistValueMeta> pvmList,
                                                     @NotNull XOneWalGroupPersist xForBinlog,
                                                     @Nullable KeyBucketsInOneWalGroup keyBucketsInOneWalGroupGiven) {
        var inner = keyBucketsInOneWalGroupGiven != null ? keyBucketsInOneWalGroupGiven :
                new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        xForBinlog.setBeginBucketIndex(inner.beginBucketIndex);

        if (ConfForGlobal.pureMemoryV2) {
            // group by bucket index
            var pvmListGroupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(pvm -> pvm.bucketIndex));
            var recordXBytesArray = new byte[pvmListGroupByBucketIndex.size()][];
            var count = 0;
            for (var entry : pvmListGroupByBucketIndex.entrySet()) {
                var bucketIndex = entry.getKey();
                var pvmListThisBucket = entry.getValue();

                var bos = new ByteArrayOutputStream();
                try (var dataOs = new DataOutputStream(bos)) {
                    dataOs.writeInt(bucketIndex);
                    dataOs.writeInt(pvmListThisBucket.size());

                    for (var pvm : pvmListThisBucket) {
                        var recordId = AllKeyHashBuckets.pvmToRecordId(pvm);
                        var putR = allKeyHashBuckets.put(pvm.keyHash32, bucketIndex, pvm.expireAt, pvm.seq, pvm.valueBytesLength, pvm.shortType, recordId);
                        if (putR.isExists()) {
                            metaChunkSegmentFillRatio.remove(putR.segmentIndex(), putR.oldValueBytesLength());
                        }

                        dataOs.writeInt(pvm.keyHash32);
                        dataOs.writeLong(pvm.expireAt);
                        dataOs.writeByte(pvm.shortType);
                        dataOs.writeLong(recordId);
                        dataOs.writeLong(pvm.seq);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                recordXBytesArray[count] = bos.toByteArray();
                count++;
            }
            xForBinlog.setRecordXBytesArray(recordXBytesArray);
        } else {
            inner.putAllPvmList(pvmList);
        }

        doAfterPutAll(walGroupIndex, xForBinlog, inner);
    }

    /**
     * Persists a batch of short values in a specific WAL group.
     *
     * @param walGroupIndex  The WAL group index.
     * @param shortValueList The collection of short values.
     * @param xForBinlog     The XOneWalGroupPersist object for binlog operations.
     */
    public void persistShortValueListBatchInOneWalGroup(int walGroupIndex,
                                                        @NotNull Collection<Wal.V> shortValueList,
                                                        @NotNull XOneWalGroupPersist xForBinlog) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        xForBinlog.setBeginBucketIndex(inner.beginBucketIndex);

        inner.putAll(shortValueList);
        doAfterPutAll(walGroupIndex, xForBinlog, inner);
    }

    /**
     * Retrieves the records bytes array in a specific WAL group.
     *
     * @param beginBucketIndex The starting bucket index.
     * @return The records bytes array.
     */
    public byte[][] getRecordsBytesArrayInOneWalGroup(int beginBucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(beginBucketIndex);
        return allKeyHashBuckets.getRecordsBytesArrayByWalGroupIndex(walGroupIndex);
    }

    /**
     * Updates the records bytes array for slave nodes.
     *
     * @param recordXBytesArray The records bytes array.
     * @return The total number of records updated.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public int updateRecordXBytesArray(byte[][] recordXBytesArray) {
        if (!ConfForGlobal.pureMemoryV2) {
            return 0;
        }

        var n = 0;
        for (var bytes : recordXBytesArray) {
            var buffer = ByteBuffer.wrap(bytes);
            var bucketIndex = buffer.getInt();
            var recordSize = buffer.getInt();
            for (int i = 0; i < recordSize; i++) {
                var keyHash32 = buffer.getInt();
                var recordId = buffer.getLong();
                var expireAtAndShortType = buffer.getLong();
                var expireAt = expireAtAndShortType >>> 16;
                byte shortType = (byte) (expireAtAndShortType & 0xFF);
                var seq = buffer.getLong();
                var valueBytesLength = buffer.getInt();

                // for unit test
                if (seq == 0L) {
                    continue;
                }

                var putR = allKeyHashBuckets.put(keyHash32, bucketIndex, expireAt, seq, valueBytesLength, shortType, recordId);
                if (putR.isExists()) {
                    metaChunkSegmentFillRatio.remove(putR.segmentIndex(), putR.oldValueBytesLength());
                }
            }
            n += recordSize;
        }
        return n;
    }

    /**
     * Writes a shared bytes list for a specific WAL group. Do this after the key buckets updated in this WAL group.
     *
     * @param sharedBytesListBySplitIndex The shared bytes list indexed by split index.
     * @param beginBucketIndex            The starting bucket index.
     * @return An array of sequence numbers corresponding to each split index.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public long[] writeSharedBytesList(byte[][] sharedBytesListBySplitIndex, int beginBucketIndex) {
        var seqArray = new long[sharedBytesListBySplitIndex.length];
        for (int splitIndex = 0; splitIndex < sharedBytesListBySplitIndex.length; splitIndex++) {
            var sharedBytes = sharedBytesListBySplitIndex[splitIndex];
            if (sharedBytes == null) {
                continue;
            }

            if (fdReadWriteArray.length <= splitIndex) {
                var oldFdReadWriteArray = fdReadWriteArray;
                fdReadWriteArray = new FdReadWrite[splitIndex + 1];
                System.arraycopy(oldFdReadWriteArray, 0, fdReadWriteArray, 0, oldFdReadWriteArray.length);
            }

            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                initFds((byte) (splitIndex + 1));
                fdReadWrite = fdReadWriteArray[splitIndex];
            }

            fdReadWrite.writeSharedBytesForKeyBucketsInOneWalGroup(beginBucketIndex, sharedBytes);
            seqArray[splitIndex] = snowFlake.nextId();
        }
        return seqArray;
    }

    /**
     * Removes a single key from a specific bucket. This method uses WAL delay removal instead of immediate removal.
     * use wal delay remove instead of remove immediately
     *
     * @param bucketIndex The bucket index.
     * @param keyBytes    The key bytes.
     * @param keyHash     The hash of the key.
     * @param keyHash32   The 32-bit hash of the key.
     * @return true if the key was successfully removed, false otherwise.
     */
    @TestOnly
    boolean removeSingleKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32) {
        if (ConfForGlobal.pureMemoryV2) {
            var removeR = allKeyHashBuckets.remove(keyHash32, bucketIndex);
            if (removeR.isExists()) {
                metaChunkSegmentFillRatio.remove(removeR.segmentIndex(), removeR.oldValueBytesLength());
            }
            return removeR.isExists();
        }

        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, false);
        if (keyBucket == null) {
            return false;
        }

        var isDeleted = keyBucket.del(keyBytes, keyHash, true);
        if (isDeleted) {
            updateKeyBucketInner(bucketIndex, keyBucket, false);
        }

        return isDeleted;
    }

    @VisibleForTesting
    int intervalRemoveExpiredLastBucketIndex = 0;

    void intervalRemoveExpiredForSaveMemory() {
        assert ConfForGlobal.pureMemoryV2;

        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        var r = allKeyHashBuckets.removeExpired(intervalRemoveExpiredLastBucketIndex);
        for (var entry : r.entrySet()) {
            metaChunkSegmentFillRatio.remove(entry.getKey(), entry.getValue());
        }

        if (intervalRemoveExpiredLastBucketIndex == 0) {
            log.info("Key buckets interval remove expired, refer segment count={}", r.size());
        }

        intervalRemoveExpiredLastBucketIndex++;
        if (intervalRemoveExpiredLastBucketIndex >= bucketsPerSlot) {
            intervalRemoveExpiredLastBucketIndex = 0;
        }
    }

    /**
     * Get persisted big string uuid list, not expired.
     *
     * @param bucketIndex The bucket index.
     * @return The persisted big string uuid with key list.
     */
    List<BigStringFiles.IdWithKey> getPersistedBigStringUuidList(int bucketIndex) {
        // pure memory, todo

        var list = new ArrayList<BigStringFiles.IdWithKey>();
        var currentTimeMillis = System.currentTimeMillis();

        var keyBuckets = readKeyBuckets(bucketIndex);
        for (var keyBucket : keyBuckets) {
            if (keyBucket == null) {
                continue;
            }

            keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                if (expireAt == CompressedValue.NO_EXPIRE || expireAt > currentTimeMillis) {
                    var buf = Unpooled.wrappedBuffer(valueBytes);
                    var cv = CompressedValue.decode(buf, keyBytes, keyHash);
                    if (cv.isBigString()) {
                        list.add(new BigStringFiles.IdWithKey(cv.getBigStringMetaUuid(), new String(keyBytes)));
                    }
                }
            });
        }
        return list;
    }

    @VisibleForTesting
    int intervalDeleteExpiredBigStringFilesLastBucketIndex = 0;

    /**
     * Delete expired big string files.
     *
     * @return The count of deleted big string files.
     */
    @VisibleForTesting
    int intervalDeleteExpiredBigStringFiles() {
        // pure memory, todo

        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        final int[] countArray = {0};
        var currentTimeMillis = System.currentTimeMillis();

        var keyBuckets = readKeyBuckets(intervalDeleteExpiredBigStringFilesLastBucketIndex);
        for (var keyBucket : keyBuckets) {
            if (keyBucket == null) {
                continue;
            }

            keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
                    var buf = Unpooled.wrappedBuffer(valueBytes);
                    var cv = CompressedValue.decode(buf, keyBytes, keyHash);
                    if (cv.isBigString()) {
                        KeyLoader.this.cvExpiredOrDeletedCallBack.handle(new String(keyBytes), cv);
                        countArray[0]++;
                    }
                }
            });
        }

        if (intervalDeleteExpiredBigStringFilesLastBucketIndex % 16384 == 0 || countArray[0] > 0) {
            log.info("Key buckets interval delete expired big string files, bucket index={}, slot={}, refer big string files count={}",
                    intervalDeleteExpiredBigStringFilesLastBucketIndex, slot, countArray[0]);
        }

        intervalDeleteExpiredBigStringFilesLastBucketIndex++;
        if (intervalDeleteExpiredBigStringFilesLastBucketIndex >= bucketsPerSlot) {
            intervalDeleteExpiredBigStringFilesLastBucketIndex = 0;
        }

        return countArray[0];
    }

    @Override
    public String name() {
        return "check and delete expired big string files";
    }

    @Override
    public void run() {
        intervalDeleteExpiredBigStringFiles();
    }

    @Override
    public int executeOnceAfterLoopCount() {
        // execute once every 10ms
        return 1;
    }

    /**
     * Flushes all data managed by this KeyLoader, clearing meta information and truncating file descriptors.
     */
    @SlaveNeedReplay
    @SlaveReplay
    public void flush() {
        metaKeyBucketSplitNumber.clear();
        metaOneWalGroupSeq.clear();
        statKeyCountInBuckets.clear();

        if (ConfForGlobal.pureMemoryV2) {
            allKeyHashBuckets.flush();
            metaChunkSegmentFillRatio.flush();
        } else {
            for (int splitIndex = 0; splitIndex < MAX_SPLIT_NUMBER; splitIndex++) {
                if (fdReadWriteArray.length <= splitIndex) {
                    continue;
                }
                var fdReadWrite = fdReadWriteArray[splitIndex];
                if (fdReadWrite == null) {
                    continue;
                }
                fdReadWrite.truncate();
            }
        }
    }

    /**
     * Collects metrics about the KeyLoader.
     *
     * @return A map of metric names to their double values.
     */
    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        map.put("key_loader_bucket_count", (double) bucketsPerSlot);
        map.put("persist_key_count", (double) getKeyCount());

        if (ConfForGlobal.pureMemoryV2) {
            // chunk segment fill ratio
            for (int i = 0; i < MetaChunkSegmentFillRatio.FILL_RATIO_BUCKETS; i++) {
                map.put("chunk_segment_fill_ratio_percent_" + ((i + 1) * 5), (double) metaChunkSegmentFillRatio.fillRatioBucketSegmentCount[i]);
            }
        }

        var diskUsage = 0L;
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    diskUsage += fdReadWrite.writeIndex;
                    map.putAll(fdReadWrite.collect());
                }
            }
        }

        diskUsage += metaKeyBucketSplitNumber.allCapacity;
        diskUsage += metaOneWalGroupSeq.allCapacity;
        diskUsage += statKeyCountInBuckets.allCapacity;
        map.put("key_loader_disk_usage", (double) diskUsage);

        return map;
    }
}
