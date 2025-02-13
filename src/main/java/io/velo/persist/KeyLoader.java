package io.velo.persist;

import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import jnr.posix.LibC;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class KeyLoader implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, CanSaveAndLoad {
    private static final int PAGE_NUMBER_PER_BUCKET = 1;
    public static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * LocalPersist.PAGE_SIZE;

    // one split file max 2GB, 2 * 1024 * 1024 / 4 = 524288
    // one split index one file
    static final int MAX_KEY_BUCKET_COUNT_PER_FD = 2 * 1024 * 1024 / 4;

    @TestOnly
    KeyLoader(short slot, int bucketsPerSlot, @NotNull File slotDir, @NotNull SnowFlake snowFlake) {
        this(slot, bucketsPerSlot, slotDir, snowFlake, null);
    }

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

        this.allKeyHashBuckets = new AllKeyHashBuckets(bucketsPerSlot, oneSlot);
    }

    @Override
    public String toString() {
        return "KeyLoader{" +
                "slot=" + slot +
                ", bucketsPerSlot=" + bucketsPerSlot +
                '}';
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        size += metaKeyBucketSplitNumber.estimate(sb);
        size += metaOneWalGroupSeq.estimate(sb);
        size += statKeyCountInBuckets.estimate(sb);

        size += allKeyHashBuckets.estimate(sb);
        if (!ConfForGlobal.pureMemoryV2) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    size += fdReadWrite.estimate(sb);
                }
            }
        }
        return size;
    }

    private final short slot;
    final int bucketsPerSlot;
    private final File slotDir;
    final SnowFlake snowFlake;

    private final OneSlot oneSlot;
    final KeyBucket.CvExpiredOrDeletedCallBack cvExpiredOrDeletedCallBack;

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

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.write(metaKeyBucketSplitNumber.getInMemoryCachedBytes());
        os.write(metaOneWalGroupSeq.getInMemoryCachedBytes());
        os.write(statKeyCountInBuckets.getInMemoryCachedBytes());

        if (ConfForGlobal.pureMemoryV2) {
            allKeyHashBuckets.writeToSavedFileWhenPureMemory(os);
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

    @VisibleForTesting
    MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    byte[] getMetaKeyBucketSplitNumberBatch(int beginBucketIndex, int bucketCount) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot=" + slot + ", begin bucket index=" + beginBucketIndex);
        }

        return metaKeyBucketSplitNumber.getBatch(beginBucketIndex, bucketCount);
    }

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

    public byte maxSplitNumberForRepl() {
        return metaKeyBucketSplitNumber.maxSplitNumber();
    }

    @SlaveReplay
    // read only, important
    public byte[] getMetaKeyBucketSplitNumberBytesToSlaveExists() {
        return metaKeyBucketSplitNumber.getInMemoryCachedBytes();
    }

    @SlaveReplay
    public void overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(byte[] bytes) {
        metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(bytes);
        log.warn("Repl overwrite meta key bucket split number bytes from master exists, slot={}", slot);
    }

    @TestOnly
    void setMetaKeyBucketSplitNumber(int bucketIndex, byte splitNumber) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot=" + slot + ", begin bucket index=" + bucketIndex);
        }

        metaKeyBucketSplitNumber.set(bucketIndex, splitNumber);
    }

    private MetaOneWalGroupSeq metaOneWalGroupSeq;

    public long getMetaOneWalGroupSeq(byte splitIndex, int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        return metaOneWalGroupSeq.get(walGroupIndex, splitIndex);
    }

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
    public static final byte MAX_SPLIT_NUMBER = 9;
    static final int SPLIT_MULTI_STEP = 3;
    // you can change here, the bigger, key buckets will split more times, like load factor
    // compare to KeyBucket.INIT_CAPACITY
    static final int KEY_OR_CELL_COST_TOLERANCE_COUNT_WHEN_CHECK_SPLIT = 0;

    @VisibleForTesting
    LibC libC;
    // index is split index
    @VisibleForTesting
    FdReadWrite[] fdReadWriteArray;

    // for pure memory mode v2
    @VisibleForTesting
    final AllKeyHashBuckets allKeyHashBuckets;

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(KeyLoader.class);

    private StatKeyCountInBuckets statKeyCountInBuckets;

    public short getKeyCountInBucketIndex(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot=" + slot + ", bucket index=" + bucketIndex);
        }

        return statKeyCountInBuckets.getKeyCountForBucketIndex(bucketIndex);
    }

    public long getKeyCount() {
        // for unit test
        if (statKeyCountInBuckets == null) {
            return 0L;
        }

        return statKeyCountInBuckets.getKeyCount();
    }

    @SlaveReplay
    public byte[] getStatKeyCountInBucketsBytesToSlaveExists() {
        return statKeyCountInBuckets.getInMemoryCachedBytes();
    }

    @SlaveReplay
    public void overwriteStatKeyCountInBucketsBytesFromMasterExists(byte[] bytes) {
        statKeyCountInBuckets.overwriteInMemoryCachedBytes(bytes);
        log.warn("Repl overwrite stat key count in buckets bytes from master exists, slot={}", slot);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void updateKeyCountBatch(int walGroupIndex, int beginBucketIndex, short[] keyCountArray) {
        if (beginBucketIndex < 0 || beginBucketIndex + keyCountArray.length > bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot=" + slot + ", begin bucket index=" + beginBucketIndex);
        }

        statKeyCountInBuckets.setKeyCountBatch(walGroupIndex, beginBucketIndex, keyCountArray);
    }

    public void initFds(LibC libC) throws IOException {
        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, slotDir);
        this.metaOneWalGroupSeq = new MetaOneWalGroupSeq(slot, slotDir);
        this.statKeyCountInBuckets = new StatKeyCountInBuckets(slot, slotDir);

        if (!ConfForGlobal.pureMemoryV2) {
            this.libC = libC;
            this.fdReadWriteArray = new FdReadWrite[MAX_SPLIT_NUMBER];

            var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
            this.initFds(maxSplitNumber);
        }
    }

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
                fdReadWrite = new FdReadWrite(slot, name, libC, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fdReadWrite.initByteBuffers(false);

            fdReadWriteArray[splitIndex] = fdReadWrite;
        }
        log.info("Persist key bucket files fd opened, split number={}, slot={}", splitNumber, slot);
    }

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

    @VisibleForTesting
    boolean isBytesValidAsKeyBucket(byte[] bytes, int position) {
        if (bytes == null) {
            return false;
        }

        // init is 0, not write yet
        var firstLong = ByteBuffer.wrap(bytes, position, 8).getLong();
        return firstLong != 0;
    }

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

    public static byte transferToShortType(int spType) {
        if (CompressedValue.isTypeString(spType)) {
            return typeAsByteString;
        } else if (CompressedValue.isList(spType)) {
            return typeAsByteList;
        } else if (CompressedValue.isSet(spType)) {
            return typeAsByteSet;
        } else if (CompressedValue.isZSet(spType)) {
            return typeAsByteZSet;
        } else if (CompressedValue.isHash(spType)) {
            return typeAsByteHash;
        } else {
            return typeAsByteIgnore;
        }
    }

    // todo, need change to glob match
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

    public record ScanCursorWithReturnKeys(@NotNull ScanCursor scanCursor, @NotNull ArrayList<String> keys) {
    }

    // need skip already wal keys
    public ScanCursorWithReturnKeys scan(final int walGroupIndex,
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

        final int[] countArray = new int[]{count};
        for (int j = walGroupIndex; j < walGroupNumber; j++) {
            for (int i = 0; i < maxSplitNumber; i++) {
                if (j == walGroupIndex && i < splitIndex) {
                    continue;
                }

                final var skipCountInThisWalGroupThisSplitIndex = i == splitIndex && j == walGroupIndex ? skipCount : 0;

                var scanCursor = readKeysToList(keys, j, (byte) i, skipCountInThisWalGroupThisSplitIndex,
                        typeAsByte, matchPattern, countArray, beginScanSeq, inWalKeys);
                if (scanCursor != null) {
                    return new ScanCursorWithReturnKeys(scanCursor, keys);
                }

                if (i == maxSplitNumber - 1 && j == walGroupNumber - 1) {
                    return new ScanCursorWithReturnKeys(ScanCursor.END, keys);
                }
            }
        }
        return null;
    }

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

    // not exact correct when split, just for test or debug, not public
    @TestOnly
    void putValueByKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32, long expireAt, long seq, byte[] valueBytes) {
        if (ConfForGlobal.pureMemoryV2) {
            // seq as record id
            allKeyHashBuckets.put(keyHash32, bucketIndex, expireAt, seq, typeAsByteIgnore, seq);
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

    // not exact correct when split, just for test or debug, not public
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

    @TestOnly
    public String readKeyBucketsToStringForDebug(int bucketIndex) {
        var keyBuckets = readKeyBuckets(bucketIndex);

        var sb = new StringBuilder();
        for (var one : keyBuckets) {
            sb.append(one).append("\n");
        }
        return sb.toString();
    }

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

    public byte[] readBatchInOneWalGroup(byte splitIndex, int beginBucketIndex) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }
        return fdReadWrite.readKeyBucketsSharedBytesInOneWalGroup(beginBucketIndex);
    }

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
                        allKeyHashBuckets.put(pvm.keyHash32, bucketIndex, pvm.expireAt, pvm.seq, pvm.shortType, recordId);

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

    public void persistShortValueListBatchInOneWalGroup(int walGroupIndex,
                                                        @NotNull Collection<Wal.V> shortValueList,
                                                        @NotNull XOneWalGroupPersist xForBinlog) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        xForBinlog.setBeginBucketIndex(inner.beginBucketIndex);

        inner.putAll(shortValueList);
        doAfterPutAll(walGroupIndex, xForBinlog, inner);
    }

    public byte[][] getRecordsBytesArrayInOneWalGroup(int beginBucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(beginBucketIndex);
        return allKeyHashBuckets.getRecordsBytesArrayByWalGroupIndex(walGroupIndex);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public int updateRecordXBytesArray(byte[][] recordXBytesArray) {
        var n = 0;
        for (var bytes : recordXBytesArray) {
            var buffer = ByteBuffer.wrap(bytes);
            var bucketIndex = buffer.getInt();
            var recordSize = buffer.getInt();
            for (int i = 0; i < recordSize; i++) {
                var keyHash32 = buffer.getInt();
                var expireAt = buffer.getLong();
                var shortType = buffer.get();
                var recordId = buffer.getLong();
                var seq = buffer.getLong();

                allKeyHashBuckets.put(keyHash32, bucketIndex, expireAt, seq, shortType, recordId);
            }
            n += recordSize;
        }
        return n;
    }

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

    // use wal delay remove instead of remove immediately
    @TestOnly
    boolean removeSingleKey(int bucketIndex, byte[] keyBytes, long keyHash, int keyHash32) {
        if (ConfForGlobal.pureMemoryV2) {
            return allKeyHashBuckets.remove(keyHash32, bucketIndex);
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

    @SlaveNeedReplay
    @SlaveReplay
    public void flush() {
        metaKeyBucketSplitNumber.clear();
        metaOneWalGroupSeq.clear();
        statKeyCountInBuckets.clear();

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

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        map.put("key_loader_bucket_count", (double) bucketsPerSlot);
        map.put("persist_key_count", (double) getKeyCount());

        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite == null) {
                    continue;
                }
                map.putAll(fdReadWrite.collect());
            }
        }

        return map;
    }
}
