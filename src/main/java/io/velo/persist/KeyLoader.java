package io.velo.persist;

import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import jnr.posix.LibC;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class KeyLoader implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp {
    private static final int PAGE_NUMBER_PER_BUCKET = 1;
    public static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * LocalPersist.PAGE_SIZE;

    // one split file max 2GB, 2 * 1024 * 1024 / 4 = 524288
    // one split index one file
    static final int MAX_KEY_BUCKET_COUNT_PER_FD = 2 * 1024 * 1024 / 4;

    @TestOnly
    KeyLoader(short slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake) {
        this(slot, bucketsPerSlot, slotDir, snowFlake, null);
    }

    public KeyLoader(short slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake, OneSlot oneSlot) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.bucketsPerSlot = bucketsPerSlot;
        this.slotDir = slotDir;
        this.snowFlake = snowFlake;
        this.oneSlot = oneSlot;
        this.shortValueCvExpiredCallBack = (key, cvExpired) -> {
            if (oneSlot == null) {
                log.warn("Short value cv expired, slot: {}", slot);
                return;
            }

            if (cvExpired.isBigString()) {
                var uuid = cvExpired.getBigStringMetaUuid();
                var isDeleted = oneSlot.getBigStringFiles().deleteBigStringFileIfExist(uuid);
                if (!isDeleted) {
                    throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
                } else {
                    log.warn("Delete big string file, s={}, key={}, uuid={}", slot, key, uuid);
                }
            }
        };
    }

    @Override
    public String toString() {
        return "KeyLoader{" +
                "slot=" + slot +
                ", bucketsPerSlot=" + bucketsPerSlot +
                '}';
    }

    @Override
    public long estimate() {
        long size = 0;
        size += metaKeyBucketSplitNumber.estimate();
        size += metaOneWalGroupSeq.estimate();
        size += statKeyCountInBuckets.estimate();
        for (var fdReadWrite : fdReadWriteArray) {
            if (fdReadWrite != null) {
                size += fdReadWrite.estimate();
            }
        }
        return size;
    }

    private final short slot;
    private final String slotStr;
    final int bucketsPerSlot;
    private final File slotDir;
    final SnowFlake snowFlake;

    private final OneSlot oneSlot;

    final KeyBucket.ShortValueCvExpiredCallBack shortValueCvExpiredCallBack;

    @VisibleForTesting
    MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    byte[] getMetaKeyBucketSplitNumberBatch(int beginBucketIndex, int bucketCount) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
        }

        return metaKeyBucketSplitNumber.getBatch(beginBucketIndex, bucketCount);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public boolean updateMetaKeyBucketSplitNumberBatchIfChanged(int beginBucketIndex, byte[] splitNumberArray) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
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
        log.warn("Repl overwrite meta key bucket split number bytes from master exists, slot: {}", slot);
    }

    @TestOnly
    void setMetaKeyBucketSplitNumber(int bucketIndex, byte splitNumber) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot: " + slot + ", begin bucket index: " + bucketIndex);
        }

        metaKeyBucketSplitNumber.set(bucketIndex, splitNumber);
    }

    private MetaOneWalGroupSeq metaOneWalGroupSeq;

    public long getMetaOneWalGroupSeq(byte splitIndex, int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        return metaOneWalGroupSeq.get(walGroupIndex, splitIndex);
    }

    @SlaveReplay
    public void setMetaOneWalGroupSeq(byte splitIndex, int bucketIndex, long seq) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
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

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(KeyLoader.class);

    private StatKeyCountInBuckets statKeyCountInBuckets;

    public short getKeyCountInBucketIndex(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot: " + slot + ", bucket index: " + bucketIndex);
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
        log.warn("Repl overwrite stat key count in buckets bytes from master exists, slot: {}", slot);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void updateKeyCountBatch(int walGroupIndex, int beginBucketIndex, short[] keyCountArray) {
        if (beginBucketIndex < 0 || beginBucketIndex + keyCountArray.length > bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
        }

        statKeyCountInBuckets.setKeyCountBatch(walGroupIndex, beginBucketIndex, keyCountArray);
    }

    public void initFds(LibC libC) throws IOException {
        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, slotDir);
        this.metaOneWalGroupSeq = new MetaOneWalGroupSeq(slot, slotDir);
        this.statKeyCountInBuckets = new StatKeyCountInBuckets(slot, slotDir);

        this.libC = libC;
        this.fdReadWriteArray = new FdReadWrite[MAX_SPLIT_NUMBER];

        var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
        this.initFds(maxSplitNumber);
    }

    @VisibleForTesting
    void initFds(byte splitNumber) {
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
        log.info("Persist key bucket files fd opened, split number: {}, slot: {}", splitNumber, slot);
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
            r.shortValueCvExpiredCallBack = shortValueCvExpiredCallBack;
            return r;
        }

        if (!isBytesValidAsKeyBucket(bytes, 0)) {
            return null;
        }
        var r = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, bytes, snowFlake);
        r.shortValueCvExpiredCallBack = shortValueCvExpiredCallBack;
        return r;
    }

    KeyBucket.ValueBytesWithExpireAtAndSeq getValueByKey(int bucketIndex, byte[] keyBytes, long keyHash) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, true);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getValueByKey(keyBytes, keyHash);
    }

    // not exact correct when split, just for test or debug, not public
    @TestOnly
    void putValueByKey(int bucketIndex, byte[] keyBytes, long keyHash, long expireAt, long seq, byte[] valueBytes) {
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
    private void updateKeyBucketInner(int bucketIndex, KeyBucket keyBucket, boolean isRefreshLRUCache) {
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

    private void doAfterPutAll(int walGroupIndex, XOneWalGroupPersist xForBinlog, KeyBucketsInOneWalGroup inner) {
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

        if (oneSlot != null) {
            oneSlot.clearKvInTargetWalGroupIndexLRU(walGroupIndex);
        }
    }

    public void updatePvmListBatchAfterWriteSegments(int walGroupIndex, ArrayList<PersistValueMeta> pvmList, XOneWalGroupPersist xForBinlog) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        xForBinlog.setBeginBucketIndex(inner.beginBucketIndex);

        inner.putAllPvmList(pvmList);
        doAfterPutAll(walGroupIndex, xForBinlog, inner);
    }

    public void persistShortValueListBatchInOneWalGroup(int walGroupIndex, Collection<Wal.V> shortValueList, XOneWalGroupPersist xForBinlog) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        xForBinlog.setBeginBucketIndex(inner.beginBucketIndex);

        inner.putAll(shortValueList);
        doAfterPutAll(walGroupIndex, xForBinlog, inner);
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
    boolean removeSingleKey(int bucketIndex, byte[] keyBytes, long keyHash) {
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
