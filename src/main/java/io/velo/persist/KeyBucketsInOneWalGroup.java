package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages key buckets within a specific WAL group.
 */
public class KeyBucketsInOneWalGroup {
    /**
     * @param slot          the slot index to which the key buckets belong
     * @param walGroupIndex the index of the WAL group
     * @param keyLoader     the KeyLoader instance used to load key data
     */
    public KeyBucketsInOneWalGroup(short slot, int walGroupIndex, @NotNull KeyLoader keyLoader) {
        this(slot, walGroupIndex, keyLoader, true);
    }

    /**
     * @param slot          the slot index to which the key buckets belong
     * @param walGroupIndex the index of the WAL group
     * @param keyLoader     the KeyLoader instance used to load key data
     * @param readExisting  whether to load the current key-bucket index before updates
     */
    KeyBucketsInOneWalGroup(short slot, int walGroupIndex, @NotNull KeyLoader keyLoader, boolean readExisting) {
        this.slot = slot;
        this.keyLoader = keyLoader;

        this.oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.keyCountForStatsTmp = new short[oneChargeBucketNumber];
        this.beginBucketIndex = oneChargeBucketNumber * walGroupIndex;

        if (readExisting) {
            this.readBeforePutBatch();
        } else {
            this.splitNumberTmp = new byte[oneChargeBucketNumber];
            Arrays.fill(this.splitNumberTmp, ConfForSlot.global.confBucket.initialSplitNumber);
            for (int i = 0; i < ConfForSlot.global.confBucket.initialSplitNumber; i++) {
                listList.add(prepareListInitWithNull());
            }
        }
    }

    private final short slot;
    private final int oneChargeBucketNumber;
    // index is bucket index - begin bucket index
    byte[] splitNumberTmp;
    final short[] keyCountForStatsTmp;
    final int beginBucketIndex;

    private final KeyLoader keyLoader;

    private static final Logger log = LoggerFactory.getLogger(KeyBucketsInOneWalGroup.class);

    // outer index is split index, inner index is relative (bucket index - begin bucket index)
    @VisibleForTesting
    ArrayList<ArrayList<KeyBucket>> listList = new ArrayList<>();

    private ArrayList<KeyBucket> prepareListInitWithNull() {
        var listInitWithNull = new ArrayList<KeyBucket>();
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            // init size with null
            listInitWithNull.add(null);
        }
        return listInitWithNull;
    }

    @VisibleForTesting
    void readBeforePutBatch() {
        this.splitNumberTmp = keyLoader.getMetaKeyBucketSplitNumberBatch(beginBucketIndex, oneChargeBucketNumber);
        byte maxSplitNumber = 1;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumber) {
                maxSplitNumber = splitNumberTmp[i];
            }
        }

        // Slow-path recovery for the crash-window mismatch (bug 48 finding 1). The invariant
        // we restore is "metadata must match the data layout actually encoded in
        // lastUpdateSeq" - not "metadata should only go up". Per-key lookups are
        // hash-routed via KeyHash.splitIndex(keyHash, splitNumber, bucketIndex), so a
        // stale higher metadata in Window B is unsafe even though it "covers" more splits.
        //
        // We do it lazily, per-bucket, on the first mismatch: the strict constructor
        // throws if the on-disk data was encoded with a different split number. The
        // fallback helper retries with splitNumber=-1 to decode the embedded value and
        // we patch splitNumberTmp so the rest of this load uses the correct routing.
        // In the common consistent case this is a single try with no extra work.
        var sharedBytesList = new byte[maxSplitNumber][];
        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            sharedBytesList[splitIndex] = keyLoader.readBatchInOneWalGroup((byte) splitIndex, beginBucketIndex);
        }

        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            if (listList.size() <= splitIndex) {
                // init size with null
                listList.add(null);
            }
        }

        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            var list = prepareListInitWithNull();
            listList.set(splitIndex, list);

            var sharedBytes = sharedBytesList[splitIndex];
            if (sharedBytes == null) {
                continue;
            }

            for (int i = 0; i < oneChargeBucketNumber; i++) {
                var bucketIndex = beginBucketIndex + i;
                var currentSplitNumber = splitNumberTmp[i];
                var position = KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i;
                if (position >= sharedBytes.length) {
                    continue;
                }
                if (!keyLoader.isBytesValidAsKeyBucket(sharedBytes, position)) {
                    continue;
                }
                var result = KeyBucket.readWithFallback(slot, bucketIndex, (byte) splitIndex, currentSplitNumber,
                        sharedBytes, position, keyLoader.snowFlake);
                if (result.wasRecovered() && result.effectiveSplitNumber() != currentSplitNumber) {
                    // The on-disk split layout disagrees with metadata. Patch splitNumberTmp
                    // so subsequent iterations in this load (and per-key reads after the
                    // load completes) use the correct routing. The constructor has
                    // already decoded the bucket with the embedded value, so iterate()
                    // would find the keys in their actual data layout.
                    splitNumberTmp[i] = result.effectiveSplitNumber();
                }
                var keyBucket = result.keyBucket();
                keyBucket.cvExpiredOrDeletedCallBack = keyLoader.cvExpiredOrDeletedCallBack;
                // one key bucket max size = KeyBucket.INIT_CAPACITY (48), max split number = 9 or 27, 27 * 48 = 1296 < short max value
                keyCountForStatsTmp[i] += keyBucket.size;
                list.set(i, keyBucket);
            }
        }

        // Persist the repair so the next read (which may bypass this code path) sees
        // the corrected metadata. updateMetaKeyBucketSplitNumberBatchIfChanged is a no-op
        // when nothing changed, so this is cheap in the common case.
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(beginBucketIndex, splitNumberTmp);
    }

    /**
     * @param bucketIndex the index of the bucket
     * @param key         the key
     * @param keyHash     the hash of the key
     * @return the expiration time and sequence number
     */
    KeyBucket.ExpireAtAndSeq getExpireAtAndSeq(int bucketIndex, String key, long keyHash) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];
        var splitIndex = KeyHash.splitIndex(keyHash, currentSplitNumber, bucketIndex);

        var list = listList.get(splitIndex);
        if (list == null) {
            return null;
        }

        var keyBucket = list.get(relativeBucketIndex);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getExpireAtAndSeqByKey(key, keyHash);
    }

    /**
     * @param bucketIndex the index of the bucket
     * @param key         the key
     * @param keyHash     the hash of the key
     * @return the value, expiration time, and sequence number
     */
    @TestOnly
    KeyBucket.ValueBytesWithExpireAtAndSeq getValueX(int bucketIndex, String key, long keyHash) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];
        var splitIndex = KeyHash.splitIndex(keyHash, currentSplitNumber, bucketIndex);

        var list = listList.get(splitIndex);
        if (list == null) {
            return null;
        }

        var keyBucket = list.get(relativeBucketIndex);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getValueXByKey(key, keyHash);
    }

    /**
     * @return the updated shared bytes
     */
    byte[][] encodeAfterPutBatch() {
        byte maxSplitNumberTmp = 1;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumberTmp) {
                maxSplitNumberTmp = splitNumberTmp[i];
            }
        }

        var sharedBytesList = new byte[maxSplitNumberTmp][];

        for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
            var list = listList.get(splitIndex);
            for (int i = 0; i < list.size(); i++) {
                var keyBucket = list.get(i);
                if (keyBucket != null) {
                    keyBucket.splitNumber = splitNumberTmp[i];
                    keyBucket.encode(true);
                }
            }

            var isAllSharedBytes = true;
            for (var keyBucket : list) {
                if (keyBucket == null || !keyBucket.isSharedBytes()) {
                    isAllSharedBytes = false;
                    break;
                }
            }

            byte[] sharedBytes;
            if (isAllSharedBytes) {
                sharedBytes = list.getFirst().bytes;
            } else {
                sharedBytes = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE * oneChargeBucketNumber];
                for (int i = 0; i < oneChargeBucketNumber; i++) {
                    int destPos = KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i;

                    var keyBucket = list.get(i);
                    if (keyBucket == null) {
                        System.arraycopy(KeyBucket.EMPTY_BYTES, 0, sharedBytes, destPos, KeyLoader.KEY_BUCKET_ONE_COST_SIZE);
                    } else {
                        System.arraycopy(keyBucket.bytes, keyBucket.position, sharedBytes, destPos, KeyLoader.KEY_BUCKET_ONE_COST_SIZE);
                    }
                }
            }

            sharedBytesList[splitIndex] = sharedBytes;
        }
        return sharedBytesList;
    }

    @VisibleForTesting
    boolean isSplit = false;

    /**
     * @param pvmListThisBucket the list of PersistValueMeta objects
     * @param bucketIndex       the index of the bucket
     */
    @VisibleForTesting
    void putPvmListToTargetBucket(@NotNull List<PersistValueMeta> pvmListThisBucket, Integer bucketIndex) {
        putPvmListToTargetBucket(pvmListThisBucket, bucketIndex, false);
    }

    private void putPvmListToTargetBucket(@NotNull List<PersistValueMeta> pvmListThisBucket,
                                          Integer bucketIndex,
                                          boolean removeExistingPvm) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];

        var map = new HashMap<String, PersistValueMeta>(KeyBucket.INIT_CAPACITY * currentSplitNumber * KeyLoader.SPLIT_MULTI_STEP);
        final long currentTimeMillis = System.currentTimeMillis();

        // read to map
        for (var list : listList) {
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                continue;
            }

            keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
                if (removeExistingPvm && PersistValueMeta.isPvm(valueBytes)) {
                    return;
                }

                if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
                    cvExpiredOrDeleted(key, valueBytes);
                    return;
                }

                var pvm = new PersistValueMeta();
                pvm.keyHash = keyHash;
                pvm.expireAt = expireAt;
                pvm.seq = seq;
                pvm.key = key;
                pvm.extendBytes = valueBytes;
                map.put(key, pvm);
            });
        }

        // do delete / update or add
        for (var pvm : pvmListThisBucket) {
            // delete
            if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
                var existPvm = map.remove(pvm.key);
                if (existPvm != null) {
                    cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);
                }
            } else if (pvm.expireAt != CompressedValue.NO_EXPIRE && pvm.expireAt < currentTimeMillis) {
                var existPvm = map.remove(pvm.key);
                if (existPvm != null) {
                    cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);
                }
            } else {
                var existPvm = map.put(pvm.key, pvm);
                // update
                if (existPvm != null) {
                    assert existPvm.seq <= pvm.seq;
                    if (pvm.seq != existPvm.seq) {
                        cvExpiredOrDeleted(existPvm.key, existPvm.extendBytes);
                    }
                }
            }
        }

        // check if need split, repeat until all splits fit or max split number reached
        byte targetSplitNumber = currentSplitNumber;
        while (true) {
            var cellCostBySplitIndex = new int[targetSplitNumber];
            boolean overCapacity = false;
            for (var entry : map.entrySet()) {
                var pvm = entry.getValue();
                var splitIndex = KeyHash.splitIndex(pvm.keyHash, targetSplitNumber, bucketIndex);
                cellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
                if (cellCostBySplitIndex[splitIndex] > KeyBucket.INIT_CAPACITY) {
                    overCapacity = true;
                    break;
                }
            }
            if (!overCapacity) {
                break;
            }

            var newMaxSplitNumber = targetSplitNumber * KeyLoader.SPLIT_MULTI_STEP;
            if (newMaxSplitNumber > KeyLoader.MAX_SPLIT_NUMBER) {
                log.warn("Bucket full, split number exceed max split number=" + KeyLoader.MAX_SPLIT_NUMBER + ", slot={}, bucket index={}",
                        slot, bucketIndex);
                log.warn("Failed keys to put={}", pvmListThisBucket.stream().map(pvm -> pvm.key).collect(Collectors.toList()));
                throw new BucketFullException("Bucket full, split number exceed max split number=" + KeyLoader.MAX_SPLIT_NUMBER +
                        ", slot=" + slot + ", bucket index=" + bucketIndex);
            }
            targetSplitNumber = (byte) newMaxSplitNumber;
        }

        if (targetSplitNumber > currentSplitNumber) {
            if (listList.size() < targetSplitNumber) {
                for (int i = listList.size(); i < targetSplitNumber; i++) {
                    listList.add(prepareListInitWithNull());
                }
            }

            currentSplitNumber = targetSplitNumber;
            splitNumberTmp[relativeBucketIndex] = targetSplitNumber;
            isSplit = true;
        }

        // clear key bucket cells first, then put all
        for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket != null) {
                keyBucket.clearAll();
            } else {
                keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, null, 0, keyLoader.snowFlake);
                list.set(relativeBucketIndex, keyBucket);
            }
        }

        for (var entry : map.entrySet()) {
            var pvm = entry.getValue();
            var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);

            var doPutResult = keyBucket.put(pvm.key, pvm.keyHash, pvm.expireAt, pvm.seq,
                    pvm.extendBytes != null ? pvm.extendBytes : pvm.encode(), false);
            if (!doPutResult.isPut()) {
                log.warn("Failed to put key={}", pvm.key);
                throw new BucketFullException("Bucket full, slot=" + slot + ", bucket index=" + bucketIndex +
                        ", split index=" + splitIndex + ", key=" + pvm.key);
            }
        }

        keyCountForStatsTmp[relativeBucketIndex] = (short) map.size();
    }

    private void cvExpiredOrDeleted(String key, byte[] valueBytes) {
        if (!PersistValueMeta.isPvm(valueBytes)) {
            var shortStringCv = CompressedValue.decode(valueBytes, Wal.keyBytes(key), 0L);
            keyLoader.cvExpiredOrDeletedCallBack.handle(key, shortStringCv);
        } else {
            var pvm = PersistValueMeta.decode(valueBytes);
            keyLoader.cvExpiredOrDeletedCallBack.handle(key, pvm);
        }
    }

    /**
     * @param pvmList the list of PersistValueMeta objects
     */
    void putAllPvmList(@NotNull ArrayList<PersistValueMeta> pvmList) {
        // group by bucket index
        var pvmListGroupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(pvm -> pvm.bucketIndex));
        for (var entry : pvmListGroupByBucketIndex.entrySet()) {
            var bucketIndex = entry.getKey();
            var pvmListThisBucket = entry.getValue();

            putPvmListToTargetBucket(pvmListThisBucket, bucketIndex);
        }
    }

    /**
     * Replaces persisted-value metadata rebuilt from chunk records while keeping short values
     * that live only in key buckets.
     *
     * @param pvmList the rebuilt list of live PersistValueMeta objects
     */
    void replaceAllPvmListPreserveShortValues(@NotNull ArrayList<PersistValueMeta> pvmList) {
        var pvmListGroupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(pvm -> pvm.bucketIndex));
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            var bucketIndex = beginBucketIndex + i;
            var pvmListThisBucket = pvmListGroupByBucketIndex.get(bucketIndex);

            putPvmListToTargetBucket(pvmListThisBucket == null ? List.of() : pvmListThisBucket, bucketIndex, true);
        }
    }

    /**
     * @param shortValueList the list of Wal.V objects
     */
    void putAll(@NotNull Collection<Wal.V> shortValueList) {
        var pvmList = new ArrayList<PersistValueMeta>();
        for (var v : shortValueList) {
            pvmList.add(transferWalV(v));
        }
        putAllPvmList(pvmList);
    }

    /**
     * @param v the Wal.V object
     * @return the PersistValueMeta object
     */
    @VisibleForTesting
    static PersistValueMeta transferWalV(@NotNull Wal.V v) {
        var pvm = new PersistValueMeta();
        pvm.expireAt = v.expireAt();
        pvm.seq = v.seq();
        pvm.key = v.key();
        pvm.keyHash = v.keyHash();
        pvm.bucketIndex = v.bucketIndex();
        pvm.isFromMerge = v.isFromMerge();
        pvm.extendBytes = v.cvEncoded();
        return pvm;
    }
}
