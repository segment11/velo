package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class representing a group of key buckets in a write-ahead log group (WAL).
 * This class manages the key buckets within a specific slot and a specific WAL group.
 */
public class KeyBucketsInOneWalGroup {
    /**
     * Constructs a KeyBucketsInOneWalGroup instance.
     *
     * @param slot          The slot number to which the key buckets belong.
     * @param walGroupIndex The index of the WAL group.
     * @param keyLoader     The KeyLoader instance used to load key data.
     */
    public KeyBucketsInOneWalGroup(short slot, int walGroupIndex, @NotNull KeyLoader keyLoader) {
        this.slot = slot;
        this.keyLoader = keyLoader;

        this.oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.keyCountForStatsTmp = new short[oneChargeBucketNumber];
        this.beginBucketIndex = oneChargeBucketNumber * walGroupIndex;

        if (!ConfForGlobal.pureMemoryV2) {
            this.readBeforePutBatch();
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
        // for unit test
        if (keyLoader == null) {
            return;
        }

        this.splitNumberTmp = keyLoader.getMetaKeyBucketSplitNumberBatch(beginBucketIndex, oneChargeBucketNumber);
        byte maxSplitNumber = 1;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumber) {
                maxSplitNumber = splitNumberTmp[i];
            }
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

            var sharedBytes = keyLoader.readBatchInOneWalGroup((byte) splitIndex, beginBucketIndex);
            if (sharedBytes == null) {
                continue;
            }

            for (int i = 0; i < oneChargeBucketNumber; i++) {
                var bucketIndex = beginBucketIndex + i;
                var currentSplitNumber = splitNumberTmp[i];
                var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, sharedBytes,
                        KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i, keyLoader.snowFlake);
                keyBucket.cvExpiredOrDeletedCallBack = keyLoader.cvExpiredOrDeletedCallBack;
                // one key bucket max size = KeyBucket.INIT_CAPACITY (48), max split number = 9 or 27, 27 * 48 = 1296 < short max value
                keyCountForStatsTmp[i] += keyBucket.size;
                list.set(i, keyBucket);
            }
        }
    }

    /**
     * Retrieves the expiration time and sequence number for a given key.
     *
     * @param bucketIndex The index of the bucket.
     * @param key         The key.
     * @param keyHash     The hash of the key.
     * @return The expiration time and sequence number.
     */
    KeyBucket.ExpireAtAndSeq getExpireAtAndSeq(int bucketIndex, String key, long keyHash) {
        if (ConfForGlobal.pureMemoryV2) {
            return keyLoader.getExpireAtAndSeqByKey(bucketIndex, key, keyHash, KeyHash.hash32(key.getBytes()));
        }

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
     * Retrieves the value, expiration time, and sequence number for a given key.
     *
     * @param bucketIndex The index of the bucket.
     * @param key         The key.
     * @param keyHash     The hash of the key.
     * @return The value, expiration time, and sequence number.
     */
    KeyBucket.ValueBytesWithExpireAtAndSeq getValueX(int bucketIndex, String key, long keyHash) {
        if (ConfForGlobal.pureMemoryV2) {
            return keyLoader.getValueXByKey(bucketIndex, key, keyHash, KeyHash.hash32(key.getBytes()));
        }

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
     * Writes changes to the key buckets and returns the updated shared bytes.
     *
     * @return The updated shared bytes.
     */
    byte[][] writeAfterPutBatch() {
        byte maxSplitNumberTmp = 1;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumberTmp) {
                maxSplitNumberTmp = splitNumberTmp[i];
            }
        }

        var sharedBytesList = new byte[maxSplitNumberTmp][];

        for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
            if (!isUpdatedBySplitIndex[splitIndex]) {
                continue;
            }

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

    @VisibleForTesting
    final boolean[] isUpdatedBySplitIndex = new boolean[KeyLoader.MAX_SPLIT_NUMBER];

    /**
     * Puts a list of PersistValueMeta objects into the target bucket after clearing it if split is needed.
     *
     * @param needAddNewList List of PersistValueMeta objects to add.
     * @param needUpdateList List of PersistValueMeta objects to update.
     * @param needDeleteList List of PersistValueMeta objects to delete.
     * @param bucketIndex    The index of the bucket.
     */
    @VisibleForTesting
    void putPvmListToTargetBucketAfterClearAllIfSplit(@NotNull List<PersistValueMeta> needAddNewList,
                                                      @NotNull List<PersistValueMeta> needUpdateList,
                                                      @NotNull List<PersistValueMeta> needDeleteList,
                                                      Integer bucketIndex) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        // if split, current split number is new split number
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];

        needAddNewList.addAll(needUpdateList);
        for (var pvm : needAddNewList) {
            if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
                continue;
            }

            var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                keyBucket = new KeyBucket(slot, bucketIndex, splitIndex, currentSplitNumber, null, 0, keyLoader.snowFlake);
                keyBucket.cvExpiredOrDeletedCallBack = keyLoader.cvExpiredOrDeletedCallBack;
                list.set(relativeBucketIndex, keyBucket);
            }

            var doPutResult = keyBucket.put(pvm.key, pvm.keyHash, pvm.expireAt, pvm.seq,
                    pvm.extendBytes != null ? pvm.extendBytes : pvm.encode(), false);
            if (!doPutResult.isPut()) {
                // log all keys
                log.warn("Failed keys to put={}", needAddNewList.stream().map(pvmInner -> pvmInner.key).collect(Collectors.toList()));
                throw new BucketFullException("Bucket full, slot=" + slot + ", bucket index=" + bucketIndex +
                        ", split index=" + splitIndex + ", key=" + pvm.key);
            }

            isUpdatedBySplitIndex[splitIndex] = true;
            if (!doPutResult.isUpdate()) {
                keyCountForStatsTmp[relativeBucketIndex]++;
            }
        }

        for (var pvm : needDeleteList) {
            var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                continue;
            }

            var isDeleted = keyBucket.del(pvm.key, pvm.keyHash, true);
            if (isDeleted) {
                isUpdatedBySplitIndex[splitIndex] = true;
                keyCountForStatsTmp[relativeBucketIndex]--;
            }
        }
    }

    /**
     * Puts a list of PersistValueMeta objects into the target bucket.
     *
     * @param pvmListThisBucket List of PersistValueMeta objects.
     * @param bucketIndex       The index of the bucket.
     */
    @VisibleForTesting
    void putPvmListToTargetBucket(@NotNull List<PersistValueMeta> pvmListThisBucket, Integer bucketIndex) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];

        List<PersistValueMeta> needAddNewList = new ArrayList<>();
        List<PersistValueMeta> needDeleteList = new ArrayList<>();
        List<PersistValueMeta> needUpdateList = new ArrayList<>();

        var splitMultiStep = checkIfNeedSplit(pvmListThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber);
        if (splitMultiStep > 1) {
            var newMaxSplitNumber = currentSplitNumber * splitMultiStep;
            if (newMaxSplitNumber > KeyLoader.MAX_SPLIT_NUMBER) {
                log.warn("Bucket full, split number exceed max split number=" + KeyLoader.MAX_SPLIT_NUMBER + ", slot={}, bucket index={}",
                        slot, bucketIndex);
                // log all keys
                log.warn("Failed keys to put={}", pvmListThisBucket.stream().map(pvm -> pvm.key).collect(Collectors.toList()));
                throw new BucketFullException("Bucket full, split number exceed max split number=" + KeyLoader.MAX_SPLIT_NUMBER +
                        ", slot=" + slot + ", bucket index=" + bucketIndex);
            }

            if (listList.size() < newMaxSplitNumber) {
                for (int i = listList.size(); i < newMaxSplitNumber; i++) {
                    listList.add(prepareListInitWithNull());
//                    assert listList.size() == i + 1;
                }
            }
            splitNumberTmp[relativeBucketIndex] = (byte) newMaxSplitNumber;

            // rehash
            List<PersistValueMeta> existsWithoutNeedUpdatePvmList = new ArrayList<>();
            for (var list : listList) {
                var keyBucket = list.get(relativeBucketIndex);
                if (keyBucket == null) {
                    continue;
                }

                keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
                    if (!needUpdateList.isEmpty()) {
                        for (var needUpdatePvm : needUpdateList) {
                            if (needUpdatePvm.keyHash == keyHash && needUpdatePvm.key.equals(key)) {
                                return;
                            }
                        }
                    }

                    var pvm = new PersistValueMeta();
                    pvm.expireAt = expireAt;
                    pvm.seq = seq;
                    pvm.key = key;
                    pvm.keyHash = keyHash;
                    pvm.bucketIndex = bucketIndex;
                    pvm.extendBytes = valueBytes;
                    existsWithoutNeedUpdatePvmList.add(pvm);
                });
            }
            needAddNewList.addAll(existsWithoutNeedUpdatePvmList);

            // clear all and then re-put
            for (var list : listList) {
                var keyBucket = list.get(relativeBucketIndex);
                if (keyBucket != null) {
                    keyBucket.clearAll();
                }
            }
            keyCountForStatsTmp[relativeBucketIndex] = 0;

            isSplit = true;
        }

        putPvmListToTargetBucketAfterClearAllIfSplit(needAddNewList, needUpdateList, needDeleteList, bucketIndex);
    }

    /**
     * Checks if the bucket needs to be split based on the given PersistValueMeta list.
     *
     * @param pvmListThisBucket  List of PersistValueMeta objects.
     * @param needAddNewList     List to store keys that need to be added.
     * @param needUpdateList     List to store keys that need to be updated.
     * @param needDeleteList     List to store keys that need to be deleted.
     * @param bucketIndex        The index of the bucket.
     * @param currentSplitNumber The current split number of the bucket.
     * @return The number of splits needed.
     */
    @VisibleForTesting
    int checkIfNeedSplit(@NotNull List<PersistValueMeta> pvmListThisBucket,
                         @NotNull List<PersistValueMeta> needAddNewList,
                         @NotNull List<PersistValueMeta> needUpdateList,
                         @NotNull List<PersistValueMeta> needDeleteList,
                         int bucketIndex,
                         byte currentSplitNumber) {
        var relativeBucketIndex = bucketIndex - beginBucketIndex;

        int currentTotalKeyCountThisBucket = 0;
        int currentTotalCellCostThisBucket = 0;
        int[] existsKeyCountBySplitIndex = new int[currentSplitNumber];
        int[] existsCellCostBySplitIndex = new int[currentSplitNumber];
        for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                continue;
            }

            keyBucket.clearAllExpired();
            currentTotalKeyCountThisBucket += keyBucket.size;
            currentTotalCellCostThisBucket += keyBucket.cellCost;

            existsKeyCountBySplitIndex[splitIndex] = keyBucket.size;
            existsCellCostBySplitIndex[splitIndex] = keyBucket.cellCost;
        }

        int[] needAddKeyCountBySplitIndex = new int[currentSplitNumber];
        int[] needDeleteKeyCountBySplitIndex = new int[currentSplitNumber];
        int[] needAddCellCostBySplitIndex = new int[currentSplitNumber];
        int[] needDeleteCellCostBySplitIndex = new int[currentSplitNumber];

        for (var pvm : pvmListThisBucket) {
            var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                if (!pvm.isFromMerge) {
                    needAddNewList.add(pvm);

                    needAddKeyCountBySplitIndex[splitIndex]++;
                    needAddCellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
                }
                continue;
            }

            var currentOne = keyBucket.getValueXByKey(pvm.key, pvm.keyHash);
            if (currentOne != null) {
                // wal remove delay use expire now
                if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
                    needDeleteList.add(pvm);

                    needDeleteKeyCountBySplitIndex[splitIndex]++;
                    needDeleteCellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
                    continue;
                }

                // pvm list include those from merge, so need check seq
                if (pvm.seq >= currentOne.seq()) {
                    needUpdateList.add(pvm);
                }
            } else {
                if (pvm.isFromMerge) {
                    continue;
                }

                if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
                    // not exists
                    continue;
                }

                // not exists
                needAddNewList.add(pvm);

                needAddKeyCountBySplitIndex[splitIndex]++;
                needAddCellCostBySplitIndex[splitIndex] += pvm.cellCostInKeyBucket();
            }
        }

        var canPutKeyCountThisBucket = KeyBucket.INIT_CAPACITY * currentSplitNumber;

        var newKeyCountNeedThisBucket = currentTotalKeyCountThisBucket + needAddNewList.size() - needDeleteList.size();
        int newCellCostNeedThisBucket = currentTotalCellCostThisBucket;
        for (var pvm : needAddNewList) {
            newCellCostNeedThisBucket += pvm.cellCostInKeyBucket();
        }
        for (var pvm : needDeleteList) {
            newCellCostNeedThisBucket -= pvm.cellCostInKeyBucket();
        }

        final int tolerance = KeyLoader.KEY_OR_CELL_COST_TOLERANCE_COUNT_WHEN_CHECK_SPLIT;

        int splitMultiStep = KeyLoader.SPLIT_MULTI_STEP;
        var needSplit = false;
        if (newKeyCountNeedThisBucket > canPutKeyCountThisBucket - tolerance) {
            needSplit = true;
            if (newKeyCountNeedThisBucket > canPutKeyCountThisBucket * KeyLoader.SPLIT_MULTI_STEP) {
                splitMultiStep *= KeyLoader.SPLIT_MULTI_STEP;
                log.warn("Bucket split once 2 times 1 -> 9 for slot={}, bucket index={}, once add key count={}", slot, bucketIndex, newKeyCountNeedThisBucket);
            }
        } else if (newCellCostNeedThisBucket > canPutKeyCountThisBucket - tolerance) {
            needSplit = true;
            if (newCellCostNeedThisBucket > canPutKeyCountThisBucket * KeyLoader.SPLIT_MULTI_STEP) {
                splitMultiStep *= KeyLoader.SPLIT_MULTI_STEP;
                log.warn("Bucket split once 2 times 1 -> 9 for slot={}, bucket index={}, once add cell cost={}", slot, bucketIndex, newCellCostNeedThisBucket);
            }
        }

        if (!needSplit) {
            // compare by each split index
            for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
                var existsKeyCount = existsKeyCountBySplitIndex[splitIndex];
                var needAddKeyCount = needAddKeyCountBySplitIndex[splitIndex];
                var needDeleteKeyCount = needDeleteKeyCountBySplitIndex[splitIndex];
                if (existsKeyCount + needAddKeyCount - needDeleteKeyCount > KeyBucket.INIT_CAPACITY - tolerance) {
                    needSplit = true;
                    // split number * 3 can cover ? need not check, because wal group once number is not too large
                    break;
                }

                var existsCellCost = existsCellCostBySplitIndex[splitIndex];
                var needAddCellCost = needAddCellCostBySplitIndex[splitIndex];
                var needDeleteCellCost = needDeleteCellCostBySplitIndex[splitIndex];
                // delete cell count is not correct, as one key length may be too lange, deleted two keys cell cost is smaller than added one key cell cost
                // fix this, todo
                if (existsCellCost + needAddCellCost - needDeleteCellCost > KeyBucket.INIT_CAPACITY - tolerance) {
                    needSplit = true;
                    // split number * 3 can cover ? need not check, because wal group once number is not too large
                    break;
                }
            }
        }

        if (needSplit) {
            return splitMultiStep;
        } else {
            return 1;
        }
    }

    /**
     * Puts a list of PersistValueMeta objects.
     *
     * @param pvmList List of PersistValueMeta objects.
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
     * Puts a list of Wal.V objects.
     *
     * @param shortValueList List of Wal.V objects.
     */
    void putAll(@NotNull Collection<Wal.V> shortValueList) {
        var pvmList = new ArrayList<PersistValueMeta>();
        for (var v : shortValueList) {
            pvmList.add(transferWalV(v));
        }
        putAllPvmList(pvmList);
    }

    /**
     * Transfer Wal.V to PersistValueMeta
     *
     * @param v Wal.V object
     * @return PersistValueMeta object
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
