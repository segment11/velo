package io.velo.persist;

import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
     * @param slot          the slot index to which the key buckets belong
     * @param walGroupIndex the index of the WAL group
     * @param keyLoader     the KeyLoader instance used to load key data
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
     * @param bucketIndex the index of the bucket
     * @param key         the key
     * @param keyHash     the hash of the key
     * @return the expiration time and sequence number
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
     * @param bucketIndex the index of the bucket
     * @param key         the key
     * @param keyHash     the hash of the key
     * @return the value, expiration time, and sequence number
     */
    @TestOnly
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
     * Encode the key buckets and returns the updated shared bytes.
     *
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
     * Puts a list of PersistValueMeta objects into the target bucket.
     *
     * @param pvmListThisBucket the list of PersistValueMeta objects
     * @param bucketIndex       the index of the bucket
     */
    @VisibleForTesting
    void putPvmListToTargetBucket(@NotNull List<PersistValueMeta> pvmListThisBucket, Integer bucketIndex) {
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
                if (expireAt != CompressedValue.NO_EXPIRE && expireAt < currentTimeMillis) {
                    cvExpiredOrDeleted(key, valueBytes);
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

        // check if need split
        boolean needSplit = false;
        var cellCostBySplitIndex = new int[currentSplitNumber];
        for (var entry : map.entrySet()) {
            var pvm = entry.getValue();

            var splitIndex = KeyHash.splitIndex(pvm.keyHash, currentSplitNumber, bucketIndex);
            cellCostBySplitIndex[splitIndex]++;
            if (cellCostBySplitIndex[splitIndex] > KeyBucket.INIT_CAPACITY) {
                needSplit = true;
                break;
            }
        }

        if (needSplit) {
            var newMaxSplitNumber = currentSplitNumber * KeyLoader.SPLIT_MULTI_STEP;
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

            currentSplitNumber = (byte) newMaxSplitNumber;
            splitNumberTmp[relativeBucketIndex] = (byte) newMaxSplitNumber;
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
            var shortStringCv = CompressedValue.decode(Unpooled.wrappedBuffer(valueBytes), key.getBytes(), 0L);
            keyLoader.cvExpiredOrDeletedCallBack.handle(key, shortStringCv);
        } else {
            var pvm = PersistValueMeta.decode(valueBytes);
            keyLoader.cvExpiredOrDeletedCallBack.handle(key, pvm);
        }
    }

    /**
     * Puts a list of PersistValueMeta objects.
     *
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
     * Puts a list of Wal.V objects.
     *
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
     * Transfer Wal.V to PersistValueMeta
     *
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
