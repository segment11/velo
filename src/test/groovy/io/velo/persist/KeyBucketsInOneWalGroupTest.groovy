package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.KeyHash
import spock.lang.Specification

class KeyBucketsInOneWalGroupTest extends Specification {
    def 'test put all'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        final short slot = 0

        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        and:
        def n = KeyBucket.INIT_CAPACITY + 1
        def shortValueList = Mock.prepareShortValueList(n, (byte) 0) { v ->
            if (v.seq() == KeyBucket.INIT_CAPACITY - 1) {
                // last one set expire time
                def v2 = new Wal.V(v.seq(), 0, v.keyHash(), System.currentTimeMillis() + 1000, CompressedValue.NULL_DICT_SEQ,
                        v.key(), v.cvEncoded(), false)
                return v2
            } else if (v.seq() == KeyBucket.INIT_CAPACITY - 2) {
                def pvm = new PersistValueMeta()
                pvm.shortType = (byte) 0
                pvm.subBlockIndex = (byte) 0
                pvm.segmentIndex = 10
                pvm.segmentOffset = 10
                def v2 = new Wal.V(v.seq(), 0, v.keyHash(), System.currentTimeMillis() + 1000, CompressedValue.NULL_DICT_SEQ,
                        v.key(), pvm.encode(), false)
                return v2
            } else {
                return v
            }
        }

        def shortValueList2 = Mock.prepareShortValueList(n, (byte) 1)
        shortValueList.addAll(shortValueList2)

        when:
        inner.putAll(shortValueList)
        then:
        shortValueList.every {
            inner.getValueX(it.bucketIndex(), it.key(), it.keyHash()).valueBytes() == it.cvEncoded()
        }
        inner.isSplit == (n > KeyBucket.INIT_CAPACITY)
        inner.getValueX(2, 'xxx', 100L) == null

        when:
        def v00 = shortValueList[0]
        then:
        inner.getExpireAtAndSeq(v00.bucketIndex(), v00.key(), v00.keyHash()).expireAt() == CompressedValue.NO_EXPIRE
        inner.getExpireAtAndSeq(2, 'xxx', 100L) == null

        when:
        def sharedBytesList = inner.encodeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp);
        def firstShortValue = shortValueList[0]
        def valueBytesWithExpireAt = keyLoader.getValueXByKey(firstShortValue.bucketIndex(), firstShortValue.key(),
                firstShortValue.keyHash())
        then:
        valueBytesWithExpireAt.valueBytes() == firstShortValue.cvEncoded()

        when:
        // read again
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        then:
        inner2.listList.size() == sharedBytesList.length

        when:
        Thread.sleep(1000 * 2)
        // set the first key to delete
        def v0 = shortValueList[0]
        def vDel = new Wal.V(v0.seq(), v0.bucketIndex(), v0.keyHash(), CompressedValue.EXPIRE_NOW,
                CompressedValue.NULL_DICT_SEQ, v0.key(), v0.cvEncoded(), false)
        shortValueList[0] = vDel
        // set the second key seq changed
        def v1 = shortValueList[1]
        def v11 = new Wal.V(v1.seq() + 1, 0, v1.keyHash(), v1.seq(), v1.spType(),
                v1.key(), v1.cvEncoded(), false)
        shortValueList[1] = v11
        // put again
        inner2.putAll(shortValueList)
        then:
        !inner2.isSplit
        inner2.getValueX(v0.bucketIndex(), v0.key(), v0.keyHash()) == null

        // bucket full
        when:
        def exception = false
        def shortValueListBig = Mock.prepareShortValueList(n * 10, (byte) 0)
        try {
            inner2.putAll(shortValueListBig)
        } catch (BucketFullException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test expired entries are not reinserted during merge'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        final short slot = 0
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        // First round: persist 5 entries, entry 0 has already-expired TTL
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        def shortValueList = Mock.prepareShortValueList(5, (byte) 0) { v ->
            if (v.seq() == 0L) {
                return new Wal.V(v.seq(), v.bucketIndex(), v.keyHash(),
                        System.currentTimeMillis() - 1, CompressedValue.NULL_DICT_SEQ,
                        v.key(), v.cvEncoded(), false)
            }
            return v
        }

        when:
        inner.putAll(shortValueList)
        def sharedBytesList = inner.encodeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        then:
        inner.keyCountForStatsTmp[0] == 5

        when:
        // Second round: reload from disk, merge with empty list for bucket 0
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        def emptyPvmList = []
        inner2.putPvmListToTargetBucket(emptyPvmList, 0)
        then:
        inner2.getValueX(0, shortValueList[0].key(), shortValueList[0].keyHash()) == null
        inner2.keyCountForStatsTmp[0] == 4
        shortValueList[1..4].every {
            inner2.getValueX(it.bucketIndex(), it.key(), it.keyHash()) != null
        }

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test split decision uses cell cost not entry count'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        final short slot = 0
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        // 40-byte keys + short value -> 2+40+1+23 = 66 > 60 -> 2 cells each
        // 25 entries * 2 cells = 50 > 48 (INIT_CAPACITY), but entry count 25 < 48
        def longKeyList = []
        25.times { i ->
            def key = 'long_key_for_multicell_test_' + i.toString().padLeft(16, '0')
            def keyBytes = key.bytes
            def keyHash = KeyHash.hash(keyBytes)
            def cv = new CompressedValue()
            cv.compressedData = ('value' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            longKeyList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }

        when:
        inner.putAll(longKeyList)

        then:
        inner.isSplit
        longKeyList.every {
            inner.getValueX(it.bucketIndex(), it.key(), it.keyHash()).valueBytes() == it.cvEncoded()
        }

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test split rechecked after increasing split number'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        final short slot = 0
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        // Use controlled keyHash values where upper 32 bits are multiples of 3.
        // (keyHash >> 32) % 3 == 0 for all entries -> all cluster in split 0 at splitNumber=3
        // (keyHash >> 32) % 9 varies (3,6) -> spread across splits at splitNumber=9
        // 25 entries * 2 cells = 50 cells:
        //   - splitNumber=1: 50 > 48 -> need split
        //   - splitNumber=3: all in split 0, 50 > 48 -> need another split
        //   - splitNumber=9: ~25 cells per used split < 48 -> fits
        def upperBits = [3L, 6L, 12L, 15L, 21L, 24L]
        def longKeyList = []
        25.times { i ->
            def key = 'long_key_for_recheck_test_' + i.toString().padLeft(16, '0')
            def cv = new CompressedValue()
            cv.compressedData = ('value' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            def keyHash = upperBits[i % upperBits.size()] << 32
            longKeyList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }

        when:
        inner.putAll(longKeyList)

        then:
        inner.isSplit
        longKeyList.every {
            inner.getValueX(it.bucketIndex(), it.key(), it.keyHash()).valueBytes() == it.cvEncoded()
        }

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }
}
