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
        def isSplitNumberChanged = keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        def firstShortValue = shortValueList[0]
        def valueBytesWithExpireAt = keyLoader.getValueXByKey(firstShortValue.bucketIndex(), firstShortValue.key(),
                firstShortValue.keyHash(), KeyHash.hash32(firstShortValue.key().bytes))
        then:
        isSplitNumberChanged == inner.isSplit
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
}
