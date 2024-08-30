package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.KeyHash
import io.velo.SnowFlake
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
        def shortValueList = Mock.prepareShortValueList(n)
        def shortValueList2 = Mock.prepareShortValueList(n, (byte) 1)
        shortValueList.addAll(shortValueList2)

        when:
        inner.putAll(shortValueList)
        then:
        shortValueList.every {
            inner.getValue(it.bucketIndex(), it.key().bytes, it.keyHash()).valueBytes() == it.cvEncoded()
        }
        inner.isSplit == (n > KeyBucket.INIT_CAPACITY)
        inner.getValue(2, 'xxx'.bytes, 100L) == null

        when:
        def sharedBytesList = inner.writeAfterPutBatch()
        // mock one split index not change
        inner.isUpdatedBySplitIndex[2] = false
        def sharedBytesList2 = inner.writeAfterPutBatch()
        inner.isUpdatedBySplitIndex[2] = true
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        def isSplitNumberChanged = keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        def firstShortValue = shortValueList[0]
        def valueBytesWithExpireAt = keyLoader.getValueByKey(firstShortValue.bucketIndex(), firstShortValue.key().bytes, firstShortValue.keyHash())
        then:
        isSplitNumberChanged == inner.isSplit
        valueBytesWithExpireAt.valueBytes() == firstShortValue.cvEncoded()
        sharedBytesList2[2] == null

        when:
        // read again
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        then:
        inner2.listList.size() == sharedBytesList.length

        when:
        // mock one split index not change
        def afterPutOneSplitIndexBytes = sharedBytesList[sharedBytesList.length - 1]
        sharedBytesList[sharedBytesList.length - 1] = new byte[afterPutOneSplitIndexBytes.length]
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        then:
        inner2.listList.size() == sharedBytesList.length
        inner2.listList.last.first.size == 0

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test check if need split'() {
        given:
        final short slot = 0
        ConfForSlot.global.confBucket.initialSplitNumber = 1

        def inner = new KeyBucketsInOneWalGroup(slot, 0, null)
        def snowFlake = new SnowFlake(1, 1)

        and:
        def n = 10
        def shortValueList10 = Mock.prepareShortValueList(n)

        List<PersistValueMeta> needAddNewList = []
        List<PersistValueMeta> needUpdateList = []
        List<PersistValueMeta> needDeleteList = []

        def pvmList10ThisBucket = shortValueList10.collect { v ->
            KeyBucketsInOneWalGroup.transferWalV(v)
        }
        pvmList10ThisBucket[-1].isFromMerge = true
        pvmList10ThisBucket[-2].expireAt = CompressedValue.EXPIRE_NOW

        when:
        final int bucketIndex = 0
        final byte currentSplitNumber = 1
        inner.listList = [[null]]
        def splitMultiStep = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep == 1
        needAddNewList.size() == 9

        when:
        def keyBucket = new KeyBucket(slot, bucketIndex, (byte) 0, (byte) 1, null, snowFlake)
        pvmList10ThisBucket[0..<5].each {
            keyBucket.put(it.keyBytes, it.keyHash, 0L, 0L, it.extendBytes)
        }
        inner.listList = [[keyBucket]]
        needAddNewList.clear()
        def splitMultiStep2 = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep2 == 1
        needAddNewList.size() == 3
        needUpdateList.size() == 5

        when:
        pvmList10ThisBucket[-1].expireAt = CompressedValue.EXPIRE_NOW
        pvmList10ThisBucket[4].expireAt = CompressedValue.EXPIRE_NOW
        needAddNewList.clear()
        needUpdateList.clear()
        needDeleteList.clear()
        def splitMultiStep3 = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep3 == 1
        needAddNewList.size() == 3
        needUpdateList.size() == 4
        needDeleteList.size() == 1

        // corner cases
        when:
        // only one but seq is less than same key in key bucket
        def firstPvm = pvmList10ThisBucket[0]
        firstPvm.seq = 100L
        pvmList10ThisBucket.clear()
        pvmList10ThisBucket << firstPvm
        keyBucket.clearAll()
        keyBucket.put(firstPvm.keyBytes, firstPvm.keyHash, 0L, firstPvm.seq + 1, 'value'.bytes)
        needAddNewList.clear()
        needUpdateList.clear()
        needDeleteList.clear()
        def splitMultiStep4 = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep4 == 1
        needAddNewList.size() == 0
        needUpdateList.size() == 0
        needDeleteList.size() == 0

        when:
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, null)
        inner2.listList = [[null]]
        def n2 = KeyBucket.INIT_CAPACITY * (KeyLoader.SPLIT_MULTI_STEP + 1)
        def shortValueList2 = Mock.prepareShortValueList(n2)
        List<PersistValueMeta> pvmList2 = []
        for (v in shortValueList2) {
            pvmList2.add(KeyBucketsInOneWalGroup.transferWalV(v))
        }
        needAddNewList.clear()
        needUpdateList.clear()
        needDeleteList.clear()
        def splitMultiStep5 = inner2.checkIfNeedSplit(pvmList2, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep5 == 9

        // cell cost too many
        when:
        def n3 = KeyBucket.INIT_CAPACITY
        def shortValueList3 = Mock.prepareShortValueList(n3)
        List<PersistValueMeta> pvmList3 = []
        for (v in shortValueList3) {
            def pvmInner = KeyBucketsInOneWalGroup.transferWalV(v)
            // cost 2 cells
            // key length 16, 2 + 16 + 1 = 60 - 41
            pvmInner.extendBytes = new byte[41 + 1]
            pvmList3.add(pvmInner)
        }
        needAddNewList.clear()
        needUpdateList.clear()
        needDeleteList.clear()
        def splitMultiStep6 = inner2.checkIfNeedSplit(pvmList3, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep6 == 3

        when:
        needAddNewList.clear()
        needUpdateList.clear()
        needDeleteList.clear()
        for (pvmInner in pvmList3) {
            // cost 4 cells
            pvmInner.keyBytes = new byte[100]
            pvmInner.extendBytes = new byte[100]
        }
        def splitMultiStep7 = inner2.checkIfNeedSplit(pvmList3, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)
        then:
        splitMultiStep7 == 9
    }

    def 'test some branches'() {
        given:
        final short slot = 0
        ConfForSlot.global.confBucket.initialSplitNumber = 1

        def keyLoader = KeyLoaderTest.prepareKeyLoader()
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)

        def targetKey = prepareOneKeyInTargetSplitIndex(0, 0)

        when:
        inner.listList.clear()
        inner.listList.add(null)
        then:
        inner.getValue(0, targetKey.bytes, KeyHash.hash(targetKey.bytes)) == null

        when:
        ArrayList<KeyBucket> list = []
        list.add(null)
        inner.listList.set(0, list)
        then:
        inner.getValue(0, targetKey.bytes, KeyHash.hash(targetKey.bytes)) == null

        when:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 3, null, snowFlake)
        list.set(0, keyBucket)
        ('a'..'z').eachWithIndex { it, i ->
            keyBucket.put(it.bytes, 97L + i, 0L, 97L + i, it.bytes)
        }
        def shortValueList = Mock.prepareShortValueList(KeyBucket.INIT_CAPACITY)
        List<PersistValueMeta> pvmList = []
        for (v in shortValueList) {
            pvmList.add(KeyBucketsInOneWalGroup.transferWalV(v))
        }
        inner.putPvmListToTargetBucket(pvmList, 0)
        then:
        inner.getValue(0, 'a'.bytes, 97L).valueBytes() == 'a'.bytes

        when:
        keyBucket.clearAll()
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner2.listList.set(0, list)
        ('a'..'z').eachWithIndex { it, i ->
            keyBucket.put(it.bytes, 97L + i, 0L, 97L + i, it.bytes)
        }
        // mock update
        def firstPvm = pvmList[0]
        keyBucket.put(firstPvm.keyBytes, firstPvm.keyHash, 0L, firstPvm.seq, 'value'.bytes)
        // mock delete
        def lastPvm = pvmList[-1]
        lastPvm.expireAt = CompressedValue.EXPIRE_NOW
        keyBucket.put(lastPvm.keyBytes, lastPvm.keyHash, 0L, lastPvm.seq, lastPvm.extendBytes)
        inner2.putPvmListToTargetBucket(pvmList, 0)
        then:
        inner2.getValue(0, 'a'.bytes, 97L).valueBytes() == 'a'.bytes
        inner2.getValue(0, firstPvm.keyBytes, firstPvm.keyHash).valueBytes() != 'value'.bytes

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test write after put batch'() {
        given:
        final short slot = 0
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        def keyLoader = KeyLoaderTest.prepareKeyLoader()
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)

        when:
        ArrayList<KeyBucket> list = []
        oneChargeBucketNumber.times {
            list.add(null)
        }
        inner.listList.set(0, list)
        inner.isUpdatedBySplitIndex[0] = true
        def sharedBytesList = inner.writeAfterPutBatch()
        then:
        sharedBytesList.length == 1

        when:
        def snowFlake = new SnowFlake(1, 1)
        def sharedBytes = new byte[4096 * oneChargeBucketNumber]
        oneChargeBucketNumber.times { i ->
            def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, sharedBytes, 4096 * i, snowFlake)
            list.set(i, keyBucket)
        }
        sharedBytesList = inner.writeAfterPutBatch()
        then:
        sharedBytesList[0] == sharedBytes
    }

    def 'test split max'() {
        given:
        final short slot = 0

        ConfForSlot.global.confBucket.initialSplitNumber = KeyLoader.MAX_SPLIT_NUMBER / KeyLoader.SPLIT_MULTI_STEP

        def keyLoader = KeyLoaderTest.prepareKeyLoader()
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)

        List<PersistValueMeta> pvmList = []
        (0..<KeyLoader.MAX_SPLIT_NUMBER).each { splitIndex ->
            prepareOneKeyInTargetSplitIndexN(0, splitIndex, KeyLoader.MAX_SPLIT_NUMBER, KeyBucket.INIT_CAPACITY).each { key ->
                def p = new PersistValueMeta()
                p.expireAt = 0L
                p.seq = 100L
                p.keyBytes = key.bytes
                p.keyHash = KeyHash.hash(key.bytes)
                p.bucketIndex = 0
                p.isFromMerge = false
                p.extendBytes = key.bytes

                pvmList << p
            }
        }

        when:
        // use pvm encode bytes
        pvmList[-1].extendBytes = null
        inner.putPvmListToTargetBucket(pvmList, 0)
        then:
        inner.isSplit

        when:
        pvmList.clear()
        def pvm = new PersistValueMeta()
        pvm.expireAt = 0L
        pvm.seq = 97L
        pvm.keyBytes = 'a'.bytes
        pvm.keyHash = KeyHash.hash('a'.bytes)
        pvm.bucketIndex = 0
        pvm.isFromMerge = false
        pvm.extendBytes = 'a'.bytes
        pvmList << pvm
        boolean exception = false
        try {
            inner.putPvmListToTargetBucket(pvmList, 0)
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

    def 'test some corner branches'() {
        given:
        final short slot = 0
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        def snowFlake = new SnowFlake(1, 1)

        when:
        ArrayList<KeyBucket> list = []
        oneChargeBucketNumber.times {
            list.add(null)
        }
        inner.listList = [list]
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)
        list.set(0, keyBucket)
        // hash conflict
        keyBucket.put('a'.bytes, 97L, 0L, 0L, 'a'.bytes)
        def pvm = new PersistValueMeta()
        pvm.expireAt = 0L
        pvm.seq = 97L
        pvm.keyBytes = 'aa'.bytes
        pvm.keyHash = 97L
        pvm.bucketIndex = 0
        pvm.isFromMerge = false
        pvm.extendBytes = 'aa'.bytes
        def pvm2 = new PersistValueMeta()
        pvm2.expireAt = 0L
        pvm2.seq = 97L
        pvm2.keyBytes = 'a'.bytes
        pvm2.keyHash = 97L
        pvm2.bucketIndex = 0
        pvm2.isFromMerge = false
        pvm2.extendBytes = 'a'.bytes
        List<PersistValueMeta> pvmList = []
        pvmList << pvm
        pvmList << pvm2
        def n = KeyBucket.INIT_CAPACITY
        def shortValueList = Mock.prepareShortValueList(n)
        shortValueList.each {
            pvmList << KeyBucketsInOneWalGroup.transferWalV(it)
        }
        inner.splitNumberTmp = new byte[oneChargeBucketNumber]
        inner.splitNumberTmp[0] = 1
        inner.putPvmListToTargetBucket(pvmList, 0)
        then:
        inner.isSplit

        when:
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        ArrayList<KeyBucket> list2 = []
        oneChargeBucketNumber.times {
            list2.add(null)
        }
        inner2.listList = [list2]
        // delete
        pvm.expireAt = CompressedValue.EXPIRE_NOW
        pvmList.clear()
        pvmList << pvm
        // no key bucket
        inner2.putPvmListToTargetBucket(pvmList, 0)
        then:
        !inner2.isUpdatedBySplitIndex[0]

        when:
        // need delete
        def keyBucket2 = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)
        keyBucket2.put('aa'.bytes, 97L, 0L, 0L, 'aa'.bytes)
        list2.set(0, keyBucket2)
        inner2.putPvmListToTargetBucket(pvmList, 0)
        then:
        inner2.isUpdatedBySplitIndex[0]

        when:
        def inner3 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        ArrayList<KeyBucket> list3 = []
        oneChargeBucketNumber.times {
            list3.add(null)
        }
        inner3.listList = [list3]
        inner3.putPvmListToTargetBucketAfterClearAllIfSplit([], [], pvmList, 0)
        then:
        !inner3.isUpdatedBySplitIndex[0]

        when:
        // delete but not exists
        def keyBucket3 = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)
        keyBucket3.put('aaa'.bytes, 97L, 0L, 0L, 'aaa'.bytes)
        list3.set(0, keyBucket3)
        inner3.putPvmListToTargetBucketAfterClearAllIfSplit([], [], pvmList, 0)
        then:
        !inner3.isUpdatedBySplitIndex[0]

        when:
        keyBucket3.put('aa'.bytes, 97L, 0L, 0L, 'aa'.bytes)
        // update
        pvmList[0].expireAt = 0L
        inner3.putPvmListToTargetBucketAfterClearAllIfSplit([], pvmList, [], 0)
        then:
        inner3.isUpdatedBySplitIndex[0]

        when:
        def n2 = KeyBucket.INIT_CAPACITY + 1
        def shortValueList2 = Mock.prepareShortValueList(n2)
        pvmList.clear()
        shortValueList2.each {
            pvmList << KeyBucketsInOneWalGroup.transferWalV(it)
        }
        pvmList[-2].extendBytes = null
        boolean exception = false
        try {
            inner3.putPvmListToTargetBucketAfterClearAllIfSplit(pvmList, [], [], 0)
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

    static ArrayList<String> prepareOneKeyInTargetSplitIndexN(int bucketIndex, int targetSplitIndex, byte splitNumber, int n) {
        ArrayList<String> list = []
        for (i in (0..<(100 * n))) {
            def key = 'key:' + i
            def keyHash = KeyHash.hash(key.bytes)
            if (targetSplitIndex == KeyHash.splitIndex(keyHash, splitNumber, bucketIndex)) {
                list.add(key)
                if (list.size() == n) {
                    break
                }
            }
        }
        list
    }

    static String prepareOneKeyInTargetSplitIndex(int bucketIndex, int targetSplitIndex) {
        def currentSplitNumber = ConfForSlot.global.confBucket.initialSplitNumber
        'key:' + (0..<100).find {
            def key = 'key:' + it
            def keyHash = KeyHash.hash(key.bytes)
            targetSplitIndex == KeyHash.splitIndex(keyHash, currentSplitNumber, bucketIndex)
        }
    }
}
