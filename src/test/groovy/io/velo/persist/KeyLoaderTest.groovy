package io.velo.persist

import io.velo.*
import spock.lang.Specification

import java.nio.ByteBuffer

class KeyLoaderTest extends Specification {
    static KeyLoader prepareKeyLoader(boolean deleteFiles = true) {
        if (deleteFiles && Consts.slotDir.exists()) {
            for (f in Consts.slotDir.listFiles()) {
                if (f.name.startsWith('key-bucket-split-') || f.name.startsWith('meta_key_bucket_split_number')) {
                    f.delete()
                }
            }
        }

        def snowFlake = new SnowFlake(1, 1)

        short slot = 0
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, snowFlake)
        // do nothing, just for test coverage
        keyLoader.cleanUp()
        keyLoader.keyCount

        keyLoader.initFds()
        keyLoader.initFds((byte) 1)
        keyLoader
    }

    final short slot = 0
    final short slotNumber = 1
    final byte splitIndex = 0

    def 'test base'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()
        def bucketsPerSlot = keyLoader.bucketsPerSlot
        def oneKeyBucketLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber
        println keyLoader
        println 'in memory size estimate: ' + keyLoader.estimate(new StringBuilder())
        keyLoader.collect()

        expect:
        !keyLoader.isBytesValidAsKeyBucket(null, 0)
        !keyLoader.isBytesValidAsKeyBucket(new byte[8], 0)
        keyLoader.keyCount == 0
        keyLoader.getKeyCountInBucketIndex(0) == 0
        KeyLoader.getPositionInSharedBytes(0) == 0
        KeyLoader.getPositionInSharedBytes(1) == oneKeyBucketLength
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber) == 0
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber + 2) == oneKeyBucketLength * 2

        when:
        def exception = false
        try {
            keyLoader.getKeyCountInBucketIndex(-1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.getKeyCountInBucketIndex(bucketsPerSlot)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def splitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBatch(0, oneChargeBucketNumber)
        then:
        splitNumberBytes.length == oneChargeBucketNumber

        when:
        exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(-1, 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(bucketsPerSlot, 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(-1, new byte[0])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(bucketsPerSlot, new byte[0])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def splitNumberArray = new byte[1]
        splitNumberArray[0] = (byte) 3
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(0, splitNumberArray)
        then:
        keyLoader.metaKeyBucketSplitNumber.get(0) == (byte) 3
        !keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(0, splitNumberArray)

        when:
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 1)
        then:
        keyLoader.metaKeyBucketSplitNumber.get(0) == (byte) 1

        when:
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumber(-1, (byte) 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumber(keyLoader.bucketsPerSlot, (byte) 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        short[] keyCountArray = new short[2]
        keyCountArray[0] = (short) 1
        keyCountArray[1] = (short) 2
        keyLoader.updateKeyCountBatch(0, 0, keyCountArray)
        then:
        keyLoader.getKeyCountInBucketIndex(0) == 1
        keyLoader.getKeyCountInBucketIndex(1) == 2

        when:
        exception = false
        try {
            keyLoader.updateKeyCountBatch(0, -1, new short[1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.updateKeyCountBatch(0, bucketsPerSlot, new short[1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test write and read one key'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()
        println 'in memory size estimate: ' + keyLoader.estimate(new StringBuilder())

        when:
        def expireAt = keyLoader.getExpireAt(0, 'a', 10L, 10)
        def expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a', 10L, 10)
        def valueBytesX = keyLoader.getValueXByKey(0, 'a', 10L, 10)
        then:
        expireAt == null
        expireAtAndSeq == null
        valueBytesX == null

        when:
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a', 10L, 10, 0L, 1L, encodeAsShortStringA)
        expireAt = keyLoader.getExpireAt(0, 'a', 10L, 10)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a', 10L, 10)
        valueBytesX = keyLoader.getValueXByKey(0, 'a', 10L, 10)
        then:
        expireAt == 0L
        !expireAtAndSeq.isExpired()
        valueBytesX.valueBytes() == encodeAsShortStringA

        when:
        def k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, false)
        k0.splitNumber = (byte) 2
        def bytes = k0.encode(true)
        keyLoader.fdReadWriteArray[0].writeOneInner(0, bytes, false)
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 2)
        keyLoader.putValueByKey(0, 'b', 11L, 11, 0L, 1L, 'b'.bytes)
        def keyBuckets = keyLoader.readKeyBuckets(0)
        println keyLoader.readKeyBucketsToStringForDebug(0)
        then:
        keyBuckets.size() == 2
        keyBuckets.count {
            if (!it) {
                return false
            }
            it.getValueXByKey('b', 11L)?.valueBytes() == 'b'.bytes
        } == 1

        when:
        def isRemoved = keyLoader.removeSingleKey(0, 'a', 10L, 10)
        then:
        isRemoved
        keyLoader.getValueXByKey(0, 'a', 10L, 10) == null

        when:
        def n = keyLoader.warmUp()
        then:
        n >= 0

        cleanup:
        println 'in memory size estimate: ' + keyLoader.estimate(new StringBuilder())
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test some branches'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1

        def keyLoader = prepareKeyLoader()
        def oneKeyBucketLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        when:
        // never write yet
        def keyBucketsOverFdWriteIndex = keyLoader.readKeyBuckets(0)
        then:
        keyBucketsOverFdWriteIndex[0] == null

        when:
        def rawFdReadWrite = keyLoader.fdReadWriteArray[0]
        keyLoader.fdReadWriteArray[0] = null
        def keyBuckets = keyLoader.readKeyBuckets(0)
        def valueBytesWithExpireAt0 = keyLoader.getValueXByKey(0, 'a', 10L, 10)
        def bytesBatch0 = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved0 = keyLoader.removeSingleKey(0, 'a', 10L, 10)
        then:
        keyBuckets[0] == null
        valueBytesWithExpireAt0 == null
        bytesBatch0 == null
        !isRemoved0

        when:
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def keyBucket = new KeyBucket(slot, 0, splitIndex, (byte) 1, null, keyLoader.snowFlake)
        rawFdReadWrite.writeOneInner(0, keyBucket.encode(true), false)
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a', 10L, 10, 0L, 1L, encodeAsShortStringA)
        def valueBytesWithExpireAt = keyLoader.getValueXByKey(0, 'a', 10L, 10)
        def bytesBatch = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved = keyLoader.removeSingleKey(0, 'a', 10L, 10)
        def isRemoved2 = keyLoader.removeSingleKey(0, 'b', 11L, 11)
        then:
        valueBytesWithExpireAt.valueBytes() == encodeAsShortStringA
        bytesBatch != null
        isRemoved
        !isRemoved2

        when:
        keyLoader.fdReadWriteArray = new FdReadWrite[2]
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def sharedBytesListBySplitIndex = new byte[3][]
        def sharedBytes0 = new byte[oneChargeBucketNumber * oneKeyBucketLength]
        def sharedBytes2 = new byte[oneChargeBucketNumber * oneKeyBucketLength]
        // mock split index = 2, bucket index 0, is valid key bucket
        ByteBuffer.wrap(sharedBytes2).putLong(3L)
        sharedBytesListBySplitIndex[0] = sharedBytes0
        sharedBytesListBySplitIndex[1] = null
        sharedBytesListBySplitIndex[2] = sharedBytes2
        keyLoader.writeSharedBytesList(sharedBytesListBySplitIndex, 0)
        then:
        keyLoader.fdReadWriteArray[1] != null
        keyLoader.readKeyBucketForSingleKey(0, (byte) 1, (byte) 3, false) == null

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test interval delete big string files'() {
        given:
        def keyLoader = prepareKeyLoader()

        when:
        keyLoader.intervalDeleteExpiredBigStringFiles()
        keyLoader.intervalDeleteExpiredBigStringFilesLastBucketIndex = ConfForSlot.global.confBucket.bucketsPerSlot - 1
        keyLoader.intervalDeleteExpiredBigStringFiles()
        then:
        1 == 1

        when:
        def sKey = BaseCommand.slot('1234', slotNumber)
        def cvBigString = new CompressedValue()
        cvBigString.seq = 1234L
        cvBigString.keyHash = sKey.keyHash()
        cvBigString.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvBigString.setCompressedDataAsBigString(1234L, CompressedValue.NULL_DICT_SEQ)
        keyLoader.putValueByKey(0, '1234', sKey.keyHash(), sKey.keyHash32(), 0L, 1234L, cvBigString.encode())
        then:
        keyLoader.getPersistedBigStringIdList(0).size() == 1

        when:
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a', 10L, 10, 0L, 1L, encodeAsShortStringA)
        def cv = new CompressedValue()
        cv.keyHash = 11L
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(1234L, Dict.SELF_ZSTD_DICT_SEQ)
        def bigStringCvEncoded = cv.encode()
        keyLoader.putValueByKey(0, 'b', 11L, 11, System.currentTimeMillis() - 1000, 11L, bigStringCvEncoded)
        int count = keyLoader.intervalDeleteExpiredBigStringFiles()
        then:
        count == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'persist short value list'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        def shortValueList = Mock.prepareShortValueList(10, 0, { v ->
            if (v.seq() != 9) {
                return v
            } else {
                // last one expired, and set type big string, so can callback
                def uuid = keyLoader.snowFlake.nextId()
                // skip write to file

                def bigStringCv = new CompressedValue()
                bigStringCv.seq = v.seq()
                bigStringCv.keyHash = v.keyHash()
                bigStringCv.expireAt = System.currentTimeMillis() - 1
                bigStringCv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
                bigStringCv.setCompressedDataAsBigString(uuid, Dict.SELF_ZSTD_DICT_SEQ)
                def bigStringCvEncoded = bigStringCv.encode()

                def v2 = new Wal.V(v.seq(), 0, v.keyHash(), bigStringCv.expireAt, bigStringCv.dictSeqOrSpType,
                        v.key(), bigStringCvEncoded, false)
                return v2
            }
        })

        when:
        def pvm = new PersistValueMeta()
        // put a expired pvm so can callback
        keyLoader.putValueByKey(0, 'normal-key', 100L, 100, System.currentTimeMillis() - 1, 100L, pvm.encode())
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        shortValueList.every {
            if (it.isExpired()) {
                return true
            }
            keyLoader.getValueXByKey(0, it.key(), it.keyHash(), KeyHash.hash32(it.key().bytes)).valueBytes() == it.cvEncoded()
        }

        when:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds()
        keyLoader2.initFds((byte) 1)
        keyLoader2.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        // put again
        keyLoader2.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        1 == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        keyLoader2.flush()
        keyLoader2.cleanUp()
    }

    def 'persist pvm list'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        List<PersistValueMeta> pvmList = []
        10.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def pvm = new PersistValueMeta()
            pvm.key = key
            pvm.keyHash = KeyHash.hash(keyBytes)
            pvm.keyHash32 = KeyHash.hash32(keyBytes)
            pvm.bucketIndex = 0
            pvm.segmentOffset = it
            pvmList << pvm
        }

        when:
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, null)
        then:
        pvmList.every {
            keyLoader.getValueXByKey(0, it.key, it.keyHash, it.keyHash32).valueBytes() == it.encode()
        }

        when:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds()
        keyLoader2.initFds((byte) 1)
        keyLoader2.updatePvmListBatchAfterWriteSegments(0, pvmList, null)
        then:
        1 == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        keyLoader2.flush()
        keyLoader2.cleanUp()
    }

    def 'test scan'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 3

        expect:
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_HASH) == KeyLoader.typeAsByteHash
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_LIST) == KeyLoader.typeAsByteList
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_SET) == KeyLoader.typeAsByteSet
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_ZSET) == KeyLoader.typeAsByteZSet
        KeyLoader.transferToShortType(CompressedValue.NULL_DICT_SEQ) == KeyLoader.typeAsByteString
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_GEO) == KeyLoader.typeAsByteString
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_BLOOM_BITMAP) == KeyLoader.typeAsByteString
        KeyLoader.transferToShortType(CompressedValue.SP_TYPE_STREAM) == KeyLoader.typeAsByteStream
        KeyLoader.transferToShortType(Integer.MIN_VALUE) == KeyLoader.typeAsByteIgnore
        KeyLoader.isKeyMatch('aaa', null)
        KeyLoader.isKeyMatch('aaa', '*')
        KeyLoader.isKeyMatch('aaa', 'aaa')
        KeyLoader.isKeyMatch('aaa', 'aa*')
        KeyLoader.isKeyMatch('aaa', '*aa')
        KeyLoader.isKeyMatch('abc', '*b*')
        KeyLoader.isKeyMatch('abc', 'ab*')
        !KeyLoader.isKeyMatch('aaa', 'bbb')

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader

        when:
        // no keys put yet
        def r = keyLoader.scan(0, (byte) 0, (short) 0, (byte) 0, null, 10, 0L)
        then:
        // only one slot
        r.scanCursor() == ScanCursor.END

        when:
        // scan all key buckets until to the end
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = ConfForSlot.global.confBucket.initialSplitNumber * Wal.calcWalGroupNumber()
        r = keyLoader.scan(0, (byte) 0, (short) 0, (byte) 0, null, 10, 0L)
        then:
        r.scanCursor() == ScanCursor.END

        when:
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = 1024
        r = keyLoader.scan(0, (byte) 0, (short) 0, (byte) 0, null, 10, 0L)
        then:
        r.scanCursor() == ScanCursor.END

        when:
        List<PersistValueMeta> pvmList = []
        10.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def pvm = new PersistValueMeta()
            pvm.key = key
            pvm.keyHash = KeyHash.hash(keyBytes)
            pvm.keyHash32 = KeyHash.hash32(keyBytes)
            pvm.bucketIndex = 0
            pvm.segmentOffset = it

            // last one expired, or will clear when put
            if (it == 9) {
                pvm.expireAt = System.currentTimeMillis() - 1000
            }

            // one key not prefix match
            if (it == 6) {
                pvm.key = 'xxx'
            }

            // one type not match
            if (it == 7) {
                pvm.shortType = KeyLoader.typeAsByteHash
            }

            pvmList << pvm
        }
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, null)
        r = keyLoader.scan(0, (byte) 0, (short) 1, KeyLoader.typeAsByteString, 'key:*', 6, 0L)
        then:
        r.keys().size() == 6
        r.keys().every { key ->
            key in pvmList.collect { it.key }
        }

        when:
        // skip first split index
        r = keyLoader.scan(0, (byte) 1, (short) 1, KeyLoader.typeAsByteString, 'key:*', 6, 0L)
        then:
        !r.keys().isEmpty()
        r.keys().size() < 6

        when:
        // scan seq -1 means skip all keys as there seq is > -1
        r = keyLoader.scan(0, (byte) 0, (short) 1, KeyLoader.typeAsByteString, 'key:*', 6, -1L)
        then:
        r.keys().isEmpty()

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
