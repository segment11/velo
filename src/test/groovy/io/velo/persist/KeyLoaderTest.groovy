package io.velo.persist

import io.velo.*
import io.velo.repl.incremental.XOneWalGroupPersist
import io.velo.repl.incremental.XOneWalGroupPersistTest
import jnr.ffi.LibraryLoader
import jnr.posix.LibC
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
        keyLoader.statKeyCountInBucketsBytesToSlaveExists.length == bucketsPerSlot * 2
        keyLoader.maxSplitNumberForRepl() == 1
        KeyLoader.getPositionInSharedBytes(0) == 0
        KeyLoader.getPositionInSharedBytes(1) == oneKeyBucketLength
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber) == 0
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber + 2) == oneKeyBucketLength * 2

        when:
        def statKeyCountBytes = new byte[bucketsPerSlot * 2]
        ByteBuffer.wrap(statKeyCountBytes).putShort(0, (short) 1)
        keyLoader.overwriteStatKeyCountInBucketsBytesFromMasterExists(statKeyCountBytes)
        then:
        keyLoader.keyCount == 1
        keyLoader.getKeyCountInBucketIndex(0) == 1

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
        def metaSplitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()
        keyLoader.setMetaOneWalGroupSeq((byte) 0, 0, 1L)
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(metaSplitNumberBytes)
        then:
        metaSplitNumberBytes != null
        keyLoader.getMetaOneWalGroupSeq((byte) 0, 0) == 1L
        metaSplitNumberBytes == keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()

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

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemory = true
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()

        when:
        keyLoader.fdReadWriteArray[0].setSharedBytesFromLastSavedFileToMemory(new byte[1024], 1)
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        keyLoader.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        keyLoader.loadFromLastSavedFileWhenPureMemory(is)
        then:
        keyLoader.fdReadWriteArray[0].allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[1].length == 1024

        when:
        ConfForGlobal.pureMemoryV2 = true
        def keyLoader2 = prepareKeyLoader()
        keyLoader2.putValueByKey(0, 'test'.bytes, 1L, 1, 1L, 1L, new byte[10])
        def bos2 = new ByteArrayOutputStream()
        def os2 = new DataOutputStream(bos2)
        keyLoader2.writeToSavedFileWhenPureMemory(os2)
        def bis2 = new ByteArrayInputStream(bos2.toByteArray())
        def is2 = new DataInputStream(bis2)
        keyLoader2.loadFromLastSavedFileWhenPureMemory(is2)
        then:
        keyLoader2.getExpireAt(0, 'test'.bytes, 1L, 1) == 1L

        cleanup:
        ConfForGlobal.pureMemoryV2 = false
        keyLoader.flush()
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test repl'() {
        given:
        ConfForGlobal.pureMemory = false
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()

        expect:
        keyLoader.maxSplitNumberForRepl() == (byte) 1

        when:
        def metaSplitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(metaSplitNumberBytes)
        then:
        metaSplitNumberBytes != null
        metaSplitNumberBytes == keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()

        cleanup:
        ConfForGlobal.pureMemory = false
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test write and read one key'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()
        println 'in memory size estimate: ' + keyLoader.estimate(new StringBuilder())

        when:
        def expireAt = keyLoader.getExpireAt(0, 'a'.bytes, 10L, 10)
        def expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a'.bytes, 10L, 10)
        def valueBytesX = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        then:
        expireAt == null
        expireAtAndSeq == null
        valueBytesX == null

        when:
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a'.bytes, 10L, 10, 0L, 1L, encodeAsShortStringA)
        expireAt = keyLoader.getExpireAt(0, 'a'.bytes, 10L, 10)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a'.bytes, 10L, 10)
        valueBytesX = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        then:
        expireAt == 0L
        !expireAtAndSeq.isExpired()
        valueBytesX.valueBytes() == encodeAsShortStringA

        when:
        ConfForGlobal.pureMemoryV2 = true
        keyLoader.resetForPureMemoryV2()
        expireAt = keyLoader.getExpireAt(0, 'a'.bytes, 10L, 10)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a'.bytes, 10L, 10)
        valueBytesX = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        then:
        expireAt == null
        expireAtAndSeq == null
        valueBytesX == null

        when:
        keyLoader.putValueByKey(0, 'a'.bytes, 10L, 10, 0L, 1L, encodeAsShortStringA)
        expireAt = keyLoader.getExpireAt(0, 'a'.bytes, 10L, 10)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a'.bytes, 10L, 10)
        valueBytesX = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        then:
        expireAt == 0L
        expireAtAndSeq != null
        expireAtAndSeq.seq() != 0
        valueBytesX != null
        valueBytesX.seq() != 0

        when:
        ConfForGlobal.pureMemoryV2 = false
        def k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, false)
        k0.splitNumber = (byte) 2
        def bytes = k0.encode(true)
        keyLoader.fdReadWriteArray[0].writeOneInner(0, bytes, false)
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 2)
        keyLoader.putValueByKey(0, 'b'.bytes, 11L, 11, 0L, 1L, 'b'.bytes)
        def keyBuckets = keyLoader.readKeyBuckets(0)
        println keyLoader.readKeyBucketsToStringForDebug(0)
        then:
        keyBuckets.size() == 2
        keyBuckets.count {
            if (!it) {
                return false
            }
            it.getValueXByKey('b'.bytes, 11L)?.valueBytes() == 'b'.bytes
        } == 1

        when:
        def isRemoved = keyLoader.removeSingleKey(0, 'a'.bytes, 10L, 10)
        then:
        isRemoved
        keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10) == null

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
        def valueBytesWithExpireAt0 = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        def bytesBatch0 = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved0 = keyLoader.removeSingleKey(0, 'a'.bytes, 10L, 10)
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
        keyLoader.putValueByKey(0, 'a'.bytes, 10L, 10, 0L, 1L, encodeAsShortStringA)
        def valueBytesWithExpireAt = keyLoader.getValueXByKey(0, 'a'.bytes, 10L, 10)
        def bytesBatch = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved = keyLoader.removeSingleKey(0, 'a'.bytes, 10L, 10)
        def isRemoved2 = keyLoader.removeSingleKey(0, 'b'.bytes, 11L, 11)
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

        when:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.pureMemoryV2 = true
        keyLoader.resetForPureMemoryV2()
        def walGroupNumber = Wal.calcWalGroupNumber()
        def splitNumberArray = new byte[oneChargeBucketNumber]
        splitNumberArray[0] = (byte) 3
        keyLoader.metaKeyBucketSplitNumber.setBatch(0, splitNumberArray)
        keyLoader.fdReadWriteArray[0].resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndex(walGroupNumber)
        keyLoader.fdReadWriteArray[1].resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndex(walGroupNumber)
        keyLoader.fdReadWriteArray[2].resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndex(walGroupNumber)
        keyLoader.writeSharedBytesList(sharedBytesListBySplitIndex, 0)
        def nn = keyLoader.updateRecordXBytesArray(XOneWalGroupPersistTest.mockRecordXBytesArray(2))
        def bb = keyLoader.getRecordsBytesArrayInOneWalGroup(0)
        def keyBucketListFromMemory = keyLoader.readKeyBuckets(0)
        then:
        nn == 4
        bb.length == ConfForSlot.global.confWal.oneChargeBucketNumber
        keyBucketListFromMemory.size() == 3
        keyBucketListFromMemory[0] == null
        keyBucketListFromMemory[1] == null
        keyBucketListFromMemory[2] != null
        keyLoader.readKeyBucketForSingleKey(0, (byte) 0, (byte) 3, false) == null
        keyLoader.readKeyBucketForSingleKey(0, (byte) 2, (byte) 3, false) != null

        when:
        keyLoader.intervalRemoveExpired()
        keyLoader.intervalRemoveExpiredLastBucketIndex = ConfForSlot.global.confBucket.bucketsPerSlot - 1
        keyLoader.intervalRemoveExpired()
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemory = false
        ConfForGlobal.pureMemoryV2 = false
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
                def bigStringCvEncoded = bigStringCv.encodeAsBigStringMeta(uuid)

                def v2 = new Wal.V(v.seq(), 0, v.keyHash(), bigStringCv.expireAt, bigStringCv.dictSeqOrSpType,
                        v.key(), bigStringCvEncoded, false)
                return v2
            }
        })

        when:
        def pvm = new PersistValueMeta()
        // put a expired pvm so can callback
        keyLoader.putValueByKey(0, 'normal-key'.bytes, 100L, 100, System.currentTimeMillis() - 1, 100L, pvm.encode())
        def xForBinlog = new XOneWalGroupPersist(true, false, 0)
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList, xForBinlog)
        then:
        shortValueList.every {
            keyLoader.getValueXByKey(0, it.key().bytes, it.keyHash(), KeyHash.hash32(it.key().bytes)).valueBytes() == it.cvEncoded()
        }

        when:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds()
        keyLoader2.initFds((byte) 1)
        keyLoader2.persistShortValueListBatchInOneWalGroup(0, shortValueList, xForBinlog)
        // put again
        keyLoader2.persistShortValueListBatchInOneWalGroup(0, shortValueList, xForBinlog)
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
            pvm.keyBytes = keyBytes
            pvm.keyHash = KeyHash.hash(keyBytes)
            pvm.keyHash32 = KeyHash.hash32(keyBytes)
            pvm.bucketIndex = 0
            pvm.segmentOffset = it
            pvmList << pvm
        }

        when:
        def xForBinlog = new XOneWalGroupPersist(true, false, 0)
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, xForBinlog, null)
        then:
        pvmList.every {
            keyLoader.getValueXByKey(0, it.keyBytes, it.keyHash, it.keyHash32).valueBytes() == it.encode()
        }

        when:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds()
        keyLoader2.initFds((byte) 1)
        keyLoader2.updatePvmListBatchAfterWriteSegments(0, pvmList, xForBinlog, null)
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
        r.scanCursor().isWalIterateEnd()

        when:
        // scan all key buckets until to the end
        ConfForSlot.global.confBucket.onceScanMaxReadCount = ConfForSlot.global.confBucket.initialSplitNumber * Wal.calcWalGroupNumber()
        r = keyLoader.scan(0, (byte) 0, (short) 0, (byte) 0, null, 10, 0L)
        then:
        r.scanCursor() == ScanCursor.END

        when:
        List<PersistValueMeta> pvmList = []
        10.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def pvm = new PersistValueMeta()
            pvm.keyBytes = keyBytes
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
                pvm.keyBytes = 'xxx'.bytes
            }

            // one type not match
            if (it == 7) {
                pvm.shortType = KeyLoader.typeAsByteHash
            }

            pvmList << pvm
        }
        def xForBinlog = new XOneWalGroupPersist(true, false, 0)
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, xForBinlog, null)
        r = keyLoader.scan(0, (byte) 0, (short) 1, KeyLoader.typeAsByteString, 'key:*', 6, 0L)
        then:
        r.keys().size() == 6
        r.keys().every { key ->
            key in pvmList.collect { new String(it.keyBytes) }
        }

        when:
        r = keyLoader.scan(0, (byte) 0, (short) 1, KeyLoader.typeAsByteString, 'key:*', 6, -1L)
        then:
        r.keys().isEmpty()

        when:
        ConfForGlobal.pureMemoryV2 = true
        keyLoader.resetForPureMemoryV2()
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, xForBinlog, null)
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemoryV2 = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
