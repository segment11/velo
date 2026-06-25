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
        def keyCountSkew = keyLoader.describeKeyCountSkew()
        then:
        keyCountSkew.contains('key_bucket_key_count_total=3')
        keyCountSkew.contains('key_bucket_key_count_max=2')
        keyCountSkew.contains('key_bucket_non_empty_count=2')
        keyCountSkew.contains('calc_cost_ms=')

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
        def expireAt = keyLoader.getExpireAt(0, 'a', 10L)
        def expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a', 10L)
        def valueBytesX = keyLoader.getValueXByKey(0, 'a', 10L)
        then:
        expireAt == null
        expireAtAndSeq == null
        valueBytesX == null

        when:
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a', 10L, 0L, 1L, encodeAsShortStringA)
        expireAt = keyLoader.getExpireAt(0, 'a', 10L)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'a', 10L)
        valueBytesX = keyLoader.getValueXByKey(0, 'a', 10L)
        then:
        expireAt == 0L
        !expireAtAndSeq.isExpired()
        valueBytesX.valueBytes() == encodeAsShortStringA

        when:
        keyLoader.putValueByKey(0, 'expired', 12L, System.currentTimeMillis() - 1000, 2L, 'expired'.bytes)
        expireAt = keyLoader.getExpireAt(0, 'expired', 12L)
        expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(0, 'expired', 12L)
        valueBytesX = keyLoader.getValueXByKey(0, 'expired', 12L)
        then:
        expireAt == null
        expireAtAndSeq == null
        valueBytesX.isExpired()

        when:
        def k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, false)
        k0.splitNumber = (byte) 2
        def bytes = k0.encode(true)
        keyLoader.fdReadWriteArray[0].writeOneInner(0, bytes, false)
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 2)
        keyLoader.putValueByKey(0, 'b', 11L, 0L, 1L, 'b'.bytes)
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
        def isRemoved = keyLoader.removeSingleKey(0, 'a', 10L)
        then:
        isRemoved
        keyLoader.getValueXByKey(0, 'a', 10L) == null

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
        def valueBytesWithExpireAt0 = keyLoader.getValueXByKey(0, 'a', 10L)
        def bytesBatch0 = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved0 = keyLoader.removeSingleKey(0, 'a', 10L)
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
        keyLoader.putValueByKey(0, 'a', 10L, 0L, 1L, encodeAsShortStringA)
        def valueBytesWithExpireAt = keyLoader.getValueXByKey(0, 'a', 10L)
        def bytesBatch = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved = keyLoader.removeSingleKey(0, 'a', 10L)
        def isRemoved2 = keyLoader.removeSingleKey(0, 'b', 11L)
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

        expect:
        keyLoader.executeOnceAfterLoopCount() == 1

        when:
        keyLoader.run()
        keyLoader.intervalDeleteExpiredBigStringFiles()
        keyLoader.intervalDeleteExpiredBigStringFilesLastBucketIndex = ConfForSlot.global.confBucket.bucketsPerSlot - 1
        keyLoader.intervalDeleteExpiredBigStringFiles()
        then:
        1 == 1

        when:
        def encodeAsShortStringA = Mock.prepareShortStringCvEncoded('a', 'a')
        keyLoader.putValueByKey(0, 'a', 10L, 0L, 1L, encodeAsShortStringA)
        def cv = new CompressedValue()
        cv.keyHash = 11L
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(1234L, Dict.SELF_ZSTD_DICT_SEQ)
        def bigStringCvEncoded = cv.encode()
        keyLoader.putValueByKey(0, 'b', 11L, System.currentTimeMillis() - 1000, 11L, bigStringCvEncoded)
        int count = keyLoader.intervalDeleteExpiredBigStringFiles()
        then:
        count == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test interval delete expired big string files skips pvm entries'() {
        given:
        def keyLoader = prepareKeyLoader()

        def pvm = new PersistValueMeta()
        pvm.segmentIndex = 1
        pvm.segmentOffset = 2
        keyLoader.putValueByKey(0, 'expired-pvm-key', 200L, System.currentTimeMillis() - 1000, 200L, pvm.encode())

        when:
        int count = keyLoader.intervalDeleteExpiredBigStringFiles()

        then:
        count == 0
        noExceptionThrown()

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
        keyLoader.putValueByKey(0, 'normal-key', 100L, System.currentTimeMillis() - 1, 100L, pvm.encode())
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        shortValueList.every {
            if (it.isExpired()) {
                return true
            }
            keyLoader.getValueXByKey(0, it.key(), it.keyHash()).valueBytes() == it.cvEncoded()
        }

        when:
        def slice = new Slice()
        keyLoader.encodeShortStringListToBuf(0, slice)
        def sliceForRead = new Slice(slice.array, 0, slice.writeIndex)
        int count = 0
        KeyLoader.decodeShortStringListFromBuf(sliceForRead) { keyHash, expireAt, seq, key, valueBytes ->
            println "key: $key, seq: $seq, keyHash: $keyHash, expireAt: $expireAt"
            count++
        }
        then:
        // one expired
        count == shortValueList.size() - 1

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
            pvm.bucketIndex = 0
            pvm.segmentOffset = it
            pvmList << pvm
        }

        when:
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, null)
        then:
        pvmList.every {
            keyLoader.getValueXByKey(0, it.key, it.keyHash).valueBytes() == it.encode()
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

    def 'replace pvm list for rebuild does not merge with existing key buckets'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()

        and:
        List<PersistValueMeta> existingPvmList = []
        3.times {
            def key = "old-key:" + it
            def pvm = new PersistValueMeta()
            pvm.key = key
            pvm.keyHash = KeyHash.hash(key.bytes)
            pvm.bucketIndex = 0
            pvm.segmentIndex = 1
            pvm.segmentOffset = it
            pvm.seq = it
            pvm.shortType = KeyLoader.typeAsByteString
            existingPvmList << pvm
        }
        keyLoader.updatePvmListBatchAfterWriteSegments(0, existingPvmList, null)

        and:
        List<PersistValueMeta> rebuiltPvmList = []
        def rebuiltKey = 'rebuilt-key:0'
        def rebuiltPvm = new PersistValueMeta()
        rebuiltPvm.key = rebuiltKey
        rebuiltPvm.keyHash = KeyHash.hash(rebuiltKey.bytes)
        rebuiltPvm.bucketIndex = 0
        rebuiltPvm.segmentIndex = 9
        rebuiltPvm.segmentOffset = 99
        rebuiltPvm.seq = 99L
        rebuiltPvm.shortType = KeyLoader.typeAsByteString
        rebuiltPvmList << rebuiltPvm

        expect:
        existingPvmList.every {
            keyLoader.getValueXByKey(0, it.key, it.keyHash).valueBytes() == it.encode()
        }
        keyLoader.getKeyCountInBucketIndex(0) == 3

        when:
        keyLoader.replacePvmListBatchInOneWalGroupForRebuild(0, rebuiltPvmList)

        then:
        existingPvmList.every {
            keyLoader.getValueXByKey(0, it.key, it.keyHash) == null
        }
        keyLoader.getValueXByKey(0, rebuiltPvm.key, rebuiltPvm.keyHash).valueBytes() == rebuiltPvm.encode()
        keyLoader.getKeyCountInBucketIndex(0) == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test scan cursor counts post-scan-start entries'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader

        and:
        List<PersistValueMeta> pvmList = []
        20.times { i ->
            def pvm = new PersistValueMeta()
            pvm.key = "key:" + i.toString().padLeft(12, '0')
            pvm.keyHash = KeyHash.hash(pvm.key.bytes)
            pvm.bucketIndex = 0
            pvm.segmentOffset = i
            pvm.shortType = KeyLoader.typeAsByteString
            pvm.seq = i < 5 ? 1000L : 100L
            pvmList << pvm
        }
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList, null)

        when:
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = 10000
        def beginScanSeq = 500L
        def r1 = keyLoader.scan(0, (byte) 0, (short) 0, KeyLoader.typeAsByteString, 'key:*', 3, beginScanSeq)

        then:
        r1.keys().size() == 3
        r1.keys().every { k -> k.startsWith('key:') }
        r1.scanCursor().keyBucketsSkipCount() == 5 + 3

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
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

    def 'test decodeShortStringListFromBuf throws on oversized key length'() {
        given:
        def slice = new Slice(256)
        // write a record with valid outer length but oversized keyLength
        // header: seq(8) + keyHash(8) + expireAt(8) + keyLength(4) + valueLength(4) = 32
        def keyLength = 100
        def valueLength = 10
        def outerLength = 32 + keyLength + valueLength
        slice.writeInt(outerLength)
        slice.writeLong(1L)   // seq
        slice.writeLong(2L)   // keyHash
        slice.writeLong(0L)   // expireAt
        // write keyLength larger than what fits in a small outer length
        slice.writeInt(outerLength + 1000) // oversized keyLength
        slice.writeInt(valueLength)
        // pad remaining
        while (slice.writeIndex < slice.array.length) {
            slice.writeByte(0)
        }

        def sliceForRead = new Slice(slice.array, 0, outerLength + 4)

        when:
        KeyLoader.decodeShortStringListFromBuf(sliceForRead) { keyHash, expireAt, seq, key, valueBytes -> }

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('key length')
    }

    def 'test decodeShortStringListFromBuf throws on oversized value length'() {
        given:
        def slice = new Slice(256)
        def keyLength = 5
        def valueLength = 10
        def outerLength = 32 + keyLength + valueLength
        slice.writeInt(outerLength)
        slice.writeLong(1L)   // seq
        slice.writeLong(2L)   // keyHash
        slice.writeLong(0L)   // expireAt
        slice.writeInt(keyLength)
        // write key bytes
        5.times { slice.writeByte('x'.bytes[0]) }
        // write valueLength larger than what remains
        slice.writeInt(outerLength + 1000)
        while (slice.writeIndex < slice.array.length) {
            slice.writeByte(0)
        }

        def sliceForRead = new Slice(slice.array, 0, outerLength + 4)

        when:
        KeyLoader.decodeShortStringListFromBuf(sliceForRead) { keyHash, expireAt, seq, key, valueBytes -> }

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('value length')
    }

    def 'test scan loop count not overcounted when a split has no valid buckets'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        // The bug under test is that `countArray[1]` in KeyLoader.readKeysToList (line 458) is
        // not reset between calls, so an iteration over an empty split that returns early
        // still bumps scanLoopCount (the `if (countArray[1] > 0)` check at KeyLoader.java:570
        // sees the stale value from a previous real-scan call). We assert on the cursor
        // position: with the bug the scan terminates early at a low walGroup (because the
        // overcount exhausts onceScanMaxLoopCount quickly), but with the fix countArray[1] is
        // reset at the top of readKeysToList and only real-scan iterations count, so the scan
        // completes wal group 0 and reaches ScanCursor.END.
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = 1000

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader

        and:
        // Trigger splits 1 -> 3 -> 9 by inserting 25 multi-cell keys. Use controlled keyHash
        // values such that (keyHash >> 32) % 3 == 0 (so at splitNumber=3 all 50 cells cluster
        // in split 0, which is still over 48, forcing a re-split to 9). At splitNumber=9 the
        // keys land in splits 3 and 6 only, so splits 0, 1, 2, 4, 5, 7, 8 have FDs but no valid
        // buckets - exactly the state that exposes the countArray[1] overcount.
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        def upperBits = [3L, 6L, 12L, 15L, 21L, 24L]
        List<Wal.V> longKeyList = []
        25.times { i ->
            def key = 'long_key_for_scan_overcount_test_' + i.toString().padLeft(16, '0')
            def cv = new CompressedValue()
            cv.compressedData = ('value' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            def keyHash = upperBits[i % upperBits.size()] << 32
            longKeyList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }
        inner.putAll(longKeyList)

        // Persist to disk - mirrors what KeyLoader.doAfterPutAll does: update key count stats
        // (the scan's early-return path at KeyLoader.java:436 reads statKeyCountInBuckets),
        // then write shared bytes, then update split-number metadata so maxSplitNumber becomes 9.
        def sharedBytesList = inner.encodeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        keyLoader.updateKeyCountBatch(0, inner.beginBucketIndex, inner.keyCountForStatsTmp)

        expect:
        inner.isSplit
        sharedBytesList.length == 9
        longKeyList.size() == 25
        longKeyList.every {
            inner.getValueX(it.bucketIndex(), it.key(), it.keyHash()) != null
        }

        when:
        // count = 100 is large enough to hold all 25 keys in a single call; the only thing
        // that can short-circuit the scan is scanLoopCount >= onceScanMaxLoopCount.
        // beginScanSeq = 100 (greater than all test Wal.V seq values 0..24) so the
        // `seq > beginScanSeq` filter at KeyLoader.java:504 does not drop any key.
        def r = keyLoader.scan(0, (byte) 0, (short) 0, (byte) 0, null, 100, 100L)

        then:
        // With the bug: countArray[1] is never reset, so the first real scan (in split 3)
        // bumps scanLoopCount to 1, and every subsequent early-return iteration in empty
        // splits AND in wal groups 1..N keeps bumping scanLoopCount because the stale
        // countArray[1] > 0 trips the check. With onceScanMaxLoopCount=1000 the scan
        // terminates well before completing all 2048 wal groups, so the cursor is not END.
        // With the fix: countArray[1] is reset at the top of readKeysToList, so only real
        // scans count. The scan completes wal group 0 (real scans at splits 3 and 6, bumps
        // scanLoopCount to 2), then runs through all remaining wal groups (all early-return,
        // no bump), and finally calls scanNextSlotCursor. Since this is the only slot, the
        // returned cursor is ScanCursor.END.
        r.scanCursor() == ScanCursor.END

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test key bucket read recovers from crash window data-new metadata-old'() {
        given:
        // Crash window #1: key bucket bytes were written with the new split number
        // (encodeAfterPutBatch encodes the bucket with splitNumberTmp[i] which is the new
        // value, including for cleared buckets in lower splits whose lastUpdateSeq is set
        // by the encode call), but the metadata file still has the old split number.
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader

        and:
        // Trigger a split 1 -> 3 by inserting 25 multi-cell keys. Use controlled keyHash
        // values so at splitNumber=3 all 50 cells cluster in split 0 (forcing a re-split to
        // 9 - or use a count that stops at 3). The point is just to bump splitNumber > 1
        // for bucket 0.
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        def upperBits = [3L, 6L, 12L, 15L, 21L, 24L]
        List<Wal.V> longKeyList = []
        25.times { i ->
            def key = 'crash_window_1_test_' + i.toString().padLeft(16, '0')
            def cv = new CompressedValue()
            cv.compressedData = ('value' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            def keyHash = upperBits[i % upperBits.size()] << 32
            longKeyList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }
        inner.putAll(longKeyList)
        def sharedBytesList = inner.encodeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        keyLoader.updateKeyCountBatch(0, inner.beginBucketIndex, inner.keyCountForStatsTmp)
        def finalSplitNumber = inner.splitNumberTmp[0]
        assert finalSplitNumber > 1, "split must have happened, got splitNumber=" + finalSplitNumber

        when:
        // Simulate crash window #1: data already has the new split number (cleared buckets
        // in lower splits got lastUpdateSeq set by encode(true)), but the metadata file still
        // has the old split number. Use setMetaKeyBucketSplitNumber which writes the byte
        // AND updates the in-memory cache.
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 1)

        and:
        // Now simulate "restart": build a fresh KeyBucketsInOneWalGroup and run
        // readBeforePutBatch. With the bug this throws IllegalStateException at
        // KeyBucket.java:170 because lastUpdateSplitNumber (the new value, e.g. 9) does
        // not match the splitNumber we passed in (1). With the fix, the read path detects
        // the mismatch, retries with splitNumber=-1 to decode from lastUpdateSeq, and
        // monotonically repairs the metadata to the new value.
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner2.readBeforePutBatch()

        then:
        // No exception thrown - the recovery is transparent to the caller.
        // The keys are still readable.
        longKeyList.every {
            inner2.getValueX(it.bucketIndex(), it.key(), it.keyHash()) != null
        }
        // The metadata byte for bucket 0 was monotonically repaired from 1 to finalSplitNumber.
        keyLoader.getMetaKeyBucketSplitNumberBatch(0, 1)[0] == finalSplitNumber

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test key bucket read handles crash window data-old metadata-new and downgrades to match data'() {
        given:
        // Crash window #2: metadata file was updated to a higher split number, but the
        // corresponding data files were never written (or were written with the old split
        // number). The recovery must DOWNGRADE the metadata to match the on-disk data
        // layout. The "don't downgrade" intuition only holds for full-iteration reads;
        // per-key lookups are hash-routed via KeyHash.splitIndex(keyHash, splitNumber,
        // bucketIndex), so a stale higher metadata would route the key to a different
        // (non-existent) split and the lookup would silently fail. The data layout is
        // ground truth; metadata must follow.
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader

        and:
        // Persist data with splitNumber=1 (no split).
        def inner = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner.readBeforePutBatch()

        List<Wal.V> longKeyList = []
        10.times { i ->
            def key = 'crash_window_2_test_' + i.toString().padLeft(16, '0')
            def cv = new CompressedValue()
            cv.compressedData = ('value' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            def keyHash = (3L + i) << 32
            longKeyList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }
        inner.putAll(longKeyList)
        def sharedBytesList = inner.encodeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)
        keyLoader.updateKeyCountBatch(0, inner.beginBucketIndex, inner.keyCountForStatsTmp)
        // Sanity: data was written with splitNumber=1.
        assert inner.splitNumberTmp[0] == 1

        when:
        // Simulate crash window #2: update the metadata to a higher split number (e.g. 3)
        // WITHOUT writing the corresponding data files for splits 1 and 2. The data in
        // split 0 still has lastUpdateSeq encoding splitNumber=1.
        keyLoader.setMetaKeyBucketSplitNumber(0, (byte) 3)

        and:
        // Simulate "restart": re-read. With the bug this throws IllegalStateException at
        // KeyBucket.java:170 because lastUpdateSplitNumber (1, from data) does not match
        // the metadata split number (3). With the fix, the recovery decodes via the
        // splitNumber=-1 path, picks the embedded value (1) as the ground truth, and
        // downgrades the metadata to 1 so subsequent hash-routed lookups work correctly.
        def inner2 = new KeyBucketsInOneWalGroup(slot, 0, keyLoader)
        inner2.readBeforePutBatch()

        then:
        // No exception - the recovery is transparent.
        // The keys are still readable (the bucket was constructed with the data's split
        // number 1 via the recovery path).
        longKeyList.every {
            inner2.getValueX(it.bucketIndex(), it.key(), it.keyHash()) != null
        }
        // The metadata WAS downgraded from 3 to 1, because the data layout is the
        // ground truth and per-key hash routing must align with it.
        keyLoader.getMetaKeyBucketSplitNumberBatch(0, 1)[0] == (byte) 1

        and:
        // Regression: the next write batch after a Window-B recovery must not AIOOBE.
        // encodeAfterPutBatch sizes sharedBytesList by the post-repair maxSplitNumberTmp
        // but the original loop bound was listList.size() (pre-repair), so on Window B
        // (metadata downgrade) the loop would index past the array.
        when:
        def putList = []
        1.times { i ->
            def key = 'crash_window_2_write_after_' + i.toString().padLeft(16, '0')
            def cv = new CompressedValue()
            cv.compressedData = ('after' + i).bytes
            def cvEncoded = cv.encodeAsShortString()
            def keyHash = (100L + i) << 32
            putList << new Wal.V(i, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
        }
        inner2.putAll(putList)
        def sharedBytesListAfterWrite = inner2.encodeAfterPutBatch()

        then:
        // No exception; the shared bytes array matches the repaired max split number (= 1).
        sharedBytesListAfterWrite.length == 1
        // The new key is readable after the write.
        putList.every {
            inner2.getValueX(it.bucketIndex(), it.key(), it.keyHash()) != null
        }

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
