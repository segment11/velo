package io.velo.persist

import com.github.luben.zstd.Zstd
import io.velo.*
import io.velo.repl.incremental.XOneWalGroupPersist
import spock.lang.Specification

class ChunkMergeJobTest extends Specification {
    final short slot = 0
    final short slotNumber = 1

    def 'merge segments'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker

        def cv = new CompressedValue()
        def cvWithKeyAndSegmentOffset = new ChunkMergeJob.CvWithKeyAndSegmentOffset(cv, 'key', 0, 0, (byte) 0)
        println cvWithKeyAndSegmentOffset.shortString()
        println cvWithKeyAndSegmentOffset

        and:
        int segmentIndex = 0
        ArrayList<Integer> needMergeSegmentIndexList = []
        10.times {
            needMergeSegmentIndexList << (segmentIndex + it)
        }
        oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.Flag.new_write.flagByte(), 1L, 0)
        oneSlot.setSegmentMergeFlag(segmentIndex + 1, Chunk.Flag.init.flagByte(), 1L, 0)
        def job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, oneSlot.snowFlake)

        when:
        // chunk segments not write yet, in wal
        OneSlotTest.batchPut(oneSlot, 300)
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        def bucketIndex0KeyList = OneSlotTest.batchPut(oneSlot, 300, 100, 0, slotNumber)
        def bucketIndex1KeyList = OneSlotTest.batchPut(oneSlot, 300, 100, 1, slotNumber)
        def testRemovedByWalKey0 = bucketIndex0KeyList[0]
        def testUpdatedByWalKey1 = bucketIndex0KeyList[1]
        def testRemovedByWalKey2 = bucketIndex0KeyList[2]
        def testUpdatedByKeyLoaderKey3 = bucketIndex0KeyList[3]
        def sTest0 = BaseCommand.slot(testRemovedByWalKey0.bytes, slotNumber)
        def sTest1 = BaseCommand.slot(testUpdatedByWalKey1.bytes, slotNumber)
        def sTest2 = BaseCommand.slot(testRemovedByWalKey2.bytes, slotNumber)
        def sTest3 = BaseCommand.slot(testUpdatedByKeyLoaderKey3.bytes, slotNumber)

        oneSlot.removeDelay(testRemovedByWalKey0, 0, sTest0.keyHash())
        oneSlot.keyLoader.removeSingleKey(0, testRemovedByWalKey2.bytes, sTest2.keyHash())
        oneSlot.keyLoader.putValueByKey(0, testUpdatedByKeyLoaderKey3.bytes, sTest3.keyHash(), 0, 0L, new byte[10])
        def cv2 = new CompressedValue()
        cv2.keyHash = sTest1.keyHash()
        cv2.seq = oneSlot.snowFlake.nextId()
        oneSlot.put(testUpdatedByWalKey1, 0, cv2)
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        2.times {
            OneSlotTest.batchPut(oneSlot, 100, 100, 0, slotNumber)
        }
        List<Long> seqList = []
        10.times {
            seqList << 1L
        }
        oneSlot.setSegmentMergeFlagBatch(segmentIndex, 10, Chunk.Flag.new_write.flagByte(), seqList, 0)
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 999
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        boolean exception = false
        needMergeSegmentIndexList[-1] = segmentIndex + 2
        try {
            job.mergeSegments(needMergeSegmentIndexList)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def needMergeSegmentIndexList2 = [0, 1, oneSlot.chunk.maxSegmentIndex - 1, oneSlot.chunk.maxSegmentIndex]
        def job2 = new ChunkMergeJob(slot, needMergeSegmentIndexList2, chunkMergeWorker, oneSlot.snowFlake)
        job2.run()
        then:
        1 == 1

        when:
        needMergeSegmentIndexList2[0] = 1
        exception = false
        try {
            job2.run()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test some branches'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker

        and:
        int walGroupIndex = 0
        int bucketIndex = 0
        int segmentIndex = 0
        ArrayList<Integer> needMergeSegmentIndexList = []
        10.times {
            needMergeSegmentIndexList << (segmentIndex + it)
            oneSlot.setSegmentMergeFlag(segmentIndex + it, Chunk.Flag.new_write.flagByte(), 1L, 0)
        }
        def job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, oneSlot.snowFlake)

        and:
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 999

        and:
        ArrayList<Wal.V> valueList = []
        Mock.prepareTargetBucketIndexKeyList(400, bucketIndex).eachWithIndex { key, it ->
            def keyBytes = key.bytes
            def keyHash = KeyHash.hash(keyBytes)

            def cv = new CompressedValue()
            cv.seq = it
            cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
            cv.keyHash = keyHash
            cv.compressedData = new byte[10]
            cv.compressedLength = 10
            cv.uncompressedLength = 10
            if (it % 10 == 0) {
                cv.expireAt = System.currentTimeMillis() - 1
            }

            if (it % 55 == 0) {
                // when merge need check if need compress use new dict
                cv.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
                cv.compressedData = Zstd.compress(new byte[100])
                cv.compressedLength = cv.compressedData.length
                cv.uncompressedLength = 100
            }

            def encoded = it % 20 == 0 ? cv.encodeAsBigStringMeta(it) : cv.encode()
            def v = new Wal.V(it, bucketIndex, keyHash, cv.expireAt, key, encoded, false)
            valueList << v
        }

        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        TrainSampleJob.keyPrefixOrSuffixGroupList = ['xh!']
        dictMap.putDict('xh!', Dict.SELF_ZSTD_DICT)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5, 6]
        ArrayList<PersistValueMeta> returnPvmList = []

        def segmentBatch = new SegmentBatch(slot, oneSlot.snowFlake)
        def r = segmentBatch.splitAndTight(valueList, nextNSegmentIndex, returnPvmList)
        println 'split and tight: ' + r.size() + ' segments, ' + returnPvmList.size() + ' pvm list'

        def fdChunk = oneSlot.chunk.fdReadWriteArray[0]
        fdChunk.writeOneInner(segmentIndex, r[0].tightBytesWithLength(), false)
        fdChunk.writeOneInner(segmentIndex + 1, r[1].tightBytesWithLength(), false)
        println 'write segment ' + segmentIndex + ', ' + (segmentIndex + 1)

        def xForBinlog = new XOneWalGroupPersist(true, false, 0)
        oneSlot.keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, returnPvmList, xForBinlog)
        println 'bucket ' + bucketIndex + ' key count: ' + oneSlot.keyLoader.getKeyCountInBucketIndex(bucketIndex)

        when:
        job.mergeSegments(needMergeSegmentIndexList)
        println 'after merge segments, valid cv count: ' + job.validCvCountAfterRun + ', invalid cv count: ' + job.invalidCvCountAfterRun
        then:
        job.invalidCvCountAfterRun == 40

        cleanup:
        oneSlot.cleanUp()
        dictMap.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
