package io.velo.persist

import io.velo.Debug
import spock.lang.Specification

class ChunkMergeWorkerTest extends Specification {
    final short slot = 0

    def 'test base'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker
        chunkMergeWorker.resetThreshold(Wal.calcWalGroupNumber())
        chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD = 1000

        chunkMergeWorker.collect()
        chunkMergeWorker.mergedSegmentCount = 1
        chunkMergeWorker.collect()

        println 'in memory size estimate: ' + chunkMergeWorker.estimate()

        def ms0 = new ChunkMergeWorker.MergedSegment(0, 1)
        def ms1 = new ChunkMergeWorker.MergedSegment(1, 1)
        println ms0
        println ms1
        expect:
        ms0 < ms1

        when:
        int walGroupIndex = 0
        int bucketIndex = 0
        int segmentIndex = 0
        then:
        chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex) == null

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, 32, 1))
        chunkMergeWorker.addMergedSegment(0, 1)
        then:
        !chunkMergeWorker.isMergedSegmentSetEmpty()
        chunkMergeWorker.firstMergedSegmentIndex() == 0
        chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex) == null

        when:
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
        chunkMergeWorker.addMergedSegment(segmentIndex, 1)
        def r = chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex)
        then:
        r.segmentIndexList().size() == 1
        r.vList().size() == 1

        when:
        chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal([0], 0)
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 1000
        chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal([1], 1)
        chunkMergeWorker.clearMergedCvList()
        chunkMergeWorker.clearMergedSegmentSet()
        then:
        chunkMergeWorker.mergedCvListSize == 0
        chunkMergeWorker.mergedSegmentSetSize == 0

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test persist'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker
        chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD = 1000

        and:
        int bucketIndex = 0
        int segmentIndex = 0

        when:
        def cvList = Mock.prepareCompressedValueList(10)
        for (cv in cvList) {
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
        }
        chunkMergeWorker.addMergedSegment(segmentIndex, cvList.size())
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 999
        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()
        chunkMergeWorker.persistAllMergedCvListInTargetSegmentIndexList([segmentIndex])
        then:
        chunkMergeWorker.mergedCvListSize == 0
        chunkMergeWorker.mergedSegmentSetSize == 0

        when:
        def cvList2 = Mock.prepareCompressedValueList(chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD)
        for (cv in cvList2) {
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
        }
        chunkMergeWorker.addMergedSegment(segmentIndex, cvList2.size())
        10.times {
            chunkMergeWorker.addMergedSegment(segmentIndex + it + 1, 1)
        }
        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()
        println 'in memory size estimate: ' + chunkMergeWorker.estimate()
        then:
        chunkMergeWorker.mergedCvListSize == 0
        // once persist segment number: chunkMergeWorker.MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST
        chunkMergeWorker.mergedSegmentSetSize == 1 + 10 - chunkMergeWorker.MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        for (cv in cvList2) {
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq + 10000, bucketIndex + 32, segmentIndex + 1))
        }
        chunkMergeWorker.addMergedSegment(segmentIndex, cvList2.size())
        chunkMergeWorker.addMergedSegment(segmentIndex + 1, cvList2.size())
        chunkMergeWorker.logMergeCount = 999
        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()
        then:
        chunkMergeWorker.mergedCvListSize == 0
        chunkMergeWorker.mergedSegmentSetSize == 0

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
