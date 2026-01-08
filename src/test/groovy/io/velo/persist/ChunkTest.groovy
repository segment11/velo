package io.velo.persist

import io.velo.ConfForSlot
import spock.lang.Specification

class ChunkTest extends Specification {
    static Chunk prepareOne(short slot, boolean withKeyLoader = false) {
        def confChunk = ConfForSlot.global.confChunk
        confChunk.fdPerChunk = 2
        confChunk.segmentNumberPerFd = 4096

        def keyLoader = withKeyLoader ? KeyLoaderTest.prepareKeyLoader() : null
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)

        def chunk = new Chunk(slot, Consts.slotDir, oneSlot)
        oneSlot.chunk = chunk

        chunk
    }

    final short slot = 0

    def 'test base'() {
        given:
        def flagInit = Chunk.Flag.init
        println flagInit
        println flagInit.flagByte()
        println new Chunk.SegmentFlag(flagInit.flagByte(), 1L, 0)

        expect:
        Chunk.Flag.canReuse(Chunk.Flag.init.flagByte()) && Chunk.Flag.canReuse(Chunk.Flag.merged_and_persisted.flagByte())
        !Chunk.Flag.canReuse(Chunk.Flag.new_write.flagByte())
    }

    def 'test segment index'() {
        given:
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot
        def confChunk = ConfForSlot.global.confChunk

        println chunk
        println chunk.maxSegmentIndex

        when:
        chunk.collect()
        chunk.persistCallCountTotal = 1
        chunk.persistCvCountTotal = 100
        chunk.updatePvmBatchCostTimeTotalUs = 100
        chunk.segmentDecompressCountTotal = 10
        chunk.segmentDecompressTimeTotalUs = 100
        chunk.collect()

        def segmentNumberPerFd = confChunk.segmentNumberPerFd
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
        chunk.cleanUp()
        then:
        chunk.targetFdIndex(0) == 0
        chunk.targetFdIndex(segmentNumberPerFd - 1) == 0
        chunk.targetFdIndex(segmentNumberPerFd) == 1
        chunk.targetFdIndex(segmentNumberPerFd * 2 - 1) == 1
        chunk.targetSegmentIndexTargetFd(0) == 0
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd - 1) == segmentNumberPerFd - 1
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd) == 0
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd * 2 - 1) == segmentNumberPerFd - 1

        when:
        chunk.moveSegmentIndexNext(1)
        then:
        chunk.segmentIndex == 1
        chunk.targetFdIndex() == 0
        chunk.targetSegmentIndexTargetFd() == 1

        when:
        chunk.moveSegmentIndexNext(segmentNumberPerFd - 1)
        then:
        chunk.segmentIndex == segmentNumberPerFd

        when:
        chunk.moveSegmentIndexNext(segmentNumberPerFd - 1)
        then:
        chunk.segmentIndex == 0

        when:
        boolean isMoveOverflow = false
        try {
            chunk.moveSegmentIndexNext(confChunk.maxSegmentNumber() + 1)
        } catch (SegmentOverflowException e) {
            println e.message
            isMoveOverflow = true
        }
        then:
        isMoveOverflow

        when:
        chunk.segmentIndex = confChunk.maxSegmentNumber() - 1
        chunk.moveSegmentIndexNext(1)
        then:
        chunk.segmentIndex == 0

        when:
        List<Integer> segmentIndexListNewAppend = []
        List<Integer> prevFindSegmentIndexListNotNewAppend = []
        confChunk.maxSegmentNumber().times {
            segmentIndexListNewAppend << chunk.prevFindSegmentIndexSkipHalf(true, it)
            prevFindSegmentIndexListNotNewAppend << chunk.prevFindSegmentIndexSkipHalf(false, it)
        }
        println segmentIndexListNewAppend
        println prevFindSegmentIndexListNotNewAppend
        then:
        chunk.prevFindSegmentIndexSkipHalf(true, halfSegmentNumber) == 0
        chunk.prevFindSegmentIndexSkipHalf(true, halfSegmentNumber + 10) == 10
        chunk.prevFindSegmentIndexSkipHalf(true, halfSegmentNumber - 10) == -1
        chunk.prevFindSegmentIndexSkipHalf(false, halfSegmentNumber - 10) == halfSegmentNumber * 2 - 10
        segmentIndexListNewAppend.count { it == -1 } == halfSegmentNumber
        new HashSet(segmentIndexListNewAppend.findAll { it != -1 }).size() == halfSegmentNumber
        new HashSet(prevFindSegmentIndexListNotNewAppend).size() == confChunk.maxSegmentNumber()
        segmentIndexListNewAppend[halfSegmentNumber] == 0
        segmentIndexListNewAppend[0] == -1
        segmentIndexListNewAppend[-1] == halfSegmentNumber - 1
        prevFindSegmentIndexListNotNewAppend[halfSegmentNumber] == 0
        prevFindSegmentIndexListNotNewAppend[0] == halfSegmentNumber
        prevFindSegmentIndexListNotNewAppend[halfSegmentNumber - 1] == confChunk.maxSegmentNumber() - 1
        prevFindSegmentIndexListNotNewAppend[-1] == halfSegmentNumber - 1

        when:
        chunk.segmentIndex = halfSegmentNumber - 1
        chunk.moveSegmentIndexNext(1)
        then:
        chunk.segmentIndex == halfSegmentNumber

        when:
        chunk.resetAsFlush()
        then:
        chunk.segmentIndex == 0

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test read'() {
        given:
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot
        chunk.initFds()

        when:
        def sb = new StringBuilder()
        chunk.estimate(sb)
        println sb.toString()
        then:
        1 == 1

        when:
        def bytes = chunk.readForMerge(0, 10)
        then:
        bytes == null

        when:
        boolean exception = false
        try {
            chunk.readForMerge(0, FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        bytes = chunk.readForRepl(0)
        then:
        bytes == null

        when:
        bytes = chunk.readOneSegment(0)
        then:
        bytes == null

        cleanup:
        chunk.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test persist'() {
        given:
        def chunk = prepareOne(slot, true)
        def oneSlot = chunk.oneSlot
        def confChunk = ConfForSlot.global.confChunk

        and:
        chunk.initFds()
        chunk.collect()

        when:
        def vList = Mock.prepareValueList(100)
        chunk.segmentIndex = 0
        chunk.persist(0, vList, null)
        then:
        chunk.segmentIndex == 2

        when:
        List<Long> blankSeqList = []
        4.times {
            blankSeqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init.flagByte(), blankSeqList, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vList, null)
        then:
        chunk.segmentIndex == 2

        when:
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init.flagByte(), blankSeqList, 0)
        chunk.segmentIndex = 0
        def vListManyCount = Mock.prepareValueList(100, 0)
        (1..<16).each {
            vListManyCount.addAll Mock.prepareValueList(100, it)
        }
        chunk.persist(0, vListManyCount, null)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE

        when:
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
        chunk.fdLengths[0] = 4096 * 100
        List<Long> seqList = []
        32.times {
            seqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, seqList.size(), Chunk.Flag.merged_and_persisted.flagByte(), seqList, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vListManyCount, null)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE

        when:
        chunk.fdLengths[0] = 0
        chunk.segmentIndex = halfSegmentNumber
        chunk.persist(0, vList, null)
        then:
        1 == 1

        when:
        def exception = false
        try {
            chunk.writeSegments(new byte[0], 2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        chunk.cleanUp()
        oneSlot.keyLoader.flush()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }
}
