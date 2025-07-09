package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import io.velo.repl.incremental.XOneWalGroupPersist
import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import spock.lang.Specification

import static io.velo.persist.FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD

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

    def 'test pread'() {
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
        def bytes = chunk.preadForMerge(0, 10)
        then:
        bytes == null

        when:
        boolean exception = false
        try {
            chunk.preadForMerge(0, FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        bytes = chunk.preadForRepl(0)
        then:
        bytes == null

        when:
        bytes = chunk.preadOneSegment(0)
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

        def xForBinlog = new XOneWalGroupPersist(true, false, 0)

        when:
        def vList = Mock.prepareValueList(100)
        chunk.segmentIndex = 0
        chunk.persist(0, vList, xForBinlog, null)
        then:
        chunk.segmentIndex == 2

        when:
        List<Long> blankSeqList = []
        4.times {
            blankSeqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init.flagByte(), blankSeqList, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vList, xForBinlog, null)
        then:
        chunk.segmentIndex == 2

        when:
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init.flagByte(), blankSeqList, 0)
        chunk.segmentIndex = 0
        def vListManyCount = Mock.prepareValueList(100, 0)
        (1..<16).each {
            vListManyCount.addAll Mock.prepareValueList(100, it)
        }
        chunk.persist(0, vListManyCount, xForBinlog, null)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE

        when:
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
        chunk.fdLengths[0] = 4096 * 100
        List<Long> seqList = []
        32.times {
            seqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, seqList.size(), Chunk.Flag.merged_and_persisted.flagByte(), seqList, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vListManyCount, xForBinlog, null)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE

        when:
        chunk.fdLengths[0] = 0
        chunk.segmentIndex = halfSegmentNumber
        chunk.persist(0, vList, xForBinlog, null)
        then:
        1 == 1

        when:
        ConfForGlobal.pureMemory = true
        chunk.fdReadWriteArray[0].initByteBuffers(true, 0)
        println 'mock pure memory chunk append segments bytes, fd: ' + chunk.fdReadWriteArray[0].name
        for (i in 0..<oneSlot.keyLoader.fdReadWriteArray.length) {
            def frw = oneSlot.keyLoader.fdReadWriteArray[i]
            if (frw != null) {
                frw.initByteBuffers(false, i)
                println 'mock pure memory key loader set key buckets bytes, fd: ' + frw.name
            }
        }
        // begin persist
        List<Long> blankSeqListMany = []
        32.times {
            blankSeqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqListMany.size(), Chunk.Flag.init.flagByte(), blankSeqListMany, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vListManyCount, xForBinlog, null)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE

        when:
        List<Long> blankSeqListForAll = []
        ConfForSlot.global.confChunk.maxSegmentNumber().times {
            blankSeqListForAll << 0L
        }
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqListForAll.size(), Chunk.Flag.new_write.flagByte(), blankSeqListForAll, 0)
        boolean exception = false
        try {
            chunk.persist(0, vList, xForBinlog, null)
        } catch (SegmentOverflowException ignore) {
            exception = true
        }
        then:
        exception

        when:
        chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        then:
        chunk.segmentIndex == 0

        when:
        def n = chunk.getSegmentRealLength(0)
        def n2 = chunk.getSegmentRealLength(1)
        then:
        n == 4096
        n2 == 0

        when:
        chunk.clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(0)
        then:
        chunk.preadOneSegment(0) == null

        cleanup:
        ConfForGlobal.pureMemory = false
        chunk.cleanUp()
        oneSlot.keyLoader.flush()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemory = true
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk

        when:
        chunk.segmentIndex = 1
        def contentBytes = new byte[chunk.chunkSegmentLength * 2]
        contentBytes[0] = (byte) 1
        contentBytes[4096] = (byte) 1
        int[] segmentRealLengths = [4096, 4096]
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(contentBytes, 0, 2, segmentRealLengths)
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        chunk.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        chunk.loadFromLastSavedFileWhenPureMemory(is)
        then:
        chunk.segmentIndex == 1
        !chunk.fdReadWriteArray[0].isTargetSegmentIndexNullInMemory(1)

        when:
        chunk.truncateChunkFdFromSegmentIndex(1)
        then:
        chunk.preadOneSegment(1) == null
        chunk.preadOneSegment(0) != null

        when:
        // all byte 0 means clear
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(new byte[chunk.chunkSegmentLength], 0, 1, segmentRealLengths)
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(new byte[chunk.chunkSegmentLength * 2], 1, 2, segmentRealLengths)
        then:
        chunk.preadOneSegment(0) == null
        chunk.preadOneSegment(1) == null
        chunk.preadOneSegment(2) == null

        when:
        chunk.clearSegmentBytesWhenPureMemory(0)
        then:
        chunk.preadOneSegment(0) == null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }

    def 'test repl'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk

        when:
        def replBytes = new byte[4096]
        int[] segmentRealLengths = new int[1]
        segmentRealLengths[0] = 4096
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(replBytes, 0, 1, segmentRealLengths)
        then:
        1 == 1

        when:
        // write again, fd length will not change
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(replBytes, 0, 1, segmentRealLengths)
        then:
        1 == 1

        when:
        boolean exception = false
        try {
            chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(replBytes, 0, REPL_ONCE_SEGMENT_COUNT_PREAD + 1, segmentRealLengths)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ConfForGlobal.pureMemory = true
        for (i in 0..<chunk.fdReadWriteArray.length) {
            def frw = chunk.fdReadWriteArray[i]
            frw.initByteBuffers(true, i)
        }
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(replBytes, 0, 1, segmentRealLengths)
        then:
        1 == 1

        when:
        def replBytes2 = new byte[4096 * 2]
        def segmentRealLengths2 = new int[2]
        segmentRealLengths2[0] = 4096
        segmentRealLengths2[1] = 4096
        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(replBytes2, 0, 2, segmentRealLengths2)
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemory = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
