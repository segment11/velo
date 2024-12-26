package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import spock.lang.Specification

import static Consts.getSlotDir
import static io.velo.persist.Chunk.NO_NEED_MERGE_SEGMENT_INDEX

class MetaChunkSegmentFlagSeqTest extends Specification {
    final short slot = 0

    def 'test for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)
        println 'in memory size estimate: ' + one.estimate(new StringBuilder())

        when:
        def oneBatchBytes = one.getOneBatch(0, 1024)
        then:
        oneBatchBytes.length == one.ONE_LENGTH * 1024

        when:
        Arrays.fill(oneBatchBytes, (byte) 1)
        one.overwriteOneBatch(oneBatchBytes, 0, 1024)
        then:
        one.getOneBatch(0, 1024) == oneBatchBytes

        when:
        ConfForGlobal.pureMemory = true
        one.overwriteOneBatch(oneBatchBytes, 0, 1024)
        then:
        one.getOneBatch(0, 1024) == oneBatchBytes

        when:
        boolean exception = false
        def bytes0WrongSize = new byte[oneBatchBytes.length - 1]
        try {
            one.overwriteOneBatch(bytes0WrongSize, 0, 1024)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }

    def 'test read write seq'() {
        given:
        ConfForGlobal.pureMemory = false
        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        one.setSegmentMergeFlag(10, Chunk.Flag.merging.flagByte(), 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)
        then:
        segmentFlag.flagByte() == Chunk.Flag.merging.flagByte()
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0

        when:
        ConfForGlobal.pureMemory = true
        def one2 = new MetaChunkSegmentFlagSeq(slot, slotDir)
        one2.setSegmentMergeFlag(10, Chunk.Flag.merging.flagByte(), 1L, 0)
        def segmentFlag2 = one2.getSegmentMergeFlag(10)
        then:
        segmentFlag2.flagByte() == Chunk.Flag.merging.flagByte()
        segmentFlag2.segmentSeq() == 1L
        segmentFlag2.walGroupIndex() == 0

        when:
        one2.overwriteInMemoryCachedBytes(new byte[one2.allCapacity])
        def segmentFlag3 = one.getSegmentMergeFlag(3)
        then:
        segmentFlag3.segmentSeq() == 0L
        one2.getInMemoryCachedBytes().length == one2.allCapacity

        when:
        boolean exception = false
        try {
            one2.overwriteInMemoryCachedBytes(new byte[one2.allCapacity + 1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        ConfForGlobal.pureMemory = false
        one.clear()
        one.cleanUp()
        ConfForGlobal.pureMemory = true
        one2.clear()
        one2.cleanUp()
        ConfForGlobal.pureMemory = false
    }

    def 'test read write seq pure memory'() {
        given:
        ConfForGlobal.pureMemory = true

        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        one.setSegmentMergeFlag(10, Chunk.Flag.merging.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(11, Chunk.Flag.merged.flagByte(), 2L, 11)
        def segmentFlag = one.getSegmentMergeFlag(10)
        def segmentFlagList = one.getSegmentMergeFlagBatch(10, 2)
        then:
        segmentFlag.flagByte() == Chunk.Flag.merging.flagByte()
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0

        segmentFlagList.size() == 2
        segmentFlagList[0].flagByte() == Chunk.Flag.merging.flagByte()
        segmentFlagList[0].segmentSeq() == 1L
        segmentFlagList[0].walGroupIndex() == 0
        segmentFlagList[1].flagByte() == Chunk.Flag.merged.flagByte()
        segmentFlagList[1].segmentSeq() == 2L
        segmentFlagList[1].walGroupIndex() == 11

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForGlobal.pureMemory = false
    }

    def 'test read batch for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        ConfForGlobal.pureMemory = false
        List<Long> seqLongList = []
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.merging.flagByte(), seqLongList, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        when:
        ConfForGlobal.pureMemory = true
        seqLongList.clear()
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.merging.flagByte(), seqLongList, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        when:
        def seq0List = seqLongList.collect { 0L }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.merging.flagByte(), null, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seq0List

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForGlobal.pureMemory = false
    }

    // need shell:
    // cp persist/slot-0/meta_chunk_segment_flag_seq.dat test-persist/test-slot/
    def 'test iterate'() {
        given:
        def confChunk = ConfForSlot.global.confChunk
        def c10m = ConfForSlot.ConfChunk.c10m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = c10m.segmentNumberPerFd
        confChunk.fdPerChunk = c10m.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        new File('chunk_segment_flag.txt').withWriter { writer ->
            one.iterateAll { segmentIndex, flagByte, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flagByte, $seq, $walGroupIndex")
            }
        }
        new File('chunk_segment_flag_range.txt').withWriter { writer ->
            one.iterateRange(1024, 1024) { segmentIndex, flagByte, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flagByte, $seq, $walGroupIndex")
            }
        }
        then:
        1 == 1

        cleanup:
        confChunk.segmentNumberPerFd = ConfForSlot.c1m.confChunk.segmentNumberPerFd
        confChunk.fdPerChunk = ConfForSlot.c1m.confChunk.fdPerChunk
        one.clear()
        one.cleanUp()
    }

    def 'test iterate and find'() {
        given:
        ConfForGlobal.pureMemory = true

        // c1m
        def confChunk = ConfForSlot.global.confChunk
        def c100mConfChunk = ConfForSlot.ConfChunk.c100m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = c100mConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = c100mConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        int targetWalGroupIndex = 1

        and:
        def chunk = new Chunk(slot, slotDir, null, null, null)

        when:
        def r = one.iterateAndFindThoseNeedToMerge(1024, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r[0] == NO_NEED_MERGE_SEGMENT_INDEX
        r[1] == 0

        when:
        one.setSegmentMergeFlag(1024, Chunk.Flag.new_write.flagByte(), 1L, targetWalGroupIndex)
        def r2 = one.iterateAndFindThoseNeedToMerge(1024, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == 1024
        r2[1] == 1

        when:
        one.setSegmentMergeFlag(1024, Chunk.Flag.reuse_new.flagByte(), 1L, targetWalGroupIndex)
        r2 = one.iterateAndFindThoseNeedToMerge(1024, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == 1024
        r2[1] == 1

        when:
        one.setSegmentMergeFlag(1024, Chunk.Flag.merged.flagByte(), 1L, targetWalGroupIndex)
        r2 = one.iterateAndFindThoseNeedToMerge(1024, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == 1024
        r2[1] == 1

        when:
        10.times {
            one.setSegmentMergeFlag(1024 + it, Chunk.Flag.reuse_new.flagByte(), 1L, targetWalGroupIndex)
        }
        r2 = one.iterateAndFindThoseNeedToMerge(1024, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == 1024
        // max 4
        r2[1] == 4

        when:
        // cross fd
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd - 1, Chunk.Flag.reuse_new.flagByte(), 1L, targetWalGroupIndex)
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd, Chunk.Flag.reuse_new.flagByte(), 1L, targetWalGroupIndex)
        r2 = one.iterateAndFindThoseNeedToMerge(confChunk.segmentNumberPerFd - 1, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == confChunk.segmentNumberPerFd - 1
        r2[1] == 1

        when:
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd - 1, Chunk.Flag.init.flagByte(), 1L, targetWalGroupIndex)
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd, Chunk.Flag.init.flagByte(), 1L, targetWalGroupIndex)
        r2 = one.iterateAndFindThoseNeedToMerge(confChunk.segmentNumberPerFd - 1, 1024 * 10, targetWalGroupIndex, chunk)
        then:
        r2[0] == NO_NEED_MERGE_SEGMENT_INDEX
        r2[1] == 0

        cleanup:
        confChunk.segmentNumberPerFd = ConfForSlot.c1m.confChunk.segmentNumberPerFd
        confChunk.fdPerChunk = ConfForSlot.c1m.confChunk.fdPerChunk
        one.clear()
        one.cleanUp()
    }

    def 'test get merged segment index'() {
        given:
        ConfForGlobal.pureMemory = true

        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        // all init
        def i = one.getMergedSegmentIndexEndLastTime(0, 0)
        then:
        i == NO_NEED_MERGE_SEGMENT_INDEX

        when:
        def maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber()
        int halfSegmentNumber = (maxSegmentNumber / 2).intValue()
        one.setSegmentMergeFlag(10, Chunk.Flag.new_write.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(11, Chunk.Flag.new_write.flagByte(), 1L, 0)
        def i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)
        then:
        i2 == 9

        when:
        one.setSegmentMergeFlag(10, Chunk.Flag.merged.flagByte(), 1L, 0)
        i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)
        then:
        i2 == 10

        when:
        one.setSegmentMergeFlag(10, Chunk.Flag.merged_and_persisted.flagByte(), 1L, 0)
        i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)
        then:
        i2 == 10

        when:
        def i3 = one.getMergedSegmentIndexEndLastTime(0, halfSegmentNumber)
        then:
        i3 == NO_NEED_MERGE_SEGMENT_INDEX

        cleanup:
        ConfForGlobal.pureMemory = false
    }
}
