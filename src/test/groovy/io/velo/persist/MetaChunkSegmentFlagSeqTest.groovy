package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import spock.lang.Specification

import static Consts.getSlotDir

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
        one.setSegmentMergeFlag(10, Chunk.Flag.new_write.flagByte(), 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)
        then:
        segmentFlag.flagByte() == Chunk.Flag.new_write.flagByte()
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0

        when:
        ConfForGlobal.pureMemory = true
        def one2 = new MetaChunkSegmentFlagSeq(slot, slotDir)
        one2.setSegmentMergeFlag(10, Chunk.Flag.new_write.flagByte(), 1L, 0)
        def segmentFlag2 = one2.getSegmentMergeFlag(10)
        then:
        segmentFlag2.flagByte() == Chunk.Flag.new_write.flagByte()
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
        one.setSegmentMergeFlag(10, Chunk.Flag.new_write.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(11, Chunk.Flag.new_write.flagByte(), 2L, 11)
        def segmentFlag = one.getSegmentMergeFlag(10)
        def segmentFlagList = one.getSegmentMergeFlagBatch(10, 2)
        then:
        segmentFlag.flagByte() == Chunk.Flag.new_write.flagByte()
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0
        segmentFlagList.size() == 2
        segmentFlagList[0].flagByte() == Chunk.Flag.new_write.flagByte()
        segmentFlagList[0].segmentSeq() == 1L
        segmentFlagList[0].walGroupIndex() == 0
        segmentFlagList[1].flagByte() == Chunk.Flag.new_write.flagByte()
        segmentFlagList[1].segmentSeq() == 2L
        segmentFlagList[1].walGroupIndex() == 11

        when:
        one.setSegmentMergeFlag(0, Chunk.Flag.init.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(1, Chunk.Flag.init.flagByte(), 1L, 0)
        then:
        one.isAllFlagsNotWrite(0, 2)

        when:
        one.setSegmentMergeFlag(1, Chunk.Flag.new_write.flagByte(), 1L, 0)
        then:
        !one.isAllFlagsNotWrite(0, 2)

        when:
        one.setSegmentMergeFlag(1, Chunk.Flag.reuse_new.flagByte(), 1L, 0)
        then:
        !one.isAllFlagsNotWrite(0, 2)

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
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.new_write.flagByte(), seqLongList, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        when:
        ConfForGlobal.pureMemory = true
        seqLongList.clear()
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.new_write.flagByte(), seqLongList, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        when:
        def seq0List = seqLongList.collect { 0L }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.new_write.flagByte(), null, 0)
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

    def 'test find those need to merge'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)

        when:
        def r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.addSegmentIndexToTargetWalGroup(0, 0, 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 10

        when:
        100.times {
            one.addSegmentIndexToTargetWalGroup(0, it * 10, 10)
        }
        100.times {
            // find then clear
            one.findThoseNeedToMerge(0)
        }
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }

    def 'test find can reuse segment index'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)

        when:
        def segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == 0

        when:
        one.setSegmentMergeFlag(0, Chunk.Flag.new_write.flagByte(), 1L, 0)
        segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == 1

        when:
        one.setSegmentMergeFlag(2, Chunk.Flag.new_write.flagByte(), 1L, 0)
        segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == 3

        when:
        for (int i = 4; i < ConfForSlot.global.confChunk.maxSegmentNumber(); i += 9) {
            one.setSegmentMergeFlag(i, Chunk.Flag.new_write.flagByte(), 1L, 0)
        }
        segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == -1

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }

    def 'test check if file can truncate'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        ConfForSlot.global = ConfForSlot.debugMode
        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)

        expect:
        one.name() == 'segments all merged check for truncate file'
        one.executeOnceAfterLoopCount() == 10

        when:
        8.times {
            one.run()
        }
        then:
        one.canTruncateFdIndex == 0

        when:
        one.updateBitSetCanReuseForSegmentIndex(0, 0, false)
        then:
        one.canTruncateFdIndex == -1

        when:
        one.setSegmentMergeFlag(8192, Chunk.Flag.new_write.flagByte(), 1L, 0)
        one.updateBitSetCanReuseForSegmentIndex(1, 0, false)
        8.times {
            one.run()
        }
        then:
        one.canTruncateFdIndex == -1

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }
}
