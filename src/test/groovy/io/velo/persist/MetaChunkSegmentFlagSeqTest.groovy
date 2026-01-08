package io.velo.persist

import io.velo.ConfForSlot
import spock.lang.Specification

import static Consts.getSlotDir

class MetaChunkSegmentFlagSeqTest extends Specification {
    final short slot = 0

    def 'test read write seq'() {
        given:
        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        one.setSegmentMergeFlag(10, Chunk.Flag.new_write.flagByte(), 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)
        then:
        segmentFlag.flagByte() == Chunk.Flag.new_write.flagByte()
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'test read batch for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq(slot, slotDir)

        when:
        List<Long> seqLongList = []
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, Chunk.Flag.new_write.flagByte(), seqLongList, 0)
        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        cleanup:
        one.clear()
        one.cleanUp()
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
                true
            }
        }
        new File('chunk_segment_flag_range.txt').withWriter { writer ->
            one.iterateRange(1024, 1024) { segmentIndex, flagByte, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flagByte, $seq, $walGroupIndex")
                true
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
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.markPersistedSegmentIndexToTargetWalGroup(0, 0, (short) 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 10

        when:
        one.markPersistedSegmentIndexToTargetWalGroup(0, 0, (short) 34)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 17

        when:
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 17
        r[1] == 17

        when:
        one.preReadFindTimesForOncePersist = 1
        one.markPersistedSegmentIndexToTargetWalGroup(0, 0, (short) 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 5

        when:
        one.markPersistedSegmentIndexToTargetWalGroup(0, 0, (short) 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 5
        r[1] == 5

        when:
        99.times {
            one.markPersistedSegmentIndexToTargetWalGroup(0, it * 10, (short) 10)
        }
        200.times {
            // find then clear
            one.findThoseNeedToMerge(0)
        }
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        10.times {
            one.markPersistedSegmentIndexToTargetWalGroup(0, it, (short) 1)
        }
        r = one.findThoseNeedToMerge(0)
        then:
        r[1] == 1

        when:
        one.clear()
        def markedCount = one.reloadMarkPersistedSegmentIndex()
        then:
        markedCount == 0

        when:
        one.setSegmentMergeFlag(0, Chunk.Flag.new_write.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(1, Chunk.Flag.new_write.flagByte(), 1L, 0)
        one.setSegmentMergeFlag(2, Chunk.Flag.new_write.flagByte(), 1L, 1)
        one.setSegmentMergeFlag(3, Chunk.Flag.new_write.flagByte(), 1L, 1)
        one.setSegmentMergeFlag(5, Chunk.Flag.new_write.flagByte(), 1L, 2)
        markedCount = one.reloadMarkPersistedSegmentIndex()
        one.printMarkedPersistedSegmentIndex(0)
        one.printMarkedPersistedSegmentIndex(1)
        one.printMarkedPersistedSegmentIndex(2)
        then:
        markedCount == 3

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
