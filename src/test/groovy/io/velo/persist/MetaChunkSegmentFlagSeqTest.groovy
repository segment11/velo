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
        one.setSegmentMergeFlag(10, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)
        then:
        segmentFlag.flagByte() == Chunk.SEGMENT_FLAG_HAS_DATA
        segmentFlag.segmentSeq() == 1L
        segmentFlag.walGroupIndex() == 0

        when:
        def sb = new StringBuilder()
        one.estimate(sb)
        println sb.toString()
        then:
        1 == 1

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
        one.setSegmentMergeFlagBatch(10, 10, Chunk.SEGMENT_FLAG_HAS_DATA, seqLongList, 0)
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
        def markPersisted = { int walGroupIndex, int beginSegmentIndex, short segmentCount ->
            one.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, beginSegmentIndex, segmentCount)
            segmentCount.times { i ->
                one.setSegmentMergeFlag(beginSegmentIndex + i, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, walGroupIndex)
            }
        }

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
        markPersisted(0, 0, (short) 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 10

        when: 'commit the range'
        one.commitMergedRange(0, r[0], r[1])
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when: 'mark 34 segments — split returns first half (17)'
        markPersisted(0, 0, (short) 34)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 17

        when: 'commit first half, mark REUSABLE, then find second half'
        one.commitMergedRange(0, 0, 17)
        one.setSegmentMergeFlagBatch(0, 17, Chunk.SEGMENT_FLAG_REUSABLE, null, 0)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 17
        r[1] == 17

        when: 'commit second half'
        one.commitMergedRange(0, 17, 17)
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.splitMarkedRunForPreRead = MetaChunkSegmentFlagSeq.SPLIT_MARKED_RUN_ONCE_FOR_PRE_READ
        markPersisted(0, 0, (short) 10)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 0
        r[1] == 5

        when: 'commit first half, mark REUSABLE, find second half'
        one.commitMergedRange(0, 0, 5)
        one.setSegmentMergeFlagBatch(0, 5, Chunk.SEGMENT_FLAG_REUSABLE, null, 0)
        r = one.findThoseNeedToMerge(0)
        then:
        r[0] == 5
        r[1] == 5

        when: 'commit second half'
        one.commitMergedRange(0, 5, 5)
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.splitMarkedRunForPreRead = MetaChunkSegmentFlagSeq.PRE_READ_WHOLE_MARKED_RUN
        99.times {
            markPersisted(0, it * 10, (short) 10)
        }
        200.times {
            def found = one.findThoseNeedToMerge(0)
            if (found[0] != -1) {
                one.commitMergedRange(0, found[0], found[1])
            }
        }
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.clear()
        100.times { i ->
            markPersisted(0, i, (short) 1)
        }
        then:
        noExceptionThrown()

        when:
        one.markPersistedSegmentIndexToTargetWalGroup(0, 100, (short) 1)
        then:
        thrown(IllegalStateException)

        when:
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        r = one.findThoseNeedToMerge(0)
        one.commitMergedRange(0, r[0], r[1])
        markPersisted(0, 100, (short) 1)
        then:
        r[1] == 1
        noExceptionThrown()

        when:
        one.clear()
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        markPersisted(0, 0, (short) 1)
        one.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_REUSABLE, 0L, 0)
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.clear()
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        markPersisted(0, 0, (short) 4)
        one.setSegmentMergeFlagBatch(0, 4, Chunk.SEGMENT_FLAG_REUSABLE, null, 0)
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.clear()
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        one.splitMarkedRunForPreRead = MetaChunkSegmentFlagSeq.SPLIT_MARKED_RUN_ONCE_FOR_PRE_READ
        markPersisted(0, 0, (short) 4)
        one.setSegmentMergeFlagBatch(0, 4, Chunk.SEGMENT_FLAG_REUSABLE, null, 0)
        r = one.findThoseNeedToMerge(0)
        then:
        r == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        when:
        one.splitMarkedRunForPreRead = MetaChunkSegmentFlagSeq.PRE_READ_WHOLE_MARKED_RUN
        one.clear()
        def markedCount = one.reloadMarkPersistedSegmentIndex()
        then:
        markedCount == 0

        when:
        one.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
        one.setSegmentMergeFlag(1, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
        one.setSegmentMergeFlag(2, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 1)
        one.setSegmentMergeFlag(3, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 1)
        one.setSegmentMergeFlag(5, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 2)
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

    def 'test reload marks persisted segment run ending at chunk tail'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        ConfForSlot.global = ConfForSlot.debugMode
        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)
        def walGroupIndex = 7
        def tailBeginSegmentIndex = ConfForSlot.global.confChunk.maxSegmentNumber() - 2

        when:
        one.setSegmentMergeFlagBatch(tailBeginSegmentIndex, 2, Chunk.SEGMENT_FLAG_HAS_DATA, [1L, 2L], walGroupIndex)
        def markedCount = one.reloadMarkPersistedSegmentIndex()
        def needMerge = one.findThoseNeedToMerge(walGroupIndex)

        then:
        markedCount == 1
        needMerge[0] == tailBeginSegmentIndex
        needMerge[1] == 2

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
        one.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
        segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == 1

        when:
        one.setSegmentMergeFlag(2, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
        segmentIndex = one.findCanReuseSegmentIndex(0, 10)
        then:
        segmentIndex == 3

        when:
        for (int i = 4; i < ConfForSlot.global.confChunk.maxSegmentNumber(); i += 9) {
            one.setSegmentMergeFlag(i, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
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
        one.setSegmentMergeFlag(8192, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, 0)
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

    def 'test truncate check uses per-FD bitset index for fd index > 0'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        ConfForSlot.global = ConfForSlot.debugMode
        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)

        when: 'make FD 0 not fully reusable so truncation does not stop at FD 0'
        one.updateBitSetCanReuseForSegmentIndex(0, 0, false)

        and: 'run 16 iterations: 8 for FD 0 (fails), 8 for FD 1 (all reusable by default)'
        16.times {
            one.run()
        }

        then: 'FD 1 segments are all reusable, so canTruncateFdIndex should be 1'
        one.canTruncateFdIndex == 1

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }

    def 'test find those need to merge does not consume marker until commit'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)
        one.isOverHalfSegmentNumberForFirstReuseLoop = true

        def markPersisted = { int walGroupIndex, int beginSegmentIndex, short segmentCount ->
            one.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, beginSegmentIndex, segmentCount)
            segmentCount.times { i ->
                one.setSegmentMergeFlag(beginSegmentIndex + i, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, walGroupIndex)
            }
        }

        when: 'mark 10 segments for merge'
        markPersisted(0, 0, (short) 10)
        def r1 = one.findThoseNeedToMerge(0)

        then: 'range returned'
        r1[0] == 0
        r1[1] == 10

        when: 'find again WITHOUT committing — marker should still be present'
        def r2 = one.findThoseNeedToMerge(0)

        then: 'same range returned again (marker was not consumed)'
        r2[0] == 0
        r2[1] == 10

        when: 'commit the merged range'
        one.commitMergedRange(0, r1[0], r1[1])

        then: 'marker is consumed, no more range to merge'
        one.findThoseNeedToMerge(0) == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }

    def 'test commitMergedRange handles split case'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)
        one.isOverHalfSegmentNumberForFirstReuseLoop = true
        one.splitMarkedRunForPreRead = MetaChunkSegmentFlagSeq.SPLIT_MARKED_RUN_ONCE_FOR_PRE_READ

        def markPersisted = { int walGroupIndex, int beginSegmentIndex, short segmentCount ->
            one.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, beginSegmentIndex, segmentCount)
            segmentCount.times { i ->
                one.setSegmentMergeFlag(beginSegmentIndex + i, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, walGroupIndex)
            }
        }

        when: 'mark 10 segments with split flag — findThoseNeedToMerge returns first half (5)'
        markPersisted(0, 0, (short) 10)
        def r1 = one.findThoseNeedToMerge(0)

        then: 'first half returned'
        r1[0] == 0
        r1[1] == 5

        when: 'find again without commit — still returns first half (marker not consumed)'
        def r2 = one.findThoseNeedToMerge(0)

        then:
        r2[0] == 0
        r2[1] == 5

        when: 'commit first half — marker should update to second half'
        one.commitMergedRange(0, 0, 5)
        // mark first half as REUSABLE so isMarkedSegmentRangeStillMergeable passes for second half
        one.setSegmentMergeFlagBatch(0, 5, Chunk.SEGMENT_FLAG_REUSABLE, null, 0)
        def r3 = one.findThoseNeedToMerge(0)

        then: 'second half returned'
        r3[0] == 5
        r3[1] == 5

        when: 'commit second half'
        one.commitMergedRange(0, 5, 5)

        then: 'no more ranges'
        one.findThoseNeedToMerge(0) == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }

    def 'test findThoseNeedToMerge returns marker array index for O(1) commitMergedRange'() {
        given:
        final File slotDirTmp = new File(Consts.persistDir, 'test-slot-tmp')
        slotDirTmp.mkdirs()

        def one = new MetaChunkSegmentFlagSeq(slot, slotDirTmp)
        one.isOverHalfSegmentNumberForFirstReuseLoop = true

        def markPersisted = { int walGroupIndex, int beginSegmentIndex, short segmentCount ->
            one.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, beginSegmentIndex, segmentCount)
            segmentCount.times { i ->
                one.setSegmentMergeFlag(beginSegmentIndex + i, Chunk.SEGMENT_FLAG_HAS_DATA, 1L, walGroupIndex)
            }
        }

        when: 'mark 3 separate ranges'
        markPersisted(0, 0, (short) 5)
        markPersisted(0, 10, (short) 3)
        markPersisted(0, 20, (short) 7)
        // add REUSABLE segments between ranges so they are separate markers
        for (int i = 5; i < 10; i++) one.setSegmentMergeFlag(i, Chunk.SEGMENT_FLAG_REUSABLE, 0L, 0)
        for (int i = 13; i < 20; i++) one.setSegmentMergeFlag(i, Chunk.SEGMENT_FLAG_REUSABLE, 0L, 0)

        def r1 = one.findThoseNeedToMerge(0)

        then: 'result has 3 elements — segmentIndex, segmentCount, markerIdx'
        r1.length == 3
        r1[0] == 0
        r1[1] == 5
        r1[2] >= 0

        when: 'commit using the markerIdx — should clear the exact marker'
        one.commitMergedRangeWithMarkerIdx(0, r1[0], r1[1], r1[2])

        then: 'first marker consumed'
        def r2 = one.findThoseNeedToMerge(0)
        r2[0] == 10
        r2[1] == 3
        r2[2] >= 0

        when: 'commit second marker using its returned index'
        one.commitMergedRangeWithMarkerIdx(0, r2[0], r2[1], r2[2])

        then: 'second marker consumed, third remains'
        def r3 = one.findThoseNeedToMerge(0)
        r3[0] == 20
        r3[1] == 7

        when: 'commit third'
        one.commitMergedRangeWithMarkerIdx(0, r3[0], r3[1], r3[2])

        then: 'all markers consumed'
        one.findThoseNeedToMerge(0) == MetaChunkSegmentFlagSeq.NOT_FIND_SEGMENT_INDEX_AND_COUNT

        cleanup:
        slotDirTmp.deleteDir()
        ConfForSlot.global = ConfForSlot.c1m
    }
}
