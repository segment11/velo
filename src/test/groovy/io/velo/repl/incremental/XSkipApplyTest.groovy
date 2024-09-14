package io.velo.repl.incremental

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XSkipApplyTest extends Specification {
    def 'test encode and decode'() {
        given:
        def xSkipApply = new XSkipApply(1L, 0, -1)
        println xSkipApply.seq
        println xSkipApply.chunkCurrentSegmentIndex
        println xSkipApply.chunkMergedSegmentIndexEndLastTime

        expect:
        xSkipApply.type() == BinlogContent.Type.skip_apply

        when:
        def encoded = xSkipApply.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def xSkipApply1 = XSkipApply.decodeFrom(buffer)
        then:
        xSkipApply1.encodedLength() == encoded.length
        xSkipApply1.seq == xSkipApply.seq
        xSkipApply1.chunkCurrentSegmentIndex == xSkipApply.chunkCurrentSegmentIndex
        xSkipApply1.chunkMergedSegmentIndexEndLastTime == xSkipApply.chunkMergedSegmentIndexEndLastTime

        when:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def replPair = ReplPairTest.mockAsSlave()
        xSkipApply.apply(slot, replPair)
        then:
        replPair.slaveCatchUpLastSeq == xSkipApply.seq
        oneSlot.chunk.segmentIndex == xSkipApply.chunkCurrentSegmentIndex
        oneSlot.chunk.mergedSegmentIndexEndLastTime == xSkipApply.chunkMergedSegmentIndexEndLastTime

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
