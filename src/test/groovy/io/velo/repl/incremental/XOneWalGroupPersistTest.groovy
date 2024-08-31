package io.velo.repl.incremental

import io.velo.ConfForSlot
import io.velo.persist.Chunk
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XOneWalGroupPersistTest extends Specification {
    def 'test encode and decode'() {
        given:
        def x = new XOneWalGroupPersist(true, true, 0)

        expect:
        x.type() == BinlogContent.Type.one_wal_group_persist

        when:
        x.beginBucketIndex = 0
        x.keyCountForStatsTmp = [1, 2, 3]
        def sharedBytesList = new byte[3][]
        sharedBytesList[0] = new byte[ConfForSlot.global.confWal.oneChargeBucketNumber * 4096]
        x.sharedBytesListBySplitIndex = sharedBytesList
        x.oneWalGroupSeqArrayBySplitIndex = [0L, 1L, 2L]
        byte[] splitNumberAfterPut = [3, 3, 3]
        x.splitNumberAfterPut = splitNumberAfterPut
        x.putUpdatedChunkSegmentFlagWithSeq(0, Chunk.Flag.new_write, 0L)
        x.putUpdatedChunkSegmentFlagWithSeq(1, Chunk.Flag.new_write, 1L)
        x.putUpdatedChunkSegmentBytes(0, new byte[4096])
        x.chunkSegmentIndexAfterPersist = 100
        x.chunkMergedSegmentIndexEndLastTime = 1000
        x.lastSegmentSeq = 1L

        def encoded = x.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x2 = XOneWalGroupPersist.decodeFrom(buffer)
        then:
        x2.encodedLength() == encoded.length

        when:
        buffer.position(1 + 4)
        buffer.put((byte) 0)
        buffer.put((byte) 0)
        // skip type
        buffer.position(1)
        x2 = XOneWalGroupPersist.decodeFrom(buffer)
        then:
        x2.encodedLength() == encoded.length
        x2.encodeWithType().length == encoded.length

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XOneWalGroupPersist.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        x.apply(slot, replPair)
        then:
        1 == 1

        when:
        x.shortValue = false
        x.chunkMergedSegmentIndexEndLastTime = -1
        x.lastSegmentSeq = 0L
        x.apply(slot, replPair)
        then:
        1 == 1

        when:
        x.clearWalAfterApply = false
        x.apply(slot, replPair)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
