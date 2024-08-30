package io.velo.repl.incremental

import io.velo.persist.Chunk
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XChunkSegmentFlagUpdateTest extends Specification {
    def 'test encode and decode'() {
        given:
        def x = new XChunkSegmentFlagUpdate()

        expect:
        x.type() == BinlogContent.Type.chunk_segment_flag_update
        x.isEmpty()

        when:
        x.putUpdatedChunkSegmentFlagWithSeq(0, Chunk.Flag.new_write, 0L)
        x.putUpdatedChunkSegmentFlagWithSeq(1, Chunk.Flag.new_write, 1L)

        def encoded = x.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x2 = XChunkSegmentFlagUpdate.decodeFrom(buffer)
        then:
        !x2.isEmpty()
        x2.encodedLength() == encoded.length

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XChunkSegmentFlagUpdate.decodeFrom(buffer)
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

        cleanup:
        localPersist.cleanUp()
        io.velo.persist.Consts.persistDir.deleteDir()
    }
}