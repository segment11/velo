package io.velo.repl.incremental

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XChunkSegmentSlimUpdateTest extends Specification {
    def 'test encode and decode'() {
        given:
        def x = new XChunkSegmentSlimUpdate(0, new byte[4096])

        expect:
        x.type() == BinlogContent.Type.chunk_segment_slim_update

        when:
        def encoded = x.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x2 = XChunkSegmentSlimUpdate.decodeFrom(buffer)
        then:
        x2.encodedLength() == encoded.length

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
        Consts.persistDir.deleteDir()
    }
}
