package io.velo.repl.incremental

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XFlushTest extends Specification {
    def 'test encode and decode'() {
        given:
        def xFlush = new XFlush()

        expect:
        xFlush.type() == BinlogContent.Type.flush

        when:
        def encoded = xFlush.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def xFlush1 = XFlush.decodeFrom(buffer)
        then:
        xFlush1.encodedLength() == encoded.length

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XFlush.decodeFrom(buffer)
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
        xFlush.apply(slot, replPair)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
