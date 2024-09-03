package io.velo.repl.incremental

import io.activej.eventloop.Eventloop
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class XReverseIndexPutWordTest extends Specification {
    def 'test encode and decode'() {
        given:
        def word = 'bad'
        def x = new XReverseIndexPutWord(word, 1L)

        expect:
        x.type() == BinlogContent.Type.reverse_index_put_word

        when:
        def encoded = x.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x2 = XReverseIndexPutWord.decodeFrom(buffer)
        then:
        x2.encodedLength() == encoded.length

        when:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        def replPair = ReplPairTest.mockAsSlave()
        x.apply(slot, replPair)
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        localPersist.indexHandlerPool.getIndexHandler((byte) 0).getTotalCount(word) == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
