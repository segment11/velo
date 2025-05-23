package io.velo.repl.incremental

import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XUpdateSeqTest extends Specification {
    def 'test encode and decode'() {
        given:
        def XUpdateSeq = new XUpdateSeq(1L, System.currentTimeMillis())
        println XUpdateSeq.seq
        println XUpdateSeq.timeMillis

        expect:
        XUpdateSeq.type() == BinlogContent.Type.update_seq

        when:
        def encoded = XUpdateSeq.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def XUpdateSeq1 = XUpdateSeq.decodeFrom(buffer)
        then:
        XUpdateSeq1.encodedLength() == encoded.length
        XUpdateSeq1.seq == XUpdateSeq.seq
        XUpdateSeq1.timeMillis == XUpdateSeq.timeMillis

        when:
        Thread.sleep(100L)
        final short slot = 0
        def replPair = ReplPairTest.mockAsSlave()
        XUpdateSeq.apply(slot, replPair)
        then:
        replPair.slaveCatchUpLastSeq == XUpdateSeq.seq
        replPair.slaveCatchUpLastTimeMillisInMaster == XUpdateSeq.timeMillis
        replPair.slaveCatchUpLastTimeMillisDiff >= 100L
    }
}
