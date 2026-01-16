package io.velo.repl

import io.netty.buffer.Unpooled
import spock.lang.Specification

class ReplRequestTest extends Specification {
    final short slot = 0

    def 'test all'() {
        given:
        def req = new ReplRequest(1L, slot, ReplType.test, new byte[10], 20)

        expect:
        req.slaveUuid == 1L
        req.slot == slot
        req.type == ReplType.test
        req.data.length == 10
        !req.fullyRead
        req.leftToRead() == 10

        when:
        req.nextRead(Unpooled.wrappedBuffer(new byte[10]), 10)
        then:
        req.fullyRead

        when:
        req.slaveUuid = 2L
        req.slot = (short) 1
        req.type = ReplType.ping
        req.data = new byte[20]
        then:
        req.fullyRead

        when:
        def req2 = req.copyShadow()
        then:
        req2.fullyRead
    }
}
