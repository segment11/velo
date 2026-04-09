package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import io.velo.persist.Wal
import spock.lang.Specification

class PingTest extends Specification {
    def 'test all'() {
        given:
        def address = 'localhost:6380'
        def ping = new Ping(address)
        def pong = new Pong(address)

        expect:
        ping.encodeLength() == address.length()
        pong.encodeLength() == address.length()

        when:
        def bytes = new byte[ping.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        ping.encodeTo(buf)
        then:
        bytes == address.bytes

        when:
        bytes = new byte[pong.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        pong.encodeTo(buf)
        then:
        bytes == address.bytes
    }

    def 'test non ascii address uses utf8 bytes'() {
        given:
        def address = '测试:6380'
        def addressBytes = Wal.keyBytes(address)
        def ping = new Ping(address)
        def pong = new Pong(address)

        expect:
        ping.encodeLength() == addressBytes.length
        pong.encodeLength() == addressBytes.length

        when:
        def bytes = new byte[ping.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        ping.encodeTo(buf)

        then:
        bytes == addressBytes

        when:
        bytes = new byte[pong.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        pong.encodeTo(buf)

        then:
        bytes == addressBytes
    }
}
