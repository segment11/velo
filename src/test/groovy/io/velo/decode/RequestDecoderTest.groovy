package io.velo.decode

import io.activej.bytebuf.ByteBuf
import io.activej.bytebuf.ByteBufs
import io.velo.repl.Repl
import io.velo.repl.ReplType
import io.velo.repl.content.Ping
import spock.lang.Specification

import java.nio.ByteBuffer

class RequestDecoderTest extends Specification {
    def "test decode"() {
        given:
        def decoder = new RequestDecoder()

        and:
        def buf1 = ByteBuf.wrapForReading(
                ('*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n' +
                        '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n').bytes
        )
        def buf2 = ByteBuf.wrapForReading(
                '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'.bytes
        )

        def bufs = new ByteBufs(2)
        bufs.add(buf1)
        bufs.add(buf2)

        when:
        def requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 3
        requestList.every { !it.isHttp() && !it.isRepl() }

        when:
        def buf3 = ByteBuf.wrapForReading(
                '*'.bytes
        )
        def bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        def requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.isEmpty()

        when:
        buf3 = ByteBuf.wrapForReading(
                '+0\r\n'.bytes
        )
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.size() == 1

        when:
        buf3 = ByteBuf.wrapForReading(
                '*2\r\r\r\r\r\r'.bytes
        )
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.isEmpty()

        when:
        buf3 = ByteBuf.wrapForReading(
                '*1\r\n$'.bytes
        )
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.isEmpty()

        when:
        buf3 = ByteBuf.wrapForReading(
                '*1\r\n$1'.bytes
        )
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.isEmpty()
    }

    def "test decode http"() {
        given:
        def decoder = new RequestDecoder()

        and:
        def buf = ByteBuf.wrapForReading(
                "GET /?get&mykey HTTP/1.1\r\nHost: localhost:8080\r\nConnection: keep-alive\r\n\r\n".bytes
        )

        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].isHttp()
        requestList[0].data.length == 2
        requestList[0].data[0] == 'get'.bytes
        requestList[0].data[1] == 'mykey'.bytes

        when:
        buf = ByteBuf.wrapForReading(
                "POST / HTTP/1.1\r\nContent-Length: 9\r\n\r\nget mykey".bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].isHttp()
        requestList[0].data.length == 2
        requestList[0].data[0] == 'get'.bytes
        requestList[0].data[1] == 'mykey'.bytes

        when:
        buf = ByteBuf.wrapForReading(
                "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n".bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].data[0] == 'zzz'.bytes

        when:
        buf = ByteBuf.wrapForReading(
                "PUT / HTTP/1.1\r\nContent-Length: 0\r\n\r\n".bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].data[0] == 'zzz'.bytes

        when:
        // http not ok
        def buf2 = ByteBuf.wrapForReading(
                "GET /?set&mykey&myvalue HTTP/1.1\r\nHost: localhost:8080\r\nConnection: keep-alive\r\n".bytes
        )
        def bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        def requestList2 = decoder.tryDecode(bufs2)
        then:
        requestList2.isEmpty()

        when:
        // no params
        def buf3 = ByteBuf.wrapForReading(
                "GET /xxx HTTP/1.1\r\nHost: localhost:8080\r\nConnection: keep-alive\r\n\r\n".bytes
        )
        def bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        def requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList.size() == 1
        requestList[0].data[0] == 'zzz'.bytes

        when:
        buf3 = ByteBuf.wrapForReading(
                "DELETE /xxx HTTP/1.1\r\nHost: localhost:8080\r\nConnection: keep-alive\r\n\r\n".bytes
        )
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList.size() == 1
        requestList[0].data[0] == 'zzz'.bytes

        when:
        buf3 = ByteBuf.wrapForReading(new byte[1])
        bufs3 = new ByteBufs(1)
        bufs3.add(buf3)
        buf3.head(1)
        requestList3 = decoder.tryDecode(bufs3)
        then:
        requestList3.isEmpty()
    }

    def 'test decode repl'() {
        given:
        def decoder = new RequestDecoder()

        and:
        def ping = new Ping('127.0.0.1:7379')
        def buf = Repl.buffer(0L, (byte) 0, ReplType.ping, ping)

        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].isRepl()
        requestList[0].replRequest != null
        requestList[0].replRequest.slaveUuid == 0L
        requestList[0].replRequest.slot == 0
        requestList[0].replRequest.type == ReplType.ping
        requestList[0].replRequest.data.length == ping.encodeLength()

        when:
        def buf2 = ByteBuf.wrapForReading(
                "X-REPLx".bytes
        )
        def bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        def requestList2 = decoder.tryDecode(bufs2)
        then:
        requestList2.isEmpty()

        when:
        def bb = new byte[6 + 14 + 1]
        def buffer = ByteBuffer.wrap(bb)
        buffer.put('X-REPL'.bytes)
        buffer.putLong(0L)
        buffer.putShort((short) -1)
        buf2 = ByteBuf.wrapForReading(bb)
        bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        requestList = decoder.tryDecode(bufs2)
        then:
        requestList.isEmpty()
    }
}
