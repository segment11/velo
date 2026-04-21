package io.velo.decode

import io.activej.bytebuf.ByteBuf
import io.activej.bytebuf.ByteBufs
import io.activej.common.exception.MalformedDataException
import io.velo.repl.Repl
import io.velo.repl.ReplType
import io.velo.repl.content.Ping
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

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
        decoder.tryDecode(bufs3)
        then:
        thrown(MalformedDataException)

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

    def 'test decode http uses utf8 tokens'() {
        given:
        def decoder = new RequestDecoder()

        when:
        def getBuf = ByteBuf.wrapForReading(
                "GET /?get&%E4%BD%A0%E5%A5%BD HTTP/1.1\r\nHost: localhost:8080\r\n\r\n".getBytes(StandardCharsets.UTF_8)
        )
        def getBufs = new ByteBufs(1)
        getBufs.add(getBuf)
        def getRequestList = decoder.tryDecode(getBufs)

        then:
        getRequestList.size() == 1
        new String(getRequestList[0].data[1], StandardCharsets.UTF_8) == '你好'

        when:
        def postBody = 'set 你好 值'
        def postBuf = ByteBuf.wrapForReading(
                "POST / HTTP/1.1\r\nContent-Length: ${postBody.getBytes(StandardCharsets.UTF_8).length}\r\n\r\n${postBody}"
                        .getBytes(StandardCharsets.UTF_8)
        )
        def postBufs = new ByteBufs(1)
        postBufs.add(postBuf)
        def postRequestList = decoder.tryDecode(postBufs)

        then:
        postRequestList.size() == 1
        new String(postRequestList[0].data[1], StandardCharsets.UTF_8) == '你好'
        new String(postRequestList[0].data[2], StandardCharsets.UTF_8) == '值'
    }

    def 'test decode http query decodes tokens after splitting'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                "GET /?get&my%26key HTTP/1.1\r\nHost: localhost:8080\r\n\r\n".getBytes(StandardCharsets.UTF_8)
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 1
        requestList[0].data.length == 2
        new String(requestList[0].data[1], StandardCharsets.UTF_8) == 'my&key'
    }

    def 'test decode malformed complete http request line throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                "GET /bad\r\nHost: localhost:8080\r\n\r\n".bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }

    def 'test decode malformed http header line throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                "GET / HTTP/1.1\r\nHost localhost\r\n\r\n".bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }

    def 'test decode malformed resp bulk string delimiter throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                '*1\r\n$4\r\nPINGxx'.bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }

    def 'test decode malformed resp line ending throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                '*1x\n$4\r\nPING\r\n'.bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }

    def 'test decode oversized resp number throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                '*4294967297\r\n'.bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }

    def 'test decode negative resp bulk length throws'() {
        given:
        def decoder = new RequestDecoder()
        def buf = ByteBuf.wrapForReading(
                '*1\r\n$-1\r\n'.bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
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
        boolean exception = false
        try {
            requestList = decoder.tryDecode(bufs2)
        } catch (Exception e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test decode fragmented http prefix shorter than 6 bytes'() {
        given:
        def decoder = new RequestDecoder()
        def bufs = new ByteBufs(2)

        when:
        bufs.add(ByteBuf.wrapForReading('GE'.bytes))
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.isEmpty()

        when:
        bufs.add(ByteBuf.wrapForReading('T /?get&mykey HTTP/1.1\r\nHost: localhost:8080\r\n\r\n'.bytes))
        requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 1
        requestList[0].isHttp()
        requestList[0].data.length == 2
        requestList[0].data[0] == 'get'.bytes
        requestList[0].data[1] == 'mykey'.bytes
    }

    def 'test decode fragmented repl prefix shorter than 6 bytes'() {
        given:
        def decoder = new RequestDecoder()
        def ping = new Ping('127.0.0.1:7379')
        def replBuf = Repl.buffer(0L, (byte) 0, ReplType.ping, ping)
        def replBytes = replBuf.array()
        def firstPart = Arrays.copyOfRange(replBytes, 0, 4)
        def secondPart = Arrays.copyOfRange(replBytes, 4, replBuf.tail())
        def bufs = new ByteBufs(2)

        when:
        bufs.add(ByteBuf.wrapForReading(firstPart))
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.isEmpty()

        when:
        bufs.add(ByteBuf.wrapForReading(secondPart))
        requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 1
        requestList[0].isRepl()
        requestList[0].replRequest != null
        requestList[0].replRequest.type == ReplType.ping
    }

    def 'test decode unknown repl type throws'() {
        given:
        def decoder = new RequestDecoder()
        def bb = new byte[21 + 1]
        def buffer = ByteBuffer.wrap(bb)
        buffer.put('X-REPL'.bytes)
        buffer.putLong(0L)
        buffer.putShort((short) 0)
        buffer.put((byte) 127)
        buffer.putInt(1)
        buffer.put((byte) 1)
        def bufs = new ByteBufs(1)
        bufs.add(ByteBuf.wrapForReading(bb))

        when:
        decoder.tryDecode(bufs)

        then:
        thrown(MalformedDataException)
    }
}
