package io.velo.decode

import io.activej.bytebuf.ByteBuf
import io.activej.bytebuf.ByteBufs
import io.velo.repl.Repl
import io.velo.repl.ReplType
import io.velo.repl.content.Ping
import spock.lang.Specification

import java.nio.ByteBuffer

class ReplDecoderTest extends Specification {
    def 'test decode'() {
        given:
        def decoder = new ReplDecoder()

        when:
        // fully read
        def ping = new Ping('127.0.0.1:7379')
        def buf = Repl.buffer(0L, (byte) 0, ReplType.ping, ping)
        def bufs = new ByteBufs(1)
        bufs.add(buf)
        def requestList = decoder.tryDecode(bufs)
        then:
        requestList.size() == 1
        requestList[0].slaveUuid == 0L
        requestList[0].slot == 0
        requestList[0].type == ReplType.ping
        requestList[0].data.length == ping.encodeLength()
        decoder.toFullyReadRequest == null

        when:
        // data not ready
        def buf2 = ByteBuf.wrapForReading(
                "X-REPLx".bytes
        )
        def bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        def requestList2 = decoder.tryDecode(bufs2)
        then:
        requestList2.isEmpty()

        when:
        // 22 is header
        def bb = new byte[22 + 10]
        def buffer = ByteBuffer.wrap(bb)
        buffer.put('X-REPL'.bytes)
        buffer.putLong(0L)
        buffer.putShort((short) 0)
        buffer.putShort((short) ReplType.test.code)
        // expect data length 20
        buffer.putInt(20)
        buf2 = ByteBuf.wrapForReading(bb)
        bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        requestList2 = decoder.tryDecode(bufs2)
        then:
        // not fully read, left 10 need read
        requestList2.isEmpty()
        decoder.toFullyReadRequest != null

        when:
        def bb2 = new byte[5]
        buf2 = ByteBuf.wrapForReading(bb2)
        bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        // read left 5 bytes
        requestList2 = decoder.tryDecode(bufs2)
        then:
        requestList2.isEmpty()
        decoder.toFullyReadRequest != null

        when:
        def bb3 = new byte[5]
        buf2 = ByteBuf.wrapForReading(bb3)
        bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        // read left 5 bytes
        requestList2 = decoder.tryDecode(bufs2)
        then:
        requestList2.size() == 1
        requestList2[0].data.length == 20
        requestList2[0].fullyRead
        decoder.toFullyReadRequest == null
    }
}
