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

        and:
        def ping = new Ping('127.0.0.1:7379')
        def buf = Repl.buffer(0L, (byte) 0, ReplType.ping, ping)

        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def dataList = decoder.tryDecode(bufs)
        then:
        dataList.size() == 1
        dataList[0].length == 4
        ByteBuffer.wrap(dataList[0][0]).getLong() == 0L
        dataList[0][1][0] == 0
        dataList[0][2][0] == ReplType.ping.code
        dataList[0][3].length == ping.encodeLength()

        when:
        def buf2 = ByteBuf.wrapForReading(
                "X-REPLx".bytes
        )
        def bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        def dataList2 = decoder.tryDecode(bufs2)
        then:
        dataList2.isEmpty()

        when:
        def bb = new byte[6 + 14 + 1]
        def buffer = ByteBuffer.wrap(bb)
        buffer.put('X-REPL'.bytes)
        buffer.putLong(0L)
        buffer.putShort((short) -1)
        buf2 = ByteBuf.wrapForReading(bb)
        bufs2 = new ByteBufs(1)
        bufs2.add(buf2)
        dataList = decoder.tryDecode(bufs2)
        then:
        dataList.isEmpty()
    }
}
