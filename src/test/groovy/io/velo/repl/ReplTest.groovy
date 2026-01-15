package io.velo.repl

import io.netty.buffer.Unpooled
import io.velo.repl.content.Hello
import io.velo.repl.content.Ping
import spock.lang.Specification

import java.nio.ByteBuffer

class ReplTest extends Specification {
    def 'test all'() {
        given:
        final short slot = 0
        final ReplPair replPair = ReplPairTest.mockAsSlave()

        Repl.test(slot, replPair, 'test')
        Repl.error(slot, replPair, 'error')
        Repl.error(slot, replPair.slaveUuid, 'error')

        when:
        def ping = new Ping('localhost:6380')
        def reply = Repl.reply(slot, replPair, ReplType.ping, ping)
        then:
        reply.isReplType(ReplType.ping)
        !reply.isReplType(ReplType.pong)
        !reply.isEmpty()
        reply.buffer().limit() == Repl.HEADER_LENGTH + ping.encodeLength()
        !Repl.emptyReply().isReplType(ReplType.pong)
        Repl.emptyReply().isEmpty()

        when:
        def emptyReply = Repl.emptyReply()
        then:
        emptyReply.isEmpty()

        when:
        def pingBytes = reply.buffer().array()
        def nettyBuf = Unpooled.wrappedBuffer(pingBytes)
        def request = Repl.decode(nettyBuf)
        then:
        request.isFullyRead()
        request.slaveUuid == replPair.slaveUuid
        request.slot == slot
        request.type == ReplType.ping
        new String(request.data) == 'localhost:6380'

        when:
        def hello = new Hello(replPair.slaveUuid, 'localhost:6380')
        def reply2 = Repl.reply(slot, replPair, ReplType.hello, hello)
        def helloBytes = reply2.buffer().array()
        def nettyBuf2 = Unpooled.wrappedBuffer(helloBytes)
        def request2 = Repl.decode(nettyBuf2)
        then:
        request2.isFullyRead()
        request2.slaveUuid == replPair.slaveUuid
        request2.slot == slot
        request2.type == ReplType.hello
        request2.data.length == hello.encodeLength()

        when:
        pingBytes[Repl.PROTOCOL_KEYWORD_BYTES.length + 8] = -1
        nettyBuf.readerIndex(0)
        boolean exception = false
        try {
            Repl.decode(nettyBuf)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ByteBuffer.wrap(pingBytes).putShort(Repl.PROTOCOL_KEYWORD_BYTES.length + 8, (short) 0)
        ByteBuffer.wrap(pingBytes).putShort(Repl.PROTOCOL_KEYWORD_BYTES.length + 8 + 2, (short) -10)
        nettyBuf.readerIndex(0)
        request = Repl.decode(nettyBuf)
        then:
        request == null

        when:
        pingBytes[Repl.PROTOCOL_KEYWORD_BYTES.length + 8 + 1] = ReplType.ping.code
        def lessBytes = new byte[pingBytes.length - 1]
        System.arraycopy(pingBytes, 0, lessBytes, 0, lessBytes.length)
        request = Repl.decode(Unpooled.wrappedBuffer(lessBytes))
        then:
        request == null

        when:
        def nettyBuffer2 = Unpooled.wrappedBuffer(new byte[1])
        def data2 = Repl.decode(nettyBuffer2)
        then:
        data2 == null

        when:
        def xx = new Repl.ReplReplyFromBytes(1L, (short) 0, ReplType.ping, new byte[10], 5, 5)
        then:
        xx.buffer().limit() == Repl.HEADER_LENGTH + 5
    }
}
