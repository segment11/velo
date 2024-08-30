package io.velo.command

import io.activej.net.socket.tcp.TcpSocket
import io.velo.BaseCommand
import io.velo.SocketInspector
import io.velo.persist.LocalPersist
import io.velo.reply.ErrorReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
import spock.lang.Specification

import java.nio.channels.SocketChannel

class UGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = UGroup.parseSlots('ux', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def uGroup = new UGroup('unsubscribe', data1, null)
        uGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = uGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        uGroup.cmd = 'ux'
        reply = uGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test subscribe'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        def uGroup = new UGroup('unsubscribe', data4, socket)
        uGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.socketInspector = new SocketInspector()
        def reply = uGroup.unsubscribe()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 3 * 3

        when:
        def data1 = new byte[1][]
        uGroup.data = data1
        reply = uGroup.unsubscribe()
        then:
        reply == ErrorReply.FORMAT
    }
}
