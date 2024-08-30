package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import io.velo.BaseCommand
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.reply.*
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class CGroupTest extends Specification {
    final short slot = 0

    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        when:
        LocalPersist.instance.addOneSlotForTest2(slot)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def sCopyList = CGroup.parseSlots('copy', data3, slotNumber)
        def sConfigList = CGroup.parseSlots('config', data3, slotNumber)
        then:
        sCopyList.size() == 2
        sConfigList.size() == 1
        sConfigList[0].slot() == slot

        when:
        def data2 = new byte[2][]
        sCopyList = CGroup.parseSlots('copy', data2, slotNumber)
        then:
        sCopyList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('client', data1, null)
        cGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'config'
        cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'copy'
        reply = cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'zzz'
        reply = cGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test client'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'id'.bytes

        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        def cGroup = new CGroup('client', data2, socket)
        cGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = cGroup.client()
        then:
        reply instanceof IntegerReply

        when:
        data2[1] = 'setinfo'.bytes
        reply = cGroup.client()
        then:
        reply == OKReply.INSTANCE

        when:
        data2[1] = 'zzz'.bytes
        reply = cGroup.client()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test copy'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def cGroup = new CGroup('copy', data3, null)
        cGroup.byPassGetSet = inMemoryGetSet
        cGroup.from(BaseCommand.mockAGroup())

        when:
        cGroup.slotWithKeyHashListParsed = CGroup.parseSlots('copy', data3, cGroup.slotNumber)
        def reply = cGroup.copy()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = io.velo.persist.Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = cGroup.copy()
        then:
        reply == IntegerReply.REPLY_1

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'replace_'.bytes
        cGroup.data = data4
        reply = cGroup.copy()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[3] = 'replace'.bytes
        reply = cGroup.copy()
        then:
        reply == IntegerReply.REPLY_1

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        cGroup.crossRequestWorker = true
        reply = cGroup.copy()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')

        reply = cGroup.copy()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        data4[3] = 'replace_'.bytes
        reply = cGroup.copy()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
