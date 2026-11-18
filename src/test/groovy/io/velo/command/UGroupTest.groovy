package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.reply.ErrorReply
import io.velo.reply.MultiBulkReply
import spock.lang.Specification

import java.time.Duration

class UGroupTest extends Specification {
    def _UGroup = new UGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _UGroup.parseSlots('ux', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def uGroup = new UGroup(null, null, null)
        uGroup.from(BaseCommand.mockAGroup())

        expect:
        uGroup.execute(input) == expected

        where:
        input         | expected
        'unsubscribe' | ErrorReply.FORMAT
        'ux'          | ErrorReply.UNKNOWN_COMMAND('ux')
    }

    def 'test unsubscribe'() {
        given:
        def inspector = new SocketInspector()
        LocalPersist.instance.socketInspector = inspector
        def netWorkerEventloop = Eventloop.builder()
                .withThreadName('uGroupTest-net-worker')
                .withIdleInterval(Duration.ofMillis(50))
                .build()
        netWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.sleep(200)
        inspector.initByNetWorkerEventloopArray([netWorkerEventloop] as Eventloop[], [netWorkerEventloop] as Eventloop[])

        and:
        def socket = SocketInspectorTest.mockTcpSocket()
        def otherSocket = SocketInspectorTest.mockTcpSocket()

        def uGroup = new UGroup('unsubscribe', null, socket)
        uGroup.from(BaseCommand.mockAGroup())

        // this client subscribes to a, b, c; another client also subscribes to a
        inspector.subscribe('a', false, socket)
        inspector.subscribe('b', false, socket)
        inspector.subscribe('c', false, socket)
        inspector.subscribe('a', false, otherSocket)

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        uGroup.data = data2
        def reply = uGroup.unsubscribe(false)
        then:
        // redis counts this client's remaining subscriptions (b, c), not the channel socket count
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        (reply as MultiBulkReply).replies[2].getInteger() == 2L

        when:
        // a channel this client was not subscribed to still replies with this client's own count
        def data2d = new byte[2][]
        data2d[1] = 'd'.bytes
        uGroup.data = data2d
        def reply2 = uGroup.unsubscribe(false)
        then:
        (reply2 as MultiBulkReply).replies[2].getInteger() == 2L

        when:
        def data3 = new byte[3][]
        data3[1] = 'b'.bytes
        data3[2] = 'c'.bytes
        uGroup.data = data3
        def reply3 = uGroup.unsubscribe(false)
        then:
        (reply3 as MultiBulkReply).replies.length == 6
        (reply3 as MultiBulkReply).replies[2].getInteger() == 1L
        (reply3 as MultiBulkReply).replies[5].getInteger() == 0L

        when:
        def data1 = new byte[1][]
        uGroup.data = data1
        reply = uGroup.unsubscribe(false)
        then:
        reply == ErrorReply.FORMAT

        when:
        // a socket that never subscribed gets the redis count 0
        def freshSocket = SocketInspectorTest.mockTcpSocket()
        then:
        inspector.unsubscribe('a', false, freshSocket) == 0

        cleanup:
        netWorkerEventloop.breakEventloop()
        LocalPersist.instance.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
