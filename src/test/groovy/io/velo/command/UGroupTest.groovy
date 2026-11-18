package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
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
        LocalPersist.instance.socketInspector = new SocketInspector()
        def uGroup = new UGroup(null, null, null)
        uGroup.from(BaseCommand.mockAGroup())

        expect:
        uGroup.execute('ux') == ErrorReply.UNKNOWN_COMMAND('ux')

        when:
        // no argument unsubscribe answers the nil channel confirmation, not an error
        def reply = uGroup.execute('unsubscribe')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        (reply as MultiBulkReply).replies[0] == new BulkReply('unsubscribe')
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE
        ((reply as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 0L
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
        ((reply as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 2L

        when:
        // a channel this client was not subscribed to still replies with this client's own count
        def data2d = new byte[2][]
        data2d[1] = 'd'.bytes
        uGroup.data = data2d
        def reply2 = uGroup.unsubscribe(false)
        then:
        ((reply2 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 2L

        when:
        // a pattern subscription is not counted in the channel confirmation, redis counts per type
        inspector.subscribe('news.*', true, socket)
        uGroup.data = data2d
        def reply2p = uGroup.unsubscribe(false)
        then:
        ((reply2p as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 2L

        when:
        // no arguments: redis unsubscribes all the channels of this client, one confirmation each
        def data1 = new byte[1][]
        uGroup.data = data1
        def reply4 = uGroup.unsubscribe(false)
        then:
        reply4 instanceof MultiBulkReply
        (reply4 as MultiBulkReply).replies.length == 6
        ((reply4 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 1L
        ((reply4 as MultiBulkReply).replies[5] as IntegerReply).getInteger() == 0L

        when:
        // subscribed to nothing (of this type): redis still replies, with a nil channel
        def reply5 = uGroup.unsubscribe(false)
        then:
        (reply5 as MultiBulkReply).replies.length == 3
        (reply5 as MultiBulkReply).replies[0] == new BulkReply('unsubscribe')
        (reply5 as MultiBulkReply).replies[1] == NilReply.INSTANCE
        ((reply5 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 0L

        when:
        // explicit channels already unsubscribed still reply, with the own count
        def data3 = new byte[3][]
        data3[1] = 'b'.bytes
        data3[2] = 'c'.bytes
        uGroup.data = data3
        def reply3 = uGroup.unsubscribe(false)
        then:
        (reply3 as MultiBulkReply).replies.length == 6
        ((reply3 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 0L
        ((reply3 as MultiBulkReply).replies[5] as IntegerReply).getInteger() == 0L

        when:
        // a socket that never subscribed gets the redis count 0
        def freshSocket = SocketInspectorTest.mockTcpSocket()
        then:
        inspector.unsubscribe('a', false, freshSocket) == 0

        when:
        // no-argument punsubscribe removes the pattern subscription, with the punsubscribe keyword
        uGroup.data = data1
        def reply6 = uGroup.unsubscribe(true)
        then:
        (reply6 as MultiBulkReply).replies.length == 3
        (reply6 as MultiBulkReply).replies[0] == new BulkReply('punsubscribe')
        (reply6 as MultiBulkReply).replies[1] == new BulkReply('news.*')
        ((reply6 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 0L

        when:
        // no pattern left: the nil channel punsubscribe confirmation
        def reply7 = uGroup.unsubscribe(true)
        then:
        (reply7 as MultiBulkReply).replies.length == 3
        (reply7 as MultiBulkReply).replies[0] == new BulkReply('punsubscribe')
        (reply7 as MultiBulkReply).replies[1] == NilReply.INSTANCE
        ((reply7 as MultiBulkReply).replies[2] as IntegerReply).getInteger() == 0L

        cleanup:
        netWorkerEventloop.breakEventloop()
        LocalPersist.instance.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
