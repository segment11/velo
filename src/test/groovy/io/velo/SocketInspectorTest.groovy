package io.velo

import io.activej.async.callback.AsyncComputation
import io.activej.common.function.SupplierEx
import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import io.velo.command.XGroup
import io.velo.repl.ReplPairTest
import io.velo.reply.BulkReply
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class SocketInspectorTest extends Specification {
    static TcpSocket mockTcpSocket(Eventloop eventloop = null, int port = 46379) {
        return TcpSocket.wrapChannel(eventloop, SocketChannel.open(),
                new InetSocketAddress('localhost', port), null)
    }

    def 'test base'() {
        given:
        def socket = mockTcpSocket()

        expect:
        !SocketInspector.isResp3(null)
        !SocketInspector.isResp3(socket)
        !SocketInspector.isConnectionReadonly(null)
        !SocketInspector.isConnectionReadonly(socket)
        SocketInspector.getAuthUser(null) == null
        SocketInspector.getAuthUser(socket) == null

        when:
        SocketInspector.clearUserData(socket)
        SocketInspector.setResp3(socket, true)
        SocketInspector.setResp3(socket, true)
        then:
        SocketInspector.isResp3(socket)

        when:
        SocketInspector.clearUserData(socket)
        SocketInspector.setResp3(socket, false)
        SocketInspector.setResp3(socket, false)
        then:
        !SocketInspector.isResp3(socket)

        when:
        SocketInspector.clearUserData(socket)
        SocketInspector.setConnectionReadonly(socket, true)
        SocketInspector.setConnectionReadonly(socket, true)
        then:
        SocketInspector.isConnectionReadonly(socket)

        when:
        SocketInspector.clearUserData(socket)
        SocketInspector.setConnectionReadonly(socket, false)
        SocketInspector.setConnectionReadonly(socket, false)
        then:
        !SocketInspector.isConnectionReadonly(socket)

        when:
        SocketInspector.clearUserData(socket)
        SocketInspector.setAuthUser(socket, 'default')
        SocketInspector.setAuthUser(socket, 'default')
        then:
        SocketInspector.getAuthUser(socket) == 'default'
    }

    def 'test connect'() {
        given:
        def inspector = new SocketInspector()
        def socket = mockTcpSocket()

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]
        inspector.connectedClientCountArray = [0]
        inspector.onConnect(socket)
        inspector.onDisconnect(socket)
        inspector.subscribe('test_channel', false, socket)
        inspector.onDisconnect(socket)
        inspector.onReadTimeout(socket)
        inspector.onRead(socket, null)
        inspector.onReadEndOfStream(socket)
        inspector.onReadError(socket, null)
        inspector.onWriteTimeout(socket)
        inspector.onWrite(socket, null, 10)
        inspector.onWriteError(socket, null)
        then:
        inspector.lookup(SocketInspector.class) == null

        when:
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed = true
        socket.userData = new VeloUserDataInSocket(ReplPairTest.mockAsSlave())
        inspector.onConnect(socket)
        inspector.onDisconnect(socket)
        then:
        1 == 1

        when:
        socket.userData = null
        inspector.maxConnections = 1
        println inspector.maxConnections
        boolean exception = false
        try {
            inspector.onConnect(socket)
            inspector.onConnect(socket)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        inspector.isServerStopped = true
        try {
            inspector.onConnect(socket)
        } catch (Exception e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        inspector.clearAll()
    }

    def 'test subscribe'() {
        given:
        def inspector = new SocketInspector()
        def socket = mockTcpSocket()

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def eventloop2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        eventloop2.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        Thread.start {
            eventloop2.run()
        }
        Thread.sleep(1000)

        Eventloop[] eventloopArray = [eventloop2, eventloop]
        inspector.initByNetWorkerEventloopArray(eventloopArray)

        def one = new SocketInspector.ChannelAndIsPattern('test_channel', false)
        def one1 = new SocketInspector.ChannelAndIsPattern('test_channel', false)
        def two = new SocketInspector.ChannelAndIsPattern('test_*', true)
        def two2 = new SocketInspector.ChannelAndIsPattern('test_*', true)
        def three = new Object()

        expect:
        one.equals(one)
        one.equals(one1)
        !one.equals(two)
        !one.equals(three)
        one.hashCode() == one1.hashCode()
        two.hashCode() == two2.hashCode()
        two.equals(two2)
        !two.equals(one)
        one.isMatch('test_channel')
        !one.isMatch('test_channel2')
        two.isMatch('test_channel')
        two.isMatch('test_channel2')
        inspector.connectedClientCount() == 0
        inspector.allSubscribeOnes().isEmpty()
        inspector.subscribeClientCount() == 0

        when:
        def channel = 'test_channel'
        def channel2 = 'test_channel2'
        def messageReply = new BulkReply('test_message'.bytes)
        def n = inspector.publish(channel, messageReply, (s, r) -> { })
        then:
        n == 0
        inspector.subscribeSocketCount(channel, false) == 0

        when:
        n = inspector.subscribe(channel, false, socket)
        then:
        n == 1
        inspector.subscribeClientCount() == 1
        inspector.subscribeSocketCount(channel, false) == 1
        inspector.allSubscribeOnes().size() == 1
        inspector.filterSubscribeOnesByChannel(channel, false).getFirst().channel == channel
        inspector.filterSubscribeOnesByChannel('xxx', false) == []

        when:
        // not subscribe yet
        def n2 = inspector.unsubscribe(channel2, false, socket)
        then:
        n2 == 0

        when:
        // in eventloop thread
        SupplierEx<Integer> supplierEx = () -> inspector.subscribe(channel2, false, socket)
        eventloop.submit(AsyncComputation.of(supplierEx)).get()
        then:
        inspector.subscribeSocketCount(channel2, false) == 1

        when:
        n = inspector.unsubscribe(channel, false, socket)
        then:
        n == 0

        when:
        n = inspector.publish(channel, messageReply, (s, r) -> { })
        n2 = inspector.publish(channel2, messageReply, (s, r) -> {
            println 'async callback to write message to target socket'
        })
        then:
        n == 0
        n2 == 1

        when:
        inspector.subscribe(channel, false, socket)
        n = inspector.publish(channel, messageReply, (s, r) -> { })
        then:
        n == 1

        cleanup:
        Thread.sleep(1000)
        eventloop.breakEventloop()
        eventloop2.breakEventloop()
        inspector.clearAll()
    }
}
