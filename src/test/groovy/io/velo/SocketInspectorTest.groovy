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
        inspector.subscribe('test_channel', socket)
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

        when:
        def channel = 'test_channel'
        def channel2 = 'test_channel2'
        def messageReply = new BulkReply('test_message'.bytes)
        def n = inspector.publish(channel, messageReply, (s, r) -> { })
        then:
        n == 0
        inspector.subscribeSocketCount(channel) == 0

        when:
        n = inspector.subscribe(channel, socket)
        then:
        n == 1
        inspector.subscribeSocketCount(channel) == 1
        inspector.filterSubscribeChannels(null) == [channel]
        inspector.filterSubscribeChannels('xxx') == []

        when:
        // in eventloop thread
        SupplierEx<Integer> supplierEx = () -> inspector.subscribe(channel2, socket)
        eventloop.submit(AsyncComputation.of(supplierEx)).get()
        then:
        inspector.subscribeSocketCount(channel2) == 1

        when:
        n = inspector.unsubscribe(channel, socket)
        then:
        n == 0

        when:
        n = inspector.publish(channel, messageReply, (s, r) -> { })
        def n2 = inspector.publish(channel2, messageReply, (s, r) -> {
            println 'async callback to write message to target socket'
        })
        then:
        n == 0
        n2 == 1

        when:
        inspector.subscribe(channel, socket)
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
