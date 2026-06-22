package io.velo

import io.activej.async.callback.AsyncComputation
import io.activej.bytebuf.ByteBuf
import io.activej.common.function.SupplierEx
import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import io.velo.command.BlockingList
import io.velo.command.XGroup
import io.velo.repl.ReplPairTest
import io.velo.reply.BulkReply
import spock.lang.Specification

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class SocketInspectorTest extends Specification {
    static TcpSocket mockTcpSocket(Eventloop eventloop = null, int port = 46379) {
        def socket = TcpSocket.wrapChannel(eventloop, SocketChannel.open(),
                new InetSocketAddress('localhost', port), null)
        SocketInspector.createUserDataIfNotSet(socket)
        return socket
    }

    def 'test base'() {
        given:
        def socket = mockTcpSocket()
        SocketInspector.createUserDataIfNotSet(socket)
        SocketInspector.updateLastSendCommand(socket, "", 1)
        SocketInspector.updateLastSetSeq(socket, 1L, (short) 0)

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

        when:
        def veloUserData = SocketInspector.createUserDataIfNotSet(socket)
        veloUserData.clientName = 'test_client'
        def clientInfo = SocketInspector.getClientInfo(socket)
        then:
        clientInfo != null

        // ----- new isSubscribed(): null-safe and reports false on an empty inspector -----
        def inspector = new SocketInspector()
        !inspector.isSubscribed(socket)
        !inspector.isSubscribed(null)
    }

    def 'test isSubscribed'() {
        given:
        def inspector = new SocketInspector()
        def subscribedSocket = mockTcpSocket()
        def otherSocket = mockTcpSocket()

        // subscribe() needs at least one net-worker eventloop to register the socket,
        // otherwise it short-circuits and never adds the socket to the subscription map.
        def netWorkerEventloop = Eventloop.builder()
                .withThreadName('is-subscribed-test')
                .withIdleInterval(Duration.ofMillis(50))
                .build()
        netWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.sleep(200)
        inspector.initByNetWorkerEventloopArray([netWorkerEventloop] as Eventloop[], [netWorkerEventloop] as Eventloop[])

        expect: 'empty inspector has no subscribers'
        !inspector.isSubscribed(subscribedSocket)
        !inspector.isSubscribed(otherSocket)
        !inspector.isSubscribed(null)

        when: 'a socket is subscribed'
        inspector.subscribe('test_chan', false, subscribedSocket)
        then:
        inspector.isSubscribed(subscribedSocket)
        // a different socket is still not subscribed
        !inspector.isSubscribed(otherSocket)

        when: 'the socket is unsubscribed'
        inspector.unsubscribe('test_chan', false, subscribedSocket)
        then:
        !inspector.isSubscribed(subscribedSocket)

        cleanup:
        netWorkerEventloop.breakEventloop()
    }

    def 'test formatRedisAddress'() {
        expect: 'null is preserved'
        SocketInspector.formatRedisAddress(null) == null

        when: 'IPv4 address'
        def v4 = new InetSocketAddress('127.0.0.1', 46379)
        then: 'rendered as ip:port with no host/ prefix'
        SocketInspector.formatRedisAddress(v4) == '127.0.0.1:46379'

        when: 'IPv4 hostname that resolves to 127.0.0.1'
        def v4ByName = new InetSocketAddress('localhost', 46379)
        then: 'still rendered as the IP, not host/ip:port'
        SocketInspector.formatRedisAddress(v4ByName) == '127.0.0.1:46379'

        when: 'IPv6 loopback'
        def v6 = new InetSocketAddress(InetAddress.getByName('::1'), 46379)
        then: 'rendered as [ip]:port with brackets'
        SocketInspector.formatRedisAddress(v6) == '[0:0:0:0:0:0:0:1]:46379'

        when: 'port 0 is preserved'
        def anyPort = new InetSocketAddress('127.0.0.1', 0)
        then:
        SocketInspector.formatRedisAddress(anyPort) == '127.0.0.1:0'
    }

    def 'test connect'() {
        given:
        def inspector = new SocketInspector()
        def socket = mockTcpSocket()

        and:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)

        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]
        inspector.connectedClientCountArray = [0]
        inspector.onConnect(socket)
        inspector.onDisconnect(socket)
        inspector.subscribe('test_channel', false, socket)
        inspector.onDisconnect(socket)
        inspector.onReadTimeout(socket)
        inspector.onRead(socket, ByteBuf.empty())
        inspector.onReadEndOfStream(socket)
        inspector.onReadError(socket, null)
        inspector.onWriteTimeout(socket)
        inspector.onWrite(socket, null, 10)
        inspector.onWriteError(socket, null)
        then:
        inspector.lookup(SocketInspector.class) == null
        inspector.netInBytesLength() == 0
        inspector.netOutBytesLength() == 10

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

    def 'test publish dispatches to net worker eventloop not slot worker'() {
        given:
        def inspector = new SocketInspector()

        // Simulate distinct net-worker and slot-worker eventloops
        def netWorkerEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def slotWorkerEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netWorkerEventloop.keepAlive(true)
        slotWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.start { slotWorkerEventloop.run() }
        Thread.sleep(500)

        // Init with separate arrays: slot workers and net workers are distinct
        Eventloop[] slotWorkerEventloopArray = [slotWorkerEventloop]
        Eventloop[] netWorkerEventloopArray = [netWorkerEventloop]
        inspector.initByNetWorkerEventloopArray(slotWorkerEventloopArray, netWorkerEventloopArray)

        // Create a socket that is "owned" by the net worker eventloop
        // onConnect sets the netWorkerEventloop in VeloUserDataInSocket
        def socket = mockTcpSocket(netWorkerEventloop)
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [netWorkerEventloop.getEventloopThread().threadId()]
        inspector.connectedClientCountArray = [0]
        // onConnect must run on the net worker eventloop thread (as in production)
        def connectDone = new java.util.concurrent.CountDownLatch(1)
        netWorkerEventloop.execute {
            inspector.onConnect(socket)
            connectDone.countDown()
        }
        connectDone.await(2, java.util.concurrent.TimeUnit.SECONDS)

        // Subscribe is called from the slot worker (simulated by submitting to slot worker eventloop)
        def channel = 'test_channel'
        def subscribeDone = new java.util.concurrent.CountDownLatch(1)
        slotWorkerEventloop.execute {
            inspector.subscribe(channel, false, socket)
            subscribeDone.countDown()
        }
        subscribeDone.await(2, java.util.concurrent.TimeUnit.SECONDS)

        expect:
        inspector.subscribeClientCount() == 1

        when:
        // Publish from the slot worker eventloop
        // The callback should be dispatched to the NET worker eventloop, NOT the slot worker
        def callbackThreadRef = new AtomicReference<Thread>()
        def publishDone = new java.util.concurrent.CountDownLatch(1)
        slotWorkerEventloop.execute {
            inspector.publish(channel, new BulkReply('test_message'), (s, r) -> {
                callbackThreadRef.set(Thread.currentThread())
                publishDone.countDown()
            })
        }
        publishDone.await(2, java.util.concurrent.TimeUnit.SECONDS)

        then:
        // The callback must have run on the net worker thread, not the slot worker thread
        callbackThreadRef.get() != null
        callbackThreadRef.get().threadId() == netWorkerEventloop.getEventloopThread().threadId()
        callbackThreadRef.get().threadId() != slotWorkerEventloop.getEventloopThread().threadId()

        cleanup:
        netWorkerEventloop.breakEventloop()
        slotWorkerEventloop.breakEventloop()
        inspector.clearAll()
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = null
    }

    def 'test concurrent subscribe and publish do not throw ConcurrentModificationException'() {
        given:
        def inspector = new SocketInspector()

        def netWorkerEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def slotWorkerEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netWorkerEventloop.keepAlive(true)
        slotWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.start { slotWorkerEventloop.run() }
        Thread.sleep(500)

        Eventloop[] slotWorkerEventloopArray = [slotWorkerEventloop]
        Eventloop[] netWorkerEventloopArray = [netWorkerEventloop]
        inspector.initByNetWorkerEventloopArray(slotWorkerEventloopArray, netWorkerEventloopArray)

        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [netWorkerEventloop.getEventloopThread().threadId()]
        inspector.connectedClientCountArray = [0]

        def channel = 'concurrent_test'
        def sockets = []
        for (int i = 0; i < 20; i++) {
            def socket = mockTcpSocket(netWorkerEventloop)
            sockets << socket
            def connectDone = new java.util.concurrent.CountDownLatch(1)
            netWorkerEventloop.execute {
                inspector.onConnect(socket)
                connectDone.countDown()
            }
            connectDone.await(2, java.util.concurrent.TimeUnit.SECONDS)
        }

        when:
        def errors = Collections.synchronizedList(new ArrayList<Throwable>())
        def done = new java.util.concurrent.CountDownLatch(4)

        // Thread 1: subscribe sockets
        Thread.start {
            try {
                for (int i = 0; i < sockets.size(); i++) {
                    inspector.subscribe(channel, false, sockets[i])
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Thread 2: unsubscribe some sockets
        Thread.start {
            try {
                Thread.sleep(50)
                for (int i = 0; i < sockets.size() / 2; i++) {
                    inspector.unsubscribe(channel, false, sockets[i])
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Thread 3-4: publish repeatedly
        2.times {
            Thread.start {
                try {
                    100.times {
                        inspector.publish(channel, new BulkReply('msg'), (s, r) -> { })
                    }
                } catch (Throwable t) {
                    errors << t
                } finally {
                    done.countDown()
                }
            }
        }

        done.await(5, java.util.concurrent.TimeUnit.SECONDS)

        then:
        errors.isEmpty()

        cleanup:
        netWorkerEventloop.breakEventloop()
        slotWorkerEventloop.breakEventloop()
        inspector.clearAll()
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = null
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
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)

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
        inspector.setMaxConnections(100)
        then:
        inspector.maxConnections == 100

        when:
        inspector.setMaxConnections(0)
        then:
        thrown(IllegalArgumentException)

        when:
        inspector.setMaxConnections(-1)
        then:
        thrown(IllegalArgumentException)

        when:
        def channel = 'test_channel'
        def channel2 = 'test_channel2'
        def messageReply = new BulkReply('test_message')
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

    def 'test max connections atomic admission'() {
        given:
        def inspector = new SocketInspector()
        inspector.setMaxConnections(3)

        and:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]
        inspector.connectedClientCountArray = [0]

        expect:
        inspector.maxConnections == 3

        when:
        def s1 = mockTcpSocket(eventloopCurrent, 46380)
        def s2 = mockTcpSocket(eventloopCurrent, 46381)
        def s3 = mockTcpSocket(eventloopCurrent, 46382)
        inspector.onConnect(s1)
        inspector.onConnect(s2)
        inspector.onConnect(s3)

        then:
        inspector.socketMap.size() == 3
        inspector.connectionCount.get() == 3

        when:
        def s4 = mockTcpSocket(eventloopCurrent, 46383)
        inspector.onConnect(s4)

        then:
        inspector.socketMap.size() == 3
        inspector.connectionCount.get() == 3

        when:
        inspector.onDisconnect(s2)

        then:
        inspector.socketMap.size() == 2
        inspector.connectionCount.get() == 2

        when:
        def s5 = mockTcpSocket(eventloopCurrent, 46384)
        inspector.onConnect(s5)

        then:
        inspector.socketMap.size() == 3
        inspector.connectionCount.get() == 3

        cleanup:
        inspector.clearAll()
    }

    def 'test setMaxConnections is visible across threads'() {
        given:
        def inspector = new SocketInspector()
        inspector.setMaxConnections(100)
        def seen = new AtomicReference<Integer>()
        def latch = new CountDownLatch(1)

        when:
        Thread.start {
            inspector.setMaxConnections(42)
            latch.countDown()
        }
        latch.await(5, TimeUnit.SECONDS)

        then:
        inspector.maxConnections == 42

        cleanup:
        inspector.clearAll()
    }

    def 'test clearAll resets connectionCount'() {
        given:
        def inspector = new SocketInspector()
        inspector.setMaxConnections(5)

        and:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]
        inspector.connectedClientCountArray = [0]

        when:
        def s1 = mockTcpSocket(eventloopCurrent, 46390)
        def s2 = mockTcpSocket(eventloopCurrent, 46391)
        inspector.onConnect(s1)
        inspector.onConnect(s2)

        then:
        inspector.connectionCount.get() == 2
        inspector.socketMap.size() == 2

        when:
        inspector.clearAll()

        then:
        inspector.connectionCount.get() == 0
        inspector.socketMap.size() == 0

        when:
        def s3 = mockTcpSocket(eventloopCurrent, 46392)
        inspector.onConnect(s3)

        then:
        inspector.connectionCount.get() == 1
        inspector.socketMap.size() == 1

        cleanup:
        inspector.clearAll()
    }
}
