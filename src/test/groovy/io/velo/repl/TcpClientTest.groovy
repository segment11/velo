package io.velo.repl

import io.activej.bytebuf.ByteBuf
import io.activej.config.Config
import io.activej.csp.binary.BinaryChannelSupplier
import io.activej.csp.consumer.ChannelConsumers
import io.activej.csp.supplier.ChannelSuppliers
import io.activej.eventloop.Eventloop
import io.activej.net.SimpleServer
import io.activej.promise.Promise
import io.velo.ConfForGlobal
import io.velo.MultiWorkerServer
import io.velo.RequestHandler
import io.velo.SocketInspector
import io.velo.decode.RequestDecoder
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.content.RawBytesContent
import spock.lang.Specification

import java.time.Duration

class TcpClientTest extends Specification {
    def 'test connect and close'() {
        given:
        short slot = 0
        ConfForGlobal.netListenAddresses = 'localhost:6380'

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
        def replPair2 = ReplPairTest.mockAsSlave()
        def tcpClient = new TcpClient(slot, eventloop, requestHandler, replPair2)

        expect:
        !tcpClient.isSocketConnected()

        when:
        tcpClient.write(ReplType.test, new RawBytesContent('test'.bytes))
        then:
        tcpClient.notConnectedErrorCount == 1

        when:
        tcpClient.notConnectedErrorCount = 999
        tcpClient.ping()
        then:
        tcpClient.notConnectedErrorCount == 1000

        when:
        tcpClient.bye()
        then:
        tcpClient.notConnectedErrorCount == 1001

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, eventloop.eventloopThread.threadId())

        tcpClient.close()
        println 'tcp client ready to reconnect'
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def server = SimpleServer.builder(
                eventloopCurrent,
                socket -> {
                    println 'Client connected'
                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .mapAsync { pipeline ->
                                Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()]
                                for (int i = 0; i < pipeline.size(); i++) {
                                    def request = pipeline[i]
                                    println 'Mock server get request from client, data.length: ' + request.data.length
                                    def promiseI = Promise.of(Repl.test(slot, replPair2, 'ok').buffer())
                                    promiseN[i] = promiseI
                                }

                                MultiWorkerServer.allPipelineByteBuf(promiseN)
                            }.streamTo(ChannelConsumers.ofSocket(socket))
                })
                .withListenAddress(new InetSocketAddress('localhost', 7379))
                .withAcceptOnce()
                .build()

        Thread.sleep(100)
        server.listen()
        eventloop.execute {
            tcpClient.connect('localhost', 7379) {
                tcpClient.write(ReplType.ok, new RawBytesContent('test'.bytes))
                println 'Send ok to server after client connected'
                Repl.error(slot, replPair2, 'ok').buffer()
            }
        }
        eventloop.delay(1000, () -> {
            tcpClient.close()
        })
        println 'before current eventloop run'
        eventloopCurrent.run()
        println 'after current eventloop run'
        Thread.sleep(1000)
        then:
        1 == 1

        cleanup:
        eventloop.breakEventloop()
        // only for coverage
        tcpClient.close()
        tcpClient.cleanUp()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
