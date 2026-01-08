package io.velo.repl

import io.activej.config.Config
import io.activej.csp.binary.BinaryChannelSupplier
import io.activej.csp.consumer.ChannelConsumers
import io.activej.csp.supplier.ChannelSuppliers
import io.activej.eventloop.Eventloop
import io.activej.net.SimpleServer
import io.velo.*
import io.velo.decode.RequestDecoder
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.content.RawBytesContent
import spock.lang.Specification

import java.time.Duration

class ReplPairTest extends Specification {
    static ReplPair mockOne(short slot = 0, boolean asMaster = true, String host = 'localhost', int port = 6379) {
        def replPair = new ReplPair(slot, asMaster, host, port)
        replPair
    }

    static ReplPair mockAsMaster(long masterUuid = 0L) {
        def replPair = mockOne()
        replPair.masterUuid = masterUuid
        replPair
    }

    static ReplPair mockAsSlave(long masterUuid = 0L, long slaveUuid = 1L) {
        def replPair = mockOne((byte) 0, false, 'localhost', 6380)
        replPair.masterUuid = masterUuid
        replPair.slaveUuid = slaveUuid
        replPair
    }

    final short slot = 0

    def 'test base'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6380'

        def replPairAsMaster = mockAsMaster()
        def replPairAsSlave = mockAsSlave()
        println replPairAsMaster
        println replPairAsSlave
        replPairAsSlave.remoteReplProperties = ConfForSlot.global.generateReplProperties()
        ReplPair.parseHostAndPort(null)
        println ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)
        for (replType in ReplType.values()) {
            replPairAsMaster.increaseStatsCountForReplType(replType)
            replPairAsSlave.increaseStatsCountForReplType(replType)
        }

        replPairAsSlave.increaseStatsCountWhenSlaveSkipFetch(ReplType.error)
        replPairAsSlave.increaseStatsCountWhenSlaveSkipFetch(ReplType.s_exists_wal)
        replPairAsSlave.increaseStatsCountWhenSlaveSkipFetch(ReplType.s_exists_chunk_segments)
        replPairAsSlave.increaseStatsCountWhenSlaveSkipFetch(ReplType.s_exists_key_buckets)
        println replPairAsSlave.statsCountForSlaveSkipFetchAsString

        // trigger log
        100.times {
            replPairAsMaster.increaseStatsCountForReplType(ReplType.catch_up)
            replPairAsSlave.increaseStatsCountForReplType(ReplType.s_catch_up)
        }

        println replPairAsMaster.statsCountForReplTypeAsString
        println replPairAsSlave.statsCountForReplTypeAsString

        replPairAsSlave.slaveCatchUpLastSeq = 1000L
        replPairAsSlave.slaveCatchUpLastTimeMillis = System.currentTimeMillis() - 1000L
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset = null
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset = null
        replPairAsSlave.increaseFetchedBytesLength(1000)
        replPairAsSlave.masterReadonly = false
        replPairAsSlave.allCaughtUp = false
        replPairAsSlave.disconnectTimeMillis = System.currentTimeMillis()
        replPairAsSlave.putToDelayListToRemoveTimeMillis = System.currentTimeMillis()

        expect:
        replPairAsMaster.slot == slot
        replPairAsMaster.host == 'localhost'
        replPairAsMaster.port == 6379
        replPairAsMaster.hostAndPort == 'localhost:6379'
        replPairAsMaster.asMaster
        replPairAsMaster.masterUuid == 0L
        replPairAsMaster.lastPingGetTimestamp == 0L

        replPairAsSlave.slaveCatchUpLastSeq == 1000L
        replPairAsSlave.slaveCatchUpLastTimeMillisInMaster > 0L
        replPairAsSlave.slaveCatchUpLastTimeMillisDiff >= 1000L
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset == null
        replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset == 0L
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset == null
        replPairAsSlave.fetchedBytesLengthTotal == 1000L
        !replPairAsSlave.masterReadonly
        !replPairAsSlave.allCaughtUp
        replPairAsSlave.disconnectTimeMillis > 0
        replPairAsSlave.putToDelayListToRemoveTimeMillis > 0

        !replPairAsMaster.sendBye
        !replPairAsMaster.ping()
        !replPairAsMaster.write(ReplType.ping, null)
        !replPairAsMaster.bye()
        !replPairAsMaster.write(ReplType.ping, null)
        !replPairAsMaster.toFetchBigStringIdList
        !replPairAsMaster.doFetchingBigStringIdList
        replPairAsMaster.doingFetchBigStringId() == null

        replPairAsSlave.slot == slot
        replPairAsSlave.hostAndPort == 'localhost:6380'
        !replPairAsSlave.asMaster
        replPairAsSlave.masterUuid == 0L
        replPairAsSlave.slaveUuid == 1L
        replPairAsSlave.remoteReplProperties != null
        replPairAsSlave.lastPongGetTimestamp == 0L

        def replPairMaster1 = mockOne(slot, true, 'localhost', 16379)
        def replPairAsMaster11 = mockOne(slot, true, 'local', 6379)

        !replPairAsMaster.equals(null)
        !replPairAsMaster.equals(Integer.valueOf(0))
        replPairAsMaster != replPairAsSlave
        replPairAsMaster != replPairMaster1
        replPairAsMaster != replPairAsMaster11
        replPairAsMaster.equals(replPairAsMaster)
        !replPairAsMaster.linkUp

        when:
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 0)
        then:
        replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset == 0L

        when:
        replPairAsMaster.isSendBye = true
        replPairAsMaster.sendBye = true
        then:
        !replPairAsMaster.ping()
        !replPairAsMaster.write(ReplType.ping, null)

        when:
        replPairAsMaster.lastPingGetTimestamp = System.currentTimeMillis() - 1000L
        replPairAsSlave.lastPongGetTimestamp = System.currentTimeMillis() - 2000L
        then:
        replPairAsMaster.linkUp
        !replPairAsSlave.linkUp

        when:
        replPairAsMaster.slaveConnectSocketInMaster = null
        replPairAsMaster.closeSlaveConnectSocket()
        then:
        1 == 1

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        replPairAsMaster.closeSlaveConnectSocket()
        replPairAsMaster.slaveConnectSocketInMaster = socket
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopCurrent.execute {
            replPairAsMaster.closeSlaveConnectSocket()
        }
        eventloopCurrent.run()
        Thread.sleep(1000)
        then:
        replPairAsMaster.slaveConnectSocketInMaster == null

        when:
        replPairAsMaster.addToFetchBigStringId(1L, 0, 1L, 'a')
        then:
        replPairAsMaster.doingFetchBigStringId().uuid() == 1L
        replPairAsMaster.doFetchingBigStringIdList[0].uuid() == 1L

        when:
        replPairAsMaster.doneFetchBigStringUuid(100L)
        replPairAsMaster.doneFetchBigStringUuid(1L)
        then:
        replPairAsMaster.doFetchingBigStringIdList.size() == 0

        when:
        replPairAsSlave.masterCanNotConnect = false
        def millis = System.currentTimeMillis() - 1000L
        replPairAsSlave.lastGetCatchUpResponseMillis = millis
        then:
        replPairAsSlave.lastGetCatchUpResponseMillis == millis
        !replPairAsSlave.masterCanNotConnect

        when:
        replPairAsSlave.initAsSlave(null, null)
        then:
        !replPairAsSlave.isLinkUp()
    }

    def 'test connect'() {
        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance

        def replPairAsMaster = mockAsMaster()
        def replPairAsSlave = mockAsSlave()

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
                            .map { pipeline ->
                                def request = pipeline[0]
                                def data = request.getData()
                                println 'Mock server get request from client, data.length: ' + data.length
                                Repl.test(slot, replPairAsSlave, 'ok').buffer()
                            }.streamTo(ChannelConsumers.ofSocket(socket))
                })
                .withListenAddress(new InetSocketAddress('localhost', 6380))
                .withAcceptOnce()
                .build()
        Thread.sleep(1000)
        server.listen()
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
        replPairAsSlave.initAsSlave(eventloopCurrent, requestHandler)
        boolean[] isLinkUpArray = [false]
        replPairAsSlave.lastPongGetTimestamp = System.currentTimeMillis()
        replPairAsSlave.sendBye = false
        println 'before current eventloop run'
        eventloopCurrent.delay(2000, () -> {
            isLinkUpArray[0] = replPairAsSlave.isLinkUp()
            replPairAsSlave.initAsSlave(eventloopCurrent, requestHandler)

            replPairAsSlave.ping()
            replPairAsSlave.write(ReplType.test, new RawBytesContent('test'.bytes))
            replPairAsSlave.bye()
            Thread.sleep(100)
            replPairAsSlave.close()
        })
        eventloopCurrent.run()
        println 'after current eventloop run'
        then:
        isLinkUpArray[0]

        cleanup:
        replPairAsMaster.close()
        replPairAsSlave.close()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
