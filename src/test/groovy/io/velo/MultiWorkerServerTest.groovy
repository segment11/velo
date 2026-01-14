package io.velo

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.activej.inject.binding.OptionalDependency
import io.velo.acl.U
import io.velo.decode.Request
import io.velo.persist.Consts
import io.velo.persist.KeyBucket
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.LeaderSelector
import io.velo.repl.ReplType
import io.velo.repl.cluster.MultiShard
import io.velo.repl.cluster.MultiSlotRange
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.NilReply
import io.velo.task.TaskRunnable
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class MultiWorkerServerTest extends Specification {
    final short slot0 = 0
    final short slot1 = 1
    final byte workerId0 = 0
    final byte workerId1 = 1
    final short slotNumber = 2
    final byte slotWorkers = 2
    final byte netWorkers = 2

    def 'test mock inject and handle'() {
        // only for coverage
        given:
        def dirFile = new File('/tmp/velo-data')
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', slotNumber.toString())
                .with('slotWorkers', slotWorkers.toString())
                .with('netWorkers', netWorkers.toString())
                .with("net.listenAddresses", "localhost:7379")

        dirFile.deleteDir()

        def m = new MultiWorkerServer()
        m.slotWorkerEventloopArray = new Eventloop[2]
        m.netWorkerEventloopArray = new Eventloop[2]
        m.requestHandlerArray = new RequestHandler[2]
        m.scheduleRunnableArray = new TaskRunnable[2]
        m.refreshLoader = m.refreshLoader()
        println MultiWorkerServer.UP_TIME

        def dirFile2 = m.dirFile(config, true)
        def dirFile3 = m.dirFile(config, false)

        def snowFlakes = m.snowFlakes(ConfForSlot.global, config)

        expect:
        dirFile2.exists()
        dirFile3.exists()
        m.STATIC_GLOBAL_V.socketInspector == null
        m.primaryReactor(config) != null
        m.workerPool(null, config) == null
        m.workerReactor(workerId0, OptionalDependency.empty(), config) != null
        m.config() != null
        snowFlakes != null
        m.getModule() != null
        m.getBusinessLogicModule() != null

        when:
        boolean hasException = false
        try {
            m.dirFile(config, true)
        } catch (Exception e) {
            println e.message
            hasException = true
        }
        then:
        hasException

        when:
        MultiWorkerServer.MAIN_ARGS = ['/etc/velo.properties']
        m.config()
        then:
        1 == 1

        when:
        MultiWorkerServer.MAIN_ARGS = []
        m.config()
        then:
        1 == 1

        when:
        def givenConfigFile = new File(Utils.projectPath("/" + MultiWorkerServer.PROPERTIES_FILE))
        if (!givenConfigFile.exists()) {
            givenConfigFile.createNewFile()
        }
        m.config()
        then:
        1 == 1

        when:
        def httpReply = m.wrapHttpResponse(new BulkReply('xxx'))
        def httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('200')

        when:
        httpReply = m.wrapHttpResponse(ErrorReply.FORMAT)
        httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('500')

        when:
        httpReply = m.wrapHttpResponse(ErrorReply.NO_AUTH)
        httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('401')

        when:
        httpReply = m.wrapHttpResponse(NilReply.INSTANCE)
        httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('404')

        when:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist(netWorkers, slotNumber)
        localPersist.fixSlotThreadId(slot0, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())
        def snowFlake = new SnowFlake(1, 1)
        m.requestHandlerArray[0] = new RequestHandler(workerId0, netWorkers, slotNumber, snowFlake, config)
        m.requestHandlerArray[1] = new RequestHandler(workerId1, netWorkers, slotNumber, snowFlake, config)
        m.scheduleRunnableArray[0] = new TaskRunnable(workerId0, netWorkers)
        m.scheduleRunnableArray[1] = new TaskRunnable(workerId1, netWorkers)
        then:
        1 == 1

        when:
        m.configInject = config
        def eventloop0 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop0.keepAlive(true)
        def eventloop1 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop1.keepAlive(true)
        Thread.start {
            eventloop0.run()
        }
        Thread.start {
            eventloop1.run()
        }
        Thread.sleep(100)
        m.slotWorkerEventloopArray[0] = eventloop0
        m.slotWorkerEventloopArray[1] = eventloop1
        m.netWorkerEventloopArray[0] = eventloop0
        m.netWorkerEventloopArray[1] = eventloop1
        m.socketInspector = new SocketInspector()
        m.onStart()
        m.primaryScheduleRunnable.run()
        then:
        1 == 1

        when:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def socket = SocketInspectorTest.mockTcpSocket(eventloop0)
        // handle request
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = 'key'.bytes
        def getRequest = new Request(getData2, false, false)
        getRequest.slotNumber = slotNumber
        m.requestHandlerArray[0].parseSlots(getRequest)
        def p = m.handleRequest(getRequest, socket)
        eventloopCurrent.run()
        then:
        p.whenResult { reply ->
            reply != null
        }.result

        when:
        boolean exception = false
        try {
            m.consumer(config).accept(socket)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def data3 = new byte[3][]
        data3[0] = 'copy'.bytes
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        // for async reply
        def copyRequest = new Request(data3, false, false)
        copyRequest.slotNumber = slotNumber
        m.requestHandlerArray[0].parseSlots(copyRequest)
        p = m.handleRequest(copyRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        SocketInspector.setConnectionReadonly(socket, true)
        p = m.handleRequest(copyRequest, socket)
        eventloopCurrent.run()
        then:
        p.whenResult { reply ->
            reply.toString().contains('readonly')
        }.result

        when:
        copyRequest.u = new U('test')
        copyRequest.u.on = false
        p = m.handleRequest(copyRequest, socket)
        eventloopCurrent.run()
        then:
        p.whenResult { reply ->
            reply.toString().contains('permit limit')
        }.result

        when:
        def mgetData6 = new byte[6][]
        mgetData6[0] = 'mget'.bytes
        mgetData6[1] = 'key1'.bytes
        mgetData6[2] = 'key2'.bytes
        mgetData6[3] = 'key3'.bytes
        mgetData6[4] = 'key4'.bytes
        mgetData6[5] = 'key5'.bytes
        def mgetRequest = new Request(mgetData6, true, false)
        mgetRequest.slotNumber = slotNumber
        m.requestHandlerArray[0].parseSlots(mgetRequest)
        p = m.handleRequest(mgetRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        // ping need not parse slots, any slot worker can handle it
        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)
        pingRequest.slotNumber = slotNumber
        m.requestHandlerArray[0].parseSlots(pingRequest)
        p = m.handleRequest(pingRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        pingRequest = new Request(pingData, true, false)
        p = m.handleRequest(pingRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def pingData2 = new byte[2][]
        pingData2[0] = 'ping'.bytes
        pingData2[1] = 'world'.bytes
        def pingRequest2 = new Request(pingData2, false, false)
        p = m.handleRequest(pingRequest2, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        Debug.instance.logCmd = true
        def echoData = new byte[1][]
        echoData[0] = 'echo'.bytes
        def echoRequest = new Request(echoData, false, false)
        p = m.handleRequest(echoRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def echoData2 = new byte[2][]
        echoData2[0] = 'echo'.bytes
        echoData2[1] = 'world'.bytes
        def echoRequest2 = new Request(echoData2, false, false)
        p = m.handleRequest(echoRequest2, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        // no slots and cross request worker
        def flushdbData = new byte[1][]
        flushdbData[0] = 'flushdb'.bytes
        def flushdbRequest = new Request(flushdbData, false, false)
        flushdbRequest.slotNumber = slotNumber
        m.requestHandlerArray[0].parseSlots(flushdbRequest)
        p = m.handleRequest(flushdbRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def flushdbRequest2 = new Request(flushdbData, true, false)
        p = m.handleRequest(flushdbRequest2, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def emptyPipeline = new ArrayList<Request>()
        p = m.handlePipeline(emptyPipeline, socket, slotNumber)
        then:
        p != null

        when:
        ArrayList<Request> pipeline = new ArrayList<>()
        pipeline << getRequest
        p = m.handlePipeline(pipeline, socket, slotNumber)
        eventloopCurrent.run()
        then:
        p != null

        when:
        pipeline << getRequest
        p = m.handlePipeline(pipeline, socket, slotNumber)
        eventloopCurrent.run()
        then:
        p != null

        when:
        pipeline.clear()
        def replData = new byte[4][]
        replData[0] = new byte[8]
        replData[1] = new byte[2]
        replData[2] = new byte[1]
        ByteBuffer.wrap(replData[0]).putLong(11L)
        replData[2][0] = ReplType.hello.code
        def replRequest = new Request(replData, false, true)
        pipeline << replRequest
        p = m.handlePipeline(pipeline, socket, slotNumber)
        eventloopCurrent.run()
        then:
        p != null

        when:
        m.onStop()
        then:
        m.requestHandlerArray[0].isStopped
        m.requestHandlerArray[1].isStopped

        when:
        def m1 = new MultiWorkerServer.InnerModule()
        def c = m1.confForSlot(config)
        then:
        1 == 1

        when:
        def b = m1.beforeCreateHandler(c, snowFlakes, config)
        then:
        1 == 1

        when:
        def cc = ConfForSlot.global
        def configX = Config.create()
                .with('doFileLock', "false")
                .with("net.listenAddresses", "localhost:7379")
                .with("debugMode", "true")
                .with("pureMemory", "true")
                .with("zookeeperConnectString", 'localhost:2181')
                .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                .with("bucket.lruPerFd.percent", "100")
                .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                .with("chunk.lruPerFd.maxSize", cc.confChunk.lruPerFd.maxSize.toString())
                .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
                .with("repl.binlogForReadCacheSegmentMaxCount", cc.confRepl.binlogForReadCacheSegmentMaxCount.toString())
                .with("bigString.lru.maxSize", cc.lruBigString.maxSize.toString())
                .with("kv.lru.maxSize", cc.lruKeyAndCompressedValueEncoded.maxSize.toString())
                .with("dynConfig", Config.create().with("a", "b"))
        m1.skipZookeeperConnectCheck = true
        m1.confForSlot(configX)
        m1.beforeCreateHandler(c, snowFlakes, configX)
        then:
        ConfForGlobal.initDynConfigItems.size() == 1
        1 == 1

        when:
        exception = false
        m1.skipZookeeperConnectCheck = false
        try {
            m1.confForSlot(configX)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        1 == 1

        when:
        m1.skipZookeeperConnectCheck = true
        exception = false
        def config2 = Config.create()
                .with("slotNumber", (LocalPersist.MAX_SLOT_NUMBER + 1).toString())

        def snowFlakes1 = m.snowFlakes(ConfForSlot.global, config2)
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config22 = Config.create()
                .with("slotNumber", "0")
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config22)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config3 = Config.create()
                .with("slotNumber", "3")
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config3)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ConfForGlobal.clusterEnabled = false
        exception = false
        def config4 = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", (MultiWorkerServer.MAX_NET_WORKERS + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config4)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def cpuNumber = Runtime.getRuntime().availableProcessors()
        def config5 = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", (cpuNumber + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config5)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config6 = Config.create()
                .with("slotNumber", "2")
                .with("netWorkers", '3')
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config6)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config7 = Config.create()
                .with("slotNumber", "4")
                .with("netWorkers", '3')
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config7)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config77 = Config.create()
                .with("slotNumber", "4")
                .with("slotWorkers", '3')
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config77)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config44 = Config.create()
                .with("indexWorkers", (MultiWorkerServer.MAX_INDEX_WORKERS + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config44)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config55 = Config.create()
                .with("indexWorkers", (cpuNumber + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config55)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config8 = Config.create()
                .with("bucket.bucketsPerSlot", (KeyBucket.MAX_BUCKETS_PER_SLOT + 1).toString())
        try {
            m1.confForSlot(config8)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config9 = Config.create()
                .with("bucket.bucketsPerSlot", '1023')
        try {
            m1.confForSlot(config9)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config10 = Config.create()
                .with("bucket.bucketsPerSlot", '1024')
                .with("chunk.fdPerChunk", (ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK + 1).toString())
        try {
            m1.confForSlot(config10)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config11 = Config.create()
                .with("chunk.fdPerChunk", '16')
                .with("wal.oneChargeBucketNumber", '4')
        try {
            m1.confForSlot(config11)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        m1.requestHandlerArray(snowFlakes, b, config)
        then:
        1 == 1

        when:
        m1.scheduleRunnableArray(b, config)
        then:
        1 == 1

        when:
        m1.socketInspector(config)
        then:
        1 == 1

        cleanup:
        givenConfigFile.delete()
        eventloop0.breakEventloop()
        eventloop1.breakEventloop()
    }

    // need zookeeper server
    def 'test do repl'() {
        given:
        ConfForGlobal.slotNumber = 1
        def leaderSelector = LeaderSelector.instance
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'
        ConfForGlobal.netListenAddresses = leaderSelector.masterAddressLocalMocked

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot0, Thread.currentThread().threadId())

        boolean doThisCase = Consts.checkConnectAvailable()

        when:
        if (doThisCase) {
            MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        }
        then:
        1 == 1

        when:
        leaderSelector.masterAddressLocalMocked = 'localhost:7380'
        if (doThisCase) {
            MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        }
        then:
        1 == 1

        when:
        ConfForGlobal.isAsSlaveOfSlave = true
        if (doThisCase) {
            MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        }
        then:
        1 == 1

        when:
        leaderSelector.masterAddressLocalMocked = null
        if (doThisCase) {
            MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        }
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test static global v'() {
        given:
        def staticGlobalV = MultiWorkerServer.STATIC_GLOBAL_V

        when:
        staticGlobalV.slotWorkerThreadIds = [-1L]
        staticGlobalV.netWorkerThreadIds = [-1L]
        then:
        staticGlobalV.slotThreadLocalIndexByCurrentThread == -1
        staticGlobalV.netThreadLocalIndexByCurrentThread == -1

        when:
        staticGlobalV.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        staticGlobalV.netWorkerThreadIds = [Thread.currentThread().threadId()]
        then:
        staticGlobalV.slotThreadLocalIndexByCurrentThread == 0
        staticGlobalV.netThreadLocalIndexByCurrentThread == 0

        when:
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        def config = Config.create()
        staticGlobalV.resetInfoServer(config)
        def infoServerList = staticGlobalV.infoServerList
        then:
        infoServerList.size() == 12
    }

    def 'test cluster slot cross shards'() {
        given:
        ConfForGlobal.clusterEnabled = true
        def eventloop0 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop0.keepAlive(true)
        Thread.start {
            eventloop0.run()
        }
        Thread.sleep(100)

        def m = new MultiWorkerServer()
        m.isMockHandle = true
        m.slotWorkerEventloopArray = new Eventloop[1]
        m.slotWorkerEventloopArray[0] = eventloop0

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        RequestHandler.initMultiShardShadows((byte) 1)
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId0, netWorkers, slotNumber, snowFlake, Config.create())
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = requestHandler
        m.slotWorkerThreadIds = MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds

        and:
        Consts.persistDir.mkdirs()
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        def multiShard = new MultiShard(Consts.persistDir)
        multiShard.mySelfShard().multiSlotRange.addSingle(0, 8191)
        def shard1 = new Shard()
        shard1.nodes << new Node(master: true, host: 'localhost', port: 7380)
        shard1.multiSlotRange = new MultiSlotRange(list: [])
        shard1.multiSlotRange.addSingle(8192, 16383)
        multiShard.shards << shard1

        when:
        RequestHandler.updateMultiShardShadows(multiShard)
        // handle request
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = 'key'.bytes
        def getRequest = new Request(getData2, false, false)
        getRequest.slotNumber = slotNumber
        requestHandler.parseSlots(getRequest)
        // MOVED
        m.handleRequest(getRequest, null)
        then:
        1 == 1

        when:
        def checkResult = m.checkClusterSlot(getRequest.slotWithKeyHashList)
        then:
        checkResult instanceof ErrorReply
        (checkResult as ErrorReply).message.startsWith('MOVED')

        when:
        ArrayList<BaseCommand.SlotWithKeyHash> ignoreSlotWithKeyHashList = [BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT]
        def checkResult2 = m.checkClusterSlot(ignoreSlotWithKeyHashList)
        then:
        checkResult2 == null

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        multiShard.shards[0].nodes[0].mySelf = false
        multiShard.shards[1].nodes[0].mySelf = true
        RequestHandler.updateMultiShardShadows(multiShard)
        m.handleRequest(getRequest, socket)
        then:
        1 == 1

        when:
        def getData5 = new byte[5][]
        getData5[0] = 'mget'.bytes
        for (i in 1..<5) {
            getData5[i] = ('key:' + i).bytes
        }
        def getRequest2 = new Request(getData5, false, false)
        getRequest2.slotNumber = slotNumber
        requestHandler.parseSlots(getRequest2)
        m.handleRequest(getRequest2, socket)
        then:
        1 == 1

        when:
        multiShard.shards[0].multiSlotRange.list[0].end = 4095
        def shard2 = new Shard()
        shard2.nodes << new Node(master: true, host: 'localhost', port: 7381)
        shard2.multiSlotRange = new MultiSlotRange(list: [])
        shard2.multiSlotRange.addSingle(4096, 8191)
        multiShard.shards << shard2
        RequestHandler.updateMultiShardShadows(multiShard)
        m.handleRequest(getRequest2, socket)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.clusterEnabled = false
    }
}
