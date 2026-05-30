package io.velo

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.activej.inject.binding.OptionalDependency
import io.velo.acl.AclUsers
import io.velo.acl.RCmd
import io.velo.acl.U
import io.velo.decode.Request
import io.velo.persist.Consts
import io.velo.persist.KeyBucket
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.LeaderSelector
import io.velo.repl.ReplRequest
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
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class MultiWorkerServerTest extends Specification {
    final short slot0 = 0
    final short slot1 = 1
    final byte workerId0 = 0
    final byte workerId1 = 1
    final short slotNumber = 2
    final byte slotWorkers = 2
    final byte netWorkers = 2

    def 'test shutdown command requests server stop when acl permits'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")
        def eventloopCurrent = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopCurrent.keepAlive(true)
        def waitLatch = new CountDownLatch(1)
        Thread.start {
            waitLatch.countDown()
            eventloopCurrent.run()
        }
        waitLatch.await()
        def socket = SocketInspectorTest.mockTcpSocket(eventloopCurrent)
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = eventloopArray
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        ConfForGlobal.PASSWORD = null
        def firstShutdownLatch = new CountDownLatch(1)
        def duplicateShutdownLatch = new CountDownLatch(1)
        def shutdownCount = new AtomicInteger()
        m.shutdownHandler = {
            def count = shutdownCount.incrementAndGet()
            if (count == 1) {
                firstShutdownLatch.countDown()
            }
            if (count == 2) {
                duplicateShutdownLatch.countDown()
            }
        }

        def badData = new byte[2][]
        badData[0] = 'shutdown'.bytes
        badData[1] = 'later'.bytes
        def badRequest = new Request(badData, false, false)
        def duplicateSaveData = new byte[3][]
        duplicateSaveData[0] = 'shutdown'.bytes
        duplicateSaveData[1] = 'save'.bytes
        duplicateSaveData[2] = 'nosave'.bytes
        def duplicateSaveRequest = new Request(duplicateSaveData, false, false)
        def goodData = new byte[3][]
        goodData[0] = 'shutdown'.bytes
        goodData[1] = 'nosave'.bytes
        goodData[2] = 'now'.bytes
        def goodRequest = new Request(goodData, false, false)
        def secondGoodData = new byte[3][]
        secondGoodData[0] = 'shutdown'.bytes
        secondGoodData[1] = 'nosave'.bytes
        secondGoodData[2] = 'force'.bytes
        def secondGoodRequest = new Request(secondGoodData, false, false)
        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)
        String pingResponseText = null
        String badResponseText = null
        String duplicateSaveResponseText = null
        String responseText = null
        String secondResponseText = null

        when:
        def pingP = m.handleRequest(pingRequest, socket)
        pingP.whenResult { reply ->
            pingResponseText = new String(reply.array())
        }.result
        def badP = m.handleRequest(badRequest, socket)
        badP.whenResult { reply ->
            badResponseText = new String(reply.array())
        }.result
        def duplicateSaveP = m.handleRequest(duplicateSaveRequest, socket)
        duplicateSaveP.whenResult { reply ->
            duplicateSaveResponseText = new String(reply.array())
        }.result
        def shutdownNotRequestedForBadOption = firstShutdownLatch.await(150, TimeUnit.MILLISECONDS)
        def p = m.handleRequest(goodRequest, socket)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result
        def p2 = m.handleRequest(secondGoodRequest, socket)
        p2.whenResult { reply ->
            secondResponseText = new String(reply.array())
        }.result

        then:
        pingResponseText == '+PONG\r\n'
        badResponseText == '-ERR format\r\n'
        duplicateSaveResponseText == '-ERR format\r\n'
        !shutdownNotRequestedForBadOption
        responseText == '+OK\r\n'
        secondResponseText == '+OK\r\n'
        firstShutdownLatch.await(1, TimeUnit.SECONDS)
        !duplicateShutdownLatch.await(300, TimeUnit.MILLISECONDS)
        shutdownCount.get() == 1

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test shutdown command is blocked by acl dangerous disallow'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")
        def eventloopCurrent = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopCurrent.keepAlive(true)
        def waitLatch = new CountDownLatch(1)
        Thread.start {
            waitLatch.countDown()
            eventloopCurrent.run()
        }
        waitLatch.await()
        def socket = SocketInspectorTest.mockTcpSocket(eventloopCurrent)
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = eventloopArray
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        AclUsers.instance.upInsert('default') { u ->
            u.addRCmd(true, RCmd.fromLiteral('+@all'))
            u.addRCmdDisallow(true, RCmd.fromLiteral('-@dangerous'))
        }
        ConfForGlobal.PASSWORD = null
        def shutdownLatch = new CountDownLatch(1)
        m.shutdownHandler = { shutdownLatch.countDown() }

        def data = new byte[1][]
        data[0] = 'shutdown'.bytes
        def request = new Request(data, false, false)
        String responseText = null

        when:
        def p = m.handleRequest(request, socket)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        responseText == '-ERR user acl permit limit !NOPERM!\r\n'
        !shutdownLatch.await(100, TimeUnit.MILLISECONDS)

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test fast auth gate keeps quit available'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")
        def eventloopCurrent = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopCurrent.keepAlive(true)
        def waitLatch = new CountDownLatch(1)
        Thread.start {
            waitLatch.countDown()
            eventloopCurrent.run()
        }
        waitLatch.await()
        def pingSocket = SocketInspectorTest.mockTcpSocket(eventloopCurrent)
        def echoSocket = SocketInspectorTest.mockTcpSocket(eventloopCurrent, 46380)
        def quitSocket = SocketInspectorTest.mockTcpSocket(eventloopCurrent, 46381)
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = eventloopArray
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        ConfForGlobal.PASSWORD = 'password'

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)
        def echoData = new byte[2][]
        echoData[0] = 'echo'.bytes
        echoData[1] = 'hello'.bytes
        def echoRequest = new Request(echoData, false, false)
        def quitData = new byte[1][]
        quitData[0] = 'quit'.bytes
        def quitRequest = new Request(quitData, false, false)
        String pingResponseText = null
        String echoResponseText = null
        String quitResponseText = null

        when:
        def pingPromise = m.handleRequest(pingRequest, pingSocket)
        pingPromise.whenResult { reply ->
            pingResponseText = new String(reply.array())
        }.result
        def echoPromise = m.handleRequest(echoRequest, echoSocket)
        echoPromise.whenResult { reply ->
            echoResponseText = new String(reply.array())
        }.result
        def quitPromise = m.handleRequest(quitRequest, quitSocket)
        quitPromise.whenResult { reply ->
            quitResponseText = new String(reply.array())
        }.result

        then:
        pingResponseText == '-ERR no auth\r\n'
        echoResponseText == '-ERR no auth\r\n'
        quitResponseText == '+OK\r\n'
        quitSocket.isClosed()

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test pipeline auth refresh acl state'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")
        def eventloopCurrent = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopCurrent.keepAlive(true)
        def waitLatch = new CountDownLatch(1)
        Thread.start {
            waitLatch.countDown()
            eventloopCurrent.run()
        }
        waitLatch.await()
        def socket = SocketInspectorTest.mockTcpSocket(eventloopCurrent)
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = eventloopArray
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        AclUsers.instance.upInsert('limited-user') { u ->
            u.on = true
            u.password = U.Password.plain('password')
        }
        ConfForGlobal.PASSWORD = null

        def authData = new byte[3][]
        authData[0] = 'auth'.bytes
        authData[1] = 'limited-user'.bytes
        authData[2] = 'password'.bytes
        def authRequest = new Request(authData, false, false)

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)

        ArrayList<Request> pipeline = [authRequest, pingRequest]
        String responseText = null

        when:
        def p = m.handlePipeline(pipeline, socket, (short) 1)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        responseText == '+OK\r\n-ERR user acl permit limit !NOPERM!\r\n'

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test pipeline auth barrier before later commands'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")

        def netEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netEventloop.keepAlive(true)
        def waitLatch1 = new CountDownLatch(1)
        Thread.start {
            waitLatch1.countDown()
            netEventloop.run()
        }
        waitLatch1.await()

        def slotEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        slotEventloop.keepAlive(true)
        def waitLatch2 = new CountDownLatch(1)
        def slotThreadId = new long[1]
        Thread.start {
            slotThreadId[0] = Thread.currentThread().threadId()
            waitLatch2.countDown()
            slotEventloop.run()
        }
        waitLatch2.await()

        def socket = SocketInspectorTest.mockTcpSocket(netEventloop)
        Eventloop[] netEventloopArray = [netEventloop]
        Eventloop[] slotEventloopArray = [slotEventloop]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(netEventloopArray, slotEventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, slotThreadId[0])

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = netEventloopArray
        m.slotWorkerEventloopArray = slotEventloopArray
        m.isReuseNetWorkerEventloop = false
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        AclUsers.instance.upInsert('default') { u ->
            u.on = true
            u.password = U.Password.plain('password')
        }
        ConfForGlobal.PASSWORD = 'password'

        def authData = new byte[2][]
        authData[0] = 'auth'.bytes
        authData[1] = 'password'.bytes
        def authRequest = new Request(authData, false, false)

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)

        ArrayList<Request> pipeline = [authRequest, pingRequest]
        String responseText = null

        when:
        def p = m.handlePipeline(pipeline, socket, (short) 1)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        responseText == '+OK\r\n+PONG\r\n'

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test pipeline readonly barrier before write commands'() {
        given:
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")

        def netEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netEventloop.keepAlive(true)
        def waitLatch1 = new CountDownLatch(1)
        Thread.start {
            waitLatch1.countDown()
            netEventloop.run()
        }
        waitLatch1.await()

        def slotEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        slotEventloop.keepAlive(true)
        def waitLatch2 = new CountDownLatch(1)
        def slotThreadId = new long[1]
        Thread.start {
            slotThreadId[0] = Thread.currentThread().threadId()
            waitLatch2.countDown()
            slotEventloop.run()
        }
        waitLatch2.await()

        def socket = SocketInspectorTest.mockTcpSocket(netEventloop)
        Eventloop[] netEventloopArray = [netEventloop]
        Eventloop[] slotEventloopArray = [slotEventloop]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(netEventloopArray, slotEventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, slotThreadId[0])

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = netEventloopArray
        m.slotWorkerEventloopArray = slotEventloopArray
        m.isReuseNetWorkerEventloop = false
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        ConfForGlobal.PASSWORD = null

        SocketInspector.setAuthUser(socket, 'default')

        // set connection to readonly
        SocketInspector.setConnectionReadonly(socket, true)

        // PING is not a write command, so readonly check won't block it
        // Use a pipeline: READWRITE, PING to verify the barrier and updated state
        def readwriteData = new byte[1][]
        readwriteData[0] = 'readwrite'.bytes
        def readwriteRequest = new Request(readwriteData, false, false)

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)

        // pipeline: READWRITE, PING
        ArrayList<Request> pipeline = [readwriteRequest, pingRequest]
        String responseText = null

        when:
        def p = m.handlePipeline(pipeline, socket, (short) 1)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        // READWRITE returns OK, PING returns PONG — no READONLY error for PING
        responseText == '+OK\r\n+PONG\r\n'
        // verify readonly was cleared by READWRITE
        !SocketInspector.isConnectionReadonly(socket)

        cleanup:
        ConfForGlobal.PASSWORD = null
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test pipeline connection state command at end'() {
        given:
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")

        def netEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netEventloop.keepAlive(true)
        def waitLatch1 = new CountDownLatch(1)
        Thread.start {
            waitLatch1.countDown()
            netEventloop.run()
        }
        waitLatch1.await()

        def slotEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        slotEventloop.keepAlive(true)
        def waitLatch2 = new CountDownLatch(1)
        def slotThreadId = new long[1]
        Thread.start {
            slotThreadId[0] = Thread.currentThread().threadId()
            waitLatch2.countDown()
            slotEventloop.run()
        }
        waitLatch2.await()

        def socket = SocketInspectorTest.mockTcpSocket(netEventloop)
        Eventloop[] netEventloopArray = [netEventloop]
        Eventloop[] slotEventloopArray = [slotEventloop]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(netEventloopArray, slotEventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, slotThreadId[0])

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = netEventloopArray
        m.slotWorkerEventloopArray = slotEventloopArray
        m.isReuseNetWorkerEventloop = false
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        ConfForGlobal.PASSWORD = null

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)

        def readonlyData = new byte[1][]
        readonlyData[0] = 'readonly'.bytes
        def readonlyRequest = new Request(readonlyData, false, false)

        // pipeline: PING, READONLY — state command at end
        ArrayList<Request> pipeline = [pingRequest, readonlyRequest]
        String responseText = null

        when:
        def p = m.handlePipeline(pipeline, socket, (short) 1)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        responseText.contains('+PONG')
        responseText.contains('+OK')

        cleanup:
        ConfForGlobal.PASSWORD = null
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

    def 'test pipeline with commands before and after barrier'() {
        given:
        def oldPassword = ConfForGlobal.PASSWORD
        def oldSocketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector
        def config = Config.create()
                .with('doFileLock', "false")
                .with('slotNumber', '1')
                .with('slotWorkers', '1')
                .with('netWorkers', '1')
                .with("net.listenAddresses", "localhost:7379")

        def netEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        netEventloop.keepAlive(true)
        def waitLatch1 = new CountDownLatch(1)
        Thread.start {
            waitLatch1.countDown()
            netEventloop.run()
        }
        waitLatch1.await()

        def slotEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        slotEventloop.keepAlive(true)
        def waitLatch2 = new CountDownLatch(1)
        def slotThreadId = new long[1]
        Thread.start {
            slotThreadId[0] = Thread.currentThread().threadId()
            waitLatch2.countDown()
            slotEventloop.run()
        }
        waitLatch2.await()

        def socket = SocketInspectorTest.mockTcpSocket(netEventloop)
        Eventloop[] netEventloopArray = [netEventloop]
        Eventloop[] slotEventloopArray = [slotEventloop]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(netEventloopArray, slotEventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 1)
        LocalPersist.instance.fixSlotThreadId((short) 0, slotThreadId[0])

        def m = new MultiWorkerServer()
        m.configInject = config
        m.primaryReactor(config)
        m.netWorkerEventloopArray = netEventloopArray
        m.slotWorkerEventloopArray = slotEventloopArray
        m.isReuseNetWorkerEventloop = false
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), config)
        m.scheduleRunnableArray = new TaskRunnable[1]
        m.scheduleRunnableArray[0] = new TaskRunnable((byte) 0, (byte) 1)
        m.socketInspector = inspector
        m.onStart()

        AclUsers.instance.initForTest()
        AclUsers.instance.upInsert('default') { u ->
            u.on = true
            u.password = U.Password.plain('password')
        }
        ConfForGlobal.PASSWORD = 'password'

        // pre-authenticate so first PING passes the auth gate
        SocketInspector.setAuthUser(socket, 'default')

        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)

        // second auth request to a different user won't work since only 'default' exists
        // use readwrite as the barrier command instead
        def readwriteData = new byte[1][]
        readwriteData[0] = 'readwrite'.bytes
        def readwriteRequest = new Request(readwriteData, false, false)

        SocketInspector.setConnectionReadonly(socket, true)

        // pipeline: PING, READWRITE, PING — commands before and after barrier
        ArrayList<Request> pipeline = [pingRequest, readwriteRequest, pingRequest]
        String responseText = null

        when:
        def p = m.handlePipeline(pipeline, socket, (short) 1)
        p.whenResult { reply ->
            responseText = new String(reply.array())
        }.result

        then:
        responseText == '+PONG\r\n+OK\r\n+PONG\r\n'
        !SocketInspector.isConnectionReadonly(socket)

        cleanup:
        ConfForGlobal.PASSWORD = oldPassword
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = oldSocketInspector
        if (m?.primaryScheduleRunnable != null) {
            m.onStop()
        }
    }

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
        replRequest.replRequest = new ReplRequest(0L, (short) 0, ReplType.hi, new byte[10], 10)
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
                .with("dynConfig", Config.create().with("repl_connect_timeout_millis", "6000"))
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

        when: 'indexWorkers > cpu number should still be rejected'
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
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId0, netWorkers, slotNumber, snowFlake, Config.create())
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = requestHandler
        m.slotWorkerThreadIds = MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds

        and:
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        def multiShard = localPersist.getMultiShard()
        multiShard.mySelfShard().multiSlotRange.addSingle(0, 8191)
        def shard1 = new Shard()
        shard1.nodes << new Node(master: true, host: 'localhost', port: 7380)
        shard1.multiSlotRange = new MultiSlotRange(list: [])
        shard1.multiSlotRange.addSingle(8192, 16383)
        multiShard.shards << shard1

        when:
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
        (checkResult as ErrorReply).message == "MOVED ${getRequest.slotWithKeyHashList.first().toClientSlot()} localhost:7380"

        when:
        ArrayList<BaseCommand.SlotWithKeyHash> ignoreSlotWithKeyHashList = [BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT]
        def checkResult2 = m.checkClusterSlot(ignoreSlotWithKeyHashList)
        then:
        checkResult2 == null

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        multiShard.shards[0].nodes[0].mySelf = false
        multiShard.shards[1].nodes[0].mySelf = true
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
        m.handleRequest(getRequest2, socket)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.clusterEnabled = false
    }

    def 'test cluster slot cross shards from non slot thread'() {
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
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId0, netWorkers, slotNumber, snowFlake, Config.create())
        m.requestHandlerArray = new RequestHandler[1]
        m.requestHandlerArray[0] = requestHandler
        m.slotWorkerThreadIds = MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds

        and:
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        def multiShard = localPersist.getMultiShard()
        multiShard.mySelfShard().multiSlotRange.addSingle(0, 8191)
        def shard1 = new Shard()
        shard1.nodes << new Node(master: true, host: 'localhost', port: 7380)
        shard1.multiSlotRange = new MultiSlotRange(list: [])
        shard1.multiSlotRange.addSingle(8192, 16383)
        multiShard.shards << shard1
        and:
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = 'key'.bytes
        def getRequest = new Request(getData2, false, false)
        getRequest.slotNumber = slotNumber
        requestHandler.parseSlots(getRequest)

        when:
        def resultRef = new AtomicReference<ErrorReply>()
        def errorRef = new AtomicReference<Throwable>()
        def worker = Thread.start {
            try {
                resultRef.set(m.checkClusterSlot(getRequest.slotWithKeyHashList))
            } catch (Throwable t) {
                errorRef.set(t)
            }
        }
        worker.join()

        then:
        errorRef.get() == null
        resultRef.get() != null
        resultRef.get().message == "MOVED ${getRequest.slotWithKeyHashList.first().toClientSlot()} localhost:7380"

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.clusterEnabled = false
    }

    def 'test bucket LRU percent computes correct maxSize for non-100 percent'() {
        given:
        var m1 = new MultiWorkerServer.InnerModule()
        var cc = ConfForSlot.global

        def config50 = Config.create()
                .with('doFileLock', "false")
                .with("net.listenAddresses", "localhost:7379")
                .with("debugMode", "true")
                .with("pureMemory", "true")
                .with("zookeeperConnectString", 'localhost:2181')
                .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                .with("bucket.lruPerFd.percent", "50")
                .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
        m1.skipZookeeperConnectCheck = true

        when:
        m1.confForSlot(config50)

        then:
        def buckets = ConfForSlot.global.confBucket.bucketsPerSlot
        ConfForSlot.global.confBucket.lruPerFd.maxSize == buckets / 2
    }

    def 'test startup dyn config items clear when config omits dynConfig'() {
        given:
        var m1 = new MultiWorkerServer.InnerModule()
        var cc = ConfForSlot.global
        m1.skipZookeeperConnectCheck = true
        ConfForGlobal.initDynConfigItems.clear()

        def baseConfig = {
            Config.create()
                    .with('doFileLock', "false")
                    .with("net.listenAddresses", "localhost:7379")
                    .with("debugMode", "true")
                    .with("pureMemory", "true")
                    .with("zookeeperConnectString", 'localhost:2181')
                    .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                    .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                    .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                    .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                    .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                    .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                    .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                    .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
        }

        when:
        m1.confForSlot(baseConfig().with("dynConfig", Config.create().with("type_zset_max_size", "4096")))

        then:
        ConfForGlobal.initDynConfigItems["type_zset_max_size"] == "4096"

        when:
        m1.confForSlot(baseConfig())

        then:
        ConfForGlobal.initDynConfigItems.isEmpty()

        cleanup:
        ConfForGlobal.initDynConfigItems.clear()
    }

    def 'test onceScanMaxLoopCount accepts values above 127 via config'() {
        given:
        var m1 = new MultiWorkerServer.InnerModule()
        var cc = ConfForSlot.global

        def config = Config.create()
                .with('doFileLock', "false")
                .with("net.listenAddresses", "localhost:7379")
                .with("debugMode", "true")
                .with("pureMemory", "true")
                .with("zookeeperConnectString", 'localhost:2181')
                .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                .with("bucket.onceScanMaxLoopCount", "512")
                .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
                .with("wal.onceScanMaxLoopCount", "512")
        m1.skipZookeeperConnectCheck = true

        when:
        m1.confForSlot(config)

        then:
        ConfForSlot.global.confBucket.onceScanMaxLoopCount == 512
        ConfForSlot.global.confWal.onceScanMaxLoopCount == 512
    }

    def 'test repl short config rejects overflow values'() {
        given:
        var m1 = new MultiWorkerServer.InnerModule()
        var cc = ConfForSlot.global
        m1.skipZookeeperConnectCheck = true

        def makeConfig = { String key, String value ->
            Config.create()
                    .with('doFileLock', "false")
                    .with("net.listenAddresses", "localhost:7379")
                    .with("debugMode", "true")
                    .with("pureMemory", "true")
                    .with("zookeeperConnectString", 'localhost:2181')
                    .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                    .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                    .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                    .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                    .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                    .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                    .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                    .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
                    .with(key, value)
        }

        when: '65537 would truncate to short 1, should be rejected'
        m1.confForSlot(makeConfig("repl.binlogForReadCacheSegmentMaxCount", "65537"))
        then:
        def e1 = thrown(IllegalArgumentException)
        e1.message.contains("binlogForReadCacheSegmentMaxCount")

        when: '65538 would truncate to short 2, should be rejected'
        m1.confForSlot(makeConfig("repl.binlogFileKeepMaxCount", "65538"))
        then:
        def e2 = thrown(IllegalArgumentException)
        e2.message.contains("binlogFileKeepMaxCount")

        when: 'negative value should be rejected for cache segment max count'
        m1.confForSlot(makeConfig("repl.binlogForReadCacheSegmentMaxCount", "-1"))
        then:
        def e3 = thrown(IllegalArgumentException)
        e3.message.contains("binlogForReadCacheSegmentMaxCount")

        when: 'negative value should be rejected for file keep max count'
        m1.confForSlot(makeConfig("repl.binlogFileKeepMaxCount", "-1"))
        then:
        def e4 = thrown(IllegalArgumentException)
        e4.message.contains("binlogFileKeepMaxCount")

        when: 'valid values should be accepted'
        def validConfig = Config.create()
                .with('doFileLock', "false")
                .with("net.listenAddresses", "localhost:7379")
                .with("debugMode", "true")
                .with("pureMemory", "true")
                .with("zookeeperConnectString", 'localhost:2181')
                .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
                .with("repl.binlogForReadCacheSegmentMaxCount", "20")
                .with("repl.binlogFileKeepMaxCount", "30")
        m1.confForSlot(validConfig)
        then:
        ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount == (short) 20
        ConfForSlot.global.confRepl.binlogFileKeepMaxCount == (short) 30
    }

    def 'test bucket initialSplitNumber rejects overflow and accepts only 1, 3, or 9'() {
        given:
        var m1 = new MultiWorkerServer.InnerModule()
        var cc = ConfForSlot.global
        m1.skipZookeeperConnectCheck = true

        def makeConfig = { String value ->
            Config.create()
                    .with('doFileLock', "false")
                    .with("net.listenAddresses", "localhost:7379")
                    .with("pureMemory", "true")
                    .with("zookeeperConnectString", 'localhost:2181')
                    .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                    .with("bucket.initialSplitNumber", value)
                    .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                    .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                    .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                    .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                    .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                    .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
        }

        when: '257 would truncate to byte 1, should be rejected'
        m1.confForSlot(makeConfig("257"))
        then:
        def e1 = thrown(IllegalArgumentException)
        e1.message.contains("initialSplitNumber")

        when: '259 would truncate to byte 3, should be rejected'
        m1.confForSlot(makeConfig("259"))
        then:
        def e2 = thrown(IllegalArgumentException)
        e2.message.contains("initialSplitNumber")

        when: 'invalid value 2 should be rejected'
        m1.confForSlot(makeConfig("2"))
        then:
        def e3 = thrown(IllegalArgumentException)
        e3.message.contains("initialSplitNumber")

        when: 'valid value 1'
        m1.confForSlot(makeConfig("1"))
        then:
        ConfForSlot.global.confBucket.initialSplitNumber == (byte) 1

        when: 'valid value 3'
        m1.confForSlot(makeConfig("3"))
        then:
        ConfForSlot.global.confBucket.initialSplitNumber == (byte) 3

        when: 'valid value 9'
        m1.confForSlot(makeConfig("9"))
        then:
        ConfForSlot.global.confBucket.initialSplitNumber == (byte) 9
    }

    def 'test prepareConfig respects MAIN_ARGS for socket settings'() {
        given:
        def tempFile = File.createTempFile('velo-test-socket-', '.properties')
        tempFile.deleteOnExit()
        tempFile.text = '''
            ApplicationSettings.SocketSettings.sendBufferSize=222222
            ApplicationSettings.SocketSettings.receiveBufferSize=222222
            ApplicationSettings.ServerSocketSettings.backlog=333333
            ApplicationSettings.ServerSocketSettings.receiveBufferSize=333333
            '''.stripIndent()

        def oldMainArgs = MultiWorkerServer.MAIN_ARGS
        def oldSendBuf = System.getProperty('io.activej.reactor.net.SocketSettings.sendBufferSize')
        def oldRecvBuf = System.getProperty('io.activej.reactor.net.SocketSettings.receiveBufferSize')
        def oldBacklog = System.getProperty('io.activej.reactor.net.ServerSocketSettings.backlog')
        def oldSsRecvBuf = System.getProperty('io.activej.reactor.net.ServerSocketSettings.receiveBufferSize')

        when: 'prepareConfig with MAIN_ARGS null ignores custom file'
        MultiWorkerServer.MAIN_ARGS = null
        MultiWorkerServer.prepareConfig()
        def sendBufBeforeMainArgs = System.getProperty('io.activej.reactor.net.SocketSettings.sendBufferSize')

        then: 'socket settings come from default config, not our custom file'
        sendBufBeforeMainArgs != '222222'

        when: 'prepareConfig with MAIN_ARGS set to custom file'
        MultiWorkerServer.MAIN_ARGS = [tempFile.absolutePath] as String[]
        MultiWorkerServer.prepareConfig()

        then: 'socket settings come from our custom file'
        System.getProperty('io.activej.reactor.net.SocketSettings.sendBufferSize') == '222222'
        System.getProperty('io.activej.reactor.net.SocketSettings.receiveBufferSize') == '222222'
        System.getProperty('io.activej.reactor.net.ServerSocketSettings.backlog') == '333333'
        System.getProperty('io.activej.reactor.net.ServerSocketSettings.receiveBufferSize') == '333333'

        cleanup:
        MultiWorkerServer.MAIN_ARGS = oldMainArgs
        if (oldSendBuf != null) {
            System.setProperty('io.activej.reactor.net.SocketSettings.sendBufferSize', oldSendBuf)
        } else {
            System.clearProperty('io.activej.reactor.net.SocketSettings.sendBufferSize')
        }
        if (oldRecvBuf != null) {
            System.setProperty('io.activej.reactor.net.SocketSettings.receiveBufferSize', oldRecvBuf)
        } else {
            System.clearProperty('io.activej.reactor.net.SocketSettings.receiveBufferSize')
        }
        if (oldBacklog != null) {
            System.setProperty('io.activej.reactor.net.ServerSocketSettings.backlog', oldBacklog)
        } else {
            System.clearProperty('io.activej.reactor.net.ServerSocketSettings.backlog')
        }
        if (oldSsRecvBuf != null) {
            System.setProperty('io.activej.reactor.net.ServerSocketSettings.receiveBufferSize', oldSsRecvBuf)
        } else {
            System.clearProperty('io.activej.reactor.net.ServerSocketSettings.receiveBufferSize')
        }
        tempFile.delete()
    }

    def 'test beforeCreateHandler rejects non-positive worker counts'() {
        given:
        def m1 = new MultiWorkerServer.InnerModule()
        def c = ConfForSlot.global
        def snowFlakes1 = new SnowFlake[1]
        snowFlakes1[0] = new SnowFlake(1, 1)

        when: 'netWorkers=0 should be rejected'
        def configNetZero = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "0")
        m1.beforeCreateHandler(c, snowFlakes1, configNetZero)
        then:
        def e1 = thrown(IllegalArgumentException)
        e1.message.contains("Net workers")

        when: 'netWorkers=-1 should be rejected'
        def configNetNeg = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "-1")
        m1.beforeCreateHandler(c, snowFlakes1, configNetNeg)
        then:
        def e2 = thrown(IllegalArgumentException)
        e2.message.contains("Net workers")

        when: 'slotWorkers=0 should be rejected'
        def configSlotZero = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "1")
                .with("slotWorkers", "0")
        m1.beforeCreateHandler(c, snowFlakes1, configSlotZero)
        then:
        def e3 = thrown(IllegalArgumentException)
        e3.message.contains("Slot workers")

        when: 'slotWorkers=-1 should be rejected'
        def configSlotNeg = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "1")
                .with("slotWorkers", "-1")
        m1.beforeCreateHandler(c, snowFlakes1, configSlotNeg)
        then:
        def e4 = thrown(IllegalArgumentException)
        e4.message.contains("Slot workers")

        when: 'indexWorkers=0 should be rejected'
        def configIdxZero = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "1")
                .with("slotWorkers", "1")
                .with("indexWorkers", "0")
        m1.beforeCreateHandler(c, snowFlakes1, configIdxZero)
        then:
        def e5 = thrown(IllegalArgumentException)
        e5.message.contains("Index workers")

        when: 'indexWorkers=-1 should be rejected'
        def configIdxNeg = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", "1")
                .with("slotWorkers", "1")
                .with("indexWorkers", "-1")
        m1.beforeCreateHandler(c, snowFlakes1, configIdxNeg)
        then:
        def e6 = thrown(IllegalArgumentException)
        e6.message.contains("Index workers")
    }

    def 'test pid file truncated when reused from prior process'() {
        given:
        def tempDir = new File(System.getProperty('java.io.tmpdir'), "velo-test-pid-${System.nanoTime()}")
        tempDir.mkdirs()
        def pidFile = new File(tempDir, 'velo.pid')

        // simulate a prior process that wrote a longer PID (e.g. 10 digits)
        pidFile.text = '9999999999'

        def config = Config.create()
                .with('dir', tempDir.absolutePath)
                .with('doFileLock', 'true')

        when:
        MultiWorkerServer.dirFile(config, true)

        then: 'pid file contains exactly the current PID, not stale trailing digits'
        def currentPid = String.valueOf(ProcessHandle.current().pid())
        pidFile.text == currentPid

        cleanup:
        // release file lock and channel via reflection since fields are private
        try {
            def lockField = MultiWorkerServer.class.getDeclaredField('pidFileLock')
            lockField.setAccessible(true)
            def lock = (FileLock) lockField.get(null)
            if (lock != null) {
                lock.release()
            }
            def channelField = MultiWorkerServer.class.getDeclaredField('pidFileChannel')
            channelField.setAccessible(true)
            def channel = (FileChannel) channelField.get(null)
            if (channel != null) {
                channel.close()
            }
        } catch (Exception ignored) {}
        tempDir.deleteDir()
    }

    def 'test concurrent dirFile attempt does not truncate locked pid file'() {
        given:
        def tempDir = new File(System.getProperty('java.io.tmpdir'), "velo-test-pid2-${System.nanoTime()}")
        tempDir.mkdirs()
        def pidFile = new File(tempDir, 'velo.pid')

        // simulate a running process: write a long PID and hold a lock
        pidFile.text = '9999999999'
        def firstChannel = FileChannel.open(pidFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        def firstLock = firstChannel.tryLock()
        assert firstLock != null: 'first lock should succeed'

        def config = Config.create()
                .with('dir', tempDir.absolutePath)
                .with('doFileLock', 'true')

        when: 'second startup attempt while first holds the lock'
        MultiWorkerServer.dirFile(config, true)

        then: 'should throw because lock is held'
        def ex = thrown(RuntimeException)

        and: 'pid file content is NOT destroyed — still has the original PID'
        pidFile.text == '9999999999'

        cleanup:
        firstLock.release()
        firstChannel.close()
        // release any channel dirFile may have opened before failing
        try {
            def channelField = MultiWorkerServer.class.getDeclaredField('pidFileChannel')
            channelField.setAccessible(true)
            def channel = (FileChannel) channelField.get(null)
            if (channel != null) {
                channel.close()
            }
        } catch (Exception ignored) {}
        tempDir.deleteDir()
    }
}
