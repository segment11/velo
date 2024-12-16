package io.velo

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.acl.AclUsers
import io.velo.acl.U
import io.velo.command.AGroup
import io.velo.command.XGroup
import io.velo.decode.Request
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.LeaderSelector
import io.velo.repl.Repl
import io.velo.repl.ReplType
import io.velo.repl.cluster.MultiShard
import io.velo.reply.*
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class RequestHandlerTest extends Specification {
    final short slot = 0
    final byte workerId = 0
    final byte netWorkers = 1
    final short slotNumber = 1

    def 'test handle'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId, netWorkers, slotNumber, snowFlake, Config.create())
        println requestHandler
        requestHandler.requestHandlerGauge.collect()

        def config2 = Config.create().with('localTest', 'true')
        def requestHandler2 = new RequestHandler(workerId, netWorkers, slotNumber, snowFlake, config2)

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def socket = SocketInspectorTest.mockTcpSocket(eventloopCurrent)

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        expect:
        requestHandler.workerId == workerId
        requestHandler.netWorkers == netWorkers
        requestHandler.slotNumber == slotNumber
        requestHandler.snowFlake == snowFlake
        requestHandler2.localTestRandomValueList.size() > 0
        requestHandler.getA_ZGroupCommand('a'.bytes[0]) instanceof AGroup

        when:
        def requestList = ('a'..'z').collect {
            def cmd = it + 'zzz'
            def data = new byte[1][]
            data[0] = cmd.bytes
            new Request(data, false, false)
        }
        def requestList2 = ('A'..'Z').collect {
            def cmd = it + 'zzz'
            def data = new byte[1][]
            data[0] = cmd.bytes
            new Request(data, false, false)
        }
        def someRequestList = ['ping', 'quit', 'auth'].collect {
            def data = new byte[1][]
            data[0] = it.bytes
            new Request(data, false, false)
        }
        def otherData = new byte[1][]
        otherData[0] = '123'.bytes
        def otherRequest = new Request(otherData, false, false)
        requestHandler.parseSlots(otherRequest)
        then:
        requestList.every {
            requestHandler.parseSlots(it)
            it.slotWithKeyHashList.size() == 0
        }
        requestList2.every {
            requestHandler.parseSlots(it)
            it.slotWithKeyHashList.size() == 0
        }
        someRequestList.every {
            requestHandler.parseSlots(it)
            it.slotWithKeyHashList == null
        }
        otherRequest.slotWithKeyHashList == null

        // test handle
        when:
        requestHandler.stop()
        then:
        requestHandler.handle(requestList[0], socket) == ErrorReply.SERVER_STOPPED

        when:
        requestHandler.isStopped = false
        ConfForGlobal.requestSummary = true
        then:
        requestList.every {
            requestHandler.handle(it, socket) == NilReply.INSTANCE
        }
        requestList2.every {
            requestHandler.handle(it, socket) == NilReply.INSTANCE
        }

        when:
        def reply = requestHandler.handle(someRequestList[0], socket)
        then:
        reply == PongReply.INSTANCE

        when:
        reply = requestHandler.handle(someRequestList[1], socket)
        then:
        reply == OKReply.INSTANCE

        when:
        reply = requestHandler.handle(someRequestList[2], socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        AclUsers.instance.initForTest()
        // default user nopass
        def authData = new byte[2][]
        authData[0] = 'auth'.bytes
        authData[1] = 'password'.bytes
        def authRequest = new Request(authData, false, false)
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        AclUsers.instance.upInsert('default') { u ->
            u.password = U.Password.plain('password1')
        }
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        AclUsers.instance.upInsert('default') { u ->
            u.password = U.Password.plain('password')
        }
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        def authData3 = new byte[3][]
        authData3[0] = 'auth'.bytes
        authData3[1] = 'test-user'.bytes
        authData3[2] = 'password'.bytes
        def authRequest3 = new Request(authData3, false, false)
        reply = requestHandler.handle(authRequest3, socket)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        AclUsers.instance.upInsert('test-user') { u ->
            u.on = false
            u.password = U.Password.plain('password')
        }
        reply = requestHandler.handle(authRequest3, socket)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        AclUsers.instance.upInsert('test-user') { u ->
            u.on = true
        }
        reply = requestHandler.handle(authRequest3, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        ConfForGlobal.PASSWORD = 'password'
        SocketInspector.clearUserData(socket)
        def getData1 = new byte[1][]
        getData1[0] = 'get'.bytes
        def getRequest = new Request(getData1, false, false)
        getRequest.u = new U('test-user')
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.NO_AUTH

        when:
        // mock already authed
        SocketInspector.setAuthUser(socket, 'test-user')
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        ConfForGlobal.PASSWORD = null
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        AclUsers.instance.upInsert('default') { u ->
            u.password = U.Password.plain('password')
        }
        def socket2 = SocketInspectorTest.mockTcpSocket(eventloopCurrent, 46380)
        ConfForGlobal.PASSWORD = 'password'
        SocketInspector.clearUserData(socket2)
        def authRequestAsHttp = new Request(authData, true, false)
        def base64Encoded = new String(Base64.getEncoder().encode('default:123456'.bytes))
        authRequestAsHttp.httpHeaders = ['Authorization': 'Basic ' + base64Encoded]
        reply = requestHandler.handle(authRequestAsHttp, socket2)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        AclUsers.instance.upInsert('default') { u ->
            u.password = U.Password.plain('123456')
        }
        reply = requestHandler.handle(authRequestAsHttp, socket2)
        then:
        reply == NilReply.INSTANCE

        when:
        AclUsers.instance.delete('test-user')
        SocketInspector.clearUserData(socket2)
        def base64Encoded2 = new String(Base64.getEncoder().encode('test-user:123456'.bytes))
        authRequestAsHttp.httpHeaders = ['Authorization': 'Basic ' + base64Encoded2]
        reply = requestHandler.handle(authRequestAsHttp, socket2)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        AclUsers.instance.upInsert('test-user') { u ->
            u.on = false
            u.password = U.Password.plain('123456')
        }
        reply = requestHandler.handle(authRequestAsHttp, socket2)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        // new client
        def socket3 = SocketInspectorTest.mockTcpSocket(eventloopCurrent, 46381)
        authRequestAsHttp.removeHttpHeader('Authorization')
        reply = requestHandler.handle(authRequestAsHttp, socket3)
        then:
        reply == ErrorReply.NO_AUTH

        when:
        ConfForGlobal.PASSWORD = null
        reply = requestHandler.handle(authRequestAsHttp, socket3)
        then:
        reply == NilReply.INSTANCE

        when:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def key = 'key'
        def sKey = BaseCommand.slot(key.bytes, slotNumber)
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = key.bytes
        def getRequest2 = new Request(getData2, false, false)
        getRequest2.slotNumber = slotNumber
        requestHandler.parseSlots(getRequest2)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        getData2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def cv = new CompressedValue()
        cv.compressedData = new byte[10]
        cv.compressedLength = 10
        cv.uncompressedLength = 10
        cv.keyHash = sKey.keyHash()
        oneSlot.put(key, sKey.bucketIndex(), cv)
        getData2[1] = key.bytes
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 10

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cv.keyHash = sKey.keyHash()
        oneSlot.put(key, sKey.bucketIndex(), cv)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof ErrorReply

        when:
        getData2[1] = (XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ',' + XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD).bytes
        requestHandler.parseSlots(getRequest2)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof BulkReply

        when:
        getData2[1] = RequestHandler.AS_KEY_GET_SLOT_RANGE_IN_CURRENT_CONNECTION_THREAD_LOCALLY.bytes
        requestHandler.parseSlots(getRequest2)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw) == '0/0-16383'

        when:
        def setData3 = new byte[3][]
        setData3[0] = 'set'.bytes
        setData3[1] = key.bytes
        setData3[2] = 'value'.bytes
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())
        def setRequest = new Request(setData3, false, false)
        setRequest.slotNumber = slotNumber
        requestHandler.parseSlots(setRequest)
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        setData3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        setData3[1] = key.bytes
        setData3[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        oneSlot.readonly = true
        setData3[2] = 'value'.bytes
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.READONLY

        when:
        oneSlot.readonly = false
        setData3[0] = '123'.bytes
        def setRequest2 = new Request(setData3, false, false)
        reply = requestHandler.handle(setRequest2, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        // repl
        def slaveUuid = 11L
        def replData = new byte[4][]
        replData[0] = new byte[8]
        ByteBuffer.wrap(replData[0]).putLong(slaveUuid)
        replData[1] = new byte[2]
        ByteBuffer.wrap(replData[1]).putShort(slot)
        replData[2] = new byte[1]
        replData[2][0] = ReplType.test.code
        replData[3] = new byte[0]
        oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, 'localhost', 6380)
        def replRequest = new Request(replData, false, true)
        replRequest.slotNumber = slotNumber
        reply = requestHandler.handle(replRequest, socket)
        then:
        reply instanceof Repl.ReplReply

        when:
        // http metrics
        def httpData = new byte[1][]
        httpData[0] = 'metrics'.bytes
        def httpRequest = new Request(httpData, true, false)
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('dict_size')

        when:
        // for haproxy
        // http metrics
        def leaderSelector = LeaderSelector.instance
        leaderSelector.hasLeadershipLocalMocked = true
        httpData[0] = 'master'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('master')

        when:
        leaderSelector.hasLeadershipLocalMocked = false
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        httpData[0] = 'master_or_slave'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('master or slave')

        when:
        leaderSelector.hasLeadershipLocalMocked = true
        httpData[0] = 'slave'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        leaderSelector.hasLeadershipLocalMocked = false
        httpData[0] = 'slave'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('slave')

        when:
        httpData[0] = 'slave_with_zone=zone1'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        leaderSelector.hasLeadershipLocalMocked = true
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        leaderSelector.hasLeadershipLocalMocked = false
        ConfForGlobal.targetAvailableZone = 'zone1'
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('zone1')

        // cmd stat count
        when:
        httpData[0] = 'cmd_stat_count'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply

        when:
        httpData[0] = 'cmd_stat_count=all'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply

        when:
        httpData[0] = 'cmd_stat_count=get'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply

        when:
        httpData[0] = null
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        httpData[0] = '123'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        Debug.instance.logCmd = true
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        def httpData2 = new byte[2][]
        httpData2[0] = '123'.bytes
        httpData2[1] = '123'.bytes
        def httpRequest2 = new Request(httpData2, true, false)
        reply = requestHandler.handle(httpRequest2, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        httpData2[0] = null
        reply = requestHandler.handle(httpRequest2, socket)
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        Debug.instance.logCmd = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test cmd stat count'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId, netWorkers, slotNumber, snowFlake, Config.create())

        expect:
        requestHandler.cmdStatCountTotal() == 0

        when:
        def random = new Random()
        ('a'..'z').each { ch ->
            10.times { i ->
                def firstByte = ch.bytes[0]
                def cmd = ch + i
                requestHandler.getCmdCountStat(cmd)

                requestHandler.increaseCmdStatArray(cmd)

                (1 + random.nextInt(100)).times { j ->
                    requestHandler.increaseCmdStatArray(cmd)
                }

                if (i > 0) {
                    def lastCmd = ch + (i - 1)
                    requestHandler.increaseCmdStatArray(lastCmd)
                }
            }
        }
        println 'total cmd count: ' + requestHandler.cmdStatCountTotal()
        println requestHandler.cmdStatAsPrometheusFormatString()
        ('a'..'z').each { ch ->
            def cmd1 = ch + 1
            println 'cmd ' + cmd1 + ' count: ' + requestHandler.getCmdCountStat(cmd1)
        }
        then:
        1 == 1
    }

    def 'test cluster multi shard shadows'() {
        given:
        Consts.persistDir.mkdirs()
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        RequestHandler.initMultiShardShadows((byte) 2)
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]

        when:
        def multiShard = new MultiShard(Consts.persistDir)
        RequestHandler.updateMultiShardShadows(multiShard)
        then:
        RequestHandler.getMultiShardShadow().mySelfShard == multiShard.mySelfShard()

        cleanup:
        Consts.persistDir.deleteDir()
    }
}
