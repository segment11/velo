package io.velo.command

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.RequestHandler
import io.velo.SnowFlake
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.decode.Request
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.test.tools.RedisServer
import redis.clients.jedis.Jedis
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.TimeUnit

class MGroupTest extends Specification {
    def _MGroup = new MGroup(null, null, null)
    final short slot = 0

    def 'test parse slot - mget mset'() {
        given:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '0'.bytes
        data5[3] = '0'.bytes
        data5[4] = '0'.bytes
        int slotNumber = 128

        expect:
        _MGroup.parseSlots(cmd, data5, slotNumber).size() == expectedSize

        where:
        cmd    | expectedSize
        'mget' | 4
        'mset' | 2
        'mxxx' | 0
    }

    def 'test parse slot - edge cases'() {
        given:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '0'.bytes
        data5[3] = '0'.bytes
        data5[4] = '0'.bytes
        def data1 = new byte[1][]
        def data4 = new byte[4][]
        int slotNumber = 128

        expect:
        _MGroup.parseSlots('mget', data1, slotNumber).size() == 0
        _MGroup.parseSlots('mset', data1, slotNumber).size() == 0
        _MGroup.parseSlots('mset', data4, slotNumber).size() == 0
        _MGroup.parseSlots('mset', data5, slotNumber).size() == 2

        when:
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        data5[3] = 'view-persist-key-count'.bytes
        then:
        _MGroup.parseSlots('manage', data5, slotNumber).size() == 1
        _MGroup.parseSlots('manage', data1, slotNumber).size() == 0

        when:
        data5[1] = 'xxx'.bytes
        then:
        _MGroup.parseSlots('zzz', data5, slotNumber).size() == 0
    }

    def 'test handle - format errors'() {
        given:
        def mGroup = new MGroup(null, null, null)
        mGroup.from(BaseCommand.mockAGroup())

        expect:
        mGroup.execute(input) == expected

        where:
        input    | expected
        'mget'   | ErrorReply.FORMAT
        'mset'   | ErrorReply.FORMAT
        'msetnx' | ErrorReply.FORMAT
        'move'   | ErrorReply.NOT_SUPPORT
        'zzz'    | NilReply.INSTANCE
    }

    def 'test handle - manage'() {
        given:
        def mGroup = new MGroup(null, null, null)
        mGroup.from(BaseCommand.mockAGroup())
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        expect:
        mGroup.execute('manage') == ErrorReply.FORMAT
    }

    def 'test mget'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mget', data3, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when:
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('mget', data3, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mget()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = mGroup.mget()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == cv.compressedData
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        mGroup.crossRequestWorker = true
        reply = mGroup.mget()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test mset'() {
        given:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '1'.bytes
        data5[3] = 'b'.bytes
        data5[4] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mset', data5, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when:
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('mset', data5, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mset()
        then:
        reply == OKReply.INSTANCE

        when:
        def valA = mGroup.get(mGroup.slotWithKeyHashListParsed[0])
        def valB = mGroup.get(mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '1'.bytes
        valB == '2'.bytes

        when:
        data5[2] = '11'.bytes
        data5[4] = '22'.bytes
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        mGroup.crossRequestWorker = true
        reply = mGroup.mset()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        valA = mGroup.get(mGroup.slotWithKeyHashListParsed[0])
        valB = mGroup.get(mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '11'.bytes
        valB == '22'.bytes

        when:
        def data4 = new byte[4][]
        mGroup.data = data4
        reply = mGroup.mset()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test msetnx'() {
        given:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '1'.bytes
        data5[3] = 'b'.bytes
        data5[4] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('msetnx', data5, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when:
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('msetnx', data5, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.msetnx()
        then:
        reply == IntegerReply.REPLY_1

        when:
        def valA = mGroup.get(mGroup.slotWithKeyHashListParsed[0])
        def valB = mGroup.get(mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '1'.bytes
        valB == '2'.bytes

        when:
        reply = mGroup.msetnx()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data5[1] = 'aa'.bytes
        data5[2] = '11'.bytes
        data5[3] = 'bb'.bytes
        data5[4] = '22'.bytes
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('msetnx', data5, mGroup.slotNumber)
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        mGroup.crossRequestWorker = true
        reply = mGroup.msetnx()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.toCompletableFuture().get() == IntegerReply.REPLY_1

        when:
        valA = mGroup.get(mGroup.slotWithKeyHashListParsed[0])
        valB = mGroup.get(mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '11'.bytes
        valB == '22'.bytes

        when:
        // aa already exists
        data5[1] = 'aa'.bytes
        data5[2] = '11'.bytes
        data5[3] = 'cc'.bytes
        data5[4] = '33'.bytes
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('msetnx', data5, mGroup.slotNumber)
        mGroup.crossRequestWorker = true
        reply = mGroup.msetnx()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.toCompletableFuture().get() == IntegerReply.REPLY_0

        when:
        def data4 = new byte[4][]
        mGroup.data = data4
        reply = mGroup.msetnx()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test migrate'() {
        given:
        if (!RedisServer.isBinExists()) {
            println 'skip test migrate, because redis server binary not exists'
            return
        }

        def redisServer = new RedisServer('test-migrate').noSave().randomPort()
        Thread.start {
            redisServer.run()
        }

        and:
        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup(null, null, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when: 'the migrate command must register the migrated keys, so the request is routed to the slot owner'
        def migrateData = new byte[9][]
        migrateData[0] = 'migrate'.bytes
        migrateData[1] = '127.0.0.1'.bytes
        migrateData[2] = redisServer.port.toString().bytes
        migrateData[3] = ''.bytes
        migrateData[4] = '0'.bytes
        migrateData[5] = '1000'.bytes
        migrateData[6] = 'keys'.bytes
        migrateData[7] = 'a'.bytes
        migrateData[8] = 'b'.bytes
        def parsedSlots = mGroup.parseSlots('migrate', migrateData, mGroup.slotNumber)
        then:
        parsedSlots.size() == 2
        parsedSlots[0].rawKey == 'a'
        parsedSlots[1].rawKey == 'b'

        when: 'the request handler parses the slots of a migrate request'
        def request = new Request(migrateData, false, false)
        request.setSlotNumber(mGroup.slotNumber)
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, new SnowFlake(1, 1), Config.create())
        requestHandler.parseSlots(request)
        then:
        request.getSlotWithKeyHashList().size() == 2
        request.getSlotWithKeyHashList().get(0).rawKey == 'a'
        request.getSlotWithKeyHashList().get(1).rawKey == 'b'

        when: 'a migrate command line with an unknown option has no slots'
        def badMigrateData = migrateData.clone()
        badMigrateData[6] = 'foo'.bytes
        then:
        mGroup.parseSlots('migrate', badMigrateData, mGroup.slotNumber).size() == 0

        when: 'the copy option before the keys option does not hide the migrated keys'
        def copyMigrateData = new byte[10][]
        copyMigrateData[0] = 'migrate'.bytes
        copyMigrateData[1] = '127.0.0.1'.bytes
        copyMigrateData[2] = redisServer.port.toString().bytes
        copyMigrateData[3] = ''.bytes
        copyMigrateData[4] = '0'.bytes
        copyMigrateData[5] = '1000'.bytes
        copyMigrateData[6] = 'copy'.bytes
        copyMigrateData[7] = 'keys'.bytes
        copyMigrateData[8] = 'a'.bytes
        copyMigrateData[9] = 'b'.bytes
        then:
        mGroup.parseSlots('migrate', copyMigrateData, mGroup.slotNumber).size() == 2

        when: 'the replace option before the keys option does not hide the migrated keys'
        def replaceMigrateData = copyMigrateData.clone()
        replaceMigrateData[6] = 'replace'.bytes
        then:
        mGroup.parseSlots('migrate', replaceMigrateData, mGroup.slotNumber).size() == 2

        when: 'a migrate command line without the keys option has no slots'
        def noKeysData = new byte[7][]
        noKeysData[0] = 'migrate'.bytes
        noKeysData[1] = '127.0.0.1'.bytes
        noKeysData[2] = redisServer.port.toString().bytes
        noKeysData[3] = ''.bytes
        noKeysData[4] = '0'.bytes
        noKeysData[5] = '1000'.bytes
        noKeysData[6] = 'copy'.bytes
        then:
        mGroup.parseSlots('migrate', noKeysData, mGroup.slotNumber).size() == 0

        when: 'a migrate command line with too few arguments has no slots'
        def shortMigrateData = new byte[5][]
        shortMigrateData[0] = 'migrate'.bytes
        shortMigrateData[1] = '127.0.0.1'.bytes
        shortMigrateData[2] = redisServer.port.toString().bytes
        shortMigrateData[3] = ''.bytes
        shortMigrateData[4] = '0'.bytes
        then:
        mGroup.parseSlots('migrate', shortMigrateData, mGroup.slotNumber).size() == 0

        when:
        def reply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS a b')
        then:
        reply == MGroup.NOKEY_REPLY

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cv0 = cvList[0]
        def cv1 = cvList[1]
        cv0.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cv0.compressedData = 'value0'.bytes
        cv1.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cv1.compressedData = 'value1'.bytes
        inMemoryGetSet.put('a', cv0)
        inMemoryGetSet.put('b', cv1)
        reply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS a b')
        def jedis = redisServer.initJedis('127.0.0.1', redisServer.port)
        then:
        reply == OKReply.INSTANCE
        jedis.get('a') == 'value0'
        jedis.get('b') == 'value1'

        when:
        jedis.del('a')
        jedis.del('b')
        // the migrate without the copy option deleted the source keys, put them back
        inMemoryGetSet.put('a', cv0)
        inMemoryGetSet.put('b', cv1)
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        mGroup.crossRequestWorker = true
        reply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            reply == OKReply.INSTANCE
        }.result
        jedis.get('a') == 'value0'
        jedis.get('b') == 'value1'

        when: 'a key with a type that can not be dumped is skipped in the local path'
        def hllCv = Mock.prepareCompressedValueList(1)[0]
        hllCv.dictSeqOrSpType = CompressedValue.SP_TYPE_HLL
        inMemoryGetSet.put('hll', hllCv)
        mGroup.crossRequestWorker = false
        def hllReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS hll')
        then:
        hllReply == MGroup.NOKEY_REPLY

        when: 'a key with a type that can not be dumped is skipped in the cross worker path'
        mGroup.crossRequestWorker = true
        def hllReply2 = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS hll')
        eventloopCurrent.run()
        then:
        hllReply2 instanceof AsyncReply
        (hllReply2 as AsyncReply).settablePromise.toCompletableFuture().get(10, TimeUnit.SECONDS) == MGroup.NOKEY_REPLY

        when: 'migrate without the copy option must delete the source key'
        def cvC = Mock.prepareCompressedValueList(1)[0]
        cvC.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvC.compressedData = 'valuec'.bytes
        inMemoryGetSet.put('c', cvC)
        mGroup.crossRequestWorker = false
        def noCopyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS c')
        then:
        noCopyReply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'c', 0, 0L) == null

        when: 'migrate with the copy option must keep the source key'
        def cvD = Mock.prepareCompressedValueList(1)[0]
        cvD.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvD.compressedData = 'valued'.bytes
        inMemoryGetSet.put('d', cvD)
        def copyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 COPY KEYS d')
        then:
        copyReply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'd', 0, 0L) != null

        when: 'migrate with the copy and the replace options must keep the source key'
        def cvE = Mock.prepareCompressedValueList(1)[0]
        cvE.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvE.compressedData = 'valuee'.bytes
        inMemoryGetSet.put('e', cvE)
        def copyReplaceReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 COPY REPLACE KEYS e')
        then:
        copyReplaceReply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'e', 0, 0L) != null

        when: 'migrate without the copy option in the cross worker path must delete the source key'
        def cvF = Mock.prepareCompressedValueList(1)[0]
        cvF.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvF.compressedData = 'valuef'.bytes
        inMemoryGetSet.put('f', cvF)
        mGroup.crossRequestWorker = true
        def crossNoCopyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS f')
        eventloopCurrent.run()
        then:
        crossNoCopyReply instanceof AsyncReply
        (crossNoCopyReply as AsyncReply).settablePromise.toCompletableFuture().get(10, TimeUnit.SECONDS) == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'f', 0, 0L) == null

        when: 'migrate with the copy option in the cross worker path must keep the source key'
        def cvG = Mock.prepareCompressedValueList(1)[0]
        cvG.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvG.compressedData = 'valueg'.bytes
        inMemoryGetSet.put('g', cvG)
        def crossCopyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 COPY REPLACE KEYS g')
        eventloopCurrent.run()
        then:
        crossCopyReply instanceof AsyncReply
        (crossCopyReply as AsyncReply).settablePromise.toCompletableFuture().get(10, TimeUnit.SECONDS) == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'g', 0, 0L) != null

        when: 'migrate with the auth option to a password protected destination'
        def authRedisServer = new RedisServer('test-migrate-auth').noSave().randomPort().args('--requirepass', 'secret')
        Thread.start {
            authRedisServer.run()
        }
        def authJedis = initAuthJedis('127.0.0.1', authRedisServer.port, 'secret')
        def cvH = Mock.prepareCompressedValueList(1)[0]
        cvH.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvH.compressedData = 'valueh'.bytes
        inMemoryGetSet.put('h', cvH)
        mGroup.crossRequestWorker = false
        def authReply = mGroup.execute('migrate 127.0.0.1 ' + authRedisServer.port + ' "" 0 1000 AUTH secret KEYS h')
        then:
        authReply == OKReply.INSTANCE
        authJedis.get('h') == 'valueh'

        when: 'migrate with the auth2 option to a password protected destination'
        def cvI = Mock.prepareCompressedValueList(1)[0]
        cvI.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvI.compressedData = 'valuei'.bytes
        inMemoryGetSet.put('i', cvI)
        def auth2Reply = mGroup.execute('migrate 127.0.0.1 ' + authRedisServer.port + ' "" 0 1000 AUTH2 default secret KEYS i')
        then:
        auth2Reply == OKReply.INSTANCE
        authJedis.get('i') == 'valuei'

        when: 'the auth option without a password is a syntax error'
        def authNoPasswordReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 AUTH')
        then:
        authNoPasswordReply == ErrorReply.SYNTAX

        when: 'the auth2 option without a password is a syntax error'
        def auth2NoPasswordReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 AUTH2 default')
        then:
        auth2NoPasswordReply == ErrorReply.SYNTAX

        when: 'migrate the single key form, the key argument is the key to migrate'
        def cvJ = Mock.prepareCompressedValueList(1)[0]
        cvJ.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvJ.compressedData = 'valuej'.bytes
        inMemoryGetSet.put('j', cvJ)
        mGroup.crossRequestWorker = false
        def singleKeyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' j 0 1000')
        then:
        singleKeyReply == OKReply.INSTANCE
        jedis.get('j') == 'valuej'

        when: 'the single key form with an empty key argument is a syntax error'
        def emptyKeyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000')
        then:
        emptyKeyReply == ErrorReply.SYNTAX

        when: 'the key argument must be empty when the keys option is used'
        def notEmptyKeyReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' notempty 0 1000 KEYS a')
        then:
        notEmptyKeyReply instanceof ErrorReply
        notEmptyKeyReply.getMessage() == 'When using MIGRATE KEYS option, the key argument must be set to the empty string'

        when: 'the auth option without the keys option is a syntax error'
        def authNoKeysReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 AUTH secret')
        then:
        authNoKeysReply == ErrorReply.SYNTAX

        when: 'the keys option without any key is a syntax error'
        def noKeysReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 KEYS')
        then:
        noKeysReply == ErrorReply.SYNTAX

        when: 'an unknown option is a syntax error'
        def unknownOptionReply = mGroup.execute('migrate 127.0.0.1 ' + redisServer.port + ' "" 0 1000 foo KEYS a')
        then:
        unknownOptionReply == ErrorReply.SYNTAX

        when: 'the destination is not reachable'
        def unreachableReply = mGroup.execute('migrate 127.0.0.1 1 "" 0 1000 KEYS a')
        then:
        unreachableReply instanceof ErrorReply

        cleanup:
        jedis.close()
        redisServer.stop()
        authJedis?.close()
        authRedisServer.stop()
    }

    private static Jedis initAuthJedis(String host, int port, String password) {
        for (int i = 0; i < 20; i++) {
            try {
                def jedis = new Jedis(host, port)
                jedis.auth(password)
                jedis.ping()
                return jedis
            } catch (Exception ignored) {
                Thread.sleep(500)
            }
        }
        throw new RuntimeException('redis server with password is not ready')
    }
}
