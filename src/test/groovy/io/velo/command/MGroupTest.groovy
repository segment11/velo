package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.test.tools.RedisServer
import spock.lang.Specification

import java.time.Duration

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

        cleanup:
        jedis.close()
        redisServer.stop()
    }
}
