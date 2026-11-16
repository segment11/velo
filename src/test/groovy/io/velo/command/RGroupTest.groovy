package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.*
import io.velo.mock.InMemoryGetSet
import io.velo.persist.*
import io.velo.rdb.RDBParser
import io.velo.repl.ReplPairTest
import io.velo.reply.*
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class RGroupTest extends Specification {
    final short slot = 0
    def _RGroup = new RGroup(null, null, null)

    /**
     * A slot worker eventloop running on its own thread.
     *
     * @param eventloop the worker eventloop
     * @param threadId  the id of the worker thread
     * @param thread    the worker thread
     */
    static record SlotWorker(Eventloop eventloop, long threadId, Thread thread) {
    }

    /**
     * A mock that fails every write of one slot, to drive the unexpected error path.
     */
    static class FailingPutGetSet extends InMemoryGetSet {
        /** The slot whose writes fail. */
        short failSlot = -1

        @Override
        void put(short slot, String key, int bucketIndex, CompressedValue cv) {
            if (slot == failSlot) {
                throw new RuntimeException("mock write failure")
            }
            super.put(slot, key, bucketIndex, cv)
        }
    }

    /**
     * Starts a slot worker eventloop on its own thread.
     *
     * @param name the worker thread name
     * @return the started slot worker
     */
    private static SlotWorker startWorkerThread(String name) {
        LinkedBlockingQueue<Eventloop> loopQ = new LinkedBlockingQueue<Eventloop>()
        Eventloop eventloop = Eventloop.builder().withIdleInterval(Duration.ofMillis(20)).build()
        eventloop.keepAlive(true)
        Thread thread = Thread.start({
            loopQ.put(eventloop)
            eventloop.run()
        })
        Eventloop loop = loopQ.take()
        CountDownLatch ready = new CountDownLatch(1)
        loop.execute({ ready.countDown() })
        ready.await(5, TimeUnit.SECONDS)
        return new SlotWorker(loop, loop.getEventloopThread().threadId(), thread)
    }

    def 'test parse slot - single key'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        int slotNumber = 128

        expect:
        _RGroup.parseSlots(cmd, data3, slotNumber).size() == expectedSize

        where:
        cmd          | expectedSize
        'rename'     | 2
        'renamenx'   | 2
        'rpoplpush'  | 2
        'restore'    | 0
        'rpop'       | 1
        'rpush'      | 1
        'rpushx'     | 1
        'rxxx'       | 0
    }

    def 'test parse slot - edge cases'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        def data1 = new byte[1][]
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        int slotNumber = 128

        expect:
        _RGroup.parseSlots('rename', data4, slotNumber).size() == 0
        _RGroup.parseSlots('rpoplpush', data4, slotNumber).size() == 0
        _RGroup.parseSlots('restore', data4, slotNumber).size() == 1
        _RGroup.parseSlots('rpop', data4, slotNumber).size() == 0
        _RGroup.parseSlots('rpush', data1, slotNumber).size() == 0
        _RGroup.parseSlots('rpushx', data1, slotNumber).size() == 0
        _RGroup.parseSlots('rpop', data2, slotNumber).size() == 1
    }

    def 'test handle - format errors'() {
        given:
        def rGroup = new RGroup(null, null, null)
        rGroup.from(BaseCommand.mockAGroup())

        expect:
        rGroup.execute(input) == expected

        where:
        input       | expected
        'rename'    | ErrorReply.FORMAT
        'renamenx'  | ErrorReply.FORMAT
        'randomkey a'| ErrorReply.FORMAT
        'replicaof' | ErrorReply.FORMAT
        'restore'   | ErrorReply.FORMAT
        'rpop'      | ErrorReply.FORMAT
        'rpoplpush' | ErrorReply.FORMAT
        'rpush'     | ErrorReply.FORMAT
        'rpushx'    | ErrorReply.FORMAT
        'zzz'       | NilReply.INSTANCE
    }

    def 'test handle - readonly readwrite role'() {
        given:
        def socket = SocketInspectorTest.mockTcpSocket()
        def rGroup = new RGroup('readonly', new byte[1][], socket)
        rGroup.from(BaseCommand.mockAGroup())

        expect:
        rGroup.handle() == OKReply.INSTANCE

        when:
        rGroup.cmd = 'readwrite'
        then:
        rGroup.handle() == OKReply.INSTANCE

        when:
        rGroup.cmd = 'role'
        then:
        rGroup.handle() instanceof MultiBulkReply
    }

    def 'test randomkey'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup(null, null, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        and:
        ConfForSlot.global = ConfForSlot.debugMode
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = rGroup.execute('randomkey')
        then:
        reply == NilReply.INSTANCE

        when:
        def oneSlot = localPersist.oneSlot(slot)
        def keyLoader = oneSlot.keyLoader
        for (i in 0..<ConfForSlot.global.confBucket.bucketsPerSlot) {
            def shortValueList = Mock.prepareShortValueList(10, i)
            def walGroupIndex = Wal.calcWalGroupIndex(i)
            keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, shortValueList)
        }
        println 'done mock keys'
        reply = rGroup.execute('randomkey')
        then:
        reply instanceof BulkReply

        when:
        def cvList = Mock.prepareCompressedValueList(10)
        for (walGroupIndex in 0..<Wal.calcWalGroupNumber()) {
            for (cv in cvList) {
                oneSlot.putKvInTargetWalGroupIndexLRU(walGroupIndex, 'key:' + cv.seq, cv.encode())
            }
        }
        reply = rGroup.execute('randomkey')
        then:
        reply instanceof BulkReply

        cleanup:
        ConfForSlot.global = ConfForSlot.c1m
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test rename'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup(null, null, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.execute('rename a b')
        then:
        reply == ErrorReply.NO_SUCH_KEY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.execute('rename a b')
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = rGroup.execute('renamenx a b')
        then:
        reply == IntegerReply.REPLY_0

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = rGroup.execute('renamenx a b')
        then:
        reply == IntegerReply.REPLY_1

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

        rGroup.crossRequestWorker = true
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.rename(false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = rGroup.execute('renamenx a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        reply = rGroup.execute('rename >key b')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = rGroup.execute('rename a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'bug 85.3: a cross worker rename whose destination write fails must answer the client'
        List<SlotWorker> workers = [startWorkerThread('velo-rgroup-rename-w1'),
                                    startWorkerThread('velo-rgroup-rename-w2')]
        Eventloop loop1 = workers[0].eventloop()
        Eventloop loop2 = workers[1].eventloop()
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        LocalPersist.instance.initSlotRuntimeBindings([loop1, loop2] as Eventloop[], new io.velo.RequestHandler[2],
                [workers[0].threadId(), workers[1].threadId()] as long[])

        and: 'a source key on worker 1 and a destination key on worker 2'
        def renameGroup = new RGroup(null, null, null)
        renameGroup.from(BaseCommand.mockAGroup(0 as byte, 2 as byte, 2 as short))
        renameGroup.crossRequestWorker = true
        def srcKey85 = (1..500).collect { 'src85_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def dstKey85 = (1..500).collect { 'dst85_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def seeded85 = new CountDownLatch(1)
        loop1.execute({
            renameGroup.set('v'.bytes, BaseCommand.slot(srcKey85, 2))
            seeded85.countDown()
        })
        seeded85.await(5, TimeUnit.SECONDS)

        and: 'the destination slot is readonly, so the destination write fails'
        LocalPersist.instance.oneSlot(1 as short).setReadonly(true)
        def asyncRename85 = []
        def done85 = new CountDownLatch(1)
        loop1.execute({
            asyncRename85 << renameGroup.execute("rename ${srcKey85} ${dstKey85}")
            done85.countDown()
        })
        done85.await(5, TimeUnit.SECONDS)
        def renamePromise85 = (asyncRename85[0] as AsyncReply).settablePromise
        for (int i = 0; i < 100 && !renamePromise85.isComplete(); i++) {
            Thread.sleep(20)
        }

        then: 'the reply promise is settled instead of hanging forever'
        renamePromise85.isComplete()
        renamePromise85.isResult()
        renamePromise85.getResult() == ErrorReply.READONLY

        when: 'bug 85.3: a cross worker rename whose destination write fails with an unexpected error answers the client too'
        def failMock85 = new FailingPutGetSet()
        failMock85.failSlot = 1 as short
        def failGroup85 = new RGroup(null, null, null)
        failGroup85.byPassGetSet = failMock85
        failGroup85.from(BaseCommand.mockAGroup(0 as byte, 2 as byte, 2 as short))
        failGroup85.crossRequestWorker = true
        failMock85.put(0 as short, srcKey85, BaseCommand.slot(srcKey85, 2).bucketIndex(),
                Mock.prepareCompressedValueList(1)[0])
        def asyncFail85 = []
        def doneFail85 = new CountDownLatch(1)
        loop1.execute({
            asyncFail85 << failGroup85.execute("rename ${srcKey85} ${dstKey85}")
            doneFail85.countDown()
        })
        doneFail85.await(5, TimeUnit.SECONDS)
        def failPromise85 = (asyncFail85[0] as AsyncReply).settablePromise
        for (int i = 0; i < 100 && !failPromise85.isComplete(); i++) {
            Thread.sleep(20)
        }

        then: 'the failure is reported to the client and the source key is kept'
        failPromise85.isComplete()
        failPromise85.isException()
        failPromise85.getException().getMessage() == 'mock write failure'
        failMock85.getBuf(0 as short, srcKey85, BaseCommand.slot(srcKey85, 2).bucketIndex(), 0L) != null

        cleanup:
        eventloop.breakEventloop()
        workers?.each { SlotWorker w -> w.thread().interrupt() }
    }

    def 'test restore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup(null, null, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        Debug.instance.logRestore = true
        def reply = rGroup.execute('restore a 0 bbb replace absttl idletime 0 freq 0')
        then:
        // Serialized value too short
        reply instanceof ErrorReply

        when:
        reply = rGroup.execute('restore string0 0 bbb replace') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'string0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().compressedData == 'values'.bytes

        when:
        reply = rGroup.execute('restore int0 0 bbb replace') { data ->
            data[3] = RDBParser.dumpString('1'.bytes)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'int0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isTypeNumber()
        bufOrCv.cv().numberValue() == 1

        when:
        reply = rGroup.execute('restore list0 0 bbb replace') { data ->
            def rl = new RedisList()
            rl.addLast('a'.bytes)
            rl.addLast('b'.bytes)
            rl.addLast('c'.bytes)
            data[3] = RDBParser.dumpList(rl)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'list0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isList()
        RedisList.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = rGroup.execute('restore hash0 0 bbb replace') { data ->
            def rhh = new RedisHH()
            rhh.put('a', 'a'.bytes)
            rhh.put('b', 'b'.bytes)
            rhh.put('c', 'c'.bytes)
            data[3] = RDBParser.dumpHash(rhh)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'hash0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isHash()
        RedisHH.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = rGroup.execute('restore hash1 0 bbb replace') { data ->
            def rhh = new RedisHH()
            rhh.put('a', 'a'.bytes)
            rhh.put('b', 'b'.bytes)
            rhh.put('c', 'c'.bytes)
            data[3] = RDBParser.dumpHash(rhh)
        }
        var keysKey = RedisHashKeys.keysKey('hash1')
        bufOrCv = inMemoryGetSet.getBuf(slot, keysKey, 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isHash()
        RedisHashKeys.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        reply = rGroup.execute('restore set0 0 bbb replace') { data ->
            def rhk = new RedisHashKeys()
            rhk.add('a')
            rhk.add('b')
            rhk.add('c')
            data[3] = RDBParser.dumpSet(rhk)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'set0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isSet()
        RedisHashKeys.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        reply = rGroup.execute('restore zset0 0 bbb replace') { data ->
            def rz = new RedisZSet()
            rz.add(1.0d, 'a')
            rz.add(2.0d, 'b')
            rz.add(3.0d, 'c')
            data[3] = RDBParser.dumpZSet(rz)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'zset0', 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isZSet()
        RedisZSet.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        Debug.instance.logRestore = false
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.execute('restore a 1000 bbb')
        then:
        reply == ErrorReply.TARGET_KEY_BUSY

        when:
        reply = rGroup.execute('restore a 1000 bbb absttl')
        then:
        reply == ErrorReply.TARGET_KEY_BUSY

        when:
        reply = rGroup.execute('restore a 0 bbb replace absttl idletime 0 freq -1')
        then:
        reply instanceof ErrorReply

        when:
        reply = rGroup.execute('restore a 0 bbb replace absttl idletime -1 freq 0')
        then:
        reply instanceof ErrorReply

        when:
        reply = rGroup.execute('restore a 0 bbb replace absttl idletime')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = rGroup.execute('restore a 0 bbb replace absttl freq')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test role'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def rGroup = new RGroup(null, null, null)
        rGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = rGroup.role()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == new BulkReply('master')
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_0
        (reply as MultiBulkReply).replies[2] == MultiBulkReply.EMPTY

        when:
        def replPairAsMaster = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster)
        reply = rGroup.role()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == new BulkReply('master')
        (reply as MultiBulkReply).replies[2] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[2]).replies[0] instanceof MultiBulkReply

        when: 'slave with no live TCP client (default mock has empty linkUpFlagArray, so isLinkUpAnyOk returns false)'
        oneSlot.replPairs.clear()
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, oneSlot.masterUuid)
        oneSlot.replPairs.add(replPairAsSlave)
        reply = rGroup.role()
        then: 'link state must reflect actual TCP state, not hard-coded "connected"'
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5
        (reply as MultiBulkReply).replies[0] == new BulkReply('slave')
        (reply as MultiBulkReply).replies[3] == new BulkReply('connecting')

        when: 'link-up flag set via the test hook'
        replPairAsSlave.addLinkUpFlag(true)
        reply = rGroup.role()
        then: 'with a real link-up flag, state is "connected"'
        (reply as MultiBulkReply).replies[3] == new BulkReply('connected')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test rpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup(null, null, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = rGroup.execute('rpop a')
        then:
        reply == NilReply.INSTANCE
    }

    def 'test rpoplpush'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup(null, null, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.execute('rpoplpush a b')
        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlA = new RedisList()
        10.times {
            rlA.addLast(it.toString().bytes)
        }
        cvA.compressedData = rlA.encode()
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlB = new RedisList()
        10.times {
            rlB.addLast(it.toString().bytes)
        }
        cvB.compressedData = rlB.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = rGroup.execute('rpoplpush a b')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == '9'.bytes

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
        rGroup.crossRequestWorker = true
        reply = rGroup.rpoplpush()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof BulkReply && (result as BulkReply).raw == '8'.bytes
        }.result

        when:
        reply = rGroup.execute('rpoplpush >key b')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = rGroup.execute('rpoplpush a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        // bug 4 regression: wrong-type dest should not lose source element
        inMemoryGetSet.remove(slot, 'src')
        inMemoryGetSet.remove(slot, 'dst')
        def cvSrc = Mock.prepareCompressedValueList(1)[0]
        cvSrc.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlSrc = new RedisList()
        rlSrc.addFirst('a'.bytes)
        cvSrc.compressedData = rlSrc.encode()
        inMemoryGetSet.put(slot, 'src', 0, cvSrc)
        // dst is a string, not a list
        def cvDst = new CompressedValue()
        cvDst.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvDst.compressedData = 'not-list'.bytes
        inMemoryGetSet.put(slot, 'dst', 0, cvDst)
        rGroup.crossRequestWorker = false
        reply = rGroup.execute('rpoplpush src dst')
        then:
        reply == ErrorReply.WRONG_TYPE
        // source must still contain [a]
        def rlSrcAfter = LGroup.getRedisList(rGroup.slotWithKeyHashListParsed.getFirst(), rGroup)
        rlSrcAfter != null
        rlSrcAfter.size() == 1

        when:
        // bug 4 regression: moving last element should delete source key
        inMemoryGetSet.remove(slot, 'src2')
        inMemoryGetSet.remove(slot, 'dst2')
        def cvSrc2 = Mock.prepareCompressedValueList(1)[0]
        cvSrc2.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlSrc2 = new RedisList()
        rlSrc2.addFirst('x'.bytes)
        cvSrc2.compressedData = rlSrc2.encode()
        inMemoryGetSet.put(slot, 'src2', 0, cvSrc2)
        rGroup.execute('rpoplpush src2 dst2')
        then:
        // source key should be removed (empty list deleted)
        rGroup.getCv(rGroup.slotWithKeyHashListParsed.getFirst()) == null

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test rpush and rpushx'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rpush', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = _RGroup.parseSlots('rpush', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = rGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        inMemoryGetSet.remove(slot, 'a')
        rGroup.cmd = 'rpushx'
        reply = rGroup.handle()
        then:
        reply == IntegerReply.REPLY_0
    }

    def 'test move block'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def s1 = BaseCommand.slot('a', 1)
        def s2 = BaseCommand.slot('b', 1)
        def reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 0)
        then:
        // timeout 0 = block indefinitely
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 1)
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        def rl = new RedisList()
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 0)
        then:
        // timeout 0 = block indefinitely (empty list)
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 1)
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        rl.addFirst('1'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 0)
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == '1'.bytes

        when:
        // bug 4 regression: moveBlock wrong-type dest should not lose source element
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'dst-wrong')
        def cvSrcBlock = Mock.prepareCompressedValueList(1)[0]
        cvSrcBlock.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlSrcBlock = new RedisList()
        rlSrcBlock.addFirst('z'.bytes)
        cvSrcBlock.compressedData = rlSrcBlock.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvSrcBlock)
        def cvDstWrong = new CompressedValue()
        cvDstWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvDstWrong.compressedData = 'not-list'.bytes
        def sDstWrong = BaseCommand.slot('dst-wrong', 1)
        inMemoryGetSet.put(slot, 'dst-wrong', 0, cvDstWrong)
        rGroup.crossRequestWorker = false
        reply = rGroup.moveBlock(s1, sDstWrong, true, true, 1.0)
        then:
        reply == ErrorReply.WRONG_TYPE
        def rlSrcBlockAfter = LGroup.getRedisList(s1, rGroup)
        rlSrcBlockAfter != null
        rlSrcBlockAfter.size() == 1

        when: 'a move timeout that fires after a delivery cannot nil the reply'
        // drop waiters still pending from the earlier indefinite cases, so the push reaches this one
        BlockingList.clearBlockingListPromisesForAllKeys()
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def asyncMoveTimeout = rGroup.moveBlock(s1, s2, true, true, 0.2) as AsyncReply
        def lpushData = new byte[3][]
        lpushData[0] = 'lpush'.bytes
        lpushData[1] = 'a'.bytes
        lpushData[2] = 'moved'.bytes
        def lGroup = new LGroup('lpush', lpushData, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(rGroup)
        lGroup.slotWithKeyHashListParsed = lGroup.parseSlots('lpush', lpushData, 1)
        def pushReply = lGroup.handle()
        then:
        pushReply == IntegerReply.REPLY_1
        asyncMoveTimeout.settablePromise.isComplete()
        asyncMoveTimeout.settablePromise.getResult() instanceof BulkReply
        (asyncMoveTimeout.settablePromise.getResult() as BulkReply).raw == 'moved'.bytes
        when:
        Thread.sleep(400)
        eventloopCurrent.run()
        then:
        asyncMoveTimeout.settablePromise.getResult() instanceof BulkReply
        (asyncMoveTimeout.settablePromise.getResult() as BulkReply).raw == 'moved'.bytes

        when:
        localPersist.cleanUp()
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        localPersist.fixSlotThreadId(s1.slot(), Thread.currentThread().threadId())
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        Thread.sleep(100)
        localPersist.fixSlotThreadId(s2.slot(), eventloop.eventloopThread.threadId())
        localPersist.oneSlot(s2.slot()).slotWorkerEventloop = eventloop
        rl.addFirst('1'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        rGroup.crossRequestWorker = true
        reply = rGroup.moveBlock(
                s1,
                s2,
                true, true, 0)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof BulkReply && (result as BulkReply).raw == '1'.bytes
        }.result

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
    }
}
