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
import java.util.concurrent.atomic.AtomicReference

class RGroupTest extends Specification {
    final short slot = 0
    def _RGroup = new RGroup(null, null, null)

    /**
     * A slot worker eventloop running on its own thread.
     */
    static class SlotWorker {
        private final Eventloop eventloop
        private final long threadId
        private final Thread thread

        SlotWorker(Eventloop eventloop, long threadId, Thread thread) {
            this.eventloop = eventloop
            this.threadId = threadId
            this.thread = thread
        }

        /**
         * @return the worker eventloop
         */
        Eventloop getEventloop() {
            return eventloop
        }

        /**
         * @return the id of the worker thread
         */
        long getThreadId() {
            return threadId
        }

        /**
         * @return the worker thread
         */
        Thread getThread() {
            return thread
        }
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
     * Waits until the asynchronous reply is settled and returns its result.
     *
     * @param asyncReply   the asynchronous reply
     * @param timeoutMillis the maximum wait time in milliseconds
     * @return the settled reply result
     */
    private static Reply awaitReply(AsyncReply asyncReply, long timeoutMillis = 5000) {
        long deadline = System.currentTimeMillis() + timeoutMillis
        while (!asyncReply.settablePromise.isComplete() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20)
        }
        return asyncReply.settablePromise.getResult()
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

        when: 'bug 85.12: an unknown command is answered like redis does'
        def unknownReply = rGroup.execute('rfoo')
        then:
        unknownReply instanceof ErrorReply
        (unknownReply as ErrorReply).getMessage() == "unknown command 'rfoo'"

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

        when: 'bug 85.8: a key of another slot is found'
        LocalPersistTest.prepareLocalPersist(1 as byte, 2 as short)
        localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        def otherSlotCvList = Mock.prepareCompressedValueList(10)
        for (walGroupIndex in 0..<Wal.calcWalGroupNumber()) {
            for (cv in otherSlotCvList) {
                localPersist.oneSlot((short) 1)
                        .putKvInTargetWalGroupIndexLRU(walGroupIndex, 'key:' + cv.seq, cv.encode())
            }
        }
        then: 'the key of the other slot is returned'
        rGroup.execute('randomkey') instanceof BulkReply

        when: 'bug 85.9: an expired key is not returned'
        def expiredKey = 'expired:key'
        def expiredKeyHash = KeyHash.hash(expiredKey.bytes)
        def expireAtSoon = System.currentTimeMillis() + 300
        def expiredList = Mock.prepareShortValueList(1, 0, { v ->
            new Wal.V(v.seq(), v.bucketIndex(), expiredKeyHash, expireAtSoon,
                    v.spType(), expiredKey, v.cvEncoded(), v.isFromMerge())
        })
        localPersist.oneSlot(slot).keyLoader
                .persistShortValueListBatchInOneWalGroup(Wal.calcWalGroupIndex(0), expiredList)
        and: 'the key expires'
        Thread.sleep(500)
        then: 'the expired key of the bucket is not returned'
        rGroup.randomKeyInBucket(localPersist.oneSlot(slot), 0, new Random()) == null

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

        when: 'bug 85.9: the random index inside the bucket is honoured'
        def distinctKeys = [] as Set
        100.times {
            def randomReply = rGroup.execute('randomkey')
            distinctKeys << new String((randomReply as BulkReply).raw)
        }
        then: 'more than one key of the bucket is returned'
        distinctKeys.size() > 1

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

        when: 'bug 85.8: a key of another slot worker is found'
        List<SlotWorker> randomKeyWorkers = [startWorkerThread('velo-rgroup-randomkey-w1'),
                                              startWorkerThread('velo-rgroup-randomkey-w2')]
        Eventloop randomKeyLoop1 = randomKeyWorkers[0].eventloop
        Eventloop randomKeyLoop2 = randomKeyWorkers[1].eventloop
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        LocalPersist.instance.initSlotRuntimeBindings([randomKeyLoop1, randomKeyLoop2] as Eventloop[],
                new io.velo.RequestHandler[2],
                [randomKeyWorkers[0].threadId, randomKeyWorkers[1].threadId] as long[])

        and: 'only the slot of the second worker holds keys'
        def otherWorkerCvList = Mock.prepareCompressedValueList(10)
        for (walGroupIndex in 0..<Wal.calcWalGroupNumber()) {
            for (cv in otherWorkerCvList) {
                localPersist.oneSlot((short) 1)
                        .putKvInTargetWalGroupIndexLRU(walGroupIndex, 'key:' + cv.seq, cv.encode())
            }
        }

        and: 'the command runs on the first worker'
        def asyncRandomKey = []
        def doneRandomKey = new CountDownLatch(1)
        randomKeyLoop1.execute({
            asyncRandomKey << rGroup.execute('randomkey')
            doneRandomKey.countDown()
        })
        doneRandomKey.await(5, TimeUnit.SECONDS)
        def randomKeyReply = asyncRandomKey[0]

        then: 'the key of the other worker is returned'
        randomKeyReply instanceof AsyncReply
        awaitReply(randomKeyReply as AsyncReply) instanceof BulkReply

        cleanup:
        ConfForSlot.global = ConfForSlot.c1m
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        randomKeyWorkers?.each { SlotWorker w -> w.thread.interrupt() }
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
        awaitReply(reply as AsyncReply) == OKReply.INSTANCE

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = rGroup.execute('renamenx a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        awaitReply(reply as AsyncReply) == IntegerReply.REPLY_0

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
        Eventloop loop1 = workers[0].eventloop
        Eventloop loop2 = workers[1].eventloop
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        LocalPersist.instance.initSlotRuntimeBindings([loop1, loop2] as Eventloop[], new RequestHandler[2],
                [workers[0].threadId, workers[1].threadId] as long[])

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
        workers?.each { SlotWorker w -> w.thread.interrupt() }
        // the readonly flag is kept in the slot, reset it for the other test classes
        LocalPersist.instance.oneSlot(1 as short).setReadonly(false)
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

        when: 'bug 85.7: an unknown option is a syntax error'
        reply = rGroup.execute('restore unknown_opt 0 bbb badoption') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.SYNTAX

        when: 'bug 85.7: a negative ttl is rejected'
        reply = rGroup.execute('restore negative_ttl -1 bbb') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply instanceof ErrorReply
        reply != OKReply.INSTANCE

        when: 'bug 85.7: idletime and freq together are a syntax error'
        reply = rGroup.execute('restore both_opt 0 bbb replace idletime 5 freq 3') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.SYNTAX

        when: 'bug 85.7: freq and idletime together are a syntax error'
        reply = rGroup.execute('restore both_opt2 0 bbb replace freq 3 idletime 5') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.SYNTAX

        when: 'bug 85.6: a non numeric ttl is rejected'
        reply = rGroup.execute('restore ttl_not_int abc bbb') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE

        when: 'bug 85.6: a non numeric idletime is rejected'
        reply = rGroup.execute('restore ttl_not_int 0 bbb replace idletime abc') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE

        when: 'bug 85.6: a non numeric freq is rejected'
        reply = rGroup.execute('restore ttl_not_int 0 bbb replace freq abc') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE

        when: 'bug 85.6: the busy key check comes before the ttl parse'
        inMemoryGetSet.remove(slot, 'busy_ttl')
        inMemoryGetSet.put(slot, 'busy_ttl', 0, Mock.prepareCompressedValueList(1)[0])
        reply = rGroup.execute('restore busy_ttl abc bbb') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.TARGET_KEY_BUSY

        when: 'bug 85.5: a too long key is rejected'
        reply = rGroup.execute('restore >key 0 bbb') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'bug 85.6: an absolute ttl is used as it is'
        def absttlAt = System.currentTimeMillis() + 100000
        reply = rGroup.execute("restore absttl_key ${absttlAt} bbb absttl") { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        then:
        reply == OKReply.INSTANCE
        rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst()) == absttlAt

        when: 'bug 85.4: a restored list keeps the ttl'
        reply = rGroup.execute('restore list_ttl 100000 bbb') { data ->
            def rlTtl = new RedisList()
            rlTtl.addLast('a'.bytes)
            data[3] = RDBParser.dumpList(rlTtl)
        }
        then:
        reply == OKReply.INSTANCE
        def listExpireAt = rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst())
        listExpireAt != null
        listExpireAt > System.currentTimeMillis() + 50000
        listExpireAt <= System.currentTimeMillis() + 100000

        when: 'bug 85.4: a restored set keeps the ttl'
        reply = rGroup.execute('restore set_ttl 100000 bbb') { data ->
            def rhkTtl = new RedisHashKeys()
            rhkTtl.add('a')
            data[3] = RDBParser.dumpSet(rhkTtl)
        }
        then:
        reply == OKReply.INSTANCE
        def setExpireAt = rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst())
        setExpireAt != null
        setExpireAt > System.currentTimeMillis() + 50000

        when: 'bug 85.4: a restored zset keeps the ttl'
        reply = rGroup.execute('restore zset_ttl 100000 bbb') { data ->
            def rzTtl = new RedisZSet()
            rzTtl.add(1.0d, 'a')
            data[3] = RDBParser.dumpZSet(rzTtl)
        }
        then:
        reply == OKReply.INSTANCE
        def zsetExpireAt = rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst())
        zsetExpireAt != null
        zsetExpireAt > System.currentTimeMillis() + 50000

        when: 'bug 85.4: a restored hash keeps the ttl'
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = rGroup.execute('restore hash_ttl 100000 bbb') { data ->
            def rhhTtl = new RedisHH()
            rhhTtl.put('a', 'a'.bytes)
            data[3] = RDBParser.dumpHash(rhhTtl)
        }
        then:
        reply == OKReply.INSTANCE
        def hashExpireAt = rGroup.getExpireAt(rGroup.slotWithKeyHashListParsed.getFirst())
        hashExpireAt != null
        hashExpireAt > System.currentTimeMillis() + 50000

        cleanup:
        LocalPersist.instance.hashSaveMemberTogether = false
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
        awaitReply(reply as AsyncReply) instanceof BulkReply &&
                (awaitReply(reply as AsyncReply) as BulkReply).raw == '8'.bytes

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

        when: 'the source list is empty'
        def emptyGroup = new RGroup(null, null, null)
        emptyGroup.byPassGetSet = inMemoryGetSet
        emptyGroup.from(BaseCommand.mockAGroup())
        inMemoryGetSet.remove(slot, 'empty-src')
        inMemoryGetSet.remove(slot, 'empty-dst')
        def cvEmpty = Mock.prepareCompressedValueList(1)[0]
        cvEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvEmpty.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'empty-src', 0, cvEmpty)
        def emptyReply = emptyGroup.execute('rpoplpush empty-src empty-dst')
        then:
        emptyReply == NilReply.INSTANCE

        when: 'the source key holds a value of another type'
        inMemoryGetSet.remove(slot, 'not-a-list-src')
        inMemoryGetSet.remove(slot, 'not-a-list-dst')
        inMemoryGetSet.put(slot, 'not-a-list-src', 0, Mock.prepareCompressedValueList(1)[0])
        def wrongSrcReply = emptyGroup.execute('rpoplpush not-a-list-src not-a-list-dst')
        then:
        wrongSrcReply == ErrorReply.WRONG_TYPE

        when: 'a move onto the same key moves the element to the head of the same list'
        def sameGroup = new RGroup(null, null, null)
        sameGroup.byPassGetSet = inMemoryGetSet
        sameGroup.from(BaseCommand.mockAGroup())
        inMemoryGetSet.remove(slot, 'same')
        def rlSame = new RedisList()
        rlSame.addLast('1'.bytes)
        rlSame.addLast('2'.bytes)
        def cvSame = Mock.prepareCompressedValueList(1)[0]
        cvSame.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvSame.compressedData = rlSame.encode()
        inMemoryGetSet.put(slot, 'same', 0, cvSame)
        def sameReply = sameGroup.execute('rpoplpush same same')
        then:
        sameReply instanceof BulkReply
        (sameReply as BulkReply).raw == '2'.bytes
        def rlSameAfter = LGroup.getRedisList(sameGroup.slotWithKeyHashListParsed.getFirst(), sameGroup)
        rlSameAfter.size() == 2
        rlSameAfter.get(0) == '2'.bytes

        when: 'bug 85.1: a cross worker rpoplpush moves the element to the destination slot'
        List<SlotWorker> moveWorkers = [startWorkerThread('velo-rgroup-move-w1'),
                                        startWorkerThread('velo-rgroup-move-w2')]
        Eventloop moveLoop1 = moveWorkers[0].eventloop
        Eventloop moveLoop2 = moveWorkers[1].eventloop
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        LocalPersist.instance.initSlotRuntimeBindings([moveLoop1, moveLoop2] as Eventloop[], new io.velo.RequestHandler[2],
                [moveWorkers[0].threadId, moveWorkers[1].threadId] as long[])

        and: 'a source list on worker 1 and a destination key on worker 2'
        def moveGroup = new RGroup(null, null, null)
        moveGroup.from(BaseCommand.mockAGroup(0 as byte, 2 as byte, 2 as short))
        moveGroup.crossRequestWorker = true
        def srcKeyMove = (1..500).collect { 'srcmove_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def dstKeyMove = (1..500).collect { 'dstmove_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def srcSwkhMove = BaseCommand.slot(srcKeyMove, 2)
        def dstSwkhMove = BaseCommand.slot(dstKeyMove, 2)
        def rlMove = new RedisList()
        rlMove.addLast('a'.bytes)
        rlMove.addLast('b'.bytes)
        def seededMove = new CountDownLatch(1)
        moveLoop1.execute({
            moveGroup.set(rlMove.encode(), srcSwkhMove, CompressedValue.SP_TYPE_LIST)
            seededMove.countDown()
        })
        seededMove.await(5, TimeUnit.SECONDS)

        and: 'the move runs on the source worker'
        def asyncMove = []
        def doneMove = new CountDownLatch(1)
        moveLoop1.execute({
            asyncMove << moveGroup.execute("rpoplpush ${srcKeyMove} ${dstKeyMove}")
            doneMove.countDown()
        })
        doneMove.await(5, TimeUnit.SECONDS)
        def movePromise = asyncMove[0] instanceof AsyncReply ? (asyncMove[0] as AsyncReply).settablePromise : null
        if (movePromise != null) {
            for (int i = 0; i < 100 && !movePromise.isComplete(); i++) {
                Thread.sleep(20)
            }
        }

        then: 'the cross worker move does not throw'
        asyncMove[0] != null
        movePromise != null

        and: 'the element is moved and answered to the client'
        movePromise.isComplete()
        movePromise.getResult() instanceof BulkReply
        (movePromise.getResult() as BulkReply).raw == 'b'.bytes

        and: 'the destination list on worker 2 holds the moved element'
        def dstList = new AtomicReference<Object>()
        def readDst = new CountDownLatch(1)
        moveLoop2.execute({
            dstList.set(LGroup.getRedisList(dstSwkhMove, moveGroup))
            readDst.countDown()
        })
        readDst.await(5, TimeUnit.SECONDS)
        dstList.get() != null
        (dstList.get() as RedisList).size() == 1
        (dstList.get() as RedisList).get(0) == 'b'.bytes

        and: 'the source list on worker 1 keeps the element that was not moved'
        def srcList = new AtomicReference<Object>()
        def readSrc = new CountDownLatch(1)
        moveLoop1.execute({
            srcList.set(LGroup.getRedisList(srcSwkhMove, moveGroup))
            readSrc.countDown()
        })
        readSrc.await(5, TimeUnit.SECONDS)
        srcList.get() != null
        (srcList.get() as RedisList).size() == 1
        (srcList.get() as RedisList).get(0) == 'a'.bytes

        when: 'bug 85.1: a cross worker rpoplpush with a wrong type destination answers WRONGTYPE and keeps the source'
        def wrongTypeKey = (1..500).collect { 'wrongtype_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def wrongTypeSwkh = BaseCommand.slot(wrongTypeKey, 2)
        def seededWrongType = new CountDownLatch(1)
        moveLoop2.execute({
            moveGroup.set('not-a-list'.bytes, wrongTypeSwkh)
            seededWrongType.countDown()
        })
        seededWrongType.await(5, TimeUnit.SECONDS)
        def asyncWrongType = []
        def doneWrongType = new CountDownLatch(1)
        moveLoop1.execute({
            asyncWrongType << moveGroup.execute("rpoplpush ${srcKeyMove} ${wrongTypeKey}")
            doneWrongType.countDown()
        })
        doneWrongType.await(5, TimeUnit.SECONDS)
        def wrongTypePromise = (asyncWrongType[0] as AsyncReply).settablePromise
        for (int i = 0; i < 100 && !wrongTypePromise.isComplete(); i++) {
            Thread.sleep(20)
        }

        then: 'the destination type is checked on the destination worker'
        wrongTypePromise.isComplete()
        wrongTypePromise.getResult() == ErrorReply.WRONG_TYPE

        and: 'the source list is not modified by the wrong type destination'
        def srcListAfterWrongType = new AtomicReference<Object>()
        def readSrcAfterWrongType = new CountDownLatch(1)
        moveLoop1.execute({
            srcListAfterWrongType.set(LGroup.getRedisList(srcSwkhMove, moveGroup))
            readSrcAfterWrongType.countDown()
        })
        readSrcAfterWrongType.await(5, TimeUnit.SECONDS)
        (srcListAfterWrongType.get() as RedisList).size() == 1
        (srcListAfterWrongType.get() as RedisList).get(0) == 'a'.bytes

        when: 'bug 85.1: a cross worker blmove moves the element too'
        def blmoveSrcKey = (1..500).collect { 'blmovesrc_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def blmoveDstKey = (1..500).collect { 'blmovedst_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def blmoveSrcSwkh = BaseCommand.slot(blmoveSrcKey, 2)
        def blmoveDstSwkh = BaseCommand.slot(blmoveDstKey, 2)
        def rlBlmove = new RedisList()
        rlBlmove.addLast('x'.bytes)
        def seededBlmove = new CountDownLatch(1)
        moveLoop1.execute({
            moveGroup.set(rlBlmove.encode(), blmoveSrcSwkh, CompressedValue.SP_TYPE_LIST)
            seededBlmove.countDown()
        })
        seededBlmove.await(5, TimeUnit.SECONDS)
        def asyncBlmove = []
        def doneBlmove = new CountDownLatch(1)
        moveLoop1.execute({
            asyncBlmove << moveGroup.moveBlock(blmoveSrcSwkh, blmoveDstSwkh, false, true, 0)
            doneBlmove.countDown()
        })
        doneBlmove.await(5, TimeUnit.SECONDS)
        def blmovePromise = (asyncBlmove[0] as AsyncReply).settablePromise
        for (int i = 0; i < 100 && !blmovePromise.isComplete(); i++) {
            Thread.sleep(20)
        }

        then: 'the element is moved and answered to the client'
        blmovePromise.isComplete()
        blmovePromise.getResult() instanceof BulkReply
        (blmovePromise.getResult() as BulkReply).raw == 'x'.bytes

        when: 'bug 85.14: a cross worker rpoplpush whose destination write fails answers the client'
        def failSrcKey = (1..500).collect { 'failsrc_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def failDstKey = (1..500).collect { 'faildst_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def failSrcSwkh = BaseCommand.slot(failSrcKey, 2)
        def rlFail = new RedisList()
        rlFail.addLast('f'.bytes)
        def seededFail = new CountDownLatch(1)
        moveLoop1.execute({
            moveGroup.set(rlFail.encode(), failSrcSwkh, CompressedValue.SP_TYPE_LIST)
            seededFail.countDown()
        })
        seededFail.await(5, TimeUnit.SECONDS)

        and: 'the destination slot is readonly, so the destination write fails'
        LocalPersist.instance.oneSlot(1 as short).setReadonly(true)
        def asyncFailMove = []
        def doneFailMove = new CountDownLatch(1)
        moveLoop1.execute({
            asyncFailMove << moveGroup.execute("rpoplpush ${failSrcKey} ${failDstKey}")
            doneFailMove.countDown()
        })
        doneFailMove.await(5, TimeUnit.SECONDS)
        def failMovePromise = asyncFailMove[0] instanceof AsyncReply ? (asyncFailMove[0] as AsyncReply).settablePromise : null
        if (failMovePromise != null) {
            for (int i = 0; i < 100 && !failMovePromise.isComplete(); i++) {
                Thread.sleep(20)
            }
        }

        then: 'the client gets an answer instead of hanging forever'
        failMovePromise != null
        failMovePromise.isComplete()
        failMovePromise.isResult()
        failMovePromise.getResult() == ErrorReply.READONLY

        cleanup:
        eventloop.breakEventloop()
        moveWorkers?.each { SlotWorker w -> w.thread.interrupt() }
        // the readonly flag is kept in the slot, reset it for the other test classes
        LocalPersist.instance.oneSlot(1 as short).setReadonly(false)
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
        awaitReply(reply as AsyncReply) == NilReply.INSTANCE

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
        awaitReply(reply as AsyncReply) == NilReply.INSTANCE

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
        awaitReply(reply as AsyncReply) instanceof BulkReply &&
                (awaitReply(reply as AsyncReply) as BulkReply).raw == '1'.bytes

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
    }
}
