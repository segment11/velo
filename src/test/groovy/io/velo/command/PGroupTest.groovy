package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.acl.AclUsers
import io.velo.acl.RPubSub
import io.velo.mock.ByPassGetSet
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.persist.OneSlot
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class PGroupTest extends Specification {
    def _PGroup = new PGroup(null, null, null)

    /**
     * A by pass get set that records the thread which wrote each key, to check the slot thread affinity.
     */
    static class RecordingGetSet implements ByPassGetSet {
        final Map<String, CompressedValue> map = new ConcurrentHashMap<>()
        final Map<String, Long> putThreadId = new ConcurrentHashMap<>()
        final Map<String, AtomicInteger> readCount = new ConcurrentHashMap<>()

        @Override
        void put(short slot, String key, int bucketIndex, CompressedValue cv) {
            map.put(key, cv)
            putThreadId.put(key, Thread.currentThread().threadId())
        }

        @Override
        boolean remove(short slot, String key) {
            return map.remove(key) != null
        }

        @Override
        OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash) {
            readCount.computeIfAbsent(key) { new AtomicInteger() }.incrementAndGet()
            var cv = map.get(key)
            if (cv == null) {
                return null
            }
            return new OneSlot.BufOrCompressedValue(null, cv)
        }
    }

    /**
     * Waits until the async reply is settled.
     */
    static Object waitForReply(AsyncReply reply, long timeoutMs = 5000) {
        def deadline = System.currentTimeMillis() + timeoutMs
        while (!reply.settablePromise.isComplete() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5)
        }
        return reply.settablePromise.getResult()
    }

    static SocketInspector initSocketInspector() {
        def inspector = new SocketInspector()
        def eventloop = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloop]
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        return inspector
    }

    final short slot = 0

    def 'test parse slot - single key'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        int slotNumber = 128

        expect:
        _PGroup.parseSlots('pexpire', data4, slotNumber).size() == 1
        _PGroup.parseSlots('pexpireat', data4, slotNumber).size() == 1
        _PGroup.parseSlots('pexpiretime', data2, slotNumber).size() == 1
        _PGroup.parseSlots('pfadd', data2, slotNumber).size() == 1
        _PGroup.parseSlots('pfcount', data2, slotNumber).size() == 1
        _PGroup.parseSlots('pfmerge', data2, slotNumber).size() == 1
        _PGroup.parseSlots('pttl', data2, slotNumber).size() == 1
        _PGroup.parseSlots('persist', data2, slotNumber).size() == 1
        _PGroup.parseSlots('psetex', data4, slotNumber).size() == 1
        _PGroup.parseSlots('pxxx', data2, slotNumber).size() == 0
    }

    def 'test parse slot - edge cases'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        def data1 = new byte[1][]
        int slotNumber = 128

        expect:
        _PGroup.parseSlots('pexpireat', data3, slotNumber).size() == 1
        _PGroup.parseSlots('pfadd', data3, slotNumber).size() == 1
        _PGroup.parseSlots('pfmerge', data3, slotNumber).size() == 2

        // wrong sizes
        _PGroup.parseSlots('pexpire', data2, slotNumber).size() == 0
        _PGroup.parseSlots('pexpireat', data2, slotNumber).size() == 0
        _PGroup.parseSlots('pexpiretime', data4, slotNumber).size() == 0
        _PGroup.parseSlots('pfcount', data1, slotNumber).size() == 0
        _PGroup.parseSlots('pttl', data4, slotNumber).size() == 0
        _PGroup.parseSlots('persist', data4, slotNumber).size() == 0
        _PGroup.parseSlots('psetex', data2, slotNumber).size() == 0
    }

    def 'test handle - format errors'() {
        given:
        def pGroup = new PGroup(null, null, null)
        pGroup.from(BaseCommand.mockAGroup())

        expect:
        pGroup.execute(input) == expected

        where:
        input    | expected
        'persist'| ErrorReply.FORMAT
        'pfadd'  | ErrorReply.FORMAT
        'pfcount'| ErrorReply.FORMAT
        'pfmerge'| ErrorReply.FORMAT
        'psetex' | ErrorReply.FORMAT
        'publish'| ErrorReply.FORMAT
        'pubsub' | ErrorReply.FORMAT
        'pxxx'   | ErrorReply.UNKNOWN_COMMAND('pxxx')
    }

    def 'test handle - functional'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '60000'.bytes

        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()

        def pGroup = new PGroup('pexpire', data3, socket)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('pexpire', data3, pGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.handle()
        then:
        reply == IntegerReply.REPLY_0

        when:
        pGroup.cmd = 'pexpireat'
        reply = pGroup.handle()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        pGroup.cmd = 'pexpiretime'
        pGroup.data = data2
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('pexpiretime', data2, pGroup.slotNumber)
        reply = pGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2

        when:
        pGroup.data = data2
        pGroup.cmd = 'pttl'
        reply = pGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '60000'.bytes
        data4[3] = 'value'.bytes
        pGroup.cmd = 'psetex'
        pGroup.data = data4
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('psetex', data4, pGroup.slotNumber)
        reply = pGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        def socketInspector = initSocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        data2[0] = 'psubscribe'.bytes
        data2[1] = 'test_channel'.bytes
        pGroup.cmd = 'psubscribe'
        pGroup.data = data2
        reply = pGroup.handle()
        then:
        reply instanceof MultiBulkReply

        when:
        data2[0] = 'punsubscribe'.bytes
        pGroup.cmd = 'punsubscribe'
        reply = pGroup.handle()
        then:
        reply instanceof MultiBulkReply
    }

    def 'test pexpire'() {
        given:
        final short slot = 0
        def inMemoryGetSet = new InMemoryGetSet()
        def pGroup = new PGroup(null, null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        when:
        // an overflowing expire time must be rejected, redis 7.2 answers
        // 'ERR invalid expire time in 'pexpire' command' and keeps the ttl untouched
        def reply = pGroup.execute('pexpire a 9223372036854775807')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == "invalid expire time in 'pexpire' command"
        cv.expireAt == CompressedValue.NO_EXPIRE

        when:
        reply = pGroup.execute('pexpire a 100000')
        then:
        reply == IntegerReply.REPLY_1
        cv.expireAt > System.currentTimeMillis()

        when: 'a not integer ttl'
        reply = pGroup.execute('pexpire a abc')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'value is not an integer or out of range'
    }

    def 'test pexpireat'() {
        given:
        final short slot = 0
        def inMemoryGetSet = new InMemoryGetSet()
        def pGroup = new PGroup(null, null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        when:
        // an absolute expire time does not overflow, redis 7.2 accepts it
        def reply = pGroup.execute('pexpireat a 9223372036854775807')
        then:
        reply == IntegerReply.REPLY_1
        cv.expireAt == 9223372036854775807L

        when:
        // an expire time in the past deletes the key, redis 7.2 does the same
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = pGroup.execute('pexpireat a 0')
        then:
        reply == IntegerReply.REPLY_1
        inMemoryGetSet.getBuf(slot, 'a', 0, 0) == null

        when: 'a not integer expire time'
        reply = pGroup.execute('pexpireat a abc')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'value is not an integer or out of range'
    }

    def 'test persist'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('persist', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        cv.expireAt = System.currentTimeMillis() + 1000
        inMemoryGetSet.put(slot, 'a', 0, cv)
        def seqBeforePersist = cv.seq
        reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_1

        when:
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a', 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt == CompressedValue.NO_EXPIRE
        // the write must allocate a new sequence number, the replication compares the sequences
        bufOrCv.cv().seq != seqBeforePersist

        when:
        reply = pGroup.execute('persist')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test pfadd'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfcount', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('pfadd a abc')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = pGroup.execute('pfadd a abc')
        then:
        reply == IntegerReply.REPLY_0

        when: 'PFADD on a wrong type key answers WRONGTYPE'
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        def wrongTypeReply = pGroup.execute('pfadd a abc')
        then:
        wrongTypeReply instanceof ErrorReply
        (wrongTypeReply as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'

        when: 'PFCOUNT on a wrong type key answers WRONGTYPE'
        wrongTypeReply = pGroup.execute('pfcount a')
        then:
        wrongTypeReply instanceof ErrorReply
        (wrongTypeReply as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'

        when: 'PFADD key-only on missing key creates HLL and returns 1'
        inMemoryGetSet.remove(slot, 'hll_empty')
        def keyOnlyReply = pGroup.execute('pfadd hll_empty')
        then:
        keyOnlyReply == IntegerReply.REPLY_1

        when: 'PFADD key-only on existing HLL returns 0'
        keyOnlyReply = pGroup.execute('pfadd hll_empty')
        then:
        keyOnlyReply == IntegerReply.REPLY_0

        when: 'PFADD key-only on wrong type key answers WRONGTYPE'
        inMemoryGetSet.remove(slot, 'hll_wrong')
        def cvWrong = Mock.prepareCompressedValueList(1)[0]
        cvWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'hll_wrong', 0, cvWrong)
        def keyOnlyWrongTypeReply = pGroup.execute('pfadd hll_wrong')
        then:
        keyOnlyWrongTypeReply instanceof ErrorReply
        (keyOnlyWrongTypeReply as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'

        when:
        inMemoryGetSet.remove(slot, 'ttl_hll')
        def ttlReply = pGroup.execute('pfadd ttl_hll first')
        def sTtlHll = pGroup.slot('ttl_hll'.bytes, pGroup.slotNumber)
        def ttlCv = inMemoryGetSet.getBuf(slot, 'ttl_hll', sTtlHll.bucketIndex(), sTtlHll.keyHash()).cv()
        def expireAt = System.currentTimeMillis() + 60_000
        ttlCv.expireAt = expireAt
        ttlReply = pGroup.execute('pfadd ttl_hll second')
        def ttlCvAfterRewrite = inMemoryGetSet.getBuf(slot, 'ttl_hll', sTtlHll.bucketIndex(), sTtlHll.keyHash()).cv()
        then:
        ttlReply == IntegerReply.REPLY_1
        ttlCvAfterRewrite.expireAt == expireAt

        when: 'a PFADD that alters the HLL registers but not the estimated cardinality must be persisted'
        // 2471 elements, the estimated cardinality is 2467
        def manyElements = (0..2470).collect { "e$it" }.join(' ')
        def manyReply = pGroup.execute('pfadd big_hll ' + manyElements)
        def sBigHll = pGroup.slot('big_hll'.bytes, pGroup.slotNumber)
        def countReply = pGroup.execute('pfcount big_hll')
        then:
        manyReply == IntegerReply.REPLY_1
        (countReply as IntegerReply).integer == 2467

        when: 'the next element alters a register but the estimated cardinality stays the same'
        def beforeBytes = inMemoryGetSet.getBuf(slot, 'big_hll', sBigHll.bucketIndex(), sBigHll.keyHash()).cv().compressedData
        def oneMoreReply = pGroup.execute('pfadd big_hll e2471')
        def afterBytes = inMemoryGetSet.getBuf(slot, 'big_hll', sBigHll.bucketIndex(), sBigHll.keyHash()).cv().compressedData
        then:
        // redis answers 1 when at least one HyperLogLog internal register was altered
        oneMoreReply == IntegerReply.REPLY_1
        // the altered HLL must not be dropped
        afterBytes != beforeBytes

        when: 'pfadd reads the key only once when persisting a change and preserves expireAt'
        def recordingStore = new RecordingGetSet()
        def countingGroup = new PGroup('pfadd', null, null)
        countingGroup.byPassGetSet = recordingStore
        countingGroup.from(BaseCommand.mockAGroup())
        countingGroup.execute('pfadd single_pfadd init')
        def singleExpireAt = System.currentTimeMillis() + 60_000
        recordingStore.map['single_pfadd'].expireAt = singleExpireAt
        recordingStore.readCount.clear()
        def singlePfaddReply = countingGroup.execute('pfadd single_pfadd another')
        then:
        singlePfaddReply == IntegerReply.REPLY_1
        recordingStore.map['single_pfadd'].expireAt == singleExpireAt
        recordingStore.readCount['single_pfadd'].get() == 1
    }

    def 'test pfcount and pfmerge'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfcount', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        and:
        def eventloopDst = null
        def eventloopSrc = null

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('pfcount a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        pGroup.execute('pfadd a abc')
        pGroup.execute('pfadd b xyz')
        reply = pGroup.execute('pfcount a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        pGroup.execute('pfadd a shared')
        pGroup.execute('pfadd b shared')
        reply = pGroup.execute('pfcount a b')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3

        when:
        reply = pGroup.execute('pfmerge dst a b')
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'dst')
        reply = pGroup.execute('pfmerge dst a missing')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = pGroup.execute('pfcount dst')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

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
        pGroup.crossRequestWorker = true
        reply = pGroup.execute('pfcount a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 3
        }.result

        when:
        reply = pGroup.execute('pfmerge dst a missing')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when: 'a cross worker pfcount on a wrong type key answers WRONGTYPE'
        def wrongTypeCv = Mock.prepareCompressedValueList(1)[0]
        wrongTypeCv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'b', 0, wrongTypeCv)
        reply = pGroup.execute('pfcount a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        waitForReply(reply as AsyncReply) instanceof ErrorReply
        (waitForReply(reply as AsyncReply) as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'

        when: 'a cross worker pfmerge with a wrong type destination answers WRONGTYPE'
        inMemoryGetSet.put(slot, 'dst_wrong_type', 0, wrongTypeCv)
        reply = pGroup.execute('pfmerge dst_wrong_type a')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        waitForReply(reply as AsyncReply) instanceof ErrorReply
        (waitForReply(reply as AsyncReply) as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'

        when:
        pGroup.crossRequestWorker = false
        reply = pGroup.execute('pfcount dst')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when: 'a cross worker pfmerge writes the destination key on the destination slot thread'
        eventloopDst = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloopDst.keepAlive(true)
        def dstOwnerThreadId = new AtomicLong(-1)
        Thread.start {
            dstOwnerThreadId.set(Thread.currentThread().threadId())
            eventloopDst.run()
        }
        eventloopSrc = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        // the destination slot is owned by the other thread, the source slot by the current thread
        LocalPersist.instance.addOneSlotForTest2((short) 0)
        LocalPersist.instance.oneSlot((short) 0).setSlotWorkerEventloop(eventloopDst)
        LocalPersist.instance.addOneSlotForTest2((short) 1)
        LocalPersist.instance.oneSlot((short) 1).setSlotWorkerEventloop(eventloopSrc)

        def recordingGetSet = new RecordingGetSet()
        def crossGroup = new PGroup('pfmerge', null, null)
        crossGroup.byPassGetSet = recordingGetSet
        crossGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 2, (short) 2))
        crossGroup.crossRequestWorker = true

        def dstKey = null
        def srcKey = null
        for (int i = 0; i < 1000 && (dstKey == null || srcKey == null); i++) {
            def candidate = "pfmerge_key_$i"
            def candidateSlot = crossGroup.slot(candidate, 2).slot()
            if (candidateSlot == 0 && dstKey == null) {
                dstKey = candidate
            } else if (candidateSlot == 1 && srcKey == null) {
                srcKey = candidate
            }
        }

        def crossReply = crossGroup.execute("pfmerge $dstKey $srcKey")
        // run the source slot task, the aggregation callback runs on the source slot thread
        def deadline = System.currentTimeMillis() + 5000
        while (!crossReply.settablePromise.isComplete() && System.currentTimeMillis() < deadline) {
            eventloopSrc.run()
            Thread.sleep(5)
        }
        then:
        dstKey != null
        srcKey != null
        crossReply instanceof AsyncReply
        waitForReply(crossReply as AsyncReply) == OKReply.INSTANCE
        // the destination key must be written on the destination slot owner thread
        recordingGetSet.putThreadId[dstKey] == dstOwnerThreadId.get()
        // cross-worker pfmerge reads dstKey only once
        recordingGetSet.readCount[dstKey].get() == 1

        when: 'local pfmerge reads dst only once and preserves its expireAt'
        def localStore = new RecordingGetSet()
        def localGroup = new PGroup('pfmerge', null, null)
        localGroup.byPassGetSet = localStore
        localGroup.from(BaseCommand.mockAGroup())
        localGroup.execute('pfadd local_dst e1')
        localGroup.execute('pfadd local_src e2')
        def localExpireAt = System.currentTimeMillis() + 60_000
        localStore.map['local_dst'].expireAt = localExpireAt
        localStore.readCount.clear()
        def localMergeReply = localGroup.execute('pfmerge local_dst local_src')
        then:
        localMergeReply == OKReply.INSTANCE
        localStore.map['local_dst'].expireAt == localExpireAt
        localStore.readCount['local_dst'].get() == 1

        cleanup:
        eventloop?.breakEventloop()
        eventloopDst?.breakEventloop()
        eventloopSrc?.breakEventloop()
    }

    def 'test pfmerge destkey-only'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfmerge', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when: 'PFMERGE destkey-only on missing key creates empty HLL and returns OK'
        inMemoryGetSet.remove(slot, 'dst_empty')
        def reply = pGroup.execute('pfmerge dst_empty')
        then:
        reply == OKReply.INSTANCE

        when: 'PFMERGE destkey-only on existing HLL preserves it and returns OK'
        pGroup.execute('pfadd existing_hll abc')
        reply = pGroup.execute('pfmerge existing_hll')
        then:
        reply == OKReply.INSTANCE
        def countReply = pGroup.execute('pfcount existing_hll')
        (countReply as IntegerReply).integer == 1

        when: 'PFMERGE destkey-only on wrong type key answers WRONGTYPE'
        inMemoryGetSet.remove(slot, 'dst_wrong')
        def cvWrong = Mock.prepareCompressedValueList(1)[0]
        cvWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'dst_wrong', 0, cvWrong)
        def wrongTypeReply = pGroup.execute('pfmerge dst_wrong')
        then:
        wrongTypeReply instanceof ErrorReply
        (wrongTypeReply as ErrorReply).message == 'WRONGTYPE Key is not a valid HyperLogLog string value.'
    }

    def 'test pfdebug'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfdebug', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when: 'wrong number of arguments'
        def reply = pGroup.execute('pfdebug')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = pGroup.execute('pfdebug encoding')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = pGroup.execute('pfdebug encoding hll1 extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'the key does not exist'
        inMemoryGetSet.remove(slot, 'missing')
        reply = pGroup.execute('pfdebug encoding missing')
        then:
        reply == new ErrorReply('The specified key does not exist')

        when: 'the key lookup and the type check run before the subcommand dispatch'
        reply = pGroup.execute('pfdebug nosuch missing')
        then:
        reply == new ErrorReply('The specified key does not exist')

        when: 'a wrong type key answers the HLL WRONGTYPE'
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'wrongtype', 0, cv)
        reply = pGroup.execute('pfdebug encoding wrongtype')
        then:
        reply == ErrorReply.WRONG_TYPE_HLL

        when: 'key too long'
        reply = pGroup.execute('pfdebug encoding >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'an unknown subcommand on a valid HLL key'
        inMemoryGetSet.remove(slot, 'hll1')
        pGroup.execute('pfadd hll1 a b c')
        reply = pGroup.execute('pfdebug nosuch hll1')
        then:
        reply == new ErrorReply("Unknown PFDEBUG subcommand 'nosuch'")

        when: 'encoding of a sparse HLL'
        reply = pGroup.execute('pfdebug encoding hll1')
        then:
        reply == new BulkReply('sparse')

        when: 'todense converts and replies 1'
        reply = pGroup.execute('pfdebug todense hll1')
        then:
        reply == IntegerReply.REPLY_1

        when: 'after the conversion the encoding is dense'
        reply = pGroup.execute('pfdebug encoding hll1')
        then:
        reply == new BulkReply('dense')

        when: 'todense on an already dense HLL replies 0'
        reply = pGroup.execute('pfdebug todense hll1')
        then:
        reply == IntegerReply.REPLY_0

        when: 'the conversion preserves the estimated cardinality'
        reply = pGroup.execute('pfcount hll1')
        then:
        (reply as IntegerReply).integer == 3

        when: 'getreg replies the dense register array'
        reply = pGroup.execute('pfdebug getreg hll1')
        then:
        reply instanceof MultiBulkReply
        def regs = (reply as MultiBulkReply).replies
        regs.length == 16384
        regs.findAll { (it as IntegerReply).integer > 0 }.size() > 0

        when: 'getreg on a sparse HLL persists the dense conversion'
        inMemoryGetSet.remove(slot, 'hll2')
        pGroup.execute('pfadd hll2 x')
        reply = pGroup.execute('pfdebug getreg hll2')
        def encodingAfterGetreg = pGroup.execute('pfdebug encoding hll2')
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies).length == 16384
        encodingAfterGetreg == new BulkReply('dense')

        when: 'todense keeps the key expire time'
        inMemoryGetSet.remove(slot, 'ttl_hll')
        pGroup.execute('pfadd ttl_hll first')
        def sTtlHll = pGroup.slot('ttl_hll'.bytes, pGroup.slotNumber)
        def ttlCv = inMemoryGetSet.getBuf(slot, 'ttl_hll', sTtlHll.bucketIndex(), sTtlHll.keyHash()).cv()
        def expireAt = System.currentTimeMillis() + 60_000
        ttlCv.expireAt = expireAt
        pGroup.execute('pfdebug todense ttl_hll')
        def ttlCvAfter = inMemoryGetSet.getBuf(slot, 'ttl_hll', sTtlHll.bucketIndex(), sTtlHll.keyHash()).cv()
        then:
        ttlCvAfter.expireAt == expireAt

        when: 'decode on a dense HLL'
        reply = pGroup.execute('pfdebug decode hll1')
        then:
        reply == new ErrorReply('HLL encoding is not sparse')

        when: 'decode on a sparse HLL renders the opcode stream'
        inMemoryGetSet.remove(slot, 'hll3')
        (0..<100).each { pGroup.execute("pfadd hll3 e$it") }
        reply = pGroup.execute('pfdebug decode hll3')
        then:
        reply instanceof BulkReply
        def decoded = (reply as BulkReply).asString()
        decoded.startsWith('Z:')
        decoded.contains('v:')

        when: 'the decoded opcodes reproduce the registers getreg reports for the same HLL'
        def regsReply = pGroup.execute('pfdebug getreg hll3')
        def rebuilt = new int[16384]
        def idx = 0
        decoded.split(' ').each { token ->
            if (token.startsWith('z:')) {
                idx += Integer.parseInt(token.substring(2))
            } else if (token.startsWith('Z:')) {
                idx += Integer.parseInt(token.substring(2))
            } else if (token.startsWith('v:')) {
                def parts = token.substring(2).split(',')
                def value = Integer.parseInt(parts[0])
                def count = Integer.parseInt(parts[1])
                count.times { rebuilt[idx++] = value }
            }
        }
        then:
        idx == 16384
        def registerArray = (regsReply as MultiBulkReply).replies.collect { (it as IntegerReply).integer }
        (0..<16384).every { rebuilt[it] == registerArray[it] }

        when: 'the subcommand names are case-insensitive'
        inMemoryGetSet.remove(slot, 'hll4')
        pGroup.execute('pfadd hll4 a b')
        reply = pGroup.execute('pfdebug ENCODING hll4')
        then:
        reply == new BulkReply('sparse')
    }

    def 'test pfselftest'() {
        given:
        def pGroup = new PGroup('pfselftest', null, null)
        pGroup.from(BaseCommand.mockAGroup())

        when: 'wrong number of arguments'
        def reply = pGroup.execute('pfselftest extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'the self test passes'
        reply = pGroup.execute('pfselftest')
        then:
        reply == OKReply.INSTANCE

        when: 'the error bound check reports a failure for an out of bound estimate'
        def failure = PGroup.selfTestMaxerrExceeded(10, 13)
        then:
        failure == 'TESTFAILED Too big error. card:10 abserr:3'

        when: 'the checkpoint 10 special case allows one unit of error'
        failure = PGroup.selfTestMaxerrExceeded(10, 9)
        then:
        failure == null

        when: 'the error bound scales with the checkpoint'
        failure = PGroup.selfTestMaxerrExceeded(100000, 95000)
        then:
        failure == 'TESTFAILED Too big error. card:100000 abserr:5000'

        when: 'a good estimate passes at a big checkpoint'
        failure = PGroup.selfTestMaxerrExceeded(100000, 99700)
        then:
        failure == null
    }

    def 'test psetex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def pGroup = new PGroup('psetex', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = pGroup.execute('psetex a 100000 value')
        then:
        reply == OKReply.INSTANCE

        when:
        def sTtl = pGroup.slot('a'.bytes, pGroup.slotNumber)
        def cv = inMemoryGetSet.getBuf(slot, 'a', sTtl.bucketIndex(), sTtl.keyHash()).cv()
        then:
        cv.expireAt > System.currentTimeMillis()

        when: 'a not integer ttl'
        reply = pGroup.execute('psetex a abc value')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'value is not an integer or out of range'

        when: 'a not positive ttl'
        reply = pGroup.execute('psetex a 0 value')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == "invalid expire time in 'psetex' command"
    }

    def 'test psubscribe'() {
        given:
        AclUsers.instance.initForTest()
        def socketInspector = initSocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        def socket = SocketInspectorTest.mockTcpSocket()
        def pGroup = new PGroup('psubscribe', null, socket)
        pGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = pGroup.execute('psubscribe news.*')

        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        // redis pushes the confirmation with the first element 'psubscribe'
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'psubscribe'
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == 'news.*'
        ((reply as MultiBulkReply).replies[2] as IntegerReply).integer == 1
    }

    def 'test punsubscribe'() {
        given:
        AclUsers.instance.initForTest()
        def socketInspector = initSocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        def socket = SocketInspectorTest.mockTcpSocket()
        def pGroup = new PGroup('psubscribe', null, socket)
        pGroup.from(BaseCommand.mockAGroup())
        pGroup.execute('psubscribe news.*')

        when:
        def reply = pGroup.execute('punsubscribe news.*')

        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        // redis pushes the confirmation with the first element 'punsubscribe'
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'punsubscribe'
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == 'news.*'
        ((reply as MultiBulkReply).replies[2] as IntegerReply).integer == 0
    }

    def 'test publish'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'test_channel'.bytes
        data3[2] = 'message'.bytes

        and:
        LocalPersist.instance.socketInspector = new SocketInspector()

        and:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()

        when:
        def reply = _PGroup.publish(data3, null)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        aclUsers.upInsert('default') {u ->
            u.addRPubSub(true, RPubSub.fromLiteral('&special_channel'))
        }
        reply = _PGroup.publish(data3, socket)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        aclUsers.upInsert('default') {u ->
            u.on = false
        }
        reply = _PGroup.publish(data3, socket)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        def data1 = new byte[1][]
        reply = _PGroup.publish(data1, null)
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test pubsub'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pubsub', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        def socketInspector = initSocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        def reply = pGroup.execute('pubsub channels')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def channel = 'test_channel'
        reply = pGroup.execute('pubsub channels ' + channel)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        socketInspector.subscribe(channel, false, socket)
        reply = pGroup.execute('pubsub channels ' + channel)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == channel

        when:
        reply = pGroup.execute('pubsub numpat')
        then:
        // redis returns the number of the unique patterns all the clients are subscribed to
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = pGroup.execute('pubsub numsub ' + channel)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == channel
        (reply as MultiBulkReply).replies[1] instanceof IntegerReply
        ((reply as MultiBulkReply).replies[1] as IntegerReply).integer == 1

        when: 'a pattern subscription is not counted by PUBSUB NUMSUB'
        def patternSocket = SocketInspectorTest.mockTcpSocket()
        socketInspector.subscribe('test_*', true, patternSocket)
        reply = pGroup.execute('pubsub numsub ' + channel)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        ((reply as MultiBulkReply).replies[1] as IntegerReply).integer == 1

        when: 'the pattern subscriptions are counted by PUBSUB NUMPAT'
        reply = pGroup.execute('pubsub numpat')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'a channel without any subscriber is reported as zero'
        reply = pGroup.execute('pubsub numsub no_such_channel')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'no_such_channel'
        ((reply as MultiBulkReply).replies[1] as IntegerReply).integer == 0

        when: 'the pattern subscriptions are not listed as channels'
        reply = pGroup.execute('pubsub channels')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == channel

        when:
        reply = pGroup.execute('pubsub channels test_*')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == channel

        when: 'a glob that matches no channel returns an empty reply'
        reply = pGroup.execute('pubsub channels other_*')
        then:
        reply == MultiBulkReply.EMPTY

        when: 'PUBSUB NUMSUB without a channel returns an empty list'
        reply = pGroup.execute('pubsub numsub')
        then:
        reply == MultiBulkReply.EMPTY

        when: 'an unknown subcommand is an error'
        reply = pGroup.execute('pubsub xxx')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == "unknown subcommand 'xxx'. Try PUBSUB HELP."
    }
}
