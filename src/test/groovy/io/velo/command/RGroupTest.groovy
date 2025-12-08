package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.*
import io.velo.mock.InMemoryGetSet
import io.velo.persist.*
import io.velo.rdb.RDBParser
import io.velo.repl.ReplPairTest
import io.velo.repl.incremental.XOneWalGroupPersist
import io.velo.reply.*
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.time.Duration

class RGroupTest extends Specification {
    final short slot = 0
    def _RGroup = new RGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        when:
        def sRenameList = _RGroup.parseSlots('rename', data3, slotNumber)
        def sRenamenxList = _RGroup.parseSlots('renamenx', data3, slotNumber)
        def sRpoplpushList = _RGroup.parseSlots('rpoplpush', data3, slotNumber)
        def sRestoreList = _RGroup.parseSlots('restore', data3, slotNumber)
        def sRpopList = _RGroup.parseSlots('rpop', data3, slotNumber)
        def sRpushList = _RGroup.parseSlots('rpush', data3, slotNumber)
        def sRpushxList = _RGroup.parseSlots('rpushx', data3, slotNumber)
        def sList = _RGroup.parseSlots('rxxx', data3, slotNumber)
        then:
        sRenameList.size() == 2
        sRenamenxList.size() == 2
        sRpoplpushList.size() == 2
        sRestoreList.size() == 0
        sRpopList.size() == 1
        sRpushList.size() == 1
        sRpushxList.size() == 1
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        sRenameList = _RGroup.parseSlots('rename', data4, slotNumber)
        sRpoplpushList = _RGroup.parseSlots('rpoplpush', data4, slotNumber)
        sRestoreList = _RGroup.parseSlots('restore', data4, slotNumber)
        sRpopList = _RGroup.parseSlots('rpop', data4, slotNumber)
        then:
        sRenameList.size() == 0
        sRpoplpushList.size() == 0
        sRestoreList.size() == 1
        sRpopList.size() == 0

        when:
        def data1 = new byte[1][]
        sRpushList = _RGroup.parseSlots('rpush', data1, slotNumber)
        sRpushxList = _RGroup.parseSlots('rpushx', data1, slotNumber)
        then:
        sRpushList.size() == 0
        sRpushxList.size() == 0

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        sRpopList = _RGroup.parseSlots('rpop', data2, slotNumber)
        then:
        sRpopList.size() == 1
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]
        def socket = SocketInspectorTest.mockTcpSocket()

        def rGroup = new RGroup('readonly', data1, socket)
        rGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = rGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        rGroup.cmd = 'readwrite'
        reply = rGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        rGroup.cmd = 'rename'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'renamenx'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        rGroup.cmd = 'randomkey'
        rGroup.data = data2
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'replicaof'
        rGroup.data = data1
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'restore'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'role'
        reply = rGroup.handle()
        then:
        reply instanceof MultiBulkReply

        when:
        rGroup.cmd = 'rpop'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'rpoplpush'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'rpush'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'rpushx'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'zzz'
        reply = rGroup.handle()
        then:
        reply == NilReply.INSTANCE
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
            def xForBinlog = new XOneWalGroupPersist(true, false, 0)
            def walGroupIndex = Wal.calcWalGroupIndex(i)
            keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, shortValueList, xForBinlog)
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

        cleanup:
        eventloop.breakEventloop()
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
        reply = rGroup.execute('restore a 0 bbb replace') { data ->
            data[3] = RDBParser.dumpString('values'.bytes)
        }
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().compressedData == 'values'.bytes

        when:
        reply = rGroup.execute('restore a 0 bbb replace') { data ->
            data[3] = RDBParser.dumpString('1'.bytes)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().numberValue() == 1

        when:
        reply = rGroup.execute('restore list0 0 bbb replace') { data ->
            def rl = new RedisList()
            rl.addLast('a'.bytes)
            rl.addLast('b'.bytes)
            rl.addLast('c'.bytes)
            data[3] = RDBParser.dumpList(rl)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'list0'.bytes, 0, 0L)
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
        bufOrCv = inMemoryGetSet.getBuf(slot, 'hash0'.bytes, 0, 0L)
        then:
        reply == OKReply.INSTANCE
        bufOrCv.cv().isHash()
        RedisHH.decode(bufOrCv.cv().compressedData).size() == 3

        when:
        reply = rGroup.execute('restore set0 0 bbb replace') { data ->
            def rhk = new RedisHashKeys()
            rhk.add('a')
            rhk.add('b')
            rhk.add('c')
            data[3] = RDBParser.dumpSet(rhk)
        }
        bufOrCv = inMemoryGetSet.getBuf(slot, 'set0'.bytes, 0, 0L)
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
        bufOrCv = inMemoryGetSet.getBuf(slot, 'zset0'.bytes, 0, 0L)
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
        (reply as MultiBulkReply).replies[0] == new BulkReply('master'.bytes)
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_0
        (reply as MultiBulkReply).replies[2] == MultiBulkReply.EMPTY

        when:
        def replPairAsMaster = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster)
        reply = rGroup.role()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == new BulkReply('master'.bytes)
        (reply as MultiBulkReply).replies[2] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[2]).replies[0] instanceof MultiBulkReply

        when:
        oneSlot.replPairs.clear()
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, oneSlot.masterUuid)
        oneSlot.replPairs.add(replPairAsSlave)
        reply = rGroup.role()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5
        (reply as MultiBulkReply).replies[0] == new BulkReply('slave'.bytes)

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
            result instanceof BulkReply && ((BulkReply) result).raw == '8'.bytes
        }.result

        when:
        reply = rGroup.execute('rpoplpush >key b')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = rGroup.execute('rpoplpush a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

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
        def s1 = BaseCommand.slot('a'.bytes, 1)
        def s2 = BaseCommand.slot('b'.bytes, 1)
        def reply = rGroup.moveBlock(
                'a'.bytes, s1,
                'b'.bytes, s2,
                true, true, 0)
        then:
        reply == NilReply.INSTANCE

        when:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        reply = rGroup.moveBlock(
                'a'.bytes, s1,
                'b'.bytes, s2,
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
                'a'.bytes, s1,
                'b'.bytes, s2,
                true, true, 0)
        then:
        reply == NilReply.INSTANCE

        when:
        reply = rGroup.moveBlock(
                'a'.bytes, s1,
                'b'.bytes, s2,
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
                'a'.bytes, s1,
                'b'.bytes, s2,
                true, true, 0)
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == '1'.bytes

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
                'a'.bytes, s1,
                'b'.bytes, s2,
                true, true, 0)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof BulkReply && ((BulkReply) result).raw == '1'.bytes
        }.result

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
    }
}
