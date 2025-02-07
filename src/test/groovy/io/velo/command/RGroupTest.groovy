package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.Debug
import io.velo.SocketInspectorTest
import io.velo.mock.InMemoryGetSet
import io.velo.persist.*
import io.velo.repl.incremental.XOneWalGroupPersist
import io.velo.reply.*
import io.velo.type.RedisList
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
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rename', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = _RGroup.parseSlots('rename', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.rename(false)
        then:
        reply == ErrorReply.NO_SUCH_KEY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.rename(false)
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = rGroup.rename(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = rGroup.rename(true)
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
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = rGroup.rename(true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rename(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rename(false)
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
        VeloRDBImporter.DEBUG = true
        Debug.instance.logRestore = true
        def reply = rGroup.execute('restore a 0 bbb replace absttl idletime 0 freq 0')
        then:
        reply == OKReply.INSTANCE

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

        cleanup:
        VeloRDBImporter.DEBUG = false
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
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rpoplpush', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = _RGroup.parseSlots('rpoplpush', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.rpoplpush()
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
        reply = rGroup.rpoplpush()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '9'.bytes

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
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof BulkReply && ((BulkReply) result).raw == '8'.bytes
        }.result

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rpoplpush()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rpoplpush()
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
        ((IntegerReply) reply).integer == 1

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
        ((AsyncReply) reply).settablePromise.whenResult { result ->
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
        ((AsyncReply) reply).settablePromise.whenResult { result ->
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
        ((BulkReply) reply).raw == '1'.bytes

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
        localPersist.oneSlot(s2.slot()).netWorkerEventloop = eventloop
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
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof BulkReply && ((BulkReply) result).raw == '1'.bytes
        }.result

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
    }
}
