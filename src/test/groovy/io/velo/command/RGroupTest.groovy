package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.reply.*
import io.velo.type.RedisList
import spock.lang.Specification

import java.time.Duration

class RGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        when:
        def sRenameList = RGroup.parseSlots('rename', data3, slotNumber)
        def sRpoplpushList = RGroup.parseSlots('rpoplpush', data3, slotNumber)
        def sRestoreList = RGroup.parseSlots('restore', data3, slotNumber)
        def sRpopList = RGroup.parseSlots('rpop', data3, slotNumber)
        def sRpushList = RGroup.parseSlots('rpush', data3, slotNumber)
        def sRpushxList = RGroup.parseSlots('rpushx', data3, slotNumber)
        def sList = RGroup.parseSlots('rxxx', data3, slotNumber)
        then:
        sRenameList.size() == 2
        sRpoplpushList.size() == 2
        sRestoreList.size() == 0
        sRpopList.size() == 1
        sRpushList.size() == 1
        sRpushxList.size() == 1
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        sRenameList = RGroup.parseSlots('rename', data4, slotNumber)
        sRpoplpushList = RGroup.parseSlots('rpoplpush', data4, slotNumber)
        sRestoreList = RGroup.parseSlots('restore', data4, slotNumber)
        sRpopList = RGroup.parseSlots('rpop', data4, slotNumber)
        then:
        sRenameList.size() == 0
        sRpoplpushList.size() == 0
        sRestoreList.size() == 1
        sRpopList.size() == 0

        when:
        def data1 = new byte[1][]
        sRpushList = RGroup.parseSlots('rpush', data1, slotNumber)
        sRpushxList = RGroup.parseSlots('rpushx', data1, slotNumber)
        then:
        sRpushList.size() == 0
        sRpushxList.size() == 0

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        sRpopList = RGroup.parseSlots('rpop', data2, slotNumber)
        then:
        sRpopList.size() == 1
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def rGroup = new RGroup('rename', data1, null)
        rGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'restore'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'rpoplpush'
        reply = rGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        rGroup.cmd = 'zzz'
        reply = rGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test rename'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rename', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = RGroup.parseSlots('rename', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.rename()
        then:
        reply == ErrorReply.NO_SUCH_KEY

        when:
        def cv = io.velo.persist.Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.rename()
        then:
        reply == OKReply.INSTANCE

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
        reply = rGroup.rename()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rename()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = rGroup.rename()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test rpop'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rpop', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = RGroup.parseSlots('rpop', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = rGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test rpoplpush'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rpoplpush', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = RGroup.parseSlots('rpoplpush', data3, rGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = rGroup.rpoplpush()
        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = io.velo.persist.Mock.prepareCompressedValueList(2)
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
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def rGroup = new RGroup('rpush', data3, null)
        rGroup.byPassGetSet = inMemoryGetSet
        rGroup.from(BaseCommand.mockAGroup())

        when:
        rGroup.slotWithKeyHashListParsed = RGroup.parseSlots('rpush', data3, rGroup.slotNumber)
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
}
