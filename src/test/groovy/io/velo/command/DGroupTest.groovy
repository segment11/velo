package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.Dict
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class DGroupTest extends Specification {
    def _DGroup = new DGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sDecrList = _DGroup.parseSlots('decr', data2, slotNumber)
        def sDecrByList = _DGroup.parseSlots('decrby', data2, slotNumber)
        def sDelList = _DGroup.parseSlots('del', data2, slotNumber)
        def sList = _DGroup.parseSlots('dxxx', data2, slotNumber)
        then:
        sDecrList.size() == 1
        sDecrByList.size() == 1
        sDelList.size() == 1
        sList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        sDelList = _DGroup.parseSlots('del', data3, slotNumber)
        then:
        sDelList.size() == 2

        when:
        data3[1] = 'object'.bytes
        def sDebugList = _DGroup.parseSlots('debug', data3, slotNumber)
        then:
        sDebugList.size() == 1

        when:
        def data1 = new byte[1][]
        sDebugList = _DGroup.parseSlots('debug', data1, slotNumber)
        sDecrList = _DGroup.parseSlots('decr', data1, slotNumber)
        sDelList = _DGroup.parseSlots('del', data1, slotNumber)
        then:
        sDebugList.size() == 0
        sDecrList.size() == 0
        sDelList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('debug', data1, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = dGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        dGroup.cmd = 'del'
        reply = dGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        dGroup.data = data2
        dGroup.cmd = 'dbsize'
        reply = dGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        dGroup.data = data1
        dGroup.cmd = 'decr'
        reply = dGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        dGroup.cmd = 'decrby'
        reply = dGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        dGroup.data = data2
        dGroup.cmd = 'decr'
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('decr', data2, dGroup.slotNumber)
        reply = dGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -1

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        dGroup.data = data3
        dGroup.cmd = 'decrby'
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('decrby', data3, dGroup.slotNumber)
        reply = dGroup.handle()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // decrby
        data3[1] = 'n'.bytes
        data3[2] = '1'.bytes
        dGroup.setNumber('n'.bytes, 0, dGroup.slotWithKeyHashListParsed.getFirst())
        reply = dGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -1

        when:
        dGroup.cmd = 'zzz'
        reply = dGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test debug'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'object'.bytes
        data3[2] = 'key'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('debug', data3, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup())

        when:
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('debug', data3, dGroup.slotNumber)
        def reply = dGroup.debug()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = new CompressedValue()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        cv.compressedData = new byte[1]
        cv.compressedLength = 1
        inMemoryGetSet.put(slot, 'key', 0, cv)
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':int')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_DOUBLE
        cv.compressedData = new byte[8]
        cv.compressedLength = 8
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':embstr')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':hashtable')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':quicklist')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':hashtable')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':ziplist')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_STREAM
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':stream')

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':embstr')

        when:
        cv.dictSeqOrSpType = -200
        reply = dGroup.debug()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains(':unknown')

        when:
        data3[1] = '_'.bytes
        reply = dGroup.debug()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test del'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('del', data2, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup())

        when:
        def data1 = new byte[1][]
        dGroup.data = data1
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('del', data1, dGroup.slotNumber)
        def reply = dGroup.del()
        then:
        reply == ErrorReply.FORMAT

        when:
        dGroup.data = data2
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('del', data2, dGroup.slotNumber)
        reply = dGroup.del()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data2[1] = 'a'.bytes
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('del', data2, dGroup.slotNumber)
        reply = dGroup.del()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = dGroup.del()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

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
        dGroup.crossRequestWorker = true
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        dGroup.data = data3
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('del', data3, dGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = dGroup.del()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test dbsize'() {
        given:
        final short slot = 0

        def data1 = new byte[1][]

        def dGroup = new DGroup('dbsize', data1, null)
        dGroup.from(BaseCommand.mockAGroup())

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
        def reply = dGroup.dbsize()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 0
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test decr by'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('decrby', data2, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup())

        when:
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('decrby', data2, dGroup.slotNumber)
        def cv = new CompressedValue()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        // 0
        cv.compressedData = new byte[1]
        cv.compressedLength = 1
        inMemoryGetSet.put(slot, 'a', 0, cv)
        def reply = dGroup.decrBy(1, 0)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -1

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('decrby', data2, dGroup.slotNumber)
        reply = dGroup.decrBy(1, 0)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data2[1] = 'a'.bytes
        dGroup.slotWithKeyHashListParsed = _DGroup.parseSlots('decrby', data2, dGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        reply = dGroup.decrBy(1, 0)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -1

        when:
        cv.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = dGroup.decrBy(1, 0)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = dGroup.decrBy(1, 0)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        cv.compressedLength = 4
        cv.compressedData = '1234'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = dGroup.decrBy(1, 0)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1233

        when:
        // float
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_DOUBLE
        cv.compressedLength = 8
        def doubleBytes = new byte[8]
        ByteBuffer.wrap(doubleBytes).putDouble(1.1)
        cv.compressedData = doubleBytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = dGroup.decrBy(0, 1)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '0.10'.bytes

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cv.compressedLength = 3
        cv.compressedData = '1.1'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = dGroup.decrBy(0, 1)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '0.10'.bytes
    }
}
