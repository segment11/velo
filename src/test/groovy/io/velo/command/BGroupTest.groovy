package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisList
import spock.lang.Specification

import java.time.Duration

class BGroupTest extends Specification {
    def _BGroup = new BGroup(null, null, null)
    final short slot = 0

    def 'test blocking list promise'() {
        expect:
        !BGroup.setReplyIfBlockingListExist('a', new byte[1][0])

        when:
        def settablePromise = new SettablePromise<Reply>()
        BGroup.addBlockingListPromiseByKey('a', settablePromise, true)
        def elementValueBytesArray = new byte[1][]
        elementValueBytesArray[0] = 'a'.bytes
        def bb = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray)
        then:
        settablePromise.isComplete()
        settablePromise.getResult() instanceof BulkReply
        (settablePromise.getResult() as BulkReply).raw == 'a'.bytes
        bb.length == 0

        when:
        var settablePromise2 = new SettablePromise<Reply>()
        var settablePromise3 = new SettablePromise<Reply>()
        var settablePromise4 = new SettablePromise<Reply>()
        settablePromise2.set(NilReply.INSTANCE)
        BGroup.addBlockingListPromiseByKey('a', settablePromise2, true)
        BGroup.addBlockingListPromiseByKey('a', settablePromise3, false)
        BGroup.addBlockingListPromiseByKey('a', settablePromise4, true)
        def elementValueBytesArray2 = new byte[1][]
        elementValueBytesArray2[0] = 'a'.bytes
        def bb2 = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray2)
        then:
        settablePromise2.isComplete()
        settablePromise3.isComplete()
        !settablePromise4.isComplete()
        bb2.length == 0

        when:
        BGroup.clearBlockingListPromisesForAllKeys()
        var settablePromise33 = new SettablePromise<Reply>()
        var settablePromise44 = new SettablePromise<Reply>()
        BGroup.addBlockingListPromiseByKey('a', settablePromise44, true)
        BGroup.addBlockingListPromiseByKey('a', settablePromise33, false)
        def elementValueBytesArray33 = new byte[1][]
        elementValueBytesArray33[0] = 'a'.bytes
        def bb33 = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray33)
        then:
        settablePromise44.isComplete()
        !settablePromise33.isComplete()
        bb33.length == 0

        when:
        BGroup.clearBlockingListPromisesForAllKeys()
        var settablePromise333 = new SettablePromise<Reply>()
        var settablePromise444 = new SettablePromise<Reply>()
        BGroup.addBlockingListPromiseByKey('a', settablePromise333, true)
        BGroup.addBlockingListPromiseByKey('a', settablePromise444, true)
        def elementValueBytesArray333 = new byte[1][]
        elementValueBytesArray333[0] = 'a'.bytes
        def bb333 = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray333)
        then:
        settablePromise333.isComplete()
        !settablePromise444.isComplete()
        bb333.length == 0

        when:
        BGroup.clearBlockingListPromisesForAllKeys()
        var settablePromise3333 = new SettablePromise<Reply>()
        var settablePromise4444 = new SettablePromise<Reply>()
        BGroup.addBlockingListPromiseByKey('a', settablePromise3333, false)
        BGroup.addBlockingListPromiseByKey('a', settablePromise4444, false)
        def elementValueBytesArray3333 = new byte[1][]
        elementValueBytesArray3333[0] = 'a'.bytes
        def bb3333 = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray3333)
        then:
        settablePromise3333.isComplete()
        !settablePromise4444.isComplete()
        bb3333.length == 0

        when:
        BGroup.clearBlockingListPromisesForAllKeys()
        var settablePromise5 = new SettablePromise<Reply>()
        var settablePromise6 = new SettablePromise<Reply>()
        BGroup.addBlockingListPromiseByKey('a', settablePromise5, true)
        BGroup.addBlockingListPromiseByKey('a', settablePromise6, false)
        def elementValueBytesArray5 = new byte[3][]
        elementValueBytesArray5[0] = 'a'.bytes
        elementValueBytesArray5[1] = 'b'.bytes
        elementValueBytesArray5[2] = 'c'.bytes
        def bb5 = BGroup.setReplyIfBlockingListExist('a', elementValueBytesArray5)
        then:
        settablePromise5.isComplete()
        (settablePromise5.getResult() as BulkReply).raw == 'a'.bytes
        settablePromise6.isComplete()
        (settablePromise6.getResult() as BulkReply).raw == 'c'.bytes
        bb5.length == 1
    }

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _BGroup.parseSlots('bitcount', data2, slotNumber)
        then:
        sList.size() == 1

        when:
        def data1 = new byte[1][]
        sList = _BGroup.parseSlots('bitcount', data1, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _BGroup.parseSlots('bitfield', data2, slotNumber)
        then:
        sList.size() == 1

        when:
        sList = _BGroup.parseSlots('bitfield_ro', data2, slotNumber)
        then:
        sList.size() == 1

        when:
        sList = _BGroup.parseSlots('bitpos', data2, slotNumber)
        then:
        sList.size() == 1

        when:
        sList = _BGroup.parseSlots('bitop', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        def data5 = new byte[5][]
        data5[1] = 'and'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'c'.bytes
        sList = _BGroup.parseSlots('bitop', data5, slotNumber)
        then:
        sList.size() == 3

        when:
        sList = _BGroup.parseSlots('bgsave', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _BGroup.parseSlots('blpop', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _BGroup.parseSlots('brpop', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes
        sList = _BGroup.parseSlots('blpop', data4, slotNumber)
        then:
        sList.size() == 2

        when:
        sList = _BGroup.parseSlots('brpop', data4, slotNumber)
        then:
        sList.size() == 2
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def bGroup = new BGroup('bitcount', data1, null)
        bGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bitpos'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bgsave'
        reply = bGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        bGroup.cmd = 'blpop'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'brpop'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'zzz'
        reply = bGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test bitcount'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bitcount a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'foobar'.bytes
        cv.compressedLength = 6
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bitcount a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 26

        when:
        reply = bGroup.execute('bitcount a 0 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

        when:
        reply = bGroup.execute('bitcount a 0 -1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 26

        when:
        reply = bGroup.execute('bitcount a 2 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = bGroup.execute('bitcount a 1 1 byte')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 6

        when:
        reply = bGroup.execute('bitcount a 5 30 bit')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 17

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bitcount a')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        reply = bGroup.execute('bitcount a b 5')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = bGroup.execute('bitcount a 5 30 bit x')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data3 = new byte[3][]
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        data3[2] = '0'.bytes
        bGroup.data = data3
        reply = bGroup.bitcount()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test bitpos'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bitpos a 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -1

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'foobar'.bytes
        cv.compressedLength = 6
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bitpos a 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 0 0 -1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 0 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 0 2 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -1

        when:
        reply = bGroup.execute('bitpos a 0 0 -1 byte')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 1 0 -1 byte')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 0 0 -1 bit')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        reply = bGroup.execute('bitpos a 1 0 -1 bit')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer != -1

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bitpos a 1')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        reply = bGroup.execute('bitpos a 1 _ _')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = bGroup.execute('bitpos a 1 _ _ _ _')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bitpos a 2 0 -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bitpos a 10 0 -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data3 = new byte[3][]
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        data3[2] = '0'.bytes
        bGroup.data = data3
        reply = bGroup.bitpos()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test blpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('blpop a 0')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('blpop a 0')
        then:
        reply == NilReply.INSTANCE

        when:
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('blpop a 0')
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).raw == 'a'.bytes
        LGroup.getRedisList('a'.bytes, bGroup.slotWithKeyHashListParsed.getFirst(), bGroup) == null

        when:
        // rl is already removed
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        reply = bGroup.execute('blpop a 1')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        reply = bGroup.execute('blpop a x')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = bGroup.execute('blpop a 3601')
        then:
        reply instanceof ErrorReply

        when:
        def data3 = new byte[4][]
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        data3[2] = '0'.bytes
        bGroup.data = data3
        reply = bGroup.blpop(true)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
