package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisList
import spock.lang.Specification

import java.time.Duration

class BGroupTest extends Specification {
    def _BGroup = new BGroup(null, null, null)
    final short slot = 0

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
        def sBfList = _BGroup.parseSlots('bf.add', data2, slotNumber)
        then:
        sBfList.size() == 1

        when:
        sList = _BGroup.parseSlots('bgsave', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _BGroup.parseSlots('blmove', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _BGroup.parseSlots('brpoplpush', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'b'.bytes
        data6[3] = 'left'.bytes
        data6[4] = 'right'.bytes
        data6[5] = '0'.bytes
        sList = _BGroup.parseSlots('blmove', data6, slotNumber)
        then:
        sList.size() == 2

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
        bGroup.cmd = 'bf.add'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.madd'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.card'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.exists'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.mexists'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.info'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.insert'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.loadchunk'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.reserve'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.scandump'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        bGroup.cmd = 'bf.xxx'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.SYNTAX

        when:
        bGroup.cmd = 'bgsave'
        reply = bGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        bGroup.cmd = 'blmove'
        reply = bGroup.handle()
        then:
        reply == ErrorReply.FORMAT

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
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = '3601'.bytes
        bGroup.cmd = 'brpoplpush'
        bGroup.data = data4
        bGroup.slotWithKeyHashListParsed = _BGroup.parseSlots('brpoplpush', data4, 1)
        reply = bGroup.handle()
        then:
        // timeout exceeds 3600
        reply instanceof ErrorReply

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

    def 'test bf add'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.add a item0')
        then:
        reply == IntegerReply.REPLY_1

        when:
        inMemoryGetSet.remove(slot, 'b')
        def reply2 = bGroup.execute('bf.madd b item0 item1')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 2
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply2 as MultiBulkReply).replies[1] == IntegerReply.REPLY_1

        when:
        reply = bGroup.execute('bf.add a item1')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply2 = bGroup.execute('bf.madd b item3')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 1
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_1

        when:
        reply = bGroup.execute('bf.add a item1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply2 = bGroup.execute('bf.madd b item0 item1')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 2
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_0
        (reply2 as MultiBulkReply).replies[1] == IntegerReply.REPLY_0

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.add a item1')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test bf card'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.card a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        bGroup.execute('bf.add a item0')
        reply = bGroup.execute('bf.card a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.card a')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test bf exist'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.exists a item0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        inMemoryGetSet.remove(slot, 'b')
        def reply2 = bGroup.execute('bf.mexists b item0 item1')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 2
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_0
        (reply2 as MultiBulkReply).replies[1] == IntegerReply.REPLY_0

        when:
        bGroup.execute('bf.add a item0')
        reply = bGroup.execute('bf.exists a item0')
        then:
        reply == IntegerReply.REPLY_1

        when:
        bGroup.execute('bf.madd b item0 item1')
        reply2 = bGroup.execute('bf.mexists b item0 item1')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 2
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply2 as MultiBulkReply).replies[1] == IntegerReply.REPLY_1

        when:
        reply = bGroup.execute('bf.exists a item1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply2 = bGroup.execute('bf.mexists a item2')
        then:
        reply2 instanceof MultiBulkReply
        (reply2 as MultiBulkReply).replies.length == 1
        (reply2 as MultiBulkReply).replies[0] == IntegerReply.REPLY_0

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.exists a item0')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test bf info'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.info a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = bGroup.execute('bf.info a capacity')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        bGroup.execute('bf.add a item0')
        reply = bGroup.execute('bf.info a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10

        when:
        reply = bGroup.execute('bf.info a capacity')
        reply = bGroup.execute('bf.info a size')
        reply = bGroup.execute('bf.info a filters')
        reply = bGroup.execute('bf.info a items')
        reply = bGroup.execute('bf.info a expansion')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        reply = bGroup.execute('bf.info a xxx')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.info a')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test bf insert'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.insert a nocreate items item0')
        then:
        reply == ErrorReply.BF_NOT_EXISTS

        when:
        reply = bGroup.execute('bf.insert a capacity 1000 error 0.01 expansion 2 nonscaling items item0 item1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_1

        when:
        reply = bGroup.execute('bf.insert a capacity 1000 items item0 item1')
        then:
        reply == ErrorReply.BF_ALREADY_EXISTS

        when:
        reply = bGroup.execute('bf.insert a items item0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_0

        when:
        reply = bGroup.execute('bf.insert a capacity 100 items')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bf.insert a error 0.01 capacity')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bf.insert a capacity 100 error')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bf.insert a capacity 100 error 0')
        then:
        // error should > 0 and < 1
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.insert a capacity 100 error 1')
        then:
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.insert a capacity 100 expansion')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bf.insert a capacity 100 expansion 11')
        then:
        // expansion should <= 10
        reply instanceof ErrorReply

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.insert a items item0')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test bf scandump and loadchunk'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.scandump a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        bGroup.execute('bf.add a item0')
        reply = bGroup.execute('bf.scandump a 0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        def encodedAndCompressed = ((reply as MultiBulkReply).replies[1] as BulkReply).raw
        println 'encoded and compressed length: ' + encodedAndCompressed.length
        def data4 = new byte[4][]
        data4[0] = 'bf.loadchunk'.bytes
        data4[1] = 'a'.bytes
        data4[2] = '0'.bytes
        data4[3] = encodedAndCompressed
        bGroup.cmd = 'bf.loadchunk'
        bGroup.data = data4
        reply = bGroup.handle()
        then:
        reply == OKReply.INSTANCE
        bGroup.execute('bf.exists a item0') == IntegerReply.REPLY_1

        when:
        def cv = new CompressedValue()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bf.scandump a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = bGroup.execute('bf.scandump a 1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bf.scandump a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = bGroup.execute('bf.loadchunk a 1 xxx')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bf.loadchunk a a xxx')
        then:
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test bf reserve'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.reserve a 0.01 1000 expansion 2 nonscaling')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = bGroup.execute('bf.reserve a 0.01 1000')
        then:
        reply == ErrorReply.BF_ALREADY_EXISTS

        when:
        reply = bGroup.execute('bf.reserve a 0.01 1000 expansion')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bf.reserve a 0.01 1000 expansion 11')
        then:
        // expansion should <= 10
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.reserve a 0 1000')
        then:
        // error should > 0 and < 1
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.reserve a 1 1000')
        then:
        // error should > 0 and < 1
        reply instanceof ErrorReply
    }

    def 'test blpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

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

        // multi-keys
        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply &&
                    ((result as MultiBulkReply).replies[1] as BulkReply).raw == 'b'.bytes
        }.result

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        reply = bGroup.execute('blpop a b 1')
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

        cleanup:
        localPersist.cleanUp()
    }
}
