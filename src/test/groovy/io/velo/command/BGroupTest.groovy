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

    def 'test parse slot - single key'() {
        given:
        int slotNumber = 128
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        expect:
        _BGroup.parseSlots(cmd, data2, slotNumber).size() == expectedSize

        where:
        cmd           | expectedSize
        'bitcount'    | 1
        'bitfield'    | 1
        'bitfield_ro' | 1
        'bitpos'      | 1
        'bf.add'      | 1
        'bitop'       | 0
        'bgsave'      | 0
        'blmove'      | 0
        'brpoplpush'  | 0
        'blpop'       | 0
        'brpop'       | 0
    }

    def 'test parse slot - multi key'() {
        given:
        int slotNumber = 128
        def data1 = new byte[1][]
        def data5 = new byte[5][]
        data5[1] = 'and'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'c'.bytes
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'b'.bytes
        data6[3] = 'left'.bytes
        data6[4] = 'right'.bytes
        data6[5] = '0'.bytes

        expect:
        _BGroup.parseSlots('bitcount', data1, slotNumber).size() == 0
        _BGroup.parseSlots('bitop', data5, slotNumber).size() == 3
        _BGroup.parseSlots('blpop', data4, slotNumber).size() == 2
        _BGroup.parseSlots('brpop', data4, slotNumber).size() == 2
        _BGroup.parseSlots('blmove', data6, slotNumber).size() == 2
    }

    def 'test handle - format errors'() {
        given:
        def bGroup = new BGroup(null, null, null)
        bGroup.from(BaseCommand.mockAGroup())

        expect:
        bGroup.execute(cmd) == ErrorReply.FORMAT

        where:
        cmd << [
                'bitcount', 'bitpos',
                'bf.add', 'bf.madd', 'bf.card', 'bf.exists', 'bf.mexists',
                'bf.info', 'bf.insert', 'bf.loadchunk', 'bf.reserve', 'bf.scandump',
                'blmove', 'blpop', 'brpop'
        ]
    }

    def 'test handle - special cases'() {
        given:
        def bGroup = new BGroup(null, null, null)
        bGroup.from(BaseCommand.mockAGroup())

        expect:
        bGroup.execute(input) == expected

        where:
        input       | expected
        'bf.xxx'    | ErrorReply.SYNTAX
        'bgsave'    | OKReply.INSTANCE
        'zzz'       | NilReply.INSTANCE
    }

    def 'test handle - brpoplpush timeout'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = '3601'.bytes
        def bGroup = new BGroup('brpoplpush', data4, null)
        bGroup.from(BaseCommand.mockAGroup())
        bGroup.slotWithKeyHashListParsed = _BGroup.parseSlots('brpoplpush', data4, 1)

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        when:
        def reply = bGroup.handle()
        eventloopCurrent.breakEventloop()
        then:
        // no MAX_TIMEOUT_SECONDS cap, large timeout should be accepted as blocking
        reply instanceof AsyncReply

        cleanup:
        localPersist.cleanUp()
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
        reply = bGroup.execute('bitcount >key 0')
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
        reply = bGroup.execute('bitpos >key 0')
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

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('bf.insert b capacity nope items item0')
        then:
        // bad capacity should return NOT_INTEGER
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = bGroup.execute('bf.insert b error nope items item0')
        then:
        // bad error should return NOT_FLOAT
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = bGroup.execute('bf.insert b expansion nope items item0')
        then:
        // bad expansion should return NOT_INTEGER
        reply == ErrorReply.NOT_INTEGER
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

        when:
        reply = bGroup.execute('bf.reserve a nope 1000')
        then:
        // bad fpp should return NOT_FLOAT
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = bGroup.execute('bf.reserve a 0.01 nope')
        then:
        // bad capacity should return NOT_INTEGER
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test bf reserve rejects non-positive expansion'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.reserve a 0.01 100 expansion 0')
        then:
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.reserve b 0.01 100 expansion -1')
        then:
        reply instanceof ErrorReply
    }

    def 'test bf insert rejects non-positive expansion'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = bGroup.execute('bf.insert a capacity 1000 error 0.01 expansion 0 items item0')
        then:
        reply instanceof ErrorReply

        when:
        reply = bGroup.execute('bf.insert a capacity 1000 error 0.01 expansion -1 items item0')
        then:
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
        // timeout 0 = block indefinitely
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('blpop a 0')
        then:
        // timeout 0 = block indefinitely (empty list)
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('blpop a 0')
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).raw == 'a'.bytes
        LGroup.getRedisList(bGroup.slotWithKeyHashListParsed.getFirst(), bGroup) == null

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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        // multi-keys
        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('blpop a b 0')
        then:
        // timeout 0 = block indefinitely, promise stays pending
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply &&
                    ((result as MultiBulkReply).replies[1] as BulkReply).raw == 'b'.bytes
        }.result

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('blpop a b 0')
        then:
        // timeout 0 = block indefinitely, promise stays pending
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        reply = bGroup.execute('blpop a b 1')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

        when:
        reply = bGroup.execute('blpop a x')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // negative timeout should be rejected
        reply = bGroup.execute('blpop a -1')
        then:
        reply instanceof ErrorReply

        when:
        // NaN should be rejected
        reply = bGroup.execute('blpop a nan')
        then:
        reply instanceof ErrorReply

        when:
        // Infinity should be rejected
        reply = bGroup.execute('blpop a inf')
        then:
        reply instanceof ErrorReply

        when:
        // fractional timeout should be accepted
        inMemoryGetSet.remove(slot, 'a')
        reply = bGroup.execute('blpop a 0.5')
        then:
        reply instanceof AsyncReply

        when:
        reply = bGroup.execute('blpop >key 0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        localPersist.cleanUp()
    }
}
