package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.bytebuf.ByteBuf
import io.activej.promise.Promise
import io.velo.MultiWorkerServer
import io.velo.decode.Request
import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdOutputStream
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisBF
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

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
        // bitfield, bitfield_ro and bitop are not implemented, no slot is parsed for them
        'bitfield'    | 0
        'bitfield_ro' | 0
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
        _BGroup.parseSlots('bitop', data5, slotNumber).size() == 0
        _BGroup.parseSlots('blpop', data4, slotNumber).size() == 2
        _BGroup.parseSlots('brpop', data4, slotNumber).size() == 2
        _BGroup.parseSlots('blmove', data6, slotNumber).size() == 2
    }

    def 'test parse slot - blocking sorted set pops'() {
        given:
        int slotNumber = 128
        def data1 = new byte[1][]

        when: 'bzpopmin/bzpopmax: last arg is the timeout, all previous args are keys'
        def dataBzPop = new byte[3][]
        dataBzPop[1] = 'a'.bytes
        dataBzPop[2] = '1.0'.bytes
        def dataBzPopMulti = new byte[4][]
        dataBzPopMulti[1] = 'a'.bytes
        dataBzPopMulti[2] = 'b'.bytes
        dataBzPopMulti[3] = '1.0'.bytes
        then:
        _BGroup.parseSlots('bzpopmax', dataBzPop, slotNumber).size() == 1
        _BGroup.parseSlots('bzpopmin', dataBzPop, slotNumber).size() == 1
        _BGroup.parseSlots('bzpopmax', dataBzPopMulti, slotNumber).size() == 2
        _BGroup.parseSlots('bzpopmin', dataBzPopMulti, slotNumber).size() == 2
        // too few args, no slot is parsed
        _BGroup.parseSlots('bzpopmin', data1, slotNumber).size() == 0

        when: 'bzmpop: timeout + numkeys + keys + direction'
        def dataBzMpop = new byte[5][]
        dataBzMpop[1] = '1.0'.bytes
        dataBzMpop[2] = '1'.bytes
        dataBzMpop[3] = 'a'.bytes
        dataBzMpop[4] = 'min'.bytes
        then:
        _BGroup.parseSlots('bzmpop', dataBzMpop, slotNumber).size() == 1
        // too few args
        _BGroup.parseSlots('bzmpop', data1, slotNumber).size() == 0

        when: 'invalid numkeys, no slot is parsed'
        dataBzMpop[2] = 'x'.bytes
        then:
        _BGroup.parseSlots('bzmpop', dataBzMpop, slotNumber).size() == 0

        when: 'numkeys zero, no slot is parsed'
        dataBzMpop[2] = '0'.bytes
        then:
        _BGroup.parseSlots('bzmpop', dataBzMpop, slotNumber).size() == 0

        when: 'numkeys greater than the provided keys, no slot is parsed'
        dataBzMpop[2] = '2'.bytes
        then:
        _BGroup.parseSlots('bzmpop', dataBzMpop, slotNumber).size() == 0
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
        (bGroup.execute(input) instanceof ErrorReply) == (expected instanceof ErrorReply) && bGroup.execute(input) == expected

        where:
        input       | expected
        'bf.xxx'    | ErrorReply.SYNTAX
        'bgsave'    | OKReply.INSTANCE
    }

    def 'test handle - unknown command'() {
        given:
        def bGroup = new BGroup(null, null, null)
        bGroup.from(BaseCommand.mockAGroup())

        expect:
        def reply = bGroup.execute('zzz')
        reply instanceof ErrorReply
        (reply as ErrorReply).message == ErrorReply.UNKNOWN_COMMAND('zzz').message
    }

    def 'test unimplemented bitmap commands report unknown command'() {
        given:
        def bGroup = new BGroup(null, null, null)
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def bitopData = new byte[4][]
        bitopData[0] = 'bitop'.bytes
        bitopData[1] = 'and'.bytes
        bitopData[2] = 'd'.bytes
        bitopData[3] = 's'.bytes
        def bitfieldData = new byte[2][]
        bitfieldData[0] = 'bitfield'.bytes
        bitfieldData[1] = 's'.bytes

        expect: 'bitop and bitfield are not implemented, they must not answer with a nil reply'
        (bGroup.execute('bitop and d s') as ErrorReply).message == ErrorReply.UNKNOWN_COMMAND('bitop').message
        (bGroup.execute('bitfield s get i8 0') as ErrorReply).message == ErrorReply.UNKNOWN_COMMAND('bitfield').message
        (bGroup.execute('bitfield_ro s get i8 0') as ErrorReply).message == ErrorReply.UNKNOWN_COMMAND('bitfield_ro').message

        and: 'and they must not be advertised as available commands'
        CommandRegistry.get('bitop') == null
        CommandRegistry.get('bitfield') == null

        and: 'no slot is parsed for them'
        bGroup.parseSlots('bitop', bitopData, 1).isEmpty()
        bGroup.parseSlots('bitfield', bitfieldData, 1).isEmpty()
        bGroup.parseSlots('bitfield_ro', bitfieldData, 1).isEmpty()
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

        when: 'BIT indexes the most significant bit of the first byte as position 0'
        def cvAsymmetric = Mock.prepareCompressedValueList(1)[0]
        cvAsymmetric.compressedData = [(byte) 0x80] as byte[]
        inMemoryGetSet.put(slot, 'bitkey', 0, cvAsymmetric)
        reply = bGroup.execute('bitcount bitkey 0 0 bit')
        then:
        (reply as IntegerReply).integer == 1

        when:
        reply = bGroup.execute('bitcount bitkey 1 1 bit')
        then:
        (reply as IntegerReply).integer == 0

        when:
        reply = bGroup.execute('bitcount bitkey 7 7 bit')
        then:
        (reply as IntegerReply).integer == 0

        when: 'BIT counting agrees with BITPOS on the same key'
        reply = bGroup.execute('bitpos bitkey 1 0 7 bit')
        then:
        (reply as IntegerReply).integer == 0

        when: 'LSB first byte counts in BYTE mode are unaffected'
        cvAsymmetric.compressedData = [(byte) 0x01] as byte[]
        inMemoryGetSet.put(slot, 'bitkey', 0, cvAsymmetric)
        reply = bGroup.execute('bitcount bitkey 0 0 bit')
        then:
        (reply as IntegerReply).integer == 0

        when:
        reply = bGroup.execute('bitcount bitkey 7 7 bit')
        then:
        (reply as IntegerReply).integer == 1

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = bGroup.execute('bitcount a')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        reply = bGroup.execute('bitcount a b 5')
        then:
        reply == ErrorReply.INVALID_INTEGER

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

        when: 'missing key is an infinite array of zero bits, so bit 0 is at position 0'
        reply = bGroup.execute('bitpos a 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

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

        when: 'all bits are ones, the right of the string is padded with zeros when no range or start only'
        def cvOnes = Mock.prepareCompressedValueList(1)[0]
        cvOnes.compressedData = [0xff, 0xff, 0xff] as byte[]
        inMemoryGetSet.put(slot, 'b', 0, cvOnes)
        reply = bGroup.execute('bitpos b 0')
        then:
        (reply as IntegerReply).integer == 24

        when: 'start only'
        reply = bGroup.execute('bitpos b 0 0')
        then:
        (reply as IntegerReply).integer == 24

        when: 'both start and end given, no clear bit in range'
        reply = bGroup.execute('bitpos b 0 0 2')
        then:
        (reply as IntegerReply).integer == -1

        when: 'both start and end given with BIT unit, no clear bit in range'
        reply = bGroup.execute('bitpos b 0 0 5 bit')
        then:
        (reply as IntegerReply).integer == -1

        when: 'searching a set bit never uses the padding'
        def cvZeros = Mock.prepareCompressedValueList(1)[0]
        cvZeros.compressedData = [0x00, 0x00, 0x00] as byte[]
        inMemoryGetSet.put(slot, 'c', 0, cvZeros)
        reply = bGroup.execute('bitpos c 1')
        then:
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
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bitpos a 1 _ _ _ _')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = bGroup.execute('bitpos a 2 0 -1')
        then:
        reply == new ErrorReply('The bit argument must be 1 or 0.')

        when:
        reply = bGroup.execute('bitpos a 10 0 -1')
        then:
        reply == new ErrorReply('The bit argument must be 1 or 0.')

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
        // bad capacity should return INVALID_INTEGER
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bf.insert b error nope items item0')
        then:
        // bad error should return INVALID_FLOAT
        reply == ErrorReply.INVALID_FLOAT

        when:
        reply = bGroup.execute('bf.insert b expansion nope items item0')
        then:
        // bad expansion should return INVALID_INTEGER
        reply == ErrorReply.INVALID_INTEGER

        when: 'zero capacity is rejected before reaching guava'
        inMemoryGetSet.remove(slot, 'c')
        reply = bGroup.execute('bf.insert c capacity 0 items item0')
        then:
        reply == ErrorReply.CAPACITY_NOT_POSITIVE
        bGroup.execute('bf.card c') == IntegerReply.REPLY_0

        when: 'negative capacity is rejected with a fixed error, not a guava exception'
        reply = bGroup.execute('bf.insert d capacity -1 items item0')
        then:
        reply == ErrorReply.CAPACITY_NOT_POSITIVE
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
        def encodedAndCompressed = ((reply as MultiBulkReply).replies[1] as BulkReply).bufferAsHttp().asArray()
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
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bf.loadchunk a 1 xxx')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = bGroup.execute('bf.loadchunk a a xxx')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when: 'chunk without any payload after the length prefix'
        reply = loadchunk(bGroup, declaredLength(64, new byte[0]))
        then:
        reply == ErrorReply.BAD_DATA_FORMAT

        when: 'client declared length far bigger than any allowed value'
        reply = loadchunk(bGroup, declaredLength(100_000_000))
        then:
        // no allocation driven by client input, fixed error instead of zstd/OutOfMemory
        reply == ErrorReply.BAD_DATA_FORMAT

        when: 'client declared negative length'
        reply = loadchunk(bGroup, declaredLength(-1))
        then:
        reply == ErrorReply.BAD_DATA_FORMAT

        when: 'client declared zero length'
        reply = loadchunk(bGroup, declaredLength(0))
        then:
        reply == ErrorReply.BAD_DATA_FORMAT

        when: 'declared length does not match the real zstd frame content size'
        def encodedReal = new RedisBF(100, 0.01, (byte) 2, false).encode()
        reply = loadchunk(bGroup, declaredLength(encodedReal.length + 64, Zstd.compress(encodedReal)))
        then:
        reply == ErrorReply.BAD_DATA_FORMAT

        when: 'valid frame that does not declare its content size'
        reply = loadchunk(bGroup, declaredLength(encodedReal.length, compressWithoutContentSize(encodedReal)))
        then:
        reply == OKReply.INSTANCE
        bGroup.execute('bf.exists a item0') == IntegerReply.REPLY_0
    }

    private static byte[] compressWithoutContentSize(byte[] raw) {
        def baos = new ByteArrayOutputStream()
        def zos = new ZstdOutputStream(baos)
        zos.write(raw)
        zos.close()
        return baos.toByteArray()
    }

    private static byte[] declaredLength(int length, byte[] payload = 'junk-payload'.bytes) {
        def bytes = new byte[4 + payload.length]
        ByteBuffer.wrap(bytes).putInt(length).put(payload)
        return bytes
    }

    private static Reply loadchunk(BGroup bGroup, byte[] dumpBytes) {
        return bGroup.execute('bf.loadchunk a 0 x', { byte[][] data -> data[3] = dumpBytes } as BaseCommand.DataArrayReplacer)
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
        // bad fpp should return INVALID_FLOAT
        reply == ErrorReply.INVALID_FLOAT

        when:
        reply = bGroup.execute('bf.reserve a 0.01 nope')
        then:
        // bad capacity should return INVALID_INTEGER
        reply == ErrorReply.INVALID_INTEGER

        when: 'zero capacity is rejected before reaching guava'
        inMemoryGetSet.remove(slot, 'c')
        reply = bGroup.execute('bf.reserve c 0.01 0')
        then:
        reply == ErrorReply.CAPACITY_NOT_POSITIVE
        bGroup.execute('bf.card c') == IntegerReply.REPLY_0

        when: 'negative capacity is rejected with a fixed error, not a guava exception'
        reply = bGroup.execute('bf.reserve d 0.01 -5')
        then:
        reply == ErrorReply.CAPACITY_NOT_POSITIVE
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
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == 'a'
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
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        // multi-keys
        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('blpop a b 0')
        then:
        // timeout 0 = block indefinitely, promise stays pending
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when: 'the recheck delivery releases the waiter from every key it was registered on'
        BlockingList.clearBlockingListPromisesForAllKeys()
        def recheckPromise = new SettablePromise<Reply>()
        def recheckSlotA = BaseCommand.slot('recheck-a', (short) 1)
        def recheckSlotB = BaseCommand.slot('recheck-b', (short) 1)
        def recheckOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(recheckPromise, null, true,
                System.currentTimeMillis(), null, 0, [recheckSlotA, recheckSlotB])
        BlockingList.addOne('recheck-a', recheckOne)
        BlockingList.addOne('recheck-b', recheckOne)
        def recheckCv = Mock.prepareCompressedValueList(1)[0]
        recheckCv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def recheckRl = new RedisList()
        recheckRl.addLast('value'.bytes)
        recheckCv.compressedData = recheckRl.encode()
        inMemoryGetSet.put(slot, 'recheck-a', 0, recheckCv)
        def recheckCompleted = BGroup.completeIfListNotEmpty(recheckSlotA, 'recheck-a'.bytes, bGroup, bGroup.dictMap, recheckOne, true)
        then:
        recheckCompleted
        recheckPromise.isComplete()
        BlockingList.blockingClientCount() == 0
        BlockingList.blockingKeyCount() == 0

        when: 'the same key twice, the client is registered once per distinct key'
        inMemoryGetSet.remove(slot, 'recheck-a')
        BlockingList.clearBlockingListPromisesForAllKeys()
        inMemoryGetSet.remove(slot, 'dup')
        reply = bGroup.execute('blpop dup dup 0')
        then:
        reply instanceof AsyncReply
        BlockingList.blockingClientCount() == 1
        BlockingList.blockingKeyCount() == 1

        when: 'second key holds an existing but empty list, still blocks'
        def cvEmptyList = Mock.prepareCompressedValueList(1)[0]
        cvEmptyList.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvEmptyList.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'b', 0, cvEmptyList)
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()
        inMemoryGetSet.remove(slot, 'b')

        when:
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = bGroup.execute('blpop a b 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply &&
                    ((result as MultiBulkReply).replies[1] as BulkReply).asString() == 'b'
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
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        when:
        reply = bGroup.execute('blpop a x')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when:
        // negative timeout should be rejected
        reply = bGroup.execute('blpop a -1')
        then:
        reply == new ErrorReply('timeout is negative')

        when:
        // NaN should be rejected
        reply = bGroup.execute('blpop a NaN')
        then:
        reply instanceof ErrorReply

        when:
        // Infinity should be rejected
        reply = bGroup.execute('blpop a Infinity')
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

        when: 'a push lands between the check and the registration, the re-check completes the reply'
        inMemoryGetSet.remove(slot, 'race')
        def promiseForRace = new SettablePromise<Reply>()
        def oneForRace = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promiseForRace, null, true, System.currentTimeMillis(), null)
        BlockingList.addOne('race', oneForRace)
        def completedWhenEmpty = BGroup.completeIfListNotEmpty(BaseCommand.slot('race', 1), 'race'.bytes, bGroup, bGroup.dictMap, oneForRace, true)
        then:
        !completedWhenEmpty
        !promiseForRace.isComplete()

        when:
        def cvRace = Mock.prepareCompressedValueList(1)[0]
        cvRace.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlRace = new RedisList()
        rlRace.addFirst('pushed-in-window'.bytes)
        cvRace.compressedData = rlRace.encode()
        inMemoryGetSet.put(slot, 'race', 0, cvRace)
        BlockingList.addOne('race', oneForRace)
        def completedWhenNotEmpty = BGroup.completeIfListNotEmpty(BaseCommand.slot('race', 1), 'race'.bytes, bGroup, bGroup.dictMap, oneForRace, true)
        then:
        completedWhenNotEmpty
        promiseForRace.isComplete()
        promiseForRace.getResult() instanceof MultiBulkReply
        (promiseForRace.getResult() as MultiBulkReply).replies[1] instanceof BulkReply
        ((promiseForRace.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'pushed-in-window'

        when: 'an already completed promise is left alone'
        def completedTwice = BGroup.completeIfListNotEmpty(BaseCommand.slot('race', 1), 'race'.bytes, bGroup, bGroup.dictMap, oneForRace, true)
        then:
        !completedTwice

        when: 'a later key holds another type, the multi key check fails and the error is forwarded'
        inMemoryGetSet.remove(slot, 'wtypea')
        def cvWrongTypeKey = Mock.prepareCompressedValueList(1)[0]
        cvWrongTypeKey.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvWrongTypeKey.compressedData = 'not-a-list'.bytes
        inMemoryGetSet.put(slot, 'wtypeb', 0, cvWrongTypeKey)
        reply = bGroup.execute('blpop wtypea wtypeb 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() == null
        (reply as AsyncReply).settablePromise.getResult() == ErrorReply.WRONG_TYPE
        new String(encodeAsyncReply(reply as AsyncReply).getResult().asArray()) ==
                '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

        when: 'same for brpop'
        inMemoryGetSet.put(slot, 'wtypeb', 0, cvWrongTypeKey)
        reply = bGroup.execute('brpop wtypea wtypeb 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() == null
        (reply as AsyncReply).settablePromise.getResult() == ErrorReply.WRONG_TYPE
        new String(encodeAsyncReply(reply as AsyncReply).getResult().asArray()) ==
                '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
        inMemoryGetSet.remove(slot, 'wtypeb')

        when: 'corrupt list payloads still use generic errors, not WRONGTYPE'
        def cvCorruptPop = Mock.prepareCompressedValueList(1)[0]
        cvCorruptPop.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvCorruptPop.compressedData = 'corrupted-list-payload'.bytes
        inMemoryGetSet.put(slot, 'wtypeb', 0, cvCorruptPop)
        for (String popCommand : ['blpop', 'brpop']) {
            def corruptReply = bGroup.execute(popCommand + ' wtypea wtypeb 0') as AsyncReply
            assert corruptReply.settablePromise.getException() != null
            assert new String(encodeAsyncReply(corruptReply).getResult().asArray()).startsWith('-ERR ')
        }
        inMemoryGetSet.remove(slot, 'wtypeb')
        then:
        noExceptionThrown()

        when: 'the last element is taken when popping from the right'
        def promiseForRight = new SettablePromise<Reply>()
        def oneForRight = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promiseForRight, null, false, System.currentTimeMillis(), null)
        BlockingList.addOne('raceright', oneForRight)
        def rlRight = new RedisList()
        rlRight.addFirst('first'.bytes)
        rlRight.addLast('last'.bytes)
        cvRace.compressedData = rlRight.encode()
        inMemoryGetSet.put(slot, 'raceright', 0, cvRace)
        def completedFromRight = BGroup.completeIfListNotEmpty(BaseCommand.slot('raceright', 1), 'raceright'.bytes, bGroup, bGroup.dictMap, oneForRight, false)
        then:
        completedFromRight
        ((promiseForRight.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'last'

        when: 'an existing but empty list does not complete the promise'
        def promiseForEmpty = new SettablePromise<Reply>()
        def oneForEmpty = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promiseForEmpty, null, true, System.currentTimeMillis(), null)
        BlockingList.addOne('emptyone', oneForEmpty)
        def rlEmpty = new RedisList()
        cvRace.compressedData = rlEmpty.encode()
        inMemoryGetSet.put(slot, 'emptyone', 0, cvRace)
        def completedWhenEmptyList = BGroup.completeIfListNotEmpty(BaseCommand.slot('emptyone', 1), 'emptyone'.bytes, bGroup, bGroup.dictMap, oneForEmpty, true)
        then:
        !completedWhenEmptyList
        !promiseForEmpty.isComplete()
        BlockingList.removeOne('emptyone', oneForEmpty)

        when: 'the delivery arbiter is one-shot'
        def casPromise = new SettablePromise<Reply>()
        def casOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(casPromise, null, true, System.currentTimeMillis(), null)
        casOne.tryMarkDelivered()
        then:
        !casOne.tryMarkDelivered()

        when: 'a waiter that lost the delivery race never pops the list'
        def cvCas = Mock.prepareCompressedValueList(1)[0]
        cvCas.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlCas = new RedisList()
        rlCas.addFirst('must-survive'.bytes)
        cvCas.compressedData = rlCas.encode()
        inMemoryGetSet.put(slot, 'cas', 0, cvCas)
        def lostPromise = new SettablePromise<Reply>()
        def lostOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(lostPromise, null, true, System.currentTimeMillis(), null)
        lostOne.tryMarkDelivered()
        def lostCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('cas', 1), 'cas'.bytes, bGroup, bGroup.dictMap, lostOne, true)
        then:
        !lostCompleted
        !lostPromise.isComplete()

        when: 'a fresh waiter still gets the element the loser never popped'
        def winPromise = new SettablePromise<Reply>()
        def winOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(winPromise, null, true, System.currentTimeMillis(), null)
        def winCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('cas', 1), 'cas'.bytes, bGroup, bGroup.dictMap, winOne, true)
        then:
        winCompleted
        winPromise.isComplete()
        ((winPromise.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'must-survive'

        when: 'a foreign completion during the re-check does not lose the element'
        def foreignPromise = new SettablePromise<Reply>()
        def hookedGetSet = new InMemoryGetSet() {
            @Override
            io.velo.persist.OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash) {
                if (key == 'foreign' && !foreignPromise.isComplete()) {
                    foreignPromise.set(NilReply.INSTANCE)
                }
                return super.getBuf(slot, key, bucketIndex, keyHash)
            }
        }
        def bGroupForeign = new BGroup(null, null, null)
        bGroupForeign.byPassGetSet = hookedGetSet
        bGroupForeign.from(BaseCommand.mockAGroup())
        def cvForeign = Mock.prepareCompressedValueList(1)[0]
        cvForeign.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlForeign = new RedisList()
        rlForeign.addFirst('foreign-window'.bytes)
        cvForeign.compressedData = rlForeign.encode()
        hookedGetSet.put(slot, 'foreign', 0, cvForeign)
        def foreignOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(foreignPromise, null, true, System.currentTimeMillis(), null)
        def foreignCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('foreign', 1), 'foreign'.bytes, bGroupForeign, bGroupForeign.dictMap, foreignOne, true)
        then:
        !foreignCompleted
        foreignPromise.isComplete()
        foreignPromise.getResult() == NilReply.INSTANCE

        when: 'the element the loser rolled back is still deliverable'
        def afterForeignPromise = new SettablePromise<Reply>()
        def afterForeignOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(afterForeignPromise, null, true, System.currentTimeMillis(), null)
        def afterForeignCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('foreign', 1), 'foreign'.bytes, bGroupForeign, bGroupForeign.dictMap, afterForeignOne, true)
        then:
        afterForeignCompleted
        ((afterForeignPromise.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'foreign-window'

        when: 'a foreign completion during the right-pop re-check also rolls the element back'
        def foreignRightPromise = new SettablePromise<Reply>()
        def hookedRightGetSet = new InMemoryGetSet() {
            @Override
            io.velo.persist.OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash) {
                if (key == 'foreign-right' && !foreignRightPromise.isComplete()) {
                    foreignRightPromise.set(NilReply.INSTANCE)
                }
                return super.getBuf(slot, key, bucketIndex, keyHash)
            }
        }
        def bGroupForeignRight = new BGroup(null, null, null)
        bGroupForeignRight.byPassGetSet = hookedRightGetSet
        bGroupForeignRight.from(BaseCommand.mockAGroup())
        def cvForeignRight = Mock.prepareCompressedValueList(1)[0]
        cvForeignRight.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlForeignRight = new RedisList()
        rlForeignRight.addFirst('foreign-right-val'.bytes)
        cvForeignRight.compressedData = rlForeignRight.encode()
        hookedRightGetSet.put(slot, 'foreign-right', 0, cvForeignRight)
        def foreignRightOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(foreignRightPromise, null, false, System.currentTimeMillis(), null)
        def foreignRightCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('foreign-right', 1), 'foreign-right'.bytes, bGroupForeignRight, bGroupForeignRight.dictMap, foreignRightOne, false)
        then:
        !foreignRightCompleted
        foreignRightPromise.getResult() == NilReply.INSTANCE

        when: 'the right-pop loser rolled its element back to the tail'
        def afterForeignRightPromise = new SettablePromise<Reply>()
        def afterForeignRightOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(afterForeignRightPromise, null, false, System.currentTimeMillis(), null)
        def afterForeignRightCompleted = BGroup.completeIfListNotEmpty(BaseCommand.slot('foreign-right', 1), 'foreign-right'.bytes, bGroupForeignRight, bGroupForeignRight.dictMap, afterForeignRightOne, false)
        then:
        afterForeignRightCompleted
        ((afterForeignRightPromise.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'foreign-right-val'

        when: 'a timeout that fires after a delivery cannot nil the reply'
        inMemoryGetSet.remove(slot, 'timeout-after-delivery')
        def asyncTimeout = bGroup.execute('blpop timeout-after-delivery 0.2') as AsyncReply
        def earlyValues = new byte[1][]
        earlyValues[0] = 'early'.bytes
        def earlyConsumed = BlockingList.setReplyLPushIfBlockingListExist('timeout-after-delivery', earlyValues)
        then:
        earlyConsumed != null && earlyConsumed.length == 0
        asyncTimeout.settablePromise.isComplete()
        when:
        Thread.sleep(400)
        eventloopCurrent.run()
        then:
        asyncTimeout.settablePromise.getResult() instanceof MultiBulkReply

        when: 'a multi-key timeout that fires after a delivery cannot nil the reply'
        inMemoryGetSet.remove(slot, 'multi-timeout-1')
        inMemoryGetSet.remove(slot, 'multi-timeout-2')
        def asyncMultiTimeout = bGroup.execute('blpop multi-timeout-1 multi-timeout-2 0.2') as AsyncReply
        def multiValues = new byte[1][]
        multiValues[0] = 'multi-early'.bytes
        def multiConsumed = BlockingList.setReplyLPushIfBlockingListExist('multi-timeout-1', multiValues)
        then:
        multiConsumed != null && multiConsumed.length == 0
        asyncMultiTimeout.settablePromise.isComplete()
        when:
        Thread.sleep(400)
        eventloopCurrent.run()
        then:
        asyncMultiTimeout.settablePromise.getResult() instanceof MultiBulkReply

        when: 'two slot workers, the waiter must be registered on the worker owning the pushed key'
        localPersist.cleanUp()
        def workers = [startWorkerThread('velo-test-worker-1'), startWorkerThread('velo-test-worker-2')]
        def loop1 = workers[0][0]
        def loop2 = workers[1][0]
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        localPersist.initSlotRuntimeBindings([loop1, loop2] as Eventloop[], new io.velo.RequestHandler[2],
                [workers[0][1] as long, workers[1][1] as long] as long[])
        localPersist.oneSlot(0 as short)
        localPersist.oneSlot(1 as short)
        BlockingList.initBySlotWorkerEventloopArray([loop1, loop2] as Eventloop[])
        BlockingList.clearBlockingListPromisesForAllKeys()

        and: 'two keys hashed into two different slots'
        def bGroupWorkers = new BGroup(null, null, null)
        bGroupWorkers.from(BaseCommand.mockAGroup(0 as byte, 2 as byte, 2 as short))
        def keyOnWorker1 = (1..500).collect { 'multi1_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def keyOnWorker2 = (1..500).collect { 'multi2_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }

        and: 'blpop on worker-1 for one key on worker-1 and one key on worker-2'
        def asyncReplyCross = []
        def registered = new CountDownLatch(1)
        loop1.execute({
            asyncReplyCross << bGroupWorkers.execute("blpop ${keyOnWorker1} ${keyOnWorker2} 0")
            registered.countDown()
        })
        registered.await(5, TimeUnit.SECONDS)
        Thread.sleep(300)
        and: 'push to the key owned by worker-2'
        def pushedOnWorker2 = []
        def pushDone = new CountDownLatch(1)
        loop2.execute({
            def values = new byte[1][]
            values[0] = 'from-worker-2'.bytes
            pushedOnWorker2 << BlockingList.setReplyLPushIfBlockingListExist(keyOnWorker2, values)
            pushDone.countDown()
        })
        pushDone.await(5, TimeUnit.SECONDS)
        and: 'the blocking reply is completed'
        def crossPromise = (asyncReplyCross[0] as AsyncReply).settablePromise
        def crossReply = null
        crossPromise.whenResult { r -> crossReply = r }
        for (int i = 0; i < 100 && crossReply == null; i++) {
            Thread.sleep(50)
        }
        then:
        // empty array means every pushed value was handed to a waiting client
        pushedOnWorker2[0] != null && (pushedOnWorker2[0] as byte[][]).length == 0
        crossReply instanceof MultiBulkReply
        ((crossReply as MultiBulkReply).replies[0] as BulkReply).asString() == keyOnWorker2
        ((crossReply as MultiBulkReply).replies[1] as BulkReply).asString() == 'from-worker-2'

        cleanup:
        BlockingList.clearBlockingListPromisesForAllKeys()
        // rebind BlockingList to this thread again, later tests in this spec run here
        BlockingList.initBySlotWorkerEventloopArray([Eventloop.builder().withCurrentThread().build()] as Eventloop[])
        workers?.each { w ->
            try {
                w[0].breakEventloop()
                w[2].join(3000)
            } catch (Throwable ignored) {
            }
        }
        localPersist.cleanUp()
    }

    private static Promise<ByteBuf> encodeAsyncReply(AsyncReply reply) {
        def transfer = MultiWorkerServer.getDeclaredMethod('transferAsyncReply', Request, AsyncReply, Boolean.TYPE)
        transfer.accessible = true
        def request = new Request(['blpop'.bytes] as byte[][], false, false)
        return transfer.invoke(new MultiWorkerServer(), request, reply, false) as Promise<ByteBuf>
    }

    private static List startWorkerThread(String name) {
        var loopQ = new LinkedBlockingQueue<Eventloop>()
        def eventloop = Eventloop.builder().withIdleInterval(Duration.ofMillis(20)).build()
        eventloop.keepAlive(true)
        def thread = Thread.start {
            loopQ.put(eventloop)
            eventloop.run()
        }
        def loop = loopQ.take()
        def ready = new CountDownLatch(1)
        loop.execute { ready.countDown() }
        ready.await(5, TimeUnit.SECONDS)
        return [loop, loop.getEventloopThread().threadId(), thread]
    }

    def 'test brpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when: 'second key has values, the last one is taken'
        inMemoryGetSet.remove(slot, 'a')
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('first'.bytes)
        rl.addLast('last'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'b', 0, cv)
        def reply = bGroup.execute('brpop a b 0')
        then:
        reply instanceof AsyncReply
        // same worker, the promise is already complete when the reply is returned
        def brpopResult = (reply as AsyncReply).settablePromise.getResult()
        brpopResult instanceof MultiBulkReply
        ((brpopResult as MultiBulkReply).replies[0] as BulkReply).asString() == 'b'
        ((brpopResult as MultiBulkReply).replies[1] as BulkReply).asString() == 'last'

        when: 'second key is empty, the waiter stays pending'
        def cvEmpty = Mock.prepareCompressedValueList(1)[0]
        cvEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvEmpty.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'b', 0, cvEmpty)
        reply = bGroup.execute('brpop a b 0')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        cleanup:
        localPersist.cleanUp()
    }

    def 'test blmpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when: 'format error - too few args'
        def reply = bGroup.execute('blmpop')
        then:
        reply == ErrorReply.FORMAT

        when: 'timeout not number'
        reply = bGroup.execute('blmpop abc 1 mylist LEFT')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when: 'negative timeout'
        reply = bGroup.execute('blmpop -1 1 mylist LEFT')
        then:
        reply instanceof ErrorReply

        when: 'numkeys not integer'
        reply = bGroup.execute('blmpop 0 abc 1 mylist LEFT')
        then:
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'numkeys zero'
        reply = bGroup.execute('blmpop 1 0 mylist LEFT')
        then:
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'wrong direction'
        reply = bGroup.execute('blmpop 1 1 mylist UP')
        then:
        reply == ErrorReply.SYNTAX

        when: 'count not integer'
        reply = bGroup.execute('blmpop 1 1 mylist LEFT COUNT abc')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'count zero rejected'
        reply = bGroup.execute('blmpop 1 1 mylist LEFT COUNT 0')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'wrong type key returns WRONGTYPE'
        inMemoryGetSet.remove(slot, 'notlist')
        def cvWrong = new CompressedValue()
        cvWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvWrong.compressedData = 'hello'.bytes
        inMemoryGetSet.put(slot, 'notlist', 0, cvWrong)
        reply = bGroup.execute('blmpop 1 1 notlist LEFT')
        then:
        reply == ErrorReply.WRONG_TYPE

        when: 'key too long'
        reply = bGroup.execute('blmpop 1 1 >key LEFT')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'immediate pop from non-empty list'
        inMemoryGetSet.remove(slot, 'mylist')
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addLast('a'.bytes)
        rl.addLast('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'mylist', 0, cv)
        reply = bGroup.execute('blmpop 1 1 mylist LEFT COUNT 1')
        def mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        mb.replies.length == 2
        (mb.replies[0] as BulkReply).asString() == 'mylist'
        (mb.replies[1] as MultiBulkReply).replies.length == 1
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'a'

        when: 'right pop'
        reply = bGroup.execute('blmpop 1 1 mylist RIGHT')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'b'

        when: 'all keys empty — block with timeout'
        inMemoryGetSet.remove(slot, 'x')
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def reactorMpop = eventloopCurrent
        reply = bGroup.execute('blmpop 1 1 x LEFT')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        when: 'a push completes the reply before the timeout fires, the timeout changes nothing'
        inMemoryGetSet.remove(slot, 'x')
        reply = bGroup.execute('blmpop 0.5 1 x LEFT')
        def inTimeValues = new byte[1][]
        inTimeValues[0] = 'in-time'.bytes
        BlockingList.setReplyLPushIfBlockingListExist('x', inTimeValues)
        Thread.sleep(1000)
        reactorMpop.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.isComplete()
        ((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[1] instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[1] as MultiBulkReply)
                .replies[0] instanceof BulkReply
        ((((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[1] as MultiBulkReply)
                .replies[0] as BulkReply).asString() == 'in-time'

        when: 'first key empty, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlB = new RedisList()
        rlB.addLast('x'.bytes)
        cvB.compressedData = rlB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = bGroup.execute('blmpop 1 2 a b LEFT COUNT 3')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        (mb.replies[0] as BulkReply).asString() == 'b'
        (mb.replies[1] as MultiBulkReply).replies.length == 1
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'x'

        when: 'second key holds another type, WRONGTYPE is returned'
        def cvWrongType = Mock.prepareCompressedValueList(1)[0]
        cvWrongType.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvWrongType.compressedData = 'notalist'.bytes
        inMemoryGetSet.put(slot, 'b', 0, cvWrongType)
        reply = bGroup.execute('blmpop 1 2 a b LEFT')
        then:
        reply == ErrorReply.WRONG_TYPE
        inMemoryGetSet.remove(slot, 'b')

        when: 'first key holds an existing but empty list, the second key is used'
        def cvFirstEmpty = Mock.prepareCompressedValueList(1)[0]
        cvFirstEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvFirstEmpty.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'a', 0, cvFirstEmpty)
        def cvSecondHas = Mock.prepareCompressedValueList(1)[0]
        cvSecondHas.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlSecondHas = new RedisList()
        rlSecondHas.addLast('from-second'.bytes)
        cvSecondHas.compressedData = rlSecondHas.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvSecondHas)
        reply = bGroup.execute('blmpop 1 2 a b LEFT')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'b'

        when: 'a middle key holds an existing but empty list, the last key is used'
        inMemoryGetSet.remove(slot, 'a')
        def cvMiddleEmpty = Mock.prepareCompressedValueList(1)[0]
        cvMiddleEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvMiddleEmpty.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'c', 0, cvMiddleEmpty)
        def cvLastHas = Mock.prepareCompressedValueList(1)[0]
        cvLastHas.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlLastHas = new RedisList()
        rlLastHas.addLast('from-last'.bytes)
        cvLastHas.compressedData = rlLastHas.encode()
        inMemoryGetSet.put(slot, 'd', 0, cvLastHas)
        reply = bGroup.execute('blmpop 1 3 a c d LEFT')
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'd'
        ((reply as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] instanceof BulkReply
        (((reply as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'from-last'

        when: 'a key payload that cannot be decoded is reported as an error reply'
        inMemoryGetSet.remove(slot, 'c')
        inMemoryGetSet.remove(slot, 'd')
        def cvCorrupt = Mock.prepareCompressedValueList(1)[0]
        cvCorrupt.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvCorrupt.compressedData = 'corrupted-list-payload'.bytes
        inMemoryGetSet.put(slot, 'b', 0, cvCorrupt)
        reply = bGroup.execute('blmpop 1 2 a b LEFT')
        then:
        reply instanceof ErrorReply
        inMemoryGetSet.remove(slot, 'b')

        when: 'a push lands between the check and the registration, the re-check completes the reply'
        def mpopPromiseForRace = new SettablePromise<Reply>()
        def mpopOneForRace = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(mpopPromiseForRace, null, true, System.currentTimeMillis(), null, 2)
        BlockingList.addOne('mpoprace', mpopOneForRace)
        def notCompletedWhenEmpty = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpoprace', 1), 'mpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace, true, 2)
        then:
        !notCompletedWhenEmpty
        !mpopPromiseForRace.isComplete()

        when: 'an existing but empty list does not complete the promise'
        def cvMpopEmptyList = Mock.prepareCompressedValueList(1)[0]
        cvMpopEmptyList.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvMpopEmptyList.compressedData = new RedisList().encode()
        inMemoryGetSet.put(slot, 'mpopempty', 0, cvMpopEmptyList)
        def mpopPromiseForEmpty = new SettablePromise<Reply>()
        def mpopOneForEmpty = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(mpopPromiseForEmpty, null, true, System.currentTimeMillis(), null, 2)
        BlockingList.addOne('mpopempty', mpopOneForEmpty)
        def notCompletedWhenEmptyList = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpopempty', 1), 'mpopempty'.bytes, bGroup, bGroup.dictMap, mpopOneForEmpty, true, 2)
        then:
        !notCompletedWhenEmptyList
        !mpopPromiseForEmpty.isComplete()
        BlockingList.removeOne('mpopempty', mpopOneForEmpty)

        when:
        def cvMpopRace = Mock.prepareCompressedValueList(1)[0]
        cvMpopRace.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlMpopRace = new RedisList()
        rlMpopRace.addFirst('one'.bytes)
        rlMpopRace.addLast('two'.bytes)
        rlMpopRace.addLast('three'.bytes)
        cvMpopRace.compressedData = rlMpopRace.encode()
        inMemoryGetSet.put(slot, 'mpoprace', 0, cvMpopRace)
        BlockingList.addOne('mpoprace', mpopOneForRace)
        def completedMpop = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpoprace', 1), 'mpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace, true, 2)
        then:
        completedMpop
        mpopPromiseForRace.isComplete()
        def mpopRaceReply = mpopPromiseForRace.getResult() as MultiBulkReply
        (mpopRaceReply.replies[0] as BulkReply).asString() == 'mpoprace'
        (mpopRaceReply.replies[1] as MultiBulkReply).replies.length == 2
        ((mpopRaceReply.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'one'
        ((mpopRaceReply.replies[1] as MultiBulkReply).replies[1] as BulkReply).asString() == 'two'

        when: 'an already completed promise is left alone'
        def completedMpopTwice = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpoprace', 1), 'mpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace, true, 2)
        then:
        !completedMpopTwice

        when: 'an mpop waiter that lost the delivery race never pops the list'
        def cvMpopCas = Mock.prepareCompressedValueList(1)[0]
        cvMpopCas.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rlMpopCas = new RedisList()
        rlMpopCas.addFirst('m-one'.bytes)
        rlMpopCas.addLast('m-two'.bytes)
        cvMpopCas.compressedData = rlMpopCas.encode()
        inMemoryGetSet.put(slot, 'mpopcas', 0, cvMpopCas)
        def mpopLostPromise = new SettablePromise<Reply>()
        def mpopLostOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(mpopLostPromise, null, true, System.currentTimeMillis(), null, 2)
        mpopLostOne.tryMarkDelivered()
        def mpopLostCompleted = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpopcas', 1), 'mpopcas'.bytes, bGroup, bGroup.dictMap, mpopLostOne, true, 2)
        then:
        !mpopLostCompleted
        !mpopLostPromise.isComplete()

        when: 'a fresh mpop waiter still gets both elements the loser never popped'
        def mpopWinPromise = new SettablePromise<Reply>()
        def mpopWinOne = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(mpopWinPromise, null, true, System.currentTimeMillis(), null, 2)
        def mpopWinCompleted = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpopcas', 1), 'mpopcas'.bytes, bGroup, bGroup.dictMap, mpopWinOne, true, 2)
        then:
        mpopWinCompleted
        mpopWinPromise.isComplete()
        def mpopWinReply = mpopWinPromise.getResult() as MultiBulkReply
        (mpopWinReply.replies[1] as MultiBulkReply).replies.length == 2
        ((mpopWinReply.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'm-one'
        ((mpopWinReply.replies[1] as MultiBulkReply).replies[1] as BulkReply).asString() == 'm-two'

        when: 'two slot workers, the second key must be checked on the worker owning it'
        localPersist.cleanUp()
        def workersMpop = [startWorkerThread('velo-mpop-worker-1'), startWorkerThread('velo-mpop-worker-2')]
        def mpopLoop1 = workersMpop[0][0]
        def mpopLoop2 = workersMpop[1][0]
        LocalPersistTest.prepareLocalPersist(2 as byte, 2 as short)
        localPersist.initSlotRuntimeBindings([mpopLoop1, mpopLoop2] as Eventloop[], new io.velo.RequestHandler[2],
                [workersMpop[0][1] as long, workersMpop[1][1] as long] as long[])
        localPersist.oneSlot(0 as short)
        localPersist.oneSlot(1 as short)
        BlockingList.initBySlotWorkerEventloopArray([mpopLoop1, mpopLoop2] as Eventloop[])
        BlockingList.clearBlockingListPromisesForAllKeys()

        and: 'two keys hashed into two different slots'
        def mpopGroup = new BGroup(null, null, null)
        mpopGroup.from(BaseCommand.mockAGroup(0 as byte, 2 as byte, 2 as short))
        def mpopKey1 = (1..500).collect { 'mpop1_' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def mpopKey2 = (1..500).collect { 'mpop2_' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }

        and: 'blmpop runs on worker-1 and must not touch the slot of worker-2 directly'
        def mpopReplies = []
        def mpopExecuted = new CountDownLatch(1)
        def mpopThrown = null
        mpopLoop1.execute({
            try {
                mpopReplies << mpopGroup.execute("blmpop 0 2 ${mpopKey1} ${mpopKey2} left count 2")
            } catch (Throwable e) {
                mpopThrown = e
            } finally {
                mpopExecuted.countDown()
            }
        })
        mpopExecuted.await(5, TimeUnit.SECONDS)
        then:
        mpopThrown == null
        mpopReplies[0] instanceof AsyncReply
        !(mpopReplies[0] as AsyncReply).settablePromise.isComplete()

        when: 'a push to the key owned by worker-2 wakes the blocked client up'
        def mpopPushed = []
        def mpopPushDone = new CountDownLatch(1)
        mpopLoop2.execute({
            def values = new byte[1][]
            values[0] = 'mpop-value'.bytes
            mpopPushed << BlockingList.setReplyLPushIfBlockingListExist(mpopKey2, values)
            mpopPushDone.countDown()
        })
        mpopPushDone.await(5, TimeUnit.SECONDS)
        def mpopResultHolder = new Reply[1]
        (mpopReplies[0] as AsyncReply).settablePromise.whenComplete({ Reply r, Exception e -> mpopResultHolder[0] = r })
        for (int i = 0; i < 100 && mpopResultHolder[0] == null; i++) {
            Thread.sleep(50)
        }
        then:
        (mpopPushed[0] as byte[][]).length == 0
        mpopResultHolder[0] instanceof MultiBulkReply
        ((mpopResultHolder[0] as MultiBulkReply).replies[0] as BulkReply).asString() == mpopKey2
        ((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies.length == 1
        ((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] instanceof BulkReply
        (((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'mpop-value'

        when: 'a pending foreign-key wrong-type error retains the Redis error prefix'
        def protocolStore = new InMemoryGetSet()
        def protocolCv = Mock.prepareCompressedValueList(1)[0]
        protocolCv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        protocolCv.compressedData = 'wrong-list-type'.bytes
        protocolStore.put((short) 1, mpopKey2, 0, protocolCv)
        mpopGroup.byPassGetSet = protocolStore
        mpopGroup.crossRequestWorker = true
        def protocolReplies = new LinkedBlockingQueue<String>()
        mpopLoop1.execute {
            def protocolReply = mpopGroup.execute("blmpop 1 2 ${mpopKey1} ${mpopKey2} left") as AsyncReply
            // Completion must bounce to this reactor after execute returns.
            assert !protocolReply.settablePromise.isComplete()
            encodeAsyncReply(protocolReply).whenResult { ByteBuf buf ->
                protocolReplies.offer(new String(buf.asArray()))
            }
        }
        then:
        protocolReplies.poll(5, TimeUnit.SECONDS) ==
                '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

        cleanup:
        BlockingList.clearBlockingListPromisesForAllKeys()
        BlockingList.initBySlotWorkerEventloopArray([Eventloop.builder().withCurrentThread().build()] as Eventloop[])
        workersMpop?.each { w ->
            try {
                w[0].breakEventloop()
                w[2].join(3000)
            } catch (Throwable ignored) {
            }
        }
        localPersist.cleanUp()
    }

    def 'test bzpopmin'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when: 'format error - too few args'
        def reply = bGroup.execute('bzpopmin')
        then:
        reply == ErrorReply.FORMAT

        when: 'timeout not a number'
        reply = bGroup.execute('bzpopmin a x')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when: 'negative timeout'
        reply = bGroup.execute('bzpopmin a -1')
        then:
        reply == new ErrorReply('timeout is negative')

        when: 'NaN timeout'
        reply = bGroup.execute('bzpopmin a NaN')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when: 'Infinity timeout'
        reply = bGroup.execute('bzpopmin a Infinity')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when: 'key too long'
        reply = bGroup.execute('bzpopmin >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'wrong type key returns WRONGTYPE'
        inMemoryGetSet.remove(slot, 'notzset')
        def cvWrong = new CompressedValue()
        cvWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvWrong.compressedData = 'hello'.bytes
        inMemoryGetSet.put(slot, 'notzset', 0, cvWrong)
        reply = bGroup.execute('bzpopmin notzset 1')
        then:
        reply == ErrorReply.WRONG_TYPE
        inMemoryGetSet.remove(slot, 'notzset')

        when: 'immediate pop from a non-empty zset returns the lowest score'
        inMemoryGetSet.remove(slot, 'myzset')
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'one')
        rz.add(2.0, 'two')
        rz.add(3.0, 'three')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'myzset', 0, cv)
        reply = bGroup.execute('bzpopmin myzset 1')
        def mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        mb.replies.length == 3
        (mb.replies[0] as BulkReply).asString() == 'myzset'
        (mb.replies[1] as BulkReply).asString() == 'one'
        (mb.replies[2] as BulkReply).asString() == '1'

        when: 'a missing key blocks indefinitely'
        inMemoryGetSet.remove(slot, 'a')
        reply = bGroup.execute('bzpopmin a 0')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when: 'an existing but empty zset still blocks'
        def cvEmpty = Mock.prepareCompressedValueList(1)[0]
        cvEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        cvEmpty.compressedData = new RedisZSet().encode()
        inMemoryGetSet.put(slot, 'emptyzset', 0, cvEmpty)
        reply = bGroup.execute('bzpopmin emptyzset 0')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()
        inMemoryGetSet.remove(slot, 'emptyzset')

        when: 'a blocked client is woken by zadd'
        inMemoryGetSet.remove(slot, 'bzpopmin-wake')
        reply = bGroup.execute('bzpopmin bzpopmin-wake 0')
        def zGroupForWake = new ZGroup(null, null, null)
        zGroupForWake.byPassGetSet = inMemoryGetSet
        zGroupForWake.from(BaseCommand.mockAGroup())
        zGroupForWake.execute('zadd bzpopmin-wake 5.5 hello')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.isComplete()
        def wakeup = (reply as AsyncReply).settablePromise.getResult() as MultiBulkReply
        wakeup.replies.length == 3
        (wakeup.replies[0] as BulkReply).asString() == 'bzpopmin-wake'
        (wakeup.replies[1] as BulkReply).asString() == 'hello'
        (wakeup.replies[2] as BulkReply).asString() == '5.5'

        when: 'all keys empty — block with timeout'
        inMemoryGetSet.remove(slot, 'x')
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        reply = bGroup.execute('bzpopmin x 1')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE
        // redis answers a timed out blocking command with a null array, not a null bulk
        (reply as AsyncReply).settablePromise.getResult().buffer().asArray() == '*-1\r\n'.bytes

        when: 'first key missing, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        rzB.add(1.0, 'min-b')
        rzB.add(2.0, 'max-b')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = bGroup.execute('bzpopmin a b 1')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply &&
                    ((result as MultiBulkReply).replies[0] as BulkReply).asString() == 'b' &&
                    ((result as MultiBulkReply).replies[1] as BulkReply).asString() == 'min-b' &&
                    ((result as MultiBulkReply).replies[2] as BulkReply).asString() == '1'
        }.result

        when: 'all keys empty in the multi key form, the timeout replies nil'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('bzpopmin a b 1')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        when: 'a later key holds another type, the multi key check fails and the error is forwarded'
        inMemoryGetSet.remove(slot, 'wtypea')
        def cvWrongTypeKey = Mock.prepareCompressedValueList(1)[0]
        cvWrongTypeKey.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvWrongTypeKey.compressedData = 'not-a-zset'.bytes
        inMemoryGetSet.put(slot, 'wtypeb', 0, cvWrongTypeKey)
        reply = bGroup.execute('bzpopmin wtypea wtypeb 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ErrorReply.WRONG_TYPE
        inMemoryGetSet.remove(slot, 'wtypeb')

        when: 'a key payload that cannot be decoded is reported as an error reply'
        def cvCorrupt = Mock.prepareCompressedValueList(1)[0]
        cvCorrupt.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        cvCorrupt.compressedData = 'corrupted-zset-payload'.bytes
        inMemoryGetSet.put(slot, 'wcorrupt', 0, cvCorrupt)
        reply = bGroup.execute('bzpopmin wmissing wcorrupt 0')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() != null
        inMemoryGetSet.remove(slot, 'wcorrupt')

        when: 'the wrong type completion releases the waiter from every key it was registered on'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def wrongTypePromise = new SettablePromise<Reply>()
        def wrongTypeSlotA = BaseCommand.slot('wtype-multi-a', (short) 1)
        def wrongTypeSlotB = BaseCommand.slot('wtype-multi-b', (short) 1)
        def wrongTypeOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(wrongTypePromise, null, true,
                System.currentTimeMillis(), 0, [wrongTypeSlotA, wrongTypeSlotB])
        BlockingZSet.addOne('wtype-multi-a', wrongTypeOne)
        BlockingZSet.addOne('wtype-multi-b', wrongTypeOne)
        def wrongTypeCv = Mock.prepareCompressedValueList(1)[0]
        wrongTypeCv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        wrongTypeCv.compressedData = 'not-a-zset'.bytes
        inMemoryGetSet.put(slot, 'wtype-multi-a', 0, wrongTypeCv)
        def wrongTypeCompleted = BGroup.completeIfZSetNotEmpty(wrongTypeSlotA, 'wtype-multi-a'.bytes, bGroup,
                bGroup.dictMap, wrongTypeOne)
        then:
        wrongTypeCompleted
        wrongTypePromise.getResult() == ErrorReply.WRONG_TYPE
        BlockingZSet.blockingClientCount() == 0
        BlockingZSet.blockingKeyCount() == 0
        inMemoryGetSet.remove(slot, 'wtype-multi-a')

        when: 'a zadd lands between the check and the registration, the re-check completes the reply'
        def promiseForRace = new SettablePromise<Reply>()
        def oneForRace = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promiseForRace, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('zrace', oneForRace)
        def completedWhenEmpty = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zrace', 1), 'zrace'.bytes, bGroup, bGroup.dictMap, oneForRace)
        then:
        !completedWhenEmpty
        !promiseForRace.isComplete()

        when:
        def cvRace = Mock.prepareCompressedValueList(1)[0]
        cvRace.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzRace = new RedisZSet()
        rzRace.add(1.5, 'pushed-in-window')
        cvRace.compressedData = rzRace.encode()
        inMemoryGetSet.put(slot, 'zrace', 0, cvRace)
        BlockingZSet.addOne('zrace', oneForRace)
        def completedWhenNotEmpty = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zrace', 1), 'zrace'.bytes, bGroup, bGroup.dictMap, oneForRace)
        then:
        completedWhenNotEmpty
        promiseForRace.isComplete()
        def raceResult = promiseForRace.getResult() as MultiBulkReply
        (raceResult.replies[1] as BulkReply).asString() == 'pushed-in-window'
        (raceResult.replies[2] as BulkReply).asString() == '1.5'

        when: 'an already completed promise is left alone'
        def completedTwice = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zrace', 1), 'zrace'.bytes, bGroup, bGroup.dictMap, oneForRace)
        then:
        !completedTwice

        when: 'a waiter that lost the delivery race never pops the zset'
        def cvCas = Mock.prepareCompressedValueList(1)[0]
        cvCas.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzCas = new RedisZSet()
        rzCas.add(1.0, 'must-survive')
        cvCas.compressedData = rzCas.encode()
        inMemoryGetSet.put(slot, 'zcas', 0, cvCas)
        def lostPromise = new SettablePromise<Reply>()
        def lostOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(lostPromise, null, true, System.currentTimeMillis(), 0)
        lostOne.tryMarkDelivered()
        def lostCompleted = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zcas', 1), 'zcas'.bytes, bGroup, bGroup.dictMap, lostOne)
        then:
        !lostCompleted
        !lostPromise.isComplete()

        when: 'a fresh waiter still gets the member the loser never popped'
        def winPromise = new SettablePromise<Reply>()
        def winOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(winPromise, null, true, System.currentTimeMillis(), 0)
        def winCompleted = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zcas', 1), 'zcas'.bytes, bGroup, bGroup.dictMap, winOne)
        then:
        winCompleted
        winPromise.isComplete()
        ((winPromise.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'must-survive'

        when: 'the key changed to another type while waiting, the waiter gets WRONGTYPE'
        def cvWType = Mock.prepareCompressedValueList(1)[0]
        cvWType.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvWType.compressedData = 'not-a-zset'.bytes
        inMemoryGetSet.put(slot, 'zwtype', 0, cvWType)
        def promiseForWType = new SettablePromise<Reply>()
        def oneForWType = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promiseForWType, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('zwtype', oneForWType)
        def completedWType = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zwtype', 1), 'zwtype'.bytes, bGroup, bGroup.dictMap, oneForWType)
        then:
        completedWType
        promiseForWType.getResult() == ErrorReply.WRONG_TYPE

        when: 'the delivery arbiter is one-shot'
        def casOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(new SettablePromise<Reply>(), null, true, System.currentTimeMillis(), 0)
        casOne.tryMarkDelivered()
        then:
        !casOne.tryMarkDelivered()

        when: 'a waiter registered on two keys is served by one of them'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def multiPromise = new SettablePromise<Reply>()
        def multiSlotA = BaseCommand.slot('bzmulti-a', (short) 1)
        def multiSlotB = BaseCommand.slot('bzmulti-b', (short) 1)
        def multiOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(multiPromise, null, true,
                System.currentTimeMillis(), 0, [multiSlotA, multiSlotB])
        BlockingZSet.addOne('bzmulti-a', multiOne)
        BlockingZSet.addOne('bzmulti-b', multiOne)
        then:
        // one client blocked on two keys, it counts once
        BlockingZSet.blockingClientCount() == 1
        BlockingZSet.blockingKeyCount() == 2

        when: 'the first key is served, the waiter must be released from both keys'
        inMemoryGetSet.remove(slot, 'bzmulti-a')
        def cvMulti = Mock.prepareCompressedValueList(1)[0]
        cvMulti.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzMulti = new RedisZSet()
        rzMulti.add(1.0, 'multi-member')
        cvMulti.compressedData = rzMulti.encode()
        inMemoryGetSet.put(slot, 'bzmulti-a', 0, cvMulti)
        def multiCompleted = BGroup.completeIfZSetNotEmpty(multiSlotA, 'bzmulti-a'.bytes, bGroup, bGroup.dictMap, multiOne)
        then:
        multiCompleted
        multiPromise.isComplete()
        BlockingZSet.blockingClientCount() == 0
        BlockingZSet.blockingKeyCount() == 0
    }

    def 'test bzpopmax'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when: 'immediate pop from a non-empty zset returns the highest score'
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'one')
        rz.add(2.0, 'two')
        rz.add(3.0, 'three')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'myzset', 0, cv)
        def reply = bGroup.execute('bzpopmax myzset 1')
        def mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        mb.replies.length == 3
        (mb.replies[0] as BulkReply).asString() == 'myzset'
        (mb.replies[1] as BulkReply).asString() == 'three'
        (mb.replies[2] as BulkReply).asString() == '3'

        when: 'the next highest score is returned on the next call'
        reply = bGroup.execute('bzpopmax myzset 1')
        mb = reply as MultiBulkReply
        then:
        (mb.replies[1] as BulkReply).asString() == 'two'

        when: 'a blocked client is woken by zadd and receives the highest of the added members'
        inMemoryGetSet.remove(slot, 'bzpopmax-wake')
        reply = bGroup.execute('bzpopmax bzpopmax-wake 0')
        def zGroupForWakeMax = new ZGroup(null, null, null)
        zGroupForWakeMax.byPassGetSet = inMemoryGetSet
        zGroupForWakeMax.from(BaseCommand.mockAGroup())
        zGroupForWakeMax.execute('zadd bzpopmax-wake 3.0 c 9.9 z')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.isComplete()
        def wakeup = (reply as AsyncReply).settablePromise.getResult() as MultiBulkReply
        (wakeup.replies[1] as BulkReply).asString() == 'z'
        (wakeup.replies[2] as BulkReply).asString() == '9.9'

        when: 'the member the waiter did not pop stays in the zset'
        def rzLeft = ZGroup.getRedisZSet(BaseCommand.slot('bzpopmax-wake', 1), bGroup)
        then:
        rzLeft != null
        rzLeft.size() == 1
        rzLeft.contains('c')
    }

    def 'test bzmpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        and: 'the redis shape nests one array per member/score pair'
        def memberPairs = { reply ->
            def arr = reply as MultiBulkReply
            arr.replies.collect { pair ->
                def onePair = pair as MultiBulkReply
                onePair.replies.length == 2
                [(onePair.replies[0] as BulkReply).asString(), (onePair.replies[1] as BulkReply).asString()]
            }
        }

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when: 'format error - too few args'
        def reply = bGroup.execute('bzmpop')
        then:
        reply == ErrorReply.FORMAT

        when: 'timeout not number'
        reply = bGroup.execute('bzmpop abc 1 myzset MIN')
        then:
        reply == new ErrorReply('timeout is not a float or out of range')

        when: 'negative timeout'
        reply = bGroup.execute('bzmpop -1 1 myzset MIN')
        then:
        reply == new ErrorReply('timeout is negative')

        when: 'numkeys not integer'
        reply = bGroup.execute('bzmpop 1 abc myzset MIN')
        then:
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'numkeys zero'
        reply = bGroup.execute('bzmpop 1 0 myzset MIN')
        then:
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'numkeys greater than the provided keys'
        reply = bGroup.execute('bzmpop 1 2 myzset MIN')
        then:
        reply == ErrorReply.FORMAT

        when: 'wrong direction'
        reply = bGroup.execute('bzmpop 1 1 myzset UP')
        then:
        reply == ErrorReply.SYNTAX

        when: 'count not integer'
        reply = bGroup.execute('bzmpop 1 1 myzset MIN COUNT abc')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'count zero rejected'
        reply = bGroup.execute('bzmpop 1 1 myzset MIN COUNT 0')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'syntax error - extra args'
        reply = bGroup.execute('bzmpop 1 1 myzset MIN COUNT 1 extra')
        then:
        reply == ErrorReply.SYNTAX

        when: 'key too long'
        reply = bGroup.execute('bzmpop 1 1 >key MIN')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'wrong type key returns WRONGTYPE'
        inMemoryGetSet.remove(slot, 'notzset')
        def cvWrong = new CompressedValue()
        cvWrong.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvWrong.compressedData = 'hello'.bytes
        inMemoryGetSet.put(slot, 'notzset', 0, cvWrong)
        reply = bGroup.execute('bzmpop 1 1 notzset MIN')
        then:
        reply == ErrorReply.WRONG_TYPE
        inMemoryGetSet.remove(slot, 'notzset')

        when: 'immediate pop from a non-empty zset, MIN with default count'
        inMemoryGetSet.remove(slot, 'myzset')
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'one')
        rz.add(2.0, 'two')
        rz.add(3.0, 'three')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'myzset', 0, cv)
        reply = bGroup.execute('bzmpop 1 1 myzset MIN')
        def mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        mb.replies.length == 2
        (mb.replies[0] as BulkReply).asString() == 'myzset'
        memberPairs(mb.replies[1]) == [['one', '1']]

        when: 'MAX direction pops the highest score'
        reply = bGroup.execute('bzmpop 1 1 myzset MAX')
        mb = reply as MultiBulkReply
        then:
        memberPairs(mb.replies[1]) == [['three', '3']]

        when: 'COUNT 2 pops at most two members, only one is left'
        reply = bGroup.execute('bzmpop 1 1 myzset MIN COUNT 2')
        mb = reply as MultiBulkReply
        then:
        (mb.replies[0] as BulkReply).asString() == 'myzset'
        memberPairs(mb.replies[1]) == [['two', '2']]

        when: 'all keys empty — block with timeout'
        inMemoryGetSet.remove(slot, 'x')
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        reply = bGroup.execute('bzmpop 1 1 x MIN')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        when: 'all keys empty in the multi key form, the timeout replies nil'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('bzmpop 1 2 a b MIN')
        Thread.sleep(2000)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == NullArrayReply.INSTANCE

        when: 'a multi key BZMPOP with timeout 0 stays pending'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = bGroup.execute('bzmpop 0 2 a b MAX')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when: 'a blocked BZMPOP is woken by zadd and pops up to COUNT members'
        inMemoryGetSet.remove(slot, 'bzmpop-wake')
        reply = bGroup.execute('bzmpop 0 1 bzmpop-wake MIN COUNT 2')
        def zGroupForWakeMpop = new ZGroup(null, null, null)
        zGroupForWakeMpop.byPassGetSet = inMemoryGetSet
        zGroupForWakeMpop.from(BaseCommand.mockAGroup())
        zGroupForWakeMpop.execute('zadd bzmpop-wake 1.0 a 2.0 b')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.isComplete()
        def wakeup = (reply as AsyncReply).settablePromise.getResult() as MultiBulkReply
        (wakeup.replies[0] as BulkReply).asString() == 'bzmpop-wake'
        memberPairs(wakeup.replies[1]) == [['a', '1'], ['b', '2']]

        when: 'first key missing, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        rzB.add(1.0, 'x')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = bGroup.execute('bzmpop 1 2 a b MIN COUNT 3')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        (mb.replies[0] as BulkReply).asString() == 'b'
        memberPairs(mb.replies[1]) == [['x', '1']]

        when: 'first key holds an existing but empty zset, the second key is used'
        inMemoryGetSet.remove(slot, 'b')
        def cvFirstEmpty = Mock.prepareCompressedValueList(1)[0]
        cvFirstEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        cvFirstEmpty.compressedData = new RedisZSet().encode()
        inMemoryGetSet.put(slot, 'a', 0, cvFirstEmpty)
        def cvSecondHas = Mock.prepareCompressedValueList(1)[0]
        cvSecondHas.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzSecondHas = new RedisZSet()
        rzSecondHas.add(4.0, 'from-second')
        cvSecondHas.compressedData = rzSecondHas.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvSecondHas)
        reply = bGroup.execute('bzmpop 1 2 a b MIN')
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'b'
        memberPairs((reply as MultiBulkReply).replies[1]) == [['from-second', '4']]

        when: 'second key holds another type, WRONGTYPE is returned'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvWrongType = Mock.prepareCompressedValueList(1)[0]
        cvWrongType.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvWrongType.compressedData = 'notazset'.bytes
        inMemoryGetSet.put(slot, 'b', 0, cvWrongType)
        reply = bGroup.execute('bzmpop 1 2 a b MIN')
        then:
        reply == ErrorReply.WRONG_TYPE
        inMemoryGetSet.remove(slot, 'b')

        when: 'a key payload that cannot be decoded is reported as an error reply'
        def cvCorrupt = Mock.prepareCompressedValueList(1)[0]
        cvCorrupt.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        cvCorrupt.compressedData = 'corrupted-zset-payload'.bytes
        inMemoryGetSet.put(slot, 'b', 0, cvCorrupt)
        reply = bGroup.execute('bzmpop 1 2 a b MIN')
        then:
        reply instanceof ErrorReply
        inMemoryGetSet.remove(slot, 'b')

        when: 'a push lands between the check and the registration, the re-check completes the BZMPOP reply'
        def mpopPromiseForRace = new SettablePromise<Reply>()
        def mpopOneForRace = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(mpopPromiseForRace, null, true, System.currentTimeMillis(), 2)
        BlockingZSet.addOne('zmpoprace', mpopOneForRace)
        def mpopNotCompletedWhenEmpty = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zmpoprace', 1), 'zmpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace)
        then:
        !mpopNotCompletedWhenEmpty
        !mpopPromiseForRace.isComplete()

        when:
        def cvMpopRace = Mock.prepareCompressedValueList(1)[0]
        cvMpopRace.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzMpopRace = new RedisZSet()
        rzMpopRace.add(1.0, 'first')
        rzMpopRace.add(2.0, 'second')
        rzMpopRace.add(3.0, 'third')
        cvMpopRace.compressedData = rzMpopRace.encode()
        inMemoryGetSet.put(slot, 'zmpoprace', 0, cvMpopRace)
        BlockingZSet.addOne('zmpoprace', mpopOneForRace)
        def mpopCompletedWhenNotEmpty = BGroup.completeIfZSetNotEmpty(BaseCommand.slot('zmpoprace', 1), 'zmpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace)
        then:
        mpopCompletedWhenNotEmpty
        mpopPromiseForRace.isComplete()
        def mpopRaceResult = mpopPromiseForRace.getResult() as MultiBulkReply
        (mpopRaceResult.replies[0] as BulkReply).asString() == 'zmpoprace'
        memberPairs(mpopRaceResult.replies[1]) == [['first', '1'], ['second', '2']]
    }

}
