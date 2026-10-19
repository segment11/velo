package io.velo.command

import io.activej.eventloop.Eventloop
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
import spock.lang.Specification

import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
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
        // bad fpp should return NOT_FLOAT
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = bGroup.execute('bf.reserve a 0.01 nope')
        then:
        // bad capacity should return NOT_INTEGER
        reply == ErrorReply.NOT_INTEGER

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
        ((promiseForRace.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'pushed-in-window'.bytes

        when: 'an already completed promise is left alone'
        def completedTwice = BGroup.completeIfListNotEmpty(BaseCommand.slot('race', 1), 'race'.bytes, bGroup, bGroup.dictMap, oneForRace, true)
        then:
        !completedTwice

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
        ((promiseForRight.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'last'.bytes

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
        def registered = new java.util.concurrent.CountDownLatch(1)
        loop1.execute({
            asyncReplyCross << bGroupWorkers.execute("blpop ${keyOnWorker1} ${keyOnWorker2} 0")
            registered.countDown()
        })
        registered.await(5, java.util.concurrent.TimeUnit.SECONDS)
        Thread.sleep(300)
        and: 'push to the key owned by worker-2'
        def pushedOnWorker2 = []
        def pushDone = new java.util.concurrent.CountDownLatch(1)
        loop2.execute({
            def values = new byte[1][]
            values[0] = 'from-worker-2'.bytes
            pushedOnWorker2 << BlockingList.setReplyLPushIfBlockingListExist(keyOnWorker2, values)
            pushDone.countDown()
        })
        pushDone.await(5, java.util.concurrent.TimeUnit.SECONDS)
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
        ((crossReply as MultiBulkReply).replies[0] as BulkReply).raw == keyOnWorker2.bytes
        ((crossReply as MultiBulkReply).replies[1] as BulkReply).raw == 'from-worker-2'.bytes

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

    private static List startWorkerThread(String name) {
        var loopQ = new java.util.concurrent.LinkedBlockingQueue<Eventloop>()
        def eventloop = Eventloop.builder().withIdleInterval(Duration.ofMillis(20)).build()
        eventloop.keepAlive(true)
        def thread = Thread.start {
            loopQ.put(eventloop)
            eventloop.run()
        }
        def loop = loopQ.take()
        def ready = new java.util.concurrent.CountDownLatch(1)
        loop.execute { ready.countDown() }
        ready.await(5, java.util.concurrent.TimeUnit.SECONDS)
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
        ((brpopResult as MultiBulkReply).replies[0] as BulkReply).raw == 'b'.bytes
        ((brpopResult as MultiBulkReply).replies[1] as BulkReply).raw == 'last'.bytes

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
        reply == ErrorReply.NOT_INTEGER

        when: 'negative timeout'
        reply = bGroup.execute('blmpop -1 1 mylist LEFT')
        then:
        reply instanceof ErrorReply

        when: 'numkeys not integer'
        reply = bGroup.execute('blmpop 0 abc 1 mylist LEFT')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'numkeys zero'
        reply = bGroup.execute('blmpop 1 0 mylist LEFT')
        then:
        reply == ErrorReply.RANGE_OUT_OF_INDEX

        when: 'wrong direction'
        reply = bGroup.execute('blmpop 1 1 mylist UP')
        then:
        reply == ErrorReply.SYNTAX

        when: 'count not integer'
        reply = bGroup.execute('blmpop 1 1 mylist LEFT COUNT abc')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'count zero rejected'
        reply = bGroup.execute('blmpop 1 1 mylist LEFT COUNT 0')
        then:
        reply instanceof ErrorReply

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
        (mb.replies[0] as BulkReply).raw == 'mylist'.bytes
        (mb.replies[1] as MultiBulkReply).replies.length == 1
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'a'.bytes

        when: 'right pop'
        reply = bGroup.execute('blmpop 1 1 mylist RIGHT')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'b'.bytes

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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == NilReply.INSTANCE
        }.result

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
                .replies[0] as BulkReply).raw == 'in-time'.bytes

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
        (mb.replies[0] as BulkReply).raw == 'b'.bytes
        (mb.replies[1] as MultiBulkReply).replies.length == 1
        ((mb.replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'x'.bytes

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).raw == 'b'.bytes

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).raw == 'd'.bytes
        ((reply as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] instanceof BulkReply
        (((reply as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'from-last'.bytes

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
        (mpopRaceReply.replies[0] as BulkReply).raw == 'mpoprace'.bytes
        (mpopRaceReply.replies[1] as MultiBulkReply).replies.length == 2
        ((mpopRaceReply.replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'one'.bytes
        ((mpopRaceReply.replies[1] as MultiBulkReply).replies[1] as BulkReply).raw == 'two'.bytes

        when: 'an already completed promise is left alone'
        def completedMpopTwice = BGroup.completeMpopIfListNotEmpty(BaseCommand.slot('mpoprace', 1), 'mpoprace'.bytes, bGroup, bGroup.dictMap, mpopOneForRace, true, 2)
        then:
        !completedMpopTwice

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
        def mpopExecuted = new java.util.concurrent.CountDownLatch(1)
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
        mpopExecuted.await(5, java.util.concurrent.TimeUnit.SECONDS)
        then:
        mpopThrown == null
        mpopReplies[0] instanceof AsyncReply
        !(mpopReplies[0] as AsyncReply).settablePromise.isComplete()

        when: 'a push to the key owned by worker-2 wakes the blocked client up'
        def mpopPushed = []
        def mpopPushDone = new java.util.concurrent.CountDownLatch(1)
        mpopLoop2.execute({
            def values = new byte[1][]
            values[0] = 'mpop-value'.bytes
            mpopPushed << BlockingList.setReplyLPushIfBlockingListExist(mpopKey2, values)
            mpopPushDone.countDown()
        })
        mpopPushDone.await(5, java.util.concurrent.TimeUnit.SECONDS)
        def mpopResultHolder = new Reply[1]
        (mpopReplies[0] as AsyncReply).settablePromise.whenComplete({ Reply r, Exception e -> mpopResultHolder[0] = r })
        for (int i = 0; i < 100 && mpopResultHolder[0] == null; i++) {
            Thread.sleep(50)
        }
        then:
        (mpopPushed[0] as byte[][]).length == 0
        mpopResultHolder[0] instanceof MultiBulkReply
        ((mpopResultHolder[0] as MultiBulkReply).replies[0] as BulkReply).raw == mpopKey2.bytes
        ((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies.length == 1
        ((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] instanceof BulkReply
        (((mpopResultHolder[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies[0] as BulkReply).raw == 'mpop-value'.bytes

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
}
