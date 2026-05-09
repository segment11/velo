package io.velo.command

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Mock
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class TGroupTest extends Specification {
    def _TGroup = new TGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        int slotNumber = 128

        expect:
        _TGroup.parseSlots(cmd, data2, slotNumber).size() == expectedSize

        where:
        cmd     | expectedSize
        'ttl'   | 1
        'type'  | 1
        'txxx'  | 0
    }

    def 'test parse slot - insufficient data'() {
        given:
        def data1 = new byte[1][]
        int slotNumber = 128

        expect:
        _TGroup.parseSlots('type', data1, slotNumber).size() == 0
        _TGroup.parseSlots('ttl', data1, slotNumber).size() == 0
    }

    def 'test handle - format errors'() {
        given:
        def tGroup = new TGroup(null, null, null)
        tGroup.from(BaseCommand.mockAGroup())

        expect:
        tGroup.execute(input) == expected

        where:
        input  | expected
        'ttl'  | ErrorReply.FORMAT
        'type' | ErrorReply.FORMAT
        'zzz'  | NilReply.INSTANCE
    }

    def 'test handle - time'() {
        given:
        def tGroup = new TGroup(null, null, null)
        tGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = tGroup.execute('time')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
    }

    def 'test time uses epoch seconds'() {
        given:
        def tGroup = new TGroup('time', new byte[1][], null)
        tGroup.from(BaseCommand.mockAGroup())
        def nowSeconds = System.currentTimeMillis() / 1000

        when:
        def reply = tGroup.handle() as MultiBulkReply
        def seconds = ((BulkReply) reply.replies[0]).asString().toLong()
        def microseconds = ((BulkReply) reply.replies[1]).asString().toLong()

        then:
        Math.abs(seconds - nowSeconds) <= 1
        microseconds >= 0
        microseconds < 1_000_000
    }

    def 'test ttl'() {
        given:
        final short slot = 0

        def inMemoryGetSet = new InMemoryGetSet()

        def tGroup = new TGroup(null, null, null)
        tGroup.byPassGetSet = inMemoryGetSet
        tGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = tGroup.execute('ttl a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('ttl a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -1

        when:
        cv.expireAt = System.currentTimeMillis() + 2500
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('ttl a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        tGroup.slotWithKeyHashListParsed = _TGroup.parseSlots('ttl', data2, tGroup.slotNumber)
        tGroup.data = data2
        reply = tGroup.ttl(true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer > 2000

        when:
        reply = tGroup.execute('ttl >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test type'() {
        given:
        final short slot = 0

        def inMemoryGetSet = new InMemoryGetSet()

        def tGroup = new TGroup(null, null, null)
        tGroup.byPassGetSet = inMemoryGetSet
        tGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_NONE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_HASH

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_HASH

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_LIST

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_SET

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_ZSET

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_GEO
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_ZSET

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_STREAM
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_STREAM

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.execute('type a')
        then:
        reply == TGroup.TYPE_STRING

        when:
        reply = tGroup.execute('type >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
