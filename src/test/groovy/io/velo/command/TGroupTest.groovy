package io.velo.command

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Mock
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.NilReply
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class TGroupTest extends Specification {
    def _TGroup = new TGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sTypeList = _TGroup.parseSlots('type', data2, slotNumber)
        def sTtlList = _TGroup.parseSlots('ttl', data2, slotNumber)
        def sList = _TGroup.parseSlots('txxx', data2, slotNumber)
        then:
        sTypeList.size() == 1
        sTtlList.size() == 1
        sList.size() == 0


        when:
        def data1 = new byte[1][]
        sTypeList = _TGroup.parseSlots('type', data1, slotNumber)
        sTtlList = _TGroup.parseSlots('ttl', data1, slotNumber)
        then:
        sTypeList.size() == 0
        sTtlList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def tGroup = new TGroup('type', data1, null)
        tGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = tGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        tGroup.cmd = 'ttl'
        reply = tGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        tGroup.cmd = 'zzz'
        reply = tGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test type'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def tGroup = new TGroup('type', data2, null)
        tGroup.byPassGetSet = inMemoryGetSet
        tGroup.from(BaseCommand.mockAGroup())

        when:
        tGroup.slotWithKeyHashListParsed = _TGroup.parseSlots('type', data2, tGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = tGroup.type()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_HASH

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_HASH

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_LIST

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_SET

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_ZSET

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_STREAM
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_STREAM

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.type()
        then:
        reply == _TGroup.TYPE_STRING

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = tGroup.type()
        then:
        reply == ErrorReply.KEY_TOO_LONG

    }

    def 'test ttl'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def tGroup = new TGroup('ttl', data2, null)
        tGroup.byPassGetSet = inMemoryGetSet
        tGroup.from(BaseCommand.mockAGroup())

        when:
        tGroup.slotWithKeyHashListParsed = _TGroup.parseSlots('ttl', data2, tGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = tGroup.ttl(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.ttl(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -1

        when:
        cv.expireAt = System.currentTimeMillis() + 2500
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = tGroup.ttl(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = tGroup.ttl(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer > 2000

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = tGroup.ttl(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
