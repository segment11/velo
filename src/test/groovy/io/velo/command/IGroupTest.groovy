package io.velo.command

import io.velo.BaseCommand
import io.velo.mock.InMemoryGetSet
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.NilReply
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class IGroupTest extends Specification {
    def _IGroup = new IGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sIncrList = _IGroup.parseSlots('incr', data2, slotNumber)
        def sIncrbyList = _IGroup.parseSlots('incrby', data2, slotNumber)
        def sIncrbyfloatList = _IGroup.parseSlots('incrbyfloat', data2, slotNumber)
        def sList = _IGroup.parseSlots('ixxx', data2, slotNumber)
        then:
        sIncrList.size() == 1
        sIncrbyList.size() == 1
        sIncrbyfloatList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]

        sIncrbyList = _IGroup.parseSlots('incrby', data1, slotNumber)
        then:
        sIncrbyList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def iGroup = new IGroup('incr', data1, null)
        iGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        iGroup.cmd = 'info'
        iGroup.data = data3
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'zzz'
        reply = iGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test handle2'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('incr', data2, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup())

        when:
        iGroup.slotWithKeyHashListParsed = _IGroup.parseSlots('incr', data2, iGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = iGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes
        iGroup.data = data3
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data3[2] = 'a'.bytes
        reply = iGroup.handle()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.NOT_FLOAT
    }
}
