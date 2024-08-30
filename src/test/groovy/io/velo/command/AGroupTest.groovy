package io.velo.command

import io.velo.BaseCommand
import io.velo.mock.InMemoryGetSet
import io.velo.reply.ErrorReply
import io.velo.reply.NilReply
import spock.lang.Specification

class AGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes

        when:
        def sAppendList = AGroup.parseSlots('append', data3, slotNumber)
        then:
        sAppendList.size() == 1

        when:
        sAppendList = AGroup.parseSlots('axxx', data3, slotNumber)
        then:
        sAppendList.size() == 0

        when:
        def data1 = new byte[1][]
        sAppendList = AGroup.parseSlots('append', data1, slotNumber)
        then:
        sAppendList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def aGroup = new AGroup('append', data1, null)
        aGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = aGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        aGroup.cmd = 'zzz'
        reply = aGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test append'() {
        given:
        def data3 = new byte[3][]

        and:
        data3[1] = 'a'.bytes
        data3[2] = '123'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup = new AGroup('append', data3, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup())

        when:
        aGroup.slotWithKeyHashListParsed = AGroup.parseSlots('append', data3, aGroup.slotNumber)
        aGroup.append()
        then:
        aGroup.get('a'.bytes) == '123'.bytes

        when:
        data3[2] = '456'.bytes
        aGroup.append()
        then:
        aGroup.get('a'.bytes) == '123456'.bytes

        when:
        def data2 = new byte[2][]
        def aGroup2 = new AGroup('append', data2, null)
//        aGroup2.byPassGetSet = inMemoryGetSet
//        aGroup2.from(BaseCommand.mockAGroup())
        def reply = aGroup2.append()
        then:
        reply == ErrorReply.FORMAT
    }
}
