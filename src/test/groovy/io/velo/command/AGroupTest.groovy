package io.velo.command

import io.velo.BaseCommand
import io.velo.mock.InMemoryGetSet
import io.velo.reply.ErrorReply
import io.velo.reply.NilReply
import spock.lang.Specification

class AGroupTest extends Specification {
    def _AGroup = new AGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes

        when:
        def sAppendList = _AGroup.parseSlots('append', data3, slotNumber)
        then:
        sAppendList.size() == 1

        when:
        sAppendList = _AGroup.parseSlots('axxx', data3, slotNumber)
        then:
        sAppendList.size() == 0

        when:
        def data1 = new byte[1][]
        sAppendList = _AGroup.parseSlots('append', data1, slotNumber)
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
        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup = new AGroup(null, null, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup())

        when:
        aGroup.execute('append a 123')
        then:
        aGroup.get('a'.bytes) == '123'.bytes

        when:
        aGroup.execute('append a 456')
        then:
        aGroup.get('a'.bytes) == '123456'.bytes

        when:
        def reply = aGroup.execute('append a')
        then:
        reply == ErrorReply.FORMAT
    }
}
