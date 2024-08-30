package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import spock.lang.Specification

class OGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = OGroup.parseSlots('ox', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def oGroup = new OGroup('incr', data1, null)
        oGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = oGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
