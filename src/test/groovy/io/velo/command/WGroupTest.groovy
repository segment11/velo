package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import spock.lang.Specification

class WGroupTest extends Specification {
    def _WGroup = new WGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _WGroup.parseSlots('wx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def wGroup = new WGroup('incr', data1, null)
        wGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = wGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
