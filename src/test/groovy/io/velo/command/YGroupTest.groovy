package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import spock.lang.Specification

class YGroupTest extends Specification {
    def _YGroup = new YGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _YGroup.parseSlots('yx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def yGroup = new YGroup('incr', data1, null)
        yGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = yGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
