package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import spock.lang.Specification

class JGroupTest extends Specification {
    def _JGroup = new JGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _JGroup.parseSlots('jx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def jGroup = new JGroup('incr', data1, null)
        jGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = jGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
