package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import spock.lang.Specification

class KGroupTest extends Specification {
    def _KGroup = new KGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _KGroup.parseSlots('kx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def kGroup = new KGroup('incr', data1, null)
        kGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = kGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
