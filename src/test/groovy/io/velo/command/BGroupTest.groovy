package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import io.velo.reply.OKReply
import spock.lang.Specification

class BGroupTest extends Specification {
    def _BGroup = new BGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]
        int slotNumber = 128

        when:
        def sBgsaveList = _BGroup.parseSlots('bgsave', data1, slotNumber)
        then:
        sBgsaveList.size() == 0
    }

    def 'test handle'() {
        given:
        def bGroup = new BGroup(null, null, null)
        bGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = bGroup.execute('bgsave')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = bGroup.execute('zzz')
        then:
        reply == NilReply.INSTANCE
    }
}
