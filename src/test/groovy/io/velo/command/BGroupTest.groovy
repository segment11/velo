package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.NilReply
import io.velo.reply.OKReply
import spock.lang.Specification

class BGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]
        int slotNumber = 128

        when:
        def sBgsaveList = BGroup.parseSlots('bgsave', data1, slotNumber)
        then:
        sBgsaveList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]
        def bGroup = new BGroup('bgsave', data1, null)
        bGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = bGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        bGroup.cmd = 'zzz'
        reply = bGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
