package io.velo.command

import io.velo.BaseCommand
import io.velo.reply.BulkReply
import spock.lang.Specification

class ExtendCommandTest extends Specification {
    def _ExtendCommand = new ExtendCommand()

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _ExtendCommand.parseSlots('extend', data1, 1).size() == 0
    }

    def 'test handle'() {
        given:
        def data3 = new byte[3][]

        def eGroup = new EGroup('extend', data3, null)
        eGroup.from(BaseCommand.mockAGroup())
        def extendCommand = new ExtendCommand(eGroup)

        when:
        def reply = extendCommand.handle()
        then:
        reply instanceof BulkReply
    }
}
