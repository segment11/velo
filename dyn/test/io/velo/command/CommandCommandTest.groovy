package io.velo.command

import io.activej.config.Config
import io.velo.BaseCommand
import io.velo.RequestHandler
import io.velo.SnowFlake
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.MultiBulkReply
import spock.lang.Specification

class CommandCommandTest extends Specification {
    def _CommandCommand = new CommandCommand()

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _CommandCommand.parseSlots('command', data1, 1).size() == 0
    }

    def 'test handle'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)

        when:
        def reply = commandCommand.execute('command')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = commandCommand.execute('command count')
        then:
        reply instanceof IntegerReply

        when:
        reply = commandCommand.execute('command docs')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = commandCommand.execute('command getkeys get')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = commandCommand.execute('command zzz')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test getkeys'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)
        commandCommand.from(cGroup)

        and:
        def snowFlake = new SnowFlake(1, 1)
        commandCommand.requestHandler = new RequestHandler((byte) 0, (byte) 1, cGroup.slotNumber, snowFlake, Config.create())

        when:
        def reply = commandCommand.execute('command getkeys mget a b')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        reply = commandCommand.execute('command getkeys acl whoami')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        // invalid command
        reply = commandCommand.execute('command getkeys 123 123')
        then:
        reply instanceof ErrorReply
    }
}
