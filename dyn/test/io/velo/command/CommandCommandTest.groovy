package io.velo.command

import io.activej.config.Config
import io.velo.BaseCommand
import io.velo.RequestHandler
import io.velo.SnowFlake
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
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
        reply instanceof MultiBulkReply
        // bare COMMAND returns info for all commands
        (reply as MultiBulkReply).replies.length == CommandRegistry.size()
        // each element is itself a 10-element array
        ((reply as MultiBulkReply).replies[0] as MultiBulkReply).replies.length == 10

        when:
        reply = commandCommand.execute('command count')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == CommandRegistry.size()
        (reply as IntegerReply).integer > 0

        when:
        reply = commandCommand.execute('command list')
        then:
        reply instanceof MultiBulkReply
        def listReply = reply as MultiBulkReply
        listReply.replies.length == CommandRegistry.size()
        listReply.replies.length > 0
        // names match the registry, insertion order preserved
        def expectedNames = CommandRegistry.all().collect { it.name() }
        def actualNames = listReply.replies.collect { new String((it as BulkReply).raw) }
        actualNames == expectedNames

        when: 'filter by aclcat connection'
        reply = commandCommand.execute('command list filterby aclcat connection')
        then:
        reply instanceof MultiBulkReply
        def connNames = (reply as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        connNames.size() == CommandRegistry.all().count { it.aclCategories().contains('@connection') }
        connNames.every { CommandRegistry.get(it).aclCategories().contains('@connection') }

        when: 'filter by pattern cl*'
        reply = commandCommand.execute('command list filterby pattern cl*')
        then:
        reply instanceof MultiBulkReply
        def patNames = (reply as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        patNames == CommandRegistry.all().collect { it.name() }.findAll { it.startsWith('cl') }

        when: 'filter by pattern is case-insensitive (Redis stringmatchlen nocase)'
        reply = commandCommand.execute('command list filterby pattern CL*')
        then:
        reply instanceof MultiBulkReply
        def ciNames = (reply as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        ciNames == patNames

        when: 'filter by module (always empty)'
        reply = commandCommand.execute('command list filterby module foo')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 0

        when: 'invalid filter type'
        reply = commandCommand.execute('command list filterby bogus foo')
        then:
        reply == ErrorReply.SYNTAX

        when: 'filter missing value'
        reply = commandCommand.execute('command list filterby aclcat')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = commandCommand.execute('command docs')
        then:
        reply instanceof MultiBulkReply
        // DOCS returns a map (pairs of name, docsMap); group is always present
        (reply as MultiBulkReply).replies.length == CommandRegistry.size() * 2

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

    def 'test getkeysandflags'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)
        commandCommand.from(cGroup)

        and:
        def snowFlake = new SnowFlake(1, 1)
        commandCommand.requestHandler = new RequestHandler((byte) 0, (byte) 1, cGroup.slotNumber, snowFlake, Config.create())

        when: 'write command -> RW, access'
        def reply = commandCommand.execute('command getkeysandflags mset a 1 b 2')
        then:
        reply instanceof MultiBulkReply
        def pairs = (reply as MultiBulkReply).replies
        pairs.length == 2
        def first = pairs[0] as MultiBulkReply
        new String((first.replies[0] as BulkReply).raw) == 'a'
        def flags = (first.replies[1] as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        flags.contains('RW')
        flags.contains('access')

        when: 'read command -> RO, access'
        reply = commandCommand.execute('command getkeysandflags mget a b')
        then:
        def pairs2 = (reply as MultiBulkReply).replies
        def readFlags = ((pairs2[0] as MultiBulkReply).replies[1] as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        readFlags.contains('RO')
        readFlags.contains('access')
    }

    def 'test info'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)
        commandCommand.from(cGroup)

        when: 'info for a known command (copy)'
        def reply = commandCommand.execute('command info copy')
        then:
        reply instanceof MultiBulkReply
        def outer = reply as MultiBulkReply
        outer.replies.length == 1
        def ten = outer.replies[0] as MultiBulkReply
        ten.replies.length == 10
        new String((ten.replies[0] as BulkReply).raw) == 'copy'
        (ten.replies[1] as IntegerReply).integer == -3
        (ten.replies[3] as IntegerReply).integer == 1
        (ten.replies[4] as IntegerReply).integer == 2
        (ten.replies[5] as IntegerReply).integer == 1
        def flags = (ten.replies[2] as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) } as Set
        flags.contains('write')

        when: 'info for known + unknown name'
        reply = commandCommand.execute('command info copy doesnotexist')
        then:
        def outer2 = reply as MultiBulkReply
        outer2.replies.length == 2
        outer2.replies[0] instanceof MultiBulkReply
        outer2.replies[1] == NilReply.INSTANCE

        when: 'info with no names -> all commands'
        reply = commandCommand.execute('command info')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == CommandRegistry.size()
    }

    def 'test docs'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)
        commandCommand.from(cGroup)

        when: 'docs for a known command'
        def reply = commandCommand.execute('command docs copy')
        then:
        reply instanceof MultiBulkReply
        def docsPairs = reply as MultiBulkReply
        docsPairs.replies.length == 2
        new String((docsPairs.replies[0] as BulkReply).raw) == 'copy'
        def docsMap = docsPairs.replies[1] as MultiBulkReply
        def mapKeys = new ArrayList<String>()
        for (int i = 0; i < docsMap.replies.length; i += 2) {
            mapKeys.add(new String((docsMap.replies[i] as BulkReply).raw))
        }
        mapKeys.contains('group')
        mapKeys.contains('summary')
        mapKeys.contains('since')
        mapKeys.contains('complexity')

        when: 'docs for unknown command is skipped'
        reply = commandCommand.execute('command docs doesnotexist')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 0
    }

    def 'test help'() {
        given:
        def cGroup = new CGroup('command', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def commandCommand = new CommandCommand(cGroup)
        commandCommand.from(cGroup)

        when:
        def reply = commandCommand.execute('command help')
        then:
        reply instanceof MultiBulkReply
        def helpReplies = (reply as MultiBulkReply).replies
        helpReplies.length == 21
        new String((helpReplies[0] as BulkReply).raw).startsWith('COMMAND <subcommand>')
        def texts = helpReplies.collect { new String((it as BulkReply).raw) }
        texts.contains('COUNT')
        texts.contains('LIST')
        texts.contains('HELP')
    }
}
