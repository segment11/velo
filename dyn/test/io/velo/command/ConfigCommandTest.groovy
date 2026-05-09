package io.velo.command

import io.velo.BaseCommand
import io.velo.MultiWorkerServer
import io.velo.SocketInspector
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.*
import spock.lang.Specification

class ConfigCommandTest extends Specification {
    def _ConfigCommand = new ConfigCommand()

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _ConfigCommand.parseSlots('config', data1, 1).size() == 0
    }

    def 'test handle - subcommand routing'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        expect:
        def reply = configCommand.execute(input)
        reply.getClass() == replyType

        where:
        input            | replyType
        'config help'    | MultiBulkReply
        'config'         | ErrorReply
        'config set key' | ErrorReply
        'config get key' | NilReply
        'config get'     | ErrorReply
        'config xxx key' | ErrorReply
    }

    def 'test get'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()

        expect:
        configCommand.execute("config get ${key}").getClass() == replyType

        where:
        key               | replyType
        'unknown_key'     | NilReply
        'max_connections' | BulkReply
    }

    def 'test set - unsupported key returns error'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        expect:
        configCommand.execute("config set ${key} ${value}") == ErrorReply.SYNTAX

        where:
        key       | value
        'key'     | 'value'
        'timeout' | '10'
    }

    def 'test set - valid and invalid max_connections'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()

        expect:
        configCommand.execute("config set max_connections ${value}") == expected

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()

        where:
        value | expected
        '100' | OKReply.INSTANCE
        '0'   | ErrorReply.INVALID_INTEGER
        '-1'  | ErrorReply.INVALID_INTEGER
    }
}
