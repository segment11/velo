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

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[0] = 'config'.bytes
        data2[1] = 'help'.bytes

        def cGroup = new CGroup('config', data2, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        when:
        def reply = configCommand.handle()
        then:
        reply instanceof MultiBulkReply

        when:
        def data1 = new byte[1][]
        data1[0] = 'config'.bytes
        configCommand.data = data1
        reply = configCommand.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        data3[0] = 'config'.bytes
        data3[1] = 'set'.bytes
        data3[2] = 'key'.bytes
        configCommand.data = data3
        reply = configCommand.handle()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data3[1] = 'get'.bytes
        reply = configCommand.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        data3[1] = 'xxx'.bytes
        reply = configCommand.handle()
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test get'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        when:
        def reply = configCommand.execute('config get key')
        then:
        reply == NilReply.INSTANCE

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        reply = configCommand.execute('config get max_connections')
        then:
        reply instanceof BulkReply
    }

    def 'test set'() {
        given:
        def cGroup = new CGroup('config', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = configCommand.execute('config set key value')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = configCommand.execute('config set timeout 10')
        then:
        reply == ErrorReply.SYNTAX

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        reply = configCommand.execute('config set max_connections 100')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = configCommand.execute('config set max_connections 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = configCommand.execute('config set max_connections -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = configCommand.execute('config')
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
