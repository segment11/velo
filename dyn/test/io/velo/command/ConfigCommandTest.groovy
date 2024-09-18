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
        def data3 = new byte[3][]
        data3[0] = 'config'.bytes
        data3[1] = 'get'.bytes
        data3[2] = 'key'.bytes

        def cGroup = new CGroup('config', data3, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        when:
        def reply = configCommand._get()
        then:
        reply == NilReply.INSTANCE

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        data3[2] = 'max_connections'.bytes
        reply = configCommand._get()
        then:
        reply instanceof BulkReply

        when:
        def data1 = new byte[1][]
        data1[0] = 'config'.bytes
        configCommand.data = data1
        reply = configCommand._get()
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test set'() {
        given:
        def data4 = new byte[4][]
        data4[0] = 'config'.bytes
        data4[1] = 'set'.bytes
        data4[2] = 'key'.bytes
        data4[3] = 'value'.bytes

        def cGroup = new CGroup('config', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def configCommand = new ConfigCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = configCommand._set()
        then:
        reply == OKReply.INSTANCE

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        data4[2] = 'max_connections'.bytes
        data4[3] = '100'.bytes
        reply = configCommand._set()
        then:
        reply == OKReply.INSTANCE

        when:
        def data1 = new byte[1][]
        data1[0] = 'config'.bytes
        configCommand.data = data1
        reply = configCommand._set()
        then:
        reply == ErrorReply.SYNTAX

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
