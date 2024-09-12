package io.velo.command

import io.velo.BaseCommand
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import spock.lang.Specification

class InfoCommandTest extends Specification {
    def _InfoCommand = new InfoCommand()

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _InfoCommand.parseSlots('info', data1, 1).size() == 1
    }

    def 'test handle'() {
        given:
        def data3 = new byte[3][]

        def iGroup = new IGroup('info', data3, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        when:
        def reply = infoCommand.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data1 = new byte[1][]
        data1[0] = 'info'.bytes
        infoCommand.data = data1
        reply = infoCommand.handle()
        then:
        reply instanceof BulkReply

        when:
        def data2 = new byte[2][]
        data2[1] = 'info'.bytes
        infoCommand.data = data2
        reply = infoCommand.handle()
        then:
        reply instanceof BulkReply
    }

    def 'test replication'() {
        given:
        def data2 = new byte[2][]
        data2[0] = 'info'.bytes
        data2[1] = 'replication'.bytes

        def iGroup = new IGroup('info', data2, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def reply = infoCommand.handle()
        then:
        ClusterxCommandTest.infoToLines(reply).find { it.contains('master_link_status:down') } != null

        when:
        oneSlot.doMockWhenCreateReplPairAsSlave = true
        def replPairAsSlave = oneSlot.createReplPairAsSlave('localhost', 7379)
        reply = infoCommand.handle()
        then:
        ClusterxCommandTest.infoToLines(reply).find { it.contains('connected_slaves:0') } != null

        when:
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        reply = infoCommand.handle()
        then:
        ClusterxCommandTest.infoToLines(reply).find { it.contains('master_repl_offset:1024') } != null
        ClusterxCommandTest.infoToLines(reply).find { it.contains('slave_repl_offset:1024') } != null

        when:
        oneSlot.removeReplPairAsSlave()
        def replPairAsMaster = oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 7380)
        reply = infoCommand.handle()
        then:
        ClusterxCommandTest.infoToLines(reply).find { it.contains('connected_slaves:1') } != null

        when:
        oneSlot.binlog.moveToNextSegment(true)
        replPairAsMaster.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        reply = infoCommand.handle()
        then:
        ClusterxCommandTest.infoToLines(reply).find { it.contains('master_repl_offset:1048576') } != null
        ClusterxCommandTest.infoToLines(reply).find { it.contains('slave_repl_offset:1024') } != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
