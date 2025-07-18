package io.velo.command

import io.velo.BaseCommand
import io.velo.MultiWorkerServer
import io.velo.SocketInspector
import io.velo.monitor.RuntimeCpuCollector
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.AsyncReply
import io.velo.reply.BulkReply
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

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()

        when:
        def reply = infoCommand.handle()
        then:
        reply instanceof BulkReply

        when:
        data3[1] = 'server'.bytes
        data3[2] = 'keyspace'.bytes
        reply = infoCommand.handle()
        then:
        reply instanceof AsyncReply

        when:
        def data1 = new byte[1][]
        data1[0] = 'info'.bytes
        infoCommand.data = data1
        reply = infoCommand.handle()
        then:
        reply instanceof AsyncReply

        when:
        def data2 = new byte[2][]
        data2[1] = 'zzz'.bytes
        infoCommand.data = data2
        reply = infoCommand.handle()
        then:
        reply instanceof BulkReply

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test server'() {
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        // 10 seconds ago
        MultiWorkerServer.UP_TIME = System.currentTimeMillis() - 1000 * 10
        def reply = infoCommand.execute('info server')
        then:
        reply instanceof BulkReply
        new String((reply as BulkReply).raw).contains('uptime_in_seconds:10')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test clients'(){
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        and:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()

        when:
        def reply = infoCommand.execute('info clients')
        then:
        reply instanceof BulkReply
        new String((reply as BulkReply).raw).contains('connected_clients:')
    }

    def 'test memory'() {
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        when:
        def reply = infoCommand.execute('info memory')
        then:
        reply instanceof BulkReply
        new String((reply as BulkReply).raw).contains('total_system_memory:')
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
        def lines = ClusterxCommandTest.infoToLines(reply)
        then:
        lines.find { it.contains('master_host:') } != null
        lines.find { it.contains('master_port:') } != null
        lines.find { it.contains('master_link_status:down') } != null

        when:
        oneSlot.doMockWhenCreateReplPairAsSlave = true
        def replPairAsSlave = oneSlot.createReplPairAsSlave('localhost', 7379)
        reply = infoCommand.handle()
        lines = ClusterxCommandTest.infoToLines(reply)
        then:
        lines.find { it.contains('connected_slaves:0') } != null

        when:
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        reply = infoCommand.handle()
        lines = ClusterxCommandTest.infoToLines(reply)
        then:
        lines.find { it.contains('master_repl_offset:1024') } != null
        lines.find { it.contains('slave_repl_offset:1024') } != null

        when:
        oneSlot.removeReplPairAsSlave()
        def replPairAsMaster = oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 7380)
        reply = infoCommand.handle()
        lines = ClusterxCommandTest.infoToLines(reply)
        then:
        lines.find { it.contains('connected_slaves:1') } != null

        when:
        oneSlot.binlog.moveToNextSegment(true)
        replPairAsMaster.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1024L)
        reply = infoCommand.handle()
        lines = ClusterxCommandTest.infoToLines(reply)
        then:
        lines.find { it.contains('master_repl_offset:1048576') } != null
        lines.find { it.contains('slave_repl_offset:1024') } != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test cpu'() {
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        when:
        def reply = infoCommand.execute('info cpu')
        then:
        reply instanceof BulkReply
        new String((reply as BulkReply).raw).contains('used_cpu_sys:')

        cleanup:
        RuntimeCpuCollector.close()
    }

    def 'test cluster'(){
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        when:
        def reply = infoCommand.execute('info cluster')
        then:
        reply instanceof BulkReply
        new String((reply as BulkReply).raw).contains('cluster_enabled:')

    }

    def 'test keyspace'() {
        given:
        def iGroup = new IGroup('info', null, null)
        iGroup.from(BaseCommand.mockAGroup())
        def infoCommand = new InfoCommand(iGroup)

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = infoCommand.execute('info keyspace')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof BulkReply
                    && new String((result as BulkReply).raw).contains('keys:0')
        }.result

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
