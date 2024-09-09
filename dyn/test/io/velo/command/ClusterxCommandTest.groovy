package io.velo.command

import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.reply.*
import spock.lang.Specification

class ClusterxCommandTest extends Specification {
    def _ClusterxCommand = new ClusterxCommand()

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _ClusterxCommand.parseSlots('cluster', data1, 1).size() == 1
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        when:
        def reply = clusterx.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'info'.bytes
        clusterx.data = data2
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'migrate'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'nodes'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'setnodeid'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'setnodes'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'setslot'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'slots'.bytes
        reply = clusterx.handle()
        then:
        reply == ClusterxCommand.CLUSTER_DISABLED

        when:
        data2[1] = 'zzz'.bytes
        reply = clusterx.handle()
        then:
        reply == ErrorReply.SYNTAX
    }

    private List<String> infoToLines(Reply reply) {
        if (reply instanceof BulkReply) {
            return new String(((BulkReply) reply).raw).split('\n')
        } else if (reply instanceof AsyncReply) {
            return new String(((BulkReply) reply.settablePromise.getResult()).raw).split('\n')
        } else {
            throw new RuntimeException("reply type error: ${reply.getClass()}")
        }
    }

    def 'test info'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        def reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('cluster_state:fail') } != null

        when:
        localPersist.multiShard.shards[0].multiSlotRange.addSingle(0, 16383)
        reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('cluster_state:ok') } != null
        infoToLines((AsyncReply) reply).find { it.contains('migrating_state:success') } != null

        when:
        localPersist.multiShard.shards[0].exportMigratingSlot = Shard.FAIL_MIGRATED_SLOT
        reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('migrating_state:fail') } != null

        when:
        localPersist.multiShard.shards[0].exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        localPersist.multiShard.shards[0].importMigratingSlot = Shard.FAIL_MIGRATED_SLOT
        reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('migrating_state:fail') } != null

        when:
        localPersist.multiShard.shards[0].exportMigratingSlot = 0
        reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('migrating_slot:0') } != null

        when:
        localPersist.multiShard.shards[0].exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        localPersist.multiShard.shards[0].importMigratingSlot = 0
        reply = clusterx.info()
        then:
        infoToLines((AsyncReply) reply).find { it.contains('migrating_slot:0') } != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test migrate'() {
        given:
        def data4 = new byte[4][]

        def cGroup = new CGroup('cluster', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        var multiShard = localPersist.multiShard
        var shards = multiShard.shards
        data4[2] = '0'.bytes
        data4[3] = shards[0].nodes[0].nodeId().bytes
        def reply = clusterx.migrate()
        then:
        // self shard is to shard
        reply instanceof ErrorReply

        when:
        shards[0].nodes[0].master = false
        reply = clusterx.migrate()
        then:
        // self node is not master
        reply instanceof ErrorReply

        when:
        shards[0].nodes[0].master = true
        shards << new Shard()
        shards[1].nodes << new Node(master: false, nodeIdFix: 'new_node_id', host: 'localhost', port: 7379)
        data4[3] = shards[1].nodes[0].nodeId().bytes
        reply = clusterx.migrate()
        then:
        // to shard node is not master
        reply instanceof ErrorReply

        when:
        shards[1].nodes[0].master = true
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == 0
        shards[1].importMigratingSlot == 0
        shards[0].migratingToHost == 'localhost'
        shards[0].migratingToPort == 7379

        when:
        // skip
        data4[2] = '1'.bytes
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK

        when:
        data4[3] = 'xxx_not_exist_node_id'.bytes
        reply = clusterx.migrate()
        then:
        reply instanceof ErrorReply

        when:
        def data3 = new byte[3][]
        clusterx.data = data3
        reply = clusterx.migrate()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test nodes'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        var multiShard = localPersist.multiShard
        var shards = multiShard.shards
        def reply = clusterx.nodes()
        then:
        infoToLines((BulkReply) reply).find { it.contains(shards[0].nodes[0].nodeId()) } != null

        // setnodeid
        when:
        reply = clusterx.setnodeid()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        data3[1] = 'setnodeid'.bytes
        data3[2] = 'new_node_id'.bytes
        clusterx.data = data3
        reply = clusterx.setnodeid()
        then:
        reply == ClusterxCommand.OK
        shards[0].nodes[0].nodeId() == 'new_node_id'

        // setnodes
        when:
        reply = clusterx.setnodes()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data4 = new byte[4][]
        data4[1] = 'setnodes'.bytes
        data4[2] = 'new_node_id localhost 7379 master - 0 10-20 \nnew_node_id2 localhost 7380 slave new_node_id\n'.bytes
        // cluster version
        data4[3] = '1'.bytes
        clusterx.data = data4
        reply = clusterx.setnodes()
        then:
        reply == ClusterxCommand.OK
        shards[0].nodes.size() == 2
        shards[0].multiSlotRange.list.size() == 2

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test slots'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        var multiShard = localPersist.multiShard
        var shards = multiShard.shards

        def shard1 = shards[0]
        shard1.multiSlotRange.addSingle(0, 16383)
        def reply = clusterx.slots()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'slots'.bytes
        clusterx.data = data2
        reply = clusterx.slots()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof MultiBulkReply

        // setslot
        when:
        reply = clusterx.setslot()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = shard1.nodes[0].nodeId().bytes
        // cluster version
        data6[5] = '2'.bytes
        clusterx.data = data6
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        def shard2 = new Shard()
        shard2.multiSlotRange.addSingle(0, 8191)
        shards << shard2
        shard1.multiSlotRange.list.clear()
        shard1.multiSlotRange.addSingle(8192, 16383)
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.getResult() == ClusterxCommand.OK
        shard1.multiSlotRange.contains(0)
        !shard2.multiSlotRange.contains(0)

        when:
        data6[4] = 'xxx_not_exist_node_id'.bytes
        reply = clusterx.setslot()
        then:
        reply instanceof ErrorReply

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
