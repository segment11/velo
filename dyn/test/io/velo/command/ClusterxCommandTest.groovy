package io.velo.command

import io.activej.config.Config
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.SnowFlake
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.persist.OneSlot
import io.velo.repl.LeaderSelector
import io.velo.repl.cluster.MultiShard
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.reply.*
import org.apache.commons.codec.digest.DigestUtils
import spock.lang.Specification

class ClusterxCommandTest extends Specification {
    def _ClusterxCommand = new ClusterxCommand()

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _ClusterxCommand.parseSlots('cluster', data1, 1).size() == 1

        when:
        def data3 = new byte[3][]
        data3[1] = 'countkeysinslot'.bytes
        data3[2] = '0'.bytes
        then:
        _ClusterxCommand.parseSlots('cluster', data3, 1).size() == 1

        when:
        data3[2] = 'a'.bytes
        then:
        _ClusterxCommand.parseSlots('cluster', data3, 1).size() == 0

        when:
        data3[2] = '-1'.bytes
        then:
        _ClusterxCommand.parseSlots('cluster', data3, 1).size() == 0

        when:
        data3[2] = '16384'.bytes
        then:
        _ClusterxCommand.parseSlots('cluster', data3, 1).size() == 0

        when:
        data3[1] = 'getkeysinslot'.bytes
        then:
        _ClusterxCommand.parseSlots('cluster', data3, 1).size() == 1
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
        clusterx.data = data2
        def subCmdList = '''
addslots
addslotsrange
delslots
delslotsrange
failover
flushslots
countkeysinslot
getkeysinslot
info
migrate
meet
myid
myshardid
nodes
replicas
replicate
reset
saveconfig
slaves
setnodeid
setnodes
setslot
shards
slots
'''.readLines().collect { it.trim() }.findAll { it }
        then:
        subCmdList.every {
            data2[1] = it.bytes
            def r = clusterx.handle() == ClusterxCommand.CLUSTER_DISABLED
            if (!r) {
                println it + '!!!'
            }
            r
        }

        when:
        data2[1] = 'keyslot'.bytes
        reply = clusterx.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'zzz'.bytes
        reply = clusterx.handle()
        then:
        reply == ErrorReply.SYNTAX
    }

    static List<String> infoToLines(Reply reply) {
        if (reply instanceof BulkReply) {
            return new String(((BulkReply) reply).raw).split('\n')
        } else if (reply instanceof AsyncReply) {
            return new String(((BulkReply) reply.settablePromise.getResult()).raw).split('\n')
        } else {
            throw new RuntimeException("reply type error: ${reply.getClass()}")
        }
    }

    def 'test addslots'() {
        given:
        def cGroup = new CGroup('cluster', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        def reply = clusterx.execute('cluster addslots')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = clusterx.execute('cluster addslots 0 1')
        then:
        reply == ClusterxCommand.OK

        when:
        reply = clusterx.execute('cluster addslotsrange 10 20')
        then:
        reply == ClusterxCommand.OK

        when:
        reply = clusterx.execute('cluster addslotsrange 10 20 30')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = clusterx.execute('cluster addslotsrange 10')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = clusterx.execute('cluster addslots 0 1')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains('busy')

        when:
        reply = clusterx.execute('cluster delslots 0 1')
        then:
        reply == ClusterxCommand.OK

        when:
        reply = clusterx.execute('cluster delslotsrange 0 1')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains('not in my range')

        when:
        def multiShard = localPersist.multiShard
        multiShard.shards[0].multiSlotRange.list.clear()
        // my self
        multiShard.shards[0].nodes[0].master = false
        multiShard.shards[0].nodes << new Node(master: true, host: 'localhost', port: 7380)
        reply = clusterx.execute('cluster addslots 0 1')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains('only master can')

        when:
        // flushslots
        multiShard.shards[0].multiSlotRange.addSingle(0, 1024)
        // has data
        cGroup.set('a', 'a'.bytes)
        reply = clusterx.execute('cluster flushslots')
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('there are data')

        when:
        // clear data
        def firstOneSlot = localPersist.firstOneSlot()
        firstOneSlot.flush()
        reply = clusterx.execute('cluster flushslots')
        then:
        reply == ClusterxCommand.OK

        when:
        reply = clusterx.execute('cluster flushslots 0')
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test countkeysinslot and getkeysinslot'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'countkeysinslot'.bytes
        data3[2] = '0'.bytes

        def cGroup = new CGroup('cluster', data3, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        ConfForGlobal.clusterEnabled = true
        def reply = clusterx.countkeysinslot()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data3[2] = 'a'.bytes
        reply = clusterx.countkeysinslot()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = '-1'.bytes
        reply = clusterx.countkeysinslot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = '16384'.bytes
        reply = clusterx.countkeysinslot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data2 = new byte[2][]
        clusterx.data = data2
        reply = clusterx.countkeysinslot()
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = clusterx.getkeysinslot()
        then:
        reply == ErrorReply.FORMAT

        when:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)
        and:
        clusterx.data = null
        reply = clusterx.execute('cluster getkeysinslot 0 10')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == MultiBulkReply.EMPTY
        }.result

        when:
        def toClientSlot0Keys = Mock.prepareToClientSlotKeyList((short) 1, (short) 0)
        for (kk in toClientSlot0Keys) {
            cGroup.set(kk, kk.bytes)
        }
        Thread.sleep(100)
        reply = clusterx.execute('cluster getkeysinslot 0 10')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply
            (result as MultiBulkReply).replies.length == 1
        }.result

        when:
        reply = clusterx.execute('cluster getkeysinslot a 10')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = clusterx.execute('cluster getkeysinslot -1 10')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = clusterx.execute('cluster getkeysinslot 16384 10')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = clusterx.execute('cluster getkeysinslot 0 a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = clusterx.execute('cluster getkeysinslot 0 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = clusterx.execute('cluster getkeysinslot 0 10001')
        then:
        // count should <= 10000
        reply instanceof ErrorReply

        cleanup:
        localPersist.cleanUp()
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
        def oneSlot = localPersist.oneSlot(slot)

        def shards = localPersist.multiShard.shards
        def shard0 = shards[0]

        when:
        ConfForGlobal.clusterEnabled = true
        def reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('cluster_state:fail') } != null

        when:
        shard0.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard0.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard0.multiSlotRange.addSingle(0, 16383)
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('cluster_state:ok') } != null
        infoToLines(reply).find { it.contains('migrating_state:success') } != null

        when:
        shard0.exportMigratingSlot = 0
        shard0.migratingToHost = 'localhost'
        shard0.migratingToPort = 7380
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:0') } != null

        when:
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 7380)
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:0') } != null

        when:
        oneSlot.replPairAsMasterList[0].allCaughtUp = true
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:success') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:0') } != null

        when:
        shard0.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard0.importMigratingSlot = 0
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:-1') } != null
        infoToLines(reply).find { it.contains('import_migrating_slot:0') } != null

        when:
        shard0.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard0.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:success') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:-1') } != null
        infoToLines(reply).find { it.contains('import_migrating_slot:-1') } != null

        when:
        // not my self shard
        def shard1 = new Shard()
        shard1.importMigratingSlot = 0
        shards << shard1
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:-1') } != null
        infoToLines(reply).find { it.contains('import_migrating_slot:0') } != null

        when:
        shard1.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard1.exportMigratingSlot = 0
        reply = clusterx.info()
        then:
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:0') } != null
        infoToLines(reply).find { it.contains('import_migrating_slot:-1') } != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test keyslot'() {
        given:
        def data3 = new byte[3][]
        data3[2] = 'somekey'.bytes

        def cGroup = new CGroup('cluster', data3, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        when:
        def reply = clusterx.keyslot()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 11058
    }

    def 'test info does not NPE on export path when the local slot is released/null'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        def savedSlotNumber = ConfForGlobal.slotNumber
        ConfForGlobal.slotNumber = (short) 2
        // initialize both slots with cluster disabled (lazy-init would skip unowned slot 1 otherwise);
        // a prior test may have left clusterEnabled=true, so force it off here
        ConfForGlobal.clusterEnabled = false

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        def shard0 = localPersist.multiShard.shards[0]

        and: 'inner slot 1 is released (null), while the shard is stale-marked as exporting a client slot in it'
        localPersist.releaseOneSlot((short) 1)
        ConfForGlobal.clusterEnabled = true
        shard0.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        shard0.exportMigratingSlot = 8192  // client slot 8192 -> inner slot 1 for slotNumber=2

        expect:
        localPersist.oneSlot((short) 1) == null

        when: 'clusterx info reports the export without dereferencing the null local slot'
        def reply = clusterx.info()

        then:
        reply != null
        infoToLines(reply).find { it.contains('migrating_state:migrating') } != null
        infoToLines(reply).find { it.contains('export_migrating_slot:8192') } != null

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        ConfForGlobal.slotNumber = savedSlotNumber
        localPersist.cleanUp()
        localPersist.resetForTest()
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
        shards[0].multiSlotRange.addSingle(0, 16383)
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == 0
        shards[0].migratingToHost == 'localhost'
        shards[0].migratingToPort == 7379

        when: 'cancel clears the in-flight export (source keeps ownership, slot migratable again)'
        data4[3] = 'cancel'.bytes
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == Shard.NO_MIGRATING_SLOT

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

    def 'test migrate rejects when another migration is in progress'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        def savedSlotNumber = ConfForGlobal.slotNumber
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = (short) 4

        def data4 = new byte[4][]
        def cGroup = new CGroup('cluster', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        shards[0].nodes[0].master = true
        // this shard owns the slots it exports (client 0..8191 = inner slots 0 and 1 for slotNumber=4)
        shards[0].multiSlotRange.addSingle(0, 8191)
        shards << new Shard()
        shards[1].nodes << new Node(master: true, nodeIdFix: 'new_node_id', host: 'localhost', port: 7379)
        shards << new Shard()
        shards[2].nodes << new Node(master: true, nodeIdFix: 'other_node_id', host: 'localhost', port: 7380)

        when: 'a first migrate of aligned slot 0 starts and records the in-progress export slot'
        data4[2] = '0'.bytes
        data4[3] = shards[1].nodes[0].nodeId().bytes
        def reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == 0

        when: 'a second migrate for a different aligned slot arrives while the first is in progress'
        data4[2] = '4096'.bytes
        reply = clusterx.migrate()
        then: 'it is rejected and the in-progress migration is left intact'
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('in progress')
        shards[0].exportMigratingSlot == 0

        when: 're-targeting the same slot to a different node while in progress is also rejected'
        data4[2] = '0'.bytes
        data4[3] = shards[2].nodes[0].nodeId().bytes
        reply = clusterx.migrate()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('in progress')
        shards[0].migratingToPort == 7379

        when: 're-issuing the exact same migrate (same slot and target) is idempotent'
        data4[2] = '0'.bytes
        data4[3] = shards[1].nodes[0].nodeId().bytes
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == 0

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        ConfForGlobal.slotNumber = savedSlotNumber
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test migrate rejects a slot not owned by this shard'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data4 = new byte[4][]
        def cGroup = new CGroup('cluster', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def shards = localPersist.multiShard.shards
        shards[0].nodes[0].master = true
        shards << new Shard(nodes: [new Node(master: true, nodeIdFix: 'to_node', host: 'localhost', port: 7379)])

        when: 'this shard owns no slots — exporting slot 0 is rejected and no export marker is set'
        shards[0].multiSlotRange.list.clear()
        data4[2] = '0'.bytes
        data4[3] = 'to_node'.bytes
        def reply = clusterx.migrate()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('not owned')
        shards[0].exportMigratingSlot == Shard.NO_MIGRATING_SLOT

        when: 'once the shard owns the slot, the migrate is accepted'
        shards[0].multiSlotRange.addSingle(0, 16383)
        reply = clusterx.migrate()
        then:
        reply == ClusterxCommand.OK
        shards[0].exportMigratingSlot == 0

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test meet'() {
        given:
        def cGroup = new CGroup('cluster', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        when:
        ConfForGlobal.clusterEnabled = true
        def reply = clusterx.execute('cluster meet 127.0.0.1 6379')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains('not implemented')

        when:
        reply = clusterx.execute('cluster meet 127.0.0.1 -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = clusterx.execute('cluster meet 127.0.0.1 65536')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = clusterx.execute('cluster meet 127.0.0.1 aaa')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = clusterx.execute('cluster meet aaa 6379')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = clusterx.execute('cluster meet 127.0.0.1')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test myid and myshardid and saveconfig and shards and reset'() {
        given:
        def data2 = new byte[2][]

        def cGroup = new CGroup('cluster', data2, null)
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
        def mySelfNodeId = shards[0].nodes[0].nodeId()
        def reply = clusterx.myid()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw) == mySelfNodeId

        when:
        def reply2 = clusterx.myshardid()
        then:
        reply2 instanceof BulkReply
        new String(((BulkReply) reply2).raw) == DigestUtils.sha256Hex('shard_0')

        when:
        shards.clear()
        reply = clusterx.myid()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw) == mySelfNodeId

        when:
        reply2 = clusterx.myshardid()
        then:
        reply2 instanceof ErrorReply
        (reply2 as ErrorReply).message == 'not in cluster'

        when:
        def reply3 = clusterx.saveconfig()
        then:
        reply3 == ClusterxCommand.OK

        when:
        shards << new Shard()
        shards[0].nodes << new Node(master: true, host: 'localhost', port: 6379)
        shards[0].nodes << new Node(master: false, host: 'localhost', port: 6380)
        def reply4 = clusterx.shards()
        then:
        reply4 == MultiBulkReply.EMPTY

        when:
        shards[0].multiSlotRange.addSingle(0, 16383)
        def sb = new StringBuilder()
        reply4 = clusterx.shards()
        then:
        reply4 instanceof MultiBulkReply
        ((MultiBulkReply) reply4).dumpForTest(sb, 0)

        when:
        println sb.toString()
        def reply5 = clusterx.reset()
        then:
        reply5 instanceof AsyncReply
        (reply5 as AsyncReply).settablePromise.whenResult { result ->
            result == ClusterxCommand.OK
        }.result

        when:
        def data3 = new byte[3][]
        data3[2] = 'hard'.bytes
        clusterx.data = data3
        reply5 = clusterx.reset()
        then:
        reply5 == ClusterxCommand.OK

        when:
        data3[2] = 'soft'.bytes
        reply5 = clusterx.reset()
        then:
        reply5 == ClusterxCommand.OK

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
        infoToLines(reply).find { it.contains(shards[0].nodes[0].nodeId()) } != null

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

        when:
        shards.clear()
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
        def leaderSelector = LeaderSelector.instance
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'
        def data4 = new byte[4][]
        data4[1] = 'setnodes'.bytes
        data4[2] = 'new_node_id localhost 7379 master - 0 10-20 \nnew_node_id2 localhost 7380 slave new_node_id\n'.bytes
        // cluster version
        data4[3] = '1'.bytes
        clusterx.data = data4
        reply = clusterx.setnodes()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        shards[0].nodes.size() == 2
        shards[0].multiSlotRange.list.size() == 2

        when:
        // master to slave
        data4[2] = 'new_node_id2 localhost 7380 master - 0 10-20 \nnew_node_id localhost 7379 slave new_node_id2\n'.bytes
        reply = clusterx.setnodes()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // slave to master again
        data4[2] = 'new_node_id localhost 7379 master - 0 10-20 \nnew_node_id2 localhost 7380 slave new_node_id\n'.bytes
        reply = clusterx.setnodes()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // delete from cluster, reset as master
        data4[2] = 'new_node_id2 localhost 7380 master - 0 10-20 \n'.bytes
        reply = clusterx.setnodes()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // setnodes error path: my self is slave, reset as slave fails
        def oldMocked = leaderSelector.masterAddressLocalMocked
        try {
            leaderSelector.masterAddressLocalMocked = null
            data4[2] = 'new_node_id2 localhost 7380 master - 0 10-20 \nnew_node_id localhost 7379 slave new_node_id2\n'.bytes
            reply = clusterx.setnodes()
        } finally {
            leaderSelector.masterAddressLocalMocked = oldMocked
        }
        then:
        reply instanceof AsyncReply
        def errResult = (reply as AsyncReply).settablePromise.getResult()
        errResult instanceof ErrorReply
        (errResult as ErrorReply).message.contains('reset as slave')
        !(errResult as ErrorReply).message.contains('reset as master')
        shards[0].nodes.size() == 1
        shards[0].nodes[0].nodeId() == 'new_node_id2'

        when:
        // clean up: delete from cluster so subsequent failover test sees 'not in cluster'
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'
        data4[2] = 'new_node_id2 localhost 7380 master - 0 10-20 \n'.bytes
        reply = clusterx.setnodes()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // failover
        data3[1] = 'failover'.bytes
        data3[2] = 'force'.bytes
        clusterx.data = data3
        reply = clusterx.failover()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('not in cluster')

        when:
        shards[0].nodes[0].mySelf = true
        reply = clusterx.failover()
        then:
        reply == ClusterxCommand.OK

        when:
        shards[0].nodes[0].master = false
        shards[0].nodes << new Node(nodeIdFix: 'old_master', master: true, slaveIndex: 0)
        reply = clusterx.failover()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        clusterx.data = data4
        reply = clusterx.failover()
        then:
        reply == ErrorReply.FORMAT

        // replicate
        when:
        // two shards
        def shard0 = new Shard()
        shard0.nodes << new Node(nodeIdFix: 'new_node_id1', master: true, slaveIndex: 0, mySelf: true)
        shard0.nodes << new Node(nodeIdFix: 'new_node_id2', master: false, slaveIndex: 0, followNodeId: 'new_node_id1')
        def shard1 = new Shard()
        shard1.nodes << new Node(nodeIdFix: 'new_node_id3', master: true, slaveIndex: 0)
        shard1.nodes << new Node(nodeIdFix: 'new_node_id4', master: false, slaveIndex: 0, followNodeId: 'new_node_id3')
        shards.clear()
        shards << shard0
        shards << shard1
        data3[1] = 'replicate'.bytes
        data3[2] = 'not_exist_node_id'.bytes
        clusterx.data = data3
        reply = clusterx.replicate()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('master node id not found')

        when:
        // myself
        data3[2] = 'new_node_id1'.bytes
        reply = clusterx.replicate()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('self node id is')

        when:
        // self is master
        data3[2] = 'new_node_id3'.bytes
        reply = clusterx.replicate()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains('self node is master')

        when:
        // replicate to shard1
        shard0.nodes[0].mySelf = false
        shard0.nodes[1].mySelf = true
        reply = clusterx.replicate()
        println shard0
        println shard1
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // replicate to shard0
        data3[2] = 'new_node_id1'.bytes
        reply = clusterx.replicate()
        println shard0
        println shard1
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // replicate error path: reset as slave fails
        def oldMockedR = leaderSelector.masterAddressLocalMocked
        try {
            leaderSelector.masterAddressLocalMocked = null
            reply = clusterx.replicate()
        } finally {
            leaderSelector.masterAddressLocalMocked = oldMockedR
        }
        then:
        reply instanceof AsyncReply
        def errReply = (reply as AsyncReply).settablePromise.getResult()
        errReply instanceof ErrorReply
        (errReply as ErrorReply).message.contains('reset as slave')
        !(errReply as ErrorReply).message.contains('reset as master')

        when:
        // replicate to shard1 where existing slave has slaveIndex=5 (gap)
        // new slave must get smallest unused non-negative slaveIndex (0), not size-1 (1)
        shard1.nodes[1].slaveIndex = 5
        data3[2] = 'new_node_id3'.bytes
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'
        reply = clusterx.replicate()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        shard1.nodes.last().slaveIndex == 0

        when:
        clusterx.data = data4
        reply = clusterx.replicate()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test replicas'() {
        given:
        def cGroup = new CGroup('cluster', null, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        ConfForGlobal.clusterEnabled = true
        var multiShard = localPersist.multiShard
        var shards = multiShard.shards

        def shard0 = shards[0]
        shard0.multiSlotRange.addSingle(0, 16383)

        when:
        def data3 = new byte[3][]
        data3[1] = 'replicas'.bytes
        data3[2] = shard0.nodes[0].nodeId().bytes
        clusterx.data = data3
        def reply = clusterx.replicas()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        shard0.nodes << new Node(nodeIdFix: 'xxx', master: false, slaveIndex: 0, followNodeId: shard0.nodes[0].nodeId())
        reply = clusterx.replicas()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        new String(((BulkReply) (reply as MultiBulkReply).replies[0]).raw).startsWith shard0.nodes[1].nodeId()

        when:
        // two shards with different slot ranges - reply must contain ONLY matched shard's slot range
        def shard1 = new Shard()
        shard1.multiSlotRange.addSingle(8192, 16383)
        shard1.nodes << new Node(nodeIdFix: 'other_master', master: true)
        shards << shard1
        reply = clusterx.replicas()
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        def replicaLine = new String(((BulkReply) (reply as MultiBulkReply).replies[0]).raw)
        replicaLine.contains('0-16383')
        !replicaLine.contains('8192-16383')

        when:
        data3[2] = 'xxx'.bytes
        reply = clusterx.replicas()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message.contains 'master node id not found'

        when:
        def data1 = new byte[1][]
        clusterx.data = data1
        reply = clusterx.replicas()
        then:
        reply == ErrorReply.FORMAT

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
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        def shard0 = shards[0]
        shard0.multiSlotRange.addSingle(0, 16383)
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
        data6[4] = shard0.nodes[0].nodeId().bytes
        // cluster version
        data6[5] = '2'.bytes
        clusterx.data = data6
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // set slot not myself, ignore
        def shard1 = new Shard()
        shard1.nodes << new Node(nodeIdFix: 'xxx')
        shards << shard1
        data6[4] = shard1.nodes[0].nodeId().bytes
        def oldSlavePair = oneSlot.createReplPairAsSlave('localhost', 6380)
        def oldMasterPair = oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6381)
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        oldSlavePair.sendBye
        oldMasterPair.sendBye
        oneSlot.onlyOneReplPairAsSlave == null
        oneSlot.replPairAsMasterList.isEmpty()

        when:
        shard1.multiSlotRange.addSingle(0, 8191)
        shard0.multiSlotRange.list.clear()
        shard0.multiSlotRange.addSingle(8192, 16383)
        data6[4] = shard0.nodes[0].nodeId().bytes
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        shard0.multiSlotRange.contains(0)
        !shard1.multiSlotRange.contains(0)

        when:
        // no myself node, flush
        shard0.nodes[0].mySelf = false
        reply = clusterx.setslot()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK

        when:
        // not margin
        data6[2] = '1'.bytes
        reply = clusterx.setslot()
        then:
        reply == ClusterxCommand.OK

        when:
        data6[4] = 'xxx_not_exist_node_id'.bytes
        reply = clusterx.setslot()
        then:
        reply instanceof ErrorReply

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test setnodes initializes local assigned inner slots'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        def savedSlotNumber = ConfForGlobal.slotNumber
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = (short) 4

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def leaderSelector = LeaderSelector.instance
        def oldMocked = leaderSelector.masterAddressLocalMocked
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'

        def data4 = new byte[4][]
        data4[1] = 'setnodes'.bytes
        data4[2] = 'node-0 localhost 7380 master - 0-4095\nnode-1 localhost 7379 master - 4096-8191\n'.bytes
        data4[3] = '11'.bytes
        def cGroup = new CGroup('cluster', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        expect:
        !localPersist.isOneSlotInitialized((short) 1)

        when:
        def reply = clusterx.setnodes()

        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        localPersist.isOneSlotInitialized((short) 1)

        cleanup:
        leaderSelector.masterAddressLocalMocked = oldMocked
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        ConfForGlobal.slotNumber = savedSlotNumber
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot does not persist ownership when local slot init fails'() {
        given:
        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        ConfForGlobal.clusterEnabled = true
        def multiShard = localPersist.multiShard
        def shard0 = multiShard.shards[0]
        shard0.multiSlotRange.addSingle(0, 16383)

        and:
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = shard0.nodes[0].nodeId().bytes // self is the target node
        data6[5] = '5'.bytes
        clusterx.data = data6

        and: 'make local slot init fail by clearing the slots array (ensureOneSlotInitialized throws)'
        def versionBefore = multiShard.clusterCurrentEpoch
        localPersist.clearOneSlotsForTest()

        when:
        def thrown = null
        try {
            clusterx.setslot()
        } catch (RuntimeException e) {
            thrown = e
        }

        then: 'local slot init failed'
        thrown != null
        and: 'cluster metadata was NOT persisted (no ownership advertised for an unservable slot)'
        multiShard.clusterCurrentEpoch == versionBefore

        cleanup:
        ConfForGlobal.clusterEnabled = false
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot resets local target shard replica'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        and:
        def multiShard = localPersist.multiShard
        def targetShard = multiShard.shards[0]
        def localNode = targetShard.nodes[0]
        localNode.master = false
        localNode.slaveIndex = 1
        localNode.followNodeId = 'target-master'
        def targetMaster = new Node(nodeIdFix: 'target-master', host: 'localhost', port: 6380, master: true)
        targetShard.nodes.add(0, targetMaster)
        targetShard.multiSlotRange.addSingle(4096, 8191)

        and:
        def oldSlavePair = oneSlot.createReplPairAsSlave('localhost', 6380)
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = 'target-master'.bytes
        data6[5] = '7'.bytes
        clusterx.data = data6

        when:
        def reply = clusterx.setslot()

        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        oldSlavePair.sendBye
        oneSlot.onlyOneReplPairAsSlave != null
        !oneSlot.onlyOneReplPairAsSlave.is(oldSlavePair)
        oneSlot.onlyOneReplPairAsSlave.host == 'localhost'
        oneSlot.onlyOneReplPairAsSlave.port == 6380
        !oneSlot.canRead
        targetShard.multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot promotes local target node from slave to master'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        and: 'self node is the migration target; it was importing (a slave) and now owns the slot'
        def multiShard = localPersist.multiShard
        def targetShard = multiShard.shards[0]
        def localNode = targetShard.nodes[0]
        localNode.master = false
        localNode.slaveIndex = 1
        localNode.followNodeId = 'old-source-master'
        targetShard.multiSlotRange.addSingle(4096, 8191)

        and:
        def oldSlavePair = oneSlot.createReplPairAsSlave('localhost', 6380)
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = localNode.nodeId().bytes // self node is the target -> new owner
        data6[5] = '9'.bytes
        clusterx.data = data6

        when:
        def reply = clusterx.setslot()

        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        and: 'old slave repl pair is closed and NOT replaced — promoted to master, not re-slaved'
        oldSlavePair.sendBye
        oneSlot.onlyOneReplPairAsSlave == null
        and: 'slot is now writable/readable on the new owner'
        oneSlot.canRead
        !oneSlot.isReadonly()
        targetShard.multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot defers cutover until a configured replica is synced'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        and: 'self node is the migration target (importing, has an upstream) and the shard has a replica node'
        def targetShard = localPersist.multiShard.shards[0]
        def localNode = targetShard.nodes[0]
        localNode.master = false
        targetShard.multiSlotRange.addSingle(4096, 8191)
        targetShard.nodes << new Node(master: false, nodeIdFix: 'replica_node', host: 'localhost', port: 6390)
        oneSlot.createReplPairAsSlave('localhost', 6380) // the import upstream

        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = localNode.nodeId().bytes
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'a configured replica has not subscribed/synced yet — cutover defers (retryable) and does not mutate'
        def reply = clusterx.setslot()
        then:
        (reply as AsyncReply).settablePromise.getResult() instanceof ErrorReply
        !targetShard.multiSlotRange.contains(0)

        when: 'a caught-up downstream replica pair now exists'
        def downstream = oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6390)
        downstream.allCaughtUp = true
        reply = clusterx.setslot()
        then: 'cutover proceeds'
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        targetShard.multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot does not commit ownership when the local cutover fails'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = false // initialize the slot first

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance

        and: 'inject a slot whose local shed-cutover (flush) throws, running inline on this thread'
        def throwingSlot = new OneSlot(slot, Consts.slotDir, null, null) {
            @Override
            void flush() { throw new RuntimeException('boom flush') }
        }
        localPersist.oneSlots()[slot] = throwingSlot
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and: 'this node is not in the target shard, so setslot takes the shed (flush) branch'
        ConfForGlobal.clusterEnabled = true
        def shards = localPersist.multiShard.shards
        shards[0].nodes[0].master = true
        shards << new Shard(nodes: [new Node(master: true, nodeIdFix: 'to_node', host: 'localhost', port: 7379)])

        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = 'to_node'.bytes
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'the local cutover throws'
        def reply = clusterx.setslot()

        then: 'the command errors and the ownership/topology is NOT committed (routing never runs ahead of state)'
        (reply as AsyncReply).settablePromise.getResult() instanceof ErrorReply
        !shards[1].multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist.oneSlots()[slot] = null
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot does not commit stale ownership when a newer setslot advanced the epoch mid-cutover'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = false // initialize the slot first

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        def multiShard = localPersist.multiShard

        and: 'inject a slot whose local shed-cutover simulates a newer setslot committing epoch=20 mid-flush'
        def hookSlot = new OneSlot(slot, Consts.slotDir, null, null) {
            @Override
            void flush() { multiShard.updateClusterVersion(20) }
        }
        localPersist.oneSlots()[slot] = hookSlot
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and: 'this node is not in the target shard, so setslot takes the shed (flush) branch'
        ConfForGlobal.clusterEnabled = true
        def shards = multiShard.shards
        shards[0].nodes[0].master = true
        shards << new Shard(nodes: [new Node(master: true, nodeIdFix: 'to_node', host: 'localhost', port: 7379)])

        and: 'this setslot carries the older version 9'
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = 'to_node'.bytes
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'the older setslot reaches the deferred commit after the epoch was advanced to 20'
        def reply = clusterx.setslot()

        then: 'the stale ownership change is dropped (routing never rolls back) and the epoch stays at 20'
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        !shards[1].multiSlotRange.contains(0)
        multiShard.clusterCurrentEpoch == 20

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist.oneSlots()[slot] = null
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot does not change local role when a newer setslot advanced the epoch before cutover'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = false // initialize the slot first

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        def multiShard = localPersist.multiShard

        and: 'self node is the charge (import) target with an upstream; the durability gate reads hasUpstream, and'
        and: 'a newer setslot advances the epoch to 20 during that read, before the destructive cutover runs'
        boolean[] droppedUpstream = [false]
        def hookSlot = new OneSlot(slot, Consts.slotDir, null, null) {
            private boolean bumped = false
            @Override
            boolean hasUpstream() {
                if (!bumped) { bumped = true; multiShard.updateClusterVersion(20) }
                return true
            }
            @Override
            void dropUpstream() { droppedUpstream[0] = true }
        }
        localPersist.oneSlots()[slot] = hookSlot
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and: 'single-node self shard is the target, so this node is the charge node with no configured replica'
        ConfForGlobal.clusterEnabled = true
        def selfShard = multiShard.shards[0]
        def selfNode = selfShard.nodes[0]
        selfNode.master = false

        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = selfNode.nodeId().bytes // self node is the new owner (charge/promote path)
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'the older setslot reaches the cutover after the epoch was advanced to 20'
        def reply = clusterx.setslot()

        then: 'the stale request never promotes the local slot (no dropUpstream) and never commits ownership'
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        !droppedUpstream[0]
        !selfShard.multiSlotRange.contains(0)
        multiShard.clusterCurrentEpoch == 20

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist.oneSlots()[slot] = null
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot returns a clean error when the metadata commit fails to persist'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = false

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        def multiShard = localPersist.multiShard

        and: 'this node is not in the target shard and holds no local copy, so oneSlot is null (no cutover)'
        ConfForGlobal.clusterEnabled = true
        localPersist.oneSlots()[slot] = null
        def shards = multiShard.shards
        shards[0].nodes[0].master = true
        shards << new Shard(nodes: [new Node(master: true, nodeIdFix: 'to_node', host: 'localhost', port: 7379)])

        and: 'make saveMeta throw IOException by turning the metadata file into a directory'
        def metaFile = new File(Consts.persistDir, 'nodes.json')
        metaFile.delete()
        metaFile.mkdirs()

        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = 'to_node'.bytes
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'the deferred metadata commit fails to persist'
        def reply = clusterx.setslot()

        then: 'the failure surfaces as a clean error on the promise instead of escaping the callback'
        noExceptionThrown()
        (reply as AsyncReply).settablePromise.getResult() instanceof ErrorReply

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        metaFile.deleteDir()
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot stays in-memory consistent after a metadata persist failure and converges on retry'() {
        given: 'the destructive local cutover (shed/flush) runs, then the metadata persist fails'
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = false // initialize the slot first

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        def multiShard = localPersist.multiShard

        and: 'inject a slot on the shed path whose flush is recorded (slot 0 is release-exempt, so it survives)'
        boolean[] flushed = [false]
        def hookSlot = new OneSlot(slot, Consts.slotDir, null, null) {
            @Override
            void flush() { flushed[0] = true }
        }
        localPersist.oneSlots()[slot] = hookSlot
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and: 'this node is not in the target shard, so setslot takes the shed (flush) branch'
        ConfForGlobal.clusterEnabled = true
        def shards = multiShard.shards
        shards[0].nodes[0].master = true
        shards << new Shard(nodes: [new Node(master: true, nodeIdFix: 'to_node', host: 'localhost', port: 7379)])

        and: 'make the first saveMeta throw IOException by turning the metadata file into a directory'
        def metaFile = new File(Consts.persistDir, 'nodes.json')
        metaFile.delete()
        metaFile.mkdirs()

        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = 'to_node'.bytes
        data6[5] = '9'.bytes
        clusterx.data = data6

        when: 'the persist fails after the local cutover already shed the slot'
        def reply = clusterx.setslot()

        then: 'a clean error is surfaced'
        (reply as AsyncReply).settablePromise.getResult() instanceof ErrorReply
        and: 'the local cutover did run (irreversible), and the in-memory ranges match it: the slot moved to the'
        and: 'target shard, NOT rolled back to this node — so routing and local state stay mutually consistent'
        flushed[0]
        shards[1].multiSlotRange.contains(0)
        !shards[0].multiSlotRange.contains(0)

        when: 'the filesystem recovers and the controller retries the same setslot'
        metaFile.deleteDir()
        def replyRetry = clusterx.setslot()

        then: 'the retry converges idempotently to OK and the topology is unchanged'
        (replyRetry as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        shards[1].multiSlotRange.contains(0)
        !shards[0].multiSlotRange.contains(0)
        multiShard.clusterCurrentEpoch == 9

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        metaFile.deleteDir()
        localPersist.oneSlots()[slot] = null
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot closes repl pairs and flushes on old source node'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        and: 'self shard currently owns the slot; the slot migrates to a different shard'
        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        def mySelfShard = shards[0]
        mySelfShard.multiSlotRange.addSingle(0, 16383)
        shards << new Shard()
        shards[1].nodes << new Node(master: true, nodeIdFix: 'new_node_id', host: 'localhost', port: 7379)

        and: 'the old source has both a slave-side and a master-side repl pair open'
        def oldSlavePair = oneSlot.createReplPairAsSlave('localhost', 6380)
        def oldMasterPair = oneSlot.createIfNotExistReplPairAsMaster(123L, 'localhost', 7399)

        and:
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = shards[1].nodes[0].nodeId().bytes // target is in the other shard
        data6[5] = '11'.bytes
        clusterx.data = data6

        when:
        def reply = clusterx.setslot()

        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() == ClusterxCommand.OK
        and: 'both repl pairs are closed (before the flush) so no stray XFlush reaches the new owner'
        oldSlavePair.sendBye
        oldMasterPair.sendBye
        oneSlot.onlyOneReplPairAsSlave == null
        oneSlot.getReplPairAsMasterList().isEmpty()
        and: 'ownership moved from the old source shard to the target shard'
        shards[1].multiSlotRange.contains(0)
        !mySelfShard.multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setslot ignores stale cluster version'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        ConfForGlobal.clusterEnabled = true

        def data1 = new byte[1][]
        def cGroup = new CGroup('cluster', data1, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.doMockWhenCreateReplPairAsSlave = true

        and:
        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        def mySelfShard = shards[0]
        mySelfShard.multiSlotRange.addSingle(0, 16383)
        shards << new Shard()
        shards[1].nodes << new Node(master: true, nodeIdFix: 'new_node_id', host: 'localhost', port: 7379)
        multiShard.updateClusterVersion(10) // advance epoch to 10

        and: 'the old source has repl pairs open that a real cutover would tear down'
        def oldSlavePair = oneSlot.createReplPairAsSlave('localhost', 6380)
        def oldMasterPair = oneSlot.createIfNotExistReplPairAsMaster(123L, 'localhost', 7399)

        and: 'a stale setslot (version 5 < current epoch 10) targeting the other shard'
        def data6 = new byte[6][]
        data6[1] = 'setslot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'node'.bytes
        data6[4] = shards[1].nodes[0].nodeId().bytes
        data6[5] = '5'.bytes
        clusterx.data = data6

        when:
        def reply = clusterx.setslot()

        then: 'the stale version is ignored synchronously, with no teardown and no ownership change'
        reply == ClusterxCommand.OK
        !oldSlavePair.sendBye
        !oldMasterPair.sendBye
        multiShard.clusterCurrentEpoch == 10
        mySelfShard.multiSlotRange.contains(0)
        !shards[1].multiSlotRange.contains(0)

        cleanup:
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        localPersist?.cleanUp()
        localPersist?.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test setnodes ignores stale cluster version'() {
        given:
        def savedClusterEnabled = ConfForGlobal.clusterEnabled
        def savedSlotNumber = ConfForGlobal.slotNumber
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = (short) 4

        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())

        def leaderSelector = LeaderSelector.instance
        def oldMocked = leaderSelector.masterAddressLocalMocked
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'

        def multiShard = localPersist.multiShard
        multiShard.updateClusterVersion(20) // advance epoch to 20
        def shardsSizeBefore = multiShard.shards.size()

        def data4 = new byte[4][]
        data4[1] = 'setnodes'.bytes
        data4[2] = 'node-0 localhost 7380 master - 0-4095\nnode-1 localhost 7379 master - 4096-8191\n'.bytes
        data4[3] = '11'.bytes // stale: 11 < current epoch 20
        def cGroup = new CGroup('cluster', data4, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        when:
        def reply = clusterx.setnodes()

        then: 'the stale setnodes is ignored synchronously, topology and epoch unchanged'
        reply == ClusterxCommand.OK
        multiShard.clusterCurrentEpoch == 20
        multiShard.shards.size() == shardsSizeBefore

        cleanup:
        leaderSelector.masterAddressLocalMocked = oldMocked
        ConfForGlobal.clusterEnabled = savedClusterEnabled
        ConfForGlobal.slotNumber = savedSlotNumber
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    def 'test countkeysinslot returns 0 for uninitialized slot'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'countkeysinslot'.bytes
        data3[2] = '0'.bytes // client slot 0 -> inner slot 0 (not owned/initialized here)
        def cGroup = new CGroup('cluster', data3, null)
        cGroup.from(BaseCommand.mockAGroup())
        def clusterx = new ClusterxCommand(cGroup)

        and:
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = (short) 4
        ConfForGlobal.netListenAddress = 'localhost:7379'
        def localPersist = LocalPersist.instance
        localPersist.resetForTest()
        Consts.persistDir.mkdirs()

        and:
        def multiShard = new MultiShard(Consts.persistDir)
        multiShard.mySelfShard().multiSlotRange.addSingle(8192, 12287) // only inner slot 2 initialized
        multiShard.updateClusterVersion(1)

        and:
        def snowFlakes = [new SnowFlake(1, 1)] as SnowFlake[]
        localPersist.initSlots((byte) 1, (short) 4, snowFlakes, Consts.persistDir, Config.create())

        when: 'inner slot 0 is null (uninitialized) — must not NPE'
        def reply = clusterx.countkeysinslot()

        then: 'returns 0 (Redis "no keys here" semantics), restoring pre-lazy-init behavior'
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        cleanup:
        ConfForGlobal.clusterEnabled = false
        ConfForGlobal.slotNumber = (short) 1
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }
}
