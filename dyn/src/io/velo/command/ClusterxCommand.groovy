package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.repl.ReplPair
import io.velo.repl.cluster.MultiShard
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.cluster.SlotRange
import io.velo.reply.*

// act same as kvrocks clusterx commands
@CompileStatic
class ClusterxCommand extends BaseCommand {
    static final String version = '1.0.0'

    ClusterxCommand(CGroup cGroup) {
        super(cGroup.cmd, cGroup.data, cGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        if (data.length < 2) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1]).toLowerCase()

        if ("info" == subCmd) {
            return info()
        }

        if ('migrate' == subCmd) {
            return migrate()
        }

        // cluster nodes
        if ('nodes' == subCmd) {
            return nodes()
        }

        if ('setnodeid' == subCmd) {
            return setnodeid()
        }

        if ('setnodes' == subCmd) {
            return setnodes()
        }

        if ('setslot' == subCmd) {
            return setslot()
        }

        if ('slots' == subCmd) {
            return slots()
        }

        return ErrorReply.SYNTAX
    }

    private final BulkReply OK = new BulkReply('OK'.bytes)

    /*
cluster_state:fail
cluster_slots_assigned:0
cluster_slots_ok:0
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:1
cluster_size:0
cluster_current_epoch:0
cluster_my_epoch:0
cluster_stats_messages_sent:0
cluster_stats_messages_received:0
migrating_state:ok
     */
    private static final ErrorReply CLUSTER_DISABLED = new ErrorReply('This instance has cluster support disable')

    Reply info() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        TreeSet<Integer> slotSet = []
        TreeSet<String> hostSet = []
        shards.each { shard ->
            shard.multiSlotRange.list.each { slotRange ->
                for (i in slotRange.begin..slotRange.end) {
                    slotSet << i
                }
            }

            shard.nodes.each { node ->
                hostSet << node.host
            }
        }
        def isClusterStateOk = slotSet.size() == MultiShard.TO_CLIENT_SLOT_NUMBER

        def isMigrateFail = shards.any { shard ->
            shard.migratingSlot == -100
        }
        def isMigrateOk = shards.every { shard ->
            shard.migratingSlot == -1
        }

        Map<String, Object> r = [:]
        r.cluster_state = isClusterStateOk ? 'ok' : 'fail'
        r.migrating_state = isMigrateOk ? 'success' : (isMigrateFail ? 'fail' : 'doing')
        r.cluster_known_nodes = hostSet.size()
        r.cluster_current_epoch = multiShard.clusterCurrentEpoch
        r.cluster_my_epoch = multiShard.clusterMyEpoch

        def lines = r.collect { entry ->
            entry.key + ':' + entry.value
        }.join("\r\n") + "\r\n"

        new BulkReply(lines.bytes)
    }

    Reply migrate() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        // clusterx migrate slot toNodeId
        if (data.length != 4) {
            return ErrorReply.FORMAT
        }

        def slot = (short) Integer.parseInt(new String(data[2]))
        def toNodeId = new String(data[3])

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        def shard = shards.find { ss ->
            ss.nodes.find { nn ->
                nn.nodeId() == toNodeId
            } != null
        }

        if (shard) {
            shard.migratingSlot = slot

            // todo, slot repl ***
            // mock migrate done
//            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
//            firstOneSlot.delayRun(1000 * 10, () -> {
//                shard.migratingSlot = -1
//            })

            shard.migratingSlot = -1
            OK
        } else {
            return new ErrorReply('node id not found: ' + toNodeId)
        }
    }

    Reply nodes() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        List<String> list = []
        shards.each { shard ->
            list.addAll shard.clusterNodesSlotRangeList()
        }

        def lines = list.join("\r\n") + "\r\n"
        new BulkReply(lines.bytes)
    }

    Reply setnodeid() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        if (data.length != 3) {
            return ErrorReply.FORMAT
        }

        def nodeIdFix = new String(data[2])
        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        shards.each { shard ->
            def selfNode = shard.nodes.find { it.mySelf }
            if (selfNode) {
                selfNode.nodeIdFix = nodeIdFix
                log.warn 'Clusterx set node id: {} for self', nodeIdFix
            }
        }

        OK
    }

    Reply setnodes() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        if (data.length != 4) {
            return ErrorReply.FORMAT
        }

        // clusterx setnodes ** \n ** clusterVersion
        /*
${nodeId} ${ip} ${port} master -
${nodeId} ${ip} ${port} master - 0
${nodeId} ${ip} ${port} master - 0-8191 10000
${nodeId} ${ip} ${port} slave ${primaryNodeId}
         */
        def args = new String(data[2])
        def lines = args.readLines().collect { it.trim() }.findAll { it }

        def clusterVersion = new String(data[3]) as int

        var selfHostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)

        ArrayList<Shard> shards = []
        lines.findAll { it.contains('master') }.each {
            def arr = it.split(' ')
            def node = new Node()
            node.nodeIdFix = arr[0]
            node.host = arr[1]
            node.port = arr[2] as int
            node.master = true
            node.mySelf = node.host == selfHostAndPort.host() && node.port == selfHostAndPort.port()

            def shard = new Shard()
            shard.nodes << node

            def multiSlotRange = shard.multiSlotRange
            if (arr.length > 4) {
                for (i in 4..<arr.length) {
                    def tmp = arr[i]
                    if (tmp[0] == '-') {
                        continue
                    }

                    if (tmp.contains('-')) {
                        def subArr = tmp.split('-')
                        multiSlotRange.addSingle(subArr[0] as int, subArr[1] as int)
                        continue
                    }

                    multiSlotRange.addSingle(tmp as int, tmp as int)
                }
            }

            shards << shard
        }

        lines.findAll { !it.contains('master') }.each {
            def arr = it.split(' ')
            def node = new Node()
            node.nodeIdFix = arr[0]
            node.host = arr[1]
            node.port = arr[2] as int
            node.master = false
            node.mySelf = node.host == selfHostAndPort.host() && node.port == selfHostAndPort.port()
            def followNodeId = arr[4]
            node.followNodeId = followNodeId

            def shard = shards.find { ss ->
                ss.nodes.find { nn ->
                    nn.nodeId() == followNodeId
                } != null
            }
            shard.nodes << node
        }

        def multiShard = localPersist.multiShard
        multiShard.refreshAllShards(shards, clusterVersion)
        OK
    }

    Reply setslot() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        // clusterx setslot slot node nodeId clusterVersion
        if (data.length != 6) {
            return ErrorReply.FORMAT
        }

        def slot = (short) Integer.parseInt(new String(data[2]))
        def nodeId = new String(data[4])
        def clusterVersion = Integer.parseInt(new String(data[5]))

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        def shard = shards.find { ss ->
            ss.nodes.find { nn ->
                nn.nodeId() == nodeId
            }
        }

        if (shard) {
            shard.multiSlotRange.addOneSlot(slot)
            shard.migratingSlot = -1
            shards.each { ss ->
                if (ss != shard) {
                    ss.multiSlotRange.removeOneSlot(slot)
                    ss.migratingSlot = -1
                }
            }
            multiShard.updateClusterVersion(clusterVersion)
        } else {
            return new ErrorReply('node id not found: ' + nodeId)
        }

        OK
    }

    Reply slots() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        if (data.length != 2) {
            return ErrorReply.FORMAT
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        List<SlotRange> slotRangeList = []
        shards.each { shard ->
            shard.multiSlotRange.list.each {
                slotRangeList << it
            }
        }

        def replies = new Reply[slotRangeList.size()]
        for (i in 0..<slotRangeList.size()) {
            def slotRange = slotRangeList.get(i)

            def shardHasThisSlotRange = shards.find { shard ->
                shard.multiSlotRange.list.find { it == slotRange }
            }
            def subReplies = new Reply[2 + shardHasThisSlotRange.nodes.size()]

            subReplies[0] = new IntegerReply(slotRange.begin)
            subReplies[1] = new IntegerReply(slotRange.end)

            shardHasThisSlotRange.nodes.eachWithIndex { node, j ->
                def ss = new Reply[3]
                subReplies[2 + j] = new MultiBulkReply(ss)

                ss[0] = new BulkReply(node.host.bytes)
                ss[1] = new IntegerReply(node.port)
                ss[2] = new BulkReply(node.nodeId().bytes)
            }

            replies[i] = new MultiBulkReply(subReplies)
        }

        new MultiBulkReply(replies)
    }
}