package io.velo.command

import groovy.transform.CompileStatic
import io.activej.common.function.RunnableEx
import io.activej.common.function.SupplierEx
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.repl.ReplPair
import io.velo.repl.cluster.MultiShard
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.cluster.SlotRange
import io.velo.reply.*
import org.jetbrains.annotations.VisibleForTesting

// act same as kvrocks clusterx commands
@CompileStatic
class ClusterxCommand extends BaseCommand {

    ClusterxCommand() {
        super(null, null, null)
    }

    ClusterxCommand(CGroup cGroup) {
        super(cGroup.cmd, cGroup.data, cGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        // fix the first net worker event loop thread
        list << new SlotWithKeyHash((short) 0, 0, 0L)
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

    @VisibleForTesting
    static final BulkReply OK = new BulkReply('OK'.bytes)

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
    @VisibleForTesting
    static final ErrorReply CLUSTER_DISABLED = new ErrorReply('This instance has cluster support disable')

    @VisibleForTesting
    Reply info() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        TreeSet<Integer> slotSet = []
        TreeSet<String> hostSet = []
        shards.each { ss ->
            ss.multiSlotRange.list.each { sr ->
                for (i in sr.begin..sr.end) {
                    slotSet << i
                }
            }

            ss.nodes.each { node ->
                hostSet << node.host
            }
        }
        def isAllToClientSlotSet = slotSet.size() == MultiShard.TO_CLIENT_SLOT_NUMBER

        def isMigrateFail = shards.any { ss ->
            ss.importMigratingSlot == Shard.FAIL_MIGRATED_SLOT || ss.exportMigratingSlot == Shard.FAIL_MIGRATED_SLOT
        }
        def isMigrateOk = shards.every { ss ->
            ss.importMigratingSlot == Shard.NO_MIGRATING_SLOT && ss.exportMigratingSlot == Shard.NO_MIGRATING_SLOT
        }
        def importMigratingSlotShard = shards.find { ss ->
            ss.importMigratingSlot >= 0
        }
        def exportMigratingSlotShard = shards.find { ss ->
            ss.exportMigratingSlot >= 0
        }

        Map<String, Object> r = [:]
        r.cluster_state = isAllToClientSlotSet ? 'ok' : 'fail'
        r.cluster_known_nodes = hostSet.size()
        r.cluster_current_epoch = multiShard.clusterCurrentEpoch
        r.cluster_my_epoch = multiShard.clusterMyEpoch

        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        // check if already done
        if (exportMigratingSlotShard) {
            // check if repl pair as master, slave is catch up
            def exportMigratingSlot = exportMigratingSlotShard.exportMigratingSlot
            def innerSlot = MultiShard.asInnerSlotByToClientSlot(exportMigratingSlot)
            def oneSlot = localPersist.oneSlot(innerSlot)

            oneSlot.asyncCall(SupplierEx.of {
                def replPairAsMasterList = oneSlot.replPairAsMasterList
                def replPairAsMaster = replPairAsMasterList.find {
                    it.host == exportMigratingSlotShard.migratingToHost && it.port == exportMigratingSlotShard.migratingToPort
                }
                if (!replPairAsMaster) {
                    log.warn 'Clusterx repl pair as master not found, wait slave connect to master, slave host: {}, slave port: {}',
                            exportMigratingSlotShard.migratingToHost, exportMigratingSlotShard.migratingToPort
                    return false
                }

                if (replPairAsMaster.allCaughtUp) {
                    oneSlot.readonly = true
                    return true
                } else {
                    return false
                }
            }).whenComplete { isMigratingDone, e ->
                if (e) {
                    finalPromise.set(new ErrorReply('error when check repl pair as slave: ' + e.message))
                    return
                }

                r.migrating_state = isMigratingDone ? 'success' : 'migrating'
                r.import_migrating_slot = Shard.NO_MIGRATING_SLOT
                r.export_migrating_slot = exportMigratingSlot

                def lines = r.collect { entry ->
                    entry.key + ':' + entry.value
                }.join("\r\n") + "\r\n"

                finalPromise.set(new BulkReply(lines.bytes))
            }
        } else {
            r.migrating_state = isMigrateOk ? 'success' : (isMigrateFail ? 'fail' : 'migrating')
            r.import_migrating_slot = importMigratingSlotShard ? importMigratingSlotShard.importMigratingSlot : Shard.NO_MIGRATING_SLOT
            r.export_migrating_slot = exportMigratingSlotShard ? exportMigratingSlotShard.exportMigratingSlot : Shard.NO_MIGRATING_SLOT

            def lines = r.collect { entry ->
                entry.key + ':' + entry.value
            }.join("\r\n") + "\r\n"

            finalPromise.set(new BulkReply(lines.bytes))
        }

        asyncReply
    }

    // refer to segment_kvrocks_controller
    // after call 'clusterx migrate 0 toNodeId', need call 'manage slot 0 migrate_from host port force'
    // refer ManageCommand
    @VisibleForTesting
    Reply migrate() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        // clusterx migrate slot toNodeId
        if (data.length != 4) {
            return ErrorReply.FORMAT
        }

        def toClientSlot = (short) Integer.parseInt(new String(data[2]))
        def toNodeId = new String(data[3])

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        def selfShard = shards.find { ss -> ss.mySelf() != null }
        def selfNode = selfShard.mySelf()
        if (!selfNode.master) {
            return new ErrorReply('only master can migrate slot')
        }

        def toShard = shards.find { ss ->
            ss.nodes.find { nn ->
                nn.nodeId() == toNodeId
            } != null
        }

        if (!toShard) {
            return new ErrorReply('node id not found: ' + toNodeId)
        }

        if (selfShard == toShard) {
            return new ErrorReply('self shard and target shard are the same')
        }

        def toShardMasterNode = toShard.master()
        if (!toShardMasterNode) {
            return new ErrorReply('to shard master node not found')
        }

        if (MultiShard.isToClientSlotSkip(toClientSlot)) {
            return OK
        }

        selfShard.migratingToHost = toShardMasterNode.host
        selfShard.migratingToPort = toShardMasterNode.port
        selfShard.exportMigratingSlot = toClientSlot
        log.warn 'Clusterx set self shard export migrating slot {} to node id {}', toClientSlot, toNodeId


        toShard.importMigratingSlot = toClientSlot
        log.warn 'Clusterx set target shard import migrating slot {} to node id {}', toClientSlot, toNodeId

        OK
    }

    @VisibleForTesting
    Reply nodes() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        List<String> list = []
        shards.each { ss ->
            list.addAll ss.clusterNodesSlotRangeList()
        }

        def lines = list.join("\r\n") + "\r\n"
        new BulkReply(lines.bytes)
    }

    @VisibleForTesting
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
        shards.each { ss ->
            def selfNode = ss.nodes.find { nn -> nn.mySelf }
            if (selfNode) {
                selfNode.nodeIdFix = nodeIdFix
                log.warn 'Clusterx set node id: {} for self', nodeIdFix
            }
        }

        OK
    }

    @VisibleForTesting
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

    @VisibleForTesting
    Reply setslot() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        // clusterx setslot slot node nodeId clusterVersion
        if (data.length != 6) {
            return ErrorReply.FORMAT
        }

        def toClientSlot = (short) Integer.parseInt(new String(data[2]))
        def nodeId = new String(data[4])
        def clusterVersion = Integer.parseInt(new String(data[5]))

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards
        def toShard = shards.find { ss ->
            ss.nodes.find { nn ->
                nn.nodeId() == nodeId
            }
        }

        if (!toShard) {
            return new ErrorReply('node id not found: ' + nodeId)
        }

        toShard.multiSlotRange.addOneSlot(toClientSlot)
        toShard.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        shards.each { ss ->
            if (ss != toShard) {
                ss.multiSlotRange.removeOneSlot(toClientSlot)
                ss.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
            }
        }
        multiShard.updateClusterVersion(clusterVersion)

        def innerSlot = MultiShard.asInnerSlotByToClientSlot(toClientSlot)
        def oneSlot = localPersist.oneSlot(innerSlot)

        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        oneSlot.asyncRun(RunnableEx.of {
            oneSlot.canRead = true
            oneSlot.readonly = false
        }).whenComplete { done, e ->
            if (e) {
                finalPromise.set(new ErrorReply('error when set slot: ' + e.message))
                return
            }
            finalPromise.set(OK)
        }

        asyncReply
    }

    @VisibleForTesting
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
        shards.each { ss ->
            ss.multiSlotRange.list.each { sr ->
                slotRangeList << sr
            }
        }

        def replies = new Reply[slotRangeList.size()]
        for (i in 0..<slotRangeList.size()) {
            def slotRange = slotRangeList.get(i)

            def shardHasThisSlotRange = shards.find { ss ->
                ss.multiSlotRange.list.find { sr -> sr == slotRange }
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