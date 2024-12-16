package io.velo.command

import groovy.transform.CompileStatic
import io.activej.common.function.RunnableEx
import io.activej.common.function.SupplierEx
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.repl.LeaderSelector
import io.velo.repl.ReplPair
import io.velo.repl.cluster.MultiShard
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.cluster.SlotRange
import io.velo.reply.*
import org.jetbrains.annotations.VisibleForTesting
import redis.clients.jedis.util.JedisClusterCRC16

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
        list << SlotWithKeyHash.TO_FIX_FIRST_SLOT
        list
    }

    @Override
    Reply handle() {
        if (data.length < 2) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1]).toLowerCase()

        if ('addslots' == subCmd) {
            return addslots(false, false)
        }

        if ('addslotsrange' == subCmd) {
            return addslots(true, false)
        }

        if ('delslots' == subCmd) {
            return addslots(false, true)
        }

        if ('delslotsrange' == subCmd) {
            return addslots(true, true)
        }

        if ('info' == subCmd) {
            return info()
        }

        if ('keyslot' == subCmd) {
            return keyslot()
        }

        if ('migrate' == subCmd) {
            return migrate()
        }

        if ('myid' == subCmd) {
            return myid()
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
    Reply addslots(boolean isRange, boolean isDelete) {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        if (isRange) {
            if (data.length < 4 || data.length % 2 != 0) {
                return ErrorReply.FORMAT
            }
        } else {
            if (data.length < 3) {
                return ErrorReply.FORMAT
            }
        }

        TreeSet<Integer> toClientSlots = []
        if (isRange) {
            for (int i = 2; i < data.length - 1; i += 2) {
                def begin = Integer.parseInt(new String(data[i]))
                def end = Integer.parseInt(new String(data[i + 1]))
                for (slot in begin..end) {
                    toClientSlots << slot
                }
            }
        } else {
            for (i in 2..data.length - 1) {
                toClientSlots << Integer.parseInt(new String(data[i]))
            }
        }

        def multiShard = localPersist.multiShard

        def mySelfShard = multiShard.mySelfShard()
        def mySelfNode = mySelfShard.mySelfNode()
        if (!mySelfNode.master) {
            return new ErrorReply('only master can add slots')
        }

        if (isDelete) {
            for (slot in toClientSlots) {
                if (!mySelfShard.multiSlotRange.contains(slot)) {
                    return new ErrorReply("slot ${slot} is not in my range, node: ${mySelfNode.nodeId()}")
                }
            }

            TreeSet<Integer> add = []
            mySelfShard.multiSlotRange.removeOrAddSet(toClientSlots, add)

            log.warn 'Cluster delete slots success, slots: {}, node: {}', toClientSlots, mySelfNode.nodeId()
            multiShard.saveMeta()
            return OK
        }

        for (ss in multiShard.shards) {
            for (slot in toClientSlots) {
                if (ss.multiSlotRange.contains(slot)) {
                    return new ErrorReply("slot ${slot} is already busy, node: ${ss.master().nodeId()}")
                }
            }
        }

        for (toClientSlot in toClientSlots) {
            mySelfShard.multiSlotRange.addOneSlot(toClientSlot)
        }

        log.warn 'Cluster add slots success, slots: {}, node: {}', toClientSlots, mySelfNode.nodeId()
        multiShard.saveMeta()
        OK
    }

    @VisibleForTesting
    Reply info() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard
        def shards = multiShard.shards

        TreeSet<Integer> slotSet = []
        TreeSet<String> hostWithPortSet = []
        shards.each { ss ->
            ss.multiSlotRange.list.each { sr ->
                for (i in sr.begin..sr.end) {
                    slotSet << i
                }
            }

            ss.nodes.each { node ->
                hostWithPortSet << (node.host + ':' + node.port)
            }
        }
        def isAllToClientSlotSet = slotSet.size() == MultiShard.TO_CLIENT_SLOT_NUMBER

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
        r.cluster_known_nodes = hostWithPortSet.size()
        r.cluster_current_epoch = multiShard.clusterCurrentEpoch
        r.cluster_my_epoch = multiShard.clusterMyEpoch

        def mySelfShard = multiShard.mySelfShard()
        if (importMigratingSlotShard == mySelfShard) {
            r.migrating_state = 'migrating'
            r.import_migrating_slot = importMigratingSlotShard.importMigratingSlot
            r.export_migrating_slot = Shard.NO_MIGRATING_SLOT

            def lines = r.collect { entry ->
                entry.key + ':' + entry.value
            }.join("\r\n") + "\r\n"

            return new BulkReply(lines.bytes)
        }

        if (exportMigratingSlotShard != mySelfShard) {
            r.migrating_state = isMigrateOk ? 'success' : 'migrating'
            r.import_migrating_slot = importMigratingSlotShard ? importMigratingSlotShard.importMigratingSlot : Shard.NO_MIGRATING_SLOT
            r.export_migrating_slot = exportMigratingSlotShard ? exportMigratingSlotShard.exportMigratingSlot : Shard.NO_MIGRATING_SLOT

            def lines = r.collect { entry ->
                entry.key + ':' + entry.value
            }.join("\r\n") + "\r\n"

            return new BulkReply(lines.bytes)
        }

        // my self shard is doing export migrating slot
        // check if already done
        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

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
                log.debug 'Clusterx repl pair as master not found, wait slave connect to master, slave host={}, slave port={}',
                        exportMigratingSlotShard.migratingToHost, exportMigratingSlotShard.migratingToPort
                return 'migrating'
            }

            if (replPairAsMaster.allCaughtUp) {
                if (!oneSlot.readonly) {
                    oneSlot.readonly = true
                }
                return 'success'
            } else {
                return 'migrating'
            }
        }).whenComplete { state, e ->
            if (e) {
                finalPromise.set(new ErrorReply('error when check repl pair as slave: ' + e.message))
                return
            }

            r.migrating_state = state
            r.import_migrating_slot = Shard.NO_MIGRATING_SLOT
            r.export_migrating_slot = exportMigratingSlot

            def lines = r.collect { entry ->
                entry.key + ':' + entry.value
            }.join("\r\n") + "\r\n"

            finalPromise.set(new BulkReply(lines.bytes))
        }

        asyncReply
    }

    @VisibleForTesting
    Reply keyslot() {
        if (data.length != 3) {
            return ErrorReply.FORMAT
        }

        def keyBytes = data[2]
        var toClientSlot = JedisClusterCRC16.getSlot(keyBytes)
        new IntegerReply(toClientSlot)
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

        def mySelfShard = multiShard.mySelfShard()
        def mySelfNode = mySelfShard.mySelfNode()
        if (!mySelfNode.master) {
            return new ErrorReply('only master can migrate slot')
        }

        def shards = multiShard.shards
        def toShard = shards.find { ss ->
            ss.nodes.find { nn ->
                nn.nodeId() == toNodeId
            } != null
        }

        if (!toShard) {
            return new ErrorReply('node id not found: ' + toNodeId)
        }

        if (mySelfShard == toShard) {
            return new ErrorReply('self shard and target shard are the same')
        }

        def toShardMasterNode = toShard.master()
        if (!toShardMasterNode) {
            return new ErrorReply('to shard master node not found')
        }

        if (MultiShard.isToClientSlotSkip(toClientSlot)) {
            return OK
        }

        mySelfShard.migratingToHost = toShardMasterNode.host
        mySelfShard.migratingToPort = toShardMasterNode.port
        mySelfShard.exportMigratingSlot = toClientSlot
        log.warn 'Clusterx set my self shard export migrating slot {} to node id {}', toClientSlot, toNodeId

        OK
    }

    @VisibleForTesting
    Reply myid() {
        if (!ConfForGlobal.clusterEnabled) {
            return CLUSTER_DISABLED
        }

        def multiShard = localPersist.multiShard

        def mySelfShard = multiShard.mySelfShard()
        if (!mySelfShard) {
            def node = new Node()
            def hostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)
            node.host = hostAndPort.host()
            node.port = hostAndPort.port()
            return new BulkReply(node.nodeId().bytes)
        }

        def mySelfNode = mySelfShard.mySelfNode()
        new BulkReply(mySelfNode.nodeId().bytes)
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

        def mySelfShard = multiShard.mySelfShard()
        if (!mySelfShard) {
            def node = new Node()
            def hostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)
            node.host = hostAndPort.host()
            node.port = hostAndPort.port()
            node.master = true
            node.mySelf = true
            node.nodeIdFix = nodeIdFix
            multiShard.shards << new Shard(nodes: [node])
        } else {
            def mySelfNode = mySelfShard.mySelfNode()
            mySelfNode.nodeIdFix = nodeIdFix
        }

        log.warn 'Clusterx set node id={} for my self', nodeIdFix
        multiShard.saveMeta()

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
        log.info '--- setnodes ---'
        lines.each {
            log.info it
        }

        def clusterVersion = new String(data[3]) as int

        def mySelfHostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)

        ArrayList<Shard> shards = []
        lines.findAll { it.contains('master') }.each {
            def arr = it.split(' ')
            def node = new Node()
            node.nodeIdFix = arr[0]
            node.host = arr[1]
            node.port = arr[2] as int
            node.master = true
            node.mySelf = node.host == mySelfHostAndPort.host() && node.port == mySelfHostAndPort.port()

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
            node.mySelf = node.host == mySelfHostAndPort.host() && node.port == mySelfHostAndPort.port()
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

        boolean resetMySelfAsMaster = false
        boolean resetMySelfAsSlave = false

        def mySelfShard = multiShard.mySelfShard()
        if (!mySelfShard) {
            // delete my self node from cluster, reset as master
            resetMySelfAsMaster = true
        } else {
            def mySelfNode = mySelfShard.mySelfNode()
            if (mySelfNode.master) {
                resetMySelfAsMaster = true
            } else {
                resetMySelfAsSlave = true
            }
        }

        if (resetMySelfAsMaster) {
            SettablePromise<Reply> finalPromise = new SettablePromise<>()
            def asyncReply = new AsyncReply(finalPromise)

            def leaderSelector = LeaderSelector.instance
            leaderSelector.resetAsMaster(true, (e) -> {
                if (e != null) {
                    log.error('Reset as master failed', e)
                    finalPromise.set(new ErrorReply('error when reset as master: ' + e.message))
                } else {
                    log.debug('Reset as master success')
                    finalPromise.set(OK)
                }
            })
            return asyncReply
        }

        if (resetMySelfAsSlave) {
            def toMasterNode = mySelfShard.master()

            SettablePromise<Reply> finalPromise = new SettablePromise<>()
            def asyncReply = new AsyncReply(finalPromise)

            def leaderSelector = LeaderSelector.instance
            leaderSelector.resetAsSlave(toMasterNode.host, toMasterNode.port, (e) -> {
                if (e != null) {
                    log.error('Reset as slave failed', e)
                    finalPromise.set(new ErrorReply('error when reset as master: ' + e.message))
                } else {
                    log.debug('Reset as slave success')
                    finalPromise.set(OK)
                }
            })
            return asyncReply
        }

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

        if (MultiShard.isToClientSlotSkip(toClientSlot)) {
            // already set batch when margin
            return OK
        }

        // set batch
        def innerSlot = MultiShard.asInnerSlotByToClientSlot(toClientSlot)
        int toClientSlotEnd = toClientSlot + (MultiShard.TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber).intValue()
        TreeSet<Integer> set = []
        TreeSet<Integer> setEmpty = []
        for (int i = toClientSlot; i < toClientSlotEnd; i++) {
            set << i
        }

        // add
        toShard.multiSlotRange.removeOrAddSet(setEmpty, set)
        toShard.importMigratingSlot = Shard.NO_MIGRATING_SLOT
        toShard.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        shards.each { ss ->
            if (ss != toShard) {
                // remove
                ss.multiSlotRange.removeOrAddSet(set, setEmpty)
                ss.importMigratingSlot = Shard.NO_MIGRATING_SLOT
                ss.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
            }
        }
        multiShard.updateClusterVersion(clusterVersion)

        // check if to node id is my self node charge
        boolean isMySelfNodeChargeThisInnerSlot = false
        def mySelfShard = multiShard.mySelfShard()
        if (mySelfShard) {
            def mySelfNode = mySelfShard.mySelfNode()
            isMySelfNodeChargeThisInnerSlot = mySelfNode.nodeId() == nodeId
        }

        def oneSlot = localPersist.oneSlot(innerSlot)

        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        oneSlot.asyncRun(RunnableEx.of {
            if (isMySelfNodeChargeThisInnerSlot) {
                def isSelfSlave = oneSlot.removeReplPairAsSlave()
                if (isSelfSlave) {
                    oneSlot.resetAsMaster()
                } else {
                    log.info 'Clusterx set slot {} to node id {} is my self node charge, is already master, do nothing, slot={}',
                            toClientSlot, nodeId, innerSlot
                }
            } else {
                oneSlot.flush()
            }
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