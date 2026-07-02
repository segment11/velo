package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.MultiWorkerServer
import io.velo.monitor.RuntimeCpuCollector
import io.velo.repl.ReplPair
import io.velo.reply.AsyncReply
import io.velo.reply.BulkReply
import io.velo.reply.Reply
import org.apache.lucene.util.RamUsageEstimator
import oshi.SystemInfo

import java.lang.management.ManagementFactory

/**
 * Implements the INFO command, returning server statistics and runtime information sections.
 */
@CompileStatic
class InfoCommand extends BaseCommand {
    /**
     * Creates an InfoCommand with no bound group (used for dispatch).
     */
    InfoCommand() {
        super(null, null, null)
    }

    /**
     * Creates an InfoCommand from the given IGroup, copying its command data and socket.
     *
     * @param iGroup the group providing the command context
     */
    InfoCommand(IGroup iGroup) {
        super(iGroup.cmd, iGroup.data, iGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list << SlotWithKeyHash.TO_FIX_FIRST_SLOT
        list
    }

    // exclude keyspace (async reply)
    private static final String[] sections = ['server', 'clients', 'memory', 'replication', 'cpu', 'cluster']

    private class StringOrReply {
        String s
        AsyncReply reply

        StringOrReply(String s) {
            this.s = s
        }

        StringOrReply(AsyncReply reply) {
            this.reply = reply
        }
    }

    private StringOrReply getOneSection(String section) {
        if ('server' == section) {
            return new StringOrReply(server())
        } else if ('clients' == section) {
            return new StringOrReply(clients())
        } else if ('memory' == section) {
            return new StringOrReply(memory())
        } else if ('replication' == section) {
            return new StringOrReply(replication())
        } else if ('cpu' == section) {
            return new StringOrReply(cpu())
        } else if ('cluster' == section) {
            return new StringOrReply(cluster())
        } else if ('keyspace' == section) {
            return new StringOrReply(keyspace())
        } else {
            // Unknown or empty section. Redis returns the default content (all standard sections)
            // rather than erroring or emitting a placeholder. Iterating the known `sections` array
            // (which never reaches this branch) also avoids the StringIndexOutOfBoundsException that
            // `section[0]` / `section.substring(1)` threw for an empty section argument.
            def sb = new StringBuilder()
            for (knownSection in sections) {
                sb << getOneSection(knownSection).s << '\r\n'
            }
            return new StringOrReply(sb.toString())
        }
    }

    @Override
    Reply handle() {
        if (data.length == 2) {
            def section = new String(data[1])
            def stringOrReply = getOneSection(section)
            if (stringOrReply.s != null) {
                return new BulkReply(stringOrReply.s.bytes)
            } else {
                return stringOrReply.reply
            }
        } else if (data.length == 1) {
            List<StringOrReply> infoList = []
            for (section in sections) {
                infoList << getOneSection(section)
            }

            def prefixContent = infoList.collect { it.s }.join('\r\n')
            return keyspace(prefixContent)
        } else {
            boolean hasKeyspace = false
            List<StringOrReply> infoList = []
            for (int i = 1; i < data.length; i++) {
                def bytes = data[i]
                // for unit test
                if (bytes == null) {
                    continue
                }

                def section = new String(bytes)
                if ('keyspace' == section) {
                    hasKeyspace = true
                    continue
                }
                infoList << getOneSection(section)
            }

            def prefixContent = infoList.collect { it.s }.join('\r\n')
            if (hasKeyspace) {
                return keyspace(prefixContent)
            } else {
                return new BulkReply(prefixContent.bytes)
            }
        }
    }

    private String server() {
        // a copy one
        def list = MultiWorkerServer.STATIC_GLOBAL_V.infoServerList

        def upSeconds = ((System.currentTimeMillis() - MultiWorkerServer.UP_TIME) / 1000).intValue()
        def upDays = (upSeconds / 3600 / 24).intValue()
        list << new Tuple2<>('uptime_in_seconds', upSeconds.toString())
        list << new Tuple2<>('uptime_in_days', upDays.toString())

        def firstOneSlot = localPersist.firstOneSlot()
        list << new Tuple2<>('run_id', firstOneSlot.masterUuid.toString().padLeft(40, '0'))

        def sb = new StringBuilder()
        sb << "# Server\r\n"
        list.each { Tuple2<String, String> tuple ->
            sb << tuple.v1 << ':' << tuple.v2 << '\r\n'
        }

        sb.toString()
    }

    private static String clients() {
        def socketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector

        def r = """# Clients
connected_clients:${socketInspector.connectedClientCount()}
cluster_connections:0
maxclients:${socketInspector.maxConnections}
blocked_clients:${BlockingList.blockingClientCount()}
total_blocking_keys:${BlockingList.blockingKeyCount()}
"""
        r.toString()
    }

    private static List<Tuple2<String, Object>> slaveConnectState(ReplPair replPairAsSlave, int slaveIndex) {
        List<Tuple2<String, Object>> list = []

        def state = "ip=${replPairAsSlave.host},port=${replPairAsSlave.port}," +
                "state=${replPairAsSlave.isLinkUpAnyOk() ? 'online' : 'offline'},offset=${replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset},lag=1"
        list << new Tuple2("slave${slaveIndex}", state)

        list
    }

    private static String memory() {
        def memoryMXBean = ManagementFactory.getMemoryMXBean()
        def heapMemoryUsage = memoryMXBean.heapMemoryUsage
        def nonHeapMemoryUsage = memoryMXBean.nonHeapMemoryUsage

        def totalUsed = heapMemoryUsage.used + nonHeapMemoryUsage.used
        def totalUsedHumanReadable = RamUsageEstimator.humanReadableUnits(totalUsed).replace(' ', '')

        // nonHeapMemoryUsage.max may == -1
        def totalMax = heapMemoryUsage.max + nonHeapMemoryUsage.max
        def totalMaxHumanReadable = RamUsageEstimator.humanReadableUnits(totalMax).replace(' ', '')

        def usedPercent = (totalUsed / totalMax) * 100

        def si = new SystemInfo()
        def globalMemory = si.hardware.memory
        long totalPhysicalMemory = globalMemory.total
        def totalPhysicalMemoryHumanReadable = RamUsageEstimator.humanReadableUnits(totalPhysicalMemory).replace(' ', '')

        def r = """# Memory
used_memory:${totalUsed}
used_memory_human:${totalUsedHumanReadable}
used_memory_rss:${totalUsed}
used_memory_rss_human:${totalUsedHumanReadable}
used_memory_peak:${totalMax}
used_memory_peak_human:${totalMaxHumanReadable}
used_memory_peak_perc:${usedPercent.round(2)}%
total_system_memory:${totalPhysicalMemory}
total_system_memory_human:${totalPhysicalMemoryHumanReadable}
maxmemory:${totalMax}
maxmemory_human:${totalMaxHumanReadable}
"""
        r.toString()
    }

    private String replication() {
        def firstOneSlot = localPersist.currentThreadFirstOneSlot()

        LinkedList<Tuple2<String, Object>> list = []

        def isSelfSlave = firstOneSlot.isAsSlave()
        list << new Tuple2('role', isSelfSlave ? 'slave' : 'master')

        if (isSelfSlave) {
            list << new Tuple2('connected_slaves', 0)
        } else {
            def slaveReplPairList = firstOneSlot.slaveReplPairListSelfAsMaster
            list << new Tuple2('connected_slaves', slaveReplPairList.size())
        }

        if (isSelfSlave) {
            def replPairAsSlave = firstOneSlot.onlyOneReplPairAsSlave
            list << new Tuple2('master_host', replPairAsSlave.host)
            list << new Tuple2('master_port', replPairAsSlave.port)

            list << new Tuple2('master_replid', replPairAsSlave.slaveUuid.toString().padLeft(40, '0'))
            list << new Tuple2('master_replid2', '0' * 40)

            // Real link state from the actual replication TCP client.
            // Previously hard-coded "connected" — which masked Sentinel failover signals.
            def tcpClientUp = replPairAsSlave.isLinkUpAnyOk()
            list << new Tuple2('master_link_status', tcpClientUp ? 'up' : 'down')

            def masterFo = replPairAsSlave.masterBinlogCurrentFileIndexAndOffset
            list << new Tuple2('master_repl_offset', masterFo ? masterFo.asReplOffset() : 0)

            list << new Tuple2('slave_read_repl_offset', replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset)
            list << new Tuple2('slave_repl_offset', replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset)

            list << new Tuple2('slave_read_only', firstOneSlot.isReadonly() ? 1 : 0)
            list << new Tuple2('slave_priority', ConfForGlobal.sentinelReplicaPriority)
            list << new Tuple2('replica_priority', ConfForGlobal.sentinelReplicaPriority)

            // Sentinel-configured announce host/port, with fall-back to local listen address.
            def announcedHp = ConfForGlobal.announcedHostPort()
            list << new Tuple2('replica_announced', 1)
            list << new Tuple2('replica-announce-ip', announcedHp.host ?: '')
            list << new Tuple2('replica-announce-port', String.valueOf(announcedHp.port))

            list << new Tuple2('master_failover_state', MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState)
            list << new Tuple2('repl_backlog_active', 1)
            list << new Tuple2('repl_backlog_size', 1048576)
            list << new Tuple2('repl_backlog_first_byte_offset', 1)
            list << new Tuple2('repl_backlog_histlen', replPairAsSlave.slaveLastCatchUpBinlogAsReplOffset)
        } else {
            // Master mode: prefer replicaAnnounceIp/Port so Sentinel sees a reachable address.
            def announcedHp = ConfForGlobal.announcedHostPort()
            list << new Tuple2('master_host', announcedHp.host ?: '')
            list << new Tuple2('master_port', String.valueOf(announcedHp.port))

            def replPairAsMasterList = firstOneSlot.replPairAsMasterList
            if (!replPairAsMasterList.isEmpty()) {
                // Real Redis emits all slaveN: lines consecutively immediately after connected_slaves
                // and before master_replid; keep the same order so Redis Sentinel's line-based parser
                // sees every replica in the expected position. One line per replica, so that
                // connected_slaves and the emitted slaveN lines always agree (no hard cap).
                for (int i = 0; i < replPairAsMasterList.size(); i++) {
                    list.addAll slaveConnectState(replPairAsMasterList.get(i), i)
                }

                def firstReplPair = replPairAsMasterList.getFirst()
                list << new Tuple2('master_replid', firstReplPair.slaveUuid.toString().padLeft(40, '0'))
                list << new Tuple2('master_repl_offset', firstOneSlot.binlog.currentReplOffset())

                // for redis 6.x compat
                list << new Tuple2('master_link_status', firstReplPair.isLinkUpAnyOk() ? 'up' : 'down')
                list << new Tuple2('slave_repl_offset', firstReplPair.slaveLastCatchUpBinlogAsReplOffset)

                if (replPairAsMasterList.size() > 1) {
                    def secondReplPair = replPairAsMasterList.get(1)

                    list << new Tuple2('master_replid2', secondReplPair.slaveUuid.toString().padLeft(40, '0'))
                    list << new Tuple2('second_repl_offset', secondReplPair.slaveLastCatchUpBinlogAsReplOffset)
                } else {
                    list << new Tuple2('master_replid2', '0' * 40)
                    list << new Tuple2('second_repl_offset', -1)
                }

                list << new Tuple2('repl_backlog_histlen', firstReplPair.slaveLastCatchUpBinlogAsReplOffset)
            } else {
                list << new Tuple2('master_replid', '0' * 40)
                list << new Tuple2('master_replid2', '0' * 40)

                // Healthy master with no replicas still produces binlog; report real offset.
                // Previous code emitted -1 here, which Sentinel treats as a broken master.
                list << new Tuple2('master_repl_offset', firstOneSlot.binlog.currentReplOffset())
                list << new Tuple2('second_repl_offset', -1)

                // for redis 6.x compat — no slaves so no link to report.
                list << new Tuple2('master_link_status', 'down')
                list << new Tuple2('slave_repl_offset', '0')

                list << new Tuple2('repl_backlog_histlen', 0)
            }

            list << new Tuple2('master_failover_state', MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState)
            list << new Tuple2('repl_backlog_active', 1)
            list << new Tuple2('repl_backlog_size', 1048576)
            list << new Tuple2('repl_backlog_first_byte_offset', 1)
        }

        def sb = new StringBuilder()
        sb << "# Replication\r\n"
        list.each { Tuple2<String, Object> tuple ->
            sb << tuple.v1 << ':' << tuple.v2 << '\r\n'
        }

        sb.toString()
    }

    private static String cpu() {
        def process = RuntimeCpuCollector.collect()

        def r = """# CPU
used_cpu_sys:${(process.getKernelTime() / 1000).round(6)}
used_cpu_user:${(process.getUserTime() / 1000).round(6)}
"""
        r.toString()
    }

    private static String cluster() {
        def r = """# Cluster
cluster_enabled:${ConfForGlobal.clusterEnabled ? 1 : 0}
"""
        return r.toString()
    }

    private AsyncReply keyspace(String prefixContent = null) {
        localPersist.doSthInSlots(oneSlot -> {
            def n1 = oneSlot.getAllKeyCount()
            def n2 = oneSlot.getAvgTtlInSecond().longValue()
            return new Tuple2<Long, Long>(n1, n2)
        }, resultList -> {
            long keysTotal = 0
            long avgTtlTotal = 0
            for (one in resultList) {
                Tuple2<Long, Long> tuple2 = one as Tuple2<Long, Long>
                keysTotal += tuple2.v1
                avgTtlTotal += tuple2.v2
            }
            def avgTtlFinal = (avgTtlTotal / resultList.size()).longValue()

            def content = """# Keyspace
db0:keys=${keysTotal},expires=0,avg_ttl=${avgTtlFinal}
"""
            if (prefixContent) {
                return new BulkReply((prefixContent + "\r\n" + content.toString()).bytes)
            } else {
                return new BulkReply(content.toString().bytes)
            }
        })
    }
}
