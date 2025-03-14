package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.MultiWorkerServer
import io.velo.ValkeyRawConfSupport
import io.velo.repl.ReplPair
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.Reply
import org.apache.lucene.util.RamUsageEstimator
import oshi.SystemInfo

import java.lang.management.ManagementFactory

@CompileStatic
class InfoCommand extends BaseCommand {
    InfoCommand() {
        super(null, null, null)
    }

    InfoCommand(IGroup iGroup) {
        super(iGroup.cmd, iGroup.data, iGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list << SlotWithKeyHash.TO_FIX_FIRST_SLOT
        list
    }

    @Override
    Reply handle() {
        if (data.length != 1 && data.length != 2) {
            return ErrorReply.FORMAT
        }

        if (data.length == 2) {
            def section = new String(data[1])

            if ('keyspace' == section) {
                return keyspace()
            } else if ('memory' == section) {
                return memory()
            } else if ('replication' == section) {
                return replication()
            } else if ('server' == section) {
                return server()
            } else {
                return ErrorReply.SYNTAX
            }
        } else {
            def infoServer = server() as BulkReply
            def infoReplication = replication() as BulkReply

            def sb = new StringBuilder()
            sb << new String(infoServer.raw)
            sb << '\r\n'
            sb << new String(infoReplication.raw)

            return new BulkReply(sb.toString().bytes)
        }
    }

    private Reply keyspace() {
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
            return new BulkReply(content.bytes)
        })
    }

    private static List<Tuple2<String, Object>> slaveConnectState(ReplPair replPairAsSlave, int slaveIndex) {
        List<Tuple2<String, Object>> list = []

        def slaveFo = replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset
        def state = "ip=${replPairAsSlave.host},port=${replPairAsSlave.port}," +
                "state=${replPairAsSlave.isLinkUp() ? 'online' : 'offline'},offset=${slaveFo ? slaveFo.asReplOffset() : 0},lag=1"
        list << new Tuple2("slave${slaveIndex}", state)

        list
    }

    private static Reply memory() {
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

        String r = """# Memory
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
        new BulkReply(r.bytes)
    }

    private Reply replication() {
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

            list << new Tuple2('master_link_status', replPairAsSlave.isLinkUp() ? 'up' : 'down')

            def masterFo = replPairAsSlave.masterBinlogCurrentFileIndexAndOffset
            list << new Tuple2('master_repl_offset', masterFo ? masterFo.asReplOffset() : 0)

            def slaveFo = replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset
            list << new Tuple2('slave_read_repl_offset', slaveFo ? slaveFo.asReplOffset() : 0)
            list << new Tuple2('slave_repl_offset', slaveFo ? slaveFo.asReplOffset() : 0)

            list << new Tuple2('slave_read_only', firstOneSlot.isReadonly() ? 1 : 0)
            list << new Tuple2('slave_priority', ValkeyRawConfSupport.replicaPriority)

            // fix values, may be need change, todo
            list << new Tuple2('replica_announced', 1)
            list << new Tuple2('master_failover_state', 'no-failover')
            list << new Tuple2('repl_backlog_active', 1)
            list << new Tuple2('repl_backlog_size', 1048576)
            list << new Tuple2('repl_backlog_first_byte_offset', 1)
            list << new Tuple2('repl_backlog_histlen', slaveFo ? slaveFo.asReplOffset() : 0)
        } else {
            def hostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses)
            list << new Tuple2('master_host', hostAndPort.host())
            list << new Tuple2('master_port', hostAndPort.port())

            def replPairAsMasterList = firstOneSlot.replPairAsMasterList
            if (!replPairAsMasterList.isEmpty()) {
                def firstReplPair = replPairAsMasterList.getFirst()
                list.addAll slaveConnectState(firstReplPair, 0)
                list << new Tuple2('master_replid', firstReplPair.slaveUuid.toString().padLeft(40, '0'))
                list << new Tuple2('master_repl_offset', firstOneSlot.binlog.currentReplOffset())

                def slaveFo = firstReplPair.slaveLastCatchUpBinlogFileIndexAndOffset
                // for redis 6.x compat
                list << new Tuple2('master_link_status', firstReplPair.isLinkUp() ? 'up' : 'down')
                list << new Tuple2('slave_repl_offset', slaveFo ? slaveFo.asReplOffset() : 0)

                if (replPairAsMasterList.size() > 1) {
                    def secondReplPair = replPairAsMasterList.get(1)
                    def slaveFo2 = secondReplPair.slaveLastCatchUpBinlogFileIndexAndOffset

                    list.addAll slaveConnectState(secondReplPair, 1)
                    list << new Tuple2('master_replid2', secondReplPair.slaveUuid.toString().padLeft(40, '0'))
                    list << new Tuple2('second_repl_offset', slaveFo2 ? slaveFo2.asReplOffset() : 0)
                } else {
                    list << new Tuple2('master_replid2', '0' * 40)
                    list << new Tuple2('second_repl_offset', -1)
                }

                list << new Tuple2('repl_backlog_histlen', slaveFo ? slaveFo.asReplOffset() : 0)
            } else {
                list << new Tuple2('master_replid', '0' * 40)
                list << new Tuple2('master_replid2', '0' * 40)

                list << new Tuple2('master_repl_offset', -1)
                list << new Tuple2('second_repl_offset', -1)

                // for redis 6.x compat
                list << new Tuple2('master_link_status', 'down')
                list << new Tuple2('slave_repl_offset', '0')

                list << new Tuple2('repl_backlog_histlen', 0)
            }

            // fix values, may be need change, todo
            list << new Tuple2('master_failover_state', 'no-failover')
            list << new Tuple2('repl_backlog_active', 1)
            list << new Tuple2('repl_backlog_size', 1048576)
            list << new Tuple2('repl_backlog_first_byte_offset', 1)
        }

        def sb = new StringBuilder()
        sb << "# Replication\r\n"
        list.each { Tuple2<String, Object> tuple ->
            sb << tuple.v1 << ':' << tuple.v2 << '\r\n'
        }

        new BulkReply(sb.toString().bytes)
    }

    private Reply server() {
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

        new BulkReply(sb.toString().bytes)
    }
}
