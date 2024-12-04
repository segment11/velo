package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.Reply

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

            if ('replication' == section) {
                return replication()
            } else {
                return new BulkReply(''.bytes)
            }
        } else {
            // todo
            return new BulkReply(''.bytes)
        }
    }

    Reply replication() {
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
            list << new Tuple2('master_link_status', replPairAsSlave.isLinkUp() ? 'up' : 'down')
            def masterFo = replPairAsSlave.masterBinlogCurrentFileIndexAndOffset
            list << new Tuple2('master_repl_offset', masterFo ? masterFo.asReplOffset() : 0)
            def slaveFo = replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset
            list << new Tuple2('slave_repl_offset', slaveFo ? slaveFo.asReplOffset() : 0)
        } else {
            def replPairAsMasterList = firstOneSlot.replPairAsMasterList
            if (!replPairAsMasterList.isEmpty()) {
                def firstReplPair = replPairAsMasterList.getFirst()
                list << new Tuple2('master_link_status', firstReplPair.isLinkUp() ? 'up' : 'down')
                list << new Tuple2('master_repl_offset', firstOneSlot.binlog.currentReplOffset())
                def slaveFo = firstReplPair.slaveLastCatchUpBinlogFileIndexAndOffset
                list << new Tuple2('slave_repl_offset', slaveFo ? slaveFo.asReplOffset() : 0)
            } else {
                list << new Tuple2('master_link_status', 'down')
                list << new Tuple2('master_repl_offset', '0')
                list << new Tuple2('slave_repl_offset', '0')
            }
        }

        def sb = new StringBuilder()
        list.each { Tuple2<String, Object> tuple ->
            sb << tuple.v1 << ':' << tuple.v2 << '\r\n'
        }

        new BulkReply(sb.toString().bytes)
    }
}
