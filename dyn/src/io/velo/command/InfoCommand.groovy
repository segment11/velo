package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.Reply

@CompileStatic
class InfoCommand extends BaseCommand {
    static final String version = '1.0.0'

    InfoCommand(IGroup iGroup) {
        super(iGroup.cmd, iGroup.data, iGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        if (data.length != 1 && data.length != 2) {
            return ErrorReply.FORMAT
        }

        log.info 'Dyn info command version: {}', version

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

        // todo

        def sb = new StringBuilder()
        list.each { Tuple2<String, Object> tuple ->
            sb << tuple.v1 << ':' << tuple.v2 << '\r\n'
        }

        new BulkReply(sb.toString().bytes)
    }
}
