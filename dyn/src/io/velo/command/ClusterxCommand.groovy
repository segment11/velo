package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.Reply

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
        if (data.length < 3) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('setnodeid' == subCmd) {
            return setnodeid()
        }

        if ('setnodes' == subCmd) {
            return setnodes()
        }

        if ('setslot' == subCmd) {
            return setslot()
        }

        if ('migrate' == subCmd) {
            return migrate()
        }

        return null
    }

    private final BulkReply OK = new BulkReply('OK'.bytes)

    Reply setnodeid() {
        OK
    }

    Reply setnodes() {
        OK
    }

    Reply setslot() {
        OK
    }

    Reply migrate() {
        OK
    }
}