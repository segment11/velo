package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.reply.BulkReply
import io.velo.reply.Reply

@CompileStatic
class ExtendCommand extends BaseCommand {
    ExtendCommand() {
        super(null, null, null)
    }

    ExtendCommand(EGroup eGroup) {
        super(eGroup.cmd, eGroup.data, eGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        // write your code here
        return new BulkReply("extend command handle result xxx".bytes)
    }
}