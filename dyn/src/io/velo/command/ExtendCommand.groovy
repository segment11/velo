package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.reply.BulkReply
import io.velo.reply.Reply

/**
 * Example extension command skeleton for user-defined commands.
 */
@CompileStatic
class ExtendCommand extends BaseCommand {
    /**
     * Creates an ExtendCommand with no bound group (used for dispatch).
     */
    ExtendCommand() {
        super(null, null, null)
    }

    /**
     * Creates an ExtendCommand from the given EGroup, copying its command data and socket.
     *
     * @param eGroup the group providing the command context
     */
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