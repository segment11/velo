package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.reply.*
import org.jetbrains.annotations.VisibleForTesting

@CompileStatic
class CommandCommand extends BaseCommand {
    static final String version = '1.0.0'

    CommandCommand() {
        super(null, null, null)
    }

    CommandCommand(CGroup cGroup) {
        super(cGroup.cmd, cGroup.data, cGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        log.info 'Dyn command command version={}', version

        if (data.length < 2) {
            return ErrorReply.FORMAT
//            return new ErrorReply('wrong number of arguments for \'command\' command')
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('count' == subCmd) {
            // todo
            return new IntegerReply(200);
        } else if ('docs' == subCmd) {
            return docs()
        } else if ('getkeys' == subCmd) {
            return getkeys()
        } else {
            return ErrorReply.SYNTAX
        }
    }

    @VisibleForTesting
    Reply docs() {
        // todo
        MultiBulkReply.EMPTY
    }

    @VisibleForTesting
    Reply getkeys() {
        if (data.length < 4) {
            return ErrorReply.FORMAT
        }

        def targetCmd = new String(data[2]).toLowerCase()
        def firstByte = targetCmd.bytes[0]
        // a - z
        if (firstByte < 97 || firstByte > 122) {
            return new ErrorReply('command name must start with a-z')
        }

        def dd = new byte[data.length - 2][]
        for (int i = 2; i < data.length; i++) {
            dd[i - 2] = data[i]
        }

        def a_zGroup = requestHandler.getA_ZGroupCommand(firstByte)
        def sList = a_zGroup.parseSlots(targetCmd, dd, ConfForGlobal.slotNumber)

        if (!sList) {
            return MultiBulkReply.EMPTY
        }

        def replies = new Reply[sList.size()]
        for (int i = 0; i < sList.size(); i++) {
            replies[i] = new BulkReply(sList[i].rawKey().bytes)
        }
        new MultiBulkReply(replies)
    }
}
