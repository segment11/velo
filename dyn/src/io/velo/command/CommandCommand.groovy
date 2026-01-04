package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.reply.*

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
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('count' == subCmd) {
            // todo
            return new IntegerReply(241)
        } else if ('docs' == subCmd) {
            return docs()
        } else if ('getkeys' == subCmd) {
            return getkeys()
        } else if ('getkeysandflags' == subCmd) {
            return getkeys(true)
        } else if ('help' == subCmd) {
            // todo
            return new BulkReply('command help count docs getkeys'.getBytes())
        } else if ('info' == subCmd) {
            return info()
        } else if ('list' == subCmd) {
            return list()
        } else {
            return ErrorReply.SYNTAX
        }
    }

    private Reply info() {
        // todo
        MultiBulkReply.EMPTY
    }

    private Reply docs() {
        // todo
        MultiBulkReply.EMPTY
    }

    private Reply getkeys(boolean withFlags = false) {
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
            def keyBytes = sList[i].rawKey().bytes
            if (withFlags) {
                def subReplies = new Reply[2]
                subReplies[0] = new BulkReply(keyBytes)
                def flagsReplies = new Reply[2]
                // todo, add flags reply
                subReplies[1] = new MultiBulkReply(flagsReplies)
                replies[i] = new MultiBulkReply(subReplies)
            } else {
                replies[i] = new BulkReply(keyBytes)
            }
        }
        new MultiBulkReply(replies)
    }

    private Reply list() {
        // todo
        MultiBulkReply.EMPTY
    }
}
