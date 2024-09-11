package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.Reply

@CompileStatic
class SentinelCommand extends BaseCommand {
    static final String version = '1.0.0'

    SentinelCommand(SGroup sGroup) {
        super(sGroup.cmd, sGroup.data, sGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        log.info 'Dyn sentinel command version={}', version

        if (data.length < 2) {
            return new ErrorReply('wrong number of arguments for \'sentinel\' command')
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('myid' == subCmd) {
            // sentinel myid
            if (data.length != 2) {
                return ErrorReply.SYNTAX
            }

            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
            return new BulkReply(firstOneSlot.masterUuid.toString().bytes)
        }

        if ('masters' == subCmd) {
            // sentinel masters
            if (data.length != 2) {
                return ErrorReply.SYNTAX
            }

            def firstOneSlot = localPersist.currentThreadFirstOneSlot()

            def values = [:]
            values.name = ConfForGlobal.netListenAddresses
            def array = ConfForGlobal.netListenAddresses.split(':')
            values.ip = array[0]
            values.port = array[1]
            values.runid = firstOneSlot.masterUuid.toString()
            values.flags = 'master'
            values['pending-commands'] = 0
            values['last-ok-ping-reply'] = 0
            values['last-ping-reply'] = 0
            values['info-refresh'] = 0
            values['master-link-down-time'] = 0
            values['master-link-status'] = 'ok'
            values['master-host'] = 'localhost'
            values['master-port'] = array[0]
            values['slave-priority'] = array[1]

            def repliesInner = new Reply[values.size() * 2]
            int i = 0
            values.each { k, v ->
                repliesInner[i++] = new BulkReply(k.toString().bytes)
                repliesInner[i++] = new BulkReply(v.toString().bytes)
            }

            Reply[] repliesOuter = [
                    new MultiBulkReply(repliesInner)
            ]
            return new MultiBulkReply(repliesOuter)
        }

        if ('get-master-addr-by-name' == subCmd) {
            // sentinel get-master-addr-by-name <master-name>
            if (data.length != 3) {
                return ErrorReply.SYNTAX
            }

            // ignore master name as self server mock as sentinel
            def masterName = new String(data[2])
            log.debug 'Repl sentinel get-master-addr-by-name master name={}', masterName

            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
            def replPairAsSlave = firstOneSlot.onlyOneReplPairAsSlave
            if (replPairAsSlave != null) {
                Reply[] replies = [
                        new BulkReply(replPairAsSlave.host.bytes),
                        new BulkReply(replPairAsSlave.port.toString().bytes)
                ]
                return new MultiBulkReply(replies)
            } else {
                // self is master
                def listenAddress = ConfForGlobal.netListenAddresses
                def array = listenAddress.split(':')
                Reply[] replies = [
                        new BulkReply(array[0].bytes),
                        new BulkReply(array[1].bytes)
                ]
                return new MultiBulkReply(replies)
            }
        }

        ErrorReply.NOT_SUPPORT
    }
}
