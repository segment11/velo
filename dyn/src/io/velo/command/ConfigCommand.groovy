package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.MultiWorkerServer
import io.velo.reply.*

@CompileStatic
class ConfigCommand extends BaseCommand {
    static final String version = '1.0.0'

    ConfigCommand(CGroup cGroup) {
        super(cGroup.cmd, cGroup.data, cGroup.socket)
    }

    @Override
    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> list = []
        list
    }

    @Override
    Reply handle() {
        log.info 'Dyn config command version: {}', version

        if (data.length < 2) {
            return new ErrorReply('wrong number of arguments for \'config\' command')
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('help' == subCmd) {
            Reply[] replies = [
                    new BulkReply('CONFIG <subcommand> arg arg ... arg. Subcommands are:'.bytes),
                    new BulkReply('GET <pattern> -- Return parameters matching the glob-like <pattern> and their values.'.bytes),
                    new BulkReply('SET <parameter> <value> -- Set parameter to value.'.bytes),
            ]
            return new MultiBulkReply(replies)
        }

        if ('set' == subCmd) {
            return _set()
        } else if ('get' == subCmd) {
            return _get()
        } else {
            return ErrorReply.SYNTAX
        }
    }

    Reply _set() {
        if (data.length < 4) {
            return ErrorReply.SYNTAX
        }

        def configKey = new String(data[2]).toLowerCase()
        def configValue = new String(data[3])

        if ("max_connections" == configKey) {
            int maxConnections
            try {
                maxConnections = Integer.parseInt(configValue)
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(maxConnections)
            log.warn "Global config set max_connections={}", maxConnections

            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
            try {
                firstOneSlot.dynConfig.update(configKey, maxConnections)
            } catch (IOException e) {
                log.error 'Global config update dyn config error', e
                return new ErrorReply("update dyn config error: " + e.message)
            }
            return OKReply.INSTANCE
        } else {
            // todo
        }

        OKReply.INSTANCE
    }

    Reply _get() {
        if (data.length < 3) {
            return ErrorReply.SYNTAX
        }

        def configKey = new String(data[2]).toLowerCase()
        if ('max_connections' == configKey) {
            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
            def maxConnections = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections
            return new BulkReply(maxConnections.toString().bytes)
        } else {
            // todo
        }

        NilReply.INSTANCE
    }
}
