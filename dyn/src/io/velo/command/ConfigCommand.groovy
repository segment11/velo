package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.MultiWorkerServer
import io.velo.reply.*

/**
 * Implements the CONFIG command, supporting runtime get/set of configuration parameters.
 */
@CompileStatic
class ConfigCommand extends BaseCommand {
    static final String version = '1.0.0'

    /**
     * Creates a ConfigCommand with no bound group (used for dispatch).
     */
    ConfigCommand() {
        super(null, null, null)
    }

    /**
     * Creates a ConfigCommand from the given CGroup, copying its command data and socket.
     *
     * @param cGroup the group providing the command context
     */
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
        log.info 'Dyn config command version={}', version

        if (data.length < 2) {
            return ErrorReply.FORMAT
//            return new ErrorReply('wrong number of arguments for \'config\' command')
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('help' == subCmd) {
            Reply[] replies = [
                    new BulkReply('CONFIG <subcommand> arg arg ... arg. Subcommands are:'),
                    new BulkReply('GET <pattern> -- Return parameters matching the glob-like <pattern> and their values.'),
                    new BulkReply('SET <parameter> <value> -- Set parameter to value.'),
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

    /**
     * Handles CONFIG SET, updating a runtime configuration parameter (e.g. max_connections).
     */
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

            if (maxConnections <= 0) {
                return ErrorReply.INVALID_INTEGER
            }

            MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(maxConnections)
            log.warn "Global config set max_connections={}", maxConnections

            def firstOneSlot = localPersist.currentThreadFirstOneSlot()
            try {
                firstOneSlot.dynConfig.update(configKey, maxConnections)
            } catch (IOException e) {
                log.error 'Global config update dyn config error', e
                return new ErrorReply("update dyn config error=" + e.message)
            }
            return OKReply.INSTANCE
        }

        ErrorReply.SYNTAX
    }

    /**
     * Handles CONFIG GET, returning the value of a runtime configuration parameter.
     */
    Reply _get() {
        if (data.length < 3) {
            return ErrorReply.SYNTAX
        }

        def configKey = new String(data[2]).toLowerCase()
        if ('max_connections' == configKey) {
            def maxConnections = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections
            return new BulkReply(maxConnections.toString().bytes)
        } else {
            // todo
        }

        NilReply.INSTANCE
    }
}
