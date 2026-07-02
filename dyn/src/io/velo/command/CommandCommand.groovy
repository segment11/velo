package io.velo.command

import groovy.transform.CompileStatic
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.reply.*

import java.util.regex.Pattern

/**
 * Implements the COMMAND command, returning metadata and key extraction for supported commands.
 */
@CompileStatic
class CommandCommand extends BaseCommand {
    static final String version = '1.0.0'

    /**
     * Creates a CommandCommand with no bound group (used for dispatch).
     */
    CommandCommand() {
        super(null, null, null)
    }

    /**
     * Creates a CommandCommand from the given CGroup, copying its command data and socket.
     *
     * @param cGroup the group providing the command context
     */
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

        // bare COMMAND (no subcommand) returns info for all commands
        if (data.length == 1) {
            return info()
        }

        if (data.length < 2) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1]).toLowerCase()
        if ('count' == subCmd) {
            return new IntegerReply(CommandRegistry.size())
        } else if ('docs' == subCmd) {
            return docs()
        } else if ('getkeys' == subCmd) {
            return getkeys()
        } else if ('getkeysandflags' == subCmd) {
            return getkeys(true)
        } else if ('help' == subCmd) {
            return help()
        } else if ('info' == subCmd) {
            return info()
        } else if ('list' == subCmd) {
            return list()
        } else {
            return ErrorReply.SYNTAX
        }
    }

    private Reply info() {
        // COMMAND INFO with no names (or bare COMMAND) -> all commands;
        // COMMAND INFO name1 name2 -> one entry per name, NilReply for unknown.
        boolean isAll = data.length <= 2

        if (isAll) {
            def entries = CommandRegistry.all()
            def replies = new Reply[entries.size()]
            int i = 0
            for (CommandEntry entry : entries) {
                replies[i++] = commandInfoReply(entry)
            }
            return new MultiBulkReply(replies)
        }

        def replies = new Reply[data.length - 2]
        for (int i = 2; i < data.length; i++) {
            def name = new String(data[i]).toLowerCase()
            def entry = CommandRegistry.get(name)
            replies[i - 2] = entry != null ? commandInfoReply(entry) : NilReply.INSTANCE
        }
        new MultiBulkReply(replies)
    }

    /**
     * Builds the 10-element array for one command, matching Redis {@code addReplyCommandInfo}:
     * name, arity, flags[], firstkey, lastkey, keystep, acl_categories(set), tips[], key_specs[], subcommands[].
     */
    private static Reply commandInfoReply(CommandEntry entry) {
        def ten = new Reply[10]
        ten[0] = new BulkReply(entry.name().getBytes())
        ten[1] = new IntegerReply(entry.arity())

        def flagsArr = new Reply[entry.flags().size()]
        int j = 0
        for (String f : entry.flags()) {
            flagsArr[j++] = new BulkReply(f.getBytes())
        }
        ten[2] = new MultiBulkReply(flagsArr)

        ten[3] = new IntegerReply(entry.firstKey())
        ten[4] = new IntegerReply(entry.lastKey())
        ten[5] = new IntegerReply(entry.keyStep())

        def catsArr = new Reply[entry.aclCategories().size()]
        j = 0
        for (String c : entry.aclCategories()) {
            catsArr[j++] = new BulkReply(c.getBytes())
        }
        ten[6] = new MultiBulkReply(catsArr, false, true)

        ten[7] = new MultiBulkReply(new Reply[0])
        ten[8] = new MultiBulkReply(new Reply[0])
        ten[9] = new MultiBulkReply(new Reply[0])
        new MultiBulkReply(ten)
    }

    private Reply docs() {
        // COMMAND DOCS with no names -> all commands; with names -> only found ones (unknown skipped).
        boolean isAll = data.length <= 2

        if (isAll) {
            def entries = CommandRegistry.all()
            def pairs = new Reply[entries.size() * 2]
            int i = 0
            for (CommandEntry entry : entries) {
                pairs[i++] = new BulkReply(entry.name().getBytes())
                pairs[i++] = commandDocsReply(entry)
            }
            return new MultiBulkReply(pairs, true, false)
        }

        def pairs = new ArrayList<Reply>()
        for (int i = 2; i < data.length; i++) {
            def name = new String(data[i]).toLowerCase()
            def entry = CommandRegistry.get(name)
            if (entry == null) {
                continue
            }
            pairs.add(new BulkReply(entry.name().getBytes()))
            pairs.add(commandDocsReply(entry))
        }
        new MultiBulkReply(pairs.toArray(new Reply[0]), true, false)
    }

    /**
     * Builds the documentation map for one command, matching Redis {@code addReplyCommandDocs}.
     * Order: summary, since, group (always), complexity.
     */
    private static Reply commandDocsReply(CommandEntry entry) {
        def fields = new ArrayList<Reply>()
        if (entry.summary() != null) {
            fields.add(new BulkReply('summary'.getBytes()))
            fields.add(new BulkReply(entry.summary().getBytes()))
        }
        if (entry.since() != null) {
            fields.add(new BulkReply('since'.getBytes()))
            fields.add(new BulkReply(entry.since().getBytes()))
        }
        // group is always present
        fields.add(new BulkReply('group'.getBytes()))
        fields.add(new BulkReply(entry.group().getBytes()))
        if (entry.complexity() != null) {
            fields.add(new BulkReply('complexity'.getBytes()))
            fields.add(new BulkReply(entry.complexity().getBytes()))
        }
        new MultiBulkReply(fields.toArray(new Reply[0]), true, false)
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

        // Best-effort v1: derive per-key RW/RO + access from the command's flags.
        def targetEntry = CommandRegistry.get(targetCmd)
        boolean isWrite = targetEntry != null && targetEntry.flags().contains('write')
        def keyFlags = isWrite
                ? new Reply[]{new BulkReply('RW'.getBytes()), new BulkReply('access'.getBytes())}
                : new Reply[]{new BulkReply('RO'.getBytes()), new BulkReply('access'.getBytes())}

        def replies = new Reply[sList.size()]
        for (int i = 0; i < sList.size(); i++) {
            def keyBytes = sList[i].rawKey().bytes
            if (withFlags) {
                def subReplies = new Reply[2]
                subReplies[0] = new BulkReply(keyBytes)
                subReplies[1] = new MultiBulkReply(keyFlags)
                replies[i] = new MultiBulkReply(subReplies)
            } else {
                replies[i] = new BulkReply(keyBytes)
            }
        }
        new MultiBulkReply(replies)
    }

    private Reply list() {
        String filterType = null
        String filterArg = null
        for (int i = 2; i < data.length; i++) {
            def opt = new String(data[i]).toLowerCase()
            if ('filterby' == opt && i + 2 < data.length) {
                def ft = new String(data[i + 1]).toLowerCase()
                if ('module' == ft || 'aclcat' == ft || 'pattern' == ft) {
                    filterType = ft
                    filterArg = new String(data[i + 2])
                    i += 2
                } else {
                    return ErrorReply.SYNTAX
                }
            } else {
                return ErrorReply.SYNTAX
            }
        }

        def entries = CommandRegistry.all()
        List<String> names = new ArrayList<>()
        for (CommandEntry entry : entries) {
            if (filterType == null) {
                names.add(entry.name())
            } else if ('module' == filterType) {
                // Velo has no module commands
                break
            } else if ('aclcat' == filterType) {
                def cat = ('@' + filterArg).toLowerCase()
                if (entry.aclCategories().contains(cat)) {
                    names.add(entry.name())
                }
            } else if ('pattern' == filterType) {
                if (globMatch(filterArg, entry.name())) {
                    names.add(entry.name())
                }
            }
        }

        def replies = new Reply[names.size()]
        for (int i = 0; i < names.size(); i++) {
            replies[i] = new BulkReply(names[i].getBytes())
        }
        new MultiBulkReply(replies)
    }

    private static Reply help() {
        String header = 'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
        String[] lines = [
                '(no subcommand)',
                '    Return details about all Redis commands.',
                'COUNT',
                '    Return the total number of commands in this Redis server.',
                'LIST',
                '    Return a list of all commands in this Redis server.',
                'INFO [<command-name> ...]',
                '    Return details about multiple Redis commands.',
                '    If no command names are given, documentation details for all',
                '    commands are returned.',
                'DOCS [<command-name> ...]',
                '    Return documentation details about multiple Redis commands.',
                '    If no command names are given, documentation details for all',
                '    commands are returned.',
                'GETKEYS <full-command>',
                '    Return the keys from a full Redis command.',
                'GETKEYSANDFLAGS <full-command>',
                '    Return the keys and the access flags from a full Redis command.',
                'HELP',
                '    Print this help.'
        ]
        def replies = new Reply[lines.length + 1]
        replies[0] = new BulkReply(header.getBytes())
        for (int i = 0; i < lines.length; i++) {
            replies[i + 1] = new BulkReply(lines[i].getBytes())
        }
        new MultiBulkReply(replies)
    }

    /**
     * Simple glob matcher: {@code *} matches any sequence, {@code ?} matches a single character.
     * Regex metacharacters in the pattern are escaped, matching Redis {@code stringmatchlen}.
     */
    static boolean globMatch(String pattern, String str) {
        def sb = new StringBuilder()
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i)
            if (c == '*') {
                sb.append('.*')
            } else if (c == '?') {
                sb.append('.')
            } else {
                sb.append(Pattern.quote(String.valueOf(c)))
            }
        }
        Pattern.compile(sb.toString()).matcher(str).matches()
    }
}
