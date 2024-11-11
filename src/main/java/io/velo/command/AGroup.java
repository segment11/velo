package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AGroup extends BaseCommand {
    public AGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("append".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    @Override
    public Reply handle() {
        if ("acl".equals(cmd)) {
            return acl();
        }

        if ("append".equals(cmd)) {
            return append();
        }

        return NilReply.INSTANCE;
    }

    private static final List<String> ACL_CATEGORIES = List.of(
            "keyspace", "read", "write",
            "set", "sortedset", "list", "hash", "string", "bitmap",
            "hyperloglog", "geo", "stream",
            "pubsub", "admin", "fast", "slow",
            "blocking", "dangerous", "connection",
            "connection", "transaction", "scripting");

    private static final Map<String, List<String>> ACL_CMD_LIST_BY_CATEGORY = new HashMap<>();

    static {
        List<String> dangerousCmdList = List.of("flushdb", "acl", "showlog",
                "debug", "role", "keys", "pfselftest",
                "client", "bgrewriteaof", "replicaof",
                "monitor", "restore-asking", "latency",
                "replconf", "pfdebug", "bgsave", "sync",
                "config", "flushall", "cluster", "info",
                "lastsave", "slaveof", "swapdb", "module",
                "restore", "migrate", "save", "shutdown",
                "psync", "sort");
        ACL_CMD_LIST_BY_CATEGORY.put("dangerous", dangerousCmdList);
    }

    @VisibleForTesting
    Reply acl() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();

        if ("cat".equals(subCmd)) {
            if (data.length != 2 && data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var category = data.length == 3 ? new String(data[2]).toLowerCase() : null;
            if (category == null) {
                var replies = new Reply[ACL_CATEGORIES.size()];
                for (int i = 0; i < ACL_CATEGORIES.size(); i++) {
                    replies[i] = new BulkReply(ACL_CATEGORIES.get(i).getBytes());
                }
                return new MultiBulkReply(replies);
            } else {
                if (!ACL_CATEGORIES.contains(category)) {
                    return ErrorReply.SYNTAX;
                }

                var cmdList = ACL_CMD_LIST_BY_CATEGORY.get(category);
                var replies = new Reply[cmdList.size()];
                for (int i = 0; i < cmdList.size(); i++) {
                    replies[i] = new BulkReply(cmdList.get(i).getBytes());
                }
                return new MultiBulkReply(replies);
            }
        } else if ("deluser".equals(subCmd)) {
            if (data.length < 3) {
                return ErrorReply.SYNTAX;
            }

            List<String> userList = new ArrayList<>();
            for (int i = 2; i < data.length; i++) {
                var user = new String(data[i]);
                userList.add(user);
            }

            // todo

            return IntegerReply.REPLY_1;
        }

        // todo: implement other sub commands

        return ErrorReply.SYNTAX;
    }

    @VisibleForTesting
    Reply append() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var valueBytes = data[2];

        int length;

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var existsValueBytes = get(keyBytes, slotWithKeyHash);
        if (existsValueBytes == null) {
            set(keyBytes, valueBytes, slotWithKeyHash);
            length = valueBytes.length;
        } else {
            var newValueBytes = new byte[existsValueBytes.length + valueBytes.length];
            System.arraycopy(existsValueBytes, 0, newValueBytes, 0, existsValueBytes.length);
            System.arraycopy(valueBytes, 0, newValueBytes, existsValueBytes.length, valueBytes.length);

            set(keyBytes, newValueBytes, slotWithKeyHash);
            length = newValueBytes.length;
        }

        return new IntegerReply(length);
    }
}
