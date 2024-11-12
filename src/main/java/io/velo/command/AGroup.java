package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.acl.Category;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

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
            var categories = Category.values();
            if (category == null) {
                var replies = new Reply[categories.length];
                for (int i = 0; i < categories.length; i++) {
                    replies[i] = new BulkReply(categories[i].name().getBytes());
                }
                return new MultiBulkReply(replies);
            } else {
                int index = -1;
                for (int i = 0; i < categories.length; i++) {
                    if (categories[i].name().equals(category)) {
                        index = i;
                        break;
                    }
                }
                if (index == -1) {
                    return ErrorReply.SYNTAX;
                }

                var cmdList = Category.getCmdListByCategory(categories[index]);
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
