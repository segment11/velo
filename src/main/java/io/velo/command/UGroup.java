package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

public class UGroup extends BaseCommand {
    public UGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if (cmd.equals("unsubscribe")) {
            return unsubscribe(false);
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply unsubscribe(boolean isPattern) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var channels = new ArrayList<String>(data.length - 1);
        for (int i = 1; i < data.length; i++) {
            var channel = new String(data[i]);
            channels.add(channel);
        }

        var socketInInspector = localPersist.getSocketInspector();

        var replies = new Reply[channels.size() * 3];
        int j = 0;
        for (var channel : channels) {
            replies[j++] = new BulkReply("unsubscribe");
            replies[j++] = new BulkReply(channel);
            var size = socketInInspector.unsubscribe(channel, isPattern, socket);
            replies[j++] = new IntegerReply(size);
        }

        return new MultiBulkReply(replies);
    }
}
