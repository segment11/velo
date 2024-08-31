package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.ErrorReply;
import io.velo.reply.IntegerReply;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

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
        if ("append".equals(cmd)) {
            return append();
        }

        return NilReply.INSTANCE;
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
