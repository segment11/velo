package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.NilReply;
import io.velo.reply.OKReply;
import io.velo.reply.Reply;

import java.util.ArrayList;

public class BGroup extends BaseCommand {
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("bgsave".equals(cmd)) {
            // already saved when handle request
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }
}
