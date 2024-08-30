package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;

import java.util.ArrayList;

public class YGroup extends BaseCommand {
    public YGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        return NilReply.INSTANCE;
    }
}
