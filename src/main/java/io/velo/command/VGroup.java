package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;

import java.util.ArrayList;

/**
 * Handles Redis commands starting with letter 'V'.
 * This class is part of command organization where commands are grouped alphabetically.
 */
public class VGroup extends BaseCommand {
    public VGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    private static final int limit = 10;

    public Reply handle() {
        return NilReply.INSTANCE;
    }
}
