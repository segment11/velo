package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * Handles Redis commands starting with letter 'U'.
 * This class is part of command organization where commands are grouped alphabetically.
 */
public class UGroup extends BaseCommand {
    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public UGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    /**
     * Parses slot information from the command.
     *
     * @param cmd        the command name
     * @param data       the command arguments
     * @param slotNumber current slot number
     * @return list containing slot with key hash, or empty list
     */
    @Override
    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    /**
     * Handles the command and returns the reply.
     *
     * @return the reply for this command
     */
    @Override
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
