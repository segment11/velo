package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.ErrorReply;
import io.velo.reply.Reply;

import java.util.ArrayList;

/**
 * Handles Redis commands starting with letter 'J'.
 * Currently used as a placeholder with no specific commands implemented.
 */
public class JGroup extends BaseCommand {
    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public JGroup(String cmd, byte[][] data, ITcpSocket socket) {
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
        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }
}
