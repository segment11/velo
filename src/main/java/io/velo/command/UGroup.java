package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'U'.
 * This class is part of command organization where commands are grouped alphabetically.
 */
public class UGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "unsubscribe", -1,
                Set.of("pubsub", "noscript", "loading", "stale"),
                0, 0, 0,
                Set.of("@pubsub", "@slow"),
                "pubsub",
                "Stop listening for messages posted to the given channels.",
                "2.0.0", "O(N)"));
    }

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

        // the command is not implemented, redis answers an unknown command error
        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }

    @VisibleForTesting
    Reply unsubscribe(boolean isPattern) {
        var socketInInspector = localPersist.getSocketInspector();

        var channels = new ArrayList<String>();
        if (data.length < 2) {
            // redis unsubscribes all the channels (or patterns) of this client
            channels.addAll(socketInInspector.subscriptionsOf(socket, isPattern));
            if (channels.isEmpty()) {
                // subscribed to nothing: redis still replies, with a nil channel
                return new MultiBulkReply(new Reply[]{
                        new BulkReply(isPattern ? "punsubscribe" : "unsubscribe"),
                        NilReply.INSTANCE,
                        new IntegerReply(0)});
            }
        } else {
            for (int i = 1; i < data.length; i++) {
                channels.add(new String(data[i]));
            }
        }

        // redis replies one 3-element frame per channel, not one flattened 3N-element array
        var triples = new ArrayList<Reply>(channels.size());
        for (var channel : channels) {
            triples.add(new MultiBulkReply(new Reply[]{
                    new BulkReply(isPattern ? "punsubscribe" : "unsubscribe"),
                    new BulkReply(channel),
                    new IntegerReply(socketInInspector.unsubscribe(channel, isPattern, socket))}));
        }

        // a single confirmation is the frame itself
        if (triples.size() == 1) {
            return triples.getFirst();
        }

        return new MultiFrameReply(triples.toArray(new Reply[0]));
    }
}
