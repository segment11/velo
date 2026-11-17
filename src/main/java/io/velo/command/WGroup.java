package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.SocketInspector;
import io.velo.persist.OneSlot;
import io.velo.reply.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'W'.
 * This includes commands like WAIT.
 */
public class WGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "wait", 3,
                Set.of(),
                0, 0, 0,
                Set.of("@connection", "@fast", "@slow"),
                "generic",
                "Block until the specified number of replicas have acknowledged all the writes.",
                "3.0.0", "O(1)"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public WGroup(String cmd, byte[][] data, ITcpSocket socket) {
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
        if ("wait".equals(cmd)) {
            return cmdWait();
        }

        // the command is not implemented, redis answers an unknown command error
        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }

    private Reply cmdWait() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        long numReplicas;
        long timeoutMillis;
        try {
            numReplicas = Long.parseLong(new String(data[1]));
            timeoutMillis = Long.parseLong(new String(data[2]));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }

        // redis only rejects a negative timeout, with this exact text; timeout 0 means no deadline
        if (timeoutMillis < 0) {
            return new ErrorReply("timeout is negative");
        }

        var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
        var lastSetSeq = veloUserData.getLastSetSeq();
        if (lastSetSeq == 0L) {
            return IntegerReply.REPLY_0;
        }

        var oneSlot = localPersist.oneSlot(veloUserData.getLastSetSlot());
        var isAsSlave = oneSlot.isAsSlave();
        if (isAsSlave) {
            return IntegerReply.REPLY_0;
        }

        // as master
        var slaveReplPairList = oneSlot.getSlaveReplPairListSelfAsMaster();
        if (slaveReplPairList.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        // redis answers immediately with the current ack count when numreplicas <= 0
        if (numReplicas <= 0) {
            return new IntegerReply(countCaughtUpSlaves(oneSlot, lastSetSeq));
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        oneSlot.delayRun((int) timeoutMillis, () -> {
            // check again
            var slaveReplPairList2 = oneSlot.getSlaveReplPairListSelfAsMaster();
            if (slaveReplPairList2.isEmpty()) {
                finalPromise.set(IntegerReply.REPLY_0);
                return;
            }

            // check slaves catch up seq
            int catchUpSlaveCount = 0;
            for (var slaveReplPair : slaveReplPairList2) {
                if (slaveReplPair.getSlaveCatchUpLastSeq() >= lastSetSeq) {
                    catchUpSlaveCount++;
                    if (catchUpSlaveCount == numReplicas) {
                        break;
                    }
                }
            }

            finalPromise.set(new IntegerReply(catchUpSlaveCount));
        });

        return asyncReply;
    }

    /**
     * Counts the slaves of the slot that acknowledged the given sequence number.
     *
     * @param oneSlot    the slot this node acts as master for
     * @param lastSetSeq the sequence number to compare against
     * @return the number of slaves whose catch-up seq is at least lastSetSeq
     */
    private static int countCaughtUpSlaves(@NotNull OneSlot oneSlot, long lastSetSeq) {
        int count = 0;
        for (var slaveReplPair : oneSlot.getSlaveReplPairListSelfAsMaster()) {
            if (slaveReplPair.getSlaveCatchUpLastSeq() >= lastSetSeq) {
                count++;
            }
        }
        return count;
    }
}
