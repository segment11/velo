package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.SocketInspector;
import io.velo.persist.OneSlot;
import io.velo.reply.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
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
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }
        try {
            timeoutMillis = Long.parseLong(new String(data[2]));
        } catch (NumberFormatException e) {
            return new ErrorReply("timeout is not an integer or out of range");
        }

        // redis only rejects a negative timeout, with this exact text; timeout 0 means no deadline
        if (timeoutMillis < 0) {
            return new ErrorReply("timeout is negative");
        }

        var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
        var lastSetSeq = veloUserData.getLastSetSeq();
        if (lastSetSeq == 0L) {
            // redis waits on offset 0 for a client without writes, satisfied by every online replica;
            // velo replicates per slot, so count the distinct replica endpoints of the slots this node masters
            return new IntegerReply(countDistinctReplicas());
        }

        var oneSlot = localPersist.oneSlot(veloUserData.getLastSetSlot());
        var isAsSlave = oneSlot.isAsSlave();
        if (isAsSlave) {
            // the redis error, so a client can tell a replica from "0 replicas acked"
            return new ErrorReply("WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        }

        // as master

        // redis answers immediately with the current ack count when numreplicas <= 0
        if (numReplicas <= 0) {
            return new IntegerReply(countCaughtUpSlaves(oneSlot, lastSetSeq));
        }

        // first try without blocking at all, like redis replication.c waitCommand
        int firstTryCount = countCaughtUpSlaves(oneSlot, lastSetSeq);
        if (firstTryCount >= numReplicas) {
            return new IntegerReply(firstTryCount);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // poll until satisfied or the deadline; timeout 0 means no deadline, like redis
        pollWaitForReplicas(oneSlot, finalPromise, lastSetSeq, numReplicas, timeoutMillis, System.currentTimeMillis());

        return asyncReply;
    }

    /**
     * Polls the slave catch-up state on the slot event loop until enough slaves acknowledged the sequence number,
     * or until the deadline. A timeout of 0 never expires, like redis.
     *
     * @param oneSlot       the slot this node acts as master for
     * @param finalPromise  the promise to settle with the ack count
     * @param lastSetSeq    the sequence number to compare against
     * @param numReplicas   the number of replicas the client waits for, always positive here
     * @param timeoutMillis the timeout in milliseconds, 0 means no deadline
     * @param beginMillis   the wall clock when the wait started
     */
    private static void pollWaitForReplicas(@NotNull OneSlot oneSlot, SettablePromise<Reply> finalPromise,
                                            long lastSetSeq, long numReplicas, long timeoutMillis, long beginMillis) {
        final int pollIntervalMillis = 10;
        oneSlot.delayRun(pollIntervalMillis, () -> {
            // re-read the slave list on every tick, the list may change while waiting
            int count = countCaughtUpSlaves(oneSlot, lastSetSeq);
            if (count >= numReplicas) {
                finalPromise.set(new IntegerReply(count));
                return;
            }

            if (timeoutMillis > 0 && System.currentTimeMillis() - beginMillis >= timeoutMillis) {
                finalPromise.set(new IntegerReply(count));
                return;
            }

            pollWaitForReplicas(oneSlot, finalPromise, lastSetSeq, numReplicas, timeoutMillis, beginMillis);
        });
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

    /**
     * Counts the distinct replicas of all the slots this node acts as master for, by remote endpoint.
     * Used for a client without writes: like redis, every online replica already satisfies offset 0.
     *
     * @return the number of distinct replica endpoints
     */
    private int countDistinctReplicas() {
        var endpoints = new HashSet<String>();
        for (var oneSlot : localPersist.initializedOneSlots()) {
            for (var replPair : oneSlot.getSlaveReplPairListSelfAsMaster()) {
                endpoints.add(replPair.getHostAndPort());
            }
        }
        return endpoints.size();
    }
}
