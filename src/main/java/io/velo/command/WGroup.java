package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.SocketInspector;
import io.velo.reply.*;

import java.util.ArrayList;

public class WGroup extends BaseCommand {
    public WGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("wait".equals(cmd)) {
            return cmdWait();
        }

        return NilReply.INSTANCE;
    }

    private Reply cmdWait() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        int numReplicas;
        int timeoutMillis;
        try {
            numReplicas = Integer.parseInt(new String(data[1]));
            timeoutMillis = Integer.parseInt(new String(data[2]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numReplicas <= 0 || timeoutMillis <= 0) {
            return ErrorReply.INVALID_INTEGER;
        }
        final int maxTimeoutMillis = 1000 * 60;
        if (timeoutMillis > maxTimeoutMillis) {
            return new ErrorReply("timeout should <= " + maxTimeoutMillis);
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

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        oneSlot.delayRun(timeoutMillis, () -> {
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
}
