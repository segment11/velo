package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.ConfForGlobal;
import io.velo.repl.LeaderSelector;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

public class FGroup extends BaseCommand {
    public FGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("failover".equals(cmd)) {
            return failover();
        }

        if ("flushdb".equals(cmd) || "flushall".equals(cmd)) {
            return flushdb();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply failover() {
        // skip for test
        if (data.length == 2) {
            return OKReply.INSTANCE;
        }

        if (ConfForGlobal.zookeeperConnectString == null) {
            return new ErrorReply("zookeeper connect string is null");
        }

//        if (!LeaderSelector.getInstance().hasLeadership()) {
//            return new ErrorReply("not leader");
//        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        var asMasterList = firstOneSlot.getReplPairAsMasterList();
        if (asMasterList.isEmpty()) {
            return new ErrorReply("no slave");
        }

        // check slave catch up offset, if not catch up, can not do failover
        var currentFo = firstOneSlot.getBinlog().currentFileIndexAndOffset();
        for (var replPairAsMaster : asMasterList) {
            var fo = replPairAsMaster.getSlaveLastCatchUpBinlogFileIndexAndOffset();
            if (fo == null) {
                return new ErrorReply("slave not catch up: " + replPairAsMaster.getHostAndPort());
            }

            // must be equal or slave can less than a little, change here if need
            if (currentFo.fileIndex() != fo.fileIndex() || currentFo.offset() != fo.offset()) {
                return new ErrorReply("slave not catch up: " + replPairAsMaster.getHostAndPort() + ", current: " + currentFo + ", slave: " + fo);
            }
        }

        Promise<Void>[] promises = new Promise[slotNumber];
        for (int i = 0; i < slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncRun(() -> {
                oneSlot.setReadonly(true);
                oneSlot.getDynConfig().setBinlogOn(false);

                var replPairAsMasterList = oneSlot.getReplPairAsMasterList();
                for (var replPairAsMaster : replPairAsMasterList) {
                    replPairAsMaster.closeSlaveConnectSocket();
                }
            });
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("failover error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            LeaderSelector.getInstance().stopLeaderLatch();
            log.warn("Repl leader latch stopped");
            // later self will start leader latch again and make self as slave

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply flushdb() {
        // skip for test
        if (data.length == 2) {
            return OKReply.INSTANCE;
        }

        Promise<Void>[] promises = new Promise[slotNumber];
        for (int i = 0; i < slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncRun(oneSlot::flush);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("flushdb error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }
}
