package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.ConfForGlobal;
import io.velo.repl.LeaderSelector;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.OKReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
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
            var slaveFo = replPairAsMaster.getSlaveLastCatchUpBinlogFileIndexAndOffset();
            if (slaveFo == null) {
                return new ErrorReply("slave not catch up=" + replPairAsMaster.getHostAndPort());
            }

            // must be equal or slave can less than a little, change here if you need
            if (currentFo.fileIndex() != slaveFo.fileIndex() || currentFo.offset() != slaveFo.offset()) {
                return new ErrorReply("slave not catch up=" + replPairAsMaster.getHostAndPort() + ", current=" + currentFo + ", slave=" + slaveFo);
            }
        }

        return localPersist.doSthInSlots(oneSlot -> {
            try {
                oneSlot.setReadonly(true);
                oneSlot.getDynConfig().setBinlogOn(false);

                var replPairAsMasterList = oneSlot.getReplPairAsMasterList();
                for (var replPairAsMaster : replPairAsMasterList) {
                    replPairAsMaster.closeSlaveConnectSocket();
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, resultList -> {
            LeaderSelector.getInstance().stopLeaderLatch();
            log.warn("Repl leader latch stopped");
            // later self will start leader latch again and make self as slave

            return OKReply.INSTANCE;
        });
    }

    @VisibleForTesting
    Reply flushdb() {
        // skip for test
        if (data.length == 2) {
            return OKReply.INSTANCE;
        }

        return localPersist.doSthInSlots(oneSlot -> {
            oneSlot.flush();

            if (oneSlot == localPersist.firstOneSlot()) {
                var indexHandlerPool = localPersist.getIndexHandlerPool();
                if (indexHandlerPool != null) {
                    indexHandlerPool.getKeyAnalysisHandler().flushdb();
                }
            }
            return true;
        }, resultList -> OKReply.INSTANCE);
    }
}
