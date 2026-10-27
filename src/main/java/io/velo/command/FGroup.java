package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.repl.LeaderSelector;
import io.velo.reply.AsyncReply;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.OKReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'F'.
 * This includes commands like FAILOVER, FLUSHDB, FLUSHALL, etc.
 */
public class FGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "failover", -1,
                Set.of("admin", "noscript", "stale"),
                0, 0, 0,
                Set.of("@admin", "@dangerous", "@slow"),
                "server",
                "Start a coordinated failover from a server to one of its replicas.",
                "6.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "flushall", -1,
                Set.of("write"),
                0, 0, 0,
                Set.of("@keyspace", "@write", "@slow", "@dangerous"),
                "server",
                "Remove all keys from all databases.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "flushdb", -1,
                Set.of("write"),
                0, 0, 0,
                Set.of("@keyspace", "@write", "@slow", "@dangerous"),
                "server",
                "Remove all keys from the current database.",
                "1.0.0", "O(N)"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public FGroup(String cmd, byte[][] data, ITcpSocket socket) {
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
        if ("failover".equals(cmd)) {
            return failover();
        }

        if ("flushdb".equals(cmd) || "flushall".equals(cmd)) {
            return flushdb();
        }

        return NilReply.INSTANCE;
    }

    /** Per-slot failover outcome: the readonly state before this command fenced the slot, and the first error (null when ok). */
    private record FailoverSlotResult(boolean wasReadonly, String error) {}

    @VisibleForTesting
    Reply failover() {
        // FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
        // this failover is synchronous, there is no ongoing failover state to abort
        if (data.length == 2 && "abort".equalsIgnoreCase(new String(data[1]))) {
            return new ErrorReply("No failover in progress.");
        }

        // iterate a captured slot list so per-slot results stay index-aligned with the rollback below
        var slots = localPersist.initializedOneSlots();
        // per slot, on the slot's own eventloop: fence first so no new write can advance the binlog,
        // then validate this slot's slaves catch-up, only then demote it (close slave sockets, binlog off)
        Promise<FailoverSlotResult>[] promises = new Promise[slots.size()];
        for (int i = 0; i < slots.size(); i++) {
            var oneSlot = slots.get(i);
            promises[i] = oneSlot.asyncCall(() -> {
                try {
                    var wasReadonly = oneSlot.isReadonly();
                    oneSlot.setReadonly(true);

                    var asMasterList = oneSlot.getReplPairAsMasterList();
                    if (asMasterList.isEmpty()) {
                        return new FailoverSlotResult(wasReadonly, "no slave");
                    }

                    // must be equal or slave can less than a little, change here if you need
                    var currentFo = oneSlot.getBinlog().currentFileIndexAndOffset();
                    for (var replPairAsMaster : asMasterList) {
                        var slaveFo = replPairAsMaster.getSlaveLastCatchUpBinlogFileIndexAndOffset();
                        if (slaveFo == null) {
                            return new FailoverSlotResult(wasReadonly, "slave not catch up=" + replPairAsMaster.getHostAndPort());
                        }
                        if (currentFo.fileIndex() != slaveFo.fileIndex() || currentFo.offset() != slaveFo.offset()) {
                            return new FailoverSlotResult(wasReadonly, "slave not catch up=" + replPairAsMaster.getHostAndPort() + ", current=" + currentFo + ", slave=" + slaveFo);
                        }
                    }

                    oneSlot.getDynConfig().setBinlogOn(false);
                    for (var replPairAsMaster : asMasterList) {
                        replPairAsMaster.closeSlaveConnectSocket();
                    }
                    return new FailoverSlotResult(wasReadonly, null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("async all error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            String firstError = null;
            for (var result : promises) {
                if (result.getResult().error() != null) {
                    firstError = result.getResult().error();
                    break;
                }
            }

            if (firstError != null) {
                log.warn("Failover aborted, error={}", firstError);
                // best effort rollback: un-fence the slots this command fenced, binlog back on for demoted ones
                for (int i = 0; i < slots.size(); i++) {
                    var result = promises[i].getResult();
                    if (result.wasReadonly()) {
                        continue;
                    }

                    var oneSlot = slots.get(i);
                    oneSlot.asyncExecute(() -> {
                        try {
                            oneSlot.setReadonly(false);
                            if (result.error() == null) {
                                // this slot was demoted (its slaves were caught up), restore the binlog
                                oneSlot.getDynConfig().setBinlogOn(true);
                            }
                        } catch (IOException io) {
                            log.error("Failover rollback error, slot={}, error={}", oneSlot.slot(), io.getMessage());
                        }
                    });
                }

                finalPromise.set(new ErrorReply(firstError));
                return;
            }

            var leaderSelector = LeaderSelector.getInstance();
            leaderSelector.stopLeaderLatch();
            log.warn("Repl leader latch stopped");
            // later self will start leader latch again and make self as slave

            finalPromise.set(OKReply.INSTANCE);
        });

        return new AsyncReply(finalPromise);
    }

    private Reply flushdb() {
        // reject on a node-level role transition write fence, same as other write paths
        if (localPersist.isRoleTransitionWriteFenced()) {
            return ErrorReply.READONLY;
        }

        // reject on any readonly slot (e.g. replica), same as other write paths
        for (var oneSlot : localPersist.initializedOneSlots()) {
            if (oneSlot.isReadonly()) {
                return ErrorReply.READONLY;
            }
        }

        // FLUSHDB [ASYNC | SYNC] — both map to the same per-slot implementation; other tokens are syntax errors
        if (data.length == 2) {
            var flushType = new String(data[1]);
            if (!"async".equalsIgnoreCase(flushType) && !"sync".equalsIgnoreCase(flushType)) {
                return ErrorReply.SYNTAX;
            }
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
