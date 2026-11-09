package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.persist.KeyLoader;
import io.velo.persist.OneSlot;
import io.velo.persist.ScanCursor;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'K'.
 * This includes commands like KEYS.
 */
public class KGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "keys", 2,
                Set.of("readonly"),
                0, 0, 0,
                Set.of("@keyspace", "@read", "@slow", "@dangerous"),
                "generic",
                "Find all keys matching the given pattern.",
                "1.0.0", "O(N)"));
    }

    /**
     * Dyn config key capping the number of keys one KEYS call returns, see DynConfig.
     */
    @VisibleForTesting
    static final String MATCH_MAX_COUNT_KEY = "keys.matchMaxCount";

    @VisibleForTesting
    static final int DEFAULT_MATCH_MAX_COUNT = 100;

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public KGroup(String cmd, byte[][] data, ITcpSocket socket) {
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
        if (cmd.equals("keys")) {
            return keys();
        }

        return NilReply.INSTANCE;
    }

    private Reply keys() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        // an empty pattern can match no key, key names are never empty
        if (data[1].length == 0) {
            return MultiBulkReply.EMPTY;
        }

        var pattern = new String(data[1]);

        int maxCount = DEFAULT_MATCH_MAX_COUNT;
        var firstOneSlot = localPersist.firstOneSlot();
        if (firstOneSlot != null) {
            var maxCountValue = firstOneSlot.getDynConfig().get(MATCH_MAX_COUNT_KEY);
            if (maxCountValue instanceof Integer i) {
                maxCount = i;
            }
        }
        final int matchMaxCount = maxCount;

        // KEYS has no cursor and no resume, so unlike SCAN it takes no snapshot boundary:
        // every key visible in the wal or the key buckets at scan time is returned, including
        // keys written in the same millisecond as this call, see bug 80 re-review 3
        final long scanAllSeq = Long.MAX_VALUE;

        // iterate the real keyspace (wal first, then key buckets) on each slot worker thread
        var oneSlots = localPersist.initializedOneSlots();
        var promises = new ArrayList<Promise<ArrayList<String>>>(oneSlots.size());
        for (var oneSlot : oneSlots) {
            promises.add(oneSlot.asyncCall(() -> collectMatchedKeys(oneSlot, pattern, matchMaxCount, scanAllSeq)));
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.toList(promises).whenComplete((lists, e) -> {
            if (e != null) {
                log.error("keys error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            // round-robin across the per-slot lists so the cap does not starve
            // high-numbered slots, see bug 80 re-review 3 S1
            var keys = new ArrayList<String>();
            for (int index = 0; keys.size() < matchMaxCount; index++) {
                boolean added = false;
                for (var list : lists) {
                    if (index < list.size()) {
                        keys.add(list.get(index));
                        added = true;
                        if (keys.size() >= matchMaxCount) {
                            break;
                        }
                    }
                }
                if (!added) {
                    // every slot list is exhausted
                    break;
                }
            }

            var replies = new Reply[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                replies[i] = new BulkReply(keys.get(i));
            }
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }

    /**
     * Collects up to maxCount keys matching the glob pattern from one slot. Iterates the WAL
     * groups first and then the key buckets, the same iteration shape and glob matcher
     * (KeyLoader.isKeyMatch) as SGroup.scan. Must run on the slot worker thread, call via
     * oneSlot.asyncCall.
     *
     * @param oneSlot      the slot to iterate
     * @param pattern      the glob pattern
     * @param maxCount     the max number of matched keys to return
     * @param scanSeqLimit the seq limit for the scan filters, Long.MAX_VALUE means no limit
     * @return the matched keys, at most maxCount
     */
    private static ArrayList<String> collectMatchedKeys(OneSlot oneSlot, String pattern, int maxCount, long scanSeqLimit) {
        var keys = new ArrayList<String>();

        // scan wal first
        var scanCursor = new ScanCursor(oneSlot.slot(), 0, (short) 0, (short) 0, (byte) 0, (byte) 0);
        while (!scanCursor.isWalIterateEnd() && keys.size() < maxCount) {
            var wal = oneSlot.getWalByGroupIndex(scanCursor.walGroupIndex());
            var r = wal.scan(scanCursor.walSkipCount(), KeyLoader.typeAsByteIgnore, pattern, maxCount - keys.size(), scanSeqLimit);
            keys.addAll(r.keys());
            scanCursor = r.scanCursor();
        }

        // then scan key buckets
        var keyLoader = oneSlot.getKeyLoader();
        int walGroupIndex = 0;
        byte splitIndex = 0;
        byte bucketIndex = 0;
        short keyBucketsSkipCount = 0;
        while (keys.size() < maxCount) {
            var r = keyLoader.scan(walGroupIndex, splitIndex, bucketIndex, keyBucketsSkipCount,
                    KeyLoader.typeAsByteIgnore, pattern, maxCount - keys.size(), scanSeqLimit);
            keys.addAll(r.keys());

            var next = r.scanCursor();
            if (next == ScanCursor.END || next.slot() != oneSlot.slot()) {
                // end, or the cursor hands over to the next slot of a multi-slot scan chain
                break;
            }
            walGroupIndex = next.walGroupIndex();
            splitIndex = next.splitIndex();
            bucketIndex = next.bucketIndex();
            keyBucketsSkipCount = next.keyBucketsSkipCount();
        }

        return keys;
    }
}
