package io.velo.command;

import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.DictMap;
import io.velo.ThreadNeedLocal;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * For blocking sorted set pop: BZPOPMIN / BZPOPMAX / BZMPOP.
 * <p>
 * Mirrors {@link BlockingList}: waiters are registered on the worker thread owning the key, so a
 * member added to a previously empty sorted set can wake exactly one waiter per member batch.
 */
public class BlockingZSet {
    // all is static, need thread safe
    private BlockingZSet() {
    }

    /**
     * Promise with min or max and created time, for blocking sorted set commands.
     * <p>
     * A class rather than a record: it carries a one-shot delivery arbiter ({@link #tryMarkDelivered()}),
     * so exactly one completer may consume members and complete the promise even when several workers
     * race. Waiters are removed by identity, which is safe because every waiter holds a
     * distinct promise.
     * <p>
     * Instances are immutable except for the delivery arbiter.
     */
    public static final class PromiseWithMinOrMaxAndCreatedTime {
        private final SettablePromise<Reply> settablePromise;
        private final ITcpSocket socket;
        private final boolean isMin;
        private final long createdTime;
        private final int mpopCount;
        private final List<BaseCommand.SlotWithKeyHash> registeredSlots;

        private final AtomicBoolean delivered = new AtomicBoolean();

        /**
         * @param settablePromise promise
         * @param socket          the client socket
         * @param isMin           pop the lowest scored member when true, the highest one otherwise
         * @param createdTime     created time for timeout
         * @param mpopCount       number of members to pop for BZMPOP, 0 means BZPOPMIN/BZPOPMAX (single triple reply)
         */
        public PromiseWithMinOrMaxAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                 ITcpSocket socket,
                                                 boolean isMin,
                                                 long createdTime,
                                                 int mpopCount) {
            this(settablePromise, socket, isMin, createdTime, mpopCount, List.of());
        }

        /**
         * @param settablePromise  promise
         * @param socket           the client socket
         * @param isMin            pop the lowest scored member when true, the highest one otherwise
         * @param createdTime      created time for timeout
         * @param mpopCount        number of members to pop for BZMPOP, 0 means BZPOPMIN/BZPOPMAX (single triple reply)
         * @param registeredSlots  the keys this waiter is registered on, a multi key block registers the
         *                          same waiter on every key and serving one key must release all of them
         */
        public PromiseWithMinOrMaxAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                 ITcpSocket socket,
                                                 boolean isMin,
                                                 long createdTime,
                                                 int mpopCount,
                                                 List<BaseCommand.SlotWithKeyHash> registeredSlots) {
            this.settablePromise = settablePromise;
            this.socket = socket;
            this.isMin = isMin;
            this.createdTime = createdTime;
            this.mpopCount = mpopCount;
            this.registeredSlots = List.copyOf(registeredSlots);
        }

        /**
         * @return the keys this waiter is registered on
         */
        public List<BaseCommand.SlotWithKeyHash> registeredSlots() {
            return registeredSlots;
        }

        /**
         * @return the promise to complete when members are delivered
         */
        public SettablePromise<Reply> settablePromise() {
            return settablePromise;
        }

        /**
         * @return the client socket
         */
        public ITcpSocket socket() {
            return socket;
        }

        /**
         * @return true when members are taken from the low end of the sorted set
         */
        public boolean isMin() {
            return isMin;
        }

        /**
         * @return created time in milliseconds, used for timeout
         */
        public long createdTime() {
            return createdTime;
        }

        /**
         * @return number of members to pop for BZMPOP, 0 means BZPOPMIN/BZPOPMAX (single triple reply)
         */
        public int mpopCount() {
            return mpopCount;
        }

        /**
         * One-shot delivery arbiter: the single compare-and-set winner is the only completer allowed to
         * consume members and complete the promise. ActiveJ promises are not thread-safe and its
         * {@code trySet} is check-then-act, so the atomic arbiter runs before any value is touched.
         *
         * @return true for the first caller, false for every later one
         */
        public boolean tryMarkDelivered() {
            return delivered.compareAndSet(false, true);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BlockingZSet.class);

    /**
     * Array of event loops, each associated with a network worker thread.
     */
    @ThreadNeedLocal
    private static Eventloop[] slotWorkerEventloopArray;

    /**
     * Array of inner instances, each corresponding to a specific thread.
     */
    @ThreadNeedLocal
    private static Inner[] inners;

    static {
        inners = new Inner[1];
        inners[0] = new Inner(Thread.currentThread().threadId());
    }

    /**
     * Initializes the slot worker event loop array and creates one {@link Inner} instance per worker thread.
     *
     * @param slotWorkerEventloopArray the event loop array, one entry per slot worker thread
     */
    public static void initBySlotWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray) {
        BlockingZSet.slotWorkerEventloopArray = slotWorkerEventloopArray;

        inners = new Inner[slotWorkerEventloopArray.length];
        for (int i = 0; i < slotWorkerEventloopArray.length; i++) {
            var eventloop = slotWorkerEventloopArray[i];
            var eventloopThread = eventloop.getEventloopThread();
            inners[i] = new Inner(eventloopThread != null ? eventloopThread.threadId() : Thread.currentThread().threadId());
        }

        log.info("Blocking zset init by slot worker eventloop array");
    }

    /**
     * Returns the Inner instance associated with the current thread.
     *
     * @return the Inner instance, or null if no matching instance is found
     */
    private static Inner getInner() {
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }

        throw new IllegalStateException("No inner instance found for thread ID: " + currentThreadId);
    }

    /**
     * Returns the Inner instance associated with the current thread, or null when the thread is not a
     * registered slot worker. An unregistered thread can not hold waiters, so a notification from it
     * has nothing to do.
     *
     * @return the Inner instance, or null if no matching instance is found
     */
    private static Inner getInnerOrNull() {
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }

        return null;
    }

    @ThreadNeedLocal
    private static class Inner {
        public Inner(long expectThreadId) {
            this.expectThreadId = expectThreadId;
        }

        final long expectThreadId;

        final HashMap<String, List<PromiseWithMinOrMaxAndCreatedTime>> blockingZSetPromisesByKey = new HashMap<>();
        // only the worker thread owning this Inner ever mutates them, plain int is enough
        // they are diagnostics only (INFO and the prometheus gauge), a stale read is acceptable
        private int clientCount;
        private int keyCount;

        void addOne(String key, PromiseWithMinOrMaxAndCreatedTime one) {
            var list = blockingZSetPromisesByKey.computeIfAbsent(key, k -> new LinkedList<>());
            if (list.isEmpty()) {
                keyCount++;
            }
            list.add(one);
            clientCount++;
        }

        void removeOne(String key, PromiseWithMinOrMaxAndCreatedTime one) {
            var list = blockingZSetPromisesByKey.get(key);
            if (list == null) {
                return;
            }
            if (list.remove(one)) {
                clientCount--;
                if (list.isEmpty()) {
                    keyCount--;
                    // the key has no waiter left, do not keep an empty entry
                    blockingZSetPromisesByKey.remove(key);
                }
            }
        }
    }

    /**
     * Add one blocking sorted set promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void addOne(String key, PromiseWithMinOrMaxAndCreatedTime one) {
        getInner().addOne(key, one);
    }

    /**
     * Remove one blocking sorted set promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void removeOne(String key, PromiseWithMinOrMaxAndCreatedTime one) {
        getInner().removeOne(key, one);
    }

    /**
     * blocking client count
     *
     * @return blocking client count
     */
    public static int blockingClientCount() {
        var total = 0;
        for (var inner : inners) {
            total += inner.clientCount;
        }
        return total;
    }

    /**
     * blocking key count
     *
     * @return blocking key count
     */
    public static int blockingKeyCount() {
        var total = 0;
        for (var inner : inners) {
            total += inner.keyCount;
        }
        return total;
    }

    private static final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Wake the waiters of a key after a sorted set was saved with at least one member. Every writer
     * path goes through {@link ZGroup#saveRedisZSet}, so ZADD / ZINCRBY / the store variants all
     * notify. The completion runs on the worker owning the key and re-reads the persisted sorted set,
     * a completion pops members and saves again which re-enters this method, hence the snapshot before
     * scheduling and the one-shot delivery arbiter inside the completion.
     *
     * @param slotWithKeyHash slot and key hash of the saved key
     * @param baseCommand     the command owning the storage access
     * @param dictMap         dictionary map used when the popped sorted set is saved back
     */
    public static void onZSetSaved(BaseCommand.SlotWithKeyHash slotWithKeyHash,
                                   BaseCommand baseCommand, DictMap dictMap) {
        var key = slotWithKeyHash.rawKey();
        var inner = getInnerOrNull();
        if (inner == null) {
            // an unregistered thread can not hold waiters, nothing to notify
            return;
        }
        var list = inner.blockingZSetPromisesByKey.get(key);
        if (list == null || list.isEmpty()) {
            return;
        }

        var keyBytes = Wal.keyBytes(key);
        var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());
        // snapshot: an inline completion pops and saves again, re-entering this method and mutating the list
        var snapshot = list.toArray(new PromiseWithMinOrMaxAndCreatedTime[0]);
        for (var one : snapshot) {
            if (one.settablePromise().isComplete()) {
                // already completed elsewhere, timeout or a foreign setter, drop it as stale
                inner.removeOne(key, one);
                continue;
            }
            oneSlot.asyncExecute(() -> completeIfZSetNotEmpty(slotWithKeyHash, keyBytes, baseCommand, dictMap, one));
        }
    }

    /**
     * Complete a pending blocking reply when the sorted set is not empty, used to close the window
     * between the emptiness check and the waiter registration and to wake waiters after a save.
     *
     * @param slotWithKeyHash slot and key hash of the blocking key
     * @param keyBytes        the blocking key
     * @param baseCommand     the command owning the storage access
     * @param dictMap         dictionary map used when the sorted set is saved back
     * @param one             the promise wrapper
     * @return true when the promise was completed
     */
    static boolean completeIfZSetNotEmpty(BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                          BaseCommand baseCommand, DictMap dictMap,
                                          PromiseWithMinOrMaxAndCreatedTime one) {
        return BGroup.completeIfZSetNotEmpty(slotWithKeyHash, keyBytes, baseCommand, dictMap, one);
    }

    /**
     * Removes the waiter from every key it was registered on. A multi key block registers the same
     * waiter on every key, serving one of them must release all of them, else the waiter stays in
     * the other keys and the blocked client count keeps growing.
     *
     * @param one the promise wrap
     */
    public static void removeEverywhere(PromiseWithMinOrMaxAndCreatedTime one) {
        for (var slotWithKeyHash : one.registeredSlots()) {
            var slot = slotWithKeyHash.slot();
            if (!localPersist.isOneSlotInitialized(slot)) {
                continue;
            }
            localPersist.oneSlot(slot).asyncExecute(() -> {
                var inner = getInnerOrNull();
                if (inner != null) {
                    inner.removeOne(slotWithKeyHash.rawKey(), one);
                }
            });
        }
    }

    /**
     * Remove blocking sorted set promise by socket
     *
     * @param socket the client socket
     */
    public static void removeBySocket(ITcpSocket socket) {
        for (var eventloop : slotWorkerEventloopArray) {
            eventloop.execute(() -> {
                var inner = getInner();
                int totalRemoved = 0;
                var iter = inner.blockingZSetPromisesByKey.values().iterator();
                while (iter.hasNext()) {
                    var list = iter.next();
                    int before = list.size();
                    list.removeIf(one -> one.socket == socket);
                    totalRemoved += before - list.size();
                    if (list.isEmpty()) {
                        iter.remove();
                    }
                }
                if (totalRemoved > 0) {
                    inner.clientCount -= totalRemoved;
                    inner.keyCount = inner.blockingZSetPromisesByKey.size();
                }
            });
        }
    }

    @TestOnly
    static void clearBlockingZSetPromisesForAllKeys() {
        for (var inner : inners) {
            inner.blockingZSetPromisesByKey.clear();
            inner.clientCount = 0;
            inner.keyCount = 0;
        }
    }
}
