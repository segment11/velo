package io.velo.command;

import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.ThreadNeedLocal;
import io.velo.persist.LocalPersist;
import io.velo.reply.BulkReply;
import io.velo.reply.MultiBulkReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * For blocking list pop, move
 */
public class BlockingList {
    // all is static, need thread safe
    private BlockingList() {
    }

    /**
     * Parameter when do move
     *
     * @param dstSlotWithKeyHash destination key slot and hash value
     * @param dstLeft            destination list, is left or right
     */
    record DstKeyAndDstLeftWhenMove(BaseCommand.SlotWithKeyHash dstSlotWithKeyHash,
                                    boolean dstLeft) {
    }

    /**
     * Promise with left or right and created time
     *
     * @param settablePromise promise
     * @param socket          the client socket
     * @param isLeft          is left or right
     * @param createdTime     created time for timeout
     * @param xx              parameter when do move
     */
    public record PromiseWithLeftOrRightAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                       ITcpSocket socket,
                                                       boolean isLeft,
                                                       long createdTime,
                                                       DstKeyAndDstLeftWhenMove xx) {
    }

    private static final Logger log = LoggerFactory.getLogger(BlockingList.class);

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

    public static void initBySlotWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray) {
        BlockingList.slotWorkerEventloopArray = slotWorkerEventloopArray;

        inners = new Inner[slotWorkerEventloopArray.length];
        for (int i = 0; i < slotWorkerEventloopArray.length; i++) {
            var eventloop = slotWorkerEventloopArray[i];
            var eventloopThread = eventloop.getEventloopThread();
            inners[i] = new Inner(eventloopThread != null ? eventloopThread.threadId() : Thread.currentThread().threadId());
        }

        log.info("Blocking list init by slot worker eventloop array");
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

    @ThreadNeedLocal
    private static class Inner {
        public Inner(long expectThreadId) {
            this.expectThreadId = expectThreadId;
        }

        final long expectThreadId;

        final HashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new HashMap<>();

        void addOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
            blockingListPromisesByKey.computeIfAbsent(key, k -> new LinkedList<>()).add(one);
        }

        void removeOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
            var list = blockingListPromisesByKey.get(key);
            if (list == null) {
                return;
            }
            list.remove(one);
        }
    }

    /**
     * Add one blocking list promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void addOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
        getInner().addOne(key, one);
    }

    /**
     * Remove one blocking list promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void removeOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
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
            for (var one : inner.blockingListPromisesByKey.values()) {
                total += one.size();
            }
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
            total += inner.blockingListPromisesByKey.size();
        }
        return total;
    }

    @TestOnly
    static byte[][] setReplyLPushIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
        return setReplyIfBlockingListExist(key, true, elementValueBytesArray, null);
    }

    @TestOnly
    static byte[][] setReplyRPushIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
        return setReplyIfBlockingListExist(key, false, elementValueBytesArray, null);
    }

    private static final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Set reply to blocking clients when list values are added
     *
     * @param key                    target key
     * @param addFirst               is added from left
     * @param elementValueBytesArray list values added
     * @param baseCommand            base command
     * @return list values exclude sent to blocking clients, null if key is not blocking
     */
    public static byte[][] setReplyIfBlockingListExist(String key, boolean addFirst, byte[][] elementValueBytesArray, BaseCommand baseCommand) {
        var list = getInner().blockingListPromisesByKey.get(key);
        if (list == null || list.isEmpty()) {
            return null;
        }

        byte[][] fromLeftToRight;
        if (addFirst) {
            fromLeftToRight = new byte[elementValueBytesArray.length][];
            for (int i = 0; i < elementValueBytesArray.length; i++) {
                fromLeftToRight[i] = elementValueBytesArray[elementValueBytesArray.length - i - 1];
            }
        } else {
            fromLeftToRight = elementValueBytesArray;
        }

        var it = list.iterator();
        int leftI = 0;
        int rightI = 0;
        while (it.hasNext()) {
            var promise = it.next();
            if (promise.settablePromise.isComplete()) {
                it.remove();
                continue;
            }

            if (promise.isLeft) {
                if (leftI >= fromLeftToRight.length) {
                    break;
                }

                var leftValueBytes = fromLeftToRight[leftI];
                // already reset by other promise
                if (leftValueBytes == null) {
                    break;
                }

                fromLeftToRight[leftI] = null;
                leftI++;

                var xx = promise.xx();
                if (xx != null) {
                    // do move
                    var dstSlot = xx.dstSlotWithKeyHash.slot();
                    var dstOneSlot = localPersist.oneSlot(dstSlot);

                    var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                    rGroup.from(baseCommand);
                    dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstSlotWithKeyHash, xx.dstLeft, leftValueBytes, promise.settablePromise::set));
                } else {
                    var replies = new Reply[2];
                    replies[0] = new BulkReply(key);
                    replies[1] = new BulkReply(leftValueBytes);
                    promise.settablePromise.set(new MultiBulkReply(replies));
                }
                it.remove();
            } else {
                var index = fromLeftToRight.length - 1 - rightI;
                if (index < 0) {
                    break;
                }

                var rightValueBytes = fromLeftToRight[index];
                // already reset by other promise
                if (rightValueBytes == null) {
                    break;
                }

                fromLeftToRight[index] = null;
                rightI++;

                var xx = promise.xx();
                if (xx != null) {
                    // do move
                    var dstSlot = xx.dstSlotWithKeyHash.slot();
                    var dstOneSlot = localPersist.oneSlot(dstSlot);

                    var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                    rGroup.from(baseCommand);
                    dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstSlotWithKeyHash, xx.dstLeft, rightValueBytes, promise.settablePromise::set));
                } else {
                    var replies = new Reply[2];
                    replies[0] = new BulkReply(key);
                    replies[1] = new BulkReply(rightValueBytes);
                    promise.settablePromise.set(new MultiBulkReply(replies));
                }
                it.remove();
            }
        }

        int returnLength = fromLeftToRight.length - leftI - rightI;
        if (returnLength == 0) {
            return new byte[0][];
        }

        var returnBytesArray = new byte[returnLength][];
        System.arraycopy(fromLeftToRight, leftI, returnBytesArray, 0, returnBytesArray.length);

        if (addFirst) {
            // sort back
            var fromRightToLeft = new byte[returnBytesArray.length][];
            for (int i = 0; i < fromRightToLeft.length; i++) {
                fromRightToLeft[i] = fromLeftToRight[returnBytesArray.length - i - 1];
            }
            return fromRightToLeft;
        } else {
            return returnBytesArray;
        }
    }

    @TestOnly
    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, ITcpSocket socket, boolean isLeft) {
        return addBlockingListPromiseByKey(key, promise, socket, isLeft, null);
    }

    /**
     * Add blocking list promise by key
     *
     * @param key     target key
     * @param promise blocking promise
     * @param socket  the client socket
     * @param isLeft  is left or right
     * @param xx      parameter for move
     * @return promise with left or right and created time
     */
    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, ITcpSocket socket,
                                                                            boolean isLeft, DstKeyAndDstLeftWhenMove xx) {
        var one = new PromiseWithLeftOrRightAndCreatedTime(promise, socket, isLeft, System.currentTimeMillis(), xx);
        getInner().blockingListPromisesByKey.computeIfAbsent(key, k -> new LinkedList<>()).add(one);
        return one;
    }

    /**
     * Remove blocking list promise by key when timeout
     *
     * @param key target key
     * @param one promise with left or right and created time
     */
    static void removeBlockingListPromiseByKey(String key, PromiseWithLeftOrRightAndCreatedTime one) {
        var list = getInner().blockingListPromisesByKey.get(key);
        if (list == null) {
            return;
        }
        list.remove(one);
    }

    /**
     * Remove blocking list promise by socket
     *
     * @param socket the client socket
     */
    public static void removeBySocket(ITcpSocket socket) {
        for (var eventloop : slotWorkerEventloopArray) {
            eventloop.execute(() -> {
                var inner = getInner();
                for (var list : inner.blockingListPromisesByKey.values()) {
                    list.removeIf(one -> one.socket == socket);
                }
            });
        }
    }

    @TestOnly
    static void clearBlockingListPromisesForAllKeys() {
        for (var inner : inners) {
            inner.blockingListPromisesByKey.clear();
        }
    }
}
