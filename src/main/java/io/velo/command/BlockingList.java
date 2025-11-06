package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.persist.LocalPersist;
import io.velo.reply.BulkReply;
import io.velo.reply.MultiBulkReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
     * @param dstKeyBytes        destination key
     * @param dstSlotWithKeyHash destination key slot and hash value
     * @param dstLeft            destination list, is left or right
     */
    record DstKeyAndDstLeftWhenMove(byte[] dstKeyBytes, BaseCommand.SlotWithKeyHash dstSlotWithKeyHash,
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

    // All blocking list commands executing, store by promises
    private static final ConcurrentHashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new ConcurrentHashMap<>();

    /**
     * Add one blocking list promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void addOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
        blockingListPromisesByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(one);
    }

    /**
     * Remove one blocking list promise
     *
     * @param key target blocking key
     * @param one the promise wrap
     */
    public static void removeOne(String key, PromiseWithLeftOrRightAndCreatedTime one) {
        blockingListPromisesByKey.computeIfPresent(key, (k, v) -> {
            v.remove(one);
            return v.isEmpty() ? null : v;
        });
    }

    /**
     * blocking client count
     *
     * @return blocking client count
     */
    public static int blockingClientCount() {
        var total = 0;
        for (var one : blockingListPromisesByKey.values()) {
            total += one.size();
        }
        return total;
    }

    /**
     * blocking key count
     *
     * @return blocking key count
     */
    public static int blockingKeyCount() {
        return blockingListPromisesByKey.size();
    }

    @TestOnly
    static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
        return setReplyIfBlockingListExist(key, elementValueBytesArray, null);
    }

    private static final LocalPersist localPersist = LocalPersist.getInstance();

    private static class BytesArrayWrap {
        byte[][] bytesArray;
    }

    /**
     * Set reply to blocking clients when list values are added
     *
     * @param key                    target key
     * @param elementValueBytesArray list values added
     * @param baseCommand            base command
     * @return list values exclude sent to blocking clients
     */
    public synchronized static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray, BaseCommand baseCommand) {
        final var rr = new BytesArrayWrap();
        blockingListPromisesByKey.computeIfPresent(key, (k, list) -> {
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
                    if (leftI >= elementValueBytesArray.length) {
                        break;
                    }

                    var leftValueBytes = elementValueBytesArray[leftI];
                    // already reset by other promise
                    if (leftValueBytes == null) {
                        break;
                    }

                    elementValueBytesArray[leftI] = null;
                    leftI++;

                    var xx = promise.xx();
                    if (xx != null) {
                        // do move
                        var dstSlot = xx.dstSlotWithKeyHash.slot();
                        var dstOneSlot = localPersist.oneSlot(dstSlot);

                        var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                        rGroup.from(baseCommand);
                        dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstKeyBytes, xx.dstSlotWithKeyHash, xx.dstLeft, leftValueBytes, promise.settablePromise::set));
                    } else {
                        var replies = new Reply[2];
                        replies[0] = new BulkReply(key.getBytes());
                        replies[1] = new BulkReply(leftValueBytes);
                        promise.settablePromise.set(new MultiBulkReply(replies));
                    }
                    it.remove();
                } else {
                    var index = elementValueBytesArray.length - 1 - rightI;
                    if (index < 0) {
                        break;
                    }

                    var rightValueBytes = elementValueBytesArray[index];
                    // already reset by other promise
                    if (rightValueBytes == null) {
                        break;
                    }

                    elementValueBytesArray[index] = null;
                    rightI++;

                    var xx = promise.xx();
                    if (xx != null) {
                        // do move
                        var dstSlot = xx.dstSlotWithKeyHash.slot();
                        var dstOneSlot = localPersist.oneSlot(dstSlot);

                        var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                        rGroup.from(baseCommand);
                        dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstKeyBytes, xx.dstSlotWithKeyHash, xx.dstLeft, rightValueBytes, promise.settablePromise::set));
                    } else {
                        var replies = new Reply[2];
                        replies[0] = new BulkReply(key.getBytes());
                        replies[1] = new BulkReply(rightValueBytes);
                        promise.settablePromise.set(new MultiBulkReply(replies));
                    }
                    it.remove();
                }
            }

            rr.bytesArray = new byte[elementValueBytesArray.length - leftI - rightI][];
            System.arraycopy(elementValueBytesArray, leftI, rr.bytesArray, 0, rr.bytesArray.length);

            return list.isEmpty() ? null : list;
        });

        if (rr.bytesArray == null || rr.bytesArray.length == 0) {
            return new byte[0][];
        }
        return rr.bytesArray;
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
        blockingListPromisesByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(one);
        return one;
    }

    /**
     * Remove blocking list promise by key when timeout
     *
     * @param key target key
     * @param one promise with left or right and created time
     */
    static void removeBlockingListPromiseByKey(String key, PromiseWithLeftOrRightAndCreatedTime one) {
        blockingListPromisesByKey.computeIfPresent(key, (k, list) -> {
            list.remove(one);
            return list.isEmpty() ? null : list;
        });
    }

    /**
     * Remove blocking list promise by socket
     *
     * @param socket the client socket
     */
    public static void removeBySocket(ITcpSocket socket) {
        blockingListPromisesByKey.values().forEach(list -> list.removeIf(one -> one.socket == socket));
    }

    @TestOnly
    static void clearBlockingListPromisesForAllKeys() {
        blockingListPromisesByKey.clear();
    }
}
