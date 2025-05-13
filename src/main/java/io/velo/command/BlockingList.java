package io.velo.command;

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
     * @param isLeft          is left or right
     * @param createdTime     created time for timeout
     * @param xx              parameter when do move
     */
    record PromiseWithLeftOrRightAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                boolean isLeft,
                                                long createdTime,
                                                DstKeyAndDstLeftWhenMove xx) {
    }

    /**
     * All blocking list commands executing, store by promises
     */
    static final ConcurrentHashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new ConcurrentHashMap<>();

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

    @TestOnly
    static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
        return setReplyIfBlockingListExist(key, elementValueBytesArray, null);
    }

    /**
     * Set reply to blocking clients when list values are added
     *
     * @param key                    target key
     * @param elementValueBytesArray list values added
     * @param baseCommand            base command
     * @return list values exclude sent to blocking clients
     */
    public static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray, BaseCommand baseCommand) {
        var blockingListPromises = blockingListPromisesByKey.get(key);
        if (blockingListPromises == null || blockingListPromises.isEmpty()) {
            return null;
        }

        var it = blockingListPromises.iterator();
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
                    var dstOneSlot = LocalPersist.getInstance().oneSlot(dstSlot);

                    var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                    rGroup.from(baseCommand);
                    dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstKeyBytes, xx.dstSlotWithKeyHash, xx.dstLeft, leftValueBytes, promise.settablePromise::set));
                } else {
                    var replies = new Reply[2];
                    replies[0] = new BulkReply(key.getBytes());
                    replies[1] = new BulkReply(leftValueBytes);
                    promise.settablePromise.set(new MultiBulkReply(replies));
                }
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
                    var dstOneSlot = LocalPersist.getInstance().oneSlot(dstSlot);

                    var rGroup = new RGroup(null, baseCommand.getData(), baseCommand.getSocket());
                    rGroup.from(baseCommand);
                    dstOneSlot.asyncRun(() -> rGroup.moveDstCallback(xx.dstKeyBytes, xx.dstSlotWithKeyHash, xx.dstLeft, rightValueBytes, promise.settablePromise::set));
                } else {
                    var replies = new Reply[2];
                    replies[0] = new BulkReply(key.getBytes());
                    replies[1] = new BulkReply(rightValueBytes);
                    promise.settablePromise.set(new MultiBulkReply(replies));
                }
            }
        }

        var returnValueBytesArray = new byte[elementValueBytesArray.length - leftI - rightI][];
        if (returnValueBytesArray.length == 0) {
            return new byte[0][];
        }

        System.arraycopy(elementValueBytesArray, leftI, returnValueBytesArray, 0, returnValueBytesArray.length);
        return returnValueBytesArray;
    }

    @TestOnly
    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, boolean isLeft) {
        return addBlockingListPromiseByKey(key, promise, isLeft, null);
    }

    /**
     * Add blocking list promise by key
     *
     * @param key     target key
     * @param promise blocking promise
     * @param isLeft  is left or right
     * @param xx      parameter for move
     * @return promise with left or right and created time
     */
    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, boolean isLeft, DstKeyAndDstLeftWhenMove xx) {
        var one = new PromiseWithLeftOrRightAndCreatedTime(promise, isLeft, System.currentTimeMillis(), xx);
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
        var blockingListPromises = blockingListPromisesByKey.get(key);
        if (blockingListPromises != null) {
            blockingListPromises.remove(one);
        }
    }

    @TestOnly
    static void clearBlockingListPromisesForAllKeys() {
        blockingListPromisesByKey.clear();
    }
}
