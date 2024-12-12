package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.persist.LocalPersist;
import io.velo.reply.*;
import io.velo.type.RedisBF;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BGroup extends BaseCommand {
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        if ("bitcount".equals(cmd) || "bitfield".equals(cmd) || "bitfield_ro".equals(cmd) ||
                "bitpos".equals(cmd) || cmd.startsWith("bf.")) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("bitop".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }

            // begin with dst key
            // eg. bitop and dest src1 src2
            for (int i = 2; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("blmove".equals(cmd) || "brpoplpush".equals(cmd)) {
            if ("blmove".equals(cmd) && data.length != 6) {
                return slotWithKeyHashList;
            }
            if ("brpoplpush".equals(cmd) && data.length != 4) {
                return slotWithKeyHashList;
            }

            var srcKeyBytes = data[1];
            var dstKeyBytes = data[2];
            var s1 = slot(srcKeyBytes, slotNumber);
            var s2 = slot(dstKeyBytes, slotNumber);
            slotWithKeyHashList.add(s1);
            slotWithKeyHashList.add(s2);
            return slotWithKeyHashList;
        }

        if ("blpop".equals(cmd) || "brpop".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }

            // eg. blpop key1 key2 timeout
            for (int i = 1; i < data.length - 1; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("bitcount".equals(cmd)) {
            return bitcount();
        }

        if ("bitpos".equals(cmd)) {
            return bitpos();
        }

        if (cmd.startsWith("bf.")) {
            return bf();
        }

        if ("bgsave".equals(cmd)) {
            // pure memory need to flush to disk, todo
            return OKReply.INSTANCE;
        }

        if ("blmove".equals(cmd)) {
            var lGroup = new LGroup(null, data, socket);
            lGroup.from(this);
            return lGroup.lmove(true);
        }

        if ("blpop".equals(cmd)) {
            return blpop(true);
        }

        if ("brpop".equals(cmd)) {
            return blpop(false);
        }

        if ("brpoplpush".equals(cmd)) {
            var dd = new byte[6][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = "right".getBytes();
            dd[4] = "left".getBytes();
            // timeout
            dd[5] = data[3];

            var lGroup = new LGroup(null, dd, socket);
            lGroup.from(this);
            return lGroup.lmove(true);
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply bitcount() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int end;
        boolean isIndexUseBit;
        if (data.length == 2) {
            start = 0;
            end = -1;
            isIndexUseBit = false;
        } else {
            if (data.length != 4 && data.length != 5) {
                return ErrorReply.SYNTAX;
            }

            try {
                start = Integer.parseInt(new String(data[2]));
                end = Integer.parseInt(new String(data[3]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            isIndexUseBit = data.length == 5 && "bit".equalsIgnoreCase(new String(data[4]));
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
        var canIndexValueBytesLength = isIndexUseBit ? valueBytes.length * 8 : valueBytes.length;

        var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
        if (!startEndWith.valid()) {
            return IntegerReply.REPLY_0;
        }

        var bitset = BitSet.valueOf(valueBytes);
        int count = 0;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if (bitset.get(i)) {
                    count++;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if (bitset.get(i)) {
                    count++;
                }
            }
        }

        return new IntegerReply(count);
    }

    @VisibleForTesting
    Reply bitpos() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var bit1or0Bytes = data[2];
        if (bit1or0Bytes.length != 1) {
            return ErrorReply.INVALID_INTEGER;
        }
        var isBit1 = bit1or0Bytes[0] == '1';
        if (!isBit1 && bit1or0Bytes[0] != '0') {
            return ErrorReply.INVALID_INTEGER;
        }

        int start;
        int end;
        boolean isIndexUseBit;
        if (data.length == 3) {
            start = 0;
            end = -1;
            isIndexUseBit = false;
        } else {
            if (data.length != 4 && data.length != 5 && data.length != 6) {
                return ErrorReply.SYNTAX;
            }

            try {
                start = Integer.parseInt(new String(data[3]));
                end = data.length >= 5 ? Integer.parseInt(new String(data[4])) : -1;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            isIndexUseBit = data.length == 6 && "bit".equalsIgnoreCase(new String(data[5]));
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return new IntegerReply(-1);
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
        var canIndexValueBytesLength = isIndexUseBit ? valueBytes.length * 8 : valueBytes.length;

        var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
        if (!startEndWith.valid()) {
            return new IntegerReply(-1);
        }

        var bitset = BitSet.valueOf(valueBytes);
        int pos = -1;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if ((bitset.get(i) && isBit1) || (!bitset.get(i) && !isBit1)) {
                    pos = i;
                    break;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if ((bitset.get(i) && isBit1) || (!bitset.get(i) && !isBit1)) {
                    pos = i;
                    break;
                }
            }
        }

        return new IntegerReply(pos);
    }

    @VisibleForTesting
    Reply bf() {
        // bf.***
        var bfCmdSuffix = cmd.substring(3);
        if ("add".equals(bfCmdSuffix)) {
            return bfAdd();
        }

        return ErrorReply.SYNTAX;
    }

    private Reply bfAdd() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var itemBytes = data[2];
        var item = new String(itemBytes);

        var s = slotWithKeyHashListParsed.getFirst();

        RedisBF redisBF;

        var cv = getCv(keyBytes, s);
        if (cv == null) {
            redisBF = new RedisBF(true);
        } else {
            if (!cv.isBloomFilter()) {
                return ErrorReply.WRONG_TYPE;
            }

            redisBF = RedisBF.decode(cv.getCompressedData());
        }

        var isPut = redisBF.put(item);
        if (isPut) {
            var encoded = redisBF.encode();
            set(keyBytes, encoded, s, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        }

        return isPut ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    public record DstKeyAndDstLeftWhenMove(byte[] dstKeyBytes, SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft) {
    }

    public record PromiseWithLeftOrRightAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                       boolean isLeft,
                                                       long createdTime,
                                                       DstKeyAndDstLeftWhenMove xx) {
    }

    private static final ConcurrentHashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new ConcurrentHashMap<>();


    public static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
        return setReplyIfBlockingListExist(key, elementValueBytesArray, null);
    }

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

    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, boolean isLeft) {
        return addBlockingListPromiseByKey(key, promise, isLeft, null);
    }

    static PromiseWithLeftOrRightAndCreatedTime addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, boolean isLeft, DstKeyAndDstLeftWhenMove xx) {
        var one = new PromiseWithLeftOrRightAndCreatedTime(promise, isLeft, System.currentTimeMillis(), xx);
        blockingListPromisesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(one);
        return one;
    }

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

    // max 1 hour check
    static final int MAX_TIMEOUT_SECONDS = 3600;

    @VisibleForTesting
    Reply blpop(boolean isLeft) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 1; i < data.length - 1; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            keys.add(new String(keyBytes));
        }

        var timeoutBytes = data[data.length - 1];
        int timeoutSeconds;
        try {
            timeoutSeconds = Integer.parseInt(new String(timeoutBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (timeoutSeconds > MAX_TIMEOUT_SECONDS) {
            return new ErrorReply("timeout must be <= " + MAX_TIMEOUT_SECONDS);
        }

        boolean isNoWait = timeoutSeconds <= 0;

        var firstKeyBytes = data[1];
        var firstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var rl = LGroup.getRedisList(firstKeyBytes, firstSlotWithKeyHash, this);
        if (rl == null || rl.size() == 0) {
            // performance bad, get all other values
            if (keys.size() > 1) {
                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                boolean isSet = false;
                for (int i = 1; i < keys.size(); i++) {
                    var otherKey = keys.get(i);
                    var otherKeyBytes = otherKey.getBytes();
                    var otherSlotWithKeyHash = slotWithKeyHashListParsed.get(i);

                    var oneSlot = localPersist.oneSlot(otherSlotWithKeyHash.slot());
                    var p = oneSlot.asyncCall(() -> {
                        var otherRl = LGroup.getRedisList(otherKeyBytes, otherSlotWithKeyHash, this);
                        if (otherRl == null || otherRl.size() == 0) {
                            return null;
                        }

                        var otherValueBytes = isLeft ? otherRl.removeFirst() : otherRl.removeLast();
                        LGroup.saveRedisList(otherRl, otherKeyBytes, otherSlotWithKeyHash, this, dictMap);
                        return otherValueBytes;
                    });

                    // wait until this promise complete
                    var otherValueBytes = p.whenComplete((r, e) -> {
                        if (e != null) {
                            log.error("{} error={}", isLeft ? "blpop" : "brpop", e.getMessage());
                            finalPromise.setException(e);
                            return;
                        }

                        if (r != null) {
                            var replies = new Reply[2];
                            replies[0] = new BulkReply(otherKeyBytes);
                            replies[1] = new BulkReply(r);
                            finalPromise.set(new MultiBulkReply(replies));
                        }
                    }).getResult();

                    if (otherValueBytes != null) {
                        isSet = true;
                        break;
                    }
                }

                if (!isSet) {
                    if (isNoWait) {
                        finalPromise.set(NilReply.INSTANCE);
                    } else {
                        var one = new PromiseWithLeftOrRightAndCreatedTime(finalPromise, isLeft, System.currentTimeMillis(), null);
                        for (var key : keys) {
                            blockingListPromisesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(one);
                        }

                        var reactor = Reactor.getCurrentReactor();
                        reactor.delay(timeoutSeconds * 1000, () -> {
                            if (!finalPromise.isComplete()) {
                                finalPromise.set(NilReply.INSTANCE);
                                // remove form blocking list
                                for (var key : keys) {
                                    blockingListPromisesByKey.get(key).remove(one);
                                }
                            }
                        });
                    }
                }

                return asyncReply;
            }

            // only one key
            if (isNoWait) {
                return NilReply.INSTANCE;
            } else {
                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var firstKey = keys.getFirst();
                var one = new PromiseWithLeftOrRightAndCreatedTime(finalPromise, isLeft, System.currentTimeMillis(), null);
                blockingListPromisesByKey.computeIfAbsent(firstKey, k -> new ArrayList<>()).add(one);

                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutSeconds * 1000, () -> {
                    if (!finalPromise.isComplete()) {
                        finalPromise.set(NilReply.INSTANCE);
                        // remove form blocking list
                        blockingListPromisesByKey.get(firstKey).remove(one);
                    }
                });

                return asyncReply;
            }
        }

        var valueBytes = isLeft ? rl.removeFirst() : rl.removeLast();
        LGroup.saveRedisList(rl, firstKeyBytes, firstSlotWithKeyHash, this, dictMap);

        var replies = new Reply[2];
        replies[0] = new BulkReply(firstKeyBytes);
        replies[1] = new BulkReply(valueBytes);
        return new MultiBulkReply(replies);
    }
}
