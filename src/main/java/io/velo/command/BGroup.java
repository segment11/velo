package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
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
                "bitpos".equals(cmd)) {
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

        if ("bgsave".equals(cmd)) {
            // pure memory need to flush to disk, todo
            return OKReply.INSTANCE;
        }

        if ("blpop".equals(cmd)) {
            return blpop(true);
        }

        if ("brpop".equals(cmd)) {
            return blpop(false);
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

    public record PromiseWithLeftOrRightAndCreatedTime(SettablePromise<Reply> settablePromise,
                                                       boolean isLeft,
                                                       long createdTime) {
    }

    private static final ConcurrentHashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new ConcurrentHashMap<>();

    public static byte[][] setReplyIfBlockingListExist(String key, byte[][] elementValueBytesArray) {
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
                promise.settablePromise.set(new BulkReply(leftValueBytes));
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
                promise.settablePromise.set(new BulkReply(rightValueBytes));
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
    static void addBlockingListPromiseByKey(String key, SettablePromise<Reply> promise, boolean isLeft) {
        blockingListPromisesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(new PromiseWithLeftOrRightAndCreatedTime(promise, isLeft, System.currentTimeMillis()));
    }

    @TestOnly
    static void clearBlockingListPromisesForAllKeys() {
        blockingListPromisesByKey.clear();
    }

    @VisibleForTesting
    Reply blpop(boolean isLeft) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        for (int i = 1; i < data.length - 1; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
        }

        var timeoutBytes = data[data.length - 1];
        int timeoutSeconds;
        try {
            timeoutSeconds = Integer.parseInt(new String(timeoutBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        // max 1 hour check
        final int MAX_TIMEOUT_SECONDS = 3600;
        if (timeoutSeconds > MAX_TIMEOUT_SECONDS) {
            return new ErrorReply("timeout must be <= " + MAX_TIMEOUT_SECONDS);
        }

        boolean isNoWait = timeoutSeconds <= 0;

        // support first key in single thread
        var keyBytes = data[1];
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var rl = LGroup.getRedisList(keyBytes, slotWithKeyHash, this);
        if (rl == null || rl.size() == 0) {
            if (isNoWait) {
                return NilReply.INSTANCE;
            } else {
                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var key = new String(keyBytes);
                var one = new PromiseWithLeftOrRightAndCreatedTime(finalPromise, isLeft, System.currentTimeMillis());
                blockingListPromisesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(one);

                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutSeconds * 1000, () -> {
                    if (!finalPromise.isComplete()) {
                        finalPromise.set(NilReply.INSTANCE);
                        // remove form blocking list
                        blockingListPromisesByKey.get(key).remove(one);
                    }
                });

                return asyncReply;
            }
        }

        var valueBytes = rl.removeFirst();
        LGroup.saveRedisList(rl, keyBytes, slotWithKeyHash, this, dictMap);
        return new BulkReply(valueBytes);
    }
}
