package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.*;
import io.velo.reply.*;
import io.velo.type.RedisList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static io.velo.command.BGroup.MAX_TIMEOUT_SECONDS;

public class LGroup extends BaseCommand {
    public LGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("lmove".equals(cmd)) {
            if (data.length != 5 && data.length != 6) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            slotWithKeyHashList.add(slot(data[2], slotNumber));
            return slotWithKeyHashList;
        }

        if ("lindex".equals(cmd) || "linsert".equals(cmd)
                || "llen".equals(cmd) || "lpop".equals(cmd) || "lpos".equals(cmd)
                || "lpush".equals(cmd) || "lpushx".equals(cmd)
                || "lrange".equals(cmd) || "lrem".equals(cmd)
                || "lset".equals(cmd) || "ltrim".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("lastsave".equals(cmd)) {
            return new IntegerReply(BGroup.lastBgSaveMillis);
        }

        if ("lindex".equals(cmd)) {
            return lindex();
        }

        if ("linsert".equals(cmd)) {
            return linsert();
        }

        if ("llen".equals(cmd)) {
            return llen();
        }

        if ("lmove".equals(cmd)) {
            return lmove();
        }

        if ("lpop".equals(cmd)) {
            return lpop(true);
        }

        if ("lpos".equals(cmd)) {
            return lpos();
        }

        if ("lpush".equals(cmd)) {
            return lpush(true, false);
        }

        if ("lpushx".equals(cmd)) {
            return lpush(true, true);
        }

        if ("lrange".equals(cmd)) {
            return lrange();
        }

        if ("lrem".equals(cmd)) {
            return lrem();
        }

        if ("lset".equals(cmd)) {
            return lset();
        }

        if ("ltrim".equals(cmd)) {
            return ltrim();
        }

//        if ("load-rdb".equals(cmd)) {
//            try {
//                return loadRdb();
//            } catch (Exception e) {
//                return new ErrorReply(e.getMessage());
//            }
//        }

        return NilReply.INSTANCE;
    }

    private Reply lindex() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var indexBytes = data[2];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int index;
        try {
            index = Integer.parseInt(new String(indexBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (index >= RedisList.LIST_MAX_SIZE) {
            return ErrorReply.LIST_SIZE_TO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_LIST);
        if (encodedBytes == null) {
            // -1 or nil ? todo
            return NilReply.INSTANCE;
        }

        var buffer = ByteBuffer.wrap(encodedBytes);
        var size = buffer.getShort();
        if (index >= size) {
            return NilReply.INSTANCE;
        }

        if (index < 0) {
            index = size + index;
        }
        if (index < 0) {
            return NilReply.INSTANCE;
        }

        final byte[][] returnBytesArray = new byte[1][];
        int finalIndex = index;
        RedisList.iterate(encodedBytes, true, (bytes, i) -> {
            if (i == finalIndex) {
                returnBytesArray[0] = bytes;
                return true;
            }
            return false;
        });

        return new BulkReply(returnBytesArray[0]);
    }

    private Reply addToList(byte[] keyBytes, byte[][] valueBytesArr, boolean addFirst,
                            boolean considerBeforeOrAfter, boolean isBefore, byte[] pivotBytes, boolean needKeyExist) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        // lpushx / rpushx
        if (rl == null && needKeyExist) {
            return IntegerReply.REPLY_0;
        }

        if (rl != null) {
            if (rl.size() >= RedisList.LIST_MAX_SIZE) {
                return ErrorReply.LIST_SIZE_TO_LONG;
            }
        } else {
            if (considerBeforeOrAfter) {
                return IntegerReply.REPLY_0;
            }

            rl = new RedisList();
        }

        if (considerBeforeOrAfter) {
            // find pivot index
            int pivotIndex = rl.indexOf(pivotBytes);
            if (pivotIndex == -1) {
                // -1 or size ? todo
                return new IntegerReply(rl.size());
            }

            // only one
            var valueBytes = valueBytesArr[0];
            rl.addAt(isBefore ? pivotIndex : pivotIndex + 1, valueBytes);
        } else {
            if (addFirst) {
                for (var valueBytes : valueBytesArr) {
                    rl.addFirst(valueBytes);
                }
            } else {
                for (var valueBytes : valueBytesArr) {
                    rl.addLast(valueBytes);
                }
            }
        }

        saveRedisList(rl, keyBytes, slotWithKeyHash, dictMap);
        return new IntegerReply(rl.size());
    }

    public static RedisList getRedisList(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash, BaseCommand baseCommand) {
        var encodedBytes = baseCommand.get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_LIST);
        if (encodedBytes == null) {
            return null;
        }

        return RedisList.decode(encodedBytes);
    }

    static void saveRedisList(RedisList rl, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash, BaseCommand baseCommand, DictMap dictMap) {
        var key = new String(keyBytes);
        if (rl.size() == 0) {
            baseCommand.removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        baseCommand.set(keyBytes, rl.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_LIST);
    }

    private RedisList getRedisList(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        return getRedisList(keyBytes, slotWithKeyHash, this);
    }

    private void saveRedisList(RedisList rl, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash, DictMap dictMap) {
        saveRedisList(rl, keyBytes, slotWithKeyHash, this, dictMap);
    }

    private Reply linsert() {
        if (data.length != 5) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var beforeOrAfterBytes = data[2];
        var pivotBytes = data[3];
        var valueBytes = data[4];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (pivotBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        var beforeOrAfter = new String(beforeOrAfterBytes).toLowerCase();
        boolean isBefore = "before".equals(beforeOrAfter);
        boolean isAfter = "after".equals(beforeOrAfter);
        if (!isBefore && !isAfter) {
            return ErrorReply.SYNTAX;
        }

        byte[][] valueBytesArr = {null};
        valueBytesArr[0] = valueBytes;
        return addToList(keyBytes, valueBytesArr, false, true, isBefore, pivotBytes, false);
    }

    private Reply llen() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_LIST);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        var size = RedisList.getSizeWithoutDecode(encodedBytes);
        return new IntegerReply(size);
    }

    private Reply lmove() {
        return lmove(false);
    }

    Reply lmove(boolean isBlock) {
        if (!isBlock && data.length != 5) {
            return ErrorReply.FORMAT;
        }
        if (isBlock && data.length != 6) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        if (srcKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var srcLeftOrRightBytes = data[3];
        var dstLeftOrRightBytes = data[4];

        var srcLeftOrRight = new String(srcLeftOrRightBytes).toLowerCase();
        boolean isSrcLeft = "left".equals(srcLeftOrRight);
        boolean isSrcRight = "right".equals(srcLeftOrRight);
        if (!isSrcLeft && !isSrcRight) {
            return ErrorReply.SYNTAX;
        }

        var dstLeftOrRight = new String(dstLeftOrRightBytes).toLowerCase();
        boolean isDstLeft = "left".equals(dstLeftOrRight);
        boolean isDstRight = "right".equals(dstLeftOrRight);
        if (!isDstLeft && !isDstRight) {
            return ErrorReply.SYNTAX;
        }

        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();

        var rGroup = new RGroup(cmd, data, socket);
        rGroup.from(this);

        if (isBlock) {
            var timeoutBytes = data[5];
            int timeoutSeconds;
            try {
                timeoutSeconds = Integer.parseInt(new String(timeoutBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            if (timeoutSeconds > MAX_TIMEOUT_SECONDS) {
                return new ErrorReply("timeout must be <= " + MAX_TIMEOUT_SECONDS);
            }

            return rGroup.moveBlock(srcKeyBytes, srcSlotWithKeyHash, dstKeyBytes, dstSlotWithKeyHash, isSrcLeft, isDstLeft, timeoutSeconds);
        } else {
            return rGroup.move(srcKeyBytes, srcSlotWithKeyHash, dstKeyBytes, dstSlotWithKeyHash, isSrcLeft, isDstLeft);
        }
    }

    Reply lpop(boolean popFirst) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var countBytes = data.length == 3 ? data[2] : null;

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean isWithCount = countBytes != null;
        int count = 1;
        if (countBytes != null) {
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (count < 0) {
                return ErrorReply.RANGE_OUT_OF_INDEX;
            }
        }

        var isResp3 = SocketInspector.isResp3(socket);

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null || rl.size() == 0) {
            if (isWithCount) {
                if (isResp3) {
                    return NilReply.INSTANCE;
                } else {
                    return MultiBulkReply.NULL;
                }
            } else {
                return NilReply.INSTANCE;
            }
        }

        if (count == 0) {
            return MultiBulkReply.EMPTY;
        }

        ArrayList<Reply> replies = new ArrayList<>();

        int min = Math.min(count, Math.max(1, rl.size()));
        for (int i = 0; i < min; i++) {
            if (popFirst) {
                replies.add(new BulkReply(rl.removeFirst()));
            } else {
                replies.add(new BulkReply(rl.removeLast()));
            }
        }

        saveRedisList(rl, keyBytes, slotWithKeyHash, dictMap);
        if (!isWithCount) {
            return replies.getFirst();
        }

        var arr = new Reply[replies.size()];
        replies.toArray(arr);
        return new MultiBulkReply(arr);
    }

    private Reply lpos() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var valueBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        int rank = 1;
        int count = 1;
        // max compare times
        int maxlen = 0;
        for (int i = 3; i < data.length; i++) {
            String arg = new String(data[i]).toLowerCase();
            switch (arg) {
                case "rank" -> {
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        rank = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.NOT_INTEGER;
                    }
                }
                case "count" -> {
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        count = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.NOT_INTEGER;
                    }
                    if (count < 0) {
                        return ErrorReply.INVALID_INTEGER;
                    }
                }
                case "maxlen" -> {
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        maxlen = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.NOT_INTEGER;
                    }
                    if (maxlen < 0) {
                        return ErrorReply.INVALID_INTEGER;
                    }
                }
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            return NilReply.INSTANCE;
        }

        var list = rl.getList();

        Iterator<byte[]> it;
        boolean isReverse = false;
        if (rank < 0) {
            rank = -rank;
            it = list.descendingIterator();
            isReverse = true;
        } else {
            it = list.iterator();
        }

        ArrayList<Integer> posList = new ArrayList<>();

        int i = 0;
        while (it.hasNext()) {
            if (maxlen != 0 && i >= maxlen) {
                break;
            }

            var e = it.next();
            if (Arrays.equals(e, valueBytes)) {
                if (rank <= 1) {
                    posList.add(i);
                    if (count != 0) {
                        if (count <= posList.size()) {
                            break;
                        }
                    }
                }
                rank--;
            }
            i++;
        }

        if (posList.isEmpty()) {
            if (count == 1) {
                return NilReply.INSTANCE;
            } else {
                return MultiBulkReply.EMPTY;
            }
        }

        int maxIndex = rl.size() - 1;

        if (count == 1) {
            var pos = posList.getFirst();
            if (isReverse) {
                return new IntegerReply(maxIndex - pos);
            } else {
                return new IntegerReply(pos);
            }
        }

        Reply[] arr = new Reply[posList.size()];
        for (int j = 0; j < posList.size(); j++) {
            var pos = posList.get(j);
            arr[j] = isReverse ? new IntegerReply(maxIndex - pos) : new IntegerReply(pos);
        }
        return new MultiBulkReply(arr);
    }

    Reply lpush(boolean addFirst, boolean needKeyExist) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] valueBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var elementValueBytes = data[i];
            valueBytesArr[i - 2] = elementValueBytes;

            if (elementValueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                return ErrorReply.VALUE_TOO_LONG;
            }
        }

        var key = new String(keyBytes);
        var afterPopValueBytesArray = BlockingList.setReplyIfBlockingListExist(key, valueBytesArr, this);
        // no blocking for this key
        if (afterPopValueBytesArray == null) {
            return addToList(keyBytes, valueBytesArr, addFirst, false, false, null, needKeyExist);
        }

        // all elements are popped, need to add to list
        if (afterPopValueBytesArray.length == 0) {
            // no need to add to list
            var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
            var rl = getRedisList(keyBytes, slotWithKeyHash);
            if (rl == null || rl.size() == 0) {
                return IntegerReply.REPLY_1;
            } else {
                return new IntegerReply(rl.size() + 1);
            }
        }

        var reply = addToList(keyBytes, afterPopValueBytesArray, addFirst, false, false, null, needKeyExist);
        if (reply instanceof IntegerReply) {
            return new IntegerReply(((IntegerReply) reply).getInteger() + (valueBytesArr.length - afterPopValueBytesArray.length));
        } else {
            return reply;
        }
    }

    private Reply lrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int end;
        try {
            start = Integer.parseInt(new String(data[2]));
            end = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_LIST);
        if (encodedBytes == null) {
            return MultiBulkReply.EMPTY;
        }

        var buffer = ByteBuffer.wrap(encodedBytes);
        var size = buffer.getShort();

        var startEnd = IndexStartEndReset.reset(start, end, size);
        if (!startEnd.valid()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[startEnd.end() - startEnd.start() + 1];
        RedisList.iterate(encodedBytes, true, (bytes, i) -> {
            if (i > startEnd.end()) {
                return true;
            }

            if (i >= startEnd.start()) {
                replies[i - start] = new BulkReply(bytes);
            }
            return false;
        });

        return new MultiBulkReply(replies);
    }

    private Reply lrem() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var countBytes = data[2];
        var valueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        int count;
        try {
            count = Integer.parseInt(new String(countBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            return IntegerReply.REPLY_0;
        }

        var list = rl.getList();
        var it = count < 0 ? list.descendingIterator() : list.iterator();

        int absCount = count < 0 ? -count : count;
        int removed = 0;

        while (it.hasNext()) {
            var e = it.next();
            if (Arrays.equals(e, valueBytes)) {
                it.remove();
                removed++;
                if (count != 0) {
                    if (removed >= absCount) {
                        break;
                    }
                }
            }
        }

        if (removed > 0) {
            saveRedisList(rl, keyBytes, slotWithKeyHash, dictMap);
        }
        return new IntegerReply(removed);
    }

    private Reply lset() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var indexBytes = data[2];
        var valueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        int index;
        try {
            index = Integer.parseInt(new String(indexBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (index >= RedisList.LIST_MAX_SIZE) {
            return ErrorReply.LIST_SIZE_TO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            return ErrorReply.NO_SUCH_KEY;
        }

        int size = rl.size();
        if (index < 0) {
            index = size + index;
        }

        if (index < 0 || index >= size) {
            return ErrorReply.INDEX_OUT_OF_RANGE;
        }

        var valueBytesOld = rl.get(index);
        rl.setAt(index, valueBytes);

        if (!Arrays.equals(valueBytesOld, valueBytes)) {
            saveRedisList(rl, keyBytes, slotWithKeyHash, dictMap);
        }
        return OKReply.INSTANCE;
    }

    private Reply ltrim() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int end;
        try {
            start = Integer.parseInt(new String(data[2]));
            end = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            // or ErrorReply.NO_SUCH_KEY
            return OKReply.INSTANCE;
        }

        int size = rl.size();
        if (start > size || start > end) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), new String(keyBytes), slotWithKeyHash.keyHash());
            return OKReply.INSTANCE;
        }

        var startEnd = IndexStartEndReset.reset(start, end, size);
        if (!startEnd.valid()) {
            return ErrorReply.INVALID_INTEGER;
        }

        // keep index from start to stop
        var list = rl.getList();
        var it = list.iterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            if (i < startEnd.start()) {
                it.remove();
            }
            if (i > startEnd.end()) {
                it.remove();
            }
            i++;
        }

        saveRedisList(rl, keyBytes, slotWithKeyHash, dictMap);
        return OKReply.INSTANCE;
    }

//    Reply loadRdb() throws URISyntaxException, IOException {
//        if (data.length != 2 && data.length != 3) {
//            return ErrorReply.FORMAT;
//        }
//
//        var filePathBytes = data[1];
//        var filePath = new String(filePathBytes);
//        if (!filePath.startsWith("/") || !filePath.endsWith(".rdb")) {
//            return ErrorReply.INVALID_FILE;
//        }
//
//        var file = new File(filePath);
//        if (!file.exists()) {
//            return ErrorReply.NO_SUCH_FILE;
//        }
//
//        boolean onlyAnalysis = false;
//        if (data.length == 3) {
//            onlyAnalysis = "analysis".equals(new String(data[2]).toLowerCase());
//        }
//
//        var r = new RedisReplicator("redis://" + filePath);
//        r.setRdbVisitor(new ValueIterableRdbVisitor(r));
//
//        var eventListener = new MyRDBVisitorEventListener(this, onlyAnalysis);
//        r.addEventListener(eventListener);
//        r.open();
//
//        if (onlyAnalysis) {
//            int n = 10;
//            for (int i = 0; i < n; i++) {
//                eventListener.radixTree.sumIncrFromRoot(new int[i]);
//            }
//        }
//
//        return new IntegerReply(eventListener.keyCount);
//    }
}
