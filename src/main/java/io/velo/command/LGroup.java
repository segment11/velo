package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.TrainSampleJob;
import io.velo.reply.*;
import io.velo.type.RedisList;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class LGroup extends BaseCommand {
    public LGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("lmove".equals(cmd)) {
            if (data.length != 5) {
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

        if ("lindex".equals(cmd) || "linsert".equals(cmd)
                || "llen".equals(cmd) || "lpop".equals(cmd) || "lpos".equals(cmd)
                || "lpush".equals(cmd) || "lpushx".equals(cmd)
                || "lrange".equals(cmd) || "lrem".equals(cmd)
                || "lset".equals(cmd) || "ltrim".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
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

    @VisibleForTesting
    Reply lindex() {
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
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            // -1 or nil ? todo
            return NilReply.INSTANCE;
        }

        if (index >= rl.size()) {
            return NilReply.INSTANCE;
        }

        if (index < 0) {
            index = rl.size() + index;
        }
        if (index < 0) {
            return NilReply.INSTANCE;
        }

        return new BulkReply(rl.get(index));
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

        saveRedisList(rl, keyBytes, slotWithKeyHash);
        return new IntegerReply(rl.size());
    }

    private RedisList getRedisList(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_LIST);
        if (encodedBytes == null) {
            return null;
        }

        return RedisList.decode(encodedBytes);
    }


    private void saveRedisList(RedisList rl, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var key = new String(keyBytes);
        if (rl.size() == 0) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(keyBytes, rl.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_LIST);
    }

    @VisibleForTesting
    Reply linsert() {
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

    @VisibleForTesting
    Reply llen() {
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

    @VisibleForTesting
    Reply lmove() {
        if (data.length != 5) {
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

        return rGroup.move(srcKeyBytes, srcSlotWithKeyHash, dstKeyBytes, dstSlotWithKeyHash, isSrcLeft, isDstLeft);
    }

    @VisibleForTesting
    Reply lpop(boolean popFirst) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var countBytes = data.length == 3 ? data[2] : null;

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        if (countBytes != null) {
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (count <= 0) {
                return ErrorReply.INVALID_INTEGER;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            return NilReply.INSTANCE;
        }

        ArrayList<Reply> replies = new ArrayList<>();

        boolean isUpdated = false;

        int min = Math.min(count, Math.max(1, rl.size()));
        for (int i = 0; i < min; i++) {
            if (rl.size() == 0) {
                replies.add(NilReply.INSTANCE);
                continue;
            }

            isUpdated = true;
            if (popFirst) {
                replies.add(new BulkReply(rl.removeFirst()));
            } else {
                replies.add(new BulkReply(rl.removeLast()));
            }
        }

        if (isUpdated) {
            saveRedisList(rl, keyBytes, slotWithKeyHash);
        }

        if (count == 1) {
            return replies.getFirst();
        }

        var arr = new Reply[replies.size()];
        replies.toArray(arr);
        return new MultiBulkReply(arr);
    }

    @VisibleForTesting
    Reply lpos() {
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

    @VisibleForTesting
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
            valueBytesArr[i - 2] = data[i];

            if (valueBytesArr[i - 2].length > CompressedValue.VALUE_MAX_LENGTH) {
                return ErrorReply.VALUE_TOO_LONG;
            }
        }

        return addToList(keyBytes, valueBytesArr, addFirst, false, false, null, needKeyExist);
    }

    @VisibleForTesting
    Reply lrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var startBytes = data[2];
        var stopBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int stop;
        try {
            start = Integer.parseInt(new String(startBytes));
            stop = Integer.parseInt(new String(stopBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rl = getRedisList(keyBytes, slotWithKeyHash);
        if (rl == null) {
            return MultiBulkReply.EMPTY;
        }

        int size = rl.size();
        if (start < 0) {
            start = size + start;
            if (start < 0) {
                start = 0;
            }
        }
        if (stop < 0) {
            stop = size + stop;
            if (stop < 0) {
                return MultiBulkReply.EMPTY;
            }
        }
        if (start >= size) {
            return MultiBulkReply.EMPTY;
        }
        if (stop >= size) {
            stop = size - 1;
        }
        if (start > stop) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[stop - start + 1];
        for (int i = start; i <= stop; i++) {
            replies[i - start] = new BulkReply(rl.get(i));
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply lrem() {
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
            saveRedisList(rl, keyBytes, slotWithKeyHash);
        }
        return new IntegerReply(removed);
    }

    @VisibleForTesting
    Reply lset() {
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
            saveRedisList(rl, keyBytes, slotWithKeyHash);
        }
        return OKReply.INSTANCE;
    }

    @VisibleForTesting
    Reply ltrim() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var startBytes = data[2];
        var stopBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int stop;

        try {
            start = Integer.parseInt(new String(startBytes));
            stop = Integer.parseInt(new String(stopBytes));
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

        if (start < 0) {
            start = size + start;
        }
        if (stop < 0) {
            stop = size + stop;
        }

        if (start < 0) {
            start = 0;
        }
        if (stop < 0) {
            stop = 0;
        }

        if (start > size || start > stop) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), new String(keyBytes), slotWithKeyHash.keyHash());
            return OKReply.INSTANCE;
        }

        // keep index from start to stop
        var list = rl.getList();
        var it = list.iterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            if (i < start) {
                it.remove();
            }
            if (i > stop) {
                it.remove();
            }
            i++;
        }

        saveRedisList(rl, keyBytes, slotWithKeyHash);
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
