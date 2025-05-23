package io.velo.command;

import com.github.luben.zstd.Zstd;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.reply.*;
import io.velo.type.RedisBF;
import io.velo.type.RedisBitSet;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

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
            slotWithKeyHashList.add(slot(data[1], slotNumber));
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
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            slotWithKeyHashList.add(slot(data[2], slotNumber));
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

    public static long lastBgSaveMillis = 0;

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
            lastBgSaveMillis = System.currentTimeMillis();
            if (ConfForGlobal.pureMemory) {
                for (var oneSlot : localPersist.oneSlots()) {
                    oneSlot.asyncRun(oneSlot::writeToSavedFileWhenPureMemory);
                }
            }

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

        var bs = new RedisBitSet(valueBytes);
        int pos = -1;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if ((bs.get(i) && isBit1) || (!bs.get(i) && !isBit1)) {
                    pos = i;
                    break;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if ((bs.get(i) && isBit1) || (!bs.get(i) && !isBit1)) {
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
            return bfAdd(false);
        }

        if ("card".equals(bfCmdSuffix)) {
            return bfCard();
        }

        if ("exists".equals(bfCmdSuffix)) {
            return bfExists(false);
        }

        if ("info".equals(bfCmdSuffix)) {
            return bfInfo();
        }

        if ("insert".equals(bfCmdSuffix)) {
            return bfInsert();
        }

        if ("loadchunk".equalsIgnoreCase(bfCmdSuffix)) {
            return bfLoadchunk();
        }

        if ("madd".equals(bfCmdSuffix)) {
            return bfAdd(true);
        }

        if ("mexists".equals(bfCmdSuffix)) {
            return bfExists(true);
        }

        if ("reserve".equalsIgnoreCase(bfCmdSuffix)) {
            return bfReserve();
        }

        if ("scandump".equalsIgnoreCase(bfCmdSuffix)) {
            return bfScandump();
        }

        return ErrorReply.SYNTAX;
    }

    private Reply bfAdd(boolean isMulti) {
        if (isMulti) {
            if (data.length < 3) {
                return ErrorReply.FORMAT;
            }
        } else {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }
        }

        var keyBytes = data[1];
        ArrayList<String> items = new ArrayList<>(data.length - 2);
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            var item = new String(itemBytes);
            items.add(item);
        }

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

        ArrayList<Boolean> isPutList = new ArrayList<>(items.size());
        for (var item : items) {
            var isPutInner = redisBF.put(item);
            isPutList.add(isPutInner);
        }

        var isPut = isPutList.stream().anyMatch(x -> x);
        if (isPut) {
            var encoded = redisBF.encode();
            set(keyBytes, encoded, s, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        }

        if (isMulti) {
            var replies = new Reply[isPutList.size()];
            for (int i = 0; i < isPutList.size(); i++) {
                replies[i] = isPutList.get(i) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
            }
            return new MultiBulkReply(replies);
        } else {
            return isPut ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
    }

    private Reply bfCard() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];

        var s = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, s);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }
        if (!cv.isBloomFilter()) {
            return ErrorReply.WRONG_TYPE;
        }

        var redisBF = RedisBF.decode(cv.getCompressedData());
        return new IntegerReply(redisBF.itemInserted());
    }

    private Reply bfExists(boolean isMulti) {
        if (isMulti) {
            if (data.length < 3) {
                return ErrorReply.FORMAT;
            }
        } else {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }
        }

        var keyBytes = data[1];
        ArrayList<String> items = new ArrayList<>(data.length - 2);
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            var item = new String(itemBytes);
            items.add(item);
        }

        var s = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, s);
        if (cv == null) {
            if (isMulti) {
                var replies = new Reply[items.size()];
                for (int i = 0; i < items.size(); i++) {
                    replies[i] = IntegerReply.REPLY_0;
                }
                return new MultiBulkReply(replies);
            } else {
                return IntegerReply.REPLY_0;
            }
        }
        if (!cv.isBloomFilter()) {
            return ErrorReply.WRONG_TYPE;
        }

        var redisBF = RedisBF.decode(cv.getCompressedData());

        ArrayList<Boolean> isExistsList = new ArrayList<>(items.size());
        for (var item : items) {
            var isExists = redisBF.mightContain(item);
            isExistsList.add(isExists);
        }

        if (isMulti) {
            var replies = new Reply[isExistsList.size()];
            for (int i = 0; i < isExistsList.size(); i++) {
                replies[i] = isExistsList.get(i) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
            }
            return new MultiBulkReply(replies);
        } else {
            return isExistsList.getFirst() ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
    }

    private Reply bfInfo() {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];

        var s = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, s);
        if (cv == null) {
            return MultiBulkReply.EMPTY;
        }
        if (!cv.isBloomFilter()) {
            return ErrorReply.WRONG_TYPE;
        }

        var redisBF = RedisBF.decode(cv.getCompressedData());

        var field = data.length == 3 ? new String(data[2]).toLowerCase() : null;
        if (field == null) {
            return new MultiBulkReply(new Reply[]{
                    new BulkReply("Capacity".getBytes()),
                    new IntegerReply(redisBF.capacity()),
                    new BulkReply("Size".getBytes()),
                    new IntegerReply(redisBF.memoryAllocatedEstimate()),
                    new BulkReply("Number of filters".getBytes()),
                    new IntegerReply(redisBF.listSize()),
                    new BulkReply("Number of items inserted".getBytes()),
                    new IntegerReply(redisBF.itemInserted()),
                    new BulkReply("Expansion rate".getBytes()),
                    new IntegerReply(redisBF.getExpansion())
            });
        } else {
            var replies = new Reply[1];
            switch (field) {
                case "capacity":
                    replies[0] = new IntegerReply(redisBF.capacity());
                    break;
                case "size":
                    replies[0] = new IntegerReply(redisBF.memoryAllocatedEstimate());
                    break;
                case "filters":
                    replies[0] = new IntegerReply(redisBF.listSize());
                    break;
                case "items":
                    replies[0] = new IntegerReply(redisBF.itemInserted());
                    break;
                case "expansion":
                    replies[0] = new IntegerReply(redisBF.getExpansion());
                    break;
                default:
                    return ErrorReply.SYNTAX;
            }
            return new MultiBulkReply(replies);
        }
    }

    private Reply bfInsert() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];

        var initCapacity = RedisBF.DEFAULT_CAPACITY;
        var initFpp = RedisBF.DEFAULT_FPP;
        var initExpansion = RedisBF.DEFAULT_EXPANSION;
        boolean noCreate = false;
        boolean nonScaling = false;

        boolean needCreateNew = false;

        ArrayList<String> items = new ArrayList<>();
        for (int i = 2; i < data.length; i++) {
            var arg = new String(data[i]);

            if ("ITEMS".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }

                for (int j = i + 1; j < data.length; j++) {
                    var itemBytes = data[j];
                    var item = new String(itemBytes);
                    items.add(item);
                }
                break;
            }

            if ("CAPACITY".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                initCapacity = Integer.parseInt(new String(data[i + 1]));
                needCreateNew = true;
            }

            if ("ERROR".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                initFpp = Double.parseDouble(new String(data[i + 1]));
                if (initFpp <= 0 || initFpp >= 1) {
                    return new ErrorReply("error must be > 0 and < 1");
                }
                needCreateNew = true;
            }

            if ("EXPANSION".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                initExpansion = Byte.parseByte(new String(data[i + 1]));
                if (initExpansion > RedisBF.MAX_EXPANSION) {
                    return new ErrorReply("expansion too large");
                }
                needCreateNew = true;
            }

            if ("NOCREATE".equalsIgnoreCase(arg)) {
                noCreate = true;
            }

            if ("NONSCALING".equalsIgnoreCase(arg)) {
                nonScaling = true;
                needCreateNew = true;
            }
        }

        var key = new String(keyBytes);
        var s = slotWithKeyHashListParsed.getFirst();
        var isExists = exists(s.slot(), s.bucketIndex(), key, s.keyHash(), s.keyHash32());

        RedisBF redisBF;
        if (isExists) {
            if (needCreateNew) {
                return ErrorReply.BF_ALREADY_EXISTS;
            }
            var cv = getCv(keyBytes, s);
            if (!cv.isBloomFilter()) {
                return ErrorReply.WRONG_TYPE;
            }
            redisBF = RedisBF.decode(cv.getCompressedData());
        } else {
            if (noCreate) {
                return ErrorReply.BF_NOT_EXISTS;
            }
            redisBF = new RedisBF(initCapacity, initFpp, initExpansion, nonScaling);
        }

        boolean isPut = false;
        var replies = new Reply[items.size()];
        for (int i = 0; i < items.size(); i++) {
            var item = items.get(i);
            var isPutInner = redisBF.put(item);
            replies[i] = isPutInner ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
            isPut |= isPutInner;
        }

        if (isPut) {
            set(keyBytes, redisBF.encode(), s, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        }

        return new MultiBulkReply(replies);
    }

    private Reply bfLoadchunk() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var s = slotWithKeyHashListParsed.getFirst();

        var iteratorBytes = data[2];
        int iterator = 0;
        try {
            iterator = Integer.parseInt(new String(iteratorBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        // only support 0, dump all once
        if (iterator != 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var dumpBytes = data[3];
        var buffer = ByteBuffer.wrap(dumpBytes);
        var encodedLength = buffer.getInt();
        var encoded = new byte[encodedLength];

        Zstd.decompressByteArray(encoded, 0, encodedLength, dumpBytes, 4, dumpBytes.length - 4);
        set(keyBytes, encoded, s, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        return OKReply.INSTANCE;
    }

    private Reply bfScandump() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var s = slotWithKeyHashListParsed.getFirst();

        var iteratorBytes = data[2];
        int iterator = 0;
        try {
            iterator = Integer.parseInt(new String(iteratorBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        // only support 0, dump all once
        if (iterator != 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var cv = getCv(keyBytes, s);
        if (cv == null) {
            return MultiBulkReply.EMPTY;
        }
        if (!cv.isBloomFilter()) {
            return MultiBulkReply.EMPTY;
        }

        var encoded = cv.getCompressedData();
        var compressed = Zstd.compress(encoded);

        var dumpBytes = new byte[4 + compressed.length];
        ByteBuffer.wrap(dumpBytes).putInt(encoded.length).put(compressed);

        return new MultiBulkReply(new Reply[]{
                // 0 -> dump all
                new IntegerReply(0),
                new BulkReply(dumpBytes)
        });
    }

    private Reply bfReserve() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var initExpansion = RedisBF.DEFAULT_EXPANSION;
        boolean nonScaling = false;

        var initFpp = Double.parseDouble(new String(data[2]));
        if (initFpp <= 0 || initFpp >= 1) {
            return new ErrorReply("error must be > 0 and < 1");
        }

        var initCapacity = Integer.parseInt(new String(data[3]));

        for (int i = 4; i < data.length; i++) {
            var arg = new String(data[i]);

            if ("EXPANSION".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                initExpansion = Byte.parseByte(new String(data[i + 1]));
                if (initExpansion > RedisBF.MAX_EXPANSION) {
                    return new ErrorReply("expansion too large");
                }
            }

            if ("NONSCALING".equalsIgnoreCase(arg)) {
                nonScaling = true;
            }
        }

        var key = new String(keyBytes);
        var s = slotWithKeyHashListParsed.getFirst();
        var isExists = exists(s.slot(), s.bucketIndex(), key, s.keyHash(), s.keyHash32());

        if (isExists) {
            return ErrorReply.BF_ALREADY_EXISTS;
        }

        var redisBF = new RedisBF(initCapacity, initFpp, initExpansion, nonScaling);
        set(keyBytes, redisBF.encode(), s, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        return OKReply.INSTANCE;
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
                        var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, isLeft, System.currentTimeMillis(), null);
                        for (var key : keys) {
                            BlockingList.blockingListPromisesByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(one);
                        }

                        var reactor = Reactor.getCurrentReactor();
                        reactor.delay(timeoutSeconds * 1000, () -> {
                            if (!finalPromise.isComplete()) {
                                finalPromise.set(NilReply.INSTANCE);
                                // remove form blocking list
                                for (var key : keys) {
                                    BlockingList.blockingListPromisesByKey.get(key).remove(one);
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
                var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, isLeft, System.currentTimeMillis(), null);
                BlockingList.blockingListPromisesByKey.computeIfAbsent(firstKey, k -> Collections.synchronizedList(new ArrayList<>())).add(one);

                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutSeconds * 1000, () -> {
                    if (!finalPromise.isComplete()) {
                        finalPromise.set(NilReply.INSTANCE);
                        // remove form blocking list
                        BlockingList.blockingListPromisesByKey.get(firstKey).remove(one);
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
