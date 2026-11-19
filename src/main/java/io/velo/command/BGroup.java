package io.velo.command;

import com.github.luben.zstd.Zstd;
import com.google.common.annotations.VisibleForTesting;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Utils;
import io.velo.persist.Wal;
import io.velo.reply.*;
import io.velo.DictMap;
import io.velo.TypeMismatchException;
import io.velo.type.RedisBF;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import io.velo.type.RedisBitSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles Redis commands starting with letter 'B'.
 * This includes commands like BITCOUNT, BF.*, BLPOP, BRPOP, BGSAVE, etc.
 */
public class BGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "bgsave", -1,
                Set.of("admin", "noscript"),
                0, 0, 0,
                Set.of("@admin", "@dangerous", "@slow"),
                "server",
                "Asynchronously save the dataset to disk.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bitcount", -2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bitmap", "@slow"),
                "bitmap",
                "Count the number of set bits (population counting) in a string.",
                "2.6.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "bitpos", -3,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bitmap", "@slow"),
                "bitmap",
                "Find the position of the first bit set or clear in a string.",
                "2.8.7", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "blmove", 6,
                Set.of("write", "denyoom", "blocking"),
                1, 2, 1,
                Set.of("@write", "@list", "@slow", "@blocking"),
                "list",
                "Blocking version of LMOVE.",
                "6.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "blmpop", -5,
                Set.of("write", "blocking", "movablekeys"),
                0, 0, 0,
                Set.of("@write", "@list", "@slow", "@blocking"),
                "list",
                "Blocking variant of LMPOP.",
                "7.0.0", "O(N+M)"));
        CommandRegistry.register(new CommandEntry(
                "blpop", -3,
                Set.of("write", "blocking"),
                1, -2, 1,
                Set.of("@write", "@list", "@slow", "@blocking"),
                "list",
                "Remove and get the first element in a list, or block until one is available.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "brpop", -3,
                Set.of("write", "blocking"),
                1, -2, 1,
                Set.of("@write", "@list", "@slow", "@blocking"),
                "list",
                "Remove and get the last element in a list, or block until one is available.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "brpoplpush", 4,
                Set.of("write", "denyoom", "blocking"),
                1, 2, 1,
                Set.of("@write", "@list", "@slow", "@blocking"),
                "list",
                "Pop an element from a list, push it to another list and return it; or block until one is available.",
                "2.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bzmpop", -5,
                Set.of("write", "blocking", "movablekeys"),
                0, 0, 0,
                Set.of("@write", "@sortedset", "@slow", "@blocking"),
                "sorted_set",
                "Blocking variant of ZMPOP.",
                "7.0.0", "O(K)+O(M*log(N))"));
        CommandRegistry.register(new CommandEntry(
                "bzpopmax", -3,
                Set.of("write", "fast", "blocking"),
                1, -2, 1,
                Set.of("@write", "@sortedset", "@fast", "@blocking"),
                "sorted_set",
                "Remove and return the member with the highest score, or block until one is available.",
                "5.0.0", "O(log(N))"));
        CommandRegistry.register(new CommandEntry(
                "bzpopmin", -3,
                Set.of("write", "fast", "blocking"),
                1, -2, 1,
                Set.of("@write", "@sortedset", "@fast", "@blocking"),
                "sorted_set",
                "Remove and return the member with the lowest score, or block until one is available.",
                "5.0.0", "O(log(N))"));
        CommandRegistry.register(new CommandEntry(
                "bf.add", -3,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@write", "@bloom"),
                "bloom",
                "Add an element to a Bloom filter.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bf.card", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bloom"),
                "bloom",
                "Return the cardinality of a Bloom filter.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bf.exists", 3,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bloom"),
                "bloom",
                "Determine whether an item exists in a Bloom filter.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bf.info", -2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bloom"),
                "bloom",
                "Return information about a Bloom filter.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bf.insert", -4,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@write", "@bloom"),
                "bloom",
                "Add one or more elements to a Bloom filter, creating it if it does not yet exist.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "bf.loadchunk", 4,
                Set.of("write"),
                1, 1, 1,
                Set.of("@write", "@bloom"),
                "bloom",
                "Restore a Bloom filter previously saved using SCANDUMP.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "bf.madd", -3,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@write", "@bloom"),
                "bloom",
                "Add multiple elements to a Bloom filter.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "bf.mexists", -3,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bloom"),
                "bloom",
                "Determine whether multiple items exist in a Bloom filter.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "bf.reserve", -4,
                Set.of("write"),
                1, 1, 1,
                Set.of("@write", "@bloom"),
                "bloom",
                "Create an empty Bloom filter with a desired initial capacity and false positive ratio.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "bf.scandump", 3,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@bloom"),
                "bloom",
                "Begin an incremental save of a Bloom filter.",
                "1.0.0", "O(N)"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
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
        if ("bitcount".equals(cmd) || "bitpos".equals(cmd) || cmd.startsWith("bf.")) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
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

        if ("blmpop".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }
            int numKeys;
            try {
                numKeys = Integer.parseInt(new String(data[2]));
            } catch (NumberFormatException e) {
                return slotWithKeyHashList;
            }
            if (numKeys <= 0 || data.length < 3 + numKeys + 1) {
                return slotWithKeyHashList;
            }
            for (int i = 0; i < numKeys; i++) {
                slotWithKeyHashList.add(slot(data[3 + i], slotNumber));
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

        if ("bzmpop".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }
            int numKeys;
            try {
                numKeys = Integer.parseInt(new String(data[2]));
            } catch (NumberFormatException e) {
                return slotWithKeyHashList;
            }
            if (numKeys <= 0 || data.length < 3 + numKeys + 1) {
                return slotWithKeyHashList;
            }
            for (int i = 0; i < numKeys; i++) {
                slotWithKeyHashList.add(slot(data[3 + i], slotNumber));
            }
            return slotWithKeyHashList;
        }

        if ("bzpopmax".equals(cmd) || "bzpopmin".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }

            // eg. bzpopmin key1 key2 timeout
            for (int i = 1; i < data.length - 1; i++) {
                slotWithKeyHashList.add(slot(data[i], slotNumber));
            }
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    /**
     * The timestamp (in milliseconds) of the last successful BGSAVE operation.
     */
    public static final AtomicLong lastBgSaveMillis = new AtomicLong(0);

    /**
     * Handles the command and returns the reply.
     *
     * @return the reply for this command
     */
    @Override
    public Reply handle() {
        if (cmd.startsWith("bf.")) {
            return bf();
        }

        if ("bgsave".equals(cmd)) {
            lastBgSaveMillis.set(System.currentTimeMillis());
            return OKReply.INSTANCE;
        }

        if ("bitcount".equals(cmd)) {
            return bitcount();
        }

        if ("bitpos".equals(cmd)) {
            return bitpos();
        }

        if ("blmpop".equals(cmd)) {
            return blmpop();
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

        if ("bzmpop".equals(cmd)) {
            return bzmpop();
        }

        if ("bzpopmax".equals(cmd) || "bzpopmin".equals(cmd)) {
            return bzpop("bzpopmin".equals(cmd));
        }

        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }

    private Reply bitcount() {
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
                return ErrorReply.INVALID_INTEGER;
            }

            isIndexUseBit = data.length == 5 && "bit".equalsIgnoreCase(new String(data[4]));
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, slotWithKeyHash);
        var canIndexValueBytesLength = isIndexUseBit ? valueBytes.length * 8 : valueBytes.length;

        var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
        if (!startEndWith.valid()) {
            return IntegerReply.REPLY_0;
        }

        var bs = new RedisBitSet(valueBytes);
        int count = 0;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if (bs.get(i)) {
                    count++;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if (bs.get(i)) {
                    count++;
                }
            }
        }

        return new IntegerReply(count);
    }

    private Reply bitpos() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var bit1or0Bytes = data[2];
        int bit;
        try {
            bit = Integer.parseInt(new String(bit1or0Bytes));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }
        if (bit != 0 && bit != 1) {
            return new ErrorReply("The bit argument must be 1 or 0.");
        }
        boolean isBit1 = bit == 1;

        int start;
        int end;
        boolean isIndexUseBit;
        boolean isEndGiven;
        if (data.length == 3) {
            start = 0;
            end = -1;
            isIndexUseBit = false;
            isEndGiven = false;
        } else {
            if (data.length != 4 && data.length != 5 && data.length != 6) {
                return ErrorReply.SYNTAX;
            }

            try {
                start = Integer.parseInt(new String(data[3]));
                end = data.length >= 5 ? Integer.parseInt(new String(data[4])) : -1;
            } catch (NumberFormatException e) {
                return ErrorReply.INVALID_INTEGER;
            }

            isIndexUseBit = data.length == 6 && "bit".equalsIgnoreCase(new String(data[5]));
            isEndGiven = data.length >= 5;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            // a missing key is an infinite array of zero bits
            return new IntegerReply(isBit1 ? -1 : 0);
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, slotWithKeyHash);
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

        if (pos == -1 && !isBit1 && !isEndGiven) {
            // the right of the string is padded with zeros when no range or the start argument only is given
            pos = valueBytes.length * 8;
        }

        return new IntegerReply(pos);
    }

    private Reply bf() {
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

        ArrayList<String> items = new ArrayList<>(data.length - 2);
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            var item = new String(itemBytes);
            items.add(item);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        RedisBF redisBF;
        var cv = getCv(slotWithKeyHash);
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
            set(encoded, slotWithKeyHash, CompressedValue.SP_TYPE_BLOOM_BITMAP);
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
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

        ArrayList<String> items = new ArrayList<>(data.length - 2);
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            var item = new String(itemBytes);
            items.add(item);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
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
                    new BulkReply("Capacity"),
                    new IntegerReply(redisBF.capacity()),
                    new BulkReply("Size"),
                    new IntegerReply(redisBF.memoryAllocatedEstimate()),
                    new BulkReply("Number of filters"),
                    new IntegerReply(redisBF.listSize()),
                    new BulkReply("Number of items inserted"),
                    new IntegerReply(redisBF.itemInserted()),
                    new BulkReply("Expansion rate"),
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
                try {
                    initCapacity = Integer.parseInt(new String(data[i + 1]));
                } catch (NumberFormatException e) {
                    return ErrorReply.INVALID_INTEGER;
                }
                if (initCapacity <= 0) {
                    return ErrorReply.CAPACITY_NOT_POSITIVE;
                }
                needCreateNew = true;
            }

            if ("ERROR".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    initFpp = Double.parseDouble(new String(data[i + 1]));
                } catch (NumberFormatException e) {
                    return ErrorReply.INVALID_FLOAT;
                }
                if (initFpp <= 0 || initFpp >= 1) {
                    return new ErrorReply("error must be > 0 and < 1");
                }
                needCreateNew = true;
            }

            if ("EXPANSION".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    initExpansion = Byte.parseByte(new String(data[i + 1]));
                } catch (NumberFormatException e) {
                    return ErrorReply.INVALID_INTEGER;
                }
                if (initExpansion > RedisBF.MAX_EXPANSION) {
                    return new ErrorReply("expansion too large");
                }
                if (initExpansion <= 0) {
                    return ErrorReply.INVALID_INTEGER;
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var isExists = exists(slotWithKeyHash);

        RedisBF redisBF;
        if (isExists) {
            if (needCreateNew) {
                return ErrorReply.BF_ALREADY_EXISTS;
            }
            var cv = getCv(slotWithKeyHash);
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
            set(redisBF.encode(), slotWithKeyHash, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        }

        return new MultiBulkReply(replies);
    }

    private Reply bfLoadchunk() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var iteratorBytes = data[2];
        int iterator = 0;
        try {
            iterator = Integer.parseInt(new String(iteratorBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }
        // only support 0, dump all once
        if (iterator != 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var dumpBytes = data[3];
        if (dumpBytes.length <= 4) {
            return ErrorReply.BAD_DATA_FORMAT;
        }
        var buffer = ByteBuffer.wrap(dumpBytes);
        var encodedLength = buffer.getInt();
        // the length prefix is client controlled, never allocate before it is bounded
        if (encodedLength <= 0 || encodedLength > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.BAD_DATA_FORMAT;
        }
        var frameContentSize = Zstd.getFrameContentSize(dumpBytes, 4, dumpBytes.length - 4);
        if (frameContentSize > 0 && frameContentSize != encodedLength) {
            return ErrorReply.BAD_DATA_FORMAT;
        }

        var encoded = new byte[encodedLength];
        Zstd.decompressByteArray(encoded, 0, encodedLength, dumpBytes, 4, dumpBytes.length - 4);
        RedisBF.decode(encoded);
        set(encoded, slotWithKeyHash, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        return OKReply.INSTANCE;
    }

    private Reply bfScandump() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var iteratorBytes = data[2];
        int iterator = 0;
        try {
            iterator = Integer.parseInt(new String(iteratorBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }
        // only support 0, dump all once
        if (iterator != 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var cv = getCv(slotWithKeyHash);
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

        var initExpansion = RedisBF.DEFAULT_EXPANSION;
        boolean nonScaling = false;

        double initFpp;
        try {
            initFpp = Double.parseDouble(new String(data[2]));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_FLOAT;
        }
        if (initFpp <= 0 || initFpp >= 1) {
            return new ErrorReply("error must be > 0 and < 1");
        }

        int initCapacity;
        try {
            initCapacity = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.INVALID_INTEGER;
        }
        if (initCapacity <= 0) {
            return ErrorReply.CAPACITY_NOT_POSITIVE;
        }

        for (int i = 4; i < data.length; i++) {
            var arg = new String(data[i]);

            if ("EXPANSION".equalsIgnoreCase(arg)) {
                if (data.length <= i + 1) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    initExpansion = Byte.parseByte(new String(data[i + 1]));
                } catch (NumberFormatException e) {
                    return ErrorReply.INVALID_INTEGER;
                }
                if (initExpansion > RedisBF.MAX_EXPANSION) {
                    return new ErrorReply("expansion too large");
                }
                if (initExpansion <= 0) {
                    return ErrorReply.INVALID_INTEGER;
                }
            }

            if ("NONSCALING".equalsIgnoreCase(arg)) {
                nonScaling = true;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var isExists = exists(slotWithKeyHash);

        if (isExists) {
            return ErrorReply.BF_ALREADY_EXISTS;
        }

        var redisBF = new RedisBF(initCapacity, initFpp, initExpansion, nonScaling);
        set(redisBF.encode(), slotWithKeyHash, CompressedValue.SP_TYPE_BLOOM_BITMAP);
        return OKReply.INSTANCE;
    }


    private Reply blpop(boolean isLeft) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 1; i < data.length - 1; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            keys.add(Wal.keyString(keyBytes));
        }

        var timeoutBytes = data[data.length - 1];
        double timeoutSeconds;
        try {
            timeoutSeconds = Double.parseDouble(new String(timeoutBytes));
        } catch (NumberFormatException e) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0) {
            return new ErrorReply("timeout is negative");
        }
        if (!Double.isFinite(timeoutSeconds)) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        boolean isBlockIndefinitely = timeoutSeconds == 0.0;

        var firstKeyBytes = data[1];
        var firstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var rl = LGroup.getRedisList(firstSlotWithKeyHash, this);
        if (rl == null || rl.isEmpty()) {
            // performance bad, get all other values
            if (keys.size() > 1) {
                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, socket, isLeft,
                        System.currentTimeMillis(), null, 0, distinctSlots(slotWithKeyHashListParsed));

                // each key is checked on the worker owning it, never blocking the current worker
                // In production, completions bounce back to this first key's worker reactor,
                // which also schedules the timeout and settles the reply.
                popOtherKeyAsync(1, keys, isLeft).whenComplete((r, e) -> {
                    if (e != null) {
                        log.error("{} error={}", isLeft ? "blpop" : "brpop", e.getMessage());
                        if (e instanceof TypeMismatchException) {
                            finalPromise.set(ErrorReply.WRONG_TYPE);
                        } else {
                            finalPromise.setException(Utils.wrapException(isLeft ? "blpop" : "brpop", e));
                        }
                        return;
                    }

                    if (r != null) {
                        finalPromise.set(r);
                        return;
                    }

                    // register the waiter on every key owning worker, so a push to any of them can wake it up
                    addBlockingWaiter(keys, one);
                    // close the check then register window, a push may land between both steps
                    recheckBlockingList(keys, one, isLeft);

                    if (!isBlockIndefinitely) {
                        long timeoutMillis = (long) (timeoutSeconds * 1000);
                        var reactor = Reactor.getCurrentReactor();
                        reactor.delay(timeoutMillis, () -> {
                            // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                            if (one.tryMarkDelivered()) {
                                finalPromise.set(NullArrayReply.INSTANCE);
                                removeBlockingWaiter(keys, one);
                            }
                        });
                    }
                });

                return asyncReply;
            }

            // only one key
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            var firstKey = keys.getFirst();
            var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, socket, isLeft,
                    System.currentTimeMillis(), null, 0, distinctSlots(slotWithKeyHashListParsed));
            BlockingList.addOne(firstKey, one);

            if (!isBlockIndefinitely) {
                long timeoutMillis = (long) (timeoutSeconds * 1000);
                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutMillis, () -> {
                    // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                    if (one.tryMarkDelivered()) {
                        finalPromise.set(NullArrayReply.INSTANCE);
                        BlockingList.removeOne(firstKey, one);
                    }
                });
            }

            return asyncReply;
        }

        var valueBytes = isLeft ? rl.removeFirst() : rl.removeLast();
        LGroup.saveRedisList(rl, firstSlotWithKeyHash, this, dictMap);

        var replies = new Reply[2];
        replies[0] = new BulkReply(firstKeyBytes);
        replies[1] = new BulkReply(valueBytes);
        return new MultiBulkReply(replies);
    }

    /**
     * Pop one value from the first non-empty key, starting at {@code fromIndex}.
     * Every key is read and written on the worker owning it, chained without blocking the current worker,
     * so the first key with a value wins and the following keys are never touched.
     *
     * @param fromIndex index of the first key to check
     * @param keys      all blocking keys
     * @param isLeft    take the first element when true, the last one otherwise
     * @return promise of the reply, null when all the checked keys are empty
     */
    private Promise<Reply> popOtherKeyAsync(int fromIndex, ArrayList<String> keys, boolean isLeft) {
        if (fromIndex >= keys.size()) {
            return Promise.of(null);
        }

        var keyBytes = Wal.keyBytes(keys.get(fromIndex));
        var slotWithKeyHash = slotWithKeyHashListParsed.get(fromIndex);
        return localPersist.oneSlot(slotWithKeyHash.slot()).asyncCall(() -> {
            var rl = LGroup.getRedisList(slotWithKeyHash, this);
            if (rl == null || rl.isEmpty()) {
                return (Reply) null;
            }

            var valueBytes = isLeft ? rl.removeFirst() : rl.removeLast();
            LGroup.saveRedisList(rl, slotWithKeyHash, this, dictMap);
            var replies = new Reply[2];
            replies[0] = new BulkReply(keyBytes);
            replies[1] = new BulkReply(valueBytes);
            return (Reply) new MultiBulkReply(replies);
        }).then(reply -> reply != null ? Promise.of(reply) : popOtherKeyAsync(fromIndex + 1, keys, isLeft));
    }

    /**
     * The distinct keys of a blocking command, redis registers a blocked client once per distinct key.
     *
     * @param slots the parsed slots of the command
     * @return the slots without the duplicate keys, the order is kept
     */
    private static ArrayList<SlotWithKeyHash> distinctSlots(ArrayList<SlotWithKeyHash> slots) {
        var distinct = new ArrayList<SlotWithKeyHash>();
        var seen = new HashSet<String>();
        for (var slotWithKeyHash : slots) {
            if (seen.add(slotWithKeyHash.rawKey())) {
                distinct.add(slotWithKeyHash);
            }
        }
        return distinct;
    }

    /**
     * Register one blocking promise on the worker owning each key.
     *
     * @param keys the blocking keys
     * @param one  the promise wrapper
     */
    private void addBlockingWaiter(ArrayList<String> keys, BlockingList.PromiseWithLeftOrRightAndCreatedTime one) {
        // redis registers a blocked client once per distinct key, BLPOP k k must not register it twice
        var seen = new HashSet<String>();
        for (int i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            if (!seen.add(key)) {
                continue;
            }
            var slot = slotWithKeyHashListParsed.get(i).slot();
            localPersist.oneSlot(slot).asyncExecute(() -> BlockingList.addOne(key, one));
        }
    }

    /**
     * Remove one blocking promise from the worker owning each key.
     *
     * @param keys the blocking keys
     * @param one  the promise wrapper
     */
    private void removeBlockingWaiter(ArrayList<String> keys, BlockingList.PromiseWithLeftOrRightAndCreatedTime one) {
        for (int i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            var slot = slotWithKeyHashListParsed.get(i).slot();
            localPersist.oneSlot(slot).asyncExecute(() -> BlockingList.removeOne(key, one));
        }
    }

    /**
     * Complete the blocking reply when a push landed between the emptiness check and the waiter
     * registration, the list is re-read on its owning worker.
     *
     * @param keys   the blocking keys
     * @param one    the promise wrapper
     * @param isLeft take the first element when true, the last one otherwise
     */
    private void recheckBlockingList(ArrayList<String> keys, BlockingList.PromiseWithLeftOrRightAndCreatedTime one,
                                     boolean isLeft) {
        for (int i = 0; i < keys.size(); i++) {
            var keyBytes = Wal.keyBytes(keys.get(i));
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
            localPersist.oneSlot(slotWithKeyHash.slot()).asyncExecute(() -> {
                completeIfListNotEmpty(slotWithKeyHash, keyBytes, this, dictMap, one, isLeft);
            });
        }
    }

    /**
     * Complete a pending blocking reply with one value when the list is not empty,
     * used to close the window between the emptiness check and the waiter registration.
     *
     * @param slotWithKeyHash slot and key hash of the blocking key
     * @param keyBytes        the blocking key
     * @param baseCommand     the command owning the storage access
     * @param dictMap         dictionary map used when the list is saved back
     * @param one             the promise wrapper
     * @param isLeft          take the first element when true, the last one otherwise
     * @return true when the promise was completed
     */
    @VisibleForTesting
    static boolean completeIfListNotEmpty(BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                          BaseCommand baseCommand, DictMap dictMap,
                                          BlockingList.PromiseWithLeftOrRightAndCreatedTime one, boolean isLeft) {
        if (one.settablePromise().isComplete()) {
            return false;
        }

        var rl = LGroup.getRedisList(slotWithKeyHash, baseCommand);
        if (rl == null || rl.isEmpty()) {
            return false;
        }

        // one-shot delivery arbiter: only the winner may pop, save and complete
        if (!one.tryMarkDelivered()) {
            return false;
        }

        BlockingList.removeOne(Wal.keyString(keyBytes), one);
        // a multi key block registers the waiter on every key, release all of them
        BlockingList.removeEverywhere(one);
        var valueBytes = isLeft ? rl.removeFirst() : rl.removeLast();

        var replies = new Reply[2];
        replies[0] = new BulkReply(keyBytes);
        replies[1] = new BulkReply(valueBytes);
        var won = one.settablePromise().trySet(new MultiBulkReply(replies));
        if (!won) {
            // unreachable while every completer honours the arbiter; safety net for a foreign setter
            if (isLeft) {
                rl.addFirst(valueBytes);
            } else {
                rl.addLast(valueBytes);
            }
        }
        LGroup.saveRedisList(rl, slotWithKeyHash, baseCommand, dictMap);
        return won;
    }

    private Reply blmpop() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        double timeoutSeconds;
        try {
            timeoutSeconds = Double.parseDouble(new String(data[1]));
        } catch (NumberFormatException e) {
            return new ErrorReply("timeout is not a float or out of range");
        }
        if (timeoutSeconds < 0) {
            return new ErrorReply("timeout is negative");
        }
        if (!Double.isFinite(timeoutSeconds)) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(data[2]));
        } catch (NumberFormatException e) {
            return new ErrorReply("numkeys should be greater than 0");
        }
        if (numKeys <= 0) {
            return new ErrorReply("numkeys should be greater than 0");
        }
        if (data.length < 3 + numKeys + 1) {
            return ErrorReply.FORMAT;
        }

        boolean isLeft;
        var direction = new String(data[3 + numKeys]).toLowerCase();
        if ("left".equals(direction)) {
            isLeft = true;
        } else if ("right".equals(direction)) {
            isLeft = false;
        } else {
            return ErrorReply.SYNTAX;
        }

        int count = 1;
        if (data.length > 4 + numKeys) {
            if (data.length == 4 + numKeys + 2 && "count".equalsIgnoreCase(new String(data[4 + numKeys]))) {
                try {
                    count = Integer.parseInt(new String(data[5 + numKeys]));
                } catch (NumberFormatException e) {
                    return new ErrorReply("count should be greater than 0");
                }
                if (count <= 0) {
                    return new ErrorReply("count should be greater than 0");
                }
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            if (data[3 + i].length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            keys.add(Wal.keyString(data[3 + i]));
        }

        // the first key is owned by the worker running this command, check it inline
        var firstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var firstCv = getCv(firstSlotWithKeyHash);
        if (firstCv != null) {
            if (!firstCv.isList()) {
                return ErrorReply.WRONG_TYPE;
            }
            var firstRl = RedisList.decode(getValueBytesByCv(firstCv, firstSlotWithKeyHash));
            if (!firstRl.isEmpty()) {
                return popMpopReply(firstSlotWithKeyHash, data[3], firstRl, isLeft, count);
            }
        }

        // all lists empty or missing — block
        final boolean isBlockIndefinitely = timeoutSeconds == 0.0;
        final int popCount = count;

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // the remaining keys are checked on the worker owning them, without blocking this one
        popOtherKeyMpopAsync(1, keys, isLeft, popCount).whenComplete((r, e) -> {
            if (e != null) {
                if (e instanceof TypeMismatchException) {
                    finalPromise.set(ErrorReply.WRONG_TYPE);
                } else {
                    finalPromise.setException(Utils.wrapException("blmpop", e));
                }
                return;
            }

            if (r != null) {
                finalPromise.set(r);
                return;
            }

            var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, socket, isLeft,
                    System.currentTimeMillis(), null, popCount, distinctSlots(slotWithKeyHashListParsed));
            var waiter = one;
            // register the waiter on every key owning worker, so a push to any of them can wake it up
            addBlockingWaiter(keys, waiter);
            // close the check then register window, a push may land between both steps
            recheckBlockingListMpop(keys, waiter, isLeft, popCount);

            if (!isBlockIndefinitely) {
                long timeoutMillis = (long) (timeoutSeconds * 1000);
                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutMillis, () -> {
                    // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                    if (one.tryMarkDelivered()) {
                        finalPromise.set(NullArrayReply.INSTANCE);
                        removeBlockingWaiter(keys, one);
                    }
                });
            }
        });

        if (finalPromise.isComplete()) {
            var exception = finalPromise.getException();
            if (exception != null) {
                return exception instanceof TypeMismatchException ? ErrorReply.WRONG_TYPE :
                        new ErrorReply(exception.getMessage());
            }
            // all the keys are owned by this worker, the reply is already known
            return finalPromise.getResult();
        }

        return asyncReply;
    }

    /**
     * Pop up to {@code count} values from the first non-empty key, starting at {@code fromIndex}.
     * Every key is read and written on the worker owning it, chained without blocking the current worker.
     *
     * @param fromIndex index of the first key to check
     * @param keys      all blocking keys
     * @param isLeft    take from the head when true, from the tail otherwise
     * @param count     maximum number of values to pop
     * @return promise of the reply, null when all the checked keys are empty
     */
    private Promise<Reply> popOtherKeyMpopAsync(int fromIndex, ArrayList<String> keys, boolean isLeft, int count) {
        if (fromIndex >= keys.size()) {
            return Promise.of(null);
        }

        var keyBytes = Wal.keyBytes(keys.get(fromIndex));
        var slotWithKeyHash = slotWithKeyHashListParsed.get(fromIndex);
        return localPersist.oneSlot(slotWithKeyHash.slot()).asyncCall(() -> {
            var rl = LGroup.getRedisList(slotWithKeyHash, this);
            if (rl == null || rl.isEmpty()) {
                return (Reply) null;
            }

            return popMpopReply(slotWithKeyHash, keyBytes, rl, isLeft, count);
        }).then(reply -> reply != null ? Promise.of(reply) : popOtherKeyMpopAsync(fromIndex + 1, keys, isLeft, count));
    }

    /**
     * Pop up to {@code count} values from a list and build the BLMPOP reply.
     *
     * @param slotWithKeyHash slot and key hash of the popped key
     * @param keyBytes        the popped key
     * @param rl              the list to pop from
     * @param isLeft          take from the head when true, from the tail otherwise
     * @param count           maximum number of values to pop
     * @return the BLMPOP reply
     */
    private Reply popMpopReply(BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes, RedisList rl,
                               boolean isLeft, int count) {
        return popMpopReply(this, dictMap, slotWithKeyHash, keyBytes, rl, isLeft, count);
    }

    private static Reply popMpopReply(BaseCommand baseCommand, DictMap dictMap,
                                      BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes, RedisList rl,
                                      boolean isLeft, int count) {
        int min = Math.min(count, rl.size());
        var valueReplies = new Reply[min];
        for (int j = 0; j < min; j++) {
            valueReplies[j] = new BulkReply(isLeft ? rl.removeFirst() : rl.removeLast());
        }
        LGroup.saveRedisList(rl, slotWithKeyHash, baseCommand, dictMap);

        var arr = new Reply[]{new BulkReply(keyBytes), new MultiBulkReply(valueReplies)};
        return new MultiBulkReply(arr);
    }

    /**
     * Complete the BLMPOP reply when a push landed between the emptiness check and the waiter
     * registration, the list is re-read on its owning worker.
     *
     * @param keys  the blocking keys
     * @param one   the promise wrapper
     * @param isLeft take from the head when true, from the tail otherwise
     * @param count maximum number of values to pop
     */
    private void recheckBlockingListMpop(ArrayList<String> keys, BlockingList.PromiseWithLeftOrRightAndCreatedTime one,
                                         boolean isLeft, int count) {
        for (int i = 0; i < keys.size(); i++) {
            var keyBytes = Wal.keyBytes(keys.get(i));
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
            localPersist.oneSlot(slotWithKeyHash.slot()).asyncExecute(() -> {
                completeMpopIfListNotEmpty(slotWithKeyHash, keyBytes, this, dictMap, one, isLeft, count);
            });
        }
    }

    /**
     * Complete a pending BLMPOP reply when the list is not empty, used to close the window between
     * the emptiness check and the waiter registration.
     *
     * @param slotWithKeyHash slot and key hash of the blocking key
     * @param keyBytes        the blocking key
     * @param baseCommand     the command owning the storage access
     * @param dictMap         dictionary map used when the list is saved back
     * @param one             the promise wrapper
     * @param isLeft          take from the head when true, from the tail otherwise
     * @param count           maximum number of values to pop
     * @return true when the promise was completed
     */
    @VisibleForTesting
    static boolean completeMpopIfListNotEmpty(BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                              BaseCommand baseCommand, DictMap dictMap,
                                              BlockingList.PromiseWithLeftOrRightAndCreatedTime one,
                                              boolean isLeft, int count) {
        if (one.settablePromise().isComplete()) {
            return false;
        }

        var rl = LGroup.getRedisList(slotWithKeyHash, baseCommand);
        if (rl == null || rl.isEmpty()) {
            return false;
        }

        // one-shot delivery arbiter: only the winner may pop, save and complete
        if (!one.tryMarkDelivered()) {
            return false;
        }

        BlockingList.removeOne(Wal.keyString(keyBytes), one);
        // a multi key block registers the waiter on every key, release all of them
        BlockingList.removeEverywhere(one);
        one.settablePromise().set(popMpopReply(baseCommand, dictMap, slotWithKeyHash, keyBytes, rl, isLeft, count));
        return true;
    }

    /**
     * BZPOPMIN key [key ...] timeout / BZPOPMAX key [key ...] timeout.
     * Pops one member from the first non-empty sorted set, blocks until one is available or the
     * timeout expires. The timeout in seconds comes last, 0 means block indefinitely.
     *
     * @param isMin pop the lowest scored member when true, the highest one otherwise
     * @return the reply: nil on timeout, or a [key, member, score] array
     */
    private Reply bzpop(boolean isMin) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 1; i < data.length - 1; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            keys.add(Wal.keyString(keyBytes));
        }

        var timeoutBytes = data[data.length - 1];
        double timeoutSeconds;
        try {
            timeoutSeconds = Double.parseDouble(new String(timeoutBytes));
        } catch (NumberFormatException e) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0) {
            return new ErrorReply("timeout is negative");
        }
        if (!Double.isFinite(timeoutSeconds)) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        boolean isBlockIndefinitely = timeoutSeconds == 0.0;

        var firstKeyBytes = data[1];
        var firstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        // the first key is owned by the worker running this command, check it inline
        var firstCv = getCv(firstSlotWithKeyHash);
        if (firstCv != null) {
            if (!firstCv.isZSet()) {
                return ErrorReply.WRONG_TYPE;
            }
            var firstRz = RedisZSet.decode(getValueBytesByCv(firstCv, firstSlotWithKeyHash));
            if (!firstRz.isEmpty()) {
                return popZSetOneReply(this, dictMap, firstSlotWithKeyHash, firstKeyBytes, firstRz, isMin);
            }
        }

        if (keys.size() > 1) {
            // performance bad, get all other values
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            var one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(finalPromise, socket, isMin,
                    System.currentTimeMillis(), 0, distinctSlots(slotWithKeyHashListParsed));

            // each key is checked on the worker owning it, never blocking the current worker
            // In production, completions bounce back to this first key's worker reactor,
            // which also schedules the timeout and settles the reply.
            popOtherKeyZAsync(1, keys, isMin).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("{} error={}", isMin ? "bzpopmin" : "bzpopmax", e.getMessage());
                    if (e instanceof TypeMismatchException) {
                        // the key holds another type, answer the redis wrong type error
                        finalPromise.set(ErrorReply.WRONG_TYPE);
                    } else {
                        finalPromise.setException(Utils.wrapException(isMin ? "bzpopmin" : "bzpopmax", e));
                    }
                    return;
                }

                if (r != null) {
                    finalPromise.set(r);
                    return;
                }

                // register the waiter on every key owning worker, so a member added to any of them can wake it up
                addZSetBlockingWaiter(keys, one);
                // close the check then register window, a member may land between both steps
                recheckBlockingZSet(keys, one);

                if (!isBlockIndefinitely) {
                    long timeoutMillis = (long) (timeoutSeconds * 1000);
                    var reactor = Reactor.getCurrentReactor();
                    reactor.delay(timeoutMillis, () -> {
                        // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                        if (one.tryMarkDelivered()) {
                            finalPromise.set(NullArrayReply.INSTANCE);
                            BlockingZSet.removeEverywhere(one);
                        }
                    });
                }
            });

            return asyncReply;
        }

        // only one key
        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var firstKey = keys.getFirst();
        var one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(finalPromise, socket, isMin,
                System.currentTimeMillis(), 0, slotWithKeyHashListParsed);
        BlockingZSet.addOne(firstKey, one);

        if (!isBlockIndefinitely) {
            long timeoutMillis = (long) (timeoutSeconds * 1000);
            var reactor = Reactor.getCurrentReactor();
            reactor.delay(timeoutMillis, () -> {
                // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                if (one.tryMarkDelivered()) {
                    finalPromise.set(NullArrayReply.INSTANCE);
                    BlockingZSet.removeEverywhere(one);
                }
            });
        }

        return asyncReply;
    }

    /**
     * BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count].
     * Pops up to count members from the first non-empty sorted set, blocks until one is available
     * or the timeout expires. The timeout in seconds comes first, 0 means block indefinitely.
     *
     * @return the reply: nil on timeout, or a [key, [[member, score], ...]] array
     */
    private Reply bzmpop() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        double timeoutSeconds;
        try {
            timeoutSeconds = Double.parseDouble(new String(data[1]));
        } catch (NumberFormatException e) {
            return new ErrorReply("timeout is not a float or out of range");
        }
        if (timeoutSeconds < 0) {
            return new ErrorReply("timeout is negative");
        }
        if (!Double.isFinite(timeoutSeconds)) {
            return new ErrorReply("timeout is not a float or out of range");
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(data[2]));
        } catch (NumberFormatException e) {
            return new ErrorReply("numkeys should be greater than 0");
        }
        if (numKeys <= 0) {
            return new ErrorReply("numkeys should be greater than 0");
        }
        if (data.length < 3 + numKeys + 1) {
            return ErrorReply.FORMAT;
        }

        boolean isMin;
        var direction = new String(data[3 + numKeys]).toLowerCase();
        if ("min".equals(direction)) {
            isMin = true;
        } else if ("max".equals(direction)) {
            isMin = false;
        } else {
            return ErrorReply.SYNTAX;
        }

        int count = 1;
        if (data.length > 4 + numKeys) {
            if (data.length == 4 + numKeys + 2 && "count".equalsIgnoreCase(new String(data[4 + numKeys]))) {
                try {
                    count = Integer.parseInt(new String(data[5 + numKeys]));
                } catch (NumberFormatException e) {
                    return new ErrorReply("count should be greater than 0");
                }
                if (count <= 0) {
                    return new ErrorReply("count should be greater than 0");
                }
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            if (data[3 + i].length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            keys.add(Wal.keyString(data[3 + i]));
        }

        // the first key is owned by the worker running this command, check it inline
        var firstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var firstCv = getCv(firstSlotWithKeyHash);
        if (firstCv != null) {
            if (!firstCv.isZSet()) {
                return ErrorReply.WRONG_TYPE;
            }
            var firstRz = RedisZSet.decode(getValueBytesByCv(firstCv, firstSlotWithKeyHash));
            if (!firstRz.isEmpty()) {
                return popZSetValuesReply(this, dictMap, firstSlotWithKeyHash, data[3], firstRz, isMin, count);
            }
        }

        // all sorted sets empty or missing — block
        final boolean isBlockIndefinitely = timeoutSeconds == 0.0;
        final int popCount = count;

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // the remaining keys are checked on the worker owning them, without blocking this one
        popOtherKeyZMpopAsync(1, keys, isMin, popCount).whenComplete((r, e) -> {
            if (e != null) {
                if (e instanceof TypeMismatchException) {
                    // the key holds another type, answer the redis wrong type error
                    finalPromise.set(ErrorReply.WRONG_TYPE);
                } else {
                    finalPromise.setException(Utils.wrapException("bzmpop", e));
                }
                return;
            }

            if (r != null) {
                finalPromise.set(r);
                return;
            }

            var one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(finalPromise, socket, isMin,
                    System.currentTimeMillis(), popCount, distinctSlots(slotWithKeyHashListParsed));
            var waiter = one;
            // register the waiter on every key owning worker, so a member added to any of them can wake it up
            addZSetBlockingWaiter(keys, waiter);
            // close the check then register window, a member may land between both steps
            recheckBlockingZSet(keys, waiter);

            if (!isBlockIndefinitely) {
                long timeoutMillis = (long) (timeoutSeconds * 1000);
                var reactor = Reactor.getCurrentReactor();
                reactor.delay(timeoutMillis, () -> {
                    // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                    if (one.tryMarkDelivered()) {
                        finalPromise.set(NullArrayReply.INSTANCE);
                        BlockingZSet.removeEverywhere(one);
                    }
                });
            }
        });

        if (finalPromise.isComplete()) {
            var exception = finalPromise.getException();
            if (exception != null) {
                return exception instanceof TypeMismatchException ? ErrorReply.WRONG_TYPE :
                        new ErrorReply(exception.getMessage());
            }
            // all the keys are owned by this worker, the reply is already known
            return finalPromise.getResult();
        }

        return asyncReply;
    }

    /**
     * Pop one member from the first non-empty sorted set, starting at {@code fromIndex}.
     * Every key is read and written on the worker owning it, chained without blocking the current worker,
     * so the first key with a member wins and the following keys are never touched.
     *
     * @param fromIndex index of the first key to check
     * @param keys      all blocking keys
     * @param isMin     pop the lowest scored member when true, the highest one otherwise
     * @return promise of the reply, null when all the checked keys are empty
     */
    private Promise<Reply> popOtherKeyZAsync(int fromIndex, ArrayList<String> keys, boolean isMin) {
        if (fromIndex >= keys.size()) {
            return Promise.of(null);
        }

        var keyBytes = Wal.keyBytes(keys.get(fromIndex));
        var slotWithKeyHash = slotWithKeyHashListParsed.get(fromIndex);
        return localPersist.oneSlot(slotWithKeyHash.slot()).asyncCall(() -> {
            var rz = ZGroup.getRedisZSet(slotWithKeyHash, this);
            if (rz == null || rz.isEmpty()) {
                return (Reply) null;
            }

            return popZSetOneReply(this, dictMap, slotWithKeyHash, keyBytes, rz, isMin);
        }).then(reply -> reply != null ? Promise.of(reply) : popOtherKeyZAsync(fromIndex + 1, keys, isMin));
    }

    /**
     * Pop up to {@code count} members from the first non-empty sorted set, starting at {@code fromIndex}.
     * Every key is read and written on the worker owning it, chained without blocking the current worker.
     *
     * @param fromIndex index of the first key to check
     * @param keys      all blocking keys
     * @param isMin     pop from the low end when true, from the high end otherwise
     * @param count     maximum number of members to pop
     * @return promise of the reply, null when all the checked keys are empty
     */
    private Promise<Reply> popOtherKeyZMpopAsync(int fromIndex, ArrayList<String> keys, boolean isMin, int count) {
        if (fromIndex >= keys.size()) {
            return Promise.of(null);
        }

        var keyBytes = Wal.keyBytes(keys.get(fromIndex));
        var slotWithKeyHash = slotWithKeyHashListParsed.get(fromIndex);
        return localPersist.oneSlot(slotWithKeyHash.slot()).asyncCall(() -> {
            var rz = ZGroup.getRedisZSet(slotWithKeyHash, this);
            if (rz == null || rz.isEmpty()) {
                return (Reply) null;
            }

            return popZSetValuesReply(this, dictMap, slotWithKeyHash, keyBytes, rz, isMin, count);
        }).then(reply -> reply != null ? Promise.of(reply) : popOtherKeyZMpopAsync(fromIndex + 1, keys, isMin, count));
    }

    /**
     * Pop one member from a sorted set and build the BZPOPMIN/BZPOPMAX reply: [key, member, score].
     *
     * @param baseCommand     the base command owning the storage access
     * @param dictMap         dictionary map used when the sorted set is saved back
     * @param slotWithKeyHash slot and key hash of the popped key
     * @param keyBytes        the popped key
     * @param rz              the sorted set to pop from
     * @param isMin           pop the lowest scored member when true, the highest one otherwise
     * @return the BZPOPMIN/BZPOPMAX reply
     */
    private static Reply popZSetOneReply(BaseCommand baseCommand, DictMap dictMap,
                                         BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                         RedisZSet rz, boolean isMin) {
        var sv = isMin ? rz.pollFirst() : rz.pollLast();
        ZGroup.saveRedisZSet(rz, slotWithKeyHash, baseCommand, dictMap);

        var replies = new Reply[3];
        replies[0] = new BulkReply(keyBytes);
        replies[1] = new BulkReply(sv.member());
        replies[2] = new BulkReply(sv.score());
        return new MultiBulkReply(replies);
    }

    /**
     * Pop up to {@code count} members from a sorted set and build the BZMPOP reply:
     * [key, [[member, score], ...]].
     *
     * @param baseCommand     the base command owning the storage access
     * @param dictMap         dictionary map used when the sorted set is saved back
     * @param slotWithKeyHash slot and key hash of the popped key
     * @param keyBytes        the popped key
     * @param rz              the sorted set to pop from
     * @param isMin           pop from the low end when true, from the high end otherwise
     * @param count           maximum number of members to pop
     * @return the BZMPOP reply
     */
    private static Reply popZSetValuesReply(BaseCommand baseCommand, DictMap dictMap,
                                            BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                            RedisZSet rz, boolean isMin, int count) {
        int min = Math.min(count, rz.size());
        var valueReplies = new Reply[min];
        for (int j = 0; j < min; j++) {
            var sv = isMin ? rz.pollFirst() : rz.pollLast();
            // redis replies with one array per member/score pair, even for a single member
            valueReplies[j] = new MultiBulkReply(new Reply[]{new BulkReply(sv.member()), new BulkReply(sv.score())});
        }
        ZGroup.saveRedisZSet(rz, slotWithKeyHash, baseCommand, dictMap);

        var arr = new Reply[]{new BulkReply(keyBytes), new MultiBulkReply(valueReplies)};
        return new MultiBulkReply(arr);
    }

    /**
     * Register one blocking sorted set promise on the worker owning each key.
     *
     * @param keys the blocking keys
     * @param one  the promise wrapper
     */
    private void addZSetBlockingWaiter(ArrayList<String> keys, BlockingZSet.PromiseWithMinOrMaxAndCreatedTime one) {
        // redis registers a blocked client once per distinct key
        var seen = new HashSet<String>();
        for (int i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            if (!seen.add(key)) {
                continue;
            }
            var slot = slotWithKeyHashListParsed.get(i).slot();
            localPersist.oneSlot(slot).asyncExecute(() -> BlockingZSet.addOne(key, one));
        }
    }

    /**
     * Complete the blocking reply when a member landed between the emptiness check and the waiter
     * registration, the sorted set is re-read on its owning worker.
     *
     * @param keys the blocking keys
     * @param one  the promise wrapper
     */
    private void recheckBlockingZSet(ArrayList<String> keys, BlockingZSet.PromiseWithMinOrMaxAndCreatedTime one) {
        for (int i = 0; i < keys.size(); i++) {
            var keyBytes = Wal.keyBytes(keys.get(i));
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
            localPersist.oneSlot(slotWithKeyHash.slot()).asyncExecute(() -> {
                completeIfZSetNotEmpty(slotWithKeyHash, keyBytes, this, dictMap, one);
            });
        }
    }

    /**
     * Complete a pending blocking sorted set reply when the sorted set is not empty,
     * used to close the window between the emptiness check and the waiter registration and to wake
     * waiters after a save.
     *
     * @param slotWithKeyHash slot and key hash of the blocking key
     * @param keyBytes        the blocking key
     * @param baseCommand     the command owning the storage access
     * @param dictMap         dictionary map used when the sorted set is saved back
     * @param one             the promise wrapper
     * @return true when the promise was completed
     */
    @VisibleForTesting
    static boolean completeIfZSetNotEmpty(BaseCommand.SlotWithKeyHash slotWithKeyHash, byte[] keyBytes,
                                           BaseCommand baseCommand, DictMap dictMap,
                                           BlockingZSet.PromiseWithMinOrMaxAndCreatedTime one) {
        if (one.settablePromise().isComplete()) {
            return false;
        }

        RedisZSet rz;
        try {
            rz = ZGroup.getRedisZSet(slotWithKeyHash, baseCommand);
        } catch (TypeMismatchException e) {
            // the key changed to another type while waiting, fail the blocked client instead of hanging
            if (!one.tryMarkDelivered()) {
                return false;
            }
            BlockingZSet.removeOne(Wal.keyString(keyBytes), one);
            // a multi key block registers the waiter on every key, release all of them
            BlockingZSet.removeEverywhere(one);
            one.settablePromise().set(ErrorReply.WRONG_TYPE);
            return true;
        }
        if (rz == null || rz.isEmpty()) {
            return false;
        }

        // one-shot delivery arbiter: only the winner may pop, save and complete
        if (!one.tryMarkDelivered()) {
            // a waiter that lost the race consumes no member, drop it as stale
            BlockingZSet.removeOne(Wal.keyString(keyBytes), one);
            return false;
        }

        BlockingZSet.removeOne(Wal.keyString(keyBytes), one);
        // a multi key block registers the waiter on every key, release all of them
        BlockingZSet.removeEverywhere(one);
        if (one.mpopCount() > 0) {
            one.settablePromise().set(popZSetValuesReply(baseCommand, dictMap, slotWithKeyHash, keyBytes, rz, one.isMin(), one.mpopCount()));
        } else {
            one.settablePromise().set(popZSetOneReply(baseCommand, dictMap, slotWithKeyHash, keyBytes, rz, one.isMin()));
        }
        return true;
    }
}
