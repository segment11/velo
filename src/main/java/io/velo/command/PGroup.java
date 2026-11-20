package io.velo.command;

import com.github.luben.zstd.Zstd;
import com.github.prasanthj.hll.HyperLogLog;
import com.github.prasanthj.hll.HyperLogLogUtils;
import com.google.common.annotations.VisibleForTesting;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.TypeMismatchException;
import io.velo.Utils;
import io.velo.persist.LocalPersist;
import io.velo.reply.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles Redis commands starting with letter 'P'.
 * This includes commands like PERSIST, PEXPIRE, PFADD, PFMERGE.
 */
public class PGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "persist", 2,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@write", "@fast"),
                "generic",
                "Remove the expiration from a key.",
                "2.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pexpire", -3,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@write", "@fast"),
                "generic",
                "Set a key's time to live in milliseconds.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pexpireat", -3,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@write", "@fast"),
                "generic",
                "Set the expiration for a key as a UNIX milliseconds timestamp.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pexpiretime", 2,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@read", "@fast"),
                "generic",
                "Get the expiration time of a key in milliseconds.",
                "7.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pfadd", -2,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hyperloglog", "@fast"),
                "hyperloglog",
                "Adds the specified elements to the specified HyperLogLog.",
                "2.8.9", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pfcount", -2,
                Set.of("readonly"),
                1, -1, 1,
                Set.of("@read", "@hyperloglog", "@slow"),
                "hyperloglog",
                "Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).",
                "2.8.9", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "pfmerge", -2,
                Set.of("write", "denyoom"),
                1, -1, 1,
                Set.of("@write", "@hyperloglog", "@slow"),
                "hyperloglog",
                "Merge N different HyperLogLogs into a single one.",
                "2.8.9", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "pfdebug", 3,
                Set.of("write", "denyoom", "admin"),
                2, 2, 1,
                Set.of("@write", "@hyperloglog", "@admin", "@dangerous", "@slow"),
                "hyperloglog",
                "Internal commands for debugging HyperLogLog values.",
                "2.8.9", "N/A"));
        CommandRegistry.register(new CommandEntry(
                "pfselftest", 1,
                Set.of("admin"),
                0, 0, 0,
                Set.of("@hyperloglog", "@admin", "@dangerous", "@slow"),
                "hyperloglog",
                "An internal command for testing HyperLogLog values.",
                "2.8.9", "N/A"));
        CommandRegistry.register(new CommandEntry(
                "psetex", 4,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@write", "@string", "@slow"),
                "string",
                "Set the value and expiration in milliseconds of a key.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "psubscribe", -2,
                Set.of("pubsub", "noscript", "loading", "stale", "denyoom"),
                0, 0, 0,
                Set.of("@pubsub", "@slow"),
                "pubsub",
                "Listen for messages published to channels matching the given patterns.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "pttl", 2,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@read", "@fast"),
                "generic",
                "Get the time to live for a key in milliseconds.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "publish", 3,
                Set.of("pubsub", "loading", "stale", "fast"),
                0, 0, 0,
                Set.of("@pubsub", "@fast"),
                "pubsub",
                "Post a message to a channel.",
                "2.0.0", "O(N+M)"));
        CommandRegistry.register(new CommandEntry(
                "pubsub", -2,
                Set.of("pubsub", "loading", "stale"),
                0, 0, 0,
                Set.of("@pubsub", "@slow"),
                "pubsub",
                "A container for pubsub commands.",
                "2.8.0", "Depends on subcommand."));
        CommandRegistry.register(new CommandEntry(
                "punsubscribe", -1,
                Set.of("pubsub", "noscript", "loading", "stale"),
                0, 0, 0,
                Set.of("@pubsub", "@slow"),
                "pubsub",
                "Stop listening for messages posted to channels matching the given patterns.",
                "2.0.0", "O(N)"));
    }

    private static HyperLogLog emptyHll() {
        return HyperLogLog.builder()
                .setEncoding(HyperLogLog.EncodingType.SPARSE)
                .build();
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public PGroup(String cmd, byte[][] data, ITcpSocket socket) {
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

        if ("pexpire".equals(cmd) || "pexpireat".equals(cmd)) {
            if (data.length != 3 && data.length != 4) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pexpiretime".equals(cmd) || "pttl".equals(cmd) || "persist".equals(cmd)) {
            if (data.length != 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pfadd".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pfcount".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("pfmerge".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("pfdebug".equals(cmd)) {
            // eg. pfdebug encoding key, the key is the third argument
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[2], slotNumber));
            return slotWithKeyHashList;
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    /**
     * Handles the command and returns the reply.
     *
     * @return the reply for this command
     */
    @Override
    public Reply handle() {
        try {
            return handleCommand();
        } catch (TypeMismatchException e) {
            // the key holds a value of another type, redis answers WRONGTYPE
            return ErrorReply.WRONG_TYPE_HLL;
        }
    }

    private Reply handleCommand() {
        if ("persist".equals(cmd)) {
            return persist();
        }

        if ("pexpire".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(false, true);
        }

        if ("pexpireat".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(true, true);
        }

        if ("pexpiretime".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expiretime(true);
        }

        if ("pfadd".equals(cmd)) {
            return pfadd();
        }

        if ("pfcount".equals(cmd)) {
            return pfcount();
        }

        if ("pfmerge".equals(cmd)) {
            return pfmerge();
        }

        if ("pfdebug".equals(cmd)) {
            return pfdebug();
        }

        if ("pfselftest".equals(cmd)) {
            return pfselftest();
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[3], "px".getBytes(), data[2]};
            var sGroup = new SGroup(cmd, dd, socket);
            sGroup.from(this);
            return sGroup.set(dd);
        }

        if ("psubscribe".equals(cmd)) {
            var sGroup = new SGroup(cmd, data, socket);
            sGroup.from(this);
            return sGroup.subscribe(true);
        }

        if ("pttl".equals(cmd)) {
            var tGroup = new TGroup(cmd, data, socket);
            tGroup.from(this);
            return tGroup.ttl(true);
        }

        if ("publish".equals(cmd)) {
            return publish(data, socket);
        }

        if ("pubsub".equals(cmd)) {
            return pubsub();
        }

        if ("punsubscribe".equals(cmd)) {
            var uGroup = new UGroup(cmd, data, socket);
            uGroup.from(this);
            return uGroup.unsubscribe(true);
        }

        // the command is not implemented, redis answers an unknown command error
        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }

    private Reply persist() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        if (cv.getExpireAt() == CompressedValue.NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }

        // a new sequence number is needed, the replication compares the sequences to know
        // which value is the newer one, the expire time change is a new write
        cv.setSeq(snowFlake.nextId());
        cv.setExpireAt(CompressedValue.NO_EXPIRE);
        putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);
        return IntegerReply.REPLY_1;
    }

    private void saveHll(SlotWithKeyHash s, HyperLogLog hll, long expireAt) throws IOException {
        saveHllBytes(s, serializeHll(hll), expireAt);
    }

    private static byte[] serializeHll(HyperLogLog hll) throws IOException {
        var os = new ByteArrayOutputStream();
        HyperLogLogUtils.serializeHLL(os, hll);
        return os.toByteArray();
    }

    private void saveHllBytes(SlotWithKeyHash s, byte[] encoded, long expireAt) throws IOException {
        var beginT = System.nanoTime();
        var compressed = Zstd.compress(encoded);
        var costT = (System.nanoTime() - beginT) / 1000;

        var cv = new CompressedValue();
        cv.setSeq(snowFlake.nextId());
        cv.setDictSeqOrSpType(CompressedValue.SP_TYPE_HLL);
        cv.setKeyHash(s.keyHash());
        cv.setExpireAt(expireAt);
        cv.setCompressedData(compressed);
        putToOneSlot(s.slot(), s, cv);

        // stats
        compressStats.totalInputLength += encoded.length;
        compressStats.compressedCount++;
        compressStats.compressedTotalLength += compressed.length;
        compressStats.compressedCostTimeTotalUs += costT;
    }

    /**
     * The HyperLogLog of a key and the serialized bytes it was loaded from.
     *
     * @param hll      the HyperLogLog, null when the key does not exist
     * @param bytes    the serialized bytes of the stored HyperLogLog, null when the key does not exist
     * @param expireAt the key expire time in milliseconds, or {@link CompressedValue#NO_EXPIRE} when missing or no expire
     */
    private record HllWithBytes(HyperLogLog hll, byte[] bytes, long expireAt) {
    }

    private HllWithBytes getHllWithBytes(SlotWithKeyHash s) {
        var cv = getCv(s);
        if (cv == null) {
            return new HllWithBytes(null, null, CompressedValue.NO_EXPIRE);
        }

        if (!cv.isHll()) {
            throw new TypeMismatchException(ErrorReply.WRONG_TYPE_HLL.getMessage());
        }

        var beginT = System.nanoTime();
        var decompressed = cv.decompress(Dict.SELF_ZSTD_DICT);
        var costT = System.nanoTime() - beginT;

        // stats
        compressStats.decompressedCount++;
        compressStats.decompressedCostTimeTotalNs += costT;

        var is = new ByteArrayInputStream(decompressed);
        try {
            return new HllWithBytes(HyperLogLogUtils.deserializeHLL(is), decompressed, cv.getExpireAt());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HyperLogLog getHll(SlotWithKeyHash s) {
        return getHllWithBytes(s).hll();
    }

    private Reply pfadd() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var hllWithBytes = getHllWithBytes(slotWithKeyHash);
        var hll = hllWithBytes.hll();
        if (hll == null) {
            hll = emptyHll();
        }

        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            hll.addBytes(itemBytes);
        }

        // redis answers 1 when at least one HyperLogLog internal register was altered,
        // the estimated cardinality may stay the same, so compare the serialized state
        byte[] hllBytes;
        try {
            hllBytes = serializeHll(hll);
        } catch (IOException e) {
            return new ErrorReply(e.getMessage());
        }
        var isChanged = hllWithBytes.bytes() == null || !Arrays.equals(hllWithBytes.bytes(), hllBytes);

        if (isChanged) {
            try {
                saveHllBytes(slotWithKeyHash, hllBytes, hllWithBytes.expireAt());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        return isChanged ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    private void mergeHllInto(HyperLogLog dstHll, HyperLogLog srcHll) {
        if (srcHll != null) {
            dstHll.merge(srcHll);
        }
    }

    /**
     * Reads the HyperLogLogs of the given keys, must be called on the owner thread of their slot.
     * The type mismatch is not thrown, so it never escapes into the slot worker event loop.
     *
     * @param subList the keys to read
     * @return the HyperLogLogs, null when one of the keys holds a value of another type
     */
    private ArrayList<HyperLogLog> readHllList(List<MGroup.IndexAndSlotWithKeyHash> subList) {
        var hllList = new ArrayList<HyperLogLog>();
        for (var one : subList) {
            try {
                hllList.add(getHll(one.slotWithKeyHash()));
            } catch (TypeMismatchException e) {
                return null;
            }
        }
        return hllList;
    }

    private Reply pfcount() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        assert data.length - 1 == slotWithKeyHashListParsed.size();
        if (!isCrossRequestWorker) {
            var merged = emptyHll();
            for (var i = 1; i < data.length; i++) {
                var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
                mergeHllInto(merged, getHll(slotWithKeyHash));
            }
            return new IntegerReply(merged.count());
        }

        ArrayList<MGroup.IndexAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new MGroup.IndexAndSlotWithKeyHash(j, slotWithKeyHash));
        }

        ArrayList<Promise<ArrayList<HyperLogLog>>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> readHllList(subList));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("pfcount error={}", e.getMessage());
                finalPromise.setException(Utils.wrapException("pfcount", e));
                return;
            }

            var merged = emptyHll();
            for (var p : promises) {
                var hllList = p.getResult();
                if (hllList == null) {
                    // one of the keys holds a value of another type, redis answers WRONGTYPE
                    finalPromise.set(ErrorReply.WRONG_TYPE_HLL);
                    return;
                }
                for (var hll : hllList) {
                    mergeHllInto(merged, hll);
                }
            }
            finalPromise.set(new IntegerReply(merged.count()));
        });

        return asyncReply;
    }

    private Reply pfmerge() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var dstS = slotWithKeyHashListParsed.getFirst();

        if (!isCrossRequestWorker) {
            var dstHllWithBytes = getHllWithBytes(dstS);
            var dstHll = dstHllWithBytes.hll();
            if (dstHll == null) {
                dstHll = emptyHll();
            }
            for (var i = 2; i < data.length; i++) {
                var srcS = slotWithKeyHashListParsed.get(i - 1);
                mergeHllInto(dstHll, getHll(srcS));
            }

            try {
                saveHll(dstS, dstHll, dstHllWithBytes.expireAt());
                return OKReply.INSTANCE;
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        ArrayList<MGroup.IndexAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 2, j = 0; i < data.length; i++, j++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
            list.add(new MGroup.IndexAndSlotWithKeyHash(j, slotWithKeyHash));
        }

        ArrayList<Promise<ArrayList<HyperLogLog>>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> readHllList(subList));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("pfmerge error={}", e.getMessage());
                finalPromise.setException(Utils.wrapException("pfmerge", e));
                return;
            }

            try {
                var srcHllList = new ArrayList<HyperLogLog>();
                for (var p : promises) {
                    var hllList = p.getResult();
                    if (hllList == null) {
                        // one of the source keys holds a value of another type, redis answers WRONGTYPE
                        finalPromise.set(ErrorReply.WRONG_TYPE_HLL);
                        return;
                    }
                    srcHllList.addAll(hllList);
                }

                // The destination key may only be touched on its owning slot worker thread.
                // On the reactor path (production: handle() runs on the dst slot worker and
                // Promise.ofFuture bounces every source promise completion back to that reactor),
                // this callback already runs on the dst worker thread and asyncCall executes inline.
                // On the non-reactor fallback path (no bounce, e.g. tests calling execute() directly),
                // this callback runs on a source slot worker thread, so asyncCall submits the merge
                // to the dst slot eventloop instead — see bug 84.1.
                var dstOneSlot = localPersist.oneSlot(dstS.slot());
                dstOneSlot.asyncCall(() -> {
                    HllWithBytes dstHllWithBytes;
                    try {
                        dstHllWithBytes = getHllWithBytes(dstS);
                    } catch (TypeMismatchException e1) {
                        // the destination holds a value of another type, redis answers WRONGTYPE
                        return Boolean.FALSE;
                    }
                    var dstHll = dstHllWithBytes.hll();
                    if (dstHll == null) {
                        dstHll = emptyHll();
                    }
                    for (var hll : srcHllList) {
                        mergeHllInto(dstHll, hll);
                    }
                    saveHll(dstS, dstHll, dstHllWithBytes.expireAt());
                    return Boolean.TRUE;
                }).whenComplete((r1, e1) -> {
                    if (e1 != null) {
                        finalPromise.setException(Utils.wrapException("pfmerge", e1));
                        return;
                    }
                    if (Boolean.FALSE.equals(r1)) {
                        finalPromise.set(ErrorReply.WRONG_TYPE_HLL);
                        return;
                    }
                    finalPromise.set(OKReply.INSTANCE);
                });
            } catch (Exception e1) {
                log.error("pfmerge error={}", e1.getMessage());
                finalPromise.setException(Utils.wrapException("pfmerge", e1));
            }
        });

        return asyncReply;
    }

    /**
     * Converts a HyperLogLog to the dense encoding. The library's own sparse to dense register
     * conversion only runs through the merge of a dense HyperLogLog, the same path a pfmerge into
     * a dense destination takes. An already dense HyperLogLog is returned as is.
     *
     * @param hll the HyperLogLog to convert
     * @return the HyperLogLog with the dense encoding
     */
    @VisibleForTesting
    static HyperLogLog toDenseHll(HyperLogLog hll) {
        if (hll.getEncoding() == HyperLogLog.EncodingType.DENSE) {
            return hll;
        }
        var dense = HyperLogLog.builder().setEncoding(HyperLogLog.EncodingType.DENSE).build();
        dense.merge(hll);
        return dense;
    }

    /**
     * Renders the register state in the redis sparse opcode format: {@code z:<len>} for a zero run
     * of at most 64 registers, {@code Z:<len>} for a longer zero run and {@code v:<value>,<len>}
     * for runs of equal nonzero values, the run length capped at 4 as the redis VAL opcode does.
     *
     * @param registers the dense register array, one byte per register
     * @return the decoded opcode stream, items separated by a space
     */
    @VisibleForTesting
    static String decodeSparseRegisters(byte[] registers) {
        var sb = new StringBuilder();
        int i = 0;
        while (i < registers.length) {
            int value = registers[i] & 0xff;
            int j = i;
            while (j < registers.length && (registers[j] & 0xff) == value) {
                j++;
            }
            int run = j - i;
            if (value == 0) {
                sb.append(run <= 64 ? "z:" : "Z:").append(run).append(' ');
            } else {
                while (run > 0) {
                    int len = Math.min(run, 4);
                    sb.append("v:").append(value).append(',').append(len).append(' ');
                    run -= len;
                }
            }
            i = j;
        }
        return sb.isEmpty() ? "" : sb.substring(0, sb.length() - 1);
    }

    private Reply pfdebug() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var subCmd = new String(data[1]);

        // redis resolves the key and type checks it before dispatching the subcommand
        var s = slotWithKeyHashListParsed.getFirst();
        HllWithBytes hllWithBytes;
        try {
            hllWithBytes = getHllWithBytes(s);
        } catch (TypeMismatchException e) {
            return ErrorReply.WRONG_TYPE_HLL;
        }
        var hll = hllWithBytes.hll();
        if (hll == null) {
            return new ErrorReply("The specified key does not exist");
        }

        if ("encoding".equalsIgnoreCase(subCmd)) {
            var encoding = hll.getEncoding() == HyperLogLog.EncodingType.DENSE ? "dense" : "sparse";
            return new BulkReply(encoding);
        }

        if ("todense".equalsIgnoreCase(subCmd)) {
            if (hll.getEncoding() == HyperLogLog.EncodingType.DENSE) {
                return IntegerReply.REPLY_0;
            }
            try {
                saveHll(s, toDenseHll(hll), hllWithBytes.expireAt());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
            return IntegerReply.REPLY_1;
        }

        if ("getreg".equalsIgnoreCase(subCmd)) {
            var dense = toDenseHll(hll);
            if (dense != hll) {
                // redis persists the sparse to dense conversion done by getreg
                try {
                    saveHll(s, dense, hllWithBytes.expireAt());
                } catch (IOException e) {
                    return new ErrorReply(e.getMessage());
                }
            }
            var registers = dense.getHLLDenseRegister().getRegister();
            var replies = new Reply[registers.length];
            for (int i = 0; i < registers.length; i++) {
                replies[i] = new IntegerReply(registers[i] & 0xff);
            }
            return new MultiBulkReply(replies);
        }

        if ("decode".equalsIgnoreCase(subCmd)) {
            if (hll.getEncoding() == HyperLogLog.EncodingType.DENSE) {
                return new ErrorReply("HLL encoding is not sparse");
            }

            // the register state rendered in the redis opcode format, the same registers getreg
            // reports, the sparse map itself is not stored in the redis opcode layout
            var registers = toDenseHll(hll).getHLLDenseRegister().getRegister();
            return new BulkReply(decodeSparseRegisters(registers));
        }

        return new ErrorReply("Unknown PFDEBUG subcommand '" + subCmd + "'");
    }

    /** The register count of the default HyperLogLog, 2^14, the same as redis. */
    private static final int SELF_TEST_REGISTERS = 1 << 14;

    /** The number of unique elements the self test adds, redis runs up to 10000000 which is too heavy. */
    private static final long SELF_TEST_MAX_ELEMENTS = 100_000;

    /** The fixed random seed keeps the self test result deterministic. */
    private static final long SELF_TEST_SEED = 0xadc83b19L;

    /**
     * Checks the approximation error bound of the self test, redis allows an absolute error of
     * at most ceil(6 * relerr * checkpoint) with the standard error 1.04/sqrt(registers), the
     * checkpoint 10 is special-cased to one unit of error as redis does.
     *
     * @param checkpoint the number of unique elements added
     * @param estimate   the estimated cardinality
     * @return null when the estimate is in bounds, the TESTFAILED message otherwise
     */
    @VisibleForTesting
    static String selfTestMaxerrExceeded(long checkpoint, long estimate) {
        var relerr = 1.04 / Math.sqrt(SELF_TEST_REGISTERS);
        long maxerr = (long) Math.ceil(relerr * 6 * checkpoint);
        if (checkpoint == 10) {
            maxerr = 1;
        }
        var abserr = Math.abs(checkpoint - estimate);
        if (abserr > maxerr) {
            return "TESTFAILED Too big error. card:" + checkpoint + " abserr:" + abserr;
        }
        return null;
    }

    /**
     * Runs the HyperLogLog self test: a dense register set/get roundtrip followed by an
     * approximation error check over the checkpoints 1, 10, 100, ... of unique elements,
     * both the sparse estimate and the dense conversion estimate must stay in bounds.
     *
     * @return null on success, the TESTFAILED message on failure
     */
    @VisibleForTesting
    static String runSelfTest() {
        // test 1: the dense registers retain the set values and do not affect adjacent values
        var dense = HyperLogLog.builder().setEncoding(HyperLogLog.EncodingType.DENSE).build();
        var denseRegister = dense.getHLLDenseRegister();
        var random = new Random(SELF_TEST_SEED);
        var expected = new byte[SELF_TEST_REGISTERS];
        for (int i = 0; i < SELF_TEST_REGISTERS; i++) {
            var value = (byte) random.nextInt(64);
            expected[i] = value;
            denseRegister.set(i, value);
        }
        var registers = denseRegister.getRegister();
        for (int i = 0; i < SELF_TEST_REGISTERS; i++) {
            if (registers[i] != expected[i]) {
                return "TESTFAILED Register " + i + " should be " + expected[i] + " but is " + registers[i];
            }
        }

        // test 2: the approximation error stays in bounds at every checkpoint
        var hll = emptyHll();
        var checkpoint = 1L;
        for (long j = 1; j <= SELF_TEST_MAX_ELEMENTS; j++) {
            hll.addLong(j ^ SELF_TEST_SEED);

            if (j == checkpoint) {
                // redis asserts the sparse encoding is still used for the small checkpoints,
                // below sparse max bytes over two, the library promotes at ~2450 elements
                if (checkpoint < 1_500 && hll.getEncoding() != HyperLogLog.EncodingType.SPARSE) {
                    return "TESTFAILED sparse encoding not used";
                }

                for (var estimate : new long[]{hll.count(), toDenseHll(hll).count()}) {
                    var failure = selfTestMaxerrExceeded(checkpoint, estimate);
                    if (failure != null) {
                        return failure;
                    }
                }

                checkpoint *= 10;
            }
        }
        return null;
    }

    private Reply pfselftest() {
        if (data.length != 1) {
            return ErrorReply.FORMAT;
        }

        var failure = runSelfTest();
        if (failure != null) {
            return new ErrorReply(failure);
        }
        return OKReply.INSTANCE;
    }

    private static final BulkReply MESSAGE = new BulkReply("message");

    /**
     * Publishes a message to all subscribers of a channel and returns the number of receivers.
     *
     * @param dataGiven the command data: [PUBLISH, channel, message]
     * @param socket    the client socket, used for ACL checking; may be null
     * @return a reply with the number of clients that received the message, or an error reply
     */
    public static Reply publish(byte[][] dataGiven, ITcpSocket socket) {
        if (dataGiven.length != 3) {
            return ErrorReply.FORMAT;
        }

        var localPersist = LocalPersist.getInstance();
        var socketInInspector = localPersist.getSocketInspector();

        var channel = new String(dataGiven[1]);
        var message = new String(dataGiven[2]);

        if (socket != null) {
            // check acl
            var u = BaseCommand.getAuthU(socket);
            if (u == null || !u.isOn() || !u.checkChannels(channel)) {
                return ErrorReply.ACL_PERMIT_LIMIT;
            }
        }

        var replies = new Reply[3];
        replies[0] = MESSAGE;
        replies[1] = new BulkReply(channel);
        replies[2] = new BulkReply(message);

        var n = socketInInspector.subscribeSocketCount(channel, false);

        socketInInspector.publish(channel, new MultiBulkReply(replies), (s, r) -> {
            s.write(r.buffer());
        });
        return new IntegerReply(n);
    }

    private Reply pubsub() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var socketInspector = localPersist.getSocketInspector();

        var subCmd = new String(data[1]).toLowerCase();
        if ("channels".equals(subCmd)) {
            var pattern = data.length == 3 ? new String(data[2]) : null;
            // redis lists only the channels, the pattern subscriptions are excluded
            var channels = socketInspector.subscribeChannels(pattern);
            if (channels.isEmpty()) {
                return MultiBulkReply.EMPTY;
            } else {
                var replies = new Reply[channels.size()];
                for (int i = 0; i < channels.size(); i++) {
                    replies[i] = new BulkReply(channels.get(i));
                }
                return new MultiBulkReply(replies);
            }
        } else if ("numpat".equals(subCmd)) {
            return new IntegerReply(socketInspector.subscribePatternCount());
        } else if ("numsub".equals(subCmd)) {
            if (data.length < 3) {
                // redis returns an empty list when no channel is given
                return MultiBulkReply.EMPTY;
            }

            var channels = new ArrayList<String>();
            for (int i = 2; i < data.length; i++) {
                channels.add(new String(data[i]));
            }

            var replies = new Reply[channels.size() * 2];
            int j = 0;
            for (var channel : channels) {
                // redis excludes the clients subscribed to patterns from PUBSUB NUMSUB
                var size = socketInspector.exactSubscribeSocketCount(channel);
                replies[j++] = new BulkReply(channel);
                replies[j++] = new IntegerReply(size);
            }
            return new MultiBulkReply(replies);
        } else {
            return new ErrorReply("unknown subcommand '" + subCmd + "'. Try PUBSUB HELP.");
        }
    }
}
