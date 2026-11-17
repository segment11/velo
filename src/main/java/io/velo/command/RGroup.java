package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.persist.OneSlot;
import io.velo.persist.ReadonlyException;
import io.velo.persist.Wal;
import io.velo.reply.*;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Handles Redis commands starting with letter 'R'.
 * This includes commands like RANDOMKEY, READONLY, RENAME, RPOP, RPUSH, RSET.
 */
public class RGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "randomkey", 1,
                Set.of("readonly"),
                0, 0, 0,
                Set.of("@keyspace", "@read", "@slow"),
                "generic",
                "Return a random key from the keyspace.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "readonly", 1,
                Set.of("fast", "loading", "stale"),
                0, 0, 0,
                Set.of("@fast", "@connection"),
                "cluster",
                "Enables read queries for a connection to a cluster replica node.",
                "3.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "readwrite", 1,
                Set.of("fast", "loading", "stale"),
                0, 0, 0,
                Set.of("@fast", "@connection"),
                "cluster",
                "Enables read and write queries for a connection to a cluster replica node.",
                "3.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "rename", 3,
                Set.of("write"),
                1, 2, 1,
                Set.of("@keyspace", "@write", "@slow"),
                "generic",
                "Rename a key.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "renamenx", 3,
                Set.of("write", "fast"),
                1, 2, 1,
                Set.of("@keyspace", "@write", "@fast"),
                "generic",
                "Rename a key, only if the new key does not exist.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "replicaof", 3,
                Set.of("admin", "noscript", "stale"),
                0, 0, 0,
                Set.of("@admin", "@dangerous", "@slow"),
                "server",
                "Configure a server as replica of another, or promote it to a master.",
                "5.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "restore", -4,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@keyspace", "@write", "@slow", "@dangerous"),
                "generic",
                "Create a key using the serialized value, previously obtained using DUMP.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "role", 1,
                Set.of("noscript", "loading", "stale", "fast"),
                0, 0, 0,
                Set.of("@admin", "@dangerous", "@fast"),
                "server",
                "Return the role of the instance.",
                "2.8.12", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "rpop", -2,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@list", "@fast"),
                "list",
                "Remove and return the last elements of a list.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "rpoplpush", 3,
                Set.of("write", "denyoom"),
                1, 2, 1,
                Set.of("@write", "@list", "@slow"),
                "list",
                "Remove the last element in a list, prepend it to another list and return it.",
                "1.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "rpush", -3,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@list", "@fast"),
                "list",
                "Append one or more elements to a list.",
                "1.0.0", "O(1) for each element added"));
        CommandRegistry.register(new CommandEntry(
                "rpushx", -3,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@list", "@fast"),
                "list",
                "Append an element to a list, only if the list exists.",
                "2.2.0", "O(1) for each element added"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public RGroup(String cmd, byte[][] data, ITcpSocket socket) {
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

        if ("rename".equals(cmd) || "renamenx".equals(cmd) || "rpoplpush".equals(cmd)) {
            if (data.length != 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            slotWithKeyHashList.add(slot(data[2], slotNumber));
            return slotWithKeyHashList;
        }

        if ("restore".equals(cmd)) {
            if (data.length < 4) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("rpop".equals(cmd)) {
            if (data.length != 2 && data.length != 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("rpush".equals(cmd) || "rpushx".equals(cmd)) {
            if (data.length < 3) {
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
        if ("randomkey".equals(cmd)) {
            return randomkey();
        }

        if ("readonly".equals(cmd)) {
            if (!ConfForGlobal.clusterEnabled) {
                // redis answers an error when the cluster support is disabled
                return CLUSTER_SUPPORT_DISABLED;
            }
            SocketInspector.setConnectionReadonly(socket, true);
            return OKReply.INSTANCE;
        }

        if ("readwrite".equals(cmd)) {
            if (!ConfForGlobal.clusterEnabled) {
                // redis answers an error when the cluster support is disabled
                return CLUSTER_SUPPORT_DISABLED;
            }
            SocketInspector.setConnectionReadonly(socket, false);
            return OKReply.INSTANCE;
        }

        if ("rename".equals(cmd)) {
            return rename(false);
        }

        if ("renamenx".equals(cmd)) {
            return rename(true);
        }

        if ("replicaof".equals(cmd)) {
            var sGroup = new SGroup(cmd, data, socket);
            sGroup.from(this);
            return sGroup.slaveof();
        }

        if ("restore".equals(cmd)) {
            return restore();
        }

        if ("role".equals(cmd)) {
            return role();
        }

        if ("rpop".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpop(false);
        }

        if ("rpoplpush".equals(cmd)) {
            return rpoplpush();
        }

        if ("rpush".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpush(false, false);
        }

        if ("rpushx".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpush(false, true);
        }

        // the command is not registered, redis answers an unknown command error
        return ErrorReply.UNKNOWN_COMMAND(cmd);
    }

    private Reply randomkey() {
        if (data.length != 1) {
            return ErrorReply.FORMAT;
        }

        // the slots of the current worker are owned by this thread, they can be read here
        var currentSlots = localPersist.currentThreadSlots();
        for (var oneSlot : currentSlots) {
            var key = randomKeyInOneSlot(oneSlot);
            if (key != null) {
                return new BulkReply(key);
            }
        }

        // all the slots belong to this worker, the keyspace is empty
        if (currentSlots.size() == localPersist.initializedOneSlots().size()) {
            return NilReply.INSTANCE;
        }

        // no key in the slots of this worker, ask the other slot workers
        Function<OneSlot, String> searchOneSlot = this::randomKeyInOneSlot;
        return localPersist.doSthInSlots(searchOneSlot, keyList -> {
            for (var oneKey : keyList) {
                if (oneKey != null) {
                    return new BulkReply(oneKey);
                }
            }
            return NilReply.INSTANCE;
        });
    }

    /**
     * Searches a random live key of one slot, must run on the slot worker thread.
     *
     * @param oneSlot the slot to search
     * @return a live key of the slot, or null when the slot has no live key
     */
    private String randomKeyInOneSlot(OneSlot oneSlot) {
        var random = new Random();
        final int maxTryTimes = 10;
        for (int i = 0; i < maxTryTimes; i++) {
            var bucketIndex = random.nextInt(ConfForSlot.global.confBucket.bucketsPerSlot);

            var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
            var keyInLRU = oneSlot.randomKeyInLRU(walGroupIndex);
            if (keyInLRU != null) {
                return keyInLRU;
            }

            var keyInBucket = randomKeyInBucket(oneSlot, bucketIndex, random);
            if (keyInBucket != null) {
                return keyInBucket;
            }
        }
        return null;
    }

    /**
     * Picks a random live key of one bucket, the random index inside the bucket is honoured and the
     * expired keys are not returned.
     *
     * @param oneSlot     the slot that owns the bucket
     * @param bucketIndex the bucket index
     * @param random      the random source
     * @return a live key of the bucket, or null when the bucket has no live key
     */
    @VisibleForTesting
    String randomKeyInBucket(OneSlot oneSlot, int bucketIndex, Random random) {
        var keyCount = oneSlot.getKeyLoader().getKeyCountInBucketIndex(bucketIndex);
        if (keyCount <= 0) {
            return null;
        }

        var skipN = random.nextInt(keyCount);
        final int[] countArray = {0};
        final String[] targetKeyArray = new String[1];

        var keyBuckets = oneSlot.getKeyLoader().readKeyBuckets(bucketIndex);
        for (var keyBucket : keyBuckets) {
            if (keyBucket == null) {
                continue;
            }

            keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
                if (targetKeyArray[0] != null) {
                    // the key is found already, the iteration cannot be stopped
                    return;
                }
                if (expireAt != CompressedValue.NO_EXPIRE && expireAt <= System.currentTimeMillis()) {
                    // an expired key is not a key of the database anymore
                    return;
                }
                if (countArray[0] == skipN) {
                    targetKeyArray[0] = key;
                    return;
                }
                countArray[0]++;
            });

            if (targetKeyArray[0] != null) {
                break;
            }
        }

        return targetKeyArray[0];
    }

    private Reply rename(boolean isNx) {
        if (data.length != 3) {
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

        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var srcCv = getCv(srcSlotWithKeyHash);
        if (srcCv == null) {
            return ErrorReply.NO_SUCH_KEY;
        }

        var dstS = slotWithKeyHashListParsed.getLast();
        if (!isCrossRequestWorker) {
            if (isNx) {
                var isExist = exists(dstS);
                if (isExist) {
                    return IntegerReply.REPLY_0;
                }
            }

            removeDelay(srcSlotWithKeyHash);
            setCv(srcCv, dstS);
            return isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE;
        }

        var dstSlot = dstS.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);

        // handle() runs on the src slot worker (src is the first key), so the dst write must be
        // dispatched to the dst slot eventloop. The src removeDelay in the whenComplete below
        // relies on the Promise.ofFuture bounce: on the reactor path the dst promise completion
        // comes back to this (src) worker, so it also runs on the src slot worker thread.
        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncCall(() -> {
            try {
                if (isNx) {
                    var isExist = exists(dstS);
                    if (isExist) {
                        finalPromise.set(IntegerReply.REPLY_0);
                        return false;
                    }
                }

                setCv(srcCv, dstS);
                finalPromise.set(isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE);
                return true;
            } catch (ReadonlyException e) {
                // the destination slot rejected the write, the client must get an answer
                finalPromise.set(ErrorReply.READONLY);
                return false;
            } catch (Exception e) {
                log.error("Rename to destination slot={} failed", dstSlot, e);
                // the promise must always complete or the client connection hangs;
                // transferAsyncReply builds the error reply from getMessage(), so the
                // exception must carry a readable message
                finalPromise.setException(Utils.wrapException("rename", e));
                return false;
            }
        }).whenComplete((r, e) -> {
            // only remove the source key when the destination write succeeded
            if (Boolean.TRUE.equals(r)) {
                removeDelay(srcSlotWithKeyHash);
            }
        });

        return asyncReply;
    }

    /** The redis error reply for the readonly and readwrite commands when the cluster support is disabled. */
    private final static ErrorReply CLUSTER_SUPPORT_DISABLED = new ErrorReply("This instance has cluster support disabled");

    private final static String REPLACE = "replace";
    private final static String ABSTTL = "absttl";
    private final static String IDLETIME = "idletime";
    private final static String FREQ = "freq";

    private Reply restore() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        // refer kvrocks: cmd_server.cc redis::CommandRestore
        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        var ttlBytes = data[2];
        var serializedValue = data[3];

        var key = Wal.keyString(keyBytes);
        var ttl = new String(ttlBytes);

        // for debug
        var debug = Debug.getInstance();
        if (debug.logRestore) {
            log.info("key={}", key);
            log.info("ttl={}", ttl);
        }

        boolean replace = false;
        boolean absttl = false;
        int idleTime = 0;
        int freq = 0;
        boolean hasIdleTime = false;
        boolean hasFreq = false;
        if (data.length > 4) {
            for (int i = 4; i < data.length; i++) {
                var arg = new String(data[i]);
                if (REPLACE.equalsIgnoreCase(arg)) {
                    replace = true;
                } else if (ABSTTL.equalsIgnoreCase(arg)) {
                    absttl = true;
                } else if (IDLETIME.equalsIgnoreCase(arg)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    if (hasFreq) {
                        // redis does not accept idletime and freq together
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        idleTime = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.INVALID_INTEGER;
                    }
                    if (idleTime < 0) {
                        return new ErrorReply("idletime can't be negative");
                    }
                    hasIdleTime = true;
                    i++; // consume the idletime argument
                } else if (FREQ.equalsIgnoreCase(arg)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    if (hasIdleTime) {
                        // redis does not accept idletime and freq together
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        freq = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.INVALID_INTEGER;
                    }
                    if (freq < 0 || freq > 255) {
                        return new ErrorReply("freq must be in range 0..255");
                    }
                    hasFreq = true;
                    i++; // consume the freq argument
                } else {
                    // redis answers a syntax error for an unknown option
                    return ErrorReply.SYNTAX;
                }
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        // check if key exists, redis checks the busy key before the ttl
//        replace = true;
        if (!replace) {
            // only the existence of the key matters here, do not read the value
            if (exists(slotWithKeyHash)) {
                return ErrorReply.BUSYKEY;
            }
            // in the split hash mode the hash is stored under the derived keys key, the main key does not exist
            if (exists(slot(RedisHashKeys.keysKey(key)))) {
                return ErrorReply.BUSYKEY;
            }
        }

        long expireAt;
        if (ttlBytes.length == 0 || ttl.equals("0")) {
            // -1 means no expire, different from redis, important!!!
            expireAt = CompressedValue.NO_EXPIRE;
        } else {
            long ttlValue;
            try {
                ttlValue = Long.parseLong(ttl);
            } catch (NumberFormatException e) {
                return ErrorReply.INVALID_INTEGER;
            }
            if (ttlValue < 0) {
                return new ErrorReply("Invalid TTL value, must be >= 0");
            }
            if (absttl) {
                expireAt = ttlValue;
            } else {
                expireAt = System.currentTimeMillis() + ttlValue;
            }
        }

        var nettyBuf = Unpooled.wrappedBuffer(serializedValue);
        var rdbImporter = new VeloRDBImporter();
        try {
            final long finalExpireAt = expireAt;
            final RGroup _rGroup = this;

            rdbImporter.restore(nettyBuf, new RDBCallback() {
                @Override
                public void onString(byte[] valueBytes) {
                    set(valueBytes, slotWithKeyHash, CompressedValue.NULL_DICT_SEQ, finalExpireAt);
                }

                @Override
                public void onList(RedisList rl) {
                    LGroup.saveRedisList(rl, slotWithKeyHash, _rGroup, dictMap, finalExpireAt);
                }

                @Override
                public void onSet(RedisHashKeys rhk) {
                    SGroup.saveRedisSet(rhk, slotWithKeyHash, _rGroup, dictMap, false, finalExpireAt);
                }

                @Override
                public void onZSet(RedisZSet rz) {
                    ZGroup.saveRedisZSet(rz, slotWithKeyHash, _rGroup, dictMap, finalExpireAt);
                }

                @Override
                public void onHash(RedisHH rhh) {
                    var hGroup = new HGroup(cmd, data, socket);
                    hGroup.from(_rGroup);
                    hGroup.setSlotWithKeyHashListParsed(_rGroup.slotWithKeyHashListParsed);

                    if (hGroup.isUseHH(keyBytes)) {
                        hGroup.saveRedisHH(rhh, slotWithKeyHash, finalExpireAt);
                    } else {
                        var map = rhh.getMap();
                        var rhk = new RedisHashKeys();

                        for (var entry : map.entrySet()) {
                            var field = entry.getKey();
                            var fieldKey = RedisHashKeys.fieldKey(key, field);
                            var fieldValueBytes = entry.getValue();
                            var slotWithKeyHashThisField = slot(fieldKey);
                            set(fieldValueBytes, slotWithKeyHashThisField, CompressedValue.NULL_DICT_SEQ, finalExpireAt);

                            rhk.add(field);
                        }

                        hGroup.saveRedisHashKeys(rhk, Wal.keyString(keyBytes), finalExpireAt);
                    }
                }
            });
            return OKReply.INSTANCE;
        } catch (Exception e) {
            log.error("Restore error", e);
            return new ErrorReply(e.getMessage());
        }
    }

    private static final MultiBulkReply ROLE_AS_MASTER_REPLY = new MultiBulkReply(new Reply[]{
            new BulkReply("master"),
            IntegerReply.REPLY_0,
            MultiBulkReply.EMPTY
    });

    @VisibleForTesting
    Reply role() {
        if (localPersist.oneSlots() == null) {
            // for unit test
            return ROLE_AS_MASTER_REPLY;
        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        var isSelfSlave = firstOneSlot.isAsSlave();

        if (!isSelfSlave) {
            var slaveReplPairList = firstOneSlot.getSlaveReplPairListSelfAsMaster();
            // no slaves
            if (slaveReplPairList.isEmpty()) {
                return ROLE_AS_MASTER_REPLY;
            } else {
                var array = slaveReplPairList.stream().map(x -> new MultiBulkReply(new Reply[]{
                        new BulkReply(x.getHost()),
                        new BulkReply(x.getPort()),
                        new BulkReply(x.getSlaveLastCatchUpBinlogAsReplOffset())
                })).toArray();
                var replies = new Reply[array.length];
                for (int i = 0; i < array.length; i++) {
                    replies[i] = (MultiBulkReply) array[i];
                }

                return new MultiBulkReply(new Reply[]{
                        new BulkReply("master"),
                        new IntegerReply(firstOneSlot.getBinlog().currentReplOffset()),
                        new MultiBulkReply(replies)
                });
            }
        } else {
            // as slave
            var replPair = firstOneSlot.getOnlyOneReplPairAsSlave();
            assert replPair != null;

            // Redis-compatible link state: report "connected" only when the slave TCP client is
            // actually up; otherwise "down" so Sentinel can mark the slave SDOWN / ODOWN.
            var linkState = replPair.isLinkUpAnyOk() ? "connected" : "connecting";

            var replies = new Reply[]{
                    new BulkReply("slave"),
                    new BulkReply(replPair.getHost()),
                    new IntegerReply(replPair.getPort()),
                    new BulkReply(linkState),
                    new IntegerReply(replPair.getSlaveLastCatchUpBinlogAsReplOffset())
            };
            return new MultiBulkReply(replies);
        }
    }

    void moveDstCallback(SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft, byte[] memberValueBytes,
                         Consumer<Reply> consumer) {
        var cvDst = getCv(dstSlotWithKeyHash);

        RedisList rlDst;
        if (cvDst == null) {
            rlDst = new RedisList();
        } else {
            if (!cvDst.isList()) {
                consumer.accept(ErrorReply.WRONG_TYPE);
                return;
            }

            var encodedBytesDst = getValueBytesByCv(cvDst, dstSlotWithKeyHash);
            rlDst = RedisList.decode(encodedBytesDst);
        }

        if (dstLeft) {
            rlDst.addFirst(memberValueBytes);
        } else {
            rlDst.addLast(memberValueBytes);
        }

        set(rlDst.encode(), dstSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);
        consumer.accept(new BulkReply(memberValueBytes));
    }

    Reply move(SlotWithKeyHash srcSlotWithKeyHash, SlotWithKeyHash dstSlotWithKeyHash,
               boolean srcLeft, boolean dstLeft) {
        var cvSrc = getCv(srcSlotWithKeyHash);
        if (cvSrc == null) {
            return NilReply.INSTANCE;
        }
        if (!cvSrc.isList()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytesSrc = getValueBytesByCv(cvSrc, srcSlotWithKeyHash);
        var rlSrc = RedisList.decode(valueBytesSrc);

        var size = rlSrc.size();
        if (size == 0) {
            return NilReply.INSTANCE;
        }

        if (!isCrossRequestWorker) {
            // the destination slot is owned by this worker, validate the destination type here
            var dstTypeReply = checkDstTypeOnCurrentWorker(srcSlotWithKeyHash, dstSlotWithKeyHash);
            if (dstTypeReply != null) {
                return dstTypeReply;
            }

            var memberValueBytes = popFromList(srcSlotWithKeyHash, rlSrc, srcLeft);

            final Reply[] finalReplyArray = {null};
            moveDstCallback(dstSlotWithKeyHash, dstLeft, memberValueBytes, reply -> finalReplyArray[0] = reply);
            return finalReplyArray[0];
        }

        // the destination slot is owned by another slot worker
        return moveCrossWorker(srcSlotWithKeyHash, dstSlotWithKeyHash, srcLeft, dstLeft);
    }

    /**
     * Moves one element between two slots that are owned by different slot workers.
     *
     * <p>The destination type is validated on the destination worker first, redis does not touch the
     * source when the destination holds a value of another type. The element is then popped on the
     * source worker and pushed on the destination worker.
     *
     * @param srcSlotWithKeyHash the source key
     * @param dstSlotWithKeyHash the destination key
     * @param srcLeft            true to pop the first element, false to pop the last one
     * @param dstLeft            true to push the element to the head of the destination list
     * @return the asynchronous reply
     */
    private Reply moveCrossWorker(SlotWithKeyHash srcSlotWithKeyHash, SlotWithKeyHash dstSlotWithKeyHash,
                                  boolean srcLeft, boolean dstLeft) {
        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);
        var srcOneSlot = localPersist.oneSlot(srcSlotWithKeyHash.slot());
        var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());

        // step 1: the destination worker validates the destination type
        dstOneSlot.asyncExecute(() -> {
            boolean dstTypeOk;
            try {
                dstTypeOk = checkDstTypeOnDstWorker(dstSlotWithKeyHash);
            } catch (Exception e) {
                finalPromise.setException(e);
                return;
            }
            if (!dstTypeOk) {
                finalPromise.set(ErrorReply.WRONG_TYPE);
                return;
            }

            // step 2: the source worker pops the element
            srcOneSlot.asyncExecute(() -> {
                byte[] memberValueBytes;
                try {
                    memberValueBytes = popFromSource(srcSlotWithKeyHash, srcLeft);
                } catch (Exception e) {
                    finalPromise.setException(e);
                    return;
                }
                if (memberValueBytes == null) {
                    finalPromise.set(NilReply.INSTANCE);
                    return;
                }
                // step 3: the destination worker pushes the element
                dstOneSlot.asyncExecute(() ->
                        pushToDestination(dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise));
            });
        });

        return asyncReply;
    }

    /**
     * Validates the destination list type on the current worker.
     *
     * @param srcSlotWithKeyHash the source key
     * @param dstSlotWithKeyHash the destination key
     * @return an error reply when the destination cannot accept a list element, null otherwise
     */
    private Reply checkDstTypeOnCurrentWorker(SlotWithKeyHash srcSlotWithKeyHash, SlotWithKeyHash dstSlotWithKeyHash) {
        if (srcSlotWithKeyHash.rawKey().equals(dstSlotWithKeyHash.rawKey())) {
            return null;
        }

        var cvDst = getCv(dstSlotWithKeyHash);
        if (cvDst != null && !cvDst.isList()) {
            return ErrorReply.WRONG_TYPE;
        }
        return null;
    }

    /**
     * Validates the destination list type, must run on the destination slot worker.
     * The cross worker path always moves between two different slots, so the source and the
     * destination are never the same key here.
     *
     * @param dstSlotWithKeyHash the destination key
     * @return true when the destination is a list or does not exist
     */
    private boolean checkDstTypeOnDstWorker(SlotWithKeyHash dstSlotWithKeyHash) {
        var cvDst = getCv(dstSlotWithKeyHash);
        return cvDst == null || cvDst.isList();
    }

    /**
     * Pops one element from the source list and saves the source list.
     *
     * @param srcSlotWithKeyHash the source key
     * @param rlSrc              the decoded source list
     * @param srcLeft            true to pop the first element, false to pop the last one
     * @return the popped element bytes
     */
    private byte[] popFromList(SlotWithKeyHash srcSlotWithKeyHash, RedisList rlSrc, boolean srcLeft) {
        var memberValueBytes = srcLeft ? rlSrc.removeFirst() : rlSrc.removeLast();
        // save after remove, auto-delete empty list
        if (rlSrc.size() == 0) {
            removeDelay(srcSlotWithKeyHash);
        } else {
            set(rlSrc.encode(), srcSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);
        }
        return memberValueBytes;
    }

    /**
     * Reads the source list on the source slot worker and pops one element from it.
     *
     * @param srcSlotWithKeyHash the source key
     * @param srcLeft            true to pop the first element, false to pop the last one
     * @return the popped element bytes, or null when the source list is gone, empty or not a list
     */
    private byte[] popFromSource(SlotWithKeyHash srcSlotWithKeyHash, boolean srcLeft) {
        var cvSrc = getCv(srcSlotWithKeyHash);
        if (cvSrc == null || !cvSrc.isList()) {
            return null;
        }
        var rlSrc = RedisList.decode(getValueBytesByCv(cvSrc, srcSlotWithKeyHash));
        if (rlSrc.size() == 0) {
            return null;
        }
        return popFromList(srcSlotWithKeyHash, rlSrc, srcLeft);
    }

    private AsyncReply doBlockWhenMove(String srcKey, boolean srcLeft, double timeoutSeconds,
                                       SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft) {
        var xx = new BlockingList.DstKeyAndDstLeftWhenMove(dstSlotWithKeyHash, dstLeft);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var one = BlockingList.addBlockingListPromiseByKey(srcKey, finalPromise, socket, srcLeft, xx);

        if (timeoutSeconds > 0) {
            long timeoutMillis = (long) (timeoutSeconds * 1000);
            var reactor = Reactor.getCurrentReactor();
            reactor.delay(timeoutMillis, () -> {
                // one-shot delivery arbiter: a timeout that lost to a delivery must not touch the promise
                if (one.tryMarkDelivered()) {
                    finalPromise.set(NilReply.INSTANCE);
                    BlockingList.removeBlockingListPromiseByKey(srcKey, one);
                }
            });
        }

        return asyncReply;
    }

    Reply moveBlock(SlotWithKeyHash srcSlotWithKeyHash, SlotWithKeyHash dstSlotWithKeyHash,
                    boolean srcLeft, boolean dstLeft, double timeoutSeconds) {
        var cvSrc = getCv(srcSlotWithKeyHash);
        if (cvSrc == null) {
            return doBlockWhenMove(srcSlotWithKeyHash.rawKey(), srcLeft, timeoutSeconds, dstSlotWithKeyHash, dstLeft);
        }

        if (!cvSrc.isList()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytesSrc = getValueBytesByCv(cvSrc, srcSlotWithKeyHash);
        var rlSrc = RedisList.decode(valueBytesSrc);

        var size = rlSrc.size();
        if (size == 0) {
            return doBlockWhenMove(srcSlotWithKeyHash.rawKey(), srcLeft, timeoutSeconds, dstSlotWithKeyHash, dstLeft);
        }

        if (!isCrossRequestWorker) {
            // the destination slot is owned by this worker, validate the destination type here
            var dstTypeReply = checkDstTypeOnCurrentWorker(srcSlotWithKeyHash, dstSlotWithKeyHash);
            if (dstTypeReply != null) {
                return dstTypeReply;
            }

            var memberValueBytes = popFromList(srcSlotWithKeyHash, rlSrc, srcLeft);

            final Reply[] finalReplyArray = {null};
            moveDstCallback(dstSlotWithKeyHash, dstLeft, memberValueBytes, reply -> finalReplyArray[0] = reply);
            return finalReplyArray[0];
        }

        // the destination slot is owned by another slot worker
        return moveBlockCrossWorker(srcSlotWithKeyHash, dstSlotWithKeyHash, srcLeft, dstLeft, timeoutSeconds);
    }

    /**
     * Moves one element between two slots that are owned by different slot workers, blocking until an
     * element is available when the source list is empty.
     *
     * @param srcSlotWithKeyHash the source key
     * @param dstSlotWithKeyHash the destination key
     * @param srcLeft            true to pop the first element, false to pop the last one
     * @param dstLeft            true to push the element to the head of the destination list
     * @param timeoutSeconds     the blocking timeout in seconds, 0 to block forever
     * @return the asynchronous reply
     */
    private Reply moveBlockCrossWorker(SlotWithKeyHash srcSlotWithKeyHash, SlotWithKeyHash dstSlotWithKeyHash,
                                       boolean srcLeft, boolean dstLeft, double timeoutSeconds) {
        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);
        var srcOneSlot = localPersist.oneSlot(srcSlotWithKeyHash.slot());
        var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());

        // step 1: the destination worker validates the destination type
        dstOneSlot.asyncExecute(() -> {
            boolean dstTypeOk;
            try {
                dstTypeOk = checkDstTypeOnDstWorker(dstSlotWithKeyHash);
            } catch (Exception e) {
                finalPromise.setException(e);
                return;
            }
            if (!dstTypeOk) {
                finalPromise.set(ErrorReply.WRONG_TYPE);
                return;
            }

            // step 2: the source side runs on the source slot worker
            srcOneSlot.asyncExecute(() -> {
                byte[] memberValueBytes;
                try {
                    memberValueBytes = popFromSource(srcSlotWithKeyHash, srcLeft);
                } catch (Exception e) {
                    finalPromise.setException(e);
                    return;
                }
                if (memberValueBytes == null) {
                    // the source list is empty, wait for an element to arrive
                    var blockingReply = doBlockWhenMove(srcSlotWithKeyHash.rawKey(), srcLeft, timeoutSeconds,
                            dstSlotWithKeyHash, dstLeft);
                    blockingReply.getSettablePromise().whenComplete((r, e2) -> {
                        if (e2 != null) {
                            finalPromise.setException(e2);
                            return;
                        }
                        finalPromise.set(r);
                    });
                    return;
                }
                // step 3: the destination worker pushes the element
                dstOneSlot.asyncExecute(() ->
                        pushToDestination(dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise));
            });
        });

        return asyncReply;
    }

    /**
     * Pushes the moved element to the destination list on the destination slot worker, a failing write
     * must be reported to the client instead of leaving the request unanswered.
     *
     * @param dstSlotWithKeyHash the destination key
     * @param dstLeft            true to push the element to the head of the destination list
     * @param memberValueBytes   the moved element bytes
     * @param finalPromise       the promise that carries the reply
     */
    private void pushToDestination(SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft, byte[] memberValueBytes,
                                   SettablePromise<Reply> finalPromise) {
        try {
            moveDstCallback(dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise::set);
        } catch (ReadonlyException e) {
            // the destination slot rejected the write, the client must get an answer
            finalPromise.set(ErrorReply.READONLY);
        } catch (Exception e) {
            log.error("Move to destination slot={} failed", dstSlotWithKeyHash.slot(), e);
            // the promise must always complete or the client connection hangs;
            // transferAsyncReply builds the error reply from getMessage(), so the
            // exception must carry a readable message
            finalPromise.setException(Utils.wrapException("move", e));
        }
    }

    @VisibleForTesting
    Reply rpoplpush() {
        if (data.length != 3) {
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

        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();

        return move(srcSlotWithKeyHash, dstSlotWithKeyHash, false, true);
    }
}
