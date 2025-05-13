package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.persist.Wal;
import io.velo.reply.*;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;

public class RGroup extends BaseCommand {
    public RGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("rename".equals(cmd) || "renamenx".equals(cmd) || "rpoplpush".equals(cmd)) {
            if (data.length != 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1And2);
            return slotWithKeyHashList;
        }

        if ("restore".equals(cmd)) {
            if (data.length < 4) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        if ("rpop".equals(cmd)) {
            if (data.length != 2 && data.length != 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        if ("rpush".equals(cmd) || "rpushx".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("randomkey".equals(cmd)) {
            return randomkey();
        }

        if ("readonly".equals(cmd)) {
            SocketInspector.setConnectionReadonly(socket, true);
            return OKReply.INSTANCE;
        }

        if ("readwrite".equals(cmd)) {
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

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply randomkey() {
        if (data.length != 1) {
            return ErrorReply.FORMAT;
        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();

        var random = new Random();
        final int maxTryTimes = 10;
        for (int i = 0; i < maxTryTimes; i++) {
            var bucketIndex = random.nextInt(ConfForSlot.global.confBucket.bucketsPerSlot);

            if (!ConfForGlobal.pureMemory) {
                var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
                var keyInLRU = firstOneSlot.randomKeyInLRU(walGroupIndex);
                if (keyInLRU != null) {
                    return new BulkReply(keyInLRU.getBytes());
                }
            }

            var keyCount = firstOneSlot.getKeyLoader().getKeyCountInBucketIndex(bucketIndex);
            if (keyCount > 0) {
                var skipN = random.nextInt(keyCount);
                final int[] countArray = {0};
                final byte[][] targetKeyBytesArray = new byte[1][1];

                var keyBuckets = firstOneSlot.getKeyLoader().readKeyBuckets(bucketIndex);
                for (var keyBucket : keyBuckets) {
                    if (keyBucket == null) {
                        continue;
                    }

                    keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                        if (countArray[0] == skipN) {
                            targetKeyBytesArray[0] = keyBytes;
                            return;
                        }
                        countArray[0]++;
                    });

                    if (targetKeyBytesArray[0] != null) {
                        break;
                    }
                }

                return new BulkReply(targetKeyBytesArray[0]);
            }
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply rename(boolean isNx) {
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
        var srcCv = getCv(srcKeyBytes, srcSlotWithKeyHash);
        if (srcCv == null) {
            return ErrorReply.NO_SUCH_KEY;
        }

        var dstS = slotWithKeyHashListParsed.getLast();
        if (!isCrossRequestWorker) {
            if (isNx) {
                var isExist = exists(dstS.slot(), dstS.bucketIndex(), dstS.rawKey(), dstS.keyHash(), dstS.keyHash32());
                if (isExist) {
                    return IntegerReply.REPLY_0;
                }
            }

            removeDelay(srcSlotWithKeyHash.slot(), srcSlotWithKeyHash.bucketIndex(), srcSlotWithKeyHash.rawKey(), srcSlotWithKeyHash.keyHash());
            setCv(dstKeyBytes, srcCv, dstS);
            return isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE;
        }

        var dstSlot = dstS.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncCall(() -> {
            if (isNx) {
                var isExist = exists(dstS.slot(), dstS.bucketIndex(), dstS.rawKey(), dstS.keyHash(), dstS.keyHash32());
                if (isExist) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return false;
                }
            }

            setCv(dstKeyBytes, srcCv, dstS);
            finalPromise.set(isNx ? IntegerReply.REPLY_1 : OKReply.INSTANCE);
            return true;
        }).whenComplete((r, e) -> {
            if (r) {
                removeDelay(srcSlotWithKeyHash.slot(), srcSlotWithKeyHash.bucketIndex(), srcSlotWithKeyHash.rawKey(), srcSlotWithKeyHash.keyHash());
            }
        });

        return asyncReply;
    }

    private final static String REPLACE = "replace";
    private final static String ABSTTL = "absttl";
    private final static String IDLETIME = "idletime";
    private final static String FREQ = "freq";

    @VisibleForTesting
    Reply restore() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        // refer kvrocks: cmd_server.cc redis::CommandRestore
        var keyBytes = data[1];
        var ttlBytes = data[2];
        var serializedValue = data[3];

        var key = new String(keyBytes);
        var ttl = new String(ttlBytes);

        // for debug
        if (Debug.getInstance().logRestore) {
            log.info("key={}", key);
            log.info("ttl={}", ttl);
        }

        boolean replace = false;
        boolean absttl = false;
        int idleTime = 0;
        int freq = 0;
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
                    idleTime = Integer.parseInt(new String(data[i + 1]));
                    if (idleTime < 0) {
                        return new ErrorReply("idletime can't be negative");
                    }
                } else if (FREQ.equalsIgnoreCase(arg)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    freq = Integer.parseInt(new String(data[i + 1]));
                    if (freq < 0 || freq > 255) {
                        return new ErrorReply("freq must be in range 0..255");
                    }
                }
            }
        }

        long expireAt;
        if (ttlBytes.length == 0 || ttl.equals("0")) {
            // -1 means no expire, different from redis, important!!!
            expireAt = CompressedValue.NO_EXPIRE;
        } else {
            if (absttl) {
                expireAt = Long.parseLong(ttl);
            } else {
                expireAt = System.currentTimeMillis() + Long.parseLong(ttl);
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        // check if key exists
//        replace = true;
        if (!replace) {
            var cv = getCv(keyBytes, slotWithKeyHash);
            if (cv != null) {
                return ErrorReply.TARGET_KEY_BUSY;
            }
        }

        var buf = Unpooled.wrappedBuffer(serializedValue);
        var rdbImporter = new VeloRDBImporter();
        try {
            final long finalExpireAt = expireAt;
            final RGroup _rGroup = this;

            rdbImporter.restore(buf, new RDBCallback() {
                @Override
                public void onInteger(Integer value) {
                    setNumber(keyBytes, value, slotWithKeyHash, finalExpireAt);
                }

                @Override
                public void onString(byte[] valueBytes) {
                    set(keyBytes, valueBytes, slotWithKeyHash, CompressedValue.NULL_DICT_SEQ, finalExpireAt);
                }

                @Override
                public void onList(RedisList rl) {
                    LGroup.saveRedisList(rl, keyBytes, slotWithKeyHash, _rGroup, dictMap);
                }

                @Override
                public void onSet(RedisHashKeys rhk) {
                    SGroup.saveRedisSet(rhk, keyBytes, slotWithKeyHash, _rGroup, dictMap);
                }

                @Override
                public void onZSet(RedisZSet rz) {
                    ZGroup.saveRedisZSet(rz, keyBytes, slotWithKeyHash, _rGroup, dictMap);
                }

                @Override
                public void onHash(RedisHH rhh) {
                    var hGroup = new HGroup(cmd, data, socket);
                    hGroup.from(_rGroup);
                    hGroup.setSlotWithKeyHashListParsed(_rGroup.slotWithKeyHashListParsed);

                    if (hGroup.isUseHH(keyBytes)) {
                        hGroup.saveRedisHH(rhh, keyBytes, slotWithKeyHash);
                    } else {
                        var map = rhh.getMap();
                        var rhk = new RedisHashKeys();

                        for (var entry : map.entrySet()) {
                            var field = entry.getKey();
                            var fieldKey = RedisHashKeys.fieldKey(key, field);
                            var fieldValueBytes = entry.getValue();
                            var slotWithKeyHashThisField = slot(fieldKey.getBytes());
                            set(fieldKey.getBytes(), fieldValueBytes, slotWithKeyHashThisField);

                            rhk.add(field);
                        }

                        hGroup.saveRedisHashKeys(rhk, new String(keyBytes));
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
            new BulkReply("master".getBytes()),
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
                        new BulkReply(x.getHost().getBytes()),
                        new BulkReply(x.getPort()),
                        new BulkReply(x.getSlaveLastCatchUpBinlogAsReplOffset())
                })).toArray();
                var replies = new Reply[array.length];
                for (int i = 0; i < array.length; i++) {
                    replies[i] = (MultiBulkReply) array[i];
                }

                return new MultiBulkReply(new Reply[]{
                        new BulkReply("master".getBytes()),
                        new IntegerReply(firstOneSlot.getBinlog().currentReplOffset()),
                        new MultiBulkReply(replies)
                });
            }
        } else {
            // as slave
            var replPair = firstOneSlot.getOnlyOneReplPairAsSlave();
            assert replPair != null;

            var replies = new Reply[]{
                    new BulkReply("slave".getBytes()),
                    new BulkReply(replPair.getHost().getBytes()),
                    new IntegerReply(replPair.getPort()),
                    new BulkReply("connected".getBytes()),
                    new IntegerReply(replPair.getSlaveLastCatchUpBinlogAsReplOffset())
            };
            return new MultiBulkReply(replies);
        }
    }

    void moveDstCallback(byte[] dstKeyBytes, SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft, byte[] memberValueBytes,
                         Consumer<Reply> consumer) {
        var cvDst = getCv(dstKeyBytes, dstSlotWithKeyHash);

        RedisList rlDst;
        if (cvDst == null) {
            rlDst = new RedisList();
        } else {
            if (!cvDst.isList()) {
                consumer.accept(ErrorReply.WRONG_TYPE);
                return;
            }

            var encodedBytesDst = getValueBytesByCv(cvDst, dstKeyBytes, dstSlotWithKeyHash);
            rlDst = RedisList.decode(encodedBytesDst);
        }

        if (dstLeft) {
            rlDst.addFirst(memberValueBytes);
        } else {
            rlDst.addLast(memberValueBytes);
        }

        set(dstKeyBytes, rlDst.encode(), dstSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);
        consumer.accept(new BulkReply(memberValueBytes));
    }

    Reply move(byte[] srcKeyBytes, SlotWithKeyHash srcSlotWithKeyHash, byte[] dstKeyBytes, SlotWithKeyHash dstSlotWithKeyHash,
               boolean srcLeft, boolean dstLeft) {
        var cvSrc = getCv(srcKeyBytes, srcSlotWithKeyHash);
        if (cvSrc == null) {
            return NilReply.INSTANCE;
        }
        if (!cvSrc.isList()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytesSrc = getValueBytesByCv(cvSrc, srcKeyBytes, srcSlotWithKeyHash);
        var rlSrc = RedisList.decode(valueBytesSrc);

        var size = rlSrc.size();
        if (size == 0) {
            return NilReply.INSTANCE;
        }

        var memberValueBytes = srcLeft ? rlSrc.removeFirst() : rlSrc.removeLast();
        // save after remove
        set(srcKeyBytes, rlSrc.encode(), srcSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);

        if (!isCrossRequestWorker) {
            final Reply[] finalReplyArray = {null};
            moveDstCallback(dstKeyBytes, dstSlotWithKeyHash, dstLeft, memberValueBytes, reply -> finalReplyArray[0] = reply);
            return finalReplyArray[0];
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var dstSlot = dstSlotWithKeyHash.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);
        dstOneSlot.asyncRun(() -> moveDstCallback(dstKeyBytes, dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise::set));

        return asyncReply;
    }

    private AsyncReply doBlockWhenMove(String srcKey, boolean srcLeft, int timeoutSeconds,
                                       byte[] dstKeyBytes, SlotWithKeyHash dstSlotWithKeyHash, boolean dstLeft) {
        var xx = new BlockingList.DstKeyAndDstLeftWhenMove(dstKeyBytes, dstSlotWithKeyHash, dstLeft);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var one = BlockingList.addBlockingListPromiseByKey(srcKey, finalPromise, srcLeft, xx);

        var reactor = Reactor.getCurrentReactor();
        reactor.delay(timeoutSeconds * 1000L, () -> {
            if (!finalPromise.isComplete()) {
                finalPromise.set(NilReply.INSTANCE);
                // remove form blocking list
                BlockingList.removeBlockingListPromiseByKey(srcKey, one);
            }
        });

        return asyncReply;
    }

    Reply moveBlock(byte[] srcKeyBytes, SlotWithKeyHash srcSlotWithKeyHash, byte[] dstKeyBytes, SlotWithKeyHash dstSlotWithKeyHash,
                    boolean srcLeft, boolean dstLeft, int timeoutSeconds) {
        boolean isNoWait = timeoutSeconds <= 0;

        var cvSrc = getCv(srcKeyBytes, srcSlotWithKeyHash);
        if (cvSrc == null) {
            if (isNoWait) {
                return NilReply.INSTANCE;
            } else {
                var srcKey = new String(srcKeyBytes);
                return doBlockWhenMove(srcKey, srcLeft, timeoutSeconds, dstKeyBytes, dstSlotWithKeyHash, dstLeft);
            }
        }

        if (!cvSrc.isList()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytesSrc = getValueBytesByCv(cvSrc, srcKeyBytes, srcSlotWithKeyHash);
        var rlSrc = RedisList.decode(valueBytesSrc);

        var size = rlSrc.size();
        if (size == 0) {
            if (isNoWait) {
                return NilReply.INSTANCE;
            } else {
                var srcKey = new String(srcKeyBytes);
                return doBlockWhenMove(srcKey, srcLeft, timeoutSeconds, dstKeyBytes, dstSlotWithKeyHash, dstLeft);
            }
        }

        var memberValueBytes = srcLeft ? rlSrc.removeFirst() : rlSrc.removeLast();
        // save after remove
        set(srcKeyBytes, rlSrc.encode(), srcSlotWithKeyHash, CompressedValue.SP_TYPE_LIST);

        if (!isCrossRequestWorker) {
            final Reply[] finalReplyArray = {null};
            moveDstCallback(dstKeyBytes, dstSlotWithKeyHash, dstLeft, memberValueBytes, reply -> finalReplyArray[0] = reply);
            return finalReplyArray[0];
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var dstSlot = dstSlotWithKeyHash.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);
        dstOneSlot.asyncRun(() -> moveDstCallback(dstKeyBytes, dstSlotWithKeyHash, dstLeft, memberValueBytes, finalPromise::set));

        return asyncReply;
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

        return move(srcKeyBytes, srcSlotWithKeyHash, dstKeyBytes, dstSlotWithKeyHash, false, true);
    }
}
