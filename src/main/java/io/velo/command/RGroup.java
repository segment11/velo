package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.netty.buffer.Unpooled;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Debug;
import io.velo.reply.*;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.function.Consumer;

public class RGroup extends BaseCommand {
    public RGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("rename".equals(cmd) || "rpoplpush".equals(cmd)) {
            if (data.length != 3) {
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

        if ("restore".equals(cmd)) {
            if (data.length < 4) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("rpop".equals(cmd)) {
            if (data.length != 2 && data.length != 3) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("rpush".equals(cmd) || "rpushx".equals(cmd)) {
            if (data.length < 3) {
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
        if ("rename".equals(cmd)) {
            return rename();
        }

        if ("restore".equals(cmd)) {
            return restore();
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
    Reply rename() {
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

        var srcKey = new String(srcKeyBytes);
        removeDelay(srcSlotWithKeyHash.slot(), srcSlotWithKeyHash.bucketIndex(), srcKey, srcSlotWithKeyHash.keyHash());

        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
        if (!isCrossRequestWorker) {
            setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
            return OKReply.INSTANCE;
        }

        var dstSlot = dstSlotWithKeyHash.slot();
        var dstOneSlot = localPersist.oneSlot(dstSlot);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncRun(() -> {
            setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
            finalPromise.set(OKReply.INSTANCE);
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
                return new ErrorReply("target key name is busy");
            }
        }

        var buf = Unpooled.wrappedBuffer(serializedValue);
        // todo
        RDBImporter rdbImporter = null;
        try {
            final long finalExpireAt = expireAt;

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
                public void onList(byte[] valueBytes) {
                    set(keyBytes, valueBytes, slotWithKeyHash, CompressedValue.SP_TYPE_LIST, finalExpireAt);
                }

                @Override
                public void onSet(byte[] encodedBytes) {
                    set(keyBytes, encodedBytes, slotWithKeyHash, CompressedValue.SP_TYPE_SET, finalExpireAt);
                }

                @Override
                public void onZSet(byte[] encodedBytes) {
                    set(keyBytes, encodedBytes, slotWithKeyHash, CompressedValue.SP_TYPE_ZSET, finalExpireAt);
                }

                @Override
                public void onHashKeys(byte[] encodedBytes) {
                    var keysKey = RedisHashKeys.keysKey(key);
                    var keysKeyBytes = keysKey.getBytes();
                    var slotWithKeyHash = slot(keysKeyBytes, slotNumber);
                    set(keysKeyBytes, encodedBytes, slotWithKeyHash, CompressedValue.SP_TYPE_HASH, finalExpireAt);
                }

                @Override
                public void onHashFieldValues(String field, byte[] fieldValueBytes) {
                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    var fieldKeyBytes = fieldKey.getBytes();
                    var slotWithKeyHash = slot(fieldKeyBytes, slotNumber);
                    set(fieldKeyBytes, fieldValueBytes, slotWithKeyHash);
                }
            });
            return OKReply.INSTANCE;
        } catch (Exception e) {
            log.error("Restore error", e);
            return new ErrorReply(e.getMessage());
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
        var xx = new BGroup.DstKeyAndDstLeftWhenMove(dstKeyBytes, dstSlotWithKeyHash, dstLeft);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var one = BGroup.addBlockingListPromiseByKey(srcKey, finalPromise, srcLeft, xx);

        var reactor = Reactor.getCurrentReactor();
        reactor.delay(timeoutSeconds * 1000, () -> {
            if (!finalPromise.isComplete()) {
                finalPromise.set(NilReply.INSTANCE);
                // remove form blocking list
                BGroup.removeBlockingListPromiseByKey(srcKey, one);
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
