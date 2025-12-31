package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
import io.velo.type.RedisHashKeys;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

public class TGroup extends BaseCommand {
    @VisibleForTesting
    static final BulkReply TYPE_STRING = new BulkReply("string".getBytes());
    @VisibleForTesting
    static final BulkReply TYPE_HASH = new BulkReply("hash".getBytes());
    @VisibleForTesting
    static final BulkReply TYPE_LIST = new BulkReply("list".getBytes());
    @VisibleForTesting
    static final BulkReply TYPE_SET = new BulkReply("set".getBytes());
    @VisibleForTesting
    static final BulkReply TYPE_ZSET = new BulkReply("zset".getBytes());
    @VisibleForTesting
    static final BulkReply TYPE_STREAM = new BulkReply("stream".getBytes());

    public TGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("ttl".equals(cmd) || "type".equals(cmd)) {
            if (data.length != 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("time".equals(cmd)) {
            var nanoTime = System.nanoTime();
            var seconds = nanoTime / 1_000_000_000;
            var microseconds = (nanoTime % 1_000_000_000) / 1_000;
            return new MultiBulkReply(new Reply[]{
                    new BulkReply(seconds),
                    new BulkReply(microseconds)
            });
        }

        if ("ttl".equals(cmd)) {
            return ttl(false);
        }

        if ("type".equals(cmd)) {
            return type();
        }

        return NilReply.INSTANCE;
    }

    private Reply type() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        // need not decompress at all, todo: optimize
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            // hash keys changed
            var keysKey = RedisHashKeys.keysKey(new String(keyBytes));
            cv = getCv(slot(keysKey));

            if (cv == null) {
                return NilReply.INSTANCE;
            }
        }

        if (cv.isHash()) {
            return TYPE_HASH;
        }
        if (cv.isList()) {
            return TYPE_LIST;
        }
        if (cv.isSet()) {
            return TYPE_SET;
        }
        if (cv.isZSet() || cv.isGeo()) {
            return TYPE_ZSET;
        }
        if (cv.isStream()) {
            return TYPE_STREAM;
        }

        return TYPE_STRING;
    }

    @VisibleForTesting
    Reply ttl(boolean isMilliseconds) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var expireAt = getExpireAt(slotWithKeyHash);
        if (expireAt == null) {
            return new IntegerReply(-2);
        }
        if (expireAt == CompressedValue.NO_EXPIRE) {
            return new IntegerReply(-1);
        }

        var ttlMilliseconds = expireAt - System.currentTimeMillis();
        return new IntegerReply(isMilliseconds ? ttlMilliseconds : ttlMilliseconds / 1000);
    }
}
