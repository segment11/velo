package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.persist.Wal;
import io.velo.reply.*;
import io.velo.type.RedisHashKeys;
import org.jetbrains.annotations.VisibleForTesting;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'T'.
 * This includes commands like TIME, TTL, TOUCH, TYPE.
 */
public class TGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "time", 1,
                Set.of("loading", "stale", "fast"),
                0, 0, 0,
                Set.of("@fast", "@connection"),
                "server",
                "Return the current server time.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "ttl", 2,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@read", "@fast"),
                "generic",
                "Get the time to live for a key in seconds.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "type", 2,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@keyspace", "@read", "@fast"),
                "generic",
                "Determine the type stored at key.",
                "1.0.0", "O(1)"));
    }

    @VisibleForTesting
    static final BulkReply TYPE_NONE = new BulkReply("none");
    @VisibleForTesting
    static final BulkReply TYPE_STRING = new BulkReply("string");
    @VisibleForTesting
    static final BulkReply TYPE_HASH = new BulkReply("hash");
    @VisibleForTesting
    static final BulkReply TYPE_LIST = new BulkReply("list");
    @VisibleForTesting
    static final BulkReply TYPE_SET = new BulkReply("set");
    @VisibleForTesting
    static final BulkReply TYPE_ZSET = new BulkReply("zset");
    @VisibleForTesting
    static final BulkReply TYPE_STREAM = new BulkReply("stream");

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public TGroup(String cmd, byte[][] data, ITcpSocket socket) {
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

        if ("ttl".equals(cmd) || "type".equals(cmd)) {
            if (data.length != 2) {
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
        if ("time".equals(cmd)) {
            var now = Instant.now();
            var seconds = now.getEpochSecond();
            var microseconds = now.getNano() / 1_000;
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
            var keysKey = RedisHashKeys.keysKey(Wal.keyString(keyBytes));
            cv = getCv(slot(keysKey));

            if (cv == null) {
                return TYPE_NONE;
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
