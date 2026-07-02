package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.velo.*;
import io.velo.acl.AclUsers;
import io.velo.persist.KeyLoader;
import io.velo.persist.Wal;
import io.velo.reply.*;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;

/**
 * Handles Redis commands starting with letter 'H'.
 * This includes commands like HGET, HSET, HMGET, HMSET, HDEL, HEXISTS, HKEYS, HLEN, HGETALL, etc.
 */
public class HGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "hdel", -3,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Delete one or more hash fields.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hello", -1,
                Set.of("noscript", "loading", "stale", "fast", "no_auth"),
                0, 0, 0,
                Set.of("@fast", "@connection"),
                "connection",
                "Handshake with Redis.",
                "6.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hexists", 3,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Determine whether a field exists in a hash.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hexpire", -6,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the time to live of a hash field in seconds.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hexpireat", -6,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the expiration time of a hash field as a UNIX timestamp.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hexpiretime", -4,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the expiration time of a hash field as a UNIX timestamp.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hget", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get the value of a hash field.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hgetall", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get all the fields and values in a hash.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hgetdel", -5,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Get the value of a hash field and delete it.",
                "1.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hgetex", -4,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Get the value of a hash field and optionally set its expiration.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hincrby", 4,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Increment the integer value of a hash field by the given amount.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hincrbyfloat", 4,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Increment the floating point value of a hash field by the given amount.",
                "2.6.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hkeys", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get all the fields in a hash.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hlen", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get the number of fields in a hash.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hmget", -3,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the values of all the given hash fields.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hmset", -4,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set multiple hash fields to multiple values.",
                "2.0.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hpersist", -4,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Remove the expiration time of a hash field.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hpexpire", -6,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the time to live of a hash field in milliseconds.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hpexpireat", -6,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the expiration time of a hash field as a UNIX milliseconds timestamp.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hpexpiretime", -4,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the expiration time of a hash field as a UNIX milliseconds timestamp.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hpttl", -4,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the time to live of a hash field in milliseconds.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hrandfield", -2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get one or multiple random fields from a hash.",
                "6.2.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "hscan", -3,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Incrementally iterate hash fields and associated values.",
                "2.8.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hset", -4,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the string value of a hash field.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hsetex", -6,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the value of a hash field with expiration.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hsetnx", 4,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@hash", "@fast"),
                "hash",
                "Set the value of a hash field, only when the field doesn't exist.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hstrlen", 3,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the length of the value of a hash field.",
                "3.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "httl", -4,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@hash", "@fast"),
                "hash",
                "Get the time to live of a hash field in seconds.",
                "7.4.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "hvals", 2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@hash", "@slow"),
                "hash",
                "Get all the values in a hash.",
                "2.0.0", "O(N)"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public HGroup(String cmd, byte[][] data, ITcpSocket socket) {
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

        if ("hdel".equals(cmd) || "hexists".equals(cmd) || "hexpire".equals(cmd) || "hexpireat".equals(cmd) ||
                "hexpiretime".equals(cmd) || "hget".equals(cmd) || "hgetall".equals(cmd) ||
                "hgetdel".equals(cmd) || "hgetex".equals(cmd) || "hincrby".equals(cmd) || "hincrbyfloat".equals(cmd) ||
                "hkeys".equals(cmd) || "hlen".equals(cmd) || "hmget".equals(cmd) || "hmset".equals(cmd) ||
                "hpersist".equals(cmd) || "hpexpire".equals(cmd) || "hpexpireat".equals(cmd) || "hpexpiretime".equals(cmd) ||
                "hpttl".equals(cmd) || "hrandfield".equals(cmd) || "hscan".equals(cmd) || "hset".equals(cmd) ||
                "hsetex".equals(cmd) || "hsetnx".equals(cmd) || "hstrlen".equals(cmd) || "httl".equals(cmd) ||
                "hvals".equals(cmd)) {
            if (data.length < 2) {
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
        if ("hdel".equals(cmd)) {
            return hdel();
        }

        if ("hello".equals(cmd)) {
            return hello();
        }

        if ("hexists".equals(cmd)) {
            return hexists();
        }

        if ("hexpire".equals(cmd)) {
            return hexpire(false, false);
        }

        if ("hexpireat".equals(cmd)) {
            return hexpire(true, false);
        }

        if ("hexpiretime".equals(cmd)) {
            return hIterateFields(false, false, false);
        }

        if ("hget".equals(cmd)) {
            return hget(false);
        }

        if ("hgetall".equals(cmd)) {
            return hgetall();
        }

        if ("hgetdel".equals(cmd)) {
            return hgetdel();
        }

        if ("hgetex".equals(cmd)) {
            return hgetex();
        }

        if ("hincrby".equals(cmd)) {
            return hincrby(false);
        }

        if ("hincrbyfloat".equals(cmd)) {
            return hincrby(true);
        }

        if ("hkeys".equals(cmd)) {
            return hkeys(false);
        }

        if ("hlen".equals(cmd)) {
            return hkeys(true);
        }

        if ("hmget".equals(cmd)) {
            return hmget();
        }

        if ("hmset".equals(cmd)) {
            return hmset(false);
        }

        if ("hpersist".equals(cmd)) {
            return hIterateFields(true, false, false);
        }

        if ("hpexpire".equals(cmd)) {
            return hexpire(false, true);
        }

        if ("hpexpireat".equals(cmd)) {
            return hexpire(true, true);
        }

        if ("hpexpiretime".equals(cmd)) {
            return hIterateFields(false, true, false);
        }

        if ("hpttl".equals(cmd)) {
            return hIterateFields(false, true, true);
        }

        if ("hrandfield".equals(cmd)) {
            return hrandfield();
        }

        if ("hscan".equals(cmd)) {
            return hscan();
        }

        if ("hset".equals(cmd)) {
            return hmset(true);
        }

        if ("hsetex".equals(cmd)) {
            return hsetex();
        }

        if ("hsetnx".equals(cmd)) {
            return hsetnx();
        }

        if ("hstrlen".equals(cmd)) {
            return hget(true);
        }

        if ("httl".equals(cmd)) {
            return hIterateFields(false, false, true);
        }

        if ("hvals".equals(cmd)) {
            return hvals();
        }

        return NilReply.INSTANCE;
    }

    private RedisHashKeys getRedisHashKeys(String key) {
        var keysKey = RedisHashKeys.keysKey(key);

        var keysValueBytes = get(slot(keysKey), false, CompressedValue.SP_TYPE_HASH);
        if (keysValueBytes == null) {
            return null;
        }

        return RedisHashKeys.decode(keysValueBytes);
    }

    void saveRedisHashKeys(RedisHashKeys rhk, String key) {
        var keysKey = RedisHashKeys.keysKey(key);
        var slotWithKeyHash = slot(keysKey);

        if (rhk.size() == 0) {
            removeDelay(slotWithKeyHash);
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(rhk.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_HASH);
    }

    private RedisHH getRedisHH(SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return null;
        }

        return RedisHH.decode(encodedBytes);
    }

    void saveRedisHH(RedisHH rhh, SlotWithKeyHash slotWithKeyHash) {
        if (rhh.size() == 0) {
            removeDelay(slotWithKeyHash);
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(slotWithKeyHash.rawKey());
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(rhh.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_HH);
    }

    boolean isUseHH(byte[] keyBytes) {
        int checkPrefixLength = RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX.length;
        if (keyBytes.length > checkPrefixLength) {
            // check prefix match
            if (Arrays.equals(keyBytes, 0, checkPrefixLength,
                    RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX, 0, checkPrefixLength)) {
                return false;
            }
        }
        return localPersist.getIsHashSaveMemberTogether();
    }

    private Reply hdel() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        ArrayList<String> fields = new ArrayList<>();
        for (int i = 2; i < data.length; i++) {
            var fieldBytes = data[i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            fields.add(new String(fieldBytes));
        }

        if (isUseHH(keyBytes)) {
            return hdel2(fields);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        for (var field : fields) {
            if (rhk.remove(field)) {
                removed++;

                var fieldKey = RedisHashKeys.fieldKey(key, field);
                var slotWithKeyHashThisField = slot(fieldKey);
                removeDelay(slotWithKeyHashThisField);
            }
        }

        saveRedisHashKeys(rhk, key);
        return new IntegerReply(removed);
    }

    private Reply hdel2(ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            return IntegerReply.REPLY_0;
        }

        var removed = 0;
        for (var field : fields) {
            if (rhh.remove(field)) {
                removed++;
            }
        }

        saveRedisHH(rhh, slotWithKeyHash);
        return new IntegerReply(removed);
    }

    private Reply hello() {
        boolean isResp3 = SocketInspector.isResp3(socket);
        boolean isResp3Old = isResp3;
        if (data.length > 1) {
            var ver = new String(data[1]);
            if ("3".equals(ver)) {
                SocketInspector.setResp3(socket, true);
                isResp3 = true;
            } else if ("2".equals(ver)) {
                SocketInspector.setResp3(socket, false);
                isResp3 = false;
            } else {
                return new ErrorReply("Server protover not support: " + ver);
            }
        }

        if (data.length > 2) {
            for (int i = 2; i < data.length; i++) {
                var arg = new String(data[i]).toLowerCase();
                if ("auth".equals(arg)) {
                    if (i + 2 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }

                    var user = new String(data[i + 1]);
                    var password = new String(data[i + 2]);

                    // acl check
                    var u = aclUsers.get(user);
                    if (u == null) {
                        var clientInfo = ((TcpSocket) socket).getRemoteAddress().toString();
                        AclUsers.recordAclLog("auth", "auth", user, user, clientInfo);
                        SocketInspector.setResp3(socket, isResp3Old);
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.isOn()) {
                        var clientInfo = ((TcpSocket) socket).getRemoteAddress().toString();
                        AclUsers.recordAclLog("auth", "auth", user, user, clientInfo);
                        SocketInspector.setResp3(socket, isResp3Old);
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.checkPassword(password)) {
                        var clientInfo = ((TcpSocket) socket).getRemoteAddress().toString();
                        AclUsers.recordAclLog("auth", "auth", user, user, clientInfo);
                        SocketInspector.setResp3(socket, isResp3Old);
                        return ErrorReply.AUTH_FAILED;
                    }

                    SocketInspector.setAuthUser(socket, user);
                    i += 2;
                } else if ("setname".equals(arg)) {
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }

                    var clientName = new String(data[i + 1]);
                    var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
                    veloUserData.setClientName(clientName);
                    i += 1;
                }
            }
        }

        var replies = new Reply[14];
        replies[0] = new BulkReply("server");
        replies[1] = new BulkReply("velo");
        replies[2] = new BulkReply("version");
        replies[3] = new BulkReply("1.0.0");
        replies[4] = new BulkReply("proto");
        replies[5] = new IntegerReply(3);
        replies[6] = new BulkReply("id");
        replies[7] = new IntegerReply(socket.hashCode());
        replies[8] = new BulkReply("mode");
        // no sentinel
        replies[9] = new BulkReply(ConfForGlobal.clusterEnabled ? "cluster" : "standalone");

        replies[10] = new BulkReply("role");
        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        var isSlave = firstOneSlot.isAsSlave();
        replies[11] = new BulkReply(isSlave ? "replica" : "master");

        replies[12] = new BulkReply("modules");
        replies[13] = MultiBulkReply.EMPTY;

        return new MultiBulkReply(replies, isResp3, false);
    }

    private Reply hexists() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hexists2(fieldBytes);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldCv = getCv(slot(fieldKey));
        return fieldCv != null ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;

//        var rhk = getRedisHashKeys(keyBytes);
//        if (rhk == null) {
//            return IntegerReply.REPLY_0;
//        }
//
//        return rhk.contains(new String(fieldBytes)) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    private Reply hexists2(byte[] fieldBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        final String[] fieldToFind = {new String(fieldBytes)};
        final boolean[] isFind = {false};
        RedisHH.iterate(encodedBytes, true, (field, value, expireAt) -> {
            if (field.equals(fieldToFind[0])) {
                isFind[0] = true;
                return true;
            }
            return false;
        });

        return isFind[0] ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
    static final IntegerReply FIELD_NOT_FOUND = new IntegerReply(-2);
    private static final IntegerReply FIELD_HAS_NO_EXPIRE = new IntegerReply(-1);

    private Reply hIterateFields(boolean isPersist, boolean isMilliseconds, boolean isTtl) {
        // HPERSIST key FIELDS numfields field [field ...]
        // HEXPIRETIME key FIELDS numfields field [field ...]
        // HPEXPIRETIME key FIELDS numfields field [field ...]
        // HTTL key FIELDS numfields field [field ...]
        // HPTTL key FIELDS numfields field [field ...]
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (!"fields".equalsIgnoreCase(new String(data[2]))) {
            return ErrorReply.SYNTAX;
        }

        int numFields;
        try {
            numFields = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        // check if we have enough fields
        if (data.length < 4 + numFields) {
            return ErrorReply.SYNTAX;
        }

        // validate field lengths
        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            if (data[4 + i].length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            fields.add(new String(data[4 + i]));
        }

        if (isUseHH(keyBytes)) {
            return hIterateFields2(fields, isPersist, isMilliseconds, isTtl);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            var replies = new Reply[numFields];
            for (int i = 0; i < numFields; i++) {
                replies[i] = FIELD_NOT_FOUND;
            }
            return new MultiBulkReply(replies);
        }

        boolean ttlCacheModified = false;
        var replies = new Reply[numFields];
        for (int i = 0; i < numFields; i++) {
            var field = fields.get(i);
            var isFieldExist = rhk.contains(field);
            if (!isFieldExist) {
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldSlotWithKeyHash = slot(fieldKey);
            var fieldCv = getCv(fieldSlotWithKeyHash);
            if (fieldCv == null) {
                // may be expired at just this time
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldExpireAt = fieldCv.getExpireAt();
            if (fieldExpireAt == CompressedValue.NO_EXPIRE) {
                replies[i] = FIELD_HAS_NO_EXPIRE;
                continue;
            }

            if (isPersist) {
                // remove expiration
                fieldCv.setSeq(snowFlake.nextId());
                fieldCv.setExpireAt(CompressedValue.NO_EXPIRE);
                setCv(fieldCv, fieldSlotWithKeyHash);
                rhk.putCachedExpireAt(field, CompressedValue.NO_EXPIRE);
                ttlCacheModified = true;

                replies[i] = IntegerReply.REPLY_1;
            } else if (isTtl) {
                var ttlMillis = fieldExpireAt - System.currentTimeMillis();
                replies[i] = isMilliseconds ? new IntegerReply(ttlMillis) : new IntegerReply(ttlMillis / 1000);
            } else {
                // get expire time
                replies[i] = isMilliseconds ? new IntegerReply(fieldExpireAt) : new IntegerReply(fieldExpireAt / 1000);
            }
        }

        if (ttlCacheModified) {
            saveRedisHashKeys(rhk, key);
        }

        return new MultiBulkReply(replies);
    }

    private Reply hIterateFields2(ArrayList<String> fields, boolean isPersist, boolean isMilliseconds, boolean isTtl) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            var replies = new Reply[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                replies[i] = FIELD_NOT_FOUND;
            }
            return new MultiBulkReply(replies);
        }

        var numFields = fields.size();
        var map = rhh.getMap();

        var replies = new Reply[numFields];
        for (int i = 0; i < numFields; i++) {
            var field = fields.get(i);
            var isFieldExist = map.containsKey(field);
            if (!isFieldExist) {
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldExpireAt = rhh.getExpireAt(field);
            if (fieldExpireAt == CompressedValue.NO_EXPIRE) {
                replies[i] = FIELD_HAS_NO_EXPIRE;
                continue;
            }

            if (isPersist) {
                // remove expiration
                rhh.putExpireAt(field, CompressedValue.NO_EXPIRE);
                replies[i] = IntegerReply.REPLY_1;
            } else if (isTtl) {
                var ttlMillis = fieldExpireAt - System.currentTimeMillis();
                replies[i] = isMilliseconds ? new IntegerReply(ttlMillis) : new IntegerReply(ttlMillis / 1000);
            } else {
                // get expire time
                replies[i] = isMilliseconds ? new IntegerReply(fieldExpireAt) : new IntegerReply(fieldExpireAt / 1000);
            }
        }

        if (isPersist) {
            saveRedisHH(rhh, slotWithKeyHash);
        }
        return new MultiBulkReply(replies);
    }

    private Reply hexpire(boolean isAt, boolean isMilliseconds) {
        // HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
        // HPEXPIRE key milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
        // HEXPIREAT key timestamp [NX | XX | GT | LT] FIELDS numfields field [field ...]
        // HPEXPIREAT key timestamp [NX | XX | GT | LT] FIELDS numfields field [field ...]
        if (data.length < 6) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var timeBytes = data[2];
        long timeValue;
        try {
            timeValue = Long.parseLong(new String(timeBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        long expireAt;
        if (isMilliseconds) {
            expireAt = isAt ? timeValue : System.currentTimeMillis() + timeValue;
        } else {
            expireAt = isAt ? timeValue * 1000 : System.currentTimeMillis() + timeValue * 1000;
        }

        // parse options (NX | XX | GT | LT)
        boolean isNx = false;
        boolean isXx = false;
        boolean isGt = false;
        boolean isLt = false;
        int fieldsIndex = 3;

        if (data.length > 3) {
            var optionBytes = data[3];
            var option = new String(optionBytes);
            if ("nx".equalsIgnoreCase(option)) {
                isNx = true;
                fieldsIndex = 4;
            } else if ("xx".equalsIgnoreCase(option)) {
                isXx = true;
                fieldsIndex = 4;
            } else if ("gt".equalsIgnoreCase(option)) {
                isGt = true;
                fieldsIndex = 4;
            } else if ("lt".equalsIgnoreCase(option)) {
                isLt = true;
                fieldsIndex = 4;
            }
        }

        // check for fields keyword
        if (data.length <= fieldsIndex || !"fields".equalsIgnoreCase(new String(data[fieldsIndex]))) {
            return ErrorReply.SYNTAX;
        }
        fieldsIndex++;

        // data.length > 6
        assert data.length > fieldsIndex;
        int numFields;
        try {
            numFields = Integer.parseInt(new String(data[fieldsIndex]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        fieldsIndex++;

        // check if we have enough fields
        if (data.length < fieldsIndex + numFields) {
            return ErrorReply.SYNTAX;
        }

        // validate field lengths
        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            if (data[fieldsIndex + i].length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            fields.add(new String(data[fieldsIndex + i]));
        }

        if (isUseHH(keyBytes)) {
            return hexpire2(expireAt, isNx, isXx, isGt, isLt, fields);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            var replies = new Reply[numFields];
            for (int i = 0; i < numFields; i++) {
                replies[i] = FIELD_NOT_FOUND;
            }
            return new MultiBulkReply(replies);
        }

        boolean ttlCacheModified = false;
        var replies = new Reply[numFields];
        for (int i = 0; i < numFields; i++) {
            var field = fields.get(i);
            var isFieldExist = rhk.contains(field);
            if (!isFieldExist) {
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldSlotWithKeyHash = slot(fieldKey);
            var fieldCv = getCv(fieldSlotWithKeyHash);
            if (fieldCv == null) {
                // may be expired at just this time
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldExpireAt = fieldCv.getExpireAt();
            // check conditions
            if (skipSetExpireAt(expireAt, isNx, isXx, isGt, isLt, replies, i, fieldExpireAt)) {
                continue;
            }

            // set expiration
            fieldCv.setSeq(snowFlake.nextId());
            fieldCv.setExpireAt(expireAt);
            setCv(fieldCv, fieldSlotWithKeyHash);
            rhk.putCachedExpireAt(field, expireAt);
            ttlCacheModified = true;

            replies[i] = IntegerReply.REPLY_1;
        }

        if (ttlCacheModified) {
            saveRedisHashKeys(rhk, key);
        }

        return new MultiBulkReply(replies);
    }

    private boolean skipSetExpireAt(long expireAt, boolean isNx, boolean isXx, boolean isGt, boolean isLt, Reply[] replies, int fieldIndex, long fieldExpireAt) {
        if (isNx && fieldExpireAt != CompressedValue.NO_EXPIRE) {
            replies[fieldIndex] = IntegerReply.REPLY_0;
            return true;
        }
        if (isXx && fieldExpireAt == CompressedValue.NO_EXPIRE) {
            replies[fieldIndex] = IntegerReply.REPLY_0;
            return true;
        }
        if (isGt && fieldExpireAt != CompressedValue.NO_EXPIRE && fieldExpireAt >= expireAt) {
            replies[fieldIndex] = IntegerReply.REPLY_0;
            return true;
        }
        if (isLt && fieldExpireAt != CompressedValue.NO_EXPIRE && fieldExpireAt <= expireAt) {
            replies[fieldIndex] = IntegerReply.REPLY_0;
            return true;
        }
        return false;
    }

    private Reply hexpire2(long expireAt, boolean isNx, boolean isXx, boolean isGt, boolean isLt, ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            var replies = new Reply[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                replies[i] = FIELD_NOT_FOUND;
            }
            return new MultiBulkReply(replies);
        }

        var numFields = fields.size();
        var map = rhh.getMap();

        var replies = new Reply[numFields];
        for (int i = 0; i < numFields; i++) {
            var field = fields.get(i);
            var isFieldExist = map.containsKey(field);
            if (!isFieldExist) {
                replies[i] = FIELD_NOT_FOUND;
                continue;
            }

            var fieldExpireAt = rhh.getExpireAt(field);
            // check conditions
            if (skipSetExpireAt(expireAt, isNx, isXx, isGt, isLt, replies, i, fieldExpireAt)) {
                continue;
            }

            // set expiration
            rhh.putExpireAt(field, expireAt);
            replies[i] = IntegerReply.REPLY_1;
        }

        saveRedisHH(rhh, slotWithKeyHash);
        return new MultiBulkReply(replies);
    }

    private Reply hget(boolean onlyReturnLength) {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hget2(fieldBytes, onlyReturnLength);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var sFieldKey = slot(fieldKey);
        var fieldCv = getCv(sFieldKey);

        if (fieldCv == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }

        var fieldValueBytes = getValueBytesByCv(fieldCv, sFieldKey);
        return onlyReturnLength ? new IntegerReply(fieldValueBytes.length) : new BulkReply(fieldValueBytes);
    }

    private Reply hget2(byte[] fieldBytes, boolean onlyReturnLength) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }

        final String[][] fieldToFind = {{new String(fieldBytes)}};
        final byte[][] fieldValueBytes = {null};
        RedisHH.iterate(encodedBytes, true, (field, value, expireAt) -> {
            if (field.equals(fieldToFind[0][0])) {
                fieldValueBytes[0] = value;
                return true;
            }
            return false;
        });

        var fieldValueByte = fieldValueBytes[0];
        if (fieldValueByte == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }
        return onlyReturnLength ? new IntegerReply(fieldValueByte.length) : new BulkReply(fieldValueByte);
    }

    private Reply hgetall() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hgetall2();
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var liveFields = rhk.liveFieldsByCache();
        if (liveFields.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[liveFields.size() * 2];
        int i = 0;
        for (var field : liveFields) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var sFieldKey = slot(fieldKey);
            var fieldCv = getCv(sFieldKey);
            replies[i++] = new BulkReply(field);
            replies[i++] = fieldCv == null ? NilReply.INSTANCE : new BulkReply(getValueBytesByCv(fieldCv, sFieldKey));
        }
        return new MultiBulkReply(replies);
    }

    private Reply hgetall2() {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            return MultiBulkReply.EMPTY;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[map.size() * 2];
        int i = 0;
        for (var entry : map.entrySet()) {
            replies[i++] = new BulkReply(entry.getKey());
            replies[i++] = new BulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }

    private Reply hgetdel() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (!"fields".equalsIgnoreCase(new String(data[2]))) {
            return ErrorReply.SYNTAX;
        }

        int numFields;
        try {
            numFields = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numFields < 0) {
            return ErrorReply.NOT_INTEGER;
        }

        if (data.length != 4 + numFields) {
            return ErrorReply.SYNTAX;
        }

        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            var fieldBytes = data[4 + i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            fields.add(new String(fieldBytes));
        }

        if (isUseHH(keyBytes)) {
            return hgetdel2(fields);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            var replies = new Reply[numFields];
            for (int i = 0; i < numFields; i++) {
                replies[i] = NilReply.INSTANCE;
            }
            return new MultiBulkReply(replies);
        }

        var replies = new Reply[numFields];
        boolean anyRemoved = false;
        for (int i = 0; i < numFields; i++) {
            var field = fields.get(i);
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var sFieldKey = slot(fieldKey);
            var fieldCv = getCv(sFieldKey);
            if (fieldCv == null) {
                replies[i] = NilReply.INSTANCE;
            } else {
                replies[i] = new BulkReply(getValueBytesByCv(fieldCv, sFieldKey));
            }

            if (rhk.remove(field)) {
                anyRemoved = true;
                removeDelay(sFieldKey);
            }
        }

        if (anyRemoved) {
            saveRedisHashKeys(rhk, key);
        }

        return new MultiBulkReply(replies);
    }

    private Reply hgetdel2(ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            var replies = new Reply[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                replies[i] = NilReply.INSTANCE;
            }
            return new MultiBulkReply(replies);
        }

        var replies = new Reply[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            var field = fields.get(i);
            var value = rhh.get(field);
            replies[i] = value == null ? NilReply.INSTANCE : new BulkReply(value);
            rhh.remove(field);
        }

        saveRedisHH(rhh, slotWithKeyHash);
        return new MultiBulkReply(replies);
    }

    private Reply hgetex() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        long expireAt = CompressedValue.NO_EXPIRE;
        boolean hasExpireOption = false;
        int fieldsIndex = 2;

        if ("fields".equalsIgnoreCase(new String(data[fieldsIndex]))) {
            fieldsIndex++;
        } else {
            hasExpireOption = true;
            if ("ex".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                try {
                    long seconds = Long.parseLong(new String(data[fieldsIndex + 1]));
                    expireAt = System.currentTimeMillis() + seconds * 1000;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                fieldsIndex += 2;
            } else if ("px".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                try {
                    long ms = Long.parseLong(new String(data[fieldsIndex + 1]));
                    expireAt = System.currentTimeMillis() + ms;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                fieldsIndex += 2;
            } else if ("exat".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                try {
                    long unixSec = Long.parseLong(new String(data[fieldsIndex + 1]));
                    expireAt = unixSec * 1000;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                fieldsIndex += 2;
            } else if ("pxat".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                try {
                    expireAt = Long.parseLong(new String(data[fieldsIndex + 1]));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                fieldsIndex += 2;
            } else if ("persist".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                fieldsIndex += 1;
            } else {
                return ErrorReply.SYNTAX;
            }

            if (data.length <= fieldsIndex || !"fields".equalsIgnoreCase(new String(data[fieldsIndex]))) {
                return ErrorReply.SYNTAX;
            }
            fieldsIndex++;
        }

        if (data.length <= fieldsIndex) {
            return ErrorReply.SYNTAX;
        }

        int numFields;
        try {
            numFields = Integer.parseInt(new String(data[fieldsIndex]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        fieldsIndex++;

        if (numFields < 0) {
            return ErrorReply.NOT_INTEGER;
        }

        if (data.length != fieldsIndex + numFields) {
            return ErrorReply.SYNTAX;
        }

        ArrayList<String> fieldList = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            var fieldBytes = data[fieldsIndex + i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            fieldList.add(new String(fieldBytes));
        }

        if (isUseHH(keyBytes)) {
            return hgetex2(fieldList, expireAt, hasExpireOption);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            var replies = new Reply[numFields];
            for (int i = 0; i < numFields; i++) {
                replies[i] = NilReply.INSTANCE;
            }
            return new MultiBulkReply(replies);
        }

        var replies = new Reply[numFields];
        boolean ttlCacheModified = false;
        for (int i = 0; i < numFields; i++) {
            var field = fieldList.get(i);
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var sFieldKey = slot(fieldKey);
            var fieldCv = getCv(sFieldKey);
            if (fieldCv == null) {
                replies[i] = NilReply.INSTANCE;
            } else {
                replies[i] = new BulkReply(getValueBytesByCv(fieldCv, sFieldKey));
            }

            if (hasExpireOption && fieldCv != null) {
                fieldCv.setSeq(snowFlake.nextId());
                fieldCv.setExpireAt(expireAt);
                setCv(fieldCv, sFieldKey);
                rhk.putCachedExpireAt(field, expireAt);
                ttlCacheModified = true;
            }
        }

        if (ttlCacheModified) {
            saveRedisHashKeys(rhk, key);
        }

        return new MultiBulkReply(replies);
    }

    private Reply hgetex2(ArrayList<String> fields, long expireAt, boolean hasExpireOption) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            var replies = new Reply[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                replies[i] = NilReply.INSTANCE;
            }
            return new MultiBulkReply(replies);
        }

        var replies = new Reply[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            var field = fields.get(i);
            var value = rhh.get(field);
            replies[i] = value == null ? NilReply.INSTANCE : new BulkReply(value);
        }

        if (hasExpireOption) {
            for (var field : fields) {
                rhh.putExpireAt(field, expireAt);
            }
            saveRedisHH(rhh, slotWithKeyHash);
        }

        return new MultiBulkReply(replies);
    }

    private Reply hincrby(boolean isFloat) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];
        var byBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int by = 0;
        double byFloat = 0;
        if (isFloat) {
            try {
                byFloat = Double.parseDouble(new String(byBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }
        } else {
            try {
                by = Integer.parseInt(new String(byBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        if (isUseHH(keyBytes)) {
            return hincrby2(fieldBytes, by, byFloat, isFloat);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);

        var rhk = getRedisHashKeys(key);
        long oldCachedExpireAt = CompressedValue.NO_EXPIRE;
        if (rhk != null) {
            oldCachedExpireAt = rhk.getCachedExpireAt(field);
        }

        byte[][] dd = {null, Wal.keyBytes(fieldKey)};
        var dGroup = new DGroup(cmd, dd, socket);
        dGroup.from(this);
        dGroup.setSlotWithKeyHashListParsed(dGroup.parseSlots("decr", dd, slotNumber));

        Reply reply;
        if (isFloat) {
            reply = dGroup.decrBy(0, -byFloat);
        } else {
            reply = dGroup.decrBy(-by, 0);
        }

        if (reply instanceof IntegerReply || reply instanceof DoubleReply) {
            if (oldCachedExpireAt != CompressedValue.NO_EXPIRE && rhk != null) {
                rhk.putCachedExpireAt(field, CompressedValue.NO_EXPIRE);
                saveRedisHashKeys(rhk, key);
            }
        }

        return reply;
    }

    private Reply hincrby2(byte[] fieldBytes, int by, double byFloat, boolean isByFloat) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        var field = new String(fieldBytes);
        var fieldValueBytes = rhh.get(field);

        if (fieldValueBytes == null) {
            if (isByFloat) {
                var newValue = BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
                var newValueBytes = newValue.toPlainString().getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, slotWithKeyHash);
                return new DoubleReply(newValue);
            } else {
                var newValueBytes = String.valueOf(by).getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, slotWithKeyHash);
                return new IntegerReply(by);
            }
        } else {
            final var NOT_NUMBER_REPLY = isByFloat ? ErrorReply.NOT_FLOAT : ErrorReply.NOT_INTEGER;

            double doubleValue = 0;
            long longValue = 0;
            try {
                var numberStr = new String(fieldValueBytes);
                if (isByFloat) {
                    doubleValue = Double.parseDouble(numberStr);
                } else {
                    longValue = Long.parseLong(numberStr);
                }
            } catch (NumberFormatException e) {
                return NOT_NUMBER_REPLY;
            }

            if (isByFloat) {
                var newValue = BigDecimal.valueOf(doubleValue).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP)
                        .add(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));
                var newValueBytes = newValue.toPlainString().getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, slotWithKeyHash);
                return new DoubleReply(newValue);
            } else {
                long newValue = longValue + by;
                var newValueBytes = String.valueOf(newValue).getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, slotWithKeyHash);
                return new IntegerReply(newValue);
            }
        }
    }

    private Reply hkeys(boolean onlyReturnSize) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hkeys2(onlyReturnSize);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();

        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        var liveFields = rhk.liveFieldsByCache();

        if (onlyReturnSize) {
            return new IntegerReply(liveFields.size());
        }

        if (liveFields.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[liveFields.size()];
        int i = 0;
        for (var field : liveFields) {
            replies[i++] = new BulkReply(field);
        }
        return new MultiBulkReply(replies);
    }

    private Reply hkeys2(boolean onlyReturnSize) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        var rhh = RedisHH.decode(encodedBytes);
        var size = rhh.size();
        if (size == 0) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        if (onlyReturnSize) {
            return new IntegerReply(size);
        }

        var replies = new Reply[size];
        final int[] i = {0};
        rhh.iterate((field, value, expireAt) -> {
            replies[i[0]++] = new BulkReply(field);
            return false;
        });
        return new MultiBulkReply(replies);
    }

    private Reply hmget() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var fields = new ArrayList<String>();
        for (int i = 2; i < data.length; i++) {
            var fieldBytes = data[i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            fields.add(new String(fieldBytes));
        }

        if (isUseHH(keyBytes)) {
            return hmget2(fields);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();

        var replies = new Reply[fields.size()];
        int i = 0;
        for (var field : fields) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = get(slot(fieldKey));
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    private Reply hmget2(ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);

        var replies = new Reply[fields.size()];
        int i = 0;
        for (var field : fields) {
            var fieldValueBytes = rhh == null ? null : rhh.get(field);
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    private Reply hmset(boolean isHset) {
        if (data.length < 4 || data.length % 2 != 0) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        LinkedHashMap<String, byte[]> fieldValues = new LinkedHashMap<>();
        for (int i = 2; i < data.length; i += 2) {
            var fieldBytes = data[i];
            var fieldValueBytes = data[i + 1];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            if (fieldValueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                return ErrorReply.VALUE_TOO_LONG;
            }

            if (fieldBytes.length == 0 || fieldValueBytes.length == 0) {
                return ErrorReply.SYNTAX;
            }

            fieldValues.put(new String(fieldBytes), fieldValueBytes);
        }

        if (isUseHH(keyBytes)) {
            return hmset2(fieldValues, isHset);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        int newFieldsCount = 0;
        for (var entry : fieldValues.entrySet()) {
            if (rhk.size() >= RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.HASH_SIZE_TO_LONG;
            }

            var field = entry.getKey();
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = entry.getValue();
            var slotWithKeyHashThisField = slot(fieldKey);
            set(fieldValueBytes, slotWithKeyHashThisField);
            rhk.putCachedExpireAt(field, CompressedValue.NO_EXPIRE);

            if (rhk.add(field)) {
                newFieldsCount++;
            }
        }

        saveRedisHashKeys(rhk, key);
        if (isHset) {
            return new IntegerReply(newFieldsCount);
        } else {
            return OKReply.INSTANCE;
        }
    }

    private Reply hmset2(LinkedHashMap<String, byte[]> fieldValues, boolean isHset) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        int newFieldsCount = 0;
        for (var entry : fieldValues.entrySet()) {
            if (rhh.size() >= RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.HASH_SIZE_TO_LONG;
            }

            var isNew = rhh.get(entry.getKey()) == null;
            rhh.put(entry.getKey(), entry.getValue());
            if (isNew) {
                newFieldsCount++;
            }
        }

        saveRedisHH(rhh, slotWithKeyHash);
        if (isHset) {
            return new IntegerReply(newFieldsCount);
        } else {
            return OKReply.INSTANCE;
        }
    }

    private Reply hsetex() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean fnx = false;
        boolean fxx = false;
        int idx = 2;

        var option = new String(data[idx]);
        if ("fnx".equalsIgnoreCase(option)) {
            fnx = true;
            idx++;
        } else if ("fxx".equalsIgnoreCase(option)) {
            fxx = true;
            idx++;
        }

        long expireAt = CompressedValue.NO_EXPIRE;
        boolean hasExpireOption = false;
        boolean keepttl = false;

        var expOption = new String(data[idx]);
        if ("ex".equalsIgnoreCase(expOption)) {
            try {
                long seconds = Long.parseLong(new String(data[idx + 1]));
                expireAt = System.currentTimeMillis() + seconds * 1000;
                hasExpireOption = true;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            idx += 2;
        } else if ("px".equalsIgnoreCase(expOption)) {
            try {
                long ms = Long.parseLong(new String(data[idx + 1]));
                expireAt = System.currentTimeMillis() + ms;
                hasExpireOption = true;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            idx += 2;
        } else if ("exat".equalsIgnoreCase(expOption)) {
            try {
                long unixSec = Long.parseLong(new String(data[idx + 1]));
                expireAt = unixSec * 1000;
                hasExpireOption = true;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            idx += 2;
        } else if ("pxat".equalsIgnoreCase(expOption)) {
            try {
                expireAt = Long.parseLong(new String(data[idx + 1]));
                hasExpireOption = true;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            idx += 2;
        } else if ("keepttl".equalsIgnoreCase(expOption)) {
            keepttl = true;
            idx++;
        }

        if (data.length <= idx || !"fields".equalsIgnoreCase(new String(data[idx]))) {
            return ErrorReply.SYNTAX;
        }
        idx++;

        if (data.length <= idx) {
            return ErrorReply.SYNTAX;
        }

        int numFields;
        try {
            numFields = Integer.parseInt(new String(data[idx]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        idx++;

        if (numFields < 0) {
            return ErrorReply.NOT_INTEGER;
        }

        if (data.length != idx + numFields * 2) {
            return ErrorReply.SYNTAX;
        }

        LinkedHashMap<String, byte[]> fieldValues = new LinkedHashMap<>();
        for (int i = 0; i < numFields; i++) {
            var fieldBytes = data[idx++];
            var valueBytes = data[idx++];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            fieldValues.put(new String(fieldBytes), valueBytes);
        }

        if (isUseHH(keyBytes)) {
            return hsetex2(fieldValues, fnx, fxx, expireAt, hasExpireOption, keepttl);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        int setCount = 0;
        for (var entry : fieldValues.entrySet()) {
            var field = entry.getKey();
            var value = entry.getValue();

            if (fnx && rhk.contains(field)) {
                continue;
            }
            if (fxx && !rhk.contains(field)) {
                continue;
            }

            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var sFieldKey = slot(fieldKey);

            long fieldExpireAt = keepttl ? CompressedValue.NO_EXPIRE : expireAt;
            set(value, sFieldKey, CompressedValue.NULL_DICT_SEQ, fieldExpireAt);

            rhk.add(field);
            rhk.putCachedExpireAt(field, fieldExpireAt);
            setCount++;
        }

        saveRedisHashKeys(rhk, key);
        return new IntegerReply(setCount > 0 ? 1 : 0);
    }

    private Reply hsetex2(LinkedHashMap<String, byte[]> fieldValues, boolean fnx, boolean fxx,
                          long expireAt, boolean hasExpireOption, boolean keepttl) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        int setCount = 0;
        for (var entry : fieldValues.entrySet()) {
            var field = entry.getKey();
            var value = entry.getValue();

            if (fnx && rhh.get(field) != null) {
                continue;
            }
            if (fxx && rhh.get(field) == null) {
                continue;
            }

            long fieldExpireAt = keepttl ? CompressedValue.NO_EXPIRE : expireAt;
            rhh.put(field, value, fieldExpireAt);
            setCount++;
        }

        saveRedisHH(rhh, slotWithKeyHash);
        return new IntegerReply(setCount > 0 ? 1 : 0);
    }

    private Reply hrandfield() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        boolean withValues = false;
        for (int i = 2; i < data.length; i++) {
            var arg = new String(data[i]);
            if ("withvalues".equalsIgnoreCase(arg)) {
                withValues = true;
            } else {
                try {
                    count = Integer.parseInt(arg);
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
            }
        }

        if (isUseHH(keyBytes)) {
            return hrandfield2(count, withValues);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var liveFields = rhk.liveFieldsByCache();
        if (liveFields.isEmpty()) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = liveFields.size();
        if (count > size) {
            count = size;
        }
        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = getRandIndex(count, size, absCount);

        var replies = new Reply[withValues ? absCount * 2 : absCount];
        int i = 0;
        int j = 0;
        for (var field : liveFields) {
            if (indexes.contains(j)) {
                replies[i++] = new BulkReply(field);
                if (withValues) {
                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    var fieldValueBytes = get(slot(fieldKey));
                    replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
                }
            }
            j++;
        }
        return new MultiBulkReply(replies);
    }

    private Reply hrandfield2(int count, boolean withValues) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = map.size();
        if (count > size) {
            count = size;
        }
        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = getRandIndex(count, size, absCount);

        var replies = new Reply[withValues ? absCount * 2 : absCount];
        int i = 0;
        int j = 0;
        for (var entry : map.entrySet()) {
            if (indexes.contains(j)) {
                var field = entry.getKey();
                var fieldValueBytes = entry.getValue();
                replies[i++] = new BulkReply(field);
                if (withValues) {
                    replies[i++] = new BulkReply(fieldValueBytes);
                }
            }
            j++;
        }
        return new MultiBulkReply(replies);
    }

    @NotNull
    static ArrayList<Integer> getRandIndex(int count, int size, int absCount) {
        ArrayList<Integer> indexes = new ArrayList<>();
        if (count == size) {
            // need not random, return all fields
            for (int i = 0; i < size; i++) {
                indexes.add(i);
            }
        } else {
            boolean canUseSameField = count < 0;
            var rand = new Random();
            for (int i = 0; i < absCount; i++) {
                int index;
                do {
                    index = rand.nextInt(size);
                } while (!canUseSameField && indexes.contains(index));
                indexes.add(index);
            }
        }
        return indexes;
    }

    private Reply hscan() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var cursorBytes = data[2];
        var cursor = new String(cursorBytes);
        long cursorLong;
        try {
            cursorLong = Long.parseLong(cursor);
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        String matchPattern = null;
        int count = 10;
        boolean noValues = false;
        if (data.length > 3) {
            for (int i = 3; i < data.length; i++) {
                var tmp = new String(data[i]).toLowerCase();
                if ("match".equals(tmp)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    matchPattern = new String(data[i + 1]);
                    i++;
                } else if ("count".equals(tmp)) {
                    if (data.length <= i + 1) {
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
                    i++;
                } else if ("novalues".equals(tmp)) {
                    noValues = true;
                } else if ("withvalues".equals(tmp)) {
                    noValues = false;
                } else {
                    return ErrorReply.SYNTAX;
                }
            }
        }

        if (isUseHH(keyBytes)) {
            return hscan2(cursorLong, matchPattern, count, noValues);
        }

        // performance bad
        if (!noValues) {
            return ErrorReply.NOT_SUPPORT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null || rhk.size() == 0) {
            return MultiBulkReply.SCAN_EMPTY;
        }

        var liveFields = rhk.liveFieldsByCache();
        if (liveFields.isEmpty()) {
            return MultiBulkReply.SCAN_EMPTY;
        }

        long skipCount = cursorLong;

        ArrayList<String> matchFields = new ArrayList<>();
        int loopCount = 0;
        for (var field : liveFields) {
            loopCount++;
            if (!KeyLoader.isKeyMatch(field, matchPattern)) {
                continue;
            }

            if (skipCount > 0) {
                skipCount--;
                continue;
            }

            matchFields.add(field);
            if (matchFields.size() >= count) {
                break;
            }
        }

        if (matchFields.isEmpty()) {
            return MultiBulkReply.SCAN_EMPTY;
        }

        var replies = new Reply[matchFields.size()];
        int i = 0;
        for (var field : matchFields) {
            replies[i++] = new BulkReply(field);
        }

        var isEnd = loopCount == liveFields.size();
        var nextCursor = String.valueOf(isEnd ? 0L : cursorLong + matchFields.size());
        // always end
        return new MultiBulkReply(new Reply[]{new BulkReply(nextCursor), new MultiBulkReply(replies)});
    }

    private Reply hscan2(long cursorLong, String matchPattern, int count, boolean noValues) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null || rhh.size() == 0) {
            return MultiBulkReply.SCAN_EMPTY;
        }

        long skipCount = cursorLong;

        ArrayList<String> matchFields = new ArrayList<>();
        int loopCount = 0;
        for (var entry : rhh.getMap().entrySet()) {
            loopCount++;
            var field = entry.getKey();
            if (!KeyLoader.isKeyMatch(field, matchPattern)) {
                continue;
            }

            if (skipCount > 0) {
                skipCount--;
                continue;
            }

            matchFields.add(field);
            if (matchFields.size() >= count) {
                break;
            }
        }

        if (matchFields.isEmpty()) {
            return MultiBulkReply.SCAN_EMPTY;
        }

        var replies = new Reply[noValues ? matchFields.size() : matchFields.size() * 2];
        int i = 0;
        for (var field : matchFields) {
            replies[i++] = new BulkReply(field);
            if (!noValues) {
                replies[i++] = new BulkReply(rhh.get(field));
            }
        }

        var isEnd = loopCount == rhh.size();
        var nextCursor = String.valueOf(isEnd ? 0L : cursorLong + matchFields.size());
        return new MultiBulkReply(new Reply[]{new BulkReply(nextCursor), new MultiBulkReply(replies)});
    }

    private Reply hsetnx() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];
        var fieldValueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldValueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        if (fieldBytes.length == 0 || fieldValueBytes.length == 0) {
            return ErrorReply.SYNTAX;
        }

        if (isUseHH(keyBytes)) {
            return hsetnx2(fieldBytes, fieldValueBytes);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        var field = new String(fieldBytes);
        if (rhk.contains(field)) {
            return IntegerReply.REPLY_0;
        }

        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var slotWithKeyHashThisField = slot(fieldKey);

        var fieldCv = getCv(slotWithKeyHashThisField);
        if (fieldCv != null) {
            throw new IllegalStateException("Hash field cv exists, key=" + key + ", field=" + field);
//            return IntegerReply.REPLY_0;
        }

        set(fieldValueBytes, slotWithKeyHashThisField);
        rhk.add(field);
        saveRedisHashKeys(rhk, key);
        return IntegerReply.REPLY_1;
    }

    private Reply hsetnx2(byte[] fieldBytes, byte[] fieldValueBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        var field = new String(fieldBytes);
        if (rhh.get(field) != null) {
            return IntegerReply.REPLY_0;
        }

        rhh.put(field, fieldValueBytes);
        saveRedisHH(rhh, slotWithKeyHash);
        return IntegerReply.REPLY_1;
    }

    private Reply hvals() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hvals2();
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var rhk = getRedisHashKeys(key);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var liveFields = rhk.liveFieldsByCache();
        if (liveFields.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[liveFields.size()];
        int i = 0;
        for (var field : liveFields) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = get(slot(fieldKey));
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    private Reply hvals2() {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(slotWithKeyHash);
        if (rhh == null) {
            return MultiBulkReply.EMPTY;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[map.size()];
        int i = 0;
        for (var entry : map.entrySet()) {
            replies[i++] = new BulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }
}
