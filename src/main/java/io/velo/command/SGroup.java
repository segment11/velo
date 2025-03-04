package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.*;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.persist.KeyLoader;
import io.velo.persist.ScanCursor;
import io.velo.repl.LeaderSelector;
import io.velo.reply.*;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.velo.CompressedValue.NO_EXPIRE;

public class SGroup extends BaseCommand {
    public SGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("set".equals(cmd) || "setbit".equals(cmd) || "setex".equals(cmd) || "setrange".equals(cmd) ||
                "setnx".equals(cmd) || "strlen".equals(cmd) || "substr".equals(cmd) ||
                "sadd".equals(cmd) || "scard".equals(cmd) ||
                "sismember".equals(cmd) || "smembers".equals(cmd) || "smismember".equals(cmd) ||
                "sort".equals(cmd) || "sort_ro".equals(cmd) ||
                "spop".equals(cmd) || "srandmember".equals(cmd) || "srem".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        if ("sdiff".equals(cmd) || "sinter".equals(cmd) || "sunion".equals(cmd) ||
                "sdiffstore".equals(cmd) || "sinterstore".equals(cmd) || "sunionstore".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("sintercard".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin2);
            return slotWithKeyHashList;
        }

        if ("smove".equals(cmd)) {
            if (data.length != 4) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1And2);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("save".equals(cmd)) {
            BGroup.lastBgSaveMillis = System.currentTimeMillis();
            return save();
        }

        if ("scan".equals(cmd)) {
            return scan();
        }

        if ("sentinel".equals(cmd)) {
            return sentinel();
        }

        if ("set".equals(cmd)) {
            return set(data);
        }

        if ("setbit".equals(cmd)) {
            return setbit();
        }

        if ("setex".equals(cmd)) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[3], "ex".getBytes(), data[2]};
            return set(dd);
        }

        if ("setnx".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[2], "nx".getBytes()};
            var reply = set(dd);
            if (reply instanceof ErrorReply) {
                return reply;
            }
            return reply == OKReply.INSTANCE ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }

        if ("setrange".equals(cmd)) {
            return setrange();
        }

        if ("strlen".equals(cmd)) {
            return strlen();
        }

        if ("substr".equals(cmd)) {
            var gGroup = new GGroup(cmd, data, socket);
            gGroup.from(this);
            return gGroup.getrange();
        }

        if ("select".equals(cmd)) {
            return select();
        }

        // set group
        if ("sadd".equals(cmd)) {
            return sadd();
        }

        if ("scard".equals(cmd)) {
            return scard();
        }

        if ("sdiff".equals(cmd)) {
            return sdiff(false, false);
        }

        if ("sdiffstore".equals(cmd)) {
            return sdiffstore(false, false);
        }

        if ("sinter".equals(cmd)) {
            return sdiff(true, false);
        }

        if ("sintercard".equals(cmd)) {
            return sintercard();
        }

        if ("sinterstore".equals(cmd)) {
            return sdiffstore(true, false);
        }

        if ("sismember".equals(cmd)) {
            return sismember();
        }

        if ("smembers".equals(cmd)) {
            return smembers();
        }

        if ("smismember".equals(cmd)) {
            return smismember();
        }

        if ("smove".equals(cmd)) {
            return smove();
        }

        if ("sort".equals(cmd)) {
            return sort(true);
        }

        if ("sort_ro".equals(cmd)) {
            return sort(false);
        }

        if ("spop".equals(cmd)) {
            return srandmember(true);
        }

        if ("srandmember".equals(cmd)) {
            return srandmember(false);
        }

        if ("srem".equals(cmd)) {
            return srem();
        }

        if ("subscribe".equals(cmd)) {
            return subscribe(false);
        }

        if ("sunion".equals(cmd)) {
            return sdiff(false, true);
        }

        if ("sunionstore".equals(cmd)) {
            return sdiffstore(false, true);
        }

        if ("slaveof".equals(cmd)) {
            return slaveof();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply save() {
        if (!ConfForGlobal.pureMemory) {
            return OKReply.INSTANCE;
        }

        return localPersist.doSthInSlots(oneSlot -> {
            try {
                oneSlot.writeToSavedFileWhenPureMemory();
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, resultList -> OKReply.INSTANCE);
    }

    private MultiBulkReply scanResultToReply(KeyLoader.ScanCursorWithReturnKeys r, VeloUserDataInSocket veloUserData, int count) {
        var keys = r.keys();
        assert (!keys.isEmpty());
        if (keys.size() < count) {
            // reach the end
            veloUserData.setLastScanAssignCursor(0);
            veloUserData.setBeginScanSeq(0);
        } else {
            veloUserData.setLastScanAssignCursor(r.scanCursor().toLong());
        }

        var replies = new Reply[2];
        replies[0] = new BulkReply(r.scanCursor().toLong());

        var keysReplies = new Reply[keys.size()];
        replies[1] = new MultiBulkReply(keysReplies);

        int i = 0;
        for (var key : keys) {
            keysReplies[i++] = new BulkReply(key.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply scan() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var cursorBytes = data[1];
        var cursor = new String(cursorBytes);
        long cursorLong;
        try {
            cursorLong = Long.parseLong(cursor);
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        var scanCursor = ScanCursor.fromLong(cursorLong);

        var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
        if (cursorLong == 0) {
            veloUserData.setLastScanAssignCursor(0);
        } else {
            if (cursorLong != veloUserData.getLastScanAssignCursor()) {
                return new ErrorReply("cursor is not match");
            }
        }

        String matchPattern = null;
        String type = null;
        int count = 10;
        if (data.length > 2) {
            for (int i = 2; i < data.length; i++) {
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
                } else if ("type".equals(tmp)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    type = new String(data[i + 1]).toLowerCase();
                    i++;
                } else {
                    return ErrorReply.SYNTAX;
                }
            }
        }

        // string
        byte typeAsByte = KeyLoader.typeAsByteIgnore;
        if (type != null) {
            // only support string / hash / list / set / zset
            var isString = "string".equals(type);
            var isHash = "hash".equals(type);
            var isList = "list".equals(type);
            var isSet = "set".equals(type);
            var isZset = "zset".equals(type);
            var isAny = "*".equals(type);
            if (!isString && !isHash && !isList && !isSet && !isZset && !isAny) {
                return new ErrorReply("type not support");
            }

            if (isHash) {
                typeAsByte = KeyLoader.typeAsByteHash;
            } else if (isList) {
                typeAsByte = KeyLoader.typeAsByteList;
            } else if (isSet) {
                typeAsByte = KeyLoader.typeAsByteSet;
            } else if (isZset) {
                typeAsByte = KeyLoader.typeAsByteZSet;
            } else if (isString) {
                typeAsByte = KeyLoader.typeAsByteString;
            }
        }

        var oneSlot = localPersist.oneSlot(scanCursor.slot());
        if (cursorLong == 0) {
            veloUserData.setBeginScanSeq(oneSlot.getSnowFlake().getLastNextId());
        }

        int leftCount = count;

        KeyLoader.ScanCursorWithReturnKeys rWal = null;
        if (!scanCursor.isWalIterateEnd()) {
            var wal = oneSlot.getWalByGroupIndex(scanCursor.walGroupIndex());
            rWal = wal.scan(scanCursor.walSkipCount(), typeAsByte, matchPattern, count, veloUserData.getBeginScanSeq());

            if (rWal != null) {
                if (rWal.keys().size() == count) {
                    // need not scan key buckets
                    return scanResultToReply(rWal, veloUserData, count);
                } else {
                    leftCount -= rWal.keys().size();
                }
            }
        }

        var keyLoader = oneSlot.getKeyLoader();
        var r = keyLoader.scan(scanCursor.walGroupIndex(), scanCursor.splitIndex(), scanCursor.keyBucketsSkipCount(),
                typeAsByte, matchPattern, leftCount, veloUserData.getBeginScanSeq());

        if (r == null || r.keys().isEmpty()) {
            if (rWal != null) {
                // only return wal scan result
                var rAll = new KeyLoader.ScanCursorWithReturnKeys(ScanCursor.END, rWal.keys());
                var finalReply = scanResultToReply(rAll, veloUserData, count);
                // overwrite, reach the end
                veloUserData.setLastScanAssignCursor(0L);
                return finalReply;
            } else {
                // reach the end
                veloUserData.setLastScanAssignCursor(0L);
                return MultiBulkReply.SCAN_EMPTY;
            }
        } else {
            if (rWal != null) {
                // merged keys from wal scan
                r.keys().addAll(rWal.keys());
            }
        }

        return scanResultToReply(r, veloUserData, count);
    }

    private Reply sentinel() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/SentinelCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("sGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }

    private static final String IPV4_REGEX =
            "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";

    public static final Pattern IPv4_PATTERN = Pattern.compile(IPV4_REGEX);

    @VisibleForTesting
    Reply slaveof() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var host = new String(data[1]);

        var leaderSelector = LeaderSelector.getInstance();

        var isNoOne = "no".equalsIgnoreCase(host);
        if (isNoOne) {
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            leaderSelector.resetAsMaster(true, (e) -> {
                if (e != null) {
                    log.error("slaveof error={}", e.getMessage());
                    finalPromise.set(new ErrorReply(e.getMessage()));
                    return;
                }

                finalPromise.set(OKReply.INSTANCE);
            });

            return asyncReply;
        }

        var matcher = IPv4_PATTERN.matcher(host);
        if (!matcher.matches()) {
            return ErrorReply.SYNTAX;
        }

        int port;
        try {
            port = Integer.parseInt(new String(data[2]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (port < 0 || port > 65535) {
            return ErrorReply.INVALID_INTEGER;
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        leaderSelector.resetAsSlave(host, port, (e) -> {
            if (e != null) {
                log.error("slaveof error={}", e.getMessage());
                finalPromise.set(new ErrorReply(e.getMessage()));
                return;
            }

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply set(byte[][] dd) {
        if (dd.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = dd[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        // for local test, random value, test compress ratio
        var valueBytes = dd[2];
        if (localTest) {
            int randomValueIndex = new Random().nextInt(localTestRandomValueList.size());
            var randomValueBytes = localTestRandomValueList.get(randomValueIndex);

            valueBytes = new byte[randomValueBytes.length];

            // change last 16 bytes same as key
            System.arraycopy(randomValueBytes, 0, valueBytes, 0, valueBytes.length - keyBytes.length);
            System.arraycopy(keyBytes, 0, valueBytes, valueBytes.length - keyBytes.length, keyBytes.length);
        }

        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        boolean isNx = false;
        boolean isXx = false;
        long ex = -1;
        long px = -1;
        long exAt = -1;
        long pxAt = -1;
        boolean isExpireAtSet = false;
        boolean isKeepTtl = false;
        boolean isReturnExist = false;
        for (int i = 3; i < dd.length; i++) {
            var arg = new String(dd[i]);
            isNx = "nx".equalsIgnoreCase(arg);
            isXx = "xx".equalsIgnoreCase(arg);
            if (isNx || isXx) {
                continue;
            }

            isKeepTtl = "keepttl".equalsIgnoreCase(arg);
            if (isKeepTtl) {
                continue;
            }

            isReturnExist = "get".equalsIgnoreCase(arg);
            if (isReturnExist) {
                continue;
            }

            boolean isEx = "ex".equalsIgnoreCase(arg);
            boolean isPx = "px".equalsIgnoreCase(arg);
            boolean isExAt = "exat".equalsIgnoreCase(arg);
            boolean isPxAt = "pxat".equalsIgnoreCase(arg);

            isExpireAtSet = isEx || isPx || isExAt || isPxAt;
            if (!isExpireAtSet) {
                continue;
            }

            if (dd.length <= i + 1) {
                return ErrorReply.SYNTAX;
            }
            long value;
            try {
                value = Long.parseLong(new String(dd[i + 1]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (isEx) {
                ex = value;
            } else if (isPx) {
                px = value;
            } else if (isExAt) {
                exAt = value;
            } else {
//            } else if (isPxAt) {
                pxAt = value;
            }

            i++;
        }

        long expireAt = NO_EXPIRE;
        if (isExpireAtSet) {
            if (ex != -1) {
                expireAt = System.currentTimeMillis() + ex * 1000;
            } else if (px != -1) {
                expireAt = System.currentTimeMillis() + px;
            } else if (exAt != -1) {
                expireAt = exAt * 1000;
            } else if (pxAt != -1) {
                expireAt = pxAt;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        CompressedValue cv = null;
        if (isReturnExist || isNx || isXx || isKeepTtl) {
            cv = getCv(keyBytes, slotWithKeyHash);
            boolean isOldExist = cv != null;
            if (isNx && isOldExist) {
                return NilReply.INSTANCE;
            }
            if (isXx && !isOldExist) {
                return NilReply.INSTANCE;
            }

            // check if not string type
            if (isOldExist && isReturnExist) {
                if (!cv.isTypeString()) {
                    log.debug("Key {} is not string type", new String(keyBytes));
                    return ErrorReply.NOT_STRING;
                }
            }

            // keep ttl
            if (isOldExist && isKeepTtl) {
                expireAt = cv.getExpireAt();
            }

            set(keyBytes, valueBytes, slotWithKeyHash, 0, expireAt);
        } else {
            set(keyBytes, valueBytes, slotWithKeyHash, 0, expireAt);
        }

        if (isReturnExist) {
            if (cv == null) {
                return NilReply.INSTANCE;
            } else {
                return new BulkReply(getValueBytesByCv(cv, keyBytes, slotWithKeyHash));
            }
        }

        return OKReply.INSTANCE;
    }

    @VisibleForTesting
    Reply setbit() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var offsetBytes = data[2];
        int offset;
        try {
            offset = Integer.parseInt(new String(offsetBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (offset < 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        // max offset limit, redis is 512MB
        // velo is 1MB
        final int MAX_OFFSET = 1024 * 1024;
        if (offset >= MAX_OFFSET) {
            return ErrorReply.INVALID_INTEGER;
        }

        var bit1or0Bytes = data[3];
        if (bit1or0Bytes.length != 1) {
            return ErrorReply.INVALID_INTEGER;
        }
        var isBit1 = bit1or0Bytes[0] == '1';
        if (!isBit1 && bit1or0Bytes[0] != '0') {
            return ErrorReply.INVALID_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytesExist = get(keyBytes, slotWithKeyHash);
        byte[] valueBytesNew;
        if (valueBytesExist == null) {
            int len = (offset + 1) / 8;
            if ((offset + 1) % 8 != 0) {
                len++;
            }
            valueBytesNew = new byte[len];
        } else {
            int len = valueBytesExist.length;
            if (len * 8 <= offset) {
                byte[] valueBytesNewTmp = new byte[offset / 8 + 1];
                System.arraycopy(valueBytesExist, 0, valueBytesNewTmp, 0, valueBytesExist.length);
                valueBytesNew = valueBytesNewTmp;
            } else {
                valueBytesNew = valueBytesExist;
            }
        }

        var bitSet = BitSet.valueOf(valueBytesNew);
        var isNotChange = bitSet.get(offset) == isBit1;
        if (isNotChange) {
            return isBit1 ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        } else {
            var oldIsBit1 = bitSet.get(offset);
            bitSet.set(offset, isBit1);
            set(keyBytes, bitSet.toByteArray(), slotWithKeyHash);
            return oldIsBit1 ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
    }

    @VisibleForTesting
    Reply setrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var offsetBytes = data[2];
        var valueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        int offset;
        try {
            offset = Integer.parseInt(new String(offsetBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (offset < 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        int lengthResult;
        var valueBytesExist = get(keyBytes, slotWithKeyHash);
        int len = offset + valueBytes.length;
        if (valueBytesExist == null) {
            lengthResult = len;

            // padding 0
            var setBytes = new byte[len];
            System.arraycopy(valueBytes, 0, setBytes, offset, valueBytes.length);

            set(keyBytes, setBytes, slotWithKeyHash);
        } else {
            int maxLength = Math.max(valueBytesExist.length, len);
            lengthResult = maxLength;

            var setBytes = new byte[maxLength];
            System.arraycopy(valueBytes, 0, setBytes, offset, valueBytes.length);
            if (maxLength > len) {
                System.arraycopy(valueBytesExist, len, setBytes, len, maxLength - len);
            }
            if (offset > 0) {
                int minLength = Math.min(valueBytesExist.length, offset);
                System.arraycopy(valueBytesExist, 0, setBytes, 0, minLength);
            }

            set(keyBytes, setBytes, slotWithKeyHash);
        }
        return new IntegerReply(lengthResult);
    }

    @VisibleForTesting
    Reply strlen() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        return new IntegerReply(cv.getUncompressedLength());
    }

    @VisibleForTesting
    Reply select() {
        return OKReply.INSTANCE;
    }

    private RedisHashKeys getRedisSet(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_SET);
        if (encodedBytes == null) {
            return null;
        }

        return RedisHashKeys.decode(encodedBytes);
    }

    static void saveRedisSet(RedisHashKeys rhk, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash, BaseCommand baseCommand, DictMap dictMap) {
        var key = new String(keyBytes);
        if (rhk.size() == 0) {
            baseCommand.removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        baseCommand.set(keyBytes, rhk.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_SET);
    }

    private void saveRedisSet(RedisHashKeys rhk, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        saveRedisSet(rhk, keyBytes, slotWithKeyHash, this, dictMap);
    }

    @VisibleForTesting
    Reply sadd() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        // use RedisHashKeys to store set
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        int added = 0;
        for (var memberBytes : memberBytesArr) {
            boolean isNewAdded = rhk.add(new String(memberBytes));
            if (rhk.size() > RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.SET_SIZE_TO_LONG;
            }
            if (isNewAdded) {
                added++;
            }
        }

        saveRedisSet(rhk, keyBytes, slotWithKeyHash);
        return new IntegerReply(added);
    }

    @VisibleForTesting
    Reply scard() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_SET);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        var size = RedisHashKeys.getSizeWithoutDecode(encodedBytes);
        return new IntegerReply(size);
    }

    @VisibleForTesting
    private void operateSet(TreeSet<String> set, ArrayList<RedisHashKeys> otherRhkList, boolean isInter, boolean isUnion) {
        for (var otherRhk : otherRhkList) {
            if (otherRhk != null) {
                var otherSet = otherRhk.getSet();
                if (isInter) {
                    if (otherSet.isEmpty()) {
                        set.clear();
                        break;
                    }
                    set.retainAll(otherSet);
                } else if (isUnion) {
                    set.addAll(otherSet);
                } else {
                    // diff
                    set.removeAll(otherSet);
                }
                if (set.isEmpty()) {
                    break;
                }
            } else {
                if (isInter) {
                    set.clear();
                    break;
                }
            }
        }
    }

    @VisibleForTesting
    Reply sdiff(boolean isInter, boolean isUnion) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        ArrayList<SlotWithKeyHash> list = new ArrayList<>(data.length - 1);
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(slotWithKeyHash);
        }

        var first = list.getFirst();
        var rhk = getRedisSet(first.rawKey().getBytes(), first);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
            if (isInter) {
                return MultiBulkReply.EMPTY;
            }
            if (!isUnion) {
                return MultiBulkReply.EMPTY;
            }
        }

        var set = rhk.getSet();
        if (!isCrossRequestWorker) {
            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.rawKey().getBytes(), other);
                otherRhkList.add(otherRhk);
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            if (set.isEmpty()) {
                return MultiBulkReply.EMPTY;
            }

            var replies = new Reply[set.size()];
            int i = 0;
            for (var value : set) {
                replies[i++] = new BulkReply(value.getBytes());
            }
            return new MultiBulkReply(replies);
        }

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size() - 1);
        for (int i = 1; i < list.size(); i++) {
            var other = list.get(i);
            var oneSlot = localPersist.oneSlot(other.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(other.rawKey().getBytes(), other));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sdiff error={}, isInter={}, isUnion={}", e.getMessage(), isInter, isUnion);
                finalPromise.setException(e);
                return;
            }

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (var promise : promises) {
                otherRhkList.add(promise.getResult());
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            if (set.isEmpty()) {
                finalPromise.set(MultiBulkReply.EMPTY);
                return;
            }

            var replies = new Reply[set.size()];
            int i = 0;
            for (var value : set) {
                replies[i++] = new BulkReply(value.getBytes());
            }
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sdiffstore(boolean isInter, boolean isUnion) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var dstKeyBytes = data[1];
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        ArrayList<SlotWithKeyHash> list = new ArrayList<>(data.length - 2);
        // begin from 2
        // j = 1 -> dst key bytes is 0
        for (int i = 2, j = 1; i < data.length; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(slotWithKeyHash);
        }

        if (!isCrossRequestWorker) {
            // first key may be in other thread eventloop
            var first = list.getFirst();
            var rhk = getRedisSet(first.rawKey().getBytes(), first);
            if (rhk == null) {
                removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                return IntegerReply.REPLY_0;
            }
            if (rhk.size() == 0) {
                if (isInter) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    return IntegerReply.REPLY_0;
                }
                if (!isUnion) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    return IntegerReply.REPLY_0;
                }
            }

            var set = rhk.getSet();

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.rawKey().getBytes(), other);
                otherRhkList.add(otherRhk);
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            saveRedisSet(rhk, dstKeyBytes, dstSlotWithKeyHash);
            return set.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(set.size());
        }

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            var other = list.get(i);
            var oneSlot = localPersist.oneSlot(other.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(other.rawKey().getBytes(), other));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sdiffstore error={}, isInter={}, isUnion={}", e.getMessage(), isInter, isUnion);
                finalPromise.setException(e);
                return;
            }

            var rhk = promises.getFirst().getResult();
            if (rhk == null) {
                removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                finalPromise.set(IntegerReply.REPLY_0);
                return;
            }
            if (rhk.size() == 0) {
                if (isInter) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }
                if (!isUnion) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }
            }

            var set = rhk.getSet();

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (var promise : promises) {
                otherRhkList.add(promise.getResult());
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            saveRedisSet(rhk, dstKeyBytes, dstSlotWithKeyHash);
            finalPromise.set(set.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(set.size()));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sintercard() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var numkeysBytes = data[1];
        int numkeys;
        try {
            numkeys = Integer.parseInt(new String(numkeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numkeys < 2) {
            return ErrorReply.INVALID_INTEGER;
        }

        ArrayList<SlotWithKeyHash> list = new ArrayList<>(numkeys);
        // begin from 2
        for (int i = 2, j = 0; i < numkeys + 2; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(slotWithKeyHash);
        }

        int limit = 0;
        // limit
        if (data.length > numkeys + 2) {
            if (data.length != numkeys + 4) {
                return ErrorReply.SYNTAX;
            }

            var limitFlagBytes = data[numkeys + 2];
            if (!"limit".equals(new String(limitFlagBytes))) {
                return ErrorReply.SYNTAX;
            }

            var limitBytes = data[numkeys + 3];
            try {
                limit = Integer.parseInt(new String(limitBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var first = list.getFirst();
        var rhk = getRedisSet(first.rawKey().getBytes(), first);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }
        if (rhk.size() == 0) {
            return IntegerReply.REPLY_0;
        }

        var set = rhk.getSet();
        if (!isCrossRequestWorker) {
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.rawKey().getBytes(), other);

                if (otherRhk != null) {
                    set.retainAll(otherRhk.getSet());
                    if (set.isEmpty()) {
                        break;
                    }
                    if (limit != 0 && set.size() >= limit) {
                        break;
                    }
                } else {
                    set.clear();
                    break;
                }
            }

            int min = limit != 0 ? Math.min(set.size(), limit) : set.size();
            return min == 0 ? IntegerReply.REPLY_0 : new IntegerReply(min);
        }

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size() - 1);
        for (int i = 1; i < list.size(); i++) {
            var other = list.get(i);
            var oneSlot = localPersist.oneSlot(other.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(other.rawKey().getBytes(), other));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        int finalLimit = limit;
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sintercard error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            for (var promise : promises) {
                var otherRhk = promise.getResult();
                if (otherRhk != null) {
                    set.retainAll(otherRhk.getSet());
                    if (set.isEmpty()) {
                        break;
                    }
                    if (finalLimit != 0 && set.size() >= finalLimit) {
                        break;
                    }
                } else {
                    set.clear();
                    break;
                }
            }

            int min = finalLimit != 0 ? Math.min(set.size(), finalLimit) : set.size();
            finalPromise.set(min == 0 ? IntegerReply.REPLY_0 : new IntegerReply(min));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sismember() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
            return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        var isMember = rhk.contains(new String(memberBytes));
        return isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
    Reply smembers() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
            return MultiBulkReply.EMPTY;
        }

        var set = rhk.getSet();

        var replies = new Reply[set.size()];
        int i = 0;
        for (var value : set) {
            replies[i++] = new BulkReply(value.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply smismember() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[memberBytesArr.length];
        for (int i = 0; i < memberBytesArr.length; i++) {
            var isMember = rhk.contains(new String(memberBytesArr[i]));
            replies[i] = isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply sort(boolean canDoStore) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        String byPattern = null;
        int offset = 0;
        int count = -1;
        ArrayList<String> getPatternList = new ArrayList<>();
        boolean isAsc = true;
        boolean isAlpha = false;
        byte[] dstKeyBytes = null;

        for (int i = 2; i < data.length; i++) {
            var arg = new String(data[i]).toUpperCase();
            switch (arg) {
                case "BY":
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    byPattern = new String(data[i + 1]);
                    i += 1;
                    continue;
                case "LIMIT":
                    if (i + 2 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        offset = Integer.parseInt(new String(data[i + 1]));
                        count = Integer.parseInt(new String(data[i + 2]));
                    } catch (NumberFormatException ignore) {
                        return ErrorReply.NOT_INTEGER;
                    }
                    i += 2;
                    continue;
                case "GET":
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    getPatternList.add(new String(data[i + 1]));
                    i += 1;
                    continue;
                case "DESC":
                    isAsc = false;
                    continue;
                case "ALPHA":
                    isAlpha = true;
                    continue;
                case "STORE":
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    dstKeyBytes = data[i + 1];
                    i += 1;
                    continue;
                default:
                    return ErrorReply.SYNTAX;
            }
        }

        if (offset < 0) {
            return new ErrorReply("offset must >= 0");
        }

        if (!canDoStore && dstKeyBytes != null) {
            return new ErrorReply("sort_ro not support store");
        }
        var isStore = dstKeyBytes != null;

        if (byPattern != null) {
            return new ErrorReply("sort by pattern not support yet");
        }

        var s = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, s);
        if (cv == null) {
            return isStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        if (!cv.isList() && !cv.isSet() && !cv.isZSet()) {
            return ErrorReply.WRONG_TYPE;
        }

        var encodedBytes = getValueBytesByCv(cv, keyBytes, s);
        if (cv.isList()) {
            var rl = RedisList.decode(encodedBytes);
            var list = rl.getList();
            if (count <= 0) {
                count = list.size();
            }

            if (isAlpha) {
                boolean finalIsAsc = isAsc;
                list.sort((o1, o2) -> {
                    var r = new String(o1).compareTo(new String(o2));
                    return finalIsAsc ? r : -r;
                });
            } else {
                try {
                    boolean finalIsAsc1 = isAsc;
                    list.sort((o1, o2) -> {
                        long l = Long.parseLong(new String(o1)) - Long.parseLong(new String(o2));
                        var r = l > 0 ? 1 : l < 0 ? -1 : 0;
                        return finalIsAsc1 ? r : -r;
                    });
                } catch (NumberFormatException ignore) {
                    return ErrorReply.NOT_INTEGER;
                }
            }

            // offset and count
            List<byte[]> subList = offset >= list.size() ? Collections.emptyList() :
                    list.subList(offset, Math.min(offset + count, list.size()));

            if (isStore) {
                var dstSlotWithKeyHash = slot(dstKeyBytes);
                var dstRl = new RedisList();
                for (var e : subList) {
                    dstRl.addLast(e);
                }

                var targetOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                byte[] finalDstKeyBytes = dstKeyBytes;
                var subListSize = subList.size();

                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var p = targetOneSlot.asyncRun(() -> {
                    LGroup.saveRedisList(dstRl, finalDstKeyBytes, dstSlotWithKeyHash, this, dictMap);
                });

                p.whenComplete((v, t) -> {
                    if (t != null) {
                        finalPromise.setException(t);
                    } else {
                        finalPromise.set(new IntegerReply(subListSize));
                    }
                });

                return asyncReply;
            } else {
                return new MultiBulkReply(subList.stream().map(BulkReply::new).toArray(Reply[]::new));
            }
        } else if (cv.isSet()) {
            var rhk = RedisHashKeys.decode(encodedBytes);
            var set = rhk.getSet();
            if (count <= 0) {
                count = set.size();
            }

            ArrayList<String> subList = new ArrayList<>();
            if (!isAlpha) {
                TreeSet<Long> sortedByLong;
                try {
                    sortedByLong = set.stream().map(Long::parseLong).collect(Collectors.toCollection(TreeSet::new));
                } catch (NumberFormatException ignore) {
                    return ErrorReply.NOT_INTEGER;
                }

                // offset and count
                var it = isAsc ? sortedByLong.iterator() : sortedByLong.descendingIterator();
                int c = 0;
                while (it.hasNext()) {
                    if (c >= count) {
                        break;
                    }
                    var e = it.next();
                    if (offset <= 0) {
                        subList.add(e.toString());
                        offset--;
                        c++;
                    } else {
                        offset--;
                    }
                }
            } else {
                // already sorted
                var it = isAsc ? set.iterator() : set.descendingIterator();
                int c = 0;
                while (it.hasNext()) {
                    if (c >= count) {
                        break;
                    }
                    var e = it.next();
                    if (offset <= 0) {
                        subList.add(e);
                        offset--;
                        c++;
                    } else {
                        offset--;
                    }
                }
            }

            if (isStore) {
                var dstSlotWithKeyHash = slot(dstKeyBytes);
                var dstRhk = new RedisHashKeys();
                for (var e : subList) {
                    dstRhk.add(e);
                }

                var targetOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                byte[] finalDstKeyBytes = dstKeyBytes;
                var subListSize = subList.size();

                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var p = targetOneSlot.asyncRun(() -> {
                    saveRedisSet(dstRhk, finalDstKeyBytes, dstSlotWithKeyHash, this, dictMap);
                });

                p.whenComplete((v, t) -> {
                    if (t != null) {
                        finalPromise.setException(t);
                    } else {
                        finalPromise.set(new IntegerReply(subListSize));
                    }
                });

                return asyncReply;
            } else {
                return subList.isEmpty() ? MultiBulkReply.EMPTY : new MultiBulkReply(subList.stream().map(x -> new BulkReply(x.getBytes())).toArray(Reply[]::new));
            }
        } else {
            var rz = RedisZSet.decode(encodedBytes);
            var set = rz.getSet();
            if (count <= 0) {
                count = set.size();
            }

            ArrayList<String> subList = new ArrayList<>();
            if (!isAlpha) {
                // already sorted by score
                // offset and count
                var it = isAsc ? set.iterator() : set.descendingIterator();
                int c = 0;
                while (it.hasNext()) {
                    if (c >= count) {
                        break;
                    }
                    var e = it.next();
                    if (offset <= 0) {
                        subList.add(e.member());
                        offset--;
                        c++;
                    } else {
                        offset--;
                    }
                }
            } else {
                var sortedList = set.stream().map(RedisZSet.ScoreValue::member).sorted().collect(Collectors.toCollection(TreeSet::new));
                var it = isAsc ? sortedList.iterator() : sortedList.descendingIterator();
                int c = 0;
                while (it.hasNext()) {
                    if (c >= count) {
                        break;
                    }
                    var e = it.next();
                    if (offset <= 0) {
                        subList.add(e);
                        offset--;
                        c++;
                    } else {
                        offset--;
                    }
                }
            }

            if (isStore) {
                var dstSlotWithKeyHash = slot(dstKeyBytes);
                var dstRz = new RedisZSet();
                for (var e : subList) {
                    var sv = rz.get(e);
                    dstRz.add(sv.score(), sv.member());
                }

                var targetOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                byte[] finalDstKeyBytes = dstKeyBytes;
                var subListSize = subList.size();

                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var p = targetOneSlot.asyncRun(() -> {
                    ZGroup.saveRedisZSet(dstRz, finalDstKeyBytes, dstSlotWithKeyHash, this, dictMap);
                });

                p.whenComplete((v, t) -> {
                    if (t != null) {
                        finalPromise.setException(t);
                    } else {
                        finalPromise.set(new IntegerReply(subListSize));
                    }
                });

                return asyncReply;
            } else {
                return subList.isEmpty() ? MultiBulkReply.EMPTY : new MultiBulkReply(subList.stream().map(x -> new BulkReply(x.getBytes())).toArray(Reply[]::new));
            }
        }
    }

    @VisibleForTesting
    Reply smove() {
        if (data.length != 4) {
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

        var memberBytes = data[3];
        if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
            return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
        }

        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();

        var srcRhk = getRedisSet(srcKeyBytes, srcSlotWithKeyHash);
        if (srcRhk == null) {
            return IntegerReply.REPLY_0;
        }

        var member = new String(memberBytes);
        var isMember = srcRhk.remove(member);
        if (!isMember) {
            return IntegerReply.REPLY_0;
        }

        saveRedisSet(srcRhk, srcKeyBytes, srcSlotWithKeyHash);

        if (!isCrossRequestWorker) {
            var dstRhk = getRedisSet(dstKeyBytes, dstSlotWithKeyHash);
            if (dstRhk == null) {
                dstRhk = new RedisHashKeys();
            }
            dstRhk.add(member);

            saveRedisSet(dstRhk, dstKeyBytes, dstSlotWithKeyHash);
            return IntegerReply.REPLY_1;
        }

        var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncRun(() -> {
            var dstRhk = getRedisSet(dstKeyBytes, dstSlotWithKeyHash);
            if (dstRhk == null) {
                dstRhk = new RedisHashKeys();
            }
            dstRhk.add(member);

            saveRedisSet(dstRhk, dstKeyBytes, dstSlotWithKeyHash);
            finalPromise.set(IntegerReply.REPLY_1);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply srandmember(boolean doPop) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean hasCount = data.length == 3;
        int count = 1;
        if (hasCount) {
            var countBytes = data[2];
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }
        if (rhk.size() == 0) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var set = rhk.getSet();
        int size = set.size();
        if (count > size) {
            count = size;
        }

        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = HGroup.getRandIndex(count, size, absCount);

        var members = new String[indexes.size()];

        int j = 0;
        var it = set.iterator();
        while (it.hasNext()) {
            var member = it.next();

            // only remove once
            boolean isAlreadyRemoved = false;

            for (int k = 0; k < indexes.size(); k++) {
                Integer index = indexes.get(k);
                if (index != null && index == j) {
                    members[k] = member;
                    if (!isAlreadyRemoved && doPop) {
                        it.remove();
                        isAlreadyRemoved = true;
                    }
                    indexes.set(k, null);
                }
            }
            j++;
        }

        if (doPop) {
            saveRedisSet(rhk, keyBytes, slotWithKeyHash);
        }

        if (hasCount) {
            var replies = new Reply[members.length];
            for (int i = 0; i < members.length; i++) {
                replies[i] = new BulkReply(members[i].getBytes());
            }
            return new MultiBulkReply(replies);
        } else {
            return new BulkReply(members[0].getBytes());
        }
    }

    @VisibleForTesting
    Reply srem() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }
        if (rhk.size() == 0) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        for (var memberBytes : memberBytesArr) {
            var isMember = rhk.remove(new String(memberBytes));
            if (isMember) {
                removed++;
            }
        }

        saveRedisSet(rhk, keyBytes, slotWithKeyHash);
        return new IntegerReply(removed);
    }

    @VisibleForTesting
    Reply subscribe(boolean isPattern) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var channels = new ArrayList<String>(data.length - 1);
        for (int i = 1; i < data.length; i++) {
            var channel = new String(data[i]);
            channels.add(channel);
        }

        // check acl
        var u = getAuthU();
        if (!u.isOn()) {
            return ErrorReply.ACL_PERMIT_LIMIT;
        }

        for (var channel : channels) {
            if (!u.checkChannels(channel)) {
                return ErrorReply.ACL_PERMIT_LIMIT;
            }
        }

        var socketInInspector = localPersist.getSocketInspector();

        var replies = new Reply[channels.size() * 3];
        int j = 0;
        for (var channel : channels) {
            replies[j++] = new BulkReply("subscribe".getBytes());
            replies[j++] = new BulkReply(channel.getBytes());
            var size = socketInInspector.subscribe(channel, isPattern, socket);
            replies[j++] = new IntegerReply(size);
        }

        return new MultiBulkReply(replies);
    }
}
