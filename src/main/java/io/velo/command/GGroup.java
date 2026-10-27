package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.TypeMismatchException;
import io.velo.reply.*;
import io.velo.type.RedisBitSet;
import io.velo.type.RedisGeo;
import io.velo.type.RedisZSet;
import org.jetbrains.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static io.velo.CompressedValue.NO_EXPIRE;

/**
 * Handles Redis commands starting with letter 'G'.
 * This includes commands like GET, GETBIT, GETDEL, GETEX, GETRANGE, GETSET, GEO.*, etc.
 */
public class GGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "geoadd", -5,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@geo", "@write", "@slow"),
                "geo",
                "Add one or more geospatial items in the geospatial index represented using a sorted set.",
                "3.2.0", "O(log(N)) for each item added"));
        CommandRegistry.register(new CommandEntry(
                "geodist", -4,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@geo", "@read", "@slow"),
                "geo",
                "Returns the distance between two members of a geospatial index.",
                "3.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "geohash", -2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@geo", "@read", "@slow"),
                "geo",
                "Returns members from a geospatial index as geohash strings.",
                "3.2.0", "O(1) for each member requested"));
        CommandRegistry.register(new CommandEntry(
                "geopos", -2,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@geo", "@read", "@slow"),
                "geo",
                "Returns the longitude and latitude of members of a geospatial index.",
                "3.2.0", "O(1) for each member requested"));
        CommandRegistry.register(new CommandEntry(
                "georadius", -6,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@geo", "@write", "@slow"),
                "geo",
                "Query a sorted set representing a geospatial index to fetch members matching a maximum distance from a point.",
                "3.2.0", "O(N+log(M))"));
        CommandRegistry.register(new CommandEntry(
                "georadiusbymember", -5,
                Set.of("write", "denyoom"),
                1, 1, 1,
                Set.of("@geo", "@write", "@slow"),
                "geo",
                "Query a sorted set representing a geospatial index to fetch members matching a maximum distance from a member.",
                "3.2.0", "O(N+log(M))"));
        CommandRegistry.register(new CommandEntry(
                "geosearch", -7,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@geo", "@read", "@slow"),
                "geo",
                "Query a sorted set representing a geospatial index to fetch members inside an area of a box or a circle.",
                "6.2.0", "O(N+log(M))"));
        CommandRegistry.register(new CommandEntry(
                "geosearchstore", -8,
                Set.of("write", "denyoom"),
                1, 2, 1,
                Set.of("@geo", "@write", "@slow"),
                "geo",
                "Query a sorted set representing a geospatial index to fetch members matching an area, store the result in another key.",
                "6.2.0", "O(N+log(M))"));
        CommandRegistry.register(new CommandEntry(
                "get", 2,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@string", "@fast"),
                "string",
                "Get the value of a key.",
                "1.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "getbit", 3,
                Set.of("readonly", "fast"),
                1, 1, 1,
                Set.of("@read", "@bitmap", "@fast"),
                "bitmap",
                "Returns the bit value at offset in the string value stored at key.",
                "2.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "getdel", 2,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@string", "@fast"),
                "string",
                "Get the value of a key and delete the key.",
                "6.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "getex", -2,
                Set.of("write", "fast"),
                1, 1, 1,
                Set.of("@write", "@string", "@fast"),
                "string",
                "Get the value of a key and optionally set its expiration time.",
                "6.2.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "getrange", 4,
                Set.of("readonly"),
                1, 1, 1,
                Set.of("@read", "@string", "@slow"),
                "string",
                "Get a substring of the string stored at a key.",
                "2.4.0", "O(N)"));
        CommandRegistry.register(new CommandEntry(
                "getset", 3,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@string", "@fast"),
                "string",
                "Set the string value of a key and return its old value.",
                "1.0.0", "O(1)"));
    }

    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public GGroup(String cmd, byte[][] data, ITcpSocket socket) {
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

        // geo category
        if (cmd.startsWith("geo")) {
            if ("geosearchstore".equals(cmd)) {
                if (data.length < 3) {
                    return slotWithKeyHashList;
                }

                var dstS = BaseCommand.slot(data[1], slotNumber);
                var srcS = BaseCommand.slot(data[2], slotNumber);
                // source key first
                slotWithKeyHashList.add(srcS);
                slotWithKeyHashList.add(dstS);
                return slotWithKeyHashList;
            }

            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("get".equals(cmd) || "getbit".equals(cmd) || "getdel".equals(cmd) || "getex".equals(cmd)
                || "getrange".equals(cmd) || "getset".equals(cmd)) {
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
        if (cmd.startsWith("geo")) {
            return geo();
        }

        if ("getbit".equals(cmd)) {
            return getbit();
        }

        if ("getdel".equals(cmd)) {
            return getdel();
        }

        if ("getex".equals(cmd)) {
            return getex();
        }

        if ("getrange".equals(cmd)) {
            return getrange();
        }

        if ("getset".equals(cmd)) {
            return getset();
        }

        return NilReply.INSTANCE;
    }

    private Reply getbit() {
        if (data.length != 3) {
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytes = get(slotWithKeyHash, true);
        if (valueBytes == null) {
            return IntegerReply.REPLY_0;
        }

        var bs = new RedisBitSet(valueBytes);
        return bs.get(offset) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    private Reply getdel() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytes = get(slotWithKeyHash, true);
        if (valueBytes != null) {
            removeDelay(slotWithKeyHash);
            return new BulkReply(valueBytes);
        } else {
            return NilReply.INSTANCE;
        }
    }

    private static ErrorReply invalidExpireTime() {
        return new ErrorReply("invalid expire time in 'getex' command");
    }

    private Reply getex() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        long ex = -1;
        long px = -1;
        long exAt = -1;
        long pxAt = -1;
        boolean isPersist = false;

        if (data.length == 2) {
            // do nothing
        } else if (data.length == 3) {
            isPersist = "persist".equalsIgnoreCase(new String(data[2]));
            if (!isPersist) {
                return ErrorReply.SYNTAX;
            }
        } else if (data.length == 4) {
            var arg = new String(data[2]);
            var arg2 = new String(data[3]);
            long x;
            try {
                x = Long.parseLong(arg2);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (x < 0) {
                return ErrorReply.INVALID_INTEGER;
            }

            if ("ex".equalsIgnoreCase(arg)) {
                ex = x;
            } else if ("px".equalsIgnoreCase(arg)) {
                px = x;
            } else if ("exat".equalsIgnoreCase(arg)) {
                exAt = x;
            } else if ("pxat".equalsIgnoreCase(arg)) {
                pxAt = x;
            } else {
                return ErrorReply.SYNTAX;
            }
        } else {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var key = slotWithKeyHash.rawKey();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            return NilReply.INSTANCE;
        }

        if (!cv.isTypeString()) {
            throw new TypeMismatchException("Expect type string, but got sp type=" + cv.getDictSeqOrSpType());
        }

        var valueBytes = getValueBytesByCv(cv, slotWithKeyHash);

        long expireAt = cv.getExpireAt();
        long expireAtOld = expireAt;
        final long now = System.currentTimeMillis();
        if (isPersist) {
            expireAt = NO_EXPIRE;
        } else if (ex > -1) {
            // seconds to milliseconds, check the overflow before the multiply and before the add
            if (ex > Long.MAX_VALUE / 1000 || ex > (Long.MAX_VALUE - now) / 1000) {
                return invalidExpireTime();
            }
            expireAt = now + ex * 1000;
        } else if (px > -1) {
            // check the overflow before the add
            if (px > Long.MAX_VALUE - now) {
                return invalidExpireTime();
            }
            expireAt = now + px;
        } else if (exAt > -1) {
            // seconds to milliseconds, check the overflow before the multiply
            if (exAt > Long.MAX_VALUE / 1000) {
                return invalidExpireTime();
            }
            expireAt = exAt * 1000;
        } else if (pxAt > -1) {
            expireAt = pxAt;
        }

        if (expireAt != expireAtOld) {
            cv.setSeq(snowFlake.nextId());
            cv.setKeyHash(slotWithKeyHash.keyHash());
            cv.setExpireAt(expireAt);

            // write the value back as it is, only the expire time changes,
            // do not use setCv() here, it re-encodes the value and drops the type marker
            putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);
        }
        return new BulkReply(valueBytes);
    }

    private final static Reply BLANK_REPLY = new BulkReply(new byte[0]);

    Reply getrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        long startLong;
        long endLong;
        try {
            startLong = Long.parseLong(new String(data[2]));
            endLong = Long.parseLong(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytes = get(slotWithKeyHash, true);
        if (valueBytes == null) {
            return BLANK_REPLY;
        }

        // redis parses 64 bit offsets, clamp them to the value length range before index reset
        int start = (int) Math.max(-valueBytes.length, Math.min(valueBytes.length, startLong));
        int end = (int) Math.max(-valueBytes.length, Math.min(valueBytes.length, endLong));

        var startEnd = IndexStartEndReset.reset(start, end, valueBytes.length);
        if (!startEnd.valid()) {
            return BLANK_REPLY;
        }

        // use utf-8 ? or use bytes
//        var value = new String(valueBytes);

        var subBytes = new byte[startEnd.end() - startEnd.start() + 1];
        System.arraycopy(valueBytes, startEnd.start(), subBytes, 0, subBytes.length);
        return new BulkReply(subBytes);
    }

    private Reply getset() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var valueBytes = data[2];
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        // wrong type key replies WRONGTYPE, other string-compatible types will be overwritten
        var valueBytesExist = get(slotWithKeyHash, true);

        set(valueBytes, slotWithKeyHash);

        if (valueBytesExist == null) {
            return NilReply.INSTANCE;
        } else {
            return new BulkReply(valueBytesExist);
        }
    }

    private Reply geo() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if ("geoadd".equals(cmd)) {
            return geoadd();
        }

        if ("geodist".equals(cmd)) {
            return geodist();
        }

        if ("geohash".equals(cmd)) {
            return geohash(false);
        }

        if ("geopos".equals(cmd)) {
            return geohash(true);
        }

        if ("georadius".equals(cmd)) {
            // deprecated, use geosearch instead
            return ErrorReply.NOT_SUPPORT;
        }

        if ("georadius_ro".equals(cmd)) {
            // deprecated, use geosearch instead
            return ErrorReply.NOT_SUPPORT;
        }

        if ("georadiusbymember".equals(cmd)) {
            // deprecated, use geosearch instead
            return ErrorReply.NOT_SUPPORT;
        }

        if ("georadiusbymember_ro".equals(cmd)) {
            // deprecated, use geosearch instead
            return ErrorReply.NOT_SUPPORT;
        }

        if ("geosearch".equals(cmd)) {
            return geosearch(data, null, null);
        }

        if ("geosearchstore".equals(cmd)) {
            if (data.length < 3) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length - 1][];
            System.arraycopy(data, 2, dd, 1, data.length - 2);

            var dstKeyBytes = data[1];
            var dstS = slotWithKeyHashListParsed.getLast();

            return geosearch(dd, dstKeyBytes, dstS);
        }

        return ErrorReply.SYNTAX;
    }

    private record GeoItem(double lon, double lat, String member) {
    }

    /**
     * Formats a geo distance reply with fixed 4 decimals, mirroring redis
     * geo.c addReplyDoubleDistance() = fixedpoint_d2string(distance, 4).
     */
    private static BulkReply distanceReply(double distance) {
        return new BulkReply(BigDecimal.valueOf(distance).setScale(4, RoundingMode.HALF_UP).toPlainString());
    }

    /**
     * Formats a geo coordinate as a plain decimal string, java Double.toString
     * switches to scientific notation for values like 1.0E-4 which redis never sends.
     */
    private static String coordinate(double d) {
        return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
    }

    private RedisGeo getRedisGeo(SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(slotWithKeyHash, false, CompressedValue.SP_TYPE_GEO);
        if (encodedBytes == null) {
            return null;
        }

        return RedisGeo.decode(encodedBytes);
    }

    @VisibleForTesting
    void saveRedisGeo(RedisGeo rg, SlotWithKeyHash slotWithKeyHash) {
        if (rg.isEmpty()) {
            removeDelay(slotWithKeyHash);
            return;
        }

        set(rg.encode(), slotWithKeyHash, CompressedValue.SP_TYPE_GEO);
    }

    private Reply geoadd() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        boolean isNx = false;
        boolean isXx = false;
        boolean isIncludeCh = false;

        ArrayList<GeoItem> itemList = new ArrayList<>();
        for (int i = 2; i < data.length; i++) {
            var arg = new String(data[i]);
            if (arg.equalsIgnoreCase("nx")) {
                isNx = true;
            } else if (arg.equalsIgnoreCase("xx")) {
                isXx = true;
            } else if (arg.equalsIgnoreCase("ch")) {
                isIncludeCh = true;
            } else {
                if (i + 2 >= data.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    var lon = Double.parseDouble(new String(data[i]));
                    var lat = Double.parseDouble(new String(data[i + 1]));
                    // NaN passes every < > comparison, reject it explicitly;
                    // infinite values are already caught by the bounds check
                    if (Double.isNaN(lon) || Double.isNaN(lat) ||
                        lon > RedisGeo.GEO_LONG_MAX || lon < RedisGeo.GEO_LONG_MIN ||
                        lat > RedisGeo.GEO_LAT_MAX || lat < RedisGeo.GEO_LAT_MIN) {
                        return ErrorReply.NOT_FLOAT;
                    }
                    var member = new String(data[i + 2]);
                    itemList.add(new GeoItem(lon, lat, member));
                    i += 2;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }
        }

        if (itemList.isEmpty()) {
            return ErrorReply.SYNTAX;
        }

        if (isNx && isXx) {
            return ErrorReply.SYNTAX;
        }

        var rg = getRedisGeo(slotWithKeyHash);
        if (rg == null) {
            rg = new RedisGeo();
        }

        int added = 0;
        int changed = 0;
        for (var item : itemList) {
            var member = item.member;
            if (isNx) {
                // nx
                if (rg.contains(member)) {
                    continue;
                }
            } else if (isXx) {
                // xx
                if (!rg.contains(member)) {
                    continue;
                }
            }

            var oldP = rg.get(member);
            if (oldP != null) {
                if (oldP.lat() != item.lat || oldP.lon() != item.lon) {
                    rg.add(member, item.lon, item.lat);
                    changed++;
                }
            } else {
                rg.add(member, item.lon, item.lat);
                added++;
            }
        }

        var handled = added + changed;
        if (handled > 0) {
            saveRedisGeo(rg, slotWithKeyHash);
        }
        return new IntegerReply(isIncludeCh ? changed + added : added);
    }

    private Reply geodist() {
        if (data.length != 4 && data.length != 5) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        var m0 = new String(data[2]);
        var m1 = new String(data[3]);

        RedisGeo.Unit unit = RedisGeo.Unit.M;
        if (data.length == 5) {
            var unitString = new String(data[4]);
            unit = RedisGeo.Unit.fromString(unitString);
            if (unit == RedisGeo.Unit.UNKNOWN) {
                return ErrorReply.SYNTAX;
            }
        }

        var rg = getRedisGeo(slotWithKeyHash);
        if (rg == null) {
            return NilReply.INSTANCE;
        }

        var p0 = rg.get(m0);
        var p1 = rg.get(m1);
        if (p0 == null || p1 == null) {
            return NilReply.INSTANCE;
        }

        var distance = RedisGeo.distance(p0, p1);
        return distanceReply(unit.fromMeters(distance));
    }

    private Reply geohash(boolean isGeopos) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var replies = new Reply[data.length - 2];

        var rg = getRedisGeo(slotWithKeyHash);
        if (rg == null) {
            Arrays.fill(replies, NilReply.INSTANCE);
        } else {
            for (int i = 2; i < data.length; i++) {
                var member = new String(data[i]);
                var p = rg.get(member);
                if (p == null) {
                    replies[i - 2] = NilReply.INSTANCE;
                } else {
                    if (isGeopos) {
                        var subReplies = new Reply[]{
                                new BulkReply(coordinate(p.lon())),
                                new BulkReply(coordinate(p.lat()))
                        };
                        replies[i - 2] = new MultiBulkReply(subReplies);
                    } else {
                        replies[i - 2] = new BulkReply(RedisGeo.hash(p));
                    }
                }
            }
        }

        return new MultiBulkReply(replies);
    }

    private record GeoSearchResult(String member, RedisGeo.P p, double distance) {
    }

    /**
     * Deletes the geosearchstore destination key on an empty result / missing source and replies 0,
     * mirroring redis geo.c which dbDelete(s) the store key before replying 0.
     */
    private Reply deleteDstKeyAndReplyZero(SlotWithKeyHash dstS) {
        if (!isCrossRequestWorker) {
            removeDelay(dstS);
            return IntegerReply.REPLY_0;
        }

        var dstOneSlot = localPersist.oneSlot(dstS.slot());

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var p = dstOneSlot.asyncRun(() -> removeDelay(dstS));
        p.whenComplete((v, t) -> {
            if (t != null) {
                finalPromise.setException(t);
            } else {
                finalPromise.set(IntegerReply.REPLY_0);
            }
        });

        return asyncReply;
    }

    private Reply geosearch(byte[][] dd, byte[] dstKeyBytes, SlotWithKeyHash dstS) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        double fromLon = 0d;
        double fromLat = 0d;
        String fromMember = null;

        double byRadius = -1;
        var byRadiusUnit = RedisGeo.Unit.M;
        double byBoxWidth = -1;
        double byBoxHeight = -1;
        var byBoxUnit = RedisGeo.Unit.M;
        var outputUnit = RedisGeo.Unit.M;

        boolean isDesc = false;
        boolean hasOrder = false;
        int count = -1;
        boolean isWithDist = false;
        boolean isWithHash = false;
        boolean isWithCoord = false;
        boolean isStoreDist = false;
        boolean hasCenterSource = false;
        boolean hasFromLonLat = false;
        boolean hasByRadius = false;
        boolean hasByBox = false;

        var isStoreMode = dstKeyBytes != null;

        for (int i = 2; i < dd.length; i++) {
            var arg = new String(dd[i]);
            if ("asc".equalsIgnoreCase(arg)) {
                if (hasOrder) {
                    return ErrorReply.SYNTAX;
                }
                hasOrder = true;
            } else if ("desc".equalsIgnoreCase(arg)) {
                if (hasOrder) {
                    return ErrorReply.SYNTAX;
                }
                isDesc = true;
                hasOrder = true;
            } else if ("count".equalsIgnoreCase(arg)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    count = Integer.parseInt(new String(dd[i + 1]));
                    if (count <= 0) {
                        return ErrorReply.VALUE_NOT_POSITIVE;
                    }
                    i++;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                if (i + 1 < dd.length && "any".equalsIgnoreCase(new String(dd[i + 1]))) {
                    i++;
                }
            } else if ("storedist".equalsIgnoreCase(arg)) {
                if (!isStoreMode) {
                    return ErrorReply.SYNTAX;
                }
                isStoreDist = true;
            } else if ("withdist".equalsIgnoreCase(arg)) {
                if (isStoreMode) {
                    return ErrorReply.SYNTAX;
                }
                isWithDist = true;
            } else if ("withhash".equalsIgnoreCase(arg)) {
                if (isStoreMode) {
                    return ErrorReply.SYNTAX;
                }
                isWithHash = true;
            } else if ("withcoord".equalsIgnoreCase(arg)) {
                if (isStoreMode) {
                    return ErrorReply.SYNTAX;
                }
                isWithCoord = true;
            } else if ("fromlonlat".equalsIgnoreCase(arg)) {
                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                if (hasFromLonLat || fromMember != null) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    fromLon = Double.parseDouble(new String(dd[i + 1]));
                    fromLat = Double.parseDouble(new String(dd[i + 2]));
                    if (Double.isNaN(fromLon) || Double.isInfinite(fromLon) ||
                        Double.isNaN(fromLat) || Double.isInfinite(fromLat) ||
                        fromLon > RedisGeo.GEO_LONG_MAX || fromLon < RedisGeo.GEO_LONG_MIN ||
                        fromLat > RedisGeo.GEO_LAT_MAX || fromLat < RedisGeo.GEO_LAT_MIN) {
                        return ErrorReply.NOT_FLOAT;
                    }
                    i += 2;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
                hasFromLonLat = true;
                hasCenterSource = true;
            } else if ("frommember".equalsIgnoreCase(arg)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                if (hasFromLonLat || fromMember != null) {
                    return ErrorReply.SYNTAX;
                }
                fromMember = new String(dd[i + 1]);
                i++;
                hasCenterSource = true;
            } else if ("byradius".equalsIgnoreCase(arg)) {
                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                if (hasByRadius || hasByBox) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    byRadius = Double.parseDouble(new String(dd[i + 1]));
                    if (byRadius <= 0 || Double.isInfinite(byRadius) || Double.isNaN(byRadius)) {
                        return ErrorReply.NOT_FLOAT;
                    }
                    i++;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }

                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                var unitString = new String(dd[i + 1]);
                byRadiusUnit = RedisGeo.Unit.fromString(unitString);
                if (byRadiusUnit == RedisGeo.Unit.UNKNOWN) {
                    return ErrorReply.SYNTAX;
                }
                byRadius = byRadiusUnit.toMeters(byRadius);
                i++;
                hasByRadius = true;
            } else if ("bybox".equalsIgnoreCase(arg)) {
                if (i + 3 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                if (hasByRadius || hasByBox) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    byBoxWidth = Double.parseDouble(new String(dd[i + 1]));
                    byBoxHeight = Double.parseDouble(new String(dd[i + 2]));
                    if (byBoxWidth <= 0 || Double.isInfinite(byBoxWidth) || Double.isNaN(byBoxWidth) ||
                        byBoxHeight <= 0 || Double.isInfinite(byBoxHeight) || Double.isNaN(byBoxHeight)) {
                        return ErrorReply.NOT_FLOAT;
                    }
                    i += 2;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }

                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                var unitString = new String(dd[i + 1]);
                byBoxUnit = RedisGeo.Unit.fromString(unitString);
                if (byBoxUnit == RedisGeo.Unit.UNKNOWN) {
                    return ErrorReply.SYNTAX;
                }
                byBoxWidth = byBoxUnit.toMeters(byBoxWidth);
                byBoxHeight = byBoxUnit.toMeters(byBoxHeight);
                i++;
                hasByBox = true;
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        if (byRadius == -1 && byBoxWidth == -1) {
            return ErrorReply.SYNTAX;
        }

        if (!hasCenterSource) {
            return ErrorReply.SYNTAX;
        }

        if (byRadius != -1) {
            outputUnit = byRadiusUnit;
        } else {
            outputUnit = byBoxUnit;
        }

        var isNeedStoreToDstKey = dstKeyBytes != null;

        var rg = getRedisGeo(slotWithKeyHash);
        if (rg == null) {
            if (isNeedStoreToDstKey) {
                // redis deletes the stale destination key when the source key does not exist
                return deleteDstKeyAndReplyZero(dstS);
            }
            return MultiBulkReply.EMPTY;
        }

        if (fromMember != null) {
            var p = rg.get(fromMember);
            if (p == null) {
                // the source key exists but the member does not, redis replies an error here
                return new ErrorReply("could not decode requested zset member");
            }
            fromLon = p.lon();
            fromLat = p.lat();
        }

        var fromP = new RedisGeo.P(fromLon, fromLat);

        var map = rg.getMap();
        List<GeoSearchResult> resultList = new ArrayList<GeoSearchResult>();
        for (var entry : map.entrySet()) {
            var member = entry.getKey();
            var p = entry.getValue();
            double distance = RedisGeo.distance(p, fromP);

            if (byRadius != -1) {
                if (distance > byRadius) {
                    continue;
                }
            }

            if (byBoxWidth != -1) {
                if (!RedisGeo.isWithinBox(p, fromLon, fromLat, byBoxWidth, byBoxHeight)) {
                    continue;
                }
            }

            resultList.add(new GeoSearchResult(member, p, distance));
        }

        if (resultList.isEmpty()) {
            if (isNeedStoreToDstKey) {
                // redis deletes the stale destination key when no member matches
                return deleteDstKeyAndReplyZero(dstS);
            }
            return MultiBulkReply.EMPTY;
        }

        if (isDesc) {
            resultList.sort(Comparator.comparingDouble(GeoSearchResult::distance).reversed());
        } else {
            resultList.sort(Comparator.comparingDouble(GeoSearchResult::distance));
        }
        if (count != -1) {
            resultList = resultList.subList(0, Math.min(count, resultList.size()));
        }

        if (isNeedStoreToDstKey) {
            if (isStoreDist) {
                var dstRz = new RedisZSet();
                for (var result : resultList) {
                    dstRz.add(outputUnit.fromMeters(result.distance()), result.member());
                }
                if (!isCrossRequestWorker) {
                    ZGroup.saveRedisZSet(dstRz, dstS, this, dictMap);
                    return new IntegerReply(dstRz.size());
                } else {
                    var dstOneSlot = localPersist.oneSlot(dstS.slot());

                    SettablePromise<Reply> finalPromise = new SettablePromise<>();
                    var asyncReply = new AsyncReply(finalPromise);

                    var p = dstOneSlot.asyncRun(() -> {
                        ZGroup.saveRedisZSet(dstRz, dstS, this, dictMap);
                    });

                    p.whenComplete((v, t) -> {
                        if (t != null) {
                            finalPromise.setException(t);
                        } else {
                            finalPromise.set(new IntegerReply(dstRz.size()));
                        }
                    });

                    return asyncReply;
                }
            }

            var dstRg = new RedisGeo();
            for (var result : resultList) {
                // not sorted, todo
                dstRg.add(result.member(), result.p.lon(), result.p.lat());
            }

            if (!isCrossRequestWorker) {
                saveRedisGeo(dstRg, dstS);
                return new IntegerReply(dstRg.size());
            } else {
                var dstOneSlot = localPersist.oneSlot(dstS.slot());

                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var p = dstOneSlot.asyncRun(() -> {
                    saveRedisGeo(dstRg, dstS);
                });

                p.whenComplete((v, t) -> {
                    if (t != null) {
                        finalPromise.setException(t);
                    } else {
                        finalPromise.set(new IntegerReply(dstRg.size()));
                    }
                });

                return asyncReply;
            }
        }

        var subRepliesLength = 1;
        if (isWithDist) {
            subRepliesLength++;
        }
        if (isWithHash) {
            subRepliesLength++;
        }
        if (isWithCoord) {
            subRepliesLength++;
        }

        var replies = new Reply[resultList.size()];
        for (int i = 0; i < replies.length; i++) {
            var result = resultList.get(i);
            // only return member
            if (subRepliesLength == 1) {
                replies[i] = new BulkReply(result.member());
                continue;
            }

            var subReplies = new Reply[subRepliesLength];
            subReplies[0] = new BulkReply(result.member());

            var ii = 1;
            if (isWithDist) {
                subReplies[ii] = distanceReply(outputUnit.fromMeters(result.distance()));
                ii++;
            }
            if (isWithHash) {
                // redis replies the geohash score with an integer reply
                subReplies[ii] = new IntegerReply(RedisGeo.hashAsStore(result.p));
                ii++;
            }
            if (isWithCoord) {
                subReplies[ii] = new MultiBulkReply(new Reply[]{
                        new BulkReply(coordinate(result.p.lon())),
                        new BulkReply(coordinate(result.p.lat()))
                });
            }

            replies[i] = new MultiBulkReply(subReplies);
        }

        return new MultiBulkReply(replies);
    }
}
