package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
import io.velo.type.RedisGeo;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static io.velo.CompressedValue.NO_EXPIRE;

public class GGroup extends BaseCommand {
    public GGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("get".equals(cmd) || "getbit".equals(cmd) || "getdel".equals(cmd) || "getex".equals(cmd)
                || "getrange".equals(cmd) || "getset".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

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
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
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

        if (cmd.startsWith("geo")) {
            return geo();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply getbit() {
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
        var valueBytes = get(keyBytes, slotWithKeyHash);
        if (valueBytes == null) {
            return IntegerReply.REPLY_0;
        }

        int byteIndex = offset / 8;
        int bitIndex = offset % 8;
        if (byteIndex >= valueBytes.length) {
            return IntegerReply.REPLY_0;
        }

        byte b = valueBytes[byteIndex];
        int bit = (b >> bitIndex) & 1;
        return bit == 1 ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
    Reply getdel() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytes = get(keyBytes, slotWithKeyHash);
        if (valueBytes != null) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), new String(keyBytes), slotWithKeyHash.keyHash());
            return new BulkReply(valueBytes);
        } else {
            return NilReply.INSTANCE;
        }
    }

    @VisibleForTesting
    Reply getex() {
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
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return NilReply.INSTANCE;
        }

        var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);

        long expireAt = cv.getExpireAt();
        long expireAtOld = expireAt;
        if (isPersist) {
            expireAt = NO_EXPIRE;
        } else if (ex > -1) {
            expireAt = System.currentTimeMillis() + ex * 1000;
        } else if (px > -1) {
            expireAt = System.currentTimeMillis() + px;
        } else if (exAt > -1) {
            expireAt = exAt * 1000;
        } else if (pxAt > -1) {
            expireAt = pxAt;
        }

        if (expireAt != expireAtOld) {
            cv.setExpireAt(expireAt);
            setCv(keyBytes, cv, slotWithKeyHash);
        }
        return new BulkReply(valueBytes);
    }

    private final static Reply BLANK_REPLY = new BulkReply(new byte[0]);

    @VisibleForTesting
    Reply getrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];

        int start;
        int end;
        try {
            start = Integer.parseInt(new String(data[2]));
            end = Integer.parseInt(new String(data[3]));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var valueBytes = get(keyBytes, slotWithKeyHash);
        if (valueBytes == null) {
            return NilReply.INSTANCE;
        }

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

    @VisibleForTesting
    Reply getset() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var valueBytes = data[2];

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        // not only for type string, other types will be overwritten
        var valueBytesExist = get(keyBytes, slotWithKeyHash);
        if (valueBytesExist == null) {
            return NilReply.INSTANCE;
        }

        set(keyBytes, valueBytes, slotWithKeyHash);
        return new BulkReply(valueBytesExist);
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

    private RedisGeo getRedisGeo(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_GEO);
        if (encodedBytes == null) {
            return null;
        }

        return RedisGeo.decode(encodedBytes);
    }

    @VisibleForTesting
    void saveRedisGeo(RedisGeo rg, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var key = new String(keyBytes);
        if (rg.isEmpty()) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        set(keyBytes, rg.encode(), slotWithKeyHash, CompressedValue.SP_TYPE_GEO);
    }

    private Reply geoadd() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var s = slotWithKeyHashListParsed.getFirst();

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

        var rg = getRedisGeo(keyBytes, s);
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
            saveRedisGeo(rg, keyBytes, s);
        }
        return new IntegerReply(isIncludeCh ? changed + added : added);
    }

    private Reply geodist() {
        if (data.length != 4 && data.length != 5) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var s = slotWithKeyHashListParsed.getFirst();

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

        var rg = getRedisGeo(keyBytes, s);
        if (rg == null) {
            return NilReply.INSTANCE;
        }

        var p0 = rg.get(m0);
        var p1 = rg.get(m1);
        if (p0 == null || p1 == null) {
            return NilReply.INSTANCE;
        }

        var distance = RedisGeo.distance(p0, p1);
        return new BulkReply(unit.toMeters(distance));
    }

    private Reply geohash(boolean isGeopos) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var s = slotWithKeyHashListParsed.getFirst();
        var replies = new Reply[data.length - 2];

        var rg = getRedisGeo(keyBytes, s);
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
                                new BulkReply(p.lon()),
                                new BulkReply(p.lat())
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

    private Reply geosearch(byte[][] dd, byte[] dstKeyBytes, SlotWithKeyHash dstS) {
        var keyBytes = dd[1];
        var s = slotWithKeyHashListParsed.getFirst();

        double fromLon = 0d;
        double fromLat = 0d;
        String fromMember = null;

        double byRadius = -1;
        var byRadiusUnit = RedisGeo.Unit.M;
        double byBoxWidth = -1;
        double byBoxHeight = -1;
        var byBoxUnit = RedisGeo.Unit.M;

        boolean isDesc = false;
        int count = -1;
        boolean isWithDist = false;
        boolean isWithHash = false;
        boolean isWithCoord = false;

        for (int i = 2; i < dd.length; i++) {
            var arg = new String(dd[i]);
            if ("asc".equalsIgnoreCase(arg)) {
                isDesc = false;
            } else if ("desc".equalsIgnoreCase(arg)) {
                isDesc = true;
            } else if ("count".equalsIgnoreCase(arg)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    count = Integer.parseInt(new String(dd[i + 1]));
                    if (count <= 0) {
                        return ErrorReply.INVALID_INTEGER;
                    }
                    i++;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
            } else if ("fromlonlat".equalsIgnoreCase(arg)) {
                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    fromLon = Double.parseDouble(new String(dd[i + 1]));
                    fromLat = Double.parseDouble(new String(dd[i + 2]));
                    i += 2;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            } else if ("frommember".equalsIgnoreCase(arg)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                fromMember = new String(dd[i + 1]);
                i++;
            } else if ("byradius".equalsIgnoreCase(arg)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    byRadius = Double.parseDouble(new String(dd[i + 1]));
                    i++;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }

                // maybe has unit
                if (dd.length > i + 1) {
                    var unitString = new String(dd[i + 1]);
                    byRadiusUnit = RedisGeo.Unit.fromString(unitString);
                    if (byRadiusUnit != RedisGeo.Unit.UNKNOWN) {
                        byRadius = byRadiusUnit.toMeters(byRadius);
                        i++;
                    }
                }
            } else if ("bybox".equalsIgnoreCase(arg)) {
                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                try {
                    byBoxWidth = Double.parseDouble(new String(dd[i + 1]));
                    byBoxHeight = Double.parseDouble(new String(dd[i + 2]));
                    i += 2;
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }

                // maybe has unit
                if (dd.length > i + 1) {
                    var unitString = new String(dd[i + 1]);
                    byBoxUnit = RedisGeo.Unit.fromString(unitString);
                    if (byBoxUnit != RedisGeo.Unit.UNKNOWN) {
                        byBoxWidth = byBoxUnit.toMeters(byBoxWidth);
                        byBoxHeight = byBoxUnit.toMeters(byBoxHeight);
                        i++;
                    }
                }
            } else if ("withdist".equalsIgnoreCase(arg)) {
                isWithDist = true;
            } else if ("withhash".equalsIgnoreCase(arg)) {
                isWithHash = true;
            } else if ("withcoord".equalsIgnoreCase(arg)) {
                isWithCoord = true;
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        if (byRadius == -1 && byBoxWidth == -1) {
            return ErrorReply.SYNTAX;
        }

        var isNeedStoreToDstKey = dstKeyBytes != null;

        var rg = getRedisGeo(keyBytes, s);
        if (rg == null) {
            return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        if (fromMember != null) {
            var p = rg.get(fromMember);
            if (p == null) {
                return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
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
            return isNeedStoreToDstKey ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
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
            var dstRg = new RedisGeo();
            for (var result : resultList) {
                // not sorted, todo
                dstRg.add(result.member(), result.p.lon(), result.p.lat());
            }

            if (!isCrossRequestWorker) {
                saveRedisGeo(dstRg, dstKeyBytes, dstS);
                return new IntegerReply(dstRg.size());
            } else {
                var dstOneSlot = localPersist.oneSlot(dstS.slot());

                SettablePromise<Reply> finalPromise = new SettablePromise<>();
                var asyncReply = new AsyncReply(finalPromise);

                var p = dstOneSlot.asyncRun(() -> {
                    saveRedisGeo(dstRg, dstKeyBytes, dstS);
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

            var subReplies = new Reply[subRepliesLength];
            subReplies[0] = new BulkReply(result.member().getBytes());

            var ii = 1;
            if (isWithDist) {
                subReplies[ii] = new BulkReply(result.distance());
                ii++;
            }
            if (isWithHash) {
                subReplies[ii] = new BulkReply(RedisGeo.hashAsStore(result.p));
                ii++;
            }
            if (isWithCoord) {
                subReplies[ii] = new MultiBulkReply(new Reply[]{
                        new BulkReply(result.p.lon()),
                        new BulkReply(result.p.lat())
                });
            }

            replies[i] = new MultiBulkReply(subReplies);
        }

        return new MultiBulkReply(replies);
    }
}
