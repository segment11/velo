package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.reply.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class EGroup extends BaseCommand {
    public EGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    private final CachedGroovyClassLoader cl = CachedGroovyClassLoader.getInstance();

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("exists".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("expire".equals(cmd) || "expireat".equals(cmd) || "expiretime".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("extend".equals(cmd)) {
            var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ExtendCommandParseSlots.groovy");

            var variables = new HashMap<String, Object>();
            variables.put("cmd", cmd);
            variables.put("data", data);
            variables.put("slotNumber", slotNumber);

            return (ArrayList<SlotWithKeyHash>) cl.eval(scriptText, variables);
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("echo".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }
            return new BulkReply(data[1]);
        }

        if ("exists".equals(cmd)) {
            return exists();
        }

        if ("expire".equals(cmd)) {
            return expire(false, false);
        }

        if ("expireat".equals(cmd)) {
            return expire(true, false);
        }

        if ("expiretime".equals(cmd)) {
            return expiretime(false);
        }

        if ("extend".equals(cmd)) {
            return extend();
        }

        return NilReply.INSTANCE;
    }

    private Reply exists() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            int n = 0;
            for (int i = 1, j = 0; i < data.length; i++, j++) {
                var keyBytes = data[i];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }

                var s = slotWithKeyHashListParsed.get(j);
                // remove delay, perf better
                var isExists = exists(s.slot(), s.bucketIndex(), new String(keyBytes), s.keyHash(), s.keyHash32());
                if (isExists) {
                    n++;
                }
            }
            return new IntegerReply(n);
        }

        ArrayList<Promise<ArrayList<Boolean>>> promises = new ArrayList<>();
        // group by slot
        var groupBySlot = slotWithKeyHashListParsed.stream().collect(Collectors.groupingBy(SlotWithKeyHash::slot));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<Boolean> valueList = new ArrayList<>();
                for (var s : subList) {
                    var isExists = exists(oneSlot.slot(), s.bucketIndex(), s.rawKey(), s.keyHash(), s.keyHash32());
                    valueList.add(isExists);
                }
                return valueList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("exists error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            int n = 0;
            for (var p : promises) {
                for (var b : p.getResult()) {
                    if (b) {
                        n++;
                    }
                }
            }

            finalPromise.set(new IntegerReply(n));
        });

        return asyncReply;
    }

    Reply expire(boolean isAt, boolean isMilliseconds) {
        if (data.length != 3 && data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var secondsBytes = data[2];

        long seconds;
        try {
            seconds = Long.parseLong(new String(secondsBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        long expireAt;
        if (isMilliseconds) {
            expireAt = isAt ? seconds : System.currentTimeMillis() + seconds;
        } else {
            expireAt = isAt ? seconds * 1000 : System.currentTimeMillis() + seconds * 1000;
        }

        boolean isNx = false;
        boolean isXx = false;
        boolean isGt = false;
        boolean isLt = false;

        if (data.length == 4) {
            var typeBytes = data[3];
            var type = new String(typeBytes);
            isNx = "nx".equalsIgnoreCase(type);
            isXx = "xx".equalsIgnoreCase(type);
            isGt = "gt".equalsIgnoreCase(type);
            isLt = "lt".equalsIgnoreCase(type);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        // if nx xx gt lt, need not read cv, just read expire at from key bucket, todo
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        var expireAtExist = cv.getExpireAt();
        if (isNx && expireAtExist != CompressedValue.NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }
        if (isXx && expireAtExist == CompressedValue.NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }
        if (isGt && expireAtExist != CompressedValue.NO_EXPIRE && expireAtExist >= expireAt) {
            return IntegerReply.REPLY_0;
        }
        if (isLt && expireAtExist != CompressedValue.NO_EXPIRE && expireAtExist <= expireAt) {
            return IntegerReply.REPLY_0;
        }

        cv.setSeq(snowFlake.nextId());
        cv.setExpireAt(expireAt);

        setCv(cv, slotWithKeyHash);
        return IntegerReply.REPLY_1;
    }

    Reply expiretime(boolean isMilliseconds) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(slotWithKeyHash);
        if (cv == null) {
            return new IntegerReply(-2);
        }

        var expireAt = cv.getExpireAt();
        if (expireAt == CompressedValue.NO_EXPIRE) {
            return new IntegerReply(-1);
        }

        return new IntegerReply(isMilliseconds ? expireAt : expireAt / 1000);
    }

    private Reply extend() {
        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ExtendCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("eGroup", this);
        return (Reply) cl.eval(scriptText, variables);
    }
}
