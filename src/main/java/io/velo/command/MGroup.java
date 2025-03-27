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
import org.jetbrains.annotations.VisibleForTesting;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.RestoreParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class MGroup extends BaseCommand {
    public MGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("mget".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("mset".equals(cmd) || "msetnx".equals(cmd)) {
            if (data.length < 3 || data.length % 2 == 0) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1Step2);
            return slotWithKeyHashList;
        }

        if ("manage".equals(cmd)) {
            var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ManageCommandParseSlots.groovy");

            var variables = new HashMap<String, Object>();
            variables.put("cmd", cmd);
            variables.put("data", data);
            variables.put("slotNumber", slotNumber);

            return (ArrayList<SlotWithKeyHash>) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("manage".equals(cmd)) {
            return manage();
        }

        if ("mget".equals(cmd)) {
            return mget();
        }

        if ("mset".equals(cmd)) {
            return mset(false);
        }

        if ("msetnx".equals(cmd)) {
            return mset(true);
        }

        if ("migrate".equals(cmd)) {
            return migrate();
        }

        if ("move".equals(cmd)) {
            return ErrorReply.NOT_SUPPORT;
        }

        return NilReply.INSTANCE;
    }

    record KeyBytesAndSlotWithKeyHash(byte[] keyBytes, int index, SlotWithKeyHash slotWithKeyHash) {
    }

    record KeyValueBytesAndSlotWithKeyHash(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHash) {
    }

    private record ValueBytesAndIndex(byte[] valueBytes, int index) {
    }

    @VisibleForTesting
    Reply mget() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            var replies = new Reply[data.length - 1];
            for (int i = 1, j = 0; i < data.length; i++, j++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                var cv = getCv(keyBytes, slotWithKeyHash);
                if (cv == null || !cv.isTypeString()) {
                    replies[j] = NilReply.INSTANCE;
                } else {
                    var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
                    replies[j] = new BulkReply(valueBytes);
                }
            }
            return new MultiBulkReply(replies);
        }

        ArrayList<KeyBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var keyBytes = data[i];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new KeyBytesAndSlotWithKeyHash(keyBytes, j, slotWithKeyHash));
        }

        ArrayList<Promise<ArrayList<ValueBytesAndIndex>>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
                for (var one : subList) {
                    var cv = getCv(one.keyBytes, one.slotWithKeyHash);
                    byte[] valueBytes;
                    if (cv == null || !cv.isTypeString()) {
                        valueBytes = null;
                    } else {
                        valueBytes = getValueBytesByCv(cv, one.keyBytes, one.slotWithKeyHash);
                    }
                    valueList.add(new ValueBytesAndIndex(valueBytes, one.index));
                }
                return valueList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mget error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
            for (var p : promises) {
                valueList.addAll(p.getResult());
            }

            var replies = new Reply[data.length - 1];
            for (var one : valueList) {
                var index = one.index();
                var valueBytes = one.valueBytes();
                if (valueBytes == null) {
                    replies[index] = NilReply.INSTANCE;
                } else {
                    replies[index] = new BulkReply(valueBytes);
                }
            }
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply mset(boolean isNx) {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            int setNumber = 0;
            for (int i = 1, j = 0; i < data.length; i += 2, j++) {
                var keyBytes = data[i];
                var valueBytes = data[i + 1];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                if (isNx) {
                    if (!exists(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), slotWithKeyHash.rawKey(), slotWithKeyHash.keyHash(), slotWithKeyHash.keyHash32())) {
                        setNumber++;
                        set(keyBytes, valueBytes, slotWithKeyHash);
                    }
                } else {
                    set(keyBytes, valueBytes, slotWithKeyHash);
                }
            }
            return isNx ? new IntegerReply(setNumber) : OKReply.INSTANCE;
        }

        ArrayList<KeyValueBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i += 2, j++) {
            var keyBytes = data[i];
            var valueBytes = data[i + 1];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new KeyValueBytesAndSlotWithKeyHash(keyBytes, valueBytes, slotWithKeyHash));
        }

        ArrayList<Promise<Integer>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                int setNumber = 0;
                for (var one : subList) {
                    if (isNx) {
                        var s = one.slotWithKeyHash;
                        if (!exists(s.slot(), s.bucketIndex(), s.rawKey(), s.keyHash(), s.keyHash32())) {
                            setNumber++;
                            set(one.keyBytes, one.valueBytes, one.slotWithKeyHash);
                        }
                    } else {
                        set(one.keyBytes, one.valueBytes, one.slotWithKeyHash);
                    }
                }
                return setNumber;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mset error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            var setNumber = 0;
            for (var p : promises) {
                setNumber += p.getResult();
            }

            finalPromise.set(isNx ? new IntegerReply(setNumber) : OKReply.INSTANCE);
        });

        return asyncReply;
    }

    // todo, migrate use dump
    private byte[] dump(CompressedValue cv, byte[] keyBytes, SlotWithKeyHash s) {
        return getValueBytesByCv(cv, keyBytes, s);
    }

    private void restore(byte[] keyBytes, byte[] dumpBytes, long expireAt, RestoreParams restoreParams, Jedis jedisTo) {
//        jedisTo.restore(keyBytes, expireAt, dumpBytes, restoreParams);
        if (expireAt != CompressedValue.NO_EXPIRE) {
            jedisTo.psetex(keyBytes, expireAt - System.currentTimeMillis(), dumpBytes);
        } else {
            jedisTo.set(keyBytes, dumpBytes);
        }
    }

    private record DumpBytesAndTtlAndIndex(byte[] dumpBytes, long expireAt, int index) {
    }

    private Reply migrate() {
        if (data.length < 7) {
            return ErrorReply.FORMAT;
        }

        var host = new String(data[1]);
        var port = Integer.parseInt(new String(data[2]));
        var keyOrBlank = new String(data[3]);
        var destinationDb = Integer.parseInt(new String(data[4]));
        var timeoutMs = Integer.parseInt(new String(data[5]));

        boolean isCopy = false;
        boolean isReplace = false;
        String authUsername = null;
        String authPassword = null;

        ArrayList<String> keys = new ArrayList<>();
        for (int i = 6; i < data.length; i++) {
            var arg = new String(data[i]);
            if (arg.equalsIgnoreCase("keys")) {
                if (i + 1 < data.length) {
                    return ErrorReply.SYNTAX;
                }
                for (int j = i + 1; j < data.length; j++) {
                    keys.add(new String(data[j]));
                }
                break;
            } else if (arg.equalsIgnoreCase("copy")) {
                isCopy = true;
            } else if (arg.equalsIgnoreCase("replace")) {
                isReplace = true;
            } else if (arg.equalsIgnoreCase("auth")) {
                if (i + 2 < data.length) {
                    return ErrorReply.SYNTAX;
                }
                if (new String(data[i + 2]).equalsIgnoreCase("keys")) {
                    authPassword = new String(data[i + 1]);
                } else {
                    authUsername = new String(data[i + 1]);
                    authPassword = new String(data[i + 2]);
                }
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        if (keys.isEmpty()) {
            return ErrorReply.SYNTAX;
        }

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(timeoutMs).build();
        Jedis jedisTo;
        try {
            jedisTo = new Jedis(host, port, jedisClientConfig);
            jedisTo.select(destinationDb);

            if (authUsername != null) {
                jedisTo.auth(authUsername, authPassword);
            } else if (authPassword != null) {
                jedisTo.auth(authPassword);
            }
        } catch (Exception e) {
            return new ErrorReply(e.getMessage());
        }
        log.info("Connect to {}:{}", host, port);

        var restoreParams = new RestoreParams();
        restoreParams.absTtl();
        if (isReplace) {
            restoreParams.replace();
        }

        if (!isCrossRequestWorker) {
            int count = 0;
            for (var key : keys) {
                var s = slot(key.getBytes());
                var cv = getCv(key.getBytes(), s);
                if (cv == null) {
                    continue;
                }

                var dumpBytes = dump(cv, key.getBytes(), s);
                restore(key.getBytes(), dumpBytes, cv.getExpireAt(), restoreParams, jedisTo);
                count++;

                if (isReplace) {
                    removeDelay(s.slot(), s.bucketIndex(), key, s.keyHash());
                }
            }
            log.info("Migrate {} keys to {}:{}", count, host, port);

            jedisTo.close();
            log.info("Close jedis {}:{}", host, port);

            return count != 0 ? OKReply.INSTANCE : new BulkReply("NOKEY".getBytes());
        } else {
            ArrayList<KeyBytesAndSlotWithKeyHash> list = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes);
                list.add(new KeyBytesAndSlotWithKeyHash(keyBytes, i, slotWithKeyHash));
            }

            ArrayList<Promise<ArrayList<DumpBytesAndTtlAndIndex>>> promises = new ArrayList<>();
            var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
            for (var entry : groupBySlot.entrySet()) {
                var slot = entry.getKey();
                var subList = entry.getValue();

                var oneSlot = localPersist.oneSlot(slot);
                var p = oneSlot.asyncCall(() -> {
                    ArrayList<DumpBytesAndTtlAndIndex> dumpBytesList = new ArrayList<>();
                    for (var one : subList) {
                        var cv = getCv(one.keyBytes, one.slotWithKeyHash);
                        if (cv == null) {
                            continue;
                        }

                        var dumpBytes = dump(cv, one.keyBytes, one.slotWithKeyHash);
                        dumpBytesList.add(new DumpBytesAndTtlAndIndex(dumpBytes, cv.getExpireAt(), one.index));
                    }
                    return dumpBytesList;
                });
                promises.add(p);
            }

            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            Promises.all(promises).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("migrate error={}", e.getMessage());
                    finalPromise.setException(e);
                    return;
                }

                var count = 0;
                for (var p : promises) {
                    for (var d : p.getResult()) {
                        var one = list.get(d.index);
                        restore(one.slotWithKeyHash.rawKey().getBytes(), d.dumpBytes, d.expireAt, restoreParams, jedisTo);
                        count++;
                    }
                }

                jedisTo.close();
                log.info("Close jedis {}:{}", host, port);

                var rr = count != 0 ? OKReply.INSTANCE : new BulkReply("NOKEY".getBytes());
                finalPromise.set(rr);
            });

            return asyncReply;
        }
    }

    private Reply manage() {
        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ManageCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("mGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }
}
