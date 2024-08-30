package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class MGroup extends BaseCommand {
    public MGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("mget".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }

            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("mset".equals(cmd)) {
            if (data.length < 3 || data.length % 2 == 0) {
                return slotWithKeyHashList;
            }

            for (int i = 1; i < data.length; i += 2) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
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
        if ("mget".equals(cmd)) {
            return mget();
        }

        if ("mset".equals(cmd)) {
            return mset();
        }

        if ("manage".equals(cmd)) {
            return manage();
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
                var valueBytes = get(keyBytes, slotWithKeyHash);
                if (valueBytes == null) {
                    replies[j] = NilReply.INSTANCE;
                } else {
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
                    var valueBytes = get(one.keyBytes, one.slotWithKeyHash);
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
                log.error("mget error: {}", e.getMessage());
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
    Reply mset() {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            for (int i = 1, j = 0; i < data.length; i += 2, j++) {
                var keyBytes = data[i];
                var valueBytes = data[i + 1];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                set(keyBytes, valueBytes, slotWithKeyHash);
            }
            return OKReply.INSTANCE;
        }

        ArrayList<KeyValueBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i += 2, j++) {
            var keyBytes = data[i];
            var valueBytes = data[i + 1];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new KeyValueBytesAndSlotWithKeyHash(keyBytes, valueBytes, slotWithKeyHash));
        }

        ArrayList<Promise<Void>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncRun(() -> {
                for (var one : subList) {
                    set(one.keyBytes, one.valueBytes, one.slotWithKeyHash);
                }
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mset error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }

    private Reply manage() {
        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ManageCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("mGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }
}
