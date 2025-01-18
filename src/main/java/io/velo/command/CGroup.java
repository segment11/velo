package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.SocketInspector;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;

public class CGroup extends BaseCommand {
    public CGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("copy".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1And2);
            return slotWithKeyHashList;
        }

        // client can use any slot
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("client".equals(cmd)) {
            return client();
        }

        if ("cluster".equals(cmd) || "clusterx".equals(cmd)) {
            return clusterx();
        }

        if ("command".equals(cmd)) {
            return command();
        }

        if ("config".equals(cmd)) {
            return config();
        }

        if ("copy".equals(cmd)) {
            return copy();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply client() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();
        if ("getname".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            var clientName = veloUserData.getClientName();
            return clientName == null ? NilReply.INSTANCE : new BulkReply(clientName.getBytes());
        }

        if ("id".equals(subCmd)) {
            return new IntegerReply(socket.hashCode());
        }

        if ("setinfo".equals(subCmd)) {
            return OKReply.INSTANCE;
        }

        if ("setname".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            veloUserData.setClientName(new String(data[2]));
            return OKReply.INSTANCE;
        }

        // todo
        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply clusterx() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ClusterxCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply command() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/CommandCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply config() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ConfigCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply copy() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        boolean replace = false;
        for (int i = 3; i < data.length; i++) {
            if ("replace".equalsIgnoreCase(new String(data[i]))) {
                replace = true;
                break;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var srcCv = getCv(srcKeyBytes, slotWithKeyHash);
        if (srcCv == null) {
            return IntegerReply.REPLY_0;
        }

        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
        if (isCrossRequestWorker) {
            // current net worker is src key slot's net worker
            var dstSlot = dstSlotWithKeyHash.slot();
            var dstOneSlot = localPersist.oneSlot(dstSlot);

            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            boolean finalReplace = replace;
            dstOneSlot.asyncRun(() -> {
                var dstCv = getCv(dstKeyBytes, dstSlotWithKeyHash);
                if (dstCv != null && !finalReplace) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }

                setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
                finalPromise.set(IntegerReply.REPLY_1);
            });

            return asyncReply;
        } else {
            var existCv = getCv(dstKeyBytes, dstSlotWithKeyHash);
            if (existCv != null && !replace) {
                return IntegerReply.REPLY_0;
            }

            setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
            return IntegerReply.REPLY_1;
        }
    }
}
