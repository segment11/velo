package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;

public class IGroup extends BaseCommand {
    public IGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("incr".equals(cmd) || "incrby".equals(cmd) || "incrbyfloat".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndex1);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("incr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(-1, 0);
        }

        if ("incrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            int by;
            try {
                by = Integer.parseInt(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(-by, 0);
        }

        if ("incrbyfloat".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            double by;
            try {
                by = Double.parseDouble(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(0, -by);
        }

        if ("info".equals(cmd)) {
            return info();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply info() {
        if (data.length != 1 && data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/InfoCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("iGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }
}
