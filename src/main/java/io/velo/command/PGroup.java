package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.persist.LocalPersist;
import io.velo.reply.*;

import java.util.ArrayList;

public class PGroup extends BaseCommand {
    public PGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("pexpire".equals(cmd) || "pexpireat".equals(cmd)) {
            if (data.length != 3 && data.length != 4) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("pexpiretime".equals(cmd) || "pttl".equals(cmd)) {
            if (data.length != 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("pexpire".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(false, true);
        }

        if ("pexpireat".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(true, true);
        }

        if ("pexpiretime".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expiretime(true);
        }

        if ("pttl".equals(cmd)) {
            var tGroup = new TGroup(cmd, data, socket);
            tGroup.from(this);
            return tGroup.ttl(true);
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[3], "px".getBytes(), data[2]};
            var sGroup = new SGroup(cmd, dd, socket);
            sGroup.from(this);
            return sGroup.set(dd);
        }

        if ("publish".equals(cmd)) {
            return publish(data);
        }

        return NilReply.INSTANCE;
    }

    public static Reply publish(byte[][] dataGiven) {
        if (dataGiven.length != 3) {
            return ErrorReply.FORMAT;
        }

        var localPersist = LocalPersist.getInstance();
        var socketInInspector = localPersist.getSocketInspector();

        var channel = new String(dataGiven[1]);
        var message = new String(dataGiven[2]);

        var n = socketInInspector.subscribeSocketCount(channel);

        var replies = new Reply[3];
        replies[0] = new BulkReply(channel.getBytes());
        replies[1] = new BulkReply(message.getBytes());
        replies[2] = new IntegerReply(n);

        socketInInspector.publish(channel, new MultiBulkReply(replies), (s, r) -> {
            s.write(r.buffer());
        });
        return new IntegerReply(n);
    }
}
