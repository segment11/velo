package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

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
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
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
}
