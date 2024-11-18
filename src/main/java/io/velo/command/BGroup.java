package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.BitSet;

public class BGroup extends BaseCommand {
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        if ("bitcount".equals(cmd) || "bitfield".equals(cmd) || "bitfield_ro".equals(cmd) ||
                "bitpos".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("bitop".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }

            // begin with dst key
            // eg. bitop and dest src1 src2
            for (int i = 2; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("bitcount".equals(cmd)) {
            return bitcount();
        }

        if ("bitpos".equals(cmd)) {
            return bitpos();
        }

        if ("bgsave".equals(cmd)) {
            // pure memory need to flush to disk, todo
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply bitcount() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int start;
        int end;
        boolean isIndexUseBit;
        if (data.length == 2) {
            start = 0;
            end = -1;
            isIndexUseBit = false;
        } else {
            if (data.length != 4 && data.length != 5) {
                return ErrorReply.SYNTAX;
            }

            try {
                start = Integer.parseInt(new String(data[2]));
                end = Integer.parseInt(new String(data[3]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            isIndexUseBit = data.length == 5 && "bit".equalsIgnoreCase(new String(data[4]));
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
        var canIndexValueBytesLength = isIndexUseBit ? valueBytes.length * 8 : valueBytes.length;

        var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
        if (!startEndWith.valid()) {
            return IntegerReply.REPLY_0;
        }

        var bitset = BitSet.valueOf(valueBytes);
        int count = 0;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if (bitset.get(i)) {
                    count++;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if (bitset.get(i)) {
                    count++;
                }
            }
        }

        return new IntegerReply(count);
    }

    @VisibleForTesting
    Reply bitpos() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var bit1or0Bytes = data[2];
        if (bit1or0Bytes.length != 1) {
            return ErrorReply.INVALID_INTEGER;
        }
        var isBit1 = bit1or0Bytes[0] == '1';
        if (!isBit1 && bit1or0Bytes[0] != '0') {
            return ErrorReply.INVALID_INTEGER;
        }

        int start;
        int end;
        boolean isIndexUseBit;
        if (data.length == 3) {
            start = 0;
            end = -1;
            isIndexUseBit = false;
        } else {
            if (data.length != 4 && data.length != 5 && data.length != 6) {
                return ErrorReply.SYNTAX;
            }

            try {
                start = Integer.parseInt(new String(data[3]));
                end = data.length >= 5 ? Integer.parseInt(new String(data[4])) : -1;
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            isIndexUseBit = data.length == 6 && "bit".equalsIgnoreCase(new String(data[5]));
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return new IntegerReply(-1);
        }

        if (!cv.isTypeString()) {
            return ErrorReply.WRONG_TYPE;
        }

        var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
        var canIndexValueBytesLength = isIndexUseBit ? valueBytes.length * 8 : valueBytes.length;

        var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
        if (!startEndWith.valid()) {
            return new IntegerReply(-1);
        }

        var bitset = BitSet.valueOf(valueBytes);
        int pos = -1;
        if (isIndexUseBit) {
            for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
                if ((bitset.get(i) && isBit1) || (!bitset.get(i) && !isBit1)) {
                    pos = i;
                    break;
                }
            }
        } else {
            for (int i = startEndWith.start() * 8; i <= (startEndWith.end() + 1) * 8 - 1; i++) {
                if ((bitset.get(i) && isBit1) || (!bitset.get(i) && !isBit1)) {
                    pos = i;
                    break;
                }
            }
        }

        return new IntegerReply(pos);
    }
}
