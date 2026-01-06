package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Handles Redis commands starting with letter 'K'.
 * This includes commands like KEYS.
 */
public class KGroup extends BaseCommand {
    public KGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if (cmd.equals("keys")) {
            return keys();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    static final ErrorReply ONLY_SUPPORT_PREFIX_MATCH_PATTERN = new ErrorReply("only support prefix match pattern");

    private Reply keys() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        // only support prefix match pattern
        var patternFirstByte = data[1][0];
        if (patternFirstByte == '*' || patternFirstByte == '?' || patternFirstByte == '['
                || patternFirstByte == '.' || patternFirstByte == '+') {
            return ONLY_SUPPORT_PREFIX_MATCH_PATTERN;
        }

        var patternString = new String(data[1]);
        TreeSet<Integer> indexes = new TreeSet<>();
        indexes.add(-1);
        indexes.add(patternString.indexOf("*"));
        indexes.add(patternString.indexOf("?"));
        indexes.add(patternString.indexOf("["));
        indexes.add(patternString.indexOf("."));
        indexes.add(patternString.indexOf("+"));
        indexes.remove(-1);

        var index = indexes.isEmpty() ? -1 : indexes.first();
        if (index == -1) {
            return ONLY_SUPPORT_PREFIX_MATCH_PATTERN;
        }
        var keyPrefix = patternString.substring(0, index);

        // refer ManageCommand
        // manage dyn-config key value
        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        var maxCountStr = firstOneSlot.getDynConfig().get("keys.matchMaxCount");
        int maxCount = maxCountStr == null ? 100 : Integer.parseInt((String) maxCountStr);

        // todo, check if need add . before *
        var pattern = Pattern.compile(patternString);

        var keyAnalysisHandler = localPersist.getIndexHandlerPool().getKeyAnalysisHandler();
        var f = keyAnalysisHandler.prefixMatch(keyPrefix, pattern, maxCount);

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        f.whenComplete((r, e) -> {
            if (e != null) {
                log.error("keys error={}", e.getMessage());
                finalPromise.setException((Exception) e);
                return;
            }

            var replies = new Reply[r.size()];
            for (int i = 0; i < r.size(); i++) {
                replies[i] = new BulkReply(r.get(i));
            }
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }
}
