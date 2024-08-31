package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class DGroup extends BaseCommand {
    public DGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("del".equals(cmd)) {
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

        if ("decr".equals(cmd) || "decrby".equals(cmd)) {
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
        if ("del".equals(cmd)) {
            return del();
        }

        if ("dbsize".equals(cmd)) {
            return dbsize();
        }

        if ("decr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            return decrBy(1, 0);
        }

        if ("decrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            try {
                int by = Integer.parseInt(new String(data[2]));
                return decrBy(by, 0);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply del() {
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
                var key = new String(keyBytes);

                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                var slot = slotWithKeyHash.slot();
                var bucketIndex = slotWithKeyHash.bucketIndex();
                var keyHash = slotWithKeyHash.keyHash();

                // remove delay, perf better
                var isRemoved = remove(slot, bucketIndex, key, keyHash);
                if (isRemoved) {
                    n++;
                }
            }
            return new IntegerReply(n);
        }

        ArrayList<SlotWithKeyHashWithKeyBytes> list = new ArrayList<>(data.length - 1);
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new SlotWithKeyHashWithKeyBytes(slotWithKeyHash, data[i]));
        }

        ArrayList<Promise<ArrayList<Boolean>>> promises = new ArrayList<>();
        // group by slot
        var groupBySlot = list.stream().collect(Collectors.groupingBy(it -> it.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<Boolean> valueList = new ArrayList<>();
                for (var one : subList) {
                    var key = new String(one.keyBytes());
                    var bucketIndex = one.slotWithKeyHash().bucketIndex();
                    var keyHash = one.slotWithKeyHash().keyHash();

                    var isRemoved = remove(oneSlot.slot(), bucketIndex, key, keyHash);
                    valueList.add(isRemoved);
                }
                return valueList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("del error: {}", e.getMessage());
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

    @VisibleForTesting
    Reply dbsize() {
        // skip
        if (data.length == 2) {
            return ErrorReply.FORMAT;
        }

        Promise<Long>[] promises = new Promise[slotNumber];
        for (int i = 0; i < slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncCall(oneSlot::getAllKeyCount);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("dbsize error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            long n = 0;
            for (var p : promises) {
                n += p.getResult();
            }

            finalPromise.set(new IntegerReply(n));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply decrBy(int by, double byFloat) {
        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean isByFloat = byFloat != 0;
        final var NOT_NUMBER_REPLY = isByFloat ? ErrorReply.NOT_FLOAT : ErrorReply.NOT_INTEGER;

        long longValue = 0;
        double doubleValue = 0;

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);

        if (cv != null && cv.isTypeNumber()) {
            if (isByFloat) {
                doubleValue = cv.numberValue().doubleValue();
            } else {
                longValue = cv.numberValue().longValue();
            }
        } else {
            if (cv != null && cv.isCompressed()) {
                return NOT_NUMBER_REPLY;
            }

            if (cv != null) {
                try {
                    var numberStr = new String(cv.getCompressedData());
                    if (isByFloat) {
                        doubleValue = Double.parseDouble(numberStr);
                    } else {
                        longValue = Long.parseLong(numberStr);
                    }
                } catch (NumberFormatException e) {
                    return NOT_NUMBER_REPLY;
                }
            }
        }

        if (isByFloat) {
            // scale 2, todo
            var newValue = BigDecimal.valueOf(doubleValue).setScale(2, RoundingMode.UP)
                    .subtract(BigDecimal.valueOf(byFloat).setScale(2, RoundingMode.UP));

            setNumber(keyBytes, newValue.doubleValue(), slotWithKeyHash);
            // double use bulk reply
            return new BulkReply(newValue.toPlainString().getBytes());
        } else {
            long newValue = longValue - by;
            setNumber(keyBytes, newValue, slotWithKeyHash);
            return new IntegerReply(newValue);
        }
    }
}
