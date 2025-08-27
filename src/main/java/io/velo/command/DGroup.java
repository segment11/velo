package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.persist.OneSlot;
import io.velo.reply.*;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DGroup extends BaseCommand {
    public DGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("debug".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            var subCmd = new String(data[1]).toLowerCase();
            // debug object key
            if ("object".equals(subCmd)) {
                var keyBytes = data[2];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
                return slotWithKeyHashList;
            }
            // add other debug sub command
        }

        if ("del".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("decr".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
        }

        if ("decrby".equals(cmd) || "decrbyfloat".equals(cmd)) {
            if (data.length != 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
        }

        if ("dump".equals(cmd)) {
            if (data.length != 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("debug".equals(cmd)) {
            return debug();
        }

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

        if ("decrbyfloat".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            double by;
            try {
                by = Double.parseDouble(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            return decrBy(0, by);
        }

        if ("dump".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            return dump();
        }

        return NilReply.INSTANCE;
    }

    private String wrapEncodingType(String encodingType, CompressedValue cv) {
        return "refcount:1 encoding:" + encodingType + " serializedlength:" + cv.getCompressedLength();
    }

    private Reply debug() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();
        if ("object".equals(subCmd)) {
            var keyBytes = data[2];
            var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
            var cv = getCv(keyBytes, slotWithKeyHash);
            if (cv == null) {
                return NilReply.INSTANCE;
            }

            if (cv.isTypeNumber()) {
                if (cv.isTypeDouble()) {
                    return new BulkReply(wrapEncodingType("embstr", cv).getBytes());
                } else {
                    return new BulkReply(wrapEncodingType("int", cv).getBytes());
                }
            } else if (cv.isHash()) {
                return new BulkReply(wrapEncodingType("hashtable", cv).getBytes());
            } else if (cv.isList()) {
                return new BulkReply(wrapEncodingType("quicklist", cv).getBytes());
            } else if (cv.isSet()) {
                // intset
                return new BulkReply(wrapEncodingType("hashtable", cv).getBytes());
            } else if (cv.isZSet()) {
                // skiplist
                return new BulkReply(wrapEncodingType("ziplist", cv).getBytes());
            } else if (cv.isStream()) {
                return new BulkReply(wrapEncodingType("stream", cv).getBytes());
            } else if (cv.isTypeString()) {
                return new BulkReply(wrapEncodingType("embstr", cv).getBytes());
            } else {
                return new BulkReply(wrapEncodingType("unknown", cv).getBytes());
            }
        } else if ("log".equals(subCmd)) {
            final String prefix = "DEBUG LOG:";
            log.info("{} {}", prefix, new String(data[2]));
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }

    private Reply del() {
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
                var isRemoved = remove(s.slot(), s.bucketIndex(), new String(keyBytes), s.keyHash(), s.keyHash32());
                if (isRemoved) {
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
                    var isRemoved = remove(oneSlot.slot(), s.bucketIndex(), s.rawKey(), s.keyHash(), s.keyHash32());
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
                log.error("del error={}", e.getMessage());
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

    private Reply dbsize() {
        // skip
        if (data.length == 2) {
            return ErrorReply.FORMAT;
        }

        return localPersist.doSthInSlots(OneSlot::getAllKeyCount, resultList -> {
            long n = resultList.stream().mapToLong(Long::valueOf).sum();
            return new IntegerReply(n);
        });
    }

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
            var newValue = BigDecimal.valueOf(doubleValue).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP)
                    .subtract(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));

            setNumber(keyBytes, newValue.doubleValue(), slotWithKeyHash);
            return new DoubleReply(newValue);
        } else {
            long newValue = longValue - by;
            setNumber(keyBytes, newValue, slotWithKeyHash);
            return new IntegerReply(newValue);
        }
    }

    boolean isUseHH(byte[] keyBytes) {
        int checkPrefixLength = RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX.length;
        if (keyBytes.length > checkPrefixLength) {
            // check prefix match
            if (Arrays.equals(keyBytes, 0, checkPrefixLength,
                    RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX, 0, checkPrefixLength)) {
                return false;
            }
        }
        return localPersist.getIsHashSaveMemberTogether();
    }

    private Reply dump() {
        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return NilReply.INSTANCE;
        }

        if (cv.isTypeString()) {
            var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
            var dumpBytes = VeloRDBImporter.dumpString(valueBytes);
            return new BulkReply(dumpBytes);
        } else if (cv.isSet()) {
            var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
            var rhk = RedisHashKeys.decode(valueBytes);
            if (rhk.size() == 0) {
                return NilReply.INSTANCE;
            }

            var dumpBytes = VeloRDBImporter.dumpSet(rhk);
            return new BulkReply(dumpBytes);
        } else if (cv.isHash()) {
            if (isUseHH(keyBytes)) {
                var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
                var rhh = RedisHH.decode(valueBytes);
                if (rhh.size() == 0) {
                    return NilReply.INSTANCE;
                }

                var dumpBytes = VeloRDBImporter.dumpHash(rhh);
                return new BulkReply(dumpBytes);
            } else {
                var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
                var rhk = RedisHashKeys.decode(valueBytes);
                if (rhk.getSet().isEmpty()) {
                    return NilReply.INSTANCE;
                }

                var rhh = new RedisHH();
                var key = new String(keyBytes);
                for (var field : rhk.getSet()) {
                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    var sFieldKey = slot(fieldKey.getBytes());
                    var fieldCv = getCv(fieldKey.getBytes(), sFieldKey);
                    var fieldValueBytes = getValueBytesByCv(fieldCv, fieldKey.getBytes(), sFieldKey);
                    rhh.put(field, fieldValueBytes);
                }

                var dumpBytes = VeloRDBImporter.dumpHash(rhh);
                return new BulkReply(dumpBytes);
            }
        } else if (cv.isList()) {
            var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
            var rl = RedisList.decode(valueBytes);
            if (rl.size() == 0) {
                return NilReply.INSTANCE;
            }

            var dumpBytes = VeloRDBImporter.dumpList(rl);
            return new BulkReply(dumpBytes);
        } else if (cv.isZSet()) {
            var valueBytes = getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
            var rz = RedisZSet.decode(valueBytes);
            if (rz.isEmpty()) {
                return NilReply.INSTANCE;
            }

            var dumpBytes = VeloRDBImporter.dumpZSet(rz);
            return new BulkReply(dumpBytes);
        } else {
            return ErrorReply.DUMP_TYPE_NOT_SUPPORT;
        }
    }
}
