package io.velo.command;

import com.github.luben.zstd.Zstd;
import com.github.prasanthj.hll.HyperLogLog;
import com.github.prasanthj.hll.HyperLogLogUtils;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.persist.LocalPersist;
import io.velo.reply.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

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
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pexpiretime".equals(cmd) || "pttl".equals(cmd) || "persist".equals(cmd)) {
            if (data.length != 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pfadd".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("pfcount".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("pfmerge".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
            return slotWithKeyHashList;
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("persist".equals(cmd)) {
            return persist();
        }

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

        if ("pfadd".equals(cmd)) {
            return pfadd();
        }

        if ("pfcount".equals(cmd)) {
            return pfcount();
        }

        if ("pfmerge".equals(cmd)) {
            return pfmerge();
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

        if ("psubscribe".equals(cmd)) {
            var sGroup = new SGroup(cmd, data, socket);
            sGroup.from(this);
            return sGroup.subscribe(true);
        }

        if ("publish".equals(cmd)) {
            return publish(data, socket);
        }

        if ("pubsub".equals(cmd)) {
            return pubsub();
        }

        if ("punsubscribe".equals(cmd)) {
            var uGroup = new UGroup(cmd, data, socket);
            uGroup.from(this);
            return uGroup.unsubscribe(true);
        }

        return NilReply.INSTANCE;
    }

    private Reply persist() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var s = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(s);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        if (cv.getExpireAt() == CompressedValue.NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }

        cv.setExpireAt(CompressedValue.NO_EXPIRE);
        putToOneSlot(s.slot(), s, cv);
        return IntegerReply.REPLY_1;
    }

    private void saveHll(SlotWithKeyHash s, HyperLogLog hll) throws IOException {
        var os = new ByteArrayOutputStream();
        HyperLogLogUtils.serializeHLL(os, hll);
        var encoded = os.toByteArray();

        var beginT = System.nanoTime();
        var compressed = Zstd.compress(encoded);
        var costT = (System.nanoTime() - beginT) / 1000;

        var cv = new CompressedValue();
        cv.setSeq(snowFlake.nextId());
        cv.setDictSeqOrSpType(CompressedValue.SP_TYPE_HLL);
        cv.setKeyHash(s.keyHash());
        cv.setCompressedData(compressed);
        putToOneSlot(s.slot(), s, cv);

        // stats
        compressStats.rawTotalLength += encoded.length;
        compressStats.compressedCount++;
        compressStats.compressedTotalLength += compressed.length;
        compressStats.compressedCostTimeTotalUs += costT;
    }

    private HyperLogLog getHll(SlotWithKeyHash s) {
        var cv = getCv(s);
        if (cv == null) {
            return null;
        }

        if (!cv.isHll()) {
            throw new RuntimeException(ErrorReply.WRONG_TYPE.getMessage());
        }

        var beginT = System.nanoTime();
        var decompressed = cv.decompress(Dict.SELF_ZSTD_DICT);
        var costT = System.nanoTime() - beginT;

        // stats
        compressStats.decompressedCount++;
        compressStats.decompressedCostTimeTotalNs += costT;

        var is = new ByteArrayInputStream(decompressed);
        try {
            return HyperLogLogUtils.deserializeHLL(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Reply pfadd() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var s = slotWithKeyHashListParsed.getFirst();

        var hll = getHll(s);
        if (hll == null) {
            hll = HyperLogLog.builder()
                    .setEncoding(HyperLogLog.EncodingType.DENSE)
                    .build();
        }

        var oldCount = hll.count();
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            hll.addBytes(itemBytes);
        }
        var newCount = hll.count();
        var isChanged = oldCount != newCount;

        if (isChanged) {
            try {
                saveHll(s, hll);
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        return isChanged ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    private long getHllCount(SlotWithKeyHash s) {
        var hll = getHll(s);
        if (hll == null) {
            return 0;
        }
        return hll.count();
    }

    private Reply pfcount() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            long sum = 0L;
            for (var i = 1; i < data.length; i++) {
                var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
                var count = getHllCount(slotWithKeyHash);
                sum += count;
            }
            return new IntegerReply(sum);
        }

        ArrayList<MGroup.IndexAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new MGroup.IndexAndSlotWithKeyHash(j, slotWithKeyHash));
        }

        ArrayList<Promise<Long>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                long sum = 0L;
                for (var one : subList) {
                    var count = getHllCount(one.slotWithKeyHash());
                    sum += count;
                }
                return sum;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("pfcount error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            long sum = 0L;
            for (var p : promises) {
                sum += p.getResult();
            }
            finalPromise.set(new IntegerReply(sum));
        });

        return asyncReply;
    }

    private Reply pfmerge() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var dstS = slotWithKeyHashListParsed.getFirst();

        if (!isCrossRequestWorker) {
            var dstHll = HyperLogLog.builder()
                    .setEncoding(HyperLogLog.EncodingType.DENSE)
                    .build();
            for (var i = 2; i < data.length; i++) {
                var srcS = slotWithKeyHashListParsed.get(i - 1);
                var hll = getHll(srcS);
                dstHll.merge(hll);
            }

            try {
                saveHll(dstS, dstHll);
                return OKReply.INSTANCE;
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        ArrayList<MGroup.IndexAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 2, j = 0; i < data.length; i++, j++) {
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
            list.add(new MGroup.IndexAndSlotWithKeyHash(j, slotWithKeyHash));
        }

        ArrayList<Promise<ArrayList<HyperLogLog>>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<HyperLogLog> hllList = new ArrayList<>();
                for (var one : subList) {
                    var hll = getHll(one.slotWithKeyHash());
                    hllList.add(hll);
                }
                return hllList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("pfmerge error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            var dstHll = HyperLogLog.builder()
                    .setEncoding(HyperLogLog.EncodingType.DENSE)
                    .build();
            for (var p : promises) {
                var hllList = p.getResult();
                for (var hll : hllList) {
                    if (hll != null) {
                        dstHll.merge(hll);
                    }
                }
            }
            try {
                saveHll(dstS, dstHll);
                finalPromise.set(OKReply.INSTANCE);
            } catch (IOException e1) {
                finalPromise.setException(e1);
            }
        });

        return asyncReply;
    }

    private static final BulkReply MESSAGE = new BulkReply("message".getBytes());

    public static Reply publish(byte[][] dataGiven, ITcpSocket socket) {
        if (dataGiven.length != 3) {
            return ErrorReply.FORMAT;
        }

        var localPersist = LocalPersist.getInstance();
        var socketInInspector = localPersist.getSocketInspector();

        var channel = new String(dataGiven[1]);
        var message = new String(dataGiven[2]);

        if (socket != null) {
            // check acl
            var u = BaseCommand.getAuthU(socket);
            if (!u.isOn() || !u.checkChannels(channel)) {
                return ErrorReply.ACL_PERMIT_LIMIT;
            }
        }

        var replies = new Reply[3];
        replies[0] = MESSAGE;
        replies[1] = new BulkReply(channel.getBytes());
        replies[2] = new BulkReply(message.getBytes());

        var n = socketInInspector.subscribeSocketCount(channel, false);

        socketInInspector.publish(channel, new MultiBulkReply(replies), (s, r) -> {
            s.write(r.buffer());
        });
        return new IntegerReply(n);
    }

    private Reply pubsub() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var socketInspector = localPersist.getSocketInspector();

        var subCmd = new String(data[1]).toLowerCase();
        if ("channels".equals(subCmd)) {
            var pattern = data.length == 3 ? new String(data[2]) : null;
            var ones = pattern == null ? socketInspector.allSubscribeOnes() :
                    socketInspector.filterSubscribeOnesByChannel(pattern, true);
            if (ones.isEmpty()) {
                return MultiBulkReply.EMPTY;
            } else {
                var replies = new Reply[ones.size()];
                for (int i = 0; i < ones.size(); i++) {
                    replies[i] = new BulkReply(ones.get(i).getChannel().getBytes());
                }
                return new MultiBulkReply(replies);
            }
        } else if ("numpat".equals(subCmd)) {
            return NilReply.INSTANCE;
        } else if ("numsub".equals(subCmd)) {
            if (data.length < 3) {
                return ErrorReply.FORMAT;
            }

            var channels = new ArrayList<String>();
            for (int i = 2; i < data.length; i++) {
                channels.add(new String(data[i]));
            }

            var replies = new Reply[channels.size() * 2];
            int j = 0;
            for (var channel : channels) {
                var size = socketInspector.subscribeSocketCount(channel, true);
                replies[j++] = new BulkReply(channel.getBytes());
                replies[j++] = new IntegerReply(size);
            }
            return new MultiBulkReply(replies);
        } else {
            return NilReply.INSTANCE;
        }
    }
}
