package io.velo.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.decode.Request;
import io.velo.persist.*;
import io.velo.repl.*;
import io.velo.repl.content.*;
import io.velo.repl.incremental.XSkipApply;
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.BulkReply;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;
import io.velo.type.RedisHashKeys;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static io.velo.repl.ReplType.*;

/**
 * Handles Velo master-slave replications.
 */
public class XGroup extends BaseCommand {
    public XGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public XGroup(String cmd, byte[][] data, ITcpSocket socket, RequestHandler requestHandler, Request request) {
        super(cmd, data, socket);
        super.init(requestHandler);
        this.slotWithKeyHashListParsed = request.getSlotWithKeyHashList();
        this.isCrossRequestWorker = request.isCrossRequestWorker();
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        // x_repl sub_cmd
        if (data.length < 2) {
            return slotWithKeyHashList;
        }

        var isHasSlotBytes = "slot".equals(new String(data[1]));
        // repl request without slot
        if (!isHasSlotBytes) {
            return slotWithKeyHashList;
        }

        if (data.length < 4) {
            return slotWithKeyHashList;
        }

        // x_repl slot 0 ***
        var slotBytes = data[2];
        short slot;
        try {
            slot = Byte.parseByte(new String(slotBytes));
        } catch (NumberFormatException ignored) {
            return slotWithKeyHashList;
        }

        if (slot >= slotNumber) {
            slot = (short) (slot % slotNumber);
        }

        slotWithKeyHashList.add(new SlotWithKeyHash(slot, 0, 0L, 0));
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if (data.length < 2) {
            return NilReply.INSTANCE;
        }

        var subCmd = new String(data[1]);

        if (X_CONF_FOR_SLOT_AS_SUB_CMD.equals(subCmd)) {
            // get x_repl x_conf_for_slot
            var checkValuesFromMaster = ConfForSlot.global.getSlaveCheckValues();
            var objectMapper = new ObjectMapper();
            try {
                var jsonStr = objectMapper.writeValueAsString(checkValuesFromMaster);
                return new BulkReply(jsonStr);
            } catch (JsonProcessingException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        // has slot
        // data.length >= 4, subCmd is 'slot'
        // x_repl slot 0 ***
        if (slotWithKeyHashListParsed.isEmpty()) {
            return ErrorReply.SYNTAX;
        }

        var subCmd2 = new String(data[3]);

        if (X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD.equals(subCmd2)) {
            // x_repl slot 0 get_first_slave_listen_address
            var slot = slotWithKeyHashListParsed.getFirst().slot();
            var oneSlot = localPersist.oneSlot(slot);
            var replPairAsMaster = oneSlot.getFirstReplPairAsMaster();

            if (replPairAsMaster == null) {
                return NilReply.INSTANCE;
            } else {
                return new BulkReply(replPairAsMaster.getHostAndPort());
            }
        }

        if (X_CATCH_UP_AS_SUB_CMD.equals(subCmd2)) {
            // x_repl slot 0 x_catch_up long long int long long
            if (data.length != 9) {
                return ErrorReply.SYNTAX;
            }

            var slot = slotWithKeyHashListParsed.getFirst().slot();
            var oneSlot = localPersist.oneSlot(slot);
            var replPairAsMaster = oneSlot.getFirstReplPairAsMaster();

            if (replPairAsMaster == null) {
                log.warn("Repl master repl pair not found, maybe already closed, slot={}", slot);
                // just mock one
                var slaveUuid = Long.parseLong(new String(data[4]));
                replPairAsMaster = new ReplPair(slot, true, "localhost", 7379);
                replPairAsMaster.setSlaveUuid(slaveUuid);
            }

            var lastUpdatedMasterUuid = Long.parseLong(new String(data[5]));
            var lastUpdatedFileIndex = Integer.parseInt(new String(data[6]));
            var marginLastUpdatedOffset = Long.parseLong(new String(data[7]));
            var lastUpdatedOffset = Long.parseLong(new String(data[8]));

            var contentBytes = toMasterCatchUpRequestBytes(lastUpdatedMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);

            this.replPair = replPairAsMaster;
            try {
                var reply = catch_up(slot, contentBytes);
                var nettyByteBuf = Unpooled.wrappedBuffer(reply.buffer().array());
                var replRequest1 = Repl.decode(nettyByteBuf);
                if (replRequest1 == null) {
                    return NilReply.INSTANCE;
                }

                assert replRequest1.isFullyRead();
                if (reply.isReplType(error)) {
                    return new ErrorReply(new String(replRequest1.getData()));
                }

                return new BulkReply(replRequest1.getData());
            } catch (Exception e) {
                log.error("Repl master handle x_repl x_catch_up error, slot={}", slot, e);
                return new ErrorReply(e.getMessage());
            }
        }

        return NilReply.INSTANCE;
    }

    public static final String X_CATCH_UP_AS_SUB_CMD = "x_catch_up";
    public static final String X_CONF_FOR_SLOT_AS_SUB_CMD = "x_conf_for_slot";
    // like redis, use 'info replication', todo
    public static final String X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD = "x_get_first_slave_listen_address";
    public static final String X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH = "x_repl";

    // for publish
    public static final byte[] X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES = "+switch-master".getBytes();
//    public static final String X_MASTER_RESET_PUBLISH_CHANNEL = "+reset-master";

    public void setReplPair(ReplPair replPair) {
        this.replPair = replPair;
    }

    private ReplPair replPair;

    public Reply handleRepl(@NotNull ReplRequest replRequest) {
        var slaveUuid = replRequest.getSlaveUuid();
        var slotRemote = replRequest.getSlot();
        var replType = replRequest.getType();

        if (slotRemote >= ConfForGlobal.slotNumber) {
            log.warn("Repl do nothing for slot remote={}, repl type={}", slotRemote, replType);
            return Repl.emptyReply();
        }

        try {
            return handleReplInner(slotRemote, replType, slaveUuid, replRequest.getData());
        } catch (Exception e) {
            log.error("Repl handle error, slot remote={}, repl type={}", slotRemote, replType, e);
            return Repl.error(slotRemote, slaveUuid, e.getMessage());
        }
    }

    private Reply handleReplInner(short slot, ReplType replType, long slaveUuid, byte[] contentBytes) {
        var oneSlot = localPersist.oneSlot(slot);
        if (this.replPair == null) {
            if (replType.isSlaveSend) {
                this.replPair = oneSlot.getReplPairAsMaster(slaveUuid);
            } else {
                // slave uuid == self one slot master uuid
                this.replPair = oneSlot.getReplPairAsSlave(slaveUuid);

                if (this.replPair == null) {
                    if (replType != error) {
                        log.warn("Repl handle error: repl pair as slave not found, maybe closed already, slave uuid={}, repl type={}, slot={}",
                                slaveUuid, replType, slot);
                        return Repl.emptyReply();
                    }
                }
            }
        }

        if (replPair != null) {
            replPair.increaseStatsCountForReplType(replType);
            replPair.increaseFetchedBytesLength(contentBytes.length);

            // for proactively close slave connection in master
            if (replPair.isAsMaster()) {
                replPair.setSlaveConnectSocketInMaster((TcpSocket) socket);
            }
        }

        return switch (replType) {
            case error -> {
                log.error("Repl handle receive error={}, slot={}", new String(contentBytes), slot);
                yield Repl.emptyReply();
            }
            case ping -> {
                // server received ping from client
                var netListenAddresses = new String(contentBytes);
                var array = netListenAddresses.split(":");
                var host = array[0];
                var port = Integer.parseInt(array[1]);

                if (replPair == null) {
                    replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
                    replPair.increaseStatsCountForReplType(ping);
                }

                replPair.setLastPingGetTimestamp(System.currentTimeMillis());
                yield Repl.reply(slot, replPair, pong, new Pong(ConfForGlobal.netListenAddresses));
            }
            case pong -> {
                // client received pong from server
                assert replPair != null;
                replPair.setLastPongGetTimestamp(System.currentTimeMillis());

                var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
                if (metaChunkSegmentIndex.isExistsDataAllFetched()) {
                    var millis = replPair.getLastGetCatchUpResponseMillis();
                    // if 5s
                    if (millis != 0 && System.currentTimeMillis() - millis > 1000 * 5) {
                        // trigger catch up
                        // content bytes length == 2 means do not reset master readonly flag
                        s_catch_up(slot, new byte[2]);
                    }
                }

                yield Repl.emptyReply();
            }
            case hello -> hello(slot, contentBytes);
            case hi -> hi(slot, contentBytes);
            case test -> {
                log.info("Repl handle test: slave uuid={}, message={}, slot={}", slaveUuid, new String(contentBytes), slot);
                yield Repl.emptyReply();
            }
            case bye -> {
                // server received bye from client
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl master handle bye: slave uuid={}, net listen addresses={}, slot={}", slaveUuid, netListenAddresses, slot);

                if (replPair == null) {
                    yield Repl.emptyReply();
                }

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.reply(slot, replPair, byeBye, new Pong(ConfForGlobal.netListenAddresses));
            }
            case byeBye -> {
                // client received bye from server
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl slave handle bye bye: slave uuid={}, net listen addresses={}, slot={}", slaveUuid, netListenAddresses, slot);

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.emptyReply();
            }
            case exists_wal -> exists_wal(slot, contentBytes);
            case exists_chunk_segments -> exists_chunk_segments(slot, contentBytes);
            case exists_big_string -> exists_big_string(slot, contentBytes);
            case exists_short_string -> exists_short_string(slot, contentBytes);
            case incremental_big_string -> incremental_big_string(slot, contentBytes);
            case exists_dict -> exists_dict(slot, contentBytes);
            case exists_all_done -> exists_all_done(slot, contentBytes);
            case catch_up -> catch_up(slot, contentBytes);
            case s_exists_wal -> s_exists_wal(slot, contentBytes);
            case s_exists_chunk_segments -> s_exists_chunk_segments(slot, contentBytes);
            case s_exists_big_string -> s_exists_big_string(slot, contentBytes);
            case s_exists_short_string -> s_exists_short_string(slot, contentBytes);
            case s_incremental_big_string -> s_incremental_big_string(slot, contentBytes);
            case s_exists_dict -> s_exists_dict(slot, contentBytes);
            case s_exists_all_done -> s_exists_all_done(slot, contentBytes);
            case s_catch_up -> s_catch_up(slot, contentBytes);
        };
    }

    private Repl.ReplReply hello(short slot, byte[] contentBytes) {
        // server received hello from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();

        var len = buffer.getInt();
        var b = new byte[len];
        buffer.get(b);
        var netListenAddresses = new String(b);

        // ignore remote slot number
        var slaveSlotNumber = buffer.getShort();
        var replProperties = new ConfForSlot.ReplProperties(buffer.getInt(), buffer.getInt(),
                buffer.getInt(), buffer.get(), buffer.getInt(), buffer.get() == 1);

        var array = netListenAddresses.split(":");
        var host = array[0];
        var port = Integer.parseInt(array[1]);

        var oneSlot = localPersist.oneSlot(slot);
        if (replPair == null) {
            replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
            replPair.increaseStatsCountForReplType(hello);
        }

        log.warn("Repl master handle hello: slave uuid={}, net listen addresses={}, slot={}", slaveUuid, netListenAddresses, slot);

        replPair.setRemoteReplProperties(replProperties);

        // start binlog
        var binlog = oneSlot.getBinlog();
        try {
            oneSlot.getDynConfig().setBinlogOn(true);
            log.warn("Repl master start binlog, master uuid={}, slot={}", replPair.getMasterUuid(), slot);

            var currentFo = binlog.currentFileIndexAndOffset();
            var earliestFo = binlog.earliestFileIndexAndOffset();

            var chunk = oneSlot.getChunk();
            var content = new Hi(slaveUuid, oneSlot.getMasterUuid(), currentFo, earliestFo, chunk.getSegmentIndex());

            // append a skip apply, so offset will not be 0, so the migrate tool or failover manager is easier to check if slave is all caught up
            var xSkipApply = new XSkipApply(oneSlot.getSnowFlake().nextId());
            binlog.append(xSkipApply);
            return Repl.reply(slot, replPair, hi, content);
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: start binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
        }
    }

    private static RawBytesContent toMasterCatchUp(long binlogMasterUuid, int lastUpdatedFileIndex, long marginLastUpdatedOffset, long lastUpdatedOffset) {
        return new RawBytesContent(toMasterCatchUpRequestBytes(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset));
    }

    private static byte[] toMasterCatchUpRequestBytes(long binlogMasterUuid, int lastUpdatedFileIndex, long marginLastUpdatedOffset, long lastUpdatedOffset) {
        var requestBytes = new byte[8 + 4 + 8 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.putLong(binlogMasterUuid);
        requestBuffer.putInt(lastUpdatedFileIndex);
        requestBuffer.putLong(marginLastUpdatedOffset);
        requestBuffer.putLong(lastUpdatedOffset);
        return requestBytes;
    }

    private interface FillBytes {
        void fillBytes(ByteBuffer buffer);
    }

    private RawBytesContent fillBytes(int size, FillBytes fillBytes) {
        var bytes = new byte[size];
        var buffer = ByteBuffer.wrap(bytes);
        fillBytes.fillBytes(buffer);
        return new RawBytesContent(bytes);
    }

    private Repl.ReplReply hi(short slot, byte[] contentBytes) {
        // client received hi from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();
        var masterUuid = buffer.getLong();
        // master binlog current (latest) file index and offset
        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();
        // master binlog earliest (not deleted yet) file index and offset
        var earliestFileIndex = buffer.getInt();
        var earliestOffset = buffer.getLong();
        var currentSegmentIndex = buffer.getInt();

        var masterSlotNumber = buffer.getShort();
        var replProperties = new ConfForSlot.ReplProperties(buffer.getInt(), buffer.getInt(),
                buffer.getInt(), buffer.get(), buffer.getInt(), buffer.get() == 1);

        // should not happen
        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl slave handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}, slot={}",
                    replPair.getSlaveUuid(), slaveUuid, slot);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl slave handle hi: slave uuid={}, master uuid={}, slot={}", slaveUuid, masterUuid, slot);

        replPair.setRemoteReplProperties(replProperties);

        if (slot == localPersist.firstOneSlot().slot()) {
            localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(false);
        }

        var oneSlot = localPersist.oneSlot(slot);
        log.warn("Repl slave set meta chunk segment index, segment index={}, slot={}", currentSegmentIndex, slot);

        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var lastUpdatedMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var isExistsDataAllFetched = metaChunkSegmentIndex.isExistsDataAllFetched();
        if (lastUpdatedMasterUuid == masterUuid && isExistsDataAllFetched) {
            // last updated means next batch, but not fetch yet, refer end of method s_cache_up
            var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
            var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
            var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

            if (lastUpdatedFileIndex >= earliestFileIndex && lastUpdatedOffset >= earliestOffset) {
                // need not fetch exists data from master
                // start fetch incremental data from master binlog
                log.warn("Repl slave skip fetch exists data and start catch up from master binlog, " +
                                "slave uuid={}, master uuid={}, last updated file index={}, offset={}, slot={}",
                        slaveUuid, masterUuid, lastUpdatedFileIndex, lastUpdatedOffset, slot);

                // not catch up any binlog segment yet, start from the beginning
                if (lastUpdatedFileIndex == 0 && lastUpdatedOffset == 0) {
                    var content = toMasterCatchUp(masterUuid, 0, 0L, 0L);
                    return Repl.reply(slot, replPair, catch_up, content);
                }

                // last fetched binlog segment is not a complete segment, need to re-fetch this segment
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                if (marginLastUpdatedOffset != lastUpdatedOffset) {
                    var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                    return Repl.reply(slot, replPair, catch_up, content);
                }

                var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, lastUpdatedOffset, lastUpdatedOffset);
                return Repl.reply(slot, replPair, catch_up, content);
            }
        }

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false,
                currentFileIndex, currentOffset);
        log.warn("Repl slave set master binlog current/latest file index and offset for incremental catch up, master binlog file index={}, offset={}, slot={}",
                currentFileIndex, currentOffset, slot);
        log.warn("Repl slave begin fetch all exists data from master, slot={}", slot);

        // dict is global, only first slot do fetch
        var firstSlot = localPersist.firstOneSlot();
        if (firstSlot.slot() != slot) {
            log.warn("Repl slave skip fetch exists dict, slot={}", slot);
            return fetchExistsBigString(slot, 0);
        }

        // begin to fetch exist data from master
        // first fetch dict
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        if (cacheDictBySeqCopy.isEmpty()) {
            return Repl.reply(slot, replPair, exists_dict, NextStepContent.INSTANCE);
        } else {
            var content = fillBytes(4 * cacheDictBySeqCopy.size(), buf -> {
                for (var entry : cacheDictBySeqCopy.entrySet()) {
                    var seq = entry.getKey();
                    buf.putInt(seq);
                }
            });

            return Repl.reply(slot, replPair, exists_dict, content);
        }
    }

    private Repl.ReplReply exists_wal(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var walGroupIndex = buffer.getInt();

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (walGroupIndex < 0 || walGroupIndex >= walGroupNumber) {
            log.error("Repl master send wal exists bytes error: group index out of range, slot={}, group index={}",
                    slot, walGroupIndex);
            return Repl.error(slot, replPair, "Repl master send wal exists bytes error: group index out of range");
        }

        if (slot == 0 && walGroupIndex % 1024 == 0) {
            log.warn("Repl master will fetch exists wal, group index={}, slot={}", walGroupIndex, slot);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByGroupIndex(walGroupIndex);

        try {
            var toSlaveBytes = targetWal.toSlaveExistsOneWalGroupBytes();
            if (toSlaveBytes == null) {
                if (slot == 0 && walGroupIndex % 1024 == 0) {
                    log.warn("Repl master will skip exists wal, group index={}, slot={}", walGroupIndex, slot);
                }

                // no data
                var content = fillBytes(4, buf -> buf.putInt(walGroupIndex));
                return Repl.reply(slot, replPair, s_exists_wal, content);
            } else {
                if (slot == 0 && walGroupIndex % 1024 == 0) {
                    log.warn("Repl master fetch exists wal, group index={}, slot={}, length={}", walGroupIndex, slot, toSlaveBytes.length);
                }

                return Repl.reply(slot, replPair, s_exists_wal, new RawBytesContent(toSlaveBytes));
            }
        } catch (IOException e) {
            log.error("Repl master get wal exists bytes error, group index={}, slot={}", walGroupIndex, slot, e);
            return Repl.error(slot, replPair, "Repl master get wal exists bytes error=" + e.getMessage());
        }
    }

    private Repl.ReplReply s_exists_wal(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        // remote group index, ignore
        var walGroupIndex = buffer.getInt();

        if (slot == 0 && walGroupIndex % 1024 == 0) {
            log.info("Repl slave update wal exists bytes, group index={}, slot={}, length={}", walGroupIndex, slot, contentBytes.length);
        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        if (buffer.hasRemaining()) {
            // use any wal is ok
            var firstWal = firstOneSlot.getWalByGroupIndex(0);
            try {
                HashMap<Short, HashMap<String, Wal.V>> groupedBySlot = new HashMap<>();
                HashMap<Short, HashMap<String, Wal.V>> groupedShortValueBySlot = new HashMap<>();
                firstWal.fromMasterExistsOneWalGroupBytes(contentBytes, ((isShortValue, key, v) -> {
                    var keyHash = KeyHash.hash(key.getBytes());
                    var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
                    if (isShortValue) {
                        groupedShortValueBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, v);
                    } else {
                        groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, v);
                    }
                }));

                putToTargetWal(false, groupedBySlot);
                putToTargetWal(true, groupedShortValueBySlot);
            } catch (IOException e) {
                log.error("Repl slave update wal exists bytes error, group index={}, slot={}", walGroupIndex, slot, e);
                throw new RuntimeException("Repl slave update wal exists bytes error, group index=" + walGroupIndex + ", slot=" + slot + ", e=" + e.getMessage());
            }
        }

        var remoteReplProperties = replPair.getRemoteReplProperties();
        var walGroupNumberRemote = remoteReplProperties.bucketsPerSlot() / remoteReplProperties.oneChargeBucketNumber();

        if (walGroupIndex == walGroupNumberRemote - 1) {
            log.warn("Repl slave fetch exists all done after fetch wal when slot is not the first slot, slot={}", slot);
            return Repl.reply(slot, replPair, exists_all_done, NextStepContent.INSTANCE);
        } else {
            var nextGroupIndex = walGroupIndex + 1;
            if (nextGroupIndex % 1024 == 0) {
                // delay
                firstOneSlot.delayRun(1000, () -> {
                    replPair.write(exists_wal, fillBytes(4, buf -> buf.putInt(nextGroupIndex)));
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_wal, fillBytes(4, buf -> buf.putInt(nextGroupIndex)));
            }
        }
    }

    private void putToTargetWal(boolean isShortValue, HashMap<Short, HashMap<String, Wal.V>> groupedBySlot) {
        if (groupedBySlot.isEmpty()) {
            return;
        }

        for (var entry : groupedBySlot.entrySet()) {
            var slotInner = entry.getKey();
            var oneSlot = localPersist.oneSlot(slotInner);
            oneSlot.asyncExecute(() -> {
                for (var entry2 : entry.getValue().entrySet()) {
                    var key = entry2.getKey();
                    var v = entry2.getValue();

                    var bucketIndex = KeyHash.bucketIndex(v.keyHash());
                    var targetWal = oneSlot.getWalByBucketIndex(bucketIndex);
                    var putResult = targetWal.put(isShortValue, key, v);
                    if (putResult.needPersist()) {
                        oneSlot.doPersist(targetWal.getGroupIndex(), key, putResult);
                    }
                }
            });
        }
    }

    private Repl.ReplReply exists_chunk_segments(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();

        if (slot == 0 && beginSegmentIndex % (1024 * 100) == 0) {
            log.warn("Repl master fetch exists chunk segments, begin segment index={}, slot={}",
                    beginSegmentIndex, slot);
        }

        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var segmentBatchCount = ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl;

        var oneSlot = localPersist.oneSlot(slot);
        var chunkSegmentsBytes = oneSlot.readForRepl(beginSegmentIndex);
        if (chunkSegmentsBytes == null) {
            var content = fillBytes(4 + 4 + 4 + 4, buf -> {
                buf.putInt(beginSegmentIndex);
                buf.putInt(0);
                buf.putInt(segmentBatchCount);
                buf.putInt(segmentLength);
            });
            return Repl.reply(slot, replPair, s_exists_chunk_segments, content);
        }

        var segmentCount = chunkSegmentsBytes.length / segmentLength;
        // begin segment index, segment count, segment length, segment bytes
        var content = fillBytes(4 + 4 + 4 + 4 + chunkSegmentsBytes.length, buf -> {
            buf.putInt(beginSegmentIndex);
            buf.putInt(segmentCount);
            buf.putInt(segmentBatchCount);
            buf.putInt(segmentLength);
            buf.put(chunkSegmentsBytes);
        });

        return Repl.reply(slot, replPair, s_exists_chunk_segments, content);
    }

    private Repl.ReplReply s_exists_chunk_segments(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();
        var segmentBatchCount = buffer.getInt();
        var segmentLength = buffer.getInt();

        var remoteReplProperties = replPair.getRemoteReplProperties();
        assert segmentLength == remoteReplProperties.segmentLength();

        if (slot == 0 && beginSegmentIndex % (1024 * 100) == 0) {
            log.warn("Repl slave ready to fetch exists chunk segments, begin segment index={}, segment count={}, slot={}",
                    beginSegmentIndex, segmentCount, slot);
        }

        HashMap<Short, HashMap<String, CompressedValue>> groupedBySlot = new HashMap<>();
        for (int i = 0; i < segmentCount; i++) {
            if (!buffer.hasRemaining()) {
                break;
            }

            var offset = buffer.position();

            if (SegmentBatch2.isSegmentBytesTight(buffer.array(), offset)) {
//                assert remoteReplProperties.isSegmentUseCompression();

                var buffer2 = ByteBuffer.wrap(buffer.array(), offset, segmentLength).slice();
                // iterate sub blocks, refer to SegmentBatch.tight
                for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
                    buffer2.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
                    var subBlockOffset = buffer2.getShort();
                    if (subBlockOffset == 0) {
                        // skip
                        break;
                    }
                    var subBlockLength = buffer2.getShort();

                    var decompressedBytes = new byte[segmentLength];
                    var d = Zstd.decompressByteArray(decompressedBytes, 0, segmentLength,
                            buffer.array(), offset + subBlockOffset, subBlockLength);
                    if (d != segmentLength) {
                        var segmentIndex = beginSegmentIndex + i;
                        throw new IllegalStateException("Decompress error, s=" + slot
                                + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + segmentLength);
                    }

                    SegmentBatch2.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                        if (cv.isExpired()) {
                            return;
                        }

                        var keyHash = KeyHash.hash(key.getBytes());
                        var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
                        groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, cv);
                    });
                }
            } else {
                SegmentBatch2.iterateFromSegmentBytes(buffer.array(), offset, segmentLength, (key, cv, offsetInThisSegment) -> {
                    if (cv.isExpired()) {
                        return;
                    }

                    var keyHash = KeyHash.hash(key.getBytes());
                    var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
                    groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, cv);
                });
            }

            buffer.position(offset + segmentLength);
        }

        for (var entry : groupedBySlot.entrySet()) {
            var slotInner = entry.getKey();
            var oneSlot = localPersist.oneSlot(slotInner);
            oneSlot.asyncExecute(() -> {
                for (var entry2 : entry.getValue().entrySet()) {
                    var key = entry2.getKey();
                    var cv = entry2.getValue();
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash());
                    oneSlot.put(key, bucketIndex, cv);
                }
            });
        }

        var maxSegmentNumber = remoteReplProperties.segmentNumberPerFd() * remoteReplProperties.fdPerChunk();
        boolean isLastBatch = maxSegmentNumber == beginSegmentIndex + segmentBatchCount;
        if (isLastBatch) {
            // next step, fetch exists wal
            log.warn("Repl slave fetch data, go to step={}, slot={}", exists_wal.name(), slot);
            return Repl.reply(slot, replPair, exists_wal, new RawBytesContent(new byte[4]));
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentBatchCount;
            var content = fillBytes(4, buf -> {
                buf.putInt(nextBatchBeginSegmentIndex);
            });

            var oneSlot = localPersist.oneSlot(slot);
            if (nextBatchBeginSegmentIndex % (64 * 1024) == 0) {
                oneSlot.delayRun(100, () -> {
                    replPair.write(exists_chunk_segments, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_chunk_segments, content);
            }
        }
    }

    private Repl.ReplReply incremental_big_string(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        var kenLength = buffer.getInt();
        var keyBytes = new byte[kenLength];
        buffer.get(keyBytes);
        var key = new String(keyBytes);

        log.warn("Repl master fetch incremental big string, uuid={}, key={}, slot={}", uuid, key, slot);

        var s = slot(key);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringFiles = oneSlot.getBigStringFiles();
        var bigStringBytes = bigStringFiles.getBigStringBytes(uuid, s.bucketIndex(), s.keyHash());
        if (bigStringBytes == null) {
            log.warn("Repl master fetch incremental big string, uuid={}, key={}, slot={}, big string bytes is null", uuid, key, slot);
            bigStringBytes = new byte[0];
        }

        byte[] finalBigStringBytes = bigStringBytes;
        var content = fillBytes(8 + 4 + kenLength + bigStringBytes.length, buf -> {
            buf.putLong(uuid);
            buf.putInt(kenLength);
            buf.put(keyBytes);
            if (finalBigStringBytes.length > 0) {
                buf.put(finalBigStringBytes);
            }
        });

        return Repl.reply(slot, replPair, s_incremental_big_string, content);
    }

    private Repl.ReplReply s_incremental_big_string(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        var kenLength = buffer.getInt();
        var keyBytes = new byte[kenLength];
        buffer.get(keyBytes);
        var key = new String(keyBytes);

        // master big string file already deleted, skip
        if (buffer.hasRemaining()) {
            var bigStringBytes = new byte[buffer.remaining()];
            buffer.get(bigStringBytes);
            var s = slot(key);
            var oneSlot = localPersist.oneSlot(s.slot());
            oneSlot.asyncExecute(() -> {
                var bigStringFiles = oneSlot.getBigStringFiles();
                bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
                log.warn("Repl slave fetch incremental big string done, uuid={}, key={}, slot={}", uuid, key, s.slot());
            });
        }

        replPair.doneFetchBigStringUuid(uuid);
        return Repl.emptyReply();
    }

    private Repl.ReplReply exists_big_string(short slot, byte[] contentBytes) {
        // server received from client
        // send back exists big string to client, with flag can do next step
        // client already persisted big string uuid, send to client exclude sent big string
        var buffer = ByteBuffer.wrap(contentBytes);
        var bucketIndex = buffer.getInt();
        var sentIdList = new ArrayList<BigStringFiles.IdWithKey>();
        while (buffer.hasRemaining()) {
            var uuid = buffer.getLong();
            var keyHash = buffer.getLong();
            sentIdList.add(new BigStringFiles.IdWithKey(uuid, bucketIndex, keyHash, ""));
        }
        if (slot == 0 && bucketIndex % (1024 * 100) == 0) {
            log.warn("Repl master fetch exists big string, slave sent id list={}, bucket index={}, slot={}", sentIdList, bucketIndex, slot);
        }

        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringFiles = oneSlot.getBigStringFiles();
        var idListInMaster = bigStringFiles.getBigStringFileIdList(bucketIndex);
        if (idListInMaster.isEmpty() && bucketIndex == bucketsPerSlot - 1) {
            return Repl.reply(slot, replPair, s_exists_big_string, NextStepContent.INSTANCE);
        }

        var bigStringDir = oneSlot.getBigStringDir();
        var toSlaveExistsBigString = new ToSlaveExistsBigString(bucketIndex, bigStringDir, idListInMaster, sentIdList);
        return Repl.reply(slot, replPair, s_exists_big_string, toSlaveExistsBigString);
    }

    private Repl.ReplReply s_exists_big_string(short slot, byte[] contentBytes) {
        // client received from server
        if (NextStepContent.isNextStep(contentBytes)) {
            // next step, fetch exists chunk segments
            log.warn("Repl slave fetch data, go to step={}, slot={}", exists_short_string.name(), slot);
            return Repl.reply(slot, replPair, exists_short_string, new RawBytesContent(new byte[4]));
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        // remote bucket index
        var bucketIndex = buffer.getInt();
        var bigStringCount = buffer.getInt();
        var isSendAllOnce = buffer.get() == 1;

        var remoteReplProperties = replPair.getRemoteReplProperties();
        var bucketsPerSlotRemote = remoteReplProperties.bucketsPerSlot();

        if (slot == 0 && bucketIndex % (1024 * 100) == 0) {
            log.warn("Repl slave fetch exists big string, master sent big string count={}, bucket index={}, slot={}", bigStringCount, bucketIndex, slot);
        }

        if (bigStringCount > 0) {
            var firstOneSlot = localPersist.currentThreadFirstOneSlot();
            final var sSet = findKeyMustInSlot(firstOneSlot.slot(), "exists_big_string_uuids_bucket_index_" + bucketIndex);
            var rhk = SGroup.getRedisSet(sSet, this);
            if (rhk == null) {
                rhk = new RedisHashKeys();
            }

            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var keyHash = buffer.getLong();
                var bigStringBytesLength = buffer.getInt();
                var offset = buffer.position();

                var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
                var bucketIndexInner = KeyHash.bucketIndex(keyHash);
                var oneSlot = localPersist.oneSlot(slotInner);
                oneSlot.asyncExecute(() -> {
                    oneSlot.getBigStringFiles().writeBigStringBytes(uuid, bucketIndexInner, keyHash, contentBytes, offset, bigStringBytesLength);
                });
                buffer.position(offset + bigStringBytesLength);

                rhk.add(uuid + "_" + keyHash);
            }

            RedisHashKeys finalRhk = rhk;
            firstOneSlot.asyncExecute(() -> {
                SGroup.saveRedisSet(finalRhk, sSet, this, dictMap);
            });
        }

        if (isSendAllOnce) {
            if (bucketIndex == bucketsPerSlotRemote - 1) {
                log.warn("Repl slave fetch data, go to step={}, slot={}", exists_short_string.name(), slot);
                return Repl.reply(slot, replPair, exists_short_string, new RawBytesContent(new byte[4]));
            } else {
                return fetchExistsBigString(slot, bucketIndex + 1);
            }
        } else {
            return fetchExistsBigString(slot, bucketIndex);
        }
    }

    @VisibleForTesting
    SlotWithKeyHash findKeyMustInSlot(short targetSlot, String prefix) {
        for (int i = 0; i < 10_000; i++) {
            var key = prefix + i;
            var s = BaseCommand.slot(key, slotNumber);
            if (s.slot() == targetSlot) {
                return s;
            }
        }
        throw new RuntimeException("not found key must in slot");
    }

    @VisibleForTesting
    Repl.ReplReply fetchExistsBigString(short slot, int bucketIndex) {
        // use a list to store exists big string uuid
        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        final var s = findKeyMustInSlot(firstOneSlot.slot(), "exists_big_string_uuids_bucket_index_" + bucketIndex);
        var rhk = SGroup.getRedisSet(s, this);
        var set = rhk != null ? rhk.getSet() : new HashSet<String>();
        if (set.isEmpty()) {
            var content = fillBytes(4, buf -> buf.putInt(bucketIndex));
            return Repl.reply(slot, replPair, exists_big_string, content);
        }

        var content = fillBytes(4 + 16 * set.size(), buf -> {
            buf.putInt(bucketIndex);
            for (var one : set) {
                var arr = one.split("_");
                var uuid = Long.parseLong(arr[0]);
                var keyHash = Long.parseLong(arr[1]);
                buf.putLong(uuid);
                buf.putLong(keyHash);
            }
        });

        return Repl.reply(slot, replPair, exists_big_string, content);
    }

    private Reply exists_short_string(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var walGroupIndex = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        var bb = Unpooled.buffer();
        bb.writeInt(walGroupIndex);

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        var beginBucketIndex = walGroupIndex * oneChargeBucketNumber;
        for (int bucketIndex = beginBucketIndex; bucketIndex < beginBucketIndex + oneChargeBucketNumber; bucketIndex++) {
            oneSlot.getKeyLoader().encodeShortStringListToBuf(bucketIndex, bb);
        }

        return new Repl.ReplReplyFromBytes(replPair.getSlaveUuid(), slot, s_exists_short_string, bb.array(), 0, bb.writerIndex());
    }

    private Repl.ReplReply s_exists_short_string(short slot, byte[] contentBytes) {
        // client received from server
        var bb = Unpooled.wrappedBuffer(contentBytes);
        assert bb.readableBytes() >= 4;
        // remote
        var walGroupIndex = bb.readInt();

        HashMap<Short, HashMap<String, CompressedValue>> groupedBySlot = new HashMap<>();
        KeyLoader.decodeShortStringListFromBuf(bb, (keyHash, expireAt, seq, key, valueBytes) -> {
            var slotInner = BaseCommand.calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(valueBytes), key.getBytes(), keyHash);
            groupedBySlot.computeIfAbsent(slotInner, k -> new HashMap<>()).put(key, cv);
        });

        for (var entry : groupedBySlot.entrySet()) {
            var slotInner = entry.getKey();
            var oneSlot = localPersist.oneSlot(slotInner);
            oneSlot.asyncExecute(() -> {
                for (var entry2 : entry.getValue().entrySet()) {
                    var key = entry2.getKey();
                    var cv = entry2.getValue();
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash());
                    oneSlot.put(key, bucketIndex, cv);
                }
            });
        }

        var remoteReplProperties = replPair.getRemoteReplProperties();
        var walGroupNumberRemote = remoteReplProperties.bucketsPerSlot() / remoteReplProperties.oneChargeBucketNumber();

        if (walGroupIndex == walGroupNumberRemote - 1) {
            log.warn("Repl slave fetch data, go to step={}, slot={}", exists_chunk_segments.name(), slot);
            return Repl.reply(slot, replPair, exists_chunk_segments, new RawBytesContent(new byte[4]));
        } else {
            var firstOneSlot = localPersist.currentThreadFirstOneSlot();

            var nextGroupIndex = walGroupIndex + 1;
            if (nextGroupIndex % 1024 == 0) {
                // delay
                firstOneSlot.delayRun(1000, () -> {
                    replPair.write(exists_short_string, fillBytes(4, buf -> buf.putInt(nextGroupIndex)));
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_short_string, fillBytes(4, buf -> buf.putInt(nextGroupIndex)));
            }
        }
    }

    private Repl.ReplReply exists_dict(short slot, byte[] contentBytes) {
        // server received from client
        // client already persisted dict seq, send to client exclude sent dict
        ArrayList<Integer> sentDictSeqList = new ArrayList<>();
        if (contentBytes.length >= 4) {
            var sentDictSeqCount = contentBytes.length / 4;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentDictSeqCount; i++) {
                sentDictSeqList.add(buffer.getInt());
            }
        }
        log.warn("Repl master fetch exists dict, slave sent dict seq list={}, slot={}", sentDictSeqList, slot);

        var cacheDictCopy = dictMap.getCacheDictCopy();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        // master always sends global zstd dict to slave
        cacheDictCopy.put(Dict.GLOBAL_ZSTD_DICT_KEY, Dict.GLOBAL_ZSTD_DICT);
        cacheDictBySeqCopy.put(Dict.GLOBAL_ZSTD_DICT_SEQ, Dict.GLOBAL_ZSTD_DICT);

        var content = new ToSlaveExistsDict(cacheDictCopy, cacheDictBySeqCopy, sentDictSeqList);
        return Repl.reply(slot, replPair, s_exists_dict, content);
    }

    private Repl.ReplReply s_exists_dict(short slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var buffer = ByteBuffer.wrap(contentBytes);
        var dictCount = buffer.getInt();
        log.warn("Repl slave fetch exists dict, master sent dict count={}, slot={}", dictCount, slot);

        // decode
        try {
            for (int i = 0; i < dictCount; i++) {
                var encodeLength = buffer.getInt();
                var encodeBytes = new byte[encodeLength];
                buffer.get(encodeBytes);

                var is = new DataInputStream(new ByteArrayInputStream(encodeBytes));
                var dictWithKeyPrefixOrSuffix = Dict.decode(is);

                if (dictWithKeyPrefixOrSuffix == null) {
                    throw new IllegalArgumentException("Repl slave decode dict error, slot=" + slot);
                }

                var dict = dictWithKeyPrefixOrSuffix.dict();
                var keyPrefixOrSuffix = dictWithKeyPrefixOrSuffix.keyPrefixOrSuffix();
                if (keyPrefixOrSuffix.equals(Dict.GLOBAL_ZSTD_DICT_KEY)) {
                    dictMap.updateGlobalDictBytes(dict.getDictBytes());
                } else {
                    dictMap.putDict(keyPrefixOrSuffix, dict);
                }
                log.warn("Repl slave save master exists dict: dict with key={}, slot={}", dictWithKeyPrefixOrSuffix, slot);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // next step, fetch big string
        log.warn("Repl slave fetch data, go to step={}, slot={}", exists_big_string.name(), slot);
        return fetchExistsBigString(slot, 0);
    }

    private Repl.ReplReply exists_all_done(short slot, byte[] contentBytes) {
        // server received from client
        log.warn("Repl slave exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);
        return Repl.reply(slot, replPair, s_exists_all_done, NextStepContent.INSTANCE);
    }

    private Repl.ReplReply s_exists_all_done(short slot, byte[] contentBytes) {
        // client received from server
        log.warn("Repl master reply exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
        oneSlot.updateChunkSegmentIndexFromMeta();

        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        // update exists data all fetched done
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                lastUpdatedFileIndex, lastUpdatedOffset);

        if (slot == localPersist.firstOneSlot().slot()) {
            localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(true);
        }

        // begin incremental data catch up
        var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
        var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
        return Repl.reply(slot, replPair, catch_up, content);
    }

    private Repl.ReplReply catch_up(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var binlogMasterUuid = buffer.getLong();
        var needFetchFileIndex = buffer.getInt();
        var needFetchOffset = buffer.getLong();
        var lastUpdatedOffset = buffer.getLong();

        if (needFetchOffset == 0) {
            log.debug("Repl master handle catch up from new binlog file, slot={}, slave uuid={}, {}, need fetch file index={}, offset={}",
                    slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), needFetchFileIndex, needFetchOffset);
        }

        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (needFetchOffset % binlogOneSegmentLength != 0) {
            throw new IllegalArgumentException("Repl master handle error: catch up offset=" + needFetchOffset +
                    " is not a multiple of binlog one segment length=" + binlogOneSegmentLength);
        }

        var oneSlot = localPersist.oneSlot(slot);
        if (oneSlot.getMasterUuid() != binlogMasterUuid) {
            var errorMessage = "Repl master handle error: master uuid not match";
            log.error(errorMessage);
            return Repl.error(slot, replPair, errorMessage);
        }

        var isMasterReadonlyByte = oneSlot.isReadonly() ? (byte) 1 : (byte) 0;

        // is readonly byte + current file index + current offset
        var binlog = oneSlot.getBinlog();
        var currentFo = binlog.currentFileIndexAndOffset();

        var onlyReadonlyResponseContent = fillBytes(1 + 4 + 8, buf -> {
            buf.put(isMasterReadonlyByte);
            buf.putInt(currentFo.fileIndex());
            buf.putLong(currentFo.offset());
        });

        if (needFetchOffset != lastUpdatedOffset) {
            // check if slave already catch up to last binlog segment offset
            if (currentFo.fileIndex() == needFetchFileIndex && currentFo.offset() == lastUpdatedOffset) {
                replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(currentFo);
                replPair.setAllCaughtUp(true);
                return Repl.reply(slot, replPair, s_catch_up, onlyReadonlyResponseContent);
            }
        }

        byte[] readSegmentBytes;
        try {
            readSegmentBytes = binlog.readPrevRafOneSegment(needFetchFileIndex, needFetchOffset);
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: read binlog file error";
            // need not exception stack trace
            log.error("{}={}", errorMessage, e.getMessage());
            return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
        }

        // all fetched
        if (readSegmentBytes == null) {
            var fo = new Binlog.FileIndexAndOffset(needFetchFileIndex, lastUpdatedOffset);
            replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(fo);
            replPair.setAllCaughtUp(true);
            return Repl.reply(slot, replPair, s_catch_up, onlyReadonlyResponseContent);
        }

        // 1 byte for readonly
        // 4 bytes for need fetch file index
        // 8 bytes for need fetch offset
        // 4 bytes for current file index
        // 8 bytes for current offset
        var content = fillBytes(1 + 4 + 8 + 4 + 8 + 4 + readSegmentBytes.length, buf -> {
            buf.put(isMasterReadonlyByte);
            buf.putInt(needFetchFileIndex);
            buf.putLong(needFetchOffset);
            buf.putInt(currentFo.fileIndex());
            buf.putLong(currentFo.offset());
            buf.putInt(readSegmentBytes.length);
            buf.put(readSegmentBytes);
        });

        replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(new Binlog.FileIndexAndOffset(needFetchFileIndex,
                needFetchOffset + readSegmentBytes.length));
        return Repl.reply(slot, replPair, s_catch_up, content);
    }

    private long catchUpLoopCount = 0L;

    private Repl.ReplReply s_catch_up(short slot, byte[] contentBytes) {
        catchUpLoopCount++;
        var catchUpIntervalMillis = ConfForSlot.global.confRepl.catchUpIntervalMillis;

        // client received from server
        replPair.setLastGetCatchUpResponseMillis(System.currentTimeMillis());

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();

        // last updated means next batch, but not fetch yet, refer end of this method
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        if (slot != localPersist.firstOneSlot().slot()) {
            // wait the first slot fetched exists all done
            if (!localPersist.isAsSlaveFirstSlotFetchedExistsAllDone()) {
                // use margin file offset
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                oneSlot.delayRun(catchUpIntervalMillis, () -> {
                    replPair.write(catch_up, content);
                });
                return Repl.emptyReply();
            }
        }

        // master has no more binlog to catch up, delay to catch up again
        // 13 -> readonly byte + current file index + current offset
        // 2 -> pong trigger slave begin to do catch_up again
        if (contentBytes.length == 13 || contentBytes.length == 2) {
            boolean resetMasterReadonlyByContentBytes = contentBytes.length == 13;
            boolean isMasterReadonly = false;
            if (resetMasterReadonlyByContentBytes) {
                var buffer = ByteBuffer.wrap(contentBytes);
                isMasterReadonly = buffer.get() == 1;
                var masterCurrentFileIndex = buffer.getInt();
                var masterCurrentOffset = buffer.getLong();

                replPair.setMasterReadonly(isMasterReadonly);
                replPair.setAllCaughtUp(true);
                var masterCurrentFo = new Binlog.FileIndexAndOffset(masterCurrentFileIndex, masterCurrentOffset);
                replPair.setMasterBinlogCurrentFileIndexAndOffset(masterCurrentFo);
                replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(masterCurrentFo);
            }

            if (!isMasterReadonly) {
                // use margin file offset
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                oneSlot.delayRun(catchUpIntervalMillis, () -> {
                    replPair.write(catch_up, content);
                });
            }
            return Repl.emptyReply();
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var isMasterReadonly = buffer.get() == 1;
        var fetchedFileIndex = buffer.getInt();
        var fetchedOffset = buffer.getLong();

        var masterCurrentFileIndex = buffer.getInt();
        var masterCurrentOffset = buffer.getLong();
        var masterCurrentFo = new Binlog.FileIndexAndOffset(masterCurrentFileIndex, masterCurrentOffset);

        var readSegmentLength = buffer.getInt();
        var readSegmentBytes = new byte[readSegmentLength];
        buffer.get(readSegmentBytes);

        replPair.setMasterReadonly(isMasterReadonly);
        replPair.setAllCaughtUp(fetchedFileIndex == masterCurrentFileIndex && masterCurrentOffset == fetchedOffset + readSegmentLength);
        replPair.setMasterBinlogCurrentFileIndexAndOffset(masterCurrentFo);

        // only when self is as slave but also as master, need to write binlog
        var binlog = oneSlot.getBinlog();
        try {
            binlog.writeFromMasterOneSegmentBytes(readSegmentBytes, fetchedFileIndex, fetchedOffset);
        } catch (IOException e) {
            log.error("Repl slave write binlog from master error, slot={}", slot, e);
        }

        // update last catch up file index and offset
        var skipBytesN = 0;
        var isLastTimeCatchUpThisSegmentButNotCompleted = lastUpdatedFileIndex == fetchedFileIndex && lastUpdatedOffset > fetchedOffset;
        if (isLastTimeCatchUpThisSegmentButNotCompleted) {
            skipBytesN = (int) (lastUpdatedOffset - fetchedOffset);
        }

        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (skipBytesN > binlogOneSegmentLength) {
            throw new IllegalStateException("Repl slave handle error: skip bytes n=" + skipBytesN +
                    " is greater than binlog one segment length=" + binlogOneSegmentLength + ", slot=" + slot);
        }

        try {
            var n = Binlog.decodeAndApply(slot, readSegmentBytes, skipBytesN, replPair);
            if (fetchedOffset == 0 && catchUpLoopCount % 10 == 0) {
                log.info("Repl binlog catch up success, slave uuid={}, {}, catch up file index={}, catch up offset={}, apply n={}, slot={}",
                        replPair.getSlaveUuid(), replPair.getHostAndPort(), fetchedFileIndex, fetchedOffset, n, slot);
            }
        } catch (Exception e) {
            var errorMessage = "Repl slave handle error: decode and apply binlog error, slot=" + slot;
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
        }

        // set can read if catch up to current file, and offset not too far
        var isCatchUpToCurrentFile = fetchedFileIndex == masterCurrentFileIndex;
        boolean canRead;
        if (isCatchUpToCurrentFile) {
            var diffOffset = masterCurrentOffset - fetchedOffset - skipBytesN;
            canRead = diffOffset < ConfForSlot.global.confRepl.catchUpOffsetMinDiff;
        } else {
            canRead = false;
        }

        if ((canRead && !oneSlot.isCanRead()) || (!canRead && oneSlot.isCanRead())) {
            try {
                oneSlot.setCanRead(canRead);
                log.warn("Repl slave can read={}, as already catch up nearly to master latest, slot={}", canRead, slot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(new Binlog.FileIndexAndOffset(fetchedFileIndex, fetchedOffset + readSegmentLength));

        // catch up latest segment, delay to catch up again
        var marginCurrentOffset = Binlog.marginFileOffset(masterCurrentOffset);
        var isCatchUpOffsetInLatestSegment = isCatchUpToCurrentFile && fetchedOffset == marginCurrentOffset;
        if (isCatchUpOffsetInLatestSegment) {
            metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                    fetchedFileIndex, fetchedOffset + readSegmentLength);

            // still catch up current (latest) segment, delay
            var content = toMasterCatchUp(binlogMasterUuid, fetchedFileIndex, fetchedOffset, fetchedOffset + readSegmentLength);
            oneSlot.delayRun(catchUpIntervalMillis, () -> {
                replPair.write(catch_up, content);
            });
            return Repl.emptyReply();
        }

        if (readSegmentLength != binlogOneSegmentLength) {
            throw new IllegalStateException("Repl slave handle error: read segment length=" + readSegmentLength +
                    " is not equal to binlog one segment length=" + binlogOneSegmentLength);
        }

        var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
        var isCatchUpLastSegmentInOneFile = fetchedOffset == (binlogOneFileMaxLength - binlogOneSegmentLength);
        var nextCatchUpFileIndex = isCatchUpLastSegmentInOneFile ? fetchedFileIndex + 1 : fetchedFileIndex;
        // one segment length may != binlog one segment length, need to re-fetch this segment
        var nextCatchUpOffset = isCatchUpLastSegmentInOneFile ? 0 : fetchedOffset + binlogOneSegmentLength;

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                nextCatchUpFileIndex, nextCatchUpOffset);

        var content = toMasterCatchUp(binlogMasterUuid, nextCatchUpFileIndex, nextCatchUpOffset, nextCatchUpOffset);
        // when catch up to next file, delay to catch up again
        if (nextCatchUpOffset == 0) {
            log.info("Repl slave ready to catch up to next file, slave uuid={}, {}, binlog file index={}, offset={}, slot={}",
                    replPair.getSlaveUuid(), replPair.getHostAndPort(), nextCatchUpFileIndex, nextCatchUpOffset, slot);

            oneSlot.delayRun(catchUpIntervalMillis, () -> {
                replPair.write(catch_up, content);
            });
            return Repl.emptyReply();
        } else {
            return Repl.reply(slot, replPair, catch_up, content);
        }
    }

    private static boolean skipTryCatchUpAgainAfterSlaveTcpClientClosed;

    @TestOnly
    public static boolean isSkipTryCatchUpAgainAfterSlaveTcpClientClosed() {
        return skipTryCatchUpAgainAfterSlaveTcpClientClosed;
    }

    @TestOnly
    public static void setSkipTryCatchUpAgainAfterSlaveTcpClientClosed(boolean skipTryCatchUpAgainAfterSlaveTcpClientClosed) {
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed = skipTryCatchUpAgainAfterSlaveTcpClientClosed;
    }

    public static void tryCatchUpAgainAfterSlaveTcpClientClosed(ReplPair replPairAsSlave, byte[] mockResultBytes) {
        if (skipTryCatchUpAgainAfterSlaveTcpClientClosed) {
            return;

        }
        var log = LoggerFactory.getLogger(XGroup.class);
        var localPersist = LocalPersist.getInstance();

        final var targetSlot = replPairAsSlave.getSlot();
        var oneSlot = localPersist.oneSlot(targetSlot);
        oneSlot.asyncExecute(() -> {
            var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

            var isExistsDataAllFetched = metaChunkSegmentIndex.isExistsDataAllFetched();
            if (!isExistsDataAllFetched) {
                System.out.println("Repl slave try catch up again after slave tcp client close, but exists data not all fetched, slot=" + targetSlot);
                return;
            }

            var lastUpdatedMasterUuid = metaChunkSegmentIndex.getMasterUuid();
            if (lastUpdatedMasterUuid != replPairAsSlave.getMasterUuid()) {
                System.out.println("Repl slave try catch up again after slave tcp client close, but master uuid not match, slot=" + targetSlot);
                return;
            }

            var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
            var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
            var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

            var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);

            byte[] resultBytes = null;
            if (mockResultBytes != null) {
                resultBytes = mockResultBytes;
            } else {
                // use jedis to get data sync, because need try to connect to master
                var jedisPoolHolder = JedisPoolHolder.getInstance();
                var jedisPool = jedisPoolHolder.createIfNotCached(replPairAsSlave.getHost(), replPairAsSlave.getPort());
                try {
                    resultBytes = JedisPoolHolder.exe(jedisPool, jedis -> {
                        var pong = jedis.ping();
                        System.out.println("Repl slave try ping after slave tcp client close, to " + replPairAsSlave.getHostAndPort() + ", pong=" + pong + ", slot=" + targetSlot);
                        // get data from master
                        // refer RequestHandler.transferDataForXGroup
                        return jedis.get(
                                (
                                        XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ","
                                                + "slot,"
                                                + targetSlot + ","
                                                + X_CATCH_UP_AS_SUB_CMD + ","
                                                + replPairAsSlave.getSlaveUuid() + ","
                                                + lastUpdatedMasterUuid + ","
                                                + lastUpdatedFileIndex + ","
                                                + marginLastUpdatedOffset + ","
                                                + lastUpdatedOffset
                                ).getBytes()
                        );
                    });
                } catch (Exception e) {
                    System.out.println("Repl slave try catch up again after slave tcp client close error=" + e.getMessage() + ", slot=" + targetSlot);
                    replPairAsSlave.setMasterCanNotConnect(true);
                }
            }

            if (resultBytes == null) {
                System.out.println("Repl slave try catch up again after slave tcp client close, but get data from master is null, slot=" + targetSlot);
                return;
            }

            try {
                var xGroup = new XGroup("", null, null);
                xGroup.replPair = replPairAsSlave;
                xGroup.s_catch_up(targetSlot, resultBytes);

                if (replPairAsSlave.isAllCaughtUp()) {
                    System.out.println("Repl slave try catch up again, is all caught up!!!, slot=" + targetSlot);
                } else {
                    System.out.println("Repl slave try catch up again, is not!!! all caught up!!!, slot=" + targetSlot);
                    // todo, try to loop if not all caught up
                }
            } catch (Exception e) {
                log.error("Repl slave try catch up again after slave tcp client close error", e);
                replPairAsSlave.setMasterCanNotConnect(true);
            }
        });
    }
}
