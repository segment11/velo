package io.velo.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.decode.Request;
import io.velo.persist.*;
import io.velo.repl.Binlog;
import io.velo.repl.Repl;
import io.velo.repl.ReplPair;
import io.velo.repl.ReplType;
import io.velo.repl.content.*;
import io.velo.repl.incremental.XSkipApply;
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.BulkReply;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;
import io.velo.type.RedisHashKeys;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

import static io.velo.repl.ReplType.*;

/**
 * Handles Redis commands starting with letter 'X'.
 * This includes commands like XADD, XTRIM, XRANGE.
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
            var map = ConfForSlot.global.slaveCanMatchCheckValues();
            var objectMapper = new ObjectMapper();
            try {
                var jsonStr = objectMapper.writeValueAsString(map);
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
                var data4 = Repl.decode(nettyByteBuf);
                if (data4 == null) {
                    return NilReply.INSTANCE;
                }

                if (reply.isReplType(error)) {
                    return new ErrorReply(new String(data4[3]));
                }

                return new BulkReply(data4[3]);
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

    public Reply handleRepl() {
        var slaveUuid = ByteBuffer.wrap(data[0]).getLong();
        var slot = ByteBuffer.wrap(data[1]).getShort();
        var replType = fromCode(data[2][0]);
        if (replType == null) {
            log.error("Repl handle error: unknown repl type code={}, slot={}", data[2][0], slot);
            return null;
        }

        try {
            return handleReplInner(slot, replType, slaveUuid);
        } catch (Exception e) {
            return Repl.error(slot, slaveUuid, e.getMessage());
        }
    }

    private Reply handleReplInner(short slot, ReplType replType, long slaveUuid) {
        var contentBytes = data[3];

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
            case exists_key_buckets -> exists_key_buckets(slot, contentBytes);
            case meta_key_bucket_split_number -> meta_key_bucket_split_number(slot, contentBytes);
            case stat_key_count_in_buckets -> stat_key_count_in_buckets(slot, contentBytes);
            case exists_big_string -> exists_big_string(slot, contentBytes);
            case incremental_big_string -> incremental_big_string(slot, contentBytes);
            case exists_dict -> exists_dict(slot, contentBytes);
            case exists_all_done -> exists_all_done(slot, contentBytes);
            case catch_up -> catch_up(slot, contentBytes);
            case s_exists_wal -> s_exists_wal(slot, contentBytes);
            case s_exists_chunk_segments -> s_exists_chunk_segments(slot, contentBytes);
            case s_exists_key_buckets -> s_exists_key_buckets(slot, contentBytes);
            case s_meta_key_bucket_split_number -> s_meta_key_bucket_split_number(slot, contentBytes);
            case s_stat_key_count_in_buckets -> s_stat_key_count_in_buckets(slot, contentBytes);
            case s_exists_big_string -> s_exists_big_string(slot, contentBytes);
            case s_incremental_big_string -> s_incremental_big_string(slot, contentBytes);
            case s_exists_dict -> s_exists_dict(slot, contentBytes);
            case s_exists_all_done -> s_exists_all_done(slot, contentBytes);
            case s_catch_up -> s_catch_up(slot, contentBytes);
        };
    }

    @VisibleForTesting
    Repl.ReplReply hello(short slot, byte[] contentBytes) {
        // server received hello from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();

        var len = buffer.getInt();
        var b = new byte[len];
        buffer.get(b);
        var netListenAddresses = new String(b);

        var slaveSlotNumber = buffer.getInt();
        var replProperties = new ConfForSlot.ReplProperties(buffer.getInt(), buffer.getInt(), buffer.get(), buffer.getInt(), buffer.getInt());

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
        replPair.setRedoSet(slaveSlotNumber == ConfForGlobal.slotNumber && replProperties.isReplRedoSet(ConfForSlot.global.generateReplProperties()));

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
            var xSkipApply = new XSkipApply(oneSlot.getSnowFlake().nextId(), chunk.getSegmentIndex());
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

    @VisibleForTesting
    Repl.ReplReply hi(short slot, byte[] contentBytes) {
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

        var masterSlotNumber = buffer.getInt();
        var replProperties = new ConfForSlot.ReplProperties(buffer.getInt(), buffer.getInt(), buffer.get(), buffer.getInt(), buffer.getInt());

        // should not happen
        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl slave handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}, slot={}",
                    replPair.getSlaveUuid(), slaveUuid, slot);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl slave handle hi: slave uuid={}, master uuid={}, slot={}", slaveUuid, masterUuid, slot);

        replPair.setRemoteReplProperties(replProperties);
        replPair.setRedoSet(masterSlotNumber == ConfForGlobal.slotNumber && replProperties.isReplRedoSet(ConfForSlot.global.generateReplProperties()));

        if (slot == localPersist.firstOneSlot().slot()) {
            localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(false);
        }

        var oneSlot = localPersist.oneSlot(slot);
        if (!replPair.isRedoSet()) {
            // after exist all done, when catch up, XOneWalGroupSeq will update chunk segment index
            oneSlot.setMetaChunkSegmentIndexInt(currentSegmentIndex);
        }
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
            var rawBytes = new byte[4 * cacheDictBySeqCopy.size()];
            var rawBuffer = ByteBuffer.wrap(rawBytes);
            for (var entry : cacheDictBySeqCopy.entrySet()) {
                var seq = entry.getKey();
                rawBuffer.putInt(seq);
            }

            return Repl.reply(slot, replPair, exists_dict, new RawBytesContent(rawBytes));
        }
    }

    @VisibleForTesting
    Repl.ReplReply exists_wal(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, s_exists_wal, NextStepContent.INSTANCE);
        }

        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var groupIndex = buffer.getInt();
        var lastSeqAfterPut = buffer.getLong();
        var lastSeqShortValueAfterPut = buffer.getLong();

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (groupIndex < 0 || groupIndex >= walGroupNumber) {
            log.error("Repl master send wal exists bytes error: group index out of range, slot={}, group index={}",
                    slot, groupIndex);
            return Repl.error(slot, replPair, "Repl master send wal exists bytes error: group index out of range");
        }

        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByGroupIndex(groupIndex);

        if (lastSeqAfterPut == targetWal.getLastSeqAfterPut() && lastSeqShortValueAfterPut == targetWal.getLastSeqShortValueAfterPut()) {
            if (groupIndex % 1000 == 0) {
                log.warn("Repl master skip send wal exists bytes, group index={}, slot={}", groupIndex, slot);
            }

            // only reply group index, no need to send wal exists bytes
            var responseBytes = new byte[4];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.putInt(groupIndex);
            return Repl.reply(slot, replPair, s_exists_wal, new RawBytesContent(responseBytes));
        } else {
            if (groupIndex % 1000 == 0) {
                log.warn("Repl master will fetch exists wal, group index={}, slot={}", groupIndex, slot);
            }
        }

        try {
            var toSlaveBytes = targetWal.toSlaveExistsOneWalGroupBytes();
            return Repl.reply(slot, replPair, s_exists_wal, new RawBytesContent(toSlaveBytes));
        } catch (IOException e) {
            log.error("Repl master get wal exists bytes error, group index={}, slot={}", groupIndex, slot, e);
            return Repl.error(slot, replPair, "Repl master get wal exists bytes error=" + e.getMessage());
        }
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_wal(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, exists_all_done, NextStepContent.INSTANCE);
        }

        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var groupIndex = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        if (contentBytes.length > 4) {
            var targetWal = oneSlot.getWalByGroupIndex(groupIndex);
            try {
                targetWal.fromMasterExistsOneWalGroupBytes(contentBytes);
            } catch (IOException e) {
                log.error("Repl slave update wal exists bytes error, group index={}, slot={}", groupIndex, slot, e);
                return Repl.error(slot, replPair, "Repl slave update wal exists bytes error=" + e.getMessage());
            }
        } else {
            // skip
            replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_wal);
            if (groupIndex % 1000 == 0) {
                log.info("Repl slave skip update wal exists bytes, group index={}, slot={}", groupIndex, slot);
            }
        }

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (groupIndex == walGroupNumber - 1) {
            log.warn("Repl slave fetch exists all done after fetch wal when slot is not the first slot, slot={}", slot);
            return Repl.reply(slot, replPair, exists_all_done, NextStepContent.INSTANCE);
        } else {
            var nextGroupIndex = groupIndex + 1;
            if (nextGroupIndex % 1000 == 0) {
                // delay
                oneSlot.delayRun(1000, () -> {
                    replPair.write(exists_wal, requestExistsWal(oneSlot, nextGroupIndex));
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_wal, requestExistsWal(oneSlot, nextGroupIndex));
            }
        }
    }

    private RawBytesContent requestExistsWal(OneSlot oneSlot, int walGroupIndex) {
        // 4 bytes int for group index, 8 bytes long for last seq, 8 bytes long for last seq of short value
        var requestBytes = new byte[4 + 8 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.putInt(walGroupIndex);

        var targetWal = oneSlot.getWalByGroupIndex(walGroupIndex);
        requestBuffer.putLong(targetWal.getLastSeqAfterPut());
        requestBuffer.putLong(targetWal.getLastSeqShortValueAfterPut());

        return new RawBytesContent(requestBytes);
    }

    Repl.ReplReply exists_chunk_segments_redo(short slot, byte[] contentBytes) {
        // server received from client
        return null;
    }

    @VisibleForTesting
    Repl.ReplReply exists_chunk_segments(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return exists_chunk_segments_redo(slot, contentBytes);
        }

        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();

        if (beginSegmentIndex % (segmentCount * 100) == 0) {
            log.warn("Repl master fetch exists chunk segments, begin segment index={}, segment count={}, slot={}",
                    beginSegmentIndex, segmentCount, slot);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var chunk = oneSlot.getChunk();
        var metaChunkSegmentFlagSeq = oneSlot.getMetaChunkSegmentFlagSeq();
        var masterMetaBytes = metaChunkSegmentFlagSeq.getOneBatch(beginSegmentIndex, segmentCount);
        if (ToMasterExistsChunkSegments.isSlaveSameForThisBatch(masterMetaBytes, contentBytes)) {
            var responseBytes = new byte[4 + 4];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.putInt(beginSegmentIndex);
            responseBuffer.putInt(segmentCount);
            return Repl.reply(slot, replPair, s_exists_chunk_segments, new RawBytesContent(responseBytes));
        }

        var nextSegmentsHasData = true;
        var chunkSegmentsBytes = oneSlot.preadForRepl(beginSegmentIndex);
        if (chunkSegmentsBytes == null) {
            chunkSegmentsBytes = new byte[0];
            nextSegmentsHasData = oneSlot.hasData(beginSegmentIndex, chunk.getMaxSegmentIndex() - beginSegmentIndex + 1);
        }

        var responseBytes = new byte[4 + 4 + 4 + masterMetaBytes.length + (segmentCount * 4) + 4 + chunkSegmentsBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(beginSegmentIndex);
        responseBuffer.putInt(segmentCount);
        responseBuffer.putInt(masterMetaBytes.length);
        responseBuffer.put(masterMetaBytes);

        if (ConfForGlobal.pureMemory) {
            for (int i = 0; i < segmentCount; i++) {
                var targetSegmentIndex = beginSegmentIndex + i;
                var segmentRealLength = chunk.getSegmentRealLength(targetSegmentIndex);
                responseBuffer.putInt(segmentRealLength);
            }
        } else {
            responseBuffer.position(responseBuffer.position() + (segmentCount * 4));
        }

        if (nextSegmentsHasData) {
            responseBuffer.putInt(chunkSegmentsBytes.length);
            if (chunkSegmentsBytes.length > 0) {
                responseBuffer.put(chunkSegmentsBytes);
            }
        } else {
            responseBuffer.putInt(-1);
        }

        return Repl.reply(slot, replPair, s_exists_chunk_segments, new RawBytesContent(responseBytes));
    }

    Repl.ReplReply s_exists_chunk_segments_redo(short slot, byte[] contentBytes) {
        // client received from server
        return null;
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_chunk_segments(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return s_exists_chunk_segments_redo(slot, contentBytes);
        }

        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();
        // segmentCount == FdReadWrite.REPL_ONCE_INNER_COUNT

        if (beginSegmentIndex % (segmentCount * 100) == 0) {
            log.warn("Repl slave ready to fetch exists chunk segments, begin segment index={}, segment count={}, slot={}",
                    beginSegmentIndex, segmentCount, slot);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var chunk = oneSlot.getChunk();
        var metaChunkSegmentFlagSeq = oneSlot.getMetaChunkSegmentFlagSeq();
        // content bytes length == 8 -> slave is same for this batch, skip
        if (contentBytes.length != 8) {
            var metaBytesLength = buffer.getInt();
            var metaBytes = new byte[metaBytesLength];
            buffer.get(metaBytes);
            metaChunkSegmentFlagSeq.overwriteOneBatch(metaBytes, beginSegmentIndex, segmentCount);

            var segmentRealLengths = new int[segmentCount];
            for (int i = 0; i < segmentCount; i++) {
                segmentRealLengths[i] = buffer.getInt();
            }

            var chunkSegmentsLength = buffer.getInt();
            if (chunkSegmentsLength == -1) {
                log.warn("Repl slave fetch exists chunk segments no more segments, begin segment index={}, slot={}", beginSegmentIndex, slot);
                // next segments no data
                chunk.truncateChunkFdFromSegmentIndex(beginSegmentIndex);

                // update meta segment flag init, not exactly same as master
                var leftSegmentCount = chunk.getMaxSegmentIndex() - beginSegmentIndex + 1;
                oneSlot.setSegmentMergeFlagBatch(beginSegmentIndex, leftSegmentCount,
                        Chunk.Flag.init.flagByte(), null, MetaChunkSegmentFlagSeq.INIT_WAL_GROUP_INDEX);

                // next step, fetch exists wal
                log.warn("Repl slave fetch data, go to step={}, slot={}", exists_wal.name(), slot);
                return Repl.reply(slot, replPair, exists_wal, requestExistsWal(oneSlot, 0));
            } else if (chunkSegmentsLength == 0) {
                log.warn("Repl slave fetch exists chunk segments batch all bytes 0, begin segment index={}, slot={}", beginSegmentIndex, slot);
                if (ConfForGlobal.pureMemory) {
                    for (int i = 0; i < segmentCount; i++) {
                        var targetSegmentIndex = beginSegmentIndex + i;
                        chunk.clearSegmentBytesWhenPureMemory(targetSegmentIndex);
                    }
                } else {
                    // write 0 to files
                    oneSlot.writeChunkSegmentsFromMasterExists(ConfForSlot.ConfChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE,
                            beginSegmentIndex, segmentCount, segmentRealLengths);
                }
            } else {
                var chunkSegmentsBytes = new byte[chunkSegmentsLength];
                buffer.get(chunkSegmentsBytes);

                var realSegmentCount = chunkSegmentsLength / ConfForSlot.global.confChunk.segmentLength;
                oneSlot.writeChunkSegmentsFromMasterExists(chunkSegmentsBytes,
                        beginSegmentIndex, realSegmentCount, segmentRealLengths);
            }
        } else {
            replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_chunk_segments);
        }

        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        boolean isLastBatch = maxSegmentNumber == beginSegmentIndex + segmentCount;
        if (isLastBatch) {
            // next step, fetch exists wal
            log.warn("Repl slave fetch data, go to step={}, slot={}", exists_wal.name(), slot);
            return Repl.reply(slot, replPair, exists_wal, requestExistsWal(oneSlot, 0));
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentCount;
            var nextBatchMetaBytes = metaChunkSegmentFlagSeq.getOneBatch(nextBatchBeginSegmentIndex, segmentCount);
            var content = new ToMasterExistsChunkSegments(nextBatchBeginSegmentIndex, segmentCount, nextBatchMetaBytes);

            if (nextBatchBeginSegmentIndex % (segmentCount * 100) == 0) {
                oneSlot.delayRun(100, () -> {
                    replPair.write(exists_chunk_segments, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_chunk_segments, content);
            }
        }
    }

    @VisibleForTesting
    Repl.ReplReply exists_key_buckets(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, s_exists_key_buckets, NextStepContent.INSTANCE);
        }

        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var splitIndex = buffer.get();
        var beginBucketIndex = buffer.getInt();
        var oneWalGroupSeq = buffer.getLong();

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (beginBucketIndex % (oneChargeBucketNumber * 1024) == 0) {
            log.warn("Repl master fetch exists key buckets, split index={}, begin bucket index={}, slot={}",
                    splitIndex, beginBucketIndex, slot);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        var maxSplitNumber = keyLoader.maxSplitNumberForRepl();

        byte[] bytes = null;
        var masterOneWalGroupSeq = keyLoader.getMetaOneWalGroupSeq(splitIndex, beginBucketIndex);
        var isSkip = masterOneWalGroupSeq == oneWalGroupSeq;
        if (!isSkip) {
            if (ConfForGlobal.pureMemoryV2) {
                // repl all key hash buckets' records
                var recordXBytesArray = keyLoader.getRecordsBytesArrayInOneWalGroup(beginBucketIndex);
                // array size int
                var len = 4;
                for (var recordXBytes : recordXBytesArray) {
                    len += 4 + recordXBytes.length;
                }

                bytes = new byte[len];
                var buffer1 = ByteBuffer.wrap(bytes);
                buffer1.putInt(recordXBytesArray.length);
                for (var recordXBytes : recordXBytesArray) {
                    buffer1.putInt(recordXBytes.length);
                    buffer1.put(recordXBytes);
                }
            } else {
                bytes = keyLoader.readBatchInOneWalGroup(splitIndex, beginBucketIndex);
            }
        }

        if (bytes == null) {
            bytes = new byte[0];
        }

        var responseBytes = new byte[1 + 1 + 4 + 1 + 8 + bytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.put(splitIndex);
        responseBuffer.put(maxSplitNumber);
        responseBuffer.putInt(beginBucketIndex);
        responseBuffer.put(isSkip ? (byte) 1 : (byte) 0);
        responseBuffer.putLong(masterOneWalGroupSeq);
        if (bytes.length != 0) {
            responseBuffer.put(bytes);
        }

        return Repl.reply(slot, replPair, s_exists_key_buckets, new RawBytesContent(responseBytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_key_buckets(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, exists_chunk_segments, NextStepContent.INSTANCE);
        }

        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();

        var buffer = ByteBuffer.wrap(contentBytes);
        var splitIndex = buffer.get();
        var maxSplitNumber = buffer.get();
        var beginBucketIndex = buffer.getInt();
        var isSkip = buffer.get() == 1;
        var masterOneWalGroupSeq = buffer.getLong();
        var leftLength = buffer.remaining();

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (beginBucketIndex % (oneChargeBucketNumber * 1024) == 0) {
            log.warn("Repl slave ready to fetch exists key buckets, split index={}, begin bucket index={}, slot={}",
                    splitIndex, beginBucketIndex, slot);
        }

        boolean isAllReceived;
        boolean isLastBatchInThisSplit = false;
        if (ConfForGlobal.pureMemoryV2) {
            if (!isSkip) {
                var arraySize = buffer.getInt();
                var recordXBytesArray = new byte[arraySize][];
                for (var i = 0; i < arraySize; i++) {
                    var recordBytesLength = buffer.getInt();
                    var recordBytes = new byte[recordBytesLength];
                    buffer.get(recordBytes);
                    recordXBytesArray[i] = recordBytes;
                }

                keyLoader.updateRecordXBytesArray(recordXBytesArray);
                keyLoader.setMetaOneWalGroupSeq(splitIndex, beginBucketIndex, masterOneWalGroupSeq);
            } else {
                replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_key_buckets);
            }

            isLastBatchInThisSplit = beginBucketIndex == ConfForSlot.global.confBucket.bucketsPerSlot - oneChargeBucketNumber;
            isAllReceived = isLastBatchInThisSplit;
        } else {
            if (!isSkip) {
                var sharedBytesList = new byte[splitIndex + 1][];
                if (leftLength == 0) {
                    // clear local key buckets
                    sharedBytesList[splitIndex] = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE * oneChargeBucketNumber];
                } else {
                    // overwrite key buckets
                    var sharedBytes = new byte[leftLength];
                    buffer.get(sharedBytes);
                    sharedBytesList[splitIndex] = sharedBytes;
                }
                keyLoader.writeSharedBytesList(sharedBytesList, beginBucketIndex);
                keyLoader.setMetaOneWalGroupSeq(splitIndex, beginBucketIndex, masterOneWalGroupSeq);
            } else {
                replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_key_buckets);
            }

            isLastBatchInThisSplit = beginBucketIndex == ConfForSlot.global.confBucket.bucketsPerSlot - oneChargeBucketNumber;
            isAllReceived = splitIndex == maxSplitNumber - 1 && isLastBatchInThisSplit;
        }

        if (isAllReceived) {
            log.warn("Repl slave fetch data, go to step={}, slot={}", exists_chunk_segments.name(), slot);
            var metaChunkSegmentFlagSeq = oneSlot.getMetaChunkSegmentFlagSeq();
            var segmentCount = FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;
            var metaBytes = metaChunkSegmentFlagSeq.getOneBatch(0, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
            return Repl.reply(slot, replPair, exists_chunk_segments, content);
        } else {
            var nextSplitIndex = isLastBatchInThisSplit ? splitIndex + 1 : splitIndex;
            var nextBeginBucketIndex = isLastBatchInThisSplit ? 0 : beginBucketIndex + oneChargeBucketNumber;

            if (ConfForGlobal.pureMemoryV2) {
                nextSplitIndex = 0;
                nextBeginBucketIndex = beginBucketIndex + oneChargeBucketNumber;
            }

            var requestBytes = new byte[1 + 4 + 8];
            var requestBuffer = ByteBuffer.wrap(requestBytes);
            requestBuffer.put((byte) nextSplitIndex);
            requestBuffer.putInt(nextBeginBucketIndex);
            var slaveOneWalGroupSeq = keyLoader.getMetaOneWalGroupSeq((byte) nextSplitIndex, nextBeginBucketIndex);
            requestBuffer.putLong(slaveOneWalGroupSeq);
            var content = new RawBytesContent(requestBytes);

            if (nextBeginBucketIndex % (oneChargeBucketNumber * 1024) == 0) {
                oneSlot.delayRun(100, () -> {
                    replPair.write(exists_key_buckets, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, exists_key_buckets, content);
            }
        }
    }

    @VisibleForTesting
    Repl.ReplReply stat_key_count_in_buckets(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, s_stat_key_count_in_buckets, NextStepContent.INSTANCE);
        }

        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        var bytes = keyLoader.getStatKeyCountInBucketsBytesToSlaveExists();
        log.warn("Repl master fetch stat key count in key buckets, slot={}, bytes length={}", slot, bytes.length);
        return Repl.reply(slot, replPair, s_stat_key_count_in_buckets, new RawBytesContent(bytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_stat_key_count_in_buckets(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, exists_key_buckets, NextStepContent.INSTANCE);
        }

        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        keyLoader.overwriteStatKeyCountInBucketsBytesFromMasterExists(contentBytes);

        // next step, fetch exists key buckets
        log.warn("Repl slave fetch data, go to step={}, slot={}", exists_key_buckets.name(), slot);
        var requestBytes = new byte[1 + 4 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.put((byte) 0);
        requestBuffer.putInt(0);
        var oneWalGroupSeq = keyLoader.getMetaOneWalGroupSeq((byte) 0, 0);
        requestBuffer.putLong(oneWalGroupSeq);
        return Repl.reply(slot, replPair, exists_key_buckets, new RawBytesContent(requestBytes));
    }

    @VisibleForTesting
    Repl.ReplReply meta_key_bucket_split_number(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, s_meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }

        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        var bytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists();
        log.warn("Repl master fetch meta key bucket split number, slot={}", slot);
        return Repl.reply(slot, replPair, s_meta_key_bucket_split_number, new RawBytesContent(bytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_meta_key_bucket_split_number(short slot, byte[] contentBytes) {
        if (replPair.isRedoSet()) {
            return Repl.reply(slot, replPair, stat_key_count_in_buckets, NextStepContent.INSTANCE);
        }

        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(contentBytes);

        // next step, fetch exists key buckets
        log.warn("Repl slave fetch data, go to step={}, slot={}", stat_key_count_in_buckets.name(), slot);
        return Repl.reply(slot, replPair, stat_key_count_in_buckets, NextStepContent.INSTANCE);
    }

    @VisibleForTesting
    Repl.ReplReply incremental_big_string(short slot, byte[] contentBytes) {
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
            bigStringBytes = new byte[0];
        }

        var responseBytes = new byte[8 + 4 + kenLength + bigStringBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putLong(uuid);
        responseBuffer.putInt(kenLength);
        responseBuffer.put(keyBytes);
        if (bigStringBytes.length > 0) {
            responseBuffer.put(bigStringBytes);
        }
        return Repl.reply(slot, replPair, s_incremental_big_string, new RawBytesContent(responseBytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_incremental_big_string(short slot, byte[] contentBytes) {
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
            if (replPair.isRedoSet()) {
                var oneSlot = localPersist.oneSlot(slot);
                var s = slot(key);
                var bigStringFiles = oneSlot.getBigStringFiles();
                bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
                log.warn("Repl slave fetch incremental big string done, uuid={}, key={}, slot={}", uuid, key, slot);
            } else {
                var s = slot(key);
                var oneSlot = localPersist.oneSlot(s.slot());
                oneSlot.asyncRun(() -> {
                    var bigStringFiles = oneSlot.getBigStringFiles();
                    bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
                    log.warn("Repl slave fetch incremental big string done, uuid={}, key={}, slot={}", uuid, key, s.slot());
                });
            }
        }

        replPair.doneFetchBigStringUuid(uuid);
        return Repl.emptyReply();
    }

    @VisibleForTesting
    Repl.ReplReply exists_big_string(short slot, byte[] contentBytes) {
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
        log.warn("Repl master fetch exists big string, slave sent id list={}, bucket index={}, slot={}", sentIdList, bucketIndex, slot);

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

    @VisibleForTesting
    Repl.ReplReply s_exists_big_string(short slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string
        if (NextStepContent.isNextStep(contentBytes)) {
            // next step, fetch meta key bucket split number
            log.warn("Repl slave fetch data, go to step={}, slot={}", meta_key_bucket_split_number.name(), slot);
            return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        // remote bucket index
        var bucketIndex = buffer.getInt();
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        var bucketsPerSlot = replPair.getRemoteReplProperties().bucketsPerSlot();

        if (bigStringCount == 0 && bucketIndex == bucketsPerSlot - 1) {
            // next step, fetch meta key bucket split number
            log.warn("Repl slave fetch data, go to step={}, slot={}", meta_key_bucket_split_number.name(), slot);
            return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }
        log.warn("Repl slave fetch exists big string, master sent big string count={}, bucket index={}, slot={}", bigStringCount, bucketIndex, slot);

        if (replPair.isRedoSet()) {
            var firstOneSlot = localPersist.currentThreadFirstOneSlot();
            final var s = findKeyMustInSlot(firstOneSlot.slot(), "exists_big_string_uuids_bucket_index_" + bucketIndex);
            var rhk = SGroup.getRedisSet(s, this);
            if (rhk == null) {
                rhk = new RedisHashKeys();
            }

            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var keyHash = buffer.getLong();
                var bigStringBytesLength = buffer.getInt();
                var offset = buffer.position();

                var slotThis = calcSlotByKeyHash(keyHash, ConfForGlobal.slotNumber);
                var bucketThis = KeyHash.bucketIndex(keyHash);
                var oneSlot = localPersist.oneSlot(slotThis);
                var bigStringFiles = oneSlot.getBigStringFiles();
                oneSlot.asyncRun(() -> {
                    bigStringFiles.writeBigStringBytes(uuid, bucketThis, keyHash, contentBytes, offset, bigStringBytesLength);
                });
                buffer.position(offset + bigStringBytesLength);

                rhk.add(uuid + "_" + keyHash);
            }

            // update exists big string local
            if (bigStringCount > 0) {
                RedisHashKeys finalRhk = rhk;
                firstOneSlot.asyncRun(() -> {
                    SGroup.saveRedisSet(finalRhk, s, this, dictMap);
                });
            }
        } else {
            var oneSlot = localPersist.oneSlot(slot);
            var bigStringFiles = oneSlot.getBigStringFiles();
            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var keyHash = buffer.getLong();
                var bigStringBytesLength = buffer.getInt();
                var offset = buffer.position();
                bigStringFiles.writeBigStringBytes(uuid, bucketIndex, keyHash, contentBytes, offset, bigStringBytesLength);
                buffer.position(offset + bigStringBytesLength);
            }
        }

        if (isSendAllOnce) {
            if (bucketIndex == bucketsPerSlot - 1) {
                // next step, fetch meta key bucket split number
                log.warn("Repl slave fetch data, go to step={}, slot={}", meta_key_bucket_split_number.name(), slot);
                return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
            } else {
                return fetchExistsBigString(slot, bucketIndex + 1);
            }
        } else {
            return fetchExistsBigString(slot, bucketIndex);
        }
    }

    private SlotWithKeyHash findKeyMustInSlot(short targetSlot, String prefix) {
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
        if (replPair.isRedoSet()) {
            // use a list to store exists big string uuid
            var firstOneSlot = localPersist.currentThreadFirstOneSlot();
            final var s = findKeyMustInSlot(firstOneSlot.slot(), "exists_big_string_uuids_bucket_index_" + bucketIndex);
            var rhk = SGroup.getRedisSet(s, this);
            var set = rhk != null ? rhk.getSet() : new HashSet<String>();
            if (set.isEmpty()) {
                return Repl.reply(slot, replPair, exists_big_string, NextStepContent.INSTANCE);
            }

            var rawBytes = new byte[4 + 16 * set.size()];
            var rawBuffer = ByteBuffer.wrap(rawBytes);
            rawBuffer.putInt(bucketIndex);
            for (var one : set) {
                var arr = one.split("_");
                var uuid = Long.parseLong(arr[0]);
                var keyHash = Long.parseLong(arr[1]);
                rawBuffer.putLong(uuid);
                rawBuffer.putLong(keyHash);
            }

            return Repl.reply(slot, replPair, exists_big_string, new RawBytesContent(rawBytes));
        }

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringFiles = oneSlot.getBigStringFiles();
        var idListLocal = bigStringFiles.getBigStringFileIdList(bucketIndex);
        if (idListLocal.isEmpty()) {
            return Repl.reply(slot, replPair, exists_big_string, NextStepContent.INSTANCE);
        }

        var rawBytes = new byte[4 + 16 * idListLocal.size()];
        var rawBuffer = ByteBuffer.wrap(rawBytes);
        rawBuffer.putInt(bucketIndex);
        for (var id : idListLocal) {
            rawBuffer.putLong(id.uuid());
            rawBuffer.putLong(id.keyHash());
        }

        return Repl.reply(slot, replPair, exists_big_string, new RawBytesContent(rawBytes));
    }

    @VisibleForTesting
    Repl.ReplReply exists_dict(short slot, byte[] contentBytes) {
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

    @VisibleForTesting
    Repl.ReplReply s_exists_dict(short slot, byte[] contentBytes) {
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

    @VisibleForTesting
    Repl.ReplReply exists_all_done(short slot, byte[] contentBytes) {
        // server received from client
        log.warn("Repl slave exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);
        return Repl.reply(slot, replPair, s_exists_all_done, NextStepContent.INSTANCE);
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_all_done(short slot, byte[] contentBytes) {
        // client received from server
        log.warn("Repl master reply exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);
        log.warn("Repl slave stats count for slave skip fetch={}, slot={}", replPair.getStatsCountForSlaveSkipFetchAsString(), slot);

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

    @VisibleForTesting
    Repl.ReplReply catch_up(short slot, byte[] contentBytes) {
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
        var onlyReadonlyResponseBytes = new byte[1 + 4 + 8];
        var onlyReadonlyResponseBuffer = ByteBuffer.wrap(onlyReadonlyResponseBytes);
        var onlyReadonlyResponseContent = new RawBytesContent(onlyReadonlyResponseBytes);

        onlyReadonlyResponseBuffer.put(isMasterReadonlyByte);

        var binlog = oneSlot.getBinlog();
        var currentFo = binlog.currentFileIndexAndOffset();
        onlyReadonlyResponseBuffer.putInt(currentFo.fileIndex());
        onlyReadonlyResponseBuffer.putLong(currentFo.offset());

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
        // 4 bytes for chunk current segment index
        // 4 bytes for need fetch file index
        // 8 bytes for need fetch offset
        // 4 bytes for current file index
        // 8 bytes for current offset
        var chunk = oneSlot.getChunk();

        var responseBytes = new byte[1 + 4 + 4 + 8 + 4 + 8 + 4 + readSegmentBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.put(isMasterReadonlyByte);
        responseBuffer.putInt(chunk.getSegmentIndex());
        responseBuffer.putInt(needFetchFileIndex);
        responseBuffer.putLong(needFetchOffset);
        responseBuffer.putInt(currentFo.fileIndex());
        responseBuffer.putLong(currentFo.offset());
        responseBuffer.putInt(readSegmentBytes.length);
        responseBuffer.put(readSegmentBytes);

        replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(new Binlog.FileIndexAndOffset(needFetchFileIndex,
                needFetchOffset + readSegmentBytes.length));
        return Repl.reply(slot, replPair, s_catch_up, new RawBytesContent(responseBytes));
    }

    private long catchUpLoopCount = 0L;

    @VisibleForTesting
    Repl.ReplReply s_catch_up(short slot, byte[] contentBytes) {
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
        var chunkCurrentSegmentIndex = buffer.getInt();
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
        if (isCatchUpToCurrentFile) {
            var diffOffset = masterCurrentOffset - fetchedOffset - skipBytesN;
            if (diffOffset < ConfForSlot.global.confRepl.catchUpOffsetMinDiff) {
                try {
                    if (!oneSlot.isCanRead()) {
                        oneSlot.setCanRead(true);
                        log.warn("Repl slave can read now as already catch up nearly to master latest, slot={}", slot);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(new Binlog.FileIndexAndOffset(fetchedFileIndex, fetchedOffset + readSegmentLength));

//        oneSlot.setMetaChunkSegmentIndexInt(chunkCurrentSegmentIndex, true);
//        var chunk = oneSlot.getChunk();
//        chunk.setSegmentIndex(chunkCurrentSegmentIndex);

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
        oneSlot.asyncRun(() -> {
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
