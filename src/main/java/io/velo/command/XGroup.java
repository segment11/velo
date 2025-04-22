package io.velo.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.SettablePromise;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.persist.*;
import io.velo.repl.Binlog;
import io.velo.repl.Repl;
import io.velo.repl.ReplPair;
import io.velo.repl.ReplType;
import io.velo.repl.content.*;
import io.velo.repl.incremental.XSkipApply;
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.*;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static io.velo.repl.ReplType.*;

public class XGroup extends BaseCommand {
    public XGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
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
                return new BulkReply(jsonStr.getBytes());
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
                return new BulkReply(replPairAsMaster.getHostAndPort().getBytes());
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
            case exists_keys_for_analysis -> exists_keys_for_analysis(slot, contentBytes);
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
            case s_exists_keys_for_analysis -> s_exists_keys_for_analysis(slot, contentBytes);
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

        var b = new byte[contentBytes.length - 8];
        buffer.get(b);
        var netListenAddresses = new String(b);

        var array = netListenAddresses.split(":");
        var host = array[0];
        var port = Integer.parseInt(array[1]);

        var oneSlot = localPersist.oneSlot(slot);
        if (replPair == null) {
            replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
            replPair.increaseStatsCountForReplType(hello);
        }

        log.warn("Repl master handle hello: slave uuid={}, net listen addresses={}, slot={}", slaveUuid, netListenAddresses, slot);

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

        // should not happen
        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl slave handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}, slot={}",
                    replPair.getSlaveUuid(), slaveUuid, slot);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl slave handle hi: slave uuid={}, master uuid={}, slot={}", slaveUuid, masterUuid, slot);

        if (slot == localPersist.firstOneSlot().slot()) {
            localPersist.setAsSlaveFirstSlotFetchedExistsAllDone(false);
        }

        var oneSlot = localPersist.oneSlot(slot);
        // after exist all done, when catch up, XOneWalGroupSeq will update chunk segment index
        oneSlot.setMetaChunkSegmentIndexInt(currentSegmentIndex);
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
        // cluster mode, need change to the first slot, todo
        var firstSlot = localPersist.oneSlots()[0].slot();
        if (firstSlot != slot) {
            log.warn("Repl slave skip fetch exists dict, slot={}", slot);
            return fetchExistsBigString(slot, oneSlot);
        }

        // begin to fetch exist data from master
        // first fetch dict
        var dictMap = DictMap.getInstance();
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

    // use rocksdb backup engine better, but need copy too many files
    Reply exists_keys_for_analysis(short slot, byte[] contentBytes) {
        // server received from client
        if (slot != localPersist.firstOneSlot().slot()) {
            // not first slot, skip
            log.warn("Repl master skip iterate keys when slave slot is not the first slot, slot={}", slot);
            return Repl.reply(slot, replPair, s_exists_keys_for_analysis, new RawBytesContent(new byte[2]));
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var beginKeyBytesLength = buffer.getInt();
        byte[] beginKeyBytes = null;
        if (beginKeyBytesLength != 0) {
            beginKeyBytes = new byte[beginKeyBytesLength];
            buffer.get(beginKeyBytes);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        var keyAnalysisHandler = localPersist.getIndexHandlerPool().getKeyAnalysisHandler();
        var os = new ByteArrayOutputStream(ConfForSlot.global.confRepl.iterateKeysOneBatchSize * 32);
        var dataOs = new DataOutputStream(os);
        var f = keyAnalysisHandler.iterateKeys(beginKeyBytes, ConfForSlot.global.confRepl.iterateKeysOneBatchSize, false,
                (keyBytes, valueBytesAsInt) -> {
                    try {
                        dataOs.writeShort((short) keyBytes.length);
                        dataOs.write(keyBytes);
                        dataOs.writeInt(valueBytesAsInt);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        f.whenComplete((ignore, e) -> {
            if (e != null) {
                var message = "Repl master iterate keys error, slot=" + slot;
                log.error(message, e);
                finalPromise.set(Repl.error(slot, replPair, message + ", error=" + e.getMessage()));
                return;
            }

            if (dataOs.size() == 0) {
                log.warn("Repl master has no keys to analysis.");
                finalPromise.set(Repl.reply(slot, replPair, s_exists_keys_for_analysis, new RawBytesContent(new byte[2])));
            } else {
                finalPromise.set(Repl.reply(slot, replPair, s_exists_keys_for_analysis, new RawBytesContent(os.toByteArray())));
            }
        });

        return asyncReply;
    }

    private Repl.ReplReply nextStepToGetFirstChunkSegment(short slot) {
        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentFlagSeq = oneSlot.getMetaChunkSegmentFlagSeq();
        log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_chunk_segments.name(), slot);
        var segmentCount = FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;
        var metaBytes = metaChunkSegmentFlagSeq.getOneBatch(0, segmentCount);
        var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
        return Repl.reply(slot, replPair, exists_chunk_segments, content);
    }

    Repl.ReplReply s_exists_keys_for_analysis(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        if (buffer.getShort() == 0) {
            return nextStepToGetFirstChunkSegment(slot);
        }

        var keyAnalysisHandler = localPersist.getIndexHandlerPool().getKeyAnalysisHandler();
        var f = keyAnalysisHandler.addBatch(contentBytes);
        try {
            var result = f.get();
            if (result.keyCount() < ConfForSlot.global.confRepl.iterateKeysOneBatchSize) {
                return nextStepToGetFirstChunkSegment(slot);
            } else {
                var lastKeyBytes = result.lastKeyBytes();
                var requestBytes = new byte[4 + lastKeyBytes.length];
                var requestBuffer = ByteBuffer.wrap(requestBytes);
                requestBuffer.putInt(lastKeyBytes.length);
                requestBuffer.put(lastKeyBytes);
                return Repl.reply(slot, replPair, exists_keys_for_analysis, new RawBytesContent(requestBytes));
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Repl.ReplReply exists_wal(short slot, byte[] contentBytes) {
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
            if (groupIndex % 100 == 0) {
                log.warn("Repl master skip send wal exists bytes, group index={}, slot={}", groupIndex, slot);
            }

            // only reply group index, no need to send wal exists bytes
            var responseBytes = new byte[4];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.putInt(groupIndex);
            return Repl.reply(slot, replPair, s_exists_wal, new RawBytesContent(responseBytes));
        } else {
            if (groupIndex % 100 == 0) {
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
            if (groupIndex % 100 == 0) {
                log.info("Repl slave skip update wal exists bytes, group index={}, slot={}", groupIndex, slot);
            }
        }

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (groupIndex == walGroupNumber - 1) {
            log.warn("Repl slave fetch exists all done after fetch wal when slot is not the first slot, slot={}", slot);
            return Repl.reply(slot, replPair, exists_all_done, NextStepContent.INSTANCE);
        } else {
            var nextGroupIndex = groupIndex + 1;
            if (nextGroupIndex % 100 == 0) {
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

    @VisibleForTesting
    Repl.ReplReply exists_chunk_segments(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();

        if (beginSegmentIndex % (segmentCount * 10) == 0) {
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

        var responseBytes = new byte[4 + 4 + 4 + masterMetaBytes.length + 4 + chunkSegmentsBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(beginSegmentIndex);
        responseBuffer.putInt(segmentCount);
        responseBuffer.putInt(masterMetaBytes.length);
        responseBuffer.put(masterMetaBytes);

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

    @VisibleForTesting
    Repl.ReplReply s_exists_chunk_segments(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();
        // segmentCount == FdReadWrite.REPL_ONCE_INNER_COUNT

        if (beginSegmentIndex % (segmentCount * 10) == 0) {
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
                log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_wal.name(), slot);
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
                            beginSegmentIndex, segmentCount);
                }
            } else {
                var chunkSegmentsBytes = new byte[chunkSegmentsLength];
                buffer.get(chunkSegmentsBytes);

                var realSegmentCount = chunkSegmentsLength / ConfForSlot.global.confChunk.segmentLength;
                oneSlot.writeChunkSegmentsFromMasterExists(chunkSegmentsBytes,
                        beginSegmentIndex, realSegmentCount);
            }
        } else {
            replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_chunk_segments);
        }

        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        boolean isLastBatch = maxSegmentNumber == beginSegmentIndex + segmentCount;
        if (isLastBatch) {
            // next step, fetch exists wal
            log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_wal.name(), slot);
            return Repl.reply(slot, replPair, exists_wal, requestExistsWal(oneSlot, 0));
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentCount;
            var nextBatchMetaBytes = metaChunkSegmentFlagSeq.getOneBatch(nextBatchBeginSegmentIndex, segmentCount);
            var content = new ToMasterExistsChunkSegments(nextBatchBeginSegmentIndex, segmentCount, nextBatchMetaBytes);

            if (nextBatchBeginSegmentIndex % (segmentCount * 10) == 0) {
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
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var splitIndex = buffer.get();
        var beginBucketIndex = buffer.getInt();
        var oneWalGroupSeq = buffer.getLong();

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (beginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
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
        if (beginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
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
            log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_keys_for_analysis.name(), slot);
            var requestBytes = new byte[4];
            return Repl.reply(slot, replPair, exists_keys_for_analysis, new RawBytesContent(requestBytes));
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

            if (nextBeginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
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
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        var bytes = keyLoader.getStatKeyCountInBucketsBytesToSlaveExists();
        log.warn("Repl master fetch stat key count in key buckets, slot={}", slot);
        return Repl.reply(slot, replPair, s_stat_key_count_in_buckets, new RawBytesContent(bytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_stat_key_count_in_buckets(short slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        keyLoader.overwriteStatKeyCountInBucketsBytesFromMasterExists(contentBytes);

        // next step, fetch exists key buckets
        log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_key_buckets.name(), slot);
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
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var keyLoader = oneSlot.getKeyLoader();
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(contentBytes);

        // next step, fetch exists key buckets
        log.warn("Repl slave fetch data, go to step: {}, slot={}", stat_key_count_in_buckets.name(), slot);
        return Repl.reply(slot, replPair, stat_key_count_in_buckets, NextStepContent.INSTANCE);
    }

    @VisibleForTesting
    Repl.ReplReply incremental_big_string(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        log.warn("Repl master fetch incremental big string, uuid={}, slot={}", uuid, slot);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringFiles = oneSlot.getBigStringFiles();
        var bigStringBytes = bigStringFiles.getBigStringBytes(uuid);
        if (bigStringBytes == null) {
            bigStringBytes = new byte[0];
        }

        var responseBytes = new byte[8 + bigStringBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putLong(uuid);
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

        // master big string file already deleted, skip
        if (contentBytes.length != 8) {
            var bigStringBytes = new byte[contentBytes.length - 8];
            buffer.get(bigStringBytes);

            var oneSlot = localPersist.oneSlot(slot);
            var bigStringFiles = oneSlot.getBigStringFiles();
            bigStringFiles.writeBigStringBytes(uuid, "ignore", bigStringBytes);
            log.warn("Repl slave fetch incremental big string done, uuid={}, slot={}", uuid, slot);
        }

        replPair.doneFetchBigStringUuid(uuid);
        return Repl.emptyReply();
    }

    @VisibleForTesting
    Repl.ReplReply exists_big_string(short slot, byte[] contentBytes) {
        // server received from client
        // send back exists big string to client, with flag can do next step
        // client already persisted big string uuid, send to client exclude sent big string
        var sentUuidList = new ArrayList<Long>();
        if (contentBytes.length >= 8) {
            var sentUuidCount = contentBytes.length / 8;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentUuidCount; i++) {
                sentUuidList.add(buffer.getLong());
            }
        }
        log.warn("Repl master fetch exists big string, slave sent uuid list={}, slot={}", sentUuidList, slot);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringFiles = oneSlot.getBigStringFiles();
        var uuidListInMaster = bigStringFiles.getBigStringFileUuidList();
        if (uuidListInMaster.isEmpty()) {
            return Repl.reply(slot, replPair, s_exists_big_string, NextStepContent.INSTANCE);
        }

        var bigStringDir = oneSlot.getBigStringDir();
        var toSlaveExistsBigString = new ToSlaveExistsBigString(bigStringDir, uuidListInMaster, sentUuidList);
        return Repl.reply(slot, replPair, s_exists_big_string, toSlaveExistsBigString);
    }

    @VisibleForTesting
        // need delete local big string file if not exists in master, todo
    Repl.ReplReply s_exists_big_string(short slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string
        if (NextStepContent.isNextStep(contentBytes)) {
            // next step, fetch meta key bucket split number
            log.warn("Repl slave fetch data, go to step: {}, slot={}", meta_key_bucket_split_number.name(), slot);
            return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (bigStringCount == 0) {
            // next step, fetch meta key bucket split number
            log.warn("Repl slave fetch data, go to step: {}, slot={}", meta_key_bucket_split_number.name(), slot);
            return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }
        log.warn("Repl slave fetch exists big string, master sent big string count={}, slot={}", bigStringCount, slot);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringDir = oneSlot.getBigStringDir();
        try {
            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var bigStringBytesLength = buffer.getInt();
                var bigStringBytes = new byte[bigStringBytesLength];
                buffer.get(bigStringBytes);

                var uuidAsFileName = String.valueOf(uuid);
                var file = new File(bigStringDir, uuidAsFileName);
                FileUtils.writeByteArrayToFile(file, bigStringBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (isSendAllOnce) {
            // next step, fetch meta key bucket split number
            log.warn("Repl slave fetch data, go to step: {}, slot={}", meta_key_bucket_split_number.name(), slot);
            return Repl.reply(slot, replPair, meta_key_bucket_split_number, NextStepContent.INSTANCE);
        } else {
            return fetchExistsBigString(slot, oneSlot);
        }
    }

    @VisibleForTesting
    Repl.ReplReply fetchExistsBigString(short slot, OneSlot oneSlot) {
        var bigStringFiles = oneSlot.getBigStringFiles();
        var uuidListLocal = bigStringFiles.getBigStringFileUuidList();
        if (uuidListLocal.isEmpty()) {
            return Repl.reply(slot, replPair, exists_big_string, NextStepContent.INSTANCE);
        }

        var rawBytes = new byte[8 * uuidListLocal.size()];
        var rawBuffer = ByteBuffer.wrap(rawBytes);
        for (var uuid : uuidListLocal) {
            rawBuffer.putLong(uuid);
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

        var dictMap = DictMap.getInstance();
        var cacheDictCopy = dictMap.getCacheDictCopy();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        // master always send global zstd dict to slave
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

        var dictMap = DictMap.getInstance();
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
        log.warn("Repl slave fetch data, go to step: {}, slot={}", exists_big_string.name(), slot);
        return fetchExistsBigString(slot, oneSlot);
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

    @VisibleForTesting
    Repl.ReplReply s_catch_up(short slot, byte[] contentBytes) {
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
            if (fetchedOffset == 0) {
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

        final var targetSlot = replPairAsSlave.getSlot();
        var oneSlot = LocalPersist.getInstance().oneSlot(targetSlot);
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
                var jedisPool = JedisPoolHolder.getInstance().create(replPairAsSlave.getHost(), replPairAsSlave.getPort());
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
