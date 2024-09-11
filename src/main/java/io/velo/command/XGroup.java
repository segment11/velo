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
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.*;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

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

        slotWithKeyHashList.add(new SlotWithKeyHash(slot, 0, 0L));
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
                log.error("Repl master handle x_repl x_catch_up error, slot=" + slot, e);
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
        var replType = ReplType.fromCode(data[2][0]);
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
            case exists_reverse_index -> exists_reverse_index(slot, contentBytes);
            case s_exists_reverse_index -> s_exists_reverse_index(slot, contentBytes);
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

        // start binlog
        try {
            oneSlot.getDynConfig().setBinlogOn(true);
            log.warn("Repl master start binlog, master uuid={}, slot={}", replPair.getMasterUuid(), slot);
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: start binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
        }
        log.warn("Repl master handle hello: slave uuid={}, net listen addresses={}, slot={}", slaveUuid, netListenAddresses, slot);

        var binlog = oneSlot.getBinlog();
        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();
        var earliestFileIndexAndOffset = binlog.earliestFileIndexAndOffset();
        var content = new Hi(slaveUuid, oneSlot.getMasterUuid(), currentFileIndexAndOffset, earliestFileIndexAndOffset,
                oneSlot.getChunk().currentSegmentIndex());
        return Repl.reply(slot, replPair, hi, content);
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

        if (slot == 0) {
            localPersist.setAsSlaveSlot0FetchedExistsAllDone(false);
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
                    return Repl.reply(slot, replPair, ReplType.catch_up, content);
                }

                // last fetched binlog segment is not a complete segment, need to re-fetch this segment
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                if (marginLastUpdatedOffset != lastUpdatedOffset) {
                    var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                    return Repl.reply(slot, replPair, ReplType.catch_up, content);
                }

                var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, lastUpdatedOffset, lastUpdatedOffset);
                return Repl.reply(slot, replPair, ReplType.catch_up, content);
            }
        }

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false,
                currentFileIndex, currentOffset);
        log.warn("Repl slave set master binlog current/latest file index and offset for incremental catch up, master binlog file index={}, offset={}, slot={}",
                currentFileIndex, currentOffset, slot);
        log.warn("Repl slave begin fetch all exists data from master, slot={}", slot);

        // dict is global, only first slot do fetch
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
            return Repl.reply(slot, replPair, ReplType.exists_dict, NextStepContent.INSTANCE);
        } else {
            var rawBytes = new byte[4 * cacheDictBySeqCopy.size()];
            var rawBuffer = ByteBuffer.wrap(rawBytes);
            for (var entry : cacheDictBySeqCopy.entrySet()) {
                var seq = entry.getKey();
                rawBuffer.putInt(seq);
            }

            return Repl.reply(slot, replPair, ReplType.exists_dict, new RawBytesContent(rawBytes));
        }
    }

    @VisibleForTesting
    static final int INDEX_EXISTS_WORDS_ONCE_READ_LENGTH = 1024 * 1024;
    @VisibleForTesting
    static final byte reverseIndexByteAsForMetaIndexWords = 0;
    @VisibleForTesting
    static final byte reverseIndexByteAsForChunk = 1;

    @VisibleForTesting
    Reply exists_reverse_index(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var isMetaIndexWords = buffer.get() == reverseIndexByteAsForMetaIndexWords;
        var indexWorkerId = buffer.get();
        var beginOffset = buffer.getInt();

        if (beginOffset % 1000 == 0) {
            log.warn("Repl master fetch exists reverse index, is meta index words={}, index worker id={}, begin offset={}, slot={}",
                    isMetaIndexWords, indexWorkerId, beginOffset, slot);
        }

        // always slot == 0
        var oneSlot = localPersist.oneSlot(slot);
        var readBytesArray = new byte[1][];

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        oneSlot.submitIndexToTargetWorkerJobRun(indexWorkerId, (indexHandler -> {
            if (isMetaIndexWords) {
                readBytesArray[0] = indexHandler.metaIndexWordsReadOneBatch(beginOffset, INDEX_EXISTS_WORDS_ONCE_READ_LENGTH);
            } else {
                // begin offset is segment index for chunk
                readBytesArray[0] = indexHandler.chunkReadOneSegment(beginOffset);
            }
        })).whenComplete((ignore, e) -> {
            if (e != null) {
                var message = "Repl master read meta index words error, slot=" + slot;
                log.error(message, e);
                finalPromise.set(Repl.error(slot, replPair, message + ", error=" + e.getMessage()));
                return;
            }

            var readBytes = readBytesArray[0];

            var responseBytes = new byte[1 + 1 + 4 + readBytes.length];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.put(isMetaIndexWords ? reverseIndexByteAsForMetaIndexWords : reverseIndexByteAsForChunk);
            responseBuffer.put(indexWorkerId);
            responseBuffer.putInt(beginOffset);
            responseBuffer.put(readBytes);

            finalPromise.set(Repl.reply(slot, replPair, s_exists_reverse_index, new RawBytesContent(responseBytes)));
        });

        return asyncReply;
    }

    Reply s_exists_reverse_index(short slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var isMetaIndexWords = buffer.get() == reverseIndexByteAsForMetaIndexWords;
        var indexWorkerId = buffer.get();
        var beginOffset = buffer.getInt();

        if (beginOffset % 1000 == 0) {
            log.warn("Repl slave fetch exists reverse index, is meta index words={}, index worker id={}, begin offset={}, slot={}",
                    isMetaIndexWords, indexWorkerId, beginOffset, slot);
        }

        var remainingLength = buffer.remaining();
        var bytes = new byte[remainingLength];
        buffer.get(bytes);

        // always slot == 0
        var oneSlot = localPersist.oneSlot(slot);

        oneSlot.submitIndexToTargetWorkerJobRun(indexWorkerId, (indexHandler -> {
            if (isMetaIndexWords) {
                indexHandler.metaIndexWordsWriteOneBatch(beginOffset, bytes);
            } else {
                indexHandler.chunkWriteOneSegment(beginOffset, bytes);
            }
        })).whenComplete((ignore, e) -> {
            if (e != null) {
                var message = "Repl slave write meta index words / index chunk error, slot=" + slot;
                log.error(message, e);
                replPair.write(error, new RawBytesContent((message + ", error=" + e.getMessage()).getBytes()));
                return;
            }

            var requestBytes = new byte[1 + 1 + 4];
            var requestBuffer = ByteBuffer.wrap(requestBytes);

            if (isMetaIndexWords) {
                // meta index words total file length is not exact n MB
                var isMetaIndexWordsDone = remainingLength < INDEX_EXISTS_WORDS_ONCE_READ_LENGTH;
                if (isMetaIndexWordsDone) {
                    // has more index worker
                    if (indexWorkerId < ConfForGlobal.indexWorkers - 1) {
                        var nextIndexWorkerId = (byte) (indexWorkerId + 1);
                        requestBuffer.put(reverseIndexByteAsForMetaIndexWords);
                        requestBuffer.put(nextIndexWorkerId);
                        requestBuffer.putInt(0);
                    } else {
                        // fetch index chunk
                        requestBuffer.put(reverseIndexByteAsForChunk);
                        requestBuffer.put((byte) 0);
                        requestBuffer.putInt(0);
                    }
                } else {
                    // fetch next batch
                    requestBuffer.put(reverseIndexByteAsForMetaIndexWords);
                    requestBuffer.put(indexWorkerId);
                    requestBuffer.putInt(beginOffset + INDEX_EXISTS_WORDS_ONCE_READ_LENGTH);
                }

                replPair.write(exists_reverse_index, new RawBytesContent(requestBytes));
                return;
            }

            // bytes.length == 1 -> read empty segment
            var isIndexChunkDone = bytes.length == 1 ||
                    beginOffset == localPersist.getIndexHandlerPool().getChunkMaxSegmentNumber() - 1;
            if (isIndexChunkDone) {
                // has more index worker
                if (indexWorkerId < ConfForGlobal.indexWorkers - 1) {
                    var nextIndexWorkerId = (byte) (indexWorkerId + 1);
                    requestBuffer.put(reverseIndexByteAsForChunk);
                    requestBuffer.put(nextIndexWorkerId);
                    requestBuffer.putInt(0);

                    replPair.write(exists_reverse_index, new RawBytesContent(requestBytes));
                } else {
                    replPair.write(exists_all_done, NextStepContent.INSTANCE);
                }
            } else {
                // fetch next segment
                requestBuffer.put(reverseIndexByteAsForChunk);
                requestBuffer.put(indexWorkerId);
                requestBuffer.putInt(beginOffset + 1);

                replPair.write(exists_reverse_index, new RawBytesContent(requestBytes));
            }
        });

        return Repl.emptyReply();
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
            return Repl.reply(slot, replPair, ReplType.s_exists_wal, new RawBytesContent(responseBytes));
        } else {
            if (groupIndex % 100 == 0) {
                log.warn("Repl master will fetch exists wal, group index={}, slot={}", groupIndex, slot);
            }
        }

        try {
            var toSlaveBytes = targetWal.toSlaveExistsOneWalGroupBytes();
            return Repl.reply(slot, replPair, ReplType.s_exists_wal, new RawBytesContent(toSlaveBytes));
        } catch (IOException e) {
            log.error("Repl master get wal exists bytes error, slot=" + slot + ", group index=" + groupIndex, e);
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
                log.error("Repl slave update wal exists bytes error, slot=" + slot + ", group index=" + groupIndex, e);
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
            if (slot != 0) {
                return Repl.reply(slot, replPair, exists_all_done, NextStepContent.INSTANCE);
            } else {
                // only slot 0 fetch exists reverse index data
                var requestBytes = new byte[1 + 1 + 4];
                var requestBuffer = ByteBuffer.wrap(requestBytes);
                requestBuffer.put(reverseIndexByteAsForMetaIndexWords);
                // begin with the first index worker
                requestBuffer.put((byte) 0);
                // begin offset
                requestBuffer.putInt(0);
                return Repl.reply(slot, replPair, exists_reverse_index, new RawBytesContent(requestBytes));
            }
        } else {
            var nextGroupIndex = groupIndex + 1;
            if (nextGroupIndex % 100 == 0) {
                // delay
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_wal, requestExistsWal(oneSlot, nextGroupIndex));
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_wal, requestExistsWal(oneSlot, nextGroupIndex));
            }
        }
    }

    private RawBytesContent requestExistsWal(OneSlot oneSlot, int groupIndex) {
        // 4 bytes int for group index, 8 bytes long for last seq, 8 bytes long for last seq of short value
        var requestBytes = new byte[4 + 8 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.putInt(groupIndex);

        var targetWal = oneSlot.getWalByGroupIndex(groupIndex);
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
        var masterMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(beginSegmentIndex, segmentCount);
        if (ToMasterExistsChunkSegments.isSlaveSameForThisBatch(masterMetaBytes, contentBytes)) {
            var responseBytes = new byte[4 + 4];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.putInt(beginSegmentIndex);
            responseBuffer.putInt(segmentCount);
            return Repl.reply(slot, replPair, ReplType.s_exists_chunk_segments, new RawBytesContent(responseBytes));
        }

        var chunkSegmentsBytes = oneSlot.preadForRepl(beginSegmentIndex);
        if (chunkSegmentsBytes == null) {
            chunkSegmentsBytes = new byte[0];
        }

        var responseBytes = new byte[4 + 4 + 4 + masterMetaBytes.length + 4 + chunkSegmentsBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(beginSegmentIndex);
        responseBuffer.putInt(segmentCount);
        responseBuffer.putInt(masterMetaBytes.length);
        responseBuffer.put(masterMetaBytes);
        responseBuffer.putInt(chunkSegmentsBytes.length);
        if (chunkSegmentsBytes.length > 0) {
            responseBuffer.put(chunkSegmentsBytes);
        }

        return Repl.reply(slot, replPair, ReplType.s_exists_chunk_segments, new RawBytesContent(responseBytes));
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
        // content bytes length == 8 -> slave is same for this batch, skip
        if (contentBytes.length != 8) {
            var metaBytesLength = buffer.getInt();
            var metaBytes = new byte[metaBytesLength];
            buffer.get(metaBytes);
            oneSlot.getMetaChunkSegmentFlagSeq().overwriteOneBatch(metaBytes, beginSegmentIndex, segmentCount);

            var chunkSegmentsLength = buffer.getInt();
            if (chunkSegmentsLength == 0) {
                oneSlot.writeChunkSegmentsFromMasterExists(ConfForSlot.global.confChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE,
                        beginSegmentIndex, segmentCount);
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
            return Repl.reply(slot, replPair, exists_wal, requestExistsWal(oneSlot, 0));
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentCount;
            var nextBatchMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(nextBatchBeginSegmentIndex, segmentCount);
            var content = new ToMasterExistsChunkSegments(nextBatchBeginSegmentIndex, segmentCount, nextBatchMetaBytes);

            if (nextBatchBeginSegmentIndex % (segmentCount * 10) == 0) {
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_chunk_segments, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
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
        var maxSplitNumber = oneSlot.getKeyLoader().maxSplitNumberForRepl();

        byte[] bytes = null;
        var masterOneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq(splitIndex, beginBucketIndex);
        var isSkip = masterOneWalGroupSeq == oneWalGroupSeq;
        if (!isSkip) {
            bytes = oneSlot.getKeyLoader().readBatchInOneWalGroup(splitIndex, beginBucketIndex);
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

        return Repl.reply(slot, replPair, ReplType.s_exists_key_buckets, new RawBytesContent(responseBytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_key_buckets(short slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);

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
            oneSlot.getKeyLoader().writeSharedBytesList(sharedBytesList, beginBucketIndex);
            oneSlot.getKeyLoader().setMetaOneWalGroupSeq(splitIndex, beginBucketIndex, masterOneWalGroupSeq);
        } else {
            replPair.increaseStatsCountWhenSlaveSkipFetch(s_exists_key_buckets);
        }

        boolean isLastBatchInThisSplit = beginBucketIndex == ConfForSlot.global.confBucket.bucketsPerSlot - oneChargeBucketNumber;
        var isAllReceived = splitIndex == maxSplitNumber - 1 && isLastBatchInThisSplit;
        if (isAllReceived) {
            log.warn("Repl slave fetch all key buckets done, slot={}", slot);

            // next step, fetch exists chunk segments
            var segmentCount = FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;
            var metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
            return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
        } else {
            var nextSplitIndex = isLastBatchInThisSplit ? splitIndex + 1 : splitIndex;
            var nextBeginBucketIndex = isLastBatchInThisSplit ? 0 : beginBucketIndex + oneChargeBucketNumber;

            var requestBytes = new byte[1 + 4 + 8];
            var requestBuffer = ByteBuffer.wrap(requestBytes);
            requestBuffer.put((byte) nextSplitIndex);
            requestBuffer.putInt(nextBeginBucketIndex);
            var slaveOneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq((byte) nextSplitIndex, nextBeginBucketIndex);
            requestBuffer.putLong(slaveOneWalGroupSeq);
            var content = new RawBytesContent(requestBytes);

            if (nextBeginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_key_buckets, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_key_buckets, content);
            }
        }
    }

    @VisibleForTesting
    Repl.ReplReply stat_key_count_in_buckets(short slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getStatKeyCountInBucketsBytesToSlaveExists();
        log.warn("Repl master fetch stat key count in key buckets, slot={}", slot);
        return Repl.reply(slot, replPair, ReplType.s_stat_key_count_in_buckets, new RawBytesContent(bytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_stat_key_count_in_buckets(short slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteStatKeyCountInBucketsBytesFromMasterExists(contentBytes);
        log.warn("Repl slave fetch stat key count in key buckets done, slot={}", slot);

        // next step, fetch exists key buckets
        var requestBytes = new byte[1 + 4 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.put((byte) 0);
        requestBuffer.putInt(0);
        var oneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq((byte) 0, 0);
        requestBuffer.putLong(oneWalGroupSeq);
        return Repl.reply(slot, replPair, ReplType.exists_key_buckets, new RawBytesContent(requestBytes));
    }

    @VisibleForTesting
    Repl.ReplReply meta_key_bucket_split_number(short slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getMetaKeyBucketSplitNumberBytesToSlaveExists();
        log.warn("Repl master fetch meta key bucket split number, slot={}", slot);
        return Repl.reply(slot, replPair, ReplType.s_meta_key_bucket_split_number, new RawBytesContent(bytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_meta_key_bucket_split_number(short slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(contentBytes);
        log.warn("Repl slave fetch meta key bucket split number done, slot={}", slot);

        // next step, fetch exists key buckets
        return Repl.reply(slot, replPair, ReplType.stat_key_count_in_buckets, NextStepContent.INSTANCE);
    }

    @VisibleForTesting
    Repl.ReplReply incremental_big_string(short slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        log.warn("Repl master fetch incremental big string, uuid={}, slot={}", uuid, slot);

        var oneSlot = localPersist.oneSlot(slot);

        var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid);
        if (bigStringBytes == null) {
            bigStringBytes = new byte[0];
        }

        var responseBytes = new byte[8 + bigStringBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putLong(uuid);
        if (bigStringBytes.length > 0) {
            responseBuffer.put(bigStringBytes);
        }
        return Repl.reply(slot, replPair, ReplType.s_incremental_big_string, new RawBytesContent(responseBytes));
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
            oneSlot.getBigStringFiles().writeBigStringBytes(uuid, "ignore", bigStringBytes);
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
        var uuidListInMaster = oneSlot.getBigStringFiles().getBigStringFileUuidList();
        if (uuidListInMaster.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.s_exists_big_string, NextStepContent.INSTANCE);
        }

        var toSlaveExistsBigString = new ToSlaveExistsBigString(oneSlot.getBigStringDir(), uuidListInMaster, sentUuidList);
        return Repl.reply(slot, replPair, ReplType.s_exists_big_string, toSlaveExistsBigString);
    }

    @VisibleForTesting
        // need delete local big string file if not exists in master, todo
    Repl.ReplReply s_exists_big_string(short slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string, next step
        if (NextStepContent.isNextStep(contentBytes)) {
            log.warn("Repl slave fetch all big string done, slot={}", slot);
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, NextStepContent.INSTANCE);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (bigStringCount == 0) {
            log.warn("Repl slave fetch all big string done, slot={}", slot);
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, NextStepContent.INSTANCE);
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
            log.warn("Repl slave fetch all big string done, slot={}", slot);
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, NextStepContent.INSTANCE);
        } else {
            return fetchExistsBigString(slot, oneSlot);
        }
    }

    @VisibleForTesting
    Repl.ReplReply fetchExistsBigString(short slot, OneSlot oneSlot) {
        var uuidListLocal = oneSlot.getBigStringFiles().getBigStringFileUuidList();
        if (uuidListLocal.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.exists_big_string, NextStepContent.INSTANCE);
        }

        var rawBytes = new byte[8 * uuidListLocal.size()];
        var rawBuffer = ByteBuffer.wrap(rawBytes);
        for (var uuid : uuidListLocal) {
            rawBuffer.putLong(uuid);
        }

        return Repl.reply(slot, replPair, ReplType.exists_big_string, new RawBytesContent(rawBytes));
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
        return Repl.reply(slot, replPair, ReplType.s_exists_dict, content);
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
        log.warn("Repl slave fetch all dict done, slot={}", slot);
        return fetchExistsBigString(slot, oneSlot);
    }

    @VisibleForTesting
    Repl.ReplReply exists_all_done(short slot, byte[] contentBytes) {
        // server received from client
        log.warn("Repl slave exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);
        return Repl.reply(slot, replPair, ReplType.s_exists_all_done, NextStepContent.INSTANCE);
    }

    @VisibleForTesting
    Repl.ReplReply s_exists_all_done(short slot, byte[] contentBytes) {
        // client received from server
        log.warn("Repl master reply exists/meta fetch all done, slave uuid={}, {}, slot={}",
                replPair.getSlaveUuid(), replPair.getHostAndPort(), slot);
        log.warn("Repl slave stats count for slave skip fetch={}, slot={}", replPair.getStatsCountForSlaveSkipFetchAsString(), slot);

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.setChunkSegmentIndexFromMeta();

        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        // update exists data all fetched done
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                lastUpdatedFileIndex, lastUpdatedOffset);

        if (slot == 0) {
            localPersist.setAsSlaveSlot0FetchedExistsAllDone(true);
        }

        // begin incremental data catch up
        var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
        var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
        return Repl.reply(slot, replPair, ReplType.catch_up, content);
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
        var onlyReadonlyResponseContent = new RawBytesContent(new byte[]{isMasterReadonlyByte});

        var binlog = oneSlot.getBinlog();
        if (needFetchOffset != lastUpdatedOffset) {
            // check if slave already catch up to last binlog segment offset
            var fo = binlog.currentFileIndexAndOffset();
            if (fo.fileIndex() == needFetchFileIndex && fo.offset() == lastUpdatedOffset) {
                replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(fo);
                replPair.setAllCaughtUp(true);
                return Repl.reply(slot, replPair, ReplType.s_catch_up, onlyReadonlyResponseContent);
            }
        }

        byte[] readSegmentBytes;
        try {
            readSegmentBytes = binlog.readPrevRafOneSegment(needFetchFileIndex, needFetchOffset);
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: read binlog file error";
            // need not exception stack trace
            log.error(errorMessage + "=" + e.getMessage());
            return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
        }

        // all fetched
        if (readSegmentBytes == null) {
            var fo = new Binlog.FileIndexAndOffset(needFetchFileIndex, lastUpdatedOffset);
            replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(fo);
            replPair.setAllCaughtUp(true);
            return Repl.reply(slot, replPair, ReplType.s_catch_up, onlyReadonlyResponseContent);
        }

        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();

        // 1 byte for readonly
        // 4 bytes for need fetch file index, 8 bytes for need fetch offset
        // 4 bytes for current file index, 8 bytes for current offset
        var responseBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + readSegmentBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.put(isMasterReadonlyByte);
        responseBuffer.putInt(needFetchFileIndex);
        responseBuffer.putLong(needFetchOffset);
        responseBuffer.putInt(currentFileIndexAndOffset.fileIndex());
        responseBuffer.putLong(currentFileIndexAndOffset.offset());
        responseBuffer.putInt(readSegmentBytes.length);
        responseBuffer.put(readSegmentBytes);

        replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(new Binlog.FileIndexAndOffset(needFetchFileIndex,
                needFetchOffset + readSegmentBytes.length));
        return Repl.reply(slot, replPair, ReplType.s_catch_up, new RawBytesContent(responseBytes));
    }

    @VisibleForTesting
    Repl.ReplReply s_catch_up(short slot, byte[] contentBytes) {
        // client received from server
        replPair.setLastGetCatchUpResponseMillis(System.currentTimeMillis());

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();

        // last updated means next batch, but not fetch yet, refer end of this method
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        if (slot != 0) {
            // wait slot0 fetched exists all done
            if (!localPersist.isAsSlaveSlot0FetchedExistsAllDone()) {
                // use margin file offset
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.catch_up, content);
                });
                return Repl.emptyReply();
            }
        }

        // master has no more binlog to catch up, delay to catch up again
        if (contentBytes.length == 1 || contentBytes.length == 2) {
            boolean resetMasterReadonlyByContentBytes = contentBytes.length == 1;
            boolean isMasterReadonly = false;
            if (resetMasterReadonlyByContentBytes) {
                // only 1 byte for readonly
                isMasterReadonly = contentBytes[0] == 1;
                replPair.setMasterReadonly(isMasterReadonly);
                replPair.setAllCaughtUp(true);
            }

            if (!isMasterReadonly) {
                // use margin file offset
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.catch_up, content);
                });
            }
            return Repl.emptyReply();
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var isMasterReadonly = buffer.get() == 1;
        var fetchedFileIndex = buffer.getInt();
        var fetchedOffset = buffer.getLong();

        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();

        var readSegmentLength = buffer.getInt();
        var readSegmentBytes = new byte[readSegmentLength];
        buffer.get(readSegmentBytes);

        replPair.setMasterReadonly(isMasterReadonly);
        replPair.setAllCaughtUp(fetchedFileIndex == currentFileIndex && currentOffset == fetchedOffset + readSegmentLength);

        // only when self is as slave but also as master, need to write binlog
        try {
            oneSlot.getBinlog().writeFromMasterOneSegmentBytes(readSegmentBytes, fetchedFileIndex, fetchedOffset);
        } catch (IOException e) {
            log.error("Repl slave write binlog from master error, slot=" + slot, e);
        }

        // update last catch up file index and offset
        var skipBytesN = 0;
        var isLastTimeCatchUpThisSegmentButNotCompleted = lastUpdatedFileIndex == fetchedFileIndex && lastUpdatedOffset > fetchedOffset;
        if (isLastTimeCatchUpThisSegmentButNotCompleted) {
            skipBytesN = (int) (lastUpdatedOffset - fetchedOffset);
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
        var isCatchUpToCurrentFile = fetchedFileIndex == currentFileIndex;
        if (isCatchUpToCurrentFile) {
            var diffOffset = currentOffset - fetchedOffset - skipBytesN;
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

        // catch up latest segment, delay to catch up again
        var marginCurrentOffset = Binlog.marginFileOffset(currentOffset);
        var isCatchUpOffsetInLatestSegment = isCatchUpToCurrentFile && fetchedOffset == marginCurrentOffset;
        if (isCatchUpOffsetInLatestSegment) {
            metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                    fetchedFileIndex, fetchedOffset + readSegmentLength);

            // still catch up current (latest) segment, delay
            var content = toMasterCatchUp(binlogMasterUuid, fetchedFileIndex, fetchedOffset, fetchedOffset + readSegmentLength);
            oneSlot.delayRun(1000, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        }

        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
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

            oneSlot.delayRun(1000, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        } else {
            return Repl.reply(slot, replPair, ReplType.catch_up, content);
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
