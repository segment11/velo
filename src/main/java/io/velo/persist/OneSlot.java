package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.activej.async.callback.AsyncComputation;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.velo.*;
import io.velo.metric.InSlotMetricCollector;
import io.velo.metric.SimpleGauge;
import io.velo.repl.*;
import io.velo.repl.content.RawBytesContent;
import io.velo.repl.incremental.XBigStrings;
import io.velo.repl.incremental.XFlush;
import io.velo.repl.incremental.XOneWalGroupPersist;
import io.velo.repl.incremental.XWalV;
import io.velo.task.ITask;
import io.velo.task.TaskChain;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static io.velo.persist.Chunk.*;
import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;

public class OneSlot implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp {
    @TestOnly
    public OneSlot(short slot, File slotDir, KeyLoader keyLoader, Wal wal) throws IOException {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = slotDir;
        this.slotNumber = 1;

        this.keyLoader = keyLoader;
        this.snowFlake = new SnowFlake(1, 1);
        this.chunkSegmentLength = 4096;

        this.bigStringFiles = new BigStringFiles(slot, slotDir);
        this.chunkMergeWorker = null;
        this.dynConfig = null;
        this.walGroupNumber = 1;
        this.walArray = new Wal[]{wal};
        this.raf = null;
        this.rafShortValue = null;
        this.masterUuid = 0L;

        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        this.binlog = null;
    }

    // only for local persist one slot array
    @TestOnly
    OneSlot(short slot) {
        this(slot, null);
    }

    // only for async run/call
    @TestOnly
    OneSlot(short slot, Eventloop eventloop) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = null;
        this.slotNumber = 1;

        this.keyLoader = null;
        this.snowFlake = null;
        this.chunkSegmentLength = 4096;

        this.bigStringFiles = null;
        this.chunkMergeWorker = null;
        this.dynConfig = null;
        this.walGroupNumber = 1;
        this.walArray = new Wal[0];
        this.raf = null;
        this.rafShortValue = null;
        this.masterUuid = 0L;

        this.metaChunkSegmentFlagSeq = null;
        this.metaChunkSegmentIndex = null;

        this.binlog = null;

        this.netWorkerEventloop = eventloop;
    }

    public OneSlot(short slot, short slotNumber, SnowFlake snowFlake, File persistDir, Config persistConfig) throws IOException {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;

        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;

        var volumeDirPath = ConfVolumeDirsForSlot.getVolumeDirBySlot(slot);
        if (volumeDirPath != null) {
            // already exists
            var volumeDir = new File(volumeDirPath);
            this.slotDir = new File(volumeDir, "slot-" + slot);
        } else {
            this.slotDir = new File(persistDir, "slot-" + slot);
        }

        if (!slotDir.exists()) {
            if (!slotDir.mkdirs()) {
                throw new IOException("Create slot dir error, slot: " + slot);
            }
        }

        this.bigStringFiles = new BigStringFiles(slot, slotDir);

        this.chunkMergeWorker = new ChunkMergeWorker(slot, this);

        var dynConfigFile = new File(slotDir, DYN_CONFIG_FILE_NAME);
        this.dynConfig = new DynConfig(slot, dynConfigFile);

        var masterUuidSaved = dynConfig.getMasterUuid();
        if (masterUuidSaved != null) {
            this.masterUuid = masterUuidSaved;
        } else {
            this.masterUuid = snowFlake.nextId();
            dynConfig.setMasterUuid(masterUuid);
        }

        this.dynConfig.setBinlogOn(persistConfig.get(ofBoolean(), "binlogOn", false));
        log.warn("Binlog on: {}", this.dynConfig.isBinlogOn());

        this.walGroupNumber = Wal.calcWalGroupNumber();
        this.walArray = new Wal[walGroupNumber];
        log.info("One slot wal group number: {}, slot: {}", walGroupNumber, slot);

        this.chunkMergeWorker.resetThreshold(walGroupNumber);

        var walSharedFile = new File(slotDir, "wal.dat");
        if (!walSharedFile.exists()) {
            FileUtils.touch(walSharedFile);
        }
        this.raf = new RandomAccessFile(walSharedFile, "rw");
        var lruMemoryRequireMBWriteInWal = walSharedFile.length() / 1024 / 1024;
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal, false);

        var walSharedFileShortValue = new File(slotDir, "wal-short-value.dat");
        if (!walSharedFileShortValue.exists()) {
            FileUtils.touch(walSharedFileShortValue);
        }
        this.rafShortValue = new RandomAccessFile(walSharedFileShortValue, "rw");
        var lruMemoryRequireMBWriteInWal2 = walSharedFileShortValue.length() / 1024 / 1024;
        log.info("LRU prepare, type: {}, short value, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal2, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal2, false);

        long initMemoryN = 0;
        for (int i = 0; i < walGroupNumber; i++) {
            var wal = new Wal(slot, i, raf, rafShortValue, snowFlake);
            walArray[i] = wal;
            initMemoryN += wal.initMemoryN;
        }

        int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
        log.info("Static memory init, type: {}, MB: {}, slot: {}", StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, false);

        initLRU(false);

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, this);

        this.binlog = new Binlog(slot, slotDir, dynConfig);
        // only set slot 0, binlog, if current instance do not include slot 0, need change here
        if (this.slot == 0) {
            DictMap.getInstance().setBinlog(this.binlog);
        }

        this.initTasks();
        this.initMetricsCollect();
    }

    @Override
    public long estimate() {
        long size = 0;
        size += inMemorySizeOfLRU();
        size += keyLoader.estimate();
        size += chunk.estimate();
        size += bigStringFiles.estimate();
        size += metaChunkSegmentFlagSeq.estimate();
        size += chunkMergeWorker.estimate();
        size += binlog.estimate();
        for (var wal : walArray) {
            size += wal.estimate();
        }
        return size;
    }

    public void initLRU(boolean doRemoveForStats) {
        int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;
        var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;
        final var maybeOneCompressedValueEncodedLength = 200;
        var lruMemoryRequireMBReadGroupByWalGroup = maxSizeForAllWalGroups * maybeOneCompressedValueEncodedLength / 1024 / 1024;
        log.info("LRU max size for each wal group: {}, all wal group number: {}, maybe one compressed value encoded length is {}B, memory require: {}MB, slot: {}",
                maxSizeForEachWalGroup,
                walGroupNumber,
                maybeOneCompressedValueEncodedLength,
                lruMemoryRequireMBReadGroupByWalGroup,
                slot);
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, lruMemoryRequireMBReadGroupByWalGroup, slot);
        if (doRemoveForStats) {
            LRUPrepareBytesStats.removeOne(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, slotStr);
        }
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, slotStr, lruMemoryRequireMBReadGroupByWalGroup, false);

        for (int walGroupIndex = 0; walGroupIndex < walGroupNumber; walGroupIndex++) {
            LRUMap<String, byte[]> lru = new LRUMap<>(maxSizeForEachWalGroup);
            kvByWalGroupIndexLRU.put(walGroupIndex, lru);
        }
    }

    @Override
    public String toString() {
        return "OneSlot{" +
                "slot=" + slot +
                ", slotNumber=" + slotNumber +
                ", slotDir=" + slotDir +
                '}';
    }

    private static final Logger log = LoggerFactory.getLogger(OneSlot.class);

    public long getMasterUuid() {
        return masterUuid;
    }

    private final long masterUuid;

    @VisibleForTesting
    final ArrayList<ReplPair> replPairs = new ArrayList<>();

    // slave need not top merge
    public boolean isAsSlave() {
        boolean isAsSlave = false;
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.isAsMaster()) {
                continue;
            }

            isAsSlave = true;
            break;
        }
        return isAsSlave;
    }

    public ArrayList<ReplPair> getSlaveReplPairListSelfAsMaster() {
        ArrayList<ReplPair> list = new ArrayList<>();
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.isAsMaster()) {
                list.add(replPair);
            }
        }
        return list;
    }

    @VisibleForTesting
    final LinkedList<ReplPair> delayNeedCloseReplPairs = new LinkedList<>();

    public void addDelayNeedCloseReplPair(ReplPair replPair) {
        replPair.setPutToDelayListToRemoveTimeMillis(System.currentTimeMillis());
        delayNeedCloseReplPairs.add(replPair);
    }

    // todo, both master - master, need change equal and init as master or slave
    public ReplPair createReplPairAsSlave(String host, int port) throws IOException {
        var replPair = new ReplPair(slot, false, host, port);
        replPair.setSlaveUuid(masterUuid);
        replPair.initAsSlave(netWorkerEventloop, requestHandler);
        log.warn("Repl create repl pair as slave, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);

        if (!isReadonly()) {
            setReadonly(true);
        }
        if (isCanRead()) {
            setCanRead(false);
        }
        return replPair;
    }

    public boolean removeReplPairAsSlave() {
        boolean isSelfSlave = false;
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.isAsMaster()) {
                continue;
            }

            replPair.bye();
            addDelayNeedCloseReplPair(replPair);
            isSelfSlave = true;
        }

        return isSelfSlave;
    }

    public void resetReadonlyFalseAsMaster() throws IOException {
        log.warn("Repl reset readonly false as master, slot: {}", slot);
        if (isReadonly()) {
            setReadonly(false);
        }
        if (!isCanRead()) {
            setCanRead(true);
        }
    }

    public ReplPair getReplPairAsMaster(long slaveUuid) {
        var list = getReplPairAsMasterList();
        return list.stream().filter(one -> one.getSlaveUuid() == slaveUuid).findFirst().orElse(null);
    }

    public ReplPair getFirstReplPairAsMaster() {
        var list = getReplPairAsMasterList();
        return list.isEmpty() ? null : list.getFirst();
    }

    public ArrayList<ReplPair> getReplPairAsMasterList() {
        ArrayList<ReplPair> list = new ArrayList<>();
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (!replPair.isAsMaster()) {
                continue;
            }

            list.add(replPair);
        }
        return list;
    }

    public ReplPair getReplPairAsSlave(long slaveUuid) {
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.isAsMaster()) {
                continue;
            }

            if (replPair.getSlaveUuid() != slaveUuid) {
                continue;
            }

            return replPair;
        }
        return null;
    }

    public ReplPair getOnlyOneReplPairAsSlave() {
        return getReplPairAsSlave(masterUuid);
    }

    public ReplPair createIfNotExistReplPairAsMaster(long slaveUuid, String host, int port) {
        var replPair = new ReplPair(slot, true, host, port);
        replPair.setSlaveUuid(slaveUuid);
        replPair.setMasterUuid(masterUuid);

        for (var replPair1 : replPairs) {
            if (replPair1.equals(replPair)) {
                log.warn("Repl pair as master already exists, host: {}, port: {}, slot: {}", host, port, slot);
                return replPair1;
            }
        }

        log.warn("Repl create repl pair as master, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);
        return replPair;
    }

    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
    }

    private Eventloop netWorkerEventloop;

    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private RequestHandler requestHandler;

    public Promise<Void> asyncRun(RunnableEx runnableEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                runnableEx.run();
                return Promise.complete();
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(netWorkerEventloop.submit(runnableEx));
    }

    public <T> Promise<T> asyncCall(SupplierEx<T> supplierEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                return Promise.of(supplierEx.get());
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(netWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
    }

    public void delayRun(int millis, Runnable runnable) {
        // for unit test
        if (netWorkerEventloop == null) {
            return;
        }

        netWorkerEventloop.delay(millis, runnable);
    }

    private final short slot;
    private final String slotStr;
    private final short slotNumber;

    public short slot() {
        return slot;
    }

    private final int chunkSegmentLength;
    final SnowFlake snowFlake;

    private final File slotDir;

    public File getSlotDir() {
        return slotDir;
    }

    private final BigStringFiles bigStringFiles;

    public BigStringFiles getBigStringFiles() {
        return bigStringFiles;
    }

    public File getBigStringDir() {
        return bigStringFiles.bigStringDir;
    }

    private final Map<Integer, LRUMap<String, byte[]>> kvByWalGroupIndexLRU = new HashMap<>();

    public int kvByWalGroupIndexLRUCountTotal() {
        int n = 0;
        for (var lru : kvByWalGroupIndexLRU.values()) {
            n += lru.size();
        }
        return n;
    }

    // perf bad, just for test or debug
    public long inMemorySizeOfLRU() {
        long size = 0;
        for (var lru : kvByWalGroupIndexLRU.values()) {
            size += RamUsageEstimator.sizeOfMap(lru);
        }
        return size;
    }

    @VisibleForTesting
    int lruClearedCount = 0;

    int clearKvInTargetWalGroupIndexLRU(int walGroupIndex) {
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (lru == null) {
            return 0;
        }

        int n = lru.size();
        lru.clear();
        if (walGroupIndex == 0) {
            lruClearedCount++;
            if (lruClearedCount % 10 == 0) {
                log.info("KV LRU cleared for wal group index: {}, I am alive, act normal", walGroupIndex);
            }
        }
        return n;
    }

    @TestOnly
    void putKvInTargetWalGroupIndexLRU(int walGroupIndex, String key, byte[] cvEncoded) {
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (lru == null) {
            return;
        }

        lru.put(key, cvEncoded);
    }

    @VisibleForTesting
    long kvLRUHitTotal = 0;
    private long kvLRUMissTotal = 0;
    private long kvLRUCvEncodedLengthTotal = 0;

    final ChunkMergeWorker chunkMergeWorker;

    private static final String DYN_CONFIG_FILE_NAME = "dyn-config.json";

    private final DynConfig dynConfig;

    public DynConfig getDynConfig() {
        return dynConfig;
    }

    private static final ArrayList<String> dynConfigKeyWhiteList = new ArrayList<>();

    static {
        // add white list here, todo, refer command config in CGroup
        dynConfigKeyWhiteList.add("testKey");
        dynConfigKeyWhiteList.add("testKey2");
    }

    public boolean updateDynConfig(String key, byte[] configValueBytes) throws IOException {
        // check key white list
        if (!dynConfigKeyWhiteList.contains(key)) {
            log.warn("Update dyn config key not in white list, key: {}, slot: {}", key, slot);
            return false;
        }

        if (key.equals("testKey")) {
            dynConfig.setTestKey(Integer.parseInt(new String(configValueBytes)));
            return true;
            // add else if here
        } else {
            log.warn("Update dyn config key not match, key: {}, slot: {}", key, slot);
            return false;
        }
    }

    public boolean isReadonly() {
        return dynConfig.isReadonly();
    }

    public void setReadonly(boolean readonly) throws IOException {
        dynConfig.setReadonly(readonly);
    }

    public boolean isCanRead() {
        return dynConfig.isCanRead();
    }

    public void setCanRead(boolean canRead) throws IOException {
        dynConfig.setCanRead(canRead);
    }

    private final int walGroupNumber;
    // index is group index
    private final Wal[] walArray;

    public Wal getWalByBucketIndex(int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        return walArray[walGroupIndex];
    }

    public Wal getWalByGroupIndex(int walGroupIndex) {
        return walArray[walGroupIndex];
    }

    private final RandomAccessFile raf;
    private final RandomAccessFile rafShortValue;

    final KeyLoader keyLoader;

    public KeyLoader getKeyLoader() {
        return keyLoader;
    }

    public long getWalKeyCount() {
        long r = 0;
        for (var wal : walArray) {
            r += wal.getKeyCount();
        }
        return r;
    }

    public long getAllKeyCount() {
        // for unit test
        if (keyLoader == null) {
            return 0;
        }
        return keyLoader.getKeyCount() + getWalKeyCount();
    }

    private LibC libC;
    Chunk chunk;

    public Chunk getChunk() {
        return chunk;
    }

    private MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;

    public MetaChunkSegmentFlagSeq getMetaChunkSegmentFlagSeq() {
        return metaChunkSegmentFlagSeq;
    }

    @VisibleForTesting
    MetaChunkSegmentIndex metaChunkSegmentIndex;

    public MetaChunkSegmentIndex getMetaChunkSegmentIndex() {
        return metaChunkSegmentIndex;
    }

    @VisibleForTesting
    int getChunkWriteSegmentIndexInt() {
        return metaChunkSegmentIndex.get();
    }

    public void setMetaChunkSegmentIndexInt(int segmentIndex) {
        setMetaChunkSegmentIndexInt(segmentIndex, false);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void setMetaChunkSegmentIndexInt(int segmentIndex, boolean updateChunkSegmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalArgumentException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }

        metaChunkSegmentIndex.set(segmentIndex);
        if (updateChunkSegmentIndex) {
            chunk.segmentIndex = segmentIndex;
        }
    }

    public void setChunkSegmentIndexFromMeta() {
        chunk.segmentIndex = metaChunkSegmentIndex.get();
    }

    private final Binlog binlog;

    public Binlog getBinlog() {
        return binlog;
    }

    public void appendBinlog(BinlogContent content) {
        if (binlog != null) {
            try {
                binlog.append(content);
            } catch (IOException e) {
                throw new RuntimeException("Append binlog error, slot: " + slot, e);
            }
        }
    }

    private final TaskChain taskChain = new TaskChain();

    public TaskChain getTaskChain() {
        return taskChain;
    }

    public void doTask(int loopCount) {
        taskChain.doTask(loopCount);
    }

    private void initTasks() {
        taskChain.add(new ITask() {
            private int loopCount = 0;

            @Override
            public String name() {
                return "repl pair slave ping";
            }

            @Override
            public void run() {
                if (loopCount % 1000 == 0) {
                    log.info("Task {} run, slot: {}, loop count: {}", name(), slot, loopCount);
                }

                for (var replPair : replPairs) {
                    if (replPair.isSendBye()) {
                        continue;
                    }

                    if (!replPair.isAsMaster()) {
                        // only slave need send ping
                        replPair.ping();

                        var toFetchBigStringUuids = replPair.doingFetchBigStringUuid();
                        if (toFetchBigStringUuids != -1) {
                            var bytes = new byte[8];
                            ByteBuffer.wrap(bytes).putLong(toFetchBigStringUuids);
                            replPair.write(ReplType.incremental_big_string, new RawBytesContent(bytes));
                            log.info("Repl do fetch incremental big string, to server: {}, slot: {}, uuid: {}",
                                    replPair.getHostAndPort(), slot, toFetchBigStringUuids);
                        }
                    }
                }

                if (!delayNeedCloseReplPairs.isEmpty()) {
                    var first = delayNeedCloseReplPairs.getFirst();
                    // delay 10s as slave will try to fetch data by jedis sync get
                    if (System.currentTimeMillis() - first.getPutToDelayListToRemoveTimeMillis() > 1000 * 10) {
                        var needCloseReplPair = delayNeedCloseReplPairs.pop();
                        needCloseReplPair.close();

                        var it = replPairs.iterator();
                        while (it.hasNext()) {
                            var replPair = it.next();
                            if (replPair.equals(needCloseReplPair)) {
                                it.remove();
                                log.warn("Remove repl pair after bye, to server: {}, slot: {}", replPair.getHostAndPort(), slot);
                                break;
                            }
                        }
                    }
                }
            }

            @Override
            public void setLoopCount(int loopCount) {
                this.loopCount = loopCount;
            }
        });
    }

    void debugMode() {
        taskChain.add(new ITask() {
            private int loopCount = 0;

            @Override
            public String name() {
                return "debug";
            }

            @Override
            public void run() {
                // reduce log
                if (slot == 0) {
                    log.info("Debug task run, slot: {}, loop count: {}", slot, loopCount);
                }
            }

            @Override
            public void setLoopCount(int loopCount) {
                this.loopCount = loopCount;
            }

            @Override
            public int executeOnceAfterLoopCount() {
                return 10;
            }
        });
    }

    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id: " + threadId + ", thread id protected for safe: " + threadIdProtectedForSafe);
        }
    }

    public Long getExpireAt(byte[] keyBytes, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var key = new String(keyBytes);
        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedFromWal.length;

            // write batch kv is the newest
            if (CompressedValue.isDeleted(cvEncodedFromWal)) {
                return null;
            }
            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedFromWal), keyBytes, keyHash);
            return cv.getExpireAt();
        }

        // from lru cache
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), keyBytes, keyHash);
            return cv.getExpireAt();
        }
        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        return valueBytesWithExpireAtAndSeq.expireAt();
    }

    public record BufOrCompressedValue(ByteBuf buf, CompressedValue cv) {
    }

    public BufOrCompressedValue get(byte[] keyBytes, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var key = new String(keyBytes);
        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedFromWal.length;

            // write batch kv is the newest
            if (CompressedValue.isDeleted(cvEncodedFromWal)) {
                return null;
            }
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedFromWal), null);
        }

        // from lru cache
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
        }
        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        var valueBytes = valueBytesWithExpireAtAndSeq.valueBytes();
        if (!PersistValueMeta.isPvm(valueBytes)) {
            // short value, just return, CompressedValue can decode
            lru.put(key, valueBytes);
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(valueBytes), null);
        }

        var pvm = PersistValueMeta.decode(valueBytes);
        var decompressedBytes = getSegmentSubBlockDecompressedBytesByPvm(pvm);
//        SegmentBatch.iterateFromSegmentBytesForDebug(decompressedBytes);

        var buf = Unpooled.wrappedBuffer(decompressedBytes);
        // crc check
//        var segmentSeq = buf.readLong();
//        var cvCount = buf.readInt();
//        var segmentMaskedValue = buf.readInt();
//        buf.skipBytes(SEGMENT_HEADER_LENGTH);

        buf.readerIndex(pvm.segmentOffset);

        // skip key header or check key
        var keyLength = buf.readShort();
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytesRead = new byte[keyLength];
        buf.readBytes(keyBytesRead);

        if (!Arrays.equals(keyBytesRead, keyBytes)) {
            throw new IllegalStateException("Key not match, key: " + new String(keyBytes) + ", key persisted: " + new String(keyBytesRead));
        }

        // set to lru cache, just target bytes
        var cv = CompressedValue.decode(buf, keyBytes, keyHash);
        lru.put(key, cv.encode());

        return new BufOrCompressedValue(null, cv);
    }

    byte[] getFromWal(String key, int bucketIndex) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        return targetWal.get(key);
    }

    private byte[] getSegmentSubBlockDecompressedBytesByPvm(PersistValueMeta pvm) {
        byte[] tightBytesWithLength = chunk.preadOneSegment(pvm.segmentIndex);
        if (tightBytesWithLength == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm: " + pvm);
        }

        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        buffer.position(SegmentBatch.subBlockMetaPosition(pvm.subBlockIndex));
        var subBlockOffset = buffer.getShort();
        var subBlockLength = buffer.getShort();

        if (subBlockOffset == 0) {
            throw new IllegalStateException("Sub block offset is 0, pvm: " + pvm);
        }

        var decompressedBytes = new byte[chunkSegmentLength];

        var beginT = System.nanoTime();
        var d = Zstd.decompressByteArray(decompressedBytes, 0, chunkSegmentLength,
                tightBytesWithLength, subBlockOffset, subBlockLength);
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        segmentDecompressTimeTotalUs += costT;
        segmentDecompressCountTotal++;

        if (d != chunkSegmentLength) {
            throw new IllegalStateException("Decompress segment sub block error, s=" + pvm.slot +
                    ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunkSegmentLength);
        }

        return decompressedBytes;
    }

    public boolean exists(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            return !CompressedValue.isDeleted(cvEncodedFromWal);
        }

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, key.getBytes(), keyHash);
        return valueBytesWithExpireAtAndSeq != null && !valueBytesWithExpireAtAndSeq.isExpired();
    }

    public boolean remove(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        if (exists(key, bucketIndex, keyHash)) {
            removeDelay(key, bucketIndex, keyHash);
            return true;
        } else {
            return false;
        }
    }

    @SlaveNeedReplay
    public void removeDelay(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var putResult = targetWal.removeDelay(key, bucketIndex, keyHash);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, putResult);
        } else {
            var xWalV = new XWalV(putResult.needPutV(), putResult.isValueShort(), putResult.offset());
            appendBinlog(xWalV);
        }
    }

    long threadIdProtectedForSafe = -1;

    public void put(String key, int bucketIndex, CompressedValue cv) {
        put(key, bucketIndex, cv, false);
    }

    @SlaveNeedReplay
    // thread safe, same slot, same event loop
    public void put(String key, int bucketIndex, CompressedValue cv, boolean isFromMerge) {
        checkCurrentThreadId();

        // before put check for better performance, todo
        if (isReadonly()) {
            throw new ReadonlyException();
        }

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];

        byte[] cvEncoded;
        boolean isValueShort = cv.noExpire() && (cv.isTypeNumber() || cv.isShortString());
        if (isValueShort) {
            if (cv.isTypeNumber()) {
                cvEncoded = cv.encodeAsNumber();
            } else {
                cvEncoded = cv.encodeAsShortString();
            }
        } else {
            cvEncoded = cv.encode();
        }
        var v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                key, cvEncoded, isFromMerge);

        // for big string, use single file
        boolean isPersistLengthOverSegmentLength = v.persistLength() + SEGMENT_HEADER_LENGTH > chunkSegmentLength;
        if (isPersistLengthOverSegmentLength || key.startsWith("kerry-test-big-string-")) {
            var uuid = snowFlake.nextId();
            var bytes = cv.getCompressedData();
            var isWriteOk = bigStringFiles.writeBigStringBytes(uuid, key, bytes);
            if (!isWriteOk) {
                throw new RuntimeException("Write big string file error, uuid: " + uuid + ", key: " + key);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            var xBigStrings = new XBigStrings(uuid, key, cvEncoded);
            appendBinlog(xBigStrings);

            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cvEncoded, isFromMerge);

            isValueShort = true;
        }

        var putResult = targetWal.put(isValueShort, key, v);
        if (!putResult.needPersist()) {
            var xWalV = new XWalV(v, isValueShort, putResult.offset());
            appendBinlog(xWalV);

            return;
        }

        doPersist(walGroupIndex, key, putResult);
    }

    @SlaveNeedReplay
    private void doPersist(int walGroupIndex, String key, Wal.PutResult putResult) {
        var targetWal = walArray[walGroupIndex];
        persistWal(putResult.isValueShort(), targetWal);

        if (putResult.isValueShort()) {
            targetWal.clearShortValues();
        } else {
            targetWal.clearValues();
        }

        var needPutV = putResult.needPutV();
        if (needPutV != null) {
            targetWal.put(putResult.isValueShort(), key, needPutV);

            var xWalV = new XWalV(needPutV, putResult.isValueShort(), putResult.offset());
            appendBinlog(xWalV);
        }
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void flush() {
        checkCurrentThreadId();

        for (var wal : walArray) {
            wal.clear(false);
        }

        if (raf != null) {
            // truncate all wal raf file
            try {
                raf.setLength(0);
                rafShortValue.setLength(0);
            } catch (IOException e) {
                throw new RuntimeException("Flush wal raf error", e);
            }
        }

        if (this.metaChunkSegmentFlagSeq != null) {
            this.metaChunkSegmentFlagSeq.clear();
        }
        if (this.metaChunkSegmentIndex != null) {
            this.metaChunkSegmentIndex.clear();
        }

        if (this.chunk != null) {
            this.chunk.resetAsFlush();
        }

        if (this.bigStringFiles != null) {
            this.bigStringFiles.deleteAllBigStringFiles();
        }

        if (this.keyLoader != null) {
            try {
                this.keyLoader.flush();
            } catch (Exception e) {
                throw new RuntimeException("Flush key loader error", e);
            }
        }

        if (this.binlog != null) {
            this.binlog.truncateAll();
        }

        appendBinlog(new XFlush());
    }

    void initFds(LibC libC) throws IOException {
        this.libC = libC;
        this.keyLoader.initFds(libC);

        // meta data
        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        // chunk
        initChunk();
    }

    private void initChunk() throws IOException {
        this.chunk = new Chunk(slot, slotDir, this, snowFlake, keyLoader);
        chunk.initFds(libC);

        var segmentIndexLastSaved = getChunkWriteSegmentIndexInt();

        // write index mmap crash recovery
        boolean isBreak = false;
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            boolean canWrite = chunk.initSegmentIndexWhenFirstStart(segmentIndexLastSaved + i);
            int currentSegmentIndex = chunk.currentSegmentIndex();
            log.warn("Move segment to write, s={}, i={}", slot, currentSegmentIndex);

            // when restart server, set persisted flag
            if (!canWrite) {
                log.warn("Segment can not write, s={}, i={}", slot, currentSegmentIndex);

                // set persisted flag reuse_new -> need merge before use
                updateSegmentMergeFlag(currentSegmentIndex, Flag.reuse_new.flagByte, snowFlake.nextId());
                log.warn("Reset segment persisted when init");

                setMetaChunkSegmentIndexInt(currentSegmentIndex);
            } else {
                setMetaChunkSegmentIndexInt(currentSegmentIndex);
                isBreak = true;
                break;
            }
        }

        if (!isBreak) {
            throw new IllegalStateException("Segment can not write after reset flag, s=" + slot + ", i=" + chunk.currentSegmentIndex());
        }
    }

    byte[] preadForMerge(int segmentIndex, int segmentCount) {
        checkCurrentThreadId();

        return chunk.preadForMerge(segmentIndex, segmentCount);
    }

    public byte[] preadForRepl(int segmentIndex) {
        checkCurrentThreadId();

        return chunk.preadForRepl(segmentIndex);
    }

    @SlaveReplay
    public void writeChunkSegmentsFromMasterExists(byte[] bytes, int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        if (bytes.length != chunk.chunkSegmentLength * segmentCount) {
            throw new IllegalStateException("Repl write chunk segments bytes length not match, bytes length: " + bytes.length +
                    ", chunk segment length: " + chunk.chunkSegmentLength + ", segment count: " + segmentCount);
        }

        chunk.writeSegmentsFromMasterExists(bytes, beginSegmentIndex, segmentCount);
        log.warn("Repl write chunk segments from master exists, s={}, i={}, c={}", slot, beginSegmentIndex, segmentCount);
    }

    @Override
    public void cleanUp() {
        checkCurrentThreadId();

        // close wal raf
        if (raf != null) {
            try {
                raf.close();
                System.out.println("Close wal raf success, slot: " + slot);

                rafShortValue.close();
                System.out.println("Close wal short value raf success, slot: " + slot);
            } catch (IOException e) {
                System.err.println("Close wal raf / wal short raf error, slot: " + slot);
            }
        }

        if (metaChunkSegmentFlagSeq != null) {
            metaChunkSegmentFlagSeq.cleanUp();
        }

        if (metaChunkSegmentIndex != null) {
            metaChunkSegmentIndex.cleanUp();
        }

        if (chunk != null) {
            chunk.cleanUp();
        }

        // big string files, need not close, it read write a single file each time

        if (keyLoader != null) {
            keyLoader.cleanUp();
        }

        if (binlog != null) {
            binlog.cleanUp();
        }

        for (var replPair : replPairs) {
            replPair.bye();
            // no reactor anymore
//            replPair.close();
        }
    }

    @VisibleForTesting
    record BeforePersistWalExtFromMerge(ArrayList<Integer> segmentIndexList,
                                        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList) {
        boolean isEmpty() {
            return segmentIndexList.isEmpty();
        }
    }

    record BeforePersistWalExt2FromMerge(ArrayList<Integer> segmentIndexList,
                                         ArrayList<Wal.V> vList) {
    }

    // for performance, before persist wal, read some segment in same wal group and  merge immediately
    @VisibleForTesting
    BeforePersistWalExtFromMerge readSomeSegmentsBeforePersistWal(int walGroupIndex) {
        var currentSegmentIndex = chunk.currentSegmentIndex();
        var needMergeSegmentIndex = chunk.needMergeSegmentIndex(false, currentSegmentIndex);

        // find continuous segments those wal group index is same from need merge segment index
        // * 4 make sure to find one
        int nextSegmentCount = Math.min(Math.max(walGroupNumber * 4, (chunk.maxSegmentIndex + 1) / 4), 16384);
        final int[] firstSegmentIndexWithReadSegmentCountArray = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                needMergeSegmentIndex, nextSegmentCount, walGroupIndex, chunk);

        logMergeCount++;
        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;

        // always consider first 10 and last 10 segments
        if (firstSegmentIndexWithReadSegmentCountArray[0] == NO_NEED_MERGE_SEGMENT_INDEX) {
            final int[] arrayLastN = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                    chunk.maxSegmentIndex - ONCE_PREPARE_SEGMENT_COUNT, ONCE_PREPARE_SEGMENT_COUNT, walGroupIndex, chunk);
            if (arrayLastN[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                firstSegmentIndexWithReadSegmentCountArray[0] = arrayLastN[0];
                firstSegmentIndexWithReadSegmentCountArray[1] = arrayLastN[1];
            } else {
                final int[] arrayFirstN = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                        0, ONCE_PREPARE_SEGMENT_COUNT, walGroupIndex, chunk);
                if (arrayFirstN[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                    firstSegmentIndexWithReadSegmentCountArray[0] = arrayFirstN[0];
                    firstSegmentIndexWithReadSegmentCountArray[1] = arrayFirstN[1];
                }
            }
        }

        if (firstSegmentIndexWithReadSegmentCountArray[0] == NO_NEED_MERGE_SEGMENT_INDEX) {
            if (doLog) {
                log.warn("No segment need merge when persist wal, s={}, i={}", slot, currentSegmentIndex);
            }
            return null;
        }

        var firstSegmentIndex = firstSegmentIndexWithReadSegmentCountArray[0];
        var segmentCount = firstSegmentIndexWithReadSegmentCountArray[1];
        var segmentBytesBatchRead = preadForMerge(firstSegmentIndex, segmentCount);

        ArrayList<Integer> segmentIndexList = new ArrayList<>();
        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE * 10);

        for (int i = 0; i < segmentCount; i++) {
            var segmentIndex = firstSegmentIndex + i;
            int relativeOffsetInBatchBytes = i * chunkSegmentLength;

            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, 0);
                if (doLog) {
                    log.info("Set segment flag to persisted as not write at all, s={}, i={}", slot, segmentIndex);
                }
                continue;
            }

            ChunkMergeJob.readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, this);
            segmentIndexList.add(segmentIndex);
        }

        return new BeforePersistWalExtFromMerge(segmentIndexList, cvList);
    }

    @VisibleForTesting
    long logMergeCount = 0;

    @SlaveNeedReplay
    void persistWal(boolean isShortValue, Wal targetWal) {
        var walGroupIndex = targetWal.groupIndex;
        var xForBinlog = new XOneWalGroupPersist(isShortValue, true, walGroupIndex);
        if (isShortValue) {
            keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, targetWal.delayToKeyBucketShortValues.values(), xForBinlog);
            return;
        }

        var delayToKeyBucketShortValues = targetWal.delayToKeyBucketShortValues;
        var delayToKeyBucketValues = targetWal.delayToKeyBucketValues;

        var list = new ArrayList<>(delayToKeyBucketValues.values());
        // sort by bucket index for future merge better
        list.sort(Comparator.comparingInt(Wal.V::bucketIndex));

        var ext = readSomeSegmentsBeforePersistWal(walGroupIndex);
        var ext2 = chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex);

        // remove those wal exist
        if (ext != null) {
            var cvList = ext.cvList;
            cvList.removeIf(one -> delayToKeyBucketShortValues.containsKey(one.key) || delayToKeyBucketValues.containsKey(one.key));
            if (!cvList.isEmpty()) {
                for (var one : cvList) {
                    var cv = one.cv;
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash(), keyLoader.bucketsPerSlot);
                    var extWalGroupIndex = Wal.calWalGroupIndex(bucketIndex);
                    if (extWalGroupIndex != walGroupIndex) {
                        throw new IllegalStateException("Wal group index not match, s=" + slot + ", wal group index=" + walGroupIndex + ", ext wal group index=" + extWalGroupIndex);
                    }
                    list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                            one.key, cv.encode(), true));
                }
            }
        }

        if (ext2 != null) {
            var vList = ext2.vList;
            vList.removeIf(one -> delayToKeyBucketShortValues.containsKey(one.key()) || delayToKeyBucketValues.containsKey(one.key()));
            if (!vList.isEmpty()) {
                list.addAll(vList);
            }
        }

        if (list.size() > 1000 * 4) {
            log.warn("Ready to persist wal with merged valid cv list, too large, s={}, wal group index={}, list size={}",
                    slot, walGroupIndex, list.size());
        }

        var needMergeSegmentIndexList = chunk.persist(walGroupIndex, list, false, xForBinlog);
        if (needMergeSegmentIndexList == null) {
            throw new IllegalStateException("Persist error, need merge segment index list is null, slot: " + slot);
        }

        if (ext != null && !ext.isEmpty()) {
            var segmentIndexList = ext.segmentIndexList;
            // continuous segment index
            if (segmentIndexList.getLast() - segmentIndexList.getFirst() == segmentIndexList.size() - 1) {
                List<Long> seq0List = new ArrayList<>(segmentIndexList.size());
                for (var ignored : segmentIndexList) {
                    seq0List.add(0L);
                }
                setSegmentMergeFlagBatch(segmentIndexList.getFirst(), segmentIndexList.size(), Flag.merged_and_persisted.flagByte, seq0List, walGroupIndex);

                for (var segmentIndex : segmentIndexList) {
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted.flagByte, 0L);
                }
            } else {
                // when read some segments before persist wal, meta is continuous, but segments read from chunk may be null, skip some, so is not continuous anymore
                // usually not happen
                for (var segmentIndex : segmentIndexList) {
                    setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, walGroupIndex);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted.flagByte, 0L);
                }
            }

            // do not remove, keep segment index continuous, chunk merge job will skip as flag is merged and persisted
//                needMergeSegmentIndexList.removeIf(segmentIndexList::contains);
        }

        if (ext2 != null) {
            var segmentIndexList = ext2.segmentIndexList;
            // usually not continuous
            for (var segmentIndex : segmentIndexList) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, walGroupIndex);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted.flagByte, 0L);
            }

            chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal(segmentIndexList, walGroupIndex);
        }

        appendBinlog(xForBinlog);

        if (!needMergeSegmentIndexList.isEmpty()) {
            doMergeJob(needMergeSegmentIndexList);
            checkFirstMergedButNotPersistedSegmentIndexTooNear();
        }

        checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false);
    }

    @VisibleForTesting
    void checkFirstMergedButNotPersistedSegmentIndexTooNear() {
        if (chunkMergeWorker.isMergedSegmentSetEmpty()) {
            return;
        }

        var currentSegmentIndex = chunk.currentSegmentIndex();
        var firstMergedButNotPersisted = chunkMergeWorker.firstMergedSegmentIndex();

        // need persist merged segments immediately, or next time wal persist will not prepare ready
        boolean needPersistMergedButNotPersisted = isNeedPersistMergedButNotPersisted(currentSegmentIndex, firstMergedButNotPersisted);
        if (needPersistMergedButNotPersisted) {
            log.warn("Persist merged segments immediately, s={}, begin merged segment index={}",
                    slot, firstMergedButNotPersisted);
            chunkMergeWorker.persistFIFOMergedCvList();
        }
    }

    private boolean isNeedPersistMergedButNotPersisted(int currentSegmentIndex, int firstMergedButNotPersisted) {
        boolean needPersistMergedButNotPersisted = false;
        if (currentSegmentIndex < chunk.halfSegmentNumber) {
            if (firstMergedButNotPersisted - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }
        } else {
            if (firstMergedButNotPersisted > currentSegmentIndex &&
                    firstMergedButNotPersisted - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }

            // recycle
            if (firstMergedButNotPersisted < currentSegmentIndex &&
                    chunk.maxSegmentIndex - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT &&
                    firstMergedButNotPersisted <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }
        }
        return needPersistMergedButNotPersisted;
    }

    @MasterReset
    public void checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(boolean isServerStart) {
        var currentSegmentIndex = chunk.currentSegmentIndex();

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        // * 2 when recycled, from 0 again
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT * 2 + chunkMergeWorker.MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST; i++) {
            var targetSegmentIndex = currentSegmentIndex + i;
            if (targetSegmentIndex == chunk.maxSegmentIndex + 1) {
                targetSegmentIndex = 0;
            } else if (targetSegmentIndex > chunk.maxSegmentIndex + 1) {
                // recycle
                targetSegmentIndex = targetSegmentIndex - chunk.maxSegmentIndex - 1;
            }

            var segmentFlag = getSegmentMergeFlag(targetSegmentIndex);
            var flagByte = segmentFlag.flagByte();

            if (isServerStart && flagByte == Flag.reuse.flagByte) {
                continue;
            }

            if (flagByte != Flag.init.flagByte && flagByte != Flag.merged_and_persisted.flagByte) {
                needMergeSegmentIndexList.add(targetSegmentIndex);
            }
        }

        if (needMergeSegmentIndexList.isEmpty()) {
            return;
        }

        log.warn("Not merged and persisted next range segment index too near, s={}, begin segment index={}",
                slot, needMergeSegmentIndexList.getFirst());

        needMergeSegmentIndexList.sort(Integer::compareTo);
        // maybe not continuous
        var validCvCountTotal = mergeTargetSegments(needMergeSegmentIndexList, isServerStart);

        if (isServerStart && validCvCountTotal > 0) {
            chunkMergeWorker.persistAllMergedCvListInTargetSegmentIndexList(needMergeSegmentIndexList);
        }
    }

    private void checkSegmentIndex(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }
    }

    private void checkBeginSegmentIndex(int beginSegmentIndex, int segmentCount) {
        if (beginSegmentIndex < 0 || beginSegmentIndex + segmentCount > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Begin segment index out of bound, s=" + slot + ", i=" + beginSegmentIndex + ", c=" + segmentCount);
        }
    }

    SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        checkSegmentIndex(segmentIndex);
        return metaChunkSegmentFlagSeq.getSegmentMergeFlag(segmentIndex);
    }

    ArrayList<SegmentFlag> getSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        return metaChunkSegmentFlagSeq.getSegmentMergeFlagBatch(beginSegmentIndex, segmentCount);
    }

    public List<Long> getSegmentSeqListBatchForRepl(int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        return metaChunkSegmentFlagSeq.getSegmentSeqListBatchForRepl(beginSegmentIndex, segmentCount);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void updateSegmentMergeFlag(int segmentIndex, byte flagByte, long segmentSeq) {
        var segmentFlag = getSegmentMergeFlag(segmentIndex);
        setSegmentMergeFlag(segmentIndex, flagByte, segmentSeq, segmentFlag.walGroupIndex());
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void setSegmentMergeFlag(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex) {
        checkSegmentIndex(segmentIndex);
        metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, flagByte, segmentSeq, walGroupIndex);

        if (ConfForGlobal.pureMemory) {
            if (Chunk.Flag.canReuse(flagByte)) {
                chunk.clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(segmentIndex);
            }
        }
    }

    void setSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount, byte flagByte, List<Long> segmentSeqList, int walGroupIndex) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(beginSegmentIndex, segmentCount, flagByte, segmentSeqList, walGroupIndex);

        if (ConfForGlobal.pureMemory) {
            if (Chunk.Flag.canReuse(flagByte)) {
                for (int i = 0; i < segmentCount; i++) {
                    var segmentIndex = beginSegmentIndex + i;
                    chunk.clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(segmentIndex);
                }
            }
        }
    }

    @VisibleForTesting
    int doMergeJob(ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    @VisibleForTesting
    int doMergeJobWhenServerStart(ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    @MasterReset
    public void persistMergingOrMergedSegmentsButNotPersisted() {
        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();

        this.metaChunkSegmentFlagSeq.iterateAll((segmentIndex, flagByte, segmentSeq, walGroupIndex) -> {
            if (Chunk.Flag.isMergingOrMerged(flagByte)) {
                log.warn("Segment not persisted after merging, s={}, i={}, flag byte={}", slot, segmentIndex, flagByte);
                needMergeSegmentIndexList.add(segmentIndex);
            }
        });

        if (needMergeSegmentIndexList.isEmpty()) {
            log.warn("No segment need merge when server start, s={}", slot);
        } else {
            mergeTargetSegments(needMergeSegmentIndexList, true);
        }
    }

    private int mergeTargetSegments(ArrayList<Integer> needMergeSegmentIndexList, boolean isServerStart) {
        int validCvCountTotal = 0;

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();

        // continuous
        if (lastSegmentIndex - firstSegmentIndex + 1 == needMergeSegmentIndexList.size()) {
            var validCvCount = isServerStart ? doMergeJobWhenServerStart(needMergeSegmentIndexList) : doMergeJob(needMergeSegmentIndexList);
            log.warn("Merge segments, is server start: {}, s={}, i={}, end i={}, valid cv count after run: {}",
                    isServerStart, slot, firstSegmentIndex, lastSegmentIndex, validCvCount);
            validCvCountTotal += validCvCount;
        } else {
            // not continuous, need split
            ArrayList<Integer> onceList = new ArrayList<>();
            onceList.add(firstSegmentIndex);

            int last = firstSegmentIndex;
            for (int i = 1; i < needMergeSegmentIndexList.size(); i++) {
                var segmentIndex = needMergeSegmentIndexList.get(i);
                if (segmentIndex - last != 1) {
                    if (!onceList.isEmpty()) {
                        var validCvCount = isServerStart ? doMergeJobWhenServerStart(onceList) : doMergeJob(onceList);
                        log.warn("Merge segments, is server start: {}, once list, s={}, i={}, end i={}, valid cv count after run: {}",
                                isServerStart, slot, onceList.getFirst(), onceList.getLast(), validCvCount);
                        validCvCountTotal += validCvCount;
                        onceList.clear();
                    }
                }
                onceList.add(segmentIndex);
                last = segmentIndex;
            }

            if (!onceList.isEmpty()) {
                var validCvCount = isServerStart ? doMergeJobWhenServerStart(onceList) : doMergeJob(onceList);
                log.warn("Merge segments, is server start: {}, once list, s={}, i={}, end i={}, valid cv count after run: {}",
                        isServerStart, slot, onceList.getFirst(), onceList.getLast(), validCvCount);
                validCvCountTotal += validCvCount;
            }
        }

        return validCvCountTotal;
    }

    @MasterReset
    public void getMergedSegmentIndexEndLastTime() {
        chunk.mergedSegmentIndexEndLastTime = metaChunkSegmentFlagSeq.getMergedSegmentIndexEndLastTime(
                chunk.currentSegmentIndex(), chunk.halfSegmentNumber);
        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart();
        log.info("Get merged segment index end last time, s={}, i={}", slot, chunk.mergedSegmentIndexEndLastTime);

        chunkMergeWorker.lastMergedSegmentIndex = chunk.mergedSegmentIndexEndLastTime;
    }

    @VisibleForTesting
    final static SimpleGauge globalGauge = new SimpleGauge("global", "Global metrics.");

    static {
        globalGauge.register();
    }

    @VisibleForTesting
    long segmentDecompressTimeTotalUs = 0;
    @VisibleForTesting
    long segmentDecompressCountTotal = 0;

    private void initMetricsCollect() {
        // only first slot show global metrics
        if (slot == 0) {
            globalGauge.addRawGetter(() -> {
                // global use slot -1
                var labelValues = List.of("-1");

                var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

                // only show global
                map.put("global_up_time", new SimpleGauge.ValueWithLabelValues((double) MultiWorkerServer.UP_TIME, labelValues));
                map.put("global_dict_size", new SimpleGauge.ValueWithLabelValues((double) DictMap.getInstance().dictSize(), labelValues));
                // global config for one slot
                map.put("global_estimate_key_number", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.estimateKeyNumber, labelValues));
                map.put("global_estimate_one_value_length", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.estimateOneValueLength, labelValues));
                map.put("global_slot_number", new SimpleGauge.ValueWithLabelValues((double) slotNumber, labelValues));
                map.put("global_key_buckets_per_slot", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confBucket.bucketsPerSlot, labelValues));
                map.put("global_chunk_segment_number_per_fd", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.segmentNumberPerFd, labelValues));
                map.put("global_chunk_fd_per_chunk", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.fdPerChunk, labelValues));
                map.put("global_chunk_segment_length", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.segmentLength, labelValues));
                map.put("global_wal_one_charge_bucket_number", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confWal.oneChargeBucketNumber, labelValues));

                map.put("lru_prepare_mb_fd_key_bucket_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_key_bucket), labelValues));
                map.put("lru_prepare_mb_fd_chunk_data_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_chunk_data), labelValues));
                map.put("lru_prepare_mb_kv_read_group_by_wal_group_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group), labelValues));
                map.put("lru_prepare_mb_kv_write_in_wal_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.kv_write_in_wal), labelValues));
                map.put("lru_prepare_mb_kv_big_string_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.big_string), labelValues));
                map.put("lru_prepare_mb_chunk_segment_merged_cv_buffer_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.chunk_segment_merged_cv_buffer), labelValues));

                map.put("lru_prepare_mb_all", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(), labelValues));

                map.put("static_memory_prepare_mb_wal_cache_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.wal_cache), labelValues));
                map.put("static_memory_prepare_mb_wal_cache_init_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.wal_cache_init), labelValues));
                map.put("static_memory_prepare_mb_meta_chunk_segment_flag_seq_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq), labelValues));
                map.put("static_memory_prepare_mb_fd_read_write_buffer_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer), labelValues));

                return map;
            });
        }
    }

    @Override
    public Map<String, Double> collect() {
        var map = new TreeMap<String, Double>();

        if (snowFlake != null) {
            map.put("slot_last_seq", (double) snowFlake.getLastNextId());
        }

        if (walArray != null && walArray.length > 0) {
            map.put("wal_key_count", (double) getWalKeyCount());

            // only show first wal group
            var firstWalGroup = walArray[0];
            map.put("wal_group_first_delay_values_size", (double) firstWalGroup.delayToKeyBucketValues.size());
            map.put("wal_group_first_delay_short_values_size", (double) firstWalGroup.delayToKeyBucketShortValues.size());
            map.put("wal_group_first_need_persist_count_total", (double) firstWalGroup.needPersistCountTotal);
            map.put("wal_group_first_need_persist_kv_count_total", (double) firstWalGroup.needPersistKvCountTotal);
            map.put("wal_group_first_need_persist_offset_total", (double) firstWalGroup.needPersistOffsetTotal);
        }

        if (keyLoader != null) {
            map.putAll(keyLoader.collect());
        }

        if (chunk != null) {
            map.putAll(chunk.collect());
        }

        if (bigStringFiles != null) {
            map.putAll(bigStringFiles.collect());
        }

        if (chunkMergeWorker != null) {
            map.putAll(chunkMergeWorker.collect());
        }

        if (binlog != null) {
            var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
            var fo = binlog.currentFileIndexAndOffset();
            var offsetFromFileIndex0 = (long) fo.fileIndex() * binlogOneFileMaxLength + fo.offset();
            map.put("binlog_current_offset_from_the_beginning", (double) offsetFromFileIndex0);
        }

        var hitMissTotal = kvLRUHitTotal + kvLRUMissTotal;
        if (hitMissTotal > 0) {
            map.put("slot_kv_lru_hit_total", (double) kvLRUHitTotal);
            map.put("slot_kv_lru_miss_total", (double) kvLRUMissTotal);
            map.put("slot_kv_lru_hit_ratio", (double) kvLRUHitTotal / hitMissTotal);
            map.put("slot_kv_lru_current_count_total", (double) kvByWalGroupIndexLRUCountTotal());
        }

        if (kvLRUHitTotal > 0) {
            var kvLRUCvEncodedLengthAvg = (double) kvLRUCvEncodedLengthTotal / kvLRUHitTotal;
            map.put("slot_kv_lru_cv_encoded_length_avg", kvLRUCvEncodedLengthAvg);
        }

        if (segmentDecompressCountTotal > 0) {
            map.put("segment_decompress_time_total_us", (double) segmentDecompressTimeTotalUs);
            map.put("segment_decompress_count_total", (double) segmentDecompressCountTotal);
            double segmentDecompressedCostTAvg = (double) segmentDecompressTimeTotalUs / segmentDecompressCountTotal;
            map.put("segment_decompress_cost_time_avg_us", segmentDecompressedCostTAvg);
        }

        var replPairAsSlave = getOnlyOneReplPairAsSlave();
        if (replPairAsSlave != null) {
            map.put("repl_as_slave_fetched_bytes_total", (double) replPairAsSlave.getFetchedBytesLengthTotal());
            map.put("repl_as_slave_is_link_up", (double) (replPairAsSlave.isLinkUp() ? 1 : 0));
            map.put("repl_as_slave_is_master_readonly", (double) (replPairAsSlave.isMasterReadonly() ? 1 : 0));
            map.put("repl_as_slave_is_all_caught_up", (double) (replPairAsSlave.isAllCaughtUp() ? 1 : 0));
            map.put("repl_as_slave_is_master_can_not_connect", (double) (replPairAsSlave.isMasterCanNotConnect() ? 1 : 0));

            var fo = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
            map.put("repl_as_slave_last_catch_up_binlog_file_index", (double) fo.fileIndex());
            map.put("repl_as_slave_last_catch_up_binlog_offset", (double) fo.offset());
        }

        var replPairAsMasterList = getReplPairAsMasterList();
        if (!replPairAsMasterList.isEmpty()) {
            map.put("repl_as_master_repl_pair_size", (double) replPairAsMasterList.size());

            for (int i = 0; i < replPairAsMasterList.size(); i++) {
                var replPair = replPairAsMasterList.get(i);
                map.put("repl_as_master_repl_pair_" + i + "_slave_catch_up_last_seq", (double) replPair.getSlaveCatchUpLastSeq());
                map.put("repl_as_master_repl_pair_" + i + "_is_link_up", (double) (replPair.isLinkUp() ? 1 : 0));

                var fo = replPair.getSlaveLastCatchUpBinlogFileIndexAndOffset();
                if (fo != null) {
                    map.put("repl_as_master_repl_pair_" + i + "_slave_last_catch_up_binlog_file_index", (double) fo.fileIndex());
                    map.put("repl_as_master_repl_pair_" + i + "_slave_last_catch_up_binlog_offset", (double) fo.offset());
                }
            }
        }

        return map;
    }
}
