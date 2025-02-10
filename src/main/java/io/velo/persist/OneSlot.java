package io.velo.persist;

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
import io.velo.monitor.BigKeyTopK;
import io.velo.persist.index.IndexHandler;
import io.velo.repl.*;
import io.velo.repl.content.RawBytesContent;
import io.velo.repl.incremental.*;
import io.velo.task.ITask;
import io.velo.task.TaskChain;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static io.velo.persist.Chunk.*;
import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
import static io.velo.persist.SegmentBatch2.SEGMENT_HEADER_LENGTH;

public class OneSlot implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, CanSaveAndLoad, HandlerWhenCvExpiredOrDeleted {
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
        handlersRegisteredList.add(bigStringFiles);

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
        this.bigKeyTopK = null;
        this.saveFileNameWhenPureMemory = "slot_" + slot + "_saved.dat";
    }

    // only for local persist one slot array
    @TestOnly
    public OneSlot(short slot) {
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
        this.bigKeyTopK = null;
        this.saveFileNameWhenPureMemory = "slot_" + slot + "_saved.dat";

        this.netWorkerEventloop = eventloop;
    }

    public OneSlot(short slot, short slotNumber,
                   @NotNull SnowFlake snowFlake,
                   @NotNull File persistDir,
                   @NotNull Config persistConfig) throws IOException {
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
                throw new IOException("Create slot dir error, slot=" + slot);
            }
        }

        this.bigStringFiles = new BigStringFiles(slot, slotDir);
        handlersRegisteredList.add(bigStringFiles);

        this.chunkMergeWorker = new ChunkMergeWorker(slot, this);

        var dynConfigFile = new File(slotDir, DYN_CONFIG_FILE_NAME);
        this.dynConfig = new DynConfig(slot, dynConfigFile, this);

        var masterUuidSaved = dynConfig.getMasterUuid();
        if (masterUuidSaved != null) {
            this.masterUuid = masterUuidSaved;
        } else {
            this.masterUuid = snowFlake.nextId();
            dynConfig.setMasterUuid(masterUuid);
        }

        this.dynConfig.setBinlogOn(persistConfig.get(ofBoolean(), "binlogOn", false));
        log.warn("Binlog on={}", this.dynConfig.isBinlogOn());

        this.walGroupNumber = Wal.calcWalGroupNumber();
        this.walArray = new Wal[walGroupNumber];
        log.info("One slot wal group number={}, slot={}", walGroupNumber, slot);

        this.chunkMergeWorker.resetThreshold(walGroupNumber);

        var walSharedFile = new File(slotDir, "wal.dat");
        if (!walSharedFile.exists()) {
            FileUtils.touch(walSharedFile);
        }
        this.raf = new RandomAccessFile(walSharedFile, "rw");
        var lruMemoryRequireMBWriteInWal = walSharedFile.length() / 1024 / 1024;
        log.info("LRU prepare, type={}, MB={}, slot={}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal, false);

        var walSharedFileShortValue = new File(slotDir, "wal-short-value.dat");
        if (!walSharedFileShortValue.exists()) {
            FileUtils.touch(walSharedFileShortValue);
        }
        this.rafShortValue = new RandomAccessFile(walSharedFileShortValue, "rw");
        var lruMemoryRequireMBWriteInWal2 = walSharedFileShortValue.length() / 1024 / 1024;
        log.info("LRU prepare, type={}, short value, MB={}, slot={}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal2, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal2, false);

        long initMemoryN = 0;
        for (int i = 0; i < walGroupNumber; i++) {
            var wal = new Wal(slot, i, raf, rafShortValue, snowFlake);
            walArray[i] = wal;
            initMemoryN += wal.initMemoryN;
        }

        int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
        log.info("Static memory init, type={}, MB={}, slot={}", StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, false);

        initLRU(false);

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, this);
        this.binlog = new Binlog(slot, slotDir, dynConfig);
        initBigKeyTopK(10);

        this.saveFileNameWhenPureMemory = "slot_" + slot + "_saved.dat";

        this.initTasks();
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        long size1 = inMemorySizeOfLRU();
        sb.append("One slot lru: ").append(size1).append("\n");
        size += size1;
        size += keyLoader.estimate(sb);
        size += chunk.estimate(sb);
        size += bigStringFiles.estimate(sb);
        size += metaChunkSegmentFlagSeq.estimate(sb);
        size += chunkMergeWorker.estimate(sb);
        size += binlog.estimate(sb);
        for (var wal : walArray) {
            size += wal.estimate(sb);
        }
        return size;
    }

    @NotPureMemoryMode
    public void initLRU(boolean doRemoveForStats) {
        if (ConfForGlobal.pureMemory) {
            return;
        }

        int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;
        var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;
        final var maybeOneCompressedValueEncodedLength = 200;
        var lruMemoryRequireMBReadGroupByWalGroup = maxSizeForAllWalGroups * maybeOneCompressedValueEncodedLength / 1024 / 1024;
        log.info("LRU max size for each wal group={}, all wal group number={}, maybe one compressed value encoded length is {}B, memory require={}MB, slot={}",
                maxSizeForEachWalGroup,
                walGroupNumber,
                maybeOneCompressedValueEncodedLength,
                lruMemoryRequireMBReadGroupByWalGroup,
                slot);
        log.info("LRU prepare, type={}, MB={}, slot={}", LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, lruMemoryRequireMBReadGroupByWalGroup, slot);
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

    private final long masterUuid;

    public long getMasterUuid() {
        return masterUuid;
    }

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

    public @NotNull ArrayList<ReplPair> getSlaveReplPairListSelfAsMaster() {
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

    public void addDelayNeedCloseReplPair(@NotNull ReplPair replPair) {
        replPair.setPutToDelayListToRemoveTimeMillis(System.currentTimeMillis());
        delayNeedCloseReplPairs.add(replPair);
    }

    @TestOnly
    private boolean doMockWhenCreateReplPairAsSlave = false;

    public void setDoMockWhenCreateReplPairAsSlave(boolean doMockWhenCreateReplPairAsSlave) {
        this.doMockWhenCreateReplPairAsSlave = doMockWhenCreateReplPairAsSlave;
    }

    // todo, both master - master, need change equal and init as master or slave
    public ReplPair createReplPairAsSlave(@NotNull String host, int port) {
        var replPair = new ReplPair(slot, false, host, port);
        replPair.setSlaveUuid(masterUuid);

        if (doMockWhenCreateReplPairAsSlave) {
            log.info("Repl create repl pair as slave, mock, host={}, port={}, slot={}", host, port, slot);
        } else {
            replPair.initAsSlave(netWorkerEventloop, requestHandler);
            log.warn("Repl create repl pair as slave, host={}, port={}, slot={}", host, port, slot);
        }
        replPairs.add(replPair);
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

            log.warn("Repl remove repl pair as slave, host={}, port={}, slot={}", replPair.getHost(), replPair.getPort(), slot);
            replPair.bye();
            addDelayNeedCloseReplPair(replPair);
            isSelfSlave = true;
        }

        return isSelfSlave;
    }

    public @Nullable ReplPair getReplPairAsMaster(long slaveUuid) {
        var list = getReplPairAsMasterList();
        return list.stream().filter(one -> one.getSlaveUuid() == slaveUuid).findFirst().orElse(null);
    }

    public @Nullable ReplPair getFirstReplPairAsMaster() {
        var list = getReplPairAsMasterList();
        return list.isEmpty() ? null : list.getFirst();
    }

    public @NotNull ArrayList<ReplPair> getReplPairAsMasterList() {
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

    public @Nullable ReplPair getReplPairAsSlave(long slaveUuid) {
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

    public @Nullable ReplPair getOnlyOneReplPairAsSlave() {
        return getReplPairAsSlave(masterUuid);
    }

    public @NotNull ReplPair createIfNotExistReplPairAsMaster(long slaveUuid, String host, int port) {
        var replPair = new ReplPair(slot, true, host, port);
        replPair.setSlaveUuid(slaveUuid);
        replPair.setMasterUuid(masterUuid);

        for (var replPair1 : replPairs) {
            if (replPair1.equals(replPair)) {
                log.warn("Repl pair as master already exists, host={}, port={}, slot={}", host, port, slot);
                return replPair1;
            }
        }

        log.warn("Repl create repl pair as master, host={}, port={}, slot={}", host, port, slot);
        replPairs.add(replPair);
        return replPair;
    }

    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
    }

    private Eventloop netWorkerEventloop;

    public void setRequestHandler(@NotNull RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private RequestHandler requestHandler;

    public Promise<Void> asyncRun(@NotNull RunnableEx runnableEx) {
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

    public <T> Promise<T> asyncCall(@NotNull SupplierEx<T> supplierEx) {
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

    public void delayRun(int millis, @NotNull Runnable runnable) {
        // for unit test
        if (netWorkerEventloop == null) {
            return;
        }

        netWorkerEventloop.delay(millis, runnable);
    }

    final ArrayList<HandlerWhenCvExpiredOrDeleted> handlersRegisteredList = new ArrayList<>();

    @Override
    public void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm) {
        for (var handler : handlersRegisteredList) {
            handler.handleWhenCvExpiredOrDeleted(key, shortStringCv, pvm);
        }
    }

    @VisibleForTesting
    int pendingSubmitIndexJobRunCount = 0;

    public void submitIndexJobDone() {
        pendingSubmitIndexJobRunCount--;
        if (pendingSubmitIndexJobRunCount < 0) {
            log.warn("Pending submit index job run count less than 0, slot={}", OneSlot.this.slot);
            pendingSubmitIndexJobRunCount = 0;
        }
    }

    public Promise<Void> submitIndexToTargetWorkerJobRun(byte indexWorkerId, @NotNull Consumer<IndexHandler> consumer) {
        var indexHandlerPool = LocalPersist.getInstance().getIndexHandlerPool();
        return indexHandlerPool.run(indexWorkerId, consumer);
    }

    // when complete, need call submitIndexJobDone
    public Promise<Void> submitIndexJobRun(@NotNull String lowerCaseWord, @NotNull Consumer<IndexHandler> consumer) {
        pendingSubmitIndexJobRunCount++;

        var indexHandlerPool = LocalPersist.getInstance().getIndexHandlerPool();
        var indexWorkerId = indexHandlerPool.getChargeWorkerIdByWordKeyHash(KeyHash.hash(lowerCaseWord.getBytes()));
        return indexHandlerPool.run(indexWorkerId, consumer);
    }

    private final short slot;
    private final String slotStr;
    private final short slotNumber;

    public short slot() {
        return slot;
    }

    private final int chunkSegmentLength;

    private final SnowFlake snowFlake;

    public SnowFlake getSnowFlake() {
        return snowFlake;
    }

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

    @NotPureMemoryMode
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

    @NotPureMemoryMode
    public String randomKeyInLRU(int walGroupIndex) {
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (lru == null || lru.isEmpty()) {
            return null;
        }

        var random = new Random();
        var skipN = random.nextInt(lru.size());
        int count = 0;
        for (var key : lru.keySet()) {
            if (count == skipN) {
                return key;
            }
            count++;
        }
        return null;
    }

    @NotPureMemoryMode
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
                log.info("KV LRU cleared for wal group index={}, I am alive, act normal", walGroupIndex);
            }
        }
        return n;
    }

    @TestOnly
    @NotPureMemoryMode
    public void putKvInTargetWalGroupIndexLRU(int walGroupIndex, @NotNull String key, byte[] cvEncoded) {
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

    public boolean updateDynConfig(@NotNull String key, @NotNull String valueString) throws IOException {
        if (key.equals("testKey")) {
            dynConfig.setTestKey(Integer.parseInt(valueString));
        } else {
            dynConfig.update(key, valueString);
        }
        return true;
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

    CompletableFuture<Boolean> walLazyReadFromFile() {
        var waitF = new CompletableFuture<Boolean>();
        // just run once
        new Thread(() -> {
            log.info("Start a single thread to read wal from file or load saved file when pure memory, slot={}", slot);
            try {
                for (var wal : walArray) {
                    wal.lazyReadFromFile();
                }

                if (ConfForGlobal.pureMemory) {
                    loadFromLastSavedFileWhenPureMemory();
                }

                waitF.complete(true);
            } catch (IOException e) {
                log.error("Wal lazy read from file or load saved file when pure memory error for slot=" + slot, e);
                waitF.completeExceptionally(e);
            } finally {
                log.info("End a single thread to read wal from file or load saved file when pure memory, slot={}", slot);
            }
        }).start();
        return waitF;
    }

    public Wal getWalByBucketIndex(int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
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
            chunk.setSegmentIndex(segmentIndex);
        }
    }

    public void setChunkSegmentIndexFromMeta() {
        chunk.setSegmentIndex(metaChunkSegmentIndex.get());
    }

    private final Binlog binlog;

    public Binlog getBinlog() {
        return binlog;
    }

    public void appendBinlog(@NotNull BinlogContent content) {
        if (binlog != null) {
            try {
                binlog.append(content);
            } catch (IOException e) {
                throw new RuntimeException("Append binlog error, slot=" + slot, e);
            }
        }
    }

    private BigKeyTopK bigKeyTopK;

    public BigKeyTopK getBigKeyTopK() {
        return bigKeyTopK;
    }

    void initBigKeyTopK(int k) {
        // may be dyn config init already init big key top k
        if (bigKeyTopK == null) {
            bigKeyTopK = new BigKeyTopK(k);
        }
    }

    public void monitorBigKeyByValueLength(byte[] keyBytes, int valueBytesLength) {
        bigKeyTopK.add(keyBytes, valueBytesLength);
    }

    private final String saveFileNameWhenPureMemory;

    enum SaveBytesType {
        wal(1), key_loader(2), chunk(3), big_strings(4), meta_chunk_segment_index(5), meta_chunk_segment_flag_seq(6);

        final int i;

        SaveBytesType(int i) {
            this.i = i;
        }
    }

    void loadFromLastSavedFileWhenPureMemory() throws IOException {
        var lastSavedFile = new File(slotDir, saveFileNameWhenPureMemory);
        if (!lastSavedFile.exists()) {
            return;
        }

        var fileLength = lastSavedFile.length();
        if (fileLength == 0) {
            return;
        }

        try (var is = new DataInputStream(new FileInputStream(lastSavedFile))) {
            var beginT = System.currentTimeMillis();
            loadFromLastSavedFileWhenPureMemory(is);
            var costT = System.currentTimeMillis() - beginT;
            log.info("Load from last saved file when pure memory, slot={}, cost={} ms, file length={} KB",
                    slot, costT, fileLength / 1024);
        }
    }

    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        var bytesType = is.readInt();
        while (bytesType != 0) {
            if (bytesType == SaveBytesType.wal.i) {
                // wal group index int, is short value byte, read bytes length int
                var walGroupIndex = is.readInt();
                var isShortValue = is.readBoolean();
                var readBytesLength = is.readInt();
                var readBytes = new byte[readBytesLength];
                is.readFully(readBytes);
                int n = walArray[walGroupIndex].readFromSavedBytes(readBytes, isShortValue);
                if (walGroupIndex == 0) {
                    log.info("Read wal group 0, n={}, is short value={}", n, isShortValue);
                }
            } else if (bytesType == SaveBytesType.key_loader.i) {
                this.keyLoader.loadFromLastSavedFileWhenPureMemory(is);
            } else if (bytesType == SaveBytesType.chunk.i) {
                this.chunk.loadFromLastSavedFileWhenPureMemory(is);
            } else if (bytesType == SaveBytesType.big_strings.i) {
                this.bigStringFiles.loadFromLastSavedFileWhenPureMemory(is);
            } else if (bytesType == SaveBytesType.meta_chunk_segment_index.i) {
                var metaBytes = new byte[this.metaChunkSegmentIndex.allCapacity];
                is.readFully(metaBytes);
                this.metaChunkSegmentIndex.overwriteInMemoryCachedBytes(metaBytes);
            } else if (bytesType == SaveBytesType.meta_chunk_segment_flag_seq.i) {
                var metaBytes = new byte[this.metaChunkSegmentFlagSeq.allCapacity];
                is.readFully(metaBytes);
                this.metaChunkSegmentFlagSeq.overwriteInMemoryCachedBytes(metaBytes);
            } else {
                throw new IllegalStateException("Unexpected value: " + bytesType);
            }

            if (is.available() < 4) {
                bytesType = 0;
            } else {
                bytesType = is.readInt();
            }
        }
    }

    public void writeToSavedFileWhenPureMemory() throws IOException {
        var lastSavedFile = new File(slotDir, saveFileNameWhenPureMemory);
        if (!lastSavedFile.exists()) {
            FileUtils.touch(lastSavedFile);
        }

        try (var os = new DataOutputStream(new FileOutputStream(lastSavedFile))) {
            var beginT = System.currentTimeMillis();
            writeToSavedFileWhenPureMemory(os);
            var costT = System.currentTimeMillis() - beginT;
            log.info("Write to saved file when pure memory, slot={}, cost={} ms", slot, costT);
        }

        var fileLength = lastSavedFile.length();
        log.info("Saved file length={} KB", fileLength / 1024);
    }

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        for (var wal : walArray) {
            if (wal.getKeyCount() == 0) {
                continue;
            }

            var shortValues = wal.delayToKeyBucketShortValues;
            if (!shortValues.isEmpty()) {
                os.writeInt(SaveBytesType.wal.i);
                os.writeInt(wal.groupIndex);
                os.writeBoolean(true);
                var writeBytes = wal.writeToSavedBytes(true);
                os.writeInt(writeBytes.length);
                os.write(writeBytes);
            }

            var values = wal.delayToKeyBucketValues;
            if (!values.isEmpty()) {
                os.writeInt(SaveBytesType.wal.i);
                os.writeInt(wal.groupIndex);
                os.writeBoolean(false);
                var writeBytes = wal.writeToSavedBytes(false);
                os.writeInt(writeBytes.length);
                os.write(writeBytes);
            }
        }

        os.writeInt(SaveBytesType.key_loader.i);
        keyLoader.writeToSavedFileWhenPureMemory(os);

        os.writeInt(SaveBytesType.chunk.i);
        chunk.writeToSavedFileWhenPureMemory(os);

        os.writeInt(SaveBytesType.big_strings.i);
        bigStringFiles.writeToSavedFileWhenPureMemory(os);

        os.writeInt(SaveBytesType.meta_chunk_segment_index.i);
        os.write(metaChunkSegmentIndex.getInMemoryCachedBytes());

        os.writeInt(SaveBytesType.meta_chunk_segment_flag_seq.i);
        os.write(metaChunkSegmentFlagSeq.getInMemoryCachedBytes());
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
                // do every 100 loop, 1s
                if (loopCount % 100 != 0) {
                    return;
                }

                // do log every 1000s
                if (loopCount % (100 * 1000) == 0) {
                    log.info("Task {} run, slot={}, loop count={}", name(), slot, loopCount);
                }

                for (var replPair : replPairs) {
                    if (replPair.isSendBye()) {
                        continue;
                    }

                    if (!replPair.isAsMaster()) {
                        // only slave need send ping
                        replPair.ping();

                        var toFetchBigStringUuids = replPair.doingFetchBigStringUuid();
                        if (toFetchBigStringUuids != BigStringFiles.SKIP_UUID) {
                            var bytes = new byte[8];
                            ByteBuffer.wrap(bytes).putLong(toFetchBigStringUuids);
                            replPair.write(ReplType.incremental_big_string, new RawBytesContent(bytes));
                            log.info("Repl do fetch incremental big string, to server={}, uuid={}, slot={}",
                                    replPair.getHostAndPort(), toFetchBigStringUuids, slot);
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
                                log.warn("Remove repl pair after bye, to server={}, slot={}", replPair.getHostAndPort(), slot);
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

        if (ConfForGlobal.pureMemory) {
            // do every 10ms
            taskChain.add(new ITask() {
                private int lastCheckedSegmentIndex = 0;

                @Override
                public String name() {
                    return "check and save memory";
                }

                @Override
                public void run() {
                    OneSlot.this.checkAndSaveMemory(lastCheckedSegmentIndex);

                    lastCheckedSegmentIndex++;
                    if (lastCheckedSegmentIndex >= chunk.getMaxSegmentIndex()) {
                        lastCheckedSegmentIndex = 0;
                    }
                }

                public void setLoopCount(int loopCount) {
                }
            });
        }
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
                // do every 1000 loop, 10s
                if (loopCount % 1000 != 0) {
                    return;
                }

                // reduce log
                var firstOneSlot = LocalPersist.getInstance().firstOneSlot();
                if (firstOneSlot != null && slot == firstOneSlot.slot) {
                    log.info("Debug task run, slot={}, loop count={}", slot, loopCount);
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
            throw new IllegalStateException("Thread id not match, thread id=" + threadId + ", thread id protected for safe=" + threadIdProtectedForSafe);
        }
    }

    public Long getExpireAt(byte[] keyBytes, int bucketIndex, long keyHash, int keyHash32) {
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
        if (!ConfForGlobal.pureMemory) {
            var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
            var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
            var cvEncodedBytesFromLRU = lru.get(key);
            if (cvEncodedBytesFromLRU != null) {
                kvLRUHitTotal++;
                kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

                var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), keyBytes, keyHash);
                return cv.getExpireAt();
            }
        }

        kvLRUMissTotal++;
        return keyLoader.getExpireAt(bucketIndex, keyBytes, keyHash, keyHash32);
    }

    public int warmUp() {
        return keyLoader.warmUp();
    }

    public record BufOrCompressedValue(@Nullable ByteBuf buf, @Nullable CompressedValue cv) {
    }

    public BufOrCompressedValue get(byte[] keyBytes, int bucketIndex, long keyHash, int keyHash32) {
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
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (!ConfForGlobal.pureMemory) {
            var cvEncodedBytesFromLRU = lru.get(key);
            if (cvEncodedBytesFromLRU != null) {
                kvLRUHitTotal++;
                kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

                return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
            }
        }

        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueXByKey(bucketIndex, keyBytes, keyHash, keyHash32);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        var valueBytes = valueBytesWithExpireAtAndSeq.valueBytes();
        if (!PersistValueMeta.isPvm(valueBytes)) {
            // short value, just return, CompressedValue can decode
            if (!ConfForGlobal.pureMemory) {
                lru.put(key, valueBytes);
            }
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(valueBytes), null);
        }

        var pvm = PersistValueMeta.decode(valueBytes);
        var segmentBytes = getSegmentBytesBySegmentIndex(pvm.segmentIndex);
        if (segmentBytes == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm=" + pvm);
        }

        if (SegmentBatch2.isSegmentBytesSlim(segmentBytes, 0)) {
//            // crc check
//            var segmentSeq = buf.readLong();
//            var cvCount = buf.readInt();
//            var segmentMaskedValue = buf.readInt();
//            buf.skipBytes(SEGMENT_HEADER_LENGTH);

            var keyBytesAndValueBytes = SegmentBatch2.getKeyBytesAndValueBytesInSegmentBytesSlim(segmentBytes, pvm.subBlockIndex, pvm.segmentOffset);
            if (keyBytesAndValueBytes == null) {
                throw new IllegalStateException("No key value found in segment bytes slim, pvm=" + pvm);
            }

            var keyBytesRead = keyBytesAndValueBytes.keyBytes();
            if (!Arrays.equals(keyBytesRead, keyBytes)) {
                throw new IllegalStateException("Key not match, key=" + new String(keyBytes) + ", key persisted=" + new String(keyBytesRead));
            }

            // set to lru cache, just target bytes
            ByteBuf buf = Unpooled.wrappedBuffer(keyBytesAndValueBytes.valueBytes());
            var cv = CompressedValue.decode(buf, keyBytes, keyHash);
            if (!ConfForGlobal.pureMemory) {
                lru.put(key, cv.encode());
            }

            return new BufOrCompressedValue(null, cv);
        } else {
            if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
                segmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
            }
            ByteBuf buf = Unpooled.wrappedBuffer(segmentBytes);
//            SegmentBatch2.iterateFromSegmentBytes(segmentBytes, 0, segmentBytes.length, new SegmentBatch2.ForDebugCvCallback());

//            // crc check
//            var segmentSeq = buf.readLong();
//            var cvCount = buf.readInt();
//            var segmentMaskedValue = buf.readInt();
//            buf.skipBytes(SEGMENT_HEADER_LENGTH);

            buf.readerIndex(pvm.segmentOffset);

            // skip key header or check key
            var keyLength = buf.readShort();
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length=" + keyLength);
            }

            var keyBytesRead = new byte[keyLength];
            buf.readBytes(keyBytesRead);

            if (!Arrays.equals(keyBytesRead, keyBytes)) {
                throw new IllegalStateException("Key not match, key=" + new String(keyBytes) + ", key persisted=" + new String(keyBytesRead));
            }

            // set to lru cache, just target bytes
            var cv = CompressedValue.decode(buf, keyBytes, keyHash);
            if (!ConfForGlobal.pureMemory) {
                lru.put(key, cv.encode());
            }

            return new BufOrCompressedValue(null, cv);
        }
    }

    byte[] getFromWal(@NotNull String key, int bucketIndex) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        return targetWal.get(key);
    }

    private byte[] getSegmentBytesBySegmentIndex(int segmentIndex) {
        return chunk.preadOneSegment(segmentIndex);
    }

    public boolean exists(@NotNull String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            return !CompressedValue.isDeleted(cvEncodedFromWal);
        }

        var expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(bucketIndex, key.getBytes(), keyHash, keyHash32);
        return expireAtAndSeq != null && !expireAtAndSeq.isExpired();
    }

    public boolean remove(@NotNull String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

        if (exists(key, bucketIndex, keyHash, keyHash32)) {
            removeDelay(key, bucketIndex, keyHash);
            return true;
        } else {
            return false;
        }
    }

    @SlaveNeedReplay
    public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
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

    public long getThreadIdProtectedForSafe() {
        return threadIdProtectedForSafe;
    }

    public void put(@NotNull String key, int bucketIndex, @NotNull CompressedValue cv) {
        put(key, bucketIndex, cv, false);
    }

    @VisibleForTesting
    long ttlTotalInSecond = 0;
    @VisibleForTesting
    long putCountTotal = 0;

    public double getAvgTtlInSecond() {
        if (putCountTotal == 0) {
            return 0;
        }

        return (double) ttlTotalInSecond / putCountTotal;
    }

    @SlaveNeedReplay
    // thread safe, same slot, same event loop
    public void put(@NotNull String key, int bucketIndex, @NotNull CompressedValue cv, boolean isFromMerge) {
        checkCurrentThreadId();

        // before put check for better performance, todo
        if (isReadonly()) {
            throw new ReadonlyException();
        }

        putCountTotal++;
        if (!cv.noExpire()) {
            ttlTotalInSecond += (cv.getExpireAt() - System.currentTimeMillis()) / 1000;
        }

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];

        byte[] cvEncoded;
        boolean isValueShort;
        // is use faster kv style, save all key and value in record log
        if (ConfForGlobal.pureMemoryV2) {
            isValueShort = false;
        } else {
            isValueShort = cv.noExpire() && (cv.isTypeNumber() || cv.isShortString());
        }
        if (isValueShort) {
            if (cv.isTypeNumber()) {
                cvEncoded = cv.encodeAsNumber();
            } else {
                cvEncoded = cv.encodeAsShortString();
            }
        } else {
            cvEncoded = cv.encode();
        }
        var v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                key, cvEncoded, isFromMerge);

        // for big string, use single file
        boolean isPersistLengthOverSegmentLength = v.persistLength() + SEGMENT_HEADER_LENGTH > chunkSegmentLength;
        if (isPersistLengthOverSegmentLength || key.startsWith("kerry-test-big-string-")) {
            var uuid = snowFlake.nextId();
            var bytes = cv.getCompressedData();
            var isWriteOk = bigStringFiles.writeBigStringBytes(uuid, key, bytes);
            if (!isWriteOk) {
                throw new RuntimeException("Write big string file error, uuid=" + uuid + ", key=" + key);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            var xBigStrings = new XBigStrings(uuid, key, cvEncoded);
            appendBinlog(xBigStrings);

            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
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
    private void doPersist(int walGroupIndex, @NotNull String key, @NotNull Wal.PutResult putResult) {
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

        log.warn("One slot flush, s={}", slot);

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

    void initFds(@NotNull LibC libC) throws IOException {
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
            int currentSegmentIndex = chunk.getSegmentIndex();
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
            throw new IllegalStateException("Segment can not write after reset flag, s=" + slot + ", i=" + chunk.getSegmentIndex());
        }
    }

    public boolean hasData(int beginSegmentIndex, int segmentCount) {
        final boolean[] hasDataArray = {false};
        metaChunkSegmentFlagSeq.iterateRange(beginSegmentIndex, segmentCount, (segmentIndex, flagByte, seq, walGroupIndex) -> {
            if (!hasDataArray[0]) {
                if (!Chunk.Flag.canReuse(flagByte)) {
                    hasDataArray[0] = true;
                }
            }
        });
        return hasDataArray[0];
    }

    byte[] preadForMerge(int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        if (!hasData(beginSegmentIndex, segmentCount)) {
            return null;
        }

        return chunk.preadForMerge(beginSegmentIndex, segmentCount);
    }

    public byte[] preadForRepl(int beginSegmentIndex) {
        checkCurrentThreadId();

        if (!hasData(beginSegmentIndex, FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)) {
            return null;
        }

        return chunk.preadForRepl(beginSegmentIndex);
    }

    @SlaveReplay
    public void writeChunkSegmentsFromMasterExists(byte[] bytes, int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        if (bytes.length != chunk.chunkSegmentLength * segmentCount) {
            throw new IllegalStateException("Repl write chunk segments bytes length not match, bytes length=" + bytes.length +
                    ", chunk segment length=" + chunk.chunkSegmentLength + ", segment count=" + segmentCount + ", slot=" + slot);
        }

        chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(bytes, beginSegmentIndex, segmentCount);
        if (beginSegmentIndex % 4096 == 0) {
            log.warn("Repl write chunk segments from master exists, begin segment index={}, segment count={}, slot={}",
                    beginSegmentIndex, segmentCount, slot);
        }
    }

    @Override
    public void cleanUp() {
        checkCurrentThreadId();

        // close wal raf
        if (raf != null) {
            try {
                raf.close();
                System.out.println("Close wal raf success, slot=" + slot);

                rafShortValue.close();
                System.out.println("Close wal short value raf success, slot=" + slot);
            } catch (IOException e) {
                System.err.println("Close wal raf / wal short raf error, slot=" + slot);
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
    record BeforePersistWalExtFromMerge(@NotNull ArrayList<Integer> segmentIndexList,
                                        @NotNull ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList,
                                        @NotNull ArrayList<Integer> updatedFlagPersistedSegmentIndexList) {
        boolean isEmpty() {
            return segmentIndexList.isEmpty();
        }
    }

    record BeforePersistWalExt2FromMerge(@NotNull ArrayList<Integer> segmentIndexList,
                                         @NotNull ArrayList<Wal.V> vList) {
    }

    // for performance, before persist wal, read some segment in same wal group and  merge immediately
    @VisibleForTesting
    BeforePersistWalExtFromMerge readSomeSegmentsBeforePersistWal(int walGroupIndex) {
        var currentSegmentIndex = chunk.getSegmentIndex();
        var needMergeSegmentIndex = chunk.needMergeSegmentIndex(false, currentSegmentIndex);

        if (ConfForGlobal.pureMemory) {
            // do pre-read and merge more frequently to save memory
            if (chunk.calcSegmentCountStepWhenOverHalfEstimateKeyNumber != 0) {
                if (currentSegmentIndex >= chunk.calcSegmentCountStepWhenOverHalfEstimateKeyNumber) {
                    var begin = currentSegmentIndex - chunk.calcSegmentCountStepWhenOverHalfEstimateKeyNumber - 1;
                    if (begin < 0) {
                        begin = 0;
                    }
                    needMergeSegmentIndex = begin;
                } else {
                    needMergeSegmentIndex = chunk.maxSegmentIndex + currentSegmentIndex - chunk.calcSegmentCountStepWhenOverHalfEstimateKeyNumber;
                }
            }
        }

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
        ArrayList<Integer> updatedFlagPersistedSegmentIndexList = new ArrayList<>();

        for (int i = 0; i < segmentCount; i++) {
            var segmentIndex = firstSegmentIndex + i;
            int relativeOffsetInBatchBytes = i * chunkSegmentLength;

            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, 0);

                // clear merged but not persisted in chunk merge worker
                ArrayList<Integer> innerSegmentIndexList = new ArrayList<>();
                innerSegmentIndexList.add(segmentIndex);
                chunkMergeWorker.removeMergedButNotPersisted(innerSegmentIndexList, walGroupIndex);

                updatedFlagPersistedSegmentIndexList.add(segmentIndex);

                if (doLog) {
                    log.info("Set segment flag to persisted as not write at all, s={}, i={}", slot, segmentIndex);
                }
                continue;
            }

            ChunkMergeJob.readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, slot);
            segmentIndexList.add(segmentIndex);
        }

        return new BeforePersistWalExtFromMerge(segmentIndexList, cvList, updatedFlagPersistedSegmentIndexList);
    }

    @VisibleForTesting
    long logMergeCount = 0;

    @VisibleForTesting
    long extCvListCheckCountTotal = 0;
    @VisibleForTesting
    long extCvValidCountTotal = 0;
    @VisibleForTesting
    long extCvInvalidCountTotal = 0;

    @SlaveNeedReplay
    @VisibleForTesting
    void persistWal(boolean isShortValue, @NotNull Wal targetWal) {
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

        KeyBucketsInOneWalGroup keyBucketsInOneWalGroup = null;
        if (ext != null) {
            extCvListCheckCountTotal++;

            // add to binlog if some segments flag updated
            var tmpSegmentList = ext.updatedFlagPersistedSegmentIndexList();
            if (!tmpSegmentList.isEmpty()) {
                for (var si : tmpSegmentList) {
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(si, Flag.merged_and_persisted.flagByte, 0L);
                }
            }

            var cvList = ext.cvList;
            var cvListCount = cvList.size();

            int validCount = 0;
            // remove those wal exist
            cvList.removeIf(one -> delayToKeyBucketShortValues.containsKey(one.key) || delayToKeyBucketValues.containsKey(one.key));
            if (!cvList.isEmpty()) {
                // remove those expired or updated
                keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, keyLoader);

                for (var one : cvList) {
                    var cv = one.cv;
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash(), keyLoader.bucketsPerSlot);
                    var extWalGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
                    if (extWalGroupIndex != walGroupIndex) {
                        throw new IllegalStateException("Wal group index not match, s=" + slot + ", wal group index=" + walGroupIndex + ", ext wal group index=" + extWalGroupIndex);
                    }

                    var expireAtAndSeq = keyBucketsInOneWalGroup.getExpireAtAndSeq(bucketIndex, one.key.getBytes(), cv.getKeyHash());
                    var isThisKeyExpired = expireAtAndSeq == null || expireAtAndSeq.isExpired();
                    if (isThisKeyExpired) {
                        continue;
                    }

                    var isThisKeyUpdated = expireAtAndSeq.seq() != cv.getSeq();
                    if (isThisKeyUpdated) {
                        continue;
                    }

                    list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                            one.key, cv.encode(), true));
                    validCount++;
                }
            }
            int invalidCount = cvListCount - validCount;
            extCvValidCountTotal += validCount;
            extCvInvalidCountTotal += invalidCount;
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

        var needMergeSegmentIndexList = chunk.persist(walGroupIndex, list, false, xForBinlog, keyBucketsInOneWalGroup);
        if (needMergeSegmentIndexList == null) {
            throw new IllegalStateException("Persist error, need merge segment index list is null, slot=" + slot);
        }

        if (ext != null && !ext.isEmpty()) {
            var segmentIndexList = ext.segmentIndexList;
            // continuous segment index
            if (segmentIndexList.getLast() - segmentIndexList.getFirst() == segmentIndexList.size() - 1) {
                setSegmentMergeFlagBatch(segmentIndexList.getFirst(), segmentIndexList.size(),
                        Flag.merged_and_persisted.flagByte, null, walGroupIndex);

                // clear merged but not persisted in chunk merge worker
                chunkMergeWorker.removeMergedButNotPersisted(segmentIndexList, walGroupIndex);

                for (var segmentIndex : segmentIndexList) {
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted.flagByte, 0L);
                }
            } else {
                // when read some segments before persist wal, meta is continuous, but segments read from chunk may be null, skip some, so is not continuous anymore
                // usually not happen
                for (var segmentIndex : segmentIndexList) {
                    setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, walGroupIndex);

                    // clear merged but not persisted in chunk merge worker
                    ArrayList<Integer> innerSegmentIndexList = new ArrayList<>();
                    innerSegmentIndexList.add(segmentIndex);
                    chunkMergeWorker.removeMergedButNotPersisted(innerSegmentIndexList, walGroupIndex);

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

            chunkMergeWorker.removeMergedButNotPersisted(segmentIndexList, walGroupIndex);
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

        var currentSegmentIndex = chunk.getSegmentIndex();
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
        var currentSegmentIndex = chunk.getSegmentIndex();

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
        if (beginSegmentIndex < 0 || beginSegmentIndex + segmentCount - 1 > chunk.maxSegmentIndex) {
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
                if (segmentIndex % 4096 == 0) {
                    log.info("Clear one segment memory bytes when reuse, segment index={}, slot={}", segmentIndex, slot);
                }
            }
        }
    }

    public void setSegmentMergeFlagBatch(int beginSegmentIndex,
                                         int segmentCount,
                                         byte flagByte,
                                         @Nullable List<Long> segmentSeqList,
                                         int walGroupIndex) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(beginSegmentIndex, segmentCount,
                flagByte, segmentSeqList, walGroupIndex);

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
    int doMergeJob(@NotNull ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    @VisibleForTesting
    int doMergeJobWhenServerStart(@NotNull ArrayList<Integer> needMergeSegmentIndexList) {
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

    @MasterReset
    public void resetAsMaster() throws IOException {
        log.warn("Repl reset as master, slot={}", slot);
        persistMergingOrMergedSegmentsButNotPersisted();
        checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false);
        getMergedSegmentIndexEndLastTime();

        // set binlog same as old master last updated
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);

        binlog.reopenAtFileIndexAndMarginOffset(lastUpdatedFileIndex, marginLastUpdatedOffset);
        binlog.moveToNextSegment(true);
        log.warn("Repl reset binlog file index and offset as old master, file index={}, offset={}, slot={}",
                lastUpdatedFileIndex, marginLastUpdatedOffset, slot);

        // clear old as slave catch up binlog info
        // need fetch from the beginning, for data consistency
        // when next time begin slave again
        metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset();

        if (isReadonly()) {
            setReadonly(false);
        }
        if (!isCanRead()) {
            setCanRead(true);
        }
        log.warn("Repl reset readonly false and can read, slot={}", slot);
    }

    @SlaveReset
    public void resetAsSlave(@NotNull String host, int port) throws IOException {
        // clear old as slave catch up binlog info
        // need fetch from the beginning, for data consistency
        metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset();
        log.warn("Repl clear fetched binlog file index and offset as old slave, slot={}", slot);

        binlog.moveToNextSegment();

        createReplPairAsSlave(host, port);

        if (!isReadonly()) {
            setReadonly(true);
        }
        if (isCanRead()) {
            setCanRead(false);
        }
        log.warn("Repl reset readonly true and can not read, slot={}", slot);

        // do not write binlog as slave
        dynConfig.setBinlogOn(false);
        log.warn("Repl reset binlog on false as slave, slot={}", slot);
    }

    private int mergeTargetSegments(@NotNull ArrayList<Integer> needMergeSegmentIndexList, boolean isServerStart) {
        int validCvCountTotal = 0;

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();

        // continuous
        if (lastSegmentIndex - firstSegmentIndex + 1 == needMergeSegmentIndexList.size()) {
            var validCvCount = isServerStart ? doMergeJobWhenServerStart(needMergeSegmentIndexList) : doMergeJob(needMergeSegmentIndexList);
            log.warn("Merge segments, is server start={}, s={}, i={}, end i={}, valid cv count after run={}",
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
                        log.warn("Merge segments, is server start={}, once list, s={}, i={}, end i={}, valid cv count after run={}",
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
                log.warn("Merge segments, is server start={}, once list, s={}, i={}, end i={}, valid cv count after run={}",
                        isServerStart, slot, onceList.getFirst(), onceList.getLast(), validCvCount);
                validCvCountTotal += validCvCount;
            }
        }

        return validCvCountTotal;
    }

    @MasterReset
    public void getMergedSegmentIndexEndLastTime() {
        if (chunkMergeWorker.getLastMergedSegmentIndex() != NO_NEED_MERGE_SEGMENT_INDEX) {
            chunk.setMergedSegmentIndexEndLastTime(chunkMergeWorker.getLastMergedSegmentIndex());
        } else {
            // todo, need check
            var mergedSegmentIndexEndLastTime = metaChunkSegmentFlagSeq.getMergedSegmentIndexEndLastTime(chunk.getSegmentIndex(), chunk.halfSegmentNumber);
            chunk.setMergedSegmentIndexEndLastTime(mergedSegmentIndexEndLastTime);
            chunkMergeWorker.setLastMergedSegmentIndex(mergedSegmentIndexEndLastTime);
        }

        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart();
        log.info("Get merged segment index end last time, s={}, i={}", slot, chunk.getMergedSegmentIndexEndLastTime());
    }

    @VisibleForTesting
    void checkAndSaveMemory(int targetSegmentIndex) {
        var segmentFlag = metaChunkSegmentFlagSeq.getSegmentMergeFlag(targetSegmentIndex);
        if (segmentFlag.flagByte() != Flag.new_write.flagByte() && segmentFlag.flagByte() != Flag.reuse_new.flagByte()) {
            return;
        }

        var segmentBytes = chunk.preadOneSegment(targetSegmentIndex, false);
        if (segmentBytes == null) {
            return;
        }

        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE * 10);
        int expiredCount = ChunkMergeJob.readToCvList(cvList, segmentBytes, 0, chunkSegmentLength, targetSegmentIndex, slot);

        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> invalidCvList = new ArrayList<>();

        for (var one : cvList) {
            var cv = one.cv;
            var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash(), keyLoader.bucketsPerSlot);

            var tmpWalValueBytes = getFromWal(one.key, bucketIndex);
            if (tmpWalValueBytes != null) {
                invalidCvList.add(one);
                continue;
            }

            var keyBytes = one.key.getBytes();
            var keyHash32 = KeyHash.hash32(keyBytes);
            var expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(bucketIndex, keyBytes, cv.getKeyHash(), keyHash32);
            if (expireAtAndSeq == null || expireAtAndSeq.isExpired()) {
                invalidCvList.add(one);
                continue;
            }

            if (expireAtAndSeq.seq() != cv.getSeq()) {
                invalidCvList.add(one);
            }
        }

        // all is invalid
        if (invalidCvList.size() == cvList.size()) {
            setSegmentMergeFlag(targetSegmentIndex, Flag.merged_and_persisted.flagByte, 0L, 0);

            var xChunkSegmentFlagUpdate = new XChunkSegmentFlagUpdate();
            xChunkSegmentFlagUpdate.putUpdatedChunkSegmentFlagWithSeq(targetSegmentIndex, Flag.merged_and_persisted.flagByte, 0L);
            appendBinlog(xChunkSegmentFlagUpdate);
            return;
        }

        var totalCvCount = expiredCount + cvList.size();
        var invalidRate = (invalidCvList.size() + expiredCount) * 100 / totalCvCount;
        final int invalidMergedTriggerRate = 50;
        if (invalidRate > invalidMergedTriggerRate) {
            var validCvList = cvList.stream().filter(one -> !invalidCvList.contains(one)).toList();
            var encodedSlim = SegmentBatch2.encodeValidCvListSlim(validCvList);
            if (encodedSlim != null) {
                chunk.writeSegmentsFromMasterExistsOrAfterSegmentSlim(encodedSlim, targetSegmentIndex, 1);

                var xChunkSegmentSlimUpdate = new XChunkSegmentSlimUpdate(targetSegmentIndex, encodedSlim);
                appendBinlog(xChunkSegmentSlimUpdate);
            }
        }
    }

    @VisibleForTesting
    final static SimpleGauge globalGauge = new SimpleGauge("global", "Global metrics.");

    static {
        globalGauge.register();
    }

    void initMetricsCollect() {
        globalGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            // only first slot show global metrics
            var firstOneSlot = LocalPersist.getInstance().firstOneSlot();
            if (firstOneSlot == null || slot == firstOneSlot.slot) {
                return map;
            }

            // global use slot -1
            var labelValues = List.of("-1");

            // only show global
            map.put("global_up_time", new SimpleGauge.ValueWithLabelValues((double) MultiWorkerServer.UP_TIME, labelValues));
            map.put("global_dict_size", new SimpleGauge.ValueWithLabelValues((double) DictMap.getInstance().dictSize(), labelValues));
            // global config for one slot
            map.put("global_estimate_key_number", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.estimateKeyNumber, labelValues));
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

    // change dyn config or global config, todo
    private static final int BIG_KEY_LENGTH_CHECK = 2048;

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
            map.put("binlog_current_offset_from_the_beginning", (double) binlog.currentReplOffset());
        }

        if (bigKeyTopK != null) {
            map.put("big_key_count", (double) bigKeyTopK.sizeIfBiggerThan(BIG_KEY_LENGTH_CHECK));
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

        var replPairAsSlave = getOnlyOneReplPairAsSlave();
        if (replPairAsSlave != null) {
            map.put("repl_as_slave_is_all_caught_up", (double) (replPairAsSlave.isAllCaughtUp() ? 1 : 0));
            map.put("repl_as_slave_catch_up_last_seq", (double) replPairAsSlave.getSlaveCatchUpLastSeq());
            map.put("repl_as_slave_is_link_up", (double) (replPairAsSlave.isLinkUp() ? 1 : 0));
            map.put("repl_as_slave_fetched_bytes_total", (double) replPairAsSlave.getFetchedBytesLengthTotal());
            map.put("repl_as_slave_is_master_readonly", (double) (replPairAsSlave.isMasterReadonly() ? 1 : 0));
            map.put("repl_as_slave_is_master_can_not_connect", (double) (replPairAsSlave.isMasterCanNotConnect() ? 1 : 0));

            var slaveFo = replPairAsSlave.getSlaveLastCatchUpBinlogFileIndexAndOffset();
            if (slaveFo != null) {
                map.put("repl_as_slave_last_catch_up_binlog_file_index", (double) slaveFo.fileIndex());
                map.put("repl_as_slave_last_catch_up_binlog_offset", (double) slaveFo.offset());
            }

            var masterFo = replPairAsSlave.getMasterBinlogCurrentFileIndexAndOffset();
            if (masterFo != null) {
                map.put("repl_as_slave_master_binlog_current_file_index", (double) masterFo.fileIndex());
                map.put("repl_as_slave_master_binlog_current_offset", (double) masterFo.offset());
            }
        }

        var replPairAsMasterList = getReplPairAsMasterList();
        if (!replPairAsMasterList.isEmpty()) {
            map.put("repl_as_master_repl_pair_size", (double) replPairAsMasterList.size());

            for (int i = 0; i < replPairAsMasterList.size(); i++) {
                var replPairAsMaster = replPairAsMasterList.get(i);
                map.put("repl_as_master_repl_pair_" + i + "_is_all_caught_up", (double) (replPairAsMaster.isAllCaughtUp() ? 1 : 0));
                map.put("repl_as_master_repl_pair_" + i + "_is_link_up", (double) (replPairAsMaster.isLinkUp() ? 1 : 0));
                map.put("repl_as_master_repl_pair_" + i + "_fetched_bytes_total", (double) replPairAsMaster.getFetchedBytesLengthTotal());

                var slaveFo = replPairAsMaster.getSlaveLastCatchUpBinlogFileIndexAndOffset();
                if (slaveFo != null) {
                    map.put("repl_as_master_repl_pair_" + i + "_slave_last_catch_up_binlog_file_index", (double) slaveFo.fileIndex());
                    map.put("repl_as_master_repl_pair_" + i + "_slave_last_catch_up_binlog_offset", (double) slaveFo.offset());
                }
            }
        }

        map.put("slot_pending_submit_index_job_count", (double) pendingSubmitIndexJobRunCount);

        map.put("slot_avg_ttl_in_second", getAvgTtlInSecond());

        if (extCvListCheckCountTotal > 0) {
            map.put("slot_ext_cv_list_check_count_total", (double) extCvListCheckCountTotal);
            map.put("slot_ext_cv_valid_count_total", (double) extCvValidCountTotal);
            map.put("slot_ext_cv_invalid_count_total", (double) extCvInvalidCountTotal);
            map.put("slot_ext_cv_invalid_once_check", (double) extCvInvalidCountTotal / extCvListCheckCountTotal);

            var sum = extCvValidCountTotal + extCvInvalidCountTotal;
            if (sum > 0) {
                map.put("slot_ext_cv_invalid_ratio", (double) extCvInvalidCountTotal / sum);
            }
        }

        return map;
    }
}
