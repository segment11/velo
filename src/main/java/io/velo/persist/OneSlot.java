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
import io.velo.command.BlockingList;
import io.velo.metric.InSlotMetricCollector;
import io.velo.metric.SimpleGauge;
import io.velo.monitor.BigKeyTopK;
import io.velo.repl.*;
import io.velo.repl.content.RawBytesContent;
import io.velo.repl.incremental.XBigStrings;
import io.velo.repl.incremental.XFlush;
import io.velo.repl.incremental.XUpdateSeq;
import io.velo.repl.incremental.XWalV;
import io.velo.task.ITask;
import io.velo.task.TaskChain;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static io.velo.persist.Chunk.Flag;
import static io.velo.persist.Chunk.SegmentFlag;
import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
import static io.velo.persist.SegmentBatch2.SEGMENT_HEADER_LENGTH;

public class OneSlot implements InMemoryEstimate, InSlotMetricCollector, NeedCleanUp, HandlerWhenCvExpiredOrDeleted {
    /**
     * Constructor for unit testing only, with specified slot, directory, key loader, and write-ahead log (WAL).
     *
     * @param slot      the slot index
     * @param slotDir   the directory of the slot
     * @param keyLoader the key loader
     * @param wal       the write-ahead log
     * @throws IOException if an I/O error occurs
     */
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
    }

    /**
     * Constructor for unit testing only, with specified slot.
     * only for local persist one slot array
     *
     * @param slot the slot index
     */
    @TestOnly
    public OneSlot(short slot) {
        this(slot, null);
    }

    /**
     * Constructor for async execution or calls, requiring an event loop.
     * only for async run/call
     *
     * @param slot      the slot index
     * @param eventloop the event loop
     */
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

        this.slotWorkerEventloop = eventloop;
    }

    /**
     * Main constructor for OneSlot, initializes all necessary components and configurations.
     *
     * @param slot          the slot index
     * @param slotNumber    the slot number
     * @param snowFlake     the SnowFlake ID generator
     * @param persistDir    the directory for persistent storage
     * @param persistConfig the persistent configuration
     * @throws IOException if an I/O error occurs
     */
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

        var walSharedFile = new File(slotDir, "wal.dat");
        if (!walSharedFile.exists()) {
            FileUtils.touch(walSharedFile);
        }
        this.raf = new RandomAccessFile(walSharedFile, "rw");
        var lruMemoryRequireMBWriteInWal = walSharedFile.length() / 1024 / 1024;
        log.info("LRU prepare, type={}, MB={}, slot={}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal, false);
        this.walWriteIndex = this.raf.length();

        var walSharedFileShortValue = new File(slotDir, "wal-short-value.dat");
        if (!walSharedFileShortValue.exists()) {
            FileUtils.touch(walSharedFileShortValue);
        }
        this.rafShortValue = new RandomAccessFile(walSharedFileShortValue, "rw");
        var lruMemoryRequireMBWriteInWal2 = walSharedFileShortValue.length() / 1024 / 1024;
        log.info("LRU prepare, type={}, short value, MB={}, slot={}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal2, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, slotStr, (int) lruMemoryRequireMBWriteInWal2, false);
        this.walShortValueWriteIndex = this.rafShortValue.length();

        long initMemoryN = 0;
        for (int i = 0; i < walGroupNumber; i++) {
            var wal = new Wal(slot, this, i, raf, rafShortValue, snowFlake);
            walArray[i] = wal;
            initMemoryN += wal.initMemoryN;
        }

        int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
        log.info("Static memory init, type={}, MB={}, slot={}", StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, false);

        initLRU(false);

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, this);
        // meta data
        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        this.binlog = new Binlog(slot, slotDir, dynConfig);
        initBigKeyTopK(10);

        this.initTasks();
    }

    /**
     * Estimates the memory usage of OneSlot and its components.
     *
     * @param sb the StringBuilder to append the estimation details
     * @return the total estimated memory usage in bytes
     */
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
        size += binlog.estimate(sb);
        for (var wal : walArray) {
            size += wal.estimate(sb);
        }
        return size;
    }

    /**
     * Initializes the Least Recently Used (LRU) cache for the OneSlot.
     *
     * @param doRemoveForStats whether to remove statistics for LRU preparation
     */
    public void initLRU(boolean doRemoveForStats) {
        int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;
        var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;
        if (maxSizeForEachWalGroup < 10) {
            maxSizeForEachWalGroup = 10;
        }
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


    /**
     * Returns the string representation of OneSlot.
     *
     * @return the string representation
     */
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

    /**
     * Returns the unique master UUID of the OneSlot.
     *
     * @return the master UUID
     */
    public long getMasterUuid() {
        return masterUuid;
    }

    private final ArrayList<ReplPair> replPairs = new ArrayList<>();

    @VisibleForTesting
    public ArrayList<ReplPair> getReplPairs() {
        return replPairs;
    }

    /**
     * Checks if the OneSlot is operating as a slave in any replication pairs.
     *
     * @return true if operating as a slave, false otherwise
     */
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

    /**
     * Returns the list of replication pairs where OneSlot is the master.
     *
     * @return the list of replication pairs
     */
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

    /**
     * Adds a replication pair to the delay close list.
     *
     * @param replPair the replication pair to add
     */
    public void addDelayNeedCloseReplPair(@NotNull ReplPair replPair) {
        replPair.setPutToDelayListToRemoveTimeMillis(System.currentTimeMillis());
        delayNeedCloseReplPairs.add(replPair);
    }

    @TestOnly
    private boolean doMockWhenCreateReplPairAsSlave = false;

    /**
     * Sets the mock flag for creating a replication pair as a slave.
     *
     * @param doMockWhenCreateReplPairAsSlave the mock flag
     */
    public void setDoMockWhenCreateReplPairAsSlave(boolean doMockWhenCreateReplPairAsSlave) {
        this.doMockWhenCreateReplPairAsSlave = doMockWhenCreateReplPairAsSlave;
    }


    /**
     * Creates a replication pair as a slave.
     * todo, both master - master, need change equal and init as master or slave
     *
     * @param host the host of the slave
     * @param port the port of the slave
     * @return the created replication pair
     */
    public ReplPair createReplPairAsSlave(@NotNull String host, int port) {
        var replPair = new ReplPair(slot, false, host, port);
        replPair.setSlaveUuid(masterUuid);

        if (doMockWhenCreateReplPairAsSlave) {
            log.info("Repl create repl pair as slave, mock, host={}, port={}, slot={}", host, port, slot);
        } else {
            replPair.initAsSlave(slotWorkerEventloop, requestHandler);
            log.warn("Repl create repl pair as slave, host={}, port={}, slot={}", host, port, slot);
        }
        replPairs.add(replPair);
        return replPair;
    }

    /**
     * Removes all replication pairs where OneSlot is the slave.
     *
     * @return true if a replication pair was removed, false otherwise
     */
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

    /**
     * Retrieves a replication pair where OneSlot is the master by slave UUID.
     *
     * @param slaveUuid the slave UUID
     * @return the replication pair, or null if not found
     */
    public @Nullable ReplPair getReplPairAsMaster(long slaveUuid) {
        var list = getReplPairAsMasterList();
        return list.stream().filter(one -> one.getSlaveUuid() == slaveUuid).findFirst().orElse(null);
    }

    /**
     * Retrieves the first replication pair where OneSlot is the master.
     *
     * @return the first replication pair, or null if not found
     */
    public @Nullable ReplPair getFirstReplPairAsMaster() {
        var list = getReplPairAsMasterList();
        return list.isEmpty() ? null : list.getFirst();
    }

    /**
     * Retrieves all replication pairs where OneSlot is the master.
     *
     * @return the list of replication pairs
     */
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

    /**
     * Retrieves a replication pair where OneSlot is the slave by slave UUID.
     *
     * @param slaveUuid the slave UUID
     * @return the replication pair, or null if not found
     */
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

    /**
     * Retrieves the only replication pair where OneSlot is the slave.
     *
     * @return the replication pair, or null if not found
     */
    public @Nullable ReplPair getOnlyOneReplPairAsSlave() {
        return getReplPairAsSlave(masterUuid);
    }

    /**
     * Creates a replication pair as a master if it does not already exist.
     *
     * @param slaveUuid the slave UUID
     * @param host      the host of the master
     * @param port      the port of the master
     * @return the created or existing replication pair
     */
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

    /**
     * Sets the event loop for slot operations.
     *
     * @param slotWorkerEventloop the event loop
     */
    public void setSlotWorkerEventloop(Eventloop slotWorkerEventloop) {
        this.slotWorkerEventloop = slotWorkerEventloop;
    }

    private Eventloop slotWorkerEventloop;

    /**
     * Sets the request handler for processing requests.
     *
     * @param requestHandler the request handler
     */
    public void setRequestHandler(@NotNull RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private RequestHandler requestHandler;

    /**
     * Asynchronously runs a given task.
     *
     * @param runnableEx the task to run
     * @return the promise representing the asynchronous computation
     */
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

        return Promise.ofFuture(slotWorkerEventloop.submit(runnableEx));
    }

    /**
     * Asynchronously calls a supplier and returns the result.
     *
     * @param supplierEx the supplier to call
     * @param <T>        the return type of the supplier
     * @return the promise representing the result of the supplier
     */
    public <T> Promise<T> asyncCall(@NotNull SupplierEx<T> supplierEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                return Promise.of(supplierEx.get());
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(slotWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
    }

    /**
     * Delays the execution of a task by a specified number of milliseconds.
     *
     * @param millis   the delay in milliseconds
     * @param runnable the task to run
     */
    public void delayRun(int millis, @NotNull Runnable runnable) {
        // for unit test
        if (slotWorkerEventloop == null) {
            return;
        }

        slotWorkerEventloop.delay(millis, runnable);
    }

    final ArrayList<HandlerWhenCvExpiredOrDeleted> handlersRegisteredList = new ArrayList<>();

    /**
     * Handles the event when a compressed value (CV) expires or is deleted.
     *
     * @param key           the key associated with the CV
     * @param shortStringCv the compressed value for short strings, nullable
     * @param pvm           the metadata of the persisted value, nullable
     */
    @Override
    public void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm) {
        for (var handler : handlersRegisteredList) {
            handler.handleWhenCvExpiredOrDeleted(key, shortStringCv, pvm);
        }
    }

    private final short slot;
    private final String slotStr;
    private final short slotNumber;

    /**
     * Returns the slot index.
     *
     * @return the slot index
     */
    public short slot() {
        return slot;
    }

    private final int chunkSegmentLength;

    final SnowFlake snowFlake;

    /**
     * Returns the SnowFlake ID generator used for generating unique identifiers.
     *
     * @return the SnowFlake ID generator
     */
    public SnowFlake getSnowFlake() {
        return snowFlake;
    }

    private final File slotDir;

    /**
     * Returns the slot directory.
     *
     * @return the slot directory
     */
    public File getSlotDir() {
        return slotDir;
    }

    private final BigStringFiles bigStringFiles;

    /**
     * Returns the BigStringFiles component used for handling large strings.
     *
     * @return the BigStringFiles component
     */
    public BigStringFiles getBigStringFiles() {
        return bigStringFiles;
    }

    /**
     * Returns the directory for storing big strings.
     *
     * @return the big string directory
     */
    public File getBigStringDir() {
        return bigStringFiles.bigStringDir;
    }

    private final Map<Integer, LRUMap<String, byte[]>> kvByWalGroupIndexLRU = new HashMap<>();

    /**
     * Returns the total count of key-value entries in the LRU cache for all WAL groups.
     *
     * @return the total count of key-value entries
     */
    public int kvByWalGroupIndexLRUCountTotal() {
        int n = 0;
        for (var lru : kvByWalGroupIndexLRU.values()) {
            n += lru.size();
        }
        return n;
    }

    /**
     * Estimates the total in-memory size of the LRU cache.
     * perf bad, just for test or debug
     *
     * @return the total in-memory size in bytes
     */
    public long inMemorySizeOfLRU() {
        long size = 0;
        for (var lru : kvByWalGroupIndexLRU.values()) {
            size += RamUsageEstimator.sizeOfMap(lru);
        }
        return size;
    }

    @VisibleForTesting
    int lruClearedCount = 0;

    /**
     * Returns a random key from the LRU cache of a specified WAL group.
     *
     * @param walGroupIndex the WAL group index
     * @return a random key, or null if the LRU cache is empty
     */
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

    /**
     * Clears the key-value entries in the LRU cache of a specified WAL group.
     *
     * @param walGroupIndex the WAL group index
     * @return the count of cleared key-value entries
     */
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

    /**
     * Puts a key-value entry into the LRU cache of a specified WAL group.
     *
     * @param walGroupIndex the WAL group index
     * @param key           the key
     * @param cvEncoded     the compressed value as a byte array
     */
    @TestOnly
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

    private static final String DYN_CONFIG_FILE_NAME = "dyn-config.json";

    private final DynConfig dynConfig;

    /**
     * Returns the DynConfig component used for dynamic configuration.
     *
     * @return the DynConfig component
     */
    public DynConfig getDynConfig() {
        return dynConfig;
    }

    /**
     * Updates a configuration key with a given value string.
     *
     * @param key         the configuration key
     * @param valueString the value string
     * @return true if the update was successful
     * @throws IOException if an I/O error occurs
     */
    public boolean updateDynConfig(@NotNull String key, @NotNull String valueString) throws IOException {
        if (key.equals("testKey")) {
            dynConfig.setTestKey(Integer.parseInt(valueString));
        } else {
            dynConfig.update(key, valueString);
        }
        return true;
    }

    /**
     * Checks if OneSlot is in read-only mode.
     *
     * @return true if in read-only mode, false otherwise
     */
    public boolean isReadonly() {
        return dynConfig.isReadonly();
    }

    /**
     * Sets the read-only mode of OneSlot.
     *
     * @param readonly the read-only mode
     * @throws IOException if an I/O error occurs
     */
    public void setReadonly(boolean readonly) throws IOException {
        dynConfig.setReadonly(readonly);
    }

    /**
     * Checks if OneSlot can read.
     *
     * @return true if it can read, false otherwise
     */
    public boolean isCanRead() {
        return dynConfig.isCanRead();
    }

    /**
     * Sets the read capability of OneSlot.
     *
     * @param canRead the read capability
     * @throws IOException if an I/O error occurs
     */
    public void setCanRead(boolean canRead) throws IOException {
        dynConfig.setCanRead(canRead);
    }

    private final int walGroupNumber;
    // index is group index
    private final Wal[] walArray;

    private long walWriteIndex;
    private long walShortValueWriteIndex;

    /**
     * Reads WAL data from files asynchronously.
     *
     * @return the promise representing the asynchronous computation
     */
    CompletableFuture<Boolean> walLazyReadFromFile() {
        var waitF = new CompletableFuture<Boolean>();
        // just run once
        new Thread(() -> {
            log.info("Start a single thread to read wal from file or load saved file when pure memory, slot={}", slot);
            try {
                for (var wal : walArray) {
                    wal.lazyReadFromFile();
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

    /**
     * Retrieves the WAL for a specified bucket index.
     *
     * @param bucketIndex the bucket index
     * @return the WAL
     */
    public Wal getWalByBucketIndex(int bucketIndex) {
        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        return walArray[walGroupIndex];
    }

    /**
     * Retrieves the WAL for a specified WAL group index.
     *
     * @param walGroupIndex the WAL group index
     * @return the WAL
     */
    public Wal getWalByGroupIndex(int walGroupIndex) {
        return walArray[walGroupIndex];
    }

    private final RandomAccessFile raf;
    private final RandomAccessFile rafShortValue;

    final KeyLoader keyLoader;

    /**
     * Returns the key loader used for managing keys.
     *
     * @return the key loader
     */
    public KeyLoader getKeyLoader() {
        return keyLoader;
    }

    /**
     * Returns the total count of keys stored in WALs.
     *
     * @return the total count of keys
     */
    public long getWalKeyCount() {
        long r = 0;
        for (var wal : walArray) {
            r += wal.getKeyCount();
        }
        return r;
    }

    /**
     * Returns the total count of keys stored in OneSlot.
     *
     * @return the total count of keys
     */
    public long getAllKeyCount() {
        // for unit test
        if (keyLoader == null) {
            return 0;
        }
        return keyLoader.getKeyCount() + getWalKeyCount();
    }

    Chunk chunk;

    /**
     * Returns the Chunk component used for managing segments.
     *
     * @return the Chunk component
     */
    public Chunk getChunk() {
        return chunk;
    }

    MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;

    /**
     * Returns the MetaChunkSegmentFlagSeq component used for managing segment flags.
     *
     * @return the MetaChunkSegmentFlagSeq component
     */
    public MetaChunkSegmentFlagSeq getMetaChunkSegmentFlagSeq() {
        return metaChunkSegmentFlagSeq;
    }

    @VisibleForTesting
    MetaChunkSegmentIndex metaChunkSegmentIndex;

    /**
     * Returns the MetaChunkSegmentIndex component used for managing segment indices.
     *
     * @return the MetaChunkSegmentIndex component
     */
    public MetaChunkSegmentIndex getMetaChunkSegmentIndex() {
        return metaChunkSegmentIndex;
    }

    /**
     * Returns the write segment index as an integer.
     *
     * @return the write segment index
     */
    @VisibleForTesting
    public int getChunkWriteSegmentIndexInt() {
        return metaChunkSegmentIndex.get();
    }

    /**
     * Sets the write segment index as an integer.
     *
     * @param segmentIndex the write segment index
     */
    public void setMetaChunkSegmentIndexInt(int segmentIndex) {
        setMetaChunkSegmentIndexInt(segmentIndex, false);
    }

    /**
     * Sets the write segment index as an integer and optionally updates the chunk segment index.
     *
     * @param segmentIndex            the write segment index
     * @param updateChunkSegmentIndex whether to update the chunk segment index
     */
    public void setMetaChunkSegmentIndexInt(int segmentIndex, boolean updateChunkSegmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalArgumentException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }

        metaChunkSegmentIndex.set(segmentIndex);
        if (updateChunkSegmentIndex) {
            chunk.setSegmentIndex(segmentIndex);
        }
    }

    /**
     * Updates the chunk segment index from the metadata.
     */
    public void updateChunkSegmentIndexFromMeta() {
        chunk.setSegmentIndex(metaChunkSegmentIndex.get());
    }

    private final Binlog binlog;

    /**
     * Returns the Binlog component used for logging binary data.
     *
     * @return the Binlog component
     */
    public Binlog getBinlog() {
        return binlog;
    }

    /**
     * Appends content to the binlog.
     *
     * @param content the binlog content
     */
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

    /**
     * Returns the BigKeyTopK component used for monitoring big keys.
     *
     * @return the BigKeyTopK component
     */
    public BigKeyTopK getBigKeyTopK() {
        return bigKeyTopK;
    }

    /**
     * Initializes the BigKeyTopK component with the specified maximum size.
     *
     * @param k the maximum size
     */
    void initBigKeyTopK(int k) {
        // may be dyn config init already init big key top k
        if (bigKeyTopK == null) {
            bigKeyTopK = new BigKeyTopK(k);
        }
    }

    /**
     * Monitors a big key by its value length.
     *
     * @param key              the key
     * @param valueBytesLength the length of the value
     */
    public void monitorBigKeyByValueLength(String key, int valueBytesLength) {
        if (valueBytesLength >= BIG_KEY_LENGTH_CHECK) {
            bigKeyTopK.add(key, valueBytesLength);
        }
    }

    private final TaskChain taskChain = new TaskChain();

    /**
     * Returns the task chain used for managing tasks.
     *
     * @return the task chain
     */
    public TaskChain getTaskChain() {
        return taskChain;
    }

    /**
     * Executes scheduled tasks in the task chain.
     *
     * @param loopCount the loop count
     */
    public void doTask(int loopCount) {
        if (MultiWorkerServer.isStopping) {
            return;
        }

        taskChain.doTask(loopCount);

        var canTruncateFdIndex = metaChunkSegmentFlagSeq.canTruncateFdIndex;
        if (canTruncateFdIndex != -1) {
            truncateChunkFile(canTruncateFdIndex);
        }

        if (loopCount % 10 == 0) {
            // execute once every 100ms
            for (var wal : walArray) {
                // unit test
                if (wal == null) {
                    continue;
                }

                var count = wal.intervalDeleteExpiredBigStringFiles();
                if (count > 0 || wal.groupIndex == 0) {
                    log.debug("Wal interval delete expired big string files, slot={}, group index={}, refer big string files count={}",
                            slot, wal.groupIndex, count);
                }
            }
        }

        // execute once every 10ms
        intervalDeleteOverwriteBigStringFiles();
    }

    @VisibleForTesting
    final LinkedList<BigStringFiles.IdWithKey> delayToDeleteBigStringFileIds = new LinkedList<>();

    CompletableFuture<Integer> initCheck() {
        var waitF = new CompletableFuture<Integer>();
        // just run once
        new Thread(() -> {
            log.info("Start a single thread to do init check, slot={}", slot);
            try {
                int n = 0;
                // delete big string files those are overwritten
                if (bigStringFiles.bigStringFilesCount > 0) {
                    for (int i : bigStringFiles.bucketIndexesWhenFirstServerStart) {
                        n += intervalDeleteOverwriteBigStringFiles(i);
                    }
                }
                log.info("Init check delete overwrite big string files count={}", n);
                // no use after init check
                bigStringFiles.bucketIndexesWhenFirstServerStart.clear();
                waitF.complete(n);
            } catch (Exception e) {
                log.error("Init check error for slot=" + slot, e);
                waitF.completeExceptionally(e);
            } finally {
                log.info("End slot do init check, slot={}", slot);
            }
        }).start();
        return waitF;
    }

    @VisibleForTesting
    int deleteOverwriteBigStringFilesLastBucketIndex = 0;

    void intervalDeleteOverwriteBigStringFiles() {
        // unit test
        if (keyLoader == null) {
            return;
        }

        intervalDeleteOverwriteBigStringFiles(deleteOverwriteBigStringFilesLastBucketIndex);
        deleteOverwriteBigStringFilesLastBucketIndex++;
        if (deleteOverwriteBigStringFilesLastBucketIndex >= keyLoader.bucketsPerSlot) {
            deleteOverwriteBigStringFilesLastBucketIndex = 0;
        }
    }

    /**
     * Delete big string files those are overwritten.
     */
    int intervalDeleteOverwriteBigStringFiles(int targetBucketIndex) {
        if (!delayToDeleteBigStringFileIds.isEmpty()) {
            var oneId = delayToDeleteBigStringFileIds.removeFirst();
            bigStringFiles.deleteBigStringFileIfExist(oneId.uuid(), oneId.bucketIndex(), oneId.keyHash());
        }

        int count = 0;

        var idList = bigStringFiles.getBigStringFileIdList(targetBucketIndex);
        if (!idList.isEmpty()) {
            var walGroupIndex = Wal.calcWalGroupIndex(targetBucketIndex);
            var targetWal = walArray[walGroupIndex];

            var persistedIdWithKeyList = keyLoader.getPersistedBigStringIdList(targetBucketIndex);
            if (persistedIdWithKeyList.isEmpty()) {
                for (var id : idList) {
                    if (targetWal.bigStringFileUuidByKey.containsValue(id.uuid())) {
                        continue;
                    }

                    delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(id.uuid(), targetBucketIndex, id.keyHash(), ""));
                    count++;
                }
            } else {
                var map = new HashMap<Long, String>();
                for (var one : persistedIdWithKeyList) {
                    map.put(one.uuid(), one.key());
                }
                // check those not exists in key buckets or wal cached
                for (var id : idList) {
                    boolean canDelete;
                    if (map.containsKey(id.uuid())) {
                        var key = map.get(id.uuid());
                        // wal is newer
                        canDelete = targetWal.hasKey(key);
                    } else {
                        canDelete = !targetWal.bigStringFileUuidByKey.containsValue(id.uuid());
                    }

                    if (canDelete) {
                        delayToDeleteBigStringFileIds.add(new BigStringFiles.IdWithKey(id.uuid(), targetBucketIndex, id.keyHash(), ""));
                        count++;
                    }
                }
            }

            if (count > 0 && targetBucketIndex % 16384 == 0) {
                log.info("Interval delete overwrite big string files, slot={}, bucket index={}, count={}", slot, targetBucketIndex, count);
            }
        }
        return count;
    }

    /**
     * Truncates a chunk file at the specified file descriptor index.
     *
     * @param fdIndex the file descriptor index
     */
    void truncateChunkFile(int fdIndex) {
        // when do task interval unit test
        if (chunk == null) {
            return;
        }

        var fdLength = chunk.fdLengths[fdIndex];
        if (fdLength != 0) {
            var fd = chunk.fdReadWriteArray[fdIndex];
            fd.truncate();
            chunk.fdLengths[fdIndex] = 0;
        }
        metaChunkSegmentFlagSeq.canTruncateFdIndex = -1;
    }

    /**
     * Initializes the tasks to be executed in the task chain.
     */
    private void initTasks() {
        taskChain.add(new ITask() {
            private long loopCount = 0;

            @Override
            public String name() {
                return "repl pair slave ping";
            }

            @Override
            public void run() {
                // do log every 1000s
                if (loopCount % (100 * 1000) == 0) {
                    log.info("Task {} run, slot={}, loop count={}", name(), slot, loopCount);
                }

                boolean isAsMaster = false;
                for (var replPair : replPairs) {
                    if (replPair.isSendBye()) {
                        continue;
                    }

                    if (!replPair.isAsMaster()) {
                        // only slave needs to send ping
                        replPair.ping();

                        var toFetchBigStringId = replPair.doingFetchBigStringId();
                        if (toFetchBigStringId != null) {
                            var bytes = new byte[8 + 4 + toFetchBigStringId.key().length()];
                            var buffer = ByteBuffer.wrap(bytes);
                            buffer.putLong(toFetchBigStringId.uuid());
                            buffer.putInt(toFetchBigStringId.key().length());
                            buffer.put(toFetchBigStringId.key().getBytes());
                            replPair.write(ReplType.incremental_big_string, new RawBytesContent(bytes));
                            log.info("Repl do fetch incremental big string, to server={}, uuid={}, key={}, slot={}",
                                    replPair.getHostAndPort(), toFetchBigStringId.uuid(), toFetchBigStringId.key(), slot);
                        }
                    } else {
                        isAsMaster = true;
                    }
                }

                // add update seq binlog every 100ms
                if (isAsMaster && loopCount % 10 == 0) {
                    var xUpdateSeq = new XUpdateSeq(snowFlake.getLastNextId(), System.currentTimeMillis());
                    appendBinlog(xUpdateSeq);
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
            public void setLoopCount(long loopCount) {
                this.loopCount = loopCount;
            }

            // do every 100 loop, 1s
            @Override
            public int executeOnceAfterLoopCount() {
                return 100;
            }
        });
    }

    /**
     * Adds a debug task to the task chain for logging purposes.
     */
    void debugMode() {
        taskChain.add(new ITask() {
            private long loopCount = 0;

            @Override
            public String name() {
                return "debug";
            }

            @Override
            public void run() {
                // reduce log
                var localPersist = LocalPersist.getInstance();
                var firstOneSlot = localPersist.firstOneSlot();
                if (firstOneSlot != null && slot == firstOneSlot.slot) {
                    log.info("Debug task run, slot={}, loop count={}", slot, loopCount);
                }
            }

            @Override
            public void setLoopCount(long loopCount) {
                this.loopCount = loopCount;
            }

            // do every 1000 loop, 10s
            @Override
            public int executeOnceAfterLoopCount() {
                return 1000;
            }
        });
    }

    /**
     * Checks if the current thread ID matches the protected thread ID for safe operations.
     * All operations in this slot is handle by the same thread.
     */
    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id=" + threadId + ", thread id protected for safe=" + threadIdProtectedForSafe);
        }
    }

    /**
     * Retrieves the expiration time for a key stored in OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param keyHash32   the 32-bit key hash
     * @return the expiration time, or null if the key does not exist or is expired
     */
    public Long getExpireAt(String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var v = targetWal.getV(key);
        if (v != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += v.cvEncoded().length;

            // write batch kv is the newest
            if (CompressedValue.isDeleted(v.cvEncoded())) {
                return null;
            }

            return v.expireAt();
        }

        // from lru cache
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), key.getBytes(), keyHash);
            return cv.getExpireAt();
        }

        kvLRUMissTotal++;
        return keyLoader.getExpireAt(bucketIndex, key, keyHash, keyHash32);
    }

    /**
     * Warms up the OneSlot key buckets by preloading data asynchronously.
     *
     * @return the promise representing the number of keys preloaded
     */
    public CompletableFuture<Integer> warmUp() {
        var waitF = new CompletableFuture<Integer>();
        // just run once
        new Thread(() -> {
            log.info("Start a single thread to read all key buckets from file for better cache hit, slot={}", slot);
            try {
                int n = keyLoader.warmUp();
                waitF.complete(n);
            } catch (Exception e) {
                log.error("Slot read all key buckets from file for better cache hit error for slot=" + slot, e);
                waitF.completeExceptionally(e);
            } finally {
                log.info("End slot read all key buckets from file for better cache hit, slot={}", slot);
            }
        }).start();
        return waitF;
    }

    public record BufOrCompressedValue(@Nullable ByteBuf buf, @Nullable CompressedValue cv) {
    }

    /**
     * Retrieves a key-value entry from OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param keyHash32   the 32-bit key hash
     * @return the BufOrCompressedValue record containing the key-value entry
     */
    public BufOrCompressedValue get(String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

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
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
        }

        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueXByKey(bucketIndex, key, keyHash, keyHash32);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        var expireAt = valueBytesWithExpireAtAndSeq.expireAt();
        if (expireAt != CompressedValue.NO_EXPIRE && expireAt < System.currentTimeMillis()) {
            return null;
        }

        var valueBytes = valueBytesWithExpireAtAndSeq.valueBytes();
        if (!PersistValueMeta.isPvm(valueBytes)) {
            // short value, just return, CompressedValue can decode
            lru.put(key, valueBytes);
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(valueBytes), null);
        }

        var pvm = PersistValueMeta.decode(valueBytes);
        var segmentBytes = getSegmentBytesBySegmentIndex(pvm.segmentIndex);
        if (segmentBytes == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm=" + pvm);
        }


        if (ConfForSlot.global.confChunk.isSegmentUseCompression) {
            segmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
        }
        var buf = Unpooled.wrappedBuffer(segmentBytes);
//        SegmentBatch2.iterateFromSegmentBytes(segmentBytes, new SegmentBatch2.ForDebugCvCallback());

//        // crc check
//        var segmentSeq = buf.readLong();
//        var cvCount = buf.readInt();
//        var segmentMaskedValue = buf.readInt();
//        buf.skipBytes(SEGMENT_HEADER_LENGTH);

        buf.readerIndex(pvm.segmentOffset);

        // skip key header or check key
        var keyLength = buf.readShort();
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }

        var keyBytesRead = new byte[keyLength];
        buf.readBytes(keyBytesRead);

        if (!Arrays.equals(keyBytesRead, key.getBytes())) {
            throw new IllegalStateException("Key not match, key=" + key + ", key persisted=" + new String(keyBytesRead));
        }

        // set to lru cache, just target bytes
        var cv = CompressedValue.decode(buf, key.getBytes(), keyHash);
        lru.put(key, cv.encode());

        return new BufOrCompressedValue(null, cv);
    }

    /**
     * Retrieves only the key bytes from a segment in OneSlot.
     *
     * @param pvm the metadata of the persisted value
     * @return the key bytes as a byte array
     */
    byte[] getOnlyKeyBytesFromSegment(PersistValueMeta pvm) {
        var segmentBytes = getSegmentBytesBySegmentIndex(pvm.segmentIndex);
        if (segmentBytes == null) {
            return null;
        }

        byte[] rawSegmentBytes;
        if (SegmentBatch2.isSegmentBytesTight(segmentBytes, 0)) {
            rawSegmentBytes = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, segmentBytes, pvm, chunk);
        } else {
            rawSegmentBytes = segmentBytes;
        }

        var buf = Unpooled.wrappedBuffer(rawSegmentBytes);
        buf.readerIndex(pvm.segmentOffset);

        // skip key header or check key
        var keyLength = buf.readShort();
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buf.readBytes(keyBytes);
        return keyBytes;
    }

    /**
     * Retrieves the value for a key from the write-ahead log (WAL).
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @return the value as a byte array
     */
    byte[] getFromWal(@NotNull String key, int bucketIndex) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var v = targetWal.getV(key);
        if (v == null || v.isExpired()) {
            return null;
        }
        return v.cvEncoded();
    }

    /**
     * Retrieves the bytes of a segment by its segment index.
     *
     * @param segmentIndex the segment index
     * @return the segment bytes as a byte array
     */
    private byte[] getSegmentBytesBySegmentIndex(int segmentIndex) {
        return chunk.readOneSegment(segmentIndex);
    }

    /**
     * Checks if a key exists in OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param keyHash32   the 32-bit key hash
     * @return true if the key exists and is not expired, false otherwise
     */
    public boolean exists(@NotNull String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            return !CompressedValue.isDeleted(cvEncodedFromWal);
        }

        var expireAtAndSeq = keyLoader.getExpireAtAndSeqByKey(bucketIndex, key, keyHash, keyHash32);
        return expireAtAndSeq != null && !expireAtAndSeq.isExpired();
    }

    /**
     * Removes a key from OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param keyHash32   the 32-bit key hash
     * @return true if the key was removed, false otherwise
     */
    public boolean remove(@NotNull String key, int bucketIndex, long keyHash, int keyHash32) {
        checkCurrentThreadId();

        if (exists(key, bucketIndex, keyHash, keyHash32)) {
            removeDelay(key, bucketIndex, keyHash);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Removes a key from OneSlot with a delay.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     */
    public void removeDelay(@NotNull String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var putResult = targetWal.removeDelay(key, bucketIndex, keyHash, lastPersistTimeMs);

        var xWalV = new XWalV(putResult.needPutV(), putResult.isValueShort());
        appendBinlog(xWalV);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, putResult);
        }
    }

    long threadIdProtectedForSafe = -1;

    @VisibleForTesting
    long ttlTotalInSecond = 0;
    @VisibleForTesting
    long putCountTotal = 0;

    /**
     * Gets the average TTL (Time To Live) in seconds.
     *
     * @return the average TTL in seconds
     */
    public double getAvgTtlInSecond() {
        if (putCountTotal == 0) {
            return 0;
        }

        return (double) ttlTotalInSecond / putCountTotal;
    }

    /**
     * Inserts a key-value entry into OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param cv          the compressed value
     */
    public void put(@NotNull String key, int bucketIndex, @NotNull CompressedValue cv) {
        put(key, bucketIndex, cv, false);
    }

    /**
     * Inserts a key-value entry into OneSlot.
     *
     * @param key         the key
     * @param bucketIndex the bucket index
     * @param cv          the compressed value
     * @param isFromMerge is from merge
     */
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

        var isValueShort = cv.isShortString();
        Wal.V v;
        // is use faster kv style, save short string in key buckets, not short string save key and value in record log
        if (isValueShort) {
            byte[] cvEncoded;
            if (cv.isTypeNumber()) {
                cvEncoded = cv.encodeAsNumber();
            } else if (cv.isBigString()) {
                cvEncoded = cv.encode();
            } else {
                cvEncoded = cv.encodeAsShortString();
            }

            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                    key, cvEncoded, isFromMerge);

            // for metrics update
            if (walShortValueWriteIndex < targetWal.fileToWriteIndex) {
                walShortValueWriteIndex = targetWal.fileToWriteIndex;
            }
        } else {
            var cvEncodedLength = cv.encodedLength();
            var persistLength = Wal.V.persistLength(key.length(), cvEncodedLength);
            boolean isPersistLengthOverSegmentLength = persistLength + SEGMENT_HEADER_LENGTH > chunkSegmentLength;

            // for big string, use single file
            if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
                var keyHashAsUuid = cv.getKeyHash();
                var bytes = cv.getCompressedData();
                var isWriteOk = bigStringFiles.writeBigStringBytes(keyHashAsUuid, bucketIndex, keyHashAsUuid, bytes);
                if (!isWriteOk) {
                    throw new RuntimeException("Write big string file error, uuid=" + keyHashAsUuid + ", key=" + key);
                }

                var cvAsBigString = new CompressedValue();
                cvAsBigString.setSeq(cv.getSeq());
                cvAsBigString.setKeyHash(cv.getKeyHash());
                cvAsBigString.setExpireAt(cv.getExpireAt());
                cvAsBigString.setDictSeqOrSpType(CompressedValue.SP_TYPE_BIG_STRING);
                cvAsBigString.setCompressedDataAsBigString(keyHashAsUuid, cv.getDictSeqOrSpType());

                var cvBigStringEncoded = cvAsBigString.encode();
                var xBigStrings = new XBigStrings(keyHashAsUuid, bucketIndex, cv.getKeyHash(), key, cvBigStringEncoded);
                appendBinlog(xBigStrings);

                v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), CompressedValue.SP_TYPE_BIG_STRING,
                        key, cvBigStringEncoded, isFromMerge);

                isValueShort = true;

                // for metrics update
                if (walShortValueWriteIndex < targetWal.fileToWriteIndex) {
                    walShortValueWriteIndex = targetWal.fileToWriteIndex;
                }
            } else {
                var cvEncoded = cv.encode();
                v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                        key, cvEncoded, isFromMerge);

                // for metrics update
                if (walWriteIndex < targetWal.fileToWriteIndex) {
                    walWriteIndex = targetWal.fileToWriteIndex;
                }
            }
        }

        var putResult = targetWal.put(isValueShort, key, v, lastPersistTimeMs);

        var xWalV = new XWalV(v, isValueShort);
        appendBinlog(xWalV);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, putResult);
        }
    }

    private long lastPersistTimeMs = 0L;

    /**
     * Do persist after Wal do put, need persist a batch of key values.
     *
     * @param walGroupIndex the wal group index
     * @param key           the key
     * @param putResult     Wal put result
     */
    public void doPersist(int walGroupIndex, @NotNull String key, @NotNull Wal.PutResult putResult) {
        var targetWal = walArray[walGroupIndex];
        putValueToWal(putResult.isValueShort(), targetWal);
        lastPersistTimeMs = System.currentTimeMillis();

        if (putResult.isValueShort()) {
            targetWal.clearShortValues();
        } else {
            targetWal.clearValues();
        }

        var needPutV = putResult.needPutV();
        if (needPutV != null) {
            targetWal.put(putResult.isValueShort(), key, needPutV, lastPersistTimeMs);
        }
    }

    /**
     * Reset write position after bulk load.
     */
    public void resetWritePositionAfterBulkLoad() {
        for (var wal : walArray) {
            wal.resetWritePositionAfterBulkLoad();
        }
    }

    /**
     * Flush OneSlot, clear all data, including WAL, chunk segments, key buckets, big strings, metadata.
     */
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

                walWriteIndex = 0L;
                walShortValueWriteIndex = 0L;
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

        var xFlush = new XFlush();
        appendBinlog(xFlush);
    }

    /**
     * Initialize the file descriptors.
     *
     * @throws IOException if an I/O error occurs
     */
    void initFds() throws IOException {
        this.keyLoader.initFds();

        // do every 10ms
        // for truncate file to save ssd space
        taskChain.add(metaChunkSegmentFlagSeq);
        // for delete expired big string files
        taskChain.add(keyLoader);

        // chunk
        initChunk();
    }

    private void initChunk() throws IOException {
        this.chunk = new Chunk(slot, slotDir, this);
        chunk.initFds();

        updateChunkSegmentIndexFromMeta();
    }

    /**
     * Check if there is valid data in the specified segment range.
     *
     * @param beginSegmentIndex the begin segment index
     * @param segmentCount      the segment count
     * @return true if there is valid data in the specified segment range, false otherwise
     */
    public boolean hasData(int beginSegmentIndex, int segmentCount) {
        final boolean[] hasDataArray = {false};
        metaChunkSegmentFlagSeq.iterateRange(beginSegmentIndex, segmentCount, (segmentIndex, flagByte, seq, walGroupIndex) -> {
            if (hasDataArray[0]) {
                return false;
            }

            if (!Chunk.Flag.canReuse(flagByte)) {
                hasDataArray[0] = true;
                return false;
            }

            return true;
        });
        return hasDataArray[0];
    }

    /**
     * Read chunk segments from master exists.
     *
     * @param beginSegmentIndex the begin segment index
     * @param segmentCount      the segment count
     * @return segments bytes
     */
    byte[] readForMerge(int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        if (!hasData(beginSegmentIndex, segmentCount)) {
            return null;
        }

        return chunk.readForMerge(beginSegmentIndex, segmentCount);
    }

    /**
     * Read chunk segments for replication.
     *
     * @param beginSegmentIndex the begin segment index
     * @return segments bytes
     */
    public byte[] readForRepl(int beginSegmentIndex) {
        checkCurrentThreadId();

        if (!hasData(beginSegmentIndex, ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)) {
            return null;
        }

        return chunk.readForRepl(beginSegmentIndex);
    }

    /**
     * Clean up the OneSlot, when server stops.
     */
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

    /**
     * Before persist wal, read some segment in same wal group and merge immediately.
     *
     * @param segmentIndexList                     the segment index list read
     * @param cvList                               the cv list read
     * @param updatedFlagPersistedSegmentIndexList the segment index list need to update flag to merged and persisted
     */
    @VisibleForTesting
    record BeforePersistWalExtFromMerge(@NotNull ArrayList<Integer> segmentIndexList,
                                        @NotNull ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset> cvList,
                                        @NotNull ArrayList<Integer> updatedFlagPersistedSegmentIndexList) {
        boolean isEmpty() {
            return segmentIndexList.isEmpty();
        }
    }

    /**
     * Read some segments before persist wal.
     * for performance, before persist wal, read some segment in same wal group and merge immediately
     *
     * @param walGroupIndex the wal group index
     * @return a BeforePersistWalExtFromMerge object
     */
    @VisibleForTesting
    BeforePersistWalExtFromMerge readSomeSegmentsBeforePersistWal(int walGroupIndex) {
        final int[] firstSegmentIndexWithReadSegmentCountArray = metaChunkSegmentFlagSeq.findThoseNeedToMerge(walGroupIndex);
        if (firstSegmentIndexWithReadSegmentCountArray[0] == -1) {
            return null;
        }

        var firstSegmentIndex = firstSegmentIndexWithReadSegmentCountArray[0];
        var segmentCount = firstSegmentIndexWithReadSegmentCountArray[1];
        var segmentBytesBatchRead = readForMerge(firstSegmentIndex, segmentCount);

        ArrayList<Integer> segmentIndexList = new ArrayList<>();
        ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE * 10);
        ArrayList<Integer> updatedFlagPersistedSegmentIndexList = new ArrayList<>();

        for (int i = 0; i < segmentCount; i++) {
            var segmentIndex = firstSegmentIndex + i;
            int relativeOffsetInBatchBytes = i * chunkSegmentLength;

            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted.flagByte, 0L, 0);
                updatedFlagPersistedSegmentIndexList.add(segmentIndex);

                continue;
            }

            SegmentBatch2.readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, slot);
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

    /**
     * Put value to wal, after do persist, the latest Wal value is not persisted yet, need put it back
     *
     * @param isShortValue is short value
     * @param targetWal    target wal
     */
    void putValueToWal(boolean isShortValue, @NotNull Wal targetWal) {
        var walGroupIndex = targetWal.groupIndex;
        if (isShortValue) {
            keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, targetWal.delayToKeyBucketShortValues.values());
            return;
        }

        var delayToKeyBucketShortValues = targetWal.delayToKeyBucketShortValues;
        var delayToKeyBucketValues = targetWal.delayToKeyBucketValues;

        var list = new ArrayList<>(delayToKeyBucketValues.values());
        // sort by bucket index for future merge better
        list.sort(Comparator.comparingInt(Wal.V::bucketIndex));

        var ext = readSomeSegmentsBeforePersistWal(walGroupIndex);

        KeyBucketsInOneWalGroup keyBucketsInOneWalGroup = null;
        if (ext != null) {
            extCvListCheckCountTotal++;

            var cvList = ext.cvList;
            var cvListCount = cvList.size();

            int validCount = 0;
            // remove those wal exist
            cvList.removeIf(one -> delayToKeyBucketShortValues.containsKey(one.key()) || delayToKeyBucketValues.containsKey(one.key()));
            if (!cvList.isEmpty()) {
                // remove those expired or updated
                keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, keyLoader);

                for (var one : cvList) {
                    var cv = one.cv();
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash());
                    var extWalGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
                    if (extWalGroupIndex != walGroupIndex) {
                        throw new IllegalStateException("Wal group index not match, s=" + slot + ", wal group index=" + walGroupIndex + ", ext wal group index=" + extWalGroupIndex);
                    }

                    var expireAtAndSeq = keyBucketsInOneWalGroup.getExpireAtAndSeq(bucketIndex, one.key(), cv.getKeyHash());
                    var isThisKeyExpired = expireAtAndSeq == null || expireAtAndSeq.isExpired();
                    if (isThisKeyExpired) {
                        continue;
                    }

                    var isThisKeyUpdated = expireAtAndSeq.seq() != cv.getSeq();
                    if (isThisKeyUpdated) {
                        continue;
                    }

                    list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                            one.key(), cv.encode(), true));
                    validCount++;
                }
            }
            int invalidCount = cvListCount - validCount;
            extCvValidCountTotal += validCount;
            extCvInvalidCountTotal += invalidCount;
        }

        if (list.size() > 1000 * 4) {
            log.warn("Ready to persist wal with merged valid cv list, too large, s={}, wal group index={}, list size={}",
                    slot, walGroupIndex, list.size());
        }

        chunk.persist(walGroupIndex, list, keyBucketsInOneWalGroup);

        if (ext != null && !ext.isEmpty()) {
            var segmentIndexList = ext.segmentIndexList;
            // continuous segment index
            assert (segmentIndexList.getLast() - segmentIndexList.getFirst() == segmentIndexList.size() - 1);

            setSegmentMergeFlagBatch(segmentIndexList.getFirst(), segmentIndexList.size(),
                    Flag.merged_and_persisted.flagByte, null, walGroupIndex);
        }
    }

    /**
     * check segment index is valid
     *
     * @param segmentIndex the target segment index
     */
    private void checkSegmentIndex(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }
    }

    /**
     * check begin segment index is valid
     *
     * @param beginSegmentIndex the begin segment index
     * @param segmentCount      the segment count
     */
    private void checkBeginSegmentIndex(int beginSegmentIndex, int segmentCount) {
        if (beginSegmentIndex < 0 || beginSegmentIndex + segmentCount - 1 > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Begin segment index out of bound, s=" + slot + ", i=" + beginSegmentIndex + ", c=" + segmentCount);
        }
    }

    /**
     * Gets the merge flag for a segment.
     *
     * @param segmentIndex the segment index
     * @return the segment flag with segment sequence number and wal group index
     */
    @TestOnly
    SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        checkSegmentIndex(segmentIndex);
        return metaChunkSegmentFlagSeq.getSegmentMergeFlag(segmentIndex);
    }

    /**
     * Update one segment merge flag.
     *
     * @param segmentIndex the segment index
     * @param flagByte     the flag byte
     * @param segmentSeq   the segment sequence number
     */
    @TestOnly
    public void updateSegmentMergeFlag(int segmentIndex, byte flagByte, long segmentSeq) {
        var segmentFlag = getSegmentMergeFlag(segmentIndex);
        setSegmentMergeFlag(segmentIndex, flagByte, segmentSeq, segmentFlag.walGroupIndex());
    }

    /**
     * Set segment merge flag.
     *
     * @param segmentIndex  the segment index
     * @param flagByte      the flag byte
     * @param segmentSeq    the segment sequence number
     * @param walGroupIndex the wal group index
     */
    public void setSegmentMergeFlag(int segmentIndex, byte flagByte, long segmentSeq, int walGroupIndex) {
        checkSegmentIndex(segmentIndex);
        metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, flagByte, segmentSeq, walGroupIndex);
    }

    /**
     * Set segment merge flag batch.
     *
     * @param beginSegmentIndex the begin segment index
     * @param segmentCount      the segment count
     * @param flagByte          the flag byte
     * @param segmentSeqList    the segment sequence list
     * @param walGroupIndex     the wal group index
     */
    public void setSegmentMergeFlagBatch(int beginSegmentIndex,
                                         int segmentCount,
                                         byte flagByte,
                                         @Nullable List<Long> segmentSeqList,
                                         int walGroupIndex) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(beginSegmentIndex, segmentCount,
                flagByte, segmentSeqList, walGroupIndex);
    }

    /**
     * Reset as master. Do this when master reset.
     *
     * @throws IOException if any IO exception occurs
     */
    @MasterReset
    public void resetAsMaster() throws IOException {
        log.warn("Repl reset as master, slot={}", slot);

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

    /**
     * Reset as slave. Do this when master reset.
     *
     * @param host master host
     * @param port master port
     * @throws IOException if any IO exception occurs
     */
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

    private final static OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    private final DictMap dictMap = DictMap.getInstance();

    /**
     * Global gauge for collecting global metrics.
     * Metrics collected here are shared across all slots.
     */
    @VisibleForTesting
    final static SimpleGauge globalGauge = new SimpleGauge("global", "Global metrics.");

    static {
        globalGauge.register();
    }

    /**
     * Clear metrics collect for global metrics, when first slot changed
     */
    void clearGlobalMetricsCollect() {
        globalGauge.clearRawGetterList();
    }

    /**
     * Initializes global metrics collection by registering a raw getter that provides
     * various global metrics such as uptime, dictionary size, configuration parameters,
     * memory preparation statistics, and more.
     */
    void addGlobalMetricsCollect() {
        log.warn("Init global metrics collect, slot={}", slot);
        globalGauge.clearRawGetterList();
        globalGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            // only the first slot shows global metrics
            var localPersist = LocalPersist.getInstance();
            var firstOneSlot = localPersist.firstOneSlot();
            if (firstOneSlot == null || slot != firstOneSlot.slot) {
                return map;
            }

            // global use slot -1
            var labelValues = List.of("-1");

            // only show global
            map.put("global_up_time", new SimpleGauge.ValueWithLabelValues((double) MultiWorkerServer.UP_TIME, labelValues));
            map.put("global_dict_size", new SimpleGauge.ValueWithLabelValues((double) dictMap.dictSize(), labelValues));
            // global config for one slot
            map.put("global_estimate_key_number", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.estimateKeyNumber, labelValues));
            map.put("global_slot_number", new SimpleGauge.ValueWithLabelValues((double) slotNumber, labelValues));
            map.put("global_net_workers", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.netWorkers, labelValues));
            map.put("global_slot_workers", new SimpleGauge.ValueWithLabelValues((double) ConfForGlobal.slotWorkers, labelValues));

            map.put("global_key_buckets_per_slot", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confBucket.bucketsPerSlot, labelValues));
            map.put("global_key_initial_split_number", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confBucket.initialSplitNumber, labelValues));
            map.put("global_key_bucket_once_scan_max_loop_count", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confBucket.onceScanMaxLoopCount, labelValues));

            map.put("global_chunk_segment_number_per_fd", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.segmentNumberPerFd, labelValues));
            map.put("global_chunk_fd_per_chunk", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.fdPerChunk, labelValues));
            map.put("global_chunk_segment_length", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confChunk.segmentLength, labelValues));

            map.put("global_wal_one_charge_bucket_number", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confWal.oneChargeBucketNumber, labelValues));
            map.put("global_key_wal_once_scan_max_loop_count", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.confWal.onceScanMaxLoopCount, labelValues));

            // connected clients
            var socketInspector = MultiWorkerServer.STATIC_GLOBAL_V.socketInspector;
            map.put("global_connected_client_count", new SimpleGauge.ValueWithLabelValues((double) socketInspector.connectedClientCount(), labelValues));
            map.put("global_subscribe_client_count", new SimpleGauge.ValueWithLabelValues((double) socketInspector.subscribeClientCount(), labelValues));
            map.put("global_blocking_client_count", new SimpleGauge.ValueWithLabelValues((double) BlockingList.blockingClientCount(), labelValues));

            // net in / out bytes
            map.put("global_net_in_bytes", new SimpleGauge.ValueWithLabelValues((double) socketInspector.netInBytesLength(), labelValues));
            map.put("global_net_out_bytes", new SimpleGauge.ValueWithLabelValues((double) socketInspector.netOutBytesLength(), labelValues));

            // lru prepare bytes
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

            map.put("global_system_load_average", new SimpleGauge.ValueWithLabelValues(osBean.getSystemLoadAverage(), labelValues));

            return map;
        });
    }

    // change dyn config or global config, todo
    private static final int BIG_KEY_LENGTH_CHECK = 2048;

    /**
     * Collects and returns slot-specific metrics in a map.
     * These metrics include slot-specific statistics like sequence IDs, WAL key counts,
     * key loader metrics, chunk metrics, binary log offsets, big key statistics, and LRU hit/miss ratios.
     *
     * @return a map of metric names to their respective values
     */
    @Override
    public Map<String, Double> collect() {
        var map = new TreeMap<String, Double>();

        if (snowFlake != null) {
            map.put("slot_last_seq", (double) snowFlake.getLastNextId());
        }

        if (walArray != null && walArray.length > 0) {
            map.put("wal_key_count", (double) getWalKeyCount());
            map.put("wal_disk_usage", (double) (walWriteIndex + walShortValueWriteIndex));

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
            map.put("big_string_files_disk_usage", (double) bigStringFiles.diskUsage);

            map.put("big_string_files_read_file_count", (double) bigStringFiles.readFileCountTotal);
            map.put("big_string_files_read_file_cost_us_avg", (double) bigStringFiles.readFileCostTotalUs / bigStringFiles.readFileCountTotal);
            map.put("big_string_files_read_byte_length_avg", (double) bigStringFiles.readByteLengthTotal / bigStringFiles.readFileCountTotal);

            map.put("big_string_files_write_file_count", (double) bigStringFiles.writeFileCountTotal);
            map.put("big_string_files_write_file_cost_us_avg", (double) bigStringFiles.writeFileCostTotalUs / bigStringFiles.writeFileCountTotal);
            map.put("big_string_files_write_byte_length_avg", (double) bigStringFiles.writeByteLengthTotal / bigStringFiles.writeFileCountTotal);

            map.put("big_string_files_delete_file_count", (double) bigStringFiles.deleteFileCountTotal);
            map.put("big_string_files_delete_file_cost_us_avg", (double) bigStringFiles.deleteFileCostTotalUs / bigStringFiles.deleteFileCountTotal);
            map.put("big_string_files_delete_byte_length_avg", (double) bigStringFiles.deleteByteLengthTotal / bigStringFiles.deleteFileCountTotal);

            map.put("big_string_files_delay_to_delete_count", (double) delayToDeleteBigStringFileIds.size());
        }

        if (binlog != null) {
            map.put("binlog_current_offset_from_the_beginning", (double) binlog.currentReplOffset());
            map.put("binlog_disk_usage", (double) binlog.getDiskUsage());
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
            map.put("repl_as_slave_catch_up_last_time_millis_in_master", (double) replPairAsSlave.getSlaveCatchUpLastTimeMillisInMaster());
            map.put("repl_as_slave_catch_up_last_time_millis_diff", (double) replPairAsSlave.getSlaveCatchUpLastTimeMillisDiff());
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
