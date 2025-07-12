package io.velo.persist;

import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.*;
import io.velo.persist.index.IndexHandlerPool;
import io.velo.repl.cluster.MultiShard;
import io.velo.reply.AsyncReply;
import io.velo.reply.Reply;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.activej.config.converter.ConfigConverters.ofBoolean;

/**
 * LocalPersist manages the local storage and operations for the persistence layer.
 * It uses multiple slots to distribute the data and handles various operations like
 * initializing slots, reading from WAL (Write-Ahead Log), and cleaning up resources.
 */
public class LocalPersist implements NeedCleanUp {
    /**
     * The size of a page in bytes, defined by the system's page manager.
     */
    public static final int PAGE_SIZE = (int) PageManager.getInstance().pageSize();

    /**
     * The protection flags for memory pages, allowing read, write, and execution.
     */
    public static final int PROTECTION = PageManager.PROT_READ | PageManager.PROT_WRITE | PageManager.PROT_EXEC;

    /**
     * The default number of slots used for data distribution.
     */
    public static final short DEFAULT_SLOT_NUMBER = 4;

    // 1024 slots, one slot max 100 million keys, 100 million * 1024 = 102.4 billion keys
    // one slot cost 50-100GB, all cost will be 50-100TB
    /**
     * The maximum number of slots allowed.
     * This is set to 1024, allowing for a large number of keys across slots.
     */
    public static final short MAX_SLOT_NUMBER = 1024;

    /**
     * Singleton instance of LocalPersist.
     * Ensures that only one instance is created and used throughout the application.
     */
    private static final LocalPersist instance = new LocalPersist();

    /**
     * Retrieves the singleton instance of LocalPersist.
     *
     * @return the singleton instance
     */
    public static LocalPersist getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(LocalPersist.class);

    /**
     * Private constructor to prevent instantiation from outside the class.
     * Initializes the LibC library loader for C library.
     */
    private LocalPersist() {
    }

    private OneSlot[] oneSlots;

    /**
     * Retrieves all the slots.
     *
     * @return an array of OneSlot instances
     */
    public OneSlot[] oneSlots() {
        return oneSlots;
    }

    /**
     * Retrieves a specific slot by its index.
     *
     * @param slot the index of the slot to retrieve
     * @return the OneSlot instance at the specified index
     */
    public OneSlot oneSlot(short slot) {
        return oneSlots[slot];
    }

    /**
     * Performs an operation in all slots asynchronously.
     *
     * @param fnApplyOneSlot the function to apply to each slot
     * @param fn             a function to aggregate the results from all slots
     * @param <R>            the type of result from fnApplyOneSlot
     * @return an AsyncReply containing the aggregated results
     */
    public <R> AsyncReply doSthInSlots(@NotNull Function<OneSlot, R> fnApplyOneSlot, @NotNull Function<ArrayList<R>, Reply> fn) {
        Promise<R>[] promises = new Promise[oneSlots.length];
        for (int i = 0; i < oneSlots.length; i++) {
            var oneSlot = oneSlots[i];
            promises[i] = oneSlot.asyncCall(() -> fnApplyOneSlot.apply(oneSlot));
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("async all error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            ArrayList<R> resultList = new ArrayList<>();
            for (var p : promises) {
                resultList.add(p.getResult());
            }

            finalPromise.set(fn.apply(resultList));
        });

        return asyncReply;
    }

    /**
     * Adds a new slot for testing purposes.
     * This method is intended for test environments only.
     *
     * @param slot      the index of the slot to add
     * @param eventloop the Eventloop instance associated with the new slot
     */
    @TestOnly
    public void addOneSlot(short slot, @NotNull Eventloop eventloop) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var oneSlot = new OneSlot(slot, eventloop);
        assert eventloop.getEventloopThread() != null;
        oneSlot.threadIdProtectedForSafe = eventloop.getEventloopThread().threadId();
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
    }

    /**
     * Adds a new slot for testing purposes without an Eventloop.
     * This method is intended for test environments only.
     *
     * @param slot the index of the slot to add
     */
    @TestOnly
    public void addOneSlotForTest2(short slot) {
        var oneSlot = new OneSlot(slot);
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
    }

    private File persistDir;
    private Config persistConfig;

    /**
     * Initializes the slots with the given parameters.
     *
     * @param netWorkers    the number of network workers
     * @param slotNumber    the number of slots to initialize
     * @param snowFlakes    array of SnowFlake instances for generating unique identifiers
     * @param persistDir    the directory for persistence storage
     * @param persistConfig the configuration for persistence
     * @throws IOException if an I/O error occurs during initialization
     */
    public void initSlots(byte netWorkers, short slotNumber,
                          @NotNull SnowFlake[] snowFlakes,
                          @NotNull File persistDir,
                          @NotNull Config persistConfig) throws IOException {
        this.persistDir = persistDir;
        this.persistConfig = persistConfig;
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber);

        isHashSaveMemberTogether = persistConfig.get(ofBoolean(), "isHashSaveMemberTogether", true);

        this.oneSlots = new OneSlot[slotNumber];
        for (short slot = 0; slot < slotNumber; slot++) {
            var i = slot % netWorkers;
            var oneSlot = new OneSlot(slot, slotNumber, snowFlakes[i], persistDir, persistConfig);
            oneSlot.initFds();

            oneSlots[slot] = oneSlot;
        }

        this.multiShard = new MultiShard(persistDir);
        this.initSlotsAgainAfterMultiShardLoadedOrChanged();
    }

    /**
     * Reinitialized slots after MultiShard is loaded or changed.
     *
     * @throws IOException if an I/O error occurs during reinitialization
     */
    public void initSlotsAgainAfterMultiShardLoadedOrChanged() throws IOException {
        for (var oneSlot : oneSlots) {
            oneSlot.clearGlobalMetricsCollect();
        }

        var firstOneSlot = firstOneSlot();
        if (firstOneSlot != null) {
            firstOneSlot.addGlobalMetricsCollect();

            if (!ConfForGlobal.initDynConfigItems.isEmpty()) {
                for (var entry : ConfForGlobal.initDynConfigItems.entrySet()) {
                    firstOneSlot.getDynConfig().update(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Performs WAL read asynchronously for each slot.
     *
     * @return true if all WAL reads were successful, false otherwise
     */
    public boolean walLazyReadFromFile() {
        var beginT = System.currentTimeMillis();
        CompletableFuture<Boolean>[] fArray = new CompletableFuture[oneSlots.length];
        for (int i = 0; i < oneSlots.length; i++) {
            var oneSlot = oneSlots[i];
            fArray[i] = oneSlot.walLazyReadFromFile();
        }

        log.info("Wait for all slots wal read from file done");
        CompletableFuture.allOf(fArray).join();
        var endT = System.currentTimeMillis();
        log.info("All slots wal read from file done, cost={}ms", endT - beginT);

        var isEveryOk = true;
        for (var f : fArray) {
            if (f.isCompletedExceptionally()) {
                isEveryOk = false;
                break;
            }
        }
        return isEveryOk;
    }

    /**
     * Perform warm up asynchronously for each slot.
     *
     * @return true if warm up was successful for all slots, false otherwise
     */
    public boolean warmUp() {
        var beginT = System.currentTimeMillis();
        CompletableFuture<Integer>[] fArray = new CompletableFuture[oneSlots.length];
        for (int i = 0; i < oneSlots.length; i++) {
            var oneSlot = oneSlots[i];
            fArray[i] = oneSlot.warmUp();
        }

        log.info("Wait for all slots key buckets read from file done");
        CompletableFuture.allOf(fArray).join();
        var endT = System.currentTimeMillis();
        log.info("All slots key buckets read from file done, cost={}ms", endT - beginT);

        var isEveryOk = true;
        for (var f : fArray) {
            if (f.isCompletedExceptionally()) {
                isEveryOk = false;
                break;
            }
        }
        return isEveryOk;
    }

    private boolean isHashSaveMemberTogether;

    /**
     * Checks if hash saving is set to save members together.
     *
     * @return true if hash saving is set to save members together, false otherwise
     */
    public boolean getIsHashSaveMemberTogether() {
        return isHashSaveMemberTogether;
    }

    /**
     * Sets the flag for saving members together in hashes.
     * This method is intended for test environments only.
     *
     * @param hashSaveMemberTogether the flag value to set
     */
    @TestOnly
    public void setHashSaveMemberTogether(boolean hashSaveMemberTogether) {
        isHashSaveMemberTogether = hashSaveMemberTogether;
    }

    private boolean isDebugMode = false;

    /**
     * Checks if the system is in debug mode.
     *
     * @return true if in debug mode, false otherwise
     */
    public boolean isDebugMode() {
        return isDebugMode;
    }

    /**
     * Activates debug mode for the system.
     * Also activates debug mode for each slot.
     */
    public void debugMode() {
        isDebugMode = true;
        for (var oneSlot : oneSlots) {
            oneSlot.debugMode();
        }
    }

    /**
     * Fixes the thread ID for a specific slot.
     *
     * @param slot     the index of the slot
     * @param threadId the new thread ID to set
     */
    public void fixSlotThreadId(short slot, long threadId) {
        oneSlots[slot].threadIdProtectedForSafe = threadId;
        log.warn("Fix slot thread id, s={}, tid={}", slot, threadId);
    }

    /**
     * Retrieves the first slot associated with the current thread.
     *
     * @return the first OneSlot associated with the current thread
     * @throws IllegalStateException if no slot is associated with the current thread
     */
    public OneSlot currentThreadFirstOneSlot() {
        for (var oneSlot : oneSlots) {
            if (oneSlot.threadIdProtectedForSafe == Thread.currentThread().threadId()) {
                return oneSlot;
            }
        }
        throw new IllegalStateException("No one slot for current thread");
    }

    /**
     * Retrieves the first slot to handle client requests.
     *
     * @return the first OneSlot to handle client requests, or null if not found
     */
    public @Nullable OneSlot firstOneSlot() {
        // for unit test
        if (oneSlots == null) {
            return null;
        }

        if (!ConfForGlobal.clusterEnabled) {
            return oneSlots[0];
        }

        var toClientSlot = multiShard == null ? null : multiShard.firstToClientSlot();
        return getInnerOneSlotByToClientSlot(toClientSlot);
    }

    public @Nullable OneSlot lastOneSlot() {
        if (!ConfForGlobal.clusterEnabled) {
            return oneSlots[oneSlots.length - 1];
        }

        var toClientSlot = multiShard == null ? null : multiShard.lastToClientSlot();
        return getInnerOneSlotByToClientSlot(toClientSlot);
    }

    public @Nullable OneSlot nextOneSlot(short slot) {
        if (!ConfForGlobal.clusterEnabled) {
            return oneSlots[slot + 1];
        }

        var eachInnerSlotChargeToClientSlotNumber = MultiShard.TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber;
        var toClientSlotNext = multiShard.nextToClientSlot(slot * eachInnerSlotChargeToClientSlotNumber);
        if (toClientSlotNext == null) {
            return null;
        }
        return getInnerOneSlotByToClientSlot(toClientSlotNext);
    }

    @Nullable
    private OneSlot getInnerOneSlotByToClientSlot(Integer toClientSlot) {
        if (toClientSlot == null) {
            return null;
        }

        var slot = MultiShard.asInnerSlotByToClientSlot(toClientSlot);
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() == slot) {
                return oneSlot;
            }
        }
        return null;
    }

    private IndexHandlerPool indexHandlerPool;

    /**
     * Retrieves the IndexHandlerPool instance.
     *
     * @return the IndexHandlerPool instance
     */
    public IndexHandlerPool getIndexHandlerPool() {
        return indexHandlerPool;
    }

    /**
     * Starts the IndexHandlerPool.
     *
     * @throws IOException if an I/O error occurs during startup
     */
    public void startIndexHandlerPool() throws IOException {
        this.indexHandlerPool = new IndexHandlerPool(ConfForGlobal.indexWorkers, persistDir, persistConfig);
        this.indexHandlerPool.start();
    }

    private volatile boolean isAsSlaveFirstSlotFetchedExistsAllDone = false;

    /**
     * Checks if the first slot fetch for a slave is complete.
     *
     * @return true if the fetch is complete, false otherwise
     */
    public boolean isAsSlaveFirstSlotFetchedExistsAllDone() {
        return isAsSlaveFirstSlotFetchedExistsAllDone;
    }

    /**
     * Sets the flag indicating whether the first slot fetch for a slave is complete.
     *
     * @param asSlaveFirstSlotFetchedExistsAllDone the flag value to set
     */
    public void setAsSlaveFirstSlotFetchedExistsAllDone(boolean asSlaveFirstSlotFetchedExistsAllDone) {
        isAsSlaveFirstSlotFetchedExistsAllDone = asSlaveFirstSlotFetchedExistsAllDone;
    }

    private MultiShard multiShard;

    /**
     * Retrieves the MultiShard instance.
     *
     * @return the MultiShard instance
     */
    public MultiShard getMultiShard() {
        return multiShard;
    }

    private SocketInspector socketInspector;

    /**
     * Retrieves the SocketInspector instance.
     *
     * @return the SocketInspector instance
     */
    public SocketInspector getSocketInspector() {
        return socketInspector;
    }

    /**
     * Sets the SocketInspector instance.
     *
     * @param socketInspector the SocketInspector instance to set
     */
    public void setSocketInspector(@Nullable SocketInspector socketInspector) {
        this.socketInspector = socketInspector;
    }

    /**
     * Cleans up resources used by LocalPersist.
     * Cleans up each slot and the IndexHandlerPool.
     */
    @Override
    public void cleanUp() {
        for (var oneSlot : oneSlots) {
            oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId();
            oneSlot.cleanUp();
        }

        if (indexHandlerPool != null) {
            indexHandlerPool.cleanUp();
        }
    }
}