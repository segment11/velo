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
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
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

public class LocalPersist implements NeedCleanUp {
    public static final int PAGE_SIZE = (int) PageManager.getInstance().pageSize();
    public static final int PROTECTION = PageManager.PROT_READ | PageManager.PROT_WRITE | PageManager.PROT_EXEC;
    public static final short DEFAULT_SLOT_NUMBER = 4;

    // 16384, todo
    // 1024 slots, one slot max 100 million keys, 100 million * 1024 = 102.4 billion keys
    // one slot cost 50-100GB, all cost will be 50-100TB
    public static final short MAX_SLOT_NUMBER = 1024;

    // singleton
    private static final LocalPersist instance = new LocalPersist();

    public static LocalPersist getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(LocalPersist.class);

    private LocalPersist() {
        System.setProperty("jnr.ffi.asm.enabled", "false");
        libC = LibraryLoader.create(LibC.class).load("c");
    }

    private final LibC libC;

    public void persistMergedSegmentsJobUndone() {
        for (var oneSlot : oneSlots) {
            oneSlot.asyncRun(() -> {
                oneSlot.persistMergingOrMergedSegmentsButNotPersisted();
                // merged segments may do merge again, it is ok, only do once when server restart
                oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(true);
                oneSlot.getMergedSegmentIndexEndLastTime();
            });
        }
    }

    private OneSlot[] oneSlots;

    public OneSlot[] oneSlots() {
        return oneSlots;
    }

    public OneSlot oneSlot(short slot) {
        return oneSlots[slot];
    }

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

    @TestOnly
    public void addOneSlotForTest2(short slot) {
        var oneSlot = new OneSlot(slot);
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
    }

    private File persistDir;
    private Config persistConfig;

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
            oneSlot.initFds(libC);

            oneSlots[slot] = oneSlot;
        }

        this.multiShard = new MultiShard(persistDir);
        initSlotsAgainAfterMultiShardLoadedOrChanged();
    }

    public void initSlotsAgainAfterMultiShardLoadedOrChanged() throws IOException {
        var firstOneSlot = firstOneSlot();

        for (var oneSlot : oneSlots) {
            oneSlot.initMetricsCollect();

            // set dict map binlog same as the first slot binlog
            if (firstOneSlot != null && oneSlot.slot() == firstOneSlot.slot()) {
                DictMap.getInstance().setBinlog(oneSlot.getBinlog());
                log.warn("Set dict map binlog to slot={}", oneSlot.slot());

                if (!ConfForGlobal.initDynConfigItems.isEmpty()) {
                    for (var entry : ConfForGlobal.initDynConfigItems.entrySet()) {
                        oneSlot.getDynConfig().update(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

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

    private boolean isHashSaveMemberTogether;

    public boolean getIsHashSaveMemberTogether() {
        return isHashSaveMemberTogether;
    }

    @TestOnly
    public void setHashSaveMemberTogether(boolean hashSaveMemberTogether) {
        isHashSaveMemberTogether = hashSaveMemberTogether;
    }

    private boolean isDebugMode = false;

    public boolean isDebugMode() {
        return isDebugMode;
    }

    public void debugMode() {
        isDebugMode = true;
        for (var oneSlot : oneSlots) {
            oneSlot.debugMode();
        }
    }

    public void fixSlotThreadId(short slot, long threadId) {
        oneSlots[slot].threadIdProtectedForSafe = threadId;
        log.warn("Fix slot thread id, s={}, tid={}", slot, threadId);
    }

    public OneSlot currentThreadFirstOneSlot() {
        for (var oneSlot : oneSlots) {
            if (oneSlot.threadIdProtectedForSafe == Thread.currentThread().threadId()) {
                return oneSlot;
            }
        }
        throw new IllegalStateException("No one slot for current thread");
    }

    public @Nullable OneSlot firstOneSlot() {
        if (!ConfForGlobal.clusterEnabled) {
            return oneSlots[0];
        }

        var firstToClientSlot = multiShard == null ? null : multiShard.firstToClientSlot();
        if (firstToClientSlot == null) {
            return null;
        }

        var slot = MultiShard.asInnerSlotByToClientSlot(firstToClientSlot);
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() == slot) {
                return oneSlot;
            }
        }
        return null;
    }

    private IndexHandlerPool indexHandlerPool;

    public IndexHandlerPool getIndexHandlerPool() {
        return indexHandlerPool;
    }

    public void startIndexHandlerPool() throws IOException {
        this.indexHandlerPool = new IndexHandlerPool(ConfForGlobal.indexWorkers, persistDir, persistConfig);
        this.indexHandlerPool.start();
    }

    private volatile boolean isAsSlaveFirstSlotFetchedExistsAllDone = false;

    public boolean isAsSlaveFirstSlotFetchedExistsAllDone() {
        return isAsSlaveFirstSlotFetchedExistsAllDone;
    }

    public void setAsSlaveFirstSlotFetchedExistsAllDone(boolean asSlaveFirstSlotFetchedExistsAllDone) {
        isAsSlaveFirstSlotFetchedExistsAllDone = asSlaveFirstSlotFetchedExistsAllDone;
    }

    private MultiShard multiShard;

    public MultiShard getMultiShard() {
        return multiShard;
    }

    private SocketInspector socketInspector;

    public SocketInspector getSocketInspector() {
        return socketInspector;
    }

    public void setSocketInspector(@Nullable SocketInspector socketInspector) {
        this.socketInspector = socketInspector;
    }

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
