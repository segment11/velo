package io.velo.persist;

import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.velo.ConfVolumeDirsForSlot;
import io.velo.NeedCleanUp;
import io.velo.SnowFlake;
import io.velo.SocketInspector;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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

    public static final int O_DIRECT = 040000;

    public void persistMergedSegmentsJobUndone() {
        for (var oneSlot : oneSlots) {
            oneSlot.persistMergingOrMergedSegmentsButNotPersisted();
            // merged segments may do merge again, it is ok, only do once when server restart
            oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(true);
            oneSlot.getMergedSegmentIndexEndLastTime();
        }
    }

    private OneSlot[] oneSlots;

    public OneSlot[] oneSlots() {
        return oneSlots;
    }

    public OneSlot oneSlot(short slot) {
        return oneSlots[slot];
    }

    @TestOnly
    public void addOneSlot(short slot, Eventloop eventloop) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        var oneSlot = new OneSlot(slot, eventloop);
        oneSlot.threadIdProtectedForSafe = eventloop.getEventloopThread().threadId();
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
    }

    public void addOneSlotForTest2(short slot) {
        var oneSlot = new OneSlot(slot);
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
    }

    public void initSlots(byte netWorkers, short slotNumber, SnowFlake[] snowFlakes, File persistDir, Config persistConfig) throws IOException {
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber);

        isHashSaveMemberTogether = persistConfig.get(ofBoolean(), "isHashSaveMemberTogether", true);

        this.oneSlots = new OneSlot[slotNumber];
        for (short slot = 0; slot < slotNumber; slot++) {
            var i = slot % netWorkers;
            var oneSlot = new OneSlot(slot, slotNumber, snowFlakes[i], persistDir, persistConfig);
            oneSlot.initFds(libC);

            oneSlots[slot] = oneSlot;
        }
    }

    private boolean isHashSaveMemberTogether;

    public boolean getIsHashSaveMemberTogether() {
        return isHashSaveMemberTogether;
    }

    @TestOnly
    public void setHashSaveMemberTogether(boolean hashSaveMemberTogether) {
        isHashSaveMemberTogether = hashSaveMemberTogether;
    }

    public void debugMode() {
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

    private SocketInspector socketInspector;

    public SocketInspector getSocketInspector() {
        return socketInspector;
    }

    public void setSocketInspector(SocketInspector socketInspector) {
        this.socketInspector = socketInspector;
    }

    @Override
    public void cleanUp() {
        for (var oneSlot : oneSlots) {
            oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId();
            oneSlot.cleanUp();
        }
    }
}
