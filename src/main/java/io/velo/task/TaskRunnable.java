package io.velo.task;

import io.activej.eventloop.Eventloop;
import io.velo.RequestHandler;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Runnable task for slot workers that processes slots in a loop.
 */
public class TaskRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TaskRunnable.class);

    /** The slot worker ID. */
    private final byte slotWorkerId;

    /** The total number of slot workers. */
    private final byte slotWorkers;

    /**
     * @param slotWorkerId the slot worker ID
     * @param slotWorkers  the total number of slot workers
     */
    public TaskRunnable(byte slotWorkerId, byte slotWorkers) {
        this.slotWorkerId = slotWorkerId;
        this.slotWorkers = slotWorkers;
    }

    /**
     * Slots assigned to this worker. CopyOnWriteArrayList because lazy registration
     * ({@link #addOneSlotIfCharged}) can run on a different worker thread than the one that owns
     * this TaskRunnable, while {@link #run()} iterates this list every tick. Mutations are rare
     * (lazy slot init) and iteration is frequent, so snapshot iteration is the simplest safe model.
     */
    @VisibleForTesting
    final CopyOnWriteArrayList<OneSlot> oneSlots = new CopyOnWriteArrayList<>();

    /**
     * Assigns slots to this worker based on worker ID. Skips null (uninitialized) entries so a
     * sparse {@code LocalPersist.oneSlots()} array in cluster mode only charges owned slots.
     *
     * @param oneSlots all slots to consider
     */
    public void chargeOneSlots(OneSlot[] oneSlots) {
        for (var oneSlot : oneSlots) {
            if (oneSlot == null) {
                continue;
            }
            if (oneSlot.slot() % slotWorkers == slotWorkerId) {
                this.oneSlots.add(oneSlot);

                oneSlot.setSlotWorkerEventloop(slotWorkerEventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
    }

    /**
     * Register a lazily created slot into this worker's task list if it belongs to this worker.
     * Idempotent: skips slots already present or not owned by this worker.
     *
     * @param oneSlot the slot to register
     */
    public void addOneSlotIfCharged(OneSlot oneSlot) {
        if (oneSlot.slot() % slotWorkers != slotWorkerId || oneSlots.contains(oneSlot)) {
            return;
        }
        oneSlots.add(oneSlot);
        oneSlot.setSlotWorkerEventloop(slotWorkerEventloop);
        oneSlot.setRequestHandler(requestHandler);
    }

    /**
     * Stop driving {@code doTask} for a slot being released (shed at migration cutover). Idempotent.
     *
     * @param oneSlot the slot to unregister
     */
    public void removeOneSlot(OneSlot oneSlot) {
        oneSlots.remove(oneSlot);
    }

    /** The Eventloop for this slot worker. */
    private Eventloop slotWorkerEventloop;

    /** @param slotWorkerEventloop the Eventloop for this worker */
    public void setSlotWorkerEventloop(Eventloop slotWorkerEventloop) {
        this.slotWorkerEventloop = slotWorkerEventloop;
    }

    /** The RequestHandler for this slot worker. */
    private RequestHandler requestHandler;

    /** @param requestHandler the RequestHandler for this worker */
    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    /** The number of loops completed. */
    private long loopCount = 0;

    /**
     * Runs tasks for all assigned slots, reschedules every 10ms.
     * Delays 1s on first run if server not started.
     */
    @Override
    public void run() {
        if (isStopped) {
            return;
        }

        final long INTERVAL_MS = 10L;
        if (!isStartDone) {
            slotWorkerEventloop.delay(INTERVAL_MS * 100, this);
            return;
        }

        for (var oneSlot : oneSlots) {
            try {
                oneSlot.doTask(loopCount);
            } catch (Exception e) {
                log.error("Slot task error, slot worker id={}, loop count={}", slotWorkerId, loopCount, e);
            }
        }
        loopCount++;

        slotWorkerEventloop.delay(INTERVAL_MS, this);
    }

    private volatile boolean isStopped = false;
    private volatile boolean isStartDone = false;

    /** @param isStartDone true if startup is complete */
    public void startDone(boolean isStartDone) {
        this.isStartDone = isStartDone;
    }

    /** Stops the task and prints a message. */
    public void stop() {
        isStopped = true;
        System.out.println(
                "Task delay stopped for slot worker eventloop, slot worker id=" + slotWorkerId);
    }
}