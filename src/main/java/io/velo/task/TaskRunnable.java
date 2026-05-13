package io.velo.task;

import io.activej.eventloop.Eventloop;
import io.velo.RequestHandler;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * Runnable task for slot workers that processes slots in a loop.
 */
public class TaskRunnable implements Runnable {
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

    /** The slots assigned to this worker. */
    @VisibleForTesting
    final ArrayList<OneSlot> oneSlots = new ArrayList<>();

    /**
     * Assigns slots to this worker based on worker ID.
     *
     * @param oneSlots all slots to consider
     */
    public void chargeOneSlots(OneSlot[] oneSlots) {
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() % slotWorkers == slotWorkerId) {
                this.oneSlots.add(oneSlot);

                oneSlot.setSlotWorkerEventloop(slotWorkerEventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
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
    private int loopCount = 0;

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
            oneSlot.doTask(loopCount);
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