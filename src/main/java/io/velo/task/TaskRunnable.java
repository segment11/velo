package io.velo.task;

import io.activej.eventloop.Eventloop;
import io.velo.RequestHandler;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * A Runnable implementation that represents a task for a slot worker.
 * It processes tasks associated with specific slots and runs them in a loop.
 */
public class TaskRunnable implements Runnable {
    /**
     * The ID of the slot worker.
     */
    private final byte slotWorkerId;

    /**
     * The total number of slot workers.
     */
    private final byte slotWorkers;

    /**
     * Constructs a TaskRunnable with the specified slot worker ID and total number of slot workers.
     *
     * @param slotWorkerId The ID of the slot worker.
     * @param slotWorkers  The total number of slot workers.
     */
    public TaskRunnable(byte slotWorkerId, byte slotWorkers) {
        this.slotWorkerId = slotWorkerId;
        this.slotWorkers = slotWorkers;
    }

    /**
     * A list of slots that this task runnable is responsible for.
     */
    @VisibleForTesting
    final ArrayList<OneSlot> oneSlots = new ArrayList<>();

    /**
     * Adds slots to the task runnable based on the worker ID.
     * Only slots where slot() % netWorkers equals netWorkerId are added to this worker's list.
     *
     * @param oneSlots The array of slots to be potentially added to this worker's list.
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

    /**
     * The event loop for this slot worker.
     */
    private Eventloop slotWorkerEventloop;

    /**
     * Sets the event loop for this slot worker.
     *
     * @param slotWorkerEventloop The event loop to be used.
     */
    public void setSlotWorkerEventloop(Eventloop slotWorkerEventloop) {
        this.slotWorkerEventloop = slotWorkerEventloop;
    }

    /**
     * The request handler used by this slot worker.
     */
    private RequestHandler requestHandler;

    /**
     * Sets the request handler for this slot worker.
     *
     * @param requestHandler The request handler to be used.
     */
    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    /**
     * The number of loops completed by this task runnable.
     */
    private int loopCount = 0;

    /**
     * Runs the tasks of the slots managed by this task runnable in a loop.
     * If the task runnable is stopped, it does not schedule itself again.
     */
    @Override
    public void run() {
        if (isStartDone) {
            for (var oneSlot : oneSlots) {
                oneSlot.doTask(loopCount);
            }
            loopCount++;

            if (isStopped) {
                return;
            }

            final long INTERVAL_MS = 10L;
            slotWorkerEventloop.delay(INTERVAL_MS, this);
        }
    }

    /**
     * A flag indicating whether this task runnable should be stopped.
     */
    private volatile boolean isStopped = false;
    private volatile boolean isStartDone = false;

    /**
     * Sets the start done flag for this task runnable.
     *
     * @param isStartDone The flag indicating whether the task runnable has started.
     */
    public void startDone(boolean isStartDone) {
        this.isStartDone = isStartDone;
    }

    /**
     * Stops this task runnable by setting the isStopped flag.
     * It also prints a message indicating that the task delay has stopped.
     */
    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for slot worker eventloop, slot worker id=" + slotWorkerId);
    }
}