package io.velo.task;

import io.activej.eventloop.Eventloop;
import io.velo.RequestHandler;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * A Runnable implementation that represents a task for a network worker.
 * It processes tasks associated with specific slots and runs them in a loop.
 */
public class TaskRunnable implements Runnable {
    /**
     * The ID of the network worker.
     */
    private final byte netWorkerId;

    /**
     * The total number of network workers.
     */
    private final byte netWorkers;

    /**
     * Constructs a TaskRunnable with the specified network worker ID and total number of network workers.
     *
     * @param netWorkerId The ID of the network worker.
     * @param netWorkers  The total number of network workers.
     */
    public TaskRunnable(byte netWorkerId, byte netWorkers) {
        this.netWorkerId = netWorkerId;
        this.netWorkers = netWorkers;
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
            if (oneSlot.slot() % netWorkers == netWorkerId) {
                this.oneSlots.add(oneSlot);

                oneSlot.setNetWorkerEventloop(netWorkerEventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
    }

    /**
     * The event loop for this network worker.
     */
    private Eventloop netWorkerEventloop;

    /**
     * Sets the event loop for this network worker.
     *
     * @param netWorkerEventloop The event loop to be used.
     */
    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
    }

    /**
     * The request handler used by this network worker.
     */
    private RequestHandler requestHandler;

    /**
     * Sets the request handler for this network worker.
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
        for (var oneSlot : oneSlots) {
            oneSlot.doTask(loopCount);
        }
        loopCount++;

        if (isStopped) {
            return;
        }

        final long INTERVAL_MS = 10L;
        netWorkerEventloop.delay(INTERVAL_MS, this);
    }

    /**
     * A flag indicating whether this task runnable should be stopped.
     */
    private volatile boolean isStopped = false;

    /**
     * Stops this task runnable by setting the isStopped flag.
     * It also prints a message indicating that the task delay has stopped.
     */
    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for net worker eventloop, net worker id=" + netWorkerId);
    }
}