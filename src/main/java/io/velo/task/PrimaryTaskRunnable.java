package io.velo.task;

import io.activej.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Runnable task that executes repeatedly with 1s delay on the primary Eventloop.
 */
public class PrimaryTaskRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PrimaryTaskRunnable.class);

    private final Consumer<Integer> task;

    /**
     * @param task the task to execute, accepts loop count as parameter
     */
    public PrimaryTaskRunnable(Consumer<Integer> task) {
        this.task = task;
    }

    private Eventloop primaryEventloop;

    /** @param primaryEventloop the Eventloop for scheduling this task */
    public void setPrimaryEventloop(Eventloop primaryEventloop) {
        this.primaryEventloop = primaryEventloop;
    }

    private int loopCount = 0;

    /** Executes the task, increments loop count, and reschedules after 1s. */
    @Override
    public void run() {
        if (isStopped) {
            return;
        }

        try {
            task.accept(loopCount);
        } catch (Exception e) {
            log.error("Primary task error, loop count={}", loopCount, e);
        }
        loopCount++;

        if (isStopped) {
            return;
        }

        primaryEventloop.delay(1000L, this);
    }

    private volatile boolean isStopped = false;

    /** Stops the task from rescheduling. */
    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for primary eventloop");
    }
}
