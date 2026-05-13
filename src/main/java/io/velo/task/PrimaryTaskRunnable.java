package io.velo.task;

import io.activej.eventloop.Eventloop;

import java.util.function.Consumer;

/**
 * Runnable task that executes repeatedly with 1s delay on the primary Eventloop.
 */
public class PrimaryTaskRunnable implements Runnable {
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
        task.accept(loopCount);
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