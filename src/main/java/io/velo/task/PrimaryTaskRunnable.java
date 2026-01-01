package io.velo.task;

import io.activej.eventloop.Eventloop;

import java.util.function.Consumer;

/**
 * A Runnable implementation designed to execute a task repeatedly with a delay on an Eventloop.
 * The task is represented by a Consumer that accepts an integer parameter, which can represent a loop count.
 * This task is running in the primary reactor of the Application.
 */
public class PrimaryTaskRunnable implements Runnable {
    private final Consumer<Integer> task;

    /**
     * Constructs a PrimaryTaskRunnable with a specified task.
     *
     * @param task the Consumer<Integer> that defines the task to be executed; the integer parameter can be used as a loop count
     */
    public PrimaryTaskRunnable(Consumer<Integer> task) {
        this.task = task;
    }

    private Eventloop primaryEventloop;

    /**
     * Sets the Eventloop on which this task will be scheduled to run.
     *
     * @param primaryEventloop the Eventloop instance to use for scheduling the task.
     */
    public void setPrimaryEventloop(Eventloop primaryEventloop) {
        this.primaryEventloop = primaryEventloop;
    }

    private int loopCount = 0;

    /**
     * Executes the assigned task, increments the loop count, and schedules itself to run again after a delay of 1000 milliseconds.
     * The task execution is stopped if the instance has been marked for stopping.
     */
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

    /**
     * Marks this task for stopping, preventing it from scheduling itself to run again.
     * Once stopped, the task will not execute further.
     */
    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for primary eventloop");
    }
}