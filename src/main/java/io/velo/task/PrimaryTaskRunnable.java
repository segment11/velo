package io.velo.task;

import io.activej.eventloop.Eventloop;

import java.util.function.Consumer;

public class PrimaryTaskRunnable implements Runnable {
    private final Consumer<Integer> task;

    public PrimaryTaskRunnable(Consumer<Integer> task) {
        this.task = task;
    }

    private Eventloop primaryEventloop;

    public void setPrimaryEventloop(Eventloop primaryEventloop) {
        this.primaryEventloop = primaryEventloop;
    }

    private int loopCount = 0;

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

    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for primary eventloop");
    }
}
