package io.velo.task;

import io.activej.eventloop.Eventloop;
import io.velo.RequestHandler;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

public class TaskRunnable implements Runnable {
    private final byte netWorkerId;
    private final byte netWorkers;

    public TaskRunnable(byte netWorkerId, byte netWorkers) {
        this.netWorkerId = netWorkerId;
        this.netWorkers = netWorkers;
    }

    @VisibleForTesting
    final ArrayList<OneSlot> oneSlots = new ArrayList<>();

    public void chargeOneSlots(OneSlot[] oneSlots) {
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() % netWorkers == netWorkerId) {
                this.oneSlots.add(oneSlot);

                oneSlot.setNetWorkerEventloop(netWorkerEventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
    }

    private Eventloop netWorkerEventloop;

    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
    }

    private RequestHandler requestHandler;

    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private int loopCount = 0;

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

    private volatile boolean isStopped = false;

    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for net worker eventloop, net worker id=" + netWorkerId);
    }
}
