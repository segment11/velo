package io.velo.persist.index;

import io.activej.common.function.RunnableEx;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.NeedCleanUp;

public class IndexHandler implements NeedCleanUp {
    private final byte workerId;
    private final Eventloop eventloop;

    IndexHandler(byte workerId, Eventloop eventloop) {
        this.workerId = workerId;
        this.eventloop = eventloop;
    }

    long threadIdProtectedForSafe = -1;

    public Promise<Void> asyncRun(RunnableEx runnableEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                runnableEx.run();
                return Promise.complete();
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(eventloop.submit(runnableEx));
    }

    @Override
    public void cleanUp() {
        threadIdProtectedForSafe = Thread.currentThread().threadId();
        System.out.println("Index handler begin to clean up: " + workerId);
    }
}
