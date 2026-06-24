package io.velo.persist.index;

import io.activej.common.function.RunnableEx;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.NeedCleanUp;
import io.velo.extend.BetaExtend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example handler demonstrating how to extend Velo with an alternate index/persist backend.
 * Wraps a single-worker event loop and serializes work onto that event loop thread.
 */
// only a test to give an example, extend velo support other persist system
@BetaExtend
public class IndexHandler implements NeedCleanUp {
    private final byte workerId;
    private final Eventloop eventloop;

    IndexHandler(byte workerId, Eventloop eventloop) {
        this.workerId = workerId;
        this.eventloop = eventloop;
    }

    private static final Logger log = LoggerFactory.getLogger(IndexHandler.class);

    long threadIdProtectedForSafe = -1;

    /**
     * Runs the given runnable on this handler's event loop thread. If the caller
     * is already on that thread the runnable is executed inline.
     *
     * @param runnableEx the work to run
     * @return a promise completing when the runnable finishes, or with the thrown exception
     */
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
        System.out.println("Index handler begin to clean up=" + workerId);
    }
}
