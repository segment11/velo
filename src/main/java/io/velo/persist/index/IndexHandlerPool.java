package io.velo.persist.index;

import io.activej.common.function.RunnableEx;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class IndexHandlerPool implements NeedCleanUp {

    @VisibleForTesting
    final IndexHandler[] indexHandlers;
    private final Eventloop[] workerEventloopArray;

    private static final Logger log = LoggerFactory.getLogger(IndexHandlerPool.class);

    public IndexHandlerPool(byte indexWorkers) {
        this.indexHandlers = new IndexHandler[indexWorkers];
        this.workerEventloopArray = new Eventloop[indexWorkers];
        for (int i = 0; i < indexWorkers; i++) {
            var eventloop = Eventloop.builder()
                    .withThreadName("index-worker-" + i)
                    .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventLoopIdleMillis))
                    .build();
            workerEventloopArray[i] = eventloop;

            var indexHandler = new IndexHandler((byte) i, eventloop);
            indexHandlers[i] = indexHandler;
        }
    }

    public void start() {
        var affinityThreadFactory = new AffinityThreadFactory("index-worker",
                AffinityStrategies.SAME_SOCKET, AffinityStrategies.DIFFERENT_CORE);

        for (int i = 0; i < indexHandlers.length; i++) {
            var eventloop = workerEventloopArray[i];

            int finalI = i;
            affinityThreadFactory.newThread(() -> {
                eventloop.keepAlive(true);
                log.warn("Index worker eventloop started, will run, worker id: {}", finalI);
                eventloop.run();
            }).start();

            eventloop.execute(() -> {
                var threadId = Thread.currentThread().threadId();
                indexHandlers[finalI].threadIdProtectedForSafe = threadId;
                log.warn("Fix index handler thread id, i={}, tid={}", finalI, threadId);
            });
        }
    }

    public Promise<Void> run(byte workerId, RunnableEx runnableEx) {
        return indexHandlers[workerId].asyncRun(runnableEx);
    }

    public byte getChargeWorkerIdByWordKeyHash(long wordKeyHash) {
        if (indexHandlers.length == 1) {
            return 0;
        }
        return (byte) (wordKeyHash % indexHandlers.length);
    }

    @Override
    public void cleanUp() {
        for (var eventloop : workerEventloopArray) {
            eventloop.breakEventloop();
            log.warn("Index worker eventloop stopped, worker thread id: {}", eventloop.getEventloopThread().threadId());
        }

        for (var indexHandler : indexHandlers) {
            indexHandler.cleanUp();
        }
    }
}
