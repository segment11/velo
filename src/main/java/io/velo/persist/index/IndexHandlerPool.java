package io.velo.persist.index;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;

public class IndexHandlerPool implements NeedCleanUp {

    @VisibleForTesting
    final IndexHandler[] indexHandlers;
    private final Eventloop[] workerEventloopArray;

    private static final Logger log = LoggerFactory.getLogger(IndexHandlerPool.class);

    private static final String INDEX_DIR_NAME = "reverse-index";

    public IndexHandlerPool(byte indexWorkers, File persistDir, Config persistConfig) throws IOException {
        var eachIndexHandlerChunkFdNumber = getEachIndexHandlerChunkFdNumber(indexWorkers);

        var indexDir = new File(persistDir, INDEX_DIR_NAME);
        if (!indexDir.exists()) {
            boolean isOk = indexDir.mkdirs();
            if (!isOk) {
                throw new RuntimeException("Create dir " + indexDir.getAbsolutePath() + " failed");
            }
        }

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

            var workerIdDir = new File(indexDir, "worker-" + i);
            if (!workerIdDir.exists()) {
                boolean isOk = workerIdDir.mkdirs();
                if (!isOk) {
                    throw new RuntimeException("Create dir " + workerIdDir.getAbsolutePath() + " failed");
                }
            }
            indexHandler.initChunk((byte) eachIndexHandlerChunkFdNumber, workerIdDir, persistConfig);
        }
    }

    private static long getEachIndexHandlerChunkFdNumber(byte indexWorkers) {
        var totalEstimateKeyNumber = ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber;
        var totalReverseIndexChunkFdNumber = totalEstimateKeyNumber / ReverseIndexChunk.ONE_FD_ESTIMATE_KV_COUNT;
        if (totalReverseIndexChunkFdNumber == 0) {
            totalReverseIndexChunkFdNumber = 1;
        }

        var eachIndexHandlerChunkFdNumber = totalReverseIndexChunkFdNumber / indexWorkers;
        if (eachIndexHandlerChunkFdNumber == 0) {
            eachIndexHandlerChunkFdNumber = 1;
        }
        return eachIndexHandlerChunkFdNumber;
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

    public Promise<Void> run(byte workerId, Consumer<IndexHandler> consumer) {
        var indexHandler = indexHandlers[workerId];
        return indexHandler.asyncRun(() -> {
            consumer.accept(indexHandler);
        });
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
