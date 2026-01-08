package io.velo.persist.index;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;

public class IndexHandlerPool implements NeedCleanUp {

    @VisibleForTesting
    final IndexHandler[] indexHandlers;

    private final KeyAnalysisHandler keyAnalysisHandler;

    public KeyAnalysisHandler getKeyAnalysisHandler() {
        return keyAnalysisHandler;
    }

    @TestOnly
    public IndexHandler getIndexHandler(byte workerId) {
        return indexHandlers[workerId];
    }

    private final Eventloop[] workerEventloopArray;

    private static final Logger log = LoggerFactory.getLogger(IndexHandlerPool.class);

    private static final String KEYS_FOR_ANALYSIS = "keys-for-analysis";

    public IndexHandlerPool(byte indexWorkers, File persistDir, Config persistConfig) throws IOException {
        this.indexHandlers = new IndexHandler[indexWorkers];
        this.workerEventloopArray = new Eventloop[indexWorkers];
        for (int i = 0; i < indexWorkers; i++) {
            var eventloop = Eventloop.builder()
                    .withThreadName("index-worker-" + i)
                    .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventloopIdleMillis))
                    .build();
            workerEventloopArray[i] = eventloop;

            var indexHandler = new IndexHandler((byte) i, eventloop);
            indexHandlers[i] = indexHandler;
        }

        var keysDir = new File(persistDir, KEYS_FOR_ANALYSIS);
        if (!keysDir.exists()) {
            boolean isOk = keysDir.mkdirs();
            if (!isOk) {
                throw new RuntimeException("Create dir " + keysDir.getAbsolutePath() + " failed");
            }
        }

        try {
            this.keyAnalysisHandler = new KeyAnalysisHandler(keysDir, workerEventloopArray[0], persistConfig);
        } catch (RocksDBException e) {
            throw new RuntimeException("Create rocksdb for key analysis failed");
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
                log.warn("Index worker eventloop started, will run, worker id={}", finalI);
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
        var i = 0;
        for (var eventloop : workerEventloopArray) {
            eventloop.execute(() -> {
                System.out.println("Index worker eventloop stopping");
            });
            eventloop.breakEventloop();
            log.warn("Index worker eventloop stopped, worker index={}", i);
            i++;
        }

        for (var indexHandler : indexHandlers) {
            indexHandler.cleanUp();
        }

        keyAnalysisHandler.cleanUp();
    }
}
