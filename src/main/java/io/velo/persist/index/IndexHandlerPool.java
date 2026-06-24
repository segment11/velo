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

/**
 * Pool of {@link IndexHandler} workers backed by dedicated event loops, plus a shared
 * {@link KeyAnalysisHandler} for RocksDB based key analysis. Work is dispatched to a
 * worker by worker id.
 */
public class IndexHandlerPool implements NeedCleanUp {

    @VisibleForTesting
    final IndexHandler[] indexHandlers;

    private final KeyAnalysisHandler keyAnalysisHandler;

    /**
     * Returns the shared key analysis handler used for RocksDB based key analysis.
     *
     * @return the key analysis handler
     */
    public KeyAnalysisHandler getKeyAnalysisHandler() {
        return keyAnalysisHandler;
    }

    /**
     * Returns the index handler for the given worker id. Test only.
     *
     * @param workerId the worker id
     * @return the index handler for that worker
     */
    @TestOnly
    public IndexHandler getIndexHandler(byte workerId) {
        return indexHandlers[workerId];
    }

    private final Eventloop[] workerEventloopArray;

    private static final Logger log = LoggerFactory.getLogger(IndexHandlerPool.class);

    private static final String KEYS_FOR_ANALYSIS = "keys-for-analysis";

    /**
     * Creates the worker pool and the key analysis handler.
     *
     * @param indexWorkers  the number of index worker event loops to create
     * @param persistDir    the persist directory; a keys-for-analysis sub-directory will be created
     * @param persistConfig the configuration used for key analysis
     * @throws IOException if the keys-for-analysis directory cannot be created
     */
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

    /**
     * Starts all worker event loops on dedicated affinity-pinned threads.
     */
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

    /**
     * Runs the given consumer on the specified worker's event loop thread.
     *
     * @param workerId the target worker id
     * @param consumer the work to perform with that worker's handler
     * @return a promise completing when the work is done
     */
    public Promise<Void> run(byte workerId, Consumer<IndexHandler> consumer) {
        var indexHandler = indexHandlers[workerId];
        return indexHandler.asyncRun(() -> {
            consumer.accept(indexHandler);
        });
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
