package io.velo.persist.index;

import io.activej.eventloop.Eventloop;
import io.velo.NeedCleanUp;
import io.velo.metric.InSlotMetricCollector;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class KeyAnalysisHandler implements InSlotMetricCollector, NeedCleanUp {
    private final Eventloop eventloop;
    private final RocksDB db;

    private static final byte[] NO_VALUE_BYTES = new byte[1];

    private final AtomicLong addCount = new AtomicLong();
    private final AtomicLong removeCount = new AtomicLong();

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisHandler.class);

    public KeyAnalysisHandler(File keysDir, Eventloop eventloop) throws RocksDBException {
        this.eventloop = eventloop;

        RocksDB.loadLibrary();
        var options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options, keysDir.getAbsolutePath());
        log.warn("Key analysis handler started, keysDir={}", keysDir.getAbsolutePath());
    }

    public void addKey(String key) {
        var f = eventloop.submit(() -> {
            db.put(key.getBytes(), NO_VALUE_BYTES);
        });
        f.whenComplete((v, e) -> {
            if (e != null) {
                log.error("Error in addKey, key={}", key, e);
            } else {
                addCount.incrementAndGet();
            }
        });
    }

    public void removeKey(String key) {
        var f = eventloop.submit(() -> {
            db.delete(key.getBytes());
        });
        f.whenComplete((v, e) -> {
            if (e != null) {
                log.error("Error in removeKey, key={}", key, e);
            } else {
                removeCount.incrementAndGet();
            }
        });
    }

    public CompletableFuture<Void> iterateKeys(byte[] beginKeyBytes, int batchSize, Consumer<byte[]> consumer) {
        return eventloop.submit(() -> {
            var iterator = db.newIterator();
            if (beginKeyBytes != null) {
                iterator.seek(beginKeyBytes);
                if (!iterator.isValid()) {
                    iterator.seekToFirst();
                }
            } else {
                iterator.seekToFirst();
            }

            int count = 0;
            while (iterator.isValid() && count < batchSize) {
                var keyBytes = iterator.key();
                consumer.accept(keyBytes);
                iterator.next();
                count++;
            }
        });
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        map.put("key_analysis_add_count", (double) addCount.get());
        map.put("key_analysis_remove_count", (double) removeCount.get());

        return map;
    }

    @Override
    public void cleanUp() {
        db.close();
        System.out.println("Close key analysis db");
    }
}
