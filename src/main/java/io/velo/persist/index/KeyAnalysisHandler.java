package io.velo.persist.index;

import io.activej.eventloop.Eventloop;
import io.velo.NeedCleanUp;
import io.velo.metric.SimpleGauge;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class KeyAnalysisHandler implements Runnable, NeedCleanUp {
    public interface InnerTask {
        void run(int loopCount);
    }

    private final Eventloop eventloop;
    // null when do unit test
    private final InnerTask innerTask;
    private final RocksDB db;

    private long addCount = 0;
    private long addValueLengthTotal = 0;
    private long removeOrExpireCount = 0;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisHandler.class);

    public KeyAnalysisHandler(File keysDir, Eventloop eventloop) throws RocksDBException {
        this.eventloop = eventloop;

        RocksDB.loadLibrary();

        // 100 million keys, use one more cpu vcore, cost about 3GB total file size, and less than 1GB memory
        // refer to TestRocksDBConfig.groovy
        var options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setNumLevels(2)
                .setLevelZeroFileNumCompactionTrigger(8)
                .setMaxOpenFiles(64)
                .setMaxBackgroundJobs(4);
        this.db = RocksDB.open(options, keysDir.getAbsolutePath());
        log.warn("Key analysis handler started, keysDir={}", keysDir.getAbsolutePath());

        this.innerTask = new KeyAnalysisTask(db);
        eventloop.delay(1000, this);

        this.initMetricsCollect();
    }

    public void addKey(String key, int valueLengthAsInt) {
        var bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(valueLengthAsInt);
        eventloop.submit(() -> {
            db.put(key.getBytes(), bytes);
            addCount++;
            addValueLengthTotal += valueLengthAsInt;
        });
    }

    public void removeKey(String key) {
        eventloop.submit(() -> {
            db.delete(key.getBytes());
            removeOrExpireCount++;
        });
    }

    public CompletableFuture<Void> iterateKeys(byte[] beginKeyBytes, int batchSize, BiConsumer<byte[], Integer> consumer) {
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
                var valueBytes = iterator.value();
                var valueLengthAsInt = ByteBuffer.wrap(valueBytes).getInt();
                consumer.accept(keyBytes, valueLengthAsInt);
                iterator.next();
                count++;
            }
        });
    }

    private volatile boolean isStopped = false;

    private int loopCount = 0;

    @Override
    public void run() {
        loopCount++;
        if (innerTask != null) {
            innerTask.run(loopCount);
        }

        if (isStopped) {
            return;
        }

        eventloop.delay(1000L, this);
    }

    @VisibleForTesting
    final static SimpleGauge keyAnalysisGauge = new SimpleGauge("keys", "Key analysis metrics.");

    static {
        keyAnalysisGauge.register();
    }

    private void initMetricsCollect() {
        // only first slot show global metrics
        keyAnalysisGauge.addRawGetter(() -> {
            var labelValues = List.of("-1");

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            map.put("key_analysis_add_count", new SimpleGauge.ValueWithLabelValues((double) addCount, labelValues));
            map.put("key_analysis_remove_or_expire_count", new SimpleGauge.ValueWithLabelValues((double) removeOrExpireCount, labelValues));

            if (addCount > 0) {
                var addValueLengthAvg = (double) addValueLengthTotal / addCount;
                map.put("key_analysis_add_value_length_avg", new SimpleGauge.ValueWithLabelValues(addValueLengthAvg, labelValues));
            }

            return map;
        });
    }

    @Override
    public void cleanUp() {
        isStopped = true;
        System.out.println("Key analysis handler scheduler stopped");

        db.close();
        System.out.println("Close key analysis db");
    }
}
