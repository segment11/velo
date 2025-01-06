package io.velo.persist.index;

import io.activej.async.callback.AsyncComputation;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.velo.NeedCleanUp;
import io.velo.metric.SimpleGauge;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.config.converter.ConfigConverters.ofInteger;

public class KeyAnalysisHandler implements Runnable, NeedCleanUp {
    public interface InnerTask {
        void run(int loopCount);
    }

    private final Eventloop eventloop;
    @VisibleForTesting
    final RocksDB db;

    // null when do unit test
    private KeyAnalysisTask innerTask;

    @TestOnly
    public KeyAnalysisTask getInnerTask() {
        return innerTask;
    }

    public void resetInnerTask(Config persistConfig) {
        this.innerTask = new KeyAnalysisTask(this, db, persistConfig);
    }

    long addCount = 0;
    long addValueLengthTotal = 0;
    long removeOrExpireCount = 0;

    static final long LOOP_INTERVAL_MILLIS = 10000L;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisHandler.class);

    public KeyAnalysisHandler(File keysDir, Eventloop eventloop, Config persistConfig) throws RocksDBException {
        this.eventloop = eventloop;

        RocksDB.loadLibrary();

        var keyMatchPrefixLength = persistConfig.get(ofInteger(), "keyAnalysis.keyMatchPrefixLength", 3);

        // 100 million keys, use one more cpu vcore, cost about 3GB total file size, and less than 1GB memory
        // refer to TestRocksDBConfig.groovy
        var options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setNumLevels(2)
                .setLevelZeroFileNumCompactionTrigger(8)
                .setMaxOpenFiles(64)
                .setMaxBackgroundJobs(4)
                .useFixedLengthPrefixExtractor(keyMatchPrefixLength);
        this.db = RocksDB.open(options, keysDir.getAbsolutePath());
        log.warn("Key analysis handler started, keysDir={}", keysDir.getAbsolutePath());

        this.innerTask = new KeyAnalysisTask(this, db, persistConfig);
        eventloop.delay(LOOP_INTERVAL_MILLIS, this);

        this.initMetricsCollect();
    }

    public void addKey(String key, int valueLengthHigh24WithShortTypeLow8) {
        var bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(valueLengthHigh24WithShortTypeLow8);
        eventloop.submit(() -> {
            db.put(key.getBytes(), bytes);
            addCount++;
            addValueLengthTotal += (valueLengthHigh24WithShortTypeLow8 >> 8);
        });
    }

    public record LastKeyBytesWithKeyCount(byte[] lastKeyBytes, int keyCount) {
    }

    public CompletableFuture<LastKeyBytesWithKeyCount> addBatch(byte[] keysWithValueInt) {
        return eventloop.submit(AsyncComputation.of(() -> {
            var wb = new WriteBatch();
            var buffer = ByteBuffer.wrap(keysWithValueInt);

            int keyCount = 0;
            byte[] lastKeyBytes = null;
            while (buffer.hasRemaining()) {
                var keyBytesLength = buffer.getShort();
                var keyBytes = new byte[keyBytesLength];
                buffer.get(keyBytes);

                var bytes = new byte[4];
                buffer.get(bytes);

                wb.put(keyBytes, bytes);
                lastKeyBytes = keyBytes;

                keyCount++;
            }

            db.write(new WriteOptions(), wb);
            return new LastKeyBytesWithKeyCount(lastKeyBytes, keyCount);
        }));
    }

    public void removeKey(String key) {
        eventloop.submit(() -> {
            db.delete(key.getBytes());
            removeOrExpireCount++;
        });
    }

    private void seekIterator(byte[] beginKeyBytes, RocksIterator iterator, boolean isIncludeBeginKey) {
        if (beginKeyBytes != null) {
            iterator.seek(beginKeyBytes);
            if (!iterator.isValid()) {
                iterator.seekToFirst();
            }

            if (!isIncludeBeginKey) {
                // move to next
                iterator.next();
                if (!iterator.isValid()) {
                    iterator.seekToLast();
                }
            }
        } else {
            iterator.seekToFirst();
        }
    }

    public CompletableFuture<Void> iterateKeys(byte[] beginKeyBytes, int batchSize, boolean isIncludeBeginKey, @NotNull BiConsumer<byte[], Integer> consumer) {
        return eventloop.submit(() -> {
            var iterator = db.newIterator();
            seekIterator(beginKeyBytes, iterator, isIncludeBeginKey);

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

    // begin key exclude
    public CompletableFuture<ArrayList<String>> filterKeys(byte[] beginKeyBytes, int expectedCount,
                                                           @Nullable Predicate<String> keyFilter,
                                                           @Nullable Predicate<Integer> valueBytesAsIntFilter) {
        return eventloop.submit(AsyncComputation.of(() -> {
            var iterator = db.newIterator();
            seekIterator(beginKeyBytes, iterator, false);

            var result = new ArrayList<String>();
            while (iterator.isValid() && result.size() < expectedCount) {
                boolean isKeyMatch = true;
                boolean isValueMatch = true;

                var keyBytes = iterator.key();
                var key = new String(keyBytes);

                if (keyFilter != null) {
                    isKeyMatch = keyFilter.test(key);
                }

                if (isKeyMatch) {
                    if (valueBytesAsIntFilter != null) {
                        var valueBytes = iterator.value();
                        var valueLengthAsInt = ByteBuffer.wrap(valueBytes).getInt();
                        isValueMatch = valueBytesAsIntFilter.test(valueLengthAsInt);
                    }
                }

                if (isKeyMatch && isValueMatch) {
                    result.add(key);
                }

                iterator.next();
            }

            return result;
        }));
    }

    public CompletableFuture<ArrayList<String>> prefixMatch(@NotNull String prefix, Pattern pattern, int maxCount) {
        return eventloop.submit(AsyncComputation.of(() -> {
            var iterator = db.newIterator();
            iterator.seek(prefix.getBytes());

            var result = new ArrayList<String>();
            while (iterator.isValid() && result.size() < maxCount) {
                var keyBytes = iterator.key();
                var key = new String(keyBytes);
                if (key.startsWith(prefix)) {
                    if (pattern.matcher(key).matches()) {
                        result.add(key);
                    }
                } else {
                    break;
                }
                iterator.next();
            }

            return result;
        }));
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

        eventloop.delay(LOOP_INTERVAL_MILLIS, this);
    }

    public CompletableFuture<Map<String, Integer>> getTopKPrefixCounts() {
        // use a copy one
        return eventloop.submit(AsyncComputation.of(() -> new HashMap<>(innerTask.topKPrefixCounts)));
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
