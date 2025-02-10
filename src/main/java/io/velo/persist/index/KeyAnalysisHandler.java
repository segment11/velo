package io.velo.persist.index;

import io.activej.async.callback.AsyncComputation;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import io.velo.metric.SimpleGauge;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.config.converter.ConfigConverters.ofInteger;

public class KeyAnalysisHandler implements Runnable, NeedCleanUp {
    public interface InnerTask {
        boolean run(int loopCount);
    }

    private final File keysDir;
    private final Eventloop eventloop;
    private final Config persistConfig;
    @VisibleForTesting
    RocksDB db;

    private final int doAddWhenAddLoopIncreaseCount;
    private final long keyAnalysisNumberTotal;

    // thread not safe
    @VisibleForTesting
    final TreeMap<String, byte[]> allKeysInMemory = new TreeMap<>(String::compareTo);

    // null when do unit test
    private KeyAnalysisTask innerTask;

    @TestOnly
    public KeyAnalysisTask getInnerTask() {
        return innerTask;
    }

    public void resetInnerTask(Config persistConfig) {
        this.innerTask = new KeyAnalysisTask(this, allKeysInMemory, db, persistConfig);
    }

    long addLoopCount = 0;
    long addCount = 0;
    long addValueLengthTotal = 0;

    volatile boolean isKeyAnalysisNumberFull = false;

    static final long LOOP_INTERVAL_MILLIS = 10000L;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisHandler.class);

    private Options openOptions(Config persistConfig) {
        var rocksDBWriteBufferSize = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBWriteBufferSize", 4 * 1024 * 1024);
        var rocksDBMaxWriteBufferNumber = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBMaxWriteBufferNumber", 4);
        var rocksDBMinWriteBufferNumberToMerge = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBMinWriteBufferNumberToMerge", 2);
        var rocksDBNumLevels = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBNumLevels", 2);
        var rocksDBLevelZeroFileNumCompactionTrigger = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBLevelZeroFileNumCompactionTrigger", 8);
        var rocksDBMaxOpenFiles = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBMaxOpenFiles", 128);
        var rocksDBMaxBackgroundJobs = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBMaxBackgroundJobs", 4);
        var rocksDBFixedLengthPrefixExtractor = persistConfig.get(ofInteger(), "keyAnalysis.rocksDBFixedLengthPrefixExtractor", 3);

//        long totalEstimateKeyNumber = ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber;

        // 100 million keys, use one more cpu vcore, cost about 3GB total file size, and less than 1GB memory
        // refer to TestRocksDBConfig.groovy
        return new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setWriteBufferSize(rocksDBWriteBufferSize)
                .setMaxWriteBufferNumber(rocksDBMaxWriteBufferNumber)
                .setMinWriteBufferNumberToMerge(rocksDBMinWriteBufferNumberToMerge)
                .setNumLevels(rocksDBNumLevels)
                .setLevelZeroFileNumCompactionTrigger(rocksDBLevelZeroFileNumCompactionTrigger)
                .setMaxOpenFiles(rocksDBMaxOpenFiles)
                .setMaxBackgroundJobs(rocksDBMaxBackgroundJobs)
                .useFixedLengthPrefixExtractor(rocksDBFixedLengthPrefixExtractor);
    }

    private void createDB() throws RocksDBException {
        this.db = RocksDB.open(openOptions(persistConfig), keysDir.getAbsolutePath());
        log.warn("Key analysis db created, keysDir={}", keysDir.getAbsolutePath());

        this.addCount = db.getLongProperty("rocksdb.estimate-num-keys");
    }

    public KeyAnalysisHandler(File keysDir, Eventloop eventloop, Config persistConfig) throws RocksDBException {
        this.keysDir = keysDir;
        this.eventloop = eventloop;
        this.persistConfig = persistConfig;

        RocksDB.loadLibrary();
        if (!ConfForGlobal.pureMemory) {
            createDB();
        }
        this.innerTask = new KeyAnalysisTask(this, allKeysInMemory, db, persistConfig);

        this.doAddWhenAddLoopIncreaseCount = 100 / ConfForGlobal.keyAnalysisNumberPercent;
        this.keyAnalysisNumberTotal = ConfForGlobal.keyAnalysisNumberPercent *
                ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber / 100;
        log.warn("Key analysis number total={}, do add when add loop increase count={}",
                keyAnalysisNumberTotal, doAddWhenAddLoopIncreaseCount);

        eventloop.delay(LOOP_INTERVAL_MILLIS, this);

        this.initMetricsCollect();
    }

    public void addKey(String key, int valueLengthHigh24WithShortTypeLow8) {
        if (isKeyAnalysisNumberFull) {
            return;
        }

        if (addLoopCount % doAddWhenAddLoopIncreaseCount != 0) {
            addLoopCount++;
            // skip
            return;
        }
        addLoopCount++;

        var bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(valueLengthHigh24WithShortTypeLow8);
        eventloop.submit(() -> {
            if (ConfForGlobal.pureMemory) {
                allKeysInMemory.put(key, bytes);
            } else {
                db.put(key.getBytes(), bytes);
            }

            addCount++;
            addValueLengthTotal += (valueLengthHigh24WithShortTypeLow8 >> 8);

            if (addCount >= keyAnalysisNumberTotal) {
                isKeyAnalysisNumberFull = true;
            }
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
                lastKeyBytes = keyBytes;

                if (ConfForGlobal.pureMemory) {
                    allKeysInMemory.put(new String(keyBytes), bytes);
                } else {
                    wb.put(keyBytes, bytes);
                }

                keyCount++;
            }

            if (!ConfForGlobal.pureMemory) {
                db.write(new WriteOptions(), wb);
            }
            return new LastKeyBytesWithKeyCount(lastKeyBytes, keyCount);
        }));
    }

    @TestOnly
    void removeKey(String key) {
        eventloop.submit(() -> {
            if (ConfForGlobal.pureMemory) {
                allKeysInMemory.remove(key);
            } else {
                db.delete(key.getBytes());
            }
        });
    }

    public long allKeyCount() {
        if (ConfForGlobal.pureMemory) {
            return allKeysInMemory.size();
        } else {
            try {
                return db.getLongProperty("rocksdb.estimate-num-keys");
            } catch (RocksDBException e) {
                return -1L;
            }
        }
    }

    void clearAllKeysAfterAnalysis() throws IOException, RocksDBException {
        if (ConfForGlobal.pureMemory) {
            allKeysInMemory.clear();
        } else {
            db.close();
            log.warn("Close key analysis db");
            FileUtils.deleteDirectory(keysDir);
            log.warn("Delete key analysis dir");

            createDB();
            this.innerTask = new KeyAnalysisTask(this, allKeysInMemory, db, persistConfig);
        }

        addCount = 0;
        addValueLengthTotal = 0;

        isKeyAnalysisNumberFull = false;
    }

    public CompletableFuture<Void> flushdb() {
        return eventloop.submit(this::clearAllKeysAfterAnalysis);
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
            if (ConfForGlobal.pureMemory) {
                int count = 0;
                for (var entry : beginKeyBytes != null ? allKeysInMemory.tailMap(new String(beginKeyBytes), false).entrySet() : allKeysInMemory.entrySet()) {
                    var key = entry.getKey();
                    var valueBytes = entry.getValue();
                    var valueLengthAsInt = ByteBuffer.wrap(valueBytes).getInt();
                    consumer.accept(key.getBytes(), valueLengthAsInt);
                    if (++count >= batchSize) {
                        break;
                    }
                }
                return;
            }

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
            var result = new ArrayList<String>();
            if (ConfForGlobal.pureMemory) {
                for (var entry : beginKeyBytes != null ? allKeysInMemory.tailMap(new String(beginKeyBytes), false).entrySet() : allKeysInMemory.entrySet()) {
                    boolean isKeyMatch = true;
                    boolean isValueMatch = true;

                    var key = entry.getKey();
                    var valueBytes = entry.getValue();

                    if (keyFilter != null) {
                        isKeyMatch = keyFilter.test(key);
                    }

                    if (isKeyMatch) {
                        if (valueBytesAsIntFilter != null) {
                            var valueLengthAsInt = ByteBuffer.wrap(valueBytes).getInt();
                            isValueMatch = valueBytesAsIntFilter.test(valueLengthAsInt);
                        }
                    }

                    if (isKeyMatch && isValueMatch) {
                        result.add(key);
                        if (result.size() >= expectedCount) {
                            break;
                        }
                    }
                }
                return result;
            }

            var iterator = db.newIterator();
            seekIterator(beginKeyBytes, iterator, false);

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
            var result = new ArrayList<String>();

            if (ConfForGlobal.pureMemory) {
                for (var entry : allKeysInMemory.tailMap(prefix).entrySet()) {
                    var key = entry.getKey();
                    if (key.startsWith(prefix)) {
                        if (pattern.matcher(key).matches()) {
                            result.add(key);
                            if (result.size() >= maxCount) {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }

                return result;
            }

            var iterator = db.newIterator();
            iterator.seek(prefix.getBytes());

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
            var isEnd = innerTask.run(loopCount);
            if (isEnd) {
                try {
                    clearAllKeysAfterAnalysis();
                } catch (Exception e) {
                    log.error("Key analysis clear all keys after analysis failed", e);
                }
            }
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
            if (addCount > 0) {
                map.put("key_analysis_all_key_count", new SimpleGauge.ValueWithLabelValues((double) allKeyCount(), labelValues));

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

        if (!ConfForGlobal.pureMemory) {
            db.close();
            System.out.println("Close key analysis db");
        }
    }
}
