package io.velo.persist.index;

import io.activej.async.callback.AsyncComputation;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import io.velo.metric.SimpleGauge;
import io.velo.persist.Wal;
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

/**
 * Background key analysis handler backed by RocksDB. Samples keys written to the system,
 * periodically groups them by prefix during quiet periods, and exposes the resulting
 * top-k prefix counts. All DB access is serialized onto a single event loop.
 */
public class KeyAnalysisHandler implements Runnable, NeedCleanUp {
    /**
     * Inner periodic task interface run by the handler on the event loop.
     */
    public interface InnerTask {
        /**
         * Runs one iteration of the task.
         *
         * @param loopCount the current loop count
         * @return true if the task has reached its end
         */
        boolean run(int loopCount);
    }

    private final File keysDir;
    private final Eventloop eventloop;
    private final Config persistConfig;
    @VisibleForTesting
    RocksDB db;

    private final int doAddWhenAddLoopIncreaseCount;
    private final long keyAnalysisNumberTotal;

    // null when do unit test
    private KeyAnalysisTask innerTask;

    private Runnable metricsUnregister;

    /**
     * Returns the current inner analysis task. Test only.
     *
     * @return the inner task, or null during unit tests
     */
    @TestOnly
    public KeyAnalysisTask getInnerTask() {
        return innerTask;
    }

    /**
     * Replaces the inner analysis task with a new one built from the given config.
     *
     * @param persistConfig the configuration for the new task
     */
    public void resetInnerTask(Config persistConfig) {
        this.innerTask = new KeyAnalysisTask(this, db, persistConfig);
    }

    long addLoopCount = 0;
    long addCount = 0;

    volatile boolean isKeyAnalysisNumberFull = false;

    static final long LOOP_INTERVAL_MILLIS = 10000L;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisHandler.class);

    private Options initOptions;

    private Options openOptions(Config persistConfig) {
        if (initOptions != null) {
            return initOptions;
        }

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
        initOptions = new Options()
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
        return initOptions;
    }

    private void createDB() throws RocksDBException {
        this.db = RocksDB.open(openOptions(persistConfig), keysDir.getAbsolutePath());
        log.warn("Key analysis db created, keysDir={}", keysDir.getAbsolutePath());

        this.addCount = db.getLongProperty("rocksdb.estimate-num-keys");
    }

    /**
     * Creates the handler, opens (or creates) the RocksDB instance under the given directory,
     * and schedules periodic analysis on the provided event loop.
     *
     * @param keysDir       the directory holding the RocksDB files
     * @param eventloop     the event loop used for serialized DB access and periodic runs
     * @param persistConfig the configuration for RocksDB options and analysis tuning
     * @throws RocksDBException if the RocksDB instance cannot be opened
     */
    public KeyAnalysisHandler(File keysDir, Eventloop eventloop, Config persistConfig) throws RocksDBException {
        this.keysDir = keysDir;
        this.eventloop = eventloop;
        this.persistConfig = persistConfig;

        RocksDB.loadLibrary();
        createDB();
        this.innerTask = new KeyAnalysisTask(this, db, persistConfig);

        this.doAddWhenAddLoopIncreaseCount = 100 / ConfForGlobal.keyAnalysisNumberPercent;
        this.keyAnalysisNumberTotal = ConfForGlobal.keyAnalysisNumberPercent *
                ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber / 100;
        log.warn("Key analysis number total={}, do add when add loop increase count={}",
                keyAnalysisNumberTotal, doAddWhenAddLoopIncreaseCount);

        eventloop.delay(LOOP_INTERVAL_MILLIS, this);

        this.initMetricsCollect();
    }

    /**
     * Samples a key into the analysis store. Sampling rate is controlled by the configured
     * percent and writing stops once the configured key analysis number is full.
     *
     * @param key                              the key to sample
     * @param valueLengthHigh24WithShortTypeLow8 the value length (high 24 bits) combined with short type (low 8 bits)
     */
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
            db.put(Wal.keyBytes(key), bytes);

            addCount++;
            if (addCount >= keyAnalysisNumberTotal) {
                isKeyAnalysisNumberFull = true;
            }
        });
    }

    /**
     * Result of a batched add: the last key bytes written and how many keys were written.
     *
     * @param lastKeyBytes the last key bytes written in the batch
     * @param keyCount     the number of keys written in the batch
     */
    public record LastKeyBytesWithKeyCount(byte[] lastKeyBytes, int keyCount) {
    }

    /**
     * Writes a batch of keys (each with a 4-byte value int) into the analysis store.
     *
     * @param keysWithValueInt a serialized buffer of [2-byte key length][key bytes][4-byte value int] entries
     * @return a future completing with the last key bytes written and the key count
     */
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

                wb.put(keyBytes, bytes);

                keyCount++;
            }

            db.write(new WriteOptions(), wb);
            return new LastKeyBytesWithKeyCount(lastKeyBytes, keyCount);
        }));
    }

    @TestOnly
    void removeKey(String key) {
        eventloop.submit(() -> {
            db.delete(Wal.keyBytes(key));
        });
    }

    /**
     * Returns the estimated total number of keys in the analysis store.
     *
     * @return the estimated key count, or -1 if it cannot be read
     */
    public long allKeyCount() {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            return -1L;
        }
    }

    void clearAllKeysAfterAnalysis() throws IOException, RocksDBException {
        db.close();
        log.warn("Close key analysis db");
        FileUtils.deleteDirectory(keysDir);
        log.warn("Delete key analysis dir");

        createDB();
        this.innerTask = new KeyAnalysisTask(this, db, this.innerTask);

        addCount = 0;

        isKeyAnalysisNumberFull = false;
    }

    /**
     * Clears the analysis store, deleting all keys and resetting counters. Runs on the event loop.
     *
     * @return a future completing when the store has been cleared and recreated
     */
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

    /**
     * Iterates up to {@code batchSize} keys starting from (optionally) the given begin key,
     * invoking the consumer with each key's bytes and value int.
     *
     * @param beginKeyBytes       the key to seek to, or null to start from the first key
     * @param batchSize           the maximum number of keys to visit
     * @param isIncludeBeginKey   whether the begin key itself is included in the iteration
     * @param consumer            called with each key bytes and its value length int
     * @return a future completing when the iteration finishes
     */
    public CompletableFuture<Void> iterateKeys(byte[] beginKeyBytes, int batchSize, boolean isIncludeBeginKey, @NotNull BiConsumer<byte[], Integer> consumer) {
        return eventloop.submit(() -> {
            try (var iterator = db.newIterator()) {
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
            }
        });
    }

    /**
     * Collects up to {@code expectedCount} keys (excluding the begin key) that optionally
     * match the key and value-int filters.
     *
     * @param beginKeyBytes          the exclusive begin key, or null to start from the first key
     * @param expectedCount          the maximum number of matching keys to return
     * @param keyFilter              optional predicate on the decoded key, or null
     * @param valueBytesAsIntFilter  optional predicate on the value length int, or null
     * @return a future completing with the list of matching keys
     */
    // begin key exclude
    public CompletableFuture<ArrayList<String>> filterKeys(byte[] beginKeyBytes, int expectedCount,
                                                           @Nullable Predicate<String> keyFilter,
                                                           @Nullable Predicate<Integer> valueBytesAsIntFilter) {
        return eventloop.submit(AsyncComputation.of(() -> {
            var result = new ArrayList<String>();
            try (var iterator = db.newIterator()) {
                seekIterator(beginKeyBytes, iterator, false);

                while (iterator.isValid() && result.size() < expectedCount) {
                    boolean isKeyMatch = true;
                    boolean isValueMatch = true;

                    var keyBytes = iterator.key();
                    var key = Wal.keyString(keyBytes);

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
            }
        }));
    }

    /**
     * Collects up to {@code maxCount} keys that start with the given prefix and match the
     * optional regex pattern.
     *
     * @param prefix  the key prefix to seek to and match
     * @param pattern an optional regex pattern, or null to match all prefixed keys
     * @param maxCount the maximum number of matching keys to return
     * @return a future completing with the list of matching keys
     */
    public CompletableFuture<ArrayList<String>> prefixMatch(@NotNull String prefix, Pattern pattern, int maxCount) {
        return eventloop.submit(AsyncComputation.of(() -> {
            var result = new ArrayList<String>();
            try (var iterator = db.newIterator()) {
                iterator.seek(Wal.keyBytes(prefix));

                while (iterator.isValid() && result.size() < maxCount) {
                    var keyBytes = iterator.key();
                    var key = Wal.keyString(keyBytes);
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
            }
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

    /**
     * Returns a snapshot of the current top-k prefix counts collected by the analysis task.
     *
     * @return a future completing with a map of prefix to count
     */
    public CompletableFuture<Map<String, Integer>> getTopKPrefixCounts() {
        return eventloop.submit(AsyncComputation.of(() -> new LinkedHashMap<>(innerTask.topKPrefixCounts)));
    }

    @VisibleForTesting
    final static SimpleGauge keyAnalysisGauge = new SimpleGauge("keys", "Key analysis metrics.", "slot");

    static {
        keyAnalysisGauge.register();
    }

    private void initMetricsCollect() {
        // only first slot show global metrics
        metricsUnregister = keyAnalysisGauge.addRawGetter(() -> {
            var labelValues = List.of("-1");

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            map.put("key_analysis_add_count", new SimpleGauge.ValueWithLabelValues((double) addCount, labelValues));
            if (addCount > 0) {
                map.put("key_analysis_all_key_count", new SimpleGauge.ValueWithLabelValues((double) allKeyCount(), labelValues));
            }

            return map;
        });
    }

    @Override
    public void cleanUp() {
        isStopped = true;
        System.out.println("Key analysis handler scheduler stopped");
        if (metricsUnregister != null) {
            metricsUnregister.run();
        }

        db.close();
        System.out.println("Close key analysis db");
    }
}
