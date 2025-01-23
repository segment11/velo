package io.velo.persist.index;

import io.activej.config.Config;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.config.converter.ConfigConverters.ofLocalTime;

public class KeyAnalysisTask implements KeyAnalysisHandler.InnerTask {
    private final KeyAnalysisHandler handler;
    private final RocksDB db;

    private final int notBusyAddCountIncreasedLastSecond;
    private final LocalTime notBusyBeginTime;
    private final LocalTime notBusyEndTime;

    private final int onceIterateKeyCount;
    private final int doLogByKeyPrefixCountIfBiggerThan;
    private final int groupByMinLengthForKey;
    private final int groupByMaxLengthForKey;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisTask.class);

    public KeyAnalysisTask(KeyAnalysisHandler handler, RocksDB db, Config persistConfig) {
        this.handler = handler;
        this.db = db;

        this.notBusyAddCountIncreasedLastSecond = persistConfig.get(ofInteger(), "keyAnalysis.notBusyAddCountIncreasedLastSecond", 1000);
        this.notBusyBeginTime = persistConfig.get(ofLocalTime(), "keyAnalysis.notBusyBeginTime", LocalTime.parse("00:00:00.0"));
        this.notBusyEndTime = persistConfig.get(ofLocalTime(), "keyAnalysis.notBusyEndTime", LocalTime.parse("06:00:00.0"));

        this.onceIterateKeyCount = persistConfig.get(ofInteger(), "keyAnalysis.onceIterateKeyCount", 10000);
        this.doLogByKeyPrefixCountIfBiggerThan = this.onceIterateKeyCount / 10;

        this.groupByMinLengthForKey = persistConfig.get(ofInteger(), "keyAnalysis.groupByMinLengthForKey", 5);
        this.groupByMaxLengthForKey = persistConfig.get(ofInteger(), "keyAnalysis.groupByMaxLengthForKey", 10);
    }

    private long handlerAddCountLastSecond = 0;

    private long continueBeBusyCount = 0;

    @TestOnly
    boolean isNotBusyAfterRun;

    @Override
    public void run(int loopCount) {
        var addCountIncreasedLast10Second = handler.addCount - handlerAddCountLastSecond;
        handlerAddCountLastSecond = handler.addCount;

        boolean isNotBusy = false;
        var now = LocalTime.now();
        if (now.isAfter(notBusyBeginTime) && now.isBefore(notBusyEndTime)) {
            isNotBusy = true;
        }

        if (isNotBusy) {
            if (addCountIncreasedLast10Second > notBusyAddCountIncreasedLastSecond) {
                isNotBusy = false;
            }
        }
        isNotBusyAfterRun = isNotBusy;

        if (!isNotBusy) {
            continueBeBusyCount++;
            if (continueBeBusyCount % 10 == 0) {
                log.info("Key analysis task continue be busy {} times", continueBeBusyCount);
            }
            return;
        }

        // not busy now, can do target job
        continueBeBusyCount = 0;
        doMyTask();
    }

    private byte[] lastIterateKeyBytes = null;

    final Map<String, Integer> topKPrefixCounts = new HashMap<>();

    @TestOnly
    public void addTopKPrefixCount(String prefix, int count) {
        topKPrefixCounts.put(prefix, count);
    }

    @VisibleForTesting
    int doMyTaskSkipTimes = 0;

    @VisibleForTesting
    void doMyTask() {
        if (doMyTaskSkipTimes > 0) {
            doMyTaskSkipTimes--;
            return;
        }

        var iterator = db.newIterator();
        if (lastIterateKeyBytes != null) {
            iterator.seek(lastIterateKeyBytes);
            if (!iterator.isValid()) {
                iterator.seekToFirst();
                log.warn("Key analysis task iterator seek to {} failed, seek to first again.", new String(lastIterateKeyBytes));
                topKPrefixCounts.clear();
            }
        } else {
            iterator.seekToFirst();
            log.warn("Key analysis task iterator seek to first again.");
            topKPrefixCounts.clear();
        }

        Map<String, Integer> prefixCounts = new HashMap<>();

        String fromKey = null;

        int count = 0;
        while (iterator.isValid() && count < onceIterateKeyCount) {
            var keyBytes = iterator.key();
            var key = new String(keyBytes);

            if (key.length() >= groupByMinLengthForKey) {
                for (int length = groupByMinLengthForKey; length <= groupByMaxLengthForKey; length++) {
                    if (key.length() >= length) {
                        var prefix = key.substring(0, length);
                        prefixCounts.put(prefix, prefixCounts.getOrDefault(prefix, 0) + 1);
                    }
                }
            }

            iterator.next();
            count++;
            if (count == 1) {
                fromKey = key;
            }
        }
        lastIterateKeyBytes = iterator.key();

        if (prefixCounts.isEmpty()) {
            doMyTaskSkipTimes = 2;
            return;
        }

        doMyTaskSkipTimes = 0;
        var sortedPrefixCounts = sortMapByValues(prefixCounts);

        // for performance
        final int maxDoLogCountInOneBatch = 100;
        // cost little memory
        final int maxTmpSaveTopKSize = 1000;

        var sb = new StringBuilder();
        int innerLoopCount = 0;
        for (var entry : sortedPrefixCounts) {
            if (entry.getValue() >= doLogByKeyPrefixCountIfBiggerThan) {
                if (innerLoopCount < maxDoLogCountInOneBatch) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
                }

                if (topKPrefixCounts.size() < maxTmpSaveTopKSize) {
                    topKPrefixCounts.put(entry.getKey(), entry.getValue());
                }
                innerLoopCount++;
            }
        }
        log.info("Key analysis task one batch iterate from {} to {}, iterate count: {}, tmp save top k size: {}, counts group by prefix:\n{}",
                fromKey, new String(lastIterateKeyBytes), count, topKPrefixCounts.size(), sb);

        if (count < onceIterateKeyCount) {
            // start first again
            lastIterateKeyBytes = null;
            log.warn("Key analysis task will iterate start from first again after 6 hours.");
            // skip next 6 hours
            doMyTaskSkipTimes = (int) ((6 * 3600 * 1000) / KeyAnalysisHandler.LOOP_INTERVAL_MILLIS);
        }
    }

    public static <V extends Comparable<? super V>> List<Map.Entry<String, V>> sortMapByValues(Map<String, V> map) {
        List<Map.Entry<String, V>> sortedEntries = new ArrayList<>(map.entrySet());
        sortedEntries.sort((a, b) -> {
            int compareValue = a.getValue().compareTo(b.getValue());
            // key length bigger first
            return compareValue == 0 ? b.getKey().length() - a.getKey().length() : compareValue;
        });
        return sortedEntries;
    }
}
