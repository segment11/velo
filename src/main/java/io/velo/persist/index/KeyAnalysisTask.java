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
        if (loopCount % 100 == 0) {
            log.info("Key analysis task loop count: {}", loopCount);
        }

        var addCountIncreasedLastSecond = handler.addCount - handlerAddCountLastSecond;
        handlerAddCountLastSecond = handler.addCount;

        boolean isNotBusy = false;
        var now = LocalTime.now();
        if (now.isAfter(notBusyBeginTime) && now.isBefore(notBusyEndTime)) {
            isNotBusy = true;
        }

        if (isNotBusy) {
            if (addCountIncreasedLastSecond > notBusyAddCountIncreasedLastSecond) {
                isNotBusy = false;
            }
        }
        isNotBusyAfterRun = isNotBusy;

        if (!isNotBusy) {
            continueBeBusyCount++;
            if (continueBeBusyCount % 100 == 0) {
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
    void doMyTask() {
        var iterator = db.newIterator();
        if (lastIterateKeyBytes != null) {
            iterator.seek(lastIterateKeyBytes);
            if (!iterator.isValid()) {
                iterator.seekToFirst();
            }
        } else {
            iterator.seekToFirst();
        }

        Map<String, Integer> prefixCounts = new HashMap<>();

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
        }
        lastIterateKeyBytes = iterator.key();

        var sortedPrefixCounts = sortMapByValues(prefixCounts);

        // for performance
        final int maxDoSaveTopKCount = 100;

        topKPrefixCounts.clear();
        var sb = new StringBuilder();
        int innerLoopCount = 0;
        for (var entry : sortedPrefixCounts) {
            if (entry.getValue() >= doLogByKeyPrefixCountIfBiggerThan) {
                if (innerLoopCount < maxDoSaveTopKCount) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
                    topKPrefixCounts.put(entry.getKey(), entry.getValue());
                }
                innerLoopCount++;
            }
        }
        log.info("Key analysis task top {} prefix counts:\n{}", maxDoSaveTopKCount, sb);
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
