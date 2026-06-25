package io.velo;

import io.velo.persist.KeyBucket;

import java.util.Random;

/**
 * Measures the cost of the simple key-bucket capacity metrics path.
 *
 * <p>The simple path avoids scanning every bucket. Scrape-time metrics are derived from
 * per-slot total key counts, and skew/risk signals are updated opportunistically when
 * a bucket is written.</p>
 */
public class TestSimpleKeyBucketMetricCalcCost {
    private static final int DEFAULT_ROUNDS = 10;
    private static final int DEFAULT_SLOT_COUNT = 1024;
    private static final int DEFAULT_WRITE_UPDATES = 1_000_000;

    record SimpleSnapshot(long persistKeyCount,
                          long estimatedCapacity,
                          double avgKeysPerBucket,
                          double estimatedFillRate,
                          int observedMaxKeyCount,
                          double observedMaxFillRate,
                          long fullRiskWriteCountTotal,
                          long splitToMaxCountTotal,
                          long bucketFullExceptionCountTotal) {
    }

    static final class ObservedBucketMetrics {
        int observedMaxKeyCount;
        double observedMaxFillRate;
        long fullRiskWriteCountTotal;
        long splitToMaxCountTotal;
        long bucketFullExceptionCountTotal;

        void onBucketWrite(int keyCount, int splitNumber, boolean bucketFullException) {
            int capacity = KeyBucket.INIT_CAPACITY * splitNumber;
            double fillRate = capacity == 0 ? 0 : keyCount / (double) capacity;

            if (keyCount > observedMaxKeyCount) {
                observedMaxKeyCount = keyCount;
            }
            if (fillRate > observedMaxFillRate) {
                observedMaxFillRate = fillRate;
            }
            if (keyCount * 10 >= capacity * 9) {
                fullRiskWriteCountTotal++;
            }
            if (splitNumber == 9) {
                splitToMaxCountTotal++;
            }
            if (bucketFullException) {
                bucketFullExceptionCountTotal++;
            }
        }
    }

    public static void main(String[] args) {
        int slotCount = args.length >= 1 ? Integer.parseInt(args[0]) : DEFAULT_SLOT_COUNT;
        int bucketsPerSlot = args.length >= 2 ? Integer.parseInt(args[1]) : KeyBucket.MAX_BUCKETS_PER_SLOT;
        int writeUpdates = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_WRITE_UPDATES;
        int rounds = args.length >= 4 ? Integer.parseInt(args[3]) : DEFAULT_ROUNDS;

        runScrapeScenario(slotCount, bucketsPerSlot, rounds);
        runWriteUpdateScenario(writeUpdates, rounds);
    }

    private static void runScrapeScenario(int slotCount, int bucketsPerSlot, int rounds) {
        long[] slotKeyCounts = new long[slotCount];
        fillSlotKeyCounts(slotKeyCounts);
        var observed = new ObservedBucketMetrics();
        observed.observedMaxKeyCount = 402;
        observed.observedMaxFillRate = 0.93;
        observed.fullRiskWriteCountTotal = 64;
        observed.splitToMaxCountTotal = 2703;
        observed.bucketFullExceptionCountTotal = 1;

        SimpleSnapshot last = null;
        for (int i = 0; i < 3; i++) {
            last = buildSimpleSnapshot(slotKeyCounts, bucketsPerSlot, observed);
        }

        long minNs = Long.MAX_VALUE;
        long maxNs = Long.MIN_VALUE;
        long totalNs = 0;
        for (int i = 0; i < rounds; i++) {
            long beginNs = System.nanoTime();
            last = buildSimpleSnapshot(slotKeyCounts, bucketsPerSlot, observed);
            long costNs = System.nanoTime() - beginNs;
            minNs = Math.min(minNs, costNs);
            maxNs = Math.max(maxNs, costNs);
            totalNs += costNs;
        }

        System.out.printf(
                "simple scrape calc: slots=%d, bucketsPerSlot=%d, rounds=%d, avg=%.6f ms, min=%.6f ms, max=%.6f ms%n",
                slotCount, bucketsPerSlot, rounds, nanosToMillis(totalNs / (double) rounds), nanosToMillis(minNs), nanosToMillis(maxNs));
        System.out.printf(
                "snapshot: persistKeyCount=%d, estimatedCapacity=%d, avgKeysPerBucket=%.6f, fillRate=%.9f, observedMaxKeyCount=%d, observedMaxFillRate=%.6f, fullRiskWrites=%d, splitToMax=%d, bucketFullExceptions=%d%n",
                last.persistKeyCount(), last.estimatedCapacity(), last.avgKeysPerBucket(), last.estimatedFillRate(),
                last.observedMaxKeyCount(), last.observedMaxFillRate(), last.fullRiskWriteCountTotal(),
                last.splitToMaxCountTotal(), last.bucketFullExceptionCountTotal());
    }

    private static void runWriteUpdateScenario(int writeUpdates, int rounds) {
        int[] keyCounts = new int[writeUpdates];
        int[] splitNumbers = new int[writeUpdates];
        boolean[] bucketFullExceptions = new boolean[writeUpdates];
        fillWriteUpdates(keyCounts, splitNumbers, bucketFullExceptions);

        long minNs = Long.MAX_VALUE;
        long maxNs = Long.MIN_VALUE;
        long totalNs = 0;
        ObservedBucketMetrics last = null;
        for (int round = 0; round < rounds; round++) {
            var observed = new ObservedBucketMetrics();
            long beginNs = System.nanoTime();
            for (int i = 0; i < writeUpdates; i++) {
                observed.onBucketWrite(keyCounts[i], splitNumbers[i], bucketFullExceptions[i]);
            }
            long costNs = System.nanoTime() - beginNs;
            minNs = Math.min(minNs, costNs);
            maxNs = Math.max(maxNs, costNs);
            totalNs += costNs;
            last = observed;
        }

        System.out.printf(
                "simple write-update calc: updates=%d, rounds=%d, avg=%.6f ms, min=%.6f ms, max=%.6f ms, avgPerUpdate=%.3f ns%n",
                writeUpdates, rounds, nanosToMillis(totalNs / (double) rounds), nanosToMillis(minNs), nanosToMillis(maxNs),
                totalNs / (double) rounds / writeUpdates);
        System.out.printf(
                "observed: maxKeyCount=%d, maxFillRate=%.6f, fullRiskWrites=%d, splitToMax=%d, bucketFullExceptions=%d%n",
                last.observedMaxKeyCount, last.observedMaxFillRate, last.fullRiskWriteCountTotal,
                last.splitToMaxCountTotal, last.bucketFullExceptionCountTotal);
    }

    private static SimpleSnapshot buildSimpleSnapshot(long[] slotKeyCounts, int bucketsPerSlot,
                                                      ObservedBucketMetrics observed) {
        long persistKeyCount = 0;
        for (long slotKeyCount : slotKeyCounts) {
            persistKeyCount += slotKeyCount;
        }

        long bucketCountTotal = (long) slotKeyCounts.length * bucketsPerSlot;
        long estimatedCapacity = bucketCountTotal * KeyBucket.INIT_CAPACITY;
        double avgKeysPerBucket = persistKeyCount / (double) bucketCountTotal;
        double estimatedFillRate = persistKeyCount / (double) estimatedCapacity;

        return new SimpleSnapshot(persistKeyCount, estimatedCapacity, avgKeysPerBucket, estimatedFillRate,
                observed.observedMaxKeyCount, observed.observedMaxFillRate, observed.fullRiskWriteCountTotal,
                observed.splitToMaxCountTotal, observed.bucketFullExceptionCountTotal);
    }

    private static void fillSlotKeyCounts(long[] slotKeyCounts) {
        for (int i = 0; i < slotKeyCounts.length; i++) {
            slotKeyCounts[i] = 300_000L + (i % 17) * 10_000L;
        }
    }

    private static void fillWriteUpdates(int[] keyCounts, int[] splitNumbers, boolean[] bucketFullExceptions) {
        Random random = new Random(1L);
        for (int i = 0; i < keyCounts.length; i++) {
            int splitNumber = i % 97 == 0 ? 9 : (i % 11 == 0 ? 3 : 1);
            int capacity = KeyBucket.INIT_CAPACITY * splitNumber;

            if (i % 4096 == 0) {
                keyCounts[i] = (int) Math.ceil(capacity * 0.93);
            } else if (i % 257 == 0) {
                keyCounts[i] = (int) Math.ceil(capacity * 0.72);
            } else {
                keyCounts[i] = random.nextInt(Math.max(1, capacity / 2));
            }
            splitNumbers[i] = splitNumber;
            bucketFullExceptions[i] = i % 100_000 == 0;
        }
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1_000_000.0;
    }
}
