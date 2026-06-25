package io.velo;

import io.velo.persist.KeyBucket;
import io.velo.persist.KeyLoader;

import java.util.Random;

/**
 * Measures the cost of a periodic, cached key-bucket metric snapshot.
 *
 * <p>This intentionally scans in-memory arrays only, matching the proposed use of
 * StatKeyCountInBuckets plus MetaKeyBucketSplitNumber cached bytes. It does not
 * read key-bucket files and does not sort.</p>
 */
public class TestKeyBucketMetricCalcCost {
    private static final int DEFAULT_ROUNDS = 5;

    record Snapshot(long keyCountTotal,
                    long capacityTotal,
                    double fillRate,
                    double keyCountAvg,
                    int keyCountMax,
                    double skewRatioMaxToAvg,
                    int nonEmptyCount,
                    int highFillCount,
                    int fullRiskCount,
                    int splitNumberMax,
                    double splitNumberAvg,
                    int maxSplitBucketCount) {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            int bucketsPerSlot = KeyBucket.MAX_BUCKETS_PER_SLOT;
            runScenario(1, bucketsPerSlot, DEFAULT_ROUNDS);
            runScenario(8, bucketsPerSlot, DEFAULT_ROUNDS);
            runScenario(64, bucketsPerSlot, DEFAULT_ROUNDS);
            System.out.println("Pass args to run one custom scenario: <slotCount> <bucketsPerSlot> <rounds>");
            System.out.println("Example worst-case CPU scan: 1024 " + KeyBucket.MAX_BUCKETS_PER_SLOT + " 3");
            return;
        }

        int slotCount = args.length >= 1 ? Integer.parseInt(args[0]) : 1;
        int bucketsPerSlot = args.length >= 2 ? Integer.parseInt(args[1]) : KeyBucket.MAX_BUCKETS_PER_SLOT;
        int rounds = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_ROUNDS;
        runScenario(slotCount, bucketsPerSlot, rounds);
    }

    private static void runScenario(int slotCount, int bucketsPerSlot, int rounds) {
        short[] keyCounts = new short[bucketsPerSlot];
        byte[] splitNumbers = new byte[bucketsPerSlot];
        fillTestData(keyCounts, splitNumbers);

        Snapshot last = null;
        for (int i = 0; i < 3; i++) {
            last = scanSlots(slotCount, keyCounts, splitNumbers);
        }

        long minNs = Long.MAX_VALUE;
        long maxNs = Long.MIN_VALUE;
        long totalNs = 0;
        for (int i = 0; i < rounds; i++) {
            long beginNs = System.nanoTime();
            last = scanSlots(slotCount, keyCounts, splitNumbers);
            long costNs = System.nanoTime() - beginNs;
            minNs = Math.min(minNs, costNs);
            maxNs = Math.max(maxNs, costNs);
            totalNs += costNs;
        }

        System.out.printf(
                "key bucket metric scan: slots=%d, bucketsPerSlot=%d, rounds=%d, avg=%.3f ms, min=%.3f ms, max=%.3f ms%n",
                slotCount, bucketsPerSlot, rounds, nanosToMillis(totalNs / (double) rounds), nanosToMillis(minNs), nanosToMillis(maxNs));
        System.out.printf(
                "snapshot: keyCountTotal=%d, capacityTotal=%d, fillRate=%.6f, avg=%.3f, max=%d, skew=%.3f, nonEmpty=%d, highFill=%d, fullRisk=%d, splitMax=%d, splitAvg=%.3f, maxSplitBuckets=%d%n",
                last.keyCountTotal(), last.capacityTotal(), last.fillRate(), last.keyCountAvg(), last.keyCountMax(),
                last.skewRatioMaxToAvg(), last.nonEmptyCount(), last.highFillCount(), last.fullRiskCount(),
                last.splitNumberMax(), last.splitNumberAvg(), last.maxSplitBucketCount());
    }

    private static Snapshot scanSlots(int slotCount, short[] keyCounts, byte[] splitNumbers) {
        long keyCountTotal = 0;
        long capacityTotal = 0;
        long splitNumberTotal = 0;
        int keyCountMax = 0;
        int nonEmptyCount = 0;
        int highFillCount = 0;
        int fullRiskCount = 0;
        int splitNumberMax = 0;
        int maxSplitBucketCount = 0;

        for (int slot = 0; slot < slotCount; slot++) {
            for (int i = 0; i < keyCounts.length; i++) {
                int keyCount = Short.toUnsignedInt(keyCounts[i]);
                int splitNumber = splitNumbers[i];
                int capacity = KeyBucket.INIT_CAPACITY * splitNumber;

                keyCountTotal += keyCount;
                capacityTotal += capacity;
                splitNumberTotal += splitNumber;

                if (keyCount > keyCountMax) {
                    keyCountMax = keyCount;
                }
                if (keyCount > 0) {
                    nonEmptyCount++;
                }
                if (keyCount * 10 >= capacity * 7) {
                    highFillCount++;
                }
                if (keyCount * 10 >= capacity * 9) {
                    fullRiskCount++;
                }
                if (splitNumber > splitNumberMax) {
                    splitNumberMax = splitNumber;
                }
                if (splitNumber == KeyLoader.MAX_SPLIT_NUMBER) {
                    maxSplitBucketCount++;
                }
            }
        }

        long bucketCountTotal = (long) slotCount * keyCounts.length;
        double keyCountAvg = keyCountTotal / (double) bucketCountTotal;
        double fillRate = capacityTotal == 0 ? 0 : keyCountTotal / (double) capacityTotal;
        double skewRatio = keyCountAvg == 0 ? 0 : keyCountMax / keyCountAvg;
        double splitNumberAvg = splitNumberTotal / (double) bucketCountTotal;

        return new Snapshot(keyCountTotal, capacityTotal, fillRate, keyCountAvg, keyCountMax, skewRatio,
                nonEmptyCount, highFillCount, fullRiskCount, splitNumberMax, splitNumberAvg, maxSplitBucketCount);
    }

    private static void fillTestData(short[] keyCounts, byte[] splitNumbers) {
        Random random = new Random(1L);
        for (int i = 0; i < keyCounts.length; i++) {
            int splitNumber = i % 97 == 0 ? KeyLoader.MAX_SPLIT_NUMBER : (i % 11 == 0 ? 3 : 1);
            int capacity = KeyBucket.INIT_CAPACITY * splitNumber;

            int keyCount;
            if (i % 4096 == 0) {
                keyCount = (int) Math.ceil(capacity * 0.93);
            } else if (i % 257 == 0) {
                keyCount = (int) Math.ceil(capacity * 0.72);
            } else if (random.nextInt(10) == 0) {
                keyCount = random.nextInt(Math.max(1, capacity / 2));
            } else {
                keyCount = 0;
            }

            keyCounts[i] = (short) keyCount;
            splitNumbers[i] = (byte) splitNumber;
        }
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1_000_000.0;
    }
}
