package io.velo;

import io.velo.metric.SimpleGauge;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.List;

/**
 * Represents compression statistics for monitoring and analysis.
 * Tracks various metrics related to compression and decompression processes.
 */
public class CompressStats {
    /**
     * Total count of raw data items processed.
     */
    public long rawCount = 0;

    /**
     * Total count of compressed data items processed.
     */
    public long compressedCount = 0;

    /**
     * Total length of raw data in bytes.
     */
    public long rawTotalLength = 0;

    /**
     * Total length of compressed data in bytes.
     */
    public long compressedTotalLength = 0;

    /**
     * Total time spent on compression in microseconds.
     */
    public long compressedCostTimeTotalUs = 0;

    /**
     * Total count of decompressed data items processed.
     */
    public long decompressedCount = 0;

    /**
     * Total time spent on decompression in nanoseconds.
     */
    public long decompressedCostTimeTotalNs = 0;

    /**
     * Initializes a new instance of the CompressStats class with the specified name and prefix.
     *
     * @param name   the name of the statistics instance
     * @param prefix the prefix used for metric labels
     */
    public CompressStats(String name, String prefix) {
        compressStatsGauge.addRawGetter(() -> {
            var labelValues = List.of(name);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            if (compressedCount > 0) {
                map.put(prefix + "raw_count", new SimpleGauge.ValueWithLabelValues((double) rawCount, labelValues));
                map.put(prefix + "compressed_count", new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValues));
                map.put(prefix + "compressed_cost_time_total_ms", new SimpleGauge.ValueWithLabelValues((double) compressedCostTimeTotalUs / 1000, labelValues));
                double costTAvg = (double) compressedCostTimeTotalUs / compressedCount;
                map.put(prefix + "compressed_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(costTAvg, labelValues));

                map.put(prefix + "raw_total_length", new SimpleGauge.ValueWithLabelValues((double) rawTotalLength, labelValues));
                map.put(prefix + "compressed_total_length", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength, labelValues));
                map.put(prefix + "compression_ratio", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength / rawTotalLength, labelValues));
            }

            if (decompressedCount > 0) {
                map.put(prefix + "decompressed_count", new SimpleGauge.ValueWithLabelValues((double) decompressedCount, labelValues));
                map.put(prefix + "decompressed_cost_time_total_ms", new SimpleGauge.ValueWithLabelValues((double) decompressedCostTimeTotalNs / 1000 / 1000, labelValues));
                double decompressedCostTAvg = (double) decompressedCostTimeTotalNs / decompressedCount;
                map.put(prefix + "decompressed_cost_time_avg_ns", new SimpleGauge.ValueWithLabelValues(decompressedCostTAvg, labelValues));
            }

            return map;
        });
    }

    /**
     * Static gauge for compress stats.
     */
    @VisibleForTesting
    final static SimpleGauge compressStatsGauge = new SimpleGauge("compress_stats", "Net worker request handle compress stats.",
            "name");

    static {
        compressStatsGauge.register();
    }
}
