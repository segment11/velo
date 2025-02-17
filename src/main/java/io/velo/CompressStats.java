package io.velo;

import io.velo.metric.SimpleGauge;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.List;

public class CompressStats {
    public long rawCount = 0;
    public long compressedCount = 0;
    public long rawTotalLength = 0;
    public long compressedTotalLength = 0;
    public long compressedCostTimeTotalUs = 0;
    public long decompressedCount = 0;
    public long decompressedCostTimeTotalNs = 0;

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

    @VisibleForTesting
    final static SimpleGauge compressStatsGauge = new SimpleGauge("compress_stats", "Net worker request handle compress stats.",
            "name");

    static {
        compressStatsGauge.register();
    }
}
