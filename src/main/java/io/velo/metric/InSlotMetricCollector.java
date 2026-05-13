package io.velo.metric;

import java.util.Map;

/**
 * Collects metrics for in-slot operations.
 */
public interface InSlotMetricCollector {

    /**
     * @return a map of metric names to their values
     */
    Map<String, Double> collect();
}