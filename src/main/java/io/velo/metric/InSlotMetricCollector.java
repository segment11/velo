package io.velo.metric;

import java.util.Map;

/**
 * Interface for collecting metrics related to "In-Slot" operations.
 * Implementations of this interface are expected to provide a method
 * to collect and return metrics as a map of string keys to double values.
 */
public interface InSlotMetricCollector {

    /**
     * Collects and returns metrics related to "In-Slot" operations.
     * Each metric is represented as a key-value pair in the returned map,
     * where the key is a string and the value is a double.
     *
     * @return a map containing the collected metrics with string keys and double values
     */
    Map<String, Double> collect();
}