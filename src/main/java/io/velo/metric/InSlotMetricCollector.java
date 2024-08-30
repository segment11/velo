package io.velo.metric;

import java.util.Map;

public interface InSlotMetricCollector {
    Map<String, Double> collect();
}
