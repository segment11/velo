package io.velo.metric;

import io.prometheus.client.Collector;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Prometheus gauge collector with support for raw getters.
 */
public class SimpleGauge extends Collector {

    /**
     * @param value       the gauge value
     * @param labelValues the label values for this gauge
     */
    public record ValueWithLabelValues(Double value, List<String> labelValues) {
    }

    /**
     * Provides gauge values on demand.
     */
    public interface RawGetter {
        /**
         * @return a map of gauge names to their values and labels
         */
        Map<String, ValueWithLabelValues> get();
    }

    private final CopyOnWriteArrayList<RawGetter> rawGetterList = new CopyOnWriteArrayList<>();

    /**
     * @return the list of raw getters (testing only)
     */
    @TestOnly
    public List<RawGetter> getRawGetterList() {
        return rawGetterList;
    }

    /**
     * @param rawGetter the raw getter to add
     */
    public void addRawGetter(RawGetter rawGetter) {
        rawGetterList.add(rawGetter);
    }

    /** Clears all registered raw getters. */
    public void clearRawGetterList() {
        rawGetterList.clear();
    }

    private final Map<String, ValueWithLabelValues> gauges = new ConcurrentHashMap<>();

    private final List<String> labels;

    private final String familyName;

    private final String help;

    /**
     * Constructs a new SimpleGauge instance.
     *
     * @param familyName the name of the gauge family.
     * @param help       the help string describing the gauge.
     * @param labels     the label names for this gauge.
     */
    public SimpleGauge(String familyName, String help, String... labels) {
        this.familyName = familyName;
        this.help = help;
        this.labels = List.of(labels);
    }

    /**
     * Sets the value of a gauge with a given name and label values.
     *
     * @param name        the name of the gauge.
     * @param value       the value to set.
     * @param labelValues the label values for the gauge.
     */
    public void set(String name, double value, String... labelValues) {
        gauges.put(name, new ValueWithLabelValues(value, List.of(labelValues)));
    }

    /**
     * Collects the metrics from this gauge and any added RawGetters.
     *
     * @return a list of MetricFamilySamples representing the collected metrics.
     */
    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> list = new ArrayList<>();

        List<MetricFamilySamples.Sample> samples = new ArrayList<>();
        var dsMfs = new MetricFamilySamples(familyName, Type.GAUGE, help, samples);
        list.add(dsMfs);

        for (var entry : gauges.entrySet()) {
            var entryValue = entry.getValue();
            samples.add(new MetricFamilySamples.Sample(entry.getKey(), labels, entryValue.labelValues, entryValue.value));
        }

        for (var rawGetter : rawGetterList) {
            var raw = rawGetter.get();
            if (raw == null) {
                continue;
            }

            for (var entry : raw.entrySet()) {
                var entryValue = entry.getValue();
                samples.add(new MetricFamilySamples.Sample(entry.getKey(), labels, entryValue.labelValues, entryValue.value));
            }
        }

        return list;
    }
}