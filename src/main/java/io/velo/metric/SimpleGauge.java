package io.velo.metric;

import io.prometheus.client.Collector;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple gauge implementation for Prometheus metric collection.
 * This class extends Prometheus's Collector and allows setting metrics values
 * either directly or through user-provided RawGetters.
 */
public class SimpleGauge extends Collector {

    /**
     * A record to hold a gauge value along with its label values.
     */
    public record ValueWithLabelValues(Double value, List<String> labelValues) {
    }

    /**
     * An interface for classes that can provide a map of gauge names to their values and label values.
     */
    public interface RawGetter {
        /**
         * Returns a map of gauge names to their corresponding values and label values.
         *
         * @return a map with gauge names as keys and ValueWithLabelValues as values.
         */
        Map<String, ValueWithLabelValues> get();
    }

    private final ArrayList<RawGetter> rawGetterList = new ArrayList<>();

    /**
     * Returns the list of raw getters. This method is for testing purposes only.
     *
     * @return the list of raw getters.
     */
    @TestOnly
    public ArrayList<RawGetter> getRawGetterList() {
        return rawGetterList;
    }

    /**
     * Adds a RawGetter to the list of raw getters.
     *
     * @param rawGetter the RawGetter to add.
     */
    public void addRawGetter(RawGetter rawGetter) {
        rawGetterList.add(rawGetter);
    }

    /**
     * Clears the list of raw getters.
     */
    public void clearRawGetterList() {
        rawGetterList.clear();
    }

    private final Map<String, ValueWithLabelValues> gauges = new HashMap<>();

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