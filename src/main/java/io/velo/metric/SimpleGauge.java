package io.velo.metric;

import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleGauge extends Collector {
    public record ValueWithLabelValues(Double value, List<String> labelValues) {
    }

    public interface RawGetter {
        Map<String, ValueWithLabelValues> get();
    }

    private final ArrayList<RawGetter> rawGetterList = new ArrayList<>();

    public ArrayList<RawGetter> getRawGetterList() {
        return rawGetterList;
    }

    public void addRawGetter(RawGetter rawGetter) {
        rawGetterList.add(rawGetter);
    }

    private final Map<String, ValueWithLabelValues> gauges = new HashMap<>();

    private final List<String> labels;

    private final String familyName;

    private final String help;

    public SimpleGauge(String familyName, String help, String... labels) {
        this.familyName = familyName;
        this.help = help;
        this.labels = List.of(labels);
    }

    public void set(String name, double value, String... labelValues) {
        gauges.put(name, new ValueWithLabelValues(value, List.of(labelValues)));
    }

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
