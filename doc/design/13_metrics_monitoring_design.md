# Velo Metrics Monitoring Design

## Overview

Velo integrates with **Prometheus** for comprehensive observability at global and per-slot levels.

## Metrics Architecture

```
Request → RequestHandler → Update metrics → Prometheus Gauge
                │
                ├─> request_time (summary)
                ├─> compress_stats (gauge)
                ├─> dict (gauge)
                └─> custom metrics
```

## Global Metrics

### Endpoint: `/?metrics`

### Categories

**1. Request Time**
```
Metric: request_time
Type: Summary (count, sum, quantiles)
Labels: command (e.g., "get", "set", "config")

Example:
  request_time{command="set",quantile="0.5"} 0.00015
  request_time{command="set",quantile="0.99"} 0.00030
  request_time{command="set"}_count 1000000
  request_time{command="set"}_sum 150.0
```

**2. Compress Statistics**
```
Metric: compress_stats
Type: Gauge
Labels: name (SlotWorker ID)

Example:
  compress_stats{name="slot_worker_0",type="compressed_count"} 1000000
  compress_stats{name="slot_worker_0",type="compressed_total_length"} 30000000
  compress_stats{name="slot_worker_0",type="compressed_ratio"} 0.10
  compress_stats{name="slot_worker_0",type="compressed_cost_time_avg_us"} 3.5
```

**3. Dictionary Performance**
```
Metric: dict
Type: Gauge
Labels: dict_seq, name ("compressed_count", "compressed_ratio")

Example:
  dict_compressed_count_12345678{name="key:"} 500000
  dict_compressed_ratio_12345678{name="key:"} 0.15
```

**4. Global Configuration**
```
Metric: global
Type: Gauge
Labels: configuration parameter name

Example:
  global_slot_number 4
  global_slot_workers 4
  global_net_workers 2
  global_chunk_segment_length 4096
  global_estimate_key_number 10000000
```

**5. JVM Metrics (via Hotspot Collector)**
```
jvm_memory_bytes_used{area="heap"}
jvm_memory_bytes_used{area="nonheap"}  
jvm_gc_collection_seconds{gc="ZGC Minor Cycles"}
jvm_buffer_pool_used_bytes{pool="direct"}
```

## Per-Slot Metrics

### Endpoint: `/?manage&slot&{N}&view-metrics`

### Categories

**1. Persistence Metrics**
```
persist_key_count{slot="0"} 5000000
wal_key_count{slot="0"} 693946
chunk_current_segment_index{slot="0"} 991089
```

**2. File Descriptors**
```
fd_c_0_read_bytes_total{slot="0"} 300000000
fd_k_0_lru_hit_ratio{slot="0"} 0.95
fd_read_time_avg_us{slot="0"} 15.5
```

**3. LRU Metrics**
```
slot_kv_lru_hit_ratio{slot="0"} 0.42
slot_kv_lru_hit_total{slot="0"} 2000000
slot_kv_lru_miss_total{slot="0"} 2760000
```

**4. Compression Stats (per chunk)**
```
chunk_decompress_cost_time_avg_us{slot="0"} 8.5
chunk_cv_count_when_over_half_segment_count{slot="0"} 0
```

## Metric Collection

### SimpleGauge Implementation

```java
public class SimpleGauge extends Collector {
    private final String familyName;
    private final Map<String, ValueWithLabelValues> values = new HashMap<>();
    private final List<RawGetter> rawGetters = new ArrayList<>();

    public void set(String name, double value, String... labelValues) {
        values.put(name, new ValueWithLabelValues(value, Arrays.asList(labelValues)));
    }

    public void addRawGetter(RawGetter getter) {
        rawGetters.add(getter);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> samples = new ArrayList<>();

        // Add static values
        for (Map.Entry<String, ValueWithLabelValues> entry : values.entrySet()) {
            samples.add(createSample(entry.getKey(), entry.getValue()));
        }

        // Add dynamic values
        for (RawGetter getter : rawGetters) {
            samples.add(createFromRawGetter(getter));
        }

        return samples;
    }
}
```

### In-Slot Collection

```java
public interface InSlotMetricCollector {
    Map<String, Double> collect();
}

public class OneSlot implements InSlotMetricCollector {
    public Map<String, Double> collect() {
        Map<String, Double> metrics = new HashMap<>();

        // Key count
        metrics.put("persist_key_count", (double) persistKeyCount);

        // LRU hit rate
        metrics.put("slot_kv_lru_hit_ratio",
            lruHitTotal + lruMissTotal > 0 ?
            (double) lruHitTotal / (lruHitTotal + lruMissTotal) : 0.0);

        // Segment metrics
        metrics.put("chunk_current_segment_index", (double) chunk.getCurrentSegmentIndex().get());

        return metrics;
    }
}
```

## Performance Tracking

### Request Latency Distribution

```
Histogram buckets:
  10µs, 50µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s

Aggregation:
  count: Total requests
  sum: Total time (seconds)
  quantile[0.5, 0.90, 0.95, 0.99]: Requested percentiles
```

### Compression Tracking

```java
class CompressStats {
    long compressedCount;
    long compressedTotalLength;
    long rawTotalLength;
    long costTimeTotalUs;

    float getCompressionRatio() {
        return compressedTotalLength / rawTotalLength;
    }

    float getAvgCostUs() {
        return costTimeTotalUs / compressedCount;
    }
}
```

## Prometheus Configuration

### Scrape Configuration

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: velo
    metrics_path: /?metrics
    static_configs:
      - targets: ['127.0.0.1:7379']

  - job_name: velo-slots
    metrics_path: /?manage&slot&{slot}&view-metrics
    static_configs:
      - targets: ['127.0.0.1:7379']
```

### Grafana Dashboard

```
Dashboard: "Velo Overview"

Panels:
  - Request Rate (QPS) per command
  - Request Latency P99 per command
  - Compression Ratio per slot
  - Key Count per slot
  - LRU Hit Rate per slot
  - JVM Memory Usage
  - GC Pauses (ZGC)
```

## Related Documentation

-Existing: [doc/metrics/README.md](../doc/metrics/README.md)

## Key Source Files

- `src/main/java/io/velo/metric/SimpleGauge.java` - Prometheus collector
- `src/main/java/io/velo/persist/OneSlot.java` - Metric collection

---

**Version:** 1.0  
**Last Updated:** 2025-02-05  
