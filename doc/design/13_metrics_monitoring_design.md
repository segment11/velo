# Velo Metrics Monitoring Design

## Overview

Metrics are exposed through Prometheus collectors and a small HTTP management surface.
The key building blocks are:

- [`SimpleGauge`](/home/kerry/ws/velo/src/main/java/io/velo/metric/SimpleGauge.java)
- [`OneSlot.collect()`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)
- HTTP handling in [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)
- JVM collector registration in [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java)

## HTTP Endpoints

Current code paths document two important HTTP usages:

- `/?metrics` for Prometheus-format metrics
- `/?manage&slot&N&view-metrics` for per-slot metric inspection

These are management-style endpoints implemented through normal request handling, not a separate admin server.

The old metrics README also included real sample output. That remains useful context: the exposed metrics are a mix of
custom Velo gauges, request summaries, and JVM metrics registered into the default Prometheus registry.

## Collector Model

`SimpleGauge` is a custom Prometheus collector that supports:

- directly set gauges
- dynamically computed gauges through `RawGetter`

This pattern is reused by several subsystems to publish metrics without duplicating collector plumbing.

## Metric Sources

Important current metric producers include:

- [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) for global and slot metrics
- [`CompressStats`](/home/kerry/ws/velo/src/main/java/io/velo/CompressStats.java)
- [`DictMap`](/home/kerry/ws/velo/src/main/java/io/velo/DictMap.java)
- [`KeyAnalysisHandler`](/home/kerry/ws/velo/src/main/java/io/velo/persist/index/KeyAnalysisHandler.java)
- persistence-layer collectors such as `Chunk`, `SegmentBatch`, `FdReadWrite`, and `BigStringFiles`

## JVM Metrics

`MultiWorkerServer` registers standard JVM exporters with the default Prometheus registry:

- `StandardExports`
- `BufferPoolsExports`
- `MemoryPoolsExports`
- `GarbageCollectorExports`

## Scope Of Current Metrics

The metrics surface is broader than request counters alone. It includes:

- configuration and runtime environment values
- storage usage
- WAL and binlog positions
- LRU hit/miss statistics
- compression and dictionary metrics
- replication state

This breadth is the main reason the metrics surface should be documented separately from the pure architecture docs.

## Related Documents

- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Compression](/home/kerry/ws/velo/doc/design/07_compression_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
