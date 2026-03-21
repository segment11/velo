# Velo Compression Design

## Overview

Compression is split across two layers:

- value-envelope compression through [`CompressedValue`](/home/kerry/ws/velo/src/main/java/io/velo/CompressedValue.java)
- container-specific compression inside type classes such as `RedisHH`, `RedisList`, and `RedisZSet`

Zstd is the primary compression engine in the current code.

## CompressedValue

`CompressedValue` stores:

- metadata header fields
- compressed or raw payload bytes
- a dictionary sequence or special type marker

It is used by command handlers and persistence code as the common transport/storage format for values.

## Dictionary Management

The dictionary registry lives in [`DictMap`](/home/kerry/ws/velo/src/main/java/io/velo/DictMap.java).
Important current behaviors:

- dictionaries are keyed both by sequence and by key prefix/suffix grouping
- dictionaries are persisted to `dict-map.dat`
- dictionary updates can be appended to binlog through `XDict`
- dictionary metrics are exported through `SimpleGauge`

## Training Path

Training support is wired through:

- [`TrainSampleJob`](/home/kerry/ws/velo/src/main/java/io/velo/TrainSampleJob.java)
- per-request sample collection in [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)

This is why compression is partly a request-handling concern and not only a persistence concern.

## Type-Level Compression

Several container types encode themselves with optional dictionary compression:

- [`RedisHH`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHH.java)
- [`RedisHashKeys`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHashKeys.java)
- [`RedisList`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisList.java)
- [`RedisZSet`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisZSet.java)

These classes decide whether compression is worthwhile based on body length and compression ratio.

## Large String Handling

Large values are special-cased:

- `bigStringNoMemoryCopySize` controls a decoding optimization
- `bigStringNoCompressMinSize` avoids expensive compression for very large values
- the persistence layer can spill oversized values into `BigStringFiles`

Compression design therefore interacts with both decode-time and persistence-time logic.

## Metrics

Compression statistics are exported by:

- [`CompressStats`](/home/kerry/ws/velo/src/main/java/io/velo/CompressStats.java)
- [`DictMap`](/home/kerry/ws/velo/src/main/java/io/velo/DictMap.java)
- persistence-layer collectors such as `Chunk`, `FdReadWrite`, and `SegmentBatch`

## Related Documents

- [Type System](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Metrics](/home/kerry/ws/velo/doc/design/13_metrics_monitoring_design.md)
