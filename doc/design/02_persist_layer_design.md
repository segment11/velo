# Velo Persistence Layer Design

## Overview

Persistence is managed by [`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java),
which owns an array of per-slot [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) instances.

This is the real storage boundary in the current codebase.

## Main Components

Each `OneSlot` composes several storage structures:

- [`Wal`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Wal.java) for recent writes and delayed persistence
- [`KeyLoader`](/home/kerry/ws/velo/src/main/java/io/velo/persist/KeyLoader.java) for key buckets and metadata lookup
- [`Chunk`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Chunk.java) for persisted value segments
- [`BigStringFiles`](/home/kerry/ws/velo/src/main/java/io/velo/persist/BigStringFiles.java) for oversized values
- [`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java) for slot-local mutable settings
- [`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java) when replication/binlog is enabled

## Additional Slot-Storage Details

The legacy `hash_buckets` notes captured a few implementation-oriented details that are still worth keeping:

- key-bucket layout is sized from slot configuration rather than resized dynamically at runtime in a generic hash-table style
- bucket entries track key bytes, key hash, expiration, and value location metadata
- some short values can stay close to bucket-oriented storage instead of always going through chunk-segment persistence
- bucket splitting exists, but it is a bounded mechanism and is treated as something to minimize rather than a primary growth strategy

These details match the current code structure in `KeyBucket`, `KeyLoader`, and `OneSlot`, even though exact capacities are configuration-dependent.

## Slot Initialization

`LocalPersist.initSlots(...)` creates one `OneSlot` per configured slot and calls `initFds()` on each one.
Slot data is laid out under a `persist/slot-N` directory tree beneath the configured data directory.

`LocalPersist` also initializes:

- volume mapping through `ConfVolumeDirsForSlot`
- `MultiShard` cluster metadata
- index-worker infrastructure through `IndexHandlerPool`

## Write Path

At a high level, writes go through `OneSlot.put(...)`.
The code distinguishes several storage cases:

- short values can stay in key-bucket-oriented storage
- larger values are written through WAL and then persisted into chunk segments
- very large values can be spilled into big-string files

`OneSlot` also groups temporary write state by WAL group. Older docs described this as "16 or 32 key buckets in one wal group";
the current code should be treated as configuration-driven, but the important architectural point remains:

- writes are accumulated in WAL-group-local structures
- persistence and merge work is often scheduled around WAL-group boundaries
- LRU caches are also maintained per WAL-group path

The current implementation is therefore more nuanced than "everything goes straight to chunks".

## Read Path

Reads start from `OneSlot.get(...)`.
The implementation checks:

- WAL-delayed values
- slot-local LRU caches
- persisted metadata via `KeyLoader`
- chunk segments or big-string files as needed

This explains why the persistence design mixes WAL, key metadata, chunk data, and specialized file paths.

## Merge And Cleanup Work

`OneSlot` also owns background-style persistence maintenance:

- WAL lazy load on startup
- merge of persisted segments
- expired/deleted value cleanup
- overwrite cleanup for big-string files
- binlog reset/reopen helpers for replication role changes

Before flushing new data, `OneSlot` can pre-read related persisted segments in the same WAL-group region and merge valid values
forward. That is still the right high-level explanation for the segment-merge behavior.

## LRU Layers

The persistence layer uses more than one cache/LRU surface:

- latest key-to-compressed-value reads, including big-string cases
- cached key-bucket bytes
- optional chunk-segment related cached reads

The exact limits are configuration-driven, but the layered-cache design remains part of the storage architecture.

These behaviors are implementation reality and should be documented as part of the storage design.

## Metrics

`OneSlot` implements [`InSlotMetricCollector`](/home/kerry/ws/velo/src/main/java/io/velo/metric/InSlotMetricCollector.java).
Its `collect()` method exports persistence metrics covering WAL, key loader, chunks, big strings, LRU, and replication.

## Related Documents

- [Compression](/home/kerry/ws/velo/doc/design/07_compression_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Metrics](/home/kerry/ws/velo/doc/design/13_metrics_monitoring_design.md)
