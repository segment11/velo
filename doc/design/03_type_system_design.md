# Velo Type System Design

## Overview

Velo's type system is built around
[`CompressedValue`](/home/kerry/ws/velo/src/main/java/io/velo/CompressedValue.java).
This class is the common envelope for stored values and carries:

- sequence
- expiration
- key hash
- compressed payload bytes
- either a dictionary sequence or a special type marker

## Special Type Markers

The current implementation uses negative `dictSeqOrSpType` values to represent special storage modes.
Examples include:

- numeric encodings
- short string
- big string
- hash
- list
- set
- zset
- geo
- HyperLogLog
- bloom filter

This means "type" is encoded in storage metadata, not only in the Java class hierarchy.

The legacy `compress_value` notes are still directionally useful here: the stored envelope carries sequence/LSN-style identity,
expiration, key hash, compressed-size information, and the type-or-dictionary selector in one binary header.

## Core Container Types

The main explicit type implementations under `io.velo.type` are:

- [`RedisHH`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHH.java)
- [`RedisHashKeys`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHashKeys.java)
- [`RedisList`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisList.java)
- [`RedisZSet`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisZSet.java)
- [`RedisGeo`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisGeo.java)
- [`RedisBF`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisBF.java)

These classes own their own encode/decode logic and are used by the relevant command groups.

## Strings And Numbers

Simple string-like values do not always use a dedicated container class.
The implementation optimizes for:

- inline short strings
- numeric special encodings
- optional compression for larger strings
- big-string spill files when values exceed thresholds

Maximum key length is still constrained by `CompressedValue.KEY_MAX_LENGTH`, and value size limits are enforced in the storage layer.

## HyperLogLog

HyperLogLog support is real, but it is not represented by a separate dedicated type class in `io.velo.type`.
Instead, [`PGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/PGroup.java) stores HLL data using the
`SP_TYPE_HLL` marker on `CompressedValue`.

## GEO And Bloom Filter

GEO and bloom-filter support are both real command families:

- GEO commands live in [`GGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/GGroup.java)
- BF commands live in [`BGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/BGroup.java)

Their data representations are implemented in `RedisGeo` and `RedisBF`.

## Hash Tags And Slotting

Type handling is independent from slot routing, but commands rely on
[`BaseCommand.slot(...)`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java) to keep related keys
in the same internal slot when Redis hash tags are used.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Compression](/home/kerry/ws/velo/doc/design/07_compression_design.md)
- [RDB](/home/kerry/ws/velo/doc/design/14_rdb_format_design.md)
