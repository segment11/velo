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

## Type Limits

Velo stores every value inside a `CompressedValue` envelope, and every container type encodes its
element count in a **16-bit (`short`) header field**. Both facts create hard limits that Redis does
not have. This section is the reference for them; the command-level guards are listed in
[Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md).

### Envelope Limits

| Limit | Constant | Default | Error reply |
|---|---|---|---|
| key length | [`CompressedValue.KEY_MAX_LENGTH`](/home/kerry/ws/velo/src/main/java/io/velo/CompressedValue.java) | 256 bytes | `ERR key too long (max length is 256)` |
| value length | [`CompressedValue.VALUE_MAX_LENGTH`](/home/kerry/ws/velo/src/main/java/io/velo/CompressedValue.java) | 1 MiB (1048576 bytes) | `ERR value too long (max length is 1048576)` |

Both are compile-time constants. Every command group checks the key length before it reads or
writes the key, and the value length before it writes the value.

### Container Element-Count Limits

The element count lives in a `short` header field, so a container can hold at most
`Short.MAX_VALUE` (32767) elements. Each type exposes its own ceiling, and the command layer
checks it **before** the write:

| Type | Constant | Default | Error reply |
|---|---|---|---|
| list | [`RedisList.LIST_MAX_SIZE`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisList.java) | 32767 | `ERR list size too long` |
| hash (both storage modes) | [`RedisHashKeys.HASH_MAX_SIZE`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHashKeys.java) | 4096 | `ERR hash size too long` |
| set (a set is stored as hash keys) | [`RedisHashKeys.HASH_MAX_SIZE`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHashKeys.java) | 4096 | `ERR set size too long` |
| sorted set | [`RedisZSet.ZSET_MAX_SIZE`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisZSet.java) | 4096 | `ERR zset size too long` |

`encode()` is the last line of defence and throws `IllegalStateException` when the size exceeds the
ceiling (`RedisList.java:131`, `RedisHashKeys.java:184`, `RedisHH.java:139`, `RedisZSet.java:326`,
`RedisGeo.java:324`). The command layer rejects an over-sized write first, so that exception is
only reached by a **multi-element** write that starts below the ceiling and crosses it — e.g.
`LPUSH key a b c` on a list that is one element short of the ceiling, because the list guard
compares the size *before* the push (`LGroup.addToList()`, `LGroup.java:365`). `RequestHandler`
converts the exception into an error reply (`RequestHandler.java:571`) and the write is not
persisted.

### Member-Length Limits

| Type | Constant | Default | Error reply |
|---|---|---|---|
| set member | [`RedisHashKeys.SET_MEMBER_MAX_LENGTH`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisHashKeys.java) | 255 bytes | `ERR set member length too long` |
| sorted set member | [`RedisZSet.ZSET_MEMBER_MAX_LENGTH`](/home/kerry/ws/velo/src/main/java/io/velo/type/RedisZSet.java) | 255 bytes | `ERR zset member length too long` |

A bloom filter capacity must be positive: `ERR capacity must be > 0`
(`ErrorReply.CAPACITY_NOT_POSITIVE`).

### The Limits Are Runtime Configurable

The five container limits are `DynConfig` items and can be changed at runtime per slot with
`CONFIG SET` (or `manage dyn-config`):

| Config key | Constant it writes |
|---|---|
| `type_list_max_size` | `RedisList.LIST_MAX_SIZE` |
| `type_hash_max_size` | `RedisHashKeys.HASH_MAX_SIZE` |
| `type_zset_max_size` | `RedisZSet.ZSET_MAX_SIZE` |
| `type_set_member_max_length` | `RedisHashKeys.SET_MEMBER_MAX_LENGTH` |
| `type_zset_member_max_length` | `RedisZSet.ZSET_MEMBER_MAX_LENGTH` |

Every value is validated by `parsePositiveShort` (`DynConfig.java:207`): a positive integer that is
at most `Short.MAX_VALUE`. The targets are `short` fields, so the ceiling can be lowered below
32767 but never raised above the format limit. See
[Configuration Design](/home/kerry/ws/velo/doc/design/08_configuration_design.md).

### Reporting A Velo Limit Instead Of The Redis Behaviour

**Reporting a Velo-specific limit instead of the Redis behaviour is a deliberate design decision.**
Redis has no element-count ceiling of this kind — `list-max-len` was removed in Redis 7.0 — and it
allows keys and values far larger than Velo does. So Redis answers `nil` for an out-of-range
`LINDEX` index and `index out of range` for `LSET`, while Velo reports the storage limit
explicitly, because a request beyond the limit can never be satisfied by a Velo value:

| request | Velo | Redis 7.2 |
|---|---|---|
| `LINDEX key 32767` | `ERR list size too long` | `nil` |
| `LSET key 32767 v` | `ERR list size too long` | `ERR index out of range` |
| `LPUSH key v` on a full list | `ERR list size too long` | the list grows |
| `SET key v` with a 300-byte key | `ERR key too long (max length is 256)` | accepted |

The reply texts come from the `ErrorReply` constants (`LIST_SIZE_TO_LONG`, `HASH_SIZE_TO_LONG`,
`SET_SIZE_TO_LONG`, `ZSET_SIZE_TO_LONG`, `SET_MEMBER_LENGTH_TO_LONG`, `ZSET_MEMBER_LENGTH_TO_LONG`,
`KEY_TOO_LONG`, `VALUE_TOO_LONG`, `CAPACITY_NOT_POSITIVE`). The decision to keep the `LINDEX`
behaviour is recorded in `doc/bug_reviews/bug_82_lgroup_commands_review_round_1.md` (finding 82.6,
reverted in `56fb3137`) — do not "align" these replies with Redis without a product decision.

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
