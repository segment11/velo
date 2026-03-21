# Velo Command Processing Design

## Overview

Command processing is driven by [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)
and the A-Z command-group classes under
[`src/main/java/io/velo/command`](/home/kerry/ws/velo/src/main/java/io/velo/command).

The current implementation uses 26 command groups, one per initial letter:
`AGroup` through `ZGroup`.

## Dispatch Model

[`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java) creates and reuses one
instance of each command group per slot-worker thread. For each request it:

1. normalizes special cases such as `ping`, `echo`, `quit`, `hello`, and `auth`
2. parses slot information for key-bearing commands
3. chooses the command group by the first command byte
4. resets the selected command instance with the current request context
5. executes `handle()`

This reuse pattern is the reason many command classes keep mutable fields and rely on thread affinity.

## BaseCommand Contract

[`BaseCommand`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java) defines the shared contract:

- `parseSlots(String cmd, byte[][] data, int slotNumber)`
- `handle()`
- `resetContext(...)`
- shared helpers for slot hashing, auth lookup, type checks, common get/set helpers, and mock helpers for tests

The slot resolution helper implements Redis-style hash-tag handling and uses
`JedisClusterCRC16` for the client-visible cluster slot while keeping Velo's internal slot number separately.

## Slot Parsing

Commands describe their key positions explicitly in `parseSlots(...)`. Common patterns include:

- first argument is the key
- alternating key/value arguments
- source and destination keys for copy/move/store commands
- replication requests handled through `XGroup`

Multi-key commands usually group keys by internal slot before executing fan-out work.

## Cross-Slot Behavior

The codebase does support cross-slot operations in selected commands. Typical implementations:

- group input keys by `slotWithKeyHash.slot()`
- submit work to the relevant slot worker or `OneSlot`
- aggregate replies back into one `Reply`

Examples appear in:

- [`MGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/MGroup.java) for `mget`/`mset` variants
- [`PGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/PGroup.java) for `pfcount` and `pfmerge`
- [`SGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/SGroup.java) for set operations

## Reply Modes

Client reply mode is not globally fixed. Command handling can return replies that are later encoded as:

- RESP2 through `Reply.buffer()`
- RESP3 through `Reply.bufferAsResp3()`
- HTTP through `Reply.bufferAsHttp()`

This is why command code generally returns reply objects rather than writing directly to sockets.

## Special Command Families

- ACL and auth flow live mainly in [`AGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/AGroup.java).
- Bloom filter commands live in [`BGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/BGroup.java).
- GEO commands live in [`GGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/GGroup.java).
- HyperLogLog commands live in [`PGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/PGroup.java).
- Replication protocol handling lives in [`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java).

## Corrections To Older Versions

- The codebase has 26 command group classes, not 21.
- `SlotWithKeyHash` is defined inside [`BaseCommand`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java), not as a separate top-level source file.
- Several groups once described as empty now contain handlers or utility entry points and should not be treated as placeholders without checking code.

## Related Documents

- [Type System](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Response Encoding](/home/kerry/ws/velo/doc/design/06_response_encoding_design.md)
