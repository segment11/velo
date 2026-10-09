# Velo Overall Architecture Design

## Overview

This document describes the architecture that is present in the current codebase, centered on
[`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java),
[`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java), and
[`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java).

Velo is a Java server built on ActiveJ. It accepts Redis RESP traffic, a small HTTP control surface,
and the internal `X-REPL` replication protocol. Data is partitioned into slots, stored in per-slot
`OneSlot` instances, and exposed through Redis-like command groups.

## Main Runtime Pieces

- Server bootstrap and worker orchestration live in [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java).
- Request decoding lives in [`RequestDecoder`](/home/kerry/ws/velo/src/main/java/io/velo/decode/RequestDecoder.java).
- Command dispatch lives in [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java).
- Shared command helpers live in [`BaseCommand`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java).
- Persistence entry points live in [`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java) and [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java).
- Replication lives under `io/velo/repl`.
- ACL, metrics, configuration, and dictionary management are separate support subsystems.

## Request Flow

A request crosses two execution stages: the **net worker** owns the socket, and the **slot worker** owns
command execution. Concretely:

1. An ActiveJ socket is accepted and a net worker (`net-worker-<id>`) owns the connection.
2. `RequestDecoder.tryDecode` detects HTTP, RESP, or `X-REPL` from the first bytes, and produces a **pipeline**
   of `Request` objects — not a single request.
3. `MultiWorkerServer.handlePipeline` calls `RequestHandler.parseSlots(...)` for every request in the pipeline.
   **This runs on the net worker thread**, using the command groups owned by `requestHandlerArray[0]`. It is safe
   there only because slot parsing is pure computation — it resolves slot numbers and key hashes and does not
   touch slot data.
4. The pipeline is split at connection-state commands (`auth`, `hello`, `readonly`, `readwrite`), which act as
   barriers whose replies must be awaited before later requests run.
5. `MultiWorkerServer.handleRequest` applies the net-worker-side gates in order: replication, `quit`, the auth
   gate, the ACL check, `shutdown`, the `echo`/`ping` fast path, the readonly-connection check, the cluster `MOVED`
   check, and cross-worker detection.
6. The request is handed to a slot worker: `slotWorkerEventloopArray[slot % workerCount].submit(...)`, running
   `RequestHandler.handle`. Slotless commands (`ping`, `echo`, `hello`, `auth`) get a random worker.
7. `RequestHandler.handle` short-circuits `GET` (arity 2) and `SET` (exactly 3 args), then dispatches by first
   command byte into a reused A-Z command-group instance via `resetContext(...)` + `handle()`.
8. The command reads or writes a [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java),
   returning a [`Reply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/Reply.java).
9. **Back on the net worker**, the reply is serialized as HTTP, RESP3, or RESP2, and coalesced into one `ByteBuf`
   per pipeline, which ActiveJ streams to the socket.

Two consequences worth internalizing:

- **Command execution happens on the slot worker, not the net worker.** The single exception is
  `slotWorkers == 1 && netWorkers == 1`, where `isReuseNetWorkerEventloop` makes `handle` run inline.
- **Reply encoding happens on the net worker**, not inside command code. This is why commands return `Reply`
  objects instead of touching sockets.

## Storage Model

The storage design is slot-local. Each slot is represented by one [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) and owns:

- WAL state via a `Wal[]` array — one `Wal` per WAL group, all sharing two files (`wal.dat` for long values,
  `wal-short-value.dat` for short values)
- key-bucket and metadata loaders via [`KeyLoader`](/home/kerry/ws/velo/src/main/java/io/velo/persist/KeyLoader.java)
- chunk segment storage via [`Chunk`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Chunk.java)
- big-string spill files via [`BigStringFiles`](/home/kerry/ws/velo/src/main/java/io/velo/persist/BigStringFiles.java)
- optional replication binlog via [`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java)
- per-slot dynamic configuration via [`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java)
- a per-WAL-group KV LRU (`kvByWalGroupIndexLRU`)

`CompressedValue` is the common value envelope. It carries sequence, expiration, hash, and either
a dictionary sequence or a special type marker.

A write appends to the WAL first in every case; the storage class (short value / chunk / big string) only decides
where the WAL group is later flushed to. A read checks WAL, then the WAL-group LRU, then the key bucket, then either
returns the short value directly or follows the 12-byte `PersistValueMeta` pointer into a chunk segment. Big-string
payload bytes are *not* read by `OneSlot.get`; they are resolved one layer up in `BaseCommand.getCv`. See
[Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md) for the exact thresholds.

## Threading Model

- A primary ActiveJ reactor accepts connections.
- Net workers own sockets, decode requests, apply gates, encode replies, and write them.
- Slot workers own command execution and all slot-owned persistence state.
- Index workers are created separately inside the persistence layer for background/index work.
- Many core classes are marked with `@ThreadNeedLocal` to make thread affinity explicit.

The code is optimized around keeping slot-local work on one thread and using event-loop handoff for
cross-slot or cross-worker cases. `OneSlot` enforces this at runtime via its `threadIdProtectedForSafe` guard.
See [Multithreading](/home/kerry/ws/velo/doc/design/11_multithreading_design.md).

## Replication And Cluster State

Replication is implemented in Java, not in Groovy command scripts. The relevant pieces are:

- wire format: [`Repl`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java)
- message taxonomy: [`ReplType`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplType.java)
- command entry point: [`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java)
- per-slot replication state: [`ReplPair`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplPair.java)
- ZooKeeper leadership: [`LeaderSelector`](/home/kerry/ws/velo/src/main/java/io/velo/repl/LeaderSelector.java)
- cluster metadata model: [`MultiShard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/MultiShard.java)

## Dynamic Groovy Support

Groovy is used in three distinct places, which are easy to conflate:

1. **Hot-reloaded manage/cluster commands** live under `dyn/src/io/velo/command/` and are loaded through
   [`CachedGroovyClassLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/CachedGroovyClassLoader.java) and
   [`RefreshLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/RefreshLoader.java):
   `ClusterxCommand`, `CommandCommand`, `ConfigCommand`, `ExtendCommand`, `InfoCommand`, `ManageCommand`.
2. **Cluster watch / failover logic** lives under `src/main/groovy/io/velo/repl/cluster/watch/`:
   `FailoverManager.groovy`, `Guarder.groovy`, `OneClusterMeta.groovy`, `OneEndpointStatus.groovy`,
   `TmpEndpointsStatus.groovy`.
3. **Test server helpers** live under `src/main/groovy/io/velo/test/tools/`: `RedisServer.groovy`,
   `VeloServer.groovy`.

Redis command semantics themselves are implemented in Java under `io.velo.command`; the Groovy tree is for cluster
management and operational tooling, not a general command framework.

## Notes For Contributors

- Command groups are A-Z: there are 26 group classes, `AGroup` through `ZGroup`, indexed by
  `firstByte - 'a'`.
- `SlotWithKeyHash` is a nested record in `BaseCommand`, not a top-level source file.
- Several groups that once looked empty now contain handlers or utility entry points; do not treat them as
  placeholders without checking code.
- HTTP support is a thin request/reply adapter, not a separate REST application layer. `Reply.bufferAsHttp()`
  defaults to `null` and is overridden in only seven reply classes.
- Most `velo.properties` tuning keys ship commented out, so the code defaults in `ConfForSlot` are what actually
  run unless overridden.

## Related Documents

- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Multithreading](/home/kerry/ws/velo/doc/design/11_multithreading_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
