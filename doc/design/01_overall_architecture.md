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
- Replication lives under [`io/velo/repl`](/home/kerry/ws/velo/src/main/java/io/velo/repl).
- ACL, metrics, configuration, and dictionary management are separate support subsystems.

## Request Flow

1. ActiveJ accepts a socket on the primary server and a net worker owns the connection.
2. [`RequestDecoder`](/home/kerry/ws/velo/src/main/java/io/velo/decode/RequestDecoder.java) detects HTTP, RESP, or `X-REPL`.
3. [`RequestHandler.parseSlots`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java) resolves the target slot list for the request.
4. The request is executed by a reused A-Z command-group instance inside a slot worker.
5. The command reads or writes a [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java).
6. The resulting [`Reply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/Reply.java) is serialized as RESP, RESP3, HTTP, or `X-REPL`.

## Storage Model

The storage design is slot-local. Each slot is represented by one [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) and owns:

- WAL state via [`Wal`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Wal.java)
- key-bucket and metadata loaders via [`KeyLoader`](/home/kerry/ws/velo/src/main/java/io/velo/persist/KeyLoader.java)
- chunk segment storage via [`Chunk`](/home/kerry/ws/velo/src/main/java/io/velo/persist/Chunk.java)
- big-string spill files via [`BigStringFiles`](/home/kerry/ws/velo/src/main/java/io/velo/persist/BigStringFiles.java)
- optional replication binlog via [`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java)
- per-slot dynamic configuration via [`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java)

`CompressedValue` is the common value envelope. It carries sequence, expiration, hash, and either
a dictionary sequence or a special type marker.

## Threading Model

- A primary ActiveJ reactor accepts connections.
- Net workers own sockets and decode requests.
- Slot workers own command execution and most mutable state.
- Index workers are created separately inside the persistence layer for background/index work.
- Many core classes are marked with `@ThreadNeedLocal` to make thread affinity explicit.

The code is optimized around keeping slot-local work on one thread and using event-loop handoff for
cross-slot or cross-worker cases.

## Replication And Cluster State

Replication is implemented in Java, not in Groovy command scripts. The relevant pieces are:

- wire format: [`Repl`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java)
- message taxonomy: [`ReplType`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplType.java)
- command entry point: [`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java)
- per-slot replication state: [`ReplPair`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplPair.java)
- ZooKeeper leadership: [`LeaderSelector`](/home/kerry/ws/velo/src/main/java/io/velo/repl/LeaderSelector.java)
- cluster metadata model: [`MultiShard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/MultiShard.java)

## Dynamic Groovy Support

The repository contains Groovy loading infrastructure in
[`CachedGroovyClassLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/CachedGroovyClassLoader.java) and
[`RefreshLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/RefreshLoader.java).

What exists today in-repo is smaller than earlier docs claimed:

- the runtime loader is configured from Java
- checked-in Groovy files are mostly under `dyn/`
- there is not a populated `src/main/groovy` command tree in this checkout

## Important Corrections To Older Docs

- Command groups are A-Z: there are 26 group classes, not 21.
- Cluster and replication control are primarily Java code under `io.velo.repl`, not a Groovy command framework.
- The design docs should not assume a populated `src/main/groovy` module because it is absent in this checkout.
- HTTP support is a thin request/reply adapter, not a separate REST application layer.

## Related Documents

- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
