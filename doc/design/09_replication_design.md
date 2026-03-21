# Velo Replication Design

## Overview

Replication is implemented in Java under
[`src/main/java/io/velo/repl`](/home/kerry/ws/velo/src/main/java/io/velo/repl)
and the replication command entry point is
[`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java).

The current design combines:

- a custom `X-REPL` wire protocol
- per-slot replication state
- optional per-slot binlogs
- a fetch-existing-data phase followed by incremental catch-up
- ZooKeeper-based leadership decisions

## Wire Protocol

[`Repl`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java) encodes and decodes frames with:

- protocol marker `X-REPL`
- slave UUID
- slot
- [`ReplType`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplType.java)
- encoded content payload

The type enum distinguishes:

- handshake messages such as `hello`, `hi`, `ping`, `pong`
- fetch-existing-data requests and responses
- incremental catch-up traffic
- error and test messages

## Replication State

The key state holders are:

- [`ReplPair`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplPair.java)
- [`SlaveNeedReplay`](/home/kerry/ws/velo/src/main/java/io/velo/repl/SlaveNeedReplay.java)
- [`SlaveReplay`](/home/kerry/ws/velo/src/main/java/io/velo/repl/SlaveReplay.java)
- [`MasterReset`](/home/kerry/ws/velo/src/main/java/io/velo/repl/MasterReset.java)
- [`SlaveReset`](/home/kerry/ws/velo/src/main/java/io/velo/repl/SlaveReset.java)

`OneSlot.collect()` also exposes replication state as metrics, which is a useful reflection of the actual runtime model.

## Existing-Data Sync

Before pure incremental replay, the slave can fetch existing data in stages. The code includes explicit
message families for:

- existing WAL data
- existing chunk segments
- existing big strings
- existing short strings
- existing dictionaries
- completion markers for fetch-all

These flows are orchestrated inside [`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java),
not in an external replication daemon.

## Incremental Replay

Incremental state is represented as `X*` binlog content objects under
[`io/velo/repl/incremental`](/home/kerry/ws/velo/src/main/java/io/velo/repl/incremental).
Examples include:

- `XWalV`
- `XBigStrings`
- `XDict`
- `XDynConfig`
- `XAclUpdate`
- `XFlush`

These objects let the master append meaningful events to the binlog and let the slave replay them with
typed handling logic.

## Binlog

Per-slot binlog support is implemented by
[`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java).
`OneSlot` owns the binlog and exposes helper methods for appending content and for reset/reopen cases
during replication transitions.

Binlog usage depends on runtime configuration and role.

## Leadership And Failover

Leadership is handled by
[`LeaderSelector`](/home/kerry/ws/velo/src/main/java/io/velo/repl/LeaderSelector.java),
which uses Curator and ZooKeeper leader latches.

The current code supports:

- leader self-election when `canBeLeader` is true
- reading the leader listen address when running as a follower
- leader-latch start/stop during lifecycle changes

This is related to replication topology selection, not a full Redis Sentinel clone.

Historically the repo also carried separate overview diagrams for "slave sync and catch up" and "auto failover". Those diagrams
reflected the same two major ideas that still apply:

- replication has a staged bootstrap followed by incremental catch-up
- failover decisions are tied to leader/master discovery rather than simple local role toggles

## Corrections To Older Versions

- Replication is not primarily a Groovy feature.
- The current codebase has a richer fetch-existing-data phase than a simple handshake-plus-binlog description.
- Leader selection is ZooKeeper-based Java code, not a generic scripted failover manager.

## Related Documents

- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Cluster Management](/home/kerry/ws/velo/doc/design/16_cluster_management_design.md)
