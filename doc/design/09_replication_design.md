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

## 2N Scale-Up Slave Replication

A slave may have `slotNumber == master slotNumber * 2`. This is the "shadow scale-up" mode used for online capacity
growth (see [Scale-Up Design](/home/kerry/ws/velo/doc/design/18_scale_up_design.md)).

### Topology

- Master has `N` slots; slave has `2N` slots.
- Only stream slots `[0, N)` on the slave open a `ReplPair` and connect to the master (send `hello`).
- Extra slots `[N, 2N)` are reset as slave target slots (readonly, not readable, binlog off) but do **not** connect.
- Data from any master stream can fan out to any local slot because the non-cluster slot hash is doubling-hostile.

### Read visibility (global gate)

Because any local slot can hold keys from any master stream, read visibility is global: all local slots become
readable only when **every** stream slot is read-ready, and flip closed when any stream stops being read-ready.

This is implemented by a central, lock-serialized gate in `LocalPersist`:
- Each stream publishes its own readiness into `streamReadReady[streamSlot]`.
- The gate recomputes the AND and fans `canRead` to all `2N` local slots only when the AND changes.
- All under `scaleUpGateLock` so two streams cannot interleave fan-outs (no check-then-act race).
- A stream teardown (`removeReplPairAsSlave`) publishes `false` so the gate closes.

### Incremental replay ordering

In 2N mode, `s_catch_up` always routes through the async `applyAsync` path (`useAsync = hasAsyncApply ||
isAsSlaveScaleUp()`). `Promises.all` awaits all cross-slot writes before `finishSlaveCatchUpApply` advances the
persisted fetched-offset (await-before-advance). Per-key order is preserved by the target slot's FIFO eventloop.

### FLUSH handling

`FLUSHALL`/`FLUSHDB` produces `N` independent un-coordinated `XFlush` binlog entries. A lagging stream's flush
would wipe a leading stream's post-flush writes. For v1, `XFlush.apply`/`applyAsync` **throw** in scale-up mode
(stuck-but-safe): the stream stalls at that offset, the read gate stays closed, no data loss. A cross-stream flush
barrier is a future task.

### Promotion

`doResetAsMaster` promotes both stream slots (via the existing `removeReplPairAsSlave` → `resetAsMaster` path) and
extra slots (new: when `replPairAsSlave == null` and `isAsSlaveScaleUp()` and `slot >= masterSlotNumber`).

Known limitation: extra slots are promoted with data but no binlog history (their offset was zeroed at slave reset
and never advanced). Downstream replication from a promoted 2N node for `[N, 2N)` is not supported in v1.
