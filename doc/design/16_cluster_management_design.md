# Velo Cluster Management Design

## Overview

Cluster-related logic in the current codebase is split between:

- Java cluster metadata classes under [`io/velo/repl/cluster`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster)
- replication leadership under [`LeaderSelector`](/home/kerry/ws/velo/src/main/java/io/velo/repl/LeaderSelector.java)
- migration-oriented command code such as parts of [`MGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/MGroup.java)
- a small amount of operational Groovy scripting under `dyn/`

The older description of a Groovy-first `ClusterxCommand` implementation does not match this checkout.

## Cluster Metadata Model

The current Java model includes:

- [`MultiShard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/MultiShard.java)
- [`Shard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/Shard.java)
- [`Node`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/Node.java)
- [`SlotRange`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/SlotRange.java)

Cluster topology is read from the shared [`MultiShard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/MultiShard.java)
model owned by the persistence layer.

## Leadership

The active leadership mechanism is ZooKeeper-based and implemented by
[`LeaderSelector`](/home/kerry/ws/velo/src/main/java/io/velo/repl/LeaderSelector.java).

It provides:

- ZooKeeper connection management
- leader latch lifecycle
- self-leader detection
- lookup of the current master listen address when running as a follower

This is the concrete implementation behind current failover decisions.

## Data Movement

The codebase contains migration-style logic in command handlers, especially in
[`MGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/MGroup.java),
which connects to remote Redis-compatible nodes through Jedis and iterates keys by slot.

That is the currently visible implementation path for moving data between nodes in this repository.

## Groovy Control Scripts

There is a checked-in control script:

- `dyn/ctrl/FailoverManagerCtrl.groovy`

This supports the idea of operational scripting, but it is not the same as a large scripted cluster-command framework.

## Corrections To Older Versions

- There is no checked-in `src/main/groovy/io/velo/repl/cluster/` tree.
- There is no checked-in `dyn/src/io/velo/command/ClusterxCommand.groovy`.
- Cluster topology and leadership are primarily Java code today.

## Related Documents

- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Dynamic Groovy](/home/kerry/ws/velo/doc/design/15_dynamic_groovy_design.md)
