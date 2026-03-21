# Velo Configuration Design

## Overview

Configuration is split across three layers:

- global process-wide settings in [`ConfForGlobal`](/home/kerry/ws/velo/src/main/java/io/velo/ConfForGlobal.java)
- slot-level presets in [`ConfForSlot`](/home/kerry/ws/velo/src/main/java/io/velo/ConfForSlot.java)
- mutable per-slot runtime settings in [`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java)

Bootstrap validation and wiring happen inside
[`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java).

## Global Configuration

`ConfForGlobal` is a static holder. It includes:

- deployment identity such as datacenter and machine ID
- worker counts and slot count
- network listen address
- compression toggles and large-string thresholds
- replication and ZooKeeper settings
- ACL password bootstrap
- initial dynamic-config overrides

`checkIfValid()` performs lightweight validation and also seeds the default ACL user when a password is configured.

## Slot Configuration

[`ConfForSlot`](/home/kerry/ws/velo/src/main/java/io/velo/ConfForSlot.java) provides preset bundles:

- `debugMode`
- `c1m`
- `c10m`

Each preset carries nested configuration for:

- buckets
- chunks
- WAL
- replication compatibility
- LRU sizes

The active preset is exposed as `ConfForSlot.global`.

## Dynamic Configuration

[`DynConfig`](/home/kerry/ws/velo/src/main/java/io/velo/persist/DynConfig.java) is owned by each `OneSlot`.
It is used for settings that can change after startup, including binlog-related behavior and other
slot-scoped runtime switches.

`LocalPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()` also applies any entries from
`ConfForGlobal.initDynConfigItems`.

## Volume Layout

Volume-to-slot mapping is initialized by
[`ConfVolumeDirsForSlot`](/home/kerry/ws/velo/src/main/java/io/velo/ConfVolumeDirsForSlot.java).
This is the configuration layer that supports spreading slot data across configured directories.

## Validation In Bootstrap

`MultiWorkerServer` validates and publishes several key settings during startup:

- `slotNumber`
- `netWorkers`
- `slotWorkers`
- `indexWorkers`
- data directory and pid-file lock

The code also checks divisibility constraints between slot count and worker counts.

## Corrections To Older Versions

- The checked-in server uses `velo.properties` through ActiveJ config, but many runtime values are immediately copied into static holders.
- The dynamic-config layer is per slot and Java-based; it is not a general Groovy configuration system.

## Related Documents

- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
