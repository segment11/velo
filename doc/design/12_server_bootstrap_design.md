# Velo Server Bootstrap Design

## Overview

Server bootstrap is implemented by
[`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java),
an ActiveJ `Launcher`.

This class is responsible for validating configuration, creating worker pools, initializing persistence,
and starting the network listeners.

## Bootstrap Sequence

The current startup path includes these major steps:

1. load ActiveJ config from `velo.properties`
2. validate worker counts, slot count, and directory settings
3. create and lock the pid/data directory
4. initialize dictionary management and persistence
5. create request handlers and task runnables
6. start slot-worker event loops
7. initialize ACL users from file
8. register JVM Prometheus collectors
9. start ActiveJ network servers
10. start replication/leadership helpers as needed

## Persistence Initialization

Bootstrap calls into:

- [`DictMap`](/home/kerry/ws/velo/src/main/java/io/velo/DictMap.java)
- [`LocalPersist.initSlots(...)`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java)

This means storage and dictionary state are ready before normal request traffic.

## Worker Initialization

`onStart()` creates and starts slot-worker event loops and records their thread IDs.
Net workers are provided through ActiveJ worker-pool bindings.

ACL initialization also depends on the slot-worker event-loop array, which is why ACL setup happens after
those loops exist.

## Dynamic Groovy Wiring

Bootstrap creates a `RefreshLoader` around the cached Groovy class loader and points it at `dyn` source paths.
This is infrastructure wiring, not evidence of a large Groovy application module in this checkout.

## Shutdown

The server owns cleanup for:

- leader selector
- Jedis pool holder
- local persistence
- dictionary map
- pid-file lock resources

The cleanup path is centralized in `run()`/lifecycle handling rather than scattered across commands.

## Related Documents

- [Configuration](/home/kerry/ws/velo/doc/design/08_configuration_design.md)
- [Multithreading](/home/kerry/ws/velo/doc/design/11_multithreading_design.md)
- [Dynamic Groovy](/home/kerry/ws/velo/doc/design/15_dynamic_groovy_design.md)
