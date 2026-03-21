# Velo Multithreading Design

## Overview

The runtime uses several worker domains:

- a primary ActiveJ reactor
- net workers for socket ownership and protocol decoding
- slot workers for command execution
- index workers inside the persistence layer

This split is visible in [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java)
and [`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java).

## Net Workers

Net workers are ActiveJ worker reactors. They:

- own client sockets
- decode requests
- forward work toward slot-execution paths
- encode and write replies

The configured count is validated during bootstrap and published into `ConfForGlobal.netWorkers`.

## Slot Workers

Slot workers execute command logic. `RequestHandler` instances are created per slot worker and are marked
with `@ThreadNeedLocal` because they reuse mutable command-group instances.

This is the main execution domain for:

- slot parsing
- command dispatch
- persistence access
- request-local compression sample collection

## Index Workers

Index workers are separate from the net/slot ActiveJ worker pools.
They are created inside the persistence subsystem through `IndexHandlerPool` and are used for index-oriented
or background work.

## Thread-Local Design

Several key classes use `@ThreadNeedLocal`, including:

- `RequestHandler`
- `BaseCommand`
- `AclUsers`

The intended design is to keep mutable state thread-affine instead of protecting it with coarse locks.

## Cross-Thread Coordination

Cross-slot or cross-worker operations are handled with asynchronous dispatch and reply aggregation rather than
by sharing slot internals directly between threads. This pattern appears in command groups such as `MGroup`,
`PGroup`, and `SGroup`.

## Bootstrap Constraints

`MultiWorkerServer` validates worker counts against:

- configured slot count
- CPU count
- max supported worker constants

It also requires slot counts to divide cleanly by worker counts for the relevant pools.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
