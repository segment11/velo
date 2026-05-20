# Velo Multithreading Design

## Overview

The runtime uses several worker domains:

- a primary ActiveJ reactor
- net workers for socket ownership, request decoding, and reply writing
- slot workers for command execution and slot-owned persistence access
- index workers inside the persistence layer

This split is visible in [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java)
and [`LocalPersist`](/home/kerry/ws/velo/src/main/java/io/velo/persist/LocalPersist.java).

The current design is mostly thread-affine: mutable slot state is owned by one slot worker and cross-thread work is
scheduled onto the owner eventloop. A few shared structures intentionally use immutable snapshots, concurrent maps, or
atomic counters instead of per-thread copies.

## Worker Domains

### Primary Reactor

The primary reactor owns bootstrap and shutdown coordination. It creates worker eventloops, initializes shared
singletons, and stops the server.

Production shutdown must not directly close slot-owned persistence from the primary thread. `MultiWorkerServer.onStop()`
stops request/scheduled work first, then calls `LocalPersist.cleanUpAsync()` and waits for it before breaking slot
eventloops. `LocalPersist.cleanUpAsync()` submits each `OneSlot.cleanUp()` through `OneSlot.asyncCall()`, so cleanup
runs on the owning slot worker. The synchronous `LocalPersist.cleanUp()` path is marked test-only because it rewrites
the slot thread guard and should not be used as production cleanup.

### Net Workers

Net workers are ActiveJ worker reactors. They:

- own client sockets
- decode RESP, HTTP, and replication requests
- perform request gating that can be decided before slot execution
- forward requests toward slot-execution paths
- encode and write replies

Socket state lives on the socket user-data object (`VeloUserDataInSocket`) and is accessed through `SocketInspector`.
Most socket fields are expected to be touched by the owning net worker. Some state can be read or updated from command
code running on slot workers; current code accepts that `isResp3` and `replyMode` can change during a pipeline and that
some replies in that pipeline may use the old mode. This is an accepted compatibility/performance tradeoff, not a
strict cross-thread synchronization guarantee.

Connection admission and socket maps are shared across workers through concurrent/atomic state in `SocketInspector`.
`SocketInspector.clearAll()` must reset both the socket map and admission counters so later max-connection checks do
not drift.

### Slot Workers

Slot workers execute command logic. `RequestHandler` instances are created per slot worker and are marked
with `@ThreadNeedLocal` because they reuse mutable command-group instances and request-local helper state.

This is the main execution domain for:

- slot parsing
- command dispatch
- persistence access through `OneSlot`
- request-local compression sample collection
- slot-owned replication/binlog work

`OneSlot` protects slot-owned state with `threadIdProtectedForSafe`. Direct calls into `OneSlot` must happen on the
owning slot worker. Cross-slot command paths use `OneSlot.asyncCall()`, `OneSlot.asyncRun()`, or
`OneSlot.asyncExecute()` to schedule work onto the target slot eventloop.

### Index Workers

Index workers are separate from the net/slot ActiveJ worker pools. They are created inside the persistence subsystem
through `IndexHandlerPool` and are used for index-oriented or background work. Index handler state is also protected by
an owner-thread guard and should be accessed through the index worker eventloop APIs.

## Thread-Local And Shared-State Rules

Classes marked with `@ThreadNeedLocal`, such as `RequestHandler` and `BaseCommand`, rely on per-worker instances rather
than coarse-grained locks. Mutable command-group fields should be treated as worker-local unless explicitly documented
otherwise.

Not all shared runtime state is thread-local:

- `AclUsers` is a singleton backed by an `AtomicReference<Map<String, U>>` immutable snapshot.
- `TrainSampleJob` prefix/suffix groups are published as immutable `List` snapshots through an `AtomicReference`.
- `SocketInspector` uses concurrent maps and atomic counters for cross-worker socket accounting.
- `BlockingList` is global state and must use its own synchronization/concurrent discipline when coordinating blocking
  list replies.

When adding new shared state, prefer one of these explicit patterns:

- keep it owned by a worker and use eventloop handoff;
- publish immutable snapshots with `volatile` or atomic references;
- use concurrent collections/atomic counters for true shared accounting;
- avoid exposing mutable live collections across worker boundaries.

## ACL Snapshot Design

ACL users intentionally use one global copy-on-write snapshot instead of per-slot copies. Readers on net workers, slot
workers, and tests read from the same `AtomicReference`. Writers build new immutable `U` objects, freeze them, create a
new immutable map, and CAS-publish it.

This removes async ACL propagation and avoids per-slot `AclUsers.Inner` instances. The accepted tradeoff is that a
request may read the previous ACL snapshot at the exact moment a rule changes. That can allow or deny one in-flight
operation according to the old rule, but it keeps ACL reads lock-free and simple on hot command paths.

## Cross-Thread Coordination

Cross-slot or cross-worker operations are handled with asynchronous dispatch and reply aggregation rather than by
sharing slot internals directly between threads. This pattern appears in command groups such as `MGroup`, `PGroup`,
`SGroup`, `ZGroup`, and replication-related `XGroup` paths.

`LocalPersist.doSthInSlots()` dispatches one function to each slot and aggregates the results. Code running inside that
function is on the owning slot worker and may access the matching `OneSlot` directly. Code outside that function should
not assume it owns slot state.

Pipeline handling may involve several commands for the same socket while socket metadata changes. Protocol mode and
reply mode are socket-level state; a pipeline is not treated as an atomic snapshot of those fields.

## Bulk Load And WAL Durability

Bulk-load WAL skipping is slot/persistence-owned state, not a global debug flag. `Wal.bulkLoad` is a per-WAL volatile
flag. Ingest commands call `OneSlot.setBulkLoad(true)` on the slot worker before ingesting that slot, and call
`OneSlot.resetWritePositionAfterBulkLoad()` in `finally`.

`Wal.put()` skips direct WAL file writes only when that WAL's own `bulkLoad` flag is true.
`Wal.resetWritePositionAfterBulkLoad()` rewrites delayed values to the WAL file and then clears the flag. This prevents
ingest on one slot from making normal writes on another slot skip WAL.

The manage command `manage debug log-switch bulkLoad ...` is wired to `LocalPersist.doSthInSlots()` and sets the
per-slot WAL flags through `OneSlot.setBulkLoad()`. There is no longer a global `Debug.bulkLoad` durability switch.

## Dictionary Prefix/Suffix Publication

Dictionary prefix/suffix groups are shared by slot command paths and management/config paths. They are stored in
`TrainSampleJob` as an `AtomicReference<List<String>>`. Writers sort and publish an immutable copied list. Readers take
one snapshot and iterate it without locking.

Do not mutate or expose a live `ArrayList` for this state. New code should preserve copy-on-write publication so slot
workers cannot observe concurrent list mutation while selecting dictionaries.

## Bootstrap Constraints

`MultiWorkerServer` validates worker counts against:

- configured slot count
- CPU count
- max supported worker constants

Slot count must be `1` or a power of two. Slot count must be greater than or equal to both net worker count and slot
worker count, and it must divide cleanly by each of those worker counts.

During bootstrap:

- `ConfForGlobal.netWorkers` and `ConfForGlobal.slotWorkers` publish the validated counts.
- `LocalPersist.initSlots(...)` creates slot-owned persistence state.
- `AclUsers.initBySlotWorkerEventloopArray(...)` is kept as an API-compatible no-op because ACL now uses one global
  snapshot.
- `BlockingList.initBySlotWorkerEventloopArray(...)` still records slot worker eventloops for blocking list
  coordination.

## Invariants For New Multithreaded Code

- Do not bypass `OneSlot` owner-thread checks by rewriting `threadIdProtectedForSafe` in production code.
- Do not close slot-owned resources from a non-owner thread; submit cleanup to the owner eventloop and wait for it.
- Do not use global flags to change durability behavior for slot-owned WALs.
- Do not publish mutable shared collections to net/slot workers.
- Do not assume socket protocol metadata is stable for an entire pipeline unless the code explicitly snapshots it.
- Prefer existing eventloop handoff helpers over ad hoc thread creation or direct cross-thread access.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
