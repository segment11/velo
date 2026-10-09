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

The net/slot boundary is worth stating plainly, because it is a common source of confusion:

| Stage | Thread | Responsibility |
|---|---|---|
| Net worker | `net-worker-<id>` | decode, slot parsing, auth/ACL/gates, cross-worker detection, reply encoding, socket write |
| Slot worker | `slot-worker-<i>` | command dispatch, `OneSlot` access, compression samples, binlog work |

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

Net workers are ActiveJ worker reactors (thread name `net-worker-<id>`). They:

- own client sockets
- decode RESP, HTTP, and replication requests
- perform request gating that can be decided before slot execution — auth, ACL, `shutdown`, `echo`/`ping`,
  readonly connections, cluster `MOVED`, cross-worker detection
- **resolve the slot list** for every request in a pipeline, via
  `RequestHandler.parseSlots(...)` on the command groups owned by `requestHandlerArray[0]`
  (`MultiWorkerServer.java:703-706`). This is safe on the net worker only because slot parsing is pure
  computation over the request bytes — it never touches slot-owned state
- forward requests to the owning slot worker
- encode and write replies

Encoding is deliberately on this side of the handoff, so command code returns `Reply` objects rather than writing
to sockets. See [Reply Encoding](/home/kerry/ws/velo/doc/design/04_command_processing_design.md#reply-encoding).

Socket state lives on the socket user-data object (`VeloUserDataInSocket`) and is accessed through `SocketInspector`.
Most socket fields are expected to be touched by the owning net worker. Some state can be read or updated from command
code running on slot workers; current code accepts that `isResp3` and `replyMode` can change during a pipeline and that
some replies in that pipeline may use the old mode. This is an accepted compatibility/performance tradeoff, not a
strict cross-thread synchronization guarantee.

Connection admission and socket maps are shared across workers through concurrent/atomic state in `SocketInspector`.
`SocketInspector.clearAll()` must reset both the socket map and admission counters so later max-connection checks do
not drift.

### Slot Workers

Slot workers execute command logic (thread name `slot-worker-<i>`). `RequestHandler` instances are created one per
slot worker (`MultiWorkerServer.java:1711-1713`) and are marked with `@ThreadNeedLocal` because they reuse mutable
command-group instances and request-local helper state.

This is the main execution domain for:

- command dispatch into the reused A-Z group instances
- persistence access through `OneSlot`
- request-local compression sample collection
- slot-owned replication/binlog work

Note what is *not* on this list: **slot parsing happens on the net worker**, not here. It is a pure hashing step
completed before the handoff.

When `slotWorkers == 1 && netWorkers == 1`, `MultiWorkerServer` sets `isReuseNetWorkerEventloop = true`
(`MultiWorkerServer.java:1041-1052`) and the slot worker eventloop *is* the net worker eventloop, so
`RequestHandler.handle` runs inline on the net worker thread instead of being submitted.

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

The net-to-slot handoff is a plain eventloop submit:

```java
// MultiWorkerServer.java:630
var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));
```

The target worker is `slot % slotWorkerCount`, chosen from `request.getSingleSlot()`. Commands with no slot
(`Request.SLOT_CAN_HANDLE_BY_ANY_WORKER == -1`: `ping`, `echo`, `hello`, `auth`, and anything else with an empty slot
list) get a random worker, because they touch no slot state.

Cross-slot or cross-worker operations are handled with asynchronous dispatch and reply aggregation rather than by
sharing slot internals directly between threads. This pattern appears in command groups such as `MGroup`, `PGroup`,
`SGroup`, `ZGroup`, and replication-related `XGroup` paths. Such commands return an `AsyncReply`, which the net worker
resolves in `transferAsyncReply` using the same HTTP/RESP3/RESP2 rules as a synchronous reply.

`LocalPersist.doSthInSlots()` dispatches one function to each slot and aggregates the results. Code running inside that
function is on the owning slot worker and may access the matching `OneSlot` directly. Code outside that function should
not assume it owns slot state.

### Pipeline Barriers

One decode produces a pipeline of `Request` objects, and `handlePipelineSegmented`
(`MultiWorkerServer.java:731`) splits it at connection-state commands — `auth`, `hello`, `readonly`, `readwrite`
(`isConnectionStateCommand`, `MultiWorkerServer.java:685`). The barrier's reply is awaited before later requests in
the pipeline run, so a connection-state change is visible to everything after it.

This is a narrow guarantee, not a general atomicity one. Protocol mode and reply mode remain socket-level state
outside those barriers, so a pipeline is **not** an atomic snapshot of those fields, and replies already in flight may
use the older value.

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
- Do not move slot parsing onto the slot worker, or command execution onto the net worker, without re-checking the
  thread-affinity assumptions on both sides of the handoff.
- Prefer existing eventloop handoff helpers over ad hoc thread creation or direct cross-thread access.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
