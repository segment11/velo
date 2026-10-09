# Velo Command Processing Design

## Overview

A request passes through two distinct execution stages, and it is important not to conflate them:

1. **Net worker** — [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java) owns the
   socket, decodes, gates, dispatches, encodes the reply, and writes it.
2. **Slot worker** — [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java) owns command
   execution, dispatching into the A-Z command-group classes under
   [`src/main/java/io/velo/command`](/home/kerry/ws/velo/src/main/java/io/velo/command).

There are 26 command groups, one per initial letter: `AGroup` through `ZGroup`.

## Pipeline Entry Point

The real entry point is `MultiWorkerServer.consumer(...)`, not `RequestHandler`:

```java
// MultiWorkerServer.java:841-850
return socket -> {
    var byteBufSupplier = ChannelSuppliers.prefetch(8, ChannelSuppliers.ofAsyncSupplier(socket::read, socket));
    BinaryChannelSupplier.of(byteBufSupplier)
            .decodeStream(new RequestDecoder())
            .mapAsync(pipeline -> handlePipeline(pipeline, socket, (short) slotNumber))
            .streamTo(ChannelConsumers.ofSocket(socket));
};
```

One decode produces a **pipeline** of `Request` objects, not a single request. `handlePipeline`
(`MultiWorkerServer.java:698`) then:

1. parses slots for every request in the pipeline, on the net worker thread, using
   `requestHandlerArray[0].parseSlots(request)` (`MultiWorkerServer.java:703-706`) — safe because `parseSlots` only
   computes slot numbers and key hashes, it does not touch slot data
2. splits the pipeline into segments at connection-state commands via `handlePipelineSegmented`
   (`MultiWorkerServer.java:731`). `isConnectionStateCommand` (`MultiWorkerServer.java:685`) matches `auth`, `hello`,
   `readonly`, and `readwrite`. These act as barriers: the barrier's reply is awaited before later requests run,
   so that socket-level state changes are visible to them.
3. coalesces all replies into a single `ByteBuf` via `allPipelineByteBuf` (`MultiWorkerServer.java:809`)

## Net Worker Stage

`handleRequest(request, socket)` (`MultiWorkerServer.java:503`) runs in this exact order:

| # | Check | Location | Result |
|---|---|---|---|
| 1 | replication request | `:504` | `handleReplRequest(...)` |
| 2 | `quit` | `:509-511` | `handleQuit` closes the socket, replies `OK` |
| 3 | auth gate | `:513-517` | `NO_AUTH` unless cmd is `auth` or `hello` |
| 4 | ACL check | `:519-531` | `ACL_PERMIT_LIMIT` / `ACL_PERMIT_KEY_LIMIT`, plus `AclUsers.recordAclLog` |
| 5 | `shutdown` | `:533-535` | `handleShutdown`, arg-validated, runs on a daemon thread after 100ms |
| 6 | `echo` / `ping` | `:537-540` | `handleFast` replies `BulkReply` / `PongReply` — **never reaches a command group** |
| 7 | readonly connection | `:542-544` | `READONLY` for write commands |
| 8 | cluster slot check | `:546-552` | `checkClusterSlot` → `CLUSTER_SLOT_NOT_SET` / `CLUSTER_SLOT_CROSS_SHARDS` / `MOVED` |
| 9 | cross-worker detection | `:554-571` | sets `request.isCrossRequestWorker` when a multi-key request spans slot workers |
| 10 | reply mode | `:577-579` | `ByteBuf.empty()` when `replyMode != ReplyMode.on` |
| 11 | dispatch to slot worker | `:597-608` | see [Thread Handoff](#thread-handoff) |

Steps 3 and 4 are the real authentication/authorization path. ACL command and key permission checking is **not**
performed in `RequestHandler.handle`; it happens here through `request.isAclCheckOk()`
(`Request.java:97-110` → `U.checkCmdAndKey`).

## Thread Handoff

```java
// MultiWorkerServer.java:597-608
var firstSlot = request.getSingleSlot();
if (firstSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER) {   // -1: ping, echo, hello, auth, ...
    i = new Random().nextInt(requestHandlerArray.length);
} else {
    i = firstSlot % requestHandlerArray.length;
}
var targetHandler = requestHandlerArray[i];
var otherNetWorkerEventloop = slotWorkerEventloopArray[i];
return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, otherNetWorkerEventloop);
```

`Request.getSingleSlot()` (`Request.java:208`) returns `-1` (`SLOT_CAN_HANDLE_BY_ANY_WORKER`) when the slot list is
null or empty, in which case any slot worker is eligible. Otherwise the worker is chosen as `slot % workerCount`.

The actual submit is `MultiWorkerServer.java:630`:

```java
var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));
```

**Special case:** when `slotWorkers == 1 && netWorkers == 1`, the slot worker reuses the net worker eventloop
(`isReuseNetWorkerEventloop = true`, set at `MultiWorkerServer.java:1041-1052`). In that configuration
`RequestHandler.handle` is called inline on the net worker at `MultiWorkerServer.java:576-595`.

Replication requests use the same handoff, keyed on `replRequest.getSlot()` (`MultiWorkerServer.java:488-496`).

`RequestHandler` instances are created one per slot worker (`MultiWorkerServer.java:1711-1713`):

```java
var list = new RequestHandler[slotWorkers];
for (int i = 0; i < slotWorkers; i++) {
    list[i] = new RequestHandler((byte) i, (byte) slotWorkers, (short) slotNumber, snowFlakes[i], config);
}
```

## Slot Parsing

`RequestHandler.parseSlots(Request)` (`RequestHandler.java:190`) is called from the net worker, on the command-group
instances owned by `requestHandlerArray[0]`. It:

- returns a synthetic single-slot entry for replication requests
- returns an **empty** slot list for `echo`, `ping`, `quit`, `hello`, `auth` (`RequestHandler.java:200-207`), which is
  what makes those commands eligible for any worker
- lowercases the first command byte; returns early if it is outside `a-z`
- has a special case: if the group is `GGroup` and the first argument starts with the `x_repl` dispatch prefix, it
  re-parses through `XGroup`'s slot logic instead (`RequestHandler.java:222-233`)
- otherwise delegates to `commandGroup.parseSlots(cmd, data, slotNumber)`

Commands describe their key positions explicitly in `parseSlots(...)`. Common patterns:

- first argument is the key
- alternating key/value arguments
- source and destination keys for copy/move/store commands

The result is stored on the request as `Request.slotWithKeyHashList`.

## Dispatch Model

`commandGroups` is a plain array field, not a map:

```java
// RequestHandler.java:126
private final BaseCommand[] commandGroups = new BaseCommand[26];
```

`initCommandGroups()` (`RequestHandler.java:135-170`) creates one instance of each group, injects the snowflake and
`this` handler, and also keeps two aliases used by the fast paths: `reuseGGroup` (`GGroup`) and `reuseSGroup`
(`SGroup`).

Lookup is by first byte, via `getA_ZGroupCommand(byte firstByte)` → `commandGroups[firstByte - 'a']`
(`RequestHandler.java:178`). The normalization is:

```java
// RequestHandler.java:570-579
var firstByte = data[0][0];
if (firstByte >= 'A' && firstByte <= 'Z') { firstByte += 32; }
if (firstByte < 'a' || firstByte > 'z') { return ErrorReply.FORMAT; }
var commandGroup = commandGroups[firstByte - 'a'];
commandGroup.resetContext(cmd, data, socket, request);
```

`RequestHandler` is annotated `@ThreadNeedLocal` (`RequestHandler.java:35`). Group instances are reused across
requests through `resetContext(...)` (`BaseCommand.java:150-157`) rather than allocated per request. This is why many
command classes keep mutable fields and rely on thread affinity.

Note that the same `commandGroups` array is used for parsing on the net worker thread. This is safe only because
`parseSlots` is pure computation.

## BaseCommand Contract

[`BaseCommand`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java) defines the shared contract:

- `parseSlots(String cmd, byte[][] data, int slotNumber)`
- `handle()`
- `resetContext(...)`
- shared helpers for slot hashing, auth lookup, type checks, common get/set helpers, and mock helpers for tests

The slot resolution helper implements Redis-style hash-tag handling and uses `JedisClusterCRC16` for the
client-visible cluster slot while keeping Velo's internal slot number separately
(`BaseCommand.slot(...)`, `BaseCommand.java:620`). Hash-tag extraction is `tagHash(...)` (`BaseCommand.java:585`).

### SlotWithKeyHash

`SlotWithKeyHash` is a **nested record inside `BaseCommand`** (`BaseCommand.java:556-580`), not a top-level source
file:

```java
public record SlotWithKeyHash(short slot, short toClientSlot, int bucketIndex,
                              long keyHash, String rawKey) {
    public static final short IGNORE_TO_CLIENT_SLOT = -1;
    // 4-arg and 3-arg convenience constructors set toClientSlot = IGNORE_TO_CLIENT_SLOT
    public static final SlotWithKeyHash TO_FIX_FIRST_SLOT = new SlotWithKeyHash((short) 0, 0, 0L, null);
}
```

`toClientSlot` is skipped by `MultiWorkerServer.checkClusterSlot` when it equals `IGNORE_TO_CLIENT_SLOT`.
`TO_FIX_FIRST_SLOT` is an unknown-slot placeholder used by e.g. `AGroup.java:92` and `U.java:607`.

## Slot Worker Stage

`RequestHandler.handle(request, socket)` (`RequestHandler.java:281`) runs in this order:

1. **stopped check** (`:282-284`) → `ErrorReply.SERVER_STOPPED`
2. **replication** (`:288-292`) → allocates a per-request `XGroup` and calls `handleRepl(request.getReplRequest())`
3. **HTTP single-token requests** (`:295-404`) — Basic-auth gate for non-health-check URLs, `?metrics` Prometheus
   dump, and the HAProxy health-check URLs `master`, `master_or_slave`, `slave`, `slave_with_zone=<zone>`
4. **format check** (`:406-408`) → `ErrorReply.FORMAT` if `data[0] == null`
5. **request timer** (`:410-411`) — `requestTimeSummary.labels(cmd).startTimer()` in a try-with-resources wrapper that
   spans the rest of the method
6. **HTTP Basic auth** (`:413-461`) — `Authorization` header, Base64 decode, `aclUsers.get(user)`,
   `SocketInspector.setAuthUser`
7. **`AUTH` command** (`:462-494`) — handled inline, not dispatched to a group; checks user existence, `isOn()`, and
   password, logging each failure via `AclUsers.recordAclLog`; returns `OKReply.INSTANCE`
8. **`GET` shortcut** (`:497-538`) — exactly `GET <key>`; also routes `x_repl`-prefixed keys to a freshly allocated
   `XGroup` via `transferDataForXGroup` (`RequestHandler.java:251`)
9. **`SET` shortcut** (`:542-567`) — exactly `SET <key> <value>`; validates `KEY_MAX_LENGTH` / `VALUE_MAX_LENGTH`,
   then calls `reuseSGroup.set(...)` directly, bypassing `SGroup`'s full option parsing
10. **group dispatch** (`:570-581`) — see [Dispatch Model](#dispatch-model)
11. **exception mapping** (`:582-602`) — `CannotReadException` → `CANNOT_READ`, `ReadonlyException` → `READONLY`,
    `TypeMismatchException` → `WRONG_TYPE`, anything else → generic `ErrorReply`; `errorReplyCountTotal++` on every
    error reply

Note that `hello` is **not** special-cased here. It is dispatched through `HGroup` (`HGroup.java:313` → `hello()` at
`HGroup.java:555`), and that is where the RESP3 flag is flipped:

```java
if ("3".equals(ver)) { SocketInspector.setResp3(socket, true); isResp3 = true; }
else if ("2".equals(ver)) { SocketInspector.setResp3(socket, false); isResp3 = false; }
```

## Cross-Slot Behavior

Cross-slot operations are supported in selected commands. The typical implementation:

- group input keys by `slotWithKeyHash.slot()`
- submit work to the owning slot worker via `OneSlot.asyncCall(...)` / `asyncRun(...)` / `asyncExecute(...)`
- aggregate replies into one `Reply`, usually by returning an `AsyncReply` that resolves when the sub-results settle

Examples:

- [`MGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/MGroup.java) for `mget`/`mset` variants
- [`PGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/PGroup.java) for `pfcount` and `pfmerge`
- [`SGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/SGroup.java) for set operations
  (e.g. `SGroup.java:1261-1270`)

Commands also consult `request.isCrossRequestWorker` (copied in by `resetContext`) to decide whether extra
coordination is needed — see `SGroup.java:1240`, `GGroup.java:908`, `MGroup.java:181`.

## Reply Encoding

Reply encoding happens on the **net worker**, after `RequestHandler.handle` returns, not inside the command code:

```java
// MultiWorkerServer.java:652-656
return Promise.of(
        request.isHttp() ?
                wrapHttpResponse(reply) :
                (isResp3 ? reply.bufferAsResp3() : reply.buffer())
);
```

The same ternary is duplicated at `:591-593` (same-eventloop fast path) and `:679-681` (`transferAsyncReply`).

Decision rules:

| Mode | Condition | Notes |
|---|---|---|
| HTTP | `request.isHttp()` | `wrapHttpResponse` (`MultiWorkerServer.java:304`) maps `NilReply` → 404, auth errors → `401`, other errors → `500`, else `200`. Body from `reply.bufferAsHttp()` |
| RESP3 | `SocketInspector.createUserDataIfNotSet(socket).isResp3` | field `VeloUserDataInSocket.isResp3`, default `false`. Only `HELLO 3` sets it |
| RESP2 | otherwise | the default |
| suppressed | `veloUserData.replyMode != ReplyMode.on` | `ByteBuf.empty()`; set by `CLIENT REPLY` (`CGroup.java:185-199`) |

`Reply.bufferAsHttp()` defaults to `null` (`Reply.java:30`) and is overridden only in `BulkReply`, `MultiBulkReply`,
`NilReply`, `ErrorReply`, `OKReply`, `PongReply`, and `IntegerReply`. Likewise `Reply.bufferAsResp3()` defaults to
`buffer()` (`Reply.java:23`) and is overridden in `BulkReply`, `NilReply`, `BoolReply`, `DoubleReply`, and
`MultiBulkReply`.

Two rewrites happen immediately before encoding:

- `ErrorReply.FORMAT` becomes `ErrorReply.WRONG_NUMBER(cmd)` (`:587-589`, `:648-650`, `:675-677`)
- an `AsyncReply` is routed to `transferAsyncReply` (`:667-683`), which applies the same HTTP/RESP3/RESP2 rules once
  the promise settles

Socket state lives on `VeloUserDataInSocket` and is reached through `SocketInspector`. Because a pipeline is not an
atomic snapshot, `isResp3` and `replyMode` may change mid-pipeline, and replies already in flight use the older value.
See [Multithreading](/home/kerry/ws/velo/doc/design/11_multithreading_design.md).

## Special Command Families

- ACL subcommands (`ACL SETUSER`, `ACL LIST`, etc.) live in
  [`AGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/AGroup.java). The `AUTH` command is handled inline in
  `RequestHandler` (`:462-494`) and `HELLO ... AUTH` lives in
  [`HGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/HGroup.java). See
  [ACL Security Design](/home/kerry/ws/velo/doc/design/10_acl_security_design.md).
- Bloom filter commands live in [`BGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/BGroup.java).
- GEO commands live in [`GGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/GGroup.java).
- HyperLogLog commands live in [`PGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/PGroup.java).
- Replication protocol handling lives in [`XGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/XGroup.java).

## Related Documents

- [Overall Architecture](/home/kerry/ws/velo/doc/design/01_overall_architecture.md)
- [Persistence](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Type System](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Response Encoding](/home/kerry/ws/velo/doc/design/06_response_encoding_design.md)
