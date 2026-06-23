# Bug 52 ActiveJ Usage Review Round 1

## Scope

This review checks Velo's direct ActiveJ usage against the local ActiveJ source under
`/home/kerry/ws/src_learn/activej`, focusing on reactor thread affinity and promise/future handoff
patterns.

ActiveJ baseline used for this review:

- `TcpSocket.read()` and `TcpSocket.write(...)` require the socket's owning reactor thread:
  `/home/kerry/ws/src_learn/activej/core-net/src/main/java/io/activej/net/socket/tcp/TcpSocket.java:347-348`
  and `:443-447`.
- `TcpSocket.closeEx(...)`, called by `close()`, also requires the owning reactor thread:
  `/home/kerry/ws/src_learn/activej/core-net/src/main/java/io/activej/net/socket/tcp/TcpSocket.java:554-558`.
- `TcpSocket.doClose()` performs inspector disconnect callbacks on the closing thread:
  `/home/kerry/ws/src_learn/activej/core-net/src/main/java/io/activej/net/socket/tcp/TcpSocket.java:567-572`.
- ActiveJ `AbstractPromise.complete(...)` invokes chained callbacks synchronously in the thread that completes the
  promise: `/home/kerry/ws/src_learn/activej/core-promise/src/main/java/io/activej/promise/AbstractPromise.java:108-126`.

## Finding 1: Master-side replication socket close is scheduled on the slot worker instead of the socket's net-worker reactor

**Severity:** High

**Files:**

- `src/main/java/io/velo/repl/ReplPair.java:565-579`
- `src/main/java/io/velo/command/XGroup.java:259-266`
- `src/main/java/io/velo/MultiWorkerServer.java:488-495`

**Code excerpts:**

```java
// ReplPair.java:565-579
public void closeSlaveConnectSocket() {
    if (slaveConnectSocketInMaster != null && !slaveConnectSocketInMaster.isClosed()) {
        var localPersist = LocalPersist.getInstance();
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.asyncExecute(() -> {
            try {
                slaveConnectSocketInMaster.close();
                log.warn("Repl pair master close slave socket, {}:{}, slot={}", host, port, slot);
            } catch (Exception e) {
                log.error("Repl pair master close slave socket error={}, {}:{}, slot={}", e.getMessage(), host, port, slot);
            } finally {
                slaveConnectSocketInMaster = null;
            }
        });
    }
}
```

```java
// XGroup.java:259-266
if (replPair != null) {
    replPair.increaseStatsCountForReplType(replType);
    replPair.increaseFetchedBytesLength(contentBytes.length);

    // for proactively close slave connection in master
    if (replPair.isAsMaster()) {
        replPair.setSlaveConnectSocketInMaster((TcpSocket) socket);
    }
}
```

```java
// MultiWorkerServer.java:488-495
private Promise<ByteBuf> handleReplRequest(Request request, ITcpSocket socket) {
    var slot = request.getSlotNumber();
    var targetEventloop = slotWorkerEventloopArray[slot % slotWorkerEventloopArray.length];
    var requestHandler = requestHandlerArray[slot % requestHandlerArray.length];

    return getByteBufPromiseByOtherEventloop(request, socket, targetEventloop, requestHandler);
}
```

**Root cause:**

The master stores the accepted replication `TcpSocket` in `XGroup.handleReplInner(...)`, but replication requests are
explicitly moved from the net worker to the slot worker before `XGroup` handles them. Later, `ReplPair.closeSlaveConnectSocket()`
schedules `slaveConnectSocketInMaster.close()` on `oneSlot.asyncExecute(...)`, i.e. the slot worker. ActiveJ `TcpSocket.close()`
must run on the socket's owning reactor thread, not an arbitrary eventloop.

There is already a correct pattern nearby for normal quit handling:

```java
// MultiWorkerServer.java:483-485
if (socket instanceof TcpSocket) {
    return ((TcpSocket) socket).getReactor().submit(socket::close).then($ -> Promise.of(OKReply.INSTANCE.buffer()));
}
```

**Impact:**

This can throw ActiveJ's `"Not in reactor thread"` exception and leave the slave connection open or only partially cleaned up.
Because `TcpSocket.doClose()` runs `inspector.onDisconnect(...)`, a wrong-thread close also risks skipping or mis-threading
connection accounting and socket cleanup. The affected paths include failover closing master-side slave sockets and delayed
replication pair removal.

**Suggested fix direction:**

Snapshot the current `TcpSocket` into a local variable, clear or compare the field safely, and schedule close on
`socket.getReactor().execute(...)` or `socket.getReactor().submit(...)`. Keep the `null` assignment consistent with the
socket snapshot so a newer slave connection is not accidentally cleared by an older close task.

## Finding 2: `KEYS` completes an ActiveJ promise from the key-analysis eventloop

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/KGroup.java:95-113`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:301-325`
- `src/main/java/io/velo/MultiWorkerServer.java:667-682`

**Code excerpts:**

```java
// KGroup.java:95-113
var keyAnalysisHandler = localPersist.getIndexHandlerPool().getKeyAnalysisHandler();
var f = keyAnalysisHandler.prefixMatch(keyPrefix, pattern, maxCount);

SettablePromise<Reply> finalPromise = new SettablePromise<>();
var asyncReply = new AsyncReply(finalPromise);

f.whenComplete((r, e) -> {
    if (e != null) {
        log.error("keys error={}", e.getMessage());
        finalPromise.setException((Exception) e);
        return;
    }

    var replies = new Reply[r.size()];
    for (int i = 0; i < r.size(); i++) {
        replies[i] = new BulkReply(r.get(i));
    }
    finalPromise.set(new MultiBulkReply(replies));
});
```

```java
// KeyAnalysisHandler.java:301-325
public CompletableFuture<ArrayList<String>> prefixMatch(@NotNull String prefix, Pattern pattern, int maxCount) {
    return eventloop.submit(AsyncComputation.of(() -> {
        var result = new ArrayList<String>();
        var iterator = db.newIterator();
        try {
            iterator.seek(Wal.keyBytes(prefix));

            while (iterator.isValid() && result.size() < maxCount) {
                var keyBytes = iterator.key();
                var key = Wal.keyString(keyBytes);
                if (key.startsWith(prefix)) {
                    if (pattern.matcher(key).matches()) {
                        result.add(key);
                    }
                } else {
                    break;
                }
                iterator.next();
            }

            return result;
        } finally {
            iterator.close();
        }
    }));
}
```

```java
// MultiWorkerServer.java:667-682
private Promise<ByteBuf> transferAsyncReply(AsyncReply asyncReply, Request request) {
    var promise = asyncReply.getSettablePromise();
    return promise.map(reply -> {
        var isResp3 = request.isResp3();
        var userData = request.getU();
        if (userData != null) {
            if (userData.isResp3) {
                isResp3 = true;
            }
        }

        return isResp3 ? reply.bufferAsResp3() : reply.buffer();
    });
}
```

**Root cause:**

`KeyAnalysisHandler.prefixMatch(...)` returns a Java `CompletableFuture` completed by the key-analysis ActiveJ eventloop.
`KGroup.keys()` attaches `CompletableFuture.whenComplete(...)` and calls `SettablePromise.set(...)` directly from that
completion callback. Java `CompletableFuture.whenComplete(...)` runs in the completing thread unless an executor is supplied.

ActiveJ `SettablePromise` completion then synchronously invokes chained callbacks in that same thread. In this path, the
`transferAsyncReply(...)` `promise.map(...)` response conversion can therefore run on the key-analysis eventloop instead of
the request reactor that owns the command processing chain.

**Impact:**

The response path crosses eventloop ownership without an ActiveJ handoff. Today the `map(...)` mostly builds a `ByteBuf`, but
it also reads request/user-data state and feeds the request promise chain. Future changes or error paths may touch socket or
reactor-owned state from the key-analysis eventloop. This is the same class of bug ActiveJ's `Promise.ofFuture(...)` helper
is designed to avoid by bouncing completion back to the current reactor.

**Suggested fix direction:**

Bridge the `CompletableFuture` to ActiveJ on the request reactor. For example, call `Promise.ofFuture(f)` while still in the
request reactor and map the result to `MultiBulkReply`, or capture `Reactor.getCurrentReactor()` before registering the Java
callback and complete `finalPromise` through `reactor.execute(...)`. Audit other `CompletableFuture.whenComplete(...)` to
`SettablePromise.set(...)` bridges for the same pattern.

## Finding 3: `FLUSHDB` ignores the key-analysis eventloop future

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/command/FGroup.java:123-133`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:214-216`

**Code excerpts:**

```java
// FGroup.java:123-133
return localPersist.doSthInSlots(oneSlot -> {
    oneSlot.flush();

    if (oneSlot == localPersist.firstOneSlot()) {
        var indexHandlerPool = localPersist.getIndexHandlerPool();
        if (indexHandlerPool != null) {
            indexHandlerPool.getKeyAnalysisHandler().flushdb();
        }
    }
    return true;
}, resultList -> OKReply.INSTANCE);
```

```java
// KeyAnalysisHandler.java:214-216
public CompletableFuture<Void> flushdb() {
    return eventloop.submit(this::clearAllKeysAfterAnalysis);
}
```

**Root cause:**

`KeyAnalysisHandler.flushdb()` schedules RocksDB close/delete/recreate work on the key-analysis eventloop and returns a
`CompletableFuture<Void>`, but `FGroup.flushdb()` discards the future and returns `OK` once slot flush work finishes. No
ActiveJ or Java future bridge observes completion or failure of the index clear.

**Impact:**

`FLUSHDB` / `FLUSHALL` can acknowledge success before the key-analysis index is cleared. A following `KEYS` request can see
stale index entries until the queued key-analysis task runs. If `clearAllKeysAfterAnalysis()` throws, the exception is lost
to the client path and the command still returns `OK`, leaving index state inconsistent with flushed slot data.

**Suggested fix direction:**

Include the key-analysis future in the command's async completion. The fix should preserve reactor affinity: either convert
the future with `Promise.ofFuture(...)` from the request reactor, or route completion back to the request reactor before
building the final reply. If the existing `localPersist.doSthInSlots(...)` API cannot compose this future cleanly, add a
small focused helper for `FLUSHDB` rather than returning before the index eventloop work completes.

## Summary

| Finding | Severity | Status | Primary risk |
| --- | --- | --- | --- |
| 1 | High | Needs AI agent 2 verification | Wrong-reactor `TcpSocket.close()` for master-side replication sockets |
| 2 | High | Needs AI agent 2 verification | ActiveJ promise chain completed from key-analysis eventloop |
| 3 | Medium | Needs AI agent 2 verification | `FLUSHDB` acknowledges before async key-analysis clear completes |

## Suggested Fix Order

1. Fix Finding 1 first because it can directly violate ActiveJ socket thread affinity and break replication connection cleanup.
2. Fix Finding 2 next because it is a reusable async bridge bug pattern and may affect response reactor ownership.
3. Fix Finding 3 last, likely reusing the safe future-to-promise bridge established for Finding 2.

Per `AGENTS.md`, each confirmed bug fix should be handled separately with TDD, relevant focused tests, JaCoCo inspection for
the changed lines/branches, and one commit per bug.
