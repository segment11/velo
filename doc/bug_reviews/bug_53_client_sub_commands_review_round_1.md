# Bug 53: Client Sub-Commands Review Round 1

## Scope

Reviewed branch: `feat/client-sub-commands`

Reviewed commit set provided by user:

- `3c8bcb61` - Monotonic client id foundation (`SocketInspector` + `VeloUserDataInSocket`)
- `0f8247c4` - Test reshape (single `def 'test client'()` per project convention)
- `22d78170` - Production: `CLIENT LIST`, `KILL` filters, arity, `NO-EVICT` / `NO-TOUCH`, `NOT_SUPPORT`, unknown to `SYNTAX`
- `2e490343` - Doc: `doc/redis_command_support.md` matrix
- `52f0de80` - Test: repl and maxAge-too-low coverage branches

Observed during review: branch head also contained `e617b2cc` (`test: cover new public methods in target test classes`) beyond the five commits listed above.

## Review Summary

The implementation adds useful CLIENT command compatibility, especially stable client ids, `CLIENT LIST`, filter-form `CLIENT KILL`, and explicit handling for unsupported CLIENT subcommands. The main risks found are protocol-visible address formatting and a mismatch between the replica test setup and the production replication socket lifecycle.

## Findings

### 1. Important: `CLIENT KILL ADDR` and legacy `CLIENT KILL ip:port` do not match Redis-form addresses

Files:

- `src/main/java/io/velo/command/CGroup.java`, reviewed line around `matchesClientKillFilter`, `ADDR` comparison
- `src/main/java/io/velo/SocketInspector.java`, reviewed line around `getClientInfo`, `addr=` output
- `src/test/groovy/io/velo/command/CGroupTest.groovy`, reviewed cases for legacy and `ADDR` kill forms

Code excerpt from reviewed branch:

```java
var remoteAddress = ((TcpSocket) candidate).getRemoteAddress().toString();
if (!filter.addr().equals(remoteAddress)) {
    return false;
}
```

Root cause:

`InetSocketAddress.toString()` can render host-qualified values such as `localhost/127.0.0.1:46411` or slash-prefixed values such as `/127.0.0.1:46411`. Redis clients and Redis `CLIENT LIST addr=` use `ip:port` form, for example `127.0.0.1:46411`.

Impact:

Clients using the documented Redis address form may fail to kill matching clients. `CLIENT LIST` can also report an `addr=` value that cannot be fed back into `CLIENT KILL ADDR` in the normal Redis-compatible form. The current tests miss this because they build expected kill addresses using `remoteAddress.toString()`.

Suggested fix:

Format remote addresses with explicit host/address and port fields, and use that same helper for both `CLIENT LIST addr=` and `CLIENT KILL ADDR` matching. Add tests that use `127.0.0.1:<port>` rather than `InetSocketAddress.toString()`.

### 2. Important: `CLIENT KILL TYPE replica/slave` does not cover real replication sockets

Files:

- `src/main/java/io/velo/repl/TcpClient.java`, reviewed line where repl sockets are assigned `new VeloUserDataInSocket(replPair)`
- `src/main/java/io/velo/SocketInspector.java`, reviewed repl early return in `onConnect`
- `src/main/java/io/velo/command/CGroup.java`, reviewed `killClientsMatching` snapshot over `socketMap`
- `src/test/groovy/io/velo/command/CGroupTest.groovy`, reviewed replica kill test setup

Code excerpts from reviewed branch:

```java
socket.setUserData(new VeloUserDataInSocket(replPair));
socket.setInspector(MultiWorkerServer.STATIC_GLOBAL_V.socketInspector);
MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.onConnect(socket);
```

```java
if (veloUserData.replPairAsSlaveInTcpClient != null) {
    // this socket is a slave connection master
    // need not check max connections
    var remoteAddress = socket.getRemoteAddress();
    log.info("Inspector on repl connect, remote address={}, slot={}", remoteAddress, veloUserData.replPairAsSlaveInTcpClient.getSlot());
    return;
}
```

```java
var snapshot = new ArrayList<>(socketInspector.socketMap.entrySet());
```

Root cause:

Production replication sockets already have `replPairAsSlaveInTcpClient` set before `SocketInspector.onConnect()` runs. `onConnect()` returns early for those sockets and does not add them to `socketMap`. `CLIENT KILL` only scans `socketMap`, so `TYPE replica` / `TYPE slave` cannot match real replication sockets.

The test creates a normal socket, registers it in `socketMap`, then mutates its user data into a repl socket. That is not the production lifecycle, so the test proves a path that real repl sockets do not take.

Impact:

`CLIENT KILL TYPE replica` and `CLIENT KILL TYPE slave` are advertised as supported but may return `0` for actual replication sockets. The test also logs an async fatal error during cleanup because it drives the repl disconnect path with an incomplete test fixture.

Suggested fix:

Either keep replica/slave type parsing unsupported until repl sockets are tracked in a killable registry, or extend `SocketInspector` to track repl sockets separately and make `CLIENT KILL TYPE replica/slave` operate on that registry. Update tests to create repl user data before `onConnect()` so they match production behavior.

### 3. Medium: focused CLIENT command test passes while logging a fatal async error

File:

- `src/test/groovy/io/velo/command/CGroupTest.groovy`, reviewed cleanup helper around `unregisterSocketOnInspector`

Observed verification output:

```text
> Task :test
CGroupTest > test client STANDARD_OUT
...
ERROR [io.activej.eventloop.Eventloop] - Fatal error in io.velo.command.CGroupTest$_unregisterSocketOnInspector_closure2
java.lang.NullPointerException: Cannot invoke "io.activej.eventloop.Eventloop.execute(java.lang.Runnable)" because "this.slotWorkerEventloop" is null
...
BUILD SUCCESSFUL
```

Root cause:

The test manually calls `SocketInspector.onDisconnect()` for a socket that has been mutated into a repl socket. That enters the repl catch-up path, which expects a fuller replication fixture than the CLIENT command test provides.

Impact:

The build stays green, but the test output contains a fatal eventloop error. This makes the regression signal noisy and can hide future real failures in the same area.

Suggested fix:

Avoid mutating a normal registered socket into a repl socket during cleanup, or set the existing repl test bypass (`XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed`) around this case if the test intentionally exercises repl disconnect without a full slot fixture. Prefer the production-lifecycle test change from finding 2.

## Verification Performed

Commands run during review:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest"
python3 scripts/jacoco_cover.py io.velo.command.CGroup 229 482 --src
```

Results:

- `CGroupTest` completed with `BUILD SUCCESSFUL`.
- The same run logged the fatal ActiveJ eventloop error described above.
- Coverage for `CGroup` lines 229-482 reported `Lines: 129 | Covered: 110 | Partial: 12 | Not-covered: 7`.
- Notable uncovered branches included the positive pubsub type branch and several null/error edge branches.

## Review Feedback

Summary of fix/feature:

The branch meaningfully expands Redis CLIENT compatibility by introducing monotonic client ids, `CLIENT LIST`, filter-form `CLIENT KILL`, no-op compatibility for `NO-EVICT` and `NO-TOUCH`, and explicit errors for unsupported stateful subcommands.

Strengths:

- Stable client ids are a real improvement over using `socket.hashCode()` for command-visible ids.
- `CLIENT KILL` now closes sockets via the owning reactor, which is the right direction for ActiveJ socket ownership.
- The tests cover many arity, syntax, filter, and reactor-close cases.

Concerns:

- Address formatting should be normalized before this is treated as Redis-compatible.
- Replica/slave kill support is currently overclaimed by implementation and tests.
- The test suite should not emit fatal eventloop errors during a successful run.

Follow-ups before merge:

- Add a shared address formatting helper and test `CLIENT LIST` plus `CLIENT KILL ADDR` with `127.0.0.1:<port>`.
- Decide whether replica/slave `CLIENT KILL` is supported now or should return syntax/not-supported until repl sockets are tracked.
- Remove the fatal async test log and rerun focused tests plus branch-focused JaCoCo inspection.
