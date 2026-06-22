# Client Sub-Commands Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Velo's `CLIENT` command family explicit and Redis-compatible for the subcommands Velo can safely support, starting with full `CLIENT KILL` filter parsing.

**Architecture:** Keep `CLIENT` dispatch in `src/main/java/io/velo/command/CGroup.java`, but move subcommand-specific parsing into small private helpers so each Redis subcommand has clear arity, syntax, and reply behavior. Use `SocketInspector` and `VeloUserDataInSocket` as the source of connection metadata, adding only the missing state needed for Redis-compatible filtering and reporting.

**Tech Stack:** Java 21, ActiveJ `TcpSocket` / `Eventloop`, Velo `SocketInspector`, Spock tests, Gradle 8.14+, JaCoCo.

---

## Redis Reference

Primary reference: <https://redis.io/docs/latest/commands/client-kill/>

Redis lists the current `CLIENT *` command family as:

- `CLIENT CACHING`
- `CLIENT GETNAME`
- `CLIENT GETREDIR`
- `CLIENT ID`
- `CLIENT INFO`
- `CLIENT KILL`
- `CLIENT LIST`
- `CLIENT NO-EVICT`
- `CLIENT NO-TOUCH`
- `CLIENT PAUSE`
- `CLIENT REPLY`
- `CLIENT SETINFO`
- `CLIENT SETNAME`
- `CLIENT TRACKING`
- `CLIENT TRACKINGINFO`
- `CLIENT UNBLOCK`
- `CLIENT UNPAUSE`

`CLIENT KILL` details from Redis:

- Legacy form: `CLIENT KILL ip:port`
- Filter form: `CLIENT KILL <filter> <value> ...`
- Supported filters: `ID`, `TYPE`, `USER`, `ADDR`, `LADDR`, `SKIPME`, `MAXAGE`
- Multiple filters use logical AND.
- Filter form returns the number of killed clients, including zero.
- `SKIPME YES` is the default; `SKIPME NO` allows killing the issuing connection.
- `TYPE` values are `NORMAL`, `MASTER`, `SLAVE`, `REPLICA`, and `PUBSUB`.

## Current Code Findings

- `CLIENT` dispatch lives in `src/main/java/io/velo/command/CGroup.java`.
- Current implemented subcommands in code: `GETNAME`, `ID`, `INFO`, `REPLY`, `SETINFO`, `SETNAME`, and a narrow `KILL`.
- Current `CLIENT KILL` only accepts exactly `CLIENT KILL TYPE normal`; all extra filters return syntax errors.
- `SocketInspector.socketMap` tracks connected sockets by remote address.
- `SocketInspector.getClientInfo(socket)` already emits a Redis-like single-client info line.
- `VeloUserDataInSocket` stores client name, auth user, RESP version, lib name/version, command count, connected time, last command time, and replication-client marker.
- `doc/redis_command_support.md` currently marks only `CLIENT GETNAME`, `CLIENT ID`, `CLIENT REPLY`, and `CLIENT SETNAME` as supported.
- Focused tests are in `src/test/groovy/io/velo/command/CGroupTest.groovy`.

## Compatibility Boundary

Implement real behavior where Velo already tracks enough state:

- `CLIENT ID`
- `CLIENT INFO`
- `CLIENT LIST`
- `CLIENT GETNAME`
- `CLIENT SETNAME`
- `CLIENT REPLY`
- `CLIENT SETINFO`
- `CLIENT KILL`

Return explicit unsupported-command errors for Redis client-side caching / tracking features until Velo has invalidation tracking:

- `CLIENT CACHING`
- `CLIENT GETREDIR`
- `CLIENT TRACKING`
- `CLIENT TRACKINGINFO`

Implement no-op compatibility only if Redis clients commonly issue the command and a no-op does not lie about behavior:

- `CLIENT NO-EVICT`
- `CLIENT NO-TOUCH`

Do not implement `CLIENT PAUSE`, `CLIENT UNPAUSE`, or `CLIENT UNBLOCK` as no-ops. Those affect server scheduling and blocking command semantics; unsupported is safer until there is real request admission and blocked-client state.

## Implementation Tasks

### Task 1: Add a focused CLIENT command test matrix

**Files:**

- Modify: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Split broad `test client` coverage into focused sections**

Keep existing setup helpers, but add dedicated Spock methods:

```groovy
def 'test client metadata subcommands'() {
    given:
    def fixture = clientFixture()
    def cGroup = fixture.cGroup

    expect:
    cGroup.execute('client id') instanceof IntegerReply
    cGroup.execute('client info') instanceof BulkReply
    cGroup.execute('client getname') == NilReply.INSTANCE
    cGroup.execute('client setname app') == OKReply.INSTANCE
    cGroup.execute('client getname') instanceof BulkReply

    cleanup:
    fixture.cleanup()
}
```

Add a local fixture helper inside the test class:

```groovy
private Map clientFixture() {
    def eventloopCurrent = Eventloop.builder()
            .withCurrentThread()
            .withIdleInterval(Duration.ofMillis(100))
            .build()
    Eventloop[] eventloopArray = [eventloopCurrent]
    def inspector = new SocketInspector()
    inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
    BlockingList.initBySlotWorkerEventloopArray(eventloopArray)
    MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = new long[]{Thread.currentThread().threadId()}
    MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = new long[]{Thread.currentThread().threadId()}

    def socket = SocketInspectorTest.mockTcpSocket()
    LocalPersist.instance.setSocketInspector(inspector)
    def cGroup = new CGroup(null, null, socket)
    cGroup.from(BaseCommand.mockAGroup())

    [
            inspector: inspector,
            socket: socket,
            cGroup: cGroup,
            cleanup: {
                inspector.onDisconnect(socket)
                LocalPersist.instance.setSocketInspector(null)
            }
    ]
}
```

**Step 2: Run the focused baseline**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client*"
```

Expected: current tests pass before new assertions are added.

**Step 3: Commit test reshaping only if it is large**

```bash
git add src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "test: split client command coverage"
```

### Task 2: Implement `CLIENT LIST`

**Files:**

- Modify: `src/main/java/io/velo/command/CGroup.java`
- Modify: `src/main/java/io/velo/SocketInspector.java` if a helper is cleaner
- Test: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Write the failing test**

```groovy
def 'test client list'() {
    given:
    def fixture = clientFixture()
    def inspector = fixture.inspector
    def socket = fixture.socket
    def cGroup = fixture.cGroup
    inspector.onConnect(socket)

    when:
    def reply = cGroup.execute('client list')

    then:
    reply instanceof BulkReply
    new String(reply.raw) =~ /id=\d+/
    new String(reply.raw).contains('addr=')
    new String(reply.raw).contains('user=default')

    cleanup:
    fixture.cleanup()
}
```

Also add arity tests:

```groovy
expect:
cGroup.execute('client list extra') == ErrorReply.FORMAT
```

**Step 2: Run and verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client list"
```

Expected: fail because `CLIENT LIST` currently returns `NilReply.INSTANCE`.

**Step 3: Implement minimal code**

In `CGroup.client()`:

```java
if ("list".equals(subCmd)) {
    return clientList();
}
```

Add:

```java
@VisibleForTesting
Reply clientList() {
    if (data.length != 2) {
        return ErrorReply.FORMAT;
    }

    var socketInspector = localPersist.getSocketInspector();
    if (socketInspector == null) {
        return new BulkReply(new byte[0]);
    }

    var sb = new StringBuilder();
    var sockets = new ArrayList<>(socketInspector.socketMap.values());
    for (var s : sockets) {
        if (s != null) {
            sb.append(SocketInspector.getClientInfo(s));
        }
    }
    return new BulkReply(sb.toString().getBytes());
}
```

Use the existing `BulkReply` constructor shape from nearby tests/code if it differs from `byte[]`.

**Step 4: Run tests**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client list"
```

Expected: pass.

**Step 5: Inspect coverage**

Run:

```bash
python3 scripts/jacoco_cover.py io.velo.command.CGroup 88 245 --src
```

Expected: `clientList()` dispatch and body lines are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/command/CGroup.java src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "feat: support client list"
```

### Task 3: Implement full `CLIENT KILL` filter parsing

**Files:**

- Modify: `src/main/java/io/velo/command/CGroup.java`
- Modify: `src/main/java/io/velo/SocketInspector.java` only if reusable address helpers are needed
- Modify: `src/main/java/io/velo/VeloUserDataInSocket.java` only if connection type accessors are needed
- Test: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Write failing tests for Redis forms**

Add one Spock method for all parsing and no-match cases:

```groovy
def 'test client kill filter parsing'() {
    given:
    def fixture = clientFixture()
    def cGroup = fixture.cGroup

    expect:
    cGroup.execute(command) == expected

    cleanup:
    fixture.cleanup()

    where:
    command                                      | expected
    'client kill'                                | ErrorReply.FORMAT
    'client kill id'                             | ErrorReply.SYNTAX
    'client kill id not-a-number'                | ErrorReply.SYNTAX
    'client kill type whatever'                  | ErrorReply.SYNTAX
    'client kill skipme maybe'                   | ErrorReply.SYNTAX
    'client kill maxage -1'                      | ErrorReply.SYNTAX
    'client kill id 999999'                      | IntegerReply.REPLY_0
    'client kill type normal addr 127.0.0.1:1'   | IntegerReply.REPLY_0
    'client kill user unknown'                   | IntegerReply.REPLY_0
}
```

Add behavior tests for real matches:

```groovy
def 'test client kill matches address id user type and maxage'() {
    given:
    def fixture = clientFixture()
    def inspector = fixture.inspector
    def cGroup = fixture.cGroup
    def otherSocket = SocketInspectorTest.mockTcpSocket()
    SocketInspector.setAuthUser(otherSocket, 'alice')
    inspector.onConnect(otherSocket)
    def otherId = otherSocket.hashCode()
    def otherAddr = otherSocket.remoteAddress.toString()

    expect:
    cGroup.execute("client kill id ${otherId}") instanceof IntegerReply

    cleanup:
    fixture.cleanup()
}
```

Keep the existing asynchronous close test for reactor-thread correctness, and extend it for:

- legacy `CLIENT KILL ip:port`
- `CLIENT KILL ADDR ip:port`
- `CLIENT KILL TYPE normal SKIPME yes`
- `CLIENT KILL TYPE normal SKIPME no` where the issuing socket is included only when it is tracked
- `CLIENT KILL USER <username>`
- `CLIENT KILL MAXAGE 0`
- `CLIENT KILL LADDR <listen-address>` if Velo has enough local address data; otherwise test no-match until local socket address is tracked.

**Step 2: Run and verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client kill*"
```

Expected: fail because current implementation rejects all filters except exact `TYPE normal`.

**Step 3: Add a small filter model**

Inside `CGroup.java`, add a private record:

```java
private record ClientKillFilter(
        Long id,
        String type,
        String user,
        String addr,
        String laddr,
        boolean skipMe,
        Long maxAgeSeconds
) {
    static ClientKillFilter defaults() {
        return new ClientKillFilter(null, null, null, null, null, true, null);
    }
}
```

If repeated filters are allowed, let later filters override earlier filters, matching Redis's permissive parsing style. If Redis rejects duplicates in observed behavior, encode that in tests before implementation.

**Step 4: Parse legacy and filter forms**

Add helpers:

```java
private Reply clientKill() {
    if (data.length < 3) {
        return ErrorReply.FORMAT;
    }

    var parsed = parseClientKillFilter();
    if (parsed == null) {
        return ErrorReply.SYNTAX;
    }
    return killClients(parsed);
}
```

```java
private ClientKillFilter parseClientKillFilter() {
    if (data.length == 3) {
        var legacyAddr = new String(data[2]);
        if (legacyAddr.contains(":")) {
            return new ClientKillFilter(null, null, null, normalizeAddress(legacyAddr), null, true, null);
        }
    }

    Long id = null;
    String type = null;
    String user = null;
    String addr = null;
    String laddr = null;
    boolean skipMe = true;
    Long maxAgeSeconds = null;

    for (int i = 2; i < data.length; i += 2) {
        if (i + 1 >= data.length) {
            return null;
        }
        var filter = new String(data[i]).toLowerCase();
        var value = new String(data[i + 1]);
        switch (filter) {
            case "id" -> {
                try {
                    id = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            case "type" -> {
                var lowerValue = value.toLowerCase();
                if (!isSupportedClientKillType(lowerValue)) {
                    return null;
                }
                type = lowerValue;
            }
            case "user" -> user = value;
            case "addr" -> addr = normalizeAddress(value);
            case "laddr" -> laddr = normalizeAddress(value);
            case "skipme" -> {
                if ("yes".equalsIgnoreCase(value)) {
                    skipMe = true;
                } else if ("no".equalsIgnoreCase(value)) {
                    skipMe = false;
                } else {
                    return null;
                }
            }
            case "maxage" -> {
                try {
                    maxAgeSeconds = Long.parseLong(value);
                    if (maxAgeSeconds < 0) {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            default -> {
                return null;
            }
        }
    }

    return new ClientKillFilter(id, type, user, addr, laddr, skipMe, maxAgeSeconds);
}
```

Use Java 21 switch formatting consistent with the repository. Replace `normalizeAddress` with the exact implementation chosen in Step 5.

**Step 5: Implement matching**

Add helpers:

```java
private boolean matchesClientKillFilter(ITcpSocket candidate, ClientKillFilter filter) {
    if (filter.skipMe && candidate == socket) {
        return false;
    }
    if (filter.id != null && candidate.hashCode() != filter.id) {
        return false;
    }
    if (filter.type != null && !filter.type.equals(clientType(candidate))) {
        return false;
    }
    if (filter.user != null) {
        var authUser = SocketInspector.getAuthUser(candidate);
        var user = authUser == null ? "default" : authUser;
        if (!filter.user.equals(user)) {
            return false;
        }
    }
    if (filter.addr != null && !filter.addr.equals(remoteAddress(candidate))) {
        return false;
    }
    if (filter.laddr != null && !filter.laddr.equals(localAddress(candidate))) {
        return false;
    }
    if (filter.maxAgeSeconds != null) {
        var ud = SocketInspector.createUserDataIfNotSet(candidate);
        var ageSeconds = (System.currentTimeMillis() - ud.getConnectedTimeMillis()) / 1000;
        if (ageSeconds < filter.maxAgeSeconds) {
            return false;
        }
    }
    return true;
}
```

Add `public long getConnectedTimeMillis()` to `VeloUserDataInSocket` if needed.

Define `clientType(candidate)` conservatively:

- `replica` for sockets whose user data has `getReplPairAsSlaveInTcpClient() != null`
- `normal` for ordinary command connections
- `pubsub` only if `SocketInspector` can identify subscription membership for that socket
- `master` / `slave` return no matches until Velo tracks those server-side connection roles

The important behavior is that unsupported types do not accidentally match normal clients.

**Step 6: Implement reactor-safe close**

Keep the existing close scheduling pattern:

```java
private boolean closeClientSocket(ITcpSocket s, InetSocketAddress addr) {
    try {
        s.getReactor().submit(() -> {
            try {
                s.close();
            } catch (Exception ex) {
                log.warn("Client kill close error, addr={}, msg={}", addr, ex.getMessage());
            }
        });
        return true;
    } catch (Exception e) {
        log.warn("Client kill error, addr={}, msg={}", addr, e.getMessage());
        return false;
    }
}
```

**Step 7: Run tests**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client kill*"
```

Expected: pass.

**Step 8: Inspect coverage**

Run:

```bash
python3 scripts/jacoco_cover.py io.velo.command.CGroup 170 285 --src
python3 scripts/jacoco_cover.py io.velo.VeloUserDataInSocket 1 220 --src
```

Expected: parsing branches, match branches, and reactor-safe close lines are covered.

**Step 9: Commit**

```bash
git add src/main/java/io/velo/command/CGroup.java src/main/java/io/velo/VeloUserDataInSocket.java src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "feat: support client kill filters"
```

### Task 4: Harden existing metadata subcommands

**Files:**

- Modify: `src/main/java/io/velo/command/CGroup.java`
- Modify: `src/main/java/io/velo/SocketInspector.java`
- Test: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Write failing arity and behavior tests**

```groovy
def 'test client metadata arity'() {
    given:
    def fixture = clientFixture()
    def cGroup = fixture.cGroup

    expect:
    cGroup.execute(command) == expected

    cleanup:
    fixture.cleanup()

    where:
    command                          | expected
    'client id extra'                | ErrorReply.FORMAT
    'client info extra'              | ErrorReply.FORMAT
    'client getname extra'           | ErrorReply.FORMAT
    'client setname'                 | ErrorReply.FORMAT
    'client reply'                   | ErrorReply.FORMAT
    'client reply invalid extra'     | ErrorReply.FORMAT
    'client setinfo'                 | ErrorReply.FORMAT
    'client setinfo lib-version 1.0' | ErrorReply.SYNTAX
}
```

**Step 2: Run and verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client metadata arity"
```

Expected: fail for currently permissive `CLIENT ID` and `CLIENT INFO` if they accept extra arguments.

**Step 3: Implement strict arity**

In `CGroup.client()`:

```java
if ("id".equals(subCmd)) {
    if (data.length != 2) {
        return ErrorReply.FORMAT;
    }
    return new IntegerReply(socket.hashCode());
}

if ("info".equals(subCmd)) {
    if (data.length != 2) {
        return ErrorReply.FORMAT;
    }
    var clientInfo = SocketInspector.getClientInfo(socket);
    return new BulkReply(clientInfo);
}
```

**Step 4: Verify**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client metadata*"
```

Expected: pass.

**Step 5: Inspect coverage**

Run:

```bash
python3 scripts/jacoco_cover.py io.velo.command.CGroup 88 170 --src
```

Expected: strict arity branches are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/command/CGroup.java src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "fix: validate client metadata arity"
```

### Task 5: Add no-op compatibility for safe subcommands

**Files:**

- Modify: `src/main/java/io/velo/command/CGroup.java`
- Test: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Write failing tests**

```groovy
def 'test client safe compatibility subcommands'() {
    given:
    def fixture = clientFixture()
    def cGroup = fixture.cGroup

    expect:
    cGroup.execute('client no-evict on') == OKReply.INSTANCE
    cGroup.execute('client no-evict off') == OKReply.INSTANCE
    cGroup.execute('client no-touch on') == OKReply.INSTANCE
    cGroup.execute('client no-touch off') == OKReply.INSTANCE
    cGroup.execute('client no-evict maybe') == ErrorReply.SYNTAX
    cGroup.execute('client no-touch maybe') == ErrorReply.SYNTAX

    cleanup:
    fixture.cleanup()
}
```

**Step 2: Run and verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client safe compatibility subcommands"
```

Expected: fail because unknown subcommands currently return `NilReply.INSTANCE`.

**Step 3: Implement no-op handlers**

```java
if ("no-evict".equals(subCmd) || "no-touch".equals(subCmd)) {
    if (data.length != 3) {
        return ErrorReply.FORMAT;
    }
    var value = new String(data[2]).toLowerCase();
    if (!"on".equals(value) && !"off".equals(value)) {
        return ErrorReply.SYNTAX;
    }
    return OKReply.INSTANCE;
}
```

Document in a short comment that Velo does not evict keys or update LRU/LFU state from reads in the Redis sense, so these connection flags are accepted as compatibility no-ops.

**Step 4: Verify and inspect coverage**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client safe compatibility subcommands"
python3 scripts/jacoco_cover.py io.velo.command.CGroup 88 180 --src
```

Expected: pass and covered.

**Step 5: Commit**

```bash
git add src/main/java/io/velo/command/CGroup.java src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "feat: accept safe client compatibility flags"
```

### Task 6: Return explicit unsupported errors for stateful CLIENT features

**Files:**

- Modify: `src/main/java/io/velo/command/CGroup.java`
- Test: `src/test/groovy/io/velo/command/CGroupTest.groovy`

**Step 1: Write failing tests**

```groovy
def 'test client unsupported stateful subcommands'() {
    given:
    def fixture = clientFixture()
    def cGroup = fixture.cGroup

    expect:
    cGroup.execute(command) instanceof ErrorReply

    cleanup:
    fixture.cleanup()

    where:
    command << [
            'client caching yes',
            'client getredir',
            'client tracking on',
            'client trackinginfo',
            'client pause 10',
            'client unpause',
            'client unblock 1'
    ]
}
```

**Step 2: Run and verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client unsupported stateful subcommands"
```

Expected: fail because unknown subcommands currently return `NilReply.INSTANCE`.

**Step 3: Implement explicit unsupported handling**

Prefer an existing unsupported error reply if the codebase has one. If not, use a standard error string pattern already used by command groups:

```java
private static final ErrorReply CLIENT_SUBCOMMAND_UNSUPPORTED =
        new ErrorReply("ERR CLIENT subcommand is not supported");
```

Dispatch:

```java
if ("caching".equals(subCmd)
        || "getredir".equals(subCmd)
        || "tracking".equals(subCmd)
        || "trackinginfo".equals(subCmd)
        || "pause".equals(subCmd)
        || "unpause".equals(subCmd)
        || "unblock".equals(subCmd)) {
    return CLIENT_SUBCOMMAND_UNSUPPORTED;
}
```

Keep truly unknown subcommands as `NilReply.INSTANCE` only if that is a project-wide pattern; otherwise change unknown `CLIENT` subcommands to `ErrorReply.SYNTAX` in a separate test-backed commit.

**Step 4: Verify and inspect coverage**

Run:

```bash
./gradlew :test --tests "io.velo.command.CGroupTest.test client unsupported stateful subcommands"
python3 scripts/jacoco_cover.py io.velo.command.CGroup 88 180 --src
```

Expected: pass and covered.

**Step 5: Commit**

```bash
git add src/main/java/io/velo/command/CGroup.java src/test/groovy/io/velo/command/CGroupTest.groovy
git commit -m "fix: reject unsupported client subcommands"
```

### Task 7: Update command support documentation

**Files:**

- Modify: `doc/redis_command_support.md`

**Step 1: Update the CLIENT entries**

Add or update entries near the existing client section:

```markdown
- [x] CLIENT CACHING
- [√] CLIENT GETNAME
- [x] CLIENT GETREDIR
- [√] CLIENT ID
- [√] CLIENT INFO
- [√] CLIENT KILL
- [√] CLIENT LIST
- [√] CLIENT NO-EVICT
- [√] CLIENT NO-TOUCH
- [x] CLIENT PAUSE
- [√] CLIENT REPLY
- [√] CLIENT SETINFO
- [√] CLIENT SETNAME
- [x] CLIENT TRACKING
- [x] CLIENT TRACKINGINFO
- [x] CLIENT UNBLOCK
- [x] CLIENT UNPAUSE
```

Use `[x]` for explicitly unsupported behavior unless this document distinguishes unsupported from unimplemented.

**Step 2: Verify documentation diff**

Run:

```bash
git diff -- doc/redis_command_support.md
```

Expected: only CLIENT support matrix changes.

**Step 3: Commit**

```bash
git add doc/redis_command_support.md
git commit -m "docs: update client command support"
```

### Task 8: Final verification

**Files:**

- Verify: `src/main/java/io/velo/command/CGroup.java`
- Verify: `src/main/java/io/velo/SocketInspector.java`
- Verify: `src/main/java/io/velo/VeloUserDataInSocket.java`
- Verify: `src/test/groovy/io/velo/command/CGroupTest.groovy`
- Verify: `doc/redis_command_support.md`

**Step 1: Run focused tests**

```bash
./gradlew :test --tests "io.velo.command.CGroupTest"
```

Expected: pass.

**Step 2: Run compile**

```bash
./gradlew :compileJava :compileTestGroovy
```

Expected: pass.

If `:compileTestGroovy` is not a valid task in this build, run:

```bash
./gradlew :compileTestJava :testClasses
```

**Step 3: Inspect coverage**

```bash
python3 scripts/jacoco_cover.py io.velo.command.CGroup 88 300 --src
python3 scripts/jacoco_cover.py io.velo.SocketInspector 130 210 --src
python3 scripts/jacoco_cover.py io.velo.VeloUserDataInSocket 1 240 --src
```

Expected: all changed lines and important `CLIENT KILL` branches are executed. If any changed branch is not covered, add a focused Spock case before merging.

**Step 4: Check diff quality**

```bash
git diff --check
git status --short
```

Expected: no whitespace errors and only intended files changed.

**Step 5: Optional Redis compatibility smoke test**

If a local Velo server and `redis-cli` are available:

```bash
redis-cli -p 7379 CLIENT ID
redis-cli -p 7379 CLIENT INFO
redis-cli -p 7379 CLIENT LIST
redis-cli -p 7379 CLIENT SETNAME velo-test
redis-cli -p 7379 CLIENT GETNAME
redis-cli -p 7379 CLIENT KILL ID 999999
redis-cli -p 7379 CLIENT KILL TYPE normal SKIPME yes MAXAGE 0
```

Expected: Redis-shaped replies; `CLIENT KILL ID 999999` returns `0`.

## Open Decisions

- Whether unknown `CLIENT <subcommand>` should remain `NilReply.INSTANCE` for consistency with current command groups, or become `ErrorReply.SYNTAX` for Redis compatibility.
- Whether `CLIENT KILL TYPE pubsub` should match subscribed sockets now by scanning `SocketInspector` subscription maps, or remain no-match until subscription metadata has a socket-to-type helper.
- Whether `CLIENT KILL LADDR` can be exact today. `SocketInspector.getClientInfo()` currently reports `ConfForGlobal.netListenAddresses`, not necessarily the accepted socket's concrete local address.
- Whether `CLIENT PAUSE` / `UNPAUSE` should be planned separately with request admission control across net workers and slot workers.

---

## Review Feedback (2026-06-22)

Reviewer cross-checked the plan against `AGENTS.md`, `doc/how_to_write_high_coverage_test_cases_for_commands.md`, `src/main/java/io/velo/command/CGroup.java`, `src/main/java/io/velo/SocketInspector.java`, `src/main/java/io/velo/VeloUserDataInSocket.java`, `src/main/java/io/velo/reply/IntegerReply.java`, the existing `CGroupTest.groovy`, and `SocketInspectorTest.groovy`.

### Blocking issues

**B1. Test convention violation.** `doc/how_to_write_high_coverage_test_cases_for_commands.md` is explicit: *"For one Redis command, write one test method named `def 'test <command>'()`. Do not split one command into separate methods like `test hgetdel error cases`."* Task 1 proposes splitting `test client` into ~7 separate Spock methods (`test client list`, `test client kill filter parsing`, `test client metadata arity`, `test client safe compatibility subcommands`, `test client unsupported stateful subcommands`, …). This directly contradicts the project guideline. All new `when/then` cases must be appended to the existing single `def 'test client'()` in `CGroupTest.groovy:59`. The proposed `clientFixture()` helper is still useful — extract the setup, but keep one test method.

**B2. `IntegerReply.REPLY_0` / `REPLY_1` equality is broken in proposed tests.** `IntegerReply` does not override `equals` (`src/main/java/io/velo/reply/IntegerReply.java:9`), and `IntegerReply.REPLY_0` is an anonymous `Reply` subclass while the implementation returns `new IntegerReply(0)` — a different class and instance. Groovy `==` falls back to `Object.equals` → reference inequality. Rows like:

```groovy
'client kill id 999999'                      | IntegerReply.REPLY_0
'client kill type normal addr 127.0.0.1:1'   | IntegerReply.REPLY_0
'client kill user unknown'                   | IntegerReply.REPLY_0
```

will fail even after a correct implementation. The existing test at `CGroupTest.groovy:196-198` already shows the correct pattern:

```groovy
reply instanceof IntegerReply
(reply as IntegerReply).integer == 1
```

Every REPLY_0/REPLY_1 row in Task 3 must be rewritten as a `when/then` block using `instanceof` + `.integer`, or dropped from the `where` table.

### Incomplete / undefined code

**B3. Helper methods referenced but never defined.** Task 3 Step 5 calls `normalizeAddress(...)`, `remoteAddress(candidate)`, `localAddress(candidate)`, `clientType(candidate)`. The plan says "Replace `normalizeAddress` with the exact implementation chosen in Step 5" — but Step 5 still doesn't define it. Either inline concrete implementations or scope each helper out with an explicit SYNTAX / no-match return.

**B4. Outer iteration in `killClients()` is never shown.** Only `matchesClientKillFilter(candidate, filter)` is sketched. The existing code at `CGroup.java:212-243` iterates `socketMap.keySet()` and looks up each socket; the plan must specify the new iteration strategy (and how `closeClientSocket()` reuses the reactor-safe close pattern) so the snapshot semantics are unambiguous.

**B5. `CLIENT KILL TYPE pubsub` is half-specified.** Task 3 Step 5 says "pubsub only if SocketInspector can identify subscription membership for that socket" — that requires scanning `subscribeByChannel` (`SocketInspector.java:398`). Either commit to the scan (with a perf note — it's O(channels × subscribers)) or omit `pubsub` from `isSupportedClientKillType` and document it as unsupported.

### Correctness concerns

**B6. LADDR matching is not reliable today.** `SocketInspector.getClientInfo()` (`SocketInspector.java:193-194`) reports `laddr=ConfForGlobal.netListenAddresses`, i.e. the configured listen address list, not the accepted socket's concrete local address. The "Open Decisions" section acknowledges this, but Task 3 Step 5 still codes `filter.laddr.equals(localAddress(candidate))` as if it were exact. Either gate LADDR behind a TODO (return SYNTAX or always-no-match until real local-address capture exists), or implement local-address capture in this plan.

**B7. `CLIENT ID` uses `socket.hashCode()`.** Redis IDs are monotonic 64-bit integers unique for the lifetime of a server. `hashCode()` is not unique across restarts and can collide (two live sockets with the same hash would both match `CLIENT KILL ID <x>`). Since the plan exposes `CLIENT KILL ID`, this can kill the wrong connection. Recommend allocating a monotonic `AtomicLong` in `SocketInspector` and stamping it into `VeloUserDataInSocket` at `onConnect`. If kept as `hashCode()`, add an explicit "known limitation" note.

**B8. `CLIENT LIST` arity is stricter than Redis.** Real Redis accepts `CLIENT LIST TYPE normal` / `CLIENT LIST ID 1,2,3`. Task 2 only tests `client list extra → FORMAT`. That is fine as a first cut, but the task description should say "no filters supported yet" instead of implying full compatibility.

### Test design gaps

**B9. SKIPME NO test cannot exercise the issuing socket.** In `clientFixture()`, the issuing `socket` is never registered via `inspector.onConnect(socket)` (it's a test-only socket outside `socketMap`). Therefore SKIPME NO has no extra candidate to match and the test degenerates into the SKIPME YES case. To exercise SKIPME NO meaningfully, register the issuing socket in `socketMap` first.

**B10. Behavior assertions are too weak.** `cGroup.execute("client kill id ${otherId}") instanceof IntegerReply` passes even when 0 clients are killed. Assert `(reply as IntegerReply).integer == 1` and verify `inspector.socketMap.size()` shrank by one after the kill.

**B11. Cleanup leaks `otherSocket`.** The behavior tests register `otherSocket` via `inspector.onConnect`, but `fixture.cleanup()` only disconnects the main socket. Add `inspector.onDisconnect(otherSocket)` to the cleanup, or extend the fixture to track all registered sockets and disconnect them all.

**B12. Missing parse-table rows.** Task 3's `where` table omits at minimum:
- `'client kill type'` (data.length=3, "type" has no colon, should fall through to filter loop and return SYNTAX)
- `'client kill maxage 0'` and `'client kill maxage abc'` — Step 1's prose lists `MAXAGE 0` as a required test but the table only covers `-1`.

**B13. Task 4's "expected to fail" framing is overstated.** Most rows (`getname extra`, `setname`, `reply invalid extra`, `setinfo lib-version 1.0`) already pass on current code. Only `client id extra` and `client info extra` are genuinely new arity checks. Reword Step 2 so the implementing agent doesn't think the baseline is broken when most rows are already green.

### Minor / nits

**N1.** Task 5 should also cover arity-2 cases (`'client no-evict'`, `'client no-touch'`) → FORMAT.

**N2.** Task 6 leaves truly unknown subcommands as `NilReply.INSTANCE`. Redis returns `ERR Unknown CLIENT subcommand`. Decide explicitly — don't silently preserve the current NilReply behavior.

**N3.** Task 7 doc table marks `CLIENT GETNAME / ID / REPLY / SETNAME` as `[√]`, but `doc/redis_command_support.md:59-62` already does. The actual diff should add INFO / LIST / KILL / SETINFO / NO-EVICT / NO-TOUCH lines and fix the missing KILL entry (Sentinel-era code already implements `CLIENT KILL TYPE normal`, so the current doc is out of date).

**N4.** Commit messages follow the project's conventional style (`fix:`, `feat:`, `doc:`) — consistent with recent history (`git log --oneline -15`). Good.

### Required changes before execution

1. Collapse all new test cases into the single existing `def 'test client'()` method (`CGroupTest.groovy:59`); keep `clientFixture()` as a private helper.
2. Rewrite every `IntegerReply.REPLY_0` / `REPLY_1` assertion to use `instanceof` + `.integer` (or a `when/then` block).
3. Define the deferred helpers (`normalizeAddress`, `remoteAddress`, `localAddress`, `clientType`) inline, or scope each out with explicit SYNTAX / no-match returns.
4. Decide LADDR and TYPE pubsub concretely — either implement or omit.
5. Strengthen KILL behavior assertions to check the actual kill count and `socketMap.size()` delta.
6. Add the missing parse-table rows from B12.
7. Either upgrade `CLIENT ID` to a monotonic ID or document the `hashCode()` limitation.

### Overall assessment

The compatibility boundary (real / unsupported / no-op) is well-reasoned, and the per-task TDD + JaCoCo discipline matches `AGENTS.md`. The structural problems are concentrated in the test layer (convention split + `IntegerReply` equality) and in under-specified helpers (B3, B4, B5). Fixing B1–B5 before execution will avoid most rework; B6–B13 can be addressed inside the relevant tasks.
