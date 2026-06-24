# Replication Module Bug Review (Round 1)

Date: 2026-06-24
Author: AI agent 1

Scope: replication and failover review against the current workspace. This review intentionally avoids re-reporting
previously documented fixed issues such as data-carrying `allCaughtUp`, exists-phase async gating, and first-slot dict
barrier reset.

## Bug 1: incremental big-string fetch is marked done before the file is durably written, and missing files are also marked done

**Severity:** High

**Files:** `src/main/java/io/velo/command/XGroup.java:1029-1103`,
`src/main/java/io/velo/repl/ReplPair.java:669-689`

```java
var bigStringBytes = bigStringFiles.getBigStringBytes(uuid, s.bucketIndex(), s.keyHash());
if (bigStringBytes == null) {
    log.warn("Repl master fetch incremental big string, uuid={}, key={}, slot={}, big string bytes is null", uuid, key, slot);
    bigStringBytes = new byte[0];
}
```

```java
if (buffer.hasRemaining()) {
    ...
    oneSlot.asyncExecute(() -> {
        var bigStringFiles = oneSlot.getBigStringFiles();
        bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
        ...
    });
} else {
    log.warn("Repl slave fetch incremental big string, master file missing, uuid={}, key={}, slot={}", uuid, key, slot);
}

replPair.doneFetchBigStringUuid(uuid);
```

```java
public void doneFetchBigStringUuid(long uuid) {
    doFetchingBigStringIdList.removeIf(e -> e.uuid() == uuid);
}
```

When the master cannot read the requested big-string file, it silently encodes the response with an empty payload. The
slave logs the missing-file case but still calls `doneFetchBigStringUuid(uuid)`, removing the only in-memory tracking
entry for that file. The logical value metadata was already applied by the `XBigStrings` binlog record, so the slave can
be left with a key whose compressed value points at a big-string file that was never copied.

Even when the payload is present, the slave writes the file with `oneSlot.asyncExecute(...)` and immediately removes the
uuid from `doFetchingBigStringIdList`. If the target slot is different from the REPL connection's slot, this is
fire-and-forget work on another slot event loop. A crash, disconnect, or role switch before that task executes loses the
file while the fetch queue says it is complete.

Impact: replica reads can later fail or return corrupt/missing big-string content, and reconnect has no retry source
because both `toFetchBigStringIdList` and `doFetchingBigStringIdList` are transient in-memory queues.

Suggested fix direction: encode an explicit status in `s_incremental_big_string` so "missing on master" is not
indistinguishable from "empty payload", and only call `doneFetchBigStringUuid(uuid)` after a successful write promise
completes. For cross-slot writes, use `asyncRun(...)` and return an `AsyncReply` or otherwise retain the uuid until the
write has completed.

## Bug 2: stale slave-posted health can suppress failover indefinitely

**Severity:** High

**Files:** `src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:55-67`,
`src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:401-445`

```groovy
private final ConcurrentHashMap<String, Map<String, Map<HostAndPort, OneEndpointStatus>>> oneEndpointStatusMapByClusterNamePostBySlave = new ConcurrentHashMap<>()

void addOneEndpointStatusMapByClusterNamePostBySlave(String slaveListenAddress, Map<String, Map<HostAndPort, OneEndpointStatus>> oneEndpointStatusMapByClusterName) {
    oneEndpointStatusMapByClusterNamePostBySlave[slaveListenAddress] = oneEndpointStatusMapByClusterName
}
```

```groovy
for (entry3 in oneEndpointStatusMapByClusterNamePostBySlave.entrySet()) {
    ...
    def oneEndpointStatusBySlave = oneEndpointStatusMapByClusterName[hostAndPort]
    ...
    if (oneEndpointStatusBySlave.isPingOk()) {
        isPingOkPostBySlave = true
        break
    }
}

if (isPingOkPostBySlave) {
    continue
}

doFailover(oneClusterName, hostAndPort)
```

Followers post endpoint health to the leader in `postStatusToLeader()`, and the leader uses those slave-posted reports
to avoid failover when another node can still ping the suspected master. But the leader stores each slave's last report
forever by `slaveListenAddress`; there is no timestamp, expiry, removal when the slave stops posting, or freshness check
in `doFailoverIfNeed()`.

This means one old `PING_OK` report from any slave can keep `isPingOkPostBySlave` true after that slave is dead,
partitioned from the leader, or no longer monitoring the cluster. If the leader itself now sees the master as failed, it
will still skip `doFailover(...)` based on stale data and can leave the shard without an available master indefinitely.

Impact: a real master outage can fail to trigger promotion after a stale slave health report remains in
`oneEndpointStatusMapByClusterNamePostBySlave`.

Suggested fix direction: store a received timestamp with each slave-posted status map and ignore or remove reports older
than a small multiple of the failover check interval. Also consider pruning reports for slave listen addresses that no
longer appear in cluster metadata.


---


## Fix Notes: Bug 2 stale slave-posted health

Date: 2026-06-24
Implementer: AI agent 1

Changed FailoverManager to record a receive timestamp for each slave-posted endpoint status map. doFailoverIfNeed now calls isPingOkPostBySlaveFresh, which ignores and prunes slave reports whose receive timestamp is missing or older than the configured max age before allowing a slave-posted PING_OK to suppress failover.

Added a regression test in FailoverManagerTest that sets a short test max age, posts a PING_OK report, waits for it to expire, and verifies the report is no longer trusted.

Verification:

- Red: focused stale slave-posted endpoint status test failed with MissingPropertyException before implementation.
- Green: same focused test passed after implementation.
- Regression: ./gradlew :test --tests io.velo.repl.cluster.watch.FailoverManagerTest passed.
- Coverage: python3 scripts/jacoco_cover.py io.velo.repl.cluster.watch.FailoverManager 55 470 --src showed the changed timestamp and freshness-helper lines covered.

---

## Round 2 Author Findings

Date: 2026-06-24
Author: AI agent 1

Scope: follow-up replication review after fixing Bug 2. Per request, this round intentionally ignores the open
incremental big-string fetch finding from Bug 1.

## Bug 3: malformed `hello` address length can overflow the payload-size guard and force a huge allocation

**Severity:** High

**File:** `src/main/java/io/velo/command/XGroup.java:353-368`

```java
var len = buffer.getInt();
if (len < 0) {
    throw new IllegalArgumentException("Repl master handle error: hello address length invalid=" + len + ", slot=" + slot);
}
if (buffer.remaining() < len + 20) {
    throw new IllegalArgumentException("Repl master handle error: hello payload too short, slot=" + slot);
}
var b = new byte[len];
```

The `hello` handler validates the advertised listen-address length with `buffer.remaining() < len + 20`, but the
addition is done in signed `int` arithmetic. A peer can send a small REPL `hello` frame with `len = Integer.MAX_VALUE`.
`len + 20` overflows negative, so the "payload too short" branch is skipped even though the buffer does not contain the
address bytes. The next line then attempts to allocate `new byte[Integer.MAX_VALUE]`.

The transport-level `Repl.MAX_CONTENT_LENGTH` cap does not prevent this because the frame payload itself can be small;
only the internal length field is malicious. A bad or compromised replica connection can therefore drive the master into
an `OutOfMemoryError` / "requested array size exceeds VM limit" path instead of receiving a controlled protocol error.

Suggested fix direction: validate with overflow-safe arithmetic, for example `if (len > buffer.remaining() - 20)`, after
first confirming `buffer.remaining() >= 20`, or cast to `long` before adding. Also cap the advertised address length to a
reasonable host:port maximum before allocating.

## Bug 4: one bad slave replication-offset read can abort failover even when another replica is promotable

**Severity:** High

**Files:** `src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:241-251`,
`src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:576-587`

```groovy
long getSlaveReplOffset(Node node) {
    ...
    node.exe { jedis ->
        def text = jedis.info('replication')
        def infoLines = text.readLines()
        def targetLine = infoLines.find { line -> line.contains('slave_repl_offset:') }
        def arr = targetLine.split(':')
        return arr[1] as long
    }
}
```

```groovy
def targetSlaveNodeList = targetShard.nodes.findAll { !it.master }
...
def sortedSlaveNodeList = targetSlaveNodeList.sort { a ->
    getSlaveReplOffset(a)
}
def targetSlaveNode = sortedSlaveNodeList.getLast()
```

During failover, the manager sorts every replica by `getSlaveReplOffset(...)` and promotes the largest offset. That
offset probe has no failure isolation: a connection failure, malformed `INFO replication` response, or missing
`slave_repl_offset:` line throws out of the sort closure. There is no `try/catch` around the sort or per-candidate probe,
so one broken replica can abort the whole failover attempt before the manager evaluates other replicas in the shard.

This is especially problematic because the whole point of this path is handling failure. A shard with two replicas, one
healthy and caught up and one unreachable or returning unexpected INFO text, can fail to promote the healthy replica
because the unhealthy candidate happens to be queried during sorting.

Suggested fix direction: collect candidate offsets in an explicit loop, catching and logging per-replica failures. Filter
out candidates whose offset cannot be read, then promote the remaining candidate with the highest offset. If no candidate
has a readable offset, fail the failover with a clear log message instead of throwing from inside `sort`.

---

## Fix Notes: Bug 3 malformed hello address length

Date: 2026-06-24
Implementer: AI agent 1

Changed `XGroup.hello` to reject empty or too-long advertised listen-address lengths before allocating the address
buffer. The maximum accepted encoded address length is 256 bytes. The payload-size check now avoids signed integer
addition overflow by first requiring the fixed trailing property bytes to be present, then comparing `len` against
`buffer.remaining() - 20`.

Added a regression case in `XGroupTest.test as master handle hello` that sends a 257-byte listen address and verifies the
master returns a controlled replication error containing `hello address length invalid`.

Verification:

- Red: `./gradlew :test --tests "io.velo.command.XGroupTest.test as master handle hello"` failed before the production
  change because the malformed 257-byte address reached `HostAndPort.parse` instead of the hello length guard.
- Green: the same focused test passed after the bound check change.
- Coverage: `python3 scripts/jacoco_cover.py io.velo.command.XGroup 353 371 --src` showed the changed length guard line
  covered with all 4 branches covered.
- Whitespace: `git diff --check -- src/main/java/io/velo/command/XGroup.java src/test/groovy/io/velo/command/XGroupTest.groovy doc/bug_reviews/bug_54_replication_module_review_round_1.md` passed.

---

## Fix Notes: Bug 4 failover skips unreadable slave offsets

Date: 2026-06-24
Implementer: AI agent 1

Changed `FailoverManager.doFailoverOneShard` to probe slave replication offsets one candidate at a time. A slave whose
offset cannot be read is logged and skipped instead of aborting the whole failover. If no slave has a readable offset,
the failover attempt logs a clear warning and returns without promotion.

The selected promotion target is now the candidate with the highest successfully read offset, preserving the previous
selection rule for healthy candidates.

Added a regression test in `FailoverManagerTest` where one slave offset provider throws and another slave reports a
valid offset. The test verifies that no exception escapes and the readable slave is promoted.

Verification:

- Red: `./gradlew :test --tests "io.velo.repl.cluster.watch.FailoverManagerTest.do failover one shard skips slaves whose replication offset cannot be read"` failed before implementation with `UnallowedExceptionThrownError`, caused by `IllegalStateException: offset unavailable` escaping from the sort closure.
- Green: the same focused test passed after replacing the direct sort with per-candidate probing.
- Regression: `./gradlew :test --tests "io.velo.repl.cluster.watch.FailoverManagerTest"` passed.
- Coverage: `python3 scripts/jacoco_cover.py io.velo.repl.cluster.watch.FailoverManager 147 155 --src` covered the candidate helper constructor; `python3 scripts/jacoco_cover.py io.velo.repl.cluster.watch.FailoverManager 593 610 --src` covered the candidate collection, skip-log path, no-readable-candidate return, and max-offset selection lines.
- Whitespace: `git diff --check -- src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy src/test/groovy/io/velo/repl/cluster/watch/FailoverManagerTest.groovy doc/bug_reviews/bug_54_replication_module_review_round_1.md` passed.
