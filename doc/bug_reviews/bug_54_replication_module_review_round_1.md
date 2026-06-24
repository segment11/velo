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
