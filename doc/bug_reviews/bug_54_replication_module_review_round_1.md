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

---

## Round 3 Author Findings

Date: 2026-06-24
Author: AI agent 1

Scope: follow-up replication review after fixing Bugs 3 and 4. This round avoids re-reporting the open big-string fetch
finding from Bug 1 and the older replication review findings around frame length caps, IPv6 parsing, dyn_config stubs,
async exists/catch-up ordering, and dict barrier reset.

## Bug 5: `clusterx setnodes` persists the new cluster role before the local reset succeeds

**Severity:** High

**Files:** `dyn/src/io/velo/command/ClusterxCommand.groovy:987-1039`,
`src/main/java/io/velo/repl/cluster/MultiShard.java:127-170`,
`src/main/java/io/velo/repl/LeaderSelector.java:432-481`

```groovy
def multiShard = localPersist.multiShard
multiShard.refreshAllShards(shards, clusterVersion)
...
def leaderSelector = LeaderSelector.instance
leaderSelector.resetAsSlave(toMasterNode.host, toMasterNode.port, (e) -> {
    if (e != null) {
        log.error('Reset as slave failed', e)
        finalPromise.set(new ErrorReply('error when reset as slave: ' + e.message))
    } else {
        log.warn('Reset as slave success')
        finalPromise.set(OK)
    }
})
```

```java
public synchronized void refreshAllShards(ArrayList<Shard> shardsNew, int clusterVersion) throws IOException {
    shards.clear();
    shards.addAll(shardsNew);
    clusterCurrentEpoch = clusterVersion;
    saveMeta();
}
...
FileUtils.writeStringToFile(metaFile, metaJson, StandardCharsets.UTF_8);
localPersist.initSlotsAgainAfterMultiShardLoadedOrChanged();
```

```java
var checkMasterConfigMatch = checkMasterConfigMatch(host, port, callback);
if (!checkMasterConfigMatch) {
    MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
    return;
}
```

`clusterx setnodes` parses the incoming topology and immediately calls `multiShard.refreshAllShards(...)`. That method
replaces the in-memory shard list, writes `nodes.json`, and re-initializes slot routing before the local role transition
has succeeded. Only after that durable mutation does `setnodes()` call `LeaderSelector.resetAsMaster(...)` or
`resetAsSlave(...)`.

The slave reset path can fail before touching any slot, for example when `checkMasterConfigMatch(...)` cannot connect to
the proposed master or the master's replication configuration does not match. In that case `setnodes()` returns an error
to the caller, but the node has already persisted the new cluster metadata saying it is a slave of that master. The local
slot state can remain master/readable/binlog-on while the durable cluster view and command routing now describe the node
as a replica.

Impact: a rejected `clusterx setnodes` can leave `nodes.json` and runtime shard metadata ahead of the actual replication
role. After restart, command routing and cluster introspection can advertise the failed topology, while the per-slot
replication state never completed the demotion/promotion. This can cause writes to be rejected or routed according to a
role transition that did not happen, and can make a later retry start from an already-mutated cluster version.

Suggested fix direction: validate the parsed topology first, perform the local role transition against a staged shard
view, and only call `refreshAllShards(...)` / `saveMeta()` after `resetAsMaster` or `resetAsSlave` succeeds. If the
broadcast protocol requires metadata to be visible during reset, add rollback on callback error and tests covering
`checkMasterConfigMatch` failure after `setnodes`.

## Bug 6: demoting a master to slave leaves existing master-side replication pairs alive

**Severity:** High

**Files:** `src/main/java/io/velo/repl/LeaderSelector.java:443-463`,
`src/main/java/io/velo/persist/OneSlot.java:336-379`,
`src/main/java/io/velo/persist/OneSlot.java:1986-2008`,
`src/main/java/io/velo/command/XGroup.java:270-350`

```java
promises[i] = oneSlot.asyncRun(() -> {
    var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
    if (replPairAsSlave != null) {
        if (replPairAsSlave.getHost().equals(host) && replPairAsSlave.getPort() == port) {
            log.debug("Repl old repl pair as slave is same as new master, slot={}", oneSlot.slot());
            return;
        } else {
            oneSlot.removeReplPairAsSlave();
        }
    }

    oneSlot.resetAsSlave(host, port);
});
```

```java
public boolean removeReplPairAsSlave() {
    ...
    if (replPair.isAsMaster()) {
        continue;
    }
    replPair.bye();
    addDelayNeedCloseReplPair(replPair);
}
```

```java
public void resetAsSlave(@NotNull String host, int port) throws IOException {
    metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset();
    binlog.moveToNextSegment();
    createReplPairAsSlave(host, port);
    setReadonly(true);
    setCanRead(false);
    dynConfig.setBinlogOn(false);
}
```

When a node is demoted from master to slave, `LeaderSelector.resetAsSlave(...)` only removes an existing
`ReplPair` where the node was already acting as a slave. `OneSlot.removeReplPairAsSlave()` explicitly skips
`replPair.isAsMaster()`, and `OneSlot.resetAsSlave(...)` creates the new upstream slave connection but never closes or
removes the existing master-side `ReplPair` entries for downstream replicas.

Those master-side pairs remain visible through `getReplPairAsMasterList()` because they are not marked `bye`, and their
TCP sockets are not closed. `XGroup.handleRepl(...)` still services incoming slave-sent replication messages such as
`ping`, `hello`, `exists_*`, and `catch_up` based on the stale pair if an old downstream replica keeps talking to this
node. At the same time the slot has been set readonly, cannot read, and has binlog disabled as a slave.

Impact: after failover or `clusterx setnodes` demotes a master, old replicas can remain connected to the demoted node and
continue treating it as their master. The demoted node can keep answering replication traffic with stale or incomplete
state instead of forcing those replicas to reconnect to the promoted master. This creates a split replication topology
and can leave downstream replicas lagging permanently or catching up from the wrong node.

Suggested fix direction: during `resetAsSlave`, close and remove every active `ReplPair` where `isAsMaster()` is true
before creating the new slave pair. Use the same delayed-close mechanism or an explicit `bye`/socket close, but ensure
`getReplPairAsMasterList()` is empty after demotion. Add a regression test that creates a master-side repl pair, calls
`LeaderSelector.resetAsSlave` or `OneSlot.resetAsSlave`, and verifies the old master pair is no longer active.

---

## Review Notes: Bug 6 (AI agent 2)

Date: 2026-06-24
Reviewer: AI agent 2

**Verdict: CONFIRMED.** The finding is accurate; every cited line matches the current code and the causal chain holds.

Verification against current source:

1. `LeaderSelector.doResetAsSlave` (`LeaderSelector.java:432-487`): the per-slot `asyncRun` body (lines 448-463) calls
   `oneSlot.removeReplPairAsSlave()` only when an existing slave pair points at a *different* master, then
   `oneSlot.resetAsSlave(host, port)`. Master-side pairs are never enumerated or closed in this path.
2. `OneSlot.removeReplPairAsSlave` (`OneSlot.java:336-354`): explicitly `continue`s on `replPair.isAsMaster()`
   (lines 343-345), so by construction it cannot remove a master-side pair. The doc quote is exact.
3. `OneSlot.resetAsSlave` (`OneSlot.java:1986-2008`): clears master binlog index, moves to next segment,
   `createReplPairAsSlave`, sets readonly/can't-read/binlog-off. It never byes or removes existing master-side pairs.
4. `XGroup.handleReplInner` (`XGroup.java:242-352`): for slave-sent repl types it resolves the still-live pair via
   `oneSlot.getReplPairAsMaster(slaveUuid)` (line 246) and continues servicing `ping`/`hello`/`exists_*`/`catch_up`.
   Worse, `ping` re-creates a master pair via `createIfNotExistReplPairAsMaster` (line 282) even if one were removed, so
   a downstream replica that keeps pinging would resurrect the stale master role on the demoted node.
5. No staleness/timeout pruning exists for master-side pairs. The only removal mechanism is an explicit `bye()` plus
   `addDelayNeedCloseReplPair` (`OneSlot.java:314-326`, drained at `OneSlot.java:1025-1041`); full close otherwise only
   happens in `cleanUp()` at shutdown (`OneSlot.java:1771-1775`). The periodic ping task (`OneSlot.java:989-1023`) keeps
   iterating these pairs and treats the slot as `isAsMaster`.

Demotion entry points both reach the same gap: `clusterx setnodes` resetMySelfAsSlave branch
(`ClusterxCommand.groovy:1032-1039`) and Sentinel/leader-driven demotion both call `LeaderSelector.resetAsSlave`.

Additional notes for the implementer:
- The fix must remove master-side pairs *and* prevent immediate resurrection. Since `ping`/`createIfNotExistReplPairAsMaster`
  will recreate a master pair on the readonly slot, a one-time `bye` during `resetAsSlave` is necessary but not fully
  sufficient — consider also rejecting/`bye`-ing slave-sent repl handling when the slot is readonly + binlog-off, so a
  stubborn downstream replica is actively pushed to reconnect rather than silently re-accepted.
- The suggested `getReplPairAsMasterList()`-should-be-empty assertion is a good regression check, but note the
  delayed-close path leaves the pair in `replPairs` (marked `isSendBye`) for ~10s; the assertion should rely on
  `isSendBye()`/`getReplPairAsMasterList()` (which filters bye'd pairs) rather than raw `getReplPairs()` size.

Recommendation: proceed to fix following the Bug Fix Execution Workflow (failing test first, then close master-side
pairs in `resetAsSlave`, JaCoCo confirmation, single commit).

---

## Fix Notes: Bug 6 demotion leaves master-side repl pairs alive

Date: 2026-06-24
Implementer: AI agent 2

Added `OneSlot.removeReplPairAsMaster()` (mirror of `removeReplPairAsSlave()`), which byes and delay-closes every active
master-side `ReplPair` and skips already-byed pairs. `OneSlot.resetAsSlave(...)` now calls it before
`createReplPairAsSlave(...)`, so demotion drops all downstream-replica pairs. After demotion `getReplPairAsMasterList()`
is empty and the old pairs are routed through the existing 10s delayed-close mechanism instead of staying live.

Added a regression test `OneSlotTest.'test reset as slave removes master side repl pairs'` that registers an active
master-side pair, an already-byed master-side pair, and a slave-side pair, calls `resetAsSlave`, and asserts the active
master pair is byed and queued for close, the byed pair is not re-added, the slave pair is untouched, and the new upstream
slave pair exists.

Verification:

- Red: `./gradlew :test --tests "io.velo.persist.OneSlotTest.test reset as slave removes master side repl pairs"` failed
  at the `getReplPairAsMasterList().isEmpty()` assertion before the production change (master pair stayed active).
- Green: the same focused test passed after adding `removeReplPairAsMaster()` to `resetAsSlave`.
- Regression: `./gradlew :test --tests "io.velo.persist.OneSlotTest" --tests "io.velo.repl.LeaderSelectorTest"` passed.
- Coverage: `python3 scripts/jacoco_cover.py io.velo.persist.OneSlot 362 380` showed all 12 lines and all 3 branch lines
  fully covered; `resetAsSlave` lines including the new `removeReplPairAsMaster()` call were covered.
- Whitespace: `git diff --check` passed for the changed Java and Groovy files.

Note: as flagged in the review, a stubborn downstream replica can still re-create a master-side pair via `ping` →
`createIfNotExistReplPairAsMaster` while the slot is readonly. This fix closes the existing pairs on demotion; actively
rejecting slave-sent repl traffic on a readonly/binlog-off slot is left as a follow-up.
