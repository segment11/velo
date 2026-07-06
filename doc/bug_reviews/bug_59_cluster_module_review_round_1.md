# Bug 59 — Cluster versioning is half-wired (controller omits version; receiver never rejects stale)

**Module:** cluster (`repl/cluster`, `dyn` cluster commands, `groovy` watch controller)
**Round:** 1 (author: automated exploration during the 2026-07-06 cluster robustness test effort)
**Status:** both findings need a human design decision before any fix — the correct
behavior depends on where the cluster version is meant to originate. Do **not**
auto-fix; see "Fix direction (open questions)".

These two findings are reported together because they are two ends of the same
missing contract: the cluster version (epoch) is neither reliably *sent* by the
controller nor *validated* by the receiver.

---

## Bug 59a — FailoverManager broadcasts `clusterx setnodes` without the required version arg (HIGH)

**Files:**
- `src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:300`
- `src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy:677`
- Receiver: `dyn/src/io/velo/command/ClusterxCommand.groovy:976` (`setnodes()` arity check) and `:994` (version parse)

**Excerpt (sender, both sites identical in shape):**
```groovy
// FailoverManager.groovy:677
byte[] r = jedis.sendCommand(command, 'setnodes'.bytes, clusterxNodesArgs.bytes) as byte[]
```
The wire command is `clusterx setnodes <nodesArgs>` → the handler receives
`data = [clusterx, setnodes, nodesArgs]`, length **3**.

**Excerpt (receiver):**
```groovy
// ClusterxCommand.groovy:976
if (data.length != 4) {
    return ErrorReply.FORMAT
}
...
def clusterVersion = new String(data[3]) as int   // :994
```
`setnodes()` hard-requires `data.length == 4` (`clusterx setnodes <nodesArgs> <version>`).

**Status update (fixed 2026-07-06):** both broadcast sites now append a cluster
version. `FailoverManager.computeNextClusterVersion(nodes)` reads each live node's
`cluster_current_epoch` (via `clusterx info`), takes the max, and returns max+1 —
always newer than any receiver's epoch, so the 59b guard accepts it, and monotonic
because each broadcast advances the nodes' epochs. The failover broadcast excludes
the failed (down) node from the epoch scan; the restart re-broadcast derives the
version from the rejoining node's own epoch. This is stateless (no per-cluster
counter needed) and matches the intended design ("collect each node's version,
choose the bigger as current, broadcast a newer one"). Test:
`FailoverManagerTest 'test compute next cluster version'` (max+1, skip-unreachable,
default-1). The live `clusterx info` read and the actual broadcast wire path need a
running redis/ZooKeeper and remain covered only by the manual/e2e harness, like the
sibling `getSlaveReplOffset`.

**Root cause:** the two `setnodes` broadcast sites — the only ones in the whole
controller (`grep` confirms lines 300 and 677 are the sole senders) — never append
the `clusterVersion` argument. A `setnodes()` was later given a mandatory 4th
version arg, and the controller broadcast was not updated to match.

**Impact:** every real (non-mock) auto-failover / server-restart re-setnodes
broadcast returns `ERR ...FORMAT`, so `setnodes()` never applies the topology and
`FailoverManager` throws `RuntimeException('failover manager clusterx setnodes
fail...')` at `:685`. In production, ZooKeeper-driven automatic failover cannot
publish the new topology. It is invisible to the test suite because
`FailoverManagerTest` runs with `mockSetNodes = true` (`:295`, `:671`), which
returns `'OK'` without ever hitting the real command decode.

**Why it wasn't caught:** no test exercises the real `setnodes` wire format from
the controller; the mock short-circuits it. `ClusterxBase` (the process-level
harness) builds its own `SETNODES` args *with* a monotonic version, so it also
never reproduces the controller's arg list.

---

## Bug 59b — Receiver accepts a stale / out-of-order cluster version (MEDIUM)

**Files:**
- `src/main/java/io/velo/repl/cluster/MultiShard.java:178-183` (`refreshAllShards`, used by `setnodes`)
- `src/main/java/io/velo/repl/cluster/MultiShard.java:191-197` (`updateClusterVersion`, used by `setslot`)

**Excerpt:**
```java
public synchronized void refreshAllShards(ArrayList<Shard> shardsNew, int clusterVersion) throws IOException {
    shards.clear();
    shards.addAll(shardsNew);
    clusterCurrentEpoch = clusterVersion;   // <-- overwrites unconditionally
    saveMeta();
}

public synchronized void updateClusterVersion(int clusterVersion) throws IOException {
    if (clusterVersion != 0) {
        clusterCurrentEpoch = clusterVersion; // <-- only guards against 0, not against regressions
    }
    saveMeta();
}
```

**Status update (fixed 2026-07-06):** receiver-side guard added. `refreshAllShards`
and `updateClusterVersion` now reject a **strictly lower** non-zero version
(`clusterVersion < clusterCurrentEpoch`); equal and higher are accepted. The
command layer (`ClusterxCommand.setnodes` / `setslot`) rejects the stale version
**before** any repl-role change or slot-range mutation, so a stale broadcast has no
side effects. Equal is accepted deliberately: the controller broadcasts one version
to N nodes and may retry, so duplicate/retried deliveries must remain idempotent
(and `test nodes` legitimately re-applies distinct topologies at the same version).
Tests: `MultiShardTest 'test cluster version rejects stale updates'`,
`ClusterxCommandTest 'test setslot ignores stale cluster version'` /
`'test setnodes ignores stale cluster version'`. **59a (controller sending the
version) is still open** — until it lands, the controller's real broadcast still
omits the version (FORMAT error), so this guard is currently exercised only by the
harness/tests, not the live controller.

**Root cause:** neither method compares the incoming `clusterVersion` against the
stored `clusterCurrentEpoch`. Any version — lower, equal, or higher — is accepted
and overwrites the epoch.

**Impact:** a delayed, duplicated, or reordered controller broadcast can roll the
topology *backwards*. Combined with the fact that `saveMeta()` auto-increments
`clusterMyEpoch` on every save (`MultiShard.java:227`), the "my epoch" and
"current epoch" already diverge, so there is no reliable monotonic guard anywhere.
This is a split-brain / stale-topology hazard: node A applies epoch 10, then a
straggler epoch-8 `setnodes` arrives and reverts ownership.

**Why a naive fix is unsafe (do not just add `if (v <= current) reject`):**
- On a node restart, the persisted `clusterCurrentEpoch` may be high while a fresh
  controller bootstrap legitimately re-issues a *low* version — strict rejection
  would break rebootstrap.
- `CLUSTERX RESET` and initial cluster creation also legitimately move the version
  down/there is no established "reset" semantics.
- Bug 59a means the controller currently doesn't even send a coherent version, so
  the receiver has nothing trustworthy to compare against yet.

---

## Fix direction (open questions — needs human decision)

The real fix is to define one coherent cluster-version contract, then wire both
ends to it:

1. **Where does the authoritative version come from?** A controller-owned
   monotonic counter (persisted in ZooKeeper), incremented per topology change,
   and included in *every* `setnodes`/`setslot` broadcast? Or derived from
   `clusterMyEpoch`?
2. **What is the receiver's acceptance rule?** Redis-style "accept only strictly
   newer epoch, except on explicit RESET/bootstrap"? Define the bootstrap/reset
   escape hatch explicitly so 59b's fix doesn't break restart.
3. **Fix 59a** by appending the chosen version to both broadcast sites (and add a
   test that drives the *real* `setnodes` decode, not the mock).
4. **Fix 59b** by adding the acceptance rule to `refreshAllShards` /
   `updateClusterVersion` once the version source is trustworthy.

Recommended: resolve 59a first (it is a clear, currently-broken path), decide the
version-source design, then 59b becomes a well-defined monotonicity guard.

## Suggested regression tests (once behavior is decided)
- Controller `setnodes` broadcast round-trips through the real command decode and
  returns OK (kills 59a; must not rely on `mockSetNodes`).
- `refreshAllShards` / `updateClusterVersion` reject a strictly-lower version
  during normal operation but accept a bootstrap/reset (kills 59b) — this is plan
  item **T3** in `docs/plans/2026-07-06-cluster-robustness-test-plan.md`.
