# Redis Sentinel Auto-Failover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Velo work as a Redis Sentinel-managed master/replica data node so external Redis Sentinel processes can monitor, promote, demote, and reconfigure Velo instances during automatic failover.

**Architecture:** Keep Redis Sentinel as the external high-availability controller. Velo should not reimplement Sentinel election; it should expose Redis-compatible replication metadata and execute Sentinel-issued `SLAVEOF` / `REPLICAOF`, `SLAVEOF NO ONE`, and client-disconnect commands safely. Treat Velo cluster-mode failover and ZooKeeper leadership as separate controllers unless explicitly bridged later.

**Tech Stack:** Java 21, Groovy command scripts, ActiveJ event loops, Velo `X-REPL`, Spock tests, Gradle 8.14+, JaCoCo.

---

## Current Code Findings

- `SENTINEL` is intentionally unsupported on data nodes: `src/main/java/io/velo/command/SGroup.java:452`.
- `SLAVEOF` exists and delegates to `LeaderSelector.resetAsMaster(true, ...)` for `NO ONE`, or `LeaderSelector.resetAsSlave(host, port, ...)` for a new master: `src/main/java/io/velo/command/SGroup.java:464`.
- `REPLICAOF` aliases to `SLAVEOF`: `src/main/java/io/velo/command/RGroup.java:111`.
- `ROLE` exists and reports master/slave role plus offsets: `src/main/java/io/velo/command/RGroup.java:409`.
- `INFO replication` exists in Groovy and already exposes Sentinel-relevant fields such as `role`, `connected_slaves`, `master_host`, `master_port`, `master_link_status`, `master_repl_offset`, `slave_repl_offset`, and `slave_priority`: `dyn/src/io/velo/command/InfoCommand.groovy:198`.
- Promotion and demotion flow through `LeaderSelector`: `src/main/java/io/velo/repl/LeaderSelector.java:289` and `src/main/java/io/velo/repl/LeaderSelector.java:389`.
- Per-slot role reset changes readonly, can-read, binlog, and replication-pair state: `src/main/java/io/velo/persist/OneSlot.java:1952`.
- `ConfForGlobal.zookeeperRootPath` is currently documented as also used as a Sentinel master name, but Sentinel mode should get explicit config to avoid coupling it to ZooKeeper naming.

## Compatibility Boundary

Redis Sentinel manages non-clustered Redis-style master/replica groups. Initial Velo support should target standalone Velo master/replica groups.

For Velo cluster mode, either:

- run one Sentinel master group per Velo shard, or
- keep using Velo's `clusterx` / ZooKeeper failover paths.

Do not let Sentinel and Velo's internal failover controller promote or demote the same node at the same time.

When `sentinelModeEnabled=true`, Velo must not start ZooKeeper leader-election latches or use
`LeaderSelector.tryConnectAndGetMasterListenAddress()` to discover/promote a master. Sentinel is the topology controller in
that mode. `LeaderSelector.resetAsMaster(...)` and `resetAsSlave(...)` may still be used as local role-transition helpers for
Sentinel-issued `SLAVEOF` / `REPLICAOF` commands.

## Implementation Tasks

### Task 1: Add Sentinel Compatibility Regression Tests

**Files:**

- Modify: `src/test/groovy/io/velo/command/SGroupTest.groovy`
- Modify: `src/test/groovy/io/velo/command/RGroupTest.groovy`
- Modify: `src/test/groovy/io/velo/command/InfoCommandTest.groovy` if present, otherwise add coverage in the existing INFO command test file.

**Steps:**

1. Add tests for `SLAVEOF host port`, `SLAVEOF NO ONE`, `REPLICAOF host port`, and `REPLICAOF NO ONE`.
2. Add tests for Redis-compatible errors: wrong arity, invalid port, invalid host, and unsupported Sentinel data-node command.
3. Add tests for `ROLE` reply shape in master mode and slave mode.
4. Add tests for `INFO replication` fields in master mode, slave mode, with zero replicas, and with at least one replica.
5. Run focused tests:

```bash
./gradlew :test --tests "io.velo.command.SGroupTest.test slaveof" --tests "io.velo.command.RGroupTest.test handle - readonly readwrite role" --tests "*InfoCommand*"
```

Expected before implementation: failures for any missing Sentinel-compatible behavior.

### Task 2: Make `SLAVEOF` / `REPLICAOF` Fully Redis-Compatible

**Files:**

- Modify: `src/main/java/io/velo/command/SGroup.java`
- Modify: `src/main/java/io/velo/command/RGroup.java` only if alias behavior needs adjustment.
- Test: `src/test/groovy/io/velo/command/SGroupTest.groovy`
- Test: `src/test/groovy/io/velo/command/RGroupTest.groovy`

**Steps:**

1. Support `NO ONE` only when both arguments are present and match Redis semantics.
2. Support hostnames and announced names, not only IPv4 literals. Keep port validation strict.
3. Keep `REPLICAOF` as a strict alias for `SLAVEOF`.
4. Ensure repeated `SLAVEOF` to the same master is idempotent.
5. Preserve existing ZooKeeper master-switch publish behavior while changing command parsing.
6. Run focused command tests.
7. Inspect JaCoCo HTML for `SGroup.slaveof()` and `RGroup.handle()`.
8. Commit:

```bash
git add src/main/java/io/velo/command/SGroup.java src/main/java/io/velo/command/RGroup.java src/test/groovy/io/velo/command/SGroupTest.groovy src/test/groovy/io/velo/command/RGroupTest.groovy
git commit -m "fix: improve sentinel replica commands"
```

### Task 3: Make `ROLE` and `INFO replication` Sentinel-Grade

**Files:**

- Modify: `src/main/java/io/velo/command/RGroup.java`
- Modify: `dyn/src/io/velo/command/InfoCommand.groovy`
- Test: existing command tests for `ROLE` and `INFO`.

**Steps:**

1. Verify `ROLE` reply shape against Redis: master returns role, master offset, and connected replica list; slave returns role, master host, master port, link state, and replication offset.
2. Ensure `INFO replication` uses monotonic offsets and does not report placeholder values such as `master_repl_offset:-1` for a healthy master with no replicas.
3. Ensure `slave_priority` / `replica_priority` behavior is explicit and configurable.
4. Use existing `ValkeyRawConfSupport.replicaPriority` where possible before adding new priority state.
5. Ensure `master_link_status` and `ROLE` slave link state report actual replication link state instead of hard-coded healthy values.
6. Run focused tests.
7. Inspect JaCoCo HTML for changed Java lines and Groovy coverage where available.
8. Commit:

```bash
git add src/main/java/io/velo/command/RGroup.java dyn/src/io/velo/command/InfoCommand.groovy src/test/groovy
git commit -m "fix: align replication metadata with sentinel"
```

### Task 4: Add Sentinel Runtime Configuration

**Files:**

- Modify: `src/main/java/io/velo/ConfForGlobal.java`
- Modify: Velo properties parsing files that load global config.
- Modify: docs or sample properties under `src/main/resources/` if applicable.
- Test: relevant config tests.

**Config Items:**

- `sentinelMasterName`
- `replicaPriority`, using or bridging existing `ValkeyRawConfSupport.replicaPriority`
- `replicaAnnounceIp`
- `replicaAnnouncePort`
- `sentinelModeEnabled`
- authentication fields needed for Sentinel and replicas to connect consistently, including Redis `requirepass` / `masterauth` and ACL username/password behavior.

**Steps:**

1. Add config defaults without changing current behavior.
2. Decouple Sentinel master name from `zookeeperRootPath`.
3. Preserve existing ZooKeeper publish paths in `LeaderSelector.publishMasterSwitchMessage(...)`; those messages must keep using `zookeeperRootPath`.
4. Resolve auth semantics before landing this task so Sentinel and replicas can reconnect after promotion.
5. Wire announced host/port into `INFO replication` and `ROLE` output where Redis/Sentinel expects externally reachable addresses.
6. Run config and INFO tests.
7. Inspect JaCoCo for changed config paths.
8. Commit:

```bash
git add src/main/java/io/velo/ConfForGlobal.java src/main/resources src/test/groovy
git commit -m "feat: add sentinel runtime config"
```

### Task 5: Implement Minimal `CLIENT KILL TYPE normal`

**Files:**

- Modify: command group handling `CLIENT`, likely `src/main/java/io/velo/command/CGroup.java`.
- Modify: socket/client tracking code if needed.
- Test: relevant command and server tests.

**Steps:**

1. Add a failing test for `CLIENT KILL TYPE normal`.
2. Implement enough normal-client disconnect behavior for Redis Sentinel reconfiguration.
3. Keep unsupported `CLIENT KILL` variants explicit rather than silently accepting them.
4. Verify normal data clients are disconnected and replication/internal connections are not accidentally killed.
5. Run focused tests.
6. Inspect JaCoCo HTML for changed command and socket paths.
7. Commit:

```bash
git add src/main/java/io/velo/command/CGroup.java src/test/groovy
git commit -m "feat: support client kill for sentinel"
```

### Task 6: Harden Promotion/Demotion State and Reporting

**Files:**

- Modify: `src/main/java/io/velo/repl/LeaderSelector.java`
- Modify: `src/main/java/io/velo/persist/OneSlot.java`
- Test: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`
- Test: `src/test/groovy/io/velo/persist/OneSlotTest.groovy`

**Steps:**

1. Add tests for Sentinel promotion via `SLAVEOF NO ONE` when the replica is caught up.
2. Add tests for Sentinel promotion when the old master was not marked readonly. `SLAVEOF NO ONE` already calls
   `resetAsMaster(true, ...)`; document and verify this forced-promotion behavior rather than accidentally weakening it.
3. Add tests for demoting an old master after it returns and receives `SLAVEOF newMaster`.
4. Track failover state for `INFO replication` where useful, instead of always reporting `master_failover_state:no-failover`.
5. Ensure `sentinelModeEnabled=true` disables ZooKeeper leader election/discovery while preserving local role-transition helpers.
6. Make sure role reset is applied consistently across all slots.
7. Run focused tests.
8. Inspect JaCoCo for `LeaderSelector` and `OneSlot` changed lines.
9. Commit:

```bash
git add src/main/java/io/velo/repl/LeaderSelector.java src/main/java/io/velo/persist/OneSlot.java src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy src/test/groovy/io/velo/persist/OneSlotTest.groovy
git commit -m "fix: harden sentinel role transitions"
```

### Task 7: Persist and Restore Sentinel Role State

**Files:**

- Modify: persistence/config classes that store per-slot dynamic config and replication role.
- Modify: `src/main/java/io/velo/persist/DynConfig.java`
- Test: `src/test/groovy/io/velo/persist/DynConfigTest.groovy`
- Test: restart/e2e tests if available.

**Steps:**

1. Add tests proving a node restarted after `SLAVEOF host port` remains a replica.
2. Add tests proving a node restarted after `SLAVEOF NO ONE` remains writable master.
3. Persist master host/port, role, readonly, and relevant offsets.
4. On boot, restore replication role before accepting normal writes.
5. Run focused persistence and restart tests.
6. Inspect JaCoCo for changed persistence lines.
7. Commit:

```bash
git add src/main/java/io/velo/persist/DynConfig.java src/test/groovy/io/velo/persist/DynConfigTest.groovy
git commit -m "fix: persist sentinel role state"
```

### Task 8: Add Sentinel Command-Sequence and Integration Tests

**Files:**

- Create or modify: `src/test/groovy/io/velo/e2e/SentinelFailoverTest.groovy`
- Modify: test utilities for starting Velo and Redis Sentinel processes.
- Add: Sentinel config templates if needed.

**Primary Harness Scenario:**

Add a stable Spock test that drives Velo through the command sequence Redis Sentinel relies on:

1. `PING`
2. `INFO replication`
3. `ROLE`
4. `SLAVEOF NO ONE`
5. `SLAVEOF <new-master-host> <new-master-port>`
6. `CLIENT KILL TYPE normal`

Assert that each command returns Redis-compatible replies and that Velo's local role state changes as expected.

**Real Sentinel Scenario:**

1. Start one Velo master and two Velo replicas.
2. Start three Redis Sentinel processes monitoring the master name.
3. Verify Sentinel sees the master and replicas.
4. Kill or stop the master.
5. Wait for Sentinel to promote one replica.
6. Verify promoted Velo replies as master through `ROLE` and `INFO replication`.
7. Verify the remaining replica follows the promoted master.
8. Restart the old master and verify Sentinel demotes it to replica.
9. Write to the new master and verify data reaches replicas through Velo replication.

The real Sentinel subprocess test is valuable, but it may be slower and more brittle in CI. Make the command-sequence harness
the primary deliverable, then add the multi-process Sentinel e2e as a follow-up or gated integration test.

**Commands:**

```bash
./gradlew :test --tests "io.velo.e2e.SentinelFailoverTest"
```

After the test passes, inspect JaCoCo for the changed role-transition and command paths.

### Task 9: Document Deployment and Limits

**Files:**

- Create: `doc/redis_sentinel_compatibility.md`
- Modify: sample config files if applicable.

**Content:**

- Supported topology: standalone Velo master with Velo replicas managed by external Redis Sentinel.
- Required Sentinel config example.
- Required Velo config example.
- Expected `INFO replication` and `ROLE` output.
- Behavior of `SLAVEOF`, `REPLICAOF`, and `CLIENT KILL TYPE normal`.
- Warning that Velo cluster-mode failover and Sentinel failover are separate controllers.
- Guidance for one Sentinel master group per Velo shard if cluster deployments must use Sentinel.

**Verification:**

```bash
./gradlew :test --tests "*Sentinel*" --tests "io.velo.command.SGroupTest.test slaveof" --tests "io.velo.command.RGroupTest.test handle - readonly readwrite role"
```

Commit:

```bash
git add doc/redis_sentinel_compatibility.md src/main/resources
git commit -m "docs: add sentinel compatibility guide"
```

## Acceptance Criteria

- External Redis Sentinel can monitor Velo master and replicas using `PING`, `INFO replication`, and `ROLE`.
- Sentinel can promote a Velo replica with `SLAVEOF NO ONE`.
- Sentinel can reconfigure other Velo nodes with `SLAVEOF <new-master-host> <new-master-port>`.
- Velo returns Redis-compatible replication metadata before, during, and after failover.
- Velo disconnects normal clients when Sentinel sends `CLIENT KILL TYPE normal`.
- A demoted old master does not continue accepting writes after Sentinel reconfigures it.
- A restarted Velo node restores the correct master/replica role before serving normal writes.
- Focused tests pass and JaCoCo confirms changed lines or branches were exercised.

## Open Questions

- Should `SLAVEOF NO ONE` always force promotion for Sentinel compatibility, or should Velo expose a separate manual-command path that can reject unsafe promotion when the replica is not caught up?
- Should Velo support Sentinel-managed cluster mode at all, or document that Sentinel is standalone-only and `clusterx` remains the cluster failover path?
- Which authentication mode should be the default for Sentinel: existing password only, ACL username/password, or both?

Resolved architecture rule: when `sentinelModeEnabled=true`, Sentinel owns topology decisions. ZooKeeper leader election and
master discovery must be disabled in that mode, while local reset-as-master/reset-as-slave helpers remain available for
Sentinel-issued commands.

## Review Notes (AI agent 2 — reviewer)

I read the plan and cross-checked every cited file path and line number against the current tree. All references check out. The plan is reasonable and OK to execute as written, with the refinements below.

### Verified code references
- `SGroup.java:452` — `SENTINEL` returns `ErrorReply.NOT_SUPPORT` (intentional, data node).
- `SGroup.java:464` — `slaveof()` delegates to `LeaderSelector.resetAsMaster` / `resetAsSlave`.
- `SGroup.java:473-489` — `NO ONE` branch only inspects `data[1]`, ignores `data[2]`. Wrong for legacy `SLAVEOF NO ONE` (Redis requires "NO ONE" in both args). The plan's Task 2 fix is needed.
- `SGroup.java:491-494` — strict IPv4 regex; existing test at `SGroupTest.groovy:2372` even asserts `localhost` returns `ErrorReply.SYNTAX`. Plan's "support hostnames" is a real fix, not a refactor.
- `RGroup.java:111` — `REPLICAOF` is a strict alias to `SGroup.slaveof()`.
- `RGroup.java:409` — `role()` returns `master`/`slave` plus offsets and replica list. Reply shape already matches Redis.
- `InfoCommand.groovy:198` — replication section already emits `role`, `connected_slaves`, `master_host/port`, `master_link_status`, `master_repl_offset`, `slave_repl_offset`, `slave_priority`.
- `InfoCommand.groovy:271` — for a master with zero replicas the code emits `master_repl_offset:-1` and `second_repl_offset:-1`. This is a real bug: a healthy master with no replicas is still producing binlog and should report `currentReplOffset()`. Plan's Task 3 should call this out as a regression to fix, not just "align with Sentinel".
- `InfoCommand.groovy:234` — `master_failover_state:no-failover` is hard-coded. Plan's Task 6 should wire this to actual failover state.
- `LeaderSelector.java:289` / `:389` — `resetAsMaster(force, ...)` / `resetAsSlave(host, port, ...)`. `force=true` already skips the `canResetSelfAsMasterNow` check at line 316, so `SLAVEOF NO ONE` already permits forced promotion. Task 6 "harden semantics" is mostly about tracking failover state and ensuring per-slot consistency, not about adding forced promotion.
- `OneSlot.java:1952` — `resetAsMaster` already clears readonly / canRead / old binlog offset and is annotated `@MasterReset`. Reasonable anchor for Task 6.
- `ConfForGlobal.java:117-119` — the javadoc on `zookeeperRootPath` literally says "also used as sentinel master name", so plan's Task 4 decoupling is correct.
- `CGroup.java:87-164` — `client()` supports `getname/id/info/reply/setinfo/setname`; unknown subcommand falls through to `// todo` and returns `NilReply.INSTANCE`. `CLIENT KILL` is genuinely unimplemented.
- `dyn/test/io/velo/command/InfoCommandTest.groovy:130` — `test replication` already covers master-with-no-replicas, slave, and master-with-replica paths. Task 1 can extend this rather than create a new file.
- `ValkeyRawConfSupport.java:42` — `replicaPriority = 100` already exists, so Task 3's `slave_priority` work is mostly wiring, not new state.

### Strengths
- TDD-first ordering (regression tests → commands → config → client-kill → role transitions → persistence → e2e → docs) matches the repo's TDD workflow in `AGENTS.md`.
- Every task has a focused test command, JaCoCo inspection step, and a single commit, which matches the bug-fix execution rules.
- Compatibility boundary is honest: standalone master/replica only; cluster mode keeps `clusterx`/ZK. Good defensive scope.
- Acceptance criteria are concrete and testable.

### Required refinements before execution
1. **Task 4 — preserve ZK publish paths.** `LeaderSelector.java:446` and `:461` build publish messages using `zookeeperRootPath`. Adding a separate `sentinelMasterName` is correct, but the ZK publish paths must keep using `zookeeperRootPath` unchanged, or existing ZK consumers will break. The task should state this explicitly.
2. **Task 4 — resolve the auth open question first.** The auth open question at the bottom of the plan is the most important unresolved item — `requirepass` / `masterauth` / ACL consistency between Sentinel and replicas must be settled before Task 4 lands, otherwise promoted replicas cannot reconnect. Add a sub-bullet to Task 4 calling this out.
3. **Task 6 — split real semantics from cosmetic reporting.** `LeaderSelector.resetAsMaster` already iterates all slots and already supports `force=true` from `SLAVEOF NO ONE` (verified). The interesting changes in Task 6 are (a) tracking `master_failover_state` for `INFO replication` (currently hard-coded `no-failover` at `InfoCommand.groovy:234`), and (b) any defensive checks around demoting an old master that returns. The "harden promotion" framing in the current Task 6 steps understates that most of the work is observation/reporting.
4. **Task 8 — add a Sentinel-command-sequence harness test as the primary deliverable.** Starting 3 Velo processes + 3 Redis Sentinels as subprocesses in a Gradle test is brittle and slow. Recommend a two-tier approach: (a) primary — a Spock test that drives a Velo test harness through the exact Sentinel command sequence (`PING` → `INFO replication` → `ROLE` → `SLAVEOF NO ONE` → `SLAVEOF host port` → `CLIENT KILL TYPE normal`) and asserts state transitions, (b) follow-up — a true 3-process e2e that depends on real Sentinel binaries. This still meets the acceptance criteria while keeping CI stable.
5. **Open question — make the `LeaderSelector` / ZK rule hard, not ambiguous.** The compatibility boundary already says "do not let Sentinel and Velo's internal failover controller promote or demote the same node at the same time." Convert this into a hard rule for `sentinelModeEnabled=true`: `LeaderSelector` must not start the leader latch and must not call `tryConnectAndGetMasterListenAddress()` for promotion. Otherwise the two controllers can race. Resolve the open question at the bottom of the plan in this direction before Task 6.

### Pre-execution checks I would do
- `git status` clean on a fresh `review/repl` branch off `main` (per the bug-review branch convention, though this is a plan review not a bug review).
- `./gradlew :test --tests "io.velo.command.SGroupTest.test slaveof" --tests "io.velo.command.RGroupTest.test handle - readonly readwrite role" --tests "io.velo.command.RGroupTest.test role" --tests "io.velo.command.InfoCommandTest.test replication"` once for a green baseline before adding failing tests.
- Inspect JaCoCo for `SGroup.slaveof`, `RGroup.role`, `InfoCommand.replication` so we know the baseline coverage of the code we are about to change.

### Post-execution checks I would do
- Per-task JaCoCo HTML inspection on the changed lines.
- A manual `redis-cli -p <port> SLAVEOF NO ONE` smoke test against a locally started Velo to confirm the Sentinel command path actually works end to end.
- A dry-run of a Sentinel `sentinel monitor` configuration against the same instance to confirm `PING` / `INFO replication` / `ROLE` are accepted.

Overall: plan is sound, scope is right, TDD discipline is in place. With the five refinements above folded in (especially #1, #2, and #5), this is ready to execute.

## Review Feedback (AI agent — post-implementation review of commit `00d90f55`)

This section reviews the landed implementation against the plan above. The commit `feat: support redis sentinel for auto-failover` implements Tasks 1–6 (minus Task 7 persistence and Tasks 8–9 e2e/docs) in a single commit rather than the per-task commits the plan prescribed.

### Summary of the fix
- Adds `sentinelModeEnabled`, `sentinelMasterName`, `replicaAnnounceIp/Port`, `sentinelReplicaPriority` config to `ConfForGlobal` with mutual-exclusion validation against `zookeeperConnectString`.
- Makes `SLAVEOF`/`REPLICAOF` accept hostnames (not only IPv4) and require both `NO` and `ONE` tokens for no-one.
- Replaces hard-coded `master_link_status:connected` / `master_repl_offset:-1` in `INFO replication` and `ROLE` with real values from `ReplPair.isLinkUpAnyOk()` and `binlog.currentReplOffset()`.
- Adds `CLIENT KILL TYPE normal` in `CGroup.clientKill()`, skipping the issuing socket and replication sockets.
- Wires `master_failover_state` to `MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState`, set transiently inside `LeaderSelector.resetAsMaster` / `resetAsSlave`.

### Strengths
- Mutual-exclusion guard in `ConfForGlobal.checkIfValid()` (sentinel vs. ZK) directly satisfies plan refinement #5 — the two controllers cannot race at startup.
- `clientKill` correctly preserves the issuing socket and any socket carrying a `replPairAsSlaveInTcpClient`; matches the plan's Task 5 acceptance criterion that replication connections are not accidentally killed.
- `getAnnouncedHostAndPort()` falls back gracefully from announce IP/port to parsed `netListenAddresses`, and the test suite covers 9 edge cases including hostname, empty, null, and unparseable port inputs.
- `SGroup.slaveof()` hostname regex + IPv4 fallback matches Redis Sentinel's reality (it may announce either form), satisfying plan Task 2 step 2.
- The `master_repl_offset:-1` bug called out in the plan's Review Notes (`InfoCommand.groovy:271`) is fixed — healthy masters with no replicas now report `currentReplOffset()`.

### Concerns

#### 1. Bug — `masterFailoverState` observability never actually surfaces (high)
`LeaderSelector.java:290-309` and `:403-419`. Both `doResetAsMaster` and `doResetAsSlave` are asynchronous: they schedule work via `oneSlot.asyncRun(...)` and chain `Promises.all(promises).whenComplete(...)`, then return immediately. The `try { doResetAsMaster(...); } finally { ... reset state ... }` therefore runs the `finally` before any slot work completes, so `masterFailoverState` is flipped back to `no-failover` within microseconds. Sentinel polling `INFO replication` will essentially never observe `promotion-in-progress` / `waiting-for-promotion`. This defeats the purpose of plan Task 6 step 4.

**Fix:** set/clear the state inside the `whenComplete` callback instead of around the synchronous setup, e.g.:
```java
Promises.all(promises).whenComplete((r, e) -> {
    MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
    // ... existing callback.accept(...) ...
});
```
The current `InfoCommandTest` only passes because it sets the field directly rather than driving `resetAsMaster`.

#### 2. Unrelated default change slipped in (medium)
`velo.properties:33` changes `estimateKeyNumber=10000000` → `1000000` (10× lower). Not mentioned in the commit message and unrelated to Sentinel. If intentional, call it out in the commit message or split into its own commit; if not, revert — it silently shrinks the default hash-index capacity for every deployment that relies on the shipped properties file.

#### 3. Two divergent replica-priority sources (medium)
`InfoCommand.groovy:234` now emits both `slave_priority` (from `ValkeyRawConfSupport.replicaPriority`) and `replica_priority` (from `ConfForGlobal.sentinelReplicaPriority`). They can drift. Plan Task 3 step 4 explicitly said "Use existing `ValkeyRawConfSupport.replicaPriority` where possible before adding new priority state." Recommend having one delegate to the other, or dropping the duplicate line.

#### 4. `HOSTNAME_PATTERN` total-length claim is unenforced (low)
`SGroup.java:468-472` javadoc claims "total length <= 253" but the regex only enforces per-label length. Either add a `host.length() <= 253` check after matching, or fix the comment. Not blocking — Sentinel always sends reasonable hostnames.

#### 5. `SLAVEOF NO ONE` arity change is a behavior break (low, worth a release note)
Previously `"no".equalsIgnoreCase(data[1])` triggered no-one regardless of `data[2]`; now both args must match. The new behavior matches real Redis and is what Sentinel sends, but any existing automation that issued `SLAVEOF NO <port>` (relying on the old loose match) will now try to parse `<port>` against host `NO`. Fine for Sentinel compatibility; document it.

#### 6. `RGroupTest` link-state assertion is too permissive (low)
`RGroupTest.groovy:420` asserts `connected || connecting`. This passes regardless of whether the fix is present. Tighten to assert `connecting` explicitly when the test harness knows the TCP client is down, so the regression guard is real.

#### 7. Style nits
- `CGroupTest.groovy:62` — `Eventloop[] eventloopArray = [eventloopCurrent]` lost its indentation (column 0).
- `ConfForGlobalTest.groovy` is missing a trailing newline (`\ No newline at end of file` in the diff).
- `getAnnouncedHostAndPort()` preserves a trailing colon in the host for input like `'myhost:'` — tested as current behavior, but a latent foot-gun if a downstream consumer copies `master_host` verbatim.

### Pre-commit follow-ups
- Fix concern #1 (move state clear into `whenComplete`) and add a test that actually drives `resetAsMaster` / `resetAsSlave` and observes the state during the async window — otherwise the observability feature is inert.
- Resolve concern #2 (revert or justify the `estimateKeyNumber` change).
- Reconcile concern #3 (single priority source).

### Post-commit follow-ups
- Plan Tasks 7 (persist sentinel role state across restart) and 8 (Sentinel command-sequence harness + multi-process e2e) and 9 (deployment docs) are still open. The commit message claims end-to-end Sentinel verification was done manually, but there is no automated regression test for the full failover sequence in the repo — Task 8's harness test is the right place to lock that in.
- Once concern #1 is fixed, manually confirm via `redis-cli -p <port> INFO replication` during a real `SLAVEOF NO ONE` that `master_failover_state` transitions are visible to a poller.

## Review Feedback (AI agent — review of fix-up commit `57627144`)

This section reviews the follow-up commit `fix: address post-implementation review of sentinel support` against the concerns raised in the post-implementation review above.

### Summary of the fix-up
- Moves `masterFailoverState` restore from a synchronous `finally` into the `Promises.all(promises).whenComplete(...)` callback in both `resetAsMaster` and `resetAsSlave`, so Sentinel polling `INFO replication` can observe the transition window (addresses concern #1 from the prior review).
- Adds `LeaderSelectorTest.test failover state observability`, which parks the slot eventloop with a `CountDownLatch`, calls `resetAsMaster`, asserts the state is `promotion-in-progress` while work is queued, then releases.
- Consolidates `slave_priority` and `replica_priority` in `InfoCommand.groovy` to both read from `ConfForGlobal.sentinelReplicaPriority`, and deletes `ValkeyRawConfSupport.replicaPriority` + its `valkey.conf` parser hook + its test (addresses concern #3).
- Adds an explicit `arg1.length() <= 253` check in `SGroup.slaveof` (addresses concern #4).
- Tightens `RGroupTest` to assert exact `connecting` / `connected` values instead of the tautological `connected || connecting` (addresses concern #6).
- Fixes `CGroupTest` indentation and `ConfForGlobalTest` trailing newline (addresses concern #7).

### Concerns addressed
- **Concern #1 (high — async state):** ✅ Fixed correctly. The `whenComplete` callback now restores the state after the async role transition completes. The `doResetAsSlave` early-return path from `checkMasterConfigMatch` failure (`LeaderSelector.java:420-424`) also restores the state — caught a path the original review missed.
- **Concern #3 (medium — divergent priorities):** ✅ Fixed cleanly. Removing `ValkeyRawConfSupport.replicaPriority` entirely (field + parser + test) is better than a delegating shim.
- **Concern #4 (low — hostname length):** ✅ Fixed with an explicit length check; javadoc updated to match.
- **Concern #6 (low — permissive test):** ✅ Fixed. The test now sets a link-up flag via the Groovy-accessible `addLinkUpFlag(true)` and asserts `connected`, and asserts `connecting` without it.
- **Concern #7 (style nits):** ✅ Fixed.

### Concerns still open

#### Concern #2 (medium) — `estimateKeyNumber` default change still not addressed
`velo.properties:33` is still `estimateKeyNumber=1000000`. The fix-up commit does not touch this file and the commit message does not mention it. This 10× reduction from the original `10000000` remains unexplained and silently shrinks default hash-index capacity for every deployment using the shipped properties. Needs an explicit decision: revert, or split into its own commit with justification.

### New concerns introduced by the fix-up

#### A. Synchronous-throw gap in the async-state restore (medium)
`LeaderSelector.resetAsMaster` (`:299-305`) sets `masterFailoverState = "promotion-in-progress"` then calls `doResetAsMaster(force, prevFailoverState, callback)`. The restore now lives in `whenComplete`. But if `doResetAsMaster` throws **synchronously** before reaching `Promises.all(...).whenComplete(...)` — e.g. `localPersist.oneSlot(i)` returns null, or `asyncRun` throws — the state is never restored and gets stuck at `promotion-in-progress` permanently. The old `try/finally` covered synchronous exceptions; the new code does not.

The `resetAsSlave` path handles its `checkMasterConfigMatch` early-return (`:420-424`) but has the same gap for any other synchronous throw inside `doResetAsSlave`.

**Fix:** wrap the body of `doResetAsMaster` / `doResetAsSlave` in `try { ... } catch (RuntimeException e) { MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState; throw e; }`. This is complementary to the `whenComplete` restore — `catch` covers sync throws, `whenComplete` covers the normal async completion.

#### B. Regression test only covers `resetAsMaster` (low)
`LeaderSelectorTest.test failover state observability` exercises the `resetAsMaster` path only. The symmetric `resetAsSlave` fix has the same shape but no regression test. Not blocking since the change is clearly parallel, but a second case mirroring the latch approach against `resetAsSlave` would lock in the symmetry.

### Test design note
The `CountDownLatch`-based parking approach is exactly right and is the only way to deterministically observe the mid-transition state. The cleanup (`slotEventloop.breakEventloop()`, `localPersist.cleanUp()`, `Consts.persistDir.deleteDir()`) looks complete, though `ConfForGlobal.netListenAddresses` is set to `'localhost:7380'` and not restored in cleanup — minor state leakage risk if a later test in the same class depends on the original value.

### Verdict
Four of the five actionable concerns from the prior review are resolved correctly. The remaining work is: (1) decide on the `estimateKeyNumber` change, (2) close the synchronous-throw gap in the state restore, and (3) optionally mirror the regression test for `resetAsSlave`. After those, the Sentinel failover-state observability path is solid.

## Review Feedback (AI agent — review of four Sentinel feature commits through `a0a39d8c`)

This section reviews the four feature commits:

- `00d90f55` — `feat: support redis sentinel for auto-failover`
- `57627144` — `fix: address post-implementation review of sentinel support`
- `e31ad43e` — `fix: close synchronous-throw gap in failover state restore`
- `a0a39d8c` — `fix: address remaining review concerns`

I re-read the plan, the current code paths, and the touched tests rather than relying only on the earlier review notes.

### Summary
- `SLAVEOF` / `REPLICAOF` now accept hostnames, enforce strict `NO ONE`, and keep the alias behavior.
- `INFO replication` and `ROLE` now report real replication offsets/link state in the main Sentinel-facing paths.
- Sentinel runtime config was added (`sentinelModeEnabled`, `sentinelMasterName`, `replicaAnnounceIp`, `replicaAnnouncePort`, `replicaPriority`) with validation in `ConfForGlobal`.
- `LeaderSelector` now exposes transient failover state and the follow-up commits fixed the async restore and synchronous-throw restore gaps.
- `CLIENT KILL TYPE normal` was added as the minimal Sentinel reconfiguration command.

### Findings

#### 1. `CLIENT KILL TYPE normal` can report success without disconnecting clients (high)

`src/main/java/io/velo/command/CGroup.java:221-223` calls `s.close()` directly and only increments the killed count after the call returns. Other socket-close paths, such as `QUIT`, submit the close through the socket reactor in `MultiWorkerServer.handleQuit()`.

During focused verification, `CGroupTest.test client` logged:

```text
Client kill error, addr=localhost/127.0.0.1:46379, msg=Cannot invoke "io.activej.reactor.Reactor.inReactorThread()" because "reactor" is null
```

The test still passed because `src/test/groovy/io/velo/command/CGroupTest.groovy:174-176` only asserts the reply is an `IntegerReply` and that the integer is `>= 0`. That assertion would pass even if no normal client was disconnected. This weakens one of the plan's acceptance criteria: Sentinel must be able to drop stale normal clients after promotion.

Recommended fix: close target sockets on their owning reactor/eventloop, or delegate to an existing socket-inspector/server close helper. Then make the test assert that a connected non-issuing normal client is actually closed/removed or counted as killed, and that the issuing socket plus replication sockets are preserved.

#### 2. `CLIENT KILL TYPE normal` silently ignores unsupported trailing filters (medium)

`CGroup.clientKill()` validates only `data[2] == "type"` and `data[3] == "normal"`. Any additional arguments are ignored, so commands such as:

```text
CLIENT KILL TYPE normal ID 123
CLIENT KILL TYPE normal ADDR 127.0.0.1:12345
```

will kill all normal clients instead of rejecting or honoring the filters. The plan explicitly says unsupported `CLIENT KILL` variants should be explicit rather than silently accepted.

Recommended fix: for the minimal Sentinel-compatible implementation, require exactly four arguments for `CLIENT KILL TYPE normal`. If more Redis filters are desired later, parse and test them deliberately.

#### 3. Sentinel/ZooKeeper mutual exclusion is validated after ZooKeeper parsing and connect-check (medium)

`MultiWorkerServer.InnerModule.confForSlot()` reads `zookeeperConnectString` and may attempt the ZooKeeper TCP connect check before it reads `sentinelModeEnabled`:

- `src/main/java/io/velo/MultiWorkerServer.java:1383-1412` reads and validates ZooKeeper config.
- `src/main/java/io/velo/MultiWorkerServer.java:1416` reads `sentinelModeEnabled`.
- `src/main/java/io/velo/ConfForGlobal.java:245-248` rejects Sentinel + ZooKeeper only later in `checkIfValid()`.

The architecture rule in the plan is that Sentinel mode and ZooKeeper topology control are mutually exclusive. With the current ordering, an invalid Sentinel+ZooKeeper config can still trigger ZooKeeper connection validation before the mutual-exclusion error is reported.

Recommended fix: read `sentinelModeEnabled` before the ZooKeeper block and either skip ZooKeeper parsing/connect-check in Sentinel mode or immediately reject the combination before any ZooKeeper side effects.

#### 4. `resetAsSlave` sync-error test allows the stuck state it is meant to reject (low)

`src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy:586-592` says the state should be restored after the synchronous early-return path, but the assertion permits either the previous state or `"waiting-for-promotion"`.

That means the test would not fail if the state were still stuck in the transition value. Since `e31ad43e` and `a0a39d8c` specifically target stuck failover state, this regression test should assert the restored previous state once the callback has fired.

### Scope Still Open

The four commits do not complete the whole plan:

- Task 7 remains open: persist and restore Sentinel role state across restart before serving normal writes.
- Task 8 remains open: add an automated Sentinel command-sequence harness, and optionally a gated real Redis Sentinel integration test.
- Task 9 remains open: document Sentinel deployment, supported topology, config, command behavior, and limits.

### Verification Notes

I attempted a focused Gradle run:

```bash
./gradlew :test --tests "io.velo.command.SGroupTest.test slaveof" --tests "io.velo.command.RGroupTest.test handle - readonly readwrite role" --tests "io.velo.command.CGroupTest.test client" --tests "io.velo.repl.LeaderSelectorTest" --tests "*InfoCommandTest.test replication" --tests "io.velo.ConfForGlobalTest"
```

The run was interrupted after producing no output for several minutes, so it did not complete. Before interruption it reached the command tests and exposed the `CLIENT KILL` close warning above. No JaCoCo inspection was completed for this review pass.
