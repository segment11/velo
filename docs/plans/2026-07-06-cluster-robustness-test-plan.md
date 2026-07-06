# Cluster Robustness Test Plan — Find Bugs, Then Fix Them

> **Goal:** Design cluster test cases that harden Velo's cluster path (topology,
> slot migration, failover, replication cutover). Each new test targets a
> specific fragile area; when a test fails and exposes a real defect, fix it via
> the repo's bug-review TDD loop and commit the fix. The tests are the
> deliverable *and* the bug-finding instrument.

**Date:** 2026-07-06
**Owner:** kerry
**Related:** `docs/plans/2026-07-04-velo-cluster-e2e.md` (python/bash harness),
`scripts/e2e/velo_cluster/cluster_migration_failover_bug_review.md` (Bugs 1–4),
`doc/bug_reviews/bug_58_replication_module_review_round_1.md`.

---

## 1. Why now

The last ~40 commits are one long cluster-migration/replication hardening push
(big-string replay routing, `setslot` finalization, lazy `OneSlot` init, 2N
scale-up). Four real bugs (Bugs 1–4) were found and fixed *by hand during a
manual repro*, not by an automated test. The wire format for big-string
full-sync changed **twice in two days** (`e4686651` → `bc6e829b`). That churn
rate on code with almost no cluster-scenario test coverage is exactly where the
next regression hides.

Today the only cluster end-to-end tests are:

- `src/test/groovy/io/velo/repl/cluster/ClusterMigrationFailoverManualTest.groovy`
  — opt-in (`VELO_CLUSTER_MANUAL=true`), 2 scenarios: **single-slot** migrate,
  and a **single** replica forced failover. Data is only written *before*
  migration.
- `src/test/groovy/io/velo/e2e/ClusterMovedSlotTest.groovy` — one static
  preseeded MOVED-redirect assertion.
- Unit tests (`dyn/test/.../ClusterxCommandTest`, `ManageCommandTest`,
  `MultiShardTest`, `watch/FailoverManagerTest`) cover command arg-parsing and
  metadata mutation with everything mocked — no data movement.

That leaves the actual data-durability behavior of migration and failover almost
entirely untested. Section 4 lists the concrete gaps.

---

## 2. Strategy: two tracks + a bug loop

**Track A — fast in-JVM tests** (extend existing unit/integration specs). Best
for logic invariants that don't need real replication: epoch validation, the
`setslot` three-way branch, repl-pair lifecycle, lazy-init null-safety,
big-string wire decode edge cases. Milliseconds, run in normal CI.

**Track B — process-level scenarios** (extend `ClusterxBase`). Required for
anything that depends on real master→slave streaming and timing: concurrent
writes during migration, big-string/TTL migration, failover-during-migration,
crash durability. Opt-in (`VELO_CLUSTER_MANUAL`) because they spawn JVMs.

**Bug loop** (per `CLAUDE.md` "Bug Fix Execution Workflow"): when a new test
goes red for a *real* reason, stop adding tests. Open/append a bug-review doc
under `doc/bug_reviews/bug_<N>_cluster_review_round_1.md` (severity, cited
files/lines, excerpt, root cause). Then, one bug at a time: keep the failing
test, write the minimal fix, rerun the test + inspect JaCoCo, and commit that
single fix before moving on. Do not batch fixes.

**Prioritization.** P0 = high churn + plausible live defect + data-loss impact.
P1 = plausible defect, contained impact. P2 = completeness / regression fence.

---

## 3. Task 0 — Harness extensions (prerequisite for Track B)

Add to `ClusterxBase.groovy` (keep the file the single home for reusable infra):

1. **`Thread startBackgroundWriter(NodeHandle node, String keyPrefix, Closure valueFor, AtomicBoolean stop, List sink)`**
   — spawns a daemon thread that SETs `keyPrefix:{seq}` in a tight loop until
   `stop`, recording each acked `(key,value)` into `sink`. Used to write *during*
   migration/failover. Must tolerate `MOVED`/readonly mid-cutover (record only
   acked writes).
2. **`void killNode(int index)` / `NodeHandle restartNode(int index)`** — hard
   `destroyForcibly()` a node's JVM (crash simulation) and relaunch it against
   the *same* data dir (reuse the node's props/port). Needed for durability
   tests. Reuse `waitPing`.
3. **`Object setnodesRaw(NodeHandle node, String nodesArgs, int version)`** —
   send `CLUSTERX SETNODES` with an *explicit* version, bypassing the monotonic
   `clusterVersion++` counter, so a test can replay a stale/equal version.
4. **`int currentEpoch(NodeHandle node)`** — parse `cluster_current_epoch` out of
   `clusterInfo`, for epoch-monotonicity assertions.
5. **`String bigValue(int approxKb)`** — deterministic filler that reliably
   exceeds the big-string threshold (`CompressedValue` big-string cutoff; confirm
   the constant before use so the value truly takes the `ToSlaveExistsBigString`
   path, not the short-string path).
6. **`void migrateSlot(int clientSlot, NodeHandle from, NodeHandle to)`** — factor
   the migrate→wait `migrating_state:success`→`broadcastSetSlot` sequence out of
   `ClusterMigrationFailoverManualTest` so every Track-B test reuses one correct
   cutover, not a copy-paste.

Extension #3 (`setnodesRaw`) and #4 (`currentEpoch`) are also needed by the
Track-A epoch test (T3) — implement them first.

---

## 4. Test cases

New Track-A cases land in the existing `dyn/test/.../ClusterxCommandTest.groovy`,
`MultiShardTest.groovy`, and `src/test/groovy/io/velo/command/XGroupTest.groovy`.
New Track-B cases land in `ClusterMigrationFailoverManualTest.groovy` (or a new
`ClusterMigrationStressManualTest.groovy` if that file gets long).

### P0 — most likely to surface a real bug

**T1 — Big-string values survive slot migration (Track B).**
Target: fragile area #1 (`XGroup.s_exists_big_string` `XGroup.java:1232-1310`,
`ToSlaveExistsBigString.java`, `BigStringFiles.getBigStringFileIdList:311-337`).
Setup: write N keys to slot 0 with `bigValue(64)` values (mix of big + small in
the same slot). Migrate slot 0 → node1. Assert every key present with exact value
on the new owner **and** its shard replica (node3). Then migrate a *second* slot
that also has big strings, to exercise `removeIf(empty key)` / `keyLength<=0`
paths with real files.
Suspected defect surface: `bc6e829b` makes the master **silently skip** a big
string whose uuid→key can't be resolved (`idListInMaster.removeIf(id ->
id.key().isEmpty())`). Add an assertion that no acked big-string key is missing
after migration — if the key index isn't populated for a legitimately-written
big string, this test should catch the silent drop.

**T2 — Concurrent writes during migration are not lost (Track B).**
Target: fragile area #2 + the `flush()` danger window (`ClusterxCommand.setslot`
`:1228-1247`, guarded by `65c02860`). Setup: seed slot 0, start
`startBackgroundWriter` against the *source* (node0) for slot-0 keys, run the full
migrate→setslot cutover while the writer runs, stop the writer, drain. Assert:
every acked write is readable on the new owner (node1) and its replica; the old
source returns MOVED; no value is stale. This directly exercises "writes that
arrive during the exists-phase then stream via `catch_up`" and the cutover flush.

**T3 — Stale / out-of-order cluster version is rejected (Track A, likely a real bug).**
Target: command-surface finding — `MultiShard.refreshAllShards:178-183`
overwrites `clusterCurrentEpoch = clusterVersion` **unconditionally**, and
`updateClusterVersion:191-197` only guards `!= 0`. There is **no epoch
monotonicity check** anywhere. Write a red test asserting that applying
`SETNODES`/`SETSLOT` with a version ≤ the current epoch is rejected (or at least
does not regress ownership). Expectation: this fails today → it is a genuine
split-brain hazard (a delayed/duplicated controller broadcast can roll topology
backwards). If confirmed, fix = reject stale versions in `refreshAllShards` /
`updateClusterVersion` (compare against `clusterCurrentEpoch`), then keep the
test. This is the single most promising "find a bug" case.

**T4 — `setslot` cutover preserves data for every node role (Track A).**
Target: fragile area #2 (Bugs 3 & 4 lived here). With mocked `LocalPersist`/
`MultiShard`, drive `setslot()` through all three branches and assert: new-owner
node → `resetAsMaster` + not flushed; target-shard replica → reset as slave of
the new master (not left stale); old source → repl pairs closed **before**
`flush()` (order matters; assert no `XFlush` send after the pairs are torn down).
Extends the existing `setslot resets local target shard replica` (`:1177`) with
the old-source-flush-ordering assertion that Bug 4 was about.

### P1 — plausible defect, contained impact

**T5 — Repeated migrate → setslot → migrate-back leaves no stale/byed repl pair (Track B).**
Target: fragile area #3 (`OneSlot.createIfNotExistReplPairAsMaster:589-611` skips
`isSendBye()` pairs). Cycle slot 0 between node0 and node1 three times; after each
cycle assert data intact on the current owner + replica, and that no NPE
(`getSlaveUuid()` on null) or accumulating repl pairs occur. Check node logs for
repl-pair leak warnings.

**T6 — Forced failover *during* an in-flight migration (Track B).**
Target: gap #8 + fragile area #6 (`LeaderSelector.resetAsMaster:400-462`,
`force` bypasses caught-up guard). Start migrating slot STEP (owned by node1,
replica node3), and before `setslot`, force-failover node3. Assert the cluster
converges to a consistent single owner for the slot and no acked write is lost or
double-owned. This is the messiest interleaving; expect it to be flaky first —
use it to decide whether Velo needs to *reject* failover while a shard has an
active `import/exportMigratingSlot`.

**T7 — TTL / expire survives migration (Track B).**
Target: gap #12. SET keys with `PEXPIRE` in slot 0, migrate, assert TTL is present
and within tolerance on the new owner + replica, and that an already-expired key
does not resurrect. Exercises expire metadata through the exists-phase + binlog
replay.

**T8 — Write to a freshly-owned-but-never-initialized slot (Track A/B).**
Target: fragile area #4 (lazy `OneSlot` init; `oneSlot == null` guards in
`setslot:1201-1226`, `migrateFrom:556-559`,
`ensureLocalShardSlotsInitialized:1131-1141`). Give a node ownership of an inner
slot it never touched via `SETNODES`, then immediately SET a key routing there.
Assert the write succeeds (slot inits on demand) and `COUNTKEYSINSLOT` on a
lazy/unowned slot returns 0 without NPE.

**T9 — Multi-slot / range migration (Track B).**
Target: gap #7. Migrate a *range* of slots (multiple inner-slot batches) from one
shard to another in one logical operation; assert all keys across all migrated
slots land correctly and un-migrated slots are untouched. Surfaces the
`isToClientSlotSkip` batch alignment (`MultiShard.java:43`,
`ClusterxCommand:646,1173`).

### P2 — regression fences / completeness

**T10 — Master crash mid-write, replica promoted (Track B).**
Target: gap #9. Background-write to node1, `killNode(1)`, force-failover node3,
assert every write acked *before* the kill is present on node3 (durability
boundary). Documents the actual durability guarantee (may reveal an
acked-but-unreplicated window worth a fix or a doc).

**T11 — Dynamic MOVED target updates after a live migration (Track B).**
Target: gap #14. After migrating slot 0 to node1, assert that a client hitting
node0 gets `MOVED ... node1:port` (not the stale pre-migration target). Extends
the static `ClusterMovedSlotTest`.

**T12 — Empty/unassigned slot routing (Track A).**
Target: gap #10. With a slot owned by no shard, assert `cluster_state:fail` in
`info` and that GET/SET on that slot returns `CLUSTER_SLOT_NOT_SET` rather than
NPE or wrong routing (`MultiWorkerServer.checkClusterSlot:341-391`).

**T13 — Dynamic cluster commands load without error in cluster mode (Track A).**
Target: the exploration note that `dyn/` commands emit static-type-check warnings
on reload which can mask real load failures. Smoke-assert `ClusterxCommand` /
`ManageCommand` compile+load cleanly under `clusterEnabled=true`.

**T14 — `catch_up` across binlog file rollover (Track A/B).**
Target: fragile area #8 (`XGroup.java:1616-1693` segment-alignment asserts,
`marginFileOffset` partial-segment refetch). Force enough writes on the source to
roll a binlog file during migration, assert the target reproduces the exact key
set (no off-by-one at the segment/file boundary).

---

## 5. Execution order

1. **Task 0** harness extensions (#3, #4 first — unblock T3).
2. **T3** (epoch rejection) — cheapest, highest bug-probability. If red for a real
   reason, run the bug loop and fix before continuing.
3. **T4** (`setslot` roles) — Track A, cements the Bug 3/4 fixes as regressions.
4. **T1, T2** (big-string + concurrent-write migration) — the P0 Track-B pair.
5. Remaining P1 (T5–T9), then P2 (T10–T14) as time allows.

Each test is added red-first where it asserts new behavior; for pure regression
fences (T4, T11) confirm they pass green against current code and document that
they lock in an existing fix.

---

## 6. Bug-handling protocol

When any test exposes a defect:

1. Append to `doc/bug_reviews/bug_<N>_cluster_review_round_1.md`: severity, cited
   `file:line`, code excerpt, root cause, impact. (New bug number continues the
   existing sequence — last was bug 58.)
2. Keep the failing test. Implement the **minimal** production fix.
3. Rerun the specific test; inspect `build/reports/jacocoHtml/` to confirm the
   changed lines/branches executed (`scripts/jacoco_cover.py`).
4. Commit that single fix (`fix: ...`, history style), then move to the next.
5. A second agent appends a "Review Feedback" section per the `CLAUDE.md`
   two-agent workflow.

Known likely-bug candidates already surfaced by the exploration, in priority
order: **(a) no stale-epoch rejection** (T3), (b) big-string silent-drop on
unrecoverable key (T1), (c) failover-during-migration having no guard (T6).

---

## 7. Risks & notes

- **Flakiness.** Track-B tests use real timing; keep the poll-with-deadline
  helpers, never fixed sleeps. Interleaving tests (T6, T10) may need generous
  deadlines and retry-tolerant assertions on the *invariant* (no data loss / one
  owner), not on exact intermediate states.
- **Cost.** Each Track-B scenario boots 4×512m JVMs. Keep them `@IgnoreIf`
  opt-in; consider a single shared-cluster spec with multiple `when/then` blocks
  if boot time dominates.
- **Big-string threshold.** Confirm the actual cutoff constant before writing
  `bigValue()` so T1 truly takes the big-string path.
- **Not in scope here.** Gossip/`meet` (intentionally unimplemented), ZooKeeper
  `FailoverManager` integration (already unit-tested, mocked), and cross-DC
  concerns. This plan is about single-controller topology + data durability.
- **Overlap.** The python `scripts/e2e/velo_cluster/` harness covers the same
  narrow single-slot-migrate/single-failover ground as the current Groovy manual
  test; this plan deliberately does not extend the python driver — new coverage
  goes through `ClusterxBase` so it lives with the JVM test suite and Jedis
  assertions.
