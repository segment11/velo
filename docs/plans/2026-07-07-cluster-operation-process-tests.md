# Plan: Cluster-operation process tests (find bugs by running real clusters)

**Date:** 2026-07-07
**Goal:** Expand the opt-in, process-level cluster tests (4 real JVMs, `extends ClusterxBase`) to exercise real
cluster operations end-to-end, inspect server logs, and surface bugs — the same approach that found review
findings F1/F2/F3. Several code paths are currently **unit-tested only** and never run in a real cluster.

## Why

- `ClusterMigrationStressManualTest` (T1/T2/T3) only migrates **inner slot 0**, which `releaseOneSlot` **exempts**
  — so the shed/release path never runs in a real cluster.
- Import **rollback** (Task 11: `migrate_from_abort` / `clusterx migrate ... cancel`) is unit-tested only.
- The **F2/F3** fixes and the **F1** ordering deserve end-to-end confirmation against real logs.
- Repeated-migration churn (repl-pair lifecycle, socket close, resource reclamation) is only lightly covered.

## Harness / method

- All tests `extend ClusterxBase`, gated by `@IgnoreIf({ VELO_CLUSTER_MANUAL != 'true' })`.
- Run with `VELO_CLUSTER_MANUAL=true VELO_CLUSTER_KEEP_DIR=true` and **read `node-*/server.log`** for each run:
  grep for `ERROR`, `NullPointerException`, `ReadonlyException`, `Released shed one slot`, `setslot defer`,
  `Repl create/remove repl pair`, stranded/again messages.
- Add a small harness helper: `serverLogContains(int nodeIndex, String text)` reading `baseDir/node-N/server.log`
  (dir layout already exists in `ClusterxBase.startCluster`).
- Loop: write test → run → inspect logs → if a bug surfaces, file it, fix with TDD, re-run → commit.

## Test scenarios (prioritized by bug-finding potential)

### Tier 1 — exercises code never run end-to-end
- **T4 — non-zero inner slot migrate + release + migrate-back.** Migrate client slot 8192 (**inner slot 2**,
  owned by node-2) to node-0, then back. Asserts: value moves to node-0; node-2 redirects inner slot 2 but
  **still serves inner slot 3** (release of slot 2 must not disturb slot 3); node-2 log shows
  `Released shed one slot=2`; migrate-back re-initializes the released slot and the value returns. *Bugs it may
  find:* `releaseOneSlot` breaking sibling slots, re-init-after-release failures, stranded repl sockets.
- **T5 — import abort/rollback.** `migrate_from` on target + `clusterx migrate` on source, then abort
  (`migrate_from_abort` on target, `clusterx migrate <slot> cancel` on source) **before** cutover. Asserts:
  source still owns and serves the slot (data intact); target reverted (no upstream, not advertising the slot);
  a subsequent fresh migrate of the same slot still succeeds. *Bugs:* partial-import residue, marker not cleared,
  target stuck readonly.

### Tier 2 — validates the just-made fixes against a real cluster
- **T6 — F3 fail-safe: cutover without resync blocks.** Migrate to node-1 (has replica node-3) **without** the
  pre-cutover resync; assert `setslot` on node-1 does **not** complete within a bounded wait and its log shows
  repeated `setslot defer`. Confirms the durability gate refuses to promote unsynced. (Then resync and confirm it
  completes.)
- **T7 — F2: migrate on a non-owner + info.** Send `clusterx migrate <slot> <node>` to a node that does **not**
  own the slot; assert it is rejected (`not owned`) and a following `clusterx info` on that node returns cleanly
  (no NPE). Confirms F2 in a live cluster with lazy slots.

### Tier 3 — concurrency / robustness / leaks
- **T8 — concurrent migrations of two different slots.** Migrate slot A (node-2→node-0) and slot B
  (node-2→node-1) overlapping; assert both complete and data for both is correct, no cross-talk in logs.
- **T9 — kill source mid-migration.** Start a migrate, kill the source JVM before cutover; assert the cluster
  does not lose the source's still-owned data path / the target does not falsely promote (no data-loss window).
- **T11 — repeated migrate churn.** Migrate a slot back and forth N times; watch logs for repl-pair leaks,
  `delayNeedCloseReplPairs` growth, un-closed sockets, or slot re-init errors.

### Tier 4 — versioning (bug 59 regression, live)
- **T10 — stale/reordered setslot.** Broadcast a `setslot` with a stale cluster version; assert it is ignored
  (`ignore stale cluster version`) and ownership is unchanged.

## Deliverables / order

1. Add `serverLogContains` helper to `ClusterxBase`.
2. Implement **T4** and **T5** first (Tier 1 — most likely to find real bugs), run + inspect logs, fix anything
   found (each fix its own TDD commit), then commit the tests.
3. Add **T6/T7** (confirm F2/F3), then Tier 3 (**T8/T9/T11**) and **T10** as time allows.
4. For every bug found: file under `doc/bug_reviews/`, fix with a failing unit test first where possible, keep
   the process test as the regression fence.

## Notes / risks

- Process tests are slow (4 JVMs, ~1–2 min each) and opt-in; keep them off the default CI path.
- Killing JVMs (T9) and concurrency (T8) can be flaky; bound all waits and prefer log-signal assertions over
  timing.
- T6 asserts a **non-completion** (gate blocks) — use a short bounded wait, not the 90s `waitClusterxOk`, to
  avoid a slow test.
