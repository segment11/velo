# Bug 61 — Replica subscribes to a slot its master does not own, causing master-pair churn + a null-pair window during migration

**Module:** cluster / replication (topology-driven slave subscription)
**Round:** 1 (found while analyzing bug 60's residual "repl pair as master not found" during `ClusterMigrationStressManualTest` T1)
**Severity:** MEDIUM — migration still completes (the reconnect loop + bug-60 central guard keep it correct), but every migrate churns a create→demote→recreate of the target's downstream master pair and opens a transient window where an in-flight `exists_big_string` finds no master pair. Under lazy OneSlot init this fires on every migration because each node initializes a fallback inner slot 0 it does not own.

**Status (fixed 2026-07-07):** fixed by a ping/pong sync-topology exchange (see below). T1 passes; the bootstrap create-then-demote churn and the residual guard warning are both gone in the process-level log.

## How it was found

Bug 60's fix left a benign-but-real WARN in node-1's log during T1: `Repl master handle but repl pair as master not found, maybe topology switching, repl type=exists_big_string, slot=0`. Tracing the node-1 / node-3 logs showed the master pair for slot 0 toward the target's replica being **created at bootstrap, removed by the migrate_from demotion, then recreated at cutover** — the replica had subscribed to a slot the master did not own.

## Root cause

Topology (`SLOT_NUMBER=4`, so `innerSlot = clientSlot / 4096`): node-0 owns inner slot 0, **node-1 owns inner slot 1**, node-3 is node-1's replica. T1 migrates client slot 0 (→ inner slot 0) from node-0 to node-1.

1. At bootstrap node-3 becomes node-1's slave via `LeaderSelector.resetAsSlave`, which iterates **`initializedOneSlots()`** — and under lazy init that includes node-3's always-initialized **fallback inner slot 0**, which neither node-3 nor node-1's shard owns.
2. node-3 hello's node-1 for inner slot 0; node-1's `hello` handler **unconditionally** `createIfNotExistReplPairAsMaster` (it does not check ownership), so node-1 becomes a momentary master-of-an-empty-slot-0 for node-3.
3. `migrate_from` on node-1 runs `resetAsSlave` → `removeReplPairAsMaster()` (the intended demotion), tearing down that just-created pair; node-3's in-flight `exists_big_string` then finds no master pair (the bug-60 window).
4. The master `ping` handler **also** `createIfNotExistReplPairAsMaster` unconditionally, so after the demotion node-3's next ping would re-create the pair for a slot node-1 still does not own.

Net: a replica's slot subscription was keyed on *what it had initialized*, and the master served *any* slot a downstream asked for — neither was keyed on **shard ownership**.

## Data flow (before vs after)

Legend: `═▶` migrate/import control, `──▶` repl message, `✗` torn-down pair, `∅` null-pair window.

### Before — create → demote → recreate churn (per migration)

```
 node-0                 node-1  (target; owns inner slot 1, NOT slot 0)      node-3  (node-1's replica)
 (owns slot 0)          |                                                    |
                        |  bootstrap: resetAsSlave(node-1)                   |
                        | ◀─────────────── hello slot 0 (fallback, unowned) ─┤
                        |  createReplPairAsMaster(slot 0)  ── UNCONDITIONAL   |
                        | ─────────────────────────── hi ──────────────────▶ |  subscribed to slot 0
                        |                                                    |
   ═══ migrate_from ═══▶|  resetAsSlave(node-0)  → removeReplPairAsMaster(0) |
                        |  ✗ pair toward node-3 torn down (demotion)         |
                        | ◀──────────── exists_big_string (in flight) ───────┤
                        |  ∅ no master pair  → NPE (bug 60) / guard WARN     |
                        |                                                    |
   ═══ data import ════▶|  ...pulls slot 0 from node-0...                    |
   ═══ setslot ════════▶|  now owns slot 0: createReplPairAsMaster(0) again  |
                        | ─────────────────────────── hi ──────────────────▶ |  re-fetch (finally real data)
```

### After — decline + drop, one clean subscription at cutover

```
 node-0                 node-1  (target; does NOT own slot 0 yet)            node-3  (node-1's replica)
 (owns slot 0)          |                                                    |
                        |  bootstrap: resetAsSlave(node-1)                   |
                        | ◀─────────────── hello slot 0 (fallback, unowned) ─┤
                        |  clusterEnabled && !owned → DECLINE (empty)        |
                        | ─────────────────── (no pair created) ───────────▶ |
                        | ◀──────────────── ping slot 0 (myEpoch) ───────────┤
                        | ──────── pong serveSlot=0, masterEpoch ──────────▶ |
                        |                                     masterEpoch ≥ myEpoch → removeReplPairAsSlave(0)
                        |                                                    |  ✗ drops unowned slot 0
   ═══ migrate_from ═══▶|  resetAsSlave(node-0)  (no downstream pair to churn)
   ═══ data import ════▶|  ...pulls slot 0 from node-0...                    |
   ═══ setslot ════════▶|  now owns slot 0; setslot re-subscribes node-3     |
                        | ◀─────────────── hello slot 0 (now owned) ─────────┤
                        |  owned → createReplPairAsMaster(0), serveSlot=1    |
                        | ─────────────────────────── hi ──────────────────▶ |  fetch real data (first try)
```

## Fix — ping/pong sync-topology with an epoch guard (cluster-only)

A master becomes authoritative about **which slots it serves a downstream replica**, and a slave drops subscriptions the master declines.

- **Wire (`io.velo.repl.content.Ping` / `Pong`):** ping appends the slave's `clusterMyEpoch`; pong appends the master's `clusterMyEpoch` + a `serveSlot` flag. Single-arg constructors stay byte-for-byte legacy (used by `bye`/`byeBye`).
- **Ownership (`LocalPersist.isInnerSlotOwnedByMySelfShard`):** true when cluster is disabled (plain master-slave unchanged), else whether `mySelfShard` owns the inner slot's client block — never the fallback slot 0.
- **Master `hello`/`ping` (`XGroup`):** when `clusterEnabled && !owned`, decline — do not create a master pair (hello returns empty; ping replies pong with `serveSlot=0`).
- **Slave `pong` (`XGroup`):** when `clusterEnabled && serveSlot=0 && masterEpoch >= myEpoch`, `removeReplPairAsSlave()` and stop. The epoch guard ("use newest") stops a **stale** master from making a newer-epoch slave abandon a slot it legitimately owns.
- **Re-subscription is safe:** at cutover, `ClusterxCommand.setslot` re-subscribes the replica (`resetAsSlave(toMasterNode)`) for the newly-owned inner slot, against a master that now owns it (`serveSlot=1`) — so the slave syncs the real data on the first try.

## Verification

- Unit: `LocalPersistTest.'test isInnerSlotOwnedByMySelfShard'`;
  `XGroupTest.'test as master handle ping serve flag by shard ownership in cluster mode'` (serve gating + hello decline);
  `XGroupTest.'test as slave handle pong drops or keeps repl pair by serve flag and epoch'` (drop, epoch guard, non-cluster no-op). JaCoCo confirms the new branches execute.
- Process-level: `ClusterMigrationStressManualTest` T1 passes. Kept-dir logs confirm node-1 now logs `Repl master decline hello for a slot not owned by my shard, slot=0`, node-3 logs `Repl slave drop repl pair as slave: master no longer serves this slot, master epoch=2, my epoch=2, slot=0`, node-1 creates the slot-0 master pair toward node-3 **only once (at cutover)**, and there is **no** `repl pair as master not found` / NPE for `exists_big_string`.

## Relationship to bug 60

Bug 60's central `handleReplInner` guard (empty reply on transient null master pair) remains as a defensive safety net; this fix removes the condition that made it fire in the normal migration path.

## Reproduction
- `VELO_CLUSTER_MANUAL=true VELO_CLUSTER_KEEP_DIR=true ./gradlew :test --tests "*ClusterMigrationStressManualTest.T1*big string*" --rerun-tasks`
