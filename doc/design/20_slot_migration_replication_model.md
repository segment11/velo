# Velo Slot-Migration Replication Model — Dual-Role `OneSlot` (Upstream / Downstream)

**Status:** Proposal / design discussion (Phase 1 shipped as bug 61; Phase 2 not yet built)
**Related:** [`09_replication_design.md`](/home/kerry/ws/velo/doc/design/09_replication_design.md),
[`16_cluster_management_design.md`](/home/kerry/ws/velo/doc/design/16_cluster_management_design.md),
[`18_scale_up_design.md`](/home/kerry/ws/velo/doc/design/18_scale_up_design.md),
bug reviews `bug_60_*`, `bug_61_*`.

## Goal

Make slot migration treat a [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java) as
holding **two orthogonal links at once**, instead of flipping its whole role:

- an **upstream** link — a source it *fetches data from* (its own master, or the migration source shard).
- one or more **downstream** links — replicas it *serves data to* (its own shard's replicas).

Today `resetAsSlave` collapses both into a single "I am now a slave" role. That collapse is the root of the
migrate-time repl-pair churn (bug 61), the post-cutover no-replica durability window, and a full re-sync of
the whole slot on the downstream replica after cutover.

## Terminology — upstream / downstream vs master / slave

The existing code names repl pairs by *this node's role in the pair* (`ReplPairAsMaster` / `ReplPairAsSlave`),
which reads ambiguously because "master/slave" also names the intra-shard role. This doc uses **direction of
data flow** instead, which is unambiguous and composes cleanly when a slot has both at once:

| Data-flow term (this doc) | Meaning | Current code |
|---|---|---|
| **upstream** | a peer this slot *fetches from* (its master / migration source) | `ReplPairAsSlave`, `getOnlyOneReplPairAsSlave()`, `createReplPairAsSlave()` |
| **downstream** | a peer this slot *serves* (a replica of this slot) | `ReplPairAsMaster`, `getReplPairAsMasterList()`, `createIfNotExistReplPairAsMaster()` |

So "keep serving my replicas while importing from the source" becomes simply **keep downstream, add upstream**.
Adopting this vocabulary in the API (below) is part of the proposal; renaming the underlying repl-pair methods
is an optional follow-up (see Decision points).

## Background — the migration data flow

Migration is **replication-based**, not Redis-style key-by-key. The target temporarily becomes a slave of the
source for one inner slot, full-syncs, then is promoted at cutover. It reuses the ordinary repl message
primitives (`hello → hi → exists_* → catch_up`):

```
 controller             node-1 (TARGET / importer)                     node-0 (SOURCE / owner)
   manage migrate_from ─▶| ensureOneSlotInitialized(0)                    |
                         | resetAsSlave(node-0) → import migrating slot    |
                         | ── hello ─────────────────────────────────────▶|
                         | ◀─ hi (masterUuid, binlog fo) ─────────────────|
                         |   exists phase (bulk one-shot):                 |
                         | ── exists_dict / big_string / short_string ────▶|
                         | ── exists_chunk_segments / exists_wal ─────────▶|
                         | ◀─ s_exists_* payloads applied to target ──────|
                         | ── exists_all_done ───────────────────────────▶|
                         |   catch_up phase (incremental, loops):          |
                         | ── catch_up (from binlog offset) ─────────────▶|
                         | ◀─ s_catch_up (binlog segments) ───────────────|
   clusterx migrate ──────────────────────────────────────────────────────▶ set exportMigratingSlot
   wait migrating_state:success (source sees target allCaughtUp)            |
   clusterx setslot ────▶| removeReplPairAsSlave → resetAsMaster(0) ══════▶ flush slot 0
   (broadcast, epoch)    | now OWNS slot 0                                 |
```

## Current model and why it caps

`resetAsSlave` performs a **full role swap**
([`OneSlot.java:2654`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)):

```
resetAsSlave(host, port, openReplStream):
    metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset()
    binlog.moveToNextSegment()
    removeReplPairAsMaster()          // ← drops downstream replicas   (demotion)
    if (openReplStream) createReplPairAsSlave(source)
    setReadonly(true); setCanRead(false)
    binlogOn = false                  // ← stops downstream propagation
```

Two of these steps are the problem for a **migration** import (as opposed to a **failover** demotion):

- `removeReplPairAsMaster()` tears down this node's pairs to its own replicas. During migrate_from the target
  had just created a pair to its replica (via that replica's topology-driven subscription), so the sequence is
  create → demote → recreate. See bug 61.
- `binlogOn = false` means nothing the target imports is written to its binlog, so its replicas cannot follow
  the import; they can only re-sync **after** cutover.

The structure does **not** force this. `replPairs` is a single list holding master pairs *and* slave pairs,
filtered by `isAsMaster()`
([`OneSlot.java:340`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)); a slot can already hold
both. There is also a relay precedent: 2N scale-up uses `resetAsSlave(host, port, openReplStream=false)` for
"target slots whose data arrives via **fan-out from stream slots** rather than their own master connection"
([`OneSlot.java:2643`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)).

## Proposed model — dual-role via `addUpstream` / `dropUpstream`

Model migration as **adding a temporary upstream to a slot that stays a master**, rather than swapping roles.
Prefer explicit operations over a `resetAsSlave(..., keepMaster)` flag, so "migration import" and "failover
demotion" stay distinct:

```
addUpstream(source):                 // migrate_from target: fetch from source, keep serving replicas
    createReplPairAsSlave(source)     // add the upstream link
    // KEEP downstream links (do NOT removeReplPairAsMaster)
    // KEEP binlogOn = true           // imported writes propagate to downstream replicas
    setReadonly(true); setCanRead(false)   // no client writes; not readable yet

dropUpstream():                      // at cutover: drop the upstream, become a plain master again
    removeReplPairAsSlave()           // remove the upstream link
    setReadonly(false); setCanRead(true)
    // downstream links were never dropped → replicas are already near-synced

resetAsSlave(...):                   // unchanged: full role swap (drop downstream + add upstream), failover only
```

Read plainly: `addUpstream` = "**add an upstream, keep the downstream**"; `resetAsSlave` = "**drop the
downstream, swap to an upstream**". The migration import wants the former; failover wants the latter.

This turns the source → target → downstream path into a **relay chain**:

```
 node-0 (SOURCE)          node-1 (TARGET, master-and-slave)              node-3 (node-1's replica)
        | ── exists_* / catch_up ─▶ apply locally                        |
        |                          AND append to node-1 binlog ─────────▶ | catch_up follows the import live
        |                          (master pairs kept, binlogOn kept)     | (canRead stays false until cutover)
   ═ setslot ═▶ dropUpstream(): node-1 promotes; node-3 already in sync ─▶ | flip canRead → serving immediately
```

Benefits over the Phase-1 (bug 61) gating approach:

- **No create→demote→recreate churn** — the master pair is never dropped.
- **No post-cutover no-replica window** — the replica tracks the import throughout, so at cutover the migrated
  slot already has an up-to-date replica.
- **No full replica re-sync** — the replica applies the import incrementally instead of re-fetching the whole
  (possibly large) slot after cutover.

## Hard problems / open questions

1. **Exists-phase must reach the binlog.** The bulk exists phase is applied via direct writes; the write path
   only feeds `appendBinlog` when `binlogOn`
   ([`OneSlot.java:1180`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)). For a relay, the
   import-apply must emit binlog records for the exists phase too, or downstream sees only the `catch_up` tail.
   This is the bulk of the work. Reusing the 2N fan-out path is the preferred route.
2. **Chained readiness.** The downstream replica must stay `canRead=false` for the slot until the target
   finishes importing *and* the downstream drains the relay, then flip at cutover. Replication lag composes
   across two hops; readiness/`allCaughtUp` accounting must account for the chain.
3. **`masterUuid` identity across cutover.** *(Resolved.)* The target slot's `masterUuid` is `final`
   ([`OneSlot.java:329`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)) — stable across
   `addUpstream`/`dropUpstream`. So a downstream that follows the **target's** binlog under the target's stable
   `masterUuid` keeps tracking one identity through migration *and* cutover. The one requirement this imposes:
   `dropUpstream` must be a **soft promote** — unlike `resetAsMaster`, it must *not* rebase/reopen the binlog to
   the source's offset (that would break a downstream mid-follow). It only clears the catch-up-from-source
   bookkeeping and lifts readonly, keeping the binlog position continuous. Implemented in `OneSlot.dropUpstream`.
4. **Rollback.** An aborted import must cleanly revert the target *and* the partial state it relayed downstream.
5. **Ownership still matters.** A node must not relay a slot it will never own (e.g. the always-initialized
   fallback inner slot 0). The bug-61 ownership gate remains the guard for that; `addUpstream` applies only to
   the migrate_from target, which *will* own the slot.

## Relationship to bug 60 / bug 61

- **Bug 60** — a transient null master pair during the topology switch NPE'd; fixed with a central
  empty-reply guard in `handleReplInner`. Remains a defensive net.
- **Bug 61** — the replica subscribed to a slot its master did not own, causing the create→demote→recreate
  churn; fixed with a cluster-only ping/pong sync-topology exchange (master declines an unowned slot; slave
  drops it, guarded by newest epoch). This is **Phase 1**: correct and churn-free, but it re-subscribes only at
  cutover, so it keeps the no-replica window and the full re-sync.
- **This proposal (`asSlaveKeepMaster` / `addUpstream`)** is **Phase 2**: complementary, not competing. Bug 61
  keeps a node from relaying a slot it will never own; Phase 2 lets the migrate target relay a slot it *is*
  acquiring, closing the durability window and the re-sync cost.

## Phased plan

- **Phase 1 (done):** bug 61 ownership gating — stop the churn/NPE, keep migration correct. Shipped, T1-green.
- **Phase 2 (this doc):** introduce `addUpstream`/`dropUpstream` for the migrate_from target only, reusing the
  2N fan-out/relay machinery to propagate the import (exists + catch_up) to downstream replicas live. Gate
  behind a config flag; default off until validated.

## Test plan (Phase 2)

- **Durability:** kill the new owner immediately after `setslot`; assert the migrated slot's data survives on
  its replica (no no-replica window).
- **Efficiency:** migrate a large slot; assert the downstream replica does **not** perform a full re-sync after
  cutover (it applied the relay incrementally) — e.g. by asserting bounded post-cutover fetch bytes / time.
- **Churn/identity:** assert the target's master pair to its replica is never dropped during migration, and the
  downstream's `(masterUuid, offset)` bookkeeping survives the promotion without a forced full re-fetch.
- **Rollback:** abort an in-flight import; assert both target and downstream revert cleanly.
- **Regression:** existing `ClusterMigrationStressManualTest` T1/T2 stay green with the flag on and off.

## Decision points

- **Explicit ops vs a flag** — `addUpstream`/`dropUpstream` (recommended) vs `resetAsSlave(..., keepMaster)`.
  Explicit ops keep failover-demotion and migration-import from sharing one overloaded method with divergent
  invariants, and the upstream/downstream vocabulary states the intent directly.
- **Whether to also rename the repl-pair API** — optional follow-up: `getReplPairAsMasterList` → downstream
  getters, `createReplPairAsSlave` → `createUpstream`, `removeReplPairAsMaster` → `dropDownstream`, etc. This
  makes the whole replication layer read in one vocabulary, but touches many call sites; can land separately
  from the Phase-2 behavior change so the rename stays a pure, reviewable refactor.
- **Relay via binlog vs via 2N fan-out** — reuse the existing fan-out apply path rather than a parallel relay,
  to avoid a second propagation mechanism.
- **Whether to keep the bug-61 gate once Phase 2 lands** — yes: it still governs the fallback-slot-0 and any
  never-owned slot, which Phase 2 does not address.
