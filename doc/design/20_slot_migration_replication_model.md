# Velo Slot-Migration Replication Model — Dual-Role `OneSlot` (Upstream / Downstream)

**Status:** Phase 1 (bug 61) shipped. Phase 2 **Tier A implemented** (Tasks 6–12: dual-role primitives,
catch_up relay, wiring, import rollback, and the durability-window closure — all committed, process-tested).
Tier B (no full re-sync) and `releaseOneSlot` (Task 13) remain.
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

### What Tier A actually implements (vs the ideal live relay chain)

The ideal would be a pure **relay chain** where the downstream follows the whole import live (source → target →
downstream) and is already fully in sync at cutover. That is **Tier B** — it needs raw-segment binlog content
types (the exists/historical bulk has no binlog representation today; see Hard problems). **Tier A** — what is
built — gets the downstream synced *before* cutover a different way:

```
 node-0 (SOURCE)     node-1 (TARGET, master-and-slave)                 node-3 (node-1's replica)
   ── exists/catch_up ─▶ import locally; relay only catch_up to binlog  |  (not subscribed during import:
   (target upstream not caught up → does NOT serve slot to downstream)  |   hasCaughtUpUpstream=false)
   ─ migrating_state:success → target upstream caught up ──────────────▶|
   resync: node-3 migrate_from(node-1) ◀───────────────────────────────┤  fresh physical exists-fetch
   (now serves: hasCaughtUpUpstream=true) ── exists + catch_up ────────▶|  → node-3 caught up
   ═ setslot (charge node first) ═▶ gate: wait downstream allCaughtUp,   |
                                    then dropUpstream/soft-promote       |  already synced ⇒ serving
   ═ setslot (source last) ═▶ node-0 flushes only after node-1 promoted  |
```

Benefits **delivered by Tier A**:

- **No create→demote→recreate churn** — the master pair is never dropped (`addUpstream` keeps downstream).
- **No post-cutover no-replica window (when replicas are configured)** — if the shard has replica nodes, the
  target defers cutover until it has an **established and caught-up** downstream pair
  (`hasDownstream() && downstreamAllCaughtUp()`; `downstreamAllCaughtUp` alone is vacuously true before any
  replica subscribes), and the source flushes last — so the slot always has a synced replica at the moment of
  promotion (verified by test T3). For a shard with **no replicas configured** there is no such window, so
  cutover proceeds immediately — this is a weaker-durability-by-configuration mode, not a gap.

Benefit **not** delivered by Tier A (needs Tier B):

- **Avoiding the full replica re-sync** — the replica still does a physical exists-fetch during the resync; Tier
  A moves it *before* cutover rather than eliminating it. For a very large slot this is still a full copy.

## Hard problems / open questions

1. **Getting the exists (historical) bulk to the downstream — Tier A vs Tier B.** *(Design fork.)* The binlog is
   a **logical write-log**: its only data content types are `XWalV`, `XBigStrings`, `XDict`
   (`src/main/java/io/velo/repl/incremental/`) — there is **no raw chunk-segment type**. The exists phase copies
   **physical, already-persisted historical segments** whose originating writes have rotated out of the binlog,
   so they cannot be replayed as binlog records. Both `XWalV.apply` (catch_up) and the exists apply write via
   `targetWal.put` directly, bypassing the binlog. Two ways forward:
   - **Tier A (recommended):** the **downstream re-fetches the exists phase physically from the target** (reuse
     existing exists machinery) once the target holds the data, and the target relays only the **incremental
     `catch_up`** window as binlog records (`XWalV`/`XBigStrings` appended when `binlogOn`). Closes the
     post-cutover **no-replica window** (downstream syncs before cutover). Does **not** avoid the exists
     physical copy — it moves it earlier. Moderate effort, no new on-disk format.
   - **Tier B:** add raw-segment binlog content types (`XChunkSegments`, big-string files, dict) so the whole
     import — exists + catch_up — relays as binlog records; the downstream never physically re-fetches. Also
     achieves the **no-full-re-sync** benefit, but is a large new-format change with its own compatibility and
     compaction concerns.
2. **Chained readiness.** *(Resolved for Tier A.)* Rather than a live two-hop chain, Tier A keeps the target
   from serving the slot until its own upstream has caught up (`hasCaughtUpUpstream`), so a downstream never
   subscribes to a still-importing (empty) slot and stalls. The downstream resyncs *after* import, and the
   target gates cutover on `downstreamAllCaughtUp` — so readiness is a single check on the target, not a
   composed two-hop lag.
3. **`masterUuid` identity across cutover.** *(Resolved.)* The target slot's `masterUuid` is `final`
   ([`OneSlot.java:329`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java)) — stable across
   `addUpstream`/`dropUpstream`. So a downstream that follows the **target's** binlog under the target's stable
   `masterUuid` keeps tracking one identity through migration *and* cutover. The one requirement this imposes:
   `dropUpstream` must be a **soft promote** — unlike `resetAsMaster`, it must *not* rebase/reopen the binlog to
   the source's offset (that would break a downstream mid-follow). It only clears the catch-up-from-source
   bookkeeping and lifts readonly, keeping the binlog position continuous. Implemented in `OneSlot.dropUpstream`.
4. **Rollback.** *(Done — Task 11.)* `manage slot N migrate_from_abort` reverts the target (drop upstream, drop
   relay downstream, discard partial data, clear the import marker); `clusterx migrate N cancel` clears the
   source's export marker (source keeps ownership, untouched).
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
- **`addUpstream` / dual-role** is **Phase 2**: complementary, not competing. Bug 61 keeps a node from serving a
  slot it will never own; Phase 2 lets the migrate target serve a slot it *has* acquired, closing the churn and
  the durability window. (Tier A does **not** close the re-sync cost — that is Tier B.)

## Phased plan

- **Phase 1 (done):** bug 61 ownership gating — stop the churn/NPE, keep migration correct. Shipped, T1-green.
- **Phase 2 Tier A (done & committed, T1/T2/T3-green):** dual-role migration via `addUpstream`/`dropUpstream`.
  - `OneSlot.addUpstream`/`dropUpstream` primitives (dual-role; soft-promote preserves binlog continuity).
  - Identity model resolved: target slot `masterUuid` is stable, so a downstream tracks it through cutover.
  - `catch_up` relay: `XWalV.apply` re-logs applied writes (self-gated on `binlogOn`); big-string catch_up
    already relays via `OneSlot.put`.
  - Wiring: `migrate_from`→`addUpstream`, `setslot`→`dropUpstream`.
  - Serve predicate = `owned || hasCaughtUpUpstream` — a slot is served to a downstream only once the target has
    finished importing it, so a replica never subscribes to a still-importing (empty) slot and stalls.
  - **Durability-window closure (node-internal, no new command):** the target's own `setslot` gates on its
    downstream `ReplPair.allCaughtUp` (`downstreamAllCaughtUp`), deferring (retryable) until synced, then
    soft-promotes. The migrate sequence resyncs the target's replicas after import (`migrate_from force` forces a
    fresh fetch even from the same master) and broadcasts cutover **charge node first, source last** (the source
    flushes only after the new owner has promoted with a synced replica). No external "downstream-caught-up"
    query and no `migrating_state` change — the target owns the information and the wait lives where the state is.
  - Import rollback (Task 11): `migrate_from_abort` / `clusterx migrate ... cancel`.
- **Remaining:**
  - **Tier B** — raw-segment binlog content types so the whole import relays and the downstream never does a full
    physical re-fetch (the one Tier-A benefit still outstanding). Large, new on-disk format.
  - **`releaseOneSlot` (Task 13)** — reclaim a shed slot's resources / restore the lazy-init invariant.

> *Deeper alternative (Tier B):* relay the exists phase via new binlog content types, so the replica follows the
> target as an ordinary slave with no explicit re-fetch trigger or cutover wait at all — at the cost of a new
> on-disk binlog format (see the Tier A/B fork above).

## Test plan (Phase 2)

- **Durability:** kill the new owner immediately after `setslot`; assert the migrated slot's data survives on
  its replica (no no-replica window).
- **Efficiency (Tier B only):** migrate a large slot; assert the downstream replica does **not** perform a full
  physical fetch (it followed the relay). Not applicable to Tier A, which still does a physical exists-fetch (the
  resync), just before cutover instead of after.
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
