# Simpler Replication, Heavier Controller — Design Review & Plan

**Date:** 2026-07-09
**Status:** Design proposal (no code changes yet). Supersedes the ad-hoc direction in the bug-67 discussion.
**Branch context:** `feat/oneslot-lazy-init`.
**Related:** `20_slot_migration_replication_model.md`, `09_replication_design.md`, `18_scale_up_design.md`,
`doc/bug_reviews/bug_66_net_module_review_round_1.md` (bugs 66/67), lazy-init plan
`docs/plans/2026-07-03-oneslot-lazy-init.md`.

## Guiding principle

Move topology intelligence **out of the replication layer** and **into the cluster controller**. The
controller is the single source of topology truth; every node is a **dumb executor** that reconciles its local
state to controller broadcasts. Replication becomes a pure, command-driven data-transfer mechanism with no
self-healing heuristics.

- **Replication layer (simpler):** `hello` establishes a pair (sole creator), `ping` is pure liveness,
  `exists_*`/`catch_up` move data. No inferring topology from repl signals, no fabricating pairs, no re-hello
  heuristics.
- **Controller (heavier):** owns all topology transitions, broadcasts them authoritatively with
  acknowledgement + retry, replays them to a (re)joining node, and **sequences** them so the replication layer
  is never asked to do something it cannot yet do (e.g. subscribe a replica to a still-importing master).

This is not a rewrite: the controller-broadcast-drives-local-state model already exists for `setslot` /
`setnodes` / `migrate_from`. The plan removes the *competing* self-healing paths in replication and hardens the
controller's delivery + sequencing so it is the sole mechanism.

---

## Current architecture (grounded in code)

### Controller side (already exists)
- `src/main/groovy/io/velo/repl/cluster/watch/FailoverManager.groovy` / `Guarder.groovy` — the leader/controller.
  Detects a dead master via ZooKeeper (`checkFailover` `:375`, `doFailover` `:540`, `doFailoverOneShard` `:580`)
  and **broadcasts `clusterx setnodes` over Jedis** to the surviving nodes (`:731`). It never calls `OneSlot`
  role methods directly — it drives nodes through broadcast commands.

### Node side — controller broadcasts reconcile local state
Every topology command is handled locally by each node and maps to `OneSlot`/`LocalPersist` primitives:

| Operation (command) | Handler | Init | Release | Role primitives |
|---|---|---|---|---|
| migrate_from (import target) | `ManageCommand.migrateFrom` | `ensureOneSlotInitialized` :592 | — | `addUpstream` :627 (`removeReplPairAsSlave` first on `force`) |
| migrate_from_abort | `ManageCommand` :639 | — | — | `removeReplPairAsSlave` :669, `removeReplPairAsMaster` :670, `flush` |
| migrate (source start) | `ClusterxCommand.migrate` :621 | — | — | sets `exportMigratingSlot` only |
| migrate cancel | `ClusterxCommand` :645 | — | — | clears export marker, `readonly=false` :659 |
| **setslot (cutover, broadcast)** | `ClusterxCommand.setslot` :1219 | `ensureOneSlotInitialized` :1285 (target shard) | `releaseOneSlot` :1393 (source shed) | charge: `dropUpstream` :1365; replica: `resetAsSlave` :1384; source: `removeReplPairAsSlave`+`removeReplPairAsMaster`+`flush` |
| setnodes (join/leave, broadcast) | `ClusterxCommand.setnodes` :1120 | `ensureOneSlotInitialized` :1210 | — | `resetAsMaster` (→#master) / `resetAsSlave` (→#slave) |
| failover / `SLAVEOF NO ONE` | `ClusterxCommand.failover` :287 / `SGroup.slaveof` | — | — | `LeaderSelector.resetAsMaster` → `resetAsMaster` per **initialized** slot |
| replicate / `SLAVEOF host port` | `ClusterxCommand.replicate` :868 | — | — | `LeaderSelector.resetAsSlave` → `resetAsSlave` per **initialized** slot |
| CLUSTER RESET | `ClusterxCommand.reset` :951 | — | — | `resetAsMaster`+`flush` all slots |

Note the epoch guards already in place (`setslot` :1240/:1305/:1357, `setnodes` :1120): a stale/reordered
broadcast is rejected — the foundation for authoritative delivery is present.

### Replication layer — where it currently carries *controller-like* intelligence (the smells)
These are the paths that let replication infer or repair topology on its own, which this plan removes:

1. **ping-create resurrection** — `XGroup.java:328`. A `ping` (liveness) creates a downstream master pair when
   none exists and the slot is servable. The pair is **incomplete**: ping carries no repl properties and sends
   no `Hi`, so it cannot serve the `exists_*` phase (`requireRemoteReplProperties` throws) and the slave never
   bootstraps. Root cause of **bug 67**.
2. **pong serve-flag topology sync (bug 61)** — `XGroup.java:324` computes `serveSlot`, `:363` the slave drops
   its subscription when the master replies `serve=0`. This is replication *inferring* "the master no longer
   owns this slot" from a repl signal, duplicating what the controller broadcast already says.
3. **re-hello heuristic (bug 67 fix, `202ce156`)** — `XGroup.java:374`. The slave re-sends `hello` when
   `serve=1 && masterUuid==0`. A defensive net compensating for the handshake/controller race.
4. **side-channel catch-up on disconnect** — `XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed` :2064, opening a
   one-shot Jedis `catch_up` (:2100). A self-heal hack because `TcpClient` has **no auto-reconnect**
   (`TcpClient.connect` :167 only logs on failure; re-establishing a slave pair is only re-driven by
   `resetAsSlave`/`addUpstream`).

### The seam (why self-healing exists at all)
- `LeaderSelector.resetAsSlave`/`resetAsMaster` iterate **`initializedOneSlots()`** (`:565`, `:396`) — they
  reconcile only slots the node already initialized. A node that is **missing** a slot its shard should own/
  replicate is not fixed by these paths. Today the ping/pong sync and ping-create paper over the resulting
  transient-null and missing-pair states.
- `TcpClient` never reconnects. So any pair lost while the socket stays alive (topology-switch transient null)
  relies on ping-create; any pair lost with the socket closed relies on an external re-subscribe. Neither is a
  clean, controller-owned recovery.

---

## Target design

### Replication layer (delete self-healing)
- `hello` is the **sole** pair creator (already carries repl properties + sends `Hi`).
- `ping` is **pure liveness** — never creates a pair.
- Remove ping-create (`XGroup.java:326-331`).
- Once the controller reconcile is authoritative (below), the pong serve-flag drop (#2) and the re-hello
  heuristic (#3) become **redundant** and are removed in a later phase — a node drops/adds a slot because the
  controller *told* it to, not because a pong hinted at it.

### Controller (make authoritative + heavier)
Chosen model: **controller re-broadcasts to a (re)joining node.** Nodes stay dumb; all intelligence is in the
controller.

1. **Authoritative delivery** — extend the existing `setnodes`/`setslot` broadcast with per-node
   acknowledgement + retry, so a node that missed a broadcast (down, slow, partitioned) is brought current. The
   epoch guards already make redelivery idempotent.
2. **Rejoin replay** — on detecting a node (re)join, the controller replays the authoritative per-slot topology
   to *that* node (a targeted `setnodes` re-broadcast, which already calls `ensureLocalShardSlotsInitialized` →
   `ensureOneSlotInitialized` per owned slot at `:1210`, then `resetAsMaster`/`resetAsSlave`). This is the
   piece that **replaces** replication self-healing: the node reconciles from the broadcast, not from repl
   signals.
   ```
   controller on nodeJoin(n):
     for slot in authoritativeTopology:
       sendSetslot(n, slot, owner, epoch)   // node handles each as normal; acks + retries
   ```
3. **Sequencing** — the controller orders transitions so replication is never asked to do the impossible:
   - subscribe a replica to an importing master **only after** the master's upstream has caught up
     (`hasCaughtUpUpstream`), so `hello` is never declined → **bug 67 becomes a controller-ordering property,
     not a replication bug** (the `Thread.sleep(2000)` in the bug-67 test was compensating for this);
   - keep the existing charge-node-first / source-last cutover ordering (`setslot` already does this).

### What stays
- The lazy-init + `releaseOneSlot` wiring in `setslot`/`setnodes`/`migrate_from` — it *is* the
  controller-driven local reconcile; keep it.
- `addUpstream`/`dropUpstream` dual-role migration (Tier A) — orthogonal and correct.
- Epoch guards — the correctness backbone of authoritative delivery.

---

## Delete-list (replication) / Add-list (controller)

**Delete (replication):**
- `XGroup.java:326-331` — ping-create block (Phase 1).
- `XGroup.java:343-393` pong serve-flag drop + re-hello — after controller reconcile lands (Phase 3).
- Re-evaluate `tryCatchUpAgainAfterSlaveTcpClientClosed` (`XGroup.java:2064`) — replace the Jedis side-channel
  with controller-driven re-subscribe (Phase 3).

**Add (controller):**
- Per-node ack + retry on `setnodes`/`setslot` broadcast (`FailoverManager.groovy` / the controller entry).
- Rejoin detection → targeted topology replay to the joining node.
- Sequencing guard: don't emit the replica-subscribe step until the master reports `hasCaughtUpUpstream`.

---

## Phased plan (TDD; one concern per commit)

- **Phase 1 — remove ping-create.** Make `ping` pure liveness. Keep the shipped re-hello (`202ce156`) as the
  temporary net so no regression while the controller work is pending. Tests: XGroup ping handler no longer
  creates a pair; existing migration process tests stay green.
- **Phase 2 — controller authoritative delivery + rejoin replay.** Ack/retry on broadcast; targeted replay on
  join. Tests: a node that missed a `setslot` converges after the controller replays; idempotent under epoch
  guard.
- **Phase 3 — controller sequencing (in the controller repo), then retire repl self-healing.** See the
  controller-reality note below: the sequencing work lives in `segment_kvrocks_controller`, not Velo.
  Only after it lands + process-tests pass do we remove the pong serve-flag drop, the re-hello heuristic,
  and the Jedis side-channel from Velo.

### Controller reality (segment_kvrocks_controller, kvrocks-style CLI)

Velo's cluster controller is an external CLI, designed like the Apache Kvrocks controller
(`~/ws/segment_kvrocks_controller`). Migration is orchestrated by `MigrateSlotsJobTask` /
`KvrocksDBOperator`:

1. `migrateFromSlot` → target: `manage slot X migrate_from <source> force` (import).
2. `migrateSlot` → source: `clusterx migrate`, then **`waitUntilMigrateSuccess`** polls the source's
   `migrating_state` until `success` (target caught up), max ~5 min.
3. `setSlotAll` → broadcasts `clusterx setslot` to every node (cutover).

Two facts this establishes:

- **Bug 67's precondition doesn't arise under correct controller operation.** The replica subscribes only at
  the `setslot` broadcast — *after* `migrating_state:success` — so the new master already has
  `hasCaughtUpUpstream == true` and accepts the hello. Bug 67 fired only because the **test harness** issued
  the replica's `migrate_from` *during* import. So the re-hello net and the pong serve-flag drop are defense
  against **manual / out-of-sequence** operation, not the controller happy path.
- **The real gap is in the controller, not Velo.** The charge node's F3 gate returns
  `ErrorReply('downstream replica not caught up, retry setslot')` (`ClusterxCommand.groovy:1423`) until its
  replica has subscribed + caught up. But the controller's `setSlot` **throws on any non-OK and never
  retries**, and it does **not** pre-resync the replica before cutover — so for a shard **with replicas**,
  `setSlotAll` would hit the defer and fail the migration.

**Phase 3 controller work (in `segment_kvrocks_controller`):** after `waitUntilMigrateSuccess`, add a
pre-cutover step that resyncs the target's replicas (`manage slot X migrate_from <newMaster> force` on each
replica) and **waits** until they are caught up (mirror `waitUntilMigrateSuccess`, or poll the charge node's
downstream-caught-up signal), *then* broadcast `setSlotAll`. This makes the F3 gate pass on the first
`setslot` and guarantees the replica only ever subscribes to a master that already serves — removing the last
reason the Velo self-healing nets exist. Alternatively/additionally, make `setSlot` retry on the F3 defer
error instead of throwing.

**Velo work (only after the above + process tests):** remove the pong serve-flag drop, the re-hello net, and
the Jedis side-channel. Until then, **keep the nets** — they are the defense against manual/out-of-sequence
migration that the controller cannot prevent from Velo's side.

Each phase: relevant tests + JaCoCo on changed lines, single-concern commit.

---

## Validation status (2026-07-09)

- **Phase 1 (velo `3d656072`)** — done. `XGroup` ping no longer creates a master pair; unit tests green.
- **Phase 2 (velo `f356b265`)** — done. `FailoverManager` setnodes broadcast retries per node and registers
  un-acked nodes for the existing rejoin re-push instead of aborting; unit tests green.
- **Phase 3 (controller `90cfbc4`)** — done in `segment_kvrocks_controller`: `resyncTargetReplicasBeforeCutover`
  (resync each target replica after `migrating_state:success`, before cutover) + `setSlotWithRetry` (waits out
  the F3 defer). Unit tests green (5 Spock cases). This makes replica-backed migration work and keeps a replica
  from ever subscribing to a still-importing master.
- **End-to-end (velo)** — the full opt-in 4-JVM suite passed after Phase 1:
  `ClusterMigrationStressManualTest` (7) + `ClusterOperationProcessManualTest` (18) = **25/25 green** in ~8 min.
  Covers replica-backed migration + durability (T1/T2/T3/T6), the F3 gate (T6), failover, churn, MOVED, epoch
  monotonicity, reset/rebalance, and introspection — confirming ping-as-pure-liveness causes no regression.

### T9 (dedicated replica-backed health e2e) — **skipped, intentional**

A standalone "replica-backed migration with no-stall / no-`Repl handle error` / no-NPE" test was planned as a
Phase-1 regression fence. **Not added**, because the path is already covered:
- **T1** asserts data lands on both the new owner and its replica after a replica-backed migration.
- **T3** asserts the replica holds the data *at* cutover (synced before, not after) — i.e. the F3 gate worked.
- **T6** exercises the F3 defer → replica resync → cutover explicitly and completed cleanly with ping-create
  removed.
- **T8 / T11-churn** already assert `countServerLog(..., 'Repl handle error') == 0` across migration endpoints.

Adding T9 would duplicate that coverage. If a dedicated fence is wanted later, the delta over T1/T3 would be
only the cross-all-nodes health assertions (`no 'exists data not all fetched'`, `no 'Repl handle error'`, `no
NullPointerException`), which the churn tests already partially assert.

### Self-healing nets — **kept** (not retired)

Per the plan, the Velo repl-layer nets (re-hello `202ce156`, pong serve-flag drop) stay as cheap defense
against manual/out-of-sequence `migrate_from`. The controller now sequences correctly, so under controller-
driven operation they are redundant — but removing them is deferred; they cost ~15 lines each and protect a
real operator-error path.

---

## Open risks / decisions

- **Delivery guarantees.** "Heavier controller" concentrates correctness in broadcast delivery. Needs ack +
  retry + a bounded convergence guarantee; a permanently-partitioned node must be fenced (epoch already fences
  stale writers).
- **Single controller vs. gossip.** The `setslot` comment (`:273`) already prefers "a central controller." This
  plan commits to that. Multiple controllers would reintroduce the concurrency the epoch guards only *defend*
  against, not prevent.
- **`LeaderSelector.resetAsSlave` over `initializedOneSlots()`.** Once rejoin-replay lands, the missing-slot
  case is handled by the controller re-broadcast (which `ensureOneSlotInitialized`s first), so this iteration
  set is no longer a gap — but confirm no other caller depends on the old "only touch initialized" behavior.
- **bug 65 (stale export pair on re-migrate).** Independent; still tracked in its own review doc. Controller
  sequencing may subsume part of it (don't start a re-migrate until the prior export pair is torn down).
- **Whether to keep the re-hello net after Phase 3.** Recommend removing it once sequencing is proven, to keep
  replication free of topology heuristics; revisit if operational data shows a residual race.
```
