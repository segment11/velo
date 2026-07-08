# Design note: decouple replication dict-sync from the first-slot barrier

**Date:** 2026-07-08
**Owner:** kerry
**Related:** `doc/bug_reviews/bug_62_cluster_module_review_round_1.md`,
`doc/bug_reviews/bug_63_cluster_module_review_round_1.md`

## Problem

During a slave sync, only the **first** slot fetches the node-wide per-key-prefix trained dicts
(`DictMap`); every **non-first** slot **skips** the dict fetch (`XGroup.java:702-708`) and then **waits**
in `s_catch_up` until the first slot has finished its exists phase, gated by the node-global boolean
`isAsSlaveFirstSlotFetchedExistsAllDone` (`XGroup.java:1784-1799`).

This couples two decisions — *skip dict fetch on non-first slots* and *global-barrier ordering* — that
stand or fall together. The coupling has two recurring costs:

1. **Fragile global-boolean barrier.** The single flag has produced three bugs so far:
   - `be06d260` — zero-owned node (null `firstOneSlot()`).
   - `9d20f5a7` — barrier not reset on master-uuid change (failover).
   - **bug 62** — first slot owned locally as master (single non-first-slot migration stalled forever).
   Each new topology shape not imagined when the flag was written becomes a stall.

2. **Wrong assumption for migration.** The design assumes a slave mirrors *one master's whole DictMap*.
   That holds for full-node fresh sync / 2N scale-up, but **not** for a cross-node single-slot migration,
   where the imported (non-first) slot skips fetching the *source's* dicts and may end up unable to
   decode dict-compressed migrated values (**bug 63**).

There is also head-of-line blocking: every non-first slot's catch-up waits behind slot 0's *entire*
exists phase, not just behind the (small) dicts.

## Proposal

Replace "skip + global barrier" with **per-stream idempotent dict sync**:

- Each replication stream (each slot) fetches the dict seqs it needs from *its own* master and merges them
  into the node-wide `DictMap`. `DictMap.putDict` is already idempotent by seq, so concurrent/duplicate
  merges from multiple slots are safe.
- Remove the non-first-slot skip branch (`XGroup.java:702-708`) and the `s_catch_up` first-slot barrier
  (`XGroup.java:1784-1799`) together with the `isAsSlaveFirstSlotFetchedExistsAllDone` flag and its
  reset sites (`XGroup.java:648-657`, `1649-1653`).

### Why this is better

- **Removes the entire barrier bug class.** No global flag, no ordering assumption about which slot leads.
- **Fixes bug 63 for free.** A migrated slot fetches its source's dicts as part of its own exists phase,
  so cross-node migration is correct by construction.
- **No head-of-line blocking.** Slots sync independently.

### Cost / tradeoff

- Redundant dict transfer: N slots may each fetch overlapping dict sets. Dicts are small relative to slot
  data (trained Zstd dictionaries, typically ≤ tens of KiB each, and the fetch sends only the seqs the
  slave is missing). The bandwidth cost is negligible next to the correctness and robustness win.
- Slightly more work in the exists phase per slot (one extra dict round-trip). Bounded and parallelizable.

## Migration / rollout

1. Add an idempotent `merge dicts by seq` step to every slot's exists phase (not just slot 0).
2. Keep the barrier in place but make it a no-op behind a flag; verify full-node sync + 2N scale-up +
   single-slot migration all converge with per-stream dict fetch.
3. Delete the skip branch, the barrier, and the global flag once the per-stream path is proven.

## Test coverage to add

- **Bug 63 reproduction** (see that doc): migrate a slot whose values use a source-only trained dict to a
  target with a disjoint `DictMap`; assert exact bytes on target + replica.
- **Full-node fresh sync** still converges with dicts present on all slots (regression for the removed
  barrier).
- **2N scale-up** stream readiness unaffected.
- **Concurrent multi-slot** sync from the same master: no `DictMap` corruption (idempotent merge).

## Decision

Not started. Recorded so the bug-62 minimal fix (`isAsSlave()` gate) and the bug-63 concern are on record
with a concrete better-design path before deciding whether to invest in the rework.
