# Bug 63 — Cluster module review (round 1)

## Bug 63: cross-node slot migration skips fetching the source's trained dicts (potentially undecodable values)

**Severity:** Medium–High (suspected; needs a reproduction). Migrated values compressed with a
per-key-prefix trained dict that exists on the source but not the target could be undecodable on the
target after migration.

**Status:** Reported, not yet reproduced or fixed. Filed as the "adjacent concern" surfaced while fixing
[bug 62](bug_62_cluster_module_review_round_1.md).

### Cited files / lines

- `src/main/java/io/velo/command/XGroup.java:702-708` — non-first slot skips the exists-dict fetch.
- `src/main/java/io/velo/command/XGroup.java:1784-1799` — the (now `isAsSlave()`-gated) first-slot dict
  barrier in `s_catch_up`.
- `src/main/java/io/velo/DictMap.java` — node-wide singleton of per-key-prefix trained dicts
  (`cacheDict`, `cacheDictBySeq`), persisted to `dict-map.dat`.
- `src/main/java/io/velo/Dict.java:32,37` — `SELF_ZSTD_DICT` is a fixed built-in (never replicated); the
  dicts that move over the wire are the trained per-prefix ones (seq > 1).

### Code excerpt (`hi`, slave side)

```java
// dict is global, only the first slot fetches it. A node that owns no client slots yet (null
// firstOneSlot()) is migrating in a single slot that leads its own fetch, so it must not skip.
var firstSlot = localPersist.firstOneSlot();
if (firstSlot != null && firstSlot.slot() != slot) {
    log.warn("Repl slave skip fetch exists dict, slot={}", slot);
    return fetchExistsBigString(slot, 0);   // <-- skips exists_dict entirely
}
```

### Root cause / hypothesis

The replication design assumes a slave mirrors **one master's entire node-wide DictMap**: the first slot
fetches all trained dicts once, and every other slot skips the dict fetch and relies on them being
present (serialized by the barrier — see bug 62).

That assumption holds for a **full-node fresh sync / 2N scale-up** (target and source share a dict
domain). It does **not** hold for a **cross-node single-slot migration**: the imported slot is almost
always a *non-first* slot on the target, so it takes the skip branch and never fetches the **source's**
trained dicts for that slot's key prefixes. If the source trained a dict (seq > `SELF_ZSTD_DICT_SEQ`)
that the target's DictMap does not already contain, the migrated values compressed with it cannot be
decompressed on the target.

### Why the existing tests did not catch it

- `ClusterMigrationStressManualTest` T1/T3 migrate **big-string** values (a different, non-dict path).
- T4/T5 migrate **plain short** values below `TO_COMPRESS_USE_SELF_DICT_MIN_DATA_LENGTH` (256), so no
  trained dict is used.
- No test writes enough same-key-prefix, dict-trained values on a source and then migrates that slot to a
  target with a *disjoint* DictMap.

### Suggested reproduction

1. On the source node, write many values sharing a key prefix, each ≥ 256 bytes and compressible, until a
   per-prefix trained dict (seq > 1) is created in the source's `DictMap` (confirm via `dict-map.dat` or a
   dict metric).
2. Migrate that slot to a target whose `DictMap` does **not** contain that seq.
3. Assert every migrated value reads back with the exact original bytes on the target (and its replica).
   Expected today: a decode failure / wrong bytes for dict-compressed values.

### Candidate fixes

- **Preferred (also retires the bug-62 barrier class):** each stream fetches the dicts it needs and merges
  them into the node-wide `DictMap` idempotently (`putDict` is already idempotent by seq). Removes the
  skip+barrier coupling entirely and makes cross-node migration correct by construction. See the design
  note `docs/plans/2026-07-08-repl-dict-sync-decoupling.md`.
- **Minimal:** in the skip branch, when the sync is a single-slot migration (source is a different dict
  domain), do **not** skip — have the imported slot fetch the source's dicts before applying its data.

### Cross-reference

- Depends on / relates to [bug 62](bug_62_cluster_module_review_round_1.md) (same barrier/skip design).
- Design discussion: `docs/plans/2026-07-08-repl-dict-sync-decoupling.md`.
