# Bug 62 — Cluster module review (round 1)

## Bug 62: migrating a non-first inner slot onto a node that owns its first slot as master stalls forever

**Severity:** High (migration of any non-first inner slot never completes; the source is stuck exporting)

> **Update (2026-07-08):** the minimal `isAsSlave()` gate below (`dbb0228c`) has since been superseded —
> the first-slot barrier it guarded was removed entirely by the dict-sync decoupling refactor, so the stall
> can no longer occur. See `docs/plans/2026-07-08-repl-dict-sync-decoupling.md`.

**Discovered by:** process-level test `ClusterMigrationStressManualTest` (T4 / T5), migrating client slot
8192 (inner slot 2) between real JVM nodes. The source never reaches `migrating_state:success`; the
import on the target completes its exists phase and then silently stops making progress until the 90 s
wait times out.

### Cited files / lines

- `src/main/java/io/velo/command/XGroup.java:1784-1796` — the `s_catch_up` first-slot barrier gate.
- `src/main/java/io/velo/command/XGroup.java:1649-1653` — the only place the barrier flag is set true
  (first slot completes `s_exists_all_done` **as a slave**).
- `src/main/java/io/velo/command/XGroup.java:704-708` — non-first slot skips fetching exists dict.
- `src/main/java/io/velo/persist/LocalPersist.java:691-698` — `isAsSlaveFirstSlotFetchedExistsAllDone`
  node-global flag, default `false`.
- `src/main/java/io/velo/persist/OneSlot.java:357-372` — `isAsSlave()` signal.

### Code excerpt (`s_catch_up`, current)

```java
// null firstOneSlot() (node owns no client slots yet) means this slot is the de-facto first slot,
// so it leads rather than waits.
var firstOneSlotForWait = localPersist.firstOneSlot();
if (firstOneSlotForWait != null && slot != firstOneSlotForWait.slot()) {
    // wait the first slot fetched exists all done
    if (!localPersist.isAsSlaveFirstSlotFetchedExistsAllDone()) {
        var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
        var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
        oneSlot.delayRun(catchUpIntervalMillis, () -> {
            replPair.write(catch_up, content);
        });
        return Repl.emptyReply();
    }
}
```

### Root cause

The barrier `isAsSlaveFirstSlotFetchedExistsAllDone` exists so that, on a **full-node slave sync**, the
non-first slots wait until the **first slot** has fetched the node-wide per-key-prefix trained dicts
(`DictMap` singleton) and reached `s_exists_all_done`. The flag is set true **only** by the first slot's
own slave-side `s_exists_all_done` (`XGroup.java:1651`).

During a **single non-first-slot migration** onto a node that already owns its first slot **as master**:

- `firstOneSlot()` returns that first slot (non-null), so the "de-facto first slot" escape at line 1785
  does not apply (that escape only covers a *zero-owned* node, fixed earlier in `be06d260`).
- The first slot is owned as master and is **never slave-synced**, so its `s_exists_all_done` never runs
  and the flag stays `false` for the life of the process.
- The imported non-first slot finishes its own exists phase (it correctly skips the dict fetch at
  `XGroup.java:704-708` because the node already holds the node-wide dicts), then enters `s_catch_up`,
  sees the flag still `false`, and **reschedules catch-up indefinitely**. `allCaughtUp` never flips, so
  the exporting source's `clusterx info` never advances past `migrating_state:migrating`.

Note the "dict is global" wording in the surrounding comments is stale: `Dict.SELF_ZSTD_DICT` is a fixed
built-in (never replicated); the dicts that actually sync are the per-key-prefix trained dicts in the
node-wide `DictMap`. The barrier mechanism itself is still live, only its rationale label is outdated.

### Impact

Any migration of an inner slot other than the node's first owned slot, onto a target that already owns a
lower (first) slot as master, hangs at cutover. Inner slot 0 happens to work only because it is the
first slot (and `releaseOneSlot` exempts it), which is why prior single-slot-0 tests never caught this.

### Fix

Gate the wait on whether the first slot is **itself currently being slave-synced**. If the first slot is
owned locally as master, the node-wide `DictMap` is already populated, so a non-first imported slot must
not wait:

```java
if (firstOneSlotForWait != null && slot != firstOneSlotForWait.slot()
        && firstOneSlotForWait.isAsSlave()) {
    if (!localPersist.isAsSlaveFirstSlotFetchedExistsAllDone()) { ... wait ... }
}
```

### Regression fences

- New fast unit test in `XGroupTest`: a non-first slave slot's `s_catch_up` does not wait when the first
  slot is owned locally (master), and marks the stream caught up.
- Process tests `ClusterMigrationStressManualTest` T4 (migrate inner slot 2 out + release + back) and T5
  (import abort/rollback of inner slot 2) exercise the path end-to-end against real JVMs.

### Adjacent concern (separate, not fixed here)

`XGroup.java:704-708` makes a non-first migrated slot **skip fetching the source's per-key-prefix trained
dicts**. That is safe for a full-node replica (same dict domain as the master) but for a **cross-node**
slot migration the source may hold trained dict seqs the target lacks, which could make dict-compressed
migrated values undecodable on the target. Tracked separately.
