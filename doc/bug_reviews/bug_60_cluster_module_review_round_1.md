# Bug 60 — Big-string values break slot migration (migration hangs, never completes)

**Module:** cluster / replication (big-string exists-phase apply)
**Round:** 1 (found by process-level test `ClusterMigrationStressManualTest` T1)
**Severity:** HIGH — a slot that contains any big-string value cannot be migrated; the
migration stalls at `migrating_state:migrating` forever and `SETSLOT` cutover never runs.

## How it was found

`ClusterMigrationStressManualTest.'T1: big string values survive slot migration'`
(opt-in, `VELO_CLUSTER_MANUAL=true`) writes five 8 KiB values (above the configured
4096 big-string threshold) into slot 0, confirms they became big strings on disk,
then migrates slot 0 from node-0 to node-1. `migrateSlot` times out waiting for
`migrating_state:success`:

```
Timed out waiting for migrating_state:success, last info=cluster_state:ok
... migrating_state:migrating  export_migrating_slot:0
```

## Evidence (target node-1 server.log, during migrate-in of slot 0)

```
ERROR io.velo.BaseCommand(263) - Repl handle error, slot remote=0, repl type=exists_big_string
java.lang.NullPointerException: Cannot invoke "io.velo.repl.ReplPair.getSlaveUuid()" because "replPair" is null
    at io.velo.command.XGroup.exists_big_string(XGroup.java:1233)
...
ERROR io.velo.BaseCommand(833) - Repl slave exists apply error, slot=0
io.velo.persist.ReadonlyException
    at io.velo.command.XGroup.lambda$s_exists_big_string$24(XGroup.java:1334)
    at io.velo.command.XGroup.s_exists_big_string(XGroup.java:1334)
Repl slave try catch up again after slave tcp client close, but exists data not all fetched, slot=0
```

The slave (target) never finishes the `exists_big_string` phase, so it never reaches
`catch_up`, so the master (source) never sets `allCaughtUp`, so `migrating_state`
never flips to `success`.

## Primary root cause — ReadonlyException applying the big-string bookkeeping set

`XGroup.s_exists_big_string` applies fetched big strings in two writes per bucket:
1. the big-string **bytes** → `targetOneSlot.getBigStringFiles().writeBigStringBytes(...)`
   (`XGroup.java:1327`) — a raw file write, bypasses the slot readonly check; OK.
2. a **bookkeeping redis-set** (`exists_big_string_uuids_bucket_index_N`) recording which
   uuids were fetched → `SGroup.saveRedisSet(rhk, sSet, this, dictMap)` on `firstOneSlot`
   (`XGroup.java:1334`).

`saveRedisSet` (`SGroup.java:1092`) calls `baseCommand.set(...)` → `OneSlot.put(...)` /
`putIfSeqBigger(...)`, which throw `ReadonlyException` when the slot is readonly and
`ignoreReadonly` is false (`OneSlot.java:1948`, `1980`).

During migration the migrating slot is set **readonly** by `resetAsSlave`
(`OneSlot.java:2669`), and `firstOneSlot = currentThreadFirstOneSlot()` resolves to that
very slot (slot 0 is the lowest index on the worker), so the bookkeeping write throws.
Normal replication applies (e.g. `XWalV.apply`, `XGroup.java`/`ReplPair` WAL path) write to
`targetWal.put(...)` **directly**, bypassing the readonly check — but this big-string
bookkeeping write goes through the full command `set()` path, which enforces readonly.

**Why unit tests missed it:** `XGroupTest.'test as slave handle s exists big string'`
drives `s_exists_big_string` but does not set the slave slot readonly, so `saveRedisSet`
succeeds and the readonly path is never exercised with big strings present.

## Secondary symptom — NPE `replPair == null` in the master `exists_big_string`

`XGroup.exists_big_string` (`XGroup.java:1233`, `Repl.reply(slot, replPair, ...)`) NPEs
because the XGroup `replPair` field is null. It fires on node-1 during the migrate_from
reconfiguration (node-1 is simultaneously master of its own shard with replica node-3, and
becoming slave of node-0 for slot 0). This is the long-noted "replPair-null on topology
switch" symptom. It may be a transient of the role switch rather than the completion
blocker, but it should be guarded (null-check before `Repl.reply`, or skip the response
when the pair is gone) and understood.

## Suggested fix direction

1. **ReadonlyException (blocker):** the big-string exists-phase bookkeeping write must bypass
   readonly like every other repl apply. Options: give `saveRedisSet` an `ignoreReadonly`
   path through `set()`/`put()`, or write the bookkeeping set via a repl-level WAL write that
   bypasses the readonly gate. Verify the fix against a readonly target slot.
2. **replPair NPE:** null-guard the `exists_big_string` master handler (and audit sibling
   handlers) so a request arriving mid-topology-switch returns cleanly instead of NPEing.
3. **Regression coverage:** unit test `s_exists_big_string` with the slave slot **readonly**
   and big strings present (fast repro of #1); keep T1 as the end-to-end fence.

## Reproduction
- `VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "*ClusterMigrationStressManualTest.T1*big string*" --rerun-tasks`
- Add `VELO_CLUSTER_KEEP_DIR=true` to keep node logs.

## Note on T2 (concurrent writes during migration)
T2 also timed out on `migrating_state:success`, but for a different reason: the throttled
background writer keeps advancing the source binlog so the slave never reaches the current
offset (`allCaughtUp`). That is likely a *test-pacing* issue (migration needs the write rate
to fall below catch-up throughput, or a quiesce before cutover), not necessarily a product
bug — it needs separate confirmation and possibly a "pause writes before final cutover" step.
