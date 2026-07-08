# Bug 64 — Cluster module review (round 1)

## Bug 64: `clusterx migrate <slot> cancel` leaves the source slot stuck readonly

**Severity:** High (after a cancelled migration the source still owns the slot but rejects all writes with
`ERR readonly` until manually cleared or the process restarts)

**Discovered by:** process test `ClusterMigrationStressManualTest` T5 (import abort/rollback). After
`clusterx migrate 8192 cancel` on the source, a subsequent `SET` to the slot fails:
`redis.clients.jedis.exceptions.JedisDataException: ERR readonly`.

### Cited files / lines

- `dyn/src/io/velo/command/ClusterxCommand.groovy:645-651` — the `cancel` branch clears
  `exportMigratingSlot` but does not reset `oneSlot.readonly`.
- `dyn/src/io/velo/command/ClusterxCommand.groovy:573-575` — the `info` export path sets the source slot
  `readonly = true` once the migrating slave is caught up.

### Code excerpt

```groovy
// info(): source goes readonly once the slave caught up, so the cutover is consistent
if (replPairAsMaster.allCaughtUp) {
    if (!oneSlot.readonly) {
        oneSlot.readonly = true
    }
    return 'success'
}

// migrate() cancel branch: clears the marker but NOT readonly
if ('cancel'.equalsIgnoreCase(toNodeId)) {
    if (mySelfShard.exportMigratingSlot == toClientSlot) {
        mySelfShard.exportMigratingSlot = Shard.NO_MIGRATING_SLOT
        log.warn 'Clusterx migrate cancel export migrating slot={}', toClientSlot
    }
    return OK
}
```

### Root cause

During export, once the migrating slave reports caught-up (observed through `clusterx info`), the source
sets the slot `readonly = true` so no writes are lost in the window between "caught up" and the ownership
cutover. On a successful cutover the source sheds/flushes the slot, so the readonly flag goes away with
it. But on **cancel**, the source keeps the slot — and nothing clears `readonly`. The slot is left owned
but write-rejecting.

### Impact

Any aborted migration (`clusterx migrate <slot> cancel`) that got far enough for the slave to catch up
leaves the source unable to serve writes for that slot, silently, until `manage slot <n> set-not-readonly`
or a restart. Reads still work, so it can go unnoticed.

### Fix

In the `cancel` branch, reset the slot to writable. Because `readonly` is persisted via the slot's
`DynConfig`, run the reset on the slot thread (mirror the `info`/`setslot` `asyncRun` pattern) and return
an `AsyncReply`; keep the synchronous `OK` fast path when the slot was not readonly.

### Regression fences

- Unit: `ClusterxCommandTest` `test migrate` — after arming an export, marking the source readonly, then
  `cancel`, assert the slot is writable again (`!oneSlot.readonly`).
- Process: `ClusterMigrationStressManualTest` T5 — abort an in-flight import, then `SET` on the source
  succeeds.
