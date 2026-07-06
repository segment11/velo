# Velo Cluster Migration / Failover Bug Review

Date: 2026-07-04

## Scope

This note records manual e2e findings from a local Velo cluster created with `CLUSTERX SETNODES`.
The cluster bootstrap path works, but data is not preserved across slot migration or replica promotion.

Relevant local helpers:

- `scripts/e2e/velo_cluster/e2e_velo_cluster.sh`
- `scripts/e2e/velo_cluster/velo_cluster_e2e.py`
- `src/test/groovy/io/velo/repl/cluster/tmp/ClusterMigrationFailoverManualTest.groovy`

## Baseline: Cluster Creation Works

The e2e harness successfully creates a 3-shard cluster plus one replica:

```text
node-0 master 0-4095
node-1 master 4096-8191
node-2 master 8192-16383
node-3 slave of node-1
```

Replay:

```bash
KEEP_DATA=true RUN_MIGRATION=false RUN_FAILOVER=false scripts/e2e/velo_cluster/e2e_velo_cluster.sh
```

Expected/observed:

- `PING` succeeds on all four nodes.
- `CLUSTERX SETNODES` returns `OK` on all nodes.
- `CLUSTERX MYID` returns each configured node id.
- `CLUSTERX INFO` returns `cluster_state:ok`.
- `CLUSTERX NODES`, `SLOTS`, and `SHARDS` show the expected 3-shard layout.
- A wrong-owner write returns MOVED.

## Bug 1: Slot Migration Reports Success But Key Is Missing On Target

Severity: High

### Replay With Python Harness

```bash
KEEP_DATA=true scripts/e2e/velo_cluster/e2e_velo_cluster.sh
```

The migration phase performs the same sequence used by the controller:

```text
SET <slot-0-key> before-migrate                         # on old owner node-0
MANAGE SLOT 0 MIGRATE_FROM 127.0.0.1 <node-0-port> FORCE # on target node-1
CLUSTERX MIGRATE 0 <node-1-id>                           # on source node-0
poll CLUSTERX INFO until migrating_state:success          # on source node-0
broadcast CLUSTERX SETSLOT 0 NODE <node-1-id> <version>   # all nodes
GET <slot-0-key>                                          # on target node-1
```

Observed:

```text
GET on old owner returns MOVED to node-1
GET on node-1 returns null
```

Example from the final run:

```text
BUG CANDIDATE: Migrated key lost or stale on target:
key=velo-e2e:slot-0:3632 got=None
```

### Replay With Groovy Manual Test

The opt-in Groovy probe reproduces the same symptom without redis-py:

```bash
VELO_CLUSTER_MANUAL=true ./gradlew :test \
  --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" \
  --rerun-tasks
```

Observed failure:

```text
nodes[1].jedis.get(key) == 'before-migrate'
actual: null
key: velo-manual:slot-0:5858
```

### Source Analysis

Controller-equivalent import starts in `ManageCommand.migrateFrom()`:

- `dyn/src/io/velo/command/ManageCommand.groovy:560-568` sets `mySelfShard.importMigratingSlot`.
- `dyn/src/io/velo/command/ManageCommand.groovy:571-593` initializes the target inner slot and calls `oneSlot.resetAsSlave(host, port)`.

Source export starts in `ClusterxCommand.migrate()`:

- `dyn/src/io/velo/command/ClusterxCommand.groovy:649-651` sets `migratingToHost`, `migratingToPort`, and `exportMigratingSlot`.

Ownership is finalized in `ClusterxCommand.setslot()`:

- `dyn/src/io/velo/command/ClusterxCommand.groovy:1178-1180` initializes or locates the target `OneSlot`.
- `dyn/src/io/velo/command/ClusterxCommand.groovy:1182-1194` mutates slot ownership metadata.
- `dyn/src/io/velo/command/ClusterxCommand.groovy:1205-1210` removes slave replication and calls `oneSlot.resetAsMaster()` on the new owner.

Promotion itself is in `OneSlot.resetAsMaster()`:

- `src/main/java/io/velo/persist/OneSlot.java:2582-2597` reopens binlog using the recorded master binlog position, then clears master binlog metadata.
- `src/main/java/io/velo/persist/OneSlot.java:2599-2605` makes the slot readable/writable.

Current hypothesis:

The source considers migration successful once the target replica is caught up, but the target does not retain or expose the migrated key after `SETSLOT` promotion. The next likely checks are:

- Verify target slot data exists immediately after `MANAGE SLOT ... MIGRATE_FROM` and before `CLUSTERX SETSLOT`.
- Verify `oneSlot.removeReplPairAsSlave()` plus `oneSlot.resetAsMaster()` does not discard fetched data or reopen at an empty/incorrect binlog position.
- Verify the inner-slot mapping used by `ManageCommand.parseSlots()` matches `MultiShard.asInnerSlotByToClientSlot()` for the migrated client slot.
- Verify migrated data is not flushed by a later replica reset caused by broadcasting `SETSLOT` to the replica node.

## Bug 2: Forced Failover Promotes Replica But Promoted Master Misses Replicated Key

Severity: High

### Replay With Python Harness

Run failover independently of migration:

```bash
KEEP_DATA=true RUN_MIGRATION=false scripts/e2e/velo_cluster/e2e_velo_cluster.sh
```

The failover phase:

```text
SET <slot-4096-key> before-failover      # on master node-1
sleep 2 seconds                          # settle async replication
CLUSTERX FAILOVER FORCE                  # on replica node-3
broadcast CLUSTERX SETNODES ...          # node-3 master for 4096-8191
GET <slot-4096-key>                      # on promoted node-3
```

Observed:

```text
node-3 becomes master and cluster_state remains ok
GET on node-3 returns null
```

Example:

```text
BUG CANDIDATE: Failover promoted node cannot read replicated key:
key=velo-e2e:slot-4096:1409 got=None
```

### Replay With Groovy Manual Test

Same command as above:

```bash
VELO_CLUSTER_MANUAL=true ./gradlew :test \
  --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" \
  --rerun-tasks
```

Observed failure:

```text
nodes[3].jedis.get(key) == 'before-failover'
actual: null
key: velo-manual:slot-4096:7847
```

### Source Analysis

`ClusterxCommand.failover()` promotes the local replica metadata first:

- `dyn/src/io/velo/command/ClusterxCommand.groovy:291-299` flips the local node to master and other shard nodes to slaves.
- `dyn/src/io/velo/command/ClusterxCommand.groovy:314` calls `LeaderSelector.resetAsMaster(force, ...)`.

`LeaderSelector.resetAsMaster()` then promotes every initialized local slot:

- `src/main/java/io/velo/repl/LeaderSelector.java:403-407` checks the slave pair caught-up state when not forced.
- `src/main/java/io/velo/repl/LeaderSelector.java:413-418` skips that guard when `force=true`.
- `src/main/java/io/velo/repl/LeaderSelector.java:422-425` removes the slave pair and calls `oneSlot.resetAsMaster()`.

Current hypothesis:

`CLUSTERX FAILOVER FORCE` can promote a replica before it has applied the latest write. Even with a 2-second wait in the harness, the promoted node still returns null, suggesting either:

- replication had not actually applied the key, and the test needs a stronger catch-up marker before failover; or
- replication applied the key but `resetAsMaster()`/metadata reset makes it inaccessible after promotion.

The server logs also showed `Cannot invoke "io.velo.repl.ReplPair.getSlaveUuid()" because "replPair" is null` during topology switch in some failover runs, so the repl-pair removal path should be inspected for races.

## Additional Runtime Warning: Dynamic ManageCommand Compile Errors

Every e2e server run logs static type-check errors when loading `dyn/src/io/velo/command/ManageCommand.groovy`, for example:

```text
Cannot find matching method io.velo.persist.LocalPersist#initializedOneSlots()
Cannot find matching method java.lang.Object#resetAsSlave(java.lang.String, int)
Cannot find matching method java.lang.Object#asyncCall(groovy.lang.Closure)
```

The split command files still load and `MANAGE SLOT ... MIGRATE_FROM` returns `OK`, but this is noisy and may hide real dynamic-command reload failures.

## Recommended Next Debug Steps

1. Add temporary logging around `ManageCommand.migrateFrom()` after initial sync:
   log target `innerSlot`, `importMigratingSlot`, `onlyOneReplPairAsSlave`, and whether the test key exists before `SETSLOT`.
2. Add temporary logging around `ClusterxCommand.setslot()` on the target:
   log before/after `removeReplPairAsSlave()`, before/after `resetAsMaster()`, and key existence.
3. For failover, wait on an explicit replicated marker rather than sleeping:
   write a marker on master and poll promoted candidate before failover if readonly read is allowed, or inspect repl pair caught-up state via command/log.
4. Fix or suppress the dynamic `ManageCommand.groovy` static compile errors so e2e logs are actionable.
5. Once root cause is confirmed, convert the opt-in manual Groovy probe into a normal regression test or keep a smaller unit-level regression in `ClusterxCommandTest` / `LeaderSelectorTest`.

## Fix Review: 2026-07-04

### Confirmed Root Causes

1. `CLUSTERX SETNODES` did not initialize the local inner slots assigned by the new cluster view before calling
   `LeaderSelector.resetAsMaster()` / `resetAsSlave()`.
   With lazy `OneSlot` initialization, a node could advertise ownership for client slot range `4096-8191` while
   local inner slot `1` was still `null`. The first write then failed with:

   ```text
   Cannot invoke "io.velo.persist.OneSlot.isReadonly()" because "oneSlot" is null
   ```

2. Lazy slot creation reused ActiveJ's protected startup `Config` after application startup.
   Creating inner slot `1` from `SETNODES` then failed with:

   ```text
   Config must be used during application start-up time only
   ```

3. Full replication replay grouped fetched keys with `BaseCommand.calcSlotByKeyHash(...)`.
   In cluster mode, the target inner slot must be derived from Redis client slot routing, not raw Velo key hash.
   Otherwise migrated or replicated keys can be replayed into the wrong local slot.

### Fixes

- `ClusterxCommand.setnodes()` now initializes the inner slots covered by the local shard's assigned client-slot
  ranges before role reset and metadata refresh.
- `LocalPersist` caches `binlogOn` during `initSlots(...)`, and lazy slot creation passes that resolved boolean to
  `OneSlot` without rereading protected `Config`.
- `XGroup` now routes replayed key payloads through `targetSlotForReplKey(...)`; in cluster mode this uses
  `BaseCommand.slot(keyBytes, slotNumber)` so replay follows Redis client-slot mapping.
- The e2e harness and opt-in Groovy probe now wait until the replica exposes the failover marker before issuing
  `CLUSTERX FAILOVER FORCE`. A fixed 2-second sleep was racy because full sync can still be in the `cannot read`
  phase.

### Regression Tests

```bash
./gradlew :test \
  --tests "io.velo.command.ClusterxCommandTest.test setnodes initializes local assigned inner slots" \
  --tests "io.velo.command.XGroupTest.test replication target slot uses redis cluster slot when cluster enabled" \
  --rerun-tasks
```

Result:

```text
BUILD SUCCESSFUL
```

Coverage checks:

- `io.velo.command.ClusterxCommand:1131-1144` covers the local shard slot initialization helper.
- `io.velo.persist.LocalPersist:142-152` covers lazy `OneSlot` creation through the cached `binlogOn` path.
- `io.velo.persist.OneSlot:168-195` covers the boolean-backed production constructor.
- `io.velo.command.XGroup:935-936` covers the cluster-mode replay slot calculation branch.

### E2E Replay

Build and run:

```bash
./gradlew jar
KEEP_DATA=true scripts/e2e/velo_cluster/e2e_velo_cluster.sh
KEEP_DATA=true RUN_MIGRATION=false scripts/e2e/velo_cluster/e2e_velo_cluster.sh
VELO_CLUSTER_MANUAL=true ./gradlew :test \
  --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" \
  --rerun-tasks
```

Final verified runs:

```text
E2E workspace: /tmp/velo-cluster-e2e.5eb6Kx
All Velo cluster e2e checks passed

E2E workspace: /tmp/velo-cluster-e2e.ZeYeTp
All Velo cluster e2e checks passed

E2E workspace: /tmp/velo-cluster-e2e.a3e0py
All Velo cluster e2e checks passed

ClusterMigrationFailoverManualTest
BUILD SUCCESSFUL
```

The successful run covered:

- four-node cluster creation with three shards and one replica;
- wrong-owner write returning MOVED;
- slot `0` migration from node-0 to node-1 with `GET` on node-1 returning `before-migrate`;
- forced failover of node-3 for the migrated shard with `GET` returning `before-failover`;
- post-failover write on the promoted node.

## Fix Review: 2026-07-06

### Bug 3: Target Shard Replica Does Not Catch Up Newly Migrated Slot

Severity: High

Question checked:

```text
When slot migration makes one inner slot master on the target shard, can that shard's slave catch up records?
```

Observed before the fix:

```text
CLUSTERX SETSLOT 0 NODE velo-e2e-1 3
GET <slot-0-key> on target master node-1 -> before-migrate
GET <slot-0-key> on target replica node-3 -> ERROR cannot read until timeout
```

The Python e2e now explicitly waits for the migrated key on the target shard replica after `SETSLOT`:

```python
self.wait_for_value(self.nodes[3], slot0_key, "before-migrate")
```

### Root Cause

Two replication lifecycle issues combined during migration finalization:

- `ClusterxCommand.setslot()` only promoted the target master. A local node in the same target shard but not the
  target master could keep an old or partial slave state for that inner slot.
- `OneSlot.createIfNotExistReplPairAsMaster()` treated a byed master-side `ReplPair` as reusable because equality
  only checks slot, role, host, and port. Normal lookup skips byed pairs, so the next fetch request had no active
  pair and failed with:

  ```text
  Cannot invoke "io.velo.repl.ReplPair.getSlaveUuid()" because "replPair" is null
  ```

### Fixes

- `ClusterxCommand.setslot()` now handles local nodes that are replicas in the target shard:
  it removes any old slave pair for the moved inner slot and calls `resetAsSlave(targetMaster.host, targetMaster.port)`.
  That forces a fresh full fetch from the new target master after ownership is finalized.
- `OneSlot.createIfNotExistReplPairAsMaster()` now ignores pairs that already sent BYE when checking for an existing
  master-side pair, so a reconnect creates a new active replication pair.

### Replay

Migration-only:

```bash
./gradlew jar
KEEP_DATA=true RUN_FAILOVER=false scripts/e2e/velo_cluster/e2e_velo_cluster.sh
```

Full migration plus failover:

```bash
KEEP_DATA=true scripts/e2e/velo_cluster/e2e_velo_cluster.sh
```

Focused unit and coverage checks:

```bash
./gradlew :test \
  --tests "io.velo.command.ClusterxCommandTest" \
  --tests "io.velo.persist.OneSlotTest.test repl pair" \
  --rerun-tasks

python3 scripts/jacoco_cover.py io.velo.persist.OneSlot 590 610 --src
python3 scripts/jacoco_cover.py io.velo.command.ClusterxCommand 1238 1247 --src
```

Verified runs:

```text
E2E workspace: /tmp/velo-cluster-e2e.6XQXS3
All Velo cluster e2e checks passed

E2E workspace: /tmp/velo-cluster-e2e.jcOnaA
All Velo cluster e2e checks passed

OneSlot 590-610: 14/15 covered, byed-pair branch covered.
ClusterxCommand 1238-1247: target-shard replica branch covered.
```

## Fix Review: 2026-07-06, Groovy Spock Harness

### Refactor

`ClusterMigrationFailoverManualTest` now extends `ClusterxBase`, which owns the reusable process-cluster helpers:
starting four local Velo nodes, bootstrapping `CLUSTERX SETNODES`, broadcasting `SETSLOT`, polling command success,
finding keys for a Redis cluster slot, and waiting for replicated values.

Replay:

```bash
./gradlew jar
VELO_CLUSTER_MANUAL=true ./gradlew :test \
  --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" \
  --rerun-tasks
```

### Bug 4: Source Flush Can Propagate To Migrated Target

Severity: High

Observed before the fix in the Groovy process test:

```text
SET <slot-0-key> before-migrate on source node-0
MANAGE SLOT 0 MIGRATE_FROM ... on target node-1
CLUSTERX MIGRATE 0 <node-1-id> on source node-0
broadcast CLUSTERX SETSLOT 0 NODE <node-1-id>
GET <slot-0-key> on target node-1 -> null
```

Root cause:

During `CLUSTERX SETSLOT` on a local node that no longer belongs to the target shard, `ClusterxCommand.setslot()`
flushed the local source slot while old slave-side or master-side replication pairs could still be active. If the
source still had a master-side pair to the migration target, that local flush could be sent as an `XFlush` and wipe
the target after data had already migrated.

Fix:

The non-target branch in `ClusterxCommand.setslot()` now closes both slave-side and master-side repl pairs before
calling `oneSlot.flush()`. That makes the old source discard its local copy without sending a final flush to the
new owner.

Focused regression:

```bash
./gradlew :test --tests "io.velo.command.ClusterxCommandTest" --rerun-tasks
```

The `test slots` case now seeds both an old slave pair and an old master pair before moving slot `0` to another
shard, then asserts both pairs sent BYE and no active pair remains.

Coverage:

```bash
python3 scripts/jacoco_cover.py io.velo.command.ClusterxCommand 1241 1250 --src
```

Confirmed:

```text
ClusterxCommand 1248-1250 covered: remove slave pair, remove master pair, flush.
```

Verified run:

```text
./gradlew jar
VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" --rerun-tasks
BUILD SUCCESSFUL

KEEP_DATA=true scripts/e2e/velo_cluster/e2e_velo_cluster.sh
E2E workspace: /tmp/velo-cluster-e2e.Fu4qhG
All Velo cluster e2e checks passed
```
