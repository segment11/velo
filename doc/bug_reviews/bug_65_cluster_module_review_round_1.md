# Bug 65 — Cluster module review (round 1)

## Bug 65: re-migrating a slot after a cancelled migration can lose data (stale export repl pair reused)

**Severity:** High (data loss). A migrate that immediately follows a cancelled migration of the same slot
to the same target can report `migrating_state:success` **before** the new data has synced, cut over, and
strand freshly-written keys on the old source (now redirecting).

**Discovered by:** `ClusterMigrationStressManualTest` T5, extended to re-migrate after abort. The
re-migrated key was written on the source *after* the abort, then lost: the new owner ended up owning the
slot but returning `null` for the key.

### Evidence (server logs, node-2 = source, slot 2)

```
14:27:11.653  Repl create repl pair as master, host=127.0.0.1, port=45647(node-0), slot=2   # first migrate
14:27:16.720  readonly=true                                                                  # first migrate caught up
14:27:16.725  Repl master handle bye: slave ...:45647, slot=2                                # abort -> node-0 byes
14:27:16.735  readonly=false                                                                 # clusterx migrate cancel (bug 64 fix)
14:27:16.756  readonly=true                                                                  # SECOND migrate "success" — 10ms after it started
14:27:16.772  Repl master handle bye: slave ...:45647, slot=2                                # second cutover
14:27:16.826  Repl remove repl pair as master, host=127.0.0.1, port=45647, slot=2            # pair finally removed — too late
```

The first migrate's `replPairAsMaster` (created 11.653) is only **bye'd** on abort (16.725) and is not
actually removed until 16.826. During the second migrate (16.746–16.772) that stale pair — already
`allCaughtUp` from the first migrate — is still present.

### Cited files / lines

- `dyn/src/io/velo/command/ClusterxCommand.groovy:561-576` — `info()` export success check finds a
  `replPairAsMaster` by `migratingToHost/migratingToPort` and reports `success` when it is `allCaughtUp`;
  it does not exclude a bye'd/stale pair, nor verify the pair belongs to the *current* migration.
- `dyn/src/io/velo/command/ClusterxCommand.groovy:645-668` — `cancel` clears the export marker and (now)
  resets readonly, but does **not** tear down the export `replPairAsMaster`.
- `src/main/java/io/velo/persist/OneSlot.java:473-501` — `removeReplPairAsSlave` byes + delays close.
- `dyn/src/io/velo/command/ManageCommand.groovy:666-673` — `migrate_from_abort` byes the target's pairs
  and flushes; the source's master pair is closed only on its own delayed-close cycle.

### Root cause

Cancel/abort leaves the source's export `replPairAsMaster` alive (bye'd, delayed-close) rather than
removing it promptly. A re-migrate to the same target reuses that stale pair, whose `allCaughtUp` flag is
still `true` from the previous run, so `info()` reports `migrating_state:success` almost immediately —
before the new import has fetched the newly-written data. The controller then cuts over ownership and the
fresh key is stranded on the old source.

### Impact

Any abort-then-retry of a migration (a natural operator/controller pattern) risks silently dropping writes
made between the abort and the retry. The window is small in wall-clock terms but deterministic when the
retry is prompt (the stale pair has not yet hit its delayed close).

### Candidate fixes

- **Tear down the export pair on cancel.** In the `cancel` branch, remove the specific export
  `replPairAsMaster` (matching `migratingToHost/migratingToPort`) so a re-migrate must create a fresh,
  not-yet-caught-up pair. Must avoid removing legitimate shard-replica master pairs — target by host/port,
  not `removeReplPairAsMaster()` (which drops all).
- **Harden the success check.** In `info()`, require the matched `replPairAsMaster` to be non-bye'd and to
  belong to the current migration session (e.g., match on a per-migration id / freshly reset
  `allCaughtUp`), so a stale caught-up pair cannot report success for a new migration.
- Ideally both (defense in depth): correct teardown *and* a success check that cannot trust a stale pair.

### Status

Documented, not yet fixed. Deeper than the bug-62/64 minimal fixes because it touches the repl-pair
delayed-close lifecycle and the migration success-detection contract. The T5 process test was trimmed to
its bug-64 core (abort → source writable) so it stays green; a dedicated re-migrate-after-abort test should
be added with the fix as its regression fence.

### Cross-reference

- Follows [bug 64](bug_64_cluster_module_review_round_1.md) (cancel cleanup); found while validating that fix.
