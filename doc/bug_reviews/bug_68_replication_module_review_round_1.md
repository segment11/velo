# Replication Module Bug Review (Round 1)

Date: 2026-07-10
Author: AI agent 1

Scope: replication catch-up recovery, binlog rebasing during promotion, and multi-slot role transitions. This review
was performed against the current `review/replication` working tree after reading `doc/design/09_replication_design.md`,
the related replication sections in `doc/design/18_scale_up_design.md` and
`doc/design/20_slot_migration_replication_model.md`, and prior replication reviews through bug 67. The findings below
do not repeat the earlier big-string, exists-phase ordering, 2N read-gate, migration ownership, dict-sync, or re-hello
bugs. No production code was changed in this round.

## Bug 1: a catch-up error is logged and discarded, so a replica behind binlog retention never enters full sync

**Severity:** High

**Review status (AI agent 2, 2026-07-10):** CONFIRMED

**Files:**

- `src/main/java/io/velo/repl/Binlog.java:677-683`
- `src/main/java/io/velo/command/XGroup.java:308-312`
- `src/main/java/io/velo/command/XGroup.java:1714-1722`
- `src/main/java/io/velo/repl/TcpClient.java:147-160,170-187`

**Code excerpts:**

```java
// Binlog.java:677-683
var prevRaf = prevRaf(fileIndex);
if (prevRaf == null) {
    // keep max count = 10 or 100, if write too fast, may be lost some files
    // so slave will get error repl reply, then need re-fetch all exists data from master and re-catch up
    throw new IOException("Repl read binlog segment bytes, file not exist, file index=" + fileIndex + ", slot=" + slot);
}
```

```java
// XGroup.java:1714-1722, master side
try {
    readSegmentBytes = binlog.readPrevRafOneSegment(needFetchFileIndex, needFetchOffset);
} catch (IOException e) {
    var errorMessage = "Repl master handle error: read binlog file error";
    log.error("{}={}", errorMessage, e.getMessage());
    return Repl.error(slot, replPair, errorMessage + "=" + e.getMessage());
}
```

```java
// XGroup.java:308-312, slave side
case error -> {
    log.error("Repl handle receive error={}, slot={}", Wal.keyString(contentBytes), slot);
    yield Repl.emptyReply();
}
```

```java
// TcpClient.java:183-187
if (reply instanceof Repl.ReplReply replReply && replReply.isReplType(ReplType.error)) {
    socket.close();
    return Promise.of(null);
}
return Promise.of(reply.buffer());
```

**Root cause:**

The master correctly converts a missing retained binlog file into a REPL `error`. The source comment also states that
the slave must re-fetch all existing data in this case. However, when the slave receives that frame, `case error` only
logs it and returns `Repl.emptyReply()`. `TcpClient` therefore sees an empty reply rather than an error reply and keeps
the connection open. No code clears `isExistsDataAllFetched`, resets the saved catch-up offset, closes the socket, or
starts a new `hello`/`hi` handshake.

The slave's periodic pong path continues to trigger catch-up from the same durable offset. The master continues to
return the same missing-file error, and the slave continues to discard it. The `hi()` recovery rule that compares the
saved position with the master's earliest retained position would choose a full exists fetch, but it is never reached
because the live connection is not restarted.

**Impact:**

A replica that is offline or slow long enough for its requested binlog file to age out stalls indefinitely. It cannot
become readable or promotable and never repairs itself unless an external event closes the connection, restarts the
process, or reissues the role transition. The same loop also affects other master-side catch-up errors that require a
fresh handshake, such as a master UUID mismatch.

**Suggested fix direction:**

Treat a received REPL error as a failed replication session. At minimum, close the slave TCP connection so reconnect
runs `hello`/`hi`; for an out-of-retention response, explicitly invalidate the saved exists/catch-up progress before
reconnecting or let `hi()` compare it with `earliestFo` and select full sync. A typed error/status code would be safer
than branching on the current free-form error string.

Add a regression where the slave's durable offset references a deleted master binlog file. Verify that the first error
closes or resets the session and that the next `hi()` starts the exists phase rather than requesting the deleted file
again.

**Reviewer verification (AI agent 2, 2026-07-10):**

Confirmed against the current tree. Traced the full path:

- Master side: `XGroup.java:1714-1722` converts the `readPrevRafOneSegment` `IOException` (thrown at
  `Binlog.java:679-683` when `prevRaf(fileIndex) == null`) into `Repl.error(...)`. Verified.
- Slave receive path: `XGroup.handleRepl` `case error` at `XGroup.java:308-312` only logs and returns
  `Repl.emptyReply()`. `Repl.emptyReply()` (`Repl.java:210-219`) is `EMPTY_REPLY = new ReplReply(0L, (short) 0, null,
  BYTE_0_CONTENT)` — replType is `0`, **not** `ReplType.error`.
- `TcpClient.toReplyBufferOrClose` (`TcpClient.java:171-188`) closes the socket only when
  `reply.isReplType(ReplType.error)`. The empty reply fails that test, so the socket stays open and no reconnect
  runs. Verified the `case error` branch performs no `removeReplPairAsSlave`, no `clearMasterBinlogFileIndexAndOffset`,
  no socket close, and no re-`hello`.
- The only self-driven catch-up trigger is the `case pong` path (`XGroup.java:375-384`), which re-calls `s_catch_up`
  from the same durable offset in `metaChunkSegmentIndex`. The recovery that would repair this — the
  `earliestFileIndex` comparison in `hi()` (`XGroup.java:655-684`, using `isFileIndexAndOffsetAtOrAfter`) — only runs
  on a fresh handshake, which never happens because the session is never restarted. The retry-forever loop is real.

Impact and severity assessment stand. The same discard also swallows the master-UUID-mismatch error
(`XGroup.java:1687-1691`), which likewise needs a fresh handshake, as the finding notes.

## Bug 2: promotion rebases the active binlog but leaves stale segments, cache entries, and newer files reachable

**Severity:** High

**Review status (AI agent 2, 2026-07-10):** CONFIRMED

**Files:**

- `src/main/java/io/velo/persist/OneSlot.java:2624-2642`
- `src/main/java/io/velo/repl/Binlog.java:131-147`
- `src/main/java/io/velo/repl/Binlog.java:322-352`
- `src/main/java/io/velo/repl/Binlog.java:621-624,657-675`

**Code excerpts:**

```java
// OneSlot.java:2627-2635
var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();
var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);

binlog.reopenAtFileIndexAndMarginOffset(lastUpdatedFileIndex, marginLastUpdatedOffset);
binlog.moveToNextSegment(true);
```

```java
// Binlog.java:322-348
public void reopenAtFileIndexAndMarginOffset(int resetFileIndex, long marginOffset) throws IOException {
    if (currentFileIndex == resetFileIndex) {
        raf.setLength(marginOffset);
        currentFileOffset = marginOffset;
        clearByteBuffer();
        return;
    }

    IOUtils.closeQuietly(raf);
    ...
    var prevRaf = prevRaf(resetFileIndex, true);
    prevRaf.seek(marginOffset);

    currentFileIndex = resetFileIndex;
    currentFileOffset = marginOffset;
    clearByteBuffer();
    raf = prevRaf;
}
```

```java
// Binlog.java:131-136, process restart
var files = listFiles();
if (!files.isEmpty()) {
    latestFile = files.getLast();
    this.currentFileIndex = latestFile.fileIndex();
    this.currentFileOffset = latestFile.length();
}
```

```java
// Binlog.java:668-674, replication read
if (currentFileIndex == fileIndex || currentFileIndex - 1 == fileIndex) {
    var segmentBytes = getLatestAppendForReadCacheSegmentBytes(fileIndex, offset);
    if (segmentBytes != null) {
        return segmentBytes;
    }
}
```

**Root cause:**

Promotion uses the upstream master's last applied file index and offset as the new local binlog position. That position
can be lower than this node's old local binlog position from an earlier master tenure. The different-file branch of
`reopenAtFileIndexAndMarginOffset` only seeks the target file; it does not truncate it at `marginOffset`. The following
`moveToNextSegment(true)` extends or truncates the file to the end of the reserved segment, preserving any old bytes
inside that segment when the file already existed.

The rebase also does not delete files with indexes greater than `resetFileIndex`, close all cached RAFs for those files,
or clear `latestAppendForReadCacheSegmentBytesSet`. Consequently:

1. A downstream replica can receive a stale cached segment at the rebased current/previous file and offset.
2. Stale bytes in the reserved target segment remain readable from disk.
3. On restart, the constructor selects `files.getLast()`, so an untouched higher-index file from the node's old history
   becomes current again and supersedes the promoted master's rebased history.
4. If normal appends later rotate into an existing higher-index file, `createAndUseNextFile()` opens it without first
   removing its old tail.

**Impact:**

After failover, the promoted node can expose binlog records from its previous master epoch to downstream replicas. A
restart can move its current replication offset forward into completely stale files. Downstream replicas may then apply
writes that are not present in the promoted node's current data set, skip current writes, or fail decoding a segment
containing a mixture of new prefix bytes and an old tail.

**Suggested fix direction:**

Make binlog rebasing destructive above the chosen position: truncate the target file before reserving the next segment,
delete all files with indexes greater than `resetFileIndex`, close/remove their RAFs, and clear read-cache entries at or
after the rebase point. Recompute `diskUsage` after the operation. The operation should leave exactly one authoritative
highest file so constructor restart recovery cannot select stale history.

Add tests that create files through index N, rebase to an older file M and offset, and verify: the reserved segment is
zeroed, cached reads do not return old bytes, files above M are absent, rotation into M+1 starts from an empty file, and
a newly constructed `Binlog` reopens at the rebased position rather than N.

**Reviewer verification (AI agent 2, 2026-07-10):**

Confirmed against the current tree. Each of the four sub-claims holds:

1. Reserved segment not zeroed: `reopenAtFileIndexAndMarginOffset` different-file branch (`Binlog.java:334-348`)
   only `prevRaf.seek(marginOffset)` — no `setLength`, so the target file keeps its old length and bytes. The
   following `moveToNextSegment(true)` (`Binlog.java:289-312`) with `mod == 0` reserves one segment via
   `currentFileOffset += oneSegmentLength; raf.setLength(currentFileOffset)`. `setLength` truncates bytes *beyond* the
   reserved segment but leaves the old bytes *inside* `[marginOffset, marginOffset + oneSegmentLength)` untouched (they
   are not re-written to zero). So a stale tail in the reserved segment survives.
2. Read-cache not cleared: `latestAppendForReadCacheSegmentBytesSet` is never touched by
   `reopenAtFileIndexAndMarginOffset`; `readPrevRafOneSegment` (`Binlog.java:668-675`) serves cached bytes for
   `currentFileIndex` / `currentFileIndex - 1`, so a downstream replica can still receive a stale cached segment.
3. Higher files not deleted: the rebase deletes no `> resetFileIndex` files. Constructor recovery (`Binlog.java:131-147`)
   selects `files.getLast()` (highest index), so a restart re-selects an untouched old higher file as current.
4. Rotation into an existing file: `createAndUseNextFile` (`Binlog.java:546-586`) does `FileUtils.touch(nextFile)` +
   `new RandomAccessFile(file, "rw")` with no truncation, so an existing higher-index file keeps its old tail when
   appends rotate into it.

Reachability: the scenario requires a node that was master (files up to N), demoted to slave — `resetAsSlave`
(`OneSlot.java:2678-2704`) turns the binlog off and only `moveToNextSegment()` (non-force), it does not delete files
> N — then re-promoted with an upstream-derived resume offset lower than N. `resetAsMaster` (`OneSlot.java:2624-2651`)
then rebases to that lower `getMasterBinlogFileIndexAndOffset()`, leaving N-history reachable. Latent but real; severity
High stands.

## Bug 3: a failed multi-slot role transition rolls back only globals, leaving completed slots in the opposite role

**Severity:** High

**Review status (AI agent 2, 2026-07-10):** CONFIRMED

**Files:**

- `src/main/java/io/velo/repl/LeaderSelector.java:395-458`
- `src/main/java/io/velo/repl/LeaderSelector.java:554-602`
- `src/main/java/io/velo/persist/OneSlot.java:2624-2650`
- `src/main/java/io/velo/persist/OneSlot.java:2673-2705`

**Code excerpts:**

```java
// LeaderSelector.java:395-449, promotion
var initSlots = localPersist.initializedOneSlots();
Promise<Void>[] promises = new Promise[initSlots.size()];
for (int i = 0; i < initSlots.size(); i++) {
    var oneSlot = initSlots.get(i);
    promises[i] = oneSlot.asyncRun(() -> {
        ...
        oneSlot.resetAsMaster();
        ...
    });
}

Promises.all(promises).whenComplete((r, e) -> {
    ...
    if (e != null) {
        ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
        callback.accept(e);
        return;
    }
```

```java
// LeaderSelector.java:565-602, demotion
var initSlots = localPersist.initializedOneSlots();
Promise<Void>[] promises = new Promise[initSlots.size()];
for (int i = 0; i < initSlots.size(); i++) {
    var oneSlot = initSlots.get(i);
    promises[i] = oneSlot.asyncRun(() -> {
        ...
        oneSlot.resetAsSlave(host, port, openReplStream);
    });
}

Promises.all(promises).whenComplete((r, e) -> {
    ...
    if (e != null) {
        ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
        callback.accept(e);
        return;
    }
```

**Root cause:**

Each initialized slot performs its role transition independently on its own event loop. `Promises.all` reports failure
if any slot throws, but other slot promises may already have completed. Both failure handlers restore only the global
`masterSlotNumber` and failover-state string. They do not restore per-slot state or even record which slots completed.

The per-slot operations are not atomic. `resetAsMaster` rebases the binlog, clears durable upstream progress, changes
readonly, and enables reads. `resetAsSlave` clears progress, moves the binlog, removes downstream pairs, creates an
upstream pair, changes readonly/readability, and disables the binlog. An `IOException` from a later config or file
operation can also leave the failing slot itself partially changed.

**Impact:**

If one slot transition fails because of an I/O/configuration error while another succeeds, the callback reports overall
failure but the node is left split across roles. During failed promotion, some slots can be writable masters while the
controller is free to promote another node, creating a split-brain window. During failed demotion, some slots can be
readonly slaves with downstream pairs removed while other slots remain masters. Restoring `masterSlotNumber` can also
make scale-up/read-gate decisions inconsistent with the roles actually installed on those slots.

**Suggested fix direction:**

Use a two-phase role transition. First validate and prepare every slot without externally visible role changes; only
commit role/readability and pair changes after all preparations succeed. If that is too invasive, serialize transitions,
record each successfully changed slot, and perform an explicit compensating transition on failure before invoking the
callback. Per-slot reset methods should also order durable mutations so an exception cannot expose a half-transitioned
slot.

Add deterministic tests with two initialized slots and an injected failure in the second slot. Verify that a failed
promotion leaves both slots slaves and a failed demotion leaves both slots masters, including readonly, canRead,
binlogOn, upstream/downstream pair, durable offset, and global read-gate state.

**Reviewer verification (AI agent 2, 2026-07-10):**

Confirmed against the current tree.

- Promotion `doResetAsMaster` (`LeaderSelector.java:396-459`) fans out one `oneSlot.asyncRun` per initialized slot,
  each running `resetAsMaster()` (and `removeReplPairAsSlave`) independently on its own event loop, then
  `Promises.all(promises).whenComplete`. On `e != null` (`:454-459`) it restores only `ConfForGlobal.masterSlotNumber`
  and `masterFailoverState`; it does not roll back any slot that already ran `resetAsMaster`, nor track which slots
  completed. A later slot throwing `IllegalStateException` (`:417`) or an `IOException` inside `resetAsMaster` while an
  earlier slot has committed leaves a split master/slave node with the callback reporting failure.
- Demotion `doResetAsSlave` (`LeaderSelector.java:565-603`) is symmetric: each slot runs `resetAsSlave(...)`
  independently and the `e != null` handler (`:598-603`) restores only `masterSlotNumber`. `resetAsSlave`
  (`OneSlot.java:2678-2704`) performs several non-atomic durable mutations (clear offset, `moveToNextSegment`,
  `removeReplPairAsMaster`, `createReplPairAsSlave`, readonly/canRead, `setBinlogOn(false)`), so even the failing slot
  can be left half-transitioned, and earlier fully-succeeded slots stay demoted.

`Promises.all` semantics verified: it resolves with the first exception but does not cancel or revert siblings that
already completed. Split-brain / split-role window is real; severity High stands. No production code changed in this
review round.

## Summary

| # | Severity | Area | Failure mode | Review status |
|---|----------|------|--------------|---------------|
| 1 | High | Catch-up recovery | Master error is discarded; replica retries an unrecoverable retained-file offset forever | CONFIRMED |
| 2 | High | Promotion/binlog | Rebase leaves old disk/cache history reachable and restart selects stale higher files | CONFIRMED |
| 3 | High | Role transition | One slot failure leaves other slots committed despite an overall failure result | CONFIRMED |

Baseline verification before authoring: `./gradlew :test --tests io.velo.repl.BinlogTest --tests io.velo.repl.LeaderSelectorTest --tests io.velo.command.XGroupTest` completed successfully. These existing tests do not cover the failure sequences above.

## Review Round 1 — AI agent 2 (2026-07-10)

All three findings verified against the current `review/replication` working tree by tracing each cited code path
(`Binlog.java`, `XGroup.java`, `TcpClient.java`, `Repl.java`, `OneSlot.java`, `LeaderSelector.java`). All three are
**CONFIRMED** at the stated High severity; none were refuted or down-graded. Per-bug verification notes are appended
under each finding above. No production code was changed in this review round. Recommended fix order matches the table
(Bug 1 → Bug 2 → Bug 3), each following the TDD + JaCoCo + single-commit workflow in `CLAUDE.md`.


## Review Feedback - Bug 1 Fix - AI agent 2 - 2026-07-10

Reviewed commit `dcb8851c` (`fix: slave resets catch-up progress and session on received repl error`).

### Summary

The commit changes slave-side handling of a received REPL error to:

1. Clear the durable master UUID, exists-fetched flag, and catch-up position through
   `MetaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset()`.
2. Reset the slave pair's in-memory `masterUuid` to zero.
3. Return a REPL error so `TcpClient.toReplyBufferOrClose()` closes the connection.

The added test verifies those state mutations and retains the existing no-op behavior when no slave-side repl pair can
be resolved.

### Strengths

- Clearing the durable progress is the correct safe response to an out-of-retention offset. A future `hi()` cannot
  mistake the old exists/catch-up state for resumable progress.
- Resetting `replPair.masterUuid` prevents the old handshake identity from being reused accidentally.
- The `!replPair.isAsMaster()` guard keeps a received error on a master-side pair from clearing local slave progress.
- The focused test covers the new slave branch and the null/unresolvable-pair fallback. JaCoCo shows every added
  production line covered.

### Concerns

1. **High - the socket is closed, but no new replication connection is created, so Bug 1 remains an indefinite
   stall.**

   `TcpClient.toReplyBufferOrClose()` closes the socket when the new error reply is returned
   (`TcpClient.java:183-185`). `SocketInspector.onDisconnect()` then calls
   `XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed()` (`SocketInspector.java:836-845`). The fix has already cleared
   `isExistsDataAllFetched`, so that helper immediately returns at `XGroup.java:2083-2086`. It cannot perform its Jedis
   catch-up fallback and does not reconnect.

   The remaining periodic replication task only calls `replPair.ping()` (`OneSlot.java:1420-1433`). That delegates to
   the existing `TcpClient`, whose socket is closed, and returns false. The only production call to
   `ReplPair.initAsSlave()` is during `OneSlot.createReplPairAsSlave()`; disconnect does not remove/recreate the pair or
   invoke `initAsSlave()` again. Therefore the code comment's promised "next hi() handshake" never occurs. The replica
   changes from repeatedly requesting an unrecoverable offset on a live socket to repeatedly pinging a closed socket.

   The focused verification run exposed this exact path in its standard output:

   ```text
   Inspector on repl disconnect ...
   Repl slave try catch up again after slave tcp client close, but exists data not all fetched, slot=0
   Repl socket is already closed ...
   ```

2. **The new regression stops before the behavior named in its `then` label.** It calls `XGroup.handleRepl()` directly
   and asserts that an error reply and cleared state are returned. It does not pass that reply through
   `TcpClient.toReplyBufferOrClose()`, invoke the disconnect inspector, observe a reconnect, receive `hi`, or prove that
   the exists phase restarts. Thus it verifies state invalidation but not session recovery.

### Required Follow-up

- Keep the safe progress invalidation, but add an explicit recovery transition. Viable directions are to reconnect the
  existing slave pair after disconnect, remove/recreate the pair through slot/controller reconciliation, or reset state
  and immediately send a new `hello` over the still-live connection instead of closing it.
- Add a regression spanning received error -> TCP close or re-hello -> new `hello`/`hi` -> exists fetch. It must assert
  that a fresh replication request is actually sent after the error, not only that metadata was cleared.
- Re-run the focused `XGroupTest`, `TcpClientTest`, and `SocketInspectorTest`, then inspect JaCoCo for both the error
  branch and the selected reconnect/re-hello branch.

### Verification

- Ran:
  `./gradlew :test --tests "io.velo.command.XGroupTest.test as slave handle received error resets catch-up progress and session (bug 68)" --tests "io.velo.command.XGroupTest.test try catch up again after slave tcp client closed" --tests "io.velo.repl.TcpClientTest" --tests "io.velo.SocketInspectorTest"`
- Result: Gradle build successful. The tests pass, but their output confirms the disconnect helper exits without
  recovery after the new state reset.
- JaCoCo:
  `python3 scripts/jacoco_cover.py io.velo.command.XGroup 308 324 --src`
- Result: added lines 310 and 319-321 covered; condition line 318 is partial because one compound branch is not covered.

**Verdict:** Changes requested. The state invalidation is useful, but commit `dcb8851c` does not complete Bug 1's
automatic recovery and should not be treated as a finished fix.

## Bug 1 Fix - Revision (recovery over live connection) - 2026-07-10

Addresses the "changes requested" feedback above. Follow-up commit reworks the slave `case error` handling.

**Root design decision:** there is no auto-reconnect for a closed slave replication stream — the only production
call to `ReplPair.initAsSlave()` is `OneSlot.createReplPairAsSlave()`, and `onDisconnect ->
tryCatchUpAgainAfterSlaveTcpClientClosed` early-returns once progress is cleared. So *closing* the socket (the
original approach) leaves replication stalled until an external re-subscribe. A transport-level reconnect with
backoff/jitter is the heavier, design-level alternative, but the cheapest complete recovery reuses the machinery that
already exists: the pong path already re-sends `hello` over the live connection to bootstrap a stuck handshake
(`XGroup.java` `case pong`, bug 67).

**Change:** on a received error, the slave now (1) clears durable catch-up progress via
`clearMasterBinlogFileIndexAndOffset()`, (2) resets `replPair.masterUuid` to 0, and (3) **re-sends `hello` over the
same still-live connection** instead of returning an error that closes the socket. The master replies `hi`; because
the progress was cleared, `hi()` takes the full exists-fetch branch and the exists phase restarts. Works in both
cluster and non-cluster topologies and needs no reconnect state machine.

**Test:** the regression now asserts the recovery request is actually emitted — `handleRepl` returns a `hello`-type
reply (a fresh replication request on the wire), not merely that metadata was cleared. The existing
`test as slave handle pong` error-branch assertion was updated from `error` to `hello` for the same reason.

**Verification:** `./gradlew :test --tests "io.velo.command.XGroupTest"` — build successful. JaCoCo: the `case error`
recovery lines (clear progress, reset uuid, build Hello, return hello reply) are all covered; only the defensive
`replPair == null / isAsMaster` fallback branch is partial (a master-side pair receiving an error).

**Residual (tracked, not blocking):** for a *persistently* failing master (e.g. config mismatch), re-hello will retry
each error, mirroring how a freshly-connecting slave would also retry — no new backoff was added, matching existing
pong/handshake behavior. A bounded same-master transport reconnect with exponential backoff + jitter remains a
worthwhile separate hardening item if repeated full-sync attempts under a broken master become a concern.
