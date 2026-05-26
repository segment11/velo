# Bug 32 Persist Layer Data Flow Review Round 1

Author: AI agent 1

Review date: 2026-05-25

## Scope

Fresh review of the persist-layer data **write** flow, focusing on the WAL buffer management path
(Wal.java `put()` method, `rewriteOneGroup`, `putVToFile`). This round is a follow-up to prior
persist layer reviews (bugs 11–31) and specifically looks for bugs not covered in those rounds.

Prior rounds covered:
- Orphaned big-string files on persist failure (bugs 22, 31)
- PVM decode bounds validation (bug 30)
- Segment overflow / marker-buffer exhaustion (bug 31)
- WAL recovery after `recoverNeedPutV` removal (bug 31)
- `isStopping` static flag lifecycle (bug 37)

This round drills into:
- `Wal.put()` — the WAL buffer overflow path and the `rewriteOneGroup` compaction mechanism
- `Wal.putVToFile()` — the file-writing layer beneath `put()`

Design documents reviewed:
- `doc/design/02_persist_layer_design.md`

Code reviewed:
- `src/main/java/io/velo/persist/Wal.java` (full)
- `src/main/java/io/velo/persist/OneSlot.java` (put/get/remove/doPersist paths)
- `src/main/java/io/velo/persist/Chunk.java` (persist method)
- `src/main/java/io/velo/persist/BigStringFiles.java` (write/delete paths)

---

## Finding 1: `Wal.put()` uses stale local `offset` after `rewriteOneGroup` — data written at wrong WAL position

**Severity:** High

**Files:**
- `src/main/java/io/velo/persist/Wal.java:831-870` (`put` method)
- `src/main/java/io/velo/persist/Wal.java:751-772` (`rewriteOneGroup`)
- `src/main/java/io/velo/persist/Wal.java:732-744` (`putVToFile`)

**Code excerpt:**

```java
// Wal.java:831-870
public PutResult put(boolean isValueShort, @NotNull String key, @NotNull V v, long lastPersistTimeMs) {
    var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
    var offset = isValueShort ? writePositionShortValue : writePosition;  // (A) capture offset

    var encodeLength = v.encodeLength();
    if (offset + encodeLength > ONE_GROUP_BUFFER_SIZE) {                  // (B) overflow check
        boolean needPersist = true;
        var keyCount = isValueShort ? delayToKeyBucketShortValues.size() : delayToKeyBucketValues.size();
        if (keyCount < ... && isOnRewrite) {
            var newOffset = rewriteOneGroup(isValueShort);                // (C) rewrites file, updates writePosition*
            if (newOffset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
                return new PutResult(true, isValueShort, v, v, 0);
            }
            needPersist = false;                                          // (D) rewrite succeeded
        }
        if (needPersist) {
            return new PutResult(true, isValueShort, v, v, 0);
        }
    }

    // fall-through: offset is STALE after rewriteOneGroup
    if (!bulkLoad) {
        putVToFile(v, isValueShort, offset, targetGroupBeginOffset);     // (E) writes at old position!
    }
    if (isValueShort) {
        writePositionShortValue += encodeLength;                          // (F) increments from newOffset
    } else {
        writePosition += encodeLength;
    }
    ...
}
```

```java
// Wal.java:751-772
private int rewriteOneGroup(boolean isValueShort) {
    var writeBytes = writeToSavedBytes(isValueShort, true);
    int newOffset = writeBytes.length - 4;
    var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
    if (isValueShort) {
        walSharedFileShortValue.seek(targetGroupBeginOffset);            // seek to group start
        walSharedFileShortValue.write(writeBytes);
        writePositionShortValue = newOffset;                              // updates instance field
    } else {
        walSharedFile.seek(targetGroupBeginOffset);
        walSharedFile.write(writeBytes);
        writePosition = newOffset;                                        // updates instance field
    }
    return newOffset;                                                     // returned but never assigned to 'offset'
}
```

**Root cause and impact:**

`rewriteOneGroup()` at step (C) rewrites all current WAL entries compactly from the beginning of
the group buffer, and updates the instance field `writePosition` or `writePositionShortValue` to
the new compacted offset `newOffset`. It returns `newOffset`, but the **local variable `offset`**
captured at step (A) is never updated.

When the rewrite succeeds (step D sets `needPersist = false`), the code falls through to step (E)
where `putVToFile` writes using the **stale** `offset` — the old, pre-rewrite position. This
writes the new entry at the wrong location in the WAL file, potentially overwriting data just
compacted by `rewriteOneGroup`.

Meanwhile, step (F) increments `writePosition*` from the **new** value set by `rewriteOneGroup`,
so the position pointer believes the next write should be at `newOffset + encodeLength`.

**Concrete example:**

Before `put()`:
- `writePositionShortValue = 900`
- `v.encodeLength() = 200`
- `ONE_GROUP_BUFFER_SIZE = 65536`

Enter `put()`:
- `offset = 900` ← captured at (A)
- `900 + 200 = 1100 > ONE_GROUP_BUFFER_SIZE` → overflow check triggered (B)
- `rewriteOneGroup(true)` compacts all entries → `writePositionShortValue = 600`, returns `600` (C)
- `600 + 200 = 800 <= ONE_GROUP_BUFFER_SIZE` → rewrite succeeded (D)
- `putVToFile(v, true, 900, targetGroupBeginOffset)` → writes at position **900** (E)
- `writePositionShortValue += 200` → becomes `600 + 200 = 800` (F)

Result: data physically at position 900, but the write position pointer thinks the next entry
goes at 800. The gap between 600-800 contains data from `rewriteOneGroup`. The data from 800-900
is now a no-man's-land (the gap between old and new write positions). Subsequent writes will
overwrite from 800 onward, potentially corrupting the compacted data written by `rewriteOneGroup`.

**Reachability:**

`isOnRewrite` defaults to `true` (Wal.java:776) and is never set to `false` anywhere in the
codebase. The rewrite path is therefore reachable whenever:
1. A WAL buffer overflow occurs: `offset + encodeLength > ONE_GROUP_BUFFER_SIZE`
2. The current key count is below the hot-data threshold (`< trigger / 10`)

This is most likely during warm-up or low-write-rate periods when the WAL accumulates data
slowly and a single entry pushes it over the buffer boundary.

**Suggested fix direction:**

Add `offset = newOffset;` after line 850 (`needPersist = false`):

```java
if (keyCount < ... && isOnRewrite) {
    var newOffset = rewriteOneGroup(isValueShort);
    if (newOffset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
        return new PutResult(true, isValueShort, v, v, 0);
    }
    needPersist = false;
    offset = newOffset;  // ← fix: sync local variable with updated write position
}
```

Add a unit test that:
1. Fills a WAL group buffer to near capacity (offset > ONE_GROUP_BUFFER_SIZE - 200)
2. Triggers `put()` with an entry that causes overflow
3. Verifies the entry is written at the correct file position (matches `writePosition* - encodeLength`)
4. Verifies that rewriting and re-reading the WAL group recovers the correct entries

---

## Finding 2: `Wal.put()` trailer decision uses stale `offset` after rewrite, potentially omitting the 4-byte delimiter

**Severity:** Low (follow-on from Finding 1; the stale offset also affects the trailer decision in `putVToFile`)

**Files:**
- `src/main/java/io/velo/persist/Wal.java:736` (`putVToFile` trailer check)
- `src/main/java/io/velo/persist/Wal.java:831-870` (`put` method)

**Code excerpt:**

```java
// Wal.java:732-744
private void putVToFile(@NotNull V v, boolean isValueShort, int offset, int targetGroupBeginOffset) {
    var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
    try {
        raf.seek(targetGroupBeginOffset + offset);
        if (offset + v.encodeLength() < ONE_GROUP_BUFFER_SIZE - 4) {   // trailer decision
            raf.write(v.encode(true));    // with 4-byte int(0) trailer
        } else {
            raf.write(v.encode(false));   // without trailer
        }
    } catch (IOException e) {
        log.error("Write to file error", e);
        throw new RuntimeException("Write to file error=" + e.getMessage());
    }
}
```

**Root cause:**

`putVToFile` uses `offset` for two purposes: (1) the seek position, and (2) the trailer decision
(`offset + v.encodeLength() < ONE_GROUP_BUFFER_SIZE - 4`). When `offset` is stale due to Finding 1,
the trailer decision is also based on the wrong position.

If the stale `offset` is larger than `newOffset`, the trailer decision may incorrectly conclude
there is less room than there actually is, omitting the 4-byte delimiter. On subsequent WAL
recovery (`lazyReadFromFile`), the reader uses the delimiter to detect the end of each entry.
A missing delimiter may cause the reader to misparse entries or lose the last entry in the group.

**Impact:**

This is a secondary consequence of Finding 1. The trailer is parsed during WAL lazy-read recovery
(`readBytesToList` at line 460-491). If the trailer is missing where expected, the recovery may
skip or truncate the affected entry. Combined with the wrong write position from Finding 1,
recovery from the WAL file after a restart could return corrupted or incomplete data for the
affected WAL group.

**Suggested fix direction:**

Same fix as Finding 1 (update `offset` after `rewriteOneGroup`). This also corrects the trailer
decision.

---

## Finding 3: `OneSlot.put()` big-string test-prefix gate in production path

**Severity:** Low (noted for completeness; previously mentioned in bug_31 scope)

**Files:**
- `src/main/java/io/velo/persist/OneSlot.java:1452`

**Code excerpt:**

```java
if (isPersistLengthOverSegmentLength || key.contains("kerry-test-big-string-")) {
```

**Root cause:**

The admission check for the big-string storage path includes a hardcoded test-only key prefix
`"kerry-test-big-string-"`. This means any production key containing this substring in its name
is forced into the big-string storage path, regardless of whether its value actually exceeds
the segment length. This can cause unexpected behavior for users who happen to use this key name.

**Impact:**

- Keys matching the pattern are stored as big strings even if the value is small, wasting the
  big-string file overhead and bypassing normal chunk-segment persistence.
- This is a test scaffolding leak into production code.

**Suggested fix direction:**

Remove the `|| key.contains("kerry-test-big-string-")` clause. If this prefix is needed for
testing, convert it to a configuration flag or a `@TestOnly` method that sets a test-mode flag.

---

## Summary

| Finding | Description | Severity | Status |
|---------|-------------|----------|--------|
| 1 | `Wal.put()` stale local `offset` after `rewriteOneGroup` — data written at wrong WAL position | **High** | **Fixed** (commit `222048ee`) |
| 2 | Trailer decision in `putVToFile` uses same stale `offset` (follow-on from #1) | **Low** | **Fixed** (commit `222048ee`, same one-line fix) |
| 3 | Hardcoded test-only key prefix in production big-string admission check | **Low** | Not confirmed as bug (intentional) |

Finding 1 is the critical finding in this round. It is a reachable data integrity bug in the WAL
buffer compaction path. The stale offset causes entries to be written at incorrect file positions,
leading to corrupted WAL data on disk and incorrect recovery after restart.

Finding 2 is a downstream consequence of the same root cause.

Finding 3 is a code cleanliness issue noted for completeness; it was previously identified in
the scope of bug_31 but not yet fixed.

---

## AI Agent 2 Review Notes

Reviewer: AI agent 2

Review date: 2026-05-25

### Finding 1 Review: Confirmed — Fixed

Status: **Fixed in commit `222048ee`.**

Verified current code:
- `Wal.put()` captures `offset` from `writePosition` / `writePositionShortValue` before the overflow branch
  (`src/main/java/io/velo/persist/Wal.java:831-836`).
- When the overflow branch rewrites the WAL group, `rewriteOneGroup()` returns `newOffset` and updates the
  instance write-position field (`src/main/java/io/velo/persist/Wal.java:751-769`).
- The local `offset` in `put()` is not reassigned after the successful rewrite path
  (`src/main/java/io/velo/persist/Wal.java:840-851`).
- The later physical file write still uses that stale local offset
  (`src/main/java/io/velo/persist/Wal.java:862-864`), while the in-memory write-position field is incremented
  from the compacted value (`src/main/java/io/velo/persist/Wal.java:866-869`).

The root cause is valid. A successful rewrite necessarily means the compacted `newOffset` plus the new entry
fits in the group, while the old `offset` plus the new entry did not. In normal cases that makes the stale
`offset` larger than `newOffset`. The misplaced write therefore usually lands after the zero delimiter appended
by `rewriteOneGroup()` rather than directly overwriting compacted entries. This is still a data-integrity bug:
`readBytesToList()` stops when `V.decode()` sees the zero length at the compacted end marker
(`src/main/java/io/velo/persist/Wal.java:480-488` and `src/main/java/io/velo/persist/Wal.java:115-118`), so a
new entry written at the stale offset can become invisible to WAL reload/recovery. Subsequent writes use the
advanced in-memory write-position field and can continue writing after an already-placed end marker.

Fix direction is correct: after a successful rewrite, assign `offset = newOffset` before calling `putVToFile()`.
The regression test should assert both the returned `PutResult.offset` and the reload/readback behavior, because
the runtime maps can hide the file-position bug until WAL recovery.

### Finding 2 Review: Confirmed — Fixed

Status: **Fixed in commit `222048ee` (same one-line fix as Finding 1).**

`putVToFile()` uses the passed `offset` both for `seek()` and for choosing whether to append the 4-byte zero
delimiter (`src/main/java/io/velo/persist/Wal.java:732-740`). If `put()` passes the stale pre-rewrite offset,
both decisions are made against the wrong position.

The main recovery failure mode is slightly broader than "missing delimiter": after `rewriteOneGroup()` appends a
zero delimiter at `newOffset`, writing the next entry at the stale offset can leave that delimiter before the new
entry, so recovery stops before the misplaced entry. If the stale offset is also close enough to the group end to
choose `v.encode(false)`, the entry-local trailing delimiter can also be omitted incorrectly. The same one-line
fix from Finding 1 addresses both aspects.

### Finding 3 Review: Not Confirmed as a Data-Flow Bug

Status: **Confirmed behavior, not confirmed as a persist data-flow bug without maintainer policy input.**

The production branch still contains the hardcoded key substring gate at
`src/main/java/io/velo/persist/OneSlot.java:1452-1453`, so the described behavior is real: a key containing
`kerry-test-big-string-` is forced into the big-string file path even if its encoded length does not require it.
Tests also use that prefix to exercise big-string behavior.

However, the current source includes an inline comment stating that this prefix gate is deliberate and
intentionally kept in production. I therefore do not confirm it as a definite bug in this round. It remains a
low-severity product/design concern unless maintainers explicitly accept this key-name side effect as part of
the supported behavior.

### Reviewer Summary

Confirmed bugs to fix:
- Finding 1: **confirmed high-severity WAL recovery/data-integrity bug** — **Fixed** (commit `222048ee`).
- Finding 2: **confirmed low-severity follow-on from Finding 1** — **Fixed** (commit `222048ee`).

Not confirmed as a definite bug:
- Finding 3: behavior exists, but current code marks it intentional; classify as maintainer-decision/design debt.

---

## Review Feedback: Finding 1 Fix

Reviewer: AI agent 2

Review date: 2026-05-25

Reviewed revision: commit `222048ee` (`fix: update stale offset after rewriteOneGroup in Wal.put()`).

### Summary of the Fix

The production change assigns `offset = newOffset` immediately after a successful
`rewriteOneGroup()` in `Wal.put()`. This keeps the later `putVToFile()` seek position and trailer
decision aligned with the compacted write position.

The regression test fills the short-value WAL file position by repeatedly writing the same key,
then writes a second key that triggers `rewriteOneGroup()`. It reloads a fresh `Wal` from the same
file and asserts both keys are recovered, which exercises the failure mode where the stale write
would sit after the compacted zero delimiter and be skipped during reload.

### Findings

No code correctness issues found in the fix.

### Strengths

- The production change is minimal and targets the confirmed root cause.
- The new test validates the file/reload behavior rather than only the in-memory delay map, which
  is the right level for this bug.
- JaCoCo confirms the successful rewrite branch and new assignment were executed:
  `Wal.java.html` marks lines 841, 850, 851, and 865 as fully covered.

### Concerns

- I did not independently run the test against the pre-fix code to prove the red phase in this
  review pass. The test shape directly targets the confirmed stale-offset path, but strict TDD
  evidence should be preserved by the fixing agent if available.

### Verification

Command run:

```bash
./gradlew :test --tests "io.velo.persist.WalTest.put after rewriteOneGroup uses correct offset - stale offset causes data loss on reload" --rerun-tasks
```

Result: `BUILD SUCCESSFUL`; all 13 tasks executed, the focused `WalTest` method passed, and
`jacocoTestReport` ran. The test report lists the method as `success`, and JaCoCo marks
`Wal.java` lines 841, 850, 851, and 865 as fully covered.
