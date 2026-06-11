# Bug 45: HyperLogLog Commands Review Round 1

Author: AI agent 1
Date: 2026-06-02

## Scope

Redis docs fetched from `https://redis.io/docs/latest/commands//?group=hyperloglog` and related command pages list
the HyperLogLog category commands as:

- `PFADD`
- `PFCOUNT`
- `PFDEBUG`
- `PFMERGE`
- `PFSELFTEST`

Velo's support table marks `PFDEBUG` and `PFSELFTEST` unsupported, so this review focused on the commands currently
marked supported in `doc/redis_command_support.md`: `PFADD`, `PFCOUNT`, and `PFMERGE`.

Reference docs:

- `https://redis.io/docs/latest/develop/data-types/probabilistic/hyperloglogs/`
- `https://redis.io/docs/latest/commands/pfadd/`
- `https://redis.io/docs/latest/commands/pfcount/`
- `https://redis.io/docs/latest/commands/pfmerge/`

## Bug 1: `PFADD key` is rejected even though Redis allows no element arguments

Status: **Partially Fixed (see notes)**

Severity: Medium

Files:

- `src/main/java/io/velo/command/PGroup.java:70-75`
- `src/main/java/io/velo/command/PGroup.java:264-292`

Code excerpt:

```java
if ("pfadd".equals(cmd)) {
    if (data.length < 3) {
        return slotWithKeyHashList;
    }
    slotWithKeyHashList.add(slot(data[1], slotNumber));
    return slotWithKeyHashList;
}
...
private Reply pfadd() {
    if (data.length < 3) {
        return ErrorReply.FORMAT;
    }

    var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
    ...
}
```

Root cause:

Redis syntax is `PFADD key [element [element ...]]`, and the docs explicitly say calling the command with only the key
is valid: it creates the HLL when the key is missing and returns `1`, or performs no operation and returns `0` when the
key already exists. Velo requires at least one element in both slot parsing and command handling, so `PFADD hll` returns
a wrong-number/format error before the Redis-defined key-only behavior can run.

Impact:

- Valid Redis clients can fail when they use `PFADD key` to initialize an empty HyperLogLog.
- The server does not create the empty HLL for a missing key, so later `TYPE`, persistence, or `PFCOUNT` behavior can
  diverge from Redis.
- The slot parser also omits the key for this valid arity, so a production fix in `pfadd()` must update slot parsing at
  the same time.

Suggested fix:

Allow `data.length >= 2` in `parseSlots()` and `pfadd()`. For the key-only case, load the existing value: return `0`
if it is already an HLL, return `WRONGTYPE` for a non-HLL value, and save an empty HLL with `NO_EXPIRE` then return `1`
when the key is missing. Add focused cases to `PGroupTest.'test pfadd'` for missing key, existing HLL, and wrong-type
key.

## Bug 2: `PFMERGE destkey` is rejected even though Redis syntax makes sources optional

Status: **Fixed** (commit `ec539be5`)

Severity: Medium

Files:

- `src/main/java/io/velo/command/PGroup.java:86-91`
- `src/main/java/io/velo/command/PGroup.java:370-450`

Code excerpt:

```java
if ("pfmerge".equals(cmd)) {
    if (data.length < 2) {  // Changed from < 3 to < 2
        return slotWithKeyHashList;
    }
    addToSlotWithKeyHashList(slotWithKeyHashList, data, slotNumber, BaseCommand.KeyIndexBegin1);
    return slotWithKeyHashList;
}
...
private Reply pfmerge() {
    if (data.length < 2) {  // Changed from < 3 to < 2
        return ErrorReply.FORMAT;
    }

    var dstS = slotWithKeyHashListParsed.getFirst();
    var dstHll = getHll(dstS);
    if (dstHll == null) {  // New: load existing or create empty
        dstHll = emptyHll();
    }
    // ... merge sources into dstHll ...
    ...
}
```

Root cause:

Redis documents `PFMERGE destkey [sourcekey [sourcekey ...]]` with arity `-2`, so the destination key alone is a valid
minimum arity. Velo treats fewer than three RESP items as a format error and does not parse the destination key for that
arity.

Impact:

- Valid Redis command `PFMERGE dst` fails in Velo.
- When `dst` is missing, Redis creates the destination as an empty HyperLogLog and returns `OK`; Velo returns a
  wrong-number/format error instead.
- When `dst` already exists, Redis treats it as one of the source sets, so `PFMERGE dst` should preserve its cardinality
  and return `OK`; Velo rejects the request before checking the destination type or preserving the value.

Suggested fix:

Allow `data.length >= 2` for `PFMERGE`. Parse the destination key even when no source keys are supplied. In the no-source
case, preserve an existing destination HLL, create an empty HLL for a missing destination, and return `WRONGTYPE` if the
destination exists with a non-HLL type. Add focused tests to `PGroupTest.'test pfcount and pfmerge'` for missing,
existing-HLL, and wrong-type destination cases.

## Notes

I inspected the `com.github.prasanthj:hll` bytecode because Velo computes the `PFADD` reply by comparing
`hll.count()` before and after `addBytes()`. The library internally tracks whether a register changed, but `addBytes()`
returns `void`, so Velo cannot directly use that flag. I did not file this as a confirmed bug in this round because I
did not produce a concrete counterexample where a register changes while the rounded estimate is unchanged.

---

## Reviewer Notes (AI agent 2)

Date: 2026-06-02

### Bug 1: PARTIALLY FIXED (need verification)

Verified against Redis official docs at `https://redis.io/docs/latest/commands/pfadd/`:

- PFADD arity is `-2`, meaning minimum 2 args (command name + key). Elements are optional.
- Docs explicitly state: "To call the command without elements but just the variable name is valid, this will result
  into no operation performed if the variable already exists, or just the creation of the data structure if the key
  does not exist (in the latter case 1 is returned)."

Verified against Velo code:

- `PGroup.java:71` — parseSlots uses `data.length < 2` (NOT `< 3` as doc described), so `PFADD key` (data.length == 2) correctly returns 1 slot.
- `PGroup.java:265` — `pfadd()` uses `data.length < 2` (NOT `< 3` as doc described).
- Test at `PGroupTest.groovy:46` confirms parseSlots works: `_Group.parseSlots('pfadd', data2, slotNumber).size() == 1`.

**However**, the test at lines 256-265 (`PFADD key-only on missing key creates HLL and returns 1`, `PFADD key-only on existing HLL returns 0`) FAILS with "wrong type" error when trying to call `PFADD` on an already-created HLL. This suggests a separate bug in the pfadd() handler's logic for handling existing HLL values with key-only calls. The test at line 263 throws `RuntimeException: wrong type`.

**Conclusion**: The arity check bug described in the doc has been fixed, but there appears to be a different bug in the handler when an HLL already exists and PFADD is called with key-only.

### Bug 2: CONFIRMED (still present)

Verified against Redis official docs at `https://redis.io/docs/latest/commands/pfmerge/`:

- PFMERGE arity is `-2`, meaning minimum 2 args (command name + destkey). Source keys are optional.
- Docs state: "The computed merged HyperLogLog is set to the destination variable, which is created if does not
  exist (defaulting to an empty HyperLogLog)."
- Docs also state: "If the destination variable exists, it is treated as one of the source sets and its cardinality
  will be included in the cardinality of the computed HyperLogLog." So `PFMERGE dst` when dst exists as an HLL should
  preserve it and return `OK`.

Verified against Velo code:

- `PGroup.java:87` — parseSlots rejects `data.length < 3`, so `PFMERGE destkey` (data.length == 2) returns empty slot list.
- `PGroup.java:371` — `pfmerge()` rejects `data.length < 3`, returning `ErrorReply.FORMAT`.
- The existing test in `PGroupTest.groovy` line 48 confirms: `_Group.parseSlots('pfmerge', data2, slotNumber).size() == 0`.
- Additionally, in the sync path (`PGroup.java:378`), the code always starts with `emptyHll()` and merges from
  index 2 onwards. When `data.length == 2`, the for loop `for (var i = 2; i < data.length; i++)` would correctly
  iterate zero times, but the code never reaches that point due to the earlier guard. However, note that the current
  sync path always creates a fresh empty HLL and does NOT read the existing destination value first, which means even
  after relaxing the arity check, the sync path would overwrite an existing destination HLL with an empty one when no
  sources are provided — this diverges from Redis behavior (Redis preserves the existing value). The fix should also
  address this by loading the existing destination HLL when no sources are specified, or always reading the destination
  as a source when it exists.

**Status**: Bug 2 is CONFIRMED present in the codebase. The arity check at lines 87 and 371 both use `data.length < 3`
and need to be changed to `data.length < 2`.

The root cause, impact, and suggested fix are correct. I add a refinement: the suggested fix for Bug 2 should also
ensure that in the no-source case, an existing destination HLL is preserved rather than overwritten with an empty one.
The sync code path at `PGroup.java:378` starts with `var dstHll = emptyHll()` unconditionally, which would need to
check if the destination already exists and load it instead.

### Notes Section Review

The note about `addBytes()` returning `void` and the comparison-based approach is accurate. The `pfadd()` method at
`PGroup.java:276-282` computes `oldCount = hll.count()`, adds elements, then computes `newCount = hll.count()`, and
compares. This is a valid approach though it may not detect single-register changes that don't affect the rounded
estimate. Not filing this as a bug is reasonable without a concrete counterexample.

---

## Review Feedback for Commit `869c66d6`

Reviewer: AI agent 1
Date: 2026-06-02

Commit reviewed: `869c66d6 fix: support PFADD with no elements (key-only)`

### Summary

The original Bug 1 arity defect is fixed in production code: `parseSlots("pfadd", ...)` and `pfadd()` now accept `data.length == 2`, and the key-only missing-key path saves an empty HLL and returns `1`.

### Strengths

- `PGroup.java:70-75` now parses the key for `PFADD key`, so routing and cluster-slot checks can see the valid key-only command.
- `PGroup.java:271-287` now accepts `PFADD key` and creates an empty HLL for a missing key, matching Redis documented behavior.
- The current working tree includes focused tests for key-only parse slots, missing key, existing HLL, and wrong-type key in `PGroupTest.groovy`. The focused test run passed with those tests present.

### Concerns

1. **Medium - commit introduces silent HLL decode failure handling**

   `PGroup.java:241-259` catches decompression and deserialization failures, logs a warning, and returns `null`. Callers treat `null` as a missing key. This changes corrupted or incompatible HLL data from an explicit command failure into silent data loss or incorrect results:

   - `PFADD key` can overwrite an unreadable existing HLL with a new empty HLL and return `1`.
   - `PFADD key element` can discard the unreadable existing HLL and recreate it from only the new element(s).
   - `PFCOUNT key` can return `0` for an unreadable HLL.
   - `PFMERGE` can skip unreadable source HLLs.

   This behavior is outside the original Bug 1 arity fix and is not covered by the focused tests. Prefer preserving the previous fail-fast behavior, or return an explicit error, so storage corruption is not interpreted as key absence.

2. **Low - regression tests are not included in the reviewed commit**

   The commit only changes `src/main/java/io/velo/command/PGroup.java`. The current working tree has useful test updates in `src/test/groovy/io/velo/command/PGroupTest.groovy`, but they are unstaged/uncommitted relative to `869c66d6`. The fix commit should include the Bug 1 regression tests, especially because this workflow requires TDD evidence per bug fix.

3. **Low - new decode-error branches are uncovered**

   JaCoCo after the focused test run shows `PGroup.java:244-246` and `PGroup.java:257-259` are not covered. If the catch-and-treat-as-missing behavior is intentionally retained, it needs explicit tests documenting that behavior. If not retained, remove those branches as part of addressing concern 1.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.PGroupTest"` on 2026-06-02: build successful.
- Ran `python3 scripts/jacoco_cover.py io.velo.command.PGroup 70 287 --src`: key Bug 1 parse and `pfadd()` lines are covered; decode-error catch branches remain uncovered.

### Follow-ups Before Accepting

- Remove or revise the `getHll()` catch blocks so unreadable HLL data is not treated as missing.
- Include the focused `PGroupTest.groovy` regression cases in the fix commit.
- Re-run `./gradlew :test --tests "io.velo.command.PGroupTest"` and inspect JaCoCo for the final touched lines.

---

## Review Feedback for Addressed Commits `757947b7` and `76d25aa2`

Reviewer: AI agent 1
Date: 2026-06-02

Commits reviewed:

- `757947b7 fix: remove getHll silent failure handling per review feedback`
- `76d25aa2 test: add PFADD key-only regression tests`

### Summary

The prior review concerns for Bug 1 are addressed. Commit `757947b7` removes the catch-and-treat-as-missing behavior from `getHll()`, restoring fail-fast behavior for unreadable HLL data. Commit `76d25aa2` commits the PFADD key-only regression tests that were previously only present in the working tree.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.PGroupTest"` on 2026-06-02: build successful.
- Ran `python3 scripts/jacoco_cover.py io.velo.command.PGroup 70 280 --src`: the PFADD arity, key-only missing-key path, existing-HLL path, and wrong-type path are covered.
- Ran `python3 scripts/jacoco_cover.py io.velo.command.PGroup 230 252 --src`: `getHll()` no longer contains the silent decompression/deserialization failure branches from `869c66d6`; the remaining `IOException` rethrow branch is not covered, but it is restored fail-fast behavior and not part of the Bug 1 arity fix.

### Verdict

Bug 1 is accepted as fixed after the addressed commits. No remaining concerns for the PFADD key-only fix. Bug 2 (`PFMERGE destkey`) remains separate and still needs its own fix.

---

## Bug 2 Fix (commit `ec539be5`)

Date: 2026-06-02

### Changes

1. **parseSlots** (`PGroup.java:87`): Changed `data.length < 3` to `data.length < 2` to allow key-only PFMERGE

2. **pfmerge() handler** (`PGroup.java:371`): Changed `data.length < 3` to `data.length < 2`

3. **Sync path** (`PGroup.java:378`): When `data.length == 2` (no sources), now loads existing destination HLL if it exists, otherwise creates empty HLL

4. **Async path** (`PGroup.java:426`): Same fix for cross-request-worker mode

### Behavior

- `PFMERGE dst` on missing key: creates empty HLL, returns OK
- `PFMERGE dst` on existing HLL: preserves it, returns OK
- `PFMERGE dst` on wrong type: throws WRONGTYPE

### Tests Added

- `test pfmerge destkey-only`: Tests missing key, existing HLL, and wrong type cases
- Updated `test parse slot - single key`: Changed expectation from `pfmerge data2.size() == 0` to `== 1`

### Verification

- All 17 PGroupTest tests pass
- JaCoCo coverage shows full coverage of the sync path (lines 377-389)

---

## Review Feedback for Commit `ec539be5`

Reviewer: AI agent 1
Date: 2026-06-02

Commit reviewed: `ec539be5 fix: support PFMERGE with no source keys (destkey-only)`

### Summary

Bug 2 is fixed. `PFMERGE destkey` now parses the destination key, accepts the two-token command, creates an empty HLL when the destination is missing, preserves an existing destination HLL, and returns `WRONGTYPE` for a non-HLL destination. The implementation also correctly starts normal `PFMERGE dest src...` from the existing destination HLL instead of unconditionally replacing it with an empty HLL.

### Strengths

- `PGroup.java:86-91` now parses `PFMERGE destkey` as a valid one-key command.
- `PGroup.java:370-389` now accepts `data.length == 2` and preserves or creates the destination HLL in the synchronous path.
- `PGroup.java:429-440` applies the same destination-preservation behavior in the async merge completion path.
- `PGroupTest.groovy` adds regression coverage for missing destination, existing HLL destination, wrong-type destination, and slot parsing.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.PGroupTest" --rerun-tasks` on 2026-06-02: build successful, 13 tasks executed.
- Ran `python3 scripts/jacoco_cover.py io.velo.command.PGroup 86 91 --src`: all `PFMERGE` parse-slot lines and branches are covered.
- Ran `python3 scripts/jacoco_cover.py io.velo.command.PGroup 370 443 --src`: the sync `PFMERGE` arity, missing destination, existing destination, source merge, and save paths are covered.

### Remaining Test Gap

No blocking issue found. The async path branch where `dstHll == null` at `PGroup.java:430-431` is not covered. This is not the destkey-only bug, because a destkey-only command has only one slot and should not be cross-worker. It would be a useful follow-up regression for `PFMERGE missing_dst src1 src2` when sources require cross-worker handling.

### Verdict

Bug 2 is accepted as fixed.
