# Bug 47: KeyBucket Class Review — Round 1

**Author:** AI Agent 1
**Date:** 2026-06-11
**Module:** persist
**Class:** `io.velo.persist.KeyBucket` (`src/main/java/io/velo/persist/KeyBucket.java`, 746 lines)

---

## Overview

`KeyBucket` is a fixed-capacity (48-cell) hash-index bucket that stores key-value pairs in a byte buffer. Each cell holds 60 bytes of key+value data plus 24 bytes of meta (hash, expireAt, seq). Multi-cell entries use `PRE_KEY` (-1) markers in subsequent cell meta fields. The bucket header is 12 bytes: `lastUpdateSeq(8) + size(2) + cellCost(2)`.

The bucket is designed for a run-to-completion threading model (thread-local, no concurrent access). In production, `KeyBucket` is used exclusively through a **clear-all → batch-repopulate** pattern in `KeyBucketsInOneWalGroup.putPvmListToTargetBucket()`.

---

## ~~Bug 1~~ — RETRACTED: Non-atomic delete-then-put causing data loss on put failure

- **Severity:** ~~MEDIUM~~ → RETRACTED
- **Cited files:** `src/main/java/io/velo/persist/KeyBucket.java`, lines 406, 419–427
- **Original claim:** The `put()` method deletes an existing entry (line 406) **before** confirming contiguous cell space is available (line 419). When space is insufficient, `DoPutResult(false, false)` is returned — with the old entry already deleted.

- **Why retracted:** The `put()` method's non-atomicity is **not reachable in production**. The single production caller — `KeyBucketsInOneWalGroup.putPvmListToTargetBucket()` — always calls `keyBucket.clearAll()` (line 341) **before** the `put` loop (lines 348–362). After `clearAll()`, `size == 0`, so `del()` in `put()` returns `false` immediately (line 688–689: `if (size == 0) return false;`). No existing entry is ever deleted by `put()` in the production flow.

- **Production flow trace:**

```
KeyBucketsInOneWalGroup.putPvmListToTargetBucket():  // line 234
  1. Read all existing entries into HashMap           // lines 243-268
  2. Merge incoming entries (deletes, updates, adds)  // lines 270-293
  3. Split check (per-split cellCost ≤ 48)            // lines 295-322
  4. clearAll() ALL key buckets                       // lines 336-346  ← buckets now empty
  5. For each map entry → keyBucket.put()             // lines 348-362  ← del() is always no-op
```

  Additionally, the split pre-check (step 3) uses `PersistValueMeta.cellCostInKeyBucket()` — the same cell-cost formula used by `KeyBucket.KVMeta.calcCellCount()` at put time. After the pre-check passes and buckets are cleared, entries are placed contiguously from index 0 into an empty bucket. Since total cell cost per split ≤ 48, `put` always succeeds.

- **Residual design note:** The `put()` method IS non-atomic in isolation. If ever called on a non-empty bucket without a prior `clearAll()`, the data-loss scenario could occur. The `@TestOnly` method `KeyLoader.putValueByKey()` (line 704) calls `put()` without checking `isPut()`, which is acceptable for test code. If `put()` were used in future production code outside the clear-repopulate pattern, the non-atomicity would need to be addressed.

---

## Bug 2 — Incorrect buffer position in PRE_KEY cleanup loop

- **Severity:** LOW
- **Cited files:** `src/main/java/io/velo/persist/KeyBucket.java`, lines 479–493
- **Root cause:** The while loop in `putTo()` that resets stale PRE_KEY markers to NO_KEY does not reposition the buffer for subsequent iterations. After the first iteration's two absolute `putLong` calls write to the current cell's meta fields, `buffer.position()` remains at the **seq field** of the just-cleaned cell. The next iteration's `buffer.getLong()` reads from this position instead of the **hash field** of the next cell.

- **Code excerpt:**

```java
// lines 479-493
buffer.position(metaIndex(beginResetOldCellIndex));
while (beginResetOldCellIndex < capacity) {
    var targetCellHashValue = buffer.getLong();                    // pos += 8
    buffer.position(buffer.position() + EXPIRE_AT_VALUE_LENGTH);  // pos += 8 → now at seq field

    if (targetCellHashValue != PRE_KEY) {
        break;
    }

    // never happen here, because before put, always delete target key first
    buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH, NO_EXPIRE);           // absolute write
    buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH - HASH_VALUE_LENGTH, NO_KEY); // absolute write
    beginResetOldCellIndex++;
    // BUG: buffer.position is still at the seq field.
    // Next getLong() reads the seq field (value 0L = NO_KEY) instead of next cell's hash.
}
```

- **Impact:** If multiple consecutive PRE_KEY cells exist (which the comment asserts "never happens"), only the first is cleaned up. The second PRE_KEY cell's hash is never read, and the loop breaks because the seq field's value (0L) equals `NO_KEY` (0L), not `PRE_KEY` (-1).

  In the production `clearAll()` → repopulate flow, old PRE_KEY markers are cleared by `clearAll()` before any `putTo()` runs. In the `rePutAll()` path, `buffer.position(0).put(EMPTY_BYTES)` clears everything first. So the PRE_KEY cleanup loop is **effectively dead code** in all current code paths. The structural bug remains but has no runtime impact.

---

## Design Concern 1 — Lost `isUpdate` flag during rePutAll retry

- **Severity:** LOW (design note)
- **Cited files:** `src/main/java/io/velo/persist/KeyBucket.java`, lines 406, 414, 424–425
- **Root cause:** When `put` retries after `rePutAll` (due to fragmented free space), the retry call passes `doDeleteTargetKeyFirst=false`. This means `del` is not called, `isExists` remains `false`, and `isUpdate` is always set to `false` at line 414.

- **Impact:** Production code uses the `clearAll()` → repopulate pattern, so `put` is always called on empty buckets and `rePutAll` is never triggered in production. The `isUpdate` flag loss is therefore **unreachable in production**. It could only affect test code paths that trigger `rePutAll`.

---

## Design Note 2 — Sentinel hash values not reserved at API boundary

- **Severity:** NOTE (theoretical, no practical impact)
- **Cited files:** `src/main/java/io/velo/persist/KeyBucket.java`, lines 59–60, 467, 504–513, 644, 696; `src/main/java/io/velo/KeyHash.java`, lines 73; `src/main/java/io/velo/CompressedValue.java`, line 850

- **Observation:** `KeyBucket` uses `NO_KEY = 0` and `PRE_KEY = -1` as structural sentinels in the hash field. `KeyHash.hash()` returns raw XXHash64 output which could theoretically produce these values. However:

  1. **Probability is ~10⁻¹⁹ per key.** XXHash64 with seed `0x9747b28c` has uniform 64-bit output. The expected number of keys before hitting either sentinel is ~9.2 × 10¹⁸. At millions of keys per second, this would take centuries.

  2. **The codebase already mitigates hash=0 in a related path.** `CompressedValue.decode()` (line 850) has explicit handling: `if (keyHash == 0 && keyBytes != null) { keyHash = KeyHash.hash(keyBytes); }`. The developers are aware of the sentinel collision and guard it where it matters for the wire protocol.

  3. **KeyHashTest** (lines 55–71) asserts `keyHash != 0` for sample keys, confirming awareness.

  4. **No guard is needed in `KeyBucket.put()`.** Adding a `if (keyHash == 0 || keyHash == -1)` remap would add overhead to every write for an event that will never occur in practice. The engineering trade-off is correct as-is.

- **Verdict: NOT A BUG.** The theoretical invariant violation has no practical impact and the codebase already handles the concern where it matters (wire protocol deserialization).

---

## Summary

| # | Description | Severity | Verdict |
|---|-------------|----------|---------|
| 1 | Non-atomic delete-before-space-check → data loss | ~~MEDIUM~~ | **RETRACTED** — not reachable in production |
| 2 | Incorrect buffer position in PRE_KEY cleanup loop | LOW | **VALID** — structural bug in dead code path |
| DC1 | `isUpdate` flag lost during rePutAll retry | LOW | **NOTE** — unreachable in production flow |
| DN2 | Sentinel hash values not reserved | NOTE | **NOT A BUG** — probability ~10⁻¹⁹, existing guards in CompressedValue |

---

## Correction — Thread Safety and the clear-repopulate Pattern

The `KeyBucket` class lives within a **per-slot, thread-local, run-to-completion** model. Each slot worker thread has exclusive access to its slot's data. The production write path (`putPvmListToTargetBucket`) uses a **read-merge-clear-repopulate** batch pattern:

1. Read all entries from the existing bucket into a local `HashMap`
2. Merge incoming WAL entries into the map (handling deletes, updates, adds)
3. Run a split pre-check: redistribute entries across splits using the same `cellCostInKeyBucket()` formula that `put` uses; increase split number if any split exceeds `INIT_CAPACITY` (48 cells)
4. **Clear all key buckets** → buckets are empty (`size=0`, `cellCost=0`)
5. Repopulate from the map: each `put` operates on an empty bucket; `del()` is always a no-op; entries are placed contiguously from index 0

Because:
- The pre-check ensures per-split total cells ≤ 48
- Buckets are empty before repopulation
- Entries are placed contiguously without gaps

…every `put` in step 5 always succeeds. The cell-cost pre-check and the clear-repopulate pattern together eliminate the non-atomicity risk.

---

## Verification Instructions for AI Agent 2 (Reviewer)

1. **Bug 1 retraction:** Confirm that `clearAll()` at `KeyBucketsInOneWalGroup.java:341` runs before the `put` loop at line 355, so `del()` in `KeyBucket.put()` at line 688 returns `false` immediately (size==0).

2. **Bug 2:** Confirm the buffer position is not reset between iterations of the while loop at lines 479–493. Verify the code path is dead in all current flows (`clearAll()` clears PRE_KEY markers before `putTo()` runs).

3. **DC1:** Confirm that the `rePutAll` retry path is not triggered in production (entries are always placed contiguously into empty buckets).

4. **DN2:** Confirm that `CompressedValue.decode()` at line 850 already handles hash=0 during wire deserialization, and that XXHash64 collision probability (~10⁻¹⁹) makes the KeyBucket path practically immune.

---

## Review Feedback

Reviewed by: AI agent 2
Date: 2026-06-11
Commit reviewed: `5ed37d9c` `fix: reject sentinel key hash values 0 and -1 in KeyBucket.put`

### Summary of the Fix

A defensive guard was added in `KeyBucket.put()` (lines 398–401) that throws `IllegalArgumentException` with the message "Reserved key hash for key bucket, key hash=..., key=..." when the incoming `keyHash` equals `NO_KEY` (0) or `PRE_KEY` (-1). This rejects the only known input that could collide with the bucket's structural sentinels and produce the "invisible entry" or "permanently blocked cell" failure modes described in the original Bug 3 analysis.

A targeted test was added in `KeyBucketTest.groovy` (`'reject sentinel key hash values'`, lines 235–253) that verifies the exception is thrown for both `keyHash=0L` and `keyHash=-1L`. The pre-existing test fixture `TestSplitInner.groovy` was updated to use `keyHash=1L` instead of `0L` so the local scratch test script does not trigger the new guard.

### Verification

1. **Code change is correct and minimal.** The guard sits in the first lines of the private `put(...)` overload, after the existing `valueBytes.length` size check. It is executed unconditionally for every put call — no skip paths. The check uses the existing private constants `NO_KEY` and `PRE_KEY` (no new constants were introduced), and the exception message includes the offending `keyHash` and `key` for debuggability.

2. **The guard is correct from a domain-modeling perspective.** The author of Design Note 2 is right that the practical probability of an XXHash64 collision with `0` or `-1` is ~10⁻¹⁹ per key, and that the production `clearAll()` → repopulate flow would still produce a correctly-shaped bucket even if a collision occurred. However, the guard is cheap (two `long` comparisons per put), runs in O(1) per call, and the test directly exercises it. Adding the guard is a textbook example of *defense in depth*: a sentinel that the format reserves should be rejected at the boundary that introduces values into the format, even if the boundary is internal. This matches the existing precedent in `CompressedValue.decode()` (line 850), which already explicitly remaps `keyHash == 0`.

3. **Test correctness.** The new test (`'reject sentinel key hash values'`) uses `thrown(IllegalArgumentException)` from Spock, asserts the message contains `'Reserved key hash'`, and covers both sentinel values. JaCoCo coverage on the changed lines (393–401) is 4/4 lines and 2/2 branches covered — both the `== NO_KEY` and `== PRE_KEY` branches are exercised. The full `KeyBucketTest` suite re-runs clean (BUILD SUCCESSFUL).

4. **Test fixture update is consistent.** `TestSplitInner.groovy` is a one-off scratch file used during development, not part of the regular test suite. Changing its `keyHash` literal from `0L` to `1L` is purely a "make the fixture still run after the new guard" change; it has no semantic effect on what the scratch script demonstrates.

### Strengths

- The fix is the minimal possible change to address the invariant violation: a 4-line guard with no new constants, no new dependencies, and no refactor of the read paths.
- The chosen error type (`IllegalArgumentException`) is consistent with the other validation in the same `put(...)` method (`valueBytes.length` and `cellCount >= INIT_CAPACITY`).
- The error message includes the offending `keyHash` value and the key string, which is exactly the context an operator needs when this fires.
- A new test was added that directly exercises the guard; the test was placed in `KeyBucketTest` next to the other `put`-related cases for discoverability.
- The `TestSplitInner.groovy` update shows the author checked that the guard does not silently break the local scratch workflow.

### Concerns

- **The doc's `NOT A BUG` verdict is at odds with the actual code change.** Design Note 2 (lines 87–102) argues that the guard is unnecessary and that "no guard is needed in `KeyBucket.put()`" because the probability is ~10⁻¹⁹. The committed fix in `KeyBucket.put()` says otherwise. This is not a bug in the code — it is a documentation/code inconsistency. The fix is correct, but the doc's `Summary` table and the `Verdict: NOT A BUG` line should probably be reconciled: either upgrade DN2 to a confirmed fix (low-severity) and update the summary row, or leave DN2 as-is and add a note that the code was fixed defensively despite the "not a bug" verdict.

- **The guard is only in `put()`.** If a future caller writes to the cell hash field through a different path (e.g., direct buffer manipulation, a new overload, a recovery tool), the guard will not fire. The bucket format invariant is now enforced at the `put()` boundary, but not at the `buffer.putLong(metaIndex, ...)` boundary. This is acceptable today because `put()` is the only write path, but it is worth a sentence in the doc.

- **No coverage of the existing `KeyHash` path that *could* still return 0 or -1.** `KeyHash.hash()` and `KeyHash.hashOffset()` are unchanged. In production, every `KeyHash.hash()` call passes through `KeyBucket.put()` (or one of the other callers: `XGroup`, `XBigStrings`, `CompressedValue`), so the new guard catches any value coming from those paths at the bucket boundary. But `KeyHash.hash()` itself still has the same theoretical invariant violation. The fix is localized to the data structure, which is correct, but the doc's discussion of "KeyHash returns raw XXHash64" is now slightly out of date — the conversation is "KeyHash returns raw XXHash64 AND the bucket rejects the sentinels", not "KeyHash returns raw XXHash64 and no one cares".

### Pre-commit Follow-ups (none open)

None — the commit is correct, the test is in place, JaCoCo confirms coverage, and `KeyBucketTest` passes clean.

### Post-commit Follow-ups (suggested, non-blocking)

1. **Reconcile the doc with the code.** Either:
   - Change DN2's verdict to "FIXED — defensive guard added in `KeyBucket.put()`" and update the `Summary` table accordingly, or
   - Leave DN2 as "NOT A BUG" and add a one-line note that "the code was patched defensively despite the verdict, see commit `5ed37d9c`".

2. **Optional: add a property test for `KeyHash.hash()`.** A test that calls `KeyHash.hash()` on a few thousand random inputs and asserts the result is never `0L` or `-1L` would lock in the *current* behavior even though it has near-zero chance of catching a regression. This is purely a documentation-of-contract test; skip if the team prefers minimal coverage.

3. **Optional: add a similar guard in `KeyHash.hashOffset()`.** The other public `KeyHash` entry point is not protected. If a key uses hash tags (see `BaseCommand.tagHash`, line 527), the tag-hash path also flows into `KeyBucket.put()` and is now protected there. No additional code change required unless the team wants belt-and-suspenders coverage at the `KeyHash` layer too.
