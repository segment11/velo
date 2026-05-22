# Bug 35 Metrics & Monitoring Module Review Round 2

Author: AI agent 1
Scope: `src/main/java/io/velo/metric/`, `src/main/java/io/velo/monitor/`, and metrics/monitoring callers in
`BaseCommand`, `OneSlot`, `ManageCommand`, `DictMap`, and `KeyAnalysisHandler`.
Status: Open for AI agent 2 review

## Context

Round 1 found and reviewed issues around `BigKeyTopK` equality, dynamic `monitor_big_key_top_k` updates, and
`RuntimeCpuCollector.close()`. This round looks for additional metrics/monitoring correctness problems in the current code.

## Finding 1: Big-key monitoring only records reads and never removes stale keys

Severity: Medium

Files:
- `src/main/java/io/velo/BaseCommand.java:706-742`
- `src/main/java/io/velo/BaseCommand.java:924-1086`
- `src/main/java/io/velo/persist/OneSlot.java:818-821`
- `src/main/java/io/velo/persist/OneSlot.java:1315-1350`
- `src/main/java/io/velo/persist/OneSlot.java:1566-1616`
- `src/main/java/io/velo/persist/OneSlot.java:2136-2138`
- `dyn/src/io/velo/command/ManageCommand.groovy:176-181`

Code excerpts:

```java
// BaseCommand.getValueBytesByCv(...)
if (slotWithKeyHash != null && byPassGetSet == null) {
    localPersist.oneSlot(slotWithKeyHash.slot).monitorBigKeyByValueLength(slotWithKeyHash.rawKey, decompressed.length);
}
```

```java
// OneSlot.monitorBigKeyByValueLength(...)
public void monitorBigKeyByValueLength(String key, int valueBytesLength) {
    if (valueBytesLength >= BIG_KEY_LENGTH_CHECK) {
        bigKeyTopK.add(key, valueBytesLength);
    }
}
```

```java
// OneSlot.collect()
if (bigKeyTopK != null) {
    map.put("big_key_count", (double) bigKeyTopK.sizeIfBiggerThan(BIG_KEY_LENGTH_CHECK));
}
```

Root cause:
`BigKeyTopK` is only updated from `BaseCommand.getValueBytesByCv(...)`, which is a read/materialization path. The write path
in `BaseCommand.set(...)` already has the raw `valueBytes` length, but it does not call `monitorBigKeyByValueLength(...)`.
Deletion paths (`BaseCommand.remove(...)`, `OneSlot.remove(...)`, `OneSlot.removeDelay(...)`) and `OneSlot.flush()` also do
not remove or reset entries from `bigKeyTopK`.

Impact:

1. A large value written by `SET` is invisible to `view-big-key-top-k` and `big_key_count` until it is later read.
2. A key that was once read as a big value remains in `BigKeyTopK` after the key is deleted, flushed, expired, or overwritten
   with a smaller value.
3. `big_key_count` and `manage slot N view-big-key-top-k` can report keys that no longer exist or are no longer big, which
   makes the monitoring output misleading during incident/debug workflows.

Suggested fix:
Update the tracker on writes and lifecycle removals, not only reads. A minimal design would add a `remove(String key)` or
`update(String key, int length)` API to `BigKeyTopK`; call it from `BaseCommand.set(...)`, `BaseCommand.remove(...)` /
`OneSlot.removeDelay(...)`, and `OneSlot.flush()`. If the intended semantics are "big keys observed by reads", rename the
metric and endpoint to make that explicit and avoid using it as current stored-key state.

---

## Finding 2: Global and key-analysis Prometheus labels are silently dropped

Severity: Medium

Files:
- `src/main/java/io/velo/metric/SimpleGauge.java:71-75`
- `src/main/java/io/velo/metric/SimpleGauge.java:101-115`
- `src/main/java/io/velo/persist/OneSlot.java:1984-1988`
- `src/main/java/io/velo/persist/OneSlot.java:2008-2013`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:348-361`

Code excerpts:

```java
// SimpleGauge constructor and collection
public SimpleGauge(String familyName, String help, String... labels) {
    this.familyName = familyName;
    this.help = help;
    this.labels = List.of(labels);
}

samples.add(new MetricFamilySamples.Sample(entry.getKey(), labels, entryValue.labelValues, entryValue.value));
```

```java
// OneSlot.globalGauge has no label names
final static SimpleGauge globalGauge = new SimpleGauge("global", "Global metrics.");

// but raw values pass a slot label value
var labelValues = List.of("-1");
map.put("global_up_time", new SimpleGauge.ValueWithLabelValues((double) MultiWorkerServer.UP_TIME, labelValues));
```

```java
// KeyAnalysisHandler.keyAnalysisGauge also has no label names
final static SimpleGauge keyAnalysisGauge = new SimpleGauge("keys", "Key analysis metrics.");

var labelValues = List.of("-1");
map.put("key_analysis_add_count", new SimpleGauge.ValueWithLabelValues((double) addCount, labelValues));
```

Root cause:
`SimpleGauge` uses the constructor-supplied `labels` list as the Prometheus label names for every sample. `globalGauge` and
`keyAnalysisGauge` are created without label names, but their raw getters pass `List.of("-1")` as a label value. Prometheus
text serialization iterates label names, not extra label values, so the intended `slot="-1"` label is not emitted.

Impact:

1. Global metrics intended to be marked as slot `-1` are exported without that label in the `/?metrics` Prometheus output.
2. Key-analysis metrics have the same mismatch and lose their intended global-slot context.
3. Existing tests inspect collected sample counts, but they do not verify serialized label output, so this can regress
   unnoticed.

Suggested fix:
Construct these gauges with the expected label name:

```java
final static SimpleGauge globalGauge = new SimpleGauge("global", "Global metrics.", "slot");
final static SimpleGauge keyAnalysisGauge = new SimpleGauge("keys", "Key analysis metrics.", "slot");
```

Also consider validating in `SimpleGauge.collect()` that `entryValue.labelValues.size() == labels.size()` so future metrics
cannot silently drop or underflow labels.

---

## Finding 3: `view-metrics` emits malformed Prometheus label syntax

Severity: Low

Files:
- `dyn/src/io/velo/command/ManageCommand.groovy:166-175`

Code excerpt:

```groovy
if (subSubCmd == 'view-metrics') {
    def all = oneSlot.collect()

    // prometheus format
    def sb = new StringBuilder()
    all.each { k, v ->
        sb << k << '{slot="' << slot << '",} ' << v << '\n'
    }
    return new BulkReply(sb.toString().bytes)
}
```

Root cause:
The management endpoint claims to return Prometheus format, but it renders labels as `{slot="0",}` with a trailing comma
before `}`. Prometheus/OpenMetrics label sets do not use a trailing comma.

Impact:

1. `manage slot N view-metrics` can produce output that Prometheus parsers reject or treat as non-standard.
2. This endpoint is documented in `doc/design/13_metrics_monitoring_design.md` as a per-slot metric inspection endpoint, so
   users may reasonably try to scrape or parse it.
3. Root `/?metrics` uses `TextFormat.write004(...)`, but the per-slot management endpoint bypasses that safer formatter.

Suggested fix:
Remove the trailing comma:

```groovy
sb << k << '{slot="' << slot << '"} ' << v << '\n'
```

Or reuse Prometheus `Collector`/`TextFormat` plumbing for per-slot metrics instead of hand-rolling text exposition.

---

## Finding 4: `view-big-key-top-k` returns heap iteration order, not sorted top-k order

Severity: Low

Files:
- `src/main/java/io/velo/monitor/BigKeyTopK.java:50-56`
- `dyn/src/io/velo/command/ManageCommand.groovy:176-181`

Code excerpt:

```groovy
} else if (subSubCmd == 'view-big-key-top-k') {
    def queue = oneSlot.bigKeyTopK.queue
    def str = queue.collect {
        it.key() + ': ' + it.length()
    }.join(', ')
    return new BulkReply(str.bytes)
}
```

Root cause:
`BigKeyTopK` stores entries in a `PriorityQueue` ordered as a min-heap by length. The management endpoint iterates the raw
queue directly. A `PriorityQueue` only guarantees that `peek()` is the smallest element; its iterator does not return
elements in sorted order.

Impact:
The endpoint name says `view-big-key-top-k`, but users do not get a descending biggest-to-smallest list. The output can look
arbitrary, which undermines the operational purpose of the endpoint when trying to identify the largest keys quickly.

Suggested fix:
Expose a sorted snapshot method from `BigKeyTopK`, for example:

```java
public List<BigKey> snapshotDescending() {
    return queue.stream()
            .sorted(Comparator.comparingInt(BigKey::length).reversed())
            .toList();
}
```

Then have `ManageCommand` render that snapshot instead of the mutable raw queue.

## Summary

| # | Finding | Severity | Impact |
|---|---------|----------|--------|
| 1 | Big-key monitoring only records reads and never removes stale keys | Medium | `big_key_count` and top-k views can miss current big keys and keep deleted/small keys |
| 2 | Global/key-analysis labels are silently dropped | Medium | Prometheus output loses intended `slot="-1"` context |
| 3 | `view-metrics` emits malformed label syntax | Low | Per-slot metric endpoint is not valid Prometheus-style text |
| 4 | `view-big-key-top-k` returns heap iteration order | Low | Management output is not sorted as top-k users expect |

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-20
Branch: `review/metrics_monitoring`

### Finding 1 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `monitorBigKeyByValueLength` is called only from `BaseCommand.java:734,740` inside `getValueBytesByCv(...)`, which is a read/materialization path. No other caller exists in the codebase (grep confirmed: only 3 matches total — the method definition in `OneSlot.java` and 2 calls in `BaseCommand.java`).
2. The write path `BaseCommand.set(...)` at lines 924–1086 has the raw `valueBytes` length available but never calls `monitorBigKeyByValueLength`. A `SET` of a 100 KB value will not appear in `view-big-key-top-k` until it is later read.
3. Deletion paths (`BaseCommand.remove(...)`, `OneSlot.remove(...)`, `OneSlot.removeDelay(...)`, `OneSlot.flush()`) do not remove entries from `BigKeyTopK`. Deleted or expired keys remain in the top-k tracker indefinitely.
4. `BigKeyTopK` has no `remove(String key)` method at all, so even if callers wanted to clean up, the API does not support it.

**Verdict:** Confirmed. The tracker is write-blind and delete-blind. It only captures keys observed during reads, and stale entries are never evicted.

### Finding 2 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `globalGauge` at `OneSlot.java:1985` is constructed as `new SimpleGauge("global", "Global metrics.")` with no label names. `SimpleGauge` stores `this.labels = List.of(labels)` which produces an empty list `[]`.
2. `keyAnalysisGauge` at `KeyAnalysisHandler.java:348` is constructed as `new SimpleGauge("keys", "Key analysis metrics.")` with no label names — same empty labels list.
3. In `SimpleGauge.collect()` at lines 103 and 114, each sample is created as `new MetricFamilySamples.Sample(entry.getKey(), labels, entryValue.labelValues, ...)`. The Prometheus `TextFormat.write004` serialization iterates `labelNames` to emit `name="value"` pairs. When `labelNames` is empty but `labelValues` is `["-1"]`, the loop body never executes, and the label value is silently discarded.
4. For comparison, `requestHandlerGauge` at `RequestHandler.java:588` and `compressStatsGauge` at `CompressStats.java:87` are constructed with `"name"` as the label name, so their labels work correctly. The bug is specific to `globalGauge` and `keyAnalysisGauge`.

**Verdict:** Confirmed. Two gauges pass label values without corresponding label names, causing Prometheus text serialization to drop the intended `slot="-1"` context entirely.

### Finding 3 — CONFIRMED (Severity: Low)

**Verification performed:**

1. `ManageCommand.groovy:173` renders `'{slot="' << slot << '",} '` which produces `{slot="0",}` — a trailing comma before the closing brace.
2. Prometheus/OpenMetrics label syntax does not allow a trailing comma. Valid format is `{slot="0"}`.
3. The root `/?metrics` endpoint uses `TextFormat.write004(...)` which generates correct syntax, but the per-slot `manage slot N view-metrics` endpoint bypasses that and hand-rolls the format.

**Verdict:** Confirmed. The trailing comma produces invalid Prometheus label syntax.

### Finding 4 — CONFIRMED (Severity: Low)

**Verification performed:**

1. `ManageCommand.groovy:177-180` calls `oneSlot.bigKeyTopK.queue.collect { ... }`, which iterates the `PriorityQueue` via Groovy's `.collect()` method.
2. `PriorityQueue.iterator()` is inherited from `AbstractCollection` and does NOT return elements in priority order. It returns them in internal heap array order.
3. For a min-heap tracking the top 3 largest keys, the raw iteration order could be `[key:7, key:9, key:8]` instead of the expected descending `[key:9, key:8, key:7]`.
4. `BigKeyTopK` has no sorted snapshot method. The `getQueue()` accessor returns the mutable internal queue directly.

**Verdict:** Confirmed. The endpoint iterates the raw heap instead of a sorted snapshot, producing non-descending output.

### Review Summary

| # | AI agent 2 status | Notes |
|---|-------------------|-------|
| 1 | Confirmed | Read-only tracking with no write or delete integration. Tracker lacks a `remove` API. |
| 2 | Confirmed | Two gauges constructed without label names cause Prometheus serialization to drop label values. |
| 3 | Confirmed | Trailing comma in hand-rolled Prometheus text format. |
| 4 | Confirmed | `PriorityQueue.iterator()` does not yield sorted order. Needs a sorted snapshot method. |

## Bug 1 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-20
Reviewed commit: `2a2f3c53fd72dd3af381f980f624785f03ad89f1` (`fix: track big keys on write path and remove on delete`)

### Summary of the fix

The commit adds `BigKeyTopK.remove(String key)`, calls it from `OneSlot.removeDelay(...)`, and adds a normal
`BaseCommand.set(...)` write-path call to `monitorBigKeyByValueLength(...)` after the regular compressed/raw value write.
It also adds a focused unit test for `BigKeyTopK.remove(...)`.

### Strengths

1. Explicit delete through `OneSlot.removeDelay(...)` now removes the key from the top-k tracker.
2. Normal non-big-string `BaseCommand.set(...)` writes now feed raw `valueBytes.length` into the tracker.
3. `BigKeyTopK.remove(...)` is covered by `BigKeyTopKTest`; JaCoCo shows lines 109-110 covered after running
   `./gradlew :test --tests "io.velo.monitor.BigKeyTopKTest" --rerun-tasks`.

### Concerns

1. **Blocking: overwrite-to-small still leaves stale big-key entries.** `OneSlot.monitorBigKeyByValueLength(...)` only adds
   when `valueBytesLength >= BIG_KEY_LENGTH_CHECK` and does nothing for smaller values. Because the new set-path call uses
   this method directly, a key that was previously tracked as big remains in `BigKeyTopK` after being overwritten with a
   small value.

2. **Blocking: `OneSlot.flush()` still does not clear `BigKeyTopK`.** The original finding called out flush as a stale-entry
   source. The current `flush()` implementation clears WALs, LRUs, chunks, big-string files, key loader state, and binlog,
   but never clears or recreates `bigKeyTopK`, so `big_key_count` and `view-big-key-top-k` can still report keys after a
   flush.

3. **Blocking: big-string writes still bypass write-path tracking.** `BaseCommand.set(...)` returns from the
   `bigStringNoMemoryCopy != null` branch immediately after `oneSlot.put(...)`, before the new
   `monitorBigKeyByValueLength(...)` call. Large values stored through this branch remain invisible until a later read.

4. **Test gap: the production integration paths are not covered.** The added test validates only the helper method. It does
   not prove that `SET` adds a big key, smaller overwrite removes it, delete removes it, flush clears it, or the big-string
   branch records it. JaCoCo confirms the new `BaseCommand.java:1087` and `OneSlot.java:1330` integration lines were not
   covered by the focused test run.

### Verdict

Bug 1 is only partially fixed. Keep the commit's helper API and explicit-delete cleanup, but extend the fix before closing
this bug: add an update API that removes entries for below-threshold values, call it from all write paths including the
big-string branch, clear or recreate the tracker during `OneSlot.flush()`, and add regression tests that exercise those
production paths.

## Bug 1 Fix Review Feedback - Second Pass

Reviewer: AI agent 2
Date: 2026-05-20
Reviewed commit: `d4ef80554336ed59565458fd211685cd35aad453`
(`fix: handle overwrite-to-small, flush, and big-string write tracking for big keys`)

### Summary of the fix

This commit addresses the three blocking gaps from the prior fix review:

1. `OneSlot.monitorBigKeyByValueLength(...)` now removes a key when the observed length is below
   `BIG_KEY_LENGTH_CHECK`, fixing big-to-small overwrites.
2. The `bigStringNoMemoryCopy` branch in `BaseCommand.set(...)` now records `bigStringNoMemoryCopy.length` before the
   early return.
3. `OneSlot.flush()` now recreates `BigKeyTopK` with the same configured capacity.

### Strengths

1. The small-overwrite behavior now uses the same monitor API as writes and reads, so callers do not need a separate
   below-threshold cleanup path.
2. The big-string early-return path is now explicitly covered.
3. Recreating the tracker during flush preserves the configured top-k capacity while dropping stale entries.

### Concern

1. **Blocking: expired keys can still remain in `BigKeyTopK`.** The original Finding 1 included keys that are "deleted,
   flushed, expired, or overwritten." This commit fixes delete, flush, overwrite-to-small, and big-string writes, but
   expiration callbacks still only notify `handlersRegisteredList`. `OneSlot.handleWhenCvExpiredOrDeleted(...)` does not
   remove the expired key from `bigKeyTopK`, and `BaseCommand.getCv(...)` returns `null` for expired values before
   `getValueBytesByCv(...)` can update the tracker. A big key with TTL can therefore remain in `big_key_count` and
   `view-big-key-top-k` after it expires.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.monitor.BigKeyTopKTest" --tests "io.velo.persist.OneSlotTest" --tests "io.velo.BaseCommandTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms execution of the newly added production lines:

1. `BaseCommand.java:973` big-string monitor call: covered.
2. `BaseCommand.java:1092` normal set monitor call: covered.
3. `OneSlot.java:825` below-threshold remove branch: covered.
4. `OneSlot.java:1335` explicit delete remove branch: covered.
5. `OneSlot.java:1641` flush tracker recreation: covered.

However, this commit changed only production files and did not add Bug 1 regression assertions. The current coverage is
mostly incidental from existing broad tests, and there is still no regression test proving expiration removes a stale
big-key entry.

### Verdict

Bug 1 is improved but still not fully fixed. Add `bigKeyTopK.remove(key)` to the expiration/deletion callback path, or make
`OneSlot` itself register a tracker cleanup handler, then add a regression test for an expired tracked big key disappearing
from `big_key_count` / `view-big-key-top-k`.

## Bug 1 Fix Review Feedback - Third Pass

Reviewer: AI agent 2
Date: 2026-05-20
Reviewed commit: `08899006db6b6b672863dad06f5492caeacc9a92`
(`fix: remove expired keys from big key tracker`)

### Summary of the fix

The commit adds `bigKeyTopK.remove(key)` to `OneSlot.handleWhenCvExpiredOrDeleted(...)`. This improves the callback-based
expiration/deletion path used by `KeyLoader` callbacks and `Wal.intervalDeleteExpiredBigStringFiles()`.

### Strengths

1. The callback path now cleans the top-k tracker before forwarding to registered lifecycle handlers.
2. The change is small and preserves existing handler behavior.

### Concern

1. **Blocking: expired WAL entries still bypass tracker cleanup.** Not all expiration flows go through
   `handleWhenCvExpiredOrDeleted(...)`. `OneSlot.getFromWal(...)` checks the current WAL value and, if `v.isExpired()`,
   only sets `isExpiredFlagArray[0] = true` and returns `null`. Callers such as `OneSlot.get(...)` and `OneSlot.exists(...)`
   then return without invoking `handleWhenCvExpiredOrDeleted(...)` or removing the key from `bigKeyTopK`. A large value
   written with a near-term TTL can still remain in `big_key_count` and `view-big-key-top-k` after it expires while it is
   still represented by WAL.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest" --rerun-tasks
```

Result: passed.

JaCoCo shows the new callback cleanup line `OneSlot.java:525` covered. The same report also shows the expired-WAL branch
`OneSlot.java:1292-1294` covered, but that branch still performs no tracker cleanup. The commit did not add a regression
test that tracks a big key, lets it expire from WAL, reads/checks existence, and verifies the key is removed from
`BigKeyTopK`.

### Verdict

Bug 1 is still not fully fixed. The callback-based expiration path is improved, but the WAL expiration path remains stale.
Add tracker cleanup when `getFromWal(...)` detects an expired value, or route that branch through the same lifecycle cleanup
mechanism, then add a focused regression assertion for an expired tracked WAL key.

## Bug 1 Fix Review Feedback - Fourth Pass

Reviewer: AI agent 2
Date: 2026-05-20
Reviewed commit: `de7df096f0c6928c6c44247781d3e326e713513f`
(`fix: remove expired keys from big key tracker on WAL expiry detection`)

### Summary of the fix

The commit adds `bigKeyTopK.remove(key)` inside `OneSlot.getFromWal(...)` when the current WAL value is expired. This fixes
the expired-WAL stale tracker case identified in the third-pass review.

### Strengths

1. The WAL expiration branch now cleans the tracker before setting `isExpiredFlagArray[0]`.
2. The fix applies to both `OneSlot.get(...)` and `OneSlot.exists(...)`, because both route through `getFromWal(...)`.
3. The change is narrow and does not alter the existing expired-value return semantics.

### Concern

1. **Blocking: persisted key-bucket expiration still bypasses tracker cleanup.** After the WAL miss path,
   `OneSlot.get(...)` calls `keyLoader.getValueXByKey(...)`. `KeyBucket.getValueXByKey(...)` returns the value plus
   `expireAt` even when it is expired, and `OneSlot.get(...)` then returns `null` at the `expireAt < now` check without
   removing the key from `bigKeyTopK`. `OneSlot.exists(...)` has the same shape through
   `keyLoader.getExpireAtAndSeqByKey(...)`, which returns `null` for expired entries without invoking the
   `handleWhenCvExpiredOrDeleted(...)` cleanup path. A tracked big key that has already been persisted out of WAL can
   therefore remain in `big_key_count` and `view-big-key-top-k` after TTL expiry.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest" --rerun-tasks
```

Result: passed.

JaCoCo shows the new WAL-expiry cleanup line `OneSlot.java:1294` covered. The same report also shows the persisted-value
expiry branch at `OneSlot.java:1201-1202` covered, but that branch still returns without tracker cleanup. The commit did
not add a focused regression test that proves an expired tracked key is removed from `BigKeyTopK`.

### Verdict

Bug 1 is still not fully fixed. WAL expiry is now handled, but persisted key-bucket expiry remains stale. Add tracker
cleanup when the persisted `valueBytesWithExpireAtAndSeq` / `ExpireAtAndSeq` path determines a key is expired, or route
those paths through the same lifecycle cleanup mechanism, then add focused regression assertions for both WAL and persisted
expired tracked keys.

## Bug 1 Fix Review Feedback - Fifth Pass

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `b81039006ffb03651fabdb5a8858716973573590`
(`fix: remove expired persisted keys from big key tracker on get and exists`)

### Summary of the fix

The commit adds tracker cleanup for expired persisted values in `OneSlot.get(...)` and attempts to add equivalent cleanup
in `OneSlot.exists(...)`.

### Strengths

1. The persisted `get(...)` path now removes the key from `BigKeyTopK` before returning `null` for an expired persisted
   value.
2. This closes the persisted-read stale tracker case identified in the fourth-pass review.

### Concern

1. **Blocking: the persisted `exists(...)` cleanup branch is ineffective.** `OneSlot.exists(...)` calls
   `keyLoader.getExpireAtAndSeqByKey(...)`, but the underlying `KeyBucket.getExpireAtAndSeqByKey(...)` returns `null` for
   expired entries (`return r.isExpired() ? null : r`). As a result, the new `expireAtAndSeq != null &&
   expireAtAndSeq.isExpired()` branch in `OneSlot.exists(...)` cannot run for expired persisted keys, so an `EXISTS`-style
   access can still leave a tracked persisted big key stale.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms `OneSlot.java:1203` (persisted `get(...)` cleanup) is covered. It also confirms
`OneSlot.java:1327-1328` (persisted `exists(...)` cleanup) is not covered, matching the control-flow issue above. The commit
still does not add a focused regression test that asserts an expired tracked persisted key is removed from `BigKeyTopK`.

### Verdict

Bug 1 is improved but still not fully fixed. The persisted `get(...)` path is now handled, but the persisted `exists(...)`
path needs a reachable way to observe expired metadata and remove the key, or it should route through another cleanup path
that does not discard expiration state before `OneSlot.exists(...)` can act on it. Add a focused regression assertion for
that `exists(...)` case.

## Bug 1 Fix Review Feedback - Sixth Pass

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `67289415e1bef283f17a7463a0411d18c4d4894f`
(`fix: remove stale big keys from tracker when exists detects null persisted entry`)

### Summary of the fix

The commit changes the persisted `exists(...)` miss path to remove the key from `BigKeyTopK` when
`keyLoader.getExpireAtAndSeqByKey(...)` returns `null`. This makes the cleanup reachable for expired persisted entries,
because `KeyBucket.getExpireAtAndSeqByKey(...)` intentionally maps expired metadata to `null`.

### Strengths

1. The previously unreachable `expireAtAndSeq.isExpired()` cleanup branch is replaced with a reachable null-result cleanup.
2. The change is idempotent for genuinely missing keys because removing a non-tracked key from `BigKeyTopK` is harmless.
3. Together with the prior commits, the original Bug 1 lifecycle cases are now covered: write tracking, overwrite-to-small
   cleanup, explicit delete cleanup, flush reset, callback expiration cleanup, WAL expiration cleanup, persisted `get(...)`
   expiration cleanup, and persisted `exists(...)` expiration cleanup.

### Concerns

No blocking concerns found.

Residual test-quality gap: the current tests execute the changed lines, but they still do not include a focused assertion
that a tracked big key disappears from `BigKeyTopK` / `big_key_count` for each lifecycle path. The coverage is sufficient
to review this commit's control flow, but a targeted regression test would make the fix harder to regress.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms the relevant cleanup lines are executed:

1. `OneSlot.java:1203` persisted `get(...)` expiration cleanup: covered.
2. `OneSlot.java:1297` WAL expiration cleanup: covered.
3. `OneSlot.java:1328` persisted `exists(...)` null-result cleanup: covered.
4. `OneSlot.java:1350` explicit remove cleanup: covered.
5. `OneSlot.java:1656` flush tracker recreation: covered.

### Verdict

Bug 1 is confirmed fixed by the current commit stack. Recommended follow-up: add focused regression assertions around
`BigKeyTopK` state for the write, delete, flush, overwrite-to-small, WAL-expiry, persisted-get-expiry, and
persisted-exists-expiry paths.

## Bug 2 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `fc4d838895060c1b6cf4fbd832eb698bf9fd67a3`
(`fix: add missing slot label name to globalGauge and keyAnalysisGauge`)

### Summary of the fix

The commit adds the missing `"slot"` label name to `OneSlot.globalGauge` and
`KeyAnalysisHandler.keyAnalysisGauge`. This aligns both metric collectors with the raw getter label values that already
pass `List.of("-1")` for global and key-analysis samples.

### Strengths

1. The production change is minimal and directly addresses the confirmed root cause.
2. Both affected metric families now have matching label names and label values, so Prometheus text serialization can emit
   `slot="-1"` instead of silently dropping the label value.
3. The added `SimpleGaugeTest` coverage verifies that constructor label names are propagated into collected samples.

### Concerns

No blocking concerns found.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.metric.SimpleGaugeTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms `SimpleGauge.java` coverage for the constructor and both `collect()` sample creation paths.

### Verdict

Bug 2 is confirmed fixed by commit `fc4d8388`.

## Bug 3 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `3d09ea0919b654ef69c255018d9eea8af9499592`
(`fix: remove trailing comma in view-metrics Prometheus label syntax`)

### Summary of the fix

The commit changes `manage slot N view-metrics` output from `{slot="N",}` to `{slot="N"}` in
`ManageCommand.manageInOneSlot()`.

### Strengths

1. The production change is exactly scoped to the malformed Prometheus label syntax.
2. The existing dynamic command test now exercises the `view-metrics` branch and asserts that the old `",}` token is absent.
3. The resulting label shape matches standard Prometheus text format label syntax for a single `slot` label.

### Concerns

No blocking concerns found.

Residual test-quality note: the regression assertion checks that the old malformed token is absent. A stricter assertion
that the response contains `{slot="0"}` would make the intended output format more explicit.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.command.ManageCommandTest.test manage in on slot" --rerun-tasks
```

Result: passed.

JaCoCo confirms `ManageCommand.groovy:173`, the modified formatter line, is covered.

### Verdict

Bug 3 is confirmed fixed by commit `3d09ea09`.

## Bug 4 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `dfb5d8bbe25ddf072a8f2e1eff0de3235fe260fe`
(`fix: return sorted descending order in view-big-key-top-k`)

### Summary of the fix

The commit adds `BigKeyTopK.snapshotDescending()` and changes `manage slot N view-big-key-top-k` to render that sorted
snapshot instead of iterating the internal `PriorityQueue` heap directly.

### Strengths

1. The fix addresses the actual root cause: `PriorityQueue` iteration order is heap array order, not sorted order.
2. The new API returns a snapshot sorted by key length descending, which matches the endpoint name and operational use.
3. The command no longer depends on the mutable internal queue for display order.

### Concerns

No blocking concerns found.

Residual test-quality note: the unit test verifies `BigKeyTopK.snapshotDescending()` ordering, and the command test covers
the command branch, but the command test still only asserts that a `BulkReply` is returned for `view-big-key-top-k`. A
future assertion on the exact rendered order would give stronger end-to-end regression coverage.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.monitor.BigKeyTopKTest" --tests "io.velo.command.ManageCommandTest.test manage in on slot" --rerun-tasks
```

Result: passed.

JaCoCo confirms `BigKeyTopK.java:117-119` and `ManageCommand.groovy:177` are covered.

### Verdict

Bug 4 is confirmed fixed by commit `dfb5d8bb`.
