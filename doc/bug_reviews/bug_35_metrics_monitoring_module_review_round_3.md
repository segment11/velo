# Bug 35 Metrics & Monitoring Module Review Round 3

Author: AI agent 1
Scope: Follow-up review of the metrics/monitoring module after round 2 fixes, focused on key-analysis monitoring paths in
`KeyAnalysisHandler`, `KeyAnalysisTask`, and related management output.
Status: Open for AI agent 2 review

## Context

Round 1 covered `BigKeyTopK` equality, dynamic `monitor_big_key_top_k` updates, and `RuntimeCpuCollector.close()`.
Round 2 covered big-key lifecycle tracking, dropped Prometheus labels, malformed `view-metrics` labels, and raw
`PriorityQueue` iteration in `view-big-key-top-k`.

This round looks for remaining correctness issues in the key-analysis monitoring surface.

## Finding 1: Key-analysis prefix "top-k" keeps and returns non-top prefixes

Severity: Medium

Files:
- `src/main/java/io/velo/persist/index/KeyAnalysisTask.java:104-164`
- `src/main/java/io/velo/persist/index/KeyAnalysisTask.java:209-216`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:342-344`
- `dyn/src/io/velo/command/ManageCommand.groovy:968-995`

Code excerpts:

```java
final Map<String, Integer> topKPrefixCounts = new HashMap<>();

var sortedPrefixCounts = sortMapByValues(prefixCounts);

for (var entry : sortedPrefixCounts) {
    if (entry.getValue() >= doLogByKeyPrefixCountIfBiggerThan) {
        if (topKPrefixCounts.size() < maxTmpSaveTopKSize) {
            topKPrefixCounts.put(entry.getKey(), entry.getValue());
        }
    }
}
```

```java
public static <V extends Comparable<? super V>> List<Map.Entry<String, V>> sortMapByValues(Map<String, V> map) {
    List<Map.Entry<String, V>> sortedEntries = new ArrayList<>(map.entrySet());
    sortedEntries.sort((a, b) -> {
        int compareValue = a.getValue().compareTo(b.getValue());
        // key length bigger first
        return compareValue == 0 ? b.getKey().length() - a.getKey().length() : compareValue;
    });
    return sortedEntries;
}
```

```java
public CompletableFuture<Map<String, Integer>> getTopKPrefixCounts() {
    // use a copy one
    return eventloop.submit(AsyncComputation.of(() -> new HashMap<>(innerTask.topKPrefixCounts)));
}
```

```groovy
def f = localPersist.indexHandlerPool.keyAnalysisHandler.topKPrefixCounts

f.whenComplete { r, e ->
    ...
    def replies = new Reply[r.size()]
    r.eachWithIndex { entry, i ->
        replies[i] = new BulkReply((entry.key + ': ' + entry.value).bytes)
    }
    finalPromise.set(new MultiBulkReply(replies))
}
```

Root cause:
The prefix-analysis data structure and output path do not preserve "top-k" semantics:

1. `sortMapByValues(...)` sorts counts ascending because it returns `a.getValue().compareTo(b.getValue())`.
2. `iterateAndDoAnalysis(...)` then fills `topKPrefixCounts` while `topKPrefixCounts.size() < 1000`, so when there are more
   than 1000 qualifying prefixes it keeps the lowest qualifying counts instead of the highest counts.
3. `topKPrefixCounts` is a `HashMap`, and `getTopKPrefixCounts()` copies it into another `HashMap`, so even the management
   output order is arbitrary.
4. `manage debug key-analysis ...` renders the map iteration order directly.

Impact:

1. The key-analysis endpoint can show low-frequency prefixes while omitting higher-frequency prefixes once the temporary
   1000-entry cap is reached.
2. Operators using `manage debug key-analysis prefix-count-top-k` to find dominant key prefixes can be misled during
   capacity or hot-key investigation.
3. The output is not stable or sorted, making it difficult to compare repeated snapshots.

Suggested fix:
Sort prefix counts descending by count, keep only the largest `maxTmpSaveTopKSize`, and return a sorted snapshot for the
management command. For example, change the comparator to descending value order and use a `LinkedHashMap` or a
`List<Map.Entry<String, Integer>>` snapshot for `getTopKPrefixCounts()`. Add a regression test where more than the cap of
qualifying prefixes exists and verify that the highest-count prefixes are retained and rendered first.

---

## Finding 2: Key-analysis fullness and metrics count repeated writes as new analyzed keys

Severity: Medium

Files:
- `src/main/java/io/velo/BaseCommand.java:880-886`
- `src/main/java/io/velo/BaseCommand.java:1084-1089`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:103-107`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:119-121`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:130-153`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:361-366`

Code excerpts:

```java
var valueLengthHigh24WithShortTypeLow8 = cv.getUncompressedLength() << 8 | shortType;
indexHandlerPool.getKeyAnalysisHandler().addKey(slotWithKeyHash.rawKey, valueLengthHigh24WithShortTypeLow8);
```

```java
var valueLengthHigh24WithShortTypeLow8 = valueBytes.length << 8 | shortType;
indexHandlerPool.getKeyAnalysisHandler().addKey(key, valueLengthHigh24WithShortTypeLow8);
```

```java
private void createDB() throws RocksDBException {
    this.db = RocksDB.open(openOptions(persistConfig), keysDir.getAbsolutePath());
    ...
    this.addCount = db.getLongProperty("rocksdb.estimate-num-keys");
}
```

```java
this.keyAnalysisNumberTotal = ConfForGlobal.keyAnalysisNumberPercent *
        ConfForGlobal.estimateKeyNumber * ConfForGlobal.slotNumber / 100;
```

```java
eventloop.submit(() -> {
    db.put(Wal.keyBytes(key), bytes);

    addCount++;
    addValueLengthTotal += (valueLengthHigh24WithShortTypeLow8 >> 8);

    if (addCount >= keyAnalysisNumberTotal) {
        isKeyAnalysisNumberFull = true;
    }
});
```

```java
map.put("key_analysis_add_count", new SimpleGauge.ValueWithLabelValues((double) addCount, labelValues));
if (addCount > 0) {
    map.put("key_analysis_all_key_count", new SimpleGauge.ValueWithLabelValues((double) allKeyCount(), labelValues));

    var addValueLengthAvg = (double) addValueLengthTotal / addCount;
    map.put("key_analysis_add_value_length_avg", new SimpleGauge.ValueWithLabelValues(addValueLengthAvg, labelValues));
}
```

Root cause:
`KeyAnalysisHandler.addKey(...)` writes by key into RocksDB, so repeated writes to the same Redis key overwrite the same
analysis record. However, `addCount` and `addValueLengthTotal` are incremented on every sampled write event rather than
only when a new key is added or when the existing key's prior value is replaced consistently.

The surrounding code treats `addCount` as a key cardinality estimate:

1. Startup initializes `addCount` from RocksDB `rocksdb.estimate-num-keys`, which is an estimate of unique stored keys.
2. `keyAnalysisNumberTotal` is derived from configured estimated key cardinality.
3. `isKeyAnalysisNumberFull` stops future key-analysis writes once `addCount >= keyAnalysisNumberTotal`.
4. Metrics publish `key_analysis_add_count` and average value length from `addCount`.

Impact:

1. A small set of hot keys rewritten repeatedly can drive `addCount` to `keyAnalysisNumberTotal` and set
   `isKeyAnalysisNumberFull = true`, after which new keys are no longer recorded for analysis.
2. `key_analysis_add_count` can diverge sharply from `key_analysis_all_key_count`, even though startup treats the same
   counter as unique key cardinality.
3. `key_analysis_add_value_length_avg` becomes an average over sampled write events, not over analyzed keys, which can be
   heavily biased by hot keys.

Suggested fix:
Decide whether the metric is intended to count sampled write events or unique analyzed keys. If it is unique-key based,
check for an existing RocksDB record before incrementing `addCount`, update `addValueLengthTotal` by subtracting the prior
stored length when replacing a key, and use unique-key cardinality for `isKeyAnalysisNumberFull`. If the intended metric is
write-event based, rename the metric and use `allKeyCount()` or a separate unique counter for fullness so hot-key rewrites
cannot stop analysis of new keys.

Add a regression test that writes the same key repeatedly until the old implementation reaches the configured quota, then
verifies that a different new key is still accepted and appears in key analysis.

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-21
Branch: `review/metrics_monitoring`

### Finding 1 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `sortMapByValues(...)` at `KeyAnalysisTask.java:209-216` uses `a.getValue().compareTo(b.getValue())` which sorts **ascending**.
   The secondary tiebreaker sorts by key length descending, but the primary sort is lowest-count-first.

2. `iterateAndDoAnalysis(...)` at `KeyAnalysisTask.java:156-164` iterates the ascending-sorted list and adds entries to
   `topKPrefixCounts` while `topKPrefixCounts.size() < maxTmpSaveTopKSize` (1000). Since the list is sorted ascending, the
   first qualifying entries are the lowest-count prefixes. Once 1000 entries fill the map, all remaining higher-count prefixes
   are silently dropped.

3. `topKPrefixCounts` is a plain `HashMap` at `KeyAnalysisTask.java:104`. `getTopKPrefixCounts()` at
   `KeyAnalysisHandler.java:342-344` copies it into another `HashMap`. HashMap has no ordering guarantees.

4. The management endpoint at `ManageCommand.groovy:991-994` iterates the HashMap with `r.eachWithIndex`, producing
   arbitrary-order output.

**Concrete scenario:** If 2000 prefixes each appear 100+ times (above threshold), only the first 1000 encountered in
ascending count order are kept — these are the lowest-frequency prefixes. The top 1000 by count are discarded.

**Verdict:** Confirmed. The ascending sort combined with the size cap keeps the wrong entries and discards the actual top-k.

### Finding 2 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `KeyAnalysisHandler.addKey(...)` at lines 130-148 submits a RocksDB `put()` and unconditionally increments `addCount` and
   `addValueLengthTotal` on every sampled write. `db.put()` overwrites existing keys in RocksDB, but `addCount++` runs
   regardless of whether the key was new or existing.

2. Startup at `KeyAnalysisHandler.java:103-108` initializes `addCount` from `rocksdb.estimate-num-keys`, which is a unique-key
   cardinality estimate. But after startup, every sampled write increments this counter, including repeated writes to the same
   key. This creates a semantic mismatch: the counter starts as unique-key cardinality but grows as sampled write events.

3. `keyAnalysisNumberTotal` at lines 119-121 is derived from estimated unique key count. `isKeyAnalysisNumberFull` at line 149
   compares `addCount` (now inflated by rewrites) against this unique-key budget. Hot keys can exhaust the budget and stop
   analysis of new keys.

4. Metrics at `KeyAnalysisHandler.java:361-366` publish `key_analysis_add_count` and compute `key_analysis_add_value_length_avg`
   from the inflated `addCount`, so the average is weighted by write frequency, not by key distribution.

**Verdict:** Confirmed. `addCount` conflates sampled write events with unique key cardinality. Hot-key rewrites can prematurely
trigger `isKeyAnalysisNumberFull` and stop new-key analysis.

### Review Summary

| # | AI agent 2 status | Notes |
|---|-------------------|-------|
| 1 | Confirmed | Ascending sort + size cap keeps lowest-count prefixes, discards highest. Output is unsorted HashMap order. |
| 2 | Confirmed | `addCount` counts write events, not unique keys; hot-key rewrites can exhaust the unique-key budget. |

## Bug 1 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `9a9a77c190326a04a3af8c8df7991b8e02640358`
(`fix: sortMapByValues descending so top-k keeps highest-count prefixes`)

### Summary of the fix

The commit changes `KeyAnalysisTask.sortMapByValues(...)` to sort by count descending, changes
`topKPrefixCounts` to `LinkedHashMap`, and returns a `LinkedHashMap` copy from `KeyAnalysisHandler.getTopKPrefixCounts()`.

### Strengths

1. The old ascending comparator is fixed, so each analysis batch now logs and inserts higher-count prefixes first.
2. `LinkedHashMap` preserves insertion order inside `topKPrefixCounts` and in the asynchronous snapshot returned to the
   management command.
3. The added regression covers the comparator directly, including the equal-count tie branch.

### Concern

1. **Blocking: the fix still does not maintain a global top-k across analysis batches.** `iterateAndDoAnalysis(...)` only
   appends while `topKPrefixCounts.size() < maxTmpSaveTopKSize`. Once an earlier batch fills the 1000-entry cap, later
   batches cannot replace lower-count prefixes with higher-count prefixes. The comparator fix ensures each individual
   batch is descending, but it does not guarantee the retained 1000 prefixes are the highest-count prefixes across the full
   analysis scan. This leaves the "keeps and returns non-top prefixes" bug partially open for datasets that span multiple
   RocksDB iteration batches.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.index.KeyAnalysisTaskTest" --tests "io.velo.persist.index.KeyAnalysisHandlerTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms `KeyAnalysisTask.java:212-213` and `KeyAnalysisHandler.java:344` are covered. Coverage also shows the
full-cap branch at `KeyAnalysisTask.java:162` is only partially covered, matching the remaining risk above.

### Verdict

Bug 1 is improved but not fully fixed. The per-batch sort and output order are fixed, but the capped retained set is still
not a true top-k across multiple batches.

## Bug 1 Fix Follow-up Review (commit `8f0eabbc`)

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `8f0eabbc` (`fix: top-k trim across batches — always put then trim lowest when over cap`)

### Summary

The follow-up commit changes `iterateAndDoAnalysis(...)` to always `put` entries into `topKPrefixCounts`
regardless of current size, then after each batch trims to `maxTmpSaveTopKSize` by sorting descending
and keeping only the highest-count entries. The cap is extracted to a `@VisibleForTesting` field so
tests can exercise the trim with a small value.

### Resolution of Previous Blocking Concern

The blocking concern from the first review is **resolved**. Later batches can now replace lower-count
prefixes with higher-count prefixes because:

1. Every qualifying entry from every batch is unconditionally `put` into the map.
2. After each batch, if the map exceeds the cap, it is re-sorted descending and trimmed to the cap size.
3. This ensures the retained set is a true top-k across all batches seen so far.

### Test and Coverage Notes

JaCoCo confirms full coverage of the new trim block:
- `KeyAnalysisTask.java:168` — both branches of `if (topKPrefixCounts.size() > maxTmpSaveTopKSize)` covered
- `KeyAnalysisTask.java:169-172` — all lines `fc` (fully covered)

### Verdict

Bug 1 is now **fully fixed**.

## Bug 1 Fix Follow-up Review - Current Pass

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `8f0eabbc5635ec6dac312407516274c4e9eb1e57`
(`fix: top-k trim across batches — always put then trim lowest when over cap`)

### Summary of the fix

The follow-up commit changes `iterateAndDoAnalysis(...)` so every qualifying prefix is inserted into
`topKPrefixCounts`, then the map is sorted descending and trimmed back to `maxTmpSaveTopKSize` when it exceeds the cap.

### Strengths

1. The previous blocking issue is addressed: later batches can now replace lower-count prefixes retained from earlier
   batches.
2. The trim path rebuilds the `LinkedHashMap` from the descending sorted list, so after trimming the retained map order
   matches the retained top-k order.
3. The cap is now `@VisibleForTesting`, allowing the trim branch to be exercised without creating more than 1000 prefixes.

### Concerns

No blocking concerns found.

Residual follow-up: when the total number of qualifying prefixes stays below the cap, the map is not globally re-sorted
after appending a later batch. The retained set is correct in that case because all qualifying prefixes fit, but the
management output can still be batch insertion order rather than strictly descending by count. If the endpoint promises
descending display order, return a sorted snapshot from `getTopKPrefixCounts()`.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.index.KeyAnalysisTaskTest" --tests "io.velo.persist.index.KeyAnalysisHandlerTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms the trim branch at `KeyAnalysisTask.java:168` has both outcomes covered and lines
`KeyAnalysisTask.java:169-172` are executed.

### Verdict

Bug 1's retained-set/top-k correctness issue is fixed by commit `8f0eabbc`.

## Bug 2 Fix Review Feedback

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `506e87df3227124ce9a0908c4f9ac014135a20c8`
(`fix: addCount only increments for new keys, not rewrite events`)

### Summary of the fix

The commit changes `KeyAnalysisHandler.addKey(...)` to read the existing RocksDB value before writing. New keys increment
`addCount` and add their value length to `addValueLengthTotal`; rewrites keep `addCount` unchanged and adjust
`addValueLengthTotal` by the new-minus-old length delta.

### Strengths

1. The hot-key rewrite path no longer inflates `addCount`, so repeated writes to one key do not prematurely exhaust the
   unique-key analysis budget.
2. Live rewrites now adjust `addValueLengthTotal` by delta, so the average value length remains correct for keys added and
   rewritten during the same process lifetime.
3. The new regression test exercises repeated writes of the same key followed by a different key, and verifies
   `key_analysis_add_count == 2`.

### Concern

1. **Blocking: persisted key-analysis state still initializes `addCount` without initializing `addValueLengthTotal`.**
   `createDB()` restores `addCount` from `rocksdb.estimate-num-keys`, but `addValueLengthTotal` remains `0`. After restart,
   `key_analysis_add_value_length_avg` is wrong before any rewrite. Worse, rewriting an existing analyzed key subtracts the
   old stored length from zero before adding the new length delta; a shorter replacement can make `addValueLengthTotal`
   negative and export a negative average. This leaves the metric-average part of Bug 2 unresolved for non-empty persisted
   key-analysis DBs.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.index.KeyAnalysisHandlerTest" --tests "io.velo.persist.index.KeyAnalysisTaskTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms both branches at `KeyAnalysisHandler.java:152` are covered, with the new-key path at
`KeyAnalysisHandler.java:156-157`, the rewrite delta path at `KeyAnalysisHandler.java:153-154`, and the metric average path
at `KeyAnalysisHandler.java:374-375` covered.

### Verdict

Bug 2 is improved but not fully fixed. The unique `addCount` and fullness behavior for live rewrites is fixed, but
`addValueLengthTotal` remains inconsistent after opening an existing key-analysis DB.

## Bug 2 Fix Follow-up Review

Reviewer: AI agent 2
Date: 2026-05-21
Reviewed commit: `09a3017f3434794fbfc6f5770f25d145dcecd703`
(`fix: initialize addValueLengthTotal from persisted keys on DB open`)

### Summary of the fix

The follow-up commit updates `KeyAnalysisHandler.createDB()` to scan existing RocksDB key-analysis values on open and
initialize `addValueLengthTotal` from their encoded value lengths. The regression now closes and reopens the DB, then
asserts that `addCount` and `addValueLengthTotal` are restored consistently.

### Strengths

1. The previous blocking concern is addressed: persisted key-analysis state now restores both unique key count and total
   value length.
2. The restored total uses the same encoded value format as the live rewrite delta path.
3. The test covers a restart/reopen scenario and verifies the expected total of two unique 10-byte keys.

### Concerns

No blocking concerns found.

Non-blocking resource note: `createDB()` creates a RocksDB iterator for the scan and does not explicitly close it. Existing
code has similar iterator usage, but this new startup scan is a good place to use try/finally or try-with-resources if
`RocksIterator` supports it in this version.

### Test and Coverage Notes

I ran:

```bash
./gradlew :test --tests "io.velo.persist.index.KeyAnalysisHandlerTest" --tests "io.velo.persist.index.KeyAnalysisTaskTest" --rerun-tasks
```

Result: passed.

JaCoCo confirms `KeyAnalysisHandler.java:110-118` are covered for the persisted total rebuild, including both outcomes of
the iterator loop at `KeyAnalysisHandler.java:113`. The live rewrite branches at `KeyAnalysisHandler.java:162-167` and the
metric average at `KeyAnalysisHandler.java:384` are also covered.

### Verdict

Bug 2 is confirmed fixed by commits `506e87df` and `09a3017f`.
