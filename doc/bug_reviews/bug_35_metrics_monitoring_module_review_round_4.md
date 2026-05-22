# Bug 35 Metrics & Monitoring Module Review Round 4

Author: AI agent 1
Scope: Follow-up review of the metrics/monitoring module after round 3 fixes, focused on metric collector lifecycles and
key-analysis monitoring paths.
Status: Open for AI agent 2 review

## Context

Round 1 covered `BigKeyTopK` equality, dynamic `monitor_big_key_top_k` updates, and `RuntimeCpuCollector.close()`.
Round 2 covered big-key lifecycle tracking, dropped Prometheus labels, malformed `view-metrics` labels, and raw
`PriorityQueue` iteration in `view-big-key-top-k`.
Round 3 covered key-analysis prefix top-k ordering and rewrite accounting for key-analysis metrics.

This round looks for remaining lifecycle and operational issues in the metrics collection surface.

## Finding 1: Static metric raw getters survive component cleanup and keep stale instances

Severity: Medium

Files:
- `src/main/java/io/velo/metric/SimpleGauge.java:47-53`
- `src/main/java/io/velo/metric/SimpleGauge.java:94-115`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:367-389`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:392-399`
- `src/main/java/io/velo/RequestHandler.java:69-72`
- `src/main/java/io/velo/RequestHandler.java:587-603`
- `src/main/java/io/velo/CompressStats.java:55-92`
- `src/main/java/io/velo/MultiWorkerServer.java:1198-1227`
- `src/main/java/io/velo/persist/LocalPersist.java:471-479`
- `src/main/java/io/velo/persist/index/IndexHandlerPool.java:101-118`

Code excerpts:

```java
public void addRawGetter(RawGetter rawGetter) {
    rawGetterList.add(rawGetter);
}

/** Clears all registered raw getters. */
public void clearRawGetterList() {
    rawGetterList.clear();
}
```

```java
for (var rawGetter : rawGetterList) {
    var raw = rawGetter.get();
    ...
    samples.add(new MetricFamilySamples.Sample(entry.getKey(), labels, entryValue.labelValues, entryValue.value));
}
```

```java
final static SimpleGauge keyAnalysisGauge = new SimpleGauge("keys", "Key analysis metrics.", "slot");

private void initMetricsCollect() {
    keyAnalysisGauge.addRawGetter(() -> {
        var labelValues = List.of("-1");
        ...
        map.put("key_analysis_add_count", new SimpleGauge.ValueWithLabelValues((double) addCount, labelValues));
        if (addCount > 0) {
            map.put("key_analysis_all_key_count", new SimpleGauge.ValueWithLabelValues((double) allKeyCount(), labelValues));
            ...
        }
        return map;
    });
}

public void cleanUp() {
    isStopped = true;
    ...
    db.close();
}
```

```java
void stop() {
    System.out.println("Worker " + workerId + " stopped callback");
    isStopped = true;
}

private void initMetricsCollect() {
    requestHandlerGauge.addRawGetter(() -> {
        var labelValues = List.of(workerIdStr);
        ...
        return map;
    });
}
```

```java
public CompressStats(String name, String prefix) {
    compressStatsGauge.addRawGetter(() -> {
        var labelValues = List.of(name);
        ...
        return map;
    });
}
```

Root cause:
`SimpleGauge` supports adding raw getters to a static collector, but it only supports clearing the entire getter list. Several
runtime components register lambdas that capture instance state:

1. `KeyAnalysisHandler` registers a raw getter on the static `keyAnalysisGauge`.
2. `RequestHandler` registers a raw getter on the static `requestHandlerGauge`.
3. `CompressStats` registers a raw getter on the static `compressStatsGauge`.

Shutdown closes or stops those components, but it does not unregister the raw getters. `MultiWorkerServer.onStop()` calls
`requestHandler.stop()` and `localPersist.cleanUpAsync()`, which eventually calls `IndexHandlerPool.cleanUp()` and
`KeyAnalysisHandler.cleanUp()`. None of these paths remove the static raw getters that were registered by the stopped
instances.

Impact:

1. Recreating the server or these components in the same JVM adds duplicate raw getters with the same metric names and label
   values, for example `request_handler{worker_id="0"}` and `compress_stats{name="slot_worker_0"}`.
2. Old getters keep stopped instances reachable, creating a memory leak across test lifecycles, embedded-server restarts, or
   launcher reinitialization in the same JVM.
3. The key-analysis getter can continue to call `allKeyCount()` on a handler whose RocksDB instance was closed. Even when this
   only returns fallback values, the Prometheus scrape contains stale or duplicate samples.
4. The global `/?metrics` endpoint invokes `CollectorRegistry.defaultRegistry.metricFamilySamples()`, so every stale getter is
   executed during a scrape.

Suggested fix:
Add an unregister mechanism to `SimpleGauge.addRawGetter(...)`, such as returning a handle that removes that exact getter from
the `CopyOnWriteArrayList`. Store the handle in each lifecycle-owned component and call it from `cleanUp()` or `stop()`.
Alternatively, make registration idempotent by owner and label values, but avoid clearing whole static collectors from one
component because other live instances may share the same collector.

Add regression coverage that constructs and cleans up two `KeyAnalysisHandler` or `RequestHandler` instances in the same JVM,
then verifies a collection has only one sample per metric/label pair and that closed handlers are not called.

---

## Finding 2: Key-analysis metric initialization scans the entire RocksDB key-analysis database during startup

Severity: Medium

Files:
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:104-119`
- `src/main/java/io/velo/persist/index/IndexHandlerPool.java:65-69`
- `src/main/java/io/velo/persist/LocalPersist.java:423-425`
- `src/main/java/io/velo/MultiWorkerServer.java:1071-1074`

Code excerpts:

```java
private void createDB() throws RocksDBException {
    this.db = RocksDB.open(openOptions(persistConfig), keysDir.getAbsolutePath());
    log.warn("Key analysis db created, keysDir={}", keysDir.getAbsolutePath());

    this.addCount = db.getLongProperty("rocksdb.estimate-num-keys");

    long total = 0;
    var iter = db.newIterator();
    iter.seekToFirst();
    while (iter.isValid()) {
        var valueBytes = iter.value();
        total += ByteBuffer.wrap(valueBytes).getInt() >> 8;
        iter.next();
    }
    this.addValueLengthTotal = total;
}
```

```java
this.keyAnalysisHandler = new KeyAnalysisHandler(keysDir, workerEventloopArray[0], persistConfig);
```

```java
public void startIndexHandlerPool() throws IOException {
    this.indexHandlerPool = new IndexHandlerPool(ConfForGlobal.indexWorkers, persistDir, persistConfig);
    this.indexHandlerPool.start();
}
```

```java
localPersist.startIndexHandlerPool();

socketInspector.initByNetWorkerEventloopArray(slotWorkerEventloopArray, netWorkerEventloopArray);
```

Root cause:
`createDB()` rebuilds `addValueLengthTotal` by iterating every key-analysis record synchronously immediately after opening
RocksDB. This aggregate only supports the derived `key_analysis_add_value_length_avg` metric, but the scan runs on the
server startup path before the socket inspector and request handling are fully initialized.

The design allows key analysis to contain a large sampled key set. A startup path that reads every analyzed key just to
initialize a metric aggregate changes the startup cost from opening the DB to O(number of analyzed keys).

Impact:

1. A production server with a large persisted key-analysis DB can spend a long time in startup before it accepts traffic.
2. `clearAllKeysAfterAnalysis()` also closes and recreates the DB, so the same synchronous rebuild can create a pause during
   key-analysis reset.
3. Metrics correctness for an average value length can delay service availability, even though the value could be rebuilt
   lazily or persisted as metadata.

Suggested fix:
Remove `addValueLengthTotal` and the derived `key_analysis_add_value_length_avg` metric. The aggregate is only used for this
average and does not justify a full RocksDB scan during startup. After removing the metric, `createDB()` should only open the
database and initialize `addCount` from `rocksdb.estimate-num-keys`.

Add a regression or benchmark-style test that opens a populated key-analysis DB and asserts that construction does not perform
a full iterator pass on the startup thread.

---

## Finding 3: Key-analysis RocksDB iterators are not closed

Severity: Low

Files:
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:110-118`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:258-272`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:276-311`
- `src/main/java/io/velo/persist/index/KeyAnalysisHandler.java:314-334`
- `src/main/java/io/velo/persist/index/KeyAnalysisTask.java:191-214`

Code excerpts:

```java
var iter = db.newIterator();
iter.seekToFirst();
while (iter.isValid()) {
    var valueBytes = iter.value();
    total += ByteBuffer.wrap(valueBytes).getInt() >> 8;
    iter.next();
}
```

```java
var iterator = db.newIterator();
seekIterator(beginKeyBytes, iterator, isIncludeBeginKey);

int count = 0;
while (iterator.isValid() && count < batchSize) {
    ...
    iterator.next();
    count++;
}
```

```java
var iterator = db.newIterator();
iterator.seek(Wal.keyBytes(prefix));

while (iterator.isValid() && result.size() < maxCount) {
    ...
    iterator.next();
}
```

```java
var iterator = db.newIterator();
...
return iterateAndDoAnalysis(iterator, null);
```

Root cause:
RocksDB iterators are native resources and should be closed after use. The key-analysis monitoring and query paths create
iterators for startup metric reconstruction, key listing, prefix matching, and periodic prefix analysis, but none of the
shown paths close the iterator in a `try`/`finally` or try-with-resources block.

Impact:

1. Repeated management queries such as key iteration, filtering, and prefix matching can leak native RocksDB iterator
   resources.
2. The periodic key-analysis task creates a new iterator each time it runs, so long-running servers can accumulate native
   memory pressure even without explicit management queries.
3. The startup metric scan from Finding 2 also leaks an iterator on every DB open or recreate.

Suggested fix:
Wrap every `db.newIterator()` usage in `try`/`finally` and call `iterator.close()` after the scan completes. Add regression
coverage with a fake or wrapper iterator where possible, or at minimum add focused tests around the methods while inspecting
resource closure through a test seam.

## AI Agent 2 Review Notes

Reviewer: AI agent 2
Date: 2026-05-22
Branch: `review/metrics_monitoring`

### Finding 1 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `SimpleGauge.rawGetterList` is a `CopyOnWriteArrayList` (instance field). `addRawGetter()` appends; the only removal
   method is `clearRawGetterList()` which removes all getters indiscriminately. There is no per-getter unregister mechanism.

2. `KeyAnalysisHandler` registers a lambda on the `static final keyAnalysisGauge` in `initMetricsCollect()`. `cleanUp()` closes
   the RocksDB but does not remove the getter. After cleanup, `allKeyCount()` would throw on the closed DB.

3. `RequestHandler` registers a lambda on the `static final requestHandlerGauge`. `stop()` does not remove it.

4. `CompressStats` registers a lambda on the `static final compressStatsGauge` in its constructor. The class has no cleanup
   method at all — once created, the instance is permanently pinned.

**Nuance:** The `rawGetterList` is technically an instance field on `SimpleGauge`, but since every gauge in question is itself
a `static final` field, the practical effect is global accumulation.

**Verdict:** Confirmed. Stopped components leave raw getters on static collectors, causing duplicate samples, memory leaks,
and potential exceptions on closed resources.

### Finding 2 — CONFIRMED (Severity: Medium)

**Verification performed:**

1. `createDB()` at lines 104-119 iterates every key in the RocksDB synchronously to compute `addValueLengthTotal`. This runs
   in the constructor (line 127), which is on the startup path.

2. The scan is synchronous, unbounded, and blocking. For an empty DB it is trivial, but for a large persisted key-analysis DB
   (up to 100 million keys as noted in code comments), this can cause significant startup latency.

3. `clearAllKeysAfterAnalysis()` also calls `createDB()`, so the same pause occurs during periodic key-analysis resets.

**Verdict:** Confirmed. The synchronous full scan on the startup path makes startup latency proportional to key-analysis
cardinality.

**Owner clarification:** `addValueLengthTotal` should be removed rather than persisted or rebuilt. Since it only exists to
publish `key_analysis_add_value_length_avg`, the preferred fix is to drop both the aggregate and that average metric, leaving
startup independent of a full key-analysis DB scan.

### Finding 3 — CONFIRMED (Severity: Low)

**Verification performed:**

1. `RocksIterator` extends `NativeReference` and implements `AutoCloseable`. It must be closed to release native memory.

2. In `KeyAnalysisHandler.java`, 4 iterators are created and never closed:
   - `createDB()` line 111
   - `iterateKeys()` line 260
   - `filterKeys()` line 281
   - `prefixMatch()` line 317

3. In `KeyAnalysisTask.java`, 1 iterator is created and never closed:
   - `doMyTask()` line 199 — runs every 10 seconds when not busy, causing recurring native memory leaks.

4. None use `try-with-resources` or `try/finally`.

**Verdict:** Confirmed. All `db.newIterator()` calls leak native RocksDB iterator resources.

### Review Summary

| # | AI agent 2 status | Severity | Notes |
|---|-------------------|----------|-------|
| 1 | Confirmed | Medium | Static raw getters accumulate across component lifecycles; no per-getter removal. |
| 2 | Confirmed | Medium | Synchronous full RocksDB scan in `createDB()` on startup path. |
| 3 | Confirmed | Low | 5 unclosed `RocksIterator` instances across key-analysis code. |
