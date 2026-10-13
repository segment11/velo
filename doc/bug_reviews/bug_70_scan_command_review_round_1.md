# Bug 70 — SCAN Command Review (Round 1)

**Module:** `scan` command path — `io.velo.command.SGroup` (`scan`, `sscan`) plus its persist-layer
helpers `io.velo.persist.ScanCursor`, `io.velo.persist.KeyLoader` (`scan`, `readKeysToList`), and
`io.velo.persist.Wal` (`scan`, `inWalKeysFormScan`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** reviewed — all 8 findings verified (7 confirmed as reported, 1 confirmed with refinements: 70.5); none fixed yet
**Review branch:** `review/scan`
**Review outcome:** all findings confirmed — ready for the fix phase, see [Review Feedback (AI agent 2)](#review-feedback-ai-agent-2-round-1) for per-finding verdicts and the recommended fix order

## Scope

Files read for this round:

- `src/main/java/io/velo/command/SGroup.java` — `parseSlots()` (lines 301-357), `scan()` (547-699),
  `scanResultToReply()` (519-543), `sscan()` (2134-2222)
- `src/main/java/io/velo/persist/ScanCursor.java` — whole file (bit packing / sentinel handling)
- `src/main/java/io/velo/persist/KeyLoader.java` — `isKeyMatch()` (432-452), `readKeysToList()`
  (466-580), `scan()` (598-648), `scanNextSlotCursor()` (650-665), `readBatchInOneWalGroup()`
  (843-849), `MAX_SPLIT_NUMBER` (201)
- `src/main/java/io/velo/persist/Wal.java` — delay maps (344-345), `inWalKeysFormScan()` (371-386),
  `scan()` (396-462), `scanCursorForNextWalGroup()` (464-473), `calcWalGroupNumber()` (336-338),
  WAL put/persist trigger (909-951), delay-map mutation sites (570-572, 751-757, 935-936)
- `src/main/java/io/velo/persist/FdReadWrite.java` — `readInnerByBuffer()` (565-646),
  `readKeyBucketsSharedBytesInOneWalGroup()` (776-778) — shared buffer + `RandomAccessFile` usage
- `src/main/java/io/velo/MultiWorkerServer.java` — request dispatch (575-608), snowflake provider
  (932-941)
- `src/main/java/io/velo/RequestHandler.java` — `parseSlots()` (190-236)
- `src/main/java/io/velo/decode/Request.java` — `getSingleSlot()` / `SLOT_CAN_HANDLE_BY_ANY_WORKER`
  (202-219)
- `src/main/java/io/velo/persist/OneSlot.java` — thread affinity helpers `asyncRun`/`asyncExecute`/
  `asyncCall` (659-734), `threadIdProtectedForSafe` (1938, 1602-1603), WAL init (208-245)
- `src/main/java/io/velo/persist/LocalPersist.java` — `oneSlot` (106-108), `nextOneSlot` /
  `lastOneSlot` (670-699), slot-thread binding (217, 321, 631)
- `src/main/java/io/velo/ConfForSlot.java` — `ConfBucket` (240-310), `ConfWal` (446-516):
  defaults `bucketsPerSlot=4096`, `oneChargeBucketNumber=32`, `onceScanMaxLoopCount=1024`,
  `valueSizeTrigger=200`, `shortValueSizeTrigger=200`
- `src/main/java/io/velo/persist/KeyBucket.java` — `INIT_CAPACITY = 48` (25)
- `src/main/java/io/velo/SnowFlake.java` — id layout: timestamp(41) | datacenter(5) | machine(5) | seq(12)
- Tests for coverage context: `src/test/groovy/io/velo/command/SGroupTest.groovy` ('test scan',
  176-410), `src/test/groovy/io/velo/command/ScanCursorTest.groovy`,
  `src/test/groovy/io/velo/persist/WalTest.groovy` (scan cases, 564-632),
  `src/test/groovy/io/velo/persist/KeyLoaderTest.groovy` (scan cases, 593-700, 830),
  `src/test/groovy/io/velo/tools/TestLoopScan.groovy`

Reference for Redis parity: Redis 7.x `scanGenericCommand` (db.c) — MATCH uses full glob
(`stringmatchlen`), COUNT must be a positive integer, cursors are stateless across connections,
and the iteration guarantee is "keys present from start to end are returned at least once".

Not re-reported here (other docs): generic FdReadWrite/LRU thread-safety outside the scan path,
cluster migration, replication.

---

## Finding 1 (Bug 70.1): WAL-phase-end cursor leaks the *last* WAL group index; the resumed call then starts the key-bucket phase at the last WAL group and silently skips key buckets of all earlier groups

**Severity:** High
**Class:** Correctness — full SCAN iteration misses most persisted keys
**Status:** ✅ CONFIRMED (reproduced, see Review Feedback) — not fixed

### Cited files / lines

- `src/main/java/io/velo/persist/Wal.java:464-472` — end-of-WAL cursor carries the last group's index:
  ```java
  private KeyLoader.@NotNull ScanCursorWithReturnKeys scanCursorForNextWalGroup(ArrayList<String> keys) {
      var isLastWalGroup = groupIndex == calcWalGroupNumber() - 1;
      ScanCursor scanCursor;
      if (isLastWalGroup) {
          scanCursor = new ScanCursor(slot, groupIndex, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) 0);
      } else {
          scanCursor = new ScanCursor(slot, groupIndex + 1, (short) 0, (short) 0, (byte) 0);
      }
      ...
  ```
- `src/main/java/io/velo/command/SGroup.java:662-698` — the WAL loop and the key-bucket resume:
  ```java
  while (!lastScanCursor.isWalIterateEnd()) {
      ...
      if (keys.size() == count) {
          return scanResultToReply(lastScanCursor, keys, veloUserData);   // line 670-671
      }
      ...
  }
  ...
  } else {
      rKeyBuckets = keyLoader.scan(lastScanCursor.walGroupIndex(), lastScanCursor.splitIndex(), lastScanCursor.keyBucketsSkipCount(),
              typeAsByte, matchPattern, leftCount, beginScanSeq);          // line 693-694
  }
  ```
- `src/main/java/io/velo/persist/KeyLoader.java:616` — `for (int j = walGroupIndex; j < walGroupNumber; j++)`

### Root cause

When the WAL phase finishes on the **last** WAL group, `Wal.scanCursorForNextWalGroup` returns
`ScanCursor(slot, walGroupNumber-1, ONE_WAL_SKIP_COUNT_ITERATE_END, 0, 0)`. The `walSkipCount=1023`
sentinel means "WAL done, key buckets not started yet", but the cursor's `walGroupIndex` field still
holds the **last** group index (e.g. 127 with the default `bucketsPerSlot=4096` /
`oneChargeBucketNumber=32`, i.e. `calcWalGroupNumber() = 128`).

If this cursor is handed back to the client — which happens exactly when
`keys.size() == count` at `SGroup.java:670` — the next `SCAN <cursor>` call takes the
`isScanWalFirst == false` branch at `SGroup.java:692-695` and calls
`keyLoader.scan(lastScanCursor.walGroupIndex(), ...)` with `walGroupIndex = 127`. The loop at
`KeyLoader.java:616` starts at group 127, scans only that group's key buckets, then
`scanNextSlotCursor` advances to the next slot / END. Key buckets of WAL groups 0..126 are never
visited in this scan iteration.

The correct path exists for the other case: when the WAL ends with `keys.size() < count`, the
same call continues to key buckets from `(0, 0, 0)` (`SGroup.java:689-691`).

### Trigger / impact

Trigger: a SCAN call where cumulative WAL-phase keys reach exactly `COUNT` at the moment the last
WAL group's iteration completes (`nextTimeSkipCount >= allValues.size()`, `Wal.java:455-460`).
Concrete scenario with defaults (`COUNT 10`): dataset of any size persisted in key buckets, plus
10 recent writes sitting in the last WAL group. `SCAN 0 COUNT 10` returns those 10 WAL keys with
the end-of-WAL cursor; the next call scans only group 127's key buckets and then reports the slot
done (or jumps to the next slot). The client's full iteration silently omits essentially all keys
stored in key buckets of groups 0..126 (127/128 of the keyspace for that slot). Violates the Redis
SCAN guarantee ("keys present the whole iteration are returned at least once").

Existing tests miss this because they use `COUNT` values that never coincide with the WAL-exhaustion
return (`SGroupTest.groovy:176-410`).

### Suggested fix direction

Make the end-of-WAL cursor unambiguous: either encode `walGroupIndex = 0` in the sentinel cursor
returned for the last group, or in `SGroup.scan` detect "cursor arrived with
`isWalIterateEnd() && !isScanWalFirst` from a WAL-phase hand-off" and start key buckets at
`(0, 0, 0)`. A regression test should assert a full iteration returns all persisted keys when the
WAL phase ends exactly at `COUNT`.

---

## Finding 2 (Bug 70.2): `readKeysToList` re-applies the skip count to *every* physical bucket and encodes the resume position relative to a single bucket — resumed scans skip never-examined entries (missed keys) and re-examine earlier buckets (duplicates)

**Severity:** High
**Class:** Correctness — SCAN misses/duplicates keys inside one WAL group
**Status:** ✅ CONFIRMED (reproduced, see Review Feedback) — not fixed

### Cited files / lines

- `src/main/java/io/velo/persist/KeyLoader.java:492-524` — counters are reset per bucket,
  including the skip position:
  ```java
  for (int i = 0; i < ConfForSlot.global.confWal.oneChargeBucketNumber; i++) {   // 32 buckets per group
      ...
      final short[] addedKeyCount = {0};
      final short[] tmpSkipCount = {skipCount};      // reset to the FULL skipCount for every bucket
      final short[] expiredOrNotMatchedCount = {0};
      final short[] postScanStartSkippedCount = {0};
      ...
      keyBucket.iterate((keyHash, expireAt, seq, key, valueBytes) -> {
          if (tmpSkipCount[0] > 0) { tmpSkipCount[0]--; return; }
          ...
  ```
- `src/main/java/io/velo/persist/KeyLoader.java:574-577` — the returned cursor position only counts
  the bucket where `count` was satisfied:
  ```java
  if (countArray[0] <= 0) {
      var nextTimeSkipCount = skipCount + expiredOrNotMatchedCount[0] + postScanStartSkippedCount[0] + addedKeyCount[0];
      return new ScanCursor(slot, walGroupIndex, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) nextTimeSkipCount, splitIndex);
  }
  ```
- `src/main/java/io/velo/command/SGroup.java:693-694` and
  `src/main/java/io/velo/persist/KeyLoader.java:623-625` — the cursor's `keyBucketsSkipCount` is fed
  back as `skipCount` on resume.

### Root cause

A WAL group spans `oneChargeBucketNumber` (default 32) physical key buckets, each holding up to
`KeyBucket.INIT_CAPACITY = 48` keys. When `count` is satisfied in bucket `b > 0` (normal whenever
bucket 0 alone cannot fill `count` — e.g. with `MATCH`/`TYPE` filtering, sparse buckets, or
`COUNT <` eligible entries in bucket 0):

- the returned cursor's `keyBucketsSkipCount` is the position **within bucket `b`**
  (`skipCount + bucket-b-only counters`), losing the fact that buckets `0..b-1` were fully examined
  and buckets `b+1..31` were never touched;
- on the next call, `tmpSkipCount` is re-initialized to the full `skipCount` for **each** of the 32
  buckets, so buckets `b+1..31` — never examined — each skip `skipCount` entries: up to
  `skipCount` never-examined entries per bucket are silently dropped (**missed keys**), while
  buckets `0..b-1` get re-examined past the skip point (**duplicate keys**).

Production keys are spread over the 32 buckets by key hash, so multi-bucket resumes are the norm
whenever `COUNT` is not filled by the first physical bucket of a group.

Existing tests do not catch this because `Mock.prepareShortValueList(n, bucketIndex = 0)` forces
all test keys into **one** bucket (`src/test/groovy/io/velo/persist/Mock.groovy:19-31`), so the
per-bucket reset never matters and `keyBucketsSkipCount == keys returned` always holds
(`SGroupTest.groovy:258, 272`).

### Impact

`SCAN ... MATCH user:* COUNT 10` style iterations can skip entire buckets (48-key capacity) per
group per call — pre-existing keys are permanently omitted from that full iteration — and can
re-return keys already returned. Both violate / strain the Redis SCAN contract (misses are a hard
violation).

### Suggested fix direction (with a constraint)

The skip must be consumed **once** across the bucket sequence (carry the remainder across
buckets), and the cursor must encode a position relative to the whole `(walGroup, splitIndex)`
span. Constraint: one span can hold `32 * 48 = 1536` entries > 1023, so it does **not** fit in the
current 10-bit `keyBucketsSkipCount` (`ScanCursor.toLong()` asserts `< 1024` and packs 10 bits,
`ScanCursor.java:38-41`). The cursor layout needs a bucket-relative encoding (e.g. bucket ordinal
inside the group + in-bucket skip) rather than a flat skip count.

---

## Finding 3 (Bug 70.3): SCAN is dispatched to a *random* worker thread and reads slot state (WAL `HashMap`s, shared read buffers, `RandomAccessFile`) without synchronization, racing the slot-owner thread

**Severity:** High
**Class:** Concurrency — data race / `ConcurrentModificationException` / corrupted reads
**Status:** ✅ CONFIRMED by inspection (not stress-reproduced) — not fixed

### Cited files / lines

- `src/main/java/io/velo/command/SGroup.java:301-357` — `parseSlots()` does not list `"scan"`,
  so the slot list is empty.
- `src/main/java/io/velo/decode/Request.java:213-215` — empty slot list ⇒ `SLOT_CAN_HANDLE_BY_ANY_WORKER`.
- `src/main/java/io/velo/MultiWorkerServer.java:597-605` — random dispatch:
  ```java
  if (firstSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER) {
      i = new Random().nextInt(requestHandlerArray.length);
      targetHandler = requestHandlerArray[i];
  } else {
      i = firstSlot % requestHandlerArray.length;
  ```
- `src/main/java/io/velo/persist/Wal.java:344-345` — plain `HashMap`s:
  ```java
  HashMap<String, V> delayToKeyBucketValues;
  HashMap<String, V> delayToKeyBucketShortValues;
  ```
  mutated on the slot-owner thread (put/remove: `Wal.java:570-572, 751-757, 935-936`) with no
  synchronization anywhere in the file.
- `src/main/java/io/velo/persist/Wal.java:371-386` — `inWalKeysFormScan` iterates both maps
  (`entrySet()` loop) on the scan thread.
- `src/main/java/io/velo/persist/Wal.java:410-412` — `Wal.scan` copies under iteration:
  ```java
  var allValues = new HashMap<String, V>();
  allValues.putAll(delayToKeyBucketShortValues);
  allValues.putAll(delayToKeyBucketValues);
  ```
- `src/main/java/io/velo/persist/FdReadWrite.java:776-778` → `565-646` — batch bucket reads use a
  **shared** `ByteBuffer` (`forOneWalGroupBatchBuffer`, cleared at line 620) and a single
  `RandomAccessFile` with non-atomic `raf.seek(offset); raf.getChannel().read(buffer)` (625-626).
- Design-intent counter-evidence: `OneSlot.asyncRun/asyncExecute/asyncCall`
  (`OneSlot.java:659-734`) and `threadIdProtectedForSafe` (`OneSlot.java:1602-1603`) exist to keep
  per-slot work on the owning event-loop thread; `BaseCommand` is annotated `@ThreadNeedLocal`
  (`BaseCommand.java:35`).

### Root cause

Normal writes to a slot are dispatched to a deterministic worker (`firstSlot % handlerCount`), but
`scan` (empty `parseSlots` result) lands on a **random** worker. The scan handler then directly
calls `oneSlot.getWalByGroupIndex(...).scan(...)`, `inWalKeysFormScan(...)`,
`keyLoader.scan(...)` on that thread — bypassing `asyncCall`/`asyncRun` — while the slot's owner
thread concurrently mutates the WAL delay maps, runs `putBatch` writes through the same
`FdReadWrite` (same `RandomAccessFile`, same per-purpose buffers), and updates
`statKeyCountInBuckets` / `metaKeyBucketSplitNumber`. A multi-slot scan additionally touches
*other* slots' `OneSlot` objects, which are by construction owned by different threads.

### Impact

- `ConcurrentModificationException` from `putAll`/`entrySet` iteration of the delay maps while the
  owner thread puts/removes (WAL flush removes happen at `valueSizeTrigger=200`, i.e. routinely).
- Torn / corrupted bucket reads: interleaved `seek`+`read` on the shared `RandomAccessFile` and the
  shared `forOneWalGroupBatchBuffer` (cleared concurrently) can read the wrong offset window,
  producing garbage keys or `RuntimeException("Read error, n=...")`.
- Races with `writeSharedBytesList` during `putBatch` → scan sees half-written bucket bytes.

### Suggested fix direction

**Dispatch scan to the cursor slot's owning worker.** The client-supplied cursor already encodes
the slot (`ScanCursor.slot()`, 16 bits of the packed long), and the existing dispatch machinery can
consume it:

1. In `SGroup.parseSlots` add a `"scan"` branch: when `data.length >= 2`, parse `data[1]` as a
   long (on `NumberFormatException` return the empty list — `scan()` already replies
   `NOT_INTEGER` without touching slot state), decode `ScanCursor.fromLong(cursorLong).slot()`
   (cursor `0` decodes to slot 0, which is exactly where a fresh scan starts), and add
   `slotWithKeyHashList.add(new SlotWithKeyHash(slot, 0, 0L))`.
2. `Request.getSingleSlot()` then returns that slot and `MultiWorkerServer.handleRequest` routes
   via `firstSlot % requestHandlerArray.length` (`MultiWorkerServer.java:603`) — the same modulo
   used to bind slots to workers (`i = oneSlot.slot() % slotWorkerThreadIds.length`,
   `LocalPersist.java:216`; the two arrays are parallel per-worker arrays populated together in
   `initSlotRuntimeBindings`, `LocalPersist.java:246-255`), so the request lands on the slot's
   owning thread.
3. ACL caveat: `U.checkCmdAndKey` iterates the slot list and calls
   `rKey.match(slotWithKeyHash.rawKey(), ...)` for every non-sentinel entry (`U.java:617-636`).
   The existing `SlotWithKeyHash.TO_FIX_FIRST_SLOT` sentinel (`BaseCommand.java:579`,
   rawKey `null`) is skipped by reference equality — but it is hardcoded to slot 0, so it only fits
   the cursor-0 call. The fix must extend `U.java` to skip entries with `rawKey == null` generally
   (same spirit as the comment "skip check, just use to fix event loop", `U.java:610-613`), or the
   added entries will be key-checked against a null key.

Why per-slot dispatch is sufficient (no `asyncCall` hop needed): one `scan` call only touches the
single `OneSlot` referenced by the cursor — `SGroup.java:643-698` (`oneSlot(scanCursor.slot())`,
its WAL loop, and its `keyLoader.scan`). The slot transition in
`KeyLoader.scanNextSlotCursor` (`KeyLoader.java:650-665`) merely computes the next cursor from the
`LocalPersist` slot registry (`lastOneSlot`/`nextOneSlot` — array/multiShard reads, no other
slot's data structures), so the *next* request (cursor carrying the new slot) will be dispatched to
that slot's worker by the same rule. Cross-slot hand-off is therefore per-request, and each
request's data access is confined to its owning thread.

Longer term (independent of dispatch): give the scan read path snapshot-friendly access
(copy-on-write or per-read locking for the WAL delay maps, dedicated read buffers / synchronized
fd access for batch bucket reads) if scan ever needs to run off the owner thread.

### Round 1 addendum — re-check after review feedback (dispatch-by-cursor-slot)

Reviewer question: "the scan cursor long contains the slot index — the request handler can read it
and dispatch to the target slot worker; check again."

Re-verified against current code — the finding stands, because **no code reads the cursor before
dispatch**:

- `handlePipeline` → `requestHandlerArray[0].parseSlots(request)` (`MultiWorkerServer.java:703-705`)
  → `RequestHandler.parseSlots` (`RequestHandler.java:190-236`) → `SGroup.parseSlots`
  (`SGroup.java:301-357`): the method has branches for `sadd`/`scard`/`set`/... /`sscan` (single-key
  list), `sdiff`-family, `sintercard`, `smove` — **no `"scan"` branch**, so the list is empty.
- `Request.getSingleSlot()` (`Request.java:213-215`) returns `SLOT_CAN_HANDLE_BY_ANY_WORKER` (-1)
  for the empty list.
- `MultiWorkerServer.handleRequest` (`MultiWorkerServer.java:597-605`) then picks
  `new Random().nextInt(requestHandlerArray.length)` — a random worker, not the cursor's slot
  worker. (`MultiWorkerServer.java:489` uses `getSingleSlot()` for slot routing only in
  `handleReplRequest`, which is replication traffic, not client `scan`.)
- Grep over `src/main/java` for `"scan"` confirms the only occurrences are the command registry
  (`SGroup.java:50`), the handler dispatch (`SGroup.java:380`), and ACL category lists — nothing in
  the parse/dispatch path parses `data[1]`.

So the cursor-slot dispatch described above is the (correct, available) fix, not the current
behavior. The rest of Finding 3 (unsynchronized `HashMap` iteration, shared
`forOneWalGroupBatchBuffer`, non-atomic `seek`+`read`) is unchanged by this observation.

---

## Finding 4 (Bug 70.4): `isKeyMatch` implements only single-`*` prefix/suffix matching — Redis glob patterns (`a*b`, `?`, `[...]`, escapes) silently match nothing

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** ✅ CONFIRMED (reproduced, see Review Feedback) — not fixed

### Cited files / lines

- `src/main/java/io/velo/persist/KeyLoader.java:432-452`:
  ```java
  public static boolean isKeyMatch(@NotNull String key, @Nullable String matchPattern) {
      if (matchPattern == null) return true;
      if ("*".equals(matchPattern)) return true;
      if (matchPattern.endsWith("*")) {
          if (matchPattern.startsWith("*")) {
              return key.contains(matchPattern.substring(1, matchPattern.length() - 1));
          }
          return key.startsWith(matchPattern.substring(0, matchPattern.length() - 1));
      } else if (matchPattern.startsWith("*")) {
          return key.endsWith(matchPattern.substring(1));
      } else {
          return key.equals(matchPattern);
      }
  }
  ```
- Consumers: `Wal.java:428`, `KeyLoader.java:533`, `SGroup.java:2195` (sscan), and the same helper
  is used by `hscan`/`zscan`.

### Root cause and impact

Redis `MATCH` uses full glob (`stringmatchlen`): `*`, `?`, `[...]`, `\`-escapes. Here:

- `MATCH user:*:cart` (interior `*`) does not end with `*` → falls to `key.equals(...)` → **no keys
  ever match**; the iteration appears to complete immediately with empty batches.
- `MATCH h?llo`, `MATCH [a-c]x`, `MATCH user:\**` similarly degrade to exact-equality.
- `MATCH *a*b*` is treated as `contains("a*b")` (literal inner `*`) instead of glob semantics.

Clients that use these common patterns get silent empty results instead of an error, which is
worse than failing loudly.

### Suggested fix direction

Port Redis `stringmatchlen` semantics (a small recursive matcher, no regex needed); keep the fast
paths for `null`/`"*"`.

---

## Finding 5 (Bug 70.5): `Wal.scan` uses a positional cursor over a mutable `HashMap`; iteration-order changes between calls shift the skip position, so pre-existing keys can be skipped

**Severity:** Medium
**Class:** Correctness under mutation — SCAN guarantee violation
**Status:** ✅ CONFIRMED with refinements (severity lowered to Low; see Review Feedback) — not fixed

### Cited files / lines

- `src/main/java/io/velo/persist/Wal.java:396-462` — the whole scan is positional:
  ```java
  for (var entry : allValues.entrySet()) {
      if (tmpSkipCount > 0) { tmpSkipCount--; continue; }   // skip by iteration position
      ...
  }
  var nextTimeSkipCount = skipCount + removedOrExpiredOrNotMatchedCount + otherSkippedCount + keys.size();
  var isIteratedAll = nextTimeSkipCount >= allValues.size();
  ```

### Root cause and impact

Between two SCAN calls, the slot thread may put new keys into the delay maps (they are filtered by
`v.seq > beginScanSeq`, but they still consume positions and can rehash the map) or remove keys
(WAL flush to key buckets, `Wal.java:570-572/751-757`). A rehash or removal reorders the remaining
entries, so the recorded `skipCount` no longer points at the same logical entry: entries that were
already returned may be returned again (tolerable), and entries never returned can move *before*
the resume position and are then skipped (**missed**, violates the SCAN guarantee even after
Finding 3's thread fix). Key buckets have the analogous positional exposure when buckets split
during a scan (entries move across `splitIndex`), though bucket contents are far more stable than
the WAL maps.

### Suggested fix direction

Iterate a stable ordering (e.g. sort the snapshot keys, or iterate by bucket-hash order), or switch
the WAL cursor to a content-addressed marker (last-returned key under the stable order).

---

## Finding 6 (Bug 70.6): `COUNT 0` is accepted and makes `Wal.scan` return one key per WAL group with a negative remaining budget

**Severity:** Low
**Class:** Redis protocol compatibility / edge-case behavior
**Status:** ✅ CONFIRMED (reproduced, see Review Feedback) — not fixed

### Cited files / lines

- `src/main/java/io/velo/command/SGroup.java:585-597` — only `count < 0` is rejected:
  ```java
  count = Integer.parseInt(new String(data[i + 1]));
  ...
  if (count < 0) {
      return ErrorReply.INVALID_INTEGER;
  }
  ```
- `src/main/java/io/velo/persist/Wal.java:448-451` — the add happens before the size check:
  ```java
  keys.add(key);
  if (keys.size() >= count) {
      break;
  }
  ```
  With `count == 0` the first eligible key is added anyway (size becomes 1 ≥ 0).

### Root cause and impact

Redis rejects `COUNT 0` ("COUNT should be greater than 0"). Velo accepts it; with `count = 0` each
WAL group's scan adds one key, `leftCount` goes negative (`SGroup.java:673`), and the call can
return up to `walGroupNumber` (32) keys despite `COUNT 0`. The key-bucket phase behaves
differently (`countArray[0] <= 0` adds nothing), so the result is inconsistent between phases.

### Suggested fix direction

Reject `count < 1` with Redis's "COUNT should be greater than 0" style error (align `sscan`'s
`count <= 0` check too, which currently returns generic `SYNTAX`).

---

## Finding 7 (Bug 70.7): `beginScanSeq` is minted from slot 0's `SnowFlake` but compared against seqs minted by every slot's independent `SnowFlake` sharing the same worker bits — same-millisecond ordering inversions can hide pre-existing keys

**Severity:** Low
**Class:** Correctness edge case (multi-slot instances)
**Status:** ✅ CONFIRMED (code + cross-slot SnowFlake construction) — not fixed

### Cited files / lines

- `src/main/java/io/velo/command/SGroup.java:643-651`:
  ```java
  var oneSlot = localPersist.oneSlot(scanCursor.slot());   // cursor 0 ⇒ slot 0
  long beginScanSeq;
  if (cursorLong == 0) {
      beginScanSeq = oneSlot.getSnowFlake().nextId();
  ```
- `src/main/java/io/velo/MultiWorkerServer.java:932-941` — every slot gets
  `new SnowFlake(ConfForGlobal.datacenterId, ConfForGlobal.machineId)` (identical worker bits).
- `src/main/java/io/velo/SnowFlake.java:19-59` — layout `timestamp(41) | datacenter(5) | machine(5) | seq(12)`.
- Filter sites comparing cross-instance ids: `Wal.java:443`, `Wal.java:375`,
  `KeyLoader.java:560`.

### Root cause and impact

Sequence numbers are only ordered *within* one `SnowFlake` instance. All slots' flakes share the
same datacenter/machine bits and private 12-bit sequences, so for keys written in the **same
millisecond** the scan snapshot begins, a slot-N key written *before* `beginScanSeq` can still carry
`seq > beginScanSeq` (slot-N's sequence counter ahead of slot-0's) and is then skipped by the
`seq > beginScanSeq` filters — a pre-existing key missed. The window is one millisecond, so impact
is low, but the comparison is not principled. (Single-slot deployments are unaffected.)

### Suggested fix direction

Mint `beginScanSeq` from a clock value that is comparable across slots (e.g.
`START_STAMP + System.currentTimeMillis() << TIMESTAMP_LEFT` style epoch), or take
`max(snowflake epoch, beginScanSeq minus 1ms safety margin)`.

---

## Finding 8 (Bug 70.8): SCAN cursor is stateful per connection (`CURSOR_NOT_MATCH`) — cross-connection resume and interleaved scans fail, unlike Redis stateless cursors

**Severity:** Low (documented deviation)
**Class:** Redis protocol compatibility / client behavior
**Status:** ✅ CONFIRMED (design trade-off, keep documented) — not fixed

### Cited files / lines

- `src/main/java/io/velo/command/SGroup.java:545-569`:
  ```java
  private final ErrorReply CURSOR_NOT_MATCH = new ErrorReply("cursor is not match");
  ...
  if (cursorLong != veloUserData.getLastScanAssignCursor()) {
      return CURSOR_NOT_MATCH;
  }
  ```
- `src/main/java/io/velo/command/SGroup.java:519-526` — `lastScanAssignCursor` /
  `beginScanSeq` stored on `VeloUserDataInSocket`.

### Root cause and impact

Redis cursors are opaque and stateless: a client may resume a scan on another connection or run
several scans concurrently on one connection. Velo binds the cursor to the socket and to strict
sequential use, so (a) reconnecting mid-scan (common with resilient clients) and continuing with
the last cursor errors out; (b) two interleaved scans on one connection error out. Given the
cursor encodes internal positions that are only meaningful with the saved `beginScanSeq`, this is
understandable as a design trade-off, but it should be documented as a known deviation, and
`CURSOR_NOT_MATCH` text does not match Redis error style (`ERR ...`).

### Suggested fix direction

At minimum prefix the error with `ERR`; ideally store `{cursor -> beginScanSeq}` for a small number
of concurrent cursors per connection instead of a single value.

---

## Notes / non-findings

- `ScanCursor` bit packing: `splitIndex` is bounded by `MAX_SPLIT_NUMBER = 9`
  (`KeyLoader.java:201`) so 4 bits suffice; `walSkipCount` is bounded by
  `valueSizeTrigger + shortValueSizeTrigger = 400 < 1023`; `walGroupIndex` (24 bits) is far below
  `calcWalGroupNumber() = 4096 / 32 = 128`. No packing overflow today — but see Finding 2's
  constraint note before extending `keyBucketsSkipCount` semantics.
- `sscan` cursor math (`SGroup.java:2188-2221`) is self-consistent (cursor counts matched members
  only); it inherits the `isKeyMatch` glob gap (Finding 4) and the positional-cursor-under-mutation
  weakness (Finding 5) but is slot-dispatched so it has no Finding 3 race.
- `scanResultToReply` correctly resets `lastScanAssignCursor`/`beginScanSeq` on END
  (`SGroup.java:520-526`); `ScanCursor.END.toLong() == 0` round-trips as a fresh-start cursor by
  design.

## Reviewer checklist (for AI agent 2)

1. Verify Finding 1 by constructing: persisted bucket keys + exactly-COUNT eligible keys in the
   last WAL group; assert the next call's key-bucket phase starts at group 0.
2. Verify Finding 2 with keys hashed into ≥ 2 buckets of one WAL group (do not use
   `Mock.prepareShortValueList`'s fixed `bucketIndex`); assert a full iteration returns each key
   exactly once with `COUNT` smaller than one bucket's eligible yield.
3. Verify Finding 3 by inspection of dispatch (`parseSlots` absence + random worker) and the
   unsynchronized `HashMap`/`RandomAccessFile` usage; optionally a stress test scan vs writes.
4. Findings 4-8 are direct code/parity reads against Redis semantics.

---

## Review Feedback (AI agent 2, round 1)

**Reviewer:** AI agent 2
**Date:** 2026-10-13
**Verdict:** all 8 findings verified — 6 confirmed exactly as described, 1 confirmed with
refinements (70.5, severity lowered), 1 confirmed as a design trade-off to document (70.8).
None are refuted. No production code changed in this round; the temporary verification spec used
to reproduce 70.1 / 70.2 / 70.4 / 70.6 was deleted after collecting evidence (it must be re-created
as real regression tests during the fix phase).

### Method

Re-read every cited line, plus the surrounding call graph
(`SGroup.scan` → `Wal.scan` / `KeyLoader.scan` → `readKeysToList`, `MultiWorkerServer.handle` →
`Request.getSingleSlot`, `LocalPersist`/`OneSlot` thread-affinity helpers). For the deterministic
bugs I built a throwaway Spock spec (`io.velo.command.ScanReviewTmpTest`, single slot, default
`bucketsPerSlot=4096`, `initialSplitNumber=1`, keys written into *distinct* physical key buckets
via `Wal.V(..., bucketIndex, ...)` + `KeyLoader.persistShortValueListBatchInOneWalGroup`) and ran
`./gradlew :test --tests "io.velo.command.ScanReviewTmpTest"`. Outputs quoted below are from that
run.

### Finding 1 (70.1) — CONFIRMED (reproduced)

Code trace confirms the hand-off: `Wal.scanCursorForNextWalGroup()` returns
`(slot, groupIndex, 1023, 0, 0)` for the last group (`Wal.java:464-473`); `SGroup.scan` returns that
cursor at `SGroup.java:670` when `keys.size() == count`; the next call then skips the WAL loop
(`isWalIterateEnd()` true → `isScanWalFirst` stays false) and calls
`keyLoader.scan(walGroupIndex=127, 0, 0, ...)` at `SGroup.java:693-694`.

Reproduction (10 keys persisted in key buckets of WAL group 0, 10 keys in the WAL of group 127,
`COUNT 10`):

```
walGroupNumber=128 lastGroup=127
r1 keys=10 cursor=2147467264
c1 walGroupIndex=127 walIterateEnd=true
r2 keys=[] cursor=0            <-- the 10 persisted bucket keys are never returned
```

So a full iteration returns 10 of 20 keys. Corrections to the original text: with default config
`calcWalGroupNumber()` is **128** (not 32), so the missed share is 127/128 of the key-bucket space;
doc text updated above.

Refinement on the fix direction: encoding `walGroupIndex = 0` in the last-group sentinel is safe and
sufficient. The only other producer of a `walSkipCount == 1023` cursor is `readKeysToList`
(`KeyLoader.java:576`), whose `keyBucketsSkipCount` is always `> 0` when the position is genuinely
inside a bucket (the count can only be exhausted by *adding* a key or by resuming with
`skipCount > 0`), so a decoded `(0, 1023, 0, 0)` cursor unambiguously means "WAL done, key buckets
not started". Still, an explicit sentinel value would be more robust than overloading field values.

### Finding 2 (70.2) — CONFIRMED (reproduced)

`tmpSkipCount` is re-initialized to the *full* `skipCount` for every one of the
`oneChargeBucketNumber` (=32) buckets in the group loop (`KeyLoader.java:517`), and the returned
cursor (`KeyLoader.java:574-577`) records only the position inside the bucket where the count was
satisfied — the bucket ordinal `i` is thrown away (only `splitIndex` is kept).

Reproduction (10 keys in bucket 0 + 10 keys in bucket 1 of WAL group 0, `COUNT 10`):

```
step cursor=0          keys=[10 x key:B0*]  next=16760992
step cursor=16760992  keys=[]              next=0
ALL=100 unique=10      <-- 10 iterations, only the 10 bucket-0 keys, bucket-1 keys never returned
```

Note the extra symptom: because the second call returns no keys *and* cursor 0, a naive client loop
restarts and re-reads the same 10 keys forever — an infinite-loop / livelock risk on top of the
miss. This is worse than the "duplicates" wording in the original text; the primary failure is
**permanent omission**, not duplication (duplicates only appear when the skipped remainder happens
to be re-walked).

Constraint analysis accepted: one `(walGroup, splitIndex)` span holds
`oneChargeBucketNumber * (INIT_CAPACITY << splitNumber)` entries, far above the 1023 limit of the
10-bit `keyBucketsSkipCount` (`ScanCursor.java:38-41`), so a flat skip count cannot be widened
safely. The bucket-ordinal + in-bucket-skip encoding the author suggests is the right shape
(bucket ordinal needs 5 bits for 32 buckets; note the current `splitIndex` field is only 4 bits and
`KeyBucket.MAX_SPLIT_NUMBER = 9`, so the cursor layout has to be re-checked in the fix).

### Finding 3 (70.3) — CONFIRMED (inspection)

Verified end to end: `SGroup.parseSlots` (`SGroup.java:301-358`) has no `"scan"` branch, so the slot
list is empty → `Request.getSingleSlot()` returns `SLOT_CAN_HANDLE_BY_ANY_WORKER`
(`Request.java:213-215`) → `MultiWorkerServer.java:599-601` picks a **random** worker. The scan
body then touches per-slot state directly (`oneSlot.getWalByGroupIndex(..).scan(..)`,
`inWalKeysFormScan`, `keyLoader.scan(..)`) with no `asyncRun`/`asyncCall` hop, while the WAL delay
maps are plain `HashMap`s (`Wal.java:344-345`) mutated by the owning slot thread (`Wal.java:570-572`,
`751-757`, `935-936`), and bucket reads go through the shared `forOneWalGroupBatchBuffer` +
`RandomAccessFile` (`FdReadWrite.java:776-778`, `565-646`).

Refinements:
- With a **single** worker thread (`requestHandlerArray.length == 1`) the random pick always lands on
  the owner, so the race needs ≥ 2 workers; production defaults have many, so the finding stands.
- `scanNextSlotCursor` (`KeyLoader.java:650-665`) advances the cursor into the *next* slot, so a
  multi-slot scan always touches `OneSlot` objects owned by other threads — the race is structural,
  not an edge case.
- Not stress-reproduced here (needs a live server); inspection is conclusive for the unsynchronized
  maps, and a scan-vs-write stress test should be part of the fix verification.

### Finding 4 (70.4) — CONFIRMED (reproduced)

`isKeyMatch` (`KeyLoader.java:432-452`) has exactly the four branches shown. Reproduction with keys
`key:K0000xx` persisted in buckets:

```
'SCAN 0 MATCH key:K*:cart COUNT 10' -> keys=0 cursor=0
```

i.e. an interior `*` degrades to `String.equals` and matches nothing, silently completing the
iteration. Same for `?`, `[...]`, `\`-escapes and for `*a*b*` (treated as `contains("a*b")`).
Confirmed for every consumer (`Wal.java:428`, `KeyLoader.java:533`, `SGroup.sscan`,
`hscan`/`zscan`). Severity Medium agreed; this is a silent-wrong-answer bug for common client
patterns, so it should be fixed relatively early even though it is not memory-unsafe.

### Finding 5 (70.5) — CONFIRMED with refinements (severity: Low, not Medium)

Mechanism confirmed: `Wal.scan` copies the two delay maps into a fresh `HashMap` and walks
`entrySet()` positionally (`Wal.java:410-452`), so the recorded `skipCount` is an *ordinal* into a
snapshot whose order can change between calls (insertions/removals/rehash at `Wal.java:570-572`,
`751-757`, `935-936`). Two refinements to the original text:

1. The `seq > beginScanSeq` filter (`Wal.java:443`) already excludes freshly-inserted keys from the
   *result*, and removals only shift positions forward, so the dominant observable effect is
   **duplicate** returns (which Redis explicitly tolerates during SCAN) rather than omission.
   Omission needs a rehash that moves a never-returned entry *before* the resume position — a much
   narrower window.
2. Redis has comparable exposure during rehash; unlike Redis, Velo's WAL map churns continuously
   (`valueSizeTrigger=200`), so it is worth hardening, but it is not a clean contract violation in
   the same sense as 70.1/70.2.

The author's fix direction is still right, but "sort the snapshot keys" alone is insufficient
(it only stabilizes order, not positions, when the key set changes). A content-addressed marker
(last returned key + stable ordering, e.g. sorted keys compared lexicographically) is the robust
form. Fixing 70.3 first (doing the WAL read on the owning thread) removes most of the churn window
and is the cheaper mitigation.

### Finding 6 (70.6) — CONFIRMED (reproduced, impact description refined)

`count < 0` is rejected (`SGroup.java:594-596`) while `count == 0` is accepted, and `Wal.scan`
adds a key before testing `keys.size() >= count` (`Wal.java:448-451`). Reproduction:

```
COUNT 0, WAL group 0 holds 4 keys            -> keys=4 [key:X000000..X000003] cursor=0
COUNT 0, two non-empty WAL groups            -> keys=8 cursor=0
COUNT 0, first WAL group empty               -> keys=0 cursor=<advanced to next group>
```

So: one key per non-empty WAL group is returned despite `COUNT 0`, `leftCount` goes negative
(`SGroup.java:673`), and when the first group is empty the `keys.size() == count` equality at
`SGroup.java:670` short-circuits and advances exactly one group per call. The key-bucket phase
adds nothing (`KeyLoader.java:565-567`), so the two phases disagree — exactly the inconsistency the
author describes. Redis rejects `COUNT 0`; `sscan` already rejects `count <= 0`
(`SGroup.java:2172-2174`, though with generic `SYNTAX` instead of a COUNT-specific message).
Confirmed, Low, cheap fix.

### Finding 7 (70.7) — CONFIRMED

Verified: `MultiWorkerServer.snowFlakes()` creates one `SnowFlake` per slot, all with identical
`datacenterId`/`machineId` bits (`MultiWorkerServer.java:932-941`); the scan mints
`beginScanSeq` from the cursor's slot only (`SGroup.java:643-651`, slot 0 for cursor 0), and the
value is then compared against seqs produced by every other slot at `Wal.java:375`, `Wal.java:443`
and `KeyLoader.java:560`. Since `SnowFlake` seq bits are private per instance
(`SnowFlake.java:19-59`), cross-slot ids minted in the same millisecond are not ordered, so a key
written *before* the scan started can carry `seq > beginScanSeq` and be filtered out. Confirmed,
Low (1 ms window, single-slot deployments unaffected). The author's fix direction (derive the
compare value from the shared timestamp part of the id, or use a safety margin) is acceptable.

### Finding 8 (70.8) — CONFIRMED (design trade-off, document rather than "fix")

`SGroup.scan` compares the supplied cursor against the single per-socket
`veloUserData.lastScanAssignCursor` and returns `CURSOR_NOT_MATCH` (`SGroup.java:545-569`); the
`beginScanSeq` needed to decode the cursor lives in the same socket-scoped object
(`SGroup.java:645-651`), so stateless cross-connection resume is impossible by construction.
Confirmed. Two notes:
- The error text `"cursor is not match"` is emitted without the `ERR ` prefix Redis clients expect;
  `ErrorReply` does not add one automatically (see `reply/ErrorReply.java`), so Redis-protocol
  clients may mis-parse it. Cheap, safe improvement.
- Do **not** treat "make cursors stateless" as a quick fix: without a server-side `cursor ->
  beginScanSeq` map (with TTL/size bounds) the cursor cannot be decoded correctly at all. Keep as
  documented deviation for now.

### Additional observations from the review (not separate bugs)

- `KeyLoader.java:574` returns a resume cursor whenever `countArray[0] <= 0`, and
  `KeyLoader.java:631` only accepts it when `keys.size() == count`. With `COUNT 0` these two
  interact badly (see 70.6) and should be revisited together with that fix.
- `scanNextSlotCursor` (`KeyLoader.java:661`) encodes "next slot, key buckets not started" in
  `keyBucketsSkipCount = 1023` while `Wal.scanCursorForNextWalGroup` encodes it in
  `walSkipCount = 1023`; three different fields are now used as phase sentinels, which is the root
  of 70.1's ambiguity. A cleanup introducing an explicit phase field in `ScanCursor` would remove a
  whole class of these bugs (and would also give 70.2 room for a bucket-ordinal field).
- Test-coverage gap behind 70.1/70.2: `Mock.prepareShortValueList(n, bucketIndex)` /
  `Mock.prepareTargetBucketIndexKeyList(n, bucketIndex)` already support writing keys into specific
  buckets (`src/test/groovy/io/velo/persist/Mock.groovy:9-40`) and `KeyHash.hash` has a test-only
  prefix for the same purpose (`src/main/java/io/velo/KeyHash.java:44-79`). The fix phase should
  use those helpers (bucket indices 0 and 1 of the same WAL group) rather than the single-bucket
  fixture that let both bugs through.

### Recommended fix order (for the fix agent)

1. **70.1** — smallest change, hard SCAN-guarantee violation, isolated to
   `Wal.scanCursorForNextWalGroup` / `SGroup.scan`.
2. **70.2** — hard SCAN-guarantee violation with a livelock symptom; needs the `ScanCursor` layout
   change, so do it while the cursor semantics are still fresh from 70.1 (consider fixing both in
   the same cursor-layout commit if the tests allow).
3. **70.3** — structural; do it after 70.1/70.2 so the new cursor logic is tested single-threaded
   first, then re-verify under a multi-worker server.
4. **70.6** and **70.4** — small, independent, parity-only.
5. **70.8** (error text + documentation), **70.7**, **70.5** — hardening/documentation last; 70.5
   becomes much less pressing once 70.3 is fixed.

Each fix: add the regression test first, see it fail, implement minimally, confirm with
`./gradlew :test --tests ...` and check the touched lines in `build/reports/jacocoHtml/`.
