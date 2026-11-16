# Bug 84 — PGroup Commands Review (Round 1)

**Module:** `io.velo.command.PGroup` (the `P*` command group: `persist`, `pexpire`, `pexpireat`, `pexpiretime`, `pfadd`, `pfcount`, `pfmerge`, `psetex`, `psubscribe`, `pttl`, `publish`, `pubsub`, `punsubscribe`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** closed — round 1: 10/10 CONFIRMED (reproduced) and fixed in 10 commits; re-review ACCEPTED (per-commit RED/GREEN + JaCoCo at HEAD + live slotWorkers=2 e2e matching Redis 7.2.11)
**Review branch:** `review/pgroup`
**Reviewed commit:** `3d8ee5326ba81b7764e4829a453f9333dd444d40`

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/PGroup.java` — whole file (1-644)
  - static command registry (28-133), `parseSlots()` (159-211), `handle()` (219-292),
    `persist()` (294-312), `saveHll()` (314-336), `getHll()` (338-362), `getHllExpireAt()` (364-370),
    `pfadd()` (372-410), `mergeHllInto()` (412-416), `pfcount()` (418-476), `pfmerge()` (478-556),
    `publish()` (558-597), `pubsub()` (599-643)
- `src/main/java/io/velo/BaseCommand.java` — `FromToKeyIndex` (40-65), `addToSlotWithKeyHashList()` (67-84),
  `resetContext()` (150-157), `getAuthU()` (194-219), `getCv()` (749-790), `getExpireAt()` (705-725),
  `putToOneSlot()` (1224-1247), `removeDelay()` (1267-1283), `remove()` (1249-1265)
- `src/main/java/io/velo/command/EGroup.java` — `expire()` (241-329), `expiretime()` (331-348)
- `src/main/java/io/velo/command/SGroup.java` — `set()` (834-975), `subscribe()` (2268-2303)
- `src/main/java/io/velo/command/UGroup.java` — `unsubscribe()` (65-88)
- `src/main/java/io/velo/command/TGroup.java` — `ttl()` (164-186)
- `src/main/java/io/velo/SocketInspector.java` — `updateLastSetSeq()` (82-92), `ChannelAndIsPattern` (356-430),
  `subscribeByChannel` (449), `allSubscribeOnes()` (456-457), `filterSubscribeOnesByChannel()` (498-507),
  `subscribe()` (517-555), `unsubscribe()` (565-572), `subscribeSocketCount()` (579-593),
  `publish()` (616-627), `publishOne()` (637-660)
- `src/main/java/io/velo/persist/OneSlot.java` — `asyncCall()` (726-737), `checkCurrentThreadId()` (1600-1605),
  `getExpireAt()` (1615-1620), `get()` (1751-1810), `putIfSeqBigger()` (2048-2070), `put()` (2080-2180)
- `src/main/java/io/velo/persist/LocalPersist.java` — `bindOneSlotIfInitialized()` (212-225),
  `initSlotRuntimeBindings()` (246-255)
- `src/main/java/io/velo/MultiWorkerServer.java` — cross-worker detection (576-592), worker dispatch (622-634),
  `transferAsyncReply()` (694-701), slot worker eventloop array (1061-1098)
- `src/main/java/io/velo/RequestHandler.java` — command dispatch and exception mapping (548-579)
- `src/main/java/io/velo/reply/ErrorReply.java` — error texts (60-168), `buffer()` (199-203)
- `src/main/java/io/velo/CompressedValue.java` — `SP_TYPE_HLL` (64), `isHll()` (522-535), `decompress()` (625-655)
- `src/test/groovy/io/velo/command/PGroupTest.groovy` — whole file (1-584) (the existing coverage baseline)

Reference docs (ground truth):

- https://redis.io/docs/latest/commands/pfadd/ — "1 if at least one HyperLogLog internal register was altered"
- https://redis.io/docs/latest/commands/pfcount/
- https://redis.io/docs/latest/commands/pfmerge/
- https://redis.io/docs/latest/commands/pubsub-channels/ — "An active channel is a Pub/Sub channel with one or more subscribers (excluding clients subscribed to patterns)"
- https://redis.io/docs/latest/commands/pubsub-numsub/ — "Returns the number of subscribers (exclusive of clients subscribed to patterns)"; "it is valid to call this command without channels. In this case it will just return an empty list"
- https://redis.io/docs/latest/commands/pubsub-numpat/ — "the number of unique patterns all the clients are subscribed to"
- https://redis.io/docs/latest/commands/psubscribe/ — "one message with the first element being the string `psubscribe`"
- https://redis.io/docs/latest/commands/punsubscribe/
- https://redis.io/docs/latest/commands/persist/, https://redis.io/docs/latest/commands/pexpire/,
  https://redis.io/docs/latest/commands/pexpireat/, https://redis.io/docs/latest/commands/pexpiretime/,
  https://redis.io/docs/latest/commands/psetex/, https://redis.io/docs/latest/commands/pttl/
- `doc/design/11_multithreading_design.md` — "OneSlot protects slot-owned state with `threadIdProtectedForSafe`.
  Direct calls into OneSlot must happen on the owning slot worker. Cross-slot command paths use
  `OneSlot.asyncCall()` … to schedule work onto the target slot eventloop." (lines 87-95, 141-146)
- Live differential: local `redis-server` **v7.2.11** (sha d4c381df) on port 47999, used for every Redis quote below
- Library ground truth: `com.github.prasanthj:hyperloglog:1.1` (+ `it.unimi.dsi:fastutil:8.5.15`), the HLL
  implementation Velo uses in `PGroup`

Evidence already gathered by agent 1 (live Redis 7.2.11 quotes):

```
$ redis-cli -p 47999 psubscribe 'news.*'
psubscribe
news.*
1
$ redis-cli -p 47999 pubsub channels          # with a 'news.tech' subscriber and a 'news.*' pattern subscriber
news.tech
$ redis-cli -p 47999 pubsub numpat
1
$ redis-cli -p 47999 pubsub numsub news.tech  # pattern subscriber excluded by Redis
news.tech
1
$ redis-cli -p 47999 pubsub numsub            # no channel -> empty array
(empty)
$ redis-cli -p 47999 pubsub bogus
ERR unknown subcommand 'bogus'. Try PUBSUB HELP.
$ redis-cli -p 47999 pfadd str a              # 'str' holds a plain string
WRONGTYPE Key is not a valid HyperLogLog string value.
```

---

## Finding 1 (Bug 84.1): PFMERGE across workers touches the destination key from a foreign slot thread and never answers the client

**Severity:** High
**Class:** Concurrency
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + stack trace, see Review Feedback

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:503-555 — the cross-worker branch of pfmerge()
        ArrayList<MGroup.IndexAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 2, j = 0; i < data.length; i++, j++) {      // 504: sources only, the destination is NOT in the list
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
            list.add(new MGroup.IndexAndSlotWithKeyHash(j, slotWithKeyHash));
        }
        ...
            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> { ... getHll(one.slotWithKeyHash()) ... });   // 515-524: runs on the SOURCE slot thread
        ...
        Promises.all(promises).whenComplete((r, e) -> {           // 530: runs on the thread that completes the LAST promise
            ...
            var dstHll = getHll(dstS);                            // 537: the DESTINATION slot, owned by another worker
            ...
                saveHll(dstS, dstHll, getHllExpireAt(dstS));      // 548: write to the destination slot from that foreign thread
                finalPromise.set(OKReply.INSTANCE);
        });
```
```java
// src/main/java/io/velo/persist/OneSlot.java:1600-1605 — the owner-thread guard that both calls trip
    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id=" + threadId + ", thread id protected for safe=" + threadIdProtectedForSafe);
        }
    }
```
```java
// src/main/java/io/velo/persist/OneSlot.java:1751-1753 and 2080-2082 — get()/put() both call checkCurrentThreadId()
    public BufOrCompressedValue get(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();
```

### Root cause
`OneSlot` is owned by exactly one slot worker thread (`threadIdProtectedForSafe`, bound by
`LocalPersist.bindOneSlotIfInitialized()` as `slot % slotWorkerThreadIds.length`). The cross-worker branch of
`pfmerge()` schedules the *source* keys with `OneSlot.asyncCall()` (correct — the read then happens on the owning
thread), but the *destination* key is read and written **outside** any `asyncCall()`, directly inside the
`Promises.all(...).whenComplete()` callback. That callback is executed by ActiveJ on the thread that completes the last
source promise, i.e. on the worker that owns the last source slot — which is a different worker from the one that owns
the destination slot (that is exactly what made the command "cross worker" in the first place:
`MultiWorkerServer.java:576-592` sets `isCrossRequestWorker` when the keys map to different
`slot % requestHandlerArray.length`).

So `getCv(dstS)` → `OneSlot.get()` → `checkCurrentThreadId()` throws `IllegalStateException`, and
`saveHll(dstS, …)` → `putToOneSlot()` → `OneSlot.put()` would throw the same.

### Trigger / impact
Trigger: a `PFMERGE destkey srckey …` where `destkey` and `srckey` hash to slots owned by different slot workers
(`slotWorkers >= 2`). The destination slot is the first key, so the request is dispatched to the destination's worker
(`MultiWorkerServer.java:596-633`); the source promise completes on the source's worker thread.

Impact:
1. The exception is raised **inside** the `whenComplete` callback, i.e. inside the ActiveJ promise completion
   machinery. The `finalPromise` is therefore **never settled**: `finalPromise.set(OKReply.INSTANCE)` (line 549) is
   never reached. The client gets **no reply at all** — the request hangs until the client times out. The same
   happens on the error path: `finalPromise.setException(e)` is only called for the `e != null` branch, an exception
   thrown by the callback body itself is not converted into an error reply.
2. The exception escapes the promise completion into the slot worker event loop task that completed the promise
   (`OneSlot.asyncCall()` → `slotWorkerEventloop.submit(AsyncComputation.of(...))`), i.e. it is raised on a slot worker
   thread. Whether that kills the worker thread or is only logged by the event loop needs to be confirmed by the
   reviewer (see the checklist).
3. Even in the lucky interleaving where the last completing source slot happens to live in the destination's worker,
   the destination is read/written without the ordering guarantees of the destination's own worker, so the write is not
   serialized with the other commands of that slot.

Redis semantics: `PFMERGE` is a plain single-key write on the destination; it never fails because of the key
distribution. Note also that `pfcount()` (line 418-476) gets this right — it only touches slot state inside
`asyncCall()`, so it is not affected.

### Suggested fix direction
Do not touch the destination slot from the aggregation callback. Either
(a) schedule the destination read+merge+write on the destination's own slot event loop, e.g. by adding the destination
slot to the `groupBySlot` map and doing the merge inside that slot's `asyncCall()` (the merge itself only manipulates
plain `HyperLogLog` objects, so it can be done in whichever slot task runs last, as long as the *destination* read and
write happen inside the destination slot's own task), or
(b) use `LocalPersist.doSthInSlots()`-style dispatch (see `doc/design/11_multithreading_design.md`, "Cross-Thread
Coordination") for the destination.
Whatever the shape, the callback must also never leave `finalPromise` unsettled: wrap the callback body in a
`try/catch` that calls `finalPromise.setException(e)`.

---

## Finding 2 (Bug 84.2): PFADD decides whether to persist by comparing the estimated cardinality, so an altered HLL is silently dropped

**Severity:** Medium
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — agent 2, library repro + end-to-end spec, see Review Feedback

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:372-410 — pfadd()
        var hll = getHll(slotWithKeyHash);
        ...
        var oldCount = hll.count();                 // 393
        for (int i = 2; i < data.length; i++) {
            var itemBytes = data[i];
            hll.addBytes(itemBytes);                 // 396: the register state may change here
        }
        var newCount = hll.count();                  // 398
        var isChanged = oldCount != newCount;        // 399: the ESTIMATE is used as the change detector

        if (isChanged) {                             // 401
            try {
                saveHll(slotWithKeyHash, hll, getHllExpireAt(slotWithKeyHash));   // 403: only persisted when the estimate changed
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        return isChanged ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;   // 409
```

### Root cause
Redis decides "did the HyperLogLog change" by looking at the **register state**: the PFADD return information is
"1 if at least one HyperLogLog internal register was altered" (https://redis.io/docs/latest/commands/pfadd/), and
Redis' `pfaddCommand` replaces the HLL object whenever `hllSparseAdd()` reports a modification.

Velo instead compares the *estimated cardinality* before and after the additions. The estimate is a lossy function of
the register state: adding an element can alter a register (the in-memory HLL is genuinely modified) while leaving the
rounded estimate unchanged. In that case Velo (a) answers `0` where Redis answers `1`, and (b) **discards the modified
HLL** — the element is registered in the in-memory HLL but the store keeps the older state, so the update is lost
forever.

The prasanthj HLL used by Velo switches from the sparse to the dense representation above a few thousand registers, and
from that point on the estimate changes only every few additions. Measured with the exact library Velo ships
(`com.github.prasanthj:hyperloglog:1.1`, JDK repro in the checklist):

```
20000 sequential PFADD-equivalent additions (elements "e0".."e19999"):
  count unchanged but the serialized HLL state changed : 1937 times (9.7 %)
first occurrence                         : element "e2471", estimate 2467 -> 2467 (state changed)
200000 additions                         : 162180 unchanged-estimate events (81 %)
```

### Trigger / impact
Trigger: an HLL that holds more than ~2500 distinct elements, updated with one element at a time (the common
"add events one by one" pattern). Every such update has a high probability of altering a register without moving the
estimate.

Impact:
1. The PFADD reply is `0` where Redis answers `1` (the documented semantics is "a register was altered").
2. The altered HLL is not written back, so the persisted HLL diverges from what the client was told and from what
   Redis stores. The divergence is invisible in the immediate `PFCOUNT` (the estimate did not move) but it changes the
   register state, which is what later `PFMERGE`s and later additions operate on — the two stores drift apart.
3. A client that uses the PFADD reply as an "was it new?" signal (a common pattern) silently loses data.

Note the same class of defect does **not** exist in `pfmerge()`: it always calls `saveHll()`.

### Suggested fix direction
Detect the change on the register state, not on the estimate. The cheapest correct approach with the current library:
serialize the HLL before and after the additions (`HyperLogLogUtils.serializeHLL`) and compare the bytes; if they
differ, the HLL was altered → answer `1` and persist. Keep the estimate only for the reply of the "key only" form.
(`HLLSparseRegister.add()` also returns a "modified" boolean, but `HyperLogLog.add()` does not expose it, so the
serialized-bytes comparison is the pragmatic route.)

---

## Finding 3 (Bug 84.3): PSUBSCRIBE / PUNSUBSCRIBE answer with the wrong confirmation keyword

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:265-269 and 285-289
        if ("psubscribe".equals(cmd)) {
            var sGroup = new SGroup(cmd, data, socket);
            sGroup.from(this);
            return sGroup.subscribe(true);          // 268: isPattern = true
        }
        ...
        if ("punsubscribe".equals(cmd)) {
            var uGroup = new UGroup(cmd, data, socket);
            uGroup.from(this);
            return uGroup.unsubscribe(true);        // 288: isPattern = true
        }
```
```java
// src/main/java/io/velo/command/SGroup.java:2293-2300 — the keyword is hardcoded to "subscribe"
        var replies = new Reply[channels.size() * 3];
        int j = 0;
        for (var channel : channels) {
            replies[j++] = new BulkReply("subscribe");
            replies[j++] = new BulkReply(channel);
            var size = socketInInspector.subscribe(channel, isPattern, socket);
            replies[j++] = new IntegerReply(size);
        }
```
```java
// src/main/java/io/velo/command/UGroup.java:78-85 — the keyword is hardcoded to "unsubscribe"
        for (var channel : channels) {
            replies[j++] = new BulkReply("unsubscribe");
```

### Root cause
`isPattern` is threaded through to `SocketInspector.subscribe()/unsubscribe()` (which is correct — the subscription is
registered as a pattern), but the confirmation keyword that is pushed back to the client is hardcoded to
`"subscribe"` / `"unsubscribe"` in `SGroup.subscribe()` and `UGroup.unsubscribe()`, regardless of `isPattern`.

Redis pushes a confirmation whose first element is the literal command name: `psubscribe` for `PSUBSCRIBE` and
`punsubscribe` for `PUNSUBSCRIBE` (https://redis.io/docs/latest/commands/psubscribe/ — "one message with the first
element being the string `psubscribe`"; live Redis 7.2.11 confirms both).

### Trigger / impact
Trigger: any `PSUBSCRIBE` or `PUNSUBSCRIBE`.

Impact: the client receives `["subscribe", <pattern>, <count>]` / `["unsubscribe", <pattern>, <count>]` instead of
`["psubscribe", …]` / `["punsubscribe", …]`. Client libraries that dispatch on the confirmation kind (Jedis'
`JedisPubSub`, redis-py, Lettuce …) treat the message as a plain channel subscription, so the pattern subscription is
mis-registered on the client side, and a `PUNSUBSCRIBE` confirmation is not recognised as an unsubscribe.

### Suggested fix direction
In `SGroup.subscribe()` / `UGroup.unsubscribe()`, use `isPattern ? "psubscribe" : "subscribe"` (resp.
`"punsubscribe"` / `"unsubscribe"`) for the first element of the confirmation. The two methods are shared with the
`SUBSCRIBE`/`UNSUBSCRIBE` commands, so the keyword must be derived from `isPattern`, not hardcoded.

---

## Finding 4 (Bug 84.4): PUBSUB NUMSUB counts the pattern subscribers, Redis excludes them

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:622-639 — the numsub branch of pubsub()
        } else if ("numsub".equals(subCmd)) {
            ...
            for (var channel : channels) {
                var size = socketInspector.subscribeSocketCount(channel, true);   // 635: isPattern = true
                replies[j++] = new BulkReply(channel);
                replies[j++] = new IntegerReply(size);
            }
```
```java
// src/main/java/io/velo/SocketInspector.java:579-593 and 498-507
    public int subscribeSocketCount(String channel, boolean isPattern) {
        var oneList = filterSubscribeOnesByChannel(channel, isPattern);
        ...
        for (var one : oneList) {
            var sockets = subscribeByChannel.get(one);
            if (sockets != null) {
                count += sockets.size();          // 589: the pattern subscribers are counted too
            }
        }
    }
    ...
    public ArrayList<ChannelAndIsPattern> filterSubscribeOnesByChannel(@NotNull String channel, boolean isPattern) {
        var two = new ChannelAndIsPattern(channel, isPattern);
        ArrayList<ChannelAndIsPattern> oneList = new ArrayList<>();
        subscribeByChannel.forEach((one, value) -> {
            if (one.isMatch(channel) || two.isMatch(one.channel())) {   // 502: one.isMatch() is true for a PATTERN entry
                oneList.add(one);
            }
        });
        return oneList;
    }
```

### Root cause
`PUBSUB NUMSUB` must report "the number of subscribers (**exclusive of clients subscribed to patterns**) for the
specified channels" (https://redis.io/docs/latest/commands/pubsub-numsub/). The implementation calls
`subscribeSocketCount(channel, /* isPattern */ true)`, i.e. it treats the *queried* channel name as a glob. The filter
then also matches every **pattern subscription** whose pattern matches the queried channel, because
`one.isMatch(channel)` is true for a pattern entry (`ChannelAndIsPattern.isMatch()` uses the glob matcher when
`isPattern` is true, `SocketInspector.java:423-429`).

So a client that subscribed to `news.*` inflates the `PUBSUB NUMSUB news.tech` count.

### Trigger / impact
Trigger: at least one pattern subscription whose pattern matches the queried channel.

Impact: the reported subscriber count is larger than Redis' (live Redis 7.2.11 with a `news.tech` subscriber and a
`news.*` pattern subscriber answers `1`, Velo answers `2`). Monitoring and "is anybody listening" logic built on
`PUBSUB NUMSUB` gets a wrong number.

### Suggested fix direction
For `PUBSUB NUMSUB` the count must be restricted to the **exact** channel subscriptions. Add an exact-count helper to
`SocketInspector` (e.g. `subscribeSocketCount(channel, /* isPattern */ false)` must not match pattern entries either —
today `filterSubscribeOnesByChannel(channel, false)` still matches pattern entries through `one.isMatch(channel)`), or
add a dedicated method that only looks at the `ChannelAndIsPattern(channel, /* isPattern */ false)` entry.

---

## Finding 5 (Bug 84.5): PUBSUB CHANNELS lists the pattern subscriptions as if they were channels

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:607-619 — the channels branch of pubsub()
        if ("channels".equals(subCmd)) {
            var pattern = data.length == 3 ? new String(data[2]) : null;
            var ones = pattern == null ? socketInspector.allSubscribeOnes() :           // 609: every entry, patterns included
                    socketInspector.filterSubscribeOnesByChannel(pattern, true);
            if (ones.isEmpty()) {
                return MultiBulkReply.EMPTY;
            } else {
                var replies = new Reply[ones.size()];
                for (int i = 0; i < ones.size(); i++) {
                    replies[i] = new BulkReply(ones.get(i).getChannel());               // 616: the pattern name is returned as a channel
                }
                return new MultiBulkReply(replies);
            }
        }
```
```java
// src/main/java/io/velo/SocketInspector.java:456-457 — allSubscribeOnes() returns channels *and* patterns
    public ArrayList<ChannelAndIsPattern> allSubscribeOnes() {
        return new ArrayList<>(subscribeByChannel.keySet());
    }
```

### Root cause
The subscription registry `subscribeByChannel` is keyed by `ChannelAndIsPattern`, i.e. it holds both the real channel
subscriptions and the pattern subscriptions. `PUBSUB CHANNELS` must list only the real channels: "An active channel is
a Pub/Sub channel with one or more subscribers (**excluding clients subscribed to patterns**)"
(https://redis.io/docs/latest/commands/pubsub-channels/). `allSubscribeOnes()` returns the pattern entries as well and
the reply loop cannot tell them apart, because `ChannelAndIsPattern` does not even expose its `isPattern` flag
(`SocketInspector.java:356-430`: only `getChannel()`, `equals()`, `hashCode()` and the package-private `isMatch()`).

The same happens with a glob argument: `filterSubscribeOnesByChannel(pattern, true)` returns the pattern entries whose
own pattern matches the given glob (e.g. `PUBSUB CHANNELS 'news.*'` returns the pattern name `news.*` itself).

### Trigger / impact
Trigger: any client that has issued a `PSUBSCRIBE`.

Impact: `PUBSUB CHANNELS` returns pattern names mixed with the channel names, and the count of active channels is
inflated. Live Redis 7.2.11 with a `news.tech` subscriber and a `news.*` pattern subscriber answers
`PUBSUB CHANNELS` → `news.tech` only; Velo answers `news.tech` **and** `news.*`.

### Suggested fix direction
Expose the `isPattern` flag on `SocketInspector.ChannelAndIsPattern` (e.g. a public `isPattern()` accessor) and filter
the pattern entries out in `PGroup.pubsub()`:
- without a glob: only the entries with `isPattern == false`;
- with a glob: only the entries with `isPattern == false` whose channel name matches the glob.
The glob matching itself must reuse the same matcher (`ChannelAndIsPattern.isMatch`).

---

## Finding 6 (Bug 84.6): PUBSUB NUMPAT returns a null bulk reply instead of the number of unique patterns

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:620-621 — the numpat branch of pubsub()
        } else if ("numpat".equals(subCmd)) {
            return NilReply.INSTANCE;
```

### Root cause
The `numpat` subcommand is not implemented: it returns a null bulk reply (`$-1`) instead of an integer reply.

Redis: "Returns the number of unique patterns that are subscribed to by clients … the number of patterns all the
clients are subscribed to" (https://redis.io/docs/latest/commands/pubsub-numpat/); live Redis 7.2.11 answers `0` with no
subscriber and `1` with one `PSUBSCRIBE`.

### Trigger / impact
Trigger: any `PUBSUB NUMPAT`.

Impact: the reply type is wrong (a null bulk reply instead of an integer) — a client that parses the reply as an
integer gets a protocol error or a nil, and the information is simply not available. `PUBSUB NUMPAT` is the only way to
observe the pattern subscriptions, so the pattern-subscription state cannot be monitored at all.

### Suggested fix direction
Implement the subcommand: count the distinct `ChannelAndIsPattern` entries with `isPattern == true` in
`SocketInspector` (e.g. a `subscribePatternCount()` helper next to `allSubscribeOnes()`), and answer an
`IntegerReply`. The "unique patterns" wording means distinct pattern names, not distinct clients.

---

## Finding 7 (Bug 84.7): PFADD / PFCOUNT / PFMERGE on a wrong-type key throw a raw RuntimeException instead of answering WRONGTYPE

**Severity:** Low
**Class:** Redis protocol compatibility / error handling
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec (wire text) + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:338-346 — getHll()
    private HyperLogLog getHll(SlotWithKeyHash s) {
        var cv = getCv(s);
        if (cv == null) {
            return null;
        }
        if (!cv.isHll()) {
            throw new RuntimeException(ErrorReply.WRONG_TYPE.getMessage());   // 345: a bare RuntimeException
        }
```
```java
// src/main/java/io/velo/RequestHandler.java:564-579 — how an escaping exception is turned into a reply
            } catch (TypeMismatchException e) {
                log.warn(e.getMessage());
                errorReplyCountTotal++;
                return ErrorReply.WRONG_TYPE;          // 575: the typed path
            } catch (Exception e) {
                log.error("Request handle error.", e);  // 577: the generic path logs a full stack trace
                errorReplyCountTotal++;
                return new ErrorReply(e.getMessage());  // 579: -> "-ERR wrong type"
            }
```

### Root cause
A key that holds a non-HLL value makes `getHll()` raise a bare `RuntimeException` whose message is
`ErrorReply.WRONG_TYPE.getMessage()` == `"wrong type"`. The generic `catch (Exception e)` in `RequestHandler` turns it
into `-ERR wrong type`, i.e. the reply does **not** start with the `WRONGTYPE` error code that Redis uses
(`-WRONGTYPE Key is not a valid HyperLogLog string value.`), and the failure is logged at ERROR level with a stack
trace even though it is a plain client error.

Note that the codebase already has the right mechanism for this: `TypeMismatchException` is caught separately and
answered with `ErrorReply.WRONG_TYPE` (`RequestHandler.java:572-575`).

### Trigger / impact
Trigger: `PFADD`/`PFCOUNT`/`PFMERGE` on a key that holds a string, a hash, a list, …

Impact:
1. The client sees `-ERR wrong type` instead of `-WRONGTYPE …`; clients that classify the error by the `WRONGTYPE`
   prefix (Jedis raises `JedisDataException` instead of a wrong-type error, and several frameworks branch on the
   prefix) mis-classify the error.
2. Every such client mistake produces an ERROR-level stack trace in the server log (`log.error("Request handle
   error.", e)`) — log noise and a false alarm in the error dashboards.
3. In the cross-worker path of `pfcount`/`pfmerge` the exception travels through the promise instead, and
   `finalPromise.setException(e)` produces the same `-ERR wrong type` text — the same divergence, plus the promise
   path never reaches `finalPromise.set(...)`.

### Suggested fix direction
Throw the typed `TypeMismatchException` instead of a bare `RuntimeException` in `getHll()` (the same class the other
command groups use), and give the reply the Redis text, e.g. a dedicated
`ErrorReply("WRONGTYPE Key is not a valid HyperLogLog string value.", /* rawErrorPrefix */ true)` so that the reply
starts with `WRONGTYPE` (see `ErrorReply.READONLY` / `ErrorReply.clusterMoved()` for the `rawErrorPrefix` pattern).

---

## Finding 8 (Bug 84.8): PUBSUB with an unknown subcommand answers a null bulk reply, and PUBSUB NUMSUB without a channel answers an arity error

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:599-643 — pubsub()
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }
        var subCmd = new String(data[1]).toLowerCase();
        if ("channels".equals(subCmd)) { ... }
        else if ("numpat".equals(subCmd)) { ... }
        else if ("numsub".equals(subCmd)) {
            if (data.length < 3) {
                return ErrorReply.FORMAT;          // 624: PUBSUB NUMSUB without a channel is an error in Velo
            }
            ...
        } else {
            return NilReply.INSTANCE;              // 641: an unknown subcommand answers a null bulk reply
        }
```

### Root cause
Two divergences from Redis 7.2:
1. `PUBSUB NUMSUB` without a channel must return an **empty array** — "it is valid to call this command without
   channels. In this case it will just return an empty list" (https://redis.io/docs/latest/commands/pubsub-numsub/).
   Velo answers `ErrorReply.FORMAT`, which the server turns into `*wrong number of arguments for 'pubsub' command`.
2. An unknown subcommand must be an **error**: live Redis 7.2.11 answers
   `ERR unknown subcommand 'bogus'. Try PUBSUB HELP.`; Velo answers a null bulk reply (`$-1`).

### Trigger / impact
Trigger: `PUBSUB NUMSUB` (no channel) or `PUBSUB <unknown>`.

Impact: the reply type/content does not match Redis. A client that probes `PUBSUB NUMSUB` to test whether anybody is
subscribed gets an error instead of an empty list, and a client that mistypes a subcommand gets a nil instead of an
error, so the mistake is not reported.

### Suggested fix direction
- `numsub` with `data.length == 2` → return an empty `MultiBulkReply` instead of `ErrorReply.FORMAT`.
- the `else` branch → return an error reply, e.g.
  `new ErrorReply("unknown subcommand '" + subCmd + "'. Try PUBSUB HELP.", true)` (the `rawErrorPrefix` flag so that
  the text starts with `ERR`, as Redis does).

---

## Finding 9 (Bug 84.9): PERSIST writes the value back with the old sequence number

**Severity:** Low
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + inspection of `putIfSeqBigger`

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:294-312 — persist()
        cv.setExpireAt(CompressedValue.NO_EXPIRE);   // 309: only the expire time is changed
        putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);   // 310: no new sequence number is allocated
        return IntegerReply.REPLY_1;
```
```java
// src/main/java/io/velo/command/EGroup.java:321-328 — the equivalent EXPIRE/PEXPIRE path
        cv.setSeq(snowFlake.nextId());        // 321: a new sequence number IS allocated here
        cv.setKeyHash(slotWithKeyHash.keyHash());
        cv.setExpireAt(expireAt);
        putToOneSlot(slotWithKeyHash.slot(), slotWithKeyHash, cv);
```

### Root cause
Every write in Velo allocates a fresh sequence number (`snowFlake.nextId()`); the sequence is what orders the writes
of a key and what the replication/merge paths compare. `EGroup.expire()` (the EXPIRE/PEXPIRE family) allocates a new
sequence before writing the value back, but `PGroup.persist()` reuses the sequence of the value it read.

### Trigger / impact
Trigger: `PERSIST key` on a key that has a TTL.

Impact: the TTL removal is written with the sequence number of the *previous* write of that key. The replication
full-sync path compares the sequences to decide whether the incoming value is newer:
`OneSlot.putIfSeqBigger()` (`OneSlot.java:2048-2070`) skips the write when the local sequence is `>=` the incoming
sequence. A slave that did not (yet) apply the `PERSIST` holds the same sequence number, so the master's
TTL-less value is skipped and the slave keeps the TTL — a master/slave TTL divergence that survives the full sync.
(`SocketInspector.updateLastSetSeq()` is not called either, so `WAIT` does not see the write — the same as the
EXPIRE family, which is why this is reported as Low.)

### Suggested fix direction
Allocate a new sequence in `PGroup.persist()`, exactly as `EGroup.expire()` does:
`cv.setSeq(snowFlake.nextId())` before `putToOneSlot(...)`.

---

## Finding 10 (Bug 84.10): the PEXPIRE / PEXPIREAT / PSETEX error texts diverge from Redis

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11

### Cited files / lines
```java
// src/main/java/io/velo/command/PGroup.java:224-234 — pexpire/pexpireat delegate to EGroup
        if ("pexpire".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(false, true);
        }
```
```java
// src/main/java/io/velo/command/EGroup.java:263-270 — the non-integer TTL
        long seconds;
        try {
            seconds = Long.parseLong(new String(secondsBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;        // 269: "-ERR not integer"
        }
```
```java
// src/main/java/io/velo/command/SGroup.java:892-900 — the PSETEX millisecond argument
            long value;
            try {
                value = Long.parseLong(new String(dd[i + 1]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;    // 896: "-ERR not integer"
            }
            if (value <= 0) {
                return ErrorReply.INVALID_INTEGER; // 899: "-ERR invalid integer"
            }
```

### Root cause
The error texts used by the `P*` TTL family are the generic ones of `ErrorReply`:
`NOT_INTEGER` = `"not integer"` and `INVALID_INTEGER` = `"invalid integer"`. Redis 7.2 answers:

| command | Redis 7.2 | Velo |
|---|---|---|
| `PEXPIRE k abc` | `ERR value is not an integer or out of range` | `-ERR not integer` |
| `PEXPIREAT k abc` | `ERR value is not an integer or out of range` | `-ERR not integer` |
| `PSETEX k abc v` | `ERR value is not an integer or out of range` | `-ERR not integer` |
| `PSETEX k 0 v` | `ERR invalid expire time in 'psetex' command` | `-ERR invalid integer` |

(`ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE` = `"value is not an integer or out of range"` already exists and matches
Redis; `EGroup` has a dedicated `invalidExpireTime(cmd)` helper for the "invalid expire time" text.)

### Trigger / impact
Trigger: a non-integer or non-positive TTL argument.

Impact: only the error text differs — the commands still fail — but error strings are part of the Redis protocol
surface that clients and tests match on. The `PEXPIRE`/`PEXPIREAT` root cause is in `EGroup.expire()` (shared with the
`EXPIRE`/`EXPIREAT` commands, which were reviewed in bug 75); the `PSETEX` root cause is in `SGroup.set()` (shared
with `SET`, reviewed in an earlier round) — both are reachable through the `P*` commands.

### Suggested fix direction
Use `ErrorReply.NOT_INTEGER_OR_OUT_OF_RANGE` for the non-integer TTL and the Redis "invalid expire time in '<cmd>'
command" text for a non-positive TTL (`EGroup.invalidExpireTime(cmd)` already builds it). Because the fix sits in
`EGroup`/`SGroup`, it touches commands outside the `P` group: coordinate with the owners of those modules (bug 75 /
the string-module round) or mark it as a cross-module follow-up.

---

## Notes / non-findings

Checked and found **correct** (kept here so later rounds do not re-investigate them):

- `PERSIST` returns `0` for a missing key and for a key without a TTL, `1` when the TTL was removed — matches Redis
  (`PGroup.java:294-312`).
- `PEXPIRE` / `PEXPIREAT` with a TTL in the past delete the key and answer `1`, like Redis
  (`EGroup.java:315-319`); the `NX`/`XX`/`GT`/`LT` options are evaluated **before** the delete decision, which
  matches Redis 7.2.
- `PEXPIRE` overflow protection: `EGroup.expire()` rejects an overflowing TTL with
  `invalid expire time in 'pexpire' command` and leaves the TTL untouched — matches Redis 7.2 (already covered by
  `PGroupTest.'test pexpire'`).
- `PEXPIRETIME` returns `-2` for a missing key, `-1` for a key without a TTL and the millisecond timestamp otherwise —
  matches Redis.
- `PTTL` returns `-2` / `-1` / the remaining milliseconds — matches Redis (`TGroup.ttl()`).
- `PSETEX` maps onto `SET key value PX ms` (`PGroup.java:254-263`); the argument order (`dd[2]` = value, `dd[4]` = ms)
  is correct and the TTL is preserved on the value.
- `PFADD key` (no element) creates an empty HLL and answers `1` on a missing key, `0` on an existing one — matches
  Redis 7.2 (verified live).
- `PFMERGE` preserves the TTL of the destination key — verified live against Redis 7.2.11
  (`PFMERGE` on a destination with `EX 100` keeps `TTL 100`).
- `PFMERGE` with a destination that is not an HLL leaves the destination untouched and answers a `WRONGTYPE` error —
  matches Redis (the destination type is checked before the sources are merged in the non-cross path).
- `PUBLISH` returns the number of message deliveries (channels **and** matching patterns), which is what Redis counts
  as well (Redis counts a client twice when it is subscribed both directly and through a pattern).
- `pfcount()` is *not* affected by finding 84.1: it only touches slot state inside `OneSlot.asyncCall()`, so the
  cross-worker aggregation callback never enters a foreign slot.
- `pfcount`/`pfmerge` group the keys by slot before dispatching, so a key that appears several times in the same slot
  is handled correctly.
- `parseSlots()` registers the right keys for every command: one key for `persist`/`pexpire`/`pexpireat`/
  `pexpiretime`/`pfadd`/`psetex`/`pttl`, and every argument from index 1 for `pfcount`/`pfmerge` (all of them are keys
  in Redis too).
- The command registry entries of `PGroup` (arity, key positions, ACL categories, command flags) match the Redis 7.2
  command table.

## Reviewer checklist (for AI agent 2)

- **84.1 (High)** — reproduce the cross-worker `PFMERGE`:
  1. Start a Velo server with `slotWorkers = 2` (two slot worker eventloops) and enough slots that two keys can land
     on different workers; find two keys `dst` and `src` whose slots have different `slot % 2` values
     (`KEYSLOT`/`CLUSTER KEYSLOT` or the slot from the `PGroup.parseSlots()` helper).
  2. `PFADD dst a`, `PFADD src b`, then `PFMERGE dst src`.
  3. Expect: no reply at all for `PFMERGE` (the client hangs), and on the server side an
     `IllegalStateException: Thread id not match, thread id=…, thread id protected for safe=…` raised on the source
     slot's worker thread (check the worker logs). Confirm whether the slot worker thread survives or dies.
  4. Alternative (no server needed): a Spock test with two `OneSlot`s bound to two different eventloops (see
     `PGroupTest.'test pfcount and pfmerge'`, lines 404-433, which sets up one slot on a second eventloop), the
     destination key in the slot owned by the *current* thread and the source key in the slot owned by the other
     eventloop, `crossRequestWorker = true`. The `whenComplete` callback then runs on the foreign eventloop and
     `getHll(dstS)` must throw. Note that the existing test only passes because `byPassGetSet` (InMemoryGetSet) bypasses
     `OneSlot` and therefore the owner-thread guard.
- **84.2 (Medium)** — PFADD must persist an altered HLL even when the estimate does not move:
  1. `PFADD hll e0 e1 … e2470` (2471 elements) → `1`; `PFCOUNT hll` → `2467`.
  2. `PFADD hll e2471` → Velo answers `0` and does **not** write the value back; Redis 7.2 answers `1` when a
     register was altered (run the same sequence against the local Redis 7.2.11 and compare the replies).
  3. Cross-check with the JDK repro against `com.github.prasanthj:hyperloglog:1.1`: add `e0`…`e2471` one by one and
     compare `count()` with the serialized state before/after each addition — the state changes while the count stays
     `2467` (first occurrence at `e2471`); 1937 of 20000 additions behave that way.
- **84.3 (Medium)** — `PSUBSCRIBE news.*` must push `["psubscribe","news.*",1]`, `PUNSUBSCRIBE news.*` must push
  `["punsubscribe","news.*",0]`; compare with the local Redis 7.2.11 (already quoted in the Scope section).
- **84.4 (Medium)** — with one exact subscriber of `news.tech` and one pattern subscriber of `news.*`,
  `PUBSUB NUMSUB news.tech` must answer `news.tech` / `1` (Redis), Velo answers `2`.
- **84.5 (Medium)** — same setup: `PUBSUB CHANNELS` must answer only `news.tech`; `PUBSUB CHANNELS 'news.*'` must
  answer only `news.tech` (not the pattern name).
- **84.6 (Medium)** — `PUBSUB NUMPAT` must answer an integer (`0` / `1` with a pattern subscriber); Velo answers a
  null bulk reply (`$-1`).
- **84.7 (Low)** — `SET str hello` then `PFADD str a` must answer a reply starting with `WRONGTYPE`; Velo answers
  `-ERR wrong type` and logs an ERROR with a stack trace. Same for `PFCOUNT str` and `PFMERGE str other`.
- **84.8 (Low)** — `PUBSUB NUMSUB` (no channel) must answer an empty array (Redis) — Velo answers an arity error;
  `PUBSUB bogus` must answer `ERR unknown subcommand 'bogus'. Try PUBSUB HELP.` — Velo answers `$-1`.
- **84.9 (Low)** — inspect the sequence number of the value written by `PERSIST`: it must differ from the sequence of
  the previous write of the same key (compare with `EGroup.expire()`, which allocates `snowFlake.nextId()`).
- **84.10 (Low)** — `PEXPIRE k abc`, `PEXPIREAT k abc`, `PSETEX k abc v`, `PSETEX k 0 v` — compare the error texts
  with the Redis 7.2.11 table in the finding.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (`Bug84Agent2VerifyTempSpec`, asserted the current behavior, deleted after evidence
capture) + live Redis 7.2.11 differential (port 47999) + static inspection + ActiveJ 6.0-beta2 bytecode.
Full evidence log: `doc/bug_reviews/logs/20261110_194025_pgroup_review_agent2_bug_84.log`.
Verification ran at commit `9556bdf1` (parent = the reviewed commit `3d8ee532`), branch `review/pgroup`.
Temp spec result: 8 feature methods, all green (each assertion asserts the *buggy* behavior).

| Finding | Verdict | Evidence |
|---|---|---|
| 84.1 | CONFIRMED (reproduced) | Temp spec with two real OneSlots (slot 0 owned by the dst worker thread, slot 1 rebound to a second eventloop thread B), `crossRequestWorker=true`, no `byPassGetSet`. Control: cross-worker `PFCOUNT` on the same keys settles correctly (`IntegerReply 0`). `PFMERGE dst src`: worker B held busy during subscription so the source promise completes on B, then the `whenComplete` callback runs on B — log shows `java.lang.IllegalStateException: Thread id not match, thread id=53, thread id protected for safe=1` with the stack `OneSlot.get:1752 ← BaseCommand.getCv:768 ← PGroup.getHll:339 ← PGroup.lambda$pfmerge$5:537 ← AbstractPromise completion ← OneSlot.lambda$asyncCall$2:743 ← Eventloop.run` — i.e. the exact cited line inside the promise machinery on the foreign worker. `finalPromise` never settles (`settled=false` after 2.5 s) → the client hangs with no reply; worker B survives (`isAlive` asserted, eventloop kept running). |
| 84.2 | CONFIRMED (reproduced) | Library repro with the shipped `com.github.prasanthj:hyperloglog:1.1` reproduced agent 1's numbers exactly: first unchanged-estimate-but-altered-register element `e2471`, estimate 2467, 1925/20000 unchanged-estimate events (agent 1 measured 1937 — same phenomenon, immaterial count diff). End-to-end: `pfadd hll e0..e2470` → 1; `pfadd hll e2471` → `REPLY_0` and the stored bytes equal the reference state *without* e2471 and differ from the reference *with* e2471 — the altered HLL is silently dropped. Live Redis 7.2.11: `PFADD dhll e2471` after the same batch → `1` (dup again → `0`). |
| 84.3 | CONFIRMED (reproduced) | Temp spec: `PSUBSCRIBE news.*` pushes `["subscribe","news.*",1]`, `PUNSUBSCRIBE news.*` pushes `["unsubscribe","news.*",0]`. Live Redis 7.2.11 pushes `psubscribe` / `punsubscribe` as the first element. |
| 84.4 | CONFIRMED (reproduced) | Temp spec with one exact subscriber of `news.tech` + one pattern subscriber of `news.*`: `PUBSUB NUMSUB news.tech` → `2`. Live Redis 7.2.11 same setup → `news.tech 1`. |
| 84.5 | CONFIRMED (reproduced) | Same setup: `PUBSUB CHANNELS` → `[news.*, news.tech]` and `PUBSUB CHANNELS news.*` → `[news.*, news.tech]` (the pattern name itself is listed). Live Redis 7.2.11 → `[news.tech]` for both. |
| 84.6 | CONFIRMED (reproduced) | Temp spec: `PUBSUB NUMPAT` → `NilReply.INSTANCE` (null bulk `$-1`) with one pattern subscription active. Live Redis 7.2.11 → `(integer) 1`. |
| 84.7 | CONFIRMED (reproduced) | Temp spec: `pfadd str a` on a hash key throws bare `RuntimeException("wrong type")`; the RequestHandler generic path (`new ErrorReply(e.getMessage())`) renders the wire text `-ERR wrong type\r\n`. Live Redis 7.2.11 answers `WRONGTYPE Key is not a valid HyperLogLog string value.` for `PFADD`/`PFCOUNT`/`PFMERGE` on a string key (all three quoted). |
| 84.8 | CONFIRMED (reproduced) | Temp spec: `pubsub numsub` → `ErrorReply.FORMAT` (arity error), `pubsub bogus` → `NilReply.INSTANCE`. Live Redis 7.2.11: `PUBSUB NUMSUB` → empty array; `PUBSUB bogus` → `ERR unknown subcommand 'bogus'. Try PUBSUB HELP.` |
| 84.9 | CONFIRMED (reproduced) | Temp spec: cv with `expireAt` set and seq S; `PERSIST` → `REPLY_1`, stored cv has `expireAt == NO_EXPIRE` but `seq == S` — no new sequence allocated (contrast `EGroup.expire()` line 321 `cv.setSeq(snowFlake.nextId())`, inspection). Impact chain by inspection: `OneSlot.putIfSeqBigger()` lines 2058/2063 skip the incoming value when the local seq `>=` the incoming seq, so a full-sync merge of the PERSIST-rewritten value is dropped on a replica that already holds seq S. |
| 84.10 | CONFIRMED (reproduced) | Temp spec: `pexpire k abc`/`pexpireat k abc`/`psetex k abc v` → `-ERR not integer`; `psetex k 0 v` → `-ERR invalid integer`. Live Redis 7.2.11: first three → `ERR value is not an integer or out of range`, last → `ERR invalid expire time in 'psetex' command`. All four rows of the table in the finding match. |

### Additional observations

1. **84.1 refinement — three interleavings, two of them broken.** ActiveJ `AbstractPromise.map()` runs the
   callback inline on the *subscribing* thread when the source promise is already complete, and its `next0`
   (verified in the 6.0-beta2 bytecode) asserts `!isComplete()` while subscribing:
   - *pending-subscribe* (the production norm with ≥ 2 foreign source slots): callback runs on the foreign
     worker → guard throws → **no reply ever** (reproduced deterministically above);
   - *complete-before-subscribe*: callback runs inline on the subscribing thread. In production the request is
     dispatched to the first key's (destination's) worker, so this interleaving works by luck — the bug is
     interleaving-dependent, which makes it nastier to observe, not less real;
   - *complete-during-subscribe* (observed once during verification): `Promises.all` throws `AssertionError`
     from `next0`'s assert under `-ea`; in production (assertions disabled by default) the subscriber is
     attached to an already-complete promise and never fires — a second no-reply hang path inside the same bug.
2. **84.1 checklist question answered:** the slot worker thread **survives** — the ActiveJ eventloop catches the
   task exception via its exception handler (the ISE + stack appears in the log) and keeps running. Damage is
   the silent hang + lost write, not a dead worker.
3. **84.1 scope note:** with `slotWorkers=1` nothing is cross-worker, so the bug needs ≥ 2 slot workers; the
   non-cross path of `pfmerge()` is unaffected (verified green in the existing `PGroupTest`).
4. **84.2 note:** the reviewer's HLL estimate (`2467` for e0..e2470) and Redis's own (`2453`) differ because the
   two implementations approximate differently; that is expected and *not* the bug — the bug is the reply
   semantics (register-altered vs estimate-moved) and the dropped write, both reproduced.
5. **84.9 note:** `Mock.prepareCompressedValueList` seeds seq=0, so the spec proves "seq unchanged" with 0; a
   real write path always carries a snowflake seq, and the reuse is what matters (inspection + spec).
6. **Non-findings spot-checked live:** `PFADD key` key-only → 1 then 0; `PFMERGE` on a string destination →
   WRONGTYPE with the destination untouched; `PFMERGE` preserves the destination TTL (ttl 100 before and
   after). All match the report's non-findings list.

### Recommended fix order (for the fix agent)

1. **84.1** (High) — move the destination read+merge+write into the destination slot's own task and make the
   callback exception-safe so `finalPromise` is always settled; one bug one commit.
2. **84.2** (Medium) — detect the change on the serialized register state, persist and reply 1 when altered.
3. **84.3, 84.4, 84.5, 84.6** (Medium) — pubsub compatibility cluster; 84.6 is the smallest (implement NUMPAT),
   84.4/84.5 need an exact-channel count/list helper on `SocketInspector`, 84.3 needs the keyword derived from
   `isPattern` in `SGroup.subscribe()`/`UGroup.unsubscribe()`.
4. **84.7, 84.8** (Low) — typed `TypeMismatchException` + WRONGTYPE text; numsub empty array + unknown
   subcommand error.
5. **84.9** (Low) — `cv.setSeq(snowFlake.nextId())` in `PGroup.persist()`.
6. **84.10** (Low, cross-module) — the fix lives in `EGroup.expire()` / `SGroup.set()` shared with the
   EXPIRE/EXPIREAT/SET commands (bug 75 / string-module territory): coordinate or split into a cross-module
   follow-up so the shared error texts change once, deliberately.

## Response (AI agent 1, round 1)

Stage 3 was skipped on the user's instruction (all ten findings were confirmed by agent 2, no disagreement to
express), the fix phase started directly.

## Fix Log (AI agent 1, round 1 fix phase)

Every bug was fixed with the TDD cycle of `doc/write_better_test_cases_for_ai_agents.md`: the regression test was
added first and watched failing for the expected reason, then the minimal production change was made, then the
focused test was re-run green, then the JaCoCo report of the touched class was inspected. One bug = one commit.

The permanent regression tests live in `src/test/groovy/io/velo/command/PGroupTest.groovy` (the new cases were added
to the existing per-command methods: `test pfadd`, `test pfcount and pfmerge`, `test pfmerge destkey-only`,
`test persist`, `test pexpire`, `test pexpireat`, `test pubsub`, plus the new `test psetex`, `test psubscribe`,
`test punsubscribe` methods).

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 84.1 | RED `PGroupTest.'test pfcount and pfmerge'`: `recordingGetSet.putThreadId[dstKey] == dstOwnerThreadId.get()` → **false** (`putThreadId=[pfmerge_key_3:1]` vs the destination owner `54`) — the destination key was written on the *source* slot thread (the test worker, id 1) instead of the destination slot owner (id 54) | `8870d553` `fix: pfmerge cross worker writes the destination key on its own slot thread` | `./gradlew :test --tests "io.velo.command.PGroupTest"` → 21 tests, 0 failures | `PGroup.java` 530-566 (the aggregation + the new destination slot task): **covered**; the two defensive `catch` branches (557-559, 563-565) stay uncovered |
| 84.2 | RED `PGroupTest.'test pfadd'`: `oneMoreReply == IntegerReply.REPLY_1` → **false** (`REPLY_0`); the count assertion `2467` passed first, so the scenario was the intended one (the register was altered, the estimate was not) | `21a91799` `fix: pfadd detects the hll register change, not the estimated cardinality` | same focused run → green | `PGroup.java` 314-435 (`serializeHll`, `saveHllBytes`, `getHllWithBytes`, the new `isChanged` computation 418, the save 420-428): **covered**, all 4 branches of 418 covered; only the unreachable `IOException` branch of `serializeHll` (415-417) is uncovered |
| 84.3 | RED `PGroupTest.'test psubscribe'` / `'test punsubscribe'`: the first element of the confirmation was `subscribe` / `unsubscribe` (`[115,117,98,115,...]`) instead of `psubscribe` / `punsubscribe` | `1383e019` `fix: psubscribe and punsubscribe reply with the redis confirmation keyword` | `:test --tests "io.velo.command.PGroupTest" --tests "io.velo.command.SGroupTest" --tests "io.velo.command.UGroupTest"` → green | `SGroup.java:2295` and `UGroup.java:81`: **both branches covered** (the pattern and the plain keyword) |
| 84.4 | RED `PGroupTest.'test pubsub'`: `PUBSUB NUMSUB test_channel` → **2** instead of 1 (the pattern subscriber `test_*` was counted) | `090a0a48` `fix: pubsub numsub excludes the pattern subscribers` | `:test --tests "io.velo.command.PGroupTest"` → green | `PGroup.java:669` and `SocketInspector.java:616-617` (`exactSubscribeSocketCount`): **covered**, both branches (a channel with and without a subscriber) |
| 84.5 | RED `PGroupTest.'test pubsub'`: `PUBSUB CHANNELS` → **2** replies (`test_channel` **and** the pattern `test_*`), `PUBSUB CHANNELS test_*` → 2 replies as well | `8a233333` `fix: pubsub channels lists only the channels, not the pattern subscriptions` | same focused run → green | `PGroup.java:640-651` and `SocketInspector.java:626-640` (`subscribeChannels` + the new `isPattern()` accessor): **covered**, all 4 branches of the glob match |
| 84.6 | RED `PGroupTest.'test pubsub'`: `PUBSUB NUMPAT` → `NilReply.INSTANCE` (a null bulk reply) instead of an integer | `73170e07` `fix: pubsub numpat returns the number of the subscribed patterns` | same focused run → green | `PGroup.java:693` and `SocketInspector.java:623-629` (`subscribePatternCount`): **covered**, both branches |
| 84.7 | RED `PGroupTest.'test pfadd'` / `'test pfmerge destkey-only'`: the `RuntimeException("wrong type")` escaped the command (`java.lang.RuntimeException: wrong type at io.velo.command.PGroup.getHllWithBytes(PGroup.java:361)`) instead of an error reply | `900ed778` `fix: pf commands answer the redis WRONGTYPE error for a wrong type key` | `:test --tests "io.velo.command.PGroupTest"` → green, and the whole command module (`--tests "io.velo.command.*"`) → green | `PGroup.java` 223-224 (the `TypeMismatchException` catch in `handle()`), 354-362 (`getHllWithBytes`), 447-470 (`readHllList`), the async wrong-type branches: **covered**; the `e != null` promise-error branches stay uncovered (pre-existing) |
| 84.8 | RED `PGroupTest.'test pubsub'`: `PUBSUB NUMSUB` (no channel) → `ErrorReply{message='format'}` instead of an empty list; `PUBSUB xxx` → `NilReply.INSTANCE` instead of an error | `73775785` `fix: pubsub numsub without a channel returns an empty list, unknown subcommand is an error` | same focused run → green | `PGroup.java:692-715`: **covered**, all 5 branches |
| 84.9 | RED `PGroupTest.'test persist'`: `bufOrCv.cv().seq != seqBeforePersist` → **false** (`seq=0` before and after, i.e. the write reused the old sequence) | `6cfd1409` `fix: persist allocates a new sequence number for the write` | same focused run → green | `PGroup.java:306-328`: **covered**, all 3 branches |
| 84.10 | RED `PGroupTest.'test pexpire'` / `'test pexpireat'` / `'test psetex'`: the message was `not integer` / `invalid integer` instead of `value is not an integer or out of range` / `invalid expire time in 'psetex' command` | `53a62389` `fix: pexpire, pexpireat and psetex answer the redis error texts` | `:test --tests "io.velo.command.PGroupTest" --tests "io.velo.command.SGroupTest" --tests "io.velo.command.EGroupTest"` → 67 tests green, and the whole command module → green | `EGroup.java:263-275` and `SGroup.java:893-905`: **covered**, all branches |

### Cross-module impact of the 84.10 fix (declared)

The root cause of 84.10 is shared: `EGroup.expire()` (used by `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`) and
`SGroup.set()` (used by `SET` and by `PSETEX` through `PGroup.psetex()`). The fix was applied in the shared code, so
the error texts of the commands outside the `P` group changed as well — they now match Redis 7.2 exactly:

| command | before | after (= Redis 7.2) |
|---|---|---|
| `EXPIRE` / `EXPIREAT` / `PEXPIRE` / `PEXPIREAT` `<not a number>` | `-ERR not integer` | `-ERR value is not an integer or out of range` |
| `SET ... EX\|PX\|EXAT\|PXAT <not a number>` | `-ERR not integer` | `-ERR value is not an integer or out of range` |
| `SET ... EX\|PX\|EXAT\|PXAT <not positive>` | `-ERR invalid integer` | `-ERR invalid expire time in 'set' command` |
| `PSETEX <not positive>` | `-ERR invalid integer` | `-ERR invalid expire time in 'psetex' command` |

The affected existing assertions were updated: `EGroupTest.groovy` (the `expire` case) and `SGroupTest.groovy`
(the seven `set` option cases of `'test set'`). No other module behaviour changed.

## Re-review (AI agent 2)

**Method:** per-commit independent RED/GREEN re-derivation in a worktree at each fix commit (GREEN: focused test
at the commit; RED: same tree with only the production files reverted to the parent — reviewed **commit by
commit**, no loop) + focused regression and JaCoCo at HEAD in the main workspace + live Velo `slotWorkers=2`
e2e at HEAD (jar built from `bfd1dd0c`, port 7397).
Full evidence log: `doc/bug_reviews/logs/20261116_132922_pgroup_rereview_agent2_bug_84.log`.

**Reviewer infrastructure note:** in a fresh worktree, `:compileTestGroovy` after `git checkout` churn OOMs
fast even with a 16g daemon — the groovy incremental-compile state corrupts on tree flip-flops. Running
`:cleanCompileTestGroovy` + the compile as a separate invocation before the test run makes every build
reliable (~10 s per compile). Same family as the "gradle daemon heap" open item of bug 73.

### Per-commit verification (10/10 RED + GREEN re-derived)

| Bug | Commit | RED re-derived (production reverted to parent) | GREEN re-derived |
|---|---|---|---|
| 84.1 | `8870d553` | `putThreadId[dstKey] == dstOwnerThreadId.get()` → **false**: dst `pfmerge_key_3` written on thread **1**, dst owner is **53** — the foreign-thread write | `test pfcount and pfmerge` 1/1 |
| 84.2 | `21a91799` | `oneMoreReply == REPLY_1` → **false** (`REPLY_0` for e2471) | `test pfadd` 1/1 |
| 84.3 | `1383e019` | confirmations were `"subscribe"` / `"unsubscribe"` (both methods) | 2/2 |
| 84.4 | `090a0a48` | `PUBSUB NUMSUB` count == **2** (pattern subscriber counted) | 1/1 |
| 84.5 | `8a233333` | `PUBSUB CHANNELS` length == **2** (`test_channel` + pattern `test_*`) | 1/1 |
| 84.6 | `73170e07` | `reply instanceof IntegerReply` → **false** (`NilReply`) | 1/1 |
| 84.7 | `900ed778` | bare `RuntimeException: wrong type` escaped `getHllWithBytes:361` (both methods) | 2/2 |
| 84.8 | `73775785` | `PUBSUB NUMSUB` → `ErrorReply{message='format'}` instead of the empty list | 1/1 |
| 84.9 | `6cfd1409` | `seq != seqBeforePersist` → **false** (0 → 0, old sequence reused) | 1/1 |
| 84.10 | `53a62389` | message was `not integer` (28 diffs vs the Redis text; both methods) | 2/2 |

Every RED matches the failing-test evidence column of the Fix Log; every GREEN was re-run at the fix commit.

### JaCoCo at HEAD (focused set: PGroupTest+SGroupTest+UGroupTest+EGroupTest+SocketInspectorTest, all green)

| Class / lines (HEAD) | Bug | Coverage |
|---|---|---|
| `PGroup.java` 592-615 (dst slot task) | 84.1 | 15/17 lines; only the defensive `whenComplete` exception branch (614-615) uncovered — as declared |
| `PGroup.java` 425-440 (isChanged + save) | 84.2 | both branch lines fully covered; only the two defensive `IOException` catches (429-430, 437-438) uncovered — same category as declared |
| `PGroup.java` 680-720 (pubsub) | 84.4/84.5/84.6/84.8 | 27/27 lines, 9/9 branch lines fully covered |
| `PGroup.java` 220-230 (`TypeMismatchException` catch) | 84.7 | 3/3 |
| `PGroup.java` 300-330 (persist) | 84.9 | 15/15, all branches |
| `SGroup.java` 2290-2300 / `UGroup.java` 78-86 | 84.3 | 7/7 and 8/8, both keyword branches |
| `EGroup.java` 263-280 / `SGroup.java` 890-910 | 84.10 | 9/9 and 13/13, all branches |
| `SocketInspector.java` 620-668 + `isPattern()` | 84.4/84.5/84.6 | 20/20 lines, 7/7 branch lines (incl. all 4 glob branches) — via the reviewer-added direct tests below |

**Reviewer-added coverage (this re-review):** the fix phase covered the new `SocketInspector` public helpers
only indirectly through `PGroupTest`. Added `SocketInspectorTest.'test pubsub channel and pattern helpers'`
(`subscribePatternCount`, `subscribeChannels(null/glob)`, `exactSubscribeSocketCount` incl. the
pattern-exclusion and unsubscribe paths, `ChannelAndIsPattern.isPattern()`) — focused run green, class 12/12,
JaCoCo as above. Test-only commit `5380f463`.

### Live e2e at HEAD (slotNumber=8, slotWorkers=2)

Key placement computed with the project's own `BaseCommand.slot(key, 8)` (Velo **inner** slots, not cluster
slots): `k1` → slot 0 / worker 0, `k0` → slot 1 / worker 1.

- Cross-worker `PFMERGE k1 k0` and the reverse `PFMERGE k0 k1`: both **+OK**, `PFCOUNT` = 4, 5 repeats exit 0 —
  no hang (pre-fix the client received no reply at all).
- The full RESP differential (psubscribe/punsubscribe confirmations, `pubsub channels`/`numsub`/`numpat` with
  one exact + one pattern subscriber, WRONGTYPE on `pfadd`/`pfcount` of a string key, `pubsub numsub` empty
  array, `pubsub bogus` error, the four 84.10 error texts, the e2471 register-alteration PFADD → 1 / dup → 0,
  `persist` + `pttl -1`) — **all identical to the live Redis 7.2.11 ground truth** captured in round 1.
- Server log after the whole e2e: **0** occurrences of `IllegalStateException` / `Thread id not match` / ERROR.

### Verdict

**ACCEPTED — 10/10 fixes verified.** All four Fix Log evidence columns (failing test → commit → focused green →
JaCoCo) were independently reproduced per commit; the cross-module impact of 84.10 (EXPIRE/EXPIREAT/SET error
texts, EGroupTest/SGroupTest updated) is green at HEAD; the live slotWorkers=2 server at HEAD behaves like
Redis 7.2.11 on every fixed behavior. The Residual Risk list below stands as written; nothing to add except
the worktree/daemon note above.

## Residual Risk

1. **The ActiveJ promise race is not closed by 84.1 (velo-wide).** Agent 2's observation: a listener that is
   registered on a promise which completes concurrently can be lost (`AbstractPromise.next0()` only asserts
   `!isComplete()`, and without `-ea` the listener is silently dropped → the callback never fires → the reply is
   never sent). The 84.1 fix removes the *PGroup-specific* trigger (the destination slot was read and written from a
   foreign worker thread, which raised the `IllegalStateException` that left `finalPromise` unsettled), but the
   underlying `Promises.all(...).whenComplete(...)` pattern is exposed to the same race in **every** cross-worker
   command group (`EGroup.exists`, `MGroup`, `GGroup`, `ZGroup`, `SGroup`, …). A follow-up round on the shared
   promise usage is recommended; it is out of the scope of the `P` group.
2. **`PUBSUB CHANNELS` still lists a channel whose last subscriber left.** `SocketInspector.unsubscribe()` removes the
   socket but keeps the (now empty) map entry, so the channel stays in the registry. Redis only lists the channels with
   at least one subscriber. The 84.5 fix removed the pattern entries from the answer but did not touch the empty
   entries — the fix is in `SocketInspector`, shared with the publish path.
3. **The glob matching is Java's, not Redis'.** `SocketInspector.ChannelAndIsPattern` builds a
   `java.nio.file.PathMatcher` with the `glob:` syntax, while Redis uses its own `stringmatchlen()`: a Java glob `*`
   does not cross a `/` and the `\` escape and the `[^...]` class differ. The `PSUBSCRIBE` matching and the
   `PUBSUB CHANNELS <pattern>` filter share that matcher, so the divergence is pre-existing and out of the scope of
   the `P` group findings.
4. **PFADD now serializes the HLL on every call.** The 84.2 fix compares the serialized state instead of the
   estimate, so a PFADD pays one HLL serialization (8 KB for a dense HLL) even when nothing changed. Redis compares
   the sparse register set instead. The cost is O(m) per PFADD, in line with the compression cost that Velo already
   pays for every saved HLL.
5. **A pre-existing test isolation flake in `io.velo.persist.OneSlotTest.'test get short type'`.** During the
   regression run of the fix phase (`--tests "io.velo.command.*" --tests "io.velo.SocketInspectorTest" --tests
   "io.velo.repl.*" --tests "io.velo.persist.*"`, 908 tests) that test failed once with
   `getShortType(...) == -1`; it passed when re-run alone, and the same selection at HEAD and at the reviewed commit
   `3d8ee532` (a separate worktree) is green. The test shares `/tmp/velo-data/test-persist` with the other persist
   tests, so the failure is a state/order flake of the persist test fixtures, not a regression of this round (no
   persist code was touched).
6. **The HLL cardinality estimate still differs from Redis.** Velo uses `com.github.prasanthj:hyperloglog` and Redis
   uses its own implementation, so `PFCOUNT` returns a slightly different number for the same elements
   (2467 vs 2453 for the 2471 elements of the 84.2 scenario). This is expected and is not a bug.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 84.1 | PFMERGE across workers touches the destination slot from a foreign thread and never answers the client | High | Concurrency | CONFIRMED (reproduced) | fixed |
| 84.2 | PFADD uses the estimated cardinality as the change detector → the altered HLL is dropped | Medium | Correctness | CONFIRMED (reproduced) | fixed |
| 84.3 | PSUBSCRIBE / PUNSUBSCRIBE answer with "subscribe" / "unsubscribe" | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.4 | PUBSUB NUMSUB counts the pattern subscribers | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.5 | PUBSUB CHANNELS lists the pattern subscriptions | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.6 | PUBSUB NUMPAT answers a null bulk reply | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.7 | The wrong-type path throws a bare RuntimeException → `-ERR wrong type` | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.8 | `PUBSUB <unknown>` / `PUBSUB NUMSUB` (no channel) diverge | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
| 84.9 | PERSIST writes the value back with the old sequence number | Low | Correctness | CONFIRMED (reproduced) | fixed |
| 84.10 | The PEXPIRE / PEXPIREAT / PSETEX error texts diverge from Redis | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed |
