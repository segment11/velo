# Bug 72 — BGroup Commands Review (Round 1)

**Module:** `io.velo.command.BGroup` (BITCOUNT / BITPOS / BITOP / BITFIELD / BF.* / BGSAVE / BLPOP / BRPOP / BLMOVE / BLMPOP / BRPOPLPUSH)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete (2026-10-19) — 9 findings fixed in 9 commits, 72.4 retracted (no code change)
**Review branch:** `review/bgroup`
**Reviewed commit:** `330683c45009aec8bab0f626aff6b61b39ce9767`

> Delta declaration: round 1, no prior round on this module per STATE.md. Adjacent prior work that must NOT be re-reported: `bug_41_list_module_review_round_1.md` — BLPOP/BRPOP/BLMOVE/BRPOPLPUSH timeout semantics (double parsing, timeout-0 = block indefinitely, non-finite rejection) were fixed and are correct at this commit; LMPOP/BLMPOP "not dispatched" (41.5) has since been implemented. Both excluded here.

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/BGroup.java —` full file: static CommandRegistry block (25-186), `parseSlots()` (206-278), `handle()` (291-343), `bitcount()` (345-412), `bitpos()` (414-492), `bf()` (494-539), `bfAdd()` (541-592), `bfCard()` (594-610), `bfExists()` (612-664), `bfInfo()` (666-719), `bfInsert()` (721-840), `bfLoadchunk()` (842-870), `bfScandump()` (872-910), `bfReserve()` (912-972), `blpop()` (975-1105), `blmpop()` (1107-1224)
- `src/main/java/io/velo/type/RedisBitSet.java —` class (1-90), `set()`/`get()` MSB-first logic (59-89)
- `src/main/java/io/velo/command/BlockingList.java —` full file: `Inner` per-thread map (118-151), `getInner()` (107-116), `setReplyIfBlockingListExist()` (220-375), `addOne`/`removeOne` (159-171), `removeBySocket()` (430-451)
- `src/main/java/io/velo/command/LGroup.java —` `getRedisList()`/`saveRedisList()` (403-433), `lpush` region with `setReplyIfBlockingListExist` call (840-880)
- `src/main/java/io/velo/command/MGroup.java —` `mget()` cross-worker pattern (170-230) — reference for the framework's expected `isCrossRequestWorker` contract
- `src/main/java/io/velo/BaseCommand.java —` `getCv()` (734-777), `getValueBytesByCv()` (779-836), `get()` (837-868)
- `src/main/java/io/velo/RequestHandler.java —` dispatch + exception mapping (520-580)
- `src/main/java/io/velo/MultiWorkerServer.java —` cross-worker detection and worker routing (540-640)
- `src/main/java/io/velo/persist/OneSlot.java —` `asyncExecute()`/`asyncCall()` inline-vs-submit (695-745), `checkCurrentThreadId()` (1602-1603), `get()` (starts ~1595)
- `src/main/java/io/velo/command/IndexStartEndReset.java —` `reset()` (29-53)
- `src/main/java/io/velo/type/RedisBF.java —` constants (39-49), constructors (81-103), `capacity()`/`itemInserted()` (113-120)
- `src/main/java/io/velo/decode/Request.java —` `getSingleSlot()` (208-220)
- `src/test/groovy/io/velo/command/BGroupTest.groovy —` 'test bitcount' (136-209), 'test bitpos' (211-308), 'test bf reserve' (659-711), 'test bf reserve rejects non-positive expansion' (713-730), 'test blpop' (751-788+)
- `doc/redis_command_support.md —` bitmap + bloom filter sections (1-20)
- `doc/design/04_command_processing_design.md —` pipeline steps incl. cross-worker detection, Thread Handoff
- `doc/design/11_multithreading_design.md —` @ThreadNeedLocal / slot worker ownership
- `doc/bug_reviews/bug_41_list_module_review_round_1.md —` prior round exclusions

Reference docs (ground truth):

- https://redis.io/commands/bitpos/ (Redis 7.2 semantics: bit convention, missing-key case, all-ones clear-bit case)
- https://redis.io/commands/bitcount/ (BIT unit since 7.0, bit-index convention shared with GETBIT/SETBIT)
- https://redis.io/commands/bitop/ (arity -4, single-source valid, NOT unary, zero-padding rules)
- https://redis.io/commands/bf.reserve/ (syntax `BF.RESERVE key error_rate capacity [...]`)
- https://redis.io/commands/bf.loadchunk/ (syntax `BF.LOADCHUNK key iterator data` = exactly 4 tokens)

Environment notes for verification:

- Default `slotWorkers=1` (`src/main/resources/velo.properties:10`); concurrency findings 72.3/72.4/72.5 require `slotWorkers > 1`, a supported production config (design doc 11).
- Unit tests bypass slot data via `byPassGetSet` (`InMemoryGetSet`) and `localPersist.fixSlotThreadId(slot, currentThreadId)` (BGroupTest.groovy:762), which masks all three concurrency findings.

## Finding 1 (Bug 72.1): BITOP / BITFIELD / BITFIELD_RO advertised by registry but unimplemented — nil reply instead of executing or erroring

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced + inspection)` — **fixed** in `503930f4`, see Fix Log

### Cited files / lines

```java
// BGroup.java:42-57 — registry advertises bitfield (-2) and bitop (-4) as existing commands
CommandRegistry.register(new CommandEntry("bitfield", -2, ...));
CommandRegistry.register(new CommandEntry("bitop", -4, ...));

// BGroup.java:291-343 — handle() has no "bitfield"/"bitfield_ro"/"bitop" branch; falls through to:
return NilReply.INSTANCE;   // BGroup.java:342

// BGroup.java:217-219 — bitop parseSlots demands >= 2 source keys
if ("bitop".equals(cmd)) {
    if (data.length < 5) {
        return slotWithKeyHashList;
    }

// BGroup.java:208 — bitfield_ro parsed for slots but never registered nor handled
if ("bitcount".equals(cmd) || "bitfield".equals(cmd) || "bitfield_ro".equals(cmd) || ...
```

### Root cause

`BITFIELD` and `BITOP` are registered in `CommandRegistry` (so `COMMAND`/`COMMAND DOCS` reports them as available and clients trust them), but `handle()` has no dispatch branch — both fall through to the default `NilReply.INSTANCE`. Additionally `bitfield_ro` has a `parseSlots` branch but no registry entry (dead path → "unknown command"), and the `bitop` slot parser requires `data.length >= 5`, i.e. at least two source keys, while Redis arity is -4 (`BITOP AND dest src` with a single source is valid; `NOT` is unary).

### Trigger / impact

`bitop and d s` after `set s x` → returns `(nil)` instead of `(integer) 1`; `bitfield s get i8 0` → `(nil)` instead of `(integer) 120`. BITOP is a write command: a client sees a nil (non-error) reply and may believe the destination was written — silent data loss illusion. `doc/redis_command_support.md:6-8` marks all three `[x]` (documented unsupported), so full implementation may be out of scope; the protocol violation is advertising them via the registry while silently returning nil instead of `ERR unknown command`.

### Suggested fix direction

Either (a) implement bitop (byte-wise AND/OR/XOR/NOT with zero-padding per doc) and gate bitfield behind the same decision, or (b) minimum-compat fix: remove the registry entries (and the dead `bitfield_ro`/`bitfield` parseSlots branches) so clients get `ERR unknown command`, and fix the `data.length < 5` check to `< 4` if implementation lands. Keep one `def 'test bitop'()` method per repo test doc.

## Finding 2 (Bug 72.2): bf.loadchunk trusts client-controlled length prefix for allocation — memory-exhaustion DoS

**Severity:** High
**Class:** Security
**Status:** `CONFIRMED (reproduced)` — **fixed** in `8642442d`, see Fix Log

### Cited files / lines

```java
// BGroup.java:861-866 — bfLoadchunk()
var dumpBytes = data[3];
var buffer = ByteBuffer.wrap(dumpBytes);
var encodedLength = buffer.getInt();       // client-controlled, unvalidated
var encoded = new byte[encodedLength];     // allocation sized by client
Zstd.decompressByteArray(encoded, 0, encodedLength, dumpBytes, 4, dumpBytes.length - 4);
```

```java
// RequestHandler.java:575-580 — generic guard catches Exception only, not OutOfMemoryError
} catch (Exception e) {
    log.error("Request handle error.", e);
    errorReplyCountTotal++;
    return new ErrorReply(e.getMessage());
}
```

### Root cause

The 4-byte little-endian header written by `bfScandump` (BGroup.java:902-903) is re-read on load and used directly as the destination array size without any sanity bound. Any connected client can send `bf.loadchunk k 0 <4-byte-len><garbage>` with `len` up to `Integer.MAX_VALUE`, forcing `new byte[len]` before zstd ever validates the payload. Values within heap limits (e.g. 512MB each) allocate real zeroed arrays; a handful of concurrent requests exhausts the heap. `NegativeArraySizeException` is caught by the generic Exception handler, but the `OutOfMemoryError` variant is an `Error` and bypasses it.

### Trigger / impact

Single small request → multi-hundred-MB allocation; N concurrent requests → heap exhaustion / OOM kill / unresponsive workers. No ACL category distinguishes loadchunk from other bloom writes.

### Suggested fix direction

Validate before allocating: `encodedLength` must be `> 0` and bounded (e.g. `<= CompressedValue.VALUE_MAX_LENGTH` or a module constant), and ideally cross-checked against the zstd frame's decompressed-size hint (`Zstd.getFrameContentSize`); reject with a Redis-style error. TDD: failing test with a crafted huge/zero/negative length first.

## Finding 3 (Bug 72.3): Multi-key BLPOP/BRPOP register blocking waiters on the wrong worker thread — missed wakeups

**Severity:** High (when `slotWorkers > 1`)
**Class:** Concurrency
**Status:** `CONFIRMED (reproduced)` — **fixed** in `bc55bcad`, see Fix Log

### Cited files / lines

```java
// BGroup.java:1053-1057 — blpop() multi-key path: waiters registered from the FIRST key's worker
var one = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(finalPromise, socket, isLeft, System.currentTimeMillis(), null);
for (var key : keys) {
    BlockingList.addOne(key, one);          // addOne uses getInner() == CURRENT thread's map
}
// BGroup.java:1204-1207 — blmpop() same pattern

// BlockingList.java:107-116 + 126 — waiters live in a per-thread map (@ThreadNeedLocal)
private static Inner getInner() { /* matches by Thread.currentThread().threadId() */ }
final HashMap<String, List<PromiseWithLeftOrRightAndCreatedTime>> blockingListPromisesByKey = new HashMap<>();

// BlockingList.java:220-224 — push side looks up ONLY the pushing thread's map
public static byte[][] setReplyIfBlockingListExist(String key, ...) {
    var inner = getInner();                  // LPUSH runs on slot(key) % workers
    var list = inner.blockingListPromisesByKey.get(key);

// MultiWorkerServer.java:603 — requests routed by firstSlot: i = firstSlot % requestHandlerArray.length
```

### Root cause

A request executes on `slot(firstKey) % workers` (MultiWorkerServer.java:603). `blpop()` registers the waiting promise for **all** keys into the current (first-key) worker's `Inner` map. An `LPUSH` to a later key is routed to that key's own worker and looks up **that** worker's map — where no waiter exists. The waiter is invisible to the push, so the element is stored in the list and the blocked client is never woken. BGroup also never reads the framework's `isCrossRequestWorker` flag that other groups honor (contrast MGroup.java:181).

### Trigger / impact

`slotWorkers > 1`; client A: `BLPOP k1 k2 5` (empty lists, different workers). Within the timeout another client `LPUSH k2 v` → A still blocks the full 5s and gets nil; `v` sits in `k2`. Silent delayed/lost delivery under load. With `slotWorkers == 1` all keys share one thread and the bug cannot fire (which is why BGroupTest passes).

### Suggested fix direction

Register each key's waiter on the key's owning worker (e.g. via `oneSlot(slot).asyncExecute(() -> BlockingList.addOne(key, one))`), or make BlockingList worker-aware for cross-worker keys. Re-check emptiness after registration (see also 72.4's race note). Needs a multi-threaded test harness; at minimum a two-thread test driving `addOne` from thread T1 and `setReplyIfBlockingListExist` from T2.

## Finding 4 (Bug 72.4): ~~BLPOP multi-key path blocks the slot worker event loop via `Promise.getResult()` — cross-worker circular deadlock~~

**Severity:** High (when `slotWorkers > 1`)
**Class:** Concurrency
**Status:** `RETRACTED` — retracted by author 2026-10-19 after independently verifying the reviewer's refutation (see Response section); evidence kept, do not re-report

> **Retraction note (author):** The central premise is false. `javap` on `io/activej/promise/AbstractPromise.class` (activej-promise-6.0-beta2) shows `getResult()` is a plain field read (`result != PROMISE_NOT_SET ? result : null`) — non-blocking, returns `null` for incomplete promises. No thread parking → no circular wait → no deadlock. What remains of this finding is already covered by 72.3 plus the reviewer's "Additional observations": across workers the loop at BGroup.java:1032-1045 always reads `otherValueBytes == null` ("remote list is empty"), so the `isSet` decision is bogus cross-worker, and delivery of a non-empty remote list happens only later via the `whenComplete` callback. The `Promises.all(...).whenComplete(...)` recommendation survives as the fix shape for 72.3, not as a deadlock fix.

### Cited files / lines

```java
// BGroup.java:1030-1045 — blpop() multi-key immediate check
var p = oneSlot.asyncCall(() -> { ... });   // posted to the OTHER key's worker when cross-worker
// wait until this promise complete
var otherValueBytes = p.whenComplete((r, e) -> { ... }).getResult();  // getResult() BLOCKS the current thread
```

```java
// OneSlot.java:726-745 — asyncCall submits to the other worker's eventloop (not inline) when cross-worker
if (Reactor.getCurrentReactorOrNull() != null) {
    return Promise.ofFuture(slotWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
}

// Contrast — MGroup.java:213-229 (mget) aggregates cross-worker calls NON-blocking:
Promises.all(promises).whenComplete((r, e) -> { ... });
```

### Root cause

`handle()` runs inside a slot worker event loop; `Promise.getResult()` parks that thread until another worker completes the check. While parked, this worker processes nothing else (all its slots stall). If two workers are each parked waiting for the other — client A on worker T1: `BLPOP kA kB` (kB owned by T2), client B on worker T2: `BLPOP kD kE` (kE owned by T1), both lists empty — T1 waits for T2's queued task and T2 waits for T1's: circular wait, both event loops dead. The codebase's own cross-worker pattern (MGroup.mget) is non-blocking (`Promises.all(...).whenComplete`); BGroup deviates. Secondary: there is also a check-then-register race (empty-check on T2 for k2 completes, then push lands on k2 before T1 executes `BlockingList.addOne(k2, one)` → that push finds no waiter → lost wakeup even after fixing 72.3's thread placement, unless emptiness is re-checked after registration).

### Trigger / impact

`slotWorkers > 1`, two concurrent multi-key blocking pops with reversed key ownership. Both workers freeze; every slot they own stops serving. Even without the deadlock, any cross-worker BLPOP stalls its worker for the duration of the remote check.

### Suggested fix direction

Replace the blocking loop with per-key promises + `Promises.all(...).whenComplete(...)` (MGroup.mget pattern): first non-null result wins (`SettablePromise.trySet`-style single completion), then register blocking waiters and re-verify emptiness. Recipe evidence can be a two-thread jstack repro or inspection contrast with MGroup.

## Finding 5 (Bug 72.5): BLMPOP ignores `isCrossRequestWorker` — direct cross-worker slot access throws IllegalStateException

**Severity:** Medium (when `slotWorkers > 1`)
**Class:** Concurrency
**Status:** `CONFIRMED (reproduced)` — **fixed** in `4cffebaf`, see Fix Log

### Cited files / lines

```java
// BGroup.java:1170-1191 — blmpop() immediate path touches every key directly from the first key's worker
for (int i = 0; i < numKeys; i++) {
    var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
    var cv = getCv(slotWithKeyHash);                      // OneSlot.get -> checkCurrentThreadId()
    ...
    var rl = LGroup.getRedisList(slotWithKeyHash, this);  // read on foreign slot
    ...
    LGroup.saveRedisList(rl, slotWithKeyHash, this, dictMap);  // WRITE on foreign slot

// OneSlot.java:1602-1603
if (threadId != threadIdProtectedForSafe) {
    throw new IllegalStateException("Thread id not match, thread id=" + threadId + ", thread id protected for safe=" + threadIdProtectedForSafe);
}

// Contrast — BGroup.java:1019-1029 (blpop) and MGroup.java:181-229 (mget) both branch/dipatch per-slot
```

### Root cause

`blmpop()` has no `!isCrossRequestWorker` guard (the framework sets this flag in MultiWorkerServer.java:554-571 exactly for this purpose; PGroup/RGroup/GGroup/MGroup honor it). For a key owned by another worker, `getCv` → `OneSlot.get` → `checkCurrentThreadId()` throws `IllegalStateException`, which the generic handler converts to `ErrorReply(e.getMessage())` — the client receives an internal thread-id message and the command fails.

### Trigger / impact

`slotWorkers > 1`; `BLMPOP 1 2 k1 k2 LEFT` where `slot(k2) % workers != slot(k1) % workers` and `k1` empty/missing → error reply `Thread id not match, ...` (and the loop aborts before ever checking `k2`'s data). Same-worker multi-key (flag false) and single-key cases are safe, and tests use `byPassGetSet` so they never see it.

### Suggested fix direction

Mirror blpop/mget: when `isCrossRequestWorker`, group keys by owning slot and use `oneSlot.asyncCall(...)` for the check/pop/save, or reject cross-worker key sets explicitly at parse time with a documented error. Failing test: prepare LocalPersist with two slots pinned to two different thread ids and execute cross-slot blmpop.

## Finding 6 (Bug 72.6): BITCOUNT with BIT unit counts with LSB-first bit order — Redis numbers bits MSB-first

**Severity:** Medium
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — **fixed** in `33a698e0`, see Fix Log

### Cited files / lines

```java
// BGroup.java:395-402 — bitcount() BIT-mode uses java.util.BitSet (little-endian: bit 0 = LSB of byte 0)
var bitset = BitSet.valueOf(valueBytes);
if (isIndexUseBit) {
    for (int i = startEndWith.start(); i <= startEndWith.end(); i++) {
        if (bitset.get(i)) { count++; }
    }
}

// Contrast — BGroup.java:473 (bitpos), SGroup.java:1016 (setbit), GGroup.java:264 (getbit) all use MSB-first:
var bs = new RedisBitSet(valueBytes);
// RedisBitSet.java:64 — var oldBit = valueBytes[byteIndex] >> (7 - bitIndex) & 1;   // bit 0 = MSB of byte 0
```

### Root cause

Redis bit convention: "the first byte's most significant bit is at position 0" (redis.io/commands/bitpos, shared by GETBIT/SETBIT/BITCOUNT/BITPOS). `bitcount()`'s BIT-mode loop indexes `java.util.BitSet.valueOf`, which numbers bits LSB-first per byte. Counts are equal only when the covered bit pattern is symmetric within each byte. BYTE mode is unaffected (counting a whole-byte range is order-independent). The existing test value `'foobar'` happens to be bit-palindromic in every byte the range covers (`0x66` reversed = `0x66`; `0x62`/`0x6f` ranges coincide), so `bitcount a 5 30 bit == 17` passes both ways and cannot discriminate.

### Trigger / impact

`SET k "\x80"` then `BITCOUNT k 0 0 BIT` → Redis returns 1 (MSB of byte 0), Velo returns 0 (LSB read). Any asymmetric bitmap with sub-byte BIT ranges gets wrong counts; BITCOUNT disagrees with SETBIT/GETBIT/BITPOS on the same key.

### Suggested fix direction

Use `RedisBitSet` (as bitpos already does) or index `bitset.get(i)` through an MSB-first mapping; add a discriminating test (`\x80` byte, `0 0 bit` → expect 1; also `\x01 0 0 bit` → 0, and full-range invariants).

## Finding 7 (Bug 72.7): BITPOS on missing key searching for 0 returns -1 — Redis returns 0

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** in `92718814`, see Fix Log

### Cited files / lines

```java
// BGroup.java:455-459 — bitpos()
var cv = getCv(slotWithKeyHash);
if (cv == null) {
    return new IntegerReply(-1);        // applies to both bit=1 and bit=0
}
```

### Root cause

Redis doc (bitpos): "If the key does not exist, it is treated as an infinite array of zero bits: searching for a clear bit (0) returns 0, while searching for a set bit (1) returns -1." Velo returns -1 unconditionally for missing keys.

### Trigger / impact

`BITPOS missing 0` → Velo -1, Redis 0. Clients using BITPOS-0 as "first free slot" logic misbehave on cold keys. Existing test only asserts the bit=1 case (BGroupTest.groovy:221-224).

### Suggested fix direction

`if (cv == null) return isBit1 ? integerReply(-1) : integerReply(0);` — note: an *existing* empty string "" must still return -1 for both (that path already works via the zero-length range check).

## Finding 8 (Bug 72.8): BITPOS searching for a clear bit in an all-ones string returns -1 — Redis returns the first bit past the string (when no end / start-only)

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** in `127ff5c4`, see Fix Log

### Cited files / lines

```java
// BGroup.java:468-491 — bitpos(): not-found always -1, no distinction for bit=0 without explicit end
var startEndWith = IndexStartEndReset.reset(start, end, canIndexValueBytesLength);
if (!startEndWith.valid()) {
    return new IntegerReply(-1);
}
... loop ...
return new IntegerReply(pos);           // pos stays -1 when not found
```

### Root cause

Redis doc (bitpos): for clear-bit search, "the function considers the right of the string as padded with zeros if you look for clear bits and specify no range or the _start_ argument **only**" — `SET k "\xff\xff\xff"; BITPOS k 0` → 24. When **both** start and end are given and no clear bit is in range, Redis returns -1 (Velo is already correct for that sub-case). Velo returns -1 for all not-found clear-bit searches, including the padded cases (no range: `data.length == 3`; start-only: `data.length == 4` where end defaults to -1).

### Trigger / impact

`SET k "\xff"` → `BITPOS k 0` → Velo -1, Redis 8. `BITPOS k 0 0` (start-only) → Velo -1, Redis 8. Only differs when every bit in scope is 1.

### Suggested fix direction

Track `endGiven` (true only when `data.length >= 5`); when `!isBit1 && pos == -1 && !endGiven`, return `valueBytes.length * 8` (byte mode; for BIT mode the padding boundary is still the string end per Redis). Differential-testable against Redis 7.2.

## Finding 9 (Bug 72.9): bf.reserve / bf.insert don't validate capacity — Guava's raw IllegalArgumentException message leaks to clients

**Severity:** Low
**Class:** Correctness
**Status:** `REFINED` — **fixed** in `4e09e22d`, see Fix Log

### Cited files / lines

```java
// BGroup.java:930-935 — bfReserve(): capacity parsed but never validated
int initCapacity;
try {
    initCapacity = Integer.parseInt(new String(data[3]));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}
// ... no initCapacity <= 0 check; same in bfInsert() CAPACITY at BGroup.java:751-761

// RedisBF.java:101 — Guava throws deep inside construction
one.filter = BloomFilter.create(STRING_FUNNEL, initCapacity, initFpp);

// RequestHandler.java:575-580 — generic catch surfaces the raw Java message
return new ErrorReply(e.getMessage());
```

### Root cause

> **Corrected per REFINED verdict (author-verified):** Guava 33.4.8-jre bytecode shows `checkArgument(expectedInsertions >= 0, "Expected insertions (%s) must be >= 0")` — capacity **0 is accepted** by Guava and Velo replies OK (RedisBloom errors `capacity must be > 0`); only **negative** capacity throws and leaks the raw message. Original text below overstated the leak.

`bf.reserve k 0.01 -5` passes Velo's own validation; Guava's `BloomFilter.create` then throws `IllegalArgumentException("Expected insertions (-5) must be >= 0")`, which reaches the client as an error reply with Java-specific text instead of a RedisBloom-style upfront validation error. Separately, `bf.reserve k 0.01 0` is silently accepted (OK) where RedisBloom errors — a wrong-value acceptance, not a leaked exception. Existing tests cover bad-fpp and non-numeric capacity but not capacity <= 0 (BGroupTest.groovy:700-710).

### Trigger / impact

`bf.reserve k 0.01 -5` / `bf.insert k CAPACITY -1 ITEMS x` → client sees `Expected insertions (-5) must be >= 0`-style messages; error contract differs from RedisBloom, and message stability is tied to Guava internals. `bf.reserve k 0.01 0` / `bf.insert k CAPACITY 0 ITEMS x` → OK / normal reply while RedisBloom rejects with `capacity must be > 0`.

### Suggested fix direction

Validate `initCapacity > 0` in both bfReserve and bfInsert right after parsing, returning a fixed Redis-style error (e.g. `capacity must be > 0`), before constructing RedisBF.

## Finding 10 (Bug 72.10): bf.loadchunk / bf.reserve / bf.insert registry arity values don't match their real argument grammars — COMMAND metadata is wrong

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (inspection)` — **fixed** in `b11e003a`, see Fix Log

### Cited files / lines

```java
// BGroup.java:146-153 — arity 3
CommandRegistry.register(new CommandEntry("bf.loadchunk", 3, ...));
// bfLoadchunk() actually requires exactly 4 args: BGroup.java:843-845  if (data.length != 4)

// BGroup.java:170-177 — arity -3
CommandRegistry.register(new CommandEntry("bf.reserve", -3, ...));
// bfReserve() requires >= 4 args: BGroup.java:913-915  if (data.length < 4)

// BGroup.java:138-145 — arity -2
CommandRegistry.register(new CommandEntry("bf.insert", -2, ...));
// bfInsert() requires >= 4 args: BGroup.java:722-724  if (data.length < 4)
```

### Root cause

RedisBloom grammar: `BF.LOADCHUNK key iterator data` = exactly 4 tokens (arity 4); `BF.RESERVE key error_rate capacity [...]` = min 4 tokens (arity -4); `BF.INSERT key [options] ITEMS item [item...]` = min 4 tokens (arity -4). The registered arities (3 / -3 / -2) disagree with both the real handlers and the reference grammar; the values are served to clients via the dyn `command` implementation (dyn/src/io/velo/command/CommandCommand.groovy reads CommandRegistry). Arity is not enforced at runtime, so the inconsistency is invisible to tests.

### Trigger / impact

`COMMAND` / `COMMAND DOCS` output reports wrong arity for the three bloom commands; clients that build validators from COMMAND metadata (redis-cli hints, cluster proxies) mis-handle them. The previous commit (`330683c4`) fixed exactly this class of metadata drift for sort_ro/sentinel.

### Suggested fix direction

Correct the three CommandEntry arities (4 / -4 / -4); assert against `redis.io/commands/bf.*` syntax. Extend the existing registry metadata test (if any) or add one that compares CommandRegistry arity with a table for B commands.

## Notes / non-findings

- **bgsave is a no-op returning OK** (BGroup.java:296-299): consistent with the continuous-persistence design; `save` (SGroup.java:398-401) is also a no-op; `lastBgSaveMillis` only feeds `lastsave`. Intentional — not reported.
- **BLPOP/BRPOP timeout semantics** (double parse, timeout-0 = indefinite, non-finite rejection, "timeout is not a float or out of range") — fixed by bug_41, verified correct at this commit; not re-reported.
- **blmpop argument grammar** (timeout/numkeys/direction/COUNT validation and reply shape) — inspected, matches Redis; immediate-path WRONG_TYPE ordering (checked per key, skipping missing/empty) also matches Redis scanning order.
- **BlockingList multi-waiter value distribution** (`fromLeftToRight` nulling, lazy removal of completed promises at BlockingList.java:243-247 before consuming values) — correct; stale waiters left on other keys after a wakeup are cleaned lazily without value loss.
- **bitcount/bitpos BYTE-mode range normalization** via `IndexStartEndReset.reset` — negative indexes, clamping, and empty-range → 0/-1 match Redis (BITCOUNT missing key → 0 ✓).
- **bitpos on an existing empty string** → -1 for both bits — matches Redis.
- **bf.scandump/bf.loadchunk one-shot iterator protocol** — self-consistent pair (0 = dump-all/complete); not wire-compatible with real RedisBloom chunk payloads, accepted as a Velo-internal format.
- **bf.info on missing key returns empty array** where RedisBloom errors — minor deviation, not separately reported (low impact, defensible choice).
- **bf.insert BF_ALREADY_EXISTS-vs-WRONGTYPE precedence** when options given on an existing non-bloom key (BGroup.java:810-817 checks `needCreateNew` first) — edge case, noted only.
- **Registry `bitfield_ro` missing entry** while parseSlots references it — folded into 72.1.
- **Existing test palindromic coincidence**: `bitcount a 5 30 bit == 17` (BGroupTest.groovy:183-186) passes under both endiannesses — documented here so future rounds don't cite the green test as proof of correctness for 72.6.

## Reviewer checklist (for AI agent 2)

- Finding 72.1: static — confirm no `bitfield`/`bitop` branch in `BGroup.handle()` and no handler in dyn/src or src/main/groovy; `redis-cli bitop and d s` after `set s x` returns nil. Check `doc/redis_command_support.md` `[x]` rows and decide implement-vs-unregister direction.
- Finding 72.2: Spock temp spec — craft `bf.loadchunk k 0 <4-byte LE int 0x20000000><junk>` via `bGroup.execute(...)`, observe huge allocation/error leak (use a bounded value like 1<<20 in test to prove the length is used unvalidated; assert an error reply is *not* produced by Velo's own validation since none exists).
- Finding 72.3: two-thread test — `BlockingList.initBySlotWorkerEventloopArray` with 2 eventloops; register a waiter for key "k" from worker-1 thread, then from worker-2 thread call `setReplyIfBlockingListExist("k", true, [v])` and assert it returns null (waiter invisible). Inspection fallback: `getInner()` thread-id matching vs MultiWorkerServer.java:603 routing.
- Finding 72.4: inspection — `Promise.getResult()` is a blocking call invoked inside an eventloop callback (BGroup.java:1045); jstack repro: two threads cross-`asyncCall` + `getResult` mutually; contrast MGroup.java `Promises.all(...).whenComplete`.
- Finding 72.5: Spock — prepare LocalPersist with `slotWorkers=2`-style setup: `fixSlotThreadId(slotA, T1)` and `fixSlotThreadId(slotB, T2)`, execute `blmpop 1 2 kA kB LEFT` from T1 with kA missing → expect IllegalStateException / error reply containing "Thread id not match".
- Finding 72.6: Spock — put type-string cv with value bytes `[0x80]`; `bitcount a 0 0 bit` → currently 0, Redis says 1. Also `[0x01]` full-range BIT → 1 both ways (control).
- Finding 72.7: Spock — remove key; `bitpos a 0` → currently -1, Redis says 0. Control: `bitpos a 1` → -1 ✓.
- Finding 72.8: Spock — value `[0xff,0xff,0xff]`; `bitpos a 0` → currently -1, Redis says 24; `bitpos a 0 0` → 24; control `bitpos a 0 0 -1` → -1 (already correct).
- Finding 72.9: Spock — `bf.reserve a 0.01 0` → currently ErrorReply with Guava message; expected fixed Velo-side error text. Same for `bf.insert a capacity 0 items x`.
- Finding 72.10: run the dyn `command` implementation (or inspect CommandRegistry) — bf.loadchunk=3 (should be 4), bf.reserve=-3 (-4), bf.insert=-2 (-4); cross-check syntax pages cited in Scope.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (`src/test/groovy/io/velo/command/Bug72ReviewTempSpec.groovy`, 10 cases,
`./gradlew :test --tests "io.velo.command.Bug72ReviewTempSpec"` → `BUILD SUCCESSFUL`, 10 tests, 0 failed)
+ Redis 7.2 doc ground truth (redis.io, fetched 2026-10-19) + ActiveJ 6.0-beta2 bytecode inspection
+ static inspection of the cited lines.

Full transcripts: `doc/bug_reviews/logs/20261019_153643_verify_bug_72_bgroup_commands_review_round_1.log`
(grep `72\.` for every printed transcript line).

The temp spec is a reviewer artifact — agent 1 must rebuild the confirmed repros as permanent regression
tests inside the existing `def 'test ...'()` methods of `BGroupTest.groovy` (one method per command per
`doc/how_to_write_high_coverage_test_cases_for_commands.md`) and must **not** commit
`Bug72ReviewTempSpec`.

| Finding | Verdict | Evidence |
|---|---|---|
| 72.1 | `CONFIRMED (reproduced + inspection)` | `bitop and d s` → `NilReply`, `bitfield s get i8 0` → `NilReply`, `bitfield_ro ...` → `NilReply`; `CommandRegistry` arity for `bitfield_ro` = `null` (unregistered). No `bitop`/`bitfield` branch in `BGroup.handle()` (291-343) and no handler anywhere in `src/`, `src/main/groovy`, `dyn/src` (grep). `doc/redis_command_support.md:6-8` marks all three `[x]` (unsupported) — the doc/registry contradiction is real. |
| 72.2 | `CONFIRMED (reproduced)` | 20-byte crafted request `bf.loadchunk a 0 <int=100000000><junk>` → heap delta **98MB during the call** before zstd validates anything (`thrown=ZstdException: Unknown frame descriptor`). Negative prefix → `java.lang.NegativeArraySizeException: -1` escapes the command; zero prefix → `ZstdException: Src size is incorrect`. No Velo-side bound exists, exactly as cited. |
| 72.3 | `CONFIRMED (reproduced)` | Two real event loops (`keepAlive(true)`) + `BlockingList.initBySlotWorkerEventloopArray([loop1, loop2])`: register waiter on worker-1 → `blockingClientCount=1`; push on worker-2 → `setReplyLPushIfBlockingListExist` returns **`null`** (no waiter visible). Same-worker control on worker-1 returns `byte[0][]` (value delivered) — so the partition is by thread, not a broken call. Routing cited by the report is confirmed by `MultiWorkerServer.java:603` (`i = firstSlot % requestHandlerArray.length`) and `Request.getSingleSlot()` returning the **first** slot. |
| 72.4 | `REFUTED` | The premise is wrong: ActiveJ `AbstractPromise.getResult()` is **non-blocking**. Bytecode of `activej-promise-6.0-beta2` `AbstractPromise.getResult()` returns `result` or `null` immediately — no wait, no park. Live repro with 2 slot workers bound to 2 event loops: worker-1 `oneSlot(1).asyncCall(...).getResult()` returned `null` immediately while the remote value was `'checked-by-worker2'`; the mutual (deadlock-shaped) case **completed within 5s** (`worker-1 state=RUNNABLE`, `blocked at = null`). Nothing freezes. See "Additional observations" for the weaker real defect this exposes. |
| 72.5 | `CONFIRMED (reproduced)` | `LocalPersistTest.prepareLocalPersist(2, 2)` + `fixSlotThreadId(0, myTid)` / `fixSlotThreadId(1, myTid+100000)`, keys pinned to different slots (`kA1`→slot 0, `kB1`→slot 1), **no** `byPassGetSet` so the real `OneSlot.get` path runs → `blmpop 0.1 2 kA1 kB1 left` throws `java.lang.IllegalStateException: Thread id not match, thread id=1, thread id protected for safe=100001`. Via `RequestHandler`'s generic `catch (Exception e)` this reaches the client as an error reply containing a Java thread-id message. |
| 72.6 | `CONFIRMED (reproduced)` | value `0x80`: `bitcount a 0 0 bit` → **0**, Redis → **1**; `bitcount a 1 1 bit` → 0 (Redis 0); `bitcount a 7 7 bit` → 1 (Redis 1). Internal contradiction also shown: `bitpos a 1 0 7 bit` → 0, i.e. `bitpos` (MSB-first, `RedisBitSet`) disagrees with `bitcount` (LSB-first, `java.util.BitSet`) on the same key. BYTE mode `bitcount a` → 1 ✓. Redis ground truth: "the first byte's most significant bit is at position 0 … The same bit position convention is followed by GETBIT and SETBIT" (redis.io/commands/bitpos), and BITCOUNT's BIT index is the same convention (redis.io/commands/bitcount). |
| 72.7 | `CONFIRMED (reproduced)` | missing key: `bitpos a 0` → **-1**, Redis → **0**; control `bitpos a 1` → -1 ✓. Ground truth quoted: "If the key does not exist, it is treated as an infinite array of zero bits: searching for a clear bit (0) returns 0, while searching for a set bit (1) returns -1." (redis.io/commands/bitpos, fetched 2026-10-19). |
| 72.8 | `CONFIRMED (reproduced)` | value `0xff 0xff 0xff`: `bitpos a 0` → **-1** (Redis 24), `bitpos a 0 0` → **-1** (Redis 24). Controls already correct: `bitpos a 0 0 2` → -1 and `bitpos a 0 0 5 bit` → -1, matching "The function considers the right of the string as padded with zeros … only [when] you … specify no range or the _start_ argument only". Note: `BITPOS key 0 0 BIT` (start-only + BIT) is **not** valid Redis syntax — Redis treats `bit` as `end` and errors; Velo does the same, so no fix needed there. |
| 72.9 | `REFINED` | Root cause (no `capacity` validation, Guava text leaks) is confirmed, but the cited trigger is wrong. `bf.reserve a 0.01 0` → **OK** (Guava accepts `>= 0`, Velo silently creates a 0-capacity filter — RedisBloom errors `ERR capacity must be > 0`); `bf.insert c capacity 0 items item0` → **MultiBulkReply** (no error at all). Negative capacity is what leaks: `bf.reserve b 0.01 -5` and `bf.insert d capacity -1 items item0` → `java.lang.IllegalArgumentException: Expected insertions (-5) must be >= 0` escaping the command. The fix must therefore validate `capacity > 0` (rejects 0 too) and return a fixed Velo/Redis-style error. |
| 72.10 | `CONFIRMED (inspection)` | Runtime dump of `CommandRegistry.all()`: `bf.loadchunk arity=3`, `bf.reserve arity=-3`, `bf.insert arity=-2` (control: `bf.scandump=3` ✓, `bitop=-4` ✓, `bitfield=-2` ✓, `bitcount=-2` ✓, `bitpos=-3` ✓). RedisBloom grammar (redis.io, fetched 2026-10-19): `BF.LOADCHUNK key iterator data` = exactly 4 tokens → arity 4; `BF.RESERVE key error_rate capacity [EXPANSION …] [NONSCALING]` = min 4 → -4; `BF.INSERT key [options] ITEMS item…` = min 4 → -4. Values are served to clients via `dyn/src/io/velo/command/CommandCommand.groovy:103` (`ten[1] = new IntegerReply(entry.arity())`). |

### Additional observations

- **72.4 — what is real underneath the refuted claim.** `Promise.getResult()` returning `null` for an
  incomplete promise means `blpop`'s cross-worker check at BGroup.java:1032-1045 always reads
  `otherValueBytes == null`, i.e. "this list is empty", for every key owned by another worker. The remote
  task still runs later and its `whenComplete` callback sets `finalPromise`, so the value is eventually
  delivered *after* the waiters were registered — the end-user symptom is the 72.3 missed wakeup, not a
  freeze. The code is still wrong (the loop's `isSet` decision and the "remote list is empty" assumption
  are bogus across workers, and same-worker behaviour depends on `asyncCall` having already completed
  inline). Recommendation: fold this into 72.3's fix — replace the `getResult()` loop with the
  `Promises.all(...).whenComplete(...)` pattern of `MGroup.mget()` (MGroup.java:213-229), which is
  non-blocking **and** actually reads the remote value. Do not open a separate ticket.
- **72.3 — the fix must also close the check-then-register window.** Even after the waiter is registered on
  the key's own worker, `BGroup.blpop()` checks emptiness on the other worker *before* registering on the
  first key's worker (BGroup.java:1006-1057). Re-checking emptiness after registration (on the owning
  worker) is required, otherwise the race the report mentions stays.
- **72.5 — `blpop` and `blmpop` disagree.** `blpop()` correctly dispatches per-slot via
  `oneSlot.asyncCall(...)` (BGroup.java:1019-1029), `blmpop()` does not. A single shared helper for
  "group keys by owning slot, fan out non-blocking, first non-empty wins" would close 72.3, 72.5 and the
  72.4 residue in one change — the fix agent should consider that shape.
- **72.2 — ACL note.** `bf.loadchunk` is registered `Set.of("write")` (BGroup.java:148) without
  `denyoom`, so no ACL category distinguishes it from other bloom writes; the only guard would be the
  length check itself.
- **72.6 — test-coincidence claim verified.** `bitcount a 5 30 bit == 17` is indeed bit-order agnostic
  (`0x66` is bit-palindromic; the covered byte prefix counts match under both mappings), so the existing
  green assertion in `BGroupTest.groovy` is not evidence of correctness. A discriminating regression test
  must use an asymmetric byte such as `0x80`/`0x01`.
- **Report path corrections (non-blocking).** `OneSlot.java` is at `src/main/java/io/velo/persist/OneSlot.java`,
  not `src/main/java/io/velo/OneSlot.java` (cited in Scope and in 72.4/72.5 excerpts). `CommandEntry`'s
  record component is `name`, not `command`. `MultiWorkerServer.java:603` is `i = firstSlot %
  requestHandlerArray.length` — correct as cited.
- **bf.loadchunk heap-growth detail:** the crafted 95MB allocation happened *before* any zstd frame check,
  so the fix can use `Zstd.getFrameContentSize(dumpBytes, 4)` as the authoritative bound and keep
  `encodedLength` only as a sanity cross-check.

### Recommended fix order (for the fix agent)

| # | Bug | Why this position |
|---|---|---|
| 1 | 72.2 | Highest severity and the cheapest, most self-contained fix; a regression test needs no thread setup. |
| 2 | 72.7 + 72.8 | Two-line `bitpos` changes on the same method, one test method, Redis-documented expectations. |
| 3 | 72.6 | Same command family as 72.7/72.8; reuse the discriminating value fixtures. |
| 4 | 72.9 | Two call sites (`bfReserve`, `bfInsert`), fixed error text, trivial tests. |
| 5 | 72.10 | Metadata-only; must be its own commit (previous commit `330683c4` did exactly this class of fix). |
| 6 | 72.3 (+ 72.4 residue) | Highest-effort change: needs worker-aware `BlockingList` registration plus the post-registration emptiness re-check; do it after the cheap wins so the concurrency work is reviewed on a stable base. |
| 7 | 72.5 | Reuse the 72.3 helper (group-by-slot fan-out); fix after 72.3 lands. |
| 8 | 72.1 | Needs a product decision first (implement BITOP/BITFIELD vs. unregister). Recommend unregistering the three registry entries + deleting the dead `bitfield`/`bitfield_ro`/`bitop` `parseSlots` branches so clients get `ERR unknown command`, matching `doc/redis_command_support.md`. If BITOP is to be implemented, that is its own plan, not a bug fix. |

Refuted items: **72.4** — no commit, no fix log row; keep the finding in this document so a future round
does not re-report a `getResult()` deadlock.

## Response (AI agent 1, round 1)

Per-finding response. All disagreements were settled by re-checking the codebase/dependencies, not opinion.

| Finding | Response | Basis |
|---|---|---|
| 72.1 | Agree — CONFIRMED | No new evidence needed. Accept reviewer's implement-vs-unregister framing; default position for fix phase: unregister `bitfield`/`bitop` + dead `bitfield_ro` parseSlots branch, matching `doc/redis_command_support.md` `[x]` rows, unless the user decides to implement. |
| 72.2 | Agree — CONFIRMED | Repro aligns with cited code. Accept the ACL note (no `denyoom` on `bf.loadchunk`) and the `Zstd.getFrameContentSize`-based fix hint. |
| 72.3 | Agree — CONFIRMED | Two-eventloop repro matches the thread-partition mechanism. Accept both additional requirements: register waiters on the owning worker AND re-check emptiness after registration (check-then-register window). |
| 72.4 | **Retracted by author** | Independently reproduced the reviewer's refutation. `javap -c` of `io/activej/promise/AbstractPromise.class` (activej-promise-6.0-beta2 jar from the Gradle cache): `getResult()` is `getfield result; if_acmpeq PROMISE_NOT_SET → aconst_null; areturn` — a non-blocking field read. My "getResult() parks the thread" premise was wrong for this Promise API (I conflated it with a `Future.get()`-style API). No parking → no circular wait → no deadlock. Residue (bogus `isSet` across workers, late `whenComplete` delivery) is real but belongs to 72.3; no separate ticket, no fix-log row. |
| 72.5 | Agree — CONFIRMED | Repro (real `OneSlot.get` path, no `byPassGetSet`, mismatched `fixSlotThreadId`) matches the cited `checkCurrentThreadId` throw. Accept the shared fan-out helper shape with 72.3. |
| 72.6 | Agree — CONFIRMED | Asymmetric-byte repro (`0x80` → 0 vs Redis 1) and the `bitpos`-disagrees-with-`bitcount` demonstration confirm the endianness split; my palindromic-test caveat was also verified. |
| 72.7 / 72.8 | Agree — CONFIRMED | Doc-quoted ground truth matches my citations; controls (`bit=1` missing key → -1; both-ends range → -1) already correct. Accept reviewer's note that start-only + `BIT` token is invalid syntax in Redis too — no action there. |
| 72.9 | Agree — REFINED, finding text corrected | Verified Guava 33.4.8-jre bytecode: `checkArgument(... "Expected insertions (%s) must be >= 0")` — capacity 0 accepted (Velo OK, RedisBloom errors), only negative leaks. My original trigger text and leaked-message quote were wrong; Root cause / Trigger sections updated in place. Fix direction unchanged (`initCapacity > 0` rejects both). |
| 72.10 | Agree — CONFIRMED | Registry dump matches cited registrations; arity values 4 / -4 / -4 per RedisBloom grammar. |

Verification evidence pasted (author, 2026-10-19):

```
$ javap -p -c io/activej/promise/AbstractPromise.class   # from activej-promise-6.0-beta2.jar
  public T getResult();
    Code:
       0: aload_0
       1: getfield      #13    // Field result:Ljava/lang/Object;
       4: getstatic     #7     // Field PROMISE_NOT_SET:Ljava/lang/Object;
       7: if_acmpeq     17
      10: aload_0
      11: getfield      #13    // Field result:Ljava/lang/Object;
      14: goto          18
      17: aconst_null
      18: areturn
```

```
$ javap -c com/google/common/hash/BloomFilter.class        # from guava-33.4.8-jre.jar
      16: ldc  #145  // String Expected insertions (%s) must be >= 0
      18: lload_1
      19: invokestatic #147 // Preconditions.checkArgument:(ZLjava/lang/String;J)V
```

Scope correction applied: OneSlot path fixed to `src/main/java/io/velo/persist/OneSlot.java` (reviewer was right; the code excerpt labels `OneSlot.java:NNN` elsewhere in the doc are unambiguous and left as-is).



## Fix Log (AI agent 1, round 1 fix phase)

Branch `review/bgroup`, based on `main` @ `330683c4`. Every bug followed red-green: the regression case was
added to the existing `BGroupTest.groovy` command method (or to `CommandRegistryTest` for the metadata bug),
run to see it fail for the expected reason, then the minimal production change was made.

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 72.2 | `test bf scandump and loadchunk`: `bf.loadchunk a 0 <int=100000000><junk>` → `ZstdException: Unknown frame descriptor` escaped from `BGroup.bfLoadchunk(BGroup.java:866)` after a **98MB** heap allocation; `<int=-1>` → `NegativeArraySizeException`; `<int=0>` → `ZstdException` | `8642442d` | `./gradlew :test --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | `BGroup` 861-880: **15/15 lines, 3/3 branch lines fully covered** (payload-less chunk, negative/zero/oversized length, frame-size mismatch, frame without content size) |
| 72.7 | `test bitpos`: `bitpos a 0` on a missing key → `-1`, expected `0` | `92718814` | `./gradlew :test --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | `BGroup` bitpos 455-465: 6/6 lines, all branch lines fully covered (`isBit1 ? -1 : 0`) |
| 72.8 | `test bitpos`: `bitpos b 0` and `bitpos b 0 0` on `[0xff,0xff,0xff]` → `-1`, expected `24` | `127ff5c4` | `./gradlew :test --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | `BGroup` 433-500: **39/39 lines, 16/16 branch lines fully covered** (padding rule, no-padding control, set-bit control, both start+end) |
| 72.6 | `test bitcount`: `bitcount bitkey 0 0 bit` on `[0x80]` → `0`, expected `1` | `33a698e0` | `./gradlew :test --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | `BGroup` 395-411: 9/9 lines, 5/5 branch lines fully covered (MSB-first + BYTE mode + `0x01` control) |
| 72.9 | `test bf reserve` / `test bf insert`: `bf.reserve c 0.01 0` → `OKReply`, `bf.insert c capacity 0 items item0` → `MultiBulkReply`, negative capacity → raw Guava `IllegalArgumentException` | `4e09e22d` | `./gradlew :test --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | `BGroup` 764-771 and 953-962: 7/7 lines each, all branch lines fully covered |
| 72.10 | `test bloom filter arity matches the redis bloom grammar`: `bf.loadchunk` arity `3` (expected `4`), `bf.reserve` `-3`, `bf.insert` `-2` | `b11e003a` | `./gradlew :test --tests "io.velo.command.CommandRegistryTest" --tests "io.velo.command.CommandRegistryPopulationTest" --tests "io.velo.command.BGroupTest"` → BUILD SUCCESSFUL | metadata-only change, no new executable lines; registry is data, asserted directly by the new spec method |
| 72.3 | `test blpop` (two worker event loops): push to the key owned by worker-2 returned `null` (no waiter visible) and the blocked reply stayed pending | `bc55bcad` | `./gradlew :test --tests "io.velo.command.BGroupTest" --tests "io.velo.command.BlockingListTest" --tests "io.velo.command.RGroupTest"` → BUILD SUCCESSFUL | `BGroup` 1110-1230: **49/49 lines, all branch lines fully covered** (cross-worker waiter registration, cross-worker pop chain, re-check helper) |
| 72.5 | `test blmpop` (two worker event loops): `IllegalStateException: Thread id not match, thread id=53, thread id protected for safe=54` from `BGroup.blmpop(BGroup.java:1287)` | `4cffebaf` | `./gradlew :test --tests "io.velo.command.BGroupTest" --tests "io.velo.command.BlockingListTest" --tests "io.velo.command.RGroupTest" --tests "io.velo.command.MGroupTest"` → BUILD SUCCESSFUL | `BGroup` 1276-1465: **76/77 lines covered, 0 not covered**; one defensive branch stays partial (`firstRl == null` while the cv exists) — see Residual Risk |
| 72.1 | `test unimplemented bitmap commands report unknown command` + `test handle - unknown command`: `bitop`/`bitfield`/`bitfield_ro` answered `(nil)`, registry advertised `bitop`/`bitfield` | `503930f4` | `./gradlew :test --tests "io.velo.command.BGroupTest" --tests "io.velo.command.CommandRegistryTest" --tests "io.velo.command.CommandRegistryPopulationTest"` → BUILD SUCCESSFUL | `BGroup` 190-215 and 270-320: 14/14 and 29/29 lines fully covered |

### Notes on the fixes

- **72.2** bound the client length prefix with `<= 0 || > CompressedValue.VALUE_MAX_LENGTH` and cross-checked it
  against `Zstd.getFrameContentSize`, so the allocation only happens for a frame that really declares that size.
  A frame without a content size still loads (the cross-check is skipped, the 1MB bound still applies).
- **72.3 / 72.5** share the same shape: every key is read, written, registered and removed on
  `localPersist.oneSlot(slotOfKey)`, chained with `Promise.then(...)` so no worker is blocked, and each list is
  re-checked right after the waiter registration to close the check-then-register window
  (`BGroup.completeIfListNotEmpty` / `BGroup.completeMpopIfListNotEmpty`, both `@VisibleForTesting` so the race
  window itself is unit-tested). Multi-key BLPOP/BRPOP/BLMPOP still answer with an `AsyncReply`, exactly as before.
- **72.1** removed the `bitop`/`bitfield` registry entries, the dead `parseSlots` branches for
  `bitfield`/`bitfield_ro`/`bitop`, and made the BGroup fall-through return `ErrorReply.UNKNOWN_COMMAND(cmd)`,
  matching the pattern already used by the empty groups (N/O/Q/J/V/YGroup).
  `CommandRegistry.size()` moved 213 → 211 accordingly.

### Test environment notes

- `./gradlew :test --tests "io.velo.command.*"` shows 11 failures unrelated to this module:
  `Bug71ReviewTempSpec` (7, the bug-71 reviewer artifact, asserts pre-fix behaviour) and `LGroupTest > test lpush`.
  Both fail identically on `main` @ `330683c4` without these commits.
- `Bug72ReviewTempSpec` (the reviewer artifact of this round) was **not committed**; every case it proved was
  rebuilt as a permanent case inside `BGroupTest.groovy` / `CommandRegistryTest.groovy`.

## Re-review (AI agent 2)

Not started yet. Commits to verify, in fix order: `8642442d`, `92718814`, `127ff5c4`, `33a698e0`,
`4e09e22d`, `b11e003a`, `bc55bcad`, `4cffebaf`, `503930f4`.

## Residual Risk

- `LGroupTest > test lpush` and the `Bug71ReviewTempSpec` cases fail on `main` @ `330683c4` as well; they are
  unrelated to this module but were observed while running the broad `io.velo.command.*` set.
- One defensive branch stays partial in JaCoCo after `4cffebaf`: `BGroup.java:1294`
  (`firstRl == null` while the compressed value exists and is a list). Reaching it needs a list value whose
  stored bytes decode to null, which the current `RedisList` encoding cannot produce (an empty list still
  encodes a 6-byte header). It is a null-safety check, not a behaviour gap.
- Multi-key BLPOP/BRPOP/BLMPOP now register their waiters per owning worker, but `BlockingList` still stores
  the waiter maps per worker thread; a key that moves slots at runtime (cluster rebalance) is not
  re-registered. That is beyond this round.
- `BITOP`/`BITFIELD` remain unimplemented, they are now honestly reported as unknown commands. Implementing
  them is a separate feature, not a bug fix.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 72.1 | BITOP/BITFIELD advertised but unimplemented → nil reply | Medium | Redis protocol compatibility | `CONFIRMED (reproduced + inspection)` | fixed (`503930f4`) — unregistered, now `ERR unknown command` |
| 72.2 | bf.loadchunk client-controlled allocation → OOM DoS | High | Security | `CONFIRMED (reproduced)` | fixed (`8642442d`) |
| 72.3 | Multi-key BLPOP waiters on wrong worker thread | High (>1 worker) | Concurrency | `CONFIRMED (reproduced)` | fixed (`bc55bcad`) |
| 72.4 | BLPOP `getResult()` blocks event loop → circular deadlock | High (>1 worker) | Concurrency | `REFUTED` | retracted by author — non-blocking `getResult()` verified via bytecode; residue folded into 72.3 fix |
| 72.5 | BLMPOP ignores isCrossRequestWorker → IllegalStateException | Medium (>1 worker) | Concurrency | `CONFIRMED (reproduced)` | fixed (`4cffebaf`) |
| 72.6 | BITCOUNT BIT unit LSB-first vs Redis MSB-first | Medium | Correctness | `CONFIRMED (reproduced)` | fixed (`33a698e0`) |
| 72.7 | BITPOS missing key + bit=0 → -1 (Redis: 0) | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`92718814`) |
| 72.8 | BITPOS all-ones + bit=0 → -1 (Redis: first bit past string) | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`127ff5c4`) |
| 72.9 | bf.reserve/insert capacity<=0 → Guava message leak | Low | Correctness | `REFINED` (capacity 0 silently OK; negative leaks Guava text) | fixed (`4e09e22d`) |
| 72.10 | bf.loadchunk/reserve/insert registry arity wrong | Low | Redis protocol compatibility | `CONFIRMED (inspection)` | fixed (`b11e003a`) |
