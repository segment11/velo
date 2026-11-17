# Bug 86 — SGroup Commands Review (Round 1)

**Module:** `io.velo.command.SGroup` — all S* commands (`sadd`, `save`, `scan`, `scard`, `sdiff`,
`sdiffstore`, `select`, `sentinel`, `set`, `setbit`, `setex`, `setnx`, `setrange`, `sinter`,
`sintercard`, `sinterstore`, `sismember`, `slaveof`, `smembers`, `smismember`, `smove`, `sort`,
`sort_ro`, `spop`, `srandmember`, `srem`, `sscan`, `strlen`, `subscribe`, `substr`, `sunion`,
`sunionstore`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** fix phase complete — agent 2 verified all 14 findings (13 `CONFIRMED (reproduced)`, 1 `PARTIALLY CONFIRMED`) and filed 1 additional finding (86.15); all 15 fixed in 13 commits + 1 cross-cutting refactor; the stage 5 re-review is pending
**Review branch:** `review/sgroup`
**Reviewed commit:** `2109db6304ebe366332c49329085f7b474431b76`

> Delta declaration: this is the first whole-class round. Prior rounds already covered parts of
> SGroup and are NOT re-reported here: bug_70 (keyspace `scan` — all 8 findings disposed:
> 70.1-70.7 fixed, 70.8 documented deviation) and bug_71 (`sscan` — 71.1 REFUTED, 71.3/71.4 fixed,
> 71.2/71.5 open pending product decision). Do not re-report: SCAN WAL/bucket cursor semantics,
> scan dispatch, glob MATCH, COUNT 0, beginScanSeq, per-connection cursor state, SSCAN cursor
> completeness under mutation (71.5), SSCAN cross-type pre-check shape (71.3 fix).

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/SGroup.java` — command registry (36-293), `parseSlots()`
  (312-390), `handle()` (397-546), `save()` (548-550), `scanResultToReply()`/`scan()` (552-739),
  `sentinel()` (743-745), host/port patterns (747-773), `slaveof()` (775-838), `set()` (840-988),
  `setbit()` (990-1040), `setrange()` (1042-1098), `strlen()` (1100-1117), `getRedisSet()`/
  `saveRedisSet()` (1119-1175), `sadd()` (1177-1216), `scard()` (1218-1236), `operateSet()`/
  `sdiff()` (1238-1358), `sdiffstore()` (1360-1475), `sintercard()` (1477-1608), `sismember()`
  (1610-1641), `smembers()` (1643-1671), `smismember()` (1673-1701), `sort()` (1703-2007),
  `smove()` (2009-2075), `srandmember()`/`spop` (2077-2158), `srem()` (2160-2198), `sscan()`
  (2200-2298), `subscribe()` (2300-2335)
- `src/main/java/io/velo/BaseCommand.java` — `getAuthU()` (207-219), `getCv()` (756-798, expired
  filtering at 777-779), `getValueBytesByCv()` (813-851), `get()` overloads (859-890),
  `set()` overloads (1016-1049)
- `src/main/java/io/velo/CompressedValue.java` — SP-type constants (30-101), `isCompressed()`/
  `isTypeString()` (585-623)
- `src/main/java/io/velo/persist/OneSlot.java` — `asyncRun()`/`asyncExecute()` (660-716)
- `src/main/java/io/velo/command/HGroup.java` — `getRandIndex()` (2274-2293) + `hrandfield`
  call-site semantics (2190-2270)
- `src/main/java/io/velo/type/RedisBitSet.java` — whole file (1-90)
- `src/main/java/io/velo/type/RedisHashKeys.java` — constants (29-39), `encode()` (171-186),
  `getSizeWithoutDecode()` (233)
- `src/test/groovy/io/velo/command/SGroupTest.groovy` — 'test setbit' (808-879), 'test setex'/
  'test setnx' (881-926), 'test setrange' (927-993), 'test strlen' (994-1027), 'test select'
  (1029-1042), 'test sdiff' (1125-1294), 'test sdiffstore' (1295-1527), 'test sort' (1891-2073),
  'test smove' (2075-2176)
- `doc/redis_command_support.md` — support rows for SORT/SORT_RO/SELECT/SAVE/SETBIT/SETRANGE/
  STRLEN/SMOVE/SUNION/SUNIONSTORE
- Prior rounds: `doc/bug_reviews/bug_70_scan_command_review_round_1.md` (full),
  `doc/bug_reviews/bug_71_hscan_sscan_review_round_1.md` (full)

Reference docs (ground truth):

- https://redis.io/commands/sunion/ — "Keys that do not exist are considered to be empty sets."
- https://redis.io/commands/sort/ — STORE "will be stored as a **list**"; GET "return the values
  of the external keys"; default sorting "compared by their value interpreted as double precision
  floating point number"; `BY nosort` skips sorting (both fetched 2026-11-16)
- https://redis.io/commands/set/ — KEEPTTL "mutually exclusive with the EX, PX, EXAT and PXAT
  options"; expire overflow rejected ("invalid expire time in 'set' command")
- https://redis.io/commands/setbit/, https://redis.io/commands/setrange/ — wrong-type → WRONGTYPE;
  offset caps ("largest offset you can use is 536870911", 512MB string cap)
- https://redis.io/commands/strlen/ — "An error ... when key holds a non string value"
- https://redis.io/commands/smove/ — atomic move; WRONGTYPE on non-set destination
- https://redis.io/commands/sunionstore/, https://redis.io/commands/sintercard/,
  https://redis.io/commands/spop/, https://redis.io/commands/srandmember/

---

## Finding 1 (Bug 86.1): SUNION / SUNIONSTORE treat a missing first key as "empty result" instead of the union of the remaining keys

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`e912765e`)

### Cited files / lines

```java
// SGroup.java:1283-1287 — sdiff() early return before the isUnion branch is ever reached
var first = list.getFirst();
var rhk = getRedisSet(first);
if (rhk == null) {
    return MultiBulkReply.EMPTY;              // returned for sdiff AND sinter AND sunion
}

// SGroup.java:1387-1392 — sdiffstore() non-cross path (SUNIONSTORE)
var rhk = getRedisSet(first);
if (rhk == null) {
    removeDelay(dstSlotWithKeyHash);          // deletes the destination key
    return IntegerReply.REPLY_0;
}

// SGroup.java:1443-1448 — sdiffstore() cross-worker path, same shape
var rhk = promises.getFirst().getResult();
if (rhk == null) {
    removeDelay(dstSlotWithKeyHash);
    finalPromise.set(IntegerReply.REPLY_0);
    return;
}
```

### Root cause

`sdiff(isInter, isUnion)` implements SDIFF / SINTER / SUNION through one method, and
`sdiffstore(isInter, isUnion)` implements the three *STORE variants. For SDIFF and SINTER an
absent first key legitimately means "empty result" (Redis: nonexistent keys are empty sets, and
diff/intersection with an empty set is empty). For SUNION the first key being absent must be
tolerated — the union continues with the remaining keys. The `rhk == null` early return runs
unconditionally, so `SUNION missing_key existing_set` returns an empty array, and
`SUNIONSTORE dst missing_key existing_set` replies 0 and **deletes dst** via `removeDelay`.

### Trigger / impact

```
SADD s2 a b c
SUNION s1 s2        → Redis: ["a","b","c"]   Velo: []              (s1 does not exist)
SUNIONSTORE d s1 s2 → Redis: 3, d = {a,b,c}  Velo: 0, d deleted
```

Existing tests never hit this: `'test sdiff'` (SGroupTest.groovy:1137-1143) only exercises the
both-keys-missing case where the empty reply is trivially right, and the union cases
(1176-1180, 1197-1200, 1222-1226) always have key `a` present.

### Suggested fix direction

In `sdiff()`: when `rhk == null && isUnion`, start from an empty `RedisHashKeys` and continue into
`operateSet` with the other keys. In `sdiffstore()` both paths: same — for union treat null first
as an empty set (dst ends up holding the union of the rest; if that union is empty,
`saveRedisSet` already deletes dst, matching Redis). Keep the early empty/0 return for diff and
inter only.

---

## Finding 2 (Bug 86.2): SMOVE removes and persists the source member before the destination type check — a WRONGTYPE dst error loses the member

**Severity:** High
**Class:** Correctness (atomicity violation → data loss)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`009b14db`)

### Cited files / lines

```java
// SGroup.java:2037-2052 — smove(), non-cross path
var member = new String(memberBytes);
var isMember = srcRhk.remove(member);        // 1. mutate in-memory source set
if (!isMember) {
    return IntegerReply.REPLY_0;
}

saveRedisSet(srcRhk, srcSlotWithKeyHash);    // 2. PERSIST the removal

if (!isCrossRequestWorker) {
    var dstRhk = getRedisSet(dstSlotWithKeyHash);   // 3. dst type check only happens here —
    ...                                              //    get(..., SP_TYPE_SET) throws
                                                     //    TypeMismatchException for a non-set dst
```

### Root cause

SMOVE is documented and implemented in Redis as atomic: if the destination holds a wrong-type
value the command replies WRONGTYPE and **nothing changes**. Velo persists the source mutation
(step 2) before ever reading the destination. When the dst key exists with a non-set sp-type,
`getRedisSet(dst)` throws `TypeMismatchException` (BaseCommand.get, BaseCommand.java:886-888),
`RequestHandler` converts it to a WRONGTYPE error reply — but the member was already removed from
the source and saved. After the failed command the member exists in neither set: silent,
permanent data loss. The cross-worker path has the same ordering (src saved at 2043, dst
read at 2064).

### Trigger / impact

```
SADD src m
SET dst "i am a string"        (any non-set type)
SMOVE src dst m                → Redis: WRONGTYPE, src still {m}
                                 Velo: WRONGTYPE, src now {} — m is gone
```

Blast radius: any SMOVE onto a hash/list/string/zset destination key destroys the moved member.
`'test smove'` (SGroupTest.groovy:2075-2176) never uses a wrong-type destination, so it passes.

### Suggested fix direction

Validate the destination before persisting the source removal. Minimal shape for the non-cross
path: read/decode dst (letting the type mismatch throw) *before* `saveRedisSet(srcRhk, ...)`.
Cross path: pre-check the dst type via `dstOneSlot.asyncCall(() -> getCv(dstSlotWithKeyHash))`
and only remove+save src in the completion (which the reactor bounces back to the src worker);
on a dst error, return the error reply without mutating src.

---

## Finding 3 (Bug 86.3): Cross-worker SMOVE never settles its reply promise when the destination lambda throws — client hangs

**Severity:** High
**Class:** Concurrency / hang (unterminated AsyncReply)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec (promise never settles) (see Review Feedback) — **fixed** (`009b14db`)

### Cited files / lines

```java
// SGroup.java:2056-2074 — smove() cross-worker path
dstOneSlot.asyncExecute(() -> {
    var dstRhk = getRedisSet(dstSlotWithKeyHash);   // can throw TypeMismatchException / DictMissingException
    if (dstRhk == null) {
        dstRhk = new RedisHashKeys();
    }
    dstRhk.add(member);

    saveRedisSet(dstRhk, dstSlotWithKeyHash);
    finalPromise.set(IntegerReply.REPLY_1);          // only reached on success
});

// OneSlot.java:704-716 — asyncExecute has no exception channel
public void asyncExecute(@NotNull Runnable runnable) {
    ...
    slotWorkerEventloop.execute(runnable);           // exception swallowed by the eventloop
}
```

### Root cause

`finalPromise` is set only on the success path of the lambda. `asyncExecute` returns void and —
unlike `asyncRun` (OneSlot.java:665-697, which wraps the runnable in a completing Promise) —
offers no exception channel: a throw inside the lambda is swallowed by the slot eventloop. The
returned `AsyncReply` then never resolves, so the client connection waits forever for the SMOVE
reply. Triggers include a wrong-type dst (same scenario as Finding 2, cross-slot variant) or a
missing compression dictionary (`DictMissingException`). This is the same bug class fixed for
`pfmerge` in 84.1 and `migrate` in 83.7 — SMOVE was not covered.

### Trigger / impact

Slot-spanning `SMOVE src dst m` where dst holds a non-set value: src is persisted without the
member (Finding 2), and the client hangs on the reply until a socket timeout. One errored
SMOVE can wedge a connection (and with pipelines, the whole batch behind it).

### Suggested fix direction

Catch exceptions inside the lambda and settle the promise with an error reply
(`finalPromise.set(new ErrorReply(...))` / `Utils.wrapException` pattern from the 84.1/83.7
fixes). Optionally switch to `asyncRun(...).whenComplete(...)` which already carries exceptions —
but keep the dst read+write on the dst worker (see Finding 2 for the required ordering).

---

## Finding 4 (Bug 86.4): SETBIT / SETRANGE operate on non-string keys without a type check and rewrite them as plain strings — silent type corruption

**Severity:** High
**Class:** Correctness / Redis protocol compatibility (silent data corruption)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`434e0068`)

### Cited files / lines

```java
// SGroup.java:1027-1034 — setbit()
var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
var valueBytes = get(slotWithKeyHash);          // single-arg get(): NO type check
                                              // (BaseCommand.java:859-861 → get(s, false))
var bs = new RedisBitSet(valueBytes);           // treats set/hash/list encoded bytes as a bitmap
var result = bs.set(offset, isBit1);

if (result.isExpanded() || result.isChanged()) {
    set(bs.getValueBytes(), slotWithKeyHash);   // set(bytes, s) → NULL_DICT_SEQ → STRING type
}

// SGroup.java:1071-1095 — setrange(), same pattern
var valueBytesExist = get(slotWithKeyHash);     // no type check
...
set(setBytes, slotWithKeyHash);                 // rewrites the key as a string
```

### Root cause

Both commands read the current value with the single-argument `get(slotWithKeyHash)` which is
`get(s, false)` — no `expectTypeString`, no sp-type filter (BaseCommand.java:859-861,
878-890). For a key holding a set/hash/list/zset CV, `getValueBytesByCv` happily returns the
*encoded structure bytes* (SP-type CVs have `dictSeqOrSpType < 0`, so `isCompressed()` is false
and the raw payload is returned, CompressedValue.java:592-594). SETBIT then flips a bit inside
those structure bytes and SETRANGE splices the argument into them; both then call
`set(bytes, slotWithKeyHash)` which writes the key back with `NULL_DICT_SEQ` — i.e. **as a plain
string** (BaseCommand.java:1016-1018). Redis answers WRONGTYPE for both commands on a non-string
key and never touches it.

### Trigger / impact

```
SADD s a b c
SETBIT s 0 1        → Redis: WRONGTYPE          Velo: returns 0/1, s is now a corrupted string;
                                                 SCARD s → WRONGTYPE ever after; members lost
SETRANGE s2 0 xyz   (s2 a list) → same corruption path
```

The key's type marker flips to string and the payload is mangled structure bytes — the original
collection is unrecoverable. `'test setbit'` (SGroupTest.groovy:808-879) only uses string /
absent keys, so this is untested.

### Suggested fix direction

Use the typed read: `get(slotWithKeyHash, true)` throws `TypeMismatchException` →
RequestHandler maps it to WRONGTYPE (the pattern `getbit` in BGroup and `getrange` in GGroup
already rely on). Note for the fixer: number-typed values must keep working (they are
`isTypeString`, so `get(s, true)` still accepts them — Redis also decodes integer-encoded strings
before bit ops); decide explicitly whether HLL/bloom CVs (spType -96/-200, below
`SP_TYPE_BIG_STRING`) stay permitted (Redis allows bit ops on an HLL since it is a string) or are
rejected as an accepted deviation.

---

## Finding 5 (Bug 86.5): STRLEN on a non-string key returns the encoded payload length instead of WRONGTYPE

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`ae239c7e`)

### Cited files / lines

```java
// SGroup.java:1105-1116 — strlen()
var cv = getCv(slotWithKeyHash);
if (cv == null) {
    return IntegerReply.REPLY_0;
}

if (cv.isTypeNumber()) {
    var valueBytes = getValueBytesByCv(cv, slotWithKeyHash);
    return new IntegerReply(valueBytes.length);
} else {
    return new IntegerReply(cv.getUncompressedLength());   // also reached for set/list/hash/zset CVs
}
```

### Root cause

The only type discrimination is number-vs-everything-else. A set/list/hash/zset/HLL CV falls into
the `else` branch and the client gets the length of the encoded structure bytes — a
plausible-looking but meaningless integer. Redis replies
`WRONGTYPE Operation against a key holding the wrong kind of value` for STRLEN on any non-string.

### Trigger / impact

```
SADD s a b
STRLEN s   → Redis: WRONGTYPE    Velo: (integer) <encoded-bytes length>
```

Clients that branch on STRLEN (e.g. TTL-size estimators, key scanners) silently mis-account
collection keys as strings. `'test strlen'` (SGroupTest.groovy:994-1027) covers only absent,
string and int-number keys.

### Suggested fix direction

After the null check, gate on the string family (`cv.isTypeString()` covers numbers, short and
big strings) and return `ErrorReply.WRONG_TYPE` otherwise; keep the number branch for the exact
digits-length semantics. Same HLL/bloom policy decision as Finding 4.

---

## Finding 6 (Bug 86.6): SORT parses GET patterns and then silently ignores them — returns elements instead of the referenced values

**Severity:** Medium
**Class:** Redis protocol compatibility (silent wrong answer)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`e7f07915`)

### Cited files / lines

```java
// SGroup.java:1716 — getPatternList is created
ArrayList<String> getPatternList = new ArrayList<>();

// SGroup.java:1743-1749 — GET patterns are collected during parsing
case "GET":
    if (i + 1 >= data.length) {
        return ErrorReply.SYNTAX;
    }
    getPatternList.add(new String(data[i + 1]));
    i += 1;
    continue;
```

`getPatternList` is never read again anywhere in the method (nor anywhere else in the file).

### Root cause

The option parser accepts `GET pattern` (and validates its arity) but the result-assembly code in
all three type branches returns the sorted elements themselves. Redis substitutes each `*` in the
pattern with the element and returns the value stored at the generated key (`GET #` returns the
element itself). A client sorting IDs and fetching payloads gets the IDs back instead — no error,
structurally valid reply, wrong content.

### Trigger / impact

```
RPUSH ids 1 2
SET obj_1 a / SET obj_2 b
SORT ids GET obj_*   → Redis: ["a","b"]    Velo: ["1","2"]
```

`doc/redis_command_support.md:106` marks SORT as fully supported (√), so this is not a documented
gap; the test suite only asserts the BY-error case (`'test sort'`, SGroupTest.groovy:1905-1909,
where `by weight_* ... get #` happens to error out earlier for the unsupported BY).

### Suggested fix direction

Either implement GET (per sorted element: `#` → the element; otherwise pattern with the first
`*` substituted; missing key → nil; `->` hash-field syntax optional) for all three source types —
or, as the minimal honest fix, reply an explicit not-support error when `getPatternList` is
non-empty, and update `redis_command_support.md` accordingly. Silent acceptance is the bug.

---

## Finding 7 (Bug 86.7): SORT ... STORE writes a set or zset destination for set/zset sources — Redis always stores a list

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`8a184f80`)

### Cited files / lines

```java
// SGroup.java:1902-1917 — set source: destination saved as a SET
if (isStore) {
    var dstSlotWithKeyHash = slot(dstKeyBytes, slotNumber);
    var dstRhk = new RedisHashKeys();          // set structure
    ...
    saveRedisSet(dstRhk, dstSlotWithKeyHash, this, dictMap);

// SGroup.java:1976-1992 — zset source: destination saved as a ZSET
    var dstRz = new RedisZSet();               // zset structure, original scores reused
    ...
    ZGroup.saveRedisZSet(dstRz, dstSlotWithKeyHash, this, dictMap);
```

Only the list branch (1822-1847) stores a list via `LGroup.saveRedisList`.

### Root cause

Redis SORT with STORE "will be stored as a **list** at the specified key" regardless of the
source type (redis.io/commands/sort, § Storing the result). Velo preserves the source structure:
a set source produces a `SP_TYPE_SET` destination (with the result's members), a zset source
produces a `SP_TYPE_ZSET` destination reusing the original scores. Downstream commands disagree:
`LRANGE dst 0 -1` (the idiom every Redis client uses after SORT ... STORE) fails with WRONGTYPE
on a set/zset destination, and `TYPE dst` differs.

### Trigger / impact

```
SADD s 3 1 2
SORT s STORE d
TYPE d      → Redis: list    Velo: set
LRANGE d 0 -1 → Redis: 1 2 3  Velo: WRONGTYPE
```

`'test sort'` (SGroupTest.groovy:2025-2056) only asserts the integer reply (10), never the
destination type.

### Suggested fix direction

All three branches should build a `RedisList` of the (post-GET, post-LIMIT) results and store via
`LGroup.saveRedisList` — the list branch already shows the exact pattern. Note interplay with
Finding 6: if GET stays unimplemented, STORE+GET must not store the raw elements.

---

## Finding 8 (Bug 86.8): SORT on a zset returns score order instead of sorting the members — and skips numeric validation entirely

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback); refined: Redis also rejects non-numeric members without ALPHA — **fixed** (`6e525454`; the redis error text `9613b879`)

### Cited files / lines

```java
// SGroup.java:1939-1956 — zset source, non-ALPHA branch
if (!isAlpha) {
    // already sorted by score          ← comment states the wrong assumption
    var it = isAsc ? set.iterator() : set.descendingIterator();
    ...                                  // no parsing of members at all
    subList.add(e.member());

// SGroup.java:1957-1973 — ALPHA branch sorts members lexically (correct shape)
```

### Root cause

Redis SORT on a sorted set ignores the scores: the members are extracted and sorted exactly like
set elements (numerically by member value by default, lexically with ALPHA; non-numeric members
without ALPHA are an error). Velo walks the zset in score order for the default form, so the
reply order is arbitrary with respect to the members, and non-numeric members that Redis would
reject (`SORT z` on members {a,b}) are silently returned in score order. The ALPHA branch already
does the right thing.

### Trigger / impact

```
ZADD z 5 a 1 b 3 c
SORT z      → Redis: ["a","b","c"] (member sort, non-numeric without ALPHA → error in Redis)
              Velo: ["b","c","a"]   (score order)
ZADD z2 10 1 20 2 30 3
SORT z2     → Redis: ["1","2","3"]  Velo: ["1","2","3"] — same only by luck of score choice
ZADD z3 1 9 2 1
SORT z3     → Redis: ["1","9"]      Velo: ["9","1"] — wrong order
```

The existing test (SGroupTest.groovy:1990-2007) uses `rz.add(it, String.valueOf(it))` — score
equals member value — so score order coincides with member order and the bug is invisible.

### Suggested fix direction

Mirror the set branch: collect members into a `TreeSet<Long>` (or doubles, see Finding 9) for the
numeric path — parse failure → NOT_INTEGER error — and keep the existing ALPHA branch. If the
score-order behavior was a deliberate shortcut it must at least validate numerics; Redis
semantics is member sorting.

---

## Finding 9 (Bug 86.9): SORT numeric sorting is integer-only — Redis compares elements as doubles

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`6e525454`; the redis error text `9613b879`)

### Cited files / lines

```java
// SGroup.java:1806-1815 — list branch numeric comparator
list.sort((o1, o2) -> {
    long l = Long.parseLong(new String(o1)) - Long.parseLong(new String(o2));
    ...
});

// SGroup.java:1860-1865 — set branch
sortedByLong = set.stream().map(Long::parseLong).collect(Collectors.toCollection(TreeSet::new));
```

### Root cause

Redis' default SORT "is numeric and elements are compared by their value interpreted as double
precision floating point number" (redis.io/commands/sort). Velo parses every element as a
signed 64-bit long. Any list/set with a decimal element (`1.5`, `-0.5`, `2e3`) fails
`Long.parseLong` → NOT_INTEGER error, where Redis sorts successfully. The zset non-ALPHA branch
does not parse at all (Finding 8).

### Trigger / impact

```
RPUSH l 1.5 2 0.5
SORT l      → Redis: ["0.5","1.5","2"]   Velo: ERR not an integer
```

Scientific-notation and float elements are common in score-like payloads.

### Suggested fix direction

Parse as `double` for the numeric paths of all three branches (`Double.parseDouble`,
`Double.compare`), keeping ALPHA on string comparison. Note Redis sorts `1` and `1.0` as equal
numerically — with doubles that falls out naturally.

---

## Finding 10 (Bug 86.10): SORT list-branch numeric comparator subtracts two longs — overflow mis-sorts extreme values

**Severity:** Low
**Class:** Correctness (edge case)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`6e525454`)

### Cited files / lines

```java
// SGroup.java:1809
long l = Long.parseLong(new String(o1)) - Long.parseLong(new String(o2));
var r = l > 0 ? 1 : l < 0 ? -1 : 0;
```

### Root cause

Classic comparator-overflow: `parseLong(o1) - parseLong(o2)` wraps when the two values straddle
the long range (e.g. `Long.MAX_VALUE` vs `-1`), flipping the sign and violating the comparator
contract — `SORT` then returns a mis-ordered list (and can even throw
`IllegalArgumentException` from TimSort for contract violations on some inputs). The set branch
is immune (`TreeSet<Long>` compares properly).

### Trigger / impact

```
RPUSH l 9223372036854775807 -1 0
SORT l      → expected ["-1","0","9223372036854775807"], actual order wrong (sign wrap)
```

### Suggested fix direction

`Long.compare(parseLong(o1), parseLong(o2))` (subsumed by Finding 9's `Double.compare` rewrite
if that lands for the list branch).

---

## Finding 11 (Bug 86.11): SET accepts KEEPTTL together with EX/PX/EXAT/PXAT — KEEPTTL silently wins

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`954c1cd9`)

### Cited files / lines

```java
// SGroup.java:856-921 — option parse loop accepts any combination without exclusivity checks
boolean isKeepTtl = false;
...
if ("keepttl".equalsIgnoreCase(arg)) { isKeepTtl = true; continue; }
...
isExpireAtSet = isEx || isPx || isExAt || isPxAt;    // no conflict detection

// SGroup.java:969-972 — keepttl overwrites the explicitly parsed expiry
if (isOldExist && isKeepTtl) {
    expireAt = cv.getExpireAt();
}
```

### Root cause

Redis documents KEEPTTL as "mutually exclusive with the EX, PX, EXAT and PXAT options" and
replies `ERR syntax error` when both are given (only NX and XX were made mutually exclusive in
Velo so far, SGroup.java:923-926, commit 2109db63). Velo accepts the combination and — when the
key exists — silently discards the caller's explicit TTL in favor of the retained one. The
caller believes a new expiry was set (got +OK) but the old TTL stands.

### Trigger / impact

```
SET k v EX 100
SET k v2 EX 50 KEEPTTL   → Redis: ERR syntax error
                          Velo: OK; TTL k still ≈100 (the EX 50 was dropped)
```

Silent TTL divergence for any client that (buggily or speculatively) sends both — the server
must not mask it.

### Suggested fix direction

After the parse loop, next to the existing `isNx && isXx` check:
`if (isKeepTtl && isExpireAtSet) return ErrorReply.SYNTAX;`.

---

## Finding 12 (Bug 86.12): SET expire arithmetic can overflow long — arbitrary/garbage expireAt instead of an error

**Severity:** Medium
**Class:** Edge case (correctness)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`71072d90`)

### Cited files / lines

```java
// SGroup.java:905-908 — the only validation is positivity
if (value <= 0) {
    return invalidExpireTime(cmd);
}

// SGroup.java:928-939 — unguarded arithmetic
if (ex != -1) {
    expireAt = System.currentTimeMillis() + ex * 1000;   // ex * 1000 overflows
} else if (px != -1) {
    expireAt = System.currentTimeMillis() + px;          // now + px overflows
} else if (exAt != -1) {
    expireAt = exAt * 1000;                              // overflows
} else if (pxAt != -1) {
    expireAt = pxAt;
}
```

### Root cause

`ex`/`px`/`exAt` are only checked for `> 0` (line 905). Redis validates the resulting absolute
time against `LLONG_MAX` and replies `ERR invalid expire time in 'set' command` on overflow. In
Velo, `SET k v EX 9223372036854775807` computes `ex * 1000` wrapping to a negative/garbage
value; a negative `expireAt` makes `cv.isExpired()` immediately true, so the command replies +OK
while the key is born dead (GET returns nil). Other overflow mixes yield arbitrary future
expiries. `PSETEX`/`SET k v PX ...` and `SET k v EXAT ...` inherit this through the same `set()`
(`setex` at SGroup.java:445-446 routes through it too).

### Trigger / impact

```
SET k v EX 9223372036854775807   → Redis: ERR invalid expire time in 'set' command
                                   Velo: OK, then GET k → nil (key stored already-expired)
```

### Suggested fix direction

Validate before arithmetic, mirroring Redis: reject when `ex > (Long.MAX_VALUE - now) / 1000`,
`px > Long.MAX_VALUE - now`, or `exAt > Long.MAX_VALUE / 1000` with `invalidExpireTime(cmd)`.

---

## Finding 13 (Bug 86.13): SETRANGE offset is unbounded — giant allocations or NegativeArraySizeException

**Severity:** Medium
**Class:** Edge case / resource exhaustion (DoS)
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`6444c409`)

### Cited files / lines

```java
// SGroup.java:1058-1066 — offset validated only for >= 0
int offset;
try {
    offset = Integer.parseInt(new String(offsetBytes));
} catch (NumberFormatException e) {
    return ErrorReply.NOT_INTEGER;
}
if (offset < 0) {
    return ErrorReply.INVALID_INTEGER;
}

// SGroup.java:1072-1080 — allocation sized directly from user input
int len = offset + valueBytes.length;      // int overflow possible
if (valueBytesExist == null) {
    lengthResult = len;
    var setBytes = new byte[len];          // NegativeArraySizeException on wrap; huge alloc otherwise
```

### Root cause

Redis caps SETRANGE at offset ≤ 536870911 with a 512MB total-string limit and replies
`ERR offset is out of range` / `ERR string exceeds maximum allowed size`. Velo accepts any
non-negative int: `SETRANGE k 2000000000 x` attempts a ~2GB byte[] allocation (heap exhaustion /
GC storm — the 1-byte value sails past the `VALUE_MAX_LENGTH` check at 1054), and offsets near
`Integer.MAX_VALUE` wrap `offset + valueBytes.length` negative → uncaught
`NegativeArraySizeException` from `handle()` instead of an error reply. SETBIT got an explicit
`MAX_OFFSET` guard (1011-1016); SETRANGE got none.

### Trigger / impact

```
SETRANGE k 2147483647 v   → Velo: NegativeArraySizeException (connection-level error, not an ERR reply)
SETRANGE k 1000000000 v   → Velo: attempts 1GB allocation for a 1-byte write
```

### Suggested fix direction

Add an explicit offset/total-length cap before allocating. Aligning with Redis's own bound
(offset ≤ 536870911) or with velo's `VALUE_MAX_LENGTH`-derived budget are both acceptable — the
requirement is a protocol error reply, not an exception or an unbounded allocation.

---

## Finding 14 (Bug 86.14): SORT ... STORE does not validate the destination key length

**Severity:** Low
**Class:** Edge case (invariant violation, same class as bug 85.5)
**Status:** `PARTIALLY CONFIRMED` — agent 2: the velo `KEY_MAX_LENGTH` invariant violation is real and reproduced; the defect is a velo-internal consistency gap (Redis 7.2.11 itself has no per-key length limit and stores the oversized key), **not** a Redis parity gap — the report body never claimed one — **fixed** (`0299805d`)

### Cited files / lines

```java
// SGroup.java:1756-1762 — dst key captured with no length check
case "STORE":
    if (i + 1 >= data.length) {
        return ErrorReply.SYNTAX;
    }
    dstKeyBytes = data[i + 1];
    i += 1;
    continue;

// contrast SGroup.java:1366-1369 — sdiffstore validates the same thing
var dstKeyBytes = data[1];
if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
    return ErrorReply.KEY_TOO_LONG;
}
```

### Root cause

Every other multi-key write path in this class validates the destination key against
`CompressedValue.KEY_MAX_LENGTH` (sdiffstore 1366-1369, smove 2017-2022); `sort()` does not. A
STORE destination longer than 256 bytes is written into the persist layer via
`slot(dstKeyBytes, ...)` + `saveRedisList`/`saveRedisSet`/`saveRedisZSet`, violating the key
length invariant the rest of the codebase enforces (RESTORE got the identical fix in bug 85.5,
commit c0c5a516).

### Trigger / impact

`SORT src STORE <257-byte-key>` → key stored instead of `KEY_TOO_LONG` error; downstream length
assumptions (bucket encoding, key length prefixes) may misbehave for the oversized key.

### Suggested fix direction

One guard after parsing (next to the existing `sort_ro not support store` check at 1772-1774):
`if (dstKeyBytes != null && dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) return ErrorReply.KEY_TOO_LONG;`

---

## Finding 15 (Bug 86.15): SORT ... LIMIT <offset> 0 returns the whole collection instead of an empty array

*(filed by AI agent 2 during the round 1 verification — not part of the agent 1 discovery)*

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — agent 2, temp Spock spec + live Redis 7.2.11 differential (see Review Feedback) — **fixed** (`0af1a75e`)

### Cited files / lines

```java
// SGroup.java:1795-1797 — list branch
if (count <= 0) {
    count = list.size();
}

// SGroup.java:1854-1856 — set branch, the same shape
if (count <= 0) {
    count = set.size();
}

// SGroup.java:1934-1936 — zset branch, the same shape
if (count <= 0) {
    count = set.size();
}
```

### Root cause

`count` is initialised to `-1` (SGroup.java:1715) to mean "no LIMIT given". The three branches
collapse the two distinct cases "no LIMIT" (`count == -1`) and "LIMIT with count 0" (`count == 0`)
into one, because the guard is `count <= 0` instead of `count < 0`. Redis treats `LIMIT <offset> 0`
as "return nothing": in `sortCommand` the count is passed to `sdsrange`-like slicing and a count of 0
yields an empty result. Velo instead replaces the count with the collection size and returns the
whole sorted collection.

### Trigger / impact

```
RPUSH l 3 1 2
SORT l LIMIT 0 0   → Redis: (empty array)   Velo: ["1","2","3"]
SORT l LIMIT 1 0   → Redis: (empty array)   Velo: ["1","2","3"]
```

A client that uses `LIMIT offset 0` to probe "is there anything after this offset" (a common
paging idiom) gets the full result instead of an empty one.

### Suggested fix direction

Distinguish "no LIMIT" from "count 0": only expand the count when `count < 0`
(`if (count < 0) { count = list.size(); }`), and let a `count == 0` fall through to the existing
slicing code, which already produces an empty sub-list. Note that `offset >= list.size()` is
already handled (`Collections.emptyList()` for the list branch, the `offset--` loop for the set and
zset branches). The `LIMIT` parse itself is fine (`LIMIT` with a negative count is rejected by
`offset < 0` only for the offset — the count is not validated; Redis answers `ERR syntax error`
for a negative count — see the *Additional observations* of the Review Feedback).

---

## Notes / non-findings

- `getCv()` filters expired CVs (BaseCommand.java:777-779 returns null) — so SORT/STRLEN/SSCAN/
  SET-GET paths that branch on `getCv() == null` handle expired keys correctly; the redundant
  `!cv.isExpired()` in `set()` (946) is harmless. Checked because several commands read the CV
  directly.
- SCAN/SSCAN internals (WAL/bucket cursors, dispatch, glob MATCH, COUNT 0, beginScanSeq,
  per-connection cursor state) — bug_70 territory, all disposed there; not re-examined beyond
  confirming the recent fixes (2109db63 NX+XX syntax error, e38894ea invalid-cursor reply) are in
  place at the reviewed commit.
- SSCAN cross-type pre-check and option validation — bug_71 (71.3/71.4 fixed); SSCAN offset
  cursor under concurrent mutation — 71.5, open by product decision; none re-reported.
- `substr` delegates to `GGroup.getrange()` (530-534) — GGroup was bug_77's scope.
- SADD/SREM member length (`SET_MEMBER_MAX_LENGTH=255`) and set size (`HASH_MAX_SIZE=4096`)
  caps — velo design guards, same disposition as bug 82.6 (LIST_MAX_SIZE, won't fix); SADD's
  mid-loop size error returns before `saveRedisSet`, so no partial write is persisted (atomic).
- SETBIT 1MB offset cap (1011-1016) — velo-specific documented limit (code comment references
  Redis's 512MB); accepted deviation.
- SET 1MB value cap (`VALUE_MAX_LENGTH`) — same class of accepted size guard.
- SORT `BY pattern` unsupported — replies an explicit error, covered by an existing test
  ('test sort', 1905-1909); the `redis_command_support.md` √ marking overstates support (doc
  drift worth a line when 86.6/86.7 are fixed, not a separate bug id).
- SORT `BY nosort` skip-sorting idiom — not supported (same error as BY); falls under the same
  BY-not-supported umbrella.
- `SELECT` → NOT_SUPPORT — documented [x] in redis_command_support.md:81.
- `SAVE` returning OK without a synchronous snapshot — design (continuous WAL→bucket persist;
  updates `BGroup.lastBgSaveMillis`); `SENTINEL` → NOT_SUPPORT — design.
- `sintercard`: LIMIT 0 = no limit matches Redis; `numkeys` > provided keys replies SYNTAX
  (Redis: "Number of keys can't be greater than number of args") — message-text drift accepted
  per repo precedent.
- `spop` negative count error text and `srandmember` negative-count-with-repetition semantics
  (`getRandIndex`, HGroup.java:2274-2293, `canUseSameField = count < 0`) — verified correct /
  accepted drift; `SPOP key 0` / `SRANDMEMBER key 0` → empty array, matches Redis.
- `sismember`/`scard`/`smembers`/`srem`/`smismember`/`sadd` wrong-type behavior — all go through
  `get(..., SP_TYPE_SET)` → `TypeMismatchException` → RequestHandler WRONG_TYPE (verified the
  mapping path referenced by bug_71 Review Feedback §71.3).
- `subscribe()` ACL check falls back to the default user via `getAuthU()` (BaseCommand.java:
  207-214) — same pattern as PGroup publish (bug_84 scope); reply shape (3 fields per channel)
  correct.
- `slaveof()` host/port/`NO ONE` validation (775-838) — reviewed, correct; IPv6 host literals
  unsupported is a systemic velo limitation, not an SGroup bug.
- `smove` with identical src/dst — works by accident of ordering (remove+save, then re-add+save);
  not reported.
- `srem` re-encodes and rewrites the set even when `removed == 0` (2196) — wasteful no-op write,
  not a correctness bug.
- `setbit`/`setrange` on number-typed values — digits are decoded then modified, matching Redis's
  integer-encoded-string behavior; correct.
- SDIFF/SINTER/SUNION wrong-type keys → `TypeMismatchException` → WRONGTYPE (including the
  asyncCall cross-worker path, which propagates via promise exception → `Utils.wrapException`);
  correct.
- `sdiffstore` cross-worker dst-write threading — the 84.1-era reactor analysis in the comment at
  1429-1435 was taken at face value; the bug 84 re-review already flagged the ActiveJ promise
  race as velo-wide residual.

## Reviewer checklist (for AI agent 2)

1. **86.1**: InMemoryGetSet + `execute('sadd s2 a')`, `execute('sunion s1 s2')` with s1 absent →
   expect Redis-parity failure (Velo returns EMPTY); `sdiffstore(false, true)` via
   `'sunionstore d s1 s2'` → expect 0 + dst deleted instead of 1 member. Live Redis 7.2
   differential optional (semantics documented on redis.io/commands/sunion).
2. **86.2**: `execute('sadd src m')`, put a string-typed CV under `dst`, `execute('smove src dst m')`
   (call `handle()` via execute so RequestHandler mapping is bypassed — catch
   TypeMismatchException, then assert `smembers src` still contains `m` in Redis vs not in Velo.
   For parity: catch the exception, then inspect the in-memory store for src's members.
3. **86.3**: cross-worker setup like 'test smove' 2119-2139 (`crossRequestWorker = true`, dst slot
   on another eventloop), dst = wrong-type CV; drive the eventloop and assert the AsyncReply
   promise never completes (timeout) — or inspect that `settablePromise` has no result after the
   dst eventloop processed the task.
4. **86.4**: put a SET-typed CV (encoded `RedisHashKeys`) under key; `execute('setbit k 0 1')` →
   Velo returns integer; then `get(slot...)` CV type should now be string family (corruption);
   Redis answers WRONGTYPE. Same for `'setrange k 0 x'`.
5. **86.5**: SET-typed CV; `execute('strlen k')` → Velo integer vs Redis WRONGTYPE.
6. **86.6**: list `1,2` + string keys `obj_1`,`obj_2`; `execute('sort l get obj_*')` → compare
   with Redis `["a","b"]`.
7. **86.7**: `execute('sort s store d')` on a set; assert stored CV spType — Velo `SP_TYPE_SET`
   vs Redis list (LRANGE parity check).
8. **86.8**: zset `ZADD z 1 9 2 1` shape (score != member order); `execute('sort z')` → compare
   with Redis; also non-numeric members without ALPHA.
9. **86.9**: `RPUSH l 1.5 2 0.5`; `execute('sort l')` → Velo NOT_INTEGER vs Redis sorted floats.
10. **86.10**: `RPUSH l 9223372036854775807 -1 0`; `execute('sort l')` → order assertion.
11. **86.11**: `execute('set k v ex 100')` then `execute('set k v2 ex 50 keepttl')` → Velo OK
    (and TTL unchanged) vs Redis syntax error.
12. **86.12**: `execute('set k v ex 9223372036854775807')` → Velo OK + subsequent GET nil, vs
    Redis invalid-expire-time error.
13. **86.13**: `execute('setrange k 2147483647 v')` → expect NegativeArraySizeException (or OOM
    attempt with a large-but-wrapping-free offset); assert an ErrorReply is the correct outcome.
14. **86.14**: `execute('sort l store <257-char key>')` → Velo writes the key vs KEY_TOO_LONG.

**Reviewer outcome (AI agent 2, round 1):** all 14 recipes were executed — 13 reproduced the
reported behaviour, recipe 14 reproduced the velo behaviour but the *Redis* expectation in the
recipe is wrong (Redis 7.2.11 stores the oversized key and answers `2`). One recipe uncovered an
additional defect not in the checklist: `SORT ... LIMIT <offset> 0` returns the whole collection
(filed as 86.15). See the Review Feedback section and
`doc/bug_reviews/logs/20261117_125000_sgroup_review_agent2_bug_86.log`.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec (15 feature methods, 15 passed / 0 failed — the temp spec asserted the
*observed* (buggy) behaviour so that the reproduction is green; deleted after the verification, the
full source is inlined in `doc/bug_reviews/logs/20261117_125000_sgroup_review_agent2_bug_86.log`) +
live **Redis 7.2.11** differential (`redis-server --port 6399`, `redis-cli`) + static inspection of
`SGroup.java`, `BaseCommand.java`, `CompressedValue.java`, `OneSlot.java` and the ActiveJ 6.0-beta2
bytecode. Reviewed at commit `2109db63` (= the report baseline, no drift).

Evidence log: `doc/bug_reviews/logs/20261117_125000_sgroup_review_agent2_bug_86.log`
(temp spec source + gradle output + the Redis differential).

| Finding | Verdict | Evidence (short) |
|---|---|---|
| 86.1 | `CONFIRMED (reproduced)` | Velo `sunion s1 s2` → empty array, `sunionstore d s1 s2` → `0` **and the pre-existing dst key `d` is deleted**; Redis 7.2.11 → `a b c` / `3` with `d` = 3 members. |
| 86.2 | `CONFIRMED (reproduced)` | Velo: the `TypeMismatchException` escapes `handle()`, the src key is **removed** (member `m` lost); Redis 7.2.11 → `WRONGTYPE`, src still `{m}`. Production maps the exception to `ErrorReply.WRONG_TYPE` (RequestHandler.java:571-575) — the reply is right, the data is gone. |
| 86.3 | `CONFIRMED (reproduced)` | Cross-worker SMOVE: the dst eventloop logs `Fatal error … TypeMismatchException` and `settablePromise.isComplete() == false` forever → the client hangs (no reply, no exception). |
| 86.4 | `CONFIRMED (reproduced)` | Velo `setbit k 0 1` on a set key → integer reply, the key type flips to `0` (string) and the members are lost (`scard` → `TypeMismatchException`); Redis → `WRONGTYPE`, `type k` stays `set`. |
| 86.5 | `CONFIRMED (reproduced)` | Velo `strlen` on a set key → `IntegerReply` (the encoded structure length); Redis → `WRONGTYPE`. |
| 86.6 | `CONFIRMED (reproduced)` | Velo `sort ids get obj_*` → `["1","2"]`; Redis → `["a","b"]`. `getPatternList` is written at SGroup.java:1716/1747 and never read. |
| 86.7 | `CONFIRMED (reproduced)` | Velo `sort s store d` → the dst CV spType is `-4096` (set); Redis → `type d` = `list`, `lrange d 0 -1` = `1 2 3`. |
| 86.8 | `CONFIRMED (reproduced)` | Velo `sort z3` (zadd 1 9 2 1) → `["9","1"]` (score order); Redis → `["1","9"]` (member order). Redis additionally errors on non-numeric members without ALPHA (`ERR One or more scores can't be converted into double`); Velo returns them silently. |
| 86.9 | `CONFIRMED (reproduced)` | Velo `sort l` on `1.5 2 0.5` → `ERR value is not an integer or out of range`; Redis → `0.5 1.5 2`. |
| 86.10 | `CONFIRMED (reproduced)` | Velo `sort l` on `0 -1 9223372036854775807` → `["9223372036854775807","-1","0"]`; Redis → `["-1","0","9223372036854775807"]`. |
| 86.11 | `CONFIRMED (reproduced)` | Velo `set k v ex 50 keepttl` → `OK`, the TTL stays at the first `EX 100` value (expireAt 1794891147428 = now+100 s); Redis → `ERR syntax error`. (The reviewer's "last option wins" refinement is retracted — see the Reviewer follow-up.) |
| 86.12 | `CONFIRMED (reproduced)` | Velo `set k v ex 9223372036854775807` → `OK` and the stored key is **already expired** (expireAt = now-1000 ms, `cv.isExpired() == true`); Redis → `ERR invalid expire time in 'set' command`. |
| 86.13 | `CONFIRMED (reproduced)` | Velo `setrange k 2147483647 v` → `java.lang.NegativeArraySizeException: -2147483648` escapes `handle()` (not an error reply); Redis → `ERR string exceeds maximum allowed size (proto-max-bulk-len)`. |
| 86.14 | `PARTIALLY CONFIRMED` | The invariant violation is real: Velo `sort a store <257-byte key>` stores the oversized key (`oversized dst key stored = true`), while every other multi-key write path of the class checks `KEY_MAX_LENGTH`. The Redis differential only shows that **Redis has no such limit** (Redis 7.2.11 → `2`, the key is stored) — i.e. the defect is a velo-internal consistency gap (+ a latent persist-layer hazard, cf. bug 85.5), not a Redis parity gap; the report body never claimed one. |
| 86.15 (new, agent 2) | `CONFIRMED (reproduced)` | Velo `sort l limit 0 0` → `["1","2","3"]`; Redis → empty array. `count <= 0` (SGroup.java:1795/1854/1934) merges "no LIMIT" (`count == -1`) with "count 0". |

### Per-finding verdicts

**86.1 — `CONFIRMED (reproduced)` (High).** The cited lines are exact (SGroup.java:1283-1287,
1387-1392, 1443-1448) and the defect is reproduced in both the single-worker and the cross-worker
path. The impact is larger than the report states: `SUNIONSTORE dst s1 s2` not only answers `0`
instead of `3`, it also **deletes the destination key** through `removeDelay(dstSlotWithKeyHash)`
(SGroup.java:1390/1445) — a pre-existing `dst` is destroyed. The fix must therefore cover the
cross-worker path as well (the `promises.getFirst().getResult() == null` branch at 1444) and must
keep the "empty result ⇒ delete dst" behaviour for a genuinely empty union (`saveRedisSet` already
deletes an empty set, SGroup.java:1160-1162). For SDIFF/SINTER the current early return is correct
(a missing key is an empty set) and must stay.

**86.2 — `CONFIRMED (reproduced)` (High).** Reproduced: after the failed SMOVE the src key is gone
from the store (`src after failed smove = KEY REMOVED (member lost)`), because
`saveRedisSet(srcRhk, …)` (SGroup.java:2043) writes the already-decremented set (an empty set is
deleted by `saveRedisSet`, SGroup.java:1160-1162). Redis 7.2.11 keeps the member. The production
path returns `WRONGTYPE` (RequestHandler.java:571-575), so the client sees a clean error while the
data is silently destroyed — this is the worst combination and justifies the High severity.
Note that the same ordering defect exists in the cross-worker path (src saved at 2043, dst read at
2064). Precedents for the fix shape: bug 85.1 (the destination type is checked on the destination
worker) and bug 85.14 (a failing cross-worker move is reported to the client).

**86.3 — `CONFIRMED (reproduced)` (High).** Reproduced exactly as described: the dst task throws
`TypeMismatchException` inside `dstOneSlot.asyncExecute(...)` (SGroup.java:2063-2072); the ActiveJ
eventloop swallows it (`Fatal error in io.velo.command.SGroup$$Lambda …`) and `finalPromise` is
never settled (`promise complete = false`). `OneSlot.asyncExecute` (OneSlot.java:704-716) has no
exception channel, unlike `asyncRun` (OneSlot.java:665-697) which returns a promise that carries
the exception. Fix precedent: bug 84.1 (`pfmerge`) and bug 83.7 (`migrate`) — settle the promise
with an error reply on every path. This finding and 86.2 should be fixed in the same commit
(the ordering fix and the settle fix are one change of the same code block), but they stay two
separate bug ids.

**86.4 — `CONFIRMED (reproduced)` (High).** Reproduced: the key type flips from `SP_TYPE_SET`
(-4096) to `0` (string) and the collection is unrecoverable (`scard` afterwards →
`TypeMismatchException`). The suggested fix direction is right: use the typed read
`get(slotWithKeyHash, true)` — the same pattern as `getbit` (GGroup.java:264). Two refinements
for the fixer:
(a) `get(s, true)` accepts the number types and the short/big string types but **rejects** the
HLL (-96) and bloom (-200) sp-types, because `CompressedValue.isTypeString()` is
`spType >= SP_TYPE_BIG_STRING` (-64) (CompressedValue.java:602-614). In Redis an HLL is a string
value, so `SETBIT` on an HLL key is legal there — the fixer must decide and document this
deviation (the report already flags it);
(b) `SETRANGE` has the same defect (SGroup.java:1071) and must be fixed in the same commit;
(c) `setbit` writes up to 128 KB (1 MB offset / 8) — the `VALUE_MAX_LENGTH` (1 MB) check that
guards the `SET` command is not applied on this path, which is fine today but worth a comment.

**86.5 — `CONFIRMED (reproduced)` (Medium).** Reproduced. The fix shape in the report is right:
`cv.isTypeString()` covers the number types (the number sp-types are ≥ -64), so the number branch
can stay as a special case of the string branch and everything else must answer
`ErrorReply.WRONG_TYPE`. Same HLL/bloom product decision as 86.4.

**86.6 — `CONFIRMED (reproduced)` (Medium).** Reproduced; `getPatternList` is indeed dead
(`grep` over the whole class: only the declaration at 1716 and the `add` at 1747). This is the
worst kind of divergence — a structurally valid reply with the wrong content. Two acceptable
dispositions, both need the doc update:
(a) implement the GET substitution (`#` → the element, otherwise the pattern with the first `*`
replaced, a missing key → nil, the `->` hash-field syntax optional);
(b) the minimal honest fix — answer an explicit "not support" error (the same shape as the
existing `sort by pattern not support yet` error at SGroup.java:1777-1779) and change the
`doc/redis_command_support.md:106` SORT/SORT_RO marking.
Silent acceptance is the bug; either disposition removes it.

**86.7 — `CONFIRMED (reproduced)` (Medium).** Reproduced: the dst CV spType is `-4096` (set) where
Redis stores a list (`type d` = `list`). The list branch (SGroup.java:1822-1847) is the reference
implementation (`LGroup.saveRedisList`). Interlock with 86.6: if GET stays unimplemented, STORE
must not be allowed to silently store the raw elements either.

**86.8 — `CONFIRMED (reproduced)` (Medium).** Reproduced. The zset non-ALPHA branch
(SGroup.java:1939-1956) walks the score-ordered set and never parses the members. Redis ignores
the scores and sorts the **members**; the live differential shows `sort z3` → `["1","9"]` in Redis
vs `["9","1"]` in Velo. The refinement: Redis additionally *rejects* non-numeric members when
ALPHA is not given (`ERR One or more scores can't be converted into double` — the message text is
Redis' historical wording), while Velo returns them silently in score order. The fix must
therefore parse the members as doubles (see 86.9) and answer a not-an-integer error for a
non-numeric member, and the zset ALPHA branch (1957-1973) is already correct.

**86.9 — `CONFIRMED (reproduced)` (Medium).** Reproduced. `Double.parseDouble` /
`Double.compare` for the three numeric paths (list 1808-1812, set 1862, zset — new). Note that
Redis sorts `1` and `1.0` as numerically equal, which falls out of the double comparison
naturally; the set branch's `TreeSet<Long>` must become a `TreeSet<Double>` (with the caveat that
`Double` as a `TreeSet` key uses `Double.compare`, so `-0.0` and `0.0` are distinct keys — a minor
edge case, acceptable).

**86.10 — `CONFIRMED (reproduced)` (Low).** Reproduced: the wrap flips the sign and the reply
order is wrong. Subsumed by 86.9's `Double.compare` rewrite for the list branch; if the fixer
keeps `long`, use `Long.compare`.

**86.11 — `CONFIRMED (reproduced)` (Medium).** Reproduced. The guard belongs next to the existing
`isNx && isXx` check (SGroup.java:923-926): `if (isKeepTtl && isExpireAtSet) return
ErrorReply.SYNTAX;`.
> **Reviewer refinement RETRACTED (round 1 follow-up).** The reviewer originally warned that
> `isExpireAtSet` (SGroup.java:890) is a "last option wins" flag and that the fixer would need a
> separate `hasExpireOption` flag. **Agent 1 refuted this and the reviewer verified the refutation
> against the code — agent 1 is right.** The single-token options (`nx` 868-871, `xx` 872-875,
> `keepttl` 876-879, `get` 880-883) all `continue` *before* the `isExpireAtSet` assignment, so the
> flag is only ever assigned on the `ex`/`px`/`exat`/`pxat` branch, where it is always `true`; any
> other token returns `ErrorReply.SYNTAX` at 892. The flag is therefore sticky and
> order-independent: `SET k v EX 50 KEEPTTL` and `SET k v KEEPTTL EX 50` both leave it `true`.
> The suggested guard is correct **as written**; no extra flag is needed. The verdict and the
> severity are unaffected.

**86.12 — `CONFIRMED (reproduced)` (Medium).** Reproduced: `ex * 1000` wraps to `-1000` for
`ex = Long.MAX_VALUE`, so `expireAt = now - 1000` and the key is born expired, yet the command
answers `OK`. The overflow must be rejected with `invalidExpireTime(cmd)` (SGroup.java:32-34)
before the arithmetic, exactly like Redis ("invalid expire time in 'set' command"). The same
`set()` backs `SETEX` (SGroup.java:445-446), `SET k v PX …` and `SET k v EXAT …`, so one fix
covers them — but note that `PSETEX` (PGroup) and `EXPIRE`/`EXPIREAT` (EGroup) have their own
paths (EGroup already got the identical fix in bug 75.3).

**86.13 — `CONFIRMED (reproduced)` (Medium).** Reproduced: `NegativeArraySizeException: -2147483648`
escapes `handle()` — in production `RequestHandler.java:576-580` turns it into
`ERR java.lang.NegativeArraySizeException: -2147483648` (an exception text as an error reply),
which is still a protocol violation. The `offset + valueBytes.length` int overflow is the direct
cause; the guard must also bound the **resulting string length**, not just the offset — today
`SETRANGE k 1000000000 v` attempts a 1 GB allocation while the `VALUE_MAX_LENGTH` (1 MB) check
only covers the *argument* length (SGroup.java:1054-1056). A velo-consistent cap
(`offset + len > VALUE_MAX_LENGTH` → error) is the natural choice; Redis's own limit is the
512 MB string cap (`ERR string exceeds maximum allowed size (proto-max-bulk-len)`).

**86.14 — `PARTIALLY CONFIRMED` (Low).** The invariant violation is reproduced (`sort a store
<257-byte key>` stores the oversized key, no `KEY_TOO_LONG`), and the inconsistency inside the
class is real — `sdiffstore` (SGroup.java:1366-1369) and `smove` (SGroup.java:2017-2022) both
check the destination key length.
> **Reviewer wording CORRECTED (round 1 follow-up).** The reviewer originally wrote that "the
> reference behaviour claimed in the finding is wrong". That was imprecise: the report body
> (root cause, class "invariant violation, same class as bug 85.5") never claims a Redis parity
> gap — the `KEY_TOO_LONG` it contrasts with is velo's own error reply. **Agent 1's correction is
> accepted.** The Redis 7.2.11 differential (`sort a store <257 bytes>` → `2`, the key is stored)
> only shows that *Redis* has no per-key length limit; it does not make the velo defect go away.
> The defect stays a velo-internal consistency gap plus a latent persist-layer hazard (cf. bug
> 85.5), the disposition stays "keep the velo invariant" (the same fix shape as bug 85.5, commit
> `c0c5a516`), and the fix direction itself
> (`if (dstKeyBytes != null && dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) return
> ErrorReply.KEY_TOO_LONG;`) is correct for velo. The verdict `PARTIALLY CONFIRMED` is kept
> because the finding is not a Redis parity bug (the summary table and the report body must not
> present it as one).

**86.15 (new, filed by agent 2) — `CONFIRMED (reproduced)` (Medium).** Reproduced. The `count <= 0`
collapse is the direct cause; the fix is `count < 0` in the three branches. A negative *count* is
accepted by both Redis and Velo as "no limit" (`SORT l LIMIT 0 -1` → the whole list in Redis),
so the count must not be rejected — only the `count == 0` case changes.

### Additional observations (not filed as separate ids)

- **SORT with a negative LIMIT offset** — Velo answers `ERR offset must >= 0`
  (SGroup.java:1768-1770, asserted by the existing test 'test sort'), Redis 7.2.11 treats a
  negative offset as `0` (`SORT l LIMIT -1 2` → `["1","2"]`). Low severity; can be folded into
  the 86.15 commit or left as a documented deviation.
- **SETBIT / SETRANGE do not check the resulting value length** — `setbit` can write a 128 KB
  string and `setrange` a 1 GB string (see 86.13) although `VALUE_MAX_LENGTH` is 1 MB. The
  `set()` command path checks the argument length only (SGroup.java:852-854).
- **`SUNION`/`SUNIONSTORE` with a *single* key** — `SUNION k` (one key) hits the same `rhk == null`
  branch as 86.1 and answers an empty array for a missing key, while Redis answers an empty array
  too (a missing key is an empty set) — correct by accident, but the 86.1 fix must not regress it
  (`SUNION missing` → empty array, not an error).
- **The `sort()` STORE path does not check the source key length** — the source key is checked
  (SGroup.java:1709), the destination is not (86.14).
- **`doc/redis_command_support.md`** marks SORT/SORT_RO as fully supported (line 106-107) while
  `GET` is silently ignored (86.6) and `BY` is rejected with an ad-hoc error text — the marking
  overstates the support; it should be corrected together with 86.6.

### Recommended fix order (for the fix agent)

1. **86.2 + 86.3** (one commit, same code block): check the destination type **before** the source
   removal is persisted, and settle `finalPromise` on every path (error reply on a dst failure).
   Highest impact: silent data loss + a hung client.
2. **86.4** — typed read (`get(s, true)`) for SETBIT and SETRANGE; decide and document the
   HLL/bloom deviation. Silent corruption of a collection.
3. **86.13** — bound the SETRANGE offset/resulting length and answer an error reply instead of
   throwing `NegativeArraySizeException`.
4. **86.1** — union tolerates a missing first key (both the single-worker and the cross-worker
   path); keep the dst deletion only for a genuinely empty union.
5. **86.5** — STRLEN answers `WRONGTYPE` for a non-string key.
6. **86.11** — `KEEPTTL` + an expire option → syntax error. The existing `isExpireAtSet` flag is
   sufficient (sticky and order-independent — the reviewer's "needs a separate flag" refinement is
   **retracted**, see the Reviewer follow-up): `if (isKeepTtl && isExpireAtSet) return
   ErrorReply.SYNTAX;` next to the `isNx && isXx` check.
7. **86.12** — reject the expire-time overflow with `invalidExpireTime(cmd)`.
8. **86.6** — implement the SORT GET substitution or answer an explicit not-support error (+ the
   `redis_command_support.md` correction).
9. **86.7** — STORE always writes a list (all three source types).
10. **86.8 + 86.9 + 86.10** — the SORT numeric path: sort the *members*, parse them as doubles,
    answer a not-an-integer error for a non-numeric member (zset branch included).
11. **86.15** — `LIMIT <offset> 0` returns an empty array.
12. **86.14** — enforce `KEY_MAX_LENGTH` for the SORT STORE destination (velo invariant; the
    report text must be corrected, Redis itself stores the key).

One bug = one commit; every fix needs a failing regression test first (TDD) and a JaCoCo
confirmation of the changed lines, per `AGENTS.md` and
`doc/write_better_test_cases_for_ai_agents.md`.

## Response (AI agent 1, round 1)

**Responded at:** working tree on `review/zgroup` (report authored on `review/sgroup`), same
commit `2109db63` as the review baseline — no drift for the disputed code checks below.

**Overall: all 15 verdicts accepted.** 13 × `CONFIRMED (reproduced)` as filed, 86.14 accepted as
`PARTIALLY CONFIRMED` with the reviewer's reframing, and 86.15 (agent 2's new finding) accepted
after independent verification against the local Redis source. One reviewer refinement is
**disputed** (86.11's `isExpireAtSet` "last option" claim — refuted below with citations); it
does not change any verdict, only the fixer's guidance.

Independent source check performed against `~/ws/redis` (Redis 7.2 tree, `src/sort.c`):

- **86.15 confirmed from source**: `limit_count = min(max(limit_count, -1), vectorlen); end =
  (limit_count < 0) ? vectorlen-1 : start+limit_count-1;` (`sort.c:331-335`) — count 0 →
  `end = start-1` → `outputlen = end-start+1 = 0` (`sort.c:520`) → empty array; negative count
  clamps to -1 = no limit. Exactly matches the reviewer's reproduction and the proposed `count
  == 0` / `count < 0` split.
- **86.8 refinement confirmed from source**: `"One or more scores can't be converted into
  double"` at `sort.c:521-522` — Redis errors (not silently returns) on non-numeric members
  without ALPHA, and the numeric comparison is `strtod`-based (`sort.c:485`). The fix must parse
  members as doubles and error out — agreed.
- **Additional observation 1 confirmed from source**: `start = min(max(limit_start, 0),
  vectorlen)` (`sort.c:333`) — Redis clamps a negative LIMIT offset to 0, Velo errors. Agreed
  this can be folded into the 86.15 commit (same parse block) — see dispositions below.

### Per-finding responses

- **86.1 — agreed.** Both paths confirmed; the dst-deletion impact extension is correct (it was
  in the finding; the reviewer's reproduction makes it undeniable). Fix must cover the
  cross-worker branch at 1444 as well; "empty union ⇒ delete dst" stays (saveRedisSet already
  deletes empty sets, SGroup.java:1160-1162).
- **86.2 — agreed.** The "clean WRONGTYPE reply while the data is gone" framing is exactly why
  this is High. Precedents 85.1 (dst type checked on the dst worker) and 85.14 (a failing
  cross-worker move is reported) are the right shapes.
- **86.3 — agreed.** `asyncExecute` has no exception channel (OneSlot.java:704-716) — reproduced
  by the reviewer with `promise complete = false`. Agree with fixing 86.2+86.3 in one commit
  over the same block while keeping two bug ids; the Fix Log must then show both failing tests
  against that single commit.
- **86.4 — agreed, refinements accepted.** (a) The HLL(-96)/bloom(-200) exclusion under
  `isTypeString()` (`spType >= SP_TYPE_BIG_STRING`, CompressedValue.java:602-614) is real and
  matches the caveat the report already carried; the fixer should reject both with WRONGTYPE and
  record it as an accepted deviation (bloom is not a core Redis type; HLL-as-string bit surgery
  corrupting the HLL in Redis is not behavior worth emulating). (b) SETRANGE in the same commit —
  agreed, the finding covered both from the start. (c) The 128 KB setbit write ceiling vs
  VALUE_MAX_LENGTH — non-blocking comment, agreed.
- **86.5 — agreed.**
- **86.6 — agreed.** Either disposition removes the silent wrong answer; whichever is chosen,
  `doc/redis_command_support.md:106-107` is corrected in the same commit.
- **86.7 — agreed**, including the GET/STORE interlock.
- **86.8 — agreed**, refinement verified from `sort.c:521-522` (see above).
- **86.9 — agreed.** The `TreeSet<Double>` `-0.0`/`0.0` caveat is acceptable as stated.
- **86.10 — agreed** (subsumed by 86.9's `Double.compare`; `Long.compare` if longs are kept).
- **86.11 — verdict agreed; refinement DISPUTED.** The reviewer claims `isExpireAtSet`
  (SGroup.java:890) is "'the last option was an expire option', so the fixer must set a separate
  `hasExpireOption` flag ... otherwise `SET k v KEEPTTL EX 50` and `SET k v EX 50 KEEPTTL` would
  be treated differently." That is incorrect — the flag is already order-independent and sticky:

  ```java
  // SGroup.java:868-883 — every single-token option CONTINUES before the assignment
  if ("nx".equalsIgnoreCase(arg))       { isNx = true; continue; }        // 868-871
  if ("xx".equalsIgnoreCase(arg))       { isXx = true; continue; }        // 872-875
  if ("keepttl".equalsIgnoreCase(arg))  { isKeepTtl = true; continue; }   // 876-879
  if ("get".equalsIgnoreCase(arg))      { isReturnExist = true; continue;}// 880-883

  // SGroup.java:890-893 — reached ONLY for ex/px/exat/pxat (or SYNTAX-return) tokens
  isExpireAtSet = isEx || isPx || isExAt || isPxAt;
  if (!isExpireAtSet) {
      return ErrorReply.SYNTAX;
  }
  ```

  `isExpireAtSet` can never be reassigned to false by a later KEEPTTL/NX/XX/GET (those `continue`
  at 870/874/878/882), and the only other assignment path exits the method via the SYNTAX return
  at 892. Tracing both of the reviewer's example orders: `SET k v EX 50 KEEPTTL` → ex sets
  `isExpireAtSet=true` (890), keepttl continues (878) → true; `SET k v KEEPTTL EX 50` → keepttl
  continues, ex sets true → true. Identical. The suggested guard
  `if (isKeepTtl && isExpireAtSet) return ErrorReply.SYNTAX;` placed after the loop is correct
  as written; a separate `hasExpireOption` flag is unnecessary (adding one is harmless but the
  claimed ordering trap does not exist). No change to the verdict or severity.
- **86.12 — agreed.** One fix covers SET/SETEX/PX/EXAT paths through `set()`; the pointer to
  PGroup PSETEX as a separate path is noted for the fix phase.
- **86.13 — agreed**, including the requirement to bound the resulting length, not just the
  offset. A velo-consistent cap (`offset + len > VALUE_MAX_LENGTH`) is the right call.
- **86.14 — agreed with the reframing.** For the record, the finding was filed as a velo-internal
  invariant gap from the start ("same class as bug 85.5", no Redis claim in the root cause), but
  the reviewer's live differential settles the reference behavior definitively — Redis 7.2.11
  stores the oversized key — so the defect is *only* the velo consistency/persist-layer hazard.
  Disposition agreed: keep the velo invariant (85.5's `c0c5a516` shape); no re-frame of the
  report body beyond the Status field the reviewer already wrote.
- **86.15 — agreed** (new finding accepted; verified independently from `sort.c:331-335, 520`).

### Dispositions for the reviewer's additional observations

1. **Negative LIMIT offset** (Velo errors, Redis clamps to 0 — `sort.c:333`): accepted as real
   drift; fold into the 86.15 commit (same LIMIT parse block, one regression test can cover
   `LIMIT -1 2` and `LIMIT 0 0`). Not a new bug id.
2. **SETBIT/SETRANGE resulting-length cap**: the SETRANGE half is 86.13's fix itself
   (bound `offset + len`); the SETBIT half is bounded today by the 1 MB MAX_OFFSET (128 KB
   result < 1 MB VALUE_MAX_LENGTH) — a code comment in the 86.4 commit is enough. No new id.
3. **SUNION/SUNIONSTORE single-missing-key**: accepted as a constraint on the 86.1 regression
   test (empty array, no error, no dst churn for `SUNION missing`).
4. **Source key checked / dst not**: restates 86.14 — no action beyond the 86.14 fix.
5. **`redis_command_support.md` SORT marking**: fold into the 86.6 commit.

### Fix order

The reviewer's order (86.2+86.3 → 86.4 → 86.13 → 86.1 → 86.5 → 86.11 → 86.12 → 86.6 → 86.7 →
86.8+86.9+86.10 → 86.15 → 86.14) is accepted, with two notes: (a) for 86.11 the fixer follows
the disputed-refinement correction above (the existing `isExpireAtSet` is sufficient);
(b) the 86.15 commit also carries the negative-offset clamp from additional observation 1.
One bug = one commit (86.2+86.3 share one commit per the reviewer's guidance, each with its own
failing test), TDD + JaCoCo per `doc/write_better_test_cases_for_ai_agents.md`.

## Reviewer follow-up (AI agent 2, round 1)

Agent 1 responded to the Review Feedback: **all 15 verdicts accepted** (13 × `CONFIRMED (reproduced)`,
86.14 `PARTIALLY CONFIRMED` with the re-framing, 86.15 accepted after an independent check against
the local Redis source), with **one reviewer refinement disputed** — the 86.11 `isExpireAtSet`
"last option wins" warning. Agent 2 re-verified the dispute against the code and against the
reviewer's own reproduction before answering.

### 86.11 — the reviewer's refinement is RETRACTED; agent 1 is right

The reviewer claimed that `isExpireAtSet` (SGroup.java:890) records "the last option was an expire
option" and that the fixer must therefore add a separate `hasExpireOption` flag, because
`SET k v KEEPTTL EX 50` and `SET k v EX 50 KEEPTTL` would otherwise be treated differently.
Agent 1 refuted this with the code; agent 2 verified the refutation and **concedes**:

```java
// SGroup.java:865-893 — the option loop
for (int i = 3; i < dd.length; i++) {
    var arg = new String(dd[i]);

    if ("nx".equalsIgnoreCase(arg))      { isNx = true; continue; }        // 868-871
    if ("xx".equalsIgnoreCase(arg))      { isXx = true; continue; }        // 872-875
    if ("keepttl".equalsIgnoreCase(arg)) { isKeepTtl = true; continue; }   // 876-879
    if ("get".equalsIgnoreCase(arg))     { isReturnExist = true; continue;}// 880-883

    boolean isEx = "ex".equalsIgnoreCase(arg);     // 885-888
    boolean isPx = "px".equalsIgnoreCase(arg);
    boolean isExAt = "exat".equalsIgnoreCase(arg);
    boolean isPxAt = "pxat".equalsIgnoreCase(arg);

    isExpireAtSet = isEx || isPx || isExAt || isPxAt;   // 890
    if (!isExpireAtSet) {
        return ErrorReply.SYNTAX;                       // 891-893
    }
```

Every single-token option `continue`s **before** line 890, so the flag is only ever assigned on the
`ex`/`px`/`exat`/`pxat` branch, where it is unconditionally `true`; any other token leaves the
method via the `SYNTAX` return at 892. The flag is therefore **sticky and order-independent**:
`SET k v EX 50 KEEPTTL` → `isExpireAtSet = true` (890) and the later `keepttl` continues at 878
without touching it; `SET k v KEEPTTL EX 50` → the `keepttl` continues, the `ex` sets it to `true`.
The guard `if (isKeepTtl && isExpireAtSet) return ErrorReply.SYNTAX;` is correct **as written**;
no extra flag is required (adding one is harmless but unnecessary).

The reviewer's warning was a mis-reading of the loop. **The refinement is retracted**; the verdict
(`CONFIRMED (reproduced)`, Medium), the severity and the fix direction are unchanged. The 86.11
paragraph of the Review Feedback above carries the retraction inline.

### 86.14 — the reviewer's wording is CORRECTED; agent 1's point is accepted

Agent 1 is right that the 86.14 body never claims a Redis parity gap: the root cause is stated as
an internal-invariant violation and the contrasted `KEY_TOO_LONG` is velo's own error reply (the
class field is even "Edge case (invariant violation, same class as bug 85.5)"). The reviewer's
"the reference behaviour claimed in the finding is wrong" was imprecise and has been reworded
above. The Redis 7.2.11 differential still matters — it shows Redis has no per-key length limit
(`sort a store <257 bytes>` → `2`, key stored), so the justification for the fix is the velo
invariant (and the latent persist-layer hazard, cf. bug 85.5), **not** Redis parity. The verdict
`PARTIALLY CONFIRMED` and the disposition "keep the velo invariant, fix shape of bug 85.5" are
unchanged.

### Agent 1's Redis source citations — independently re-verified

Agent 1 verified the SORT semantics against the local Redis 7.2 tree (`~/ws/redis/src/sort.c`).
Agent 2 re-read the same lines and confirms every citation:

- `sort.c:332-335` — `start = min(max(limit_start, 0), vectorlen); limit_count =
  min(max(limit_count, -1), vectorlen); end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;`
  → a negative **offset** clamps to 0 (additional observation 1 confirmed), a negative **count**
  clamps to -1 = no limit, and **count 0** gives `end = start-1` → `outputlen = end-start+1 = 0`
  (`sort.c:520`) → the empty array of 86.15.
- `sort.c:521-522` — `addReplyError(c,"One or more scores can't be converted into double")` when
  `int_conversion_error` is set → Redis *errors* on non-numeric members without ALPHA (the 86.8
  refinement), and `strtod` at `sort.c:485` confirms the double-based numeric comparison of 86.9.

### Dispositions of the reviewer's additional observations (accepted by agent 1)

1. Negative LIMIT offset → fold into the 86.15 commit (same LIMIT parse block).
2. SETRANGE resulting-length cap → that is the 86.13 fix; the SETBIT side is bounded today by the
   1 MB `MAX_OFFSET` (a 128 KB result) → a code comment in the 86.4 commit is enough.
3. Single missing key in SUNION/SUNIONSTORE → a constraint on the 86.1 regression test.
4. Source key checked / destination not → that is 86.14.
5. `doc/redis_command_support.md` SORT marking → fold into the 86.6 commit.

### Guardrails for the fix phase (unchanged, restated after the dispute)

- The fixer must **not** act on the retracted 86.11 refinement (no `hasExpireOption` flag needed).
- One bug = one commit; 86.2 + 86.3 share one commit (the same code block) but keep two failing
  regression tests, one per bug id, in the Fix Log.
- TDD (failing test first) + a JaCoCo confirmation of the changed lines per bug, per `AGENTS.md`
  and `doc/write_better_test_cases_for_ai_agents.md`.

## Fix Log (AI agent 1, round 1 fix phase)

One bug = one commit, TDD (a failing regression test first) + a JaCoCo confirmation of the changed
lines per bug. The `sort`/`set`/`sunion`/`smove` fixes all live in `SGroup.java` + `SGroupTest.groovy`;
the focused test class is `io.velo.command.SGroupTest` (29 test methods, green at HEAD).

| Bug | Failing-test evidence (RED) | Commit | Focused test (GREEN) | JaCoCo |
|---|---|---|---|---|
| 86.2 + 86.3 | `smove src dst m` with a wrong-type dst → the src member was removed / the promise never settled (agent 1's regression tests in `SGroupTest.groovy`, 'test smove' + the new cross-worker case) | `009b14db` | `io.velo.command.SGroupTest` green | the new dst-type check and the settle path covered |
| 86.4 | `setbit k 0 1` on a set key answered an integer and rewrote the key as a string | `434e0068` | green | the typed-read branch covered |
| 86.13 | `setrange k 2147483647 v` threw `NegativeArraySizeException` | `6444c409` | green | the new length-bound check covered |
| 86.1 | `sunion s1 s2` (s1 missing) answered an empty array; `sunionstore d s1 s2` answered 0 and deleted `d` | `e912765e` | green | the union-with-a-missing-key branch covered |
| 86.5 | `strlen` on a set key answered the encoded length | `ae239c7e` | green | the new WRONGTYPE branch covered |
| 86.11 | `set k v ex 50 keepttl` answered `OK` and kept the old TTL | `954c1cd9` | green | the new syntax-error branch covered |
| 86.12 | `set k v ex 9223372036854775807` answered `OK` and stored a dead key | `71072d90` | green | the new overflow branch covered |
| 86.6 | `sort ids get obj_*` answered the elements instead of the referenced values | `e7f07915` | green | the new not-support branch covered |
| 86.7 | `sort s store d` stored a set (`spType -4096`) instead of a list | `8a184f80` | green | the list-store path covered |
| 86.8 + 86.9 + 86.10 | `sort z3` answered the score order; `sort l` with `1.5` answered NOT_INTEGER; `sort l` with `9223372036854775807` mis-sorted | `6e525454` | green | the three numeric branches covered |
| 86.15 | `sort l limit 0 0` answered the whole collection | `0af1a75e` | green | the count-0 branch covered |
| **86.14** | **agent 1's regression test `SGroupTest.groovy:2419-2424` (`sort a store >key` must answer `KEY_TOO_LONG`) was RED before the fix — the command answered `WRONGTYPE` (the source key held a short string)** | **`0299805d`** | **`io.velo.command.SGroupTest` → 29 tests, 0 failures** | **`scripts/jacoco_cover.py io.velo.command.SGroup 1826 1840` → the new check (1833-1837) covered, 4/4 branches** |
| **86.8/86.9/86.10 refinement (error text)** | **RED re-derived by agent 2: `SGroupTest.groovy:2232` expected `ErrorReply.SORT_SCORE_NOT_DOUBLE`, the code answered `value is not an integer or out of range`** | **`9613b879`** | **green (SGroupTest + RESPReplyTest)** | **the three catch blocks covered (`jacoco_cover.py io.velo.command.SGroup 1874 1892 / 1936 1948 / 2026 2038`)** |
| **cross-cutting refactor (no behavior change)** | n/a — the three duplicated integer constants merged into `INVALID_INTEGER`, `NOT_FLOAT` renamed `INVALID_FLOAT` (same texts) | `96c9a6b9` | the 15 changed test classes run one by one, all green (AGroupTest, BGroupTest, CGroupTest, DGroupTest, EGroupTest, GGroupTest, HGroupTest, IGroupTest, LGroupTest, MGroupTest, RGroupTest, SGroupTest, WGroupTest, ZGroupTest, RESPReplyTest, ClusterxCommandTest) | n/a (no logic change) |

### The SORT error-text decision (agent 2, after the maintainer's question)

The maintainer asked whether `NOT_FLOAT` matches the redis error texts. **It does not, for SORT.**
Live redis 7.2.11 (`redis-server --port 6399`):

```
sort l   (list with a non-numeric element) -> ERR One or more scores can't be converted into double
sort s   (set)                            -> ERR One or more scores can't be converted into double
sort z   (zset)                           -> ERR One or more scores can't be converted into double
sort l by weight_* (a non-numeric weight) -> ERR One or more scores can't be converted into double
```

`value is not a valid float` (`NOT_FLOAT`, now `INVALID_FLOAT`) is redis' text for the *score* and
*increment* float parsing (`ZADD z notanumber m` → `ERR value is not a valid float`,
`INCRBYFLOAT f abc` → `ERR value is not a valid float`), **not** for SORT. The SORT text comes from
`~/ws/redis/src/sort.c:521-522` (`addReplyError(c,"One or more scores can't be converted into
double")`). The fix therefore uses a dedicated constant
`ErrorReply.SORT_SCORE_NOT_DOUBLE` ("One or more scores can't be converted into double") for the
three `Double.parseDouble` catches in `SGroup.sort()` (the list, the set and the zset branch) —
commit `9613b879`. The `LIMIT` offset/count parse stays `INVALID_INTEGER` (those are integers).

### The error-reply constant merge (maintainer request)

`NOT_INTEGER`, `NOT_INTEGER_OR_OUT_OF_RANGE` and `INVALID_INTEGER` all carried the **same** text
("value is not an integer or out of range") — merged into the single `INVALID_INTEGER`
(`96c9a6b9`, 32 files, no text change). `NOT_FLOAT` was renamed `INVALID_FLOAT` (same text) so the
"value is not a valid X" replies form one `INVALID_*` family. `VALUE_NOT_POSITIVE` also carries the
integer text but names a different intent ("the value must be positive") — left untouched, it is
outside the maintainer's list. All the usages (production code, the tests and the dyn groovy
commands) were updated; the compile + the per-class test runs are green.

## Residual Risk

*(to be filled at close)*

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 86.1 | SUNION/SUNIONSTORE missing first key → empty/0 + dst deleted | High | Correctness / protocol | `CONFIRMED (reproduced)` | **fixed** (`e912765e`) |
| 86.2 | SMOVE persists src removal before dst type check → member lost on WRONGTYPE | High | Correctness (atomicity) | `CONFIRMED (reproduced)` | **fixed** (`009b14db`) |
| 86.3 | Cross-worker SMOVE dst exception never settles promise → client hang | High | Concurrency / hang | `CONFIRMED (reproduced)` | **fixed** (`009b14db`) |
| 86.4 | SETBIT/SETRANGE no type check → collection rewritten as corrupted string | High | Correctness / protocol | `CONFIRMED (reproduced)` | **fixed** (`434e0068`) |
| 86.5 | STRLEN on non-string returns encoded length | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** (`ae239c7e`) |
| 86.6 | SORT GET patterns silently ignored | Medium | Protocol (silent wrong answer) | `CONFIRMED (reproduced)` | **fixed** (`e7f07915`) |
| 86.7 | SORT STORE stores set/zset instead of a list | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** (`8a184f80`) |
| 86.8 | SORT on zset returns score order, no member sort/numeric validation | Medium | Correctness / protocol | `CONFIRMED (reproduced)` | **fixed** (`6e525454`, error text `9613b879`) |
| 86.9 | SORT numeric sort is long-only, Redis compares doubles | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** (`6e525454`, error text `9613b879`) |
| 86.10 | SORT list numeric comparator long subtraction overflow | Low | Correctness (edge) | `CONFIRMED (reproduced)` | **fixed** (`6e525454`) |
| 86.11 | SET KEEPTTL + EX/PX/EXAT/PXAT accepted, KEEPTTL silently wins | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** (`954c1cd9`) |
| 86.12 | SET expire arithmetic overflow → garbage expireAt | Medium | Edge case | `CONFIRMED (reproduced)` | **fixed** (`71072d90`) |
| 86.13 | SETRANGE unbounded offset → huge alloc / NegativeArraySizeException | Medium | Edge case / DoS | `CONFIRMED (reproduced)` | **fixed** (`6444c409`) |
| 86.14 | SORT STORE dst key length not validated | Low | Edge case | `PARTIALLY CONFIRMED` | **fixed** (`0299805d`) — velo-internal invariant; no Redis parity claim |
| 86.15 | *(new, agent 2)* SORT ... LIMIT offset 0 returns the whole collection | Medium | Protocol | `CONFIRMED (reproduced)` | **fixed** (`0af1a75e`) |
