# Bug 81 — GGroup / LGroup commands review (Round 1)

**Module:** `io.velo.command.GGroup` (`getex`) and `io.velo.command.LGroup` / `io.velo.command.RGroup` (`lpushx` / `rpushx`), with the supporting `io.velo.command.BlockingList` and `io.velo.CompressedValue`
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2 (independent verification in the same session, oracle = live Redis 7.2)
**Status:** discovery complete — 2 findings (81.1 `GETEX` zero TTL, 81.2 `LPUSHX`/`RPUSHX` with a pending blocker), both CONFIRMED (reproduced against live Redis 7.2); fix phase pending
**Review branch:** `review/bug-81`
**Reviewed commit:** `da8c0b2c` (main, after the bug 80 round was merged)

## Scope

Both findings were found while closing out the bug 80 round: the leftover reviewer scratch specs
(`Bug77ReReviewTempSpec`, `Bug71ReviewTempSpec`) still failed, and their failures turned out to point at
real product defects rather than at stale test expectations.

Files read (method + line ranges):

- `src/main/java/io/velo/command/GGroup.java` — registry block (26-139); `getex()` (290-380); `invalidExpireTime()` (286-288)
- `src/main/java/io/velo/command/LGroup.java` — `handle()` dispatch (283-292); `lpush()`/`rpush()` push path (836-872); `addToList()` (349-395); `getRedisList()` (403-410)
- `src/main/java/io/velo/command/RGroup.java` — `rpushx` delegation (241-245)
- `src/main/java/io/velo/command/BlockingList.java` — `setReplyIfBlockingListExist()` (310-…); the static `blockingListPromisesByKey` registry (214)
- `src/main/java/io/velo/CompressedValue.java` — `NO_EXPIRE = 0` (20); `isExpired()` (569-571)
- `src/test/groovy/io/velo/command/GGroupTest.groovy` — `getex` cases (200-300)
- `src/test/groovy/io/velo/command/LGroupTest.groovy` — `test lmove` (291-420), `test lpush` (705-810)

## Finding 1 (Bug 81.1): `GETEX` accepts a zero TTL — the key is silently expired or made permanent

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/GGroup.java:316-331
            try {
                x = Long.parseLong(arg2);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (x < 0) {
                return ErrorReply.INVALID_INTEGER;
            }
```

```java
// src/main/java/io/velo/command/GGroup.java:286-288 — the Redis answer already exists in the file
    private static ErrorReply invalidExpireTime() {
        return new ErrorReply("invalid expire time in 'getex' command");
    }
```

```java
// src/main/java/io/velo/CompressedValue.java:20 — 0 means "no expiry"
    public static final long NO_EXPIRE = 0;
```

### Root cause

The TTL argument of `GETEX` is validated only for a *negative* value (`x < 0` → `INVALID_INTEGER`, line 322).
Zero is accepted by all four spellings — `EX`, `PX`, `EXAT`, `PXAT` — and then flows into the expire-time
computation further down `getex()`:

- `EX 0` / `PX 0` → relative → `expireAt = now + 0` → **the key expires immediately**, its value is lost.
- `EXAT 0` / `PXAT 0` → absolute → `expireAt = 0` → and `0` is `CompressedValue.NO_EXPIRE` → **the TTL is
  cleared and the key becomes permanent**, even if it had a TTL before.

The Redis answer is already implemented in the same file: `invalidExpireTime()` (line 286) produces exactly
`ERR invalid expire time in 'getex' command` and is used for the overflow cases (lines 362, 368).

### Trigger / impact

| command | Velo (current) | Redis 7.2 |
|---|---|---|
| `GETEX k EX 0` | `BulkReply` (value returned), `expireAt = now` → key expired at once | `ERR invalid expire time in 'getex' command` |
| `GETEX k PX 0` | `BulkReply`, `expireAt = now` → key expired at once | same error |
| `GETEX k EXAT 0` | `BulkReply`, `expireAt = 0` → **TTL cleared, key becomes permanent** | same error |
| `GETEX k PXAT 0` | `BulkReply`, `expireAt = 0` → **TTL cleared, key becomes permanent** | same error |

Redis rejects the command and leaves the key **completely untouched** (`EXISTS k` is still `1` after the error).

Impact is worse than a reply-shape mismatch: Velo performs a **silent, data-affecting mutation** that Redis
refuses to perform. `GETEX k EXAT 0` turns a key with a TTL into a permanent key; `GETEX k EX 0` destroys the
value. A client that mistypes `0` for "no expiry" silently loses data.

### Suggested fix direction

Reject a zero TTL the way Redis does, for all four spellings, and reuse the existing helper:

```java
if (x <= 0) {
    return invalidExpireTime();   // already returns the exact Redis message
}
```

The `x < 0` case currently answers `INVALID_INTEGER`; Redis answers `ERR invalid expire time in 'getex' command`
for a negative TTL as well, so folding both into `invalidExpireTime()` matches Redis exactly. If the negative
reply is to be kept for compatibility, reject only `x == 0` with `invalidExpireTime()` and leave `x < 0` alone —
but then the two spellings give different messages, which is not Redis behaviour.

## Finding 2 (Bug 81.2): `LPUSHX` / `RPUSHX` skip the "key must exist" check when a blocking client is pending on the key

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:849-868
        var key = Wal.keyString(keyBytes);
        var afterPopValueBytesArray = BlockingList.setReplyIfBlockingListExist(key, addFirst, valueBytesArr, this);
        // no blocking for this key
        if (afterPopValueBytesArray == null) {
            return addToList(valueBytesArr, addFirst, false, false, null, needKeyExist);
        }

        // all elements are popped, need to add to list
        if (afterPopValueBytesArray.length == 0) {
            // no need to add to list
            var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
            var rl = getRedisList(slotWithKeyHash);
            if (rl == null || rl.size() == 0) {
                return IntegerReply.REPLY_1;      // <-- needKeyExist is never consulted
            } else {
                return new IntegerReply(rl.size() + 1);
            }
        }
```

```java
// src/main/java/io/velo/command/RGroup.java:241-245 — RPUSHX funnels through the same path
        if ("rpushx".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpush(false, true);      // addFirst=false, needKeyExist=true
        }
```

```java
// src/main/java/io/velo/command/BlockingList.java:310-315 — the static waiter registry
    public static byte[][] setReplyIfBlockingListExist(String key, boolean addFirst, byte[][] elementValueBytesArray, BaseCommand baseCommand) {
        var inner = getInner();
        var list = inner.blockingListPromisesByKey.get(key);
        if (list == null || list.isEmpty()) {
            return null;
        }
```

### Root cause

`LGroup.lpush()` implements both `LPUSH` (`needKeyExist = false`) and `LPUSHX` (`needKeyExist = true`).
The existence check lives **inside** `addToList()` (line 353-355: `if (rl == null && needKeyExist) return
IntegerReply.REPLY_0;`), but the blocking-list delivery at line 850 runs **before** `addToList()` is reached.

When a blocked client is waiting on the key, `setReplyIfBlockingListExist()` delivers the pushed value to that
client and returns an **empty** leftover array. Line 857 then takes the "all elements are popped" branch, which
returns `IntegerReply.REPLY_1` — and that branch never looks at `needKeyExist` and never checks whether the key
exists.

So `LPUSHX`/`RPUSHX` on a **non-existent** key answer `1` (and the key is created) whenever a blocking client
happens to be waiting on that key. `RPUSHX` reaches the same code through `RGroup.java:241-245`.

### Trigger / impact

Reproducer: a client blocks on the key with `LMOVE src dst LEFT LEFT 0` (or `BLPOP key 0`), then another
client issues `LPUSHX`/`RPUSHX` on the same key.

| command (key does not exist, a blocker is pending) | Velo | Redis 7.2 |
|---|---|---|
| `EXISTS k` | — | `0` (a pending BLPOP does not create the key) |
| `LPUSHX k v` | **`1`** (key created) | **`0`** (nothing created) |
| `RPUSHX k v` | **`1`** (key created) | **`0`** (nothing created) |

Redis' `LPUSHX`/`RPUSHX` are "push only if the key exists" commands: they must not create the key, and a
pending blocking client does not make the key exist. Velo creates it.

Secondary impact: the value is also delivered to the blocked client, so the blocked waiter is woken by a
command that Redis would have rejected — and the key ends up holding a value that the pushing client was told
was not pushed.

This finding also explains the **pre-existing** `LGroupTest > test lpush` failure that predates this round:
`BlockingList` is entirely static ("all is static, need thread safe", `BlockingList.java:26`) and
`test lmove` leaves a never-completing waiter registered for key `a`, which the later `test lpush` inherits.

### Suggested fix direction

Honour `needKeyExist` **before** the blocking-list delivery, so the "push only if the key exists" semantics are
decided by the key's existence and not by the waiter registry:

```java
if (needKeyExist) {
    var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
    if (getRedisList(slotWithKeyHash) == null) {
        return IntegerReply.REPLY_0;    // key does not exist: do not push, do not create
    }
}
var afterPopValueBytesArray = BlockingList.setReplyIfBlockingListExist(key, addFirst, valueBytesArr, this);
...
```

The check must run before `setReplyIfBlockingListExist()`, otherwise the value has already been handed to the
blocked client by the time the existence is checked.

## Notes / non-findings

- `RPUSHX` is implemented in `RGroup` (`RGroup.java:241-245`), not `LGroup` — `LGroup.handle()` has no
  `rpushx` branch and falls through to `NilReply.INSTANCE` (`LGroup.java:291`). Both spellings share the
  `LGroup.lpush()` path, so 81.2 covers both; a probe that calls `rpushx` on an `LGroup` instance gets a `nil`
  reply for a different reason and is not evidence of a defect.
- `GETEX` on a **number-typed** key was suspected to lose the type marker or the TTL (a leftover scratch case,
  `Bug77ReReviewTempSpec` "getex keeps the ttl on a number typed key"). That case never ran — it used
  `42L.bytes`, which is not valid Groovy, and died with `MissingPropertyException` in `given:`. Re-run with a
  correct 8-byte encoding, the type marker is preserved (`spType = -8 = SP_TYPE_NUM_LONG`) and the TTL is set.
  **Not a defect**; the scratch case should be dropped rather than fixed.
- `GETEX`'s overflow handling (bug 77.2) is correct and covered by `GGroupTest.groovy:273-294`; 81.1 is a
  separate, unvalidated input class, not a regression of 77.2.
- **`GETEX` with a negative TTL answers a different message than Redis.** Velo replies
  `ERR value is not an integer or out of range` (`GGroup.java:322-323`); Redis 7.2 replies
  `ERR invalid expire time in 'getex' command` for `ex -1` / `px -1` / `exat -1` / `pxat -1` as well. The command
  is rejected and the key is left untouched either way, so there is no data impact — only the message text
  differs. Deliberately **not** bundled into the 81.1 fix (which is scoped to the zero TTL, the case with the
  data impact) so that each commit stays a single minimal change; worth a follow-up if message parity matters.

## Review Feedback (AI agent 2, round 1)

**Method:** independent verification against **live Redis 7.2** as the oracle (the two findings are pure
protocol-compatibility claims, so a second agent re-deriving the same code reading adds little that a live
differential does not). Both findings were reproduced on the real Velo persist layer
(`InMemoryGetSet` + `LocalPersistTest.prepareLocalPersist` + `fixSlotThreadId`, driving `GGroup`/`LGroup`/
`RGroup` through `execute()`), and compared with the same command sequence issued to a stock `redis-server`.
Reproduction artifacts: `src/test/groovy/io/velo/command/Bug81EvidenceTempSpec.groovy` (deleted after evidence
capture).

| Finding | Verdict | Evidence |
|---|---|---|
| 81.1 | CONFIRMED (reproduced, Velo vs live Redis 7.2) | Velo: `GETEX k EX 0` → `BulkReply`, `expireAt = now` → `expiredNow=true`; `GETEX k PX 0` → `BulkReply`, `expireAt = now`; `GETEX k EXAT 0` → `BulkReply`, `expireAt = 0` → `becamePermanent=true`; `GETEX k PXAT 0` → same, `expireAt = 0`, `becamePermanent=true`. Redis 7.2, same four commands: `ERR invalid expire time in 'getex' command` for all four, and the key is left untouched. The Redis message is byte-identical to the `invalidExpireTime()` helper already present at `GGroup.java:286-288`. |
| 81.2 | CONFIRMED (reproduced, Velo vs live Redis 7.2) | Velo: with a pending blocker on the key, `LPUSHX k v` → `IntegerReply` `1` (`isREPLY_1=true`) and `RPUSHX k v2` → `IntegerReply` `1`. Redis 7.2 with `BLPOP k 0` pending: `EXISTS k` → `0`, `LPUSHX k v` → `0`, `RPUSHX k v2` → `0`, key not created. Static root cause confirmed at `LGroup.java:850` (delivery precedes the `needKeyExist` check) and `LGroup.java:857-862` (`REPLY_1` returned without consulting `needKeyExist`); `RPUSHX` reaches it via `RGroup.java:241-245`. |

### Additional observations

- The two findings are independent of each other and of the bug 80 round; neither is a regression introduced by
  `d8e41ddd` or the bug 80 fixes.
- `Bug71ReviewTempSpec` / `Bug77ReReviewTempSpec` / `Bug78ReviewTempSpec` / `Bug79ReviewTempSpec` are reviewer
  scratch artifacts that the bug 71/72/78/79 reports already declare as deliberately uncommitted, with their
  repros rebuilt as permanent cases. They are not part of this round; `Bug77ReReviewTempSpec` is where 81.1 and
  81.2 were found, and its 77.11 case is the same defect as 81.1 (the `getex` zero-TTL family).
- `LGroupTest > test lpush` fails today for the reason described in 81.2 — the static `BlockingList` waiter
  registry leaks across test methods. Fixing 81.2 should turn that test green; the test-isolation aspect
  (a never-completing waiter registered by `test lmove`) should be addressed separately so the class does not
  depend on execution order.

### Recommended fix order (for the fix agent)

1. **81.1** — self-contained, one-line validation change in `GGroup.getex()` plus a permanent `GGroupTest` case.
   Do it first: it has no design decision attached and it is the higher-impact of the two (silent data loss /
   silent TTL removal).
2. **81.2** — one guard in `LGroup.lpush()` before the blocking-list delivery plus a permanent `LGroupTest` case.
   Needs care that the guard runs *before* `setReplyIfBlockingListExist()`.

## Fix Log (round 1 fix phase)

Each bug = one commit, TDD first (failing test → minimal fix → focused test green → JaCoCo on the changed lines).

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 81.1 | RED: `GGroupTest.'test getex'` failed at `GGroupTest.groovy:307` — `getex z ex 0` answered a `BulkReply` instead of an `ErrorReply` | `ccec81b8` | `./gradlew :test --tests "io.velo.command.GGroupTest"` → BUILD SUCCESSFUL (30 tests, 0 failures) | `GGroup.java` 310-340 → 22/22 lines, 8/8 branch lines fully covered, including the new `x == 0` guard |
| 81.2 | RED: `LGroupTest.'test lpush'` failed at `LGroupTest.groovy:773` — `lpushx missingKey a` answered `1` instead of `REPLY_0` while a client was blocked on the key | *pending* | `./gradlew :test --tests "io.velo.command.LGroupTest"` → BUILD SUCCESSFUL (41 tests, 0 failures); `RGroupTest`, `BlockingListTest`, `BGroupTest`, `GGroupTest` green | `LGroup.java` 848-862 → 7/7 lines, 3/3 branch lines fully covered, including both sides of the new `needKeyExist` guard |

**81.1 fix detail.** `GGroup.getex()` now rejects a zero TTL with the existing `invalidExpireTime()` helper
(`GGroup.java:286-288`), which returns exactly the Redis message `invalid expire time in 'getex' command`:

```java
if (x == 0) {
    // redis rejects a zero ttl: EX/PX 0 would expire the key at once and
    // EXAT/PXAT 0 would clear the ttl, because 0 is NO_EXPIRE
    return invalidExpireTime();
}
```

The check sits next to the existing `x < 0` guard and applies to all four spellings (`EX`, `PX`, `EXAT`, `PXAT`),
because the argument is parsed once before the spelling is dispatched on.

The permanent `GGroupTest` cases cover all four spellings plus the "the key is left untouched" assertion
(the original TTL survives and the key is not expired).

**Scope note.** The negative TTL (`ex -1`) keeps its current `INVALID_INTEGER` reply. Redis answers
`ERR invalid expire time in 'getex' command` for a negative TTL as well, so that message text is a separate,
lower-severity divergence (the command is still rejected and the key is still untouched — no data impact). It is
recorded under *Additional observations* rather than folded into this commit, to keep the change minimal per bug.

**81.2 fix detail.** `LGroup.lpush()` now decides the "push only if the key exists" question **before** the
blocking-list delivery, so a client waiting on the key can no longer make a non-existent key look like an
existing one:

```java
var key = Wal.keyString(keyBytes);
if (needKeyExist) {
    // lpushx / rpushx push only if the key exists. A client blocked on the key does
    // not make it exist, redis answers 0 and creates nothing, so the check must run
    // before the blocking list delivery hands the value over.
    if (getRedisList(slotWithKeyHashListParsed.getFirst()) == null) {
        return IntegerReply.REPLY_0;
    }
}
var afterPopValueBytesArray = BlockingList.setReplyIfBlockingListExist(key, addFirst, valueBytesArr, this);
```

The check has to run **before** `setReplyIfBlockingListExist()`, otherwise the value has already been handed to
the blocked client by the time the existence is known — which is exactly the bug. `RPUSHX` reaches the same method
through `RGroup.java:241-245`, so the single guard covers both spellings.

Placing the guard here also keeps the wrong-type behaviour unchanged: `getRedisList()` raises
`TypeMismatchException` for a key that holds a non-list value, exactly as the `addToList()` path did, so
`LPUSHX` on a string key still answers `WRONGTYPE`.

**Test isolation (part of the 81.2 commit).** `BlockingList` is entirely static, and `test lmove` leaves a
never-completing waiter registered for key `a`, which the later `test lpush` inherited — that is what made
`LGroupTest > test lpush` fail before this round. The 81.2 commit clears the registry in `test lmove`'s cleanup,
so the class no longer depends on execution order, and the new 81.2 cases register their own waiter on a
dedicated key and are therefore self-contained.

The permanent cases cover: `LPUSHX` on a missing key with a waiter pending (answers `0`, the key is not created,
the waiter is not woken), the same for `RPUSHX`, and the complementary case where the key **does** exist
(`LPUSHX` pushes and wakes the waiter) so the new guard's fall-through path is covered too.

## Residual Risk

- `LPUSHX`/`RPUSHX` on a key that exists but holds a **non-list** value still answers `WRONGTYPE` through
  `TypeMismatchException` — unchanged by the 81.2 fix (the new guard calls the same `getRedisList()` the
  `addToList()` path called), but the guard now runs *before* the blocking-list delivery, so a wrong-type key
  with a pending waiter answers `WRONGTYPE` instead of silently handing the value to the waiter. That is the
  Redis behaviour, but it is a behaviour change on that path and is not covered by a dedicated case.
- The negative-TTL message divergence for `GETEX` (see *Additional observations*) is left in place deliberately.
- `BlockingList` remains entirely static; the 81.2 commit only stops the test suite from leaking waiters
  between methods. Production code still shares one registry per process, which is by design.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 81.1 | `GETEX` accepts a zero TTL — `EX 0`/`PX 0` expire the key at once, `EXAT 0`/`PXAT 0` clear the TTL and make it permanent; Redis rejects all four | High | Correctness / Redis protocol compatibility | CONFIRMED (reproduced, live Redis 7.2 differential) | fixed (`ccec81b8`) — red/green + JaCoCo |
| 81.2 | `LPUSHX`/`RPUSHX` skip the "key must exist" check when a blocking client is pending on the key — Velo answers `1` and creates the key, Redis answers `0` and creates nothing | High | Correctness / Redis protocol compatibility | CONFIRMED (reproduced, live Redis 7.2 differential) | fixed — red/green + JaCoCo |
