# Bug 82 — LGroup commands review (Round 1)

**Module:** `io.velo.command.LGroup` (all `L*` list commands: `lastsave`, `lindex`, `linsert`, `llen`, `lmove`, `lmpop`, `lpop`, `lpos`, `lpush`, `lpushx`, `lrange`, `lrem`, `lset`, `ltrim`, `load-rdb`)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2 (independent verification in the same session, oracle = live Redis 7.2.11)
**Status:** fix phase complete — 6/6 findings CONFIRMED (reproduced) by agent 2; 5 fixed in 5 commits (`1957a4e5`…`195d89b5`), 82.6 reverted by product decision (`a1a08492` → `56fb3137`); re-review pending
**Review branch:** `review/lgroup`
**Reviewed commit:** `7d8ce05aba8e59e6a2b4647242813b787c4522be` (main)

> Delta declaration. This is the first round whose target is the **class `LGroup`** itself. Two earlier
> rounds touched the same file and are **not** re-reported here:
> - `doc/bug_reviews/bug_41_list_module_review_round_1.md` (list module, round 1) — all 5 findings fixed
>   (LINSERT `-1` on a missing pivot, LPOS `COUNT` reply shape, blocking-list timeout semantics,
>   LMOVE/RPOPLPUSH atomicity + empty-source cleanup, LMPOP/BLMPOP dispatch). Its Review Feedback also
>   noted that "LPOS silently ignores unknown option tokens because the switch has no default branch" as
>   an **out-of-scope observation** — that observation is now filed as **82.5**.
> - `doc/bug_reviews/bug_81_ggroup_lgroup_commands_review_round_1.md` (GGroup/LGroup, round 1) —
>   81.1 `GETEX` zero TTL, 81.2 `LPUSHX`/`RPUSHX` under a pending blocker; both fixed.
> Also not re-reported (checked, correct, or product decisions): `LINSERT` missing-pivot `-1` (fixed in
> bug 41), `LPUSHX`/`RPUSHX` under a pending blocker (fixed in bug 81), `LMPOP` `WRONGTYPE`/`COUNT<=0`
> (fixed in bug 41), `bgsave` being a no-op (bug 72 non-finding, `lastBgSaveMillis` only feeds
> `lastsave`).

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/LGroup.java` — registry block (26-147); `parseSlots()` (166-218);
  `handle()` dispatch (225-292); `lindex()` (294-347); `addToList()` (349-394); `getRedisList()` (403-410);
  `saveRedisList()` (412-424); `linsert()` (434-464); `llen()` (466-485); `lmove()` (487-549);
  `lpop()` (551-615); `lmpop()` (617-697); `lpos()` (699-827); `lpush()` (829-885); `lrange()` (887-936);
  `lrem()` (938-990); `lset()` (992-1041); `ltrim()` (1043-1097); `loadRdb()` (1099-1127)
- `src/main/java/io/velo/command/IndexStartEndReset.java` — `reset()` (29-53) (shared with `LRANGE`/`HGETALL`)
- `src/main/java/io/velo/command/BlockingList.java` — `setReplyIfBlockingListExist()` (310-472),
  `PromiseWithLeftOrRightAndCreatedTime` (50-151)
- `src/main/java/io/velo/command/RGroup.java` — `rpushx` delegation (241-245), `move()` (483-519),
  `moveBlock()` (542-588)
- `src/main/java/io/velo/command/BGroup.java` — `blmove`/`brpoplpush` → `LGroup.lmove(true)` delegation
  (285-311), `lastBgSaveMillis` (255) and its writers (269; `SGroup.java:400`)
- `src/main/java/io/velo/type/RedisList.java` — `LIST_MAX_SIZE` (21), `encode()` (124-161),
  `decode()` (185-220), `iterate()` (239-275), `getSizeWithoutDecode()` (167-170)
- `src/main/java/io/velo/BaseCommand.java` — `get()` / `getCv()` type checks (756-798, 878-890),
  `removeDelay()` (1272-1283), `execute()` test entry point (454-500)
- `src/main/java/io/velo/RequestHandler.java` — command dispatch and `TypeMismatchException` → `WRONG_TYPE`
  mapping (234-235, 566-572)
- `src/test/groovy/io/velo/command/LGroupTest.groovy` — `test lindex` (107-163), `test linsert` (165-),
  `test lmove` (291-424), `test lpop` (425-526), `test lpos` (528-706), `test lpush` (708-853),
  `test lrange` (854-896), `test lrem` (898-967), `test lset` (969-1036), `test ltrim` (1047-1111),
  `test lmpop` (1216-1366)

Reference docs (ground truth):

- Redis 7.2 `src/t_list.c` (upstream source of truth for the list command semantics):
  `ltrimCommand()`, `lposCommand()`, `lindexCommand()`, `lsetCommand()`, `lremCommand()`,
  `addListRangeReply()` — fetched from `https://raw.githubusercontent.com/redis/redis/7.2/src/t_list.c`
- Redis 7.2 list command reference: `https://redis.io/docs/latest/commands/ltrim/`,
  `.../lpos/`, `.../lindex/`, `.../lastsave/`
- `doc/design/04_command_processing_design.md` — command dispatch / slot routing model
- `doc/redis_command_support.md` — list-section support matrix (all `L*` commands marked `[√]`)
- `AGENTS.md` — build/test/JaCoCo commands
- Live oracle: `redis-server v=7.2.11 sha=d4c381df` on port 6399; differential transcript saved to
  `doc/bug_reviews/logs/20261109_201500_lgroup_differential_redis72_bug_82.log`

## Finding 1 (Bug 82.1): `LTRIM` with a negative `end` index deletes the whole list

**Severity:** High
**Class:** Correctness / Redis protocol compatibility (silent data loss)
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (OK + key gone for `0 -1` and `1 -1`) + live Redis 7.2.11 differential (key kept, `a,b,c` / `b,c`); see Review Feedback

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:1069-1073
        int size = rl.size();
        if (start > size || start > end) {
            removeDelay(slotWithKeyHash);
            return OKReply.INSTANCE;
        }
```

### Root cause

Redis normalizes a negative `end` **before** comparing it with `start`
(`ltrimCommand()` in Redis 7.2 `t_list.c`):

```c
    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;
    /* Invariant: start >= 0, so this test will be true when end < 0. */
    /* The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) { /* empty list */ }
```

Velo compares the **raw, un-normalized** `start` and `end`. With `start >= 0` and `end < 0` the
expression `start > end` is trivially true, so the "empty range" branch is taken and
`removeDelay(slotWithKeyHash)` **deletes the key**.

`LGroup.ltrim()` is the only list command that needs the negative-index normalization
(`lrange()` gets it from `IndexStartEndReset.reset()`, which does `end = valueLength + end` first).

### Trigger / impact

List `k = [a, b, c]`:

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LTRIM k 0 -1` | `OK`, **key deleted** (all 3 elements lost) | `OK`, key kept, `LRANGE` → `a,b,c` |
| `LTRIM k 1 -1` | `OK`, **key deleted** | `OK`, key kept, `LRANGE` → `b,c` |
| `LTRIM k 2 -1` | `OK`, **key deleted** | `OK`, key kept, `LRANGE` → `c` |

Every negative `end` in `[-size+1, -1]` with a non-negative `start` is affected, so the common
"trim from index N to the end" idiom wipes the list instead of trimming it. The reply is `OK`
in both cases, so **no client-side error is raised** — the loss is silent.

### Suggested fix direction

Normalize `start`/`end` the way Redis does, before any comparison and before deciding the range is
empty:

```java
int size = rl.size();
if (start < 0) start = size + start;
if (end < 0) end = size + end;
if (start < 0) start = 0;
// now: if (start > end || start >= size) -> empty range
```

Keep the empty-range handling of **82.2** as a separate change so the two commits stay minimal
(82.1 fixes the comparison order only; 82.2 fixes the empty-range outcome).

## Finding 2 (Bug 82.2): `LTRIM` with an empty/out-of-range range answers an error instead of emptying the list

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (`ERR invalid integer` + key kept for `3 3`, `3 10`, `-100 -90`) + live Redis 7.2.11 (OK + key deleted in all 3); see Review Feedback

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:1069-1078
        int size = rl.size();
        if (start > size || start > end) {
            removeDelay(slotWithKeyHash);
            return OKReply.INSTANCE;
        }
        var startEnd = IndexStartEndReset.reset(start, end, size);
        if (!startEnd.valid()) {
            return ErrorReply.INVALID_INTEGER;
        }
```

```java
// src/main/java/io/velo/command/IndexStartEndReset.java:42-44
        if (start >= valueLength) {
            return StartEndWithValidFlag.INVALID;
        }
```

### Root cause

Redis's `LTRIM` **never** fails on a numeric range: an empty or out-of-range range means "empty the
list" (and delete the key), and the reply is still `+OK`.

Velo returns `ErrorReply.INVALID_INTEGER` whenever `IndexStartEndReset.reset()` reports the range as
invalid, which happens for `start >= size` and for an `end` that stays negative after normalization.
The `start > size` guard also uses `>` where Redis uses `start >= llen`, so `start == size` falls
through into the invalid branch instead of the empty branch.

### Trigger / impact

List `k = [a, b, c]`:

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LTRIM k 3 3` | `ERR invalid integer`, **key kept** with `a,b,c` | `OK`, **key deleted** |
| `LTRIM k 3 10` | `ERR invalid integer`, key kept | `OK`, key deleted |
| `LTRIM k -100 -90` | `ERR invalid integer`, key kept | `OK`, key deleted |

Two divergences at once: the reply shape (`ERR` vs `OK`) **and** the resulting key state (kept vs
deleted). A client that trims a queue down past its end gets an error instead of an empty queue, and
the stale elements stay visible.

The existing test `LGroupTest.groovy:1094-1100` asserts `ltrim a -100 -90` → `ErrorReply.INVALID_INTEGER`,
i.e. the current (Redis-incompatible) behaviour is locked in by the test suite and must be updated
together with the fix.

### Suggested fix direction

Mirror Redis exactly: after the normalization of 82.1, treat
`start > end || start >= size` as "empty range" → `removeDelay(slotWithKeyHash)` + `+OK`, instead of
returning `ErrorReply.INVALID_INTEGER`. `ErrorReply.INVALID_INTEGER` must then only remain for a
genuinely unparsable argument (which is already covered by the `NOT_INTEGER` path at
`LGroup.java:902-904`).

## Finding 3 (Bug 82.3): `LASTSAVE` returns milliseconds, Redis returns seconds

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (raw ms `1762717903500` passed through) + live Redis 7.2.11 (`lastsave` = 10-digit seconds, `date +%s%3N` = 13-digit ms); see Review Feedback

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:227-229
        if ("lastsave".equals(cmd)) {
            return new IntegerReply(BGroup.lastBgSaveMillis.get());
        }
```

```java
// src/main/java/io/velo/command/BGroup.java:252-255
    /**
     * The timestamp (in milliseconds) of the last successful BGSAVE operation.
     */
    public static final AtomicLong lastBgSaveMillis = new AtomicLong(0);
```

```java
// src/main/java/io/velo/command/BGroup.java:268-271
        if ("bgsave".equals(cmd)) {
            lastBgSaveMillis.set(System.currentTimeMillis());
            return OKReply.INSTANCE;
        }
```

### Root cause

`BGroup.lastBgSaveMillis` is explicitly documented and written in **milliseconds**
(`System.currentTimeMillis()`), and `LGroup.lastsave` returns that raw value. Redis's `LASTSAVE`
returns `server.lastsave`, which is a **UNIX timestamp in seconds** (`time(NULL)`).

So the same event is reported with a 1000× different magnitude depending on the implementation.

### Trigger / impact

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LASTSAVE` after a `BGSAVE`/`SAVE` | e.g. `1794225790572` (ms) | e.g. `1794225790` (s) |

A client that computes `TIME` − `LASTSAVE` (the canonical way to age a snapshot) gets a value
~1000× too large, and any consumer that stores/compares `LASTSAVE` against a wall-clock second
timestamp is off by three orders of magnitude.

Relationship to bug 72: `doc/bug_reviews/bug_72_bgroup_commands_review_round_1.md:526` recorded that
"bgsave is a no-op returning OK … `lastBgSaveMillis` only feeds `lastsave`. Intentional — not
reported". That note is about the **no-op `BGSAVE`** design, and it does not address the **unit** of
the value that `LASTSAVE` reports. The two are independent; this finding is only about the unit.

### Suggested fix direction

Reply a seconds-based timestamp, e.g. `new IntegerReply(BGroup.lastBgSaveMillis.get() / 1000)`,
or introduce a `lastBgSaveSeconds` accessor next to the millis one. Keep the millisecond field
unchanged (it is the natural in-process timestamp) and convert only at the protocol boundary.
Note that `LASTSAVE` must keep answering `0` before any save has happened (Redis also answers `0`
when no save has occurred since start-up).

## Finding 4 (Bug 82.4): `LPOS` accepts `RANK 0`, which Redis rejects

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (`LPOS k x RANK 0` on `[y,x,x]` → integer `1`, i.e. behaves as `RANK 1`) + live Redis 7.2.11 (`ERR RANK can't be zero: ...`); see Review Feedback

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:713-730
        int rank = 1;
        int count = 1;
        boolean countProvided = false;
        // max compare times
        int maxlen = 0;
        for (int i = 3; i < data.length; i++) {
            String arg = new String(data[i]).toLowerCase();
            switch (arg) {
                case "rank" -> {
                    if (i + 1 >= data.length) {
                        return ErrorReply.SYNTAX;
                    }
                    try {
                        rank = Integer.parseInt(new String(data[i + 1]));
                    } catch (NumberFormatException e) {
                        return ErrorReply.NOT_INTEGER;
                    }
                }
```

### Root cause

Redis explicitly rejects `RANK 0` because "rank 0" is meaningless (ranks are 1-based, and a
negative rank means "count from the tail"):

```c
        if (!strcasecmp(opt,"RANK") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], -LONG_MAX, LONG_MAX, &rank, NULL) != C_OK)
                return;
            if (rank == 0) {
                addReplyError(c,"RANK can't be zero: use 1 to start from "
                                "the first match, 2 from the second ... "
                                "or use negative to start from the end of the list");
                return;
            }
        }
```

Velo parses the value with no zero check. The zero then falls into the generic
`rank <= 1` branch of the scan loop (`LGroup.java:789-797`), so `RANK 0` silently behaves like
`RANK 1` and returns the index of the first match.

### Trigger / impact

List `k = [x, y, x]`:

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LPOS k x RANK 0` | `0` (integer, as if `RANK 1`) | `ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list` |

A client that relies on the error to detect an invalid rank (e.g. computing a rank from a 0-based
counter) silently receives a plausible-looking index instead of an error.

### Suggested fix direction

Reject `rank == 0` while parsing the `RANK` option, with a dedicated error reply, e.g.

```java
if (rank == 0) {
    return new ErrorReply("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
}
```

(Exact Redis wording is not required by the protocol, but matching it makes differential testing
easy.) Do **not** reuse `ErrorReply.INVALID_INTEGER` for this: Redis uses a distinct message and the
distinction helps clients.

## Finding 5 (Bug 82.5): `LPOS` silently ignores unknown option names

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (`FOO 1` and `RANG 2` → integer reply) + live Redis 7.2.11 (both `ERR syntax error`); see Review Feedback

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:718-759
        for (int i = 3; i < data.length; i++) {
            String arg = new String(data[i]).toLowerCase();
            switch (arg) {
                case "rank" -> { ... }
                case "count" -> { ... }
                case "maxlen" -> { ... }
            }                       // <-- no default branch: unknown options are ignored
        }
```

### Root cause

Redis' option loop has an explicit `else` that rejects anything it does not recognise:

```c
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
```

Velo's `switch` has no `default` branch, so an unknown option token is skipped and the command
continues as if the option had not been given. Redis' grammar is strict here: `LPOS` accepts only
`RANK`, `COUNT` and `MAXLEN`.

### Trigger / impact

List `k = [x, y, x]`:

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LPOS k x FOO 1` | `0` (integer, option silently ignored) | `ERR syntax error` |
| `LPOS k x RANG 2` | `0` (integer, option silently ignored) | `ERR syntax error` |

Impact is limited to malformed input, but the divergence hides client bugs: a misspelled option
turns into a silently different query instead of an error.

Relationship to bug 41: the Review Feedback of
`doc/bug_reviews/bug_41_list_module_review_round_1.md` (Review Feedback For Bug 2 Fix) already
observed that "the switch has no default branch" and left it out of scope of that fix. It was never
filed as a numbered finding; it is filed here as 82.5.

### Suggested fix direction

Add a `default ->` branch to the option `switch` that returns `ErrorReply.SYNTAX`. Mind the loop
shape: the current loop also does **not** skip the option's *value* token, so a value that happens to
be spelled `rank`/`count`/`maxlen` would be re-parsed as an option. Advancing the index past the
value (as Redis does with `j++`) makes the grammar unambiguous and is the natural place to do it.

## Finding 6 (Bug 82.6): `LINDEX` answers an error for an index ≥ 32767, Redis answers nil

**Severity:** Low
**Class:** Redis protocol compatibility / edge case
**Status:** `CONFIRMED (reproduced)` — reviewer temp spec (`40000`/`32767` → `ERR list size too long`, `100` → nil) + live Redis 7.2.11 (all three → nil); see Review Feedback.
**Disposition:** the finding is **not** applied — the `RedisList.LIST_MAX_SIZE` guard stays in `lindex()` by product decision (see *Fix Log* / *Residual Risk*)

### Cited files / lines

```java
// src/main/java/io/velo/command/LGroup.java:306-314
        int index;
        try {
            index = Integer.parseInt(new String(indexBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (index >= RedisList.LIST_MAX_SIZE) {
            return ErrorReply.LIST_SIZE_TO_LONG;
        }
```

### Root cause

`RedisList.LIST_MAX_SIZE` is `Short.MAX_VALUE` (32767) — an internal Velo storage limit (the list
size is encoded in a `short`), not a protocol limit. Redis 7.2 removed the `list-max-len` check
from `LINDEX` entirely: `lindexCommand()` simply builds an iterator and replies `nil` when the
index is out of range.

Velo turns the storage limit into a protocol error, so an out-of-range index is reported as
`ERR list size too long` where Redis reports "no such element" (`nil`).

### Trigger / impact

List `k = [a, b, c]`:

| command | Velo (this commit) | Redis 7.2 |
|---|---|---|
| `LINDEX k 40000` | `ERR list size too long` | `(nil)` |
| `LINDEX k 100` | `(nil)` | `(nil)` |
| `LINDEX k 32767` | `ERR list size too long` | `(nil)` |

Only indexes ≥ 32767 diverge; ordinary out-of-range indexes already return `nil` correctly
(`LGroup.java:324-327`). A client that probes a large index gets an error where Redis gives nil.

The same limit check exists in `LSET` (`LGroup.java:1015-1017`), where Redis also answers an error
(`index out of range`), so only the reply **text** differs there — not reported as a finding.

### Suggested fix direction

Drop the `LIST_MAX_SIZE` guard from `lindex()` and let the ordinary out-of-range path
(`index >= size` → `NilReply`) handle it; the `short` header can never hold more than 32767
elements anyway, so a larger index can only ever be out of range. If the guard is kept for
defensive reasons, it should still answer `NilReply.INSTANCE` rather than an error.

## Notes / non-findings

- **`LINSERT`**: missing key → `0`, missing pivot → `-1`, wrong type → `WRONGTYPE`, insert
  position/semantics → all match Redis 7.2. (The missing-pivot `-1` was bug 41 finding 1, already
  fixed.) Recorded so later rounds do not re-investigate.
- **`LPOS` matching semantics** (`RANK` positive/negative, `COUNT` combined with `RANK`, `COUNT 0`
  = "all matches", `MAXLEN` as a scan limit, absolute index reporting for a negative rank, the
  reverse-order result list for a negative rank) were each compared against live Redis 7.2.11 and
  **match** — e.g. `LPOS k x RANK -1 COUNT 2` → `[3, 2]`, `LPOS k x RANK 2 COUNT 2` → `[2, 3]`,
  `LPOS k x COUNT 0` → all matches. Only the *validation* of the options diverges (82.4, 82.5).
- **`LPOS` negative `COUNT`/`MAXLEN`** → Velo answers `ERR invalid integer`, Redis answers
  `ERR COUNT can't be negative` / `ERR MAXLEN can't be negative`. Both are errors; only the text
  differs. Velo's generic error texts differ from Redis throughout the codebase (e.g.
  `ErrorReply.NOT_INTEGER` = `"not integer"` vs Redis `"value is not an integer or out of range"`);
  not filed as a finding.
- **`LPOP` / `RPOP`**: `COUNT` semantics, `COUNT 0` → empty array, missing key with `COUNT` →
  null array, missing key without `COUNT` → nil, `COUNT` larger than the list, and the
  "delete the key when the list becomes empty" behaviour all match Redis 7.2. `RPOP` lives in
  `RGroup` and is out of scope for this round.
- **`LLEN` / `LRANGE` / `LSET` / `LREM`**: reply shapes, negative-index handling
  (`IndexStartEndReset.reset()` implements the same normalization Redis uses in
  `addListRangeReply()`), `LREM` head/tail direction for positive/negative count, and
  `LREM`'s delete-on-empty behaviour all match Redis 7.2.
- **`LPUSH` / `LPUSHX` / `RPUSH` / `RPUSHX`**: reply value (list length after the push), the
  "push only if the key exists" rule, and the interaction with a pending blocking waiter were
  reviewed by bug 72 and bug 81 and are correct at this commit.
- **`LMPOP`**: `numkeys` handling, direction parsing, `COUNT` parsing, the per-key type check
  (wrong type → `WRONGTYPE`, missing/empty keys skipped), the "first key with a non-empty list
  wins" rule and the `[key, [elements...]]` reply shape all match Redis 7.2 (bug 41 findings 5 and
  its follow-ups, already fixed).
- **`LMOVE` / `BLMOVE` / `BRPOPLPUSH`**: the move semantics live in `RGroup.move()` /
  `RGroup.moveBlock()` and were reviewed by bug 41 (finding 4 + follow-ups, already fixed);
  `LGroup.lmove()` only validates arity and the `LEFT`/`RIGHT` tokens.
- **`load-rdb`**: a Velo-specific admin command (`@admin @dangerous`); it rejects a relative path
  and a non-`.rdb` suffix. Out of scope for a Redis-compatibility round.
- **32-bit argument parsing (systemic, not filed)**: `LGroup` parses indexes/counts with
  `Integer.parseInt`, while Redis uses `long`. Values above 2^31-1 are therefore rejected by Velo
  although Redis accepts them, e.g. `LRANGE k 0 3000000000` → Velo `ERR not integer`, Redis returns
  the whole list; `LPOP k 99999999999999` → Velo `ERR not integer`, Redis returns all elements.
  This is a codebase-wide convention (77 `Integer.parseInt` vs 30 `Long.parseLong` in
  `io/velo/command/*.java`), not an `LGroup`-specific defect, and changing it only here would be
  inconsistent — it is a product decision, tracked here as an observation for a future
  cross-module round.
- **`LASTSAVE` before any save**: Velo answers `0` until a `BGSAVE`/`SAVE` has run. Redis also
  answers `0` when no save has occurred since start-up, so only the unit of 82.3 differs.

## Reviewer checklist (for AI agent 2)

- Finding 82.1: **live Redis 7.2 differential** — `RPUSH k a b c`; `LTRIM k 0 -1`; then
  `EXISTS k` and `LRANGE k 0 -1`. Redis: `EXISTS`=1, `LRANGE`=`a,b,c`. Velo (Spock temp spec,
  `LGroup` + `InMemoryGetSet`, `Mock.prepareCompressedValueList`): expect `OKReply` **and the key
  gone** (`inMemoryGetSet.getBuf(...) == null`). Repeat with `LTRIM k 1 -1` (Redis keeps `b,c`) and
  `LTRIM k 2 -1` (Redis keeps `c`). Also assert that a positive `end` is unaffected.
- Finding 82.2: same harness — `LTRIM k 3 3` and `LTRIM k -100 -90` on a 3-element list. Redis:
  `OK` + key deleted. Velo: currently `ErrorReply.INVALID_INTEGER` + key still present.
  Also check `LTRIM k 3 10`. Confirm the existing expectation at `LGroupTest.groovy:1094-1100`
  (`ltrim a -100 -90` → `INVALID_INTEGER`) has to change with the fix.
- Finding 82.3: read `BGroup.lastBgSaveMillis` (ms) vs the `LASTSAVE` reply; compare against
  `redis-cli -p 6399 lastsave` and `date +%s` on the same box. Assert the Velo value is ~1000×
  the Redis value and that the two are the same instant.
- Finding 82.4: `LPOS k x RANK 0` — Redis: `ERR RANK can't be zero: …`; Velo: an integer reply
  (index of the first match). Use a list where the first match is not at index 0 to make the
  "behaves like RANK 1" difference visible.
- Finding 82.5: `LPOS k x FOO 1` and `LPOS k x RANG 2` — Redis: `ERR syntax error`; Velo: an
  integer reply. Also check that a value token spelled like an option is not re-parsed as an option
  (e.g. `LPOS k x COUNT 2 RANK 1`).
- Finding 82.6: `LINDEX k 40000` and `LINDEX k 32767` — Redis: `(nil)`; Velo:
  `ERR list size too long`. Confirm `LINDEX k 100` already returns nil on both.
- Cross-checks (must stay green): `LGroupTest` as a whole; `RGroupTest`, `BGroupTest`,
  `BlockingListTest`, `GGroupTest`; and the live differential transcript in
  `doc/bug_reviews/logs/20261109_201500_lgroup_differential_redis72_bug_82.log`.

## Review Feedback (AI agent 2, round 1)

**Method:** HEAD == reviewed commit `7d8ce05a` (verified). Two-sided verification per the reviewer
checklist: (1) **Velo side** — a temporary Spock spec `Bug82ReviewVerifySpec` (13 test cases, harness
identical to `LGroupTest`: `LGroup` + `InMemoryGetSet` + `Mock.prepareCompressedValueList`, key
existence asserted via `inMemoryGetSet.getBuf(slot, key, 0, 0L)`), **every case asserting the
divergent behavior and passing** = the bug is reproduced; the spec was deleted after evidence capture.
(2) **Oracle side** — fresh live Redis 7.2.11 differential on port 6399 (independent re-run, not a
replay of agent 1's transcript). Transcript + spec evidence:
`doc/bug_reviews/logs/20261109_204643_lgroup_review_agent2_bug_82.log`. Cited lines were re-read and
match the report (`LGroup.java:1069-1078` raw `start > end` comparison + `reset()` INVALID → error;
`LGroup.java:227-229`; `LGroup.java:718-759` switch with no `default` and no `RANK 0` check;
`LGroup.java:312-314`; `IndexStartEndReset.java:36-50`; `BGroup.java:253-271`). Cross-check:
`LGroupTest` 41/41 green at this commit (confirms 82.2's wrong behaviour is locked in by
`LGroupTest.groovy:1094-1100`).

| Finding | Verdict | Evidence |
|---|---|---|
| 82.1 | CONFIRMED (reproduced) | Spec: `ltrim k 0 -1` and `ltrim k 1 -1` on `[a,b,c]` → `OKReply` **and** `getBuf(...)==null` (key deleted); control `ltrim k 1 2` → key kept. Redis 7.2.11: `LTRIM k 0 -1` → `OK`, exists=1, `a,b,c`; `1 -1` → `b,c`; `2 -1` → `c`. Silent data loss confirmed on both sides. |
| 82.2 | CONFIRMED (reproduced) | Spec: `ltrim k 3 3`, `ltrim k 3 10`, `ltrim k -100 -90` → `ErrorReply.INVALID_INTEGER` **and** key still present. Redis 7.2.11: all three → `OK`, exists=0 (key deleted). Reply shape and key state both diverge; the `start > size` vs `start >= llen` trace in the report is exact. |
| 82.3 | CONFIRMED (reproduced) | Spec: set `BGroup.lastBgSaveMillis=1762717903500` → `lastsave` replies the raw ms value (`IntegerReply.integer == 1762717903500`). Redis 7.2.11: `lastsave=1794227322` vs `date +%s%3N=1794228366287` — seconds on the wire. Also verified `lastBgSaveMillis` has no other reader (only `LGroup.java:228`), so converting at the protocol boundary is safe. |
| 82.4 | CONFIRMED (reproduced) | Spec: list `[y,x,x]`, `lpos k x RANK 0` → `IntegerReply(1)` — first match at index 1, i.e. exactly `RANK 1` behavior. Redis 7.2.11: `ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list`. |
| 82.5 | CONFIRMED (reproduced) | Spec: `lpos k x FOO 1` and `lpos k x RANG 2` → `IntegerReply` (option ignored). Redis 7.2.11: both → `ERR syntax error`. The report's loop-shape note (option values not skipped) also verified by inspection — the fix should advance past the value token. |
| 82.6 | CONFIRMED (reproduced) | Spec: `lindex k 40000` / `lindex k 32767` → `ErrorReply{message='list size too long'}`; `lindex k 100` → `NilReply` (already correct). Redis 7.2.11: all three → `(nil)`. `LIST_MAX_SIZE == Short.MAX_VALUE == 32767` confirmed (`RedisList.java:21`), so index `32767` itself errs. |

### Additional observations

- **Non-finding (checked): `LTRIM` on a missing key** — Redis 7.2.11 answers `OK` (`ltrim nok 0 1` /
  `ltrim nok 5 10` → OK); Velo's `LGroup.java:1064-1067` also returns `OKReply`. No divergence.
- **82.5 fix shape**: the option loop currently does not skip the option's value token, so a value
  spelled `rank`/`count`/`maxlen` is re-parsed as an option (e.g. `LPOS k x COUNT rank` → Velo
  `ERR syntax`, Redis `ERR value is not an integer or out of range`). Advancing the index past the
  value in the same commit (as the report suggests) resolves both; note `LPOS k x COUNT 2 RANK 1`
  (non-colliding values) already matches Redis today (verified: Velo trace + live Redis `[1, 2]`).
- **82.3 residual**: `SGroup.java:400` (`save`) also writes `lastBgSaveMillis` in ms — consistent;
  the `/1000` conversion at `LGroup.java:228` covers both writers. Keep the `0`-before-first-save
  behavior (0 ms / 1000 == 0 s).

### Recommended fix order (for the fix agent)

1. **82.1** (High, silent data loss) — normalize `start`/`end` before any comparison; alone in its
   own commit, no other behavior change (positive ranges untouched — the control case must stay green).
2. **82.2** (Medium, same function) — second commit: `start > end || start >= size` → empty range →
   `removeDelay` + `+OK`; must update the locked-in expectation at `LGroupTest.groovy:1094-1100`
   and add key-state assertions (`exists` after `LTRIM k 3 3` → 0).
3. **82.4** (Medium) — dedicated `RANK can't be zero` error (not `INVALID_INTEGER`).
4. **82.3** (Medium) — one-line `/1000` at the protocol boundary.
5. **82.5** (Low) — `default ->` `ErrorReply.SYNTAX` + advance past option values.
6. **82.6** (Low) — drop the `LIST_MAX_SIZE` guard from `lindex()` (or answer `NilReply`); the
   ordinary out-of-range path already returns nil.

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 82.1 | RED: `LGroupTest.test ltrim` failed at `LGroupTest.groovy:1130` — `readList(inMemoryGetSet, 'a') == ['0','1','2']` was `null` (the mock map was empty: `LTRIM a 0 -1` had deleted the list) | `1957a4e5` `fix: normalize a negative ltrim end index against the list length` | `./gradlew :test --tests "io.velo.command.LGroupTest"` → BUILD SUCCESSFUL (41 tests, 0 failures) | `LGroup.java` 1069-1077 → 14/14 lines, 3/3 branch lines fully covered — the new `if (end < 0)` branch executes in both directions |
| 82.2 | RED: `LGroupTest.test ltrim` failed at `LGroupTest.groovy:1110` — `LTRIM a -100 -90` answered `ErrorReply{invalid integer}` instead of `OKReply` | `1f0e2dad` `fix: ltrim empties the list and replies ok for an empty range` | same command → BUILD SUCCESSFUL | `LGroup.java` 1066-1090 → 14/14 lines, every branch line fully covered, incl. the new `removeDelay` + `OK` path of the invalid range |
| 82.3 | RED: `LGroupTest.test handle - lastsave` failed at `LGroupTest.groovy:112` — the reply was `1700000000123` instead of `1700000000` | `2cf0b4e2` `fix: lastsave replies a unix timestamp in seconds` | same command → BUILD SUCCESSFUL | `LGroup.java` 227-229 → covered, all branches |
| 82.4 | RED: `LGroupTest.test lpos` failed at `LGroupTest.groovy:661` — `LPOS a a rank 0` answered `NilReply` instead of an `ErrorReply` | `69d10da5` `fix: lpos rejects a rank of zero` | same command → BUILD SUCCESSFUL | `LGroup.java` 724-778 → every line covered, all branch lines fully covered, incl. the new `rank == 0` guard |
| 82.5 | RED: `LGroupTest.test lpos` failed at `LGroupTest.groovy:668` — `LPOS a a foo 1` answered `NilReply` instead of `ErrorReply.SYNTAX` | `195d89b5` `fix: lpos rejects unknown option names` | same command → BUILD SUCCESSFUL | `LGroup.java` 724-778 → every line covered, all branch lines fully covered, incl. the new `default` branch and the option-value skip |
| 82.6 | RED: `LGroupTest.test lindex` failed at `LGroupTest.groovy:165` — `LINDEX a 65536` answered `ErrorReply{list size too long}` instead of nil | `a1a08492` `fix: lindex replies nil for an index beyond the list size` — **reverted by `56fb3137`** (product decision, see below) | same command → BUILD SUCCESSFUL | `LGroup.java` 306-335 → 17/17 lines, 4/4 branch lines fully covered |

**Fix order actually used:** 82.1 → 82.2 → 82.3 → 82.4 → 82.5 → 82.6 (agent 2 suggested 82.4 before
82.3; the two are independent, so the order difference is immaterial).

**82.6 reverted by product decision.** The `RedisList.LIST_MAX_SIZE` guard stays in
`LGroup.lindex()`, so an index ≥ 32767 keeps answering `ERR list size too long` instead of Redis
7.2's `nil`. The revert was done with the same red → green discipline: the test was changed first
(RED: `LGroupTest.test lindex` failed at `LGroupTest.groovy:164`, the reply was `nil` instead of
`ErrorReply.LIST_SIZE_TO_LONG`), then the guard was restored (`56fb3137`
`fix: keep the lindex list size limit error`). The test now pins the boundary from both sides:
`LINDEX a 32767` → error, `LINDEX a 32766` → nil. `LGroup.java` 312-324 → all lines and all branch
lines fully covered.

**Regression run after the last fix:**
`./gradlew :test --tests "io.velo.command.LGroupTest" --tests "io.velo.command.RGroupTest" --tests "io.velo.command.BGroupTest" --tests "io.velo.command.BlockingListTest" --tests "io.velo.command.GGroupTest"` → BUILD SUCCESSFUL.

**Regression cases kept green (agent 2's control cases):** `LTRIM k 1 2` (positive range) still keeps
the middle elements, `LPOS k x COUNT 2 RANK 1` still answers `[1, 2]`, `LTRIM` on a missing key still
answers `OK`.

**Environment note:** the Gradle daemon intermittently dies with `Java heap space` while compiling the
Groovy test sources (`compileTestGroovy`). The runs above used
`-Dorg.gradle.jvmargs="-Xmx3g -XX:MaxMetaspaceSize=768m"`, which makes them stable. That is the known
"gradle daemon heap" follow-up of bug 73, not a defect of these fixes.

## Re-review (AI agent 2)

## Residual Risk

- **32-bit argument parsing (not fixed here, by design of this round):** `LGroup` parses indexes and
  counts with `Integer.parseInt`, Redis uses `long`. Values above 2^31-1 are rejected by Velo
  although Redis accepts them (`LRANGE k 0 3000000000`, `LPOP k 99999999999999`). This is a
  codebase-wide convention (77 `Integer.parseInt` vs 30 `Long.parseLong` in `io/velo/command/*.java`),
  so it stays an observation for a future cross-module round.
- **82.6 is accepted on purpose:** `LINDEX` answers `ERR list size too long` for an index >= 32767
  where Redis 7.2 answers `nil` (Redis has no such limit). The product decision keeps the internal
  `RedisList.LIST_MAX_SIZE` limit visible as a protocol error; the guard is back in `lindex()`
  (`56fb3137`). Only indexes that cannot address any element of a Velo list are affected.
- **`LSET` still answers `ERR list size too long`** for an index >= 32767 where Redis answers
  `ERR index out of range`. Both are errors, only the text differs — not filed as a finding.
- **`LPOS` negative `COUNT`/`MAXLEN` still answer `ERR invalid integer`** where Redis answers
  `ERR COUNT can't be negative` / `ERR MAXLEN can't be negative`. Both are errors, only the text
  differs — not filed as a finding.
- **`LASTSAVE` still reports the moment of the last `BGSAVE`/`SAVE` command**, and `BGSAVE` is a
  no-op in Velo (bug 72 non-finding). The unit is now seconds, but the value still does not track a
  real disk snapshot.
- **`IndexStartEndReset.reset()` is shared with `LRANGE`**; `LTRIM` now normalizes a negative `end`
  itself, `LRANGE` keeps using the shared helper. `LRANGE` semantics are unchanged
  (`LRANGE key 0 -1` still returns the whole list).

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 82.1 | `LTRIM` with a negative `end` deletes the whole list | High | Correctness / Redis protocol compatibility (data loss) | CONFIRMED (reproduced) | fixed (`1957a4e5`) |
| 82.2 | `LTRIM` with an empty/out-of-range range answers an error instead of emptying the list | Medium | Correctness / Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`1f0e2dad`) |
| 82.3 | `LASTSAVE` returns milliseconds, Redis returns seconds | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`2cf0b4e2`) |
| 82.4 | `LPOS` accepts `RANK 0`, which Redis rejects | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`69d10da5`) |
| 82.5 | `LPOS` silently ignores unknown option names | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`195d89b5`) |
| 82.6 | `LINDEX` answers an error for an index >= 32767, Redis answers nil | Low | Redis protocol compatibility / edge case | CONFIRMED (reproduced) | won't fix — reverted (`a1a08492` → `56fb3137`), the list size limit stays a protocol error |
