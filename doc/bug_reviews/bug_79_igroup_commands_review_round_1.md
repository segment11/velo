# Bug 79 — IGroup Commands Review (Round 1)

**Module:** `io.velo.command.IGroup` (INCR / INCRBY / INCRBYFLOAT / INFO / INGEST / INGEST_SST) plus its
delegation targets (`DGroup.decrBy()`, `InfoCommand.groovy`, `DoubleReply`, the ingest write path)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** verified by AI agent 2 (10/10 CONFIRMED; 79.2 scoping note corrected; 79.10 adjudicated option (a)); author response appended (agrees 10/10, refines reviewer notes 79.8/79.9); fix phase done by AI agent 1 — 10/10 fixed, one commit each (`8eea029f`…`e596fe5e`), see *Fix Log*; re-review pending (AI agent 2)
**Review branch:** `review/igroup`
**Reviewed commit:** `f7c06ee227ddd7379a1be98f369798de4b8a431c` (branch `main`)

> Round 1 — no prior round for IGroup, so there is no delta declaration. Prior coverage that bounds this
> round (items there are **not** re-reported, see *Notes / non-findings*):
>
> - `bug_74_dgroup_commands_review_round_1.md` — the shared `DGroup.decrBy()` path used by `INCR*`
>   (TTL keep, double-value guard, int64 delta, overflow guard, explicit `isByFloat`, non-finite guard —
>   all fixed there and present at HEAD).
> - `bug_78_hgroup_commands_review_round_1.md` finding 78.10 — the `doubleScale=2` quantization root cause
>   was confirmed there for `HINCRBYFLOAT` and **explicitly deferred to the string-module round** ("Root is
>   shared with `DGroup.decrBy`/`IGroup` — that side belongs to a string-module round"; fix commit
>   `259f9e37` "hash side only, the string module untouched"). `IGroup` owns `INCRBYFLOAT` — that deferred
>   item is reported here as **79.1**.
> - `bug_39_string_commands_review_round_1/2.md` — INCR/INCRBY/INCRBYFLOAT were in scope but produced no
> findings.
> - `bug_60_info_command_review_round_1.md` — InfoCommand findings 1–3 (unknown/empty section, slaveN cap,
>   memory −1 sentinel) are all fixed at HEAD; not re-reported. One **new** ground-truth conflict with
>   bug_60's premise is flagged as 79.10.

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/IGroup.java` — full file (1–525): static registrations (35–84), ctor
  (91–93), `parseSlots()` (104–116), `handle()` (124–188), `info()` (192–198), `getPairValue()` (200–207),
  `ingest()` (209–433), `ingest_sst()` (435–524)
- `src/main/java/io/velo/command/DGroup.java` — `handle()` decr/decrby/decrbyfloat (173–207),
  `decrBy()` (354–425) at HEAD (post-bug_74), `isUseHH()` (427–437)
- `dyn/src/io/velo/command/InfoCommand.groovy` — full file (1–378): `getOneSection()` (62–88),
  `handle()` (90–133), `server()` (135–154), `clients()` (156–167), `memory()` (179–219),
  `replication()` (221–334), `cpu()` (336–344), `cluster()` (346–351), `keyspace()` (353–377)
- `dyn/src/io/velo/script/InfoCommandHandle.groovy` — full file (1–10)
- `src/main/java/io/velo/reply/DoubleReply.java` — full file (1–54): `buffer()` (34–39), `bufferAsResp3()` (41–53)
- `src/main/java/io/velo/BaseCommand.java` — `set()` overloads used by ingest (1008–1049)
- `src/main/java/io/velo/persist/LocalPersist.java` — `doSthInSlots()` (276–303)
- `src/main/java/io/velo/persist/OneSlot.java` — `setBulkLoad()` (2275–2279), `resetWritePositionAfterBulkLoad()` (2284–2288)
- `src/main/java/io/velo/persist/Wal.java` — `resetWritePositionAfterBulkLoad()` (816–850, `bulkLoad = false` at 849),
  `put()` (858–890), `bulkLoad` field doc (351)
- `src/main/java/io/velo/ConfForGlobal.java` — `doubleScale` (229); `src/main/java/io/velo/MultiWorkerServer.java` (1487)
- `src/test/groovy/io/velo/command/IGroupTest.groovy` — full file (1–441): parse slot (26–50), format
  errors (52–68), incrby/incrbyfloat/info (70–105), handle2 (107–148), incr (150–167), incrby (169–190),
  incrbyfloat (192–211), ingest (213–375), ingest_sst (377–440)
- `doc/bug_reviews/bug_74_dgroup_commands_review_round_1.md` — full (fix log + re-review + residual risk)
- `doc/bug_reviews/bug_78_hgroup_commands_review_round_1.md` — findings 78.3/78.10, re-review, residual notes
- `doc/bug_reviews/bug_39_string_commands_review_round_1/2.md` — scope/increment-related sections
- `doc/bug_reviews/bug_60_info_command_review_round_1.md` — full

Reference docs (ground truth):

- Redis 7.2 `INCRBYFLOAT` — https://redis.io/docs/latest/commands/incrbyfloat/ : "The precision of the
  output is fixed at 17 significant digits", "Trailing zeroes are always removed" (same wording cited by bug_78)
- Redis 7.2 `INCRBY` — https://redis.io/docs/latest/commands/incrby/ : overflow → `ERR increment or
  decrement would overflow`; argument must be a 64-bit integer literal
- Redis 7.2 `INFO` — https://redis.io/docs/latest/commands/info/ ; section names matched case-insensitively
  (upstream `strcasecmp` in `server.c:genRedisInfoString`)
- Live Redis **7.2.11** (`redis-server v=7.2.11 sha=d4c381df`) on port 31999/31991 for differential testing
  (same binary as the bug_74/bug_78 rounds)
- JDK 21 repro `/tmp/opencode/Bug79Repro.java`
- Raw evidence log: `doc/bug_reviews/logs/20261028_190300_igroup_differential_redis72_bug_79.log`

---

## Finding 1 (Bug 79.1): `INCRBYFLOAT` quantizes value and delta to `doubleScale=2` — small increments are permanently lost

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:156-173 — incrbyfloat delegates with -by
        if ("incrbyfloat".equals(cmd)) {
            ...
            return dGroup.decrBy(0, -by, true);
```

```java
// src/main/java/io/velo/command/DGroup.java:399-408 — decrBy(), float path (at HEAD)
        if (isByFloat) {
            ...
            var newValue = BigDecimal.valueOf(doubleValue).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP)
                    .subtract(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));

            setNumber(newValue.doubleValue(), slotWithKeyHash, expireAt);
            return new DoubleReply(newValue);
```

```java
// src/main/java/io/velo/ConfForGlobal.java:229
    public static int doubleScale = 2;   // default; MultiWorkerServer.java:1487 same default
// src/main/java/io/velo/reply/DoubleReply.java:36 — reply formatting applies the same scale
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
```

### Root cause

Both operands are rounded to `doubleScale` (default **2**) decimal places *before* the subtraction, and the
reply is rendered with the same scale. A display-config knob (`number.doubleScale`) is being used as the
arithmetic scale. bug_78.10 fixed this for `HINCRBYFLOAT` (commit `259f9e37`, "hash side only, the string
module untouched") and deferred the string-module side to this round; `INCRBYFLOAT` funnels through
`IGroup.handle()` → `DGroup.decrBy()` which still quantizes.

### Trigger / impact

Live Redis 7.2.11 vs Velo (reproduced by the reviewer, log §1/§2):

| command sequence | Redis 7.2.11 | Velo (doubleScale=2) |
|---|---|---|
| `INCRBYFLOAT k 0.001` ×3 | `0.001` → `0.002` → `0.003` | `0.00` → `0.00` → `0.00` — increment **permanently lost**, also on disk |
| `SET k 10.50` + `INCRBYFLOAT k 0.1` | `10.6` | `10.60` (trailing zero kept) |
| `INCRBYFLOAT k 0.1` on empty key | `0.1` | `0.10` |

Any sub-cent float counter (rates, sensor readings, micro-transactions) is silently corrupted — the
*stored* value is `newValue.doubleValue()` of the scale-2 result, so the loss is persistent, not just
display. `DECRBYFLOAT` (Velo-only command) inherits the same behavior through the identical path.

> **Reviewer note (AI agent 2):** the fix must also decide the **storage representation** — Velo stores the
> result as a binary double (`SP_TYPE_NUM_DOUBLE`), Redis stores a decimal *string* (`embstr`). A binary
> double accumulates `0.1 + 0.2` as `0.30000000000000004`, which Redis' `long double` avoids, so a fix that
> only removes the `setScale` calls but keeps storing `newValue.doubleValue()` will still diverge from Redis
> on repeated small increments. See *Review Feedback*, 79.1.

### Suggested fix direction

Mirror the hash-side fix `259f9e37`: compute with full `BigDecimal.valueOf(double)` precision (no
`setScale` on the operands) and format the reply/stored value with up-to-17-decimals +
trailing-zero stripping. The `doubleScale` config must stop being applied to incrbyfloat-family
arithmetic (keep it for display-only use sites, if any). Needs the product decision already flagged in
bug_78's fix order ("global `doubleScale` semantics vs. per-command precision"); fix here must not change
hash-module behavior.

---

## Finding 2 (Bug 79.2): `INCRBY` with delta `Long.MIN_VALUE` — negation overflow → false overflow error and silent wraparound

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:142-153 — incrby
            long by;
            try {
                by = Long.parseLong(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            ...
            return dGroup.decrBy(-by, 0, false);   // <-- -by wraps when by == Long.MIN_VALUE
```

```java
// src/main/java/io/velo/command/DGroup.java:409-421 — decrBy() integer guard (at HEAD)
        if (by > 0) {
            if (longValue < Long.MIN_VALUE + by) { return ErrorReply.INCR_DECR_OVERFLOW; }
        } else if (by < 0) {
            if (longValue > Long.MAX_VALUE + by) { return ErrorReply.INCR_DECR_OVERFLOW; }
        }
        long newValue = longValue - by;
```

### Root cause

`INCRBY key by` is implemented as `decrBy(-by)` (subtraction), but `-Long.MIN_VALUE == Long.MIN_VALUE`
(two's-complement wrap), so the delta is silently corrupted before the (otherwise correct) bug_74.4
overflow guard runs. The guard then evaluates the wrong operation.

### Trigger / impact

JDK repro + live Redis 7.2.11 differential (reproduced by the reviewer, log §1/§2):

| case | Redis 7.2.11 | Velo (reproduced) |
|---|---|---|
| `SET k 0` + `INCRBY k -9223372036854775808` | `:-9223372036854775808` (representable) | `ERR increment or decrement would overflow` — legal command rejected |
| `SET k -5` + `INCRBY k -9223372036854775808` | `ERR increment or decrement would overflow`, value stays `-5` | guard misses (`-5 > -1` false) → `longValue - by` wraps → **stores and replies `9223372036854775803`** |

bug_74.4 fixed the wrap for `DECR*`/plain `INCR`/normal `INCRBY` deltas; only the `Long.MIN_VALUE` *delta*
path is broken, and it is broken **in `IGroup.java:153`** (the negation).

> **Correction (AI agent 2, second pass — verified against upstream `redis/src/t_string.c`):** the original
> text claimed "`DGroup.decrby` does not negate, so `DECRBY k -9223372036854775808` is correct". That is
> **wrong**. Upstream `decrbyCommand` rejects the delta outright:
> `if (incr == LLONG_MIN) { addReplyError(c, "decrement would overflow"); return; }` — Redis answers
> `ERR decrement would overflow` for **every** stored value and never changes the value. Velo's
> `DGroup.decrby` has no such rejection: it *accepts* the delta and computes `longValue - by`, so on a
> stored `-5` it stores and replies `9223372036854775803` where Redis errors and keeps `-5`
> (reproduced — see *Review Feedback*, "79.2 — CONFIRMED (reproduced)"). The `DECR*` twin therefore has its
> **own** Redis divergence (a different delta, a different guard, and it lives in `DGroup`, i.e. bug_74's
> module — not in `IGroup`). It is **not** re-reported as a new finding here; it is recorded as a follow-up in
> *Additional observations* because the 79.2 fix touches the same shared `DGroup.decrBy()` code.

### Suggested fix direction

Stop negating in `IGroup`: plumb the true increment (e.g. an `incrBy(long)` variant, or an explicit
direction flag), and guard with the addition-side check `by > 0 && longValue > Long.MAX_VALUE - by` /
`by < 0 && longValue < Long.MIN_VALUE - by` (both safe against wrap), or use `Math.addExact` with
`ArithmeticException` → `INCR_DECR_OVERFLOW`. Keep `DGroup`'s existing guard untouched.

> **Reviewer note (AI agent 2):** the upstream guard is **addition-shaped**
> (`redis/src/t_string.c:610-611`: `(incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
> (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))`) because upstream never negates the delta.
> The last sentence above ("keep `DGroup`'s existing guard untouched") means *do not change the `DECR*`
> semantics* — the `DECR*` path must keep behaving as it does today — **not** "keep the subtraction-shaped
> arithmetic". The subtraction-shaped guard (`longValue - by`) cannot be made correct for a `Long.MIN_VALUE`
> delta without a separate special case, because the negation already happened in `IGroup`. The fix should
> therefore introduce an **addition-shaped** variant of `decrBy()` for the increment direction (leaving the
> `DECR*` path on the existing subtraction guard), which also matches the upstream reference. The unrelated
> `DECRBY` + `Long.MIN_VALUE` divergence noted above is a **separate** `DGroup` bug and must not be folded
> into the 79.2 commit.

---

## Finding 3 (Bug 79.3): `ingest` json `value_fields=*` (documented) stores `{"*":null}` for every key; the count-equality shortcut ignores field names

**Severity:** Medium
**Class:** Correctness (Velo-specific command)
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:210-212 — documented usage
        // ingest dir=/tmp/velo/ingest/ file_pattern=test* file_format=json key_field=name key_prefix=test_ value_fields=*

// src/main/java/io/velo/command/IGroup.java:250-255
        String[] valueFieldsArray;
        if (isJson) {
            valueFieldsArray = valueFields.split(",");   // "*" -> ["*"]
        } else {
            valueFieldsArray = null;
        }

// src/main/java/io/velo/command/IGroup.java:357-366 — per-record value projection
                String value;
                if (valueFieldsArray.length == map.size()) {
                    value = line;                        // count equality == "all fields"?
                } else {
                    var subMap = new HashMap<>();
                    for (int i = 0; i < valueFieldsArray.length; i++) {
                        subMap.put(valueFieldsArray[i], map.get(valueFieldsArray[i]));  // map.get("*") == null
                    }
                    value = objectMapper.writeValueAsString(subMap);
                }
```

### Root cause

`*` is never interpreted as "all fields" on the json path. It becomes the literal field name `"*"`,
which does not exist in the record, so every projected value is the literal string
`{"*":null}` (JDK repro in log §1). Additionally, the "keep the whole line" shortcut is decided by
`valueFieldsArray.length == map.size()` — a *count* comparison — so requesting `value_fields=a,b,c` on a
record that happens to have 3 fields `{name,age,address}` stores the **whole unprojected line**, silently
including fields that were not requested.

### Trigger / impact

`ingest dir=... file_pattern=*.json file_format=json key_field=name key_prefix=k_ value_fields=*` on any
multi-field json file (the exact form documented at IGroup.java:210): every ingested key's value is
`{"*":null}` — silent mass data corruption with a success reply reporting `put: N skip: 0`. Existing
`IGroupTest` never exercises `*` (only explicit field lists / counts that coincide), so nothing catches it.
(For csv, `value_fields_indexes=*` fails parsing and returns `ERR not integer`; for parquet `value_fields`
is ignored entirely and all schema fields are stored — so json is the broken one.)

### Suggested fix direction

Handle `*` explicitly: if `valueFields.equals("*")`, use the raw line (all fields). Otherwise build the
projection by name lookup (nulls only when a requested field is genuinely absent — optionally error on
missing fields like the key-field does). Drop the count-equality shortcut or make it verify the field-name
sets actually match.

---

## Finding 4 (Bug 79.4): `ingest file_pattern` is an anchored Java regex, not the documented glob — the documented example matches nothing; invalid patterns leak a raw exception

**Severity:** Low
**Class:** Correctness / Edge case
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:210-212 — documented usage (glob-style)
        // ingest dir=/tmp/velo/ingest/ file_pattern=test* file_format=json ...

// src/main/java/io/velo/command/IGroup.java:267-275
        ArrayList<File> list = new ArrayList<>();
        for (var file : files) {
            if (file.getName().matches(filePattern)) {   // Java regex, fully anchored
                list.add(file);
            }
        }
```

### Root cause

`String.matches()` interprets the pattern as a **regular expression**, but the in-code documentation (and
the natural reading of `file_pattern`) implies a shell glob. In regex, `test*` means "tes followed by
zero or more t" — it matches `test`/`testt` but **not** `test1.json` (JDK repro, log §1). So the documented
invocation `file_pattern=test*` matches nothing and the command fails with "not match any file" (or, worse,
a wrong subset — e.g. `*.csv` in regex is ".csv" repeated... actually `*.csv` = zero-or-more of `.` then
`csv`, matching `csv`, `.csv`, `..csv` but not `a.csv`). A syntactically invalid regex (e.g. `test[.csv`)
throws `PatternSyntaxException` out of `handle()` → `RequestHandler` maps any exception to
`ErrorReply(e.getMessage())` → the client receives the raw Java message ("Unclosed character class near
index 8 …").

### Trigger / impact

- `ingest ... file_pattern=test*` (documented form) → `ERR dir: ... file_pattern: test* not match any file`
  even though `test1.json` exists in the dir. Users must guess the undocumented regex semantics.
- `file_pattern=test[.csv` → client sees a raw `PatternSyntaxException` message (internal leak, same class
  as bug 74.8).

### Suggested fix direction

Either translate a glob to a regex (or use `PathMatcher.getFileSystem().getPathMatcher("glob:" + …)`),
or validate the pattern (`Pattern.compile` in a try/catch → descriptive `ErrorReply`) and document the
regex semantics at the usage comment. Existing tests only use exact filenames (`test.csv`), which behave
identically under both semantics — a regression test with `test*` vs `test1.json` is required.

---

## Finding 5 (Bug 79.5): `ingest` csv unvalidated `value_fields_indexes` (and negative `key_field_index`) → raw `ArrayIndexOutOfBoundsException` mid-slot with partial data already written

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:313-339 — csv per-line handling inside the slot worker
                                var lineArray = line.split(",");
                                if (finalKeyFieldIndex >= lineArray.length) {     // only upper bound, only the key
                                    throw new IllegalArgumentException(...);
                                }
                                ...
                                String value;
                                if (valueFieldsIndexes.length == lineArray.length) {
                                    value = line;
                                } else {
                                    var sb = new StringBuilder();
                                    for (int i = 0; i < valueFieldsIndexes.length; i++) {
                                        sb.append(lineArray[valueFieldsIndexes[i]]);  // <-- no bounds check
```

```java
// src/main/java/io/velo/command/IGroup.java:228-245 — index parsing accepts negatives
                keyFieldIndex = Integer.parseInt(keyField);      // "-1" parses fine
```

### Root cause

The key index is bounds-checked (upper bound only) but the value indexes are never validated, and negative
indexes pass the `>= length` check for the key too. An out-of-range or negative index throws
`ArrayIndexOutOfBoundsException` from inside the `doSthInSlots` worker lambda → the promise completes
exceptionally → the client receives `ErrorReply(e.getMessage())` with a raw Java message
("Index 5 out of bounds for length 3"). Lines already processed for that file are persisted — a partial
ingest with an unclassifiable error. Note `IGroupTest` (line 254–258) pins *an* exception for the key-index
case (`getException() != null`), so the fix should keep an exception/propagated error but make it a
descriptive validation error **before** any write.

### Trigger / impact

- csv row `1,aaa,bbb` with `value_fields_indexes=0,1,5` → AIOOBE after earlier rows were already written.
- `key_field_index=-1` → `lineArray[-1]` AIOOBE on the first row.

Admin command, so Low; but it shares the 74.8 "raw exception message leaks to the client" class plus a
partial-write side effect.

### Suggested fix direction

Validate all indexes once, before `doSthInSlots` (or at least before the first write of each file):
`0 <= keyFieldIndex`, `0 <= valueFieldsIndexes[i]`, and return a descriptive `ErrorReply` (mirroring the
existing key-index message style). Per-line upper-bound checks can stay as the IllegalArgumentException path.

---

## Finding 6 (Bug 79.6): multi-file `ingest` silently loses bulk-load mode after the first file

**Severity:** Low
**Class:** Correctness of intent / Performance
**Status:** `CONFIRMED (inspection)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:300-301 — bulk mode set once, before the file loop
        return localPersist.doSthInSlots(oneSlot -> {
            oneSlot.setBulkLoad(true);
            ...
            for (var file : list) {
                if (isCsv || isJson) {
                    ...
                    } finally {
                        oneSlot.resetWritePositionAfterBulkLoad();   // <-- per file, inside the loop
                    }
                } else {
                    ...
                    } finally {
                        oneSlot.resetWritePositionAfterBulkLoad();   // <-- parquet, same
                    }
                }
            }
```

```java
// src/main/java/io/velo/persist/Wal.java:816-850 — resetWritePositionAfterBulkLoad()
        ...
        bulkLoad = false;   // line 849 — clears the flag set by setBulkLoad(true)
```

### Root cause

`resetWritePositionAfterBulkLoad()` (which also sets `wal.bulkLoad = false`, Wal.java:849) runs in a
`finally` **per file**, while `setBulkLoad(true)` is executed once per slot before the loop. After the
first file completes, every remaining file is ingested with bulk-load disabled — the deferred-persistence
optimization that "Ingest start, set bulk load to true" promises is silently off for files 2..N.

### Trigger / impact

Any `ingest` whose `file_pattern` matches ≥2 files: only the first is bulk-loaded. No correctness loss
(rows are still written), but the throughput optimization is gone and the behavior differs by file count.
(The per-file flush itself is likely needed to bound the delayed-value buffer — see Wal.java:827 assert —
so the fix is to re-arm, not to remove the reset.)

### Suggested fix direction

Either call `oneSlot.setBulkLoad(true)` again at the top of each file iteration (keeping the per-file
flush), or restructure so the reset happens once after the loop with an explicit per-file flush variant.
Confirm intent with the persist design (Wal group buffer bounds) before choosing.

---

## Finding 7 (Bug 79.7): `ingest_sst` leaks native RocksDB resources — `Options` never closed; `db` not closed when a slot worker fails

**Severity:** Low
**Class:** Resource leak
**Status:** `CONFIRMED (inspection)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:452-459
        RocksDB db;
        try {
            db = RocksDB.open(new Options(), dirPath);   // <-- Options is a native RocksObject, never closed
        } catch (RocksDBException e) { ... }

// src/main/java/io/velo/command/IGroup.java:509-511 — db closed only in the success aggregation
        }, resultList -> {
            db.close();
```

```java
// src/main/java/io/velo/persist/LocalPersist.java:287-292 — exception path skips the aggregate fn
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                finalPromise.setException(e);   // fn (which closes db) never runs
                return;
            }
```

### Root cause

1. `new Options()` is `AutoCloseable` native memory and is never closed on any path (RocksDB Java requires
   explicit close; the finalizer is only a safety net).
2. `db.close()` lives in the aggregation callback, which `doSthInSlots` skips entirely when any slot
   worker lambda completes exceptionally (e.g. a persist-layer `RuntimeException` from `set()`). The native
   RocksDB handle then leaks for the process lifetime.

### Trigger / impact

Every `ingest_sst` call leaks one `Options`; any slot-worker exception additionally leaks the open `RocksDB`
(and leaves the directory locked on some platforms). Admin command, so Low.

### Suggested fix direction

Use try-with-resources for the `Options` (`try (var options = new Options()) { db = RocksDB.open(options, dirPath); }`),
and close `db` on the failure path as well (e.g. move the close into a `whenComplete` that handles both
outcomes, or wrap the whole block so the close is unconditional).

---

## Finding 8 (Bug 79.8): `INCRBY` / `INCRBYFLOAT` accept Java-only numeric literals Redis rejects (`+5`, `05`, `-0`, `1.5d`, `" 1.5 "`)

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```java
// src/main/java/io/velo/command/IGroup.java:142-147
            long by;
            try {
                by = Long.parseLong(new String(data[2]));   // accepts "+5", "05", "-0"
            } catch (NumberFormatException e) { return ErrorReply.NOT_INTEGER; }

// src/main/java/io/velo/command/IGroup.java:161-166
            double by;
            try {
                by = Double.parseDouble(new String(data[2])); // accepts "1.5d", " 1.5 " (trims), ".5"
            } catch (NumberFormatException e) { return ErrorReply.NOT_FLOAT; }
```

### Root cause

`Long.parseLong` (JDK 7+) accepts a leading `+`, leading zeros and `-0`; `Double.parseDouble` accepts
`d`/`f` suffixes and trims surrounding whitespace (JDK repro, log §1). Redis parses deltas with
`string2ll` / `strict_strtod` semantics, which reject all of these forms (live Redis 7.2.11, log §2:
every probe answers `ERR value is not an integer or out of range` / `ERR value is not a valid float`).
This is the delta-argument twin of bug 74.6's canonical-integer rule (fixed for the RDB writer with
`RDBParser.isRedisIntegerString`), and also applies to the `DGroup.decrby`/`decrbyfloat` twins (same parse
code — flag for symmetry, out of this module's scope to fix there).

### Trigger / impact

`INCRBY k +5` → Velo `:5`, Redis error. `INCRBYFLOAT k 1.5d` → Velo succeeds, Redis `ERR value is not a
valid float`. Clients that round-trip such literals work against Velo and fail against Redis — a hidden
incompatibility. Low because well-behaved clients never send these forms.

### Suggested fix direction

Validate the literal before parsing: reuse the Redis-canonical integer check added by 74.6
(`RDBParser.isRedisIntegerString` or a shared helper) for `incrby`, and a strict-double check
(no suffix, no surrounding whitespace, finite) for `incrbyfloat`; reply NOT_INTEGER / NOT_FLOAT otherwise.

---

## Finding 9 (Bug 79.9): `INFO` section matching is case-sensitive — `INFO SERVER` returns the whole default set instead of only that section

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)`

### Cited files / lines

```groovy
// dyn/src/io/velo/command/InfoCommand.groovy:62-88 — via IGroup.info() (IGroup.java:192-198)
    private StringOrReply getOneSection(String section) {
        if ('server' == section) {          // exact, case-sensitive Groovy String ==
            ...
        } else if ('cluster' == section) {
            ...
        } else {
            // Unknown or empty section → all default sections
            ...
        }
    }
```

### Root cause

Redis matches INFO section names case-insensitively (upstream `strcasecmp`). Velo's exact-match chain
lets `INFO SERVER` / `INFO Memory` fall into the unknown-section branch, which returns **all** default
sections joined — not an error, but the wrong content: a client asking for one section receives six.

### Trigger / impact

Live Redis 7.2.11: `INFO SERVER` → only `# Server` (log §2). Velo: `INFO SERVER` → server+clients+memory+
replication+cpu+cluster. Monitoring that requests `INFO MEMORY` gets a ~6× larger payload and must parse
the right section out itself; tools asserting single-section output break. Not covered by bug_60 (which
fixed unknown/empty-section crashing, not case handling).

### Suggested fix direction

Compare case-insensitively (`equalsIgnoreCase` / Groovy `-equalsIgnoreCase`), or lowercase the argument
once before the chain. Cover `keyspace` in the multi-section branch (119–124) the same way.

---

## Finding 10 (Bug 79.10): `INFO <unknown>` returns all default sections; Redis 7.2.11 returns an empty bulk — conflicts with bug_60's ground-truth premise (needs adjudication)

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — reviewer verdict, adjudicated in *Review Feedback* (recommendation (a))

### Cited files / lines

```groovy
// dyn/src/io/velo/command/InfoCommand.groovy:77-87 — the bug_60 fix
        } else {
            // Unknown or empty section. Redis returns the default content (all standard sections)
            // rather than erroring or emitting a placeholder. ...
            def sb = new StringBuilder()
            for (knownSection in sections) {
                sb << getOneSection(knownSection).s << '\r\n'
            }
            return new StringOrReply(sb.toString())
        }
```

### Root cause

bug_60 finding 1's fix chose "unknown section → default sections" based on the premise *"Redis returns the
default content (all sections) rather than erroring"*. Live Redis 7.2.11 (this round's differential, log §2)
returns an **empty bulk** for `INFO foo` (and `INFO all`/`INFO everything` return all 13 sections there,
while Velo's unknown branch returns the 6 default sections without keyspace). The bug_60 verification
notes were static-inspection only — no live differential was run for that claim.

### Trigger / impact

`INFO foo` → Velo: 6 sections; Redis 7.2.11: empty reply. `INFO all` → Velo: 6 sections, no keyspace;
Redis: 13 sections incl. keyspace. Impact is limited to monitoring fidelity (empty vs default), hence Low —
but the current behavior was chosen on an incorrect ground-truth claim, so it should be re-decided
deliberately rather than left as an accident.

### Suggested fix direction

Decision needed: (a) match Redis 7.2 — empty reply for unknown sections, and give `all`/`everything`
first-class handling (include keyspace); or (b) keep the friendlier default-sections behavior and document
the deliberate divergence in the comment. Either way, correct the comment's factual claim about Redis.

---

## Notes / non-findings

Checked and deliberately **not** reported (so later rounds do not re-investigate):

- **Shared `decrBy` internals are all fixed at HEAD** (DGroup.java:354–425): TTL preservation (74.1),
  double-value rejection (74.2), int64 delta (74.3), overflow guard (74.4), explicit `isByFloat` (74.5),
  non-finite guard (74.8). `IGroupTest` covers each through the INCR* paths. Not re-reported.
- `IGroup.parseSlots()` arity guards are consistent with `handle()` (exact arity checked in `handle()` before
  `slotWithKeyHashListParsed` is touched; format-error tests pin it).
- `incr` → `decrBy(-1, 0, false)`: negation of `-1` cannot wrap; safe.
- `incrbyfloat`'s `-by` negation for doubles is value-safe (no wrap; the `-0.0` case is handled by the
  explicit `isByFloat` flag from the 74.5 fix; non-finite deltas are caught by the 74.8 guard).
- `FileReader` default charset is UTF-8 on JDK 21 (JEP 400) — no charset bug in the ingest readers.
- Ingest CSV `split(",")` ignores RFC 4180 quoting — a known simplification of the Velo-specific tool, not
  a protocol surface; a json `key_field` whose value is JSON null produces a `...null` key string — same
  category. Both noted for the eventual ingest hardening, not reported.
- Ingest mid-file `IOException` is caught, logged, and the file is skipped — the reply still reports
  per-slot counts (partial ingest is visible only in server logs). Robustness gap of the admin tool; noted.
- `ingest_sst` accumulates **all** delayed values into one Wal group buffer before the single reset
  (`assert offset < ONE_GROUP_BUFFER_SIZE`, Wal.java:827/842) — a large SST risks the assert (no `-ea` in
  production, so it would write past the group buffer). Persist-layer concern triggered by an admin
  command's usage pattern; belongs to a persist round, not IGroup.
- `ingest_sst`'s first-10000-keys sampling loop is intentional (log examples), as is `db` being shared
  across slot workers (RocksDB iterators are per-thread safe).
- The 8th `schema_file` argument is silently ignored for csv/json (only parquet checks `data.length != 8`).
- INFO content breadth (no `persistence`/`stats` sections, `expires=0` hard-coded in keyspace, avg_ttl
  averaged across slots) — informational fidelity, bug_60 territory, accepted; not re-opened.
- Redis `INCRBYFLOAT` accepts `1e400` (long double, replies a 400-digit value) while Velo answers
  `ERR not float` — deliberate documented divergence from bug_78's e2e notes (no long double in Java); not
  re-reported.
- `handle()` returning `NilReply` for unknown commands is the group-wide pattern (bug_74.9 fixed the DEBUG
  variant only); IGroup registers exactly incr/incrby/incrbyfloat/info/ingest/ingest_sst.
- Command-registry metadata (`ingest` arity −7, `info` flags, `since` strings) matches Redis 7.2 where the
  command exists in Redis; `ingest*` are Velo-specific (`DECRBYFLOAT`-style `2.6.0` on `incrbyfloat` is
  correct — that one *is* a Redis 2.6.0 command).

## Reviewer checklist (for AI agent 2)

Environment: Velo via `InMemoryGetSet` bypass Spock temp spec (pattern: `Bug74VerifyTempSpec`,
`src/test/groovy/io/velo/command/`), plus live Redis 7.2.11 (`redis-server --port 31999 --daemonize yes
--save '' --appendonly no`; the author used 31991, log §2). Author log:
`doc/bug_reviews/logs/20261028_190300_igroup_differential_redis72_bug_79.log`.

- Finding 79.1: spec: `incrbyfloat a 0.001` ×3 on an empty key → assert each reply's `DoubleReply.buffer()`
  bytes: current `0.00` each time, stored cv `numberValue() == 0.0` vs Redis `0.001/0.002/0.003`;
  also `SET b 10.50`-equivalent double cv + `incrbyfloat b 0.1` → buffer `10.60` vs Redis `10.6`.
- Finding 79.2: spec: cv long 0 → `incrby a -9223372036854775808` → expect ErrorReply.INCR_DECR_OVERFLOW
  (Redis: the value); cv long −5 → same command → expect IntegerReply 9223372036854775803 and the stored cv
  changed (Redis: error + untouched). Also control: `incrby a -9223372036854775807` on −5 stays an error in
  both.
- Finding 79.3: spec: write a 2-line 3-field json file, run `ingest ... file_format=json key_field=name
  key_prefix=t_ value_fields=*` → read back a stored value → currently `{"*":null}`; also
  `value_fields=x,y,z` (3 names, none in record, counts coincide) → raw line stored.
- Finding 79.4: spec/static: dir with `test1.json`; `file_pattern=test*` → ErrorReply "not match any file"
  while `test.*` matches; `file_pattern=test[.csv` → reply contains "Unclosed character class".
- Finding 79.5: spec: csv row `1,aaa,bbb`, `value_fields_indexes=0,1,5` → AsyncReply exception message
  contains "Index 5 out of bounds"; `key_field_index=-1` → "Index -1 out of bounds".
- Finding 79.6: static (Wal.java:849 + per-file finally vs single setBulkLoad) or spec with 2 files
  instrumenting `OneSlot.walArray[0].bulkLoad` between files (needs test hook; static may suffice).
- Finding 79.7: static: `Options` never closed; `db.close()` only inside the aggregate lambda;
  `LocalPersist.doSthInSlots` (276–303) skips the aggregate on exception. Optional: reflection on
  `RocksObject` ownership/`isOwningHandle` after the command.
- Finding 79.8: spec: `incrby a +5` / `incrby a 05` / `incrby a -0` → currently IntegerReply; Redis errors
  (log §2). `incrbyfloat a 1.5d` / `" 1.5 "` → currently DoubleReply; Redis `ERR value is not a valid float`.
- Finding 79.9: live: `INFO SERVER` → Redis single `# Server` section (log §2); Velo (spec through
  `IGroup.handle()` with `data=['info','SERVER']`) → response contains `# Clients` too.
- Finding 79.10: live: `redis-cli --raw info foo` → empty (log §2); `info all | grep -c '^#'` → 13.
  Adjudicate against bug_60's premise and pick (a)/(b) from the fix direction.

## Review Feedback (AI agent 2, round 1)

**Method:** Spock temp spec + live Redis 7.2.11 differential + static inspection + upstream redis source
(`~/ws/redis`, 7.2 branch).

**Environment**

- Velo HEAD `03821ace` ("docs: bug 79 igroup review round 1 discovery"), branch `review/igroup`.
- Reviewer temp specs (prints only, **not** permanent tests):
  - `src/test/groovy/io/velo/command/Bug79ReviewTempSpec.groovy` — 8/8 green. Drives `IGroup.handle()`
    through `BaseCommand.execute()` with an `InMemoryGetSet` bypass for the `INCR*`/`INFO` cases and with a
    real `LocalPersist` (`LocalPersistTest.prepareLocalPersist()`, 1 slot) for the `ingest` cases.
  - `src/test/groovy/io/velo/command/Bug79ExtraTempSpec.groovy` — scope + multi-section probes
    (`DECRBY`/`INCRBY` extreme deltas, `info` multi-arg forms, `INCRBYFLOAT` accumulation/storage type).
- Live Redis **7.2.11** (`redis-server v=7.2.11 sha=d4c381df:0`) on port 31999 for the differential.
- Raw evidence log: `doc/bug_reviews/logs/20261109_091146_igroup_review_agent2_bug_79.log`
  (second pass, supersedes `logs/20261028_193000_igroup_review_agent2_bug_79.log`).

| Finding | Verdict | Evidence |
|---|---|---|
| 79.1 | `CONFIRMED (reproduced)` | spec: `incrbyfloat f1 0.001` ×3 → replies `0.00`,`0.00`,`0.00`, **stored** value `0.0`; `10.50`+`0.1` → reply `10.60`. Redis: `0.001`,`0.002`,`0.003` and `10.6` |
| 79.2 | `CONFIRMED (reproduced)` | spec: value `0` → `ERR increment or decrement would overflow` (Redis: `-9223372036854775808`); value `-5` → reply **and stored** `9223372036854775803` (Redis: overflow error, value stays `-5`); control `-9223372036854775807` on `-5` → overflow error, value kept (matches Redis) |
| 79.3 | `CONFIRMED (reproduced)` | spec: `value_fields=*` → stored value `{"*":null}`; `value_fields=x,y,z` on a 3-field record → stored value is the **raw unprojected line** `{"name":"name0","age":20,"address":"address0"}` |
| 79.4 | `CONFIRMED (reproduced)` | spec: `file_pattern=test*` → `ERR dir: … file_pattern: test* not match any file` while `test.*` matches; `file_pattern=test[.csv` → raw `java.util.regex.PatternSyntaxException: Unclosed character class near index 8` escapes `handle()` |
| 79.5 | `CONFIRMED (reproduced)` | spec: `value_fields_indexes=0,1,5` → `ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 4` **after row 1 was already written** (`stored after partial write = true`); `key_field_index=-1` → `ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 3` |
| 79.6 | `CONFIRMED (inspection)` | spec: `oneSlot.setBulkLoad(true)` → `wal.bulkLoad = true`; `oneSlot.resetWritePositionAfterBulkLoad()` → `wal.bulkLoad = false`. Code: `setBulkLoad(true)` is called once before the file loop (IGroup.java:301) while the reset is in the per-file `finally` (IGroup.java:373 / 415) and `Wal.resetWritePositionAfterBulkLoad()` clears the flag (Wal.java:849) |
| 79.7 | `CONFIRMED (inspection)` | IGroup.java:454 `RocksDB.open(new Options(), dirPath)` — the `Options` native handle is never closed on any path, and rocksdbjni 9.7.3 `AbstractNativeReference` has **no** finalizer/`Cleaner` (javap: no `finalize`, only `close()`), so the native memory is not reclaimed. `db.close()` exists only in the aggregate lambda (IGroup.java:510) which `LocalPersist.doSthInSlots` skips when a slot promise fails (LocalPersist.java:287-292) |
| 79.8 | `CONFIRMED (reproduced)` | spec: `incrby +5` → `5`, `incrby 05` → `10`, `incrby -0` → `10`, `incrbyfloat 1.5d` → `1.5`, `incrbyfloat " 1.5 "` → `1.5`. Redis rejects every one of them |
| 79.9 | `CONFIRMED (reproduced)` | spec: `info server` → headers `[# Server]`; `info SERVER` → headers `[# Server, # Clients, # Memory, # Replication, # CPU, # Cluster]`; `info MEMORY` → the same 6 headers. Redis: `info SERVER` → 1 section |
| 79.10 | `CONFIRMED (reproduced)` | spec: `info foo` → the 6 default headers, 1048 bytes; `info all` → the same 6 headers. Redis: `info foo` → 0 bytes; `info all` / `info everything` → 13 sections; `info default` → 11 sections |

### Finding-by-finding notes

**79.1 — CONFIRMED (reproduced).** Both halves of the claim are reproduced: the *reply* is quantized
(`$4\r\n0.00\r\n`) **and** the *stored* value is quantized (`numberValue() == 0.0`), so the loss is persistent
and not a display artefact — `setNumber(newValue.doubleValue(), …)` (DGroup.java:407) stores the rounded
double. The finding is the exact deferred twin of bug_78.10: the hash side was fixed in `259f9e37`
(`HGroup.hincrbyFloatSplit`, which uses `BigDecimal.valueOf(...)` with **no** `setScale` and
`stripTrailingZeros()`), the string side was not.

Two extra constraints for the fix agent:

1. `DoubleReply` is used **only** by `DGroup.decrBy()` — `grep -rn "new DoubleReply" src/main` yields exactly
   one hit (`DGroup.java:408`), so changing its formatting cannot leak into another command family. But
   `doubleScale` is also a *display* knob (`number.doubleScale`) and the product decision "is `doubleScale` a
   display-only setting?" is still open (bug_78 fix order). The fix must not change the hash module's
   behaviour.
2. Redis 7.2.11 replies `INCRBYFLOAT` with a **bulk string** with trailing zeros stripped (`0.001`, `10.6`);
   the RESP2 shape of Velo's `DoubleReply` is already a bulk string, so only the *content* formatting has to
   change. Upstream reference: `src/t_string.c:664-687` `incrbyfloatCommand` uses
   `createStringObjectFromLongDouble(value, 1)` (`LD_STR_HUMAN`, i.e. 17 significant digits with trailing
   zeros stripped) — `HGroup.formatFloatValue()` already mirrors that rule.

Reviewer-added note on the **storage** side (not covered by the finding text): Velo stores the result as a
**binary double** (`SP_TYPE_NUM_DOUBLE`, `CompressedValue.java:43`), not as a decimal string. `GET` on such a
key renders through `BaseCommand.getValueBytesByCv()` → `String.valueOf(cv.numberValue())`, i.e. `0.3` →
`"0.3"` (probe: `incrbyfloat g 0.1` ×3 → stored `spType = -16`, value `0.3`). Redis stores the *string*
`"0.3"` (`OBJECT ENCODING` → `embstr`) and answers `GET g` → `0.3`. The two agree for this case, but the
representations differ, so the fix agent must decide deliberately whether `INCRBYFLOAT` keeps storing a double
(matching current Velo behaviour) or switches to a decimal string (matching Redis exactly). Storing a double
also means `0.1 + 0.2` accumulates as IEEE-754 (`0.30000000000000004`), which Redis' `long double` avoids —
another reason the fix must not silently keep `BigDecimal.valueOf(double)` semantics for the *stored* value.

**79.2 — CONFIRMED (reproduced).** Both the false overflow and the silent wraparound are reproduced
(`0 → ERR`, `-5 → 9223372036854775803` stored). The root cause is exactly as cited: `IGroup.java:153`
negates the parsed delta and `-Long.MIN_VALUE == Long.MIN_VALUE`; the decr-side guard then evaluates the
wrong operation (`Long.MAX_VALUE + by == -1`). The control case (`-9223372036854775807` on `-5` → overflow
error, value kept) behaves correctly in Velo, which matches Redis and confirms the defect is limited to the
`Long.MIN_VALUE` delta.

**Correction to the finding's scoping note (see the corrected block in Finding 2 above):** the original text
claimed "`DGroup.decrby` does not negate, so `DECRBY k -9223372036854775808` is correct". **That is wrong.**
Upstream `src/t_string.c:652-660` `decrbyCommand` rejects the delta outright:

```c
    /* Overflow check: negating LLONG_MIN will cause an overflow */
    if (incr == LLONG_MIN) {
        addReplyError(c, "decrement would overflow");
        return;
    }
```

Live Redis 7.2.11 confirms: `DECRBY k -9223372036854775808` answers `ERR decrement would overflow` and
leaves the value untouched **for every stored value** (probed `0`, `-5`, `Long.MIN_VALUE`).

Velo's `DGroup.decrby` has no such rejection — it accepts the delta and computes `longValue - by`. Probe
(`Bug79ExtraTempSpec`, `DGroup.execute('decrby …')`):

```
EXTRA decrby d1 -9223372036854775808 -> ErrorReply('increment or decrement would overflow') stored=0
EXTRA decrby d2 -9223372036854775808 -> Int(9223372036854775803) stored=9223372036854775803
EXTRA decrby d3 9223372036854775808 -> ErrorReply('not integer') stored=0
```

So on a stored `-5` Velo **silently stores and replies `9223372036854775803`** where Redis errors and keeps
`-5` — a wraparound bug in the `DECR*` twin, in `DGroup` (bug_74's module), on a *different* code path than
79.2 (`IGroup.java:153`).

Consequences for the fix agent:

- 79.2 itself is **not** re-scoped: the `INCRBY` defect is still exactly `IGroup.java:153`, and it is still
  the only *newly introduced* defect in the `INCR*` family.
- The `DECRBY` divergence is a **separate, pre-existing** `DGroup` defect (bug_74 territory). It is recorded
  in *Additional observations* as a follow-up, **not** as a new finding here — it was not in the author's
  scope and it must not be silently folded into the 79.2 commit.
- The upstream overflow check is **addition-shaped**, not subtraction-shaped
  (`src/t_string.c:610-611`): `(incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
  (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))`. Upstream therefore never negates the delta.
  The 79.2 fix should plumb the **true increment** into `DGroup` (an addition-shaped guard), which is exactly
  what the finding's *Suggested fix direction* already proposes — that direction is confirmed correct.

**79.3 — CONFIRMED (reproduced).** The `*` case stores the literal `{"*":null}` — reproduced exactly. The
second half of the finding (the count-equality shortcut) is also reproduced: `value_fields=x,y,z` on a
3-field record stores the **entire raw line** including fields that were not requested. Note that the same
count-equality shortcut exists in the **csv** path (IGroup.java:328 `valueFieldsIndexes.length ==
lineArray.length`) — see *Additional observations*. The reviewer also confirms the author's scoping note:
`value_fields=*` is accepted by the parser for csv (it fails later at `Integer.parseInt("*")` → `ERR not
integer`) and is completely ignored for parquet (all schema fields are stored).

**79.4 — CONFIRMED (reproduced), with one prose correction.** The core claim holds: the documented
`file_pattern=test*` matches nothing (regex `test*` = "tes" + zero-or-more "t"), and an invalid pattern
escapes `handle()` as a raw `PatternSyntaxException`.

Correction to the finding text: the author's example "`*.csv` in regex is '.csv' repeated … matching `csv`,
`.csv`, `..csv`" is **wrong** — a leading `*` is a dangling metacharacter: `*.csv` throws
`PatternSyntaxException: Dangling meta character '*' near index 0` (JDK repro, log §4). This makes the leak
*worse* than described, not better: the most natural glob form a user would type for "all csv files" is a
hard error. Also confirmed: the raw exception message reaches the client — `RequestHandler` maps an uncaught
`Exception` to `new ErrorReply(e.getMessage())` (RequestHandler.java:494-496 / 512-514 / 573-579).

**79.5 — CONFIRMED (reproduced).** Both index defects are reproduced, and the *partial write* side effect is
reproduced: with a 2-row csv (`1,aaa,bbb` then `2,ccc,ddd,eee`) and `value_fields_indexes=0,1,5`, row 1 is
written (the count-equality shortcut even stores it unprojected) and row 2 raises
`ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 4`; the client receives that raw Java
message through `ErrorReply(e.getMessage())`. `key_field_index=-1` reproduces
`Index -1 out of bounds for length 3` on the first row. The reviewer confirms the author's note that
`IGroupTest` (lines 254-258) already pins *an* exception for the key-index case, so the fix must keep
propagating an error but make it descriptive and validate **before** any write.

**79.6 — CONFIRMED (inspection).** The mechanism is dynamically demonstrated (`setBulkLoad(true)` →
`bulkLoad = true`; `resetWritePositionAfterBulkLoad()` → `bulkLoad = false`) and the code placement is as
cited: `oneSlot.setBulkLoad(true)` once at IGroup.java:301, the reset inside the per-file `finally`
(IGroup.java:373 for csv/json and 415 for parquet), and `Wal.resetWritePositionAfterBulkLoad()` sets
`bulkLoad = false` at Wal.java:849. Consequence: files 2..N of a multi-file ingest are written **without**
bulk-load mode, so the deferred-persistence optimization promised by the log line "Ingest start, set bulk load
to true" only applies to the first file. No correctness loss (the values are still written and flushed by the
per-file reset), so Low is the right severity. The fix should re-arm the flag per file (keep the per-file
flush) — removing the reset would trip the `assert offset < ONE_GROUP_BUFFER_SIZE` (Wal.java:827/842).

**79.7 — CONFIRMED (inspection).** Two independent native-resource defects:

1. `new Options()` at IGroup.java:454 is never closed on any path. The reviewer additionally verified that
   this is a **real** leak and not "GC will clean it up": rocksdbjni 9.7.3's
   `org.rocksdb.AbstractNativeReference` implements `AutoCloseable` only — `javap -p` shows no `finalize()`
   and no `Cleaner` registration (only `close()`), the class file contains no `finalize`/`clean` reference at
   all, and `RocksObject` merely adds `disposeInternal()`.
2. `db.close()` lives only in the aggregate callback (IGroup.java:510), and
   `LocalPersist.doSthInSlots` skips that callback entirely when any slot promise completes exceptionally
   (LocalPersist.java:287-292) — so a failing slot worker leaks the open `RocksDB` handle (and the directory
   lock) for the process lifetime. Fix direction from the finding is accepted: try-with-resources for
   `Options` and an unconditional close of `db`.

**79.8 — CONFIRMED (reproduced).** All five probed literals are accepted by Velo and rejected by Redis
7.2.11. Refinement of the finding text: Redis **does** accept `.5` and `1e3` for `INCRBYFLOAT` (verified
live), so the rejected set is exactly "leading `+`, leading zeros, `-0`, `d`/`f` suffix, surrounding
whitespace" — the finding's examples are all correct, the accepted-set must not be over-broadened by the fix.
For `INCRBY`, Redis additionally rejects `1.0`, `0x10`, `1e3`, `--5` and surrounding whitespace.

Upstream reference for the fix: Redis uses `string2ll()` (`src/util.c:438-503`), which rejects a leading
`+`, leading zeros and `-0`, and `string2ld()` (`src/util.c:547-568`, `strtold` + strict end-of-string and
no-leading-space checks), which rejects the `d`/`f` suffix and surrounding whitespace while accepting `.5`
and `1e3`.

Note for the fix agent: Velo's `ErrorReply.NOT_INTEGER` renders as `ERR not integer` (not Redis's
`ERR value is not an integer or out of range`) — that message difference is pre-existing and out of scope,
the fix should reuse the existing error replies.

**79.9 — CONFIRMED (reproduced).** `info server` → one section, `info SERVER` → six sections, `info MEMORY`
→ six sections. Redis 7.2.11 matches the section name case-insensitively (`info SERVER` → 1 section,
verified live).

Upstream reference for the fix: `genInfoSectionDict()` (`src/server.c:5427-5463`) stores the requested
section names in a `stringSetDictType` dict, whose hash/compare functions are **case-insensitive**
(`dictCStrCaseHash` / `dictCStrKeyCaseCompare`, `src/server.c:559-566`), and `genRedisInfoString()` looks
sections up with `dictFind(section_dict, "server")` etc. — so the case-insensitivity comes from the dict, not
from an explicit `strcasecmp`. The `default` / `all` / `everything` keywords *are* matched with `strcasecmp`
(`src/server.c:5448-5453`).

The finding's fix direction (case-insensitive comparison) is accepted; the reviewer notes that the
`keyspace` branch in `handle()` (InfoCommand.groovy:119-124) needs the same treatment, and that the
multi-section branch (data.length > 2) is a Velo extension — Redis 7.2's `INFO` takes at most one section
argument — so it must keep working while the single-section lookup becomes case-insensitive.

**79.10 — CONFIRMED (reproduced); adjudication: option (a).** The bug_60 premise was factually wrong: live
Redis 7.2.11 answers `INFO foo` with an **empty bulk** (0 bytes), and `INFO all` / `INFO everything` with
**13** sections, `INFO default` with **11**. Velo answers `info foo` with the 6 default sections
(1048 bytes) and `info all` with the same 6 (no keyspace).

Upstream reference for the adjudication: `genInfoSectionDict()` (`src/server.c:5447-5457`) adds an unknown
name to the section dict as a plain key, but `genRedisInfoString()` only emits a section when
`dictFind(section_dict, "<known-name>")` hits — so an unknown-only request produces an **empty** reply, and
`all` / `everything` set `out_all = 1` which forces every section to be emitted. Option (a) is therefore not
just "nicer to match", it is the literal upstream behaviour.

The reviewer picks **option (a) — match Redis 7.2**: an unknown section returns an empty reply, and
`all`/`everything`/`default` get first-class handling (the section set Redis exposes is wider than Velo's 6,
so `all` should return at least everything Velo can produce, including keyspace). Rationale: the current
behaviour was chosen on a ground-truth claim that the reviewer has now disproved, so keeping it would
preserve an *accident*; and a monitoring client that asks for one section and silently receives six (79.9) or
six instead of an empty reply (79.10) has to parse the output itself. Whatever the fix agent chooses, the
comment at InfoCommand.groovy:78-79 ("Redis returns the default content (all standard sections)") must be
corrected — it is currently a false statement about Redis in the source.

### Additional observations

- **`DECRBY` with a `Long.MIN_VALUE` delta is a separate, pre-existing `DGroup` bug** (follow-up, not
  re-reported here). Upstream rejects the delta unconditionally
  (`src/t_string.c:656-659`, "decrement would overflow"); Velo's `DGroup.decrby` accepts it and on a stored
  `-5` stores/replies `9223372036854775803` (probe `EXTRA decrby d2 …` in the log). This lives in bug_74's
  module (`DGroup`), not in `IGroup`, and the author did not report it — so it is **not** a 79.x finding.
  It is flagged because the 79.2 fix touches the same shared `DGroup.decrBy()` guard; the fix agent must
  not fold it into the 79.2 commit, and should open it as a follow-up in the DGroup/string module.
- **The count-equality shortcut also exists in the csv path** (IGroup.java:328
  `valueFieldsIndexes.length == lineArray.length`): with `value_fields_indexes=0,1,5` on a 3-column row the
  whole raw row is stored instead of the requested projection, and the very same row is what hides the
  out-of-range index until a later, longer row appears (79.5's partial write). The fix for 79.3/79.5 should
  drop/repair this shortcut on **both** paths, not only the json one.
- **`INCRBYFLOAT` error text for `inf`**: Velo answers `ERR not float` (the 74.8 non-finite guard),
  Redis 7.2.11 answers `ERR increment would produce NaN or Infinity`. Message-level difference only, not
  reported as a finding; do not let the 79.1 fix change it accidentally.
- **`INFO` with no argument** returns Velo's 6 sections + keyspace (7) while Redis 7.2.11 returns its 11
  default sections. Informational fidelity, bug_60 territory — **not** re-opened here, but the 79.10 fix
  touches the same file, so the fix agent must not accidentally shrink the no-argument reply.
- **The multi-argument `INFO` branch duplicates content for an unknown section** (reviewer probe):
  `info server foo` returns headers `[# Server, # Server, # Clients, # Memory, # Replication, # CPU, # Cluster]`
  — the unknown `foo` falls into the `getOneSection()` unknown-branch and appends **all six default sections
  again**; `info SERVER foo` returns the 6 defaults *twice* (12 headers). Redis returns only `# Server` for
  both (`info server foo` → 1 section, `info foo server` → 1 section — an unknown name is simply ignored in a
  multi-section request). Same root cause as 79.9/79.10, one level deeper: the 79.10 fix must make the
  unknown-section branch return empty **in the multi-arg path too**, not only for a single argument.
- `INFO server` throws a raw NPE when `LocalPersist` has no slot (`firstOneSlot` is null,
  InfoCommand.groovy:145) — a test-environment artifact only, the production server always has a slot.
  Not a finding.

### Recommended fix order (for the fix agent)

One bug = one commit, each with its own failing regression test first and a JaCoCo confirmation afterwards
(`scripts/jacoco_cover.py`). Proposed order (highest correctness/compat impact first, lowest-risk last):

1. **79.1** — `INCRBYFLOAT` precision (Medium, user-visible data loss, deferred from 78.10; the hash-side
   precedent `259f9e37` already exists). Decide the `doubleScale` product question first, then mirror the
   hash-side fix on the string side; the hash module must not change. Decide the storage representation
   (double vs decimal string) explicitly — see the reviewer-added note in 79.1.
2. **79.2** — `INCRBY` `Long.MIN_VALUE` delta (Medium, wrong value stored + a legal command rejected). Plumb
   the true increment and use an addition-shaped guard (upstream shape, `src/t_string.c:610-611`). Do **not**
   fold in the `DECRBY` divergence — it is a separate `DGroup` bug (see *Additional observations*).
3. **79.3** — `ingest` json `value_fields=*` + the count-equality shortcut on **both** json and csv (Medium,
   silent mass data corruption on the documented invocation).
4. **79.9** — `INFO` case-insensitive section matching (Low, one-line change, unblocks 79.10's `all`/`default`
   handling). Cover the `keyspace` branch in the multi-arg path (InfoCommand.groovy:119) too.
5. **79.10** — `INFO` unknown-section reply + `all`/`everything`/`default` handling (Low; do it right after
   79.9 because it edits the same function). The unknown-section branch must return empty in the **multi-arg**
   path as well (see *Additional observations*), and the no-argument reply must not shrink.
6. **79.5** — `ingest` index validation before any write (Low, raw AIOOBE + partial write).
7. **79.4** — `ingest` `file_pattern` glob-vs-regex + invalid-pattern handling (Low; decide glob vs regex
   first, then either translate the glob or document the regex semantics).
8. **79.6** — re-arm bulk-load per file (Low, performance only).
9. **79.7** — `ingest_sst` `Options`/`db` close (Low, resource leak).
10. **79.8** — strict literal parsing for `incrby`/`incrbyfloat` (Low, protocol compatibility; keep it last
    because it is the most behaviour-changing of the Low items and needs the canonical-integer helper from
    bug_74.6). Redis accepts `.5` and `1e3` for `incrbyfloat` — do not over-reject.

## Response (AI agent 1, round 1)

**Method:** re-verified the reviewer's feedback against Velo HEAD (`03821ace`), upstream redis source
(`~/ws/redis`, 7.2 branch), and the still-running live Redis 7.2.11 on port 31999 (re-probed this session).

**Bottom line: I agree with all 10 CONFIRMED verdicts, accept both corrections to my finding text
(79.2 scoping note, 79.4 `*.csv` example — both my errors, retracted below), and accept the 79.10
adjudication (option (a)). Two reviewer notes carry factual errors that must be corrected before the fix
phase: the 79.9 "Velo extension" claim (disagreed, evidence below) and the 79.8 "rejected set is exactly
…" sentence (refined). Neither changes a verdict; both change fix-phase guidance.**

Per finding:

- **79.1 — AGREE.** Independently confirmed: `grep -rn "new DoubleReply" src/main` → exactly one hit
  (`DGroup.java:408`); upstream `incrbyfloatCommand` (`src/t_string.c:664-687`) stores and replies a
  decimal *string* via `createStringObjectFromLongDouble(value, 1)` and replicates it as `SET` with the
  final value (t_string.c:689-694). One precision on the reviewer's cite: `LD_STR_HUMAN` is
  `snprintf("%.17Lf")` — 17 digits **after the decimal point** (`src/util.c:824-840`), not "17 significant
  digits" (the Redis docs' loose wording). For the fix this is immaterial if we mirror the hash-side
  precedent `HGroup.formatFloatValue()` (HGroup.java:1645-1652, `stripTrailingZeros().toPlainString()`),
  which I verified produces the right shape. Storage-representation decision (keep `SP_TYPE_NUM_DOUBLE`
  vs decimal string) accepted as a required, explicit decision.
- **79.2 — AGREE; correction ACCEPTED; my original scoping note RETRACTED.** Verified in
  `~/ws/redis/src/t_string.c:652-662`: `decrbyCommand` rejects `LLONG_MIN` unconditionally with
  `ERR decrement would overflow` (live re-probe: error, value untouched). My claim "`DGroup.decrby` does
  not negate, so `DECRBY k -9223372036854775808` is correct" was wrong. Guard math re-checked at
  DGroup.java:410-421: for stored `-5`, `by = Long.MIN_VALUE` → `Long.MAX_VALUE + by == -1`, `-5 > -1`
  false → `longValue - by` wraps to `9223372036854775803` — matches the reviewer's EXTRA probe. Live
  controls: `INCRBY k(Long.MIN_VALUE) 0→-9223372036854775808` (legal, works), `-5` → error + untouched.
  Agreed: the `DECRBY` twin is a separate `DGroup` follow-up, not part of the 79.2 commit. Agreed: the
  reviewer's reading of my "keep DGroup's existing guard untouched" (preserve `DECR*` semantics, add an
  addition-shaped increment variant, upstream shape at `t_string.c:610-611`) is what I meant.
- **79.3 — AGREE.** The csv twin of the count-equality shortcut confirmed (IGroup.java:~326-329
  `valueFieldsIndexes.length == lineArray.length` → whole raw row stored). Fix must cover both paths.
- **79.4 — AGREE; prose correction ACCEPTED; my `*.csv` example RETRACTED.** JDK repro re-run:
  `*.csv` → `PatternSyntaxException: Dangling meta character '*' near index 0` (leading `*` is a dangling
  quantifier in Java regex — my "zero-or-more `.`" description was of `.*.csv`, not `*.csv`);
  `test*` matches neither `test1.json` nor `csv`; `test.*` matches `test1.json`. The reviewer is right
  that the natural glob form is a hard error, which makes the leak worse.
- **79.5 — AGREE.** No value-index bounds check and negative key index passes the upper-bound-only check,
  as cited.
- **79.6 — AGREE.** Line anchors verified at HEAD: `setBulkLoad(true)` once at IGroup.java:301, per-file
  `resetWritePositionAfterBulkLoad()` at IGroup.java:373 (csv/json) and :415 (parquet), flag cleared at
  Wal.java:849. Fix = re-arm per file, keep the per-file flush.
- **79.7 — AGREE; reviewer's no-finalizer verification independently confirmed.** `javap -p` on
  rocksdbjni-9.7.3.jar (Gradle cache): `AbstractNativeReference` declares only `isOwningHandle()` /
  `close()`; `RocksObject` adds only `disposeInternal(...)` — **no `finalize()` and no `Cleaner`**. My
  original "the finalizer is only a safety net" hedge was wrong: there is no finalizer, the `Options`
  leak is unconditional. `LocalPersist.doSthInSlots` skip-on-exception path confirmed
  (LocalPersist.java:287-292).
- **79.8 — AGREE with the verdict and the accepted-set warning, but the sentence "the rejected set is
  exactly 'leading `+`, leading zeros, `-0`, `d`/`f` suffix, surrounding whitespace'" is **wrong as
  written for `INCRBYFLOAT`** — it conflates the two commands. Live Redis 7.2.11 (re-probed this session):
  `INCRBYFLOAT` **accepts** `+1.5` → `1.5`, `05` → `5`, `-0` → `0`, `.5` → `0.5`, `1e3` → `1000`; it
  rejects only `1.5d` / `1.5f` / ` 1.5 `. Upstream: `string2ll` (util.c:438-502) rejects leading `+`,
  leading zeros and `-0` (first digit must be 1-9) — that is the `INCRBY` side only; `string2ld`
  (util.c:547-568) is `strtold`-based and rejects only trailing garbage, surrounding whitespace, NaN and
  ERANGE-with-inf/zero. Fix-agent rule: integer strictness applies to `incrby` only; `incrbyfloat` must
  keep accepting strtold-shaped literals, else the fix introduces a *new* incompatibility.
- **79.9 — AGREE with the verdict and fix direction; DISAGREE with the parenthetical "the multi-section
  branch (data.length > 2) is a Velo extension — Redis 7.2's `INFO` takes at most one section
  argument".** That is factually wrong: `~/ws/redis/src/commands/info.json` documents `"arity": -1` with
  history `["7.0.0", "Added support for taking multiple section arguments."]` and the section argument
  `"multiple": true`; live `INFO server clients` → `# Server` + `# Clients` (re-probed this session).
  Consequence for the fix: the multi-arg branch is **Redis-compatible surface**, not a Velo extension —
  its upstream semantics are defined by `genInfoSectionDict` + `genRedisInfoString` (each argument
  case-insensitive; unknown names silently ignored — the reviewer's own probe `info server foo` → 1
  section *is* that upstream behavior, live-confirmed). The substance of the fix guidance is unchanged
  (case-insensitive lookup + unknown → empty in **both** paths), but the justification must be "match
  upstream multi-arg semantics", not "Velo extension that must keep working".
- **79.10 — AGREE; adjudication (a) ACCEPTED.** Upstream mechanism verified: `genInfoSectionDict`
  (server.c:5445-5460) adds unknown names to the dict as plain keys; `genRedisInfoString` emits a section
  only when `dictFind(section_dict, "<known-name>")` hits (server.c:5475 et al.) → unknown-only request
  yields an empty string. Live: `info foo` → 0 bytes, `info all` → 13 sections, `info default` → 11.
  The same mechanism gives the multi-arg requirement (unknown names ignored, not expanded — fixing the
  reviewer's basis via 79.9 above). The comment at InfoCommand.groovy:78 ("Redis returns the default
  content") is confirmed false against the live server and must be corrected by the fix.

Evidence-hygiene note: the Review Feedback cites
`src/test/groovy/io/velo/command/Bug79ExtraTempSpec.groovy`, but that file does not exist in the tree
(only `Bug79ReviewTempSpec.groovy`); its §3 EXTRA probes are recorded in
`logs/20261109_091146_igroup_review_agent2_bug_79.log`, which — temp specs being prints-only by
convention — is the evidence of record. No verdict is affected; the doc should not imply the file exists.

## Fix Log (AI agent 1, round 1 fix phase)

All fixes on branch `review/igroup`, based on `03821ace`. Per the agreed order: 79.1 → 79.2 → 79.3 →
79.9 → 79.10 → 79.5 → 79.4 → 79.6 → 79.7 → 79.8. Every fix followed red-green: the failing test was
written and watched fail first, then the minimal production change, then the focused test green, then
JaCoCo line/branch confirmation via `scripts/jacoco_cover.py`.

| Bug | Failing test evidence (RED) | Fix commit | Focused test (GREEN) | JaCoCo |
|---|---|---|---|---|
| 79.1 | `test incrbyfloat`: reply `$4\r\n0.00\r\n` vs expected `$5\r\n0.001\r\n` — increment lost (SpockComparisonFailure, IGroupTest.groovy:216) | `8eea029f` | IGroupTest 18/18, DGroupTest 23/23, RESPReplyTest 4/4 (contract lines updated to valueOf inputs) | `DoubleReply.toRedisFormat()` all lines + both branches of the negative-scale guard covered; DGroup.java:404 covered |
| 79.2 | `test incrby`: `ErrorReply('increment or decrement would overflow')` for the legal `incrby lm -9223372036854775808` (SpockComparisonFailure, IGroupTest.groovy:198) | `adf61d28` | IGroupTest 18/18 (incl. wraparound + control cases), DGroupTest 23/23 (DECR* semantics preserved through the loader extraction) | `DGroup.incrBy` 14/14 lines, 0 partial; addition-shaped guards both branches covered |
| 79.3 | `test ingest`: stored value `{"*":null}` for the documented `value_fields=*` (SpockComparisonFailure) | `366c5052` | `test ingest` green (star / name-mismatch projection / csv order-projection cases) | json + csv shortcut blocks: all lines + all branches covered (allColumnsInOrder / allFieldsRequested true+false) |
| 79.9 | `test info`: `info SERVER` → 6 sections (`# Clients` present in raw bytes) vs expected 1 | `10be2caf` | `test info` green (server/SERVER/MEMORY/KEYSPACE/multi-arg + no-arg guard) | both `toLowerCase` call sites covered; keyspace compare both branches |
| 79.10 | `test info`: `info foo` → 1044 bytes of defaults vs expected empty | `2d650b14` | `test info` green (foo/all/everything/default/server-foo/foo-bar/cpu-all) | `allSectionsWithKeyspace` covered; single-arg + multi-arg `all` keyword branches covered; no-arg reply unchanged (guard case) |
| 79.5 | `test ingest`: `value_fields_indexes=-1,1` → raw `ArrayIndexOutOfBoundsException` via AsyncReply vs expected descriptive upfront `ErrorReply` | `da05d94b` | `test ingest` green (negative key/value index, per-line upper bound, no partial write) | negative-check lines 249-254 all branches; per-line upper-bound check both branches |
| 79.4 | `test ingest`: documented glob `globtest*.json` → `ERR ... not match any file` (regex `test*` semantics) | `961283c5` | `test ingest` green (glob match + stored value, invalid pattern → ErrorReply, no-match message) | `getPathMatcher` + invalid-pattern catch + `matches` line all covered |
| 79.6 | No deterministic external observable (Wal.put only skips `putVToFile` mid-loop; the per-file reset rewrites both ways — post-hoc state identical). Per the reviewer checklist ("static may suffice"): inspection + multi-file end-to-end pin `file_pattern=*.csv` → `put: 102` in one execute + JaCoCo on the re-arm line | `6aed7708` | `test ingest` green (102 puts across 2 files, keys from both present) | re-arm line `oneSlot.setBulkLoad(true)` (IGroup.java:329) covered; per-file reset lines already covered |
| 79.7 | `test ingest_sst`: reopen after failed slot worker → `RocksDBException: lock hold by current process ... /LOCK` (the leaked handle holds the lock) | `cd819dd7` | `test ingest_sst` green (failure path + reopen succeeds; happy path unchanged) | try-with-resources `Options` + catch path covered; failure-path `whenComplete` close: `e != null` both branches covered |
| 79.8 | `test incrby`: `incrby s1 +5` → `IntegerReply(5)` vs expected `NOT_INTEGER`; `test incrbyfloat`: `1.5d` accepted | `e596fe5e` | IGroupTest 20/20 (+ direct helper contract test `test redis literal helpers`), DGroupTest 23/23, RDBParserTest green (+ dedicated `test is redis integer string` contract test for the newly-public helper, all branches), HGroupTest 57/57 (hash fence) | both literal guards covered, both branches each; accepted-set preserved (`+1.5`, `.5`, `05`, `-0`, `1e3`) |

Scope fences respected:

- 79.1 did not touch the hash module — `HGroupTest` 57/57 after the change; `doubleScale` stays a
  (now display-unused) config knob, arithmetic no longer reads it.
- 79.2 kept `DECR*` on the subtraction-shaped guard; the `DECRBY Long.MIN_VALUE` divergence is **not**
  fixed here (follow-up in DGroup/string module, per *Additional observations*).
- 79.3/79.5 repaired the count-equality shortcut on **both** json and csv paths.
- 79.10 kept the no-argument reply shape (7 sections incl. keyspace) — pinned by a guard case.

Storage-representation decision for 79.1 (deliberate): `INCRBYFLOAT` keeps storing
`SP_TYPE_NUM_DOUBLE`, but the arithmetic is full-precision `BigDecimal.valueOf` at every boundary, so
decimal-exact accumulation is preserved across round-trips (`0.001` ×3 → `0.003` stored and replied);
reply formatting is `stripTrailingZeros().toPlainString()`, mirroring `HGroup.formatFloatValue` and
upstream `ld2string` LD_STR_HUMAN (`%.17Lf` + zero-strip).

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 79.1 | INCRBYFLOAT doubleScale=2 quantization (deferred from 78.10) | Medium | Correctness / Redis compat | CONFIRMED (reproduced) | fixed — `8eea029f` |
| 79.2 | INCRBY Long.MIN_VALUE delta negation wraps | Medium | Correctness / Redis compat | CONFIRMED (reproduced) — scoping note corrected (the `DECRBY` twin has its own `DGroup` bug, see *Additional observations*) | fixed — `adf61d28` |
| 79.3 | ingest json `value_fields=*` stores `{"*":null}`; count-equality shortcut | Medium | Correctness | CONFIRMED (reproduced) | fixed — `366c5052` |
| 79.4 | ingest file_pattern regex vs documented glob; raw PatternSyntaxException | Low | Correctness / Edge case | CONFIRMED (reproduced) | fixed — `961283c5` |
| 79.5 | ingest unvalidated indexes → raw AIOOBE + partial write | Low | Edge case | CONFIRMED (reproduced) | fixed — `da05d94b` |
| 79.6 | ingest bulk-load mode lost after first file | Low | Performance | CONFIRMED (inspection) | fixed — `6aed7708` |
| 79.7 | ingest_sst leaks RocksDB Options / db on failure | Low | Resource leak | CONFIRMED (inspection) | fixed — `cd819dd7` |
| 79.8 | INCRBY/INCRBYFLOAT accept Java-only literals | Low | Redis compat | CONFIRMED (reproduced) | fixed — `e596fe5e` |
| 79.9 | INFO section matching case-sensitive | Low | Redis compat | CONFIRMED (reproduced) | fixed — `10be2caf` |
| 79.10 | INFO unknown-section reply vs Redis 7.2 empty bulk (bug_60 premise conflict) | Low | Redis compat | CONFIRMED (reproduced) — adjudicated: option (a), match Redis 7.2 | fixed — `2d650b14` |
