# Bug 79 — IGroup Commands Review (Round 1)

**Module:** `io.velo.command.IGroup` (INCR / INCRBY / INCRBYFLOAT / INFO / INGEST / INGEST_SST) plus its
delegation targets (`DGroup.decrBy()`, `InfoCommand.groovy`, `DoubleReply`, the ingest write path)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 10 findings, none verified yet
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
**Status:** `OPEN — not verified`

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

Live Redis 7.2.11 vs Velo (by inspection, JDK repro in the log):

| command sequence | Redis 7.2.11 | Velo (doubleScale=2) |
|---|---|---|
| `INCRBYFLOAT k 0.001` ×3 | `0.001` → `0.002` → `0.003` | `0.00` → `0.00` → `0.00` — increment **permanently lost**, also on disk |
| `SET k 10.50` + `INCRBYFLOAT k 0.1` | `10.6` | `10.60` (trailing zero kept) |
| `INCRBYFLOAT k 0.1` on empty key | `0.1` | `0.10` |

Any sub-cent float counter (rates, sensor readings, micro-transactions) is silently corrupted — the
*stored* value is `newValue.doubleValue()` of the scale-2 result, so the loss is persistent, not just
display. `DECRBYFLOAT` (Velo-only command) inherits the same behavior through the identical path.

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
**Status:** `OPEN — not verified`

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

JDK repro + live Redis 7.2.11 (log §1/§2):

| case | Redis 7.2.11 | Velo (by inspection) |
|---|---|---|
| `SET k 0` + `INCRBY k -9223372036854775808` | `:-9223372036854775808` (representable) | `ERR increment or decrement would overflow` — legal command rejected |
| `SET k -5` + `INCRBY k -9223372036854775808` | `ERR increment or decrement would overflow`, value stays `-5` | guard misses (`-5 > -1` false) → `longValue - by` wraps → **stores and replies `9223372036854775803`** |

bug_74.4 fixed the wrap for `DECR*`/plain `INCR`/normal `INCRBY` deltas; only the `Long.MIN_VALUE` *delta*
path is broken, and only via `IGroup`'s negation (`DGroup.decrby` does not negate, so `DECRBY
k -9223372036854775808` is correct — the bug is in `IGroup.java:153`).

### Suggested fix direction

Stop negating in `IGroup`: plumb the true increment (e.g. an `incrBy(long)` variant, or an explicit
direction flag), and guard with the addition-side check `by > 0 && longValue > Long.MAX_VALUE - by` /
`by < 0 && longValue < Long.MIN_VALUE - by` (both safe against wrap), or use `Math.addExact` with
`ArithmeticException` → `INCR_DECR_OVERFLOW`. Keep `DGroup`'s existing guard untouched.

---

## Finding 3 (Bug 79.3): `ingest` json `value_fields=*` (documented) stores `{"*":null}` for every key; the count-equality shortcut ignores field names

**Severity:** Medium
**Class:** Correctness (Velo-specific command)
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified` (explicitly flagged for reviewer adjudication)

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

**Method:** <Spock temp spec | live Redis 7.2 differential | JDK repro | static inspection>

| Finding | Verdict | Evidence |
|---|---|---|
| 79.1 | | |
| 79.2 | | |
| 79.3 | | |
| 79.4 | | |
| 79.5 | | |
| 79.6 | | |
| 79.7 | | |
| 79.8 | | |
| 79.9 | | |
| 79.10 | | |

### Additional observations
### Recommended fix order (for the fix agent)

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 79.1 | | | | |

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 79.1 | INCRBYFLOAT doubleScale=2 quantization (deferred from 78.10) | Medium | Correctness / Redis compat | | |
| 79.2 | INCRBY Long.MIN_VALUE delta negation wraps | Medium | Correctness / Redis compat | | |
| 79.3 | ingest json `value_fields=*` stores `{"*":null}`; count-equality shortcut | Medium | Correctness | | |
| 79.4 | ingest file_pattern regex vs documented glob; raw PatternSyntaxException | Low | Correctness / Edge case | | |
| 79.5 | ingest unvalidated indexes → raw AIOOBE + partial write | Low | Edge case | | |
| 79.6 | ingest bulk-load mode lost after first file | Low | Performance | | |
| 79.7 | ingest_sst leaks RocksDB Options / db on failure | Low | Resource leak | | |
| 79.8 | INCRBY/INCRBYFLOAT accept Java-only literals | Low | Redis compat | | |
| 79.9 | INFO section matching case-sensitive | Low | Redis compat | | |
| 79.10 | INFO unknown-section reply vs Redis 7.2 empty bulk (bug_60 premise conflict) | Low | Redis compat | | |
