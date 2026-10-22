# Bug 74 — DGroup Commands Review (Round 1)

**Module:** `io.velo.command.DGroup` (DBSIZE, DEBUG, DECR, DECRBY, DECRBYFLOAT, DEL, DUMP)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** round 1 complete — 9/9 findings CONFIRMED (reproduced) and fixed (9 commits), re-review pending
**Review branch:** `review/dgroup`
**Reviewed commit:** `798ab06e6870217d758a1c670cfb38483054ff3b` (branch `main`, working tree clean for the reviewed files)

> Round 1 — no prior round for this module, so there is no delta declaration.
> Prior coverage of the same class is limited to `bug_39_string_commands_review_round_1.md` (DBSIZE arity check — closed as *cleanup only / not a bug*) and
> `bug_40_hash_module_review_round_1.md` (TTL cache of split-storage hash fields — a different code path, `HGroup.hincrby`, not `DGroup.decrBy`).
> Items closed there are **not** re-reported here (see *Notes / non-findings*).

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/DGroup.java` — static command registrations `DGroup()` (31–88), `parseSlots()` (107–156), `handle()` (163–222),
  `wrapEncodingType()` (224–226), `debug()` (228–271), `del()` (273–338), `dbsize()` (340–350), `decrBy()` (352–403), `isUseHH()` (405–415),
  `dumpBytes()` (417–475), `dump()` (479–500) — full file read (501 lines)
- `src/main/java/io/velo/BaseCommand.java` — `resetContext()` (150–157), `from()` (362–384), `slot()`/`slot(String)`/`slot(byte[])` (620–672),
  `getCv()` (734–776), `getValueBytesByCv()` (791–829), `setNumber(Number, SlotWithKeyHash)` (874–876),
  `setNumber(Number, SlotWithKeyHash, long)` (883–942), `setCv()` (950–977), `set(...)` (994–1200), `putToOneSlot()` (1207–1225),
  `remove()` (1233–1243), `addToSlotWithKeyHashList()` (75–84)
- `src/main/java/io/velo/CompressedValue.java` — SP type constants (20–110), `isTypeNumber()` (244–259), `isTypeDouble()` (263–279),
  `numberValue()` (359–367), `isCompressed()` (587–589)
- `src/main/java/io/velo/command/IGroup.java` — `parseSlots()` (100–121), `handle()` incl. `incr`/`incrby`/`incrbyfloat` (123–170) (delegates to `DGroup.decrBy`)
- `src/main/java/io/velo/command/HGroup.java` — `hgetall()` (1098–1135), `hexpire()` (841–1000), `hexpire2()` (1002–1035),
  `hget()` (1040–1078), `hmset()` (1666–1730), `getRedisHashKeys()` (428–437), `saveRedisHashKeys()` (439–455),
  `hincrby()` cached-TTL handling (1421–1489)
- `src/main/java/io/velo/type/RedisHashKeys.java` — `getCachedExpireAt()`/`putCachedExpireAt()`/`isLiveByCache()`/`liveFieldsByCache()` (44–108),
  `keysKey()` (106–113), `fieldKey()` (115–117)
- `src/main/java/io/velo/type/RedisHH.java` — `getMap()` (32–35), `put(key,value,expireAt)` (52–73), `iterate()` (259–275), `decode()` (222–245)
- `src/main/java/io/velo/rdb/RDBParser.java` — `writeVersionAndCrc()` (412–420), `dumpString()` (433–441), `dumpSet()` (448–460),
  `dumpHash()` (467–480), `dumpList()` (487–500), `dumpZSet()` (506–520), `writeRdbString()` (520–560), `RDB_VERSION` (25)
- `src/main/java/io/velo/persist/OneSlot.java` — `put()` (1965–2022 and 2022–2100: `Wal.V` is created with `cv.getExpireAt()`), `remove()` path
- `src/main/java/io/velo/RequestHandler.java` — command dispatch and exception mapping (470–580, `catch (Exception e) → new ErrorReply(e.getMessage())` at 576–580)
- `src/main/java/io/velo/MultiWorkerServer.java` — cross-worker detection (554–570), config loading (877–930), `localPersist.initSlots(...)` (1689–1694)
- `src/main/java/io/velo/command/KGroup.java` — `keys()` (72–110) (only prefix patterns supported)
- `src/main/java/io/velo/reply/ErrorReply.java` — `FORMAT` (12), `KEY_TOO_LONG` (34), `NOT_INTEGER` (67), `NOT_FLOAT` (70), `DUMP_TYPE_NOT_SUPPORT` (130)
- `src/test/groovy/io/velo/command/DGroupTest.groovy` — full file read (536 lines): `test parse slot - single key` (24–40),
  `test parse slot - multi key and edge cases` (42–61), `test handle - format errors` (63–81), `test handle - decr decrby decrbyfloat` (83–139),
  `test debug` (141–230), `test del` (232–290), `test dbsize` (292–320), `test decr by` (322–395), `test dump` (397–535)

Reference docs (ground truth):

- Redis 7.2 `DUMP` — https://redis.io/docs/latest/commands/dump/ (fetched 2026-10-22): "Values are encoded in the same format used by RDB… The returned value can be synthesized back into a Redis key using the RESTORE command."
- Redis 7.2 `commands.def` (source of truth for the command table) — https://raw.githubusercontent.com/redis/redis/7.2/src/commands.def (fetched 2026-10-22):
  `DECR` (arity 2), `DECRBY` (arity 3) exist; **`DECRBYFLOAT` does not exist in Redis 7.2** (Velo-only extension).
- Live Redis **7.2.11** (`redis-server v=7.2.11 sha=d4c381df`) started locally on ports 6398/6399 for differential testing.
- Velo jar `build/libs/velo-1.0.0.jar` (built 2026-10-19 22:06, `DGroup.java` unchanged since 2026-07-02) started locally on port 7391
  (HH mode, default) and on port 7392 (`persist.isHashSaveMemberTogether=false`, split mode) for differential testing.
- Raw evidence log: `doc/bug_reviews/logs/20261022_151500_dgroup_differential_redis72_bug_74.log`
- Repo design docs: `doc/design/04_command_processing_design.md`, `doc/design/19_command_registry_design.md` (command registration/metadata),
  `doc/design/14_rdb_format_design.md` (RDB/DUMP format)

---

## Finding 74.1: `DECR`/`DECRBY`/`DECRBYFLOAT` drop the key's TTL (key becomes permanent)

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + author's live differential

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:392-402 — decrBy()
        if (isByFloat) {
            var newValue = BigDecimal.valueOf(doubleValue).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP)
                    .subtract(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));

            setNumber(newValue.doubleValue(), slotWithKeyHash);   // <-- no expireAt
            return new DoubleReply(newValue);
        } else {
            long newValue = longValue - by;
            setNumber(newValue, slotWithKeyHash);                   // <-- no expireAt
            return new IntegerReply(newValue);
        }
```

```java
// src/main/java/io/velo/BaseCommand.java:874-876 / 883-942 — setNumber()
    public void setNumber(Number value, SlotWithKeyHash slotWithKeyHashReuse) {
        setNumber(value, slotWithKeyHashReuse, CompressedValue.NO_EXPIRE);   // <-- TTL dropped here
    }
```

```java
// src/main/java/io/velo/persist/OneSlot.java:2022+ — put(): the new TTL of the record comes from the new cv
            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(), key, cvEncoded, isFromMerge);
```

### Root cause

`DGroup.decrBy()` reads the existing `CompressedValue` (`cv`) but never reuses `cv.getExpireAt()`. The new value is written through
`setNumber(value, slotWithKeyHash)` → `setNumber(value, slot, CompressedValue.NO_EXPIRE)` → `set(...)` → a **new** `CompressedValue` whose
`expireAt` is `NO_EXPIRE` → `OneSlot.put(...)` stores that new `expireAt` in the WAL record. The previous TTL is overwritten with "no expiry".

Redis keeps the TTL on `INCR`/`DECR`/`INCRBY`/`DECRBY` (the key is updated in place, the expire field is untouched).

### Trigger / impact

Trigger: `SET k 100 EX 100` then `DECR k`.

| server | `DECR k` | `TTL k` afterwards |
|---|---|---|
| Velo 7391 | `:99` | `:-1`  ← TTL lost, key is now permanent |
| Redis 7.2.11 | `:99` | `:100` ← TTL preserved |

Blast radius: every key whose value is decremented/incremented loses its expiration — the key never expires (unbounded memory, stale cache
entries that should have been invalidated). `IGroup.incr`/`incrby`/`incrbyfloat` (IGroup.java:125–170) delegate to the very same
`DGroup.decrBy()`, so `INCR`/`INCRBY`/`INCRBYFLOAT` inherit the defect.

### Suggested fix direction

Capture the existing expiry before the write and pass it through, e.g. read `cv != null ? cv.getExpireAt() : CompressedValue.NO_EXPIRE`
and call `setNumber(value, slotWithKeyHash, expireAt)`. Keep `NO_EXPIRE` for the "key did not exist" case. Do **not** touch the
`RedisHashKeys` cached-TTL logic of `HGroup.hincrby` (bug 40) — that path is separate.

---

## Finding 74.2: `DECR`/`DECRBY` silently truncate a float value instead of rejecting it

**Severity:** Medium
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + author's live differential

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:367-372 — decrBy()
        if (cv != null && cv.isTypeNumber()) {
            if (isByFloat) {
                doubleValue = cv.numberValue().doubleValue();
            } else {
                longValue = cv.numberValue().longValue();   // <-- truncates a stored double
            }
        }
```

### Root cause

`cv.isTypeNumber()` is true for `SP_TYPE_NUM_DOUBLE` as well. On the integer path (`decr`, `decrby`) the code calls
`cv.numberValue().longValue()`, which silently truncates `1.5` → `1`. There is no `cv.isTypeDouble()` guard on the integer path, so the
"not an integer" contract of Redis is never enforced for values stored as doubles. The truncated value is then written back with
`setNumber(long, ...)`, so the stored value changes type as well.

### Trigger / impact

Trigger: `SET k 1.5` then `DECR k`.

| server | `DECR k` | `GET k` afterwards |
|---|---|---|
| Velo 7391 | `:0` | `"0"`  ← value silently destroyed |
| Redis 7.2.11 | `-ERR value is not an integer or out of range` | `"1.5"` ← untouched |

Blast radius: silent data corruption on a legal-looking command; a client that computes a counter from a float value gets a wrong result and
the original value is lost.

**Extension agreed with the reviewer (author, round 1):** the same truncation also destroys *non-finite* stored doubles. `SET` accepts
`Infinity`/`NaN`/`-Infinity` as a string and `BaseCommand.set()` (BaseCommand.java:1098–1104) stores them through its
`Double.parseDouble` fallback, so the key holds a `SP_TYPE_NUM_DOUBLE` cv with a non-finite value. Verified on Velo 7391:

```
SET k Infinity -> +OK      GET k -> "Infinity"     DECR k -> :9223372036854775806   (longValue = Long.MAX_VALUE)
SET k NaN      -> +OK      GET k -> "NaN"          DECR k -> :-1                    (longValue = 0)
```

Redis rejects all of these with `ERR value is not an integer or out of range` because the *string* is not an integer. The fix below must
therefore also reject non-finite doubles, not only fractional ones.

### Suggested fix direction

On the integer path, reject a double-typed cv explicitly: `if (!isByFloat && cv.isTypeDouble()) return NOT_INTEGER;`
(the same check is needed for the string branch: Redis rejects `"1.5"` with the same error — that part already works because
`Long.parseLong("1.5")` throws). Extend the guard to non-finite doubles (`!Double.isFinite(value)`) so that `Infinity`/`NaN` are rejected
as well instead of being truncated to `Long.MAX_VALUE`/`0`.

---

## Finding 74.3: `DECRBY` delta is parsed with `Integer.parseInt` — only int32 is accepted

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + author's live differential

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:181-192 — handle()
        if ("decrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            try {
                int by = Integer.parseInt(new String(data[2]));   // <-- int32 only
                return decrBy(by, 0);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }
```

### Root cause

Redis `DECRBY` takes a 64-bit signed decrement (`getLongLongFromObjectOrReply`). Velo parses the argument with `Integer.parseInt`, so any
delta outside `[-2^31, 2^31-1]` is rejected as "not integer", although the delta itself is a perfectly valid int64.

### Trigger / impact

Trigger: `SET k 10` then `DECRBY k 3000000000`.

| server | `DECRBY k 3000000000` |
|---|---|
| Velo 7391 | `-ERR not integer` |
| Redis 7.2.11 | `:-2999999990` |

Blast radius: legal commands are rejected; a client computing a counter with a large delta gets an error instead of a result.
`IGroup.incrby` (IGroup.java:139–146) has the identical `Integer.parseInt` limitation.

### Suggested fix direction

Parse with `Long.parseLong` and widen `DGroup.decrBy(int by, double byFloat)` to `decrBy(long by, double byFloat)`; keep the
`NOT_INTEGER` reply for genuinely non-numeric arguments.

---

## Finding 74.4: int64 overflow wraps around silently

**Severity:** Medium
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + author's live differential

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:398-402 — decrBy()
        } else {
            long newValue = longValue - by;    // <-- no overflow check
            setNumber(newValue, slotWithKeyHash);
            return new IntegerReply(newValue);
        }
```

### Root cause

The subtraction is plain Java `long` arithmetic, which wraps on overflow. Redis explicitly detects the overflow and answers
`ERR increment or decrement would overflow`, leaving the value untouched.

### Trigger / impact

Trigger: `SET k -9223372036854775808` then `DECR k`.

| server | `DECR k` | `GET k` afterwards |
|---|---|---|
| Velo 7391 | `:9223372036854775807` ← wrapped | `"9223372036854775807"` |
| Redis 7.2.11 | `-ERR increment or decrement would overflow` | `"-9223372036854775808"` |

Blast radius: the stored value silently becomes the opposite extreme; downstream counters/balances become wrong without any error.

### Suggested fix direction

Before the subtraction, detect underflow/overflow (Redis' rule: the result must stay within `[LLONG_MIN, LLONG_MAX]`) and return an
error reply instead of writing the wrapped value.

---

## Finding 74.5: `DECRBYFLOAT`/`INCRBYFLOAT` with a zero delta truncates a float value into an integer

**Severity:** Medium
**Class:** Correctness
**Status:** `CONFIRMED (reproduced)` — temp Spock spec (incl. `INCRBYFLOAT` via IGroup)

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:352-360 — decrBy()
    Reply decrBy(int by, double byFloat) {
        ...
        boolean isByFloat = byFloat != 0;      // <-- mode derived from the *value* of the delta
        final var NOT_NUMBER_REPLY = isByFloat ? ErrorReply.NOT_FLOAT : ErrorReply.NOT_INTEGER;
```

```java
// src/main/java/io/velo/command/IGroup.java:156-170 — incrbyfloat delegates with -by
            double by;
            try {
                by = Double.parseDouble(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }
            ...
            return dGroup.decrBy(0, -by);      // -0.0 == 0.0 in Java → also the integer path
```

### Root cause

The float/integer mode of `decrBy()` is inferred from `byFloat != 0` instead of being passed explicitly. A zero delta (`0.0`, and `-0.0`
because `-0.0 == 0.0` in Java) makes `isByFloat` false, so the command runs the **integer** path: the stored double is truncated by
`cv.numberValue().longValue()` and written back as an integer. `DECRBYFLOAT` with a zero delta must still be a float operation.

### Trigger / impact

Trigger: `SET k 10.005` then `DECRBYFLOAT k 0`.

| server | `DECRBYFLOAT k 0` | `GET k` afterwards |
|---|---|---|
| Velo 7391 | `:10` (an **integer** reply) | `"10"` ← the fractional part is destroyed |
| Redis 7.2 | n/a — `DECRBYFLOAT` does not exist in Redis 7.2 (see Scope) | — |

Blast radius: any client that periodically applies a zero delta (a common "refresh" pattern) silently truncates float values to integers.
`INCRBYFLOAT k 0` has the same defect through `IGroup` (`-0.0 == 0.0`).

### Suggested fix direction

Stop deriving the mode from the delta: change the signature to `decrBy(long by, double byFloat, boolean isByFloat)` (or add a dedicated
`decrByFloat`/`incrByFloat` method) so that `DECRBYFLOAT`/`INCRBYFLOAT` always take the float path and always answer with a double reply.

---

## Finding 74.6: `DUMP` corrupts numeric-looking strings (RESTORE yields a different value)

**Severity:** High
**Class:** Correctness / Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — temp Spock spec (`RDBParser.dumpString` payload hex) + author's end-to-end restore into real Redis 7.2.11

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:479-500 — dump()
    private Reply dump() {
        ...
        var dumpBytes = dumpBytes(cv, slotWithKeyHash);
        ...
        return new BulkReply(dumpBytes);
    }

// src/main/java/io/velo/command/DGroup.java:417-420 — dumpBytes(), string branch
        if (cv.isTypeString()) {
            var valueBytes = getValueBytesByCv(cv, slotWithKeyHash);
            return RDBParser.dumpString(valueBytes);
        }
```

```java
// src/main/java/io/velo/rdb/RDBParser.java:520-541 — writeRdbString()
    private static void writeRdbString(Slice slice, byte[] data) {
        if (data.length <= 11) {
            try {
                var str = new String(data);
                var value = Long.parseLong(str);      // <-- accepts "0123", "+123", "0007"
                if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
                    slice.writeByte((byte) (0xC0));    // RDB_ENC_INT8 → the string becomes an integer
                    slice.writeByte((byte) value);
                    return;
                }
                ...
```

### Root cause

Redis only uses the compact integer encoding inside an RDB payload when **Redis' own `string2ll()`** accepts the string. `string2ll()`
rejects a leading `0` followed by more digits and rejects a leading `+`. Java's `Long.parseLong()` accepts both, so Velo emits the
`RDB_ENC_INT8` encoding for strings that Redis would keep as raw strings. On `RESTORE`, Redis decodes `RDB_ENC_INT8` as the *integer*
value — the string content is lost.

### Trigger / impact

Trigger: `SET k 0123` then `DUMP k` (and the same for `"+123"`, `"007"`, `"-0"`, …).

| server | `DUMP k` payload (hex) |
|---|---|
| Velo 7391 | `00 c0 7b 0b 00 213dee92fde58e30` → `RDB_TYPE_STRING` + `RDB_ENC_INT8 123` |
| Redis 7.2.11 | `00 04 30313233 0b 00 633bb1e163a48dab` → `RDB_TYPE_STRING` + raw `"0123"` |

**Offending vs. legal forms (author, round 1 — measured on the real Redis 7.2.11, `DUMP` after `SET`):**

| stored string | Velo payload | Redis 7.2.11 payload | verdict |
|---|---|---|---|
| `"0123"` | `00 c0 7b …` (int8 123) | `00 04 "0123" …` (raw) | **corrupting** |
| `"007"` | `00 c0 07 …` (int8 7) | `00 03 "007" …` (raw) | **corrupting** |
| `"+123"` | `00 c0 7b …` (int8 123) | `00 04 "+123" …` (raw) | **corrupting** |
| `"-0"` | `00 c0 00 …` (int8 0) | `00 02 "-0" …` (raw) | **corrupting** |
| `"-5"` | `00 c0 fb …` (int8 -5) | `00 c0 fb …` (int8 -5) | ok (Redis-legal integer) |
| `"0"` | `00 c0 00 …` (int8 0) | `00 c0 00 …` (int8 0) | ok (Redis-legal integer) |
| `"hello"` | `00 05 "hello" …` (raw) | `00 05 "hello" …` (raw) | ok |

So the rule to implement is Redis' `string2ll()`: no leading `+`, no leading `0` followed by more digits, and `-0` is **not** an integer
(Redis keeps it as a raw string).

End-to-end proof (Velo payload restored into the real Redis 7.2.11 on port 6399):

```
   velo dump: 00c07b0b00213dee92fde58e30
   restore -> b'+OK\r\n'
   read   -> b'$3\r\n123\r\n'      <-- "0123" became "123"
```

(For comparison, all other value types round-trip correctly: string `"hello"`, list, set, zset and hash payloads produced by Velo were
restored into real Redis 7.2.11 and read back correctly — see the log file.)

Blast radius: `DUMP`/`RESTORE` based tooling (backup, migration, `MIGRATE`-style copies, cross-engine transfers) silently *changes* the value
of any key whose string content looks numeric but is not a canonical integer literal.

### Suggested fix direction

Use a Redis-compatible integer test before emitting the RDB integer encoding: reject a leading `+`, reject a leading `0` when more digits
follow, and reject an empty string. A helper such as `isRedisIntegerString(byte[])` should live next to the RDB writer so that both the
writer and any reader share the same rule.

---

## Finding 74.7: `DEL` key-length validation is missing in the cross-worker path (and partially applies before erroring)

**Severity:** Low
**Class:** Edge case / Consistency
**Status:** `CONFIRMED (reproduced)` — temp Spock spec reproduced both halves (local partial delete + cross-worker skip)

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:273-294 — del(), local path
            for (int i = 1, j = 0; i < data.length; i++, j++) {
                var keyBytes = data[i];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;      // <-- checked *after* previous keys were removed
                }
                ...
```

```java
// src/main/java/io/velo/command/DGroup.java:296-313 — del(), cross-worker path
        ArrayList<Promise<ArrayList<Boolean>>> promises = new ArrayList<>();
        // group by slot
        var groupBySlot = slotWithKeyHashListParsed.stream().collect(Collectors.groupingBy(SlotWithKeyHash::slot));
        for (var entry : groupBySlot.entrySet()) {
            ...
                var isRemoved = remove(slotWithKeyHash);      // <-- no key-length check at all
```

### Root cause

The `KEY_MAX_LENGTH` validation exists only in the single-worker branch. When the command is marked as cross-request-worker
(`MultiWorkerServer.java:554–570` sets the flag when the keys of one command land in slots owned by different net workers), the same
`DEL` runs the fan-out branch, which never validates the key length. In addition, in the local branch the check happens inside the removal
loop, so the keys that were processed before the offending key are already deleted when the error is returned.

### Trigger / impact

- `netWorkers > 1` and a `DEL` whose keys span two workers, one key longer than `CompressedValue.KEY_MAX_LENGTH` (256 bytes):
  Velo answers `0` (or a smaller count) instead of `ERR key too long` — the same command with the same keys behaves differently depending on
  which worker happens to own the slots.
- Single worker: `DEL <normal-key> <257-byte-key>` → the normal key is deleted **and** the client receives `ERR key too long` (partial effect on
  an error path).

### Suggested fix direction

Validate all key lengths once, before any removal, and do it in a shared place so both branches behave identically (e.g. a small helper
called at the top of `del()`).

---

## Finding 74.8: `DECRBYFLOAT` with an overflowing literal leaks a Java exception message

**Severity:** Low
**Class:** Edge case
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + JDK repro; leaking input set refined (see Review Feedback)

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:392-397 — decrBy(), float path
        if (isByFloat) {
            var newValue = BigDecimal.valueOf(doubleValue).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP)
                    .subtract(BigDecimal.valueOf(byFloat).setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP));
```

### Root cause

`Double.parseDouble("1e400")` returns `Infinity` **without** throwing, so the argument check at DGroup.java:199–204 accepts it.
`BigDecimal.valueOf(Infinity)` then throws `NumberFormatException`, which is not caught inside the command; the generic handler in
`RequestHandler.java:576–580` turns it into `new ErrorReply(e.getMessage())`, i.e. the raw Java message is sent to the client.

### Trigger / impact

Trigger: `DECRBYFLOAT k 1e400`.

```
Velo 7391: -ERR Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
server log: java.lang.NumberFormatException: Character I is neither a decimal digit number, ...
             at java.base/java.math.BigDecimal.<init>(BigDecimal.java:608)
             at io.velo.command.DGroup.decrBy(DGroup.java:393)
```

**Refined leaking input set (accepted from the reviewer, re-measured by the author on Velo 7391):**

| input | `Double.parseDouble` | result |
|---|---|---|
| `inf` / `nan` (lowercase) | throws `NumberFormatException` | rejected correctly with `ERR not float` |
| `NaN` | `NaN`, no throw | **leaks** `ERR Character N is neither a decimal digit number, …` |
| `Infinity` / `-Infinity` | `±Infinity`, no throw | **leaks** `ERR Character I is neither a decimal digit number, …` |
| `1e400` | `Infinity`, no throw | **leaks** `ERR Character I is neither a decimal digit number, …` |

**Stored-value path (accepted from the reviewer, verified by the author):** the same raw `NumberFormatException` escapes even with a
*finite* delta, because a non-finite double can be stored through `SET` (BaseCommand.java:1098–1104 `Double.parseDouble` fallback):

```
SET k Infinity -> +OK   GET k -> "Infinity"   DECRBYFLOAT k 1 -> -ERR Character I is neither a decimal digit number, ...
SET k NaN      -> +OK   GET k -> "NaN"        DECRBYFLOAT k 1 -> -ERR Character N is neither a decimal digit number, ...
```

Blast radius: an internal implementation detail is exposed to clients, the error is not classifiable, and the failure is logged at ERROR level
as a server-side exception for a purely client-side input error.

### Suggested fix direction

Reject non-finite deltas explicitly (`Double.isFinite(by)`) and answer `ErrorReply.NOT_FLOAT`; the same guard should be applied to the value
read from the key (`doubleValue`), because a stored `Infinity`/`NaN` reaches `BigDecimal.valueOf` as well.

---

## Finding 74.9: `DEBUG` answers a null reply for unknown subcommands and for a missing key

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — temp Spock spec + author's live differential

### Cited files / lines

```java
// src/main/java/io/velo/command/DGroup.java:228-271 — debug()
    private Reply debug() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }
        var subCmd = new String(data[1]).toLowerCase();
        if ("object".equals(subCmd)) {
            ...
            if (cv == null) {
                return NilReply.INSTANCE;          // <-- Redis answers "ERR no such key"
            }
            ...
        } else if ("log".equals(subCmd)) {
            ...
        }
        return NilReply.INSTANCE;                  // <-- unknown subcommand → null reply
    }
```

### Root cause

Every `DEBUG` subcommand other than `object` and `log` falls through to `return NilReply.INSTANCE`, i.e. the client receives a RESP null
bulk reply instead of an error. Likewise `DEBUG OBJECT <missing key>` returns null instead of an error.

### Trigger / impact

| command | Velo 7391 | Redis 7.2.11 (`--enable-debug-command yes`) |
|---|---|---|
| `DEBUG xyzzy` | null reply | `-ERR unknown subcommand or wrong number of arguments for 'xyzzy'. Try DEBUG HELP.` |
| `DEBUG OBJECT nokey` | null reply | `-ERR no such key` |
| `DEBUG` (no subcommand) | `ERR format` | `-ERR wrong number of arguments for 'debug' command` |

Blast radius: clients (monitoring scripts, `redis-cli` wrappers) cannot distinguish "unsupported" from "no result"; the null reply is also
inconsistent with the rest of the command surface, where unsupported input yields an error.

### Suggested fix direction

Return `ErrorReply` for unknown subcommands (mirroring Redis' wording is optional) and for `DEBUG OBJECT` on a missing key.

---

## Notes / non-findings

Checked and deliberately **not** reported (so later rounds do not re-investigate):

- `DBSIZE` arity check `data.length == 2` (DGroup.java:342–344) — already reviewed and closed as *cleanup only / not a bug* in
  `bug_39_string_commands_review_round_1.md` (finding 4). Not re-reported.
- `DUMP` of a hash in split mode (`persist.isHashSaveMemberTogether=false`) returns a null reply — this is **not** a DGroup defect: in split
  mode the hash has no value under its own name (`EXISTS h` → 0, `TYPE h` → `hash`), so `DUMP` is consistent with the rest of the split-mode
  behaviour. The split branch of `dumpBytes()` (DGroup.java:438–455) is therefore only reachable for mixed-mode data.
  Verified on the split-mode instance (port 7392): `HSET h3 f1 v1` → `TYPE h3` = `hash`, `EXISTS h3` = 0, `DUMP h3` = null.
- `DUMP` of a hash whose field expired (`HEXPIRE`) in HH mode returns only the live fields — correct (Redis 7.4 semantics). Verified:
  `HSET sh2 f1 v1 f2 v2` + `HEXPIRE sh2 1 FIELDS 1 f1` → after the TTL, `DUMP sh2` = `04 01 "f2" "v2" …` (only `f2`), and the payload restores
  correctly into real Redis 7.2.11.
- `DUMP` payload framing (type byte + payload + 2-byte RDB version + CRC64 over *object+version*) is byte-compatible with Redis 7.2.11 —
  verified by restoring Velo payloads for string/list/set/zset/hash into the real Redis (`restore -> +OK`, values read back identical).
- `DEL` counting semantics (duplicate keys counted once, missing keys not counted) match Redis (`DEL k k` → 1 in both).
- `DECR`/`DECRBY` on a key holding a non-string type: Velo answers `ERR not integer`, Redis answers `WRONGTYPE …`. Cosmetic difference in an
  error string; the value is not modified in either case — not reported (Velo-wide error-string convention).
- `DEBUG OBJECT` encoding labels (`hashtable` for a set, `embstr` for a number) are simplified compared to Redis (`intset`, `int`); the reply
  is informational only — not reported.
- `DECRBYFLOAT` is **not** a Redis 7.2 command (Redis 7.2 `commands.def` has `DECR`/`DECRBY` only; the local 7.2.11 server answers
  "unknown command 'decrbyfloat'"). Findings 74.5 and 74.8 are therefore judged against Velo's own contract, not against Redis.
  The command's registry metadata claims `"2.6.0"` (DGroup.java:71), which is misleading for a Velo-only extension — noted, not reported.
- `parseSlots()` (DGroup.java:107–156) argument-count guards are consistent with `handle()`; no key/slot mismatch was found.
- `BaseCommand.slot()` hash-tag handling was reviewed for this class (`{tag}` keys) and behaves as documented in `BaseCommand.tagHash()`.

## Reviewer checklist (for AI agent 2)

Environment used by the author (all commands below are reproducible):

- Velo: `cd /tmp/opencode/velorun && java -Xmx1g -XX:+UseZGC -jar <repo>/build/libs/velo-1.0.0.jar` with
  `net.listenAddresses=0.0.0.0:7391` (HH mode) and `persist.isHashSaveMemberTogether=false` on port 7392.
- Redis ground truth: `redis-server --port 6399 --daemonize yes --save '' --appendonly no` (v7.2.11).
- Author log: `doc/bug_reviews/logs/20261022_151500_dgroup_differential_redis72_bug_74.log`.

| Finding | Reproduction recipe | Expected assertion |
|---|---|---|
| 74.1 | Velo & Redis: `SET k 100 EX 100`; `DECR k`; `TTL k` | Velo `-1`, Redis `100` |
| 74.2 | Velo & Redis: `SET kf 1.5`; `DECR kf`; `GET kf` | Velo `:0` + `"0"`, Redis `-ERR value is not an integer or out of range` + `"1.5"` |
| 74.3 | Velo & Redis: `SET kb 10`; `DECRBY kb 3000000000` | Velo `-ERR not integer`, Redis `:-2999999990` |
| 74.4 | Velo & Redis: `SET k2 -9223372036854775808`; `DECR k2`; `GET k2` | Velo `:9223372036854775807`, Redis `-ERR increment or decrement would overflow` and the value unchanged |
| 74.5 | Velo: `SET k 10.005`; `DECRBYFLOAT k 0`; `GET k` | Velo `:10` + `"10"` (fraction lost); also check `INCRBYFLOAT k 0` |
| 74.6 | Velo: `SET k 0123`; `DUMP k` → hex `00c07b…`; restore that payload into Redis 6399 (`RESTORE r 0 <payload>`) then `GET r` | Velo payload must be `00 04 "0123" …`; as-is it restores as `"123"` |
| 74.7 | Static: `DGroup.del()` lines 273–294 vs 296–313; optionally run Velo with `netWorkers=2` and `DEL <key> <257-byte key>` | the cross-worker branch has no `KEY_MAX_LENGTH` check; the local branch deletes earlier keys before erroring |
| 74.8 | Velo: `DECRBYFLOAT k 1e400` | reply must not contain a Java `NumberFormatException` message |
| 74.9 | Velo & Redis (`--enable-debug-command yes`): `DEBUG xyzzy`, `DEBUG OBJECT nokey` | Velo returns a null reply, Redis returns errors |

## Review Feedback (AI agent 2, round 1)

**Method:** Every cited line range was re-read against the current working tree (all citations match). A temporary Spock spec
(`src/test/groovy/io/velo/command/Bug74VerifyTempSpec.groovy`, deleted after evidence capture) drove the commands through the real
`DGroup.handle()` / `IGroup.handle()` code paths with the `InMemoryGetSet` bypass, asserting the **current** (buggy) behavior for each
finding. A JDK 21 single-file repro checked the `Double.parseDouble` / `BigDecimal.valueOf` edge semantics used by finding 74.8.
The author's raw differential log (`doc/bug_reviews/logs/20261022_151500_dgroup_differential_redis72_bug_74.log`) was cross-checked and is
consistent with both the code and my spec results. Commands and output:

```
$ ./gradlew :test --tests "io.velo.command.Bug74VerifyTempSpec" -Dorg.gradle.jvmargs=-Xmx3g
BUILD SUCCESSFUL in 9s
# 16/16 test cases passed, 0 failures — build/test-results/test/TEST-io.velo.command.Bug74VerifyTempSpec.xml:
#   74.1 decr drops TTL                                              time=0.428
#   74.2 decr truncates a double value                               time=0.01
#   74.3 decrby rejects int64 delta                                  time=0.003
#   74.4 decr wraps on int64 underflow                               time=0.003
#   74.5 zero delta runs the integer path                            time=0.017
#   74.6 dumpString integer-encodes non-canonical integer strings    time=0.047  (6 data rows: 0123/007/+123/-0/hello/-5)
#   74.7 del local branch deletes earlier keys before key-too-long   time=0.002
#   74.7 del cross-worker branch skips key-length validation         time=0.16
#   74.8 decrbyfloat 1e400 leaks a java exception                    time=0.004
#   74.9 debug answers null instead of error                         time=0.003
#   74.5 sanity: DoubleReply for nonzero delta (control)             time=0.002
```

```
$ java ParseCheck.java   (JDK 21, /tmp/opencode)
parseDouble("nan") throws NFE
parseDouble("NaN") = NaN (no throw)          -> BigDecimal.valueOf throws NFE: Character N is neither...
parseDouble("inf") throws NFE
parseDouble("Infinity") = Infinity (no throw)-> BigDecimal.valueOf throws NFE: Character I is neither...
parseDouble("1e400") = Infinity (no throw)   -> BigDecimal.valueOf throws NFE: Character I is neither...
-0.0 == 0.0 : true
NaN != 0    : true
```

| Finding | Verdict | Evidence |
|---|---|---|
| 74.1 | CONFIRMED (reproduced) | Spec: cv with `expireAt=now+600s` → `decr a` → `:99`, stored cv `getExpireAt()==0`. Live log: Velo `ttl:-1` vs Redis `ttl:100`. Mechanism verified in code: `DGroup.java:396/400` → `BaseCommand.java:874-876` (2-arg `setNumber` forces `NO_EXPIRE`) → `set()` creates a new cv (`BaseCommand.java:1161-1166`) → `OneSlot.put` WAL takes `cv.getExpireAt()` |
| 74.2 | CONFIRMED (reproduced) | Spec: `SP_TYPE_NUM_DOUBLE` 1.5 → `decr a` → IntegerReply `0`; stored value re-read as non-double integer `0`. Live log: Velo `:0` + `"0"` vs Redis `-ERR value is not an integer or out of range` + `"1.5"`. `isTypeNumber()` includes `SP_TYPE_NUM_DOUBLE` (`CompressedValue.java:254-260`) and line 371 truncates via `numberValue().longValue()` |
| 74.3 | CONFIRMED (reproduced) | Spec: `decrby a 3000000000` → `ErrorReply.NOT_INTEGER`. Live log: Redis `:-2999999990`. Same `Integer.parseInt` at `IGroup.java:144` for `INCRBY` |
| 74.4 | CONFIRMED (reproduced) | Spec: `Long.MIN_VALUE` stored → `decr a` → IntegerReply `Long.MAX_VALUE` (wrapped), value written back. Live log: Redis `-ERR increment or decrement would overflow` |
| 74.5 | CONFIRMED (reproduced) | Spec: double 10.005 → `decrbyfloat a 0` → **IntegerReply** `10` (fraction destroyed); `incrbyfloat a 0` through the real `IGroup.handle()` → IntegerReply `10` too. Control `decrbyfloat a 0.5` → DoubleReply. JDK repro: `-0.0 == 0.0` is true, so `IGroup.java:172`'s `-by` is `0.0` on the integer path |
| 74.6 | CONFIRMED (reproduced) | Spec: `RDBParser.dumpString` payload hex — `'0123'`→`00c07b…` (int8 123), `'007'`→`00c007…`, `'+123'`→`00c07b…`, `'-0'`→`00c000…`; controls `'hello'`→raw `000568656c6c6f…`, `'-5'`→int8 `00c0fb…` (Redis-legal). `Long.parseLong` accepts all four offending forms; Redis `string2ll` rejects them. Live log: Velo dump of `"0123"` = `00c07b0b00213dee92fde58e30`, restored into real Redis 7.2.11 → `GET` = `"123"` |
| 74.7 | CONFIRMED (reproduced) | Spec (local): key `a` present + `del a >key` → `KEY_TOO_LONG` **and** `a` already removed (`getBuf` null) — partial effect on an error path. Spec (cross-worker, `crossRequestWorker=true` + `LocalPersist.addOneSlot`): `del >key` → IntegerReply, no key-length check anywhere in the fan-out branch (`DGroup.java:296-313`) |
| 74.8 | CONFIRMED (reproduced) | Spec: `decrbyfloat a 1e400` throws raw `NumberFormatException("Character I is neither a decimal digit number, …")` out of the command; `RequestHandler.java:576-580` maps any `Exception` to `new ErrorReply(e.getMessage())` → leaked to the client (matches live log verbatim). Refined input set below |
| 74.9 | CONFIRMED (reproduced) | Spec: `debug xyzzy x` → NilReply, `debug object nokey` → NilReply (`DGroup.java:237-239, 270`). Live log: Redis 7.2.11 errors on both. Note: existing `DGroupTest.groovy` pins the null behavior (lines 150–152, 226–229) — the fix must update those expectations |

### Additional observations

- **74.8 refinement (leaking input set):** the author's parenthetical is correct for the lowercase literals actually tested (`nan`, `inf` —
  both throw in `Double.parseDouble` on JDK 21), but exact-case `"NaN"` and `"Infinity"` **also** slip through the parse and then throw inside
  `BigDecimal.valueOf` with the messages `Character N is neither…` / `Character I is neither…` (see JDK repro above). The suggested fix
  (`Double.isFinite(by)` guard) also catches these because `NaN`/`±Infinity` are non-finite — no change to the fix direction needed.
- **74.8 blast radius addendum:** the stored value path is equally unguarded: a value already stored as a double `Infinity`/`NaN`
  (writable through `SET`'s own `Double.parseDouble` fallback, `BaseCommand.java:1098-1104`) would throw the same raw NFE out of
  `decrBy()` even with a finite delta. The fix should validate `doubleValue` for finiteness too, as the author already suggested.
- **74.7 reachability:** `ConfForGlobal.netWorkers` defaults to `1` (`ConfForGlobal.java:98`), so the cross-worker half requires an explicit
  multi-worker config; the local-branch partial-effect half reproduces on the default config. Low severity stands.
- **74.1 fix scope:** `IGroup.incr/incrby/incrbyfloat` all funnel into `DGroup.decrBy()` (`IGroup.java:134/153/172`), so fixing
  `decrBy()` alone covers `INCR*` as well — no separate IGroup fix needed for this finding.
- **74.3 fix scope:** widening `decrBy(int, double)` to `long` must also widen `IGroup.incrby`'s own `Integer.parseInt` (`IGroup.java:144`),
  otherwise `INCRBY` keeps the int32 limit even after `DECRBY` is fixed.
- **74.9 fix caution:** `DGroupTest.groovy:150-152` (`debug object key` on missing key → NilReply) and `:226-229` (`debug _ test_xxx` →
  NilReply) assert the buggy behavior; the fix commits must flip these expectations in the same commit.
- The temp verification spec was deleted after evidence capture; the fix agent will rebuild these as permanent failing regression tests
  per the Stage 4 workflow.

### Recommended fix order (for the fix agent)

1. **74.1** (TTL drop) — data-lifetime bug, small diff, shared by `DECR*` and `INCR*`.
2. **74.2** (float truncation on the integer path) — silent value corruption.
3. **74.6** (DUMP corruption) — affects backup/migration correctness.
4. **74.5** (zero-delta float mode) — silent value corruption; signature change to explicit `isByFloat`.
5. **74.3** (int32 delta) — rejected legal commands; widen to `long` incl. `IGroup.incrby`.
6. **74.4** (int64 overflow) — silent wrong value; naturally falls out of the 74.3/74.5 refactor area but keep a separate commit.
7. **74.8** (exception leak) — add `Double.isFinite` guards for delta and stored value.
8. **74.7** (DEL key-length consistency) — hoist the check above both branches.
9. **74.9** (DEBUG null vs error) — also update the two pinned assertions in `DGroupTest`.

## Author Response (AI agent 1, round 1)

**Verdict: I agree with all 9 findings.** No retraction, no dispute — the reviewer's verdicts match my own reproductions
(Velo jar on ports 7391/7392 vs. live Redis 7.2.11 on 6398/6399), and the reviewer's temp spec independently reproduced every finding
through the real `DGroup.handle()` / `IGroup.handle()` paths.

| Finding | Author position | Comment |
|---|---|---|
| 74.1 | **Agree** | Confirmed; the reviewer's mechanism chain (`DGroup.java:396/400` → `BaseCommand.java:874-876` → `set()` → `OneSlot.put`) is the same one I cited. Agreed with the fix-scope note: `IGroup.incr*` funnels into `DGroup.decrBy()`, so one fix covers `INCR*` too. |
| 74.2 | **Agree** | Confirmed. **Refinement accepted and folded into the finding:** the truncation also destroys *non-finite* stored doubles — `SET k Infinity` / `SET k NaN` are stored as doubles by `BaseCommand.set()` (BaseCommand.java:1098–1104) and `DECR k` then yields `9223372036854775806` / `-1` (verified on Velo 7391). The fix must reject non-finite doubles too, not only fractional ones. |
| 74.3 | **Agree** | Confirmed. Agreed with the fix-scope note: `IGroup.incrby` (IGroup.java:144) has its own `Integer.parseInt` and must be widened in the same commit, otherwise `INCRBY` keeps the int32 limit. |
| 74.4 | **Agree** | Confirmed; the reviewer exercised `Long.MIN_VALUE` through the command, my live differential exercised the same value against Redis. |
| 74.5 | **Agree** | Confirmed, including the `INCRBYFLOAT k 0` variant through the real `IGroup.handle()` — that is the case my report only inferred from `-0.0 == 0.0`. |
| 74.6 | **Agree** | Confirmed. I re-measured the offending/accepted form set on the real Redis 7.2.11 and added it to the finding: Redis' `string2ll` rejects `"0123"`, `"007"`, `"+123"` **and `"-0"`**, while `"-5"` and `"0"` are Redis-legal integers. The fix must therefore treat `-0` as a non-integer as well. |
| 74.7 | **Agree** | Confirmed. Agreed with the reachability note: the cross-worker half needs `netWorkers > 1` (default 1), the local partial-effect half reproduces on the default config. Low severity stands. |
| 74.8 | **Agree** | Confirmed, and the reviewer's refinement is **more precise than my original text** — my parenthetical ("only the overflow form `1e400` slips through") was true for the lowercase literals I tested (`inf`/`nan` do throw in `Double.parseDouble`) but wrong for the exact-case forms: `NaN`, `Infinity`, `-Infinity` also parse without throwing and leak the raw `NumberFormatException`. I re-measured all four forms on Velo 7391 and updated the finding with the full table. The reviewer's stored-value addendum is also confirmed (`SET k Infinity` + `DECRBYFLOAT k 1` → same raw NFE). No change to the fix direction (`Double.isFinite` on delta **and** stored value). |
| 74.9 | **Agree** | Confirmed. The reviewer's fix caution is important and accepted: `DGroupTest.groovy:150-152` and `:226-229` currently pin the null-reply behaviour and must be flipped in the same commit as the fix. |

No finding is downgraded, merged or retracted. Severity ordering is unchanged; the reviewer's fix order (74.1 → 74.2 → 74.6 → 74.5 → 74.3 →
74.4 → 74.8 → 74.7 → 74.9) is accepted as-is.

## Fix Log (AI agent 1, round 1 fix phase)

All fixes follow red-green: a permanent regression test was added first, watched failing for the expected reason, then the minimal
production change was made. Focused test command used for the green phase:

```
./gradlew -Dorg.gradle.jvmargs="-Xmx6g -XX:MaxMetaspaceSize=1g" cleanTest :test \
  --tests "io.velo.command.DGroupTest" --tests "io.velo.command.IGroupTest" \
  --tests "io.velo.command.HGroupTest" --tests "io.velo.command.RGroupTest" \
  --tests "io.velo.command.VeloRDBImporterTest"
```

(The `-Xmx6g` is needed because the Groovy test compilation runs out of heap with the default daemon size — a pre-existing
infrastructure issue, already noted as an open item in bug 73.)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 74.1 | `DGroupTest.test decr` — red: `inMemoryGetSet.getBuf(slot,'a',0,0L).cv().getExpireAt() == expireAt` → `0 == 1792655698691` (the stored cv had `expireAt=0`) | `fe48c5f1` `fix: keep the key ttl when decr by` | green (DGroupTest, IGroupTest) | `DGroup` 368 (`expireAt` capture, 2/2 branches), 399 + 402 (`setNumber(..., expireAt)`) covered |
| 74.2 | `DGroupTest.test decr` — red: `reply == ErrorReply.NOT_INTEGER` → `<IntegerReply integer=0>` (the double 1.5 was truncated) | `b7baeb52` `fix: do not truncate a double value when decr by` | green (DGroupTest, IGroupTest, HGroupTest) | `DGroup` 373–377 (the `isTypeDouble` guard, 2/2 branches) covered |
| 74.6 | `DGroupTest.test dump` — red: the payload was `00c07b0b00213dee92fde58e30` (int8 123) instead of the expected `0004303132330b00633bb1e163a48dab` (raw `"0123"`, the exact bytes Redis 7.2.11 produces) | `6abee041` `fix: dump only canonical integers with the rdb int encoding` | green (DGroupTest, RGroupTest, VeloRDBImporterTest) | `RDBParser` 529–555 (`isRedisIntegerString`, all branches) + 559–572 (int8/int16/int32 encodings) covered |
| 74.5 | `DGroupTest.test decrbyfloat` — red: `reply instanceof DoubleReply` → `ErrorReply{message='not integer'}`; `IGroupTest.test incrbyfloat` — red with the pre-fix sources stashed: same `ErrorReply{message='not integer'}` | `3185ee13` `fix: use the float path for decrbyfloat with a zero delta` | green (DGroupTest, IGroupTest, HGroupTest) | `DGroup` 358–374 (the explicit `isByFloat` flag) covered |
| 74.3 | `DGroupTest.test decrby` + `IGroupTest.test incrby` — red: `reply instanceof IntegerReply` → `ErrorReply{message='not integer'}` for the delta `3000000000` | `ad698642` `fix: support int64 delta for decrby and incrby` | green (DGroupTest, IGroupTest, HGroupTest) | `DGroup` 187–190, `IGroup` 144–153 covered |
| 74.4 | `DGroupTest.test decr` / `test decrby` + `IGroupTest.test incr` — red: `reply instanceof ErrorReply` → `<IntegerReply integer=9223372036854775807>` / `-9223372036854775808` (the wrapped values) | `648be980` `fix: report overflow when decr by a value out of the long range` | green (DGroupTest, IGroupTest, HGroupTest) | `DGroup` 404–415 (both overflow branches, 4/4 branches) covered |
| 74.8 | `DGroupTest.test decrbyfloat` — red: `java.lang.NumberFormatException: Character I is neither a decimal digit number, …` escaped the command (also for `NaN`, `Infinity` and a stored `Infinity` value) | `ecfa7921` `fix: reject not finite float for decrbyfloat` | green (DGroupTest, IGroupTest, HGroupTest) | `DGroup` 398–400 (`Double.isFinite` guard, 4/4 branches) covered |
| 74.7 | `DGroupTest.test del` — red: `inMemoryGetSet.getBuf(slot,'a',0,0L) != null` → `null` (the key was removed before the `KEY_TOO_LONG` error). Cross-worker half: a temporary spec (`Bug74CrossWorkerDelTempSpec`, deleted afterwards) run against the pre-fix sources never reached the length check — it went on to `DGroup.del():303` and failed with an NPE in `LocalPersist.oneSlot` | `f6b15320` `fix: check the key length before remove in del` | green (DGroupTest) | `DGroup` 279–281 (the hoisted check, 2/2 branches) covered |
| 74.9 | `DGroupTest.test debug` — red: `reply == ErrorReply.NO_SUCH_KEY` → `<NilReply>`; the two expectations that pinned the old behaviour (`DGroupTest:150-152`, `:226-229`) were flipped in the same commit | `39f4dd60` `fix: answer an error for an unsupported debug sub command` | green (DGroupTest) | `DGroup` 237–238 (`NO_SUCH_KEY`) and 270 (`DEBUG_SUB_COMMAND_NOT_SUPPORT`) covered |

Full-suite evidence for the whole fix set: with the nine regression tests in place and the **pre-fix** production sources restored
(`git checkout 798ab06e -- src/main/java/io/velo/command/{DGroup,IGroup,HGroup}.java src/main/java/io/velo/rdb/RDBParser.java
src/main/java/io/velo/reply/ErrorReply.java`), the focused run reported `126 tests completed, 9 failed` — exactly the nine new/updated
regression tests, i.e. every fix is guarded by a test that fails without it. With the fixes in place the same run is
`BUILD SUCCESSFUL` (126 tests, 0 failures).

Live differential after the fixes (Velo jar rebuilt from HEAD, port 7391; Redis 7.2.11, port 6399):

| case | Velo (after) | Redis 7.2.11 |
|---|---|---|
| `SET k 100 EX 100` + `DECR k` + `TTL k` | `:99` / `:99` (TTL kept) | `:99` / `:100` |
| `SET kf 1.5` + `DECR kf` + `GET kf` | `-ERR not integer` / `"1.5"` | `-ERR value is not an integer or out of range` / `"1.5"` |
| `SET kb 10` + `DECRBY kb 3000000000` | `:-2999999990` | `:-2999999990` |
| `SET k2 -9223372036854775808` + `DECR k2` | `-ERR increment or decrement would overflow` (value unchanged) | identical |
| `SET k 10.005` + `DECRBYFLOAT k 0` | double reply `10.01` (fraction kept) | n/a (no such command in Redis 7.2) |
| `DECRBYFLOAT k 1e400` / `NaN` / `Infinity` | `-ERR not float` | n/a |
| `DUMP` of string/list/set/zset/hash | byte-identical to Redis 7.2.11 | — |

### Post-fix notes found during the fix phase

1. **74.6 — the end-to-end symptom has a second root cause outside `DGroup`.** The fix above corrects the RDB writer
   (`RDBParser.writeRdbString`), which is where the finding was verified (the reviewer's spec asserted the `RDBParser.dumpString`
   payload bytes, the new regression test drives the same level through `DUMP`). But the *user visible* difference observed in the
   discovery phase (`SET k 0123` → `DUMP k` → int-encoded payload) is actually caused one step earlier: Velo's `SET` normalises a
   numeric-looking string to a number, because `BaseCommand.set()` (BaseCommand.java:1086–1105) uses `Long.parseLong` without
   Redis' canonical-literal rule. Measured on the fixed build:

   ```
   VELO : set 0123 -> get "123"    set +123 -> get "123"    set -0 -> get "0"    set 007 -> get "7"
   REDIS: set 0123 -> get "0123"  set +123 -> get "+123"  set -0 -> get "-0"  set 007 -> get "007"
   ```

   So a Velo key can currently never hold the string `"0123"`, and the `DUMP` payload difference is a *consequence* of that
   `SET`-time normalisation, not of the RDB writer. The RDB-writer fix is still required (it removes the latent corruption for any
   value that reaches `DUMP` as a string — e.g. data written by another node/version or by the RDB import path — and it makes the
   writer obey Redis' `string2ll` rule), but the **remaining** data-fidelity defect is the lossy `SET` normalisation, which lives in
   `BaseCommand.set()` (string module, not `DGroup`). It is filed here as a follow-up candidate for the next round and was **not**
   fixed in this round, to keep the fix phase scoped to the confirmed `DGroup` findings:

   > **Follow-up candidate (not part of round 1):** `BaseCommand.set()` converts any string that `Long.parseLong`/`Double.parseDouble`
   > accepts into a number, so `SET k 0123` stores `123` and `GET k` returns `"123"`. Redis keeps the exact string. Fix direction: use
   > the same canonical-literal test as the RDB writer before the number conversion.

2. **74.8 — one sibling path is still unguarded (out of scope for this round).** The fix in `DGroup.decrBy()` covers `DECRBYFLOAT`,
   `INCRBYFLOAT` and the *split-storage* `HINCRBYFLOAT` (which delegates to `decrBy`). The **HH-mode** `HINCRBYFLOAT`
   (`HGroup.hincrby2()`, HGroup.java:1490–1545) computes `BigDecimal.valueOf(byFloat)` itself and still leaks the raw
   `NumberFormatException` for a non-finite delta. It is a hash-module defect, tracked as a follow-up for the hash module round, not
   fixed here.

3. **74.3 — `HINCRBY`'s own delta is still int32.** `HGroup.hincrby()` parses the delta with `Integer.parseInt`
   (HGroup.java:1439) and therefore still rejects an int64 delta, although `DGroup.decrBy()` now accepts one. Also a hash-module item
   (follow-up), out of the `DGroup` scope of this round.

## Re-review (AI agent 2)

## Residual Risk

- `BaseCommand.set()` still normalises non-canonical numeric strings to numbers (see post-fix note 1) — data fidelity issue in the
  string module, not fixed in this round.
- `HGroup.hincrby2()` (HH mode) still leaks a raw `NumberFormatException` for a non-finite `HINCRBYFLOAT` delta (post-fix note 2).
- `HGroup.hincrby()` still parses the `HINCRBY` delta as int32 (post-fix note 3).
- `DECR`/`DECRBY` on a key holding a non-string type answer `ERR not integer` where Redis answers `WRONGTYPE …` (error-string
  difference only, recorded as a non-finding).
- The `DEBUG` error strings (`not such key`, `debug subcommand not support`) do not match Redis' wording verbatim.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 74.1 | `DECR`/`DECRBY`/`DECRBYFLOAT` drop the key TTL | High | Correctness | CONFIRMED (reproduced) | fixed `fe48c5f1` |
| 74.2 | `DECR`/`DECRBY` truncate a float (or non-finite) value | Medium | Correctness | CONFIRMED (reproduced) | fixed `b7baeb52` |
| 74.3 | `DECRBY` delta limited to int32 | Medium | Redis compatibility | CONFIRMED (reproduced) | fixed `ad698642` |
| 74.4 | int64 overflow wraps silently | Medium | Correctness | CONFIRMED (reproduced) | fixed `648be980` |
| 74.5 | zero delta switches `DECRBYFLOAT` to the integer path | Medium | Correctness | CONFIRMED (reproduced) | fixed `3185ee13` |
| 74.6 | `DUMP` corrupts numeric-looking strings | High | Correctness | CONFIRMED (reproduced) | fixed `6abee041` (see post-fix note 1) |
| 74.7 | `DEL` key-length check missing in the cross-worker path | Low | Edge case | CONFIRMED (reproduced) | fixed `f6b15320` |
| 74.8 | `DECRBYFLOAT` with a non-finite delta or stored value leaks a Java exception message | Low | Edge case | CONFIRMED (reproduced) | fixed `ecfa7921` (see post-fix note 2) |
| 74.9 | `DEBUG` answers null instead of an error | Low | Redis compatibility | CONFIRMED (reproduced) | fixed `39f4dd60` |

