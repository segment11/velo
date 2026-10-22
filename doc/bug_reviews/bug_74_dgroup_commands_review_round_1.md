# Bug 74 — DGroup Commands Review (Round 1)

**Module:** `io.velo.command.DGroup` (DBSIZE, DEBUG, DECR, DECRBY, DECRBYFLOAT, DEL, DUMP)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 9 findings, none verified yet
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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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

### Suggested fix direction

On the integer path, reject a double-typed cv explicitly: `if (!isByFloat && cv.isTypeDouble()) return NOT_INTEGER;`
(the same check is needed for the string branch: Redis rejects `"1.5"` with the same error — that part already works because
`Long.parseLong("1.5")` throws).

---

## Finding 74.3: `DECRBY` delta is parsed with `Integer.parseInt` — only int32 is accepted

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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

Trigger: `SET k 0123` then `DUMP k` (and the same for `"+123"`, `"007"`, …).

| server | `DUMP k` payload (hex) |
|---|---|
| Velo 7391 | `00 c0 7b 0b 00 213dee92fde58e30` → `RDB_TYPE_STRING` + `RDB_ENC_INT8 123` |
| Redis 7.2.11 | `00 04 30313233 0b 00 633bb1e163a48dab` → `RDB_TYPE_STRING` + raw `"0123"` |

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
**Status:** `OPEN — not verified`

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
**Status:** `OPEN — not verified`

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

(`DECRBYFLOAT k inf` and `DECRBYFLOAT k nan` are correctly rejected with `ERR not float`, because `Double.parseDouble` throws for those
literals — only the overflow form `1e400` slips through.)

Blast radius: an internal implementation detail is exposed to clients, the error is not classifiable, and the failure is logged at ERROR level
as a server-side exception for a purely client-side input error.

### Suggested fix direction

Reject non-finite deltas explicitly (`Double.isFinite(by)`) and answer `ErrorReply.NOT_FLOAT`; the same guard should be applied to the value
read from the key.

---

## Finding 74.9: `DEBUG` answers a null reply for unknown subcommands and for a missing key

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `OPEN — not verified`

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

**Method:** _(to be filled by the reviewer)_

| Finding | Verdict | Evidence |
|---|---|---|
| 74.1 | | |
| 74.2 | | |
| 74.3 | | |
| 74.4 | | |
| 74.5 | | |
| 74.6 | | |
| 74.7 | | |
| 74.8 | | |
| 74.9 | | |

### Additional observations

### Recommended fix order (for the fix agent)

1. **74.1** (TTL drop) — data-lifetime bug, one-line fix, shared by `DECR*` and `INCR*`.
2. **74.2** (float truncation on the integer path) — silent value corruption.
3. **74.6** (DUMP corruption) — affects backup/migration correctness.
4. **74.5** (zero-delta float mode) — silent value corruption.
5. **74.3** (int32 delta) — rejected legal commands.
6. **74.4** (int64 overflow) — silent wrong value.
7. **74.7**, **74.8**, **74.9** — small consistency/protocol issues.

## Fix Log (AI agent 1, round 1 fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 74.1 | | | | |
| 74.2 | | | | |
| 74.3 | | | | |
| 74.4 | | | | |
| 74.5 | | | | |
| 74.6 | | | | |
| 74.7 | | | | |
| 74.8 | | | | |
| 74.9 | | | | |

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 74.1 | `DECR`/`DECRBY`/`DECRBYFLOAT` drop the key TTL | High | Correctness | OPEN | — |
| 74.2 | `DECR`/`DECRBY` truncate a float value | Medium | Correctness | OPEN | — |
| 74.3 | `DECRBY` delta limited to int32 | Medium | Redis compatibility | OPEN | — |
| 74.4 | int64 overflow wraps silently | Medium | Correctness | OPEN | — |
| 74.5 | zero delta switches `DECRBYFLOAT` to the integer path | Medium | Correctness | OPEN | — |
| 74.6 | `DUMP` corrupts numeric-looking strings | High | Correctness | OPEN | — |
| 74.7 | `DEL` key-length check missing in the cross-worker path | Low | Edge case | OPEN | — |
| 74.8 | `DECRBYFLOAT 1e400` leaks a Java exception message | Low | Edge case | OPEN | — |
| 74.9 | `DEBUG` answers null instead of an error | Low | Redis compatibility | OPEN | — |
