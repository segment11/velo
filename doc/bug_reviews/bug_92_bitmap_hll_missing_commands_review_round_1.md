# Bug 92 — Bitmap / HyperLogLog Missing Commands Review (Round 1)

**Module:** `io.velo.command.BGroup` (bitmap), `io.velo.command.PGroup` (HyperLogLog)
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** discovery complete — 3 findings (missing commands), none verified yet
**Review branch:** `review/bitmap_hll`
**Reviewed commit:** `fb4f8f2988d32ff2e047a74d8f7530722d97a3f5`

> Source of the gap list: `REDIS_COMMANDS_NOT_IMPLEMENTED.md` (generated 2026-11-19) — Bitmap: 1
> missing (BITOP), HyperLogLog: 2 missing (PFDEBUG, PFSELFTEST). This round implements the three
> commands, it is a feature-gap sweep, not a behavior sweep of the existing bitmap/HLL commands
> (those were covered by bug_72 for BGroup and bug_84 for PGroup; do not re-report those findings).

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/BGroup.java` — static CommandRegistry block (32-201), `parseSlots()` (221-308), `handle()` (321-381), `bitcount()` (383-450), `bitpos()` (452-542), `bf()` (544-589)
- `src/main/java/io/velo/command/PGroup.java` — static CommandRegistry block (32-137), `parseSlots()` (163-215), `handle()/handleCommand()` (222-306), `emptyHll()` (139-143), `serializeHll()/saveHll()/saveHllBytes()` (331-359), `HllWithBytes/getHllWithBytes()` (368-395), `pfadd()` (401-438), `readHllList()` (453-463), `pfcount()` (465-523), `pfmerge()` (525-633, cross-worker dst write pattern of bug 84.1)
- `src/main/java/io/velo/command/SGroup.java` — `setbit()` (1009-1063), `setrange()` (1065-1131): string read/write + HLL/bloom WRONGTYPE deviation note
- `src/main/java/io/velo/BaseCommand.java` — `get(slotWithKeyHash, expectTypeString, ...)` (969-981), `set(valueBytes, slotWithKeyHash[, spType[, expireAt]])` (1107-1140), `getValueBytesByCv()` (889+), `isCrossRequestWorker` (286)
- `src/test/groovy/io/velo/command/PGroupTest.groovy` — `test pfadd` (338-440), `test pfcount and pfmerge` (442+)
- `src/test/groovy/io/velo/command/BGroupTest.groovy` — `test bitcount` (242+), `test bitpos` (352+)
- `src/main/java/io/velo/acl/Category.java` — lines 77-78, 167-168, 246-250: `pfdebug`/`pfselftest` already listed in ACL categories
- HLL library bytecode inspection (`com.github.prasanthj:hyperloglog:1.1`): `HyperLogLog` (builder defaults, `count()`, `merge()`, `sparseToDenseRegister()`), `HLLSparseRegister` (`encodeHash`, sparse map key layout), `HLLDenseRegister` (`set/getRegister`)

Reference docs (ground truth):

- Redis 7.2 `src/bitops.c` — `bitopCommand()` (fetched from https://raw.githubusercontent.com/redis/redis/7.2/src/bitops.c)
- Redis 7.2 `src/hyperloglog.c` — `pfdebugCommand()`, `pfselftestCommand()` (fetched from https://raw.githubusercontent.com/redis/redis/7.2/src/hyperloglog.c)
- Redis docs: https://redis.io/commands/bitop/, /pfdebug/, /pfselftest/
- `AGENTS.md`, `doc/how_to_write_high_coverage_test_cases_for_commands.md`, `doc/write_better_test_cases_for_ai_agents.md`

## Finding 1 (Bug 92.1): BITOP not implemented

**Severity:** Medium
**Class:** Redis protocol compatibility (missing command)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// BGroup.java:32-201 — the static CommandRegistry block registers bgsave/bitcount/bitpos/bl*/bz*/
// and the bf.* family; there is no "bitop" entry
// BGroup.java:380 — handle() falls through to: return ErrorReply.UNKNOWN_COMMAND(cmd);
```

### Root cause

The bitmap family is implemented for the single-key commands (BITCOUNT, BITPOS in `BGroup`; SETBIT,
GETBIT in `SGroup`/`GGroup`), but the multi-key `BITOP` was never registered nor handled, so a client
sending `BITOP AND dst srca srcb` receives an unknown-command error.

### Trigger / impact

`BITOP <and|or|xor|not> destkey srckey...` — Redis 7.2 semantics (bitops.c `bitopCommand`):

- arity -4, keys: dest is 1st key, sources follow; case-insensitive op names
- unknown op → `ERR syntax error`
- `NOT` with argc != 4 → `BITOP NOT must be called with a single source key.` (no ERR prefix)
- missing source keys are empty strings; a non-string source → `WRONGTYPE Operation against a key
  holding the wrong kind of value` (in Velo the accepted `WRONGTYPE` reply)
- result length = max source length; bytes beyond a shorter source count as 0x00
- reply = integer result length
- empty result (all sources missing/empty) → dest key is deleted if it exists, reply 0

Impact: any client using BITOP (dedup pipelines, bitmap unions) fails; coverage gap called out by
`REDIS_COMMANDS_NOT_IMPLEMENTED.md` (Bitmap 92%).

### Suggested fix direction

Register `bitop` (arity -4, first key 2) in `BGroup`; parseSlots covers dest + all source keys
(`addToSlotWithKeyHashList` like `pfmerge`); handle() implements `bitop()`:

- same-slot path: read sources via `get(s, true)` (string type guard like `setbit`), compute
  AND/OR/XOR/NOT over `maxlen`, write dest via `set(bytes, slotWithKeyHash)`, delete dest when
  `maxlen == 0`
- cross-worker path: follow the bug 84.1 pfmerge pattern — read source values per source slot with
  `oneSlot.asyncCall`, then perform the dest write inside `dstOneSlot.asyncCall`; settle a
  `SettablePromise` + `AsyncReply` in all cases

## Finding 2 (Bug 92.2): PFDEBUG not implemented

**Severity:** Low
**Class:** Redis protocol compatibility (missing command)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// PGroup.java:32-137 — no "pfdebug" entry in the CommandRegistry block
// PGroup.java:304-305 — handleCommand() falls through to ErrorReply.UNKNOWN_COMMAND(cmd)
```

### Root cause

PFADD/PFCOUNT/PFMERGE are implemented; the internal debug command `PFDEBUG` is not. Redis 7.2
`pfdebugCommand()` subcommands (ground truth):

- `PFDEBUG <sub> <key>`; key missing → `ERR The specified key does not exist`; not an HLL → the
  `WRONGTYPE Key is not a valid HyperLogLog string value.` reply Velo already uses (bug 84.7)
- `GETREG <key>` — convert sparse to dense (persisting), reply array of register values
- `DECODE <key>` — sparse only; reply bulk string of the decoded sparse opcode stream
  (`z:<len>` / `Z:<len>` / `v:<val>,<len>` space separated); dense → `ERR HLL encoding is not sparse`
- `ENCODING <key>` — status reply `dense` or `sparse`
- `TODENSE <key>` — convert if sparse, reply 1 if converted else 0
- unknown sub → `ERR Unknown PFDEBUG subcommand '<sub>'`; wrong arg count →
  `ERR Wrong number of arguments for the '<sub>' subcommand`

### Trigger / impact

Debug/ops tooling that inspects HLL internals fails. Note: Velo's HLL is `com.github.prasanthj:hll`
(Murmur3, p=14 → 16384 registers by default — same register count as Redis, but a different hash
function and sparse layout), so register *values* cannot match Redis for the same elements; the
command shape and subcommand semantics will match, values are Velo's own (documented deviation).

### Suggested fix direction

Register `pfdebug` (-3) in `PGroup`; `pfdebug()` dispatches on data[1]: ENCODING → status from
`hll.getEncoding()`; TODENSE → dense conversion via merge-into-empty-dense (the library's own
`sparseToDenseRegister` path), save, reply 1/0; GETREG → reply the dense register array
(`denseRegister.getRegister()`); DECODE → decode the sparse map (`index = key & (2^p - 1)`,
`value = byte`) into the Redis opcode format.

## Finding 3 (Bug 92.3): PFSELFTEST not implemented

**Severity:** Low
**Class:** Redis protocol compatibility (missing command)
**Status:** `OPEN — not verified`

### Cited files / lines

```java
// PGroup.java:32-137 — no "pfselftest" entry in the CommandRegistry block
```

### Root cause

Redis 7.2 `pfselftestCommand()` (no args) runs: (1) a register set/get roundtrip test, (2) a
cardinality error-bound test over checkpoints (10, 100, ...) comparing dense and sparse counts,
replying `+OK` on success or `-TESTFAILED ...` on failure.

### Trigger / impact

Internal validation tooling missing. Velo cannot reuse Redis' exact constants (different estimator:
sparse uses linear counting, dense uses HLL estimator with bias correction), so the self-test must
use Velo's own standard error (`1.04/sqrt(16384)` matches since p=14) and its own iteration budget
(Redis runs 10M adds; too heavy for a command call, use checkpoints up to 100k).

### Suggested fix direction

Register `pfselftest` (arity 1) in `PGroup`; run (1) dense register set/get roundtrip, (2) add
unique elements at checkpoints 10/100/1k/10k/100k asserting `|est - n| <= ceil(6 * relerr * n)`
(with the checkpoint-10 special-case bound of Redis), also asserting the forced-dense conversion
keeps the same estimate (tests the same conversion code PFDEBUG TODENSE uses); reply OK or
`TESTFAILED ...`.

## Notes / non-findings

- ACL categories already contain `pfdebug`/`pfselftest` (`Category.java:77-78`), no ACL change needed.
- `get(slotWithKeyHash, true)` (BaseCommand:969) already throws `TypeMismatchException` for
  non-string keys — the WRONGTYPE behavior of BITOP sources comes from it; HLL/Bloom CVs are rejected
  by the same check — consistent with the accepted `setbit` deviation recorded in bug_77/86.
- Velo's HLL default `numRegisterIndexBits` = 14 (verified by bytecode probe: 16384 registers),
  matching Redis' register count; the dense register is one byte per register (`HLLDenseRegister`).
- `HyperLogLog.count()` for SPARSE uses linear counting (bytecode `count()` → `linearCount`),
  for DENSE the HLL estimator — the PFSELFTEST error bound must account for that.
- Bitmap single-key commands and the pf* trio behaviors were reviewed by bug_72 / bug_84 and are
  out of scope here.
- The cross-worker `*STORE` destination write pattern (write dst on the dst slot worker, settle the
  promise in all paths) is the bug 84.1 / bug 90.2 lesson; BITOP must follow it.

## Reviewer checklist (for AI agent 2)

- Finding 1: `redis-cli BITOP AND d s1 s2` against Velo at `fb4f8f29` → unknown command error; confirm
  no `bitop` token in `BGroup.java` static block or `handle()`. Redis 7.2 differential for semantics:
  `SET s1 a`, `SET s2 b`, `BITOP XOR d s1 s2` → 1, `GET d` → 0x03. `BITOP NOT d` → syntax/NOT error.
- Finding 2: `PFDEBUG ENCODING k` → unknown command at the reviewed commit; Redis: after
  `PFADD k a b`, `PFDEBUG ENCODING k` → `sparse`.
- Finding 3: `PFSELFTEST` → unknown command at the reviewed commit; Redis replies `OK`.

## Review Feedback (AI agent 2, round 1)

**Method:** static inspection (grep over `src/main/java/io/velo/command/*Group.java`, `dyn/src`,
`RequestHandler.java`, `MultiWorkerServer.java` + reading the cited line ranges) **plus a live
differential** against the local Redis 7.2.14 build (`/home/kerry/ws/redis/src/redis-server`, run on
port 7799) and a live Velo instance built from `build/libs/velo-1.2.0.jar` (run on port 7919) to
confirm what a client actually receives. HLL library defaults verified by `javap -p -c` on
`com.github.prasanthj:hyperloglog:1.1`.

Reviewer note: all three findings are **feature gaps (missing commands)**, not behavior bugs in
existing code, so "reproduction" here means *the client-visible reply*, which was reproduced live.

| Finding | Verdict | Evidence |
|---|---|---|
| 92.1 | **CONFIRMED (reproduced)** — command genuinely absent; one ground-truth detail in the finding is wrong (see corrections C1) | Live Velo (port 7919, jar from `build/libs`): `redis-cli -p 7919 bitop and d s1 s2` → `ERR unknown command 'bitop'`; `BITOP AND d s1 s2` (uppercase) → same. `COMMAND INFO bitop` → empty. Static: `grep -rni "bitop" src/main/java/io/velo/command/` → no match; `grep -rni "bitop" src/main/java/io/velo/RequestHandler.java src/main/java/io/velo/MultiWorkerServer.java` → no match; repo-wide `grep -rni "bitop" src/main dyn/src` → only `src/main/java/io/velo/acl/Category.java:100,650,875` (ACL category lists only, no implementation). `BGroup.java` static registry block (32-201) registers `bgsave/bitcount/bitpos/blmove/blmpop/blpop/brpop/brpoplpush/bzmpop/bzpopmax/bzpopmin/bf.*` — no `bitop`; `parseSlots()` (221-308) has branches only for `bitcount/bitpos/bf.*`, `blmove/brpoplpush`, `blmpop`, `blpop/brpop`, `bzmpop`, `bzpopmax/bzpopmin`; `handle()` (321-381) has branches for `bf.*/bgsave/bitcount/bitpos/blmpop/blmove/blpop/brpop/brpoplpush/bzmpop/bzpopmax/bzpopmin` and ends at line 380 with `return ErrorReply.UNKNOWN_COMMAND(cmd);` → wire reply `ERR unknown command 'bitop'` (`ErrorReply.java:28-30`). |
| 92.2 | **CONFIRMED (reproduced)** — command genuinely absent; two ground-truth details in the finding need correction (see corrections C2, C3) | Live Velo: `redis-cli -p 7919 pfdebug encoding hll` → `ERR unknown command 'pfdebug'`; `COMMAND INFO pfdebug` → empty. Static: `grep -rni "pfdebug" src/main/java/io/velo/command/` → no match; `grep -rni "pfdebug" src/main/java/io/velo/RequestHandler.java src/main/java/io/velo/MultiWorkerServer.java` → no match; repo-wide `grep -rni "pfdebug" src/main dyn/src` → only `src/main/java/io/velo/acl/Category.java:77,167,248,712,934` (ACL lists). `PGroup.java` static registry block (32-137) registers `persist/pexpire/pexpireat/pexpiretime/pfadd/pfcount/pfmerge/psetex/psubscribe/pttl/publish/pubsub/punsubscribe` — no `pfdebug`; `parseSlots()` (163-215) has no `pfdebug` branch; `handleCommand()` (232-305) has no `pfdebug` branch and ends at line 305 with `return ErrorReply.UNKNOWN_COMMAND(cmd);`. |
| 92.3 | **CONFIRMED (reproduced)** — command genuinely absent; one ground-truth detail in the finding needs correction (see corrections C4) | Live Velo: `redis-cli -p 7919 pfselftest` → `ERR unknown command 'pfselftest'`; `COMMAND INFO pfselftest` → empty. Static: `grep -rni "pfselftest" src/main/java/io/velo/command/` → no match; repo-wide `grep -rni "pfselftest" src/main dyn/src` → only `src/main/java/io/velo/acl/Category.java:78,168,250,714` (ACL lists). `PGroup.java` registry (32-137) / `parseSlots()` (163-215) / `handleCommand()` (232-305) contain no `pfselftest` entry or branch → falls through to `ErrorReply.UNKNOWN_COMMAND(cmd)` at line 305. |

### Ground-truth verification against the local Redis 7.2.14 source

`/home/kerry/ws/redis/src/version.h` → `#define REDIS_VERSION "7.2.14"`. Ground truth was checked both
by reading the local source and by running the local `redis-server` (port 7799) live.

**BITOP — `src/bitops.c:607 bitopCommand()`** (all claims verified):

```
// bitops.c:618-630 — op parsing
if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname,"and")) op = BITOP_AND;
... else { addReplyErrorObject(c,shared.syntaxerr); return; }
// bitops.c:632-636 — NOT arity
if (op == BITOP_NOT && c->argc != 4) {
    addReplyError(c,"BITOP NOT must be called with a single source key.");
```

- arity **-4** — `commands/bitop.json` → `'arity': -4`; live `COMMAND INFO bitop` → `-4 / write / denyoom / firstkey 2 / lastkey -1`. Matches the finding's "arity -4, first key 2". ✅
- unknown op → `ERR syntax error` — live: `bitop foo d s1 s2` → `ERR syntax error`. ✅
- non-string source → `WRONGTYPE Operation against a key holding the wrong kind of value` — live: `bitop and d4 s1 mylist` (list key) → exactly that. ✅
- result length = max source length; bytes past a shorter source count as 0x00 — live: `bitop and d2 s1 nosuchkey` → `:1` with a 1-byte result (0x61 & 0x00). ✅
- empty result deletes dest if it exists, replies 0 — live: `set d3 x; bitop or d3 nosuchkey1 nosuchkey2` → `:0` and `exists d3` → `0`. ✅
- case-insensitive op names — `strcasecmp` in the source. ✅
- ❌ **CORRECTION C1**: the finding says the NOT error has **"no ERR prefix"**. That is wrong. `src/networking.c:486-492 addReplyErrorLength()` prepends `-ERR ` unless the message already starts with `-`, so the client sees **`ERR BITOP NOT must be called with a single source key.`** — live: `bitop not d s1 s2` → `ERR BITOP NOT must be called with a single source key.` The fix must use a normal `ERR `-prefixed error reply, not a bare message.

**PFDEBUG — `src/hyperloglog.c:1553 pfdebugCommand()`** (claims verified, two corrections):

```
// hyperloglog.c:1559-1564 — key is resolved BEFORE the subcommand is dispatched
o = lookupKeyWrite(c->db,c->argv[2]);
if (o == NULL) { addReplyError(c,"The specified key does not exist"); return; }
if (isHLLObjectOrReply(c,o) != C_OK) return;   // -> "-WRONGTYPE Key is not a valid HyperLogLog string value."
```

- subcommands `GETREG / DECODE / ENCODING / TODENSE` — all four confirmed present. ✅
- key missing → `ERR The specified key does not exist` — live: `pfdebug encoding nokey` → exactly that. ✅
- decode on dense → `ERR HLL encoding is not sparse` — live: `pfdebug decode hll` (dense) → exactly that. ✅
- unknown sub → `ERR Unknown PFDEBUG subcommand '<sub>'` — live: `pfdebug bogus hll` (valid HLL key) → `ERR Unknown PFDEBUG subcommand 'bogus'`. ✅
- GETREG converts sparse→dense **and persists it** — live: `pfadd hll4 d; pfdebug encoding hll4` → `sparse`; `pfdebug getreg hll4`; `pfdebug encoding hll4` → `dense`. GETREG returns exactly **16384** integers (`pfdebug getreg hll | wc -l` → 16384). ✅
- TODENSE replies 1 when it converted, 0 when already dense — live: `pfdebug todense hll3` → `1`, then again → `0`. ✅
- DECODE output format `z:<len>` / `Z:<len>` / `v:<val>,<len>` space separated — live: `pfdebug decode hll2` → `Z:12711 v:2,1 Z:3068 v:1,1 Z:603`. ✅
- Velo's `ErrorReply.WRONG_TYPE_HLL` text (`src/main/java/io/velo/reply/ErrorReply.java:102-103`) is `"WRONGTYPE Key is not a valid HyperLogLog string value."` — byte-identical to Redis (`src/hyperloglog.c:1223-1224`). ✅
- ❌ **CORRECTION C2**: the finding's "wrong arg count → `ERR Wrong number of arguments for the '<sub>' subcommand`" is **unreachable in Redis 7.2**. `commands/pfdebug.json` declares `'arity': 3` (exact) and live `COMMAND INFO pfdebug` → `3`, so the command table rejects `PFDEBUG ENCODING` (argc 2) first with **`ERR wrong number of arguments for 'pfdebug' command`** (live-verified). The `arityerr:` label in `pfdebugCommand()` is dead code. The finding's *fix direction* ("Register `pfdebug` (-3)") is also inconsistent with Redis: use arity **3** (exact) so that `PFDEBUG ENCODING` yields the generic arity error.
- ❌ **CORRECTION C3**: the finding does not state the **ordering** — Redis resolves the key and validates that it is an HLL *before* dispatching on the subcommand. Consequence: `PFDEBUG <unknown-sub> <non-HLL-key>` returns `WRONGTYPE Key is not a valid HyperLogLog string value.`, **not** `Unknown PFDEBUG subcommand` — live: `pfdebug bogus s1` (plain string) → `WRONGTYPE Key is not a valid HyperLogLog string value.`; `pfdebug bogus hll` (valid HLL) → `ERR Unknown PFDEBUG subcommand 'bogus'`. The Velo implementation must reproduce this order.

**PFSELFTEST — `src/hyperloglog.c:1444 pfselftestCommand()`** (claims verified, one correction):

- no arguments, replies `+OK` on success — live: `pfselftest` → `OK`; `commands/pfselftest.json` → `'arity': 1`, `'reply_schema': {'const': 'OK'}`, flags `admin`, no keys. ✅
- failure replies are `-TESTFAILED ...` — source emits `TESTFAILED Register %d should be %d but is %d`, `TESTFAILED sparse encoding not used`, `TESTFAILED dense/sparse disagree`, `TESTFAILED Too big error. card:%llu abserr:%llu`. ✅
- checkpoint-10 special case → source `if (j == 10) maxerr = 1;`. ✅
- Redis runs 10M adds → source `for (j = 1; j <= 10000000; j++)`. ✅
- ❌ **CORRECTION C4**: the finding says "cardinality error-bound test over checkpoints (10, 100, ...)". The real sequence starts at **1**: `int64_t checkpoint = 1;` and the bound is applied when `j == checkpoint`, so checkpoints are **1, 10, 100, 1000, …, 10000000** (`checkpoint *= 10`). The finding's fix direction (checkpoints 10/100/1k/10k/100k for Velo) is a deliberate *deviation* from Redis and is fine, but it must be stated as such. Also omitted from the finding: Redis's **"TESTFAILED sparse encoding not used"** check (the object must still be sparse while `j < server.hll_sparse_max_bytes/2`) — worth reproducing since it exercises the sparse→dense switch threshold.

### Corrections summary (ground-truth claims that need fixing in the finding text)

| ID | Finding | Wrong claim | Correct fact |
|---|---|---|---|
| C1 | 92.1 | BITOP NOT error has "no ERR prefix" | It **does** have the `ERR ` prefix: `ERR BITOP NOT must be called with a single source key.` (`src/networking.c:486-492`) |
| C2 | 92.2 | PFDEBUG arity `-3`; subcommand arity error is `Wrong number of arguments for the '<sub>' subcommand` | Redis arity is **3 (exact)**; a wrong arg count yields the generic `ERR wrong number of arguments for 'pfdebug' command`; the subcommand-level arity message is unreachable dead code |
| C3 | 92.2 | (omission) | Redis resolves + type-checks the key **before** dispatching the subcommand → an unknown sub on a non-HLL key returns WRONGTYPE, not "Unknown PFDEBUG subcommand" |
| C4 | 92.3 | checkpoints are "10, 100, …" | Redis checkpoints are **1, 10, 100, …, 10000000**; also a "sparse encoding not used" assertion exists that the finding omits |

### Velo HLL library default p=14 — claim VERIFIED

`javap -p -c -cp .../hyperloglog-1.1.jar 'com.github.prasanthj.hll.HyperLogLog$HyperLogLogBuilder'`:

```
public com.github.prasanthj.hll.HyperLogLog$HyperLogLogBuilder();
   4: aload_0
   5: bipush        14        <-- numRegisterIndexBits = 14
   7: putfield      #4        // Field numRegisterIndexBits:I
  11: getstatic     #6        // EncodingType.SPARSE
  18: iconst_1                // bitPacking = true
  23: iconst_1                // noBias = true
```

`2^14 = 16384` registers — identical to Redis `src/hyperloglog.c:194-197`
(`#define HLL_P 14`, `#define HLL_REGISTERS (1<<HLL_P) /* With P=14, 16384 registers. */`). ✅
Velo never overrides it: `src/main/java/io/velo/command/PGroup.java:139-143 emptyHll()` is
`HyperLogLog.builder().setEncoding(SPARSE).build()` with no `setNumRegisterIndexBits(...)` call, and
`grep -rn "setNumRegisterIndexBits" src/` → no match. ✅

Bonus: `HyperLogLog.getStandardError()` is bytecode `1.04d / sqrt(m)` with `m = 2^p = 16384`
→ `1.04/128 = 0.008125`, **exactly** Redis's `relerr = 1.04/sqrt(HLL_REGISTERS)` — the fix agent can
use `hll.getStandardError()` instead of hard-coding `1.04/sqrt(16384)`.

### Additional observations

1. **Velo's registry `arity` is metadata only, it is not enforced at dispatch.**
   `src/main/java/io/velo/command/CommandRegistry.java` is read only by `CommandCommand`
   (`COMMAND INFO/DOCS/LIST/COUNT`); the grep for `arity` in `src/main/java/io/velo/` outside the
   `CommandEntry` record finds **no** enforcement site. Every command validates its own argument count
   inside `handle()` and returns `ErrorReply.FORMAT` ("syntax error"). Consequence for the fix: the
   `CommandEntry` arity values only affect `COMMAND INFO`/`COMMAND DOCS` output, so they should still
   be set to match Redis (`bitop` → -4, `pfdebug` → 3, `pfselftest` → 1) for protocol fidelity, but the
   *actual* arity error texts must be produced by the command's own `handle()` code.
2. **`sparseToDenseRegister` is private in the HLL library — it cannot be called directly.**
   `javap -p` on `com.github.prasanthj.hll.HyperLogLog` shows
   `private HLLDenseRegister sparseToDenseRegister(HLLSparseRegister)`. The finding's 92.2 fix
   direction mentions "the library's own `sparseToDenseRegister` path"; the implementable route is the
   one already listed first in that direction — build an empty **DENSE** HLL and `merge()` the sparse
   one into it (`HyperLogLog.merge()` handles DENSE←SPARSE and switches the target's encoding), or
   hand-roll the conversion via `HLLSparseRegister.getSparseMap()` → `new HLLDenseRegister(p)` →
   `set(index, value)` → `hll.setHLLDenseRegister(byte[])` + `hll.setEncoding(DENSE)`.
3. **The sparse map key is already the register index — do not re-mask it.**
   `HLLSparseRegister.getSparseMap()` returns an `Int2ByteSortedMap` keyed by the register index
   (`encodeHash` computes `hash & mask` internally), value = register byte. The finding's 92.2 fix
   direction writes "`index = key & (2^p - 1)`", which would corrupt the index — the map key is the
   index as-is.
4. **Useful public API surface for the fix** (all public, confirmed by `javap`):
   `HyperLogLog.getEncoding()`, `getNumRegisterIndexBits()`, `getStandardError()`, `count()`,
   `getHLLDenseRegister()` → `HLLDenseRegister.getRegister() : byte[]`,
   `getHLLSparseRegister()` → `HLLSparseRegister.getSparseMap() : Int2ByteSortedMap`,
   `HyperLogLog.setHLLDenseRegister(byte[])`, `setEncoding(EncodingType)`.
5. **Cross-worker write pattern** — the finding's 92.1 direction (read each source in its own slot
   worker, write the destination inside the destination slot's worker, settle the promise on every
   path) matches the bug 84.1 / 90.2 lesson and is the highest-risk part of 92.1. Note also the exact
   Redis destination semantics (`src/bitops.c:780-792`): if the computed result length `maxlen > 0`
   the destination key is **written**; if `maxlen == 0` (every source missing or empty) the
   destination is **deleted if it exists** and is *not* created. A missing source key is treated as an
   empty string. So the fix needs both a write path and a delete path on the destination slot.
6. **ACL needs no change** — `pfdebug` / `pfselftest` / `bitop` are already present in
   `src/main/java/io/velo/acl/Category.java` (lines 77-78, 100, 167-168, 248-250, 650, 712, 714, 875,
   934). Confirmed by grep. ✅
7. **Coverage-gap source is accurate** — `REDIS_COMMANDS_NOT_IMPLEMENTED.md:27-29` (Bitmap: 1 missing
   — BITOP), `:82-85` (HyperLogLog: 2 missing — PFDEBUG, PFSELFTEST), `:273-274` (Bitmap 92%,
   HyperLogLog 71%). ✅
8. **Live Velo control commands work**, so the "unknown command" replies above are specific to the
   three commands and not a broken server: `pfadd hll a b` → `1`, `pfcount hll` → `2`,
   `bitcount s1` → `3` on the same instance (port 7919).

### Recommended fix order (for the fix agent)

**Order: 92.1 (BITOP) → 92.2 (PFDEBUG) → 92.3 (PFSELFTEST)** — the order proposed by the author is
confirmed, with the following justification:

1. **92.1 BITOP first.** It is the only one of the three that is a *user-facing, production-used*
   command (bitmap set-union/dedup pipelines) and the only Medium-severity finding; it also closes the
   whole Bitmap gap (92% → 100%). It is also the most complex (multi-key, multi-slot, cross-worker
   destination write per the bug 84.1 / 90.2 pattern), so it deserves the fix agent's freshest
   attention and its own isolated commit. Fix it **before** touching the HLL commands so the two
   independent feature areas do not collide in the same commit.
2. **92.2 PFDEBUG second.** It is the more valuable of the two HLL commands (it is what operators
   actually use to inspect/convert an HLL) and it establishes the sparse/dense conversion helper that
   92.3 reuses. Implement it with the corrected ordering and arity from C2/C3.
3. **92.3 PFSELFTEST last.** Lowest value, no key/slot handling at all, and it depends on the
   dense-conversion helper introduced by 92.2. Keep it in its own commit, per the bug-fix workflow.

Per the AGENTS.md bug-fix workflow, each of the three must go through its own red/green TDD cycle and
its own commit (test first → verify it fails for the expected reason → minimal production change →
verify it passes → confirm the changed lines in the JaCoCo HTML report under
`build/reports/jacocoHtml/` → commit). Do not batch the three into one commit.

For 92.1 the expected TDD anchors are the Redis-7.2.14 live behaviours reproduced above
(`BITOP XOR d s1 s2` → `:1` with a `0x03` result; `BITOP NOT d s1 s2` → `ERR BITOP NOT must be
called with a single source key.`; `BITOP foo …` → `ERR syntax error`; empty result → `:0` and the
destination key deleted; list source → `WRONGTYPE Operation against a key holding the wrong kind of
value`). For 92.2/92.3 the anchors are `PFDEBUG ENCODING <missing>` → `ERR The specified key does not
exist`, `PFDEBUG ENCODING <hll>` → `sparse`/`dense`, `PFDEBUG DECODE <dense>` → `ERR HLL encoding is
not sparse`, `PFDEBUG TODENSE <sparse>` → `:1` then `:0`, `PFDEBUG GETREG <hll>` → 16384 integers,
`PFDEBUG <bogus> <hll>` → `ERR Unknown PFDEBUG subcommand 'bogus'`, `PFDEBUG ENCODING` → `ERR wrong
number of arguments for 'pfdebug' command`, `PFSELFTEST` → `+OK`.

## Fix Log (AI agent 1, round 1 fix phase)

### Agent 1 response (round 1)

All four reviewer corrections accepted and adopted:

- **C1**: `BITOP NOT must be called with a single source key.` is answered as a plain Velo
  `ErrorReply` so the wire shows the `ERR ` prefix, matching live Redis 7.2.14.
- **C2**: `pfdebug` registered with exact arity 3, the handle() path enforces `data.length == 3`
  first (`ErrorReply.FORMAT` → the redis arity error), so the subcommand-level arity text stays
  unreachable exactly as in Redis.
- **C3**: `pfdebug()` resolves the key and HLL-type-checks it *before* dispatching the subcommand.
- **C4**: `PFSELFTEST` checkpoints start at 1 and the `TESTFAILED sparse encoding not used`
  assertion is implemented (asserted only below 1500 elements — the library promotes to dense at
  ~2458 elements, probe `HllProbe4`; Redis asserts below `hll_sparse_max_bytes/2 = 1500`).

One reviewer observation is **refuted**: *"the sparse map key is already the register index (don't
re-mask)"*. The library's own `sparseToDenseRegister` masks `key & (2^p - 1)` (bytecode:
`iload key; iload mask; iand; istore index`) and probe keys reached 32862801 > 16383. Moot in the
final implementation anyway: `DECODE` renders the opcode stream from the dense register state (the
same registers `GETREG` reports), and `test pfdebug` cross-validates decode == the `GETREG`
register array, so no manual sparse-key decoding exists in the production code.

### Fix table

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 92.1 BITOP | `BGroupTest.test bitop` RED at the first `then:` — got `ErrorReply{message='unknown command 'bitop''}` expected `ErrorReply.FORMAT` (test report 2026-11-20) | `996e7e40` `feat: implement bitop command (bug 92.1)` | green (`BGroupTest` 51/51) | bitop lines 570-800: 107/119 covered; every semantic line/branch covered (compute paths, both store paths, cross-worker WRONGTYPE line 734 both branches, NOT/AND/OR/XOR, case-insensitive op); uncovered = defensive promise/catch paths (724-726, 750-757) |
| 92.2 PFDEBUG | `PGroupTest.test pfdebug` RED at the first `then:` — got `unknown command 'pfdebug'` expected `ErrorReply.FORMAT` | `1ce0a857` `feat: implement pfdebug command (bug 92.2)` | green (`PGroupTest` 23/23 → 24/24) | pfdebug lines 664-800: 63/68 covered; uncovered = the two `saveHll` IOException catches (757-758, 769-770 after the pfselftest commit shifted lines, was 752-753/764-765 at `1ce0a857`) and the empty-opcode-string guard (713), all defensive |
| 92.3 PFSELFTEST | `PGroupTest.test pfselftest` RED — `unknown command 'pfselftest'`; second RED `TESTFAILED sparse encoding not used` before the 1500 bound fix | `1ee44833` `feat: implement pfselftest command (bug 92.3)` | green (`PGroupTest` 24/24) | pfselftest lines 814-891: arity + happy path + `selfTestMaxerrExceeded` failure/success branches covered; uncovered (849/863/869/886) = TESTFAILED internal branches that cannot fire with a correct implementation (Redis' own suite cannot cover them either) |
| registry count | — | `e9c081bf` `test: bump command registry count for bitop/pfdebug/pfselftest` (211 → 214 + presence assertions) | green (`CommandRegistryPopulationTest`) | n/a (test only) |
| re-review follow-up | — | (this commit) `test bitop` adds the missing-first-source and shorter-first-source cases from re-review residual 1 | green (`BGroupTest` 51/51) | `computeBitop` range 605-645: 21/22 lines, the only partial is the post-validation-unreachable `else` at 635; bitop range 570-765 now covers every reachable branch |

Final regression: `io.velo.command.*` — **590 tests, 0 failures** (2026-11-20).
Field access check: `scripts/test_field_access_check.sh BGroupTest` / `PGroupTest` — 0 issues.

## Re-review (AI agent 2)

**Re-review date:** 2026-11-20 · **Branch:** `review/bitmap_hll` · **HEAD reviewed:** `1aba0e89`
(base `fb4f8f29`) · **Method:** independent re-derivation (temporary git worktrees, fresh test
runs, fresh JaCoCo inspection, source-level comparison against the local Redis 7.2.14 tree
`/home/kerry/ws/redis/src`). The Fix Log was **not** trusted; every number below was re-derived.

> Note: the two headings above/below were empty placeholders reserved for this re-review; only the
> placeholder bodies are filled in. No pre-existing section was rewritten (the round-1 corrections
> C1–C4, the round-1 observations and the Summary table are left exactly as agent 1 wrote them —
> except for the factual correction of observation #3 recorded in the "Dispute ruling" subsection,
> which is flagged here rather than edited in place).

### Verdicts

| Commit | Bug | Content | Verdict |
|---|---|---|---|
| `996e7e40` | 92.1 | BITOP implementation + test | **ACCEPTED** |
| `1ce0a857` | 92.2 | PFDEBUG implementation + test | **ACCEPTED** |
| `1ee44833` | 92.3 | PFSELFTEST implementation + test | **ACCEPTED** |
| `e9c081bf` | — | registry count 211 → 214 + presence assertions | **ACCEPTED** |
| `1aba0e89` | — | report Fix Log + gap-list update | **ACCEPTED** (1 documentation nit, non-blocking) |

No blocking defect was found. Three non-blocking items are listed per commit below and collected
in "Residual Risk".

### 1. RED re-derivation (independently reproduced)

Production code absent at every parent commit (`git show <parent>:<file> | grep -c -i <token>`):

```
$ git show fb4f8f29:src/main/java/io/velo/command/BGroup.java | grep -c -i bitop
0
$ git show 996e7e40:src/main/java/io/velo/command/PGroup.java | grep -c -i pfdebug
0
$ git show 1ce0a857:src/main/java/io/velo/command/PGroup.java | grep -c -i pfselftest
0
```

and present at HEAD:

```
$ git show 1aba0e89:src/main/java/io/velo/command/BGroup.java | grep -c -i bitop
24
$ git show 1aba0e89:src/main/java/io/velo/command/PGroup.java | grep -c -i pfdebug
7
$ git show 1aba0e89:src/main/java/io/velo/command/PGroup.java | grep -c -i pfselftest
4
```

The tests are **not** present at the parent commits either (`grep -c "def 'test bitop'"` → 0,
`def 'test pfdebug'` → 0, `def 'test pfselftest'` → 0), so the red state was re-created by
checking out the parent and applying **only** the test file of the fix commit:

```
git worktree add /tmp/opencode/rereview-92-1 fb4f8f29
cd /tmp/opencode/rereview-92-1
git checkout 996e7e40 -- src/test/groovy/io/velo/command/BGroupTest.groovy   # test only, no production code
./gradlew :test --tests "io.velo.command.BGroupTest"
```

Result (92.1) — `tests=51 failures=2`:

```
--- test parse slot - multi key
Condition not satisfied:
_BGroup.parseSlots('bitop', data5, slotNumber).size() == 3
|       []                  |      128         0      false
	at io.velo.command.BGroupTest.test parse slot - multi key(BGroupTest.groovy:98)
--- test bitop
Condition not satisfied:
reply == ErrorReply.FORMAT
|     |  |          |
|     |  |          ErrorReply{message='syntax error'}
|     false
ErrorReply{message='unknown command 'bitop''}
	at io.velo.command.BGroupTest.test bitop(BGroupTest.groovy:504)
```

92.2 — worktree checked out at `996e7e40`, `PGroupTest.groovy` taken from `1ce0a857`
(`grep -c -i pfdebug` on the production file = 0), `tests=23 failures=1`:

```
--- test pfdebug
Condition not satisfied:
reply == ErrorReply.FORMAT
|     false
ErrorReply{message='unknown command 'pfdebug''}
	at io.velo.command.PGroupTest.test pfdebug(PGroupTest.groovy:673)
```

92.3 — worktree checked out at `1ce0a857`, `PGroupTest.groovy` taken from `1ee44833`
(`grep -c -i pfselftest` on the production file = 0), `tests=24 failures=1`:

```
--- test pfselftest
Condition not satisfied:
reply == ErrorReply.FORMAT
|     false
ErrorReply{message='unknown command 'pfselftest''}
	at io.velo.command.PGroupTest.test pfselftest(PGroupTest.groovy:823)
```

All three red states fail for exactly the expected reason (the command is unknown), i.e. the RED
evidence in the Fix Log is reproducible, and the red failure is the *first* `then:` of the new
test method (not an incidental later assertion).

> Environment note for reproducers: a worktree needs `gradle.properties` copied from the main
> checkout (`org.gradle.jvmargs=-Xmx8g`) and the `segment_common` / `segmentweb` submodule
> contents exported (`git -C segment_common archive HEAD | tar -x -C <worktree>/segment_common`),
> otherwise `:compileTestGroovy` dies with `Java heap space` / "No matching variant of project
> :segment_common". The `Java heap space` failure is the known daemon flake: re-running once after
> `./gradlew --stop` succeeds.

### 2. Focused tests green at HEAD (`1aba0e89`)

```
$ ./gradlew :test --tests "io.velo.command.BGroupTest" \
                  --tests "io.velo.command.PGroupTest" \
                  --tests "io.velo.command.CommandRegistryPopulationTest"
BUILD SUCCESSFUL in 23s        (EXIT=0)
```

```
io.velo.command.BGroupTest                tests=51 failures=0 errors=0 skipped=0
io.velo.command.PGroupTest                tests=24 failures=0 errors=0 skipped=0
io.velo.command.CommandRegistryPopulationTest tests=1 failures=0 errors=0 skipped=0
```

Test durations: `test bitop` 0.174 s, `test pfdebug` 0.075 s, `test pfselftest` 0.015 s.
Field-access check re-run: `scripts/test_field_access_check.sh BGroupTest` → **0 issues**,
`... PGroupTest` → **0 issues** (no Groovy reflection into non-public Java members).

Wider regression re-run by me on the same HEAD (`./gradlew :test --tests "io.velo.command.*"`):
`BUILD SUCCESSFUL in 6m 52s` — **40 test classes, 590 tests, 0 failures, 0 errors, 0 skipped**
(this independently confirms the "590 tests, 0 failures" claim of the Fix Log).

### 3. JaCoCo (regenerated by the run above, `build/reports/jacocoHtml/`)

```
$ python3 scripts/jacoco_cover.py io.velo.command.BGroup 570 800
Lines: 119 | Covered: 107 | Partial: 4 | Not-covered: 8
Branches: 36 lines with branches, 32 fully covered
Not-covered lines: 724, 725, 726, 750, 751, 755, 756, 757
Partial lines:     624, 635, 723, 749

$ python3 scripts/jacoco_cover.py io.velo.command.PGroup 664 800
Lines: 69 | Covered: 64 | Partial: 1 | Not-covered: 4
Not-covered lines: 757, 758, 769, 770
Partial lines:     718

$ python3 scripts/jacoco_cover.py io.velo.command.PGroup 810 895
Lines: 40 | Covered: 32 | Partial: 4 | Not-covered: 4
Not-covered lines: 849, 863, 869, 886
Partial lines:     848, 862, 868, 885
```

Class-level line coverage of the two touched classes:
`io.velo.command.BGroup` 1078/1121 = **96.2 %**, `io.velo.command.PGroup` 445/487 = **91.4 %**
(both well above the 80 % threshold of `AGENTS.md`).

Every uncovered line is a *failure* branch that cannot fire with a correct implementation:

- `BGroup` 724–726 = `if (e != null)` of `Promises.all(...)` (a source read threw);
  750–751 = `if (e1 != null)` of the destination write; 755–757 = the `catch (Exception e1)` of
  the assembly block. All three are promise-failure paths.
- `PGroup` 757–758 and 769–770 = the two `catch (IOException e)` blocks of `saveHll` in the
  `todense` / `getreg` branches.
- `PGroup` 849 = `TESTFAILED Register ...`; 863 = `TESTFAILED sparse encoding not used`;
  869 = the `TESTFAILED Too big error ...` return of the self test; 886 = the command-level
  `new ErrorReply(failure)` return.

**Two deviations from the Fix Log's claims (documentation only, not defects):**

1. The Fix Log lists the 92.2 uncovered lines as `752-753 / 764-765 / 713`. At HEAD the same
   uncovered statements sit at **757-758 / 769-770** and line 713 is now *covered* — the pfselftest
   commit (`1ee44833`) inserted 5 lines above them, so the line numbers drifted. The
   *statements* are the same (the two `saveHll` IOException catches); the Fix Log numbers were
   correct at the time of `1ce0a857` and are stale at HEAD.
2. The Fix Log says "every semantic line/branch covered" for BITOP. All *lines* are covered except
   the defensive promise paths, but 4 lines are only **partially** branch-covered: 624 and 635
   (`srcValues[..] != null && srcValues[..].length > j`) and 723 / 749 (the defensive `e != null`).
   The two missed branches on 624/635 are the *"first source missing / first source shorter than
   the result"* cases of `computeBitop` (e.g. `BITOP AND d nosuchkey s3` and `BITOP AND d s1 s3`
   with `s1` shorter than `s3`). The code is correct in both cases (verified by reading), but the
   branches are not exercised by the test → see the non-blocking item for 92.1 below.

### 4. Semantics vs Redis 7.2.14 (source-level)

Ground truth used: `/home/kerry/ws/redis/src/bitops.c` (`bitopCommand`, lines 607-793),
`/home/kerry/ws/redis/src/hyperloglog.c` (`pfdebugCommand` 1553-1652, `pfselftestCommand`
1444-1546), `/home/kerry/ws/redis/src/networking.c` (`addReplyErrorLength`),
`/home/kerry/ws/redis/src/commands/{bitop,pfdebug,pfselftest}.json`.

#### 92.1 BITOP (`BGroup.java`: registry 52-58, `parseSlots` 242-250, `handle` 360-362,
`BitopRead` 575, `computeBitop` 609-641, `storeBitopResult` 652-658, `bitop()` 661-762)

| Redis behaviour | Velo implementation | Result |
|---|---|---|
| `commands/bitop.json` arity **-4**, firstkey 2, lastkey -1, step 1 | `CommandEntry("bitop", -4, {write,denyoom}, 2, -1, 1, …)` | ✅ identical |
| arity < 4 → `ERR wrong number of arguments for 'bitop' command` | `bitop()` returns the `ErrorReply.FORMAT` singleton; `MultiWorkerServer` rewrites that singleton to `ErrorReply.WRONG_NUMBER(cmd)` on the wire (`MultiWorkerServer.java:614`, `:677` and `:705`) → `ERR wrong number of arguments for 'bitop' command` | ✅ identical on the wire |
| unknown op → `ERR syntax error` (`bitops.c:627-629`, `shared.syntaxerr`) | `ErrorReply.SYNTAX` — *not* the `FORMAT` singleton, so it is **not** rewritten → `ERR syntax error` | ✅ identical |
| `BITOP NOT` with argc != 4 → `ERR BITOP NOT must be called with a single source key.` (`bitops.c:633-636`; `networking.c:486-492` prepends `ERR `) | `new ErrorReply("BITOP NOT must be called with a single source key.")` → `-ERR BITOP NOT must be called with a single source key.` | ✅ identical (round-1 correction **C1** honoured) |
| missing source key = empty string (`bitops.c:646-652`) | `get(...)` returns `null` for a missing key → treated as an all-zero string | ✅ |
| result length = longest source length (`bitops.c:668`) | `computeBitop` computes `maxlen` over the non-null sources | ✅ |
| bytes past a shorter source count as 0 (`bitops.c:747-748`) | `srcValues[i] != null && srcValues[i].length > j ? … : 0` | ✅ |
| `NOT` inverts the single source byte (`bitops.c:749`, `unsigned char output`) | `res[j] = (byte) ~output` on an `int` holding 0..255 → identical 8-bit result | ✅ |
| `AND`/`OR`/`XOR` over all sources (`bitops.c:751-766`) | same loop, `output &= / \|= / ^= b` | ✅ |
| op name case-insensitive (`bitops.c:619-626`, `strcasecmp`) | `equalsIgnoreCase` | ✅ |
| non-string source → `WRONGTYPE Operation against a key holding the wrong kind of value` (`bitops.c:654`) | `get(s, true)` throws `TypeMismatchException` → `ErrorReply.WRONG_TYPE` (same text, `ErrorReply.java:98-99`) — same-slot path **and** cross-worker path (`readBitopSourceValues` returns `null` → `finalPromise.set(ErrorReply.WRONG_TYPE)`) | ✅ both paths |
| empty result → destination **deleted if it exists**, reply `:0` (`bitops.c:783-787`) | `storeBitopResult`: `result.length == 0` → `remove(dstS)`, reply `IntegerReply(0)` | ✅ |
| non-empty result → destination written, reply = result length (`bitops.c:780-782`, `addReplyLongLong(c,maxlen)`) | `set(result, dstS)` + `IntegerReply(result.length)` | ✅ |
| source type check happens **before** the destination write | same-slot: all reads precede `storeBitopResult`; cross-worker: the reads are awaited by `Promises.all(...)` before the destination write is scheduled | ✅ |
| destination write must run on the destination slot worker (bug 84.1 / 90.2 pattern) | `dstOneSlot.asyncCall(() -> storeBitopResult(dstS, …))` (`BGroup.java:746-747`), the promise is settled on every path | ✅ same shape as `PGroup.pfmerge` |

Additional checks I made beyond the finding:

- `addToSlotWithKeyHashList` (`BaseCommand.java:75-84`) does **not** de-duplicate, so the parsed
  list keeps the argument order (`[dst, src1, src2, …]`) even for repeated source keys — the index
  mapping `srcValues[j-1] ← parsed.get(j)` (same-slot) and `IndexAndSlotWithKeyHash(j, …)` (cross)
  is therefore correct for duplicated keys as well.
- The cross-worker assembly uses the per-read index (`srcValues[read.index() - 1]`), so the order
  of the `Collectors.groupingBy` groups does not matter. ✅
- `parseSlots` for `bitop` returns an empty list when `data.length < 4`; an empty slot list means
  `Request.getSingleSlot() == SLOT_CAN_HANDLE_BY_ANY_WORKER` (`decode/Request.java:213-215`), so the
  arity error is still answered by a worker. ✅
- `ErrorReply.FORMAT` vs `ErrorReply.SYNTAX`: both carry the text `syntax error` but are different
  instances; only the `FORMAT` singleton is rewritten to the arity error by the dispatcher. The
  implementation uses the right one in each place (unknown op → `SYNTAX`, bad arity → `FORMAT`).
  ✅

Non-blocking item (92.1): the two uncovered branches of `computeBitop` (missing / shorter *first*
source). A two-line addition to `test bitop` would close them, e.g. `bitop and dst nosuchkey s3`
(first source missing, second source 3 bytes) and `bitop and dst s1 s3` with `s1` = 1 byte and
`s3` = 3 bytes. The implementation is already correct in both cases; only the regression guard is
missing.

#### 92.2 PFDEBUG (`PGroup.java`: registry 91-98, `parseSlots` 224-231, `handle` 294-296,
`toDenseHll` 679-687, `decodeSparseRegisters` 697-718, `pfdebug()` 721-790)

| Redis behaviour (`hyperloglog.c:1553-1652`) | Velo implementation | Result |
|---|---|---|
| `commands/pfdebug.json` arity **3** (exact), flags write/denyoom/admin, firstkey 2 lastkey 2 step 1 | `CommandEntry("pfdebug", 3, {write,denyoom,admin}, 2, 2, 1, …)` | ✅ identical |
| wrong arg count → `ERR wrong number of arguments for 'pfdebug' command` (the `arityerr:` label at `hyperloglog.c:1650-1652` is dead code because the command table rejects it first) | `pfdebug()` returns the `ErrorReply.FORMAT` singleton → rewritten by the dispatcher to `ErrorReply.WRONG_NUMBER("pfdebug")` | ✅ identical on the wire (round-1 correction **C2** honoured) |
| key resolved **before** the subcommand dispatch (`hyperloglog.c:1559-1564`) | `getHllWithBytes(s)` + the type check happen before the `encoding`/`todense`/`getreg`/`decode` branches | ✅ |
| missing key → `ERR The specified key does not exist` | `new ErrorReply("The specified key does not exist")` (no `rawErrorPrefix` → `-ERR …`) | ✅ |
| non-HLL key → `WRONGTYPE Key is not a valid HyperLogLog string value.` (`isHLLObjectOrReply`) | `getHllWithBytes` throws `TypeMismatchException` → `ErrorReply.WRONG_TYPE_HLL`, byte-identical text to `hyperloglog.c:1223-1224` | ✅ |
| `ENCODING` → `sparse` / `dense` | `new BulkReply(encoding)` | ⚠️ **type deviation** (see below) — the *text* is identical, the RESP type is bulk string instead of simple string |
| `TODENSE` → `:1` when it converted, `:0` when already dense, and the conversion **persists** (`hyperloglog.c:1626-1640`) | `saveHll(s, toDenseHll(hll), hllWithBytes.expireAt())` then `IntegerReply.REPLY_1`; already dense → `REPLY_0` | ✅ (the expire time is preserved, asserted by the test) |
| `GETREG` → converts sparse→dense **and persists it** (`hyperloglog.c:1571-1582`), replies an array of **16384** integers | `toDenseHll` + `saveHll` when the encoding changed, `MultiBulkReply` of 16384 `IntegerReply` | ✅ (16384 asserted by the test, persistence asserted by the test) |
| `DECODE` on a dense key → `ERR HLL encoding is not sparse` (`hyperloglog.c:1594-1598`) | `new ErrorReply("HLL encoding is not sparse")` | ✅ |
| `DECODE` on a sparse key → bulk string `z:<len>` / `Z:<len>` / `v:<val>,<len>` space separated; `z` for a zero run ≤ 64, `Z` for a longer one (`HLL_SPARSE_ZERO_MAX_LEN 64`), `v` runs capped at 4 (`HLL_SPARSE_VAL_MAX_LEN 4`) | `decodeSparseRegisters()`: `run <= 64 ? "z:" : "Z:"`, `v:` chunks of `min(run, 4)` | ✅ same format and same boundaries |
| `DECODE` must **not** change the stored object | `pfdebug()` only calls `toDenseHll(hll)` on an in-memory copy, no `saveHll` | ✅ (not asserted by a test → minor) |
| unknown sub → `ERR Unknown PFDEBUG subcommand '<sub>'` (`hyperloglog.c:1649`) | `new ErrorReply("Unknown PFDEBUG subcommand '" + subCmd + "'")` | ✅ |
| subcommand names case-insensitive (`strcasecmp`) | `equalsIgnoreCase` | ✅ |

Round-1 correction **C3** is honoured: `PFDEBUG <unknown-sub> <non-HLL key>` returns the WRONGTYPE
reply, because the key lookup + type check precede the dispatch. The test asserts the ordering with
`pfdebug nosuch missing` → `The specified key does not exist` and `pfdebug encoding wrongtype` →
`WRONGTYPE …`; the combined "unknown sub + non-HLL key" case is not asserted but follows from the
code structure.

**Reply-type deviation (non-blocking, documented below):** Redis answers `PFDEBUG ENCODING` with a
*simple string* (`addReplyStatus`, `hyperloglog.c:1618-1621`); Velo answers with a *bulk string*
(`$5\r\nsparse\r\n`). `redis-cli` renders both as `sparse`, but a RESP-level client sees a
different type. The cause is a framework gap, not a bug in this command: `io.velo.reply` has only
`OKReply` (`+OK`) and `PongReply` (`+PONG`) — there is no generic status/simple-string reply type
in Velo. `PFDEBUG ENCODING` is very nearly the only non-`OK` status reply in the whole Redis
command set, so I do **not** request a new reply type for this command alone; it is recorded as a
known deviation.

#### 92.3 PFSELFTEST (`PGroup.java`: registry 99-106, `handle` 298-300,
`selfTestMaxerrExceeded` 814-825, `runSelfTest` 835-877, `pfselftest()` 879-889)

| Redis behaviour (`hyperloglog.c:1444-1546`) | Velo implementation | Result |
|---|---|---|
| `commands/pfselftest.json` arity **1**, flags admin, no keys | `CommandEntry("pfselftest", 1, {admin}, 0, 0, 0, …)` | ✅ identical |
| no args → `+OK` (`addReply(c,shared.ok)`) | `OKReply.INSTANCE` | ✅ |
| wrong arg count → `ERR wrong number of arguments for 'pfselftest' command` | `ErrorReply.FORMAT` singleton → dispatcher rewrites it to `ErrorReply.WRONG_NUMBER("pfselftest")` | ✅ identical on the wire |
| failures are `-ERR TESTFAILED …` (`addReplyError`/`addReplyErrorFormat` prepend `ERR `) | `new ErrorReply("TESTFAILED …")` → `-ERR TESTFAILED …` | ✅ identical |
| test 1: dense register set/get roundtrip, `TESTFAILED Register %d should be %d but is %d` | identical message and check (`runSelfTest` 837-851) | ✅ |
| test 2: `relerr = 1.04/sqrt(HLL_REGISTERS)` (= 1.04/128), `maxerr = ceil(relerr*6*checkpoint)` | `relerr = 1.04 / Math.sqrt(SELF_TEST_REGISTERS)` with `SELF_TEST_REGISTERS = 1<<14 = 16384` → identical constant; `maxerr = (long) Math.ceil(relerr * 6 * checkpoint)` | ✅ |
| the `checkpoint == 10` special case `maxerr = 1` (`hyperloglog.c:1517`) | `if (checkpoint == 10) { maxerr = 1; }` | ✅ |
| `abserr = |checkpoint − count|`, failure when `abserr > maxerr`, message `TESTFAILED Too big error. card:%llu abserr:%llu` | `Math.abs(checkpoint - estimate)`, same comparison and the same message format | ✅ |
| checkpoints start at **1** and multiply by 10 (`int64_t checkpoint = 1; … checkpoint *= 10;`) | `checkpoint = 1L`, `checkpoint *= 10` → 1, 10, 100, 1000, 10000, 100000 | ✅ (round-1 correction **C4** honoured) |
| `TESTFAILED sparse encoding not used` while `j < server.hll_sparse_max_bytes/2` (= 1500 with the default `hll_sparse_max_bytes` = 3000) | `if (checkpoint < 1_500 && hll.getEncoding() != SPARSE)` | ✅ same bound |
| `TESTFAILED dense/sparse disagree` (the dense and the sparse counts must be *equal*) | replaced by two independent bound checks (`hll.count()` and `toDenseHll(hll).count()` must both be inside the bound) | ⚠️ deliberate deviation — Velo's sparse count is a linear count over the library's 2^25 `pPrime` space and its dense count is the HLL estimator, so they are *not* numerically equal; requiring equality would fail every run. The replacement is strictly weaker in one way (it tolerates a small sparse/dense difference) and stronger in another (it bounds both estimates). Documented below. |
| 10 000 000 additions | 100 000 additions (`SELF_TEST_MAX_ELEMENTS`) | ⚠️ deliberate deviation (documented below) — the runtime is 15 ms, so a 10 M budget would also have been feasible; the deviation is stated in the report, not hidden |

The "sparse encoding not used" assertion is **meaningful** in Velo: the library promotes the sparse
encoding to dense when the sparse map exceeds `encodingSwitchThreshold = (m*6/8)/5 = 2457` entries
(`HyperLogLog.java:112/256`, `HLLSparseRegister`), i.e. at ~2458 elements — above the 1500 bound, so
the assertion at the checkpoints 1/10/100/1000 really exercises the sparse path and can only fail
if the sparse encoding regressed.

#### Registry entries — parameter encoding checked against `CommandEntry`
(`CommandEntry(name, arity, flags, firstKey, lastKey, keyStep, aclCategories, group, summary, since, complexity)`)

```
BGroup:53  "bitop",     -4, {write,denyoom},          2, -1, 1, {@write,@bitmap,@slow}, ...
PGroup:92  "pfdebug",    3, {write,denyoom,admin},    2,  2, 1, {@write,@hyperloglog,@admin,@dangerous,@slow}, ...
PGroup:100 "pfselftest", 1, {admin},                  0,  0, 0, {@hyperloglog,@admin,@dangerous,@slow}, ...
```

Compared with `/home/kerry/ws/redis/src/commands/{bitop,pfdebug,pfselftest}.json`: arity, flags and
key positions are identical. The extra ACL categories (`@admin`, `@dangerous`, `@slow`) follow the
existing Velo convention for admin commands (`AGroup:35`, `BGroup:40`, `CGroup:34`, …), and the
commands are already listed in `io.velo.acl.Category` (`Category.java:77-78`), so no ACL change was
needed. ✅

### 5. `REDIS_COMMANDS_NOT_IMPLEMENTED.md`

The update in `1aba0e89` is consistent with the code:

- "Velo implemented commands: 211 → **214**" — matches the registry size asserted by
  `CommandRegistryPopulationTest` (`size == 214`, the test passes).
- Bitmap section "1 command → **0 commands**", HyperLogLog section "2 commands → **0 commands**",
  both annotated "all implemented (verified 2026-11-20)" with the implementation file and the
  cross-worker note. ✅
- Coverage table: `Bitmap | 12 | 0 | 12 | 100%`, `HyperLogLog | 7 | 0 | 7 | 100%` — arithmetically
  consistent with the table's own rule ("implemented = commands − missing"). ✅
- Highlights: "Excellent Bitmap coverage (100%)", "Complete HyperLogLog coverage (100%)". ✅
- The HyperLogLog note states the register count matches Redis (16384, p=14) while the hash
  function and the sparse layout are the Velo library's — that is accurate (verified below). ✅

Cross-check against the local Redis 7.2.14 tree: `src/commands/` contains `pfadd, pfcount,
pfmerge, pfdebug, pfselftest` and **no** `pfselect` (PFSELECT only exists from Redis 8.0), so
"HyperLogLog: 0 missing" is correct for the 7.2 baseline this document uses.

Documentation nit (non-blocking): the "Commands (estimate)" column of the coverage table is an
estimate, and the derived "Implemented" column now reads 7 for HyperLogLog while Velo implements 5
HLL commands (`pfadd`, `pfcount`, `pfmerge`, `pfdebug`, `pfselftest`) and 7 bitmap commands
(`setbit`, `getbit`, `bitcount`, `bitpos`, `bitop`, `bitfield`, `bitfield_ro`). The estimate column
was not touched by this commit and the table still reconciles by its own documented rule, so I do
not request a change — but the "Implemented (derived)" cells for Bitmap/HyperLogLog would be more
accurate as 7 and 5.

### Dispute ruling — the sparse-key masking question (agent 1 vs. agent 2, round 1)

**Ruling: agent 1 is correct, my round-1 observation #3 was wrong.** The observation ("the sparse
map key is already the register index — do not re-mask it") must not be relied upon; it is
factually incorrect. Verified from the library sources
(`hyperloglog-1.1-sources.jar`, `com.github.prasanthj.hll`):

```java
// HLLSparseRegister.java — the sparse map key
private final int pPrimeMask;                       // = (1 << P_PRIME_VALUE) - 1 = (1 << 25) - 1
int key = encodedHash & pPrimeMask;                 // mergeTempListToSparseMap(): a 25-bit value

// HyperLogLog.java — the register index is derived by masking
int pMask = (1 << p) - 1;                           // = 16383 for p = 14
int idx = key & pMask;                              // sparseToDenseRegister()
result.set(idx, entry.getValue());
```

with `HLLConstants.P_PRIME_VALUE = 25`. So the sparse map is keyed by a **25-bit** value
(key space 2^25 = 33 554 432 — consistent with agent 1's probe keys > 16383) and the register index
is `key & (2^p − 1)`. The original finding's formula `index = key & (2^p - 1)` was right and my
correction was wrong.

**Impact on the shipped code: none.** The final implementation never decodes the sparse map:
`PFDEBUG DECODE` renders the opcode stream from the dense register state
(`toDenseHll(hll).getHLLDenseRegister().getRegister()`, `PGroup.java:786-789`), i.e. from the
library's own conversion path (`dense.merge(sparse)` → `sparseToDenseRegister` with the correct
masking). The masking bug I feared never got a chance to ship. The dispute is therefore moot for
the code, but it is **not** moot for the document: round-1 observation #3 must not be used as a
reference for future work on this module (I am not editing that section here, per the "append only"
rule of this re-review).

The second half of agent 1's refutation — that the `test pfdebug` cross-validation
(`decode == getreg`) is meaningful — is **confirmed**: `test pfdebug` rebuilds the 16384-entry
register array from the decoded opcode stream and asserts it equals the array reported by
`PFDEBUG GETREG` for the same key, with `idx == 16384` as the length check. That is a genuine
round-trip check of the opcode format (run-length encoding is lossless), even though both sides
are derived from the same dense register array. A separate, *independent* correctness anchor does
exist for the conversion: the test asserts that `PFCOUNT` still reports 3 after `PFDEBUG TODENSE`
converted the sparse HLL to dense, i.e. the conversion preserves the cardinality estimate.

### Per-commit verdicts (detail)

- **`996e7e40` (92.1 BITOP) — ACCEPTED.** RED reproduced, green at HEAD, semantics match
  `bitops.c` line by line (including the round-1 C1 correction), the destination write follows the
  bug 84.1 pattern, the registry entry matches `bitop.json`. Non-blocking: 2 uncovered branches of
  `computeBitop` (missing / shorter *first* source) — recommend 2 extra assertions in `test bitop`.
- **`1ce0a857` (92.2 PFDEBUG) — ACCEPTED.** RED reproduced, green at HEAD, key-before-dispatch
  ordering (C3) and exact arity 3 (C2) honoured, all four subcommands and all error texts match
  `hyperloglog.c`. Non-blocking: `ENCODING` answers a bulk string instead of a simple string
  (framework gap, no `StatusReply` type in Velo); the Fix Log's JaCoCo line numbers are stale at
  HEAD (drift of +5, same statements); the "DECODE does not persist the dense conversion"
  property is not asserted by the test.
- **`1ee44833` (92.3 PFSELFTEST) — ACCEPTED.** RED reproduced, green at HEAD, arity 1, the `+OK`
  reply, the `TESTFAILED` texts, the `relerr` constant, the `checkpoint == 10` special case and the
  checkpoint sequence 1…100000 all match `hyperloglog.c`. Non-blocking: Redis's
  `TESTFAILED dense/sparse disagree` equality check is replaced by two independent bound checks
  (necessary, because Velo's sparse and dense estimators are not numerically equal), and the
  iteration budget is 100 000 instead of 10 000 000.
- **`e9c081bf` (registry count) — ACCEPTED.** `size == 214` matches the registry, the three
  presence assertions pass, the test is green.
- **`1aba0e89` (docs) — ACCEPTED** with the coverage-table nit above.

## Residual Risk

Accepted with the following residual risks (none blocks the fixes):

1. **HLL hash divergence from Redis.** Velo's HLL is `com.github.prasanthj:hyperloglog:1.1`
   (Murmur3, p=14 → 16384 registers, the same register count as Redis), but a different hash
   function and a different sparse layout (a 25-bit `pPrime` keyed sparse map instead of Redis'
   byte-level sparse encoding). `PFDEBUG GETREG` / `PFDEBUG DECODE` are therefore **format
   compatible but value divergent** from Redis: the register values for the same elements differ,
   and the sparse → dense conversion path differs. This is documented in
   `REDIS_COMMANDS_NOT_IMPLEMENTED.md`; tools that compare Velo's register dump with Redis'
   register dump will see different numbers (only the zero/non-zero structure is comparable).
   `PFDEBUG DECODE` also emits `v:<val>,<len>` with `val` up to 63, while Redis' sparse `VAL`
   opcode can only encode 1..32 (`HLL_SPARSE_VAL_MAX_VALUE 32`) — values above 32 are outside the
   Redis opcode range (Redis promotes the object to dense instead).
2. **`PFDEBUG ENCODING` reply type.** Bulk string instead of Redis' simple string (`+sparse`).
   Identical text for `redis-cli`, different RESP type for protocol-level clients. Root cause:
   Velo has no generic status/simple-string reply type (`io.velo.reply` only has `OKReply` and
   `PongReply`). Fixing it properly means adding a `StatusReply` to the reply package and
   converting the other status-reply commands — out of scope for bug 92.
3. **Defensive uncovered paths.** The uncovered lines listed in the JaCoCo section are all
   failure branches (promise rejections, `saveHll` `IOException`, the `TESTFAILED` returns). They
   cannot be triggered from the outside today; a future change that makes e.g. a source read
   throw would silently produce an exception reply instead of a Redis-shaped error.
4. **BITOP big-string sources use the bitcount-family value read path.** `get(s, true)` →
   `getValueBytesByCv()` → `loadBigStringBody()` materialises the whole value in the heap for a
   big-string key (`CompressedValue.SP_TYPE_BIG_STRING` is a "type string", so no WRONGTYPE is
   raised). This is exactly the behaviour of the existing `BITCOUNT` / `BITPOS` commands, so BITOP
   is consistent with its family, but a multi-megabyte source is loaded in one piece instead of
   being streamed — a memory spike risk on the worker thread, inherited from the existing
   commands and not introduced by bug 92.
5. **PFSELFTEST iteration budget.** 100 000 additions instead of Redis' 10 000 000 (runtime
   15 ms, so the 100× budget would also have been affordable — the deviation is a stated design
   choice, not a performance necessity). The statistical power of the check is correspondingly
   lower: the error bound is 6σ, so a 100 k-element run does not exercise the tail of the
   distribution as thoroughly as Redis' 10 M run does.
6. **PFSELFTEST register test is weaker than Redis'.** Redis repeats the dense register
   set/get roundtrip 1000 times (`HLL_TEST_CYCLES`) with a random seed; Velo does it once with a
   fixed seed (`SELF_TEST_SEED`), which makes the command deterministic but also means a
   state-dependent bug in the register array would have a lower chance of being caught.
7. **PFSELFTEST replaces the dense/sparse equality check.** Velo checks both estimates against
   the error bound instead of requiring them to be equal (see the table above); a systematic
   divergence between the sparse and the dense estimator inside the bound would not be detected.
8. **Key-length / value-length limits are Velo-specific.** `KEY_MAX_LENGTH` (256 B) and
   `VALUE_MAX_LENGTH` (1 MB) checks return Velo-specific error texts
   (`ERR key too long (max length is 256)`, `ERR value too long …`) where Redis has no such
   limits for BITOP; the key-length check is also evaluated **before** the source type check, so
   `BITOP AND d <too-long-key> <list-key>` answers `key too long` where Redis answers
   `WRONGTYPE`. Consistent with the rest of Velo (`LGroup.rename`/`copy` check the key lengths
   the same way).
9. **BITOP and PFDEBUG are not replicated/cluster-aware beyond the standard slot routing.** They
   use the generic cross-worker mechanism; a cluster deployment with `clusterEnabled` relies on
   `checkClusterSlot` for the multi-key routing, exactly like `PFMERGE`.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 92.1 | BITOP not implemented | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`996e7e40`) |
| 92.2 | PFDEBUG not implemented | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`1ce0a857`) |
| 92.3 | PFSELFTEST not implemented | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`1ee44833`) |
