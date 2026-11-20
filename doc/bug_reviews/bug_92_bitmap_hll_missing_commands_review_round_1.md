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
| 92.2 PFDEBUG | `PGroupTest.test pfdebug` RED at the first `then:` — got `unknown command 'pfdebug'` expected `ErrorReply.FORMAT` | `1ce0a857` `feat: implement pfdebug command (bug 92.2)` | green (`PGroupTest` 23/23 → 24/24) | pfdebug lines 664-800: 63/68 covered; uncovered = the two `saveHll` IOException catches (752-753, 764-765) and the empty-opcode-string guard (713), all defensive |
| 92.3 PFSELFTEST | `PGroupTest.test pfselftest` RED — `unknown command 'pfselftest'`; second RED `TESTFAILED sparse encoding not used` before the 1500 bound fix | `1ee44833` `feat: implement pfselftest command (bug 92.3)` | green (`PGroupTest` 24/24) | pfselftest lines 814-891: arity + happy path + `selfTestMaxerrExceeded` failure/success branches covered; uncovered (849/863/869/886) = TESTFAILED internal branches that cannot fire with a correct implementation (Redis' own suite cannot cover them either) |
| registry count | — | `e9c081bf` `test: bump command registry count for bitop/pfdebug/pfselftest` (211 → 214 + presence assertions) | green (`CommandRegistryPopulationTest`) | n/a (test only) |

Final regression: `io.velo.command.*` — **590 tests, 0 failures** (2026-11-20).
Field access check: `scripts/test_field_access_check.sh BGroupTest` / `PGroupTest` — 0 issues.

## Re-review (AI agent 2)

## Residual Risk

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 92.1 | BITOP not implemented | Medium | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`996e7e40`) |
| 92.2 | PFDEBUG not implemented | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`1ce0a857`) |
| 92.3 | PFSELFTEST not implemented | Low | Redis protocol compatibility | CONFIRMED (reproduced) | fixed (`1ee44833`) |
