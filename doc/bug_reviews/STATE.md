# Bug Review State

Single source of truth for report id allocation and module coverage.
**Agents MUST read and update this file when creating any new report.**

## next_bug_id

81

## Allocated

| Id | Module | Rounds | Status |
|---|---|---|---|
| 1–70 | see doc/bug_reviews/ | various | closed |
| 70 | scan command (SGroup) | 1 | review complete, fix phase pending |
| 71 | hscan/sscan (HGroup/SGroup) | 1 | 71.3 fixed (ebbc7a2c), 71.4 fixed (78aca093); 71.1 REFUTED no action; 71.2/71.5 open, need product decision |
| 72 | BGroup commands | 1 | all findings fixed: 72.1–72.3 + 72.5–72.10 in 9 commits, 72.11 fixed (`b00a7354`), 72.4 retracted; full re-review pending |
| 73 | CGroup commands | 1 | 7 findings fixed + accepted; follow-ups A1 `94db1fa4` and R1 hardened by `00cbbc54`/`fbf3e06c`/`d0e37a45`/`31c3abc6`; verified e2e against a 2-slot/2-net worker server (all 5 cases pass, leak case red on the pre-fix jar); open: pipelined self-kill replies, PING/ECHO bypass CLIENT REPLY OFF |
| 74 | DGroup commands (dbsize/debug/decr/decrby/decrbyfloat/del/dump) | 1 | closed: 9/9 CONFIRMED, fixed in 9 commits (`fe48c5f1`…`39f4dd60`), re-review ACCEPTED (red/green + JaCoCo independently reproduced); follow-ups: lossy `SET` number normalisation (`BaseCommand.set()`), `HGroup.hincrby2` non-finite float, `HGroup.hincrby` int32 delta |
| 75 | EGroup commands (echo/exists/expire/expireat/expiretime) | 1 | fix phase complete: 6/6 CONFIRMED, 6 fixed in 8 commits (`bad9cf26`, `ff6c55c2`, `37ab2b4d`, `ee014c93`, `9bf87c44`, `e20e5aa5`, `71c87daa`, `126e013f`), re-review pending |
| 76 | FGroup commands (failover/flushdb/flushall) | 1 | closed: 7/7 fixed in 6 commits (`cebb1fe5`, `15cbd60d` incl. 76.5, `33b3f472`, `df62b0c5`, `3a118894`, `82470d2c`) + style `25e71691`; re-review ACCEPTED (agent 2: RED re-derived per commit, focused tests green, JaCoCo confirmed, live Velo-vs-Redis 7.2.11 differential); follow-up: failover exception path skips the rollback |
| 77 | GGroup commands (getbit/getdel/getex/getrange/getset/geo*) | 1 | fix phase complete: 9/10 fixed in 9 commits (`b720ac57`, `876e7d54`, `0d0958c9`, `ab03c146`, `010cc912`, `c279c722`, `12a70d7d`, `9607153d`, `6458e0c8`); 77.8 won't fix (product decision: GEOSEARCHSTORE dst stays geo-typed); re-review pending; residual: geo distance value divergence (exact vs geohash-decoded coords), big-string TTL-update inline duplication shared with expire |
| 78 | HGroup commands (all H* hash commands) | 1 | closed: 14/14 CONFIRMED/PARTIAL, fixed in 14 commits + 3 refactors (`3ea4a660`…`259f9e37`), **re-review ACCEPTED** (independent red/green — 19 failures on pre-fix sources, 190/190 green at HEAD — JaCoCo confirmed, string module proven untouched); residuals: split `HINCRBY` `Long.MIN_VALUE` delta, exact-format divergence of HINCRBYFLOAT vs Redis, legacy desynced fields from the a6706982→fe48c5f1 window, split keepttl-on-expired-field test gap, style debt |
| 79 | IGroup commands (incr/incrby/incrbyfloat/info/ingest/ingest_sst) | 1 | fix phase complete: 10/10 fixed, one commit each (`8eea029f`, `adf61d28`, `366c5052`, `10be2caf`, `2d650b14`, `da05d94b`, `961283c5`, `6aed7708`, `cd819dd7`, `e596fe5e`) + contract test `97098721`; re-review pending; follow-up: `DECRBY` with `Long.MIN_VALUE` delta is a separate DGroup bug (see report *Additional observations*) |
| 80 | KGroup commands (keys) | 1 | round 1 rewrite accepted (`d8e41ddd`+`db9564fc`); independent re-review 3 then found 80.8-80.11 (same-ms exclusion, cap slot starvation, knob scope, log noise) — all 4 fixed with red/green + JaCoCo: `512b44fb`, `616ef7ac`, `f25391f3`, log extraction `198b52b4`; **independent re-review 4 ACCEPTED all four** (live pipelined differential: pre-fix 67/200 keys found vs fixed 200/200, matching Redis 7.2) — closable |

## Module coverage (latest round per module)

| Module | Latest report | Open items |
|---|---|---|
| replication | bug_68 round 1 | check report |
| persist layer | bug_50/51 round 2 | check report |
| acl | bug_69 round 1 | check report |
| scan | bug_70 round 1 | fix phase pending |
| hscan/sscan | bug_71 round 1 | 71.3/71.4 fixed; 71.2 + 71.5 open (design decision pending) |
| BGroup (bitmap/bloom/blocking-list B commands) | bug_72 round 1 | everything disposed (72.11 fixed in `b00a7354`, 72.4 retracted); re-review pending |
| CGroup (client/command/config/copy C commands) | bug_73 round 1 | fixed + re-reviewed (3 rounds) + R1 hardened (4 commits); all accepted; open: pipelined self-kill replies, `handleFast` ignoring CLIENT REPLY OFF, gradle daemon heap |
| DGroup (dbsize/debug/decr/decrby/decrbyfloat/del/dump) | bug_74 round 1 | closed — 9/9 fixed, re-review ACCEPTED; follow-ups in string/hash modules (see report Residual Risk) |
| EGroup (echo/exists/expire/expireat/expiretime) | bug_75 round 1 | all 6 findings fixed (75.1 GT-on-no-TTL, 75.6 type-marker corruption, 75.2 unsupported option, 75.3 expire-time overflow, 75.4 expireat-0 delete, 75.5 exists key-length check); re-review pending |
| FGroup (failover/flushdb/flushall) | bug_76 round 1 | closed — 7/7 fixed, re-review ACCEPTED; open follow-up: R1 failover exception path skips the rollback (fenced slots stay readonly); residual: FAILOVER still a partial implementation (no promote, TO/TIMEOUT unsupported by design) |
| GGroup (getbit/getdel/getex/getrange/getset/geo*) | bug_77 round 1 | 9/10 fixed (9 commits, RED→GREEN+JaCoCo per commit), 77.8 won't fix (product decision); re-review pending; residuals in report Residual Risk |
| HGroup (all H* hash commands) | bug_78 round 1 | closed — 14/14 fixed, re-review ACCEPTED; residuals in report Residual Risk (Long.MIN_VALUE delta, HINCRBYFLOAT exact-format divergence, legacy desynced fields, one keepttl test gap, style debt) |
| IGroup (incr/incrby/incrbyfloat/info/ingest/ingest_sst) | bug_79 round 1 | fix phase complete — 10/10 fixed (RED→GREEN+JaCoCo per commit, one commit each); re-review pending; follow-up: DGroup `DECRBY` `Long.MIN_VALUE` delta divergence; hash module proven untouched (HGroupTest 57/57) |
| KGroup (keys) | bug_80 round 1 | 80.1-80.7 fixed by rewrite (accepted with Redis 7.2.11 differential); 80.8-80.11 from independent re-review 3 also fixed (`512b44fb`, `616ef7ac`, `f25391f3`, `198b52b4`) and **independently re-reviewed + ACCEPTED in re-review 4**; residual: prefixMatch dead code (cleanup candidate), defensive exception branches uncovered |
| types / config / metrics / cluster / net / compression / commands (string/hash/list/set/zset/geo/hll) | see doc/bug_reviews/ | check reports |

## Known id collisions (historical, do not propagate)

- bug_59: task scheduler AND cluster
- bug_60: info command AND cluster
- bug_32: multithreading AND persist data flow

When citing colliding ids, always qualify with the full file name.
