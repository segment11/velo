# Bug Review State

Single source of truth for report id allocation and module coverage.
**Agents MUST read and update this file when creating any new report.**

## next_bug_id

79

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
| 78 | HGroup commands (all H* hash commands) | 1 | fix phase complete: 14/14 fixed in 14 commits (`3ea4a660`, `82a7a94a`, `e0dacf6f`, `384c1d68`, `4d824b3e`+`d5d1c510`+`09a950ce`, `9b3400a8`, `be38bc96`, `c3d53159`, `b8c92730`, `12c82ec1`, `bf08c7cd`, `8cae3063`, `f26e5f85`, `259f9e37`) + refactor `607db5ad` (use the new `getShortType`); re-review pending. Refuted sub-claims kept unchanged: 78.9 HDEL-on-expired-field (Redis also answers 1), 78.12 "numFields 0 accepted" (Velo answers `format`), 78.3 `inf`/`nan` literals (already rejected), and the 78.5 premise "Redis clears the field TTL on HINCRBY" (upstream keeps it) |

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
| HGroup (all H* hash commands) | bug_78 round 1 | fix phase complete — 14/14 fixed in 14 commits (RED→GREEN+JaCoCo per commit), re-review pending; the fix log is in the report |
| types / config / metrics / cluster / net / compression / commands (string/hash/list/set/zset/geo/hll) | see doc/bug_reviews/ | check reports |

## Known id collisions (historical, do not propagate)

- bug_59: task scheduler AND cluster
- bug_60: info command AND cluster
- bug_32: multithreading AND persist data flow

When citing colliding ids, always qualify with the full file name.
