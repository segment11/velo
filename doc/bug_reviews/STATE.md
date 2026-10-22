# Bug Review State

Single source of truth for report id allocation and module coverage.
**Agents MUST read and update this file when creating any new report.**

## next_bug_id

75

## Allocated

| Id | Module | Rounds | Status |
|---|---|---|---|
| 1–70 | see doc/bug_reviews/ | various | closed |
| 70 | scan command (SGroup) | 1 | review complete, fix phase pending |
| 71 | hscan/sscan (HGroup/SGroup) | 1 | 71.3 fixed (ebbc7a2c), 71.4 fixed (78aca093); 71.1 REFUTED no action; 71.2/71.5 open, need product decision |
| 72 | BGroup commands | 1 | all findings fixed: 72.1–72.3 + 72.5–72.10 in 9 commits, 72.11 fixed (`b00a7354`), 72.4 retracted; full re-review pending |
| 73 | CGroup commands | 1 | 7 findings fixed + accepted; follow-ups A1 `94db1fa4` and R1 hardened by `00cbbc54`/`fbf3e06c`/`d0e37a45`/`31c3abc6`; verified e2e against a 2-slot/2-net worker server (all 5 cases pass, leak case red on the pre-fix jar); open: pipelined self-kill replies, PING/ECHO bypass CLIENT REPLY OFF |
| 74 | DGroup commands (dbsize/debug/decr/decrby/decrbyfloat/del/dump) | 1 | closed: 9/9 CONFIRMED, fixed in 9 commits (`fe48c5f1`…`39f4dd60`), re-review ACCEPTED (red/green + JaCoCo independently reproduced); follow-ups: lossy `SET` number normalisation (`BaseCommand.set()`), `HGroup.hincrby2` non-finite float, `HGroup.hincrby` int32 delta |

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
| types / config / metrics / cluster / net / compression / commands (string/hash/list/set/zset/geo/hll) | see doc/bug_reviews/ | check reports |

## Known id collisions (historical, do not propagate)

- bug_59: task scheduler AND cluster
- bug_60: info command AND cluster
- bug_32: multithreading AND persist data flow

When citing colliding ids, always qualify with the full file name.
