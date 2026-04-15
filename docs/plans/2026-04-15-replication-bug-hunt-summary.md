## Replication Bug Hunt Summary

Date: 2026-04-15

### Scope

This pass stayed focused on live replication behavior, first in `XGroup`, then in adjacent repl transport/state code.

Main areas reviewed:

- `src/main/java/io/velo/command/XGroup.java`
- `src/main/java/io/velo/repl/TcpClient.java`
- `src/main/java/io/velo/repl/Binlog.java`
- repl content encoding / decoding paths

### Repeated Bug Pattern

Most real bugs followed one of these shapes:

- malformed repl frame accepted after only partial validation
- error reply returned after state had already been mutated
- stale master state snapshot used to build a repl handshake reply
- invalid position comparison on `(fileIndex, offset)` pairs
- unchecked request payload trailing bytes being ignored as success

The safest handling rule turned out to be:

1. parse the whole payload
2. validate lengths, counts, flags, and trailing bytes
3. decode into temporary values
4. only then apply side effects

### Key Fixes From This Pass

Recent fixes already committed and pushed:

- `3dfe588` `fix: validate repl catch up apply order`
  - `XGroup.s_catch_up(...)` now fully decodes the fetched segment before advancing slave catch-up state.

- `09df63b` `fix: use updated binlog offset in hi`
  - `hello(...)` now builds the `hi` reply after appending `XSkipApply`, so the advertised binlog offset matches actual master state.

- `be7fb91` `fix: compare repl hi positions correctly`
  - `hi(...)` now compares `(fileIndex, offset)` lexicographically instead of using two independent `>=` checks.

Earlier in the same XGroup-focused run, additional committed fixes covered:

- malformed handshake payload validation
- malformed `exists_*` request and reply payload validation
- malformed `catch_up` request validation
- apply-order bugs in `s_exists_dict(...)`, `s_exists_big_string(...)`, and `s_catch_up(...)`
- slave socket close behavior on repl errors

### Verification Approach

Each fix was verified with focused tests and JaCoCo branch checks instead of a full-suite run.

Typical commands used:

```bash
./gradlew :test --tests "io.velo.command.XGroupTest.test as master" --rerun-tasks
./gradlew :test --tests "io.velo.command.XGroupTest.test as slave" --rerun-tasks
./gradlew :test --tests "io.velo.repl.TcpClientTest.test null repl reply closes socket"
./gradlew :test --tests "io.velo.repl.BinlogTest.test append"
```

Useful JaCoCo targets:

```text
build/reports/jacocoHtml/io.velo.command/XGroup.java.html
build/reports/jacocoHtml/io.velo.repl/TcpClient.java.html
build/reports/jacocoHtml/io.velo.repl/Binlog.java.html
```

### Current Status

`XGroup.java` no longer has another confirmed reproduced live bug from the same malformed-frame / state-advance pattern. Remaining suspicious spots looked more like protocol-semantics questions or broader hardening work than the same class of confirmed bug.

The next bug hunt should stay in the replication module outside `XGroup`, especially:

- repl transport / framing: `Repl`, `ReplRequest`, `ReplDecoder`, `TcpClient`
- incremental apply content under `src/main/java/io/velo/repl/incremental`
- binlog decoding and state transitions in `Binlog`

### Notes

- There is unrelated workspace dirt in this repo. Keep future bug-fix commits narrowly scoped.
- Do not include unrelated local changes such as `src/main/groovy/io/velo/test/tools/VeloServer.groovy` or local-only config/doc files unless explicitly asked.
