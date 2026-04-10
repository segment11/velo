# Bug Hunt Handoff

Date: 2026-04-10

## Scope Completed

This pass stayed focused on active protocol and replication paths, especially:

- `XGroup`
- `TcpClient`
- `Binlog`
- repl content / incremental repl payload encoding
- malformed protocol handling in RESP / HTTP / repl frames

ASCII-only charset issues were intentionally skipped when the payload contract is clearly ASCII-only.

## Recently Fixed

Recent replication/protocol fixes already committed and pushed:

- `0f05c35` `fix: reject past eof repl offset`
  - `Binlog.readPrevRafOneSegment(...)` now distinguishes exact EOF from past EOF.
  - `x_catch_up` no longer treats past-EOF requests as successful catch-up.

- `7d8b048` `fix: validate repl segment length`
  - `XGroup.s_exists_chunk_segments(...)` no longer relies on `assert`.
  - malformed `segmentLength` now returns repl error instead of being accepted.

- `6f3b160` `fix: close socket on null repl reply`
  - slave-side `TcpClient` now explicitly closes the socket when `xGroup.handleRepl(...)` returns `null`.
  - this makes the existing `hi()` illegal-data path actually close the connection.

Earlier fixes from the same replication-focused pass:

- `2d81d7a` `fix: use utf8 for repl text`
- `c81eb9b` `fix: use utf8 for acl repl`
- `dae5d37` `fix: use utf8 for dict repl`
- `ed80f41` `fix: keep big string repl payload`
- `7013a0a` `fix: skip missing big string repl ids`

## What Was Verified

Focused tests and JaCoCo were used for each fix instead of running the full suite.

Useful recent test commands:

```bash
./gradlew :test --tests "io.velo.command.XGroupTest.test as slave"
./gradlew :test --tests "io.velo.repl.BinlogTest.test append"
./gradlew :test --tests "io.velo.repl.TcpClientTest.test null repl reply closes socket"
```

Useful JaCoCo HTML targets:

```text
build/reports/jacocoHtml/io.velo.command/XGroup.java.html
build/reports/jacocoHtml/io.velo.repl/Binlog.java.html
build/reports/jacocoHtml/io.velo.repl/TcpClient.java.html
```

## Current Direction

The next session should continue on live replication-state and malformed-frame handling, not on ASCII-only cleanup.

The main pattern that still finds real bugs is:

- `assert` protecting a runtime protocol invariant
- `return null` conflating “normal/no data” with malformed state
- invalid repl state being treated as success or skip

## Next Targets

### 1. Continue `XGroup` receive-path validation

Look for remaining production `assert` / unchecked frame reads in active slave receive methods:

- `s_exists_short_string(...)`
- `s_exists_dict(...)`
- `s_catch_up(...)`
- command helper path for `x_repl x_catch_up`

Good first grep:

```bash
rg -n "assert |slice\\.readInt|buffer\\.getInt|buffer\\.getLong|return null;" \
  src/main/java/io/velo/command/XGroup.java
```

### 2. Continue binlog / catch-up state review

Keep checking places where stale or impossible state can still be accepted:

- `Binlog`
- `ReplPair`
- `XGroup.catch_up(...)`
- `XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(...)`

Good first grep:

```bash
rg -n "return null;|allCaughtUp|needFetchOffset|lastUpdatedOffset|readSegmentBytes == null|assert " \
  src/main/java/io/velo/repl src/main/java/io/velo/command/XGroup.java
```

### 3. Re-check slave/master close behavior

Now that `TcpClient` closes on `null` repl replies, verify there are no other comments or call sites still assuming “return null closes connection” without an explicit close.

Good first grep:

```bash
rg -n "return null;|close when return null|illegal data" \
  src/main/java/io/velo/repl src/main/java/io/velo/command/XGroup.java src/main/java/io/velo/MultiWorkerServer.java
```

## Low Priority / Skip For Now

Do not spend time first on:

- ASCII-only command/path charset cleanup
- dormant placeholder code that is registered but not emitted in live flow, unless usage is proven
- broad refactors of repl architecture without a concrete failing case

## Workspace Notes

There is unrelated local dirt in the repo. Do not include it in bug-fix commits unless explicitly asked.

Known examples:

- `src/main/groovy/io/velo/test/tools/VeloServer.groovy`
- various untracked docs, local config files, and test helpers

Keep future commits narrowly scoped to the bug under test.
