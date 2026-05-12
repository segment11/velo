# Velo ACL Security Design

## Overview

ACL support is implemented under
[`src/main/java/io/velo/acl`](/home/kerry/ws/velo/src/main/java/io/velo/acl)
and integrated into command handling through
[`BaseCommand.getAuthU(...)`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java).

## User Registry

[`AclUsers`](/home/kerry/ws/velo/src/main/java/io/velo/acl/AclUsers.java) is a singleton with per-thread
`Inner` registries. This mirrors the server's worker-thread model:

- each slot worker gets an `Inner`
- reads can use the thread-local copy
- mutations are coordinated through the owning event loop

### Pre-Initialization Staging

`AclUsers` supports a `preInitInner` staging inner that is used before slot-worker event loops exist.
`ConfForGlobal.checkIfValid()` may call `AclUsers.upInsert()` during early startup before
`initBySlotWorkerEventloopArray()` is called. The staging inner captures these writes and transfers
them into the real worker inners during initialization.

### Read vs Write Thread Access

Two methods resolve the current thread's inner:

- `getInner()` — returns the thread-owned inner, or falls back to `inners[0]` for non-worker threads
  (e.g., network workers). The fallback is intended for read-only access.
- `getOwnedInner()` — returns the thread-owned inner only, or `null` for non-worker threads.
  All mutation methods (`upInsert`, `delete`, `replaceUsers`) use this to avoid cross-thread writes.

### Mutation Fan-Out

`changeUser()` routes a mutation callback to all inners except the current thread's inner (the caller
already applied it). This prevents double-application on the owning thread. Non-owner threads have
their mutations scheduled on the target event loop.

### Default User Protection

The `default` user cannot be deleted via `ACL DELUSER`. This is enforced in
[`AGroup.acl()`](/home/kerry/ws/velo/src/main/java/io/velo/command/AGroup.java).

## ACL Source Of Truth

ACLs are loaded from the file referenced by `ValkeyRawConfSupport.aclFilename`.
`AclUsers.loadAclFile()` parses the file, ignores comment lines (starting with `#`), and requires
that the default user exists.

`ConfForGlobal.checkIfValid()` can also seed the default user password from configuration. This
happens before `initBySlotWorkerEventloopArray()`, so the password is stored in the staging inner
and transferred during initialization.

## Rule Model

The main rule/domain classes are:

- [`U`](/home/kerry/ws/velo/src/main/java/io/velo/acl/U.java) — user identity, passwords, and rule lists
- [`RCmd`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RCmd.java) — command allow/deny rules (per-command,
  per-category, per-subcommand, or all)
- [`RKey`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RKey.java) — key-pattern permissions with
  read/write/read-write access mode selectors (`%R~`, `%W~`, `%RW~`, `~`)
- [`RPubSub`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RPubSub.java) — pub/sub channel-pattern
  permissions
- [`Category`](/home/kerry/ws/velo/src/main/java/io/velo/acl/Category.java) — command category definitions
  and write/read-write command classification

These model command permissions, key-pattern permissions, and pub/sub permissions.

### Password Handling

- Passwords are stored as either plain text or SHA-256 hex encoded.
- `U.literal()` always serializes plain passwords as `#<sha256hex>` for the ACL file.
- `U.addPassword()` makes password modes mutually exclusive: adding `nopass` clears existing passwords,
  and adding a real password removes any existing `nopass` sentinel.
- Password comparison uses `MessageDigest.isEqual()` for constant-time verification.

### Key Access Modes

Key rules support read/write access mode selectors:

- `~pattern` — read-write (default)
- `%R~pattern` — read-only
- `%W~pattern` — write-only
- `%RW~pattern` — explicit read-write

`U.checkCmdAndKey()` derives the command's access mode from `Category.isWriteCmd()` and
`Category.isReadWriteCmd()` (which also handles `SET ... GET`). Mixed read-write commands
(e.g., `GETDEL`, `LPOP`, `RPOPLPUSH`) require both read and write key selectors.

### ACL Rule Semantics

- Command allow rules: OR across rules — any matching allow rule grants access.
- Command deny rules: any matching deny rule overrides allow.
- Key patterns: OR across rules — each requested key must match at least one allow rule.
- Channel patterns: OR across rules — each requested channel must match at least one allow rule.
- Commands with no category mapping are denied by category rules (fail-closed).

## Authentication Flow

There are three independent authentication paths:

### RESP AUTH Command

Handled directly in [`RequestHandler.handle()`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)
(lines ~448–471), before any command group is dispatched. The `AUTH` command is a special case that
bypasses the normal A–Z group dispatch (see [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)).

### RESP HELLO with AUTH Subcommand

Handled in [`HGroup.hello()`](/home/kerry/ws/velo/src/main/java/io/velo/command/HGroup.java)
(lines ~312–338). The `HELLO 3 AUTH user password` form performs RESP3 negotiation and authentication
in one step. The `hello` command is exempted from the password-required gate in
`MultiWorkerServer.handleRequest()` so unauthenticated clients can use it.

### HTTP Basic Auth

Handled in [`RequestHandler.handle()`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)
(lines ~404–446 for normal commands, lines ~289–328 for special one-token HTTP endpoints). Health-check
endpoints (`?master`, `?slave`, etc.) are exempted from authentication by design.

### Common Flow After Auth

All three paths store the authenticated username on the socket via `SocketInspector.setAuthUser()`.
Subsequent request processing:

1. `MultiWorkerServer.handleRequest()` calls `BaseCommand.getAuthU(socket)` to resolve the current `U`.
   For unauthenticated sockets, this returns the `default` user from the ACL registry.
2. `Request.isAclCheckOk()` checks whether the user is enabled and whether the command and keys
   pass the user's ACL rules.
3. `ACL SETUSER` uses a temp-copy-then-apply pattern (`U.copyStateFrom()`) to ensure failed rules
   do not partially mutate the live user.

## ACL Subcommands

[`AGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/AGroup.java) handles the `ACL` command
and its subcommands: `CAT`, `DELUSER`, `DRYRUN`, `GENPASS`, `GETUSER`, `LIST`, `LOAD`, `LOG`, `SAVE`,
`SETUSER`, `USERS`, `WHOAMI`.

### ACL LOG

ACL denials are recorded in a bounded circular buffer (128 entries) in `AclUsers`. Entries include
the reason (command or key), the denied command, the username, and the client address. `ACL LOG <count>`
returns the most recent entries. `ACL LOG RESET` clears the buffer. The `ACL LOG` count validation
accepts values between 1 and 100.

### ACL SETUSER Atomicity

`ACL SETUSER` validates all rules on a temporary `U` copy before applying to the live user. If any
rule is invalid, the entire operation fails and the user's previous state is preserved. Both
`AclInvalidRuleException` and `IllegalArgumentException` are caught and returned as
`ACL_SETUSER_RULE_INVALID`.

## Replication Of ACL Changes

ACL updates are represented in replication/binlog code through
[`XAclUpdate`](/home/kerry/ws/velo/src/main/java/io/velo/repl/incremental/XAclUpdate.java),
which is why ACL is not only a local configuration concern.

`ACL SETUSER`, `ACL DELUSER`, and `ACL SAVE` all call `appendAclUpdateBinlog()` which appends the
raw command line to the binlog. On the replica, `XAclUpdate.apply()` replays the command through
`AGroup.execute()`.

`ACL LOAD` replicates loaded users as individual `SETUSER` lines. Note that users removed from the
ACL file are not explicitly deleted on replicas — only the loaded users are upserted. This means
replicas may retain stale users that were deleted on the master until those users are explicitly
deleted via `ACL DELUSER` on the master.

## Corrections To Older Versions

- The `AUTH` command is handled in `RequestHandler`, not in `AGroup`. `AGroup` handles `ACL`
  subcommands only.
- The `HELLO ... AUTH` path lives in `HGroup.hello()`, not in `AGroup`.
- HTTP Basic auth is handled in `RequestHandler` with two separate gates (one for special endpoints,
  one for normal commands).
- The design previously said "a client authenticates through command handling, mainly in AGroup."
  This was inaccurate; only ACL administration lives in AGroup.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Configuration](/home/kerry/ws/velo/doc/design/08_configuration_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
