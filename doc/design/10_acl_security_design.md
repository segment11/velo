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

## ACL Source Of Truth

ACLs are loaded from the file referenced by `ValkeyRawConfSupport.aclFilename`.
`AclUsers.loadAclFile()` parses the file, ignores comment lines, and requires that the default user exists.

`ConfForGlobal.checkIfValid()` can also seed the default user password from configuration.

## Rule Model

The main rule/domain classes are:

- [`U`](/home/kerry/ws/velo/src/main/java/io/velo/acl/U.java)
- [`RCmd`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RCmd.java)
- [`RKey`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RKey.java)
- [`RPubSub`](/home/kerry/ws/velo/src/main/java/io/velo/acl/RPubSub.java)
- [`Category`](/home/kerry/ws/velo/src/main/java/io/velo/acl/Category.java)

These model command permissions, key-pattern permissions, and pub/sub permissions.

## Authentication Flow

At runtime:

1. a client authenticates through command handling, mainly in [`AGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/AGroup.java)
2. the authenticated username is stored on the socket through socket-inspector helpers
3. command execution resolves the current `U` from the socket
4. command handlers can enforce ACL checks against that user

## Replication Of ACL Changes

ACL updates are also represented in replication/binlog code through
[`XAclUpdate`](/home/kerry/ws/velo/src/main/java/io/velo/repl/incremental/XAclUpdate.java),
which is why ACL is not only a local configuration concern.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
