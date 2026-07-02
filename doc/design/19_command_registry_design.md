# Velo Command Registry & COMMAND Implementation Design

## Overview

Redis exposes a `COMMAND` family of subcommands that let clients introspect the
server's command set: how many commands exist (`COMMAND COUNT`), their names
(`COMMAND LIST`), structured metadata (`COMMAND INFO`), documentation
(`COMMAND DOCS`), key extraction (`COMMAND GETKEYS`), and so on. These are relied
on by `redis-cli`, cluster-aware clients, and tooling.

Velo today implements `COMMAND` as a hot-reloadable Groovy class
[`CommandCommand`](/home/kerry/ws/velo/dyn/src/io/velo/command/CommandCommand.groovy)
dispatched from [`CGroup`](/home/kerry/ws/velo/src/main/java/io/velo/command/CGroup.java).
However it only fully implements `GETKEYS`; the remaining subcommands are stubs:

- `COUNT` returns a hardcoded `241`
- `LIST`, `INFO`, `DOCS` return `MultiBulkReply.EMPTY`
- `HELP` returns a single bulk string
- `GETKEYSANDFLAGS` returns empty key flags

The root cause is structural: Velo has **no central command registry**. Command
names are scattered as `"foo".equals(cmd)` checks across the 26 group classes
`AGroup`–`ZGroup`, so there is nothing to enumerate.

This document proposes a lightweight **command registry** populated by `static {}`
blocks inside each group class, then consumed by `CommandCommand` to produce
Redis-compatible replies for the full `COMMAND` subcommand set.

## Goals

1. Redis-compatible output for `COMMAND`, `COMMAND COUNT`, `COMMAND LIST`,
   `COMMAND INFO`, `COMMAND DOCS`, `COMMAND HELP`, `COMMAND GETKEYS`,
   `COMMAND GETKEYSANDFLAGS`.
2. Command metadata co-located with the group that owns each command (low
   drift, easy to maintain).
3. No change to the existing dispatch / slot-parsing model.

## Non-Goals (v1)

- Full **key specs** (`begin_search` / `find_keys` objects). v1 emits the legacy
  `firstkey/lastkey/keystep` triple, which is what virtually all clients read.
  The key specs array is returned empty; can be filled later.
- **ACL filtering** of the command list (Redis omits commands the caller cannot
  use). v1 returns all commands regardless of the caller's ACL.
- **Subcommands** as nested entries (e.g. `CLIENT GETNAME`). Redis supports
  subcommands in COMMAND metadata using pipe-separated full names such as
  `client|getname`, `config|get`, and `command|info`; v1 returns an empty
  subcommands array and lists only the container commands. Full subcommand
  registry support is deferred to a later stage.
- **Argument documentation** objects in `COMMAND DOCS`. v1 omits the `arguments`
  field unless trivially available.

## Background: How Redis Does It

Redis stores every command's metadata in per-command JSON files under
`src/commands/*.json` (e.g.
[`command-info.json`](/home/kerry/ws/redis/src/commands/command-info.json)). These
are compiled into the `struct redisCommand` table (`server.h`). The relevant
serializer functions are:

- `commandCommand` ([server.c:5040](/home/kerry/ws/redis/src/server.c)) — no args,
  iterates `server.commands`.
- `commandCountCommand` ([server.c:5053](/home/kerry/ws/redis/src/server.c)) —
  `dictSize(server.commands)`.
- `commandListCommand` ([server.c:5138](/home/kerry/ws/redis/src/server.c)) — names,
  optional `FILTERBY MODULE|ACLCAT|PATTERN`.
- `commandInfoCommand` ([server.c:5180](/home/kerry/ws/redis/src/server.c)) — per
  command or all.
- `commandDocsCommand` ([server.c:5201](/home/kerry/ws/redis/src/server.c)) — map
  name→docs.
- `addReplyCommandInfo` ([server.c:4890](/home/kerry/ws/redis/src/server.c)) — the
  10-element array.
- `addReplyCommandDocs` ([server.c:4918](/home/kerry/ws/redis/src/server.c)) — the
  docs map.

Flag / category vocabularies:

- Command flags: `write`, `readonly`, `denyoom`, `admin`, `pubsub`, `noscript`,
  `blocking`, `loading`, `stale`, `fast`, `no_auth`, `no_multi`, `movablekeys`,
  ... ([server.h:206](/home/kerry/ws/redis/src/server.h), surfaced at
  [server.c:4577](/home/kerry/ws/redis/src/server.c)).
- ACL categories (surfaced as `@name`): `@keyspace`, `@read`, `@write`, `@set`,
  `@sortedset`, `@list`, `@hash`, `@string`, `@bitmap`, `@hyperloglog`, `@geo`,
  `@stream`, `@pubsub`, `@admin`, `@fast`, `@slow`, `@blocking`, `@dangerous`,
  `@connection`, `@transaction`, `@scripting`
  ([acl.c:65](/home/kerry/ws/redis/src/acl.c)).
- Key flags for `GETKEYSANDFLAGS`: `RW`, `RO`, `OW`, `RM`, `access`, `update`,
  `insert`, `delete` ([server.c:4618](/home/kerry/ws/redis/src/server.c)).

## Current State in Velo

- Dispatch: [`RequestHandler`](/home/kerry/ws/velo/src/main/java/io/velo/RequestHandler.java)
  instantiates all 26 group classes in `initCommandGroups()` (line 129) and
  selects one per request by the command's first byte (line 172).
- `COMMAND` flows: `CGroup.handle()` → `command()` (CGroup.java:496) loads the
  Groovy script [`CommandCommandHandle.groovy`](/home/kerry/ws/velo/dyn/src/io/velo/script/CommandCommandHandle.groovy)
  which constructs a `CommandCommand` and calls `handle()`.
- Key positions are already expressed in each group's `parseSlots(...)` via the
  `FromToKeyIndex` helpers in [`BaseCommand`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java)
  (line 40): `KeyIndexBegin1` (1..end step 1), `KeyIndexBegin1Step2` (1..end
  step 2), `KeyIndexBegin2` (2..end step 1), `KeyIndexBegin2Step2` (2..end
  step 2).
- `GETKEYS` already works: it delegates to the target group's `parseSlots`
  (`CommandCommand.groovy:94`). Only the per-key flags in
  `GETKEYSANDFLAGS` are missing.
- The codebase currently exposes ~172 distinct command names across the 26
  groups (derived from the `equals(cmd)` checks).

## Design

### Component 1: `CommandEntry` record

New file
`src/main/java/io/velo/command/CommandEntry.java`. A Java record (stable, not
hot-reloadable) mirroring only the fields the `COMMAND` replies actually emit.

```java
public record CommandEntry(
        String name,
        int arity,                  // > 0 = exact argc; < 0 = at least |arity|
        Set<String> flags,          // "write","readonly","fast",... (lowercase)
        int firstKey,               // index of first key arg, or -1 if none
        int lastKey,                // last key index, or -1 meaning "to end"
        int keyStep,                // distance between consecutive keys
        Set<String> aclCategories,  // "@read","@write","@string",...
        String group,               // "string","hash","list","server",...
        String summary,
        String since,
        String complexity           // nullable; null = omitted from DOCS
) { }
```

Rationale for the fields:

| Field            | Used by            | Redis source            |
|------------------|--------------------|-------------------------|
| `name`           | INFO, DOCS, LIST   | `cmd->fullname`         |
| `arity`          | INFO[1]            | `cmd->arity`            |
| `flags`          | INFO[2]            | `addReplyFlagsForCommand` |
| `firstKey`       | INFO[3]            | legacy range `bs.index.pos` |
| `lastKey`        | INFO[4]            | legacy range `fk.range.lastkey` |
| `keyStep`        | INFO[5]            | legacy range `fk.range.keystep` |
| `aclCategories`  | INFO[6]            | `addReplyCommandCategories` |
| `group`          | DOCS               | `commandGroupStr`       |
| `summary`        | DOCS               | `cmd->summary`          |
| `since`          | DOCS               | `cmd->since`            |
| `complexity`     | DOCS               | `cmd->complexity`       |

### Component 2: `CommandRegistry`

New file `src/main/java/io/velo/command/CommandRegistry.java`.

```java
public final class CommandRegistry {
    // Insertion-ordered; Redis documents NONDETERMINISTIC_OUTPUT_ORDER anyway.
    private static final Map<String, CommandEntry> ALL = new LinkedHashMap<>();

    /** Registers one command. Warns and skips on duplicate name. */
    public static void register(CommandEntry e) { ... }

    public static CommandEntry get(String name) { ... }

    /** All registered entries, insertion order. */
    public static Collection<CommandEntry> all() { ... }

    /** Count, O(1). */
    public static int size() { ... }
}
```

Thread-safety: `register` is only ever called from class-init (`static {}`)
blocks, which happen once per JVM under the class-init lock. Reads after init
are lock-free. A `LinkedHashMap` suffices.

**`register(...)` contract** (must be documented on the method): registration
happens *only* during command-group static initialization, which occurs during
the serial bootstrap in `MultiWorkerServer` (the per-worker `RequestHandler`
loop at `MultiWorkerServer.java:1706`). The registry must not be mutated after
startup; this static-init-only convention should be documented on `register(...)`
with a concise method comment.

Initialization timing: the 26 group classes are instantiated in
`RequestHandler.initCommandGroups()` (per worker thread). Class loading is
triggered by the `new XGroup(...)` call, so by the time any client request is
served the registry is fully populated. `CommandRegistry` is therefore safe to
read from the Groovy `CommandCommand`.

### Component 3: Static registration blocks in each group

Each `XGroup` class gains a `static {}` block registering the commands it owns
(the same names as its `equals(cmd)` checks). Metadata is sourced from Redis's
`src/commands/*.json` so arity / flags / categories / docs match upstream.

Example for `AGroup`:

```java
public class AGroup extends BaseCommand {
    static {
        CommandRegistry.register(new CommandEntry(
                "append", 3,
                Set.of("write", "denyoom", "fast"),
                1, 1, 1,
                Set.of("@write", "@string", "@fast"),
                "string",
                "Appends a value to a key.",
                "2.0.0", "O(1)"));
        CommandRegistry.register(new CommandEntry(
                "asking", 1,
                Set.of("fast", "loading", "stale"),
                -1, -1, 1,
                Set.of("@fast", "@connection"),
                "cluster",
                "Signals that the client is a cluster client.",
                "3.0.0", "O(1)"));
        // acl, exists, expire, expireat, ...
    }
    // ...existing parseSlots / handle unchanged...
}
```

Key-position derivation from existing `parseSlots` patterns:

| parseSlots pattern                  | firstKey | lastKey | keyStep | Example  |
|-------------------------------------|----------|---------|---------|----------|
| single key at index 1               | 1        | 1       | 1       | `get`    |
| key + value at 1,2 (`set`)          | 1        | 1       | 1       | `set`    |
| one key at index 1 + trailing args  | 1        | 1       | 1       | `lrange` |
| `KeyIndexBegin1` (1..end, step 1)   | 1        | -1      | 1       | `mget`   |
| `KeyIndexBegin1Step2` (1..end,2)    | 1        | -1      | 2       | `mset`   |
| no keys / admin command             | -1       | -1      | 1       | `acl`    |

Note: `KeyIndexBegin2` / `KeyIndexBegin2Step2` helpers exist in `BaseCommand`
but are **not used** by any current Velo command, so no index-2 example is
listed. (In Redis such positions are subcommand-prefixed commands like
`MEMORY USAGE`; none apply here yet.)

#### Dynamic-key commands (`movablekeys`)

Some commands cannot be described by a static `firstKey`/`lastKey`/`keyStep`
triple because their key set is driven by a runtime `numkeys` argument. Velo's
`parseSlots` already handles these at runtime (so `COMMAND GETKEYS` keeps
working), but the registry metadata must not lie about their key range. These
are registered with `firstKey = -1`, `lastKey = -1`, `keyStep = 0`, and the
`movablekeys` flag, matching Redis. Confirmed instances in Velo:

| Command group | Commands | numkeys arg index | key start index |
|---|---|---|---|
| `LGroup` | `lmpop` | 1 | 2 |
| `BGroup` | `blmpop` | 1 | 2 |
| `ZGroup` | `zmpop` | 1 | 2 |
| `ZGroup` | `bzmpop` | 2 | 3 |
| `ZGroup` | `zunion`, `zinter`, `zdiff`, `zintercard` | 1 | 2 |
| `ZGroup` | `zunionstore`, `zinterstore`, `zdiffstore` | 2 | 3 |

For these, `COMMAND INFO` reports `movablekeys` in the flags array and a zeroed
legacy triple; `COMMAND GETKEYS`/`GETKEYSANDFLAGS` continue to use the live
`parseSlots` result.

#### Subcommands (next stage)

Redis exposes command subcommands as first-class command metadata entries. Their
full names use a pipe separator between the container and subcommand names, for
example `command|info`, `client|getname`, and `config|get`. `COMMAND INFO
command|info` returns the metadata for the `COMMAND INFO` subcommand; `COMMAND
LIST` includes both container commands and subcommands; `COMMAND DOCS` nests
subcommand docs under the parent command when docs are requested for the parent.

Velo v1 intentionally does not model subcommands: `COMMAND INFO command|info`
returns `NilReply.INSTANCE`, `COMMAND LIST` lists only container command names,
and each `COMMAND INFO` entry returns an empty subcommands array. A later stage
should add a `subcommands` field or child-entry API to `CommandEntry`, register
pipe-separated subcommand full names, include them in `LIST` and `INFO` lookup,
leave `COUNT` as the top-level command count, and populate the nested
`subcommands` section for `COMMAND DOCS`.

### Component 4: Rewrite `CommandCommand` subcommands

All changes stay inside
[`CommandCommand.groovy`](/home/kerry/ws/velo/dyn/src/io/velo/command/CommandCommand.groovy)
(it is hot-reloadable, so iterating on reply shape requires no rebuild). Each
subcommand below reads from `CommandRegistry`.

**`COMMAND` (no args)** — `commandCommand`, server.c:5040.
Return one INFO array per registered command.

**`COMMAND COUNT`** — `commandCountCommand`, server.c:5053.
Return `IntegerReply(CommandRegistry.size())`. Removes the hardcoded `241`.

**`COMMAND LIST [FILTERBY …]`** — `commandListCommand`, server.c:5138.
- No filter: one bulk reply per name, insertion order.
- `FILTERBY ACLCAT <cat>`: keep entries whose `aclCategories` contains
  `@<cat>`.
- `FILTERBY PATTERN <glob>`: keep entries matching the glob (reuse a simple
  glob matcher; `*` and `?`).
- `FILTERBY MODULE <name>`: always empty (Velo has no modules); accepted for
  syntax compatibility.

**`COMMAND INFO [name …]`** — `commandInfoCommand`, server.c:5180.
- No names: INFO array for every command.
- With names: array position per requested name; an unknown name emits
  `NilReply.INSTANCE` (RESP2 `$-1\r\n`, RESP3 `_\r\n`), matching Redis. This
  must be the `NilReply.INSTANCE` singleton, **not** Java/Groovy `null`, because
  `MultiBulkReply.buffer()` calls `reply.buffer()` on each child and a null
  reference would throw an NPE.

**`COMMAND DOCS [name …]`** — `commandDocsCommand`, server.c:5201.
A RESP map (`MultiBulkReply` with `isMap=true`). Each entry: key = command name,
value = a map containing `summary`, `since`, `group`, and `complexity` (when
non-null). Omitted fields are simply not added, matching
`addReplyCommandDocs`'s conditional maplen counting.

RESP2/RESP3 encoding: `MultiBulkReply.buffer()` (RESP2) always emits the `*`
array marker even when `isMap` is set, so RESP2 clients correctly receive an
array of alternating key/value elements; `bufferAsResp3()` emits the `%` map
marker. This is the same down-conversion Redis applies. Tests must assert **both**
wire shapes (`buffer()` shows `*<n>\r\n…`, `bufferAsResp3()` shows `%<n>\r\n…`).

**`COMMAND HELP`** — `commandHelpCommand`, server.c:5237.
Static text from Redis: the 6 subcommand descriptions, formatted as
redis-cli's `addReplyHelp`.

**`COMMAND GETKEYS`** — unchanged; already delegates to `parseSlots`.

**`COMMAND GETKEYSANDFLAGS`** — fix the `todo` at
`CommandCommand.groovy:108`. For each key return `["RW","access"]` (write
commands) or `["RO","access"]` (read commands), derived from the command's
`flags` set. This matches Redis's per-key flags for the common cases.

**v1 is best-effort**: Redis distinguishes `RW`/`RO`/`OW`/`RM` and
`access`/`update`/`insert`/`delete` per key ([server.c:4618](/home/kerry/ws/redis/src/server.c)).
Velo v1 collapses this to read-vs-write only (overwrites like `SET` report `RW`
rather than `OW`). This is acceptable for most clients; refining per-command key
flags to the full Redis set is deferred (tracked in Open Questions).

## Reply Shapes (Exact)

### `COMMAND INFO` per-command array — 10 elements

Per `command-info.json` `reply_schema` and `addReplyCommandInfo`:

```
1. name            (bulk string)
2. arity           (integer)
3. flags           (array of bulk strings)
4. firstkey        (integer)
5. lastkey         (integer)
6. keystep         (integer)
7. acl_categories  (set / array of "@xxx" strings)
8. tips            (empty array)
9. key_specs       (empty array)
10. subcommands    (empty array)
```

`firstkey`/`lastkey`/`keystep` follow Redis's legacy convention where, if a
range exists, `lastkey` is reported relative to `firstkey` and Redis internally
adds `firstkey` when `lastkey >= 0`. For a no-key command all three are `0`.

### `COMMAND DOCS` map

```
%<n>
  "append" -> %4
      "summary" -> "Appends a value to a key."
      "since"   -> "2.0.0"
      "group"   -> "string"
      "complexity" -> "O(1)"
  "get" -> ...
```

`group` is always present (matches Redis, which always emits it). Others are
present only when non-null in the `CommandEntry`.

## Phasing & Implementation Plan

Each phase is one commit, TDD: failing test first, then code, then JaCoCo
confirmation (per AGENTS.md). Phases are ordered so that early phases unblock
clients (COUNT/LIST) before the heavier reply-shape work.

| Phase | Scope                                                                                    |
|-------|------------------------------------------------------------------------------------------|
| 1     | `CommandEntry` record + `CommandRegistry` + `CommandRegistryTest`. |
| 2     | **Register every command name** across all 26 group `static {}` blocks (name + arity + key positions; best-effort flags, `movablekeys` for dynamic-key commands). This must complete *before* any user-visible enumeration switches to the registry, otherwise `COUNT`/`LIST` would be incomplete during intermediate commits. |
| 3     | `COUNT` + `LIST` (no filter) — now safe because all names are present.                   |
| 4     | `LIST FILTERBY ACLCAT` and `FILTERBY PATTERN`.                                          |
| 5     | `INFO` (10-element array, `NilReply.INSTANCE` for unknown, no-arg = all).               |
| 6     | `DOCS` (map reply with isMap; tests assert RESP2 `*` and RESP3 `%` shapes).             |
| 7     | `HELP` static text.                                                                      |
| 8     | `GETKEYSANDFLAGS` key flags (best-effort read/write).                                    |
| 9     | **Enrichment only**: refine per-command flags/docs metadata from Redis JSON, group-by-group. No discovery risk — COUNT/LIST already correct. |
| 10    | Update [`doc/design/README.md`](/home/kerry/ws/velo/doc/design/README.md) index.        |
| 11    | **Next version/stage**: add Redis-compatible subcommand metadata: pipe-separated names like `command|info`, inclusion in `LIST` and `INFO` lookup, and nested `COMMAND DOCS` subcommands. |

Key change vs. the first draft: full name registration (phase 2) now precedes
wiring `COUNT`/`LIST` (phase 3), so no intermediate commit publishes an
undercounted registry.

## Testing Strategy

- **Unit** (`CommandRegistryTest`, Java/Spock): register, get, duplicate-skip,
  size.
- **Command** (extend
  [`CommandCommandTest.groovy`](/home/kerry/ws/velo/dyn/test/io/velo/command/CommandCommandTest.groovy),
  one `def 'test command'()` method per AGENTS.md): for each subcommand assert
  reply type, structure, and a known command's fields. Reuse the existing mock
  pattern (`BaseCommand.mockAGroup()` + `RequestHandler`).
- **JaCoCo**: after each phase, confirm via `scripts/jacoco_cover.py` that the
  new branches (filter types, null-on-unknown, map-vs-array) are executed.
- **Compat sanity**: run `redis-cli COMMAND COUNT` / `COMMAND LIST` /
  `COMMAND INFO get` against a running Velo instance and eyeball against real
  Redis output.

## Open Questions

1. **Key flags granularity** — is `RW`/`RO` + `access` per key enough for
   `GETKEYSANDFLAGS`, or should write commands that overwrite (e.g. `SET`)
   report `OW` (overwrite)? v1 proposes `RW` for write, `RO` for read; refine if
   a client depends on the distinction.
2. **`COMMAND LIST` ordering** — Redis tags this
   `NONDETERMINISTIC_OUTPUT_ORDER`. Insertion order is fine; confirm no client
   assumes alphabetical.
3. **Metadata accuracy priority** — should phase 9 block on perfect per-command
   metadata (full arity/flags from Redis JSON) or register names-first with
   best-effort flags and refine? Proposal: names-first so COUNT/LIST are correct
   immediately; flags filled group-by-group.

## Related Documents

- [Command Processing Design](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Response Encoding Design](/home/kerry/ws/velo/doc/design/06_response_encoding_design.md)
- [ACL Security Design](/home/kerry/ws/velo/doc/design/10_acl_security_design.md)
- [Dynamic Groovy Design](/home/kerry/ws/velo/doc/design/15_dynamic_groovy_design.md)
- [Redis Command Support](/home/kerry/ws/velo/doc/redis_command_support.md)

## Review Feedback - AI Agent 2

### Summary

The registry direction is sound and fits the current command-group dispatch
model. Keeping metadata co-located with the owning `XGroup` class should reduce
drift, and using `CommandCommand` as the consumer avoids changing the request
dispatch path.

### Confirmed / Adjusted Findings

1. **Registration synchronization is not required for the current init path.**
   `MultiWorkerServer.requestHandlerArray(...)` constructs `RequestHandler`
   instances serially, and each `RequestHandler` instantiates all 26 command
   groups before client requests are served. Because registration is limited to
   static initialization during this serial bootstrap path, `CommandRegistry`
   does not need synchronization or a freeze step. Add a concise comment on
   `register(...)` documenting this contract: registration happens during
   command-group static initialization, before serving requests, and the
   registry must not be mutated after startup.

2. **Avoid exposing partial registry results through `COMMAND COUNT` / `LIST`.**
   The phase plan registers only a few `AGroup` commands in phase 1, but switches
   user-visible `COUNT` and `LIST` to the registry in phase 2 while full B-Z
   rollout is phase 8. That would make `COUNT` / `LIST` dramatically incomplete
   during intermediate commits. Populate all command names before wiring
   user-visible enumeration, or keep incomplete-registry phases test-only.

3. **Dynamic key metadata needs explicit handling.**
   Static `firstKey` / `lastKey` / `keyStep` metadata works for simple patterns
   like `get`, `set`, `mget`, and `mset`, but not for commands whose key range is
   driven by a runtime `numkeys` argument, such as `lmpop`, `bzmpop`, `zunion`,
   and related commands. For v1, these should be marked as dynamic / movable-key
   commands rather than forced into an inaccurate legacy triple.

4. **Use `NilReply.INSTANCE` for unknown `COMMAND INFO` entries.**
   The design says unknown command names should return a RESP nil bulk. In Velo
   reply objects this should be represented explicitly as `NilReply.INSTANCE`,
   not Java/Groovy `null`, because `MultiBulkReply` serializes each child reply.

5. **Clarify RESP2 vs RESP3 `COMMAND DOCS` encoding.**
   `MultiBulkReply(..., isMap=true, ...)` emits a RESP3 map only when the socket
   is in RESP3 mode and `bufferAsResp3()` is used. Normal RESP2 clients receive
   an array encoding. Tests should assert the expected wire shape for both RESP2
   and RESP3, especially for `COMMAND DOCS`.

6. **Treat `GETKEYSANDFLAGS` flags as best-effort unless refined.**
   Returning only `RW` / `RO` plus `access` is useful, but it is not a full Redis
   match for overwrite, delete, insert, and update cases. Either refine the
   per-command key flags or document v1 as best-effort compatibility.

7. **Fix the `lrange` key-position example.**
   The table lists `lrange` under `KeyIndexBegin2`, but Velo parses `lrange` key
   at argument index 1. The example should be corrected before implementation to
   avoid copying wrong metadata into the registry.

## Review Response - Author

All seven findings were verified against the current code before acting; each is
technically correct and has been folded into the design above. Summary of what
changed and why:

1. **Registration sync — agreed.** Verified: `RequestHandler` instances are built
   in a serial per-worker loop (`MultiWorkerServer.java:1706`,
   `list[i] = new RequestHandler(...)`), and group class-init runs exactly once
   under the JVM class-init lock. So `CommandRegistry` needs no synchronization
   or freeze step *functionally*. The contract is now stated explicitly for
   `register(...)`: static-init only, documented by method comment, with no
   synchronization or freeze step.

2. **Partial registry in COUNT/LIST — agreed, rephased.** This was the most
   important process finding: the original plan wired `COUNT`/`LIST` to the
   registry while B–Z groups were still unregistered, so intermediate commits
   would have undercounted. The phasing table is reordered so **all command names
   are registered (phase 2) before `COUNT`/`LIST` switch to the registry
   (phase 3)**. Flags/docs enrichment moved to a late, no-risk phase 9.

3. **Dynamic-key commands — agreed, added a section.** Verified the runtime
   `numkeys` cases: `lmpop` (numkeys @ idx 1, keys @ 2), `blmpop` (1/2),
   `zmpop` (1/2), `bzmpop` (2/3), `zunion`/`zinter`/`zdiff`/`zintercard`
   (1/2), and the `*store` variants (2/3). These now register with
   `firstKey=-1, lastKey=-1, keyStep=0` plus the `movablekeys` flag; live
   `parseSlots` still powers `GETKEYS`. A dedicated table in the Design section
   enumerates them.

4. **`NilReply.INSTANCE` — agreed.** Confirmed `NilReply` exists with the right
   RESP2/RESP3 bytes, and that `MultiBulkReply.buffer()` dereferences each child
   (so a bare `null` is an NPE). The `INFO` subcommand spec now mandates
   `NilReply.INSTANCE` for unknown names.

5. **RESP2 vs RESP3 DOCS — agreed, clarified.** Confirmed `buffer()` emits `*`
   even when `isMap` (RESP2 down-converts maps to arrays) and `bufferAsResp3()`
   emits `%` — identical to Redis. The DOCS spec now states both wire shapes and
   requires tests for each.

6. **GETKEYSANDFLAGS best-effort — agreed, documented.** v1 collapses the full
   `RW/RO/OW/RM × access/update/insert/delete` matrix to read-vs-write only.
   Recorded as best-effort in the spec and kept as Open Question #1 for later
   refinement.

7. **`lrange` example — agreed, fixed.** Confirmed `lrange` parses its key at
   `data[1]` (`LGroup.java:89`), so `firstKey=1`. The table now lists `lrange`
   under a single-key-at-index-1 row. Also discovered (and noted) that
   `KeyIndexBegin2`/`KeyIndexBegin2Step2` are defined in `BaseCommand` but used
   by no command, so the fabricated index-2 example was removed rather than
   replaced with another guess.

No findings were refuted. The design is implementation-ready with these
amendments.
