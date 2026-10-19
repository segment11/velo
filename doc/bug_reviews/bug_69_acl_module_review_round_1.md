# Bug 69 — ACL Module Review (Round 1)

**Module:** `acl` (`io.velo.acl`) plus its `ACL` command integration in `io.velo.command.AGroup`
**Round:** 1
**Author:** AI agent 1
**Status:** documented, not yet fixed

## Scope

Files read for this round:

- `src/main/java/io/velo/acl/AclUsers.java`
- `src/main/java/io/velo/acl/U.java`
- `src/main/java/io/velo/acl/RCmd.java`
- `src/main/java/io/velo/acl/RKey.java`
- `src/main/java/io/velo/acl/RPubSub.java`
- `src/main/java/io/velo/acl/Category.java`
- `src/main/java/io/velo/acl/LogRow.java`
- `src/main/java/io/velo/acl/AclInvalidRuleException.java`
- `src/main/java/io/velo/command/AGroup.java` (`acl()` and `parseSlots()`)
- `src/main/java/io/velo/BaseCommand.java` (`getAuthU`)
- `src/main/java/io/velo/decode/Request.java` (`isAclCheckOk`)
- `src/main/java/io/velo/MultiWorkerServer.java` (io-loop ACL gate)
- `src/main/java/io/velo/RequestHandler.java` (AUTH + HTTP basic auth)
- `doc/design/10_acl_security_design.md`

Reference material used for Redis parity checks:

- Redis 7.2 `src/acl.c` (`ACLCommandCategories[]`, `ACLSetUser` command-rule parsing,
  `ACLCatCommand`) — <https://github.com/redis/redis/blob/7.2/src/acl.c>
- Redis 7.2 `src/server.c:setImplicitACLCategories()` — the rule that turns `CMD_WRITE`,
  `CMD_READONLY`, `CMD_ADMIN`, `CMD_PUBSUB`, `CMD_FAST`, `CMD_BLOCKING` into implicit
  `@write / @read / @admin+dangerous / @pubsub / @fast / @blocking`, plus
  "if it's not @fast it is @slow".
- Redis 7.2 `src/commands.def` (full command table with flags and category bits).
- <https://redis.io/docs/latest/commands/acl-cat/>
- <https://redis.io/docs/latest/commands/acl-setuser/>
- <https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/>
  (the "Key permissions" section is the normative source for `%R~` / `%W~` semantics)

Note: rounds 1–5 of the ACL module were reviewed in `bug_27_acl_security_module_review_round_1..5.md`.
This round deliberately re-derives ground truth from the Redis source and does **not** re-report
the bug_27 findings (`literal()` double hashing, `dataToLine()` binlog quoting, `reset*` handling,
non-atomic `ACL SAVE`, `reset` pubsub propagation) unless a genuinely new instance was found.

---

## Finding 1: `Category.writeCmdSet` is out of sync with `CommandRegistry`, so `%R~` read-only key rules grant mutating commands

**Severity:** High
**Class:** Security — read-only key permission bypass

### Cited files / lines

- `src/main/java/io/velo/acl/Category.java:789-893` — the authoritative `@write` command list
  ```java
  List<String> writeCmdList = List.of(
          "append",
          "bitfield",
          ...
          "smove",
          "sort",
          "spop",
          ...
          "zunionstore"
  );
  ...
  writeCmdSet.addAll(writeCmdList);
  ```
  This list contains **no** `lmpop`, `zmpop`, `blmpop`, `bzmpop`, `hgetdel`, `hgetex`, `hsetex`,
  `hexpire`, `hexpireat`, `hexpiretime`, `hpersist`, `hpexpire`, `hpexpireat`, `hpexpiretime`,
  `hpttl`, `httl`, or `decrbyfloat`.

- `src/main/java/io/velo/acl/Category.java:895-918` — the read-write set, which likewise misses them
  ```java
  readWriteCmdSet.addAll(List.of(
          "getdel", "getex", "getset", "lmove", "blmove", "rpoplpush", "brpoplpush",
          "lpop", "rpop", "blpop", "brpop", "spop", "zpopmax", "zpopmin", "setbit",
          "smove", "copy", "rename", "renamenx", "restore", "geosearchstore", "sort"
  ));
  ```

- `src/main/java/io/velo/acl/U.java:613-632` — where the classification is consumed
  ```java
  boolean isWrite = Category.isWriteCmd(cmd);
  boolean isReadWrite = Category.isReadWriteCmd(cmd, data);
  boolean cmdRead = !isWrite;
  boolean cmdWrite = isWrite;
  ...
  if (rKey.match(slotWithKeyHash.rawKey(), cmdRead, cmdWrite, isReadWrite)) {
  ```

- `src/main/java/io/velo/acl/RKey.java:46-49` — a `%R~` rule matches on `cmdRead`
  ```java
  } else if (type == Type.read) {
      return KeyLoader.isKeyMatch(key, pattern) && cmdRead && !needsBoth;
  ```

### Root cause

`Category.java` hand-maintains a duplicated copy of the per-command ACL category metadata that is
already declared authoritatively in every `CommandRegistry.register(new CommandEntry(...))` call.
The two copies have drifted. Velo's own registry entries prove the drift:

| Command | Declared in `CommandRegistry` | `Category.java` |
|---|---|---|
| `lmpop` | `@write @list @slow` | absent |
| `zmpop` | `@write @sortedset @slow` | absent |
| `blmpop` | `@write @blocking @list @slow` | absent |
| `bzmpop` | `@write @blocking @sortedset @slow` | absent |
| `hgetdel` | `@write @hash @fast` | absent |
| `hgetex` | `@write @hash @fast` | absent |
| `hsetex` | `@write @hash @fast` | absent |
| `hexpire`, `hexpireat`, `hpersist`, `hpexpire`, `hpexpireat` | `@write @hash @fast` | absent |
| `decrbyfloat` | `@write @string @fast` | absent |

All of these are implemented and are genuinely mutating — e.g. `HGroup.java:341` handles
`hgetdel`, `HGroup.java:345` handles `hgetex`, `LGroup.java:178` handles `lmpop`,
`ZGroup.java:442` handles `zmpop`, `BGroup.java:244` handles `blmpop`,
`ZGroup.java:329` handles `bzmpop`.

Because they are missing from `writeCmdSet`, `Category.isWriteCmd("hgetdel")` returns `false`, so
`cmdRead` is `true` and `needsBoth` is `false`.

### Impact

1. **Read-only key rules grant writes.** `ACL SETUSER reader on >pw ~* %R~data:* +@hash` (or even
   `+hgetdel` explicitly) lets `reader` run `HGETDEL data:h 0`, `HSETEX`, `HEXPIRE`,
   `LMPOP data:l 2 LEFT`, `ZMPOP data:z 1`, `BLMPOP`, `BZMPOP`, which all delete data. The
   documented read/write split (`%R~` / `%W~` / `%RW~`) is silently bypassed for these commands.
2. `ACL CAT write` under-reports, and `+@write -hgetdel`-style rule authoring silently fails to
   describe reality.
3. `ACL DRYRUN` reports `OK` for the same commands for the same reason, so operators cannot detect
   the misconfiguration.

### Verification against Redis 7.2.11 (live `redis-server` + `redis-cli`)

The normative rule, from the Redis ACL "Key permissions" documentation:

> Which type of permission is required for a command is documented through **key specifications**.
> The type of permission is based off the key's logical operation flags. The **insert, update, and
> delete flags map to the write key permission**. The access flag maps to the read key permission.

`LMPOP` / `ZMPOP` / `BLMPOP` / `BZMPOP` all carry delete flags, so they require **write** key
permission and must be denied by a `%R~` rule.

Live transcript (Redis 7.2.11, `/tmp/opencode/redisacl`):

```
$ redis-server --version
Redis server v=7.2.11 sha=d4c381df:0 malloc=jemalloc-5.3.0 bits=64 build=e3db856fd57ff434

$ redis-cli ACL SETUSER ro on '>pw' '%R~L1' +get +set +lrange +lpush +lpop +rpop +lmpop +zmpop +lmove +sadd +spop +smembers
OK
$ redis-cli ACL GETUSER ro
  commands : -@all +get +set +lrange +lpush +lpop +rpop +lmpop +zmpop +lmove +sadd +spop +smembers
  keys     : %R~L1

$ redis-cli RPUSH L1 a b c          # seeded by admin
3

--- as user 'ro' (keys = %R~L1 only) ---
GET L1              => WRONGTYPE Operation against a key holding the wrong kind of value   # read allowed
LRANGE L1 0 -1      => a b c                                                                # read allowed
SET L1 v            => NOPERM No permissions to access a key
LPUSH L1 d          => NOPERM No permissions to access a key
LPOP L1             => NOPERM No permissions to access a key
RPOP L1             => NOPERM No permissions to access a key
LMPOP 1 L1 LEFT     => NOPERM No permissions to access a key                                 # <-- key evidence
LSET L1 0 v         => NOPERM User ro has no permissions to run the 'lset' command

--- admin view: L1 is untouched ---
LRANGE L1 0 -1      => a b c

$ redis-cli ACL CAT write | grep -E '^(lmpop|zmpop|blmpop|bzmpop)$'
blmpop bzmpop zmpop lmpop

$ redis-cli ACL CAT read | grep -E '^(expiretime|pexpiretime|sintercard|zintercard)$'
expiretime pexpiretime zintercard sintercard
```

The four `*mpop` commands are unambiguously in Redis's `@write`, and Redis refuses them under a
read-only key pattern.

Now the same check against Velo's own classification, driving `Category.isWriteCmd` /
`Category.isReadWriteCmd` / `RKey.match` directly from the compiled
`build/classes/java/main` (`/tmp/opencode/vcheck/VCheck.java`):

```
cmd        isWrite  isRW     veloMatch redis(7.2.11, %R~L1)
-----------------------------------------------------------------
get        false    false    ALLOW     ALLOW
lrange     false    false    ALLOW     ALLOW
lpop       true     true     NOPERM    NOPERM
rpop       true     true     NOPERM    NOPERM
lmpop      false    false    ALLOW     NOPERM      <-- BUG
blmpop     false    false    ALLOW     NOPERM      <-- BUG
zmpop      false    false    ALLOW     NOPERM      <-- BUG
bzmpop     false    false    ALLOW     NOPERM      <-- BUG
getdel     true     true     NOPERM    NOPERM
lmove      true     true     NOPERM    NOPERM
smove      true     true     NOPERM    NOPERM
```

Because `LMPOP` is classified as a *read* by `Category`, `U.checkCmdAndKey` computes
`cmdRead = true, cmdWrite = false, needsBoth = false`, so `RKey.match` for a `%R~L1` rule returns
`true` and Velo grants the command — while Redis denies it. The `*mpop` family is confirmed.

Note: `hgetdel`, `hgetex`, `hsetex`, `hexpire*`, `hpersist`, `decrbyfloat` are Redis 7.4 commands
and are unknown to the local 7.2.11 binary (`COMMAND INFO hgetdel hgetex hsetex decrbyfloat`
returns nothing), so they cannot be cross-checked here. The evidence for those is Velo's own
`CommandRegistry`, which declares each of them `@write` while `Category.java` omits them —
an internal contradiction that is sufficient on its own.

### Suggested fix

Derive the category sets from `CommandRegistry` at startup (single source of truth) rather than
maintaining `Category.java` by hand, and add `lmpop`, `zmpop`, `blmpop`, `bzmpop` (and the hash
TTL / `hgetdel` / `hgetex` / `hsetex` / `decrbyfloat` family) to `writeCmdList` and the
read-write set in the short term. Add a unit test that asserts, for every registered command
whose `CommandEntry` declares `@write`, that `Category.isWriteCmd(cmd)` is `true`.

---

## Finding 2: `ACL CAT all` throws `NullPointerException`

**Severity:** High
**Class:** Robustness / crash on a valid-looking client command

### Cited files / lines

- `src/main/java/io/velo/command/AGroup.java:144-169`
  ```java
  var categories = Category.values();
  if (category == null) {
      var replies = new Reply[categories.length];
      for (int i = 0; i < categories.length; i++) {
          replies[i] = new BulkReply(categories[i].name());
      }
      return new MultiBulkReply(replies);
  } else {
      int index = -1;
      for (int i = 0; i < categories.length; i++) {
          if (categories[i].name().equals(category)) {
              index = i;
              break;
          }
      }
      if (index == -1) {
          return ErrorReply.SYNTAX;
      }

      var cmdList = Category.getCmdListByCategory(categories[index]);
      var replies = new Reply[cmdList.size()];   // <-- NPE when index == 0 ("all")
  ```
- `src/main/java/io/velo/acl/Category.java:8-22`
  ```java
  public enum Category {
      all, admin, bitmap, blocking, connection, dangerous,
      ...
  ```
- `src/main/java/io/velo/acl/Category.java:63-919` — the static initializer never does
  `CMD_LIST_BY_CATEGORY.put(all, ...)`, so `getCmdListByCategory(Category.all)` returns `null`
  (`Category.java:925-928`).

### Root cause

`Category.all` is a pseudo-category used internally by `RCmd` for `+@all` / `+*`. It is the
**first** enum constant, so `ACL CAT all` resolves `index == 0` and then dereferences a `null`
command list.

Redis has no such pseudo-category in its category table. From `acl.c` (7.2):

```c
} ACLCommandCategories[] = {
    {"keyspace", ACL_CATEGORY_KEYSPACE},
    {"read", ...}, {"write", ...}, {"set", ...}, {"sortedset", ...}, {"list", ...},
    {"hash", ...}, {"string", ...}, {"bitmap", ...}, {"hyperloglog", ...}, {"geo", ...},
    {"stream", ...}, {"pubsub", ...}, {"admin", ...}, {"fast", ...}, {"slow", ...},
    {"blocking", ...}, {"dangerous", ...}, {"connection", ...}, {"transaction", ...},
    {"scripting", ...},
    {NULL,0}
};
```

and:

```c
} else if (!strcasecmp(sub,"cat") && c->argc == 3) {
    uint64_t cflag = ACLGetCommandCategoryFlagByName(c->argv[2]->ptr);
    if (cflag == 0) {
        addReplyErrorFormat(c, "Unknown category '%.128s'", (char*)c->argv[2]->ptr);
        return;
    }
```

### Impact

1. `ACL CAT all` produces an unhandled `NullPointerException` instead of a protocol error reply.
   Any ACL-aware admin tooling that probes categories (e.g. enumerating names from `ACL CAT` and
   then asking `ACL CAT <name>` for each) will crash the connection.
2. `ACL CAT` (no argument) advertises `all` as a real category, which Redis does not do — a client
   that blindly iterates the list will send `ACL CAT all` and hit the NPE.

### Verification against Redis 7.2.11 (live `redis-server` + `redis-cli`)

```
$ redis-cli ACL CAT all
(error) ERR Unknown category 'all'

$ redis-cli ACL CAT | grep -c '^all$'
0
$ redis-cli ACL CAT
keyspace read write set sortedset list hash string bitmap hyperloglog geo stream
pubsub admin fast slow blocking dangerous connection transaction scripting
```

Redis has 21 categories, none of them `all`, and `ACL CAT all` is a clean error. Velo has 22 enum
constants (the extra one being `all`), advertises it, and crashes on it.

### Suggested fix

Either (a) return `new ErrorReply("Unknown category '" + category + "'")` for `Category.all` and
exclude `all` from the no-argument listing (matches Redis exactly), or (b) make
`getCmdListByCategory` never return `null` (return an empty list for `all`) and treat `all` as a
valid alias. Option (a) is Redis-compatible; option (b) at minimum removes the crash.

---

## Finding 3: `RCmd.fromLiteral` preserves the case of the command name, so `+GET` is accepted but never matches

**Severity:** Medium-High
**Class:** Correctness — silently broken ACL rule

### Cited files / lines

- `src/main/java/io/velo/acl/RCmd.java:143-153`
  ```java
  } else {
      var cmdPart = str.substring(1);
      if ("*".equals(cmdPart)) {
          type = Type.all;
          cmd = cmdPart;
      } else if (cmdPart.contains("*")) {
          throw new IllegalArgumentException("Wildcard command names are not supported: " + str);
      } else {
          type = Type.cmd;
          cmd = cmdPart;              // <-- case preserved, e.g. "GET"
      }
  }
  ```
- `src/main/java/io/velo/acl/RCmd.java:44-45` — matching is case-sensitive
  ```java
  } else if (type == Type.cmd) {
      return this.cmd.equals(cmd);
  ```
- `src/main/java/io/velo/decode/Request.java:227-233` — the incoming command is lowercased
  ```java
  cmd = new String(data[0]).toLowerCase(Locale.ROOT);
  ```

### Root cause

`ACL SETUSER alice +GET` stores an `RCmd` with `cmd == "GET"`. Every subsequent request has
`cmd == "get"`, so `this.cmd.equals(cmd)` is always `false` and the rule never grants anything.
The rule is still accepted (`ACL SETUSER` returns `OK`), is echoed back by `ACL GETUSER` and
`ACL LIST`, and is persisted into the ACL file by `ACL SAVE`, so it looks correct everywhere.

Redis is case-insensitive here: `ACLSetUser` resolves the rule name through
`ACLLookupCommand(op+1)` → `lookupCommandBySdsLogic()`, which performs a case-insensitive
`stringmatchlen()` against the command table (see `acl.c:881-887` and `acl.c:1099-1103`). So
`+GET` works in Redis.

Note the asymmetry: the *subcommand* half is already case-insensitive
(`U.java:593` lowercases `data[1]` before matching), so `+CONFIG|get` works but `+CONFIG` does not.

### Impact

- Silent failure to grant a permission. An operator writes `ACL SETUSER app +GET`, gets `+OK`, and
  the user is then denied every `GET` with a `NOPERM` that looks like an application bug.
- **The broken rule is written to disk on `ACL SAVE` and replayed on replicas, so the divergence
  persists across restart and failover.**

### Verification against Redis 7.2.11 (live `redis-server` + `redis-cli`)

```
$ redis-cli ACL SETUSER uc on '>pw' '+GET' '%R~*'
OK
$ redis-cli SET ck hello >/dev/null

$ REDISCLI_AUTH=pw redis-cli --user uc GET ck
"hello"                                     <-- uppercase rule works in Redis

$ redis-cli ACL DRYRUN uc GET ck
OK
```

Confirmed live on 7.2.11. In Velo, `RCmd.fromLiteral("+GET")` stores `cmd == "GET"` while
`Request.cmd()` lowercases to `"get"`, so the same request is denied with `NOPERM`.

### Suggested fix

Lowercase the command name in `RCmd.fromLiteral` (`cmd = cmdPart.toLowerCase(Locale.ROOT)`), and
likewise lowercase `category` input (already done) and the subcommand (already done). Note
`U.literal()` will then serialize the normalized lowercase form, which matches Redis output.

---

## Finding 4: `ACL SAVE` writes lines that `ACL LOAD` (and server startup) cannot parse

**Severity:** Medium-High
**Class:** Correctness / availability — self-inflicted ACL file corruption

### Cited files / lines

- `src/main/java/io/velo/acl/U.java:267-300` — `literal()` can emit a 3-token line
  ```java
  sb.append("user ").append(user).append(" ");
  sb.append(isOn ? "on" : "off").append(" ");
  ...
  if (!sb.isEmpty()) {
      sb.deleteCharAt(sb.length() - 1);
  }
  return sb.toString();
  ```
  A user with no passwords, no command rules, no key rules and no channel rules serializes to
  exactly `"user x off"` — 3 space-separated tokens.

- `src/main/java/io/velo/acl/U.java:371-379` — the parser rejects it
  ```java
  var parts = str.split(" ");
  if (parts.length < 4) {
      return null;
  }
  ```

- `src/main/java/io/velo/acl/AclUsers.java:113-133` — `loadAclFile()` turns that into a hard error
  ```java
  var u = U.fromLiteral(line);
  if (u == null) {
      throw new IllegalArgumentException("parse acl file error: " + line);
  }
  ```
  and rethrows as `RuntimeException`, aborting the whole load.

### Reproduction

```
> ACL SETUSER temp reset
OK
> ACL SETUSER temp off
OK
> ACL SAVE
OK
> ACL LOAD
(error) parse acl file error: user temp off
```

`ACL SETUSER x reset` / `ACL SETUSER x off` is enough to create such a user. `Redis` writes the
same shape (`ACLSaveStyle` emits `"user %s %s"` plus rules), and `ACLLoadFromFile` accepts it
because it only requires `argc >= 2` after `sdssplitargs`.

The same code path runs at server startup (`AclUsers.loadAclFile()`), so a saved ACL file
containing such a user prevents the server from starting.

A second, related gap in the same method: `AclUsers.loadAclFile()` only skips lines starting with
`#`. A blank line — which Redis explicitly skips (`acl.c` continues on `argc == 0`) — hits the
`parts.length < 4` → `null` → `IllegalArgumentException` path and aborts the load.

### Impact

- `ACL SAVE` followed by `ACL LOAD` fails permanently once any user has an empty rule set.
- A restart with that ACL file fails.
- Hand-edited ACL files with blank lines (very common) cannot be loaded.

### Suggested fix

1. Lower the parser threshold from `parts.length < 4` to `parts.length < 3` and treat a user line
   with only `user <name> <on|off>` as a valid user with no rules.
2. In `AclUsers.loadAclFile()`, skip blank/whitespace-only lines (`if (line.isBlank()) continue;`).
3. Add a round-trip test: for a matrix of user states, assert
   `U.fromLiteral(u.literal())` is non-null and semantically equal to `u`.

---

## Finding 5: `RCmd.fromLiteral("+cmd|")` throws an uncaught `ArrayIndexOutOfBoundsException`

**Severity:** Medium
**Class:** Robustness — exception escapes the ACL error handling

### Cited files / lines

- `src/main/java/io/velo/acl/RCmd.java:134-138`
  ```java
  if (str.contains("|")) {
      type = Type.cmd_with_first_arg;
      var parts = str.split("\\|");
      cmd = parts[0].substring(1);
      firstArg = parts[1];      // <-- AIOOBE when the split yields one element
  ```
  Java's `String.split` discards trailing empty strings, so `"+config|".split("\\|")` returns
  `["+config"]` and `parts[1]` throws `ArrayIndexOutOfBoundsException`.

- `src/main/java/io/velo/command/AGroup.java:442-464` — the handler only catches two exception types
  ```java
  } else if (RCmd.isRCmdLiteral(rule)) {
      if (RCmd.isAllowLiteral(rule)) {
          temp.addRCmd(false, RCmd.fromLiteral(rule));
      ...
  } catch (AclInvalidRuleException | IllegalArgumentException e) {
      log.error("Set user error={}", e.getMessage());
      return ErrorReply.ACL_SETUSER_RULE_INVALID;
  }
  ```

### Root cause

`ArrayIndexOutOfBoundsException` is a `RuntimeException` but neither `AclInvalidRuleException` nor
`IllegalArgumentException`, so it escapes `acl()` entirely instead of producing
`ACL_SETUSER_RULE_INVALID`.

Redis explicitly rejects this input with a clean protocol error. Verified live:

```
$ redis-cli ACL SETUSER bad on '+config|'
(error) ERR Error in ACL SETUSER modifier '+config|': Syntax error
```

and the Java-side failure in Velo was reproduced with a plain JDK run:

```
$ java T.java
[+config]                                                    # "+config|".split("\\|") -> length 1
AIOOBE: java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1
```

A related silent-acceptance issue in the same block: `+notarealcommand` is accepted by Velo and
stored as a rule that can never match, whereas Redis returns `ENOENT`
("Unknown command or invalid name").

### Impact

`ACL SETUSER u +config|` produces an unexpected internal error rather than a clean protocol error.
Depending on the caller's exception handling this can surface as a wrong error reply or as a
connection-level failure.

### Suggested fix

Validate before indexing:
```java
var parts = str.split("\\|", -1);
if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
    throw new IllegalArgumentException("Invalid literal: " + str);
}
```
Additionally, broaden the catch in `AGroup.acl()` (or validate in `RCmd.isRCmdLiteral`) so no
runtime exception can escape `ACL SETUSER`, and reject unknown command names.

---

## Finding 6: ~~`ACL SETUSER ... nocommands` is implemented as "deny everything" instead of "clear the allow list"~~ — **REFUTED, RETRACTED**

**Severity:** ~~Medium~~ (none — Velo matches Redis)
**Status:** Refuted during verification against Redis 7.2.11. Kept in the document so the
implementing agent does not re-investigate it.

### Verification against Redis 7.2.11 (live `redis-server` + `redis-cli`)

This finding was based on a wrong reading of the `nocommands` documentation. The normative text
says `nocommands` is an **alias for `-@all`**, not "clear the allow list":

> `nocommands`: Alias for `-@all`.

(https://redis.io/docs/latest/commands/acl-setuser/ and
https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)

Confirmed live on 7.2.11:

```
$ redis-cli ACL SETUSER nc on '>pw' '+@all' nocommands +get
OK
$ redis-cli ACL GETUSER nc
  commands: -@all                                 <-- nocommands produced exactly -@all

$ REDISCLI_AUTH=pw redis-cli --user nc GET nc
(error) NOPERM No permissions to access a key     <-- subsequent +get does NOT re-grant

$ redis-cli ACL DRYRUN nc GET x
User nc has no permissions to access the 'x' key
```

Velo's behaviour (`RCmd.fromLiteral("nocommands")` → `allow = false, category = Category.all`,
routed into `rCmdDisallowList` by `AGroup.java:442-447`, with deny beating allow in
`U.java:599-602`) is **exactly** Redis's `-@all` semantics. `allcommands` is likewise the correct
alias for `+@all`.

### Residual (non-confirmed) observation

Velo does not implement the `nokeys` / `nochannels` aliases mentioned alongside `nocommands` in
some tooling, and it has no `selectors` support at all (`(<rule list>)`, `clearselectors`). These
are missing *features*, not bugs, and are out of scope for this round.

### Code references (kept for reference)

- `src/main/java/io/velo/acl/RCmd.java:116-122` — `nocommands` → `Category.all`, `allow = false`
- `src/main/java/io/velo/acl/RCmd.java:88-101` — `isAllowLiteral` / `isRCmdLiteral`
- `src/main/java/io/velo/command/AGroup.java:442-447` — routes non-allow literals to
  `addRCmdDisallow`
- `src/main/java/io/velo/acl/U.java:599-602` — deny beats allow

---

## Finding 7: Category membership differs from Redis 7.2 for 20 implemented commands

**Severity:** Medium
**Class:** Correctness — Redis parity for `+@category` rules

### Cited files / lines

- `src/main/java/io/velo/acl/Category.java:63-919` — all hand-written category lists
- `src/main/java/io/velo/acl/RCmd.java:51-68` — where the membership is consumed
  ```java
  var categoryList = Category.getCategoryListByCmd(cmd);
  if (categoryList == null) {
      return false;
  }
  for (var cmdCategory : categoryList) {
      if (this.category == cmdCategory) {
          return true;
      }
  }
  ```

### Method

Ground truth was derived mechanically from the Redis 7.2 command table
(`src/commands.def`, 341 commands) plus `server.c:setImplicitACLCategories()`, then diffed against
`Category.java` restricted to the 204 commands Velo actually registers. The 20 mismatches are:

| Command | Velo has | Redis 7.2 has | Direction |
|---|---|---|---|
| `acl` | `@admin @dangerous` | *(none)* | over-restrictive |
| `client` | `@admin @connection @dangerous` | *(none)* | over-restrictive |
| `cluster` | `@admin @dangerous` | *(none)* | over-restrictive |
| `config` | `@admin @dangerous` | *(none)* | over-restrictive |
| `pubsub` | `@pubsub` | *(none)* | over-restrictive |
| `asking` | `@keyspace` | `@connection` | mis-categorised |
| `readonly` | `@keyspace` | `@connection` | mis-categorised |
| `readwrite` | `@keyspace` | `@connection` | mis-categorised |
| `select` | `@keyspace` | `@connection` | mis-categorised |
| `wait` | `@keyspace` | `@connection` | over-restrictive |
| `role` | — | `@admin` | under-restrictive |
| `lmpop`, `blmpop` | — | `@list @write @slow (+@blocking)` | **under-restrictive, see Finding 1** |
| `zmpop`, `bzmpop` | — | `@sortedset @write @slow (+@blocking)` | **under-restrictive, see Finding 1** |
| `expiretime`, `pexpiretime` | — | `@keyspace @read @fast` | under-grant for `+@read` |
| `sintercard` | — | `@set @read @slow` | under-grant for `+@read` |
| `zintercard` | — | `@sortedset @read @slow` | under-grant for `+@read` |

### Impact

- The example straight out of the Redis `ACL CAT` docs — `ACL SETUSER karin on +@all -@dangerous`,
  "let the user perform everything but the most dangerous operations" — behaves differently in Velo:
  `ACL`, `CLIENT`, `CLUSTER` and `CONFIG` are all classified `@dangerous` in Velo and therefore
  denied, although Redis does not deny them.
- `+@connection` does not grant `SELECT`, `ASKING`, `READONLY`, `READWRITE`, `WAIT`, or `TIME` in
  Velo, so a "connection-only" user cannot switch databases.
- `+@read` does not grant `EXPIRETIME`, `PEXPIRETIME`, `SINTERCARD`, `ZINTERCARD`.
- `ACL CAT <category>` output differs from Redis for the same reason.

### Suggested fix

Regenerate `Category.java` from `CommandRegistry` (see Finding 1) and align the
`@admin`/`@dangerous`/`@connection` assignments with Redis for `acl`, `client`, `cluster`,
`config`, `pubsub`, `asking`, `readonly`, `readwrite`, `select`, `wait`, `time`, `role`. Note that
Velo's own `CommandRegistry` already agrees with Redis on all of these except
`acl`/`client`/`cluster`/`config`/`pubsub`, so the registry can be used as the tie-breaker.

---

## Finding 8: `ACL LOG` entry `count` saturates and `ACL LOG RESET` does not reset the entry-id counter

**Severity:** Low
**Class:** Correctness — ACL LOG bookkeeping

### Cited files / lines

- `src/main/java/io/velo/acl/AclUsers.java:231-249`
  ```java
  entry.count = aclLogCount;
  ...
  entry.entryId = aclLogEntryIdCounter++;
  ...
  aclLogIndex++;
  if (aclLogCount < ACL_LOG_MAX_SIZE) {
      aclLogCount++;
  }
  ```
- `src/main/java/io/velo/acl/AclUsers.java:285-289`
  ```java
  public static synchronized void resetAclLogs() {
      Arrays.fill(aclLogBuffer, null);
      aclLogIndex = 0;
      aclLogCount = 0;
  }
  ```

### Root cause / impact

1. `entry.count` is assigned the *buffer occupancy before the write*, not a monotonic entry
   ordinal. Once the 128-entry circular buffer has filled, every subsequent entry is stamped with
   `count == 128` forever, so the `count` field returned by `ACL LOG` stops identifying entries.
   Redis increments a dedicated counter (`ACLLogger.log->count++`).
2. `resetAclLogs()` clears the buffer but leaves `aclLogEntryIdCounter` untouched, so entry ids
   keep growing across `ACL LOG RESET`. Redis resets the entry id counter in `ACLRESETLog`.

Neither affects authorization; both make the ACL audit log harder to correlate with client-side
tooling that keys on `entry-id` or `count`.

### Suggested fix

Add a dedicated `aclLogTotalCount` incremented on every record and assign that to
`entry.count`; reset `aclLogEntryIdCounter = 1` in `resetAclLogs()`.

---

## Finding 9: `BaseCommand.getAuthU` fails open to a fully-privileged, unfrozen `U.INIT_DEFAULT_U`

**Severity:** Low
**Class:** Security — latent fail-open fallback

### Cited files / lines

- `src/main/java/io/velo/BaseCommand.java:207-212`
  ```java
  public static @Nullable U getAuthU(ITcpSocket socket0) {
      var authUser = SocketInspector.getAuthUser(socket0);
      if (authUser == null) {
          var defaultU = aclUsers.get(U.DEFAULT_USER);
          return defaultU != null ? defaultU : U.INIT_DEFAULT_U;
      }
      return aclUsers.get(authUser);
  }
  ```
- `src/main/java/io/velo/acl/U.java:255-264` — the mutable singleton
  ```java
  /** Default initialized user with 'default' username. */
  public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

  static {
      INIT_DEFAULT_U.setOn(true);
      INIT_DEFAULT_U.addPassword(Password.NO_PASSWORD);
      INIT_DEFAULT_U.addRCmd(true, RCmd.fromLiteral("+*"), RCmd.fromLiteral("+@all"));
      INIT_DEFAULT_U.addRKey(true, RKey.fromLiteral("~*"));
      INIT_DEFAULT_U.addRPubSub(true, RPubSub.fromLiteral("&*"));
  }
  ```

### Root cause / impact

If the `default` user is ever absent from `AclUsers`, every unauthenticated socket is granted
`on +* +@all ~* &* nopass` — the complete built-in privilege set — bypassing whatever
`ConfForGlobal` or `ACL LOAD` configured.

Today this branch appears unreachable: `AclUsers.delete()` is guarded in `AGroup.acl()`
(`AGroup.java:183-185` skips `U.DEFAULT_USER`), `replaceUsers()` re-adds `default`
(`AclUsers.java:204-210`), and `loadAclFile()` requires it (`AclUsers.java:136-139`). It is
reported because the failure mode is silent and maximally permissive, and because
`U.INIT_DEFAULT_U` is a `public static` **mutable** object — any future code path (or test) that
calls `setOn`, `addPassword`, `addRCmd`, ... on it permanently changes the built-in default for the
whole JVM.

Redis has no equivalent fallback: an unknown/absent `default` user means `NOPERM`.

### Suggested fix

Return `null` when the `default` user is missing (the `@Nullable` contract and
`Request.isAclCheckOk()` already handle `null` by returning `FALSE_WHEN_CHECK_CMD`), and freeze
`U.INIT_DEFAULT_U` after its static initialization (or make it private with a read-only accessor)
so it cannot be mutated.

---

## Finding 10: HTTP basic-auth failures do not write an ACL LOG entry

**Severity:** Low
**Class:** Consistency — audit gap

### Cited files / lines

- `src/main/java/io/velo/RequestHandler.java:329-335` (the `data.length == 1` HTTP path)
  ```java
  var u = aclUsers.get(user);
  if (u == null || !u.isOn() || !u.checkPassword(passwordRaw)) {
      return ErrorReply.AUTH_FAILED;
  }
  ```
- `src/main/java/io/velo/RequestHandler.java:441-458` and `473-492` (the normal HTTP and the RESP
  `AUTH` paths, which **do** record)
  ```java
  var u = aclUsers.get(user);
  if (u == null) {
      var clientInfo = ((TcpSocket) socket).getRemoteAddress().toString();
      AclUsers.recordAclLog("auth", "auth", user, user, clientInfo);
      return ErrorReply.AUTH_FAILED;
  }
  ```

### Root cause / impact

The single-token HTTP endpoint auth gate collapses the three failure cases into one branch and
omits `AclUsers.recordAclLog("auth", ...)`. A failed basic-auth attempt against those endpoints
(e.g. `?metrics`) leaves no trace in `ACL LOG`, while the same attempt against a normal HTTP
command or via RESP `AUTH` is recorded. This weakens brute-force detection on exactly the
endpoints that are the easiest to probe.

### Suggested fix

Mirror the RESP path: record an `"auth"` / `"auth"` entry for each of the three failure cases
before returning `AUTH_FAILED`.

---

## Summary

| # | Finding | Severity | Area | Verified |
|---|---|---|---|---|
| 1 | `Category.writeCmdSet` out of sync with `CommandRegistry`; `%R~` grants writes | High | `Category.java`, `U.java` | live Redis 7.2.11 |
| 2 | `ACL CAT all` throws `NullPointerException` | High | `AGroup.java`, `Category.java` | live Redis 7.2.11 |
| 3 | `RCmd.fromLiteral` preserves case → `+GET` never matches | Medium-High | `RCmd.java` | live Redis 7.2.11 |
| 4 | `ACL SAVE` emits lines `ACL LOAD` cannot parse; blank lines abort load | Medium-High | `U.java`, `AclUsers.java` | JDK repro |
| 5 | `+cmd|` throws uncaught `ArrayIndexOutOfBoundsException` | Medium | `RCmd.java`, `AGroup.java` | live Redis 7.2.11 + JDK repro |
| 6 | ~~`nocommands` semantics~~ | **REFUTED / RETRACTED** | — | live Redis 7.2.11 |
| 7 | 20 category-membership mismatches vs Redis 7.2 | Medium | `Category.java` | `ACL CAT` diff on 7.2.11 |
| 8 | `ACL LOG` `count` saturates; `RESET` keeps entry-id counter | Low | `AclUsers.java` | static analysis |
| 9 | `getAuthU` fails open to mutable fully-privileged `INIT_DEFAULT_U` | Low | `BaseCommand.java`, `U.java` | static analysis |
| 10 | HTTP single-token basic-auth failures are not logged | Low | `RequestHandler.java` | static analysis |

## Verification environment

```
$ redis-server --version
Redis server v=7.2.11 sha=d4c381df:0 malloc=jemalloc-5.3.0 bits=64 build=e3db856fd57ff434
```

Started with `redis-server --port 7799 --save '' --appendonly no --dir /tmp/opencode/redisacl`,
all interactions via `redis-cli -p 7799`. Only 7.2.x binaries are available locally
(`~/.proof/bin/redis-server`, `redis-server-7`, `redis-server-7211`, `redis-server-notls`,
`redis-server-tls` — 7.2.11 / 7.2.14), so Redis 7.4+ commands (`hgetdel`, `hgetex`, `hsetex`,
`hexpire*`, `hpersist`, `decrbyfloat`) could not be cross-checked against a live binary and rely
on Velo's own `CommandRegistry` declarations plus the Redis 7.2 command table structure.

## Suggested fix order

1. Finding 2 (`ACL CAT all` NPE) — smallest change, immediately visible.
2. Finding 5 (`+cmd|` AIOOBE) — smallest change, restores clean error replies.
3. Finding 1 (write-command set) — highest security impact; do it as one self-contained change
   (populate `Category` from `CommandRegistry` + explicit additions), with a test that asserts
   `CommandRegistry`'s `@write` ⟺ `Category.isWriteCmd(cmd)`.
4. Finding 4 (`literal()` / `fromLiteral()` round trip) — add a `U.literal()` ↔ `U.fromLiteral()`
   round-trip test first, then fix the `parts.length` threshold and skip blank lines.
5. Finding 3 (command-name case normalisation) — one-line fix plus a test.
6. Finding 7 (category parity) — mechanical, do together with Finding 1 if convenient.
7. Findings 8, 9, 10 — low-risk cleanups.

Finding 6 has been retracted and must **not** be fixed.

## Notes for the implementing agent

- Findings 1 and 7 overlap. If they are fixed together, keep them in **separate commits** as the
  workflow requires: first the `writeCmdSet` correctness fix (Finding 1), then the category parity
  sweep (Finding 7).
- TDD applies to every finding. For Findings 2–6 the natural homes are
  `src/test/groovy/io/velo/acl/UTest.groovy`, `RCmdTest.groovy`, `CategoryTest.groovy`,
  `AclUsersTest.groovy` and `src/test/groovy/io/velo/command/AGroupTest.groovy`.
- After each fix, run the focused test and then confirm the touched lines/branches were actually
  executed in `build/reports/jacocoHtml/` (`scripts/jacoco_cover.py` is available for a quick
  line-range check).
