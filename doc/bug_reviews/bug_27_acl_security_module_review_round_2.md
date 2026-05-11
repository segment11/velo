# Bug 27 - ACL Security Module Review (Round 2)

Reviewer: AI agent 2
Review date: 2026-05-11
Branch: `main`
Reviewed HEAD: `0f0ba6897c2d7399343ace9fd86aba2d8aae9815`
Scope: ACL security module follow-up review after Round 1 fixes. Focus areas were ACL command semantics, authentication gates, command/key/channel authorization, ACL persistence, and replication.

## Files Reviewed

- `src/main/java/io/velo/acl/U.java`
- `src/main/java/io/velo/acl/RCmd.java`
- `src/main/java/io/velo/acl/RKey.java`
- `src/main/java/io/velo/acl/RPubSub.java`
- `src/main/java/io/velo/acl/Category.java`
- `src/main/java/io/velo/acl/AclUsers.java`
- `src/main/java/io/velo/command/AGroup.java`
- `src/main/java/io/velo/command/MGroup.java`
- `src/main/java/io/velo/command/PGroup.java`
- `src/main/java/io/velo/command/SGroup.java`
- `src/main/java/io/velo/BaseCommand.java`
- `src/main/java/io/velo/RequestHandler.java`
- `src/main/java/io/velo/MultiWorkerServer.java`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java`
- `src/test/groovy/io/velo/acl/UTest.groovy`
- `src/test/groovy/io/velo/command/AGroupTest.groovy`
- `src/test/groovy/io/velo/RequestHandlerTest.groovy`
- `doc/bug_reviews/bug_27_acl_security_module_review_round_1.md`
- `doc/design/10_acl_security_design.md`

---

## Finding 1: `ACL DRYRUN` executes the checked command, enabling privilege escalation through another user

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/AGroup.java:144-173`

**Code excerpt:**

```java
} else if ("dryrun".equals(subCmd)) {
    ...
    var redirectRequest = new Request(dd, false, false);
    redirectRequest.setU(u);
    redirectRequest.setSlotWithKeyHashList(slotWithKeyHashListParsed);

    var aclCheckResult = redirectRequest.isAclCheckOk();
    if (!aclCheckResult.asBoolean()) {
        return aclCheckResult.isKeyFail() ? ErrorReply.ACL_PERMIT_KEY_LIMIT : ErrorReply.ACL_PERMIT_LIMIT;
    }

    return requestHandler.handle(redirectRequest, socket);
}
```

**Root cause:**

`ACL DRYRUN` should report whether a user would be allowed to run the supplied command. After performing the authorization check, this implementation dispatches the synthetic request into `requestHandler.handle(...)`, which runs the command for real.

The synthetic request is also bound to the target user passed in `ACL DRYRUN <user> ...`, not necessarily the currently authenticated user. A user who is allowed to run `ACL DRYRUN` can therefore execute commands with the privileges of the named target user.

**Impact:**

- `ACL DRYRUN default SET key value` can write data instead of only returning an authorization result.
- A limited operator with only `+acl|dryrun` can choose a more privileged target user and execute commands that the operator is not directly allowed to run.
- Mutating commands, administrative commands, and data reads can all be reached if the target user has the corresponding ACL grants.
- Existing tests check dry-run replies but do not assert that target commands are not executed.

**Suggested fix:**

Stop after the ACL check and return a dry-run result reply. Do not call the normal command handler from `ACL DRYRUN`. Add regressions proving `ACL DRYRUN` for `SET`, `DEL`, or another mutating command leaves storage unchanged and cannot be used to run as a different user.

---

## Finding 2: `%R~` and `%W~` key selectors are ignored because every command is checked as both read and write

**Severity:** High

**Files:**

- `src/main/java/io/velo/acl/U.java:592-604`
- `src/main/java/io/velo/acl/RKey.java:42-52`
- `src/main/java/io/velo/acl/Category.java:37-38`, `src/main/java/io/velo/acl/Category.java:770-874`

**Code excerpt:**

```java
for (var rKey : rKeyList) {
    // todo, cmd read / write, need to check
    if (rKey.match(slotWithKeyHash.rawKey(), true, true)) {
        matched = true;
        break;
    }
}
```

```java
} else if (type == Type.read) {
    return KeyLoader.isKeyMatch(key, pattern) && cmdRead;
} else if (type == Type.write) {
    return KeyLoader.isKeyMatch(key, pattern) && cmdWrite;
}
```

**Root cause:**

`RKey` supports read-only (`%R~`) and write-only (`%W~`) key permissions, but `U.checkCmdAndKey()` always passes `cmdRead=true` and `cmdWrite=true`. The command category model already has a write-command set through `Category.isWriteCmd(...)`, but that information is not used by key ACL checks.

**Impact:**

- A user configured with `+@all %R~tenant:*` can still run write commands such as `SET tenant:1 value`, because `%R~tenant:*` matches when `cmdRead` is forced to true.
- A user configured with `+@all %W~tenant:*` can still run read commands such as `GET tenant:1`, because `cmdWrite` is forced to true.
- This defeats Redis-style read/write key separation and can expose or mutate data across tenant boundaries.

**Suggested fix:**

Derive command access mode from command metadata before calling `RKey.match(...)`. At minimum, pass `cmdWrite = Category.isWriteCmd(cmd)` and `cmdRead = !cmdWrite` for commands that are clearly single-mode. Mixed read/write commands such as `SORT ... STORE`, `LMOVE`, `SMOVE`, `GETDEL`, and migration-style commands need explicit handling and tests.

---

## Finding 3: `nopass` is sticky, so adding passwords to an existing `nopass` user does not require authentication

**Severity:** High

**Files:**

- `src/main/java/io/velo/acl/U.java:86-88`
- `src/main/java/io/velo/acl/U.java:197-203`
- `src/main/java/io/velo/command/AGroup.java:330-336`

**Code excerpt:**

```java
boolean check(String passwordRaw) {
    if (isNoPass()) {
        return true;
    }
```

```java
} else if ("nopass".equals(rule)) {
    u.addPassword(U.Password.NO_PASSWORD);
} else if ("resetpass".equals(rule)) {
    u.resetPassword();
} else if (rule.startsWith(U.ADD_PASSWORD_PREFIX)) {
    var password = rule.substring(1);
    u.addPassword(U.Password.plain(password));
}
```

**Root cause:**

`nopass` is stored as a sentinel password that accepts any raw password. `ACL SETUSER ... >password` appends a password but does not remove the existing `nopass` sentinel. The initial default user starts with `nopass`, and `ACL SETUSER default >secret` therefore leaves `default` accepting any password unless the operator also knows to include `resetpass`.

**Impact:**

- Attempting to secure an existing `nopass` user by adding a password can silently fail open.
- `ACL SETUSER default >secret` leaves the default user passwordless because `nopass` remains in the password list.
- ACL files can also preserve mixed `nopass` plus hashed-password state because `U.literal()` serializes all password entries.

**Suggested fix:**

Make password rules mutually consistent: adding a real password should remove `nopass`, and applying `nopass` should clear existing passwords before adding the sentinel. Add regressions for `ACL SETUSER default >secret` and `ACL SETUSER user nopass >secret` to prove only the intended authentication mode remains.

---

## Finding 4: Failed `ACL SETUSER` commands can partially mutate users before returning an error

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/AGroup.java:315-381`
- `src/main/java/io/velo/RequestHandler.java:514-529`

**Code excerpt:**

```java
try {
    aclUsers.upInsert(user, u -> {
        for (int i = 3; i < data.length; i++) {
            var rule = new String(data[i]);
            if ("on".equals(rule)) {
                u.setOn(true);
            ...
            } else {
                throw new IllegalArgumentException("Invalid acl rule: " + rule);
            }
        }
    });

    appendAclUpdateBinlog();
    return OKReply.INSTANCE;
} catch (AclInvalidRuleException e) {
    log.error("Set user error={}", e.getMessage());
    return ErrorReply.ACL_SETUSER_RULE_INVALID;
}
```

**Root cause:**

`ACL SETUSER` applies rules directly to the live `U` object as it parses each argument. If a later argument is invalid, earlier mutations remain in memory. The handler only catches `AclInvalidRuleException`; `IllegalArgumentException` from invalid rules or invalid categories bubbles to `RequestHandler`, which returns a generic error after the mutation has already happened.

**Impact:**

- `ACL SETUSER victim on +@all ~* invalid-rule` returns an error, but `victim` can remain enabled with broad permissions.
- The failed update is not appended to the ACL binlog because `appendAclUpdateBinlog()` is after the callback, so master memory, ACL file, and replicas can diverge.
- This is an authorization safety issue because clients and operators can trust a failed ACL command while the live ACL state has changed.

**Suggested fix:**

Parse and validate the full rule list into a temporary user state before mutating the live registry. Apply the new state only after all arguments are valid. Catch validation failures consistently and add a regression proving a failed `ACL SETUSER` leaves the previous user unchanged.

---

## Finding 5: Category ACLs allow uncategorized Velo commands under any category

**Severity:** Medium-High

**Files:**

- `src/main/java/io/velo/acl/RCmd.java:56-66`
- `src/main/java/io/velo/acl/Category.java:893-895`
- `src/main/java/io/velo/command/MGroup.java:51-67`, `src/main/java/io/velo/command/MGroup.java:509-515`

**Code excerpt:**

```java
var categoryList = Category.getCategoryListByCmd(cmd);
if (categoryList == null) {
    // velo extra commands
    return true;
}
```

```java
if ("manage".equals(cmd)) {
    return manage();
}
```

**Root cause:**

For category rules like `+@read`, `+@string`, or `+@pubsub`, `RCmd.match()` treats commands with no category mapping as a match. That makes every category rule authorize every uncategorized Velo-specific command.

`manage` is implemented in `MGroup` and backed by dynamic Groovy handlers, but it does not appear in the category map. It includes operational actions such as slot inspection, LRU configuration, migration, dictionary management, and debug controls.

**Impact:**

- A user granted a narrow category such as `+@read` can pass the command ACL check for `manage`.
- Any future command omitted from `Category` will be allowed by every category grant until explicitly categorized.
- This is especially dangerous for operational or dynamic commands because they often have no ordinary key access pattern to constrain them.

**Suggested fix:**

Unknown commands should not match category ACLs by default. Either categorize every supported command, add a dedicated Velo category for Velo-specific commands, or require explicit `+command` grants for uncategorized commands. Add tests for `+@read` denying `manage` unless `+manage` or an appropriate category is present.

---

## Finding 6: HTTP metrics and HAProxy helper endpoints bypass password and ACL checks

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/RequestHandler.java:285-352`
- `src/main/java/io/velo/RequestHandler.java:360-430`

**Code excerpt:**

```java
// http special handle
if (request.isHttp() && data.length == 1) {
    ...
    if (Arrays.equals(firstDataBytes, URL_QUERY_METRICS_BYTES)) {
        ...
        return new BulkReply(sw.toString());
    }
    ...
    switch (firstDataString) {
        case URL_QUERY_FOR_HAPROXY_FILTER_MASTER -> {
            ...
            return new BulkReply("master");
        }
```

```java
// http basic auth
if (request.isHttp()) {
    if (SocketInspector.getAuthUser(socket) == null && ConfForGlobal.PASSWORD != null) {
        ...
    }
}
```

**Root cause:**

HTTP one-token special endpoints are handled and returned before the HTTP basic-auth block. When `ConfForGlobal.PASSWORD` is configured, normal HTTP commands require Basic auth, but `?metrics`, `?master`, `?master_or_slave`, `?slave`, and `?slave_with_zone=...` never reach that gate.

**Impact:**

- A password-protected Velo instance still exposes Prometheus metrics without authentication.
- HAProxy helper endpoints leak leadership and zone status without authentication.
- If deployment assumes `PASSWORD` protects the HTTP surface, these early returns violate that security boundary.

**Suggested fix:**

Move the HTTP auth gate before special endpoint handling, or explicitly document and configure which health endpoints are public. Metrics should usually require authentication or a separate bind/listener intended for monitoring.

---

## Finding 7: `ACL LOAD` changes live ACL state but is not replicated through the ACL binlog

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/command/AGroup.java:222-237`
- `src/main/java/io/velo/command/AGroup.java:300-304`
- `src/main/java/io/velo/command/AGroup.java:315-377`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java:98-104`

**Code excerpt:**

```java
} else if ("load".equals(subCmd)) {
    ...
    try {
        aclUsers.loadAclFile();
        return OKReply.INSTANCE;
    } catch (Exception e) {
        return new ErrorReply(e.getMessage());
    }
}
```

```java
FileUtils.writeLines(aclFile, "UTF-8", lines);

appendAclUpdateBinlog();
return OKReply.INSTANCE;
```

**Root cause:**

`ACL SAVE`, `ACL SETUSER`, and `ACL DELUSER` call `appendAclUpdateBinlog()`, and replicas apply those lines through `XAclUpdate.apply(...)`. `ACL LOAD` replaces the in-memory user registry from the local ACL file but does not append any ACL update to the binlog.

**Impact:**

- A master can load stricter ACL rules from disk while replicas continue serving with older, more permissive in-memory ACLs.
- The ACL file and memory state can diverge across nodes after a successful runtime `ACL LOAD`.
- Failover can promote a replica with stale ACL policy.

**Suggested fix:**

Decide whether `ACL LOAD` is intended to be local-only or cluster-wide. If it is cluster-wide, replicate either the loaded ACL literals or a deterministic replacement command. If it is local-only, document that behavior and consider returning an error when binlog/replication is enabled to avoid a false sense of consistency.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - `ACL DRYRUN` executes the checked command | High | **Confirmed** | High |
| 2 - `%R~` / `%W~` key selectors are checked as both read and write | High | **Confirmed** | High |
| 3 - `nopass` remains active after adding real passwords | High | **Confirmed** | High |
| 4 - Failed `ACL SETUSER` can partially mutate live users | High | **Confirmed** | High |
| 5 - Category ACLs allow uncategorized Velo commands under any category | Medium-High | **Confirmed** | High |
| 6 - HTTP metrics/HAProxy helpers bypass auth | Medium | **Confirmed** | Medium-High |
| 7 - `ACL LOAD` is not replicated | Medium | **Confirmed** | Medium-High |

## Suggested Fix Order

1. Fix Finding 1 first. `ACL DRYRUN` is a direct command-execution and privilege-escalation path.
2. Fix Finding 3 next, especially for the `default` user. Adding a password must disable `nopass`.
3. Fix Finding 4 so failed ACL changes cannot leave partially applied security state.
4. Fix Finding 2 to make key read/write selectors enforce their advertised separation.
5. Fix Finding 5 by making unknown commands deny-by-default for category ACLs or by categorizing Velo commands explicitly.
6. Fix Finding 6 according to the intended HTTP deployment model.
7. Fix or document Finding 7 based on the intended replication semantics for `ACL LOAD`.

## Verification

- Ran focused baseline tests after writing the review: `./gradlew :test --tests "io.velo.acl.UTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.RequestHandlerTest"` - passed.
- JaCoCo report was generated by the test task. No production code was changed in this review round, so no changed-code coverage claim is made here.

---

## Review Feedback - Bug 1 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `17e1a5e49c1c2d7e15a544b4cd299e8d306f7f66` (`fix: ACL DRYRUN must not execute the target command`)

### Findings

No blocking issues found.

### Summary of Fix

The commit changes `ACL DRYRUN` so that, after constructing the synthetic request and checking the target user's ACL permissions, it returns `OKReply.INSTANCE` instead of dispatching the synthetic request through `requestHandler.handle(...)`. This removes the command execution and privilege-escalation path from Finding 1.

### Strengths

- The production change is the minimal behavior change needed for the bug: the dry-run ACL check remains, but command execution is removed.
- The regression updates the expected allowed dry-run reply from the old executed-command result to `OKReply.INSTANCE`.
- The new `SET` dry-run regression asserts that the target key is unchanged after `ACL DRYRUN a set dryrun-test-key value`.

### Concerns

- Non-blocking test-strength note: JaCoCo still reports one missed branch on the existing key-failure ternary in the dry-run denial path (`AGroup.java:170`). The fixed success path and new non-execution return are covered, so this does not block Bug 1.
- The exact Redis-compatible success text for `ACL DRYRUN` is not implemented; the command returns simple `OK`. That is a compatibility follow-up, not a security blocker for this finding.

### Verification

- Ran focused test with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.command.AGroupTest"` - passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.command/AGroup.java.html`.
  - `AGroup.java:164-169` dry-run synthetic request construction and ACL check are covered.
  - `AGroup.java:173` new `return OKReply.INSTANCE` line is covered.

### Follow-ups

- Pre-commit: none required for Bug 1.
- Post-commit: optionally add coverage for the dry-run key-failure response and decide whether to return a Redis-compatible dry-run success message instead of `OK`.

---

## Review Feedback - Bug 3 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `0251005f3e73ccddd04031e94f34dbcfce5af56a` (`fix: adding password removes nopass, nopass clears all passwords`)

### Findings

No blocking issues found.

### Summary of Fix

The commit updates `U.addPassword()` so password modes are mutually exclusive:

- adding `nopass` clears the existing password list before adding the sentinel
- adding any real password first removes an existing `nopass` sentinel

This addresses the Bug 3 fail-open path where `ACL SETUSER default >secret` could leave the original `nopass` sentinel active.

### Strengths

- The fix is centralized in `U.addPassword()`, so it applies to `ACL SETUSER >plain`, `ACL SETUSER #hash`, ACL file parsing, config seeding, and test helpers that add passwords directly.
- The regression covers both directions: real password after `nopass`, and `nopass` after existing real passwords.
- The literal assertion verifies saved ACL output no longer contains `nopass` after a real password replaces it.

### Concerns

- Non-blocking test-strength note: the regression is at the `U` unit level rather than an `ACL SETUSER default >secret` integration test. Since `AGroup` reaches this exact method for both plain and hashed password additions, the behavior is covered at the core state transition.
- The test assertion `u.checkPassword('pw1')` after adding `nopass` succeeds because `nopass` accepts any password, not because `pw1` remains. The preceding size and sentinel assertions prove the intended state, so this is not a blocker.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.RequestHandlerTest"` - passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/U.java.html`.
  - `U.java:198` `password.isNoPass()` branch: all branches covered.
  - `U.java:199` `passwords.clear()` for `nopass`: covered.
  - `U.java:201` `passwords.removeIf(Password::isNoPass)` for real passwords: covered.
  - `U.java:204-207` duplicate check and add path: covered.

### Follow-ups

- Pre-commit: none required for Bug 3.
- Post-commit: optionally add an `AGroupTest` integration case for `ACL SETUSER default >secret` and `ACL SETUSER user nopass >secret` to lock the command-level behavior directly.

---

## Review Feedback - Bug 2 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `107e747e342d172dbffab9f443b70ea1a4136667` (`fix: enforce %R~/%W~ key selectors using command write category`)

### Findings

1. **High - `%W~` still allows reads through write commands that return values**

   The fix now derives one access mode per command:

   - `src/main/java/io/velo/acl/U.java:602-605`

   ```java
   boolean isWrite = Category.isWriteCmd(cmd);
   ...
   if (rKey.match(slotWithKeyHash.rawKey(), !isWrite, isWrite)) {
   ```

   This fixes the simple `GET` vs `SET` cases, but it does not handle commands that both read and write the same key. `Category.writeCmdList` classifies `getdel`, `getex`, and `getset` as write commands (`Category.java:792-794`). Those commands return the previous value to the client:

   - `src/main/java/io/velo/command/GGroup.java:131-143` (`GETDEL` returns `new BulkReply(valueBytes)`)
   - `src/main/java/io/velo/command/GGroup.java:146-205` (`GETEX` returns `new BulkReply(valueBytes)`)
   - `src/main/java/io/velo/command/GGroup.java:262-279` (`GETSET` returns `new BulkReply(valueBytesExist)`)

   Because these commands are categorized as write-only by the new check, a user with `+@all %W~data:*` can still pass key ACL and read `data:x` through `GETDEL`, `GETEX`, or `GETSET`. That leaves the original `%W~ allows reads` security impact partially unresolved.

### Summary of Fix

The commit replaces the hard-coded `rKey.match(..., true, true)` call with command-category-derived access mode. Tests were added for a `%R~` user denying `SET`, a `%W~` user denying `GET`, and a user with both selectors allowing both simple read and simple write commands.

### Strengths

- The prior unconditional read/write match is removed.
- The simple read-only and write-only selector regressions directly cover the original easy bypass cases.
- JaCoCo confirms the new `Category.isWriteCmd(cmd)` path and `rKey.match(..., !isWrite, isWrite)` branch are covered.

### Concerns

- The fix needs per-command key access metadata, not only command-level write categorization. Commands that read and write, and commands with source/destination keys, need explicit read/write requirements per key.
- The current tests do not cover mixed read/write commands such as `GETDEL`, `GETEX`, `GETSET`, `LMOVE`, `SMOVE`, `RENAME`, `RESTORE`, or `GEOSEARCHSTORE`.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.RKeyTest" --tests "io.velo.acl.CategoryTest"` - passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/U.java.html`.
  - `U.java:602` `Category.isWriteCmd(cmd)` line is covered.
  - `U.java:605` `rKey.match(slotWithKeyHash.rawKey(), !isWrite, isWrite)` has all four branches covered.
- Inspected command/category evidence for the remaining gap:
  - `Category.java:792-794` includes `getdel`, `getex`, and `getset` in the write command list.
  - `GGroup.java:131-143`, `GGroup.java:146-205`, and `GGroup.java:262-279` show those commands return existing values.

### Follow-ups

- Pre-commit: add regressions proving `%W~` alone cannot read via `GETDEL`, `GETEX`, or `GETSET`, then introduce per-command/per-key access requirements so mixed commands require read access where they return existing values.
- Post-commit: broaden the metadata/tests for multi-key mixed commands such as `LMOVE`, `SMOVE`, `RENAME`, `RESTORE`, and `GEOSEARCHSTORE`.

---

## Review Feedback - Bug 2 Follow-up Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `7c3624b37fdd81bf1cd2cf78498ddbbd8dbd7175` (`fix: deny %R~/%W~ for read-write commands like GETDEL, GETSET`)

### Findings

1. **High - mixed read/write command coverage is still incomplete**

   The follow-up adds a coarse read/write command set and makes `%R~` or `%W~` alone fail when `needsBoth` is true:

   - `src/main/java/io/velo/acl/Category.java:882-892`
   - `src/main/java/io/velo/acl/U.java:602-608`
   - `src/main/java/io/velo/acl/RKey.java:46-54`

   This closes the previously cited `GETDEL`, `GETEX`, and `GETSET` bypasses, but the same leak remains for commands that are still categorized as plain writes. `RPOPLPUSH`, `BRPOPLPUSH`, and `BLMOVE` are in the write category but are not in `readWriteCmdSet`:

   - `src/main/java/io/velo/acl/Category.java:780` (`blmove`)
   - `src/main/java/io/velo/acl/Category.java:783` (`brpoplpush`)
   - `src/main/java/io/velo/acl/Category.java:834` (`rpoplpush`)
   - `src/main/java/io/velo/acl/Category.java:882-892` omits all three

   These commands remove a value from the source list, write it to the destination list, and return the moved value to the client:

   - `src/main/java/io/velo/command/RGroup.java:481-488`
   - `src/main/java/io/velo/command/RGroup.java:459-460`
   - `src/main/java/io/velo/command/RGroup.java:572-590`
   - `src/main/java/io/velo/command/BGroup.java:103-128`
   - `src/main/java/io/velo/command/LGroup.java:367-369`

   Because the ACL path still treats those commands as write-only, a user with only write key access to the source can read list data through `RPOPLPUSH`/`BRPOPLPUSH`/`BLMOVE`. That means Bug 2 is not fully fixed.

2. **High - `COPY` can still read a source key through write-only source access**

   `COPY` is also in the write category but omitted from `readWriteCmdSet`:

   - `src/main/java/io/velo/acl/Category.java:786`
   - `src/main/java/io/velo/acl/Category.java:882-892`

   The command parses both source and destination keys (`src/main/java/io/velo/command/CGroup.java:29-35`), reads the source value (`src/main/java/io/velo/command/CGroup.java:204-205`), and writes it to the destination (`src/main/java/io/velo/command/CGroup.java:227-239`). A user with `%W~secret:* %W~public:* %R~public:*` can copy data out of a write-only source key into a readable destination and then read it. This is still a `%W~` read bypass, even though `COPY` itself only returns an integer.

### Summary of Fix

The commit adds `Category.isReadWriteCmd(cmd)`, marks several known mixed commands as requiring both read and write key selectors, and extends `RKey.match()` with a `needsBoth` flag so `%R~` and `%W~` alone are rejected for those commands.

### Strengths

- The exact prior examples `GETDEL`, `GETEX`, and `GETSET` are now covered by `readWriteCmdSet`.
- New regression tests prove `%W~` and `%R~` alone are denied for `GETDEL`.
- JaCoCo confirms the new `Category.isReadWriteCmd()` and `RKey.match(..., needsBoth)` paths were executed.

### Concerns

- The command-level `needsBoth` model is still too coarse. Source/destination commands need per-key access requirements; otherwise fixes either miss leaks (`COPY`, `RPOPLPUSH`, `BRPOPLPUSH`, `BLMOVE`) or over-deny valid access patterns.
- There are likely compatibility regressions from marking whole commands as read/write regardless of options. For example, `SORT` without `STORE` is read-only, while `RESTORE` is primarily destination-write; requiring `%RW~` for every key is safe but stricter than Redis ACL semantics.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.RKeyTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.command.RGroupTest" --tests "io.velo.command.BGroupTest" --tests "io.velo.command.CGroupTest"` - passed.
- Inspected JaCoCo HTML reports:
  - `build/reports/jacocoHtml/io.velo.acl/Category.java.html`: `Category.java:31`, `Category.java:34`, and `Category.java:882` covered.
  - `build/reports/jacocoHtml/io.velo.acl/U.java.html`: `U.java:602-603` and `U.java:608` covered.
  - `build/reports/jacocoHtml/io.velo.acl/RKey.java.html`: `RKey.java:47-52` covered, with one pre-existing branch gap on line 50.

### Follow-ups

- Pre-commit: add failing regressions for `%W~` users attempting `RPOPLPUSH`, `BRPOPLPUSH`, `BLMOVE`, and `COPY` from write-only source keys.
- Pre-commit: replace the coarse command-level `needsBoth` set with per-command/per-key access metadata so source keys can require read/write while destination keys can require write only.
- Post-commit: add option-sensitive coverage for commands such as `SORT`, `RESTORE`, `RENAME`, and `RENAMENX` to avoid unnecessary ACL incompatibility while preserving the security boundary.

---

## Review Feedback - Bug 2 Second Follow-up Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `8373ad9585840a12d6f547d37e3a0330c91f60a3` (`fix: add RPOPLPUSH, BRPOPLPUSH, BLMOVE, COPY to readWrite command set`)

### Findings

1. **High - `%W~` can still read values through destructive pop commands**

   The commit adds the previously reported `RPOPLPUSH`, `BRPOPLPUSH`, `BLMOVE`, and `COPY` cases to `readWriteCmdSet`:

   - `src/main/java/io/velo/acl/Category.java:882-897`

   That closes the specific follow-up findings, but the same bug remains for other write-category commands that return removed data and are still omitted from `readWriteCmdSet`:

   - `src/main/java/io/velo/acl/Category.java:781-782` (`blpop`, `brpop`)
   - `src/main/java/io/velo/acl/Category.java:812` (`lpop`)
   - `src/main/java/io/velo/acl/Category.java:833` (`rpop`)
   - `src/main/java/io/velo/acl/Category.java:847` (`spop`)
   - `src/main/java/io/velo/acl/Category.java:865-866` (`zpopmax`, `zpopmin`)
   - `src/main/java/io/velo/acl/Category.java:882-897` omits those commands

   These commands mutate the key and return the removed values:

   - `src/main/java/io/velo/command/LGroup.java:421-436` returns popped list elements for `LPOP`; `RPOP` delegates to the same implementation in reverse.
   - `src/main/java/io/velo/command/BGroup.java:895-901` returns the key name and popped list value for `BLPOP`/`BRPOP`.
   - `src/main/java/io/velo/command/SGroup.java:1716-1741` returns popped set members for `SPOP`.
   - `src/main/java/io/velo/command/ZGroup.java:1297-1305` returns popped sorted-set members and scores for `ZPOPMAX`/`ZPOPMIN`.

   Since `U.checkCmdAndKey()` still treats any write command outside `readWriteCmdSet` as `cmdRead=false, cmdWrite=true` (`src/main/java/io/velo/acl/U.java:602-608`), a user with `+@all %W~data:*` can still read data from `data:*` using these pop commands. Bug 2 is therefore still not fully fixed.

### Summary of Fix

The commit extends the existing command-level `readWriteCmdSet` with `blmove`, `rpoplpush`, `brpoplpush`, and `copy`, and adds a `UTest` regression proving `%W~` denies `RPOPLPUSH`, `BLMOVE`, and `COPY`.

### Strengths

- The previously cited `RPOPLPUSH`, `BRPOPLPUSH`, `BLMOVE`, and `COPY` gaps are now present in the read/write command set.
- The new test covers three of the four newly added commands at the ACL-check layer.
- Fresh focused tests pass, and JaCoCo confirms the new ACL read/write path is exercised.

### Concerns

- The fix remains a manually curated command-name deny list for `%R~`/`%W~` single-mode selectors. It is missing additional commands with the same data-exposure property, so each follow-up can still leave equivalent bypasses.
- The new test name includes `BRPOPLPUSH`, but the test body does not exercise it directly; it only covers `RPOPLPUSH`, `COPY`, and `BLMOVE`.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.RKeyTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.command.LGroupTest" --tests "io.velo.command.SGroupTest" --tests "io.velo.command.ZGroupTest" --tests "io.velo.command.BGroupTest"` - passed.
- Inspected test report: `build/reports/tests/test/classes/io.velo.acl.UTest.html` shows `test %W~ denies RPOPLPUSH, BLMOVE, COPY` passed.
- Inspected JaCoCo HTML reports:
  - `build/reports/jacocoHtml/io.velo.acl/Category.java.html`: `Category.java:31`, `Category.java:34`, and `Category.java:882` covered.
  - `build/reports/jacocoHtml/io.velo.acl/U.java.html`: `U.java:603` and `U.java:608` covered.

### Follow-ups

- Pre-commit: add failing regressions for `%W~` users attempting `LPOP`, `RPOP`, `BLPOP`, `BRPOP`, `SPOP`, `ZPOPMAX`, and `ZPOPMIN` on write-only keys.
- Pre-commit: stop patching this as a manually maintained command-name list. Define the key access requirement from command/key specs so destructive reads, source keys, destination keys, and option-sensitive commands are handled consistently.

---

## Review Feedback - Bug 2 Third Follow-up Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `0e083eba3135e9aeb827e8bc28a4fb38dbc47034` (`fix: add destructive pop commands to readWrite set (LPOP, RPOP, SPOP, etc.)`)

### Findings

1. **High - latest commit still missed write commands that return prior string data**

   The commit adds `lpop`, `rpop`, `blpop`, `brpop`, `spop`, `zpopmax`, and `zpopmin` to `readWriteCmdSet`, which closes the previous destructive-pop bypass:

   - `src/main/java/io/velo/acl/Category.java:890-896`

   However, two supported string commands still allowed a `%W~` user to observe existing key data:

   - `SET key value GET` returns the previous string value from `src/main/java/io/velo/command/SGroup.java:605-610`, but plain `set` was not read/write-gated.
   - `SETBIT key offset bit` returns the previous bit from `src/main/java/io/velo/command/SGroup.java:653-665`, but `setbit` was not in `readWriteCmdSet`.

   A command-wide `set` entry would over-deny normal `SET`, so this required option-sensitive detection for `SET ... GET`.

### Summary of Fix Reviewed

The commit extended the coarse read/write command set with the destructive pop commands and added ACL unit coverage for most of them.

### Strengths

- The previously reported `LPOP`, `RPOP`, `BLPOP`, `BRPOP`, `SPOP`, `ZPOPMAX`, and `ZPOPMIN` class is now represented in the read/write command set.
- The new test preserves the security boundary for most destructive pop commands.

### Concerns

- The test initially omitted direct `ZPOPMIN` and `BRPOPLPUSH` assertions even though both are in the set.
- The coarse command-name model still missed option-sensitive `SET ... GET`; this is the clearest reason the ACL check needs either command/key metadata or at least data-aware classification.

### Local Follow-up Patch

AI agent 2 added a local follow-up patch after reviewing the commit:

- `src/main/java/io/velo/acl/Category.java:37-50` adds `isReadWriteCmd(String cmd, byte[][] data)` so `SET ... GET` is read/write while plain `SET` remains write-only.
- `src/main/java/io/velo/acl/Category.java:913` adds `setbit` to `readWriteCmdSet`.
- `src/main/java/io/velo/acl/U.java:603` switches ACL key checking to the data-aware classifier.
- `src/test/groovy/io/velo/acl/UTest.groovy:520-548` adds regression coverage proving `%W~` allows plain `SET` but denies `SET ... GET` and `SETBIT`.
- `src/test/groovy/io/velo/acl/UTest.groovy:462` and `src/test/groovy/io/velo/acl/UTest.groovy:512` add direct assertions for `BRPOPLPUSH` and `ZPOPMIN`.

### Verification

- Red check: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest.test %W~ denies write commands that return previous string values"` failed before production changes at `UTest.groovy:547`, proving `%W~` still allowed `SET ... GET`.
- Green check: the same focused test passed after the local patch.
- Ran broader focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.RKeyTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.command.SGroupTest" --tests "io.velo.command.BGroupTest" --tests "io.velo.command.LGroupTest" --tests "io.velo.command.ZGroupTest"` - passed.
- Inspected test report: `build/reports/tests/test/classes/io.velo.acl.UTest.html` shows the new `%W~` string-return regression and the destructive pop regressions passed.
- Inspected JaCoCo HTML reports:
  - `build/reports/jacocoHtml/io.velo.acl/Category.java.html`: `Category.java:38`, `Category.java:42-43`, and the `SET ... GET` loop are covered; `Category.java:42` has one expected missed branch for the `data == null` defensive case.
  - `build/reports/jacocoHtml/io.velo.acl/U.java.html`: `U.java:603` and `U.java:608` covered.

### Follow-ups

- Pre-commit: review whether numeric mutation commands such as `INCR*`, `DECR*`, `HINCRBY*`, and `ZINCRBY` should be treated as read/write because their responses reveal state derived from the old value. This is a policy decision, but it should be explicit.
- Post-commit: replace the coarse read/write command set with command/key access metadata so option-sensitive and source/destination semantics are not handled by repeated special cases.

---

## Review Feedback - Bug 4 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `e574a6e37af1c4407103a07f70ff9cadb1abf1d9` (`fix: ACL SETUSER validates all rules on temp copy before applying`)

### Findings

No blocking issues found.

### Summary of Fix

The commit changes `ACL SETUSER` so rule application is staged on a temporary `U` instance:

- `src/main/java/io/velo/command/AGroup.java:317-318` creates a temp user and copies the current state into it.
- `src/main/java/io/velo/command/AGroup.java:320-376` applies every rule to the temp user.
- `src/main/java/io/velo/command/AGroup.java:378` copies temp state back to the live user only after all rules validate.
- `src/main/java/io/velo/command/AGroup.java:383-385` catches both `AclInvalidRuleException` and `IllegalArgumentException`, converting invalid rules into `ACL_SETUSER_RULE_INVALID`.
- `src/main/java/io/velo/acl/U.java:476-488` adds `copyStateFrom()` for complete ACL state copying.

This addresses the original bug: a failed later rule no longer leaves earlier successful rule mutations on the live user.

### Strengths

- The fix preserves existing `AclUsers.upInsert()` behavior while making the callback atomic from the live user's perspective.
- Invalid literal/category failures now return an ACL rule error instead of escaping as an uncaught `IllegalArgumentException`.
- The regression test verifies an existing enabled user remains enabled and keeps its password after a failed `ACL SETUSER victim off ...`.

### Concerns

- Non-blocking test-strength note: the regression verifies `isOn` and password state, but not command/key/channel lists. The code copies those lists through the same `copyStateFrom()` method and JaCoCo confirms those copy lines are executed, so this is not a blocker.
- Non-blocking follow-up: adding a new-user failure case such as `ACL SETUSER new on +@all ~* invalid` would directly cover the "failed command must not create a privileged user" scenario from the finding.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.command.AGroupTest" --tests "io.velo.acl.UTest" --tests "io.velo.acl.AclUsersTest" --tests "io.velo.RequestHandlerTest"` - passed.
- Inspected test report: `build/reports/tests/test/classes/io.velo.command.AGroupTest.html` shows `test acl setuser failed rule does not partially mutate` passed.
- Inspected JaCoCo HTML reports:
  - `build/reports/jacocoHtml/io.velo.command/AGroup.java.html`: `AGroup.java:317-318`, `AGroup.java:374`, `AGroup.java:378`, and `AGroup.java:383-384` covered.
  - `build/reports/jacocoHtml/io.velo.acl/U.java.html`: `U.java:477-488` covered for `copyStateFrom()`.

### Follow-ups

- Pre-commit: none required for Bug 4.
- Post-commit: add optional coverage for failed new-user creation and for command/key/channel state remaining unchanged after a failed update.

---

## Review Feedback - Bug 5 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `34445c1d770d32268454b0e12a8c0c920c51aaeb` (`fix: deny uncategorized commands in category ACL checks`)

### Findings

No blocking issues found.

### Summary of Fix

The commit changes category command matching so commands with no `Category` mapping no longer match arbitrary category grants:

- `src/main/java/io/velo/acl/RCmd.java:62-64` now returns `false` when `Category.getCategoryListByCmd(cmd)` is `null`.
- `src/test/groovy/io/velo/acl/RCmdTest.groovy:49-66` updates the category-rule expectations and adds direct checks that `manage` and another uncategorized command do not match `+@admin` or `+@read`.

This addresses the original bypass where a user with a narrow category grant such as `+@read` could pass command ACL for uncategorized Velo commands like `manage`.

### Strengths

- The fix is centralized in `RCmd.match()`, so it applies to every category ACL rule without requiring command-specific checks.
- Explicit command grants are preserved: `+manage` still uses `Type.cmd` matching and is not affected by category lookup.
- The change is fail-closed for future uncategorized commands.

### Concerns

- Non-blocking test-strength note: coverage is at the `RCmd` unit level rather than an end-to-end `U.checkCmdAndKey()` or `ACL DRYRUN` case. Since `U` delegates command-rule matching to `RCmd.match()`, the security behavior is covered at the core decision point.
- `+@all` and `allcommands` still match uncategorized commands through the explicit `Category.all` branch before category lookup. That appears intentional for all-command grants, but it is worth documenting as policy if Velo-specific operational commands should require explicit grants even for `+@all`.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.RCmdTest" --tests "io.velo.acl.UTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.RequestHandlerTest"` - passed.
- Inspected test report: `build/reports/tests/test/classes/io.velo.acl.RCmdTest.html` shows `test all` and `test from literal` passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/RCmd.java.html`.
  - `RCmd.java:62` category lookup covered.
  - `RCmd.java:63` null-category branch has all branches covered.
  - `RCmd.java:64` new deny return for uncategorized commands covered.
  - `RCmd.java:67-68` mapped-category iteration and match branch covered.
  - `RCmd.java:72` mapped-but-wrong-category deny return covered.

### Follow-ups

- Pre-commit: none required for Bug 5.
- Post-commit: optionally add an integration test showing a `+@read` user is denied for `manage`, while a user with explicit `+manage` is allowed.

---

## Review Feedback - Bug 6 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `094e73a75b73109ea596d448824d7ecf1e4c6a2e` (`fix: require HTTP auth for metrics, exempt HAProxy health endpoints`)

### Findings

1. **Medium - malformed one-token HTTP requests can now throw before returning `FORMAT`**

   The new auth gate reads the first HTTP token and immediately builds a `String` from it when `ConfForGlobal.PASSWORD` is set:

   - `src/main/java/io/velo/RequestHandler.java:287-291`

   ```java
   var firstDataBytes = data[0];
   ...
   var firstDataString = new String(firstDataBytes, StandardCharsets.UTF_8);
   ```

   The existing null check is still below the metrics check:

   - `src/main/java/io/velo/RequestHandler.java:344-346`

   Before this commit, `request.isHttp() && data.length == 1 && data[0] == null` reached that `FORMAT` return. With `PASSWORD` configured and no pre-authenticated socket, it now dereferences `null` at line 291 before the null check. That means a malformed HTTP request can escape the normal `ErrorReply.FORMAT` path and fail with a runtime exception. The auth gate should check `firstDataBytes == null` before `new String(...)`.

### Summary of Fix

The commit moves an auth gate into the one-token HTTP special handling path. It requires HTTP Basic auth for `?metrics` when `ConfForGlobal.PASSWORD` is set, but intentionally treats HAProxy helper endpoints (`master`, `master_or_slave`, `slave`, and `slave_with_zone...`) as public health checks.

### Strengths

- `?metrics` no longer bypasses password authentication when `PASSWORD` is configured.
- Basic auth parsing uses the same case-insensitive `Basic ` prefix and username/password validation pattern as the normal HTTP command path.
- The test proves unauthenticated `?metrics` returns `NO_AUTH` while HAProxy health endpoints remain reachable without credentials.

### Concerns

- The HAProxy endpoints are still intentionally unauthenticated. If the intended policy is that `PASSWORD` protects the entire HTTP surface, this remains a partial fix for the original finding. If health endpoints are meant to be public, this should be documented as policy.
- The new test does not cover successful Basic auth for `?metrics`, malformed Basic headers on the special endpoint path, disabled users, wrong passwords, or case-insensitive `Authorization` headers in this early gate. JaCoCo confirms those success/failure sub-branches are not covered.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.RCmdTest" --tests "io.velo.acl.UTest" --tests "io.velo.acl.CategoryTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.RequestHandlerTest"` - passed.
- Inspected test report: `build/reports/tests/test/classes/io.velo.RequestHandlerTest.html` shows `test http metrics requires auth when PASSWORD is set`, `test malformed http basic auth returns auth failed`, and `test handle` passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo/RequestHandler.java.html`.
  - `RequestHandler.java:287`, `RequestHandler.java:290-292`, `RequestHandler.java:296-299`, `RequestHandler.java:334`, and `RequestHandler.java:345` covered.
  - `RequestHandler.java:302`, `RequestHandler.java:308`, `RequestHandler.java:323`, and `RequestHandler.java:327` not covered, so the special-endpoint valid/malformed Basic-auth paths still need tests.

### Follow-ups

- Pre-commit: move the `firstDataBytes == null` check before the new auth gate, and add a regression with `PASSWORD` set for a one-token HTTP request with `data[0] == null`.
- Pre-commit: add a positive `?metrics` Basic-auth test and malformed Basic-auth tests for the special endpoint path.
- Post-commit: document whether HAProxy health endpoints are intentionally public when `PASSWORD` is configured, or add a config flag if deployments need those endpoints protected too.
