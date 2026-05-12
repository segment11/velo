# Bug 27 - ACL Security Module Review (Round 3)

Reviewer: AI agent 1
Review date: 2026-05-12
Branch: `review/acl-security`
Scope: ACL security module follow-up review after Round 1 and Round 2 fixes. Focus on remaining vulnerabilities in authentication, authorization, user lifecycle, and audit logging.

## Files Reviewed

- `src/main/java/io/velo/acl/U.java`
- `src/main/java/io/velo/acl/AclUsers.java`
- `src/main/java/io/velo/acl/RCmd.java`
- `src/main/java/io/velo/acl/RKey.java`
- `src/main/java/io/velo/acl/RPubSub.java`
- `src/main/java/io/velo/acl/Category.java`
- `src/main/java/io/velo/acl/LogRow.java`
- `src/main/java/io/velo/command/AGroup.java`
- `src/main/java/io/velo/BaseCommand.java`
- `src/main/java/io/velo/RequestHandler.java`
- `src/main/java/io/velo/MultiWorkerServer.java`
- `src/main/java/io/velo/decode/Request.java`
- `src/main/java/io/velo/ConfForGlobal.java`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java`
- `doc/bug_reviews/bug_27_acl_security_module_review_round_1.md`
- `doc/bug_reviews/bug_27_acl_security_module_review_round_2.md`

---

## Finding 1: Deleted user's authenticated connections bypass all ACL checks

**Severity:** High

**Files:**

- `src/main/java/io/velo/BaseCommand.java:189-196`
- `src/main/java/io/velo/decode/Request.java:126-130`
- `src/main/java/io/velo/MultiWorkerServer.java:467-475`

**Code excerpt:**

```java
// BaseCommand.java:189-196
public static @NotNull U getAuthU(ITcpSocket socket0) {
    var authUser = SocketInspector.getAuthUser(socket0);
    if (authUser == null) {
        var defaultUser = aclUsers.get(U.DEFAULT_USER);
        return defaultUser != null ? defaultUser : U.INIT_DEFAULT_U;
    }
    return aclUsers.get(authUser);  // returns null when user was deleted
}
```

```java
// Request.java:126-130
public U.CheckCmdAndKeyResult isAclCheckOk() {
    // for unit test
    if (u == null) {
        return U.CheckCmdAndKeyResult.TRUE;  // all commands pass!
    }
```

```java
// MultiWorkerServer.java:467-475
request.setU(BaseCommand.getAuthU(socket));
var aclCheckResult = request.isAclCheckOk();
if (!aclCheckResult.asBoolean()) {
    return Promise.of(...);
}
```

**Root cause:**

When a non-default user is deleted via `ACL DELUSER`, the `SocketInspector.getAuthUser(socket)` on existing connections still returns the deleted username. `BaseCommand.getAuthU()` then calls `aclUsers.get(authUser)` which returns `null` because the user no longer exists in the registry. The `@NotNull` annotation on `getAuthU()` is violated.

`request.setU(null)` is called, and `Request.isAclCheckOk()` has a fallback that returns `TRUE` when `u == null`, allowing all commands through the ACL gate. The comment says "for unit test" but this path executes in production.

Round 1 Finding 1 fixed this for the `default` user by protecting it from deletion, but non-default users are still vulnerable.

**Impact:**

- `ACL DELUSER alice` immediately grants alice's existing connections unrestricted access to all commands and keys.
- The bypass persists for the lifetime of the TCP connection — alice's connection can execute any command including administrative commands.
- This is a direct privilege escalation: deleting a restricted user elevates all their active sessions to full access.
- The `@NotNull` contract violation means static analysis tools will not flag downstream null dereferences.

**Suggested fix:**

1. `isAclCheckOk()` should return `FALSE_WHEN_CHECK_CMD` when `u == null` instead of `TRUE`. The unit-test comment is misleading; production code must not bypass ACL when the user cannot be resolved.
2. Optionally, `ACL DELUSER` should iterate open connections and clear the auth user on sockets belonging to the deleted user (similar to Redis behavior), or at minimum set those sockets to use the `default` user.

---

## Finding 2: `ACL SETUSER` `reset` rule does not reset command permissions

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/AGroup.java:373-380`
- `src/main/java/io/velo/acl/U.java:457-460`

**Code excerpt:**

```java
// AGroup.java:373-380
} else if ("reset".equals(rule)) {
    temp.setOn(false);
    temp.resetPassword();
    temp.resetKey();
    temp.resetPubSub();
    if (ValkeyRawConfSupport.aclPubsubDefault) {
        temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
    }
}
```

```java
// U.java:457-460 — resetCmd() exists but is never called from ACL SETUSER
public void resetCmd() {
    rCmdList.clear();
    rCmdDisallowList.clear();
}
```

**Root cause:**

Redis 6+ defines `reset` as equivalent to `off -@all resetpass resetkeys resetchannels`. The current implementation resets passwords, keys, and channels, but does **not** call `U.resetCmd()`. Command allow and deny lists (`rCmdList` and `rCmdDisallowList`) persist through `reset`.

`U.resetCmd()` exists (line 457-460) but is never invoked from any ACL SETUSER rule handler. There is also no `resetcommands` / `resetcmds` rule handler at all, so there is no ACL command to clear command rules without using explicit deny rules.

**Impact:**

- `ACL SETUSER admin on +@all ~* >secretpass reset on >newpass` leaves the user with `+@all` commands despite `reset`. An operator expecting `reset` to fully clear the user's state gets a user that retains all command privileges.
- There is no way through the ACL SETUSER interface to clear command rules. The only workaround is to deny all commands with `-@all` (or `nocommands`), but this adds deny rules rather than clearing the allow list — and the user would need to remove `-@all` before adding new allow rules.
- This violates the Redis ACL `reset` contract and can lead to privilege retention when operators rely on `reset` to strip a user's permissions.

**Suggested fix:**

Add `temp.resetCmd()` to the `reset` handler:

```java
} else if ("reset".equals(rule)) {
    temp.setOn(false);
    temp.resetPassword();
    temp.resetCmd();      // add this
    temp.resetKey();
    temp.resetPubSub();
    if (ValkeyRawConfSupport.aclPubsubDefault) {
        temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
    }
}
```

Also add a `resetcommands` / `resetcmds` rule handler for explicit command reset:

```java
} else if ("resetcommands".equals(rule) || "resetcmds".equals(rule)) {
    temp.resetCmd();
}
```

---

## Finding 3: `Password.check()` uses non-constant-time `String.equals()`, enabling timing side-channel attacks

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/U.java:86-96`

**Code excerpt:**

```java
// Password.check()
boolean check(String passwordRaw) {
    if (isNoPass()) {
        return true;
    }

    if (encodeType == PasswordEncodedType.plain) {
        return this.passwordEncoded.equals(passwordRaw);
    } else {
        return this.passwordEncoded.equals(DigestUtils.sha256Hex(passwordRaw));
    }
}
```

**Root cause:**

`String.equals()` performs short-circuit character-by-character comparison and returns `false` on the first mismatch. This creates a timing side channel: an attacker can measure response times to progressively guess the stored password or hash value one character at a time.

For SHA-256 encoded passwords, the attacker is comparing against a hex hash, not the raw password. However, if they can recover the full hash through timing analysis, they can perform offline dictionary attacks against the hash.

This is especially relevant for the RESP AUTH path (`RequestHandler.java:466`) and HTTP Basic auth path (`RequestHandler.java:440`), where the response time is directly observable by the client.

**Impact:**

- An attacker with network access can use timing analysis to recover password hashes character by character.
- Once the hash is recovered, offline brute-force attacks become feasible.
- The NOPASS path returns immediately with `true`, which also leaks information about whether a user has `nopass` enabled.

**Suggested fix:**

Use constant-time comparison for password verification:

```java
import java.security.MessageDigest;

boolean check(String passwordRaw) {
    if (isNoPass()) {
        return true;
    }

    String expected;
    String actual;
    if (encodeType == PasswordEncodedType.sha256Hex) {
        expected = this.passwordEncoded;
        actual = DigestUtils.sha256Hex(passwordRaw);
    } else {
        expected = this.passwordEncoded;
        actual = passwordRaw;
    }

    return MessageDigest.isEqual(
        expected.getBytes(StandardCharsets.UTF_8),
        actual.getBytes(StandardCharsets.UTF_8)
    );
}
```

---

## Finding 4: `ACL LOG` returns hardcoded fake data, preventing security audit

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/command/AGroup.java:256-297`

**Code excerpt:**

```java
} else if ("log".equals(subCmd)) {
    if (data.length != 3) {
        return ErrorReply.SYNTAX;
    }

    var countOrReset = new String(data[2]).toLowerCase();
    if ("reset".equals(countOrReset)) {
        // todo, clear log rows
        return OKReply.INSTANCE;
    } else {
        int count;
        try {
            count = Integer.parseInt(countOrReset);
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        // limit count
        if (count < 1 || count > 100) {
            return new ErrorReply("count must be between 1 and 1000");
        }

        var topReplies = new Reply[count];
        for (int i = 0; i < count; i++) {
            // todo, get log row
            var logRow = new LogRow();
            logRow.count = count;
            logRow.reason = "reason";
            logRow.context = "context";
            logRow.object = "object";
            logRow.username = "username";
            logRow.ageSeconds = 1.0;
            logRow.clientInfo = "client-info";
            logRow.entryId = 1;
            logRow.timestampCreated = 1;
            logRow.timestampLastUpdated = 1;
            ...
        }
    }
}
```

**Root cause:**

`ACL LOG` is the primary tool for auditing ACL denials in Redis. The current implementation returns `count` copies of the same hardcoded `LogRow` with placeholder strings `"reason"`, `"context"`, `"object"`, `"username"`, etc. There are `// todo` comments indicating the feature was never implemented.

`ACL LOG RESET` also has a `// todo` comment and does nothing.

Additionally, the error message for count validation says "count must be between 1 and 1000" but the actual check is `count > 100`.

**Impact:**

- Operators cannot detect or investigate ACL violations through `ACL LOG`. All responses are fake data.
- Attackers performing brute-force AUTH attempts or ACL probing will not generate any audit trail.
- The fake data could mislead operators into believing ACL logging is working.
- `ACL LOG RESET` silently succeeds without clearing anything, giving false confidence.

**Suggested fix:**

Implement a bounded circular buffer of `LogRow` entries. Record an entry each time `isAclCheckOk()` returns a failure result or when AUTH fails. Return actual entries from the buffer in `ACL LOG <count>` and clear the buffer in `ACL LOG RESET`. Also fix the count validation message to say "100" instead of "1000".

---

## Finding 5: `INIT_DEFAULT_U` remains a shared mutable singleton inside every `Inner` until `replaceUsers()`

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/AclUsers.java:202-205`
- `src/main/java/io/velo/acl/U.java:264-272`

**Code excerpt:**

```java
// AclUsers.java:202-205
Inner(long expectThreadId) {
    this.expectThreadId = expectThreadId;
    users.add(U.INIT_DEFAULT_U); // shared mutable singleton reference!
}
```

```java
// U.java:264-272
public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

static {
    INIT_DEFAULT_U.setOn(true);
    INIT_DEFAULT_U.addPassword(Password.NO_PASSWORD);
    INIT_DEFAULT_U.addRCmd(true, RCmd.fromLiteral("+*"), RCmd.fromLiteral("+@all"));
    INIT_DEFAULT_U.addRKey(true, RKey.fromLiteral("~*"));
    INIT_DEFAULT_U.addRPubSub(true, RPubSub.fromLiteral("&*"));
}
```

**Root cause:**

This was noted as a follow-up in Round 1 Bug 2 but was never addressed. Every `Inner` instance is seeded with a direct reference to the mutable static `U.INIT_DEFAULT_U`. Before `replaceUsers()` is called (i.e., before `loadAclFile()` succeeds), all inners share the same `U` object for the default user.

`ConfForGlobal.checkIfValid()` modifies this object during startup:

```java
// ConfForGlobal.java:204-207
if (ConfForGlobal.PASSWORD != null) {
    var aclUsers = AclUsers.getInstance();
    aclUsers.upInsert(U.DEFAULT_USER, u -> u.setPassword(U.Password.plain(ConfForGlobal.PASSWORD)));
}
```

This mutation affects `U.INIT_DEFAULT_U` directly, changing it from `nopass` to the configured password. After `replaceUsers()`, each inner gets its own copy, and the singleton is no longer in the list. But if `loadAclFile()` is never called (no ACL file exists), the shared singleton remains.

**Impact:**

- If two code paths modify the default user through different inners before `replaceUsers()`, they are modifying the same object, which could cause race conditions or unexpected state.
- If `U.INIT_DEFAULT_U` is referenced elsewhere after startup (e.g., in `getAuthU()` fallback), it reflects the mutated state rather than the original "nopass, all-permissions" template.
- The `static final` contract suggests immutability, but the object is mutated during startup, which is confusing for maintainers.

**Suggested fix:**

Create a defensive copy of the default user in each `Inner` constructor:

```java
Inner(long expectThreadId) {
    this.expectThreadId = expectThreadId;
    var defaultCopy = new U(U.DEFAULT_USER);
    defaultCopy.copyStateFrom(U.INIT_DEFAULT_U);
    users.add(defaultCopy);
}
```

Alternatively, make `U.INIT_DEFAULT_U` truly immutable by making its lists unmodifiable and removing setter methods from the static initializer path.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - Deleted user connections bypass all ACL checks | High | **New finding** | High |
| 2 - `reset` rule does not reset command permissions | High | **New finding** | High |
| 3 - Non-constant-time password comparison | Medium | **New finding** | Medium-High |
| 4 - `ACL LOG` returns fake data, preventing audit | Medium | **New finding** | High |
| 5 - `INIT_DEFAULT_U` shared mutable singleton in each Inner | Medium | **Known follow-up from Round 1** | High |

## Suggested Fix Order

1. Fix Finding 1 first. `isAclCheckOk()` returning `TRUE` for `u == null` is a direct authorization bypass that can be triggered by deleting any non-default user.
2. Fix Finding 2 next. `reset` not clearing commands violates the security contract and can retain elevated privileges.
3. Fix Finding 5 to remove the mutable singleton coupling, reducing the risk of cross-thread state corruption.
4. Fix Finding 4 to enable ACL audit logging for operational security monitoring.
5. Fix Finding 3 to harden password verification against timing attacks.

---

## AI Agent 2 Verification Notes

### Bug Confirmation Status

| Finding | Status | Verification Details |
|---------|--------|----------------------|
| 1 - Deleted user connections bypass ACL | **FIXED** | `Request.java:128` now returns `FALSE_WHEN_CHECK_CMD` when `u == null` instead of `TRUE` |
| 2 - `reset` does not clear command permissions | **FIXED** | `AGroup.java:376` now calls `resetCmd()` to clear command permissions |
| 3 - Non-constant-time password comparison | **FIXED** | `U.java:103` now uses `MessageDigest.isEqual()` for constant-time comparison |
| 4 - `ACL LOG` returns fake data | **FIXED** | Implemented bounded circular buffer in `AclUsers`; `ACL LOG` now returns real entries; `ACL LOG RESET` now clears entries |
| 5 - `INIT_DEFAULT_U` shared mutable singleton | **CONFIRMED** | `AclUsers.java:204` adds direct reference to mutable `U.INIT_DEFAULT_U` in each Inner |

### Bug 1 Fix Summary
- **File changed:** `src/main/java/io/velo/decode/Request.java`
- **Line changed:** 128
- **Change:** `isAclCheckOk()` now returns `FALSE_WHEN_CHECK_CMD` when `u == null` instead of `TRUE`
- **Test:** `io.velo.decode.RequestTest.test all` updated to verify correct behavior
- **JaCoCo:** Line 128 fully covered

### Bug 2 Fix Summary
- **File changed:** `src/main/java/io/velo/command/AGroup.java`
- **Line changed:** 376
- **Change:** Added `temp.resetCmd()` call in the `reset` rule handler
- **Test:** `io.velo.command.AGroupTest.test acl setuser reset clears command permissions` added
- **JaCoCo:** Line 376 fully covered

### Bug 3 Fix Summary
- **File changed:** `src/main/java/io/velo/acl/U.java`
- **Lines changed:** 88-107
- **Change:** Replaced `String.equals()` with `MessageDigest.isEqual()` for constant-time password comparison
- **Test:** Existing `io.velo.acl.UTest` tests pass (behavior unchanged, only implementation hardened)
- **JaCoCo:** Line 103 fully covered

### Bug 4 Fix Summary
- **Files changed:**
  - `src/main/java/io/velo/acl/AclUsers.java` - Added `recordAclLog()`, `getAclLogs()`, `resetAclLogs()` methods and bounded circular buffer
  - `src/main/java/io/velo/command/AGroup.java` - Updated `ACL LOG` to return real entries; fixed error message "100" not "1000"
  - `src/main/java/io/velo/MultiWorkerServer.java` - Record ACL failures when ACL check fails
- **Lines changed:** AclUsers.java:366-417, AGroup.java:263,278, MultiWorkerServer.java:470-475
- **Test:** `io.velo.command.AGroupTest.test acl` updated; `ACL LOG` now returns real entries or empty array
- **JaCoCo:** Lines 263, 278 in AGroup.java fully covered; lines 387-409 in AclUsers.java (getAclLogs, resetAclLogs) fully covered

### Review Summary
All 5 bugs verified. Bugs 1-4 are now fixed. Remaining bug 5 is pending.

---

## Review Feedback - Bug 1 Fix

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `17dab2f70b2d5b486cf8b71f9fbf87b717a3c5f0` (`fix: deny commands when authenticated user is deleted from ACL registry`)

### Findings

No blocking issues found.

### Summary of Fix

The commit addresses the deleted-user ACL bypass with three coordinated changes:

- `Request.java:127` — `isAclCheckOk()` returns `FALSE_WHEN_CHECK_CMD` instead of `TRUE` when `u == null`.
- `BaseCommand.java:189,198` — `@NotNull` changed to `@Nullable` on both `getAuthU()` methods, with javadoc documenting when null is returned.
- `AGroup.java:422`, `SGroup.java:1797`, `PGroup.java:430` — All three callers that dereference `getAuthU()` now include null guards.

### Strengths

- The core vulnerability is fixed: deleted-user connections can no longer bypass all ACL checks.
- The annotation and javadoc accurately document the null contract, preventing static analysis false negatives.
- All three dereference sites (ACL WHOAMI, PUBLISH, SSUBSCRIBE/SPUBLISH) are handled.
- `RequestTest` updated to expect deny behavior.

### Concerns

- Non-blocking: the null guard in `AGroup.java` returns `ErrorReply.NO_AUTH` while `SGroup` and `PGroup` return `ErrorReply.ACL_PERMIT_LIMIT`. The error message differs by code path. This is not a security issue but may confuse operators diagnosing the same root cause (deleted user) across different commands.

### Verification

- Ran focused tests: `./gradlew :cleanTest :test --tests "io.velo.decode.RequestTest" --tests "io.velo.command.AGroupTest" --tests "io.velo.command.SGroupTest" --tests "io.velo.command.PGroupTest"` — passed.

### Follow-ups

- Pre-commit: none required for Bug 1.
- Post-commit: optionally unify the error reply for null-user across all callers.

---

## Review Feedback - Bug 2 Fix

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `758e626d770d32268454b0e12a8c0c920c51aaeb` (`fix: ACL SETUSER reset clears command permissions`)

### Findings

No blocking issues found.

### Summary of Fix

Single-line production fix: `temp.resetCmd()` added to the `reset` rule handler in `AGroup.java:376`. A new regression test creates a user with `+@all`, runs `reset on`, and verifies `ACL DRYRUN` now denies the previously-allowed command.

### Strengths

- Minimal change in the exact right place.
- Test is well-structured: proves `+@all` works before reset, then proves it's denied after.
- Uses `ACL DRYRUN` to verify ACL state without side effects.
- Proper cleanup in test.

### Concerns

- Non-blocking: there is still no standalone `resetcommands`/`resetcmds` rule handler. Operators cannot clear command rules without also resetting passwords/keys/channels. This is a minor Redis ACL compatibility gap.

### Verification

- Ran focused test: `./gradlew :cleanTest :test --tests "io.velo.command.AGroupTest.test acl setuser reset clears command permissions"` — passed.

### Follow-ups

- Pre-commit: none required for Bug 2.
- Post-commit: optionally add `resetcmds`/`resetcommands` rule handler for Redis ACL compatibility.

---

## Review Feedback - Bug 3 Fix

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `eb833243f639a35859f25ed718c877d17af3b5c7` (`fix: use constant-time comparison in Password.check()`)

### Findings

No blocking issues found.

### Summary of Fix

Replaces `String.equals()` with `MessageDigest.isEqual()` in `Password.check()`. Both plain and SHA-256 password paths now use constant-time byte array comparison.

### Strengths

- Eliminates the timing side channel for password verification.
- Plain and SHA-256 paths unified under the same comparison method.

### Concerns

- Non-blocking: the `isNoPass()` early return at the top of `check()` still returns immediately, leaking timing information about whether a user has `nopass` enabled. This is low risk since `nopass` is a known configuration state, but a truly constant-time implementation would run the comparison regardless.

### Verification

- Existing `UTest` password tests continue to pass with the new comparison method.

### Follow-ups

- Pre-commit: none required for Bug 3.
- Post-commit: none required.

---

## Review Feedback - Bug 4 Fix

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `6961a8e37af1c4407103a07f70ff9cadb1abf1d9` (`fix: implement ACL LOG with bounded circular buffer for real audit logging`)

### Findings

1. **High — `recordAclLog()` uses `synchronized` on a static method, adding contention across all slot worker threads**

   Every ACL denial now contends on the `AclUsers.class` monitor. With high throughput and many ACL denials, this becomes a synchronization bottleneck across all slot worker threads.

   - `src/main/java/io/velo/acl/AclUsers.java:374`

   Consider using a lock-free ring buffer (e.g., `AtomicReferenceArray` with `AtomicInteger` indices) or per-thread buffers merged on read.

2. **Medium — `LogRow.ageSeconds` is always `0.0` and never updated**

   `recordAclLog()` sets `entry.ageSeconds = 0.0` and never recomputes it on read. Redis ACL LOG returns the age in seconds since the event occurred. The field should be computed from `timestampCreated` at read time.

   - `src/main/java/io/velo/acl/AclUsers.java:384`

3. **Medium — `LogRow.count` is overwritten to `size` on read, losing the original value**

   `getAclLogs()` sets `result[i].count = size` for every entry. This mutates shared `LogRow` objects in the circular buffer, replacing the entry's serial count with the requested display count. Redis uses `count` as the number of times this specific log entry pattern was hit, not the display size.

   - `src/main/java/io/velo/acl/AclUsers.java:405`

### Summary of Fix

Replaces hardcoded fake `ACL LOG` data with a real bounded circular buffer (128 entries). ACL denials in `MultiWorkerServer` are recorded via `AclUsers.recordAclLog()`. `ACL LOG <count>` returns real entries, `ACL LOG RESET` clears the buffer. The count validation error message is fixed from "1000" to "100". Five new tests cover record/retrieve, count limit, reset, circular overflow, and empty buffer.

### Strengths

- The circular buffer approach is correct and bounded (128 entries).
- Recording happens at the right place — the main ACL denial path in `MultiWorkerServer`.
- `synchronized` methods are correct for thread safety.
- Tests cover record, retrieve, count limit, reset, circular overflow, and empty buffer.
- Count error message fixed from "1000" to "100".

### Concerns

- The `synchronized` contention on all slot worker threads may become a performance issue under high ACL denial rates. This is not blocking for correctness but should be tracked as a follow-up.
- `ageSeconds` always being `0.0` means operators cannot tell how old an ACL denial event is.
- Mutating `LogRow.count` in the buffer on every read corrupts the stored entries.

### Verification

- Ran focused tests: `./gradlew :cleanTest :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.command.AGroupTest"` — passed.

### Follow-ups

- Pre-commit: fix `ageSeconds` to compute from `timestampCreated` at read time. Fix `count` overwrite — do not mutate buffer entries on read.
- Pre-commit: consider replacing `synchronized` with a lock-free ring buffer to avoid contention across slot worker threads.
- Post-commit: consider recording ACL LOG entries for AUTH failures and HTTP auth failures as well.

---

## Bug 4 Fix Feedback Response

### Issues Addressed

1. **Fixed - `ageSeconds` always 0.0**: `getAclLogs()` now computes `ageSeconds` from `timestampCreated` at read time using `(now - entry.timestampCreated) / 1000.0`.

2. **Fixed - `count` overwritten on read**: `getAclLogs()` now creates a defensive copy of each `LogRow` before returning, preserving the original buffer entries.

3. **Not addressed - `synchronized` contention**: The `synchronized` on static methods remains as a follow-up optimization. Under high ACL denial rates, this could become a bottleneck. A lock-free ring buffer using `AtomicReferenceArray` would be the proper fix.

### Files Changed

- `src/main/java/io/velo/acl/AclUsers.java` - `getAclLogs()` now returns copies and computes `ageSeconds` correctly

### Verification

- JaCoCo shows full coverage of all lines in `getAclLogs()` including the new copy logic (lines 399-410)
