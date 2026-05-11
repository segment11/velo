# Bug 27 - ACL Security Module Review (Round 1)

Reviewer: AI agent 1
Review date: 2026-05-11
Branch: `main`
Scope: ACL security module — authentication, authorization, user management, rule enforcement, and replication of ACL changes.

## Files Reviewed

- `doc/design/10_acl_security_design.md`
- `src/main/java/io/velo/acl/U.java`
- `src/main/java/io/velo/acl/AclUsers.java`
- `src/main/java/io/velo/acl/RCmd.java`
- `src/main/java/io/velo/acl/RKey.java`
- `src/main/java/io/velo/acl/RPubSub.java`
- `src/main/java/io/velo/acl/Category.java`
- `src/main/java/io/velo/acl/LogRow.java`
- `src/main/java/io/velo/BaseCommand.java`
- `src/main/java/io/velo/RequestHandler.java`
- `src/main/java/io/velo/MultiWorkerServer.java`
- `src/main/java/io/velo/decode/Request.java`
- `src/main/java/io/velo/command/AGroup.java`
- `src/main/java/io/velo/command/HGroup.java`
- `src/main/java/io/velo/command/PGroup.java`
- `src/main/java/io/velo/command/SGroup.java`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java`
- `src/main/java/io/velo/ValkeyRawConfSupport.java`
- `src/main/java/io/velo/ConfForGlobal.java`
- `src/test/groovy/io/velo/acl/UTest.groovy`
- `src/test/groovy/io/velo/acl/AclUsersTest.groovy`

---

## Finding 1: `acl deluser` allows deleting the `default` user, breaking authentication

**Severity:** High

**Files:**

- `src/main/java/io/velo/command/AGroup.java:121-140`

**Code excerpt:**

```java
} else if ("deluser".equals(subCmd)) {
    if (data.length < 3) {
        return ErrorReply.SYNTAX;
    }

    List<String> userList = new ArrayList<>();
    for (int i = 2; i < data.length; i++) {
        var user = new String(data[i]);
        userList.add(user);
    }

    int count = 0;
    for (var user : userList) {
        if (aclUsers.delete(user)) {
            count++;
        }
    }
```

**Root cause:**

The `ACL DELUSER` command does not protect the `default` user from deletion. Redis ACL semantics forbid deleting the `default` user because it is always required to exist. While `loadAclFile()` enforces this at startup, `ACL DELUSER` at runtime has no such guard.

**Impact:**

- Deleting the `default` user removes the fallback user that unauthenticated connections resolve to via `BaseCommand.getAuthU()` (line 191: `authUser == null ? U.INIT_DEFAULT_U : aclUsers.get(authUser)`).
- Any existing connection that was authenticated as `default` will have `aclUsers.get("default")` return `null`, causing a `NullPointerException` in `Request.isAclCheckOk()` → `U.isOn()` or `U.checkCmdAndKey()`.
- The system cannot be operated without the `default` user, requiring a restart to recover.
- Replicating this operation to replicas via `appendAclUpdateBinlog()` propagates the broken state.

**Suggested fix:**

Skip `default` from the delete list, or reject the command entirely if `default` is in the target list:

```java
for (var user : userList) {
    if (user.equals(U.DEFAULT_USER)) {
        // Redis returns an error, Velo could skip or error
        continue;
    }
    if (aclUsers.delete(user)) {
        count++;
    }
}
```

---

## Finding 2: `INIT_DEFAULT_U` is a shared static singleton — mutation corrupts all per-thread copies

**Severity:** High

**Files:**

- `src/main/java/io/velo/acl/U.java:259-267`
- `src/main/java/io/velo/BaseCommand.java:189-192`
- `src/main/java/io/velo/acl/AclUsers.java:287-296`

**Code excerpt:**

```java
public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

static {
    INIT_DEFAULT_U.setOn(true);
    INIT_DEFAULT_U.addPassword(Password.NO_PASSWORD);
    INIT_DEFAULT_U.addRCmd(true, RCmd.fromLiteral("+*"), RCmd.fromLiteral("+@all"));
    INIT_DEFAULT_U.addRKey(true, RKey.fromLiteral("~*"));
    INIT_DEFAULT_U.addRPubSub(true, RPubSub.fromLiteral("&*"));
}
```

```java
public static @NotNull U getAuthU(ITcpSocket socket0) {
    var authUser = SocketInspector.getAuthUser(socket0);
    return authUser == null ? U.INIT_DEFAULT_U : aclUsers.get(authUser);
}
```

**Root cause:**

`U.INIT_DEFAULT_U` is a mutable singleton. `AclUsers.replaceUsers()` (line 287-296) is called during `loadAclFile()` and creates new `U` instances, but `BaseCommand.getAuthU()` returns `U.INIT_DEFAULT_U` directly (not a copy) whenever a connection is not authenticated. This is the correct design — unauthenticated connections should see the initial default user.

However, `AclUsers.Inner` constructor adds `U.INIT_DEFAULT_U` to the `users` list:

```java
Inner(long expectThreadId) {
    this.expectThreadId = expectThreadId;
    users.add(U.INIT_DEFAULT_U); // shared reference!
}
```

After `loadAclFile()` → `replaceUsers()` replaces the list, but any code path that calls `AclUsers.upInsert(U.DEFAULT_USER, ...)` (e.g. `ConfForGlobal.checkIfValid()` at line 206) operates on the `U` object found in the `users` list — which, after `replaceUsers()`, is a different `U` instance than `INIT_DEFAULT_U`. Meanwhile, `BaseCommand.getAuthU()` always returns `INIT_DEFAULT_U` for unauthenticated connections.

This creates a split-brain: the "default" user in the ACL registry can have its password changed by `ConfForGlobal.checkIfValid()`, but unauthenticated connections still see the original `INIT_DEFAULT_U` with `Password.NO_PASSWORD`. This means:

1. Setting `PASSWORD` in `velo.properties` correctly updates the registry user, but `getAuthU()` for unauthenticated sockets bypasses this entirely and returns the static immutable default.
2. An `ACL SETUSER` on `default` updates the registry user, but unauthenticated connections still get the unmodified `INIT_DEFAULT_U`.

**Impact:**

- The `PASSWORD` config property is effectively a no-op for unauthenticated connections — they always get `INIT_DEFAULT_U` with `nopass` regardless of the configured password.
- The security intent of `PASSWORD` in `velo.properties` is not enforced for connections that have not explicitly authenticated.
- `MultiWorkerServer.java:461-464` does check `getAuthUser(socket) == null && ConfForGlobal.PASSWORD != null` and blocks non-AUTH commands, so the practical impact is that unauthenticated connections are blocked from running commands but can still AUTH with any password against the `INIT_DEFAULT_U` user (since `nopass` accepts any password).

**Suggested fix:**

Either make `U.INIT_DEFAULT_U` immutable (return defensive copies), or resolve the default user through the `AclUsers` registry consistently instead of using the static singleton.

---

## Finding 3: `U.fromLiteral()` stores plain passwords unhashed in the ACL file and accepts them on reload, violating Redis 6+ ACL semantics

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/U.java:382-422`
- `src/main/java/io/velo/acl/U.java:274-305`

**Code excerpt:**

```java
var password = parts[3];
var u = new U(user);
u.setOn(isOn);
if (Password.NO_PASS.equals(password)) {
    u.addPassword(Password.NO_PASSWORD);
} else if (password.startsWith(ADD_HASH_PASSWORD_PREFIX)) {
    u.addPassword(Password.sha256HexEncoded(password.substring(1)));
} else {
    u.addPassword(Password.plain(password));
}
```

```java
// In literal()
if (firstPassword.encodeType == PasswordEncodedType.sha256Hex) {
    sb.append(ADD_HASH_PASSWORD_PREFIX).append(firstPassword.passwordEncoded).append(" ");
} else {
    sb.append(firstPassword.passwordEncoded).append(" ");
}
```

**Root cause:**

1. When `U.literal()` serializes a plain-text password, it emits the raw password string without any prefix marker (no `>` prefix). On reload via `U.fromLiteral()`, this is parsed back as `Password.plain(password)`.
2. `ACL SETUSER >mypass` correctly adds a plain password, but `literal()` serializes it as just `mypass` (without `>`). On the next `ACL SAVE` + `ACL LOAD` cycle, this will still parse correctly as plain because `fromLiteral` treats the `else` branch as plain.
3. However, `U.literal()` only emits the **first** password. If a user has multiple passwords, all others are silently dropped on save/reload. In Redis 6+, `ACL SAVE` writes all passwords (with `>` prefix for plain, `#` for SHA256).

**Impact:**

- Only the first password is persisted. Any additional passwords added via `ACL SETUSER >pass1 >pass2` are lost after save/reload.
- Plain passwords are stored in cleartext in the ACL file instead of being hashed (Redis 6+ uses SHA256 by default for `ACL SAVE`).
- The `literal()` method does not include the `>` prefix for plain passwords, creating ambiguity when a password string happens to match a known prefix (`#`, `<`, `!`, `~`, `&`, `+`, `-`).

**Suggested fix:**

1. Emit the `>` prefix for plain passwords in `literal()`.
2. Iterate and emit all passwords, not just the first.
3. Consider hashing plain passwords to SHA256 on save (as Redis does).

---

## Finding 4: `U.checkChannels()` uses AND logic across PubSub rules — a single restrictive pattern blocks all access

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/U.java:613-626`

**Code excerpt:**

```java
public boolean checkChannels(String... channels) {
    if (rPubSubList.isEmpty()) {
        return false;
    }

    for (var rPubSub : rPubSubList) {
        for (var channel : channels) {
            if (!rPubSub.match(channel)) {
                return false;
            }
        }
    }
    return true;
}
```

**Root cause:**

The method returns `false` if **any** rule in `rPubSubList` does **not** match **any** channel. This is an AND-of-ANDs check: every rule must match every channel.

The correct ACL semantics (as in Redis) is OR across rules: the access is granted if **any** rule matches the channel. For example, a user with `&foo* &bar*` should be able to publish to `foo123` (matched by first rule) even though `bar*` doesn't match it. The current code would deny access because `&bar*` doesn't match `foo123`.

The same pattern bug likely applies to `checkCmdAndKey()` for key checking. Looking at lines 590-601:

```java
for (var slotWithKeyHash : slotWithKeyHashList) {
    for (var rKey : rKeyList) {
        if (!rKey.match(slotWithKeyHash.rawKey(), true, true)) {
            return FALSE_WHEN_CHECK_KEY;
        }
    }
}
```

This has the same AND-across-rules bug: every key pattern must match every key. If a user has `~foo* ~bar*`, they should be able to access `foo123` because `~foo*` matches. The current code rejects it because `~bar*` doesn't match `foo123`.

**Impact:**

- Users with multiple PubSub patterns or key patterns can never access any channel/key unless all patterns are wildcards (`*`).
- Adding a second, more restrictive pattern to a user (e.g., `ACL SETUSER myuser resetchannels &myChannel* &otherChannel*`) effectively locks them out of all channels except those matching **both** patterns simultaneously (which is impossible for distinct patterns).
- Same for key patterns: `ACL SETUSER myuser resetkeys ~foo* ~bar*` locks the user out of any key.

**Suggested fix:**

For `checkChannels()`, use OR logic across rules — grant access if **any** rule matches:

```java
public boolean checkChannels(String... channels) {
    if (rPubSubList.isEmpty()) {
        return false;
    }
    for (var channel : channels) {
        boolean matched = false;
        for (var rPubSub : rPubSubList) {
            if (rPubSub.match(channel)) {
                matched = true;
                break;
            }
        }
        if (!matched) {
            return false;
        }
    }
    return true;
}
```

Apply the same fix for key pattern matching in `checkCmdAndKey()`.

---

## Finding 5: `AclUsers.upInsert()` double-applies the callback on the current thread's inner instance

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/AclUsers.java:259-264`

**Code excerpt:**

```java
public void upInsert(String user, UpdateCallback<U> callback) {
    var inner = getInner();
    inner.upInsert(user, callback);

    changeUser(inner2 -> inner2.upInsert(user, callback));
}
```

```java
private void changeUser(DoInTargetEventloop doInTargetEventloop) {
    var currentThreadId = Thread.currentThread().threadId();
    for (int i = 0; i < inners.length; i++) {
        var inner = inners[i];
        if (inner.expectThreadId == currentThreadId) {
            doInTargetEventloop.doSth(inner);
        } else {
            var targetEventloop = slotWorkerEventloopArray[i];
            targetEventloop.execute(() -> {
                doInTargetEventloop.doSth(inner);
            });
        }
    }
}
```

**Root cause:**

`upInsert()` first calls `inner.upInsert(user, callback)` on the current thread's inner, then calls `changeUser()` which iterates **all** inners — including the current thread's inner again (because `inner.expectThreadId == currentThreadId` matches). So the callback is invoked twice on the current thread's inner instance.

The same bug exists in `delete()` (line 272-279) and `replaceUsers()` (line 287-296).

**Impact:**

- For `upInsert` with `addPassword`, the duplicate is harmless due to the duplicate check in `U.addPassword()`.
- For `upInsert` with `setPassword` (called from `ConfForGlobal.checkIfValid()`), the password is cleared and set twice, which is functionally equivalent but wasteful.
- For `replaceUsers()`, the users list is cleared and re-populated twice — functionally correct but wasteful.
- For any future callback that has side effects (logging, metrics, external notifications), the double-invocation would be a real bug.
- The double-execution is not a correctness issue for current code paths, but it is an invariant violation that will cause subtle bugs when new callback logic is added.

**Suggested fix:**

Remove the direct call before `changeUser()`, or exclude the current thread's inner from `changeUser()`:

```java
public void upInsert(String user, UpdateCallback<U> callback) {
    changeUser(inner -> inner.upInsert(user, callback));
}
```

Or keep the direct call and skip the current thread in `changeUser()`.

---

## Finding 6: `AclUsers.getInner()` falls back to `inners[0]` when called from a non-worker thread, returning stale or wrong user data

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/AclUsers.java:90-99`

**Code excerpt:**

```java
public Inner getInner() {
    var currentThreadId = Thread.currentThread().threadId();
    for (var inner : inners) {
        if (inner.expectThreadId == currentThreadId) {
            return inner;
        }
    }
    // when run in networker thread, return the first inner instance, only for read
    return inners[0];
}
```

**Root cause:**

When `getInner()` is called from a thread that is not a slot worker (e.g., a network worker thread or a test thread), it silently falls back to `inners[0]`. The comment says "only for read", but there is no enforcement:

1. `upInsert()` (line 259-264) calls `getInner()` and then mutates the returned inner — which could be `inners[0]` from a non-worker thread, causing concurrent modification without synchronization.
2. `replaceUsers()` (line 287-296) does the same — clears and re-populates `inners[0]` from an arbitrary thread.
3. In `AclUsersTest`, after `initForTest()` (which sets `inners[0]` to the test thread), calling `loadAclFile()` → `replaceUsers()` works because `getInner()` returns the test thread's inner. But after `initBySlotWorkerEventloopArray()`, calling `getInner()` from the test thread falls back to `inners[0]`, which belongs to a slot worker eventloop thread. Mutating it from the test thread races with the slot worker.

**Impact:**

- `ArrayList` (`Inner.users`) is not thread-safe. Concurrent reads and writes from the worker thread and the calling thread can cause `ConcurrentModificationException`, lost updates, or corrupted internal state.
- The fallback was intended for read-only access from network workers, but mutation methods use it for writes too.

**Suggested fix:**

Either enforce read-only access on the fallback path (throw on write), or ensure mutation methods always run on the correct eventloop thread. For `upInsert()`, `delete()`, and `replaceUsers()`, the direct call on `getInner()` should be replaced by routing through `changeUser()` exclusively.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - `ACL DELUSER` allows deleting the `default` user | High | **Confirmed** | High |
| 2 - `INIT_DEFAULT_U` singleton mutation vs registry split-brain | High | **Confirmed** | High |
| 3 - `U.literal()` drops all passwords except the first; stores plain passwords | Medium | **Confirmed** | High |
| 4 - `checkChannels()` and key pattern matching use AND logic instead of OR across rules | Medium | **Confirmed** | High |
| 5 - `AclUsers.upInsert()` / `delete()` / `replaceUsers()` double-apply on current thread | Medium | **Confirmed** | High |
| 6 - `getInner()` fallback to `inners[0]` is used for writes from non-worker threads | Medium | **Confirmed** | Medium |

## Suggested Fix Order

1. Fix Finding 4 first — the AND-vs-OR logic in `checkChannels()` and key checking is a security correctness issue that silently denies access to users with multiple patterns.
2. Fix Finding 1 — protecting `default` user from deletion prevents a trivial crash.
3. Fix Finding 2 — resolving the `INIT_DEFAULT_U` singleton split-brain ensures password configuration is enforced.
4. Fix Finding 5 — removing the double-application simplifies the code and prevents future bugs.
5. Fix Finding 6 — restricting `getInner()` fallback to read-only or removing the mutation path prevents thread-safety violations.
6. Fix Finding 3 — password serialization improvements can be done as a cleanup.

---

## AI Agent 2 Verification Notes

Reviewer: AI agent 2
Review date: 2026-05-11
Result: Confirmed, with impact refinements noted below.

### Verification Summary

| Finding | AI agent 2 status | Confidence | Notes |
|---------|-------------------|------------|-------|
| 1 - `ACL DELUSER` allows deleting the `default` user | **Confirmed** | High | `AGroup.acl()` deletes every requested username without excluding `default` (`AGroup.java:121-140`). Impact should be refined: sockets explicitly authenticated as `default` can resolve to `null`; `Request.isAclCheckOk()` currently treats `u == null` as allowed (`Request.java:126-130`), so this can become an ACL bypass for later commands, while `acl whoami` can still NPE via `getAuthU().getUser()` (`AGroup.java:391-397`). |
| 2 - `INIT_DEFAULT_U` singleton vs ACL registry split-brain | **Confirmed** | High | `BaseCommand.getAuthU()` returns `U.INIT_DEFAULT_U` for unauthenticated sockets (`BaseCommand.java:189-192`), while ACL file loads and `ACL SETUSER default ...` update the registry users held by `AclUsers`. `AclUsers.Inner` also seeds every inner with the same mutable singleton (`AclUsers.java:153-156`). Refine one impact claim: RESP `AUTH` does not authenticate against `INIT_DEFAULT_U`; it uses `aclUsers.get(user)` (`RequestHandler.java:410-427`). The confirmed risk is that unauthenticated default-user ACL checks bypass runtime/loaded default-user rules by reading the static singleton. |
| 3 - `U.literal()` persists only one password and emits plain passwords ambiguously | **Confirmed** | High | `literal()` uses only `getFirstPassword()` and never iterates the remaining password list (`U.java:274-285`). Plain passwords are emitted without the `>` marker (`U.java:282-283`), while `fromLiteral()` treats only a leading `#` specially and otherwise creates a plain password (`U.java:394-404`). This confirms loss of secondary passwords and ambiguous plaintext serialization. |
| 4 - Key and Pub/Sub pattern checks use AND across allow rules | **Confirmed** | High | Key checks return false when any configured key rule does not match a key (`U.java:590-600`), and channel checks return false when any channel rule does not match a channel (`U.java:613-625`). Redis-style ACL allow rules should be OR per key/channel, with all requested keys/channels needing at least one matching allow rule. |
| 5 - ACL mutations double-apply on the current inner | **Confirmed** | High | `upInsert()`, `delete()`, and `replaceUsers()` mutate `getInner()` first, then call `changeUser()`, whose loop also executes immediately for the current thread's matching inner (`AclUsers.java:241-249`, `AclUsers.java:259-296`). Current behavior is often idempotent, but the double callback/execution is real. |
| 6 - `getInner()` fallback is read-intended but used by write paths | **Confirmed** | Medium-High | `getInner()` falls back to `inners[0]` for non-matching threads with a "only for read" comment (`AclUsers.java:90-98`), but write methods call `getInner()` and mutate that returned inner directly (`AclUsers.java:259-296`). This can mutate a worker-owned `Inner.users` list from the wrong thread before the eventloop-routed update runs. |

### Additional Reviewer Notes

- Finding 1 is more severe than the original text states in one path: after deleting `default`, an authenticated socket whose stored auth user is `"default"` receives `null` from `aclUsers.get("default")`; `Request.isAclCheckOk()` then returns success for `u == null`. That is an authorization bypass, not only a crash.
- Finding 2 is valid, but the original claim that unauthenticated clients can `AUTH` with any password "against `INIT_DEFAULT_U`" is not supported by the current code. RESP `AUTH` and HTTP basic auth both look up users through `aclUsers.get(...)`. The static-default bypass applies to ACL checks before a socket has an auth user recorded.
- `ConfForGlobal.checkIfValid()` calls `AclUsers.upInsert()` when `PASSWORD` is set, while server startup calls `AclUsers.initBySlotWorkerEventloopArray()` later in `MultiWorkerServer` (`MultiWorkerServer.java:1297`, then `MultiWorkerServer.java:953-954`). This may be a related startup-order bug for password-enabled configurations and should be checked when fixing Finding 2.

---

## Review Feedback - Bug 1 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `305999d25d123fafed0abc71120f32a41072d8a4` (`fix: protect default user from deletion in ACL DELUSER`)

### Findings

No blocking issues found.

### Summary of Fix

The commit updates `ACL DELUSER` handling in `AGroup.acl()` so `U.DEFAULT_USER` is skipped before calling `aclUsers.delete(user)`. The regression test adds `acl deluser default a` and verifies that `default` remains present while the non-default user is deleted and counted.

### Strengths

- The production change is narrowly scoped to the dangerous delete path.
- The test exercises both branches of the new guard in one command: `default` is skipped and user `a` is deleted.
- The fix also protects replicated ACL update replay because replicas execute the same command handler and will skip `default` as well.

### Concerns

- Redis may choose to error on attempts to delete `default`, while this implementation silently skips it and returns the count for other users. This matches the accepted option in the original finding's suggested fix, so it is not a blocker.
- The broader `Request.isAclCheckOk()` behavior that allows `u == null` remains unresolved, but that is outside this specific Bug 1 fix once `default` can no longer be deleted through `ACL DELUSER`.

### Verification

- Ran focused test: `./gradlew :test --tests "io.velo.command.AGroupTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.command/AGroup.java.html` shows the new guard line `if (user.equals(U.DEFAULT_USER))` as fully covered with both branches covered, and the `continue` line covered.

### Follow-ups

- Pre-commit: none for this fix.
- Post-commit: keep Finding 2 and the `u == null` ACL bypass concern tracked separately; they are not fully addressed by Bug 1.

---

## Review Feedback - Bug 2 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `5ff7e0d42139b445a302cbef7f3e7ec86eedbb1e` (`fix: resolve getAuthU default user from ACL registry instead of static singleton`)

### Findings

1. **High - Configured `password` still cannot safely initialize the registry default user**

   `ConfForGlobal.checkIfValid()` still applies the configured `password` by calling `AclUsers.upInsert(U.DEFAULT_USER, ...)` (`ConfForGlobal.java:204-206`). During normal server startup, this validation is reached from the config provider before `onStart()` initializes ACL inners with `AclUsers.initBySlotWorkerEventloopArray(...)` (`MultiWorkerServer.java:1297`, initialization later at `MultiWorkerServer.java:952-954`). The new `AclUsers.getInner()` null guard only makes read paths return `null`; write paths still dereference that result immediately (`AclUsers.java:90-93`, `AclUsers.java:262-264`). So the password-config part of Bug 2 remains unfixed: a configured `password` still does not reliably become the registry `default` user password, and the startup path can still fail before ACL initialization.

### Summary of Fix

The commit changes `BaseCommand.getAuthU()` so unauthenticated sockets resolve the `default` user through `aclUsers.get(U.DEFAULT_USER)` and only fall back to `U.INIT_DEFAULT_U` when the ACL registry is not initialized or does not contain `default`. It also adds a null guard to `AclUsers.getInner()` and a regression test proving `getAuthU()` returns a replacement registry default user after `replaceUsers()`.

### Strengths

- The main split-brain read path is fixed for ACL-file-loaded or `replaceUsers()`-replaced default users.
- The added regression test directly exercises the important distinction: returned user is not `U.INIT_DEFAULT_U` and accepts the configured replacement password.
- JaCoCo confirms both the unauthenticated branch and the `defaultUser != null ? ... : ...` fallback branch in `BaseCommand.getAuthU()` are covered.

### Concerns

- This is a partial Bug 2 fix. Runtime ACL registry resolution is improved, but the configuration path called out in the original finding still needs a startup-order-safe way to apply `ConfForGlobal.PASSWORD` after ACL inners exist, or an ACL initialization path that can accept the configured default user before eventloops are assigned.
- `AclUsers.Inner` still seeds each inner with the mutable `U.INIT_DEFAULT_U` singleton. That is less dangerous after `getAuthU()` prefers the registry, but writes to the initial registry default can still mutate the static singleton until the registry has been replaced with distinct `U` instances.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.BaseCommandTest" --tests "io.velo.ConfForGlobalTest"` — passed.
- Inspected JaCoCo HTML reports:
  - `build/reports/jacocoHtml/io.velo/BaseCommand.java.html` shows the changed `authUser == null` branch and the `defaultUser != null` ternary as fully covered.
  - `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html` shows the new `inners == null` guard as fully branch-covered.

### Follow-ups

- Pre-commit: add a regression test for configured `password` before ACL initialization, or explicitly move password-to-default-user application after `AclUsers.initBySlotWorkerEventloopArray(...)`.
- Post-commit: decide whether to replace `U.INIT_DEFAULT_U` in `AclUsers.Inner` with a fresh copy to prevent future singleton mutation.

---

## Review Feedback - Bug 2 Fix Re-review

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `de7d0eb3f639a35859f25ed718c877d17af3b5c7` (`fix: allow ACL upInsert before init with pre-init staging inner`)

### Findings

1. **Medium - Production startup staging path is not covered by the regression test**

   The follow-up commit adds `preInitInner` staging and copies it into real worker inners in `initBySlotWorkerEventloopArray()` (`AclUsers.java:66-82`), which is the production startup path used by `MultiWorkerServer.onStart()` (`MultiWorkerServer.java:952-954`). The new test only verifies the `initForTest()` transfer path (`AclUsersTest.groovy:114-132`). JaCoCo confirms this gap: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html` marks `initBySlotWorkerEventloopArray()` lines 76-81 as not covered, while the test-only `initForTest()` transfer lines are covered. The implementation looks consistent, but the production branch for the bug's real startup scenario still needs direct test coverage before closing the workflow.

### Summary of Fix

The commit adds a pre-initialization `Inner` to `AclUsers`. Calls to `getInner()` before worker initialization now create and return `preInitInner`; `changeUser()` no-ops while `inners` is still null; later initialization copies staged users into the initialized inners and clears the staging inner. This addresses the prior NPE risk from `ConfForGlobal.checkIfValid()` calling `AclUsers.upInsert()` before `AclUsers.initBySlotWorkerEventloopArray()`.

### Strengths

- The direct null dereference in pre-init `upInsert()` is addressed.
- Staged default-user state is preserved across `initForTest()`, and the regression verifies the password remains usable after initialization.
- The code path is small and localized to ACL user state initialization.

### Concerns

- The production copy branch in `initBySlotWorkerEventloopArray()` needs a regression test with a real `Eventloop[]`, not only `initForTest()`.
- The staged `default` user is still the mutable `U.INIT_DEFAULT_U` object because `Inner` seeds its list with the singleton before `upInsert()` updates it. This was already a known follow-up, but the staging fix continues to rely on that shared object.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.ConfForGlobalTest" --tests "io.velo.BaseCommandTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`.
  - Covered: `getInner()` pre-init branch, `changeUser()` null-inners guard, and `initForTest()` staged-state transfer.
  - Not covered: `initBySlotWorkerEventloopArray()` staged-state transfer lines 76-81.

### Follow-ups

- Pre-commit: add a regression that calls `upInsert()` before `initBySlotWorkerEventloopArray(...)`, then verifies the initialized worker inner sees the staged default-user password.
- Post-commit: consider replacing `U.INIT_DEFAULT_U` with a copied default user inside each `Inner` to remove the remaining mutable-singleton coupling.

---

## Review Feedback - Bug 2 Fix Second Re-review

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `c487dc801b2d5b486cf8b71f9fbf87b717a3c5f0` (`fix: allow ACL upInsert before init with pre-init staging inner`)

### Findings

No blocking issues found.

### Summary of Fix

The updated commit keeps the pre-initialization `Inner` staging approach, adds `resetForTest()`, and adds a regression for `upInsert()` before `initBySlotWorkerEventloopArray(...)`. This directly covers the production initialization branch that was missing in the previous review.

### Strengths

- The prior review finding is addressed: the new test drives `AclUsers.initBySlotWorkerEventloopArray(...)` after a staged `upInsert()`.
- `resetForTest()` makes the singleton state explicit in the new tests, so they no longer depend on leftover test order.
- JaCoCo now confirms the production staged-state transfer lines in `initBySlotWorkerEventloopArray()` are covered.

### Concerns

- Minor test-strength note: the worker-eventloop assertion only checks that the default user exists. It would be stronger to also assert `checkPassword('staged-password')` on that worker thread. This is not blocking because the current implementation copies the staged user list into every initialized inner, and the current-thread assertion verifies the password.
- The broader mutable `U.INIT_DEFAULT_U` coupling remains a follow-up outside this specific Bug 2 startup-order fix.

### Verification

- Ran focused tests: `./gradlew :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.ConfForGlobalTest" --tests "io.velo.BaseCommandTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`.
  - `initBySlotWorkerEventloopArray()` staged transfer lines 76-81 are now covered.
  - `changeUser()` null-inners guard is branch-covered.
  - `initForTest()` staged transfer remains covered.

### Follow-ups

- Pre-commit: none required for this fix.
- Post-commit: consider strengthening the worker-eventloop assertion to check the staged password, and separately address the mutable `U.INIT_DEFAULT_U` coupling.

---

## Review Feedback - Bug 2 Fix Third Re-review

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `c487dc801b2d5b486cf8b71f9fbf87b717a3c5f0` (`fix: allow ACL upInsert before init with pre-init staging inner`)

### Findings

No new findings. `HEAD` is still `c487dc8`; there have been no code changes since the previous re-review.

### Verification

- Re-ran focused verification at current `HEAD`: `./gradlew :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.ConfForGlobalTest" --tests "io.velo.BaseCommandTest"` — build successful.
- Confirmed current JaCoCo HTML still shows the production staged transfer in `AclUsers.initBySlotWorkerEventloopArray()` as covered (`AclUsers.java.html` lines 76-81).
- Confirmed current `HEAD` with `git rev-parse HEAD` and checked recent history with `git log --oneline -2`; no newer follow-up commit exists after `c487dc8`.

### Residual Risk

- The only remaining note is still the prior non-blocking one: the worker-eventloop assertion could be stronger if it checked the staged password, not just user existence.

---

## Review Feedback - Bug 3 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `adf844520b97b9a5346a37ea554c3e7d84b3747f` (`fix: emit all passwords with proper prefixes in U.literal()`)

### Findings

1. **Medium - Plain passwords are still persisted in cleartext**

   The fix now emits plain passwords with the `>` prefix (`U.java:279-285`) and reloads `>`-prefixed passwords as plain passwords (`U.java:400-403`). This resolves the missing-prefix ambiguity and preserves multiple passwords, but it does not address the original finding's cleartext-storage concern or Redis 6+ ACL-save behavior. A user configured with `ACL SETUSER user >pass1` will still be written as `>pass1` in the ACL file instead of a `#<sha256>` hash. If this project intentionally accepts Velo-specific cleartext ACL files, this should be documented; otherwise Bug 3 is only partially fixed.

### Summary of Fix

The commit changes `U.literal()` to iterate every password and emit `nopass`, `#<sha256>`, or `>plain` tokens. `U.fromLiteral()` now parses multiple password tokens starting at argument index 3 and keeps backwards compatibility for an old unprefixed first plain password. The new regression verifies a round-trip with two plain passwords and one SHA-256 password.

### Strengths

- Multiple passwords are no longer dropped by `U.literal()`.
- Plain passwords now have an explicit `>` marker, removing the previous ambiguity for newly saved ACL files.
- The parser remains compatible with existing ACL files that have an unprefixed first plain password.

### Concerns

- The new round-trip test checks `lit.contains('#')` but does not assert the exact hash token for `pass3`. That is not blocking because `restored.checkPassword('pass3')` verifies the behavior.
- There is no ACL `save`/`load` integration test for multiple passwords. The `U.literal()` round-trip covers the core serializer/parser path, so this is a test-strength follow-up rather than a blocker.

### Verification

- Ran focused tests with a fresh test execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.AclUsersTest" --tests "io.velo.command.AGroupTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/U.java.html`.
  - `U.literal()` password loop and `nopass`/SHA/plain branches are covered.
  - `U.fromLiteral()` `>` plain, `#` SHA, `nopass`, and backwards-compatible unprefixed first-password branches are covered.

### Follow-ups

- Pre-commit: decide whether Bug 3 requires Redis-compatible hash-on-save. If yes, update `U.literal()` to emit SHA-256 hashes for plain passwords instead of `>plain`.
- Post-commit: add an ACL `SAVE`/`LOAD` integration test for multiple passwords.

---

## Review Feedback - Bug 3 Fix Re-review

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `c7c8fcf7e0f7c1e9ccb99ed37c82ef17c2943c2f` (`fix: emit all passwords with proper prefixes in U.literal()`)

### Findings

No blocking issues found.

### Summary of Fix

The follow-up keeps the multiple-password serialization fix and changes plain-password serialization to emit `#<sha256>` instead of `>plain`. `U.fromLiteral()` still accepts `>plain`, `#hash`, `nopass`, and the old unprefixed first plain password form.

### Strengths

- The previous cleartext persistence finding is addressed: `U.literal()` now hashes plain passwords before writing them (`U.java:279-285`).
- The regression checks that `>pass1` and `>pass2` are absent and that the SHA-256 hashes are present.
- Round-trip behavior still verifies all three passwords after parsing the literal.

### Concerns

- Non-blocking coverage gap: the `fromLiteral()` `>` plain-password compatibility branch exists (`U.java:402-403`) but JaCoCo reports one branch missed for that condition in the focused run. Add a small compatibility assertion for `U.fromLiteral('user legacy on >pass ~*')`.
- There is still no ACL `SAVE`/`LOAD` integration test for multiple passwords. The serializer/parser unit test covers the core behavior, so this remains a follow-up rather than a blocker.

### Verification

- Ran focused tests with a fresh test execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.acl.AclUsersTest" --tests "io.velo.command.AGroupTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/U.java.html`.
  - `U.literal()` password loop, `nopass`, existing SHA, and plain-to-SHA serialization branches are covered.
  - `U.fromLiteral()` SHA, `nopass`, and backwards-compatible unprefixed first-password branches are covered.

### Follow-ups

- Pre-commit: none required for this fix.
- Post-commit: add compatibility coverage for `>plain` parsing and an ACL `SAVE`/`LOAD` integration test for multiple passwords.

---

## Review Feedback - Bug 4 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `0559326c06e026831dc37a8fecc66a18cb0c413e` (`fix: use OR logic across ACL key and channel allow rules`)

### Findings

No blocking issues found.

### Summary of Fix

The commit changes ACL key and Pub/Sub channel checks from "every rule must match" to "each requested key/channel must match at least one allow rule." It keeps the requirement that all requested keys/channels are authorized, while allowing distinct patterns such as `foo*` and `bar*` to authorize distinct objects.

### Strengths

- `checkCmdAndKey()` now uses OR semantics across key rules per key and returns a key failure only when no rule matches (`U.java:592-607`).
- `checkChannels()` now uses OR semantics across channel rules per channel and returns false only when a channel has no matching rule (`U.java:621-637`).
- Tests cover single-pattern hits, second-pattern hits, misses, multi-channel success, and multi-channel partial failure.

### Concerns

- Non-blocking test-strength note: the key regression checks single-key `foo`, `bar`, and `baz` cases, but not a multi-key command/list where one key matches `foo*` and another matches `bar*`, or where one key is unauthorized. The implementation loops per key correctly, and JaCoCo covers the loop branches, so this is not a blocker.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest" --tests "io.velo.command.AGroupTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/U.java.html`.
  - Key rule loop, `rKey.match(...)` hit/miss branches, and `!matched` key-failure branch are covered.
  - Channel loop, `rPubSub.match(...)` hit/miss branches, and `!matched` false branch are covered.

### Follow-ups

- Pre-commit: none required for this fix.
- Post-commit: add a multi-key mixed-pattern regression for `checkCmdAndKey()`.

---

## Review Feedback - Bug 5 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `85de28d3aafffe2fb286b6068b7d8df6550e9c63` (`fix: skip current thread inner in changeUser to avoid double-apply`)

### Findings

No blocking issues found for the committed Bug 5 fix.

### Summary of Fix

The commit changes `AclUsers.changeUser()` so the inner owned by the current thread is skipped. The public write methods still apply the mutation directly to the current thread's inner first, then route the mutation to the remaining inners. That removes the double-application on the current-thread inner for `upInsert()`, `delete()`, and `replaceUsers()`.

### Strengths

- The fix is localized to the replication fan-out point, so all three write methods benefit without duplicating skip logic.
- The `upInsert()` regression uses a side-effecting callback counter and proves the current-thread callback is invoked once, not twice.
- The committed behavior remains compatible with existing eventloop fan-out for other inners.

### Concerns

- Non-blocking test-strength note: the `replaceUsers()` regression verifies final state but does not detect double execution directly because applying the same replacement twice is idempotent.
- A newer follow-up commit, `608769cd22f9c3fcdd935149de930cbb06e90067`, now contains the non-owner-thread write routing changes in `AclUsers.java` and `AclUsersTest.groovy`. Those changes are outside the reviewed Bug 5 commit and overlap with Finding 6 rather than Bug 5.

### Verification

- Ran focused test with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.AclUsersTest"` — passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`; the current-thread skip path and eventloop scheduling path in `changeUser()` are covered in the current worktree.

### Follow-ups

- Pre-commit: none required for this Bug 5 fix.
- Post-commit: review `608769cd22f9c3fcdd935149de930cbb06e90067` as part of Bug 6, because it changes the write path beyond the committed Bug 5 scope.

## Review Feedback - Bug 6 Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `608769cd22f9c3fcdd935149de930cbb06e90067` (`fix: use thread-owned inner for writes to avoid cross-thread mutation`)

### Findings

1. **Medium - `delete()` returns `false` for successful non-owner-thread deletes.**

   `AclUsers.delete()` now uses `getOwnedInner()` and computes the return value from only the current thread's owned inner:

   - `src/main/java/io/velo/acl/AclUsers.java:326-333`

   When called from a non-owner thread, `getOwnedInner()` returns `null`, `flag` becomes `false`, and the deletion is only scheduled through `changeUser()`. The scheduled deletion can still succeed on all slot-worker inners, but the public method returns `false`. `ACL DELUSER` uses that boolean to count deleted users:

   - `src/main/java/io/velo/command/AGroup.java:132-143`

   So if the non-owner write path that this bug fix supports is used for `ACL DELUSER`, the client can receive `0` even though the user is deleted shortly afterward. The new regression calls `aclUsers.delete('routed-user')` from a non-owner thread but does not assert the return value, so this behavior is currently untested.

2. **Medium - the non-owner `replaceUsers()` path is not covered.**

   Finding 6 explicitly called out `replaceUsers()` as one of the unsafe write methods. The commit updates `replaceUsers()` to use `getOwnedInner()`, but the new non-owner regression only verifies `upInsert()` and `delete()`. After the focused test run, JaCoCo still marks the scheduled `replaceUsers()` callback body as not covered:

   - `src/main/java/io/velo/acl/AclUsers.java:348-351`
   - `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`: lines 349-350 are `nc`

   This means the Bug 6 fix does not meet the project workflow requirement to confirm the changed path with JaCoCo for every affected write method.

### Summary of Fix

The commit adds `getOwnedInner()` to separate write ownership from the existing read fallback in `getInner()`. `upInsert()`, `delete()`, and `replaceUsers()` now mutate the current inner only when the current thread owns one, then use `changeUser()` to route mutations to other slot-worker eventloops.

### Strengths

- The core cross-thread mutation issue is addressed for direct writes: non-owner callers no longer mutate `inners[0]` through the read fallback.
- The new regression exercises a non-owner caller and verifies that `upInsert()` and `delete()` are applied on both eventloop-owned inners.
- JaCoCo confirms coverage for both owner and non-owner returns from `getOwnedInner()`, and for both branches of the `upInsert()` ownership guard.

### Concerns

- `delete()` now has asynchronous non-owner semantics but still exposes a synchronous boolean result; that result is wrong in the non-owner success case.
- The non-owner `replaceUsers()` path should have its own regression because `loadAclFile()` depends on `replaceUsers()` and Bug 6 specifically named it.

### Verification

- Ran focused test with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.AclUsersTest"` - passed.
- Ran caller-focused ACL command test: `./gradlew :test --tests "io.velo.command.AGroupTest"` - passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`.
  - Covered: `getOwnedInner()` owner and non-owner branches, `upInsert()` owner/non-owner guard, `delete()` scheduled callback.
  - Not covered: non-owner scheduled `replaceUsers()` callback body (`AclUsers.java:349-350`).

### Follow-ups

- Fix or explicitly define the `AclUsers.delete()` return contract for non-owner callers before considering Bug 6 complete.
- Add a non-owner-thread `replaceUsers()` regression and confirm JaCoCo covers the scheduled replacement body.

## Review Feedback - Bug 6 Follow-up Fix

Reviewer: AI agent 2
Review date: 2026-05-11
Reviewed commit: `0f0ba6897c2d7399343ace9fd86aba2d8aae9815` (`fix: correct delete return for non-owner threads and add replaceUsers non-owner regression`)

### Findings

No blocking issues found.

### Summary of Fix

The follow-up commit addresses the two concerns from the prior Bug 6 review:

- `AclUsers.delete()` now returns `true` for a non-owner-thread delete when the read fallback can see the user before scheduling the actual eventloop delete.
- `AclUsersTest` now asserts the non-owner `delete()` return value for existing and missing users, and adds a non-owner `replaceUsers()` regression.

### Strengths

- The non-owner `delete()` return behavior is now tested directly instead of inferred from eventual state.
- The new `replaceUsers()` test exercises replacement from a non-owner thread and verifies the new user appears on both eventloop-owned inners while the old user is removed.
- The production change stays narrow and does not reintroduce cross-thread mutation of `Inner.users`.

### Concerns

- No blocking concerns. The non-owner `delete()` boolean remains an existence check before the scheduled async delete completes, but that is consistent with the existing asynchronous fan-out model.

### Verification

- Ran focused tests with fresh execution: `./gradlew :cleanTest :test --tests "io.velo.acl.AclUsersTest" --tests "io.velo.command.AGroupTest"` - passed.
- Inspected JaCoCo HTML report: `build/reports/jacocoHtml/io.velo.acl/AclUsers.java.html`.
  - `AclUsers.java:329` owner/non-owner branch: all branches covered.
  - `AclUsers.java:333` non-owner delete existence check: all branches covered.
  - `AclUsers.java:355-356` scheduled `replaceUsers()` body: covered.

### Follow-ups

- None required for Bug 6.
