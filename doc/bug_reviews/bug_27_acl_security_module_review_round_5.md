# Bug 27 - ACL Security Module Review (Round 5)

Reviewer: AI agent 1
Review date: 2026-05-12
Branch: `review/acl-security`
Scope: ACL security module follow-up review after Round 4 fixes. Focus on serialization roundtrip correctness, binlog integrity, password handling edge cases, and `fromLiteral()` parser completeness.

## Files Reviewed

- `src/main/java/io/velo/acl/U.java`
- `src/main/java/io/velo/acl/AclUsers.java`
- `src/main/java/io/velo/acl/RKey.java`
- `src/main/java/io/velo/command/AGroup.java`
- `src/main/java/io/velo/BaseCommand.java`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java`

---

## Finding 1: `U.literal()` double-hashes plain passwords, breaking password removal after ACL SAVE/LOAD roundtrip

**Severity:** High

**Files:**

- `src/main/java/io/velo/acl/U.java:300-302`
- `src/main/java/io/velo/acl/U.java:140-149`
- `src/main/java/io/velo/acl/U.java:226-228`

**Code excerpt:**

```java
// U.java:300-302 — literal() serializes plain passwords as SHA-256
} else {
    sb.append(ADD_HASH_PASSWORD_PREFIX).append(DigestUtils.sha256Hex(pwd.passwordEncoded)).append(" ");
}
```

```java
// U.java:140-149 — Password.equals() checks both encoded value AND encode type
public boolean equals(Object obj) {
    // ...
    var password = (Password) obj;
    return passwordEncoded.equals(password.passwordEncoded) && encodeType == password.encodeType;
}
```

```java
// U.java:226-228 — removePassword uses equals() to find and remove
public void removePassword(Password password) {
    passwords.stream().filter(p -> p.equals(password)).findFirst().ifPresent(passwords::remove);
}
```

**Root cause:**

When `literal()` serializes a `Password.plain("mypass")`, it outputs `#<sha256("mypass")>` instead of `>mypass`. When `fromLiteral()` parses this back, it creates `Password.sha256HexEncoded(sha256("mypass"))` — a different type (`sha256Hex` vs `plain`).

`Password.equals()` compares BOTH `passwordEncoded` AND `encodeType`. So after a SAVE → LOAD roundtrip:

1. **Password removal breaks**: `ACL SETUSER user <mypass` creates `Password.plain("mypass")`. `removePassword()` calls `equals()` against `Password.sha256HexEncoded(sha256("mypass"))`. The encode types differ (`plain` vs `sha256Hex`), so `equals()` returns false. The password is NOT removed. The user retains access with the old password.

2. **Password deduplication breaks**: `ACL SETUSER user >mypass` after a roundtrip creates `Password.plain("mypass")`. `addPassword()` checks `equals()` against `Password.sha256HexEncoded(...)`. They don't match, so a duplicate entry for the same raw password is added.

Authentication still works (both `Password.check()` paths produce the same comparison bytes), but operational management is broken.

**Impact:**

- After ACL SAVE → LOAD, operators cannot remove passwords using `<password` syntax.
- Each `>password` / `<password` cycle adds a new password entry instead of toggling it.
- `ACL LIST` shows accumulated duplicate password hashes.
- Redis compatibility: Redis stores plain passwords as `#<hash>` (SHA256) in the ACL file — same as Velo's `literal()` output. The issue is Velo's internal type system: `Password.plain` becomes `Password.sha256Hex` after roundtrip, breaking `equals()`.

**Suggested fix:**

Use `ADD_PASSWORD_PREFIX` (`>`) for plain passwords in `literal()`:

```java
} else {
    sb.append(ADD_PASSWORD_PREFIX).append(pwd.passwordEncoded).append(" ");
}
```

This preserves the encode type through the roundtrip. Note: this changes the ACL file format for existing deployments — existing files with `#<sha256hex>` for originally-plain passwords will still be parsed correctly by `fromLiteral()` (as `sha256Hex`), but the next SAVE will write them as `><password>`. Migration is automatic on the next SAVE.

---

## Finding 2: `dataToLine()` + `execute()` split breaks ACL commands with space-containing arguments (binlog corruption)

**Severity:** High

**Files:**

- `src/main/java/io/velo/BaseCommand.java:170-179`
- `src/main/java/io/velo/BaseCommand.java:387`
- `src/main/java/io/velo/command/AGroup.java:443-463`
- `src/main/java/io/velo/repl/incremental/XAclUpdate.java:126-134`

**Code excerpt:**

```java
// BaseCommand.java:170-179 — joins with spaces
protected String dataToLine() {
    var sb = new StringBuilder();
    for (var i = 0; i < data.length; i++) {
        sb.append(new String(data[i]));
        if (i != data.length - 1) {
            sb.append(" ");
        }
    }
    return sb.toString();
}
```

```java
// BaseCommand.java:387 — splits on spaces
var dataStrings = allDataString.split(" ");
```

```java
// XAclUpdate.java:126-134 — replica replays via execute()
@Override
public void apply(short slot, ReplPair replPair) {
    var aGroup = new AGroup("acl", null, null);
    for (var line : lines) {
        var reply = aGroup.execute(line);
```

**Root cause:**

ACL SETUSER with password `"my secret"` (containing a space) is valid in RESP — the client sends it as a single bulk string. `dataToLine()` joins all parts with spaces, producing `"acl setuser alice >my secret"`. When the replica replays this via `execute()`, it splits on spaces and gets `["acl", "setuser", "alice", ">my", "secret"]` — two separate rules. The password is set to `"my"` (truncated), and `"secret"` is interpreted as a plain password at position 5.

**Impact:**

- Replicas receive incorrect ACL state for any SETUSER command with a space-containing password.
- The corruption is silent — no error is thrown, the password is simply wrong.
- All binlog-replicated ACL commands are affected: SETUSER, DELUSER, LOAD.

**Suggested fix:**

Use RESP-style encoding for binlog serialization (e.g., length-prefixed or quoted strings), or use a delimiter that cannot appear in RESP bulk strings. Alternatively, store the raw `byte[][]` in the binlog entry instead of a space-joined string.

A minimal fix: in `dataToLine()`, quote arguments that contain spaces:

```java
for (var i = 0; i < data.length; i++) {
    var s = new String(data[i]);
    if (s.contains(" ")) {
        sb.append('"').append(s.replace("\"", "\\\"")).append('"');
    } else {
        sb.append(s);
    }
    // ...
}
```

And update `execute()` to handle quoted strings.

---

## Finding 3: `U.fromLiteral()` does not recognize `resetkeys`/`resetchannels`/`resetpass`/`reset` — treats them as passwords at position 3

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/acl/U.java:416-441`
- `src/main/java/io/velo/command/AGroup.java:354-389`

**Code excerpt:**

```java
// U.java:436-441 — fallback at position 3 treats unknown tokens as plain passwords
} else if (i == 3) {
    u.addPassword(Password.plain(part));
} else {
    throw new IllegalArgumentException("Invalid literal: " + part);
}
```

```java
// AGroup.java:354-389 — SETUSER handler recognizes these keywords
} else if ("resetkeys".equals(rule)) {
    temp.resetKey();
} else if ("resetchannels".equals(rule)) {
    temp.resetPubSub();
} else if ("resetpass".equals(rule)) {
    temp.resetPassword();
} else if ("reset".equals(rule)) {
    // resets everything
```

**Root cause:**

`fromLiteral()` does not handle `resetkeys`, `resetchannels`, `resetpass`, or `reset`. These are valid Redis ACL keywords handled by the SETUSER command handler but absent from the parser.

When these keywords appear at index 3 (the first argument after `user <name> on/off`), they fall through to the `else if (i == 3)` branch and are silently treated as plain passwords. At any other position, they throw `IllegalArgumentException`.

**Scenarios:**

1. `U.fromLiteral("user test on resetkeys ~* +@all")` — `resetkeys` becomes `Password.plain("resetkeys")` instead of resetting keys.
2. `U.fromLiteral("user test on >mypass resetkeys ~* +@all")` — `resetkeys` at index 4 throws `IllegalArgumentException`.

**Impact:**

- Manually edited ACL files using these keywords are silently misinterpreted.
- Redis ACL file compatibility: Redis accepts all four keywords in ACL files (verified against Redis 7.2.11). Additionally, Redis always writes `resetchannels` in the ACL file output — Velo cannot parse this correctly.
- Position-dependent behavior makes debugging difficult.

**Redis 7.2.11 verification:**

| Keyword | Redis accepts in ACL file? | Redis writes to ACL file on SAVE? | Effect |
|---------|---------------------------|----------------------------------|--------|
| `reset` | Yes — resets all, then applies subsequent rules | No — outputs resulting state only | User `off`, `-@all`, cleared keys/channels/passwords |
| `resetkeys` | Yes — clears key rules, then applies subsequent `~` rules | No — outputs current key rules only | `~oldkey resetkeys ~newkey` → only `~newkey` kept |
| `resetpass` | Yes — clears passwords, then applies subsequent `>` rules | No — outputs current passwords only | `>old resetpass >new` → only `#<sha256(new)>` kept |
| `resetchannels` | Yes — clears channels, then applies subsequent `&` rules | **Yes** — Redis always writes `resetchannels` | Always present in ACL file output |

**Suggested fix:**

Add keyword handling in `fromLiteral()`:

```java
} else if ("resetkeys".equals(part)) {
    u.resetKey();
} else if ("resetchannels".equals(part)) {
    u.resetPubSub();
} else if ("resetpass".equals(part)) {
    u.resetPassword();
} else if ("reset".equals(part)) {
    u.setOn(false);
    u.resetPassword();
    u.resetCmd();
    u.resetKey();
    u.resetPubSub();
} else if (i == 3) {
    u.addPassword(Password.plain(part));
} else {
    throw new IllegalArgumentException("Invalid literal: " + part);
}
```

---

## Finding 4: `ACL SAVE` is non-atomic — process crash during write corrupts the ACL file

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/command/AGroup.java:328-329`

**Code excerpt:**

```java
// AGroup.java:328-329
FileUtils.writeLines(aclFile, "UTF-8", lines);
```

**Root cause:**

`FileUtils.writeLines()` writes directly to the target file. If the process crashes or loses power during the write, the file may be partially written — containing some users but not others, or truncated mid-line.

On restart, `loadAclFile()` will try to parse the corrupted file and throw an exception, preventing the server from starting. The only recovery is manual file editing.

Redis uses atomic file replacement: write to a temporary file, then `rename()` (which is atomic on POSIX). If the rename fails, the old file remains intact.

**Impact:**

- A crash during ACL SAVE can leave the ACL file in an unrecoverable state.
- On restart, the server fails with a parse error from `loadAclFile()`.
- No backup of the previous file content is maintained.

**Suggested fix:**

Write to a temporary file, then atomically rename:

```java
var aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile();
var tmpFile = new File(aclFile.getParent(), aclFile.getName() + ".tmp");
FileUtils.writeLines(tmpFile, "UTF-8", lines);
Files.move(tmpFile.toPath(), aclFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
```

---

## Finding 5: `ACL SETUSER` `reset` rule does not propagate `resetchannels` default correctly to non-owner inners

**Severity:** Low

**Files:**

- `src/main/java/io/velo/command/AGroup.java:383-391`
- `src/main/java/io/velo/acl/AclUsers.java:314-321`

**Code excerpt:**

```java
// AGroup.java:383-391 — reset rule
} else if ("reset".equals(rule)) {
    temp.setOn(false);
    temp.resetPassword();
    temp.resetCmd();
    temp.resetKey();
    temp.resetPubSub();
    if (ValkeyRawConfSupport.aclPubsubDefault) {
        temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
    }
}
```

```java
// AclUsers.java:314-321 — upInsert fan-out
public void upInsert(String user, UpdateCallback<U> callback) {
    var inner = getOwnedInner();
    if (inner != null) {
        inner.upInsert(user, callback);
    }
    changeUser(inner2 -> inner2.upInsert(user, callback));
}
```

**Root cause:**

This is a minor consistency concern. The `upInsert()` callback captures `ValkeyRawConfSupport.aclPubsubDefault` at callback creation time. If the config value changes between the owner inner's execution and the event loop execution, different inners could get different pubsub defaults.

In practice, `ValkeyRawConfSupport.aclPubsubDefault` is set once at startup and never changed, so this is a theoretical concern. However, the callback closure captures a reference to a mutable static, not a snapshot of its value.

**Impact:**

- Theoretical only — `aclPubsubDefault` does not change at runtime.
- If future changes make this config dynamic, the race could cause inconsistent pubsub defaults across inners.

**Suggested fix:**

Capture the config value into a local variable before creating the callback:

```java
var pubsubDefault = ValkeyRawConfSupport.aclPubsubDefault;
aclUsers.upInsert(user, u -> {
    // ...
    if (pubsubDefault) { ... }
});
```

---

## Reviewer Verification (AI agent 2)

Reviewer: AI agent 2
Review date: 2026-05-12
Reviewed commit: HEAD of `review/acl-security` (fix commits `0e48ecb`, `8896cde`)

### Finding 1 - CONFIRMED

**Verification:** `U.java:300-301` confirms double-hashing of plain passwords:

```java
} else {
    sb.append(ADD_HASH_PASSWORD_PREFIX).append(DigestUtils.sha256Hex(pwd.passwordEncoded)).append(" ");
}
```

Plain passwords (`Password.plain("mypass")`) get serialized as `#<sha256("mypass")>` instead of `>mypass`. When `fromLiteral()` parses this back, it creates `Password.sha256HexEncoded(sha256("mypass"))` — a different encode type. `Password.equals()` at `U.java:148` compares both `passwordEncoded` AND `encodeType`, so `removePassword()` fails to match.

**Note:** The existing test at `UTest.groovy:193-219` (`test literal round-trip preserves multiple passwords`) actually codifies this buggy behavior — it asserts that `>pass1` is NOT in the literal output (line 207). This test will need to be updated when Bug 1 is fixed.

**Impact severity:** HIGH — ACL SAVE/LOAD roundtrip breaks password management operations.

---

### Finding 2 - CONFIRMED

**Verification:** `BaseCommand.java:170-179` (`dataToLine()`) and `BaseCommand.java:387` (`execute()`) confirm space-joined split issue:

```java
// dataToLine() joins with spaces
sb.append(new String(data[i]));
if (i != data.length - 1) sb.append(" ");
```

```java
// execute() splits on spaces
var dataStrings = allDataString.split(" ");
```

`XAclUpdate.apply()` at `AGroup.java:126` calls `aGroup.execute(line)` with the space-joined string. If a password contains a space, it gets split into multiple tokens.

**Impact severity:** HIGH — silent binlog corruption for any ACL SETUSER with space-containing passwords.

---

### Finding 3 - CONFIRMED

**Verification:** `U.java:436-441` shows fallback at position 3 treats unknown tokens as plain passwords:

```java
} else if (i == 3) {
    u.addPassword(Password.plain(part));
} else {
    throw new IllegalArgumentException("Invalid literal: " + part);
}
```

`AGroup.java:354-391` shows `resetkeys`, `resetchannels`, `resetpass`, and `reset` ARE recognized in SETUSER handler but NOT in `fromLiteral()`.

**Impact severity:** MEDIUM — Redis ACL file compatibility broken; manually edited ACL files misinterpreted.

---

### Finding 4 - CONFIRMED

**Verification:** `AGroup.java:329` uses direct write:

```java
FileUtils.writeLines(aclFile, "UTF-8", lines);
```

No temp file, no atomic rename. Crash during write corrupts ACL file.

**Impact severity:** MEDIUM — process crash leaves ACL file in unrecoverable state.

---

### Finding 5 - CONFIRMED (Theoretical)

**Verification:** `AclUsers.java:314-321` and `AGroup.java:358,389` show closure captures `ValkeyRawConfSupport.aclPubsubDefault` by reference:

```java
// AGroup.java:358 — inside callback lambda
if (ValkeyRawConfSupport.aclPubsubDefault) {
    temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
}
```

`ValkeyRawConfSupport.aclPubsubDefault` at `ValkeyRawConfSupport.java:37` is a `public static boolean` — mutable static. The callback captures the variable reference, not a snapshot of its value.

**Impact severity:** LOW (theoretical only) — config is set once at startup; if it ever becomes dynamic, race condition could occur.

---

## Summary

| Finding | Status | Reviewer Confidence |
|---------|--------|---------------------|
| 1 - `literal()` double-hashes plain passwords | **FIXED** (commit `55a9d11`) | High |
| 2 - `dataToLine()` + `split(" ")` breaks space passwords | **FIXED** (commit `fab6d4d`) | High |
| 3 - `fromLiteral()` missing reset keywords | **FIXED** (commit `04c084b5`) | High |
| 4 - `ACL SAVE` non-atomic | **CONFIRMED** | High |
| 5 - `reset` closure captures mutable static | **CONFIRMED** | Medium |

All 5 findings verified. Ready for fix implementation.

## Suggested Fix Order

1. Fix Finding 1 first — password removal breakage after SAVE/LOAD is a data integrity issue that silently prevents operators from removing access.
2. Fix Finding 2 — binlog corruption for space-containing passwords silently breaks replica ACL state.
3. Fix Finding 3 — `fromLiteral()` keyword support for Redis ACL file compatibility.
4. Fix Finding 4 — atomic ACL SAVE prevents file corruption on crash.
5. Fix Finding 5 — capture config snapshot in closure (trivial, defensive).

---

## Review Feedback - Bug 1 Fix (Round 5)

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `0cef56f6b66258db093ef7260f70974b1bc9cbcb` (`fix: U.literal() uses > prefix for plain passwords, not #<sha256hex>`)
Also reviewed: `ec7667f5e39909f60e29d59a67aded55749b0a22` (`docs: fix incorrect Redis compatibility claim in R5 bug 1`)

### Summary of Fix

The commit changes `U.literal()` line 301 from:
```java
sb.append(ADD_HASH_PASSWORD_PREFIX).append(DigestUtils.sha256Hex(pwd.passwordEncoded)).append(" ");
```
to:
```java
sb.append(ADD_PASSWORD_PREFIX).append(pwd.passwordEncoded).append(" ");
```

This makes plain passwords serialize as `>mypass` instead of `#<sha256hex("mypass")>`, preserving the `Password.plain` encode type through the `literal()` → `fromLiteral()` roundtrip.

### Redis Behavior Verification

Tested against Redis 7.2.11 (`~/.proof/bin/redis-server`):

| Operation | Redis ACL file / ACL LIST output |
|-----------|----------------------------------|
| `ACL SETUSER test on >mypass` then `ACL SAVE` | `#ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222` |
| `ACL SETUSER test on >mypass` then `ACL LIST` | `#ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222` |
| Edit ACL file with `>filepass`, then load | Loaded OK, normalized to `#5338fa6084cc3095292ae577ea696b4d7bfb51f87653b9ad8546268a74615a15` on next SAVE |
| `ACL SETUSER test <mypass` after SAVE/LOAD | Password removed successfully |

**Redis normalizes all passwords to `#<sha256hex>` in the ACL file.** It does NOT preserve the `>password` format. Yet password removal (`<mypass`) still works after a roundtrip because Redis's internal representation handles the equivalence correctly.

### Assessment

**The fix took the wrong approach.** The root cause was correctly identified (my Finding 1): `Password.equals()` compares encode type, so `plain` can't match `sha256Hex` after roundtrip. But the fix changed `literal()` to output `>password` instead of fixing the comparison semantics.

**Problems with the fix:**

1. **Medium — ACL file format diverges from Redis.** Redis always outputs `#<sha256hex>` for passwords (both plain and pre-hashed). Velo now outputs `>password` for plain passwords. This makes Velo ACL files incompatible with Redis — a Redis server loading a Velo-generated ACL file will accept `>password` but normalize it to `#<hash>` on the next SAVE, while Velo will keep writing `>password`. Mixed environments will have inconsistent ACL file formats.

2. **Medium — passwords stored in plaintext on disk.** `ACL SAVE` now writes `>mypass` to the ACL file, exposing plaintext passwords. Redis always hashes to SHA-256 before writing. This is a security regression.

3. **Medium — the suggested fix in the review was incorrect.** My original Finding 1 suggested changing `literal()` to output `>password`. This was wrong — I should have verified against Redis first. Agent 2 correctly identified this in commit `ec7667f`.

**Correct approach:**

The fix should revert `literal()` to output `#<sha256hex>` (matching Redis), and instead fix `Password.equals()` / `removePassword()` / `addPassword()` to handle type-equivalent matching:

```java
// In Password — check semantic equality
public boolean semanticallyEquals(Password other) {
    if (this.encodeType == other.encodeType) {
        return this.passwordEncoded.equals(other.passwordEncoded);
    }
    // Cross-type: hash the plain one and compare
    if (this.encodeType == PasswordEncodedType.plain && other.encodeType == PasswordEncodedType.sha256Hex) {
        return DigestUtils.sha256Hex(this.passwordEncoded).equals(other.passwordEncoded);
    }
    if (this.encodeType == PasswordEncodedType.sha256Hex && other.encodeType == PasswordEncodedType.plain) {
        return this.passwordEncoded.equals(DigestUtils.sha256Hex(other.passwordEncoded));
    }
    return false;
}
```

Then use `semanticallyEquals()` in `removePassword()` and the dedup check in `addPassword()`.

### Verification

- Tests pass: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest"` — BUILD SUCCESSFUL.
- The new test `'test literal round-trip preserves password removal with <password syntax'` verifies the fix works with the `>password` format.
- However, the test does not verify Redis ACL file compatibility.

### Follow-ups

- **Pre-commit**: revert `literal()` to output `#<sha256hex>` for plain passwords (matching Redis behavior).
- **Pre-commit**: implement `semanticallyEquals()` in `Password` and use it in `removePassword()` and `addPassword()`.
- **Pre-commit**: update tests to verify `#<sha256hex>` output format and password removal after roundtrip.
- **Post-commit**: verify ACL file compatibility with Redis 7.2.11 using `~/.proof/bin/redis-server`.

### Fix Implemented (commit `55a9d11`)

Implemented the reviewer's suggested approach:

1. **Reverted** `literal()` to output `#<sha256hex>` for plain passwords (matching Redis)
2. **Added** `Password.semanticallyEquals()` method for cross-type comparison:
   - If same encodeType: compare passwordEncoded directly
   - If different types: normalize plain to SHA256 hex and compare
3. **Updated** `removePassword()` to use `semanticallyEquals()` instead of `equals()`
4. **Updated** `addPassword()` deduplication to use `semanticallyEquals()`
5. **Updated** tests to expect proper deduplication and verify password removal after roundtrip

This approach matches Redis behavior: passwords are stored as `#<hash>` in ACL files, but internally Redis handles equivalence between plain-text and pre-hashed passwords correctly.

---

## Review Feedback - Bug 1 Fix Rework (Round 5)

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commits: `0cef56f` (initial fix), `55a9d11` (rework with semanticallyEquals), `cbf584b` (docs)

### Summary

Two commits form the fix:

1. `0cef56f` changed `literal()` to output `>password` for plain passwords (wrong approach — diverged from Redis, stored plaintext on disk).
2. `55a9d11` reworked the fix: reverted `literal()` back to `#<sha256hex>`, added `Password.semanticallyEquals()` for cross-type matching, used it in `addPassword()` and `removePassword()`.

### Strengths

- `literal()` now outputs `#<sha256hex>` for plain passwords — matches Redis 7.2.11 behavior exactly (verified with `~/.proof/bin/redis-server`).
- `semanticallyEquals()` correctly handles both cross-type directions: `plain→sha256Hex` and `sha256Hex→plain`.
- `addPassword()` deduplication and `removePassword()` both use `semanticallyEquals()` — password removal works after SAVE/LOAD roundtrip.
- `equals()` is preserved for strict identity comparison (used in `Password.NO_PASSWORD` checks), while `semanticallyEquals()` is used for operational matching. This is a clean separation.
- Tests updated to verify `#<sha256hex>` output format and password removal after roundtrip.

### Concerns

1. **Low — some branches in `semanticallyEquals()` are uncovered**

   JaCoCo:
   - Line 155 (`other == null`): `pc bpc` — 2 of 4 branches missed, `null` case not hit (`nc` on line 156)
   - Line 164 (`sha256Hex→plain`): `pc bpc` — 2 of 4 branches missed
   - Line 167 (fallback `return false`): `nc`

   The critical cross-type path (plain→sha256Hex, line 161-162) is covered. The uncovered branches are defensive checks that are unlikely to be hit in normal operation.

2. **Low — `0cef56f` (initial wrong fix) is still in history**

   The initial fix that output `>password` was superseded by `55a9d11`, but both commits remain. This is fine for linear history but could confuse bisect if anyone looks at the intermediate state.

3. **Informational — `semanticallyEquals()` computes SHA-256 on every comparison call**

   For `plain→sha256Hex` comparison, `DigestUtils.sha256Hex()` is called on the plain password every time. For the dedup check in `addPassword()`, this is called for each existing password in the list. In practice, password lists are very short (1-3 entries), so this is not a performance concern.

### Verification

- Tests pass: `./gradlew :cleanTest :test --tests "io.velo.acl.UTest"` — BUILD SUCCESSFUL.
- JaCoCo: `semanticallyEquals()` line 151 covered, critical branches (same-type at line 158, plain→sha256Hex at line 161-162) fully covered.
- Redis compatibility verified with `~/.proof/bin/redis-server` 7.2.11: ACL file format matches (`#<sha256hex>` for all passwords).

### Follow-ups

- **Optional**: add a test that exercises `sha256Hex→plain` direction in `semanticallyEquals()` (e.g., add a sha256Hex password, then remove with `Password.plain()`). ✓ Done (commit `ca3acff`)
- **Post-commit**: none beyond the above.

---

## Review Feedback - Bug 2 Fix (Round 5)

Reviewer: AI agent 1
Review date: 2026-05-12
Reviewed commit: `fab6d4d76f75bffe57fe1e9050db620ffecfc3b7` (`fix: handle space-containing args in dataToLine/execute roundtrip`)

### Summary of Fix

The commit adds quoting/escaping to `dataToLine()` and a corresponding `parseLine()` parser for `execute()`:

1. **`dataToLine()`** (`BaseCommand.java:171-184`): Arguments containing spaces, double-quotes, or backslashes are wrapped in double-quotes with escaping (`\` → `\\`, `"` → `\"`).
2. **`parseLine()`** (`BaseCommand.java:427-461`): New private static method that tokenizes the line respecting quoted strings and escape sequences.
3. **`execute()`** (`BaseCommand.java:393`): Replaced `split(" ")` with `parseLine()`.

Production change: 56 lines in `BaseCommand.java`. Test: 42 lines in `AGroupTest.groovy`.

### Strengths

- The approach is sound: quote-escape roundtrip handles the space-containing argument case correctly.
- `dataToLine()` also escapes `"` and `\` characters, making the encoding unambiguous.
- `parseLine()` correctly handles: unquoted tokens, quoted tokens, escaped `\"` and `\\`, and trailing tokens after the last space.
- The test `'test dataToLine and execute roundtrip with space-containing password'` directly verifies the fix: sets password `"my secret"` via `dataToLine()` → `execute()`, then confirms `checkPassword('my secret')` passes and `checkPassword('my')` / `checkPassword('secret')` fail.
- The fix is backward-compatible: existing binlog entries without quotes are parsed correctly by `parseLine()` (same behavior as old `split(" ")`).

### Concerns

1. **Medium — escape handling in `parseLine()` has zero test coverage (JaCoCo `nc` on lines 434-439)**

   The `\\` and `\"` escape handling paths are never executed. The test only uses a space-containing password, not one with quotes or backslashes. JaCoCo confirms:
   - Line 434 (`c == '\\'`): `pc bpc` — 3 of 4 branches missed
   - Lines 435-439: all `nc`

   A test with a password containing `"` or `\` would cover these paths. For example: `>my"pass` or `>my\pass`.

2. **Low — `dataToLine()` escape branches partially covered**

   Line 175: `pc bpc` — 2 of 6 branches missed. The `contains("\"")` and `contains("\\")` conditions are not independently tested. The `contains(" ")` path is covered.

3. **Informational — `parseLine()` is `private static` but `execute()` is `@TestOnly`**

   `parseLine()` is only called from `execute()` which is annotated `@TestOnly`. However, `execute()` is also called from production code in `XAclUpdate.apply()`. This pre-existing `@TestOnly` annotation inconsistency is not introduced by this fix.

### Verification

- Test passes: `./gradlew :cleanTest :test --tests "io.velo.command.AGroupTest.test dataToLine and execute roundtrip with space-containing password"` — BUILD SUCCESSFUL.
- JaCoCo:
  - `dataToLine()`: `fc` on lines 172-180, `pc bpc` on line 175 (quoting branch partially covered)
  - `parseLine()`: `fc` on lines 427-461, quote/space handling `fc bfc`, escape handling `nc` (lines 434-439)

### Follow-ups

- **Pre-commit**: add a test with a password containing `"` or `\` to cover the escape handling branches in both `dataToLine()` and `parseLine()`. ✓ Done (commit `c893697`)
- **Post-commit**: none beyond the above.
