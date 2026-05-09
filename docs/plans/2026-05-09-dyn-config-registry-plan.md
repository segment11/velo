# DynConfig Registry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace raw dynamic-config map writes with a supported-item registry that rejects unsupported keys and validates values before persistence.

**Architecture:** `DynConfig` will own a static registry of supported dynamic config items. Runtime commands, startup overrides, and JSON replay will all parse and validate through the same registry before persisting or applying side effects.

**Tech Stack:** Java 21, Groovy 4, Spock, Gradle, Jackson `ObjectMapper`.

---

### Task 1: Add registry coverage for `max_connections`

**Files:**
- Modify: `src/test/groovy/io/velo/persist/DynConfigTest.groovy`
- Modify: `src/main/java/io/velo/persist/DynConfig.java`

**Step 1: Write the failing tests**

Add cases to `DynConfigTest` that prove:

- `config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, "100")` applies successfully.
- `config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, "0")` throws or returns failure before changing `SocketInspector.maxConnections`.
- The invalid `"0"` value is not present after reloading `dyn-config.json`.

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest"
```

Expected: FAIL because `DynConfig` currently casts string values to `int`.

**Step 3: Implement minimal registry structure**

In `DynConfig.java`, add:

- a private `DynConfigItem<T>` interface;
- a static supported-items map;
- a `max_connections` item that parses `Integer.parseInt(value.toString())`, validates `> 0`, normalizes to `Integer`, and applies via `SocketInspector.setMaxConnections()`;
- an update flow that parses and validates before writing to `dyn-config.json`.

Keep existing typed setters such as `setReadonly`, `setCanRead`, `setCanWrite`, `setBinlogOn`, and `setMasterUuid` working.

**Step 4: Run test to verify it passes**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest"
```

Expected: PASS.

**Step 5: Check JaCoCo**

Open or grep:

```bash
rg -n "max_connections|SUPPORTED|Dyn config" build/reports/jacocoHtml/io.velo.persist/DynConfig.java.html
```

Expected: new parse/validation branches for `max_connections` are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/persist/DynConfig.java src/test/groovy/io/velo/persist/DynConfigTest.groovy
git commit -m "fix: validate max connections dyn config before persist"
```

### Task 2: Reject unsupported dynamic config keys

**Files:**
- Modify: `src/test/groovy/io/velo/persist/DynConfigTest.groovy`
- Modify: `dyn/test/io/velo/command/ManageCommandTest.groovy`
- Modify: `src/main/java/io/velo/persist/DynConfig.java`
- Modify: `src/main/java/io/velo/persist/OneSlot.java`
- Modify: `dyn/src/io/velo/command/ManageCommand.groovy`

**Step 1: Write the failing tests**

Add tests for:

- `config.update("unknown_key", "value")` fails and does not write the key.
- `manage dyn-config unknown_key value` returns an error reply instead of `OKReply`.

**Step 2: Run tests to verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest" --tests "io.velo.command.ManageCommandTest"
```

Expected: FAIL because unknown keys are currently persisted.

**Step 3: Implement unsupported-key rejection**

Add a public helper in `DynConfig`, for example:

```java
public static boolean isSupportedKey(String key)
```

or make `DynConfig.update()` throw `IllegalArgumentException` for unsupported keys.

Update `OneSlot.updateDynConfig()` so unsupported keys fail cleanly. Update `ManageCommand.dynConfig()` so the async reply becomes an `ErrorReply` instead of an unhandled exception where practical.

**Step 4: Run tests to verify pass**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest" --tests "io.velo.command.ManageCommandTest"
```

Expected: PASS.

**Step 5: Check JaCoCo**

Run:

```bash
rg -n "unsupported|unknown|isSupportedKey" build/reports/jacocoHtml/io.velo.persist/DynConfig.java.html build/reports/jacocoHtml/io.velo.command/ManageCommand.groovy.html
```

Expected: unsupported-key rejection lines are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/persist/DynConfig.java src/main/java/io/velo/persist/OneSlot.java dyn/src/io/velo/command/ManageCommand.groovy src/test/groovy/io/velo/persist/DynConfigTest.groovy dyn/test/io/velo/command/ManageCommandTest.groovy
git commit -m "fix: reject unsupported dyn config keys"
```

### Task 3: Add remaining supported config items

**Files:**
- Modify: `src/test/groovy/io/velo/persist/DynConfigTest.groovy`
- Modify: `src/test/groovy/io/velo/persist/OneSlotTest.groovy`
- Modify: `src/main/java/io/velo/persist/DynConfig.java`

**Step 1: Write failing tests**

Cover supported current keys:

- `dict_key_prefix_or_suffix_groups`
- `monitor_big_key_top_k`
- `type_zset_member_max_length`
- `type_set_member_max_length`
- `type_zset_max_size`
- `type_hash_max_size`
- `type_list_max_size`
- `repl_connect_timeout_millis`

For numeric fields, include one invalid value test such as `"0"` or `"abc"` that confirms no bad value is persisted.

**Step 2: Run tests to verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest" --tests "io.velo.persist.OneSlotTest"
```

Expected: FAIL for not-yet-registered keys or invalid-value persistence.

**Step 3: Implement registry items**

Add typed registry items for each key:

- string item for train dictionary groups, non-empty;
- integer item for big-key top K, `> 0`;
- short items for type limits, `> 0`;
- long item for replication connect timeout, `> 0`.

Normalize numeric values as numbers in JSON, not strings.

**Step 4: Run tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest" --tests "io.velo.persist.OneSlotTest"
```

Expected: PASS.

**Step 5: Check JaCoCo**

Run:

```bash
rg -n "type_zset|max_size|repl_connect_timeout|monitor_big_key" build/reports/jacocoHtml/io.velo.persist/DynConfig.java.html
```

Expected: new registry item paths are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/persist/DynConfig.java src/test/groovy/io/velo/persist/DynConfigTest.groovy src/test/groovy/io/velo/persist/OneSlotTest.groovy
git commit -m "fix: add typed dyn config registry items"
```

### Task 4: Clear startup `initDynConfigItems` between config loads

**Files:**
- Modify: `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`
- Modify: `src/main/java/io/velo/MultiWorkerServer.java`

**Step 1: Write failing test**

Add a test that:

1. Calls `confForSlot()` with a config containing `dynConfig.type_zset_max_size=4096`.
2. Asserts `ConfForGlobal.initDynConfigItems` contains that key.
3. Calls `confForSlot()` again with a config that has no `dynConfig`.
4. Asserts `ConfForGlobal.initDynConfigItems.isEmpty()`.

**Step 2: Run test to verify failure**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest"
```

Expected: FAIL because the static map is not cleared.

**Step 3: Implement clearing**

In `MultiWorkerServer.InnerModule.confForSlot()`, call:

```java
ConfForGlobal.initDynConfigItems.clear();
```

immediately before reading the current `dynConfig` child.

**Step 4: Run test**

Run:

```bash
./gradlew :test --tests "io.velo.MultiWorkerServerTest"
```

Expected: PASS.

**Step 5: Check JaCoCo**

Run:

```bash
rg -n "initDynConfigItems.clear|initDynConfigItems" build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html
```

Expected: clear and reload lines are covered.

**Step 6: Commit**

```bash
git add src/main/java/io/velo/MultiWorkerServer.java src/test/groovy/io/velo/MultiWorkerServerTest.groovy
git commit -m "fix: clear initial dyn config between loads"
```

### Task 5: Final focused verification

**Files:**
- Review: `doc/bug_reviews/bug_26_configuration_module_review_round_3.md`
- Review: `docs/plans/2026-05-09-dyn-config-registry-design.md`

**Step 1: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.DynConfigTest" --tests "io.velo.command.ManageCommandTest" --tests "io.velo.MultiWorkerServerTest" --tests "io.velo.persist.OneSlotTest"
```

Expected: PASS.

**Step 2: Inspect JaCoCo reports**

Check:

```bash
rg -n "max_connections|unsupported|type_zset|initDynConfigItems" build/reports/jacocoHtml/io.velo.persist/DynConfig.java.html build/reports/jacocoHtml/io.velo/MultiWorkerServer.java.html build/reports/jacocoHtml/io.velo.command/ManageCommand.groovy.html
```

Expected: touched branches are marked covered.

**Step 3: Review diff**

Run:

```bash
git status --short
git log --oneline -5
```

Expected: only intended commits/files are present.
