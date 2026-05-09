# Bug 26 - Configuration Module Review (Round 3)

Reviewer: AI agent 1
Review date: 2026-05-09
Branch: `main`
Scope: Configuration module round 3 - residual dynamic-config bugs after the round 2 fixes, startup dyn-config application, and repeated config load behavior.

## Files Reviewed

- `doc/design/08_configuration_design.md`
- `doc/bug_reviews/bug_24_configuration_module_review_round_1.md`
- `doc/bug_reviews/bug_25_configuration_module_review_round_2.md`
- `src/main/resources/velo.properties`
- `src/main/java/io/velo/MultiWorkerServer.java`
- `src/main/java/io/velo/ConfForGlobal.java`
- `src/main/java/io/velo/persist/LocalPersist.java`
- `src/main/java/io/velo/persist/DynConfig.java`
- `src/main/java/io/velo/persist/OneSlot.java`
- `src/main/java/io/velo/SocketInspector.java`
- `dyn/src/io/velo/command/ConfigCommand.groovy`
- `dyn/src/io/velo/command/ManageCommand.groovy`
- `src/test/groovy/io/velo/persist/DynConfigTest.groovy`
- `src/test/groovy/io/velo/MultiWorkerServerTest.groovy`
- `dyn/test/io/velo/command/ManageCommandTest.groovy`

## Finding 1: String-valued `max_connections` dyn config crashes before validation

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/DynConfig.java:82-89`
- `src/main/java/io/velo/MultiWorkerServer.java:1288-1292`
- `src/main/java/io/velo/persist/LocalPersist.java:237-240`
- `src/main/java/io/velo/persist/OneSlot.java:870-875`
- `dyn/src/io/velo/command/ManageCommand.groovy:886-902`

**Code excerpt:**

```java
case SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG -> {
    int valueInt = (int) value;
    if (valueInt <= 0) {
        log.error("Dyn config for global set max_connections ignored, invalid value={}, slot={}", value, currentSlot);
    } else {
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(valueInt);
        log.warn("Dyn config for global set max_connections={}, slot={}", value, currentSlot);
    }
}
```

```java
dynConfig.getChildren().forEach((k, v) -> {
    ConfForGlobal.initDynConfigItems.put(k, v.getValue());
});
...
firstOneSlot.getDynConfig().update(entry.getKey(), entry.getValue());
```

**Root cause:**

The round 2 fix validates positive integer values, but it still casts `value` directly to `int`. Not every dyn-config path stores an `Integer`:

- Startup `dynConfig.*` properties are collected as strings through `v.getValue()`.
- `manage dyn-config key value` sends a string to `OneSlot.updateDynConfig()`.
- A manually edited `dyn-config.json` may store `"max_connections": "100"` as a JSON string.

Those paths reach `DynConfig.AfterUpdateCallbackInner.afterUpdate()` with a `String`, so `(int) value` throws `ClassCastException` before the positivity check runs.

**Impact:**

- `dynConfig.max_connections=100` in `velo.properties` can crash startup while applying initial dyn-config items.
- `manage dyn-config max_connections 100` can fail the async command path instead of applying a valid value.
- A string-valued persisted `dyn-config.json` entry can crash slot initialization on restart.
- The fix for non-positive `max_connections` is incomplete for the documented initial dyn-config mechanism because the value type differs from the `CONFIG SET` path.

**Suggested fix:**

Parse through `value.toString()` and handle invalid syntax before applying:

```java
int valueInt;
try {
    valueInt = Integer.parseInt(value.toString());
} catch (NumberFormatException e) {
    log.error("Dyn config max_connections ignored, invalid integer value={}, slot={}", value, currentSlot);
    return;
}
if (valueInt <= 0) {
    log.error("Dyn config max_connections ignored, non-positive value={}, slot={}", value, currentSlot);
    return;
}
MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(valueInt);
```

Add tests for both `config.update("max_connections", "100")` and `config.update("max_connections", "0")`.

---

## Finding 2: `DynConfig.update()` persists invalid values before callbacks validate or apply them

**Severity:** High

**Files:**

- `src/main/java/io/velo/persist/DynConfig.java:295-301`
- `src/main/java/io/velo/persist/DynConfig.java:82-118`
- `dyn/src/io/velo/command/ManageCommand.groovy:886-902`
- `src/main/java/io/velo/persist/OneSlot.java:870-875`

**Code excerpt:**

```java
public void update(@NotNull String key, @NotNull Object value) throws IOException {
    data.put(key, value);
    var objectMapper = new ObjectMapper();
    objectMapper.writeValue(dynConfigFile, data);
    log.info("Update dyn config, key={}, value={}, slot={}", key, value, slot);

    afterUpdateCallback.afterUpdate(key, value);
}
```

Callback examples:

```java
case "type_zset_member_max_length" -> {
    RedisZSet.ZSET_MEMBER_MAX_LENGTH = Short.parseShort(value.toString());
}
case "type_hash_max_size" -> {
    RedisHashKeys.HASH_MAX_SIZE = Short.parseShort(value.toString());
}
```

**Root cause:**

`DynConfig.update()` mutates the in-memory map and writes the JSON file before invoking the callback that validates/parses known dynamic settings. If the callback throws, the invalid value is already persisted.

This is reachable through `manage dyn-config`, which accepts arbitrary key/value strings and dispatches them to every slot through `oneSlot.updateDynConfig(configKey, configValue)`.

Examples:

- `manage dyn-config type_zset_max_size abc` writes `"abc"` to `dyn-config.json`, then `Short.parseShort("abc")` throws.
- `manage dyn-config max_connections abc` writes `"abc"` and then the current `(int) value` cast throws.
- Any invalid persisted known key is replayed by the `DynConfig` constructor and can fail slot initialization repeatedly until the JSON file is manually repaired.

**Impact:**

- A rejected or failed dynamic config update can still poison the persistent config file.
- Restart can repeatedly fail because the constructor replays the bad value from disk.
- Multi-slot `manage dyn-config` can partially write bad values across slots before the async command reports an error.

**Suggested fix:**

Validate/apply before committing to disk, or add a rollback path:

```java
var previous = data.get(key);
afterUpdateCallback.afterUpdate(key, value);
data.put(key, value);
objectMapper.writeValue(dynConfigFile, data);
```

For callbacks with side effects, prefer a two-phase design: parse and validate first, then persist, then apply side effects. At minimum, if callback validation fails, restore the previous map value and do not write the bad value to disk.

---

## Finding 3: `initDynConfigItems` is static and never cleared, so stale startup dyn-config overrides leak across config loads

**Severity:** Medium

**Files:**

- `src/main/java/io/velo/ConfForGlobal.java:187-190`
- `src/main/java/io/velo/MultiWorkerServer.java:1288-1294`
- `src/main/java/io/velo/persist/LocalPersist.java:237-240`
- `src/test/groovy/io/velo/MultiWorkerServerTest.groovy:540-567`

**Code excerpt:**

```java
public static final HashMap<String, String> initDynConfigItems = new HashMap<>();
```

```java
var dynConfig = config.getChild("dynConfig");
if (dynConfig != null) {
    dynConfig.getChildren().forEach((k, v) -> {
        ConfForGlobal.initDynConfigItems.put(k, v.getValue());
    });
}
log.warn("Global config, initDynConfigItems={}", ConfForGlobal.initDynConfigItems);
```

```java
if (!ConfForGlobal.initDynConfigItems.isEmpty()) {
    for (var entry : ConfForGlobal.initDynConfigItems.entrySet()) {
        firstOneSlot.getDynConfig().update(entry.getKey(), entry.getValue());
    }
}
```

**Root cause:**

`initDynConfigItems` is a process-wide static map. `MultiWorkerServer.InnerModule.confForSlot()` only adds or overwrites entries when the current config contains `dynConfig.*`; it never clears entries from a previous config load.

This matters because the project tests and embedded launcher paths call `confForSlot()` more than once in the same JVM. The existing `MultiWorkerServerTest` asserts that a dyn-config entry was added, but there is no reset before later config loads.

**Impact:**

- A second server/config initialization in the same JVM can apply stale dyn-config entries even when the new config has no `dynConfig.*`.
- Removing a `dynConfig.*` key from a later config file does not remove it from `ConfForGlobal.initDynConfigItems`.
- Test order or embedded use can become configuration-dependent because stale static state is applied in `LocalPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()`.

**Suggested fix:**

Clear the map before loading current config values:

```java
ConfForGlobal.initDynConfigItems.clear();
var dynConfig = config.getChild("dynConfig");
if (dynConfig != null) {
    dynConfig.getChildren().forEach((k, v) -> ConfForGlobal.initDynConfigItems.put(k, v.getValue()));
}
```

Add a focused test that calls `confForSlot()` first with `dynConfig.a=b`, then with a config that has no dyn-config child, and asserts `ConfForGlobal.initDynConfigItems.isEmpty()`.

---

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - String-valued `max_connections` dyn config crashes before validation | High | **Confirmed** | High |
| 2 - `DynConfig.update()` persists invalid values before callback validation | High | **Confirmed** | High |
| 3 - Static `initDynConfigItems` leaks stale startup overrides across config loads | Medium | **Confirmed** | High |

## Suggested Fix Order

1. Fix Finding 2 first, because it is the persistence-order root cause that can poison `dyn-config.json`.
2. Fix Finding 1 as part of the same dynamic-config validation pass or immediately after, because it is a concrete current crash path for `max_connections`.
3. Fix Finding 3 separately with a small startup-config test, since it touches global static bootstrap state rather than dyn-config file write semantics.

---

## Reviewer Notes (AI agent 2)

Reviewer: AI agent 2
Review date: 2026-05-09
Branch: `main`

### Finding 1 — CONFIRMED

**Verdict: Confirmed, High severity.**

Verified against source:

- `DynConfig.java:83` — `int valueInt = (int) value` performs a raw cast. When `value` is a `String` (not `Integer`), this throws `ClassCastException` before the positivity check at line 84 runs.
- Three confirmed string-value entry paths:
  1. **Startup via `velo.properties`**: `MultiWorkerServer.java:1291` calls `v.getValue()` which returns a `String`. This is stored in `ConfForGlobal.initDynConfigItems`, then applied via `LocalPersist.java:239` calling `firstOneSlot.getDynConfig().update(key, value)` — the value is a `String`.
  2. **`manage dyn-config` command**: `ManageCommand.groovy:896` does `new String(configValueBytes)`, then `OneSlot.java:874` calls `dynConfig.update(key, valueString)` — also a `String`.
  3. **Constructor replay from JSON**: `DynConfig.java:279` reads the JSON as `HashMap.class`. Jackson will deserialize a bare numeric JSON value as `Integer`, but a manually edited `"max_connections": "100"` (quoted string) would be a `String`.
- `DynConfig.java:97-118` — Other callbacks (`BigKeyTopK`, `type_zset_member_max_length`, etc.) already use `Integer.parseInt(value.toString())` or `Short.parseShort(value.toString())`, which handle both `String` and `Integer` inputs gracefully. Only `max_connections` at line 83 uses the unsafe `(int) value` cast.
- `DynConfigTest.groovy:64` — The existing test passes `100` (an `Integer`) to `config.update()`, so it never exercises the `String` path. Lines 102 and 107 test non-positive values but also as `Integer`, not `String`.

**Additional observation:**

`DynConfig.java:92` has the same pattern for `TrainSampleJob.KEY_IN_DYN_CONFIG`: `((String) value).split(",")` will throw `ClassCastException` if `value` is not a `String` (e.g., if Jackson deserializes an unquoted value as a non-String type). This is a similar but lower-priority variant since that key is always expected to be a comma-separated string. Not promoted to a separate finding but noted for the fix pass.

### Finding 2 — CONFIRMED

**Verdict: Confirmed, High severity.**

Verified against source:

- `DynConfig.java:295-301` — `update()` executes in this order:
  1. `data.put(key, value)` — mutates in-memory map
  2. `objectMapper.writeValue(dynConfigFile, data)` — writes to disk
  3. `afterUpdateCallback.afterUpdate(key, value)` — validates/applies

  If step 3 throws (e.g., `(int) value` ClassCastException for `max_connections` with a String, or `Short.parseShort("abc")` for type limit keys), the invalid value is already persisted in both the in-memory map and the JSON file.
- `DynConfig.java:267-286` — The constructor replays all entries via `afterUpdateCallback.afterUpdate()` on startup (lines 282-284). A bad persisted value causes repeated initialization failures across restarts until `dyn-config.json` is manually repaired.
- `OneSlot.java:870-875` — `updateDynConfig()` passes arbitrary strings from `manage dyn-config` directly to `dynConfig.update()`, with no pre-validation.
- `ManageCommand.groovy:900-902` — Dispatches to all slots asynchronously. A bad value can be partially persisted across some slots before the error is reported.

**The root cause and persistence-order analysis are accurate.** The suggested fix (validate before persist, or rollback on callback failure) correctly addresses this.

### Finding 3 — CONFIRMED

**Verdict: Confirmed, Medium severity.**

Verified against source:

- `ConfForGlobal.java:190` — `initDynConfigItems` is `public static final HashMap<String, String>` — process-wide static state.
- `MultiWorkerServer.java:1288-1293` — `confForSlot()` only does `put()` into this map. There is no `clear()` call before loading. If the new config has no `dynConfig.*` section, old entries remain.
- `LocalPersist.java:237-241` — `initSlotsAgainAfterMultiShardLoadedOrChanged()` iterates `ConfForGlobal.initDynConfigItems` and applies all entries to the first slot. Stale entries from a previous config load will be re-applied.
- The `DynConfig.update()` call at line 239 also triggers Finding 2 — a stale string value would hit the `(int) value` cast in Finding 1.
- This is a realistic concern in tests and embedded launcher scenarios where `confForSlot()` is called multiple times in the same JVM.

**The suggested fix (`clear()` before loading) is correct and minimal.**

### Summary

All three findings are confirmed. No refutations or refinements needed. The root causes, impact analyses, and suggested fixes described by AI agent 1 are accurate.

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 - String-valued `max_connections` dyn config crashes before validation | High | **Confirmed** | High |
| 2 - `DynConfig.update()` persists invalid values before callback validation | High | **Confirmed** | High |
| 3 - Static `initDynConfigItems` leaks stale startup overrides across config loads | Medium | **Confirmed** | High |
