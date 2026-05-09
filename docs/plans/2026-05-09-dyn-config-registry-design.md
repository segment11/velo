# DynConfig Supported-Item Registry Design

Date: 2026-05-09
Status: Approved design

## Problem

`DynConfig` currently stores dynamic settings in a raw `HashMap<String, Object>` and applies side effects from a
key-based switch. This creates three classes of bugs:

- known settings can arrive as different Java types depending on the update path;
- invalid values can be written to `dyn-config.json` before callback parsing fails;
- arbitrary keys can be persisted even though the server does not support them.

The target behavior is to reject unsupported dynamic config keys and validate supported values before they are
persisted or applied.

## Chosen Approach

Use a supported-item registry. Each supported key is represented by a small parser/validator/applier object.
All dynamic config write paths must go through that registry.

Conceptually:

```java
interface DynConfigItem<T> {
    String key();
    T parse(String raw);
    void validate(T value);
    Object normalize(T value);
    void apply(short slot, OneSlot oneSlot, T value);
}
```

`DynConfig` owns a static registry:

```java
Map<String, DynConfigItem<?>> SUPPORTED_ITEMS
```

For an update:

1. Look up the key in `SUPPORTED_ITEMS`.
2. Reject the key if it is not supported.
3. Parse the raw string or existing JSON value through the item parser.
4. Validate the typed value.
5. Persist the normalized value.
6. Apply the side effect.

This keeps dynamic config extensible while avoiding raw `Object` casts and unsupported arbitrary keys.

## Supported Keys

Initial supported keys should cover current behavior:

- `max_connections`: integer, must be `> 0`, applies to `SocketInspector`.
- `dict_key_prefix_or_suffix_groups`: string, must be non-empty, applies to `TrainSampleJob`.
- `monitor_big_key_top_k`: integer, must be `> 0`, applies per slot via `OneSlot.initBigKeyTopK`.
- `type_zset_member_max_length`: short, must be `> 0`, applies to `RedisZSet.ZSET_MEMBER_MAX_LENGTH`.
- `type_set_member_max_length`: short, must be `> 0`, applies to `RedisHashKeys.SET_MEMBER_MAX_LENGTH`.
- `type_zset_max_size`: short, must be `> 0`, applies to `RedisZSet.ZSET_MAX_SIZE`.
- `type_hash_max_size`: short, must be `> 0`, applies to `RedisHashKeys.HASH_MAX_SIZE`.
- `type_list_max_size`: short, must be `> 0`, applies to `RedisList.LIST_MAX_SIZE`.
- `repl_connect_timeout_millis`: long, must be `> 0`, used by replication pair creation.

Internal slot state keys such as `masterUuid`, `readonly`, `canRead`, `canWrite`, `binlogOn`, and test-only
`testKey` should remain controlled by typed methods unless there is a concrete need to expose them through
`manage dyn-config`.

## Error Handling

Unsupported keys are rejected consistently:

- `manage dyn-config` returns an error reply instead of persisting the key.
- startup `dynConfig.*` overrides fail fast with `IllegalArgumentException`.
- persisted `dyn-config.json` entries for unsupported keys should fail startup with a clear message, or be ignored only if an explicit compatibility flag is added later.

Invalid supported values are rejected before writing to disk. The old value remains in memory and on disk.

## Data Flow

Runtime command path:

`ManageCommand.dynConfig()` -> `OneSlot.updateDynConfig()` -> `DynConfig.update()` -> registry parse/validate -> persist -> apply

Startup override path:

`MultiWorkerServer.confForSlot()` collects current `dynConfig.*` values -> `LocalPersist.initSlotsAgainAfterMultiShardLoadedOrChanged()` -> `DynConfig.update()` -> registry parse/validate -> persist -> apply

Replay path:

`DynConfig` constructor reads `dyn-config.json` -> for each entry, registry parse/validate -> apply

## Testing

Focused tests should cover:

- supported key with string input, especially `max_connections="100"`;
- invalid value for a supported key is not persisted;
- unsupported key is rejected by `manage dyn-config`;
- startup `dynConfig.unknown=value` fails fast;
- stale `ConfForGlobal.initDynConfigItems` is cleared between config loads;
- JSON replay handles normalized values and rejects invalid persisted known keys with clear errors.

## Migration Notes

Existing JSON files may contain unknown keys from older behavior. The default design rejects them because the requested policy is to reject unsupported config items. If backward compatibility is needed later, add a one-time migration or an explicit compatibility mode rather than silently accepting unknown keys.
