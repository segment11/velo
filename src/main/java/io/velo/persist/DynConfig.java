package io.velo.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.velo.MultiWorkerServer;
import io.velo.SocketInspector;
import io.velo.TrainSampleJob;
import io.velo.monitor.BigKeyTopK;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manages dynamic configuration settings for a Velo slot.
 */
public class DynConfig {
    private static final Logger log = LoggerFactory.getLogger(DynConfig.class);

    private final short slot;
    private final File dynConfigFile;
    private final OneSlot oneSlot;

    private final HashMap<String, Object> data;

    private interface DynConfigItem {
        Object parseAndValidate(@NotNull Object value);

        void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value);
    }

    private static final Map<String, DynConfigItem> SUPPORTED_ITEMS = Map.of(
            SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveInt(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    int maxConnections = (int) value;
                    MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(maxConnections);
                    log.warn("Dyn config for global set max_connections={}, slot={}", value, currentSlot);
                }
            },
            TrainSampleJob.KEY_IN_DYN_CONFIG, new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    var valueString = value.toString();
                    if (valueString.isBlank()) {
                        throw new IllegalArgumentException(TrainSampleJob.KEY_IN_DYN_CONFIG + " must not be blank");
                    }
                    return valueString;
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    ArrayList<String> keyPrefixOrSuffixGroupList = new ArrayList<>(Arrays.asList(((String) value).split(",")));
                    TrainSampleJob.setKeyPrefixOrSuffixGroupList(keyPrefixOrSuffixGroupList);
                    log.warn("Dyn config for global set dict_key_prefix_groups={}, slot={}", value, currentSlot);
                }
            },
            BigKeyTopK.KEY_IN_DYN_CONFIG, new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveInt(BigKeyTopK.KEY_IN_DYN_CONFIG, value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    oneSlot.initBigKeyTopK((int) value);
                    log.warn("Global config for current slot set monitor_big_key_top_k={}, slot={}", value, currentSlot);
                }
            },
            "type_zset_member_max_length", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveShort("type_zset_member_max_length", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    RedisZSet.ZSET_MEMBER_MAX_LENGTH = (Short) value;
                    log.warn("Dyn config for global set zset_member_max_length={}, slot={}", value, currentSlot);
                }
            },
            "type_set_member_max_length", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveShort("type_set_member_max_length", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    RedisHashKeys.SET_MEMBER_MAX_LENGTH = (Short) value;
                    log.warn("Dyn config for global set set_member_max_length={}, slot={}", value, currentSlot);
                }
            },
            "type_zset_max_size", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveShort("type_zset_max_size", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    RedisZSet.ZSET_MAX_SIZE = (Short) value;
                    log.warn("Dyn config for global set zset_max_size={}, slot={}", value, currentSlot);
                }
            },
            "type_hash_max_size", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveShort("type_hash_max_size", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    RedisHashKeys.HASH_MAX_SIZE = (Short) value;
                    log.warn("Dyn config for global set hash_max_size={}, slot={}", value, currentSlot);
                }
            },
            "type_list_max_size", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveShort("type_list_max_size", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                    RedisList.LIST_MAX_SIZE = (Short) value;
                    log.warn("Dyn config for global set list_max_size={}, slot={}", value, currentSlot);
                }
            },
            "repl_connect_timeout_millis", new DynConfigItem() {
                @Override
                public Object parseAndValidate(@NotNull Object value) {
                    return parsePositiveLong("repl_connect_timeout_millis", value);
                }

                @Override
                public void apply(short currentSlot, OneSlot oneSlot, @NotNull Object value) {
                }
            }
    );

    private static final Set<String> INTERNAL_KEYS = Set.of(
            "masterUuid",
            "readonly",
            "canRead",
            "canWrite",
            "binlogOn",
            "testKey"
    );

    /**
     * @param key the dynamic configuration key
     * @return true if the key can be updated through dynamic configuration commands
     */
    public static boolean isSupportedKey(@NotNull String key) {
        return SUPPORTED_ITEMS.containsKey(key);
    }

    private static int parsePositiveInt(@NotNull String key, @NotNull Object value) {
        int valueInt;
        try {
            valueInt = Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, given: " + value, e);
        }
        if (valueInt <= 0) {
            throw new IllegalArgumentException(key + " must be > 0, given: " + valueInt);
        }
        return valueInt;
    }

    private static short parsePositiveShort(@NotNull String key, @NotNull Object value) {
        int valueInt = parsePositiveInt(key, value);
        if (valueInt > Short.MAX_VALUE) {
            throw new IllegalArgumentException(key + " must be <= " + Short.MAX_VALUE + ", given: " + valueInt);
        }
        return (short) valueInt;
    }

    private static long parsePositiveLong(@NotNull String key, @NotNull Object value) {
        long valueLong;
        try {
            valueLong = Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a long, given: " + value, e);
        }
        if (valueLong <= 0) {
            throw new IllegalArgumentException(key + " must be > 0, given: " + valueLong);
        }
        return valueLong;
    }

    /**
     * @param key the key whose associated value is to be returned
     * @return the value associated with the specified key, or null if the key is not present
     */
    public Object get(@NotNull String key) {
        return data.get(key);
    }

    public interface AfterUpdateCallback {
        /**
         * @param key the key that was updated
         * @param value the new value for the key
         */
        void afterUpdate(@NotNull String key, @NotNull Object value);
    }

    private static class AfterUpdateCallbackInner implements AfterUpdateCallback {
        private final short currentSlot;
        private final OneSlot oneSlot;

        /**
         * @param currentSlot slot index corresponding to this dynamic configuration
         * @param oneSlot OneSlot instance associated with this dynamic configuration
         */
        public AfterUpdateCallbackInner(short currentSlot, OneSlot oneSlot) {
            this.currentSlot = currentSlot;
            this.oneSlot = oneSlot;
        }

        @Override
        public void afterUpdate(@NotNull String key, @NotNull Object value) {
            // Handle updates for specific keys
            switch (key) {
                case SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG -> {
                    int valueInt = (int) value;
                    if (valueInt <= 0) {
                        log.error("Dyn config for global set max_connections ignored, invalid value={}, slot={}", value, currentSlot);
                    } else {
                        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(valueInt);
                        log.warn("Dyn config for global set max_connections={}, slot={}", value, currentSlot);
                    }
                }
                case TrainSampleJob.KEY_IN_DYN_CONFIG -> {
                    ArrayList<String> keyPrefixOrSuffixGroupList = new ArrayList<>(Arrays.asList(((String) value).split(",")));
                    TrainSampleJob.setKeyPrefixOrSuffixGroupList(keyPrefixOrSuffixGroupList);
                    log.warn("Dyn config for global set dict_key_prefix_groups={}, slot={}", value, currentSlot);
                }
                case BigKeyTopK.KEY_IN_DYN_CONFIG -> {
                    oneSlot.initBigKeyTopK(Integer.parseInt(value.toString()));
                    log.warn("Global config for current slot set monitor_big_key_top_k={}, slot={}", value, currentSlot);
                }
                case "type_zset_member_max_length" -> {
                    RedisZSet.ZSET_MEMBER_MAX_LENGTH = Short.parseShort(value.toString());
                    log.warn("Dyn config for global set zset_member_max_length={}, slot={}", value, currentSlot);
                }
                case "type_set_member_max_length" -> {
                    RedisHashKeys.SET_MEMBER_MAX_LENGTH = Short.parseShort(value.toString());
                    log.warn("Dyn config for global set set_member_max_length={}, slot={}", value, currentSlot);
                }
                case "type_zset_max_size" -> {
                    RedisZSet.ZSET_MAX_SIZE = Short.parseShort(value.toString());
                    log.warn("Dyn config for global set zset_max_size={}, slot={}", value, currentSlot);
                }
                case "type_hash_max_size" -> {
                    RedisHashKeys.HASH_MAX_SIZE = Short.parseShort(value.toString());
                    log.warn("Dyn config for global set hash_max_size={}, slot={}", value, currentSlot);
                }
                case "type_list_max_size" -> {
                    RedisList.LIST_MAX_SIZE = Short.parseShort(value.toString());
                    log.warn("Dyn config for global set list_max_size={}, slot={}", value, currentSlot);
                }
            }
        }
    }

    private final AfterUpdateCallback afterUpdateCallback;

    /**
     * @return the AfterUpdateCallback instance
     */
    public AfterUpdateCallback getAfterUpdateCallback() {
        return afterUpdateCallback;
    }

    /**
     * @return the Master UUID, or null if not set
     */
    public Long getMasterUuid() {
        Object val = get("masterUuid");
        return val == null ? null : Long.valueOf(val.toString());
    }

    /**
     * @param masterUuid the UUID to set as the Master
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void setMasterUuid(long masterUuid) throws IOException {
        update("masterUuid", masterUuid);
    }

    /**
     * @return true if the slot is read-only, false otherwise
     */
    public boolean isReadonly() {
        var obj = get("readonly");
        return obj != null && (boolean) obj;
    }

    /**
     * @param readonly the read-only status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void setReadonly(boolean readonly) throws IOException {
        update("readonly", readonly);
    }

    /**
     * @return true if the slot is readable, false otherwise
     */
    public boolean isCanRead() {
        var obj = get("canRead");
        return obj == null || (boolean) obj;
    }

    /**
     * @param canRead the readability status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void setCanRead(boolean canRead) throws IOException {
        update("canRead", canRead);
    }

    /**
     * @return true if the slot is writable, false otherwise
     */
    public boolean isCanWrite() {
        var obj = get("canWrite");
        return obj == null || (boolean) obj;
    }

    /**
     * @param canWrite the writability status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void setCanWrite(boolean canWrite) throws IOException {
        update("canWrite", canWrite);
    }

    /**
     * @return the value of testKey, defaulting to 10 if not set
     */
    @TestOnly
    public int getTestKey() {
        var obj = get("testKey");
        return obj == null ? 10 : (int) obj;
    }

    /**
     * @param testValueInt the value to set for testKey
     * @throws IOException If an error occurs while writing to the configuration file
     */
    @TestOnly
    public void setTestKey(int testValueInt) throws IOException {
        update("testKey", testValueInt);
    }

    /**
     * @return true if the binlog is on, false otherwise
     */
    public boolean isBinlogOn() {
        var obj = get("binlogOn");
        return obj != null && (boolean) obj;
    }

    /**
     * @param binlogOn the binlog status to set
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void setBinlogOn(boolean binlogOn) throws IOException {
        update("binlogOn", binlogOn);
    }

    /**
     * @param slot slot index
     * @param dynConfigFile JSON file where the dynamic configuration is stored
     * @param oneSlot OneSlot instance associated with this configuration
     * @throws IOException If an error occurs while reading from or writing to the configuration file
     */
    public DynConfig(short slot, @NotNull File dynConfigFile, @NotNull OneSlot oneSlot) throws IOException {
        this.slot = slot;
        this.dynConfigFile = dynConfigFile;
        this.oneSlot = oneSlot;
        this.afterUpdateCallback = new AfterUpdateCallbackInner(slot, oneSlot);

        if (!dynConfigFile.exists()) {
            FileUtils.touch(dynConfigFile);
            FileUtils.writeByteArrayToFile(dynConfigFile, "{}".getBytes());
            this.data = new HashMap<>();
            log.info("Init dyn config, data={}, slot={}", data, slot);
        } else {
            var objectMapper = new ObjectMapper();
            this.data = objectMapper.readValue(dynConfigFile, HashMap.class);
            log.info("Init dyn config, data={}, slot={}", data, slot);

            for (var entry : data.entrySet()) {
                var value = parseAndValidate(entry.getKey(), entry.getValue());
                entry.setValue(value);
                applyUpdate(entry.getKey(), value);
            }
        }
    }

    private static Object parseAndValidate(@NotNull String key, @NotNull Object value) {
        var item = SUPPORTED_ITEMS.get(key);
        if (item != null) {
            return item.parseAndValidate(value);
        }
        if (INTERNAL_KEYS.contains(key)) {
            return value;
        }
        throw new IllegalArgumentException("Unsupported dyn config key: " + key);
    }

    private void applyUpdate(@NotNull String key, @NotNull Object value) {
        var item = SUPPORTED_ITEMS.get(key);
        if (item != null) {
            item.apply(slot, oneSlot, value);
        } else {
            afterUpdateCallback.afterUpdate(key, value);
        }
    }

    /**
     * @param key the key to update
     * @param value the new value for the key
     * @throws IOException If an error occurs while writing to the configuration file
     */
    public void update(@NotNull String key, @NotNull Object value) throws IOException {
        var valueToUpdate = parseAndValidate(key, value);
        data.put(key, valueToUpdate);
        var objectMapper = new ObjectMapper();
        objectMapper.writeValue(dynConfigFile, data);
        log.info("Update dyn config, key={}, value={}, slot={}", key, valueToUpdate, slot);

        applyUpdate(key, valueToUpdate);
    }

    /**
     * @param key the key to retrieve
     * @param defaultValue the default value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    public long getLongValue(@NotNull String key, long defaultValue) {
        var val = get(key);
        return val == null ? defaultValue : Long.parseLong(val.toString());
    }
}
