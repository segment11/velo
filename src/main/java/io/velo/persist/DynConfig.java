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

/**
 * This class manages dynamic configuration settings for a Velo slot.
 * It reads and writes configuration from/to a JSON file and triggers callbacks
 * when certain configuration values are updated.
 */
public class DynConfig {
    private static final Logger log = LoggerFactory.getLogger(DynConfig.class);

    private final short slot;
    private final File dynConfigFile;

    private final HashMap<String, Object> data;

    /**
     * Retrieves the value associated with a given key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with the specified key, or null if the key is not present
     */
    public Object get(@NotNull String key) {
        return data.get(key);
    }

    /**
     * Interface for callbacks to be executed after a dynamic configuration update.
     */
    public interface AfterUpdateCallback {
        /**
         * Called after a dynamic configuration update.
         *
         * @param key   the key that was updated
         * @param value the new value for the key
         */
        void afterUpdate(@NotNull String key, @NotNull Object value);
    }

    /**
     * Inner implementation of the AfterUpdateCallback interface.
     * It handles specific actions for some configuration keys.
     */
    private static class AfterUpdateCallbackInner implements AfterUpdateCallback {
        private final short currentSlot;
        private final OneSlot oneSlot;

        /**
         * Constructs the AfterUpdateCallbackInner.
         *
         * @param currentSlot the slot index corresponding to this dynamic configuration
         * @param oneSlot     the OneSlot instance associated with this dynamic configuration
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
                    MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections((int) value);
                    log.warn("Dyn config for global set max_connections={}, slot={}", value, currentSlot);
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
     * Returns the after-update callback associated with this dynamic configuration.
     *
     * @return the AfterUpdateCallback instance
     */
    public AfterUpdateCallback getAfterUpdateCallback() {
        return afterUpdateCallback;
    }

    /**
     * Retrieves the Master UUID from the configuration.
     *
     * @return the Master UUID, or null if not set
     */
    public Long getMasterUuid() {
        Object val = get("masterUuid");
        return val == null ? null : Long.valueOf(val.toString());
    }

    /**
     * Sets the Master UUID in the configuration.
     *
     * @param masterUuid the UUID to set as the Master
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void setMasterUuid(long masterUuid) throws IOException {
        update("masterUuid", masterUuid);
    }

    /**
     * Checks if the slot is read-only.
     *
     * @return true if the slot is read-only, false otherwise
     */
    public boolean isReadonly() {
        var obj = get("readonly");
        return obj != null && (boolean) obj;
    }

    /**
     * Sets the read-only status of the slot.
     *
     * @param readonly the read-only status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void setReadonly(boolean readonly) throws IOException {
        update("readonly", readonly);
    }

    /**
     * Checks if the slot is readable.
     *
     * @return true if the slot is readable, false otherwise
     */
    public boolean isCanRead() {
        var obj = get("canRead");
        return obj == null || (boolean) obj;
    }

    /**
     * Sets the readability status of the slot.
     *
     * @param canRead the readability status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void setCanRead(boolean canRead) throws IOException {
        update("canRead", canRead);
    }

    /**
     * Checks if the slot is writable.
     *
     * @return true if the slot is writable, false otherwise
     */
    public boolean isCanWrite() {
        var obj = get("canWrite");
        return obj == null || (boolean) obj;
    }

    /**
     * Sets the writability status of the slot.
     *
     * @param canWrite the writability status to set for the slot
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void setCanWrite(boolean canWrite) throws IOException {
        update("canWrite", canWrite);
    }

    /**
     * Retrieves the testKey from the configuration.
     * This is a testing method marked with @TestOnly.
     *
     * @return the value of testKey, defaulting to 10 if not set
     */
    @TestOnly
    public int getTestKey() {
        var obj = get("testKey");
        return obj == null ? 10 : (int) obj;
    }

    /**
     * Sets the testKey in the configuration.
     * This is a testing method marked with @TestOnly.
     *
     * @param testValueInt the value to set for testKey
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    @TestOnly
    public void setTestKey(int testValueInt) throws IOException {
        update("testKey", testValueInt);
    }

    /**
     * Checks if the binlog is on.
     *
     * @return true if the binlog is on, false otherwise
     */
    public boolean isBinlogOn() {
        var obj = get("binlogOn");
        return obj != null && (boolean) obj;
    }

    /**
     * Sets whether the binlog is on.
     *
     * @param binlogOn the binlog status to set
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void setBinlogOn(boolean binlogOn) throws IOException {
        update("binlogOn", binlogOn);
    }

    /**
     * Constructs a DynConfig instance for a specific slot.
     *
     * @param slot          the slot index
     * @param dynConfigFile the JSON file where the dynamic configuration is stored
     * @param oneSlot       the OneSlot instance associated with this configuration
     * @throws IOException If an error occurs while reading from or writing to the configuration file.
     */
    public DynConfig(short slot, @NotNull File dynConfigFile, @NotNull OneSlot oneSlot) throws IOException {
        this.slot = slot;
        this.dynConfigFile = dynConfigFile;
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
                afterUpdateCallback.afterUpdate(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Updates a configuration value and writes the updated configuration to the JSON file.
     *
     * @param key   the key to update
     * @param value the new value for the key
     * @throws IOException If an error occurs while writing to the configuration file.
     */
    public void update(@NotNull String key, @NotNull Object value) throws IOException {
        data.put(key, value);
        var objectMapper = new ObjectMapper();
        objectMapper.writeValue(dynConfigFile, data);
        log.info("Update dyn config, key={}, value={}, slot={}", key, value, slot);

        afterUpdateCallback.afterUpdate(key, value);
    }
}