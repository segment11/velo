package io.velo.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.velo.MultiWorkerServer;
import io.velo.TrainSampleJob;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class DynConfig {
    private final Logger log = LoggerFactory.getLogger(DynConfig.class);
    private final short slot;
    private final File dynConfigFile;

    private final HashMap<String, Object> data;

    private Object get(String key) {
        return data.get(key);
    }

    public interface AfterUpdateCallback {
        void afterUpdate(String key, Object value);
    }

    private class AfterUpdateCallbackInner implements AfterUpdateCallback {
        private final short currentSlot;

        public AfterUpdateCallbackInner(short currentSlot) {
            this.currentSlot = currentSlot;
        }

        @Override
        public void afterUpdate(String key, Object value) {
            if ("max_connections".equals(key)) {
                MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections((int) value);
                log.warn("Global config set max_connections={}, slot: {}", value, currentSlot);
            }

            if ("dict_key_prefix_groups".equals(key)) {
                var keyPrefixGroups = (String) value;
                ArrayList<String> keyPrefixGroupList = new ArrayList<>();
                for (var keyPrefixGroup : keyPrefixGroups.split(",")) {
                    keyPrefixGroupList.add(keyPrefixGroup);
                }

                TrainSampleJob.setKeyPrefixOrSuffixGroupList(keyPrefixGroupList);
                log.warn("Global config set dict_key_prefix_groups={}, slot: {}", value, currentSlot);
            }
            // todo
        }
    }

    private final AfterUpdateCallback afterUpdateCallback;

    public AfterUpdateCallback getAfterUpdateCallback() {
        return afterUpdateCallback;
    }

    Long getMasterUuid() {
        Object val = get("masterUuid");
        return val == null ? null : Long.valueOf(val.toString());
    }

    void setMasterUuid(long masterUuid) throws IOException {
        update("masterUuid", masterUuid);
    }

    // add other config items get/set here

    boolean isReadonly() {
        var obj = get("readonly");
        return obj != null && (boolean) obj;
    }

    void setReadonly(boolean readonly) throws IOException {
        update("readonly", readonly);
    }

    boolean isCanRead() {
        var obj = get("canRead");
        return obj == null || (boolean) obj;
    }

    void setCanRead(boolean canRead) throws IOException {
        update("canRead", canRead);
    }

    boolean isCanWrite() {
        var obj = get("canWrite");
        return obj == null || (boolean) obj;
    }

    void setCanWrite(boolean canWrite) throws IOException {
        update("canWrite", canWrite);
    }

    @TestOnly
    int getTestKey() {
        var obj = get("testKey");
        return obj == null ? 10 : (int) obj;
    }

    @TestOnly
    void setTestKey(int testValueInt) throws IOException {
        update("testKey", testValueInt);
    }

    public boolean isBinlogOn() {
        var obj = get("binlogOn");
        return obj != null && (boolean) obj;
    }

    public void setBinlogOn(boolean binlogOn) throws IOException {
        update("binlogOn", binlogOn);
    }

    public DynConfig(short slot, File dynConfigFile) throws IOException {
        this.slot = slot;
        this.dynConfigFile = dynConfigFile;
        this.afterUpdateCallback = new AfterUpdateCallbackInner(slot);

        if (!dynConfigFile.exists()) {
            FileUtils.touch(dynConfigFile);
            FileUtils.writeByteArrayToFile(dynConfigFile, "{}".getBytes());

            this.data = new HashMap<>();
            log.info("Init dyn config, data: {}, slot: {}", data, slot);
        } else {
            // read json
            var objectMapper = new ObjectMapper();
            this.data = objectMapper.readValue(dynConfigFile, HashMap.class);
            log.info("Init dyn config, data: {}, slot: {}", data, slot);

            for (var entry : data.entrySet()) {
                afterUpdateCallback.afterUpdate(entry.getKey(), entry.getValue());
            }
        }
    }

    public void update(String key, Object value) throws IOException {
        data.put(key, value);
        // write json
        var objectMapper = new ObjectMapper();
        objectMapper.writeValue(dynConfigFile, data);
        log.info("Update dyn config, key: {}, value: {}, slot: {}", key, value, slot);

        afterUpdateCallback.afterUpdate(key, value);
    }
}
