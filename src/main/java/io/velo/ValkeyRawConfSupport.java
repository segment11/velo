package io.velo;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Paths;

public class ValkeyRawConfSupport {
    private ValkeyRawConfSupport() {
    }

    public static final String VALKEY_CONF_FILENAME = "redis.conf";
    public static final String VALKEY_CONF_FILENAME2 = "valkey.conf";

    public static String aclFilename = "acl.conf";
    public static boolean aclPubsubDefault = true;
    public static int replicaPriority = 100;

    public static void load() throws IOException {
        var file = Paths.get(VALKEY_CONF_FILENAME).toFile();
        if (!file.exists()) {
            file = Paths.get(VALKEY_CONF_FILENAME2).toFile();
        }
        if (!file.exists()) {
            return;
        }

        // load valkey.conf
        var lines = FileUtils.readLines(file, "UTF-8");
        for (var line : lines) {
            if (line.startsWith("#")) {
                continue;
            }

            var kv = line.split(":");
            if (kv.length != 2) {
                continue;
            }

            var key = kv[0].trim();
            var value = kv[1].trim();
            if ("acl-filename".equals(key)) {
                aclFilename = value;
            } else if ("acl-pubsub-default".equals(key)) {
                aclPubsubDefault = Boolean.parseBoolean(value);
            } else if ("replica-priority".equals(key)) {
                replicaPriority = Integer.parseInt(value);
            }

            // other key-value pairs, todo
        }
    }
}
