package io.velo;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * A utility class to support loading and parsing configuration files for the Valkey system.
 * This class reads configuration settings from predefined configuration files and sets the corresponding fields.
 */
public class ValkeyRawConfSupport {
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ValkeyRawConfSupport() {
    }

    /**
     * The default filename for the Valkey configuration file.
     */
    public static final String VALKEY_CONF_FILENAME = "redis.conf";

    /**
     * An alternative filename for the Valkey configuration file.
     */
    public static final String VALKEY_CONF_FILENAME2 = "valkey.conf";

    /**
     * The filename for the Access Control List (ACL) configuration file.
     */
    public static String aclFilename = "acl.conf";

    /**
     * The default setting for whether ACLs should apply to Pub/Sub channels by default.
     */
    public static boolean aclPubsubDefault = true;

    /**
     * The priority setting for replicas.
     */
    public static int replicaPriority = 100;

    /**
     * Loads the configuration settings from the Valkey configuration file.
     * It checks both the default and alternative filenames and reads the settings.
     * Lines starting with "#" are considered comments and are ignored.
     * Key-value pairs are expected to be separated by a colon (":").
     * Supported keys include:
     * - acl-filename: Sets the filename for the Access Control List (ACL).
     * - acl-pubsub-default: Sets whether ACLs should apply to Pub/Sub channels by default.
     * - replica-priority: Sets the priority for replicas.
     *
     * @throws IOException If an error occurs while reading the configuration file.
     */
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