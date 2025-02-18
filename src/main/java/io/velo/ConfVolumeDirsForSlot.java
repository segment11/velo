package io.velo;

import io.activej.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Configuration for volume directories assigned to slots in the Velo application.
 * This class manages the mapping of slots to specific volume directories based on configuration settings.
 *
 * <p>Key features:
 * <ul>
 *   <li>Initialization from configuration</li>
 *   <li>Retrieval of volume directory for a specific slot</li>
 * </ul>
 */
public class ConfVolumeDirsForSlot {
    /**
     * Private constructor to prevent instantiation.
     */
    private ConfVolumeDirsForSlot() {
    }

    /**
     * Logger for logging messages.
     */
    private static final Logger log = LoggerFactory.getLogger(ConfVolumeDirsForSlot.class);

    /**
     * Array of volume directories indexed by slot number.
     */
    private static String[] volumeDirsBySlot;

    /**
     * Retrieves the volume directory for a specific slot.
     *
     * @param slot The slot number.
     * @return The volume directory for the given slot, or null if not configured.
     */
    public static String getVolumeDirBySlot(short slot) {
        if (volumeDirsBySlot == null) {
            return null;
        }
        return volumeDirsBySlot[slot];
    }

    /**
     * Initializes the volume directories from the provided configuration.
     * Example config: persist.volumeDirsBySlot=/mnt/data0:0-32,/mnt/data1:33-64,/mnt/data2:65-96,/mnt/data3:97-128
     * Means that the first 32 slots are stored in /mnt/data0, the next 32 in /mnt/data1, and so on.
     *
     * @param persistConfig The configuration object containing volume directory settings.
     * @param slotNumber    The total number of slots.
     * @throws IllegalArgumentException if the configuration is invalid.
     */
    public static void initFromConfig(Config persistConfig, short slotNumber) {
        volumeDirsBySlot = new String[slotNumber];

        if (persistConfig.getChild("volumeDirsBySlot").hasValue()) {
            var value = persistConfig.get("volumeDirsBySlot");
            var volumeDirs = value.split(",");
            for (var volumeDir : volumeDirs) {
                var parts = volumeDir.split(":");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config=" + value);
                }
                var dirs = parts[0];
                var dirFile = new File(dirs);
                if (!dirFile.exists() || !dirFile.isDirectory()) {
                    throw new IllegalArgumentException("Invalid dir path=" + dirs);
                }

                var range = parts[1].split("-");
                if (range.length != 2) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config=" + value);
                }
                var start = Integer.parseInt(range[0]);
                var end = Integer.parseInt(range[1]);
                if (start > end || end >= slotNumber) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config=" + value);
                }
                for (int i = start; i <= end; i++) {
                    volumeDirsBySlot[i] = dirs;
                }
                log.warn("Slot range {}-{} is assigned to volume dirs {}", start, end, dirs);
            }
        }
    }
}
