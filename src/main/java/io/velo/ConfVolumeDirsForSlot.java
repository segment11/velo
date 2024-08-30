package io.velo;

import io.activej.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ConfVolumeDirsForSlot {
    private ConfVolumeDirsForSlot() {
    }

    private static final Logger log = LoggerFactory.getLogger(ConfVolumeDirsForSlot.class);

    private static String[] volumeDirsBySlot;

    public static String getVolumeDirBySlot(short slot) {
        return volumeDirsBySlot[slot];
    }

    public static void initFromConfig(Config persistConfig, short slotNumber) {
        volumeDirsBySlot = new String[slotNumber];

        // eg. persist.volumeDirsBySlot=/mnt/data0:0-32,/mnt/data1:33-64,/mnt/data2:65-96,/mnt/data3:97-128
        if (persistConfig.getChild("volumeDirsBySlot").hasValue()) {
            var value = persistConfig.get("volumeDirsBySlot");
            var volumeDirs = value.split(",");
            for (var volumeDir : volumeDirs) {
                var parts = volumeDir.split(":");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config: " + value);
                }
                var dirs = parts[0];
                var dirFile = new File(dirs);
                if (!dirFile.exists() || !dirFile.isDirectory()) {
                    throw new IllegalArgumentException("Invalid dir path: " + dirs);
                }

                var range = parts[1].split("-");
                if (range.length != 2) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config: " + value);
                }
                var start = Integer.parseInt(range[0]);
                var end = Integer.parseInt(range[1]);
                if (start > end || end >= slotNumber) {
                    throw new IllegalArgumentException("Invalid volumeDirsBySlot config: " + value);
                }
                for (int i = start; i <= end; i++) {
                    volumeDirsBySlot[i] = dirs;
                }
                log.warn("Slot range {}-{} is assigned to volume dirs {}", start, end, dirs);
            }
        }
    }
}
