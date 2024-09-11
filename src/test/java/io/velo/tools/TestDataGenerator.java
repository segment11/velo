package io.velo.tools;

import io.velo.ConfForSlot;

import java.io.File;
import java.io.IOException;

public interface TestDataGenerator {
    default short slot() {
        return (short) 0;
    }

    default short slotNumber() {
        return 1;
    }

    default File persistDir() {
        return new File("/tmp/velo-data-test-data");
    }

    default File slotDir() {
        return new File(persistDir(), "slot-" + slot());
    }

    default ConfForSlot confForSlot() {
        return ConfForSlot.c1m;
    }

    default void prepare() {
        var slotDir = slotDir();
        if (slotDir.exists()) {
            throw new IllegalStateException("slot dir exists=" + slotDir);
        }
    }

    void generate() throws IOException;
}
