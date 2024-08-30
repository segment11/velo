package io.velo.jmh;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileInit {
    // 2GB file has 512K pages
    static final int PAGE_NUMBER = 1024 * 1024 / 2;

    static final int PAGE_SIZE = 4096;

    static final int ONE_BATCH_SIZE = 1024 * 1024 * 2;

    static final int PAGE_NUMBER_ONE_BATCH = ONE_BATCH_SIZE / PAGE_SIZE;

    static void append2GBFile(File file, boolean isOverwrite) throws IOException {
        if (file.exists()) {
            if (isOverwrite) {
                System.out.println("file exists, delete and recreate");
                file.delete();
            } else {
                System.out.println("file exists, skip");
                return;
            }
        }

        // once append 2M
        final byte[] oneBatchBytes = new byte[ONE_BATCH_SIZE];
        final int batches = 1024;

        // init each page first 4 bytes int
        var buffer = ByteBuffer.wrap(oneBatchBytes);
        for (int i = 0; i < PAGE_NUMBER_ONE_BATCH; i++) {
            buffer.position(i * PAGE_SIZE).putInt(i);
        }

        for (int j = 0; j < batches; j++) {
            FileUtils.writeByteArrayToFile(file, oneBatchBytes, true);
        }
        System.out.println("init write done, size: " + file.length() / 1024 / 1024 + " MB");
    }

}
