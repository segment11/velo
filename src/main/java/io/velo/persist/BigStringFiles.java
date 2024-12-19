package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigStringFiles implements InMemoryEstimate, InSlotMetricCollector, CanSaveAndLoad, HandlerWhenCvExpiredOrDeleted {
    private final short slot;
    final File bigStringDir;

    private static final String BIG_STRING_DIR_NAME = "big-string";

    public static final int SKIP_UUID = -1;

    private final LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    private final HashMap<Long, byte[]> allBytesByUuid = new HashMap<>();

    private int bigStringFilesCount = 0;

    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();
        map.put("big_string_files_count", (double) bigStringFilesCount);
        return map;
    }

    private static final Logger log = LoggerFactory.getLogger(BigStringFiles.class);

    public BigStringFiles(short slot, File slotDir) throws IOException {
        this.slot = slot;
        if (ConfForGlobal.pureMemory) {
            log.warn("Pure memory mode, big string files will not be used, slot={}", slot);
            this.bigStringDir = null;
            this.bigStringBytesByUuidLRU = null;
            return;
        }

        this.bigStringDir = new File(slotDir, BIG_STRING_DIR_NAME);
        if (!bigStringDir.exists()) {
            if (!bigStringDir.mkdirs()) {
                throw new IOException("Create big string dir error, slot=" + slot);
            }
        }

        var maxSize = ConfForSlot.global.lruBigString.maxSize;
        final var maybeOneBigStringBytesLength = 4096;
        var lruMemoryRequireMB = maxSize * maybeOneBigStringBytesLength / 1024 / 1024;
        log.info("LRU max size for big string={}, maybe one big string bytes length is {}B, memory require={}MB, slot={}",
                maxSize,
                maybeOneBigStringBytesLength,
                lruMemoryRequireMB,
                slot);
        log.info("LRU prepare, type={}, MB={}, slot={}", LRUPrepareBytesStats.Type.big_string, lruMemoryRequireMB, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.big_string, String.valueOf(slot), lruMemoryRequireMB, false);

        this.bigStringBytesByUuidLRU = new LRUMap<>(maxSize);

        var files = bigStringDir.listFiles();
        bigStringFilesCount = files != null ? files.length : 0;
    }

    @Override
    public long estimate(StringBuilder sb) {
        if (ConfForGlobal.pureMemory) {
            return 0;
        }

        long size = RamUsageEstimator.sizeOfMap(bigStringBytesByUuidLRU);
        sb.append("Big string files: ").append(size).append("\n");
        return size;
    }

    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        // count int
        bigStringFilesCount = is.readInt();
        for (int i = 0; i < bigStringFilesCount; i++) {
            // uuid long, bytes length int
            var uuid = is.readLong();
            var bytesLength = is.readInt();
            var bytes = new byte[bytesLength];
            is.readFully(bytes);
            allBytesByUuid.put(uuid, bytes);
        }
    }

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.writeInt(allBytesByUuid.size());
        for (var entry : allBytesByUuid.entrySet()) {
            os.writeLong(entry.getKey());
            var bytes = entry.getValue();
            os.writeInt(bytes.length);
            os.write(bytes);
        }
    }

    public List<Long> getBigStringFileUuidList() {
        var list = new ArrayList<Long>();
        if (ConfForGlobal.pureMemory) {
            for (var entry : allBytesByUuid.entrySet()) {
                list.add(entry.getKey());
            }
            return list;
        }

        var files = bigStringDir.listFiles();
        if (files == null) {
            return list;
        }

        for (File file : files) {
            list.add(Long.parseLong(file.getName()));
        }
        return list;
    }

    public byte[] getBigStringBytes(long uuid) {
        return getBigStringBytes(uuid, false);
    }

    public byte[] getBigStringBytes(long uuid, boolean doLRUCache) {
        if (ConfForGlobal.pureMemory) {
            return allBytesByUuid.get(uuid);
        }

        var bytesCached = bigStringBytesByUuidLRU.get(uuid);
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid);
        if (bytes != null && doLRUCache) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
        }
        return bytes;
    }

    private byte[] readBigStringBytes(long uuid) {
        var file = new File(bigStringDir, String.valueOf(uuid));
        if (!file.exists()) {
            log.warn("Big string file not exists, uuid={}, slot={}", uuid, slot);
            return null;
        }

        try {
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            log.error("Read big string file error, uuid={}, slot={}", uuid, slot, e);
            return null;
        }
    }

    public boolean writeBigStringBytes(long uuid, String key, byte[] bytes) {
        if (ConfForGlobal.pureMemory) {
            allBytesByUuid.put(uuid, bytes);
            return true;
        }

        var file = new File(bigStringDir, String.valueOf(uuid));
        try {
            FileUtils.writeByteArrayToFile(file, bytes);
            bigStringFilesCount++;
            return true;
        } catch (IOException e) {
            log.error("Write big string file error, uuid={}, key={}, slot={}", uuid, key, slot, e);
            return false;
        }
    }

    public boolean deleteBigStringFileIfExist(long uuid) {
        if (ConfForGlobal.pureMemory) {
            var r = allBytesByUuid.remove(uuid);
            if (r != null) {
                bigStringFilesCount--;
            }
            return true;
        }

        bigStringBytesByUuidLRU.remove(uuid);

        var file = new File(bigStringDir, String.valueOf(uuid));
        if (file.exists()) {
            bigStringFilesCount--;
            return file.delete();
        } else {
            return true;
        }
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void deleteAllBigStringFiles() {
        if (ConfForGlobal.pureMemory) {
            allBytesByUuid.clear();
            return;
        }

        bigStringBytesByUuidLRU.clear();

        try {
            FileUtils.cleanDirectory(bigStringDir);
            log.warn("Delete all big string files, slot={}", slot);
            bigStringFilesCount = 0;
        } catch (IOException e) {
            log.error("Delete all big string files error, slot={}", slot, e);
        }
    }

    @Override
    public void handleWhenCvExpiredOrDeleted(String key, CompressedValue shortStringCv, PersistValueMeta pvm) {
        if (shortStringCv == null) {
            return;
        }

        if (!shortStringCv.isBigString()) {
            return;
        }

        var uuid = shortStringCv.getBigStringMetaUuid();
        var isDeleted = deleteBigStringFileIfExist(uuid);
        if (!isDeleted) {
            throw new RuntimeException("Delete big string file error, s=" + slot + ", key=" + key + ", uuid=" + uuid);
        } else {
            log.warn("Delete big string file, s={}, key={}, uuid={}", slot, key, uuid);
        }
    }
}
