package io.velo.extend;

import com.google.protobuf.InvalidProtocolBufferException;
import io.velo.KeyHash;
import io.velo.metric.InSlotMetricCollector;
import io.velo.persist.CanSaveAndLoad;
import io.velo.persist.InMemoryEstimate;
import io.velo.persist.index.BetaExtend;
import io.velo.proto.CountListProto;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@BetaExtend
public class CountService implements InMemoryEstimate, InSlotMetricCollector, CanSaveAndLoad {

    private final int initBytesArraySize;

    @VisibleForTesting
    static final int MAX_INIT_BYTES_ARRAY_SIZE = 1024 * 1024;

    private final byte[][] countEncoded;

    private final double[] encodedCompressRatio;

    // compress ratio ~= 0.86, but encode / decode cost too much, not worth it
    private final boolean useCompress;

    public CountService(int initBytesArraySize, boolean useCompress) {
        if (initBytesArraySize <= 0 || initBytesArraySize > MAX_INIT_BYTES_ARRAY_SIZE) {
            throw new IllegalArgumentException("Count service, init bytes array size should be in (0, " + MAX_INIT_BYTES_ARRAY_SIZE + "]");
        }

        if (initBytesArraySize % 1024 != 0) {
            throw new IllegalArgumentException("Count service, init bytes array size should be multiple of 1024");
        }

        this.initBytesArraySize = initBytesArraySize;
        this.countEncoded = new byte[initBytesArraySize][];
        this.useCompress = useCompress;

        if (useCompress) {
            this.encodedCompressRatio = new double[initBytesArraySize];
        } else {
            this.encodedCompressRatio = new double[1];
        }
    }

    public double getEncodedCompressRatio(int index) {
        if (!useCompress) {
            return 1d;
        }

        return encodedCompressRatio[index];
    }

    public int get(byte[] keyBytes) throws InvalidProtocolBufferException {
        var keyHash = KeyHash.hash(keyBytes);
        var keyHash32 = KeyHash.hash32(keyBytes);
        return get(keyHash, keyHash32);
    }

    public int get(long keyHash, int keyHash32) throws InvalidProtocolBufferException {
        var index = (int) Math.abs(keyHash % initBytesArraySize);
        var bytes = countEncoded[index];
        if (bytes == null) {
            return 0;
        }

        if (!useCompress) {
            var buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                var keyHash32InBytes = buffer.getInt();
                var count = buffer.getInt();
                if (keyHash32InBytes == keyHash32) {
                    return count;
                }
            }
            return 0;
        }

        // decode
        var countList = CountListProto.CountList.parseFrom(bytes);
        for (int i = 0; i < countList.getKeyHash32AndCountCount(); i += 2) {
            if (countList.getKeyHash32AndCount(i) == keyHash32) {
                return countList.getKeyHash32AndCount(i + 1);
            }
        }
        return 0;
    }

    private void updatedEncodedBytes(int index, CountListProto.CountList countList) {
        var encodedBytes = countList.toByteArray();
        countEncoded[index] = encodedBytes;

        // 4 bytes for each key hash 32 and count
        encodedCompressRatio[index] = (double) encodedBytes.length / (countList.getKeyHash32AndCountCount() * 4);
    }

    private static final int FIRST_ADD_INIT_BYTES_SIZE = 64;

    private int addToBytes(byte[] bytes, int index, int keyHash32, int added) {
        var buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            var keyHash32InBytes = buffer.getInt();
            var count = buffer.getInt();
            if (keyHash32InBytes == 0) {
                buffer.putInt(buffer.position() - 8, keyHash32);
                buffer.putInt(buffer.position() - 4, added);
                return added;
            } else if (keyHash32InBytes == keyHash32) {
                count += added;
                buffer.putInt(buffer.position() - 4, count);
                return count;
            }
        }

        // expand and add
        var bytesNew = new byte[bytes.length * 2];
        System.arraycopy(bytes, 0, bytesNew, 0, bytes.length);
        countEncoded[index] = bytesNew;

        var bufferNew = ByteBuffer.wrap(bytesNew);
        bufferNew.position(bytes.length);
        bufferNew.putInt(keyHash32);
        bufferNew.putInt(added);
        return added;
    }

    public int increase(byte[] keyBytes, int added) throws InvalidProtocolBufferException {
        var keyHash = KeyHash.hash(keyBytes);
        var keyHash32 = KeyHash.hash32(keyBytes);
        return increase(keyHash, keyHash32, added);
    }

    public int increase(long keyHash, int keyHash32, int added) throws InvalidProtocolBufferException {
        var index = (int) Math.abs(keyHash % initBytesArraySize);
        var bytes = countEncoded[index];

        if (!useCompress) {
            if (bytes == null) {
                var bytesInit = new byte[FIRST_ADD_INIT_BYTES_SIZE];
                countEncoded[index] = bytesInit;
                return addToBytes(bytesInit, index, keyHash32, added);
            } else {
                return addToBytes(bytes, index, keyHash32, added);
            }
        }

        if (bytes == null) {
            // add to empty
            var countList = CountListProto.CountList.newBuilder()
                    .addKeyHash32AndCount(keyHash32)
                    .addKeyHash32AndCount(added)
                    .build();
            updatedEncodedBytes(index, countList);
            return added;
        } else {
            // decode, iterate and update
            var countList = CountListProto.CountList.parseFrom(bytes);
            for (int i = 0; i < countList.getKeyHash32AndCountCount(); i += 2) {
                if (countList.getKeyHash32AndCount(i) == keyHash32) {
                    var count = countList.getKeyHash32AndCount(i + 1);

                    var countNewList = countList.toBuilder()
                            .setKeyHash32AndCount(i + 1, count + added)
                            .build();
                    updatedEncodedBytes(index, countNewList);
                    return count + added;
                }
            }

            // add
            var countNewList = countList.toBuilder()
                    .addKeyHash32AndCount(keyHash32)
                    .addKeyHash32AndCount(added)
                    .build();
            updatedEncodedBytes(index, countNewList);
            return added;
        }
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long s = 0;
        for (byte[] bytes : countEncoded) {
            if (bytes != null) {
                s += bytes.length;
            }
        }
        return s;
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();
        map.put("bytes_array_length", (double) countEncoded.length);
        map.put("estimated_bytes_in_KB", (double) (estimate(new StringBuilder()) / 1024));
        return map;
    }

    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        var bytesArrayLength = is.readInt();
        for (int i = 0; i < bytesArrayLength; i++) {
            var bytesLength = is.readInt();
            if (bytesLength == 0) {
                continue;
            }
            var bytes = new byte[bytesLength];
            is.readFully(bytes);
            countEncoded[i] = bytes;
        }
    }

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        os.writeInt(countEncoded.length);
        for (byte[] bytes : countEncoded) {
            if (bytes == null) {
                os.writeInt(0);
                continue;
            }
            os.writeInt(bytes.length);
            os.write(bytes);
        }
    }

    private File persistDir;

    public File getPersistDir() {
        return persistDir;
    }

    public void setPersistDir(File persistDir) {
        this.persistDir = persistDir;
    }

    private byte slot;

    public byte getSlot() {
        return slot;
    }

    public void setSlot(byte slot) {
        this.slot = slot;
    }

    @VisibleForTesting
    static final String SAVE_FILE_NAME_PREFIX = "count_service_slot_";

    private static final Logger log = LoggerFactory.getLogger(CountService.class);

    public void loadFromLastSavedFile() throws IOException {
        var saveFileName = SAVE_FILE_NAME_PREFIX + slot + ".dat";
        var lastSavedFile = new File(persistDir, saveFileName);
        if (!lastSavedFile.exists()) {
            return;
        }

        var fileLength = lastSavedFile.length();
        if (fileLength == 0) {
            return;
        }

        try (var is = new DataInputStream(new FileInputStream(lastSavedFile))) {
            var beginT = System.currentTimeMillis();
            loadFromLastSavedFileWhenPureMemory(is);
            var costT = System.currentTimeMillis() - beginT;
            log.info("Count service, load from last saved file, slot={}, cost={} ms, file length={} KB",
                    slot, costT, fileLength / 1024);
        }
    }

    public void writeToSavedFile() throws IOException {
        var saveFileName = SAVE_FILE_NAME_PREFIX + slot + ".dat";
        var lastSavedFile = new File(persistDir, saveFileName);
        if (!lastSavedFile.exists()) {
            FileUtils.touch(lastSavedFile);
        }

        try (var os = new DataOutputStream(new FileOutputStream(lastSavedFile))) {
            var beginT = System.currentTimeMillis();
            writeToSavedFileWhenPureMemory(os);
            var costT = System.currentTimeMillis() - beginT;
            log.info("Count service, write to saved file, slot={}, cost={} ms", slot, costT);
        }

        var fileLength = lastSavedFile.length();
        log.info("Saved file length={} KB", fileLength / 1024);
    }

    CompletableFuture<Boolean> lazyReadFromFile() {
        var waitF = new CompletableFuture<Boolean>();
        // just run once
        new Thread(() -> {
            log.info("Start a single thread to read from file or load saved file, slot={}", slot);
            try {
                loadFromLastSavedFile();
                waitF.complete(true);
            } catch (IOException e) {
                log.error("Wal lazy read from file or load saved file error for slot=" + slot, e);
                waitF.completeExceptionally(e);
            } finally {
                log.info("End a single thread to read wal from file or load saved file, slot={}", slot);
            }
        }).start();
        return waitF;
    }
}
