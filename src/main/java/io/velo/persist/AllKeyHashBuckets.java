package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.NeedCleanUp;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;

// pure memory v2
// refer to faster
// https://github.com/microsoft/FASTER
public class AllKeyHashBuckets implements InMemoryEstimate, NeedCleanUp, CanSaveAndLoad {
    @VisibleForTesting
    static final long NO_RECORD_ID = -1;

    // index -> bucket index
    private final byte[][] allKeyHash32BitBytes;
    // record id long 64 bit + expire at 48 bit + rand int 16 bit
    private final byte[][] allRecordIdBytes;

    // 48 bit, enough for date timestamp
    // year 10889, date 10889:08:02
    @VisibleForTesting
    static final long MAX_EXPIRE_AT = Long.MAX_VALUE & 0xffffffffffffL;

    private static final Logger log = LoggerFactory.getLogger(AllKeyHashBuckets.class);

    public AllKeyHashBuckets(int bucketsPerSlot) {
        final int initCapacity;
        if (bucketsPerSlot <= 64 * 1024) {
            // cache line size * 2
            initCapacity = 128;
        } else if (bucketsPerSlot <= 256 * 1024) {
            initCapacity = 256;
        } else {
            initCapacity = 512;
        }

        this.allKeyHash32BitBytes = new byte[bucketsPerSlot][];
        this.allRecordIdBytes = new byte[bucketsPerSlot][];
        for (int i = 0; i < bucketsPerSlot; i++) {
            this.allKeyHash32BitBytes[i] = new byte[initCapacity];
            this.allRecordIdBytes[i] = new byte[initCapacity * 4];
        }
    }

    public record RecordIdWithExpireAtAndSeq(long recordId, long expireAt, long seq) {
    }

    public RecordIdWithExpireAtAndSeq get(int keyHash32, int bucketIndex) {
        var bytes = allKeyHash32BitBytes[bucketIndex];
        var bytesRecordId = allRecordIdBytes[bucketIndex];
        var byteBuffer = ByteBuffer.wrap(bytes);

        for (int i = 0; i < bytes.length; i += 4) {
            if (keyHash32 == byteBuffer.getInt(i)) {
                var buffer = ByteBuffer.wrap(bytesRecordId);
                var offset = i * 4;
                var recordId = buffer.getLong(offset);
                if (recordId == NO_RECORD_ID) {
                    return null;
                } else {
                    var l = buffer.getLong(offset + 8);
                    // high 48 bit is real expire at millisecond, 16 bit is random int as seq
                    var expireAt = l >>> 16;
                    var seq = l & 0xFFFF;
                    return new RecordIdWithExpireAtAndSeq(recordId, expireAt, seq);
                }
            }
        }

        return null;
    }

    public boolean remove(int keyHash32, int bucketIndex) {
        return put(keyHash32, bucketIndex, CompressedValue.EXPIRE_NOW, NO_RECORD_ID);
    }

    @TestOnly
    private final HashMap<Long, byte[]> testLocalValues = new HashMap<>();

    @TestOnly
    void putLocalValue(long recordId, byte[] valueBytes) {
        testLocalValues.put(recordId, valueBytes);
    }

    @TestOnly
    byte[] getLocalValue(long recordId) {
        return testLocalValues.get(recordId);
    }

    public short getKeyCountInBucketIndex(int bucketIndex) {
        var bytes = allKeyHash32BitBytes[bucketIndex];
        var byteBuffer = ByteBuffer.wrap(bytes);

        short n = 0;
        for (int i = 0; i < bytes.length; i += 4) {
            var targetKeyHash32 = byteBuffer.getInt(i);
            if (targetKeyHash32 != 0) {
                n++;
            }
        }
        return n;
    }

    public boolean put(int keyHash32, int bucketIndex, long expireAt, long recordId) {
        if (expireAt > MAX_EXPIRE_AT) {
            throw new IllegalArgumentException("Expire at is too large");
        }

        var l = expireAt << 16 | new Random().nextInt(65536) & 0xFFFF;

        var bytes = allKeyHash32BitBytes[bucketIndex];
        var bytesRecordId = allRecordIdBytes[bucketIndex];
        var byteBuffer = ByteBuffer.wrap(bytes);

        boolean isExists = false;

        boolean isPut = false;
        for (int i = 0; i < bytes.length; i += 4) {
            var targetKeyHash32 = byteBuffer.getInt(i);
            if (targetKeyHash32 == 0) {
                // blank, just insert
                byteBuffer.putInt(i, keyHash32);
                var buffer = ByteBuffer.wrap(bytesRecordId);
                var offset = i * 4;
                buffer.putLong(offset, recordId);
                buffer.putLong(offset + 8, l);
                isPut = true;
                break;
            } else if (keyHash32 == targetKeyHash32) {
                // update
                var buffer = ByteBuffer.wrap(bytesRecordId);
                var offset = i * 4;
                var oldRecordId = buffer.getLong(offset);
                isExists = oldRecordId != NO_RECORD_ID;

                buffer.putLong(offset, recordId);
                buffer.putLong(offset + 8, l);
                isPut = true;
                break;
            }
        }

        if (!isPut) {
            log.info("All key hash buckets expand, bucket index={}, from bytes length={}", bucketIndex, bytes.length);
            // extend capacity * 2
            var newBytes = new byte[bytes.length * 2];
            System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
            allKeyHash32BitBytes[bucketIndex] = newBytes;

            var newBytes2 = new byte[bytesRecordId.length * 2];
            System.arraycopy(bytesRecordId, 0, newBytes2, 0, bytesRecordId.length);
            allRecordIdBytes[bucketIndex] = newBytes2;

            ByteBuffer.wrap(newBytes).putInt(bytes.length, keyHash32);
            var buffer = ByteBuffer.wrap(newBytes2);
            buffer.putLong(bytesRecordId.length, recordId);
            buffer.putLong(bytesRecordId.length + 8, l);
        }

        return isExists;
    }

    @VisibleForTesting
    static @NotNull PersistValueMeta recordIdToPvm(long recordId) {
        // max segment index = 512 * 1024 * 64 - 1 < 2 ^ 25
        // max sub block index = 3 < 2 ^ 2
        // max segment length = 4 * 1024 * 16 = 2 ^ 16
        // 25 bit for segment index, 2 bit for sub block index, 18 bit for segment offset, 18 bit for length
        var pvm = new PersistValueMeta();
        pvm.segmentIndex = (int) (recordId >> (18 + 18 + 2));
        pvm.subBlockIndex = (byte) (recordId >> (18 + 18) & 0x3);
        pvm.segmentOffset = (int) (recordId >> 18) & 0x3FFFF;
        pvm.length = (int) (recordId & 0x3FFFF);
        return pvm;
    }

    @VisibleForTesting
    static long pvmToRecordId(@NotNull PersistValueMeta pvm) {
        return (((long) pvm.segmentIndex) << (18 + 18 + 2))
                | (((long) pvm.subBlockIndex) << (18 + 18))
                | (((long) pvm.segmentOffset & 0x3FFFF) << 18)
                | ((long) pvm.length & 0x3FFFF);
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        for (var bytes : allKeyHash32BitBytes) {
            size += bytes.length * 5L;
        }
        sb.append("All key hash buckets: ").append(size).append("\n");
        return size;
    }

    private void loadFromLastSavedFileWhenPureMemoryForBytesArray(byte[][] byteArray, @NotNull DataInputStream is) throws IOException {
        var bytesArrayLength = is.readInt();
        for (int i = 0; i < bytesArrayLength; i++) {
            var bytesLength = is.readInt();
            var bytes = new byte[bytesLength];
            is.readFully(bytes);
            byteArray[i] = bytes;
        }
    }

    @Override
    public void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException {
        loadFromLastSavedFileWhenPureMemoryForBytesArray(this.allKeyHash32BitBytes, is);
        loadFromLastSavedFileWhenPureMemoryForBytesArray(this.allRecordIdBytes, is);
    }

    private static void writeToSavedFileWhenPureMemoryForBytesArray(byte[][] bytesArray, @NotNull DataOutputStream os) throws IOException {
        os.writeInt(bytesArray.length);
        for (byte[] bytes : bytesArray) {
            os.writeInt(bytes.length);
            os.write(bytes);
        }
    }

    @Override
    public void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException {
        writeToSavedFileWhenPureMemoryForBytesArray(this.allKeyHash32BitBytes, os);
        writeToSavedFileWhenPureMemoryForBytesArray(this.allRecordIdBytes, os);
    }

    @Override
    public void cleanUp() {
        log.warn("Clean up all key hash buckets, estimate in memory={}KB", estimate(new StringBuilder()) / 1024);
    }
}
