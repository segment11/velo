package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

// pure memory v2
// refer to faster
// https://github.com/microsoft/FASTER
public class AllKeyHashBuckets implements InMemoryEstimate, NeedCleanUp, CanSaveAndLoad {
    @VisibleForTesting
    static final long NO_RECORD_ID = -1;

    // index -> bucket index
    private final byte[][] allKeyHash32BitBytesArray;
    // record id long 8 byte + (8 byte include expire at 48 bit + short type 8 bit) + seq long 8 byte
    private final byte[][] extendBytesArray;

    // 48 bit, enough for date timestamp
    // year 10889, date 10889:08:02
    @VisibleForTesting
    static final long MAX_EXPIRE_AT = Long.MAX_VALUE & 0xffffffffffffL;

    private static final Logger log = LoggerFactory.getLogger(AllKeyHashBuckets.class);

    public AllKeyHashBuckets(int bucketsPerSlot) {
        final int arrayLength;
        final int initCapacity;
        if (ConfForGlobal.pureMemoryV2) {
            if (bucketsPerSlot <= 64 * 1024) {
                // cache line size * 2
                initCapacity = 128;
            } else if (bucketsPerSlot <= 256 * 1024) {
                initCapacity = 256;
            } else {
                initCapacity = 512;
            }
            arrayLength = bucketsPerSlot;
        } else {
            // always init for easy test, cost less memory
            initCapacity = 64;
            arrayLength = 1024;
        }

        this.allKeyHash32BitBytesArray = new byte[arrayLength][];
        this.extendBytesArray = new byte[arrayLength][];
        for (int i = 0; i < arrayLength; i++) {
            this.allKeyHash32BitBytesArray[i] = new byte[initCapacity];
            this.extendBytesArray[i] = new byte[initCapacity * 6];
        }
    }

    // for repl
    public byte[][] getRecordsBytesArrayByWalGroupIndex(int walGroupIndex) {
        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        var recordXBytesArray = new byte[oneChargeBucketNumber][];

        var beginBucketIndex = oneChargeBucketNumber * walGroupIndex;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            var bucketIndex = i + beginBucketIndex;

            var bytes = allKeyHash32BitBytesArray[bucketIndex];
            var buffer = ByteBuffer.wrap(bytes);

            var extendBytes = extendBytesArray[bucketIndex];
            var extendBuffer = ByteBuffer.wrap(extendBytes);

            var bos = new ByteArrayOutputStream();
            try (var dataOs = new DataOutputStream(bos)) {
                dataOs.writeInt(bucketIndex);
                var recordCount = bytes.length / 4;
                dataOs.writeInt(recordCount);

                for (int j = 0; j < bytes.length; j += 4) {
                    var keyHash32 = buffer.getInt(j);

                    var offset = j * 6;
                    var recordId = extendBuffer.getLong(offset);
                    var l = extendBuffer.getLong(offset + 8);
                    var expireAt = l >>> 16;
                    var shortType = (byte) (l & 0xFF);

                    var seq = extendBuffer.getLong(offset + 16);

                    dataOs.writeInt(keyHash32);
                    dataOs.writeLong(expireAt);
                    dataOs.writeByte(shortType);
                    dataOs.writeLong(recordId);
                    dataOs.writeLong(seq);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            recordXBytesArray[i] = bos.toByteArray();
        }

        return recordXBytesArray;
    }

    public record RecordX(long recordId, long expireAt, byte shortType, long seq) {
        PersistValueMeta toPvm() {
            var pvm = recordIdToPvm(recordId);
            pvm.shortType = shortType;
            pvm.seq = seq;
            return pvm;
        }
    }

    public RecordX get(int keyHash32, int bucketIndex) {
        var bytes = allKeyHash32BitBytesArray[bucketIndex];
        var extendBytes = extendBytesArray[bucketIndex];
        var buffer = ByteBuffer.wrap(bytes);

        for (int i = 0; i < bytes.length; i += 4) {
            if (keyHash32 == buffer.getInt(i)) {
                var extendBuffer = ByteBuffer.wrap(extendBytes);
                var offset = i * 6;
                var recordId = extendBuffer.getLong(offset);
                if (recordId == NO_RECORD_ID) {
                    return null;
                } else {
                    var l = extendBuffer.getLong(offset + 8);
                    // high 48 bit is real expire at millisecond, 8 bit is short type
                    var expireAt = l >>> 16;
                    byte shortType = (byte) (l & 0xFF);
                    var seq = extendBuffer.getLong(offset + 16);
                    return new RecordX(recordId, expireAt, shortType, seq);
                }
            }
        }

        return null;
    }

    public boolean remove(int keyHash32, int bucketIndex) {
        return put(keyHash32, bucketIndex, CompressedValue.EXPIRE_NOW, 0L, KeyLoader.typeAsByteIgnore, NO_RECORD_ID);
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
        var bytes = allKeyHash32BitBytesArray[bucketIndex];
        var buffer = ByteBuffer.wrap(bytes);

        short n = 0;
        for (int i = 0; i < bytes.length; i += 4) {
            var targetKeyHash32 = buffer.getInt(i);
            if (targetKeyHash32 != 0) {
                n++;
            }
        }
        return n;
    }

    public boolean put(int keyHash32, int bucketIndex, long expireAt, long seq, byte shortType, long recordId) {
        if (expireAt > MAX_EXPIRE_AT) {
            throw new IllegalArgumentException("Expire at is too large");
        }

        var l = expireAt << 16 | shortType;

        var bytes = allKeyHash32BitBytesArray[bucketIndex];
        var extendBytes = extendBytesArray[bucketIndex];
        var buffer = ByteBuffer.wrap(bytes);

        boolean isExists = false;

        boolean isPut = false;
        for (int i = 0; i < bytes.length; i += 4) {
            var targetKeyHash32 = buffer.getInt(i);
            if (targetKeyHash32 == 0) {
                // blank, just insert
                buffer.putInt(i, keyHash32);
                var extendBuffer = ByteBuffer.wrap(extendBytes);
                var offset = i * 6;
                extendBuffer.putLong(offset, recordId);
                extendBuffer.putLong(offset + 8, l);
                extendBuffer.putLong(offset + 16, seq);
                isPut = true;
                break;
            } else if (keyHash32 == targetKeyHash32) {
                // update
                var extendBuffer = ByteBuffer.wrap(extendBytes);
                var offset = i * 6;
                var oldRecordId = extendBuffer.getLong(offset);
                isExists = oldRecordId != NO_RECORD_ID;

                extendBuffer.putLong(offset, recordId);
                extendBuffer.putLong(offset + 8, l);
                extendBuffer.putLong(offset + 16, seq);
                isPut = true;
                break;
            }
        }

        if (!isPut) {
            if (bucketIndex % 1024 == 0) {
                log.info("All key hash buckets expand, bucket index={}, from bytes length={}", bucketIndex, bytes.length);
            }
            // extend capacity * 2
            var newBytes = new byte[bytes.length * 2];
            System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
            allKeyHash32BitBytesArray[bucketIndex] = newBytes;

            var newBytes2 = new byte[extendBytes.length * 2];
            System.arraycopy(extendBytes, 0, newBytes2, 0, extendBytes.length);
            extendBytesArray[bucketIndex] = newBytes2;

            ByteBuffer.wrap(newBytes).putInt(bytes.length, keyHash32);
            var buffer2 = ByteBuffer.wrap(newBytes2);
            buffer2.putLong(extendBytes.length, recordId);
            buffer2.putLong(extendBytes.length + 8, l);
            buffer2.putLong(extendBytes.length + 16, seq);
        }

        return isExists;
    }

    @VisibleForTesting
    @NotNull
    static PersistValueMeta recordIdToPvm(long recordId) {
        // max segment index = 512 * 1024 * 64 - 1 < 2 ^ 25
        // max sub block index = 3 < 2 ^ 2
        // max segment length = 4 * 1024 * 16 = 2 ^ 16
        // 25 bit for segment index, 2 bit for sub block index, 18 bit for segment offset, 18 bit for length
        var pvm = new PersistValueMeta();
        pvm.segmentIndex = (int) (recordId >> (18 + 18 + 2));
        pvm.subBlockIndex = (byte) (recordId >> (18 + 18) & 0x3);
        pvm.segmentOffset = (int) (recordId >> 18) & 0x3FFFF;
        return pvm;
    }

    static long pvmToRecordId(@NotNull PersistValueMeta pvm) {
        return positionToRecordId(pvm.segmentIndex, pvm.subBlockIndex, pvm.segmentOffset);
    }

    static long positionToRecordId(int segmentIndex, byte subBlockIndex, int segmentOffset) {
        return (((long) segmentIndex) << (18 + 18 + 2))
                | (((long) subBlockIndex) << (18 + 18))
                | (((long) segmentOffset & 0x3FFFF) << 18);
    }

    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = allKeyHash32BitBytesArray.length * 16L;
        for (var bytes : allKeyHash32BitBytesArray) {
            size += bytes.length * 7L;
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
        loadFromLastSavedFileWhenPureMemoryForBytesArray(this.allKeyHash32BitBytesArray, is);
        loadFromLastSavedFileWhenPureMemoryForBytesArray(this.extendBytesArray, is);
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
        writeToSavedFileWhenPureMemoryForBytesArray(this.allKeyHash32BitBytesArray, os);
        writeToSavedFileWhenPureMemoryForBytesArray(this.extendBytesArray, os);
    }

    @Override
    public void cleanUp() {
        log.warn("Clean up all key hash buckets, estimate in memory={}KB", estimate(new StringBuilder()) / 1024);
    }
}
