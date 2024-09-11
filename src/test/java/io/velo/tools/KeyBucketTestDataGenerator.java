package io.velo.tools;

import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.SnowFlake;
import io.velo.persist.FdReadWrite;
import io.velo.persist.KeyBucket;
import io.velo.persist.KeyLoader;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class KeyBucketTestDataGenerator implements TestDataGenerator, WalVDataGenerator {
    @Override
    public void generate() throws IOException {
        prepare();

        var slot = slot();
        var confForSlot = confForSlot();
        var slotDir = slotDir();

        System.setProperty("jnr.ffi.asm.enabled", "false");
        var libC = LibraryLoader.create(LibC.class).load("c");

        final byte splitNumber = 3;
        var sharedBytesArray = new byte[splitNumber][];
        var fdReadWriteArray = new FdReadWrite[splitNumber];
        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            sharedBytesArray[splitIndex] = new byte[confForSlot.confWal.oneChargeBucketNumber * KeyLoader.KEY_BUCKET_ONE_COST_SIZE];
            var file = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");

            // prometheus metric labels use _ instead of -
            var name = "key_bucket_split_" + splitIndex + "_slot_" + slot;
            FdReadWrite fdReadWrite = new FdReadWrite(slot, name, libC, file);
            fdReadWrite.initByteBuffers(false);
            fdReadWriteArray[splitIndex] = fdReadWrite;
        }

        var map = generateVListByWalGroupIndex(
                confForSlot.confBucket.bucketsPerSlot,
                confForSlot.confWal.oneChargeBucketNumber,
                16,
                "key:");

        for (var entry : map.entrySet()) {
            var walGroupIndex = entry.getKey();
            var vList = entry.getValue();

            var beginBucketIndex = walGroupIndex * confForSlot.confWal.oneChargeBucketNumber;

            // group by bucket index
            var vListGroupByBucketIndex = vList.stream().collect(Collectors.groupingBy(one -> one.bucketIndex()));
            for (var entry2 : vListGroupByBucketIndex.entrySet()) {
                var bucketIndex = entry2.getKey();
                var vListByBucketIndex = entry2.getValue();

                var relativeBucketIndex = bucketIndex - beginBucketIndex;

                // group by split index
                var vListGroupBySplitIndex = vListByBucketIndex.stream().collect(Collectors.groupingBy(one -> KeyHash.splitIndex(one.keyHash(), splitNumber, bucketIndex)));
                for (var entry3 : vListGroupBySplitIndex.entrySet()) {
                    var splitIndex = entry3.getKey();
                    var vListBySplitIndex = entry3.getValue();

                    var sharedBytes = sharedBytesArray[splitIndex];
                    var position = relativeBucketIndex * KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
                    var keyBucket = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, sharedBytes, position, snowFlake);

                    for (var v : vListBySplitIndex) {
                        keyBucket.put(v.key().getBytes(), v.keyHash(), v.expireAt(), v.seq(), v.cvEncoded());
                    }
                    keyBucket.putMeta();
                }
            }

            for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
                var sharedBytes = sharedBytesArray[splitIndex];
                var fdReadWrite = fdReadWriteArray[splitIndex];
                var n = fdReadWrite.writeSharedBytesForKeyBucketsInOneWalGroup(beginBucketIndex, sharedBytes);
                if (walGroupIndex % 100 == 0) {
                    System.out.println("Done write for wal group index=" + walGroupIndex + ", split index=" + splitIndex + ", n=" + n);
                }

                Arrays.fill(sharedBytes, (byte) 0);
            }

            System.out.println("Done put for wal group index=" + walGroupIndex + ", v list size=" + vList.size());
        }

        // clean up
        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            fdReadWrite.cleanUp();
        }
    }

    private final SnowFlake snowFlake = new SnowFlake(1, 1);

    @Override
    public long nextId() {
        return snowFlake.nextId();
    }

    @Override
    public long n() {
        return 10_000_000L;
    }

    @Override
    public ConfForSlot confForSlot() {
        var r = ConfForSlot.c10m;
        ConfForSlot.global = r;
        return r;
    }

    public static void main(String[] args) throws IOException {
        new KeyBucketTestDataGenerator().generate();
    }
}
