package io.velo.tools;

import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.persist.KeyBucket;
import io.velo.persist.PersistValueMeta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IterateKeyBuckets {
    //    private static String persistDir = "/tmp/velo-data-test-data";
    private static String persistDir = "/tmp/velo-data/persist";

    public static void main(String[] args) throws IOException {
        short slot = 0;
        byte splitIndex = 0;
        byte splitNumber = 1;
        var bucketsPerSlot = ConfForSlot.ConfBucket.c100m.bucketsPerSlot;
        int[] sumArray = new int[bucketsPerSlot];

        // change here
        final int toCheckBucketIndex = -1;
        // check -d 200 compressed value length is 74, filter those not match
        // check -d 1000 compressed value length is 181, filter those not match
//        final int cvNormalLength = 74;
        final int cvNormalLength = 181;
        final int overSize = 50;

        for (int i = 0; i < splitNumber; i++) {
            iterateOneSplitIndex(slot, (byte) i, splitNumber, sumArray);
        }

        int sumTotal = 0;
        for (int i = 0; i < sumArray.length; i++) {
            sumTotal += sumArray[i];
        }
        System.out.println("sum total=" + sumTotal);

        // get size > 100
        for (var entry : keyWithValueBytesByBucketIndex.entrySet()) {
            var bucketIndex = entry.getKey();
            var keyWithValueBytes = entry.getValue();
            if (keyWithValueBytes.size() > overSize) {
                System.out.println("bucket index=" + bucketIndex + ", size=" + keyWithValueBytes.size() + ", keys=" + keyWithValueBytes.keySet());
            }

            var doLog = bucketIndex == toCheckBucketIndex;
            if (doLog) {
                System.out.println("bucket index=" + bucketIndex + ", size=" + keyWithValueBytes.size());
            }

            keyWithValueBytes.forEach((key, valueBytes) -> {
                if (PersistValueMeta.isPvm(valueBytes)) {
                    var pvm = PersistValueMeta.decode(valueBytes);
                    var isNormal = pvm.length == cvNormalLength;
                    if (doLog || !isNormal) {
                        System.out.println("key=" + key + ", pvm=" + pvm.shortString());
                    }
                } else {
                    var cv = CompressedValue.decode(Unpooled.wrappedBuffer(valueBytes), null, 0);
                    System.out.println("key=" + key + ", value=" + cv);
                }
            });

        }
    }

    private static Map<Integer, HashMap<String, byte[]>> keyWithValueBytesByBucketIndex = new HashMap<>();

    public static void iterateOneSplitIndex(short slot, byte splitIndex, byte splitNumber, int[] sumArray) throws IOException {
        var bucketsPerSlot = sumArray.length;

        var slotDir = new File(persistDir + "/slot-" + slot);
        var splitKeyBucketsFile = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");
        if (!splitKeyBucketsFile.exists()) {
            return;
        }

        final var bytes = new byte[4096];
        var fis = new FileInputStream(splitKeyBucketsFile);
        int i = 0;
        while (fis.read(bytes) != -1) {
            var size = ByteBuffer.wrap(bytes, 8, 2).getShort();
            sumArray[i] += size;
            if (size > 0) {
                var map = keyWithValueBytesByBucketIndex.get(i);
                if (map == null) {
                    map = new java.util.HashMap<>();
                    keyWithValueBytesByBucketIndex.put(i, map);
                }

//                System.out.println("size=" + size + ", bucket index=" + i);
                var keyBucket = new KeyBucket(slot, i, splitIndex, (byte) -1, bytes, null);
                var splitNumberThisKeyBucket = keyBucket.getSplitNumber();
                var splitIndexThisKeyBucket = keyBucket.getSplitIndex();
                int finalI = i;
                HashMap<String, byte[]> finalMap = map;
                keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
//                    System.out.println("key=" + new String(keyBytes) + ", key hash=" + keyHash);
                    var bucketIndexExpect = KeyHash.bucketIndex(keyHash, bucketsPerSlot);
                    if (bucketIndexExpect != finalI) {
                        System.out.println("bucket index expect=" + bucketIndexExpect + ", bucket index=" + finalI);
                        throw new IllegalStateException("bucket index expect=" + bucketIndexExpect + ", bucket index=" + finalI);
                    }

                    var splitIndexExpect = KeyHash.splitIndex(keyHash, splitNumberThisKeyBucket, bucketIndexExpect);
                    if (splitIndexExpect != splitIndexThisKeyBucket) {
                        System.out.println("split index expect=" + splitIndexExpect + ", split index=" + splitIndexThisKeyBucket);
                        throw new IllegalStateException("split index expect=" + splitIndexExpect + ", split index=" + splitIndexThisKeyBucket);
                    }
                    finalMap.put(new String(keyBytes), valueBytes);
                });
            }

            i++;
        }
        fis.close();

//        for (int j = 0; j < sumArray.length; j++) {
//            System.out.print(sumArray[j] + " ");
//            if (j % 1024 == 0) {
//                System.out.println();
//            }
//        }
//        System.out.println();
    }
}
