package io.velo;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestBucketDataSkew {
    public static void main(String[] args) throws IOException {
        int number = 100000000;
        int slotNumber = 8;
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        int[][] bucketKeyCount = new int[slotNumber][];
        for (int i = 0; i < slotNumber; i++) {
            bucketKeyCount[i] = new int[bucketsPerSlot];
        }

        for (int i = 0; i < number; i++) {
            // like redis-benchmark key generator
            var key = "key:" + Utils.leftPad(String.valueOf(i), "0", 12);
            var keyBytes = key.getBytes();

            var slotWithKeyHash = BaseCommand.slot(keyBytes, slotNumber);
            bucketKeyCount[slotWithKeyHash.slot()][slotWithKeyHash.bucketIndex()]++;
        }

        var file = new File("bucketKeyCount.txt");
        FileUtils.touch(file);
        var writer = new FileWriter(file);
        writer.write("--- key hash list split to bucket ---\n");

        for (int i = 0; i < slotNumber; i++) {
            writer.write("s " + i + "\n");
            var array = bucketKeyCount[i];
            for (int j = 0; j < bucketsPerSlot; j++) {
                writer.write("b " + j + " c: " + array[j] + "\n");
            }
        }
        writer.close();
    }
}
