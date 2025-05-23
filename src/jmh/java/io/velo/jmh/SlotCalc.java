package io.velo.jmh;

import io.velo.BaseCommand;
import io.velo.Utils;
import org.openjdk.jmh.annotations.*;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 1, time = 5)
@State(Scope.Thread)
@Threads(1)
public class SlotCalc {
    @Param({"10000000", "100000000"})
    int initKeyNumber = 10_000_000;

    //    @Param({"8", "16", "64"})
    int slotNumber = 8;

    private final Random random = new Random();

    private byte[][] initKeyBytesArray;

    @Setup
    public void setup() throws IOException {
        initKeyBytesArray = new byte[initKeyNumber][];
        for (int i = 0; i < initKeyNumber; i++) {
            var key = "key:" + Utils.leftPad(String.valueOf(i), "0", 12);
            initKeyBytesArray[i] = key.getBytes();
        }
        System.out.println("Done init key bytes array, key number: " + initKeyNumber);
    }

    /*
Benchmark           (initKeyNumber)  Mode  Cnt  Score   Error  Units
SlotCalc.calc              10000000  avgt       0.236          us/op
SlotCalc.calc             100000000  avgt       0.293          us/op
SlotCalc.calcCrc16         10000000  avgt       0.195          us/op
SlotCalc.calcCrc16        100000000  avgt       0.250          us/op
     */
    @Benchmark
    public void calc() {
        int i = random.nextInt(initKeyNumber);
        var keyBytes = initKeyBytesArray[i];
        BaseCommand.slot(keyBytes, slotNumber);
    }

    @Benchmark
    public void calcCrc16() {
        int i = random.nextInt(initKeyNumber);
        var keyBytes = initKeyBytesArray[i];
        JedisClusterCRC16.getSlot(keyBytes);
    }
}
