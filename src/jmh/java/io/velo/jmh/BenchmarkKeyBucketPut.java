package io.velo.jmh;

import io.velo.KeyHash;
import io.velo.SnowFlake;
import io.velo.persist.KeyBucket;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkKeyBucketPut {
    private String[] keys;
    private Long[] keysHash;

    private SnowFlake snowFlake;

    @Param({"10000", "100000"})
    int size = 10000;

    @Setup
    public void setup() {
        keys = new String[size];
        keysHash = new Long[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UUID.randomUUID().toString();
            keysHash[i] = KeyHash.hash(keys[i].getBytes());
        }
        System.out.printf("init keys / keys hash, size: %d\n", size);

        snowFlake = new SnowFlake(1, 1);
    }

    /*
Benchmark                        (size)  Mode  Cnt   Score   Error  Units
io.velo.jmh.BenchmarkKeyBucketPut.put         10000  avgt        2.458          ms/op
io.velo.jmh.BenchmarkKeyBucketPut.put        100000  avgt       24.519          ms/op
io.velo.jmh.BenchmarkKeyBucketPut.putAndGet   10000  avgt        2.456          ms/op
io.velo.jmh.BenchmarkKeyBucketPut.putAndGet  100000  avgt       24.516          ms/op
     */

    @Benchmark
    public void put() {
        final short slot = 0;
        final int capacity = KeyBucket.INIT_CAPACITY;
        final byte[] valueBytes = "value-test".getBytes();

        var keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake);
        for (int i = 0; i < size; i++) {
            if (i % capacity == 0) {
                keyBucket.clearAll();
            }

            var key = keys[i];
            var keyHash = keysHash[i];
            keyBucket.put(key.getBytes(), keyHash, 0L, 1L, valueBytes);
        }
    }

    @Benchmark
    public void putAndGet() {
        final short slot = 0;
        final int capacity = KeyBucket.INIT_CAPACITY;
        final byte[] valueBytes = "value-test".getBytes();

        KeyBucket keyBucket = null;
        for (int i = 0; i < size; i++) {
            if (i % capacity == 0) {
                keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake);
            }

            var key = keys[i];
            var keyHash = keysHash[i];
            keyBucket.put(key.getBytes(), keyHash, 0L, 1L, valueBytes);
            keyBucket.getValueXByKey(key.getBytes(), keyHash);
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkKeyBucketPut.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();

//        var x = new io.velo.jmh.BenchmarkKeyBucketPut();
//        x.size = 100;
//        x.setup();
//
//        var beginT = System.currentTimeMillis();
//        x.putAndGet();
//        var endT = System.currentTimeMillis();
//        System.out.printf("time cost: %d ms\n", endT - beginT);
    }
}
