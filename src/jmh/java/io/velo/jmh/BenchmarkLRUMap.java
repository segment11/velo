package io.velo.jmh;

import org.apache.commons.collections4.map.LRUMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkLRUMap {
    private String[] keys;

    @Param({"1000000", "10000000"})
    int size = 1_000_000;

    @Setup
    public void setup() {
        keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UUID.randomUUID().toString();
        }
        System.out.printf("init keys, size: %d\n", size);
    }

    private final Random random = new Random();

    private final LRUMap<String, String> map = new LRUMap<>(size / 10);

    @Benchmark
    public void put() {
        var key = keys[random.nextInt(size)];
        map.put(key, key);
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkLRUMap.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
