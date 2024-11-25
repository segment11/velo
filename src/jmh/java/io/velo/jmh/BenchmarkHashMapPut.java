package io.velo.jmh;

import com.fasterxml.jackson.databind.util.LRUMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 1, time = 2)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkHashMapPut {
    private String[] keys;

    @Param({"1000000", "10000000"})
    int size = 1_000_000;

    //    @Param({"value8-0", "value16-00000000",
//            "value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-",
//            "value200--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000--000000000-"})
//    @Param({"value8-0", "value16-00000000"})
    @Param({"value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-"})
    String value = "value8-0";

    @Setup
    public void setup() {
        keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UUID.randomUUID().toString();
        }
        System.out.printf("init keys, size: %d\n", size);
    }

    /*
    init capacity: 1000000
Benchmark                  (size)                                                                                               (value)  Mode  Cnt    Score   Error  Units
io.velo.jmh.BenchmarkHashMapPut.put   1000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt        57.294          ms/op
io.velo.jmh.BenchmarkHashMapPut.put  10000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt       945.625          ms/op
     */

    /*
    init capacity: 10000000, no rehash
Benchmark                  (size)                                                                                               (value)  Mode  Cnt    Score   Error  Units
io.velo.jmh.BenchmarkHashMapPut.put   1000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt        53.754          ms/op
io.velo.jmh.BenchmarkHashMapPut.put  10000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt       525.225          ms/op
     */

    @Benchmark
    public void put() {
        var map = new HashMap<String, String>(1000000);
        for (int i = 0; i < size; i++) {
            var key = keys[i];
            map.put(key, value);
        }
    }

/*
Benchmark                          (size)                                                                                               (value)  Mode  Cnt     Score   Error  Units
io.velo.jmh.BenchmarkHashMapPut.putToLRUMap   1000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt        244.653          ms/op
io.velo.jmh.BenchmarkHashMapPut.putToLRUMap  10000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt       2437.604          ms/op
 */

    @Benchmark
    public void putToLRUMap() {
        var map = new LRUMap<String, String>(100000, 1000000);
        for (int i = 0; i < size; i++) {
            var key = keys[i];
            map.put(key, value);
        }
    }

    /*
Benchmark                        (size)                                                                                               (value)  Mode  Cnt      Score   Error  Units
io.velo.jmh.BenchmarkHashMapPut.putToTree   1000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt         612.503          ms/op
io.velo.jmh.BenchmarkHashMapPut.putToTree  10000000  value100--000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-000000000-  avgt       11358.692          ms/op
     */
    @Benchmark
    public void putToTree() {
        var map = new TreeMap<String, String>();
        for (int i = 0; i < size; i++) {
            var key = keys[i];
            map.put(key, value);
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkHashMapPut.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
