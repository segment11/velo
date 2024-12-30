package io.velo.jmh;

import com.google.protobuf.InvalidProtocolBufferException;
import io.velo.proto.NumberListProto;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkInt32or64EncodeAndDecode {
    final int count = 1_000;

    final int bound32 = 1_000;

    byte[] encoded;

    NumberListProto.NumberList numberList;

    @Setup
    public void setup() {
        var builder = NumberListProto.NumberList.newBuilder();
        var random = new Random();
        for (int i = 0; i < count; i++) {
            builder.addNumbers32(random.nextInt(bound32));
            builder.addNumbers64(random.nextLong(bound32 * 1000));
        }
        System.out.printf("init numbers, count: %d\n", count);
        numberList = builder.build();
        encoded = numberList.toByteArray();
        System.out.printf("encoded size: %d\n", encoded.length);
    }

    @Benchmark
    public void encode() {
        numberList.toByteArray();
    }

    @Benchmark
    public void decode() throws InvalidProtocolBufferException {
        NumberListProto.NumberList.parseFrom(encoded);
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkInt32or64EncodeAndDecode.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
