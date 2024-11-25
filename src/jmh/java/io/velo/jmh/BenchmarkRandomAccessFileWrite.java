package io.velo.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(4)
public class BenchmarkRandomAccessFileWrite {
    final String dirPath = "/tmp/test_random_access_file_write_jmh";

    @Param({"1", "2"})
    int fileNumber = 1;

    ArrayList<RandomAccessFile> rafList = new ArrayList<>();

    @Setup
    public void setup() throws IOException {
        var dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        var threadId = Thread.currentThread().threadId();
        var targetDir = new File(dir, "thread_" + threadId);
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }

        for (int i = 0; i < fileNumber; i++) {
            var file = new File(targetDir, "/test_write_jmh_" + i);
            FileInit.append2GBFile(file, false);

            var raf = new RandomAccessFile(file, "rw");
            rafList.add(raf);
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        for (var raf : rafList) {
            raf.close();
        }
    }

    private final Random random = new Random();

    private final byte[] writeBytes = new byte[FileInit.PAGE_SIZE];

    /*
    Threads: 16
Benchmark                             (fileNumber)   Mode  Cnt     Score   Error   Units
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1  thrpt       1452.945          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2  thrpt        706.777          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1   avgt          0.012           ms/op
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2   avgt          0.026           ms/op
     */

    /*
    Threads: 4
Benchmark                             (fileNumber)   Mode  Cnt     Score   Error   Units
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1  thrpt       2871.535          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2  thrpt       1103.567          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1   avgt          0.001           ms/op
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2   avgt          0.004           ms/op
     */

    /*
    Threads: 1
Benchmark                             (fileNumber)   Mode  Cnt     Score   Error   Units
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1  thrpt       1148.828          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2  thrpt        553.281          ops/ms
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             1   avgt          0.001           ms/op
io.velo.jmh.BenchmarkRandomAccessFileWrite.write             2   avgt          0.002           ms/op
     */

    @Benchmark
    public void write() throws IOException {
        int segmentIndex = random.nextInt(FileInit.PAGE_NUMBER);
        for (var raf : rafList) {
            raf.seek((long) segmentIndex * FileInit.PAGE_SIZE);
            raf.write(writeBytes);
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkRandomAccessFileWrite.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
