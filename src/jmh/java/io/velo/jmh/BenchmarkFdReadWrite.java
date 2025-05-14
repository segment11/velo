package io.velo.jmh;

import io.velo.ConfForSlot;
import io.velo.persist.FdReadWrite;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(4)
public class BenchmarkFdReadWrite {
    final String dirPath = "/tmp/test_fd_read_write_jmh";

    @Param({"1"})
    int fileNumber = 1;

    LibC libC;

    ArrayList<FdReadWrite> fdReadWriteList = new ArrayList<>();

    @Setup
    public void setup() throws IOException {
        System.setProperty("jnr.ffi.asm.enabled", "false");
        libC = LibraryLoader.create(LibC.class).load("c");

        ConfForSlot.global = ConfForSlot.c10m;

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
            var file = new File(targetDir, "/test_fd_read_write_jmh_" + i);
            FileInit.append2GBFile(file, true);

            var fdReadWrite = new FdReadWrite((short) 0, "test" + i, libC, file);
            fdReadWrite.initByteBuffers(false);
            fdReadWriteList.add(fdReadWrite);
        }
    }

    @TearDown
    public void tearDown() {
        for (var fdReadWrite : fdReadWriteList) {
            fdReadWrite.cleanUp();
        }

        System.out.println("Init int value set=" + initIntValueSet);
    }

    private final Random random = new Random();

    private final Set<Integer> initIntValueSet = new HashSet<>();

        /*
    Threads: 16
Benchmark                   (fileNumber)   Mode  Cnt    Score   Error   Units
io.velo.jmh.BenchmarkFdReadWrite.read              1  thrpt       287.089          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.write             1  thrpt       460.825          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.read              1   avgt         0.110           ms/op
io.velo.jmh.BenchmarkFdReadWrite.write             1   avgt         0.032           ms/op
     */

        /*
    Threads: 8
Benchmark                   (fileNumber)   Mode  Cnt    Score   Error   Units
io.velo.jmh.BenchmarkFdReadWrite.read              1  thrpt       144.510          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.write             1  thrpt       276.889          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.read              1   avgt         0.055           ms/op
io.velo.jmh.BenchmarkFdReadWrite.write             1   avgt         0.030           ms/op
     */

    /*
    Threads: 4
Benchmark                   (fileNumber)   Mode  Cnt    Score   Error   Units
io.velo.jmh.BenchmarkFdReadWrite.read              1  thrpt        72.053          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.write             1  thrpt       108.089          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.read              1   avgt         0.057           ms/op
io.velo.jmh.BenchmarkFdReadWrite.write             1   avgt         0.038           ms/op
     */

    /*
    Threads: 1
Benchmark                   (fileNumber)   Mode  Cnt   Score   Error   Units
io.velo.jmh.BenchmarkFdReadWrite.read              1  thrpt       20.734          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.write             1  thrpt       23.757          ops/ms
io.velo.jmh.BenchmarkFdReadWrite.read              1   avgt        0.048           ms/op
io.velo.jmh.BenchmarkFdReadWrite.write             1   avgt        0.042           ms/op
     */
    @Benchmark
    public void read() {
        int segmentIndex = random.nextInt(FileInit.PAGE_NUMBER);
        for (var fdReadWrite : fdReadWriteList) {
            var bytes = fdReadWrite.readOneInner(segmentIndex, false);
            var intValueInit = ByteBuffer.wrap(bytes).getInt();
            initIntValueSet.add(intValueInit);
        }
    }

    private final byte[] writeBytes = new byte[FileInit.PAGE_SIZE];

    @Benchmark
    public void write() {
        int segmentIndex = random.nextInt(FileInit.PAGE_NUMBER);
        for (var fdReadWrite : fdReadWriteList) {
            var n = fdReadWrite.writeOneInner(segmentIndex, writeBytes, false);
            if (n != FileInit.PAGE_SIZE) {
                throw new RuntimeException("write failed");
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkFdReadWrite.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
