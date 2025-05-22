package io.velo.jmh;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.velo.*;
import io.velo.persist.LocalPersist;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 0, time = 10)
@Measurement(iterations = 1, time = 10)
@State(Scope.Benchmark)
@Threads(1)
public class BenchmarkLocalPersistPut {
    private String[] keys;

    int keyNumber = 10_000_000;

    private final File persistDir = new File("/tmp/test_jmh_persist_put");

    private final LocalPersist localPersist = LocalPersist.getInstance();

    short slotNumber = 4;

    byte slotWorkers = 4;

    private Eventloop[] slotWorkerEventloopArray;

    private AtomicInteger count = new AtomicInteger();
    private long beginTimeMs;
    private long beginTimeMs2;

    @Setup
    public void setup() throws IOException, InterruptedException {
        keys = new String[keyNumber];
        for (int i = 0; i < keyNumber; i++) {
            keys[i] = "key:" + Utils.leftPad(String.valueOf(i), "0", 12);
        }
        System.out.printf("init keys, number: %d\n", keyNumber);

        // todo, change here
        ConfForSlot.global = ConfForSlot.c10m;

        slotWorkerEventloopArray = new Eventloop[slotWorkers];
        for (int i = 0; i < slotWorkers; i++) {
            var eventloop = Eventloop.builder()
                    .withThreadName("slot-worker-" + i)
                    .withIdleInterval(Duration.ofMillis(10))
                    .build();
            eventloop.keepAlive(true);
            slotWorkerEventloopArray[i] = eventloop;

            new Thread(eventloop).start();
            Thread.sleep(100);
            System.out.println("slot worker event loop started, thread id: " + eventloop.getEventloopThread().threadId());
        }

        ConfForGlobal.netListenAddresses = "127.0.0.1:7379";
        RequestHandler.initMultiShardShadows(slotWorkers);

        if (persistDir.exists()) {
            FileUtils.deleteDirectory(persistDir);
            FileUtils.forceMkdir(persistDir);
            System.out.println("delete and recreate persist dir");
        }

        var snowFlakes = new SnowFlake[slotWorkers];
        for (int i = 0; i < slotWorkers; i++) {
            snowFlakes[i] = new SnowFlake(ConfForGlobal.datacenterId, (ConfForGlobal.machineId << 8) | i);
        }
        localPersist.initSlots(slotWorkers, slotNumber, snowFlakes, persistDir, Config.create());
        System.out.println("init local persist slots");

        for (int i = 0; i < slotWorkers; i++) {
            for (var oneSlot : localPersist.oneSlots()) {
                if (oneSlot.slot() % slotWorkers == i) {
                    oneSlot.setSlotWorkerEventloop(slotWorkerEventloopArray[i]);
                    System.out.printf("set slot worker event loop for slot=%d, slot worker id=%d\n", oneSlot.slot(), i);
                }
            }
        }

        for (short slot = 0; slot < slotNumber; slot++) {
            int i = slot % slotWorkers;
            localPersist.fixSlotThreadId(slot, slotWorkerEventloopArray[i].getEventloopThread().threadId());
        }

        beginTimeMs = System.currentTimeMillis();
        beginTimeMs2 = System.currentTimeMillis();
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        // 30s
        final int maxLoopCount = 10 * 30;
        int loopCount = 0;
        while (count.get() < keyNumber) {
            Thread.sleep(100);
            loopCount++;

            if (loopCount > maxLoopCount) {
                throw new RuntimeException("timeout");
            }
        }

        var costTimeMs = System.currentTimeMillis() - beginTimeMs;
        System.out.printf("put %d keys, cost time: %d ms\n", keyNumber, costTimeMs);

        var qps = keyNumber * 1000L / costTimeMs;
        System.out.printf("qps: %d\n", qps);

        isAfterTearDown = true;

        for (var eventloop : slotWorkerEventloopArray) {
            eventloop.breakEventloop();
        }
        System.out.println("break slot worker event loops");

        // print stats
        var currentThreadId = Thread.currentThread().threadId();
        for (var oneSlot : localPersist.oneSlots()) {
            localPersist.fixSlotThreadId(oneSlot.slot(), currentThreadId);
            var stats = oneSlot.collect();
            var sb = new StringBuilder();
            sb.append("*** *** ***").append("Slot: ").append(oneSlot.slot()).append('\n');
            stats.forEach((k, v) -> {
                sb.append(k).append('=').append(v).append('\n');
            });
            System.out.println(sb);
        }

        System.out.println("wait 10s when break slot worker event loops and then clean up local persist");
        Thread.sleep(1000 * 10);

        localPersist.cleanUp();
        System.out.println("clean up local persist");
    }

    private volatile boolean isAfterTearDown = false;

    private final Random random = new Random();

    // -d 200
//    private final byte[] valueBytes = Utils.leftPad("value", "0", 200).getBytes();
    // short values, value length <= 32
    // -d 32
    private final byte[] valueBytes = Utils.leftPad("value", "0", 32).getBytes();

    /*
    -d 32
    qps: 986679

Benchmark                     Mode  Cnt  Score   Error  Units
BenchmarkLocalPersistPut.put  avgt       0.601          us/op

    -d 200
    qps: 435293

Benchmark                     Mode  Cnt  Score   Error  Units
BenchmarkLocalPersistPut.put  avgt       0.629          us/op

TIPS: 512MB write buffer for each slot, so the real write qps is in logs. (100w cost time: ***)
     */

    @Benchmark
    public void put() {
        var key = keys[random.nextInt(keyNumber)];
        var s = BaseCommand.slot(key.getBytes(), slotNumber);
        var oneSlot = localPersist.oneSlot(s.slot());
        var eventloop = slotWorkerEventloopArray[s.slot() % slotWorkers];

        eventloop.submit(() -> {
            if (isAfterTearDown) {
                return;
            }

            var cv = new CompressedValue();
            cv.setSeq(1L);
            cv.setKeyHash(s.keyHash());
            cv.setExpireAt(CompressedValue.NO_EXPIRE);
            cv.setDictSeqOrSpType(CompressedValue.NULL_DICT_SEQ);
            cv.setCompressedData(valueBytes);

            oneSlot.put(key, s.bucketIndex(), cv);

            var c = count.incrementAndGet();
            if (c % 1_000_000 == 0) {
                var costTimeMs2 = System.currentTimeMillis() - beginTimeMs2;
                System.out.printf("put %d keys, 100w cost time: %d ms, qps: %d\n", c, costTimeMs2, 1_000_000_000L / costTimeMs2);
                beginTimeMs2 = System.currentTimeMillis();
            }
        });
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkLocalPersistPut.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
