package io.velo.jmh;

import io.activej.config.Config;
import io.velo.*;
import io.velo.persist.LocalPersist;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class OneSlotRead {
    @Param({"1000000"})
    int initKeyNumber = 1_000_000;

    final int netWorkers = 1;
    final short slotNumber = 1;
    final short slot = 0;

    final File persistDir = new File("/tmp/velo-data/test-jmh-persist/");

    static final String INIT_VALUE;
    static final String KEY_PREFIX = "key:";

    static {
        // rand length 200
        var random = new Random();
        var sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append((char) (random.nextInt(26) + 'a'));
        }
        INIT_VALUE = sb.toString();
        System.out.println("INIT_VALUE: " + INIT_VALUE);
    }

    @Setup
    public void setup() throws IOException {
        ConfForSlot.global = ConfForSlot.c1m;
        ConfForGlobal.netListenAddresses = "localhost:7379";

        var localPersist = LocalPersist.getInstance();

        RequestHandler.initMultiShardShadows((byte) netWorkers);

        var snowFlakes = new SnowFlake[netWorkers];
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(i + 1, 1);
        }
        localPersist.initSlots((byte) netWorkers, slotNumber, snowFlakes, persistDir, Config.create());
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId());

        var snowFlake = snowFlakes[0];

        var oneSlot = localPersist.oneSlot(slot);

        var beginT = System.currentTimeMillis();
        for (int i = 0; i < initKeyNumber; i++) {
            var key = KEY_PREFIX + i;
            var keyBytes = key.getBytes();
            var s = BaseCommand.slot(keyBytes, slotNumber);
            var cv = new CompressedValue();

            cv.setSeq(snowFlake.nextId());
            cv.setKeyHash(s.keyHash());

//            cv.setDictSeqOrSpType(CompressedValue.SP_TYPE_NUM_INT);
//            var intBytes = new byte[4];
//            ByteBuffer.wrap(intBytes).putInt(i);
//            cv.setCompressedData(intBytes);
//            cv.setCompressedLength(4);
//            cv.setUncompressedLength(4);

            cv.setDictSeqOrSpType(CompressedValue.NULL_DICT_SEQ);
            cv.setCompressedData(INIT_VALUE.getBytes());
            cv.setCompressedLength(INIT_VALUE.length());
            cv.setUncompressedLength(INIT_VALUE.length());

            oneSlot.put(key, s.bucketIndex(), cv);
        }
        var costT = System.currentTimeMillis() - beginT;
        System.out.println("Init " + initKeyNumber + " keys cost " + (costT / 1000) + "s");
    }

    private final Random random = new Random();

    @Benchmark
    public void read() {
        int i = random.nextInt(initKeyNumber);
        var key = KEY_PREFIX + i;

        var localPersist = LocalPersist.getInstance();
        var oneSlot = localPersist.oneSlot((short) 0);

        var keyBytes = key.getBytes();
        var s = BaseCommand.slot(keyBytes, slotNumber);
        var result = oneSlot.get(keyBytes, s.bucketIndex(), s.keyHash(), s.keyHash32());
        assert result != null;
    }

    @TearDown
    public void tearDown() throws IOException {
        var localPersist = LocalPersist.getInstance();
        var oneSlot = localPersist.oneSlot(slot);
        var metrics = oneSlot.collect();
        for (var m : metrics.entrySet()) {
            System.out.println(m);
        }

        localPersist.cleanUp();
        FileUtils.deleteDirectory(persistDir);
        System.out.println("Delete " + persistDir);
    }
}
