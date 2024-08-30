package bench;

import io.velo.Utils;
import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class LoopMGet extends Thread {
    private final int loopId;
    private final CountDownLatch latch;

    private final int batchNum = 10000;
    private final int port = 7379;
//    private final int port = 6379;

    private static final int threadNumber = 60;

    private final int mgetKeyNum = 50;

    private boolean isKeyNumberSeq = true;

    int nullGetNumber = 0;

    long costT = 0;

    public LoopMGet(int loopId, CountDownLatch latch) {
        this.loopId = loopId;
        this.latch = latch;
    }

    public static void main(String[] args) throws InterruptedException {
        var latch = new CountDownLatch(threadNumber);
        var beginT = System.currentTimeMillis();

        Thread[] threads = new Thread[threadNumber];
        // 10 threads
        for (int i = 0; i < threadNumber; i++) {
            var thread = new LoopMGet(i, latch);
            threads[i] = thread;
            thread.start();
        }
        latch.await();
        var endT = System.currentTimeMillis();
        System.out.println("cost: " + (endT - beginT) + "ms");

        int nullGetNumber = 0;
        long totalCostT = 0;
        for (int i = 0; i < threadNumber; i++) {
            var t = (LoopMGet) threads[i];
            nullGetNumber += t.nullGetNumber;
            totalCostT += t.costT;
        }
        System.out.println("null get: " + nullGetNumber);
        System.out.println("total cost: " + totalCostT / 1000000 + "ms");
    }

    private String generateRandomKey(int x, boolean isNumberSeq) {
        if (isNumberSeq) {
            return "key:" + Utils.leftPad(String.valueOf(x), "0", 16);
        }

        final int keyLength = 20;
        var sb = new StringBuilder();
        var rand = new Random();
        for (int i = 0; i < keyLength; i++) {
            sb.append((char) (rand.nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    @Override
    public void run() {
        var jedis = new Jedis("localhost", port);

        var beginI = loopId * batchNum;

        var times = batchNum / mgetKeyNum;
        var rand = new Random();
        for (int i = 0; i < times; i++) {
            var keys = new String[mgetKeyNum];
            for (int j = 0; j < mgetKeyNum; j++) {
                keys[j] = generateRandomKey(beginI + rand.nextInt(batchNum), isKeyNumberSeq);
            }

            var beginT = System.nanoTime();
            var values = jedis.mget(keys);
            var endT = System.nanoTime();
            costT += (endT - beginT);

            if (values == null || values.isEmpty()) {
                nullGetNumber++;
            }
        }

        latch.countDown();
    }
}
