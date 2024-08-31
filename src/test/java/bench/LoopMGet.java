package bench;

import io.velo.Utils;
import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class LoopMGet extends Thread {
    private final String host;
    private final int port;

    private final CountDownLatch latch;

    private final int threadIndex;
    private final int oneThreadChargeKeyNumber;

    private final int mgetOnceKeyCount;
    private final boolean isKeyNumberSeq;

    int nullGetNumber = 0;

    long costT = 0;

    public LoopMGet(String host, int port,
                    int threadNumber, int threadIndex, CountDownLatch latch,
                    int oneThreadChargeKeyNumber, int mgetOnceKeyCount, boolean isKeyNumberSeq) {
        this.host = host;
        this.port = port;

        this.threadIndex = threadIndex;
        this.latch = latch;
        this.oneThreadChargeKeyNumber = oneThreadChargeKeyNumber;

        this.mgetOnceKeyCount = mgetOnceKeyCount;
        this.isKeyNumberSeq = isKeyNumberSeq;
    }

    public static void main(String[] args) throws InterruptedException {
        var threadNumber = 2;
        var latch = new CountDownLatch(threadNumber);
        Thread[] threads = new Thread[threadNumber];

        var beginT = System.currentTimeMillis();
        // 10 threads
        for (int i = 0; i < threadNumber; i++) {
            // todo change here
            var thread = new LoopMGet("localhost", 7379, threadNumber, i, latch,
                    100000, 50, true);
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

    private static String generateRandomKey(int x, boolean isKeyNumberSeq) {
        if (isKeyNumberSeq) {
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
        var jedis = new Jedis(host, port);

        var beginI = threadIndex * oneThreadChargeKeyNumber;

        var times = oneThreadChargeKeyNumber / mgetOnceKeyCount;
        var rand = new Random();
        for (int i = 0; i < times; i++) {
            var keys = new String[mgetOnceKeyCount];
            for (int j = 0; j < mgetOnceKeyCount; j++) {
                keys[j] = generateRandomKey(beginI + rand.nextInt(oneThreadChargeKeyNumber), isKeyNumberSeq);
            }

            var beginT = System.nanoTime();
            var values = jedis.mget(keys);
            var endT = System.nanoTime();
            costT += (endT - beginT);

            if (values == null || values.isEmpty()) {
                nullGetNumber++;
            }
        }

        jedis.close();
        latch.countDown();
    }
}
