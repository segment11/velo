package bench;

import io.velo.Utils;
import org.apache.commons.io.FileUtils;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class LoopSetRandomAndThenGetAndCheckIfMatch extends Thread {
    private final int loopId;
    private final CountDownLatch latch;

    private final int batchNum = 10000;
    private final int port = 7379;
//    private final int port = 6379;

    // change here
    private static final int threadNumber = 10;

    private boolean doSet = true;
    private boolean appendFile = false;
    private boolean isKeyNumberSeq = false;

    int keyValueEndNotMatchNumber = 0;
    int valueTotalNotMatchNumber = 0;
    int nullGetNumber = 0;

    public LoopSetRandomAndThenGetAndCheckIfMatch(int loopId, CountDownLatch latch) {
        this.loopId = loopId;
        this.latch = latch;
    }

    public static void main(String[] args) throws InterruptedException {
        var latch = new CountDownLatch(threadNumber);
        var beginT = System.currentTimeMillis();

        Thread[] threads = new Thread[threadNumber];
        // 10 threads
        for (int i = 0; i < threadNumber; i++) {
            var thread = new LoopSetRandomAndThenGetAndCheckIfMatch(i, latch);
            threads[i] = thread;
            thread.start();
        }
        latch.await();
        var endT = System.currentTimeMillis();
        System.out.println("cost=" + (endT - beginT) + "ms");

        int keyValueEndNotMatchNumber = 0;
        int valueTotalNotMatchNumber = 0;
        int nullGetNumber = 0;
        for (int i = 0; i < threadNumber; i++) {
            var t = (LoopSetRandomAndThenGetAndCheckIfMatch) threads[i];
            keyValueEndNotMatchNumber += t.keyValueEndNotMatchNumber;
            valueTotalNotMatchNumber += t.valueTotalNotMatchNumber;
            nullGetNumber += t.nullGetNumber;
        }
        System.out.println("key value end not match=" + keyValueEndNotMatchNumber);
        System.out.println("value total not match=" + valueTotalNotMatchNumber);
        System.out.println("null get=" + nullGetNumber);
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
        var userHome = System.getProperty("user.home");

        try {
            var f = new File(new File(userHome), "velo-data-loopSetValues-" + loopId + ".txt");
            if (doSet) {
                FileOutputStream fos = null;
                if (appendFile) {
                    if (!f.exists()) {
                        FileUtils.touch(f);
                    } else {
                        f.delete();
                        FileUtils.touch(f);
                    }
                    fos = new FileOutputStream(f, true);
                }

                var sbAppend = new StringBuilder();
                for (int i = 0; i < batchNum; i++) {
                    var x = loopId * batchNum + i;
                    var key = generateRandomKey(x, isKeyNumberSeq);
                    if (i == 0) {
                        System.out.println("begin with=" + key + ", loop id=" + loopId);
                    }

                    var sb = new StringBuilder();
                    for (int j = 0; j < 5; j++) {
                        sb.append(UUID.randomUUID());
                    }
                    var valueSet = sb + key;
                    jedis.set(key, valueSet);

                    sbAppend.append(key).append("=").append(valueSet).append("\n");

                    if (appendFile) {
                        if (i % 1000 == 0 || i == batchNum - 1) {
                            fos.write(sbAppend.toString().getBytes());
                            sbAppend.setLength(0);
                        }
                    }
                }
                if (fos != null) {
                    fos.close();
                }
            } else {
                List<String> lines = null;
                if (appendFile) {
                    lines = FileUtils.readLines(f, "utf-8");
                }

                int c = 0;
                long valueLength = 0;
                for (int i = 0; i < batchNum; i++) {
                    String valueSetLine = null;
                    String key;
                    if (appendFile) {
                        valueSetLine = lines.get(i);
                        key = valueSetLine.substring(0, 20);
                    } else {
                        var x = loopId * batchNum + i;
                        key = generateRandomKey(x, isKeyNumberSeq);
                    }

                    var value = jedis.get(key);
                    if (value != null) {
                        c++;
                        valueLength += value.length();

                        // last 20 characters
                        var last20 = value.substring(value.length() - 20);
                        if (!last20.equals(key)) {
//                            System.out.println("value not equal, key=" + key + ", loop id=" + loopId + ", value=" + value);
                            keyValueEndNotMatchNumber++;
                        } else {
                            if (appendFile) {
                                // value match
                                if (!valueSetLine.equals(key + "=" + value)) {
//                                System.out.println("value not match, key=" + key + ", loop id=" + loopId);
//                                System.out.println("value set=" + valueSetLine + ", value=" + value);
                                    valueTotalNotMatchNumber++;
                                }
                            }
                        }
                    } else {
                        nullGetNumber++;
                    }
                }

                System.out.println(c + ", count,  loop id=" + loopId);
                System.out.println(valueLength + ", value length, loop id=" + loopId);
                System.out.println(keyValueEndNotMatchNumber + ", not match number, loop id=" + loopId);
                System.out.println(nullGetNumber + ", null get number, loop id=" + loopId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
            latch.countDown();
        }
    }
}
