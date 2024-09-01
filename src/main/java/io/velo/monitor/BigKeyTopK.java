package io.velo.monitor;

import java.util.PriorityQueue;

public class BigKeyTopK {
    public static final String KEY_IN_DYN_CONFIG = "monitor_big_key_top_k";

    public record BigKey(byte[] keyBytes, int size) {
        @Override
        public String toString() {
            return "BigKey{" +
                    "key='" + new String(keyBytes) + '\'' +
                    ", size=" + size +
                    '}';
        }
    }

    private final int k;

    private final PriorityQueue<BigKey> queue;

    public PriorityQueue<BigKey> getQueue() {
        return queue;
    }

    public int size() {
        return queue.size();
    }

    public BigKeyTopK(int k) {
        this.k = k;
        this.queue = new PriorityQueue<>(k, (a, b) -> Integer.compare(b.size, a.size));
    }

    public void add(byte[] keyBytes, int size) {
        if (queue.size() == k) {
            var peek = queue.peek();
            if (peek.size < size) {
                queue.poll();
                queue.add(new BigKey(keyBytes, size));
            }
        } else {
            queue.add(new BigKey(keyBytes, size));
        }
    }
}
