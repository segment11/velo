package io.velo.monitor;

import java.util.Arrays;
import java.util.PriorityQueue;

public class BigKeyTopK {
    public static final String KEY_IN_DYN_CONFIG = "monitor_big_key_top_k";

    public record BigKey(byte[] keyBytes, int length) {
        @Override
        public String toString() {
            return "BigKey{" +
                    "key='" + new String(keyBytes) + '\'' +
                    ", length=" + length +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            var o = (BigKey) obj;
            return Arrays.equals(keyBytes, o.keyBytes);
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

    public int sizeIfBiggerThan(int checkLength) {
        return queue.stream()
                .filter(one -> one.length() >= checkLength)
                .mapToInt(one -> 1)
                .sum();
    }

    public BigKeyTopK(int k) {
        this.k = k;
        this.queue = new PriorityQueue<>(k, (a, b) -> Integer.compare(a.length, b.length));
    }

    public void add(byte[] keyBytes, int length) {
        var added = new BigKey(keyBytes, length);
        var isRemoved = queue.removeIf(one -> one.equals(added));
        if (isRemoved) {
            queue.add(added);
            return;
        }

        assert k > 0;
        if (queue.size() == k) {
            var peek = queue.peek();
            if (peek.length < length) {
                queue.poll();
                queue.add(added);
            }
        } else {
            queue.add(added);
        }
    }
}
