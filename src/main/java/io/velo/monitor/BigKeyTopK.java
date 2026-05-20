package io.velo.monitor;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Tracks the top k biggest keys by byte length using a priority queue.
 */
public class BigKeyTopK {
    /** The configuration key for the top-k tracking interval. */
    public static final String KEY_IN_DYN_CONFIG = "monitor_big_key_top_k";

    /** A key and its byte length. */
    public record BigKey(String key, int length) {
        @Override
        public @NotNull String toString() {
            return "BigKey{" +
                    "key='" + key + '\'' +
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
            BigKey that = (BigKey) obj;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    private final int k;

    /** The priority queue ordered by byte length. */
    private final PriorityQueue<BigKey> queue;

    /** Returns the priority queue holding the keys. */
    public PriorityQueue<BigKey> getQueue() {
        return queue;
    }

    /** Returns the number of keys currently held. */
    public int size() {
        return queue.size();
    }

    /**
     * @param checkLength the minimum length threshold
     * @return the number of keys with length >= checkLength
     */
    public int sizeIfBiggerThan(int checkLength) {
        return (int) queue.stream()
                .filter(one -> one.length >= checkLength)
                .count();
    }

    /**
     * @param k the number of top keys to track
     */
    public BigKeyTopK(int k) {
        this.k = k;
        this.queue = new PriorityQueue<>(k, Comparator.comparingInt(a -> a.length));
    }

    /**
     * @param key    the key to add
     * @param length the byte length of the key
     */
    public void add(String key, int length) {
        BigKey added = new BigKey(key, length);
        boolean isRemoved = queue.removeIf(one -> one.equals(added));
        if (isRemoved) {
            queue.add(added);
            return;
        }

        assert k > 0;
        if (queue.size() == k) {
            BigKey peek = queue.peek();
            if (peek.length < length) {
                queue.poll();
                queue.add(added);
            }
        } else {
            queue.add(added);
        }
    }
}
