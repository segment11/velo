package io.velo.monitor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * This class is designed to keep track of the top k biggest keys encountered,
 * based on their byte lengths. It uses a priority queue to efficiently manage
 * and retrieve the largest keys.
 */
public class BigKeyTopK {
    /**
     * The configuration key used to specify the number of top keys to track.
     */
    public static final String KEY_IN_DYN_CONFIG = "monitor_big_key_top_k";

    /**
     * A record representing a key and its byte length.
     */
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
            BigKey that = (BigKey) obj;
            return Arrays.equals(keyBytes, that.keyBytes);
        }
    }

    private final int k;

    /**
     * The priority queue used to store the keys, ordered by their byte length.
     */
    private final PriorityQueue<BigKey> queue;

    /**
     * Returns the priority queue holding the keys.
     *
     * @return The priority queue of BigKey objects.
     */
    public PriorityQueue<BigKey> getQueue() {
        return queue;
    }

    /**
     * Returns the number of keys currently held in the priority queue.
     *
     * @return The size of the queue.
     */
    public int size() {
        return queue.size();
    }

    /**
     * Counts the number of keys in the queue that have a length greater than or equal to the specified checkLength.
     *
     * @param checkLength The minimum length of keys to count.
     * @return The number of keys with length >= checkLength.
     */
    public int sizeIfBiggerThan(int checkLength) {
        return (int) queue.stream()
                .filter(one -> one.length >= checkLength)
                .count();
    }

    /**
     * Constructs a BigKeyTopK object with a specified capacity for the priority queue.
     *
     * @param k The number of top keys to track.
     */
    public BigKeyTopK(int k) {
        this.k = k;
        this.queue = new PriorityQueue<>(k, Comparator.comparingInt(a -> a.length));
    }

    /**
     * Adds a key to the priority queue if it is larger than the current smallest key in the queue.
     * If an identical key already exists in the queue, it is replaced.
     *
     * @param keyBytes The byte array representing the key.
     * @param length   The length of the key's byte array.
     */
    public void add(byte[] keyBytes, int length) {
        BigKey added = new BigKey(keyBytes, length);
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