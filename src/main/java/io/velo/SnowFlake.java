package io.velo;

import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Generates unique IDs using the Snowflake algorithm.
 * This class provides methods to generate unique IDs based on a combination of timestamp, datacenter ID, machine ID, and sequence number.
 */
public class SnowFlake {
    /**
     * Start timestamp for the Snowflake algorithm.
     */
    private final static long START_STAMP = 1701354128058L;

    /**
     * Number of bits allocated for the sequence.
     */
    private final static long SEQUENCE_BIT = 12;

    /**
     * Number of bits allocated for the machine ID.
     */
    private final static long MACHINE_BIT = 5;

    /**
     * Number of bits allocated for the datacenter ID.
     */
    private final static long DATACENTER_BIT = 5;

    /**
     * Maximum number of datacenters.
     */
    private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);

    /**
     * Maximum number of machines.
     */
    private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);

    /**
     * Maximum sequence number.
     */
    private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);

    /**
     * Left shift bits for machine ID.
     */
    private final static long MACHINE_LEFT = SEQUENCE_BIT;

    /**
     * Left shift bits for datacenter ID.
     */
    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;

    /**
     * Left shift bits for timestamp.
     */
    private final static long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;

    /**
     * Datacenter ID.
     */
    private final long datacenterId;

    /**
     * Machine ID.
     */
    private final long machineId;

    /**
     * Sequence number.
     */
    private long sequence = 0L;

    /**
     * Last timestamp used for generating an ID.
     */
    @VisibleForTesting
    long lastStamp = -1L;

    /**
     * Constructs a new SnowFlake instance with the specified datacenter ID and machine ID.
     *
     * @param datacenterId the datacenter ID
     * @param machineId    the machine ID
     * @throws IllegalArgumentException if the datacenter ID or machine ID is out of the allowed range
     */
    public SnowFlake(long datacenterId, long machineId) {
        if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
            throw new IllegalArgumentException("Parameter datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
        }
        if (machineId > MAX_MACHINE_NUM || machineId < 0) {
            throw new IllegalArgumentException("Parameter machineId can't be greater than MAX_MACHINE_NUM or less than 0");
        }
        this.datacenterId = datacenterId;
        this.machineId = machineId;
    }

    /**
     * Last generated ID for synchronization comparison.
     */
    private long lastNextId;

    /**
     * Returns the last generated ID.
     *
     * @return the last generated ID
     */
    public long getLastNextId() {
        return lastNextId;
    }

    /**
     * Generates the next unique ID.
     *
     * @return the next unique ID
     * @throws RuntimeException if the system clock moves backwards
     */
    public long nextId() {
        long currentStamp = getNewStamp();
        if (currentStamp < lastStamp) {
            throw new RuntimeException("Clock moved backwards. refusing to generate id");
        }

        if (currentStamp == lastStamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            // reach the max sequence
            if (sequence == 0L) {
                currentStamp = getNextMill();
            }
        } else {
            // next mill
            sequence = 0L;
        }

        lastStamp = currentStamp;

        lastNextId = (currentStamp - START_STAMP) << TIMESTAMP_LEFT
                | datacenterId << DATACENTER_LEFT
                | machineId << MACHINE_LEFT
                | sequence;
        return lastNextId;
    }

    /**
     * Waits until the next millisecond and returns the new timestamp.
     *
     * @return the new timestamp
     */
    private long getNextMill() {
        long mill = getNewStamp();
        while (mill <= lastStamp) {
            mill = getNewStamp();
        }
        return mill;
    }

    /**
     * Returns the current timestamp.
     *
     * @return the current timestamp
     */
    private long getNewStamp() {
        return System.currentTimeMillis();
    }

    /**
     * Checks if the given ID is expired based on the specified number of seconds from now and a comparison timestamp.
     *
     * @param id             the ID to check
     * @param secondsFromNow the number of seconds from now
     * @param compareStamp   the comparison timestamp
     * @return true if the ID is expired, false otherwise
     */
    public static boolean isExpired(long id, int secondsFromNow, long compareStamp) {
        if (secondsFromNow <= 0) {
            return false;
        }

        return (compareStamp - secondsFromNow * 1000L) - ((id >> TIMESTAMP_LEFT) + START_STAMP) > 0;
    }

    /**
     * Checks if the given ID is expired based on the specified number of seconds from now.
     *
     * @param id             the ID to check
     * @param secondsFromNow the number of seconds from now
     * @return true if the ID is expired, false otherwise
     */
    @TestOnly
    static boolean isExpired(long id, int secondsFromNow) {
        return isExpired(id, secondsFromNow, System.currentTimeMillis());
    }
}