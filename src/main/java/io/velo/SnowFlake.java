package io.velo;

import org.jetbrains.annotations.VisibleForTesting;

public class SnowFlake {
    private final static long START_STAMP = 1701354128058L;

    private final static long SEQUENCE_BIT = 12;
    private final static long MACHINE_BIT = 5;
    private final static long DATACENTER_BIT = 5;

    private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);
    private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);
    private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);

    private final static long MACHINE_LEFT = SEQUENCE_BIT;
    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
    private final static long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;

    private final long datacenterId;
    private final long machineId;
    private long sequence = 0L;
    @VisibleForTesting
    long lastStamp = -1L;

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

    // for sync compare
    private long lastNextId;

    public long getLastNextId() {
        return lastNextId;
    }

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

    private long getNextMill() {
        long mill = getNewStamp();
        while (mill <= lastStamp) {
            mill = getNewStamp();
        }
        return mill;
    }

    private long getNewStamp() {
        return System.currentTimeMillis();
    }

    public static boolean isExpired(long id, int secondsFromNow) {
        if (secondsFromNow <= 0) {
            return false;
        }

        return (System.currentTimeMillis() - secondsFromNow * 1000L) - ((id >> TIMESTAMP_LEFT) + START_STAMP) > 0;
    }
}