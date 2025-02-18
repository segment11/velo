package io.velo;

import java.util.HashMap;

/**
 * Global configuration settings for the Velo application.
 * This class holds various configuration parameters used throughout the application.
 */
public class ConfForGlobal {
    /**
     * Private constructor to prevent instantiation.
     */
    private ConfForGlobal() {
    }

    /**
     * For snowflake ID generator.
     * Identifier for the datacenter.
     */
    public static long datacenterId;

    /**
     * For snowflake ID generator.
     * Identifier for the machine.
     */
    public static long machineId;

    /**
     * Estimated number of keys in one slot.
     */
    public static long estimateKeyNumber;

    /**
     * Percentage of keys to analyze for statistics (default is 1%).
     */
    public static int keyAnalysisNumberPercent = 1;

    /**
     * Connection timeout for Jedis pool in milliseconds (default is 1000 ms).
     */
    public static final int JEDIS_POOL_CONNECT_TIMEOUT_MILLIS = 1000;

    /**
     * Password for authentication.
     */
    public static String PASSWORD;

    /**
     * Flag to indicate if value sets should use compression.
     */
    public static boolean isValueSetUseCompression = true;

    /**
     * Flag to indicate if dynamic training dictionary should be used for compression.
     */
    public static boolean isOnDynTrainDictForCompression = false;

    /**
     * Flag to indicate if pure memory mode key buckets should use compression.
     */
    public static boolean isPureMemoryModeKeyBucketsUseCompression = false;

    /**
     * Network listen addresses.
     */
    public static String netListenAddresses;

    /**
     * Flag to indicate if direct I/O should be used.
     */
    public static boolean isUseDirectIO;

    /**
     * Directory path for data storage (default is "/tmp/velo-data").
     */
    public static String dirPath = "/tmp/velo-data";

    /**
     * Flag to indicate if pure memory mode is enabled.
     */
    public static boolean pureMemory = false;

    /**
     * Flag to indicate if pure memory mode version 2 is enabled.
     */
    public static boolean pureMemoryV2 = false;

    /**
     * Number of slots (default is 1).
     */
    public static short slotNumber = 1;

    /**
     * Number of network workers (default is 1).
     */
    public static byte netWorkers = 1;

    /**
     * Number of index workers (default is 1).
     */
    public static byte indexWorkers = 1;

    /**
     * Idle time in milliseconds for event loops (default is 10 ms).
     */
    public static int eventLoopIdleMillis = 10;

    /**
     * Zookeeper connection string.
     */
    public static String zookeeperConnectString;

    /**
     * Session timeout for Zookeeper in milliseconds (default is 30000 ms).
     */
    public static int zookeeperSessionTimeoutMs = 30000;

    /**
     * Connection timeout for Zookeeper in milliseconds (default is 10000 ms).
     */
    public static int zookeeperConnectionTimeoutMs = 10000;

    /**
     * Root path for Zookeeper (also used as sentinel master name).
     */
    public static String zookeeperRootPath;

    /**
     * Flag to indicate if this instance can be a leader.
     */
    public static boolean canBeLeader = true;

    /**
     * Flag to indicate if this instance is a slave of a slave.
     */
    public static boolean isAsSlaveOfSlave = false;

    /**
     * Wait time in seconds for a slave to become a leader during failover (default is 5 seconds).
     */
    public static final int REPL_FAILOVER_SLAVE_WAIT_SECONDS = 5;

    /**
     * Node name for leader latch.
     */
    public static final String LEADER_LATCH_NODE_NAME = "leader_latch";

    /**
     * Path for leader latch.
     */
    public static final String LEADER_LATCH_PATH = "/" + LEADER_LATCH_NODE_NAME;

    /**
     * Target available zone.
     */
    public static String targetAvailableZone;

    /**
     * Flag to indicate if cluster mode is enabled.
     */
    public static boolean clusterEnabled = false;

    /**
     * Maximum total number of connections in Jedis pool (default is 10).
     */
    public static int jedisPoolMaxTotal = 10;

    /**
     * Maximum number of idle connections in Jedis pool (default is 5).
     */
    public static int jedisPoolMaxIdle = 5;

    /**
     * Maximum wait time in milliseconds for a connection from Jedis pool (default is 5000 ms).
     */
    public static long jedisPoolMaxWaitMillis = 5000;

    /**
     * Scale factor for double values (default is 2).
     */
    public static int doubleScale = 2;

    /**
     * Initial dynamic configuration items.
     */
    public static final HashMap<String, String> initDynConfigItems = new HashMap<>();
}
