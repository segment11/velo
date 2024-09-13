package io.velo;

public class ConfForGlobal {
    private ConfForGlobal() {
    }

    public static long datacenterId;
    public static long machineId;
    public static long estimateKeyNumber;
    private static final int DEFAULT_ESTIMATE_ONE_VALUE_LENGTH = 200;
    public static int estimateOneValueLength = DEFAULT_ESTIMATE_ONE_VALUE_LENGTH;
    static final int MAX_ESTIMATE_ONE_VALUE_LENGTH = 4000;

    public static final int JEDIS_POOL_CONNECT_TIMEOUT_MILLIS = 1000;
    public static String PASSWORD;

    // compression
    public static boolean isValueSetUseCompression = true;
    public static boolean isOnDynTrainDictForCompression = true;

    public static String netListenAddresses;

    public static String dirPath = "/tmp/velo-data";
    public static boolean pureMemory = false;
    public static short slotNumber = 1;
    public static byte netWorkers = 1;
    public static byte indexWorkers = 1;
    public static int eventLoopIdleMillis = 10;

    // for repl leader select
    public static String zookeeperConnectString;
    // also as sentinel master name
    public static String zookeeperRootPath;
    public static boolean canBeLeader = true;
    // for cascade replication
    public static boolean isAsSlaveOfSlave = false;

    public static final int REPL_FAILOVER_SLAVE_WAIT_SECONDS = 5;

    public static final String LEADER_LATCH_PATH = "/leader_latch";

    public static String targetAvailableZone;

    public static boolean clusterEnabled = false;

    // for jedis
    public static int jedisPoolMaxTotal = 10;
    public static int jedisPoolMaxIdle = 5;
    public static long jedisPoolMaxWaitMillis = 5000;
}
