package io.velo;

import java.util.HashMap;

public class ConfForGlobal {
    private ConfForGlobal() {
    }

    public static long datacenterId;
    public static long machineId;
    public static long estimateKeyNumber;
    // 1%
    public static int keyAnalysisNumberPercent = 1;

    public static final int JEDIS_POOL_CONNECT_TIMEOUT_MILLIS = 1000;
    public static String PASSWORD;

    // compression
    public static boolean isValueSetUseCompression = true;
    public static boolean isOnDynTrainDictForCompression = false;
    public static boolean isPureMemoryModeKeyBucketsUseCompression = false;

    public static String netListenAddresses;
    public static boolean isUseDirectIO;

    public static String dirPath = "/tmp/velo-data";
    public static boolean pureMemory = false;
    public static boolean pureMemoryV2 = false;
    public static short slotNumber = 1;
    public static byte netWorkers = 1;
    public static byte indexWorkers = 1;
    public static int eventLoopIdleMillis = 10;

    // for repl leader select
    public static String zookeeperConnectString;
    public static int zookeeperSessionTimeoutMs = 30000;
    public static int zookeeperConnectionTimeoutMs = 10000;

    // also as sentinel master name
    public static String zookeeperRootPath;
    public static boolean canBeLeader = true;
    // for cascade replication
    public static boolean isAsSlaveOfSlave = false;

    public static final int REPL_FAILOVER_SLAVE_WAIT_SECONDS = 5;

    public static final String LEADER_LATCH_NODE_NAME = "leader_latch";
    public static final String LEADER_LATCH_PATH = "/" + LEADER_LATCH_NODE_NAME;

    public static String targetAvailableZone;

    public static boolean clusterEnabled = false;

    // for jedis
    public static int jedisPoolMaxTotal = 10;
    public static int jedisPoolMaxIdle = 5;
    public static long jedisPoolMaxWaitMillis = 5000;

    // for type
    public static int doubleScale = 2;

    // dyn-config
    public static final HashMap<String, String> initDynConfigItems = new HashMap<>();
}
