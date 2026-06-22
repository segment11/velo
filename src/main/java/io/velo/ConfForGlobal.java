package io.velo;

import io.velo.acl.AclUsers;
import io.velo.acl.U;

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
    public static long datacenterId = 0L;

    /**
     * For snowflake ID generator.
     * Identifier for the machine.
     */
    public static long machineId = 0L;

    /**
     * Estimated number of keys in one slot.
     */
    public static long estimateKeyNumber = 1_000_000L;

    /**
     * Estimated length of one value.
     */
    public static int estimateOneValueLength = 200;

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
    public static boolean isOnDynTrainDictForCompression = true;

    /**
     * Network listen addresses.
     */
    public static String netListenAddresses = "localhost:7379";

    /**
     * Directory path for data storage (default is "/tmp/velo-data").
     */
    public static String dirPath = "/tmp/velo-data";

    /**
     * Number of slots (default is 1).
     */
    public static short slotNumber = 1;

    /**
     * Number of network workers (default is 1).
     */
    public static byte netWorkers = 1;

    /**
     * Number of slot workers (default is 1).
     */
    public static byte slotWorkers = 1;

    /**
     * Number of index workers (default is 1).
     */
    public static byte indexWorkers = 1;

    /**
     * Idle time in milliseconds for event loops (default is 10 ms).
     */
    public static int eventloopIdleMillis = 10;

    /**
     * Zookeeper connection string.
     */
    public static String zookeeperConnectString;

    /**
     * Session timeout for Zookeeper in milliseconds (default is 30_000 ms).
     */
    public static int zookeeperSessionTimeoutMs = 30000;

    /**
     * Connection timeout for Zookeeper in milliseconds (default is 10_000 ms).
     */
    public static int zookeeperConnectionTimeoutMs = 10000;

    /**
     * Root path for Zookeeper (also used as sentinel master name).
     */
    public static String zookeeperRootPath;

    /**
     * When {@code true}, this instance is managed by an external Redis Sentinel process.
     * Sentinel owns topology decisions in this mode; ZooKeeper leader election must be disabled
     * and {@link #zookeeperConnectString} must not be set.
     */
    public static boolean sentinelModeEnabled = false;

    /**
     * Master name advertised to an external Redis Sentinel. Decoupled from
     * {@link #zookeeperRootPath} so Sentinel-managed deployments do not collide with the
     * existing ZooKeeper-based cluster name.
     */
    public static String sentinelMasterName = "mymaster";

    /**
     * IP address that this instance advertises to Redis Sentinel as the reachable host.
     * Defaults to {@code null}, which falls back to {@link #netListenAddresses}.
     */
    public static String replicaAnnounceIp;

    /**
     * Port that this instance advertises to Redis Sentinel as the reachable port.
     * Defaults to {@code 0}, which falls back to the port in {@link #netListenAddresses}.
     */
    public static int replicaAnnouncePort = 0;

    /**
     * Replica priority reported to Redis Sentinel via {@code INFO replication}.
     * Higher value wins Sentinel election; default 100 matches Redis.
     */
    public static int sentinelReplicaPriority = 100;

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
     * Size for big strings to avoid memory copy (default is 256 KB).
     */
    public static int bigStringNoMemoryCopySize = 1024 * 256;

    /**
     * Minimum size for big strings to avoid compression (default is 256 KB).
     * Compress big strings cost too much time, perf bad.
     */
    public static int bigStringNoCompressMinSize = 1024 * 256;

    /**
     * Minimum value length to be considered a big key for monitoring (default is 2048 bytes).
     */
    public static int bigKeyLengthCheckMinSize = 2048;

    /**
     * Initial dynamic configuration items.
     */
    public static final HashMap<String, String> initDynConfigItems = new HashMap<>();

    /**
     * Check if the configuration is valid.
     */
    public static void checkIfValid() {
        if (estimateKeyNumber > 10_000_000) {
            throw new IllegalArgumentException("Estimate key number must be less or equal 10_000_000");
        }

        if (keyAnalysisNumberPercent < 1 || keyAnalysisNumberPercent > 100) {
            throw new IllegalArgumentException("Key analysis number percent must be between 1 and 100");
        }

        if (sentinelReplicaPriority < 0) {
            throw new IllegalArgumentException("Sentinel replica priority must be >= 0");
        }

        if (sentinelModeEnabled && zookeeperConnectString != null) {
            throw new IllegalArgumentException(
                    "Sentinel mode and ZooKeeper mode are mutually exclusive: leave zookeeperConnectString unset when sentinelModeEnabled=true");
        }

        if (ConfForGlobal.PASSWORD != null) {
            var aclUsers = AclUsers.getInstance();
            aclUsers.upInsert(U.DEFAULT_USER, u -> u.setPassword(U.Password.plain(ConfForGlobal.PASSWORD)));
        }
    }

    /**
     * Resolve the externally reachable host:port this node should advertise to Redis Sentinel.
     * Prefers {@link #replicaAnnounceIp} / {@link #replicaAnnouncePort} when set, otherwise
     * falls back to the host:port parsed out of {@link #netListenAddresses}.
     *
     * @return [announcedHost, announcedPort]
     */
    public static String[] getAnnouncedHostAndPort() {
        var announcedIp = replicaAnnounceIp;
        int announcedPort = replicaAnnouncePort;
        String host = null;
        int port = 0;
        if (netListenAddresses != null) {
            var idx = netListenAddresses.lastIndexOf(':');
            if (idx > 0 && idx < netListenAddresses.length() - 1) {
                host = netListenAddresses.substring(0, idx);
                try {
                    port = Integer.parseInt(netListenAddresses.substring(idx + 1));
                } catch (NumberFormatException ignored) {
                    // fall through with port=0
                }
            } else {
                host = netListenAddresses;
            }
        }
        if (announcedIp == null || announcedIp.isEmpty()) {
            announcedIp = host;
        }
        if (announcedPort <= 0) {
            announcedPort = port;
        }
        return new String[]{announcedIp, Integer.toString(announcedPort)};
    }
}
