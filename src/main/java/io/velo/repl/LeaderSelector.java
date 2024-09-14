package io.velo.repl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.command.PGroup;
import io.velo.command.XGroup;
import io.velo.persist.LocalPersist;
import io.velo.repl.support.JedisPoolHolder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LeaderSelector implements NeedCleanUp {
    // singleton
    private LeaderSelector() {
    }

    private static final LeaderSelector instance = new LeaderSelector();

    public static LeaderSelector getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(LeaderSelector.class);

    private CuratorFramework client;

    public CuratorFramework getClient() {
        return client;
    }

    synchronized boolean connect() {
        var connectString = ConfForGlobal.zookeeperConnectString;
        if (connectString == null) {
            log.debug("Repl zookeeper connect string is null, leader select will not work");
            return false;
        }

        if (client != null) {
            log.warn("Repl zookeeper client already started, connect string={}", connectString);
            return true;
        }

        client = CuratorFrameworkFactory.builder().
                connectString(connectString).
                sessionTimeoutMs(ConfForGlobal.zookeeperSessionTimeoutMs).
                connectionTimeoutMs(ConfForGlobal.zookeeperConnectionTimeoutMs).
                retryPolicy(new ExponentialBackoffRetry(1000, 3)).
                build();
        client.start();
        log.info("Repl zookeeper client started, connect string={}", connectString);
        return true;
    }

    @VisibleForTesting
    boolean isConnected() {
        return client != null && client.getZookeeperClient().isConnected();
    }

    @VisibleForTesting
    long isLeaderLoopCount = 0;

    @TestOnly
    public String getMasterAddressLocalMocked() {
        return masterAddressLocalMocked;
    }

    @TestOnly
    public void setMasterAddressLocalMocked(String masterAddressLocalMocked) {
        this.masterAddressLocalMocked = masterAddressLocalMocked;
    }

    @TestOnly
    private String masterAddressLocalMocked;

    @TestOnly
    String tryConnectAndGetMasterListenAddress() {
        return tryConnectAndGetMasterListenAddress(true);
    }

    @VisibleForTesting
    boolean hasLeadershipLastTry;

    public String tryConnectAndGetMasterListenAddress(boolean doStartLeaderLatch) {
        if (masterAddressLocalMocked != null) {
            return masterAddressLocalMocked;
        }

        if (!isConnected()) {
            boolean isConnectOk = connect();
            if (!isConnectOk) {
                return null;
            }
        }

        if (!ConfForGlobal.canBeLeader) {
            return getMasterListenAddressAsSlave();
        }

        var isStartOk = !doStartLeaderLatch || startLeaderLatch();
        if (!isStartOk) {
            return null;
        }

        if (hasLeadership()) {
            isLeaderLoopCount++;
            if (isLeaderLoopCount % 100 == 0) {
                log.info("Repl self is leader, loop count={}", isLeaderLoopCount);
            }

            if (!hasLeadershipLastTry) {
                log.warn("Repl self become leader, {}", ConfForGlobal.netListenAddresses);
            }
            hasLeadershipLastTry = true;

            return ConfForGlobal.netListenAddresses;
        } else {
            isLeaderLoopCount = 0;
            if (hasLeadershipLastTry) {
                log.warn("Repl self lost leader, {}", ConfForGlobal.netListenAddresses);
            }
            hasLeadershipLastTry = false;

            return getMasterListenAddressAsSlave();
        }
    }

    @TestOnly
    private Boolean hasLeadershipLocalMocked;

    @TestOnly
    public void setHasLeadershipLocalMocked(Boolean hasLeadershipLocalMocked) {
        this.hasLeadershipLocalMocked = hasLeadershipLocalMocked;
    }

    public boolean hasLeadership() {
        if (hasLeadershipLocalMocked != null) {
            return hasLeadershipLocalMocked;
        }

        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    private String lastGetMasterListenAddressAsSlave;

    public String getLastGetMasterListenAddressAsSlave() {
        return lastGetMasterListenAddressAsSlave;
    }

    private String getMasterListenAddressAsSlave() {
        if (leaderLatch == null) {
            return null;
        }

        try {
            var latchPath = ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LATCH_PATH;
            var children = client.getChildren().forPath(latchPath);
            if (children.isEmpty()) {
                return null;
            }

            // sort by suffix, smaller is master
            children.sort((o1, o2) -> {
                var lastIndex1 = o1.lastIndexOf("-");
                var lastIndex2 = o2.lastIndexOf("-");
                return o1.substring(lastIndex2).compareTo(o2.substring(lastIndex1));
            });

            var dataBytes = client.getData().forPath(latchPath + "/" + children.getFirst());
            var listenAddress = new String(dataBytes);
            lastGetMasterListenAddressAsSlave = listenAddress;
            log.debug("Repl get master listen address from zookeeper={}", listenAddress);
            return listenAddress;
        } catch (Exception e) {
            lastGetMasterListenAddressAsSlave = null;
            // need not stack trace
            log.error("Repl get master listen address from zookeeper failed={}", e.getMessage());
            return null;
        }
    }

    synchronized void disconnect() {
        if (client != null) {
            client.close();
            System.out.println("Repl zookeeper client closed");
            client = null;
        }
    }

    private LeaderLatch leaderLatch;

    @TestOnly
    boolean startLeaderLatchFailMocked;

    synchronized boolean startLeaderLatch() {
        if (startLeaderLatchFailMocked) {
            return false;
        }

        if (leaderLatch != null) {
            log.debug("Repl leader latch already started");
            return true;
        }

        if (client == null) {
            log.error("Repl leader latch start failed: client is null");
            return false;
        }

        // client must not be null
        // local listen address as id
        leaderLatch = new LeaderLatch(client, ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LATCH_PATH,
                ConfForGlobal.netListenAddresses);
        try {
            leaderLatch.start();
            log.info("Repl leader latch started and wait 5s");
            leaderLatch.await(5, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            // need not stack trace
            log.error("Repl leader latch start failed={}", e.getMessage());
            return false;
        }
    }

    private long lastStopLeaderLatchTimeMillis;

    public long getLastStopLeaderLatchTimeMillis() {
        return lastStopLeaderLatchTimeMillis;
    }

    public synchronized void stopLeaderLatch() {
        if (leaderLatch != null) {
            try {
                leaderLatch.close();
                System.out.println("Repl leader latch closed");
                leaderLatch = null;
                lastStopLeaderLatchTimeMillis = System.currentTimeMillis();
            } catch (Exception e) {
                // need not stack trace
                System.err.println("Repl leader latch close failed=" + e.getMessage());
            }
        }
    }

    @Override
    public void cleanUp() {
        stopLeaderLatch();
        disconnect();
    }

    public long getLastResetAsMasterTimeMillis() {
        return lastResetAsMasterTimeMillis;
    }

    public long getLastResetAsSlaveTimeMillis() {
        return lastResetAsSlaveTimeMillis;
    }

    private long lastResetAsMasterTimeMillis;
    private long lastResetAsSlaveTimeMillis;

    public void resetAsMaster(Consumer<Exception> callback) {
        resetAsMaster(false, callback);
    }

    @VisibleForTesting
    long resetAsMasterCount = 0;
    @VisibleForTesting
    long resetAsSlaveCount = 0;

    public void resetAsMaster(boolean force, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as master"));
            return;
        }

        lastResetAsMasterTimeMillis = System.currentTimeMillis();

        var localPersist = LocalPersist.getInstance();

        Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
        for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncRun(() -> {
                boolean canResetSelfAsMasterNow = false;

                var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
                if (replPairAsSlave != null) {
                    if (replPairAsSlave.isMasterCanNotConnect()) {
                        canResetSelfAsMasterNow = true;
                    } else {
                        if (replPairAsSlave.isMasterReadonly() && replPairAsSlave.isAllCaughtUp()) {
                            canResetSelfAsMasterNow = true;
                        }
                    }
                } else {
                    canResetSelfAsMasterNow = true;
                    log.debug("Repl old repl pair as slave is null, slot={}", oneSlot.slot());
                }

                if (!force && !canResetSelfAsMasterNow) {
                    log.warn("Repl slave can not reset as master now, need wait current master readonly and slave all caught up, slot={}",
                            replPairAsSlave.getSlot());
                    XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPairAsSlave, null);
                    throw new IllegalStateException("Repl slave can not reset as master, slot=" + replPairAsSlave.getSlot());
                }

                resetAsSlaveCount = 0;

                var isSelfSlave = oneSlot.removeReplPairAsSlave();
                if (isSelfSlave) {
                    oneSlot.resetAsMaster();
                    resetAsMasterCount = 0;
                } else {
                    resetAsMasterCount++;
                    if (resetAsMasterCount % 100 == 0) {
                        log.info("Repl reset as master, is already master, do nothing, slot={}", oneSlot.slot());
                    }
                }

                if (oneSlot.slot() == 0) {
                    localPersist.getIndexHandlerPool().resetAsMaster();
                }
            });
        }

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            // publish switch master to clients
            var oldMasterHostAndPort = ReplPair.parseHostAndPort(LeaderSelector.getInstance().lastGetMasterListenAddressAsSlave);
            var selfAsMasterHostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses);
            if (oldMasterHostAndPort == null) {
                oldMasterHostAndPort = selfAsMasterHostAndPort;
            }
            publishMasterSwitchMessage(oldMasterHostAndPort, selfAsMasterHostAndPort, true);

            callback.accept(null);
        });
    }

    private boolean checkMasterConfigMatch(String host, int port, Consumer<Exception> callback) {
        log.debug("Repl reset self as slave begin, check new master global config first, self={}", ConfForGlobal.netListenAddresses);
        // sync, perf bad
        try {
            var jedisPool = JedisPoolHolder.getInstance().create(host, port);
            // may be null
            var jsonStr = JedisPoolHolder.exe(jedisPool, jedis -> {
                var pong = jedis.ping();
                log.debug("Repl slave of {}:{} pong={}", host, port, pong);
                return jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," + XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD);
            });

            var map = ConfForSlot.global.slaveCanMatchCheckValues();
            var objectMapper = new ObjectMapper();
            var jsonStrLocal = objectMapper.writeValueAsString(map);

            if (!jsonStrLocal.equals(jsonStr)) {
                log.warn("Repl reset self as slave begin, check new master global config fail, self={}", ConfForGlobal.netListenAddresses);
                log.info("Repl local={}", jsonStrLocal);
                log.info("Repl remote={}", jsonStr);
                callback.accept(new IllegalStateException("Repl slave can not match check values"));
                return false;
            } else {
                log.debug("Repl reset self as slave begin, check new master global config ok, self={}", ConfForGlobal.netListenAddresses);
                return true;
            }
        } catch (Exception e) {
            callback.accept(e);
            return false;
        }
    }

    public void resetAsSlave(String host, int port, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as slave"));
            return;
        }

        lastResetAsSlaveTimeMillis = System.currentTimeMillis();

        var checkMasterConfigMatch = checkMasterConfigMatch(host, port, callback);
        if (!checkMasterConfigMatch) {
            // already callback handle with exception
            return;
        }

        var localPersist = LocalPersist.getInstance();

        Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
        for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncRun(() -> {
                var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
                if (replPairAsSlave != null) {
                    if (replPairAsSlave.getHost().equals(host) && replPairAsSlave.getPort() == port) {
                        log.debug("Repl old repl pair as slave is same as new master, slot={}", oneSlot.slot());
                        return;
                    } else {
                        oneSlot.removeReplPairAsSlave();
                    }
                }

                oneSlot.resetAsSlave(host, port);

                resetAsMasterCount = 0;
                resetAsSlaveCount++;

                if (oneSlot.slot() == 0) {
                    localPersist.getIndexHandlerPool().resetAsSlave();
                }
            });
        }

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            // publish switch master to clients
            var selfAsOldMasterHostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses);
            var newMasterHostAndPort = new ReplPair.HostAndPort(host, port);
            publishMasterSwitchMessage(selfAsOldMasterHostAndPort, newMasterHostAndPort, false);

            callback.accept(null);
        });
    }

    private static final byte[] PUBLISH_CMD_BYTES = "publish".getBytes();

    private void publishMasterSwitchMessage(ReplPair.HostAndPort from, ReplPair.HostAndPort to, boolean isAsMaster) {
        // publish master address to clients
        var publishMessage = ConfForGlobal.zookeeperRootPath + " " + from.host() + " " + from.port() + " " +
                to.host() + " " + to.port();

        var data = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessage.getBytes()};
        PGroup.publish(data);

        var doLog = isAsMaster ? resetAsMasterCount % 100 == 0 : resetAsSlaveCount % 100 == 0;
        if (doLog) {
            log.warn("Repl publish master switch message={}, reset as master loop count={}", publishMessage, resetAsMasterCount);
        }

        // publish slave address to clients for readonly slave
        var publishMessageReadonlySlave = ConfForGlobal.zookeeperRootPath + ReplConsts.REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX + " " +
                to.host() + " " + to.port() + " " + from.host() + " " + from.port();
        var dataSlave = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessageReadonlySlave.getBytes()};
        PGroup.publish(dataSlave);
        if (doLog) {
            log.warn("Repl publish master switch message for readonly slave={}", publishMessageReadonlySlave);
        }
    }

    public String getFirstSlaveListenAddressByMasterHostAndPort(String host, int port, short slot) {
        if (masterAddressLocalMocked != null) {
            return masterAddressLocalMocked;
        }

        var jedisPool = JedisPoolHolder.getInstance().create(host, port);
        return JedisPoolHolder.exe(jedisPool, jedis ->
                // refer to XGroup handle
                // key will be transferred to x_repl slot 0 get_first_slave_listen_address, refer to request handler
                jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ",slot," + slot + "," +
                        XGroup.X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD)
        );
    }
}
