package io.velo.repl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.HostAndPort;
import io.velo.MultiWorkerServer;
import io.velo.NeedCleanUp;
import io.velo.command.PGroup;
import io.velo.command.XGroup;
import io.velo.persist.LocalPersist;
import io.velo.repl.support.JedisPoolHolder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Leader selector using ZooKeeper for master election.
 */
public class LeaderSelector implements NeedCleanUp {
    // singleton
    private LeaderSelector() {
    }

    private static final LeaderSelector instance = new LeaderSelector();

    /**
     * Returns the singleton instance.
     *
     * @return the singleton LeaderSelector instance
     */
    public static LeaderSelector getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(LeaderSelector.class);

    private CuratorFramework client;

    /**
     * Returns the underlying ZooKeeper client.
     *
     * @return the CuratorFramework client, or null if not connected
     */
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

    /**
     * Returns the locally mocked master address (test only).
     *
     * @return the mocked master address
     */
    @TestOnly
    public String getMasterAddressLocalMocked() {
        return masterAddressLocalMocked;
    }

    /**
     * Sets the locally mocked master address (test only).
     *
     * @param masterAddressLocalMocked the mocked master address
     */
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

    /**
     * Tries to connect to ZooKeeper and returns the current master's listen address.
     *
     * @param doStartLeaderLatch whether to start the leader latch if not yet started
     * @return the master's listen address, or null if it cannot be determined
     */
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
                log.warn("Repl self become leader, {}", ConfForGlobal.announcedHostPortString());
            }
            hasLeadershipLastTry = true;

            // The address published to ZK as "the cluster master" — peers read this to connect
            // as slaves, so it must be the externally-reachable announced address, not the raw
            // bind address (which is typically 0.0.0.0).
            return ConfForGlobal.announcedHostPortString();
        } else {
            isLeaderLoopCount = 0;
            if (hasLeadershipLastTry) {
                log.warn("Repl self lost leader, {}", ConfForGlobal.netListenAddress);
            }
            hasLeadershipLastTry = false;

            return getMasterListenAddressAsSlave();
        }
    }

    @TestOnly
    private Boolean hasLeadershipLocalMocked;

    /**
     * Sets the locally mocked leadership state (test only).
     *
     * @param hasLeadershipLocalMocked the mocked leadership state
     */
    @TestOnly
    public void setHasLeadershipLocalMocked(Boolean hasLeadershipLocalMocked) {
        this.hasLeadershipLocalMocked = hasLeadershipLocalMocked;
    }

    /**
     * Returns whether this node currently holds the leadership (is the master).
     *
     * @return true if this node is the leader
     */
    public synchronized boolean hasLeadership() {
        if (hasLeadershipLocalMocked != null) {
            return hasLeadershipLocalMocked;
        }

        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    private String lastGetMasterListenAddressAsSlave;

    /**
     * Returns the last master listen address observed when acting as a slave.
     *
     * @return the last master listen address as a slave, or null
     */
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
            // ZooKeeper uses 10-digit zero-padded sequence numbers (e.g., "0000000001", "0000000010")
            // so string comparison correctly orders by numeric value
            children.sort((o1, o2) -> {
                var lastIndex1 = o1.lastIndexOf("-");
                var lastIndex2 = o2.lastIndexOf("-");
                return o1.substring(lastIndex1).compareTo(o2.substring(lastIndex2));
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
                ConfForGlobal.netListenAddress);
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

    /**
     * Returns the timestamp when the leader latch was last stopped.
     *
     * @return the timestamp in milliseconds
     */
    public long getLastStopLeaderLatchTimeMillis() {
        return lastStopLeaderLatchTimeMillis;
    }

    /**
     * Stops the leader latch if it is running.
     */
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

    /**
     * Returns the timestamp when this node was last reset as master.
     *
     * @return the timestamp in milliseconds
     */
    public long getLastResetAsMasterTimeMillis() {
        return lastResetAsMasterTimeMillis;
    }

    /**
     * Returns the timestamp when this node was last reset as slave.
     *
     * @return the timestamp in milliseconds
     */
    public long getLastResetAsSlaveTimeMillis() {
        return lastResetAsSlaveTimeMillis;
    }

    private long lastResetAsMasterTimeMillis;
    private long lastResetAsSlaveTimeMillis;

    /**
     * Resets this node as master and invokes the callback when done.
     *
     * @param callback the callback invoked with an exception on failure, or null on success
     */
    public void resetAsMaster(Consumer<Exception> callback) {
        resetAsMaster(false, callback);
    }

    @VisibleForTesting
    long resetAsMasterCount = 0;
    @VisibleForTesting
    long resetAsSlaveCount = 0;

    /**
     * Resets this node as master, optionally forcing the transition.
     *
     * @param force    whether to force the reset even if the slave has not caught up
     * @param callback the callback invoked with an exception on failure, or null on success
     */
    public void resetAsMaster(boolean force, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as master"));
            return;
        }

        lastResetAsMasterTimeMillis = System.currentTimeMillis();

        // Sentinel failover state observability. The actual reset happens when the per-slot
        // async work completes, so the state is set/cleared inside the whenComplete callback
        // — otherwise a synchronous finally would clear the state before Sentinel can observe it.
        var prevFailoverState = MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState;
        MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = "promotion-in-progress";
        doResetAsMaster(force, prevFailoverState, callback);
    }

    private void doResetAsMaster(boolean force, String prevFailoverState, Consumer<Exception> callback) {
        // Save masterSlotNumber for restore on failure; cleared to 0 only on overall success so
        // in-loop isAsSlaveScaleUp() checks still see scale-up mode during the transition.
        var prevMasterSlotNumber = ConfForGlobal.masterSlotNumber;

        // Restore the failover state on any synchronous throw too — the whenComplete callback
        // below only runs if Promises.all is reached, so anything that throws before that
        // (e.g. localPersist.oneSlot returning null, asyncRun failing) would otherwise leave
        // masterFailoverState stuck at "promotion-in-progress" forever.
        try {
            var localPersist = LocalPersist.getInstance();

            Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
            for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
                var oneSlot = localPersist.oneSlot((short) i);
                promises[i] = oneSlot.asyncRun(() -> {
                    boolean canResetSelfAsMasterNow = false;

                    var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
                    if (replPairAsSlave != null) {
                        if (replPairAsSlave.isMasterReadonly() && replPairAsSlave.isAllCaughtUp()) {
                            canResetSelfAsMasterNow = true;
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
                    } else if (localPersist.isAsSlaveScaleUp()
                            && oneSlot.slot() >= ConfForGlobal.masterSlotNumber) {
                        // 2N scale-up extra slot: no slave ReplPair (data arrived via fan-out),
                        // but it IS in slave state (readonly, canRead=false). Promote it too.
                        // Known limitation: this slot has data but no binlog history (offset was zeroed
                        // at slave reset and never advanced) — downstream replication for this slot
                        // from the promoted node is not supported in v1.
                        log.warn("Repl promote scale-up extra slot to master (no binlog history), slot={}", oneSlot.slot());
                        oneSlot.resetAsMaster();
                        resetAsMasterCount = 0;
                    } else {
                        resetAsMasterCount++;
                        if (resetAsMasterCount % 100 == 0) {
                            log.info("Repl reset as master, is already master, do nothing, slot={}", oneSlot.slot());
                        }
                    }
                });
            }

            Promises.all(promises).whenComplete((r, e) -> {
                // Restore previous failover state once the role transition actually completed (or failed),
                // so Sentinel polling INFO replication can observe the transition window.
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;

                if (e != null) {
                    // Restore masterSlotNumber on failed promotion
                    ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
                    callback.accept(e);
                    return;
                }

                // Success: leaving slave mode, clear masterSlotNumber last
                ConfForGlobal.masterSlotNumber = 0;

                // publish switch master to clients
                var leaderSelector = LeaderSelector.getInstance();
                var oldMasterHostAndPort = HostAndPort.parse(leaderSelector.lastGetMasterListenAddressAsSlave);
                var selfAsMasterHostAndPort = ConfForGlobal.announcedHostPort();
                if (oldMasterHostAndPort == null) {
                    oldMasterHostAndPort = selfAsMasterHostAndPort;
                }
                publishMasterSwitchMessage(oldMasterHostAndPort, selfAsMasterHostAndPort, true);

                callback.accept(null);
            });
        } catch (RuntimeException e) {
            ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
            MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
            throw e;
        }
    }

    private @Nullable ConfForSlot.SlaveCheckValues checkMasterConfigMatch(String host, int port, Consumer<Exception> callback) {
        log.debug("Repl reset self as slave begin, check new master global config first, self={}", ConfForGlobal.netListenAddress);
        // sync, perf bad
        try {
            var jedisPoolHolder = JedisPoolHolder.getInstance();
            var jedisPool = jedisPoolHolder.createIfNotCached(host, port);
            // may be null
            var jsonStr = JedisPoolHolder.exe(jedisPool, jedis -> {
                var pong = jedis.ping();
                log.debug("Repl slave of {}:{} pong={}", host, port, pong);
                return jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," + XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD);
            });

            var objectMapper = new ObjectMapper();
            var checkValuesFromMaster = objectMapper.readValue(jsonStr, ConfForSlot.SlaveCheckValues.class);

            if (!ConfForSlot.global.slaveCanMatch(checkValuesFromMaster)) {
                log.warn("Repl reset self as slave begin, check new master global config fail, self={}", ConfForGlobal.netListenAddress);
                log.info("Repl local={}", ConfForSlot.global.getSlaveCheckValues());
                log.info("Repl remote={}", checkValuesFromMaster);
                callback.accept(new IllegalStateException("Repl slave can not match check values"));
                return null;
            } else {
                log.debug("Repl reset self as slave begin, check new master global config ok, self={}", ConfForGlobal.netListenAddress);
                return checkValuesFromMaster;
            }
        } catch (Exception e) {
            callback.accept(e);
            return null;
        }
    }

    /**
     * Resets this node as a slave of the given master host and port.
     *
     * @param host     the master host
     * @param port     the master port
     * @param callback the callback invoked with an exception on failure, or null on success
     */
    public void resetAsSlave(String host, int port, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as slave"));
            return;
        }

        lastResetAsSlaveTimeMillis = System.currentTimeMillis();

        // Sentinel demotion observability: this node is being demoted to a replica.
        // Like resetAsMaster, the actual transition is async; clear the state in whenComplete.
        var prevFailoverState = MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState;
        MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = "waiting-for-promotion";
        doResetAsSlave(host, port, prevFailoverState, callback);
    }

    private void doResetAsSlave(String host, int port, String prevFailoverState, Consumer<Exception> callback) {
        // Track the remote master's slot count so isAsSlaveScaleUp() can detect 2N mode.
        // Save the previous value for rollback on any failure path (declared before try so the
        // catch block can restore it even when the throw happens after the set).
        var prevMasterSlotNumber = ConfForGlobal.masterSlotNumber;

        // Same synchronous-throw guard as doResetAsMaster — anything that throws before the
        // whenComplete callback runs must still restore the failover state.
        try {
            var checkValuesFromMaster = checkMasterConfigMatch(host, port, callback);
            if (checkValuesFromMaster == null) {
                // already callback handle with exception — restore state so we don't get stuck
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                return;
            }

            ConfForGlobal.masterSlotNumber = (short) checkValuesFromMaster.getSlotNumber();

            var localPersist = LocalPersist.getInstance();

            // Reset the read gate unconditionally: scale-up mode sizes the array to masterSlotNumber;
            // equal-slot mode sizes it to 0 so all publish calls are no-ops and any stale array from a
            // prior 2N session is cleared.
            localPersist.resetScaleUpReadGate(localPersist.isAsSlaveScaleUp() ? ConfForGlobal.masterSlotNumber : 0);

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

                    var masterSlotNumberForStream = ConfForGlobal.masterSlotNumber > 0
                            ? ConfForGlobal.masterSlotNumber : ConfForGlobal.slotNumber;
                    boolean openReplStream = oneSlot.slot() < masterSlotNumberForStream;
                    oneSlot.resetAsSlave(host, port, openReplStream);

                    resetAsMasterCount = 0;
                    resetAsSlaveCount++;
                });
            }

            Promises.all(promises).whenComplete((r, e) -> {
                // Restore previous failover state once the role transition actually completed (or failed),
                // so Sentinel polling INFO replication can observe the transition window.
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;

                if (e != null) {
                    // Rollback masterSlotNumber on failed per-slot reset
                    ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
                    callback.accept(e);
                    return;
                }

                // publish switch master to clients
                var selfAsOldMasterHostAndPort = ConfForGlobal.announcedHostPort();
                var newMasterHostAndPort = new HostAndPort(host, port);
                publishMasterSwitchMessage(selfAsOldMasterHostAndPort, newMasterHostAndPort, false);

                callback.accept(null);
            });
        } catch (RuntimeException e) {
            ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
            MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
            throw e;
        }
    }

    private static final byte[] PUBLISH_CMD_BYTES = "publish".getBytes();

    private void publishMasterSwitchMessage(HostAndPort from, HostAndPort to, boolean isAsMaster) {
        // publish master address to clients
        var publishMessage = ConfForGlobal.zookeeperRootPath + " " + from.host + " " + from.port + " " +
                to.host + " " + to.port;

        var data = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessage.getBytes()};
        PGroup.publish(data, null);

        var doLog = isAsMaster ? resetAsMasterCount % 100 == 0 : resetAsSlaveCount % 100 == 0;
        if (doLog) {
            log.warn("Repl publish master switch message={}, reset as master loop count={}", publishMessage, resetAsMasterCount);
        }

        // publish slave address to clients for readonly slave
        var publishMessageReadonlySlave = ConfForGlobal.zookeeperRootPath + ReplConsts.REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX + " " +
                to.host + " " + to.port + " " + from.host + " " + from.port;
        var dataSlave = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessageReadonlySlave.getBytes()};
        PGroup.publish(dataSlave, null);
        if (doLog) {
            log.warn("Repl publish master switch message for readonly slave={}", publishMessageReadonlySlave);
        }
    }

    /**
     * Queries the given master host and port for the first slave's listen address for the specified slot.
     *
     * @param host the master host
     * @param port the master port
     * @param slot the slot index
     * @return the first slave's listen address, or null if none
     */
    public String getFirstSlaveListenAddressByMasterHostAndPort(String host, int port, short slot) {
        if (masterAddressLocalMocked != null) {
            return masterAddressLocalMocked;
        }

        var jedisPoolHolder = JedisPoolHolder.getInstance();
        var jedisPool = jedisPoolHolder.createIfNotCached(host, port);
        return JedisPoolHolder.exe(jedisPool, jedis ->
                // refer to XGroup handle
                // key will be transferred to x_repl slot 0 get_first_slave_listen_address, refer to request handler
                jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ",slot," + slot + "," +
                        XGroup.X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD)
        );
    }
}
