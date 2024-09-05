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

    synchronized boolean connect() {
        var connectString = ConfForGlobal.zookeeperConnectString;
        if (connectString == null) {
            log.debug("Repl zookeeper connect string is null, leader select will not work");
            return false;
        }

        if (client != null) {
            log.warn("Repl zookeeper client already started, connect string: {}", connectString);
            return true;
        }

        client = CuratorFrameworkFactory.newClient(connectString,
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        log.info("Repl zookeeper client started, connect string: {}", connectString);
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
                log.info("Repl self is leader, loop count: {}", isLeaderLoopCount);
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
            log.debug("Repl get master listen address from zookeeper: {}", listenAddress);
            return listenAddress;
        } catch (Exception e) {
            lastGetMasterListenAddressAsSlave = null;
            // need not stack trace
            log.error("Repl get master listen address from zookeeper failed: " + e.getMessage());
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
            log.error("Repl leader latch start failed: " + e.getMessage());
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
                System.err.println("Repl leader latch close failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void cleanUp() {
        stopLeaderLatch();
        disconnect();
    }

    // run in primary eventloop
    public void resetAsMaster(boolean returnExceptionIfAlreadyIsMaster, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as master"));
            return;
        }

        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        var firstOneSlot = localPersist.oneSlots()[0];
        var pp = firstOneSlot.asyncCall(firstOneSlot::isAsSlave);

        pp.whenComplete((isAsSlave, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            if (!isAsSlave) {
                // already is master
                if (returnExceptionIfAlreadyIsMaster) {
                    callback.accept(new IllegalStateException("Repl already is master"));
                } else {
                    callback.accept(null);
                }
                return;
            }

            log.warn("Repl reset self as master, {}", ConfForGlobal.netListenAddresses);
            resetAsMasterNextStep(callback);
        });
    }

    private static void resetAsMasterNextStep(Consumer<Exception> callback) {
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
                    log.warn("Repl old repl pair as slave is null, slot: {}", oneSlot.slot());
                }

                if (!canResetSelfAsMasterNow) {
                    log.warn("Repl slave can not reset as master now, need wait current master readonly and slave all caught up, slot: {}",
                            replPairAsSlave.getSlot());
                    XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPairAsSlave, null);
                    throw new IllegalStateException("Repl slave can not reset as master, slot: " + replPairAsSlave.getSlot());
                }

                oneSlot.removeReplPairAsSlave();

                // reset as master
                oneSlot.persistMergingOrMergedSegmentsButNotPersisted();
                oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false);
                oneSlot.getMergedSegmentIndexEndLastTime();

                // set binlog same as old master last updated
                var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
                var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
                var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
                var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);

                var binlog = oneSlot.getBinlog();
                binlog.reopenAtFileIndexAndMarginOffset(lastUpdatedFileIndex, marginLastUpdatedOffset);
                binlog.moveToNextSegment(true);

                // clear old as slave catch up binlog info
                // need fetch from the beginning, for data consistency
                // when next time begin slave again
                metaChunkSegmentIndex.clearMasterBinlogFileIndexAndOffset();

                oneSlot.resetReadonlyFalseAsMaster();

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
            publishMasterSwitchMessage(oldMasterHostAndPort, selfAsMasterHostAndPort);

            callback.accept(null);
        });
    }

    // run in primary eventloop
    public void resetAsSlave(boolean returnExceptionIfAlreadyIsSlave, String host, int port, Consumer<Exception> callback) {
        if (masterAddressLocalMocked != null) {
            callback.accept(null);
//            callback.accept(new RuntimeException("just test callback when reset as slave"));
            return;
        }

        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        var firstOneSlot = localPersist.oneSlots()[0];
        var pp = firstOneSlot.asyncCall(firstOneSlot::getOnlyOneReplPairAsSlave);

        pp.whenComplete((replPair, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            boolean needCloseOldReplPairAsSlave = false;
            if (replPair != null) {
                if (returnExceptionIfAlreadyIsSlave) {
                    callback.accept(new IllegalStateException("Repl already is slave"));
                    return;
                }

                if (replPair.getHostAndPort().equals(host + ":" + port)) {
                    // already is slave of target host and port
                    log.debug("Repl already is slave of target host and port: {}:{}", host, port);
                    callback.accept(null);
                    return;
                } else {
                    needCloseOldReplPairAsSlave = true;
                }
            }

            if (needCloseOldReplPairAsSlave) {
                log.warn("Repl slave ready to remove old repl pair as slave, old master: {}", replPair.getHostAndPort());

                Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
                for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
                    var oneSlot = localPersist.oneSlot((short) i);
                    // still is a slave, need not reset readonly
                    promises[i] = oneSlot.asyncRun(() -> {
                        oneSlot.removeReplPairAsSlave();
                        log.warn("Repl slave removed old repl pair as slave, old master: {}", replPair.getHostAndPort());

                        oneSlot.getBinlog().moveToNextSegment();
                    });
                }

                Promises.all(promises).whenComplete((r, ee) -> {
                    if (ee != null) {
                        callback.accept(ee);
                        return;
                    }

                    makeSelfAsSlave(host, port, callback);
                });
            } else {
                makeSelfAsSlave(host, port, callback);
            }
        });
    }

    // in primary eventloop
    private void makeSelfAsSlave(String host, int port, Consumer<Exception> callback) {
        log.warn("Repl reset self as slave begin, check new master global config first, {}", ConfForGlobal.netListenAddresses);
        try {
            var jedisPool = JedisPoolHolder.getInstance().create(host, port);
            // may be null
            var jsonStr = JedisPoolHolder.exe(jedisPool, jedis -> {
                var pong = jedis.ping();
                log.info("Repl slave of {}:{} pong: {}", host, port, pong);
                return jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," + XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD);
            });

            var map = ConfForSlot.global.slaveCanMatchCheckValues();
            var objectMapper = new ObjectMapper();
            var jsonStrLocal = objectMapper.writeValueAsString(map);

            if (!jsonStrLocal.equals(jsonStr)) {
                log.warn("Repl reset self as slave begin, check new master global config fail, {}", ConfForGlobal.netListenAddresses);
                log.info("Repl local: {}", jsonStrLocal);
                log.info("Repl remote: {}", jsonStr);
                callback.accept(new IllegalStateException("Repl slave can not match check values"));
            }
        } catch (Exception e) {
            callback.accept(e);
            return;
        }
        log.warn("Repl reset self as slave begin, check new master global config ok, {}", ConfForGlobal.netListenAddresses);

        var localPersist = LocalPersist.getInstance();

        Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
        for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((short) i);
            promises[i] = oneSlot.asyncRun(() -> {
                // clear old as slave catch up binlog info
                // need fetch from the beginning, for data consistency
                oneSlot.getMetaChunkSegmentIndex().clearMasterBinlogFileIndexAndOffset();

                oneSlot.createReplPairAsSlave(host, port);
                log.warn("Repl slave created new repl pair as slave, new master: {}:{}, slot: {}", host, port, oneSlot.slot());

                oneSlot.getBinlog().moveToNextSegment();
                // do not write binlog as slave
                oneSlot.getDynConfig().setBinlogOn(false);
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
            publishMasterSwitchMessage(selfAsOldMasterHostAndPort, newMasterHostAndPort);

            callback.accept(null);
        });
    }

    private static final byte[] PUBLISH_CMD_BYTES = "publish".getBytes();

    private static void publishMasterSwitchMessage(ReplPair.HostAndPort from, ReplPair.HostAndPort to) {
        // publish master address to clients
        var publishMessage = ConfForGlobal.zookeeperRootPath + " " + from.host() + " " + from.port() + " " +
                to.host() + " " + to.port();

        var data = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessage.getBytes()};
        PGroup.publish(data);
        log.warn("Repl publish master switch message: {}", publishMessage);

        // publish slave address to clients for readonly slave
        var publishMessageReadonlySlave = ConfForGlobal.zookeeperRootPath + ReplConsts.REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX + " " +
                to.host() + " " + to.port() + " " + from.host() + " " + from.port();
        var dataSlave = new byte[][]{
                PUBLISH_CMD_BYTES,
                XGroup.X_MASTER_SWITCH_PUBLISH_CHANNEL_BYTES,
                publishMessageReadonlySlave.getBytes()};
        PGroup.publish(dataSlave);
        log.warn("Repl publish master switch message for readonly slave: {}", publishMessageReadonlySlave);
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
