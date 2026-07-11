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
import io.velo.persist.OneSlot;
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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
     * Resumes an unfinished durable role transition after slot event loops have been bound.
     *
     * @param callback callback invoked with an exception when recovery remains fenced, or null after convergence
     */
    public void resumePendingRoleTransition(Consumer<Exception> callback) {
        var localPersist = LocalPersist.getInstance();
        final RoleTransitionState transition;
        try {
            transition = localPersist.getRoleTransitionStore().load();
        } catch (IOException ex) {
            localPersist.setRoleTransitionWriteFenced(true);
            callback.accept(ex);
            return;
        }
        if (transition == null || transition.phase() == RoleTransitionState.Phase.STABLE
                || transition.phase() == RoleTransitionState.Phase.ABORTED) {
            callback.accept(null);
            return;
        }

        localPersist.setRoleTransitionWriteFenced(true);
        if (transition.phase() == RoleTransitionState.Phase.FAILED_FENCED) {
            MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = "failed-fenced";
            callback.accept(new IllegalStateException("Role transition requires authoritative reconciliation, epoch="
                    + transition.epoch()));
            return;
        }
        if (transition.targetRole() == RoleTransitionState.TargetRole.SLAVE) {
            resetAsSlave(transition.upstreamHost(), transition.upstreamPort(), callback);
            return;
        }

        resumeMasterTransition(localPersist, transition, callback);
    }

    private void resumeMasterTransition(LocalPersist localPersist,
                                        RoleTransitionState transition,
                                        Consumer<Exception> callback) {
        var slots = localPersist.initializedOneSlots();
        var prepareError = new AtomicReference<Exception>();
        Promise<Void>[] preparePromises = new Promise[slots.size()];
        for (int i = 0; i < slots.size(); i++) {
            var oneSlot = slots.get(i);
            preparePromises[i] = oneSlot.asyncRun(() -> {
                if (transition.phase() == RoleTransitionState.Phase.COMMITTING) {
                    return;
                }
                try {
                    var target = transition.binlogTargets().get(oneSlot.slot());
                    if (target != null) {
                        oneSlot.prepareAsMaster(target.fileIndex(), target.offset());
                    }
                } catch (Exception ex) {
                    prepareError.compareAndSet(null, ex);
                }
            });
        }

        Promises.all(preparePromises).whenComplete((prepared, preparePromiseError) -> {
            var err = prepareError.get();
            if (err != null) {
                finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.ABORTED,
                        err, false, "no-failover", ConfForGlobal.masterSlotNumber, callback);
                return;
            }
            try {
                localPersist.getRoleTransitionStore().save(
                        transition.withPhase(RoleTransitionState.Phase.COMMITTING));
            } catch (IOException ex) {
                finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.FAILED_FENCED,
                        ex, true, "failed-fenced", ConfForGlobal.masterSlotNumber, callback);
                return;
            }

            var commitError = new AtomicReference<Exception>();
            Promise<Void>[] commitPromises = new Promise[slots.size()];
            for (int i = 0; i < slots.size(); i++) {
                var oneSlot = slots.get(i);
                commitPromises[i] = oneSlot.asyncRun(() -> {
                    try {
                        if (transition.binlogTargets().containsKey(oneSlot.slot())) {
                            oneSlot.removeReplPairAsSlave();
                            oneSlot.commitPreparedAsMaster();
                        }
                    } catch (Exception ex) {
                        commitError.compareAndSet(null, ex);
                    }
                });
            }
            Promises.all(commitPromises).whenComplete((committed, commitPromiseError) -> {
                var commitFailure = commitError.get();
                if (commitFailure != null) {
                    finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.FAILED_FENCED,
                            commitFailure, true, "failed-fenced", ConfForGlobal.masterSlotNumber, callback);
                    return;
                }
                try {
                    localPersist.getRoleTransitionStore().clear();
                } catch (IOException ex) {
                    finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.FAILED_FENCED,
                            ex, true, "failed-fenced", ConfForGlobal.masterSlotNumber, callback);
                    return;
                }
                ConfForGlobal.masterSlotNumber = 0;
                localPersist.setRoleTransitionWriteFenced(false);
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = "no-failover";
                callback.accept(null);
            });
        });
    }

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

            // iterate only initialized slots: in cluster mode this node may own a subset of
            // ConfForGlobal.slotNumber, and unowned oneSlots[] entries are null.
            var initSlots = localPersist.initializedOneSlots();

            // Phase 1 — validate readiness of every slot WITHOUT changing any role. A multi-slot promotion
            // must be all-or-nothing: if one slot is not ready (its master is not readonly yet, or it has not
            // caught up), no other slot may already have flipped to master, otherwise a failed promotion
            // leaves the node split across roles and creates a split-brain window (bug 68-3). Only after every
            // slot validates do we commit the role change in phase 2.
            var promotionTargets = new ConcurrentHashMap<Short, RoleTransitionState.BinlogTarget>();
            Promise<Void>[] checkPromises = new Promise[initSlots.size()];
            for (int i = 0; i < initSlots.size(); i++) {
                var oneSlot = initSlots.get(i);
                checkPromises[i] = oneSlot.asyncRun(() -> {
                    var replPairAsSlave = oneSlot.getOnlyOneReplPairAsSlave();
                    if (!force && replPairAsSlave != null
                            && (!replPairAsSlave.isMasterReadonly() || !replPairAsSlave.isAllCaughtUp())) {
                        log.warn("Repl slave can not reset as master now, need wait current master readonly and slave all caught up, slot={}",
                                replPairAsSlave.getSlot());
                        XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPairAsSlave, null);
                        throw new IllegalStateException("Repl slave can not reset as master, slot=" + replPairAsSlave.getSlot());
                    }
                    if (replPairAsSlave == null) {
                        log.debug("Repl old repl pair as slave is null, slot={}", oneSlot.slot());
                    }
                    if (shouldPromoteSlot(localPersist, oneSlot)) {
                        var position = oneSlot.getMetaChunkSegmentIndex().getMasterBinlogFileIndexAndOffset();
                        promotionTargets.put(oneSlot.slot(),
                                new RoleTransitionState.BinlogTarget(position.fileIndex(), position.offset()));
                    }
                });
            }

            Promises.all(checkPromises).whenComplete((rCheck, eCheck) -> {
                if (eCheck != null) {
                    // Validation failed before any slot changed role: restore globals and report. No slot
                    // was mutated, so no compensating rollback is needed.
                    ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
                    MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                    callback.accept(eCheck);
                    return;
                }

                final RoleTransitionState transition;
                try {
                    transition = RoleTransitionState.begin(nextTransitionEpoch(localPersist),
                            RoleTransitionState.TargetRole.MASTER, "", 0, promotionTargets);
                    localPersist.getRoleTransitionStore().save(transition);
                    localPersist.setRoleTransitionWriteFenced(true);
                } catch (IOException ex) {
                    ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
                    MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                    callback.accept(ex);
                    return;
                }

                var prepareError = new AtomicReference<Exception>();
                Promise<Void>[] preparePromises = new Promise[initSlots.size()];
                for (int i = 0; i < initSlots.size(); i++) {
                    var oneSlot = initSlots.get(i);
                    preparePromises[i] = oneSlot.asyncRun(() -> {
                        try {
                            var target = transition.binlogTargets().get(oneSlot.slot());
                            if (target != null) {
                                oneSlot.prepareAsMaster(target.fileIndex(), target.offset());
                            }
                        } catch (Exception ex) {
                            prepareError.compareAndSet(null, ex);
                        }
                    });
                }

                Promises.all(preparePromises).whenComplete((prepared, preparePromiseError) -> {
                    var err = prepareError.get();
                    if (err != null) {
                        finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.ABORTED,
                                err, false, prevFailoverState, prevMasterSlotNumber, callback);
                        return;
                    }

                    try {
                        localPersist.getRoleTransitionStore().save(
                                transition.withPhase(RoleTransitionState.Phase.COMMITTING));
                    } catch (IOException ex) {
                        finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.ABORTED,
                                ex, false, prevFailoverState, prevMasterSlotNumber, callback);
                        return;
                    }

                    var commitError = new AtomicReference<Exception>();
                    Promise<Void>[] commitPromises = new Promise[initSlots.size()];
                    for (int i = 0; i < initSlots.size(); i++) {
                        var oneSlot = initSlots.get(i);
                        commitPromises[i] = oneSlot.asyncRun(() -> {
                            try {
                                if (transition.binlogTargets().containsKey(oneSlot.slot())) {
                                    oneSlot.removeReplPairAsSlave();
                                    oneSlot.commitPreparedAsMaster();
                                }
                            } catch (Exception ex) {
                                commitError.compareAndSet(null, ex);
                            }
                        });
                    }

                    Promises.all(commitPromises).whenComplete((committed, commitPromiseError) -> {
                        var commitFailure = commitError.get();
                        if (commitFailure != null) {
                            finishFailedTransition(localPersist, transition,
                                    RoleTransitionState.Phase.FAILED_FENCED, commitFailure, true,
                                    "failed-fenced", prevMasterSlotNumber, callback);
                            return;
                        }

                        try {
                            localPersist.getRoleTransitionStore().clear();
                        } catch (IOException ex) {
                            finishFailedTransition(localPersist, transition,
                                    RoleTransitionState.Phase.FAILED_FENCED, ex, true,
                                    "failed-fenced", prevMasterSlotNumber, callback);
                            return;
                        }

                        localPersist.setRoleTransitionWriteFenced(false);
                        MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                        ConfForGlobal.masterSlotNumber = 0;
                        var oldMaster = HostAndPort.parse(lastGetMasterListenAddressAsSlave);
                        var self = ConfForGlobal.announcedHostPort();
                        publishMasterSwitchMessage(oldMaster == null ? self : oldMaster, self, true);
                        callback.accept(null);
                    });
                });
            });
        } catch (RuntimeException e) {
            ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
            MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
            callback.accept(e);
            throw e;
        }
    }

    private boolean shouldPromoteSlot(LocalPersist localPersist, OneSlot oneSlot) {
        return oneSlot.getOnlyOneReplPairAsSlave() != null
                || oneSlot.isReadonly()
                || (localPersist.isAsSlaveScaleUp() && oneSlot.slot() >= ConfForGlobal.masterSlotNumber);
    }

    private long nextTransitionEpoch(LocalPersist localPersist) throws IOException {
        var current = localPersist.getRoleTransitionStore().load();
        return current == null ? System.currentTimeMillis() : Math.max(System.currentTimeMillis(), current.epoch() + 1);
    }

    private void finishFailedTransition(LocalPersist localPersist,
                                        RoleTransitionState transition,
                                        RoleTransitionState.Phase phase,
                                        Exception error,
                                        boolean keepFenced,
                                        String failoverState,
                                        short masterSlotNumber,
                                        Consumer<Exception> callback) {
        try {
            localPersist.getRoleTransitionStore().save(transition.withError(phase, error.toString()));
        } catch (IOException persistError) {
            error.addSuppressed(persistError);
            log.error("Repl persist failed role transition state error", persistError);
        }
        localPersist.setRoleTransitionWriteFenced(keepFenced);
        ConfForGlobal.masterSlotNumber = masterSlotNumber;
        MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = failoverState;
        callback.accept(error);
    }

    @VisibleForTesting
    ConfForSlot.SlaveCheckValues masterConfigCheckValuesForTest;

    /**
     * @param values injected master configuration, or null to use the live lookup
     */
    @TestOnly
    public void setMasterConfigCheckValuesForTest(@Nullable ConfForSlot.SlaveCheckValues values) {
        masterConfigCheckValuesForTest = values;
    }

    private @Nullable ConfForSlot.SlaveCheckValues checkMasterConfigMatch(String host, int port, Consumer<Exception> callback) {
        if (masterConfigCheckValuesForTest != null) {
            return masterConfigCheckValuesForTest;
        }
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

            var initSlots = localPersist.initializedOneSlots();
            final RoleTransitionState transition;
            try {
                var pending = localPersist.getRoleTransitionStore().load();
                if (pending != null
                        && pending.targetRole() == RoleTransitionState.TargetRole.SLAVE
                        && pending.upstreamHost().equals(host)
                        && pending.upstreamPort() == port
                        && pending.phase() != RoleTransitionState.Phase.STABLE
                        && pending.phase() != RoleTransitionState.Phase.ABORTED) {
                    transition = pending;
                } else {
                    transition = RoleTransitionState.begin(nextTransitionEpoch(localPersist),
                            RoleTransitionState.TargetRole.SLAVE, host, port, Map.of());
                }
                localPersist.getRoleTransitionStore().save(transition);
                localPersist.setRoleTransitionWriteFenced(true);
            } catch (IOException ex) {
                ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                callback.accept(ex);
                return;
            }

            var prepareError = new AtomicReference<Exception>();
            Promise<Void>[] promises = new Promise[initSlots.size()];
            for (int i = 0; i < initSlots.size(); i++) {
                var oneSlot = initSlots.get(i);
                promises[i] = oneSlot.asyncRun(() -> {
                    try {
                        var masterSlotNumberForStream = ConfForGlobal.masterSlotNumber > 0
                                ? ConfForGlobal.masterSlotNumber : ConfForGlobal.slotNumber;
                        oneSlot.prepareAsSlave(host, port, oneSlot.slot() < masterSlotNumberForStream);
                        resetAsMasterCount = 0;
                        resetAsSlaveCount++;
                    } catch (Exception ex) {
                        prepareError.compareAndSet(null, ex);
                    }
                });
            }

            Promises.all(promises).whenComplete((r, e) -> {
                var err = prepareError.get();
                if (err != null) {
                    finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.FAILED_FENCED,
                            err, true, "failed-fenced", ConfForGlobal.masterSlotNumber, callback);
                    return;
                }

                try {
                    localPersist.getRoleTransitionStore().clear();
                } catch (IOException ex) {
                    finishFailedTransition(localPersist, transition, RoleTransitionState.Phase.FAILED_FENCED,
                            ex, true, "failed-fenced", ConfForGlobal.masterSlotNumber, callback);
                    return;
                }

                localPersist.setRoleTransitionWriteFenced(false);
                MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
                var selfAsOldMasterHostAndPort = ConfForGlobal.announcedHostPort();
                publishMasterSwitchMessage(selfAsOldMasterHostAndPort, new HostAndPort(host, port), false);
                callback.accept(null);
            });
        } catch (RuntimeException e) {
            ConfForGlobal.masterSlotNumber = prevMasterSlotNumber;
            MultiWorkerServer.STATIC_GLOBAL_V.masterFailoverState = prevFailoverState;
            callback.accept(e);
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
