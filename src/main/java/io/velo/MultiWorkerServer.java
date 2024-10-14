package io.velo;

import groovy.lang.GroovyClassLoader;
import io.activej.async.callback.AsyncComputation;
import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.launchers.initializers.Initializers;
import io.activej.net.PrimaryServer;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.service.adapter.ServiceAdapters;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.BufferPoolsExports;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.velo.decode.HttpHeaderBody;
import io.velo.decode.Request;
import io.velo.decode.RequestDecoder;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.persist.KeyBucket;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import io.velo.repl.LeaderSelector;
import io.velo.repl.ReplPair;
import io.velo.repl.cluster.Shard;
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.AsyncReply;
import io.velo.reply.ErrorReply;
import io.velo.reply.NilReply;
import io.velo.reply.Reply;
import io.velo.task.PrimaryTaskRunnable;
import io.velo.task.TaskRunnable;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.apache.commons.net.telnet.TelnetClient;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;

import static io.activej.config.Config.*;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;
import static org.slf4j.LoggerFactory.getLogger;

public class MultiWorkerServer extends Launcher {
//    static {
//        ApplicationSettings.set(ServerSocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//        ApplicationSettings.set(SocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//    }

    public static final String PROPERTIES_FILE = "velo.properties";

    public static long UP_TIME;

    static final int MAX_NET_WORKERS = 128;
    static final int MAX_INDEX_WORKERS = 64;

    @Inject
    PrimaryServer primaryServer;

    @Inject
    RequestHandler[] requestHandlerArray;

    Eventloop[] netWorkerEventloopArray;

    @Inject
    SocketInspector socketInspector;

    @Inject
    RefreshLoader refreshLoader;

    private static final Logger log = LoggerFactory.getLogger(MultiWorkerServer.class);

    static File dirFile(Config config) {
        var dirPath = config.get(ofString(), "dir", "/tmp/velo-data");
        ConfForGlobal.dirPath = dirPath;
        log.warn("Global config, dirPath={}", dirPath);

        var dirFile = new File(dirPath);
        if (!dirFile.exists()) {
            boolean isOk = dirFile.mkdirs();
            if (!isOk) {
                throw new RuntimeException("Create dir " + dirFile.getAbsolutePath() + " failed");
            }
        }
        var persistDir = new File(dirFile, "persist");
        if (!persistDir.exists()) {
            boolean isOk = persistDir.mkdirs();
            if (!isOk) {
                throw new RuntimeException("Create dir " + persistDir.getAbsolutePath() + " failed");
            }
        }
        return dirFile;
    }

    @Provides
    NioReactor primaryReactor(Config config) {
        // default 10ms
        var primaryEventloop = Eventloop.builder()
                .withThreadName("primary")
                .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventLoopIdleMillis))
                .initialize(Initializers.ofEventloop(config.getChild("eventloop.primary")))
                .build();

        this.primaryEventloop = primaryEventloop;
        return primaryEventloop;
    }

    @Provides
    @Worker
    NioReactor workerReactor(@WorkerId int workerId, OptionalDependency<ThrottlingController> throttlingController, Config config) {
        var netHandleEventloop = Eventloop.builder()
                .withThreadName("net-worker-" + workerId)
                .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventLoopIdleMillis))
                .initialize(ofEventloop(config.getChild("net.eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();

        netWorkerEventloopArray[workerId] = netHandleEventloop;

        return netHandleEventloop;
    }

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        int netWorkers = config.get(ofInteger(), "netWorkers", 1);
        netWorkerEventloopArray = new Eventloop[netWorkers];

        // for unit test coverage
        if (workerPools == null) {
            return null;
        }

        // already checked in beforeCreateHandler
        return workerPools.createPool(netWorkers);
    }

    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    ByteBuf wrapHttpResponse(Reply reply) {
        byte[] array;

        boolean isError = reply instanceof ErrorReply;
        boolean isNil = reply instanceof NilReply;
        if (isNil) {
            array = HttpHeaderBody.BODY_404;
        } else {
            array = reply.bufferAsHttp().array();
        }

        if (isError && (reply == ErrorReply.NO_AUTH || reply == ErrorReply.AUTH_FAILED)) {
            // response 401
            return ByteBuf.wrapForReading(HttpHeaderBody.HEADER_401);
        }

        byte[] contentLengthBytes = String.valueOf(array.length).getBytes();

        var headerPrefix = isError ? HttpHeaderBody.HEADER_PREFIX_500 : (isNil ? HttpHeaderBody.HEADER_PREFIX_404 : HttpHeaderBody.HEADER_PREFIX_200);
        var withHeaderLength = headerPrefix.length + contentLengthBytes.length + HttpHeaderBody.HEADER_SUFFIX.length + array.length;
        var withHeaderBytes = new byte[withHeaderLength];

        var httpBuf = ByteBuf.wrapForWriting(withHeaderBytes);
        httpBuf.write(headerPrefix);
        httpBuf.write(contentLengthBytes);
        httpBuf.write(HttpHeaderBody.HEADER_SUFFIX);
        httpBuf.write(array);
        return httpBuf;
    }

    Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
        var slotWithKeyHashList = request.getSlotWithKeyHashList();
        if (ConfForGlobal.clusterEnabled && slotWithKeyHashList != null) {
            // check if cross shards or not my shard
            var multiShardShadow = RequestHandler.getMultiShardShadow();
            var mySelfShard = multiShardShadow.getMySelfShard();

            boolean isIncludeMySelfShard = false;
            Shard movedToShard = null;
            int movedToClientSlot = 0;
            for (var slotWithKeyHash : slotWithKeyHashList) {
                var toClientSlot = slotWithKeyHash.toClientSlot();
                if (toClientSlot == BaseCommand.SlotWithKeyHash.IGNORE_TO_CLIENT_SLOT) {
                    continue;
                }

                var expectRequestShard = multiShardShadow.getShardBySlot(toClientSlot);
                if (expectRequestShard == null) {
                    return Promise.of(ErrorReply.CLUSTER_SLOT_NOT_SET.buffer());
                }

                if (expectRequestShard != mySelfShard) {
                    if (movedToShard == null) {
                        movedToShard = expectRequestShard;
                    } else {
                        if (expectRequestShard != movedToShard) {
                            return Promise.of(ErrorReply.CLUSTER_SLOT_CROSS_SHARDS.buffer());
                        }
                    }
                } else {
                    isIncludeMySelfShard = true;
                }
            }

            if (movedToShard != null) {
                if (isIncludeMySelfShard) {
                    return Promise.of(ErrorReply.CLUSTER_SLOT_CROSS_SHARDS.buffer());
                }

                var master = movedToShard.master();
                var movedReply = ErrorReply.clusterMoved(movedToClientSlot, master.getHost(), master.getPort());
                return Promise.of(movedReply.buffer());
            }
        }

        // some cmd already set cross slot flag
        if (!request.isCrossRequestWorker()) {
            if (slotWithKeyHashList != null && slotWithKeyHashList.size() > 1 && !request.isRepl()) {
                // check if cross threads
                int expectRequestWorkerId = -1;
                for (var slotWithKeyHash : slotWithKeyHashList) {
                    int slot = slotWithKeyHash.slot();
                    var expectRequestWorkerIdInner = slot % requestHandlerArray.length;
                    if (expectRequestWorkerId == -1) {
                        expectRequestWorkerId = expectRequestWorkerIdInner;
                    }
                    if (expectRequestWorkerId != expectRequestWorkerIdInner) {
                        request.setCrossRequestWorker(true);
                        break;
                    }
                }
            }
        }

        var firstSlot = request.getSingleSlot();
        if (firstSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER) {
            RequestHandler targetHandler;
            if (requestHandlerArray.length == 1) {
                targetHandler = requestHandlerArray[0];
            } else {
                var i = new Random().nextInt(requestHandlerArray.length);
                targetHandler = requestHandlerArray[i];
            }
            var reply = targetHandler.handle(request, socket);
            if (reply == null) {
                return Promise.of(null);
            }

            if (reply instanceof AsyncReply) {
                return transferAsyncReply(request, (AsyncReply) reply);
            } else {
                return request.isHttp() ? Promise.of(wrapHttpResponse(reply)) : Promise.of(reply.buffer());
            }
        }

        int i = firstSlot % requestHandlerArray.length;
        var targetHandler = requestHandlerArray[i];

        var currentThreadId = Thread.currentThread().threadId();
        if (currentThreadId == netWorkerThreadIds[i]) {
            return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, null);
        } else {
            var otherNetWorkerEventloop = netWorkerEventloopArray[i];
            return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, otherNetWorkerEventloop);
        }
    }

    @TestOnly
    boolean isMockHandle = false;

    private Promise<ByteBuf> getByteBufPromiseByOtherEventloop(Request request, ITcpSocket socket, RequestHandler targetHandler,
                                                               Eventloop targetEventloop) {
        if (isMockHandle) {
            return Promise.of(ByteBuf.empty());
        }

        var p = targetEventloop == null ? Promises.first(AsyncSupplier.of(() -> targetHandler.handle(request, socket))) :
                Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));

        return p.then(reply -> {
            if (reply == null) {
                return null;
            }

            if (reply instanceof AsyncReply) {
                return transferAsyncReply(request, (AsyncReply) reply);
            } else {
                return Promise.of(request.isHttp() ? wrapHttpResponse(reply) : reply.buffer());
            }
        });
    }

    private Promise<ByteBuf> transferAsyncReply(Request request, AsyncReply reply) {
        var promise = reply.getSettablePromise();
        return promise.map((r, e) -> {
            if (e != null) {
                var errorReply = new ErrorReply(e.getMessage());
                return request.isHttp() ? wrapHttpResponse(errorReply) : errorReply.buffer();
            }

            return request.isHttp() ? wrapHttpResponse(r) : r.buffer();
        });
    }

    Promise<ByteBuf> handlePipeline(ArrayList<Request> pipeline, ITcpSocket socket, short slotNumber) {
        if (pipeline == null) {
            return Promise.of(null);
        }

        for (var request : pipeline) {
            request.setSlotNumber(slotNumber);
            requestHandlerArray[0].parseSlots(request);
        }

        if (pipeline.size() == 1) {
            return handleRequest(pipeline.getFirst(), socket);
        }

        Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
        for (int i = 0; i < pipeline.size(); i++) {
            var promiseI = handleRequest(pipeline.get(i), socket);
            promiseN[i] = promiseI;
        }

        return allPipelineByteBuf(promiseN);
    }

    public static Promise<ByteBuf> allPipelineByteBuf(Promise<ByteBuf>[] promiseN) {
        return Promises.toArray(ByteBuf.class, promiseN)
                .map(bufs -> {
                    int totalN = 0;
                    for (var buf : bufs) {
                        if (buf == null) {
                            continue;
                        }
                        totalN += buf.readRemaining();
                    }
                    if (totalN == 0) {
                        return ByteBuf.empty();
                    }

                    var multiBuf = ByteBuf.wrapForWriting(new byte[totalN]);
                    for (var buf : bufs) {
                        if (buf == null) {
                            continue;
                        }
                        multiBuf.put(buf);
                    }
                    return multiBuf;
                });
    }

    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Config config) {
        int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .mapAsync(pipeline -> handlePipeline(pipeline, socket, (short) slotNumber))
                                .streamTo(ChannelConsumers.ofSocket(socket)))
                .withSocketInspector(socketInspector)
                .build();
    }

    static final int PORT = 7379;

    static String[] MAIN_ARGS;

    @Provides
    Config config() {
        String givenConfigFilePath;
        if (MAIN_ARGS != null && MAIN_ARGS.length > 0) {
            givenConfigFilePath = MAIN_ARGS[0];
        } else {
            var currentDirConfigFile = new File(Utils.projectPath("/" + PROPERTIES_FILE));
            if (currentDirConfigFile.exists()) {
                givenConfigFilePath = currentDirConfigFile.getAbsolutePath();
            } else {
                givenConfigFilePath = "/etc/" + PROPERTIES_FILE;
            }
        }
        log.warn("Config will be loaded from={}", givenConfigFilePath);

        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofProperties(givenConfigFilePath, true))
                .overrideWith(ofSystemProperties("velo-config"));
    }

    // no share
    // if support transaction, need share to generate lsn for all slots
    @Provides
    SnowFlake[] snowFlakes(ConfForSlot confForSlot, Config config) {
        int netWorkers = config.get(ofInteger(), "netWorkers", 1);
        var snowFlakes = new SnowFlake[netWorkers];

        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(ConfForGlobal.datacenterId, (ConfForGlobal.machineId << 8) | i);
        }
        return snowFlakes;
    }

    @Provides
    RefreshLoader refreshLoader() {
        var classpath = Utils.projectPath("/dyn/src");
        CachedGroovyClassLoader.getInstance().init(GroovyClassLoader.class.getClassLoader(), classpath, null);
        return RefreshLoader.create(CachedGroovyClassLoader.getInstance().getGcl())
                .addDir(Utils.projectPath("/dyn/src/io/velo"));
    }

    @Override
    protected final Module getModule() {
        var affinityThreadFactory = new AffinityThreadFactory("net-worker",
                AffinityStrategies.SAME_SOCKET, AffinityStrategies.DIFFERENT_CORE);
        return combine(
                ServiceGraphModule.builder()
                        .with(Eventloop.class, ServiceAdapters.forEventloop(affinityThreadFactory))
                        .build(),
                WorkerPoolModule.create(),
                ConfigModule.builder()
                        .withEffectiveConfigLogger()
                        .build(),
                getBusinessLogicModule()
        );
    }

    protected Module getBusinessLogicModule() {
        return new InnerModule();
    }

    @Inject
    Config configInject;

    @Inject
    TaskRunnable[] scheduleRunnableArray;

    Eventloop primaryEventloop;

    PrimaryTaskRunnable primaryScheduleRunnable;

    private void eventloopAsScheduler(Eventloop netWorkerEventloop, int index) {
        var taskRunnable = scheduleRunnableArray[index];
        taskRunnable.setNetWorkerEventloop(netWorkerEventloop);
        taskRunnable.setRequestHandler(requestHandlerArray[index]);

        taskRunnable.chargeOneSlots(LocalPersist.getInstance().oneSlots());

        // interval 1s
        netWorkerEventloop.delay(1000L, taskRunnable);
    }

    @VisibleForTesting
    long[] netWorkerThreadIds;

    @Override
    protected void onStart() throws Exception {
        netWorkerThreadIds = new long[netWorkerEventloopArray.length];

        for (int i = 0; i < netWorkerEventloopArray.length; i++) {
            var netWorkerEventloop = netWorkerEventloopArray[i];
            assert netWorkerEventloop.getEventloopThread() != null;
            netWorkerThreadIds[i] = netWorkerEventloop.getEventloopThread().threadId();

            // start schedule
            eventloopAsScheduler(netWorkerEventloop, i);
        }
        logger.info("Net worker eventloop scheduler started");

        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = netWorkerThreadIds;

        // start primary schedule
        primaryScheduleRunnable = new PrimaryTaskRunnable(loopCount -> {
            // load and execute groovy script if changed, every 10s
            if (loopCount % 10 == 0) {
                // will not throw exception
                refreshLoader.refresh();
            }

            if (loopCount % 5 == 0) {
                // need catch exception, or will not delay run task
                try {
                    doReplAfterLeaderSelect((short) 0);
                } catch (Exception e) {
                    log.error("Repl leader select error", e);
                }
            }
        });
        primaryScheduleRunnable.setPrimaryEventloop(primaryEventloop);
        // interval 1s
        primaryEventloop.delay(1000L, primaryScheduleRunnable);

        var localPersist = LocalPersist.getInstance();
        // fix slot thread id
        int slotNumber = configInject.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        for (short slot = 0; slot < slotNumber; slot++) {
            int i = slot % requestHandlerArray.length;
            localPersist.fixSlotThreadId(slot, netWorkerThreadIds[i]);
        }

        localPersist.startIndexHandlerPool();

        socketInspector.netWorkerEventloopArray = netWorkerEventloopArray;
        localPersist.setSocketInspector(socketInspector);

        // recover
        primaryEventloop.submit(localPersist::persistMergedSegmentsJobUndone);

        // metrics
        CollectorRegistry.defaultRegistry.register(new BufferPoolsExports());
        CollectorRegistry.defaultRegistry.register(new MemoryPoolsExports());
        CollectorRegistry.defaultRegistry.register(new GarbageCollectorExports());
        logger.info("Prometheus jvm hotspot metrics registered");

        UP_TIME = System.currentTimeMillis();
    }

    // run in primary eventloop
    static void doReplAfterLeaderSelect(short slot) {
        var leaderSelector = LeaderSelector.getInstance();
        // if failover, wait 20s and then start leader select
        if (System.currentTimeMillis() - leaderSelector.getLastStopLeaderLatchTimeMillis()
                < ConfForGlobal.REPL_FAILOVER_SLAVE_WAIT_SECONDS) {
            return;
        }

        var masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress(true);
        if (masterListenAddress == null) {
            return;
        }

        if (ConfForGlobal.netListenAddresses.equals(masterListenAddress)) {
            // self become master
            leaderSelector.resetAsMaster((e) -> {
                if (e != null) {
                    log.error("Reset as master failed", e);
                } else {
                    log.debug("Reset as master success");
                }
            });
            return;
        }

        // self become slave
        var array = masterListenAddress.split(":");
        var host = array[0];
        var port = Integer.parseInt(array[1]);

        if (!ConfForGlobal.isAsSlaveOfSlave) {
            // connect to master
            leaderSelector.resetAsSlave(host, port, (e) -> {
                if (e != null) {
                    log.error("Reset as slave failed", e);
                } else {
                    log.debug("Reset as slave success");
                }
            });
        } else {
            // connect to master's first slave
            var firstSlaveListenAddress = leaderSelector.getFirstSlaveListenAddressByMasterHostAndPort(host, port, slot);
            if (firstSlaveListenAddress == null) {
                log.warn("First slave listen address is null, master host={}, master port={}", host, port);
            } else {
                var arraySlave = firstSlaveListenAddress.split(":");
                var hostSlave = arraySlave[0];
                var portSlave = Integer.parseInt(arraySlave[1]);

                leaderSelector.resetAsSlave(hostSlave, portSlave, (e) -> {
                    if (e != null) {
                        log.error("Reset as slave failed", e);
                    } else {
                        log.info("Reset as slave success");
                    }
                });
            }
        }
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    @Override
    protected void onStop() throws Exception {
        try {
            for (var requestHandler : requestHandlerArray) {
                requestHandler.stop();
            }

            for (var scheduleRunnable : scheduleRunnableArray) {
                scheduleRunnable.stop();
            }

            primaryScheduleRunnable.stop();

            if (socketInspector != null) {
                socketInspector.isServerStopped = true;
                socketInspector.socketMap.values().forEach(socket -> {
                    socket.getReactor().submit(() -> {
                        socket.close();
                        System.out.println("Close connected socket=" + socket.getRemoteAddress());
                    });
                });
            }

            LeaderSelector.getInstance().cleanUp();
            JedisPoolHolder.getInstance().cleanUp();

            // close local persist
            LocalPersist.getInstance().cleanUp();
            DictMap.getInstance().cleanUp();

            for (var netWorkerEventloop : netWorkerEventloopArray) {
                System.out.println("Net worker eventloop wake up");
                netWorkerEventloop.execute(() -> {
                    System.out.println("Net worker eventloop stopping");
                });
            }

            System.out.println("Primary eventloop wake up");
//            primaryEventloop.breakEventloop();
            primaryEventloop.execute(() -> {
                System.out.println("Primary eventloop stopping");
            });
        } catch (Exception e) {
            System.err.println("Stop error=" + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    static class InnerModule extends AbstractModule {
        private final Logger logger = getLogger(getClass());

        @TestOnly
        boolean skipZookeeperConnectCheck = false;

        @Provides
        ConfForSlot confForSlot(Config config) {
            // global conf
            ConfForGlobal.estimateKeyNumber = config.get(ofLong(), "estimateKeyNumber", 1_000_000L);
            ConfForGlobal.estimateOneValueLength = config.get(ofInteger(), "estimateOneValueLength", 200);
            log.warn("Global config, estimateKeyNumber={}", ConfForGlobal.estimateKeyNumber);
            log.warn("Global config, estimateOneValueLength={}", ConfForGlobal.estimateOneValueLength);

            ConfForGlobal.datacenterId = config.get(ofLong(), "datacenterId", 0L);
            ConfForGlobal.machineId = config.get(ofLong(), "machineId", 0L);
            log.warn("Global config, datacenterId={}", ConfForGlobal.datacenterId);
            log.warn("Global config, machineId={}", ConfForGlobal.machineId);

            ConfForGlobal.isValueSetUseCompression = config.get(ofBoolean(), "isValueSetUseCompression", true);
            ConfForGlobal.isOnDynTrainDictForCompression = config.get(ofBoolean(), "isOnDynTrainDictForCompression", false);
            log.warn("Global config, isValueSetUseCompression={}", ConfForGlobal.isValueSetUseCompression);
            log.warn("Global config, isOnDynTrainDictForCompression={}", ConfForGlobal.isOnDynTrainDictForCompression);

            ConfForGlobal.netListenAddresses = config.get(ofString(), "net.listenAddresses", "localhost:" + PORT);
            logger.info("Net listen addresses={}", ConfForGlobal.netListenAddresses);
            ConfForGlobal.eventLoopIdleMillis = config.get(ofInteger(), "eventloop.idleMillis", 10);
            log.warn("Global config, eventLoopIdleMillis={}", ConfForGlobal.eventLoopIdleMillis);

            ConfForGlobal.PASSWORD = config.get(ofString(), "password", null);

            ConfForGlobal.pureMemory = config.get(ofBoolean(), "pureMemory", false);
            log.warn("Global config, pureMemory={}", ConfForGlobal.pureMemory);

            if (config.getChild("zookeeperConnectString").hasValue()) {
                ConfForGlobal.zookeeperConnectString = config.get(ofString(), "zookeeperConnectString");
                ConfForGlobal.zookeeperSessionTimeoutMs = config.get(ofInteger(), "zookeeperSessionTimeoutMs", 30000);
                ConfForGlobal.zookeeperConnectionTimeoutMs = config.get(ofInteger(), "zookeeperConnectionTimeoutMs", 10000);
                ConfForGlobal.zookeeperRootPath = config.get(ofString(), "zookeeperRootPath", "/io/velo");
                ConfForGlobal.canBeLeader = config.get(ofBoolean(), "canBeLeader", true);
                ConfForGlobal.isAsSlaveOfSlave = config.get(ofBoolean(), "isAsSlaveOfSlave", false);
                ConfForGlobal.targetAvailableZone = config.get(ofString(), "targetAvailableZone", null);

                log.warn("Global config, zookeeperConnectString={}", ConfForGlobal.zookeeperConnectString);
                log.warn("Global config, zookeeperSessionTimeoutMs={}", ConfForGlobal.zookeeperSessionTimeoutMs);
                log.warn("Global config, zookeeperConnectionTimeoutMs={}", ConfForGlobal.zookeeperConnectionTimeoutMs);
                log.warn("Global config, zookeeperRootPath={}", ConfForGlobal.zookeeperRootPath);
                log.warn("Global config, canBeLeader={}", ConfForGlobal.canBeLeader);
                log.warn("Global config, isAsSlaveOfSlave={}", ConfForGlobal.isAsSlaveOfSlave);
                log.warn("Global config, targetAvailableZone={}", ConfForGlobal.targetAvailableZone);

                if (!skipZookeeperConnectCheck) {
                    // check can connect
                    var array = ConfForGlobal.zookeeperConnectString.split(",");
                    var hostAndPort = ReplPair.parseHostAndPort(array[array.length - 1]);
                    var tc = new TelnetClient();
                    try {
                        tc.connect(hostAndPort.host(), hostAndPort.port());
                        tc.disconnect();
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Zookeeper connect string invalid, can not connect to " + hostAndPort.host() + ":" + hostAndPort.port());
                    }
                }
            }
            ConfForGlobal.clusterEnabled = config.get(ofBoolean(), "clusterEnabled", false);

            DictMap.TO_COMPRESS_MIN_DATA_LENGTH = config.get(ofInteger(), "toCompressMinDataLength", 64);

            // one slot config
            var c = ConfForSlot.from(ConfForGlobal.estimateKeyNumber);
            ConfForSlot.global = c;

            boolean debugMode = config.get(ofBoolean(), "debugMode", false);
            if (debugMode) {
                c.confBucket.bucketsPerSlot = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;
                c.confBucket.initialSplitNumber = ConfForSlot.ConfBucket.debugMode.initialSplitNumber;

                c.confChunk.segmentNumberPerFd = ConfForSlot.ConfChunk.debugMode.segmentNumberPerFd;
                c.confChunk.fdPerChunk = ConfForSlot.ConfChunk.debugMode.fdPerChunk;
                c.confChunk.segmentLength = ConfForSlot.ConfChunk.debugMode.segmentLength;

                c.confWal.oneChargeBucketNumber = ConfForSlot.ConfWal.debugMode.oneChargeBucketNumber;
                c.confWal.valueSizeTrigger = ConfForSlot.ConfWal.debugMode.valueSizeTrigger;
                c.confWal.shortValueSizeTrigger = ConfForSlot.ConfWal.debugMode.shortValueSizeTrigger;
            }

            var debugInstance = Debug.getInstance();
            debugInstance.logCmd = config.get(ofBoolean(), "debugLogCmd", false);
            debugInstance.logMerge = config.get(ofBoolean(), "debugLogMerge", false);
            debugInstance.logTrainDict = config.get(ofBoolean(), "debugLogTrainDict", false);
            debugInstance.logRestore = config.get(ofBoolean(), "debugLogRestore", false);
            debugInstance.bulkLoad = config.get(ofBoolean(), "bulkLoad", false);

            // override bucket conf
            if (config.getChild("bucket.bucketsPerSlot").hasValue()) {
                c.confBucket.bucketsPerSlot = config.get(ofInteger(), "bucket.bucketsPerSlot");
            }
            if (c.confBucket.bucketsPerSlot > KeyBucket.MAX_BUCKETS_PER_SLOT) {
                throw new IllegalArgumentException("Bucket count per slot too large, bucket count per slot should be less than " + KeyBucket.MAX_BUCKETS_PER_SLOT);
            }
            if (c.confBucket.bucketsPerSlot % 1024 != 0) {
                throw new IllegalArgumentException("Bucket count per slot should be multiple of 1024");
            }
            if (config.getChild("bucket.initialSplitNumber").hasValue()) {
                c.confBucket.initialSplitNumber = config.get(ofInteger(), "bucket.initialSplitNumber").byteValue();
            }

            if (config.getChild("bucket.lruPerFd.maxSize").hasValue()) {
                c.confBucket.lruPerFd.maxSize = config.get(ofInteger(), "bucket.lruPerFd.maxSize");
            }

            // override chunk conf
            if (config.getChild("chunk.segmentNumberPerFd").hasValue()) {
                c.confChunk.segmentNumberPerFd = config.get(ofInteger(), "chunk.segmentNumberPerFd");
            }
            if (config.getChild("chunk.fdPerChunk").hasValue()) {
                c.confChunk.fdPerChunk = Byte.parseByte(config.get("chunk.fdPerChunk"));
            }
            if (c.confChunk.fdPerChunk > ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK) {
                throw new IllegalArgumentException("Chunk fd per chunk too large, fd per chunk should be less than " + ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK);
            }

            if (config.getChild("chunk.segmentLength").hasValue()) {
                c.confChunk.segmentLength = config.get(ofInteger(), "chunk.segmentLength");
            }
            c.confChunk.isSegmentUseCompression = config.get(ofBoolean(), "chunk.isSegmentUseCompression", false);
            log.warn("Chunk segment use compression={}", c.confChunk.isSegmentUseCompression);

            if (config.getChild("chunk.lruPerFd.maxSize").hasValue()) {
                c.confChunk.lruPerFd.maxSize = config.get(ofInteger(), "chunk.lruPerFd.maxSize");
            }

            c.confChunk.resetByOneValueLength(ConfForGlobal.estimateOneValueLength);

            // override wal conf
            if (config.getChild("wal.oneChargeBucketNumber").hasValue()) {
                c.confWal.oneChargeBucketNumber = config.get(ofInteger(), "wal.oneChargeBucketNumber");
            }
            if (!Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST.contains(c.confWal.oneChargeBucketNumber)) {
                throw new IllegalArgumentException("Wal one charge bucket number invalid, wal one charge bucket number should be in " + Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST);
            }

            if (config.getChild("wal.valueSizeTrigger").hasValue()) {
                c.confWal.valueSizeTrigger = config.get(ofInteger(), "wal.valueSizeTrigger");
            }
            if (config.getChild("wal.shortValueSizeTrigger").hasValue()) {
                c.confWal.shortValueSizeTrigger = config.get(ofInteger(), "wal.shortValueSizeTrigger");
            }

            c.confWal.resetByOneValueLength(ConfForGlobal.estimateOneValueLength);

            if (config.getChild("repl.binlogForReadCacheSegmentMaxCount").hasValue()) {
                c.confRepl.binlogForReadCacheSegmentMaxCount = config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount").shortValue();
            }

            if (config.getChild("big.string.lru.maxSize").hasValue()) {
                c.lruBigString.maxSize = config.get(ofInteger(), "big.string.lru.maxSize");
            }
            if (config.getChild("kv.lru.maxSize").hasValue()) {
                c.lruKeyAndCompressedValueEncoded.maxSize = config.get(ofInteger(), "kv.lru.maxSize");
            }

            logger.info("ConfForSlot={}", c);
            return c;
        }

        @Provides
        Integer beforeCreateHandler(ConfForSlot confForSlot, SnowFlake[] snowFlakes, Config config) throws IOException {
            int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
            if (slotNumber > LocalPersist.MAX_SLOT_NUMBER) {
                throw new IllegalArgumentException("Slot number too large, slot number should be less than " + LocalPersist.MAX_SLOT_NUMBER);
            }
            if (slotNumber <= 0) {
                throw new IllegalArgumentException("Slot number should be greater than 0");
            }
            if (slotNumber != 1 && (slotNumber & (slotNumber - 1)) != 0) {
                throw new IllegalArgumentException("Slot number should be 1 or power of 2");
            }
            ConfForGlobal.slotNumber = (short) slotNumber;
            log.warn("Global config, slotNumber={}", ConfForGlobal.slotNumber);

//            if (ConfForGlobal.clusterEnabled) {
//                if (MultiShard.TO_CLIENT_SLOT_NUMBER % slotNumber != 0) {
//                    throw new IllegalArgumentException("Slot number should be divided by " + MultiShard.TO_CLIENT_SLOT_NUMBER);
//                }
//            }

            int netWorkers = config.get(ofInteger(), "netWorkers", 1);
            if (netWorkers > MAX_NET_WORKERS) {
                throw new IllegalArgumentException("Net workers too large, net workers should be less than " + MAX_NET_WORKERS);
            }
            var cpuNumber = Runtime.getRuntime().availableProcessors();
            if (netWorkers >= cpuNumber) {
                throw new IllegalArgumentException("Net workers should be less than cpu number");
            }
            if (slotNumber < netWorkers) {
                throw new IllegalArgumentException("Net workers should <= slot number");
            }
            if (slotNumber % netWorkers != 0) {
                throw new IllegalArgumentException("Slot number should be multiple of net workers");
            }
            ConfForGlobal.netWorkers = (byte) netWorkers;
            log.warn("Global config, netWorkers={}", ConfForGlobal.netWorkers);

            int indexWorkers = config.get(ofInteger(), "indexWorkers", 1);
            if (indexWorkers > MAX_INDEX_WORKERS) {
                throw new IllegalArgumentException("Index workers too large, index workers should be less than " + MAX_INDEX_WORKERS);
            }
            if (indexWorkers >= cpuNumber) {
                throw new IllegalArgumentException("Index workers should be less than cpu number");
            }
            ConfForGlobal.indexWorkers = (byte) indexWorkers;
            log.warn("Global config, indexWorkers={}", ConfForGlobal.indexWorkers);

            var dirFile = dirFile(config);

            DictMap.getInstance().initDictMap(dirFile);

            var configCompress = config.getChild("compress");
            TrainSampleJob.setDictKeyPrefixEndIndex(configCompress.get(ofInteger(), "dictKeyPrefixEndIndex", 5));

            RequestHandler.initMultiShardShadows((byte) netWorkers);

            // init local persist
            // already created when inject
            var persistDir = new File(dirFile, "persist");
            LocalPersist.getInstance().initSlots((byte) netWorkers, (short) slotNumber, snowFlakes, persistDir,
                    config.getChild("persist"));

            boolean debugMode = config.get(ofBoolean(), "debugMode", false);
            if (debugMode) {
                LocalPersist.getInstance().debugMode();
            }

            return 0;
        }

        @Provides
        RequestHandler[] requestHandlerArray(SnowFlake[] snowFlakes, Integer beforeCreateHandler, Config config) {
            int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
            int netWorkers = config.get(ofInteger(), "netWorkers", 1);

            var list = new RequestHandler[netWorkers];
            for (int i = 0; i < netWorkers; i++) {
                list[i] = new RequestHandler((byte) i, (byte) netWorkers, (short) slotNumber, snowFlakes[i], config);
            }
            return list;
        }

        @Provides
        TaskRunnable[] scheduleRunnableArray(Integer beforeCreateHandler, Config config) {
            int netWorkers = config.get(ofInteger(), "netWorkers", 1);
            var list = new TaskRunnable[netWorkers];
            for (int i = 0; i < netWorkers; i++) {
                list[i] = new TaskRunnable((byte) i, (byte) netWorkers);
            }
            return list;
        }

        @Provides
        SocketInspector socketInspector(Config config) {
            int maxConnections = config.get(ofInteger(), "maxConnections", 1000);

            var r = new SocketInspector();
            r.setMaxConnections(maxConnections);
            MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = r;
            return r;
        }
    }

    public static void main(String[] args) throws Exception {
        MAIN_ARGS = args;
        Launcher launcher = new MultiWorkerServer();
        launcher.launch(args);
    }

    public static class StaticGlobalV {
        public SocketInspector socketInspector;
        // immutable
        public long[] netWorkerThreadIds;

        // use this instead of ThreadLocal
        public int getThreadLocalIndexByCurrentThread() {
            var currentThreadId = Thread.currentThread().threadId();
            for (int i = 0; i < MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds.length; i++) {
                if (currentThreadId == MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds[i]) {
                    return i;
                }
            }
            return -1;
        }
    }

    public static final StaticGlobalV STATIC_GLOBAL_V = new StaticGlobalV();
}
