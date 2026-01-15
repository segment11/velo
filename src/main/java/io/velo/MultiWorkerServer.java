package io.velo;

import groovy.lang.GroovyClassLoader;
import groovy.lang.Tuple2;
import io.activej.async.callback.AsyncComputation;
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
import io.prometheus.client.hotspot.StandardExports;
import io.velo.acl.AclUsers;
import io.velo.acl.Category;
import io.velo.command.BlockingList;
import io.velo.decode.HttpHeaderBody;
import io.velo.decode.Request;
import io.velo.decode.RequestDecoder;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.monitor.RuntimeCpuCollector;
import io.velo.persist.LocalPersist;
import io.velo.repl.LeaderSelector;
import io.velo.repl.ReplPair;
import io.velo.repl.cluster.Shard;
import io.velo.repl.support.JedisPoolHolder;
import io.velo.reply.*;
import io.velo.task.PrimaryTaskRunnable;
import io.velo.task.TaskRunnable;
import jnr.ffi.Platform;
import org.apache.commons.net.telnet.TelnetClient;
import org.apache.lucene.util.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static io.activej.config.Config.*;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;
import static io.velo.RequestHandler.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Manages a multi-worker server for handling network connections and processing requests.
 * This class initializes and manages worker pools, event loops, and request handlers, ensuring efficient processing of client requests.
 */
public class MultiWorkerServer extends Launcher {
    /**
     * Properties file name for configuration.
     */
    public static final String PROPERTIES_FILE = "velo.properties";

    /**
     * Server uptime timestamp.
     */
    public static long UP_TIME;

    /**
     * Maximum number of network workers.
     * Usually network threads = slot worker threads * 2.
     */
    static final int MAX_NET_WORKERS = 32;

    /**
     * Maximum number of slot workers.
     */
    static final int MAX_SLOT_WORKERS = 32;

    /**
     * Maximum number of index workers.
     */
    static final int MAX_INDEX_WORKERS = 16;

    /**
     * Primary server for handling incoming connections.
     */
    @Inject
    PrimaryServer primaryServer;

    /**
     * Array of request handlers, one per slot worker.
     */
    @ThreadNeedLocal
    @Inject
    RequestHandler[] requestHandlerArray;

    /**
     * Array of event loops for networkers.
     */
    @ThreadNeedLocal
    Eventloop[] netWorkerEventloopArray;

    /**
     * Array of event loops for slot workers.
     */
    @ThreadNeedLocal
    Eventloop[] slotWorkerEventloopArray;

    /**
     * Socket inspector for managing socket connections.
     */
    @Inject
    SocketInspector socketInspector;

    /**
     * Refresh loader for dynamic class loading.
     */
    @Inject
    RefreshLoader refreshLoader;

    /**
     * Logger for logging information and errors.
     */
    private static final Logger log = LoggerFactory.getLogger(MultiWorkerServer.class);

    private static File pidFile;
    private static FileLock pidFileLock;
    private static FileChannel pidFileChannel;
    private static final String PID_FILE_NAME = "velo.pid";

    /**
     * Returns the directory file based on the configuration.
     *
     * @param config the configuration
     * @return the directory file
     */
    static File dirFile(Config config, boolean doFileLock) {
        var dirPath = config.get(ofString(), "dir", "/tmp/velo-data");
        ConfForGlobal.dirPath = dirPath;
        log.warn("Global config, dirPath={}", dirPath);

        var dirFile = new File(dirPath);
        if (!dirFile.exists()) {
            boolean isOk = dirFile.mkdirs();
            if (!isOk) {
                throw new RuntimeException("Create dir " + dirPath + " failed");
            }
        }

        if (doFileLock) {
            pidFile = new File(dirFile, PID_FILE_NAME);
            try {
                pidFileChannel = FileChannel.open(pidFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                pidFileLock = pidFileChannel.tryLock();
                if (pidFileLock == null) {
                    throw new RuntimeException("File lock check failed, please check the dir " + dirPath);
                }

                long processId = ProcessHandle.current().pid();
                var bytes = String.valueOf(processId).getBytes();
                var buffer = ByteBuffer.wrap(bytes);
                int n = pidFileChannel.write(buffer);
                log.warn("File lock check ok, the dir {}, pid={}, write n={}", dirPath, processId, n);
            } catch (Exception e) {
                log.error("File lock check failed, please check the dir {}, error={}", dirPath, e.getMessage());
                throw new RuntimeException(e);
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

    /**
     * Provides the primary reactor for the server.
     *
     * @param config the configuration
     * @return the primary reactor
     */
    @Provides
    NioReactor primaryReactor(Config config) {
        // default 10ms
        var primaryEventloop = Eventloop.builder()
                .withThreadName("primary")
                .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventloopIdleMillis))
                .initialize(Initializers.ofEventloop(config.getChild("eventloop.primary")))
                .build();

        this.primaryEventloop = primaryEventloop;
        return primaryEventloop;
    }

    /**
     * Provides the worker reactor for each worker.
     *
     * @param workerId             the worker ID
     * @param throttlingController the throttling controller
     * @param config               the configuration
     * @return the worker reactor
     */
    @Provides
    @Worker
    NioReactor workerReactor(@WorkerId int workerId, OptionalDependency<ThrottlingController> throttlingController, Config config) {
        var netHandleEventloop = Eventloop.builder()
                .withThreadName("net-worker-" + workerId)
                .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventloopIdleMillis))
                .initialize(ofEventloop(config.getChild("net.eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();

        netWorkerEventloopArray[workerId] = netHandleEventloop;

        return netHandleEventloop;
    }

    /**
     * Provides the worker pool for managing workers.
     *
     * @param workerPools the worker pools
     * @param config      the configuration
     * @return the worker pool
     */
    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        int netWorkers = config.get(ofInteger(), "netWorkers", 1);
        netWorkerEventloopArray = new Eventloop[netWorkers];

        // for unit test
        if (workerPools == null) {
            return null;
        }

        // already checked in beforeCreateHandler
        return workerPools.createPool(netWorkers);
    }

    /**
     * Provides the primary server for handling incoming connections.
     *
     * @param primaryReactor the primary reactor
     * @param workerServers  the worker servers
     * @param config         the configuration
     * @return the primary server
     */
    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    /**
     * Wraps the HTTP response with the given reply.
     *
     * @param reply the reply to wrap
     * @return the wrapped HTTP response as a ByteBuf
     */
    ByteBuf wrapHttpResponse(Reply reply) {
        byte[] array;

        boolean isError = reply instanceof ErrorReply;
        boolean isNil = reply instanceof NilReply;
        if (isNil) {
            array = HttpHeaderBody.BODY_404;
        } else {
            array = reply.bufferAsHttp().array();
        }

        if (isError && (reply == ErrorReply.NO_AUTH || reply == ErrorReply.AUTH_FAILED || reply == ErrorReply.ACL_PERMIT_LIMIT)) {
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

    /**
     * Checks if the cluster slots are valid for the given list of slots with key hashes.
     *
     * @param slotWithKeyHashList the list of slots with key hashes
     * @return an error reply if the slots are invalid, null otherwise
     */
    @VisibleForTesting
    ErrorReply checkClusterSlot(ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        // check if cross-shards or not my shard
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
                return ErrorReply.CLUSTER_SLOT_NOT_SET;
            }

            if (expectRequestShard != mySelfShard) {
                if (movedToShard == null) {
                    movedToShard = expectRequestShard;
                } else {
                    if (expectRequestShard != movedToShard) {
                        return ErrorReply.CLUSTER_SLOT_CROSS_SHARDS;
                    }
                }
            } else {
                isIncludeMySelfShard = true;
            }
        }

        if (movedToShard != null) {
            if (isIncludeMySelfShard) {
                return ErrorReply.CLUSTER_SLOT_CROSS_SHARDS;
            }

            var master = movedToShard.master();
            return ErrorReply.clusterMoved(movedToClientSlot, master.getHost(), master.getPort());
        }

        return null;
    }

    private final Debug debug = Debug.getInstance();

    private Promise<ByteBuf> handleFast(Request request, ITcpSocket socket) {
        var cmd = request.cmd();
        var data = request.getData();
        switch (cmd) {
            case ECHO_COMMAND -> {
                if (data.length == 2) {
                    return Promise.of(new BulkReply(data[1]).buffer());
                } else {
                    return Promise.of(ErrorReply.FORMAT.buffer());
                }
            }
            case PING_COMMAND -> {
                if (data.length == 1) {
                    return Promise.of(PongReply.INSTANCE.buffer());
                } else {
                    var messageBytes = data[1];
                    return Promise.of(new BulkReply(messageBytes).buffer());
                }
            }
            case QUIT_COMMAND -> {
                socket.close();
                return Promise.of(OKReply.INSTANCE.buffer());
            }
        }

        if (debug.logCmd) {
            if (data.length == 1) {
                log.info("Request cmd={}", cmd);
            } else {
                var sb = new StringBuilder();
                sb.append("Request cmd=").append(cmd).append(" ");
                for (int i = 1; i < data.length; i++) {
                    sb.append(new String(data[i])).append(" ");
                }
                log.info(sb.toString());
            }
        }

        return null;
    }

    private Promise<ByteBuf> handleReplRequest(Request request, ITcpSocket socket) {
        var slotRemote = request.getSingleSlot();

        int i = slotRemote % requestHandlerArray.length;
        var targetHandler = requestHandlerArray[i];

        var otherNetWorkerEventloop = slotWorkerEventloopArray[i];
        return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, otherNetWorkerEventloop);
    }

    /**
     * Handles a single request from a client socket.
     *
     * @param request the request to handle
     * @param socket  the client socket
     * @return a promise of the response as a ByteBuf
     */
    Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
        if (request.isRepl()) {
            return handleReplRequest(request, socket);
        }

        var r = handleFast(request, socket);
        if (r != null) {
            return r;
        }

        var isAclCheckOk = request.isAclCheckOk();
        if (!isAclCheckOk.asBoolean()) {
            return Promise.of(
                    isAclCheckOk.isKeyFail() ?
                            ErrorReply.ACL_PERMIT_KEY_LIMIT.buffer()
                            : ErrorReply.ACL_PERMIT_LIMIT.buffer()
            );
        }

        if (SocketInspector.isConnectionReadonly(socket) && Category.isWriteCmd(request.cmd())) {
            return Promise.of(ErrorReply.READONLY.buffer());
        }

        var slotWithKeyHashList = request.getSlotWithKeyHashList();
        if (ConfForGlobal.clusterEnabled && slotWithKeyHashList != null) {
            var checkResult = checkClusterSlot(slotWithKeyHashList);
            if (checkResult != null) {
                return Promise.of(checkResult.buffer());
            }
        }

        // some cmd already set a cross-slot flag
        if (!request.isCrossRequestWorker()) {
            if (slotWithKeyHashList != null && slotWithKeyHashList.size() > 1 && !request.isRepl()) {
                // check if cross-threads
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

        var veloUserData = SocketInspector.createUserDataIfNotSet(socket);

        var firstSlot = request.getSingleSlot();
        if (isReuseNetWorkerEventloop) {
            if (veloUserData.replyMode != VeloUserDataInSocket.ReplyMode.on) {
                return Promise.of(ByteBuf.empty());
            }

            var targetHandler = requestHandlerArray[0];
            var reply = targetHandler.handle(request, socket);

            if (reply instanceof AsyncReply) {
                return transferAsyncReply(request, (AsyncReply) reply, veloUserData.isResp3);
            } else {
                if (reply == ErrorReply.FORMAT) {
                    reply = ErrorReply.WRONG_NUMBER(request.cmd());
                }

                return request.isHttp() ?
                        Promise.of(wrapHttpResponse(reply)) :
                        Promise.of(veloUserData.isResp3 ? reply.bufferAsResp3() : reply.buffer());
            }
        }

        RequestHandler targetHandler;
        int i;
        if (firstSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER) {
            i = new Random().nextInt(requestHandlerArray.length);
            targetHandler = requestHandlerArray[i];
        } else {
            i = firstSlot % requestHandlerArray.length;
            targetHandler = requestHandlerArray[i];
        }

        var otherNetWorkerEventloop = slotWorkerEventloopArray[i];
        return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, otherNetWorkerEventloop);
    }

    /**
     * Flag indicating whether the handle is mocked for testing.
     */
    @TestOnly
    boolean isMockHandle = false;

    /**
     * Gets a promise of the response by handling the request on another event loop.
     *
     * @param request         the request to handle
     * @param socket          the client socket
     * @param targetHandler   the target request handler
     * @param targetEventloop the target event loop
     * @return a promise of the response as a ByteBuf
     */
    private Promise<ByteBuf> getByteBufPromiseByOtherEventloop(Request request, ITcpSocket socket, RequestHandler targetHandler,
                                                               Eventloop targetEventloop) {
        if (isMockHandle) {
            return Promise.of(ByteBuf.empty());
        }

        var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
        var isResp3 = veloUserData.isResp3;
        var replyMode = veloUserData.replyMode;

        var p = Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));

        return p.then(reply -> {
            if (reply == null) {
                return null;
            }
            if (replyMode != VeloUserDataInSocket.ReplyMode.on) {
                return Promise.of(ByteBuf.empty());
            }

            if (reply instanceof AsyncReply) {
                return transferAsyncReply(request, (AsyncReply) reply, isResp3);
            } else {
                if (reply == ErrorReply.FORMAT) {
                    reply = ErrorReply.WRONG_NUMBER(request.cmd());
                }

                return Promise.of(
                        request.isHttp() ?
                                wrapHttpResponse(reply) :
                                (isResp3 ? reply.bufferAsResp3() : reply.buffer())
                );
            }
        });
    }

    /**
     * Transfers an asynchronous reply to a ByteBuf promise.
     *
     * @param request the request that generated the reply
     * @param reply   the asynchronous reply
     * @param isResp3 true if the reply should be formatted in RESP3, false otherwise
     * @return a promise of the response as a ByteBuf
     */
    private Promise<ByteBuf> transferAsyncReply(Request request, AsyncReply reply, boolean isResp3) {
        var promise = reply.getSettablePromise();
        return promise.map((r, e) -> {
            if (e != null) {
                var errorReply = new ErrorReply(e.getMessage());
                return request.isHttp() ? wrapHttpResponse(errorReply) : errorReply.buffer();
            }

            if (r == ErrorReply.FORMAT) {
                r = ErrorReply.WRONG_NUMBER(request.cmd());
            }

            return request.isHttp() ?
                    wrapHttpResponse(r) :
                    (isResp3 ? r.bufferAsResp3() : r.buffer());
        });
    }

    /**
     * Handles a pipeline of requests from a client socket.
     *
     * @param pipeline   the pipeline of requests to handle
     * @param socket     the client socket
     * @param slotNumber the slot number for the requests
     * @return a promise of the response as a ByteBuf
     */
    Promise<ByteBuf> handlePipeline(@NotNull ArrayList<Request> pipeline, ITcpSocket socket, short slotNumber) {
        if (pipeline.isEmpty()) {
            return Promise.of(ByteBuf.empty());
        }

        SocketInspector.updateLastSendCommand(socket, pipeline.getLast().cmd(), pipeline.size());

        var u = BaseCommand.getAuthU(socket);

        for (var request : pipeline) {
            request.setSlotNumber(slotNumber);
            requestHandlerArray[0].parseSlots(request);
            request.setU(u);
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

    /**
     * Combines multiple ByteBuf promises into a single ByteBuf promise.
     *
     * @param promiseN the array of ByteBuf promises
     * @return a promise of the combined ByteBuf
     */
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

    /**
     * Provides a consumer for handling incoming TCP sockets.
     *
     * @param config the configuration
     * @return a consumer for handling TCP sockets
     */
    @Provides
    Consumer<ITcpSocket> consumer(Config config) {
        int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        return socket -> {
            var byteBufSupplier = ChannelSuppliers.prefetch(8, ChannelSuppliers.ofAsyncSupplier(socket::read, socket));
            BinaryChannelSupplier.of(byteBufSupplier)
                    .decodeStream(new RequestDecoder())
                    .mapAsync(pipeline -> handlePipeline(pipeline, socket, (short) slotNumber))
                    .streamTo(ChannelConsumers.ofSocket(socket));
        };
    }

    /**
     * Provides a worker server for handling client connections.
     *
     * @param reactor         the reactor for the worker server
     * @param socketInspector the socket inspector for managing socket connections
     * @param consumer        the consumer for handling incoming sockets
     * @param config          the configuration
     * @return a worker server
     */
    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Consumer<ITcpSocket> consumer, Config config) {
        return SimpleServer.builder(reactor, consumer)
                .withSocketInspector(socketInspector)
                .build();
    }

    private static final int PORT = 7379;

    static String[] MAIN_ARGS;

    private static void prepareConfig() {
        var configFilePath = configFilePath();
        var config = Config.create()
                .overrideWith(ofProperties(configFilePath, true));

        var applicationSettings = config.getChild("ApplicationSettings");
        var socketSettings = applicationSettings.getChild("SocketSettings");
        var map = socketSettings.toMap();
        log.warn("Reset socket settings={}", map);
        map.forEach((k, v) -> System.setProperty("io.activej.reactor.net.SocketSettings." + k, v));

        var serverSocketSettings = applicationSettings.getChild("ServerSocketSettings");
        var map1 = serverSocketSettings.toMap();
        log.warn("Reset server socket settings={}", map1);
        map1.forEach((k, v) -> System.setProperty("io.activej.reactor.net.ServerSocketSettings." + k, v));
    }

    static {
        prepareConfig();
    }

    private static String configFilePath() {
        String filePath;
        if (MAIN_ARGS != null && MAIN_ARGS.length > 0) {
            filePath = MAIN_ARGS[0];
        } else {
            var currentDirConfigFile = new File(Utils.projectPath("/" + PROPERTIES_FILE));
            if (currentDirConfigFile.exists()) {
                filePath = currentDirConfigFile.getAbsolutePath();
            } else {
                var x = new File(Utils.projectPath("/src/main/resources/" + PROPERTIES_FILE));
                if (x.exists()) {
                    filePath = x.getAbsolutePath();
                } else {
                    filePath = "/etc/" + PROPERTIES_FILE;
                }
            }
        }
        return filePath;
    }

    /**
     * Provides the configuration for the server.
     *
     * @return the configuration
     */
    @Provides
    Config config() {
        var configFilePath = configFilePath();
        log.warn("Config will be loaded from={}", configFilePath);

        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofProperties(configFilePath, true))
                .overrideWith(ofSystemProperties("velo-config"));
    }

    /**
     * Provides an array of SnowFlake instances for generating unique IDs.
     *
     * @param confForSlot the configuration for slots
     * @param config      the configuration
     * @return an array of SnowFlake instances
     */
    @Provides
    SnowFlake[] snowFlakes(ConfForSlot confForSlot, Config config) {
        int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        var snowFlakes = new SnowFlake[slotNumber];

        for (int i = 0; i < slotNumber; i++) {
            snowFlakes[i] = new SnowFlake(ConfForGlobal.datacenterId, ConfForGlobal.machineId);
        }
        return snowFlakes;
    }

    /**
     * Provides a refresh loader for dynamic class loading.
     *
     * @return a refresh loader
     */
    @Provides
    RefreshLoader refreshLoader() {
        var classpath = Utils.projectPath("/dyn/src");
        var cl = CachedGroovyClassLoader.getInstance();
        cl.init(GroovyClassLoader.class.getClassLoader(), classpath, null);
        return RefreshLoader.create(cl.getGcl()).addDir(Utils.projectPath("/dyn/src/io/velo"));
    }

    /**
     * Provides the module configuration for the application.
     *
     * @return the configured module
     */
    @Override
    protected final Module getModule() {
        var threadFactory = new NamedThreadFactory("net-worker");
        return combine(
                ServiceGraphModule.builder()
                        .with(Eventloop.class, ServiceAdapters.forEventloop(threadFactory))
                        .build(),
                WorkerPoolModule.create(),
                ConfigModule.builder()
                        .withEffectiveConfigLogger()
                        .build(),
                getBusinessLogicModule()
        );
    }

    /**
     * Provides the business logic module.
     *
     * @return the business logic module
     */
    protected Module getBusinessLogicModule() {
        return new InnerModule();
    }

    @Inject
    Config configInject;

    @Inject
    TaskRunnable[] scheduleRunnableArray;

    Eventloop primaryEventloop;

    PrimaryTaskRunnable primaryScheduleRunnable;

    /**
     * Configures the event loop as a scheduler.
     *
     * @param slotWorkerEventloop the event loop for the slot worker
     * @param index               the index of the slot worker
     */
    private void eventloopAsScheduler(Eventloop slotWorkerEventloop, int index) {
        var taskRunnable = scheduleRunnableArray[index];
        taskRunnable.setSlotWorkerEventloop(slotWorkerEventloop);
        taskRunnable.setRequestHandler(requestHandlerArray[index]);

        var localPersist = LocalPersist.getInstance();
        taskRunnable.chargeOneSlots(localPersist.oneSlots());

        // interval 1s
        slotWorkerEventloop.delay(1000L, taskRunnable);
    }

    @VisibleForTesting
    long[] slotWorkerThreadIds;

    @VisibleForTesting
    long[] netWorkerThreadIds;

    private boolean isReuseNetWorkerEventloop = false;

    /**
     * Starts the application.
     *
     * @throws Exception if an error occurs during startup
     */
    @Override
    protected void onStart() throws Exception {
        // already checked in beforeCreateHandler
        int slotWorkers = configInject.get(ofInteger(), "slotWorkers", 1);
        int netWorkers = configInject.get(ofInteger(), "netWorkers", 1);

        slotWorkerEventloopArray = new Eventloop[slotWorkers];
        slotWorkerThreadIds = new long[slotWorkers];

        // reuse networker thread
        if (slotWorkers == 1 && netWorkers == 1) {
            isReuseNetWorkerEventloop = true;
            logger.warn("Slot worker thread reuse net worker thread");

            var eventloop = netWorkerEventloopArray[0];
            slotWorkerEventloopArray[0] = eventloop;
            // networker eventloop is already running
            assert eventloop != null && eventloop.getEventloopThread() != null;
            slotWorkerThreadIds[0] = eventloop.getEventloopThread().threadId();

            // start schedule
            eventloopAsScheduler(eventloop, 0);
        } else {
            for (int i = 0; i < slotWorkers; i++) {
                var eventloop = Eventloop.builder()
                        .withThreadName("slot-worker-" + i)
                        .withIdleInterval(Duration.ofMillis(ConfForGlobal.eventloopIdleMillis))
                        .build();
                eventloop.keepAlive(true);

                var waitLatch = new CountDownLatch(1);
                final long[] threadIdArray = new long[1];
                new Thread(() -> {
                    threadIdArray[0] = Thread.currentThread().threadId();
                    logger.info("Slot worker eventloop thread created, thread id={}", threadIdArray[0]);
                    waitLatch.countDown();
                    eventloop.run();
                }).start();
                waitLatch.await();

                slotWorkerEventloopArray[i] = eventloop;
                slotWorkerThreadIds[i] = threadIdArray[0];

                // start schedule
                eventloopAsScheduler(eventloop, i);
            }
        }
        logger.info("Slot worker eventloop scheduler started");

        netWorkerThreadIds = new long[netWorkers];
        for (int i = 0; i < netWorkers; i++) {
            var eventloop = netWorkerEventloopArray[i];
            netWorkerThreadIds[i] = eventloop.getEventloopThread().threadId();
        }

        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = slotWorkerThreadIds;
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = netWorkerThreadIds;

        var aclUsers = AclUsers.getInstance();
        aclUsers.initBySlotWorkerEventloopArray(slotWorkerEventloopArray);
        aclUsers.loadAclFile();

        BlockingList.initBySlotWorkerEventloopArray(slotWorkerEventloopArray);

        // start primary schedule
        primaryScheduleRunnable = new PrimaryTaskRunnable(loopCount -> {
            // load and execute groovy scripts if changed, every 10s
            if (loopCount % 10 == 0) {
                // will not throw exception
                refreshLoader.refresh();
            }

            if (loopCount % 5 == 0) {
                // need catch exception, or will not delay run a task
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
            localPersist.fixSlotThreadId(slot, slotWorkerThreadIds[i]);
        }

        localPersist.startIndexHandlerPool();

        socketInspector.initByNetWorkerEventloopArray(slotWorkerEventloopArray, netWorkerEventloopArray);
        localPersist.setSocketInspector(socketInspector);

        var isWalLazyReadOk = localPersist.walLazyRead();
        if (!isWalLazyReadOk) {
            throw new RuntimeException("Wal lazy read from file failed");
        }

        // metrics
        CollectorRegistry.defaultRegistry.register(new StandardExports());
        CollectorRegistry.defaultRegistry.register(new BufferPoolsExports());
        CollectorRegistry.defaultRegistry.register(new MemoryPoolsExports());
        CollectorRegistry.defaultRegistry.register(new GarbageCollectorExports());
        logger.info("Prometheus jvm hotspot metrics registered");

        UP_TIME = System.currentTimeMillis();
        STATIC_GLOBAL_V.resetInfoServer(configInject);

        var c = ConfForSlot.global;
        boolean isWarmUpWhenStart;
        if (c.confBucket.lruPerFd.maxSize == c.confBucket.bucketsPerSlot) {
            isWarmUpWhenStart = true;
        } else {
            isWarmUpWhenStart = configInject.get(ofBoolean(), "bucket.lruPerFd.isWarmUpWhenStart", false);
        }
        if (isWarmUpWhenStart) {
            var isWarmUpOk = localPersist.warmUp();
            if (!isWarmUpOk) {
                throw new RuntimeException("Warm up failed");
            }
        }

        localPersist.initCheck();

        for (var scheduleRunnable : scheduleRunnableArray) {
            scheduleRunnable.startDone(true);
        }
    }

    /**
     * Performs replication after leader selection.
     *
     * @param slot the slot index
     */
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

    /**
     * Runs the application.
     *
     * @throws Exception if an error occurs during execution
     */
    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    public static volatile boolean isStopping = false;

    /**
     * Stops the application.
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    protected void onStop() throws Exception {
        isStopping = true;
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

            var leaderSelector = LeaderSelector.getInstance();
            leaderSelector.cleanUp();
            var jedisPoolHolder = JedisPoolHolder.getInstance();
            jedisPoolHolder.cleanUp();

            // close local persist
            var localPersist = LocalPersist.getInstance();
            localPersist.cleanUp();

            var dictMap = DictMap.getInstance();
            dictMap.cleanUp();

            if (!isReuseNetWorkerEventloop) {
                for (int i = 0; i < slotWorkerEventloopArray.length; i++) {
                    var slotWorkerEventloop = slotWorkerEventloopArray[i];
                    slotWorkerEventloop.breakEventloop();

                    System.out.println("Slot worker " + i + " eventloop wake up");
                    int finalI = i;
                    slotWorkerEventloop.execute(() -> {
                        System.out.println("Slot worker " + finalI + " eventloop stopping");
                    });
                }
            }

            for (int i = 0; i < netWorkerEventloopArray.length; i++) {
                var netWorkerEventloop = netWorkerEventloopArray[i];
                // for unit test
                if (netWorkerEventloop == null) {
                    continue;
                }

                System.out.println("Net worker " + i + " eventloop wake up");
                int finalI = i;
                netWorkerEventloop.execute(() -> {
                    System.out.println("Net worker " + finalI + " eventloop stopping");
                });
            }

            System.out.println("Primary eventloop wake up");
//            primaryEventloop.breakEventloop();
            primaryEventloop.execute(() -> {
                System.out.println("Primary eventloop stopping");
            });

            RuntimeCpuCollector.close();
        } catch (Exception e) {
            System.err.println("Stop error=" + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // release file lock
            if (pidFileLock != null) {
                try {
                    pidFileLock.release();
                    pidFileChannel.close();
                    if (!pidFile.delete()) {
                        System.err.println("Delete pid file failed, file=" + pidFile.getAbsolutePath());
                    }
                } catch (IOException e) {
                    System.err.println("Release file lock error=" + e.getMessage());
                }
            }
        }
    }

    /**
     * Inner module class for configuring the application.
     */
    static class InnerModule extends AbstractModule {
        private final Logger logger = getLogger(getClass());

        @TestOnly
        boolean skipZookeeperConnectCheck = false;

        /**
         * Provides the configuration for a slot.
         *
         * @param config the configuration object
         * @return the configuration for the slot
         * @throws IOException if an I/O error occurs
         */
        @Provides
        ConfForSlot confForSlot(Config config) throws IOException {
            // global conf
            ConfForGlobal.estimateKeyNumber = config.get(ofLong(), "estimateKeyNumber", 1_000_000L);
            log.warn("Global config, estimateKeyNumber={}", ConfForGlobal.estimateKeyNumber);
            ConfForGlobal.estimateOneValueLength = config.get(ofInteger(), "estimateOneValueLength", 200);
            log.warn("Global config, estimateOneValueLength={}", ConfForGlobal.estimateOneValueLength);
            ConfForGlobal.keyAnalysisNumberPercent = config.get(ofInteger(), "keyAnalysisNumberPercent", 1);
            log.warn("Global config, keyAnalysisNumberPercent={}", ConfForGlobal.keyAnalysisNumberPercent);

            ConfForGlobal.datacenterId = config.get(ofLong(), "datacenterId", 0L);
            ConfForGlobal.machineId = config.get(ofLong(), "machineId", 0L);
            log.warn("Global config, datacenterId={}", ConfForGlobal.datacenterId);
            log.warn("Global config, machineId={}", ConfForGlobal.machineId);

            ConfForGlobal.isValueSetUseCompression = config.get(ofBoolean(), "isValueSetUseCompression", true);
            ConfForGlobal.isOnDynTrainDictForCompression = config.get(ofBoolean(), "isOnDynTrainDictForCompression", false);
            log.warn("Global config, isValueSetUseCompression={}", ConfForGlobal.isValueSetUseCompression);
            log.warn("Global config, isOnDynTrainDictForCompression={}", ConfForGlobal.isOnDynTrainDictForCompression);

            ConfForGlobal.netListenAddresses = config.get(ofString(), "net.listenAddresses", "0.0.0.0:" + PORT);
            logger.info("Net listen addresses={}", ConfForGlobal.netListenAddresses);
            ConfForGlobal.eventloopIdleMillis = config.get(ofInteger(), "eventloop.idleMillis", 10);
            log.warn("Global config, eventLoopIdleMillis={}", ConfForGlobal.eventloopIdleMillis);
            if (ConfForGlobal.eventloopIdleMillis < 10 || ConfForGlobal.eventloopIdleMillis > 100) {
                throw new IllegalArgumentException("Eventloop idle millis must be between 10 and 100");
            }

            ConfForGlobal.PASSWORD = config.get(ofString(), "password", null);

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
            log.warn("Global config, clusterEnabled={}", ConfForGlobal.clusterEnabled);

            ConfForGlobal.doubleScale = config.get(ofInteger(), "number.doubleScale", 2);
            log.warn("Global config, doubleScale={}", ConfForGlobal.doubleScale);

            ConfForGlobal.bigStringNoMemoryCopySize = config.get(ofInteger(), "bigStringNoMemoryCopySize", 1024 * 256);
            log.warn("Global config, bigStringNoMemoryCopySize={}", ConfForGlobal.bigStringNoMemoryCopySize);

            ConfForGlobal.bigStringNoCompressMinSize = config.get(ofInteger(), "bigStringNoCompressMinSize", 1024 * 256);
            log.warn("Global config, bigStringNoCompressMinSize={}", ConfForGlobal.bigStringNoCompressMinSize);

            var dynConfig = config.getChild("dynConfig");
            if (dynConfig != null) {
                dynConfig.getChildren().forEach((k, v) -> {
                    ConfForGlobal.initDynConfigItems.put(k, v.getValue());
                });
            }
            log.warn("Global config, initDynConfigItems={}", ConfForGlobal.initDynConfigItems);
            ConfForGlobal.checkIfValid();

            DictMap.TO_COMPRESS_MIN_DATA_LENGTH = config.get(ofInteger(), "toCompressMinDataLength", 64);

            ValkeyRawConfSupport.load();

            // one slot config
            var c = ConfForSlot.from(ConfForGlobal.estimateKeyNumber);
            ConfForSlot.global = c;

            boolean debugMode = config.get(ofBoolean(), "debugMode", false);
            if (debugMode) {
                c.confBucket.bucketsPerSlot = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;
                c.confBucket.initialSplitNumber = ConfForSlot.ConfBucket.debugMode.initialSplitNumber;
                c.confBucket.lruPerFd.maxSize = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;

                c.confChunk.segmentNumberPerFd = ConfForSlot.ConfChunk.debugMode.segmentNumberPerFd;
                c.confChunk.fdPerChunk = ConfForSlot.ConfChunk.debugMode.fdPerChunk;
                c.confChunk.segmentLength = ConfForSlot.ConfChunk.debugMode.segmentLength;

                c.confWal.oneChargeBucketNumber = ConfForSlot.ConfWal.debugMode.oneChargeBucketNumber;
                c.confWal.valueSizeTrigger = ConfForSlot.ConfWal.debugMode.valueSizeTrigger;
                c.confWal.shortValueSizeTrigger = ConfForSlot.ConfWal.debugMode.shortValueSizeTrigger;
            }

            var debug = Debug.getInstance();
            debug.logCmd = config.get(ofBoolean(), "debugLogCmd", false);
            debug.logTrainDict = config.get(ofBoolean(), "debugLogTrainDict", false);
            debug.logRestore = config.get(ofBoolean(), "debugLogRestore", false);
            debug.bulkLoad = config.get(ofBoolean(), "bulkLoad", false);

            // override bucket conf items
            if (config.getChild("bucket.bucketsPerSlot").hasValue()) {
                c.confBucket.bucketsPerSlot = config.get(ofInteger(), "bucket.bucketsPerSlot");
            }
            if (config.getChild("bucket.initialSplitNumber").hasValue()) {
                c.confBucket.initialSplitNumber = config.get(ofInteger(), "bucket.initialSplitNumber").byteValue();
            }
            if (config.getChild("bucket.onceScanMaxLoopCount").hasValue()) {
                c.confBucket.onceScanMaxLoopCount = config.get(ofInteger(), "bucket.onceScanMaxLoopCount").byteValue();
            }
            var bucketLruPerFdPercent = config.get(ofInteger(), "bucket.lruPerFd.percent", 100);
            if (bucketLruPerFdPercent < 0 || bucketLruPerFdPercent > 100) {
                throw new IllegalArgumentException("Key bucket fd read lru percent be between 0 and 100");
            }
            c.confBucket.lruPerFd.maxSize = bucketLruPerFdPercent / 100 * c.confBucket.bucketsPerSlot;
            c.confBucket.checkIfValid();

            // override chunk conf items
            if (config.getChild("chunk.segmentNumberPerFd").hasValue()) {
                c.confChunk.segmentNumberPerFd = config.get(ofInteger(), "chunk.segmentNumberPerFd");
            }
            if (config.getChild("chunk.fdPerChunk").hasValue()) {
                c.confChunk.fdPerChunk = Byte.parseByte(config.get("chunk.fdPerChunk"));
            }
            if (config.getChild("chunk.segmentLength").hasValue()) {
                c.confChunk.segmentLength = config.get(ofInteger(), "chunk.segmentLength");
            }
            if (config.getChild("chunk.onceReadSegmentCountWhenRepl").hasValue()) {
                c.confChunk.onceReadSegmentCountWhenRepl = config.get(ofInteger(), "chunk.onceReadSegmentCountWhenRepl");
            }
            c.confChunk.isSegmentUseCompression = config.get(ofBoolean(), "chunk.isSegmentUseCompression", false);
            c.confChunk.lruPerFd.maxSize = config.get(ofInteger(), "chunk.lruPerFd.maxSize", 0);
            c.confChunk.checkIfValid();

            // override wal conf items
            var walValueSizeTrigger = 200;
            c.confWal.oneChargeBucketNumber = config.get(ofInteger(), "wal.oneChargeBucketNumber", 32);
            c.confWal.valueSizeTrigger = config.get(ofInteger(), "wal.valueSizeTrigger", walValueSizeTrigger);
            c.confWal.shortValueSizeTrigger = config.get(ofInteger(), "wal.shortValueSizeTrigger", walValueSizeTrigger);
            c.confWal.atLeastDoPersistOnceIntervalMs = config.get(ofInteger(), "wal.atLeastDoPersistOnceIntervalMs", 2);
            c.confWal.checkAtLeastDoPersistOnceSizeRate = config.get(ofDouble(), "wal.checkAtLeastDoPersistOnceSizeRate", 0.8);
            if (config.getChild("wal.onceScanMaxLoopCount").hasValue()) {
                c.confWal.onceScanMaxLoopCount = config.get(ofInteger(), "wal.onceScanMaxLoopCount").byteValue();
            }
            c.confWal.checkIfValid();

            // override repl conf items
            var replBinlogOneFileMB = 512;
            c.confRepl.binlogOneSegmentLength = config.get(ofInteger(), "repl.binlogOneSegmentLength", 1024 * 1024);
            c.confRepl.binlogOneFileMaxLength = config.get(ofInteger(), "repl.binlogOneFileMaxLength", replBinlogOneFileMB * 1024 * 1024);
            c.confRepl.binlogForReadCacheSegmentMaxCount = config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount", 10).shortValue();
            c.confRepl.binlogFileKeepMaxCount = config.get(ofInteger(), "repl.binlogFileKeepMaxCount", 10).shortValue();
            c.confRepl.catchUpOffsetMinDiff = config.get(ofInteger(), "repl.catchUpOffsetMinDiff", 1024 * 1024);
            c.confRepl.catchUpIntervalMillis = config.get(ofInteger(), "repl.catchUpIntervalMillis", 100);
            c.confRepl.checkIfValid();

            // override other conf items
            c.lruBigString.maxSize = config.get(ofInteger(), "bigString.lru.maxSize", 1000);
            c.lruKeyAndCompressedValueEncoded.maxSize = config.get(ofInteger(), "kv.lru.maxSize", 100_000);
            if (c.lruKeyAndCompressedValueEncoded.maxSize <= 0) {
                throw new IllegalArgumentException("KV get lru max size can not be 0");
            }

            // important log
            logger.info("!!!!!!\n\nConfForSlot={}\n\n", c);
            return c;
        }

        /**
         * Initializes the handler before creation.
         *
         * @param confForSlot the configuration for the slot
         * @param snowFlakes  the array of SnowFlake instances
         * @param config      the configuration object
         * @return an integer indicating the result of the initialization
         * @throws IOException if an I/O error occurs
         */
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

            var cpuNumber = Runtime.getRuntime().availableProcessors();
            int netWorkers = config.get(ofInteger(), "netWorkers", 1);
            if (netWorkers > MAX_NET_WORKERS) {
                throw new IllegalArgumentException("Net workers too large, net workers should be less than " + MAX_NET_WORKERS);
            }
            if (netWorkers > cpuNumber) {
                throw new IllegalArgumentException("Net workers should be less or equal to cpu number");
            }
            if (slotNumber < netWorkers) {
                throw new IllegalArgumentException("Net workers should <= slot number");
            }
            if (slotNumber % netWorkers != 0) {
                throw new IllegalArgumentException("Slot number should be multiple of net workers");
            }
            ConfForGlobal.netWorkers = (byte) netWorkers;
            log.warn("Global config, netWorkers={}", ConfForGlobal.netWorkers);

            int slotWorkers = config.get(ofInteger(), "slotWorkers", 1);
            if (slotWorkers > MAX_SLOT_WORKERS) {
                throw new IllegalArgumentException("Slot workers too large, slot workers should be less than " + MAX_SLOT_WORKERS);
            }
            if (slotWorkers > cpuNumber) {
                throw new IllegalArgumentException("Slot workers should be less or equal to cpu number");
            }
            if (slotNumber < slotWorkers) {
                throw new IllegalArgumentException("Slot workers should <= slot number");
            }
            if (slotNumber % slotWorkers != 0) {
                throw new IllegalArgumentException("Slot number should be multiple of slot workers");
            }
            ConfForGlobal.slotWorkers = (byte) slotWorkers;
            log.warn("Global config, slotWorkers={}", ConfForGlobal.slotWorkers);

            int indexWorkers = config.get(ofInteger(), "indexWorkers", 1);
            if (indexWorkers > MAX_INDEX_WORKERS) {
                throw new IllegalArgumentException("Index workers too large, index workers should be less than " + MAX_INDEX_WORKERS);
            }
            if (indexWorkers > cpuNumber) {
                throw new IllegalArgumentException("Index workers should be less or equal to cpu number");
            }
            ConfForGlobal.indexWorkers = (byte) indexWorkers;
            log.warn("Global config, indexWorkers={}", ConfForGlobal.indexWorkers);

            var doFileLock = config.get(ofBoolean(), "doFileLock", true);
            var dirFile = dirFile(config, doFileLock);

            var dictMap = DictMap.getInstance();
            dictMap.initDictMap(dirFile);

            TrainSampleJob.setDictKeyPrefixEndIndex(config.get(ofInteger(), "toTrainDictWhenKeyPrefixEndIndex", 5));

            RequestHandler.initMultiShardShadows((byte) netWorkers);

            // init local persist
            // already created when inject
            var persistDir = new File(dirFile, "persist");
            var localPersist = LocalPersist.getInstance();
            localPersist.initSlots((byte) netWorkers, (short) slotNumber, snowFlakes, persistDir,
                    config.getChild("persist"));

            boolean debugMode = config.get(ofBoolean(), "debugMode", false);
            if (debugMode) {
                localPersist.debugMode();
            }

            return 0;
        }

        /**
         * Provides an array of request handlers.
         *
         * @param snowFlakes          the array of SnowFlake instances
         * @param beforeCreateHandler the result of the handler initialization
         * @param config              the configuration object
         * @return an array of request handlers
         */
        @Provides
        RequestHandler[] requestHandlerArray(SnowFlake[] snowFlakes, Integer beforeCreateHandler, Config config) {
            int slotNumber = config.get(ofInteger(), "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
            int slotWorkers = config.get(ofInteger(), "slotWorkers", 1);

            var list = new RequestHandler[slotWorkers];
            for (int i = 0; i < slotWorkers; i++) {
                list[i] = new RequestHandler((byte) i, (byte) slotWorkers, (short) slotNumber, snowFlakes[i], config);
            }
            return list;
        }

        /**
         * Provides an array of task runnable.
         *
         * @param beforeCreateHandler the result of the handler initialization
         * @param config              the configuration object
         * @return an array of task runnable
         */
        @Provides
        TaskRunnable[] scheduleRunnableArray(Integer beforeCreateHandler, Config config) {
            int slotWorkers = config.get(ofInteger(), "slotWorkers", 1);
            var list = new TaskRunnable[slotWorkers];
            for (int i = 0; i < slotWorkers; i++) {
                list[i] = new TaskRunnable((byte) i, (byte) slotWorkers);
            }
            return list;
        }

        /**
         * Provides a socket inspector.
         *
         * @param config the configuration object
         * @return the socket inspector
         */
        @Provides
        SocketInspector socketInspector(Config config) {
            int maxConnections = config.get(ofInteger(), "net.maxConnections", 1000);

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

    /**
     * Holds global static variables and methods for the server.
     */
    public static class StaticGlobalV {
        /**
         * The socket inspector for managing socket connections.
         */
        public SocketInspector socketInspector;

        /**
         * Array of slot worker thread IDs.
         * This array is immutable.
         */
        public long[] slotWorkerThreadIds;

        /**
         * Array of network worker thread IDs.
         * This array is immutable.
         */
        public long[] netWorkerThreadIds;

        /**
         * List of information about the server.
         */
        private final List<Tuple2<String, String>> infoServerList = new ArrayList<>();

        /**
         * Returns a copy of the server information list.
         *
         * @return a copy of the server information list
         */
        public List<Tuple2<String, String>> getInfoServerList() {
            return new ArrayList<>(infoServerList);
        }

        /**
         * Resets the server information list with the current configuration.
         *
         * @param config the configuration object
         */
        @VisibleForTesting
        void resetInfoServer(Config config) {
            infoServerList.clear();
            infoServerList.add(new Tuple2<>("version", "1.0.0"));
            infoServerList.add(new Tuple2<>("redis_mode", ConfForGlobal.clusterEnabled ? "cluster" : "standalone"));

            var platform = Platform.getNativePlatform();
            // Linux 5.19.0-50-generic x86_64
            var osLower = platform.getOS().toString();
            var osStr = osLower.substring(0, 1).toUpperCase() + osLower.substring(1) + " " +
                    platform.getVersion() + " " + platform.getCPU().name().toLowerCase();
            infoServerList.add(new Tuple2<>("os", osStr));
            infoServerList.add(new Tuple2<>("arch_bits", platform.is64Bit() ? "64" : "32"));
            infoServerList.add(new Tuple2<>("multiplexing_api", "epoll"));
            infoServerList.add(new Tuple2<>("java_version", System.getProperty("java.version")));

            var arr = ConfForGlobal.netListenAddresses.split(":");
            infoServerList.add(new Tuple2<>("tcp_port", arr[1]));
            infoServerList.add(new Tuple2<>("listener0", "name=tcp,bind=" + arr[0] + ",port=" + arr[1]));
            infoServerList.add(new Tuple2<>("server_time_usec", String.valueOf(UP_TIME * 1000)));

            var slotWorkers = config.get(ofInteger(), "slotWorkers", 1);
            infoServerList.add(new Tuple2<>("io_threads_active", slotWorkers.toString()));

            long processId = ProcessHandle.current().pid();
            infoServerList.add(new Tuple2<>("process_id", String.valueOf(processId)));
            infoServerList.add(new Tuple2<>("process_supervised", "no"));
        }

        /**
         * Gets the index of the current thread in the slot worker thread IDs array.
         * This method uses an array for performance reasons instead of a hashtable.
         *
         * @return the index of the current thread, or -1 if not found
         */
        public int getSlotThreadLocalIndexByCurrentThread() {
            var currentThreadId = Thread.currentThread().threadId();
            for (int i = 0; i < slotWorkerThreadIds.length; i++) {
                if (currentThreadId == slotWorkerThreadIds[i]) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Gets the index of the current thread in the network worker thread IDs array.
         * This method uses an array for performance reasons instead of a hashtable.
         *
         * @return the index of the current thread, or -1 if not found
         */
        public int getNetThreadLocalIndexByCurrentThread() {
            var currentThreadId = Thread.currentThread().threadId();
            for (int i = 0; i < netWorkerThreadIds.length; i++) {
                if (currentThreadId == netWorkerThreadIds[i]) {
                    return i;
                }
            }
            return -1;
        }
    }

    /**
     * Static instance of the global variables and methods.
     */
    public static final StaticGlobalV STATIC_GLOBAL_V = new StaticGlobalV();
}
