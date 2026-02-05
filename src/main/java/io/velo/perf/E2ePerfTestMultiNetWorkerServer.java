package io.velo.perf;

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
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import io.velo.BaseCommand;
import io.velo.decode.Request;
import io.velo.decode.RequestDecoder;
import io.velo.reply.BulkReply;
import io.velo.reply.NilReply;
import io.velo.reply.OKReply;
import io.velo.reply.PongReply;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;

/**
 * This is the server part of the e2e test. It is a multi-threaded server that handles RESP (Redis Serialization Protocol)
 * requests from the client, particularly for SET and GET commands for benchmarking purposes.
 * <p>
 * The server leverages ActiveJ's asynchronous capabilities, including event loops, worker pools, and non-blocking I/O operations.
 * </p>
 */
public abstract class E2ePerfTestMultiNetWorkerServer extends Launcher {
    private static final int DEFAULT_NET_WORKERS = 2;

    int netWorkers;
    Eventloop[] netWorkerEventloopArray;
    HashMap<String, byte[]>[] inMemoryLocalData;
    HashMap<String, byte[]>[] inMemoryThreadLocalData;
    long[] threadIdArray;

    public static final String PROPERTIES_FILE = "velo-e2e-test.properties";

    @Inject
    PrimaryServer primaryServer;

    /**
     * Provides the NIO reactor for the primary event loop.
     *
     * @param config the configuration object
     * @return a new NioReactor instance configured as the primary reactor
     */
    @Provides
    NioReactor primaryReactor(Config config) {
        return Eventloop.builder()
                .initialize(ofEventloop(config.getChild("eventloop.primary")))
                .build();
    }

    /**
     * Provides the NIO reactor for worker event loops.
     *
     * @param workerId             the unique identifier for the worker
     * @param throttlingController optional throttling controller
     * @param config               the configuration object
     * @return a new NioReactor instance configured as a worker reactor
     */
    @Provides
    @Worker
    NioReactor workerReactor(@WorkerId int workerId, OptionalDependency<ThrottlingController> throttlingController, Config config) {
        var netHandleEventloop = Eventloop.builder()
                .initialize(ofEventloop(config.getChild("eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();

        netWorkerEventloopArray[workerId] = netHandleEventloop;
        return netHandleEventloop;
    }

    /**
     * Provides a worker pool with a specified number of networkers.
     *
     * @param workerPools the worker pool manager
     * @param config      the configuration object
     * @return a new WorkerPool instance with the specified number of networkers
     */
    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        var netWorkersGiven = config.get(ofInteger(), "netWorkers", DEFAULT_NET_WORKERS);
        netWorkers = netWorkersGiven;
        netWorkerEventloopArray = new Eventloop[netWorkersGiven];

        inMemoryLocalData = new HashMap[netWorkersGiven];
        inMemoryThreadLocalData = new HashMap[netWorkersGiven];
        for (int i = 0; i < netWorkersGiven; i++) {
            inMemoryLocalData[i] = new HashMap<>();
            inMemoryThreadLocalData[i] = new HashMap<>();
        }

        return workerPools.createPool(netWorkersGiven);
    }

    /**
     * Provides the primary server instance, which coordinates networkers.
     *
     * @param primaryReactor the primary event loop reactor
     * @param workerServers  the instances of worker servers
     * @param config         the configuration object
     * @return a new PrimaryServer instance
     */
    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    /**
     * Provides the configuration for the server, including default and overridden values.
     *
     * @return a new Config instance configured for the server
     */
    @Provides
    Config config() {
        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(7379)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofSystemProperties("velo-e2e-test"));
    }

    /**
     * Abstract method to handle an individual request. Each subclass must implement this method to define
     * how requests are processed and responses are generated.
     *
     * @param request the request object to be handled
     * @param socket  the TCP socket from which the request originated
     * @return a Promise containing the response buffer
     */
    abstract Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket);

    /**
     * Handles a pipeline of requests by applying the {@link #handleRequest(Request, ITcpSocket)} method to each request.
     * If the pipeline contains multiple requests, it handles each request asynchronously and combines the responses.
     *
     * @param pipeline the list of requests to be processed
     * @param socket   the TCP socket associated with the pipeline
     * @return a Promise containing the combined response buffer
     */
    private Promise<ByteBuf> handlePipeline(ArrayList<Request> pipeline, ITcpSocket socket) {
        if (pipeline.isEmpty()) {
            return Promise.of(ByteBuf.empty());
        }

        if (pipeline.size() == 1) {
            return handleRequest(pipeline.getFirst(), socket);
        }

        Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
        for (int i = 0; i < pipeline.size(); i++) {
            var promiseI = handleRequest(pipeline.get(i), socket);
            promiseN[i] = promiseI;
        }

        return Promises.toArray(ByteBuf.class, promiseN)
                .map(bufs -> {
                    int totalN = 0;
                    for (var buf : bufs) {
                        totalN += buf.readRemaining();
                    }
                    var multiBuf = ByteBuf.wrapForWriting(new byte[totalN]);
                    for (var buf : bufs) {
                        multiBuf.put(buf);
                    }
                    return multiBuf;
                });
    }

    /**
     * Provides a simple server instance that will handle incoming TCP connections and requests.
     *
     * @param reactor the worker reactor
     * @param config  the configuration object
     * @return a new SimpleServer instance
     */
    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, Config config) {
        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .mapAsync(pipeline -> handlePipeline(pipeline, socket))
                                .streamTo(ChannelConsumers.ofSocket(socket)))
                .build();
    }

    /**
     * Returns the module that binds and configures the components of the server. This includes the service graph,
     * worker pools, and configuration module.
     *
     * @return a Module instance containing the server's components and configurations
     */
    @Override
    protected final Module getModule() {
        return combine(
                ServiceGraphModule.create(),
                WorkerPoolModule.create(),
                ConfigModule.builder()
                        .withEffectiveConfigLogger()
                        .build(),
                getBusinessLogicModule()
        );
    }

    /**
     * Returns the business logic module that may contain additional bindings and configurations
     * specific to the server's processing logic.
     *
     * @return a Module instance containing the business logic
     */
    protected Module getBusinessLogicModule() {
        return Module.empty();
    }

    /**
     * Starts the server and awaits shutdown. This method is called when the server is launched.
     *
     * @throws Exception if an error occurs during the server's startup or processing
     */
    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    /**
     * Main entry point for the server application. It initializes the server with the number of networkers and
     * the mode of using a thread-local map based on system properties. It then launches the server.
     *
     * @param args command-line arguments
     * @throws Exception if an error occurs during the server's launch
     */
    public static void main(String[] args) throws Exception {
        Launcher launcher = new E2ePerfTestMultiNetWorkerServer() {
            private static final boolean isUseThreadLocalMap;

            static {
                isUseThreadLocalMap = "true".equals(System.getProperty("useThreadLocalMap", "false"));
                System.out.println("use thread local map: " + isUseThreadLocalMap);
            }

            /**
             * Called when the server starts. Initializes the thread ID array with the thread IDs of the networker event loops.
             */
            @Override
            protected void onStart() throws Exception {
                threadIdArray = new long[netWorkerEventloopArray.length];

                for (int i = 0; i < netWorkerEventloopArray.length; i++) {
                    var netWorkerEventloop = netWorkerEventloopArray[i];
                    assert netWorkerEventloop.getEventloopThread() != null;
                    threadIdArray[i] = netWorkerEventloop.getEventloopThread().threadId();
                }
            }

            /**
             * Returns the thread-local map associated with the current thread.
             *
             * @return a HashMap<String, byte[]> instance representing the thread-local map
             * @throws IllegalStateException if the thread-local map is not found
             */
            HashMap<String, byte[]> threadLocalMap() {
                var threadId = Thread.currentThread().threadId();
                for (int i = 0; i < threadIdArray.length; i++) {
                    if (threadId == threadIdArray[i]) {
                        return inMemoryThreadLocalData[i];
                    }
                }
                throw new IllegalStateException("thread local map not found");
            }

            /**
             * Handles an individual request by determining the command type and processing accordingly.
             * Supports "ping", "set", and "get" commands.
             *
             * @param request the request object to be handled
             * @param socket  the TCP socket associated with the request
             * @return a Promise containing the response buffer
             */
            @Override
            Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
                var cmd = request.cmd();
                if (cmd.equals("ping")) {
                    return Promise.of(PongReply.INSTANCE.buffer());
                }

                if (cmd.equals("set")) {
                    var keyBytes = request.getData()[1];
                    var s = BaseCommand.slot(keyBytes, netWorkers);

                    if (isUseThreadLocalMap) {
                        var map0 = threadLocalMap();
                        map0.put(new String(keyBytes), request.getData()[2]);
                        return Promise.of(OKReply.INSTANCE.buffer());
                    }

                    var map = inMemoryLocalData[s.slot()];

                    var targetEventloop = netWorkerEventloopArray[s.slot()];
                    var currentThreadId = Thread.currentThread().threadId();
                    if (targetEventloop.getEventloopThread().threadId() == currentThreadId) {
                        map.put(new String(keyBytes), request.getData()[2]);
                        return Promise.of(OKReply.INSTANCE.buffer());
                    } else {
                        return Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> {
                            map.put(new String(keyBytes), request.getData()[2]);
                            return OKReply.INSTANCE.buffer();
                        })));
                    }
                }

                if (cmd.equals("get")) {
                    var keyBytes = request.getData()[1];
                    var s = BaseCommand.slot(keyBytes, netWorkers);

                    if (isUseThreadLocalMap) {
                        var map0 = threadLocalMap();
                        var value = map0.get(new String(keyBytes));
                        return Promise.of(value == null ? NilReply.INSTANCE.buffer() : new BulkReply(value).buffer());
                    }

                    var map = inMemoryLocalData[s.slot()];

                    var targetEventloop = netWorkerEventloopArray[s.slot()];
                    var currentThreadId = Thread.currentThread().threadId();
                    if (targetEventloop.getEventloopThread().threadId() == currentThreadId) {
                        var value = map.get(new String(keyBytes));
                        return Promise.of(value == null ? NilReply.INSTANCE.buffer() : new BulkReply(value).buffer());
                    } else {
                        return Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> {
                            var value = map.get(new String(keyBytes));
                            return value == null ? NilReply.INSTANCE.buffer() : new BulkReply(value).buffer();
                        })));
                    }
                }

                return Promise.of(NilReply.INSTANCE.buffer());
            }
        };
        launcher.launch(args);
    }
}