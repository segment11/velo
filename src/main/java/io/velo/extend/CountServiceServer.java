package io.velo.extend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
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
import io.velo.SocketInspector;
import io.velo.decode.Request;
import io.velo.decode.RequestDecoder;
import io.velo.reply.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;

/**
 * A server that manages and handles requests for a count service.
 * This server uses a worker pool to handle network connections and count service operations.
 */
public abstract class CountServiceServer extends Launcher {
    private static final int DEFAULT_NET_WORKERS = 2;

    /**
     * The number of network workers.
     */
    int netWorkers;

    /**
     * An array of event loops for network workers.
     */
    Eventloop[] netWorkerEventloopArray;

    /**
     * An array of count services, one for each network worker.
     */
    CountService[] countServices;

    /**
     * An array of thread IDs for network workers.
     */
    long[] threadIdArray;

    private static final Logger log = LoggerFactory.getLogger(CountServiceServer.class);

    /**
     * Asynchronously reads the count service state from the last saved files for all slots.
     *
     * @return true if all read operations are successful, false otherwise
     */
    boolean lazyReadFromFile() {
        var beginT = System.currentTimeMillis();
        CompletableFuture<Boolean>[] fArray = new CompletableFuture[countServices.length];
        for (int i = 0; i < countServices.length; i++) {
            var countService = countServices[i];
            fArray[i] = countService.lazyReadFromFile();
        }

        log.info("Wait for all slots read from file done");
        CompletableFuture.allOf(fArray).join();
        var endT = System.currentTimeMillis();
        log.info("All slots read from file done, cost={}ms", endT - beginT);

        var isEveryOk = true;
        for (var f : fArray) {
            if (f.isCompletedExceptionally()) {
                isEveryOk = false;
                break;
            }
        }
        return isEveryOk;
    }

    /**
     * The properties file name for configuration.
     */
    public static final String PROPERTIES_FILE = "velo-count-service.properties";

    /**
     * The primary server instance.
     */
    @Inject
    PrimaryServer primaryServer;

    /**
     * Provides the primary NioReactor.
     *
     * @param config the configuration object
     * @return the primary NioReactor
     */
    @Provides
    NioReactor primaryReactor(Config config) {
        return Eventloop.builder().initialize(ofEventloop(config.getChild("eventloop.primary"))).build();
    }

    /**
     * Provides the worker NioReactor.
     *
     * @param workerId             the worker ID
     * @param throttlingController the optional throttling controller
     * @param config               the configuration object
     * @return the worker NioReactor
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
     * Provides the worker pool.
     *
     * @param workerPools the worker pools
     * @param persistDir  the persistence directory
     * @param config      the configuration object
     * @return the worker pool
     */
    @Provides
    WorkerPool workerPool(WorkerPools workerPools, File persistDir, Config config) {
        var netWorkersGiven = config.get(ofInteger(), "netWorkers", DEFAULT_NET_WORKERS);
        netWorkers = netWorkersGiven;
        netWorkerEventloopArray = new Eventloop[netWorkersGiven];
        countServices = new CountService[netWorkersGiven];

        var initBytesArraySize = config.get(ofInteger(), "initBytesArraySize", 1024 * 1024);
        final var useCompress = false;
        for (int i = 0; i < netWorkersGiven; i++) {
            countServices[i] = new CountService(initBytesArraySize, useCompress);
            countServices[i].setSlot((byte) i);
            countServices[i].setPersistDir(persistDir);
        }

        return workerPools.createPool(netWorkersGiven);
    }

    /**
     * Provides the primary server.
     *
     * @param primaryReactor the primary reactor
     * @param workerServers  the list of worker servers
     * @param config         the configuration object
     * @return the primary server
     */
    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    /**
     * Provides the configuration object.
     *
     * @return the configuration object
     */
    @Provides
    Config config() {
        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(7379)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofSystemProperties("velo-count-service"));
    }

    /**
     * Handles a single request.
     *
     * @param request the request to handle
     * @param socket  the TCP socket associated with the request
     * @return a promise of the response as a ByteBuf
     */
    abstract Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket);

    /**
     * Handles a pipeline of requests.
     *
     * @param pipeline the pipeline of requests
     * @param socket   the TCP socket associated with the requests
     * @return a promise of the combined response as a ByteBuf
     */
    private Promise<ByteBuf> handlePipeline(ArrayList<Request> pipeline, ITcpSocket socket) {
        if (pipeline == null) {
            return Promise.of(null);
        }

        if (pipeline.size() == 1) {
            return handleRequest(pipeline.getFirst(), socket);
        }

        Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
        for (int i = 0; i < pipeline.size(); i++) {
            var promiseI = handleRequest(pipeline.get(i), socket);
            promiseN[i] = promiseI;
        }

        return Promises.toArray(ByteBuf.class, promiseN).map(bufs -> {
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
     * Provides the worker server.
     *
     * @param reactor         the NioReactor for the worker
     * @param socketInspector the socket inspector
     * @param config          the configuration object
     * @return the worker server
     */
    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Config config) {
        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .mapAsync(pipeline -> handlePipeline(pipeline, socket))
                                .streamTo(ChannelConsumers.ofSocket(socket)))
                .withSocketInspector(socketInspector)
                .build();
    }

    /**
     * Gets the module configuration for the application.
     *
     * @return the module configuration
     */
    @Override
    protected final Module getModule() {
        return combine(
                ServiceGraphModule.create(),
                WorkerPoolModule.create(),
                ConfigModule.builder().withEffectiveConfigLogger().build(),
                getBusinessLogicModule()
        );
    }

    /**
     * Gets the business logic module configuration.
     *
     * @return the business logic module configuration
     */
    protected Module getBusinessLogicModule() {
        return Module.empty();
    }

    /**
     * Runs the server.
     *
     * @throws Exception if an error occurs during server execution
     */
    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    /**
     * The socket inspector.
     */
    @Inject
    SocketInspector socketInspector;

    /**
     * Provides the socket inspector.
     *
     * @param config the configuration object
     * @return the socket inspector
     */
    @Provides
    SocketInspector socketInspector(Config config) {
        int maxConnections = config.get(ofInteger(), "maxConnections", 1000);

        var r = new SocketInspector();
        r.setMaxConnections(maxConnections);
        return r;
    }

    /**
     * The persistence directory.
     */
    @Inject
    File persistDir;

    /**
     * Provides the persistence directory.
     *
     * @param config the configuration object
     * @return the persistence directory
     */
    @Provides
    File persistDir(Config config) {
        var dirPath = config.get(ofString(), "dir", "/tmp/velo-count-service-data");
        log.warn("Dir path={}", dirPath);

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
        return persistDir;
    }

    /**
     * Stops the server and performs cleanup operations.
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    protected void onStop() throws Exception {
        if (socketInspector != null) {
            socketInspector.isServerStopped = true;
            socketInspector.socketMap.values().forEach(socket -> {
                socket.getReactor().submit(() -> {
                    socket.close();
                    System.out.println("Close connected socket=" + socket.getRemoteAddress());
                });
            });
        }

        for (var netWorkerEventloop : netWorkerEventloopArray) {
            System.out.println("Net worker eventloop wake up");
            netWorkerEventloop.execute(() -> {
                System.out.println("Net worker eventloop stopping");
            });
        }

        // save slots data
        log.warn("Save slots data to file before exit.");
        for (var countService : countServices) {
            countService.writeToSavedFile();
        }
    }

    /**
     * The main method to start the server.
     *
     * @param args the command-line arguments
     * @throws Exception if an error occurs during server launch
     */
    public static void main(String[] args) throws Exception {
        Launcher launcher = new CountServiceServer() {
            @Override
            protected void onStart() throws Exception {
                threadIdArray = new long[netWorkerEventloopArray.length];

                for (int i = 0; i < netWorkerEventloopArray.length; i++) {
                    var netWorkerEventloop = netWorkerEventloopArray[i];
                    assert netWorkerEventloop.getEventloopThread() != null;
                    threadIdArray[i] = netWorkerEventloop.getEventloopThread().threadId();
                }

                var isLazyReadOk = lazyReadFromFile();
                if (!isLazyReadOk) {
                    throw new RuntimeException("Count service lazy read from file failed");
                }
            }

            @Override
            Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
                var cmd = request.cmd();
                var data = request.getData();

                if (cmd.equals("config")) {
                    // for management
                    // config get view-metrics
                    if (data.length != 3) {
                        return Promise.of(ErrorReply.FORMAT.buffer());
                    }

                    var subCmd = new String(data[1]).toLowerCase();
                    if (subCmd.equals("get")) {
                        var item = new String(data[2]).toLowerCase();
                        if (item.equals("view-metrics")) {
                            // for management
                            var om = new ObjectMapper();
                            var replies = new BulkReply[countServices.length];
                            try {
                                for (int i = 0; i < replies.length; i++) {
                                    var map = countServices[i].collect();
                                    replies[i] = new BulkReply(om.writeValueAsBytes(map));
                                }
                                return Promise.of(new MultiBulkReply(replies).buffer());
                            } catch (JsonProcessingException e) {
                                return Promise.of(new ErrorReply(e.getMessage()).buffer());
                            }
                        } else {
                            return Promise.of(ErrorReply.FORMAT.buffer());
                        }
                    } else if (subCmd.equals("set")) {
                        return Promise.of(OKReply.INSTANCE.buffer());
                    }
                }

                if (cmd.equals("incrby")) {
                    if (data.length != 3) {
                        return Promise.of(ErrorReply.FORMAT.buffer());
                    }

                    var keyBytes = request.getData()[1];
                    int added;
                    try {
                        added = Integer.parseInt(new String(request.getData()[2]));
                    } catch (NumberFormatException e) {
                        return Promise.of(ErrorReply.NOT_INTEGER.buffer());
                    }

                    var s = BaseCommand.slot(keyBytes, netWorkers);
                    var countService = countServices[s.slot()];
                    var targetEventloop = netWorkerEventloopArray[s.slot()];

                    var currentThreadId = Thread.currentThread().threadId();
                    assert targetEventloop.getEventloopThread() != null;
                    if (targetEventloop.getEventloopThread().threadId() == currentThreadId) {
                        try {
                            var count = countService.increase(s.keyHash(), s.keyHash32(), added);
                            return Promise.of(new IntegerReply(count).buffer());
                        } catch (InvalidProtocolBufferException e) {
                            return Promise.of(new ErrorReply(e.getMessage()).buffer());
                        }
                    } else {
                        final int added0 = added;
                        return Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> {
                            var count = countService.increase(s.keyHash(), s.keyHash32(), added0);
                            return new IntegerReply(count).buffer();
                        })));
                    }
                }

                if (cmd.equals("get")) {
                    var keyBytes = request.getData()[1];
                    var s = BaseCommand.slot(keyBytes, netWorkers);

                    var countService = countServices[s.slot()];
                    var targetEventloop = netWorkerEventloopArray[s.slot()];

                    var currentThreadId = Thread.currentThread().threadId();
                    assert targetEventloop.getEventloopThread() != null;
                    if (targetEventloop.getEventloopThread().threadId() == currentThreadId) {
                        try {
                            var count = countService.get(s.keyHash(), s.keyHash32());
                            return Promise.of(new IntegerReply(count).buffer());
                        } catch (InvalidProtocolBufferException e) {
                            return Promise.of(new ErrorReply(e.getMessage()).buffer());
                        }
                    } else {
                        return Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> {
                            var count = countService.get(s.keyHash(), s.keyHash32());
                            return new IntegerReply(count).buffer();
                        })));
                    }
                }

                if (cmd.equals("save")) {
                    var promises = new Promise[countServices.length];

                    var currentThreadId = Thread.currentThread().threadId();
                    for (int i = 0; i < netWorkerEventloopArray.length; i++) {
                        var targetEventloop = netWorkerEventloopArray[i];
                        if (targetEventloop.getEventloopThread().threadId() == currentThreadId) {
                            try {
                                countServices[i].writeToSavedFile();
                                promises[i] = Promise.complete();
                            } catch (IOException e) {
                                promises[i] = Promise.ofException(e);
                            }
                        } else {
                            int finalI = i;
                            promises[i] = Promise.ofFuture(targetEventloop.submit(() -> {
                                try {
                                    countServices[finalI].writeToSavedFile();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }));
                        }
                    }

                    return Promises.all(promises).then(($, e) -> e != null ?
                            Promise.of(new ErrorReply(e.getMessage()).buffer())
                            : Promise.of(OKReply.INSTANCE.buffer()));
                }

                return Promise.of(NilReply.INSTANCE.buffer());
            }
        };
        launcher.launch(args);
    }
}