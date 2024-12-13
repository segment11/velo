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

public abstract class E2ePerfTestMultiNetWorkerServer extends Launcher {
    private static final int DEFAULT_NET_WORKERS = 2;

    int netWorkers;
    Eventloop[] netWorkerEventloopArray;
    HashMap<String, byte[]>[] inMemoryLocalData;

    public static final String PROPERTIES_FILE = "velo-e2e-test.properties";

    @Inject
    PrimaryServer primaryServer;

    @Provides
    NioReactor primaryReactor(Config config) {
        return Eventloop.builder()
                .initialize(ofEventloop(config.getChild("eventloop.primary")))
                .build();
    }

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

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        var netWorkersGiven = config.get(ofInteger(), "netWorkers", DEFAULT_NET_WORKERS);
        netWorkers = netWorkersGiven;
        netWorkerEventloopArray = new Eventloop[netWorkersGiven];

        inMemoryLocalData = new HashMap[netWorkersGiven];
        for (int i = 0; i < netWorkersGiven; i++) {
            inMemoryLocalData[i] = new HashMap<>();
        }

        return workerPools.createPool(netWorkersGiven);
    }

    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    @Provides
    Config config() {
        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(7379)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofSystemProperties("velo-e2e-test"));
    }

    abstract Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket);

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

    protected Module getBusinessLogicModule() {
        return Module.empty();
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    /*
    start server:
    java -Xmx2g -Xms2g -XX:+UseZGC -XX:+ZGenerational -Dvelo-e2e-test-netWorkers=2 -cp velo-1.0.0.jar io.velo.perf.E2ePerfTestMultiNetWorkerServer
    run benchmark
    redis-benchmark -p 7379 -r 1000000 -n 10000000 -c 2 -d 200 -t set --threads 1 -P 10
    result:
    60w+ qps
    p999 0.2ms
    p9999 1ms
     */
    public static void main(String[] args) throws Exception {
        Launcher launcher = new E2ePerfTestMultiNetWorkerServer() {
            @Override
            Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
                var cmd = request.cmd();
                if (cmd.equals("ping")) {
                    return Promise.of(PongReply.INSTANCE.buffer());
                }

                if (cmd.equals("set")) {
                    var keyBytes = request.getData()[1];
                    var s = BaseCommand.slot(keyBytes, netWorkers);
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
