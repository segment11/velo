package io.velo.perf;

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
import io.velo.decode.Request;
import io.velo.decode.RequestDecoder;
import io.velo.reply.OKReply;
import io.velo.reply.PongReply;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;

public abstract class E2ePerfTestMultiNetWorkerServer extends Launcher {
    private final int DEFAULT_PORT = 7379;
    private static final int DEFAULT_NET_WORKERS = 1;

    public static final String PROPERTIES_FILE = "velo.properties";

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
    NioReactor workerReactor(Config config, OptionalDependency<ThrottlingController> throttlingController) {
        return Eventloop.builder()
                .initialize(ofEventloop(config.getChild("eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();
    }

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        return workerPools.createPool(config.get(ofInteger(), "netWorkers", DEFAULT_NET_WORKERS));
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
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(DEFAULT_PORT)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofSystemProperties("velo-config"));
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
    Workers: 1
redis-benchmark -r 1000000 -n 10000000 -c 2 -d 200 -t set --threads 1 -p 7379 -P 10
Summary:
  throughput summary: 1745200.75 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.012     0.000     0.015     0.015     0.015     1.151

    Workers: 4
redis-benchmark -r 1000000 -n 100000000 -c 4 -d 200 -t set --threads 1 -p 7379 -P 10
Summary:
  throughput summary: 2196498.75 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.013     0.000     0.015     0.023     0.023     2.663
     */
    public static void main(String[] args) throws Exception {
        Launcher launcher = new E2ePerfTestMultiNetWorkerServer() {
            @Override
            Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
                if (request.cmd().equals("ping")) {
                    return Promise.of(PongReply.INSTANCE.buffer());
                }
                return Promise.of(OKReply.INSTANCE.buffer());
            }
        };
        launcher.launch(args);
    }
}
