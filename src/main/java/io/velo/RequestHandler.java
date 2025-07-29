package io.velo;

import io.activej.config.Config;
import io.activej.net.socket.tcp.ITcpSocket;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.common.TextFormat;
import io.velo.acl.AclUsers;
import io.velo.acl.U;
import io.velo.command.*;
import io.velo.decode.Request;
import io.velo.metric.SimpleGauge;
import io.velo.persist.ReadonlyException;
import io.velo.repl.LeaderSelector;
import io.velo.repl.cluster.MultiShard;
import io.velo.repl.cluster.MultiShardShadow;
import io.velo.reply.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.activej.config.converter.ConfigConverters.ofInteger;

/**
 * Handles requests for the server.
 * Need be thread safe.
 */
@ThreadNeedLocal
public class RequestHandler {
    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    static final String ECHO_COMMAND = "echo";
    static final String PING_COMMAND = "ping";
    static final String QUIT_COMMAND = "quit";
    private static final String HELLO_COMMAND = "hello";
    public static final String AUTH_COMMAND = "auth";
    private static final String GET_COMMAND = "get";
    private static final String SET_COMMAND = "set";

    @VisibleForTesting
    final byte workerId;
    private final String workerIdStr;
    @VisibleForTesting
    final byte slotWorkers;
    @VisibleForTesting
    final short slotNumber;
    @VisibleForTesting
    final SnowFlake snowFlake;

    int trainSampleListMaxSize;

    final CompressStats compressStats;

    final TrainSampleJob trainSampleJob;
    final List<TrainSampleJob.TrainSampleKV> sampleToTrainList = new CopyOnWriteArrayList<>();

    volatile boolean isStopped = false;

    /**
     * Stops the request handler.
     */
    void stop() {
        System.out.println("Worker " + workerId + " stopped callback");
        isStopped = true;
    }

    @Override
    public String toString() {
        return "RequestHandler{" +
                "workerId=" + workerId +
                ", slotWorkers=" + slotWorkers +
                ", slotNumber=" + slotNumber +
                ", sampleToTrainList.size=" + sampleToTrainList.size() +
                ", isStopped=" + isStopped +
                '}';
    }

    /**
     * Constructs a new RequestHandler.
     *
     * @param workerId    the worker ID
     * @param slotWorkers the number of network workers
     * @param slotNumber  the slot number
     * @param snowFlake   the SnowFlake instance
     * @param config      the configuration object
     */
    public RequestHandler(byte workerId, byte slotWorkers, short slotNumber, SnowFlake snowFlake, Config config) {
        this.workerId = workerId;
        this.workerIdStr = String.valueOf(workerId);
        this.slotWorkers = slotWorkers;
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;

        var requestConfig = config.getChild("request");

        this.compressStats = new CompressStats("slot_worker_" + workerId, "slot_");
        // compress and train sample dict requestConfig
        this.trainSampleListMaxSize = requestConfig.get(ofInteger(), "trainSampleListMaxSize", 1000);

        this.trainSampleJob = new TrainSampleJob(workerId);
        this.trainSampleJob.setDictSize(requestConfig.get(ofInteger(), "dictSize", 1024));
        this.trainSampleJob.setTrainSampleMinBodyLength(requestConfig.get(ofInteger(), "trainSampleMinBodyLength", 4096));

        this.initCommandGroups();
        this.initMetricsCollect();
    }

    @ThreadNeedLocal
    private static MultiShardShadow[] multiShardShadows;

    /**
     * Initializes the multi-shard shadows.
     *
     * @param slotWorkers the number of slot workers
     */
    public static void initMultiShardShadows(byte slotWorkers) {
        multiShardShadows = new MultiShardShadow[slotWorkers];
        for (int i = 0; i < slotWorkers; i++) {
            multiShardShadows[i] = new MultiShardShadow();
        }
    }

    /**
     * Updates the multi-shard shadows.
     * This method is not thread safe.
     *
     * @param multiShard the multi-shard instance
     */
    public static synchronized void updateMultiShardShadows(MultiShard multiShard) {
        var mySelfShard = multiShard.mySelfShard();
        var shards = multiShard.getShards();

        for (var shardShadow : multiShardShadows) {
            shardShadow.setMySelfShard(mySelfShard);
            shardShadow.setShards(shards);
        }
    }

    /**
     * Gets the multi-shard shadow for the current thread.
     *
     * @return the multi-shard shadow
     */
    static MultiShardShadow getMultiShardShadow() {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getSlotThreadLocalIndexByCurrentThread();
        return multiShardShadows[threadIndex];
    }

    private final BaseCommand[] commandGroups = new BaseCommand[26];

    private GGroup reuseGGroup;
    private SGroup reuseSGroup;

    /**
     * Initializes the command groups.
     * These command groups can be reused in the same worker thread.
     */
    private void initCommandGroups() {
        commandGroups[0] = new AGroup(null, null, null);
        commandGroups[1] = new BGroup(null, null, null);
        commandGroups[2] = new CGroup(null, null, null);
        commandGroups[3] = new DGroup(null, null, null);
        commandGroups[4] = new EGroup(null, null, null);
        commandGroups[5] = new FGroup(null, null, null);
        commandGroups[6] = new GGroup(null, null, null);
        commandGroups[7] = new HGroup(null, null, null);
        commandGroups[8] = new IGroup(null, null, null);
        commandGroups[9] = new JGroup(null, null, null);
        commandGroups[10] = new KGroup(null, null, null);
        commandGroups[11] = new LGroup(null, null, null);
        commandGroups[12] = new MGroup(null, null, null);
        commandGroups[13] = new NGroup(null, null, null);
        commandGroups[14] = new OGroup(null, null, null);
        commandGroups[15] = new PGroup(null, null, null);
        commandGroups[16] = new QGroup(null, null, null);
        commandGroups[17] = new RGroup(null, null, null);
        commandGroups[18] = new SGroup(null, null, null);
        commandGroups[19] = new TGroup(null, null, null);
        commandGroups[20] = new UGroup(null, null, null);
        commandGroups[21] = new VGroup(null, null, null);
        commandGroups[22] = new WGroup(null, null, null);
        commandGroups[23] = new XGroup(null, null, null);
        commandGroups[24] = new YGroup(null, null, null);
        commandGroups[25] = new ZGroup(null, null, null);

        for (var commandGroup : commandGroups) {
            commandGroup.snowFlake = snowFlake;
            commandGroup.init(this);
        }

        reuseGGroup = (GGroup) commandGroups[6];
        reuseSGroup = (SGroup) commandGroups[18];
    }

    /**
     * Gets the command group for the given first byte.
     *
     * @param firstByte the first byte of the command
     * @return the command group
     */
    public BaseCommand getA_ZGroupCommand(byte firstByte) {
        return commandGroups[firstByte - 'a'];
    }

    private final ArrayList<BaseCommand.SlotWithKeyHash> emptySlotWithKeyHashList = new ArrayList<>();

    /**
     * Parses the slots for the given request.
     * This method is not thread safe.
     *
     * @param request the request
     */
    public void parseSlots(@NotNull Request request) {
        var cmd = request.cmd();
        if (cmd.equals(ECHO_COMMAND)
                || cmd.equals(PING_COMMAND)
                || cmd.equals(QUIT_COMMAND)
                || cmd.equals(HELLO_COMMAND)
                || cmd.equals(AUTH_COMMAND)) {
            request.setSlotWithKeyHashList(emptySlotWithKeyHashList);
            return;
        }

        var data = request.getData();
        var firstByte = data[0][0];
        if (firstByte >= 'A' && firstByte <= 'Z') {
            firstByte += 32;
        }
        if (firstByte < 'a' || firstByte > 'z') {
            return;
        }

        var commandGroup = commandGroups[firstByte - 'a'];
        if (commandGroup instanceof GGroup) {
            if (data.length >= 2) {
                var keyBytes = data[1];
                var key = new String(keyBytes);
                if (key.startsWith(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH)) {
                    var dataTransfer = transferDataForXGroup(key);
                    // x index position is 23
                    var slotWithKeyHashListForXGroup = commandGroups[23].parseSlots(
                            XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH, dataTransfer, request.getSlotNumber());
                    request.setSlotWithKeyHashList(slotWithKeyHashListForXGroup);
                    return;
                }
            }
        }

        var slotWithKeyHashList = commandGroup.parseSlots(cmd, data, request.getSlotNumber());
        request.setSlotWithKeyHashList(slotWithKeyHashList);
    }

    private static final byte[] URL_QUERY_METRICS_BYTES = "metrics".getBytes();
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_MASTER = "master";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_MASTER_OR_SLAVE = "master_or_slave";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_SLAVE = "slave";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE = "slave_with_zone";
    private static final String HEADER_NAME_FOR_BASIC_AUTH = "Authorization";

    /**
     * Transfers data for the XGroup command.
     *
     * @param keyAsData the key as data
     * @return the transferred data
     */
    private static byte[][] transferDataForXGroup(String keyAsData) {
        // eg. get x_repl,sub_cmd,sub_sub_cmd,***
        // transfer data to: x_repl sub_cmd sub_sub_cmd ***
        var array = keyAsData.split(",");
        var dataTransfer = new byte[array.length][];
        for (int i = 0; i < array.length; i++) {
            dataTransfer[i] = array[i].getBytes();
        }
        return dataTransfer;
    }

    private static final Summary requestTimeSummary = Summary.build()
            .name("request_time")
            .help("Request time in seconds.")
            .labelNames("command")
            .maxAgeSeconds(60)
            .ageBuckets(5)
            .quantile(0.99, 0.001)
            .quantile(0.999, 0.001)
            .register();

    private final Debug debug = Debug.getInstance();

    private final AclUsers aclUsers = AclUsers.getInstance();

    /**
     * Handles the given request.
     *
     * @param request the request
     * @param socket  the socket
     * @return the reply
     */
    Reply handle(@NotNull Request request, ITcpSocket socket) {
        if (isStopped) {
            return ErrorReply.SERVER_STOPPED;
        }

        var data = request.getData();

        if (request.isRepl()) {
            var xGroup = new XGroup(null, data, socket, this, request);
            // try to catch in handle repl method
            return xGroup.handleRepl();
        }

        // http special handle
        if (request.isHttp() && data.length == 1) {
            // metrics, prometheus format
            // url should be ?metrics, http://localhost:7379/?metrics
            // for one target slot beside 0 metrics: http://localhost:7379/?manage&slot&0&view-metrics
            var firstDataBytes = data[0];
            if (Arrays.equals(firstDataBytes, URL_QUERY_METRICS_BYTES)) {
                var sw = new StringWriter();
                try {
                    TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples());
                    return new BulkReply(sw.toString().getBytes());
                } catch (IOException e) {
                    return new ErrorReply(e.getMessage());
                }
            }

            // for haproxy
            if (firstDataBytes == null) {
                return ErrorReply.FORMAT;
            }

            var leaderSelector = LeaderSelector.getInstance();
            var firstDataString = new String(firstDataBytes);
            switch (firstDataString) {
                case URL_QUERY_FOR_HAPROXY_FILTER_MASTER -> {
                    var isMaster = leaderSelector.hasLeadership();
                    if (isMaster) {
                        // will response 200 status
                        return new BulkReply("master".getBytes());
                    } else {
                        // will response 404 status
                        return NilReply.INSTANCE;
                    }
                }
                case URL_QUERY_FOR_HAPROXY_FILTER_MASTER_OR_SLAVE -> {
                    // will response 200 status
                    return new BulkReply("master or slave".getBytes());
                }
                case URL_QUERY_FOR_HAPROXY_FILTER_SLAVE -> {
                    var isMaster = leaderSelector.hasLeadership();
                    if (!isMaster) {
                        // will response 200 status
                        return new BulkReply("slave".getBytes());
                    } else {
                        // will response 404 status
                        return NilReply.INSTANCE;
                    }
                }
            }

            if (firstDataString.startsWith(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE)) {
                // eg. slave_with_zone=zone1
                var targetZone = firstDataString.substring(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE.length() + 1);
                var isMaster = leaderSelector.hasLeadership();
                if (isMaster) {
                    // will response 404 status
                    return NilReply.INSTANCE;
                }

                if (targetZone.equals(ConfForGlobal.targetAvailableZone)) {
                    // will response 200 status
                    return new BulkReply(("slave with zone " + targetZone).getBytes());
                } else {
                    // will response 404 status
                    return NilReply.INSTANCE;
                }
            }
        }

        if (data[0] == null) {
            return ErrorReply.FORMAT;
        }

        var cmd = request.cmd();
        try (var requestTimer = requestTimeSummary.labels(cmd).startTimer()) {
            // http basic auth
            if (request.isHttp()) {
                if (SocketInspector.getAuthUser(socket) == null && ConfForGlobal.PASSWORD != null) {
                    var headerValue = request.getHttpHeader(HEADER_NAME_FOR_BASIC_AUTH);
                    if (headerValue == null) {
                        return ErrorReply.NO_AUTH;
                    }

                    // base64 decode
                    // trim "Basic " prefix
                    var auth = new String(Base64.getDecoder().decode(headerValue.substring(6)));
                    var user = auth.substring(0, auth.indexOf(':'));
                    var passwordRaw = auth.substring(auth.indexOf(':') + 1);

                    var u = aclUsers.get(user);
                    if (u == null) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.isOn()) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.checkPassword(passwordRaw)) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    SocketInspector.setAuthUser(socket, user);
                    // continue to handle request
                }
            } else {
                if (cmd.equals(AUTH_COMMAND)) {
                    if (data.length != 2 && data.length != 3) {
                        return ErrorReply.FORMAT;
                    }

                    var user = data.length == 3 ? new String(data[1]).toLowerCase() : U.DEFAULT_USER;
                    var passwordRaw = new String(data[data.length - 1]);

                    // acl check
                    var u = aclUsers.get(user);
                    if (u == null) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.isOn()) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    if (!u.checkPassword(passwordRaw)) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    SocketInspector.setAuthUser(socket, user);
                    return OKReply.INSTANCE;
                }
            }

            if (SocketInspector.getAuthUser(socket) == null && ConfForGlobal.PASSWORD != null) {
                // hello can do auth
                if (!cmd.equals(HELLO_COMMAND)) {
                    return ErrorReply.NO_AUTH;
                }
            }

            // do get for shortcut
            if (cmd.equals(GET_COMMAND)) {
                if (data.length != 2) {
                    return ErrorReply.FORMAT;
                }

                var keyBytes = data[1];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }

                var key = new String(keyBytes);
                if (key.startsWith(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH)) {
                    // dispatch to XGroup
                    // eg. get x_repl,sub_cmd,sub_sub_cmd,***
                    var dataTransfer = transferDataForXGroup(key);
                    // transfer data to: x_repl sub_cmd sub_sub_cmd ***
                    var xGroup = new XGroup(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH, dataTransfer, socket, this, request);
                    try {
                        return xGroup.handle();
                    } catch (Exception e) {
                        log.error("XGroup handle error", e);
                        return new ErrorReply(e.getMessage());
                    }
                }

                reuseGGroup.resetContext(cmd, data, socket, request);
                var slotWithKeyHashList = request.getSlotWithKeyHashList();
                try {
                    var bytes = reuseGGroup.get(keyBytes, slotWithKeyHashList.getFirst(), true);
                    return bytes != null ? new BulkReply(bytes) : NilReply.INSTANCE;
                } catch (TypeMismatchException e) {
                    return new ErrorReply(e.getMessage());
                } catch (DictMissingException e) {
                    return ErrorReply.DICT_MISSING;
                } catch (Exception e) {
                    log.error("Get error, key={}", new String(keyBytes), e);
                    return new ErrorReply(e.getMessage());
                }
            }

            // do set for shortcut
            // full set command handle in SGroup
            if (cmd.equals(SET_COMMAND) && data.length == 3) {
                var keyBytes = data[1];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }

                var valueBytes = data[2];
                if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                    return ErrorReply.VALUE_TOO_LONG;
                }

                reuseSGroup.resetContext(cmd, data, socket, request);
                var slotWithKeyHashList = request.getSlotWithKeyHashList();
                try {
                    reuseSGroup.set(keyBytes, valueBytes, slotWithKeyHashList.getFirst());
                } catch (ReadonlyException e) {
                    log.warn("Set but server is readonly, key={}", new String(keyBytes));
                    return ErrorReply.READONLY;
                } catch (Exception e) {
                    log.error("Set error, key={}", new String(keyBytes), e);
                    return new ErrorReply(e.getMessage());
                }

                return OKReply.INSTANCE;
            }

            // else, use enum better
            var firstByte = data[0][0];
            if (firstByte >= 'A' && firstByte <= 'Z') {
                firstByte += 32;
            }
            if (firstByte < 'a' || firstByte > 'z') {
                return ErrorReply.FORMAT;
            }

            var commandGroup = commandGroups[firstByte - 'a'];
            commandGroup.resetContext(cmd, data, socket, request);
            try {
                return commandGroup.handle();
            } catch (ReadonlyException e) {
                log.warn("Request handle but server is readonly");
                return ErrorReply.READONLY;
            } catch (Exception e) {
                log.error("Request handle error.", e);
                return new ErrorReply(e.getMessage());
            }
        }
    }

    @VisibleForTesting
    final static SimpleGauge requestHandlerGauge = new SimpleGauge("request_handler", "Net worker request handler metrics.",
            "worker_id");

    static {
        requestHandlerGauge.register();
    }

    private void initMetricsCollect() {
        requestHandlerGauge.addRawGetter(() -> {
            var labelValues = List.of(workerIdStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("request_sample_to_train_size", new SimpleGauge.ValueWithLabelValues((double) sampleToTrainList.size(), labelValues));
            return map;
        });
    }
}
