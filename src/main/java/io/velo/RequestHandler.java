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
import io.velo.persist.LocalPersist;
import io.velo.persist.ReadonlyException;
import io.velo.repl.LeaderSelector;
import io.velo.repl.cluster.MultiShard;
import io.velo.repl.cluster.MultiShardShadow;
import io.velo.repl.cluster.MultiSlotRange;
import io.velo.reply.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static io.activej.config.converter.ConfigConverters.ofInteger;

@ThreadNeedLocal
public class RequestHandler {
    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private static final String PING_COMMAND = "ping";
    public static final String AUTH_COMMAND = "auth";
    private static final String GET_COMMAND = "get";
    private static final String SET_COMMAND = "set";
    private static final String QUIT_COMMAND = "quit";
    private static final String ERROR_FOR_STAT_AS_COMMAND = "x_error";
    private static final String READONLY_FOR_STAT_AS_COMMAND = "x_readonly";

    public static final String AS_KEY_GET_SLOT_RANGE_IN_CURRENT_CONNECTION_THREAD_LOCALLY = "x_slot_range";

    @VisibleForTesting
    final byte workerId;
    private final String workerIdStr;
    @VisibleForTesting
    final byte netWorkers;
    @VisibleForTesting
    final short slotNumber;
    @VisibleForTesting
    final SnowFlake snowFlake;

    @TestOnly
    final boolean localTest;
    @TestOnly
    final int localTestRandomValueListSize;
    @TestOnly
    final ArrayList<byte[]> localTestRandomValueList;

    int trainSampleListMaxSize;

    final CompressStats compressStats;

    final TrainSampleJob trainSampleJob;
    final List<TrainSampleJob.TrainSampleKV> sampleToTrainList = new CopyOnWriteArrayList<>();

    volatile boolean isStopped = false;

    void stop() {
        System.out.println("Worker " + workerId + " stopped callback");
        isStopped = true;
    }

    @Override
    public String toString() {
        return "RequestHandler{" +
                "workerId=" + workerId +
                ", netWorkers=" + netWorkers +
                ", slotNumber=" + slotNumber +
                ", localTest=" + localTest +
                ", sampleToTrainList.size=" + sampleToTrainList.size() +
                ", isStopped=" + isStopped +
                '}';
    }

    public RequestHandler(byte workerId, byte netWorkers, short slotNumber, SnowFlake snowFlake, Config config) {
        this.workerId = workerId;
        this.workerIdStr = String.valueOf(workerId);
        this.netWorkers = netWorkers;
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;

        var toInt = ofInteger();
        this.localTest = config.get(ofBoolean(), "localTest", false);
        var localTestRandomValueLength = config.get(toInt, "localTestRandomValueLength", 200);
        this.localTestRandomValueListSize = config.get(toInt, "localTestRandomValueListSize", 10000);
        this.localTestRandomValueList = new ArrayList<>(localTestRandomValueListSize);
        if (this.localTest) {
            var rand = new Random();
            for (int i = 0; i < localTestRandomValueListSize; i++) {
                var value = new byte[localTestRandomValueLength];
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) rand.nextInt(Byte.MAX_VALUE + 1);
                }
                localTestRandomValueList.add(value);
            }
            log.info("Local test random value list mocked, size={}, value length={}", localTestRandomValueListSize, localTestRandomValueLength);
        }

        var requestConfig = config.getChild("request");

        this.compressStats = new CompressStats("net_worker_" + workerId, "net_");
        // compress and train sample dict requestConfig
        this.trainSampleListMaxSize = requestConfig.get(toInt, "trainSampleListMaxSize", 1000);

        this.trainSampleJob = new TrainSampleJob(workerId);
        this.trainSampleJob.setDictSize(requestConfig.get(toInt, "dictSize", 1024));
        this.trainSampleJob.setTrainSampleMinBodyLength(requestConfig.get(toInt, "trainSampleMinBodyLength", 4096));

        this.initCommandGroups();
        this.initMetricsCollect();
    }

    private static MultiShardShadow[] multiShardShadows;

    public static void initMultiShardShadows(byte netWorkers) {
        multiShardShadows = new MultiShardShadow[netWorkers];
        for (int i = 0; i < netWorkers; i++) {
            multiShardShadows[i] = new MultiShardShadow();
        }
    }

    public static void updateMultiShardShadows(MultiShard multiShard) {
        var mySelfShard = multiShard.mySelfShard();
        var shards = multiShard.getShards();

        for (var shardShadow : multiShardShadows) {
            shardShadow.setMySelfShard(mySelfShard);
            shardShadow.setShards(shards);
        }
    }

    static MultiShardShadow getMultiShardShadow() {
        return multiShardShadows[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()];
    }

    private final BaseCommand[] commandGroups = new BaseCommand[26];

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

        for (var cmd : commandGroups) {
            cmd.snowFlake = snowFlake;
        }
    }

    // cross threads, need be thread safe
    public void parseSlots(@NotNull Request request) {
        var cmd = request.cmd();
        if (cmd.equals(PING_COMMAND) || cmd.equals(QUIT_COMMAND) || cmd.equals(AUTH_COMMAND)) {
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
    private static final String URL_QUERY_FOR_CMD_STAT_COUNT = "cmd_stat_count";
    private static final String HEADER_NAME_FOR_BASIC_AUTH = "Authorization";

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

    // request time summary already include all cmd, for all handlers
    // all cmd count is less than 1k, so need no rehash
    private final HashMap<String, Long> cmdStatCountMap = new HashMap<>(1000);

    @VisibleForTesting
    long increaseCmdStatArray(String cmd) {
        var count = cmdStatCountMap.getOrDefault(cmd, 0L);
        count++;
        cmdStatCountMap.put(cmd, count);
        return count;
    }

    @VisibleForTesting
    String cmdStatAsPrometheusFormatString() {
        var sb = new StringBuilder();
        for (var entry : cmdStatCountMap.entrySet()) {
            sb.append("cmd_stat_count{cmd=\"").append(entry.getKey()).append("\",worker_id=\"").append(workerIdStr).append("\"} ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    @TestOnly
    long cmdStatCountTotal() {
        long total = 0;
        for (var entry : cmdStatCountMap.entrySet()) {
            total += entry.getValue();
        }
        return total;
    }

    private long getCmdCountStat(String cmd) {
        return cmdStatCountMap.getOrDefault(cmd, 0L);
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

    Reply handle(@NotNull Request request, ITcpSocket socket) {
        if (isStopped) {
            return ErrorReply.SERVER_STOPPED;
        }

        var data = request.getData();

        if (request.isRepl()) {
            var xGroup = new XGroup(null, data, socket);
            xGroup.init(this, request);

            // try catch in handle repl method
            return xGroup.handleRepl();
        }

        // http special handle
        if (request.isHttp() && data.length == 1) {
            // metrics, prometheus format
            // url should be ?metrics, eg: http://localhost:7379/?metrics
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

            var firstDataString = new String(firstDataBytes);
            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_MASTER)) {
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (isMaster) {
                    // will response 200 status code
                    return new BulkReply("master".getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_MASTER_OR_SLAVE)) {
                // will response 200 status code
                return new BulkReply("master or slave".getBytes());
            }

            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE)) {
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (!isMaster) {
                    // will response 200 status code
                    return new BulkReply("slave".getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            if (firstDataString.startsWith(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE)) {
                // eg. slave_with_zone=zone1
                var targetZone = firstDataString.substring(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE.length() + 1);
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (isMaster) {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }

                if (targetZone.equals(ConfForGlobal.targetAvailableZone)) {
                    // will response 200 status code
                    return new BulkReply(("slave with zone " + targetZone).getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            // cmd_stat_count or cmd_stat_count=cmd or cmd_stat_count=all
            if (firstDataString.startsWith(URL_QUERY_FOR_CMD_STAT_COUNT)) {
                var cmd = firstDataString.length() <= (URL_QUERY_FOR_CMD_STAT_COUNT.length() + 1) ? "all" :
                        firstDataString.substring(URL_QUERY_FOR_CMD_STAT_COUNT.length() + 1);
                if ("all".equals(cmd)) {
                    return new BulkReply(cmdStatAsPrometheusFormatString().getBytes());
                } else {
                    return new BulkReply(String.valueOf(getCmdCountStat(cmd)).getBytes());
                }
            }
        }

        if (data[0] == null) {
            return ErrorReply.FORMAT;
        }

        var cmd = request.cmd();

        Summary.Timer requestTimer = null;

        if (ConfForGlobal.requestSummary) {
            requestTimer = requestTimeSummary.labels(cmd).startTimer();
        }
        try {
            if (cmd.equals(PING_COMMAND)) {
                increaseCmdStatArray(PING_COMMAND);

                return PongReply.INSTANCE;
            }

            var doLogCmd = Debug.getInstance().logCmd;
            if (doLogCmd) {
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

            if (cmd.equals(QUIT_COMMAND)) {
                socket.close();
                return OKReply.INSTANCE;
            }

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

                    var aclUsers = AclUsers.getInstance();
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
                    increaseCmdStatArray(AUTH_COMMAND);

                    if (data.length != 2 && data.length != 3) {
                        return ErrorReply.FORMAT;
                    }

                    var user = data.length == 3 ? new String(data[1]).toLowerCase() : U.DEFAULT_USER;
                    var passwordRaw = new String(data[data.length - 1]);

                    // acl check
                    var aclUsers = AclUsers.getInstance();
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
                return ErrorReply.NO_AUTH;
            }

            if (cmd.equals(GET_COMMAND)) {
                increaseCmdStatArray(GET_COMMAND);

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
                    var xGroup = new XGroup(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH, dataTransfer, socket).init(this, request);
                    try {
                        return xGroup.handle();
                    } catch (Exception e) {
                        increaseCmdStatArray(ERROR_FOR_STAT_AS_COMMAND);
                        log.error("XGroup handle error", e);
                        return new ErrorReply(e.getMessage());
                    }
                }

                // for redis protocol compatible client, get slot range in current connection thread
                // so need not post to other threads when client use this connection, for performance
                // need change Jedis to support this feature, refer BaseCommand.calSlotInRedisClientWhenNeedBetterPerf
                if (key.equals(AS_KEY_GET_SLOT_RANGE_IN_CURRENT_CONNECTION_THREAD_LOCALLY)) {
                    ArrayList<Short> slots = new ArrayList<>();

                    // for redis cluster crc16 slots
                    var eachSlotChargeRedisClusterSlotNumber = MultiShard.TO_CLIENT_SLOT_NUMBER / slotNumber;
                    var multiSlotRange = new MultiSlotRange();

                    var currentThreadId = Thread.currentThread().threadId();
                    var localPersist = LocalPersist.getInstance();
                    var oneSlots = localPersist.oneSlots();
                    for (var oneSlot : oneSlots) {
                        if (oneSlot.getThreadIdProtectedForSafe() == currentThreadId) {
                            slots.add(oneSlot.slot());

                            // for redis cluster crc16 slots
                            var toClientSlotStart = oneSlot.slot() * eachSlotChargeRedisClusterSlotNumber;
                            var toClientSlotEnd = toClientSlotStart + eachSlotChargeRedisClusterSlotNumber - 1;
                            multiSlotRange.addSingle(toClientSlotStart, toClientSlotEnd);
                        }
                    }

                    // reply join slots, eg: 0,2,4/0-1024,2048-3072,4096-5120
                    var sb = new StringBuilder();
                    for (int i = 0; i < slots.size(); i++) {
                        sb.append(slots.get(i));
                        if (i != slots.size() - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("/");
                    sb.append(multiSlotRange);
                    return new BulkReply(sb.toString().getBytes());
                }

                var gGroup = new GGroup(cmd, data, socket).init(this, request);
                try {
                    var slotWithKeyHashList = request.getSlotWithKeyHashList();
                    var bytes = gGroup.get(keyBytes, slotWithKeyHashList.getFirst(), true);
                    return bytes != null ? new BulkReply(bytes) : NilReply.INSTANCE;
                } catch (TypeMismatchException e) {
                    increaseCmdStatArray(ERROR_FOR_STAT_AS_COMMAND);
                    return new ErrorReply(e.getMessage());
                } catch (DictMissingException e) {
                    return ErrorReply.DICT_MISSING;
                } catch (Exception e) {
                    increaseCmdStatArray(ERROR_FOR_STAT_AS_COMMAND);
                    log.error("Get error, key={}", new String(keyBytes), e);
                    return new ErrorReply(e.getMessage());
                }
            }

            // for short
            // full set command handle in SGroup
            if (cmd.equals(SET_COMMAND) && data.length == 3) {
                increaseCmdStatArray(SET_COMMAND);

                var keyBytes = data[1];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }

                // for local test, random value, test compress ratio
                var valueBytes = data[2];
                if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                    return ErrorReply.VALUE_TOO_LONG;
                }

                var sGroup = new SGroup(cmd, data, socket).init(this, request);
                try {
                    sGroup.set(keyBytes, valueBytes, sGroup.slotWithKeyHashListParsed.getFirst());
                } catch (ReadonlyException e) {
                    increaseCmdStatArray(READONLY_FOR_STAT_AS_COMMAND);
                    return ErrorReply.READONLY;
                } catch (Exception e) {
                    increaseCmdStatArray(ERROR_FOR_STAT_AS_COMMAND);
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
            try {
                increaseCmdStatArray(cmd);

                commandGroup.setCmd(cmd);
                commandGroup.setData(data);
                commandGroup.setSocket(socket);
                return commandGroup.init(this, request).handle();
            } catch (ReadonlyException e) {
                increaseCmdStatArray(READONLY_FOR_STAT_AS_COMMAND);
                return ErrorReply.READONLY;
            } catch (Exception e) {
                increaseCmdStatArray(ERROR_FOR_STAT_AS_COMMAND);
                log.error("Request handle error", e);
                return new ErrorReply(e.getMessage());
            }
        } finally {
            if (ConfForGlobal.requestSummary) {
                requestTimer.observeDuration();
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
