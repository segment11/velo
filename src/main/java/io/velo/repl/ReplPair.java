package io.velo.repl;

import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.TcpSocket;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.RequestHandler;
import io.velo.persist.BigStringFiles;
import io.velo.repl.content.Hello;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * Master-slave replication pair for a slot.
 */
public class ReplPair {
    /**
     * Constructs a new ReplPair with the specified parameters.
     *
     * @param slot     the slot identifier for this pair
     * @param asMaster whether this pair acts as the master
     * @param host     the hostname or IP address of the server
     * @param port     the port number on which the server is listening
     */
    public ReplPair(short slot, boolean asMaster, String host, int port) {
        this.slot = slot;
        this.asMaster = asMaster;
        this.host = host;
        this.port = port;
    }

    private final short slot;
    private final boolean asMaster;
    private final String host;
    private final int port;

    private long connectTimeoutMillis = 5000;

    /**
     * @param connectTimeoutMillis connection timeout in milliseconds
     */
    public void setConnectTimeoutMillis(long connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /**
     * @return the slot index
     */
    public short getSlot() {
        return slot;
    }

    /**
     * @return the hostname or IP address
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port number
     */
    public int getPort() {
        return port;
    }

    /**
     * @return host:port string
     */
    public String getHostAndPort() {
        return host + ":" + port;
    }

    private static final Logger log = LoggerFactory.getLogger(ReplPair.class);

    /**
     * A record to hold a host and port combination.
     */
    public record HostAndPort(String host, int port) {
    }

    /**
     * Parses a host and port string into a HostAndPort record.
     *
     * @param hostAndPort the host and port string to parse
     * @return the HostAndPort record, or null if the input is null
     */
    public static HostAndPort parseHostAndPort(String hostAndPort) {
        if (hostAndPort == null) {
            return null;
        }

        var separatorIndex = hostAndPort.indexOf(':');
        if (separatorIndex <= 0 || separatorIndex != hostAndPort.lastIndexOf(':')
                || separatorIndex == hostAndPort.length() - 1) {
            throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort);
        }

        var host = hostAndPort.substring(0, separatorIndex);
        try {
            var port = Integer.parseInt(hostAndPort.substring(separatorIndex + 1));
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort);
            }
            return new HostAndPort(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort, e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        var replPair = (ReplPair) obj;
        return asMaster == replPair.asMaster && slot == replPair.slot && port == replPair.port && host.equals(replPair.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + Short.hashCode(slot);
        result = 31 * result + Boolean.hashCode(asMaster);
        return result;
    }

    @Override
    public String toString() {
        if (asMaster) {
            return "ReplPair{" +
                    "slot=" + slot +
                    ", asMaster=" + true +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", masterUuid=" + masterUuid +
                    ", slaveUuid=" + slaveUuid +
                    ", lastPingGetTimestamp=" + lastPingGetTimestamp +
                    ", isLinkUp=" + isLinkUpAnyOk() +
                    '}';
        } else {
            return "ReplPair{" +
                    "slot=" + slot +
                    ", asMaster=" + false +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", masterUuid=" + masterUuid +
                    ", slaveUuid=" + slaveUuid +
                    ", lastPongGetTimestamp=" + lastPongGetTimestamp +
                    ", slaveCatchUpLastSeq=" + slaveCatchUpLastSeq +
                    ", fetchedBytesLengthTotal=" + fetchedBytesLengthTotal +
                    ", isMasterReadonly=" + isMasterReadonly +
                    ", isAllCaughtUp=" + isAllCaughtUp +
                    ", isLinkUp=" + isLinkUpAnyOk() +
                    ", isMasterCanNotConnect=" + isMasterCanNotConnect +
                    ", lastGetCatchUpResponseMillis=" + lastGetCatchUpResponseMillis +
                    '}';
        }
    }

    /**
     * @return true if this pair acts as master
     */
    public boolean isAsMaster() {
        return asMaster;
    }

    private long masterUuid;

    /**
     * @return the master UUID
     */
    public long getMasterUuid() {
        return masterUuid;
    }

    /**
     * @param masterUuid the master UUID
     */
    public void setMasterUuid(long masterUuid) {
        this.masterUuid = masterUuid;
    }

    private long slaveUuid;

    /**
     * @return the slave UUID
     */
    public long getSlaveUuid() {
        return slaveUuid;
    }

    /**
     * Remote repl properties. If self is master, this is the slave repl properties. If self is slave, this is the master repl properties.
     */
    private ConfForSlot.ReplProperties remoteReplProperties;

    /**
     * @return the remote repl properties
     */
    public ConfForSlot.ReplProperties getRemoteReplProperties() {
        return remoteReplProperties;
    }

    /**
     * @param remoteReplProperties the remote repl properties
     */
    public void setRemoteReplProperties(ConfForSlot.ReplProperties remoteReplProperties) {
        this.remoteReplProperties = remoteReplProperties;
    }

    /**
     * @param slaveUuid the slave UUID
     */
    public void setSlaveUuid(long slaveUuid) {
        this.slaveUuid = slaveUuid;
    }

    // client side send ping, server side update timestamp
    @ForMasterField
    private long lastPingGetTimestamp;

    /**
     * @return the last ping timestamp in milliseconds
     */
    public long getLastPingGetTimestamp() {
        return lastPingGetTimestamp;
    }

    /**
     * @param lastPingGetTimestamp the last ping timestamp in milliseconds
     */
    public void setLastPingGetTimestamp(long lastPingGetTimestamp) {
        this.lastPingGetTimestamp = lastPingGetTimestamp;
    }

    // server side send pong, client side update timestamp
    @ForSlaveField
    private long lastPongGetTimestamp;

    /**
     * @return the last pong timestamp in milliseconds
     */
    public long getLastPongGetTimestamp() {
        return lastPongGetTimestamp;
    }

    /**
     * @param lastPongGetTimestamp the last pong timestamp in milliseconds
     */
    public void setLastPongGetTimestamp(long lastPongGetTimestamp) {
        this.lastPongGetTimestamp = lastPongGetTimestamp;
    }


    private final long[] statsCountForReplType = new long[ReplType.values().length];

    /**
     * @param type the REPL operation type
     */
    public void increaseStatsCountForReplType(ReplType type) {
        int i = type.ordinal();
        statsCountForReplType[i]++;

        // only log for catch up
        if (type == ReplType.catch_up || type == ReplType.s_catch_up) {
            if (slot == 0 && statsCountForReplType[i] % 1000 == 0) {
                log.info("Repl pair stats count for repl type, alive, target host={}, port={}, stats={}, slot={}",
                        host, port, getStatsCountForReplTypeAsString(), slot);
            }
        }
    }

    /**
     * @return statistics string for all REPL operation types
     */
    public String getStatsCountForReplTypeAsString() {
        var sb = new StringBuilder();
        for (var type : ReplType.values()) {
            sb.append(type.name()).append("=").append(statsCountForReplType[type.ordinal()]).append("\n");
        }
        return sb.toString();
    }

    @ForSlaveField
    private long slaveCatchUpLastSeq;

    /**
     * @return the last catch-up sequence number
     */
    public long getSlaveCatchUpLastSeq() {
        return slaveCatchUpLastSeq;
    }

    /**
     * @param slaveCatchUpLastSeq the last catch-up sequence number
     */
    public void setSlaveCatchUpLastSeq(long slaveCatchUpLastSeq) {
        this.slaveCatchUpLastSeq = slaveCatchUpLastSeq;
    }

    @ForSlaveField
    private long slaveCatchUpLastTimeMillisInMaster;

    /**
     * @return the last catch-up time in master in milliseconds
     */
    public long getSlaveCatchUpLastTimeMillisInMaster() {
        return slaveCatchUpLastTimeMillisInMaster;
    }

    @ForSlaveField
    private long slaveCatchUpLastTimeMillisInSlave;

    /**
     * @param slaveCatchUpLastTimeMillisInMaster the last catch-up time in master in milliseconds
     */
    public void setSlaveCatchUpLastTimeMillis(long slaveCatchUpLastTimeMillisInMaster) {
        this.slaveCatchUpLastTimeMillisInMaster = slaveCatchUpLastTimeMillisInMaster;
        this.slaveCatchUpLastTimeMillisInSlave = System.currentTimeMillis();
    }

    /**
     * @return the difference between slave and master catch-up times in milliseconds
     */
    public long getSlaveCatchUpLastTimeMillisDiff() {
        return slaveCatchUpLastTimeMillisInSlave - slaveCatchUpLastTimeMillisInMaster;
    }

    @ForMasterField
    @ForSlaveField
    private Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset;

    /**
     * @return the last catch-up binlog file index and offset
     */
    public Binlog.FileIndexAndOffset getSlaveLastCatchUpBinlogFileIndexAndOffset() {
        return slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    /**
     * @return the REPL offset of the last catch-up binlog position
     */
    public long getSlaveLastCatchUpBinlogAsReplOffset() {
        if (slaveLastCatchUpBinlogFileIndexAndOffset == null) {
            return 0L;
        }
        return slaveLastCatchUpBinlogFileIndexAndOffset.asReplOffset();
    }

    /**
     * @param slaveLastCatchUpBinlogFileIndexAndOffset the last catch-up binlog position
     */
    public void setSlaveLastCatchUpBinlogFileIndexAndOffset(Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset) {
        this.slaveLastCatchUpBinlogFileIndexAndOffset = slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    @ForSlaveField
    private Binlog.FileIndexAndOffset masterBinlogCurrentFileIndexAndOffset;

    /**
     * @return the current master binlog file index and offset
     */
    public Binlog.FileIndexAndOffset getMasterBinlogCurrentFileIndexAndOffset() {
        return masterBinlogCurrentFileIndexAndOffset;
    }

    /**
     * @param masterBinlogCurrentFileIndexAndOffset the current master binlog position
     */
    public void setMasterBinlogCurrentFileIndexAndOffset(Binlog.FileIndexAndOffset masterBinlogCurrentFileIndexAndOffset) {
        this.masterBinlogCurrentFileIndexAndOffset = masterBinlogCurrentFileIndexAndOffset;
    }

    @ForMasterField
    @ForSlaveField
    private long fetchedBytesLengthTotal;

    /**
     * @return total fetched bytes
     */
    public long getFetchedBytesLengthTotal() {
        return fetchedBytesLengthTotal;
    }

    /**
     * @param fetchedBytesLength bytes to add to total
     */
    public void increaseFetchedBytesLength(int fetchedBytesLength) {
        fetchedBytesLengthTotal += fetchedBytesLength;
    }

    // for slave check if it can fail over as master
    @ForSlaveField
    private boolean isMasterReadonly;

    /**
     * @return true if master is read-only
     */
    public boolean isMasterReadonly() {
        return isMasterReadonly;
    }

    /**
     * @param masterReadonly read-only flag
     */
    public void setMasterReadonly(boolean masterReadonly) {
        isMasterReadonly = masterReadonly;
    }

    @ForMasterField
    @ForSlaveField
    private boolean isAllCaughtUp;

    /**
     * @return true if slave has caught up with all changes
     */
    public boolean isAllCaughtUp() {
        return isAllCaughtUp;
    }

    /**
     * @param allCaughtUp caught-up flag
     */
    public void setAllCaughtUp(boolean allCaughtUp) {
        isAllCaughtUp = allCaughtUp;
    }

    @ForSlaveField
    private boolean isMasterCanNotConnect;

    /**
     * @return true if master cannot be connected
     */
    public boolean isMasterCanNotConnect() {
        return isMasterCanNotConnect;
    }

    /**
     * @param masterCanNotConnect cannot connect flag
     */
    public void setMasterCanNotConnect(boolean masterCanNotConnect) {
        isMasterCanNotConnect = masterCanNotConnect;
    }

    // only for slave pull, master never push
    @ForSlaveField
    private TcpClient tcpClient;

    /**
     * @param eventloop      the event loop for network operations
     * @param requestHandler the request handler
     */
    public void initAsSlave(Eventloop eventloop, RequestHandler requestHandler) {
        // for unit test
        if (eventloop == null) {
            return;
        }

        if (System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                && tcpClient != null && tcpClient.isSocketConnected()) {
            log.warn("Repl pair init as slave: already connected, target host={}, port={}, slot={}", host, port, slot);
        } else {
            // Advertise the Sentinel-reachable address (replicaAnnounceIp/Port) instead of the raw
            // netListenAddresses (typically 0.0.0.0). Otherwise the master's INFO replication reports
            // slaveN:ip=0.0.0.0 and Redis Sentinel cannot reach the replica for monitoring / promotion.
            var announcedHp = ConfForGlobal.getAnnouncedHostAndPort();
            var advertisedAddress = (announcedHp[0] != null && !announcedHp[0].isEmpty())
                    ? announcedHp[0] + ":" + announcedHp[1]
                    : ConfForGlobal.netListenAddresses;
            var replContent = new Hello(slaveUuid, advertisedAddress);

            tcpClient = new TcpClient(slot, eventloop, requestHandler, this);
            tcpClient.connect(host, port, connectTimeoutMillis, () -> Repl.buffer(slaveUuid, slot, ReplType.hello, replContent));
        }
    }

    @ForSlaveField
    private long disconnectTimeMillis;

    /**
     * @return the last disconnection timestamp in milliseconds
     */
    public long getDisconnectTimeMillis() {
        return disconnectTimeMillis;
    }

    /**
     * @param disconnectTimeMillis the disconnection timestamp in milliseconds
     */
    public void setDisconnectTimeMillis(long disconnectTimeMillis) {
        this.disconnectTimeMillis = disconnectTimeMillis;
    }

    @ForSlaveField
    private long lastGetCatchUpResponseMillis;

    /**
     * @return the last catch-up response timestamp in milliseconds
     */
    public long getLastGetCatchUpResponseMillis() {
        return lastGetCatchUpResponseMillis;
    }

    /**
     * @param lastGetCatchUpResponseMillis the last catch-up response timestamp in milliseconds
     */
    public void setLastGetCatchUpResponseMillis(long lastGetCatchUpResponseMillis) {
        this.lastGetCatchUpResponseMillis = lastGetCatchUpResponseMillis;
    }

    // as both master and slave field
    // change 3 -> 5 or 10
    private final boolean[] linkUpFlagArray = new boolean[3];

    /**
     * @param flag connection status flag
     */
    private void addLinkUpFlag(boolean flag) {
        for (int i = 1; i < linkUpFlagArray.length; i++) {
            linkUpFlagArray[i - 1] = linkUpFlagArray[i];
        }
        linkUpFlagArray[linkUpFlagArray.length - 1] = flag;
    }

    /**
     * @return true if any link-up flag is true
     */
    public boolean isLinkUpAnyOk() {
        for (var flag : linkUpFlagArray) {
            if (flag) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if connection is link-up
     */
    public boolean isLinkUp() {
        if (asMaster) {
            var isPingReceivedOk = System.currentTimeMillis() - lastPingGetTimestamp < 1000 * 3;
            addLinkUpFlag(isPingReceivedOk);
            return isLinkUpAnyOk();
        } else {
            var isPongReceivedOk = System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                    && tcpClient != null && tcpClient.isSocketConnected();
            addLinkUpFlag(isPongReceivedOk);
            return isLinkUpAnyOk();
        }
    }

    @ForMasterField
    TcpSocket slaveConnectSocketInMaster;

    public void setSlaveConnectSocketInMaster(TcpSocket slaveConnectSocketInMaster) {
        if (this.slaveConnectSocketInMaster != null && this.slaveConnectSocketInMaster != slaveConnectSocketInMaster) {
            log.warn("Repl pair master, slave connect not null, overwrite");
        }

        this.slaveConnectSocketInMaster = slaveConnectSocketInMaster;
    }

    public void closeSlaveConnectSocket() {
        if (slaveConnectSocketInMaster != null && !slaveConnectSocketInMaster.isClosed()) {
            try {
                slaveConnectSocketInMaster.getReactor().submit(slaveConnectSocketInMaster::close);
                log.warn("Repl pair master close slave socket, {}:{}, slot={}", host, port, slot);
            } catch (Exception e) {
                log.error("Repl pair master close slave socket error={}, {}:{}, slot={}", e.getMessage(), host, port, slot);
            } finally {
                slaveConnectSocketInMaster = null;
            }
        }
    }

    // as both master and slave field
    @VisibleForTesting
    boolean isSendBye = false;

    /**
     * @return true if bye message has been sent
     */
    public boolean isSendBye() {
        return isSendBye;
    }

    /**
     * @param isSendBye bye sent flag
     */
    @TestOnly
    public void setSendBye(boolean isSendBye) {
        this.isSendBye = isSendBye;
    }

    /**
     * @return true if bye message was sent successfully
     */
    public boolean bye() {
        isSendBye = true;
        if (tcpClient != null) {
            return tcpClient.bye();
        }
        return false;
    }

    /**
     * @return true if ping was sent successfully
     */
    public boolean ping() {
        if (isSendBye) {
            return false;
        }

        if (tcpClient != null) {
            return tcpClient.ping();
        }
        return false;
    }

    /**
     * @param type    the message type
     * @param content the message content
     * @return true if write was successful
     */
    public boolean write(ReplType type, ReplContent content) {
        if (isSendBye) {
            return false;
        }

        if (tcpClient != null) {
            return tcpClient.write(type, content);
        }
        return false;
    }

    /**
     * Closes the connection to the server.
     */
    public void close() {
        if (tcpClient != null) {
            tcpClient.close();
            tcpClient = null;
        }

        closeSlaveConnectSocket();
    }

    /**
     * Timestamp for delayed removal from list.
     */
    private long putToDelayListToRemoveTimeMillis;

    /**
     * @return timestamp to remove this pair from delay list
     */
    public long getPutToDelayListToRemoveTimeMillis() {
        return putToDelayListToRemoveTimeMillis;
    }

    /**
     * @param putToDelayListToRemoveTimeMillis timestamp to remove this pair
     */
    public void setPutToDelayListToRemoveTimeMillis(long putToDelayListToRemoveTimeMillis) {
        this.putToDelayListToRemoveTimeMillis = putToDelayListToRemoveTimeMillis;
    }

    /**
     * Big string files to fetch after catch-up.
     */
    @ForSlaveField
    private final LinkedList<BigStringFiles.IdWithKey> toFetchBigStringIdList = new LinkedList<>();
    /**
     * Big string files currently being fetched.
     */
    @ForSlaveField
    private final LinkedList<BigStringFiles.IdWithKey> doFetchingBigStringIdList = new LinkedList<>();

    /**
     * @return list of big string files to fetch
     */
    public LinkedList<BigStringFiles.IdWithKey> getToFetchBigStringIdList() {
        return toFetchBigStringIdList;
    }

    /**
     * @return list of big string files being fetched
     */
    public LinkedList<BigStringFiles.IdWithKey> getDoFetchingBigStringIdList() {
        return doFetchingBigStringIdList;
    }

    /**
     * @param uuid        the big string file UUID
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param key         the key
     */
    public void addToFetchBigStringId(long uuid, int bucketIndex, long keyHash, String key) {
        toFetchBigStringIdList.add(new BigStringFiles.IdWithKey(uuid, bucketIndex, keyHash, key));
    }

    /**
     * @return next big string id to fetch, or null if none
     */
    public BigStringFiles.IdWithKey doingFetchBigStringId() {
        if (toFetchBigStringIdList.isEmpty()) {
            return null;
        }
        var first = toFetchBigStringIdList.pollFirst();
        doFetchingBigStringIdList.add(first);
        return first;
    }

    /**
     * @param uuid the UUID of the fetched big string
     */
    public void doneFetchBigStringUuid(long uuid) {
        doFetchingBigStringIdList.removeIf(e -> e.uuid() == uuid);
    }
}
