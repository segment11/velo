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
 * Represents a pair of REPL (slave-master-replication) instances, one acting as the master and the other as the slave.
 * This class manages the connection details, UUIDs, timestamps, and other state information for the pair.
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

    /**
     * Gets the slot identifier for this pair.
     *
     * @return the slot identifier
     */
    public short getSlot() {
        return slot;
    }

    /**
     * Gets the hostname or IP address of the server.
     *
     * @return the hostname or IP address
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number on which the server is listening.
     *
     * @return the port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the formatted host and port as a string.
     *
     * @return the formatted host and port
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

        var parts = hostAndPort.split(":");
        return new HostAndPort(parts[0], Integer.parseInt(parts[1]));
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
        return asMaster == replPair.asMaster && port == replPair.port && host.equals(replPair.host);
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
                    ", isLinkUp=" + isLinkUp() +
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
                    ", isLinkUp=" + isLinkUp() +
                    ", isMasterCanNotConnect=" + isMasterCanNotConnect +
                    ", lastGetCatchUpResponseMillis=" + lastGetCatchUpResponseMillis +
                    '}';
        }
    }

    /**
     * Checks if this pair acts as the master.
     *
     * @return true if this pair is the master, false otherwise.
     */
    public boolean isAsMaster() {
        return asMaster;
    }

    private long masterUuid;

    /**
     * Gets the UUID of the master.
     *
     * @return the master UUID
     */
    public long getMasterUuid() {
        return masterUuid;
    }

    /**
     * Sets the UUID of the master.
     *
     * @param masterUuid the master UUID to set
     */
    public void setMasterUuid(long masterUuid) {
        this.masterUuid = masterUuid;
    }

    private long slaveUuid;

    /**
     * Gets the UUID of the slave.
     *
     * @return the slave UUID
     */
    public long getSlaveUuid() {
        return slaveUuid;
    }

    @ForSlaveField
    private boolean isRedoSet = false;

    /**
     * Checks if the slave only redo set operation.
     *
     * @return true if the slave only redo set operation, false the slave can copy key buckets / chunk / wal bytes directly
     */
    public boolean isRedoSet() {
        return isRedoSet;
    }

    /**
     * Sets the slave only redo set operation.
     *
     * @param isRedoSet the value to set
     */
    public void setRedoSet(boolean isRedoSet) {
        this.isRedoSet = isRedoSet;
    }

    /**
     * Remote repl properties. If self is master, this is the slave repl properties. If self is slave, this is the master repl properties.
     */
    private ConfForSlot.ReplProperties remoteReplProperties;

    /**
     * Gets the remote repl properties.
     *
     * @return the remote repl properties
     */
    public ConfForSlot.ReplProperties getRemoteReplProperties() {
        return remoteReplProperties;
    }

    /**
     * Sets the remote repl properties.
     *
     * @param remoteReplProperties the remote repl properties to set
     */
    public void setRemoteReplProperties(ConfForSlot.ReplProperties remoteReplProperties) {
        this.remoteReplProperties = remoteReplProperties;
    }

    /**
     * Sets the UUID of the slave.
     *
     * @param slaveUuid the slave UUID to set
     */
    public void setSlaveUuid(long slaveUuid) {
        this.slaveUuid = slaveUuid;
    }

    // client side send ping, server side update timestamp
    @ForMasterField
    private long lastPingGetTimestamp;

    /**
     * Gets the timestamp of the last received ping from the master.
     *
     * @return the timestamp of the last received ping
     */
    public long getLastPingGetTimestamp() {
        return lastPingGetTimestamp;
    }

    /**
     * Sets the timestamp of the last received ping from the master.
     *
     * @param lastPingGetTimestamp the timestamp to set
     */
    public void setLastPingGetTimestamp(long lastPingGetTimestamp) {
        this.lastPingGetTimestamp = lastPingGetTimestamp;
    }

    // server side send pong, client side update timestamp
    @ForSlaveField
    private long lastPongGetTimestamp;

    /**
     * Gets the timestamp of the last received pong from the slave.
     *
     * @return the timestamp of the last received pong
     */
    public long getLastPongGetTimestamp() {
        return lastPongGetTimestamp;
    }

    /**
     * Sets the timestamp of the last received pong from the slave.
     *
     * @param lastPongGetTimestamp the timestamp to set
     */
    public void setLastPongGetTimestamp(long lastPongGetTimestamp) {
        this.lastPongGetTimestamp = lastPongGetTimestamp;
    }

    @ForSlaveField
    private final long[] statsCountWhenSlaveSkipFetch = new long[3];

    /**
     * Increases the count of a specific type of fetch skip for the slave.
     *
     * @param type the type of the fetch to skip
     */
    public void increaseStatsCountWhenSlaveSkipFetch(ReplType type) {
        int i;
        if (type == ReplType.s_exists_wal) {
            i = 0;
        } else if (type == ReplType.s_exists_chunk_segments) {
            i = 1;
        } else if (type == ReplType.s_exists_key_buckets) {
            i = 2;
        } else {
            return;
        }
        statsCountWhenSlaveSkipFetch[i]++;
    }

    /**
     * Gets the string representation of the statistics for fetch skips by the slave.
     *
     * @return the string representation of the statistics
     */
    public String getStatsCountForSlaveSkipFetchAsString() {
        return "exists_wal skip fetch count=" + statsCountWhenSlaveSkipFetch[0] +
                ", exists_chunk_segments skip fetch count=" + statsCountWhenSlaveSkipFetch[1] +
                ", exists_key_buckets skip fetch count=" + statsCountWhenSlaveSkipFetch[2];
    }

    private final long[] statsCountForReplType = new long[ReplType.values().length];

    /**
     * Increases the count for a specific type of REPL operation.
     *
     * @param type the type of the REPL operation
     */
    public void increaseStatsCountForReplType(ReplType type) {
        int i = type.ordinal();
        statsCountForReplType[i]++;

        // only log for catch up
        if (type == ReplType.catch_up || type == ReplType.s_catch_up) {
            if (statsCountForReplType[i] % 1000 == 0) {
                log.info("Repl pair stats count for repl type, alive, target host={}, port={}, stats={}, slot={}",
                        host, port, getStatsCountForReplTypeAsString(), slot);
            }
        }
    }

    /**
     * Gets the string representation of the statistics for all REPL operation types.
     *
     * @return the string representation of the statistics
     */
    public String getStatsCountForReplTypeAsString() {
        var sb = new StringBuilder();
        for (var type : ReplType.values()) {
            sb.append(type.name()).append("=").append(statsCountForReplType[type.ordinal()]).append(", ");
        }
        return sb.toString();
    }

    @ForSlaveField
    private long slaveCatchUpLastSeq;

    /**
     * Gets the last sequence number the slave has caught up to.
     *
     * @return the last caught-up sequence number for the slave
     */
    public long getSlaveCatchUpLastSeq() {
        return slaveCatchUpLastSeq;
    }

    /**
     * Sets the last sequence number the slave has caught up to.
     *
     * @param slaveCatchUpLastSeq the last caught-up sequence number to set
     */
    public void setSlaveCatchUpLastSeq(long slaveCatchUpLastSeq) {
        this.slaveCatchUpLastSeq = slaveCatchUpLastSeq;
    }

    @ForSlaveField
    private long slaveCatchUpLastTimeMillisInMaster;

    /**
     * Gets the last time the slave caught up to in milliseconds in the master.
     *
     * @return the last time the slave caught up to in milliseconds in the master
     */
    public long getSlaveCatchUpLastTimeMillisInMaster() {
        return slaveCatchUpLastTimeMillisInMaster;
    }

    @ForSlaveField
    private long slaveCatchUpLastTimeMillisInSlave;

    /**
     * Sets the last time the slave caught up to in milliseconds in the master.
     *
     * @param slaveCatchUpLastTimeMillisInMaster the last time the slave caught up to in milliseconds in the master
     */
    public void setSlaveCatchUpLastTimeMillis(long slaveCatchUpLastTimeMillisInMaster) {
        this.slaveCatchUpLastTimeMillisInMaster = slaveCatchUpLastTimeMillisInMaster;
        this.slaveCatchUpLastTimeMillisInSlave = System.currentTimeMillis();
    }

    /**
     * Gets the difference between the last time the slave caught up to in milliseconds in the slave and the master.
     *
     * @return the difference between the last time the slave caught up to in milliseconds in the slave and the master
     */
    public long getSlaveCatchUpLastTimeMillisDiff() {
        return slaveCatchUpLastTimeMillisInSlave - slaveCatchUpLastTimeMillisInMaster;
    }

    @ForMasterField
    @ForSlaveField
    private Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset;

    /**
     * Gets the last binlog file index and offset that the slave has caught up to.
     *
     * @return the binlog file index and offset
     */
    public Binlog.FileIndexAndOffset getSlaveLastCatchUpBinlogFileIndexAndOffset() {
        return slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    /**
     * Gets the REPL offset of the last binlog file index and offset that the slave has caught up to.
     *
     * @return the REPL offset of the last binlog file index and offset
     */
    public long getSlaveLastCatchUpBinlogAsReplOffset() {
        if (slaveLastCatchUpBinlogFileIndexAndOffset == null) {
            return 0L;
        }
        return slaveLastCatchUpBinlogFileIndexAndOffset.asReplOffset();
    }

    /**
     * Sets the last binlog file index and offset that the slave has caught up to.
     *
     * @param slaveLastCatchUpBinlogFileIndexAndOffset the binlog file index and offset to set
     */
    public void setSlaveLastCatchUpBinlogFileIndexAndOffset(Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset) {
        this.slaveLastCatchUpBinlogFileIndexAndOffset = slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    @ForSlaveField
    private Binlog.FileIndexAndOffset masterBinlogCurrentFileIndexAndOffset;

    /**
     * Gets the current binlog file index and offset of the master.
     *
     * @return the binlog file index and offset
     */
    public Binlog.FileIndexAndOffset getMasterBinlogCurrentFileIndexAndOffset() {
        return masterBinlogCurrentFileIndexAndOffset;
    }

    /**
     * Sets the current binlog file index and offset of the master.
     *
     * @param masterBinlogCurrentFileIndexAndOffset the binlog file index and offset to set
     */
    public void setMasterBinlogCurrentFileIndexAndOffset(Binlog.FileIndexAndOffset masterBinlogCurrentFileIndexAndOffset) {
        this.masterBinlogCurrentFileIndexAndOffset = masterBinlogCurrentFileIndexAndOffset;
    }

    @ForMasterField
    @ForSlaveField
    private long fetchedBytesLengthTotal;

    /**
     * Gets the total length of bytes fetched so far.
     *
     * @return the total length of bytes fetched
     */
    public long getFetchedBytesLengthTotal() {
        return fetchedBytesLengthTotal;
    }

    /**
     * Increases the total length of bytes fetched by a specified amount.
     *
     * @param fetchedBytesLength the amount of bytes to add to the total
     */
    public void increaseFetchedBytesLength(int fetchedBytesLength) {
        fetchedBytesLengthTotal += fetchedBytesLength;
    }

    // for slave check if it can fail over as master
    @ForSlaveField
    private boolean isMasterReadonly;

    /**
     * Checks if the master is read-only.
     *
     * @return true if the master is read-only, false otherwise.
     */
    public boolean isMasterReadonly() {
        return isMasterReadonly;
    }

    /**
     * Sets the read-only status of the master.
     *
     * @param masterReadonly the read-only status to set
     */
    public void setMasterReadonly(boolean masterReadonly) {
        isMasterReadonly = masterReadonly;
    }

    @ForMasterField
    @ForSlaveField
    private boolean isAllCaughtUp;

    /**
     * Checks if the slave has caught up with all changes from the master.
     *
     * @return true if the slave is fully caught up, false otherwise.
     */
    public boolean isAllCaughtUp() {
        return isAllCaughtUp;
    }

    /**
     * Sets the caught-up status of the slave.
     *
     * @param allCaughtUp the caught-up status to set
     */
    public void setAllCaughtUp(boolean allCaughtUp) {
        isAllCaughtUp = allCaughtUp;
    }

    @ForSlaveField
    private boolean isMasterCanNotConnect;

    /**
     * Checks if the master is not connected.
     *
     * @return true if the master is not connected, false otherwise.
     */
    public boolean isMasterCanNotConnect() {
        return isMasterCanNotConnect;
    }

    /**
     * Sets the connection status of the master.
     *
     * @param masterCanNotConnect the connection status to set
     */
    public void setMasterCanNotConnect(boolean masterCanNotConnect) {
        isMasterCanNotConnect = masterCanNotConnect;
    }

    // only for slave pull, master never push
    @ForSlaveField
    private TcpClient tcpClient;

    /**
     * Initializes this pair as a slave.
     *
     * @param eventloop      the event loop to be used for handling network operations
     * @param requestHandler the request handler for processing incoming requests
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
            var replContent = new Hello(slaveUuid, ConfForGlobal.netListenAddresses);

            tcpClient = new TcpClient(slot, eventloop, requestHandler, this);
            tcpClient.connect(host, port, () -> Repl.buffer(slaveUuid, slot, ReplType.hello, replContent));
        }
    }

    @ForSlaveField
    private long disconnectTimeMillis;

    /**
     * Gets the timestamp of the last disconnection.
     *
     * @return the disconnection timestamp
     */
    public long getDisconnectTimeMillis() {
        return disconnectTimeMillis;
    }

    /**
     * Sets the timestamp of the last disconnection.
     *
     * @param disconnectTimeMillis the timestamp to set
     */
    public void setDisconnectTimeMillis(long disconnectTimeMillis) {
        this.disconnectTimeMillis = disconnectTimeMillis;
    }

    @ForSlaveField
    private long lastGetCatchUpResponseMillis;

    /**
     * Gets the timestamp of the last catch-up response.
     *
     * @return the catch-up response timestamp
     */
    public long getLastGetCatchUpResponseMillis() {
        return lastGetCatchUpResponseMillis;
    }

    /**
     * Sets the timestamp of the last catch-up response.
     *
     * @param lastGetCatchUpResponseMillis the timestamp to set
     */
    public void setLastGetCatchUpResponseMillis(long lastGetCatchUpResponseMillis) {
        this.lastGetCatchUpResponseMillis = lastGetCatchUpResponseMillis;
    }

    // as both master and slave field
    // change 3 -> 5 or 10
    private final boolean[] linkUpFlagArray = new boolean[3];

    /**
     * Adds a connection status flag to the link-up flag array.
     *
     * @param flag the connection status flag to add
     */
    private void addLinkUpFlag(boolean flag) {
        for (int i = 1; i < linkUpFlagArray.length; i++) {
            linkUpFlagArray[i - 1] = linkUpFlagArray[i];
        }
        linkUpFlagArray[linkUpFlagArray.length - 1] = flag;
    }

    /**
     * Checks if any of the flags in the link-up flag array is true.
     *
     * @return true if any flag is true, false otherwise.
     */
    private boolean isLinkUpAnyOk() {
        for (var flag : linkUpFlagArray) {
            if (flag) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the connection is link-up based on timestamps and connection status.
     *
     * @return true if the connection is link-up, false otherwise.
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
        this.slaveConnectSocketInMaster = slaveConnectSocketInMaster;
    }

    public void closeSlaveConnectSocket() {
        if (slaveConnectSocketInMaster != null) {
            try {
                slaveConnectSocketInMaster.close();
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
     * Checks if a bye message has been sent.
     *
     * @return true if a bye message has been sent, false otherwise.
     */
    public boolean isSendBye() {
        return isSendBye;
    }

    /**
     * Sets the flag indicating if a bye message has been sent.
     *
     * @param isSendBye the flag to set
     */
    @TestOnly
    public void setSendBye(boolean isSendBye) {
        this.isSendBye = isSendBye;
    }

    /**
     * Sends a bye message to the server
     *
     * @return true if the bye message was sent successfully, false otherwise.
     */
    public boolean bye() {
        isSendBye = true;
        if (tcpClient != null) {
            return tcpClient.bye();
        }
        return false;
    }

    /**
     * Sends a ping message to the server, slave do ping
     *
     * @return true if the ping was sent successfully, false otherwise.
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
     * Writes a message to the server.
     *
     * @param type    the type of the message to be written
     * @param content the content of the message to be written
     * @return true if the write was successful, false otherwise.
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
     * Remove this repl pair self later.
     * Both master and slave can delay do remove.
     */
    private long putToDelayListToRemoveTimeMillis;

    /**
     * Gets the timestamp to remove this pair from the delay list.
     * For interval check.
     *
     * @return the timestamp to remove this pair
     */
    public long getPutToDelayListToRemoveTimeMillis() {
        return putToDelayListToRemoveTimeMillis;
    }

    /**
     * Sets the timestamp to remove this pair from the delay list.
     *
     * @param putToDelayListToRemoveTimeMillis the timestamp to set
     */
    public void setPutToDelayListToRemoveTimeMillis(long putToDelayListToRemoveTimeMillis) {
        this.putToDelayListToRemoveTimeMillis = putToDelayListToRemoveTimeMillis;
    }

    /**
     * Slave need fetch big string files after all catch up.
     */
    @ForSlaveField
    private final LinkedList<BigStringFiles.IdWithKey> toFetchBigStringIdList = new LinkedList<>();
    /**
     * Slave doing fetch big string files.
     */
    @ForSlaveField
    private final LinkedList<BigStringFiles.IdWithKey> doFetchingBigStringIdList = new LinkedList<>();

    /**
     * Gets the list of big string file UUIDs to fetch.
     *
     * @return the list of big string file UUIDs
     */
    public LinkedList<BigStringFiles.IdWithKey> getToFetchBigStringIdList() {
        return toFetchBigStringIdList;
    }

    /**
     * Gets the list of big string file UUIDs being fetching.
     *
     * @return the list of big string file UUIDs
     */
    public LinkedList<BigStringFiles.IdWithKey> getDoFetchingBigStringIdList() {
        return doFetchingBigStringIdList;
    }

    /**
     * Adds a big string file UUID to the list of files to fetch.
     *
     * @param uuid        the UUID of the big string file to fetch
     * @param bucketIndex the bucket index
     * @param keyHash     the hash of the key
     * @param key         the key of the big string file
     */
    public void addToFetchBigStringId(long uuid, int bucketIndex, long keyHash, String key) {
        toFetchBigStringIdList.add(new BigStringFiles.IdWithKey(uuid, bucketIndex, keyHash, key));
    }

    /**
     * Slave fetch a big string file id, from to fetch, FIFO list.
     *
     * @return to fetch big string id, or skip.
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
     * Remove a big string file UUID fetch done.
     *
     * @param uuid the UUID of the big string file fetched
     */
    public void doneFetchBigStringUuid(long uuid) {
        doFetchingBigStringIdList.removeIf(e -> e.uuid() == uuid);
    }
}
