package io.velo.repl;

import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.TcpSocket;
import io.velo.ConfForGlobal;
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
     * @param slot     The slot identifier for this pair.
     * @param asMaster A flag indicating whether this pair acts as the master.
     * @param host     The hostname or IP address of the server.
     * @param port     The port number on which the server is listening.
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
     * @return The slot identifier.
     */
    public short getSlot() {
        return slot;
    }

    /**
     * Gets the hostname or IP address of the server.
     *
     * @return The hostname or IP address.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number on which the server is listening.
     *
     * @return The port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the formatted host and port as a string.
     *
     * @return The formatted host and port.
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
     * @param hostAndPort The host and port string to parse.
     * @return The HostAndPort record, or null if the input is null.
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

    /**
     * Gets the UUID of the master.
     *
     * @return The master UUID.
     */
    public long getMasterUuid() {
        return masterUuid;
    }

    /**
     * Sets the UUID of the master.
     *
     * @param masterUuid The master UUID to set.
     */
    public void setMasterUuid(long masterUuid) {
        this.masterUuid = masterUuid;
    }

    /**
     * Gets the UUID of the slave.
     *
     * @return The slave UUID.
     */
    public long getSlaveUuid() {
        return slaveUuid;
    }

    /**
     * Sets the UUID of the slave.
     *
     * @param slaveUuid The slave UUID to set.
     */
    public void setSlaveUuid(long slaveUuid) {
        this.slaveUuid = slaveUuid;
    }

    /**
     * Gets the timestamp of the last received ping from the master.
     *
     * @return The timestamp of the last received ping.
     */
    public long getLastPingGetTimestamp() {
        return lastPingGetTimestamp;
    }

    /**
     * Sets the timestamp of the last received ping from the master.
     *
     * @param lastPingGetTimestamp The timestamp to set.
     */
    public void setLastPingGetTimestamp(long lastPingGetTimestamp) {
        this.lastPingGetTimestamp = lastPingGetTimestamp;
    }

    /**
     * Gets the timestamp of the last received pong from the slave.
     *
     * @return The timestamp of the last received pong.
     */
    public long getLastPongGetTimestamp() {
        return lastPongGetTimestamp;
    }

    /**
     * Sets the timestamp of the last received pong from the slave.
     *
     * @param lastPongGetTimestamp The timestamp to set.
     */
    public void setLastPongGetTimestamp(long lastPongGetTimestamp) {
        this.lastPongGetTimestamp = lastPongGetTimestamp;
    }

    private long masterUuid;
    private long slaveUuid;

    // client side send ping, server side update timestamp
    @ForMasterField
    private long lastPingGetTimestamp;
    // server side send pong, client side update timestamp
    @ForSlaveField
    private long lastPongGetTimestamp;

    @ForSlaveField
    private final long[] statsCountWhenSlaveSkipFetch = new long[3];

    /**
     * Increases the count of a specific type of fetch skip for the slave.
     *
     * @param type The type of the fetch to skip.
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
     * @return The string representation of the statistics.
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
     * @param type The type of the REPL operation.
     */
    public void increaseStatsCountForReplType(ReplType type) {
        int i = type.ordinal();
        statsCountForReplType[i]++;

        // only log for catch up
        if (type == ReplType.catch_up || type == ReplType.s_catch_up) {
            if (statsCountForReplType[i] % 100 == 0) {
                log.info("Repl pair stats count for repl type, alive, target host={}, port={}, stats={}, slot={}",
                        host, port, getStatsCountForReplTypeAsString(), slot);
            }
        }
    }

    /**
     * Gets the string representation of the statistics for all REPL operation types.
     *
     * @return The string representation of the statistics.
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
     * @return The last caught-up sequence number for the slave.
     */
    public long getSlaveCatchUpLastSeq() {
        return slaveCatchUpLastSeq;
    }

    /**
     * Sets the last sequence number the slave has caught up to.
     *
     * @param slaveCatchUpLastSeq The last caught-up sequence number to set.
     */
    public void setSlaveCatchUpLastSeq(long slaveCatchUpLastSeq) {
        this.slaveCatchUpLastSeq = slaveCatchUpLastSeq;
    }

    @ForMasterField
    @ForSlaveField
    private Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset;

    /**
     * Gets the last binlog file index and offset that the slave has caught up to.
     *
     * @return The binlog file index and offset.
     */
    public Binlog.FileIndexAndOffset getSlaveLastCatchUpBinlogFileIndexAndOffset() {
        return slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    /**
     * Gets the REPL offset of the last binlog file index and offset that the slave has caught up to.
     *
     * @return The REPL offset of the last binlog file index and offset.
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
     * @param slaveLastCatchUpBinlogFileIndexAndOffset The binlog file index and offset to set.
     */
    public void setSlaveLastCatchUpBinlogFileIndexAndOffset(Binlog.FileIndexAndOffset slaveLastCatchUpBinlogFileIndexAndOffset) {
        this.slaveLastCatchUpBinlogFileIndexAndOffset = slaveLastCatchUpBinlogFileIndexAndOffset;
    }

    @ForSlaveField
    private Binlog.FileIndexAndOffset masterBinlogCurrentFileIndexAndOffset;

    /**
     * Gets the current binlog file index and offset of the master.
     *
     * @return The binlog file index and offset.
     */
    public Binlog.FileIndexAndOffset getMasterBinlogCurrentFileIndexAndOffset() {
        return masterBinlogCurrentFileIndexAndOffset;
    }

    /**
     * Sets the current binlog file index and offset of the master.
     *
     * @param masterBinlogCurrentFileIndexAndOffset The binlog file index and offset to set.
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
     * @return The total length of bytes fetched.
     */
    public long getFetchedBytesLengthTotal() {
        return fetchedBytesLengthTotal;
    }

    /**
     * Increases the total length of bytes fetched by a specified amount.
     *
     * @param fetchedBytesLength The amount of bytes to add to the total.
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
     * @param masterReadonly The read-only status to set.
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
     * @param allCaughtUp The caught-up status to set.
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
     * @param masterCanNotConnect The connection status to set.
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
     * @param eventloop      The event loop to be used for handling network operations.
     * @param requestHandler The request handler for processing incoming requests.
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
     * @return The disconnection timestamp.
     */
    public long getDisconnectTimeMillis() {
        return disconnectTimeMillis;
    }

    /**
     * Sets the timestamp of the last disconnection.
     *
     * @param disconnectTimeMillis The timestamp to set.
     */
    public void setDisconnectTimeMillis(long disconnectTimeMillis) {
        this.disconnectTimeMillis = disconnectTimeMillis;
    }

    @ForSlaveField
    private long lastGetCatchUpResponseMillis;

    /**
     * Gets the timestamp of the last catch-up response.
     *
     * @return The catch-up response timestamp.
     */
    public long getLastGetCatchUpResponseMillis() {
        return lastGetCatchUpResponseMillis;
    }

    /**
     * Sets the timestamp of the last catch-up response.
     *
     * @param lastGetCatchUpResponseMillis The timestamp to set.
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
     * @param flag The connection status flag to add.
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
     * @param isSendBye The flag to set.
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
     * @param type    The type of the message to be written.
     * @param content The content of the message to be written.
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
     * @return The timestamp to remove this pair.
     */
    public long getPutToDelayListToRemoveTimeMillis() {
        return putToDelayListToRemoveTimeMillis;
    }

    /**
     * Sets the timestamp to remove this pair from the delay list.
     *
     * @param putToDelayListToRemoveTimeMillis The timestamp to set.
     */
    public void setPutToDelayListToRemoveTimeMillis(long putToDelayListToRemoveTimeMillis) {
        this.putToDelayListToRemoveTimeMillis = putToDelayListToRemoveTimeMillis;
    }


    /**
     * Slave need fetch big string files after all catch up.
     */
    @ForSlaveField
    private final LinkedList<Long> toFetchBigStringUuidList = new LinkedList<>();
    /**
     * Slave doing fetch big string files.
     */
    @ForSlaveField
    private final LinkedList<Long> doFetchingBigStringUuidList = new LinkedList<>();

    /**
     * Gets the list of big string file UUIDs to fetch.
     *
     * @return The list of big string file UUIDs.
     */
    public LinkedList<Long> getToFetchBigStringUuidList() {
        return toFetchBigStringUuidList;
    }

    /**
     * Gets the list of big string file UUIDs being fetching.
     *
     * @return The list of big string file UUIDs.
     */
    public LinkedList<Long> getDoFetchingBigStringUuidList() {
        return doFetchingBigStringUuidList;
    }

    /**
     * Adds a big string file UUID to the list of files to fetch.
     *
     * @param uuid The UUID of the big string file to fetch.
     */
    public void addToFetchBigStringUuid(long uuid) {
        toFetchBigStringUuidList.add(uuid);
    }

    /**
     * Slave fetch a big string file uuid, from to fetch, FIFO list.
     *
     * @return to fetch big string uuid, or skip.
     */
    public long doingFetchBigStringUuid() {
        if (toFetchBigStringUuidList.isEmpty()) {
            return BigStringFiles.SKIP_UUID;
        }
        var first = toFetchBigStringUuidList.pollFirst();
        doFetchingBigStringUuidList.add(first);
        return first;
    }

    /**
     * Remove a big string file UUID fetch done.
     *
     * @param uuid The UUID of the big string file fetched.
     */
    public void doneFetchBigStringUuid(long uuid) {
        doFetchingBigStringUuidList.removeIf(e -> e == uuid);
    }
}
