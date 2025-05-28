package io.velo;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.velo.command.XGroup;
import io.velo.reply.Reply;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Inspects and manages TCP sockets for a Velo server.
 * This class handles socket connections, subscriptions, and data transmission, ensuring efficient management of client connections.
 */
public class SocketInspector implements TcpSocket.Inspector {
    /**
     * Logger for logging information and errors.
     */
    private static final Logger log = LoggerFactory.getLogger(SocketInspector.class);

    /**
     * Clears the user data associated with a socket.
     *
     * @param socket the socket to clear user data from
     */
    @TestOnly
    public static void clearUserData(ITcpSocket socket) {
        ((TcpSocket) socket).setUserData(new VeloUserDataInSocket());
    }

    /**
     * Creates user data for a socket if it is not already set.
     *
     * @param socket the socket to create user data for
     * @return the user data associated with the socket
     */
    public static VeloUserDataInSocket createUserDataIfNotSet(ITcpSocket socket) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        if (veloUserData == null) {
            veloUserData = new VeloUserDataInSocket();
            ((TcpSocket) socket).setUserData(veloUserData);
        }
        return veloUserData;
    }

    /**
     * Updates the last send command for a socket.
     *
     * @param socket           the socket to update
     * @param lastSendCommand  the last send command
     * @param sendCommandCount the number of send commands this time
     */
    public static void updateLastSendCommand(ITcpSocket socket, String lastSendCommand, long sendCommandCount) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        veloUserData.lastSendCommandTimeMillis = System.currentTimeMillis();
        veloUserData.lastSendCommand = lastSendCommand;
        veloUserData.sendCommandCount += sendCommandCount;
    }

    /**
     * Checks if the socket uses RESP3 protocol.
     *
     * @param socket the socket to check
     * @return true if the socket uses RESP3, false otherwise
     */
    public static boolean isResp3(ITcpSocket socket) {
        // for unit test case
        if (socket == null) {
            return false;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        return veloUserData.isResp3;
    }

    /**
     * Sets whether the socket uses RESP3 protocol.
     *
     * @param socket  the socket to set
     * @param isResp3 true if the socket uses RESP3, false otherwise
     */
    public static void setResp3(ITcpSocket socket, boolean isResp3) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        veloUserData.isResp3 = isResp3;
    }

    /**
     * Checks if the connection is read-only.
     *
     * @param socket the socket to check
     * @return true if the connection is read-only, false otherwise
     */
    public static boolean isConnectionReadonly(ITcpSocket socket) {
        // for unit test case
        if (socket == null) {
            return false;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData != null && veloUserData.isConnectionReadonly;
    }

    /**
     * Sets whether the connection is read-only.
     *
     * @param socket               the socket to set
     * @param isConnectionReadonly true if the connection is read-only, false otherwise
     */
    public static void setConnectionReadonly(ITcpSocket socket, boolean isConnectionReadonly) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        veloUserData.isConnectionReadonly = isConnectionReadonly;
    }

    /**
     * Sets the authenticated user for the socket.
     *
     * @param socket   the socket to set
     * @param authUser the authenticated user
     */
    public static void setAuthUser(ITcpSocket socket, String authUser) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        veloUserData.authUser = authUser;
    }

    /**
     * Gets the authenticated user for the socket.
     *
     * @param socket the socket to get the user from
     * @return the authenticated user, or null if not set
     */
    public static String getAuthUser(ITcpSocket socket) {
        // for unit test case
        if (socket == null) {
            return null;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        return veloUserData.authUser;
    }

    /**
     * For command client info / list
     *
     * @param socket the client socket
     * @return the client info reply, refer to <a href="https://redis.io/docs/latest/commands/client-info/">client info</a>
     */
    public static String getClientInfo(ITcpSocket socket) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        assert veloUserData != null;
        var remoteAddress = ((TcpSocket) socket).getRemoteAddress();

        var sb = new StringBuilder();
        sb.append("id=");
        sb.append(socket.hashCode());
        sb.append(" addr=");
        sb.append(remoteAddress);
        sb.append(" laddr=");
        sb.append(ConfForGlobal.netListenAddresses);
        sb.append(" fd=");
        // use id as fd
        sb.append(socket.hashCode());
        sb.append(" name=");
        if (veloUserData.getClientName() != null) {
            sb.append(veloUserData.getClientName());
        }
        sb.append(" age=");
        sb.append((System.currentTimeMillis() - veloUserData.connectedTimeMillis) / 1000);
        sb.append(" idle=");
        sb.append((System.currentTimeMillis() - veloUserData.lastSendCommandTimeMillis) / 1000);
        // fake data
        sb.append(" flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=1024 qbuf-free=10240 argv-mem=1024 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=10240 events=r");
        sb.append(" cmd=");
        sb.append(veloUserData.lastSendCommand);
        sb.append(" user=");
        sb.append(veloUserData.authUser == null ? "default" : veloUserData.authUser);
        sb.append(" redir=-1 resp=");
        sb.append(veloUserData.isResp3 ? "3" : "2");
        sb.append(" lib-name=");
        sb.append(veloUserData.libName == null ? "" : veloUserData.libName);
        sb.append(" lib-ver=");
        sb.append(veloUserData.libVer == null ? "" : veloUserData.libVer);
        sb.append(" tot-net-in=");
        sb.append(veloUserData.netInBytesLength);
        sb.append(" tot-net-out=");
        sb.append(veloUserData.netOutBytesLength);
        sb.append(" tot-cmds=");
        sb.append(veloUserData.sendCommandCount);
        sb.append("\n");
        return sb.toString();
    }

    /**
     * Flag indicating whether the server is stopped.
     */
    public volatile boolean isServerStopped = false;

    /**
     * Array of event loops for slot workers.
     */
    @ThreadNeedLocal
    private Eventloop[] slotWorkerEventloopArray;

    /**
     * Array of connected client counts for each network worker.
     */
    @ThreadNeedLocal(type = "net")
    int[] connectedClientCountArray;

    /**
     * Array of network input bytes lengths for each network worker.
     * for stats
     */
    @ThreadNeedLocal(type = "net")
    long[] netInBytesLengthArray;
    /**
     * Array of network output bytes lengths for each network worker.
     * for stats
     */
    @ThreadNeedLocal(type = "net,slot")
    long[] netOutBytesLengthArray;

    /**
     * Gets the total network input bytes length across all network workers.
     *
     * @return The total network input bytes length.
     */
    public long netInBytesLength() {
        var total = 0L;
        for (long l : netInBytesLengthArray) {
            total += l;
        }
        return total;
    }

    /**
     * Gets the total network output bytes length across all network workers.
     *
     * @return The total network output bytes length.
     */
    public long netOutBytesLength() {
        var total = 0L;
        for (long l : netOutBytesLengthArray) {
            total += l;
        }
        return total;
    }

    /**
     * Initializes the inspector with the given event loop array.
     *
     * @param slotWorkerEventloopArray the array of slot event loops
     * @param netWorkerEventloopArray  the array of net event loops
     */
    public void initByNetWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray, Eventloop[] netWorkerEventloopArray) {
        this.slotWorkerEventloopArray = slotWorkerEventloopArray;

        this.connectedClientCountArray = new int[netWorkerEventloopArray.length];
        this.netInBytesLengthArray = new long[netWorkerEventloopArray.length];
        this.netOutBytesLengthArray = new long[netWorkerEventloopArray.length + slotWorkerEventloopArray.length];
    }

    /**
     * Represents a channel and whether it is a pattern.
     */
    public static class ChannelAndIsPattern {
        private final String channel;
        private final boolean isPattern;
        private final PathMatcher pathMatcher;

        /**
         * Returns the channel.
         *
         * @return the channel
         */
        public String getChannel() {
            return channel;
        }

        /**
         * Constructs a new ChannelAndIsPattern instance.
         *
         * @param channel   the channel
         * @param isPattern true if the channel is a pattern, false otherwise
         */
        ChannelAndIsPattern(@NotNull String channel, boolean isPattern) {
            this.channel = channel;
            this.isPattern = isPattern;
            if (isPattern) {
                pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + channel);
            } else {
                pathMatcher = null;
            }
        }

        /**
         * Returns the hash code of the channel and pattern flag.
         *
         * @return the hash code
         */
        @Override
        public int hashCode() {
            return isPattern ? channel.hashCode() * 31 : channel.hashCode();
        }

        /**
         * Checks if this object is equal to another object.
         *
         * @param obj the object to compare to
         * @return true if the objects are equal, false otherwise
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof ChannelAndIsPattern other)) {
                return false;
            }

            if (isPattern != other.isPattern) {
                return false;
            }

            return channel.equals(other.channel);
        }

        /**
         * Checks if the given channel matches this channel.
         *
         * @param channel2 the channel to check
         * @return true if the channel matches, false otherwise
         */
        boolean isMatch(String channel2) {
            if (!isPattern) {
                return channel.equals(channel2);
            }

            return pathMatcher.matches(FileSystems.getDefault().getPath(channel2));
        }
    }

    /**
     * Map of socket addresses to TCP sockets. For connected clients.
     */
    public final ConcurrentHashMap<InetSocketAddress, TcpSocket> socketMap = new ConcurrentHashMap<>();

    /**
     * Returns the total number of connected clients.
     *
     * @return the total number of connected clients
     */
    public int connectedClientCount() {
        return socketMap.size();
    }

    /**
     * Map of channels to subscribed sockets. Long means slot eventloop thread id.
     */
    private final ConcurrentHashMap<ChannelAndIsPattern, ConcurrentHashMap<ITcpSocket, Long>> subscribeByChannel = new ConcurrentHashMap<>();

    /**
     * Returns a list of all subscribed channels.
     *
     * @return a list of all subscribed channels
     */
    public ArrayList<ChannelAndIsPattern> allSubscribeOnes() {
        return new ArrayList<>(subscribeByChannel.keySet());
    }

    /**
     * Returns the total number of subscribed clients.
     *
     * @return the total number of subscribed clients
     */
    public int subscribeClientCount() {
        var total = 0;
        for (var one : subscribeByChannel.values()) {
            total += one.size();
        }
        return total;
    }

    /**
     * Filters subscribed channels by a given channel and pattern flag.
     *
     * @param channel   the channel to filter by
     * @param isPattern true if the channel is a pattern, false otherwise
     * @return a list of filtered subscribed channels
     */
    public ArrayList<ChannelAndIsPattern> filterSubscribeOnesByChannel(@NotNull String channel, boolean isPattern) {
        var two = new ChannelAndIsPattern(channel, isPattern);
        ArrayList<ChannelAndIsPattern> oneList = new ArrayList<>();
        subscribeByChannel.forEach((one, value) -> {
            if (one.isMatch(channel) || two.isMatch(one.channel)) {
                oneList.add(one);
            }
        });
        return oneList;
    }

    /**
     * Subscribes a socket to a channel.
     *
     * @param channel   the channel to subscribe to
     * @param isPattern true if the channel is a pattern, false otherwise
     * @param socket    the socket to subscribe
     * @return the number of sockets subscribed to the channel
     */
    public int subscribe(@NotNull String channel, boolean isPattern, ITcpSocket socket) {
        var one = new ChannelAndIsPattern(channel, isPattern);
        var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
        sockets.put(socket, Thread.currentThread().threadId());
        return sockets.size();
    }

    /**
     * Unsubscribes a socket from a channel.
     *
     * @param channel   the channel to unsubscribe from
     * @param isPattern true if the channel is a pattern, false otherwise
     * @param socket    the socket to unsubscribe
     * @return the number of sockets subscribed to the channel after unsubscription
     */
    public int unsubscribe(@NotNull String channel, boolean isPattern, ITcpSocket socket) {
        var one = new ChannelAndIsPattern(channel, isPattern);
        var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
        sockets.remove(socket);
        return sockets.size();
    }

    /**
     * Returns the number of sockets subscribed to a channel.
     *
     * @param channel   the channel to check
     * @param isPattern true if the channel is a pattern, false otherwise
     * @return the number of sockets subscribed to the channel
     */
    public int subscribeSocketCount(String channel, boolean isPattern) {
        var oneList = filterSubscribeOnesByChannel(channel, isPattern);
        if (oneList.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (var one : oneList) {
            var sockets = subscribeByChannel.get(one);
            if (sockets != null) {
                count += sockets.size();
            }
        }
        return count;
    }

    /**
     * Callback interface for publishing messages to sockets.
     */
    public interface PublishWriteSocketCallback {
        /**
         * Performs an action with the given socket and reply.
         *
         * @param socket the socket to perform the action on
         * @param reply  the reply to send
         */
        void doWithSocket(ITcpSocket socket, Reply reply);
    }

    /**
     * Publishes a message to all sockets subscribed to a channel.
     *
     * @param channel  the channel to publish to
     * @param reply    the reply to send
     * @param callback the callback to execute with each socket and reply
     * @return the number of sockets the message was published to
     */
    public int publish(String channel, Reply reply, PublishWriteSocketCallback callback) {
        var oneList = filterSubscribeOnesByChannel(channel, false);
        if (oneList.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (var one : oneList) {
            count += publishOne(one, reply, callback);
        }
        return count;
    }

    /**
     * Publishes a message to all sockets subscribed to a specific channel and pattern.
     *
     * @param one      the channel and pattern
     * @param reply    the reply to send
     * @param callback the callback to execute with each socket and reply
     * @return the number of sockets the message was published to
     */
    private int publishOne(@NotNull ChannelAndIsPattern one, Reply reply, PublishWriteSocketCallback callback) {
        var sockets = subscribeByChannel.get(one);
        if (sockets == null) {
            return 0;
        }

        for (var map : sockets.entrySet()) {
            var socket = map.getKey();
            var threadId = map.getValue();
            if (Thread.currentThread().threadId() == threadId) {
                callback.doWithSocket(socket, reply);
            } else {
                for (var eventloop : slotWorkerEventloopArray) {
                    assert eventloop.getEventloopThread() != null;
                    if (eventloop.getEventloopThread().threadId() == threadId) {
                        eventloop.execute(() -> callback.doWithSocket(socket, reply));
                    }
                }
            }
        }
        return sockets.size();
    }

    /**
     * Maximum number of connections allowed.
     */
    private int maxConnections = 1000;

    /**
     * Returns the maximum number of connections allowed.
     *
     * @return the maximum number of connections
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Sets the maximum number of connections allowed.
     *
     * @param maxConnections the maximum number of connections to set
     */
    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Configuration key for maximum connections in dynamic configuration.
     */
    public static final String MAX_CONNECTIONS_KEY_IN_DYN_CONFIG = "max_connections";

    /**
     * Called when a socket connects.
     *
     * @param socket the socket that connected
     */
    @Override
    public void onConnect(TcpSocket socket) {
        if (isServerStopped) {
            log.warn("Inspector on connect, server stopped, close the socket");
            socket.close();
            return;
        }

        var veloUserData = createUserDataIfNotSet(socket);
        if (veloUserData.replPairAsSlaveInTcpClient != null) {
            // this socket is a slave connection master
            // need not check max connections
            var remoteAddress = socket.getRemoteAddress();
            log.info("Inspector on repl connect, remote address={}, slot={}", remoteAddress, veloUserData.replPairAsSlaveInTcpClient.getSlot());
            return;
        }

        if (socketMap.size() >= maxConnections) {
            log.warn("Inspector max connections reached={}, close the socket", maxConnections);
            socket.close();
            return;
        }

        var remoteAddress = socket.getRemoteAddress();
        log.debug("Inspector on connect, remote address={}", remoteAddress);
        socketMap.put(remoteAddress, socket);

        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
        connectedClientCountArray[threadIndex]++;
    }

    @Override
    public void onReadTimeout(TcpSocket socket) {

    }

    @Override
    public void onRead(TcpSocket socket, ByteBuf buf) {
        var bytes = buf.readRemaining();
        var veloUserData = (VeloUserDataInSocket) socket.getUserData();
        assert veloUserData != null;
        veloUserData.netInBytesLength += bytes;

        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
        netInBytesLengthArray[threadIndex] += bytes;
    }

    @Override
    public void onReadEndOfStream(TcpSocket socket) {

    }

    @Override
    public void onReadError(TcpSocket socket, IOException e) {

    }

    @Override
    public void onWriteTimeout(TcpSocket socket) {

    }

    @Override
    public void onWrite(TcpSocket socket, ByteBuf buf, int bytes) {
        var veloUserData = (VeloUserDataInSocket) socket.getUserData();
        assert veloUserData != null;
        veloUserData.netOutBytesLength += bytes;

        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
        if (threadIndex != -1) {
            netOutBytesLengthArray[threadIndex] += bytes;
        } else {
            // when async reply, slot eventloop trigger write
            threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getSlotThreadLocalIndexByCurrentThread();
            assert threadIndex != -1;
            netOutBytesLengthArray[ConfForGlobal.netWorkers + threadIndex] += bytes;
        }
    }

    @Override
    public void onWriteError(TcpSocket socket, IOException e) {

    }

    /**
     * Called when a socket disconnects.
     *
     * @param socket the socket that disconnected
     */
    @Override
    public void onDisconnect(TcpSocket socket) {
        var remoteAddress = socket.getRemoteAddress();

        var veloUserData = (VeloUserDataInSocket) socket.getUserData();
        if (veloUserData != null && veloUserData.replPairAsSlaveInTcpClient != null) {
            var replPair = veloUserData.replPairAsSlaveInTcpClient;
            log.info("Inspector on repl disconnect, remote address={}, slot={}", remoteAddress, replPair.getSlot());
            replPair.setDisconnectTimeMillis(System.currentTimeMillis());
            XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPair, null);
            return;
        }

        log.debug("Inspector on disconnect, remote address={}", remoteAddress);
        socketMap.remove(remoteAddress);

        // remove from subscribe by channel
        subscribeByChannel.forEach((channel, sockets) -> sockets.remove(socket));

        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getNetThreadLocalIndexByCurrentThread();
        connectedClientCountArray[threadIndex]--;
    }

    @Override
    public <T extends TcpSocket.Inspector> @Nullable T lookup(Class<T> type) {
        return null;
    }

    public void clearAll() {
        subscribeByChannel.clear();

        socketMap.clear();
    }
}
