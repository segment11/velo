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
        ((TcpSocket) socket).setUserData(null);
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
     * Checks if the socket uses RESP3 protocol.
     *
     * @param socket the socket to check
     * @return true if the socket uses RESP3, false otherwise
     */
    public static boolean isResp3(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return false;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData != null && veloUserData.isResp3;
    }

    /**
     * Sets whether the socket uses RESP3 protocol.
     *
     * @param socket  the socket to set
     * @param isResp3 true if the socket uses RESP3, false otherwise
     */
    public static void setResp3(ITcpSocket socket, boolean isResp3) {
        createUserDataIfNotSet(socket).isResp3 = isResp3;
    }

    /**
     * Checks if the connection is read-only.
     *
     * @param socket the socket to check
     * @return true if the connection is read-only, false otherwise
     */
    public static boolean isConnectionReadonly(ITcpSocket socket) {
        // just when do unit test
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
        createUserDataIfNotSet(socket).isConnectionReadonly = isConnectionReadonly;
    }

    /**
     * Sets the authenticated user for the socket.
     *
     * @param socket   the socket to set
     * @param authUser the authenticated user
     */
    public static void setAuthUser(ITcpSocket socket, String authUser) {
        createUserDataIfNotSet(socket).authUser = authUser;
    }

    /**
     * Gets the authenticated user for the socket.
     *
     * @param socket the socket to get the user from
     * @return the authenticated user, or null if not set
     */
    public static String getAuthUser(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return null;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData == null ? null : veloUserData.authUser;
    }

    /**
     * Flag indicating whether the server is stopped.
     */
    public volatile boolean isServerStopped = false;

    /**
     * Array of event loops for network workers.
     */
    @ThreadNeedLocal
    private Eventloop[] netWorkerEventloopArray;

    /**
     * Array of connected client counts for each network worker.
     */
    @ThreadNeedLocal
    int[] connectedClientCountArray;

    /**
     * Initializes the inspector with the given event loop array.
     *
     * @param netWorkerEventloopArray the array of event loops
     */
    void initByNetWorkerEventloopArray(Eventloop[] netWorkerEventloopArray) {
        this.netWorkerEventloopArray = netWorkerEventloopArray;
        this.connectedClientCountArray = new int[netWorkerEventloopArray.length];
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
     * Map of channels to subscribed sockets. Long means thread id.
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
                for (var eventloop : netWorkerEventloopArray) {
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

        var veloUserData = (VeloUserDataInSocket) socket.getUserData();
        if (veloUserData != null && veloUserData.replPairAsSlaveInTcpClient != null) {
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

        connectedClientCountArray[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()]++;
    }

    @Override
    public void onReadTimeout(TcpSocket socket) {

    }

    @Override
    public void onRead(TcpSocket socket, ByteBuf buf) {
//        log.debug("Inspector on read, remote address={}, buf size={}", socket.getRemoteAddress(), buf.readRemaining());
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

        connectedClientCountArray[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()]--;
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
