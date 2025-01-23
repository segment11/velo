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

public class SocketInspector implements TcpSocket.Inspector {
    private static final Logger log = LoggerFactory.getLogger(SocketInspector.class);

    @TestOnly
    public static void clearUserData(ITcpSocket socket) {
        ((TcpSocket) socket).setUserData(null);
    }

    public static VeloUserDataInSocket createUserDataIfNotSet(ITcpSocket socket) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        if (veloUserData == null) {
            veloUserData = new VeloUserDataInSocket();
            ((TcpSocket) socket).setUserData(veloUserData);
        }
        return veloUserData;
    }

    public static boolean isResp3(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return false;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData != null && veloUserData.isResp3;
    }

    public static void setResp3(ITcpSocket socket, boolean isResp3) {
        createUserDataIfNotSet(socket).isResp3 = isResp3;
    }

    public static boolean isConnectionReadonly(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return false;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData != null && veloUserData.isConnectionReadonly;
    }

    public static void setConnectionReadonly(ITcpSocket socket, boolean isConnectionReadonly) {
        createUserDataIfNotSet(socket).isConnectionReadonly = isConnectionReadonly;
    }

    public static void setAuthUser(ITcpSocket socket, String authUser) {
        createUserDataIfNotSet(socket).authUser = authUser;
    }

    public static String getAuthUser(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return null;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData == null ? null : veloUserData.authUser;
    }

    public volatile boolean isServerStopped = false;

    @ThreadNeedLocal
    private Eventloop[] netWorkerEventloopArray;
    @ThreadNeedLocal
    int[] connectedClientCountArray;

    void initByNetWorkerEventloopArray(Eventloop[] netWorkerEventloopArray) {
        this.netWorkerEventloopArray = netWorkerEventloopArray;
        this.connectedClientCountArray = new int[netWorkerEventloopArray.length];
    }

    public static class ChannelAndIsPattern {
        private final String channel;
        private final boolean isPattern;
        private final PathMatcher pathMatcher;

        public String getChannel() {
            return channel;
        }

        ChannelAndIsPattern(@NotNull String channel, boolean isPattern) {
            this.channel = channel;
            this.isPattern = isPattern;
            if (isPattern) {
                pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + channel);
            } else {
                pathMatcher = null;
            }
        }

        @Override
        public int hashCode() {
            return isPattern ? channel.hashCode() * 31 : channel.hashCode();
        }

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

        boolean isMatch(String channel2) {
            if (!isPattern) {
                return channel.equals(channel2);
            }

            return pathMatcher.matches(FileSystems.getDefault().getPath(channel2));
        }
    }

    public final ConcurrentHashMap<InetSocketAddress, TcpSocket> socketMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelAndIsPattern, ConcurrentHashMap<ITcpSocket, Long>> subscribeByChannel = new ConcurrentHashMap<>();

    public ArrayList<ChannelAndIsPattern> allSubscribeOnes() {
        return new ArrayList<>(subscribeByChannel.keySet());
    }

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

    public int subscribe(@NotNull String channel, boolean isPattern, ITcpSocket socket) {
        var one = new ChannelAndIsPattern(channel, isPattern);
        var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
        sockets.put(socket, Thread.currentThread().threadId());
        return sockets.size();
    }

    public int unsubscribe(@NotNull String channel, boolean isPattern, ITcpSocket socket) {
        var one = new ChannelAndIsPattern(channel, isPattern);
        var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
        sockets.remove(socket);
        return sockets.size();
    }

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

    public interface PublishWriteSocketCallback {
        void doWithSocket(ITcpSocket socket, Reply reply);
    }

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

    private int maxConnections = 1000;

    public int getMaxConnections() {
        return maxConnections;
    }

    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public static final String MAX_CONNECTIONS_KEY_IN_DYN_CONFIG = "max_connections";

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
        log.info("Inspector on connect, remote address={}", remoteAddress);
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

        log.info("Inspector on disconnect, remote address={}", remoteAddress);
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
