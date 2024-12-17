package io.velo;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.prometheus.client.Gauge;
import io.velo.command.XGroup;
import io.velo.reply.Reply;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class SocketInspector implements TcpSocket.Inspector {
    private static final Logger log = LoggerFactory.getLogger(SocketInspector.class);

    @TestOnly
    public static void clearUserData(ITcpSocket socket) {
        ((TcpSocket) socket).setUserData(null);
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
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        if (veloUserData == null) {
            veloUserData = new VeloUserDataInSocket();
            ((TcpSocket) socket).setUserData(veloUserData);
        }
        veloUserData.isResp3 = isResp3;
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
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        if (veloUserData == null) {
            veloUserData = new VeloUserDataInSocket();
            ((TcpSocket) socket).setUserData(veloUserData);
        }
        veloUserData.isConnectionReadonly = isConnectionReadonly;
    }

    public static void setAuthUser(ITcpSocket socket, String authUser) {
        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        if (veloUserData == null) {
            veloUserData = new VeloUserDataInSocket();
            ((TcpSocket) socket).setUserData(veloUserData);
        }
        veloUserData.authUser = authUser;
    }

    public static String getAuthUser(ITcpSocket socket) {
        // just when do unit test
        if (socket == null) {
            return null;
        }

        var veloUserData = (VeloUserDataInSocket) ((TcpSocket) socket).getUserData();
        return veloUserData == null ? null : veloUserData.authUser;
    }

    volatile boolean isServerStopped = false;

    @ThreadNeedLocal
    Eventloop[] netWorkerEventloopArray;

    final ConcurrentHashMap<InetSocketAddress, TcpSocket> socketMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<ITcpSocket, Long>> subscribeByChannel = new ConcurrentHashMap<>();

    public int subscribe(String channel, ITcpSocket socket) {
        var sockets = subscribeByChannel.computeIfAbsent(channel, k -> new ConcurrentHashMap<>());
        sockets.put(socket, Thread.currentThread().threadId());
        return sockets.size();
    }

    public int unsubscribe(String channel, ITcpSocket socket) {
        var sockets = subscribeByChannel.computeIfAbsent(channel, k -> new ConcurrentHashMap<>());
        sockets.remove(socket);
        return sockets.size();
    }

    public int subscribeSocketCount(String channel) {
        var sockets = subscribeByChannel.get(channel);
        return sockets == null ? 0 : sockets.size();
    }

    public interface PublishWriteSocketCallback {
        void doWithSocket(ITcpSocket socket, Reply reply);
    }

    public int publish(String channel, Reply reply, PublishWriteSocketCallback callback) {
        var sockets = subscribeByChannel.get(channel);
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

    // inject, singleton, need not static
    private static final Gauge connectedCountGauge = Gauge.build()
            .name("connected_client_count")
            .help("connected client count")
            .register();

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

        connectedCountGauge.inc();
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

        connectedCountGauge.dec();
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
