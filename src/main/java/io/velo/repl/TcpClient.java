package io.velo.repl;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.velo.*;
import io.velo.command.XGroup;
import io.velo.decode.RequestDecoder;
import io.velo.repl.content.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

/**
 * A TCP client for handling communication as slave to a master server.
 * This client is designed to write and read data to/from a server using TCP sockets.
 * It also manages the lifecycle of the connection, including connecting, writing data, reading data, and closing the connection.
 */
public class TcpClient implements NeedCleanUp {
    private final short slot;
    private final Eventloop netWorkerEventloop;
    private final RequestHandler requestHandler;
    private final ReplPair replPair;

    /**
     * Constructs a new TcpClient with the specified parameters.
     *
     * @param slot               The slot identifier for this client.
     * @param netWorkerEventloop The event loop to be used for handling network operations.
     * @param requestHandler     The request handler for processing incoming requests.
     * @param replPair           The pair of REPLs associated with this client.
     */
    public TcpClient(short slot, Eventloop netWorkerEventloop, RequestHandler requestHandler, ReplPair replPair) {
        this.slot = slot;
        this.netWorkerEventloop = netWorkerEventloop;
        this.requestHandler = requestHandler;
        this.replPair = replPair;
    }

    private static final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private TcpSocket sock;

    /**
     * Checks if the TCP socket is currently connected and not closed.
     *
     * @return true if the socket is connected, false otherwise.
     */
    boolean isSocketConnected() {
        return sock != null && !sock.isClosed();
    }

    private long writeErrorCount = 0;
    private long notConnectedErrorCount = 0;

    /**
     * Writes a message to the server if the socket is connected.
     *
     * @param type    The type of the message to be written.
     * @param content The content of the message to be written.
     * @return true if the write was successful, false otherwise.
     */
    boolean write(ReplType type, ReplContent content) {
        if (isSocketConnected()) {
            try {
                sock.write(Repl.buffer(replPair.getSlaveUuid(), slot, type, content));
                writeErrorCount = 0;
                return true;
            } catch (Exception e) {
                // Reduce log spamming
                if (writeErrorCount % 1000 == 0) {
                    log.error("Could not write to server, to server={}, slot={}", replPair.getHostAndPort(), slot, e);
                }
                writeErrorCount++;
                return false;
            } finally {
                notConnectedErrorCount = 0;
            }
        } else {
            if (notConnectedErrorCount % 1000 == 0) {
                log.error("Socket is not connected, to server={}, slot={}", replPair.getHostAndPort(), slot);
            }
            notConnectedErrorCount++;
            return false;
        }
    }

    /**
     * Sends a ping message to the server.
     *
     * @return true if the ping was sent successfully, false otherwise.
     */
    public boolean ping() {
        return write(ReplType.ping, new Ping(ConfForGlobal.netListenAddresses));
    }

    /**
     * Sends a bye message to the server and logs the event.
     *
     * @return true if the bye message was sent successfully, false otherwise.
     */
    public boolean bye() {
        log.warn("Repl slave send bye to server={}, slot={}", replPair.getHostAndPort(), slot);
        System.out.println("Repl slave send bye to server=" + replPair.getHostAndPort() + ", slot=" + slot);
        return write(ReplType.bye, new Ping(ConfForGlobal.netListenAddresses));
    }

    /**
     * Attempts to connect to the server at the specified host and port.
     *
     * @param host              The hostname or IP address of the server.
     * @param port              The port number on which the server is listening.
     * @param connectedCallback A callback to be called when the connection is established.
     */
    public void connect(String host, int port, Callable<ByteBuf> connectedCallback) {
        TcpSocket.connect(netWorkerEventloop, new InetSocketAddress(host, port))
                .whenResult(socket -> {
                    log.info("Connected to server at {}:{}, slot={}", host, port, slot);

                    socket.setUserData(new VeloUserDataInSocket(replPair));
                    socket.setInspector(MultiWorkerServer.STATIC_GLOBAL_V.socketInspector);
                    MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.onConnect(socket);

                    sock = socket;

                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .mapAsync(pipeline -> {
                                if (pipeline == null) {
                                    log.error("Repl slave request decode fail: pipeline is null, slot={}", slot);
                                    return null;
                                }

                                Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
                                for (int i = 0; i < pipeline.size(); i++) {
                                    var request = pipeline.get(i);

                                    var xGroup = new XGroup(null, request.getData(), socket);
                                    xGroup.init(requestHandler, request);
                                    xGroup.setReplPair(replPair);

                                    try {
                                        var reply = xGroup.handleRepl();
                                        if (reply == null) {
                                            promiseN[i] = Promise.of(null);
                                        } else {
                                            promiseN[i] = Promise.of(reply.buffer());
                                        }
                                    } catch (Exception e) {
                                        promiseN[i] = Promise.of(Repl.error(slot, replPair, "Repl slave handle error=" + e.getMessage()).buffer());
                                    }
                                }

                                if (pipeline.size() == 1) {
                                    return promiseN[0];
                                } else {
                                    return MultiWorkerServer.allPipelineByteBuf(promiseN);
                                }
                            })
                            .streamTo(ChannelConsumers.ofSocket(socket));

                    if (connectedCallback != null) {
                        sock.write(connectedCallback.call());
                    }
                })
                .whenException(e -> log.error("Could not connect to server, to server={}:{}, slot={}", host, port, slot, e));
    }

    /**
     * Closes the TCP connection if it is open.
     */
    public void close() {
        if (sock != null && !sock.isClosed()) {
            sock.close();
            System.out.println("Repl closed socket, to server=" + replPair.getHostAndPort() + ", slot=" + slot);
        } else {
            System.out.println("Repl socket is already closed, to server=" + replPair.getHostAndPort() + ", slot=" + slot);
        }
    }

    @Override
    public void cleanUp() {
        close();
    }
}