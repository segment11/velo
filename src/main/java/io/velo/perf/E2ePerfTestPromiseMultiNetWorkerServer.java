package io.velo.perf;

import io.activej.bytebuf.ByteBuf;
import io.activej.launcher.Launcher;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.velo.decode.Request;
import io.velo.reply.OKReply;

public class E2ePerfTestPromiseMultiNetWorkerServer extends E2ePerfTestMultiNetWorkerServer {
    @Override
    Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
        return Promise.of(OKReply.INSTANCE.buffer());
    }

    public static void main(String[] args) throws Exception {
        Launcher launcher = new E2ePerfTestPromiseMultiNetWorkerServer();
        launcher.launch(args);
    }
}
